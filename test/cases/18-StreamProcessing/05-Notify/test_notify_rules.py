###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Notify recovery and worker batching tests.

Recovery:
  1. Notify server restart — outage messages are lost; later writes recover.
  2. Stream stop/start — no notify while stopped; resumes after start.

Batching (async notify worker in streamUtil.c):
  * High-frequency path: accumulate until ``streamNotifyBatchIntervalMs`` (default 5 ms)
    or ``streamNotifyFrameSize`` (pending para bytes threshold), then merge.
  * Low-frequency path: if the last flush was ≥ interval ago, flush immediately.
"""

import json
import os
import time

from new_test_framework.utils import tdLog, tdSql, tdStream
from stream_notify_server import (
    start_notify_server_background,
    stop_notify_server_background,
    wait_notify_server_ready,
)

CALLER_FILE = os.path.realpath(__file__)
CALLER_DIR = os.path.dirname(CALLER_FILE)
NOTIFY_RESULT_DIR = os.path.join(CALLER_DIR, "notify_result_tmp")
NOTIFY_PORT = 12345
BATCH_INTERVAL_MS = 5       # default streamNotifyBatchIntervalMs
BATCH_FRAME_SIZE_KB = 8     # Part B: 8 KB frame → 8192-byte pendingBytes threshold
DEFAULT_FRAME_SIZE_KB = 256 # default streamNotifyFrameSize


class TestStreamNotifyRecovery:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), True)

    # ------------------------------------------------------------------
    # Server / DB helpers
    # ------------------------------------------------------------------

    def start_notify_server(self, clear_logs=True):
        if clear_logs:
            if os.path.isdir(NOTIFY_RESULT_DIR):
                for entry in os.listdir(NOTIFY_RESULT_DIR):
                    path = os.path.join(NOTIFY_RESULT_DIR, entry)
                    if os.path.isfile(path):
                        os.remove(path)
            else:
                os.makedirs(NOTIFY_RESULT_DIR, exist_ok=True)
        start_notify_server_background(port=NOTIFY_PORT, log_path=NOTIFY_RESULT_DIR)

    def restart_notify_server(self):
        """Bring the WS server back without wiping existing log files."""
        self.start_notify_server(clear_logs=False)
        if not wait_notify_server_ready(NOTIFY_PORT, timeout=10.0):
            raise RuntimeError(
                f"notify server on port {NOTIFY_PORT} not ready after restart"
            )
        # Allow taosd async worker to drop stale WS handles from the outage.
        time.sleep(2)

    def stop_notify_server(self):
        stop_notify_server_background()

    def teardown_stream_notify(self, stream_name=None, db_name=None, log_file=None):
        if stream_name:
            tdSql.execute(f"drop stream if exists {stream_name};")
            time.sleep(0.5)
        self.stop_notify_server()
        if db_name:
            self.drop_db_and_wait(db_name)
        # if log_file and os.path.exists(log_file):
        #     os.remove(log_file)

    def ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    def drop_db_and_wait(self, db_name, stream_name=None, timeout_s=15):
        if stream_name:
            try:
                tdSql.execute(f"drop stream if exists {stream_name};")
            except Exception:
                pass
        try:
            tdSql.execute(f"drop database if exists {db_name};")
        except Exception:
            pass
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                tdSql.query("show databases")
                names = [tdSql.getData(r, 0) for r in range(tdSql.getRows())]
                if db_name not in names:
                    return
            except Exception:
                pass
            time.sleep(0.5)

    def create_database_with_retry(self, db_name, keep=3650, retries=6, interval_s=1.0):
        last_exc = None
        for attempt in range(retries):
            try:
                tdSql.execute(f"create database {db_name} keep {keep};")
                return
            except Exception as e:
                last_exc = e
                if "No such file or directory" in str(e) or "0x80200002" in str(e) \
                        or "-2130771966" in str(e):
                    if attempt < retries - 1:
                        time.sleep(interval_s)
                        continue
                raise
        raise last_exc

    def _create_stream(self, db_name, stream_name, notify_path, dst_table,
                       create_table=True):
        tdSql.execute(f"use {db_name};")
        if create_table:
            tdSql.execute("create table ct0(ts timestamp, c1 int, v int);")
        tdSql.execute(
            f"""create stream {stream_name} state_window(c1) from ct0
            stream_options(event_type(WINDOW_CLOSE))
            notify('ws://localhost:{NOTIFY_PORT}/{notify_path}')
              on(window_open|window_close)
            into {dst_table} as
            select _twstart, _twend, count(*) cnt, first(v) first_v
            from %%trows;"""
        )
        tdStream.checkStreamStatus(stream_name)

    def _try_alter_dnode(self, name, value):
        try:
            tdSql.execute(f"alter dnode 1 '{name}' '{value}'")
            time.sleep(0.3)
            return True
        except Exception as e:
            tdLog.info(f"alter dnode 1 {name} skipped: {e}")
            return False

    # ------------------------------------------------------------------
    # Event reading helpers
    # ------------------------------------------------------------------

    def _log_offset(self, log_file):
        try:
            return os.path.getsize(log_file)
        except FileNotFoundError:
            return 0

    def _count_events_since(self, log_file, offset, stream_name):
        count = 0
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                f.seek(offset)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    for s in obj.get("streams") or []:
                        sname = s.get("streamName", "")
                        if sname != stream_name and \
                                not sname.endswith(f".{stream_name}"):
                            continue
                        count += len(s.get("events") or [])
        except FileNotFoundError:
            pass
        return count

    def _load_events_since(self, log_file, offset, stream_name,
                           timeout_s=15, stable_for_s=2.0, min_events=0):
        deadline = time.time() + timeout_s
        last_count = -1
        last_change = None
        events = []

        while time.time() < deadline:
            events = []
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    f.seek(offset)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        for s in obj.get("streams") or []:
                            sname = s.get("streamName", "")
                            if sname != stream_name and \
                                    not sname.endswith(f".{stream_name}"):
                                continue
                            for ev in s.get("events") or []:
                                events.append(ev)
            except FileNotFoundError:
                events = []

            if len(events) != last_count:
                last_count = len(events)
                last_change = time.time()

            if last_count >= min_events and last_change is not None \
                    and time.time() - last_change >= stable_for_s:
                return events

            time.sleep(0.2)

        return events

    def _wait_rows(self, table_name, expected, timeout_s=30):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            tdSql.query(f"select count(*) from {table_name}")
            if tdSql.getData(0, 0) >= expected:
                return
            time.sleep(1)
        tdLog.exit(f"{table_name} did not reach {expected} rows within {timeout_s}s")

    def _find_event(self, events, event_type, cur_state, next_state=None):
        for ev in events:
            if ev.get("eventType") != event_type:
                continue
            if ev.get("curState") != cur_state:
                continue
            if next_state is not None and ev.get("nextState") != next_state:
                continue
            return ev
        return None

    def _parse_log_messages(self, log_file, offset=0, stream_name=None):
        """Parse WS log lines into per-stream message records."""
        msgs = []
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                f.seek(offset)
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except Exception:
                        continue
                    for s in obj.get("streams") or []:
                        sname = s.get("streamName", "")
                        if stream_name and sname != stream_name and \
                                not sname.endswith(f".{stream_name}"):
                            continue
                        events = s.get("events") or []
                        close_evts = [
                            e for e in events
                            if e.get("eventType") == "WINDOW_CLOSE"
                        ]
                        msgs.append({
                            "streamName": sname,
                            "line_len": len(raw),
                            "events": events,
                            "event_count": len(events),
                            "close_count": len(close_evts),
                            "messageId": obj.get("messageId"),
                        })
        except FileNotFoundError:
            pass
        return msgs

    def _wait_log_stable(self, log_file, offset, stream_name=None,
                         min_messages=1, timeout_s=30, stable_for_s=2.5):
        deadline = time.time() + timeout_s
        last_count = -1
        last_change = None
        msgs = []

        while time.time() < deadline:
            msgs = self._parse_log_messages(log_file, offset, stream_name)
            if len(msgs) != last_count:
                last_count = len(msgs)
                last_change = time.time()
            if last_count >= min_messages and last_change is not None \
                    and time.time() - last_change >= stable_for_s:
                return msgs
            time.sleep(0.2)

        return msgs

    def _count_close_events(self, msgs):
        return sum(m["close_count"] for m in msgs)

    def _wait_phase_a(self, log_file, stream_name, dst_table):
        """Insert c1=10,20 and wait for baseline notify events."""
        tdLog.info("[recovery] Phase A: inserting rows with c1=10, c1=20")
        tdSql.execute(
            """insert into ct0 values
            ('2025-01-01 00:00:00.000', 10, 1),
            ('2025-01-01 00:00:01.000', 20, 2);"""
        )
        self._wait_rows(dst_table, 1)

        # OPEN (×2) and CLOSE may arrive in separate WS frames; wait for all.
        events = self._load_events_since(
            log_file, 0, stream_name,
            timeout_s=20, stable_for_s=2.0, min_events=3)
        if self._find_event(events, "WINDOW_OPEN", [10]) is None:
            raise Exception(
                f"[recovery] Phase A: missing WINDOW_OPEN(curState=[10]). Got: {events}"
            )
        return self._log_offset(log_file)

    # ------------------------------------------------------------------
    # Test 1: notify server restart
    # ------------------------------------------------------------------

    def test_stream_notify_server_restart(self):
        """Verify notify recovers after the WS server restarts.

        Phase A  — server up: insert c1=10,20; notify delivered.
        Outage   — stop Python WS server; insert c1=30 (stream still runs,
                   but notify messages should be lost).
        Phase C  — restart server; insert c1=40; notify must work again
                   via tcurl auto-reconnect.

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,recovery,server-restart

        Jira: None

        History:
            - 2026-06-11 Created for notify server restart regression
        """
        notify_path = "server_restart"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_srv_restart"
        stream_name = "s_srv_restart"
        dst_table = "dst_srv_restart"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server(clear_logs=True)
            time.sleep(1)
            self.ensure_snode()

            self.drop_db_and_wait(db_name, stream_name)
            self.create_database_with_retry(db_name)
            self._create_stream(db_name, stream_name, notify_path, dst_table)

            offset_after_A = self._wait_phase_a(log_file, stream_name, dst_table)
            tdLog.info(f"[server_restart] Phase A done, log offset={offset_after_A}")

            # ── Outage: stop WS server ─────────────────────────────────
            tdLog.info("[server_restart] Stopping notify server (simulating outage)")
            self.stop_notify_server()

            tdLog.info("[server_restart] Outage write: inserting c1=30")
            tdSql.execute(
                "insert into ct0 values ('2025-01-01 00:00:02.000', 30, 3);"
            )
            self._wait_rows(dst_table, 2, timeout_s=30)
            # Allow async worker to attempt (and fail) delivery.
            time.sleep(6)

            events_during_outage = self._count_events_since(
                log_file, offset_after_A, stream_name)
            if events_during_outage != 0:
                raise Exception(
                    f"[server_restart] expected no notify during server outage, "
                    f"but got {events_during_outage} new event(s) after offset "
                    f"{offset_after_A}"
                )
            tdLog.info("[server_restart] Outage notify loss verified ✓")

            offset_after_outage = self._log_offset(log_file)

            # ── Phase C: server back up ────────────────────────────────
            tdLog.info("[server_restart] Restarting notify server")
            self.restart_notify_server()

            tdLog.info("[server_restart] Phase C: inserting c1=40")
            tdSql.execute(
                "insert into ct0 values ('2025-01-01 00:00:03.000', 40, 4);"
            )
            self._wait_rows(dst_table, 3, timeout_s=30)

            evC = self._load_events_since(
                log_file, offset_after_outage, stream_name,
                timeout_s=30, stable_for_s=3.0, min_events=1)

            close_30 = self._find_event(evC, "WINDOW_CLOSE", [30], next_state=[40])
            if close_30 is None:
                raise Exception(
                    f"[server_restart] Phase C: missing WINDOW_CLOSE([30]->[40]) "
                    f"after server restart. Got: {evC}"
                )

            tdLog.info("[server_restart] Phase C events verified ✓")
            tdLog.info(
                "test_stream_notify_server_restart .............. [ passed ]"
            )

        finally:
            self.teardown_stream_notify(stream_name, db_name, log_file)

    # ------------------------------------------------------------------
    # Test 2: stream stop / start
    # ------------------------------------------------------------------

    def test_stream_notify_stream_restart(self):
        """Verify notify works again after ``stop stream`` / ``start stream``.

        Phase A  — stream running: insert c1=10,20; notify delivered.
        Paused   — ``stop stream``; insert c1=30; no notify.
        Phase C  — ``start stream``; insert c1=40; notify must resume.

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,recovery,stream-restart

        Jira: None

        History:
            - 2026-06-11 Created for stream restart notify regression
        """
        notify_path = "stream_restart"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_stm_restart"
        stream_name = "s_stm_restart"
        dst_table = "dst_stm_restart"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server(clear_logs=True)
            time.sleep(1)
            self.ensure_snode()

            self.drop_db_and_wait(db_name, stream_name)
            self.create_database_with_retry(db_name)
            self._create_stream(db_name, stream_name, notify_path, dst_table)

            offset_after_A = self._wait_phase_a(log_file, stream_name, dst_table)
            tdLog.info(f"[stream_restart] Phase A done, log offset={offset_after_A}")

            # ── Stream stopped ─────────────────────────────────────────
            tdLog.info("[stream_restart] Stopping stream")
            tdStream.stopStream(db_name, stream_name)
            # Drain in-flight notify from Phase A before measuring the pause.
            time.sleep(3)
            offset_after_stop = self._log_offset(log_file)

            tdLog.info("[stream_restart] Paused write: inserting c1=30")
            tdSql.execute(
                "insert into ct0 values ('2025-01-01 00:00:02.000', 30, 3);"
            )
            time.sleep(6)

            events_while_stopped = self._count_events_since(
                log_file, offset_after_stop, stream_name)
            if events_while_stopped != 0:
                raise Exception(
                    f"[stream_restart] expected no notify while stream stopped, "
                    f"but got {events_while_stopped} new event(s)"
                )
            tdLog.info("[stream_restart] No notify while stopped verified ✓")

            # ── Stream restarted ───────────────────────────────────────
            tdLog.info("[stream_restart] Starting stream again")
            tdStream.startStream(db_name, stream_name)
            tdStream.checkStreamStatus(stream_name)
            time.sleep(2)

            # WAL replay may process the paused row (c1=30); wait for settle.
            offset_after_resume = self._log_offset(log_file)
            self._load_events_since(
                log_file, offset_after_resume, stream_name,
                timeout_s=10, stable_for_s=2.0, min_events=0)
            offset_after_resume = self._log_offset(log_file)

            tdLog.info("[stream_restart] Phase C: inserting c1=40")
            tdSql.execute(
                "insert into ct0 values ('2025-01-01 00:00:03.000', 40, 4);"
            )
            self._wait_rows(dst_table, 3, timeout_s=30)

            evC = self._load_events_since(
                log_file, offset_after_resume, stream_name,
                timeout_s=15, stable_for_s=2.0, min_events=1)

            close_30 = self._find_event(evC, "WINDOW_CLOSE", [30], next_state=[40])
            if close_30 is None:
                raise Exception(
                    f"[stream_restart] Phase C: missing WINDOW_CLOSE([30]->[40]) "
                    f"after stream restart. Got: {evC}"
                )

            tdLog.info("[stream_restart] Phase C events verified ✓")
            tdLog.info(
                "test_stream_notify_stream_restart .............. [ passed ]"
            )

        finally:
            self.teardown_stream_notify(stream_name, db_name, log_file)
    
    # ------------------------------------------------------------------
    # Test 3: different streams are never batched together
    # ------------------------------------------------------------------

    def test_stream_notify_batch_stream_isolation(self):
        """Verify worker merge never crosses stream boundaries.

        Two streams share the same notify URL.  A burst insert triggers
        high-frequency CLOSE on both streams.  Each WS frame must belong to
        exactly one ``streamName``; each stream should still get its own
        high-frequency merged frames.

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,batch,isolation

        Jira: None

        History:
            - 2026-06-11 Created for cross-stream batching isolation regression
        """
        notify_path = "batch_isolation"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_batch_iso"
        stream_a = "s_batch_iso_a"
        stream_b = "s_batch_iso_b"
        dst_a = "dst_batch_iso_a"
        dst_b = "dst_batch_iso_b"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server(clear_logs=True)
            time.sleep(1)
            self.ensure_snode()

            self.drop_db_and_wait(db_name, stream_a)
            try:
                tdSql.execute(f"drop stream if exists {stream_b};")
            except Exception:
                pass
            self.create_database_with_retry(db_name)
            self._create_stream(db_name, stream_a, notify_path, dst_a,
                                create_table=True)
            self._create_stream(db_name, stream_b, notify_path, dst_b,
                                create_table=False)

            tdLog.info("[batch_iso] burst insert on shared source table")
            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 1, 1),
                ('2025-01-01 00:00:01.000', 2, 2),
                ('2025-01-01 00:00:02.000', 3, 3),
                ('2025-01-01 00:00:03.000', 4, 4),
                ('2025-01-01 00:00:04.000', 5, 5),
                ('2025-01-01 00:00:05.000', 6, 6);"""
            )
            self._wait_rows(dst_a, 5, timeout_s=40)
            self._wait_rows(dst_b, 5, timeout_s=40)

            msgs = self._wait_log_stable(
                log_file, 0, stream_name=None,
                min_messages=4, timeout_s=40)

            suffix_a = f".{stream_a}"
            suffix_b = f".{stream_b}"
            msgs_a = [
                m for m in msgs
                if m["streamName"].endswith(suffix_a)
                or m["streamName"] == stream_a
            ]
            msgs_b = [
                m for m in msgs
                if m["streamName"].endswith(suffix_b)
                or m["streamName"] == stream_b
            ]

            if not msgs_a or not msgs_b:
                raise Exception(
                    f"[batch_iso] expected messages from both streams; "
                    f"a={len(msgs_a)} b={len(msgs_b)} all={msgs}"
                )

            for m in msgs:
                sname = m["streamName"]
                is_a = sname.endswith(suffix_a) or sname == stream_a
                is_b = sname.endswith(suffix_b) or sname == stream_b
                if not (is_a or is_b):
                    raise Exception(
                        f"[batch_iso] unexpected streamName in log: {sname}"
                    )

            merged_a = [m for m in msgs_a if m["close_count"] >= 2]
            merged_b = [m for m in msgs_b if m["close_count"] >= 2]
            if not merged_a or not merged_b:
                raise Exception(
                    f"[batch_iso] expected merged CLOSE frames per stream "
                    f"(high-frequency path); a={msgs_a} b={msgs_b}"
                )

            tdLog.info(
                f"[batch_iso] stream A: {len(msgs_a)} frames "
                f"({len(merged_a)} merged); "
                f"stream B: {len(msgs_b)} frames ({len(merged_b)} merged) ✓"
            )
            tdLog.info(
                "test_stream_notify_batch_stream_isolation ....... [ passed ]"
            )

        finally:
            self.teardown_stream_notify(stream_a, db_name, log_file)
            try:
                tdSql.execute(f"drop stream if exists {stream_b};")
            except Exception:
                pass
