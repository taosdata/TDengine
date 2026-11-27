# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""WebSocket long-connection keepalive test for stream notify.

Background
----------
TDengine's stream notify uses libcurl WebSocket in CONNECT_ONLY mode
(tcurl.c).  The connection lifetime is governed by:

  * TCP keepalive  — CURLOPT_TCP_KEEPALIVE=1, TCP_KEEPIDLE=120s,
                     TCP_KEEPINTVL=60s  (OS-level probes)
  * WS ping-pong — tcurlEnsureAlive() drains inbound frames, replies server
    PING with PONG, and issues a client PING/PONG probe after 30 s idle.
  * Auto-reconnect — send/keepalive failures drop the cached handle and retry
    once with a fresh connection.

This test verifies that after IDLE_SECS (≥ 120 s) of complete inactivity,
the notify channel is still functional: a second insert batch triggers events
that the Python WS server receives correctly.
"""

import json
import os
import time

from new_test_framework.utils import tdLog, tdSql, tdStream, eutil
from stream_notify_server import (
    start_notify_server_background,
    stop_notify_server_background,
    wait_notify_server_ready,
)

CALLER_FILE = os.path.realpath(__file__)
CALLER_DIR = os.path.dirname(CALLER_FILE)
NOTIFY_RESULT_DIR = os.path.join(CALLER_DIR, "notify_result_tmp")
NOTIFY_PORT = 12345

# How long to keep the connection idle between the two write phases.
# Must be ≥ TCP_KEEPIDLE (120 s) to exercise the TCP-keepalive path.
IDLE_SECS = 120

# tcurl.c TCURL_WS_PING_IDLE_MS is 30 s — wait longer so the next send probes
# the dead socket with client PING/PONG instead of silently reusing it.
PING_IDLE_WAIT_SECS = 35


class TestStreamNotifyKeepalive:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), True)

    # ------------------------------------------------------------------
    # Server helpers
    # ------------------------------------------------------------------

    def start_notify_server(self):
        if os.path.isdir(NOTIFY_RESULT_DIR):
            for entry in os.listdir(NOTIFY_RESULT_DIR):
                path = os.path.join(NOTIFY_RESULT_DIR, entry)
                if os.path.isfile(path):
                    os.remove(path)
        else:
            os.makedirs(NOTIFY_RESULT_DIR, exist_ok=True)
        start_notify_server_background(port=NOTIFY_PORT, log_path=NOTIFY_RESULT_DIR)

    def stop_notify_server(self):
        stop_notify_server_background()

    def ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    # ------------------------------------------------------------------
    # DB lifecycle helpers (safe against taosd async VNode cleanup race)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Event reading helpers
    # ------------------------------------------------------------------

    def _log_offset(self, log_file):
        """Return current byte size of log_file (0 if it does not exist yet)."""
        try:
            return os.path.getsize(log_file)
        except FileNotFoundError:
            return 0

    def _load_events_since(self, log_file, offset, stream_name,
                           timeout_s=15, stable_for_s=2.0):
        """Return a list of raw event dicts written after *offset* bytes.

        Polling strategy mirrors load_events(): wait until the event count has
        been stable for *stable_for_s* seconds OR the hard deadline is reached.
        This is necessary because the 1-second batch interval means events may
        arrive in multiple WebSocket frames.
        """
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

            if last_count > 0 and last_change is not None \
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
        """Return the first event whose eventType and curState (and optional nextState) match."""
        for ev in events:
            if ev.get("eventType") != event_type:
                continue
            if ev.get("curState") != cur_state:
                continue
            if next_state is not None and ev.get("nextState") != next_state:
                continue
            return ev
        return None

    def _check_taosd_log(self, key, retry=30):
        """Return True when *key* appears in taosdlog.0."""
        cnt = eutil.findTaosdLog(key, retry=retry)
        if cnt <= 0:
            tdLog.exit(f"taosd log check failed, key={key} not found.")
        tdLog.info(f"taosd log check ok, key={key}, count={cnt}")
        return cnt

    def _restart_notify_server(self):
        self.start_notify_server()
        if not wait_notify_server_ready(NOTIFY_PORT, timeout=10.0):
            raise RuntimeError(
                f"notify server on port {NOTIFY_PORT} not ready after restart"
            )
        time.sleep(1)

    # ------------------------------------------------------------------
    # Test case
    # ------------------------------------------------------------------

    def test_stream_notify_ws_keepalive(self):
        """Verify WebSocket long-connection keepalive across 2-minute idle.

        The test has two write phases separated by IDLE_SECS (125 s) of
        complete inactivity:

          Phase A  (t ≈ 0 s)
            Insert rows with c1=10, then c1=20.
            Expect WINDOW_OPEN(curState=[10]) and
                   WINDOW_CLOSE(curState=[10], nextState=[20]).

          Idle gap  (t ≈ 5 s … 125 s)
            No writes.  The WebSocket connection is held open by:
              - TCP keepalive probes (TCP_KEEPIDLE=120 s)
              - tcurlSend() auto-reconnect on first send failure
            The server uses ping_timeout=None so it never closes for
            unanswered WS pings.

          Phase B  (t ≈ 125 s)
            Insert a row with c1=30.
            Expect WINDOW_CLOSE(curState=[20], nextState=[30]) and
                   WINDOW_OPEN(curState=[30]).

        The test passes if Phase B events are correctly delivered,
        proving the notify channel is alive after the idle period.

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,keepalive,long-connection

        Jira: None

        History:
            - 2026-06-08 Auto-generated to cover WS keepalive regression
        """
        notify_path = "keepalive"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_keepalive"
        stream_name = "s_keepalive"
        dst_table = "dst_keepalive"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            self.drop_db_and_wait(db_name, stream_name)
            self.create_database_with_retry(db_name)
            tdSql.execute(f"use {db_name};")
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

            # ── Phase A: initial writes ────────────────────────────────
            tdLog.info("[keepalive] Phase A: inserting rows with c1=10, c1=20")
            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 10, 1),
                ('2025-01-01 00:00:01.000', 20, 2);"""
            )

            # Wait for at least 1 destination row (state-window close for c1=10)
            self._wait_rows(dst_table, 1)

            # Collect Phase A events and record the log-file offset
            offset_after_A = 0
            deadline_A = time.time() + 15
            last_count_A = -1
            last_change_A = None
            while time.time() < deadline_A:
                count = 0
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                obj = json.loads(line)
                                for s in obj.get("streams") or []:
                                    sname = s.get("streamName", "")
                                    if sname == stream_name or \
                                            sname.endswith(f".{stream_name}"):
                                        count += len(s.get("events") or [])
                            except Exception:
                                pass
                except FileNotFoundError:
                    pass
                if count != last_count_A:
                    last_count_A = count
                    last_change_A = time.time()
                if last_count_A > 0 and last_change_A is not None \
                        and time.time() - last_change_A >= 2.0:
                    break
                time.sleep(0.2)

            offset_after_A = self._log_offset(log_file)
            tdLog.info(
                f"[keepalive] Phase A done: {last_count_A} events, "
                f"log offset={offset_after_A}"
            )

            if last_count_A == 0:
                raise Exception(
                    "[keepalive] Phase A: no events received before idle gap"
                )

            # Verify Phase A has the expected WINDOW_OPEN
            evA = []
            try:
                with open(log_file, "r", encoding="utf-8") as f:
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
                            if sname == stream_name or \
                                    sname.endswith(f".{stream_name}"):
                                evA.extend(s.get("events") or [])
            except FileNotFoundError:
                pass

            open_10 = self._find_event(evA, "WINDOW_OPEN", [10])
            if open_10 is None:
                raise Exception(
                    f"[keepalive] Phase A: missing WINDOW_OPEN(curState=[10]). "
                    f"Got: {evA}"
                )
            tdLog.info("[keepalive] Phase A events verified ✓")

            # ── Idle gap ───────────────────────────────────────────────
            tdLog.info(
                f"[keepalive] Entering idle gap ({IDLE_SECS} s). "
                "WS connection should stay alive via TCP keepalive / auto-reconnect."
            )
            time.sleep(IDLE_SECS)
            tdLog.info("[keepalive] Idle gap over — proceeding to Phase B")

            # ── Phase B: writes after idle ─────────────────────────────
            tdLog.info("[keepalive] Phase B: inserting row with c1=30")
            tdSql.execute(
                "insert into ct0 values ('2025-01-01 00:01:00.000', 30, 3);"
            )

            # Phase B should trigger WINDOW_CLOSE for c1=20 → c1=30
            self._wait_rows(dst_table, 2, timeout_s=30)

            evB = self._load_events_since(
                log_file, offset_after_A, stream_name,
                timeout_s=15, stable_for_s=2.0
            )

            if len(evB) == 0:
                raise Exception(
                    "[keepalive] Phase B: no events received after idle gap — "
                    "WebSocket connection may have died and auto-reconnect failed."
                )

            close_20 = self._find_event(
                evB, "WINDOW_CLOSE", [20], next_state=[30])
            if close_20 is None:
                raise Exception(
                    f"[keepalive] Phase B: missing WINDOW_CLOSE(curState=[20], "
                    f"nextState=[30]) — WebSocket may have died after idle. "
                    f"Got: {evB}"
                )

            tdLog.info(
                f"[keepalive] Phase B events verified ✓ "
                f"({len(evB)} events received after {IDLE_SECS}s idle)"
            )
            tdLog.info(
                "test_stream_notify_ws_keepalive .............. [ passed ]"
            )

        finally:
            self.stop_notify_server()
            self.drop_db_and_wait(db_name, stream_name)
            if os.path.exists(log_file):
                os.remove(log_file)

    # def test_stream_notify_ws_dead_detected_by_pingpong(self):
    #     """Dead WS link must be caught by ping-pong and logged, not false-OK.

    #     Scenario
    #     ----------
    #     1. Phase A — establish the cached long connection and deliver notify.
    #     2. Stop the WS server so the client-side handle goes stale.
    #     3. Wait ≥ TCURL_WS_PING_IDLE_MS (30 s) so the next send runs
    #        tcurlEnsureAlive() client PING/PONG instead of only recv-drain.
    #     4. Phase B — insert while the server is still down.
    #        Expect taosd logs:
    #          * ``[notify_worker] keepalive check failed for <url>``
    #          * and a tcurl keepalive failure such as
    #            ``ws close frame received from <url>`` (server shutdown)
    #            or ``ws pong timeout on <url>`` (ping probe on stale link)
    #        Expect **no** new notify payload on the WS server (not false-OK).
    #     5. Phase C — restart server, insert again, notify must recover.

    #     Catalog:
    #         - Stream

    #     Since: v3.4.2.0

    #     Labels: common,ci,stream,notify,keepalive,ping-pong

    #     Jira: None

    #     History:
    #         - 2026-06-12 Added to verify dead-link detection via ping-pong
    #     """
    #     notify_path = "keepalive_dead"
    #     notify_url = f"ws://localhost:{NOTIFY_PORT}/{notify_path}"
    #     log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
    #     db_name = "test_sn_keepalive_dead"
    #     stream_name = "s_keepalive_dead"
    #     dst_table = "dst_keepalive_dead"

    #     worker_log_key = f"keepalive check failed for {notify_url}"
    #     # tcurlEnsureAlive may fail in drain (CLOSE frame) or ping/pong probe.
    #     # NOTE: do not prefix with "[curl]" — bare grep treats [] as a char class.
    #     curl_log_keys = [
    #         f"ws close frame received from {notify_url}",
    #         f"ws pong timeout on {notify_url}",
    #         f"ws ping to {notify_url} failed",
    #         f"ws connection closed while draining recv from {notify_url}",
    #         f"ws connection closed while waiting pong from {notify_url}",
    #         f"ws recv drain failed on {notify_url}",
    #     ]

    #     if os.path.exists(log_file):
    #         os.remove(log_file)

    #     try:
    #         self.start_notify_server()
    #         time.sleep(1)
    #         self.ensure_snode()

    #         self.drop_db_and_wait(db_name, stream_name)
    #         self.create_database_with_retry(db_name)
    #         tdSql.execute(f"use {db_name};")
    #         tdSql.execute("create table ct0(ts timestamp, c1 int, v int);")
    #         tdSql.execute(
    #             f"""create stream {stream_name} state_window(c1) from ct0
    #             stream_options(event_type(WINDOW_CLOSE))
    #             notify('{notify_url}')
    #               on(window_open|window_close)
    #             into {dst_table} as
    #             select _twstart, _twend, count(*) cnt, first(v) first_v
    #             from %%trows;"""
    #         )
    #         tdStream.checkStreamStatus(stream_name)

    #         # ── Phase A: warm up the persistent connection ─────────────
    #         tdLog.info("[dead_ping] Phase A: inserting c1=10, c1=20")
    #         tdSql.execute(
    #             """insert into ct0 values
    #             ('2025-01-01 00:00:00.000', 10, 1),
    #             ('2025-01-01 00:00:01.000', 20, 2);"""
    #         )
    #         self._wait_rows(dst_table, 1)

    #         evA = self._load_events_since(
    #             log_file, 0, stream_name, timeout_s=15, stable_for_s=2.0
    #         )
    #         if len(evA) == 0:
    #             raise Exception("[dead_ping] Phase A: no notify events received")
    #         offset_after_A = self._log_offset(log_file)
    #         tdLog.info(f"[dead_ping] Phase A ok, notify events={len(evA)}")

    #         # ── Kill server + wait past ping-idle threshold ────────────
    #         tdLog.info("[dead_ping] Stopping WS server to stale the cached link")
    #         self.stop_notify_server()
    #         tdLog.info(
    #             f"[dead_ping] Waiting {PING_IDLE_WAIT_SECS}s for ping-idle probe"
    #         )
    #         time.sleep(PING_IDLE_WAIT_SECS)

    #         # ── Phase B: trigger notify on a dead link ─────────────────
    #         tdLog.info("[dead_ping] Phase B: insert c1=30 while server is down")
    #         tdSql.execute(
    #             "insert into ct0 values ('2025-01-01 00:01:00.000', 30, 3);"
    #         )
    #         # dst row is written by stream calc even when notify fails
    #         self._wait_rows(dst_table, 2, timeout_s=30)
    #         time.sleep(3)  # allow async worker flush + taosd log flush

    #         evB = self._load_events_since(
    #             log_file, offset_after_A, stream_name,
    #             timeout_s=5, stable_for_s=1.0
    #         )
    #         if len(evB) > 0:
    #             raise Exception(
    #                 f"[dead_ping] Phase B: notify reached server on dead link "
    #                 f"(false-OK). events={evB}"
    #             )
    #         tdLog.info("[dead_ping] Phase B: no WS payload delivered (expected)")

    #         self._check_taosd_log(worker_log_key, retry=30)

    #         matched_curl_key = None
    #         for key in curl_log_keys:
    #             if eutil.findTaosdLog(key, retry=10) > 0:
    #                 matched_curl_key = key
    #                 break
    #         if matched_curl_key is None:
    #             tdLog.exit(
    #                 "[dead_ping] no tcurl keepalive failure log found; tried: "
    #                 + ", ".join(curl_log_keys)
    #             )
    #         tdLog.info(
    #             f"[dead_ping] tcurl keepalive failure logged: {matched_curl_key}"
    #         )

    #         # ── Phase C: recovery after server restart ─────────────────
    #         tdLog.info("[dead_ping] Restarting WS server for recovery")
    #         self._restart_notify_server()
    #         offset_before_C = self._log_offset(log_file)

    #         tdLog.info("[dead_ping] Phase C: insert c1=40 after server restart")
    #         tdSql.execute(
    #             "insert into ct0 values ('2025-01-01 00:02:00.000', 40, 4);"
    #         )
    #         self._wait_rows(dst_table, 3, timeout_s=30)

    #         evC = self._load_events_since(
    #             log_file, offset_before_C, stream_name,
    #             timeout_s=20, stable_for_s=2.0
    #         )
    #         if len(evC) == 0:
    #             raise Exception(
    #                 "[dead_ping] Phase C: no notify after server restart"
    #             )
    #         tdLog.info(
    #             f"[dead_ping] Phase C ok, recovered events={len(evC)}"
    #         )
    #         tdLog.info(
    #             "test_stream_notify_ws_dead_detected_by_pingpong ... [ passed ]"
    #         )

    #     finally:
    #         self.stop_notify_server()
    #         self.drop_db_and_wait(db_name, stream_name)
    #         if os.path.exists(log_file):
    #             os.remove(log_file)
