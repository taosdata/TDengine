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

"""Consuming already-persisted backlog and live WAL data at the same time.

Real consumers frequently subscribe to a topic that already has a large
backlog of committed transactions (from before the subscription existed),
and then must keep up as new transactions commit WHILE the backlog is
still being drained. This file verifies that boundary:

  mA1: TMQ meta consumer — commit N historical txns before subscribing,
       then commit MORE txns concurrently while the consumer is still
       draining the backlog. All objects (historical + live) must appear
       exactly once, none lost, none duplicated.
  mB1: SQL STREAM — FILL_HISTORY covers data written by historical batch
       txns; verify the stream also correctly picks up "live" data written
       by a batch txn AFTER the stream was created, i.e. the aggregation
       is correct across both the historical and the live portion.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, tdStream
import ctypes
import json
import threading
import time


DB = "txn_mixed_db"
TOPIC = "txn_mixed_topic"
STREAM = "txn_mixed_stream"
STREAM_RESULT = "txn_mixed_stream_result"


def _reset_db():
    tdSql.execute(f"drop database if exists {DB}")
    tdSql.execute(f"create database {DB} vgroups 1 keep 36500")
    tdSql.execute(f"use {DB}")


def _get_json_meta_str(msg):
    """Fetch the JSON meta description of a TMQ meta message via the C API's
    tmq_get_json_meta(). Returns None for data messages (or on any failure)."""
    try:
        from taos.cinterface import _libtaos
    except ImportError:
        return None
    try:
        _libtaos.tmq_get_json_meta.restype = ctypes.c_void_p
        _libtaos.tmq_get_json_meta.argtypes = [ctypes.c_void_p]
        _libtaos.tmq_free_json_meta.argtypes = [ctypes.c_void_p]
        ptr = _libtaos.tmq_get_json_meta(msg.msg)
        if not ptr:
            return None
        try:
            return ctypes.cast(ptr, ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            _libtaos.tmq_free_json_meta(ptr)
    except Exception:
        return None


def _open_consumer(group_id):
    from taos.tmq import Consumer
    c = Consumer({
        "group.id":               group_id,
        "client.id":              group_id,
        "td.connect.user":        "root",
        "td.connect.pass":        "taosdata",
        "td.connect.ip":          "localhost",
        "td.connect.port":        "6030",
        "enable.auto.commit":     "true",
        "auto.commit.interval.ms":"200",
        "auto.offset.reset":      "earliest",
        "fetch.max.wait.ms":      "500",
    })
    c.subscribe([TOPIC])
    return c


def _close_consumer(c):
    for fn in (c.unsubscribe, c.close):
        try:
            fn()
        except Exception:
            pass


def _drain_while_racing(consumer, stop_event, idle_grace_s=3, hard_timeout_s=40):
    """Drain a consumer while a background writer thread is (maybe) still
    committing new transactions. Keeps polling until `stop_event` is set
    AND no new message has arrived for `idle_grace_s` seconds, so a
    momentarily-empty poll during the race doesn't cut the drain short.
    """
    collected = []
    last_activity = time.time()
    deadline = time.time() + hard_timeout_s
    while time.time() < deadline:
        msg = consumer.poll(1)
        if msg is None:
            if stop_event.is_set() and (time.time() - last_activity) > idle_grace_s:
                break
            continue
        if msg.error() is not None:
            continue
        last_activity = time.time()

        meta_text = _get_json_meta_str(msg)
        if meta_text is not None:
            entry = {"repr": meta_text, "json": None}
            try:
                entry["json"] = json.loads(meta_text)
            except Exception:
                pass
            collected.append(entry)
            continue

        try:
            for block in msg:
                entry = {"repr": repr(block), "json": None}
                collected.append(entry)
        except Exception:
            collected.append({"repr": repr(msg), "json": None})
    return collected


def _names_in(entries):
    names = set()
    for e in entries:
        text = e["repr"] + (json.dumps(e["json"]) if e["json"] else "")
        for word in text.replace('"', ' ').replace("'", ' ').split():
            if len(word) >= 3:
                names.add(word.lower())
    return names


class TestTxnConsumerMixedSnapshotWal:
    """Mixed backlog (snapshot/pre-existing) + live WAL consumption."""

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdStream.ensureSnode(1)

    def mA1_backlog_catchup_races_live_commits(self):
        """mA1: N historical txns committed before subscribe, then M more txns
        committed concurrently while the consumer drains the backlog. All
        objects (historical + live) must be delivered exactly once."""
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute("create table stb (ts timestamp, v int) tags (grp int)")

        N_HIST = 5
        for i in range(N_HIST):
            tdSql.execute("BEGIN")
            tdSql.execute(f"create table ct_hist_{i} using stb tags({i})")
            tdSql.execute("COMMIT")
        tdLog.info(f"mA1: {N_HIST} historical txns committed before subscribe")

        tdSql.execute(f"drop topic if exists {TOPIC}")
        tdSql.execute(f"create topic {TOPIC} with meta as database {DB}")

        consumer = _open_consumer("grp_mA1")

        N_LIVE = 5
        live_names = [f"ct_live_{i}" for i in range(N_LIVE)]
        stop_event = threading.Event()
        errors = []

        def _commit_live():
            try:
                s2 = tdCom.newTdSql()
                s2.execute(f"use {DB}")
                for name in live_names:
                    s2.execute("BEGIN")
                    s2.execute(f"create table {name} using {DB}.stb tags(99)")
                    s2.execute("COMMIT")
                    time.sleep(0.2)  # spread commits out to overlap with backlog drain
                s2.close()
            except Exception as e:
                errors.append(e)
            finally:
                stop_event.set()

        t = threading.Thread(target=_commit_live)
        t.start()
        entries = _drain_while_racing(consumer, stop_event)
        t.join(timeout=30)
        _close_consumer(consumer)

        assert not errors, f"mA1: background commit thread failed: {errors}"

        names_seen = _names_in(entries)
        missing = [n for n in [f"ct_hist_{i}" for i in range(N_HIST)] + live_names
                   if n not in names_seen]
        assert not missing, f"mA1: consumer missed objects: {missing}"

        tdSql.query(f"show {DB}.tables")
        assert tdSql.queryRows == N_HIST + N_LIVE, \
            f"mA1: source has {tdSql.queryRows} tables, expected {N_HIST + N_LIVE}"

        tdLog.info("mA1 PASSED — backlog + concurrent live commits all delivered")

        tdSql.execute(f"drop topic if exists {TOPIC}")

    def mB1_stream_fill_history_plus_live_batch(self):
        """mB1: FILL_HISTORY covers data from a historical batch txn; a SECOND
        batch txn commits "live" data (with a later timestamp) after the
        stream is created. The stream must aggregate BOTH portions correctly."""
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute("create table stb_mix (ts timestamp, score int) tags (region varchar(32))")

        # Historical batch: committed BEFORE the stream exists.
        t0 = "2025-01-01T00:00:00"
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_mix_hist using stb_mix tags('hist')")
        tdSql.execute("COMMIT")
        tdSql.execute(f"insert into ct_mix_hist values ('{t0}', 10)")

        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")
        tdSql.execute(
            f"CREATE STREAM {STREAM} INTERVAL(1s) SLIDING(1s) "
            f"FROM {DB}.stb_mix "
            f"PARTITION BY tbname "
            f"STREAM_OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) "
            f"INTO {STREAM_RESULT} AS "
            f"SELECT _twstart, %%tbname AS src_tbname, first(score) AS first_score "
            f"FROM %%trows"
        )
        tdLog.info("mB1: stream created with FILL_HISTORY covering ct_mix_hist")

        # Live batch: committed AFTER the stream exists, later timestamp.
        t1 = "2025-01-01T00:00:05"
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_mix_live using stb_mix tags('live')")
        tdSql.execute("COMMIT")
        tdSql.execute(f"insert into ct_mix_live values ('{t1}', 20)")
        tdLog.info("mB1: live batch txn committed after stream creation")

        rows_ready = False
        for _ in range(20):
            time.sleep(1)
            try:
                tdSql.query(
                    f"SELECT src_tbname, first_score FROM {DB}.{STREAM_RESULT} ORDER BY src_tbname"
                )
                if tdSql.queryRows >= 2:
                    rows_ready = True
                    break
            except Exception:
                continue

        assert rows_ready, (
            f"mB1: stream result has {tdSql.queryRows} rows after 20s; expected >= 2 "
            f"(one from historical batch, one from live batch)"
        )

        result = {}
        for i in range(tdSql.queryRows):
            result[tdSql.queryResult[i][0]] = tdSql.queryResult[i][1]

        assert "ct_mix_hist" in result and result["ct_mix_hist"] == 10, \
            f"mB1: ct_mix_hist first_score={result.get('ct_mix_hist')} expected 10 (historical/FILL_HISTORY portion)"
        assert "ct_mix_live" in result and result["ct_mix_live"] == 20, \
            f"mB1: ct_mix_live first_score={result.get('ct_mix_live')} expected 20 (live portion)"

        tdLog.info("mB1 PASSED — stream aggregates historical (FILL_HISTORY) + live batch correctly")

        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")

    def test_txn_consumer_mixed_snapshot_wal(self):
        """Mixed backlog/snapshot + live WAL consumption (mA1, mB1)

        Verifies TMQ meta consumption and SQL Stream computation both
        correctly handle the boundary where a consumer must catch up on an
        already-committed backlog WHILE new transactions keep committing
        live — no lost or duplicated objects, and stream aggregation spans
        both the historical (FILL_HISTORY) and live portions correctly.

        1. backlog_catchup_races_live_commits
        2. stream_fill_history_plus_live_batch

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        """
        self.mA1_backlog_catchup_races_live_commits()
        self.mB1_stream_fill_history_plus_live_batch()
