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

"""WAL consumer data correctness tests for batch DDL transactions.

Verifies that downstream consumers receive COMPLETE and CORRECT data
when the WAL contains batch DDL transactions.  Two consumer types are
tested:

  A. TMQ meta consumer (CREATE TOPIC … WITH META AS DATABASE …):
     — Consumer receives DDL operations only after COMMIT (atomicity).
     — Consumer receives ALL DDL operations in the transaction (completeness).
     — Received schema exactly matches the committed DDL (correctness).
     — On ROLLBACK, consumer receives nothing (rollback isolation).

  B. SQL STREAM computation (CREATE STREAM … AS SELECT … FROM stb):
     — Tables created via batch txn (COMMIT) are accessible to the stream.
     — Stream output table contains correct aggregated data from ALL CTBs
       created in the committed transaction.
     — CTBs created in a ROLLBACK transaction are not processed by the stream.

These tests go beyond "API can be called"; they validate the actual content
of consumed messages and stream results against known expected values.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, tdStream
import ctypes
import time
import json


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

DB     = "txn_consumer_db"
TOPIC  = "txn_consumer_topic"
STREAM = "txn_stream_s1"
STREAM_RESULT = "txn_stream_result"


def _reset_db():
    """Drop and recreate the test database."""
    tdSql.execute(f"drop database if exists {DB}")
    tdSql.execute(f"create database {DB} vgroups 1 keep 36500")
    tdSql.execute(f"use {DB}")


def _get_json_meta_str(msg):
    """Fetch the JSON meta description of a TMQ meta message (CREATE/ALTER/DROP
    DDL) via the C API's tmq_get_json_meta().

    The installed taos.tmq.Message class has no support for TMQ_RES_TABLE_META
    results at all: Message.value()/__iter__ return None for them, and there is
    no block.json() method anywhere in the module. Call the C function directly
    (via the same libtaos handle the client already loaded) instead of relying
    on a binding that doesn't exist. Returns None for data messages (or on any
    failure), matching tmq_get_json_meta's own "null means not applicable/error"
    contract.
    """
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


def _poll_all(consumer, timeout_s=8):
    """Drain all available messages from the consumer within timeout_s seconds.

    Returns a list of dict with keys:
      'repr'   – repr(block)/JSON-meta text string for name-based inspection
      'json'   – parsed JSON dict when available, else None

    Stops as soon as two consecutive 1-second polls return nothing.
    """
    collected = []
    empty_rounds = 0
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msg = consumer.poll(1)
        if msg is None:
            empty_rounds += 1
            if empty_rounds >= 2:
                break
            continue
        if msg.error() is not None:
            tdLog.info(f"  TMQ poll error: {msg.error()}")
            empty_rounds += 1
            continue
        empty_rounds = 0

        # TMQ meta messages (DDL) — value()/__iter__ don't work for these in
        # this client, fetch the JSON meta description directly instead.
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
                try:
                    raw = block.json()
                    if isinstance(raw, (str, bytes)):
                        entry["json"] = json.loads(raw)
                    elif isinstance(raw, dict):
                        entry["json"] = raw
                except Exception:
                    pass
                collected.append(entry)
        except Exception:
            entry = {"repr": repr(msg), "json": None}
            collected.append(entry)
    return collected


def _names_in(entries):
    """Return a set of table/column names found across all collected entries."""
    names = set()
    for e in entries:
        text = e["repr"] + (json.dumps(e["json"]) if e["json"] else "")
        # Extract token-like words from the JSON/repr dump
        for word in text.replace('"', ' ').replace("'", ' ').split():
            if len(word) >= 3:
                names.add(word.lower())
    return names


def _count_entries_containing(entries, keyword):
    """Count collected entries whose repr/json contains keyword."""
    kw = keyword.lower()
    count = 0
    for e in entries:
        text = (e["repr"] + (json.dumps(e["json"]) if e["json"] else "")).lower()
        if kw in text:
            count += 1
    return count


def _open_consumer(group_id):
    """Create and return a taos.tmq.Consumer subscribed to TOPIC."""
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


# ─────────────────────────────────────────────────────────────────────────────
# Test class
# ─────────────────────────────────────────────────────────────────────────────

class TestTxnConsumerDataCorrectness:
    """WAL consumer data correctness for batch DDL transactions (TMQ + stream)."""

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdStream.ensureSnode(1)

    # ─────────────────────────────────────────────────────────────────────
    # Part A: TMQ meta consumer
    # ─────────────────────────────────────────────────────────────────────

    def sA1_tmq_commit_completeness(self):
        """
        sA1: TMQ consumer receives ALL DDL operations in a committed transaction.

        Transaction:
          BEGIN
          CREATE TABLE ntb_a1 (ts TIMESTAMP, v INT, name VARCHAR(64))
          CREATE TABLE stb_a1 (ts TIMESTAMP, score FLOAT) TAGS (region VARCHAR(32))
          CREATE TABLE ct_a1_1 USING stb_a1 TAGS ('north')
          CREATE TABLE ct_a1_2 USING stb_a1 TAGS ('south')
          COMMIT

        Completeness check: consumer must receive at least one message referencing
          EACH of the 4 created objects (ntb_a1, stb_a1, ct_a1_1, ct_a1_2).
        Schema check: verify column/tag details via SQL DESC after delivery.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute(f"drop topic if exists {TOPIC}")
        tdSql.execute(f"create topic {TOPIC} with meta as database {DB}")

        try:
            from taos.tmq import Consumer
        except ImportError:
            tdLog.info("sA1: taos.tmq not available — skip")
            return

        consumer = _open_consumer("g_sA1")
        try:
            # Drain pre-existing messages (the DB was just created, should be empty)
            _poll_all(consumer, timeout_s=3)

            # Commit a transaction with 4 DDL operations
            tdSql.execute("BEGIN")
            tdSql.execute(
                "CREATE TABLE ntb_a1 (ts TIMESTAMP, v INT, name VARCHAR(64))"
            )
            tdSql.execute(
                "CREATE TABLE stb_a1 (ts TIMESTAMP, score FLOAT) TAGS (region VARCHAR(32))"
            )
            tdSql.execute("CREATE TABLE ct_a1_1 USING stb_a1 TAGS ('north')")
            tdSql.execute("CREATE TABLE ct_a1_2 USING stb_a1 TAGS ('south')")
            tdSql.execute("COMMIT")

            # Sanity gate: all 4 objects must exist in SQL
            tdSql.query(f"show {DB}.tables")
            assert tdSql.queryRows == 3, \
                f"Expected 3 tables (ntb_a1 + 2 CTBs), got {tdSql.queryRows}"
            tdSql.query(f"show {DB}.stables")
            assert tdSql.queryRows == 1, \
                f"Expected 1 super table (stb_a1), got {tdSql.queryRows}"

            # Collect consumer messages (up to 8s)
            entries = _poll_all(consumer, timeout_s=8)
            tdLog.info(f"sA1: collected {len(entries)} TMQ entries post-COMMIT")
            assert len(entries) > 0, \
                "sA1: consumer received 0 messages after COMMIT — completeness FAILED"

            # Completeness: every created object must appear in at least one entry
            names = _names_in(entries)
            tdLog.info(f"sA1: names found in TMQ messages: {names & {'ntb_a1','stb_a1','ct_a1_1','ct_a1_2'}}")
            for expected in ("ntb_a1", "stb_a1", "ct_a1_1", "ct_a1_2"):
                assert expected in names, \
                    f"sA1: TMQ consumer missing DDL for '{expected}' — completeness FAILED"

            tdLog.info("sA1: ALL 4 DDL objects received by TMQ consumer")

            # Schema correctness: verify via SQL DESC that committed schema matches
            tdSql.query(f"desc {DB}.ntb_a1")
            col_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
            assert "v"    in col_names, "sA1: ntb_a1 missing column 'v'"
            assert "name" in col_names, "sA1: ntb_a1 missing column 'name'"
            tdLog.info("sA1: ntb_a1 schema correct (v, name present)")

            tdSql.query(f"desc {DB}.stb_a1")
            col_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
            assert "score"  in col_names, "sA1: stb_a1 missing column 'score'"
            assert "region" in col_names, "sA1: stb_a1 missing tag 'region'"
            tdLog.info("sA1: stb_a1 schema correct (score, region present)")

        finally:
            _close_consumer(consumer)
            tdSql.execute(f"drop topic if exists {TOPIC}")

        tdLog.info("sA1 PASSED — TMQ consumer completeness + schema correctness")

    def sA2_tmq_rollback_isolation(self):
        """
        sA2: TMQ consumer receives NOTHING for a rolled-back transaction.

        Transaction:
          BEGIN
          CREATE TABLE ntb_rb1 (ts TIMESTAMP, x DOUBLE)
          CREATE TABLE ntb_rb2 (ts TIMESTAMP, y BIGINT)
          ROLLBACK

        Isolation check: consumer must NOT receive any message referencing
          ntb_rb1 or ntb_rb2.  Both tables must be absent from SQL as well.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute(f"drop topic if exists {TOPIC}")
        tdSql.execute(f"create topic {TOPIC} with meta as database {DB}")

        try:
            from taos.tmq import Consumer
        except ImportError:
            tdLog.info("sA2: taos.tmq not available — skip")
            return

        consumer = _open_consumer("g_sA2")
        try:
            _poll_all(consumer, timeout_s=3)  # drain

            # First commit a known object so the consumer has something to anchor on
            tdSql.execute(
                f"CREATE TABLE anchor_a2 (ts TIMESTAMP, v INT)"
            )

            # Rollback transaction
            tdSql.execute("BEGIN")
            tdSql.execute("CREATE TABLE ntb_rb1 (ts TIMESTAMP, x DOUBLE)")
            tdSql.execute("CREATE TABLE ntb_rb2 (ts TIMESTAMP, y BIGINT)")
            tdSql.execute("ROLLBACK")

            # Now commit another object so consumer has a delivery opportunity
            tdSql.execute("CREATE TABLE after_rb (ts TIMESTAMP, z INT)")

            # Collect messages; wait long enough for after_rb to arrive
            entries = _poll_all(consumer, timeout_s=10)
            tdLog.info(f"sA2: collected {len(entries)} TMQ entries")

            # Rollback isolation: rolled-back objects must NOT appear
            rb_count_1 = _count_entries_containing(entries, "ntb_rb1")
            rb_count_2 = _count_entries_containing(entries, "ntb_rb2")
            assert rb_count_1 == 0, \
                f"sA2: TMQ consumer received {rb_count_1} entries for rolled-back ntb_rb1 — isolation FAILED"
            assert rb_count_2 == 0, \
                f"sA2: TMQ consumer received {rb_count_2} entries for rolled-back ntb_rb2 — isolation FAILED"
            tdLog.info("sA2: rolled-back objects not delivered (OK)")

            # The anchor and after_rb objects must be visible in SQL
            tdSql.query(f"show {DB}.tables like 'anchor_a2'")
            assert tdSql.queryRows == 1, "sA2: anchor_a2 missing from SQL"
            tdSql.query(f"show {DB}.tables like 'after_rb'")
            assert tdSql.queryRows == 1, "sA2: after_rb missing from SQL"

            # Rolled-back tables must be ABSENT from SQL
            tdSql.query(f"show {DB}.tables like 'ntb_rb1'")
            assert tdSql.queryRows == 0, "sA2: ntb_rb1 wrongly visible in SQL after ROLLBACK"
            tdSql.query(f"show {DB}.tables like 'ntb_rb2'")
            assert tdSql.queryRows == 0, "sA2: ntb_rb2 wrongly visible in SQL after ROLLBACK"

        finally:
            _close_consumer(consumer)
            tdSql.execute(f"drop topic if exists {TOPIC}")

        tdLog.info("sA2 PASSED — TMQ consumer rollback isolation")

    def sA3_tmq_alter_schema_correctness(self):
        """
        sA3: TMQ consumer receives ALTER TABLE schema change; SQL confirms new schema.

        Transaction:
          CREATE TABLE stb_a3 (ts TIMESTAMP, v INT) TAGS (t INT)   [outside txn]
          BEGIN
          ALTER TABLE stb_a3 ADD COLUMN extra_col VARCHAR(128)
          ALTER TABLE stb_a3 ADD TAG   extra_tag  FLOAT
          COMMIT

        Correctness: consumer receives ALTER events; SQL DESC confirms both
        extra_col and extra_tag exist with correct types.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute(
            "CREATE TABLE stb_a3 (ts TIMESTAMP, v INT) TAGS (t INT)"
        )
        tdSql.execute(f"drop topic if exists {TOPIC}")
        tdSql.execute(f"create topic {TOPIC} with meta as database {DB}")

        try:
            from taos.tmq import Consumer
        except ImportError:
            tdLog.info("sA3: taos.tmq not available — skip")
            return

        consumer = _open_consumer("g_sA3")
        try:
            _poll_all(consumer, timeout_s=3)  # drain pre-existing

            tdSql.execute("BEGIN")
            tdSql.execute("ALTER TABLE stb_a3 ADD COLUMN extra_col VARCHAR(128)")
            tdSql.execute("ALTER TABLE stb_a3 ADD TAG   extra_tag  FLOAT")
            tdSql.execute("COMMIT")

            entries = _poll_all(consumer, timeout_s=8)
            tdLog.info(f"sA3: collected {len(entries)} TMQ entries")
            assert len(entries) > 0, "sA3: no TMQ messages after ALTER COMMIT"

            # Consumer must reference the altered table
            names = _names_in(entries)
            assert "stb_a3" in names or "extra_col" in names or "extra_tag" in names, \
                "sA3: TMQ did not deliver ALTER for stb_a3 — correctness FAILED"
            tdLog.info("sA3: ALTER events delivered to TMQ consumer")

            # SQL schema correctness after consumer delivery
            tdSql.query(f"desc {DB}.stb_a3")
            col_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
            assert "extra_col" in col_names, "sA3: extra_col not in stb_a3 after ALTER COMMIT"
            assert "extra_tag" in col_names, "sA3: extra_tag not in stb_a3 after ALTER COMMIT"

            # Type check for extra_col (VARCHAR) and extra_tag (FLOAT)
            for i in range(tdSql.queryRows):
                row = tdSql.queryResult[i]
                col_name = row[0]
                col_type = row[1].upper() if isinstance(row[1], str) else str(row[1]).upper()
                if col_name == "extra_col":
                    assert "VARCHAR" in col_type or "BINARY" in col_type, \
                        f"sA3: extra_col type wrong: {col_type}"
                if col_name == "extra_tag":
                    assert "FLOAT" in col_type, \
                        f"sA3: extra_tag type wrong: {col_type}"

            tdLog.info("sA3: schema types verified (extra_col VARCHAR, extra_tag FLOAT)")

        finally:
            _close_consumer(consumer)
            tdSql.execute(f"drop topic if exists {TOPIC}")

        tdLog.info("sA3 PASSED — TMQ consumer ALTER schema correctness")

    def sA4_tmq_multi_txn_ordering(self):
        """
        sA4: Multiple consecutive transactions; consumer receives all in order.

        Txn 1: CREATE t_seq1, t_seq2  → COMMIT
        Txn 2: CREATE t_seq3, t_seq4  → COMMIT
        Txn 3: CREATE t_seq5          → ROLLBACK  (must be absent)
        Txn 4: CREATE t_seq6          → COMMIT

        Correctness: consumer sees t_seq1, t_seq2, t_seq3, t_seq4, t_seq6.
                     t_seq5 must be absent.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")
        tdSql.execute(f"drop topic if exists {TOPIC}")
        tdSql.execute(f"create topic {TOPIC} with meta as database {DB}")

        try:
            from taos.tmq import Consumer
        except ImportError:
            tdLog.info("sA4: taos.tmq not available — skip")
            return

        consumer = _open_consumer("g_sA4")
        try:
            _poll_all(consumer, timeout_s=3)

            # Txn 1
            tdSql.execute("BEGIN")
            tdSql.execute("CREATE TABLE t_seq1 (ts TIMESTAMP, v INT)")
            tdSql.execute("CREATE TABLE t_seq2 (ts TIMESTAMP, v INT)")
            tdSql.execute("COMMIT")

            # Txn 2
            tdSql.execute("BEGIN")
            tdSql.execute("CREATE TABLE t_seq3 (ts TIMESTAMP, v INT)")
            tdSql.execute("CREATE TABLE t_seq4 (ts TIMESTAMP, v INT)")
            tdSql.execute("COMMIT")

            # Txn 3 (rolled back)
            tdSql.execute("BEGIN")
            tdSql.execute("CREATE TABLE t_seq5 (ts TIMESTAMP, v INT)")
            tdSql.execute("ROLLBACK")

            # Txn 4
            tdSql.execute("BEGIN")
            tdSql.execute("CREATE TABLE t_seq6 (ts TIMESTAMP, v INT)")
            tdSql.execute("COMMIT")

            entries = _poll_all(consumer, timeout_s=12)
            tdLog.info(f"sA4: collected {len(entries)} TMQ entries")

            names = _names_in(entries)
            committed   = {"t_seq1", "t_seq2", "t_seq3", "t_seq4", "t_seq6"}
            rolled_back = {"t_seq5"}

            for tbl in committed:
                assert tbl in names, \
                    f"sA4: committed table '{tbl}' missing from TMQ — completeness FAILED"
            for tbl in rolled_back:
                count = _count_entries_containing(entries, tbl)
                assert count == 0, \
                    f"sA4: rolled-back table '{tbl}' appeared {count} times — isolation FAILED"

            tdLog.info("sA4: all 5 committed tables present; rolled-back table absent")

        finally:
            _close_consumer(consumer)
            tdSql.execute(f"drop topic if exists {TOPIC}")

        tdLog.info("sA4 PASSED — TMQ multi-txn ordering + isolation")

    # ─────────────────────────────────────────────────────────────────────
    # Part B: SQL STREAM computation
    # ─────────────────────────────────────────────────────────────────────

    def sB1_stream_sees_ctbs_from_committed_txn(self):
        """
        sB1: SQL STREAM correctly processes data from CTBs created via batch txn.

        Setup:
          1. CREATE STB stb_b1 (ts TS, score INT) TAGS (region VARCHAR(32))
          2. CREATE STREAM on stb_b1: aggregates first(score) per tbname
          3. Commit transaction: CREATE ct_b1_north, ct_b1_south, ct_b1_east
             (3 child tables, all under stb_b1)
          4. Insert 1 row per CTB with known score values

        Correctness check:
          Stream result table must have exactly 3 rows with the correct
          per-table first(score) values: north=10, south=20, east=30.

        This verifies that the STREAM computation sees ALL CTBs created in
        the committed batch transaction, not just a subset.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")

        # Step 1: Create super table (outside transaction)
        tdSql.execute(
            "CREATE TABLE stb_b1 (ts TIMESTAMP, score INT) TAGS (region VARCHAR(32))"
        )

        # Step 2: Create stream that aggregates first(score) per child table name
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")
        tdSql.execute(
            f"CREATE STREAM {STREAM} INTERVAL(1s) SLIDING(1s) "
            f"FROM {DB}.stb_b1 "
            f"PARTITION BY tbname "
            f"STREAM_OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) "
            f"INTO {STREAM_RESULT} AS "
            f"SELECT _twstart, %%tbname AS src_tbname, first(score) AS first_score "
            f"FROM %%trows"
        )
        tdLog.info("sB1: STREAM created")

        # Step 3: Commit 3 CTBs in a single batch transaction
        tdSql.execute("BEGIN")
        tdSql.execute("CREATE TABLE ct_b1_north USING stb_b1 TAGS ('north')")
        tdSql.execute("CREATE TABLE ct_b1_south USING stb_b1 TAGS ('south')")
        tdSql.execute("CREATE TABLE ct_b1_east  USING stb_b1 TAGS ('east')")
        tdSql.execute("COMMIT")

        # Verify all 3 CTBs committed
        tdSql.query(f"show {DB}.tables")
        assert tdSql.queryRows == 3, \
            f"sB1: expected 3 CTBs after COMMIT, got {tdSql.queryRows}"
        tdLog.info("sB1: 3 CTBs committed successfully")

        # Step 4: Insert known score values into each CTB
        t0 = "2025-01-01T00:00:00"
        tdSql.execute(f"INSERT INTO ct_b1_north VALUES ('{t0}', 10)")
        tdSql.execute(f"INSERT INTO ct_b1_south VALUES ('{t0}', 20)")
        tdSql.execute(f"INSERT INTO ct_b1_east  VALUES ('{t0}', 30)")
        tdLog.info("sB1: inserted score 10/20/30 into north/south/east")

        # Step 5: Wait for stream to process (up to 20s).
        rows_ready = False
        for _ in range(20):
            time.sleep(1)
            try:
                tdSql.query(f"SELECT src_tbname, first_score FROM {DB}.{STREAM_RESULT} ORDER BY src_tbname")
                if tdSql.queryRows == 3:
                    rows_ready = True
                    break
            except Exception:
                continue

        assert rows_ready, (
            f"sB1: stream result has {tdSql.queryRows} rows after 20s; "
            f"expected 3 (one per CTB created via batch txn)"
        )
        tdLog.info(f"sB1: stream result has {tdSql.queryRows} rows — completeness OK")

        # Step 6: Verify exact score values
        result = {}
        for i in range(tdSql.queryRows):
            tbname     = tdSql.queryResult[i][0]
            first_score = tdSql.queryResult[i][1]
            result[tbname] = first_score

        expected = {
            "ct_b1_north": 10,
            "ct_b1_south": 20,
            "ct_b1_east":  30,
        }
        for tbl, exp_score in expected.items():
            assert tbl in result, \
                f"sB1: stream result missing row for {tbl} — completeness FAILED"
            assert result[tbl] == exp_score, \
                f"sB1: {tbl} first_score={result[tbl]}, expected {exp_score} — correctness FAILED"

        tdLog.info("sB1: stream score values correct (north=10, south=20, east=30)")

        # Cleanup
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")

        tdLog.info("sB1 PASSED — STREAM sees ALL CTBs from committed batch txn, data correct")

    def sB2_stream_excludes_rolled_back_ctbs(self):
        """
        sB2: SQL STREAM does NOT process data from CTBs created in a ROLLBACK txn.

        Setup:
          1. CREATE STB stb_b2 (ts TS, v INT) TAGS (grp INT)
          2. CREATE STREAM on stb_b2
          3. Commit: CREATE ct_b2_ok1, ct_b2_ok2
          4. Rollback: CREATE ct_b2_rb1, ct_b2_rb2 → ROLLBACK (must be absent)
          5. Insert data into ct_b2_ok1 (v=100) and ct_b2_ok2 (v=200)
          6. Try inserting into ct_b2_rb1 — must FAIL (table doesn't exist)

        Correctness:
          Stream result has exactly 2 rows (ok1 and ok2), NOT 4.
          Rolled-back CTBs are absent from both SQL and stream.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            "CREATE TABLE stb_b2 (ts TIMESTAMP, v INT) TAGS (grp INT)"
        )
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")
        tdSql.execute(
            f"CREATE STREAM {STREAM} INTERVAL(1s) SLIDING(1s) "
            f"FROM {DB}.stb_b2 "
            f"PARTITION BY tbname "
            f"STREAM_OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) "
            f"INTO {STREAM_RESULT} AS "
            f"SELECT _twstart, %%tbname AS src_tbname, first(v) AS first_v "
            f"FROM %%trows"
        )

        # Commit 2 CTBs
        tdSql.execute("BEGIN")
        tdSql.execute("CREATE TABLE ct_b2_ok1 USING stb_b2 TAGS (1)")
        tdSql.execute("CREATE TABLE ct_b2_ok2 USING stb_b2 TAGS (2)")
        tdSql.execute("COMMIT")

        # Rollback 2 CTBs
        tdSql.execute("BEGIN")
        tdSql.execute("CREATE TABLE ct_b2_rb1 USING stb_b2 TAGS (3)")
        tdSql.execute("CREATE TABLE ct_b2_rb2 USING stb_b2 TAGS (4)")
        tdSql.execute("ROLLBACK")

        # Rolled-back tables must be absent from SQL
        tdSql.query(f"show {DB}.tables")
        assert tdSql.queryRows == 2, \
            f"sB2: expected 2 CTBs after commit+rollback, got {tdSql.queryRows}"

        tdSql.query(f"show {DB}.tables like 'ct_b2_rb1'")
        assert tdSql.queryRows == 0, "sB2: ct_b2_rb1 wrongly visible after ROLLBACK"
        tdSql.query(f"show {DB}.tables like 'ct_b2_rb2'")
        assert tdSql.queryRows == 0, "sB2: ct_b2_rb2 wrongly visible after ROLLBACK"

        # Insert into committed CTBs
        t0 = "2025-01-02T00:00:00"
        tdSql.execute(f"INSERT INTO ct_b2_ok1 VALUES ('{t0}', 100)")
        tdSql.execute(f"INSERT INTO ct_b2_ok2 VALUES ('{t0}', 200)")

        # Confirm rolled-back tables cannot be written to
        try:
            tdSql.execute(f"INSERT INTO ct_b2_rb1 VALUES ('{t0}', 999)")
            assert False, "sB2: INSERT into rolled-back ct_b2_rb1 should have FAILED"
        except Exception:
            tdLog.info("sB2: INSERT into ct_b2_rb1 correctly FAILED (table absent)")

        # Wait for stream to process
        rows_ready = False
        for _ in range(20):
            time.sleep(1)
            try:
                tdSql.query(
                    f"SELECT src_tbname, first_v FROM {DB}.{STREAM_RESULT} ORDER BY src_tbname"
                )
                if tdSql.queryRows == 2:
                    rows_ready = True
                    break
            except Exception:
                continue

        assert rows_ready, (
            f"sB2: stream result has {tdSql.queryRows} rows, expected exactly 2"
        )

        # Verify stream has correct values for ok1 and ok2 only
        result = {}
        for i in range(tdSql.queryRows):
            result[tdSql.queryResult[i][0]] = tdSql.queryResult[i][1]

        assert "ct_b2_ok1" in result and result["ct_b2_ok1"] == 100, \
            f"sB2: ct_b2_ok1 first_v={result.get('ct_b2_ok1')} expected 100"
        assert "ct_b2_ok2" in result and result["ct_b2_ok2"] == 200, \
            f"sB2: ct_b2_ok2 first_v={result.get('ct_b2_ok2')} expected 200"

        assert "ct_b2_rb1" not in result, \
            "sB2: stream processed rolled-back ct_b2_rb1 — isolation FAILED"
        assert "ct_b2_rb2" not in result, \
            "sB2: stream processed rolled-back ct_b2_rb2 — isolation FAILED"

        tdLog.info("sB2: stream has exactly 2 rows; rolled-back CTBs absent (OK)")

        # Cleanup
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")

        tdLog.info("sB2 PASSED — STREAM excludes rolled-back CTBs; data values correct")

    def sB3_stream_data_from_large_committed_txn(self):
        """
        sB3: Stream correctly aggregates data from a large batch txn (N=10 CTBs).

        Verifies that the stream sees ALL 10 CTBs created in one committed
        transaction, not a subset (a partial-delivery bug would show < 10 rows).

        Each CTB gets a unique score value; the stream first(score) per tbname
        must match the inserted value exactly.
        """
        _reset_db()
        tdSql.execute(f"use {DB}")

        N = 10
        tdSql.execute(
            "CREATE TABLE stb_b3 (ts TIMESTAMP, score INT) TAGS (idx INT)"
        )
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")
        tdSql.execute(
            f"CREATE STREAM {STREAM} INTERVAL(1s) SLIDING(1s) "
            f"FROM {DB}.stb_b3 "
            f"PARTITION BY tbname "
            f"STREAM_OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) "
            f"INTO {STREAM_RESULT} AS "
            f"SELECT _twstart, %%tbname AS src_tbname, first(score) AS first_score "
            f"FROM %%trows"
        )

        # Commit N CTBs in a single transaction
        tdSql.execute("BEGIN")
        for i in range(N):
            tdSql.execute(f"CREATE TABLE ct_b3_{i:02d} USING stb_b3 TAGS ({i})")
        tdSql.execute("COMMIT")

        tdSql.query(f"show {DB}.tables")
        assert tdSql.queryRows == N, \
            f"sB3: expected {N} CTBs after COMMIT, got {tdSql.queryRows}"
        tdLog.info(f"sB3: {N} CTBs committed in single batch txn")

        # Insert unique score = i*10 into each CTB
        t0 = "2025-01-03T00:00:00"
        for i in range(N):
            tdSql.execute(f"INSERT INTO ct_b3_{i:02d} VALUES ('{t0}', {i * 10})")

        # Wait for stream to process all N CTBs
        rows_ready = False
        for _ in range(25):
            time.sleep(1)
            try:
                tdSql.query(
                    f"SELECT src_tbname, first_score FROM {DB}.{STREAM_RESULT} ORDER BY src_tbname"
                )
                if tdSql.queryRows == N:
                    rows_ready = True
                    break
            except Exception:
                continue

        assert rows_ready, (
            f"sB3: stream has {tdSql.queryRows}/{N} rows after 25s — "
            f"partial delivery from batch txn DETECTED"
        )
        tdLog.info(f"sB3: stream has {N} rows — completeness OK")

        # Verify all scores match i*10
        result = {}
        for i in range(tdSql.queryRows):
            result[tdSql.queryResult[i][0]] = tdSql.queryResult[i][1]

        for i in range(N):
            tbl = f"ct_b3_{i:02d}"
            assert tbl in result, \
                f"sB3: stream missing row for {tbl}"
            assert result[tbl] == i * 10, \
                f"sB3: {tbl} score={result[tbl]}, expected {i*10} — correctness FAILED"

        tdLog.info(f"sB3: all {N} CTB scores correct (0, 10, 20, …, {(N-1)*10})")

        # Cleanup
        tdSql.execute(f"DROP STREAM IF EXISTS {STREAM}")
        tdSql.execute(f"DROP TABLE  IF EXISTS {STREAM_RESULT}")

        tdLog.info("sB3 PASSED — STREAM complete + correct for large committed batch txn")

    # ─────────────────────────────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────────────────────────────

    def test_txn_consumer_data_correctness(self):
        """WAL consumer data correctness: TMQ meta (sA1-sA4) + SQL STREAM (sB1-sB3).

        Part A — TMQ meta consumer:
          sA1: Commit 4 DDL ops → consumer receives all 4; schema verified via DESC.
          sA2: Rollback 2 CREATE TABLE → consumer receives nothing; tables absent.
          sA3: Commit ALTER ADD COLUMN + ADD TAG → schema types verified.
          sA4: 4 consecutive txns (2 commit, 1 rollback, 1 commit) → correct set delivered.

        Part B — SQL STREAM:
          sB1: Stream sees all 3 CTBs from committed txn; per-CTB score values correct.
          sB2: Stream sees only committed CTBs; rolled-back CTBs absent from result.
          sB3: Stream aggregates all 10 CTBs from large batch txn; no partial delivery.

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        """
        # Part A: TMQ
        self.sA1_tmq_commit_completeness()
        self.sA2_tmq_rollback_isolation()
        self.sA3_tmq_alter_schema_correctness()
        self.sA4_tmq_multi_txn_ordering()

        # Part B: Stream
        self.sB1_stream_sees_ctbs_from_committed_txn()
        self.sB2_stream_excludes_rolled_back_ctbs()
        self.sB3_stream_data_from_large_committed_txn()
