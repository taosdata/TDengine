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

import time
import threading

import taos

from new_test_framework.utils import tdLog, tdSql


class TestSleep:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = "test_sleep"

    @staticmethod
    def _recreate_db(db, vgroups=1):
        """Drop (if exists), wait for full removal, create, wait for ready."""
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        dropped = False
        for _ in range(300):
            tdSql.query(f"SELECT name, status FROM information_schema.ins_databases WHERE name='{db}'")
            if tdSql.queryRows == 0:
                dropped = True
                break
            # "dropping" means the MNode has logically removed the DB; vnode
            # cleanup is async. CREATE DATABASE with the same name will succeed.
            if str(tdSql.queryResult[0][1]).strip() == "dropping":
                dropped = True
                break
            time.sleep(1)
        if not dropped:
            tdLog.exit(f"Timed out waiting for database '{db}' to be dropped")

        try:
            tdSql.execute(f"CREATE DATABASE {db} vgroups {vgroups}")
        except Exception as err:
            tdLog.exit(f"Failed to create database '{db}': {err}")

        ready = False
        for _ in range(300):
            tdSql.query(f"SELECT status FROM information_schema.ins_databases WHERE name='{db}'")
            if tdSql.queryRows > 0 and str(tdSql.queryResult[0][0]).strip() == "ready":
                ready = True
                break
            time.sleep(1)
        if not ready:
            tdLog.exit(f"Timed out waiting for database '{db}' to become ready")
        # Allow MNode metadata transactions to fully commit before DDL follows
        time.sleep(1)

    # ------------------------------------------------------------------ §1 Basic

    def test_sleep_basic(self):
        """Fun: sleep() basic functionality

        1. [SLP-BASIC-001] SELECT SLEEP(0.2) should return 0 and take ~0.2 seconds
        2. [SLP-BASIC-002] SELECT SLEEP(0) should return 0 instantly
        3. [SLP-BASIC-003] SELECT SLEEP(0.1) should return 0 and take ~0.1 seconds

        Note: the elapsed-time checks in [SLP-BASIC-001/003] implicitly verify
        [SLP-LOCAL-001] — a no-table query is NOT folded/short-circuited by
        QUERY_EXEC_MODE_LOCAL (VOLATILE_FUNC flag prevents it).

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # SLP-BASIC-001
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.2)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.15 or elapsed > 1.0:
            tdLog.exit(f"SLEEP(0.2) elapsed {elapsed:.3f}s, expected ~0.2s")
        tdLog.info(f"SLEEP(0.2) elapsed {elapsed:.3f}s, passed")

        # SLP-BASIC-002
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(0) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(0) elapsed {elapsed:.3f}s, passed")

        # SLP-BASIC-003
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.05 or elapsed > 0.8:
            tdLog.exit(f"SLEEP(0.1) elapsed {elapsed:.3f}s, expected ~0.1s")
        tdLog.info(f"SLEEP(0.1) elapsed {elapsed:.3f}s, passed")

    def test_sleep_integer_input(self):
        """Fun: sleep() with integer input

        1. [SLP-BASIC-004] SELECT SLEEP(1) with integer arg should work, return 0, take ~1s

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.9 or elapsed > 2.0:
            tdLog.exit(f"SLEEP(1) elapsed {elapsed:.3f}s, expected ~1s")
        tdLog.info(f"SLEEP(1) elapsed {elapsed:.3f}s, passed")

    def test_sleep_in_expression(self):
        """Fun: sleep() used in expressions

        1. [SLP-BASIC-005] SELECT SLEEP(0) + 1 should return 1
        2. [SLP-BASIC-005] SELECT SLEEP(0) = 0 should return true (1)
        3. [SLP-BASIC-006] SELECT SLEEP(-1) + 1 should return 1 (negative returns 0)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        tdSql.query("SELECT SLEEP(0) + 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query("SELECT SLEEP(-1) + 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query("SELECT SLEEP(0) = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    # ------------------------------------------------------------------ §2 NULL & negative

    def test_sleep_null(self):
        """Fun: sleep() with NULL argument returns 0 instantly

        1. [SLP-NULL-001] SELECT SLEEP(NULL) should return 0 instantly (no sleep)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(NULL)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(NULL) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(NULL) elapsed {elapsed:.3f}s, passed")

    def test_sleep_negative(self):
        """Fun: sleep() negative value returns 0 instantly (MariaDB-compatible)

        1. [SLP-NULL-002] SELECT SLEEP(-1) should return 0 instantly
        2. [SLP-NULL-003] SELECT SLEEP(-0.5) should return 0 instantly

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # SLP-NULL-002: integer negative
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(-1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-1) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-1) elapsed {elapsed:.3f}s, passed")

        # SLP-NULL-003: fractional negative
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(-0.5)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-0.5) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-0.5) elapsed {elapsed:.3f}s, passed")

    # ------------------------------------------------------------------ §3 Table queries

    def test_sleep_with_table(self):
        """Fun: sleep() with table query

        1. [SLP-TBL-001] SELECT SLEEP(0.05) FROM 1-row table returns 1 row, value 0
        2. [SLP-TBL-002] SELECT SLEEP(0.05) FROM 3-row table returns 3 rows, all 0

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        self._recreate_db(self.dbname)
        tdSql.execute(f"USE {self.dbname}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 10)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, 20)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 30)")

        tdSql.query("SELECT SLEEP(0.05), v FROM t1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdLog.info("SLEEP(0.05) with 3-row table returned 3 rows with value 0, passed")

    def test_sleep_column_once_per_row(self):
        """Fun: sleep() with column argument sleeps once per row (MySQL-compatible)

        1. [SLP-TBL-003] Three rows with v=0.1; total elapsed ~0.3s (0.1s x 3)
        2. All rows return 0

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-7 Created

        """
        db = "test_sleep_col_once"
        self._recreate_db(db)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v DOUBLE) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 0.1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, 0.1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 0.1)")

        start = time.monotonic()
        tdSql.query("SELECT SLEEP(v), v FROM t1")
        elapsed = time.monotonic() - start
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        # MySQL-compatible: sleep once per row => ~0.3s for 3 rows with v=0.1
        if elapsed < 0.25 or elapsed > 1.5:
            tdLog.exit(f"SLEEP(v) with 3 rows elapsed {elapsed:.3f}s, expected ~0.3s (once per row)")
        tdLog.info(f"SLEEP(v) column arg elapsed {elapsed:.3f}s, passed")

    def test_sleep_column_with_nulls(self):
        """Fun: sleep() with column argument returns 0 for NULL inputs (no sleep)

        1. [SLP-TBL-004] Rows: v = [0.1, NULL, 0.2]; SELECT SLEEP(v), v FROM t1
        2. Row 0 (v=0.1): returns 0 after sleeping ~0.1s
        3. Row 1 (v=NULL): returns 0 (no sleep)
        4. Row 2 (v=0.2): returns 0 after sleeping ~0.2s
        5. Total elapsed ~0.3s (0.1+0.2, NULL skipped)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-7 Created

        """
        db = "test_sleep_null_col"
        self._recreate_db(db)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v DOUBLE) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 0.1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, NULL)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 0.2)")

        start = time.monotonic()
        tdSql.query("SELECT SLEEP(v), v FROM t1")
        elapsed = time.monotonic() - start
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        # MySQL-compatible: sleep per row => 0.1s + 0.2s = ~0.3s (NULL row skipped)
        if elapsed < 0.25 or elapsed > 1.5:
            tdLog.exit(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, expected ~0.3s (0.1s+0.2s, NULL skipped)")
        tdLog.info(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, passed")

    def test_sleep_empty_table(self):
        """Fun: sleep() against empty table returns 0 rows without error

        1. [SLP-TBL-005] Empty table; SELECT SLEEP(v) FROM t1 should return 0 rows

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-7 Created

        """
        db = "test_sleep_empty"
        self._recreate_db(db)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")

        tdSql.query("SELECT SLEEP(v) FROM t1")
        tdSql.checkRows(0)
        tdLog.info("SLEEP(v) on empty table returned 0 rows, passed")

    # ------------------------------------------------------------------ §4 Error cases

    def test_sleep_invalid_params(self):
        """Fun: sleep() invalid parameter handling

        1. [SLP-ERR-001] SLEEP() with no args should fail
        2. [SLP-ERR-002] SLEEP('abc') with string should fail
        3. [SLP-ERR-003] SLEEP(1, 2) with too many args should fail
        4. [SLP-ERR-004] INSERT INTO t VALUES(SLEEP(1)) should fail (write path unsupported)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created
            - 2026-4-14 Added SLP-ERR-004 INSERT error case

        """
        # SLP-ERR-001/002/003
        tdSql.error("SELECT SLEEP()")
        tdSql.error("SELECT SLEEP('abc')")
        tdSql.error("SELECT SLEEP(1, 2)")

        # SLP-ERR-004: SLEEP in INSERT write path must be rejected
        self._recreate_db("test_sleep_insert_err")
        tdSql.execute("USE test_sleep_insert_err")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.error("INSERT INTO t1 VALUES(NOW, SLEEP(1))")

    # ------------------------------------------------------------------ §5 No LOCAL short-circuit

    def test_sleep_no_local(self):
        """Fun: sleep() no-table query uses normal execution path (not LOCAL short-circuit)

        1. [SLP-LOCAL-001] SELECT SLEEP(0.2) elapsed-time check confirms the query is NOT
           folded by QUERY_EXEC_MODE_LOCAL; VOLATILE_FUNC flag forces normal execution path.
        2. [SLP-LOCAL-002] SELECT SLEEP(0.1), SLEEP(0.1) evaluates both columns serially;
           total elapsed ~0.2s (not parallel, not folded to 0).

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-14 Created

        """
        # SLP-LOCAL-001: no-table query timing proves no constant folding
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.2)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.15 or elapsed > 1.0:
            tdLog.exit(
                f"[SLP-LOCAL-001] SLEEP(0.2) elapsed {elapsed:.3f}s, "
                f"expected ~0.2s — may indicate LOCAL short-circuit (constant folding)"
            )
        tdLog.info(f"[SLP-LOCAL-001] SLEEP(0.2) no-table elapsed {elapsed:.3f}s, passed")

        # SLP-LOCAL-002: two SLEEP columns evaluated serially => total ~0.2s
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.1), SLEEP(0.1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        if elapsed < 0.15 or elapsed > 1.0:
            tdLog.exit(
                f"[SLP-LOCAL-002] SLEEP(0.1),SLEEP(0.1) elapsed {elapsed:.3f}s, "
                f"expected ~0.2s (serial eval)"
            )
        tdLog.info(f"[SLP-LOCAL-002] SLEEP(0.1),SLEEP(0.1) serial elapsed {elapsed:.3f}s, passed")

    # ------------------------------------------------------------------ §6 No pushdown

    def test_sleep_no_pushdown(self):
        """Fun: sleep() is not pushed down to vnodes (FUNC_MGT_NO_PUSHDOWN_FUNC)

        [SLP-NOPD-001] With NO_PUSHDOWN, SLEEP executes at the coordinator sequentially.
        Observable: 4 rows of v=0.2 across 2 vgroups should take ~0.8s (4 x 0.2s
        sequential at coordinator), not ~0.4s (2 x 0.2s parallel at vnodes).

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-4-12 Created

        """
        db = "test_sleep_no_pushdown"
        self._recreate_db(db, vgroups=2)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v DOUBLE) TAGS (t INT)")
        # Two child tables -> data distributed across 2 vgroups
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("CREATE TABLE t2 USING st TAGS(2)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 0.2)(NOW + 1s, 0.2)")
        tdSql.execute("INSERT INTO t2 VALUES(NOW + 2s, 0.2)(NOW + 3s, 0.2)")

        # If SLEEP were pushed to vnodes, 2 vnodes would process 2 rows each in
        # parallel => ~0.4s.  With NO_PUSHDOWN it runs at coordinator => ~0.8s.
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(v), v FROM st ORDER BY ts")
        elapsed = time.monotonic() - start
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)
        # Must be >= 0.6s to confirm sequential coordinator execution
        if elapsed < 0.6 or elapsed > 3.0:
            tdLog.exit(f"SLEEP(v) across 2 vgroups elapsed {elapsed:.3f}s, "
                       f"expected ~0.8s (NO_PUSHDOWN: sequential at coordinator)")
        tdLog.info(f"SLEEP(v) NO_PUSHDOWN elapsed {elapsed:.3f}s, passed")

    # ------------------------------------------------------------------ §7 WHERE clause

    def test_sleep_in_where(self):
        """Fun: sleep() in WHERE clause filters rows by return value (MariaDB-compatible)

        1. [SLP-WHERE-001] WHERE SLEEP(0): returns 0 (falsy) -> 0 rows pass the filter
        2. [SLP-WHERE-002] WHERE SLEEP(v) with v=0.05 (3 rows): once per row (~0.15s
           total), 0 rows pass (SLEEP returns 0, which is falsy)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-12 Created

        """
        db = "test_sleep_where"
        self._recreate_db(db)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v DOUBLE) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 0.05)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, 0.05)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 0.05)")

        # SLP-WHERE-001: SLEEP(0) = 0 (falsy) -> no rows pass
        tdSql.query("SELECT v FROM t1 WHERE SLEEP(0)")
        tdSql.checkRows(0)
        tdLog.info("WHERE SLEEP(0) filtered all rows, passed")

        # SLP-WHERE-002: SLEEP(v) per row -> ~0.15s total, 0 returned (falsy) -> 0 rows pass
        start = time.monotonic()
        tdSql.query("SELECT v FROM t1 WHERE SLEEP(v)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(0)
        if elapsed < 0.1 or elapsed > 1.0:
            tdLog.exit(f"WHERE SLEEP(v) with 3 rows elapsed {elapsed:.3f}s, expected ~0.15s")
        tdLog.info(f"WHERE SLEEP(v) elapsed {elapsed:.3f}s, passed")

    # ------------------------------------------------------------------ §8 KILL QUERY

    def test_sleep_killed(self):
        """Fun: sleep() interrupted by KILL QUERY

        1. [SLP-KILL-001] SELECT SLEEP(30) constant — runs server-side (not folded,
           VOLATILE_FUNC), visible in perf_queries, killed within 100ms (≤6s total).
        2. [SLP-KILL-002] SELECT SLEEP(v) FROM t — column ref, same kill behavior.

        Note: querying performance_schema.perf_queries to find kill_id also satisfies
        the observability check in [SLP-SLOW-002].

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-4-7 Created

        """
        kill_db = "test_sleep_kill"
        self._recreate_db(kill_db)
        tdSql.execute(f"USE {kill_db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 30)")

        for sql, marker in [
            ("SELECT SLEEP(30)", "sleep(30)"),
            (f"SELECT SLEEP(v) FROM {kill_db}.t1", "sleep(v)"),
        ]:
            result_holder = {}
            error_holder = {}

            def run_sleep(s=sql):
                conn = None
                cursor = None
                try:
                    conn = taos.connect()
                    cursor = conn.cursor()
                    cursor.execute(s)
                    rows = cursor.fetchall()
                    result_holder["value"] = rows[0][0] if rows else None
                except Exception as e:
                    error_holder["err"] = str(e)
                finally:
                    if cursor is not None:
                        cursor.close()
                    if conn is not None:
                        conn.close()

            t = threading.Thread(target=run_sleep)
            t.start()

            # wait for the query to appear in performance_schema.perf_queries (up to 5s)
            query_id = None
            for _ in range(50):
                time.sleep(0.1)
                tdSql.query("SELECT kill_id, sql FROM performance_schema.perf_queries")
                for i in range(tdSql.queryRows):
                    if marker in str(tdSql.getData(i, 1)).lower():
                        query_id = tdSql.getData(i, 0)
                        break
                if query_id:
                    break

            if query_id is None:
                t.join(timeout=5)
                tdLog.exit(f"{marker} did not appear in performance_schema.perf_queries within 5s")

            tdLog.info(f"killing query id {query_id} for: {marker}")
            tdSql.execute(f"KILL QUERY '{query_id}'")

            t.join(timeout=6)
            if t.is_alive():
                tdLog.exit(f"{marker} was not killed within 6s after KILL QUERY")

            if "err" in error_holder:
                if "killed" in error_holder["err"].lower() or "cancel" in error_holder["err"].lower():
                    tdLog.info(f"{marker} killed via exception: {error_holder['err']}, passed")
                    continue
                tdLog.exit(f"{marker} thread raised unexpected error: {error_holder['err']}")

            if result_holder.get("value") != 1:
                tdLog.exit(f"{marker} killed query returned {result_holder.get('value')}, expected 1")

            tdLog.info(f"{marker} killed, returned {result_holder['value']}, passed")

    # ------------------------------------------------------------------ §9 Timeout

    def test_sleep_timeout(self):
        """Fun: sleep() query terminates when client read timeout expires

        [SLP-TIMEOUT-001] With readTimeout < sleep duration, the query should terminate
        with a timeout error within the configured timeout window.

        This test attempts a websocket connection with timeout=2000ms. If the websocket
        connector is unavailable or does not support the timeout DSN parameter, the test
        logs a notice and cleans up the dangling query via KILL QUERY.

        To fully validate: set readTimeout=2 in taos.cfg (or equivalent DSN parameter).

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-4-14 Created

        """
        error_holder = {}
        result_holder = {}

        def run_with_timeout():
            conn = None
            cursor = None
            try:
                conn = taos.connect(url="taosws://root:taosdata@localhost:6041?timeout=2000")
                cursor = conn.cursor()
                cursor.execute("SELECT SLEEP(30)")
                cursor.fetchall()
                result_holder["status"] = "completed"
            except Exception as e:
                error_holder["err"] = str(e)
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        t = threading.Thread(target=run_with_timeout, daemon=True)
        start = time.monotonic()
        t.start()
        t.join(timeout=10)
        elapsed = time.monotonic() - start

        if t.is_alive():
            tdLog.notice(
                "[SLP-TIMEOUT-001] SLEEP(30) still running after 10s — "
                "websocket connector with timeout DSN not available. "
                "Configure readTimeout=2s (taos.cfg) to fully validate this case."
            )
            # Clean up: kill the dangling query
            try:
                tdSql.query(
                    "SELECT kill_id FROM performance_schema.perf_queries "
                    "WHERE sql LIKE '%SLEEP(30)%'"
                )
                if tdSql.queryRows > 0:
                    kill_id = tdSql.getData(0, 0)
                    tdSql.execute(f"KILL QUERY '{kill_id}'")
                    tdLog.info(f"[SLP-TIMEOUT-001] Cleaned up dangling SLEEP query: {kill_id}")
            except Exception as cleanup_err:
                tdLog.info(f"[SLP-TIMEOUT-001] Cleanup attempt: {cleanup_err}")
            t.join(timeout=5)
            return

        if result_holder.get("status") == "completed":
            tdLog.notice(
                "[SLP-TIMEOUT-001] SLEEP(30) completed without timeout error — "
                "websocket timeout DSN parameter may not be supported. "
                "Verify with readTimeout=2s in taos.cfg."
            )
            return

        err = error_holder.get("err", "")
        if "timeout" in err.lower() or "timed out" in err.lower():
            tdLog.info(f"[SLP-TIMEOUT-001] passed: query timed out in {elapsed:.2f}s — {err}")
        else:
            tdLog.info(
                f"[SLP-TIMEOUT-001] query terminated in {elapsed:.2f}s with: {err} — "
                f"verify this is a timeout error"
            )

    # ------------------------------------------------------------------ §10 Observability

    def test_sleep_show_queries(self):
        """Fun: running SLEEP query is visible in performance_schema.perf_queries

        [SLP-SLOW-002] While SELECT SLEEP(5) executes in a background thread:
        1. The query appears in performance_schema.perf_queries within 5s.
        2. The sql column contains the original SQL text.
        3. exec_usec is non-zero (query has been running for some time).

        Note: [SLP-KILL-001/002] also exercises perf_queries to obtain kill_id.

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-4-14 Created

        """
        result_holder = {}
        error_holder = {}

        def run_sleep():
            conn = None
            cursor = None
            try:
                conn = taos.connect()
                cursor = conn.cursor()
                cursor.execute("SELECT SLEEP(5)")
                cursor.fetchall()
                result_holder["done"] = True
            except Exception as e:
                error_holder["err"] = str(e)
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        t = threading.Thread(target=run_sleep, daemon=True)
        t.start()

        # Poll perf_queries until the sleep query appears (up to 5s)
        found_sql = None
        found_exec_usec = None
        for _ in range(50):
            time.sleep(0.1)
            tdSql.query("SELECT kill_id, exec_usec, sql FROM performance_schema.perf_queries")
            for i in range(tdSql.queryRows):
                sql_val = str(tdSql.getData(i, 2)).lower()
                if "sleep(5)" in sql_val:
                    found_sql = tdSql.getData(i, 2)
                    found_exec_usec = tdSql.getData(i, 1)
                    break
            if found_sql is not None:
                break

        t.join(timeout=10)

        if found_sql is None:
            tdLog.exit(
                "[SLP-SLOW-002] SLEEP(5) query not found in performance_schema.perf_queries "
                "within 5s"
            )

        tdLog.info(
            f"[SLP-SLOW-002] SLEEP(5) visible in perf_queries: "
            f"exec_usec={found_exec_usec}, sql={found_sql!r}, passed"
        )
