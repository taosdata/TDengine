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
            tdSql.query(f"SELECT name FROM information_schema.ins_databases WHERE name='{db}'")
            if tdSql.queryRows == 0:
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

    def test_sleep_basic(self):
        """Fun: sleep() basic functionality

        1. SELECT SLEEP(0.2) should return 0 and take ~0.2 seconds
        2. SELECT SLEEP(0) should return 0 instantly
        3. SELECT SLEEP(0.1) should return 0 and take ~0.1 seconds

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # SLEEP(0.2) should take ~0.2 seconds
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.2)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.15 or elapsed > 1.0:
            tdLog.exit(f"SLEEP(0.2) elapsed {elapsed:.3f}s, expected ~0.2s")
        tdLog.info(f"SLEEP(0.2) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0) should be instant
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(0) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(0) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0.1) should take ~0.1 seconds
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.05 or elapsed > 0.8:
            tdLog.exit(f"SLEEP(0.1) elapsed {elapsed:.3f}s, expected ~0.1s")
        tdLog.info(f"SLEEP(0.1) elapsed {elapsed:.3f}s, passed")

    def test_sleep_with_table(self):
        """Fun: sleep() with table query

        1. Create database and table with data
        2. SELECT SLEEP(0.1), col FROM table should return one row per input row
        3. All SLEEP results should be 0 (success)

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

    def test_sleep_negative(self):
        """Fun: sleep() negative value returns 0 instantly (MariaDB-compatible)

        1. SELECT SLEEP(-1) should return 0 instantly
        2. SELECT SLEEP(-0.5) should return 0 instantly

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # integer negative
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(-1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-1) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-1) elapsed {elapsed:.3f}s, passed")

        # fractional negative
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(-0.5)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-0.5) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-0.5) elapsed {elapsed:.3f}s, passed")

    def test_sleep_null(self):
        """Fun: sleep() with NULL argument returns NULL

        1. SELECT SLEEP(NULL) should return NULL

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        tdSql.query("SELECT SLEEP(NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_sleep_invalid_params(self):
        """Fun: sleep() invalid parameter handling

        1. SLEEP() with no args should fail
        2. SLEEP('abc') with string should fail
        3. SLEEP(1, 2) with too many args should fail

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        tdSql.error("SELECT SLEEP()")
        tdSql.error("SELECT SLEEP('abc')")
        tdSql.error("SELECT SLEEP(1, 2)")

    def test_sleep_integer_input(self):
        """Fun: sleep() with integer input

        1. SELECT SLEEP(1) with integer arg should work
        2. Verify return value is 0
        3. Verify elapsed time is ~1 second

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

        1. SELECT SLEEP(0) + 1 should return 1
        2. SELECT SLEEP(-1) + 1 should return 1 (negative returns 0, MariaDB-compatible)
        3. SELECT SLEEP(0) = 0 should return true (1)

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

    def test_sleep_killed(self):
        """Fun: sleep() interrupted by KILL QUERY

        1. SELECT SLEEP(30) constant — runs server-side (not folded, VOLATILE_FUNC)
        2. SELECT SLEEP(v) FROM t   — column ref, runs server-side
        Both should appear in SHOW QUERIES and, when killed, may either return 1
        or terminate with a cancelled/killed error accepted by this test.

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

    def test_sleep_column_once_per_row(self):
        """Fun: sleep() with column argument sleeps once per row (MySQL-compatible)

        1. Three rows with v = 0.1; SELECT SLEEP(v), v FROM t1 should sleep ~0.3s total
        2. Result for every row should be 0 (sleep succeeded)

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
        """Fun: sleep() with column argument produces per-row NULL for NULL inputs

        1. Rows: v = [0.1, NULL, 0.2]; SELECT SLEEP(v), v FROM t1
        2. Row 0 (v=0.1): returns 0 after sleeping ~0.1s
        3. Row 1 (v=NULL): returns NULL (no sleep)
        4. Row 2 (v=0.2): returns 0 after sleeping ~0.2s (MySQL-compatible: sleeps per row)
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
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 0)
        # MySQL-compatible: sleep per row => 0.1s + 0.2s = ~0.3s (NULL row skipped)
        if elapsed < 0.25 or elapsed > 1.5:
            tdLog.exit(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, expected ~0.3s (0.1s+0.2s, NULL skipped)")
        tdLog.info(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, passed")

    def test_sleep_empty_table(self):
        """Fun: sleep() against empty table returns 0 rows without error

        1. Empty table; SELECT SLEEP(v) FROM t1 should return 0 rows

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

    def test_sleep_in_where(self):
        """Fun: sleep() in WHERE clause filters rows by return value (MariaDB-compatible)

        WHERE:
        1. WHERE SLEEP(0): returns 0 (falsy) -> 0 rows pass the filter
        2. WHERE SLEEP(v) with v=0.05 (3 rows): once per row (~0.15s total), 0 rows pass

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

        # SLEEP(0) = 0 (falsy) -> no rows pass
        tdSql.query("SELECT v FROM t1 WHERE SLEEP(0)")
        tdSql.checkRows(0)
        tdLog.info("WHERE SLEEP(0) filtered all rows, passed")

        # SLEEP(v) with v=0.05: once per row -> ~0.15s total, 0 returned (falsy) -> 0 rows pass
        start = time.monotonic()
        tdSql.query("SELECT v FROM t1 WHERE SLEEP(v)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(0)
        if elapsed < 0.1 or elapsed > 1.0:
            tdLog.exit(f"WHERE SLEEP(v) with 3 rows elapsed {elapsed:.3f}s, expected ~0.15s")
        tdLog.info(f"WHERE SLEEP(v) elapsed {elapsed:.3f}s, passed")

    def test_sleep_no_pushdown(self):
        """Fun: sleep() is not pushed down to vnodes (FUNC_MGT_NO_PUSHDOWN_FUNC)

        With NO_PUSHDOWN, SLEEP executes at the coordinator sequentially, not in
        parallel at vnodes. Observable: 4 rows of v=0.2 across 2 vgroups should
        take ~0.8s (4 x 0.2s sequential at coordinator), not ~0.4s (parallel at
        2 vnodes).

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
