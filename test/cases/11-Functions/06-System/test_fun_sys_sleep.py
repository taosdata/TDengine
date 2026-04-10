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

    def test_sleep_basic(self):
        """Fun: sleep() basic functionality

        1. SELECT SLEEP(1) should return 0 and take ~1 second
        2. SELECT SLEEP(0) should return 0 instantly
        3. SELECT SLEEP(0.5) should return 0 and take ~0.5 seconds

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # SLEEP(1) should take ~1 second
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(1)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.9 or elapsed > 2.0:
            tdLog.exit(f"SLEEP(1) elapsed {elapsed:.3f}s, expected ~1s")
        tdLog.info(f"SLEEP(1) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0) should be instant
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(0) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(0) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0.5) should take ~0.5 seconds
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(0.5)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.4 or elapsed > 1.5:
            tdLog.exit(f"SLEEP(0.5) elapsed {elapsed:.3f}s, expected ~0.5s")
        tdLog.info(f"SLEEP(0.5) elapsed {elapsed:.3f}s, passed")

    def test_sleep_with_table(self):
        """Fun: sleep() with table query

        1. Create database and table with data
        2. SELECT SLEEP(1), col FROM table should sleep once, not per row
        3. Verify result rows and sleep duration is ~1s (not 3s)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
        tdSql.execute(f"CREATE DATABASE {self.dbname}")
        tdSql.execute(f"USE {self.dbname}")
        tdSql.execute(
            "CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)"
        )
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 10)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, 20)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 30)")

        start = time.monotonic()
        tdSql.query("SELECT SLEEP(1), v FROM t1")
        elapsed = time.monotonic() - start
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        # sleep once, not per row: should be ~1s, not ~3s
        if elapsed < 0.9 or elapsed > 2.5:
            tdLog.exit(f"SLEEP(1) with 3-row table elapsed {elapsed:.3f}s, expected ~1s (once, not per row)")
        tdLog.info(f"SLEEP(1) with 3-row table elapsed {elapsed:.3f}s, passed")

        tdSql.execute(f"DROP DATABASE IF EXISTS {self.dbname}")

    def test_sleep_negative(self):
        """Fun: sleep() negative value returns 1 instantly (MySQL-compatible)

        1. SELECT SLEEP(-1) should return 1 instantly
        2. SELECT SLEEP(-0.5) should return 1 instantly

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
        tdSql.checkData(0, 0, 1)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-1) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-1) elapsed {elapsed:.3f}s, passed")

        # fractional negative
        start = time.monotonic()
        tdSql.query("SELECT SLEEP(-0.5)")
        elapsed = time.monotonic() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
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

        1. SELECT SLEEP(2) with integer arg should work
        2. Verify return value is 0
        3. Verify elapsed time is ~2 seconds

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        start = time.time()
        tdSql.query("SELECT SLEEP(2)")
        elapsed = time.time() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 1.9 or elapsed > 3.0:
            tdLog.exit(f"SLEEP(2) elapsed {elapsed:.3f}s, expected ~2s")
        tdLog.info(f"SLEEP(2) elapsed {elapsed:.3f}s, passed")

    def test_sleep_in_expression(self):
        """Fun: sleep() used in expressions

        1. SELECT SLEEP(0) + 1 should return 1
        2. SELECT SLEEP(-1) + 1 should return 2
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
        tdSql.checkData(0, 0, 2)

        tdSql.query("SELECT SLEEP(0) = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def test_sleep_killed(self):
        """Fun: sleep() interrupted by KILL QUERY returns 1

        1. SELECT SLEEP(30) constant — runs server-side (not folded, VOLATILE_FUNC)
        2. SELECT SLEEP(v) FROM t   — column ref, runs server-side
        Both should appear in SHOW QUERIES and return 1 when killed.

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common

        Jira: None

        History:
            - 2026-4-7 Created

        """
        kill_db = "test_sleep_kill"
        tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")
        tdSql.execute(f"CREATE DATABASE {kill_db}")
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
                tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")
                tdLog.exit(f"{marker} did not appear in performance_schema.perf_queries within 5s")

            tdLog.info(f"killing query id {query_id} for: {marker}")
            tdSql.execute(f"KILL QUERY '{query_id}'")

            t.join(timeout=6)
            if t.is_alive():
                tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")
                tdLog.exit(f"{marker} was not killed within 6s after KILL QUERY")

            if "err" in error_holder:
                if "killed" in error_holder["err"].lower() or "cancel" in error_holder["err"].lower():
                    tdLog.info(f"{marker} killed via exception: {error_holder['err']}, passed")
                    continue
                tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")
                tdLog.exit(f"{marker} thread raised unexpected error: {error_holder['err']}")

            if result_holder.get("value") != 1:
                tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")
                tdLog.exit(f"{marker} killed query returned {result_holder.get('value')}, expected 1")

            tdLog.info(f"{marker} killed, returned {result_holder['value']}, passed")

        tdSql.execute(f"DROP DATABASE IF EXISTS {kill_db}")

    def test_sleep_column_once_per_query(self):
        """Fun: sleep() with column argument sleeps once per query

        1. Three rows with v = 1; SELECT SLEEP(v), v FROM t1 should sleep ~1s total
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
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, 1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 1)")

        start = time.time()
        tdSql.query("SELECT SLEEP(v), v FROM t1")
        elapsed = time.time() - start
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        if elapsed < 0.9 or elapsed > 2.5:
            tdLog.exit(f"SLEEP(v) with 3 rows elapsed {elapsed:.3f}s, expected ~1s (once per query)")
        tdLog.info(f"SLEEP(v) column arg elapsed {elapsed:.3f}s, passed")

        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

    def test_sleep_column_with_nulls(self):
        """Fun: sleep() with column argument produces per-row NULL for NULL inputs

        1. Rows: v = [1, NULL, 2]; SELECT SLEEP(v), v FROM t1
        2. Row 0 (v=1): returns 0 after sleeping ~1s
        3. Row 1 (v=NULL): returns NULL
        4. Row 2 (v=2): returns 0 without sleeping (sleep already done)
        5. Total elapsed ~1s (sleep fires once, not twice)

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-7 Created

        """
        db = "test_sleep_null_col"
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW, 1)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 1s, NULL)")
        tdSql.execute("INSERT INTO t1 VALUES(NOW + 2s, 2)")

        start = time.time()
        tdSql.query("SELECT SLEEP(v), v FROM t1")
        elapsed = time.time() - start
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 0)
        if elapsed < 0.9 or elapsed > 2.5:
            tdLog.exit(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, expected ~1s")
        tdLog.info(f"SLEEP(v) with NULL row elapsed {elapsed:.3f}s, passed")

        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

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
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        tdSql.execute("CREATE TABLE t1 USING st TAGS(1)")

        tdSql.query("SELECT SLEEP(v) FROM t1")
        tdSql.checkRows(0)
        tdLog.info("SLEEP(v) on empty table returned 0 rows, passed")

        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

    def test_sleep_not_pushed_to_vnode(self):
        """Fun: sleep() is not pushed down to vnode sub-plans (coordinator-only)

        Creates a stable spanning 10 vgroups with one row per vgroup. If SLEEP
        were pushed down and executed per vnode, SELECT SLEEP(1) would take
        ~10s total. With FUNC_MGT_NO_PUSHDOWN_FUNC ensuring coordinator-only
        execution it sleeps exactly once and finishes in ~1s.

        The 10x gap between the two cases makes the timing check reliable
        despite scheduling jitter.

        Catalog:
            - Functions:System

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-7 Created

        """
        db = "test_sleep_no_pushdown"
        vgroups = 10
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        # TDengine DROP DATABASE is async. With 10 vgroups + ASAN the physical
        # cleanup can take well over 30 s. Wait up to 120 s for the row to
        # vanish from ins_databases before attempting CREATE DATABASE.
        for _ in range(120):
            tdSql.query(f"SELECT name FROM information_schema.ins_databases WHERE name='{db}'")
            if tdSql.queryRows == 0:
                break
            time.sleep(1)
        # CREATE DATABASE silently no-ops (returns 0) when the old database is
        # still in SDB_STATUS_DROPPING. Retry until ins_databases shows the new
        # DB with status='ready'.
        for _ in range(120):
            try:
                tdSql.execute(f"CREATE DATABASE {db} vgroups {vgroups}")
            except Exception:
                pass
            tdSql.query(
                f"SELECT status FROM information_schema.ins_databases WHERE name='{db}'"
            )
            if tdSql.queryRows > 0 and str(tdSql.queryResult[0][0]).strip() == "ready":
                break
            time.sleep(1)
        tdSql.execute(f"USE {db}")
        tdSql.execute("CREATE STABLE st (ts TIMESTAMP, v INT) TAGS (t INT)")
        for i in range(vgroups):
            tdSql.execute(f"CREATE TABLE t{i} USING st TAGS({i})")
            tdSql.execute(f"INSERT INTO t{i} VALUES(NOW + {i}s, {i})")

        # Log the query plan for diagnostics (EXPLAIN does not expose function
        # names in expressions, so the timing below is the actual assertion)
        tdSql.query("EXPLAIN VERBOSE TRUE SELECT SLEEP(1), v FROM st")
        plan_lines = [str(tdSql.queryResult[i][0]) for i in range(tdSql.queryRows)]
        tdLog.info("EXPLAIN output:\n" + "\n".join(plan_lines))

        # If SLEEP is pushed to each of the 10 vnodes it would take ~10s.
        # Coordinator-only execution sleeps once: ~1s.
        # Upper bound of 3s gives ample room for scheduling overhead while
        # still being far below the 10s failure case.
        start = time.time()
        tdSql.query("SELECT SLEEP(1), v FROM st")
        elapsed = time.time() - start
        tdSql.checkRows(vgroups)
        if elapsed < 0.9 or elapsed > 3.0:
            tdLog.exit(
                f"SLEEP(1) on {vgroups}-vgroup stable elapsed {elapsed:.3f}s, "
                f"expected ~1s (if >3s SLEEP was pushed to vnodes)"
            )
        tdLog.info(f"SLEEP(1) on {vgroups}-vgroup stable elapsed {elapsed:.3f}s, passed")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
