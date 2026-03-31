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

        Since: v3.4.0.9

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # SLEEP(1) should take ~1 second
        start = time.time()
        tdSql.query("SELECT SLEEP(1)")
        elapsed = time.time() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed < 0.9 or elapsed > 2.0:
            tdLog.exit(f"SLEEP(1) elapsed {elapsed:.3f}s, expected ~1s")
        tdLog.info(f"SLEEP(1) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0) should be instant
        start = time.time()
        tdSql.query("SELECT SLEEP(0)")
        elapsed = time.time() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(0) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(0) elapsed {elapsed:.3f}s, passed")

        # SLEEP(0.5) should take ~0.5 seconds
        start = time.time()
        tdSql.query("SELECT SLEEP(0.5)")
        elapsed = time.time() - start
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

        Since: v3.4.0.9

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

        start = time.time()
        tdSql.query("SELECT SLEEP(1), v FROM t1")
        elapsed = time.time() - start
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

        Since: v3.4.0.9

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-26 Created

        """
        # integer negative
        start = time.time()
        tdSql.query("SELECT SLEEP(-1)")
        elapsed = time.time() - start
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        if elapsed > 0.5:
            tdLog.exit(f"SLEEP(-1) elapsed {elapsed:.3f}s, expected instant")
        tdLog.info(f"SLEEP(-1) elapsed {elapsed:.3f}s, passed")

        # fractional negative
        start = time.time()
        tdSql.query("SELECT SLEEP(-0.5)")
        elapsed = time.time() - start
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

        Since: v3.4.0.9

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

        Since: v3.4.0.9

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

        Since: v3.4.0.9

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

        Since: v3.4.0.9

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
