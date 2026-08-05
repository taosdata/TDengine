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

"""Test STMT2 multi-table restore path (INSERT INTO ? VALUES ...).

When taosdump restores a super table with many child tables using the
default (multi-table) binding mode, the bug manifests as:

  - Server-side decode failure: nColData doubles (e.g. 3 columns → 6)
  - Error: "stmt2 multi-table exec failed ... Invalid parameters"

The root cause is in clientStmt2.c:stmtCleanExecInfo's keepTable path,
where a shallow copy from qCloneCurrentTbData leaves aRowP aliases
that dangle after the source is destroyed next cycle.

The non-default timestamp column name ("myts") also exercises the
path where _c0 pseudo-column resolution is required.
"""

import os
from new_test_framework.utils import tdLog, tdSql, etool


class TestTaosBackupStmt2MultiTable:
    """Regression test for multi-table STMT2 bind bug in taosdump restore."""

    # ----------------------------------------------------------------
    # Configuration: enough child tables to reliably trigger the
    # multi-table STMT2 batch path (STMT2_MULTI_TABLE_PENDING = 64).
    # Each child table has 3 columns to keep the test fast.
    # ----------------------------------------------------------------
    # 10 large tables (>16384 rows, single-table path) + 5 small (multi-table)
    NUM_LARGE = 10
    ROWS_LARGE = 20000
    NUM_SMALL = 5
    ROWS_SMALL = 500
    NUM_CTBS = NUM_LARGE + NUM_SMALL
    DB_NAME = "stmt2mt_db"
    STB_NAME = "meters"
    TS_COL = "mine_ts_col"

    def initData(self):
        tdSql.execute("drop database if exists %s" % self.DB_NAME)
        tdSql.execute("create database %s vgroups 1" % self.DB_NAME)
        tdSql.execute("use %s" % self.DB_NAME)

        tdSql.execute(
            "create table %s (%s timestamp, val bigint unsigned, quality smallint) "
            "tags(tagname nchar(100), deviceid nchar(100))"
            % (self.STB_NAME, self.TS_COL)
        )

        # Create all child tables + insert data
        # Large tables: batch multi-row INSERT for speed
        BATCH = 20
        for i in range(self.NUM_CTBS):
            ctb = "ct%d" % i
            tdSql.execute(
                "create table %s using %s tags('tag_%d', 'dev_%d')"
                % (ctb, self.STB_NAME, i, i)
            )
            nrows = self.ROWS_LARGE if i < self.NUM_LARGE else self.ROWS_SMALL
            base = 1700000000000 + i * 1000
            for b in range(0, nrows, BATCH):
                end = min(b + BATCH, nrows)
                rows = ",".join(
                    "(%d, %d, %d)" % (base + r, i * 100 + r, r % 3)
                    for r in range(b, end)
                )
                tdSql.execute("insert into %s values %s" % (ctb, rows))

        tdLog.info("setup: %d large + %d small tables in db %s" %
                   (self.NUM_LARGE, self.NUM_SMALL, self.DB_NAME))

    # ----------------------------------------------------------------
    # test_taosbackup_stmt2_multi_table (pytest entry point)
    # ----------------------------------------------------------------
    def test_taosbackup_stmt2_multi_table(self):
        """Regression: nColData doubling in initTableColSubmitData.

        When taosdump restores a super table whose child tables have
        mixed row counts, large files (>16384 rows) go through the
        single-table path while small files trigger the multi-table
        bind path.  When these paths alternate within the same
        thread, the keepTable context reuse causes aCol entries to
        accumulate without being cleared, doubling nColData from 3 to
        6 and producing a server-side "Invalid parameters" error.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-04 Alex Duan Created
        """
        self.do_taosbackup_stmt2_multi_table()

    # ----------------------------------------------------------------
    # do_taosbackup_stmt2_multi_table
    # ----------------------------------------------------------------
    def do_taosbackup_stmt2_multi_table(self):
        """taosdump export → import with multi-file data per table.

        With ROWS_PER_CTB=5000, taosdump produces 2+ .dat files per
        child table.  During restore the same table's context is reused
        across exec cycles (keepTable path), triggering the nColData
        doubling bug in initTableColSubmitData on unfixed builds.
        """

        tmpdir = "./taosbackuptest/tmpdir_stmt2_mt"
        self.initData()

        # ---- Phase 1: export ----
        etool.taosdump("-D %s -o %s -T 1" % (self.DB_NAME, tmpdir))

        # ---- Phase 2: drop and restore ----
        tdSql.execute("drop database %s" % self.DB_NAME)
        etool.taosdump("-i %s -T 2" % tmpdir)

        # ---- Phase 3: spot-check data integrity ----
        tdSql.execute("use %s" % self.DB_NAME)
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(self.NUM_CTBS)
        tdSql.query("select count(*) from %s" % self.STB_NAME)
        total = self.NUM_LARGE * self.ROWS_LARGE + self.NUM_SMALL * self.ROWS_SMALL
        tdSql.checkData(0, 0, total)

        tdLog.info(
            "do_taosbackup_stmt2_multi_table .................... [passed]"
        )


