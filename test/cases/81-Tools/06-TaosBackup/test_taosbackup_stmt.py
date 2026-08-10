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

Test includes varchar column type and full data verification:
  - Restore to a different DB via -W and compare row counts + numeric sums.
"""

import os
from new_test_framework.utils import tdLog, tdSql, etool


class TestTaosBackupStmt2MultiTable:
    """Regression test for multi-table STMT2 bind bug in taosdump restore."""

    # ----------------------------------------------------------------
    # Configuration: enough child tables to reliably trigger the
    # multi-table STMT2 batch path (STMT2_MULTI_TABLE_PENDING = 64).
    # Each child table has 4 columns (timestamp + val + quality + vc).
    # ----------------------------------------------------------------
    # 10 large tables (>16384 rows, single-table path) + 5 small (multi-table)
    NUM_LARGE = 10
    ROWS_LARGE = 20000
    NUM_SMALL = 5
    ROWS_SMALL = 500
    NUM_CTBS = NUM_LARGE + NUM_SMALL
    DB_NAME = "stmt2mt_db"
    NEW_DB_NAME = "stmt2mt_db_restored"
    STB_NAME = "meters"
    TS_COL = "mine_ts_col"

    def initData(self):
        tdSql.execute("drop database if exists %s" % self.DB_NAME)
        tdSql.execute("drop database if exists %s" % self.NEW_DB_NAME)
        tdSql.execute("create database %s vgroups 1" % self.DB_NAME)
        tdSql.execute("use %s" % self.DB_NAME)

        tdSql.execute(
            "create table %s (%s timestamp, val bigint unsigned, quality smallint, vc varchar(100)) "
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
                    "(%d, %d, %d, 'vc_r_%d')" % (base + r, i * 100 + r, r % 3, r)
                    for r in range(b, end)
                )
                tdSql.execute("insert into %s values %s" % (ctb, rows))

        tdLog.info("setup: %d large + %d small tables in db %s" %
                   (self.NUM_LARGE, self.NUM_SMALL, self.DB_NAME))

    # ----------------------------------------------------------------
    # Helper: compare two DBs on the same STB
    # ----------------------------------------------------------------
    def _check_same(self, db_src, db_dst, stb, aggfun):
        """Compare aggregate result of `aggfun` across two databases."""
        tdSql.query("select %s from %s.%s" % (aggfun, db_src, stb))
        val_src = tdSql.getData(0, 0)
        tdSql.query("select %s from %s.%s" % (aggfun, db_dst, stb))
        val_dst = tdSql.getData(0, 0)
        if val_src == val_dst:
            tdLog.info("%s  %s: src=%s  dst=%s  [match]" % (aggfun, stb, val_src, val_dst))
        else:
            tdLog.exit("%s  %s: src=%s  dst=%s  [MISMATCH!]" % (aggfun, stb, val_src, val_dst))

    def verifyRestore(self):
        """After restoring to NEW_DB_NAME via -W, compare with original DB."""
        tdLog.info("=== Verifying restore: %s vs %s ===" % (self.DB_NAME, self.NEW_DB_NAME))

        # 1) Row count
        self._check_same(self.DB_NAME, self.NEW_DB_NAME, self.STB_NAME, "count(*)")

        # 2) Numeric column sums
        self._check_same(self.DB_NAME, self.NEW_DB_NAME, self.STB_NAME, "sum(val)")
        self._check_same(self.DB_NAME, self.NEW_DB_NAME, self.STB_NAME, "sum(quality)")

        # 3) Child table count
        tdSql.query("select count(*) from %s.%s" % (self.NEW_DB_NAME, self.STB_NAME))
        restored_rows = tdSql.getData(0, 0)
        expected_total = self.NUM_LARGE * self.ROWS_LARGE + self.NUM_SMALL * self.ROWS_SMALL
        if restored_rows == expected_total:
            tdLog.info("restored row count %d == expected %d  [match]" % (restored_rows, expected_total))
        else:
            tdLog.exit("restored row count %d != expected %d  [MISMATCH!]" % (restored_rows, expected_total))

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

        Includes varchar column and full -W restore verification.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-04 Alex Duan Created
            - 2026-08-05 Added varchar column + full -W verification
        """
        self.do_taosbackup_stmt2_multi_table()

    # ----------------------------------------------------------------
    # do_taosbackup_stmt2_multi_table
    # ----------------------------------------------------------------
    def do_taosbackup_stmt2_multi_table(self):
        """taosdump export → restore to different DB via -W → verify.

        With ROWS_PER_CTB=5000, taosdump produces 2+ .dat files per
        child table.  During restore the same table's context is reused
        across exec cycles (keepTable path), triggering the nColData
        doubling bug in initTableColSubmitData on unfixed builds.

        Uses -W to restore to a new database name and then compares
        row counts and numeric column sums between original and restored.
        """

        tmpdir = "./taosbackuptest/tmpdir_stmt2_mt"
        self.initData()

        # ---- Phase 1: export ----
        etool.taosdump("-D %s -o %s -T 1" % (self.DB_NAME, tmpdir))

        # ---- Phase 2: restore to a DIFFERENT database via -W ----
        etool.taosdump('-W "%s=%s" -i %s -T 2' % (self.DB_NAME, self.NEW_DB_NAME, tmpdir))

        # ---- Phase 3: full verification: original vs restored ----
        self.verifyRestore()

        tdLog.info(
            "do_taosbackup_stmt2_multi_table .................... [passed]"
        )



