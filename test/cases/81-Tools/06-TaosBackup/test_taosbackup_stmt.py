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
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool
from new_test_framework.utils.stmt2 import tdStmt2


class TestTaosBackupStmt2MultiTable:
    """Regression test for multi-table STMT2 bind bug in taosdump restore."""

    # ----------------------------------------------------------------
    # Configuration: enough child tables to reliably trigger the
    # multi-table STMT2 batch path (STMT2_MULTI_TABLE_PENDING = 64).
    # Each child table has 3 columns to keep the test fast.
    # ----------------------------------------------------------------
    NUM_CTBS = 100
    ROWS_PER_CTB = 3
    DB_NAME = "stmt2mt_db"
    STB_NAME = "meters"
    # Deliberately use "myts" (not "ts") to verify _c0 resolution.
    TS_COL = "primary_datatime_column_name"

    # ----------------------------------------------------------------
    # Setup: create a super table with a non-"ts" primary key and
    # many child tables with data.
    # ----------------------------------------------------------------
    def setup(self):
        tdSql.execute("drop database if exists %s" % self.DB_NAME)
        tdSql.execute("create database %s" % self.DB_NAME)
        tdSql.execute("use %s" % self.DB_NAME)

        # Super table: myts (TIMESTAMP, intentionally NOT named "ts"),
        #              v1 INT, v2 FLOAT, tag T1 INT.
        tdSql.execute(
            "create table %s (%s timestamp, v1 int, v2 float) tags(t1 int)"
            % (self.STB_NAME, self.TS_COL)
        )

        # Create NUM_CTBS child tables, each with 3 data rows,
        # distributed across 2 tag values for variety.
        for i in range(self.NUM_CTBS):
            ctb = "ct%d" % i
            tag_val = i % 2
            tdSql.execute(
                "create table %s using %s tags(%d)" % (ctb, self.STB_NAME, tag_val)
            )
            # Insert ROWS_PER_CTB rows per child table.
            # Timestamps start at base + i*1000 to keep them ordered.
            base = 1700000000000 + i * 1000
            rows = []
            for r in range(self.ROWS_PER_CTB):
                rows.append(
                    "(%d, %d, %.1f)" % (base + r, (i * 10 + r), float(i + r * 0.5))
                )
            tdSql.execute("insert into %s values %s" % (ctb, ",".join(rows)))

        tdLog.info(
            "setup: %d child tables × %d rows each created in db %s"
            % (self.NUM_CTBS, self.ROWS_PER_CTB, self.DB_NAME)
        )

    # ----------------------------------------------------------------
    # test_taosbackup_stmt2_multi_table (pytest entry point)
    # ----------------------------------------------------------------
    def test_taosbackup_stmt2_multi_table(self):
        """Test STMT2 multi-table bind nColData corruption fixes.

        Covers two paths:
          1. taosdump restore (INSERT INTO tb USING stb ...) —
             initTableColSubmitData / initTableColSubmitDataWithBoundInfo
             where nColData doubled across keepTable exec cycles.
          2. STMT2 INSERT INTO ? placeholder —
             parseStbBoundInfo path where initTableColSubmitData was
             missing entirely.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-04 Alex Duan Created
        """
        self.do_taosbackup_stmt2_multi_table()
        self.do_stmt2_placeholder_multi_table()

    # ----------------------------------------------------------------
    # do_taosbackup_stmt2_multi_table
    # ----------------------------------------------------------------
    def do_taosbackup_stmt2_multi_table(self):
        # Reproduce multi-table STMT2 bind bug during restore.
        
        tmpdir = "./taosbackuptest/tmpdir_stmt2_mt"

        self.setup()

        # ---- Phase 1: export ----        
        etool.taosdump("-D %s -o %s -T 1" % (self.DB_NAME, tmpdir))

        # ---- Phase 2: drop and restore ----
        tdSql.execute("drop database %s" % self.DB_NAME)

        # Restore WITHOUT -B (i.e. default multi-table mode).
        # The bug triggers here when >64 child tables are bound in one
        # STMT2 BINDV, producing corrupt submit data.
        etool.taosdump("-i %s -T 2" % tmpdir)

        # ---- Phase 3: verify ----
        tdSql.execute("use %s" % self.DB_NAME)

        # Super table exists
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.STB_NAME)

        # All child tables restored
        tdSql.query("show tables")
        tdSql.checkRows(self.NUM_CTBS)

        # Row count per child table should be preserved.
        # Spot-check: pick a few tables across the range.
        for idx in (0, self.NUM_CTBS // 2, self.NUM_CTBS - 1):
            ctb = "ct%d" % idx
            tdSql.query("select count(*) from %s" % ctb)
            tdSql.checkData(0, 0, self.ROWS_PER_CTB)

        # Total row count across all child tables.
        tdSql.query("select count(*) from %s" % self.STB_NAME)
        tdSql.checkData(0, 0, self.NUM_CTBS * self.ROWS_PER_CTB)

        # Data integrity: check first and last child table values.
        tdSql.query(
            "select %s, v1, v2 from ct0 order by %s" % (self.TS_COL, self.TS_COL)
        )
        tdSql.checkRows(self.ROWS_PER_CTB)
        tdSql.checkData(0, 1, 0)          # v1 = 0*10+0
        tdSql.checkData(0, 2, 0.0)        # v2 = 0+0*0.5

        last = self.NUM_CTBS - 1
        tdSql.query(
            "select %s, v1, v2 from ct%d order by %s" % (self.TS_COL, last, self.TS_COL)
        )
        tdSql.checkRows(self.ROWS_PER_CTB)
        tdSql.checkData(0, 1, last * 10 + 0)       # v1 = (last*10+0)
        tdSql.checkData(0, 2, float(last + 0 * 0.5))  # v2 = last+0*0.5

        tdLog.info(
            "do_taosbackup_stmt2_multi_table .................... [passed]"
        )

    # ----------------------------------------------------------------
    # do_stmt2_placeholder_multi_table
    # ----------------------------------------------------------------
    def do_stmt2_placeholder_multi_table(self):
        """INSERT INTO ? USING stb TAGS(...) VALUES(...) — multi-table batch."""

        db = "stmt2ph_db"
        stb = "dev"
        num_ctbs = 100
        rows_per_ctb = 3

        tdSql.execute("drop database if exists %s" % db)
        tdSql.execute("create database %s vgroups 1" % db)
        tdSql.execute("use %s" % db)

        # Super table: ts TIMESTAMP, v1 INT, v2 DOUBLE, tag t1 INT
        tdSql.execute(
            "create table %s (ts timestamp, v1 int, v2 double) tags(t1 int)" % stb
        )

        # Create child tables via SQL (no data yet — stmt2 will write)
        tbnames = []
        tags = []
        datas = []
        base_ts = int(time.time() * 1000)

        for i in range(num_ctbs):
            ctb = "d%d" % i
            tbnames.append(ctb)
            tdSql.execute(
                "create table %s using %s tags(%d)" % (ctb, stb, i % 2)
            )
            tags.append([i % 2])

            # Each child table gets rows_per_ctb rows
            tbl_data = []
            for r in range(rows_per_ctb):
                tbl_data.append([base_ts + i * 1000 + r, i * 10 + r, float(i + r * 0.5)])
            datas.append(tbl_data)

        # INSERT INTO ? USING dev TAGS(?) VALUES(?, ?, ?)
        # Table name '?' forces parseStbBoundInfo path
        sql = "INSERT INTO ? USING %s TAGS(?) VALUES(?, ?, ?)" % stb
        total_rows = num_ctbs * rows_per_ctb
        tdStmt2.execute_super_table(sql, tbnames, tags, datas, expected_rows=total_rows)

        # ---- verify ----
        tdSql.query("select count(*) from %s" % stb)
        tdSql.checkData(0, 0, total_rows)

        tdSql.query("select count(*) from d0")
        tdSql.checkData(0, 0, rows_per_ctb)

        tdSql.query(
            "select v1, v2 from d0 order by ts"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0.0)

        tdSql.query(
            "select v1, v2 from d%d order by ts" % (num_ctbs - 1)
        )
        last_idx = num_ctbs - 1
        tdSql.checkData(0, 0, last_idx * 10)
        tdSql.checkData(0, 1, float(last_idx))

        tdLog.info(
            "do_stmt2_placeholder_multi_table ................... [passed]"
        )
