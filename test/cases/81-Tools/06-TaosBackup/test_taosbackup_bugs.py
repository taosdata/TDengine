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

from new_test_framework.utils import tdLog, tdSql, etool
import os


class TestTaosBackupBugs:
    """Regression tests for known taosBackup bugs."""

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _prepare_db(self, time_unit="", precision=""):
        """Create database `db` with 3 normal tables, each containing 5 rows."""
        tdSql.execute("drop database if exists db")
        pu = f"precision '{time_unit}'" if time_unit else ""
        tdSql.execute(f"create database db keep 3649 {pu}")
        tdSql.execute("use db")
        for tbl in ("t1", "t2", "t3"):
            tdSql.execute(f"create table {tbl} (ts timestamp, n int)")
        for tbl in ("t1", "t2", "t3"):
            tdSql.execute(
                f"insert into {tbl} "
                f"values('2023-02-28 12:00:00.{precision}', 11)"
                f"      ('2023-02-28 12:00:01.{precision}', 12)"
                f"      ('2023-02-28 12:00:02.{precision}', 15)"
                f"      ('2023-02-28 12:00:03.{precision}', 16)"
                f"      ('2023-02-28 12:00:04.{precision}', 17)"
            )

    def _reset_tmpdir(self):
        if os.path.exists(self.tmpdir):
            os.system("rm -rf %s" % self.tmpdir)
        os.makedirs(self.tmpdir)

    # -----------------------------------------------------------------------
    # TS-2769 / TS-7053  – start/end time with precision variants
    # -----------------------------------------------------------------------
    #
    # Five rows with timestamps 00 … 04.  Selecting [01, 03] should return 3
    # rows; selecting from 01 onward → 4; selecting up to 03 → 4.
    #

    def _check_start_end_time(self, precision_str, time_unit):
        """Run the start/end time tests for a given precision string and unit."""
        self._prepare_db(time_unit, precision_str)
        binPath = self.binPath

        # --- t1: dump rows where ts IN [01.xxx, 03.xxx] (inclusive) → 3 rows ---
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t1 -o {self.tmpdir} -T 1 "
            f"-S 2023-02-28T12:00:01.{precision_str}+0800 "
            f"-E 2023-02-28T12:00:03.{precision_str}+0800"
        )
        tdSql.execute("drop table t1")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 3)

        # --- t2: dump rows where ts >= 01.xxx → 4 rows ---
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t2 -o {self.tmpdir} -T 1 "
            f"-S 2023-02-28T12:00:01.{precision_str}+0800"
        )
        tdSql.execute("drop table t2")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 4)

        # --- t3: dump rows where ts <= 03.xxx → 4 rows ---
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t3 -o {self.tmpdir} -T 1 "
            f"-E 2023-02-28T12:00:03.{precision_str}+0800"
        )
        tdSql.execute("drop table t3")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)

        tdLog.info(
            f"  start/end time [{time_unit if time_unit else 'ms'}] .......... [passed]"
        )

    def do_taosbackup_start_end_time(self):
        """Bug TS-7053 – -S/-E correctness across all timestamp precisions."""
        self._check_start_end_time("997000000", "ns")
        self._check_start_end_time("997000", "us")
        self._check_start_end_time("997", "ms")
        self._check_start_end_time("0", "")          # default (ms)

        tdLog.info("do_taosbackup_start_end_time ................. [passed]")

    # -----------------------------------------------------------------------
    # TS-2769 – long-form --start-time / --end-time arguments
    # -----------------------------------------------------------------------

    def do_taosbackup_start_end_time_long(self):
        """Bug TS-2769 – verify that --start-time / --end-time long options work."""
        binPath = self.binPath

        # Prepare a fresh ms-precision database
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        for tbl in ("t1", "t2", "t3"):
            tdSql.execute(f"create table {tbl} (ts timestamp, n int)")
        for tbl in ("t1", "t2", "t3"):
            tdSql.execute(
                f"insert into {tbl} "
                "values('2023-02-28 12:00:00.997', 11)"
                "      ('2023-02-28 12:00:01.997', 12)"
                "      ('2023-02-28 12:00:02.997', 15)"
                "      ('2023-02-28 12:00:03.997', 16)"
                "      ('2023-02-28 12:00:04.997', 17)"
            )

        # t1: [01.997, 03.997] → 3 rows
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t1 -o {self.tmpdir} -T 1 "
            "--start-time=2023-02-28T12:00:01.997+0800 "
            "--end-time=2023-02-28T12:00:03.997+0800"
        )
        tdSql.execute("drop table t1")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 3)

        # t2: from 01.997 onward → 4 rows
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t2 -o {self.tmpdir} -T 1 "
            "--start-time=2023-02-28T12:00:01.997+0800"
        )
        tdSql.execute("drop table t2")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 4)

        # t3: up to 03.997 → 4 rows
        self._reset_tmpdir()
        os.system(
            f"{binPath} db t3 -o {self.tmpdir} -T 1 "
            "--end-time=2023-02-28T12:00:03.997+0800"
        )
        tdSql.execute("drop table t3")
        os.system(f"{binPath} -i {self.tmpdir} -T 1")
        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)

        tdLog.info("do_taosbackup_start_end_time_long ............ [passed]")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_bugs(self):
        """taosBackup bugs

        1. Verify bug TS-7053: -S/-E start-time/end-time with all timestamp precisions
           (nanosecond, microsecond, millisecond, default)
        2. Verify bug TS-2769: --start-time / --end-time long-form arguments work correctly
        Note: do_taosdump_escaped_db (TS-3072, -R -e flags) is NOT applicable to taosBackup
        because REST mode (-R) and escape mode (-e) were removed in the taosBackup refactor.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-2769, TS-7053

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_bugs.py
              Removed do_taosdump_escaped_db (uses -R/-e flags not available in taosBackup)

        """
        self.binPath = etool.taosBackupFile()
        if self.binPath == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found: %s" % self.binPath)

        self.tmpdir = "./taosbackuptest/tmpdir_bugs"

        self.do_taosbackup_start_end_time()
        self.do_taosbackup_start_end_time_long()

        # Cleanup
        os.system("rm -rf ./taosbackuptest/")
        tdLog.info("test_taosbackup_bugs ......................... [passed]")
