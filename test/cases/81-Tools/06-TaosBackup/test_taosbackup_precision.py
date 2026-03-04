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


class TestTaosBackupPrecision:
    """Test taosBackup with nanosecond, microsecond, and millisecond precision databases."""

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _make_dirs(self, *paths):
        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)
            else:
                os.system("rm -rf %s/*" % path)

    def createdb(self, precision="ns"):
        """Create test database with given precision and insert 100 rows into 10 tables."""
        tb_nums = self.numberOfTables
        per_tb_rows = self.numberOfRecords

        if precision == "ns":
            start_time = 1625068800000000000
            ts_seed = 1000000000
        elif precision == "us":
            start_time = 1625068800000000
            ts_seed = 1000000
        elif precision == "ms":
            start_time = 1625068800000
            ts_seed = 1000
        else:
            tdLog.exit(f"Unsupported precision: {precision}")
            return

        tdSql.execute("drop database if exists timedb1")
        tdSql.execute(
            f'create database timedb1 duration 10 keep 36500 precision "{precision}"'
        )
        tdSql.execute("use timedb1")
        tdSql.execute(
            "create stable st(ts timestamp, c1 int, c2 nchar(10), c3 timestamp)"
            " tags(t1 int, t2 binary(10))"
        )

        for tb in range(tb_nums):
            tbname = "t" + str(tb)
            tdSql.execute(f"create table {tbname} using st tags(1, 'beijing')")
            sql = f"insert into {tbname} values"
            currts = start_time
            for i in range(per_tb_rows):
                sql += "(%d, %d, 'nchar%d', %d)" % (
                    currts + i * ts_seed,
                    i % 100,
                    i % 100,
                    currts + i * 100,
                )
            tdSql.execute(sql)

    # -----------------------------------------------------------------------
    # Precision-specific dump / import helpers
    # -----------------------------------------------------------------------

    def _precision_test(self, precision):
        """
        Test dump/import for a given precision (ns / us / ms).

        Scenarios:
          dumptmp1 → all data (expect 1000 rows)
          dumptmp2 → rows in [start+10s, start+60s]  (expect 510 rows for ns/us, 51 for ms? — same ratio)
          dumptmp3 → rows from start+10s onward       (expect 900 rows)
        """
        if precision == "ns":
            start = 1625068800000000000
            s_time = 1625068810000000000
            e_time = 1625068860000000000
        elif precision == "us":
            start = 1625068800000000
            s_time = 1625068810000000
            e_time = 1625068860000000
        else:  # ms
            start = 1625068800000
            s_time = 1625068810000
            e_time = 1625068860000

        base = "./taosbackuptest"
        d1 = f"{base}/{precision}_dumptmp1"
        d2 = f"{base}/{precision}_dumptmp2"
        d3 = f"{base}/{precision}_dumptmp3"
        self._make_dirs(d1, d2, d3)

        binPath = self.binPath

        # Dump all data
        os.system(f"{binPath} -g --databases timedb1 -o {d1}")
        # Dump with start and end time filter
        os.system(f"{binPath} -g --databases timedb1 -S {s_time} -E {e_time} -o {d2}")
        # Dump with start time only
        os.system(f"{binPath} -g --databases timedb1 -S {s_time} -o {d3}")

        # Import dumptmp2 → 51 rows per table * 10 tables = 510
        tdSql.execute("drop database timedb1")
        os.system(f"{binPath} -i {d2}")
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 510)

        # Import dumptmp3 → 90 rows per table * 10 tables = 900
        tdSql.execute("drop database timedb1")
        os.system(f"{binPath} -i {d3}")
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 900)

        # Import dumptmp1 → all 1000 rows
        tdSql.execute("drop database timedb1")
        os.system(f"{binPath} -i {d1}")
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 1000)

        # Verify data integrity: re-import all and compare row-by-row
        origin_res = tdSql.getResult("select * from timedb1.st")
        tdSql.execute("drop database timedb1")
        os.system(f"{binPath} -i {d1}")
        dump_res = tdSql.getResult("select * from timedb1.st")
        if origin_res == dump_res:
            tdLog.info(f"  {precision} precision: data integrity check passed.")
        else:
            tdLog.exit(f"  {precision} precision: data integrity check FAILED.")

        tdLog.info(f"do_precision_{precision} ..................... [passed]")

    # -----------------------------------------------------------------------
    # Per-precision test drivers
    # -----------------------------------------------------------------------

    def do_precision_ns(self):
        self.numberOfTables = 10
        self.numberOfRecords = 100
        self.createdb("ns")
        self._precision_test("ns")
        os.system("rm -rf ./taosbackuptest/ns_*")

    def do_precision_us(self):
        self.numberOfTables = 10
        self.numberOfRecords = 100
        self.createdb("us")
        self._precision_test("us")
        os.system("rm -rf ./taosbackuptest/us_*")

    def do_precision_ms(self):
        self.numberOfTables = 10
        self.numberOfRecords = 100
        self.createdb("ms")
        self._precision_test("ms")
        os.system("rm -rf ./taosbackuptest/ms_*")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_precision(self):
        """taosBackup precision

        1.  Create database with nanosecond precision and 10 sub-tables (100 rows each)
        2.  Dump all data, verify 1000 rows after restore
        3.  Dump data within a specific time range using -S/-E, verify 510 rows after restore
        4.  Dump data from a start time using -S only, verify 900 rows after restore
        5.  Repeat above steps for microsecond precision database
        6.  Repeat above steps for millisecond precision database
        7.  Verify full data integrity (row-by-row comparison) for each precision

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_precision.py

        """
        self.binPath = etool.taosBackupFile()
        if self.binPath == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found: %s" % self.binPath)

        self.do_precision_ns()
        self.do_precision_us()
        self.do_precision_ms()

        # Cleanup
        os.system("rm -rf ./taosbackuptest/")
        tdLog.info("test_taosbackup_precision .................... [passed]")
