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


class TestTaosBackupSchemaChange:
    """Test taosBackup behavior when the target database schema differs from the backup."""

    # -----------------------------------------------------------------------
    # Utility helpers
    # -----------------------------------------------------------------------

    def createDir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            self.clearPath(path)

    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)

    def taosbackup(self, cmd, show=True):
        """Run taosBackup with the given arguments and return the output lines."""
        return etool.taosbackup(cmd, show=show)

    def benchmark(self, command):
        """Run taosBenchmark with the given arguments."""
        benchmarkFile = etool.benchMarkFile()
        os.system(f"{benchmarkFile} {command}")

    def checkManyString(self, rlist, results):
        """Assert that every expected string in `results` appears somewhere in `rlist`."""
        if rlist is None:
            tdLog.exit("taosbackup returned None output list")
        combined = "\n".join(str(line) for line in rlist)
        for expected in results:
            if expected not in combined:
                tdLog.exit(
                    f"Expected string not found in taosBackup output:\n"
                    f"  expected: {expected!r}\n"
                    f"  output  : {combined[:500]!r}"
                )
            else:
                tdLog.info(f"  found expected string: {expected!r}")

    def checkSameResult(self, sql1, sql2):
        """Run both SQL statements and compare their results row-by-row."""
        res1 = tdSql.getResult(sql1)
        res2 = tdSql.getResult(sql2)
        if res1 == res2:
            tdLog.info(f"Results match: {sql1!r}")
        else:
            tdLog.exit(
                f"Results differ!\n  sql1={sql1!r} -> {res1}\n  sql2={sql2!r} -> {res2}"
            )

    # -----------------------------------------------------------------------
    # Data setup
    # -----------------------------------------------------------------------

    def insertData(self):
        self.benchmark(f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChange.json")
        self.benchmark(f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json")

    # -----------------------------------------------------------------------
    # Whole-database dump/import
    # -----------------------------------------------------------------------

    def dumpOut(self, db, tmpdir):
        cmd = f"-D {db} -o {tmpdir}"
        rlist = self.taosbackup(cmd)
        results = [
            "OK: total 1 table(s) of stable: meters1 schema dumped.",
            "OK: total 20 table(s) of stable: meters2 schema dumped.",
            "OK: total 30 table(s) of stable: meters3 schema dumped.",
            "OK: 9132 row(s) dumped out!",
        ]
        self.checkManyString(rlist, results)

    def dumpIn(self, db, newdb, tmpdir):
        cmd = f'-W "{db}={newdb}" -i {tmpdir}'
        rlist = self.taosbackup(cmd)
        results = [
            f"rename DB Name {db} to {newdb}",
            "OK: 9132 row(s) dumped in!",
        ]
        self.checkManyString(rlist, results)

    # -----------------------------------------------------------------------
    # Correctness verification
    # -----------------------------------------------------------------------

    def checkCorrectStb(self, db, newdb):
        col_sqls = [
            # meters1
            (f"select ts from {db}.meters1", f"select ts from {newdb}.meters1"),
            (f"select sum(fc) from {db}.meters1", f"select sum(fc) from {newdb}.meters1"),
            (f"select avg(ic) from {db}.meters1", f"select avg(ic) from {newdb}.meters1"),
            (f"select bin from {db}.meters1", f"select bin from {newdb}.meters1"),
            # meters2
            (f"select ts from {db}.meters2", f"select ts from {newdb}.meters2"),
            (f"select sum(bi) from {db}.meters2", f"select sum(bi) from {newdb}.meters2"),
            (f"select avg(ui) from {db}.meters2", f"select avg(ui) from {newdb}.meters2"),
            (f"select bi from {db}.meters2", f"select bi from {newdb}.meters2"),
            # meters3
            (f"select ts from {db}.meters3", f"select ts from {newdb}.meters3"),
            (f"select sum(ti) from {db}.meters3", f"select sum(ti) from {newdb}.meters3"),
            (f"select avg(ui) from {db}.meters3", f"select avg(ui) from {newdb}.meters3"),
            (f"select bc from {db}.meters3", f"select bc from {newdb}.meters3"),
            # meters4
            (f"select ts from {db}.meters4", f"select ts from {newdb}.meters4"),
            (f"select sum(ti) from {db}.meters4", f"select sum(ti) from {newdb}.meters4"),
            (
                f"select count(bc) from {db}.meters4 where bc=1",
                f"select count(bc) from {newdb}.meters4 where bc=1",
            ),
            (f"select bin from {db}.meters4", f"select bin from {newdb}.meters4"),
        ]
        for sql1, sql2 in col_sqls:
            self.checkSameResult(sql1, sql2)

        # new cols should be NULL in imported database
        tdSql.checkAgg(
            f"select count(*) from {newdb}.meters3 where newic is null", 3000
        )

        tag_sqls = [
            (
                f"select distinct tti,tbi,tuti,tusi,tbin,tic,tbname from {db}.meters1 order by tbname;",
                f"select distinct tti,tbi,tuti,tusi,tbin,tic,tbname from {newdb}.meters1 order by tbname;",
            ),
            (
                f"select distinct tti,tbi,tuti,tusi,tbin,tbname from {db}.meters2 order by tbname;",
                f"select distinct tti,tbi,tuti,tusi,tbin,tbname from {newdb}.meters2 order by tbname;",
            ),
        ]
        for sql1, sql2 in tag_sqls:
            self.checkSameResult(sql1, sql2)

        tdSql.checkAgg(
            f"select count(*) from {newdb}.meters1 where newtti is null", 100
        )
        tdSql.checkAgg(
            f"select count(*) from {newdb}.meters3 where newtdc is null", 2000
        )

    def checkCorrectNtb(self, db, newdb):
        ntb_sqls = [
            (
                f"select ts, c1, c2, c3, c4 from {db}.ntbd1",
                f"select ts, c1, c2, c3, c4 from {newdb}.ntbd1",
            ),
            (
                f"select ts, d1, d2, d3 from {db}.ntbd2",
                f"select ts, d1, d2, d3 from {newdb}.ntbd2",
            ),
            (
                f"select ts, c1, c4 from {db}.ntbe1",
                f"select ts, c1, c4 from {newdb}.ntbe1",
            ),
            (f"select ts, d2 from {db}.ntbe2", f"select ts, d2 from {newdb}.ntbe2"),
            (
                f"select ts, c1, c3 from {db}.ntbf1",
                f"select ts, c1, c3 from {newdb}.ntbf1",
            ),
            (f"select ts, d3 from {db}.ntbf2", f"select ts, d3 from {newdb}.ntbf2"),
        ]
        for sql1, sql2 in ntb_sqls:
            self.checkSameResult(sql1, sql2)

    def checkCorrect(self, db, newdb):
        self.checkCorrectStb(db, newdb)
        self.checkCorrectNtb(db, newdb)

    # -----------------------------------------------------------------------
    # Specify-table dump/import
    # -----------------------------------------------------------------------

    def clearEvn(self, newdb, tmpdir):
        self.clearPath(tmpdir)
        self.benchmark(
            f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        )

    def dumpOutSpecify(self, db, tmpdir):
        cmd = f"-o {tmpdir} {db} d0 meters2 meters3 meters4 ntbd1 ntbd2 ntbe1 ntbe2 ntbf1 ntbf2 ntbg1 ntbg2"
        rlist = self.taosbackup(cmd)
        results = [
            "OK: total 20 table(s) of stable: meters2 schema dumped.",
            "OK: total 30 table(s) of stable: meters3 schema dumped.",
            "OK: total 40 table(s) of stable: meters4 schema dumped.",
            "OK: 9132 row(s) dumped out!",
        ]
        self.checkManyString(rlist, results)

    # -----------------------------------------------------------------------
    # Exception: schema mismatch
    # -----------------------------------------------------------------------

    def exceptNoSameCol(self, db, newdb, tmpdir):
        """Test exception handling when target tables have incompatible schemas."""
        # Prepare newdb with schema changes
        self.benchmark(
            f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        )

        # Drop and recreate meters2/meters3 with incompatible schemas
        sqls = [
            f"drop table {newdb}.meters2",
            f"create table {newdb}.meters2(nts timestamp, age int) tags(area int)",
            f"drop table {newdb}.meters3",
            f"create table {newdb}.meters3(ts timestamp, fc float) tags(area int)",
        ]
        tdSql.executes(sqls)

        cmd = f'-W "{db}={newdb}" -i {tmpdir}'
        rlist = self.taosbackup(cmd)
        results = [
            f"rename DB Name {db} to {newdb}",
            "backup data schema no same column with server table",
            "new tag zero failed! oldt=",
            "50 failures occurred to dump in",
            "OK: 4132 row(s) dumped in!",
        ]
        self.checkManyString(rlist, results)
        tdLog.info("check except no same column .................. [OK]")

    def testExcept(self, db, newdb, tmpdir):
        self.exceptNoSameCol(db, newdb, tmpdir)

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_schema_change(self):
        """taosBackup schema change

        1.  Prepare data with taosBenchmark using schemaChange.json and schemaChangeNew.json
        2.  Dump out entire database; verify table/row counts in output
        3.  Import into new database with rename (-W); verify row count and message
        4.  Verify imported data matches source: STB columns, tags, NTB columns
        5.  Verify NULLs for newly added columns / tags that existed only in source
        6.  Re-run dump with specific table arguments; verify counts
        7.  Re-import into renamed database; verify correctness again
        8.  Test exception: alter target schema to be incompatible before import
        9.  Verify error messages about schema mismatch and partial success row count

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_schema_change.py

        """
        db = "dd"
        newdb = "newdd"

        tmpdir = "./tmp"
        self.createDir(tmpdir)

        # Insert test data
        self.insertData()

        # --- whole database dump + import ---
        self.dumpOut(db, tmpdir)
        self.dumpIn(db, newdb, tmpdir)
        self.checkCorrect(db, newdb)

        # --- specific table dump + import ---
        self.clearEvn(newdb, tmpdir)
        self.dumpOutSpecify(db, tmpdir)
        self.dumpIn(db, newdb, tmpdir)
        self.checkCorrect(db, newdb)

        # --- exception: incompatible schema ---
        self.testExcept(db, newdb, tmpdir)
