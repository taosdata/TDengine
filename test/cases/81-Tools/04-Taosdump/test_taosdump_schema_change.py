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

class TestTaosdumpSchemaChange:
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def _datadir(self, subdir):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", subdir)

    def _prepare_dir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            os.system("rm -rf %s/*" % path)
            os.makedirs(path)

    def backupIn(self, db, newdb, tmpdir):
        """Import with taosBackup from taosdump avro data."""
        taosbackup = etool.taosDumpFile()
        self.exec(f'{taosbackup} -W "{db}={newdb}" -i {tmpdir}')

    def taosdump(self, cmd, show=True):
        """Unused — kept for reference only."""
        pass

    def benchmark(self, command):
        """Run taosBenchmark with the given arguments."""
        etool.benchmark(command)

    def checkManyString(self, rlist, results):
        """Assert that every expected string in results appears somewhere in rlist."""
        if rlist is None:
            tdLog.exit("taosdump returned None output list")
        combined = "\n".join(str(line) for line in rlist)
        for expected in results:
            if expected not in combined:
                tdLog.exit(
                    f"Expected string not found in taosdump output:\n"
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

    # insert
    def insertData(self):
        """Unused — pre-generated backup data is used instead."""
        pass

    # super table
    def checkCorrectStb(self, db, newdb):
        #
        # check column
        #
        sqls = [
            # meters1
            [
                f"select (ts) from    {db}.meters1", 
                f"select (ts) from {newdb}.meters1"
            ],
            [
                f"select sum(fc) from    {db}.meters1", 
                f"select sum(fc) from {newdb}.meters1"
            ],
            [
                f"select avg(ic) from    {db}.meters1", 
                f"select avg(ic) from {newdb}.meters1"
            ],
            [
                f"select bin from    {db}.meters1", 
                f"select bin from {newdb}.meters1"
            ],
            # meters2
            [
                f"select (ts) from    {db}.meters2", 
                f"select (ts) from {newdb}.meters2"
            ],
            [
                f"select sum(bi) from    {db}.meters2", 
                f"select sum(bi) from {newdb}.meters2"
            ],
            [
                f"select avg(ui) from    {db}.meters2", 
                f"select avg(ui) from {newdb}.meters2"
            ],
            [
                f"select (bi) from    {db}.meters2", 
                f"select (bi) from {newdb}.meters2"
            ],
            # meters3
            [
                f"select (ts) from    {db}.meters3", 
                f"select (ts) from {newdb}.meters3"
            ],
            [
                f"select sum(ti) from    {db}.meters3", 
                f"select sum(ti) from {newdb}.meters3"
            ],
            [
                f"select avg(ui) from    {db}.meters3", 
                f"select avg(ui) from {newdb}.meters3"
            ],
            [
                f"select (bc) from    {db}.meters3", 
                f"select (bc) from {newdb}.meters3"
            ],
            # meters4
            [
                f"select (ts) from    {db}.meters4", 
                f"select (ts) from {newdb}.meters4"
            ],
            [
                f"select sum(ti) from    {db}.meters4", 
                f"select sum(ti) from {newdb}.meters4"
            ],
            [
                f"select count(bc) from    {db}.meters4 where bc=1", 
                f"select count(bc) from {newdb}.meters4 where bc=1"
            ],
            [
                f"select (bin) from    {db}.meters4", 
                f"select (bin) from {newdb}.meters4"
            ]
        ]

        for sql in sqls:
            self.checkSameResult(sql[0], sql[1])

        # new cols is null
        sql = f"select count(*) from {newdb}.meters3 where newic is null"
        tdSql.checkAgg(sql, 3000)

        #
        # check tag
        #

        sqls = [
            [
                f"select distinct tti,tbi,tuti,tusi,tbin,tic,tbname from    {db}.meters1 order by tbname;", 
                f"select distinct tti,tbi,tuti,tusi,tbin,tic,tbname from {newdb}.meters1 order by tbname;"
            ],
            [
                f"select distinct tti,tbi,tuti,tusi,tbin,tbname from    {db}.meters2 order by tbname;", 
                f"select distinct tti,tbi,tuti,tusi,tbin,tbname from {newdb}.meters2 order by tbname;"
            ],
        ]

        for sql in sqls:
            self.checkSameResult(sql[0], sql[1])

        # new tag is null
        sql = f"select count(*) from {newdb}.meters1 where newtti is null"
        tdSql.checkAgg(sql, 100)

        sql = f"select count(*) from {newdb}.meters3 where newtdc is null"
        tdSql.checkAgg(sql, 2000)



    # normal table
    def checkCorrectNtb(self, db, newdb):
        sqls = [
            # meters1
            [
                f"select ts, c1, c2, c3, c4 from    {db}.ntbd1", 
                f"select ts, c1, c2, c3, c4 from {newdb}.ntbd1"
            ],
            [
                f"select ts, d1, d2, d3 from    {db}.ntbd2", 
                f"select ts, d1, d2, d3 from {newdb}.ntbd2"
            ],
            [
                f"select ts, c1, c4 from    {db}.ntbe1", 
                f"select ts, c1, c4 from {newdb}.ntbe1"
            ],
            [
                f"select ts, d2 from    {db}.ntbe2", 
                f"select ts, d2 from {newdb}.ntbe2"
            ],
            [
                f"select ts, c1, c3 from    {db}.ntbf1", 
                f"select ts, c1, c3 from {newdb}.ntbf1"
            ],
            [
                f"select ts, d3 from    {db}.ntbf2", 
                f"select ts, d3 from {newdb}.ntbf2"
            ]     
        ]

        for sql in sqls:
            self.checkSameResult(sql[0], sql[1])        

    # check correct
    def checkCorrect(self, db, newdb):
        # stb
        self.checkCorrectStb(db, newdb)
        # ntb
        self.checkCorrectNtb(db, newdb)

    def test_taosdump_schema_change(self):
        """taosdump schema change

        1.  Prepare data with taosBenchmark -f schemaChange.json/schemaChangeNew.json
        2.  Use taosBackup to import backup from pre-generated data/schemachange_full/
        3.  Verify that the imported data matches the original data
        4.  Use taosBackup to import backup from pre-generated data/schemachange_spec/
        5.  Verify that the imported data matches the original data for the specified tables

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_schema_change.py
    
        """
        db    = "dd"
        newdb = "newdd"
        taosbackup = etool.taosDumpFile()

        tmpdir_full = self._datadir("schemachange_full")
        tmpdir_spec = self._datadir("schemachange_spec")

        # restore dd from pre-generated backup (data must match backup for checkCorrect to pass)
        tdSql.execute(f"drop database if exists {db}")
        self.exec(f'{taosbackup} -i {tmpdir_full}')

        #
        #  whole db — import from pre-generated backup
        #

        # drop newdd; re-create with new schema so schema-change handling is exercised
        tdSql.execute(f"drop database if exists {newdb}")
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        self.benchmark(command)
        self.backupIn(db, newdb, tmpdir_full)
        self.checkCorrect(db, newdb)

        #
        #  specify stable & single table — import from pre-generated backup
        #

        tdSql.execute(f"drop database if exists {newdb}")
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        self.benchmark(command)
        self.backupIn(db, newdb, tmpdir_spec)
        self.checkCorrect(db, newdb)
