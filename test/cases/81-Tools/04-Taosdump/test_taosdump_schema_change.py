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
    def createDir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            print("directory exists")
            self.clearPath(path)

    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)


    # insert
    def insertData(self):
        # source db
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChange.json"
        self.benchmark(command)
        # des newdb
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        self.benchmark(command)

    # dump out
    def dumpOut(self, db, tmpdir):
        cmd = f"-D {db} -o {tmpdir}"
        rlist = self.taosdump(cmd)
        results = [
            "OK: total 1 table(s) of stable: meters1 schema dumped.",
            "OK: total 20 table(s) of stable: meters2 schema dumped.",
            "OK: total 30 table(s) of stable: meters3 schema dumped.",
            "OK: 9132 row(s) dumped out!"
        ]
        self.checkManyString(rlist, results)

    # dump in
    def dumpIn(self, db, newdb, tmpdir):
        cmd = f'-W "{db}={newdb}" -i {tmpdir}'
        rlist = self.taosdump(cmd)
        results = [
            f"rename DB Name {db} to {newdb}",
            f"OK: 9132 row(s) dumped in!"
        ]
        self.checkManyString(rlist, results)

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

    #
    # ----------  specify table ------------
    #

    # clear env
    def clearEvn(self, newdb, tmpdir):
        # clear old
        self.clearPath(tmpdir)

        # des newdb re-create
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        self.benchmark(command)        

    # dump out specify
    def dumpOutSpecify(self, db, tmpdir):
        cmd = f"-o {tmpdir} {db} d0 meters2 meters3 meters4 ntbd1 ntbd2 ntbe1 ntbe2 ntbf1 ntbf2 ntbg1 ntbg2"
        rlist = self.taosdump(cmd)
        results = [
            "OK: total 20 table(s) of stable: meters2 schema dumped.",
            "OK: total 30 table(s) of stable: meters3 schema dumped.",
            "OK: total 40 table(s) of stable: meters4 schema dumped.",
            "OK: 9132 row(s) dumped out!"
        ]
        self.checkManyString(rlist, results)

    def exceptNoSameCol(self, db, newdb, tmpdir):
        # des newdb re-create
        command = f"-f {os.path.dirname(os.path.abspath(__file__))}/json/schemaChangeNew.json"
        self.benchmark(command)        

        # re-create meters2 for no same column and tags
        sqls = [
            # meters2 no same col and tag
            f"drop table {newdb}.meters2",
            f"create table {newdb}.meters2(nts timestamp, age int) tags(area int)",
            # meters3 one same col and no same tag
            f"drop table {newdb}.meters3",
            f"create table {newdb}.meters3(ts timestamp, fc float) tags(area int)"
        ]
        tdSql.executes(sqls)

        # dumpIn
        cmd = f'-W "{db}={newdb}" -i {tmpdir}'
        rlist = self.taosdump(cmd)
        results = [
            f"rename DB Name {db} to {newdb}",
            "backup data schema no same column with server table",
            "new tag zero failed! oldt=",
            "50 failures occurred to dump in",
            "OK: 4132 row(s) dumped in!"
        ]
        self.checkManyString(rlist, results)

        tdLog.info("check except no same column ...................... [OK]")

    def testExcept(self, db, newdb, tmpdir):
        # dump out , des table no same column
        self.exceptNoSameCol(db, newdb, tmpdir)

    def test_taosdump_schema_change(self):
        """taosdump precision

        1.  Prepare data with taosBenchmark -f schemaChange.json/schemaChangeNew.json
        2.  Use taosdump to export the entire database
        3.  Use taosdump to import the exported data back into a new database
        4.  Verify that the imported data matches the original data
        5.  Use taosdump to export specific tables from the original database
        6.  Use taosdump to import the exported data back into another new database
        7.  Verify that the imported data matches the original data for the specified tables
        8.  Drop old super tables and recreate new ones with schema changes (e.g., missing columns or tags)
        9. Use taosdump to import the previously exported data into the modified tables
        10. Verify that the data import handles the schema changes correctly
        11.  Test exception handling when the target tables have schema changes (e.g., missing columns or tags)


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_schema_change.py
    
        """
        # init
        db    = "dd"
        newdb = "newdd"

        # tmp dir
        tmpdir = "./tmp"
        self.createDir(tmpdir)

        # insert data
        self.insertData()

        #
        #  whole db dump out
        #
      
        # dump out 
        self.dumpOut(db, tmpdir)

        # dump in
        self.dumpIn(db, newdb, tmpdir)

        # check result correct
        self.checkCorrect(db, newdb)

        #
        #  specify stable & single table dump out
        #

        # clear env
        self.clearEvn(newdb, tmpdir)

        # dump out specify table
        self.dumpOutSpecify(db, tmpdir)

        # dump in
        self.dumpIn(db, newdb, tmpdir)

        # check result correct specify table
        self.checkCorrect(db, newdb)

        # check except
        self.testExcept(db, newdb, tmpdir)

        tdLog.success("%s successfully executed" % __file__)


