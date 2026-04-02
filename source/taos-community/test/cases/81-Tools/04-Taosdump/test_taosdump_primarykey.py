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
import json

class TestTaosdumpPrimaryKey:
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        # taosdump 
        taosdump = etool.taosDumpFile()
        if taosdump == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % taosdump)

        # taosBenchmark
        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("benchmark not found!")
        else:
            tdLog.info("benchmark found in %s" % benchmark)

        # tmp dir
        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s/*" % tmpdir)

        return taosdump, benchmark,tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb = None, checkInterval=False):
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        # db come from arguments
        if newdb is None:
            db = data["databases"][0]["dbinfo"]["name"]
        else:
            db = newdb

        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkInterval:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

    def testBenchmarkJson(self, benchmark, jsonFile, options="", checkInterval=False):
        # exe insert 
        cmd = f"{benchmark} {options} -f {jsonFile}"
        self.exec(cmd)
        self.checkCorrectWithJson(jsonFile)

    def insertData(self, benchmark, json, db):
        # insert super table
        self.testBenchmarkJson(benchmark, json)
        
        # normal table
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 binary(32))",
            f"insert into {db}.ntb values(now, 1, 'abc1')",
            f"insert into {db}.ntb values(now, 2, 'abc2')",
            f"insert into {db}.ntb values(now, 3, 'abc3')",
            f"insert into {db}.ntb values(now, 4, 'abc4')",
            f"insert into {db}.ntb values(now, 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def dumpOut(self, taosdump, db , outdir):
        # dump out
        self.exec(f"{taosdump} -D {db} -o {outdir}")

    def dumpIn(self, taosdump, db, newdb, indir):
        # dump in
        self.exec(f'{taosdump} -W "{db}={newdb}" -i {indir}')

    def check_same(self, db, newdb, stb, aggfun):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0,0)
        # sum pk newdb
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0,0)

        if sum1 == sum2:
            tdLog.info(f"{aggfun} source db:{sum1} import db:{sum2} both equal.")
        else:
            tdLog.exit(f"{aggfun} source db:{sum1} import db:{sum2} not equal.")


    def verifyResult(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)
        
        #  compare sum(pk)
        stb = "meters"
        self.check_same(db, newdb, stb, "sum(pk)")
        self.check_same(db, newdb, stb, "sum(usi)")
        self.check_same(db, newdb, stb, "sum(ic)")

        # check normal table
        self.check_same(db, newdb, "ntb", "sum(c1)")

    def test_taosdump_primarykey(self):
        """taosdump primary key

        1. Prepare database with primary key using taosBenchmark
        2. Dump out database using taosdump
        3. Dump in database using taosdump
        4. Verify data correctness with sum aggregation
        5. Verify meta correctness with taosBenchmark json file

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_primary_key.py

        """
        # database
        db = "pridb"
        newdb = "npridb"
        
        # find
        taosdump, benchmark, tmpdir = self.findPrograme()
        json = f"{os.path.dirname(os.path.abspath(__file__))}/json/primaryKey.json"

        # insert data with taosBenchmark
        self.insertData(benchmark, json, db)

        # dump out 
        self.dumpOut(taosdump, db, tmpdir)

        # dump in
        self.dumpIn(taosdump, db, newdb, tmpdir)

        # verify db
        self.verifyResult(db, newdb, json)


