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

class TestTaosdumpTypeVarbinary:
    def caseDescription(self):
        """
        case1<sdsang>: [TS-3072] taosdump dump escaped db name test
        """




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
            f"create table {db}.ntb(st timestamp, c1 int, c2 varbinary(32))",
            f"insert into {db}.ntb values(now, 0, 'abc1')",
            f"insert into {db}.ntb values(now, 1,  NULL)",
            f"insert into {db}.ntb values(now, 2, '\\x616263')",
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

    def checkAggSame(self, db, newdb, stb, aggfun):
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

    def checkProjSame(self, db, newdb, stb , row, col, where = "where tbname='d0'"):
        # sum pk db
        sql = f"select * from {db}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val1 = copy.deepcopy(tdSql.getData(row, col))
        # sum pk newdb
        sql = f"select * from {newdb}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val2 = copy.deepcopy(tdSql.getData(row, col))

        if val1 == val2:
            tdLog.info(f"{db}.{stb} {row},{col} source db:{val1} import db:{val2} both equal.")
        else:
            tdLog.exit(f"{db}.{stb} {row},{col} source db:{val1} len={len(val1)} import db:{val2} len={len(val2)} not equal.")


    def verifyResult(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)
        
        #  compare sum(pk)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6) # tag

        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6) # tag
        
        # check normal table
        tb = "ntb"
        self.checkAggSame(db, newdb, tb, "sum(c1)")
        # 0 line
        self.checkProjSame(db, newdb, tb, 0, 0, "")
        self.checkProjSame(db, newdb, tb, 0, 1, "")
        self.checkProjSame(db, newdb, tb, 0, 2, "")
        # 1 line
        self.checkProjSame(db, newdb, tb, 1, 0, "")
        self.checkProjSame(db, newdb, tb, 1, 1, "")
        self.checkProjSame(db, newdb, tb, 1, 2, "")
        # 3 line
        self.checkProjSame(db, newdb, tb, 3, 0, "")
        self.checkProjSame(db, newdb, tb, 3, 1, "")
        self.checkProjSame(db, newdb, tb, 3, 2, "")


    def test_taosdump_type_varbinary(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
            - xxx
            - xxx
        """

        # TD-33002 BLOCK this case , if fixed this bug, please open this case 
        return 

        # database
        db = "varbin"
        newdb = "nvarbin"
        
        # find
        taosdump, benchmark, tmpdir = self.findPrograme()
        json = "./taosdump/native/json/varbinary.json"

        # insert data with taosBenchmark
        self.insertData(benchmark, json, db)

        # dump out 
        self.dumpOut(taosdump, db, tmpdir)

        # dump in
        self.dumpIn(taosdump, db, newdb, tmpdir)

        # verify db
        self.verifyResult(db, newdb, json)


        tdLog.success("%s successfully executed" % __file__)


