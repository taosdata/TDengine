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

import os
import json
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        test taosdump support commandline arguments
        """

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)
    
    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)

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
            self.clearPath(tmpdir)

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

    def insertData(self, json):
        # insert super table
        db, stb, child_count, insert_rows = self.insertBenchJson(json)
        
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
        
        return db, stb, child_count, insert_rows

    def dumpOut(self, taosdump, db , outdir):
        # dump out
        self.exec(f"{taosdump} -D {db} -o {outdir}")

    def dumpIn(self, taosdump, db, newdb, indir):
        # dump in
        self.exec(f'{taosdump} -W "{db}={newdb}" -i {indir}')

    def checkSame(self, db, newdb, stb, aggfun):
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
        self.checkSame(db, newdb, stb, "sum(pk)")
        self.checkSame(db, newdb, stb, "sum(usi)")
        self.checkSame(db, newdb, stb, "sum(ic)")

        # check normal table
        self.checkSame(db, newdb, "ntb", "sum(c1)")

    
    # basic commandline
    def basicCommandLine(self, tmpdir):
        #command and check result 
        checkItems = [
            [f"-h 127.0.0.1 -P 6030 -uroot -ptaosdata -A -N -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-r result -a -e test d0 -o {tmpdir}", ["OK: table: d0 dumped", "OK: 100 row(s) dumped out!"]],
            [f"-n -D test -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-L -D test -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-s -D test -o {tmpdir}", ["dumping out schema: 1 from meters.d0", "OK: Database test dumped", "OK: 0 row(s) dumped out!"]],
            [f"-N -d deflate -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f"-N -d lzma    -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f"-N -d snappy  -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f" -S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 22 row(s) dumped out!"]],
            [f"-T 2 -B 1000 -S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' test meters -o {tmpdir}", ["OK: table: meters dumped", "OK: 22 row(s) dumped out!"]],
            [f"-g -E '2022-10-01 00:00:60.000' test -o {tmpdir}", ["OK: Database test dumped", "OK: 122 row(s) dumped out!"]],
            [f"--help", ["Report bugs to"]],
            [f"-?", ["Report bugs to"]],
            [f"-V", ["version:"]],
            [f"--usage", ["taosdump [OPTION...] -o outpath"]]
        ]

        # executes 
        for item in checkItems:
            command = item[0]
            results = item[1]
            rlist = self.taosdump(command)
            for result in results:
                self.checkListString(rlist, result)
            # clear tmp    
            self.clearPath(tmpdir)
    
    # check except
    def checkExcept(self, command):
        try:
            code = self.exec(command)
            if code == 0:
                tdLog.exit(f"Failed, not report error cmd:{command}")
            else:
                tdLog.info(f"Passed, report error code={code} is expect, cmd:{command}")
        except:
            tdLog.info(f"Passed, catch expect report error for command {command}")


    # except commandline
    def exceptCommandLine(self, taosdump, db, stb, tmpdir):
        # -o 
        self.checkExcept(taosdump + " -o= ")
        self.checkExcept(taosdump + " -o")
        self.checkExcept(taosdump + " -A -o=")
        self.checkExcept(taosdump + " -A -o  ")
        self.checkExcept(taosdump + " -A -o ./noexistpath/")
        self.checkExcept(taosdump + f" -d invalidAVRO -o {tmpdir}")
        self.checkExcept(taosdump + f" -d unknown -o {tmpdir}")
        self.checkExcept(taosdump + f" -P invalidport")
        self.checkExcept(taosdump + f" -D")
        self.checkExcept(taosdump + f" -P 65536")
        self.checkExcept(taosdump + f" -t 2 -k 2 -z 1 -C https://not-exist.com:80/cloud -D test -o {tmpdir}")
        self.checkExcept(taosdump + f" -P 65536")

    # run
    def run(self):
        
        # find
        taosdump, benchmark, tmpdir = self.findPrograme()
        json = "./tools/taosdump/native/json/insertFullType.json"

        # insert data with taosBenchmark
        db, stb, child_count, insert_rows = self.insertData(json)
        newdb = "new" + db

        # basic commandline
        self.basicCommandLine(tmpdir)

        # except commandline
        self.exceptCommandLine(taosdump, db, stb, tmpdir)

        # dump out 
        #self.dumpOut(taosdump, db, tmpdir)

        # dump in
        #self.dumpIn(taosdump, db, newdb, tmpdir)

        # verify db
        #self.verifyResult(db, newdb, json)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
