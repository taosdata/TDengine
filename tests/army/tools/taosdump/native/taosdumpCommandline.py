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
import frame.eos
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

    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)

    def findPrograme(self):
        # taosdump 
        taosdump = frame.etool.taosDumpFile()
        if taosdump == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % taosdump)

        # taosBenchmark
        benchmark = frame.etool.benchMarkFile()
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

    def checkCorrectWithJson(self, jsonFile, newdb = None, checkInterval = True):
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

        stb            = data["databases"][0]["super_tables"][0]["name"]
        child_count    = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows    = data["databases"][0]["super_tables"][0]["insert_rows"]
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
        self.checkSame(db, newdb, stb, "sum(fc)")
        self.checkSame(db, newdb, stb, "sum(ti)")
        self.checkSame(db, newdb, stb, "sum(si)")
        self.checkSame(db, newdb, stb, "sum(ic)")
        self.checkSame(db, newdb, stb, "avg(bi)")
        self.checkSame(db, newdb, stb, "sum(uti)")
        self.checkSame(db, newdb, stb, "sum(usi)")
        self.checkSame(db, newdb, stb, "sum(ui)")
        self.checkSame(db, newdb, stb, "avg(ubi)")

        # check normal table
        self.checkSame(db, newdb, "ntb", "sum(c1)")

    #  with Native Rest and WebSocket
    def dumpInOutMode(self, mode, db, json, tmpdir):
        # dump out
        self.clearPath(tmpdir)
        self.taosdump(f"{mode} -D {db} -o {tmpdir}")

        # dump in
        newdb = "new" + db
        self.taosdump(f"{mode} -W '{db}={newdb}' -i {tmpdir}")

        # check same
        self.verifyResult(db, newdb, json)


    # basic commandline
    def basicCommandLine(self, tmpdir):
        #command and check result 
        checkItems = [
            [f"-h 127.0.0.1 -P 6030 -uroot -ptaosdata -A -N -o {tmpdir}", ["OK: Database test dumped"]],
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
            self.clearPath(tmpdir)
            command = item[0]
            results = item[1]
            rlist = self.taosdump(command)
            for result in results:
                self.checkListString(rlist, result)
            # clear tmp    

    
    # check except
    def checkExcept(self, command):
        try:
            code = frame.eos.exe(command, show = True)
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
        db, stb, childCount, insertRows = self.insertData(json)

        # dumpInOut
        modes = ["", "-R" , "--cloud=http://localhost:6041"]
        for mode in modes:
            self.dumpInOutMode(mode, db , json, tmpdir)

        tdLog.info("1. native rest ws dumpIn Out  .......................... [Passed]")

        # basic commandline
        self.basicCommandLine(tmpdir)
        tdLog.info("2. basic command line  .................................. [Passed]")

        # except commandline
        self.exceptCommandLine(taosdump, db, stb, tmpdir)
        tdLog.info("3. except command line  ................................. [Passed]")

        #
        # varbinary and geometry for native
        #
        json = "./tools/taosdump/native/json/insertOther.json"
        # insert 
        db, stb, childCount, insertRows = self.insertData(json)
        # dump in/out
        self.dumpInOutMode("", db , json, tmpdir)
        tdLog.info("4. native varbinary geometry ........................... [Passed]")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
