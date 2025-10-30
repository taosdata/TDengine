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

from new_test_framework.utils import tdLog, tdSql, etool, eos
import os
import json
import platform


class TestTaosdumpCommandline:
    def caseDescription(self):
        """
        test taosdump support commandline arguments
        """

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
            f"insert into {db}.ntb values('2025-01-01 10:00:01', 1, 'abc1')",
            f"insert into {db}.ntb values('2025-01-01 10:00:02', 2, 'abc2')",
            f"insert into {db}.ntb values('2025-01-01 10:00:03', 3, 'abc3')",
            f"insert into {db}.ntb values('2025-01-01 10:00:04', 4, 'abc4')",
            f"insert into {db}.ntb values('2025-01-01 10:00:05', 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)
        
        return db, stb, child_count, insert_rows

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
        self.check_same(db, newdb, stb, "sum(fc)")
        self.check_same(db, newdb, stb, "sum(ti)")
        self.check_same(db, newdb, stb, "sum(si)")
        self.check_same(db, newdb, stb, "sum(ic)")
        self.check_same(db, newdb, stb, "avg(bi)")
        self.check_same(db, newdb, stb, "sum(uti)")
        self.check_same(db, newdb, stb, "sum(usi)")
        self.check_same(db, newdb, stb, "sum(ui)")
        self.check_same(db, newdb, stb, "avg(ubi)")

        # check normal table
        self.check_same(db, newdb, "ntb", "sum(c1)")

    #  with Native Rest and WebSocket
    def dumpInOutMode(self, mode, db, json, tmpdir):
        # dump out
        self.clearPath(tmpdir)
        self.taosdump(f"{mode} -D {db} -o {tmpdir}")

        # dump in
        newdb = "new" + db
        self.taosdump(f"{mode} -W \"{db}={newdb}\" -i {tmpdir}")

        # check same
        self.verifyResult(db, newdb, json)


    # basic commandline
    def basicCommandLine(self, tmpdir):
        #command and check result 
        checkItems = [
            [f"-Z 0 -h 127.0.0.1 -P 6030 -uroot -ptaosdata -A -N -o {tmpdir}", ["OK: Database test dumped"]],
            [f"-r result -a -e test d0 -o {tmpdir}", ["OK: table: d0 dumped", "OK: 100 row(s) dumped out!"]],
            [f"-n -D test -o {tmpdir} -d lzma", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-Z 0 -gg -P 6030 -n -D test -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
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
            [f"--usage", ["taosdump [OPTION...] -o outpath"]],
            # conn mode -Z
            [f"-Z   0 -E '2022-10-01 00:00:60.000' test -o {tmpdir}", [
                "Connect mode is : Native", 
                "OK: Database test dumped", 
                "OK: 122 row(s) dumped out!"]
            ],
            [f"-Z  1 -E '2022-10-01 00:00:60.000' test -o {tmpdir}", [
                "Connect mode is : WebSocket",
                "OK: Database test dumped", 
                "OK: 122 row(s) dumped out!"]
            ],
        ]

        # executes 
        for item in checkItems:
            self.clearPath(tmpdir) # clear tmp
            command = item[0]
            results = item[1]
            rlist = self.taosdump(command)
            self.checkManyString(rlist, results)
            # clear tmp

    
    # check except
    def checkExcept(self, command):
        try:
            code = eos.exe(command, show = True)
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

        # conn mode
        options = [
            f"-Z native -X http://127.0.0.1:6041 -D {db} -o {tmpdir}",
            f"-Z 100  -D {db} -o {tmpdir}",
            f"-Z abcdefg -D {db} -o {tmpdir}",
            f"-X -D {db} -o {tmpdir}",
            f"-X 127.0.0.1:6041 -D {db} -o {tmpdir}",
            f"-X https://gw.cloud.taosdata.com?token617ffdf... -D {db} -o {tmpdir}",
            f"-Z 1 -X https://gw.cloud.taosdata.com?token=617ffdf... -D {db} -o {tmpdir}",
            f"-X http://127.0.0.1:6042 -D {db} -o {tmpdir}"
        ]

        # do check
        for option in options:
            self.checkExcept(taosdump + " " + option)


    # expect cmd > json > evn
    def checkPriority(self, db, stb, childCount, insertRows, tmpdir):
        #
        #  cmd & env
        #
        
        # env  6043 - invalid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6043"
        # cmd 6041 - valid
        cmd = f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}"
        self.clearPath(tmpdir)
        rlist = self.taosdump(cmd)
        results = [
            "Connect mode is : WebSocket",
            "OK: Database test dumped", 
            "OK: 205 row(s) dumped out!"
        ]
        self.checkManyString(rlist, results)

        #
        # env
        #

        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6041"
        # cmd 6041 - valid
        self.clearPath(tmpdir)
        cmd = f"-D {db} -o {tmpdir}"
        rlist = self.taosdump(cmd)
        self.checkManyString(rlist, results)

        #
        # cmd
        #

        os.environ['TDENGINE_CLOUD_DSN'] = ""
        # cmd 6041 - valid
        self.clearPath(tmpdir)
        cmd = f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}"
        rlist = self.taosdump(cmd)
        self.checkManyString(rlist, results)

        # clear env
        os.environ['TDENGINE_CLOUD_DSN'] = ""


    # conn mode
    def checkConnMode(self, db, stb, childCount, insertRows, tmpdir):
        # priority
        self.checkPriority(db, stb, childCount, insertRows, tmpdir)
    
    # password
    def checkPassword(self, tmpdir):
        # 255 char max password
        user    = "test_user"
        pwd     = ""
        pwdFile = f"{os.path.dirname(os.path.abspath(__file__))}/pwdMax.txt"
        with open(pwdFile) as file:
            pwd = file.readline()
        
        sql = f"create user {user} pass '{pwd}' "
        tdSql.execute(sql)
        # enterprise must set
        sql = f"grant read on test to {user}"
        tdSql.execute(sql)

        cmds = [
            f"-u{user} -p\"{pwd}\"      -D test -o {tmpdir}",  # command pass
            f"-u{user} -p < {pwdFile} -D test -o {tmpdir}"   # input   pass
        ]

        for cmd in cmds:
            self.clearPath(tmpdir)
            rlist = self.taosdump(cmd)
            self.checkListString(rlist, "OK: Database test dumped")

    # run
    def test_taosdump_commandline(self):
        """taosdump commandline

        1. Insert data with taosBenchmark
        2. Test taosdump commandline arguments:
            - dump in/out with Native/Rest/WebSocket modes
            - basic commandline arguments
            - except commandline arguments
            - check connMode priority cmd > env
        3. Verify dump and import data is correctly.
        4. Check long password support.
        5. Inspect avro files generated with -I argument
        6. Dump/restore database with escaped argument -e
        


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_commandline.py

        """
        
        # find
        taosdump, benchmark, tmpdir = self.findPrograme()
        json = f"{os.path.dirname(os.path.abspath(__file__))}/json/insertFullType.json"

        # insert data with taosBenchmark
        db, stb, childCount, insertRows = self.insertData(json)

        #
        # long password
        #
        self.checkPassword(tmpdir)
        tdLog.info("1. check long password ................................. [Passed]")

        # dumpInOut
        modes = ["-Z native", "-Z websocket", "--dsn=http://localhost:6041"]
        for mode in modes:
            self.dumpInOutMode(mode, db , json, tmpdir)

        tdLog.info("2. native rest ws dumpIn Out  .......................... [Passed]")

        # basic commandline
        self.basicCommandLine(tmpdir)
        tdLog.info("3. basic command line  .................................. [Passed]")

        # except commandline
        self.exceptCommandLine(taosdump, db, stb, tmpdir)
        tdLog.info("4. except command line  ................................. [Passed]")

        #
        # check connMode
        #

        self.checkConnMode(db, stb, childCount, insertRows, tmpdir)
        tdLog.info("5. check conn mode  ..................................... [Passed]")


        tdLog.success("%s successfully executed" % __file__)


