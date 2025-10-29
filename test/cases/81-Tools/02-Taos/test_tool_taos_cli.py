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
import time
import platform




class TestTaosCli:
    updatecfgDict = {
        'slowLogScope':"query"
    }

    def checkDescribe(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = f"{os.path.dirname(__file__)}/json/taosCliDesc.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)
        # describe
        sql = f"describe {db}.{stb};"
        tdSql.query(sql)
        tdSql.checkRows(2 + 1000)
        # desc
        sql = f"desc {db}.{stb};"
        tdSql.query(sql)
        tdSql.checkRows(2 + 1000)


    def checkResultWithMode(self, db, stb, arg):
        result = "Query OK, 10 row(s)"
        mode = arg[0]

        # use db
        rlist = self.taos(f'{mode} -s "show databases;use {db};show databases;" ')
        self.checkListString(rlist, "Database changed")

        # hori
        cmd = f'{mode} -s "select ts,ic from {db}.{stb} limit 10'
        rlist = self.taos(cmd + '"')
        results = [
            "2022-10-01 00:00:09.000 |",
            result
        ]
        self.checkManyString(rlist, results)

        # vec
        rlist = self.taos(cmd + '\G"')
        results = [
            "****** 10.row *******",
            "ts: 2022-10-01 00:00:09.000",
            result
        ]
        self.checkManyString(rlist, results)


        # -B have some problem need todo
        self.taos(f'{mode} -B -s "select * from {db}.{stb} where ts < 1"')

        # get empty result
        rlist = self.taos(f'{mode} -r -s "select * from {db}.{stb} where ts < 1"')
        self.checkListString(rlist, "Query OK, 0 row(s) in set")


    def checkDecimalCommon(self, col, value):
        rlist = self.taos(f'-s "select {col} from testdec.test"')
        self.checkListString(rlist, value)

        outfile = "decimal.csv"
        self.taos(f'-s "select {col} from testdec.test>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, value)
        self.deleteFile(outfile)


    def checkDecimal(self):
        # prepare data
        self.taos(f'-s "drop database if exists testdec"')
        self.taos(f'-s "create database if not exists testdec"')
        self.taos(f'-s "create table if not exists testdec.test(ts timestamp, dec64 decimal(10,6), dec128 decimal(24,10)) tags (note nchar(20))"')
        self.taos(f'-s "create table testdec.d0 using testdec.test(note) tags(\'test\')"')
        self.taos(f'-s "insert into testdec.d0 values(now(), \'9876.123456\', \'123456789012.0987654321\')"')
        
        # check decimal64
        self.checkDecimalCommon("dec64", "9876.123456")

        # check decimal128
        self.checkDecimalCommon("dec128", "123456789012.0987654321")

        self.taos(f'-s "drop database if exists testdec"')


    def checkBasic(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = f"{os.path.dirname(__file__)}/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)
        # set
        self.db  = db
        self.stb = stb
        self.insert_rows      = insertRows
        self.childtable_count = childCount

        # native restful websock test
        args = [
            ["-Z native"], 
            ["-T 40 -E http://localhost:6041"]
        ]
        for arg in args:
            self.checkResultWithMode(db, stb, arg)
        
        self.checkDecimal()


    def checkDumpInOutMode(self, source, arg, db, insertRows):
        mode = arg[0]
        self.taos(f'{mode} -s "source {source}" ')
        self.taos(f'{mode} -s "select * from {db}.d0>>d0.csv" ')
        
        # use db
        rlist = self.taos(f'{mode} -s "show databases;use {db};show databases;" ')
        # update sql
        rlist = self.taos(f'{mode} -s "alter local \'resetlog\';" ')
        self.checkListString(rlist, "Query O")

        # only native support csv import
        if mode == "":
            self.taos(f'{mode} -s "delete from {db}.d0" ')
            self.taos(f'{mode} -s "insert into {db}.d0 file d0.csv" ')
        
        sql = f"select count(*) from {db}.d0"
        self.taos(f'{mode} -B -s "{sql}" ')
        tdSql.checkAgg(sql, insertRows)
        sql = f"select first(voltage) from {db}.d0"
        tdSql.checkFirstValue(sql, 1)
        sql = f"select last(voltage) from {db}.d0"
        tdSql.checkFirstValue(sql, 5)

    def checkDumpInOut(self):
        args = [
            ["",   18],
            ["-E http://localhost:6041", 21]
        ]

        source = f"{os.path.dirname(__file__)}/data/source.sql"
        db = "db"
        insertRows = 5
        for arg in args:
            # insert 
            self.checkDumpInOutMode(source, arg, db, insertRows)

    def checkVersion(self):
        rlist1 = self.taos("-V")
        # Windows not support --version
        if platform.system() == "Windows":
            rlist2 = self.taos("-V")
        else:
            rlist2 = self.taos("--version")

        self.checkSame(rlist1, rlist2)
        if len(rlist1) < 4:
            tdLog.exit(f"version lines less than 4. {rlist1}")

        if len(rlist1[2]) < 42:
            tdLog.exit("git commit id length is invalid: " + rlist1[2])
        
        keys = [
            "version:",
            "git:",
            "build:"
        ]
        self.checkManyString(rlist1, keys)


    def checkHelp(self):
        # help
        rlist1 = self.taos("--help")
        rlist2 = self.taos("-?")
        self.checkSame(rlist1, rlist2)

        # check return 
        if platform.system() == "Windows":
            strings = [
            "-a,",
            "-d,",
            "-V"
            ]
        else:
            strings = [
                "--auth=AUTH",
                "--database=DATABASE",
                "--version",
                " --help"
            ]

        for string in strings:
            self.checkListString(rlist1, string)
    
    def checkCommand(self):
        # check coredump
        queryOK = "Query OK"

        # support Both
        args = [
            ['-s "quit;"', "Welcome to the TDengine TSDB Command Line Interface"],
            ['-h "" -s "show dnodes;"', "Invalid host"],
            ['-u "" -s "show dnodes;"', "Invalid user"],
            ['-P "" -s "show dnodes;"', "Invalid port"],
            ['-u "AA" -s "show dnodes;"', "failed to connect to server"],
            ['-p"abc" -s "show dnodes;"', "[0x80000357]"],
            ['-d "abc" -s "show dnodes;"', "[0x80000388]"],
            ['-N 0 -s "show dnodes;"', "Invalid pktNum"],
            ['-N 10 -h 127.0.0.1 -s "show dnodes;"', queryOK],
            ['-w 0 -s "show dnodes;"', "Invalid displayWidth"],
            ['-w 10 -s "show dnodes;"', queryOK],
            ['-W 10 -s "show dnodes;"', None],
            ['-l 0 -s "show dnodes;"', "Invalid pktLen"],
            ['-l 10 -s "show dnodes;"', queryOK],
            ['-C', "buildinfo"],
            ['-B -s "show dnodes;"', queryOK],
            ['-s "help;"', "Timestamp expression Format"],
            ['-s ""', "Invalid commands"],
            ['-t', "2: service ok"],
            
        ]
        if platform.system() != "Windows":
            args.append(['-o "./current/log/files/" -h localhost -uroot -ptaosdata  -s"show databases;"', queryOK])
            args.append(['-uroot -w 40 -ptaosdata -c /root/taos/ -s"show databases"', queryOK])
            args.append([f'-uroot -p < {os.path.dirname(__file__)}/data/pwd.txt -s "show dnodes;"', queryOK])

        modes = ["-Z 0","-Z 1"]
        for mode in modes:
            for arg in args:
                rlist = self.taos(mode + " " + arg[0])
                if arg[1] != None:
                    self.checkListString(rlist, arg[1])

        #
        # support native only
        #
        
        # o logpath
        char = 'a'
        lname =f'-o "/root/log/{char * 1000}/" -s "quit;"' 

        args = [
            ['-a ""', "Invalid auth"],
            ['-a "abc"', "[0x80000357]"],
        ]
        if platform.system() != "Windows":
            args.append([lname, "failed to create log at"])
        for arg in args:
            rlist = self.taos("-Z 0 " + arg[0])
            if arg[1] != None:
                self.checkListString(rlist, arg[1])

    # expect cmd > json > evn
    def checkPriority(self):
        #
        #  cmd & env
        #
        
        # env  6043 - invalid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6043"
        # cmd 6041 - valid
        cmd = f'-X http://127.0.0.1:6041 -s "select ts from test.meters"'
        rlist = self.taos(cmd, checkRun = True)
        results = [
            "WebSocket Client Version",
            "2022-10-01 00:01:39.000", 
            "Query OK, 200 row(s) in set"
        ]
        self.checkManyString(rlist, results)

        #
        # env
        #

        # cloud
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6041"
        cmd = f'-s "select ts from test.meters"'
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)
        # local
        os.environ['TDENGINE_CLOUD_DSN'] = ""
        os.environ['TDENGINE_DSN']       = "http://127.0.0.1:6041"
        cmd = f'-s "select ts from test.meters"'
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)
        # local & cloud -> cloud first
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6041"  # valid
        os.environ['TDENGINE_DSN']       = "http://127.0.0.1:6042"  # invalid
        cmd = f'-s "select ts from test.meters"'
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)


        #
        # cmd
        #

        os.environ['TDENGINE_CLOUD_DSN'] = ""
        os.environ['TDENGINE_DSN']       = ""
        cmd = f'-X http://127.0.0.1:6041 -s "select ts from test.meters"'
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)


    def checkExceptCmd(self):
        # exe
        taos = etool.taosFile()
        # option
        options = [
            "-Z native -X http://127.0.0.1:6041",
            "-Z 100",
            "-Z abcdefg",
            "-X",
            "-X  ",
            "-X 127.0.0.1:6041",
            "-X https://gw.cloud.taosdata.com?token617ffdf...",
            "-Z 1 -X https://gw.cloud.taosdata.com?token=617ffdf...",
            "-X http://127.0.0.1:6042"
        ]

        # do check
        for option in options:
            self.checkExcept(taos + " -s 'show dnodes;' " + option)
    
    def checkModeVersion(self):    

        # check default conn mode        
        #DEFAULT_CONN = "WebSocket"
        DEFAULT_CONN = "Native"

        # results
        results = [
            f"{DEFAULT_CONN} Client Version",
            "2022-10-01 00:01:39.000", 
            "Query OK, 100 row(s) in set"
        ]
    
        # default
        cmd = f'-s "select ts from test.d0"'
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)
        
        # websocket
        cmd = f'-Z 1 -s "select ts from test.d0"'
        results[0] = "WebSocket Client Version"
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)        

        # native
        cmd = f'-Z 0 -s "select ts from test.d0"'
        results[0] = "Native Client Version"
        rlist = self.taos(cmd, checkRun = True)
        self.checkManyString(rlist, results)

    def checkConnMode(self):
        # priority
        self.checkPriority()
        # except
        self.checkExceptCmd()
        # mode version
        self.checkModeVersion()
    
    # password
    def checkPassword(self):
        # 255 char max password
        user    = "test_user"
        pwd     = ""
        pwdFile = f"{os.path.dirname(__file__)}/data/pwdMax.txt"
        with open(pwdFile) as file:
            pwd = file.readline()
        
        sql = f"create user {user} pass '{pwd}' "
        tdSql.execute(sql)
         
        cmds = [
            f"-u{user} -p'{pwd}'      -s 'show databases;'",  # command pass
            f"-u{user} -p < {pwdFile} -s 'show databases;'"   # input   pass
        ]

        for cmd in cmds:
            rlist = self.taos(cmd, checkRun=True, show=False)
            self.checkListString(rlist, "Query OK,")

    # run
    def test_tool_taos_cli(self):
        """taos-CLI basic test
        
        1. Insert data with taosBenchmark json format
        2. Check describe show full
        3. Check basic command in different conn mode
        4. Check version and help
        5. Check command options
        6. Check data dump in/out
        7. Check conn mode priority and except cmd
        8. Check max password length

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cmdline/test_taos_cli.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # check show whole
        self.checkDescribe()

        # check basic
        self.checkBasic()

        # version
        self.checkVersion()

        # help
        self.checkHelp()

        # check command
        self.checkCommand()

        # check data in/out
        self.checkDumpInOut()


        # check conn mode
        self.checkConnMode()

        # max password
        self.checkPassword()

        tdLog.success(f"{__file__} successfully executed")


