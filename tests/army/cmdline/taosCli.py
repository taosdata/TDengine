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

import sys
import time
import random

import frame.eos
import frame.etime
import frame.etool
import frame.etool
import frame.etool
import frame.etool
import taos
import frame.etool
import frame

from frame.log import *
from frame.sql import *
from frame.cases import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *


class TDTestCase(TBase):
    updatecfgDict = {
        'slowLogScope':"query"
    }

    def checkDescribe(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = "cmdline/json/taosCliDesc.json"
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
        rowh = arg[1]
        rowv = arg[2]
        idx  = arg[3]
        idxv = arg[4]

        # use db
        if mode != "-R":
            rlist = self.taos(f'{mode} -s "show databases;use {db};show databases;" ')
            self.checkListString(rlist, "Database changed")

        # hori
        cmd = f'{mode} -s "select * from {db}.{stb} limit 10'
        rlist = self.taos(cmd + '"')
        # line count
        self.checkSame(len(rlist), rowh)
        # last line
        self.checkSame(rlist[idx][:len(result)], result)

        # vec
        rlist = self.taos(cmd + '\G"')
        # line count
        self.checkSame(len(rlist), rowv)
        self.checkSame(rlist[idxv], "*************************** 10.row ***************************")
        # last line
        self.checkSame(rlist[idx][:len(result)], result)

        # -B have some problem need todo
        self.taos(f'{mode} -B -s "select * from {db}.{stb} where ts < 1"')

        # get empty result
        rlist = self.taos(f'{mode} -r -s "select * from {db}.{stb} where ts < 1"')
        self.checkListString(rlist, "Query OK, 0 row(s) in set")
    
    def checkBasic(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = "cmdline/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)

        # native restful websock test
        args = [
            ["",   18, 346, -2, 310], 
            ["-R", 22, 350, -3, 313],
            ["-T 40 -E http://localhost:6041", 21, 349, -3, 312]
        ]
        for arg in args:
            self.checkResultWithMode(db, stb, arg)


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
            ["-R ", 22],
            ["-E http://localhost:6041", 21]
        ]

        source = "cmdline/data/source.sql"
        db = "db"
        insertRows = 5
        for arg in args:
            # insert 
            self.checkDumpInOutMode(source, arg, db, insertRows)

    def checkVersion(self):
        rlist1 = self.taos("-V")
        rlist2 = self.taos("--version")

        self.checkSame(rlist1, rlist2)
        self.checkSame(len(rlist1), 5)

        if len(rlist1[2]) < 42:
            tdLog.exit("git commit id length is invalid: " + rlist1[2])


    def checkHelp(self):
        # help
        rlist1 = self.taos("--help")
        rlist2 = self.taos("-?")
        self.checkSame(rlist1, rlist2)

        # check return 
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

        # o logpath
        char = 'a'
        lname =f'-o "/root/log/{char * 1000}/" -s "quit;"' 
        queryOK = "Query OK"

        # invalid input check
        args = [
            [lname, "failed to create log at"],
            ['-uroot -w 40 -ptaosdata -c /root/taos/ -s"show databases"', queryOK],
            ['-o "./current/log/files/" -s"show databases;"', queryOK],
            ['-a ""', "Invalid auth"],
            ['-s "quit;"', "Welcome to the TDengine Command Line Interface"],
            ['-a "abc"', "[0x80000357]"],
            ['-h "" -s "show dnodes;"', "Invalid host"],
            ['-u "" -s "show dnodes;"', "Invalid user"],
            ['-P "" -s "show dnodes;"', "Invalid port"],
            ['-u "AA" -s "show dnodes;"', "failed to connect to server"],
            ['-p"abc" -s "show dnodes;"', "[0x80000357]"],
            ['-d "abc" -s "show dnodes;"', "[0x80000388]"],
            ['-N 0 -s "show dnodes;"', "Invalid pktNum"],
            ['-N 10 -s "show dnodes;"', queryOK],
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
            ['-uroot -p < cmdline/data/pwd.txt -s "show dnodes;"', queryOK],
        ]

        for arg in args:
            rlist = self.taos(arg[0])
            if arg[1] != None:
                self.checkListString(rlist, arg[1])    

    # run
    def run(self):
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

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
