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

    def checkDumpInOut(self, db, stb, insertRows):
        self.taos(f'-s "select * from {db}.d0 >>d0.csv" ')
        #self.taos(f'-s "delete from {db}.d0" ')
        #self.taos(f'-s "insert into {db}.d0 file d0.out " ')
        #sql = f"select count(*) from {db}.d0"
        #tdSql.checkAgg(sql, insertRows)
    
    def checkBasic(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = "cmdline/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)

        # native restful websock test
        args = [
            ["",   18, 346, -2, 310], 
            ["-R -r", 22, 350, -3, 313],
            ["-T 40 -E http://localhost:6041", 21, 349, -3, 312]
        ]
        for arg in args:
            self.checkResultWithMode(db, stb, arg)

        # dump in/out
        self.checkDumpInOut(db, stb, insertRows)


    def checkVersion(self):
        rlist1 = self.taos("-V")
        rlist2 = self.taos("--version")

        self.checkSame(rlist1, rlist2)
        self.checkSame(len(rlist1), 4)

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
        self.taos(' -uroot -w 40 -ptaosdata -c /root/taos/ -s"show databases"')

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


        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
