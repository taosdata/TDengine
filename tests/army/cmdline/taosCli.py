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

    def checkBasic(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = "cmdline/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)

        args = [
            ["",   18, 346, -2, 310], 
            ["-R", 22, 350, -3, 313],
            ["-E http://localhost:6041", 21, 349, -3, 312]
        ]
        for arg in args:
            self.checkResultWithMode(db, stb, arg)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # check basic
        self.checkBasic()

        # check show whole
        self.checkDescribe()

        # full types show


        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
