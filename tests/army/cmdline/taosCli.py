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

    def checkBasic(self):
        tdLog.info(f"check describe show full.")

        # insert
        json = "cmdline/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)
        
        result = "Query OK, 10 row(s)"


        # hori
        cmd = f'taos -s "select * {db}.{stb} limit 10"'
        rlist = etool.runBinFile("taos", cmd)
        # line count
        self.checkSame(len(rlist), 18)
        # last line
        self.checkSame(rlist[-1][:len(result)], result)

        # vec
        rlist = etool.runBinFile("taos", cmd + "\G")
        # line count
        self.checkSame(len(rlist), 346)
        self.checkSame(rlist[310], "************************** 10.row ***************************")
        # last line
        self.checkSame(rlist[-1][:len(result)], result)    

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
