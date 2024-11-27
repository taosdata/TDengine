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
        json = "cmdline/json/taosCli.json"
        db, stb, childCount, insertRows = self.insertBenchJson(json)
        # describe
        sql = f"describe {db}.{stb};"
        tdSql.query(sql)
        tdSql.checkRows(2 + 1000)
        # desc
        sql = f"desc {db}.{stb};"
        tdSql.query(sql)
        tdSql.checkRows(2 + 1000)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # check show whole
        self.checkDescribe()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
