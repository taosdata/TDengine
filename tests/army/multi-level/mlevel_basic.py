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

import taos
import frame
import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
        

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "mlevel_basic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()
        self.trimDb()
        self.compactDb()

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # save
        self.snapshotAgg()

        # do action
        self.doAction()

        # check save agg result correct
        self.checkAggCorrect()

        # check insert correct again
        self.checkInsertCorrect()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
