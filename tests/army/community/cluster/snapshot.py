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
        jfile = etool.curFile(__file__, "snapshot.json")
        etool.benchMark(json=jfile)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 10
        self.insert_rows      = 100000
        self.timestamp_step   = 10000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()

        # split vgroups
        self.splitVGroups()
        self.trimDb()
        self.checkAggCorrect()

        # balance vgroups
        self.balanceVGroupLeader()

        # replica to 1
        self.alterReplica(1)
        self.checkAggCorrect()
        self.compactDb()
        self.alterReplica(3)

        vgids = self.getVGroup(self.db)
        selid = random.choice(vgids)
        self.balanceVGroupLeaderOn(selid)

        

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
