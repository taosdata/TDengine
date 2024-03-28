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
from frame.srvCtl import *


class TDTestCase(TBase):
    updatecfgDict = {
        "countAlwaysReturnValue" : "0",
        "lossyColumns"           : "float,double",
        "fPrecision"             : "0.000000001",
        "dPrecision"             : "0.00000000000000001",
        "ifAdtFse"               : "1",
        'slowLogScope'           : "insert"
    }

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

        # create count check table
        sql = f"create table {self.db}.ta(ts timestamp, age int) tags(area int)"
        tdSql.execute(sql)

    def checkFloatDouble(self):
        sql = f"select * from {self.db}.{self.stb} where fc!=100"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = f"select * from {self.db}.{self.stb} where dc!=200"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = f"select avg(fc) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, 100)
        sql = f"select avg(dc) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, 200)

    def alterReplica3(self):
        sql = f"alter database {self.db} replica 3"
        tdSql.execute(sql, show=True)
        time.sleep(2)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        time.sleep(5)
        sc.dnodeStart(2)
        sc.dnodeStart(3)

        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True

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
        self.alterReplica3()

        vgids = self.getVGroup(self.db)
        selid = random.choice(vgids)
        self.balanceVGroupLeaderOn(selid)

        # check count always return value
        sql = f"select count(*) from {self.db}.ta"
        tdSql.query(sql)
        tdSql.checkRows(0) # countAlwaysReturnValue is false

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # check float double value ok
        self.checkFloatDouble()

        # save
        self.snapshotAgg()

        # do action
        self.doAction()

        # check save agg result correct
        self.checkAggCorrect()

        # check insert correct again
        self.checkInsertCorrect()

        # check float double value ok
        self.checkFloatDouble()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
