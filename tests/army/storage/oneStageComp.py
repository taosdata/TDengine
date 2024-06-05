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
    updatecfgDict = {
        "compressMsgSize" : "100",
    }

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, "oneStageComp.json")
        etool.benchMark(json=jfile)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 10
        self.insert_rows      = 100000
        self.timestamp_step   = 1000
    


    def checkColValueCorrect(self):
        tdLog.info(f"do action.")
        self.flushDb()

        # check all columns correct
        cnt = self.insert_rows * self.childtable_count
        sql = "select * from stb where bc!=1"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where fc=101"
        tdSql.query(sql)
        tdSql.checkRows(cnt)
        sql = "select * from stb where dc!=102"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where ti!=103"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where si!=104"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where ic!=105"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where bi!=106"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where uti!=107"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where usi!=108"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where ui!=109"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where ubi!=110"
        tdSql.query(sql)
        tdSql.checkRows(0)
    
    def insertNull(self):
        # insert 6 lines
        sql = "insert into d0(ts) values(now) (now + 1s) (now + 2s) (now + 3s) (now + 4s) (now + 5s)"
        tdSql.execute(sql)

        self.flushDb()
        self.trimDb()

        # check all columns correct
        cnt = self.insert_rows * self.childtable_count
        sql = "select * from stb where bc!=1"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "select * from stb where bc is null"
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = "select * from stb where bc=1"
        tdSql.query(sql)
        tdSql.checkRows(cnt)
        sql = "select * from stb where usi is null"
        tdSql.query(sql)
        tdSql.checkRows(6)

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
        self.checkColValueCorrect()

        # check save agg result correct
        self.checkAggCorrect()

        # insert null 
        self.insertNull()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
