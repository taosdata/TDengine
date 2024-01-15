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
        "keepColumnName" : "1",
        "ttlChangeOnWrite" : "1",
        "querySmaOptimize": "1"
    }

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, "query_basic.json")
        etool.benchMark(json=jfile)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 6
        self.insert_rows      = 100000
        self.timestamp_step   = 30000


    def doQuery(self):
        tdLog.info(f"do query.")
        
        # __group_key
        sql = f"select count(*),_group_key(uti),uti from {self.stb} partition by uti;"
        tdSql.execute(sql)
        tdSql.checkRows(251)

        sql = f"select count(*),_group_key(usi) from {self.stb} group by usi;"
        tdSql.execute(sql)
        tdSql.checkRows(997)

        # tail
        sql1 = "select ts,ui from d0 order by ts desc limit 5 offset 2;"
        sql2 = "select ts,tail(ui,5,2) from d0;"
        self.checkSameResult(sql1, sql2)

        # uninqe
        sql1 = "select distinct uti from d0 order by uti;"
        sql2 = "select UNIQUE(uti) from d0 order by uti asc;"
        self.checkSameResult(sql1, sql2)


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # check 
        self.checkConsistency("usi")

        # do action
        self.doQuery()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
