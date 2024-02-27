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
from frame.autogen import *


class TDTestCase(TBase):
    updatecfgDict = {
    }

    def insertData(self):
        tdLog.info(f"create table and insert data.")
        self.stb = "stb"
        self.db = "db"
        self.childtable_count = 10
        self.insert_rows = 10000

        self.autoGen = AutoGen(startTs = 1600000000000*1000*1000, batch=500, fillOne=True)
        self.autoGen.create_db(self.db, 2, 3, "precision 'ns'")
        self.autoGen.create_stable(stbname = self.stb, tag_cnt = 5, column_cnt = 20, binary_len = 10, nchar_len = 5)
        self.autoGen.create_child(self.stb, "child", self.childtable_count)
        self.autoGen.insert_data(self.insert_rows, True)
        
        tdLog.info("create view.")
        tdSql.execute(f"use {self.db}")
        sqls = [
            "create view viewc0c1 as select c0,c1 from stb ",
            "create view viewc0c1c2 as select c0,c1,c2 from stb ",
            "create view viewc0c3 as select c0,c3 from stb where c3=1",
            "create view viewc0c4c5 as select c4,c5 from stb ",
            "create view viewc0c6 as select c0,c1,c6 from stb ",
            "create view viewc0c7 as select c0,c1 from stb ",
            "create view viewc0c7c8 as select c0,c7,c8 from stb where c8>0",
            "create view viewc0c3c1 as select c0,c3,c1 from stb ",
            "create view viewc2c4 as select c2,c4 from stb ",
            "create view viewc2c5 as select c2,c5 from stb ",
        ]
        tdSql.executes(sqls)

    def checkView(self):
        tdLog.info(f"check view like.")

        # like
        sql = f"show views like 'view%'"
        tdSql.query(sql)
        tdSql.checkRows(10)

        sql = f"show views like 'vie_c0c1c2'"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0,0,"viewc0c1c2")

        sql = f"show views like '%c2c_'"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0,0, "viewc2c4")
        tdSql.checkData(1,0, "viewc2c5")

        sql = f"show views like '%' "
        tdSql.query(sql)
        tdSql.checkRows(10)
        
        # zero
        sql = "show views like '_' "
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "show views like 'a%' "
        tdSql.query(sql)
        tdSql.checkRows(0)


    def doQuery(self):
        tdLog.info(f"do query.")

        # __group_key
        sql = f"select count(*) from {self.stb} "
        tdSql.query(sql)
        # column index 1 value same with 2
        allRows = self.insert_rows * self.childtable_count
        tdSql.checkFirstValue(sql, allRows)

    def checkShow(self):
        # not support
        sql = "show accounts;"
        tdSql.error(sql)

        # check result
        sql = "SHOW CLUSTER;"
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = "SHOW COMPACTS;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW COMPACT 1;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW CLUSTER MACHINES;"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # run to check crash 
        sqls = [
            "show scores;",
            "SHOW CLUSTER VARIABLES",
            "SHOW BNODES;",
        ]
        tdSql.executes(sqls)


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check view
        self.checkView()

        # do action
        self.doQuery()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
