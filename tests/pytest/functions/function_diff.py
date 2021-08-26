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
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.perfix = 'dev'
        self.tables = 10

    def insertData(self):
        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, col int) tags(dev nchar(50))")

        for i in range(self.tables):
            tdSql.execute("create table %s%d using st tags(%d)" % (self.perfix, i, i))
            rows = 15 + i
            for j in range(rows):
                tdSql.execute("insert into %s%d values(%d, %d)" %(self.perfix, i, self.ts + i * 20 * 10000 + j * 10000, j))

    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        tdSql.execute("insert into test1 values(%d, 0, 0, 0, 0, 0.0, 0.0, False, ' ', ' ', 0, 0, 0, 0)" % (self.ts - 1))
        
        # diff verifacation 
        tdSql.query("select diff(col1) from test1")
        tdSql.checkRows(0)
        
        tdSql.query("select diff(col2) from test1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col3) from test1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col4) from test1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col5) from test1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col6) from test1")
        tdSql.checkRows(0)

        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))                                
        
        tdSql.error("select diff(ts) from test")
        tdSql.error("select diff(ts) from test1")
        tdSql.error("select diff(col1) from test")
        tdSql.error("select diff(col2) from test")
        tdSql.error("select diff(col3) from test")
        tdSql.error("select diff(col4) from test")
        tdSql.error("select diff(col5) from test")
        tdSql.error("select diff(col6) from test")
        tdSql.error("select diff(col7) from test")     
        tdSql.error("select diff(col7) from test1")               
        tdSql.error("select diff(col8) from test") 
        tdSql.error("select diff(col8) from test1")
        tdSql.error("select diff(col9) from test")        
        tdSql.error("select diff(col9) from test1")
        tdSql.error("select diff(col11) from test1")
        tdSql.error("select diff(col12) from test1")
        tdSql.error("select diff(col13) from test1")
        tdSql.error("select diff(col14) from test1")
        tdSql.error("select diff(col11) from test")
        tdSql.error("select diff(col12) from test")
        tdSql.error("select diff(col13) from test")
        tdSql.error("select diff(col14) from test")

        tdSql.query("select ts,diff(col1),ts from test1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from test group by tbname")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from test1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from test group by tbname")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select diff(col1) from test1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col2) from test1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col3) from test1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col4) from test1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col5) from test1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col6) from test1")
        tdSql.checkRows(10)

        self.insertData()

        tdSql.query("select diff(col) from st group by tbname")
        tdSql.checkRows(185)

        tdSql.error("select diff(col) from st group by dev")        

        tdSql.error("select diff(col) from st group by col")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
