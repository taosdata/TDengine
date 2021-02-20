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
        
    def run(self):
        tdSql.prepare()

        intData = []        
        floatData = []

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # max verifacation 
        tdSql.error("select max(ts) from test")
        tdSql.error("select max(ts) from test1")
        tdSql.error("select max(col7) from test")
        tdSql.error("select max(col7) from test1")
        tdSql.error("select max(col8) from test")
        tdSql.error("select max(col8) from test1")
        tdSql.error("select max(col9) from test")
        tdSql.error("select max(col9) from test1")
        
        tdSql.query("select max(col1) from test1")
        tdSql.checkData(0, 0, np.max(intData))

        tdSql.query("select max(col2) from test1")
        tdSql.checkData(0, 0, np.max(intData))

        tdSql.query("select max(col3) from test1")
        tdSql.checkData(0, 0, np.max(intData))

        tdSql.query("select max(col4) from test1")
        tdSql.checkData(0, 0, np.max(intData))
        
        tdSql.query("select max(col5) from test1")
        tdSql.checkData(0, 0, np.max(floatData))

        tdSql.query("select max(col6) from test1")
        tdSql.checkData(0, 0, np.max(floatData))

        # test case: https://jira.taosdata.com:18080/browse/TD-2583
        tdSql.execute("create database test days 2")
        tdSql.execute("create table car(ts timestamp, speed int)")
        tdSql.execute("insert into car values(now, -1)")
        tdSql.execute("insert into car values(now-10d, null)")

        tdSql.query("select max(speed) from car")
        tdSql.checkData(0, 0, -1)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
