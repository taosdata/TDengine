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
#import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        col0 = 0
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, col0, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))

        # operation not allowed on super table
        tdSql.error("select count(*) from test session(ts, 1s)")
        # operation not allowde on col pro
        tdSql.error("select * from test1 session(ts, 1s)")
        # operation not allowed on col except primary ts 
        tdSql.error("select * from test1 session(col1, 1s)")

        tdSql.query("select count(*) from test1 state_window(col1)")  

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.rowNum)
        # append more data

        col0 = col0 + 1
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i + 10000, col0, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))

        tdSql.query("select count(*) from test1 state_window(col1)")  

        tdSql.checkRows(2)
        tdSql.checkData(0, 0, self.rowNum)
        tdSql.checkData(1, 0, self.rowNum)

        
        tdSql.query("select first(col1) from test1 state_window(col1)")  
        tdSql.checkRows(2)
        col0 = col0 - 1
        tdSql.checkData(0, 0, col0)
        col0 = col0 + 1
        tdSql.checkData(1, 0, col0)

        tdSql.query("select first(col2) from test1 state_window(col1)")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        tdSql.query("select count(col1), first(col2) from test1 state_window(col1)")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 10)
        tdSql.checkData(1, 1, 1)


        #tdSql.query("select count(*) from test1 session(ts, 1m)")  
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 1, 11)

        #tdSql.query("select first(col1) from test1 session(ts, 1s)")  
        #tdSql.checkRows(2)
        #tdSql.checkData(0, 1, 1)
        #tdSql.checkData(1, 1, 1)

        #tdSql.query("select first(col1), last(col2) from test1 session(ts, 1s)")  
        #tdSql.checkRows(2)
        #tdSql.checkData(0, 1, 1)
        #tdSql.checkData(0, 2, 10)
        #tdSql.checkData(1, 1, 1)
        #tdSql.checkData(1, 1, 1)

        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
