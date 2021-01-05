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
                    col7 bool, col8 binary(20), col9 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d')" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # top verifacation 
        tdSql.error("select top(ts, 10) from test")
        tdSql.error("select top(col1, 0) from test")
        tdSql.error("select top(col1, 101) from test")
        tdSql.error("select top(col2, 0) from test")
        tdSql.error("select top(col2, 101) from test")
        tdSql.error("select top(col3, 0) from test")
        tdSql.error("select top(col3, 101) from test")
        tdSql.error("select top(col4, 0) from test")
        tdSql.error("select top(col4, 101) from test")
        tdSql.error("select top(col5, 0) from test")
        tdSql.error("select top(col5, 101) from test")
        tdSql.error("select top(col6, 0) from test")
        tdSql.error("select top(col6, 101) from test")        
        tdSql.error("select top(col7, 10) from test")        
        tdSql.error("select top(col8, 10) from test")
        tdSql.error("select top(col9, 10) from test")

        tdSql.query("select top(col1, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col2, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col3, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col4, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 10)

        tdSql.query("select top(col5, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 8.1)
        tdSql.checkData(1, 1, 9.1)

        tdSql.query("select top(col6, 2) from test")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 8.1)
        tdSql.checkData(1, 1, 9.1)
        
        #TD-2563 top + super_table + interval 
        tdSql.execute("create table meters(ts timestamp, c int) tags (d int)") 
        tdSql.execute("create table t1 using meters tags (1)") 
        sql = 'insert into t1 values '       
        for i in range(20000):
            sql = sql + '(%d, %d)' % (self.ts + i , i % 47)
            if i % 2000 == 0:
                tdSql.execute(sql)
                sql = 'insert into t1 values ' 
        tdSql.execute(sql)
        tdSql.query('select top(c,1) from meters interval(10a)')
        tdSql.checkData(0,1,9)
        
                   
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
