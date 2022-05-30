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

from platform import java_ver
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
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.rowNum):
            tdSql.execute("insert into test values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # percentile verifacation 
        tdSql.error("select percentile(ts ,20) from test")
        tdSql.error("select percentile(col7 ,20) from test")
        tdSql.error("select percentile(col8 ,20) from test")        
        tdSql.error("select percentile(col9 ,20) from test") 
        column_list = [1,2,3,4,11,12,13,14]
        percent_list = [0,50,100]  
        for i in column_list:  
            for j in percent_list:
                tdSql.query(f"select percentile(col{i}, {j}) from test")        
                tdSql.checkData(0, 0, np.percentile(intData, j)) 

        for i in [5,6]:
            for j in percent_list:
                tdSql.query(f"select percentile(col{i}, {j}) from test")
                tdSql.checkData(0, 0, np.percentile(floatData, j))
        
        tdSql.execute("create table meters (ts timestamp, voltage int) tags(loc nchar(20))")
        tdSql.execute("create table t0 using meters tags('beijing')")
        tdSql.execute("create table t1 using meters tags('shanghai')")
        for i in range(self.rowNum):
            tdSql.execute("insert into t0 values(%d, %d)" % (self.ts + i, i + 1))            
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i, i + 1))            
        
        # tdSql.error("select percentile(voltage, 20) from meters")


 
        tdSql.execute("create table st(ts timestamp, k int)")
        tdSql.execute("insert into st values(now, -100)(now+1a,-99)")
        tdSql.query("select apercentile(k, 20) from st")
        tdSql.checkData(0, 0, -100.00)


        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
