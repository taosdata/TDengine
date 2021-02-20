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

        # sum verifacation 
        tdSql.error("select sum(ts) from test")
        tdSql.error("select sum(col7) from test")
        tdSql.error("select sum(col8) from test")
        tdSql.error("select sum(col9) from test")

        tdSql.query("select sum(col1) from test")
        tdSql.checkData(0, 0, np.sum(intData))
        tdSql.query("select sum(col2) from test")
        tdSql.checkData(0, 0, np.sum(intData))
        tdSql.query("select sum(col3) from test")
        tdSql.checkData(0, 0, np.sum(intData))
        tdSql.query("select sum(col4) from test")
        tdSql.checkData(0, 0, np.sum(intData))
        tdSql.query("select sum(col5) from test")
        tdSql.checkData(0, 0, np.sum(floatData)) 
        tdSql.query("select sum(col6) from test")
        tdSql.checkData(0, 0, np.sum(floatData))              

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
