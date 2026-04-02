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

        tdSql.execute('''create table test(ts timestamp, col1 int, col2 int) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        tdSql.execute("create table test2 using test tags('shanghai')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d)" % (self.ts + i, i + 1, i + 1))
            tdSql.execute("insert into test2 values(%d, %d, %d)" % (self.ts + i, i + 1, i + 1))

        # arithmetic verifacation
        tdSql.query("select 0.1 + 0.1 from test")
        tdSql.checkRows(self.rowNum * 2)
        for i in range(self.rowNum * 2):
            tdSql.checkData(0, 0, 0.20000000)
        
        tdSql.query("select 4 * avg(col1) from test")
        tdSql.checkRows(1)        
        tdSql.checkData(0, 0, 22)

        tdSql.query("select 4 * sum(col1) from test")
        tdSql.checkRows(1)        
        tdSql.checkData(0, 0, 440)

        tdSql.query("select 4 * avg(col1) * sum(col2) from test")
        tdSql.checkRows(1)        
        tdSql.checkData(0, 0, 2420)

        tdSql.query("select 4 * avg(col1) * sum(col2) from test group by loc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1210)
        tdSql.checkData(1, 0, 1210)

        tdSql.error("select avg(col1 * 2)from test group by loc")
                

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
