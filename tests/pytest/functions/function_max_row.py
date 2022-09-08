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
        self.tables = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        intData = []
        floatData = []

        tdSql.execute("create table stb (ts timestamp, c1 int, c2 double, c3 float) tags(t1 int)")
        for i in range(self.tables):
            tdSql.execute("create table tb%d using stb tags(%d)" % (i, i))
            sql = "insert into tb%d values" % i
            for j in range(self.rowNum):
                sql += "(%d, %d, %f, %f)" % (self.ts + j * 3000, j, j + 0.1, j + 0.1)
                intData.append(j)
                floatData.append(j + 0.1)
            tdSql.execute(sql)
        
        tdSql.error("select max_row(ts) from stb")
        tdSql.error("select max_row(t1) from stb")
        
        tdSql.query("select max_row(c1) from stb")
        tdSql.checkData(0, 0, np.max(intData))

        tdSql.query("select max_row(c1), * from stb")
        tdSql.checkData(0, 0, np.max(intData))
        tdSql.checkData(0, 2, np.max(intData))
        tdSql.checkData(0, 3, np.max(floatData))
        tdSql.checkData(0, 4, np.max(floatData))

        tdSql.query("select max_row(c1), * from stb group by tbname")
        for i in range(self.tables):
            tdSql.checkData(i, 0, np.max(intData))
            tdSql.checkData(i, 2, np.max(intData))
            tdSql.checkData(i, 3, np.max(floatData))
            tdSql.checkData(i, 4, np.max(floatData))
        
        tdSql.query("select max_row(c1), * from stb interval(6s)")
        tdSql.checkRows(5)

        tdSql.query("select max_row(c1), * from tb1 interval(6s)")
        tdSql.checkRows(5)

        tdSql.query("select max_row(c1), * from stb interval(6s) group by tbname")
        tdSql.checkRows(50)

        tdSql.query("select max_row(c1), * from (select min_row(c1) c1, * from stb group by tbname)")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())