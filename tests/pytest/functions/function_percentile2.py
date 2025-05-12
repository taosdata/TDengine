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

from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 1000000
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        tdSql.execute("create table test(ts timestamp, col1 int, col2 float, col3 double)")
        for i in range(1000):
            sql = "insert into test values"
            batchSize = int (self.rowNum / 1000)            
            for j in range (batchSize):                
                currTime = self.ts + batchSize * i + j
                sql += "(%d, 1, 2.37, 3.1415926)" % currTime
            tdSql.execute(sql)

        tdSql.query("select percentile(col1, 20) from test")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select percentile(col2, 20) from test")
        tdSql.checkData(0, 0, 2.3699998)

        tdSql.query("select percentile(col3, 20) from test")
        tdSql.checkData(0, 0, 3.1415926)

        tdSql.query("select apercentile(col1, 20) from test")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select apercentile(col2, 20) from test")
        tdSql.checkData(0, 0, 2.3699998)

        tdSql.query("select apercentile(col3, 20) from test")
        tdSql.checkData(0, 0, 3.1415926)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
