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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.types = ["tinyint", "smallint", "int", "bigint", "float", "double", "bool", "binary(10)", "nchar(10)", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned"]
        self.ts = 1537146000000

    def checkNullValue(self, result):
        mx = np.array(result)
        [rows, cols] = mx.shape
        for i in range(rows):
            for j in range(cols):
                if j + 1 < cols and mx[i, j + 1] is not None:
                    print(mx[i, j + 1])
                    return False
        return True
    
    def run(self):
        tdSql.prepare()
        
        for i in range(len(self.types)):
            tdSql.execute("drop table if exists t0")
            tdSql.execute("drop table if exists t1")
            
            print("======== checking type %s ==========" % self.types[i])
            tdSql.execute("create table t0 (ts timestamp, col %s)" % self.types[i])
            tdSql.execute("insert into t0 values (%d, NULL)" % (self.ts))
            
            tdDnodes.stop(1)
            # tdLog.sleep(10)
            tdDnodes.start(1)
            tdSql.execute("use db")
            tdSql.query("select * from t0")
            tdSql.checkRows(1)

            if self.checkNullValue(tdSql.queryResult) is False:
                tdLog.exit("no None value is detected")

            tdSql.execute("create table t1 (ts timestamp, col %s)" % self.types[i])
            tdSql.execute("insert into t1 values (%d, NULL)" % (self.ts))
            tdDnodes.stop(1)
            # tdLog.sleep(10)
            tdDnodes.start(1)
            tdSql.execute("use db")

            for j in range(150):
                tdSql.execute("insert into t1 values (%d, NULL)" % (self.ts + j + 1));
            
            tdSql.query("select * from t1")
            tdSql.checkRows(151)

            if self.checkNullValue(tdSql.queryResult) is False:
                tdLog.exit("no None value is detected")

            print("======== None value check for type %s is OK ==========" % self.types[i])

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
