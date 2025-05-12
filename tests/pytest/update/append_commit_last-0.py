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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.ts = 1604298064000
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use db")

    def run(self):            
        tdSql.prepare()

        print("==============step1")        
        tdSql.execute("create table t1 (ts timestamp, a int)")

        for i in range(10):
            tdSql.execute("insert into t1 values(%d, 1)" % (self.ts + i))
            self.restartTaosd()
            tdSql.query("select * from t1")
            tdSql.checkRows(i + 1)
            tdSql.query("select sum(a) from t1")
            tdSql.checkData(0, 0, i + 1)

        print("==============step2")
        tdSql.execute("create table t2 (ts timestamp, a int)")
        tdSql.execute("insert into t2 values(%d, 1)" % self.ts)
        self.restartTaosd()
        tdSql.query("select * from t2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        for i in range(1, 151):
            tdSql.execute("insert into t2 values(%d, 1)" % (self.ts + i))
        
        self.restartTaosd()
        tdSql.query("select * from t2")
        tdSql.checkRows(151)
        tdSql.query("select sum(a) from t2")
        tdSql.checkData(0, 0, 151)


        print("==============step3")
        tdSql.execute("create table t3 (ts timestamp, a int)")
        tdSql.execute("insert into t3 values(%d, 1)" % self.ts)
        self.restartTaosd()
        tdSql.query("select * from t3")
        tdSql.checkRows(1)        
        tdSql.checkData(0, 1, 1)

        for i in range(8):
            for j in range(1, 11):
                tdSql.execute("insert into t3 values(%d, 1)" % (self.ts + i * 10 + j))
        
        self.restartTaosd()
        tdSql.query("select * from t3")
        tdSql.checkRows(81)
        tdSql.query("select sum(a) from t3")
        tdSql.checkData(0, 0, 81)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
