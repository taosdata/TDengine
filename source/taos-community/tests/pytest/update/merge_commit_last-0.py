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

        self.ts = 1603152000000
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use db")

    def run(self):              
        tdSql.prepare()

        print("==============step 1: UPDATE THE LAST RECORD REPEATEDLY")                     
        tdSql.execute("create table t1 (ts timestamp, a int)")

        for i in range(5):
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts, i))
            self.restartTaosd()
            tdSql.query("select * from t1")            
            tdSql.checkRows(1)            
            tdSql.checkData(0, 1, 0)

        print("==============step 2: UPDATE THE WHOLE LAST BLOCK")
        tdSql.execute("create table t2 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t2 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t2")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t2")
        tdSql.checkData(0, 0, 50)

        for i in range(50):
            tdSql.execute("insert into t2 values(%d, 2)" % (self.ts + i))
        tdSql.query("select * from t2")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t2")
        tdSql.checkData(0, 0, 50)

        self.restartTaosd()
        tdSql.query("select * from t2")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t2")
        tdSql.checkData(0, 0, 50)

        print("==============step 3: UPDATE PART OF THE LAST BLOCK")
        tdSql.execute("create table t3 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t3 values(%d, 1)" % (self.ts + i))
        self.restartTaosd()
        tdSql.query("select * from t3")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t3")
        tdSql.checkData(0, 0, 50)

        for i in range(25):
            tdSql.execute("insert into t3 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t3")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t3")
        tdSql.checkData(0, 0, 50)

        self.restartTaosd()
        tdSql.query("select * from t3")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t3")
        tdSql.checkData(0, 0, 50)
        
        print("==============step 4: UPDATE AND INSERT APPEND AT END OF DATA")
        tdSql.execute("create table t4 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t4 values(%d, 1)" % (self.ts + i))
        
        self.restartTaosd()
        tdSql.query("select * from t4")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t4")
        tdSql.checkData(0, 0, 50)

        for i in range(25):
            tdSql.execute("insert into t4 values(%d, 2)" % (self.ts + i))
        
        for i in range(50, 60):
            tdSql.execute("insert into t4 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t4")        
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t4")
        tdSql.checkData(0, 0, 70)
        
        self.restartTaosd()
        tdSql.query("select * from t4")        
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t4")
        tdSql.checkData(0, 0, 70)

        print("==============step 5: UPDATE AND INSERT PREPEND SOME DATA")
        tdSql.execute("create table t5 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t5 values(%d, 1)" % (self.ts + i))
        
        self.restartTaosd()
        tdSql.query("select * from t5")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t5")
        tdSql.checkData(0, 0, 50)
        
        for i in range(-10, 0):
            tdSql.execute("insert into t5 values(%d, 2)" % (self.ts + i))
        
        for i in range(25):
            tdSql.execute("insert into t5 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t5")
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t5")
        tdSql.checkData(0, 0, 70)

        self.restartTaosd()
        tdSql.query("select * from t5")
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t5")
        tdSql.checkData(0, 0, 70)

        for i in range(-10, 0):
            tdSql.execute("insert into t5 values(%d, 3)" % (self.ts + i))
        
        for i in range(25, 50):
            tdSql.execute("insert into t5 values(%d, 3)" % (self.ts + i))

        tdSql.query("select * from t5")        
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t5")
        tdSql.checkData(0, 0, 70)

        self.restartTaosd()
        tdSql.query("select * from t5")
        tdSql.checkRows(60)
        tdSql.query("select sum(a) from t5")
        tdSql.checkData(0, 0, 70)
        

        print("==============step 6: INSERT AHEAD A LOT OF DATA")
        tdSql.execute("create table t6 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t6 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t6")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0, 0, 50)
        
        for i in range(-1000, 0):
            tdSql.execute("insert into t6 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t6")
        tdSql.checkRows(1050)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0, 0, 2050)

        self.restartTaosd()
        tdSql.query("select * from t6")
        tdSql.checkRows(1050)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0, 0, 2050)

        print("==============step 7: INSERT AHEAD A LOT AND UPDATE")
        tdSql.execute("create table t7 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t7 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t7")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t7")
        tdSql.checkData(0, 0, 50)
        
        for i in range(-1000, 25):
            tdSql.execute("insert into t7 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t7")
        tdSql.checkRows(1050)
        tdSql.query("select sum(a) from t7")
        tdSql.checkData(0, 0, 2050)

        self.restartTaosd()
        tdSql.query("select * from t7")
        tdSql.checkRows(1050)
        tdSql.query("select sum(a) from t7")
        tdSql.checkData(0, 0, 2050)

        print("==============step 8: INSERT AFTER A LOT AND UPDATE")
        tdSql.execute("create table t8 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t8 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t8")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t8")
        tdSql.checkData(0, 0, 50)
        
        for i in range(25, 6000):
            tdSql.execute("insert into t8 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t8")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t8")
        tdSql.checkData(0, 0, 11950)

        self.restartTaosd()
        tdSql.query("select * from t8")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t8")
        tdSql.checkData(0, 0, 11950)

        print("==============step 9: UPDATE ONLY MIDDLE")
        tdSql.execute("create table t9 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t9 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t9")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t9")
        tdSql.checkData(0, 0, 50)
        
        for i in range(20, 30):
            tdSql.execute("insert into t9 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t9")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t9")
        tdSql.checkData(0, 0, 50)

        self.restartTaosd()
        tdSql.query("select * from t9")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t9")
        tdSql.checkData(0, 0, 50)
        
        print("==============step 10: A LOT OF DATA COVER THE WHOLE BLOCK")
        tdSql.execute("create table t10 (ts timestamp, a int)")

        for i in range(50):
            tdSql.execute("insert into t10 values(%d, 1)" % (self.ts + i))

        self.restartTaosd()
        tdSql.query("select * from t10")
        tdSql.checkRows(50)
        tdSql.query("select sum(a) from t10")
        tdSql.checkData(0, 0, 50)
        
        for i in range(-4000, 4000):
            tdSql.execute("insert into t10 values(%d, 2)" % (self.ts + i))
        
        tdSql.query("select * from t10")
        tdSql.checkRows(8000)
        tdSql.query("select sum(a) from t10")
        tdSql.checkData(0, 0, 15950)

        self.restartTaosd()
        tdSql.query("select * from t10")
        tdSql.checkRows(8000)
        tdSql.query("select sum(a) from t10")
        tdSql.checkData(0, 0, 15950)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
