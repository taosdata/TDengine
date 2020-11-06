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
        tdSql.init(conn.cursor(), logSql)
        
        self.numOfRecords = 10
        self.ts = 1604295582000    
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute("use udb")

    def run(self):
        tdSql.prepare()
        startTs = self.ts

        print("==============step1")        
        tdSql.execute("create database udb update 0")
        tdSql.execute("use udb")
        tdSql.execute("create table t (ts timestamp, a int)")
        tdSql.execute("insert into t values (%d, 1)" % (startTs))
        tdSql.execute("insert into t values (%d, 1)" % (startTs - 3))
        tdSql.execute("insert into t values (%d, 1)" % (startTs + 3))
        
        tdSql.query("select * from t")
        tdSql.checkRows(3)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        print("==============step2")
        tdSql.execute("insert into t values (%d, 2)" % (startTs))
        tdSql.execute("insert into t values (%d, 2)" % (startTs - 3))
        tdSql.execute("insert into t values (%d, 2)" % (startTs + 3))
        
        tdSql.query("select * from t")
        tdSql.checkRows(3)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        print("==============step3")
        tdSql.execute("insert into t values (%d, 3)" % (startTs - 4))
        tdSql.execute("insert into t values (%d, 3)" % (startTs - 2))
        tdSql.execute("insert into t values (%d, 3)" % (startTs + 2))
        tdSql.execute("insert into t values (%d, 3)" % (startTs + 4))
        
        tdSql.query("select * from t")
        tdSql.checkRows(7)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 3)
        
        print("==============step4")
        tdSql.execute("insert into t values (%d, 4)" % (startTs - 4))
        tdSql.execute("insert into t values (%d, 4)" % (startTs - 2))
        tdSql.execute("insert into t values (%d, 4)" % (startTs + 2))
        tdSql.execute("insert into t values (%d, 4)" % (startTs + 4))
        
        tdSql.query("select * from t")
        tdSql.checkRows(7)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 3)
        
        print("==============step5")
        tdSql.execute("insert into t values (%d, 5)" % (startTs - 1))
        tdSql.execute("insert into t values (%d, 5)" % (startTs + 1))
        
        tdSql.query("select * from t")
        tdSql.checkRows(9)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 3)        
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 5)        
        tdSql.checkData(6, 0, 3)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 3)
        
        print("==============step6")
        tdSql.execute("insert into t values (%d, 6)" % (startTs - 4))
        tdSql.execute("insert into t values (%d, 6)" % (startTs - 3))
        tdSql.execute("insert into t values (%d, 6)" % (startTs - 2))
        tdSql.execute("insert into t values (%d, 6)" % (startTs - 1))
        tdSql.execute("insert into t values (%d, 6)" % (startTs))
        tdSql.execute("insert into t values (%d, 6)" % (startTs + 1))
        tdSql.execute("insert into t values (%d, 6)" % (startTs + 2))
        tdSql.execute("insert into t values (%d, 6)" % (startTs + 3))
        tdSql.execute("insert into t values (%d, 6)" % (startTs + 4))
        
        tdSql.query("select * from t")
        tdSql.checkRows(9)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 3)        
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 5)        
        tdSql.checkData(6, 0, 3)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 3)
        
        # restart taosd to commit, and check
        self.restartTaosd();
        
        tdSql.query("select * from t")
        tdSql.checkRows(9)
        
        tdSql.query("select a from t")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 3)        
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 5)        
        tdSql.checkData(6, 0, 3)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 3)        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
