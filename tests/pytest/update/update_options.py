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

        self.ts = 1606700000000
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use db")

    def run(self):            
        tdSql.prepare()

        # test case for TD-2279
        print("==============step1")        
        tdSql.execute("create table t (ts timestamp, a int)")

        for i in range(3276):
            tdSql.execute("insert into t values(%d, 0)" % (self.ts + i))

        newTs = 1606700010000
        for i in range(3275):
            tdSql.execute("insert into t values(%d, 0)" % (self.ts + i))
        tdSql.execute("insert into t values(%d, 0)" % 1606700013280)
        
        self.restartTaosd()
        
        for i in range(1606700003275, 1606700006609):
            tdSql.execute("insert into t values(%d, 0)" % i)
        tdSql.execute("insert into t values(%d, 0)" % 1606700006612)
        
        self.restartTaosd()

        tdSql.execute("insert into t values(%d, 0)" % 1606700006610)        
        tdSql.query("select * from t")
        tdSql.checkRows(6612)

        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdLog.sleep(3)
        
        # test case for https://jira.taosdata.com:18080/browse/TS-402
        tdLog.info("test case for update option 1")
        tdSql.execute("create database test update 1")
        tdSql.execute("use test")

        tdSql.execute("create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute("insert into tb values(%d, 1, 2, 3)(%d, null, null, 9)" % (self.ts, self.ts))

        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, 9)

        tdSql.execute("drop table if exists tb")
        tdSql.execute("create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute("insert into tb values(%d, 1, 2, 3)(%d, null, 4, 5)(%d, 6, null, 7)" % (self.ts, self.ts, self.ts))

        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, 7)

        # https://jira.taosdata.com:18080/browse/TS-424
        tdLog.info("test case for update option 2")
        tdSql.execute("create database db2 update 2")
        tdSql.execute("use db2")

        tdSql.execute("create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute("insert into tb values(%d, 1, 2, 3)(%d, null, null, 9)" % (self.ts, self.ts))

        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 9)

        tdSql.execute("drop table if exists tb")
        tdSql.execute("create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute("insert into tb values(%d, 1, 2, 3)(%d, null, 4, 5)(%d, 6, null, 7)" % (self.ts, self.ts, self.ts))

        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 7)

        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())