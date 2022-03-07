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
import random

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 5000
        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        tdSql.execute(
            "create table if not exists st (ts timestamp, value nchar(50), speed int) tags(dev nchar(50))")
        tdSql.execute(
            "create table t1 using st tags('dev_001')")

        for i in range(self.rowNum):
            tdSql.execute(
                "insert into t1 values(%d, 'taosdata%d', %d)" %
                (self.ts + i, i + 1, i + 1))

        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)

        # TS-717
        tdLog.info("case for TS-717")
        cachelast_values = [0, 1, 3]

        for value in cachelast_values:
            tdLog.info("case for cachelast value: %d" % value)
            tdSql.execute("drop database if exists db")
            tdLog.sleep(1)
            tdSql.execute("create database db cachelast %d" % value)
            tdSql.execute("use db")
            tdSql.execute("create table stb(ts timestamp, c1 int, c2 binary(20), c3 binary(5)) tags(t1 int)")

            sql = "insert into t1 using stb tags(1) (ts, c1, c2) values"
            for i in range(self.rowNum):
                sql += "(%d, %d, 'test')" % (self.ts + i, random.randint(1,100))
            tdSql.execute(sql)

            tdSql.query("select * from stb")
            tdSql.checkRows(self.rowNum)

            tdDnodes.stop(1)
            tdDnodes.start(1)

            tdSql.query("select * from stb")
            tdSql.checkRows(self.rowNum)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
