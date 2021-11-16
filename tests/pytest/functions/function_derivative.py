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

    def insertAndCheckData(self):
        types = ["tinyint", "tinyint unsigned", "smallint", "smallint unsigned", "int", "int unsigned", "bigint", "bigint unsigned", "float", "double", "bool", "binary(20)", "nchar(20)"]

        for type in types:
            print("============== create table using %s type ================" % type)
            tdSql.execute("drop table if exists stb")
            tdSql.execute("create table stb(ts timestamp, col %s) tags (id int)" % type)
            tdSql.execute("create table tb1 using stb tags(1)")
            tdSql.execute("create table tb2 using stb tags(2)")

            if type == "tinyint" or type == "smallint" or type == "int" or type == "bigint":
                tdSql.execute("insert into tb1 values(%d, 1)(%d, 11)(%d, 21)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb1 values(%d, -1)(%d, -11)(%d, -21)" % (self.ts + 30000, self.ts + 40000, self.ts + 50000))
                tdSql.execute("insert into tb2 values(%d, 10)(%d, 20)(%d, 30)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))
                tdSql.execute("insert into tb2 values(%d, -10)(%d, -20)(%d, -30)" % (self.ts + 90000, self.ts + 1000000, self.ts + 1100000))

                tdSql.execute("insert into tb3 using stb tags(3) values(%d, 10)" % (self.ts + 1200000))
                
                tdSql.query("select derivative(col, 1s, 1) from stb group by tbname")
                tdSql.checkRows(4)

                tdSql.query("select derivative(col, 10s, 1) from stb group by tbname")
                tdSql.checkRows(4)

                tdSql.query("select derivative(col, 10s, 0) from stb group by tbname")
                tdSql.checkRows(10)

                tdSql.query("select ts,derivative(col, 10s, 1),ts from stb group by tbname")
                tdSql.checkRows(4)
                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 1, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 3, "2018-09-17 09:00:10.000")
                tdSql.checkData(3, 0, "2018-09-17 09:01:20.000")
                tdSql.checkData(3, 1, "2018-09-17 09:01:20.000")
                tdSql.checkData(3, 3, "2018-09-17 09:01:20.000")

                tdSql.query("select ts,derivative(col, 10s, 1),ts from tb1")
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 1, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 3, "2018-09-17 09:00:10.000")
                tdSql.checkData(1, 0, "2018-09-17 09:00:20.009")
                tdSql.checkData(1, 1, "2018-09-17 09:00:20.009")
                tdSql.checkData(1, 3, "2018-09-17 09:00:20.009")

                tdSql.query("select ts from(select ts,derivative(col, 10s, 0) from stb group by tbname)")

                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")

                tdSql.error("select derivative(col, 10s, 0) from tb1 group by tbname")

                tdSql.query("select derivative(col, 10s, 1) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb1")
                tdSql.checkRows(5)

                tdSql.query("select derivative(col, 10s, 1) from tb2")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb2")
                tdSql.checkRows(5)

                tdSql.query("select derivative(col, 10s, 0) from tb3")
                tdSql.checkRows(0)
                
            elif type == "tinyint unsigned" or type == "smallint unsigned" or type == "int unsigned" or type == "bigint unsigned":
                tdSql.execute("insert into tb1 values(%d, 1)(%d, 11)(%d, 21)" % (self.ts, self.ts + 10000, self.ts + 20000))                
                tdSql.execute("insert into tb2 values(%d, 10)(%d, 20)(%d, 30)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.error("select derivative(col, 1s, 1) from tb1")
                tdSql.error("select derivative(col, 10s, 0) from tb1")
                tdSql.error("select derivative(col, 999ms, 0) from tb1")
                tdSql.error("select derivative(col, 1s, 1) from tb2")
                tdSql.error("select derivative(col, 10s, 0) from tb2")
                tdSql.error("select derivative(col, 999ms, 0) from tb2")

            elif type == "float" or type == "double":
                tdSql.execute("insert into tb1 values(%d, 1.0)(%d, 11.0)(%d, 21.0)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, 3.0)(%d, 4.0)(%d, 5.0)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.query("select derivative(col, 10s, 1) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 1) from tb2")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb2")
                tdSql.checkRows(2)

            elif type == "bool":
                tdSql.execute("insert into tb1 values(%d, true)(%d, false)(%d, true)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, false)(%d, true)(%d, true)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.error("select derivative(col, 1s, 1) from tb1")
                tdSql.error("select derivative(col, 10s, 0) from tb1")
                tdSql.error("select derivative(col, 999ms, 0) from tb1")
                tdSql.error("select derivative(col, 1s, 1) from tb2")
                tdSql.error("select derivative(col, 10s, 0) from tb2")
                tdSql.error("select derivative(col, 999ms, 0) from tb2")

            else:
                tdSql.execute("insert into tb1 values(%d, 'test01')(%d, 'test01')(%d, 'test01')" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, 'test01')(%d, 'test01')(%d, 'test01')" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.error("select derivative(col, 1s, 1) from tb1")
                tdSql.error("select derivative(col, 10s, 0) from tb1")
                tdSql.error("select derivative(col, 999ms, 0) from tb1")
                tdSql.error("select derivative(col, 1s, 1) from tb2")
                tdSql.error("select derivative(col, 10s, 0) from tb2")
                tdSql.error("select derivative(col, 999ms, 0) from tb2")

            tdSql.error("select derivative(col, 10s, 1) from stb")
            tdSql.error("select derivative(col, 10s, 1) from stb group by col")
            tdSql.error("select derivative(col, 10s, 1) from stb group by id")
            tdSql.error("select derivative(col, 999ms, 1) from stb group by id")
            tdSql.error("select derivative(col, 10s, 2) from stb group by id")

    def run(self):
        tdSql.prepare()        
        self.insertAndCheckData()

        tdSql.execute("create table st(ts timestamp, c1 int, c2 int) tags(id int)")
        tdSql.execute("insert into dev1(ts, c1) using st tags(1) values(now, 1)")

        tdSql.error("select derivative(c1, 10s, 0) from (select c1 from st)")
        tdSql.query("select diff(c1) from (select derivative(c1, 1s, 0) c1 from dev1)")
        tdSql.checkRows(0)
              
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
