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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 36500")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table && insert data")
        tdSql.execute(
            "create table stb1 (ts timestamp, c11 int) TAGS(t11 int, t12 int )"
        )
        tdSql.execute(
            "create table stb2 (ts timestamp, c21 int) TAGS(t21 int, t22 int )"
        )
        tdSql.execute("create table t10 using stb1 tags(1, 10)")
        tdSql.execute("create table t20 using stb2 tags(1, 12)")
        tdSql.execute("insert into t10 values (1600000000000, 1)")
        tdSql.execute("insert into t10 values (1610000000000, 2)")
        tdSql.execute("insert into t20 values (1600000000000, 3)")
        tdSql.execute("insert into t20 values (1610000000000, 4)")

        tdLog.printNoPrefix("==========step2:query crash test")
        tdSql.query("select stb1.c11, stb1.t11, stb1.t12 from stb2,stb1 where stb2.t21 = stb1.t11 and stb1.ts = stb2.ts")
        tdSql.checkRows(2)
        tdSql.query("select stb2.c21, stb2.t21, stb2.t21 from stb1, stb2 where stb2.t21 = stb1.t11 and stb1.ts = stb2.ts")
        tdSql.checkRows(2)
        tdSql.query("select top(stb2.c21,2) from stb1, stb2 where stb2.t21 = stb1.t11 and stb1.ts = stb2.ts")
        tdSql.checkRows(2)
        tdSql.query("select last(stb2.c21) from stb1, stb2 where stb2.t21 = stb1.t11 and stb1.ts = stb2.ts")
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())