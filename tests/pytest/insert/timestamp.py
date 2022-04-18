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
        self.ts = 1607281690000

    def run(self):
        tdSql.prepare()

        # TS-806
        tdLog.info("test case for TS-806")

        # Case 1
        tdSql.execute("create table t1(ts timestamp, c1 int)")
        tdSql.execute("insert into t1(c1, ts) values(1, %d)" % self.ts)
        tdSql.query("select * from t1")
        tdSql.checkRows(1)

        # Case 2
        tdSql.execute(
            "insert into t1(c1, ts) values(2, %d)(3, %d)" %
            (self.ts + 1000, self.ts + 2000))
        tdSql.query("select * from t1")
        tdSql.checkRows(3)

        # Case 3
        tdSql.execute("create table t2(ts timestamp, c1 timestamp)")
        tdSql.execute(
            " insert into t2(c1, ts) values(%d, %d)" %
            (self.ts, self.ts + 5000))
        tdSql.query("select * from t2")
        tdSql.checkRows(1)

        tdSql.execute(
            " insert into t2(c1, ts) values(%d, %d)(%d, %d)" %
            (self.ts, self.ts + 6000, self.ts + 3000, self.ts + 8000))
        tdSql.query("select * from t2")
        tdSql.checkRows(3)

        # Case 4
        tdSql.execute(
            "create table stb(ts timestamp, c1 int, c2 binary(20)) tags(tstag timestamp, t1 int)")
        tdSql.execute(
            "insert into tb1(c2, ts, c1) using stb(t1, tstag) tags(1, now) values('test', %d, 1)" %
            self.ts)
        tdSql.query("select * from stb")
        tdSql.checkRows(1)

        # Case 5
        tdSql.execute(
            "insert into tb1(c2, ts, c1) using stb(t1, tstag) tags(1, now) values('test', now, 1) tb2(c1, ts) using stb tags(now + 2m, 1000) values(1, now - 1h)")
        tdSql.query("select * from stb")
        tdSql.checkRows(3)

        tdSql.execute(" insert into tb1(c2, ts, c1) using stb(t1, tstag) tags(1, now) values('test', now + 10s, 1) tb2(c1, ts) using stb(tstag) tags(now + 2m) values(1, now - 3h)(2, now - 2h)")
        tdSql.query("select * from stb")
        tdSql.checkRows(6)

        # Case 6
        tdSql.execute(
            "create table stb2 (ts timestamp, c1 timestamp, c2 timestamp) tags(t1 timestamp, t2 timestamp)")
        tdSql.execute(" insert into tb4(c1, c2, ts) using stb2(t2, t1) tags(now, now + 1h) values(now + 1s, now + 2s, now + 3s)(now -1s, now - 2s, now - 3s) tb5(c2, ts, c1) using stb2(t2) tags(now + 1h) values(now, now, now)")
        tdSql.query("select * from stb2")
        tdSql.checkRows(3)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
