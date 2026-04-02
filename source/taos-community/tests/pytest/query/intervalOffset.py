###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def general(self):
        tdSql.execute("create table meters(ts timestamp, s int) tags(id int)")
        tdSql.execute("create table t0 using meters tags(0)")
        tdSql.execute("create table t1 using meters tags(1)")
        tdSql.execute("create table t2 using meters tags(2)")
        tdSql.execute("create table t3 using meters tags(3)")
        tdSql.execute("create table t4 using meters tags(4)")

        tdSql.execute("insert into t0 values('2019-01-01 00:00:00', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:00:01', 1)")
        tdSql.execute("insert into t2 values('2019-01-01 00:01:00', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:01:01', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:01:02', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:01:03', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:01:30', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:01:50', 1)")
        tdSql.execute("insert into t2 values('2019-01-01 00:02:00', 1)")
        tdSql.execute("insert into t3 values('2019-01-01 00:02:02', 1)")
        tdSql.execute("insert into t3 values('2019-01-01 00:02:59', 1)")
        tdSql.execute("insert into t4 values('2019-01-01 00:02:59', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:03:10', 1)")
        tdSql.execute("insert into t2 values('2019-01-01 00:08:00', 1)")
        tdSql.execute("insert into t1 values('2019-01-01 00:08:00', 1)")

        tdSql.query("select count(*) from meters interval(1m, 1s)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 2)

        tdSql.query("select count(*) from meters interval(1m, 2s)")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 2)

        tdSql.query("select count(*) from meters interval(90s, 1500a)")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)

    def singleTable(self):
        tdSql.execute("create table car(ts timestamp, s int)")
        tdSql.execute("insert into car values('2019-01-01 00:00:00', 1)")
        tdSql.execute("insert into car values('2019-05-13 12:00:00', 1)")
        tdSql.execute("insert into car values('2019-12-31 23:59:59', 1)")
        tdSql.execute("insert into car values('2020-01-01 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-01-02 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-01-03 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-01-04 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-01-05 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-01-31 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-02-01 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-02-02 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-02-29 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-03-01 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-03-02 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-03-15 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-03-31 12:00:00', 1)")
        tdSql.execute("insert into car values('2020-05-01 12:00:00', 1)")

        tdSql.query("select count(*) from car interval(1n, 10d)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from car interval(1n, 10d) order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from car interval(2n, 5d)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 3)

        tdSql.query("select count(*) from car interval(2n) order by ts desc")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 1)

        tdSql.query("select count(*) from car interval(1y, 1n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(2, 1, 8)

        tdSql.query("select count(*) from car interval(1y, 2n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(2, 1, 5)

        tdSql.query("select count(*) from car where ts > '2019-05-14 00:00:00' interval(1y, 5d)")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(1, 1, 9)

    def superTable(self):
        tdSql.execute("create table cars(ts timestamp, s int) tags(id int)")
        tdSql.execute("create table car0 using cars tags(0)")
        tdSql.execute("create table car1 using cars tags(1)")
        tdSql.execute("create table car2 using cars tags(2)")
        tdSql.execute("create table car3 using cars tags(3)")
        tdSql.execute("create table car4 using cars tags(4)")

        tdSql.execute("insert into car0 values('2019-01-01 00:00:00', 1)")
        tdSql.execute("insert into car1 values('2019-05-13 12:00:00', 1)")
        tdSql.execute("insert into car2 values('2019-12-31 23:59:59', 1)")
        tdSql.execute("insert into car1 values('2020-01-01 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-01-02 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-01-03 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-01-04 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-01-05 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-01-31 12:00:00', 1)")
        tdSql.execute("insert into car1 values('2020-02-01 12:00:00', 1)")
        tdSql.execute("insert into car2 values('2020-02-02 12:00:00', 1)")
        tdSql.execute("insert into car2 values('2020-02-29 12:00:00', 1)")
        tdSql.execute("insert into car3 values('2020-03-01 12:00:00', 1)")
        tdSql.execute("insert into car3 values('2020-03-02 12:00:00', 1)")
        tdSql.execute("insert into car3 values('2020-03-15 12:00:00', 1)")
        tdSql.execute("insert into car4 values('2020-03-31 12:00:00', 1)")
        tdSql.execute("insert into car3 values('2020-05-01 12:00:00', 1)")

        tdSql.query("select count(*) from cars interval(1n, 10d)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from cars interval(1n, 10d) order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from cars interval(2n, 5d)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 3)

        tdSql.query("select count(*) from cars interval(2n) order by ts desc")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 6)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 1)

        tdSql.query("select count(*) from cars interval(1y, 1n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(2, 1, 8)

        tdSql.query("select count(*) from cars interval(1y, 2n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(2, 1, 5)

        tdSql.query("select count(*) from cars where ts > '2019-05-14 00:00:00' interval(1y, 5d)")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(1, 1, 9)

    def run(self):
        tdSql.prepare()
        self.general()
        self.singleTable()
        self.superTable()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

