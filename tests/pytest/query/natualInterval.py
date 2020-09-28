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

        tdSql.query("select count(*) from car interval(1n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(5, 1, 4)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from car interval(1n) order by ts desc")
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select count(*) from car interval(2n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 9)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(5, 1, 1)

        tdSql.query("select count(*) from car interval(2n) order by ts desc")
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select count(*) from car interval(1y)")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 14)

        tdSql.query("select count(*) from car interval(2y)")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 14)


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

        tdSql.query("select count(*) from cars interval(1n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(5, 1, 4)
        tdSql.checkData(6, 1, 1)

        tdSql.query("select count(*) from cars interval(1n) order by ts desc")
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select count(*) from cars interval(2n)")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 9)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(5, 1, 1)

        tdSql.query("select count(*) from cars interval(2n) order by ts desc")
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select count(*) from cars interval(1y)")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 14)

        tdSql.query("select count(*) from cars interval(2y)")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 14)


    def run(self):
        tdSql.prepare()
        self.singleTable()
        self.superTable()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
