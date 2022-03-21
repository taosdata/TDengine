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
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table cars(ts timestamp, s int) tags(id int)")
        tdSql.execute("create table car0 using cars tags(0)")
        tdSql.execute("create table car1 using cars tags(1)")
        tdSql.execute("create table car2 using cars tags(2)")
        tdSql.execute("create table car3 using cars tags(3)")
        tdSql.execute("create table car4 using cars tags(4)")

        tdSql.execute("insert into car0 values('2019-01-01 00:00:00.103', 1)")
        tdSql.execute("insert into car1 values('2019-01-01 00:00:00.234', 1)")
        tdSql.execute("insert into car0 values('2019-01-01 00:00:01.012', 1)")
        tdSql.execute("insert into car0 values('2019-01-01 00:00:02.003', 1)")
        tdSql.execute("insert into car2 values('2019-01-01 00:00:02.328', 1)")
        tdSql.execute("insert into car0 values('2019-01-01 00:00:03.139', 1)")
        tdSql.execute("insert into car0 values('2019-01-01 00:00:04.348', 1)")
        tdSql.execute("insert into car0 values('2019-01-01 00:00:05.783', 1)")
        tdSql.execute("insert into car1 values('2019-01-01 00:00:01.893', 1)")
        tdSql.execute("insert into car1 values('2019-01-01 00:00:02.712', 1)")
        tdSql.execute("insert into car1 values('2019-01-01 00:00:03.982', 1)")
        tdSql.execute("insert into car3 values('2019-01-01 00:00:01.389', 1)")
        tdSql.execute("insert into car4 values('2019-01-01 00:00:01.829', 1)")

        tdSql.error("create table strm as select count(*) from cars")

        tdSql.execute("create table strm as select count(*) from cars interval(4s)")
        tdSql.waitedQuery("select * from strm", 2, 100)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, 2)




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
