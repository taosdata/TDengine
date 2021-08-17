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
            "create table stb1 (ts timestamp, c1 int) TAGS(t1 int)"
        )
        tdSql.execute("create table t0 using stb1 tags(1)")
        tdSql.execute("insert into t0 values (-865000000, 1)")
        tdSql.execute("insert into t0 values (-864000000, 2)")
        tdSql.execute("insert into t0 values (-863000000, 3)")
        tdSql.execute("insert into t0 values (-15230000, 4)")
        tdSql.execute("insert into t0 values (-15220000, 5)")
        tdSql.execute("insert into t0 values (-15210000, 6)")

        tdLog.printNoPrefix("==========step2:query")
        # bug1:when ts > -864000000, return 0 rows;
        # bug2:when ts = -15220000, return 0 rows.
        tdSql.query('select * from t0 where ts < -864000000')
        tdSql.checkRows(1)
        tdSql.query('select * from t0 where ts <= -864000000')
        tdSql.checkRows(2)
        tdSql.query('select * from t0 where ts = -864000000')
        tdSql.checkRows(1)
        tdSql.query('select * from t0 where ts > -864000000')
        tdSql.checkRows(4)
        tdSql.query('select * from t0 where ts >= -864000000')
        tdSql.checkRows(5)
        tdSql.query('select * from t0 where ts < -15220000')
        tdSql.checkRows(4)
        tdSql.query('select * from t0 where ts <= -15220000')
        tdSql.checkRows(5)
        tdSql.query('select * from t0 where ts = -15220000')
        tdSql.checkRows(1)
        tdSql.query('select * from t0 where ts > -15220000')
        tdSql.checkRows(1)
        tdSql.query('select * from t0 where ts >= -15220000')
        tdSql.checkRows(2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())