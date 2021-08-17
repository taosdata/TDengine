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
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table and insert data")
        tdSql.execute(
            "create table stb1 (ts timestamp, c1 timestamp , c2 int) TAGS(t1 int )"
        )

        tdLog.printNoPrefix("==========step2:query data where timestamp data is null")
        tdSql.execute(
            "insert into t1 using stb1(t1) tags(1) (ts, c1, c2) values (now-1m, null, 1)"
        )
        tdSql.execute(
            "insert into t1 using stb1(t1) tags(1) (ts, c2) values (now-2m, 2)"
        )
        tdSql.query("select * from t1 where c1 is NULL")
        tdSql.checkRows(2)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
