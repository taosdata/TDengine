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

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            "create table stb1 (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )"
        )

        tdLog.printNoPrefix("==========step2:insert data with new syntax")
        tdSql.execute(
            "insert into t1 using stb1(t11, t12) tags(11, 12) (ts, c11, c12) values (now, 10, 20)"
        )

        # case for tag-value
        tdSql.execute(
            "insert into t2 using stb1(t11) tags(21) (ts, c11, c12) values (now-1m, 11, 21)"
        )
        tdSql.execute(
            "insert into t3 using stb1 tags(31, 32) (ts, c11, c12) values (now-2m, 12, 22)"
        )
        tdSql.error(
            "insert into t4 using stb1(t11, t12)  (ts, c11, c12) values (now-3m, 13, 23)"
        )
        tdSql.error(
            "insert into t5 using stb1(t11, t12) tags() (ts, c11, c12) values (now-4m, 14, 24)"
        )
        tdSql.error(
            "insert into t6 using stb1(t11, t12) tags(41) (ts, c11, c12) values (now-5m, 15, 25)"
        )
        tdSql.error(
            "insert into t7 using stb1(t12) tags(51, 52) (ts, c11, c12) values (now-6m, 16, 26)"
        )
        tdSql.execute(
            "insert into t8 using stb1(t11, t12) tags('61', 62) (ts, c11, c12) values (now-7m, 17, 27)"
        )


        # case for col-value
        tdSql.execute(
            "insert into t9 using stb1(t11, t12) tags(71, 72) values (now-8m, 18, 28)"
        )
        tdSql.error(
            "insert into t10 using stb1(t11, t12) tags(81, 82) (ts, c11, c12) values ()"
        )
        tdSql.error(
            "insert into t11 using stb1(t11, t12) tags(91, 92) (ts, c11, c12) "
        )
        tdSql.error(
            "insert into t12 using stb1(t11, t12) tags(101, 102) values (now-9m, 19)"
        )
        tdSql.error(
            "insert into t13 using stb1(t11, t12) tags(111, 112) (ts, c11) values (now-10m, 110, 210)"
        )
        tdSql.error(
            "insert into t14 using stb1(t11, t12) tags(121, 122) (ts, c11, c12) values (now-11m, 111)"
        )
        tdSql.execute(
            "insert into t15 using stb1(t11, t12) tags(131, 132) (ts, c11, c12) values (now-12m, NULL , 212)"
        )
        tdSql.execute(
            "insert into t16 using stb1(t11, t12) tags(141, 142) (ts, c11, c12) values (now-13m, 'NULL', 213)"
        )
        tdSql.error(
            "insert into t17 using stb1(t11, t12) tags(151, 152) (ts, c11, c12) values (now-14m, Nan, 214)"
        )
        tdSql.error(
            "insert into t18 using stb1(t11, t12) tags(161, 162) (ts, c11, c12) values (now-15m, 'NaN', 215)"
        )
        tdSql.execute(
            "insert into t19 using stb1(t11, t12) tags(171, 172) (ts, c11) values (now-16m, 216)"
        )
        tdSql.error(
            "insert into t20 using stb1(t11, t12) tags(181, 182) (c11, c12) values (117, 217)"
        )

        # multi-col_value
        tdSql.execute(
            "insert into t21 using stb1(t11, t12) tags(191, 192) (ts, c11, c12) values (now-17m, 118, 218)(now-18m, 119, 219)"
        )
        tdSql.execute(
            "insert into t22 using stb1(t11, t12) tags(201, 202) values (now-19m, 120, 220)(now-19m, 121, 221)"
        )
        tdSql.error(
            "insert into t23 using stb1(t11, t12) tags(211, 212) values (now-20m, 122, 222) (ts, c11, c12) values (now-21m, 123, 223)"
        )
        tdSql.error(
            "insert into t24 using stb1(t11, t12) tags(221, 222) (ts, c11, c12) values (now-22m, 124, 224) (ts, c11, c12) values (now-23m, 125, 225)"
        )
        tdSql.execute(
            "insert into t25 (ts, c11, c12) using stb1(t11, t12) tags(231, 232) values (now-24m, 126, 226)(now-25m, 127, 227)"
        )
        tdSql.error(
            "insert into t26 (ts, c11, c12) values (now-24m, 128, 228)(now-25m, 129, 229) using stb1(t11, t12) tags(241, 242) "
        )
        tdSql.error(
            "insert into t27 (ts, c11, c12) values (now-24m, 130, 230) using stb1(t11, t12) tags(251, 252) "
        )

        tdSql.query("show tables")
        tdSql.checkRows(21)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())