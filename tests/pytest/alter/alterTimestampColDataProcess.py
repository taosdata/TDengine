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
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 36500")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table && insert data")
        # timestamp list:
        #   0 -> "1970-01-01 08:00:00" | -28800000 -> "1970-01-01 00:00:00" | -946800000000 -> "1940-01-01 00:00:00"
        #   -631180800000 -> "1950-01-01 00:00:00"
        ts1 = 0
        ts2 = -28800000
        ts3 = -946800000000
        ts4 = "1950-01-01 00:00:00"
        tdSql.execute(
            "create table stb2ts (ts timestamp, ts1 timestamp, ts2 timestamp, c1 int, ts3 timestamp) TAGS(t1 int)"
        )
        tdSql.execute("create table t2ts1 using stb2ts tags(1)")

        tdSql.execute(f"insert into t2ts1 values ({ts1}, {ts1}, {ts1}, 1, {ts1})")
        tdSql.execute(f"insert into t2ts1 values ({ts2}, {ts2}, {ts2}, 2, {ts2})")
        tdSql.execute(f"insert into t2ts1 values ({ts3}, {ts3}, {ts3}, 4, {ts3})")
        tdSql.execute(f"insert into t2ts1 values ('{ts4}', '{ts4}', '{ts4}', 3, '{ts4}')")

        tdLog.printNoPrefix("==========step2:check inserted data")
        tdSql.query("select * from stb2ts where ts1=0 and ts2='1970-01-01 08:00:00' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 4,'1970-01-01 08:00:00')

        tdSql.query("select * from stb2ts where ts1=-28800000 and ts2='1970-01-01 00:00:00' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '1970-01-01 00:00:00')

        tdSql.query("select * from stb2ts where ts1=-946800000000 and ts2='1940-01-01 00:00:00' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '1940-01-01 00:00:00')

        tdSql.query("select * from stb2ts where ts1=-631180800000 and ts2='1950-01-01 00:00:00' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, '1950-01-01 00:00:00')


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())