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
from functions.function_elapsed_case import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def genTime(self, no):
        h = int(no / 60)
        hs = "%d" %h
        if h < 10:
            hs = "0%d" %h

        m = int(no % 60)
        ms = "%d" %m
        if m < 10:
            ms = "0%d" %m

        return hs, ms

    def general(self):
        # normal table
        tdSql.execute("create database wxy_db minrows 10 maxrows 200")
        tdSql.execute("use wxy_db")
        tdSql.execute("create table t1(ts timestamp, i int, b bigint, f float, d double, bin binary(10), s smallint, t tinyint, bl bool, n nchar(10), ts1 timestamp)")
        for i in range(1, 1001):
            hs, ms = self.genTime(i)
            if i < 500:
                ret = tdSql.execute("insert into t1(ts, i, b) values (\"2021-11-22 %s:%s:00\", %d, 1)" % (hs, ms, i))
            else:
                ret = tdSql.execute("insert into t1(ts, i, b) values (\"2021-11-22 %s:%s:00\", %d, 0)" % (hs, ms, i))
        tdSql.query("select count(*) from t1")
        tdSql.checkEqual(int(tdSql.getData(0, 0)), 1000)

        # empty normal table
        tdSql.execute("create table t2(ts timestamp, i int, b bigint, f float, d double, bin binary(10), s smallint, t tinyint, bl bool, n nchar(10), ts1 timestamp)")

        tdSql.execute("create database wxy_db_ns precision \"ns\"")
        tdSql.execute("use wxy_db_ns")
        tdSql.execute("create table t1 (ts timestamp, f float)")
        tdSql.execute("insert into t1 values('2021-11-18 00:00:00.000000100', 1)"
                                           "('2021-11-18 00:00:00.000000200', 2)"
                                           "('2021-11-18 00:00:00.000000300', 3)"
                                           "('2021-11-18 00:00:00.000000500', 4)")

        # super table
        tdSql.execute("use wxy_db")
        tdSql.execute("create stable st1(ts timestamp, i int, b bigint, f float, d double, bin binary(10), s smallint, t tinyint, bl bool, n nchar(10), ts1 timestamp) tags(id int)")
        tdSql.execute("create table st1s1 using st1 tags(1)")
        tdSql.execute("create table st1s2 using st1 tags(2)")
        for i in range(1, 1001):
            hs, ms = self.genTime(i)
            if 0 == i % 2:
                ret = tdSql.execute("insert into st1s1(ts, i) values (\"2021-11-22 %s:%s:00\", %d)" % (hs, ms, i))
            else:
                ret = tdSql.execute("insert into st1s2(ts, i) values (\"2021-11-22 %s:%s:00\", %d)" % (hs, ms, i))
        tdSql.query("select count(*) from st1s1")
        tdSql.checkEqual(int(tdSql.getData(0, 0)), 500)
        tdSql.query("select count(*) from st1s2")
        tdSql.checkEqual(int(tdSql.getData(0, 0)), 500)
        # empty super table
        tdSql.execute("create stable st2(ts timestamp, i int, b bigint, f float, d double, bin binary(10), s smallint, t tinyint, bl bool, n nchar(10), ts1 timestamp) tags(id int)")
        tdSql.execute("create table st2s1 using st1 tags(1)")
        tdSql.execute("create table st2s2 using st1 tags(2)")

        tdSql.execute("create stable st3(ts timestamp, i int, b bigint, f float, d double, bin binary(10), s smallint, t tinyint, bl bool, n nchar(10), ts1 timestamp) tags(id int)")

    def run(self):
        tdSql.prepare()
        self.general()
        ElapsedCase().run()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
