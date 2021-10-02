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
from util.dnodes import *
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537100000000

    def run(self):
        tdSql.prepare()
        tdSql.execute("create table ap1 (ts timestamp, pav float)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:54.119', 2.90799)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:54.317', 3.07399)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:54.517', 0.58117)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:54.717', 0.16150)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:54.918', 1.47885)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:56.569', 1.76472)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:57.381', 2.13722)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:57.574', 4.10256)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:57.776', 3.55345)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:57.976', 1.46624)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:58.187', 0.17943)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:58.372', 2.04101)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:58.573', 3.20924)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:58.768', 1.71807)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:58.964', 4.60900)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:59.155', 4.33907)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:59.359', 0.76940)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:59.553', 0.06458)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:59.742', 4.59857)")
        tdSql.execute("insert into ap1 values ('2021-07-25 02:19:59.938', 1.55081)")

        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (PREV)")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (NEXT)")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (LINEAR)")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (LINEAR)")
        tdSql.checkRows(6)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (NEXT)")
        tdSql.checkRows(6)
        tdSql.checkData(0,1,2.90799)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (PREV)")
        tdSql.checkRows(7)
        tdSql.checkData(1,1,1.47885)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (LINEAR)")
        tdSql.checkRows(7)

        # check desc order
        tdSql.error("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (PREV) order by ts desc")
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (NEXT) order by ts desc")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (LINEAR) order by ts desc")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (LINEAR) order by ts desc")
        tdSql.checkRows(6)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (NEXT) order by ts desc")
        tdSql.checkRows(6)
        tdSql.checkData(0,1,4.60900)
        tdSql.error("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (PREV) order by ts desc")
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (LINEAR) order by ts desc")
        tdSql.checkRows(7)

        # check exception
        tdSql.error("select interp(*) from ap1")
        tdSql.error("select interp(*) from ap1 FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 ts >= '2021-07-25 02:19:54' FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 ts <= '2021-07-25 02:19:54' FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 where ts >'2021-07-25 02:19:59.938' and ts < now every(1s) fill(next)")

        # test case for https://jira.taosdata.com:18080/browse/TS-241
        tdSql.execute("create database test minrows 10")
        tdSql.execute("use test")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(id int)")
        tdSql.execute("create table t1 using st tags(1)")

        for i in range(10):            
            for j in range(10):
                tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i * 3600000 + j, j))
            tdSql.query("select interp(c1) from st where ts >= '2018-09-16 20:00:00.000' and ts <= '2018-09-17 06:00:00.000' every(1h) fill(linear)")
            if i==0:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(11)
            
            tdDnodes.stop(1)
            tdDnodes.start(1)
            tdSql.query("select interp(c1) from st where ts >= '2018-09-16 20:00:00.000' and ts <= '2018-09-17 06:00:00.000' every(1h) fill(linear)")            
            if i==0:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(11)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
