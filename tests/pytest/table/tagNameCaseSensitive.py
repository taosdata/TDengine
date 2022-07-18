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

from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self._conn = conn

    def run(self):
        tdSql.prepare()

        # tag
        tdSql.error("create table `STB3`(ts timesatmp, c1 int) tags(t1 int, T1 int)")
        tdSql.execute("create table `STB3`(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("alter table `STB3` add tag `T1` int")
        tdSql.execute("create table `STB4`(ts timestamp, c1 int) tags(t1 int, `T1` int)")
        tdSql.execute("create table tt3 using `STB3`(t1) tags(1)")
        tdSql.execute("create table tt4 using `STB3`(`T1`) tags(1)")
        tdSql.query("select t1, `T1` from `STB3`")
        tdSql.checkRows(2)

        tdSql.execute("alter table `STB3` drop tag `T1`")
        tdSql.query("describe `STB3`")
        tdSql.checkRows(3)

        # cornor case
        tdSql.execute("create table `STB5`(ts timestamp, c1 int) tags(t1 int, `标签` int)")
        tdSql.execute("insert into `测试` using `STB5` tags(1, 1) values(now, 1)")
        tdSql.query("select * from `测试`")
        tdSql.checkRows(1)

        tdSql.query("select `标签` t from `测试`")
        tdSql.checkRows(1)

        tdSql.execute("alter table `STB5` add tag `标签2` double")
        tdSql.query("describe `STB5`")
        tdSql.checkRows(5)

        tdSql.execute("alter table `STB5` add tag `1` int")
        tdSql.execute("create table t2 using `STB5`(`1`) tags(2)")
        tdSql.error("alter table t2 set tag 1=2222")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())