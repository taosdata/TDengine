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

    def run(self):

        # table/stable
        tdSql.execute("create database test")
        tdSql.execute("create database `Test`")
        tdSql.execute("use test")
        tdSql.execute("create table tb(ts timestamp, c1 int)")

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("show create table tb")
        tdSql.checkRows(1)

        tdSql.error("create table Tb(ts timestamp, c1 int)") 
        tdSql.execute("create table `TB`(ts timestamp, c1 int)")

        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.query("show create table `TB`")
        tdSql.checkRows(1)

        tdSql.query("describe tb")
        tdSql.checkRows(2)

        tdSql.query("describe `TB`")
        tdSql.checkRows(2)

        tdSql.execute("insert into tb values(now, 1)")
        tdSql.error("select * from `Test`.tb")
        tdSql.query("select * from test.tb")
        tdSql.checkRows(1)

        tdSql.execute("insert into `TB` values(now, 1)")
        tdSql.error("select * from `Test`.`TB`")
        tdSql.query("select * from test.`TB`")
        tdSql.checkRows(1)         

        tdSql.execute("create stable stb(ts timestamp, c1 int) tags(t1 int)")
        tdSql.query("show stables")
        tdSql.checkRows(1)

        tdSql.error("create stable STb(ts timestamp, c1 int) tags(t1 int)")
        tdSql.error("create stable `stb`(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create stable `STB`(ts timestamp, c1 int) tags(t1 int)")
        tdSql.query("show stables")
        tdSql.checkRows(2)

        tdSql.query("describe stb")
        tdSql.checkRows(3)

        tdSql.query("describe `STB`")
        tdSql.checkRows(3)

        tdSql.execute("insert into t1 using stb tags(1) values(now, 1)")
        tdSql.query("select * from stb")
        tdSql.checkRows(1)

        tdSql.execute("insert into t2 using `STB` tags(1) values(now, 1)")
        tdSql.query("select * from `STB`")
        tdSql.checkRows(1)

        tdSql.execute("insert into `T2` using `STB` tags(1) values(now + 1s, 1)")
        tdSql.query("select * from `STB`")
        tdSql.checkRows(2)

        tdSql.query("select tbname from `STB`")
        tdSql.checkRows(2)

        tdSql.execute("alter table stb add column c2 int")
        tdSql.execute("alter table stb add tag t2 int")
        tdSql.execute("alter table `STB` add column c2 int")
        tdSql.execute("alter table `STB` add tag t2 int")
        tdSql.execute("alter table `TB` add column c2 int")

        # corner cases
        tdSql.execute("create table `超级表`(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table `子表一` using `超级表` tags(1)")
        tdSql.execute("insert into `子表二` using `超级表` tags(1) values(now, 1)")

        tdSql.query("select * from `超级表`")
        tdSql.checkRows(1)
        tdSql.query("select * from `子表二`")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(7)

        tdSql.execute("create table `普通表` (ts timestamp, c1 int)")
        tdSql.execute("insert into `普通表` values(now, 2)")
        tdSql.query("select * from `普通表`")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(8)        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())