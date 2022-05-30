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
import taos
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):        

        # database name
        tdSql.execute("create database db")
        tdSql.query("show databases")
        tdSql.checkRows(1)

        tdSql.error("create database Db")
        tdSql.error("create database `db`")
        tdSql.execute("create database `Db`")
        tdSql.checkRows(2)

        tdSql.execute("alter database db days 20")
        tdSql.execute("alter database `Db` days 20")

        tdSql.execute("use db")
        tdSql.query("select database()")
        tdSql.checkData(0, 0, 'db');

        tdSql.execute("use `Db`")
        tdSql.query("select database()")
        tdSql.checkData(0, 0, 'Db');

        tdSql.execute("drop database db")
        tdSql.execute("drop database `Db`")

        # table/stable
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table tb(ts timestamp, c1 int)")
        tdSql.query("show tables")
        tdSql.checkRows(1)

        tdSql.error("create table Tb(ts timestamp, c1 int)") 
        tdSql.execute("create table `TB`(ts timestamp, c1 int)")
        tdSql.query("show tables")
        tdSql.checkRows(2)

        tdSql.query("describe tb")
        tdSql.checkRows(2)

        tdSql.query("describe `TB`")
        tdSql.checkRows(2)

        tdSql.execute("insert into tb values(now, 1)")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)

        tdSql.execute("insert into `TB` values(now, 1)")
        tdSql.query("select * from `TB`")
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

        tdSql.execute("alter table stb add column c2 int")
        tdSql.execute("alter table stb add tag t2 int")
        tdSql.execute("alter table `TB` add column c2 int")
        tdSql.execute("alter table `TB` add tag t2 int")        
        
        # column
        tdSql.error("alter table tb add column C1 int")
        tdSql.excute("alter table tb add column `C1` int")        
        tdSql.error("alter table `TB` add column C1 int")
        tdSql.execute("alter table `TB` add column `C1` int")

        tdSql.error("create table tb2(ts timestamp, c1 int, C1 int)")
        tdSql.execute("create table tb2(ts timestamp, c1 int, `C1` int)")
        tdSql.query("describe tb2")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'ts')
        tdSql.checkData(1, 0, 'c1')
        tdSql.checkData(2, 0, 'C1')        

        tdSql.execute("insert into tb2(ts, c1) values(now, 1)")
        tdSql.execute("insert into tb2(ts, `C1`) values(now, 1)")
        tdSql.execute("insert into tb2(ts, c1, `C1`) values(now, 1, 2)")
        tdSql.query("select * from tb2")
        tdSql.checkRows(3)

        tdSql.query("select * from tb2 where c1 = 1")        
        tdSql.checkRows(2)

        tdSql.query("select * from tb2 where `C1` = 1")        
        tdSql.checkRows(1)

        tdSql.query("select c1 `C1` from tb2 where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select c1 as `C1` from tb2 where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` a from tb2 where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` as a from tb2 where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.execute("alter table tb2 drop column c1")
        tdSql.query("describe tb2")
        tdSql.checkRows(2)

        tdSql.error("create table `TB2`(ts timestamp, c1 int, C1 int)")
        tdSql.execute("create table `TB2`(ts timestamp, c1 int, `C1` int)")
        tdSql.query("describe `TB2`")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'ts')
        tdSql.checkData(1, 0, 'c1')
        tdSql.checkData(2, 0, 'C1')
        
        tdSql.execute("insert into `TB2`(ts, c1) values(now, 1)")
        tdSql.execute("insert into `TB2`(ts, `C1`) values(now, 1)")
        tdSql.execute("insert into `TB2`(ts, c1, `C1`) values(now, 1, 2)")
        tdSql.query("select * from `TB2`")
        tdSql.checkRows(3)

        tdSql.query("select * from `TB2` where c1 = 1")        
        tdSql.checkRows(2)

        tdSql.query("select * from `TB2` where `C1` = 1")        
        tdSql.checkRows(1)

        tdSql.query("select c1 `C1` from `TB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select c1 as `C1` from `TB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` a from `TB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` as a from `TB2` where `C1` = 1")
        tdSql.checkRows(1)
        
        tdSql.execute("alter table `TB2` drop column `C1`")
        tdSql.query("describe tb2")
        tdSql.checkRows(2)

        tdSql.error("create table `STB2`(ts timestamp, c1 int, C1 int) tags (t1 int)")
        tdSql.execute("create table `STB2`(ts timestamp, c1 int, `C1` int) tags (t1 int)")
        tdSql.query("describe `STB2`")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'ts')
        tdSql.checkData(1, 0, 'c1')
        tdSql.checkData(2, 0, 'C1')
        tdSql.checkData(3, 0, 't1')
        
        tdSql.execute("insert into tt2(ts, c1) using `STB2` tags(1) values(now, 1)")
        tdSql.execute("insert into tt2(ts, `C1`) using `STB2` tags(1) values(now, 1)")
        tdSql.execute("insert into tt2(ts, c1, `C1`) using `STB2` tags(1) values(now, 1, 2)")
        tdSql.query("select * from `STB2`")
        tdSql.checkRows(3)

        tdSql.query("select * from `STB2` where c1 = 1")        
        tdSql.checkRows(2)

        tdSql.query("select * from `STB2` where `C1` = 1")        
        tdSql.checkRows(1)

        tdSql.query("select c1 `C1` from `STB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select c1 as `C1` from `STB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` a from `STB2` where `C1` = 1")
        tdSql.checkRows(1)

        tdSql.query("select `C1` as a from `STB2` where `C1` = 1")
        tdSql.checkRows(1)
        
        tdSql.execute("alter table `STB2` drop column `C1`")
        tdSql.query("describe tb2")
        tdSql.checkRows(2)
        
        # tag
        tdSql.error("create table `STB3`(ts timesatmp, c1 int) tags(t1 int, T1 int)")
        tdSql.execute("create table `STB3`(ts timesatmp, c1 int) tags(t1 int)")
        tdSql.execute("alter table `STB3` add tag `T1` int")
        tdSql.execute("create table `STB4`(ts timesatmp, c1 int) tags(t1 int, `T1` int)")
        tdSql.execute("create table tt3 using `STB3`(t1) tags(1)")
        tdSql.execute("create table tt4 using `STB3`(`T1`) tags(1)")
        tdSql.query("select t1, `T1` from `STB3`")
        tdSql.checkRows(2)

        tdSql.execute("alter table `STB3` drop tag `T1`")
        tdSql.query("describe `STB3`")
        tdSql.checkRows(3)

        # schemaless
        tdSql.execute("create database line_insert")
        tdSql.execute("use line_insert")
        lines = [   "st,t1=1 c1=1,c2=\"Test\" 1626006833639000000",
                    "ST1,T2=1,T1=1,t3=2 c1=1,c2=1 1626006833639000001",
                    "st2,T3=1,T1=1,t3=2 C1=1,c2=1 1626006833639000002",
                    "st3,t1=1,T1=1,t3=2 c1=1,`C1`=2 1626006833639000003",
                    "ST4,t1=1,`T1`=1,t3=2 c1=1,C1=2 1626006833639000004"
                ]

        code = self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.execute("show tables")
        tdSql.checkRows(5)        
                        
        # corner cases
        tdSql.execute("create database `C1[~!@#$%^&*()+-]`")
        tdSql.execute("create database `测试`")
        tdSql.execute("use `测试`")
        tdSql.execute("create table st3(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("insert into tb3 using st3 tags(1) values(now, 1)")
        tdSql.query("select c1 as `C1` from st3")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select c1 as `测试` from st3")
        tdSql.checkData(0, 0, 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
