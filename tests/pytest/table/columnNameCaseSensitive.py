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
        tdSql.prepare()

        # column
        tdSql.execute("create table tb(ts timestamp, c1 int)")
        tdSql.execute("create table `TB`(ts timestamp, c1 int)")
        tdSql.error("alter table tb add column C1 int")
        tdSql.execute("alter table tb add column `C1` int")
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
        tdSql.execute("insert into tb2(ts, `C1`) values(now + 1s, 1)")
        tdSql.execute("insert into tb2(ts, c1, `C1`) values(now + 2s, 1, 2)")
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
        tdSql.execute("insert into `TB2`(ts, `C1`) values(now + 1s, 1)")
        tdSql.execute("insert into `TB2`(ts, c1, `C1`) values(now + 2s, 1, 2)")
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
        tdSql.execute("insert into tt2(ts, `C1`) using `STB2` tags(1) values(now + 1s, 1)")
        tdSql.execute("insert into tt2(ts, c1, `C1`) using `STB2` tags(1) values(now + 2s, 1, 2)")
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

        # cornor cases
        tdSql.execute("alter table `STB2` add column `数量` int")
        tdSql.execute("insert into tt3(ts, `数量`) using `STB2` tags(2) values(now + 3s, 1)")
        tdSql.query("select * from tt3")
        tdSql.checkRows(1)
        tdSql.query("select ts `TS` from tt3")
        tdSql.checkRows(1)
        tdSql.query("select ts as `TS` from tt3")
        tdSql.checkRows(1)
        tdSql.query("select ts as `时间戳` from tt3")
        tdSql.checkRows(1)
        tdSql.query("select ts `时间戳` from tt3")        
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())