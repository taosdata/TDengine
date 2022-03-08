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
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    '''
    def bug2222(self):
        tdSql.prepare()
        tdSql.execute("create table superreal(ts timestamp, addr binary(5), val float) tags (deviceNo binary(20))")
        tdSql.execute("create table real_001 using superreal tags('001')")
        tdSql.execute("create table tj_001 as select sum(val) from real_001 interval(1m)")

        t = datetime.datetime.now()
        for i in range(60):
            ts = t.strftime("%Y-%m-%d %H:%M")
            t += datetime.timedelta(minutes=1)
            sql = "insert into real_001 values('%s:0%d', '1', %d)" % (ts, 0, i)
            for j in range(4):
                sql += ",('%s:0%d', '%d', %d)" % (ts, j + 1, j + 1, i)
            tdSql.execute(sql)
            time.sleep(60 + random.random() * 60 - 30)
    '''

    def tbase300(self):
        tdLog.debug("begin tbase300")

        tdSql.prepare()
        tdSql.execute("create table mt(ts timestamp, c1 int, c2 int) tags(t1 int)")
        tdSql.execute("create table tb1 using mt tags(1)");
        tdSql.execute("create table tb2 using mt tags(2)");
        tdSql.execute("create table strm as select count(*), avg(c1), sum(c2), max(c1), min(c2),first(c1), last(c2) from mt interval(4s) sliding(2s)")
        #tdSql.execute("create table strm as select count(*), avg(c1), sum(c2), max(c1), min(c2), first(c1) from mt interval(4s) sliding(2s)")
        tdLog.sleep(10)
        tdSql.execute("insert into tb2 values(now, 1, 1)");
        tdSql.execute("insert into tb1 values(now, 1, 1)");
        tdLog.sleep(4)
        tdSql.query("select * from mt")
        tdSql.query("select * from strm")
        tdSql.execute("drop table tb1")

        tdSql.waitedQuery("select * from strm", 1, 100)
        if tdSql.queryRows < 1 or tdSql.queryRows > 2:
            tdLog.exit("rows should be 1 or 2")

        tdSql.execute("drop table tb2")
        tdSql.execute("drop table mt")
        tdSql.execute("drop table strm")

    def tbase304(self):
        tdLog.debug("begin tbase304")
        # we cannot reset query cache in server side, as a workaround,
        # set super table name to mt304, need to change back to mt later
        tdSql.execute("create table mt304 (ts timestamp, c1 int) tags(t1 int, t2 int)")
        tdSql.execute("create table tb1 using mt304 tags(1, 1)")
        tdSql.execute("create table tb2 using mt304 tags(1, -1)")
        time.sleep(0.1)
        tdSql.execute("create table strm as select count(*), avg(c1) from mt304 where t2 >= 0 interval(4s) sliding(2s)")
        tdSql.execute("insert into tb1 values (now,1)")
        tdSql.execute("insert into tb2 values (now,2)")

        tdSql.waitedQuery("select * from strm", 1, 100)
        if tdSql.queryRows < 1 or tdSql.queryRows > 2:
            tdLog.exit("rows should be 1 or 2")

        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1.000000000)
        tdSql.execute("alter table mt304 drop tag t2")
        tdSql.execute("insert into tb2 values (now,2)")
        tdSql.execute("insert into tb1 values (now,1)")
        tdSql.query("select * from strm")
        tdSql.execute("alter table mt304 add tag t2 int")
        tdLog.sleep(1)
        tdSql.query("select * from strm")

    def wildcardFilterOnTags(self):
        tdLog.debug("begin wildcardFilterOnTag")
        tdSql.prepare()
        tdSql.execute("create table stb (ts timestamp, c1 int, c2 binary(10)) tags(t1 binary(10))")
        tdSql.execute("create table tb1 using stb tags('a1')")
        tdSql.execute("create table tb2 using stb tags('b2')")
        tdSql.execute("create table tb3 using stb tags('a3')")
        tdSql.execute("create table strm as select count(*), avg(c1), first(c2) from stb where t1 like 'a%' interval(4s) sliding(2s)")
        tdSql.query("describe strm")
        tdSql.checkRows(4)

        tdLog.sleep(1)
        tdSql.execute("insert into tb1 values (now, 0, 'tb1')")
        tdLog.sleep(4)
        tdSql.execute("insert into tb2 values (now, 2, 'tb2')")
        tdLog.sleep(4)
        tdSql.execute("insert into tb3 values (now, 0, 'tb3')")

        tdSql.waitedQuery("select * from strm", 4, 60)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 0.000000000)
        if tdSql.getData(0, 3) == 'tb2':
            tdLog.exit("unexpected value of data03")
        if tdSql.getData(1, 3) == 'tb2':
            tdLog.exit("unexpected value of data13")
        if tdSql.getData(2, 3) == 'tb2':
            tdLog.exit("unexpected value of data23")
        if tdSql.getData(3, 3) == 'tb2':
            tdLog.exit("unexpected value of data33")

        tdLog.info("add table tb4 to see if stream still works correctly")
        # The vnode client needs to refresh metadata cache to allow strm calculate tb4's data.
        # But the current refreshing frequency is every 10 min
        # commented out the case below to save running time
        tdSql.execute("create table tb4 using stb tags('a4')")
        tdSql.execute("insert into tb4 values(now, 4, 'tb4')")
        tdSql.waitedQuery("select * from strm order by ts desc", 6, 60)
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, "tb4")

        tdLog.info("change tag values to see if stream still works correctly")
        tdSql.execute("alter table tb4 set tag t1='b4'")
        tdLog.sleep(3)
        tdSql.execute("insert into tb1 values (now, 1, 'tb1_a1')")
        tdLog.sleep(4)
        tdSql.execute("insert into tb4 values (now, -4, 'tb4_b4')")
        tdSql.waitedQuery("select * from strm order by ts desc", 8, 100)
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "tb1_a1")

    def datatypes(self):
        tdLog.debug("begin data types")
        tdSql.prepare()
        tdSql.execute("create table stb3 (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(15), c6 nchar(15), c7 bool) tags(t1 int, t2 binary(15))")
        tdSql.execute("create table tb0 using stb3 tags(0, 'tb0')")
        tdSql.execute("create table tb1 using stb3 tags(1, 'tb1')")
        tdSql.execute("create table tb2 using stb3 tags(2, 'tb2')")
        tdSql.execute("create table tb3 using stb3 tags(3, 'tb3')")
        tdSql.execute("create table tb4 using stb3 tags(4, 'tb4')")

        tdSql.execute("create table strm0 as select count(ts), count(c1), max(c2), min(c4), first(c5), last(c6) from stb3 where ts < now + 30s interval(4s) sliding(2s)")
        #tdSql.execute("create table strm0 as select count(ts), count(c1), max(c2), min(c4), first(c5) from stb where ts < now + 30s interval(4s) sliding(2s)")
        tdLog.sleep(1)
        tdSql.execute("insert into tb0 values (now, 0, 0, 0, 0, 'binary0', '涛思0', true) tb1 values (now, 1, 1, 1, 1, 'binary1', '涛思1', false) tb2 values (now, 2, 2, 2, 2, 'binary2', '涛思2', true) tb3 values (now, 3, 3, 3, 3, 'binary3', '涛思3', false) tb4 values (now, 4, 4, 4, 4, 'binary4', '涛思4', true) ")

        tdSql.waitedQuery("select * from strm0 order by ts desc", 2, 120)
        tdSql.checkRows(2)

        tdSql.execute("insert into tb0 values (now, 10, 10, 10, 10, 'binary0', '涛思0', true) tb1 values (now, 11, 11, 11, 11, 'binary1', '涛思1', false) tb2 values (now, 12, 12, 12, 12, 'binary2', '涛思2', true) tb3 values (now, 13, 13, 13, 13, 'binary3', '涛思3', false) tb4 values (now, 14, 14, 14, 14, 'binary4', '涛思4', true) ")
        tdSql.waitedQuery("select * from strm0 order by ts desc", 4, 120)
        tdSql.checkRows(4)

    def run(self):
        self.tbase300()
        self.tbase304()
        self.wildcardFilterOnTags()
        self.datatypes()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
