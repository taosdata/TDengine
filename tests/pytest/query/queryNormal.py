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
from util.dnodes import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")

        tdSql.execute(
            "create table stb1 (ts timestamp, c1 int, c2 float) tags(t1 int, t2 binary(10), t3 nchar(10))")
        tdSql.execute(
            "insert into tb1 using stb1 tags(1,'tb1', '表1') values ('2020-04-18 15:00:00.000', 1, 0.1), ('2020-04-18 15:00:01.000', 2, 0.1)")
        tdSql.execute(
            "insert into tb2 using stb1 tags(2,'tb2', '表2') values ('2020-04-18 15:00:02.000', 3, 2.1), ('2020-04-18 15:00:03.000', 4, 2.2)")

        tdSql.error("select * from tb 1")
        
        tdSql.query("select * from tb1 a, tb2 b where a.ts = b.ts")
        tdSql.checkRows(0)

        # join 3 tables -- bug exists
        tdSql.error("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_p.id, stb_p.dscrption, stb_p.pressure,stb_v.velocity from stb_p, stb_t, stb_v where stb_p.ts=stb_t.ts and stb_p.ts=stb_v.ts and stb_p.id = stb_t.id")

        tdSql.error("select * from stb1 whern c1 > 'test' limit 100")

        # query show stable
        tdSql.query("show stables")
        tdSql.checkRows(1)

        # query show tables
        tdSql.query("show tables")
        tdSql.checkRows(2)

        # query count
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4)

        # query first
        tdSql.query("select first(*) from stb1")
        tdSql.checkData(0, 1, 1)

        # query last
        tdSql.query("select last(*) from stb1")
        tdSql.checkData(0, 1, 4)

        # query last_row
        tdSql.query("select last_row(*) from stb1")
        tdSql.checkData(0, 1, 4)

        # query as
        tdSql.query("select t2 as number from stb1")
        tdSql.checkRows(2)

        # query first ... as
        tdSql.error("select first(*) as begin from stb1")        

        # query last ... as
        tdSql.error("select last(*) as end from stb1")

        # query last_row ... as
        tdSql.error("select last_row(*) as end from stb1")        

        # query group .. by
        tdSql.query("select sum(c1), t2 from stb1 group by t2")
        tdSql.checkRows(2)

        # query ... limit
        tdSql.query("select * from stb1 limit 2")
        tdSql.checkRows(2)

        # query ... limit offset
        tdSql.query("select * from stb1 limit 2 offset 3")
        tdSql.checkRows(1)

        # query ... alias for table
        tdSql.query("select t.ts from tb1 t")
        tdSql.checkRows(2)

        # query ... tbname
        tdSql.query("select tbname from stb1")
        tdSql.checkRows(2)

        # query ... tbname count  ---- bug
        tdSql.query("select count(tbname) from stb1")
        tdSql.checkData(0, 0, 2)

        # query ... select database ---- bug
        tdSql.query("SELECT database()")
        tdSql.checkRows(1)

        # query ... select client_version ---- bug
        tdSql.query("SELECT client_version()")
        tdSql.checkRows(1)

        # query ... select server_version ---- bug
        tdSql.query("SELECT server_version()")
        tdSql.checkRows(1)

        # query ... select server_status ---- bug
        tdSql.query("SELECT server_status()")
        tdSql.checkRows(1)

         # https://jira.taosdata.com:18080/browse/TD-3800
        tdSql.execute("create table m1(ts timestamp, k int) tags(a int)")
        tdSql.execute("create table tm0 using m1 tags(1)")
        tdSql.execute("create table tm1 using m1 tags(2)")
        tdSql.execute("insert into tm0 values('2020-3-1 1:1:1', 112)")
        tdSql.execute("insert into tm1 values('2020-1-1 1:1:1', 1)('2020-3-1 0:1:1', 421)")

        tdSql.query("select last(*) from m1 group by tbname")
        tdSql.checkData(0, 0, "2020-03-01 01:01:01")
        tdSql.checkData(0, 1, 112)
        tdSql.checkData(0, 2, "tm0")
        tdSql.checkData(1, 0, "2020-03-01 00:01:01")
        tdSql.checkData(1, 1, 421)
        tdSql.checkData(1, 2, "tm1")
        
        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("select last(*) from m1 group by tbname")
        tdSql.checkData(0, 0, "2020-03-01 01:01:01")
        tdSql.checkData(0, 1, 112)
        tdSql.checkData(0, 2, "tm0")
        tdSql.checkData(1, 0, "2020-03-01 00:01:01")
        tdSql.checkData(1, 1, 421)
        tdSql.checkData(1, 2, "tm1")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
