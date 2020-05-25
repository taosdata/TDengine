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
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")

        tdSql.execute("create table stb1 (ts timestamp, c1 int, c2 float) tags(t1 int, t2 binary(10), t3 nchar(10))")
        tdSql.execute("insert into tb1 using stb1 tags(1,'tb1', '表1') values ('2020-04-18 15:00:00.000', 1, 0.1), ('2020-04-18 15:00:01.000', 2, 0.1)")
        tdSql.execute("insert into tb2 using stb1 tags(2,'tb2', '表2') values ('2020-04-18 15:00:02.000', 3, 2.1), ('2020-04-18 15:00:03.000', 4, 2.2)")        

        # join 2 tables -- bug exists
        # tdSql.query("select * from tb1 a, tb2 b where a.ts = b.ts")
        # tdSql.checkRows(1)

        # join 3 tables -- bug exists
        # tdSql.query("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_p.id, stb_p.dscrption, stb_p.pressure,stb_v.velocity from stb_p, stb_t, stb_v where stb_p.ts=stb_t.ts and stb_p.ts=stb_v.ts and stb_p.id = stb_t.id")
        
        # query count
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4)

        # query first 
        tdSql.query("select first(*) from stb1")
        tdSql.checkData(0, 1, 1)
        
        # query last 
        tdSql.query("select last(*) from stb1")
        tdSql.checkData(0, 1, 4)

        # query as
        tdSql.query("select t2 as number from stb1")
        tdSql.checkRows(2)

        # query first ... as
        tdSql.query("select first(*) as begin from stb1")
        tdSql.checkData(0, 1, 1)
        
        # query last ... as
        tdSql.query("select last(*) as end from stb1")
        tdSql.checkData(0, 1, 4)

        # query group .. by
        tdSql.query("select sum(c1), t2 from stb1 group by t2")
        tdSql.checkRows(2)

        # query ... limit
        tdSql.query("select * from stb1 limit 2")
        tdSql.checkRows(2)

        # query ... limit offset
        tdSql.query("select * from stb1 limit 2 offset 3")
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
