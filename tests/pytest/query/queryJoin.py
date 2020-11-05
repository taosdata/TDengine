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
        tdSql.prepare()

        print("======= Step 1 prepare data=========")
        tdSql.execute(
            "create table stb1 (ts timestamp, c1 int, c2 float) tags(t1 int, t2 binary(10), t3 nchar(10))")
        tdSql.execute(
            '''insert into tb1 using stb1 tags(1,'tb1', '表1') values ('2020-04-18 15:00:00.000', 1, 0.1)
            ('2020-04-18 15:00:01.000', 1,0.1) ('2020-04-18 15:00:03.000', 3, 0.3) ('2020-04-18 15:00:04.000', 4,0.4)''')
        tdSql.execute(
            '''insert into tb2 using stb1 tags(2,'tb2', '表2') values ('2020-04-18 15:00:00.000', 21, 2.1)
            ('2020-04-18 15:00:01.000', 22,2.2) ('2020-04-18 15:00:02.000', 22, 2.1) ('2020-04-18 15:00:03.000', 23,2.2)''')

        tdSql.execute(
            "create table stb_t (ts timestamp, temperature int, humidity float) tags(id int, name binary(10), dscrption nchar(10))")
        tdSql.execute(
            '''insert into tb_t1 using stb_t tags(1,'tb_t1', '温度表1') values ('2020-04-18 15:00:00.000', 25, 0.5)
            ('2020-04-18 15:00:01.000', 25, 0.5) ('2020-04-18 15:00:02.000', 26, 0.7) ('2020-04-18 15:00:03.000', 27, 0.8)''')
        tdSql.execute(
            '''insert into tb_t2 using stb_t tags(2,'tb_t2', '温度表2') values ('2020-04-18 15:00:00.000', 33, 0.9)
            ('2020-04-18 15:00:01.000', 35, 1.1) ('2020-04-18 15:00:03.000', 36, 1.3) ('2020-04-18 15:00:04.000', 37, 1.4)''')

        tdSql.execute(
            "create table stb_p (ts timestamp, pressure float) tags(id int, name binary(10), dscrption nchar(10), location binary(20))")
        tdSql.execute(
            '''insert into tb_p1 using stb_p tags(1,'tb_p1', '压力计1', 'beijing') values ('2020-04-18 15:00:00.000', 76.6)
            ('2020-04-18 15:00:01.000', 76.5) ('2020-04-18 15:00:01.500', 77.1) ('2020-04-18 15:00:02.000', 75.3)
            ('2020-04-18 15:00:03.000', 75.1) ('2020-04-18 15:00:04.500', 77.3)''')
        tdSql.execute(
            '''insert into tb_p2 using stb_p tags(2,'tb_p2', '压力计2', 'shenzhen') values ('2020-04-18 14:59:59.000', 74.6)
            ('2020-04-18 15:00:01.000', 74.5) ('2020-04-18 15:00:01.500', 73.6) ('2020-04-18 15:00:02.000', 74.5)
            ('2020-04-18 15:00:02.500', 73.9) ('2020-04-18 15:00:03.000', 73.5)''')

        tdSql.execute(
            "create table stb_v (ts timestamp, velocity float) tags(id int, name binary(10), dscrption nchar(10), location binary(20))")
        tdSql.execute(
            '''insert into tb_v1 using stb_v tags(1,'tb_v1', '速度计1', 'beijing ') values ('2020-04-18 15:00:00.000', 176.6)
            ('2020-04-18 15:00:01.000', 176.5)''')
        tdSql.execute(
            '''insert into tb_v2 using stb_v tags(2,'tb_v2', '速度计2', 'shenzhen') values ('2020-04-18 15:00:00.000', 171.6)
            ('2020-04-18 15:00:01.000', 171.5)''')

        # explicit join should not work
        tdSql.error("select * from stb_p join stb_t on (stb_p.id = stb_t.id)")
        tdSql.error("select * from tb1 join tb2 on (tb1.ts=tb2.ts)")
        tdSql.error(
            "select * from stb_p join stb_t on (stb_p.ts=stb_t.ts and stb_p.id = stb_t.id)")

        # alias should not work
        tdSql.error("select * from stb_p p join stb_t t on (p.id = t.id)")

        # join queries
        tdSql.query(
            "select * from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")
        tdSql.checkRows(6)

        tdSql.error(
            "select ts, pressure, temperature, id, dscrption from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")

        tdSql.query("select stb_p.ts, pressure, stb_t.temperature, stb_p.id, stb_p.dscrption from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")
        tdSql.checkRows(6)

        tdSql.query("select stb_t.ts, stb_p.pressure, stb_t.temperature,stb_p.id,stb_p.dscrption from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")
        tdSql.checkRows(6)

        tdSql.error(
            "select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id group by name")
        tdSql.error(
            "select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id group by stb_t.name")
        tdSql.error(
            "select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id group by stb_t.id")
        tdSql.error(
            "select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.name;")        

        tdSql.execute("alter table stb_t add tag pid int")
        tdSql.execute("alter table tb_t1 set tag pid=2")
        tdSql.execute("alter table tb_t2 set tag pid=1")

        tdSql.query(
            "select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.location = stb_t.name")
        tdSql.checkRows(0)

        tdSql.query("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.pid")
        tdSql.checkRows(3)

        tdSql.query("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")
        tdSql.checkRows(6)

        tdSql.query("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.id, stb_p.dscrption, stb_p.pressure from stb_p, stb_t where stb_p.ts=stb_t.ts and stb_p.id = stb_t.id")
        tdSql.checkRows(6)

        tdSql.error("select stb_t.ts, stb_t.dscrption, stb_t.temperature, stb_t.pid, stb_p.id, stb_p.dscrption, stb_p.pressure,stb_v.velocity from stb_p, stb_t, stb_v where stb_p.ts=stb_t.ts and stb_p.ts=stb_v.ts and stb_p.id = stb_t.id")

        # test case for https://jira.taosdata.com:18080/browse/TD-1250
        
        tdSql.execute("create table meters1(ts timestamp, voltage int) tags(tag1 binary(20), tag2 nchar(20))")
        tdSql.execute("create table t1 using meters1 tags('beijing', 'chaoyang')")
        tdSql.execute("create table t2 using meters1 tags('shanghai', 'xuhui')") 
        tdSql.execute("insert into t1 values(1538548685000, 1) (1538548685001, 2) (1538548685002, 3)")
        tdSql.execute("insert into t1 values(1538548685004, 4) (1538548685004, 5) (1538548685005, 6)")

        tdSql.execute("create table meters2(ts timestamp, voltage int) tags(tag1 binary(20), tag2 nchar(20))")
        tdSql.execute("create table t3 using meters2 tags('beijing', 'chaoyang')")
        tdSql.execute("create table t4 using meters2 tags('shenzhen', 'nanshan')")        
        tdSql.execute("insert into t3 values(1538548685000, 7) (1538548685001, 8) (1538548685002, 9)")
        tdSql.execute("insert into t4 values(1538548685000, 10) (1538548685001, 11) (1538548685002, 12)")

        tdSql.execute("create table meters3(ts timestamp, voltage int) tags(tag1 binary(20), tag2 nchar(20))")
        
        tdSql.query("select * from meters1, meters2 where meters1.ts = meters2.ts and meters1.tag1 = meters2.tag1")
        tdSql.checkRows(3)

        tdSql.query("select * from meters1, meters2 where meters1.ts = meters2.ts and meters1.tag2 = meters2.tag2")
        tdSql.checkRows(3)

        tdSql.query("select * from meters1, meters3 where meters1.ts = meters3.ts and meters1.tag1 = meters3.tag1")
        tdSql.checkRows(0)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
