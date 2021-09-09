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
import string
import random
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length)) 
        return result_str

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
        tdSql.checkRows(6)

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

        tdSql.execute("create table join_mt0(ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) tags(t1 int, t2 binary(12))")
        tdSql.execute("create table join_mt1(ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) tags(t1 int, t2 binary(12), t3 int)")

        ts = 1538548685000
        for i in range(3):
            tdSql.execute("create table join_tb%d using join_mt0 tags(%d, 'abc')" % (i, i)) 
            sql = "insert into join_tb%d values" % i
            for j in range(500):
                val = j % 100
                sql += "(%d, %d, %f, %d, %d, %d, %f, %d, 'binary%d', 'nchar%d')" % (ts + j, val, val * 1.0, val, val, val, val * 1.0, val % 2,  val, val)
            tdSql.execute(sql)
            sql = "insert into join_tb%d values" % i
            for j in range(500, 1000):
                val = j % 100
                sql += "(%d, %d, %f, %d, %d, %d, %f, %d, 'binary%d', 'nchar%d')" % (ts + 500 + j, val, val * 1.0, val, val, val, val * 1.0, val % 2,  val, val)
            tdSql.execute(sql)
        
        for i in range(3):
            tdSql.execute("create table join_1_tb%d using join_mt1 tags(%d, 'abc%d', %d)" % (i, i, i, i))            
            sql = "insert into join_1_tb%d values" % i
            for j in range(500):
                val = j % 100
                sql += "(%d, %d, %f, %d, %d, %d, %f, %d, 'binary%d', 'nchar%d')" % (ts + j, val, val * 1.0, val, val, val, val * 1.0, val % 2,  val, val)
            tdSql.execute(sql)
            sql = "insert into join_1_tb%d values" % i
            for j in range(500, 1000):
                val = j % 100
                sql += "(%d, %d, %f, %d, %d, %d, %f, %d, 'binary%d', 'nchar%d')" % (ts + 500 + j, val, val * 1.0, val, val, val, val * 1.0, val % 2,  val, val)
            tdSql.execute(sql)

        tdSql.error("select count(join_mt0.c1), sum(join_mt1.c2), first(join_mt0.c5), last(join_mt1.c7) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts interval(10a) group by join_mt0.t1 order by join_mt0.ts desc")
        tdSql.error("select count(join_mt0.c1), first(join_mt0.c1)-first(join_mt1.c1), first(join_mt1.c9) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts")
        tdSql.error("select count(join_mt0.c1), first(join_mt0.c1), first(join_mt1.c9) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts interval(10a) group by join_mt0.t1, join_mt0.t2 order by join_mt0.t1 desc slimit 3")
        tdSql.error("select count(join_mt0.c1), first(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts interval(10a) group by join_mt0.t1, join_mt0.t2, join_mt1.t1 order by join_mt0.ts desc, join_mt1.ts asc limit 10;")
        tdSql.error("select join_mt1.c1,join_mt0.c1 from join_mt1,join_mt0 where join_mt1.ts = join_mt0.ts and join_mt1.t1 = join_mt0.t1 order by t")
        #TD-4458 join on database which using precision us 
        tdSql.execute("create database test_join_us precision 'us'")
        tdSql.execute("use test_join_us")
        ts = 1538548685000000
        for i in range(2):
            tdSql.execute("create table t%d (ts timestamp, i int)"%i)
            tdSql.execute("insert into t%d values(%d,11)(%d,12)"%(i,ts,ts+1))
        tdSql.query("select t1.ts from t0,t1 where t0.ts = t1.ts")
        tdSql.checkData(0,0,'2018-10-03 14:38:05.000000')

        #TD-6425 join result more than 1MB
        tdSql.execute("create database test_join")
        tdSql.execute("use test_join")

        ts = 1538548685000
        tdSql.execute("create table stb(ts timestamp, c1 nchar(200)) tags(id int, loc binary(20))")
        for i in range(2):
            tdSql.execute("create table tb%d using stb tags(1, 'city%d')" % (i, i))
            for j in range(1000):
                tdSql.execute("insert into tb%d values(%d, '%s')" %  (i, ts + j, self.get_random_string(200)))

        tdSql.query("select tb0.c1, tb1.c1 from tb0, tb1 where tb0.ts = tb1.ts")
        tdSql.checkRows(1000)            

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
