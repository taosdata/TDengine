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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists stb (ts timestamp, col1 int, col2 int, col3 int) tags(loc nchar(20), id int)")
                
        currTs = self.ts

        for i in range(100):
            sql = "create table tb%d using stb tags('city%d', 1)" % (i, i)
            tdSql.execute(sql)

            sql = "insert into tb%d values" % i            
            for j in range(5):
                val = 1 + j
                sql += "(%d, %d, %d, %d)" % (currTs, val, val, val)
                currTs += 1000000
            tdSql.execute(sql)    

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h)")
        tdSql.checkRows(139)        
        tdSql.checkData(0, 1, -1.5)
        tdSql.checkData(138, 1, -1.0)

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(none)")
        tdSql.checkRows(139)
        tdSql.checkData(0, 1, -1.5)
        tdSql.checkData(138, 1, -1.0)

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(value, 2.0)")
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(140, 1, 2.0)

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(prev)")
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(140, 1, -1.0)  

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(null)")
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(140, 1, None)        

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(linear)")        
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(140, 1, None) 

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(next)")
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, -1.5)
        tdSql.checkData(140, 1, None)

        tdSql.query("select max(col1) - min(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' and id = 1 group by loc, id")
        rows = tdSql.queryRows

        tdSql.query("select spread(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' and id = 1 group by loc, id")
        tdSql.checkRows(rows)

        # case for TD-14782
        tdSql.execute("drop database if exists dd ")
        tdSql.execute("create database dd keep 36500")
        tdSql.execute("use dd")
        tdSql.execute("create stable stable_1(ts timestamp , q_double double ) tags(loc nchar(100))")
        tdSql.execute("create table stable_1_1 using stable_1 tags('stable_1_1')")
        tdSql.execute("create table stable_1_2 using stable_1 tags('stable_1_2')")
        tdSql.execute("insert into stable_1_1 (ts , q_double) values(1630000000000, 1)(1630000010000, 2)(1630000020000, 3)")

        tdSql.query("select STDDEV(q_double) from stable_1 where ts between 1630000001000 and 1630100001000 interval(18d) sliding(4d) Fill(NEXT) order by ts desc")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 0.5)
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 1, 0.5)
        tdSql.checkData(3, 1, 0.5)
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())