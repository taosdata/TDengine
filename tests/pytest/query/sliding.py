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
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1500000000000

    def run(self):
        tdSql.prepare()
        
        tdSql.execute("create table meters(ts timestamp, col1 int) tags(id int, loc nchar(20))")
        sql = "insert into t0 using meters tags(1, 'beijing') values"
        for i in range(100):
            sql += "(%d, %d)" % (self.ts + i * 1000, random.randint(1, 100))        
        tdSql.execute(sql)   

        sql = "insert into t1 using meters tags(2, 'shanghai') values"
        for i in range(100):
            sql += "(%d, %d)" % (self.ts + i * 1000, random.randint(1, 100))        
        tdSql.execute(sql)
        
        tdSql.query("select count(*) from meters interval(10s) sliding(5s)")
        tdSql.checkRows(21)

        tdSql.error("select count(*) from meters sliding(5s)")

        tdSql.error("select count(*) from meters sliding(5s) interval(10s)")

        tdSql.error("select * from meters sliding(5s) order by ts desc")
                
        tdSql.query("select count(*) from meters group by loc")
        tdSql.checkRows(2)

        tdSql.error("select * from meters group by loc sliding(5s)")   

        # TD-2700
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table t1(ts timestamp, k int)")
        tdSql.execute("insert into t1 values(1500000001000, 0)")
        tdSql.query("select sum(k) from t1 interval(1d) sliding(1h)")
        tdSql.checkRows(24)

        # TD-14690
        tdSql.execute("drop database if exists ceil")
        tdSql.execute("create database ceil keep 36500")
        tdSql.execute("use ceil")
        tdSql.execute("create stable stable_1 (ts timestamp , q_nchar nchar(20) ) tags(loc nchar(100))")
        tdSql.execute("create table stable_1_1 using stable_1 tags('stable_1_1')")
        tdSql.execute("create table stable_1_2 using stable_1 tags('stable_1_2')")

        self.ts = 1630000000000
        for i in range(10):
            if i <= 5:
                tdSql.execute("insert into stable_1_1 (ts , q_nchar) values(%d, %d)" % (self.ts + i * 1000, i % 5 + 1))
                tdSql.execute("insert into stable_1_2 (ts , q_nchar) values(%d, %d)" % (self.ts + i * 1000, i % 5 + 1))
            else:
                tdSql.execute("insert into stable_1_1 (ts , q_nchar) values(%d, %d)" % (self.ts + 39000 + i * 1000, i % 5))
                tdSql.execute("insert into stable_1_2 (ts , q_nchar) values(%d, %d)" % (self.ts + 10000 + i * 1000, i % 5 + 1))

        tdSql.execute("insert into stable_1_1 (ts , q_nchar) values(%d, %d)" % (1630000049000, 5))
        tdSql.execute("insert into stable_1_2 (ts , q_nchar) values(%d, %d)" % (1630000020000, 1))

        tdSql.query("select COUNT(q_nchar) from stable_1 interval(20s) sliding(10s) order by ts")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 1, 5)

        tdSql.query("select COUNT(q_nchar) from (select * from stable_1) interval(20s) sliding(10s) order by ts")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 1, 5)
        
        tdSql.query("select COUNT(q_nchar) from (select * from stable_1 order by ts) interval(20s) sliding(10s);")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 1, 5)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
