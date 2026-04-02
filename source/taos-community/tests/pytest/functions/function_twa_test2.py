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
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()
        
        tdSql.execute("create table t1(ts timestamp, c int)")        
        for i in range(self.rowNum):
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i * 10000, i + 1))            

        # twa verifacation 
        tdSql.query("select twa(c) from t1 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.5)

        tdSql.query("select twa(c) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.5)

        tdSql.query("select twa(c) from t1 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' interval(10s)")
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 1.49995)
        tdSql.checkData(1, 1, 2.49995)
        tdSql.checkData(2, 1, 3.49995)
        tdSql.checkData(3, 1, 4.49995)
        tdSql.checkData(4, 1, 5.49995)
        tdSql.checkData(5, 1, 6.49995)
        tdSql.checkData(6, 1, 7.49995)
        tdSql.checkData(7, 1, 8.49995)
        tdSql.checkData(8, 1, 9.49995)
        tdSql.checkData(9, 1, 10)

        tdSql.query("select twa(c) from t1 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' interval(10s) sliding(5s)")
        tdSql.checkRows(20)
        tdSql.checkData(0, 1, 1.24995)
        tdSql.checkData(1, 1, 1.49995)
        tdSql.checkData(2, 1, 1.99995)
        tdSql.checkData(3, 1, 2.49995)
        tdSql.checkData(4, 1, 2.99995)
        tdSql.checkData(5, 1, 3.49995)
        tdSql.checkData(6, 1, 3.99995)
        tdSql.checkData(7, 1, 4.49995)
        tdSql.checkData(8, 1, 4.99995)
        tdSql.checkData(9, 1, 5.49995)        
        tdSql.checkData(10, 1, 5.99995)
        tdSql.checkData(11, 1, 6.49995)
        tdSql.checkData(12, 1, 6.99995)
        tdSql.checkData(13, 1, 7.49995)
        tdSql.checkData(14, 1, 7.99995)
        tdSql.checkData(15, 1, 8.49995)
        tdSql.checkData(16, 1, 8.99995)
        tdSql.checkData(17, 1, 9.49995)
        tdSql.checkData(18, 1, 9.75000)
        tdSql.checkData(19, 1, 10)

        tdSql.execute("create table t2(ts timestamp, c int)")
        tdSql.execute("insert into t2 values(%d, 1)" % (self.ts + 3000))
        tdSql.query("select twa(c) from t2 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query("select twa(c) from t2 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' interval(2s) ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select twa(c) from t2 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 09:01:30.000' interval(2s) sliding(1s) ")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        tdSql.query("select twa(c) from t2 where ts >= '2018-09-17 09:00:04.000' and ts <= '2018-09-17 09:01:30.000' ")
        tdSql.checkRows(0)

        tdSql.query("select twa(c) from t2 where ts >= '2018-09-17 08:00:00.000' and ts <= '2018-09-17 09:00:00.000' ")
        tdSql.checkRows(0)

        tdSql.execute("create table t3(ts timestamp, c int)")
        tdSql.execute("insert into t3 values(%d, 1)" % (self.ts))
        tdSql.execute("insert into t3 values(%d, -2)" % (self.ts + 3000))

        tdSql.query("select twa(c) from t3 where ts >= '2018-09-17 08:59:00.000' and ts <= '2018-09-17 09:01:30.000'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -0.5)

        tdSql.query("select twa(c) from t3 where ts >= '2018-09-17 08:59:00.000' and ts <= '2018-09-17 09:01:30.000' interval(1s)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 0.5005)
        tdSql.checkData(1, 1, -2)

        tdSql.query("select twa(c) from t3 where ts >= '2018-09-17 08:59:00.000' and ts <= '2018-09-17 09:01:30.000' interval(2s) sliding(1s)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 0.5005)
        tdSql.checkData(1, 1, 0.0005)
        tdSql.checkData(2, 1, -1.5)
        tdSql.checkData(3, 1, -2)

        #TD-2533 twa+interval with large records 
        tdSql.execute("create table t4(ts timestamp, c int)") 
        sql = 'insert into t4 values '       
        for i in range(20000):
            sql = sql + '(%d, %d)' % (self.ts + i * 500, i + 1)
            if i % 2000 == 0:
                tdSql.execute(sql)
                sql = 'insert into t4 values ' 
        tdSql.execute(sql)
        tdSql.query('select twa(c) from t4 interval(10s)')
        tdSql.checkData(0,1,10.999)

        # Test case: https://jira.taosdata.com:18080/browse/TD-2624
        tdSql.execute("create database test keep 7300")
        tdSql.execute("use test")
        tdSql.execute("create table st(ts timestamp, k int)")
        tdSql.execute("insert into st values('2011-01-02 18:42:45.326', -1)")
        tdSql.execute("insert into st values('2020-07-30 17:44:06.283', 0)")
        tdSql.execute("insert into st values('2020-07-30 17:44:19.578', 9999999)")
        tdSql.execute("insert into st values('2020-07-30 17:46:06.417', NULL)")
        tdSql.execute("insert into st values('2020-11-09 18:42:25.538', 0)")
        tdSql.execute("insert into st values('2020-12-29 17:43:11.641', 0)")
        tdSql.execute("insert into st values('2020-12-29 18:43:17.129', 0)")
        tdSql.execute("insert into st values('2020-12-29 18:46:19.109', NULL)")
        tdSql.execute("insert into st values('2021-01-03 18:40:40.065', 0)")

        tdSql.query("select twa(k),first(ts) as taos1  from st where k <50 interval(17s)")
        tdSql.checkRows(6)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
