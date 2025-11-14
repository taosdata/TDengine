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
import subprocess
import re
import random
import numpy as np
import platform
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes


dbname = 'db'
msec_per_min = 60 * 1000
class TestFunDerivative:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")

    def insertAndCheckData(self):
        types = ["tinyint", "tinyint unsigned", "smallint", "smallint unsigned", "int", "int unsigned", "bigint", "bigint unsigned", "float", "double", "bool", "binary(20)", "nchar(20)"]

        for datatype in types:
            print("============== create table using %s type ================" % datatype)
            tdSql.execute("drop table if exists stb")
            tdSql.execute("create table stb(ts timestamp, col %s) tags (id int)" % datatype)
            tdSql.execute("create table tb1 using stb tags(1)")
            tdSql.execute("create table tb2 using stb tags(2)")

            if datatype == "tinyint" or datatype == "smallint" or datatype == "int" or datatype == "bigint":
                tdSql.execute("insert into tb1 values(%d, 1)(%d, 11)(%d, 21)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb1 values(%d, -1)(%d, -11)(%d, -21)" % (self.ts + 30000, self.ts + 40000, self.ts + 50000))
                tdSql.execute("insert into tb2 values(%d, 10)(%d, 20)(%d, 30)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))
                tdSql.execute("insert into tb2 values(%d, -10)(%d, -20)(%d, -30)" % (self.ts + 90000, self.ts + 1000000, self.ts + 1100000))

                tdSql.execute("insert into tb3 using stb tags(3) values(%d, 10)" % (self.ts + 1200000))
                
                tdSql.query("select derivative(col, 1s, 1) from stb partition by tbname")
                tdSql.checkRows(4)

                tdSql.query("select derivative(col, 10s, 1) from stb partition by tbname")
                tdSql.checkRows(4)

                tdSql.query("select derivative(col, 10s, 0) from stb partition by tbname")
                tdSql.checkRows(10)

                tdSql.query("select ts,derivative(col, 10s, 1),ts from stb partition by tbname")
                tdSql.checkRows(4)
                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 1, 10)
                tdSql.checkData(0, 2, "2018-09-17 09:00:10.000")
                tdSql.checkData(3, 0, "2018-09-17 09:01:20.000")
                tdSql.checkData(3, 1, 10)
                tdSql.checkData(3, 2, "2018-09-17 09:01:20.000")

                tdSql.query("select ts,derivative(col, 10s, 1),ts from tb1")
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")
                tdSql.checkData(0, 1, 10)
                tdSql.checkData(0, 2, "2018-09-17 09:00:10.000")
                tdSql.checkData(1, 0, "2018-09-17 09:00:20.000")
                tdSql.checkData(1, 1, 10)
                tdSql.checkData(1, 2, "2018-09-17 09:00:20.000")
                tdSql.query("select ts from(select ts,derivative(col, 10s, 0) from stb partition by tbname)")

                tdSql.checkData(0, 0, "2018-09-17 09:00:10.000")

                tdSql.error("select derivative(col, 10s, 0) from tb1 group by tbname")

                tdSql.query("select derivative(col, 10s, 1) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb1")
                tdSql.checkRows(5)

                tdSql.query("select derivative(col, 10s, 1) from tb2")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb2")
                tdSql.checkRows(5)

                tdSql.query("select derivative(col, 10s, 0) from tb3")
                tdSql.checkRows(0)

            elif datatype == "tinyint unsigned" or datatype == "smallint unsigned" or datatype == "int unsigned" or datatype == "bigint unsigned":
                tdSql.execute("insert into tb1 values(%d, 1)(%d, 11)(%d, 21)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, 10)(%d, 20)(%d, 30)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.query("select derivative(col, 1s, 1) from tb1")
                tdSql.query("select derivative(col, 10s, 0) from tb1")
                tdSql.query("select derivative(col, 999a, 0) from tb1")
                tdSql.query("select derivative(col, 1s, 1) from tb2")
                tdSql.query("select derivative(col, 10s, 0) from tb2")
                tdSql.query("select derivative(col, 999a, 0) from tb2")

            elif datatype == "float" or datatype == "double":
                tdSql.execute("insert into tb1 values(%d, 1.0)(%d, 11.0)(%d, 21.0)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, 3.0)(%d, 4.0)(%d, 5.0)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.query("select derivative(col, 10s, 1) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb1")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 1) from tb2")
                tdSql.checkRows(2)

                tdSql.query("select derivative(col, 10s, 0) from tb2")
                tdSql.checkRows(2)

            elif datatype == "bool":
                tdSql.execute("insert into tb1 values(%d, true)(%d, false)(%d, true)" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, false)(%d, true)(%d, true)" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.error("select derivative(col, 1s, 1) from tb1")
                tdSql.error("select derivative(col, 10s, 0) from tb1")
                tdSql.error("select derivative(col, 999ms, 0) from tb1")
                tdSql.error("select derivative(col, 1s, 1) from tb2")
                tdSql.error("select derivative(col, 10s, 0) from tb2")
                tdSql.error("select derivative(col, 999ms, 0) from tb2")

            else:
                tdSql.execute("insert into tb1 values(%d, 'test01')(%d, 'test01')(%d, 'test01')" % (self.ts, self.ts + 10000, self.ts + 20000))
                tdSql.execute("insert into tb2 values(%d, 'test01')(%d, 'test01')(%d, 'test01')" % (self.ts + 60000, self.ts + 70000, self.ts + 80000))

                tdSql.error("select derivative(col, 1s, 1) from tb1")
                tdSql.error("select derivative(col, 10s, 0) from tb1")
                tdSql.error("select derivative(col, 999ms, 0) from tb1")
                tdSql.error("select derivative(col, 1s, 1) from tb2")
                tdSql.error("select derivative(col, 10s, 0) from tb2")
                tdSql.error("select derivative(col, 999ms, 0) from tb2")
                
            if datatype == "bool" or datatype == "binary(20)" or datatype == "nchar(20)":
                tdSql.error("select derivative(col, 10s, 1) from stb")
            else:
                tdSql.query("select derivative(col, 10s, 1) from stb")
            tdSql.error("select derivative(col, 10s, 1) from stb group by col")
            tdSql.error("select derivative(col, 10s, 1) from stb group by id")
            tdSql.error("select derivative(col, 999ms, 1) from stb group by id")
            tdSql.error("select derivative(col, 10s, 2) from stb group by id")
            
    def checkRowsTest(self):
        tdSql.execute("create database if not exists monitor")
        tdSql.execute("use monitor")
        tdSql.execute("drop table if exists st")
        tdSql.execute("create table st(ts timestamp, c1 double, c2 double) tags (t1 varchar(64))")
        tdSql.execute("create table st1 using st tags('yfbfd2_bjhg3q006_avgtemp')")
        
        for i in range(36):
            ts = "2025-11-10 %02d:%02d:00.000" % (11, 5 + i)
            c1 = 7426.0 + (random.randint(-100, 100) / 10.0)
            c2 = 162.0 + (random.randint(-10, 10) / 10.0)
            tdSql.execute("insert into st1 values('%s', %.3f, %.3f)" % (ts, c1, c2))
        
        tdSql.query("select ts, tbname from monitor.st where ts > '2025-11-10 11:06:00' and ts <= '2025-11-10 11:07:00' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname;")
        tdSql.checkRows(1)
        
        tdSql.query("select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:06:00' and ts <= '2025-11-10 11:07:00' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname;")
        tdSql.checkRows(0)
        
        tdSql.query("select ts, c1, tbname from monitor.st where ts >= '2025-11-10 11:06:00' and ts <= '2025-11-10 11:07:00' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname;")
        tdSql.checkRows(2)
        rate = (tdSql.getData(1, 1) - tdSql.getData(0, 1)) / 1.0
        
        tdSql.query("select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts >= '2025-11-10 11:06:00' and ts <= '2025-11-10 11:07:00' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-11-10 11:07:00.000")
        tdSql.checkData(0, 1, rate)
        tdSql.checkData(0, 2, "st1")

        tdSql.query("select _wstart ,sum(rate1) rate10 from (select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:06:00.000' and ts <= '2025-11-10 11:08:00.000' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname ) partition by tbname interval (1h);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-11-10 11:00:00.000")
        
        tdSql.query("select _wstart ,sum(rate1) rate10 from (select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:20:00.000' and ts <= '2025-11-10 11:30:00.000' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname ) partition by tbname interval (1h);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-11-10 11:00:00.000")
        
        tdSql.query("select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:20:00.000' and ts <= '2025-11-10 11:30:00.000' and t1 in ('yfbfd2_bjhg3q006_avgtemp')")
        tdSql.checkRows(9)
        
        tdSql.query("select _wstart, sum(rate1) rate10 from (select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:20:00.000' and ts <= '2025-11-10 11:30:00.000' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname ) partition by tbname interval (1m);")
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, "2025-11-10 11:22:00.000")
        tdSql.checkData(1, 0, "2025-11-10 11:23:00.000")
        tdSql.checkData(8, 0, "2025-11-10 11:30:00.000")
        
        tdSql.query("select _wstart, sum(rate1) rate10 from (select ts, DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where ts > '2025-11-10 11:20:00.000' and ts <= '2025-11-10 11:30:00.000' and t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname ) partition by tbname interval (2m);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-11-10 11:22:00.000")
        tdSql.checkData(1, 0, "2025-11-10 11:24:00.000")
        tdSql.checkData(4, 0, "2025-11-10 11:30:00.000")
        

        tdSql.query("select ts,DERIVATIVE(c1,1m,0) rate1, tbname from monitor.st where t1 in ('yfbfd2_bjhg3q006_avgtemp') partition by tbname;")
        tdSql.checkRows(35)


    #
    # ------------------ main ------------------
    #
    def test_func_ts_derivative(self):
        """ Fun: derivative()

        1. Basic query for different params
        2. Query on super/child/normal table
        3. Support data types
        4. Error cases
        5. Query with where condition
        6. Query with partition/group/order by
        7. Query with sub query
        8. Query with function nested
        9. Query with limit/slimit/offset/soffset
        10. Check null value

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_mavg.py

        """
        self.rowNum = 10
        self.ts = 1537146000000

        tdSql.prepare()        
        self.insertAndCheckData()

        tdSql.execute("create table st(ts timestamp, c1 int, c2 int) tags(id int)")
        tdSql.execute("insert into dev1(ts, c1) using st tags(1) values(now, 1)")

        tdSql.error("select derivative(c1, 10s, 0) from (select c1 from st)")
        tdSql.error("select diff(c1) from (select derivative(c1, 1s, 0) c1 from dev1)")
        
        self.checkRowsTest()

        tdLog.success("%s successfully executed" % __file__)
