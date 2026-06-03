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
from util.dnodes import tdDnodes

class TDTestCase:
    def init(self, conn, logSql, replicaVar = 1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def bug_6387(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create table test(ts timestamp, c1 int) tags(t1 int)")
        for i in range(5000):
            sql = "insert into t%d using test tags(1) values " % i
            for j in range(21):
                sql = sql + "(now+%ds,%d)" % (j ,j )
            tdSql.execute(sql)
#        tdSql.query("select count(*) from test interval(1s) group by tbname")
#        tdSql.checkData(0,1,1)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02")')

        print("==============step2")

        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1),
            ('2020-05-13 10:00:00.001', 1)
             dev_002 VALUES('2020-05-13 10:00:00.001', 1)""")

        tdSql.query("select * from db.st where ts='2020-05-13 10:00:00.000'")
        tdSql.checkRows(1)

#        tdSql.query("select tbname, dev from dev_001") 
#        tdSql.checkRows(1)
#        tdSql.checkData(0, 0, 'dev_001')
#        tdSql.checkData(0, 1, 'dev_01')

        tdSql.query("select tbname, dev, tagtype from dev_001") 
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'dev_001')
        tdSql.checkData(0, 1, 'dev_01')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, 'dev_001')
        tdSql.checkData(1, 1, 'dev_01')
        tdSql.checkData(1, 2, 1)

        ## test case for https://jira.taosdata.com:18080/browse/TD-2488
        tdSql.execute("create table m1(ts timestamp, k int) tags(a int)")
        tdSql.execute("create table t1 using m1 tags(1)")
        tdSql.execute("create table t2 using m1 tags(2)")
        tdSql.execute("insert into t1 values('2020-1-1 1:1:1', 1)")
        tdSql.execute("insert into t1 values('2020-1-1 1:10:1', 2)")
        tdSql.execute("insert into t2 values('2020-1-1 1:5:1', 99)")
        
        tdSql.query("select count(*) from m1 where ts = '2020-1-1 1:5:1' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("select count(*) from m1 where ts = '2020-1-1 1:5:1' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        ## test case for https://jira.taosdata.com:18080/browse/TD-1930
        tdSql.execute("create table tb(ts timestamp, c1 int, c2 binary(10), c3 nchar(10), c4 float, c5 bool)")
        for i in range(10):
            tdSql.execute("insert into tb values(%d, %d, 'binary%d', 'nchar%d', %f, %d)" % (self.ts + i, i, i, i, i + 0.1, i % 2))
        
        tdSql.error("select * from tb where c2 = binary2")
        tdSql.error("select * from tb where c3 = nchar2")

        tdSql.query("select * from tb where c2 = 'binary2' ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c3 = 'nchar2' ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c1 = '2' ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c1 = 2 ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c4 = '0.1' ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c4 = 0.1 ")
        tdSql.checkRows(1)

        tdSql.query("select * from tb where c5 = true ")
        tdSql.checkRows(5)

        tdSql.query("select * from tb where c5 = 'true' ")
        tdSql.checkRows(5)

        # For jira: https://jira.taosdata.com:18080/browse/TD-2850
        tdSql.execute("create database `Test` ")
        tdSql.execute("use `Test` ")
        tdSql.execute("create table TB(ts timestamp, `Col1` int) tags(`Tag1` int)")
        tdSql.execute("insert into Tb0 using tb tags(1) values(now, 1)")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)

        tdSql.query("select * from tb0")
        tdSql.checkRows(1)

        #For jira: https://jira.taosdata.com:18080/browse/TD-6387
        #self.bug_6387()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
