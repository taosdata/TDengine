
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

        self.ts = 1538548685000

    def bug_6387(self):
        tdSql.execute("create database bug6387 ")
        tdSql.execute("use bug6387 ")
        tdSql.execute("create table test(ts timestamp, c1 int) tags(t1 int)")
        prefix = "insert into "
        sql = ""
        for i in range(5000):
            temp = "t%d using test tags(1) values " % i
            for j in range(21):
                temp = temp + "(now+%ds,%d)" % (j ,j )
            sql = sql + temp
            if i % 1000 == 0 :
                tdSql.execute(prefix + sql)
                sql = ""
        tdSql.query("select count(*) from test interval(1s) group by tbname")
        tdSql.checkData(0,1,1)
    
    def escape_ascii(self):
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        tdSql.execute("create table car (ts timestamp, s int) tags(j int)")
        for i in range(32,127):
            if i == 96 : continue    #`
            sql = 'create table `是否出现%s` using car tags(%d)' % (chr(i), i)
            tdSql.execute(sql)
        for i in range(32,65):
            sql = 'select tbname from car where tbname like "是否出现\%s"' % chr(i)
            tdSql.query(sql)
            if i == 37 : continue  # " `
            tdSql.checkRows(1)
        for i in range(91,97):
            sql = 'select tbname from car where tbname like "是否出现\%s"' % chr(i)
            tdSql.query(sql)
            if i == 96: continue  #  `
            tdSql.checkRows(1)
        for i in range(123,127):
            sql = 'select tbname from car where tbname like "是否出现\%s"' % chr(i)
            tdSql.query(sql)
            tdSql.checkRows(1)

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

        tdSql.query("select tbname, dev from dev_001")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'dev_001')
        tdSql.checkData(0, 1, 'dev_01')

        tdSql.query("select tbname, dev, tagtype from dev_001")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'dev_001')
        tdSql.checkData(0, 1, 'dev_01')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, 'dev_001')
        tdSql.checkData(1, 1, 'dev_01')
        tdSql.checkData(1, 2, 1)

        ## TD-2488
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

        ## TD-1930
        tdSql.execute("create table tb(ts timestamp, c1 int, c2 binary(10), c3 nchar(10), c4 float, c5 bool)")
        for i in range(10):
            tdSql.execute(
                "insert into tb values(%d, %d, 'binary%d', 'nchar%d', %f, %d)" % (self.ts + i, i, i, i, i + 0.1, i % 2))

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

        # TD-2850
        tdSql.execute("create database 'Test' ")
        tdSql.execute("use 'Test' ")
        tdSql.execute("create table 'TB'(ts timestamp, 'Col1' int) tags('Tag1' int)")
        tdSql.execute("insert into 'Tb0' using tb tags(1) values(now, 1)")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.query("select * from tb0")
        tdSql.checkRows(1)

        # TD-6314
        tdSql.execute("use db")
        tdSql.execute("create stable stb_001(ts timestamp,v int) tags(c0 int)")
        tdSql.execute("insert into stb1 using stb_001 tags(1) values(now,1)")
        tdSql.query("select _block_dist() from stb_001")
        tdSql.checkRows(1)

        

        #TD-6387
        tdLog.info("case for bug_6387")
        self.bug_6387()

        #JIRA TS-583
        tdLog.info("case for JIRA TS-583")
        tdSql.execute("create database test2")
        tdSql.execute("use test2")
        tdSql.execute("create table stb(ts timestamp, c1 int) tags(t1 binary(120))")
        tdSql.execute("create table t0 using stb tags('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz')")
    
        tdSql.query("show create table t0")        
        tdSql.checkRows(1)

        tdSql.execute("create table stb2(ts timestamp, c1 int) tags(t1 nchar(120))")
        tdSql.execute("create table t1 using stb2 tags('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz')")
        
        tdSql.query("show create table t1")        
        tdSql.checkRows(1)

        #TS-636
        tdLog.info("case for TS-636")
        self.escape_ascii()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())