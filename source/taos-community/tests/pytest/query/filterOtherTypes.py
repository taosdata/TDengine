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

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("======= Verify filter for bool, nchar and binary type =========")
        tdLog.debug(
            "create table st(ts timestamp, tbcol1 bool, tbcol2 binary(10), tbcol3 nchar(20)) tags(tagcol1 bool, tagcol2 binary(10), tagcol3 nchar(10))")
        tdSql.execute(
            "create table st(ts timestamp, tbcol1 bool, tbcol2 binary(10), tbcol3 nchar(20)) tags(tagcol1 bool, tagcol2 binary(10), tagcol3 nchar(10))")

        tdSql.execute("create table st1 using st tags(true, 'table1', '水表')")
        for i in range(1, 6):
            tdSql.execute(
                "insert into st1 values(%d, %d, 'taosdata%d', '涛思数据%d')" %
                (self.ts + i, i %
                 2, i, i))

        tdSql.execute("create table st2 using st tags(false, 'table2', '电表')")
        for i in range(6, 11):
            tdSql.execute(
                "insert into st2 values(%d, %d, 'taosdata%d', '涛思数据%d')" %
                (self.ts + i, i %
                 2, i, i))

        # =============Verify stable columns====================
        # > for bool type on column
        tdSql.error("select * from st where tbcol1 > false")

        # >= for bool type on column
        tdSql.error("select * from st where tbcol1 >= false")

        # = for bool type on column
        tdSql.query("select * from st where tbcol1 = false")
        tdSql.checkRows(5)

        # <> for bool type on column
        tdSql.query("select * from st where tbcol1 <> true")
        tdSql.checkRows(5)

        # != for bool type on column
        tdSql.query("select * from st where tbcol1 != true")
        tdSql.checkRows(5)

        # > for bool type on column
        tdSql.error("select * from st where tbcol1 < true")

        # >= for bool type on column
        tdSql.error("select * from st where tbcol1 <= true")

        # % for bool type on column
        tdSql.error("select * from st where tbcol1 like '%'")

        # _ for bool type on column
        tdSql.error("select * from st where tbcol1 like '____'")

        # > for nchar type on column
        tdSql.error("select * from st where tbcol2 > 'taosdata'")

        # >= for nchar type on column
        tdSql.error("select * from st where tbcol2 >= 'taosdata'")

        # = for nchar type on column
        tdSql.query("select * from st where tbcol2 = 'taosdata1'")
        tdSql.checkRows(1)

        # <> for nchar type on column
        tdSql.query("select * from st where tbcol2 <> 'taosdata1'")
        tdSql.checkRows(9)

        # != for nchar type on column
        tdSql.query("select * from st where tbcol2 != 'taosdata1'")
        tdSql.checkRows(9)

        # > for nchar type on column
        tdSql.error("select * from st where tbcol2 < 'taodata'")

        # >= for nchar type on column
        tdSql.error("select * from st where tbcol2 <= 'taodata'")

        # % for nchar type on column case 1
        tdSql.query("select * from st where tbcol2 like '%'")
        tdSql.checkRows(10)

        # % for nchar type on column case 2
        tdSql.query("select * from st where tbcol2 like 'a%'")
        tdSql.checkRows(0)

        # % for nchar type on column case 3
        tdSql.query("select * from st where tbcol2 like 't%_'")
        tdSql.checkRows(10)

        # % for nchar type on column case 4
        tdSql.query("select * from st where tbcol2 like '%1'")
        # tdSql.checkRows(2)

        # _ for nchar type on column case 1
        tdSql.query("select * from st where tbcol2 like '____________'")
        tdSql.checkRows(0)

        # _ for nchar type on column case 2
        tdSql.query("select * from st where tbcol2 like '__________'")
        tdSql.checkRows(1)

        # _ for nchar type on column case 3
        tdSql.query("select * from st where tbcol2 like '_________'")
        tdSql.checkRows(9)

        # _ for nchar type on column case 4
        tdSql.query("select * from st where tbcol2 like 't________'")
        tdSql.checkRows(9)

        # _ for nchar type on column case 5
        tdSql.query("select * from st where tbcol2 like '%________'")
        tdSql.checkRows(10)

        # > for binary type on column
        tdSql.error("select * from st where tbcol3 > '涛思数据'")

        # >= for binary type on column
        tdSql.error("select * from st where tbcol3 >= '涛思数据'")

        # = for binary type on column
        tdSql.query("select * from st where tbcol3 = '涛思数据1'")
        tdSql.checkRows(1)

        # <> for binary type on column
        tdSql.query("select * from st where tbcol3 <> '涛思数据1'")
        tdSql.checkRows(9)

        # != for binary type on column
        tdSql.query("select * from st where tbcol3 != '涛思数据1'")
        tdSql.checkRows(9)

        # > for binary type on column
        tdSql.error("select * from st where tbcol3 < '涛思数据'")

        # >= for binary type on column
        tdSql.error("select * from st where tbcol3 <= '涛思数据'")

        # % for binary type on column case 1
        tdSql.query("select * from st where tbcol3 like '%'")
        tdSql.checkRows(10)

        # % for binary type on column case 2
        tdSql.query("select * from st where tbcol3 like '陶%'")
        tdSql.checkRows(0)

        # % for binary type on column case 3
        tdSql.query("select * from st where tbcol3 like '涛%_'")
        tdSql.checkRows(10)

        # % for binary type on column case 4
        tdSql.query("select * from st where tbcol3 like '%1'")
        tdSql.checkRows(1)

        # _ for binary type on column case 1
        tdSql.query("select * from st where tbcol3 like '_______'")
        tdSql.checkRows(0)

        # _ for binary type on column case 2
        tdSql.query("select * from st where tbcol3 like '______'")
        tdSql.checkRows(1)

        # _ for binary type on column case 2
        tdSql.query("select * from st where tbcol3 like '_____'")
        tdSql.checkRows(9)

        # _ for binary type on column case 3
        tdSql.query("select * from st where tbcol3 like '____'")
        tdSql.checkRows(0)

        # _ for binary type on column case 4
        tdSql.query("select * from st where tbcol3 like 't____'")
        tdSql.checkRows(0)

        # =============Verify stable tags====================
        # > for bool type on tag
        tdSql.error("select * from st where tagcol1 > false")

        # >= for bool type on tag
        tdSql.error("select * from st where tagcol1 >= false")

        # = for bool type on tag
        tdSql.query("select * from st where tagcol1 = false")
        tdSql.checkRows(5)

        # <> for bool type on tag
        tdSql.query("select * from st where tagcol1 <> true")
        tdSql.checkRows(5)

        # != for bool type on tag
        tdSql.query("select * from st where tagcol1 != true")
        tdSql.checkRows(5)

        # > for bool type on tag
        tdSql.error("select * from st where tagcol1 < true")

        # >= for bool type on tag
        tdSql.error("select * from st where tagcol1 <= true")

        # % for bool type on tag
        tdSql.error("select * from st where tagcol1 like '%'")

        # _ for bool type on tag
        tdSql.error("select * from st where tagcol1 like '____'")

        # > for nchar type on tag
        tdSql.query("select * from st where tagcol2 > 'table1'")
        tdSql.checkRows(5)

        # >= for nchar type on tag
        tdSql.query("select * from st where tagcol2 >= 'table1'")
        tdSql.checkRows(10)

        # = for nchar type on tag
        tdSql.query("select * from st where tagcol2 = 'table1'")
        tdSql.checkRows(5)

        # <> for nchar type on tag
        tdSql.query("select * from st where tagcol2 <> 'table1'")
        tdSql.checkRows(5)

        # != for nchar type on tag
        tdSql.query("select * from st where tagcol2 != 'table'")
        tdSql.checkRows(10)

        # > for nchar type on tag
        tdSql.query("select * from st where tagcol2 < 'table'")
        tdSql.checkRows(0)

        # >= for nchar type on tag
        tdSql.query("select * from st where tagcol2 <= 'table'")
        tdSql.checkRows(0)

        # % for nchar type on tag case 1
        tdSql.query("select * from st where tagcol2 like '%'")
        tdSql.checkRows(10)

        # % for nchar type on tag case 2
        tdSql.query("select * from st where tagcol2 like 'a%'")
        tdSql.checkRows(0)

        # % for nchar type on tag case 3
        tdSql.query("select * from st where tagcol2 like 't%_'")
        tdSql.checkRows(10)

        # % for nchar type on tag case 4
        tdSql.query("select * from st where tagcol2 like '%1'")
        tdSql.checkRows(5)

        # _ for nchar type on tag case 1
        tdSql.query("select * from st where tagcol2 like '_______'")
        tdSql.checkRows(0)

        # _ for nchar type on tag case 2
        tdSql.query("select * from st where tagcol2 like '______'")
        tdSql.checkRows(10)

        # _ for nchar type on tag case 3
        tdSql.query("select * from st where tagcol2 like 't_____'")
        tdSql.checkRows(10)

        # _ for nchar type on tag case 4
        tdSql.query("select * from st where tagcol2 like 's________'")
        tdSql.checkRows(0)

        # _ for nchar type on tag case 5
        tdSql.query("select * from st where tagcol2 like '%__'")
        tdSql.checkRows(10)

        # > for binary type on tag
        tdSql.query("select * from st where tagcol3 > '表'")
        tdSql.checkRows(10)

        # >= for binary type on tag
        tdSql.query("select * from st where tagcol3 >= '表'")
        tdSql.checkRows(10)

        # = for binary type on tag
        tdSql.query("select * from st where tagcol3 = '水表'")
        tdSql.checkRows(5)

        # <> for binary type on tag
        tdSql.query("select * from st where tagcol3 <> '水表'")
        tdSql.checkRows(5)

        # != for binary type on tag
        tdSql.query("select * from st where tagcol3 != '水表'")
        tdSql.checkRows(5)

        # > for binary type on tag
        tdSql.query("select * from st where tagcol3 < '水表'")
        tdSql.checkRows(0)

        # >= for binary type on tag
        tdSql.query("select * from st where tagcol3 <= '水表'")
        tdSql.checkRows(5)

        # % for binary type on tag case 1
        tdSql.query("select * from st where tagcol3 like '%'")
        tdSql.checkRows(10)

        # % for binary type on tag case 2
        tdSql.query("select * from st where tagcol3 like '水%'")
        tdSql.checkRows(5)

        # % for binary type on tag case 3
        tdSql.query("select * from st where tagcol3 like '数%_'")
        tdSql.checkRows(0)

        # % for binary type on tag case 4
        tdSql.query("select * from st where tagcol3 like '%表'")
        tdSql.checkRows(10)

        # % for binary type on tag case 5
        tdSql.query("select * from st where tagcol3 like '%据'")
        tdSql.checkRows(0)

        # _ for binary type on tag case 1
        tdSql.query("select * from st where tagcol3 like '__'")
        tdSql.checkRows(10)

        # _ for binary type on tag case 2
        tdSql.query("select * from st where tagcol3 like '水_'")
        tdSql.checkRows(5)

        # _ for binary type on tag case 2
        tdSql.query("select * from st where tagcol3 like '_表'")
        tdSql.checkRows(10)

        # _ for binary type on tag case 3
        tdSql.query("select * from st where tagcol3 like '___'")
        tdSql.checkRows(0)

        # _ for binary type on tag case 4
        tdSql.query("select * from st where tagcol3 like '数_'")
        tdSql.checkRows(0)

        # _ for binary type on tag case 5
        tdSql.query("select * from st where tagcol3 like '_据'")
        tdSql.checkRows(0)
        
        # test case for https://jira.taosdata.com:18080/browse/TD-857
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table meters(ts timestamp, voltage int) tags(tag1 binary(20))")
        tdSql.execute("create table t1 using meters tags('beijing')")
        tdSql.execute("create table t2 using meters tags('nanjing')")        

        tdSql.execute("insert into t1 values(1538548685000, 1) (1538548685001, 2) (1538548685002, 3)")
        tdSql.execute("insert into t2 values(1538548685000, 4) (1538548685001, 5) (1538548685002, 6)")

        tdSql.error("select * from t1 where tag1 like '%g'")        

        tdSql.error("select * from t2 where tag1 like '%g'") 

        tdSql.query("select * from meters where tag1 like '%g'")
        tdSql.checkRows(6)

        tdSql.execute("create table meters1(ts timestamp, voltage int) tags(tag1 nchar(20))")
        tdSql.execute("create table t3 using meters1 tags('北京')")
        tdSql.execute("create table t4 using meters1 tags('南京')")
        tdSql.execute("create table t5 using meters1 tags('beijing')")
        tdSql.execute("create table t6 using meters1 tags('nanjing')")        

        tdSql.execute("insert into t3 values(1538548685000, 1) (1538548685001, 2) (1538548685002, 3)")
        tdSql.execute("insert into t4 values(1538548685000, 4) (1538548685001, 5) (1538548685002, 6)")
        tdSql.execute("insert into t5 values(1538548685000, 1) (1538548685001, 2) (1538548685002, 3)")
        tdSql.execute("insert into t6 values(1538548685000, 1) (1538548685001, 2) (1538548685002, 3)")

        tdSql.error("select * from t3 where tag1 like '%京'")        

        tdSql.error("select * from t4 where tag1 like '%京'")        

        tdSql.query("select * from meters1 where tag1 like '%京'")
        tdSql.checkRows(6)
        
        tdSql.error("select * from t5 where tag1 like '%g'")        

        tdSql.error("select * from t6 where tag1 like '%g'")        

        tdSql.query("select * from meters1 where tag1 like '%g'")
        tdSql.checkRows(6)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
