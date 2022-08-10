###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
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
    def caseDescription(self):
        '''
        ttl/comment test
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.error("create table ttl_table1(ts timestamp, i int) ttl 1.1")
        tdSql.error("create table ttl_table2(ts timestamp, i int) ttl 1e1")
        tdSql.error("create table ttl_table3(ts timestamp, i int) ttl -1")

        print("============== STEP 1 ===== test normal table")

        tdSql.execute("create table normal_table1(ts timestamp, i int)")
        tdSql.execute("create table normal_table2(ts timestamp, i int) comment '' ttl 3")
        tdSql.execute("create table normal_table3(ts timestamp, i int) ttl 2100000000020 comment 'hello'")

        tdSql.query("show tables like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)


        tdSql.query("show tables like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 7, 3)
        tdSql.checkData(0, 8, '')


        tdSql.query("show tables like 'normal_table3'")
        tdSql.checkData(0, 0, 'normal_table3')
        tdSql.checkData(0, 7, 2147483647)
        tdSql.checkData(0, 8, 'hello')

        tdSql.execute("alter table normal_table1 comment 'nihao'")
        tdSql.query("show tables like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute("alter table normal_table1 comment ''")
        tdSql.query("show tables like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute("alter table normal_table2 comment 'fly'")
        tdSql.query("show tables like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute("alter table normal_table3 comment 'fly'")
        tdSql.query("show tables like 'normal_table3'")
        tdSql.checkData(0, 0, 'normal_table3')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute("alter table normal_table1 ttl 1")
        tdSql.query("show tables like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 1)

        tdSql.execute("alter table normal_table3 ttl 0")
        tdSql.query("show tables like 'normal_table3'")
        tdSql.checkData(0, 0, 'normal_table3')
        tdSql.checkData(0, 7, 0)


        print("============== STEP 2 ===== test super table")

        tdSql.execute("create table super_table1(ts timestamp, i int) tags(t int)")
        tdSql.execute("create table super_table2(ts timestamp, i int) tags(t int) comment ''")
        tdSql.execute("create table super_table3(ts timestamp, i int) tags(t int) comment 'super'")

        tdSql.query("show stables like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, None)


        tdSql.query("show stables like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, '')


        tdSql.query("show stables like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'super')


        tdSql.execute("alter table super_table1 comment 'nihao'")
        tdSql.query("show stables like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, 'nihao')

        tdSql.execute("alter table super_table1 comment ''")
        tdSql.query("show stables like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, '')

        tdSql.execute("alter table super_table2 comment 'fly'")
        tdSql.query("show stables like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, 'fly')

        tdSql.execute("alter table super_table3 comment 'tdengine'")
        tdSql.query("show stables like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'tdengine')

        print("============== STEP 3 ===== test child table")

        tdSql.execute("create table child_table1 using super_table1 tags(1) ttl 10")
        tdSql.execute("create table child_table2 using super_table1 tags(1) comment ''")
        tdSql.execute("create table child_table3 using super_table1 tags(1) comment 'child'")
        tdSql.execute("insert into child_table4 using super_table1 tags(1) values(now, 1)")


        tdSql.query("show tables like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 7, 10)
        tdSql.checkData(0, 8, None)


        tdSql.query("show tables like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, '')


        tdSql.query("show tables like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'child')

        tdSql.query("show tables like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)


        tdSql.execute("alter table child_table1 comment 'nihao'")
        tdSql.query("show tables like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute("alter table child_table1 comment ''")
        tdSql.query("show tables like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute("alter table child_table2 comment 'fly'")
        tdSql.query("show tables like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute("alter table child_table3 comment 'tdengine'")
        tdSql.query("show tables like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'tdengine')


        tdSql.execute("alter table child_table4 comment 'tdengine'")
        tdSql.query("show tables like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 8, 'tdengine')

        tdSql.execute("alter table child_table4 ttl 9")
        tdSql.query("show tables like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 9)

        tdSql.execute("alter table child_table3 ttl 9")
        tdSql.query("show tables like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 7, 9)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

