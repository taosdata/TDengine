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

from new_test_framework.utils import tdLog, tdSql

class TestTtlComment:
    def caseDescription(self):
        '''
        ttl/comment test
        '''
        return

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def test_ttl_comment(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        dbname="db"
        tdSql.prepare()

        tdSql.error(f"create table {dbname}.ttl_table1(ts timestamp, i int) ttl 1.1")
        tdSql.error(f"create table {dbname}.ttl_table2(ts timestamp, i int) ttl 1e1")
        tdSql.error(f"create table {dbname}.ttl_table3(ts timestamp, i int) ttl -1")
        tdSql.error(f"create table {dbname}.ttl_table4(ts timestamp, i int) ttl 2147483648")

        print("============== STEP 1 ===== test normal table")

        tdSql.execute(f"create table {dbname}.normal_table1(ts timestamp, i int)")
        tdSql.execute(f"create table {dbname}.normal_table2(ts timestamp, i int) comment '' ttl 3")

        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 7, 3)
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.normal_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute(f"alter table {dbname}.normal_table1 comment ''")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.normal_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute(f"alter table {dbname}.normal_table1 ttl 1")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 1)

        tdSql.execute(f"alter table {dbname}.normal_table2 ttl 0")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 7, 0)

        print("============== STEP 2 ===== test super table")

        tdSql.execute(f"create table {dbname}.super_table1(ts timestamp, i int) tags(t int)")
        tdSql.execute(f"create table {dbname}.super_table2(ts timestamp, i int) tags(t int) comment ''")
        tdSql.execute(f"create table {dbname}.super_table3(ts timestamp, i int) tags(t int) comment 'super'")

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, None)

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, '')

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'super')

        tdSql.execute(f"alter table {dbname}.super_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, 'nihao')

        tdSql.execute(f"alter table {dbname}.super_table1 comment ''")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, '')

        tdSql.execute(f"alter table {dbname}.super_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, 'fly')

        tdSql.execute(f"alter table {dbname}.super_table3 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'tdengine')

        print("============== STEP 3 ===== test child table")

        tdSql.execute(f"create table {dbname}.child_table1 using  {dbname}.super_table1 tags(1) ttl 10")
        tdSql.execute(f"create table {dbname}.child_table2 using  {dbname}.super_table1 tags(1) comment ''")
        tdSql.execute(f"create table {dbname}.child_table3 using  {dbname}.super_table1 tags(1) comment 'child'")
        tdSql.execute(f"insert into {dbname}.child_table4 using  {dbname}.super_table1 tags(1) values(now, 1)")
        tdSql.execute(f"insert into {dbname}.child_table5 using  {dbname}.super_table1 tags(1) ttl 23 comment '' values(now, 1)")
        tdSql.error(f"insert into {dbname}.child_table6 using  {dbname}.super_table1 tags(1) ttl -23 comment '' values(now, 1)")

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 7, 10)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, '')

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'child')

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table5'")
        tdSql.checkData(0, 0, 'child_table5')
        tdSql.checkData(0, 7, 23)
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.child_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute(f"alter table {dbname}.child_table1 comment ''")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.child_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute(f"alter table {dbname}.child_table3 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'tdengine')

        tdSql.execute(f"alter table {dbname}.child_table4 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 8, 'tdengine')

        tdSql.execute(f"alter table {dbname}.child_table4 ttl 9")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 9)

        tdSql.execute(f"alter table {dbname}.child_table3 ttl 9")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 7, 9)

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
