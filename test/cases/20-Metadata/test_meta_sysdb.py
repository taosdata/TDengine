
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

from new_test_framework.utils import tdLog, tdSql

class TestDdlInSysdb:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def test_meta_sysdb(self):
        """Meta system database

        1. check table type in information_schema.ins_tables
        2. check ins_tables count plan optimization

        Since: v3.3.6

        Labels: common,ci

        Jira: TS-7600

        History:
            - 2025-11-13 Tony Zhang created
            - 2025-11-26 Tony Zhang add check_ins_tables_count_plan

        """
        self.check_ins_tables_count_plan()
        self.check_table_type()

    def check_table_type(self):
        # check table type in information_schema
        tdSql.query('''select type from information_schema.ins_tables
            where table_name="ctb" and create_time < now() + 1d''', show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'CHILD_TABLE')
        tdLog.info("child table type check passed")

    def check_ins_tables_count_plan(self):
        tdLog.info(f"Checking count plan for 'information_schema.ins_tables'")

        # create test table
        tdSql.execute("create database if not exists test_meta_sysdb", show=1)
        tdSql.execute("use test_meta_sysdb")
        tdSql.execute("create table stb (ts timestamp, v1 int) tags (t1 int)", show=1)
        tdSql.execute("create table ctb using stb tags (1)", show=1)

        expected_plan = f'Table Count Row Scan on ins_tables'
        # check count plan optimization for information_schema.ins_tables
        # to guarantee the efficiency of count(*) query on it
        tdSql.query(f'explain select count(*) from information_schema.ins_tables', show=1)
        scan_plan = tdSql.getColData(0)[-1]
        assert expected_plan in scan_plan, \
            f"expected '{expected_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"

        # check count result
        tdSql.query("select count(*) from information_schema.ins_tables", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 43) # 42 sys tables + 1 user table

        # check count result with group by
        tdSql.query(f'explain select count(*) from information_schema.ins_tables group by db_name', show=1)
        scan_plan = tdSql.getColData(0)[-1]
        assert expected_plan in scan_plan, \
            f"expected '{expected_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"

        # check group by result
        tdSql.query("select count(*) from information_schema.ins_tables group by db_name", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 37) # 37 tables in information_schema
        tdSql.checkData(1, 0, 5)  # 5  tables in sys
        tdSql.checkData(2, 0, 1)  # 1  table in test_meta_sysdb

        tdLog.info("Table count scan optimization check passed for all eligible tables.")
