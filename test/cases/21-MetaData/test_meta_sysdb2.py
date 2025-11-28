
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

NUM_INFO_DB_TABLES = 47  # number of system tables in information_schema
NUM_PERF_DB_TABLES = 5  # number of system tables in performance_schema
NUM_USER_DB_TABLES = 1  # number of user tables in test_meta_sysdb
class TestMetaSysDb2:
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
            - 2025-11-27 Tony Zhang add check_ins_tables_count and check_count_distinct

        """
        self.check_ins_tables_count()
        self.check_table_type()
        self.check_count_distinct()

    def check_table_type(self):
        # check table type in information_schema
        tdSql.query('''select type from information_schema.ins_tables
            where table_name="ctb" and create_time < now() + 1d''', show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'CHILD_TABLE')
        tdLog.info("child table type check passed")

    def check_ins_tables_count(self):
        tdLog.info(f"Checking optimized plan for 'information_schema.ins_tables'")

        # create test database and table
        tdSql.execute("create database if not exists test_meta_sysdb", show=1)
        tdSql.execute("use test_meta_sysdb")
        tdSql.execute("create table stb (ts timestamp, v1 int) tags (t1 int)", show=1)
        tdSql.execute("create table ctb using stb tags (1)", show=1)
        ## create an empty database to test count on empty db
        tdSql.execute("create database if not exists empty_db_for_count_test", show=1)
        tdSql.execute("use empty_db_for_count_test")
        tdSql.execute("create table stb_empty (ts timestamp, v1 int) tags (t1 int)", show=1)

        # check count plan optimization for information_schema.ins_tables
        # to guarantee the efficiency of count(*) query on it
        ## 1.1 check basic count plan
        tdSql.query("explain select count(*) cnt from information_schema.ins_tables", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        expect_plan = 'Table Count Scan on ins_tables'
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"

        ## 1.1 check count result
        tdSql.query("select count(*) cnt from information_schema.ins_tables", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, NUM_INFO_DB_TABLES + NUM_PERF_DB_TABLES + NUM_USER_DB_TABLES) # 47 sys tables + 5 perf tables + 1 user table

        ## 2. check plan with group by
        ### 2.1 check plan with group by db_name
        tdSql.query("explain select db_name, count(*) cnt from information_schema.ins_tables \
                    group by db_name order by cnt desc", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"

        ### 2.2 check group by db_name result
        tdSql.query("select db_name, count(*) cnt from information_schema.ins_tables \
                    group by db_name order by cnt desc", show=1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, NUM_INFO_DB_TABLES) # 47 tables in information_schema
        tdSql.checkData(1, 1, NUM_PERF_DB_TABLES)  # 5  tables in sys
        tdSql.checkData(2, 1, NUM_USER_DB_TABLES)  # 1  table in test_meta_sysdb
        tdSql.checkData(3, 1, 0)  # 0  table in empty_db_for_count_test

        ### 2.3 check plan with group by stable_name
        tdSql.query("explain select stable_name, count(*) cnt from information_schema.ins_tables \
                    group by stable_name order by cnt desc", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"
        
        ### 2.4 check group by stable_name result
        tdSql.query("select stable_name, count(*) cnt from information_schema.ins_tables \
                    group by stable_name order by cnt desc", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, NUM_INFO_DB_TABLES + NUM_PERF_DB_TABLES) # 47+5 normal tables in system databases
        tdSql.checkData(1, 0, "stb")
        tdSql.checkData(1, 1, NUM_USER_DB_TABLES)  # 1  table in test_meta_sysdb.stb
        tdSql.checkData(2, 0, "stb_empty")
        tdSql.checkData(2, 1, 0)  # 0  table in empty_db_for_count_test.stb_empty

        ## 3. check plan with where condition
        ### 3.1 check plan with where db_name
        tdSql.query("explain select count(*) cnt from information_schema.ins_tables \
                    where db_name='information_schema'", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        expect_plan = 'Table Count Scan on information_schema'
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"
        
        ### 3.2 check count with where db_name result
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where db_name='information_schema'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, NUM_INFO_DB_TABLES) # 47 tables in information_schema
        ### 3.3 check plan with where stable_name
        tdSql.query("explain select count(*) cnt from information_schema.ins_tables \
                    where stable_name='stb'", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        expect_plan = 'Table Count Scan on stb'
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"
        
        ### 3.4 check count with where stable_name result
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where stable_name='stb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, NUM_USER_DB_TABLES) # 1 table in test_meta_sysdb.stb

        ### 3.5 check plan with where db_name and stable_name
        tdSql.query("explain select count(*) cnt from information_schema.ins_tables \
                    where db_name='test_meta_sysdb' and stable_name='stb'", show=1)
        scan_plan = tdSql.getColData(0)[-1]
        expect_plan = 'Table Count Scan on test_meta_sysdb.stb'
        assert expect_plan in scan_plan, \
            f"expected '{expect_plan}' in scan plan for information_schema.ins_tables, got: {scan_plan}"

        ### 3.6 check count with where db_name and stable_name result
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where db_name='test_meta_sysdb' and stable_name='stb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, NUM_USER_DB_TABLES) # 1 table in test_meta_sysdb.stb

        ### 3.7 check non-existing db_name and stable_name
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where db_name='test_meta_sysdb' and stable_name='stbbbbb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where db_name='test_meta_sysdbbbbb' and stable_name='stb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where db_name='test_meta_sysdbbbbb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) cnt from information_schema.ins_tables \
                    where stable_name='test_meta_sysdbbbbb'", show=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdLog.info("Table count scan optimization check passed for all eligible tables.")

    # if distinct keyword is enabled in count function,
    # the optimization should check it and NOT apply optimization!!!!!
    def check_count_distinct(self):
        tdSql.error("select count(distinct table_name) from information_schema.ins_tables",
                     expectErrInfo='syntax error near "distinct table_name) from information_schema.ins_tables"',
                    show=1)