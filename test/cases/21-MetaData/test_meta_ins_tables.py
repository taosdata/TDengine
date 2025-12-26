
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


from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
from math import inf
import platform
import os
import sys


NUM_INFO_DB_TABLES = 49  # number of system tables in information_schema
NUM_PERF_DB_TABLES = 6  # number of system tables in performance_schema
NUM_USER_DB_TABLES = 1  # number of user tables in test_meta_sysdb
class TestMetaSysDb2:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def test_meta_sysdb(self):
        """Meta system database

        1. Check table type in information_schema.ins_tables
        2. Check ins_tables count plan optimization
        3. Check ins_columns result correctly with table_name filter
        4. Check table count scan with group by db_name and stable_name
        5. Check table count scan after taosd restart
        6. Check count(distinct ...) function NOT optimized on ins_tables

        Since: v3.3.6

        Labels: common,ci

        Jira: TS-7600

        History:
            - 2025-11-13 Tony Zhang created
            - 2025-11-27 Tony Zhang add check_ins_tables_count and check_count_distinct
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_odbc.py
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_table_count_scan.py

        """
        self.check_ins_tables_count()
        self.check_table_type()
        self.check_count_distinct()
        self.do_odbc()
        self.do_table_count_scan()

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
        tdSql.checkData(0, 0, NUM_INFO_DB_TABLES + NUM_PERF_DB_TABLES + NUM_USER_DB_TABLES) # 48 sys tables + 6 perf tables + 1 user table

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

    #
    # ------------------ test_odbc.py ------------------
    #
    def check_ins_cols(self):
        tdSql.execute("create database if not exists db")
        tdSql.execute("create table db.ntb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 varchar(100), c13 nchar(100))")
        tdSql.execute("create table db.stb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 varchar(100), c13 nchar(100)) tags(t int)")
        tdSql.execute("insert into db.ctb using db.stb tags(1) (ts, c1) values (now, 1)")

        tdSql.execute("select count(*) from information_schema.ins_columns")

        tdSql.query("select * from information_schema.ins_columns where table_name = 'ntb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 2, "NORMAL_TABLE")

        tdSql.query("select * from information_schema.ins_columns where db_name ='db' and table_name = 'stb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 2, "SUPER_TABLE")

        tdSql.query("select db_name,table_type,col_name,col_type,col_length from information_schema.ins_columns where db_name ='db' and table_name = 'ctb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "db")
        tdSql.checkData(1, 1, "CHILD_TABLE")
        tdSql.checkData(3, 2, "c3")
        tdSql.checkData(4, 3, "INT")
        tdSql.checkData(5, 4, 8)

        tdSql.query("desc information_schema.ins_columns")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, "table_name")
        tdSql.checkData(5, 0, "col_length")
        tdSql.checkData(1, 2, 64)
        tdSql.execute("drop database if exists db")

    def do_odbc(self):
        tdSql.prepare()
        self.check_ins_cols()
    
    #
    # ------------------ test_table_count_scan.py ------------------
    #
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use tbl_count")

    def do_table_count_scan(self):
        tdSql.execute("drop database if exists test_meta_sysdb")
        tdSql.execute("drop database if exists empty_db_for_count_test")        
        tdSql.execute("drop database if exists tbl_count")
        tdSql.execute("create database if not exists tbl_count")
        tdSql.execute('use tbl_count')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        sql = "create table "
        sql += " tb1 using stb1 tags(1,'1',1.0)"
        sql += " tb2 using stb1 tags(2,'2',2.0)"
        sql += " tb3 using stb1 tags(3,'3',3.0)"
        tdSql.execute(sql)

        sql = "insert into "
        sql += ' tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' (\'2021-11-11 09:00:01\',true,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)'
        sql += ' (\'2021-11-11 09:00:02\',true,2,NULL,2,NULL,2,NULL,"234",NULL,2,NULL,2,NULL)'
        sql += ' (\'2021-11-11 09:00:03\',false,NULL,3,NULL,3,NULL,3,NULL,"3456",NULL,3,NULL,3)'
        sql += ' (\'2021-11-11 09:00:04\',true,4,4,4,4,4,4,"456","4567",4,4,4,4)'
        sql += ' (\'2021-11-11 09:00:05\',true,127,32767,2147483647,9223372036854775807,3.402823466e+38,1.79769e+308,"567","5678",254,65534,4294967294,9223372036854775807)'
        sql += ' (\'2021-11-11 09:00:06\',true,-127,-32767,-2147483647,-9223372036854775807,-3.402823466e+38,-1.79769e+308,"678","6789",0,0,0,0)'
        sql += ' tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"111","1111",1,1,1,1)'
        sql += ' (\'2021-11-11 09:00:01\',true,2,2,2,2,2,2,"222","2222",2,2,2,2)'
        sql += ' (\'2021-11-11 09:00:02\',true,3,3,2,3,3,3,"333","3333",3,3,3,3)'
        sql += ' (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4)'
        sql += ' (\'2021-11-11 09:00:04\',true,5,5,5,5,5,5,"555","5555",5,5,5,5)'
        sql += ' (\'2021-11-11 09:00:05\',true,6,6,6,6,6,6,"666","6666",6,6,6,6)'
        sql += ' (\'2021-11-11 09:00:06\',true,7,7,7,7,7,7,"777","7777",7,7,7,7)'
        tdSql.execute(sql)

        tdSql.query('select count(*),db_name, stable_name from information_schema.ins_tables group by db_name, stable_name;')
        tdSql.checkRows(3)
        for i in range(0, 3):
            db_name = tdSql.getData(i, 1)
            if db_name == 'information_schema':
                tdSql.checkData(i, 0, NUM_INFO_DB_TABLES)
                tdSql.checkData(i, 2, None)
            elif db_name == 'performance_schema':
                tdSql.checkData(i, 0, 6)
                tdSql.checkData(i, 2, None)
            elif db_name == 'tbl_count':
                tdSql.checkData(i, 0, 3)
                tdSql.checkData(i, 2, 'stb1')
            else:
                raise Exception("unexpected db_name: %s" % db_name)
        
        tdSql.query('select count(1) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v desc;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, NUM_INFO_DB_TABLES)
        tdSql.checkData(0, 1, 'information_schema')
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, 'tbl_count')
        tdSql.checkData(2, 2, 'stb1')

        tdSql.query('select count(1) v,db_name from information_schema.ins_tables group by db_name order by v asc')
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(2, 0, NUM_INFO_DB_TABLES)
        tdSql.checkData(2, 1, 'information_schema')

        tdSql.query("select count(*) from information_schema.ins_tables where db_name='tbl_count'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables where db_name=\'tbl_count\' and stable_name="stb1";')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 58)


        tdSql.execute('create table stba (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tba1 using stba tags(1,'1',1.0);")

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:00\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:01\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:02\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:04\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:05\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:06\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:07\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:08\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:09\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        self.restartTaosd(1, dbname='tbl_count')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:10\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:11\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:12\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:13\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:14\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:15\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:16\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:17\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:18\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:19\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        self.restartTaosd(1, dbname='tbl_count')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:20\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:21\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:22\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:23\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:24\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:25\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:26\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:27\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:28\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:29\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        tdSql.query('select count(*) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(0, 2, 'stba')

        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'tbl_count')
        tdSql.checkData(1, 2, 'stb1')
        tdSql.checkData(2, 0, 6)
        tdSql.checkData(2, 1, 'performance_schema')
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, NUM_INFO_DB_TABLES)
        tdSql.checkData(3, 1, 'information_schema')
        tdSql.checkData(3, 2, None)

        tdSql.query('select count(1) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(0, 2, 'stba')
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'tbl_count')
        tdSql.checkData(1, 2, 'stb1')
        tdSql.checkData(2, 0, 6)
        tdSql.checkData(2, 1, 'performance_schema')
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, NUM_INFO_DB_TABLES)
        tdSql.checkData(3, 1, 'information_schema')
        tdSql.checkData(3, 2, None)

        tdSql.query('select count(1) v,db_name from information_schema.ins_tables group by db_name order by v')
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(2, 0, NUM_INFO_DB_TABLES)
        tdSql.checkData(2, 1, 'information_schema')

        tdSql.query("select count(*) from information_schema.ins_tables where db_name='tbl_count'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query('select count(*) from information_schema.ins_tables where db_name=\'tbl_count\' and stable_name="stb1";')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 59)
        tdSql.execute('drop database tbl_count')