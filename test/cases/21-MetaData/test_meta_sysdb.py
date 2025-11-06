
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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdDnodes, tdStream

import os
import time


sysdb_tables = { 
    "information_schema": ["ins_dnodes", "ins_mnodes", "ins_modules", "ins_qnodes", "ins_snodes", "ins_cluster", "ins_databases", "ins_functions", "ins_indexes", "ins_stables", "ins_tables", "ins_tags", "ins_columns", "ins_users", "ins_grants", "ins_vgroups", "ins_configs", "ins_dnode_variables", "ins_topics", "ins_subscriptions", "ins_streams", "ins_streams_tasks", "ins_vnodes", "ins_user_privileges", "undefined"], 
    "performance_schema": ["perf_connections", "perf_queries", "perf_consumers", "perf_trans", "perf_apps", "undefined"]
    }

class TestDdlInSysdb:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))

    def ddl_sysdb(self):
        for db, _ in sysdb_tables.items():
            tdSql.error(f'create database {db}', expectErrInfo="Cannot create system database", fullMatched=False)
            tdSql.error(f'create database {db} vgroups 10', expectErrInfo="Cannot create system database", fullMatched=False)
            tdSql.error(f'alter database {db} wal_level 0', expectErrInfo="Cannot alter system database", fullMatched=False)
            tdSql.error(f'alter database {db} cachemodel \'none\'', expectErrInfo="Cannot alter system database", fullMatched=False)
            tdSql.error(f'drop database {db}', expectErrInfo="Cannot drop system database", fullMatched=False)
            tdSql.error(f'drop database {db}', expectErrInfo="Cannot drop system database", fullMatched=False)    

    def ddl_systb(self):
        tdSql.execute('drop database if exists d0')
        tdSql.execute('create database if not exists d0')
        tdSql.execute('create stable d0.stb0 (ts timestamp, c0 int) tags(t0 int)')
        for db, tbs in sysdb_tables.items():
            tdSql.execute(f'use {db}')
            for tb in tbs:
                tdSql.error(f'create table {tb} (ts timestamp, c0 int)', expectErrInfo="Cannot create table of system database", fullMatched=False)
                tdSql.error(f'create table d0.ctb0 using db.stb0 tags(0) {tb} using {tb} tags(1)', expectErrInfo="Corresponding super table not in this db", fullMatched=False)
                tdSql.error(f'create table {db}.{tb} (ts timestamp, c0 int)', expectErrInfo="Cannot create table of system database", fullMatched=False)
                tdSql.error(f'create table d0.ctb0 using db.stb0 tags(0) {db}.{tb} using {db}.{tb} tags(1)', expectErrInfo="Corresponding super table not in this db", fullMatched=False)
                tdSql.error(f'create stable {tb} (ts timestamp, c0 int) tags(t0 int)', expectErrInfo="Cannot create table of system database", fullMatched=False)
                tdSql.error(f'create stable {db}.{tb} (ts timestamp, c0 int) tags(t0 int)', expectErrInfo="Cannot create table of system database", fullMatched=False)
                tdSql.error(f'alter table {tb} add column c1 int', expectErrInfo="Cannot alter table of system database", fullMatched=False)
                tdSql.error(f'alter table {db}.{tb} add column c1 int', expectErrInfo="Cannot alter table of system database", fullMatched=False)
                tdSql.error(f'alter stable {tb} add column c1 int', expectErrInfo="Cannot alter table of system database", fullMatched=False)
                tdSql.error(f'alter stable {db}.{tb} add column c1 int', expectErrInfo="Cannot alter table of system database", fullMatched=False)
                tdSql.error(f'insert into {tb} values (now,1)', expectErrInfo="System table not allowed", fullMatched=False)
                tdSql.error(f'insert into {db}.{tb} values (now,1)', expectErrInfo="System table not allowed", fullMatched=False)
                tdSql.error(f'insert into {tb} values (now,1) using stb tags(0) values(now,1)', expectErrInfo="System table not allowed", fullMatched=False)
                tdSql.error(f'insert into {db}.{tb} values (now,1) using stb tags(0) values(now,1)', expectErrInfo="System table not allowed", fullMatched=False)
                tdSql.error(f'delete from {tb}', expectErrInfo="Cannot delete from system database", fullMatched=False)
                tdSql.error(f'delete from {db}.{tb}', expectErrInfo="Cannot delete from system database", fullMatched=False)
                tdSql.error(f'delete from {tb} where ts >= 0', expectErrInfo="Cannot delete from system database", fullMatched=False)
                tdSql.error(f'delete from {db}.{tb} where ts >= 0', expectErrInfo="Cannot delete from system database", fullMatched=False)
                tdSql.error(f'drop table {tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop table {db}.{tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop stable {tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop stable {db}.{tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop table with {tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop table with {db}.{tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop stable with {tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
                tdSql.error(f'drop stable with {db}.{tb}', expectErrInfo="Cannot drop table of system database", fullMatched=False)
        tdSql.execute('drop database if exists d0')

    def ddl_in_sysdb(self):
        self.ddl_sysdb()
        self.ddl_systb()

    def do_ddl_in_sysdb(self):
        """Meta system db same name check

        1. Create/Drop Database with same name as system database
        2. Restart taosd service
        3. Create/Drop Table with same name as system table

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_ddl_in_sysdb.py

        """

        self.ddl_in_sysdb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.ddl_in_sysdb()

        tdLog.success("%s successfully executed" % __file__)
        
    #
    # --------------- test_ins_stables.py ---------------
    #
    def do_func_sys_tbname(self):
        """Meta ins_stables

        1. Creating databases with super/child/normal tables for metadata testing
        2. Executing comprehensive queries on information_schema tables (ins_databases/ins_stables/ins_tables)
        3. Testing large-scale stable table creation (70+ stables per database)
        4. Verifying partition and limit operations on system tables
        5. Checking table counting and distinct value operations

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/sys_tbname.sim

        """

        tdSql.execute(f"create database sys_tbname;")
        tdSql.execute(f"use sys_tbname;")
        tdSql.execute(f"create stable st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create table ct1 using st tags(1);")
        tdSql.execute(f"create table ct2 using st tags(2);")

        tdSql.execute(f"create table t (ts timestamp, f int);")
        tdSql.execute(f"insert into t values(now, 1)(now+1s, 2);")

        tdSql.execute(f"create table t2 (ts timestamp, f1 int, f2 int);")
        tdSql.execute(f"insert into t2 values(now, 0, 0)(now+1s, 1, 1);")

        tdSql.query(f"select tbname from information_schema.ins_databases;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)}")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "ins_databases")

        tdSql.query(f"select distinct tbname from information_schema.ins_databases;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ins_databases")

        tdSql.query(f"select tbname from information_schema.ins_stables;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ins_stables")

        tdSql.query(f"select distinct tbname from information_schema.ins_stables;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ins_stables")

        tdSql.query(f"select * from information_schema.ins_tables where table_name='';")
        tdSql.checkRows(0)

        tdSql.query(f"select table_name from information_schema.ins_tables where db_name = 'information_schema' order by table_name")
        tdSql.checkRows(47)
        tdSql.checkData(0, 0, "ins_anodes")

        tdSql.query(f"select table_name from information_schema.ins_tables where db_name = 'performance_schema' order by table_name")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "perf_apps")
        

        tdSql.query(f"select distinct tbname from information_schema.ins_tables;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ins_tables")

        tdSql.query(f"select tbname,table_name from information_schema.ins_tags;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "ins_tags")

        tdSql.query(f"select distinct tbname from information_schema.ins_tags;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ins_tags")

        tdSql.execute(
            f"create stable stb(ts timestamp, f int) tags(t1 int, t2 int, t3 int, t4 int, t5 int);"
        )

        i = 0
        tbNum = 1000
        tbPrefix = "stb_tb"
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"create table {tb} using stb tags( {i} , {i} , {i} , {i} , {i} )"
            )
            i = i + 1

        tdSql.query(
            f"select tag_value from information_schema.ins_tags where stable_name='stb';"
        )
        tdSql.checkRows(5000)

        tdSql.execute(f"create database d1;")
        tdSql.execute(f"create stable d1.st1 (ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create stable d1.st2 (ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create table d1.ct1 using d1.st1 tags(1);")
        tdSql.execute(f"create table d1.ct2 using d1.st2 tags(2);")

        tdSql.execute(f"drop database d1;")
        tdSql.execute(f"create database d2;")
        tdSql.execute(f"create stable d2.st1(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create stable d2.st2(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create table d2.ct1 using d2.st1 tags(1);")
        tdSql.execute(f"create table d2.ct2 using d2.st2 tags(2);")

        tdSql.execute(f"drop database d2;")
        tdSql.execute(f"create database d3;")
        tdSql.execute(f"create stable d3.st1(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create stable d3.st2(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"create table d3.ct1 using d3.st1 tags(1);")
        tdSql.execute(f"create table d3.ct2 using d3.st2 tags(2);")
        tdSql.query(
            f"select count(*), stable_name, db_name from information_schema.ins_tables where db_name != 'd2' group by stable_name,db_name"
        )

        tdLog.info(f"=========================== td-24781")
        tdSql.query(
            f"select DISTINCT (`precision`) from `information_schema`.`ins_databases` PARTITION BY `precision`"
        )

        tdLog.info(f"=========================ins_stables")

        tdSql.execute(f"drop database d3;")
        tdLog.info(f"create database test vgroups 4;")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st1(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st2(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st3(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st4(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st5(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st6(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st7(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st8(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st9(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st10(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st11(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st12(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st13(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st14(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st15(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st16(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st17(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st18(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st19(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st20(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st21(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st22(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st23(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st24(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st25(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st26(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st27(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st28(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st29(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st30(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st31(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st32(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st33(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st34(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st35(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st36(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st37(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st38(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st39(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st40(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st41(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st42(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st43(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st44(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st45(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st46(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st47(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st48(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st49(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st50(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st51(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st52(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st53(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st54(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st55(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st56(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st57(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st58(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st59(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st60(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st61(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st62(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st63(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st64(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st65(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st66(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st67(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st68(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st69(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st70(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )

        tdSql.execute(f"drop database test;")
        tdLog.info(f"create database test1 vgroups 4;")
        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st1(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st2(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st3(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st4(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st5(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st6(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st7(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st8(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st9(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st10(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st11(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st12(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st13(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st14(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st15(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st16(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st17(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st18(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st19(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st20(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st21(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st22(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st23(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st24(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st25(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st26(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st27(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st28(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st29(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st30(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st31(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st32(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st33(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st34(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st35(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st36(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st37(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st38(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st39(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st40(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st41(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st42(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st43(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st44(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st45(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st46(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st47(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st48(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st49(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st50(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st51(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st52(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st53(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st54(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st55(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st56(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st57(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st58(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st59(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st60(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st61(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st62(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st63(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st64(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st65(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st66(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st67(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st68(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st69(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stable st70(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )

        tdSql.query(
            f'select * from information_schema.ins_stables where db_name = "test1" limit 68,32;'
        )

        tdSql.checkRows(2)

        tdSql.query(
            f'select * from information_schema.ins_stables where db_name = "test1" limit 68,32;'
        )

        tdSql.checkRows(2)
        
        tdSql.execute(f"drop database sys_tbname")
        tdSql.execute(f"drop database test1")        
    
    #
    # --------------- test_ins_tables.py ---------------
    #    
    def do_table_count(self):
        self.TableCount()
        tdStream.dropAllStreamsAndDbs()
        self.MetricsIdx()
        tdStream.dropAllStreamsAndDbs()

    def TableCount(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.error(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tba3 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba4 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba5 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba6 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba7 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba8 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tbb1 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb2 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb3 using stb tags(6, 6, 6);")
        tdSql.execute(f"create table tbb4 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb5 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb6 using stb tags(6, 6, 6);")
        tdSql.execute(f"create table tbb7 using stb tags(7, 7, 7);")
        tdSql.execute(f"create table tbb8 using stb tags(8, 8, 8);")
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")
        tdSql.execute(f"create database db2 vgroups 3;")
        tdSql.error(f"create database db2;")
        tdSql.execute(f"use db2;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tbb1 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb2 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb3 using stb tags(6, 6, 6);")

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by stable_name;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by db_name;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by db_name, stable_name;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select stable_name,count(table_name) from information_schema.ins_tables group by stable_name order by stable_name;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 53)

        tdSql.checkData(1, 1, 10)

        tdSql.checkData(2, 1, 11)

        tdSql.query(
            f"select db_name,count(table_name) from information_schema.ins_tables group by db_name order by db_name;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 17)

        tdSql.checkData(1, 1, 5)

        tdSql.checkData(2, 1, 47)

        tdSql.checkData(3, 1, 5)

        tdSql.query(
            f"select db_name,stable_name,count(table_name) from information_schema.ins_tables group by db_name, stable_name order by db_name, stable_name;"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 2, 8)

        tdSql.checkData(2, 2, 8)

        tdSql.checkData(3, 2, 2)

        tdSql.checkData(4, 2, 3)

        tdSql.checkData(5, 2, 47)

        tdSql.checkData(6, 2, 5)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables where db_name='db1' and stable_name='sta' group by stable_name"
        )
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 8)

        tdSql.query(f"select distinct db_name from information_schema.ins_tables;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)

    def MetricsIdx(self):
        dbPrefix = "m_me_db_idx"
        tbPrefix = "m_me_tb_idx"
        mtPrefix = "m_me_mt_idx"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix
        mt = mtPrefix

        tdSql.execute(f"drop database  if exists {db}")
        tdSql.execute(f"create database {db}")

        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")

        tbNum = 10000

        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} using {mt} tags( {i} ) values(now,  {i} )")
            i = i + 1

        tdSql.execute(f"use information_schema")
        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time > now() - 10m'
        )
        tdSql.checkData(0, 0, tbNum)

        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time < now();'
        )
        tdSql.checkData(0, 0, tbNum)

        tdSql.execute(f"use {db}")
        doubletbNum = 20000

        while i < doubletbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} using {mt} tags( {i} ) values(now,  {i} )")
            i = i + 1

        tdSql.execute(f"use information_schema")

        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time < now();'
        )
        tdSql.checkData(0, 0, doubletbNum)

        print("do table count ........................ [passed]")

    #
    # ------------------- test_ins_filesets.py ----------------
    #
    def do_ins_filesets(self):
        tdSql.execute('create database db vgroups 1')
        tdSql.execute('use db')
        tdSql.execute('create table t1 (ts timestamp, a int, b int)')
        tdSql.execute('insert into t1 values(\'2024-12-27 14:00:00\', 1, 2)')
        tdSql.execute('flush database db')

        tdLog.sleep(5)

        rows = tdSql.query('select * from information_schema.ins_filesets')
        tdSql.checkRows(1)
        tdSql.checkEqual(tdSql.getData(0, 0), 'db')
        tdSql.checkEqual(tdSql.getData(0, 1), 2)
        tdSql.checkEqual(tdSql.getData(0, 2), 2008)
        # tdSql.CheckEqual(str(tdSql.getData(0, 3)), '2024-12-23 08:00:00.000')
        # tdSql.CheckEqual(str(tdSql.getData(0, 4)), '2025-01-02 07:59:59.999')
        # tdSql.CheckEqual(tdSql.getData(0, 6), '1970-01-01 08:00:00.000')
        # tdSql.CheckEqual(tdSql.getData(0, 7), False)

        #tdDnodes.stopAll()
        tdSql.execute("drop database db")
        print("do ins filesets ....................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_meta_sysdb(self):
        """Meta system database

        1. Creating databases with super/child/normal tables for metadata testing
        2. Executing comprehensive queries on information_schema tables (ins_databases/ins_stables/ins_tables)
        3. Testing large-scale stable table creation (70+ stables per database)
        4. Verifying partition and limit operations on system tables
        5. Checking table counting and distinct value operations
        6. Create/Drop Database with same name as system database
        7. Restart taosd service
        8. Create/Drop Table with same name as system table
        9. Check information_schema.ins_tables
        10. Check information_schema.ins_stables
        11. Check information_schema.ins_tags
        12. Check information_schema.ins_databases
        13. Check information_schema.ins_streams
        14. Check information_schema.ins_users
        15. Check information_schema.ins_user_privileges
        16. Check information_schema.ins_filesets
        17. Check information_schema.ins_vgroups

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/tableCount.sim
            - 2025-5-9 Simon Guan Migrated from tsim/stable/metrics_idx.sim
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_ins_filesets.py

        """
        self.do_ins_filesets()
        self.do_table_count()
        self.do_func_sys_tbname()
        self.do_ddl_in_sysdb()
