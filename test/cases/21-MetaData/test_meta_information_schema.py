
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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdDnodes, tdStream, tdCom, TDSetSql

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
        print("\ndo ins filesets ....................... [passed]")

    #
    # ------------------- test_information_schema ----------------
    #
    def init_class(self):
        tdLog.debug("start to execute %s" % __file__)
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.stbname = 'stb'
        self.binary_length = 20 # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.ts = 1537146000000
        self.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tbnum = 21
        self.rowNum = 10
        self.tag_dict = {
            't0':'int',
            't1':f'nchar({self.nchar_length})'
        }
        self.tag_values = [
            f'1', '""'
            ]
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.ins_list = ['ins_dnodes','ins_mnodes','ins_qnodes','ins_snodes','ins_bnodes','ins_cluster','ins_databases','ins_functions',\
            'ins_indexes','ins_stables','ins_tables','ins_tags','ins_columns','ins_virtual_child_columns', 'ins_users','ins_grants','ins_vgroups','ins_configs','ins_dnode_variables',\
                'ins_topics','ins_subscriptions','ins_streams','ins_stream_tasks','ins_vnodes','ins_user_privileges','ins_views',
                'ins_compacts', 'ins_compact_details', 'ins_grants_full','ins_grants_logs', 'ins_machines', 'ins_arbgroups', 'ins_tsmas', "ins_encryptions", "ins_anodes",
                        "ins_anodes_full", "ins_disk_usagea", "ins_filesets", "ins_transaction_details", "ins_mounts", "ins_stream_recalculates", "ins_ssmigrates", 'ins_scans', 'ins_scan_details', 'ins_rsmas', 'ins_retentions', 'ins_retention_details' ]
        self.perf_list = ['perf_connections', 'perf_queries',
                         'perf_consumers',  'perf_trans', 'perf_apps']

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]}, {self.tag_values[1]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
    def count_check(self):
        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum+len(self.ins_list)+len(self.perf_list))
        tdSql.query(f'select count(*) from information_schema.ins_tables where db_name = "{self.dbname}"')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.query(f'select count(*) from information_schema.ins_tables where db_name = "{self.dbname}" and stable_name = "{self.stbname}"')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.execute('create database db1')
        tdSql.execute('create table stb1 (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute('create table tb1 using stb1 tags(1)')
        tdSql.query(f'select db_name, stable_name, count(*) from information_schema.ins_tables group by db_name, stable_name')
        for i in tdSql.queryResult:
            if i[0].lower() == 'information_schema':
                tdSql.checkEqual(i[2],len(self.ins_list))
            elif i[0].lower() == self.dbname and i[1] == self.stbname:
                tdSql.checkEqual(i[2],self.tbnum)
            elif i[0].lower() == self.dbname and i[1] == 'stb1':
                tdSql.checkEqual(i[2],1)
            elif i[0].lower() == 'performance_schema':
                tdSql.checkEqual(i[2],len(self.perf_list))
        tdSql.execute('create table db1.ntb (ts timestamp,c0 int)')
        tdSql.query(f'select db_name, count(*) from information_schema.ins_tables group by db_name')
        for i in tdSql.queryResult:
            if i[0].lower() == 'information_schema':
                tdSql.checkEqual(i[1],len(self.ins_list))
            elif i[0].lower() == 'performance_schema':
                tdSql.checkEqual(i[1],len(self.perf_list))
            elif i[0].lower() == self.dbname:
                tdSql.checkEqual(i[1],self.tbnum+1)

    def ins_col_check_4096(self):
        tdSql.execute('create database db3 vgroups 2 replica 1')
        col_str = tdCom.gen_tag_col_str("col", "int",4094)
        tdSql.execute(f'create stable if not exists db3.stb (col_ts timestamp, {col_str}) tags (t1 int)')
        for i in range(100):
            tdLog.info(f"create table db3.ctb{i} using db3.stb tags({i})")
            tdSql.execute(f"create table db3.ctb{i} using db3.stb tags({i})")
            col_value_str = '1, ' * 4093 + '1'
            tdSql.execute(f"insert into db3.ctb{i} values(now,{col_value_str})(now+1s,{col_value_str})(now+2s,{col_value_str})(now+3s,{col_value_str})")
        tdSql.query("select * from information_schema.ins_columns")

        tdSql.execute('drop database db3')
    def ins_stable_check(self):
        tdSql.execute('create database db3 vgroups 2 replica 1')
        tbnum = 10
        ctbnum = 10
        for i in range(tbnum):
            tdSql.execute(f'create stable db3.stb_{i} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table db3.ntb_{i} (ts timestamp,c0 int)')
            for j in range(ctbnum):
                tdSql.execute(f"create table db3.ctb_{i}_{j} using db3.stb_{i} tags({j})")
        tdSql.query("select stable_name,count(table_name) from information_schema.ins_tables where db_name = 'db3' group by stable_name order by stable_name")
        result = tdSql.queryResult
        for i in range(len(result)):
            if result[i][0] == None:
                tdSql.checkEqual(result[0][1],tbnum)
            else:
                tdSql.checkEqual(result[i][0],f'stb_{i-1}')
                tdSql.checkEqual(result[i][1],ctbnum)

    def ins_stable_check2(self):
        tdSql.execute('drop database if exists restful_test')
        tdSql.execute('drop database if exists log')
        tdSql.execute('drop database if exists d0')
        tdSql.execute('drop database if exists d1')
        tdSql.execute('create database restful_test vgroups 4 replica 1')
        tdSql.execute('create database log vgroups 2 replica 1')
        tdSql.execute('create database d0 vgroups 4 replica 1')
        tdSql.execute('create database d1 vgroups 4 replica 1')
        log_stb_num = 5
        rest_stb_num = 51
        for i in range(rest_stb_num):
            tdSql.execute(f'create stable restful_test._stb_{i} (ts timestamp,c0 int) tags(t0 int);')
            tdSql.execute(f'create stable d0._stb_{i} (ts timestamp,c0 int, c1 int) tags(t0 int,t1 int);')
            tdSql.execute(f'create stable d1._stb_{i} (ts timestamp,c0 int, c1 int, c2 int) tags(t0 int,t1 int, t2 int);')

        tdSql.execute(f'CREATE STABLE log.`taosadapter_restful_http_request_summary_milliseconds` (`_ts` TIMESTAMP, `sum` DOUBLE) TAGS (`request_uri` NCHAR(128));')
        tdSql.execute(f'CREATE STABLE log.`taosadapter_system_cpu_percent` (`_ts` TIMESTAMP, `gauge` DOUBLE) TAGS (`endpoint` NCHAR(45));')
        tdSql.execute(f'CREATE STABLE log.`taosadapter_restful_http_request_total` (`_ts` TIMESTAMP, `gauge` DOUBLE) TAGS (`client_ip` NCHAR(40));')
        tdSql.execute(f'CREATE STABLE log.`taosadapter_system_mem_percent` (`_ts` TIMESTAMP, `gauge` DOUBLE) TAGS (`endpoint` NCHAR(45));')
        tdSql.execute(f'CREATE STABLE log.`taosadapter_restful_http_request_fail` (`_ts` TIMESTAMP, `gauge` DOUBLE) TAGS (`request_uri` NCHAR(128), `status_code` NCHAR(4));')

        tdSql.query(f'select * from information_schema.ins_stables where db_name="restful_test" limit 0,25;') # condition 1
        result = tdSql.queryResult
        tdSql.checkEqual(len(result),25)
        for i in range(len(result)):
            tdSql.checkEqual(result[i][0][0:5],f'_stb_') # stable_name
            tdSql.checkEqual(result[i][1],f'restful_test') # db_name
            tdSql.checkEqual(result[i][5]>=result[i][2],True) # last_update >= create_time
            tdSql.checkEqual(result[i][3]>1,True) # columns
            tdSql.checkEqual(result[i][4]>0,True) # tags
            tdSql.checkEqual(result[i][6],None) # table_comment
            tdSql.checkEqual(result[i][7],f'5000a,5000a') # watermark
            tdSql.checkEqual(result[i][8],f'-1a,-1a') # max_delay
            tdSql.checkEqual(result[i][9],f'') # rollup

        tdSql.query(f'select create_time from information_schema.ins_stables where db_name="restful_test" order by create_time asc limit 10,1')
        result = tdSql.queryResult
        tdSql.checkEqual(len(result),1)
        _create_time=result[0][0]
        tdSql.query("select * from information_schema.ins_stables where db_name='restful_test' and create_time > '%s' limit 10,30" % (_create_time)) # condition 2
        result = tdSql.queryResult
        tdSql.checkEqual(len(result),30)
        for i in range(len(result)):
            tdSql.checkEqual(result[i][0][0:5],f'_stb_') # stable_name
            tdSql.checkEqual(result[i][1],f'restful_test') # db_name
            tdSql.checkEqual(result[i][5]>=result[i][2],True) # last_update >= create_time
            tdSql.checkEqual(result[i][2]>_create_time,True) # create_time
            tdSql.checkEqual(result[i][3]>1,True) # columns
            tdSql.checkEqual(result[i][4]>0,True) # tags
            tdSql.checkEqual(result[i][6],None) # table_comment
            tdSql.checkEqual(result[i][7],f'5000a,5000a') # watermark
            tdSql.checkEqual(result[i][8],f'-1a,-1a') # max_delay
            tdSql.checkEqual(result[i][9],f'') # rollup

    def ins_columns_check(self):
        tdSql.execute('drop database if exists db2')
        tdSql.execute('create database if not exists db2 vgroups 1 replica 1')
        for i in range (5):
            self.stb4096 = 'create table db2.stb%d (ts timestamp' % (i)
            for j in range (4094 - i):
                self.stb4096 += ', c%d int' % (j)
            self.stb4096 += ') tags (t1 int)'
            tdSql.execute(self.stb4096)
            for k in range(10):
                tdSql.execute("create table db2.ctb_%d_%dc using db2.stb%d tags(%d)" %(i,k,i,k))
        for t in range (2):
            tdSql.query(f'select * from information_schema.ins_columns where db_name="db2" and table_type=="SUPER_TABLE"')
            tdSql.checkEqual(20465,len(tdSql.queryResult))
        for t in range (2):
            tdSql.query(f'select * from information_schema.ins_columns where db_name="db2" and table_type=="CHILD_TABLE"')
            tdSql.checkEqual(204650,len(tdSql.queryResult))

        for i in range (5):
            self.ntb4096 = 'create table db2.ntb%d (ts timestamp' % (i)
            for j in range (4095 - i):
                self.ntb4096 += ', c%d binary(10)' % (j)
            self.ntb4096 += ')'
            tdSql.execute(self.ntb4096)
        for t in range (2):
            tdSql.query(f'select * from information_schema.ins_columns where db_name="db2" and table_type=="NORMAL_TABLE"')
            tdSql.checkEqual(20470,len(tdSql.queryResult))

        tdSql.query("select * from information_schema.ins_columns where db_name ='information_schema'")
        
        tdSql.query("select * from information_schema.ins_columns where db_name ='performance_schema'")
        tdSql.checkRows(64)

    def ins_dnodes_check(self):
        tdSql.execute('drop database if exists db2')
        tdSql.execute('create database if not exists db2 vgroups 1 replica 1')
        tdSql.query(f'select * from information_schema.ins_dnodes')
        result = tdSql.queryResult
        tdSql.checkEqual(result[0][0],1)
        tdSql.checkEqual(True, len(result[0][8]) in (0,24))
        self.str107 = 'Hc7VCc+'
        for t in range (10):
            self.str107 += 'tP+2soIXpP'
        self.str108 = self.str107 + '='
        self.str109 = self.str108 + '+'
        self.str254 = self.str108 + self.str108 + '01234567890123456789012345678901234567'
        self.str255 = self.str254 + '='
        self.str256 = self.str254 + '=('
        self.str257 = self.str254 + '=()'
        self.str510 = self.str255 + self.str255
        tdSql.error('alter dnode 1 "activeCode" "a"')
        tdSql.error('alter dnode 1 "activeCode" "' + self.str107 + '"')
        tdSql.error('alter all dnodes "activeCode" "' + self.str108 + '"')
        tdSql.error('alter dnode 1 "activeCode" "' + self.str109 + '"')
        tdSql.error('alter all dnodes "activeCode" "' + self.str510 + '"')
        tdSql.query(f'select * from information_schema.ins_dnodes')
        tdSql.checkEqual(True, len(result[0][8]) in (0,24))
        tdSql.error('alter dnode 1 "activeCode" ""')
        tdSql.error(f'select active_code,c_active_code from information_schema.ins_dnodes')
        tdSql.error('alter dnode 1 "cActiveCode" "a"')
        tdSql.error('alter dnode 1 "cActiveCode" "' + self.str107 + '"')
        tdSql.error('alter dnode 1 "cActiveCode" "' + self.str256 + '"')
        tdSql.error('alter all dnodes "cActiveCode" "' + self.str255 + '"')
        tdSql.error('alter all dnodes "cActiveCode" "' + self.str256 + '"')
        tdSql.error('alter all dnodes "cActiveCode" "' + self.str257 + '"')
        tdSql.error('alter all dnodes "cActiveCode" "' + self.str254 + '"')
        tdSql.error('alter dnode 1 "cActiveCode" "' + self.str510 + '"')
        tdSql.error(f'select active_code,c_active_code from information_schema.ins_dnodes')
        tdSql.error('alter dnode 1 "cActiveCode" "' + self.str109 + '"')
        tdSql.query(f'show dnodes')
        tdSql.error(f'select c_active_code from information_schema.ins_dnodes')
        tdSql.error('alter all dnodes "cActiveCode" ""')

    def ins_grants_check(self):
        grant_name_dict = {
            'service':'Service Time',
            'timeseries':'Timeseries',
            'dnodes':'Dnodes',
            'cpu_cores':'CPU Cores',
            'vnodes':'Vnodes',
            'storage_size':'Storage Size',
            'stream':'Stream',
            'subscription':'Subscription',
            'view':'View',
            'audit':'Audit',
            'storage':'Multi-Tier Storage',
            'backup_restore':'Data Backup & Restore',
            'object_storage':'Shared Storage',
            'active_active':'Active-Active',
            'dual_replica':'Dual-Replica HA',
            'db_encryption':'Database Encryption',
            'tdgpt':'TDgpt',
            'opc_da':'OPC_DA',
            'opc_ua':'OPC_UA',
            'pi':'Pi',
            'kafka':'Kafka',
            'influxdb':'InfluxDB',
            'mqtt':'MQTT',
            'avevahistorian':'avevaHistorian',
            'opentsdb':'OpenTSDB',
            'td2.6':'TDengine2.6',
            'td3.0':'TDengine3.0',
            'mysql':'MySQL',
            'postgres':'PostgreSQL',
            'oracle':'Oracle',
            'mssql':'SqlServer',
            'mongodb':'MongoDB',
            'csv':'CSV',
            'sparkplugb':"SparkplugB",
            'orc':'ORC',
            'kinghist':'KingHistorian',
            'idmp_ts_attr':'TDengine IDMP Time-Series Attributes',
            'idmp_nts_attr':'TDengine IDMP Non-Time-Series Attributes',
            'idmp_element':'TDengine IDMP Elements',
            'idmp_server':'TDengine IDMP Servers',
            'idmp_cpu_core':'TDengine IDMP CPU Cores',
            'idmp_user':'TDengine IDMP Users',
            'idmp_version_ctrl':'TDengine IDMP Version Control',
            'idmp_data_forecast':'TDengine IDMP Forecasting',
            'idmp_data_detect':'TDengine IDMP Anomaly Detection',
            'idmp_data_quality':'TDengine IDMP Data Quality',
            'idmp_ai_chat_gen':'TDengine IDMP AI Chat/Generate',
        }

        tdSql.execute('drop database if exists db2')
        tdSql.execute('create database if not exists db2 vgroups 1 replica 1')
        tdSql.query(f'select * from information_schema.ins_grants_full')
        result = tdSql.queryResult
        index = 0
        for i in range(0, len(result)):
            if result[i][0] in grant_name_dict:
                tdSql.checkEqual(result[i][1], grant_name_dict[result[i][0]])
                index += 1
        tdSql.checkEqual(index, len(grant_name_dict))
        tdSql.query(f'select * from information_schema.ins_grants_logs')
        result = tdSql.queryResult
        tdSql.checkEqual(True, len(result) >= 0)
        if(len(result) > 0):
            tdSql.checkEqual(True, result[0][0].find(",init,ungranted,ungranted") >= 16)
            tdSql.checkEqual(True, len(result[0][1]) == 0)
            tdSql.checkEqual(True, len(result[0][2]) >= 46)

        tdSql.query(f'select * from information_schema.ins_machines')
        tdSql.checkRows(1)
        tdSql.execute('alter cluster "activeCode" "revoked"')
        tdSql.execute('alter cluster "activeCode" "revoked"')
        tdSql.error('alter cluster "activeCode" ""')
        tdSql.error('alter cluster "activeCode" "abc"')
        tdSql.error('alter cluster "activeCode" ""')
        tdSql.execute('alter cluster "activeCode" "revoked"')

    def ins_encryptions_check(self):
        key_status_list = ['unknown', 'unset', 'set', 'loaded']

        # unset/none
        tdSql.execute('drop database if exists db2')
        tdSql.execute('create database if not exists db2 vgroups 1 replica 1')
        time.sleep(2)
        tdSql.query(f'select * from information_schema.ins_encryptions')
        result = tdSql.queryResult
        index = 0
        for i in range(0, len(result)):
            tdSql.checkEqual(True, result[i][1] in key_status_list[1])
            index += 1
        tdSql.checkEqual(True, index > 0)

        tdSql.query(f'show encryptions')
        result = tdSql.queryResult
        index = 0
        for i in range(0, len(result)):
            tdSql.checkEqual(True, result[i][1] in key_status_list[1])
            index += 1
        tdSql.checkEqual(True, index > 0)

        # loaded/sm4
        tdSql.execute('drop database if exists db2')
        tdSql.execute('create encrypt_key \'12345678\'')
        time.sleep(3)
        tdSql.execute('create database if not exists db2 vgroups 1 replica 1 encrypt_algorithm \'sm4\'')
        tdSql.query(f'select * from information_schema.ins_encryptions')
        result = tdSql.queryResult
        index = 0
        for i in range(0, len(result)):
            tdSql.checkEqual(True, result[i][1] in key_status_list[3])
            index += 1
        tdSql.checkEqual(True, index > 0)

        tdSql.query(f'show encryptions')
        result = tdSql.queryResult
        index = 0
        for i in range(0, len(result)):
            tdSql.checkEqual(True, result[i][1] in key_status_list[3])
            index += 1
        tdSql.checkEqual(True, index > 0)

    def run_query_ins_tags(self):
        sql = f'select tag_name, tag_value from information_schema.ins_tags where table_name = "{self.stbname}_0"'
        tdSql.query(sql)
        tdSql.checkRows(2)

    def do_information_schema(self):
        self.init_class()
        self.prepare_data()
        self.count_check()
        self.ins_columns_check()
        # self.ins_col_check_4096()
        self.ins_stable_check()
        self.ins_stable_check2()
        self.ins_dnodes_check()
        self.ins_grants_check()
        self.ins_encryptions_check()
        self.run_query_ins_tags()

        print("do information schema ................. [passed]")

    #
    # ------------------- test_systable_func.py ----------------
    #
    def do_systable_func(self):
        tdSql.prepare()

        tdSql.query(f"select count(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select sum(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select min(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select max(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select stddev(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select avg(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select apercentile(`columns`, 50) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select top(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select bottom(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select spread(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select histogram(`columns`, 'user_input', '[1, 3, 5]', 0) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select hyperloglog(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select sample(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select mode(`columns`) from `information_schema`.`ins_tables`;")

        tdSql.error(f"select unique(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select tail(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select leastsquares(`columns`, 1, 1) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select elapsed(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select interp(`columns`) from `information_schema`.`ins_tables` range(0, 1) every(1s) fill(null);")
        tdSql.error(f"select percentile(`columns`, 50) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select derivative(`columns`, 1s, 0) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select irate(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select last_row(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select last(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select first(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select twa(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select diff(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select statecount(`columns`, 'GE', 0) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select stateduration(`columns`, 'GE', 0, 1s) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select csum(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select mavg(`columns`, 1) from `information_schema`.`ins_tables`;")
        
        print("do systable fun ....................... [passed]")


    #
    # ------------------- main ----------------
    #
    def test_meta_sysdb(self):
        """Meta information_schema

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
        18. Check information_schema.ins_dnodes
        19. Check information_schema.ins_mnodes
        20. Check information_schema.ins_snodes
        21. Check information_schema.ins_qnodes
        22. Check information_schema.ins_bnodes
        23. Check information_schema.ins_cluster
        24. Check information_schema.ins_functions
        25. Check information_schema.ins_indexes
        26. Check information_schema.ins_tags
        27. Check information_schema.ins_columns
        28. Check information_schema.ins_virtual_child_columns
        29. Check information_schema.ins_views
        30. Check information_schema.ins_grants
        31. Check information_schema.ins_configs
        32. Check information_schema.ins_dnode_variables
        33. Check information_schema.ins_stream_tasks
        34. Check information_schema.ins_transaction_details
        35. Check information_schema.ins_mounts
        36. Check information_schema.ins_stream_recalculates
        37. Check information_schema.ins_ssmigrates
        38. Check information_schema.ins_scans
        39. Check information_schema.ins_scan_details
        40. Check information_schema.ins_rsmas
        41. Check information_schema.ins_retentions
        42. Check information_schema.ins_retention_details
        43. Check table counting and distinct value operations again after all above tests
        44. Check functions on information_schema tables
            - count/sum/min/max/stddev/avg/apercentile/
            - top/bottom/spread/histogram/hyperloglog/sample/mode
        45. Check unsupported functions on information_schema tables
            - unique/tail/leastSquares/elapsed/interp/percentile/
            - derivative/irate/last_row/last/first/twa/diff/
            - statecount/stateduration/csum/mavg

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/tableCount.sim
            - 2025-5-9 Simon Guan Migrated from tsim/stable/metrics_idx.sim
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_ins_filesets.py
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_information_schema.py
            - 2025-12-21 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_systable_func.py

        """
        self.do_ins_filesets()
        self.do_table_count()
        self.do_func_sys_tbname()
        self.do_ddl_in_sysdb()
        self.do_information_schema()
        self.do_systable_func()
