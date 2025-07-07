
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

from new_test_framework.utils import tdLog, tdSql,tdDnodes
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

    def test_ddl_in_sysdb(self):
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
        self.ddl_in_sysdb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.ddl_in_sysdb()

        tdLog.success("%s successfully executed" % __file__)
