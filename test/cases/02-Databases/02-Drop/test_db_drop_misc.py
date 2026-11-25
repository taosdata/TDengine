# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDropMisc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")


    def test_database_repeat(self):
        """Drop db many repeatly

        1. Create database
        2. Create table
        3. Drop both
        4. Repeat several times

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/repeat.sim
            - 2026-11-20 Alex Merged into 02-Drop/test_db_drop_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"============================ dnode1 start")

        tdSql.execute(f"create database d1")
        tdSql.execute(f"create table d1.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d2.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d3")
        tdSql.execute(f"create table d3.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d4")
        tdSql.execute(f"create table d4.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")
        tdSql.execute(f"drop database d3")
        tdSql.execute(f"drop database d4")

        tdSql.execute(f"create database d5")
        tdSql.execute(f"create table d5.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d6")
        tdSql.execute(f"create table d6.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d7")
        tdSql.execute(f"create table d7.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d8")
        tdSql.execute(f"create table d8.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d5")
        tdSql.execute(f"drop database d6")
        tdSql.execute(f"drop database d7")
        tdSql.execute(f"drop database d8")

        tdSql.execute(f"create database d9;")
        tdSql.execute(f"create table d9.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d10;")
        tdSql.execute(f"create table d10.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d11")
        tdSql.execute(f"create table d11.t1(ts timestamp, i int)")

        tdSql.execute(f"create database d12")
        tdSql.execute(f"create table d12.t1(ts timestamp, i int)")

        tdSql.execute(f"drop database d9")
        tdSql.execute(f"drop database d10")
        tdSql.execute(f"drop database d11")
        tdSql.execute(f"drop database d12")


    def test_database_catalog(self):
        """Drop db basic

        1. Drop database with if exists
        2. Drop empty database
        3. Drop database with only meta
        4. Drop database with super/child/normal tables
        5. Loop drop and create database for many times
        6. Verify drop database on information_schema.ins_databases


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/catalog/alterInCurrent.sim
            - 2026-11-20 Alex Merged into 02-Drop/test_db_drop_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== drop column in normal table")
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table t1 (ts timestamp, f1 int, f2 int);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1, 2);")
        tdSql.execute(f"alter table t1 drop column f2;")
        tdSql.execute(f"insert into t1 values (1591060628001, 2);")

        tdLog.info(f"======== add column in normal table")
        tdSql.execute(f"drop database db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table t1 (ts timestamp, f1 int);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1);")
        tdSql.execute(f"alter table t1 add column f2 int;")
        tdSql.execute(f"insert into t1 values (1591060628001, 2, 2);")

        tdLog.info(f"======== drop column in super table")
        tdSql.execute(f"drop database db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 int) tags (t1 int);"
        )
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1, 2);")
        tdSql.execute(f"alter table st1 drop column f2;")
        tdSql.execute(f"insert into t1 values (1591060628001, 2);")

        tdLog.info(f"======== add column in super table")
        tdSql.execute(f"drop database db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags (t1 int);")
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1);")
        tdSql.execute(f"alter table st1 add column f2 int;")
        tdSql.execute(f"insert into t1 values (1591060628001, 2, 2);")

        tdLog.info(f"======== add tag in super table")
        tdSql.execute(f"drop database db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags (t1 int);")
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1);")
        tdSql.execute(f"alter table st1 add tag t2 int;")
        tdSql.execute(f"create table t2 using st1 tags(2, 2);")

        tdLog.info(f"======== drop tag in super table")
        tdSql.execute(f"drop database db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int) tags (t1 int, t2 int);"
        )
        tdSql.execute(f"create table t1 using st1 tags(1, 1);")
        tdSql.execute(f"insert into t1 values (1591060628000, 1);")
        tdSql.execute(f"alter table st1 drop tag t2;")
        tdSql.execute(f"create table t2 using st1 tags(2);")

        tdLog.info(f"======== drop tag in super table")
        tdSql.execute(f"create database if not exists aaa;")
        tdSql.query(
            f"select table_name, db_name from information_schema.ins_tables t where t.db_name like 'aaa';"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database if exists foo;")
        tdSql.execute(f"create database if not exists foo;")
        tdSql.execute(f"create table foo.t(ts timestamp,name varchar(20));")
        tdSql.execute(f"create table foo.xt(ts timestamp,name varchar(20));")
        tdSql.query(
            f"select table_name, db_name from information_schema.ins_tables t where t.db_name like 'foo';"
        )
        tdSql.checkRows(2)


