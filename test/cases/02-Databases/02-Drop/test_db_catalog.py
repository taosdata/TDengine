import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseCatalog:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_catalog(self):
        """Drop db basic

        1. Drop database with if exists
        2. Drop empty database
        3. Drop database with only meta
        4. Drop database with super/child/normal tables
        5. Loop drop and create database for many times
        6. Verify drop database on information_schema.ins_databases


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/catalog/alterInCurrent.sim

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
