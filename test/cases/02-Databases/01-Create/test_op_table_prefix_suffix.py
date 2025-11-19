import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseTablePrefixSuffix:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_table_prefix_suffix(self):
        """Options: table prefix & suffix

        1. Create database with TABLE_PREFIX and TABLE_SUFFIX options
        2. Create tables
        3. Verify that tables are distributed across vgroups as expected

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/table_prefix_suffix.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 5 TABLE_PREFIX 1 TABLE_SUFFIX 2;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table atb1aa (ts timestamp, f1 int);")
        tdSql.execute(f"create table btb1bb (ts timestamp, f1 int);")
        tdSql.execute(f"create table ctb1cc (ts timestamp, f1 int);")
        tdSql.execute(f"create table dtb1dd (ts timestamp, f1 int);")
        tdSql.execute(f"create table atb2aa (ts timestamp, f1 int);")
        tdSql.execute(f"create table btb2bb (ts timestamp, f1 int);")
        tdSql.execute(f"create table ctb2cc (ts timestamp, f1 int);")
        tdSql.execute(f"create table dtb2dd (ts timestamp, f1 int);")
        tdSql.execute(f"create table etb2ee (ts timestamp, f1 int);")
        tdSql.query(f"show create database db1;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db1' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db1;")

        tdSql.execute(f"drop database if exists db2;")
        tdSql.execute(f"create database db2 vgroups 5 TABLE_PREFIX -1 TABLE_SUFFIX -2;")
        tdSql.execute(f"use db2;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table teee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db2;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db2' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db2;")

        tdSql.execute(f"drop database if exists db3;")
        tdSql.execute(f"create database db3 vgroups 5 TABLE_PREFIX -1;")
        tdSql.execute(f"use db3;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zaaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zeee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db3;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db3' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db3;")

        tdSql.execute(f"drop database if exists db4;")
        tdSql.execute(f"create database db4 vgroups 5 TABLE_SUFFIX -2;")
        tdSql.execute(f"use db4;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zaaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zeee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db4;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db4' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db4;")

        tdSql.execute(f"drop database if exists db5;")
        tdSql.execute(f"create database db5 vgroups 5 TABLE_PREFIX 1;")
        tdSql.execute(f"use db5;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table baaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table caaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table daaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table faaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table gbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table hbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table ibbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table jbbb11 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db5;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db5' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db5;")

        tdSql.execute(f"drop database if exists db6;")
        tdSql.execute(f"create database db6 vgroups 5 TABLE_SUFFIX 2;")
        tdSql.execute(f"use db6;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa12 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa13 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa14 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb23 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb33 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db6;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db6' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db6;")

        tdSql.execute(f"drop database if exists db7;")
        tdSql.execute(
            f"create database db7 vgroups 5 TABLE_PREFIX -100 TABLE_SUFFIX -92;"
        )
        tdSql.execute(f"use db7;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa12 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa13 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb21 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb23 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc33 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd33 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db7;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db7' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.execute(f"drop database if exists db7;")

        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX -1 TABLE_SUFFIX 2;")
        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX 191 TABLE_SUFFIX 192;")
        tdSql.error(
            f"create database db8 vgroups 5 TABLE_PREFIX -192 TABLE_SUFFIX -191;"
        )
        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX 100 TABLE_SUFFIX 92;")
