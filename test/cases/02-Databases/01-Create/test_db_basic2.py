import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic2(self):
        """db basic2

        1. -

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/basic2.sim

        """

        tdLog.info(f"=============== conflict stb")
        tdSql.execute(f"create database db vgroups 4;")
        tdSql.execute(f"use db;")
        tdSql.execute(f"create table stb (ts timestamp, i int) tags (j int);")
        tdSql.error(f"create table stb using stb tags (1);")
        tdSql.error(f"create table stb (ts timestamp, i int);")

        tdSql.execute(f"create table ctb (ts timestamp, i int);")
        tdSql.error(f"create table ctb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"create table ntb (ts timestamp, i int);")
        tdSql.error(f"create table ntb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"drop table ntb")
        tdSql.execute(f"create table ntb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"drop database db")

        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table t1 (ts timestamp, i int);")
        tdSql.execute(f"create table t2 (ts timestamp, i int);")
        tdSql.execute(f"create table t3 (ts timestamp, i int);")
        tdSql.execute(f"create table t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdSql.checkRows(3)

        tdSql.checkData(2, 0, "d1")

        tdSql.checkData(2, 2, 2)

        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create database d2")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table t1 (ts timestamp, i int);")
        tdSql.execute(f"create table t2 (ts timestamp, i int);")
        tdSql.execute(f"create table t3 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.query(f"show tables")
        tdSql.checkRows(3)
