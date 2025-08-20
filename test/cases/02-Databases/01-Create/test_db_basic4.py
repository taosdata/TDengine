import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic4(self):
        """Database: basic 4

        1. Create database
        2. Create normal table
        3. Show tables
        4. Drop table
        5. Show tables
        6. Drop database

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic4.sim

        """

        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1 vgroups 1")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(4)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, "d1")

        tdLog.info(f"=============== drop table")
        tdSql.execute(f"drop table d1.t1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(3)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, "d1")

        tdLog.info(f"=============== drop all table")
        tdSql.execute(f"drop table d1.t2")
        tdSql.execute(f"drop table d1.t3")
        tdSql.execute(f"drop table d1.t4")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(0)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, "d1")
