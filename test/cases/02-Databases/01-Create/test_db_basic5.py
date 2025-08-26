import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic5:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic5(self):
        """Database: basic 5

        1. Create database
        2. Create super table
        3. Create child table
        4. Show super tables
        5. Show tables
        6. Drop database and retest

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic5.sim

        """

        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")

        tdLog.info(f"=============== create stable and table")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags(t1 int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values (now, 1);")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== drop db")
        tdSql.execute(f"drop database db1;")

        tdSql.error(f"show stables")
        tdSql.error(f"show tables")

        tdLog.info(f"=============== re-create db and stb")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags(t1 int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query(f"show tables")
        tdSql.checkRows(0)
