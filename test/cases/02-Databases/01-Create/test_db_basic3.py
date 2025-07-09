import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic3(self):
        """create database 3

        1. create database
        2. create normal table with db. as the prefix
        3. show tables use db. as the prefix

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic3.sim

        """

        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 2)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create database d2")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d2.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t3 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.query(f"show d2.tables")
        tdSql.checkRows(3)
