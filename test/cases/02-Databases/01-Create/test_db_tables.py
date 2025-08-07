import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseTables:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_tables(self):
        """Database: cache

        1. create database and table
        2. write and query data
        3. drop them
        4. reset query cache
        5. test again

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/tables.sim

        """

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,7)}")

        tdSql.checkRows(3)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table t1 (ts timestamp, i int)")
        tdSql.execute(f"create table t2 (ts timestamp, i int)")
        tdSql.execute(f"create table t3 (ts timestamp, i int)")
        tdSql.execute(f"create table t4 (ts timestamp, i int)")
        tdSql.execute(f"create table t11 (ts timestamp, i int)")
        tdSql.execute(f"create table t12 (ts timestamp, i int)")
        tdSql.execute(f"create table t13 (ts timestamp, i int)")
        tdSql.execute(f"create table t14 (ts timestamp, i int)")
        tdSql.execute(f"create table t21 (ts timestamp, i int)")
        tdSql.execute(f"create table t22 (ts timestamp, i int)")
        tdSql.execute(f"create table t23 (ts timestamp, i int)")
        tdSql.execute(f"create table t24 (ts timestamp, i int)")
        tdSql.execute(f"create table t31 (ts timestamp, i int)")
        tdSql.execute(f"create table t32 (ts timestamp, i int)")
        tdSql.execute(f"create table t33 (ts timestamp, i int)")
        tdSql.execute(f"create table t34 (ts timestamp, i int)")

        tdLog.info(f"=============== step4")
        tdSql.execute(f"insert into t1 values(now, 1)")
        tdSql.execute(f"insert into t2 values(now, 2)")
        tdSql.execute(f"insert into t3 values(now, 3)")
        tdSql.execute(f"insert into t4 values(now, 4)")
        tdSql.execute(f"insert into t11 values(now, 1)")
        tdSql.execute(f"insert into t22 values(now, 2)")
        tdSql.execute(f"insert into t33 values(now, 3)")
        tdSql.execute(f"insert into t34 values(now, 4)")

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from t1")
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from t2")
        tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from t3")
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select * from t4")
        tdSql.checkData(0, 1, 4)

        tdLog.info(f"=============== step6")
        tdSql.execute(f"drop database db")
        tdSql.execute(f"reset query cache")

        tdLog.info(f"=============== step7")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,7)}")

        tdSql.checkRows(3)

        tdLog.info(f"=============== step8")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table t1 (ts timestamp, i int)")
        tdSql.execute(f"create table t2 (ts timestamp, i int)")
        tdSql.execute(f"create table t3 (ts timestamp, i int)")
        tdSql.execute(f"create table t4 (ts timestamp, i int)")
        tdSql.execute(f"create table t11 (ts timestamp, i int)")
        tdSql.execute(f"create table t12 (ts timestamp, i int)")
        tdSql.execute(f"create table t13 (ts timestamp, i int)")
        tdSql.execute(f"create table t14 (ts timestamp, i int)")
        tdSql.execute(f"create table t21 (ts timestamp, i int)")
        tdSql.execute(f"create table t22 (ts timestamp, i int)")
        tdSql.execute(f"create table t23 (ts timestamp, i int)")
        tdSql.execute(f"create table t24 (ts timestamp, i int)")
        tdSql.execute(f"create table t31 (ts timestamp, i int)")
        tdSql.execute(f"create table t32 (ts timestamp, i int)")
        tdSql.execute(f"create table t33 (ts timestamp, i int)")
        tdSql.execute(f"create table t34 (ts timestamp, i int)")

        tdLog.info(f"=============== step9")
        tdSql.execute(f"insert into t1 values(now, 1)")
        tdSql.execute(f"insert into t2 values(now, 2)")
        tdSql.execute(f"insert into t3 values(now, 3)")
        tdSql.execute(f"insert into t4 values(now, 4)")
        tdSql.execute(f"insert into t11 values(now, 1)")
        tdSql.execute(f"insert into t22 values(now, 2)")
        tdSql.execute(f"insert into t33 values(now, 3)")
        tdSql.execute(f"insert into t34 values(now, 4)")

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from t1")
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from t2")
        tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from t3")
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select * from t4")
        tdSql.checkData(0, 1, 4)
