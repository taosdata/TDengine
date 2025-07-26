import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseShowCreateTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_show_create_table(self):
        """show create table

        1. -

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/show_create_table.sim

        """

        tdLog.info(f"===============create three type table")
        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table meters(ts timestamp, f binary(8)) tags(loc int, zone binary(8))"
        )
        tdSql.execute(f"create table t0 using meters tags(1,'ch')")
        tdSql.execute(f"create table normalTbl(ts timestamp, zone binary(8))")

        tdSql.execute(f"use db")
        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)

        tdLog.info(f"===============check sub table")
        tdSql.query(f"show create table t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table")

        tdSql.query(f"show create table normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table")
        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdLog.info(f"===============check sub table with prefix")

        tdSql.query(f"show create table db.t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table with prefix")
        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table with prefix")
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdSql.query('alter local \'showFullCreateTableColumn\' \'1\'')

        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)

        tdLog.info(f"===============check sub table")
        tdSql.query(f"show create table t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table")

        tdSql.query(f"show create table normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table")
        tdSql.query(f"show create table meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdLog.info(f"===============check sub table with prefix")

        tdSql.query(f"show create table db.t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0")

        tdLog.info(f"===============check normal table with prefix")
        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "normaltbl")

        tdLog.info(f"===============check super table with prefix")
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")

        tdSql.query('alter local \'showFullCreateTableColumn\' \'0\'')

        tdSql.execute(f"drop database db")
