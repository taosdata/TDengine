import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseShowCreateTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_show_create_table(self):
        """Show create table

        1. Create a normal table
        2. Create a super table
        3. Create child tables
        4. Execute SHOW CREATE TABLE and verify the output
        5. Change the showFullCreateTableColumn parameter
        6. Execute SHOW CREATE TABLE again and verify the new output

        Catalog:
            - Database:Query

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

        tdSql.execute('alter local \'showFullCreateTableColumn\' \'1\'')

        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "meters")
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `f` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`loc` INT, `zone` VARCHAR(8))")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `zone` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium')")


        tdSql.execute('alter local \'showFullCreateTableColumn\' \'0\'')
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP, `f` VARCHAR(8)) TAGS (`loc` INT, `zone` VARCHAR(8))")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP, `zone` VARCHAR(8))")
        tdSql.execute(f"drop database db")
