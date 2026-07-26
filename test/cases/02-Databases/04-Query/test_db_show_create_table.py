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

        Labels: common,ci,integration,functional
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
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `f` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`loc` INT, `zone` VARCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `zone` VARCHAR(8) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium')")


        tdSql.execute('alter local \'showFullCreateTableColumn\' \'0\'')
        tdSql.query(f"show create table db.meters")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE STABLE `meters` (`ts` TIMESTAMP, `f` VARCHAR(8)) TAGS (`loc` INT, `zone` VARCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0")

        tdSql.query(f"show create table db.normalTbl")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE TABLE `normaltbl` (`ts` TIMESTAMP, `zone` VARCHAR(8))")
        tdSql.execute(f"drop database db")

    def test_empty_nchar_tag(self):
        """Show create table empty nchar tag

        1. when nchar-type tag is empty, show create table should output an empty string
        2. alter table to set nchar-type tag to a non-empty string, show create table should output the new string
        3. alter table to set nchar-type tag to an empty string, show create table should output an empty string again
        4. alter table to set nchar-type tag to a non-empty string again, show create table should output the new string again
        5. drop database


        Since: v3.3.6.14

        Labels: common,ci,nchar,tag,integration,functional
        Jira: TS-7526

        History:
            - 2025-10-24 Tony Zhang created

        """

        tdLog.info(f"===============create table with empty nchar tag")
        tdSql.execute(f"create database if not exists db")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table stb (ts timestamp, v int) tags(t1 nchar(10), gid int)", show=True)
        tdSql.execute(f"create table ctb1 using stb tags('', 1)", show=True)
        tdSql.query(f"show create table ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb1")
        tdSql.checkData(0, 1, 'CREATE TABLE `ctb1` USING `stb` (`t1`, `gid`) TAGS ("", 1)')

        tdSql.execute("alter table ctb1 set tag t1 = '测试'", show=True)
        tdSql.query(f"show create table ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb1")
        tdSql.checkData(0, 1, 'CREATE TABLE `ctb1` USING `stb` (`t1`, `gid`) TAGS ("测试", 1)')

    def test_show_create_table_after_tag_drop_and_add(self):
        """show create table keeps surviving tag values after a tag is replaced

        1. Create a child table with four tag values
        2. Drop two middle stable tags and add a replacement tag
        3. Verify SHOW CREATE preserves the later tag value and emits NULL for the replacement tag

        Catalog:
            - Database:Query

        Since: v3.3.6.35

        Labels: common,ci,tag

        Feishu: 7056595118

        History:
            - 2026-07-23 Codex created

        """

        tdSql.prepare(dbname="show_create_tag", drop=True)
        tdSql.execute("create stable stb (ts timestamp, v int) tags(t1 int, t2 int, t3 int, t4 int)")
        tdSql.execute("create table ctb using stb tags(1, 2, 3, 4)")
        tdSql.execute("alter stable stb drop tag t2")
        tdSql.execute("alter stable stb drop tag t3")
        tdSql.execute("alter stable stb add tag t5 int")

        tdSql.query("show create table ctb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "CREATE TABLE `ctb` USING `stb` (`t1`, `t4`, `t5`) TAGS (1, 4, NULL)")
