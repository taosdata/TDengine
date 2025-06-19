from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestParserAlterStable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_parser_alter_stable(self):
        """alter stable

        1.

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter_stable.sim

        """

        tdLog.info(f"========== alter_stable.sim")

        db = "demodb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        ##### alter stable test : rename tag name
        # case-1  rename tag name: new name inclue old name
        tdSql.execute(f"create table mt1 (ts timestamp, c1 int) tags (a int)")
        tdSql.execute(f"alter table mt1 rename tag a abcd")
        tdSql.execute(f"alter table mt1 rename tag abcd a")
        tdSql.error(f"alter table mt1 rename tag a 1")

        tdSql.error(f"create table mtx1 (ts timestamp, c1 int) tags (123 int)")

        tdSql.error(
            f"create table mt2 (ts timestamp, c1 int) tags (abc012345678901234567890123456789012345678901234567890123456789def int)"
        )
        tdSql.execute(
            f"create table mt3 (ts timestamp, c1 int) tags (abc012345678901234567890123456789012345678901234567890123456789 int)"
        )
        tdSql.error(
            f"alter table mt3 rename tag abc012345678901234567890123456789012345678901234567890123456789 abcdefg012345678901234567890123456789012345678901234567890123456789"
        )
        tdSql.execute(
            f"alter table mt3 rename tag abc012345678901234567890123456789012345678901234567890123456789 abcdefg0123456789012345678901234567890123456789"
        )

        # case-2 set tag value
        tdSql.execute(
            f"create table mt4 (ts timestamp, c1 int) tags (name binary(16), len int)"
        )
        tdSql.execute(f'create table tb1 using mt4 tags ("beijing", 100)')
        tdSql.execute(f'alter table tb1 set tag name = "shanghai"')
        tdSql.execute(f'alter table tb1 set tag name = ""')
        tdSql.execute(f'alter table tb1 set tag name = "shenzhen"')
        tdSql.execute(f"alter table tb1 set tag len = 379")

        # case TD-5594
        tdSql.execute(
            f"create stable st5520(ts timestamp, f int) tags(t0 bool, t1 nchar(4093), t2 nchar(1))"
        )
        tdSql.error(f"alter stable st5520 modify tag t2 nchar(2);")
        # test end
        tdSql.execute(f"drop database {db}")
