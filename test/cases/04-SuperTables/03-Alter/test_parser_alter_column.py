from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestParserAlterColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_parser_alter_column(self):
        """alter column

        1.

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter_column.sim

        """

        dbPrefix = "m_alt_db"
        tbPrefix = "m_alt_tb"
        mtPrefix = "m_alt_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        ##### alter table test, simeplest case
        tdSql.execute(
            f"create table tb (ts timestamp, c1 int, c2 binary(10), c3 nchar(10))"
        )
        tdSql.execute(f'insert into tb values (now, 1, "1", "1")')
        tdSql.execute(f"alter table tb modify column c2 binary(20);")
        tdSql.execute(f"alter table tb modify column c3 nchar(20);")

        tdSql.execute(
            f"create stable stb (ts timestamp, c1 int, c2 binary(10), c3 nchar(10)) tags(id1 int, id2 binary(10), id3 nchar(10))"
        )
        tdSql.execute(f'create table tb1 using stb tags(1, "a", "b")')
        tdSql.execute(f'insert into tb1 values (now, 1, "1", "1")')
        tdSql.execute(f"alter stable stb modify column c2 binary(20);")
        tdSql.execute(f"alter table stb modify column c2 binary(30);")
        tdSql.execute(f"alter stable stb modify column c3 nchar(20);")
        tdSql.execute(f"alter table stb modify column c3 nchar(30);")
        tdSql.execute(f"alter table stb modify tag id2 binary(11);")
        tdSql.error(f"alter stable stb modify tag id2 binary(11);")
        tdSql.execute(f"alter table stb modify tag id3 nchar(11);")
        tdSql.error(f"alter stable stb modify tag id3 nchar(11);")

        ##### ILLEGAL OPERATIONS

        # try dropping columns that are defined in metric
        tdSql.error(f"alter table tb modify column c1 binary(10);")
        tdSql.error(f"alter table tb modify column c1 double;")
        tdSql.error(f"alter table tb modify column c2 int;")
        tdSql.error(f"alter table tb modify column c2 binary(10);")
        tdSql.error(f"alter table tb modify column c2 binary(9);")
        tdSql.error(f"alter table tb modify column c2 binary(-9);")
        tdSql.error(f"alter table tb modify column c2 binary(0);")
        tdSql.error(f"alter table tb modify column c2 binary(65436);")
        tdSql.error(f"alter table tb modify column c2 nchar(30);")
        tdSql.error(f"alter table tb modify column c3 double;")
        tdSql.error(f"alter table tb modify column c3 nchar(10);")
        tdSql.error(f"alter table tb modify column c3 nchar(0);")
        tdSql.error(f"alter table tb modify column c3 nchar(-1);")
        tdSql.error(f"alter table tb modify column c3 binary(80);")
        tdSql.error(f"alter table tb modify column c3 nchar(17000);")
        tdSql.error(f"alter table tb modify column c3 nchar(100), c2 binary(30);")
        tdSql.error(f"alter table tb modify column c1 nchar(100), c2 binary(30);")
        tdSql.error(f"alter stable tb modify column c2 binary(30);")
        tdSql.error(f"alter table tb modify tag c2 binary(30);")
        tdSql.error(f"alter table stb modify tag id2 binary(10);")
        tdSql.error(f"alter table stb modify tag id2 nchar(30);")
        tdSql.error(f"alter stable stb modify tag id2 binary(10);")
        tdSql.error(f"alter stable stb modify tag id2 nchar(30);")
        tdSql.error(f"alter table stb modify tag id3 nchar(10);")
        tdSql.error(f"alter table stb modify tag id3 binary(30);")
        tdSql.error(f"alter stable stb modify tag id3 nchar(10);")
        tdSql.error(f"alter stable stb modify tag id3 binary(30);")
        tdSql.error(f"alter stable stb modify tag id1 binary(30);")
        tdSql.error(f"alter stable stb modify tag c1 binary(30);")

        tdSql.error(f"alter table tb1 modify column c2 binary(30);")
        tdSql.error(f"alter table tb1 modify column c3 nchar(30);")
        tdSql.error(f"alter table tb1 modify tag id2 binary(30);")
        tdSql.error(f"alter table tb1 modify tag id3 nchar(30);")
        tdSql.error(f"alter stable tb1 modify tag id2 binary(30);")
        tdSql.error(f"alter stable tb1 modify tag id3 nchar(30);")
        tdSql.error(f"alter stable tb1 modify column c2 binary(30);")
