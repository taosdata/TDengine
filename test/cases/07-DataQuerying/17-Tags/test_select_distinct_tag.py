from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestProjectColumn1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_distinct_tag(self):
        """select distinct tag

        1. -

        Catalog:
            - Query:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/select_distinct_tag.sim

        """

        dbPrefix = "sav_db"
        tbPrefix = "sav_tb"
        stbPrefix = "sav_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} , 0 )")
            tdSql.execute(f"insert into {tb} (ts, c1) values (now, 1);")
            i = i + 1

        tdLog.info(f"====== table created")

        #### select distinct tag
        tdSql.query(f"select distinct t1 from {stb}")
        tdSql.checkRows(tbNum)

        #### select distinct tag
        tdSql.query(f"select distinct t2 from {stb}")
        tdSql.checkRows(1)

        #### unsupport sql
        tdSql.error(f"select distinct t1, t2 from &stb")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
