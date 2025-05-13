import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoin:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join(self):
        """Join Test

        1.

        Catalog:
            - Query:Join

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join.sim

        """

        dbPrefix = "db"
        tbPrefix1 = "tba"
        tbPrefix2 = "tbb"
        mtPrefix = "stb"
        tbNum = 10
        rowNum = 2

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt1 = mtPrefix + str(i)
        i = 1
        mt2 = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt1} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )
        tdSql.execute(
            f"create table {mt2} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )

        tdLog.info(f"====== start create child tables and insert data")
        i = 0
        while i < tbNum:
            tb = tbPrefix1 + str(i)
            tdSql.execute(
                f"create table {tb} using {mt1} tags( {i} , 'aaaaaaaaaaaaaaaaaaaaaaaaaaa')"
            )

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc

                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 0
        while i < tbNum:
            tb = tbPrefix2 + str(i)
            tdSql.execute(
                f"create table {tb} using {mt2} tags( {i} , 'aaaaaaaaaaaaaaaaaaaaaaaaaaa')"
            )

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc

                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1

            i = i + 1

        tdLog.info("data inserted")
        tdSql.query(f"select * from tba0 t1, tbb0 t2 where t1.ts=t2.ts;")
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from stb0 t1, stb1 t2 where t1.ts=t2.ts and t1.tag2=t2.tag2;"
        )
        tdSql.checkRows(200)
