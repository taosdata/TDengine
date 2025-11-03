from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestPartitonBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_partitionby(self):
        """PartitionBy: basic test

        Test the use of PARTITION BY in projection queries, including combinations with SLIMIT, different filtering conditions, and usage as subqueries.

        Catalog:
            - Timeseries:PartitionBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/partitionby.sim

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

        tdSql.execute(f"create database {db} vgroups 3")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt1} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )
        tdSql.execute(f"create table tb0 using {mt1} tags(0, 'a');")
        tdSql.execute(f"create table tb1 using {mt1} tags(1, 'b');")
        tdSql.execute(f"create table tb2 using {mt1} tags(2, 'a');")
        tdSql.execute(f"create table tb3 using {mt1} tags(3, 'a');")
        tdSql.execute(f"create table tb4 using {mt1} tags(4, 'b');")
        tdSql.execute(f"create table tb5 using {mt1} tags(5, 'a');")
        tdSql.execute(f"create table tb6 using {mt1} tags(6, 'b');")
        tdSql.execute(f"create table tb7 using {mt1} tags(7, 'b');")

        tdSql.query(f"select * from {mt1} partition by tag1,tag2 limit 1;")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into tb0 values ('2022-04-26 15:15:08', 1);")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:07', 2);")
        tdSql.execute(f"insert into tb2 values ('2022-04-26 15:15:06', 3);")
        tdSql.execute(f"insert into tb3 values ('2022-04-26 15:15:05', 4);")
        tdSql.execute(f"insert into tb4 values ('2022-04-26 15:15:04', 5);")
        tdSql.execute(f"insert into tb5 values ('2022-04-26 15:15:03', 6);")
        tdSql.execute(f"insert into tb6 values ('2022-04-26 15:15:02', 7);")
        tdSql.execute(f"insert into tb7 values ('2022-04-26 15:15:01', 8);")

        tdSql.query(
            f"select _wstart as ts, count(*) from {mt1} partition by tag1 interval(1s) order by _wstart;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2022-04-26 15:15:01.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2022-04-26 15:15:02.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2022-04-26 15:15:03.000")
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, "2022-04-26 15:15:04.000")
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 0, "2022-04-26 15:15:05.000")
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, "2022-04-26 15:15:06.000")
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 0, "2022-04-26 15:15:07.000")
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 0, "2022-04-26 15:15:08.000")
        tdSql.checkData(7, 1, 1)

        tdSql.query(
            f"select * from (select _wstart as ts, count(*) from {mt1} partition by tag1 interval(1s) order by _wstart) order by ts;"
        )
        tdSql.query(
            f"select _wstart as ts, count(*) from {mt1} interval(1s) order by _wstart;"
        )
        tdSql.query(
            f"select * from (select _wstart as ts, count(*) from {mt1} interval(1s) order by _wstart) order by ts;"
        )
        tdSql.query(
            f"select diff(a) from (select _wstart as ts, count(*) a from {mt1} interval(1s) order by _wstart);"
        )
        tdSql.query(
            f"select diff(a) from (select _wstart as ts, count(*) a from {mt1} partition by tag1 interval(1s) order by _wstart);"
        )

        tdSql.execute(f"insert into tb0 values (now, 0);")
        tdSql.execute(f"insert into tb1 values (now, 1);")
        tdSql.execute(f"insert into tb2 values (now, 2);")
        tdSql.execute(f"insert into tb3 values (now, 3);")
        tdSql.execute(f"insert into tb4 values (now, 4);")
        tdSql.execute(f"insert into tb5 values (now, 5);")
        tdSql.execute(f"insert into tb6 values (now, 6);")
        tdSql.execute(f"insert into tb7 values (now, 7);")

        tdSql.query(
            f"select * from (select 1 from {mt1} where ts is not null partition by tbname limit 1);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(*) from (select ts from {mt1} where ts is not null partition by tbname slimit 2);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select count(*) from (select ts from {mt1} where ts is not null partition by tbname limit 2);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)
