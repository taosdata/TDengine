from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinPk:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_pk(self):
        """Join inner 

        1. Create 1 database and 1 super table
        2. Create 2 child tables
        3. Insert 1 rows data to each child table with different timestamps
        4. Inner join two child tables on timestamp with interval(1s)
        5. Check the result of join correctly 

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join_pk.sim

        """

        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 0)(now+1s, 1)")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now+2s, 2)(now+3s, 3)")
        tdSql.query(
            f"select * from (select _wstart - 1s as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )

        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(1, 3, 1)

        tdSql.checkData(2, 3, 1)

        tdSql.query(
            f"select * from (select _wstart - 1d as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )

        tdSql.query(
            f"select * from (select _wstart + 1a as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )

        tdSql.error(
            f"select * from (select _wstart *  3 as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )

        tdSql.execute(
            f"create table sst(ts timestamp, ts2 timestamp, f int) tags(t int);"
        )
        tdSql.execute(
            f"insert into sct1 using sst tags(1) values('2023-08-07 13:30:56', '2023-08-07 13:30:56', 0)('2023-08-07 13:30:57', '2023-08-07 13:30:57', 1)"
        )
        tdSql.execute(
            f"insert into sct2 using sst tags(2) values('2023-08-07 13:30:58', '2023-08-07 13:30:58', 2)('2023-08-07 13:30:59', '2023-08-07 13:30:59', 3)"
        )
        tdSql.query(
            f"select * from (select ts - 1s as jts from sst) as t1 inner join (select ts-1s as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from (select ts - 1s as jts from sst) as t1 inner join (select ts as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(3)

        tdSql.error(
            f"select * from (select ts2 - 1s as jts from sst) as t1 inner join (select ts2 as jts from sst) as t2 on t1.jts = t2.jts"
        )
