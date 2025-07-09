from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCaseWhenSum:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_case_when_sum(self):
        """Case When

        1. 

        Catalog:
            - Query:Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/bug3398.sim

        """

        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database test")

        tdLog.info(f'=============== create super table and child table')
        tdSql.execute(f"use test")

        tdSql.execute(f"CREATE STABLE st (day timestamp, c2 int) TAGS (vin binary(32))")

        tdSql.execute(f'insert into test.g using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.t using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.tg using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')

        tdSql.query(f"select sum(case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end), sum(case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end) from test.t t, test.g g, test.tg tg where t.day = g.day and t.day = tg.day and t.day between '2021-05-03' and '2023-05-04' and t.vin = 'TAG1' and t.vin = g.vin and t.vin = tg.vin group by t.day;")

        tdLog.info(f'{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(0,1)}')
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.checkData(0, 1, 3.000000000)

