from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestGroupByDistinct:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby_distinct(self):
        """Group By

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/groupby_distinct.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")

        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:08', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:07', 1, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:06', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 1, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 1, \"c\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:03', 1, \"c\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 1, \"d\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1, \"d\");")
        tdSql.query(f"select distinct avg(f1) as avgv from sta group by f2;")
        tdSql.checkRows(1)

        tdSql.query(f"select distinct avg(f1) as avgv from sta group by f2 limit 1,10;")
        tdSql.checkRows(0)
