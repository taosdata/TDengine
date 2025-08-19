from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSLimitLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_slimit_limit(self):
        """SLimit limit

        1.

        Catalog:
            - Query:SLimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/slimit_limit.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tba3 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba4 using sta tags(4, 4, 4);")
        tdSql.execute(f"create table tba5 using sta tags(5, 5, 5);")
        tdSql.execute(f"create table tba6 using sta tags(6, 6, 6);")
        tdSql.execute(f"create table tba7 using sta tags(7, 7, 7);")
        tdSql.execute(f"create table tba8 using sta tags(8, 8, 8);")
        tdSql.execute(f"create index index1 on sta (t2);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 11, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 2, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 22, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:01', 3, \"a\");")
        tdSql.execute(f"insert into tba4 values ('2022-04-26 15:15:01', 4, \"a\");")
        tdSql.execute(f"insert into tba5 values ('2022-04-26 15:15:01', 5, \"a\");")
        tdSql.execute(f"insert into tba6 values ('2022-04-26 15:15:01', 6, \"a\");")
        tdSql.execute(f"insert into tba7 values ('2022-04-26 15:15:01', 7, \"a\");")
        tdSql.execute(f"insert into tba8 values ('2022-04-26 15:15:01', 8, \"a\");")

        tdSql.query(f"select t1,count(*) from sta group by t1 limit 1;")
        tdSql.checkRows(8)

        tdSql.query(f"select t1,count(*) from sta group by t1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select f1,count(*) from sta group by f1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 limit 1;")
        tdSql.checkRows(10)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 slimit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,count(*) from sta group by t1 order by t1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,count(*) from sta group by t1 order by t1 slimit 1;")
        tdSql.checkRows(8)

        tdSql.query(f"select f1,count(*) from sta group by f1 order by f1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 order by f1 slimit 1;")
        tdSql.checkRows(10)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by t1, f1 order by t1,f1 limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by t1, f1 order by t1,f1 slimit 1;"
        )
        tdSql.checkRows(10)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by f1, t1 order by f1,t1 limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select t1,f1,count(*) from sta group by f1, t1 order by f1,t1 slimit 1;"
        )
        tdSql.checkRows(10)

        tdSql.query(f"select t1,count(*) from sta group by t1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select f1,count(*) from sta group by f1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by t1, f1 slimit 1 limit 1;")
        tdSql.checkRows(1)

        tdSql.query(f"select t1,f1,count(*) from sta group by f1, t1 slimit 1 limit 1;")
        tdSql.checkRows(1)
