from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestEvent:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_event(self):
        """Event: basic test

        1. Test the usage of event window, including various start/end conditions, combination with PARTITION BY and GROUP BY, usage as subqueries, etc.
        2. Test some illegal statements

        Catalog:
            - Timeseries:EventWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/event.sim

        """

        tdLog.info(f"======== prepare data")

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 5;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(10), f3 bool) tags(t1 int, t2 bool, t3 binary(10));"
        )
        tdSql.execute(f"create table tba1 using sta tags(0, false, '0');")
        tdSql.execute(f"create table tba2 using sta tags(1, true, '1');")
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:01', 0, \"a\", false);"
        )
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:02', 1, \"0\", true);"
        )
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:03', 5, \"5\", false);"
        )
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:04', 3, 'b', false);"
        )
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:05', 0, '1', false);"
        )
        tdSql.execute(f"insert into tba1 values ('2022-09-26 15:15:06', 2, 'd', true);")
        tdSql.execute(
            f"insert into tba2 values ('2022-09-26 15:15:01', 0, \"a\", false);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-26 15:15:02', 1, \"0\", true);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-26 15:15:03', 5, \"5\", false);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-26 15:15:04', 3, 'b', false);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-26 15:15:05', 0, '1', false);"
        )
        tdSql.execute(f"insert into tba2 values ('2022-09-26 15:15:06', 2, 'd', true);")

        # child table: no window
        tdSql.query(
            f"select count(*) from tba1 event_window start with f1 = 0 end with f2 = 'c';"
        )
        tdSql.checkRows(0)

        # child table: single row window
        tdLog.info(
            f"====> select count(*) from tba1 event_window start with f1 = 0 end with f3 = false;"
        )
        tdSql.query(
            f"select count(*) from tba1 event_window start with f1 = 0 end with f3 = false;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)

        # child table: multi rows window
        tdSql.query(
            f"select count(*) from tba1 event_window start with f1 = 0 end with f2 = 'b';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        # child table: multi windows
        tdLog.info(
            f"====> select count(*) from tba1 event_window start with f1 >= 0 end with f3 = true;"
        )
        tdSql.query(
            f"select count(*) from tba1 event_window start with f1 >= 0 end with f3 = true;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 4)

        # super table: no window
        # duplicate timestamp
        tdSql.error(
            f"select count(*) from sta event_window start with f1 = 0 end with f2 = 'c';"
        )

        # super table: single row window
        tdLog.info(
            f"====> select count(*) from sta event_window start with f1 = 0 end with f3 = false;"
        )
        tdSql.error(
            f"select count(*) from sta event_window start with f1 = 0 end with f3 = false;"
        )

        # super table: multi rows window
        tdSql.error(
            f"select count(*) from sta event_window start with f1 = 0 end with f2 = 'b';"
        )

        # super table: multi windows
        tdLog.info(
            f"====> select count(*) from sta event_window start with f1 >= 0 end with f3 = true;"
        )
        tdSql.error(
            f"select count(*) from sta event_window start with f1 >= 0 end with f3 = true;"
        )
        # multi-child table: no window
        tdSql.query(
            f"select tbname, count(*) from sta partition by tbname event_window start with f1 = 0 end with f2 = 'c';"
        )
        tdSql.checkRows(0)

        # multi-child table: single row window
        tdLog.info(
            f"====> select tbname, count(*) from sta partition by tbname event_window start with f1 = 0 end with f3 = false;"
        )
        tdSql.query(
            f"select tbname, count(*) from sta partition by tbname event_window start with f1 = 0 end with f3 = false;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)

        # multi-child table: multi rows window
        tdSql.query(
            f"select tbname, count(*) from sta partition by tbname event_window start with f1 = 0 end with f2 = 'b';"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 1, 4)

        # multi-child table: multi windows
        tdLog.info(
            f"====> select tbname, count(*) from sta partition by tbname event_window start with f1 >= 0 end with f3 = true;"
        )
        tdSql.query(
            f"select tbname, count(*) from sta partition by tbname event_window start with f1 >= 0 end with f3 = true;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 4)

        # where + partition by
        tdLog.info(
            f"====> select tbname, count(*) from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0;"
        )
        tdSql.query(
            f"select tbname, count(*) from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)

        # where + order by
        tdLog.info(
            f"====> select count(*) cnt from tba1 where f3 = false event_window start with f1 >0 end with f2 > 0 order by cnt desc;"
        )
        tdSql.query(
            f"select count(*) cnt from tba1 where f3 = false event_window start with f1 >0 end with f2 > 0 order by cnt desc;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)

        # where + partition by + order by
        tdLog.info(
            f"====> select tbname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt;"
        )
        tdSql.query(
            f"select tbname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        # where + partition by + order by + limit
        tdLog.info(
            f"====> select tbname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2;"
        )
        tdSql.query(
            f"select tbname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)

        # subquery(where + partition by + order by + limit)
        tdLog.info(
            f"====> select * from (select tbname tname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2);"
        )
        tdSql.query(
            f"select * from (select tbname tname, count(*) cnt from sta where f3 = false partition by tbname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)

        # subquery + where + partition by + order by + limit
        tdLog.info(
            f"====> select tname, count(*) cnt from (select tbname tname, * from sta) where f3 = false partition by tname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2;"
        )
        tdSql.query(
            f"select tname, count(*) cnt from (select tbname tname, * from sta) where f3 = false partition by tname event_window start with f1 >0 end with f2 > 0 order by cnt limit 2 offset 2;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)

        tdSql.error(
            f"select f1, f2 from sta event_window start with f1 >0 end with f2 > 0;"
        )
        tdSql.error(
            f"select count(*) from sta event_window start with f1 >0 end with f2 > 0 partition by tbname;"
        )
        tdSql.error(
            f"select count(*) from sta event_window start with f1 >0 end with f2 > 0 group by tbname;"
        )
        tdSql.error(
            f"select count(*) from sta event_window start with f1 >0 end with f2 > 0 fill(NULL);"
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791253004,5,2,3,4.1);")

        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t2 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t2 values(1648791253004,5,2,3,4.1);")

        tdSql.execute(f"insert into t3 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t3 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t3 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t3 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t3 values(1648791253004,5,2,3,4.1);")

        for i in range(4):
            tdSql.query(
                f"select  _wstart, count(*) c1, tbname from st partition by tbname event_window start with a > 0 end with b = 2  slimit 2 limit 2;"
            )
            tdSql.checkRows(4)
            tdLog.info(f"======rows={tdSql.getRows()})")
