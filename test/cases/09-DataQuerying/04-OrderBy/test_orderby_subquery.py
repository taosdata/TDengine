from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, tdCom, etool


class TestOrderByBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_orderby_basic(self):
        """Order by subquery results

        1. Sort the results of subqueries
        2. Sort time data after applying the to_charfunction
        3. Sort with multiple order by clauses
        4. Sort before and after subqueries
        5. Verify ascending and descending order combinations
        6. Verify with limit and offset

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/query/multi_order_by.sim
            - 2025-8-19 Simon Guan Migrated from tsim/query/sort-pre-cols.sim

        """

        self.MultiOrderBy()
        tdStream.dropAllStreamsAndDbs()
        self.OrderByPrecols()
        tdStream.dropAllStreamsAndDbs()

    def MultiOrderBy(self):
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t(ts timestamp, f int);")
        tdSql.execute(
            f"insert into t values(now,0)(now+1s, 1)(now+2s, 2)(now+3s,3)(now+4s,4)(now+5s,5)(now+6s,6)(now+7s,7)(now+8s,8)(now+9s,9)"
        )
        tdSql.query(
            f"select * from (select * from t order by ts desc limit 3 offset 2) order by ts;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 5)

        tdSql.checkData(1, 1, 6)

        tdSql.checkData(2, 1, 7)

        tdSql.query(
            f"select * from (select * from t order by ts limit 3 offset 2) order by ts desc;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 4)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 1, 2)

        tdSql.query(
            f"select * from (select * from t order by ts desc limit 3 offset 2) order by ts desc;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 7)

        tdSql.checkData(1, 1, 6)

        tdSql.checkData(2, 1, 5)

        tdSql.query(
            f"select * from (select * from t order by ts limit 3 offset 2) order by ts;"
        )
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(2,1)}")
        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 1, 4)

    def OrderByPrecols(self):
        tdSql.execute(f"create database d")
        tdSql.execute(f"use d")
        tdSql.execute(f"create table st(ts timestamp, v int) tags(lj json)")
        tdSql.execute(
            'insert into ct1 using st tags(\'{"instance":"200"}\') values(now, 1)(now+1s, 2);'
        )
        tdSql.execute(
            'insert into ct2 using st tags(\'{"instance":"200"}\') values(now+2s, 3)(now+3s, 4);'
        )
        tdSql.query(
            f"select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(v) from st group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), lj->'instance' order by time;"
        )
        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 0.000000000)



class TestSortElimination:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sort_elimination_subquery(self):
        """Post-split sort elimination for subquery with ORDER BY + LIMIT

        When a subquery contains ORDER BY ts LIMIT N, the planner splits the
        inner Sort into a Merge node that already guarantees global order.
        The outer redundant Sort should be eliminated by postSplitOptimize.
        Compares EXPLAIN plans and query results via .in/.ans file comparison.

        Since: v3.4.1.6

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-16 Added for dataOrderLevel sort elimination optimization

        """

        tdSql.execute("drop database if exists db_sort_elim;")
        tdSql.execute("create database db_sort_elim vgroups 2;")
        tdSql.execute("use db_sort_elim;")
        tdSql.execute(
            "create stable st(ts timestamp, val int, msg nchar(20)) tags(region int);"
        )
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.execute("create table t2 using st tags(2);")
        tdSql.execute("create table t3 using st tags(3);")
        tdSql.execute("create table t4 using st tags(1);")
        tdSql.execute("create table t5 using st tags(2);")
        tdSql.execute("create table t6 using st tags(3);")

        tdSql.execute(
            "insert into t1 values "
            "('2025-01-01 00:00:01', 10, 'a1') "
            "('2025-01-01 00:00:03', 30, 'a3') "
            "('2025-01-01 00:00:05', 50, 'a5') "
            "('2025-01-01 00:00:07', 70, 'a7') "
            "('2025-01-01 00:00:09', 90, 'a9') "
            "('2025-01-01 00:00:11', 11, 'a11') "
            "('2025-01-01 00:00:13', 31, 'a13') "
            "('2025-01-01 00:00:15', 51, 'a15');"
        )
        tdSql.execute(
            "insert into t2 values "
            "('2025-01-01 00:00:02', 20, 'b2') "
            "('2025-01-01 00:00:04', 40, 'b4') "
            "('2025-01-01 00:00:06', 60, 'b6') "
            "('2025-01-01 00:00:08', 80, 'b8') "
            "('2025-01-01 00:00:10', 100, 'b10') "
            "('2025-01-01 00:00:12', 21, 'b12') "
            "('2025-01-01 00:00:14', 41, 'b14') "
            "('2025-01-01 00:00:16', 61, 'b16');"
        )
        tdSql.execute(
            "insert into t3 values "
            "('2025-01-01 00:00:01.500', 15, 'c1') "
            "('2025-01-01 00:00:03.500', 35, 'c3') "
            "('2025-01-01 00:00:05.500', 55, 'c5') "
            "('2025-01-01 00:00:07.500', 75, 'c7') "
            "('2025-01-01 00:00:09.500', 95, 'c9') "
            "('2025-01-01 00:00:11.500', 16, 'c11') "
            "('2025-01-01 00:00:13.500', 36, 'c13') "
            "('2025-01-01 00:00:15.500', 56, 'c15');"
        )
        tdSql.execute(
            "insert into t4 values "
            "('2025-01-01 00:00:02.500', 25, 'd2') "
            "('2025-01-01 00:00:04.500', 45, 'd4') "
            "('2025-01-01 00:00:06.500', 65, 'd6') "
            "('2025-01-01 00:00:08.500', 85, 'd8') "
            "('2025-01-01 00:00:10.500', 105, 'd10') "
            "('2025-01-01 00:00:12.500', 26, 'd12') "
            "('2025-01-01 00:00:14.500', 46, 'd14') "
            "('2025-01-01 00:00:16.500', 66, 'd16');"
        )
        tdSql.execute(
            "insert into t5 values "
            "('2025-01-01 00:00:00.800', 8, 'e0') "
            "('2025-01-01 00:00:03.200', 32, 'e3') "
            "('2025-01-01 00:00:05.800', 58, 'e5') "
            "('2025-01-01 00:00:08.200', 82, 'e8') "
            "('2025-01-01 00:00:10.800', 108, 'e10') "
            "('2025-01-01 00:00:13.200', 33, 'e13') "
            "('2025-01-01 00:00:15.800', 58, 'e15') "
            "('2025-01-01 00:00:17.000', 70, 'e17');"
        )
        tdSql.execute(
            "insert into t6 values "
            "('2025-01-01 00:00:01.200', 12, 'f1') "
            "('2025-01-01 00:00:04.200', 42, 'f4') "
            "('2025-01-01 00:00:06.200', 62, 'f6') "
            "('2025-01-01 00:00:09.200', 92, 'f9') "
            "('2025-01-01 00:00:11.200', 13, 'f11') "
            "('2025-01-01 00:00:13.800', 38, 'f13') "
            "('2025-01-01 00:00:16.200', 62, 'f16') "
            "('2025-01-01 00:00:18.000', 80, 'f18');"
        )

        sqlFile = etool.curFile(__file__, "in/test_sort_elimination.in")
        ansFile = etool.curFile(__file__, "ans/test_sort_elimination.ans")
        tdCom.compare_testcase_result(sqlFile, ansFile, "test_sort_elimination")

        tdSql.execute("drop database db_sort_elim;")


class TestMergeIntervalOrder:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_merge_interval_order(self):
        """MergeAlignedInterval resultDataOrder: GLOBAL without partition, IN_GROUP with partition

        After plan splitting, the coordinator MergeAlignedInterval node merges
        all vnodes' aligned intervals into a single globally-sorted output.
        Without partition by, resultDataOrder should be GLOBAL (Merge ResBlocks: True).
        With partition by, each group is separate so resultDataOrder stays IN_GROUP
        (Merge ResBlocks: False).

        Since: v3.4.1.6

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-25 Added for MergeAlignedInterval order level validation

        """

        tdSql.execute("drop database if exists db_merge_intv;")
        tdSql.execute("create database db_merge_intv vgroups 2;")
        tdSql.execute("use db_merge_intv;")
        tdSql.execute(
            "create stable st(ts timestamp, val int) tags(region int);"
        )
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.execute("create table t2 using st tags(2);")
        tdSql.execute("create table t3 using st tags(3);")

        tdSql.execute(
            "insert into t1 values "
            "('2025-01-01 00:00:01', 10) "
            "('2025-01-01 00:00:06', 20) "
            "('2025-01-01 00:00:11', 30) "
            "('2025-01-01 00:00:16', 40);"
        )
        tdSql.execute(
            "insert into t2 values "
            "('2025-01-01 00:00:02', 15) "
            "('2025-01-01 00:00:07', 25) "
            "('2025-01-01 00:00:12', 35) "
            "('2025-01-01 00:00:17', 45);"
        )
        tdSql.execute(
            "insert into t3 values "
            "('2025-01-01 00:00:03', 18) "
            "('2025-01-01 00:00:08', 28) "
            "('2025-01-01 00:00:13', 38) "
            "('2025-01-01 00:00:18', 48);"
        )

        sqlFile = etool.curFile(__file__, "in/test_merge_interval_order.in")
        ansFile = etool.curFile(__file__, "ans/test_merge_interval_order.ans")
        tdCom.compare_testcase_result(sqlFile, ansFile, "test_merge_interval_order")

        tdSql.execute("drop database db_merge_intv;")
