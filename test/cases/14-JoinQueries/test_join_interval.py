from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinInterval:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_interval(self):
        """Join interval

        1.Create database d1 and d2
        2.Create table t1 in d1 with tags and t1 in d2 without tags
        3.Insert data into d1.t1 with different tag values
        4.Insert data into d2.t1 with same timestamps as d1.t1
        5.Join query between d1.t1 and d2.t1 with interval(1a)
        6. Check the result of join correctly

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join_interval.sim

        """

        tdLog.info(f"======== step create databases")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d1.t1(ts timestamp, i int) tags(t int);")
        tdSql.execute(f"create table d2.t1(ts timestamp, i int);")
        tdSql.execute(
            f"insert into d1.t11 using d1.t1 tags(1) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d1.t12 using d1.t1 tags(2) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d1.t13 using d1.t1 tags(3) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d2.t1 values(1500000000000,0)(1500000000001,1)(1500000000002,2)"
        )

        tdSql.query(
            f"select _wstart,_wend,count((a.ts)),count(b.ts) from d1.t1 a, d2.t1 b where a.ts is not null and a.ts = b.ts  interval(1a) ;"
        )
        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(1, 2, 3)

        tdSql.checkData(1, 3, 3)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)
