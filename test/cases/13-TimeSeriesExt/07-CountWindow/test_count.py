from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count(self):
        """Count basic

        1. Count + sliding window queries
        2. Specified column queries
        3. Combined use with PARTITION BY and ORDER BY
        4. Some illegal value checks

        Catalog:
            - Timeseries:CountWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-27 Simon Guan Migrated from tsim/query/query_count_sliding0.sim
            - 2025-8-27 Simon Guan Migrated from tsim/query/query_count0.sim
            - 2025-8-27 Simon Guan Migrated from tsim/query/query_count1.sim

        """

        self.QueryCountSliding()
        tdStream.dropAllStreamsAndDbs()
        self.Count0()
        tdStream.dropAllStreamsAndDbs()
        self.Count1()
        tdStream.dropAllStreamsAndDbs()

    def QueryCountSliding(self):
        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(1)
        # row 0
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.checkRows(1)
        # row 0
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"insert into t1 values(1648791213002,0,3,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdSql.checkRows(2)
        # row 0
        tdSql.checkData(0, 1, 3)
        # row 1
        tdSql.checkData(1, 1, 1)

        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.checkRows(2)

        # row 0
        tdSql.checkData(0, 1, 4)
        # row 1
        tdSql.checkData(1, 1, 2)

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(4)
        # row 0
        tdSql.checkData(0, 1, 4)
        # row 1
        tdSql.checkData(1, 1, 4)
        # row 2
        tdSql.checkData(2, 1, 4)
        # row 3
        tdSql.checkData(3, 1, 2)

        tdSql.execute(
            f"insert into t1 values(1648791233000,0,1,1,1.0) (1648791233001,9,2,2,1.1) (1648791233002,9,2,2,1.1) (1648791233009,0,3,3,1.0);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(6)

        tdSql.execute(
            f"insert into t1 values(1648791243000,0,1,1,1.0) (1648791243001,9,2,2,1.1);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.checkRows(7)

        tdSql.execute(
            f"insert into t1 values(1648791253000,0,1,1,1.0) (1648791253001,9,2,2,1.1) (1648791253002,9,2,2,1.1);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(9)

        tdSql.execute(f"insert into t1 values(1648791263000,0,1,1,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(9)

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002,0,3,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.checkRows(2)
        # row 0
        tdSql.checkData(0, 1, 4)
        # row 1
        tdSql.checkData(1, 1, 2)

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.checkRows(4)
        # row 0
        tdSql.checkData(0, 1, 4)
        # row 1
        tdSql.checkData(1, 1, 4)
        # row 2
        tdSql.checkData(2, 1, 4)
        # row 3
        tdSql.checkData(3, 1, 2)

        tdSql.execute(
            f"insert into t1 values(1648791233000,0,1,1,1.0) (1648791233001,9,2,2,1.1) (1648791233002,9,2,2,1.1) (1648791233009,0,3,3,1.0);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(6)

        tdSql.execute(
            f"insert into t1 values(1648791243000,0,1,1,1.0) (1648791243001,9,2,2,1.1);"
        )

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.checkRows(7)

        tdSql.execute(
            f"insert into t1 values(1648791253000,0,1,1,1.0) (1648791253001,9,2,2,1.1) (1648791253002,9,2,2,1.1);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        # row 0
        tdSql.checkRows(9)

        tdSql.execute(f"insert into t1 values(1648791263000,0,1,1,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.checkRows(9)

        tdLog.info(f"step3")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,1,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,1,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791213000,0,1,4,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,9,1,5,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009,0,1,6,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,7,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,1,8,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,1,9,1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000,0,1,10,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,9,1,11,1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009,0,1,12,1.0);")

        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4,1);"
        )

    def Count0(self):
        tdLog.info(f"step1: normatable")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3);"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 3)

        tdSql.execute(f"insert into t1 values(1648791213010,NULL,3,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213011,0,NULL,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223005,NULL,NULL,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, a);"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 3)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, b);"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 3)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, a, b);"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 3)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 3, 3)

        tdLog.info(f"step2: subper table")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009,0,3,3,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3);"
        )

        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(0, 3, 3)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)

        tdSql.execute(f"insert into t1 values(1648791213005,NULL,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213008,0,NULL,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223011,NULL,NULL,1,1.0);")

        tdSql.execute(f"insert into t2 values(1648791213005,NULL,NULL,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213008,NULL,7,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223011,NULL,5,1,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, a) order by tbname, s;"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 3)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)

        # row 4
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, b) order by tbname, s;"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 2)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 10)
        tdSql.checkData(3, 3, 3)
        # row 4
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)
        # row 5
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(5, 2, 8)
        tdSql.checkData(5, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, a, b) order by tbname, s;"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 2)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 10)
        tdSql.checkData(3, 3, 3)
        # row 4
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)

        # row 5
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(5, 2, 8)
        tdSql.checkData(5, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 1, b) order by tbname, s;"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 2)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 7)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        # row 4
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)
        # row 5
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(5, 2, 5)
        tdSql.checkData(5, 3, 3)
        # row 6
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(6, 2, 3)
        tdSql.checkData(6, 3, 3)

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 1, a, b) order by tbname, s;"
        )
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 2)
        # row 1
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 3)
        # row 2
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 3, 3)
        # row 3
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(3, 3, 3)
        # row 4
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)
        # row 5
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(4, 3, 3)

        tdLog.info(f"step3: subper table with having")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000,0,1,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,0,3,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791223003,1,4,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791223004,1,5,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791223005,2,6,3,4.1);")

        tdSql.query(
            f"select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 3;"
        )
        # row 0
        tdSql.checkRows(1)

        tdSql.query(
            f"select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 6;"
        )
        # row 0
        tdSql.checkRows(0)

        tdLog.info(f"query_count0 end")

    def Count1(self):
        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,1,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,1,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791213000,0,1,4,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,9,1,5,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009,0,1,6,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,7,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,1,8,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,1,9,1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000,0,1,10,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,9,1,11,1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009,0,1,12,1.0);")

        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4);"
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        # 2~INT32_MAX
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(-1);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(0);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(1);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(2147483647);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(10, 0);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(10, -1);"
        )
        tdSql.error(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(10, 11);"
        )

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(2147483646);"
        )

        tdLog.info(f"step3")
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

        tdSql.query(
            f"select  _wstart, count(*) c1, tbname from st partition by tbname count_window(2)  slimit 2 limit 2;"
        )
        tdSql.checkRows(4)
        tdLog.info(f"query_count0 end")
