from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count(self):
        """Count Window

        1. -

        Catalog:
            - Timeseries:EventWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/query_count0.sim

        """

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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4);"
        )
        
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(2, 2, 4)

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
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(2147483648);"
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
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(2147483647);"
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
