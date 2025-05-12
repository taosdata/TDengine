from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCountSliding:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count_sliding(self):
        """Count Window

        1. -

        Catalog:
            - Timeseries:EventWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/query_count_sliding0.sim

        """

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")

        tdLog.info(
            f"00 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        # row 0
        tdSql.checkRows(1)
        # row 0
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")

        tdLog.info(
            f"01 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.checkRows(1)

        # row 0
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"insert into t1 values(1648791213002,0,3,3,1.0);")

        tdLog.info(
            f"02 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdSql.checkRows(2)

        # row 0
        tdSql.checkData(0, 1, 3)

        # row 1
        tdSql.checkData(1, 1, 1)

        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        # row 0
        tdSql.checkRows(6)

        tdSql.execute(
            f"insert into t1 values(1648791243000,0,1,1,1.0) (1648791243001,9,2,2,1.1);"
        )

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdSql.checkRows(7)

        tdSql.execute(
            f"insert into t1 values(1648791253000,0,1,1,1.0) (1648791253001,9,2,2,1.1) (1648791253002,9,2,2,1.1);"
        )

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        # row 0
        tdSql.checkRows(9)

        tdSql.execute(f"insert into t1 values(1648791263000,0,1,1,1.0);")

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)}"
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )

        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )

        # row 0
        tdSql.checkRows(6)
        tdSql.execute(
            f"insert into t1 values(1648791243000,0,1,1,1.0) (1648791243001,9,2,2,1.1);"
        )

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )

        tdSql.checkRows(7)
        tdSql.execute(
            f"insert into t1 values(1648791253000,0,1,1,1.0) (1648791253001,9,2,2,1.1) (1648791253002,9,2,2,1.1);"
        )

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )

        # row 0
        tdSql.checkRows(9)

        tdSql.execute(f"insert into t1 values(1648791263000,0,1,1,1.0);")

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )
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

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4, 1);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st count_window(4,1);"
        )

        # rows
        tdSql.checkRows(12)

        # row 0
        tdSql.checkData(0, 1, 4)

        tdSql.checkData(0, 2, 4)

        # row 1
        tdSql.checkData(1, 1, 4)

        tdSql.checkData(1, 2, 4)

        # row 2
        tdSql.checkData(2, 1, 4)

        tdSql.checkData(2, 2, 4)

        # row 9
        tdSql.checkData(9, 1, 3)

        tdSql.checkData(9, 2, 3)

        # row 10
        tdSql.checkData(10, 1, 2)

        tdSql.checkData(10, 2, 2)
        # row 11
        tdSql.checkData(11, 1, 1)

        tdSql.checkData(11, 2, 1)
