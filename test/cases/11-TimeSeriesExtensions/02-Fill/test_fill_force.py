from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFillForce:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fill_force(self):
        """Fill Force

        1. -

        Catalog:
            - Timeseries:Fill

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/query/forceFill.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 10;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 2.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 4.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 5.0, \"b\");")

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 8.800000000)

        tdSql.checkData(2, 0, 8.800000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, None)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(14400)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(14400)

        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null_f);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, 1.000000000)

        tdSql.checkData(2, 0, 2.000000000)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, 4.000000000)

        tdSql.checkData(5, 0, 5.000000000)

        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 8.800000000)

        tdSql.checkData(2, 0, 8.800000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.checkData(1, 0, 8.800000000)

        tdSql.checkData(2, 0, 8.800000000)

        tdSql.checkData(3, 0, 8.800000000)

        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, None)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null_f);"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, None)

        tdSql.checkData(1, 0, None)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(14400)

        tdSql.checkData(0, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(null_f);"
        )
        tdSql.checkRows(14400)

        tdSql.checkData(0, 0, None)
