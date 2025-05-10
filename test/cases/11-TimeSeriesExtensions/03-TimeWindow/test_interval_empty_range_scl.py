from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestIntervalEmptyRangeScl:
    updatecfgDict = {'filterScalarMode': 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval_empty_range_scl(self):
        """interval function

        1. -

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/emptyTsRange_scl.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database if not exists db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);")
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 2.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 4.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 5.0, \"b\");")
        tdSql.query(f"select last_row(*) from sta where ts >= 1678901803783 and ts <= 1678901803783 and  _c0 <= 1678901803782 interval(10d,8d) fill(linear) order by _wstart desc;")
        tdSql.checkRows(0)

