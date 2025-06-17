import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseTwa:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_twa(self):
        """Stream twa

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim

        """

        tdStream.createSnode()

       
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams2 period(2s) from st partition by tbname options(expired_time(0s)|ignore_disorder) into streamt as select _tlocaltime, twa(a), twa(b), elapsed(ts), now, timezone() from st where ts >= _tlocaltime and ts < _tnext_localtime;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  1s, 1, 1, 1)(now +  2s, 10, 1, 1)(now + 3s, 20, 2, 2)(now + 4s, 30, 3, 3)(now + 5s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 8s, 30, 3, 3)(now + 9s, 30, 3, 3)(now + 10s, 30, 3, 3);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  1s, 1, 1, 1)(now +  2s, 10, 1, 1)(now + 3s, 20, 2, 2)(now + 4s, 30, 3, 3)(now + 5s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 8s, 30, 3, 3)(now + 9s, 30, 3, 3)(now + 10s, 30, 3, 3);"
        )

        tdSql.checkResultsByFunc(
            f"select * from test2.streamt;",
            lambda: tdSql.getRows() > 0,
        )
