import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseCheckPoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_checkpoint(self):
        """Stream checkpoint

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointInterval1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointSession0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointSession1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointState0.sim

        """

        tdStream.createSnode()

        tdLog.info(f"checkpointInterval0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams0 interval(10s) sliding(10s) from t1 options(max_delay(1s)) into streamt as select _twstart, count(*) c1, sum(a) from t1 where ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            f"create stream streams1 interval(10s) sliding(10s) from t1 into streamt1 as select _twstart, count(*) c1, sum(a) from t1 where ts >= _twstart and ts < _twend;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 3),
        )


        tdSql.execute(f"insert into t1 values(1648791213002, 3, 2, 3, 1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6),
        )
        