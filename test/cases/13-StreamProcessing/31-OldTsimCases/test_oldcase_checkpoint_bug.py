import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    sc,
    clusterComCheck,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseCheckPoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_checkpoint(self):
        """Stream checkpoint

        Test if the stream continues to run after a restart.

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

        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        streams = []
        streams.append(self.Session0())
        tdStream.checkAll(streams)

    class Session0(StreamCheckItem):
        def __init__(self):
            self.db = "test"

        def create(self):
            tdSql.execute(
                f"create table session0_t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream session0_stream session(ts, 10s) from session0_t1 stream_options(max_delay(3s)) into session0_result as select _twstart, _twend, count(*) c1, sum(a) from session0_t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into session0_t1 values(1648791213000, 1, 2, 3, 1.0);"
            )
            tdSql.execute(
                f"insert into session0_t1 values(1648791213001, 2, 2, 3, 1.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from session0_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 3),
            )

        def insert2(self):
            sc.dnodeStop(1)
            sc.dnodeStart(1)

        def check2(self):
            clusterComCheck.checkDnodes(1)
            tdStream.checkStreamStatus()

        def insert3(self):
            tdSql.execute(
                f"insert into session0_t1 values(1648791213002, 3, 2, 3, 1.1);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from session0_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6),
            )

        def insert4(self):
            tdSql.execute(
                f"insert into session0_t1 values(1648791233003, 4, 2, 3, 1.1);"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from session0_result;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:53.003")
                and tdSql.compareData(1, 1, "2022-04-01 13:33:53.003")
                and tdSql.compareData(1, 2, 1)
                and tdSql.compareData(1, 3, 4),
            )

        def insert5(self):
            sc.dnodeStop(1)
            sc.dnodeStart(1)

        def check5(self):
            clusterComCheck.checkDnodes(1)
            tdStream.checkStreamStatus()

        def insert6(self):
            tdSql.execute(
                f"insert into session0_t1 values(1648791233004, 5, 2, 3, 1.1);"
            )

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from session0_result;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:53.003")
                and tdSql.compareData(1, 1, "2022-04-01 13:33:53.004")
                and tdSql.compareData(1, 2, 2)
                and tdSql.compareData(1, 3, 9),
            )
