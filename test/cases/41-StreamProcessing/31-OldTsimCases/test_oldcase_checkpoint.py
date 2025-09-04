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
        """OldTsim: checkpoint

        Test if the stream continues to run after a restart.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/checkpointInterval0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/checkpointInterval1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/checkpointSession0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/checkpointSession1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/checkpointState0.sim

        """

        tdStream.createSnode()

        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        streams = []
        streams.append(self.Interval0())
        streams.append(self.Interval1())
        streams.append(self.Session0())
        streams.append(self.Session1())
        streams.append(self.State0())
        tdStream.checkAll(streams)

    class Interval0(StreamCheckItem):
        def __init__(self):
            self.db = "test"

        def create(self):
            tdSql.execute(
                f"create table interval0_t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream interval0_stream0 interval(10s) sliding(10s) from interval0_t1 stream_options(max_delay(3s)) into interval0_result0 as select _twstart, count(*) c1, sum(a) from interval0_t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream interval0_stream1 interval(10s) sliding(10s) from interval0_t1 into interval0_result1 as select _twstart, count(*) c1, sum(a) from interval0_t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into interval0_t1 values(1648791213000, 1, 2, 3, 1.0);"
            )
            tdSql.execute(
                f"insert into interval0_t1 values(1648791213001, 2, 2, 3, 1.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from interval0_result0;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 3),
            )

        def insert2(self):
            sc.dnodeStop(1)
            sc.dnodeStart(1)

        def check2(self):
            clusterComCheck.checkDnodes(1)
            tdStream.checkStreamStatus()

        def insert3(self):
            tdSql.execute(
                f"insert into interval0_t1 values(1648791213002, 3, 2, 3, 1.1);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from interval0_result0;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6),
            )

        def insert4(self):
            tdSql.execute(
                f"insert into interval0_t1 values(1648791223003, 4, 2, 3, 1.1);"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from interval0_result0;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(1, 2, 4),
            )
            tdSql.checkResultsByFunc(
                f"select * from interval0_result1;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6),
            )

        def insert5(self):
            sc.dnodeStop(1)
            sc.dnodeStart(1)

        def check5(self):
            clusterComCheck.checkDnodes(1)
            tdStream.checkStreamStatus()

        def insert6(self):
            tdSql.execute(
                f"insert into interval0_t1 values(1648791223004, 5, 2, 3, 1.1);"
            )

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from interval0_result0;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(1, 1, 2)
                and tdSql.compareData(1, 2, 9),
            )
            tdSql.checkResultsByFunc(
                f"select * from interval0_result1;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6),
            )

    class Interval1(StreamCheckItem):
        def __init__(self):
            self.db = "test"

        def create(self):
            tdSql.execute(
                f"create stable interval1_st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(
                f"create table interval1_t1 using interval1_st tags(1, 1, 1);"
            )
            tdSql.execute(
                f"create table interval1_t2 using interval1_st tags(2, 2, 2);"
            )

            tdSql.execute(
                f"create stream interval1_stream interval(10s) sliding(10s) from interval1_st stream_options(max_delay(3s)) into interval1_result as select _twstart, count(*) c1, sum(a) from interval1_st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into interval1_t1 values(1648791213000, 1, 2, 3, 1.0);"
            )
            tdSql.execute(
                f"insert into interval1_t2 values(1648791213001, 2, 2, 3, 1.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from interval1_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 3),
            )

        def insert6(self):
            tdSql.execute(
                f"insert into interval1_t1 values(1648791213002, 3, 2, 3, 1.1);"
            )
            tdSql.execute(
                f"insert into interval1_t2 values(1648791223003, 4, 2, 3, 1.1);"
            )

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from interval1_result;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(0, 2, 6)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(1, 2, 4),
            )

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

    class Session1(StreamCheckItem):
        def __init__(self):
            self.db = "test"

        def create(self):
            tdSql.execute(
                f"create stable session1_st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table session1_t1 using session1_st tags(1, 1, 1);")
            tdSql.execute(f"create table session1_t2 using session1_st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream session1_stream session(ts, 10s) from session1_st stream_options(max_delay(3s)) into session1_result as select _twstart, _twend, count(*) c1, sum(a) from session1_st where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into session1_t1 values(1648791213000, 1, 2, 3, 1.0);"
            )
            tdSql.execute(
                f"insert into session1_t2 values(1648791213001, 2, 2, 3, 1.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from session1_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 3),
            )

        def insert6(self):
            tdSql.execute(
                f"insert into session1_t1 values(1648791213002, 3, 2, 3, 1.1);"
            )
            tdSql.execute(
                f"insert into session1_t2 values(1648791233003, 4, 2, 3, 1.1);"
            )

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from session1_result;",
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

    class State0(StreamCheckItem):
        def __init__(self):
            self.db = "test"

        def create(self):
            tdSql.execute(
                f"create table state0_t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream state0_stream state_window(b) from state0_t1 stream_options(max_delay(3s)) into state0_result as select _twstart, _twend, count(*) c1, sum(a) from state0_t1  where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into state0_t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into state0_t1 values(1648791213001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from state0_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 3),
            )

        def insert3(self):
            tdSql.execute(f"insert into state0_t1 values(1648791213002, 3, 2, 3, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from state0_result;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 6),
            )

        def insert4(self):
            tdSql.execute(f"insert into state0_t1 values(1648791233003, 4, 3, 3, 1.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from state0_result;",
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

        def insert6(self):
            tdSql.execute(f"insert into state0_t1 values(1648791233004, 5, 3, 3, 1.1);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from state0_result;",
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
