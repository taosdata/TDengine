import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


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
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdLog.info(f"step 0")
        # interval0
        tdSql.execute(
            f"create table interval0_t1(ts timestamp, a int, b int, c int, d double);"
        )
        tdSql.execute(
            f"create stream interval0_stream0 interval(10s) sliding(10s) from interval0_t1 options(max_delay(1s)) into interval0_result0 as select _twstart, count(*) c1, sum(a) from interval0_t1 where ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            f"create stream interval0_stream1 interval(10s) sliding(10s) from interval0_t1 into interval0_result1 as select _twstart, count(*) c1, sum(a) from interval0_t1 where ts >= _twstart and ts < _twend;"
        )
        # interval1
        tdSql.execute(
            f"create stable interval1_st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table interval1_t1 using interval1_st tags(1, 1, 1);")
        tdSql.execute(f"create table interval1_t2 using interval1_st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream interval1_stream interval(10s) sliding(10s) from interval1_st options(max_delay(1s)) into interval1_result as select _twstart, count(*) c1, sum(a) from interval1_st where ts >= _twstart and ts < _twend;"
        )
        # session0
        tdSql.execute(
            f"create table session0_t1(ts timestamp, a int, b int, c int, d double);"
        )
        tdSql.execute(
            f"create stream session0_stream session(ts, 10s) from session0_t1 options(max_delay(1s)) into session0_result as select _twstart, _twend, count(*) c1, sum(a) from session0_t1 where ts >= _twstart and ts <= _twend;"
        )
        # session1
        tdSql.execute(
            f"create stable session1_st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table session1_t1 using session1_st tags(1, 1, 1);")
        tdSql.execute(f"create table session1_t2 using session1_st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream session1_stream session(ts, 10s) from session1_st options(max_delay(1s)) into session1_result as select _twstart, _twend, count(*) c1, sum(a) from session1_st where ts >= _twstart and ts <= _twend;"
        )
        # state0
        tdSql.execute(
            f"create table state0_t1(ts timestamp, a int, b int, c int, d double);"
        )
        tdSql.execute(
            f"create stream state0_stream state_window(b) from state0_t1 options(max_delay(1s)) into state0_result as select _twstart, _twend, count(*) c1, sum(a) from state0_t1  where ts >= _twstart and ts <= _twend;"
        )

        tdStream.checkStreamStatus()
        tdLog.info(f"step 1")

        # interval0
        tdSql.execute(f"insert into interval0_t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into interval0_t1 values(1648791213001, 2, 2, 3, 1.1);")
        # interval1
        tdSql.execute(f"insert into interval1_t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into interval1_t2 values(1648791213001, 2, 2, 3, 1.1);")
        # session0
        tdSql.execute(f"insert into session0_t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into session0_t1 values(1648791213001, 2, 2, 3, 1.1);")
        # session1
        tdSql.execute(f"insert into session1_t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into session1_t2 values(1648791213001, 2, 2, 3, 1.1);")
        # state0
        tdSql.execute(f"insert into state0_t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into state0_t1 values(1648791213001, 2, 2, 3, 1.1);")

        tdSql.checkResultsByFunc(
            f"select * from interval0_result0;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 3),
        )
        tdSql.checkResultsByFunc(
            f"select * from interval1_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 3),
        )
        tdSql.checkResultsByFunc(
            f"select * from session0_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
            and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(0, 3, 3),
        )
        tdSql.checkResultsByFunc(
            f"select * from session1_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
            and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(0, 3, 3),
        )
        tdSql.checkResultsByFunc(
            f"select * from state0_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
            and tdSql.compareData(0, 1, "2022-04-01 13:33:33.001")
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(0, 3, 3),
        )

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus()

        tdLog.info(f"step 2")
        # interval0
        tdSql.execute(f"insert into interval0_t1 values(1648791213002, 3, 2, 3, 1.1);")
        # session0
        tdSql.execute(f"insert into session0_t1 values(1648791213002, 3, 2, 3, 1.1);")
        # state0
        tdSql.execute(f"insert into state0_t1 values(1648791213002, 3, 2, 3, 1.1);")

        tdSql.checkResultsByFunc(
            f"select * from interval0_result0;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6),
            retry=60,
        )
        tdSql.checkResultsByFunc(
            f"select * from session0_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
            and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 6),
        )
        tdSql.checkResultsByFunc(
            f"select * from state0_result;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
            and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 6),
        )

        tdLog.info(f"step 3")
        # interval0
        tdSql.execute(f"insert into interval0_t1 values(1648791223003, 4, 2, 3, 1.1);")
        # session0
        tdSql.execute(f"insert into session0_t1 values(1648791233003, 4, 2, 3, 1.1);")
        # state0
        tdSql.execute(f"insert into state0_t1 values(1648791233003, 4, 3, 3, 1.1);")

        tdSql.checkResultsByFunc(
            f"select * from interval0_result0;",
            lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, 4),
            retry=60,
        )
        tdSql.checkResultsByFunc(
            f"select * from interval0_result1;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6),
        )

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

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus()

        tdLog.info(f"step 4")
        # interval0
        tdSql.execute(f"insert into interval0_t1 values(1648791223004, 5, 2, 3, 1.1);")
        # interval1
        tdSql.execute(f"insert into interval1_t1 values(1648791213002, 3, 2, 3, 1.1);")
        tdSql.execute(f"insert into interval1_t2 values(1648791223003, 4, 2, 3, 1.1);")
        # session0
        tdSql.execute(f"insert into session0_t1 values(1648791233004, 5, 2, 3, 1.1);")
        # session1
        tdSql.execute(f"insert into session1_t1 values(1648791213002, 3, 2, 3, 1.1);")
        tdSql.execute(f"insert into session1_t2 values(1648791233003, 4, 2, 3, 1.1);")
        # state0
        tdSql.execute(f"insert into state0_t1 values(1648791233004, 5, 3, 3, 1.1);")

        tdSql.checkResultsByFunc(
            f"select * from interval0_result0;",
            lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 9),
            retry=60,
        )
        tdSql.checkResultsByFunc(
            f"select * from interval0_result1;",
            lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
            and tdSql.compareData(0, 1, 3)
            and tdSql.compareData(0, 2, 6),
        )
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
