import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpFill:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_fill(self):
        """OldTsim: interp fill

        Validate the calculation results of the interp function when filling data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpLarge.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpLinear0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpNext0.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrev0.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrev1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpValue0.sim

        """

        tdStream.createSnode()

        self.streamInterpLarge()
        # self.streamInterpLinear0()
        # self.streamInterpNext0()
        # self.streamInterpValue0()

    def streamInterpLarge(self):
        tdLog.info(f"streamInterpLarge")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)) into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(_twstart) fill(prev);"
        )

        tdSql.pause()

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648700000000, 1, 1, 1, 1.0) (1648710000000, 100, 100, 100, 100.0) (1648720000000, 10, 10, 10, 10.0);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 20001, print=False
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648700000000, 1, 1, 1, 1.0) (1648710000000, 100, 100, 100, 100.0) (1648720000000, 10, 10, 10, 10.0);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 20001,
            print=False,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648700000000, 1, 1, 1, 1.0) (1648710000000, 100, 100, 100, 100.0) (1648720000000, 10, 10, 10, 10.0);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 20001, print=False
        )

        tdLog.info(f"step4")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test4 vgroups 1;")
        tdSql.execute(f"use test4;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 1, 2, 3, 4);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648700000000, 1, 1, 1, 1.0) (1648710000000, 100, 100, 100, 100.0) (1648720000000, 10, 10, 10, 10.0);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 20001, print=False
        )

        tdLog.info(f"step5")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test5 vgroups 1;")
        tdSql.execute(f"use test5;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648700000000, 1, 1, 1, 1.0) (1648710000000, 100, 100, 100, 100.0) (1648720000000, 10, 10, 10, 10.0);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 20001, print=False
        )

    def streamInterpLinear0(self):
        tdLog.info(f"streamInterpLinear0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.1);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 2, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 3, 3, 3, 3.1);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 14, 14, 14, 14.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 5
            and tdSql.getData(2, 1) == 8
            and tdSql.getData(3, 1) == 11
            and tdSql.getData(4, 1) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791215001, 7, 7, 7, 7.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"3 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 6
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 13,
        )

        tdLog.info(f"step2")

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791213001, 11, 11, 11, 11.0) (1648791213009, 22, 22, 22, 2.1) (1648791215001, 15, 15, 15, 15.1) (1648791217001, 34, 34, 34, 34.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 18
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(4, 1) == 24
            and tdSql.getData(5, 1) == 33,
        )

        tdLog.info(f"step2_1")

        tdSql.execute(f"create database test2_1 vgroups 1;")
        tdSql.execute(f"use test2_1;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2_1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2_1 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212011, 0, 0, 0, 0.0) (1648791212099, 20, 20, 20, 20.0) (1648791213011, 11, 11, 11, 11.0)  (1648791214099, 35, 35, 35, 35.1) (1648791215011, 10, 10, 10, 10.1) (1648791218099, 34, 34, 34, 34.1) (1648791219011, 5, 5, 5, 5.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791219011) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791219011) every(1s) fill(linear);"
        )

        tdLog.info(f"1 sql select * from streamt2_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2_1;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 11
            and tdSql.getData(1, 1) == 32
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 17
            and tdSql.getData(4, 1) == 25
            and tdSql.getData(5, 1) == 33
            and tdSql.getData(6, 1) == 5,
        )

        tdLog.info(f"step3")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 0, 0, 0, 0.0) (1648791217001, 8, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217000) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217000) every(1s) fill(linear);"
        )

        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 5,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213001, 11, 11, 11, 11.0) (1648791213009, 22, 22, 22, 22.1) (1648791215001, 15, 15, 15, 15.1)"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"2 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 18
            and tdSql.getData(2, 1) == 15
            and tdSql.getData(3, 1) == 11
            and tdSql.getData(4, 1) == 8,
        )

    def streamInterpNext0(self):
        tdLog.info(f"streamInterpNext0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 3, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 4, 4.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791215001, 5, 5, 5, 5.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"3 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 5
            and tdSql.getData(2, 1) == 5
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791213001, 1, 1, 1, 1.0) (1648791213009, 2, 2, 2, 1.1) (1648791215001, 5, 5, 5, 5.1) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 5
            and tdSql.getData(3, 1) == 5
            and tdSql.getData(4, 1) == 4
            and tdSql.getData(5, 1) == 4,
        )

        tdLog.info(
            f"{tdSql.getData(0, 0)} {tdSql.getData(0, 1)} {tdSql.getData(0, 2)} {tdSql.getData(0, 3)} {tdSql.getData(0, 4)}"
        )
        tdLog.info(
            f"{tdSql.getData(1, 0)} {tdSql.getData(1, 1)} {tdSql.getData(1, 2)} {tdSql.getData(1, 3)} {tdSql.getData(1, 4)}"
        )
        tdLog.info(
            f"{tdSql.getData(2, 0)} {tdSql.getData(2, 1)} {tdSql.getData(2, 2)} {tdSql.getData(2, 3)} {tdSql.getData(2, 4)}"
        )
        tdLog.info(
            f"{tdSql.getData(3, 0)} {tdSql.getData(3, 1)} {tdSql.getData(3, 2)} {tdSql.getData(3, 3)} {tdSql.getData(3, 4)}"
        )
        tdLog.info(
            f"{tdSql.getData(4, 0)} {tdSql.getData(4, 1)} {tdSql.getData(4, 2)} {tdSql.getData(4, 3)} {tdSql.getData(4, 4)}"
        )
        tdLog.info(
            f"{tdSql.getData(5, 0)} {tdSql.getData(5, 1)} {tdSql.getData(5, 2)} {tdSql.getData(5, 3)} {tdSql.getData(5, 4)}"
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791210001, 0, 0, 0, 0.0) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(next);"
        )

        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 7,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 1, 1.0) (1648791213009, 2, 2, 2, 1.1) (1648791215001, 5, 5, 5, 5.1)"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"2 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 5
            and tdSql.getData(4, 1) == 5
            and tdSql.getData(5, 1) == 4
            and tdSql.getData(6, 1) == 4,
        )

    def streamInterpValue0(self):
        tdLog.info(f"streamInterpValue0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 3, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 4, 4.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdLog.info(f"2 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791215001, 5, 5, 5, 5.1);")

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"3 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791213000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdLog.info(f"3 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10
            and tdSql.getData(1, 2) == 20
            and tdSql.getData(1, 3) == 30
            and tdSql.getData(1, 4) == 40.000000000,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2_1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams2_2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791213001, 1, 1, 1, 1.0) (1648791213009, 2, 2, 2, 1.1) (1648791215001, 5, 5, 5, 5.1) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None
            and tdSql.getData(5, 1) == None,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdLog.info(f"1 sql select * from streamt4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10
            and tdSql.getData(5, 1) == 10,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3_1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_1 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams3_2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_2 as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791210001, 0, 0, 0, 0.0) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(NULL);"
        )

        tdLog.info(f"1 sql select * from streamt3_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_1;",
            lambda: tdSql.getRows() == 7,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(value, 10, 20, 30, 40);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217000) every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdLog.info(f"1 sql select * from streamt3_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_2;",
            lambda: tdSql.getRows() == 7,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 1, 1.0) (1648791213009, 2, 2, 2, 1.1) (1648791215001, 5, 5, 5, 5.1)"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"2 sql select * from streamt3_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_1;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == None
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None
            and tdSql.getData(5, 1) == None
            and tdSql.getData(6, 1) == None,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791211000, 1648791217001) every(1s) fill(value, 10, 20, 30, 40);"
        )

        tdLog.info(f"2 sql select * from streamt3_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_2;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10
            and tdSql.getData(5, 1) == 10
            and tdSql.getData(6, 1) == 10,
        )
