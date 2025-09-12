import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_delete(self):
        """OldTsim: interp delete

        Verify the calculation results of the interp function when deleting data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpError.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpDelete0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpDelete1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpDelete2.sim

        """

        tdStream.createSnode()

        self.streamInterpDelete0()
        # self.streamInterpDelete1()
        # self.streamInterpDelete2()

    def streamInterpDelete0(self):
        tdLog.info(f"streamInterpDelete0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from t1 optioons(max_delay(3s)) into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(_twstart) fill(prev);"
        )

        tdSql.pause()
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791214000, 8, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0) (1648791215009, 15, 1, 1, 1.0) (1648791217001, 4, 1, 1, 1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(4, 1) == 15,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 8
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 8,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 8
            and tdSql.getData(3, 1) == 8,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791214000, 8, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0) (1648791215009, 15, 1, 1, 1.0) (1648791217001, 4, 1, 1, 1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )

    def streamInterpDelete1(self):
        tdLog.info(f"streamInterpDelete1")
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

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791214000, 8, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0) (1648791215009, 15, 1, 1, 1.0) (1648791217001, 4, 1, 1, 1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == None
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == None
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == None
            and tdSql.getData(3, 1) == None,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 100, 200, 300, 400);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791214000, 8, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0) (1648791215009, 15, 1, 1, 1.0) (1648791217001, 4, 1, 1, 1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 100
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(3, 1) == 100,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 100
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 100
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(3, 1) == 100,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 100
            and tdSql.getData(3, 1) == 100,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )

    def streamInterpDelete2(self):
        tdLog.info(f"streamInterpDelete2")
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

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791214000, 8, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0) (1648791215009, 15, 1, 1, 1.0) (1648791217001, 4, 1, 1, 1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linera);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 9
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 6
            and tdSql.getData(3, 1) == 5
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(2, 1) == 5
            and tdSql.getData(3, 1) == 4,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )
