import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpUpdate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_update(self):
        """OldTsim: interp update

        Validate the calculation results of the interp function during data updates

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpUpdate.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpUpdate1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpUpdate2.sim

        """

        tdStream.createSnode()

        self.streamInterpUpdate()
        # self.streamInterpUpdate1()
        # self.streamInterpUpdate2()

    def streamInterpUpdate(self):
        tdLog.info(f"streamInterpUpdate")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)) into streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(_twstart) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0)  (1648791217001, 4, 1, 1, 1.0)"
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
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 2, 2.1);")

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
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(4, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791215000, 20, 20, 20, 20.1);")

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
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 20
            and tdSql.getData(4, 1) == 20,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 8, 8, 8, 8.1);")

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
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 20
            and tdSql.getData(4, 1) == 20,
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
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0)  (1648791217001, 4, 1, 1, 1.0)"
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
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 2, 2.1);")

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
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 10
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791215000, 20, 20, 20, 20.1);")

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
            and tdSql.getData(0, 1) == 20
            and tdSql.getData(1, 1) == 20
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 8, 8, 8, 8.1);")

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
            and tdSql.getData(0, 1) == 20
            and tdSql.getData(1, 1) == 20
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 8,
        )

    def streamInterpUpdate1(self):
        tdLog.info(f"streamInterpUpdate1")
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
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0)  (1648791217001, 4, 1, 1, 1.0)"
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
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 2, 2.1);")

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
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791215000, 20, 20, 20, 20.1);")

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
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 8, 8, 8, 8.1);")

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
            and tdSql.getData(0, 1) == None
            and tdSql.getData(1, 1) == None
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == None
            and tdSql.getData(4, 1) == None,
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
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0)  (1648791217001, 4, 1, 1, 1.0)"
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
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

        tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 2, 2.1);")

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
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

        tdSql.execute(f"insert into t1 values(1648791215000, 20, 20, 20, 20.1);")

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
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 8, 8, 8, 8.1);")

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
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

    def streamInterpUpdate2(self):
        tdLog.info(f"streamInterpUpdate2")
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
            f"insert into t1 values(1648791212001, 1, 1, 1, 1.0) (1648791215000, 10, 1, 1, 1.0)  (1648791217001, 4, 1, 1, 1.0)"
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
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 7
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 2, 2.1);")

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
            and tdSql.getData(1, 1) == 7
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 7
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791215000, 20, 20, 20, 20.1);")

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
            and tdSql.getData(0, 1) == 7
            and tdSql.getData(1, 1) == 13
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 12
            and tdSql.getData(4, 1) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 8, 8, 8, 8.1);")

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
            and tdSql.getData(0, 1) == 7
            and tdSql.getData(1, 1) == 13
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 14
            and tdSql.getData(4, 1) == 8,
        )
