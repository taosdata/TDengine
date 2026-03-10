import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpPrimary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_primary(self):
        """OldTsim: interp compisite key

        Validate the calculation results of the interp function with cmposite keys

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey3.sim

        """

        tdStream.createSnode()

        self.streamInterpPrimaryKey0()
        # self.streamInterpPrimaryKey1()
        # self.streamInterpPrimaryKey2()
        # self.streamInterpPrimaryKey3()

    def streamInterpPrimaryKey0(self):
        tdLog.info(f"streamInterpPrimaryKey0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from st partition by tbname stream_options(max_delay(3s)) into streamt as select _irowts, interp(b) from st partition by tbname range(_twstart) fill(prev);"
        )
        tdStream.checkStreamStatus()
        tdSql.pause()

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 20, 20.0) (1648791217001, 4, 4, 4, 4.1);"
        )
        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(1, 1) == 20
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 20
            and tdSql.getData(4, 1) == 20,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname every(1s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")
        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 20
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 20
            and tdSql.getData(4, 1) == 20,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname, c every(1s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 10, 9.0);")
        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 10, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 20
            and tdSql.getData(2, 1) == 20
            and tdSql.getData(3, 1) == 20
            and tdSql.getData(4, 1) == 20,
        )

    def streamInterpPrimaryKey1(self):
        tdLog.info(f"streamInterpPrimaryKey1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(b) from st partition by tbname every(1s) fill(next);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 1
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 20, 20.0) (1648791217001, 40, 40, 40, 40.1);"
        )
        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 4, 4.1);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 20, 20.0) (1648791217001, 40, 40, 40, 40.1);"
        )
        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 4, 4.1);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname, c every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 10, 9.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 10, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 10, 20.0) (1648791217001, 40, 40, 10, 40.1);"
        )
        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

    def streamInterpPrimaryKey2(self):
        tdLog.info(f"streamInterpPrimaryKey2")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(b) from st partition by tbname every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 20, 20.0) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 16
            and tdSql.getData(2, 1) == 12
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 16
            and tdSql.getData(2, 1) == 12
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname, c every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 10, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 10, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 16
            and tdSql.getData(2, 1) == 12
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 4,
        )

    def streamInterpPrimaryKey3(self):
        tdLog.info(f"streamInterpPrimaryKey3")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(b) from st partition by tbname every(1s) fill(value, 100);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213009, 20, 20, 20, 20.0) (1648791217001, 4, 4, 4, 4.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 100
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname every(1s) fill(value, 100);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 9, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 30, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 100
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt(ts, b primary key) as select _irowts, interp(b) from st partition by tbname, c every(1s) fill(value, 100);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 9, 9, 10, 9.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 10, 10, 10, 10.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 30, 30, 10, 30.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
        )

        tdSql.execute(f"insert into t1 values(1648791217001, 4, 4, 10, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 20, 20, 10, 20.0);")

        tdLog.info(
            f"sql select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )
        tdSql.query(
            f"select _irowts, interp(b) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 9
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(2, 1) == 100
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 100,
        )
