import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpPartitionBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_partitionby(self):
        """OldTsim: interp partition by

        Validate the calculation results of the ​​interp​​ function under ​​PARTITION BY​​ clauses

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy1.sim

        """

        tdStream.createSnode()

        self.streamInterpPartitionBy0()
        # self.streamInterpPartitionBy1()

    def streamInterpPartitionBy0(self):
        tdLog.info(f"streamInterpPartitionBy0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step prev")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from st partition by tbname, b, c stream_options(max_delay(3s)) into streamt as select _irowts, interp(a), _isfilled, tbname, b, c from st where tbname=%%tbname and b=%%2 and c=%%3 range(_twstart) fill(prev);"
        )

        tdSql.pause()
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791212001, 1, 0, 0, 1.0) (1648791217001, 2, 0, 0, 2.1) t2 values(1648791212000, 0, 1, 1, 0.0) (1648791212001, 1, 1, 1, 1.0) (1648791217001, 2, 1, 1, 2.1);"
        )

        tdSql.execute(
            f"insert into t3 values(1648791212000, 0, 2, 2, 0.0) (1648791212001, 1, 2, 2, 1.0) (1648791217001, 2, 2, 2, 2.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 0 and c = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 0 and c = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 1 and c =1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 1 and c = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 2 and c = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 2 and c = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(prev) order by b, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where b = 0 and c = 0 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 0 and c = 0 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"1 sql select * from streamt where b = 1 and c = 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 1 and c = 1 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"2 sql select * from streamt where b = 2 and c = 2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 2 and c = 2 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"step next")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _irowts, interp(a), _isfilled, tbname, b, c from st partition by tbname, b, c every(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791212001, 1, 0, 0, 1.0) (1648791217001, 2, 0, 0, 2.1) t2 values(1648791212000, 0, 1, 1, 0.0) (1648791212001, 1, 1, 1, 1.0) (1648791217001, 2, 1, 1, 2.1);"
        )

        tdSql.execute(
            f"insert into t3 values(1648791212000, 0, 2, 2, 0.0) (1648791212001, 1, 2, 2, 1.0) (1648791217001, 2, 2, 2, 2.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 0 and c = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 0 and c = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 1 and c =1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 1 and c = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 2 and c = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 2 and c = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(next) order by b, 1;"
        )

        tdLog.info(f"0 sql select * from streamt2 where b = 0 and c = 0 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 0 and c = 0 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 2
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 2
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"1 sql select * from streamt2 where b = 1 and c = 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 1 and c = 1 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 2
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 2
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"2 sql select * from streamt2 where b = 2 and c = 2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 2 and c = 2 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 2
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 2
            and tdSql.getData(5, 2) == 1,
        )

    def streamInterpPartitionBy1(self):
        tdLog.info(f"streamInterpPartitionBy1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step NULL")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, interp(a), _isfilled, tbname, b, c from st partition by tbname, b, c every(1s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791212001, 1, 0, 0, 1.0) (1648791217001, 2, 0, 0, 2.1) t2 values(1648791212000, 0, 1, 1, 0.0) (1648791212001, 1, 1, 1, 1.0) (1648791217001, 2, 1, 1, 2.1);"
        )

        tdSql.execute(
            f"insert into t3 values(1648791212000, 0, 2, 2, 0.0) (1648791212001, 1, 2, 2, 1.0) (1648791217001, 2, 2, 2, 2.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(NULL) order by b, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where b = 0 and c = 0 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 0 and c = 0 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == None
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == None
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == None
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == None
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == None
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"1 sql select * from streamt where b = 1 and c = 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 1 and c = 1 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == None
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == None
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == None
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == None
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == None
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"2 sql select * from streamt where b = 2 and c = 2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where b = 2 and c = 2 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == None
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == None
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == None
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == None
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == None
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(f"step linear")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _irowts, interp(a), _isfilled, tbname, b, c from st partition by tbname, b, c every(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791212000, 0, 0, 0, 0.0) (1648791212001, 10, 0, 0, 1.0) (1648791217001, 20, 0, 0, 2.1) t2 values(1648791212000, 0, 1, 1, 0.0) (1648791212001, 10, 1, 1, 1.0) (1648791217001, 20, 1, 1, 2.1);"
        )

        tdSql.execute(
            f"insert into t3 values(1648791212000, 0, 2, 2, 0.0) (1648791212001, 10, 2, 2, 1.0) (1648791217001, 20, 2, 2, 2.1);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 0 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )

        tdLog.info(f"0 sql select * from streamt2 where b = 0 and c = 0 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 0 and c = 0 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 11
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 13
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 17
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 19
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 1 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )

        tdLog.info(f"1 sql select * from streamt2 where b = 1 and c = 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 1 and c = 1 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 11
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 13
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 17
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 19
            and tdSql.getData(5, 2) == 1,
        )

        tdLog.info(
            f"sql select _irowts, interp(a), _isfilled, b from st where b = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )
        tdSql.query(
            f"select _irowts, interp(a), _isfilled, b from st where b = 2 partition by tbname, b, c range(1648791212000, 1648791217001) every(1s) fill(linear) order by b, 1;"
        )

        tdLog.info(f"2 sql select * from streamt2 where b = 2 and c = 2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 where b = 2 and c = 2 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(1, 1) == 11
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 13
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 1) == 17
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(5, 1) == 19
            and tdSql.getData(5, 2) == 1,
        )
