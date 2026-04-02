import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseFillHistory:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_fillhistory(self):
        """OldTsim: fill history

        Verify the correctness of historical data calculation results, as well as the calculation results at the boundary between historical and real-time computation.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillHistoryBasic1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillHistoryBasic2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillHistoryBasic3.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillHistoryBasic4.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillHistoryTransform.sim

        """

        tdStream.createSnode()

        self.fillHistoryBasic1()
        # self.fillHistoryBasic2()
        # self.fillHistoryBasic3()
        # self.fillHistoryBasic4()
        # self.fillHistoryTransform()

    def fillHistoryBasic1(self):
        tdLog.info(f"fillHistoryBasic1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream stream1 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)|fill_history_first) into streamt as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
        )
        tdStream.checkStreamStatus()
        tdSql.pause()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004, 4, 2, 3, 4.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 2
            and tdSql.getData(1, 4) == 2
            and tdSql.getData(1, 5) == 3
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 12
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223002, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(1, 3) == 24
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223003, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 36
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 1, 1, 1, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 2, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 3, 3, 3.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 6
            and tdSql.getData(1, 4) == 3
            and tdSql.getData(1, 5) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791233003, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 5, 6, 7, 8.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(2, 3) == 6
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213004, 4, 2, 3, 4.1) (1648791213006, 5, 4, 7, 9.1) (1648791213004, 40, 20, 30, 40.1) (1648791213005, 4, 2, 3, 4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 4
            and tdSql.getData(0, 3) == 50
            and tdSql.getData(0, 4) == 20
            and tdSql.getData(0, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791223004, 4, 2, 3, 4.1) (1648791233006, 5, 4, 7, 9.1) (1648791223004, 40, 20, 30, 40.1) (1648791233005, 4, 2, 3, 4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 4
            and tdSql.getData(1, 3) == 46
            and tdSql.getData(1, 4) == 20
            and tdSql.getData(1, 5) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(2, 3) == 15
            and tdSql.getData(2, 4) == 4
            and tdSql.getData(2, 5) == 3,
        )

        tdLog.info(f"=====over")

    def fillHistoryBasic2(self):
        tdLog.info(f"fillHistoryBasic2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdSql.execute(f"drop stream if exists stream_t1;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
        tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

        tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.execute(f"insert into ts1 values(1648791213002, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into ts2 values(1648791213002, NULL, NULL, NULL, NULL);")

        tdSql.execute(f"insert into ts3 values(1648791213002, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into ts4 values(1648791213002, NULL, NULL, NULL, NULL);")

        tdSql.execute(f"insert into ts1 values(1648791223002, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into ts1 values(1648791233003, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into ts2 values(1648791243004, 4, 2, 43, 73.1);")
        tdSql.execute(f"insert into ts1 values(1648791213002, 24, 22, 23, 4.1);")
        tdSql.execute(f"insert into ts1 values(1648791243005, 4, 20, 3, 3.1);")
        tdSql.execute(
            f"insert into ts2 values(1648791243006, 4, 2, 3, 3.1) (1648791243007, 4, 2, 3, 3.1) ;"
        )
        tdSql.execute(
            f"insert into ts1 values(1648791243008, 4, 2, 30, 3.1) (1648791243009, 4, 2, 3, 3.1)  (1648791243010, 4, 2, 3, 3.1)  ;"
        )
        tdSql.execute(
            f"insert into ts2 values(1648791243011, 4, 2, 3, 3.1) (1648791243012, 34, 32, 33, 3.1)  (1648791243013, 4, 2, 3, 3.1) (1648791243014, 4, 2, 13, 3.1);"
        )
        tdSql.execute(
            f"insert into ts1 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
        )
        tdSql.execute(
            f"insert into ts2 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
        )
        tdSql.execute(
            f"insert into ts1 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
        )

        tdSql.execute(f"insert into ts3 values(1648791223002, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into ts4 values(1648791233003, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into ts3 values(1648791243004, 4, 2, 43, 73.1);")
        tdSql.execute(f"insert into ts4 values(1648791213002, 24, 22, 23, 4.1);")
        tdSql.execute(f"insert into ts3 values(1648791243005, 4, 20, 3, 3.1);")
        tdSql.execute(
            f"insert into ts4 values(1648791243006, 4, 2, 3, 3.1) (1648791243007, 4, 2, 3, 3.1) ;"
        )
        tdSql.execute(
            f"insert into ts3 values(1648791243008, 4, 2, 30, 3.1) (1648791243009, 4, 2, 3, 3.1)  (1648791243010, 4, 2, 3, 3.1)  ;"
        )
        tdSql.execute(
            f"insert into ts4 values(1648791243011, 4, 2, 3, 3.1) (1648791243012, 34, 32, 33, 3.1)  (1648791243013, 4, 2, 3, 3.1) (1648791243014, 4, 2, 13, 3.1);"
        )
        tdSql.execute(
            f"insert into ts3 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
        )
        tdSql.execute(
            f"insert into ts4 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
        )
        tdSql.execute(
            f"insert into ts3 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
        )

        tdSql.execute(
            f"create stream stream_t1 trigger at_once fill_history 1 watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST1 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from st interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select * from streamtST1;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 52
            and tdSql.getData(0, 4) == 52
            and tdSql.getData(0, 5) == 13
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(1, 3) == 92
            and tdSql.getData(1, 4) == 22
            and tdSql.getData(1, 5) == 3
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(2, 3) == 32
            and tdSql.getData(2, 4) == 12
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 30
            and tdSql.getData(3, 2) == 30
            and tdSql.getData(3, 3) == 180
            and tdSql.getData(3, 4) == 42
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.query(
            f"select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5, avg(d) from st interval(10s);"
        )

        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3);")

        tdSql.execute(
            f"create stream stream_t2 trigger at_once fill_history 1 watermark 20s IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST1 as select _wstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6 from st interval(10s) ;"
        )
        tdStream.checkStreamStatus()
        tdSql.checkResultsByFunc(
            f"select * from streamtST1;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2,
        )

        # max, min selectivity
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream stream_t3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST3 as select ts, min(a) c6, a, b, c, ta, tb, tc from st interval(10s) ;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3);")

        tdSql.checkResultsByFunc(
            f"select * from streamtST3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 2,
        )

    def fillHistoryBasic3(self):
        tdLog.info(f"fillHistoryBasic3")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.execute(
            f"create stream streams2 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt2 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )
        tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t2 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 3
            and tdSql.getData(4, 1) == 4
            and tdSql.getData(4, 2) == 1,
        )

    def fillHistoryBasic4(self):
        tdLog.info(f"fillHistoryBasic4")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.execute(f"use test;")

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004, 4, 2, 3, 4.1);")

        tdLog.info(
            f"create stream stream2 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s)"
        )
        tdSql.execute(
            f"create stream stream2 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 2
            and tdSql.getData(1, 4) == 2
            and tdSql.getData(1, 5) == 3
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 12
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223002, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(1, 3) == 24
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223003, 12, 14, 13, 11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 36
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 1, 1, 1, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 2, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 3, 3, 3.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 6
            and tdSql.getData(1, 4) == 3
            and tdSql.getData(1, 5) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791233003, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 5, 6, 7, 8.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(2, 3) == 6
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213004, 4, 2, 3, 4.1) (1648791213006, 5, 4, 7, 9.1) (1648791213004, 40, 20, 30, 40.1) (1648791213005, 4, 2, 3, 4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 4
            and tdSql.getData(0, 3) == 50
            and tdSql.getData(0, 4) == 20
            and tdSql.getData(0, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791223004, 4, 2, 3, 4.1) (1648791233006, 5, 4, 7, 9.1) (1648791223004, 40, 20, 30, 40.1) (1648791233005, 4, 2, 3, 4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2, c3, c4, c5 from streamt;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 4
            and tdSql.getData(1, 3) == 46
            and tdSql.getData(1, 4) == 20
            and tdSql.getData(1, 5) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(2, 3) == 15
            and tdSql.getData(2, 4) == 4
            and tdSql.getData(2, 5) == 3,
        )

        tdLog.info(f"======over")

    def fillHistoryTransform(self):
        tdLog.info(f"fillHistoryTransform")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.execute(f"use test;")

        tdLog.info(f"=====step1")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000, 10, 2, 3, 1.0);")
        tdSql.execute(
            f"create stream stream0 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, sum(a) from t1 interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 4, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 7,
        )

        tdLog.info(f"=====step1 over")

        tdLog.info(f"=====step2")
        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"insert into t1 values(1648791213000, 10, 2, 3, 1.0);")
        tdSql.execute(
            f"create stream stream1 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart, sum(a) from st interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 4, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 7,
        )

        tdLog.info(f"=====step2 over")

        tdLog.info(f"=====step3")

        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"insert into t1 values(1648791213000, 10, 2, 3, 1.0);")

        tdSql.execute(
            f"create stream stream2 trigger at_once fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart, sum(a) from st partition by ta interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 10,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 4, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 7,
        )
