import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseEvent:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_event(self):
        """Stream event

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event2.sim

        """

        tdStream.createSnode()

        self.event0()
        # self.event1()
        # self.event2()

    def event0(self):
        tdLog.info(f"event0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 event_window(start with a = 0 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts < _twend;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791243006, 1, 1, 1, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791253000, 2, 2, 2, 1.1);")
        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791243000, 0, 3, 3, 1.1);")
        tdLog.info(f"3 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791253009, 9, 4, 4, 1.1);")
        tdLog.info(f"4 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 10
            and tdSql.getData(1, 3) == 4,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database test2")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 1, 2, 2, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791223000, 0, 9, 9, 9.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 0, 9, 9, 9.0);")
        tdLog.info(f"sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 1,
        )

        tdLog.info(f"step3")
        tdLog.info(f"=============== create database test3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791233009, 1, 2, 2, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000, 0, 9, 9, 9.0);")
        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 9, 9, 9.0);")
        tdLog.info(f"2 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 9, 9, 9.0);")
        tdLog.info(f"3 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 3,
        )

    def event1(self):
        tdLog.info(f"event1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database test1")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 9, 9.0);")
        tdSql.execute(f"insert into t1 values(1648791223000, 3, 3, 3, 3.0);")
        tdLog.info(f"1 sql select * from streamt1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database test2")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 3, 3, 3.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 2, 2.0);")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 1, 4, 4.0);")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791243000, 1, 1, 5, 5.0);")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 6, 6.0);")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791253000;")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791263000, 1, 9, 7, 7.0);")
        tdSql.execute(f"delete from t1 where ts = 1648791243000;")
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database test3")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 3, 3, 3.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 2, 2.0);")
        tdSql.execute(f"insert into t2 values(1648791223000, 0, 3, 3, 3.0);")
        tdSql.execute(f"insert into t2 values(1648791233000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791243000, 1, 9, 2, 2.0);")
        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 3,
        )

        tdLog.info(f"update data")
        tdSql.execute(f"insert into t1 values(1648791243000, 1, 3, 3, 3.0);")
        tdSql.execute(f"insert into t2 values(1648791243000, 1, 3, 3, 3.0);")
        tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 3, 3.0);")
        tdSql.execute(f"insert into t2 values(1648791253000, 1, 9, 3, 3.0);")
        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4,
        )

    def event2(self):
        tdLog.info(f"event2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database test")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(3, 3, 3);")
        tdSql.execute(f"create table t4 using st tags(3, 3, 3);")

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 0, 2, 2, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791243000, 1, 3, 3, 3.0);")
        tdSql.execute(f"insert into t2 values(1648791223000, 0, 1, 4, 3.0);")
        tdSql.execute(f"insert into t2 values(1648791233000, 0, 2, 5, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791243000, 1, 3, 6, 2.0);")
        tdSql.execute(f"insert into t3 values(1648791223000, 1, 1, 7, 3.0);")
        tdSql.execute(f"insert into t3 values(1648791233000, 1, 2, 8, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791243000, 1, 3, 9, 2.0);")
        tdSql.execute(f"insert into t4 values(1648791223000, 1, 1, 10, 3.0);")
        tdSql.execute(f"insert into t4 values(1648791233000, 0, 2, 11, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791243000, 1, 9, 12, 2.0);")

        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 fill_history 1 into streamt0 as select _wstart as s, count(*) c1, sum(b), max(c), _wend as e from st partition by tbname event_window start with a = 0 end with b = 9;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 13, 2.0);")
        tdSql.execute(f"insert into t2 values(1648791253000, 1, 9, 14, 2.0);")
        tdSql.execute(f"insert into t3 values(1648791253000, 1, 9, 15, 2.0);")
        tdSql.execute(f"insert into t4 values(1648791253000, 1, 9, 16, 2.0);")
        tdLog.info(f"1 sql select * from streamt0 order by 1, 2, 3, 4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt0 order by 1, 2, 3, 4;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 2,
        )

        tdSql.execute(f"insert into t3 values(1648791222000, 0, 1, 7, 3.0);")

        tdLog.info(f"2 sql select * from streamt0 order by 1, 2, 3, 4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt0 order by 1, 2, 3, 4;",
            lambda: tdSql.getRows() == 4 and tdSql.getData(0, 1) == 5,
        )
