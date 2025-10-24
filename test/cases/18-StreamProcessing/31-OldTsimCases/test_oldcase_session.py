import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseSession:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_session(self):
        """OldTsim: session window

        Test the correctness of session windows

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/session0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/session1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/triggerSession0.sim

        """

        tdStream.createSnode()

        self.session0()
        # self.session1()
        # self.triggerSession0()

    def session0(self):
        tdLog.info(f"session0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(3)

        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, id int);"
        )
        tdSql.execute(
            f"create stream streams1 trigger session(ts, 10s) from t1 stream_options(max_delay(10s)) into streamt as select _twstart, count(*) c1, sum(a), max(a), min(d), stddev(a), last(a), first(d), max(id) s from t1 where ts >= _twstart and ts < _twend ;"
        )
        tdSql.pause()

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL, 1);"
        )
        tdSql.execute(f"insert into t1 values(1648791223001, 10, 2, 3, 1.1, 2);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1, 3);")
        tdSql.execute(
            f"insert into t1 values(1648791243003, NULL, NULL, NULL, NULL, 4);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791213002, NULL, NULL, NULL, NULL, 5) (1648791233012, NULL, NULL, NULL, NULL, 6);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(0, 4) == 2.100000000
            and tdSql.getData(0, 5) == 0
            and tdSql.getData(0, 6) == 3
            and tdSql.getData(0, 7) == 2.100000000
            and tdSql.getData(0, 8) == 6
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 10
            and tdSql.getData(1, 3) == 10
            and tdSql.getData(1, 4) == 1.100000000
            and tdSql.getData(1, 5) == 0
            and tdSql.getData(1, 6) == 10
            and tdSql.getData(1, 7) == 1.100000000
            and tdSql.getData(1, 8) == 5,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0, 7);")
        tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1, 8);")
        tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1, 9);")
        tdSql.execute(f"insert into t1 values(1648791243003, 4, 2, 3, 3.1, 10);")
        tdSql.execute(f"insert into t1 values(1648791213002, 4, 2, 3, 4.1, 11) ;")
        tdSql.execute(
            f"insert into t1 values(1648791213002, 4, 2, 3, 4.1, 12) (1648791223009, 4, 2, 3, 4.1, 13);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc ;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 7
            and tdSql.getData(0, 2) == 18
            and tdSql.getData(0, 3) == 4
            and tdSql.getData(0, 4) == 1.000000000
            # and tdSql.getData(0, 5) == 1.154700538
            and tdSql.getData(0, 6) == 4
            and tdSql.getData(0, 7) == 1.000000000
            and tdSql.getData(0, 8) == 13,
        )

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create table t2(ts timestamp, a int, b int, c int, d double, id int);"
        )
        tdSql.execute(
            f'create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   watermark 1d into streamt2 as select _wstart, apercentile(a, 30) c1, apercentile(a, 70), apercentile(a, 20,"t-digest") c2, apercentile(a, 60,"t-digest") c3, max(id) c4 from t2 session(ts, 10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t2 values(1648791213001, 1, 1, 3, 1.0, 1);")
        tdSql.execute(f"insert into t2 values(1648791213002, 2, 2, 6, 3.4, 2);")
        tdSql.execute(f"insert into t2 values(1648791213003, 4, 9, 3, 4.8, 3);")

        tdSql.execute(
            f"insert into t2 values(1648791233003, 3, 4, 3, 2.1, 4) (1648791233004, 3, 5, 3, 3.4, 5) (1648791233005, 3, 6, 3, 7.6, 6);"
        )
        time.sleep(1)
        tdSql.execute(f"insert into t2 values(1648791223003, 20, 7, 3, 10.1, 7);")

        tdSql.checkResultsByFunc(
            f"select * from streamt2 where c4=7;",
            lambda: tdSql.getRows() > 0
            # and tdSql.getData(0, 1) == 2.091607978
            # and tdSql.getData(0, 2) == 3.274823935
            # and tdSql.getData(0, 3) == 1.500000000
            and tdSql.getData(0, 3) > 1.4 and tdSql.getData(0, 3) < 1.6,
            # and tdSql.getData(0, 4) == 3.500000000,
        )

        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart, min(b), a, c from t1 session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart, max(b), a, c from t1 session(ts, 10s);"
        )
        # sql create stream streams5 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt5 as select _wstart, top(b, 3), a, c from t1 session(ts, 10s);
        # sql create stream streams6 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 as select _wstart, bottom(b, 3), a, c from t1 session(ts, 10s);
        # sql create stream streams7 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt7 as select _wstart, spread(a), elapsed(ts), hyperloglog(a) from t1 session(ts, 10s);
        tdSql.execute(
            f"create stream streams7 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt7 as select _wstart, spread(a), hyperloglog(a) from t1 session(ts, 10s);"
        )
        # sql create stream streams8 trigger at_once  watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt8 as select _wstart, histogram(a,"user_input", "[1, 3, 5, 7]", 1), histogram(a,"user_input", "[1, 3, 5, 7]", 0) from t1 session(ts, 10s);

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213001, 1, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 3, 2, 3.4);")
        tdSql.execute(f"insert into t1 values(1648791213003, 4, 9, 3, 4.8);")
        tdSql.execute(f"insert into t1 values(1648791213004, 4, 5, 4, 4.8);")

        tdSql.execute(f"insert into t1 values(1648791233004, 3, 4, 0, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791233005, 3, 0, 6, 3.4);")
        tdSql.execute(f"insert into t1 values(1648791233006, 3, 6, 7, 7.6);")
        tdSql.execute(f"insert into t1 values(1648791233007, 3, 13, 8, 7.6);")

        tdSql.execute(f"insert into t1 values(1648791223004, 20, 7, 9, 10.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() != 0,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4;",
            lambda: tdSql.getRows() != 0,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() != 0,
        )

    def session1(self):
        tdLog.info(f"session1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(3)

        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, id int);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, count(*) c1, sum(a), min(b), max(id) s from t1 session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 1, 1, 1.1, 1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 2, 2.1, 2);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 3, 3, 3.1, 3);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 4, 4, 4.1, 4);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 10
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791250005, 5, 5, 5, 5.1, 5);")
        tdSql.execute(f"insert into t1 values(1648791260006, 6, 6, 6, 6.1, 6);")
        tdSql.execute(f"insert into t1 values(1648791270007, 7, 7, 7, 7.1, 7);")
        tdSql.execute(
            f"insert into t1 values(1648791240005, 5, 5, 5, 5.1, 8) (1648791250006, 6, 6, 6, 6.1, 9);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(0, 2) == 32
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == 9
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 7
            and tdSql.getData(1, 3) == 7
            and tdSql.getData(1, 4) == 7,
        )

        tdSql.execute(
            f"insert into t1 values(1648791280008, 7, 7, 7, 7.1, 10) (1648791300009, 8, 8, 8, 8.1, 11);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791260007, 7, 7, 7, 7.1, 12) (1648791290008, 7, 7, 7, 7.1, 13) (1648791290009, 8, 8, 8, 8.1, 14);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791500000, 7, 7, 7, 7.1, 15) (1648791520000, 8, 8, 8, 8.1, 16) (1648791540000, 8, 8, 8, 8.1, 17);"
        )
        tdSql.execute(f"insert into t1 values(1648791530000, 8, 8, 8, 8.1, 18);")
        tdSql.execute(
            f"insert into t1 values(1648791220000, 10, 10, 10, 10.1, 19) (1648791290008, 2, 2, 2, 2.1, 20) (1648791540000, 17, 17, 17, 17.1, 21) (1648791500001, 22, 22, 22, 22.1, 22);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 29
            and tdSql.getData(0, 3) == 7
            and tdSql.getData(0, 4) == 22
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 33
            and tdSql.getData(1, 3) == 8
            and tdSql.getData(1, 4) == 21
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 25
            and tdSql.getData(2, 3) == 2
            and tdSql.getData(2, 4) == 20
            and tdSql.getData(3, 1) == 10
            and tdSql.getData(3, 2) == 54
            and tdSql.getData(3, 3) == 1
            and tdSql.getData(3, 4) == 19,
        )

        tdSql.execute(f"insert into t1 values(1648791000000, 1, 1, 1, 1.1, 23);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by s desc;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart, count(*) c1 from t1 where a > 5 session(ts, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 6, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000, 10, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 10, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791233000, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 1,
        )

        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams4 trigger at_once ignore update 0 ignore expired 0 into streamt4 as select _wstart, count(*) c1, count(a) c2 from st session(ts, 2s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791255100, 1, 2, 3);")
        tdSql.execute(f"insert into t1 values(1648791255300, 1, 2, 3);")
        tdSql.execute(
            f"insert into t1 values(1648791253000, 1, 2, 3) (1648791254000, 1, 2, 3);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 4,
        )

    def triggerSession0(self):
        tdLog.info(f"triggerSession0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0, 0)} {tdSql.getData(0, 1)} {tdSql.getData(0, 2)}")

        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t2(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t2 session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791222999, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791233001, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t2 values(1648791243002, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 5,
        )

        tdSql.execute(
            f"insert into t2 values(1648791223001, 1, 2, 3, 1.0) (1648791223002, 1, 2, 3, 1.0) (1648791222999, 1, 2, 3, 1.0);"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 6,
        )

        tdSql.execute(f"insert into t2 values(1648791233002, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t2 values(1648791253003, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )

        tdSql.execute(
            f"insert into t2 values(1648791243003, 1, 2, 3, 1.0) (1648791243002, 1, 2, 3, 1.0) (1648791270004, 1, 2, 3, 1.0) (1648791280005, 1, 2, 3, 1.0)  (1648791290006, 1, 2, 3, 1.0);"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )
