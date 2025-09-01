import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseState:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_state(self):
        """OldTsim: state window

        Test the correctness of state windows

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/state0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/state1.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.State00())
        # streams.append(self.State01())

        tdStream.checkAll(streams)

    class State00(StreamCheckItem):
        def __init__(self):
            self.db = "state00"

        def create(self):
            tdSql.execute(f"create database state00 vgroups 1 buffer 16;")

            tdSql.execute(f"use state00;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double, e int);"
            )

            sql = "create stream streams1 state_window(a) from t1 stream_options(max_delay(3s)) into streamt1 as select _twstart, _twend, _twrownum, count(*) c1, count(d) c2, sum(a) c3, max(a) c4, min(c) c5, max(e) c6 from t1 where ts >= _twstart and ts <= _twend;"
            tdSql.execute(sql)

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 4.0, 5);")
            tdSql.execute(f"insert into t1 values(1648791213002, 1, 12, 13, 14.0, 15);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from state00.streamt1;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:33.002")
                and tdSql.compareData(0, 2, 2)  # rownum
                and tdSql.compareData(0, 3, 2)  # count
                and tdSql.compareData(0, 4, 2)
                and tdSql.compareData(0, 5, 2)
                and tdSql.compareData(0, 6, 1)
                and tdSql.compareData(0, 7, 3)
                and tdSql.compareData(0, 8, 15),
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 22, 23, 24.0, 25);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from state00.streamt1",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, "2022-04-01 13:33:34.000")
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(0, 3, 3)
                and tdSql.compareData(0, 4, 3)
                and tdSql.compareData(0, 5, 3)
                and tdSql.compareData(0, 6, 1)
                and tdSql.compareData(0, 7, 3)
                and tdSql.compareData(0, 8, 25),
            )

        def insert2(self):
            return
            tdSql.execute(f"insert into t1 values(1648791213010, 2, 2, 3, 1.0, 4);")
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0, 5);")
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 2, 3, 1.0, 6);")

        def check2(self):
            return
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where c >=4 order by `_wstart`;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(0, 3) == 1
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 3
                and tdSql.getData(0, 6) == 5
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(1, 3) == 2
                and tdSql.getData(1, 4) == 2
                and tdSql.getData(1, 5) == 3
                and tdSql.getData(1, 6) == 4
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 1
                and tdSql.getData(2, 3) == 1
                and tdSql.getData(2, 4) == 1
                and tdSql.getData(2, 5) == 3
                and tdSql.getData(2, 6) == 6,
            )

        def insert3(self):
            return
            tdSql.execute(f"insert into t1 values(1648791213011, 1, 2, 3, 1.0, 7);")

        def check3(self):
            return
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where c in (5, 4, 7) order by `_wstart`;",
                lambda: tdSql.getRows() > 2
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 2
                and tdSql.getData(2, 3) == 2
                and tdSql.getData(2, 4) == 1
                and tdSql.getData(2, 5) == 3
                and tdSql.getData(2, 6) == 7,
            )

        def insert4(self):
            return
            tdSql.execute(f"insert into t1 values(1648791213011, 1, 2, 3, 1.0, 8);")

        def check4(self):
            return
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where c in (5, 4, 8) order by `_wstart`;",
                lambda: tdSql.getRows() > 2 and tdSql.getData(2, 6) == 8,
            )

        def insert5(self):
            return
            tdSql.execute(f"insert into t1 values(1648791213020, 1, 2, 3, 1.0, 9);")
            tdSql.execute(f"insert into t1 values(1648791213020, 3, 2, 3, 1.0, 10);")
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 2, 3, 1.0, 11);")
            tdSql.execute(
                f"insert into t1 values(1648791213011, 10, 20, 10, 10.0, 12);"
            )

        def check5(self):
            return

            tdSql.checkResultsByFunc(
                f"select * from streamt1 where c in (5, 4, 10, 11, 12) order by `_wstart`;",
                lambda: tdSql.getRows() > 4
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 1
                and tdSql.getData(2, 3) == 10
                and tdSql.getData(2, 4) == 10
                and tdSql.getData(2, 5) == 10
                and tdSql.getData(2, 6) == 12
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 1
                and tdSql.getData(3, 3) == 3
                and tdSql.getData(3, 4) == 3
                and tdSql.getData(3, 5) == 3
                and tdSql.getData(3, 6) == 10
                and tdSql.getData(4, 1) == 1
                and tdSql.getData(4, 2) == 1
                and tdSql.getData(4, 3) == 1
                and tdSql.getData(4, 4) == 1
                and tdSql.getData(4, 5) == 3
                and tdSql.getData(4, 6) == 11,
            )

        def insert6(self):
            return
            tdSql.execute(f"insert into t1 values(1648791213030, 3, 12, 12, 12.0, 13);")
            tdSql.execute(f"insert into t1 values(1648791214040, 1, 13, 13, 13.0, 14);")
            tdSql.execute(
                f"insert into t1 values(1648791213030, 3, 14, 14, 14.0, 15) (1648791214020, 15, 15, 15, 15.0, 16);"
            )

        def check6(self):
            return
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where c in (14, 15, 16) order by `_wstart`;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(0, 3) == 6
                and tdSql.getData(0, 4) == 3
                and tdSql.getData(0, 5) == 3
                and tdSql.getData(0, 6) == 15
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(1, 3) == 15
                and tdSql.getData(1, 4) == 15
                and tdSql.getData(1, 5) == 15
                and tdSql.getData(1, 6) == 16
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 1
                and tdSql.getData(2, 3) == 1
                and tdSql.getData(2, 4) == 1
                and tdSql.getData(2, 5) == 13
                and tdSql.getData(2, 6) == 14,
            )

    class State01(StreamCheckItem):
        def __init__(self):
            self.db = "state01"

        def create(self):

            tdSql.execute(f"create database test1 vgroups 1 buffer 16;")
            tdSql.query(f"select * from information_schema.ins_databases;")

            tdLog.info(
                f"{tdSql.getData(0, 0)} {tdSql.getData(0, 1)} {tdSql.getData(0, 2)}"
            )

            tdSql.execute(f"use test1;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double, e int);"
            )

            tdLog.info(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(a) c4, min(c) c5, max(e) c from t1 state_window(a);"
            )

            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(a) c4, min(c) c5, max(e) c from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791212000, 2, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 4, 1.0, 2);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1 order by c desc;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 5) == 4
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 5) == 3,
            )

    class State02(StreamCheckItem):
        def __init__(self):
            self.db = "state02"

        def create(self):
            tdSql.execute(f"create database test3 vgroups 1 buffer 16;")
            tdSql.execute(f"use test3;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double, e int);"
            )

            tdLog.info(
                f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart, count(*) c1, sum(b) c3 from t1 state_window(a);"
            )

            tdSql.execute(
                f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart, count(*) c1, sum(b) c3 from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791212000, 1, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791214000, 3, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791215000, 4, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791211000, 5, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791210000, 6, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791217000, 7, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791219000, 8, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791209000, 9, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791220000, 10, 2, 4, 1.0, 2);")

            tdSql.execute(f"insert into t1 values(1648791212000, 1, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791214000, 3, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791215000, 4, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791211000, 5, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791210000, 6, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791217000, 7, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791219000, 8, 2, 3, 1.0, 1);")
            tdSql.execute(f"insert into t1 values(1648791209000, 9, 2, 4, 1.0, 2);")
            tdSql.execute(f"insert into t1 values(1648791220000, 10, 2, 4, 1.0, 2);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 10,
            )

    class State03(StreamCheckItem):
        def __init__(self):
            self.db = "state03"

        def create(self):
            tdSql.execute(f"use test4;")
            tdSql.execute(
                f"create table st (ts timestamp, c1 tinyint, c2 smallint) tags (t1 tinyint) ;"
            )
            tdSql.execute(f"create table t1 using st tags (-81) ;")
            tdSql.execute(f"create table t2 using st tags (-81) ;")

            tdLog.info(
                f"create stream if not exists streams4 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart AS startts, min(c1), count(c1) from t1 state_window(c1);"
            )

            tdSql.execute(
                f"create stream if not exists streams4 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart AS startts, min(c1), count(c1) from t1 state_window(c1);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288209, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288210, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288211, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288212, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288213, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288214, 11);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288215, 29);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by startts;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 11
                and tdSql.getData(0, 2) == 6,
            )

        def insert2(self):
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288214 as timestamp);"
            )
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288216, 29);")
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288215 as timestamp);"
            )
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288217, 29);")
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288216 as timestamp);"
            )
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288218, 29);")
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288217 as timestamp);"
            )
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288219, 29);")
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288218 as timestamp);"
            )
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288220, 29);")
            tdSql.execute(
                f"delete from t1 where ts = cast(1668073288219 as timestamp);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by startts;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 11
                and tdSql.getData(0, 2) == 5,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288221, 65);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288222, 65);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288223, 65);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288224, 65);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288225, 65);")
            tdSql.execute(f"insert into t1 (ts, c1) values (1668073288226, 65);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by startts;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 11
                and tdSql.getData(0, 2) == 5
                and tdSql.getData(1, 1) == 29
                and tdSql.getData(1, 2) == 1,
            )

        def insert4(self):
            tdSql.execute("insert into t1 (ts, c1) values (1668073288224, 64);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by startts;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 11
                and tdSql.getData(0, 2) == 5
                and tdSql.getData(1, 1) == 29
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(2, 1) == 65
                and tdSql.getData(2, 2) == 3
                and tdSql.getData(3, 1) == 64
                and tdSql.getData(3, 2) == 1,
            )

    class State04(StreamCheckItem):
        def __init__(self):
            self.db = "state00"

        def create(self):
            tdSql.execute(f"create database test5 buffer 16;")
            tdSql.execute(f"use test5;")
            tdSql.execute(f"create table tb (ts timestamp, a int);")
            tdSql.execute(f"insert into tb values (now + 1m, 1 );")
            tdSql.execute(
                f"create table b (c timestamp, d int, e int, f int, g double);"
            )

            tdLog.info(
                f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3 from tb state_window(a);"
            )

            tdSql.execute(
                f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3 from tb state_window(a);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into b values(1648791213000, NULL, NULL, NULL, NULL);"
            )
            tdSql.query(f"select * from streamt order by c1, c2, c3;")

            tdSql.execute(
                f"insert into b values(1648791213000, NULL, NULL, NULL, NULL);"
            )
            tdSql.query(f"select * from streamt order by c1, c2, c3;")

            tdSql.execute(f"insert into b values(1648791213001, 1, 2, 2, 2.0);")
            tdSql.execute(f"insert into b values(1648791213002, 1, 3, 3, 3.0);")
            tdSql.execute(f"insert into tb values(1648791213003, 1);")

            tdSql.query(f"select * from streamt;")

            tdSql.execute(
                f"delete from b where c >= 1648791213001 and c <= 1648791213002;"
            )
            tdSql.execute(
                f"insert into b values(1648791223003, 2, 2, 3, 1.0); insert into b values(1648791223002, 2, 3, 3, 3.0);"
            )
            tdSql.execute(f"insert into tb values (now + 1m, 1 );")

            tdSql.query(f"select * from streamt;")

            tdSql.execute(f"insert into b(c, d) values (now + 6m, 6 );")
            tdSql.execute(
                f"delete from b where c >= 1648791213001 and c <= 1648791233005;;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select c2 from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 2,
            )

    class State10(StreamCheckItem):
        def __init__(self):
            self.db = "state10"

        def create(self):

            tdLog.info(f"step 1")
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database test vgroups 4 buffer 16;")
            tdSql.query(f"select * from information_schema.ins_databases;")
            tdSql.checkRows(3)

            tdSql.execute(f"use test;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double, e int);"
            )
            tdLog.info(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart, count(*) c1 from t1 state_window(a);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart, count(*) c1 from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1(ts) values(1648791213000);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 0
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 2, 3, 1.0, 3);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 1
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791215000, 2, 2, 3, 1.0, 4);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 2
            )

        def insert4(self):
            tdSql.execute(f"insert into t1(ts) values(1648791216000);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 2
            )

    class State11(StreamCheckItem):
        def __init__(self):
            self.db = "state11"

        def create(self):
            tdSql.execute(f"create database test2 vgroups 1 buffer 16;")
            tdSql.execute(f"use test2;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdLog.info(
                f"create stream streams2 trigger at_once  watermark 1000s IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart, count(*) c1, count(d) c2 from t1 partition by b state_window(a)"
            )
            tdSql.execute(
                f"create stream streams2 trigger at_once  watermark 1000s IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart, count(*) c1, count(d) c2 from t1 partition by b state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213010, 1, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;", lambda: tdSql.getRows() == 1
            )

        def insert2(self):
            tdLog.info(f"insert into t1 values(1648791213005, 2, 2, 3, 1.1)")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.1);")

        def check2(self):
            tdLog.info(f"select * from streamt2")
            tdSql.checkResultsByFunc(
                f"select * from streamt2;", lambda: tdSql.getRows() == 3
            )

    class State12(StreamCheckItem):
        def __init__(self):
            self.db = "state12"

        def create(self):

            tdSql.execute(f"create database test3 vgroups 1 buffer 16;")
            tdSql.execute(f"use test3;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
            tdLog.info(
                f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 fill_history 1 into streamt3 as select _wstart, max(a), count(*) c1 from t1 state_window(a);"
            )
            tdSql.execute(
                f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 fill_history 1 into streamt3 as select _wstart, max(a), count(*) c1 from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791203000, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 2, 3, 1.0);")

        def check1(self):
            tdLog.info(f"select * from streamt3")
            tdSql.checkResultsByFunc(
                f"select * from streamt3;", lambda: tdSql.getRows() == 2
            )
