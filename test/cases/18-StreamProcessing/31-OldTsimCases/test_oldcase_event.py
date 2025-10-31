import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseEvent:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_event(self):
        """OldTsim: event window

        Test event window deletion and update

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/event0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/event1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/event2.sim

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Event00()) pass
        # streams.append(self.Event01()) pass
        # streams.append(self.Event02()) recalculate
        # streams.append(self.Event10()) pass
        # streams.append(self.Event11()) update
        # streams.append(self.Event12()) pass
        # streams.append(self.Event20()) recalculate
        # streams.append(self.Event21()) recalculate
        tdStream.checkAll(streams)

    class Event00(StreamCheckItem):
        def __init__(self):
            self.db = "Event00"

        def create(self):
            tdSql.execute(f"create database event00 vgroups 1 buffer 8;")
            tdSql.execute(f"use event00;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 event_window(start with a = 0 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(0, 2) == 6
                and tdSql.getData(0, 3) == 3,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791243006, 1, 1, 1, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791253000, 2, 2, 2, 1.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791243000, 0, 3, 3, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791253009, 9, 4, 4, 1.1);")

        def check4(self):
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

    class Event01(StreamCheckItem):
        def __init__(self):
            self.db = "Event01"

        def create(self):
            tdSql.execute(f"create database event01 vgroups 1 buffer 8;")
            tdSql.execute(f"use event01;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams2 event_window(start with a = 0 end with b = 9) from t1 stream_options(max_delay(3s)) into streamt2 as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213009, 1, 2, 2, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 9, 9, 9.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 0, 9, 9, 9.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 1,
            )

    class Event02(StreamCheckItem):
        def __init__(self):
            self.db = "Event02"

        def create(self):
            tdSql.execute(f"create database event02 vgroups 1 buffer 8;")
            tdSql.execute(f"use event02;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams3 event_window(start with a = 0 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt3 as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791233009, 1, 2, 2, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 0, 9, 9, 9.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 9, 9, 9.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 3,
            )

        def insert3(self):
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

    class Event10(StreamCheckItem):
        def __init__(self):
            self.db = "Event10"

        def create(self):
            tdSql.execute(f"create database event10 vgroups 1 buffer 8;")
            tdSql.execute(f"use event10;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 event_window(start with a = 0 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt1 as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 9, 9.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 3, 3, 3, 3.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
            )

    class Event11(StreamCheckItem):
        def __init__(self):
            self.db = "Event11"

        def create(self):
            tdSql.execute(f"create database event11 vgroups 1 buffer 8;")
            tdSql.execute(f"use event11;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams2 event_window(start with a = 0 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt2 as select _twstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 2, 2.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 1, 1, 4, 4.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 1, 5, 5.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 0,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 6, 6.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
            )

        def insert5(self):
            tdSql.execute(f"delete from t1 where ts = 1648791253000;")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 0,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791263000, 1, 9, 7, 7.0);")

        def insert7(self):
            tdSql.execute(f"delete from t1 where ts = 1648791243000;")

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
            )

    class Event12(StreamCheckItem):
        def __init__(self):
            self.db = "Event12"

        def create(self):
            tdSql.execute(f"create database event12 vgroups 1 buffer 8;")
            tdSql.execute(f"use event12;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams3 event_window(start with a = 0 end with a = 9) from st partition by tbname stream_options(max_delay(3s)) into streamt3 as select _twstart as s, count(*) c1, sum(b), max(c) from st where tbname=%%1 and ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 9, 2, 2.0);")
            tdSql.execute(f"insert into t2 values(1648791223000, 0, 3, 3, 3.0);")
            tdSql.execute(f"insert into t2 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791243000, 1, 9, 2, 2.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 3,
            )

        def insert2(self):
            tdLog.info(f"update data")
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 3, 3, 3.0);")
            tdSql.execute(f"insert into t2 values(1648791243000, 1, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 3, 3.0);")
            tdSql.execute(f"insert into t2 values(1648791253000, 1, 9, 3, 3.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4,
            )

    class Event20(StreamCheckItem):
        def __init__(self):
            self.db = "Event20"

        def create(self):
            tdSql.execute(f"create database event20 vgroups 1 buffer 8;")
            tdSql.execute(f"use event20;")

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
                f"create stream streams0 event_window(start with a = 0 end with a = 9) from st partition by tbname stream_options(max_delay(3s)|fill_history(1648791123000)) into streamt0 as select _twstart as s, count(*) c1, sum(b), max(c), _twend as e from st where tbname=%%tbname and ts >= _twstart and ts <= _twend;"
            )
            tdStream.checkStreamStatus()

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 13, 2.0);")
            tdSql.execute(f"insert into t2 values(1648791253000, 1, 9, 14, 2.0);")
            tdSql.execute(f"insert into t3 values(1648791253000, 1, 9, 15, 2.0);")
            tdSql.execute(f"insert into t4 values(1648791253000, 1, 9, 16, 2.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt0 order by 1, 2, 3, 4;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 3,
            )

        def insert2(self):
            tdSql.execute(f"insert into t3 values(1648791222000, 0, 1, 7, 3.0);")
            tdSql.execute(f"RECALCULATE STREAM streams1 from 1648791123000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt0 order by 1, 2, 3, 4;",
                lambda: tdSql.getRows() == 4 and tdSql.getData(0, 1) == 5,
            )

    class Event21(StreamCheckItem):
        def __init__(self):
            self.db = "Event21"

        def create(self):
            tdSql.execute(f"create database event21 vgroups 1 buffer 8;")
            tdSql.execute(f"use event21;")

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
                f"create stream streams0 event_window(start with a = 0 end with b = 9) from st partition by tbname stream_options(max_delay(3s)|fill_history(1648791123000)) into streamt0 as select _twstart as s, count(*) c1, sum(b), max(c), _twend as e from st where tbname=%%tbname and ts >= _twstart and ts <= _twend;"
            )
            tdStream.checkStreamStatus()

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791253000, 1, 9, 13, 2.0);")
            tdSql.execute(f"insert into t2 values(1648791253000, 1, 9, 14, 2.0);")
            tdSql.execute(f"insert into t3 values(1648791253000, 1, 9, 15, 2.0);")
            tdSql.execute(f"insert into t4 values(1648791253000, 1, 9, 16, 2.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt0 order by 1, 2, 3, 4;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t3 values(1648791222000, 0, 1, 7, 3.0);")
            tdSql.execute(f"RECALCULATE STREAM streams1 from 1648791123000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt0 order by 1, 2, 3, 4;",
                lambda: tdSql.getRows() == 4 and tdSql.getData(0, 1) == 5,
            )
