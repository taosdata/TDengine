import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic2(self):
        """OldTsim: stream basic status

        1. Create snode
        2. Create multiple databases
        3. Create stream sliding window, tag stream, trigger interval 0, window close scenarios
        4. Insert data and verify the correctness of stream processing results
        5. Verify the status of all streams
        6. Check data correctness for all streams


        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/windowClose.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.PauseAndResume0())
        streams.append(self.PauseAndResume1())
        streams.append(self.Sliding0())
        streams.append(self.Sliding1())
        streams.append(self.Sliding2())
        streams.append(self.Sliding3())
        streams.append(self.Sliding4())
        streams.append(self.Tag0())
        # streams.append(self.Interval0()) update data
        streams.append(self.WindowClose1())
        streams.append(self.WindowClose2())
        streams.append(self.WindowClose3())
        streams.append(self.WindowClose4())
        streams.append(self.WindowClose5())
        streams.append(self.WindowClose6())

        tdStream.checkAll(streams)

    class PauseAndResume0(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume0"

        def create(self):
            tdSql.execute(f"create database pauseAndResume0 vgroups 1 buffer 8;")
            tdSql.execute(f"use pauseAndResume0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamt1 as select _twstart, _twend, count(*) c1, sum(a) c3 from %%trows;"
            )
            tdSql.execute(
                f"create stream stream1_same_dst interval(10s, 1s) sliding(10s) from st stream_options(max_delay(3s)) into streamt1 as select _twstart, _twend, count(*) c1, sum(a) c3 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"stop stream streams1;")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select status from information_schema.ins_streams where stream_name='streams1' and db_name='pauseandresume0';",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, "Stopped"),
            )

        def insert2(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 2) == 4,
            )

        def insert3(self):
            tdSql.query("start stream streams1;")

        def check3(self):
            tdStream.checkStreamStatus()

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 2 and tdSql.getData(0, 2) == 4,
            )

        def insert5(self):
            tdSql.execute(f"insert into ts1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts2 values(1648791223002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into ts3 values(1648791223002, 4, 2, 43, 73.1);")
            tdSql.execute(f"insert into ts4 values(1648791223002, 24, 22, 23, 4.1);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(1, 2) == 4
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(3, 2) == 4,
            )

    class PauseAndResume1(StreamCheckItem):
        def __init__(self):
            self.db = "pauseAndResume1"

        def create(self):
            tdSql.execute(f"create database pauseAndResume1 vgroups 1 buffer 8;")
            tdSql.execute(f"use pauseAndResume1;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams2 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt2 as select _twstart, count(*) c1, sum(a) c3 from %%trows;"
            )
            tdSql.execute(
                f"create stream if not exists streams2 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt2 as select _twstart, count(*) c1, sum(a) c3 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"stop stream streams2;")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select status from information_schema.ins_streams where stream_name='streams2' and db_name='pauseandresume1';",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, "Stopped"),
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 12, 3, 1.0);")

        def insert3(self):
            tdSql.query("start stream streams2;")

        def check3(self):
            tdStream.checkStreamStatus()

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 3, 1.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 2 and tdSql.getData(0, 1) == 1 and tdSql.getData(1, 1) == 1
            )

        def insert5(self):
            tdSql.execute(f"stop stream streams2;")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select status from information_schema.ins_streams where stream_name='streams2' and db_name='pauseandresume1';",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, "Stopped"),
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791233001, 1, 12, 3, 1.0);")

        def insert7(self):
            tdSql.query("start stream streams2;")

        def check7(self):
            tdStream.checkStreamStatus()

        def insert8(self):
            tdSql.execute(f"insert into t1 values(1648791243003, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791253003, 2, 2, 3, 1.1);")

        def check8(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 5
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(2, 0, "2022-04-01 13:33:50.000")
                and tdSql.compareData(2, 1, 1)
                and tdSql.compareData(3, 0, "2022-04-01 13:34:00.000")
                and tdSql.compareData(3, 1, 1)
                and tdSql.compareData(4, 0, "2022-04-01 13:34:10.000")
                and tdSql.compareData(4, 1, 1),
            )

    class Sliding0(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding0"

        def create(self):
            tdSql.execute(f"create database sliding0 vgroups 1 buffer 8;")

            tdSql.execute(f"use sliding0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding (5s)  from t1 stream_options(max_delay(3s))                 into streamt  as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams2 interval(10s) sliding (5s)  from t1 stream_options(max_delay(3s) | WATERMARK(1d)) into streamt2 as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream stream_t1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s))                 into streamtST as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream stream_t2 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s) | WATERMARK(1d)) into streamtST2 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791216000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791220000, 3, 2, 3, 2.1);")

            tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791216000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791220000, 3, 2, 3, 2.1);")

            tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791216000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791220000, 3, 2, 3, 2.1);")

            tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791216000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791220000, 3, 2, 3, 2.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 4
            )
            tdSql.checkResultsBySql(
                sql="select * from streamt",
                exp_sql="select _wstart, _wend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s) sliding (5s);",
            )
            tdSql.checkResultsBySql(
                sql="select * from streamtST",
                exp_sql="select _wstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st interval(10s) sliding (5s);",
            )

            tdSql.error(f"select * from streamtST2;")
            tdSql.error(f"select * from streamt2;")

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791216001, 2, 2, 3, 1.1);")

        def check2(self):
            tdSql.checkResultsBySql(
                sql="select * from streamt",
                exp_sql="select _wstart, _wend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s) sliding (5s);",
            )

            tdSql.checkResultsBySql(
                sql="select * from streamtST",
                exp_sql="select _wstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st interval(10s) sliding (5s);",
            )

            tdSql.error(f"select * from streamtST2;")
            tdSql.error(f"select * from streamt2;")

    class Sliding1(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding1"

        def create(self):
            tdSql.execute(f"create database sliding1 vgroups 1 buffer 8")
            tdSql.execute(f"use sliding1")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams11 interval(10s, 5s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams12 interval(10s, 5s) sliding(10s) from st stream_options(max_delay(3s)) into streamt2 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791243003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 2, 3, 4.1);")

            tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t2 values(1648791243003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t2 values(1648791213004, 4, 2, 3, 4.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 5
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 2
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 3
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 4,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt2",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 10
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 4
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 6
                and tdSql.getData(3, 1) == 2
                and tdSql.getData(3, 2) == 8,
            )

    class Sliding2(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding2"

        def create(self):
            tdSql.execute(f"create database sliding2 vgroups 2 buffer 8;")
            tdSql.execute(f"use sliding2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams21 interval(10s, 5s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt21 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams22 interval(10s, 5s) sliding(10s) from st stream_options(max_delay(3s)) into streamt22 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 3, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791243003, 4, 4, 4, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 5, 5, 4.1);")

            tdSql.execute(f"insert into t2 values(1648791213000, 1, 6, 6, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 2, 7, 7, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791233002, 3, 8, 8, 2.1);")
            tdSql.execute(f"insert into t2 values(1648791243003, 4, 9, 9, 3.1);")
            tdSql.execute(f"insert into t2 values(1648791213004, 4, 10, 10, 4.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt21;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 5
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 2
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 3
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 4,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt22;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 10
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 4
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 6
                and tdSql.getData(3, 1) == 2
                and tdSql.getData(3, 2) == 8,
            )

    class Sliding3(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding3"

        def create(self):
            tdSql.execute(f"create database sliding3 vgroups 2 buffer 8;")
            tdSql.execute(f"use sliding3;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams23 interval(20s) sliding(10s) from st stream_options(max_delay(3s)) into streamt23 as select _twstart, _twend, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 3, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791243003, 4, 4, 4, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 5, 5, 4.1);")

            tdSql.execute(f"insert into t2 values(1648791213000, 1, 6, 6, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 2, 7, 7, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791233002, 3, 8, 8, 2.1);")
            tdSql.execute(f"insert into t2 values(1648791243003, 4, 9, 9, 3.1);")
            tdSql.execute(f"insert into t2 values(1648791213004, 4, 10, 10, 4.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt23;",
                lambda: tdSql.getRows() == 5
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(3, 2) == 4
                and tdSql.getData(4, 2) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791343003, 4, 4, 4, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 5, 5, 4.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt23;",
                lambda: tdSql.getRows() == 15
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(3, 2) == 4
                and tdSql.getData(4, 2) == 2
                and tdSql.getData(13, 2) == 1
                and tdSql.getData(14, 2) == 1,
            )

    class Sliding4(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding4"

        def create(self):
            tdSql.execute(f"create database sliding4 vgroups 2 buffer 8;")
            tdSql.execute(f"use sliding4;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams4 interval(10s) sliding(5s) from st stream_options(max_delay(3s)) into streamt4 as select _twstart as ts, count(*), min(a) c1 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):

            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 2, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791273000, 3, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791313000, 4, 1, 1, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by 1;",
                lambda: tdSql.getRows() == 22
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(6, 1) == 1
                and tdSql.getData(6, 2) == 2
                and tdSql.getData(7, 1) == 1
                and tdSql.getData(7, 2) == 2
                and tdSql.getData(12, 1) == 1
                and tdSql.getData(12, 2) == 3
                and tdSql.getData(13, 1) == 1
                and tdSql.getData(13, 2) == 3
                and tdSql.getData(20, 1) == 1
                and tdSql.getData(20, 2) == 4
                and tdSql.getData(21, 1) == 1
                and tdSql.getData(21, 2) == 4,
            )

    class Tag0(StreamCheckItem):
        def __init__(self):
            self.db = "Tag0"

        def create(self):
            tdSql.execute(f"create database tag0 vgroups 2 buffer 8;")
            tdSql.execute(f"use tag0;")

            tdSql.execute(
                f"create table st1(ts timestamp, a int, b int, c int, d double) tags(x int);"
            )
            tdSql.execute(f"create table t1 using st1 tags(1);")
            tdSql.execute(f"create table t2 using st1 tags(2);")

            tdSql.execute(
                f"create stream streams1 interval(60s) sliding(60s) from st1 stream_options(max_delay(3s) | expired_time(5s)) into streamt as select _twstart as s, count(*) c1 from st1 where x >= 2 and ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t2 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
            )

        def insert2(self):
            tdSql.execute(f"alter table t1 set tag x=3;")

            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233009, 0, 3, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 9,
            )

        def insert3(self):
            tdSql.execute(f"alter table t1 set tag x=1;")
            tdSql.execute(f"alter table t2 set tag x=1;")

            tdSql.execute(f"insert into t1 values(1648791243000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243001, 9, 2, 2, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 0
                and tdSql.getData(1, 1) == 0,
            )

    class Interval0(StreamCheckItem):
        def __init__(self):
            self.db = "Interval0"

        def create(self):
            tdSql.execute(f"create database interval0 vgroups 1 buffer 8")

            tdSql.execute(f"use interval0")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from t1 into streamt as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223003, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 4,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 5,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791223004, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223004, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223005, 2, 2, 3, 1.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 7,
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 4, 2, 3, 3.1)")
            tdSql.execute(f"insert into t1 values(1648791213002, 4, 2, 3, 4.1);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 9
                and tdSql.getData(1, 1) == 9,
                retry=240
            )

    class WindowClose1(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose1"

        def create(self):
            tdSql.execute(f"create database windowClose1 vgroups 2 buffer 8;")
            tdSql.execute(f"use windowClose1;")

            tdSql.execute(f"create stable st(ts timestamp, a int, b int) tags(t int);")
            tdSql.execute(f"create table t1 using st tags(1);")
            tdSql.execute(f"create table t2 using st tags(2);")

            tdSql.execute(
                f"create stream stream2 interval(10s) sliding(10s) from st into streamt2 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream3 interval(10s) sliding(10s) from st stream_options(max_delay(5s)) into streamt3 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream4 interval(10s) sliding(10s) from t1 into streamt4 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream5 interval(10s) sliding(10s) from t1 stream_options(max_delay(5s)) into streamt5 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream6 session(ts, 10s) from st into streamt6 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream7 session(ts, 10s) from st stream_options(max_delay(5s)) into streamt7 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream8 session(ts, 10s) from t1 into streamt8 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream9 session(ts, 10s) from t1 stream_options(max_delay(5s)) into streamt9 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream10 state_window(b) from t1 into streamt10 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream stream11 state_window(b) from t1 stream_options(max_delay(5s)) into streamt11 as select _twstart, sum(a) from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1);")
            tdSql.execute(f"insert into t1 values(1648791213001, 2, 1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 3, 1);")
            tdSql.execute(f"insert into t1 values(1648791233000, 4, 2);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt4;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt5;",
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt6;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt7;",
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt8;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt9;",
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt10;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt11;",
                lambda: tdSql.getRows() == 2,
            )

    class WindowClose2(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose2"

        def create(self):
            tdSql.execute(f"create database WindowClose2 vgroups 4 buffer 8;")
            tdSql.execute(f"use WindowClose2;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream13 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt13 as select _twstart, sum(a), now from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            self.now02 = tdSql.getData(0, 2)
            self.now12 = tdSql.getData(1, 2)
            tdLog.info(f"now02:{self.now02}, now12:{self.now12}")

        def check4(self):
            tdLog.info(f"max delay 3s......... sleep 5s")

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == self.now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == self.now12)

    class WindowClose3(StreamCheckItem):
        def __init__(self):
            self.db = "windowClose3"

        def create(self):
            tdSql.execute(f"create database windowClose3 vgroups 4 buffer 8;")
            tdSql.execute(f"use windowClose3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream stream14 interval(10s) sliding(10s) from st partition by tbname stream_options(max_delay(3s))  into streamt14 as select _twstart, sum(a), now from %%trows"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 3, 1.1);")

            tdSql.execute(f"insert into t2 values(1648791213000, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223000, 4, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt14 order by 2;",
                lambda: tdSql.getRows() == 4,
            )

            self.now02 = tdSql.getData(0, 2)
            self.now12 = tdSql.getData(1, 2)
            self.now22 = tdSql.getData(2, 2)
            self.now32 = tdSql.getData(3, 2)

        def check4(self):
            tdLog.info(f"max delay 3s......... sleep 5s")

            tdSql.query(f"select * from streamt14 order by 2;")
            tdSql.checkAssert(tdSql.getData(0, 2) == self.now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == self.now12)
            tdSql.checkAssert(tdSql.getData(2, 2) == self.now22)
            tdSql.checkAssert(tdSql.getData(3, 2) == self.now32)

    class WindowClose4(StreamCheckItem):
        def __init__(self):
            self.db = "windowClose4"

        def create(self):
            tdSql.execute(f"create database windowClose4 vgroups 4 buffer 8;")
            tdSql.execute(f"use windowClose4;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream15 session(ts, 10s) from t1 stream_options(max_delay(3s)) into streamt13 as select _twstart, sum(a), now from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            self.now02 = tdSql.getData(0, 2)
            self.now12 = tdSql.getData(1, 2)

        def check4(self):
            tdLog.info(f"max delay 3s......... sleep 5s")

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == self.now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == self.now12)

    class WindowClose5(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose5"

        def create(self):
            tdSql.execute(f"create database windowClose5 vgroups 4 buffer 8;")
            tdSql.execute(f"use windowClose5;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream16 state_window(a) from t1 stream_options(max_delay(3s)) into streamt13 as select _twstart, sum(a), now from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            self.now02 = tdSql.getData(0, 2)
            self.now12 = tdSql.getData(1, 2)

        def check4(self):
            tdLog.info(f"max delay 5s......... sleep 6s")

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == self.now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == self.now12)

    class WindowClose6(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose6"

        def create(self):
            tdSql.execute(f"create database windowClose6 vgroups 2 buffer 8;")
            tdSql.execute(f"use windowClose6;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream17 event_window(start with a = 1 end with a = 9) from t1 stream_options(max_delay(3s)) into streamt13 as select _twstart, sum(a), now from %%trows"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791233001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233009, 9, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            self.now02 = tdSql.getData(0, 2)
            self.now12 = tdSql.getData(1, 2)

        def check4(self):
            tdLog.info(f"max delay 3s......... sleep 5s")

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == self.now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == self.now12)
