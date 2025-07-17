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
        """Stream basic test 2

        1.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/windowClose.sim

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Sliding0()) update bug
        # streams.append(self.Sliding1()) out of order bug
        streams.append(self.Sliding2())
        # streams.append(self.Sliding3()) update bug
        # streams.append(self.Tag0()) alter tag bug
        tdStream.checkAll(streams) 

    class PauseAndResume0(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume0"

        def create(self):
            tdSql.execute(f"drop stream if exists streams1;")
            tdSql.execute(f"drop database if exists test;")
            tdSql.execute(f"create database test vgroups 3;")
            tdSql.execute(f"use test;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt1 as select _twstart, count(*) c1, sum(a) c3 from st interval(10s);"
            )
            tdSql.error(
                f"create stream stream1_same_dst into streamt1 as select _wstart, count(*) c1, sum(a) c3 from st interval(10s);"
            )
            tdSql.execute(f"pause stream streams1;")

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 0,
            )

        def insert2(self):
            tdSql.query("resume stream streams1;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 4,
            )

        def insert3(self):
            tdSql.execute(f"insert into ts1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts2 values(1648791223002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into ts3 values(1648791223002, 4, 2, 43, 73.1);")
            tdSql.execute(f"insert into ts4 values(1648791223002, 24, 22, 23, 4.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4,
            )

    class PauseAndResume1(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume0"

        def create(self):
            tdSql.execute(f"create database test2 vgroups 1;")
            tdSql.execute(f"use test2;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select _twstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
            )
            tdSql.error(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select _twstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
            )
            tdSql.execute(
                f"create stream if not exists streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select _twstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
            )
            tdSql.execute(f"pause stream streams2;")

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 12, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 0,
            )

        def insert2(self):
            tdSql.query("resume stream streams2;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 3, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 1,
            )

        def insert4(self):
            tdSql.execute(f"pause stream streams2;")
            tdSql.execute(f"insert into t1 values(1648791223003, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233003, 2, 2, 3, 1.1);")
            tdSql.query("resume stream  IGNORE UNTREATED streams2;")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 1,
            )

    class PauseAndResume2(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume2"

        def create(self):
            tdSql.execute(f"create database test3 vgroups 3;")
            tdSql.execute(f"use test3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

            tdSql.execute(
                f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt3 as select _twstart, count(*) c1, sum(a) c3 from st interval(10s);"
            )
            tdSql.execute(
                f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt4 as select _twstart, count(*) c1, sum(a) c3 from st interval(10s);"
            )
            tdSql.execute(
                f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt5 as select _twstart, count(*) c1, sum(a) c3 from ts1 interval(10s);"
            )
            tdSql.execute(f"pause stream streams3;")

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

            tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4;",
                lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt5;",
                lambda: tdSql.getRows() == 1,
            )

        def insert2(self):
            tdSql.error(f"pause stream streams3333333;")
            tdSql.execute(f"pause stream IF EXISTS streams44444;")

            tdSql.error(f"resume stream streams5555555;")
            tdSql.query("resume stream IF EXISTS streams66666666;")

    class PauseAndResume3(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume3"

        def create(self):
            tdSql.execute(f"create database test6 vgroups 10;")
            tdSql.execute(f"use test6;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

            tdSql.execute(
                f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt6 as select _twstart, count(*) c1 from st interval(10s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

            tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

            tdSql.execute(f"pause stream streams6;")

            tdSql.execute(f"insert into ts1 values(1648791223001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791233001, 1, 12, 3, 1.0);")

            tdSql.query("resume stream streams6;")

            tdSql.execute(f"insert into ts3 values(1648791243001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791253001, 1, 12, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt6;",
                lambda: tdSql.getRows() == 5,
            )

    class PauseAndResume4(StreamCheckItem):
        def __init__(self):
            self.db = "PauseAndResume4"

        def create(self):
            tdSql.execute(f"create database test7 vgroups 1;")
            tdSql.execute(f"use test7;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")

            tdSql.execute(
                f"create stream streams8 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt8 as select _twstart, count(*) c1 from st interval(10s);"
            )
            tdSql.execute(
                f"create stream streams9 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt9 as select _twstart, count(*) c1 from st partition by tbname interval(10s);"
            )

        def insert1(self):
            # do nothing
            tdLog.debug("do nothing")

        def check1(self):
            tdSql.checkResultsByFunc(
                f'select status, * from information_schema.ins_streams where status != "ready";',
                lambda: tdSql.getRows() == 0,
            )

        def insert2(self):
            tdSql.execute(f"pause stream streams8;")
            tdSql.execute(f"pause stream streams9;")
            tdSql.execute(f"pause stream streams8;")
            tdSql.execute(f"pause stream streams9;")
            tdSql.execute(f"pause stream streams8;")
            tdSql.execute(f"pause stream streams9;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f'select status, * from information_schema.ins_stream_tasks where status != "paused";',
                lambda: tdSql.getRows() == 2,
            )
            tdSql.checkResultsByFunc(
                f'select status, * from information_schema.ins_streams where status == "paused";',
                lambda: tdSql.getRows() == 2,
            )

        def insert3(self):
            tdSql.query("resume stream streams8;")
            tdSql.query("resume stream streams9;")
            tdSql.query("resume stream streams8;")
            tdSql.query("resume stream streams9;")
            tdSql.query("resume stream streams8;")
            tdSql.query("resume stream streams9;")

        def check3(self):
            tdSql.checkResultsByFunc(
                f'select status, * from information_schema.ins_stream_tasks where status == "paused";',
                lambda: tdSql.getRows() == 0,
            )

            tdSql.checkResultsByFunc(
                f'select status, * from information_schema.ins_streams where status != "paused";',
                lambda: tdSql.getRows() == 2,
            )

        def insert4(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt8;",
                lambda: tdSql.getRows() == 1,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt9;",
                lambda: tdSql.getRows() == 1,
            )

    class Sliding0(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding0"

        def create(self):
            tdSql.execute(f"create database sliding0 vgroups 1 buffer 16;")
            tdSql.query(f"select * from information_schema.ins_databases;")
            tdSql.checkRows(3)

            tdSql.execute(f"use sliding0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams2 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s) | WATERMARK(1d)) into streamt2 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream stream_t1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s)) into streamtST as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
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
                retry=1,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 3
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 5
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 3,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamtST",
                lambda: tdSql.getRows() >= 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 10
                and tdSql.getData(3, 1) == 2
                and tdSql.getData(3, 2) == 6,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamtST2",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 10
                and tdSql.getData(3, 1) == 2
                and tdSql.getData(3, 2) == 6,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791216001, 2, 2, 3, 1.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 5
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(2, 2) == 7
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 3,
            )

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
                f"create stream streams11 interval(10s, 5s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts <= _twstart and ts > _twend;"
            )
            tdSql.execute(
                f"create stream streams12 interval(10s, 5s) sliding(10s) from st stream_options(max_delay(3s)) into streamt2 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts <= _twstart and ts > _twend;;"
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
            tdSql.execute(f"create database sliding3 vgroups 6;")
            tdSql.execute(f"use sliding3;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams23 interval(20s) sliding(10s) from st stream_options(max_delay(3s)) into streamt23 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
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
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 6
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 4
                and tdSql.getData(4, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791343003, 4, 4, 4, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 5, 5, 4.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt23;",
                lambda: tdSql.getRows() == 7
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 6
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 4
                and tdSql.getData(4, 1) == 2
                and tdSql.getData(5, 1) == 1
                and tdSql.getData(6, 1) == 1,
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
                f"create stream streams4 interval(10s) sliding(5s) stream_options(max_delay(3s)) into streamt4 as select _twstart as ts, count(*), min(a) c1 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):

            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 2, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791273000, 3, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791313000, 4, 1, 1, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by 1;",
                lambda: tdSql.getRows() == 8
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 2
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 2
                and tdSql.getData(4, 1) == 1
                and tdSql.getData(4, 2) == 3
                and tdSql.getData(5, 1) == 1
                and tdSql.getData(5, 2) == 3
                and tdSql.getData(6, 1) == 1
                and tdSql.getData(6, 2) == 4
                and tdSql.getData(7, 1) == 1
                and tdSql.getData(7, 2) == 4,
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
                f"create stream streams1 interval(60s) sliding(60s) from st1 stream_options(max_delay(3s) | expired_time(0s) | WATERMARK(100s)) into streamt as select _twstart as s, count(*) c1 from st1 where x >= 2 and ts >= _twstart and ts < _twend;"
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
                lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 3,
            )

        def insert2(self):
            tdSql.execute(f"alter table t1 set tag x=3;")

            tdSql.execute(f"insert into t1 values(1648791233000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233009, 0, 3, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 6,
            )

        def insert3(self):
            tdSql.execute(f"alter table t1 set tag x=1;")
            tdSql.execute(f"alter table t2 set tag x=1;")

            tdSql.execute(f"insert into t1 values(1648791243000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243001, 9, 2, 2, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 6,
            )

    class Interval0(StreamCheckItem):
        def __init__(self):
            self.db = "Interval0"

        def create(self):
            tdSql.execute(f"create database test vgroups 1")
            tdSql.query(f"select * from information_schema.ins_databases")
            tdSql.checkRows(3)

            tdSql.execute(f"use test")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams1 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 0,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223003, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 3,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791223004, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223004, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223005, 2, 2, 3, 1.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 5,
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 4, 2, 3, 3.1)")
            tdSql.execute(f"insert into t1 values(1648791213002, 4, 2, 3, 4.1);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 5,
            )

    class WindowClose0(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose0"

        def create(self):
            tdSql.execute(f"create database test vgroups 1;")
            tdSql.query(f"select * from information_schema.ins_databases")
            tdSql.checkRows(3)

            tdSql.execute(f"use test")
            tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
            tdSql.execute(f"create table tu1 using st tags(1);")
            tdSql.execute(f"create table tu2 using st tags(2);")

            tdSql.execute(
                f"create stream stream1 trigger window_close into streamt as select _twstart, sum(a) from st interval(10s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into tu1 values(now, 1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 0,
            )

    class WindowClose1(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose1"

        def create(self):
            tdSql.execute(f"create database test1 vgroups 4;")
            tdSql.execute(f"use test1;")
            tdSql.execute(f"create stable st(ts timestamp, a int, b int) tags(t int);")
            tdSql.execute(f"create table t1 using st tags(1);")
            tdSql.execute(f"create table t2 using st tags(2);")

            tdSql.execute(
                f"create stream stream2 trigger window_close into streamt2 as select _twstart, sum(a) from st interval(10s);"
            )
            tdSql.execute(
                f"create stream stream3 trigger max_delay 5s into streamt3 as select _twstart, sum(a) from st interval(10s);"
            )
            tdSql.execute(
                f"create stream stream4 trigger window_close into streamt4 as select _twstart, sum(a) from t1 interval(10s);"
            )
            tdSql.execute(
                f"create stream stream5 trigger max_delay 5s into streamt5 as select _twstart, sum(a) from t1 interval(10s);"
            )
            tdSql.execute(
                f"create stream stream6 trigger window_close into streamt6 as select _twstart, sum(a) from st session(ts, 10s);"
            )
            tdSql.execute(
                f"create stream stream7 trigger max_delay 5s into streamt7 as select _twstart, sum(a) from st session(ts, 10s);"
            )
            tdSql.execute(
                f"create stream stream8 trigger window_close into streamt8 as select _twstart, sum(a) from t1 session(ts, 10s);"
            )
            tdSql.execute(
                f"create stream stream9 trigger max_delay 5s into streamt9 as select _twstart, sum(a) from t1 session(ts, 10s);"
            )
            tdSql.execute(
                f"create stream stream10 trigger window_close into streamt10 as select _twstart, sum(a) from t1 state_window(b);"
            )
            tdSql.execute(
                f"create stream stream11 trigger max_delay 5s into streamt11 as select _twstart, sum(a) from t1 state_window(b);"
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
            tdSql.execute(f"create database test3 vgroups 4;")
            tdSql.execute(f"use test3;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream13 trigger max_delay 5s into streamt13 as select _twstart, sum(a), now from t1 interval(10s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            now02 = tdSql.getData(0, 2)
            now12 = tdSql.getData(1, 2)
            tdLog.info(f"now02:{now02}, now12:{now12}")

            tdLog.info(f"step1 max delay 5s......... sleep 6s")
            time.sleep(6)

            tdSql.query(f"select * from streamt13;")

            tdSql.checkAssert(tdSql.getData(0, 2) == now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == now12)

    class WindowClose3(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose3"

        def create(self):
            tdSql.execute(f"create database test4 vgroups 4;")
            tdSql.execute(f"use test4;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream stream14 trigger max_delay 5s into streamt14 as select _twstart, sum(a), now from st partition by tbname interval(10s);"
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

            now02 = tdSql.getData(0, 2)
            now12 = tdSql.getData(1, 2)
            now22 = tdSql.getData(2, 2)
            now32 = tdSql.getData(3, 2)

            tdLog.info(f"step2 max delay 5s......... sleep 6s")
            time.sleep(6)

            tdSql.query(f"select * from streamt14 order by 2;")
            tdSql.checkAssert(tdSql.getData(0, 2) == now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == now12)
            tdSql.checkAssert(tdSql.getData(2, 2) == now22)
            tdSql.checkAssert(tdSql.getData(3, 2) == now32)

    class WindowClose4(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose0"

        def create(self):
            tdSql.execute(f"create database test15 vgroups 4;")
            tdSql.execute(f"use test15;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream15 trigger max_delay 5s into streamt13 as select _twstart, sum(a), now from t1 session(ts, 10s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            now02 = tdSql.getData(0, 2)
            now12 = tdSql.getData(1, 2)

            tdLog.info(f"step1 max delay 5s......... sleep 6s")
            time.sleep(6)

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == now12)

    class WindowClose5(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose5"

        def create(self):
            tdSql.execute(f"create database test16 vgroups 4;")
            tdSql.execute(f"use test16;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream16 trigger max_delay 5s into streamt13 as select _twstart, sum(a), now from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 3, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 2,
            )

            now02 = tdSql.getData(0, 2)
            now12 = tdSql.getData(1, 2)

            tdLog.info(f"step1 max delay 5s......... sleep 6s")
            time.sleep(6)

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == now12)

    class WindowClose6(StreamCheckItem):
        def __init__(self):
            self.db = "WindowClose6"

        def create(self):
            tdSql.execute(f"create database test17 vgroups 4;")
            tdSql.execute(f"use test17;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream stream17 trigger max_delay 5s into streamt13 as select _twstart, sum(a), now from t1 event_window start with a = 1 end with a = 9;"
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

            now02 = tdSql.getData(0, 2)
            now12 = tdSql.getData(1, 2)

            tdLog.info(f"step1 max delay 5s......... sleep 6s")
            time.sleep(6)

            tdSql.query(f"select * from streamt13;")
            tdSql.checkAssert(tdSql.getData(0, 2) == now02)
            tdSql.checkAssert(tdSql.getData(1, 2) == now12)
