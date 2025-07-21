import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic1(self):
        """Stream basic test 1

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic3.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic4.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic5.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic53())

        tdStream.checkAll(streams)

    class Basic53(StreamCheckItem):
        def __init__(self):
            self.db = "basic53"

        def create(self):
            tdSql.execute(f"create database basic53 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic53;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams6 interval(1s) sliding(1s) from st stream_options(max_delay(3s)) into streamt6 as select _twstart, b, c, min(c), ta, tb from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams7 interval(1s) sliding(1s) from st stream_options(max_delay(3s)) into streamt7 as select ts, max(c) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams8 session(ts, 1s) from st stream_options(max_delay(3s))  into streamt8 as select ts, b, c, last(c), ta, tb from st where ts >= _twstart and ts <= _twend;;"
            )
            tdSql.execute(
                f"create stream streams9 state_window(a) from st partition by tbname stream_options(max_delay(3s)) into streamt9 as select ts, b, c, last_row(c), ta, tb from st where ts >= _twstart and ts <= _twend and tbname=%%1;"
            )
            tdSql.execute(
                f"create stream streams10 event_window(start with d = 0 end with d = 9) from st partition by tbname stream_options(max_delay(3s)) into streamt10 as select ts, b, c, last(c), ta, tb from st where ts >= _twstart and ts <= _twend and tbname=%%1;"
            )
            tdSql.execute(
                f"create stream streams11 count_window(2) from st partition by tbname stream_options(max_delay(3s) | expired_time(200s) | watermark(100s)) into streamt11 as select ts, b, c, last(c), ta, tb from st where ts >= _twstart and ts <= _twend and tbname=%%1;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 0);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 3, 4, 0);")
            tdSql.execute(f"insert into t2 values(1648791215000, 3, 4, 5, 0);")
            tdSql.execute(f"insert into t2 values(1648791217000, 4, 5, 6, 0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt6 order by 1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 3
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 1
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 5
                and tdSql.getData(2, 4) == 2
                and tdSql.getData(2, 5) == 2,
            )
