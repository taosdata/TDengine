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

        Basic test cases for streaming, part 2

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
        streams.append(self.Sliding0())
        tdStream.checkAll(streams)

    class Sliding0(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding0"

        def create(self):
            tdSql.execute(f"create database sliding0 vgroups 1 buffer 8;")
            tdSql.query(f"select * from information_schema.ins_databases;")
            tdSql.checkRows(3)

            tdSql.execute(f"use sliding0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s))                 into streamt  as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams2 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s) | WATERMARK(1d)) into streamt2 as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )
            # tdSql.execute(
            #     f"create stream stream_t1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s))                 into streamtST as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            # )
            # tdSql.execute(
            #     f"create stream stream_t2 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s) | WATERMARK(1d)) into streamtST2 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            # )

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
            
            tdSql.checkResultsByFunc(
                f"select * from streamt2;", lambda: tdSql.getRows() == 4
            )
            tdSql.checkResultsBySql(
                sql="select * from streamt2",
                exp_sql="select _wstart, _wend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s) sliding (5s);",
            )

            # tdSql.checkResultsByFunc(
            #     f"select * from streamtST",
            #     lambda: tdSql.getRows() >= 4
            #     and tdSql.getData(0, 1) == 2
            #     and tdSql.getData(0, 2) == 2
            #     and tdSql.getData(1, 1) == 4
            #     and tdSql.getData(1, 2) == 6
            #     and tdSql.getData(2, 1) == 4
            #     and tdSql.getData(2, 2) == 10
            #     and tdSql.getData(3, 1) == 2
            #     and tdSql.getData(3, 2) == 6,
            # )

            # tdSql.checkResultsByFunc(
            #     f"select * from streamtST2",
            #     lambda: tdSql.getRows() > 3
            #     and tdSql.getData(0, 1) == 2
            #     and tdSql.getData(0, 2) == 2
            #     and tdSql.getData(1, 1) == 4
            #     and tdSql.getData(1, 2) == 6
            #     and tdSql.getData(2, 1) == 4
            #     and tdSql.getData(2, 2) == 10
            #     and tdSql.getData(3, 1) == 2
            #     and tdSql.getData(3, 2) == 6,
            # )

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
