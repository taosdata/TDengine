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
        streams.append(self.Basic43())

        tdStream.checkAll(streams)

    class Basic43(StreamCheckItem):
        def __init__(self):
            self.db = "basic43"

        def create(self):
            tdSql.execute(f"create database basic43 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic43;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )

            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t3 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t4 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t5 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t6 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams4 interval(1s) sliding(1s) from st partition by tbname stream_options(event_type(window_close)) into streamt  as select _twstart, count(*), now from  %%tbname where      ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams5 interval(1s) sliding(1s) from st partition by tb     stream_options(event_type(window_close)) into streamt1 as select _twstart, count(*), now from  st where tb=%%1 and ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791211000, 1, 1, 1, 1.1)  t2 values (1648791211000, 2, 2, 2, 2.1)  t3 values(1648791211000, 3, 3, 3, 3.1) t4 values(1648791211000, 4, 4, 4, 4.1)  t5 values (1648791211000, 5, 5, 5, 5.1)  t6 values(1648791211000, 6, 6, 6, 6.1);"
            )
            tdSql.execute(
                f"insert into t1 values(1648791311000, 1, 1, 1, 1.1)  t2 values (1648791311000, 2, 2, 2, 2.1)  t3 values(1648791311000, 3, 3, 3, 3.1) t4 values(1648791311000, 4, 4, 4, 4.1)  t5 values (1648791311000, 5, 5, 5, 5.1)  t6 values(1648791311000, 6, 6, 6, 6.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 600
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 200
            )

        # def insert2(self):
        #     tdSql.execute(
        #         f"insert into t1 values(1648791311001, 1, 1, 1, 1.1)  t2 values (1648791311001, 2, 2, 2, 2.1)  t3 values(1648791311001, 3, 3, 3, 3.1) t4 values(1648791311001, 4, 4, 4, 4.1)  t5 values (1648791311001, 5, 5, 5, 5.1)  t6 values(1648791311001, 6, 6, 6, 6.1);"
        #     )
        #     tdSql.execute(
        #         f"insert into t1 values(1648791311002, 1, 1, 1, 1.1)  t2 values (1648791311002, 2, 2, 2, 2.1)  t3 values(1648791311002, 3, 3, 3, 3.1) t4 values(1648791311002, 4, 4, 4, 4.1)  t5 values (1648791311002, 5, 5, 5, 5.1)  t6 values(1648791311002, 6, 6, 6, 6.1);"
        #     )

        # def check2(self):
        #     tdSql.checkResultsByFunc(
        #         f"select * from streamt order by 1 desc;",
        #         lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        #     )
        #     tdSql.checkResultsByFunc(
        #         f"select * from streamt1 order by 1 desc;",
        #         lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        #     )

        # def insert3(self):
        #     tdSql.execute(f"insert into t1 values(1648791211010, 1, 1, 1, 1.1)")
        #     tdSql.execute(f"insert into t2 values(1648791211020, 2, 2, 2, 2.1);")
        #     tdSql.execute(f"insert into t3 values(1648791211030, 3, 3, 3, 3.1);")
        #     tdSql.execute(f"insert into t4 values(1648791211040, 4, 4, 4, 4.1);")
        #     tdSql.execute(f"insert into t5 values(1648791211050, 5, 5, 5, 5.1);")
        #     tdSql.execute(f"insert into t6 values(1648791211060, 6, 6, 6, 6.1);")

        # def check3(self):
        #     tdSql.checkResultsByFunc(
        #         f"select * from streamt order by 1 desc;",
        #         lambda: tdSql.getRows() > 2
        #         and tdSql.getData(0, 1) == 2
        #         and tdSql.getData(1, 1) == 2
        #         and tdSql.getData(2, 1) == 2,
        #     )
        #     tdSql.checkResultsByFunc(
        #         f"select * from streamt1 order by 1 desc;",
        #         lambda: tdSql.getRows() > 2
        #         and tdSql.getData(0, 1) == 2
        #         and tdSql.getData(1, 1) == 2
        #         and tdSql.getData(2, 1) == 2,
        #     )
