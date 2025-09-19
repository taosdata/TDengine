import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseContinueWindowClose:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_continue_window_close(self):
        """OldTsim: continue window close

        Verify the alternative approach to the original continuous window close trigger mode in the new streaming computation

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/nonblockIntervalBasic.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/nonblockIntervalHistory.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic1())
        streams.append(self.Basic2())
        streams.append(self.Basic3())
        streams.append(self.Basic4())
        streams.append(self.Basic5())
        streams.append(self.Basic6())
        streams.append(self.Basic7())
        streams.append(self.History1())
        streams.append(self.History2())
        streams.append(self.History3())
        tdStream.checkAll(streams)

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db = "Basic1"

        def create(self):
            tdSql.execute(f"create database basic1 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic1;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams_er1 session(ts, 10s) from st partition by tbname into streamt_et1 tags(tb varchar(32) as %%tbname) as select _twstart, count(*) c1, sum(b) c2 from %%tbname where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams_er2 state_window(a) from st partition by tbname into streamt_et2 tags(tb varchar(32) as %%tbname) as select _twstart, count(*) c1, sum(b) c2 from %%trows;"
            )
            tdSql.execute(
                f"create stream streams_er3 count_window(10) from st partition by tbname into streamt_et3 as select _twstart, count(*) c1, sum(b) c2 from st where tbname=%%tbname;"
            )
            tdSql.execute(
                f"create stream streams_er4 event_window(start with a = 0 end with b = 9) from st partition by tbname into streamt_et4 as select _twstart, count(*) c1, sum(b) c2 from %%trows;"
            )
            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st partition by tbname into streamt1 tags(tb varchar(32) as %%tbname) as select _twstart, count(*) c1, sum(b) c2 from %%tbname where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 2,
            )

            tdSql.checkResultsByFunc(
                f"show stables",
                lambda: tdSql.getRows() == 6,
            )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db = "Basic2"

        def create(self):
            tdSql.execute(f"create database basic2 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 interval(10s) sliding(5s) from st partition by tbname into streamt2 as select _twstart, count(*) c1, max(a) c2 from st where tbname=%%tbname and ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791214000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791215000, 3, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791219000, 4, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791220000, 5, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791420000, 6, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 42
                and tdSql.compareData(0, 0, "2022-04-01 13:33:25.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(1, 1, 4)
                and tdSql.compareData(2, 0, "2022-04-01 13:33:35.000")
                and tdSql.compareData(2, 1, 3)
                and tdSql.compareData(3, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(3, 1, 1)
                and tdSql.compareData(4, 0, "2022-04-01 13:33:45.000")
                and tdSql.compareData(4, 1, 0),
            )

    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db = "Basic3"

        def create(self):
            tdSql.execute(f"create database basic3 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic3;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams3 interval(10s) sliding(10s) from st into streamt3 as select _twstart, count(*) c1, sum(b) c2 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t2 values(1648791211000, 1, 2, 3);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db = "Basic4"

        def create(self):
            tdSql.execute(f"create database basic4 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic4;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams4 interval(10s) sliding(5s) from st into streamt4 as select _twstart, count(*) c1, max(a) c2 from %%trows;"
            )

            tdStream.checkStreamStatus()

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791214000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791215000, 3, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791219000, 4, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791220000, 5, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791420000, 6, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by 1, 2;",
                lambda: tdSql.getRows() == 42
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(3, 1) == 1,
            )

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db = "Basic5"

        def create(self):
            tdSql.execute(f"create database basic5 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic5;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams5 interval(10s) sliding(10s) from st partition by tb into streamt5 as select _twstart, count(*) c1, max(a) c2, %%1 as tb2 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 1, 3);")
            tdSql.execute(f"insert into t1 values(1648791214000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791215000, 3, 1, 3);")
            tdSql.execute(f"insert into t1 values(1648791219000, 4, 2, 3);")

            tdSql.execute(f"insert into t2 values(1648791211000, 1, 1, 3);")
            tdSql.execute(f"insert into t2 values(1648791214000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791215000, 3, 1, 3);")
            tdSql.execute(f"insert into t2 values(1648791219000, 4, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791220000, 5, 1, 3);")
            tdSql.execute(f"insert into t2 values(1648791220001, 6, 2, 3);")

            tdSql.execute(f"insert into t1 values(1648791420000, 6, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt5 where tb=1;",
                lambda: tdSql.getRows() == 21
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 4)
                and tdSql.compareData(0, 2, 4),
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt5 where tb=2;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 4)
                and tdSql.compareData(0, 2, 4),
            )

    class Basic6(StreamCheckItem):
        def __init__(self):
            self.db = "Basic6"

        def create(self):
            tdSql.execute(f"create database basic6 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic6;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f'create stream streams6 interval(10s) sliding(10s) from st partition by tbname, ta into streamt6 OUTPUT_SUBTABLE(concat("streams6-tbn-", cast(%%2 as varchar(10)) )) TAGS(dd varchar(100) as cast(%%2 as varchar(10))) as select _twstart, count(*) c1, max(b) c2 from %%trows;'
            )
            tdSql.execute(
                f'create stream streams7 interval(10s) sliding(10s) from st partition by ta         into streamt7 OUTPUT_SUBTABLE(concat("streams7-tbn-", cast(%%1 as varchar(10)) )) TAGS(dd varchar(100) as cast(%%1 as varchar(10))) as select _twstart, count(*) c1, max(b) c2 from %%trows;'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 1, 3);")
            tdSql.execute(f"insert into t2 values(1648791211000, 2, 2, 3);")

            tdSql.execute(f"insert into t1 values(1648791221000, 1, 3, 3);")
            tdSql.execute(f"insert into t2 values(1648791221000, 2, 4, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(f"show tables;", lambda: tdSql.getRows() == 6)
            tdSql.checkResultsByFunc(
                f"select * from streamt6;", lambda: tdSql.getRows() == 2
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt6 where dd=1;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt6 where dd=2;", lambda: tdSql.getRows() == 1
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt7;", lambda: tdSql.getRows() == 2
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt7 where dd=1;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt7 where dd=2;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="streamt6",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["c1", "BIGINT", 8, ""],
                    ["c2", "INT", 4, ""],
                    ["dd", "VARCHAR", 100, "TAG"],
                ],
            )
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="streamt7;",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["c1", "BIGINT", 8, ""],
                    ["c2", "INT", 4, ""],
                    ["dd", "VARCHAR", 100, "TAG"],
                ],
            )
            tdSql.checkResultsByFunc(
                f"select * from `streams6-tbn-1`;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from `streams7-tbn-1`;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from `streams6-tbn-2`;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from `streams7-tbn-2`;", lambda: tdSql.getRows() == 1
            )

    class Basic7(StreamCheckItem):
        def __init__(self):
            self.db = "Basic7"

        def create(self):
            tdSql.execute(f"create database basic7 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic7;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create table streamt8(ts timestamp, a bigint primary key, b int) tags(ta varchar(100));"
            )

            tdSql.execute(
                f"create stream streams8 interval(10s) sliding(10s) from st partition by tbname, ta into streamt8 tags(ta varchar(100) as cast(%%2 as varchar(100))) as select _twstart ts, count(*) a, max(b) b from %%trows;"
            )
            tdSql.execute(
                f"create stream streams9 interval(10s) sliding(10s) from st                         into streamt9(c1, c2 primary key, c3) as select _twstart, count(*) c1, max(b) c2 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 1, 3);")
            tdSql.execute(f"insert into t2 values(1648791211000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 1, 3, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt9 order by c2;",
                lambda: tdSql.getRows() > 0
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000"),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt8 where ta=1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt8",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )

        def insert2(self):
            tdSql.execute(f"insert into t2 values(1648791211001, 2, 4, 3);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt9 order by c2;",
                lambda: tdSql.getRows() > 0
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000"),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt8;", lambda: tdSql.getRows() == 1
            )

    class History1(StreamCheckItem):
        def __init__(self):
            self.db = "History1"

        def create(self):
            tdSql.execute(f"create database history1 vgroups 1 buffer 8;")
            tdSql.execute(f"use history1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(f"insert into t1 values(1648791111000, 1, 1, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791111000, 1, 3, 3);")
            tdSql.execute(f"insert into t2 values(1648791221000, 2, 4, 3);")

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st partition by tbname stream_options(fill_history(1648790221001)) into streamt1 as select _twstart, count(*) c1, sum(b) c2 from %%trows  ;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1 order by 1, 2;", lambda: tdSql.getRows() == 24
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tag_tbname='t1';",
                lambda: tdSql.getRows() == 12
                and tdSql.compareData(0, 0, "2022-04-01 13:31:50.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(11, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(11, 1, 1)
                and tdSql.compareData(11, 2, 2),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tag_tbname='t2';",
                lambda: tdSql.getRows() == 12
                and tdSql.compareData(0, 0, "2022-04-01 13:31:50.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(11, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(11, 1, 1)
                and tdSql.compareData(11, 2, 4),
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791221001, 3, 5, 3);")
            tdSql.execute(f"insert into t1 values(1648791241001, 3, 6, 3);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1 order by 1, 2;", lambda: tdSql.getRows() == 25
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tag_tbname='t1';",
                lambda: tdSql.getRows() == 13
                and tdSql.compareData(0, 0, "2022-04-01 13:31:50.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 1)
                and tdSql.compareData(11, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(11, 1, 2)
                and tdSql.compareData(11, 2, 7)
                and tdSql.compareData(12, 0, "2022-04-01 13:33:50.000")
                and tdSql.compareData(12, 1, 0)
                and tdSql.compareData(12, 2, None),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tag_tbname='t2';",
                lambda: tdSql.getRows() == 12
                and tdSql.compareData(0, 0, "2022-04-01 13:31:50.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 3)
                and tdSql.compareData(11, 0, "2022-04-01 13:33:40.000")
                and tdSql.compareData(11, 1, 1)
                and tdSql.compareData(11, 2, 4),
            )

    class History2(StreamCheckItem):
        def __init__(self):
            self.db = "History2"

        def create(self):
            tdSql.execute(f"create database history2 vgroups 1 buffer 8;")
            tdSql.execute(f"use history2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t3 using st tags(3, 3, 3);")

            tdSql.execute(f"insert into t1 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791224000, 2, 2, 3);")

            tdSql.execute(f"insert into t2 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791224000, 2, 2, 3);")

            tdSql.execute(f"insert into t3 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t3 values(1648791224000, 2, 2, 3);")

            tdSql.execute(
                f"create stream streams12 interval(1s) sliding(1s) from st partition by tbname stream_options(fill_history(1648790221000)) into streamt12 as select _twstart, count(a) c1, sum(b) c2, tbname as c3 from %%tbname where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams13 interval(1s) sliding(1s) from st partition by tbname                                             into streamt13 as select _twstart, count(a) c1, sum(b) c2, tbname as c3 from %%tbname where ts >= _twstart and ts < _twend;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t1';",
                lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2022-04-01 13:33:41.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, "t1")
                and tdSql.compareData(1, 0, "2022-04-01 13:33:42.000")
                and tdSql.compareData(1, 1, 0)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(1, 3, None)
                and tdSql.compareData(2, 0, "2022-04-01 13:33:43.000")
                and tdSql.compareData(2, 1, 0)
                and tdSql.compareData(2, 2, None)
                and tdSql.compareData(2, 3, None)
                and tdSql.compareData(3, 0, "2022-04-01 13:33:44.000")
                and tdSql.compareData(3, 1, 1)
                and tdSql.compareData(3, 2, 2)
                and tdSql.compareData(3, 3, "t1"),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t2';",
                lambda: tdSql.getRows() == 4,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t3';",
                lambda: tdSql.getRows() == 4,
            )

        def insert2(self):
            tdLog.info("==============")
            tdSql.execute(f"insert into t1 values(1648791224001, 2, 3, 5);")
            tdSql.execute(f"insert into t1 values(1648791225001, 2, 4, 6);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t1';",
                lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2022-04-01 13:33:41.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, "t1")
                and tdSql.compareData(1, 0, "2022-04-01 13:33:42.000")
                and tdSql.compareData(1, 1, 0)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(1, 3, None)
                and tdSql.compareData(2, 0, "2022-04-01 13:33:43.000")
                and tdSql.compareData(2, 1, 0)
                and tdSql.compareData(2, 2, None)
                and tdSql.compareData(2, 3, None)
                and tdSql.compareData(3, 0, "2022-04-01 13:33:44.000")
                and tdSql.compareData(3, 1, 2)
                and tdSql.compareData(3, 2, 5)
                and tdSql.compareData(3, 3, "t1"),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t2';",
                lambda: tdSql.getRows() == 4,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt12 where tag_tbname='t3';",
                lambda: tdSql.getRows() == 4,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt13;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:44.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 5)
                and tdSql.compareData(0, 3, "t1"),
            )

    class History3(StreamCheckItem):
        def __init__(self):
            self.db = "History3"

        def create(self):
            tdSql.execute(f"create database history3 vgroups 2 buffer 8;")
            tdSql.execute(f"use history3;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(f"insert into t1 values(1648791111000, 1, 1, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791111000, 1, 3, 3);")
            tdSql.execute(f"insert into t2 values(1648791221000, 2, 4, 3);")

            tdSql.execute(
                f"create stream streams3 interval(10s) sliding(10s) from st stream_options(fill_history(1648790221000)) into streamt3 as select _twstart, count(*) c1, sum(b) c2 from %%trows;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 where c2 is not NULL;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 6,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791221001, 3, 5, 3);")
            tdSql.execute(f"insert into t1 values(1648791241001, 3, 6, 3);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 where c2 is not NULL;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 11,
            )
