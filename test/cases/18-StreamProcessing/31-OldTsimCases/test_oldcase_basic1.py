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
        """OldTsim: stream basic

        Basic test cases for streaming, part 1

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic3.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic4.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/basic5.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0())
        streams.append(self.Basic10())
        streams.append(self.Basic11())
        streams.append(self.Basic12())
        streams.append(self.Basic13())
        streams.append(self.Basic14())
        streams.append(self.Basic15())
        streams.append(self.Basic20())
        streams.append(self.Basic40())
        streams.append(self.Basic41())
        streams.append(self.Basic42())
        streams.append(self.Basic43())
        streams.append(self.Basic50())
        streams.append(self.Basic51())
        streams.append(self.Basic52())
        streams.append(self.Basic53())

        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db = "basic0"

        def create(self):
            tdSql.execute(f"create database basic0 vgroups 1 buffer 8")
            tdSql.execute(f"use basic0")
            tdSql.execute(
                f"create table if not exists stb (ts timestamp, k int) tags (a int)"
            )
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1000)")
            tdSql.execute(f"create table ct2 using stb tags(2000)")
            tdSql.execute(f"create table ct3 using stb tags(3000)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            tdSql.execute(
                f"create stream s1 interval(10m) sliding(10m) from ct1 stream_options(max_delay(3s)) into outstb as select _twstart, min(k), max(k), sum(k) as sum_alias from ct1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(f"insert into ct1 values('2022-05-08 03:42:00.000', 234)")

        def check1(self):
            tdSql.checkResultsByFunc(
                sql='select * from information_schema.ins_tables where db_name="basic0" and table_name="outstb"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname="basic0",
                tbname="outstb",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["min(k)", "INT", 4, ""],
                    ["max(k)", "INT", 4, ""],
                    ["sum_alias", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql="select `_twstart`, `min(k)`, `max(k)`, sum_alias from outstb",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-05-08 03:40:00.000")
                and tdSql.compareData(0, 1, 234)
                and tdSql.compareData(0, 2, 234)
                and tdSql.compareData(0, 3, 234),
            )

        def insert2(self):
            tdSql.execute(f"insert into ct1 values('2022-05-08 03:43:00.000', -111)")

        def check2(self):
            tdSql.checkResultsByFunc(
                sql="select `_twstart`, `min(k)`, `max(k)`, sum_alias from outstb",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-05-08 03:40:00.000")
                and tdSql.compareData(0, 1, -111)
                and tdSql.compareData(0, 2, 234)
                and tdSql.compareData(0, 3, 123),
            )

        def insert3(self):
            tdSql.execute(f"insert into ct1 values('2022-05-08 03:53:00.000', 789)")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, `min(k)`, `max(k)`, sum_alias from outstb",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-05-08 03:40:00.000")
                and tdSql.compareData(0, 1, -111)
                and tdSql.compareData(0, 2, 234)
                and tdSql.compareData(0, 3, 123)
                and tdSql.compareData(1, 0, "2022-05-08 03:50:00.000")
                and tdSql.compareData(1, 1, 789)
                and tdSql.compareData(1, 2, 789)
                and tdSql.compareData(1, 3, 789),
            )

    class Basic10(StreamCheckItem):
        def __init__(self):
            self.db = "basic10"

        def create(self):
            tdSql.execute(f"create database basic10 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic10;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791243003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791213004, 4, 2, 3, 4.1);")

        def check1(self):
            tdSql.checkTableSchema(
                dbname="basic10",
                tbname="streamt;",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["c1", "BIGINT", 8, ""],
                    ["c2", "BIGINT", 8, ""],
                    ["c3", "BIGINT", 8, ""],
                    ["c4", "INT", 4, ""],
                    ["c5", "INT", 4, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(0, 3) == 5
                and tdSql.getData(0, 4) == 2
                and tdSql.getData(0, 5) == 3
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(1, 3) == 2
                and tdSql.getData(1, 4) == 2
                and tdSql.getData(1, 5) == 3
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 1
                and tdSql.getData(2, 3) == 3
                and tdSql.getData(2, 4) == 2
                and tdSql.getData(2, 5) == 3
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 1
                and tdSql.getData(3, 3) == 4
                and tdSql.getData(3, 4) == 2
                and tdSql.getData(3, 5) == 3,
            )

        def insert2(self):
            # update
            tdSql.execute(f"insert into t1 values(1648791223001, 12, 14, 13, 11.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(0, 3) == 5
                and tdSql.getData(0, 4) == 2
                and tdSql.getData(0, 5) == 3
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 1
                and tdSql.getData(1, 3) == 12
                and tdSql.getData(1, 4) == 14
                and tdSql.getData(1, 5) == 13
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 1
                and tdSql.getData(2, 3) == 3
                and tdSql.getData(2, 4) == 2
                and tdSql.getData(2, 5) == 3
                and tdSql.getData(3, 1) == 1
                and tdSql.getData(3, 2) == 1
                and tdSql.getData(3, 3) == 4
                and tdSql.getData(3, 4) == 2
                and tdSql.getData(3, 5) == 3,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223002, 12, 14, 13, 11.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 2
                and tdSql.getData(1, 3) == 24
                and tdSql.getData(1, 4) == 14
                and tdSql.getData(1, 5) == 13,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791223003, 12, 14, 13, 11.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 3
                and tdSql.getData(1, 3) == 36
                and tdSql.getData(1, 4) == 14
                and tdSql.getData(1, 5) == 13,
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 1, 1, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 2, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791223003, 3, 3, 3, 3.1);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 3
                and tdSql.getData(1, 3) == 6
                and tdSql.getData(1, 4) == 3
                and tdSql.getData(1, 5) == 1,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791233003, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 5, 6, 7, 8.1);")
            tdSql.execute(f"insert into t1 values(1648791233002, 3, 2, 3, 2.1);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 2
                and tdSql.getData(2, 3) == 6
                and tdSql.getData(2, 4) == 2
                and tdSql.getData(2, 5) == 3,
            )

        def insert7(self):
            tdSql.execute(
                f"insert into t1 values(1648791213004, 4, 2, 3, 4.1) (1648791213006, 5, 4, 7, 9.1) (1648791213004, 40, 20, 30, 40.1) (1648791213005, 4, 2, 3, 4.1);"
            )

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 4
                and tdSql.getData(0, 3) == 50
                and tdSql.getData(0, 4) == 20
                and tdSql.getData(0, 5) == 3,
            )

        def insert8(self):
            tdSql.execute(
                f"insert into t1 values(1648791223004, 4, 2, 3, 4.1) (1648791233006, 5, 4, 7, 9.1) (1648791223004, 40, 20, 30, 40.1) (1648791233005, 4, 2, 3, 4.1);"
            )

        def check8(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, c1, c2, c3, c4, c5 from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(1, 2) == 4
                and tdSql.getData(1, 3) == 46
                and tdSql.getData(1, 4) == 20
                and tdSql.getData(1, 5) == 1
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(2, 3) == 15
                and tdSql.getData(2, 4) == 4
                and tdSql.getData(2, 5) == 3,
            )

    class Basic11(StreamCheckItem):
        def __init__(self):
            self.db = "basic11"

        def create(self):
            tdSql.execute(f"create database basic11 vgroups 1 buffer 8;")
            tdSql.query(f"select * from information_schema.ins_databases;")

            tdSql.execute(f"use basic11;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t3 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t4 using st tags(2, 2, 2);")
            tdSql.execute(f"create table t5 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams2 interval(10s) sliding(10s) from st partition by tbname stream_options(max_delay(3s)) into streamt as select _twstart, count(*) c1, sum(a) c3, max(b) c4 from %%tbname where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams3 interval(10s) sliding(10s) from st partition by tbname stream_options(max_delay(3s)) into streamt3 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, now c5 from %%tbname where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, 1, 1, 1, 1.0) t2 values(1648791213000, 2, 2, 2, 2.0) t3 values(1648791213000, 3, 3, 3, 3.0) t4 values(1648791213000, 4, 4, 4, 4.0);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 4
            )

        def insert2(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, 5, 5, 5, 5.0) t2 values(1648791213000, 6, 6, 6, 6.0) t5 values(1648791213000, 7, 7, 7, 7.0);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c4 desc;",
                lambda: tdSql.getRows() == 5
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 7
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 5,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 8, 8, 8, 8.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c4 desc;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 8,
            )
            tdSql.checkResultsByFunc(
                f"select count(*) from streamt3;",
                lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 5,
            )

    class Basic12(StreamCheckItem):
        def __init__(self):
            self.db = "basic12"

        def create(self):
            tdSql.execute(f"create database basic12 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic12;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")

            tdSql.execute(
                f"create stream stream_t3 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamtST3 as select ts, min(a) c6, a, b, c, ta, tb, tc from ts1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST3;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 2) == 2,
            )

    class Basic13(StreamCheckItem):
        def __init__(self):
            self.db = "basic13"

        def create(self):
            tdSql.execute(f"create database basic13 vgroups 1 vgroups 8;")
            tdSql.execute(f"use basic13;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams4 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)) into streamt__4 as select _twstart, count(*) c1 from t1 where a > 5 and ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt__4;",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 1, 0),
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 6, 2, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt__4;",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 1, 1),
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt__4;",
                lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 1, 0),
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 10, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 10, 2, 3, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt__4;",
                lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 1, 0)
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(2, 1, 1),
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791233000, 2, 2, 3, 1.0);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt__4;",
                lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 1, 0)
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(2, 1, 0),
            )

    class Basic14(StreamCheckItem):
        def __init__(self):
            self.db = "basic14"

        def create(self):
            tdSql.execute(f"create database basic14 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic14;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")

            tdSql.execute(
                f"create stream streams5 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt5 as select _twstart, count(*), _twend, max(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams6 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt6 as select _twstart, count(*), _twend, max(a), _twstart as ts from %%trows;"
            )
            tdSql.error(
                f"create stream streams7 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt7 as select _twstart, count(*), _twstart, _twend, max(a) from %%trows;"
            )
            tdSql.error(
                f"create stream streams8 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt8 as select _twstart ts, count(*), _twstart ts, _twend, max(a) from %%trows;"
            )
            tdSql.error(
                f"create stream streams9 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt9 as select _twstart as ts, count(*), _twstart, _twend ts, max(a) from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt5;", lambda: tdSql.getRows() == 1
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt6;", lambda: tdSql.getRows() == 1
            )

    class Basic15(StreamCheckItem):
        def __init__(self):
            self.db = "basic15"

        def create(self):
            tdSql.execute(f"create database basic15 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic15;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")

            tdSql.execute(
                f"create stream streams7 interval(10s) sliding(10s) from ts1 stream_options(max_delay(3s)) into streamt7 as select _twstart, count(*) from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
            tdSql.error(f"insert into ts1 values(-1648791211000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt7;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )

        def insert2(self):
            tdSql.error(
                f"insert into ts1 values(-1648791211001, 1, 2, 3) (1648791211001, 1, 2, 3);"
            )

        def check2(self):
            tdSql.query(f"select _wstart, count(*) from ts1 interval(10s) ;")
            tdSql.checkResultsByFunc(
                f"select * from streamt7;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
            )

    class Basic20(StreamCheckItem):
        def __init__(self):
            self.db = "basic20"

        def create(self):
            tdSql.execute(f"create database basic20 vgroups 1 buffer 8")

            tdSql.execute(f"use basic20")
            tdSql.execute(
                f"create table if not exists stb (ts timestamp, k int) tags (a int)"
            )

            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1000)")
            tdSql.execute(f"create table ct2 using stb tags(2000)")
            tdSql.execute(f"create table ct3 using stb tags(3000)")

            tdSql.query(f"show tables")
            tdSql.checkRows(3)

            tdSql.error(
                f"create stream s_error interval(30s) sliding(30s) from stb into str_dst_st as select _wend, count(*) a from stb where _wstart > '2025-1-1';"
            )
            tdSql.execute(
                f"create stream s1 interval(10m) sliding(10m) from ct1 stream_options(max_delay(3s)) into outstb as select _twstart, min(k), max(k), sum(k) as sum_alias from %%trows"
            )

        def insert1(self):
            tdSql.execute(f"insert into ct1 values('2022-05-08 03:42:00.000', 234)")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, `min(k)`, `max(k)`, sum_alias from outstb",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 234
                and tdSql.getData(0, 2) == 234
                and tdSql.getData(0, 3) == 234,
            )

        def insert2(self):
            tdSql.execute(f"insert into ct1 values('2022-05-08 03:57:00.000', -111)")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select `_twstart`, `min(k)`, `max(k)`, sum_alias from outstb",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 234
                and tdSql.getData(0, 2) == 234
                and tdSql.getData(0, 3) == 234
                and tdSql.getData(1, 1) == -111
                and tdSql.getData(1, 2) == -111
                and tdSql.getData(1, 3) == -111,
            )

    class Basic40(StreamCheckItem):
        def __init__(self):
            self.db = "basic40"

        def create(self):
            tdSql.execute(f"create database basic40 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic40;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams0 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, count(*) c1 from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791214003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791215003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791216004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791217004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791218004, 4, 2, 3, 4.1);")

            tdSql.execute(f"insert into t1 values(1648791221004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791222004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791223004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791224004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791225005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791226005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791227005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791228005, 4, 2, 3, 4.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 18
            )

        def insert2(self):
            tdSql.execute(
                f"insert into t1 values(1648791231004, 4, 2, 3, 4.1) (1648791232004, 4, 2, 3, 4.1) (1648791233004, 4, 2, 3, 4.1) (1648791234004, 4, 2, 3, 4.1) (1648791235004, 4, 2, 3, 4.1) (1648791236004, 4, 2, 3, 4.1) (1648791237004, 4, 2, 3, 4.1) (1648791238004, 4, 2, 3, 4.1) (1648791239004, 4, 2, 3, 4.1) (1648791240004, 4, 2, 3, 4.1) (1648791241004, 4, 2, 3, 4.1) (1648791242004, 4, 2, 3, 4.1) (1648791243004, 4, 2, 3, 4.1);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 33
            )

    class Basic41(StreamCheckItem):
        def __init__(self):
            self.db = "basic41"

        def create(self):
            tdSql.execute(f"create database basic41 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic41;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")

            tdSql.execute(
                f"create stream streams2 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s) | waterMark(10s) | ignore_disorder) into streamt2 as select _twstart, count(*) c1 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791212001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into t1 values(1648791214003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791215003, 4, 2, 3, 3.1);")
            tdSql.execute(f"insert into t1 values(1648791216004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791217004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791218004, 4, 2, 3, 4.1);")

            tdSql.execute(f"insert into t1 values(1648791221004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791222004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791223004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791224004, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791225005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791226005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791227005, 4, 2, 3, 4.1);")
            tdSql.execute(f"insert into t1 values(1648791228005, 4, 2, 3, 4.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 8
                and tdSql.compareData(7, 0, "2022-04-01 13:33:38.000"),
            )

        def insert2(self):
            tdSql.execute(
                f"insert into t1 values(1648791231004, 4, 2, 3, 4.1) (1648791232004, 4, 2, 3, 4.1) (1648791233004, 4, 2, 3, 4.1) (1648791234004, 4, 2, 3, 4.1) (1648791235004, 4, 2, 3, 4.1) (1648791236004, 4, 2, 3, 4.1) (1648791237004, 4, 2, 3, 4.1) (1648791238004, 4, 2, 3, 4.1) (1648791239004, 4, 2, 3, 4.1) (1648791240004, 4, 2, 3, 4.1) (1648791241004, 4, 2, 3, 4.1) (1648791242004, 4, 2, 3, 4.1) (1648791243004, 4, 2, 3, 4.1);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 23
                and tdSql.compareData(22, 0, "2022-04-01 13:33:53.000"),
            )

    class Basic42(StreamCheckItem):
        def __init__(self):
            self.db = "basic42"

        def create(self):
            tdSql.execute(f"create database basic42 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic42;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams1 session(ts, 1s) from t1 stream_options(max_delay(3s)) into streamt1 as select _twstart, count(*) c1 from t1 where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791215000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791217000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791219000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791221000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791225000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791227000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791229000, 1, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791231000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791235000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791237000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791239000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791241000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791245000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791247000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791249000, 1, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791251000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791253000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791255000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791257000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791259000, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791261000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791263000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791265000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791267000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791269000, 1, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 30
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791211001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791215001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791217001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791219001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791221001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791225001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791227001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791229001, 1, 2, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(9, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791231001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791235001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791237001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791239001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791241001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791245001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791247001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791249001, 1, 2, 3, 1.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(10, 1) == 2
                and tdSql.getData(19, 1) == 2,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791251001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791253001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791255001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791257001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791259001, 1, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791261001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791263001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791265001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791267001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791269001, 1, 2, 3, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(20, 1) == 2
                and tdSql.getData(29, 1) == 2,
            )

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

        def insert2(self):
            tdSql.execute(
                f"insert into t1 values(1648791311001, 1, 1, 1, 1.1)  t2 values (1648791311001, 2, 2, 2, 2.1)  t3 values(1648791311001, 3, 3, 3, 3.1) t4 values(1648791311001, 4, 4, 4, 4.1)  t5 values (1648791311001, 5, 5, 5, 5.1)  t6 values(1648791311001, 6, 6, 6, 6.1);"
            )
            tdSql.execute(
                f"insert into t1 values(1648791311002, 1, 1, 1, 1.1)  t2 values (1648791311002, 2, 2, 2, 2.1)  t3 values(1648791311002, 3, 3, 3, 3.1) t4 values(1648791311002, 4, 4, 4, 4.1)  t5 values (1648791311002, 5, 5, 5, 5.1)  t6 values(1648791311002, 6, 6, 6, 6.1);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 600
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 200
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791211010, 1, 1, 1, 1.1)")
            tdSql.execute(f"insert into t2 values(1648791211020, 2, 2, 2, 2.1);")
            tdSql.execute(f"insert into t3 values(1648791211030, 3, 3, 3, 3.1);")
            tdSql.execute(f"insert into t4 values(1648791211040, 4, 4, 4, 4.1);")
            tdSql.execute(f"insert into t5 values(1648791211050, 5, 5, 5, 5.1);")
            tdSql.execute(f"insert into t6 values(1648791211060, 6, 6, 6, 6.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 600
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1;", lambda: tdSql.getRows() == 200
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt where tag_tbname='t1'",
                lambda: tdSql.getRows() == 100 and tdSql.compareData(0, 1, 2),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tb='1'",
                lambda: tdSql.getRows() == 100 and tdSql.compareData(0, 1, 2),
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt1 where tb='2'",
                lambda: tdSql.getRows() == 100 and tdSql.compareData(0, 1, 10),
            )

    class Basic50(StreamCheckItem):
        def __init__(self):
            self.db = "basic50"

        def create(self):
            tdSql.execute(f"create database basic50 vgroups 1 buffer 8;")
            tdSql.execute(f"use basic50;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )

            tdSql.execute(
                f"create stream streams3 state_window(a) from t1 stream_options(max_delay(3s)) into streamt3 as select _twstart, count(*) c1 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791215000, 3, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791217000, 4, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791219000, 5, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791221000, 6, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 7, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791225000, 8, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791227000, 9, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791229000, 10, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791231000, 11, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233000, 12, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791235000, 13, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791237000, 14, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791239000, 15, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791241000, 16, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243000, 17, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791245000, 18, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791247000, 19, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791249000, 20, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791251000, 21, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791253000, 22, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791255000, 23, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791257000, 24, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791259000, 25, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791261000, 26, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791263000, 27, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791265000, 28, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791267000, 29, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791269000, 30, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(9, 1) == 1,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791211001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791215001, 3, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791217001, 4, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791219001, 5, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791221001, 6, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 7, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791225001, 8, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791227001, 9, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791229001, 10, 2, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(9, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791231001, 11, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 12, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791235001, 13, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791237001, 14, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791239001, 15, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791241001, 16, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791243001, 17, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791245001, 18, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791247001, 19, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791249001, 20, 2, 3, 1.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(10, 1) == 2
                and tdSql.getData(19, 1) == 2,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791251001, 21, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791253001, 22, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791255001, 23, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791257001, 24, 2, 3, 1.1);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 30 and tdSql.getData(20, 1) == 2,
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791259001, 25, 2, 3, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791261001, 26, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791263001, 27, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791265001, 28, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791267001, 29, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791269001, 30, 2, 3, 1.0);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() == 30
                and tdSql.getData(20, 1) == 2
                and tdSql.getData(29, 1) == 2,
            )

    class Basic51(StreamCheckItem):
        def __init__(self):
            self.db = "basic51"

        def create(self):
            tdSql.execute(f"create database basic51 vgroups 4 buffer 8;")
            tdSql.execute(f"use basic51;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams4 interval(1s) sliding(1s) from st stream_options(max_delay(3s)) into streamt4 as select _twstart, first(a), b, c, ta, tb from st where ts >= _twstart and ts < _twend ;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 3, 4, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791215000, 3, 4, 5, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791217000, 4, 5, 6, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by 1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 2) == 2
                and tdSql.getData(0, 3) == 3
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 1
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(2, 3) == 5
                and tdSql.getData(2, 4) == 2
                and tdSql.getData(2, 5) == 2,
            )

    class Basic52(StreamCheckItem):
        def __init__(self):
            self.db = "basic52"

        def create(self):
            tdSql.execute(f"create database basic52 vgroups 2 buffer 8;")
            tdSql.execute(f"use basic52;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams5 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)) into streamt5 as select _twstart, b, c, ta, tb, max(b) from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213000, 2, 3, 4, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791215000, 3, 4, 5, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791217000, 4, 5, 6, 1.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt5 order by 1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 3
                and tdSql.getData(0, 3) == 1
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 5
                and tdSql.getData(2, 3) == 1
                and tdSql.getData(2, 4) == 1,
            )

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
