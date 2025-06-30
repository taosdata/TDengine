import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseConcat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_concat(self):
        """Stream concat

        Test the use of the concat function in output_subtable and tags statements.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndCol0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag2.sim

        """

        tdStream.createSnode()

        self.udTableAndCol0()
        self.udTableAndTag0()
        self.udTableAndTag1()
        self.udTableAndTag2()

    class Col00(StreamCheckItem):
        def __init__(self):
            self.db = "Col00"

        def create(self):

            tdSql.execute("create database test vgroups 1;")
            tdSql.execute("use test;")
            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.error(
                "create stream streams1 interval(10s) sliding(10s) from st into streamt1(a, b, c, d) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams2 interval(10s) sliding(10s) from st into streamt2(a, b) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams3 interval(10s) sliding(10s) from st into streamt3(a, b) as select count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                "create stream streams4 interval(10s) sliding(10s) from st into streamt4(a, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                "create stream streams5 interval(10s) sliding(10s) from st partition by tbname into streamt5(a, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname;"
            )
            tdSql.execute(
                "create stream streams6 interval(10s) sliding(10s) from st partition by tbname into streamt6(a, b, c) tags(tbn varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname tbn;"
            )
            tdSql.execute(
                "create stream streams7 interval(10s) sliding(10s) from st partition by tbname into streamt7(a, b primary key, c) tags(tbn varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname tbn;"
            )
            tdSql.error(
                "create stream streams8 interval(10s) sliding(10s) from st into streamt8(a, b, c primary key) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams9 interval(10s) sliding(10s) from st into streamt9(a primary key, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams10 interval(10s) sliding(10s) from st into streamt10(a, b primary key, c) as select count(*) c1, max(a), max(b) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams11 interval(10s) sliding(10s) from st into streamt11(a, b, a) as select _twstart, count(*) c1, max(b) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams12 interval(10s) sliding(10s) from st partition by tbname into streamt12(a, b, c, d) tags(c varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a), max(b) from st where tbname=%%tbname and ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams13 interval(10s) sliding(10s) from st partition by tbname, tc options(max_delay(1s)) into streamt13(a, b, c, d) tags(tx varchar(60)) as select _twstart, count(*) c1, max(a) c2, max(b) from %%trows where ts >= _twstart and ts < _twend;"
            )
            tdSql.error(
                "create stream streams14 interval(10s) sliding(10s) from st partition by tbname, tc into streamt14 tags(tx varchar(60) as tc) as select _twstart, count(*) tc, max(a) c1, max(b) from st where tbname=%%tbname and tc=%%2 and ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                "create stream streams14 interval(10s) sliding(10s) from st partition by tbname, tc into streamt14 tags(tx int as tc) as select _twstart, count(*) tc, max(a) c1, max(b) from st where tbname=%%tbname and tc=%%2 and ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                "create stream streams15 interval(10s) sliding(10s) from st partition by tbname, tc into streamt15 tags(tx int as tc, tz varchar(50) as '12') as select _twstart, count(*) c1, max(a) from st where tbname=%%1 and tc=%%2 and ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                "create stream streams16 interval(10s) sliding(10s) from st partition by tbname, tc into streamt16 tags(tx int as tc, tb varchar(32) as %%tbname) as select _twstart, count(*) c1, max(a) from st where tbname=%%1 and tc=%%2 and ts >= _twstart and ts < _twend;"
            )

        def check1(self):
            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt5;",
                schema=[
                    ["a", "TIMESTAMP", 8, ""],
                    ["b", "BIGINT", 8, ""],
                    ["c", "INT", 4, ""],
                    ["tag_tbname", "VARCHAR", 270, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt6;",
                schema=[
                    ["a", "TIMESTAMP", 8, ""],
                    ["b", "BIGINT", 8, ""],
                    ["c", "INT", 4, ""],
                    ["tbn", "VARCHAR", 60, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt7;",
                schema=[
                    ["a", "TIMESTAMP", 8, ""],
                    ["b", "BIGINT", 8, ""],
                    ["c", "INT", 4, ""],
                    ["tbn", "VARCHAR", 60, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt14;",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["tc", "BIGINT", 8, ""],
                    ["c1", "INT", 4, ""],
                    ["max(b)", "INT", 4, ""],
                    ["tx", "INT", 4, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt15;",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["c1", "BIGINT", 8, ""],
                    ["max(a)", "INT", 4, ""],
                    ["tx", "INT", 4, "TAG"],
                    ["tz", "VARCHAR", 50, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt16;",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["c1", "BIGINT", 8, ""],
                    ["max(a)", "INT", 4, ""],
                    ["tx", "INT", 4, "TAG"],
                    ["tb", "VARCHAR", 32, "TAG"],
                ],
            )

            tdSql.checkTableSchema(
                dbname="test",
                tbname="streamt5;",
                schema=[
                    ["a", "TIMESTAMP", 8, ""],
                    ["b", "BIGINT", 8, ""],
                    ["c", "INT", 4, ""],
                    ["tag_tbname", "VARCHAR", 270, "TAG"],
                ],
            )

    class Col01(StreamCheckItem):
        def __init__(self):
            self.db = "Col01"

        def create(self):
            tdSql.execute("create database test1 vgroups 1;")
            tdSql.execute("use test1;")
            tdSql.execute(
                "create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                "create stream streams16 interval(10s) sliding(10s) from st into streamt16 as select _twstart, count(*) c1, max(a) from st partition by tbname tc state_window(b);"
            )
            tdSql.execute(
                "create stream streams17 interval(10s) sliding(10s) from st into streamt17 as select _twstart, count(*) c1, max(a) from st partition by tbname tc event_window start with a = 0 end with a = 9;"
            )
            tdSql.execute(
                "create stream streams18 interval(10s) sliding(10s) from st  options(watermark(10s)) into streamt18 as select _twstart, count(*) c1, max(a) from st partition by tbname tc count_window(2);"
            )

    class Col02(StreamCheckItem):
        def __init__(self):
            self.db = "Col02"

        def create(self):
            tdSql.execute("create database test2 vgroups 1;")
            tdSql.execute("use test2;")

            tdSql.execute("create table t1 (ts timestamp, a int, b int);")
            tdSql.execute(
                "create table rst(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
            )
            tdSql.execute('create table rct1 using rst tags("aa");')
            tdSql.execute(
                "create table rst6(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
            )
            tdSql.execute(
                "create table rst7(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
            )

            tdSql.execute(
                "create stream streams19 sliding(1s) from t1 into streamt19 as select ts, a, b from t1;"
            )
            tdSql.execute(
                "create stream streams20 sliding(1s) from t1 into streamt20(ts, a primary key, b) as select ts, a, b from t1;"
            )
            tdSql.error(
                "create stream streams21 sliding(1s) from t1 into rst as select ts, a, b from t1;"
            )
            tdSql.execute(
                "create stream streams22 sliding(1s) from rct1 into streamt22 as select ts, 1, b from rct1;"
            )
            tdSql.execute(
                "create stream streams23 sliding(1s) from rct1 into streamt23 as select ts, a, b from rct1;"
            )
            tdSql.execute(
                "create stream streams24 sliding(1s) from rct1 into streamt24(ts, a primary key, b) as select ts, a, b from rct1;"
            )
            tdSql.error(
                "create stream streams25 sliding(1s) from rct1 into rst6 as select ts, a, b from rct1;"
            )
            tdSql.error(
                "create stream streams26 sliding(1s) from rct1 into rst7 as select ts, 1, b from rct1;"
            )
            tdSql.execute(
                "create stream streams27 sliding(1s) from rct1 into streamt27(ts, a primary key, b) as select ts, 1, b from rct1;"
            )

    class Tag00(StreamCheckItem):
        def __init__(self):
            self.db = "Tag00"

        def create(self):
            tdSql.execute("create database result vgroups 1;")
            tdSql.execute("create database test vgroups 1;")
            tdSql.execute("use test;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                'create stream streams1 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result.streamt OUTPUT_SUBTABLE(concat("aaa-", %%tbname)) as select _twstart, count(*) c1 from %%tbname;'
            )

        def insert1(self):
            tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3);")
            tdSql.execute("insert into t2 values(1648791213000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                "select * from result.streamt;", lambda: tdSql.getRows() == 2
            )

    class Tag01(StreamCheckItem):
        def __init__(self):
            self.db = "Tag01"

        def create(self):

            tdSql.execute("create database result2 vgroups 1;")
            tdSql.execute("create database test2 vgroups 4;")
            tdSql.execute("use test2;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                'create stream streams2 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result2.streamt2 output_subtable(concat("tag-", %%1)) TAGS(cc varchar(100) as concat("tag-", %%tbname)) as select _twstart, count(*) c1 from %%trows;'
            )

        def insert1(self):
            tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3);")
            tdSql.execute("insert into t2 values(1648791213000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                'select tag_name from information_schema.ins_tags where db_name="result2" and stable_name = "streamt2" order by 1;',
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == "cc"
                and tdSql.getData(1, 0) == "cc",
            )

            tdSql.checkResultsByFunc(
                "select cc from result2.streamt2 order by 1;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == "tag-t1"
                and tdSql.getData(1, 0) == "tag-t2",
            )

            tdSql.checkResultsByFunc(
                "select * from result2.streamt2;",
                lambda: tdSql.getRows() == 2,
            )

    class Tag02(StreamCheckItem):
        def __init__(self):
            self.db = "Tag02"

        def create(self):
            tdSql.execute("create database result3 vgroups 1;")
            tdSql.execute("create database test3 vgroups 4;")
            tdSql.execute("use test3;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                'create stream streams3 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result3.streamt3 output_subtable(concat("tbn-", %%tbname)) tags(dd varchar(100) as concat("tag-", %%tbname)) as select _twstart, count(*) c1 from %%trows;'
            )

        def insert1(self):

            tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3);")
            tdSql.execute("insert into t2 values(1648791213000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                'select tag_name from information_schema.ins_tags where db_name="result3" and stable_name = "streamt3" order by 1;',
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == "dd"
                and tdSql.getData(1, 0) == "dd",
            )

            tdSql.checkResultsByFunc(
                "select dd from result3.streamt3 order by 1;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == "tag-t1"
                and tdSql.getData(1, 0) == "tag-t2",
            )

            tdSql.checkResultsByFunc(
                "select * from result3.streamt3;",
                lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                'select table_name from information_schema.ins_tables where db_name="result3" order by 1;',
                lambda: tdSql.getRows() == 2,
            )

    class Tag03(StreamCheckItem):
        def __init__(self):
            self.db = "Tag03"

        def create(self):
            tdSql.execute("create database result4 vgroups 1;")
            tdSql.execute("create database test4 vgroups 4;")
            tdSql.execute("use test4;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")
            tdSql.execute("create table t3 using st tags(3, 3, 3);")

            tdSql.execute(
                'create stream streams4 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result4.streamt4 OUTPUT_SUBTABLE(concat("tbn-", %%tbname)) TAGS(dd varchar(100) as concat("tag-", %%tbname)) as select _twstart, count(*) c1 from %%trows;'
            )

        def insert1(self):

            tdSql.execute(
                "insert into t1 values(1648791213000, 1, 1, 1) t2 values(1648791213000, 2, 2, 2) t3 values(1648791213000, 3, 3, 3);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                'select table_name from information_schema.ins_tables where db_name="result4" order by 1;',
                lambda: tdSql.getRows() == 3,
            )

            tdSql.checkResultsByFunc(
                "select * from result4.streamt4 order by 3;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == "tag-t1"
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == "tag-t2"
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == "tag-t3",
            )

    class Tag04(StreamCheckItem):
        def __init__(self):
            self.db = "Tag04"

        def create(self):
            tdSql.execute("create database result6 vgroups 1;")

            tdSql.execute("create database test6 vgroups 4;")
            tdSql.execute("use test6;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta varchar(20), tb int, tc int);"
            )
            tdSql.execute('create table t1 using st tags("1", 1, 1);')
            tdSql.execute('create table t2 using st tags("2", 2, 2);')
            tdSql.execute('create table t3 using st tags("3", 3, 3);')

            tdSql.execute(
                'create stream streams6 interval(10s) sliding(10s) from st partition by ta, tbname options(max_delay(1s)) into result6.streamt6 TAGS(dd int as cast(concat(%%1, "0") as int)) as select _twstart, count(*) c1 from %%trows;'
            )

        def insert1(self):
            tdSql.execute(
                "insert into t1 values(1648791213000, 1, 1, 1) t2 values(1648791213000, 2, 2, 2) t3 values(1648791213000, 3, 3, 3);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                "select * from result6.streamt6 order by 3;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 2) == 10
                and tdSql.getData(1, 2) == 20
                and tdSql.getData(2, 2) == 30,
            )

    class Tag10(StreamCheckItem):
        def __init__(self):
            self.db = "Tag10"

        def create(self):
            tdSql.execute("create database test5 vgroups 4;")
            tdSql.execute("use test5;")
            tdSql.execute(
                "create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute("create table streamt5(ts timestamp, a int, b int, c int);")

            tdSql.execute(
                "create stream streams5 interval(10s) sliding(10s) from t1 options(max_delay(1s)) into streamt5(ts, a, b, c) as select _twstart ts, cast(count(*) as int) a, cast(1000 as int) b, cast(NULL as int) c from t1;"
            )

        def insert1(self):
            tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
            tdSql.execute("insert into t1 values(1648791223001, 2, 2, 3, 1.1);")
            tdSql.execute("insert into t1 values(1648791233002, 3, 2, 3, 2.1);")
            tdSql.execute("insert into t1 values(1648791243003, 4, 2, 3, 3.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select * from streamt5;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 1000
                and tdSql.getData(3, 1) == 4
                and tdSql.getData(3, 2) == 1000,
            )

    class Tag20(StreamCheckItem):
        def __init__(self):
            self.db = "Tag20"

        def create(self):

            tdSql.execute("create database result vgroups 1;")
            tdSql.execute("create database test vgroups 4;")
            tdSql.execute("use test;")

            tdSql.execute(
                "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute("create table t1 using st tags(1, 1, 1);")
            tdSql.execute("create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                'create stream streams1 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result.streamt OUTPUT_SUBTABLE("aaa") as select _twstart, count(*) c1 from st ;'
            )

        def insert1(self):
            tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3);")
            tdSql.execute("insert into t2 values(1648791213000, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "aaa",
            )

            tdSql.checkResultsByFunc(
                "select * from result.streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
            )
