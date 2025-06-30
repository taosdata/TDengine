import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseCheck:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_check(self):
        """Stream check stable

        Verify the computation results of streams when triggered by different windows.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable1.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0())
        # streams.append(self.Basic10())
        tdStream.checkAll(streams)

    class STable00(StreamCheckItem):
        def __init__(self):
            self.db = "STable00"

        def create(self):
            tdSql.execute(f"create database result vgroups 1;")
            tdSql.execute(f"create database test vgroups 4;")
            tdSql.execute(f"use test;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stable result.streamt0(ts timestamp, a bigint, b int) tags(tag_tbname varchar(270), ta int, tb int, tc int);"
            )

            tdSql.execute(
                "create stream streams0 interval(10s) sliding(10s) from st partition by tbname, ta, tb, tc options(MAX_DELAY(1s)) into result.streamt0 tags(tag_tbname varchar(270) as %%1, ta int as %%2, tb int as %%3, tc int as %%4) as select _twstart ts, count(*) a, max(a) b from %%tbname;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsBySql(
                sql="select * from  result.streamt0 where tag_tbname='t1'",
                exp_sql="select _wstart, count(*) c1, max(a) c2, tbname, ta, tb, tc from st where tbname='t1' partition by tbname interval(10s)",
            )
            tdSql.checkResultsBySql(
                sql="select * from  result.streamt0 where tag_tbname='t2'",
                exp_sql="select _wstart, count(*) c1, max(a) c2, tbname, ta, tb, tc from st where tbname='t2' partition by tbname interval(10s)",
            )
            tdSql.checkResultsByFunc(
                f"select * from  result.streamt0", func=lambda: tdSql.getRows() == 2
            )

    class STable01(StreamCheckItem):
        def __init__(self):
            self.db = "STable01"

        def create(self):
            tdSql.execute(f"create database result1 vgroups 1;")
            tdSql.execute(f"create database test1 vgroups 4;")
            tdSql.execute(f"use test1;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stable result1.streamt1(ts timestamp, a bigint, b int, c int) tags(ta varchar(100), tb int, tc int);"
            )

            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st partition by tbname, ta, tb, tc options(max_delay(1s)) into result1.streamt1 tags(ta varchar(100) as %%1, tb int as %%3, tc int as %%4) as select _twstart ts, count(*) a, max(a) b, min(b) c from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 10, 20, 30);")
            tdSql.execute(f"insert into t2 values(1648791213000, 40, 50, 60);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select ts, a, b, c from result1.streamt1 order by ta;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 10)
                and tdSql.compareData(0, 3, 20)
                and tdSql.compareData(1, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(1, 2, 40)
                and tdSql.compareData(1, 3, 50),
            )

    class STable02(StreamCheckItem):
        def __init__(self):
            self.db = "STable02"

        def create(self):
            tdSql.execute(f"create database result3 vgroups 1;")
            tdSql.execute(f"create database test3 vgroups 4;")
            tdSql.execute(f"use test3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 2, 3);")
            tdSql.execute(f"create table t2 using st tags(4, 5, 6);")

            tdSql.execute(
                f"create table result3.streamt3(ts timestamp, a int, b int, c bigint);"
            )

            tdSql.execute(
                f"create stream streams3 interval(10s) sliding(10s) from st options(max_delay(1s)) into result3.streamt3 as select _twstart ts, max(a) a, min(b) b, count(*) c from %%trows;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 10, 20, 30);")
            tdSql.execute(f"insert into t2 values(1648791213100, 40, 50, 60);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from result3.streamt3;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.getData(0, 1) == 40
                and tdSql.getData(0, 2) == 20
                and tdSql.getData(0, 3) == 2,
            )

    class STable03(StreamCheckItem):
        def __init__(self):
            self.db = "STable03"

        def create(self):
            tdSql.execute(f"create database result4 vgroups 1;")
            tdSql.execute(f"create database test4 vgroups 4;")
            tdSql.execute(f"use test4;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 2, 3);")
            tdSql.execute(f"create table t2 using st tags(4, 5, 6);")

            tdSql.execute(
                f"create stable result4.streamt4(ts timestamp, a int, b int, c int, d int) tags(tg1 int, tg2 int, tg3 int);"
            )

            tdSql.execute(
                f'create stream streams4 interval(10s) sliding(10s) from st partition by ta, tb, tc options(max_delay(1s)) into result4.streamt4 output_subtable(concat("tbl-", cast(%%1 + 10 as varchar(10)))) tags(tg1 int as cast(%%1 + 10 as int), tg2 int as %%2, tg3 int as %%3) as select  _twstart ts, cast(count(*) as int) a, max(a) b, min(b) c, cast(NULL as int) d from st where ta=%%1 and ts >= _twstart and ts < _twend;'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 10, 20, 30);")
            tdSql.execute(f"insert into t2 values(1648791213000, 40, 50, 60);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select ts, a, b, c, d, tg1, tg2, tg3, tbname from result4.streamt4 order by tg1;",
                lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, 10)
                and tdSql.compareData(0, 3, 20)
                and tdSql.compareData(0, 4, None)
                and tdSql.compareData(0, 5, 11)
                and tdSql.compareData(0, 6, 2)
                and tdSql.compareData(0, 7, 3)
                and tdSql.compareData(0, 8, "tbl-11")
                and tdSql.compareData(1, 0, "2022-04-01 13:33:30.000")
                and tdSql.compareData(1, 1, 1)
                and tdSql.compareData(1, 2, 40)
                and tdSql.compareData(1, 3, 50)
                and tdSql.compareData(1, 4, None)
                and tdSql.compareData(1, 5, 14)
                and tdSql.compareData(1, 6, 5)
                and tdSql.compareData(1, 7, 6)
                and tdSql.compareData(1, 8, "tbl-14"),
            )

    class STable04(StreamCheckItem):
        def __init__(self):
            self.db = "STable04"

        def create(self):
            tdSql.execute(f"create database result5 vgroups 1;")
            tdSql.execute(f"create database test5 vgroups 4;")
            tdSql.execute(f"use test5;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 2, 3);")
            tdSql.execute(f"create table t2 using st tags(4, 5, 6);")

            tdSql.execute(
                f"create stable result5.streamt5(ts timestamp, a bigint, b int, c int, d int) tags(tg1 int, tg2 int, tg3 int);"
            )

            tdSql.execute(
                f'create stream streams5 session(ts, 10s) from st partition by ta, tb, tc options(max_delay(1s)) into result5.streamt5 output_subtable( concat("tbl-", cast(%%3 as varchar(10)) ) )  tags(tg1 int as (cast(%%1+%%2 as int)), tg2 int as %%2, tg3 int as %%3) as select _twstart ts, count(*) a, max(a) b, min(b) c, cast(NULL as int) d from st where tb =%%2 and ts >= _twstart and ts <= _twend;'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select ts, a, b, c, d, tg1, tg2, tg3, tbname from result5.streamt5 order by tg1;",
                lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2022-04-01 13:33:33.000")
                and tdSql.compareData(0, 1, 1)
                and tdSql.compareData(0, 2, None)
                and tdSql.compareData(0, 3, None)
                and tdSql.compareData(0, 4, None)
                and tdSql.compareData(0, 5, 3)
                and tdSql.compareData(0, 6, 2)
                and tdSql.compareData(0, 7, 3)
                and tdSql.compareData(0, 8, "tbl-3"),
            )

    class STable10(StreamCheckItem):
        def __init__(self):
            self.db = "STable10"

        def create(self):
            tdSql.execute(f"create database test vgroups 4;")
            tdSql.execute(f"use test;")
            tdSql.execute(
                f"create stable st(ts timestamp, a bigint, b bigint, c bigint) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(1s) sliding(1s) from st options(max_delay(1s)) into streamt1 as select _twstart, count(*) c1, count(a) c2 from st;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791212000, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 2,
            )

        def insert2(self):
            tdSql.execute(f"drop stream streams1;")
            tdLog.info(f"alter table streamt1 add column c3 double")
            tdSql.execute(f"alter table streamt1 add column c3 double;")

            tdSql.execute(
                f"create stream streams1 interval(1s) sliding(1s) from st options(max_delay(1s)) into streamt1 as select _twstart, count(*) c1, count(a) c2,  avg(b) c3 from st;"
            )

        def check2(self):
            tdStream.checkStreamStatus()

        def insert3(self):
            tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791214000, 1, 2, 3);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4,
            )
