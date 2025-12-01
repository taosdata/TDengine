import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)
class TestStreamOldCaseFillInterval:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_fill_interval(self):
        """OldTsim: fill interval

        Test the results of various numerical fillings in the interval window

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalDelete0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalDelete1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalLinear.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalPartitionBy.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalPrevNext.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalPrevNext1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalRange.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/fillIntervalValue.sim

        """

        tdStream.createSnode()

        self.fillIntervalDelete0()
        # self.fillIntervalDelete1()
        # self.fillIntervalLinear()
        # self.fillIntervalPartitionBy()
        # self.fillIntervalPrevNext()
        # self.fillIntervalPrevNext1()
        # self.fillIntervalRange()
        # self.fillIntervalValue()

    def fillIntervalDelete0(self):
        tdLog.info(f"fillIntervalDelete0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop database if exists test1;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)|force_output|pre_filter(ts >= 1648791210000 and ts < 1648791261000)) into streamt1 as select _twstart as ts, max(a), sum(b), count(*) from t1 where ts >= _twstart and ts < _twend;"
        )
        # tdSql.execute(
        #     f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(value, 100, 200, 300);"
        # )
        # tdSql.execute(
        #     f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(next);"
        # )
        # tdSql.execute(
        #     f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(prev);"
        # )
        # tdSql.execute(
        #     f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt5 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(linear);"
        # )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 1,
        )
        tdSql.pause()

        tdSql.execute(f"delete from t1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 0,
        )

        # tdSql.checkResultsByFunc(
        #     f"select * from streamt2 order by ts;",
        #     lambda: tdSql.getRows() == 0,
        # )

        # tdSql.checkResultsByFunc(
        #     f"select * from streamt3 order by ts;",
        #     lambda: tdSql.getRows() == 0,
        # )

        # tdSql.checkResultsByFunc(
        #     f"select * from streamt4 order by ts;",
        #     lambda: tdSql.getRows() == 0,
        # )

        # tdSql.checkResultsByFunc(
        #     f"select * from streamt5 order by ts;",
        #     lambda: tdSql.getRows() == 0,
        # )

        tdSql.execute(f"insert into t1 values(1648791210000, 4, 4, 4, 4.0, 'ddd');")
        tdSql.execute(f"insert into t1 values(1648791215000, 2, 2, 2, 2.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791217000, 3, 3, 3, 3.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791219000, 5, 5, 5, 5.0, 'eee');")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 10,
        )

    def fillIntervalDelete1(self):
        tdLog.info(f"fillIntervalDelete1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(value, 100, 200, 300);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt5 as select _wstart as ts, max(a), sum(b), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 0, 0, 0, 0.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791215000, 5, 5, 5, 5.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791217000, 6, 6, 6, 6.0, 'ddd');")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 8,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791213000;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 100,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 5,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 0,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791212000, 5, 5, 5, 5.0, 'eee');")
        tdSql.execute(f"insert into t1 values(1648791213000, 6, 6, 6, 6.0, 'fff');")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 8
            and tdSql.getData(2, 1) == 5
            and tdSql.getData(3, 1) == 6,
        )

        tdSql.execute(
            f"delete from t1 where ts >= 1648791211000 and ts <= 1648791214000;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 100,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 5,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 0,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by ts;",
            lambda: tdSql.getRows() == 8 and tdSql.getData(3, 1) == 3,
        )

        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")
        tdSql.execute(f"drop stream if exists streams9;")
        tdSql.execute(f"drop stream if exists streams10;")
        tdSql.execute(f"drop database if exists test6;")
        tdSql.execute(f"create database test6 vgroups 1;")
        tdSql.execute(f"use test6;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double, s varchar(20)) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(1, 1, 1);")
        tdSql.execute(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 as select _wstart as ts, max(a), sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams7 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt7 as select _wstart as ts, max(a), sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(value, 100, 200, 300);"
        )
        tdSql.execute(
            f"create stream streams8 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt8 as select _wstart as ts, max(a), sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams9 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt9 as select _wstart as ts, max(a), sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams10 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt10 as select _wstart as ts, max(a), sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791215000, 6, 8, 8, 8.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791220000, 11, 10, 10, 10.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791221000, 6, 6, 6, 6.0, 'fff');")

        tdSql.execute(f"insert into t2 values(1648791212000, 4, 4, 4, 4.0, 'ddd');")
        tdSql.execute(f"insert into t2 values(1648791214000, 5, 5, 5, 5.0, 'eee');")
        tdSql.execute(f"insert into t2 values(1648791216000, 2, 2, 2, 2.0, 'bbb');")
        tdSql.execute(f"insert into t2 values(1648791222000, 6, 6, 6, 6.0, 'fff');")

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by ts;",
            lambda: tdSql.getRows() == 13 and tdSql.getData(2, 1) == 4,
        )

        tdSql.execute(f"delete from t2;")
        tdLog.info(f"delete from t2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by ts;",
            lambda: tdSql.getRows() == 12 and tdSql.getData(3, 1) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt7 order by ts;",
            lambda: tdSql.getRows() == 12 and tdSql.getData(3, 1) == 100,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt8 order by ts;",
            lambda: tdSql.getRows() == 12 and tdSql.getData(3, 1) == 6,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt9 order by ts;",
            lambda: tdSql.getRows() == 12 and tdSql.getData(3, 1) == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt10 order by ts;",
            lambda: tdSql.getRows() == 12
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(7, 1) == 8
            and tdSql.getData(9, 1) == 10,
        )

        tdSql.execute(f"drop stream if exists streams11;")
        tdSql.execute(f"drop stream if exists streams12;")
        tdSql.execute(f"drop stream if exists streams13;")
        tdSql.execute(f"drop stream if exists streams14;")
        tdSql.execute(f"drop stream if exists streams15;")
        tdSql.execute(f"drop database if exists test7;")
        tdSql.execute(f"create database test7 vgroups 1;")
        tdSql.execute(f"use test7;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams11 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt11 as select _wstart as ts, avg(a), count(*), timezone(), to_iso8601(1) from t1 where ts >= 1648791210000 and ts < 1648791240000 interval(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams12 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt12 as select _wstart as ts, avg(a), count(*), timezone(), to_iso8601(1) from t1 where ts >= 1648791210000 and ts < 1648791240000 interval(1s) fill(value, 100.0, 200);"
        )
        tdSql.execute(
            f"create stream streams13 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt13 as select _wstart as ts, avg(a), count(*), timezone(), to_iso8601(1) from t1 where ts >= 1648791210000 and ts < 1648791240000 interval(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams14 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt14 as select _wstart as ts, avg(a), count(*), timezone(), to_iso8601(1) from t1 where ts >= 1648791210000 and ts < 1648791240000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams15 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt15 as select _wstart as ts, avg(a), count(*), timezone(), to_iso8601(1) from t1 where ts >= 1648791210000 and ts < 1648791240000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791210001, 1, 1, 1, 1.0, 'aaa');")

        tdSql.execute(f"insert into t1 values(1648791215000, 2, 2, 2, 2.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791220000, 3, 3, 3, 3.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791225000, 4, 4, 4, 4.0, 'fff');")

        tdSql.execute(f"insert into t1 values(1648791230000, 5, 5, 5, 5.0, 'ddd');")
        tdSql.execute(f"insert into t1 values(1648791230001, 6, 6, 6, 6.0, 'eee');")
        tdSql.execute(f"insert into t1 values(1648791230002, 7, 7, 7, 7.0, 'fff');")

        tdSql.checkResultsByFunc(
            f"select * from streamt11 order by ts;", lambda: tdSql.getRows() == 21
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt12 order by ts;", lambda: tdSql.getRows() == 21
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt13 order by ts;", lambda: tdSql.getRows() == 21
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt14 order by ts;", lambda: tdSql.getRows() == 21
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt15 order by ts;", lambda: tdSql.getRows() == 21
        )

        tdSql.execute(
            f"delete from t1 where ts > 1648791210001 and ts < 1648791230000;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt11 order by ts;",
            lambda: tdSql.getRows() == 21
            and tdSql.getData(1, 2) == None
            and tdSql.getData(19, 2) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt12 order by ts;",
            lambda: tdSql.getRows() == 21
            and tdSql.getData(1, 2) == 200
            and tdSql.getData(19, 2) == 200,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt13 order by ts;",
            lambda: tdSql.getRows() == 21
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(19, 2) == 3,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt14 order by ts;",
            lambda: tdSql.getRows() == 21
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(19, 2) == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt15 order by ts;",
            lambda: tdSql.getRows() == 21
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(19, 2) == 2,
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")
        tdSql.execute(f"drop stream if exists streams9;")
        tdSql.execute(f"drop stream if exists streams10;")
        tdSql.execute(f"drop stream if exists streams11;")
        tdSql.execute(f"drop stream if exists streams12;")
        tdSql.execute(f"drop stream if exists streams13;")
        tdSql.execute(f"drop stream if exists streams14;")
        tdSql.execute(f"drop stream if exists streams15;")

        tdSql.execute(f"use test1;")
        tdSql.query(f"select * from t1;")

    def fillIntervalLinear(self):
        tdLog.info(f"fillIntervalLinear")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1 start")

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart as ts, max(a)+sum(c), avg(b), first(s), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'aaa') (1648791216000, 5, 5, 5, 5.0, 'bbb');"
        )
        tdSql.execute(
            f"insert into t1 values(1648791210000, 1, 1, 1, 1.0, 'ccc') (1648791219000, 2, 2, 2, 2.0, 'ddd') (1648791222000, 3, 3, 3, 3.0, 'eee');"
        )

        tdSql.execute(f"use test1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 13
            and tdSql.getData(0, 1) == 2.000000000
            and tdSql.getData(0, 2) == 1.000000000
            and tdSql.getData(0, 3) == "ccc"
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(1, 1) == 4.000000000
            and tdSql.getData(1, 2) == 2.000000000
            and tdSql.getData(1, 3) == None
            and tdSql.getData(2, 1) == 6.000000000
            and tdSql.getData(2, 2) == 3.000000000
            and tdSql.getData(2, 3) == None
            and tdSql.getData(3, 1) == 8.000000000
            and tdSql.getData(3, 2) == 4.000000000
            and tdSql.getData(3, 3) == "aaa"
            # and tdSql.getData(4, 1) == 8.666666667
            # and tdSql.getData(4, 2) == 4.333333333
            and tdSql.getData(4, 3) == None
            # and tdSql.getData(5, 1) == 9.333333333
            # and tdSql.getData(5, 2) == 4.666666667
            and tdSql.getData(5, 3) == None
            and tdSql.getData(6, 1) == 10.000000000
            and tdSql.getData(6, 2) == 5.000000000
            and tdSql.getData(7, 1) == 8.000000000
            and tdSql.getData(7, 2) == 4.000000000
            and tdSql.getData(8, 1) == 6.000000000
            and tdSql.getData(8, 2) == 3.000000000
            and tdSql.getData(9, 1) == 4.000000000
            and tdSql.getData(9, 2) == 2.000000000
            # and tdSql.getData(10, 1) == 4.666666667
            # and tdSql.getData(10, 2) == 2.333333333
            # and tdSql.getData(11, 1) == 5.333333333
            # and tdSql.getData(11, 2) == 2.666666667
            and tdSql.getData(12, 1) == 6.000000000
            and tdSql.getData(12, 2) == 3.000000000,
        )

        tdLog.info(f"step 1 end")

        tdLog.info(f"step 2 start")

        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, max(a)+sum(c), avg(b), first(s), count(*) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791210000, 1, 1, 1, 1.0, 'ccc') (1648791219000, 2, 2, 2, 2.0, 'ddd') (1648791222000, 3, 3, 3, 3.0, 'eee');"
        )
        tdSql.execute(
            f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'aaa') (1648791216000, 5, 5, 5, 5.0, 'bbb');"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by ts;",
            lambda: tdSql.getRows() == 13
            and tdSql.getData(0, 1) == 2.000000000
            and tdSql.getData(0, 2) == 1.000000000
            and tdSql.getData(0, 3) == "ccc"
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(1, 1) == 4.000000000
            and tdSql.getData(1, 2) == 2.000000000
            and tdSql.getData(1, 3) == None
            and tdSql.getData(2, 1) == 6.000000000
            and tdSql.getData(2, 2) == 3.000000000
            and tdSql.getData(2, 3) == None
            and tdSql.getData(3, 1) == 8.000000000
            and tdSql.getData(3, 2) == 4.000000000
            and tdSql.getData(3, 3) == "aaa"
            # and tdSql.getData(4, 1) == 8.666666667
            # and tdSql.getData(4, 2) == 4.333333333
            and tdSql.getData(4, 3) == None
            # and tdSql.getData(5, 1) == 9.333333333
            # and tdSql.getData(5, 2) == 4.666666667
            and tdSql.getData(5, 3) == None
            and tdSql.getData(6, 1) == 10.000000000
            and tdSql.getData(6, 2) == 5.000000000
            and tdSql.getData(7, 1) == 8.000000000
            and tdSql.getData(7, 2) == 4.000000000
            and tdSql.getData(8, 1) == 6.000000000
            and tdSql.getData(8, 2) == 3.000000000
            and tdSql.getData(9, 1) == 4.000000000
            and tdSql.getData(9, 2) == 2.000000000
            # and tdSql.getData(10, 1) == 4.666666667
            # and tdSql.getData(10, 2) == 2.333333333
            # and tdSql.getData(11, 1) == 5.333333333
            # and tdSql.getData(11, 2) == 2.666666667
            and tdSql.getData(12, 1) == 6.000000000
            and tdSql.getData(12, 2) == 3.000000000,
        )

        tdLog.info(f"step 2 end")

        tdLog.info(f"step 3 start")

        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as ts, max(a), b+c, s, b+1, 1 from t1 where ts >= 1648791150000 and ts < 1648791261000 interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791215000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791217000, 2, 2, 2, 2.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791211000, 3, 3, 3, 3.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'ddd');")

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by ts;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6.000000000
            and tdSql.getData(0, 3) == "ccc"
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 7.000000000
            and tdSql.getData(1, 3) == None
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(2, 3) == "ddd"
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 5.000000000
            and tdSql.getData(3, 3) == None
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 2.000000000
            and tdSql.getData(4, 3) == "aaa"
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 3.000000000
            and tdSql.getData(5, 3) == None
            and tdSql.getData(6, 1) == 2
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(6, 3) == "bbb",
        )

        tdSql.execute(f"insert into t1 values(1648791212000, 5, 5, 5, 5.0, 'eee');")
        tdSql.execute(
            f"insert into t1 values(1648791207000, 6, 6, 6, 6.0, 'fff') (1648791209000, 7, 7, 7, 7.0, 'ggg') (1648791219000, 8, 8, 8, 8.0, 'hhh') (1648791221000, 9, 9, 9, 9.0, 'iii');"
        )

        tdSql.checkResultsByFunc(
            f"select * from test3.streamt3 order by ts;",
            lambda: tdSql.getRows() == 15
            and tdSql.getData(0, 1) == 6
            and tdSql.getData(0, 2) == 12.000000000
            and tdSql.getData(0, 3) == "fff"
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(1, 2) == 13.000000000
            and tdSql.getData(1, 3) == None
            and tdSql.getData(2, 1) == 7
            and tdSql.getData(2, 2) == 14.000000000
            and tdSql.getData(2, 3) == "ggg"
            and tdSql.getData(3, 1) == 5
            and tdSql.getData(3, 2) == 10.000000000
            and tdSql.getData(3, 3) == None
            and tdSql.getData(5, 1) == 5
            and tdSql.getData(5, 2) == 10.000000000
            and tdSql.getData(5, 3) == "eee"
            and tdSql.getData(11, 1) == 5
            and tdSql.getData(11, 2) == 10.000000000
            and tdSql.getData(11, 3) == None
            and tdSql.getData(12, 1) == 8
            and tdSql.getData(12, 2) == 16.000000000
            and tdSql.getData(12, 3) == "hhh",
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")

        tdSql.execute(f"use test1;")
        tdSql.query(f"select * from t1;")

    def fillIntervalPartitionBy(self):
        tdLog.info(f"fillIntervalPartitionBy")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double, s varchar(20)) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart as ts, max(a) c1, sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 partition by ta interval(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, max(a) c1, sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 partition by ta interval(1s) fill(value, 100, 200, 300);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as ts, max(a) c1, sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 partition by ta interval(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart as ts, max(a) c1, sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 partition by ta interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt5 as select _wstart as ts, max(a) c1, sum(b), count(*) from st where ts >= 1648791210000 and ts < 1648791261000 partition by ta interval(1s) fill(linear);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 0, 0, 0, 0.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791215000, 5, 5, 5, 5.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791216000, 6, 6, 6, 6.0, 'ddd');")
        tdSql.execute(f"insert into t2 values(1648791210000, 7, 0, 0, 0.0, 'aaa');")
        tdSql.execute(f"insert into t2 values(1648791213000, 8, 1, 1, 1.0, 'bbb');")
        tdSql.execute(f"insert into t2 values(1648791215000, 9, 5, 5, 5.0, 'ccc');")
        tdSql.execute(f"insert into t2 values(1648791216000, 10, 6, 6, 6.0, 'ddd');")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by group_id, ts;",
            lambda: tdSql.getRows() == 14,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by group_id, ts;",
            lambda: tdSql.getRows() == 14,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by group_id, ts;",
            lambda: tdSql.getRows() == 14,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by group_id, ts;",
            lambda: tdSql.getRows() == 14,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by group_id, ts;",
            lambda: tdSql.getRows() == 14,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791216000;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by group_id, ts;",
            lambda: tdSql.getRows() == 13,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by group_id, ts;",
            lambda: tdSql.getRows() == 13,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by group_id, ts;",
            lambda: tdSql.getRows() == 13,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by group_id, ts;",
            lambda: tdSql.getRows() == 13,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by group_id, ts;",
            lambda: tdSql.getRows() == 13,
        )

        tdSql.execute(
            f"insert into t2 values(1648791217000, 11, 11, 11, 11.0, 'eee') (1648791219000, 11, 11, 11, 11.0, 'eee') t1 values(1648791217000, 11, 11, 11, 11.0, 'eee') (1648791219000, 11, 11, 11, 11.0, 'eee');"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by group_id, ts;",
            lambda: tdSql.getRows() == 20 and tdSql.getData(0, 4) != 0,
        )

        tdSql.checkResultsByFunc(
            f"select group_id, count(*) from streamt1 group by group_id;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by group_id, ts;",
            lambda: tdSql.getRows() == 20 and tdSql.getData(0, 4) != 0,
        )

        tdSql.checkResultsByFunc(
            f"select group_id, count(*) from streamt2 group by group_id;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by group_id, ts;",
            lambda: tdSql.getRows() == 20 and tdSql.getData(0, 4) != 0,
        )

        tdSql.checkResultsByFunc(
            f"select group_id, count(*) from streamt3 group by group_id;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by group_id, ts;",
            lambda: tdSql.getRows() == 20 and tdSql.getData(0, 4) != 0,
        )

        tdSql.checkResultsByFunc(
            f"select group_id, count(*) from streamt4 group by group_id;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by group_id, ts;",
            lambda: tdSql.getRows() == 20 and tdSql.getData(0, 4) != 0,
        )

        tdSql.checkResultsByFunc(
            f"select group_id, count(*) from streamt5 group by group_id;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")
        tdSql.execute(f"drop stream if exists streams9;")
        tdSql.execute(f"drop stream if exists streams10;")

        tdSql.execute(f"use test1;")
        tdSql.query(f"select * from t1;")
        tdLog.info(f"{tdSql.getData(0, 0)}")

    def fillIntervalPrevNext(self):
        tdLog.info(f"fillIntervalPrevNext")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart as ts, count(*) c1, max(b)+sum(a) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, count(*) c1, max(a)+min(c), avg(b) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'aaa') (1648791215000, 5, 5, 5, 5.0, 'aaa');"
        )
        tdSql.execute(
            f"insert into t1 values(1648791211000, 1, 1, 1, 1.0, 'aaa') (1648791217000, 2, 2, 2, 2.0, 'aaa') (1648791220000, 3, 3, 3, 3.0, 'aaa');"
        )

        tdSql.execute(f"use test1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by ts;",
            lambda: tdSql.getRows() == 10
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2.000000000
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 8.000000000
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 10.000000000
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 10.000000000
            and tdSql.getData(6, 1) == 1
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(7, 1) == 1
            and tdSql.getData(7, 2) == 4.000000000
            and tdSql.getData(8, 1) == 1
            and tdSql.getData(8, 2) == 4.000000000
            and tdSql.getData(9, 1) == 1
            and tdSql.getData(9, 2) == 6.000000000,
        )

        tdSql.execute(f"use test1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by ts;",
            lambda: tdSql.getRows() == 10
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(0, 3) == 1.000000000
            and tdSql.getData(1, 2) == 8.000000000
            and tdSql.getData(1, 3) == 4.000000000
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(2, 3) == 4.000000000
            and tdSql.getData(3, 2) == 10.000000000
            and tdSql.getData(3, 3) == 5.000000000
            and tdSql.getData(4, 2) == 10.000000000
            and tdSql.getData(4, 3) == 5.000000000
            and tdSql.getData(5, 2) == 4.000000000
            and tdSql.getData(5, 3) == 2.000000000
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(6, 3) == 2.000000000
            and tdSql.getData(7, 2) == 6.000000000
            and tdSql.getData(7, 3) == 3.000000000
            and tdSql.getData(8, 2) == 6.000000000
            and tdSql.getData(8, 3) == 3.000000000
            and tdSql.getData(9, 2) == 6.000000000
            and tdSql.getData(9, 3) == 3.000000000,
        )

        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop database if exists test5;")
        tdSql.execute(f"create database test5 vgroups 1;")
        tdSql.execute(f"use test5;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt5 as select _wstart as ts, count(*) c1, max(b)+sum(a) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 as select _wstart as ts, count(*) c1, max(a)+min(c), avg(b) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791211000, 1, 1, 1, 1.0, 'aaa') (1648791217000, 2, 2, 2, 2.0, 'aaa') (1648791220000, 3, 3, 3, 3.0, 'aaa');"
        )
        tdSql.execute(
            f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'aaa') (1648791215000, 5, 5, 5, 5.0, 'aaa');"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by ts;",
            lambda: tdSql.getRows() == 10
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2.000000000
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 8.000000000
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 10.000000000
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 10.000000000
            and tdSql.getData(6, 1) == 1
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(7, 1) == 1
            and tdSql.getData(7, 2) == 4.000000000
            and tdSql.getData(8, 1) == 1
            and tdSql.getData(8, 2) == 4.000000000
            and tdSql.getData(9, 1) == 1
            and tdSql.getData(9, 2) == 6.000000000,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by ts;",
            lambda: tdSql.getRows() == 10
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(0, 3) == 1.000000000
            and tdSql.getData(1, 2) == 8.000000000
            and tdSql.getData(1, 3) == 4.000000000
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(2, 3) == 4.000000000
            and tdSql.getData(3, 2) == 10.000000000
            and tdSql.getData(3, 3) == 5.000000000
            and tdSql.getData(4, 2) == 10.000000000
            and tdSql.getData(4, 3) == 5.000000000
            and tdSql.getData(5, 2) == 4.000000000
            and tdSql.getData(5, 3) == 2.000000000
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(6, 3) == 2.000000000
            and tdSql.getData(7, 2) == 6.000000000
            and tdSql.getData(7, 3) == 3.000000000
            and tdSql.getData(8, 2) == 6.000000000
            and tdSql.getData(8, 3) == 3.000000000
            and tdSql.getData(9, 2) == 6.000000000
            and tdSql.getData(9, 3) == 3.000000000,
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")

        tdSql.execute(f"use test1;")
        tdSql.query(f"select * from t1;")

    def fillIntervalPrevNext1(self):
        tdLog.info(f"fillIntervalPrevNext1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")
        tdSql.execute(f"drop database if exists test7;")
        tdSql.execute(f"create database test7 vgroups 1;")
        tdSql.execute(f"use test7;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams7 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt7 as select _wstart as ts, max(a), b+c, s from t1 where ts >= 1648791150000 and ts < 1648791261000 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams8 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt8 as select _wstart as ts, max(a), 1, b+1 from t1 where ts >= 1648791150000 and ts < 1648791261000 interval(1s) fill(next);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791215000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791217000, 2, 2, 2, 2.0, 'bbb');")
        tdSql.execute(f"insert into t1 values(1648791211000, 3, 3, 3, 3.0, 'ccc');")
        tdSql.execute(f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'ddd');")

        tdSql.checkResultsByFunc(
            f"select * from streamt7 order by ts;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6.000000000
            and tdSql.getData(0, 3) == "ccc"
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 6.000000000
            and tdSql.getData(1, 3) == "ccc"
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(2, 3) == "ddd"
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(3, 2) == 8.000000000
            and tdSql.getData(3, 3) == "ddd"
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 2.000000000
            and tdSql.getData(4, 3) == "aaa"
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 2.000000000
            and tdSql.getData(5, 3) == "aaa"
            and tdSql.getData(6, 1) == 2.000000000
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(6, 3) == "bbb",
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt8 order by ts;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(0, 3) == 4.000000000
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 5.000000000
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 5.000000000
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 2.000000000
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 1
            and tdSql.getData(4, 3) == 2.000000000
            and tdSql.getData(5, 1) == 2
            and tdSql.getData(5, 2) == 1
            and tdSql.getData(5, 3) == 3.000000000
            and tdSql.getData(6, 1) == 2
            and tdSql.getData(6, 2) == 1
            and tdSql.getData(6, 3) == 3.000000000,
        )

        tdSql.execute(f"insert into t1 values(1648791212000, 5, 5, 5, 5.0, 'eee');")
        tdSql.execute(
            f"insert into t1 values(1648791207000, 6, 6, 6, 6.0, 'fff') (1648791209000, 7, 7, 7, 7.0, 'ggg') (1648791219000, 8, 8, 8, 8.0, 'hhh') (1648791221000, 9, 9, 9, 9.0, 'iii');"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt7 order by ts;",
            lambda: tdSql.getRows() == 15
            and tdSql.getData(0, 1) == 6
            and tdSql.getData(0, 2) == 12.000000000
            and tdSql.getData(0, 3) == "fff"
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(1, 2) == 12.000000000
            and tdSql.getData(1, 3) == "fff"
            and tdSql.getData(2, 1) == 7
            and tdSql.getData(2, 2) == 14.000000000
            and tdSql.getData(2, 3) == "ggg"
            and tdSql.getData(3, 1) == 7
            and tdSql.getData(3, 2) == 14.000000000
            and tdSql.getData(3, 3) == "ggg"
            and tdSql.getData(5, 1) == 5
            and tdSql.getData(5, 2) == 10.000000000
            and tdSql.getData(5, 3) == "eee"
            and tdSql.getData(11, 1) == 2
            and tdSql.getData(11, 2) == 4.000000000
            and tdSql.getData(11, 3) == "bbb"
            and tdSql.getData(12, 1) == 8
            and tdSql.getData(12, 2) == 16.000000000
            and tdSql.getData(12, 3) == "hhh"
            and tdSql.getData(13, 1) == 8
            and tdSql.getData(13, 2) == 16.000000000
            and tdSql.getData(13, 3) == "hhh"
            and tdSql.getData(14, 1) == 9
            and tdSql.getData(14, 2) == 18.000000000
            and tdSql.getData(14, 3) == "iii",
        )

        tdLog.info(f"fill next-----------------890")
        tdSql.execute(f"use test7;")
        tdSql.checkResultsByFunc(
            f"select * from streamt8 order by ts;",
            lambda: tdSql.getRows() == 15
            and tdSql.getData(0, 1) == 6
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(0, 3) == 7.000000000
            and tdSql.getData(1, 1) == 7
            and tdSql.getData(1, 3) == 8.000000000
            and tdSql.getData(2, 1) == 7
            and tdSql.getData(2, 3) == 8.000000000
            and tdSql.getData(3, 1) == 3
            and tdSql.getData(3, 3) == 4.000000000
            and tdSql.getData(5, 1) == 5
            and tdSql.getData(5, 3) == 6.000000000
            and tdSql.getData(11, 1) == 8
            and tdSql.getData(11, 2) == 1
            and tdSql.getData(11, 3) == 9.000000000
            and tdSql.getData(12, 1) == 8
            and tdSql.getData(12, 3) == 9.000000000
            and tdSql.getData(13, 1) == 9
            and tdSql.getData(13, 3) == 10.000000000
            and tdSql.getData(14, 1) == 9
            and tdSql.getData(14, 3) == 10.000000000,
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")

        tdSql.execute(f"use test7;")
        tdSql.query(f"select * from t1;")
        tdLog.info(f"{tdSql.getData(0, 0)}")

    def fillIntervalRange(self):
        tdLog.info(f"fillIntervalRange")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));;"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart ts, count(*) c1 from t1 interval(1s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648795308000, 1, 2, 3, 1.0, 'aaa');")

        tdSql.checkResultsByFunc(
            f"select * from streamt where c1 > 0;", lambda: tdSql.getRows() == 2
        )

        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 4098,
        )

        tdSql.execute(f"insert into t1 values(1648800308000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.checkResultsByFunc(
            f"select * from streamt where c1 > 0;",
            lambda: tdSql.getRows() == 3,
        )

        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 9098,
        )

        tdSql.execute(f"insert into t1 values(1648786211000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.checkResultsByFunc(
            f"select * from streamt where c1 > 0;",
            lambda: tdSql.getRows() == 4,
        )

        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 14098,
        )

        tdSql.execute(
            f"insert into t1 values(1648801308000, 1, 1, 1, 1.0, 'aaa') (1648802308000, 1, 1, 1, 1.0, 'aaa') (1648803308000, 1, 1, 1, 1.0, 'aaa') (1648804308000, 1, 1, 1, 1.0, 'aaa') (1648805308000, 1, 1, 1, 1.0, 'aaa');"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt where c1 > 0;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 19098,
        )

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdLog.info(
            f"create stream streams1 trigger at_once into streamt as select _wstart ts, max(a) c1 from t1 interval(1s) fill(linear);"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart ts, max(a) c1 from t1 interval(1s) fill(linear);"
        )

        tdLog.info(
            f"create stream streams2 trigger at_once into streamt2 as select _wstart ts, max(a) c1 from t1 interval(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart ts, max(a) c1 from t1 interval(1s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648795308000, 1, 2, 3, 1.0, 'aaa');")

        tdLog.info(f"select count(*) from streamt;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 4098,
        )

        tdLog.info(f"select count(*) from streamt2;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt2;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 4098,
        )

        tdSql.execute(f"insert into t1 values(1648800308000, 1, 1, 1, 1.0, 'aaa');")
        tdLog.info(f"select count(*) from streamt;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 9098,
        )

        tdLog.info(f"select count(*) from streamt2;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt2;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 9098,
        )

        tdSql.execute(f"insert into t1 values(1648786211000, 1, 1, 1, 1.0, 'aaa');")
        tdLog.info(f"select count(*) from streamt;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 14098,
        )

        tdLog.info(f"select count(*) from streamt2;")
        tdSql.checkResultsByFunc(
            f"select count(*) from streamt2;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 14098,
        )

    def fillIntervalValue(self):
        tdLog.info(f"fillIntervalValue")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));;"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart ts, count(*) c1 from t1 where ts > 1648791210000 and ts < 1648791413000 interval(10s) fill(value, 100);"
        )
        tdSql.execute(
            f"create stream streams1a trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamta as select _wstart ts, count(*) c1 from t1 where ts > 1648791210000 and ts < 1648791413000 interval(10s) fill(value_f, 100);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791233000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791283000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791253000, 1, 2, 3, 1.0, 'aaa');")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by ts;",
            lambda: tdSql.getRows() == 8
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(5, 1) == 100
            and tdSql.getData(6, 1) == 100
            and tdSql.getData(7, 1) == 1,
        )

        tdLog.info(f'"force fill vaule"')
        tdSql.checkResultsByFunc(
            f"select * from streamta order by ts;",
            lambda: tdSql.getRows() == 8
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(5, 1) == 100
            and tdSql.getData(6, 1) == 100
            and tdSql.getData(7, 1) == 1,
        )

        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart as ts, count(*) c1, max(b)+sum(a) from t1 where ts >= 1648791210000 and ts < 1648791261000 interval(1s) fill(value, 100, 200);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791211000, 1, 1, 1, 1.0, 'aaa') (1648791217000, 2, 2, 2, 2.0, 'aaa') (1648791220000, 3, 3, 3, 3.0, 'aaa');"
        )
        tdSql.execute(
            f"insert into t1 values(1648791213000, 4, 4, 4, 4.0, 'aaa') (1648791215000, 5, 5, 5, 5.0, 'aaa');"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by ts;",
            lambda: tdSql.getRows() == 10
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(1, 1) == 100
            and tdSql.getData(1, 2) == 200.000000000
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 8.000000000
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(3, 2) == 200.000000000
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 10.000000000
            and tdSql.getData(5, 1) == 100
            and tdSql.getData(5, 2) == 200.000000000
            and tdSql.getData(6, 1) == 1
            and tdSql.getData(6, 2) == 4.000000000
            and tdSql.getData(7, 1) == 100
            and tdSql.getData(7, 2) == 200.000000000
            and tdSql.getData(8, 1) == 100
            and tdSql.getData(8, 2) == 200.000000000
            and tdSql.getData(9, 1) == 1
            and tdSql.getData(9, 2) == 6.000000000,
        )

        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create table t1(ts timestamp, a int, b int, c int, d double, s varchar(20));"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart as ts, max(b), a+b, c from t1 where ts >= 1648791200000 and ts < 1648791261000 interval(10s) sliding(3s) fill(value, 100, 200, 300);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791220000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791260000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791200000, 1, 1, 1, 1.0, 'aaa');")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by ts;",
            lambda: tdSql.getRows() == 23
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2.000000000
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 2.000000000
            and tdSql.getData(2, 3) == 1
            and tdSql.getData(3, 1) == 100
            and tdSql.getData(3, 2) == 200.000000000
            and tdSql.getData(3, 3) == 300
            and tdSql.getData(6, 1) == 100
            and tdSql.getData(6, 2) == 200.000000000
            and tdSql.getData(6, 3) == 300
            and tdSql.getData(7, 1) == 1
            and tdSql.getData(7, 2) == 2.000000000
            and tdSql.getData(7, 3) == 1
            and tdSql.getData(9, 1) == 1
            and tdSql.getData(9, 2) == 2.000000000
            and tdSql.getData(9, 3) == 1
            and tdSql.getData(10, 1) == 100
            and tdSql.getData(10, 2) == 200.000000000
            and tdSql.getData(10, 3) == 300
            and tdSql.getData(19, 1) == 100
            and tdSql.getData(19, 2) == 200.000000000
            and tdSql.getData(19, 3) == 300
            and tdSql.getData(20, 1) == 1
            and tdSql.getData(20, 2) == 2.000000000
            and tdSql.getData(20, 3) == 1
            and tdSql.getData(22, 1) == 1
            and tdSql.getData(22, 2) == 2.000000000
            and tdSql.getData(22, 3) == 1,
        )

        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"create database test4 vgroups 1;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double, s varchar(20) ) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart ts, count(*) c1, concat(tbname, 'aaa') as pname, timezone() from st where ts > 1648791000000 and ts < 1648793000000 partition by tbname interval(10s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams4a trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4a as select _wstart ts, count(*) c1, concat(tbname, 'aaa') as pname, timezone() from st where ts > 1648791000000 and ts < 1648793000000 partition by tbname interval(10s) fill(NULL_F);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791233000, 1, 2, 3, 1.0, 'aaa');")
        tdSql.execute(f"insert into t1 values(1648791273000, 1, 2, 3, 1.0, 'aaa');")

        tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0, 'bbb');")
        tdSql.execute(f"insert into t2 values(1648791233000, 1, 2, 3, 1.0, 'bbb');")
        tdSql.execute(f"insert into t2 values(1648791273000, 1, 2, 3, 1.0, 'bbb');")

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by pname, ts;",
            lambda: tdSql.getRows() == 14
            and tdSql.getData(1, 1) == None
            and tdSql.getData(1, 2) == "t1aaa"
            and tdSql.getData(3, 2) == "t1aaa"
            and tdSql.getData(4, 2) == "t1aaa"
            and tdSql.getData(5, 2) == "t1aaa"
            and tdSql.getData(8, 1) == None
            and tdSql.getData(8, 2) == "t2aaa"
            and tdSql.getData(10, 2) == "t2aaa"
            and tdSql.getData(11, 2) == "t2aaa"
            and tdSql.getData(12, 2) == "t2aaa",
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4a order by pname, ts;",
            lambda: tdSql.getRows() == 14
            and tdSql.getData(1, 1) == None
            and tdSql.getData(1, 2) == "t1aaa"
            and tdSql.getData(3, 2) == "t1aaa"
            and tdSql.getData(4, 2) == "t1aaa"
            and tdSql.getData(5, 2) == "t1aaa"
            and tdSql.getData(8, 1) == None
            and tdSql.getData(8, 2) == "t2aaa"
            and tdSql.getData(10, 2) == "t2aaa"
            and tdSql.getData(11, 2) == "t2aaa"
            and tdSql.getData(12, 2) == "t2aaa",
        )

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop stream if exists streams7;")
        tdSql.execute(f"drop stream if exists streams8;")

        tdSql.execute(f"use test;")
        tdSql.query(f"select * from t1;")
