import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseForceWindowClose:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_force_window_close(self):
        """OldTsim: force window close

        Verify the alternative approach to the original force window close trigger mode in the new streaming computation

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/forcewindowclose.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamFwcIntervalFill.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose1.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpFwcError.sim

        """

        tdStream.createSnode()

        self.forcewindowclose()
        # self.streamFwcIntervalFill()
        # self.streamInterpForceWindowClose()
        # self.streamInterpForceWindowClose1()

    def forcewindowclose(self):
        tdLog.info(f"forcewindowclose")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(
            f"===================================== force window close with sliding test"
        )
        tdLog.info(f"============ create db")
        tdSql.execute(f"create database test1 vgroups 2 precision 'us';")

        tdSql.execute(f"use test1")
        tdSql.execute(f"create stable st1(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu11 using st1 tags(1);")

        tdSql.error(
            f"create stream stream11 trigger force_window_close into str_dst1 as select _wstart, count(*) from st1 partition by tbname interval(5s) sliding(6s);"
        )
        tdSql.error(
            f"create stream stream11 trigger force_window_close into str_dst1 as select _wstart, count(*) from st1 partition by tbname interval(5s) sliding(9a);"
        )
        tdSql.error(
            f"create stream stream11 trigger force_window_close into str_dst1 as select _wstart, count(*) from st1 partition by tbname interval(5s) sliding(1.1s);"
        )
        tdSql.execute(
            f"create stream stream11 trigger force_window_close into str_dst1 as select _wstart, _wend, count(*) from st1 partition by tbname interval(5s) sliding(1s);"
        )
        tdStream.checkStreamStatus()

        time.sleep(5.5)
        tdSql.execute(f"insert into tu11 values(now, 1);")
        for i in range(19):
            time.sleep(0.5)
            tdSql.execute(f"insert into tu11 values(now, 1);")

        tdSql.checkResultsByFunc(
            f"select sum(`count(*)`) from (select * from str_dst1)",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 100,
            retry=60,
        )

        tdLog.info(f"========================================== create database")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test vgroups 2;")

        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu1 using st tags(1);")

        tdSql.execute(
            f"create stream stream1 trigger force_window_close into str_dst as select _wstart, count(*) from st partition by tbname interval(5s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into tu1 values(now, 1);")
        time.sleep(5.5)
        tdSql.execute("pause stream stream1")
        for i in range(19):
            time.sleep(0.5)
            tdSql.execute(f"insert into tu1 values(now, 1);")

        tdSql.execute("resume stream stream1")
        tdSql.checkResultsByFunc(
            f"select sum(`count(*)`) from (select * from str_dst)",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 20,
            retry=60,
        )

        tdSql.query(f"select sum(`count(*)`) from (select * from str_dst)")
        tdSql.execute(f"drop database test")

        tdLog.info(f"===================================== micro precision db test")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"============ create db")
        tdSql.execute(f"create database test vgroups 2 precision 'us';")

        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu1 using st tags(1);")

        tdSql.execute(
            f"create stream stream1 trigger force_window_close into str_dst as select _wstart, count(*) from st partition by tbname interval(5s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into tu1 values(now, 1);")
        time.sleep(5.5)
        tdSql.execute("pause stream stream1")
        for i in range(19):
            time.sleep(0.5)
            tdSql.execute(f"insert into tu1 values(now, 1);")

        tdSql.execute("resume stream stream1")
        tdSql.checkResultsByFunc(
            f"select sum(`count(*)`) from (select * from str_dst)",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 20,
            retry=60,
        )

        tdSql.execute(f"drop stream stream1")
        tdSql.execute(f"drop table str_dst")

        tdLog.info(f"============================= too long watermark test")
        tdSql.execute(f"drop table tu1;")
        tdSql.execute(f"create table tu1 using st tags(1);")
        tdSql.execute(
            f"create stream stream2 trigger force_window_close watermark 30s into str_dst as select _wstart, count(*), now() from st partition by tbname interval(5s);"
        )
        tdStream.checkStreamStatus()

        for i in range(19):
            time.sleep(0.5)
            tdSql.execute(f"insert into tu1 values(now, 1);")

        tdSql.checkResultsByFunc(
            f"select sum(`count(*)`) from (select * from str_dst)",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 19,
            retry=60,
        )

        tdSql.checkResultsByFunc(
            f"select round(timediff(`now()`, `_wstart`)/1000000) from str_dst;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 35.000000000,
            retry=60,
        )

    def streamFwcIntervalFill(self):
        tdLog.info(f"streamFwcIntervalFill")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone() from st partition by tbname, ta interval(2s) fill(value, 100, 200);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 5, 10, 10) (now +  3200a, 5, 10, 10)  (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 2, 10, 10) (now +  3200a, 30, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
        )

        tdLog.info(
            f"sql select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone() from t1 partition by tbname, ta interval(2s)"
        )
        tdSql.query(
            f"select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone() from t1 partition by tbname, ta interval(2s);"
        )

        query1_data01 = tdSql.getData(0, 1)
        query1_data11 = tdSql.getData(1, 1)

        tdLog.info(
            f"sql select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone() from t2 partition by tbname, ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone() from t2 partition by tbname, ta interval(2s);"
        )

        query2_data01 = tdSql.getData(0, 1)
        query2_data11 = tdSql.getData(1, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1 order by 1;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == query1_data01
            and tdSql.getData(1, 1) == query1_data11,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2 order by 1;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == query2_data01
            and tdSql.getData(1, 1) == query2_data11,
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() >= 6
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, count(*), ta from st partition by tbname, ta interval(2s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
        )

        tdLog.info(f"sql select _wstart, count(*) from t1 interval(2s) order by 1;")
        tdSql.query(f"select _wstart, count(*) from t1 interval(2s) order by 1;")

        query1_data01 = tdSql.getData(0, 1)
        query1_data11 = tdSql.getData(1, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1 order by 1;",
            lambda: tdSql.getRows() > 1 and tdSql.getData(0, 1) == query1_data01,
            retry=60,
        )

        tdSql.execute(
            f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
        )

        tdLog.info(f"2 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() >= 10 and tdSql.getData(0, 1) == query1_data01,
            retry=60,
        )

    def streamInterpForceWindowClose(self):
        tdLog.info(f"streamInterpForceWindowClose")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, interp(b) as b, now from t1 every(2s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.1) (now + 10s, 2, 2, 2, 2.1) (now + 20s, 3, 3, 3, 3.1);"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"2 sql select * from streamt where a == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 1;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where a == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 2;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where a == 3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 3;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, interp(b) as b, now from t1 every(2s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.1) (now + 10s, 2, 2, 2, 2.1) (now + 20s, 3, 3, 3, 3.1);"
        )

        tdLog.info(f"sql select * from t1;")

        tdLog.info(f"2 sql select * from streamt where a is null;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a is null;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"step3")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, interp(b) as b, now from t1 every(2s) fill(value, 100, 200);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.1) (now + 10s, 2, 2, 2, 2.1) (now + 20s, 3, 3, 3, 3.1);"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"2 sql select * from streamt where a == 100;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 100;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

    def streamInterpForceWindowClose1(self):
        tdLog.info(f"streamInterpForceWindowClose1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step prev")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 3;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b, c every(5s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.0) (now + 10s, 2, 1, 1, 2.0)(now + 20s, 3, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t2 values(now, 21, 1, 1, 1.0) (now + 10s, 22, 1, 1, 2.0)(now + 20s, 23, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t3 values(now, 31, 1, 1, 1.0) (now + 10s, 32, 1, 1, 2.0)(now + 20s, 33, 1, 1, 3.0)"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        tdLog.info(f"sql select * from t3;")
        tdSql.query(f"select * from t3;")

        tdLog.info(f"2 sql select * from streamt where a == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 1;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where a == 21;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 21;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where a == 31;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 31;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"sql select * from streamt where a == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 2;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"3 sql select * from streamt where a == 22;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 22;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"3 sql select * from streamt where a == 32;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 32;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"4 sql select * from streamt where a == 3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 3;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"4 sql select * from streamt where a == 23;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 23;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"4 sql select * from streamt where a == 33;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 33;",
            lambda: tdSql.getRows() >= 2,
            retry=60,
        )

        tdLog.info(f"5 sql select * from streamt where a == 3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 3;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"5 sql select * from streamt where a == 23;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 23;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"5 sql select * from streamt where a == 33;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 33;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"2 sql select * from streamt where a == 3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 3;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b, c every(2s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.0) (now + 10s, 2, 1, 1, 2.0)(now + 20s, 3, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t2 values(now, 21, 1, 1, 1.0) (now + 10s, 22, 1, 1, 2.0)(now + 20s, 23, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t3 values(now, 31, 1, 1, 1.0) (now + 10s, 32, 1, 1, 2.0)(now + 20s, 33, 1, 1, 3.0)"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        tdLog.info(f"sql select * from t3;")
        tdSql.query(f"select * from t3;")

        tdLog.info(f"2 sql select * from streamt where a is null;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a is null;",
            lambda: tdSql.getRows() >= 5,
            retry=60,
        )

        tdLog.info(f"step3")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b, c every(2s) fill(value, 100);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now, 1, 1, 1, 1.0) (now + 10s, 2, 1, 1, 2.0)(now + 20s, 3, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t2 values(now, 21, 1, 1, 1.0) (now + 10s, 22, 1, 1, 2.0)(now + 20s, 23, 1, 1, 3.0)"
        )
        tdSql.execute(
            f"insert into t3 values(now, 31, 1, 1, 1.0) (now + 10s, 32, 1, 1, 2.0)(now + 20s, 33, 1, 1, 3.0)"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        tdLog.info(f"sql select * from t3;")
        tdSql.query(f"select * from t3;")

        tdLog.info(f"2 sql select * from streamt where a == 100;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a == 100;",
            lambda: tdSql.getRows() >= 10,
            retry=60,
        )

        tdLog.info(f"end")
