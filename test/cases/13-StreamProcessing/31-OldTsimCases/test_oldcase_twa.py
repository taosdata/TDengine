import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseTwa:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_twa(self):
        """Stream twa

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim

        """

        self.streamTwaError()
        self.streamTwaFwcFill()
        self.streamTwaFwcFillPrimaryKey()
        self.streamTwaFwcInterval()
        self.streamTwaFwcIntervalPrimaryKey()
        self.streamTwaInterpFwc()

    def streamTwaError(self):
        tdLog.info(f"streamTwaError")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(a) from st partition by tbname,ta interval(2s) fill(prev);"
        )

        tdSql.error(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt2 as select _wstart, twa(a) from st partition by tbname,ta interval(2s) fill(prev);"
        )
        tdSql.error(
            f"create stream streams3 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt3 as select _wstart, twa(a) from st partition by tbname,ta interval(2s) fill(prev);"
        )
        tdSql.error(
            f"create stream streams4 trigger max_delay 5s IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt4 as select _wstart, twa(a) from st partition by tbname,ta interval(2s) fill(prev);"
        )

        tdSql.error(
            f"create stream streams5 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt5 as select _wstart, twa(a) from st interval(2s) fill(prev);"
        )
        tdSql.error(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt6 as select last(ts), twa(a) from st partition by tbname,ta;"
        )
        tdSql.error(
            f"create stream streams7 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt7 as select _wstart, twa(a) from st partition by tbname,ta session(ts, 2s);"
        )
        tdSql.error(
            f"create stream streams8 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt8 as select _wstart, twa(a) from st partition by tbname,ta state_window(a);"
        )

        tdSql.error(
            f"create stream streams9 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt9 as select _wstart, elapsed(ts) from st partition by tbname,ta interval(2s) fill(prev);"
        )

        tdSql.execute(
            f"create stream streams10 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt10 as select _wstart, sum(a) from st partition by tbname,ta interval(2s) SLIDING(1s);"
        )
        tdSql.execute(
            f"create stream streams11 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt11 as select _wstart, avg(a) from st partition by tbname,ta interval(2s) SLIDING(2s);"
        )

        tdSql.error(
            f"create stream streams10 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt10 as select _wstart, sum(a) from st interval(2s);"
        )

        tdLog.info(f"end")

    def streamTwaFwcFill(self):
        tdLog.info(f"streamTwaFwcFill")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(a), twa(b), elapsed(ts), now ,timezone(), ta  from st partition by tbname,ta interval(2s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  4s,10,1,1)  (now + 7s,20,2,2) (now + 8s,30,3,3);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  5s,10,1,1)  (now + 8s,20,2,2) (now + 9s,30,3,3);"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        time.sleep(1)
        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() < 5,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() < 5,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(a), twa(b), elapsed(ts), now ,timezone(), ta  from st partition by tbname interval(2s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  4s,10,1,1)  (now + 7s,20,2,2) (now + 8s,30,3,3);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  5s,10,1,1)  (now + 8s,20,2,2) (now + 9s,30,3,3);"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        time.sleep(2)

        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() < 5,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() < 5,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(a), twa(b), elapsed(ts), now ,timezone(), ta  from st partition by tbname interval(2s) fill(value,100,200,300);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  4s,10,1,1)  (now + 7s,20,2,2) (now + 8s,30,3,3);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  5s,10,1,1)  (now + 8s,20,2,2) (now + 9s,30,3,3);"
        )

        tdLog.info(f"sql select * from t1;")
        tdSql.query(f"select * from t1;")

        tdLog.info(f"sql select * from t2;")
        tdSql.query(f"select * from t2;")

        time.sleep(2)
        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() < 5,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() < 5,
        )

    def streamTwaFwcFillPrimaryKey(self):
        tdLog.info(f"streamTwaFwcFillPrimaryKey")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(b), count(*),ta  from st partition by tbname, ta interval(2s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.query(f"select now;")

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  3s,2,10,10) (now +  3s,3,30,30);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  4s,2,10,10) (now +  4s,3,30,30);"
        )

        tdLog.info(
            f"sql select _wstart, twa(b), count(*),ta  from t1 partition by tbname, ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, twa(b), count(*),ta  from t1 partition by tbname, ta interval(2s);"
        )

        query1_data = tdSql.getData(0, 1)

        tdLog.info(
            f"sql select _wstart, twa(b), count(*),ta  from t2 partition by tbname, ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, twa(b), count(*),ta  from t2 partition by tbname, ta interval(2s);"
        )

        query2_data = tdSql.getData(0, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query1_data,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query2_data,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(b), ta  from st partition by tbname, ta interval(2s) fill(NULL);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  3s,2,10,10) (now +  3s,3,30,30);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  4s,2,10,10) (now +  4s,3,30,30);"
        )

        tdLog.info(
            f"sql select _wstart, twa(b), count(*),ta  from t1 partition by tbname, ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, twa(b), count(*),ta  from t1 partition by tbname, ta interval(2s);"
        )

        query1_data = tdSql.getData(0, 1)

        tdLog.info(
            f"sql select _wstart, twa(b), count(*),ta  from t2 partition by tbname, ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, twa(b), count(*),ta  from t2 partition by tbname, ta interval(2s);"
        )

        query2_data = tdSql.getData(0, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query1_data,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query2_data,
        )

    def streamTwaFwcInterval(self):
        tdLog.info(f"streamTwaFwcInterval")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1 buffer 16;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, twa(a), ta  from st partition by tbname,ta interval(2s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,5,10,10) (now +  3200a,5,10,10)  (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,2,10,10) (now +  3200a,30,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);"
        )

        tdLog.info(f"sql select _wstart, twa(a)  from t1 interval(2s);")
        tdSql.query(f"select _wstart, twa(a)  from t1 interval(2s);")
        tdSql.printResult()
        query1_data01 = tdSql.getData(0, 1)
        query1_data11 = tdSql.getData(1, 1)

        tdLog.info(f"sql select _wstart, twa(a)  from t2 interval(2s);")
        tdSql.query(f"select _wstart, twa(a)  from t2 interval(2s);")
        tdSql.printResult()
        query2_data01 = tdSql.getData(0, 1)
        query2_data11 = tdSql.getData(1, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == query1_data01
            and tdSql.getData(1, 1) == query1_data11,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == query2_data01
            and tdSql.getData(1, 1) == query2_data11,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4 buffer 16;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, count(*), ta  from st partition by tbname,ta interval(2s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);"
        )

        tdLog.info(f"sql select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.query(f"select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.printResult()
        query1_data01 = tdSql.getData(0, 1)
        query1_data11 = tdSql.getData(1, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1 order by 1;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == query1_data01
            and tdSql.getData(1, 1) == query1_data11,
        )

        tdSql.execute(
            f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);"
        )

        tdLog.info(f"sql select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.query(f"select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.printResult()
        query1_data21 = tdSql.getData(2, 1)
        query1_data31 = tdSql.getData(3, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1 order by 1;",
            lambda: tdSql.getRows() >= 4
            and tdSql.getData(2, 1) == query1_data21
            and tdSql.getData(3, 1) == query1_data31,
        )

        tdSql.execute(
            f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);"
        )

        tdLog.info(f"sql select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.query(f"select _wstart, count(*)  from t1 interval(2s) order by 1;")
        tdSql.printResult()
        query1_data41 = tdSql.getData(4, 1)
        query1_data51 = tdSql.getData(5, 1)

        tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1 order by 1;",
            lambda: tdSql.getRows() >= 6
            and tdSql.getData(4, 1) == query1_data41
            and tdSql.getData(5, 1) == query1_data51,
        )

        tdLog.info(f"======step3")
        tdSql.execute(f"create database test3 vgroups 1 buffer 16;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt3 as select _wstart, twa(a), ta  from st partition by tbname,ta interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(now +  3000a,1,1,1);")
        tdSql.execute(f"flush database test;")
        tdSql.execute(f"insert into t1 values(now +  3001a,10,10,10);")
        tdSql.execute(f"insert into t1 values(now +  13s,50,50,50);")

        tdLog.info(
            f"sql select _wstart, twa(a), ta  from st partition by tbname,ta interval(10s) order by 1;"
        )
        tdSql.query(
            f"select _wstart, twa(a), ta  from st partition by tbname,ta interval(10s) order by 1;"
        )
        tdSql.printResult()
        query_data01 = tdSql.getData(0, 1)

        tdLog.info(f"2 sql select * from streamt3 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1;",
            lambda: tdSql.getRows() >= 1 and tdSql.getData(0, 1) == query_data01,
        )

    def streamTwaFwcIntervalPrimaryKey(self):
        tdLog.info(f"streamTwaFwcIntervalPrimaryKey")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, count(*), ta  from st partition by tbname,ta interval(2s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3s,1,1,1) (now +  3s,2,10,10) (now +  3s,3,30,30) (now +  11s,1,1,1) (now + 11s,2,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  4s,1,1,1) (now +  4s,2,10,10) (now +  4s,3,30,30) (now +  12s,1,1,1) (now + 12s,2,10,10);"
        )

        tdLog.info(
            f"sql select _wstart, count(*)  from st partition by tbname,ta interval(2s);"
        )
        tdSql.query(
            f"select _wstart, count(*)  from st partition by tbname,ta interval(2s);"
        )

        tdLog.info(f"2 sql select * from streamt order by")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 1;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 2,
        )

        tdLog.info(f"2 sql select * from streamt where ta == 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where ta == 2;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 2,
        )

    def streamTwaInterpFwc(self):
        tdLog.info(f"streamTwaInterpFwc")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt1 as select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s) fill(value, 100, 200);"
        )
        tdSql.execute(
            f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt2 as select _wstart, count(a), twa(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt3 as select _irowts, interp(a), interp(b), interp(c), now, timezone(), ta from st partition by tbname,ta every(2s) fill(value, 100, 200, 300);"
        )
        tdSql.execute(
            f"create stream streams4 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt4 as select _irowts, interp(a), interp(b), interp(c), now, timezone(), ta from st partition by tbname,ta every(2s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams5 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt5 as select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,5,10,10) (now +  3200a,5,10,10)  (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);"
        )
        tdSql.execute(
            f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,2,10,10) (now +  3200a,30,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);"
        )

        tdLog.info(
            f"sql  select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s) order by 1, 2;"
        )
        tdSql.query(
            f" select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s) order by 1, 2;"
        )
        tdSql.printResult()
        query1_rows = tdSql.getRows()
        query1_data01 = tdSql.getData(0, 1)

        tdLog.info(
            f"select last(*) from (select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s)) order by 1,2 desc;"
        )
        tdSql.query(
            f" select _wstart, count(a), sum(b), now, timezone(), ta  from st partition by tbname,ta interval(2s) order by 1,2 desc;"
        )

        tdLog.info(f"sql select * from streamt1 order by 1, 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1, 2;",
            lambda: tdSql.getRows() >= query1_rows
            and tdSql.getData(0, 1) == query1_data01,
        )

        tdLog.info(f"sql select * from streamt2 order by 1, 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2;",
            lambda: tdSql.getRows() >= query1_rows
            and tdSql.getData(0, 1) == query1_data01,
        )

        tdLog.info(f"sql select * from streamt3 order by 1, 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2;",
            lambda: tdSql.getRows() >= query1_rows,
        )

        tdLog.info(f"sql select * from streamt4 order by 1, 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by 1, 2;",
            lambda: tdSql.getRows() >= query1_rows,
        )

        tdLog.info(f"sql select * from streamt5 order by 1, 2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by 1, 2;",
            lambda: tdSql.getRows() >= query1_rows
            and tdSql.getData(0, 1) == query1_data01,
        )

        tdLog.info(f"step2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test4 vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams6 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt6 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_1")) as select _irowts, interp(a), _isfilled as a1 from st partition by tbname, b as cc every(2s) fill(prev);'
        )
        tdSql.execute(
            f'create stream streams7 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt7 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_2")) as select _wstart, twa(a) from st partition by tbname, b as cc interval(2s) fill(NULL);'
        )
        tdSql.execute(
            f'create stream streams8 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt8 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_3")) as select _wstart, count(a) from st partition by tbname, b as cc interval(2s);'
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(now +  3s,1,1,1);")
        tdLog.info(f"2 sql select cc,* from streamt6;")
        tdSql.checkResultsByFunc(
            f"select cc,* from streamt6;",
            lambda: tdSql.getRows() >= 2 and tdSql.getData(0, 0) == 1,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt6";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt6";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt6" and table_name like "tbn-t1_1%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt6" and table_name like "tbn-t1_1%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"2 sql select cc,* from streamt7;")
        tdSql.checkResultsByFunc(
            f"select cc,* from streamt7;",
            lambda: tdSql.getRows() >= 2 and tdSql.getData(0, 0) == 1,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt7";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt7";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt7" and table_name like "tbn-t1_2%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt7" and table_name like "tbn-t1_2%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"2 sql select cc,* from streamt8;")
        tdSql.checkResultsByFunc(
            f"select cc,* from streamt8;",
            lambda: tdSql.getRows() >= 1 and tdSql.getData(0, 0) == 1,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt8";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt8";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt8" and table_name like "tbn-t1_3%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt8" and table_name like "tbn-t1_3%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test4 vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1234567890t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t1234567890t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stable streamt9(ts timestamp,a varchar(10),b tinyint,c tinyint) tags(ta varchar(3),cc int,tc int);"
        )
        tdSql.execute(
            f"create stable streamt10(ts timestamp,a varchar(10),b tinyint,c tinyint) tags(ta varchar(3),cc int,tc int);"
        )
        tdSql.execute(
            f"create stable streamt11(ts timestamp,a varchar(10),b tinyint,c tinyint) tags(ta varchar(3),cc int,tc int);"
        )

        tdSql.execute(
            f'create stream streams9 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt9 TAGS(cc,ta) SUBTABLE(concat(concat("tbn-", tbname), "_1")) as select _irowts, interp(a), _isfilled as a1, interp(b) from st partition by tbname as ta, b as cc every(2s) fill(value, 100000,200000);'
        )
        tdSql.execute(
            f'create stream streams10 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt10 TAGS(cc,ta) SUBTABLE(concat(concat("tbn-", tbname), "_2")) as select _wstart, twa(a), sum(b),max(c) from st partition by tbname as ta, b as cc interval(2s) fill(NULL);'
        )
        tdSql.execute(
            f'create stream streams11 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt11 TAGS(cc,ta) SUBTABLE(concat(concat("tbn-", tbname), "_3")) as select _wstart, count(a),avg(c),min(b) from st partition by tbname as ta, b as cc interval(2s);'
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1234567890t1 values(now +  3s,100000,1,1);")
        tdLog.info(f"2 sql select cc,ta, * from streamt9;")
        tdSql.checkResultsByFunc(
            f"select cc,ta, * from streamt9;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 0) == 1
            and tdSql.getData(0, 1) == "t12"
            and tdSql.getData(0, 3) == "100000"
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(0, 5) == 64,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt9";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt9";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt9" and table_name like "tbn-t1234567890t1_1%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt9" and table_name like "tbn-t1234567890t1_1%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"2 sql select cc,ta, * from streamt10;")
        tdSql.checkResultsByFunc(
            f"select cc,ta, * from streamt10;",
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 0) == 1
            and tdSql.getData(0, 1) == "t12"
            and tdSql.getData(0, 3) == "100000"
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(0, 5) == 1,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt10";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt10";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt10" and table_name like "tbn-t1234567890t1_2%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt10" and table_name like "tbn-t1234567890t1_2%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"2 sql select cc,ta,* from streamt11;")
        tdSql.checkResultsByFunc(
            f"select cc,ta,* from streamt11;",
            lambda: tdSql.getRows() >= 1
            and tdSql.getData(0, 0) == 1
            and tdSql.getData(0, 1) == "t12"
            and tdSql.getData(0, 3) == "1"
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(0, 5) == 1,
        )

        tdLog.info(
            f'3 sql select * from information_schema.ins_tables where stable_name = "streamt11";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt11";'
        )
        tdSql.checkRows(1)

        tdLog.info(
            f'4 sql select * from information_schema.ins_tables where stable_name = "streamt11" and table_name like "tbn-t1234567890t1_3%";'
        )
        tdSql.query(
            f'select * from information_schema.ins_tables where stable_name = "streamt11" and table_name like "tbn-t1234567890t1_3%";'
        )
        tdSql.checkRows(1)

        tdLog.info(f"end")
