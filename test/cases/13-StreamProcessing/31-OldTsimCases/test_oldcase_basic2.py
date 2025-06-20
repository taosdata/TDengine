import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic2(self):
        """Stream basic test 2

        1.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/windowClose.sim

        """

        self.pauseAndResume()
        self.sliding()
        self.tag()
        self.triggerInterval0()
        self.windowClose()

    def pauseAndResume(self):
        tdLog.info(f"pauseAndResume")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step1")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 3;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(f"create table ts2 using st tags(2,2,2);")
        tdSql.execute(f"create table ts3 using st tags(3,2,2);")
        tdSql.execute(f"create table ts4 using st tags(4,2,2);")

        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt1 as select  _wstart, count(*) c1, sum(a) c3 from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.error(
            f"create stream stream1_same_dst into streamt1 as select _wstart, count(*) c1, sum(a) c3 from st interval(10s);"
        )

        tdSql.execute(f"pause stream streams1;")

        tdSql.execute(f"insert into ts1 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001,1,12,3,1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001,1,12,3,1.0);")

        tdLog.info(f"1 select * from streamt1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.query("resume stream streams1;")

        tdLog.info(f"2 select * from streamt1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 4,
        )

        tdSql.execute(f"insert into ts1 values(1648791223002,2,2,3,1.1);")
        tdSql.execute(f"insert into ts2 values(1648791223002,3,2,3,2.1);")
        tdSql.execute(f"insert into ts3 values(1648791223002,4,2,43,73.1);")
        tdSql.execute(f"insert into ts4 values(1648791223002,24,22,23,4.1);")

        tdLog.info(f"3 select * from streamt1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4,
        )

        tdLog.info(f"===== idle for 70 sec for checkpoint gen")
        tdLog.info(f"===== idle 70 sec completed , continue")
        tdLog.info(f"===== step 1 over")
        tdLog.info(f"===== step2")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2  vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select  _wstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
        )

        tdSql.error(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select  _wstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
        )
        tdSql.execute(
            f"create stream if not exists streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt2 as select  _wstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
        )

        tdLog.info(f"start to check stream status")
        tdStream.checkStreamStatus()

        tdLog.info(f"pause stream2")
        tdSql.execute(f"pause stream streams2;")

        tdSql.execute(f"insert into t1 values(1648791213001,1,12,3,1.0);")
        tdLog.info(f"1 select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.query("resume stream streams2;")

        tdLog.info(f"2 select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223002,2,2,3,1.1);")

        tdLog.info(f"3 select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1,
        )

        tdSql.execute(f"pause stream streams2;")

        tdSql.execute(f"insert into t1 values(1648791223003,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233003,2,2,3,1.1);")
        tdSql.query("resume stream  IGNORE UNTREATED streams2;")

        tdLog.info(f"4 select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1,
        )

        tdLog.info(f"===== step 2 over")

        tdLog.info(f"===== step3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3  vgroups 3;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(f"create table ts2 using st tags(2,2,2);")
        tdSql.execute(f"create table ts3 using st tags(3,2,2);")
        tdSql.execute(f"create table ts4 using st tags(4,2,2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt3 as select  _wstart, count(*) c1, sum(a) c3 from st interval(10s);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt4 as select  _wstart, count(*) c1, sum(a) c3 from st interval(10s);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt5 as select  _wstart, count(*) c1, sum(a) c3 from ts1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"pause stream streams3;")

        tdSql.execute(f"insert into ts1 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001,1,12,3,1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001,1,12,3,1.0);")

        tdLog.info(f"1 select * from streamt4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4;",
            lambda: tdSql.getRows() == 1,
        )

        tdLog.info(f"2 select * from streamt5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt5;",
            lambda: tdSql.getRows() == 1,
        )

        tdLog.info(f"===== step 3 over")

        tdLog.info(f"===== step 4")

        tdSql.error(f"pause stream streams3333333;")
        tdSql.execute(f"pause stream IF EXISTS streams44444;")

        tdSql.error(f"resume stream streams5555555;")
        tdSql.query("resume stream IF EXISTS streams66666666;")

        tdLog.info(f"===== step 4 over")

        tdLog.info(f"===== step5")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"drop stream if exists streams6;")
        tdSql.execute(f"drop database if exists test6;")
        tdSql.execute(f"create database test6  vgroups 10;")
        tdSql.execute(f"use test6;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(f"create table ts2 using st tags(2,2,2);")
        tdSql.execute(f"create table ts3 using st tags(3,2,2);")
        tdSql.execute(f"create table ts4 using st tags(4,2,2);")
        tdSql.execute(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt6 as select  _wstart, count(*) c1 from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001,1,12,3,1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001,1,12,3,1.0);")

        tdSql.execute(f"pause stream streams6;")

        tdSql.execute(f"insert into ts1 values(1648791223001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts2 values(1648791233001,1,12,3,1.0);")

        tdSql.query("resume stream streams6;")

        tdSql.execute(f"insert into ts3 values(1648791243001,1,12,3,1.0);")
        tdSql.execute(f"insert into ts4 values(1648791253001,1,12,3,1.0);")

        tdLog.info(f"2 select * from streamt6;")
        tdSql.checkResultsByFunc(
            f"select * from streamt6;",
            lambda: tdSql.getRows() == 5,
        )

        tdLog.info(f"===== step5 over")

        tdLog.info(f"===== step6")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test6;")
        tdSql.execute(f"create database test7  vgroups 1;")
        tdSql.execute(f"use test7;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")

        tdSql.execute(
            f"create stream streams8 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt8 as select  _wstart, count(*) c1 from st interval(10s);"
        )
        tdSql.execute(
            f"create stream streams9 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 watermark 1d into streamt9 as select  _wstart, count(*) c1 from st partition by tbname interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.checkResultsByFunc(
            f'select status, * from information_schema.ins_streams where status != "ready";',
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"pause stream streams8;")
        tdSql.execute(f"pause stream streams9;")
        tdSql.execute(f"pause stream streams8;")
        tdSql.execute(f"pause stream streams9;")
        tdSql.execute(f"pause stream streams8;")
        tdSql.execute(f"pause stream streams9;")

        tdSql.checkResultsByFunc(
            f'select status, * from information_schema.ins_stream_tasks where status != "paused";',
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f'select status, * from information_schema.ins_streams where status == "paused";',
            lambda: tdSql.getRows() == 2,
        )

        tdSql.query("resume stream streams8;")
        tdSql.query("resume stream streams9;")
        tdSql.query("resume stream streams8;")
        tdSql.query("resume stream streams9;")
        tdSql.query("resume stream streams8;")
        tdSql.query("resume stream streams9;")

        tdSql.checkResultsByFunc(
            f'select status, * from information_schema.ins_stream_tasks where status == "paused";',
            lambda: tdSql.getRows() == 0,
        )

        tdSql.checkResultsByFunc(
            f'select status, * from information_schema.ins_streams where status != "paused";',
            lambda: tdSql.getRows() == 2,
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791213001,1,12,3,1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt8;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt9;",
            lambda: tdSql.getRows() == 1,
        )

        tdLog.info(f"===== step6 over")

    def sliding(self):
        tdLog.info(f"sliding")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(3)

        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s) sliding (5s);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt2 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s) sliding (5s);"
        )
        tdSql.execute(
            f"create stream stream_t1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamtST as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from st interval(10s) sliding (5s);"
        )
        tdSql.execute(
            f"create stream stream_t2 trigger at_once watermark 1d IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamtST2 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from st interval(10s) sliding (5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791216000,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791220000,3,2,3,2.1);")

        tdSql.execute(f"insert into t1 values(1648791210000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791216000,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791220000,3,2,3,2.1);")

        tdSql.execute(f"insert into t2 values(1648791210000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791216000,2,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791220000,3,2,3,2.1);")

        tdSql.execute(f"insert into t2 values(1648791210000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791216000,2,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791220000,3,2,3,2.1);")

        tdLog.info(f"step 0")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 3,
        )

        tdLog.info(f"step 1")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 3,
        )

        tdLog.info(f"step 2")
        tdSql.checkResultsByFunc(
            f"select * from streamtST",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 10
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 6,
        )

        tdLog.info(f"step 3")
        tdSql.checkResultsByFunc(
            f"select * from streamtST2",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 10
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 6,
        )

        tdLog.info(f"step 3.1")
        tdSql.execute(f"insert into t1 values(1648791216001,2,2,3,1.1);")
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

        tdLog.info(f"step 4")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test1 vgroups 1")
        tdSql.execute(f"use test1")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams11 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s, 5s);"
        )
        tdSql.execute(
            f"create stream streams12 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt2 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from st interval(10s, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,2,3,4.1);")

        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t2 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t2 values(1648791213004,4,2,3,4.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 5
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 4,
        )

        tdLog.info(f"step 5")
        tdSql.checkResultsByFunc(
            f"select * from streamt2",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 10
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 6
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 8,
        )

        tdSql.execute(f"drop stream IF EXISTS streams21;")
        tdSql.execute(f"drop stream IF EXISTS streams22;")
        tdSql.execute(f"drop stream IF EXISTS streams23;")
        tdSql.execute(f"drop database IF EXISTS test2;")

        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 6;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams21 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt21 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s, 5s);"
        )
        tdSql.execute(
            f"create stream streams22 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt22 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from st interval(10s, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,3,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,4,4,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,5,5,4.1);")

        tdSql.execute(f"insert into t2 values(1648791213000,1,6,6,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,2,7,7,1.1);")
        tdSql.execute(f"insert into t2 values(1648791233002,3,8,8,2.1);")
        tdSql.execute(f"insert into t2 values(1648791243003,4,9,9,3.1);")
        tdSql.execute(f"insert into t2 values(1648791213004,4,10,10,4.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt21;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 5
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 4,
        )

        tdLog.info(f"step 6")
        tdSql.checkResultsByFunc(
            f"select * from streamt22;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 10
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 6
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 8,
        )

        tdLog.info(f"step 7")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 6;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams23 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt23 as select  _wstart, count(*) c1, sum(a) c3 , max(b)  c4, min(c) c5 from st interval(20s) sliding(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,3,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,4,4,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,5,5,4.1);")

        tdSql.execute(f"insert into t2 values(1648791213000,1,6,6,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,2,7,7,1.1);")
        tdSql.execute(f"insert into t2 values(1648791233002,3,8,8,2.1);")
        tdSql.execute(f"insert into t2 values(1648791243003,4,9,9,3.1);")
        tdSql.execute(f"insert into t2 values(1648791213004,4,10,10,4.1);")

        tdLog.info(f"step 7")
        tdSql.checkResultsByFunc(
            f"select * from streamt23;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791343003,4,4,4,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,5,5,4.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt23;",
            lambda: tdSql.getRows() == 7
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 2
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(6, 1) == 1,
        )

        tdLog.info(f"step 8")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop stream IF EXISTS streams4;")
        tdSql.execute(f"drop database IF EXISTS test4;")

        tdSql.execute(f"create database test4 vgroups 6;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt4 as select  _wstart as ts, count(*),min(a) c1 from st interval(10s) sliding(5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000,2,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791273000,3,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791313000,4,1,1,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by 1;",
            lambda: tdSql.getRows() == 8
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 3
            and tdSql.getData(5, 1) == 1
            and tdSql.getData(5, 2) == 3
            and tdSql.getData(6, 1) == 1
            and tdSql.getData(6, 2) == 4
            and tdSql.getData(7, 1) == 1
            and tdSql.getData(7, 2) == 4,
        )

    def tag(self):
        tdLog.info(f"tag")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 2;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create table st1(ts timestamp, a int, b int , c int, d double) tags(x int);"
        )
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"create table t2 using st1 tags(2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select  _wstart as s, count(*) c1 from st1 where x>=2 interval(60s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t2 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"alter table t1 set tag x=3;")

        tdSql.execute(f"insert into t1 values(1648791233000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233009,0,3,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 6,
        )

        tdSql.execute(f"alter table t1 set tag x=1;")
        tdSql.execute(f"alter table t2 set tag x=1;")

        tdSql.execute(f"insert into t1 values(1648791243000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243001,9,2,2,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 6,
        )

    def triggerInterval0(self):
        tdLog.info(f"triggerInterval0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use test")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  _wstart, count(*) c1, count(d) c2 , sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213001,1,2,3,1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223003,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")

        tdLog.info(f"step 0")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791233001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223004,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223004,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223005,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 5,
        )

        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791213002,4,2,3,3.1)")
        tdSql.execute(f"insert into t1 values(1648791213002,4,2,3,4.1);")

        tdLog.info(f"step 3")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 5,
        )

    def windowClose(self):
        tdLog.info(f"windowClose")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu1 using st tags(1);")
        tdSql.execute(f"create table tu2 using st tags(2);")

        tdSql.execute(
            f"create stream stream1 trigger window_close into streamt as select  _wstart, sum(a) from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into tu1 values(now, 1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 0,
        )

        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create stable st(ts timestamp, a int, b int) tags(t int);")
        tdSql.execute(f"create table t1 using st tags(1);")
        tdSql.execute(f"create table t2 using st tags(2);")

        tdSql.execute(
            f"create stream stream2 trigger window_close into streamt2 as select  _wstart, sum(a) from st interval(10s);"
        )
        tdSql.execute(
            f"create stream stream3 trigger max_delay 5s into streamt3 as select  _wstart, sum(a) from st interval(10s);"
        )
        tdSql.execute(
            f"create stream stream4 trigger window_close into streamt4 as select  _wstart, sum(a) from t1 interval(10s);"
        )
        tdSql.execute(
            f"create stream stream5 trigger max_delay 5s into streamt5 as select  _wstart, sum(a) from t1 interval(10s);"
        )
        tdSql.execute(
            f"create stream stream6 trigger window_close into streamt6 as select  _wstart, sum(a) from st session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream stream7 trigger max_delay 5s into streamt7 as select  _wstart, sum(a) from st session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream stream8 trigger window_close into streamt8 as select  _wstart, sum(a) from t1 session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream stream9 trigger max_delay 5s into streamt9 as select  _wstart, sum(a) from t1 session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream stream10 trigger window_close into streamt10 as select  _wstart, sum(a) from t1 state_window(b);"
        )
        tdSql.execute(
            f"create stream stream11 trigger max_delay 5s into streamt11 as select  _wstart, sum(a) from t1 state_window(b);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,1);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,1);")
        tdSql.execute(f"insert into t1 values(1648791213002,3,1);")
        tdSql.execute(f"insert into t1 values(1648791233000,4,2);")

        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt5;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt6;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt8;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt9;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt10;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt11;",
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"step 1 max delay 5s")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(
            f"create stream stream13 trigger max_delay 5s into streamt13 as select  _wstart, sum(a), now from t1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt13;",
            lambda: tdSql.getRows() == 2,
        )

        now02 = tdSql.getData(0, 2)
        now12 = tdSql.getData(1, 2)
        tdLog.info(f"now02:{now02}, now12:{now12}")

        tdLog.info(f"step1 max delay 5s......... sleep 6s")
        time.sleep(6)

        tdSql.query(f"select * from streamt13;")

        tdSql.checkAssert(tdSql.getData(0, 2) == now02)
        tdSql.checkAssert(tdSql.getData(1, 2) == now12)

        tdLog.info(f"step 2 max delay 5s")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test4 vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream stream14 trigger max_delay 5s into streamt14 as select  _wstart, sum(a), now from st partition by tbname interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,2,2,3,1.1);")

        tdSql.execute(f"insert into t2 values(1648791213000,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223000,4,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt14 order by 2;",
            lambda: tdSql.getRows() == 4,
        )

        now02 = tdSql.getData(0, 2)
        now12 = tdSql.getData(1, 2)
        now22 = tdSql.getData(2, 2)
        now32 = tdSql.getData(3, 2)

        tdLog.info(f"step2 max delay 5s......... sleep 6s")
        time.sleep(6)

        tdSql.query(f"select * from streamt14 order by 2;")
        tdSql.checkAssert(tdSql.getData(0, 2) == now02)
        tdSql.checkAssert(tdSql.getData(1, 2) == now12)
        tdSql.checkAssert(tdSql.getData(2, 2) == now22)
        tdSql.checkAssert(tdSql.getData(3, 2) == now32)

        tdLog.info(f"step 2 max delay 5s")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test15 vgroups 4;")
        tdSql.execute(f"use test15;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(
            f"create stream stream15 trigger max_delay 5s into streamt13 as select  _wstart, sum(a), now from t1 session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt13;",
            lambda: tdSql.getRows() == 2,
        )

        now02 = tdSql.getData(0, 2)
        now12 = tdSql.getData(1, 2)

        tdLog.info(f"step1 max delay 5s......... sleep 6s")
        time.sleep(6)

        tdSql.query(f"select * from streamt13;")
        tdSql.checkAssert(tdSql.getData(0, 2) == now02)
        tdSql.checkAssert(tdSql.getData(1, 2) == now12)

        tdLog.info(f"session max delay over")

        tdLog.info(f"step 3 max delay 5s")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test16 vgroups 4;")
        tdSql.execute(f"use test16;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(
            f"create stream stream16 trigger max_delay 5s into streamt13 as select  _wstart, sum(a), now from t1 state_window(a);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt13;",
            lambda: tdSql.getRows() == 2,
        )

        now02 = tdSql.getData(0, 2)
        now12 = tdSql.getData(1, 2)

        tdLog.info(f"step1 max delay 5s......... sleep 6s")
        time.sleep(6)

        tdSql.query(f"select * from streamt13;")
        tdSql.checkAssert(tdSql.getData(0, 2) == now02)
        tdSql.checkAssert(tdSql.getData(1, 2) == now12)

        tdLog.info(f"state max delay over")

        tdLog.info(f"step 4 max delay 5s")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test17 vgroups 4;")
        tdSql.execute(f"use test17;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(
            f"create stream stream17 trigger max_delay 5s into streamt13 as select  _wstart, sum(a), now from t1 event_window start with a = 1 end with a = 9;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791233001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233009,9,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt13;",
            lambda: tdSql.getRows() == 2,
        )

        now02 = tdSql.getData(0, 2)
        now12 = tdSql.getData(1, 2)

        tdLog.info(f"step1 max delay 5s......... sleep 6s")
        time.sleep(6)

        tdSql.query(f"select * from streamt13;")
        tdSql.checkAssert(tdSql.getData(0, 2) == now02)
        tdSql.checkAssert(tdSql.getData(1, 2) == now12)
