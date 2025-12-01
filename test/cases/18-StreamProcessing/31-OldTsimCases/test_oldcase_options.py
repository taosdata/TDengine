import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_options(self):
        """OldTsim: options

        Validate the calculation results when ignore update and ignore delete are applied

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/ignoreCheckUpdate.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/ignoreExpiredData.sim

        """

        tdStream.createSnode()

        self.ignoreCheckUpdate()
        # self.ignoreExpiredData()

    def ignoreCheckUpdate(self):
        tdLog.info(f"ignoreCheckUpdate")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1 start")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int);")

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(
            f"create stream streams0 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s)|ignore_update) into streamt as select _twstart c1, count(*) c2, max(b) c3 from t1 where ts >= _twstart and ts < _twend;"
        )

        tdSql.pause()
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1);")
        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 2);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 3, 3, 3);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 3,
        )

        tdLog.info(f"step 1 end")
        tdLog.info(f"step 2 start")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int);")

        tdLog.info(
            f"create stream streams1 trigger at_once ignore update 1 into streamt1 as select _wstart c1, count(*) c2, max(b) c3 from t1 session(ts, 10s);"
        )

        tdSql.execute(
            f"create stream streams1 trigger at_once ignore update 1 into streamt1 as select _wstart c1, count(*) c2, max(b) c3 from t1 session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1);")
        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 2);")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 3, 3, 3);")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 3,
        )

        tdLog.info(f"step 2 end")
        tdLog.info(f"step 3 start")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int);")

        tdLog.info(
            f"create stream streams2 trigger at_once ignore update 1 into streamt2 as select _wstart c1, count(*) c2, max(b) c3 from t1 state_window(c);"
        )

        tdSql.execute(
            f"create stream streams2 trigger at_once ignore update 1 into streamt2 as select _wstart c1, count(*) c2, max(b) c3 from t1 state_window(c);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1);")
        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 3, 3, 1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 3,
        )

        tdLog.info(f"step 3 end")
        tdLog.info(f"step 4 start")

        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdLog.info(
            f"create stream streams3 trigger at_once ignore update 1 into streamt3 as select _wstart c1, count(*) c2, max(b) c3 from st interval(10s);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once ignore update 1 into streamt3 as select _wstart c1, count(*) c2, max(b) c3 from st interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1);")
        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 2);")
        tdSql.execute(f"insert into t2 values(1648791213000, 1, 1, 1);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 2);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 3, 3, 3);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 5
            and tdSql.getData(0, 2) == 3,
        )

        tdSql.execute(f"insert into t2 values(1648791213000, 4, 4, 4);")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2, 3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 6
            and tdSql.getData(0, 2) == 4,
        )

    def ignoreExpiredData(self):
        tdLog.info(f"ignoreExpiredData")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use test")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 into streamt1 as select _wstart, count(*) c1, sum(a) c3 from t1 interval(10s);"
        )
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 1 into streamt2 as select _wstart, count(*) c1, sum(a) c3 from t1 session(ts, 10s);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 1 into streamt3 as select _wstart, count(*) c1, sum(a) c3 from t1 state_window(a);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002, 2, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003, 2, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791200000, 4, 2, 3, 4.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;", lambda: tdSql.getRows() == 4
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt2;", lambda: tdSql.getRows() == 4
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt3;", lambda: tdSql.getRows() == 2
        )

        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 4")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdLog.info(f"======database={tdSql.getRows()})")

        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream stream_t1 trigger at_once IGNORE EXPIRED 1 into streamtST1 as select _wstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6 from st interval(10s) ;"
        )
        tdSql.execute(
            f"create stream stream_t2 trigger at_once IGNORE EXPIRED 1 into streamtST2 as select _wstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6 from st session(ts, 10s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        time.sleep(1)
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")
        time.sleep(1)
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3);")
        time.sleep(1)
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")
        time.sleep(1)

        tdSql.checkResultsByFunc(
            f"select * from streamtST1;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamtST2;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2,
        )

        tdLog.info(f"=============== create database test2")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 4")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdLog.info(f"======database={tdSql.getRows()})")

        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table ts3 using st tags(3, 3, 3);")
        tdSql.execute(f"create table ts4 using st tags(4, 4, 4);")
        tdSql.execute(
            f"create stream streams_21 trigger at_once IGNORE EXPIRED 1 into streamt_21 as select _wstart, count(*) c1 from st interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791211001, 2, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791211002, 2, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791211003, 2, 2, 3);")
        tdSql.execute(f"insert into ts1 values(1648791211004, 2, 2, 3);")

        tdSql.execute(f"insert into ts2 values(1648791201000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791201001, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791201002, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791201003, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791201004, 2, 2, 3);")

        tdSql.execute(f"insert into ts2 values(1648791101000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791101001, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791101002, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791101003, 2, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791101004, 2, 2, 3);")

        tdLog.info(f"1 select * from streamt_21;")
        tdSql.checkResultsByFunc(
            f"select * from streamt_21;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 5
            and tdSql.getData(1, 1) == 5,
        )

        tdSql.execute(f"insert into ts3 values(1648791241000, 1, 2, 3);")
        tdSql.execute(f"insert into ts3 values(1648791231001, 2, 2, 3);")
        tdSql.execute(f"insert into ts3 values(1648791231002, 2, 2, 3);")
        tdSql.execute(f"insert into ts3 values(1648791231003, 2, 2, 3);")
        tdSql.execute(f"insert into ts3 values(1648791231004, 2, 2, 3);")

        tdLog.info(f"2 select * from streamt_21;")
        tdSql.checkResultsByFunc(
            f"select * from streamt_21;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(2, 1) == 1,
        )

        tdSql.execute(f"insert into ts4 values(1648791231001, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791231002, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791231003, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791231004, 2, 2, 3);")

        tdSql.execute(f"insert into ts4 values(1648791211001, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791211002, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791211003, 2, 2, 3);")
        tdSql.execute(f"insert into ts4 values(1648791211004, 2, 2, 3);")

        tdLog.info(f"3 select * from streamt_21;")
        tdSql.checkResultsByFunc(
            f"select * from streamt_21;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 1,
        )
