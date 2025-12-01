import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)

class TestStreamOldCasePartitionBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_partitionby(self):
        """OldTsim: partition by

        Validate the calculation results under PARTITION BY clauses

        Catalog:
            - Streams:OldTsimCases
            
        Since: v3.3.7.0
        
        Labels: common, ci
        
        Jira: None
        
        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionby.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionby1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionbyColumnInterval.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionbyColumnOther.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionbyColumnSession.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/partitionbyColumnState.sim
        """

        tdStream.createSnode()

        self.partitionby()
        # self.partitionby1()
        # self.partitionbyColumnInterval()
        # self.partitionbyColumnOther()
        # self.partitionbyColumnSession()
        # self.partitionbyColumnState()

    def partitionby(self):
        tdLog.info(f"partitionby")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"create database test0 vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
        tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")
        tdSql.execute(
            f"create stream stream_t1 interval(10s) sliding(10s) from st partition by ta, tb, tc stream_options(max_delay(3s)) into test0.streamtST1 as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from st where ta=%%1 tb=%%2 tb=%%3 and ts >= _twstart and ts < _twend;"
        )
        tdSql.pause()
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test0.streamtST1;",
            lambda: tdSql.getRows() == 4,
        )

        tdSql.execute(f"insert into ts1 values(1648791223001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts2 values(1648791223001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts3 values(1648791223001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts4 values(1648791223001, 1, 12, 3, 1.0);")

        time.sleep(1)
        tdSql.execute(f"delete from st where ts = 1648791223001;")
        tdSql.checkResultsByFunc(
            f"select * from test0.streamtST1;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"=====loop0")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 2, 3);")
        tdSql.execute(f"create table ts2 using st tags(1, 3, 4);")
        tdSql.execute(f"create table ts3 using st tags(1, 4, 5);")

        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, count(*) c1, count(a) c2 from st partition by ta, tb, tc interval(10s);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"=====loop1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, id int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream stream_t2 trigger at_once  watermark 20s IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST as select _wstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6, max(id) c7 from st partition by ta interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3, 1);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3, 2);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3, 3);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3, 4);")

        tdSql.execute(f"insert into ts2 values(1648791222002, 2, 2, 3, 5);")
        tdSql.execute(f"insert into ts2 values(1648791222002, 2, 2, 3, 6);")

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3, 7);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3, 8);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3, 9);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3, 10);")

        tdSql.checkResultsByFunc(
            f"select * from streamtST order by c7 asc;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(1, 3) == 2
            and tdSql.getData(2, 3) == 1
            and tdSql.getData(3, 3) == 4,
        )

    def partitionby1(self):
        tdLog.info(f"partitionby1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
        tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")
        tdSql.execute(
            f"create stream stream_t1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST1 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from st partition by tbname interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamtST1;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"=====loop0")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 2, 3);")
        tdSql.execute(f"create table ts2 using st tags(1, 3, 4);")
        tdSql.execute(f"create table ts3 using st tags(1, 4, 5);")

        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart, count(*) c1, count(a) c2 from st partition by tbname interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")

        tdSql.checkResultsByFunc(f"select * from streamt;", lambda: tdSql.getRows() == 2)

        tdLog.info(f"=====loop1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, id int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream stream_t2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamtST as select _wstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6, max(id) c7 from st partition by tbname interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3, 1);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3, 2);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3, 3);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3, 4);")

        tdSql.execute(f"insert into ts2 values(1648791222002, 2, 2, 3, 5);")
        tdSql.execute(f"insert into ts2 values(1648791222002, 2, 2, 3, 6);")

        tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3, 1);")
        tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3, 2);")
        tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3, 3);")
        tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3, 4);")

        tdSql.checkResultsByFunc(
            f"select * from streamtST;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == 2,
        )

    def partitionbyColumnInterval(self):
        tdLog.info(f"partitionbyColumnInterval")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3, _group_key(a) c4 from t1 partition by a interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 3,
        )

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1 buffer 32;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart c1, count(*) c2, max(c) c3, _group_key(a+b) c4 from t1 partition by a+b interval(10s)"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 1, 2, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 2.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 4, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 5, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 1, 2, 5, 2.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 6, 2.0) (1648791223002, 1, 1, 7, 2.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 7
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 5,
        )

        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 2 buffer 32;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt2 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )
        tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t2 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 3
            and tdSql.getData(4, 1) == 4
            and tdSql.getData(4, 2) == 1,
        )

        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"create database test4 vgroups 2 buffer 32;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t4 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt4 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt4 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"insert into t4 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt4 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(2, 1) == 1,
        )

        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop database if exists test5;")
        tdSql.execute(f"create database test5 vgroups 2 buffer 32;")
        tdSql.execute(f"use test5;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t4 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt5 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791213000, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791213000, 4, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791223000, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791223000, 4, 2, 3, 1.0);")

        time.sleep(1)
        tdSql.execute(f"delete from st where ts = 1648791223000;")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt5 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 11, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 21, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791223001, 31, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791223001, 41, 2, 3, 1.0);")

        time.sleep(1)
        tdSql.execute(f"delete from st where ts = 1648791223001;")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt5 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223001, 12, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 22, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791223001, 32, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791223001, 42, 2, 3, 1.0);")

        time.sleep(1)
        tdSql.execute(f"delete from st where ts = 1648791223001;")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt5 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"================step2")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test6 vgroups 2 buffer 32;")
        tdSql.execute(f"use test6;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f'create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 subtable("aaa-a") as select _wstart, count(*) from t1 partition by a interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

    def partitionbyColumnOther(self):
        tdLog.info(f"partitionbyColumnOther")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"================step1")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test0 vgroups 2  buffer 32;")
        tdSql.execute(f"use test0;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f'create stream streams0 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 watermark 100s into streamt0 subtable("aaa-a") as select _wstart, count(*) from t1 partition by a count_window(10);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt0 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

        tdLog.info(f"================step1")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 2  buffer 32;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 subtable("aaa-a") as select _wstart, count(*) from t1 partition by a event_window start with b = 2 end with b = 2;'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

    def partitionbyColumnSession(self):
        tdLog.info(f"partitionbyColumnSession")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3, _group_key(a) c4 from t1 partition by a session(ts, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 3,
        )

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart c1, count(*) c2, max(c) c3, _group_key(a+b) c4 from t1 partition by a+b session(ts, 5s)"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 1, 2, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 2.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 4, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 5, 2.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 1, 2, 5, 2.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 6, 2.0) (1648791223002, 1, 1, 7, 2.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 7,
        )

        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt2 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a session(ts, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213002, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )
        tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t2 values(1648791213001, 1, 2, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 1) == 2
            and tdSql.getData(4, 2) == 3,
        )

        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"create database test4 vgroups 4;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t3 using st tags(2, 2, 2);")
        tdSql.execute(f"create table t4 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into test.streamt4 as select _wstart c1, count(*) c2, max(a) c3 from st partition by a session(ts, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t3 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t4 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt4 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"insert into t4 values(1648791213000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt4 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(2, 1) == 1,
        )

        tdLog.info(f"================step2")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test5;")
        tdSql.execute(f"create database test5 vgroups 4;")
        tdSql.execute(f"use test5;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f'create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 subtable("aaa-a") as select _wstart, count(*) from t1 partition by a session(ts, 10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

    def partitionbyColumnState(self):
        tdLog.info(f"partitionbyColumnState")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3, _group_key(a) c4 from t1 partition by a state_window(b);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 1, 3, 1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 2, 1, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 1, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 1, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 1, 1, 3, 1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 3, 1.0) (1648791223001, 2, 2, 3, 1.0) (1648791223003, 1, 2, 3, 1.0);"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 3,
        )

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1 vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d int);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 as select _wstart c1, count(*) c2, max(d) c3, _group_key(a+b) c4 from t1 partition by a+b state_window(c)"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 1, 1);")
        tdSql.execute(f"insert into t1 values(1648791213001, 2, 1, 1, 2);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 1, 3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 2, 4);")
        tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 2, 5);")
        tdSql.execute(f"insert into t1 values(1648791223002, 1, 2, 2, 6);")
        tdSql.execute(
            f"insert into t1 values(1648791213001, 1, 1, 1, 7) (1648791223002, 1, 1, 2, 8);"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by c1, c4, c2, c3;",
            lambda: tdSql.getRows() > 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 7
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 8,
        )

        tdLog.info(f"================step2")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f'create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt6 subtable("aaa-a") as select _wstart, count(*) from t1 partition by a session(ts, 10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )
