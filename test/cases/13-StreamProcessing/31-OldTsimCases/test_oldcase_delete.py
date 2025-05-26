import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_delete(self):
        """Stream delete

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteScalar.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteSession.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteState.sim

        """

        self.deleteInterval()
        self.deleteScalar()
        self.deleteSession()
        self.deleteState()

    def deleteInterval(self):
        tdLog.info(f"deleteInterval")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  _wstart c1, count(*) c2, max(a) c3 from t1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213003,4,4,4,4.0);")

        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;", lambda: tdSql.getRows() == 2
        )

        tdSql.execute(
            f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213006,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213007,4,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,4,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791233000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791233008,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791233009,4,4,4,4.0);")

        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4,
        )

        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into test.streamt2 as select  _wstart c1, count(*) c2, max(a) c3 from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791213000;")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223003,3,2,3,1.0);")

        tdSql.execute(
            f"delete from t2 where ts >= 1648791223000 and ts <= 1648791223001;"
        )
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(1, 2) == 3,
        )

        tdSql.execute(
            f"delete from st where ts >= 1648791223000 and ts <= 1648791223003;"
        )
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213004,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213006,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223004,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213004,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213005,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213006,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223004,1,2,3,1.0);")
        tdSql.execute(
            f"delete from t2 where ts >= 1648791213004 and ts <= 1648791213006;"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223005,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223006,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223005,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223006,1,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791233005,4,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233006,2,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791233005,5,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791233006,3,2,3,1.0);")
        tdSql.execute(
            f"delete from st where ts >= 1648791213001 and ts <= 1648791233005;"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 3,
        )

        tdSql.execute(f"create database test3  vgroups 4;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into test.streamt3 as select  _wstart c1, count(*) c2, max(a) c3 from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.execute(f"delete from t1;")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from t1 where ts > 100;")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from st;")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

    def deleteScalar(self):
        tdLog.info(f"deleteScalar")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  ts, a, b from t1 partition by a;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,2,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,2,2,3,1.0);")

        tdSql.checkResultsByFunc(f"select * from streamt;", lambda: tdSql.getRows() == 6)

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 2,
        )

        tdLog.info(f"======================step 2")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1  vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt1 subtable(concat("aaa-", cast( a as varchar(10) ))) as select  ts, a, b from t1 partition by a;'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,2,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,2,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 6,
        )

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 2,
        )

        tdLog.info(f"======================step 3")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f'create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt2 subtable("aaa-a") as select  ts, a, b from t1;'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,2,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791213003,0,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213004,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,2,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 6,
        )

        tdLog.info(f"delete from t1 where ts <= 1648791213002;")
        tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 0
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 2,
        )

    def deleteSession(self):
        tdLog.info(f"deleteSession")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  _wstart c1, count(*) c2, max(a) c3 from t1 session(ts, 5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213003,4,4,4,4.0);")
        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(
            f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213006,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213007,4,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,4,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791233000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791233008,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791233009,4,4,4,4.0);")

        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4,
        )

        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into test.streamt2 as select  _wstart c1, count(*) c2, max(a) c3 from st session(ts,5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791213000;")
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223003,3,2,3,1.0);")

        tdSql.execute(
            f"delete from t2 where ts >= 1648791223000 and ts <= 1648791223001;"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None
            and tdSql.getData(1, 1) == 6
            and tdSql.getData(1, 2) == 3,
        )

        tdSql.execute(
            f"delete from st where ts >= 1648791223000 and ts <= 1648791223003;"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None,
        )

        tdSql.execute(f"insert into t1 values(1648791213004,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213006,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223004,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213004,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213005,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213006,3,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223004,1,2,3,1.0);")

        tdSql.execute(
            f"delete from t2 where ts >= 1648791213004 and ts <= 1648791213006;"
        )

        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791223005,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223006,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223005,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223006,1,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791233005,4,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233006,2,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791233005,5,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791233006,3,2,3,1.0);")

        tdSql.execute(
            f"delete from st where ts >= 1648791213001 and ts <= 1648791233005;"
        )
        tdSql.checkResultsByFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == None
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 3,
        )

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into test.streamt3 as select  _wstart c1, count(*) c2, max(a) c3 from st session(ts,5s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000,1,1,1,NULL);")
        tdSql.execute(f"insert into t1 values(1648791210001,2,2,2,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213001,3,3,3,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213003,4,4,4,NULL);")
        tdSql.execute(f"insert into t1 values(1648791216000,5,5,5,NULL);")
        tdSql.execute(f"insert into t1 values(1648791216002,6,6,6,NULL);")
        tdSql.execute(f"insert into t1 values(1648791216004,7,7,7,NULL);")
        tdSql.execute(f"insert into t2 values(1648791218001,8,8,8,NULL);")
        tdSql.execute(f"insert into t2 values(1648791218003,9,9,9,NULL);")
        tdSql.execute(f"insert into t1 values(1648791222000,10,10,10,NULL);")
        tdSql.execute(f"insert into t1 values(1648791222003,11,11,11,NULL);")
        tdSql.execute(f"insert into t1 values(1648791222005,12,12,12,NULL);")

        tdSql.execute(f"insert into t1 values(1648791232005,13,13,13,NULL);")
        tdSql.execute(f"insert into t2 values(1648791242005,14,14,14,NULL);")

        tdSql.checkResultsByFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 3,
        )

        tdSql.execute(
            f"delete from t2 where ts >= 1648791213001 and ts <= 1648791218003;"
        )
        tdSql.checkResultsByFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 7
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(2, 2) == 12
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 13
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(4, 2) == 14,
        )

        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"create database test4  vgroups 1;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdLog.info(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt4 as select  _wstart, count(*) c1 from st partition by tbname session(ts, 2s);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt4 as select  _wstart, count(*) c1 from st partition by tbname session(ts, 2s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791220000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791222000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791223000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791231000,2,2,3);")

        tdSql.execute(f"insert into t2 values(1648791210000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791220000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791231000,2,2,3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by c1 desc;;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(5, 1) == 1,
        )

        tdLog.info(f"delete from st where ts >= 1648791220000 and ts <=1648791223000;")
        tdSql.execute(
            f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by c1 desc;;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 1,
        )

    def deleteState(self):
        tdLog.info(f"deleteState")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  _wstart c1, count(*) c2, max(b) c3 from t1 state_window(a);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,1,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213002,1,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213003,1,4,4,4.0);")

        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,2,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,2,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,2,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(
            f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,1,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213005,1,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791213006,1,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791213007,1,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,2,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,2,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,2,4,4,4.0);")

        tdSql.execute(f"insert into t1 values(1648791233000,3,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,3,2,2,2.0);")
        tdSql.execute(f"insert into t1 values(1648791233008,3,3,3,3.0);")
        tdSql.execute(f"insert into t1 values(1648791233009,3,4,4,4.0);")

        tdSql.execute(
            f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4,
        )

        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"create database test4  vgroups 1;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdLog.info(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt4 as select  _wstart, count(*) c1 from st partition by tbname state_window(c);"
        )
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt4 as select  _wstart, count(*) c1 from st partition by tbname state_window(c);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000,1,2,1);")
        tdSql.execute(f"insert into t1 values(1648791220000,2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791221000,2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791222000,2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791223000,2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791231000,2,2,3);")

        tdSql.execute(f"insert into t2 values(1648791210000,1,2,1);")
        tdSql.execute(f"insert into t2 values(1648791220000,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791221000,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791231000,2,2,3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by c1 desc;;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(5, 1) == 1,
        )

        tdLog.info(f"delete from st where ts >= 1648791220000 and ts <=1648791223000;")
        tdSql.execute(
            f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by c1 desc;;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 1,
        )
