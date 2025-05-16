import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_delete(self):
        """Stream delete

        1. -

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteScalar.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteSession.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/deleteState.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/drop_stream.sim
        """

        self.deleteInterval()
        # self.deleteScalar()
        # self.deleteSession()
        # self.deleteState()
        # self.drop_stream()

    def deleteInterval(self):
        tdLog.info(f"deleteInterval")
        clusterComCheck.drop_all_streams_and_dbs()

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

        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        tdSql.queryCheckFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
            f"select * from streamt order by c1, c2, c3;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223002,3,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,2,3,1.0);")

        tdSql.queryCheckFunc(
            f"select * from streamt order by c1, c2, c3;", lambda: tdSql.getRows() == 2
        )

        tdSql.execute(
            f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
        )
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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

        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.queryCheckFunc(
            f"select * from test.streamt2 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791213000;")
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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

        tdSql.queryCheckFunc(
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

        tdSql.queryCheckFunc(
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

        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL,NULL);")
        tdSql.execute(f"insert into t2 values(1648791213000,NULL,NULL,NULL,NULL);")

        tdSql.execute(f"delete from t1;")

        tdSql.queryCheckFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from t1 where ts > 100;")
        tdSql.queryCheckFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 1,
        )

        tdSql.execute(f"delete from st;")

        tdSql.queryCheckFunc(
            f"select * from test.streamt3 order by c1, c2, c3;",
            lambda: tdSql.getRows() == 0,
        )

    def deleteScalar(self):
        tdLog.info(f"deleteScalar")
        clusterComCheck.drop_all_streams_and_dbs()

    def deleteSession(self):
        tdLog.info(f"deleteSession")
        clusterComCheck.drop_all_streams_and_dbs()

    def deleteState(self):
        tdLog.info(f"deleteState")
        clusterComCheck.drop_all_streams_and_dbs()

    def drop_stream(self):
        tdLog.info(f"drop_stream")
        clusterComCheck.drop_all_streams_and_dbs()
