import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseCheckPoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_checkpoint(self):
        """Stream checkpoint

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointInterval1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointSession0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointSession1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkpointState0.sim

        """

        self.checkpointInterval0()
        self.checkpointInterval1()
        self.checkpointSession0()
        self.checkpointSession1()
        self.checkpointState0()

    def checkpointInterval0(self):
        tdLog.info(f"checkpointInterval0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) from t1 interval(10s);"
        )
        tdSql.execute(
            f"create stream streams1 trigger window_close IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt1 as select  _wstart, count(*) c1, sum(a) from t1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3,
        )
        tdSql.checkResultsByFunc(f"select * from streamt1;", lambda: tdSql.getRows() == 0)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6,
        )

        tdSql.execute(f"insert into t1 values(1648791223003,4,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 4,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6,
        )

        tdLog.info(f"step 2")

        tdLog.info(f"restart taosd 02 ......")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791223004,5,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 9,
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6,
        )

    def checkpointInterval1(self):
        tdLog.info(f"checkpointInterval1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) from st interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3,
        )

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791223003,4,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 4,
        )

    def checkpointSession0(self):
        tdLog.info(f"checkpointSession0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) from t1 session(ts, 10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3,
        )

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6,
        )

        tdSql.execute(f"insert into t1 values(1648791233003,4,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 4,
        )

        tdLog.info(f"step 2")
        tdLog.info(f"restart taosd 02 ......")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791233004,5,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 9,
        )

    def checkpointSession1(self):
        tdLog.info(f"checkpointSession1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) from st session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,2,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3,
        )

        tdLog.info(f"waiting for checkpoint generation 1 ......")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,1.1);")
        tdSql.execute(f"insert into t2 values(1648791233003,4,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 4,
        )

    def checkpointState0(self):
        tdLog.info(f"checkpointState0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step 1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, sum(a) from t1 state_window(b);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3,
        )

        tdLog.info(f"restart taosd 01 ......")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6,
        )

        tdSql.execute(f"insert into t1 values(1648791233003,4,3,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 4,
        )

        tdLog.info(f"step 2")

        tdLog.info(f"restart taosd 02 ......")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791233004,5,3,3,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 9,
        )
