import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_delete(self):
        """OldTsim: delete data

        Test the correctness of results when deleting data in various trigger windows

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/deleteInterval.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/deleteScalar.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/deleteSession.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/deleteState.sim

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Interval0()) recalculate
        # streams.append(self.Interval1()) recalculate
        # streams.append(self.Scalar0()) recalculate
        # streams.append(self.Scalar1()) recalculate
        # streams.append(self.Scalar2()) recalculate
        # streams.append(self.Session0()) recalculate
        # streams.append(self.Session1()) recalculate
        # streams.append(self.Session2()) recalculate
        # streams.append(self.Session3()) recalculate
        # streams.append(self.State0()) recalculate
        # streams.append(self.State1()) recalculate
        tdStream.checkAll(streams)

    class Interval0(StreamCheckItem):
        def __init__(self):
            self.db = "Interval0"

        def create(self):
            tdSql.execute(f"create database interval0 vgroups 1 buffer 8;")
            tdSql.execute(f"use interval0;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams0 interval(10s) sliding(10s) from t1 stream_options(max_delay(3s) | delete_recalc) into streamt as select _twstart c1, count(*) c2, max(a) c3 from %%trows;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213003, 4, 4, 4, 4.0);")

        def insert5(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
            )

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2,
            )

        def insert7(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
            )

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert8(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213006, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213007, 4, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 4, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791233000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791233008, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791233009, 4, 4, 4, 4.0);")

        def insert9(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
            )

        def check9(self):
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
            tdSql.execute(f"create database test2 vgroups 4;")
            tdSql.execute(f"create database test vgroups 1;")
            tdSql.execute(f"use test2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart c1, count(*) c2, max(a) c3 from st interval(10s);"
            )

            tdStream.checkStreamStatus()

            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1,
            )

            tdSql.execute(f"delete from t1 where ts = 1648791213000;")
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

            tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223003, 3, 2, 3, 1.0);")

            tdSql.execute(
                f"delete from t2 where ts >= 1648791223000 and ts <= 1648791223001;"
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
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
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

            tdSql.execute(f"insert into t1 values(1648791213004, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213006, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223004, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213004, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213005, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213006, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223004, 1, 2, 3, 1.0);")
            tdSql.execute(
                f"delete from t2 where ts >= 1648791213004 and ts <= 1648791213006;"
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 3
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 1,
            )

            tdSql.execute(f"insert into t1 values(1648791223005, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223006, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223005, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223006, 1, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791233005, 4, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233006, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791233005, 5, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791233006, 3, 2, 3, 1.0);")
            tdSql.execute(
                f"delete from st where ts >= 1648791213001 and ts <= 1648791233005;"
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 3,
            )

    class Interval1(StreamCheckItem):
        def __init__(self):
            self.db = "Interval1"

        def create(self):
            tdSql.execute(f"create database interval1 vgroups 4 buffer 8;")
            tdSql.execute(f"use interval1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart c1, count(*) c2, max(a) c3 from st interval(10s);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def insert2(self):
            tdSql.execute(f"delete from t1;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1,
            )

        def insert3(self):
            tdSql.execute(f"delete from t1 where ts > 100;")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1,
            )

        def insert4(self):
            tdSql.execute(f"delete from st;")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

    class Scalar0(StreamCheckItem):
        def __init__(self):
            self.db = "Scalar0"

        def create(self):
            tdSql.execute(f"create database scalar0 vgroups 4 buffer 8;")
            tdSql.execute(f"use scalar0;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select ts, a, b from t1 partition by a;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 6
            )

        def insert2(self):
            tdLog.info(f"delete from t1 where ts <= 1648791213002;")
            tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 0
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 2,
            )

    class Scalar1(StreamCheckItem):
        def __init__(self):
            self.db = "Scalar1"

        def create(self):
            tdSql.execute(f"create database scalar1 vgroups 4 buffer 8;")
            tdSql.execute(f"use scalar1;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1 subtable(concat("aaa-", cast( a as varchar(10) ))) as select ts, a, b from t1 partition by a;'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 6,
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1 order by 1;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 0
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 2,
            )

    class Scalar2(StreamCheckItem):
        def __init__(self):
            self.db = "Scalar2"

        def create(self):
            tdSql.execute(f"create database scalar2 vgroups 1 buffer 8;")
            tdSql.execute(f"use scalar2;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f'create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 subtable("aaa-a") as select ts, a, b from t1;'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 2, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791213003, 0, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213004, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 6,
            )

        def insert2(self):
            tdLog.info(f"delete from t1 where ts <= 1648791213002;")
            tdSql.execute(f"delete from t1 where ts <= 1648791213002;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 0
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 2,
            )

    class Session0(StreamCheckItem):
        def __init__(self):
            self.db = "Session0"

        def create(self):
            tdSql.execute(f"create database session0 vgroups 1 buffer 8;")
            tdSql.execute(f"use session0;")
            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(a) c3 from t1 session(ts, 5s);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213003, 4, 4, 4, 4.0);")

        def insert5(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
            )

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2,
            )

        def insert7(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
            )

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert8(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213006, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213007, 4, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 4, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791233000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791233008, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791233009, 4, 4, 4, 4.0);")

        def insert9(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
            )

        def check9(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 4,
            )

    class Session1(StreamCheckItem):
        def __init__(self):
            self.db = "Session1"

        def create(self):
            tdSql.execute(f"create database session1 vgroups 1 buffer 8;")
            tdSql.execute(f"use session1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt2 as select _wstart c1, count(*) c2, max(a) c3 from st session(ts, 5s);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into t2 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1,
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223000, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223002, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223003, 3, 2, 3, 1.0);")

        def insert4(self):
            tdSql.execute(
                f"delete from t2 where ts >= 1648791223000 and ts <= 1648791223001;"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None
                and tdSql.getData(1, 1) == 6
                and tdSql.getData(1, 2) == 3,
            )

        def insert5(self):
            tdSql.execute(
                f"delete from st where ts >= 1648791223000 and ts <= 1648791223003;"
            )

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 1
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791213004, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213006, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223004, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213004, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213005, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213006, 3, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223004, 1, 2, 3, 1.0);")

        def insert7(self):
            tdSql.execute(
                f"delete from t2 where ts >= 1648791213004 and ts <= 1648791213006;"
            )

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(0, 2) == 3
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 1,
            )

        def insert8(self):
            tdSql.execute(f"insert into t1 values(1648791223005, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223006, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223005, 1, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223006, 1, 2, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791233005, 4, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233006, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791233005, 5, 2, 3, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791233006, 3, 2, 3, 1.0);")

        def insert9(self):
            tdSql.execute(
                f"delete from st where ts >= 1648791213001 and ts <= 1648791233005;"
            )

        def check9(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == None
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 3,
            )

    class Session2(StreamCheckItem):
        def __init__(self):
            self.db = "Session2"

        def create(self):
            tdSql.execute(f"create database session2 vgroups 1 buffer 8;")
            tdSql.execute(f"use session2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3 as select _wstart c1, count(*) c2, max(a) c3 from st session(ts, 5s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791210000, 1, 1, 1, NULL);")
            tdSql.execute(f"insert into t1 values(1648791210001, 2, 2, 2, NULL);")
            tdSql.execute(f"insert into t2 values(1648791213001, 3, 3, 3, NULL);")
            tdSql.execute(f"insert into t2 values(1648791213003, 4, 4, 4, NULL);")
            tdSql.execute(f"insert into t1 values(1648791216000, 5, 5, 5, NULL);")
            tdSql.execute(f"insert into t1 values(1648791216002, 6, 6, 6, NULL);")
            tdSql.execute(f"insert into t1 values(1648791216004, 7, 7, 7, NULL);")
            tdSql.execute(f"insert into t2 values(1648791218001, 8, 8, 8, NULL);")
            tdSql.execute(f"insert into t2 values(1648791218003, 9, 9, 9, NULL);")
            tdSql.execute(f"insert into t1 values(1648791222000, 10, 10, 10, NULL);")
            tdSql.execute(f"insert into t1 values(1648791222003, 11, 11, 11, NULL);")
            tdSql.execute(f"insert into t1 values(1648791222005, 12, 12, 12, NULL);")

            tdSql.execute(f"insert into t1 values(1648791232005, 13, 13, 13, NULL);")
            tdSql.execute(f"insert into t2 values(1648791242005, 14, 14, 14, NULL);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by c1, c2, c3;",
                lambda: tdSql.getRows() == 3,
            )

        def insert2(self):
            tdSql.execute(
                f"delete from t2 where ts >= 1648791213001 and ts <= 1648791218003;"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by c1, c2, c3;",
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

    class Session3(StreamCheckItem):
        def __init__(self):
            self.db = "Session3"

        def create(self):
            tdSql.execute(f"create database session3 vgroups 1 buffer 8;")
            tdSql.execute(f"use session3;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdLog.info(
                f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart, count(*) c1 from st partition by tbname session(ts, 2s);"
            )
            tdSql.execute(
                f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart, count(*) c1 from st partition by tbname session(ts, 2s);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791222000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 3);")
            tdSql.execute(f"insert into t1 values(1648791231000, 2, 2, 3);")

            tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791220000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791221000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1648791231000, 2, 2, 3);")

        def check1(self):
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

        def insert2(self):
            tdLog.info(
                f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
            )
            tdSql.execute(
                f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by c1 desc;;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(3, 1) == 1,
            )

    class State0(StreamCheckItem):
        def __init__(self):
            self.db = "State0"

        def create(self):
            tdSql.execute(f"create database state0 vgroups 1 buffer 8;")
            tdSql.execute(f"use state0;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams0 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart c1, count(*) c2, max(b) c3 from t1 state_window(a);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(1648791213000, NULL, NULL, NULL, NULL);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 0,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 1, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213002, 1, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213003, 1, 4, 4, 4.0);")

            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791213002;"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert5(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 2, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 2, 2, 3, 1.0);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2,
            )

        def insert6(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791223000 and ts <= 1648791223003;"
            )

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(0, 2) == 4,
            )

        def insert7(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 1, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213005, 1, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791213006, 1, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791213007, 1, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 2, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 2, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791223002, 2, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791223003, 2, 4, 4, 4.0);")

            tdSql.execute(f"insert into t1 values(1648791233000, 3, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791233001, 3, 2, 2, 2.0);")
            tdSql.execute(f"insert into t1 values(1648791233008, 3, 3, 3, 3.0);")
            tdSql.execute(f"insert into t1 values(1648791233009, 3, 4, 4, 4.0);")

        def insert8(self):
            tdSql.execute(
                f"delete from t1 where ts >= 1648791213001 and ts <= 1648791233005;"
            )

        def check8(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by c1, c2, c3;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 4,
            )

    class State1(StreamCheckItem):
        def __init__(self):
            self.db = "State1"

        def create(self):
            tdSql.execute(f"create database state1 vgroups 1 buffer 8;")
            tdSql.execute(f"use state1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdLog.info(
                f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart, count(*) c1 from st partition by tbname state_window(c);"
            )
            tdSql.execute(
                f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt4 as select _wstart, count(*) c1 from st partition by tbname state_window(c);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 1);")
            tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 2);")
            tdSql.execute(f"insert into t1 values(1648791221000, 2, 2, 2);")
            tdSql.execute(f"insert into t1 values(1648791222000, 2, 2, 2);")
            tdSql.execute(f"insert into t1 values(1648791223000, 2, 2, 2);")
            tdSql.execute(f"insert into t1 values(1648791231000, 2, 2, 3);")

            tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 1);")
            tdSql.execute(f"insert into t2 values(1648791220000, 2, 2, 2);")
            tdSql.execute(f"insert into t2 values(1648791221000, 2, 2, 2);")
            tdSql.execute(f"insert into t2 values(1648791231000, 2, 2, 3);")

        def check1(self):
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

        def insert2(self):
            tdLog.info(
                f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
            )
            tdSql.execute(
                f"delete from st where ts >= 1648791220000 and ts <=1648791223000;"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by c1 desc;;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(3, 1) == 1,
            )
