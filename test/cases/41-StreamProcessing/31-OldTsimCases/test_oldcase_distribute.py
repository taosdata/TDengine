import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseDistribute:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_distribute(self):
        """OldTsim: distribute

        Perform multiple write triggers to verify the correctness of the calculation results

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/distributeInterval0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/distributeIntervalRetrive0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/distributeMultiLevelInterval0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/distributeSession0.sim

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.Interval0()) pass
        # streams.append(self.Interval1()) pass
        # streams.append(self.Interval2()) pass
        # streams.append(self.Retrive0()) pass
        # streams.append(self.Retrive1()) pass
        # streams.append(self.Multi0())
        # streams.append(self.Session0())
        # streams.append(self.Session1())
        tdStream.checkAll(streams)

    class Interval0(StreamCheckItem):
        def __init__(self):
            self.db = "Interval0"

        def create(self):
            tdSql.execute(f"create database interval0 vgroups 2 buffer 8;")
            tdSql.execute(f"use interval0;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")

            tdSql.execute(
                f"create stream stream_t1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamtST1 as select _twstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

            tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

            tdSql.execute(
                f"insert into ts1 values(1648791213002, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791213002, NULL, NULL, NULL, NULL);"
            )

            tdSql.execute(
                f"insert into ts3 values(1648791213002, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into ts4 values(1648791213002, NULL, NULL, NULL, NULL);"
            )

        def check1(self):
            tdLog.info(f"1 select * from streamtST1;")
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
            )

        def insert2(self):
            tdSql.execute(f"insert into ts1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts1 values(1648791233003, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into ts2 values(1648791243004, 4, 2, 43, 73.1);")
            tdSql.execute(f"insert into ts1 values(1648791213002, 24, 22, 23, 4.1);")
            tdSql.execute(f"insert into ts1 values(1648791243005, 4, 20, 3, 3.1);")
            tdSql.execute(
                f"insert into ts2 values(1648791243006, 4, 2, 3, 3.1) (1648791243007, 4, 2, 3, 3.1) ;"
            )
            tdSql.execute(
                f"insert into ts1 values(1648791243008, 4, 2, 30, 3.1) (1648791243009, 4, 2, 3, 3.1)  (1648791243010, 4, 2, 3, 3.1)  ;"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791243011, 4, 2, 3, 3.1) (1648791243012, 34, 32, 33, 3.1)  (1648791243013, 4, 2, 3, 3.1) (1648791243014, 4, 2, 13, 3.1);"
            )

        def check2(self):
            tdLog.info(f"2 select * from streamtST1;")
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(3, 1) == 11,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into ts1 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(3, 1) == 13,
            )

        def insert4(self):
            tdSql.execute(
                f"insert into ts2 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 15,
            )

        def insert5(self):
            tdSql.execute(
                f"insert into ts1 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
            )

        def check5(self):
            tdLog.info(f"5 select * from streamtST1;")
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 15,
            )

        def insert6(self):
            tdSql.execute(f"insert into ts3 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts4 values(1648791233003, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into ts3 values(1648791243004, 4, 2, 43, 73.1);")
            tdSql.execute(f"insert into ts4 values(1648791213002, 24, 22, 23, 4.1);")

        def check6(self):
            tdLog.info(f"6-0 select * from streamtST1;")
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4 and tdSql.getData(0, 1) == 8,
            )

        def insert7(self):
            tdSql.execute(f"insert into ts3 values(1648791243005, 4, 20, 3, 3.1);")
            tdSql.execute(
                f"insert into ts4 values(1648791243006, 4, 2, 3, 3.1) (1648791243007, 4, 2, 3, 3.1) ;"
            )
            tdSql.execute(
                f"insert into ts3 values(1648791243008, 4, 2, 30, 3.1) (1648791243009, 4, 2, 3, 3.1)  (1648791243010, 4, 2, 3, 3.1)  ;"
            )
            tdSql.execute(
                f"insert into ts4 values(1648791243011, 4, 2, 3, 3.1) (1648791243012, 34, 32, 33, 3.1)  (1648791243013, 4, 2, 3, 3.1) (1648791243014, 4, 2, 13, 3.1);"
            )

        def check7(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4 and tdSql.getData(0, 1) == 8,
            )

        def insert8(self):
            tdSql.execute(
                f"insert into ts3 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
            )

        def check8(self):
            tdLog.info(f"6 select * from streamtST1;")
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(1, 1) == 5
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(3, 1) == 28,
            )

        def insert9(self):
            tdSql.execute(
                f"insert into ts4 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
            )
            tdSql.execute(
                f"insert into ts3 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
            )

        def check9(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 8
                and tdSql.getData(0, 2) == 6
                and tdSql.getData(0, 3) == 52
                and tdSql.getData(0, 4) == 52
                and tdSql.getData(0, 5) == 13
                and tdSql.getData(1, 1) == 6
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(1, 3) == 92
                and tdSql.getData(1, 4) == 22
                and tdSql.getData(1, 5) == 3
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(2, 2) == 4
                and tdSql.getData(2, 3) == 32
                and tdSql.getData(2, 4) == 12
                and tdSql.getData(2, 5) == 3
                and tdSql.getData(3, 1) == 30
                and tdSql.getData(3, 2) == 30
                and tdSql.getData(3, 3) == 180
                and tdSql.getData(3, 4) == 42
                and tdSql.getData(3, 5) == 3,
            )

            tdSql.query(
                f"select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5, avg(d) from st interval(10s);"
            )

    class Interval1(StreamCheckItem):
        def __init__(self):
            self.db = "Interval1"

        def create(self):
            tdSql.execute(f"create database interval1 vgroups 2 buffer 8;")
            tdSql.execute(f"use interval1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream stream_t2 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamtST1 as select _twstart, count(*) c1, count(a) c2, sum(a) c3, max(b) c5, min(c) c6 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")
            tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 2,
            )

    class Interval2(StreamCheckItem):
        def __init__(self):
            self.db = "Interval2"

        def create(self):
            tdSql.execute(f"create database interval2 vgroups 4 buffer 8;")
            tdSql.execute(f"use interval2;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream stream_t3 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamtST3 as select ts, min(a) c6, a, b, c, ta, tb, tc from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into ts1 values(1648791222001, 2, 2, 3);")
            tdSql.execute(f"insert into ts2 values(1648791211000, 1, 2, 3);")
            tdSql.execute(f"insert into ts2 values(1648791222001, 2, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST3;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 2) == 1
                and tdSql.getData(1, 2) == 2,
            )

    class Retrive0(StreamCheckItem):
        def __init__(self):
            self.db = "Retrive0"

        def create(self):
            tdSql.execute(f"create database retrive0 vgroups 3 buffer 8;")
            tdSql.execute(f"use retrive0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")
            tdSql.execute(
                f"create stream stream_t1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamtST1 as select _twstart, count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")
            tdSql.execute(
                f"insert into ts1 values(1648791213002, NULL, NULL, NULL, NULL);"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791213002, NULL, NULL, NULL, NULL);"
            )

            tdSql.execute(f"insert into ts1 values(1648791223002, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts1 values(1648791233003, 3, 2, 3, 2.1);")
            tdSql.execute(f"insert into ts2 values(1648791243004, 4, 2, 43, 73.1);")

            tdSql.execute(
                f"insert into ts1 values(1648791213002, 24, 22, 23, 4.1) (1648791243005, 4, 20, 3, 3.1);"
            )
            tdSql.execute(
                f"insert into ts3 values(1648791213001, 12, 12, 13, 14.1) (1648791243005, 14, 30, 30, 30.1);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 5
                and tdSql.getData(0, 2) == 38
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(1, 2) == 2
                and tdSql.getData(2, 1) == 1
                and tdSql.getData(2, 2) == 3
                and tdSql.getData(3, 1) == 3
                and tdSql.getData(3, 2) == 22,
            )

        def insert2(self):
            tdSql.execute(
                f"insert into ts1 values(1648791223008, 4, 2, 30, 3.1) (1648791213009, 4, 2, 3, 3.1)  (1648791233010, 4, 2, 3, 3.1) (1648791243011, 4, 2, 3, 3.1)(1648791243012, 34, 32, 33, 3.1);"
            )

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 6
                and tdSql.getData(0, 2) == 42
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(2, 2) == 7
                and tdSql.getData(3, 1) == 5
                and tdSql.getData(3, 2) == 60,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into ts4 values(1648791223008, 4, 2, 30, 3.1) (1648791213009, 4, 2, 3, 3.1)  (1648791233010, 4, 2, 3, 3.1);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 3
                and tdSql.getData(0, 1) == 7
                and tdSql.getData(0, 2) == 46
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 10
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(2, 2) == 11
                and tdSql.getData(3, 1) == 5
                and tdSql.getData(3, 2) == 60,
            )

        def insert4(self):
            tdSql.execute(f"insert into ts1 values(1648791200001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts2 values(1648791200001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts3 values(1648791200001, 1, 12, 3, 1.0);")
            tdSql.execute(f"insert into ts4 values(1648791200001, 1, 12, 3, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST1;",
                lambda: tdSql.getRows() > 4,
            )

    class Retrive1(StreamCheckItem):
        def __init__(self):
            self.db = "Retrive1"

        def create(self):
            tdSql.execute(f"create database retrive1 vgroups 4 keep 7000 buffer 8;")
            tdSql.execute(f"use retrive1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamt1 as select _twstart as c0, count(*) c1, count(a) c2 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791211000, 1, 2, 3);")

            tdSql.execute(f"insert into t1 values(1646275200000, 2, 2, 3);")
            tdSql.execute(f"insert into t2 values(1646275200000, 1, 2, 3);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select _wstart, count(*) from streamt1 interval(10d);",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(1, 1, 86400)
                and tdSql.compareData(2, 1, 86400),
                retry=180
            )

            tdLog.info(f"loop4 over")

    class Multi0(StreamCheckItem):
        def __init__(self):
            self.db = "Multi0"

        def create(self):
            tdSql.execute(f"create database multi0 vgroups 4 buffer 8;")
            tdSql.execute(f"use multi0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
            tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")
            tdSql.execute(
                f"create stream streams1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into streamt1 as select _twstart, count(*) c1, sum(a) c3, max(b) c4 from st where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into ts1 values(1648791213000, 1, 1, 3, 4.1);")
            tdSql.execute(f"insert into ts1 values(1648791223000, 2, 2, 3, 1.1);")
            tdSql.execute(f"insert into ts1 values(1648791233000, 3, 3, 3, 2.1);")
            tdSql.execute(f"insert into ts1 values(1648791243000, 4, 4, 3, 3.1);")

            tdSql.execute(f"insert into ts2 values(1648791213000, 1, 5, 3, 4.1);")
            tdSql.execute(f"insert into ts2 values(1648791223000, 2, 6, 3, 1.1);")
            tdSql.execute(f"insert into ts2 values(1648791233000, 3, 7, 3, 2.1);")
            tdSql.execute(f"insert into ts2 values(1648791243000, 4, 8, 3, 3.1);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into ts1 values(1648791213000, 1, 9, 3, 4.1);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(f"delete from ts2 where ts = 1648791243000 ;")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 2
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 1,
            )

        def insert4(self):
            tdSql.execute(f"delete from ts2 where ts = 1648791223000 ;")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 2
                and tdSql.getData(3, 1) == 1,
            )

        def insert5(self):
            tdSql.execute(f"insert into ts1 values(1648791233001, 3, 9, 3, 2.1);")

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt1;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 1
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(3, 1) == 1,
            )

    class Session0(StreamCheckItem):
        def __init__(self):
            self.db = "Session0"

        def create(self):

            tdSql.execute(f"create database session0 vgroups 1 buffer 8;")
            tdSql.execute(f"use session0;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream stream_t1 session(ts, 10s) from st stream_options(max_delay(3s)) into streamtST as select _twstart, count(*) c1, sum(a) c2, max(b) c3 from st where ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into ts1 values(1648791211000, 1, 1, 1) (1648791211005, 1, 1, 1);"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791221004, 1, 2, 3) (1648791221008, 2, 2, 3);"
            )
            tdSql.execute(f"insert into ts1 values(1648791211005, 1, 1, 1);")
            tdSql.execute(
                f"insert into ts2 values(1648791221006, 5, 5, 5) (1648791221007, 5, 5, 5);"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791221008, 5, 5, 5) (1648791221008, 5, 5, 5)(1648791221006, 5, 5, 5);"
            )
            tdSql.execute(
                f"insert into ts1 values(1648791231000, 1, 1, 1) (1648791231002, 1, 1, 1) (1648791231006, 1, 1, 1);"
            )
            tdSql.execute(
                f"insert into ts1 values(1648791211000, 6, 6, 6) (1648791231002, 2, 2, 2);"
            )
            tdSql.execute(f"insert into ts1 values(1648791211002, 7, 7, 7);")
            tdSql.execute(
                f"insert into ts1 values(1648791211002, 7, 7, 7) ts2 values(1648791221008, 5, 5, 5) ;"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST;",
                lambda: tdSql.getRows() > 0
                and tdSql.getData(0, 1) == 10
                and tdSql.getData(0, 2) == 34
                and tdSql.getData(0, 3) == 7,
            )

    class Session1(StreamCheckItem):
        def __init__(self):
            self.db = "Session1"

        def create(self):
            tdSql.execute(f"create database session1 vgroups 4 buffer 8;")
            tdSql.execute(f"use session1;")
            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream stream_t2 session(ts, 10s) from st partition by a stream_options(max_delay(3s)) into streamtST2 as select _twstart, count(*) c1, sum(a) c2, max(b) c3 from st where a=%%1 and ts >= _twstart and ts <= _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into ts1 values(1648791201000, 1, 1, 1) (1648791210000, 1, 1, 1);"
            )
            tdSql.execute(
                f"insert into ts1 values(1648791211000, 2, 1, 1) (1648791212000, 2, 1, 1);"
            )
            tdSql.execute(
                f"insert into ts2 values(1648791211000, 3, 1, 1) (1648791212000, 3, 1, 1);"
            )

        def insert2(self):
            tdSql.execute(f"delete from st where ts = 1648791211000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamtST2;",
                lambda: tdSql.getRows() == 3,
            )
