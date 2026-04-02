import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseSnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_snode(self):
        """OldTsim: snode

        Test basic operations of snode

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/schedSnode.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/snodeCheck.sim

        """

        tdStream.createSnode()

        self.schedSnode()

    def schedSnode(self):
        tdLog.info(f"schedSnode")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test vgroups 2;")
        tdSql.execute(f"create database target vgroups 1;")
        tdSql.execute(f"create snode on dnode 1")

        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table ts2 using st tags(2, 2, 2);")
        tdSql.execute(f"create table ts3 using st tags(3, 2, 2);")
        tdSql.execute(f"create table ts4 using st tags(4, 2, 2);")
        tdSql.execute(
            f"create stream stream_t1 interval(10s) sliding(10s) from st stream_options(max_delay(3s)) into target.streamtST1 as select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5 from %%trows"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts2 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.execute(f"insert into ts3 values(1648791213001, 1, 12, 3, 1.0);")
        tdSql.execute(f"insert into ts4 values(1648791213001, 1, 12, 3, 1.0);")

        tdSql.execute(f"insert into ts1 values(1648791213002, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into ts2 values(1648791213002, NULL, NULL, NULL, NULL);")

        tdSql.execute(f"insert into ts3 values(1648791213002, NULL, NULL, NULL, NULL);")
        tdSql.execute(f"insert into ts4 values(1648791213002, NULL, NULL, NULL, NULL);")

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
        tdSql.execute(
            f"insert into ts1 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
        )
        tdSql.execute(
            f"insert into ts2 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
        )
        tdSql.execute(
            f"insert into ts1 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
        )

        tdSql.execute(f"insert into ts3 values(1648791223002, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into ts4 values(1648791233003, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into ts3 values(1648791243004, 4, 2, 43, 73.1);")
        tdSql.execute(f"insert into ts4 values(1648791213002, 24, 22, 23, 4.1);")
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
        tdSql.execute(
            f"insert into ts3 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) ;"
        )
        tdSql.execute(
            f"insert into ts4 values(1648791243005, 4, 42, 3, 3.1) (1648791243003, 4, 2, 33, 3.1) (1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0) (1648791223001, 22, 22, 83, 1.1) (1648791233004, 13, 12, 13, 2.1) ;"
        )
        tdSql.execute(
            f"insert into ts3 values(1648791243006, 4, 2, 3, 3.1) (1648791213001, 1, 52, 13, 1.0)  (1648791223001, 22, 22, 83, 1.1) ;"
        )

        tdSql.checkResultsByFunc(
            f"select * from target.streamtST1;",
            lambda: tdSql.getRows() >= 4
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
            and tdSql.getData(3, 4) == 12
            and tdSql.getData(3, 5) == 3
            and tdSql.getData(3, 1) == 30
            and tdSql.getData(3, 2) == 30
            and tdSql.getData(3, 3) == 180
            and tdSql.getData(3, 4) == 42
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.query(
            f"select _wstart, count(*) c1, count(d) c2, sum(a) c3, max(b) c4, min(c) c5, avg(d) from st interval(10s);"
        )
