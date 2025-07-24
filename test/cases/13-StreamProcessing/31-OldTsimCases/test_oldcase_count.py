import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_count(self):
        """Stream count

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/count0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/count1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/count2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/count3.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/countSliding0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/countSliding1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/countSliding2.sim

        """

        tdStream.createSnode()

        self.count0()
        # self.count1()
        # self.count2()
        # self.count3()
        # self.countSliding0()
        # self.countSliding1()
        # self.countSliding2()

    def count0(self):
        tdLog.info(f"count0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 count_window(3) from t1 stream_options(max_delay(3s)|expired_time(0)|watermark(100s)) into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _wstart and ts < _twend;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdSql.pause()

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(1, 3) == 3,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(3)"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(1, 3) == 3
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(2, 2) == 6
            and tdSql.getData(2, 3) == 3,
        )

        tdLog.info(f"step3")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(
            f"create stream streams3 trigger at_once FILL_HISTORY 1 IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt3 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 6
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(1, 3) == 3,
        )

    def count1(self):
        tdLog.info(f"count1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        # stable
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 10s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from st count_window(3);"
        )

        # IGNORE EXPIRED 0
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 WATERMARK 10s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
        )

        # WATERMARK 0
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
        )

        # All
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from st count_window(3);"
        )

        # 2~INT32_MAX
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(1);"
        )
        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(2147483648);"
        )

        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 10s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(2);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 10s into streamt3 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(2147483647);"
        )

    def count2(self):
        tdLog.info(f"count2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"2 sql select * from streamt order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791212000, 0, 1, 1, 1.0);")
        tdLog.info(f"3 sql select * from streamt order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(2, 1) == 1,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(3)"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009, 0, 3, 3, 1.0);")

        tdLog.info(f"0 sql select * from streamt2 order by 1;;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1;;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791213000, 0, 1, 1, 1.0);")
        tdLog.info(f"1 sql select * from streamt2 order by 1;;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1;;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"2 sql select * from streamt2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1;",
            lambda: tdSql.getRows() == 4,
        )

        tdSql.execute(f"insert into t1 values(1648791212000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791212000, 0, 1, 1, 1.0);")

        tdLog.info(f"3 sql select * from streamt2 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(3, 1) == 3
            and tdSql.getData(4, 1) == 1
            and tdSql.getData(5, 1) == 1,
        )

    def count3(self):
        tdLog.info(f"count3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"2 sql select * from streamt order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 4, 4, 4, 4.0);")

        tdLog.info(f"3 sql select * from streamt order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 3,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791223001;")

        tdLog.info(f"3 sql select * from streamt order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(1, 1) == 2,
        )

    def countSliding0(self):
        tdLog.info(f"countSliding0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

        tdSql.execute(
            f"insert into t1 values(1648791233000, 0, 1, 1, 1.0) (1648791233001, 9, 2, 2, 1.1) (1648791233002, 9, 2, 2, 1.1) (1648791233009, 0, 3, 3, 1.0);"
        )

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 6
        )

        tdSql.execute(
            f"insert into t1 values(1648791243000, 0, 1, 1, 1.0) (1648791243001, 9, 2, 2, 1.1);"
        )

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 7,
        )
        tdSql.checkRows(7)

        tdSql.execute(
            f"insert into t1 values(1648791253000, 0, 1, 1, 1.0) (1648791253001, 9, 2, 2, 1.1) (1648791253002, 9, 2, 2, 1.1);"
        )

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.execute(f"insert into t1 values(1648791263000, 0, 1, 1, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 9,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(4, 2);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

        tdSql.execute(
            f"insert into t1 values(1648791233000, 0, 1, 1, 1.0) (1648791233001, 9, 2, 2, 1.1) (1648791233002, 9, 2, 2, 1.1) (1648791233009, 0, 3, 3, 1.0);"
        )

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 6,
        )

        tdSql.execute(
            f"insert into t1 values(1648791243000, 0, 1, 1, 1.0) (1648791243001, 9, 2, 2, 1.1);"
        )
        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 7,
        )

        tdSql.execute(
            f"insert into t1 values(1648791253000, 0, 1, 1, 1.0) (1648791253001, 9, 2, 2, 1.1) (1648791253002, 9, 2, 2, 1.1);"
        )

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.execute(f"insert into t1 values(1648791263000, 0, 1, 1, 1.0);")

        tdLog.info(f"1 sql select * from streamt2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 9,
        )

    def countSliding1(self):
        tdLog.info(f"countSliding1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

    def countSliding2(self):
        tdLog.info(f"countSliding2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 2,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791213000;")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(3, 1) == 1,
        )

        tdSql.execute(f"delete from t1 where ts = 1648791223002;")
        tdLog.info(f"1 sql select * from streamt;")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 2,
        )
