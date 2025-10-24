import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpHistory:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_history(self):
        """OldTsim: interp history

        Validate the calculation results of the interp function when processing historical data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpHistory.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpHistory1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpOther.sim
            ## - 2025-7-25 Simon Guan Migrated from tsim/stream/streamInterpOther1.sim

        """

        tdStream.createSnode()

        self.streamInterpHistory()
        # self.streamInterpOther()

    def streamInterpHistory(self):
        tdLog.info(f"streamInterpHistory")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(f"insert into t1 values(1648791212000, 1, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791215001, 2, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791212000, 31, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791216001, 41, 1, 1, 1.0);")

        tdSql.execute(
            f"create stream streams1 interval(1s) sliding(1s) from st partition by tbname stream_options(fill_history_first|max_delay(2s)) into streamt as select _irowts, _isfilled as a1, interp(a) as a2 from %%tbname range(_twstart) fill(prev);"
        )
        tdSql.pause()

        tdSql.execute(f"insert into t1 values(1648791217000, 5, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791217000, 61, 1, 1, 1.0);")

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 <= 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 < 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 2) == 2
            and tdSql.getData(5, 2) == 5,
        )

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 > 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 > 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 31
            and tdSql.getData(1, 2) == 31
            and tdSql.getData(2, 2) == 31
            and tdSql.getData(3, 2) == 31
            and tdSql.getData(4, 2) == 31
            and tdSql.getData(5, 2) == 61,
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791219001, 7, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791219001, 81, 1, 1, 1.0);")

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 <= 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 < 10 order by 1;",
            lambda: tdSql.getRows() == 8,
        )

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 > 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 > 10 order by 1;",
            lambda: tdSql.getRows() == 8,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(f"insert into t1 values(1648791212000, 1, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791215001, 2, 1, 1, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791212000, 31, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791216001, 41, 1, 1, 1.0);")

        tdSql.execute(
            f"create stream streams2 trigger at_once FILL_HISTORY 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(next);"
        )

        tdSql.execute(f"insert into t1 values(1648791217000, 5, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791217000, 61, 1, 1, 1.0);")

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(next) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(next) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 <= 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 < 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(3, 2) == 2
            and tdSql.getData(4, 2) == 5
            and tdSql.getData(5, 2) == 5,
        )

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(next) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(next) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 > 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 > 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 31
            and tdSql.getData(1, 2) == 41
            and tdSql.getData(2, 2) == 41
            and tdSql.getData(3, 2) == 41
            and tdSql.getData(4, 2) == 41
            and tdSql.getData(5, 2) == 61,
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791219001, 7, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791219001, 81, 1, 1, 1.0);")

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(next) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(next) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 <= 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 < 10 order by 1;",
            lambda: tdSql.getRows() == 8,
        )

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(next) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791219000) every(1s) fill(next) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 > 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 > 10 order by 1;",
            lambda: tdSql.getRows() == 8,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(f"insert into t1 values(1648791212000, 1, 1, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791215001, 2, 1, 1, 1.0);")

        tdSql.execute(f"insert into t2 values(1648791212000, 31, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791216001, 41, 1, 1, 1.0);")

        tdSql.execute(
            f"create stream streams3 trigger at_once FILL_HISTORY 1 IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791217000, 5, 1, 1, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791217000, 61, 1, 1, 1.0);")

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t1 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 <= 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 < 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(4, 2) == 2
            and tdSql.getData(5, 2) == 5,
        )

        tdLog.info(
            f"sql select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )
        tdSql.query(
            f"select _irowts, _isfilled as a1, interp(a) as a2 from t2 partition by tbname range(1648791212000, 1648791217000) every(1s) fill(prev) order by 3, 1;"
        )

        tdLog.info(f"0 sql select * from streamt where a2 > 10 order by 1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt where a2 > 10 order by 1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 2) == 31
            and tdSql.getData(1, 2) == 31
            and tdSql.getData(2, 2) == 31
            and tdSql.getData(3, 2) == 31
            and tdSql.getData(4, 2) == 31
            and tdSql.getData(5, 2) == 61,
        )

    def streamInterpOther(self):
        tdLog.info(f"streamInterpOther")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams1_1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1_1 as select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 every(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams1_2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1_2 as select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 every(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams1_3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1_3 as select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 every(1s) fill(linear);"
        )
        tdSql.execute(
            f"create stream streams1_4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1_4 as select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 every(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams1_5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt1_5 as select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 every(1s) fill(value, 11, 22, 33, 44);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791215000, 0, 0, 0, 0.0);")
        tdSql.execute(f"insert into t1 values(1648791212000, 10, 10, 10, 10.0);")

        tdLog.info(f"sql desc streamt1_1;")
        tdSql.checkResultsByFunc(
            f"desc streamt1_1;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.checkResultsByFunc(
            f"desc streamt1_2;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.checkResultsByFunc(
            f"desc streamt1_3;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.checkResultsByFunc(
            f"desc streamt1_4;",
            lambda: tdSql.getRows() == 9,
        )

        tdSql.checkResultsByFunc(
            f"desc streamt1_5;",
            lambda: tdSql.getRows() == 9,
        )

        tdLog.info(f"sql select * from streamt1_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_1;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"sql select * from streamt1_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_2;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"sql select * from streamt1_3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_3;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"sql select * from streamt1_4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_4;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(f"sql select * from streamt1_5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_5;",
            lambda: tdSql.getRows() == 4,
        )

        tdLog.info(
            f"sql select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 range(1648791212000, 1648791215000) every(1s) fill(value, 11, 22, 33, 44);"
        )
        tdSql.query(
            f"select interp(a), _isfilled as a1, interp(b), _isfilled as a2, interp(c), _isfilled as a3, interp(d) from t1 range(1648791212000, 1648791215000) every(1s) fill(value, 11, 22, 33, 44);"
        )

        tdLog.info(f"sql select * from streamt1_5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1_5;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(0, 2) == 0
            and tdSql.getData(0, 3) == 10
            and tdSql.getData(0, 4) == 0
            and tdSql.getData(0, 5) == 10
            and tdSql.getData(0, 6) == 0
            and tdSql.getData(0, 7) == 10.000000000
            and tdSql.getData(1, 1) == 11
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 22
            and tdSql.getData(1, 4) == 1
            and tdSql.getData(1, 5) == 33
            and tdSql.getData(1, 6) == 1
            and tdSql.getData(1, 7) == 44.000000000,
        )

        tdLog.info(f"step3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f'create stream streams3_1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_1 TAGS(cc varchar(100)) SUBTABLE(concat(concat("tbn-", tbname), "_1")) as select interp(a), _isfilled as a1 from st partition by tbname, b as cc every(1s) fill(prev);'
        )
        tdSql.execute(
            f'create stream streams3_2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_2 TAGS(cc varchar(100)) SUBTABLE(concat(concat("tbn-", tbname), "_2")) as select interp(a), _isfilled as a1 from st partition by tbname, b as cc every(1s) fill(next);'
        )
        tdSql.execute(
            f'create stream streams3_3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_3 TAGS(cc varchar(100)) SUBTABLE(concat(concat("tbn-", tbname), "_3")) as select interp(a), _isfilled as a1 from st partition by tbname, b as cc every(1s) fill(linear);'
        )
        tdSql.execute(
            f'create stream streams3_4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_4 TAGS(cc varchar(100)) SUBTABLE(concat(concat("tbn-", tbname), "_4")) as select interp(a), _isfilled as a1 from st partition by tbname, b as cc every(1s) fill(NULL);'
        )
        tdSql.execute(
            f'create stream streams3_5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into streamt3_5 TAGS(cc varchar(100)) SUBTABLE(concat(concat("tbn-", tbname), "_5")) as select interp(a), _isfilled as a1 from st partition by tbname, b as cc every(1s) fill(value, 11);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791217000, 1, 2, 3);")
        tdSql.execute(f"insert into t1 values(1648791212000, 10, 2, 3);")
        tdSql.execute(f"insert into t1 values(1648791215001, 20, 2, 3);")
        tdSql.execute(f"insert into t2 values(1648791215001, 20, 2, 3);")

        tdLog.info(f"sql select cc, * from `tbn-t1_1_streamt3_1_914568691400502130`;")
        tdSql.checkResultsByFunc(
            f"select cc, * from `tbn-t1_1_streamt3_1_914568691400502130`;",
            lambda: tdSql.getRows() == 6 and tdSql.getData(0, 0) == "2",
        )

        tdLog.info(f"sql select cc, * from `tbn-t1_2_streamt3_2_914568691400502130`;")
        tdSql.checkResultsByFunc(
            f"select cc, * from `tbn-t1_2_streamt3_2_914568691400502130`;",
            lambda: tdSql.getRows() == 6 and tdSql.getData(0, 0) == "2",
        )

        tdLog.info(f"sql select cc, * from `tbn-t1_3_streamt3_3_914568691400502130`;")
        tdSql.checkResultsByFunc(
            f"select cc, * from `tbn-t1_3_streamt3_3_914568691400502130`;",
            lambda: tdSql.getRows() == 6 and tdSql.getData(0, 0) == "2",
        )

        tdLog.info(f"sql select cc, * from `tbn-t1_4_streamt3_4_914568691400502130`;")
        tdSql.checkResultsByFunc(
            f"select cc, * from `tbn-t1_4_streamt3_4_914568691400502130`;",
            lambda: tdSql.getRows() == 6 and tdSql.getData(0, 0) == "2",
        )

        tdLog.info(f"sql select cc, * from `tbn-t1_5_streamt3_5_914568691400502130`;")
        tdSql.checkResultsByFunc(
            f"select cc, * from `tbn-t1_5_streamt3_5_914568691400502130`;",
            lambda: tdSql.getRows() == 6 and tdSql.getData(0, 0) == "2",
        )

        tdLog.info(f"sql select * from `tbn-t2_1_streamt3_1_8905952758123525205`;")
        tdSql.checkResultsByFunc(
            f"select * from `tbn-t2_1_streamt3_1_8905952758123525205`;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from `tbn-t2_2_streamt3_2_8905952758123525205`;")
        tdSql.checkResultsByFunc(
            f"select * from `tbn-t2_2_streamt3_2_8905952758123525205`;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from `tbn-t2_3_streamt3_3_8905952758123525205`;")
        tdSql.checkResultsByFunc(
            f"select * from `tbn-t2_3_streamt3_3_8905952758123525205`;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from `tbn-t2_4_streamt3_4_8905952758123525205`;")
        tdSql.checkResultsByFunc(
            f"select * from `tbn-t2_4_streamt3_4_8905952758123525205`;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from `tbn-t2_5_streamt3_5_8905952758123525205`;")
        tdSql.checkResultsByFunc(
            f"select * from `tbn-t2_5_streamt3_5_8905952758123525205`;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"step4")
        tdStream.dropAllStreamsAndDbs()
        tdLog.info(f"=============== create database")
        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"create database test4 vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.execute(
            f"create stream streams4_1 trigger at_once watermark 10s IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt4_1 as select interp(a, 1), _isfilled as a1 from t1 every(1s) fill(prev);"
        )
        tdSql.execute(
            f"create stream streams4_2 trigger at_once watermark 10s IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt4_2 as select interp(a, 1), _isfilled as a1 from t1 every(1s) fill(next);"
        )
        tdSql.execute(
            f"create stream streams4_3 trigger at_once watermark 10s IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt4_3 as select interp(a, 1), _isfilled as a1 from t1 every(1s) fill(linear);"
        )
        tdSql.execute(
            f"create stream streams4_4 trigger at_once watermark 10s IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt4_4 as select interp(a, 1), _isfilled as a1 from t1 every(1s) fill(NULL);"
        )
        tdSql.execute(
            f"create stream streams4_5 trigger at_once watermark 10s IGNORE EXPIRED 1 IGNORE UPDATE 0 into streamt4_5 as select interp(a, 1), _isfilled as a1 from t1 every(1s) fill(value, 11);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791275000, NULL, 0, 0, 0.0);")
        tdSql.execute(
            f"insert into t1 values(1648791276000, NULL, 1, 0, 0.0) (1648791277000, NULL, 2, 0, 0.0) (1648791275000, NULL, 3, 0, 0.0);"
        )

        tdLog.info(f"sql select * from streamt4_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_1;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_2;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_3;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_4;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_5;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql insert into t1 values(1648791215000, 1, 0, 0, 0.0);")
        tdSql.execute(f"insert into t1 values(1648791215000, 1, 0, 0, 0.0);")

        tdSql.execute(
            f"insert into t1 values(1648791216000, 2, 1, 0, 0.0) (1648791217000, 3, 2, 0, 0.0) (1648791215000, 4, 3, 0, 0.0);"
        )

        tdLog.info(f"sql select * from streamt4_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_1;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_2;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_3;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_4;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"sql select * from streamt4_5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_5;",
            lambda: tdSql.getRows() == 0,
        )

        tdLog.info(f"step4_3")

        tdLog.info(
            f"sql insert into t1 values(1648791278000, NULL, 2, 0, 0.0) (1648791278001, NULL, 2, 0, 0.0) (1648791279000, 1, 2, 0, 0.0) (1648791279001, NULL, 2, 0, 0.0) (1648791280000, NULL, 2, 0, 0.0)(1648791280001, NULL, 2, 0, 0.0)(1648791281000, 20, 2, 0, 0.0) (1648791281001, NULL, 2, 0, 0.0)(1648791281002, NULL, 2, 0, 0.0) (1648791282000, NULL, 2, 0, 0.0);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791278000, NULL, 2, 0, 0.0) (1648791278001, NULL, 2, 0, 0.0) (1648791279000, 1, 2, 0, 0.0) (1648791279001, NULL, 2, 0, 0.0) (1648791280000, NULL, 2, 0, 0.0)(1648791280001, NULL, 2, 0, 0.0)(1648791281000, 20, 2, 0, 0.0) (1648791281001, NULL, 2, 0, 0.0)(1648791281002, NULL, 2, 0, 0.0) (1648791282000, NULL, 2, 0, 0.0);"
        )

        tdLog.info(f"sql select * from streamt4_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_1;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(1, 2) == 1,
        )

        tdLog.info(f"sql select * from streamt4_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_2;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(1, 2) == 1,
        )

        tdLog.info(f"sql select * from streamt4_3;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_3;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(1, 2) == 1,
        )

        tdLog.info(f"sql select * from streamt4_4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_4;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(1, 2) == 1,
        )

        tdLog.info(f"sql select * from streamt4_5;")
        tdSql.checkResultsByFunc(
            f"select * from streamt4_5;",
            lambda: tdSql.getRows() == 3 and tdSql.getData(1, 2) == 1,
        )
