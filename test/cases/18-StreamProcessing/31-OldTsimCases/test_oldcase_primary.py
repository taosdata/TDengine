import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseInterpPrimary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_primary(self):
        """OldTsim: composite key

        Validate the calculation results with composite keys

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamPrimaryKey0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamPrimaryKey1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamPrimaryKey2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamPrimaryKey3.sim
        """

        tdStream.createSnode()

        self.streamPrimaryKey0()
        # self.streamPrimaryKey1()
        # self.streamPrimaryKey2()
        # self.streamPrimaryKey3()

    def streamPrimaryKey0(self):
        tdLog.info(f"streamPrimaryKey0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1=============")

        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt0(ts timestamp, a int composite key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create table streamt2(ts timestamp, a int composite key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create stream streams0 interval(1s) sliding(1s) from t1 stream_options(max_delay(3s)) into streamt0 as select _twstart, count(*) c1, max(b) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams2 interval(1s) sliding(1s) from st partition by tbname stream_options(max_delay(3s)) into streamt2 tags(ta) as select _twstart, count(*) c1, max(b) from %%trows"
        )

        tdSql.pause()
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 4, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt0")
        tdSql.checkResultsByFunc(
            f"select * from streamt0;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"1 select * from streamt2")
        tdSql.checkResultsByFunc(
            f"select * from streamt2;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"step2=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt3(ts timestamp, a int composite key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create table streamt5(ts timestamp, a int composite key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 into streamt3 as select _wstart, count(*) c1, max(b) from t1 session(ts, 1s);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once ignore expired 0 ignore update 0 into streamt5 tags(ta) as select _wstart, count(*) c1, max(b) from st partition by tbname ta session(ts, 1s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 4, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt3")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"1 select * from streamt5")
        tdSql.checkResultsByFunc(
            f"select * from streamt5;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt6(ts timestamp, a int composite key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create stream streams6 trigger at_once ignore expired 0 ignore update 0 into streamt6 tags(ta) as select _wstart, count(*) c1, max(b) from st partition by tbname ta state_window(a);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 1, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 5, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt6")
        tdSql.checkResultsByFunc(
            f"select * from streamt6;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"step3=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test3 vgroups 4;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create table st(ts timestamp, a int composite key, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')

        tdSql.execute(
            f"create stream streams3_1 trigger at_once ignore expired 0 ignore update 0 into streamt3_1 as select _wstart, a, max(b), count(*), ta from st partition by ta, a interval(10s);"
        )
        tdSql.execute(
            f"create stream streams3_2 trigger at_once ignore expired 0 ignore update 0 into streamt3_2 as select _wstart, a, max(b), count(*), ta from st partition by ta, a session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210001, 1, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791210002, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791220001, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt3_1;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_1;", lambda: tdSql.getRows() == 4
        )

        tdLog.info(f"1 select * from streamt3_2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3_2;", lambda: tdSql.getRows() == 4
        )

    def streamPrimaryKey1(self):
        tdLog.info(f"streamPrimaryKey1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1=============")

        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt1(ts timestamp, a int primary key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )

        tdSql.execute(
            f"create stream streams1 trigger at_once ignore expired 0 ignore update 0 into streamt1 as select _wstart, count(*) c1, max(b) from st interval(1s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 4, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt1")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"step2=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt3(ts timestamp, a int primary key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 into streamt3 as select _wstart, count(*) c1, max(b) from st session(ts, 1s);"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 4, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt3")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

    def streamPrimaryKey2(self):
        tdLog.info(f"streamPrimaryKey2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1=============")

        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt1(ts timestamp, a int primary key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )

        tdSql.execute(
            f"create stream streams1 trigger at_once ignore expired 0 ignore update 0 into streamt1 tags(ta) as select _wstart, count(*) c1, max(b) from st partition by tbname ta EVENT_WINDOW start with a = 1 end with a = 3;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210010, 3, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 2, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791220000, 1, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791220001, 3, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 1, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791230001, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 1, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791240001, 3, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 1, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250001, 3, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt1")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 3,
        )

        tdLog.info(f"step2=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table st(ts timestamp, a int, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')
        tdSql.execute(
            f"create table streamt3(ts timestamp, a int primary key, b bigint ) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once ignore expired 1 ignore update 0 WATERMARK 1000s into streamt3 tags(ta) as select _wstart, count(*) c1, max(b) from st partition by tbname ta COUNT_WINDOW(2);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210001, 4, 2, 3, 4.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791220001, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791230000, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791230001, 3, 2, 3, 2.1);")
        tdSql.execute(f"insert into t1 values(1648791240000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791240001, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250000, 4, 2, 3, 3.1);")
        tdSql.execute(f"insert into t1 values(1648791250001, 4, 2, 3, 3.1);")

        tdLog.info(f"1 select * from streamt3")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 5 and tdSql.getData(0, 1) == 2,
        )

    def streamPrimaryKey3(self):
        tdLog.info(f"streamPrimaryKey3")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1=============")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table st(ts timestamp, a int primary key, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')

        tdSql.execute(
            f"create stream streams1 trigger at_once ignore expired 0 ignore update 0 into streamt1(ts, a primary key, b)  as select ts, a, b from t1 partition by b;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210000, 2, 4, 3, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210000, 1, 3, 3, 1.0);")

        tdLog.info(f"1 select * from streamt1 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1, 2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 4,
        )

        tdLog.info(f"step2=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test1 vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table st(ts timestamp, a int primary key, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')

        tdSql.execute(
            f"create stream streams2 trigger at_once ignore expired 0 ignore update 0 into streamt2(ts, a primary key, b)  as select _wstart, max(b), count(*) from t1 partition by b interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210000, 2, 4, 2, 1.0);")
        time.sleep(0.5)
        tdSql.execute(f"insert into t1 values(1648791210000, 1, 3, 3, 1.0);")

        tdLog.info(f"1 select * from streamt2 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791210000, 3, 5, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 1, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 2, 4, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 3, 5, 3, 1.0);")

        tdLog.info(f"1 select * from streamt2 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 2) == 2,
        )

        tdLog.info(f"delete from t1 where ts = 1648791210000;")
        tdSql.execute(f"delete from t1 where ts = 1648791210000;")

        tdLog.info(f"1 select * from streamt2 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1, 2;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 2) == 1,
        )

        tdLog.info(f"step3=============")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create table st(ts timestamp, a int primary key, b int, c int, d double) tags(ta varchar(100), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("aa", 1, 2);')

        tdSql.execute(
            f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 into streamt3(ts, a primary key, b)  as select _wstart, max(b), count(*) from t1 partition by b session(ts, 10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 1, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210000, 2, 4, 2, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210000, 1, 3, 3, 1.0);")

        tdLog.info(f"1 select * from streamt3 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 3
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791210000, 3, 5, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 1, 3, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 2, 4, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791210001, 3, 5, 3, 1.0);")

        tdLog.info(f"1 select * from streamt3 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(2, 2) == 2,
        )

        tdLog.info(f"delete from t1 where ts = 1648791210000;")
        tdSql.execute(f"delete from t1 where ts = 1648791210000;")

        tdLog.info(f"1 select * from streamt3 order by 1, 2")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1, 2;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(2, 2) == 1,
        )
