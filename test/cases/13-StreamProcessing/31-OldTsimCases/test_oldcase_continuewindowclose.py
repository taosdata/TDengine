import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseContinueWindowClose:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_continue_window_close(self):
        """Stream continue window close

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/nonblockIntervalBasic.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/nonblockIntervalHistory.sim

        """

        self.nonblockIntervalBasic()
        self.nonblockIntervalHistory()

    def nonblockIntervalBasic(self):
        tdLog.info(f"nonblockIntervalBasic")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"========== interval window")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.error(
            f"create stream streams_er1 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt_et1 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname session(ts, 10s);"
        )
        tdSql.error(
            f"create stream streams_er2 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt_et2 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname state_window(a) ;"
        )
        tdSql.error(
            f"create stream streams_er3 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt_et3 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname count_window(10);"
        )
        tdSql.error(
            f"create stream streams_er4 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt_et4 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname event_window start with a = 0 end with b = 9;"
        )

        tdSql.execute(
            f"create stream streams1 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt1 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,1,2,3);")
        tdSql.query(
            f"select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname interval(10s) ;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2,
        )

        tdLog.info(f"============================end")

        tdLog.info(f"========== interval window step2")

        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2  vgroups 1;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt2 as select  _wstart, count(*) c1, max(a) c2  from st partition by tbname interval(10s) sliding(5s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791214000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791215000,3,2,3);")
        tdSql.execute(f"insert into t1 values(1648791219000,4,2,3);")
        tdSql.execute(f"insert into t1 values(1648791220000,5,2,3);")

        tdSql.execute(f"insert into t1 values(1648791420000,6,2,3);")
        tdSql.checkResultsByFunc(
            f"select * from streamt2 order by 1,2;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(3, 1) == 1,
        )

        tdLog.info(f"========== interval window step3")

        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3  vgroups 2;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams3 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt3 as select  _wstart, count(*) c1, sum(b) c2  from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,1,2,3);")
        tdSql.query(f"select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) ;")

        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 2,
        )

        tdSql.execute(f"insert into t2 values(1648791211000,1,2,3);")
        tdSql.query(f"select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) ;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4,
        )

        tdLog.info(f"========== interval window step4")

        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"create database test4  vgroups 2;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams4 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt4 as select  _wstart, count(*) c1, max(a) c2  from st interval(10s) sliding(5s) ;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791214000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791215000,3,2,3);")
        tdSql.execute(f"insert into t1 values(1648791219000,4,2,3);")
        tdSql.execute(f"insert into t1 values(1648791220000,5,2,3);")
        tdSql.execute(f"insert into t1 values(1648791420000,6,2,3);")
        tdSql.query(
            f"select  _wstart, count(*) c1, max(a) c2  from st partition by tbname interval(10s) sliding(5s) ;"
        )

        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by 1,2;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 3
            and tdSql.getData(3, 1) == 1,
        )

        tdLog.info(f"========== interval window step5")

        tdSql.execute(f"drop database if exists test5;")
        tdSql.execute(f"create database test5  vgroups 2;")
        tdSql.execute(f"use test5;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams5 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt5 as select  _wstart, count(*) c1, max(a) c2, b  from st partition by b interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,3);")
        tdSql.execute(f"insert into t1 values(1648791214000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791215000,3,1,3);")
        tdSql.execute(f"insert into t1 values(1648791219000,4,2,3);")

        tdSql.execute(f"insert into t2 values(1648791211000,1,1,3);")
        tdSql.execute(f"insert into t2 values(1648791214000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791215000,3,1,3);")
        tdSql.execute(f"insert into t2 values(1648791219000,4,2,3);")
        tdSql.execute(f"insert into t2 values(1648791220000,5,1,3);")
        tdSql.execute(f"insert into t2 values(1648791220001,6,2,3);")

        tdSql.execute(f"insert into t1 values(1648791420000,6,2,3);")

        tdLog.info(
            f"loop5 select  _wstart, count(*) c1, max(a) c2, b  from st partition by b interval(10s) order by 1,4;"
        )
        tdSql.query(
            f"select  _wstart, count(*) c1, max(a) c2, b  from st partition by b interval(10s) order by 1,4;"
        )

        tdLog.info(f"sql select * from streamt5 order by 1,4;")
        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by 1,4;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(3, 1) == 1,
        )

        tdLog.info(f"========== interval window step6")

        tdSql.execute(f"drop database if exists test6;")
        tdSql.execute(f"create database test6  vgroups 2;")
        tdSql.execute(f"use test6;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams6 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt6  TAGS(dd varchar(100)) SUBTABLE(concat("streams6-tbn-", cast(dd as varchar(10)) )) as select  _wstart, count(*) c1, max(b) c2  from st partition by tbname, ta as dd interval(10s);'
        )
        tdSql.execute(
            f'create stream streams7 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt7  TAGS(dd varchar(100)) SUBTABLE(concat("streams7-tbn-", cast(dd as varchar(10)) )) as select  _wstart, count(*) c1, max(b) c2  from st partition by a as dd interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,3);")
        tdSql.execute(f"insert into t2 values(1648791211000,2,2,3);")

        tdSql.execute(f"insert into t1 values(1648791221000,1,3,3);")
        tdSql.execute(f"insert into t2 values(1648791221000,2,4,3);")

        tdSql.query(f"show tables;")

        tdLog.info(f"sql show tables;")
        tdSql.checkResultsByFunc(f"show tables;", lambda: tdSql.getRows() == 6)

        tdLog.info(
            f'sql select * from information_schema.ins_tables where table_name like "streams6-tbn-%";'
        )
        tdSql.checkResultsByFunc(
            f'select * from information_schema.ins_tables where table_name like "streams6-tbn-%";',
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(
            f'sql select * from information_schema.ins_tables where table_name like "streams7-tbn-%";'
        )
        tdSql.checkResultsByFunc(
            f'select * from information_schema.ins_tables where table_name like "streams7-tbn-%";',
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"sql select * from streamt6;")
        tdSql.checkResultsByFunc(f"select * from streamt6;", lambda: tdSql.getRows() == 2)

        tdLog.info(f"sql select * from streamt7;")
        tdSql.checkResultsByFunc(f"select * from streamt7;", lambda: tdSql.getRows() == 2)

        tdLog.info(f"========== interval window step6")

        tdSql.execute(f"drop database if exists test8;")
        tdSql.execute(f"create database test8  vgroups 2;")
        tdSql.execute(f"use test8;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create table streamt8(ts timestamp, a int primary key, b bigint ) tags(ta varchar(100));"
        )

        tdSql.execute(
            f"create stream streams8 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt8 tags(ta) as select  _wstart, count(*) c1, max(b) c2  from st partition by tbname, a as ta interval(10s);"
        )
        tdSql.execute(
            f"create stream streams9 trigger continuous_window_close ignore update 0 ignore expired 0 into streamt9(c1, c2 primary key, c3) as select  _wstart, count(*) c1, max(b) c2  from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,3);")
        tdSql.execute(f"insert into t2 values(1648791211000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,1,3,3);")

        tdLog.info(f"sql select * from streamt9;")
        tdSql.checkResultsByFunc(
            f"select * from streamt9;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdSql.execute(f"insert into t2 values(1648791211001,2,4,3);")

        tdLog.info(f"sql select * from streamt8;")
        tdSql.checkResultsByFunc(f"select * from streamt8;", lambda: tdSql.getRows() == 1)

        tdSql.checkResultsByFunc(
            f"select * from streamt9;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

    def nonblockIntervalHistory(self):
        tdLog.info(f"nonblockIntervalHistory")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"========== interval window")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791111000,1,1,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791111000,1,3,3);")
        tdSql.execute(f"insert into t2 values(1648791221000,2,4,3);")

        tdSql.execute(
            f"create stream streams1 trigger continuous_window_close fill_history 1 ignore update 0 ignore expired 0 into streamt1 as select  _wstart, count(*) c1, sum(b) c2  from st partition by tbname interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"sql loop00 select * from streamt1 order by 1,2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1,2;", lambda: tdSql.getRows() == 4
        )

        tdSql.execute(f"insert into t1 values(1648791221001,3,5,3);")

        tdSql.execute(f"insert into t1 values(1648791241001,3,6,3);")

        tdLog.info(
            f"sql sql select  _wstart, count(*) c1, sum(b) c2,tbname  from st partition by tbname interval(10s) order by 1,2 ;"
        )
        tdSql.query(
            f"select  _wstart, count(*) c1, sum(b) c2,tbname  from st partition by tbname interval(10s) order by 1,2 ;"
        )

        tdLog.info(f"sql loop0 select * from streamt1 order by 1,2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1,2;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(3, 1) == 2
            and tdSql.getData(3, 2) == 7,
        )

        tdLog.info(f"========== step2")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1  vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")

        tdSql.execute(f"insert into t1 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t1 values(1648791224000,2,2,3);")

        tdSql.execute(f"insert into t2 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791224000,2,2,3);")

        tdSql.execute(f"insert into t3 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t3 values(1648791224000,2,2,3);")

        tdSql.execute(
            f"create stream streams12 trigger continuous_window_close fill_history 1 ignore update 0 ignore expired 0 into streamt12 as select  _wstart, avg(a) c1, sum(b) c2, tbname as c3  from st partition by tbname interval(1s) ;"
        )

        tdStream.checkStreamStatus()
        tdSql.checkResultsByFunc(
            f"select * from streamt12 order by 1,2;",
            lambda: tdSql.getRows() == 6,
        )

        tdSql.execute(f"insert into  t1 values(1648791224001,2,2,3);")
        tdSql.execute(f"insert into  t1 values(1648791225001,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select * from streamt12 where c3 == "t1" order by 1,2;',
            lambda: tdSql.getRows() == 2 and tdSql.getData(1, 2) == 4,
        )

        tdLog.info(f"============================end")
        tdLog.info(f"========== step3")

        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"create database test3  vgroups 2;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791111000,1,1,3);")
        tdSql.execute(f"insert into t1 values(1648791221000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791111000,1,3,3);")
        tdSql.execute(f"insert into t2 values(1648791221000,2,4,3);")

        tdSql.execute(
            f"create stream streams3 trigger continuous_window_close fill_history 1 ignore update 0 ignore expired 0 into streamt3 as select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) ;"
        )

        tdStream.checkStreamStatus()

        tdLog.info(
            f"sql sql select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) order by 1,2 ;"
        )
        tdSql.query(
            f"select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) order by 1,2 ;"
        )

        tdLog.info(f"sql loop5 select * from streamt3 order by 1,2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1,2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791221001,3,5,3);")
        tdSql.execute(f"insert into t1 values(1648791241001,3,6,3);")

        tdLog.info(
            f"sql sql select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) order by 1,2 ;"
        )
        tdSql.query(
            f"select  _wstart, count(*) c1, sum(b) c2  from st interval(10s) order by 1,2 ;"
        )

        tdLog.info(f"sql loop6 select * from streamt3 order by 1,2;")
        tdSql.checkResultsByFunc(
            f"select * from streamt3 order by 1,2;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 4
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 11,
        )
