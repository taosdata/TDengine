import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic1(self):
        """Stream basic test 1

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic3.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic4.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/basic5.sim

        """

        self.stream_basic_0()
        self.stream_basic_1()
        self.stream_basic_2()
        self.stream_basic_3()
        self.stream_basic_4()
        self.stream_basic_5()

    def stream_basic_0(self):
        tdLog.info(f"stream_basic_0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 vgroups 1")
        tdSql.execute(f"use d0")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, k int) tags (a int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== create stream")
        tdSql.execute(
            f"create stream s1 trigger at_once into outstb as select _wstart, min(k), max(k), sum(k) as sum_alias from ct1 interval(10m)"
        )
        tdStream.checkStreamStatus("s1")

        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:42:00.000', 234)")

        tdLog.info(f"=============== query data from child table")
        tdSql.checkResultsByFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 234
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 234,
        )

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:43:00.000', -111)")

        tdLog.info(f"=============== query data from child table")
        tdSql.checkResultsByFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == -111
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 123,
        )

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:53:00.000', 789)")

        tdLog.info(f"=============== query data from child table")
        tdSql.checkResultsByFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == -111
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 123
            and tdSql.getData(1, 1) == 789
            and tdSql.getData(1, 2) == 789
            and tdSql.getData(1, 3) == 789,
        )

    def stream_basic_1(self):
        tdLog.info(f"stream_basic_1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, count(d) c2 , sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s);"
        )
        tdStream.checkStreamStatus("streams1")

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,2,3,4.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 2
            and tdSql.getData(1, 4) == 2
            and tdSql.getData(1, 5) == 3
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223001,12,14,13,11.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 5
            and tdSql.getData(0, 4) == 2
            and tdSql.getData(0, 5) == 3
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 1
            and tdSql.getData(1, 3) == 12
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 1
            and tdSql.getData(2, 3) == 3
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3
            and tdSql.getData(3, 1) == 1
            and tdSql.getData(3, 2) == 1
            and tdSql.getData(3, 3) == 4
            and tdSql.getData(3, 4) == 2
            and tdSql.getData(3, 5) == 3,
        )

        tdSql.execute(f"insert into t1 values(1648791223002,12,14,13,11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(1, 3) == 24
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223003,12,14,13,11.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 36
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223001,1,1,1,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,2,2,2,2.1);")
        tdSql.execute(f"insert into t1 values(1648791223003,3,3,3,3.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(1, 1) == 3
            and tdSql.getData(1, 2) == 3
            and tdSql.getData(1, 3) == 6
            and tdSql.getData(1, 4) == 3
            and tdSql.getData(1, 5) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791233003,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,5,6,7,8.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(2, 1) == 2
            and tdSql.getData(2, 2) == 2
            and tdSql.getData(2, 3) == 6
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791213004,4,2,3,4.1) (1648791213006,5,4,7,9.1) (1648791213004,40,20,30,40.1) (1648791213005,4,2,3,4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 4
            and tdSql.getData(0, 2) == 4
            and tdSql.getData(0, 3) == 50
            and tdSql.getData(0, 4) == 20
            and tdSql.getData(0, 5) == 3,
        )

        tdSql.execute(
            f"insert into t1 values(1648791223004,4,2,3,4.1) (1648791233006,5,4,7,9.1) (1648791223004,40,20,30,40.1) (1648791233005,4,2,3,4.1);"
        )
        tdSql.checkResultsByFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(1, 2) == 4
            and tdSql.getData(1, 3) == 46
            and tdSql.getData(1, 4) == 20
            and tdSql.getData(1, 5) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(2, 3) == 15
            and tdSql.getData(2, 4) == 4
            and tdSql.getData(2, 5) == 3,
        )

        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")

        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(2,2,2);")
        tdSql.execute(f"create table t4 using st tags(2,2,2);")
        tdSql.execute(f"create table t5 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select  _wstart, count(*) c1, sum(a) c3,max(b) c4 from st partition by tbname interval(10s);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt3 as select  _wstart, count(*) c1, sum(a) c3,max(b) c4, now c5 from st partition by tbname interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1,1.0) t2 values(1648791213000,2,2,2,2.0) t3 values(1648791213000,3,3,3,3.0) t4 values(1648791213000,4,4,4,4.0);"
        )
        tdSql.checkResultsByFunc(f"select * from streamt;", lambda: tdSql.getRows() == 4)

        tdSql.execute(
            f"insert into t1 values(1648791213000,5,5,5,5.0) t2 values(1648791213000,6,6,6,6.0) t5 values(1648791213000,7,7,7,7.0);"
        )
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c4 desc;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 7
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 6
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == 5,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,8,8,8,8.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by c4 desc;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 8,
        )

        tdSql.checkResultsByFunc(
            f"select count(*) from streamt3;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 0) == 5,
        )

        tdSql.execute(f"create database test3  vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(
            f"create stream stream_t3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamtST3 as select ts, min(a) c6, a, b, c, ta, tb, tc from ts1 interval(10s) ;"
        )

        tdStream.checkStreamStatus()
        tdSql.execute(f"insert into ts1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into ts1 values(1648791222001,2,2,3);")
        tdSql.checkResultsByFunc(
            f"select * from streamtST3;",
            lambda: tdSql.getRows() > 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(1, 2) == 2,
        )

        tdSql.execute(f"create database test4  vgroups 1;")
        tdSql.execute(f"use test4;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt__4 as select  _wstart, count(*) c1 from t1 where a > 5 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.checkResultsByFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 0)

        tdSql.execute(f"insert into t1 values(1648791213000,6,2,3,1.0);")
        tdSql.checkResultsByFunc(
            f"select * from streamt__4;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,2,2,3,1.0);")
        tdSql.checkResultsByFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 0)

        tdSql.execute(f"insert into t1 values(1648791223000,2,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,10,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000,10,2,3,1.0);")
        tdSql.checkResultsByFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 2)

        tdSql.execute(f"insert into t1 values(1648791233000,2,2,3,1.0);")
        tdSql.checkResultsByFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 1)

        # for TS-2242
        tdSql.execute(f"create database test5  vgroups 1;")
        tdSql.execute(f"use test5;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt5 as select count(*), _wstart, _wend, max(a) from ts1 interval(10s) ;"
        )
        tdSql.execute(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt6 as select count(*), _wstart, _wend, max(a), _wstart as ts from ts1 interval(10s) ;"
        )
        tdStream.checkStreamStatus()

        tdSql.error(
            f"create stream streams7 trigger at_once into streamt7 as select _wstart, count(*), _wstart, _wend, max(a) from ts1 interval(10s) ;"
        )
        tdSql.error(
            f"create stream streams8 trigger at_once into streamt8 as select count(*), _wstart, _wstart, _wend, max(a) from ts1 interval(10s) ;"
        )
        tdSql.error(
            f"create stream streams9 trigger at_once into streamt9 as select _wstart as ts, count(*), _wstart as ts, _wend, max(a) from ts1 interval(10s) ;"
        )

        tdSql.execute(f"insert into ts1 values(1648791211000,1,2,3);")
        tdSql.checkResultsByFunc(f"select * from streamt5;", lambda: tdSql.getRows() == 1)

        tdSql.checkResultsByFunc(f"select * from streamt6;", lambda: tdSql.getRows() == 1)

        tdSql.execute(f"create database test7  vgroups 1;")
        tdSql.execute(f"use test7;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(
            f"create stream streams7 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt7 as select _wstart, count(*) from ts1 interval(10s) ;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into ts1 values(1648791211000,1,2,3);")
        tdSql.error(f"insert into ts1 values(-1648791211000,1,2,3);")
        tdSql.checkResultsByFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.error(
            f"insert into ts1 values(-1648791211001,1,2,3) (1648791211001,1,2,3);"
        )
        tdSql.query(f"select _wstart, count(*) from ts1 interval(10s) ;")

        tdSql.checkResultsByFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

    def stream_basic_2(self):
        tdLog.info(f"stream_basic_2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 vgroups 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use d0")

        tdLog.info(
            f"=============== create super table, include column type for count/sum/min/max/first"
        )
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, k int) tags (a int)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"===================== not support _wstart filter")
        tdSql.error(
            f"create stream s_error into str_dst_st as select _wend, count(*) a from stb where _wstart > '2025-1-1' interval(30s);"
        )

        tdSql.execute(
            f"create stream s1 trigger at_once into outstb as select _wstart, min(k), max(k), sum(k) as sum_alias from ct1 interval(10m)"
        )
        tdStream.checkStreamStatus()

        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:42:00.000', 234)")

        tdLog.info(f"=============== query data from child table")
        tdSql.checkResultsByFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 234
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 234,
        )

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:57:00.000', -111)")

        tdLog.info(f"=============== query data from child table")
        tdSql.checkResultsByFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 234
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 234
            and tdSql.getData(1, 1) == -111
            and tdSql.getData(1, 2) == -111
            and tdSql.getData(1, 3) == -111,
        )

        tdLog.info(f"step 2====================")
        tdSql.execute(f"create database test  vgroups 1 ;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.error(
            f"create stream streams1 trigger max_delay 4000a ignore update 0 ignore expired 0 into streamtST1 as select  _wstart, count(*) from st interval(5s);"
        )
        tdSql.error(
            f"create stream streams2 trigger max_delay 4s ignore update 0 ignore expired 0 into streamtST2 as select  _wstart, count(*) from st interval(5s);"
        )
        tdSql.execute(
            f"create stream streams3 trigger max_delay 5000a ignore update 0 ignore expired 0 into streamtST3 as select  _wstart, count(*) from st interval(5s);"
        )
        tdSql.execute(
            f"create stream streams4 trigger max_delay 5s ignore update 0 ignore expired 0 into streamtST4 as select  _wstart, count(*) from st interval(5s);"
        )

        tdSql.error(
            f"create stream streams5 trigger at_once ignore update 0 ignore expired 0 into streamtST5 as select  _wstart, count(*) from st interval(5s) having count(*) > 2;"
        )
        tdSql.error(
            f"create stream streams6 trigger at_once ignore update 0 ignore expired 0 into streamtST6 as select  _wstart, count(*) from st session(ts, 5s) having count(*) > 2;"
        )
        tdSql.error(
            f"create stream streams7 trigger at_once ignore update 0 ignore expired 1 into streamtST7 as select  _wstart, count(*) from st count_window(10) having count(*) > 2;"
        )
        tdSql.error(
            f"create stream streams8 trigger at_once ignore update 0 ignore expired 0 into streamtST8 as select  _wstart, count(*) from st state_window(a) having count(*) > 2;"
        )
        tdSql.error(
            f"create stream streams9 trigger at_once ignore update 0 ignore expired 0 into streamtST9 as select  _wstart, count(*) from st event_window start with a = 0 end with b = 9 having count(*) > 2;"
        )

    def stream_basic_3(self):
        tdLog.info(f"stream_basic_3")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"alter local 'keepColumnName' '1'")

        tdLog.info(f"========== interval\session\state window")
        tdSql.execute(f"CREATE DATABASE test1 VGROUPS 2;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"CREATE STABLE st (time TIMESTAMP, ca DOUBLE, cb DOUBLE, cc int) TAGS (ta VARCHAR(10) );"
        )

        tdLog.info(f"========== create table before stream")
        tdSql.execute(f"CREATE TABLE t1 using st TAGS ('aaa');")
        tdSql.execute(f"CREATE TABLE t2 using st TAGS ('bbb');")
        tdSql.execute(f"CREATE TABLE t3 using st TAGS ('ccc');")
        tdSql.execute(f"CREATE TABLE t4 using st TAGS ('ddd');")

        tdSql.execute(
            f'create stream streamd1 into streamt1 as select ca, _wstart,_wend, count(*) from st where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by ca interval(60m) fill(linear);'
        )
        tdSql.execute(
            f'create stream streamd2 into streamt2 as select tbname, _wstart,_wend, count(*) from st where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by tbname interval(60m) fill(linear);'
        )

        tdSql.execute(
            f'create stream streamd3 into streamt3 as select ca, _wstart,_wend, count(*), max(ca), min(cb), APERCENTILE(cc, 20) from st where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by ca session(time, 60m);'
        )
        tdSql.execute(
            f'create stream streamd4 into streamt4 as select tbname, _wstart,_wend, count(*), max(ca), min(cb), APERCENTILE(cc, 20) from st where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by tbname session(time, 60m);'
        )

        tdSql.execute(
            f'create stream streamd5 into streamt5 as select tbname, _wstart,_wend, count(*), max(ca), min(cb) from st where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by tbname state_window(cc);'
        )
        tdSql.execute(
            f'create stream streamd6 into streamt6 as select ca, _wstart,_wend, count(*), max(ca), min(cb) from t1 where time > "2022-01-01 00:00:00" and time < "2032-01-01 00:00:00" partition by ca state_window(cc);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"alter local 'keepColumnName' '1'")
        tdSql.execute(
            f"CREATE STABLE `meters_test_data` (`ts` TIMESTAMP, `close` FLOAT, `parttime` TIMESTAMP, `parttime_str` VARCHAR(32)) TAGS (`id` VARCHAR(32));"
        )

        tdSql.error(
            f"create stream realtime_meters fill_history 1 into realtime_meters as select last(parttime),first(close),last(close) from meters_test_data partition by tbname state_window(parttime_str);"
        )
        tdSql.error(
            f"create stream streamd7 into streamt7 as select  _wstart, _wend, count(*), first(ca), last(ca) from t1 interval(10s);"
        )
        tdSql.error(
            f"create stream streamd71 into streamt71 as select _wstart, _wend, count(*) as ca, first(ca), last(ca) as c2 from t1 interval(10s);"
        )

        tdSql.execute(
            f"create stream streamd8 into streamt8 as select _wstart, _wend, count(*), first(ca) as c1, last(ca) as c2 from t1 interval(10s);"
        )
        tdSql.query(f"desc streamt8;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.execute(
            f"create stream streamd9 into streamt9 as select  _wstart, _wend, count(*), first(ca) as c1, last(ca) from t1 interval(10s);"
        )
        tdSql.query(f"desc streamt9;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.error(
            f"create stream streamd11 into streamd11 as select _wstart, _wend, count(*), last(ca), last(ca) from t1 interval(10s);"
        )

        tdSql.execute(f"alter local 'keepColumnName' '0'")

        tdSql.execute(
            f"create stream realtime_meters fill_history 1 into realtime_meters as select last(parttime),first(close),last(close) from meters_test_data partition by tbname state_window(parttime_str);"
        )
        tdSql.query(f"desc realtime_meters;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.execute(
            f"create stream streamd7 into streamt7 as select  _wstart t1, _wend t2, count(*), first(ca), last(ca) from t1 interval(10s);"
        )
        tdSql.query(f"desc streamt7;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.execute(
            f"create stream streamd71 into streamt71 as select _wstart, _wend, count(*) as ca, first(ca), last(ca) as c2 from t1 interval(10s);"
        )
        tdSql.query(f"desc streamt71;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.execute(f"drop stream if exists streamd1;")
        tdSql.execute(f"drop stream if exists streamd2;")
        tdSql.execute(f"drop stream if exists streamd3;")
        tdSql.execute(f"drop stream if exists streamd4;")
        tdSql.execute(f"drop stream if exists streamd5;")
        tdSql.execute(f"drop stream if exists streamd6;")

        tdSql.execute(
            f"create stream streamd10 into streamd10 as select _wstart, _wend, count(*), first(ca), last(cb) as c2 from t1 interval(10s);"
        )
        tdSql.query(f"desc streamd10;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.error(
            f"create stream streamd11 into streamd11 as select _wstart, _wend, count(*), last(ca), last(ca) from t1 interval(10s);"
        )

        tdSql.execute(
            f"create stream streamd12 into streamd12 as select _wstart, _wend, count(*), last(ca), last(cb) as c2 from t1 interval(10s);"
        )
        tdSql.query(f"desc streamd12;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdLog.info(f"========== step2")
        tdSql.execute(f"CREATE DATABASE test2 VGROUPS 2;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"CREATE STABLE st (time TIMESTAMP, ca DOUBLE, cb DOUBLE, cc int) TAGS (ta VARCHAR(10) );"
        )
        tdSql.error(
            f"create stream stream_t1 trigger at_once ignore update 0 ignore expired 0 into streamtST as select time, count(*) c1, count(1) c2  from st partition by tbname group by ca,time ;"
        )
        tdSql.error(
            f"create stream stream_t1 trigger at_once ignore update 0 ignore expired 0 into streamtST as select time, count(*) c1, count(1) c2  from st group by ca,time ;"
        )

    def stream_basic_4(self):
        tdLog.info(f"stream_basic_4")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1=============")

        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams0 trigger at_once ignore expired 0 ignore update 0  into streamt as select  _wstart, count(*) c1 from t1 interval(1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791212001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791214003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791215003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791216004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791217004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791218004,4,2,3,4.1);")

        tdSql.execute(f"insert into t1 values(1648791221004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791222004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791223004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791224004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791225005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791226005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791227005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791228005,4,2,3,4.1);")

        tdSql.checkResultsByFunc(f"select * from streamt;", lambda: tdSql.getRows() == 16)

        tdSql.execute(
            f"insert into t1 values(1648791231004,4,2,3,4.1) (1648791232004,4,2,3,4.1) (1648791233004,4,2,3,4.1) (1648791234004,4,2,3,4.1) (1648791235004,4,2,3,4.1) (1648791236004,4,2,3,4.1) (1648791237004,4,2,3,4.1) (1648791238004,4,2,3,4.1) (1648791239004,4,2,3,4.1) (1648791240004,4,2,3,4.1) (1648791241004,4,2,3,4.1) (1648791242004,4,2,3,4.1) (1648791243004,4,2,3,4.1);"
        )
        tdSql.checkResultsByFunc(f"select * from streamt;", lambda: tdSql.getRows() == 29)

        tdLog.info(f"step2=============")
        tdSql.execute(f"create database test2  vgroups 10;")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(
            f"create stream streams2 trigger at_once ignore expired 0 ignore update 0 waterMark 200s  into streamt2 as select  _wstart, count(*) c1 from t1 interval(1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791212001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791214003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791215003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791216004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791217004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791218004,4,2,3,4.1);")

        tdSql.execute(f"insert into t1 values(1648791221004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791222004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791223004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791224004,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791225005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791226005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791227005,4,2,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791228005,4,2,3,4.1);")

        tdSql.checkResultsByFunc(f"select * from streamt2;", lambda: tdSql.getRows() == 16)

        tdSql.execute(
            f"insert into t1 values(1648791231004,4,2,3,4.1) (1648791232004,4,2,3,4.1) (1648791233004,4,2,3,4.1) (1648791234004,4,2,3,4.1) (1648791235004,4,2,3,4.1) (1648791236004,4,2,3,4.1) (1648791237004,4,2,3,4.1) (1648791238004,4,2,3,4.1) (1648791239004,4,2,3,4.1) (1648791240004,4,2,3,4.1) (1648791241004,4,2,3,4.1) (1648791242004,4,2,3,4.1) (1648791243004,4,2,3,4.1);"
        )
        tdSql.checkResultsByFunc(f"select * from streamt2;", lambda: tdSql.getRows() == 29)

        tdLog.info(f"step3=============")
        tdSql.execute(f"create database test1  vgroups 1;")
        tdSql.execute(f"use test1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once ignore expired 0 ignore update 0 into streamt1 as select  _wstart, count(*) c1 from t1 session(ts, 1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791215000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791217000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791219000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791221000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791225000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791227000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791229000,1,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791231000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791235000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791237000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791239000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791241000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791245000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791247000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791249000,1,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791251000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791253000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791255000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791257000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791259000,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791261000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791263000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791265000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791267000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791269000,1,2,3,1.0);")

        tdSql.checkResultsByFunc(f"select * from streamt1;", lambda: tdSql.getRows() == 30)

        tdSql.execute(f"insert into t1 values(1648791211001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791215001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791217001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791219001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791221001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791225001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791227001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791229001,1,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(9, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791231001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791235001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791237001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791239001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791241001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791245001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791247001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791249001,1,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(10, 1) == 2
            and tdSql.getData(19, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791251001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791253001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791255001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791257001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791259001,1,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791261001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791263001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791265001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791267001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791269001,1,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(20, 1) == 2
            and tdSql.getData(29, 1) == 2,
        )

        tdLog.info(f"step4====")

        tdSql.execute(f"create database test4 vgroups 1;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(2,2,2);")
        tdSql.execute(f"create table t4 using st tags(2,2,2);")
        tdSql.execute(f"create table t5 using st tags(2,2,2);")
        tdSql.execute(f"create table t6 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams4 trigger window_close IGNORE EXPIRED 0 into  streamt as select _wstart, count(*), now  from st partition by tbname interval(1s);"
        )
        tdSql.execute(
            f"create stream streams5 trigger window_close IGNORE EXPIRED 0 into  streamt1 as select _wstart, count(*), now  from st partition by b interval(1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791211000,1,1,1,1.1)  t2 values (1648791211000,2,2,2,2.1)  t3 values(1648791211000,3,3,3,3.1) t4 values(1648791211000,4,4,4,4.1)  t5 values (1648791211000,5,5,5,5.1)  t6 values(1648791211000,6,6,6,6.1);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791311000,1,1,1,1.1)  t2 values (1648791311000,2,2,2,2.1)  t3 values(1648791311000,3,3,3,3.1) t4 values(1648791311000,4,4,4,4.1)  t5 values (1648791311000,5,5,5,5.1)  t6 values(1648791311000,6,6,6,6.1);"
        )

        tdLog.info("1")
        tdSql.checkResultsByFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

        tdLog.info("2")
        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 6
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(2, 1) == 1,
        )

        tdSql.execute(
            f"insert into t1 values(1648791311001,1,1,1,1.1)  t2 values (1648791311001,2,2,2,2.1)  t3 values(1648791311001,3,3,3,3.1) t4 values(1648791311001,4,4,4,4.1)  t5 values (1648791311001,5,5,5,5.1)  t6 values(1648791311001,6,6,6,6.1);"
        )
        tdSql.execute(
            f"insert into t1 values(1648791311002,1,1,1,1.1)  t2 values (1648791311002,2,2,2,2.1)  t3 values(1648791311002,3,3,3,3.1) t4 values(1648791311002,4,4,4,4.1)  t5 values (1648791311002,5,5,5,5.1)  t6 values(1648791311002,6,6,6,6.1);"
        )

        tdLog.info("3")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1 desc;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdLog.info("4")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1 desc;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791211010,1,1,1,1.1)")
        tdSql.execute(f"insert into t2 values(1648791211020,2,2,2,2.1);")
        tdSql.execute(f"insert into t3 values(1648791211030,3,3,3,3.1);")
        tdSql.execute(f"insert into t4 values(1648791211040,4,4,4,4.1);")
        tdSql.execute(f"insert into t5 values(1648791211050,5,5,5,5.1);")
        tdSql.execute(f"insert into t6 values(1648791211060,6,6,6,6.1);")

        tdLog.info("3")
        tdSql.checkResultsByFunc(
            f"select * from streamt order by 1 desc;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 2,
        )

        tdLog.info("4")
        tdSql.checkResultsByFunc(
            f"select * from streamt1 order by 1 desc;",
            lambda: tdSql.getRows() > 2
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(2, 1) == 2,
        )

    def stream_basic_5(self):
        tdLog.info(f"stream_basic_5")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1   =============")

        tdLog.info(f"================ create snode")
        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

        tdSql.execute(f"create snode on dnode 1;")
        tdSql.query(f"show snodes;")
        tdSql.checkRows(1)

        tdLog.info(f"============== snode created , create db")

        tdSql.execute(f"create database test3  vgroups 1;")
        tdSql.execute(f"use test3;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams3 trigger at_once ignore expired 0 ignore update 0 into streamt3 as select  _wstart, count(*) c1 from t1 state_window(a);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791215000,3,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791217000,4,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791219000,5,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791221000,6,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,7,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791225000,8,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791227000,9,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791229000,10,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791231000,11,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000,12,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791235000,13,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791237000,14,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791239000,15,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791241000,16,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243000,17,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791245000,18,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791247000,19,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791249000,20,2,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791251000,21,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791253000,22,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791255000,23,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791257000,24,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791259000,25,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791261000,26,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791263000,27,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791265000,28,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791267000,29,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791269000,30,2,3,1.0);")

        tdLog.info("1")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(9, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791211001,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791215001,3,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791217001,4,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791219001,5,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791221001,6,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,7,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791225001,8,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791227001,9,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791229001,10,2,3,1.0);")

        tdLog.info("2")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(9, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791231001,11,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233001,12,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791235001,13,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791237001,14,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791239001,15,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791241001,16,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791243001,17,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791245001,18,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791247001,19,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791249001,20,2,3,1.0);")

        tdLog.info("3")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(10, 1) == 2
            and tdSql.getData(19, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791251001,21,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791253001,22,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791255001,23,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791257001,24,2,3,1.1);")

        tdLog.info("4")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 30 and tdSql.getData(20, 1) == 2,
        )

        tdSql.execute(f"insert into t1 values(1648791259001,25,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791261001,26,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791263001,27,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791265001,28,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791267001,29,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791269001,30,2,3,1.0);")

        tdLog.info("5")
        tdSql.checkResultsByFunc(
            f"select * from streamt3;",
            lambda: tdSql.getRows() == 30
            and tdSql.getData(20, 1) == 2
            and tdSql.getData(29, 1) == 2,
        )

        tdLog.info(f"step2=============")

        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int,d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams4 trigger at_once ignore expired 0 ignore update 0 into streamt4 as select  _wstart, first(a), b, c, ta, tb from st interval(1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000,2,3,4,1.1);")
        tdSql.execute(f"insert into t2 values(1648791215000,3,4,5,1.1);")
        tdSql.execute(f"insert into t2 values(1648791217000,4,5,6,1.1);")
        tdSql.checkResultsByFunc(
            f"select * from streamt4 order by 1;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 2) == 2
            and tdSql.getData(0, 3) == 3
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(0, 5) == 1
            and tdSql.getData(2, 2) == 4
            and tdSql.getData(2, 3) == 5
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 2,
        )

        tdLog.info(f"step3=============")

        tdSql.execute(f"create database test5  vgroups 4;")
        tdSql.execute(f"use test5;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int,d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams5 trigger at_once ignore expired 0 ignore update 0 into streamt5 as select  _wstart, b, c, ta, tb, max(b) from t1 interval(1s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213000,2,3,4,1.1);")
        tdSql.execute(f"insert into t1 values(1648791215000,3,4,5,1.1);")
        tdSql.execute(f"insert into t1 values(1648791217000,4,5,6,1.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt5 order by 1;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(2, 3) == 1
            and tdSql.getData(2, 4) == 1,
        )

        tdLog.info(f"step4=============")
        tdSql.execute(f"create database test6  vgroups 4;")
        tdSql.execute(f"use test6;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int,d int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams6 trigger at_once ignore expired 0 ignore update 0 into streamt6 as select  _wstart, b, c,min(c), ta, tb from st interval(1s);"
        )
        tdSql.execute(
            f"create stream streams7 trigger at_once ignore expired 0 ignore update 0 into streamt7 as select  ts, max(c) from st interval(1s);"
        )
        tdSql.execute(
            f"create stream streams8 trigger at_once ignore expired 0 ignore update 0 into streamt8 as select  ts, b, c, last(c), ta, tb from st session(ts, 1s);"
        )
        tdSql.execute(
            f"create stream streams9 trigger at_once ignore expired 0 ignore update 0 into streamt9 as select  ts, b, c, last_row(c), ta, tb from st partition by tbname state_window(a);"
        )
        tdSql.execute(
            f"create stream streams10 trigger at_once ignore expired 0 ignore update 0 into streamt10 as select  ts, b, c, last(c), ta, tb from st partition by tbname event_window start with d = 0 end with d = 9;"
        )
        tdSql.execute(
            f"create stream streams11 trigger at_once ignore expired 1 ignore update 0 watermark 100s into streamt11 as select  ts, b, c, last(c), ta, tb from st partition by tbname count_window(2);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3,0);")
        tdSql.execute(f"insert into t1 values(1648791213000,2,3,4,0);")
        tdSql.execute(f"insert into t2 values(1648791215000,3,4,5,0);")
        tdSql.execute(f"insert into t2 values(1648791217000,4,5,6,0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt6 order by 1;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 2
            and tdSql.getData(0, 2) == 3
            and tdSql.getData(0, 4) == 1
            and tdSql.getData(0, 5) == 1
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(2, 2) == 5
            and tdSql.getData(2, 4) == 2
            and tdSql.getData(2, 5) == 2,
        )
