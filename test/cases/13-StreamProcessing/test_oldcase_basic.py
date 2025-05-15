import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic(self):
        """Stream basic test

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
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
        # self.stream_basic_2()
        # self.stream_basic_3()
        # self.stream_basic_4()
        # self.stream_basic_5()

    def stream_basic_0(self):
        tdLog.info(f"stream_basic_0")
        drop_all_stream_and_db()

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
        check_stream_status("s1")

        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:42:00.000', 234)")

        tdLog.info(f"=============== query data from child table")
        tdSql.queryCheckFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 234
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 234,
        )

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:43:00.000', -111)")

        tdLog.info(f"=============== query data from child table")
        tdSql.queryCheckFunc(
            f"select `_wstart`,`min(k)`,`max(k)`,sum_alias from outstb",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == -111
            and tdSql.getData(0, 2) == 234
            and tdSql.getData(0, 3) == 123,
        )

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into ct1 values('2022-05-08 03:53:00.000', 789)")

        tdLog.info(f"=============== query data from child table")
        tdSql.queryCheckFunc(
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
        drop_all_stream_and_db()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0   into streamt as select  _wstart, count(*) c1, count(d) c2 , sum(a) c3 , max(b)  c4, min(c) c5 from t1 interval(10s);"
        )
        check_stream_status("streams1")

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,2,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791213004,4,2,3,4.1);")
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
            f"select `_wstart`, c1, c2 ,c3 ,c4, c5 from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(1, 1) == 2
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(1, 3) == 24
            and tdSql.getData(1, 4) == 14
            and tdSql.getData(1, 5) == 13,
        )

        tdSql.execute(f"insert into t1 values(1648791223003,12,14,13,11.1);")
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
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
        check_stream_status()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1,1.0) t2 values(1648791213000,2,2,2,2.0) t3 values(1648791213000,3,3,3,3.0) t4 values(1648791213000,4,4,4,4.0);"
        )
        tdSql.queryCheckFunc(f"select * from streamt;", lambda: tdSql.getRows() == 4)

        tdSql.execute(
            f"insert into t1 values(1648791213000,5,5,5,5.0) t2 values(1648791213000,6,6,6,6.0) t5 values(1648791213000,7,7,7,7.0);"
        )
        tdSql.queryCheckFunc(
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
        tdSql.queryCheckFunc(
            f"select * from streamt order by c4 desc;",
            lambda: tdSql.getRows() > 0
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 8,
        )

        tdSql.queryCheckFunc(
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

        check_stream_status()
        tdSql.execute(f"insert into ts1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into ts1 values(1648791222001,2,2,3);")
        tdSql.queryCheckFunc(
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

        check_stream_status()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.queryCheckFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 0)

        tdSql.execute(f"insert into t1 values(1648791213000,6,2,3,1.0);")
        tdSql.queryCheckFunc(
            f"select * from streamt__4;",
            lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 1,
        )

        tdSql.execute(f"insert into t1 values(1648791213000,2,2,3,1.0);")
        tdSql.queryCheckFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 0)

        tdSql.execute(f"insert into t1 values(1648791223000,2,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,10,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791233000,10,2,3,1.0);")
        tdSql.queryCheckFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 2)

        tdSql.execute(f"insert into t1 values(1648791233000,2,2,3,1.0);")
        tdSql.queryCheckFunc(f"select * from streamt__4;", lambda: tdSql.getRows() == 1)

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
        check_stream_status()

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
        tdSql.queryCheckFunc(f"select * from streamt5;", lambda: tdSql.getRows() == 1)

        tdSql.queryCheckFunc(f"select * from streamt6;", lambda: tdSql.getRows() == 1)

        tdSql.execute(f"create database test7  vgroups 1;")
        tdSql.execute(f"use test7;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table ts1 using st tags(1,1,1);")
        tdSql.execute(
            f"create stream streams7 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt7 as select _wstart, count(*) from ts1 interval(10s) ;"
        )
        check_stream_status()

        tdSql.execute(f"insert into ts1 values(1648791211000,1,2,3);")
        tdSql.error(f"insert into ts1 values(-1648791211000,1,2,3);")
        tdSql.queryCheckFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdSql.error(
            f"insert into ts1 values(-1648791211001,1,2,3) (1648791211001,1,2,3);"
        )
        tdSql.query(f"select _wstart, count(*) from ts1 interval(10s) ;")

        tdSql.queryCheckFunc(
            f"select * from streamt7;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )


def check_stream_status(stream_name=""):
    for loop in range(60):
        if stream_name == "":
            tdSql.query(f"select * from information_schema.ins_stream_tasks")
            if tdSql.getRows() == 0:
                continue
            tdSql.query(
                f'select * from information_schema.ins_stream_tasks where status != "ready"'
            )
            if tdSql.getRows() == 0:
                return
        else:
            tdSql.query(
                f'select stream_name, status from information_schema.ins_stream_tasks where stream_name = "{stream_name}" and status == "ready"'
            )
            if tdSql.getRows() == 1:
                return
        time.sleep(1)

    tdLog.exit(f"stream task status not ready in {loop} seconds")


def drop_all_stream_and_db():
    dbList = tdSql.query("show databases", row_tag=True)
    for r in range(len(dbList)):
        if (
            dbList[r][0] != "information_schema"
            and dbList[r][0] != "performance_schema"
        ):
            tdSql.execute(f"drop database {dbList[r][0]}")

    streamList = tdSql.query("show streams", row_tag=True)
    for r in range(len(streamList)):
        tdSql.execute(f"drop stream {streamList[r][0]}")

    tdLog.info(f"drop {len(dbList)} databases, {len(streamList)} streams")
