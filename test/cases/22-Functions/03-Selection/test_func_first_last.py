from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncFirstLast:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_first_last(self):
        """First Last

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/first_last.sim

        """

        dbPrefix = "first_db"
        tbPrefix = "first_tb"
        stbPrefix = "first_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 60000
        tdLog.info(f"========== first_last.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db} maxrows 400")

        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c6 = x % 128
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {x} , {x} , {x} , {x} , {x} , {c6} , true, 'BINARY', 'NCHAR' )"
                )
                x = x + 1

            i = i + 1

        ts = ts + 60000
        tb = tbPrefix + "0"
        tdSql.execute(f"insert into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "1"
        tdSql.execute(f"insert into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "2"
        tdSql.execute(f"insert into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "3"
        tdSql.execute(f"insert into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "4"
        tdSql.execute(f"insert into {tb} (ts) values ( {ts} )")
        ts = ts0 - 60000
        tb = tbPrefix + "0"
        tdSql.execute(f"import into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "1"
        tdSql.execute(f"import into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "2"
        tdSql.execute(f"import into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "3"
        tdSql.execute(f"import into {tb} (ts) values ( {ts} )")
        tb = tbPrefix + "4"
        tdSql.execute(f"import into {tb} (ts) values ( {ts} )")

        tdLog.info(f"====== test data created")

        self.first_last_query()

        tdLog.info(f"================== restart server to commit data into disk")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.first_last_query()

        tdLog.info(f"=================> insert data regression test")
        tdSql.execute(f"create database test keep 36500")
        tdSql.execute(f"use test")
        tdSql.execute(f"create table tm0 (ts timestamp, k int)")

        tdLog.info(f"=========================> td-2298")
        ts0 = 1537146000000
        xs = 6000

        x = 0
        while x < 5000:
            ts = ts0 + xs
            ts1 = ts + xs
            x1 = x + 1

            tdSql.execute(f"insert into tm0 values ( {ts} , {x} ) ( {ts1} , {x1} )")
            x = x1
            ts0 = ts1

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use test")
        tdSql.query(f"select count(*), last(ts) from tm0 interval(1s)")
        tdSql.checkRows(10000)

        tdSql.query(f"select last(ts) from tm0 interval(1s)")
        tdSql.checkRows(10000)

    def first_last_query(self):
        dbPrefix = "first_db"
        tbPrefix = "first_tb"
        stbPrefix = "first_stb"
        tbNum = 10
        rowNum = 2000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 60000
        tdLog.info(f"========== first_last_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"use {db}")
        tdSql.execute(f"use {db}")

        ##### select first/last from table
        ## TBASE-331
        tdLog.info(f"====== select first/last from table")
        tb = tbPrefix + "0"
        tdLog.info(f"select first(*) from {tb}")
        tdSql.query(f"select first(*) from {tb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 08:59:00")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(0, 2, 0)

        tdLog.info(f"tdSql.getData(0,3) = {tdSql.getData(0,3)}")
        tdSql.checkData(0, 3, 0.00000)

        tdSql.checkData(0, 4, 0.000000000)

        tdSql.checkData(0, 5, 0)

        tdSql.checkData(0, 6, 0)

        tdSql.checkData(0, 7, 1)

        tdSql.checkData(0, 8, "BINARY")

        # if $tdSql.getData(0,9,  NULL then
        tdSql.checkData(0, 9, "NCHAR")

        tdLog.info(f"select last(*) from {tb}")
        tdSql.query(f"select last(*) from {tb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-18 01:40:00")

        tdSql.checkData(0, 1, 999)

        tdSql.checkData(0, 2, 999)

        tdSql.checkData(0, 3, 999.00000)

        tdSql.checkData(0, 4, 999.000000000)

        # if $tdSql.getData(0,5,  NULL then
        tdSql.checkData(0, 5, 999)

        # if $tdSql.getData(0,6,  NULL then
        tdSql.checkData(0, 6, 103)

        # if $tdSql.getData(0,7,  NULL then
        tdSql.checkData(0, 7, 1)

        # if $tdSql.getData(0,8,  NULL then
        tdSql.checkData(0, 8, "BINARY")

        # if $tdSql.getData(0,9,  NULL then
        tdSql.checkData(0, 9, "NCHAR")

        ### test if first works for committed data. An 'order by ts desc' clause should be present, and queried data should come from at least 2 file blocks
        tb = tbPrefix + "9"
        tdSql.query(
            f"select first(ts), first(c1) from {tb} where ts < '2018-10-17 10:00:00.000'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 0)

        tb = tbPrefix + "9"
        tdSql.query(
            f"select first(ts), first(c1) from {tb} where ts < '2018-10-17 10:00:00.000'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-17 09:00:00")

        tdSql.checkData(0, 1, 0)

        tdLog.info(f"=============> add check for out of range first/last query")
        tdSql.query(
            f"select first(ts),last(ts) from first_tb4 where ts>'2018-9-18 1:40:01';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select first(ts),last(ts) from first_tb4 where ts<'2018-9-17 8:50:0';"
        )
        tdSql.checkRows(0)

        # first/last mix up query
        # select first(size),last(size) from stest interval(1d) group by tbname;
        tdLog.info(f"=====================>td-1477")

        tdSql.execute(
            f"create table stest(ts timestamp,size INT,filenum INT) tags (appname binary(500),tenant binary(500));"
        )
        tdSql.execute(
            f"insert into test1 using stest tags('test1','aaa') values ('2020-09-04 16:53:54.003',210,3);"
        )
        tdSql.execute(
            f"insert into test2 using stest tags('test1','aaa') values ('2020-09-04 16:53:56.003',210,3);"
        )
        tdSql.execute(
            f"insert into test11 using stest tags('test11','bbb') values ('2020-09-04 16:53:57.003',210,3);"
        )
        tdSql.execute(
            f"insert into test12 using stest tags('test11','bbb') values ('2020-09-04 16:53:58.003',210,3);"
        )
        tdSql.execute(
            f"insert into test21 using stest tags('test21','ccc') values ('2020-09-04 16:53:59.003',210,3);"
        )
        tdSql.execute(
            f"insert into test22 using stest tags('test21','ccc') values ('2020-09-04 16:54:54.003',210,3);"
        )
        tdSql.query(
            f"select sum(size), appname from stest group by appname order by appname;;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 420)

        tdSql.checkData(1, 0, 420)

        tdSql.checkData(2, 0, 420)

        tdSql.checkData(0, 1, "test1")

        tdSql.checkData(1, 1, "test11")

        tdSql.checkData(2, 1, "test21")

        tdSql.query(
            f"select _wstart, sum(size), appname from stest partition by appname interval(1d)  order by appname;"
        )
        tdSql.checkRows(3)

        # 2020-09-04 00:00:00.000 |                   420 | test1                          |
        # 2020-09-04 00:00:00.000 |                   420 | test11                         |
        # 2020-09-04 00:00:00.000 |                   420 | test21                         |
        tdSql.checkData(0, 0, "2020-09-04 00:00:00")

        tdSql.checkData(1, 0, "2020-09-04 00:00:00")

        tdSql.checkData(2, 0, "2020-09-04 00:00:00")

        tdSql.checkData(0, 1, 420)

        tdSql.checkData(1, 1, 420)

        tdSql.checkData(2, 1, 420)

        tdSql.checkData(0, 2, "test1")

        tdSql.checkData(1, 2, "test11")

        tdSql.checkData(2, 2, "test21")

        tdLog.info(
            f"===================>td-1477, one table has only one block occurs this bug."
        )
        tdSql.query(
            f"select _wstart, first(size), count(*), LAST(SIZE), tbname from stest where tbname in ('test1', 'test2') partition by tbname interval(1d) order by tbname asc;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-09-04 00:00:00")

        tdSql.checkData(0, 1, 210)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 210)

        tdSql.checkData(0, 4, "test1")

        tdSql.checkData(1, 0, "2020-09-04 00:00:00")

        tdSql.checkData(1, 1, 210)

        tdSql.checkData(1, 2, 1)

        tdSql.checkData(1, 3, 210)

        tdSql.checkData(1, 4, "test2")

        tdSql.execute(f"drop table stest")

        tdLog.info(f"===================>td-3779")
        tdSql.execute(f"create table m1(ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table tm0 using m1 tags(1);")
        tdSql.execute(f"create table tm1 using m1 tags(2);")
        tdSql.execute(f"insert into tm0 values('2020-3-1 1:1:1', 112);")
        tdSql.execute(
            f"insert into tm1 values('2020-1-1 1:1:1', 1)('2020-3-1 0:1:1', 421);"
        )

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")
        tdSql.execute(f"use first_db0;")

        tdSql.query(f"select last(*), tbname from m1 group by tbname order by tbname;")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-03-01 01:01:01")

        tdSql.checkData(0, 1, 112)

        tdSql.checkData(0, 2, "tm0")

        tdSql.checkData(1, 0, "2020-03-01 00:01:01")

        tdSql.checkData(1, 1, 421)

        tdSql.checkData(1, 2, "tm1")

        tdSql.execute(f"drop table m1")
