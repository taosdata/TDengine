import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    sc,
    clusterComCheck,
    tdCom,
    etool,
)


class TestSelectFirstLast:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_first_last(self):
        """Select: First Last

        1. Perform First queries on child tables and supertables.
        2. Test time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        3. Test Last LRU (insufficient memory, multiple VGroups, complex queries).
        4. Test scenarios where FIRST() and LAST() return multiple rows of data.
        5. Test last_row, first, last function support 520 parameters.

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-26 Simon Guan Migrated from tsim/parser/first_last.sim
            - 2025-8-26 Simon Guan Migrated from tsim/parser/last_both.sim
            - 2025-8-26 Simon Guan Migrated from tsim/parser/last_cache.sim
            - 2025-8-26 Simon Guan Migrated from tsim/parser/last_groupby.sim
            - 2025-8-26 Simon Guan Migrated from tsim/parser/single_row_in_tb.sim
            - 2025-8-26 Simon Guan Migrated from tsim/query/cache_last.sim
            - 2025-8-26 Simon Guan Migrated from tsim/query/cache_last_tag.sim
            - 2025-8-26 Simon Guan Migrated from tsim/query/multires_func.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/first.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/last.sim

        """

        self.PareserFirstLast()
        tdStream.dropAllStreamsAndDbs()
        self.ParserLastBoth()
        tdStream.dropAllStreamsAndDbs()
        self.ParserLastCache()
        tdStream.dropAllStreamsAndDbs()
        self.ParserLastGroupBy()
        tdStream.dropAllStreamsAndDbs()
        self.ParserSingleRowInTb()
        tdStream.dropAllStreamsAndDbs()
        self.QueryCacheLast()
        tdStream.dropAllStreamsAndDbs()
        self.QueryCacheLastTag()
        tdStream.dropAllStreamsAndDbs()
        self.MultiRes()
        tdStream.dropAllStreamsAndDbs()
        self.ComputeFirst()
        tdStream.dropAllStreamsAndDbs()
        self.ComputeLast()
        tdStream.dropAllStreamsAndDbs()

    def PareserFirstLast(self):
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

    def ParserLastBoth(self):
        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} cachemodel 'none' minrows 10 stt_trigger 1"
        )
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp) tags (id int)"
        )
        tdSql.execute(f"create table tb1 using st2 tags (1);")
        tdSql.execute(f"create table tb2 using st2 tags (2);")
        tdSql.execute(f"create table tb3 using st2 tags (3);")
        tdSql.execute(f"create table tb4 using st2 tags (4);")
        tdSql.execute(f"create table tb5 using st2 tags (1);")
        tdSql.execute(f"create table tb6 using st2 tags (2);")
        tdSql.execute(f"create table tb7 using st2 tags (3);")
        tdSql.execute(f"create table tb8 using st2 tags (4);")
        tdSql.execute(f"create table tb9 using st2 tags (5);")
        tdSql.execute(f"create table tba using st2 tags (5);")
        tdSql.execute(f"create table tbb using st2 tags (5);")
        tdSql.execute(f"create table tbc using st2 tags (5);")
        tdSql.execute(f"create table tbd using st2 tags (5);")
        tdSql.execute(f"create table tbe using st2 tags (5);")
        tdSql.execute(f"create table tbf using st2 tags (5);")

        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.000\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.001\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.002\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.003\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.004\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.005\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.006\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.007\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.008\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.009\",28,  29, '30', -1005)"
        )
        tdSql.execute(f'delete from tb9 where ts = "2021-05-09 10:12:26.000"')
        tdSql.execute(f"flush database {db}")

        tdSql.execute(
            f"insert into tb1 values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )

        tdSql.execute(
            f"insert into tb2 values (\"2021-05-09 10:11:13\",-1,-2.0,'-3',  -1001)"
        )
        tdSql.execute(
            f'insert into tb2 values ("2021-05-10 10:11:14",-4,-5.0, NULL, -2001)'
        )
        tdSql.execute(
            f"insert into tb2 values (\"2021-05-11 10:11:15\",-6,  -7, '-8', -3001)"
        )

        tdSql.execute(
            f"insert into tb3 values (\"2021-05-09 10:12:17\", 7, 8.0, '9' , -1002)"
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:17",10,11.0, NULL, -2002)'
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:18",12,NULL, NULL, -3002)'
        )

        tdSql.execute(
            f"insert into tb4 values (\"2021-05-09 10:12:19\",13,14.0,'15' , -1003)"
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-10 10:12:20",16,17.0, NULL, -2003)'
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-11 10:12:21",18,NULL, NULL, -3003)'
        )

        tdSql.execute(
            f"insert into tb5 values (\"2021-05-09 10:12:22\",19,  20, '21', -1004)"
        )
        tdSql.execute(
            f'insert into tb6 values ("2021-05-11 10:12:23",22,  23, NULL, -2004)'
        )
        tdSql.execute(
            f"insert into tb7 values (\"2021-05-10 10:12:24\",24,NULL, '25', -3004)"
        )
        tdSql.execute(
            f"insert into tb8 values (\"2021-05-11 10:12:25\",26,NULL, '27', -4004)"
        )

        tdSql.execute(
            f'insert into tba values ("2021-05-10 10:12:27",31,  32, NULL, -2005)'
        )
        tdSql.execute(
            f"insert into tbb values (\"2021-05-10 10:12:28\",33,NULL, '35', -3005)"
        )
        tdSql.execute(
            f'insert into tbc values ("2021-05-11 10:12:29",36,  37, NULL, -4005)'
        )
        tdSql.execute(
            f'insert into tbd values ("2021-05-11 10:12:29",NULL,NULL,NULL,NULL )'
        )

        tdSql.execute(f"drop table tbf;")
        tdSql.execute(f"alter table st2 add column c1 int;")
        tdSql.execute(f"alter table st2 drop column c1;")

        self.last_both_query()

        tdSql.execute(f"flush database {db}")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.last_both_query()

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} minrows 10 stt_trigger 1")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp) tags (id int)"
        )
        tdSql.execute(f"create table tb1 using st2 tags (1);")
        tdSql.execute(f"create table tb2 using st2 tags (2);")
        tdSql.execute(f"create table tb3 using st2 tags (3);")
        tdSql.execute(f"create table tb4 using st2 tags (4);")
        tdSql.execute(f"create table tb5 using st2 tags (1);")
        tdSql.execute(f"create table tb6 using st2 tags (2);")
        tdSql.execute(f"create table tb7 using st2 tags (3);")
        tdSql.execute(f"create table tb8 using st2 tags (4);")
        tdSql.execute(f"create table tb9 using st2 tags (5);")
        tdSql.execute(f"create table tba using st2 tags (5);")
        tdSql.execute(f"create table tbb using st2 tags (5);")
        tdSql.execute(f"create table tbc using st2 tags (5);")
        tdSql.execute(f"create table tbd using st2 tags (5);")
        tdSql.execute(f"create table tbe using st2 tags (5);")
        tdSql.execute(f"create table tbf using st2 tags (5);")

        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.000\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.001\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.002\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.003\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.004\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.005\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.006\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.007\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.008\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26.009\",28,  29, '30', -1005)"
        )
        tdSql.execute(f'delete from tb9 where ts = "2021-05-09 10:12:26.000"')
        tdSql.execute(f"flush database {db}")

        tdSql.execute(
            f"insert into tb1 values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )

        tdSql.execute(
            f"insert into tb2 values (\"2021-05-09 10:11:13\",-1,-2.0,'-3',  -1001)"
        )
        tdSql.execute(
            f'insert into tb2 values ("2021-05-10 10:11:14",-4,-5.0, NULL, -2001)'
        )
        tdSql.execute(
            f"insert into tb2 values (\"2021-05-11 10:11:15\",-6,  -7, '-8', -3001)"
        )

        tdSql.execute(
            f"insert into tb3 values (\"2021-05-09 10:12:17\", 7, 8.0, '9' , -1002)"
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:17",10,11.0, NULL, -2002)'
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:18",12,NULL, NULL, -3002)'
        )

        tdSql.execute(
            f"insert into tb4 values (\"2021-05-09 10:12:19\",13,14.0,'15' , -1003)"
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-10 10:12:20",16,17.0, NULL, -2003)'
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-11 10:12:21",18,NULL, NULL, -3003)'
        )

        tdSql.execute(
            f"insert into tb5 values (\"2021-05-09 10:12:22\",19,  20, '21', -1004)"
        )
        tdSql.execute(
            f'insert into tb6 values ("2021-05-11 10:12:23",22,  23, NULL, -2004)'
        )
        tdSql.execute(
            f"insert into tb7 values (\"2021-05-10 10:12:24\",24,NULL, '25', -3004)"
        )
        tdSql.execute(
            f"insert into tb8 values (\"2021-05-11 10:12:25\",26,NULL, '27', -4004)"
        )

        tdSql.execute(
            f'insert into tba values ("2021-05-10 10:12:27",31,  32, NULL, -2005)'
        )
        tdSql.execute(
            f"insert into tbb values (\"2021-05-10 10:12:28\",33,NULL, '35', -3005)"
        )
        tdSql.execute(
            f'insert into tbc values ("2021-05-11 10:12:29",36,  37, NULL, -4005)'
        )
        tdSql.execute(
            f'insert into tbd values ("2021-05-11 10:12:29",NULL,NULL,NULL,NULL )'
        )

        tdSql.execute(f"drop table tbf")
        tdSql.execute(f"alter database {db} cachemodel 'both'")
        tdSql.execute(f"alter database {db} cachesize 2")

        time.sleep(11)

        self.last_both_no_ts()
        self.last_both_query()

    def last_both_query(self):
        db = "testdb"
        tdSql.execute(f"use {db}")
        tdLog.info(f'"test tb1"')

        tdSql.query(f"select last(ts) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(tb1.*,ts,f4) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, "2021-05-12 10:10:12")
        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdLog.info(f'"test tb2"')
        tdSql.query(f"select last(ts) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")
        tdSql.query(f"select last(f1) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -6)
        tdSql.query(f"select last(*) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")
        tdSql.checkData(0, 1, -6)
        tdSql.checkData(0, 2, -7.000000000)
        tdSql.checkData(0, 3, -8)

        tdSql.query(f"select last(tb2.*,ts,f4) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")
        tdSql.checkData(0, 1, -6)
        tdSql.checkData(0, 2, -7.000000000)
        tdSql.checkData(0, 3, -8)
        tdSql.checkData(0, 5, "2021-05-11 10:11:15")

        tdLog.info(f'"test tbd"')
        tdSql.query(f"select last(*) from tbd")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:12:29")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)

        tdLog.info(f'"test tbe"')
        tdSql.query(f"select last(*) from tbe")
        tdSql.checkRows(0)

        tdLog.info(f'"test stable"')
        tdSql.query(f"select last(ts) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 37.000000000)
        tdSql.checkData(0, 3, 27)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(st2.*,ts,f4) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 37.000000000)
        tdSql.checkData(0, 3, 27)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, "2021-05-12 10:10:12")
        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdSql.query(f"select last(*), id from st2 group by id order by id")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 21)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, "2021-05-11 10:12:23")
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(1, 2, 23.000000000)
        tdSql.checkData(1, 3, -8)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(2, 0, "2021-05-10 10:12:24")
        tdSql.checkData(2, 1, 24)
        tdSql.checkData(2, 2, 11.000000000)
        tdSql.checkData(2, 3, 25)
        tdSql.checkData(2, 5, 3)
        tdSql.checkData(3, 0, "2021-05-11 10:12:25")
        tdSql.checkData(3, 1, 26)
        tdSql.checkData(3, 2, 17.000000000)
        tdSql.checkData(3, 3, 27)
        tdSql.checkData(3, 5, 4)
        tdSql.checkData(4, 0, "2021-05-11 10:12:29")
        tdSql.checkData(4, 1, 36)
        tdSql.checkData(4, 2, 37.000000000)
        tdSql.checkData(4, 3, 35)
        tdSql.checkData(4, 5, 5)

        tdSql.query(f"select last_row(*), id from st2 group by id order by id")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, "2021-05-11 10:12:23")
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(1, 2, 23.000000000)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(2, 0, "2021-05-10 10:12:24")
        tdSql.checkData(2, 1, 24)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, 25)
        tdSql.checkData(2, 5, 3)
        tdSql.checkData(3, 0, "2021-05-11 10:12:25")
        tdSql.checkData(3, 1, 26)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, 27)
        tdSql.checkData(3, 5, 4)
        tdSql.checkData(4, 0, "2021-05-11 10:12:29")

        # if $tdSql.getData(4,1, "NULL then
        #  return -1
        # endi
        # if $tdSql.getData(4,2, "NULL then
        #  print $tdSql.getData(0,2)
        #  return -1
        # endi
        tdSql.checkData(4, 3, None)

        # if $tdSql.getData(4,4, "NULL then
        #  return -1
        # endi
        tdSql.checkData(4, 5, 5)

        tdLog.info(f'"test tbn"')
        tdSql.execute(
            f"create table if not exists tbn (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp)"
        )
        tdSql.execute(
            f"insert into tbn values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-13 10:10:12", NULL,NULL, NULL,NULL)'
        )

        tdSql.query(f"select last(*) from tbn;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-13 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.execute(f"alter table tbn add column c1 int;")
        tdSql.execute(f"alter table tbn drop column c1;")

    def last_both_no_ts(self):
        db = "testdb"
        tdSql.execute(f"use {db}")
        tdLog.info(f'"test tb1"')

        tdSql.query(f"select last_row(f1, f2, f3, f4) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, "1970-01-01 07:59:57")

        tdSql.query(f"select last_row(*) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(f1, f2, f3, f4) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 37.000000000)
        tdSql.checkData(0, 2, 27)
        tdSql.checkData(0, 3, "1970-01-01 07:59:57")

        tdSql.query(f"select last(*) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 37.000000000)
        tdSql.checkData(0, 3, 27)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

    def ParserLastCache(self):
        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} cachemodel 'last_value'")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp) tags (id int)"
        )
        tdSql.execute(f"create table tb1 using st2 tags (1);")
        tdSql.execute(f"create table tb2 using st2 tags (2);")
        tdSql.execute(f"create table tb3 using st2 tags (3);")
        tdSql.execute(f"create table tb4 using st2 tags (4);")
        tdSql.execute(f"create table tb5 using st2 tags (1);")
        tdSql.execute(f"create table tb6 using st2 tags (2);")
        tdSql.execute(f"create table tb7 using st2 tags (3);")
        tdSql.execute(f"create table tb8 using st2 tags (4);")
        tdSql.execute(f"create table tb9 using st2 tags (5);")
        tdSql.execute(f"create table tba using st2 tags (5);")
        tdSql.execute(f"create table tbb using st2 tags (5);")
        tdSql.execute(f"create table tbc using st2 tags (5);")
        tdSql.execute(f"create table tbd using st2 tags (5);")
        tdSql.execute(f"create table tbe using st2 tags (5);")

        tdSql.execute(
            f"insert into tb1 values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tb1 values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )

        tdSql.execute(
            f"insert into tb2 values (\"2021-05-09 10:11:13\",-1,-2.0,'-3',  -1001)"
        )
        tdSql.execute(
            f'insert into tb2 values ("2021-05-10 10:11:14",-4,-5.0, NULL, -2001)'
        )
        tdSql.execute(
            f"insert into tb2 values (\"2021-05-11 10:11:15\",-6,  -7, '-8', -3001)"
        )

        tdSql.execute(
            f"insert into tb3 values (\"2021-05-09 10:12:17\", 7, 8.0, '9' , -1002)"
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:17",10,11.0, NULL, -2002)'
        )
        tdSql.execute(
            f'insert into tb3 values ("2021-05-09 10:12:18",12,NULL, NULL, -3002)'
        )

        tdSql.execute(
            f"insert into tb4 values (\"2021-05-09 10:12:19\",13,14.0,'15' , -1003)"
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-10 10:12:20",16,17.0, NULL, -2003)'
        )
        tdSql.execute(
            f'insert into tb4 values ("2021-05-11 10:12:21",18,NULL, NULL, -3003)'
        )

        tdSql.execute(
            f"insert into tb5 values (\"2021-05-09 10:12:22\",19,  20, '21', -1004)"
        )
        tdSql.execute(
            f'insert into tb6 values ("2021-05-11 10:12:23",22,  23, NULL, -2004)'
        )
        tdSql.execute(
            f"insert into tb7 values (\"2021-05-10 10:12:24\",24,NULL, '25', -3004)"
        )
        tdSql.execute(
            f"insert into tb8 values (\"2021-05-11 10:12:25\",26,NULL, '27', -4004)"
        )

        tdSql.execute(
            f"insert into tb9 values (\"2021-05-09 10:12:26\",28,  29, '30', -1005)"
        )
        tdSql.execute(
            f'insert into tba values ("2021-05-10 10:12:27",31,  32, NULL, -2005)'
        )
        tdSql.execute(
            f"insert into tbb values (\"2021-05-10 10:12:28\",33,NULL, '35', -3005)"
        )
        tdSql.execute(
            f'insert into tbc values ("2021-05-11 10:12:29",36,  37, NULL, -4005)'
        )
        tdSql.execute(
            f'insert into tbd values ("2021-05-11 10:12:29",NULL,NULL,NULL,NULL )'
        )

        self.last_cache_query()

        tdSql.execute(f"flush database {db}")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.last_cache_query()

    def last_cache_query(self):
        db = "testdb"
        tdSql.execute(f"use {db}")
        tdLog.info(f'"test tb1"')

        tdSql.query(f"select last(ts) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(tb1.*,ts,f4) from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, "2021-05-12 10:10:12")
        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdLog.info(f'"test tb2"')
        tdSql.query(f"select last(ts) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")

        tdSql.query(f"select last(f1) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -6)

        tdSql.query(f"select last(*) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")
        tdSql.checkData(0, 1, -6)
        tdSql.checkData(0, 2, -7.000000000)
        tdSql.checkData(0, 3, -8)

        tdSql.query(f"select last(tb2.*,ts,f4) from tb2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:11:15")
        tdSql.checkData(0, 1, -6)
        tdSql.checkData(0, 2, -7.000000000)
        tdSql.checkData(0, 3, -8)
        tdSql.checkData(0, 5, "2021-05-11 10:11:15")

        tdLog.info(f'"test tbd"')
        tdSql.query(f"select last(*) from tbd")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-11 10:12:29")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)

        tdLog.info(f'"test tbe"')
        tdSql.query(f"select last(*) from tbe")
        tdSql.checkRows(0)

        tdLog.info(f'"test stable"')
        tdSql.query(f"select last(ts) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")

        tdSql.query(f"select last(f1) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select last(*) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 37.000000000)
        tdSql.checkData(0, 3, 27)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.query(f"select last(st2.*,ts,f4) from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 37.000000000)
        tdSql.checkData(0, 3, 27)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, "2021-05-12 10:10:12")
        tdSql.checkData(0, 6, "1970-01-01 07:59:57")

        tdSql.query(f"select last(*), id from st2 group by id order by id")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-05-12 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 21)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, "2021-05-11 10:12:23")
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(1, 2, 23.000000000)
        tdSql.checkData(1, 3, -8)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(2, 0, "2021-05-10 10:12:24")
        tdSql.checkData(2, 1, 24)
        tdSql.checkData(2, 2, 11.000000000)
        tdSql.checkData(2, 3, 25)
        tdSql.checkData(2, 5, 3)
        tdSql.checkData(3, 0, "2021-05-11 10:12:25")
        tdSql.checkData(3, 1, 26)
        tdSql.checkData(3, 2, 17.000000000)
        tdSql.checkData(3, 3, 27)
        tdSql.checkData(3, 5, 4)
        tdSql.checkData(4, 0, "2021-05-11 10:12:29")
        tdSql.checkData(4, 1, 36)
        tdSql.checkData(4, 2, 37.000000000)
        tdSql.checkData(4, 3, 35)
        tdSql.checkData(4, 5, 5)

        tdLog.info(f'"test tbn"')
        tdSql.execute(
            f"create table if not exists tbn (ts timestamp, f1 int, f2 double, f3 binary(10), f4 timestamp)"
        )
        tdSql.execute(
            f"insert into tbn values (\"2021-05-09 10:10:10\", 1, 2.0, '3',  -1000)"
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-10 10:10:11", 4, 5.0, NULL, -2000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-12 10:10:12", 6,NULL, NULL, -3000)'
        )
        tdSql.execute(
            f'insert into tbn values ("2021-05-13 10:10:12", NULL,NULL, NULL,NULL)'
        )

        tdSql.query(f"select last(*) from tbn;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-05-13 10:10:12")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "1970-01-01 07:59:57")

        tdSql.execute(f"alter table tbn add column c1 int;")
        tdSql.execute(f"alter table tbn drop column c1;")

    def ParserLastGroupBy(self):
        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 float, f3 double, f4 bigint, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10)) tags (id1 int, id2 float, id3 nchar(10), id4 double, id5 smallint, id6 bigint, id7 binary(10))"
        )
        tdSql.execute(f'create table tb1 using st2 tags (1,1.0,"1",1.0,1,1,"1");')

        tdSql.execute(f'insert into tb1 values (now-200s,1,1.0,1.0,1,1,1,true,"1","1")')
        tdSql.execute(f'insert into tb1 values (now-100s,2,2.0,2.0,2,2,2,true,"2","2")')
        tdSql.execute(f'insert into tb1 values (now,3,3.0,3.0,3,3,3,true,"3","3")')
        tdSql.execute(f'insert into tb1 values (now+100s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+200s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+300s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+400s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+500s,4,4.0,4.0,4,4,4,true,"4","4")')

        tdSql.query(f"select f1, last(*) from st2 group by f1 order by f1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.00000)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 1)
        tdSql.checkData(0, 9, 1)

        tdSql.query(f"select f1, last(f1,st2.*) from st2 group by f1 order by f1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1.00000)
        tdSql.checkData(0, 5, 1.000000000)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 1)
        tdSql.checkData(0, 9, 1)

    def ParserSingleRowInTb(self):
        dbPrefix = "sr_db"
        tbPrefix = "sr_tb"
        stbPrefix = "sr_stb"
        ts0 = 1537146000000
        tdLog.info(f"========== single_row_in_tb.sim")
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 bool, c6 binary(10), c7 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        tb1 = tbPrefix + "1"
        tdSql.execute(f"create table {tb1} using {stb} tags( 1 )")
        tdSql.execute(
            f"insert into {tb1} values ( {ts0} , 1, 2, 3, 4, true, 'binay10', '涛思nchar10' )"
        )
        tdLog.info(f"====== tables created")

        self.single_row_in_tb_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.single_row_in_tb_query()

    def single_row_in_tb_query(self):
        dbPrefix = "sr_db"
        tbPrefix = "sr_tb"
        stbPrefix = "sr_stb"
        ts0 = 1537146000000
        tdLog.info(f"========== single_row_in_tb_query.sim")
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"use {db}")
        tb1 = tbPrefix + "1"

        tdSql.query(
            f"select first(ts, c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select last(ts, c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select first(ts, c1), last(c1) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select first(ts, c1), last(c2) from {stb} where ts >= {ts0} and ts < now group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)

        tdSql.query(f"select first(ts, c1) from {tb1} where ts >= {ts0} and ts < now")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select last(ts, c1) from {tb1} where ts >= {ts0} and ts < now")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select first(ts, c1), last(c1) from {tb1} where ts >= {ts0} and ts < now"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select first(ts, c1), last(c2) from {tb1} where ts >= {ts0} and ts < now"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)

        #### query a STable and using where clause
        tdSql.query(
            f"select first(ts,c1), last(ts,c1), spread(c1), t1 from {stb} where ts >= {ts0} and ts < '2018-09-20 00:00:00.000' group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2018-09-17 09:00:00")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 0.000000000)
        tdSql.checkData(0, 5, 1)

        tdSql.query(
            f"select _wstart, first(c1), last(c1) from sr_stb where ts >= 1537146000000 and ts < '2018-09-20 00:00:00.000' partition by t1 interval(1d)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 00:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select max(c1), min(c1), sum(c1), avg(c1), count(c1), t1 from {stb} where c1 > 0 group by t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.000000000)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, 1)

        tdSql.query(
            f"select _wstart, first(ts,c1), last(ts,c1) from {tb1} where ts >= {ts0} and ts < '2018-09-20 00:00:00.000' interval(1d)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 00:00:00")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "2018-09-17 09:00:00")
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"===============>safty check TD-4927")
        tdSql.query(f"select first(ts, c1) from sr_stb where ts<1 group by t1;")
        tdSql.query(f"select first(ts, c1) from sr_stb where ts>0 and ts<1;")

    def QueryCacheLast(self):
        tdSql.execute(
            f"create database if not exists db1 cachemodel 'both' cachesize 10;"
        )
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 2.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 4.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 5.0, \"b\");")
        tdSql.execute(f"create table tba2 using sta tags(2);")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 1.2, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 2.2, \"b\");")
        tdSql.execute(f"create table tba3 using sta tags(3);")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:10', 1.3, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:11', 2.3, \"b\");")
        tdSql.query(f"select count(*), last(*) from sta;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, "2022-04-26 15:15:11")
        tdSql.checkData(0, 2, 2.300000000)
        tdSql.checkData(0, 3, "b")

        tdSql.query(f"explain select count(*), last(*) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=4 width=226 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"explain select first(f1), last(*) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=4 width=226 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"select first(f1), last(*) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"select last_row(f1), last(f1) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*), last_row(f1), last(f1) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"explain select count(*), last_row(f1), last(f1) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.error(f"select count(*), last_row(f1), min(f1), f1 from sta;")
        tdSql.query(
            f"select count(*), last_row(f1), min(f1),tbname from sta partition by tbname;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),tbname from sta partition by tbname;"
        )
        tdSql.checkData(0, 0, "-> Data Exchange 2:1 (width=296)")

        tdSql.query(f"explain select count(*), last_row(f1), min(f1) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),tbname from sta group by tbname;"
        )
        tdSql.checkData(0, 0, "-> Data Exchange 2:1 (width=296)")

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),t1 from sta partition by t1;"
        )
        tdSql.checkData(0, 0, "-> Aggregate (functions=4 width=28 input_order=desc )")

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),t1 from sta group by t1;"
        )
        tdSql.checkData(0, 0, "-> Aggregate (functions=4 width=28 input_order=desc )")

        tdSql.query(
            f"explain select distinct count(*), last_row(f1), min(f1) from sta;"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1) from sta interval(1s);"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=4 width=122 input_order=asc output_order=asc mode=sort)",
        )

        tdSql.query(
            f"explain select distinct count(*), last_row(f1), min(f1) from tba1;"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"select distinct count(*), last_row(f1), min(f1) from tba1;")
        tdSql.checkRows(1)

        tdLog.info(f"step 2-------------------------------")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  cachemodel 'both';")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table stb (ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )

        tdSql.execute(f"create table t1 using stb tags(1,1,1);")
        tdSql.execute(f"create table t2 using stb tags(2,2,2);")
        tdSql.execute(f"insert into t1 values('2024-06-05 11:00:00',1,2,3);")
        tdSql.execute(f"insert into t1 values('2024-06-05 12:00:00',2,2,3);")
        tdSql.execute(f"insert into t2 values('2024-06-05 13:00:00',3,2,3);")
        tdSql.execute(f"insert into t2 values('2024-06-05 14:00:00',4,2,3);")

        tdSql.query(f"select last(ts) ts1,ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))

        tdSql.query(f"select last(ts) ts1,ts from stb group by tbname;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))

        tdSql.query(f"select last(ts) ts1,tbname, ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 1, "t2")

        tdSql.query(
            f"select last(ts) ts1,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 1, "t2")

        tdLog.info(f"step 3-------------------------------")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1  cachemodel 'both' vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table stb (ts timestamp,a int COMPOSITE key,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute("alter local 'showFullCreateTableColumn' '1'")
        tdSql.query(f"show create table stb")
        tdSql.checkData(
            0,
            1,
            "CREATE STABLE `stb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `a` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' COMPOSITE KEY, `b` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `c` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`ta` INT, `tb` INT, `tc` INT)",
        )

        tdSql.query(f"desc stb")
        tdSql.checkData(1, 3, "COMPOSITE KEY")

        tdSql.execute(f"create table aaat1 using stb tags(1,1,1);")
        tdSql.execute(f"create table bbbt2 using stb tags(2,2,2);")
        tdSql.execute(f"insert into aaat1 values('2024-06-05 11:00:00',1,2,3);")
        tdSql.execute(f"insert into aaat1 values('2024-06-05 12:00:00',2,2,3);")
        tdSql.execute(f"insert into bbbt2 values('2024-06-05 13:00:00',3,2,3);")
        tdSql.execute(f"insert into bbbt2 values('2024-06-05 14:00:00',4,2,3);")

        tdSql.query(f"select last(ts) ts1,ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")

        tdSql.query(f"select last(ts) ts1,ts from stb group by tbname order by 1 desc;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")

        tdSql.query(f"select last(ts) ts1,tbname, ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")

        tdSql.query(
            f"select last(ts) ts1,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")
        tdLog.info(f"{tdSql.getData(0,1)}")

        tdLog.info(f"step 4-------------------------------")

        tdSql.query(f"select last(a) a,ts from stb;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "2024-06-05 14:00:00")

        tdSql.query(f"select last(a) a,ts from stb group by tbname order by 1 desc;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "2024-06-05 14:00:00")

        tdSql.query(f"select last(a) a,tbname, ts from stb;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, "2024-06-05 14:00:00")

        tdSql.query(
            f"select last(a) a,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, "2024-06-05 14:00:00")

        tdLog.info(f"step 5-------------------------------")

        tdSql.query(f"select last(ts) ts1,a from stb;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, 4)

        tdSql.query(f"select last(ts) ts1,a from stb group by tbname order by 1 desc;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, 4)

        tdSql.query(f"select last(ts) ts1,tbname, a from stb;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, 4)

        tdSql.query(
            f"select last(ts) ts1,tbname, a from stb group by tbname order by 1 desc;"
        )
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, 4)

        tdSql.query(f"select last(ts), last_row(ts) from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))

    def QueryCacheLastTag(self):
        tdSql.execute(f'alter local "multiResultFunctionStarReturnTags" "0";')

        tdLog.info(f"step1=====================")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 4 CACHEMODEL 'both';")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")
        tdSql.checkCols(8)

        tdSql.execute(f'alter local "multiResultFunctionStarReturnTags" "1";')

        tdSql.query(f"select last(*),last_row(*) from st;")
        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")
        tdSql.checkCols(8)

        tdLog.info(f"step2=====================")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test1  vgroups 4 CACHEMODEL 'last_row';")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")
        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")
        tdSql.checkCols(8)

        tdLog.info(f"step3=====================")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test2  vgroups 4 CACHEMODEL 'last_value';")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")
        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")
        tdSql.checkCols(8)

        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")
        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")
        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")
        tdSql.checkCols(8)

    def MultiRes(self):
        tdSql.execute(f"create database test")
        tdSql.execute(f"use test")
        tdSql.execute(
            f"CREATE TABLE `tb` (`ts` TIMESTAMP, `c0` INT, `c1` FLOAT, `c2` BINARY(10))"
        )

        tdSql.execute(
            f'insert into tb values("2022-05-15 00:01:08.000", 1, 1.0, "abc")'
        )
        tdSql.execute(
            f'insert into tb values("2022-05-16 00:01:08.000", 2, 2.0, "bcd")'
        )
        tdSql.execute(
            f'insert into tb values("2022-05-17 00:01:08.000", 3, 3.0, "cde")'
        )

        resultfile = tdCom.generate_query_result(
            "cases/22-Functions/03-Selection/t/multires_func.sql", "test_func_multires"
        )
        tdLog.info(f"resultfile: {resultfile}")
        tdCom.compare_result_files(
            resultfile, "cases/22-Functions/03-Selection/r/multires_func.result"
        )

    def ComputeFirst(self):
        dbPrefix = "m_fi_db"
        tbPrefix = "m_fi_tb"
        mtPrefix = "m_fi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select first(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select first(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select first(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select first(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(4, 0, 4)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select first(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select first(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select first(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select first(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"select first(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select first(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select first(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select first(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ComputeLast(self):
        dbPrefix = "m_la_db"
        tbPrefix = "m_la_tb"
        mtPrefix = "m_la_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select last(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select last(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select last(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select last(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select last(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select last(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select last(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select last(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select last(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select last(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select last(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select last(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select last(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(
            f"select last(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
