from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestGroupByBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby_basic(self):
        """Group by basic

        1. Including multiple data types
        2. Including data columns and tag columns
        3. With ORDER BY clause
        4. With Limit offset clause
        5. With filtering conditions
        6. With various functions
        7. With different windows

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/groupby-basic.sim
            - 2025-5-6 Simon Guan Migrated from tsim/parser/groupby.sim
            - 2025-5-8 Simon Guan Migrated from tsim/parser/top_groupby.sim
            - 2025-5-6 Simon Guan Migrated from tsim/query/groupby.sim
            - 2025-5-6 Simon Guan Migrated from tsim/query/groupby_distinct.sim
            - 2025-5-6 Simon Guan Migrated from tsim/query/complex_group.sim

        """

        self.ParserGroupbyBasic()
        tdStream.dropAllStreamsAndDbs()
        self.ParserGroupby()
        tdStream.dropAllStreamsAndDbs()
        self.ParserTopGroupby()
        tdStream.dropAllStreamsAndDbs()
        self.QueryGroupby()
        tdStream.dropAllStreamsAndDbs()
        self.QueryGroupbyDistinct()
        tdStream.dropAllStreamsAndDbs()
        self.QueryComplexGroupby()
        tdStream.dropAllStreamsAndDbs()

    def ParserGroupbyBasic(self):
        # ========================================= setup environment ================================

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"
        tbNum = 8
        rowNum = 100
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== groupby.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"

        tdLog.info(f"==== create db, stable, ctables, insert data")
        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {x} , {x} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                # print ==== insert into $tb values ($tstart , $c , $c , $x , $x , $c , $c , $c , $binary , $nchar )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 1640966400000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        ts1 = tb1 + "ts"
        ts2 = tb2 + "ts"

        tdLog.info(f"===============================groupby_operation")
        tdLog.info(f"")
        tdLog.info(f"==== select count(*), c1 from group_tb0 group by c1 order by c1")
        tdSql.query(f"select count(*), c1 from group_tb0 group by c1 order by c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(9, 0, 10)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(9, 1, 9)

        tdLog.info(f"==== select first(ts),c1 from group_tb0 group by c1 order by c1;")
        tdSql.query(f"select first(ts),c1 from group_tb0 group by c1 order by c1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2022-01-01 00:00:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(9, 0, "2022-01-01 00:00:00.009")
        tdSql.checkData(9, 1, 9)

        tdLog.info(f"==== select first(ts),c1 from interval(5m) group_tb0 group by c1;")
        tdSql.query(f"select first(ts),c1 from group_tb0 group by c1;")
        tdLog.info(f"rows: {tdSql.getRows()})")

    def ParserGroupby(self):
        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"
        tbNum = 8
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== groupby.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        half = tbNum / 2

        i = 0
        while i < half:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"

            nextSuffix = int(i + half)
            tb1 = tbPrefix + str(nextSuffix)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {nextSuffix} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {x} , {x} , {c} , {c} , {c} , {binary} , {nchar} ) {tb1} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 100000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        ts1 = tb1 + "ts"
        ts2 = tb2 + "ts"

        tdLog.info(f"===============================groupby_operation")
        tdSql.query(
            f"select count(*),c1 from group_tb0 where c1 < 20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 1)

        tdSql.query(
            f"select first(ts),c1 from group_tb0 where c1 < 20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(9, 0, "1970-01-01 08:01:40.009")

        tdSql.checkData(9, 1, 9)

        tdSql.query(
            f"select first(ts), ts, c1 from group_tb0 where c1 < 20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)
        tdSql.checkAssert(tdSql.getData(0, 0) == tdSql.getData(0, 1))
        tdSql.checkAssert(tdSql.getData(1, 0) == tdSql.getData(1, 1))
        tdSql.checkAssert(tdSql.getData(2, 0) == tdSql.getData(2, 1))
        tdSql.checkAssert(tdSql.getData(9, 0) == tdSql.getData(9, 1))

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(1, 2, 1)

        tdSql.checkData(9, 2, 9)

        tdSql.query(
            f"select sum(c1), c1, avg(c1), min(c1), max(c2) from group_tb0 where c1 < 20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(0, 1, 0)

        tdLog.info(f"{tdSql.getData(0,2)}")
        tdSql.checkData(0, 2, 0.000000000)

        tdSql.checkData(0, 3, 0)

        tdLog.info(f"{tdSql.getData(0,4)}")
        tdSql.checkData(0, 4, 0.00000)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 1)

        tdLog.info(f"{tdSql.getData(1,2)}")
        tdSql.checkData(1, 2, 1.000000000)

        tdSql.checkData(1, 3, 1)

        tdSql.checkData(1, 4, 1.00000)

        tdSql.error(f"select sum(c1), ts, c1 from group_tb0 where c1<20 group by c1;")
        tdSql.query(
            f"      select first(ts), ts, c2 from group_tb0 where c1 < 20 group by c1;"
        )
        tdSql.error(f"select sum(c3), ts, c2 from group_tb0 where c1 < 20 group by c1;")
        tdSql.error(
            f"select sum(c3), first(ts), c2 from group_tb0 where c1 < 20 group by c1;"
        )
        tdSql.query(
            f"      select first(c3), ts, c1, c2 from group_tb0 where c1 < 20 group by c1;"
        )
        tdSql.error(
            f"select first(c3), last(c3), ts, c1 from group_tb0 where c1 < 20 group by c1;"
        )
        tdSql.error(f"select ts from group_tb0 group by c1;")

        # ===========================interval=====not support======================
        tdSql.error(
            f"select count(*), c1 from group_tb0 where c1<20 interval(1y) group by c1;"
        )
        # =====tbname must be the first in the group by clause=====================
        tdSql.query(
            f"select count(*) from group_tb0 where c1 < 20 group by c1, tbname;"
        )

        # super table group by normal columns
        tdSql.query(
            f"select count(*), c1 from group_mt0 where c1< 20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkData(0, 0, 80)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, 80)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(9, 0, 80)

        tdSql.checkData(9, 1, 9)

        tdSql.query(
            f"select first(c1), c1, ts from group_mt0 where c1<20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkAssert(tdSql.getData(0, 0) == tdSql.getData(0, 1))

        tdSql.checkData(0, 2, "1970-01-01 08:01:40")

        tdSql.checkAssert(tdSql.getData(1, 0) == tdSql.getData(1, 1))

        tdSql.checkData(1, 2, "1970-01-01 08:01:40.001")

        tdSql.checkAssert(tdSql.getData(2, 0) == tdSql.getData(2, 1))

        tdSql.checkData(2, 2, "1970-01-01 08:01:40.002")

        tdSql.checkAssert(tdSql.getData(9, 0) == tdSql.getData(9, 1))

        tdSql.checkData(9, 2, "1970-01-01 08:01:40.009")

        tdSql.query(
            f"select first(c1), last(ts), first(ts), last(c1),c1,sum(c1),avg(c1),count(*) from group_mt0 where c1<20 group by c1 order by c1;"
        )
        tdSql.checkRows(20)

        tdSql.checkAssert(tdSql.getData(0, 0) == tdSql.getData(0, 3))

        tdSql.checkData(0, 1, "1970-01-01 08:01:40.900")

        tdSql.checkData(0, 2, "1970-01-01 08:01:40")

        tdSql.checkData(0, 7, 80)

        tdSql.checkAssert(tdSql.getData(1, 0) == tdSql.getData(1, 3))

        tdSql.checkData(1, 1, "1970-01-01 08:01:40.901")

        tdSql.checkData(1, 2, "1970-01-01 08:01:40.001")

        tdSql.checkData(1, 7, 80)

        tdSql.checkAssert(tdSql.getData(9, 0) == tdSql.getData(9, 3))

        tdSql.checkData(9, 1, "1970-01-01 08:01:40.909")

        tdSql.checkData(9, 2, "1970-01-01 08:01:40.009")

        tdSql.checkData(9, 7, 80)

        tdSql.checkData(9, 5, 720)

        tdSql.checkData(9, 4, 9)

        tdSql.query(
            f"select c1,sum(c1),avg(c1),count(*) from group_mt0 where c1<5 group by c1 order by c1;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 1, 80)

        tdSql.query(
            f"select first(c1), last(ts), first(ts), last(c1),sum(c1),avg(c1),count(*),tbname from group_mt0 where c1<20 group by tbname, c1 order by c1;"
        )
        tdSql.checkRows(160)

        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.checkData(0, 1, "1970-01-01 08:01:40.900")

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 2, "1970-01-01 08:01:40")

        tdSql.checkData(0, 3, 0)

        tdSql.checkData(0, 4, 0)

        tdSql.checkData(0, 6, 10)

        tdSql.query(
            f"select count(*),first(ts),last(ts),min(c3) from group_tb1 group by c4 order by c4;"
        )
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, "1970-01-01 08:01:40")

        tdSql.checkData(0, 2, "1970-01-01 08:01:40")

        tdSql.checkData(0, 3, 0)

        tdSql.query(
            f"select count(*),first(ts),last(ts),min(c3) from group_tb1 group by c4 slimit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(*),first(ts),last(ts),min(c3) from group_tb1 group by c4 slimit 20 soffset 990;"
        )
        tdSql.checkRows(10)

        tdSql.query(
            f"select count(*),first(ts),last(ts),min(c3),max(c3),sum(c3),avg(c3),sum(c4)/count(c4) from group_tb1 group by c4;"
        )
        tdSql.checkRows(1000)

        tdLog.info(
            f"---------------------------------> group by binary|nchar data add cases"
        )
        tdSql.query(f"select count(*) from group_tb1 group by c8;")
        tdSql.checkRows(100)

        tdSql.query(
            f"select count(*),sum(c4), count(c4), sum(c4)/count(c4) from group_tb1 group by c8 order by c8;"
        )
        tdSql.checkRows(100)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 4500)

        tdSql.checkData(0, 2, 10)

        tdSql.checkData(0, 3, 450.000000000)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 4510)

        tdSql.checkData(1, 3, 451.000000000)

        tdLog.info(f"====================> group by normal column + slimit + soffset")
        tdSql.query(
            f"select count(*), c8 from group_mt0 group by c8 limit 100 offset 0;"
        )
        tdSql.checkRows(100)

        tdSql.query(
            f"select sum(c2),c8,avg(c2), sum(c2)/count(*) from group_mt0 partition by c8 slimit 2 soffset 99"
        )
        tdSql.checkRows(1)

        # if $tdSql.getData(0,0, 2160.000000000 then
        #  return -1
        # endi

        # if $tdSql.getData(0,1, @binary27@ then
        #  return -1
        # endi

        # if $tdSql.getData(0,2, 27.000000000 then
        #  return -1
        # endi

        # if $tdSql.getData(0,3, 27.000000000 then
        #  return -1
        # endi

        tdLog.info(f"============>td-1765")
        tdSql.query(
            f"select percentile(c4, 49),min(c4),max(c4),avg(c4),stddev(c4) from group_tb0 group by c8 order by c8;"
        )
        tdSql.checkRows(100)

        tdSql.checkData(0, 0, 441.000000000)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(0, 2, 900)

        tdSql.checkData(0, 3, 450.000000000)

        tdSql.checkData(0, 4, 287.228132327)

        tdSql.checkData(1, 0, 442.000000000)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 901)

        tdSql.checkData(1, 3, 451.000000000)

        tdSql.checkData(1, 4, 287.228132327)

        tdLog.info(f"================>td-2090")
        tdSql.query(
            f"select leastsquares(c2, 1, 1) from group_tb1  group by c8 order by c8;;"
        )
        tdSql.checkRows(100)

        tdSql.checkData(0, 0, "{slop:0.000000, intercept:0.000000}")

        tdSql.checkData(1, 0, "{slop:0.000000, intercept:1.000000}")

        tdSql.checkData(9, 0, "{slop:0.000000, intercept:17.000000}")

        # =========================== group by multi tags ======================
        tdSql.execute(
            f"create table st (ts timestamp, c int) tags (t1 int, t2 int, t3 int, t4 int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(1, 2, 2, 2);")
        tdSql.execute(
            f"insert into t1 values ('2020-03-27 04:11:16.000', 1)('2020-03-27 04:11:17.000', 2) ('2020-03-27 04:11:18.000', 3) ('2020-03-27 04:11:19.000', 4) ;"
        )
        tdSql.execute(
            f"insert into t1 values ('2020-03-27 04:21:16.000', 1)('2020-03-27 04:31:17.000', 2) ('2020-03-27 04:51:18.000', 3) ('2020-03-27 05:10:19.000', 4) ;"
        )
        tdSql.execute(
            f"insert into t2 values ('2020-03-27 04:11:16.000', 1)('2020-03-27 04:11:17.000', 2) ('2020-03-27 04:11:18.000', 3) ('2020-03-27 04:11:19.000', 4) ;"
        )
        tdSql.execute(
            f"insert into t2 values ('2020-03-27 04:21:16.000', 1)('2020-03-27 04:31:17.000', 2) ('2020-03-27 04:51:18.000', 3) ('2020-03-27 05:10:19.000', 4) ;"
        )

        tdLog.info(f"=================>TD-2665")
        tdSql.error(f"create table txx as select avg(c) as t from st;")
        tdSql.error(f"create table txx1 as select avg(c) as t from t1;")

        tdSql.query(f"select stddev(c),stddev(c) from st group by c order by c;")
        tdSql.checkRows(4)

        tdLog.info(f"=================>TD-2236")
        tdSql.query(f"select first(ts),last(ts) from t1 group by c order by c;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2020-03-27 04:11:16")

        tdSql.checkData(0, 1, "2020-03-27 04:21:16")

        tdSql.checkData(1, 0, "2020-03-27 04:11:17")

        tdSql.checkData(1, 1, "2020-03-27 04:31:17")

        tdSql.checkData(2, 0, "2020-03-27 04:11:18")

        tdSql.checkData(2, 1, "2020-03-27 04:51:18")

        tdSql.checkData(3, 0, "2020-03-27 04:11:19")

        tdSql.checkData(3, 1, "2020-03-27 05:10:19")

        tdLog.info(f"===============>")
        tdSql.query(
            f"select stddev(c),c from st where t2=1 or t2=2  group by c order by c;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 0.000000000)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 0, 0.000000000)

        tdSql.checkData(2, 1, 3)

        tdSql.checkData(3, 0, 0.000000000)

        tdSql.checkData(3, 1, 4)

        tdSql.error(
            f"select irate(c) from st where t1='1' and ts >= '2020-03-27 04:11:17.732' and ts < '2020-03-27 05:11:17.732'  interval(1m) sliding(15s) group by tbname,c;"
        )
        tdSql.query(
            f"select _wstart, irate(c), tbname, t1, t2 from st where t1=1 and ts >= '2020-03-27 04:11:17.732' and ts < '2020-03-27 05:11:17.732' partition by tbname,t1,t2 interval(1m) sliding(15s) order by tbname;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(40)

        tdSql.checkData(0, 2, "t1")

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 1)

        tdSql.checkData(1, 2, "t1")

        tdSql.checkData(1, 3, 1)

        tdSql.checkData(1, 4, 1)

        tdSql.query(
            f"select _wstart, irate(c), tbname, t1, t2 from st where t1=1 and ts >= '2020-03-27 04:11:17.732' and ts < '2020-03-27 05:11:17.732' partition by tbname, t1, t2 interval(1m) sliding(15s) order by tbname desc,_wstart asc limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1.000000000)

        tdSql.checkData(0, 2, "t2")

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 2)

        tdSql.execute(f"create table m1 (ts timestamp, k int, f1 int) tags(a int);")
        tdSql.execute(f"create table tm0 using m1 tags(0);")
        tdSql.execute(f"create table tm1 using m1 tags(1);")

        tdSql.execute(f"insert into tm0 values('2020-1-1 1:1:1', 1, 10);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:1:2', 1, 20);")
        tdSql.execute(f"insert into tm1 values('2020-2-1 1:1:1', 2, 10);")
        tdSql.execute(f"insert into tm1 values('2020-2-1 1:1:2', 2, 20);")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use group_db0;")

        tdLog.info(f"=========================>TD-4894")
        tdSql.query(f"select count(*),k from m1 group by k order by k;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 2)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.checkData(1, 1, 2)

        tdSql.query(f"select count(*) from m1 group by tbname,k,f1;")
        tdSql.query(f"select count(*) from m1 group by tbname,k,a;")
        tdSql.query(f"select count(*) from m1 group by k, tbname;")
        tdSql.query(f"select count(*) from m1 group by k,f1;")
        tdSql.query(f"select count(*) from tm0 group by tbname;")
        tdSql.query(f"select count(*) from tm0 group by a;")
        tdSql.query(f"select count(*) from tm0 group by k,f1;")
        tdSql.error(f"select count(*),f1 from m1 group by tbname,k;")

    def ParserTopGroupby(self):
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

        tdSql.query(f"select top(f1, 2) from tb1 group by f1;")
        tdSql.checkRows(5)

        tdSql.query(f"select bottom(f1, 2) from tb1 group by f1;")
        tdSql.checkRows(5)

        tdSql.query(f"select top(f1, 100) from tb1 group by f1;")
        tdSql.checkRows(8)

        tdSql.query(f"select bottom(f1, 100) from tb1 group by f1;")
        tdSql.checkRows(8)

    def QueryGroupby(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create table test(pos_time TIMESTAMP,target_id INT ,data DOUBLE) tags(scene_id BIGINT,data_stage VARCHAR(64),data_source VARCHAR(64));"
        )

        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232060000,413254290,1);"
        )
        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232061000,413254290,2);"
        )
        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232062000,413254290,3);"
        )
        tdSql.execute(
            f"insert into _413254000_108_1001_ using test tags(109,'1001','') values(1667232060000,413254290,3);"
        )
        tdSql.execute(
            f"insert into _413254000_108_1001_ using test tags(109,'1001','') values(1667232062000,413254290,3);"
        )

        tdSql.query(
            f"select target_name,max(time_diff) AS time_diff,(count(1)) AS track_count from (select tbname as target_name,diff(pos_time) time_diff from test where tbname in ('_413254290_108_1001_','_413254000_108_1001_') partition by tbname) a group by target_name;"
        )
        tdSql.checkRows(2)

    def QueryGroupbyDistinct(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")

        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:08', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:07', 1, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:06', 1, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 1, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 1, \"c\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:03', 1, \"c\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 1, \"d\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1, \"d\");")
        tdSql.query(f"select distinct avg(f1) as avgv from sta group by f2;")
        tdSql.checkRows(1)

        tdSql.query(f"select distinct avg(f1) as avgv from sta group by f2 limit 1,10;")
        tdSql.checkRows(0)

    def QueryComplexGroupby(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use db")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb1 tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 )")
        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+7a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+8a )'
        )

        tdLog.info(f"=============== insert data into child table ct2 (d)")
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 01:00:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 10:00:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 20:00:01.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 10:00:01.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 20:00:01.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 10:00:01.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+6a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 20:00:01.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+7a )'
        )

        tdLog.info(f"=============== insert data into child table ct3 (n)")
        tdSql.execute(
            f"insert into ct3 values ( '2021-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2021-12-31 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-07 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-31 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-28 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-08 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f'insert into ct4 values ( \'2020-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-31 01:01:36.000\', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )'
        )

        tdLog.info(f"================ start query ======================")

        tdLog.info(f"================ query 1 group by  filter")
        tdSql.query(f"select count(*) from ct3 group by c1")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c1")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c2")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c2")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c3")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c3")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c4")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c4")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c5")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c5")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c6")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c6")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c7")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c7")
        tdSql.checkRows(3)

        tdSql.query(f"select count(*) from ct3 group by c8")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c8")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c9")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c9")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c10")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c10")
        tdSql.checkRows(9)

        tdLog.info(f"================ query 2 complex with group by")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )

        tdSql.query(
            f"select abs(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select acos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select asin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select atan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select ceil(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select cos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select floor(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select log(c1,10) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select round(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select sin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select tan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")

        tdLog.info(f"================ query 1 group by  filter")
        tdSql.query(f"select count(*) from ct3 group by c1")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c1")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c2")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c2")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c3")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c3")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c4")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c4")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c5")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c5")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c6")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c6")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c7")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c7")
        tdSql.checkRows(3)

        tdSql.query(f"select count(*) from ct3 group by c8")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c8")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c9")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c9")
        tdSql.checkRows(9)

        tdSql.query(f"select count(*) from ct3 group by c10")
        tdLog.info(f"====> sql : select count(*) from ct3 group by c10")
        tdSql.checkRows(9)

        tdLog.info(f"================ query 2 complex with group by")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )

        tdSql.query(
            f"select abs(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select acos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select asin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select atan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select ceil(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select cos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select floor(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select log(c1,10) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select round(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select sin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )

        tdSql.query(
            f"select tan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
