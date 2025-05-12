from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestGroupBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby(self):
        """Group By

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/groupby.sim

        """

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

        tdSql.checkData(0, 0, '{slop:0.000000, intercept:0.000000}')

        tdSql.checkData(1, 0, '{slop:0.000000, intercept:1.000000}')

        tdSql.checkData(9, 0, '{slop:0.000000, intercept:17.000000}')

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
