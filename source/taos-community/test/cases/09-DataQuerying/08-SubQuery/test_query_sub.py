from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestQuerySub:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_query_sub(self):
        """Subquery basic

        1. Perform projection queries on subquery results
        2. Perform aggregate queries on subquery results
        3. Perform window queries on subquery results
        4. Perform DIFF function queries on subquery results

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/nestquery.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/timeline.sim

        """

        self.NestQuery()
        tdStream.dropAllStreamsAndDbs()
        self.TimeLine()
        tdStream.dropAllStreamsAndDbs()

    def NestQuery(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "nest_db"
        tbPrefix = "nest_tb"
        mtPrefix = "nest_mt"
        tbNum = 3
        rowNum = 10000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== nestquery.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database if not exists {db}")

        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)"
        )

        half = 2

        i = 0
        while i < half:
            tb = tbPrefix + str(i)

            nextSuffix = i + half
            tb1 = tbPrefix + str(nextSuffix)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {nextSuffix} )")

            x = 0
            while x < rowNum:
                y = x * 60000
                ms = 1600099200000 + y
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )  {tb1} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        i = 1
        tb = tbPrefix + str(i)

        tdLog.info(f"==============> simple nest query test")
        tdSql.query(f"select count(*) from (select count(*) from nest_mt0)")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select count(*) from (select count(*) from nest_mt0 group by tbname)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select count(*) from (select count(*) from nest_mt0 partition by tbname interval(10h) )"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 68)

        tdSql.query(
            f"select sum(a) from (select count(*) a from nest_mt0 partition by tbname interval(10h))"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 40000)

        tdLog.info(f"=================> alias name test")
        tdSql.query(
            f"select ts from (select _wstart as ts, count(*) a from nest_tb0 interval(1h))"
        )
        tdSql.checkRows(167)

        tdSql.checkData(0, 0, "2020-09-15 00:00:00")

        tdSql.query(
            f"select count(a) from (select count(*) a from nest_tb0 interval(1h))"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 167)

        tdLog.info(f"================>master query + filter")
        tdSql.query(
            f"select t.* from (select count(*) a from nest_tb0 interval(10h)) t where t.a <= 520;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from (select count(*) a, tbname f1 from nest_mt0 group by tbname) t where t.a<0 and f1 = 'nest_tb0';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from (select count(*) a, tbname f1, tbname from nest_mt0 group by tbname) t where t.a>0 and f1 = 'nest_tb0';"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10000)

        tdSql.checkData(0, 1, "nest_tb0")

        tdSql.checkData(0, 2, "nest_tb0")

        tdLog.info(f"===================> nest query interval")
        tdSql.error(f"select ts, avg(c1) from (select ts, c1 from nest_tb0);")

        tdSql.query(
            f"select _wstart, avg(c1) from (select * from nest_tb0) interval(3d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)}"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2020-09-14 00:00:00")

        tdSql.checkData(0, 1, 49.222222222)

        tdSql.checkData(1, 0, "2020-09-17 00:00:00")

        tdSql.checkData(1, 1, 49.685185185)

        tdSql.checkData(2, 0, "2020-09-20 00:00:00")

        tdSql.checkData(2, 1, 49.500000000)

        tdSql.query(f"select stddev(c1) from (select c1 from nest_tb0);")
        tdSql.error(f"select percentile(c1, 20) from (select * from nest_tb0);")
        # sql select interp(c1) from (select * from nest_tb0);
        tdSql.error(
            f"select derivative(val, 1s, 0) from (select c1 val from nest_tb0);"
        )
        tdSql.error(f"select twa(c1) from (select c1 from nest_tb0);")
        tdSql.error(f"select irate(c1) from (select c1 from nest_tb0);")
        tdSql.error(f"select diff(c1), twa(c1) from (select * from nest_tb0);")
        # sql_error select irate(c1), interp(c1), twa(c1) from (select * from nest_tb0);

        tdSql.query(
            f"select _wstart, apercentile(c1, 50) from (select * from nest_tb0) interval(1d)"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, "2020-09-15 00:00:00")

        tdSql.checkData(0, 1, 47.571428571)

        tdSql.checkData(1, 0, "2020-09-16 00:00:00")

        tdSql.checkData(1, 1, 49.666666667)

        tdSql.checkData(2, 0, "2020-09-17 00:00:00")

        tdSql.checkData(2, 1, 49.000000000)

        tdSql.checkData(3, 0, "2020-09-18 00:00:00")

        tdSql.checkData(3, 1, 48.333333333)

        tdSql.query(f"select twa(c1) from (select * from nest_tb0);")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 49.500000000)

        tdSql.query(f"select leastsquares(c1, 1, 1) from (select * from nest_tb0);")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "{slop:0.000100, intercept:49.000000}")

        tdSql.query(f"select irate(c1) from (select * from nest_tb0);")
        tdSql.checkData(0, 0, 0.016666667)

        tdSql.query(f"select derivative(c1, 1s, 0) from (select * from nest_tb0);")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(9999)

        tdSql.checkData(0, 0, 0.016666667)

        tdSql.checkData(1, 0, 0.016666667)

        tdSql.query(f"select diff(c1) from (select * from nest_tb0);")
        tdSql.checkRows(9999)

        tdSql.query(
            f"select _wstart, avg(c1),sum(c2), max(c3), min(c4), count(*), first(c7), last(c7),spread(c6) from (select * from nest_tb0) interval(1d);"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, "2020-09-15 00:00:00")

        tdSql.checkData(0, 1, 48.666666667)

        tdSql.checkData(0, 2, 70080.000000000)

        tdSql.checkData(0, 3, 99)

        tdSql.checkData(0, 4, 0)

        tdSql.checkData(0, 5, 1440)

        tdSql.checkData(0, 6, 0)

        tdSql.checkData(0, 7, 1)

        tdSql.checkData(0, 8, 99.000000000)

        tdSql.checkData(1, 0, "2020-09-16 00:00:00")

        tdSql.checkData(1, 1, 49.777777778)

        tdSql.checkData(1, 2, 71680.000000000)

        tdSql.query(f"select top(x, 20) from (select c1 x from nest_tb0);")

        tdSql.query(f"select bottom(x, 20) from (select c1 x from nest_tb0)")

        tdLog.info(f"===================> group by + having")

        tdLog.info(f"=========================> ascending order/descending order")

        tdLog.info(f"=========================> nest query join")
        tdSql.query(
            f"select a.ts,a.k,b.ts from (select _wstart ts, count(*) k from nest_tb0 interval(30a)) a, (select _wstart ts, count(*) f from nest_tb1 interval(30a)) b where a.ts = b.ts ;"
        )
        tdSql.checkRows(10000)

        tdSql.checkData(0, 0, "2020-09-15 00:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, "2020-09-15 00:00:00")

        tdSql.checkData(1, 0, "2020-09-15 00:01:00")

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, "2020-09-15 00:01:00")

        tdSql.query(
            f"select sum(a.k), sum(b.f) from (select _wstart ts, count(*) k from nest_tb0 interval(30a)) a, (select _wstart ts, count(*) f from nest_tb1 interval(30a)) b where a.ts = b.ts ;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10000)

        tdSql.checkData(0, 1, 10000)

        tdSql.query(
            f"select a.ts,a.k,b.ts,c.ts,c.ts,c.x from (select _wstart ts, count(*) k from nest_tb0 interval(30a)) a, (select _wstart ts, count(*) f from nest_tb1 interval(30a)) b, (select _wstart ts, count(*) x from nest_tb2 interval(30a)) c where a.ts = b.ts and a.ts = c.ts"
        )
        tdSql.checkRows(10000)

        tdSql.checkData(0, 0, "2020-09-15 00:00:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, "2020-09-15 00:00:00")

        tdSql.checkData(0, 3, "2020-09-15 00:00:00")

        # sql select diff(val) from (select c1 val from nest_tb0);
        # if $rows != 9999 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0, 1 then
        #  return -1
        # endi

        tdSql.error(f"select last_row(*) from (select * from nest_tb0) having c1 > 0")

        tdLog.info(f"===========>td-4805")
        tdSql.error(f"select tbname, i from (select * from nest_tb0) group by i;")

        tdSql.query(
            f"select count(*),c1 from (select * from nest_tb0) where c1 < 2 group by c1 order by c1;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 100)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, 100)

        tdSql.checkData(1, 1, 1)

        tdLog.info(f"=====================>TD-5157")
        tdSql.query(f"select _wstart, twa(c1) from nest_tb1 interval(19a);")
        tdSql.checkRows(10000)

        tdSql.checkData(0, 0, "2020-09-14 23:59:59.992")

        tdSql.checkData(0, 1, 0.000083333)

        tdLog.info(f"======================>TD-5271")
        tdSql.error(
            f"select min(val),max(val),first(val),last(val),count(val),sum(val),avg(val) from (select count(*) val from nest_mt0 group by tbname)"
        )

        tdLog.info(f"=================>us database interval query, TD-5039")
        tdSql.execute(f"create database test precision 'us';")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, k int);")
        tdSql.execute(
            f"insert into t1 values('2020-01-01 01:01:01.000', 1) ('2020-01-01 01:02:00.000', 2);"
        )
        tdSql.query(
            f"select _wstart, avg(k) from (select _wstart, avg(k) k from t1 interval(1s)) interval(1m);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-01-01 01:01:00")

        tdSql.checkData(0, 1, 1.000000000)

        tdSql.checkData(1, 0, "2020-01-01 01:02:00")

        tdSql.checkData(1, 1, 2.000000000)

    def TimeLine(self):
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"CREATE STABLE `demo` (`_ts` TIMESTAMP, `faev` DOUBLE) TAGS (`deviceid` VARCHAR(256));"
        )
        tdSql.execute(f"CREATE TABLE demo_1 USING demo (deviceid) TAGS ('1');")
        tdSql.execute(f"CREATE TABLE demo_2 USING demo (deviceid) TAGS ('2');")
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-11-30 00:00:00.000', 1.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-04 01:00:00.001', 2.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-04 02:00:00.002', 3.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-05 03:00:00.003', 4.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-11-30 00:00:00.000', 5.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-28 01:00:00.001', 6.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-28 02:00:00.002', 7.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-29 03:00:00.003', 8.0);"
        )

        tdSql.error(
            f"select diff(faev) from ((select ts, faev from demo union all select ts, faev from demo));"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts, faev from demo order by faev, _ts);"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts + 1s, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by deviceid, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev;"
        )

        tdSql.error(f"select diff(faev) from (select _ts, faev from demo);")
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo order by faev, _ts);"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by faev, _ts) partition by deviceid;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by deviceid, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM (SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )
        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM (SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )
