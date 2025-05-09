from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestIntervalFunction:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval_function(self):
        """interval function

        1. -

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/function.sim

        """

        dbPrefix = "m_func_db"
        tbPrefix = "m_func_tb"
        mtPrefix = "m_func_mt"

        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 36500")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=====================================> td-4481")
        # tdSql.execute(f"create database {db}")

        tdLog.info(
            f"=====================================> test case for twa in single block"
        )

        tdSql.execute(f"create table t1 (ts timestamp, k float);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:00:00', 2.064);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:06:00', 2.116);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:12:00', 2.028);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:18:00', 2.126);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:24:00', 2.041);")
        tdSql.execute(f"insert into t1 values('2015-08-18 00:30:00', 2.051);")

        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:05:00'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.063999891)

        tdSql.checkData(0, 1, 2.063999891)

        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.089999914)

        tdSql.checkData(0, 1, 2.089999914)

        tdSql.checkData(0, 2, 2)

        tdSql.query(
            f"select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00' interval(1m)"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2015-08-18 00:00:00")

        tdSql.checkData(0, 1, 2.068333156)

        tdSql.checkData(0, 2, 2.063999891)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(1, 0, "2015-08-18 00:06:00")

        tdSql.checkData(1, 1, 2.115999937)

        tdSql.checkData(1, 2, 2.115999937)

        tdSql.checkData(1, 3, 1)

        tdSql.query(
            f"select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00' interval(1m)"
        )
        tdSql.checkRows(2)

        tdSql.checkData(1, 0, "2015-08-18 00:06:00")

        tdSql.checkData(1, 1, 2.115999937)

        tdSql.checkData(1, 2, 2.115999937)

        tdSql.checkData(1, 3, 1)

        tdSql.checkData(0, 1, 2.068333156)

        tdSql.query(
            f"select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:27:00' interval(10m)"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 2.088666666)

        tdSql.checkData(0, 2, 2.089999914)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(1, 1, 2.077099980)

        tdSql.checkData(1, 2, 2.077000022)

        tdSql.checkData(1, 3, 2)

        tdSql.checkData(2, 1, 2.069333235)

        tdSql.checkData(2, 2, 2.040999889)

        tdSql.checkData(2, 3, 1)

        tdSql.query(
            f"select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:27:00' interval(10m)"
        )
        tdSql.checkRows(3)

        tdSql.checkData(2, 1, 2.069333235)

        tdSql.checkData(1, 1, 2.077099980)

        tdSql.checkData(0, 1, 2.088666666)

        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:30:00'"
        )
        tdSql.checkData(0, 0, 2.073699975)

        tdSql.checkData(0, 1, 2.070999980)

        tdSql.checkData(0, 2, 6)

        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:30:00'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.073699975)

        tdSql.checkData(0, 1, 2.070999980)

        tdSql.checkData(0, 2, 6)

        tdSql.query(
            f"select twa(k) from t1 where ts>'2015-8-18 00:00:00' and ts<'2015-8-18 00:00:1'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:30:00' interval(10m)"
        )
        tdSql.query(
            f"select twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:30:00' interval(10m)"
        )

        # todo add test case while column filter exists for twa query

        # sql select count(*),TWA(k) from tm0 where ts>='1970-1-1 13:43:00' and ts<='1970-1-1 13:44:10' interval(9s)

        tdSql.execute(f"create table tm0 (ts timestamp, k float);")
        tdSql.execute(f"insert into tm0 values(100000000, 5);")
        tdSql.execute(f"insert into tm0 values(100003000, -9);")
        tdSql.query(f"select twa(k) from tm0 where ts<now")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -2.000000000)

        tdSql.execute(f"create table tm1 (ts timestamp,  k int);")
        tdSql.execute(f"insert into tm1 values('2020-10-30 18:11:56.680', -1000);")
        tdSql.execute(f"insert into tm1 values('2020-11-19 18:11:45.773', NULL);")
        tdSql.execute(f"insert into tm1 values('2020-12-09 18:11:17.098', NULL);")
        tdSql.execute(f"insert into tm1 values('2020-12-20 18:11:49.412', 1);")
        tdSql.execute(f"insert into tm1 values('2020-12-23 18:11:50.412', 2);")
        tdSql.execute(f"insert into tm1 values('2020-12-28 18:11:52.412', 3);")

        tdLog.info(f"=====================> td-2610")
        tdSql.query(
            f"select twa(k)from tm1 where ts>='2020-11-19 18:11:45.773' and ts<='2020-12-9 18:11:17.098'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdLog.info(f"=====================> td-2609")
        tdSql.query(
            f"select apercentile(k, 50) from tm1 where ts>='2020-10-30 18:11:56.680' and ts<='2020-12-09 18:11:17.098'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -1000.000000000)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")

        tdSql.execute(f"use m_func_db0")

        tdLog.info(f"=====================> td-2583")
        tdSql.query(
            f"select min(k) from tm1 where ts>='2020-11-19 18:11:45.773' and ts<='2020-12-20 18:11:49.412'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=====================> td-2601")
        tdSql.query(
            f"select count(*) from tm1 where ts<='2020-6-1 00:00:00' and ts>='2020-1-1 00:00:00' interval(1n) fill(NULL)"
        )
        tdSql.checkRows(0)

        tdLog.info(f"=====================> td-2615")
        tdSql.query(f"select last(ts) from tm1 interval(17a) limit 776 offset 3")
        tdSql.checkRows(3)

        tdSql.query(f"select last(ts) from tm1 interval(17a) limit 1000 offset 4")
        tdSql.checkRows(2)

        tdSql.query(
            f"select last(ts) from tm1 interval(17a) order by ts desc limit 1000 offset 0"
        )
        tdSql.checkRows(6)

        tdLog.info(f"=============================> TD-6086")
        tdSql.execute(
            f"create stable td6086st(ts timestamp, d double) tags(t nchar(50));"
        )
        tdSql.execute(f'create table td6086ct1 using td6086st tags("ct1");')
        tdSql.execute(f'create table td6086ct2 using td6086st tags("ct2");')

        tdSql.query(
            f"SELECT LAST(d),t FROM td6086st WHERE tbname in ('td6086ct1', 'td6086ct2') and ts>=\"2019-07-30 00:00:00\" and ts<=\"2021-08-31 00:00:00\" partition BY tbname interval(1800s) fill(prev);"
        )

        tdLog.info(f"==================> td-2624")
        tdSql.execute(f"create table tm2(ts timestamp, k int, b binary(12));")
        tdSql.execute(
            f"insert into tm2 values('2011-01-02 18:42:45.326',      -1,'abc');"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-07-30 17:44:06.283',       0, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-07-30 17:44:19.578', 9999999, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-07-30 17:46:06.417',    NULL, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-11-09 18:42:25.538',       0, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-12-29 17:43:11.641',       0, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-12-29 18:43:17.129',       0, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2020-12-29 18:46:19.109',    NULL, null);"
        )
        tdSql.execute(
            f"insert into tm2 values('2021-01-03 18:40:40.065',       0, null);"
        )

        tdSql.query(
            f"select _wstart, twa(k),first(ts) from tm2 where k <50 interval(17s);"
        )
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, "2011-01-02 18:42:42")

        tdSql.checkData(0, 2, "2011-01-02 18:42:45.326")

        tdSql.checkData(1, 0, "2020-07-30 17:43:59")

        tdSql.checkData(2, 1, 0.000000000)

        tdSql.query(f"select twa(k),first(ts) from tm2 where k <50 interval(17s);")
        tdSql.checkRows(6)

        tdSql.query(
            f"select _wstart, twa(k),first(ts),count(k),first(k) from tm2 interval(17s) limit 20 offset 0;"
        )
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, "2011-01-02 18:42:42")

        tdSql.checkData(1, 0, "2020-07-30 17:43:59")

        tdLog.info(f"=================>td-2610")
        tdSql.query(f"select stddev(k) from tm2 where ts='2020-12-29 18:46:19.109'")
        tdSql.checkRows(1)

        tdSql.query(f"select twa(k) from tm2 where ts='2020-12-29 18:46:19.109'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdLog.info(f"========================> TD-1787")
        tdSql.execute(f"create table cars(ts timestamp, c int) tags(id int);")
        tdSql.execute(f"create table car1 using cars tags(1);")
        tdSql.execute(f"create table car2 using cars tags(2);")
        tdSql.execute(
            f"insert into car1 (ts, c) values (now,1) car2(ts, c) values(now, 2);"
        )
        tdSql.execute(f"drop table cars;")
        tdSql.execute(f"create table cars(ts timestamp, c int) tags(id int);")
        tdSql.execute(f"create table car1 using cars tags(1);")
        tdSql.execute(f"create table car2 using cars tags(2);")
        tdSql.execute(
            f"insert into car1 (ts, c) values (now,1) car2(ts, c) values(now, 2);"
        )

        tdLog.info(f"========================> TD-2700")
        tdSql.execute(f"create table tx(ts timestamp, k int);")
        tdSql.execute(f"insert into tx values(1500000001000, 0);")
        tdSql.query(f"select sum(k) from tx interval(1d) sliding(1h);")
        tdSql.checkRows(24)

        tdLog.info(f"========================> TD-3948")
        tdSql.execute(f"drop table if exists meters")
        tdSql.execute(
            f"create stable meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int);"
        )
        tdSql.error(
            f'insert into td3948Err1(phase) using meters tags ("Beijng.Chaoyang", 2) (ts, current) values (now, 10.2);'
        )
        tdSql.error(
            f'insert into td3948Err2(phase, voltage) using meters tags ("Beijng.Chaoyang", 2) (ts, current) values (now, 10.2);'
        )
        tdSql.error(
            f'insert into td3948Err3(phase, current) using meters tags ("Beijng.Chaoyang", 2) (ts, current) values (now, 10.2);'
        )
        tdSql.execute(
            f'insert into td3948 using meters tags ("Beijng.Chaoyang", 2) (ts, current) values (now, 10.2);'
        )
        tdSql.query(f"select count(ts) from td3948;")
        tdSql.checkRows(1)

        tdLog.info(f"========================> TD-2740")
        tdSql.execute(f"drop table if exists m1;")
        tdSql.execute(f"create table m1(ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table tm10 using m1 tags(0);")
        tdSql.execute(f"create table tm11 using m1 tags(1);")
        tdSql.execute(f"create table tm12 using m1 tags(2);")
        tdSql.execute(f"create table tm13 using m1 tags(3);")
        tdSql.execute(f"insert into tm10 values('2020-1-1 1:1:1', 0);")
        tdSql.execute(f"insert into tm11 values('2020-1-5 1:1:1', 0);")
        tdSql.execute(f"insert into tm12 values('2020-1-7 1:1:1', 0);")
        tdSql.execute(f"insert into tm13 values('2020-1-1 1:1:1', 0);")
        tdSql.query(
            f"select count(*) from m1 where ts='2020-1-1 1:1:1' partition by tbname interval(1h)"
        )
        tdSql.checkRows(2)

        tdSql.execute(f"drop table m1;")
        tdSql.execute(f"drop table if exists tm1;")
        tdSql.execute(f"drop table if exists tm2;")
        tdSql.execute(
            f"create table m1(ts timestamp, k double, b double, c int, d smallint, e int unsigned) tags(a int);"
        )
        tdSql.execute(f"create table tm1 using m1 tags(1);")
        tdSql.execute(f"create table tm2 using m1 tags(2);")
        tdSql.execute(
            f"insert into tm1 values('2021-01-27 22:22:39.294', 1, 10, NULL, 110, 123) ('2021-01-27 22:22:40.294', 2, 20, NULL, 120, 124) ('2021-01-27 22:22:41.294', 3, 30, NULL, 130, 125)('2021-01-27 22:22:43.294', 4, 40, NULL, 140, 126)('2021-01-27 22:22:44.294', 5, 50, NULL, 150, 127);"
        )
        tdSql.execute(
            f"insert into tm2 values('2021-01-27 22:22:40.688', 5, 101, NULL, 210, 321) ('2021-01-27 22:22:41.688', 5, 102, NULL, 220, 322) ('2021-01-27 22:22:42.688', 5, 103, NULL, 230, 323)('2021-01-27 22:22:43.688', 5, 104, NULL, 240, 324)('2021-01-27 22:22:44.688', 5, 105, NULL, 250, 325)('2021-01-27 22:22:45.688', 5, 106, NULL, 260, 326);"
        )

        tdSql.query(f"select stddev(k) from m1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.378704626)

        tdSql.query(f"select stddev(c) from m1")
        tdSql.checkRows(1)

        tdSql.query(f"select stddev(k), stddev(c) from m1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.378704626)

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select stddev(b),stddev(b),stddev(k) from m1;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 37.840465463)

        tdSql.checkData(0, 1, 37.840465463)

        tdSql.checkData(0, 2, 1.378704626)

        tdSql.query(f"select stddev(k), stddev(b), a from m1 group by a order by a")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 14.142135624)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 0, 0.000000000)

        tdSql.checkData(1, 1, 1.707825128)

        tdSql.checkData(1, 2, 2)

        tdSql.query(f"select stddev(k), stddev(b), a from m1 where a= 1 group by a")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 14.142135624)

        tdSql.checkData(0, 2, 1)

        tdSql.query(
            f"select stddev(k), stddev(b), tbname from m1 group by tbname order by tbname"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 14.142135624)

        tdSql.checkData(0, 2, "tm1")

        tdSql.checkData(1, 0, 0.000000000)

        tdSql.checkData(1, 1, 1.707825128)

        tdSql.checkData(1, 2, "tm2")

        tdSql.query(f"select stddev(k), stddev(b) from m1 group by tbname,a")
        tdSql.checkRows(2)

        tdSql.query(
            f"select stddev(k), stddev(b), stddev(c),tbname, a from m1  group by tbname,a order by a asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 14.142135624)

        tdSql.checkData(0, 2, None)

        tdSql.checkData(0, 3, "tm1")

        tdSql.checkData(0, 4, 1)

        tdSql.checkData(1, 0, 0.000000000)

        tdSql.checkData(1, 1, 1.707825128)

        tdSql.checkData(1, 2, None)

        tdSql.checkData(1, 3, "tm2")

        tdSql.checkData(1, 4, 2)

        tdSql.query(
            f"select _wstart, stddev(k), stddev(b), stddev(c), tbname,a  from m1 partition by tbname, a interval(10s) order by tbname"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(0, 2, 0.000000000)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, "tm1")

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 1, 1.118033989)

        tdSql.checkData(1, 2, 11.180339887)

        tdSql.checkData(1, 3, None)

        tdSql.checkData(1, 4, "tm1")

        tdSql.checkData(2, 2, 1.707825128)

        tdSql.checkData(2, 3, None)

        tdSql.checkData(2, 4, "tm2")

        tdSql.checkData(2, 5, 2)

        tdSql.query(
            f"select _wstart, count(*), first(b), stddev(b), stddev(c), a from m1  partition by a interval(10s) order by a"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-01-27 22:22:30")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 10.000000000)

        tdSql.checkData(0, 3, 0.000000000)

        tdSql.checkData(0, 4, None)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 2, 20.000000000)

        tdSql.checkData(1, 3, 11.180339887)

        tdSql.checkData(1, 4, None)

        tdSql.checkData(2, 3, 1.707825128)

        tdSql.query(
            f"select _wstart, count(*), first(b), stddev(b), stddev(c), tbname, a from m1 partition by tbname, a interval(10s) order by tbname"
        )
        tdSql.checkRows(3)

        tdSql.checkData(2, 3, 1.707825128)

        tdSql.checkData(2, 5, "tm2")

        tdSql.query(
            f"select _wstart, count(*), stddev(b), stddev(b)+20, stddev(c), tbname, a from m1 partition by tbname, a interval(10s) order by tbname"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 2, 0.000000000)

        tdSql.checkData(0, 3, 20.000000000)

        tdSql.checkData(1, 3, 31.180339887)

        tdSql.checkData(1, 4, None)

        tdSql.query(
            f"select _wstart, count(*), first(b), stddev(b)+first(b), stddev(c), tbname, a from m1 partition by tbname, a interval(10s) order by tbname"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 2, 10.000000000)

        tdSql.checkData(0, 3, 10.000000000)

        tdSql.checkData(1, 2, 20.000000000)

        tdSql.checkData(1, 3, 31.180339887)

        tdSql.checkData(2, 2, 101.000000000)

        tdSql.checkData(2, 3, 102.707825128)

        tdSql.query(f"select stddev(e), stddev(k) from m1 where a=1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 1.414213562)

        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 int) tags (id int);"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values ('2021-07-02 00:00:00', 1, 1);")

        tdSql.query(f"select stddev(f1) from st1 group by f1;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select count(tbname) from st1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(id) from st1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdLog.info(f"====================> TODO stddev + normal column filter")

        tdLog.info(f"====================> irate")
        tdSql.query(f"select irate(f1) from st1;")
        tdSql.query(f"select irate(f1) from  st1 group by tbname;")

        tdSql.query(f"select irate(k) from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000027778)

        tdSql.query(f"select irate(k) from t1 where ts>='2015-8-18 00:30:00.000'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select irate(k) from t1 where ts>='2015-8-18 00:06:00.000' and ts<='2015-8-18 00:12:00.000';"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.005633334)

        tdSql.query(f"select _wstart, irate(k) from t1 interval(10a)")
        tdSql.checkRows(6)

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(1, 1, 0.000000000)

        tdSql.checkData(5, 1, 0.000000000)

        tdSql.query(f"select _wstart, count(*), irate(k) from t1 interval(10m)")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2015-08-18 00:00:00")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 0.000144445)

        tdSql.checkData(1, 0, "2015-08-18 00:10:00")

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(1, 2, 0.000272222)

        tdSql.checkData(2, 0, "2015-08-18 00:20:00")

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 0.000000000)

        tdSql.checkData(3, 0, "2015-08-18 00:30:00")

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 0.000000000)

        tdSql.query(
            f"select _wstart, count(*),irate(k) from t1 interval(10m) order by _wstart desc"
        )
        tdSql.checkRows(4)

        tdSql.checkData(3, 0, "2015-08-18 00:00:00")

        tdSql.checkData(3, 1, 2)

        tdSql.checkData(3, 2, 0.000144445)

        tdLog.info(f"===========================> derivative")
        tdSql.execute(f"drop table t1")
        tdSql.execute(f"drop table tx;")
        tdSql.execute(f"drop table if exists m1;")
        tdSql.execute(f"drop table if exists tm0;")
        tdSql.execute(f"drop table if exists tm1;")

        tdSql.execute(f"create table tm0(ts timestamp, k double)")
        tdSql.execute(
            f"insert into tm0 values('2015-08-18T00:00:00Z', 2.064) ('2015-08-18T00:06:00Z', 2.116) ('2015-08-18T00:12:00Z', 2.028)"
        )
        tdSql.execute(
            f"insert into tm0 values('2015-08-18T00:18:00Z', 2.126) ('2015-08-18T00:24:00Z', 2.041) ('2015-08-18T00:30:00Z', 2.051)"
        )

        tdSql.error(f"select derivative(ts) from tm0;")
        tdSql.error(f"select derivative(k) from tm0;")
        tdSql.error(f"select derivative(k, 0, 0) from tm0;")
        tdSql.error(f"select derivative(k, 1, 911) from tm0;")
        tdSql.error(f"select derivative(kx, 1s, 1) from tm0;")
        tdSql.error(f"select derivative(k, -20s, 1) from tm0;")
        tdSql.query(f"select derivative(k, 20a, 0) from tm0;")
        tdSql.query(f"select derivative(k, 200a, 0) from tm0;")
        tdSql.query(f"select derivative(k, 999a, 0) from tm0;")
        tdSql.error(f"select derivative(k, 20s, -12) from tm0;")

        tdSql.query(f"select ts, derivative(k, 1s, 0) from tm0")
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2015-08-18 08:06:00")

        tdSql.checkData(0, 1, 0.000144444)

        tdSql.checkData(1, 0, "2015-08-18 08:12:00")

        tdSql.checkData(1, 1, -0.000244444)

        tdSql.checkData(2, 0, "2015-08-18 08:18:00")

        tdSql.checkData(2, 1, 0.000272222)

        tdSql.checkData(3, 0, "2015-08-18 08:24:00")

        tdSql.checkData(3, 1, -0.000236111)

        tdSql.query(f"select ts ,derivative(k, 6m, 0) from tm0;")
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2015-08-18 08:06:00")

        tdSql.checkData(0, 1, 0.052000000)

        tdSql.checkData(1, 0, "2015-08-18 08:12:00")

        tdSql.checkData(1, 1, -0.088000000)

        tdSql.checkData(2, 0, "2015-08-18 08:18:00")

        tdSql.checkData(2, 1, 0.098000000)

        tdSql.checkData(3, 0, "2015-08-18 08:24:00")

        tdSql.checkData(3, 1, -0.085000000)

        tdSql.query(f"select ts, derivative(k, 12m, 0) from tm0;")
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2015-08-18 08:06:00")

        tdSql.checkData(0, 1, 0.104000000)

        tdSql.query(f"select derivative(k, 6m, 1) from tm0;")
        tdSql.checkRows(3)

        tdSql.error(f"select derivative(k, 6m, 1) from tm0 interval(1s);")
        tdSql.error(f"select derivative(k, 6m, 1) from tm0 session(ts, 1s);")
        tdSql.error(f"select derivative(k, 6m, 1) from tm0 group by k;")

        tdSql.execute(f"drop table if exists tm0")
        tdSql.execute(f"drop table if exists m1")

        tdSql.execute(f"create table m1 (ts timestamp, k double ) tags(a int);")
        tdSql.execute(f"create table if not exists t0 using m1 tags(1);")
        tdSql.execute(f"create table if not exists t1 using m1 tags(2);")

        tdSql.execute(f"insert into t0 values('2020-1-1 1:1:1', 1);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:1:3', 3);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:2:4', 4);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:2:5', 5);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:2:6', 6);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:3:7', 7);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:3:8', 8);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:3:9', 9);")
        tdSql.execute(f"insert into t0 values('2020-1-1 1:4:10', 10);")

        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:2', 2);")
        tdLog.info(f"===========================>td-4739")
        tdSql.query(
            f"select diff(val) from (select ts, derivative(k, 1s, 0) val from t1);"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:4', 20);")
        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:6', 200);")
        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:8', 2000);")
        tdSql.execute(f"insert into t1 values('2020-1-1 1:1:10', 20000);")

        tdSql.query(f"select derivative(k, 1s, 0) from m1;")
        tdSql.error(f"select derivative(k, 1s, 0) from m1 group by a;")
        tdSql.error(f"select derivative(f1, 1s, 0) from (select k from t1);")

        tdSql.query(f"select ts, derivative(k, 1s, 0) from m1")
        tdSql.checkRows(13)

        tdLog.info(f"=========================>TD-5190")
        tdSql.query(
            f"select _wstart, stddev(f1) from st1 where ts>'2021-07-01 1:1:1' and ts<'2021-07-30 00:00:00' interval(1d) fill(NULL);"
        )
        tdSql.checkRows(29)

        tdSql.checkData(0, 0, "2021-07-01 00:00:00")

        tdSql.checkData(0, 1, None)

        tdSql.query(
            f"select derivative(test_column_alias_name, 1s, 0) from (select _wstart, avg(k) test_column_alias_name from t1 interval(1s));"
        )

        tdSql.execute(
            f"create table smeters (ts timestamp, current float, voltage int) tags (t1 int);"
        )
        tdSql.execute(f"create table smeter1 using smeters tags (1);")
        tdSql.execute(f"insert into smeter1 values ('2021-08-08 10:10:10', 10, 2);")
        tdSql.execute(f"insert into smeter1 values ('2021-08-08 10:10:12', 10, 2);")
        tdSql.execute(f"insert into smeter1 values ('2021-08-08 10:10:14', 20, 1);")

        tdSql.query(
            f"select _wstart, stddev(voltage) from smeters where ts>='2021-08-08 10:10:10.000' and ts < '2021-08-08 10:10:20.000'  and current=10 interval(1000a);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-08-08 10:10:10")

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(1, 0, "2021-08-08 10:10:12")

        tdSql.checkData(1, 1, 0.000000000)

        tdSql.query(
            f"select stddev(voltage) from smeters where ts>='2021-08-08 10:10:10.000' and ts < '2021-08-08 10:10:20.000'  and current=10;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.execute(
            f"create table ft1(ts timestamp, a int, b int , c int, d double);"
        )

        tdSql.execute(f"insert into ft1 values(1648791213000,1,2,3,1.0);")
        tdSql.error(f"select sum(_wduration), a from ft1 state_window(a);")

        tdSql.error(f"select count(_wduration), a from ft1 state_window(a);")

        tdSql.error(f"select max(_wduration), a from ft1 state_window(a);")

        tdSql.error(f"select sum(1 + _wduration), a from ft1 state_window(a);")

        tdSql.error(f"select sum(cast(_wstart as bigint)), a from ft1 state_window(a);")

        tdSql.error(f"select sum(cast(_wend as bigint)), a from ft1 state_window(a);")

        tdSql.error(
            f"create stream streams1 trigger at_once  into streamt as select  _wstart, sum(_wduration) from ft1 interval(10s);"
        )

        tdSql.error(
            f"create stream streams1 trigger at_once  into streamt as select  _wstart, sum(cast(_wend as bigint)) from ft1 interval(10s);"
        )

        tdSql.execute(f"create database test  vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000,1,1,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223000,1,2,NULL,NULL);")

        tdSql.query(
            f'select apercentile(c, 50), apercentile(d, 50, "t-digest")  from t1;'
        )

        tdSql.checkData(0, 0, 3.000000000)

        tdSql.checkData(0, 1, 1.000000000)

        tdSql.query(
            f'select apercentile(c, 50) a, apercentile(d, 50, "t-digest")  from t1 partition by b session(ts, 5s) order by a desc;'
        )

        tdSql.checkData(0, 0, 3.000000000)

        tdSql.checkData(0, 1, 1.000000000)

        tdSql.checkData(1, 0, None)

        tdSql.checkData(1, 1, None)

        tdSql.query(
            f'select apercentile(c, 50) a, apercentile(d, 50, "t-digest")  from t1 state_window(b) order by a desc;'
        )

        tdSql.checkData(0, 0, 3.000000000)

        tdSql.checkData(0, 1, 1.000000000)

        tdSql.checkData(1, 0, None)

        tdSql.checkData(1, 1, None)
