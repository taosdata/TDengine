from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSliding:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sliding(self):
        """Sliding

        1. -

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/sliding.sim

        """

        dbPrefix = "sliding_db"
        tbPrefix = "sliding_tb"
        mtPrefix = "sliding_mt"
        tbNum = 8
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== sliding.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 946656000000

        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database if not exists {db}  keep 36500")
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
                ms = str(x) + "m"
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                next = tstart + 30
                f = x + 1
                c1 = f % 100

                binary1 = "'binary" + str(c1) + "'"
                nchar1 = "'nchar" + str(c1) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} ) ({next} , {c1} , {c1} , {c1} , {c1} , {c1} , {c1} , {c1} , {binary1} , {nchar1} )"
                )
                tstart = tstart + 60
                x = x + 2

            i = i + 1
            tstart = 946656000000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "sliding_db"
        tbPrefix = "sliding_tb"
        mtPrefix = "sliding_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)

        tdLog.info(f"===============================interval_sliding query")
        tdSql.query(
            f"select _wstart, count(*) from sliding_tb0 interval(3s) sliding(3s);"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2000-01-01 00:00:00")

        tdSql.checkData(0, 1, 100)

        tdSql.checkData(1, 0, "2000-01-01 00:00:03")

        tdSql.checkData(1, 1, 100)

        tdSql.query(
            f"select _wstart, stddev(c1) from sliding_tb0 interval(10a) sliding(10a);"
        )
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, "2000-01-01 00:00:00")

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(9, 0, "2000-01-01 00:00:00.270")

        tdSql.checkData(9, 1, 0.000000000)

        tdSql.query(
            f"select _wstart, stddev(c1),count(c2),first(c3),last(c4) from sliding_tb0 interval(10a) sliding(10a) order by _wstart desc;"
        )
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, "2000-01-01 00:00:29.970")

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 99)

        tdSql.checkData(0, 4, 99)

        tdSql.checkData(9, 0, "2000-01-01 00:00:29.700")

        tdSql.checkData(9, 1, 0.000000000)

        tdSql.checkData(9, 2, 1)

        tdSql.checkData(9, 3, 90)

        tdSql.checkData(9, 4, 90)

        tdSql.query(
            f"select _wstart, count(c2),last(c4) from sliding_tb0 interval(3s) sliding(1s) order by _wstart asc;"
        )
        tdSql.checkRows(32)

        tdSql.checkData(0, 0, "1999-12-31 23:59:58")

        tdSql.checkData(0, 1, 34)

        tdSql.checkData(0, 2, 33)

        tdSql.query(
            f"select _wstart, count(c2),stddev(c3),first(c4),last(c4) from sliding_tb0 where ts>'2000-01-01 0:0:0' and ts<'2000-1-1 0:0:4' interval(3s) sliding(3s) order by _wstart asc;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 4, 99)

        tdSql.checkData(0, 1, 99)

        tdSql.checkData(0, 2, 28.577380332)

        # interval offset + limit
        tdSql.query(
            f"select _wstart, count(c2), first(c3),stddev(c4) from sliding_tb0 interval(10a) sliding(10a) order by _wstart desc limit 10 offset 90;"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2000-01-01 00:00:27.270")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 9)

        tdSql.checkData(0, 3, 0.000000000)

        tdSql.checkData(9, 0, "2000-01-01 00:00:27")

        tdSql.checkData(9, 1, 1)

        tdSql.checkData(9, 2, 0)

        tdSql.checkData(9, 3, 0.000000000)

        # interval offset test
        tdSql.query(
            f"select _wstart, count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(3s) order by _wstart asc limit 100 offset 1;"
        )
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, "2000-01-01 00:00:03")

        tdSql.checkData(0, 1, 100)

        tdSql.checkData(0, 2, 99)

        tdSql.checkData(8, 0, "2000-01-01 00:00:27")

        tdSql.checkData(8, 1, 100)

        tdSql.query(
            f"select _wstart, count(c2),last(c4),stddev(c3) from sliding_tb0 where ts>'2000-1-1 0:0:0' and ts<'2000-1-1 0:0:4' interval(3s) sliding(3s) order by _wstart asc limit 100 offset 0;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2000-01-01 00:00:00")

        tdSql.checkData(0, 1, 99)

        tdSql.checkData(0, 2, 99)

        tdSql.checkData(0, 3, 28.577380332)

        tdSql.checkData(1, 0, "2000-01-01 00:00:03")

        tdSql.checkData(1, 1, 34)

        tdSql.checkData(1, 2, 33)

        tdSql.checkData(1, 3, 9.810708435)

        tdSql.query(
            f"select _wstart, count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 1;"
        )
        tdSql.checkRows(15)

        tdSql.checkData(0, 0, "2000-01-01 00:00:00")

        tdSql.checkData(0, 1, 100)

        tdSql.checkData(0, 2, 99)

        tdSql.checkData(0, 3, 28.866070048)

        tdSql.checkData(9, 0, "2000-01-01 00:00:18")

        tdSql.checkData(9, 1, 100)

        tdSql.checkData(9, 2, 99)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 5;"
        )
        tdSql.checkRows(11)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 6;"
        )
        tdSql.checkRows(10)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 7;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 8;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 9;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 10;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 11;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 12;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 13;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 14;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 15;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3) from sliding_tb0 interval(3s) sliding(2s) order by _wstart asc limit 100 offset 16;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select _wstart, count(c2),last(c4),stddev(c3),spread(c3) from sliding_tb0 where c2 = 0 interval(3s) order by _wstart desc;"
        )
        tdSql.checkRows(10)

        # 00-01-01 00:00:27.000 |                     1 |        0 |               0.000000000 |               0.000000000 |
        tdSql.checkData(0, 0, "2000-01-01 00:00:27")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 0.000000000)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3),spread(c3) from sliding_tb0 where c2 = 0 interval(3s) sliding(2s) order by _wstart desc limit 1 offset 14;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(c2),last(c4),stddev(c3),spread(c3) from sliding_tb0 where c2 = 0 interval(3s) sliding(2s) order by _wstart desc limit 1 offset 15;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select _wstart, count(c2), first(c3),stddev(c4) from sliding_tb0 interval(10a) order by _wstart desc limit 10 offset 2;"
        )
        tdSql.checkData(0, 0, "2000-01-01 00:00:29.910")

        tdSql.query(
            f"select _wstart, count(*),stddev(c1),count(c1),first(c2),last(c3) from sliding_tb0 where ts>'2000-1-1 00:00:00' and ts<'2000-1-1 00:00:01.002' and c2 >= 0 interval(30s) sliding(10s) order by _wstart asc limit 1000;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "1999-12-31 23:59:40")

        tdSql.checkData(0, 2, 9.521904571)

        tdSql.checkData(0, 5, 33)

        tdSql.checkData(1, 0, "1999-12-31 23:59:50")

        tdSql.checkData(1, 2, 9.521904571)

        tdSql.checkData(1, 5, 33)

        tdSql.checkData(2, 5, 33)

        tdSql.query(
            f"select _wstart, count(*),stddev(c1),count(c1),first(c2),last(c3) from sliding_tb0 where ts>'2000-1-1 00:00:00' and ts<'2000-1-1 00:00:01.002' and c2 >= 0 interval(30s) sliding(10s) order by _wstart desc limit 1000;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2000-01-01 00:00:00")

        tdSql.checkData(0, 1, 33)

        tdSql.checkData(0, 2, 9.521904571)

        tdSql.checkData(0, 3, 33)

        tdSql.checkData(1, 0, "1999-12-31 23:59:50")

        tdSql.checkData(1, 1, 33)

        tdSql.checkData(1, 2, 9.521904571)

        tdSql.checkData(2, 0, "1999-12-31 23:59:40")

        tdSql.checkData(2, 1, 33)

        tdSql.checkData(2, 2, 9.521904571)

        tdLog.info(f"====================>check boundary check crash at client side")
        tdSql.query(f"select count(*) from sliding_mt0 where ts>now and ts < now-1h;")

        tdSql.query(f"select sum(c1) from sliding_tb0 interval(1a) sliding(1a);")

        tdLog.info(f"========================query on super table")

        tdLog.info(f"========================error case")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(10a) sliding(12a);")
        tdSql.error(f"select sum(c1) from sliding_tb0 sliding(1n) interval(1y);")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(-1y)  sliding(1n);")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(1y)  sliding(-1n);")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(0)  sliding(0);")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(0m)  sliding(0m);")
        tdSql.error(f"select sum(c1) from sliding_tb0 interval(m)  sliding(m);")
        tdSql.error(f"select sum(c1) from sliding_tb0 sliding(4m);")
        tdSql.error(f"select count(*) from sliding_tb0 interval(1s) sliding(10s);")
        tdSql.error(f"select count(*) from sliding_tb0 interval(10s) sliding(10a);")
