from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLimit1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit1(self):
        """Limit 1

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/limit1.sim

        """

        # ========================================= setup environment ================================

        dbPrefix = "lm1_db"
        tbPrefix = "lm1_tb"
        stbPrefix = "lm1_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit1.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = str(int(i + halfNum))
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + tbId
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"====== tables created")

        self.limit1_tb()
        self.limit1_stb()

        tdSql.execute(f"flush database {db}")
        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.limit1_tb()
        self.limit1_stb()

    def limit1_tb(self):
        dbPrefix = "lm1_db"
        tbPrefix = "lm1_tb"
        stbPrefix = "lm1_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit1_tb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"====== use {db}")
        tdSql.execute(f"use {db}")

        ##### select from table
        tdLog.info(f"====== select from table with limit offset")
        tb = tbPrefix + "0"
        tdSql.query(f"select * from {tb} order by ts desc limit 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(4, 1, 5)

        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(4, 1, 0)

        offset = rowNum - 1
        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset {offset}")
        tdSql.checkRows(1)

        tdSql.query(
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {tb} order by ts desc limit 5 offset 5"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 4.00000)
        tdSql.checkData(0, 4, 4.000000000)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 4)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, "binary4")
        tdSql.checkData(0, 9, "nchar4")

        tdSql.query(
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {tb} order by ts desc limit 5 offset {offset}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, 0.00000)
        tdSql.checkData(0, 4, 0.000000000)
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, "binary0")
        tdSql.checkData(0, 9, "nchar0")

        ## TBASE-329
        tdSql.query(
            f"select * from {tb} where c1 < 9 order by ts desc limit 1 offset 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 7)

        tdSql.query(
            f"select * from {tb} where c1 < 9 order by ts desc limit 2 offset 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 7)
        tdSql.checkData(1, 1, 6)

        tdSql.query(
            f"select * from {tb} where c1 < 9 order by ts desc limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 7)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 5)

        tdSql.query(
            f"select * from {tb} where c1 < 0 order by ts desc limit 3 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where c1 > 0 order by ts desc limit 1 offset 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 8)

        tdSql.query(
            f"select * from {tb} where c1 > 0 order by ts desc limit 2 offset 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(1, 1, 7)

        tdSql.error(f"select * from {tb} limit")
        tdSql.error(f"select * from {tb} offset")
        tdSql.error(f"select * from {tb} offset 1")
        tdSql.error(f"select * from {tb} offset 1 limit 5")
        tdSql.error(f"select * from {tb} offset 1 limit -1")

        #### aggregation function + limit offset
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        ### aggregation function + limit offset (non-interval case)
        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 < 9 and c2 < 8 and c3 >0 and c4 <= 7 and c5 <7 limit 5 offset 0"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5), min(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5), min(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 0 offset 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5), min(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5"
        )
        tdSql.checkRows(1)

        val = 45 * rowNum
        val = val / 10        
        tdSql.checkData(0, 0, val)
        tdSql.checkData(0, 1, 4.500000000)
        tdSql.checkData(0, 2, 2.872281323)
        tdSql.checkData(0, 3, 9.000000000)
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, rowNum)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, "binary9")
        tdSql.checkData(0, 8, "nchar9")

        tdSql.query(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} limit 0"
        )
        tdSql.checkRows(0)

        tdLog.info(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1>1 and c2<9 and c3>2 and c4<8 and c5>4 and c6<6 limit 1 offset 0"
        )

        tdSql.query(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1>1 and c2<9 and c3>2 and c4<8 and c5>4 and c6<6 limit 1 offset 0"
        )
        tdSql.checkRows(1)

        val = int(rowNum / 10)
        val = val * 5        
        tdSql.checkData(0, 0, val)
        tdSql.checkData(0, 1, 5.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 5.000000000)
        tdSql.checkData(0, 4, 5)

        val = int(rowNum / 10)        
        tdSql.checkData(0, 5, val)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, "binary5")
        tdSql.checkData(0, 8, "nchar5")

        tdSql.query(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select spread(ts), spread(c1), spread(c2), spread(c3), spread(c4), spread(c5), spread(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 9.000000000)
        tdSql.checkData(0, 2, 9.000000000)
        tdSql.checkData(0, 3, 9.000000000)
        tdSql.checkData(0, 4, 9.000000000)
        tdSql.checkData(0, 5, 9.000000000)
        tdSql.checkData(0, 6, 9.000000000)

        tdSql.query(
            f"select spread(ts), spread(c1), spread(c2), spread(c3), spread(c4), spread(c5), spread(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        val = tsu + delta
        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {val} limit 5 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.500000000)
        tdSql.checkData(0, 1, 4.500000000)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 4.500000000)
        tdSql.checkData(0, 5, 4.500000000)

        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {val} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        ## TBASE-354
        tdSql.query(
            f"select top(c1, 1) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts, top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 12:10:00")
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, "2018-09-17 13:50:00")
        tdSql.checkData(1, 1, 9)
        tdSql.checkData(2, 0, "2018-09-17 15:30:00")
        tdSql.checkData(2, 1, 9)

        tdSql.query(
            f"select top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select bottom(c1, 1) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts, bottom(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 10:40:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, "2018-09-17 12:20:00")
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 0, "2018-09-17 14:00:00")
        tdSql.checkData(2, 1, 0)

        tdSql.query(
            f"select bottom(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select diff(c1) from {tb}")
        res = rowNum - 1
        tdSql.checkRows(res)

        tdSql.query(f"select  ts, diff(c1) from {tb} where c1 > 5 limit 2 offset 1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 10:20:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 10:30:00")
        tdSql.checkData(1, 1, 1)

        limit = int(int(rowNum / 2))
        offset = limit - 1
        tdSql.query(
            f"select diff(c1) from {tb} where c1 >= 0 limit {limit} offset {offset}"
        )
        tdSql.checkRows(limit)

        limit = int(int(rowNum / 2))
        offset = limit + 1
        val = limit - 2
        tdSql.query(
            f"select  ts, diff(c1) from {tb} where c1 >= 0 limit {limit} offset {offset}"
        )
        tdSql.checkRows(val)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(8, 1, -9)

        ### aggregation + limit offset (with interval)
        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) limit 5"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 4)

        tdSql.query(
            f"select  _wstart, max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.00000)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(4, 1, 5)

        ## TBASE-334
        tdSql.query(
            f"select  _wstart, max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 2 offset 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 4, 5.000000000)
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 4, 8.000000000)


        tdSql.query(
            f"select  _wstart, min(c1), min(c2), min(c3), min(c4), min(c5), min(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 0)

        tdSql.query(
            f"select  _wstart, sum(c1), sum(c2), sum(c3), sum(c4), sum(c5), sum(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(4, 1, 9)

        tdSql.query(
            f"select  _wstart, sum(c1), sum(c2), sum(c3), sum(c4), sum(c5), sum(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 21)
        tdSql.checkData(2, 1, 10)
        tdSql.checkData(3, 1, 9)
        tdSql.checkData(4, 1, 18)

        tdSql.query(
            f"select _wstart,  avg(c1), avg(c2), avg(c3), avg(c4), avg(c5), avg(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)

        tdSql.query(
            f"select  _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5), avg(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 4.000000000)
        tdSql.checkData(1, 1, 7.000000000)
        tdSql.checkData(2, 1, 3.333333333)

        tdSql.query(
            f"select stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) limit 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select  _wstart, stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:30:00")
        tdSql.checkData(0, 1, 0.816496581)
        tdSql.checkData(1, 0, "2018-09-17 10:00:00")
        tdSql.checkData(2, 0, "2018-09-17 10:30:00")
        tdSql.checkData(2, 1, 4.027681991)

        tdLog.info(
            f"select  _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(27m)"
        )
        tdSql.query(
            f"select  _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(27m)"
        )
        tdSql.checkRows(371)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)

        tdSql.query(
            f"select  _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(27m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 1, 3)

        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.500000000)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 5, 4.500000000)

        tdSql.query(
            f"select first(c1), first(c2), first(c3), first(c4), first(c5), first(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select  _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 3, 9.00000)

        tdSql.query(
            f"select last(c1), last(c2), last(c3), last(c4), last(c5), last(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select  _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(2, 3, 1.00000)

        tdSql.query(
            f"select  _wstart, first(ts), first(c1), last(c2), first(c3), last(c4), first(c5), last(c6), first(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 > 0 interval(30m) limit 3 offset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "2018-09-17 09:10:00")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 5)
        tdSql.checkData(1, 4, 3.00000)
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(2, 3, 8)
        tdSql.checkData(2, 8, "binary6")
        tdSql.checkData(2, 9, "nchar8")

        tdSql.query(
            f"select  _wstart, first(ts), first(c1), last(c2), first(c3), last(c4), first(c5), last(c6), first(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 > 0 interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "2018-09-17 09:30:00")
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 5)
        tdSql.checkData(0, 4, 3.00000)
        tdSql.checkData(0, 5, 5.000000000)
        tdSql.checkData(0, 8, "binary3")
        tdSql.checkData(0, 9, "nchar5")
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 8)
        tdSql.checkData(1, 4, 6.00000)
        tdSql.checkData(1, 5, 8.000000000)
        tdSql.checkData(2, 2, 9)
        tdSql.checkData(2, 3, 1)
        tdSql.checkData(2, 4, 9.00000)
        tdSql.checkData(2, 5, 1.000000000)
        tdSql.checkData(2, 6, 9)
        tdSql.checkData(2, 7, 1)
        tdSql.checkData(2, 8, "binary9")
        tdSql.checkData(2, 9, "nchar1")

        ### order by ts + limit offset

        tdSql.query(f"select * from {tb} order by ts asc limit 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, "2018-09-17 09:10:00")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, "2018-09-17 09:20:00")
        tdSql.checkData(2, 3, 2.00000)
        tdSql.checkData(3, 4, 3.000000000)
        tdSql.checkData(4, 5, 4)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary2")
        tdSql.checkData(3, 9, "nchar3")
        tdSql.checkData(4, 0, "2018-09-17 09:40:00")

        tdSql.query(f"select * from {tb} order by ts asc limit 5 offset 1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:20:00")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, "2018-09-17 09:30:00")
        tdSql.checkData(2, 3, 3.00000)
        tdSql.checkData(3, 4, 4.000000000)
        tdSql.checkData(4, 5, 5)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary3")
        tdSql.checkData(3, 9, "nchar4")
        tdSql.checkData(4, 0, "2018-09-17 09:50:00")

        tdSql.query(f"select * from {tb} order by ts asc limit 5 offset 8")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 10:20:00")
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(1, 0, "2018-09-17 10:30:00")
        tdSql.checkData(1, 2, 9)

        tdSql.query(f"select * from {tb} where c1 > 1 order by ts asc limit 3 offset 2")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 09:40:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, "2018-09-17 09:50:00")
        tdSql.checkData(2, 0, "2018-09-17 10:00:00")
        tdSql.checkData(1, 2, 5)
        tdSql.checkData(2, 3, 6.00000)

        offset = int(rowNum / 10)
        tdSql.query(
            f"select * from {tb} where c1 < 3 and c1 > 1 order by ts asc limit 3 offset {offset}"
        )
        tdSql.checkRows(0)

        offset = int(rowNum / 10)
        offset = offset * 3
        offset = offset - 1

        tdLog.info(
            f"=== select * from {tb} where c1 < 5 and c1 > 1 order by ts asc limit 3 offset {offset}"
        )
        tdSql.query(
            f"select * from {tb} where c1 < 5 and c1 > 1 order by ts asc limit 3 offset {offset}"
        )
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)}")        
        tdSql.checkData(0, 0, "2018-09-24 06:40:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 4, 4.000000000)

        tdSql.query(f"select * from {tb} order by ts desc limit 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-24 07:30:00")
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, "2018-09-24 07:20:00")
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(2, 0, "2018-09-24 07:10:00")
        tdSql.checkData(2, 3, 7.00000)
        tdSql.checkData(3, 4, 6.000000000)
        tdSql.checkData(4, 5, 5)
        tdSql.checkData(0, 6, 9)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary7")
        tdSql.checkData(3, 9, "nchar6")
        tdSql.checkData(4, 0, "2018-09-24 06:50:00")

        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset 1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-24 07:20:00")
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(1, 0, "2018-09-24 07:10:00")
        tdSql.checkData(1, 2, 7)
        tdSql.checkData(2, 0, "2018-09-24 07:00:00")
        tdSql.checkData(2, 3, 6.00000)
        tdSql.checkData(3, 4, 5.000000000)
        tdSql.checkData(4, 5, 4)
        tdSql.checkData(0, 6, 8)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary6")
        tdSql.checkData(3, 9, "nchar5")
        tdSql.checkData(4, 0, "2018-09-24 06:40:00")

        offset = rowNum
        offset = offset - 2
        tdLog.info(f"==== select * from {tb} order by ts desc limit 5 offset {offset}")
        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset {offset}")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:00:00")
        tdSql.checkData(1, 2, 0)

        tdSql.query(
            f"select * from {tb} where c1 < 8 order by ts desc limit 3 offset 2"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-24 06:50:00")
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, "2018-09-24 06:40:00")
        tdSql.checkData(2, 0, "2018-09-24 06:30:00")
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 3, 3.00000)

        offset = int(rowNum / 10)
        tdSql.query(
            f"select * from {tb} where c1 < 8 and c1 > 6 order by ts desc limit 3 offset {offset}"
        )
        tdSql.checkRows(0)

        limit = int(rowNum / 10)
        limit = limit * 3
        offset = limit - 1
        tdSql.query(
            f"select * from {tb} where c1 < 8 and c1 > 4 order by ts desc limit {limit} offset {offset}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:50:00")
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 4, 5.000000000)

    def limit1_stb(self):
        dbPrefix = "lm1_db"
        tbPrefix = "lm1_tb"
        stbPrefix = "lm1_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit1.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        #### select from supertable

        ### select from stb + limit offset
        tdSql.query(f"select * from {stb} limit 5")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {stb} limit 5 offset 1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 1, 5)

        val = totalNum - 1
        tdSql.query(f"select * from {stb} limit {totalNum} offset 1")
        tdSql.checkRows(val)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 1, 5)

        ##TBASE-352
        offset = tbNum * rowNum
        offset = offset - 1
        tdSql.query(f"select * from {stb} order by ts limit 2 offset {offset}")
        tdSql.checkRows(1)

        offset = tbNum * rowNum
        offset = int(offset / 2)
        offset = offset - 1
        tdSql.query(f"select * from {stb} limit 2 offset {offset}")
        tdSql.checkRows(2)

        # if $tdSql.getData(0,0, "2018-11-25 19:30:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1, 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2, 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,3, 9.00000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4, 9.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5, 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6, 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,8, binary9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,9, nchar9 then
        #  return -1
        # endi
        # if $tdSql.getData(1,0, "2018-09-17 09:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(1,1, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,2, NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,3, 0.00000 then
        #  return -1
        # endi
        # if $tdSql.getData(1,4, NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,5, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,6, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,7, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(1,8, binary0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,9, nchar0 then
        #  return -1
        # endi

        ### offset >= rowsInFileBlock
        ##TBASE-352
        limit = int(totalNum / 2)
        offset = totalNum
        tdSql.query(f"select * from {stb} limit {limit} offset {offset}")
        tdSql.checkRows(0)

        offset = offset - 1
        tdSql.query(f"select * from {stb} limit {limit} offset {offset}")
        tdSql.checkRows(1)

        limit = int(totalNum / 2)
        offset = int(totalNum / 2)
        offset = offset - 1
        tdSql.query(f"select * from {stb} limit {limit} offset {offset}")
        tdSql.checkRows(limit)

        offset = int(totalNum / 2)
        offset = offset + 1
        tdSql.query(f"select * from {stb} limit {limit} offset {offset}")
        val = limit - 1
        tdSql.checkRows(val)

        tdSql.query(
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {stb} limit 1 offset 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {stb} limit 1 offset 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {stb} order by ts limit 1 offset 4;"
        )
        tdSql.checkRows(1)

        ### select from supertable + where + limit offset
        tdSql.query(
            f"select * from {stb} where ts > '2018-09-17 09:30:00.000' and ts < '2018-09-17 10:30:00.000' limit 5 offset 1"
        )
        tdSql.checkRows(5)

        offset = int(totalNum / 2)
        tdSql.query(
            f"select * from {stb} where ts >= {ts0} and ts <= {tsu} limit 5 offset {offset}"
        )
        tdSql.checkRows(5)

        # if $tdSql.getData(0,0, "2018-09-17 09:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,2, NULL then
        #  return -1
        # endi
        # if $tdSql.getData(2,3, 2.00000 then
        #  return -1
        # endi
        # if $tdSql.getData(3,4, NULL then
        #  return -1
        # endi
        # if $tdSql.getData(4,5, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,7, 1 then
        # return -1
        # endi
        # if $tdSql.getData(2,8, binary2 then
        #  return -1
        # endi
        # if $tdSql.getData(3,9, nchar3 then
        #  return -1
        # endi

        limit = int(totalNum / 2)
        tdSql.query(
            f"select * from {stb} where ts >= {ts0} and ts <= {tsu} limit {limit} offset 1"
        )
        tdSql.checkRows(limit)

        # if $tdSql.getData(0,0, "2018-09-17 09:10:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(1,2, 2 then
        #  return -1
        # endi
        # if $tdSql.getData(2,3, 3.00000 then
        #  return -1
        # endi
        # if $tdSql.getData(3,4, 4.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(4,5, 5 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(1,7, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(2,8, binary3 then
        #  return -1
        # endi
        # if $tdSql.getData(3,9, nchar4 then
        #  return -1
        # endi

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} limit 1 offset 0"
        )
        tdSql.checkRows(1)

        # if $tdSql.getData(0,0, 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1, 0 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2, 4.500000000 then
        #  return -1
        # endi
        # $val = 45 * $rowNum
        # if $tdSql.getData(0,3, $val then
        #  return -1
        # endi
        # if $tdSql.getData(0,4, 9.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6, binary9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7, nchar0 then
        #  return -1
        # endi

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and c1 > 1 and c2 < 9 and c3 > 2 and c4 < 8 and c5 > 3 and c6 < 7 and c7 != 0 and c8 like '%5' and t1 > 3 and t1 < 6 limit 1 offset 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 5.000000000)

        val = 5 * rowNum
        val = val / 10        
        tdSql.checkData(0, 3, val)
        tdSql.checkData(0, 4, 0.000000000)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, "binary5")
        tdSql.checkData(0, 7, "nchar5")

        limit = totalNum
        offset = totalNum
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 3 and t1 < 6 limit {limit} offset {offset}"
        )
        tdSql.checkRows(0)

        ## TBASE-353
        limit = int(totalNum / 2)
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 asc limit {limit} offset 1"
        )
        tdSql.checkRows(5)

        limit = int(totalNum / 2)
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), count(c4), sum(c5), spread(c6), first(c7), last(c8), first(c9),t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 asc limit {limit} offset 0"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 3, rowNum)
        tdSql.checkData(0, 9, 2)

        val = 9 * rowNum
        val = val / 2        
        tdSql.checkData(1, 4, val)
        tdSql.checkData(1, 9, 3)
        tdSql.checkData(2, 5, 9.000000000)
        tdSql.checkData(2, 6, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 2, 4.500000000)
        tdSql.checkData(3, 3, 0)
        tdSql.checkData(4, 3, 0)
        tdSql.checkData(5, 9, 7)

        limit = int(totalNum / 2)
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 desc limit {limit} offset 1"
        )
        tdSql.checkRows(5)

        limit = int(totalNum / 2)
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), count(c4), sum(c5), spread(c6), first(c7), last(c8), first(c9),t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 desc limit {limit} offset 0"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 2, 4.500000000)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(0, 9, 7)

        val = 9 * rowNum
        val = val / 2        
        tdSql.checkData(1, 4, val)
        tdSql.checkData(1, 9, 6)
        tdSql.checkData(2, 5, 9.000000000)
        tdSql.checkData(2, 6, 1)
        tdSql.checkData(3, 1, 0)
        tdSql.checkData(3, 2, 4.500000000)
        tdSql.checkData(3, 3, rowNum)

        val = 9 * rowNum
        val = val / 2        
        tdSql.checkData(4, 4, val)
        tdSql.checkData(5, 9, 2)

        ### supertable aggregation + where + interval + limit offset
        ## TBASE-355
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), count(c4), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 interval(5m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 1, 1)

        ### [TBASE-361]
        offset = int(rowNum / 2)
        offset = offset + 1
        tdLog.info(
            f"=== select _wstart, max(c1), min(c2), avg(c3), count(c4), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 interval(5m) limit {offset} offset {offset}"
        )
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), count(c4), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 interval(5m) limit {offset} offset {offset}"
        )
        val = rowNum - offset
        tdSql.checkRows(val)
        tdSql.checkData(0, 0, "2018-09-20 20:30:00")
        tdSql.checkData(0, 1, 1)

        ## supertable aggregation + where + interval + group by order by tag + limit offset
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9),t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4 partition by t1  interval(5m) order by t1 desc, max(c1) asc limit 2 offset 0"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 9, 4)

        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4 partition by t1 interval(5m) order by t1 desc limit 2 offset 1"
        )
        tdSql.checkRows(2)

        tdLog.info(
            f"=== select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4 partition by t1 interval(5m) limit 1 offset 0"
        )
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4 partition by t1 interval(5m) limit 1 offset 0"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 and c1 > 0 and c2 < 9 and c3 > 4 and c4 < 7 and c5 > 4 partition by t1  interval(5m)  limit 2 offset 0"
        )
        tdSql.checkRows(6)
