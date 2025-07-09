from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLimit3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit3(self):
        """Limit

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/limit.sim

        """

        # ========================================= setup environment ================================

        dbPrefix = "lm_db"
        tbPrefix = "lm_tb"
        stbPrefix = "lm_stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit.sim")
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
                c2 = c
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                ts = ts + i
                tdLog.info(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )

                ts = ts + int(halfNum)
                tdLog.info(
                    f"insert into {tb1} values ( {ts} , {c2} , NULL , {c2} , NULL , {c2} , {c2} , true, {binary} , {nchar} )"
                )
                tdSql.execute(
                    f"insert into {tb1} values ( {ts} , {c2} , NULL , {c2} , NULL , {c2} , {c2} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        self.limit_tb()
        self.limit_stb()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.limit_tb()
        self.limit_stb()

        tdLog.info(f"========> TD-6017")
        tdSql.execute(f"use {db}")
        tdSql.query(
            f"select * from (select ts, top(c1, 5) from {tb} where ts >= {ts0} order by ts desc limit 3 offset 1)"
        )
        tdSql.query(
            f"select * from (select ts, top(c1, 5) from {stb} where ts >= {ts0} order by ts desc limit 3 offset 1)"
        )

    def limit_tb(self):
        dbPrefix = "lm_db"
        tbPrefix = "lm_tb"
        stbPrefix = "lm_stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit_tb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"====== use db")
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
        tdLog.info(f"{tdSql.getData(0,5)}")
        tdLog.info(f"{rowNum}")
        # print $val        tdSql.checkData(0,0, val)
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

        tdSql.query(
            f"select sum(c1), avg(c2), stddev(c3), max(c4), min(c5), count(c6), first(c7), last(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1>1 and c2<9 and c3>2 and c4<8 and c5>4 and c6<6 limit 1 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 5.000000000)
        tdSql.checkData(0, 2, 0.000000000)
        tdSql.checkData(0, 3, 5.000000000)
        tdSql.checkData(0, 4, 5)
        tdSql.checkData(0, 5, 1)
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
        tdSql.checkData(0, 0, 5400000.000000000)
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

        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.500000000)
        tdSql.checkData(0, 1, 4.500000000)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 4.500000000)
        tdSql.checkData(0, 5, 4.500000000)

        tdSql.query(
            f"select twa(c1), twa(c2), twa(c3), twa(c4), twa(c5), twa(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select top(c1, 1) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdLog.info(f"========> TD-6017")
        tdSql.query(
            f"select * from (select ts, top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts desc limit 3 offset 1)"
        )

        tdSql.query(
            f"select ts, top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts desc limit 3 offset 1"
        )
        tdLog.info(
            f"select ts, top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts desc limit 3 offset 1"
        )
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 10:20:00")
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(1, 0, "2018-09-17 10:10:00")
        tdSql.checkData(1, 1, 7)
        tdSql.checkData(2, 0, "2018-09-17 10:00:00")
        tdSql.checkData(2, 1, 6)

        tdSql.query(
            f"select ts, top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 10:00:00")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(1, 0, "2018-09-17 10:10:00")
        tdSql.checkData(1, 1, 7)
        tdSql.checkData(2, 0, "2018-09-17 10:20:00")
        tdSql.checkData(2, 1, 8)

        tdSql.query(
            f"select top(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select top(c1, 101) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 98"
        )

        tdSql.query(
            f"select bottom(c1, 1) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts, bottom(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} order by ts limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:20:00")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, "2018-09-17 09:30:00")
        tdSql.checkData(2, 1, 3)

        tdSql.query(
            f"select bottom(c1, 5) from {tb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select ts, diff(c1) from {tb} where c1 > 5 limit 2 offset 1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 10:20:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 10:30:00")
        tdSql.checkData(1, 1, 1)

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
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) limit 5 offset 1"
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
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5), max(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 2 offset 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 4, 5.000000000)
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 4, 8.000000000)

        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5), min(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 9)

        tdSql.query(
            f"select sum(c1), sum(c2), sum(c3), sum(c4), sum(c5), sum(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(3, 1, 9)

        tdSql.query(
            f"select sum(c1), sum(c2), sum(c3), sum(c4), sum(c5), sum(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 1, 21)
        tdSql.checkData(2, 1, 9)

        tdSql.query(
            f"select avg(c1), avg(c2), avg(c3), avg(c4), avg(c5), avg(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)

        tdSql.query(
            f"select avg(c1), avg(c2), avg(c3), avg(c4), avg(c5), avg(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 4.000000000)
        tdSql.checkData(1, 1, 7.000000000)
        tdSql.checkData(2, 1, 9.000000000)

        tdSql.query(
            f"select stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) limit 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select _wstart, stddev(c1), stddev(c2), stddev(c3), stddev(c4), stddev(c5), stddev(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 5 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 09:30:00")
        tdSql.checkData(0, 1, 0.816496581)
        tdSql.checkData(1, 0, "2018-09-17 10:00:00")
        tdSql.checkData(2, 0, "2018-09-17 10:30:00")
        tdSql.checkData(2, 1, 0.000000000)

        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(27m)"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)

        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(27m) limit 5 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)

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
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
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
            f"select _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6) from {tb} where ts >= {ts0} and ts <= {tsu} interval(30m) limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(2, 3, 9.00000)

        tdSql.query(
            f"select _wstart, first(ts), first(c1), last(c2), first(c3), last(c4), first(c5), last(c6), first(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 > 0 interval(30m) limit 3 offset 0"
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
            f"select _wstart, first(ts), first(c1), last(c2), first(c3), last(c4), first(c5), last(c6), first(c8), last(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 > 0 interval(30m) limit 3 offset 1"
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
        tdSql.checkData(2, 3, 9)
        tdSql.checkData(2, 4, 9.00000)
        tdSql.checkData(2, 5, 9.000000000)
        tdSql.checkData(2, 6, 9)
        tdSql.checkData(2, 7, 9)
        tdSql.checkData(2, 8, "binary9")
        tdSql.checkData(2, 9, "nchar9")

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
        tdSql.checkRows(2)
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

        tdSql.query(
            f"select * from {tb} where c1 < 3 and c1 > 1 order by ts asc limit 3 offset 2"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where c1 < 5 and c1 > 1 order by ts asc limit 3 offset 2"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:40:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 4, 4.000000000)

        tdSql.query(f"select * from {tb} order by ts desc limit 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 10:30:00")
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, "2018-09-17 10:20:00")
        tdSql.checkData(1, 2, 8)
        tdSql.checkData(2, 0, "2018-09-17 10:10:00")
        tdSql.checkData(2, 3, 7.00000)
        tdSql.checkData(3, 4, 6.000000000)
        tdSql.checkData(4, 5, 5)
        tdSql.checkData(0, 6, 9)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary7")
        tdSql.checkData(3, 9, "nchar6")
        tdSql.checkData(4, 0, "2018-09-17 09:50:00")

        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset 1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 10:20:00")
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(1, 0, "2018-09-17 10:10:00")
        tdSql.checkData(1, 2, 7)
        tdSql.checkData(2, 0, "2018-09-17 10:00:00")
        tdSql.checkData(2, 3, 6.00000)
        tdSql.checkData(3, 4, 5.000000000)
        tdSql.checkData(4, 5, 4)
        tdSql.checkData(0, 6, 8)
        tdSql.checkData(1, 7, 1)
        tdSql.checkData(2, 8, "binary6")
        tdSql.checkData(3, 9, "nchar5")
        tdSql.checkData(4, 0, "2018-09-17 09:40:00")

        tdSql.query(f"select * from {tb} order by ts desc limit 5 offset 8")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2018-09-17 09:00:00")
        tdSql.checkData(1, 2, 0)

        tdSql.query(
            f"select * from {tb} where c1 < 8 order by ts desc limit 3 offset 2"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-09-17 09:50:00")
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, "2018-09-17 09:40:00")
        tdSql.checkData(2, 0, "2018-09-17 09:30:00")
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 3, 3.00000)

        tdSql.query(
            f"select * from {tb} where c1 < 8 and c1 > 6 order by ts desc limit 3 offset 2"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where c1 < 8 and c1 > 4 order by ts desc limit 3 offset 2"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:50:00")
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 4, 5.000000000)

    def limit_stb(self):
        dbPrefix = "lm_db"
        tbPrefix = "lm_tb"
        stbPrefix = "lm_stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit_stb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0
        tsu = tsu + 9

        ##### select from supertable

        # illegal operations
        # sql_error select top(c1, 1) from $stb where ts >= $ts0 and ts <= $tsu limit 5 offset 1
        # sql_error select bottom(c1, 1) from $stb where ts >= $ts0 and ts <= $tsu limit 5 offset 1

        ### select from stb + limit offset
        tdSql.query(f"select * from {stb} limit 5")
        tdSql.checkRows(5)

        tdLog.info(f"select * from {stb} order by ts limit 5 offset 1")
        tdSql.query(f"select * from {stb} order by ts limit 5 offset 1")
        tdSql.checkRows(5)

        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(4,1)}")
        tdSql.checkData(0, 1, 0)

        # if $tdSql.getData(4,1, 5 then
        #  return -1
        # endi

        tdSql.query(f"select * from {stb} order by ts desc limit 5")
        tdSql.checkRows(5)

        tdLog.info(f"select * from {stb} order by ts desc limit 5 offset 1")
        tdSql.query(f"select * from {stb} order by ts desc limit 5 offset 1")
        tdSql.checkRows(5)

        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(1,1)} {tdSql.getData(4,1)}")
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, 9)
        tdSql.checkData(4, 1, 9)

        tdSql.query(f"select * from {stb} order by ts asc limit 5")
        tdLog.info(f"select * from {stb} order by ts asc limit 5")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(4, 0, "2018-09-17 09:00:00.004")
        tdSql.checkData(0, 1, 0)

        tdLog.info(f"tdSql.getData(1,2) = {tdSql.getData(1,2)}")
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(2, 4, 0.000000000)
        tdSql.checkData(3, 5, 0)
        tdSql.checkData(4, 9, "nchar0")

        tdSql.query(f"select * from {stb} order by ts asc limit 5 offset 1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(4, 1, 0)
        tdSql.checkData(4, 0, "2018-09-17 09:00:00.005")
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.001")

        tdSql.query(f"select * from {stb} limit 500 offset 1")
        tdSql.checkRows(99)

        offset = tbNum * rowNum
        offset = offset - 1
        tdLog.info(f"select * from {stb} order by ts limit 2 offset {offset}")
        tdSql.query(f"select * from {stb} order by ts limit 2 offset {offset}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 10:30:00.009")
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, 9.00000)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 9)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, "binary9")
        tdSql.checkData(0, 9, "nchar9")

        offset = tbNum * rowNum
        offset = int(offset / 2)
        offset = offset - 1
        tdSql.query(f"select * from {stb} order by ts limit 2 offset {offset}")
        tdSql.checkRows(2)

        # if $tdSql.getData(0,0, "2018-09-17 10:30:00.002@ then
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

        offset = rowNum * tbNum
        tdSql.query(f"select * from lm_stb0 limit 2 offset {offset}")
        tdSql.checkRows(0)

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
            f"select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9 from {stb}  order by ts limit 1 offset 40;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2018-09-17 09:40:00")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 4.00000)
        tdSql.checkData(0, 4, 4.000000000)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 4)
        tdSql.checkData(0, 8, "binary4")
        tdSql.checkData(0, 9, "nchar4")

        ### select from supertable + where + limit offset
        tdSql.query(
            f"select * from {stb} where ts > '2018-09-17 09:30:00.000' and ts < '2018-09-17 10:30:00.000' order by ts asc limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 3)

        tdSql.query(
            f"select * from {stb} where ts > '2018-09-17 09:30:00.000' and ts < '2018-09-17 10:10:00.000' order by ts asc limit 5 offset 50"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {stb} where ts > '2018-09-17 09:30:00.000' and ts < '2018-09-17 10:30:00.000' order by ts asc limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(4, 1, 3)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from lm_stb0 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 10:30:00.009' limit 1 offset 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 4.500000000)

        val = 45 * rowNum
        tdSql.checkData(0, 3, val)
        tdSql.checkData(0, 4, 9.000000000)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, "binary9")
        tdSql.checkData(0, 7, "nchar0")

        # sql select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from lm_stb0 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 10:30:00.000' and c1 > 1 and c2 < 9 and c3 > 2 and c4 < 8 and c5 > 3 and c6 < 7 and c7 > 0 and c8 like '%5' and t1 > 3 and t1 < 6 limit 1 offset 0;
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from lm_stb0 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 10:30:00.000' and c1 > 1 and c2 < 9 and c3 > 2 and c4 < 8 and c5 > 3 and c6 < 7 and c7 = true and c8 like '%5' and t1 > 3 and t1 < 6 limit 1 offset 0;"
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

        tdSql.query(
            f"select c1, tbname, t1 from lm_stb0 where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-09-17 10:30:00.000' and c1 > 1 and c2 < 9 and c3 > 2 and c4 < 8 and c5 > 3 and c6 < 7 and c7 = 'true' and c8 like '%5' and t1 > 3 and t1 < 6;"
        )

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 3 and t1 < 6 limit 10 offset 10"
        )
        tdSql.checkRows(0)

        ## TBASE-345
        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 asc limit 5 offset 1"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 asc limit 5 offset 0"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 8, 2)
        tdSql.checkData(1, 8, 3)
        tdSql.checkData(2, 8, 4)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 7, "nchar0")

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 desc limit 5 offset 1"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 group by t1 order by t1 desc limit 5 offset 0"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(3, 2, 4.500000000)
        tdSql.checkData(3, 1, 0)
        tdSql.checkData(4, 7, "nchar0")
        tdSql.checkData(0, 8, 7)
        tdSql.checkData(1, 8, 6)
        tdSql.checkData(2, 4, 9.000000000)

        ### supertable aggregation + where + interval + limit offset
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 interval(5m) limit 5 offset 1"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:10:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 0, "2018-09-17 09:50:00")
        tdSql.checkData(4, 1, 5)

        offset = int(rowNum / 2)
        offset = offset + 1
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 interval(5m) limit {offset} offset {offset}"
        )
        val = rowNum - offset
        tdSql.checkRows(val)
        tdSql.checkData(0, 0, "2018-09-17 10:00:00")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(3, 0, "2018-09-17 10:30:00")
        tdSql.checkData(3, 1, 9)

        ### supertable aggregation + where + interval + group by order by tag + limit offset
        ## TBASE-345
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4  partition by t1 interval(5m) order by t1 desc, max(c1) asc limit 3 offset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 9, 4)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(1, 9, 4)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 9, 3)

        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4  partition by t1 interval(5m) order by t1 desc limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 9, 4)

        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 5 and c1 > 0 and c2 < 9 and c3 > 1 and c4 < 7 and c5 > 4  partition by t1 interval(5m) order by t1 desc limit 1 offset 0"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 9, 4)

        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c5), spread(c6), first(c7), last(c8), first(c9), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8 and c1 > 0 and c2 < 9 and c3 > 4 and c4 < 7 and c5 > 4  partition by t1 interval(5m) order by t1 desc, max(c1) asc limit 3 offset 0"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 9, 4)
        tdSql.checkData(1, 9, 4)
        tdSql.checkData(2, 3, 5.000000000)
        tdSql.checkData(2, 9, 3)

        tdSql.query(
            f"select _wstart, max(c1), min(c1), avg(c1), count(c1), sum(c1), spread(c1), first(c1), last(c1), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 1 and t1 < 8  partition by t1 interval(5m) order by t1 asc limit 1 offset 0"
        )
        tdSql.checkRows(1)

        # sql select max(c2), min(c2), avg(c2), count(c2), sum(c2), spread(c2), first(c2), last(c2) from $stb where ts >= $ts0 and ts <= $tsu and t1 > 3 and t1 < 6 interval(5m) group by t1 order by t1 desc limit 3 offset 1
        # if $rows != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0, "2018-09-17 09:20:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1, 2 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2, 2 then
        #  return -1
        # endi
        # if $tdSql.getData(0,9, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(1,3, 3.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(1,9, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(2,0, "2018-09-17 09:40:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(2,4, 1 then
        #  return -1
        # endi
        # if $tdSql.getData(2,5, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(2,6, 0.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(2,7, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(2,8, 4 then
        #  return -1
        # endi
        # if $tdSql.getData(2,9, 4 then
        #  return -1
        # endi

        tdSql.query(
            f"select _wstart, max(c2), min(c2), avg(c2), count(c2), spread(c2), first(c2), last(c2), count(ts), t1 from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 3 and t1 < 6 partition by t1 interval(5m) order by t1 desc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select top(c1, 1) from {stb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts, top(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts desc limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 9)

        tdSql.query(
            f"select ts, top(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 1"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 9)

        tdSql.query(
            f"select ts, top(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} group by t1 order by t1 desc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select ts, top(c1, 5), t1 from {stb} where ts >= {ts0} and ts <= {tsu} group by t1 order by t1 asc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select top(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 order by ts desc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select top(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select top(c1, 101) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 98"
        )

        tdSql.query(
            f"select bottom(c1, 1) from {stb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts, bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts desc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        # if $tdSql.getData(0,0, "2018-09-17 09:00:00.000@ then
        #  return -1
        # endi        tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"select ts, bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 1"
        )
        tdSql.checkRows(3)

        # if $tdSql.getData(0,0, "2018-09-17 09:00:00.000@ then
        #  return -1
        # endi        tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"select ts, bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 slimit 2 soffset 1 limit 3 offset 1"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 slimit 2 soffset 1 limit 3 offset 1"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 slimit 2 soffset 1 limit 3 offset 1"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 5"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select bottom(c1, 101) from {stb} where ts >= {ts0} and ts <= {tsu} order by ts asc limit 3 offset 98"
        )

        tdSql.query(
            f"select bottom(c1, 1) from {stb} where ts >= {ts0} and ts <= {tsu} limit 5 offset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select bottom(c1, 5) from {stb} where ts >= {ts0} and ts <= {tsu} limit 3 offset 5"
        )
        tdSql.checkRows(0)
