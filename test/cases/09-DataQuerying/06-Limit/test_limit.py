from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit(self):
        """Limit

        1. Including multiple data types
        2. With ORDER BY clause
        3. With GROUP BY clause
        4. With PARTITION BY clause
        5. With filtering conditions
        6. With various functions
        7. With different windows

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/parser/limit.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/limit1.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/limit2.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/table_merge_limit.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/projection_limit_offset.sim
            - 2025-8-19 Simon Guan Migrated from tsim/query/complex_limit.sim

        """

        self.ParserLimit()
        tdStream.dropAllStreamsAndDbs()
        self.ParserLimit1()
        tdStream.dropAllStreamsAndDbs()
        self.ParserLimit2()
        tdStream.dropAllStreamsAndDbs()
        self.ParserTableMergeLimit()
        tdStream.dropAllStreamsAndDbs()
        self.ParserProjectionLimitOffset()
        tdStream.dropAllStreamsAndDbs()
        self.QueryComplexLimit()
        tdStream.dropAllStreamsAndDbs()
        
    def ParserLimit(self):
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

        
    def ParserLimit1(self):
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

    def ParserLimit2(self):
        dbPrefix = "lm2_db"
        tbPrefix = "lm2_tb"
        stbPrefix = "lm2_stb"
        tbNum = 10
        rowNum = 10000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== limit2.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 nchar(20), t3 binary(20), t4 bigint, t5 smallint, t6 double)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            i1 = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(i1)
            tgstr = "'tb" + str(i) + "'"
            tgstr1 = "'tb" + str(i1) + "'"
            tdSql.execute(
                f"create table {tb} using {stb} tags( {i} , {tgstr} , {tgstr} , {i} , {i} , {i} )"
            )
            tdSql.execute(
                f"create table {tb1} using {stb} tags( {i1} , {tgstr1} , {tgstr1} , {i} , {i} , {i} )"
            )

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                tdSql.execute(
                    f"insert into {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        self.limit2_query()

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.limit2_query()

    def limit2_query(self):
        dbPrefix = "lm2_db"
        tbPrefix = "lm2_tb"
        stbPrefix = "lm2_stb"
        tbNum = 10
        rowNum = 10000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== limit2.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tb = tbPrefix + str(0)
        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        ##### aggregation on stb with 6 tags + where + group by + limit offset
        val1 = 1
        val2 = tbNum - 1
        tdLog.info(
            f"select count(*) from {stb} where t1 > {val1} and t1 < {val2} group by t1, t2, t3, t4, t5, t6 order by t1 asc limit 1 offset 0"
        )
        tdSql.query(
            f"select count(*), t1, t2, t3, t4, t5, t6 from {stb} where t1 > {val1} and t1 < {val2} group by t1, t2, t3, t4, t5, t6 order by t1 asc limit 1 offset 0"
        )
        val = tbNum - 3

        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, "tb2")

        tdSql.checkData(0, 3, "tb2")

        tdSql.checkData(0, 4, 2)

        tdSql.checkData(0, 5, 2)

        tdSql.query(
            f"select count(*), t3, t4 from {stb} where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 2 offset 0"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, "tb4")

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(1, 1, "tb3")

        tdSql.checkData(1, 2, 3)

        tdSql.query(
            f"select count(*) from {stb} where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 1 offset 1"
        )
        tdSql.checkRows(1)

        ## TBASE-348
        tdSql.error(f"select count(*) from {stb} where t1 like 1")

        ##### aggregation on tb + where + fill + limit offset
        tdSql.query(
            f"select _wstart, max(c1) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1) limit 10 offset 1"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2018-09-17 09:05:00.000")

        tdSql.checkData(0, 1, -1)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(9, 0, "2018-09-17 09:50:00.000")

        tdSql.checkData(9, 1, 5)

        tb5 = tbPrefix + "5"
        tdSql.query(
            f"select max(c1), min(c2) from {tb5} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -2) limit 10 offset 1"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, -1)

        tdSql.checkData(0, 1, -2)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(1, 1, -2)

        tdSql.checkData(9, 0, 5)

        tdSql.checkData(9, 1, -2)

        ### [TBASE-350]
        ## tb + interval + fill(value) + limit offset
        tb = tbPrefix + "0"
        limit = rowNum
        offset = int(limit / 2)
        tdSql.query(
            f"select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit {limit} offset {offset}"
        )
        tdSql.checkRows(limit)

        tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 8200"
        )
        tdSql.checkRows(8200)

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 100000;"
        )

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10 offset 8190;"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, 5)

        tdSql.checkData(1, 0, -1000)

        tdSql.checkData(2, 0, 6)

        tdSql.checkData(3, 0, -1000)

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10 offset 10001;"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, -1000)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, -1000)

        tdSql.checkData(3, 0, 2)

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10000 offset 10001;"
        )
        tdLog.info(f"====> needs to validate the last row result")
        tdSql.checkRows(9998)

        tdSql.query(
            f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 100 offset 20001;"
        )
        tdSql.checkRows(0)

        # tb + interval + fill(linear) + limit offset
        limit = rowNum
        offset = int(limit / 2)
        tdSql.query(
            f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(linear) limit {limit} offset {offset}"
        )
        tdSql.checkRows(limit)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 1, 0)

        tdSql.checkData(1, 3, 0.500000000)

        tdSql.checkData(3, 5, 0.000000000)

        tdSql.checkData(4, 6, 0.000000000)

        tdSql.checkData(4, 7, 1)

        tdSql.checkData(5, 7, None)

        tdSql.checkData(6, 8, "binary3")

        tdSql.checkData(7, 9, None)

        ## tb + interval + fill(prev) + limit offset
        limit = rowNum
        offset = int(limit / 2)
        tdSql.query(
            f"select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev) limit {limit} offset {offset}"
        )
        tdSql.checkRows(limit)

        limit = rowNum
        offset = int(limit / 2)
        offset = offset + 10
        tdSql.query(
            f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 = 5 interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit {limit} offset {offset}"
        )
        tdSql.checkRows(limit)

        tdSql.checkData(0, 1, 5)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(0, 3, 5.000000000)

        tdSql.checkData(0, 4, 5.000000000)

        tdSql.checkData(0, 5, 0.000000000)

        tdSql.checkData(0, 7, 1)

        tdSql.checkData(0, 8, "binary5")

        tdSql.checkData(0, 9, "nchar5")

        # tdSql.checkData(1, 8, None)

        # tdSql.checkData(1, 9, None)

        limit = rowNum
        offset = limit * 2
        offset = offset - 11
        tdSql.query(
            f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from {tb} where ts >= {ts0} and ts <= {tsu} and c1 = 5 interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit {limit} offset {offset}"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 1, -1)

        tdSql.checkData(0, 2, -2)

        tdSql.checkData(1, 1, 5)

        tdSql.checkData(1, 2, 5)

        tdSql.checkData(1, 3, 5.000000000)

        tdSql.checkData(1, 5, 0.000000000)

        tdSql.checkData(1, 6, 0.000000000)

        tdSql.checkData(1, 8, "binary5")

        tdSql.checkData(1, 9, "nchar5")

        ### [TBASE-350]
        ## stb + interval + fill + group by + limit offset
        tdSql.query(
            f"select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 partition by t1 interval(5m) fill(value, -1, -2, -3, -4 ,-7 ,'-8', '-9') limit 2 offset 10"
        )
        tdSql.checkRows(2)

        limit = 5
        offset = rowNum * 2
        offset = offset - 2
        tdSql.query(
            f"select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000  partition by t1 interval(5m) fill(value, -1, -2, -3, -4 ,-7 ,'-8', '-9') order by t1 limit {limit} offset {offset}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 0, 9)

        # tdSql.checkData(0, 1, 9)

        # tdSql.checkData(0, 2, 9.000000000)

        # tdSql.checkData(0, 3, 9.000000000)

        # tdSql.checkData(0, 4, 1)

        # tdSql.checkData(0, 5, "binary9")

        # tdSql.checkData(0, 6, "nchar9")

        # add one more test case
        tdSql.query(
            f'select max(c1), last(c8) from lm2_db0.lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(linear) limit 10 offset 4089;"'
        )

    def ParserTableMergeLimit(self):
        dbPrefix = "m_fl_db"
        tbPrefix = "m_fl_tb"
        mtPrefix = "m_fl_mt"
        tbNum = 2
        rowNum = 513
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== fill.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, c1 int) tags(tgcol int)")

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.query(f"select * from {mt} order by ts limit 10")
        tdSql.checkRows(10)

    def ParserProjectionLimitOffset(self):
        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"
        tbNum = 8
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== projection_limit_offset.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        half = tbNum / 2

        while i < half:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tbId = int(i + half)
            tb1 = tbPrefix + str(tbId)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} ) {tb1} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
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
        
        # ===============select * from super_table limit/offset[TBASE-691]=================================
        tdSql.query(f"select ts from group_mt0")
        tdLog.info(f"{tdSql.getRows()})")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 0;"
        )
        tdSql.checkRows(4008)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 1;"
        )
        tdSql.checkRows(4007)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 101;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3907)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.101")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 902;"
        )
        tdSql.checkRows(3106)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 400;"
        )
        tdSql.checkRows(3608)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 4007;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 2000 offset 4008;"
        )
        tdSql.checkRows(0)

        # ==================================order by desc, multi vnode, limit/offset===================================
        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 0;"
        )
        tdSql.checkRows(4008)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.500")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 1;"
        )
        tdSql.checkRows(4007)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.500")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 101;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3907)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.488")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 902;"
        )
        tdSql.checkRows(3106)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.388")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 400;"
        )
        tdSql.checkRows(3608)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.450")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 4007;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 2000 offset 4008;"
        )
        tdSql.checkRows(0)

        # =================================single value filter======================================
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 0;"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 2;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 4;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 7;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 8;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 9;"
        )
        tdSql.checkRows(0)

        # ===============================single value filter, order by desc============================
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 0;"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 2;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 4;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 7;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 8;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 9;"
        )
        tdSql.checkRows(0)

        # [tbase-695]
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-01-01 8:1:40' and ts<'1970-1-1 8:1:40.500' and c1<99999999 limit 10000 offset 500"
        )
        tdSql.checkRows(3500)

        # =================================parse error sql==========================================
        tdSql.error(
            f"select ts,tbname from group_mt0 order by ts desc limit 100 offset -1;"
        )
        tdSql.error(
            f"select ts,tbname from group_mt0 order by c1 asc limit 100 offset -1;"
        )
        tdSql.error(f"select ts,tbname from group_mt0 order by ts desc limit -1, 100;")
        tdSql.error(f"select ts,tbname from group_mt0 order by ts desc slimit -1, 100;")
        tdSql.error(
            f"select ts,tbname from group_mt0 order by ts desc slimit 1 soffset 1;"
        )

        # ================================functions applys to sql===================================
        tdSql.query(f"select first(t1) from group_mt0;")
        tdSql.query(f"select last(t1) from group_mt0;")
        tdSql.query(f"select min(t1) from group_mt0;")
        tdSql.query(f"select max(t1) from group_mt0;")
        tdSql.query(f"select top(t1, 20) from group_mt0;")
        tdSql.query(f"select bottom(t1, 20) from group_mt0;")
        tdSql.query(f"select avg(t1) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")

        # ====================================tbase-722==============================================
        tdLog.info(f"tbase-722")
        tdSql.query(f"select spread(ts) from group_tb0;")
        tdLog.info(f"{tdSql.getData(0,0)}")

        tdSql.checkData(0, 0, 999.000000000)

        # ====================================tbase-716==============================================
        tdLog.info(f"tbase-716")
        tdSql.query(
            f"select count(*) from group_tb0 where ts in ('2016-1-1 12:12:12');"
        )
        tdSql.error(f"select count(*) from group_tb0 where ts < '12:12:12';")

        # ===============================sql for twa==========================================
        tdSql.error(f"select twa(c1) from group_stb0;")
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by t1;"
        )
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by tbname,t1;"
        )
        tdSql.error(f"select twa(c2) from group_stb0 group by tbname,t1;")
        tdSql.error(f"select twa(c2) from group_stb0 group by tbname;")
        tdSql.error(f"select twa(c2) from group_stb0 group by t1;")
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by t1,tbname;"
        )

        # ================================first/last error check================================
        tdSql.execute(f"create table m1 (ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table tm0 using m1 tags(1);")
        tdSql.execute(f"create table tm1 using m1 tags(2);")

        tdSql.execute(
            f"insert into tm0 values(10000, 1) (20000, 2)(30000, 3) (40000, NULL) (50000, 2) tm1 values(10001, 2)(20000,4)(90000,9);"
        )

        # =============================tbase-1205
        tdSql.query(
            f"select count(*) from tm1 where ts<now and ts>= now -1d interval(1h) fill(NULL);"
        )
        tdSql.checkRows(0)

        tdLog.info(f"===================>TD-1834")
        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts asc")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts desc")
        tdSql.checkRows(0)

        tdSql.query(
            f"select _wstart, count(*),first(k),last(k) from m1 where tbname in ('tm0') interval(1s) order by _wstart desc;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "1970-01-01 08:00:50.000")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, None)

        tdSql.checkData(1, 3, None)

        tdLog.info(f"=============tbase-1324")
        tdSql.query(f"select a, k-k from m1")
        tdSql.checkRows(8)

        tdSql.query(f"select diff(k) from tm0")
        tdSql.checkRows(4)

        tdSql.checkData(2, 0, None)

        # error sql
        tdSql.error(f"select * from 1;")
        # sql_error select 1;  // equals to select server_status();
        tdSql.error(f"select k+ str(k);")
        tdSql.error(f"select k+1;")
        tdSql.error(f"select abc();")
        tdSql.query(f"select 1 where 1=2;")
        tdSql.query(f"select 1 limit 1;")
        tdSql.query(f"select 1 slimit 1;")
        tdSql.query(f"select 1 interval(1h);")
        tdSql.error(f"select count(*);")
        tdSql.error(f"select sum(k);")
        tdSql.query(f"select 'abc';")
        tdSql.error(f"select k+1,sum(k) from tm0;")
        tdSql.error(f"select k, sum(k) from tm0;")
        tdSql.error(f"select k, sum(k)+1 from tm0;")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        # =============================tbase-1205
        tdSql.query(
            f"select count(*) from tm1 where ts<now and ts>= now -1d interval(1h) fill(NULL);"
        )

        tdLog.info(f"===================>TD-1834")
        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts asc")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts desc")
        tdSql.checkRows(0)

    def QueryComplexLimit(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
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
        tdLog.info(f"================ query 1 limit/offset")
        tdSql.query(f"select * from ct1 limit 1")
        tdLog.info(f"====> sql : select * from ct1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select * from ct1 limit 9")
        tdLog.info(f"====> sql : select * from ct1 limit 9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> sql : select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from ct1 limit 2 offset 1")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        # tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        # tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 10")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select c1 from stb1 limit 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 50")
        tdLog.info(f"====> sql : select c1 from stb1 limit 50")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(33)

        tdSql.query(f"select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdLog.info(f"================ query 2 complex with limit")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        tdLog.info(f"================ query 1 limit/offset")
        tdSql.query(f"select * from ct1 limit 1")
        tdLog.info(f"====> sql : select * from ct1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select * from ct1 limit 9")
        tdLog.info(f"====> sql : select * from ct1 limit 9")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(8)

        tdSql.query(f"select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> sql : select * from ct1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        tdSql.query(f"select * from ct1 limit 2 offset 1")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        # tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> rows0: {tdSql.getData(0,0)}, {tdSql.getData(0,1)}, {tdSql.getData(0,2)}, {tdSql.getData(0,3)}, {tdSql.getData(0,4)}, {tdSql.getData(0,5)}, {tdSql.getData(0,6)}, {tdSql.getData(0,7)}, {tdSql.getData(0,8)}, {tdSql.getData(0,9)}"
        )
        tdSql.checkRows(1)

        # tdSql.checkData(0, 1, 2)

        # tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from ct1 limit 2 offset 10")
        tdLog.info(f"====> sql : select * from ct1 limit 2 offset 7")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select c1 from stb1 limit 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> rows0: {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 50")
        tdLog.info(f"====> sql : select c1 from stb1 limit 50")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(33)

        tdSql.query(f"select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> sql : select c1 from stb1 limit 1 offset 2")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 1")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 32")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> sql : select c1 from stb1 limit 2 offset 40")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdLog.info(f"================ query 2 complex with limit")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)

        tdSql.error(
            f"select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdLog.info(f"====> rows: {tdSql.getRows()})")
        # tdSql.checkRows(1)


# system sh/exec.sh -n dnode1 -s stop -x SIGINT
