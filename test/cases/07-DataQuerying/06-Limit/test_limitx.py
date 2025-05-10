from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLimit2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_limit2(self):
        """Limit

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/limit2.sim

        """

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
