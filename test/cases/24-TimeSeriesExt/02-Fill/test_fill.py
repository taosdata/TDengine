from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFill:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fill(self):
        """Fill: basic test

        1. Test fill + value, while generating multiple columns simultaneously
        2. Test various methods such as prev, NULL, none, next, linear, null, null_f, and more.

        Catalog:
            - Timeseries:Fill

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-27 Simon Guan Migrated from tsim/parser/fill.sim
            - 2025-8-27 Simon Guan Migrated from tsim/parser/fill_stb.sim
            - 2025-8-27 Simon Guan Migrated from tsim/parser/fill_us.sim
            - 2025-8-27 Simon Guan Migrated from tsim/query/forceFill.sim

        """

        self.ParserFill()
        tdStream.dropAllStreamsAndDbs()
        self.FillStb()
        tdStream.dropAllStreamsAndDbs()
        self.FillUs()
        tdStream.dropAllStreamsAndDbs()
        self.ForceFill()
        tdStream.dropAllStreamsAndDbs()

    def ParserFill(self):
        dbPrefix = "m_fl_db"
        tbPrefix = "m_fl_tb"
        mtPrefix = "m_fl_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== fill.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 bool, c7 binary(10), c8 nchar(10)) tags(tgcol int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {x} , {x} , {x} , {x} , {x} , true, 'BINARY', 'NCHAR' )"
                )
                x = x + 1
            i = i + 1

        # setup
        i = 0
        tb = tbPrefix + str(i)
        tsu = 4 * delta
        tsu = tsu + ts0

        ## fill syntax test
        # number of fill values exceeds number of selected columns
        tdSql.error(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )

        # number of fill values is smaller than number of selected columns
        tdSql.error(
            f"select _wstart, max(c1), max(c2), max(c3) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6)"
        )

        # unspecified filling method
        tdSql.error(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill (6, 6, 6, 6, 6)"
        )

        ## constant fill test
        # count_with_fill
        tdLog.info(f"constant_fill test")
        tdLog.info(f"count_with_constant_fill")
        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 1)

        # avg_with_fill
        tdLog.info(f"avg_with_constant_fill")
        tdSql.query(
            f"select _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, 6.000000000)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, 6.000000000)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, 6.000000000)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, 6.000000000)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdLog.info(f"max_with_fill")
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdLog.info(f"min_with_fill")
        tdSql.query(
            f"select _wstart, min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdLog.info(f"first_with_fill")
        tdSql.query(
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, '6', '6')"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # check double type values
        tdSql.checkData(0, 4, 0.000000000)

        tdLog.info(f"tdSql.getData(1,4) = {tdSql.getData(1,4)}")
        tdSql.checkData(1, 4, 6.000000000)
        tdSql.checkData(2, 4, 1.000000000)
        tdSql.checkData(3, 4, 6.000000000)
        tdSql.checkData(4, 4, 2.000000000)
        tdSql.checkData(5, 4, 6.000000000)
        tdSql.checkData(6, 4, 3.000000000)

        # check float type values
        tdLog.info(f"{tdSql.getData(0,3)} {tdSql.getData(1,3)}")
        tdSql.checkData(0, 3, 0.00000)
        tdSql.checkData(1, 3, 6.00000)
        tdSql.checkData(2, 3, 1.00000)
        tdSql.checkData(3, 3, 6.00000)
        tdSql.checkData(4, 3, 2.00000)
        tdSql.checkData(5, 3, 6.00000)
        tdSql.checkData(6, 3, 3.00000)
        tdSql.checkData(7, 3, 6.00000)
        tdSql.checkData(8, 3, 4.00000)

        # last_with_fill
        tdLog.info(f"last_with_fill")
        tdSql.query(
            f"select _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, '6', '6')"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # fill_negative_values
        tdSql.query(
            f"select _wstart, sum(c1), avg(c2), max(c3), min(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -1, -1, -1, -1, -1, -1, -1)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, -1)

        # fill_char_values_to_arithmetic_fields
        tdSql.query(
            f"select sum(c1), avg(c2), max(c3), min(c4), avg(c4), count(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c')"
        )

        # fill_multiple_columns
        tdSql.error(
            f"select sum(c1), avg(c2), min(c3), max(c4), count(c6), first(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99, 99, abc, abc)"
        )
        tdSql.query(
            f"select _wstart, sum(c1), avg(c2), min(c3), max(c4) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99)"
        )
        tdSql.checkRows(9)

        tdLog.info(f"tdSql.getData(0,1) = {tdSql.getData(0,1)}")
        tdLog.info(f"tdSql.getData(1,1) = {tdSql.getData(1,1)}")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 99)

        tdSql.query(f"select * from {tb}")
        tdSql.checkData(0, 8, "NCHAR")

        # fill_into_nonarithmetic_fieds
        tdLog.info(
            f"select _wstart, first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 20000000, 20000000, 20000000)"
        )
        tdSql.query(
            f"select _wstart, first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 20000000, 20000000, 20000000)"
        )

        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1, 1, 1)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1.1, 1.1, 1.1)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1e1, 1e1, 1e1)"
        )
        tdSql.query(
            f"select first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1')"
        )
        # fill quoted values into bool column will throw error unless the value is 'true' or 'false' Note:2018-10-24
        # fill values into binary or nchar columns will be set to NULL automatically Note:2018-10-24
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1','1e1')"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, true, true, true)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true', 'true','true')"
        )

        # fill nonarithmetic values into arithmetic fields
        tdSql.error(
            f"select count(*) where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, abc);"
        )
        tdSql.query(
            f"select count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true');"
        )

        tdLog.info(
            f'select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, "1e1");'
        )
        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e1');"
        )

        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1e1);"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '10');"
        )

        ## linear fill
        # feature currently switched off 2018/09/29
        # sql select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(linear)

        ## previous fill
        tdLog.info(f"fill(prev)")
        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(8, 1, 1)

        # avg_with_fill
        tdSql.query(
            f"select _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, 1.000000000)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, 2.000000000)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, 3.000000000)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdSql.query(
            f"select _wstart, min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdSql.query(
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # last_with_fill
        tdSql.query(
            f"select _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        ## NULL fill
        tdLog.info(f"fill(value, NULL)")
        # count_with_fill
        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdLog.info(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 1)

        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(none)"
        )
        tdSql.checkRows(5)

        # avg_with_fill
        tdSql.query(
            f"select _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdSql.query(
            f"select _wstart, min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdSql.query(
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # last_with_fill
        tdSql.query(
            f"select _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # desc fill query
        tdLog.info(f"asc fill query")
        tdSql.query(
            f"select _wstart,count(*) from m_fl_tb0 where ts>='2018-9-17 9:0:0' and ts<='2018-9-17 9:11:00' interval(1m) fill(value,10) order by _wstart asc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)

        tdLog.info(f"desc fill query")
        tdSql.query(
            f"select _wstart,count(*) from m_fl_tb0 where ts>='2018-9-17 9:0:0' and ts<='2018-9-17 9:11:00' interval(1m) fill(value,10) order by _wstart desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2018-09-17 09:11:00")
        tdSql.checkData(0, 1, 10)

        tdLog.info(
            f"=====================> aggregation + arithmetic + fill, need to add cases TODO"
        )
        # sql select avg(cpu_taosd) - first(cpu_taosd) from dn1 where ts<'2020-11-13 11:00:00' and ts>'2020-11-13 10:50:00' interval(10s) fill(value, 99)
        # sql select count(*), first(k), avg(k), avg(k)-first(k) from tm0 where ts>'2020-1-1 1:1:1' and ts<'2020-1-1 1:02:59' interval(10s) fill(value, 99);
        # sql select count(*), first(k), avg(k), avg(k)-first(k) from tm0 where ts>'2020-1-1 1:1:1' and ts<'2020-1-1 1:02:59' interval(10s) fill(NULL);

        tdLog.info(f"=====================> td-2060")
        tdSql.execute(f"create table m1 (ts timestamp, k int ) tags(a int);")
        tdSql.execute(f"create table if not exists tm0 using m1 tags(1);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:1:1', 1);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:1:2', 2);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:1:3', 3);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:2:4', 4);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:2:5', 5);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:2:6', 6);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:3:7', 7);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:3:8', 8);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:3:9', 9);")
        tdSql.execute(f"insert into tm0 values('2020-1-1 1:4:10', 10);")

        tdLog.info(
            f"select _wstart, max(k)-min(k),last(k)-first(k),0-spread(k) from tm0 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 1:2:15' interval(10s) fill(value, 99,91,90);"
        )
        tdSql.query(
            f"select _wstart, max(k)-min(k),last(k)-first(k),0-spread(k) from tm0 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 1:2:15' interval(10s) fill(value, 99,91,90);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2020-01-01 01:01:00")
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(0, 2, 2.000000000)
        tdSql.checkData(0, 3, -2.000000000)
        tdSql.checkData(1, 0, "2020-01-01 01:01:10")
        tdSql.checkData(1, 1, 99.000000000)
        tdSql.checkData(1, 2, 91.000000000)
        tdSql.checkData(1, 3, 90.000000000)
        tdSql.checkData(6, 0, "2020-01-01 01:02:00")
        tdSql.checkData(6, 1, 2.000000000)
        tdSql.checkData(6, 2, 2.000000000)
        tdSql.checkData(6, 3, -2.000000000)
        tdSql.checkData(7, 0, "2020-01-01 01:02:10")
        tdSql.checkData(7, 1, 99.000000000)
        tdSql.checkData(7, 2, 91.000000000)
        tdSql.checkData(7, 3, 90.000000000)

        tdSql.query(
            f"select _wstart, first(k)-avg(k),0-spread(k) from tm0 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 1:2:15' interval(10s) fill(NULL);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2020-01-01 01:01:00")
        tdSql.checkData(0, 1, -1.000000000)
        tdSql.checkData(0, 2, -2.000000000)
        tdSql.checkData(1, 0, "2020-01-01 01:01:10")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(
            f"select _wstart, max(k)-min(k),last(k)-first(k),0-spread(k) from tm0 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 4:2:15'  interval(500a) fill(value, 99,91,90) ;"
        )
        tdSql.checkRows(21749)

        tdLog.info(
            f"select _wstart, max(k)-min(k),last(k)-first(k),0-spread(k),count(1) from m1 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 1:2:15'  interval(10s) fill(value, 99,91,90,89) ;"
        )
        tdSql.query(
            f"select _wstart, max(k)-min(k),last(k)-first(k),0-spread(k),count(1) from m1 where ts>='2020-1-1 1:1:1' and ts<='2020-1-1 1:2:15'  interval(10s) fill(value, 99,91,90,89) ;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2020-01-01 01:01:00")
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(0, 2, 2.000000000)
        tdSql.checkData(0, 3, -2.000000000)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(1, 0, "2020-01-01 01:01:10")
        tdSql.checkData(1, 1, 99.000000000)
        tdSql.checkData(1, 2, 91.000000000)
        tdSql.checkData(1, 3, 90.000000000)
        tdSql.checkData(1, 4, 89)

        tdLog.info(f"==================> td-2115")
        tdSql.query(f"select count(*), min(c3)-max(c3) from m_fl_mt0 group by tgcol")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, -4.000000000)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, -4.000000000)

        tdLog.info(
            f"=====================>td-1442, td-2190 , no time range for fill option"
        )
        tdSql.error(f"select count(*) from m_fl_tb0 interval(1s) fill(prev);")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10a) fill(value, 20)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10s) fill(value, 20)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10m) fill(value, 20)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10h) fill(value, 20)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10d) fill(value, 20)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(10w) fill(value, 20)")
        tdSql.error(f"select max(c3) from m_fl_mt0 interval(1n) fill(prev)")
        tdSql.error(f"select min(c3) from m_fl_mt0 interval(1y) fill(value, 20)")

        tdSql.execute(f"create table nexttb1 (ts timestamp, f1 int);")
        tdSql.execute(f"insert into nexttb1 values ('2021-08-08 1:1:1', NULL);")
        tdSql.execute(f"insert into nexttb1 values ('2021-08-08 1:1:5', 3);")

        tdSql.query(
            f"select _wstart, last(*) from nexttb1 where ts >= '2021-08-08 1:1:1' and ts < '2021-08-08 1:1:10' interval(1s) fill(next);"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, "2021-08-08 01:01:01")
        tdSql.checkData(0, 1, "2021-08-08 01:01:01")
        tdSql.checkData(0, 2, 3)

        tdLog.info(f"=============== clear")
        # sql drop database $db
        # sql select * from information_schema.ins_databases
        # if $rows != 0 then
        #  return -1
        # endi

        tdLog.info(f"============== fill")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"insert into t1 values(1648712211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648712225000,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648712212000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648712226000,2,2,3);")

        tdSql.query(
            f"select count(*) from(select count(a)  from  st where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(value, -1));"
        )
        tdSql.checkData(0, 0, 40052)

        tdSql.query(
            f"select _wstart, count(a)  from  st where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(prev);"
        )

        tdSql.checkRows(40052)

        tdSql.query(
            f"select _wstart, count(a)  from  st where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(next);"
        )

        tdSql.checkRows(40052)

        tdSql.query(
            f"select _wstart, count(a)  from  st where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(linear);"
        )

        tdSql.checkRows(40052)

        tdSql.query(
            f"select _wstart, count(a)  from  st where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(NULL);"
        )

        tdSql.checkRows(40052)

        tdSql.query(
            f"select _wstart, count(a)  from  t1 where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(value, -1);"
        )

        tdSql.checkRows(20026)

        tdSql.query(
            f"select _wstart, count(a)  from  t1 where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(NULL);"
        )

        tdSql.checkRows(20026)

        tdSql.query(
            f"select _wstart, count(a)  from  t1 where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(prev);"
        )

        tdSql.checkRows(20026)

        tdSql.query(
            f"select _wstart, count(a)  from  t1 where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(next);"
        )

        tdSql.checkRows(20026)

        tdSql.query(
            f"select _wstart, count(a)  from  t1 where ts >= 1648712201000 and ts <= 1648732226000 partition by tbname interval(1s) fill(linear);"
        )

        tdSql.checkRows(20026)

        tdLog.info(
            f"===================== TD-25209 test fill prev/next/linear after data range"
        )
        tdSql.execute(f"use {db}")

        tdSql.query(
            f"select _wstart,_wend,count(*) from tm0 where ts >= '2020-01-01 01:03:06.000' and ts <= '2020-01-01 01:03:10.000' interval(1s) fill(prev);"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 1)

        tdSql.query(
            f"select _wstart,_wend,count(*) from tm0 where ts >= '2020-01-01 01:03:06.000' and ts <= '2020-01-01 01:03:10.000' interval(1s) fill(next);"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, None)

        tdSql.query(
            f"select _wstart,_wend,count(*) from tm0 where ts >= '2020-01-01 01:03:06.000' and ts <= '2020-01-01 01:03:10.000' interval(1s) fill(linear);"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, None)

        tdLog.info(f"===================== TD-3625 test fill value NULL")
        tdSql.execute(f"use {db}")

        tdSql.query(
            f"select _wstart,_wend,count(*) from tm0 where ts >= '2020-01-01 01:03:06.000' and ts <= '2020-01-01 01:03:10.000' interval(1s) fill(value, NULL);"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, None)

        tdSql.query(
            f"select _wstart,_wend,count(*),sum(k),avg(k) from tm0 where ts >= '2020-01-01 01:03:06.000' and ts <= '2020-01-01 01:03:10.000' interval(1s) fill(value, 1, NULL, 1);"
        )

        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, 7)
        tdSql.checkData(2, 3, 8)
        tdSql.checkData(3, 3, 9)
        tdSql.checkData(4, 3, None)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(1, 4, 7.000000000)
        tdSql.checkData(2, 4, 8.000000000)
        tdSql.checkData(3, 4, 9.000000000)
        tdSql.checkData(4, 4, 1.000000000)

    def FillStb(self):
        dbPrefix = "fl1_db"
        tbPrefix = "fl1_tb"
        stbPrefix = "fl1_stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== fill.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 nchar(20), t3 binary(20), t4 bigint, t5 bool, t6 double)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            i1 = i + halfNum
            tb = tbPrefix + str(int(i))
            tb1 = tbPrefix + str(int(i1))
            tgstr = "'tb" + str(int(i)) + "'"
            tgstr1 = "'tb" + str(int(i)) + "'"
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

        # setup
        i = 0
        tb = tbPrefix + str(i)
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        #### fill test cases for stables

        # NULL values in each group
        # sql select count(*) from $stb where ts >= '2018-09-16 00:00:00.000' and ts <= $tsu interval(1d) fill(prev) group by t1
        # $val = $tbNum * 2
        # if rows != $val then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != @18-09-16 00:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(0,2) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,1) != $rowNum then
        #  return -1
        # endi
        # if $tdSql.getData(1,2) != 0 then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != @18-09-16 00:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(2,1) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(2,2) != NULL then
        #  return -1
        # endi

        # number of fill values exceeds number of selected columns
        tdLog.info(
            f"select _wstart, count(ts), max(c1), max(c2), max(c3), max(c4), max(c5) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -2, -3, -4, -5, -6)"
        )
        tdSql.query(
            f"select _wstart, count(ts), max(c1), max(c2), max(c3), max(c4), max(c5) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -2, -3, -4, -5, -6)"
        )
        val = rowNum * 2
        val = val - 1
        tdLog.info(f"{tdSql.getRows()})  {val}")
        tdSql.checkRows(val)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(1, 2, -2)
        tdSql.checkData(1, 3, -3)
        tdSql.checkData(1, 4, -4.00000)
        tdSql.checkData(1, 5, -5.000000000)
        tdSql.checkData(3, 1, -1)
        tdSql.checkData(5, 2, -2)
        tdSql.checkData(7, 3, -3)
        tdSql.checkData(7, 4, -4.00000)

        ## fill(value) + group by
        tdLog.info(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 interval(5m) fill(value, -1, -2, -3, -4, -5)"
        )
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 interval(5m) fill(value, -1, -2, -3, -4, -5)"
        )
        val = rowNum * 2
        tdLog.info(f"{rowNum}, {val}")

        val = val - 1
        val = val * tbNum
        tdLog.info(f"==================== {val}")

        tdSql.checkRows(190)
        tdSql.checkData(1, 1, -1)

        # number of fill values is smaller than number of selected columns
        tdLog.info(
            f"select _wstart, max(c1), max(c2), max(c3) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6)"
        )
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6)"
        )
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(1, 3, 6.00000)

        # unspecified filling method
        tdSql.error(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill (6, 6, 6, 6, 6)"
        )

        # fill_char_values_to_arithmetic_fields
        tdSql.query(
            f"select sum(c1), avg(c2), max(c3), min(c4), avg(c4), count(c6), last(c7), last(c8) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c')"
        )

        # fill_multiple_columns
        tdSql.error(
            f"select sum(c1), avg(c2), min(c3), max(c4), count(c6), first(c7), last(c8) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99, 99, abc, abc)"
        )
        tdSql.query(
            f"select _wstart, sum(c1), avg(c2), min(c3), max(c4) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99)"
        )
        val = rowNum * 2
        val = val - 1
        tdSql.checkRows(val)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 99)

        tdSql.query(f"select * from {stb}")
        # print tdSql.getData(0,8) = $tdSql.getData(0,8)
        tdSql.checkData(0, 9, "nchar0")

        tdLog.info(
            f"select max(c4) from {stb} where t1 > 4 and ts >= {ts0} and ts <= {tsu} partition by t1 interval(5m) fill(value, -1)"
        )
        tdSql.query(
            f"select max(c4) from {stb} where t1 > 4 and ts >= {ts0} and ts <= {tsu} partition by t1 interval(5m) fill(value, -1)"
        )
        # if $rows != 0 then
        #  return -1
        # endi

        tdSql.query(
            f"select _wstart, min(c1), max(c4) from {stb} where t1 > 4 and ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -1)"
        )
        val = rowNum * 2
        val = val - 1
        tdSql.checkRows(val)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, -1.000000000)
        tdSql.checkData(1, 1, -1)
        tdSql.checkData(1, 2, -1.000000000)

        # fill_into_nonarithmetic_fieds
        tdSql.query(
            f"select _wstart, first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '20000000', '20000000', '20000000')"
        )
        # if $tdSql.getData(1,1) != 20000000 then
        # if $tdSql.getData(1,1) != 1 then
        #  return -1
        # endi

        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1', '1', '1')"
        )
        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1.1', '1.1', '1.1')"
        )
        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e1', '1e1', '1e1')"
        )
        tdSql.query(
            f"select first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1')"
        )
        # fill quoted values into bool column will throw error unless the value is 'true' or 'false' Note:2018-10-24
        # fill values into binary or nchar columns will be set to NULL automatically Note:2018-10-24
        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1','1e1')"
        )
        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true', 'true', 'true')"
        )
        tdSql.query(
            f"select first(c7), first(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true', 'true','true')"
        )

        # fill nonarithmetic values into arithmetic fields
        tdSql.query(
            f"select count(*) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'abc');"
        )
        tdSql.query(
            f"select count(*) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true');"
        )

        tdSql.query(
            f"select _wstart, count(*) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '2e1');"
        )

        tdSql.query(
            f"select _wstart, count(*) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 20);"
        )
        tdSql.checkRows(val)
        tdSql.checkData(0, 1, rowNum)
        # if $tdSql.getData(1,1) != 20 then
        #  return -1
        # endi

        ## linear fill
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} partition by t1 interval(5m) fill(linear)"
        )
        val = rowNum * 2
        val = val - 1
        val = val * tbNum
        tdSql.checkRows(val)

        # if $tdSql.getData(0,8) != 0 then
        #  return -1
        # endi
        # if $tdSql.getData(1,5) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,6) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,7) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(1,8) != 0 then
        #  return -1
        # endi

        ## [TBASE-365]
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(linear)"
        )
        tdLog.info(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(linear)"
        )
        tdSql.checkRows(95)

        # if $tdSql.getData(0,2) != NULL then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != NULL then
        #  return -1
        # endi
        tdSql.checkData(0, 6, "binary0")
        tdSql.checkData(0, 7, "nchar0")
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 6, None)
        tdSql.checkData(1, 7, None)

        tdSql.query(
            f"select _wstart, max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(linear)"
        )
        val = rowNum * 2
        val = val - 1
        tdSql.checkRows(val)
        tdSql.checkData(0, 7, "nchar0")
        tdSql.checkData(1, 7, None)

        tdSql.query(
            f"select max(c1), min(c2), sum(c3), avg(c4), first(c9), last(c8), first(c9) from {stb} where ts >= '2018-09-16 00:00:00.000' and ts <= '2018-09-18 00:00:00.000' interval(1d) fill(linear)"
        )
        tdSql.checkRows(3)

        ## previous fill
        tdLog.info(f"fill(prev)")
        tdLog.info(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), count(c5), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(prev) limit 5"
        )
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), count(c5), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(prev) limit 5"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 8, "nchar0")

        ## NULL fill
        tdLog.info(f"fill(NULL)")
        tdLog.info(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), count(c5), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(value, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) limit 5"
        )
        tdSql.query(
            f"select _wstart, max(c1), min(c2), avg(c3), sum(c4), count(c5), first(c7), last(c8), first(c9) from {stb} where ts >= {ts0} and ts <= {tsu} and t1 > 4 partition by t1 interval(5m) fill(NULL) limit 5"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 8, None)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FillUs(self):
        dbPrefix = "m_fl_db"
        tbPrefix = "m_fl_tb"
        mtPrefix = "m_fl_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000000  # 2018-09-17 09:00:"00+000000"
        delta = 600000000
        tdLog.info(f"========== fill_us.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db} precision 'us'")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 bool, c7 binary(10), c8 nchar(10)) tags(tgcol int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {x} , {x} , {x} , {x} , {x} , true, 'BINARY', 'NCHAR' )"
                )
                x = x + 1
            i = i + 1

        # setup
        i = 0
        tb = tbPrefix + str(i)
        tsu = 4 * delta
        tsu = tsu + ts0

        ## fill syntax test
        # number of fill values exceeds number of selected columns
        tdLog.info(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )
        tdSql.error(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )

        # number of fill values is smaller than number of selected columns
        tdLog.info(
            f"sql select _wstart, max(c1), max(c2), max(c3) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6)"
        )
        tdSql.error(
            f"select _wstart, max(c1), max(c2), max(c3) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6)"
        )

        # unspecified filling method
        tdSql.error(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill (6, 6, 6, 6, 6)"
        )

        ## constant fill test
        # count_with_fill
        tdLog.info(f"constant_fill test")
        tdLog.info(f"count_with_constant_fill")
        tdLog.info(
            f"sql select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )
        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 1)

        # avg_with_fill
        tdLog.info(f"avg_witt_constant_fill")
        tdSql.query(
            f"select avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, 6.000000000)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, 6.000000000)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, 6.000000000)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, 6.000000000)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdLog.info(f"max_with_fill")
        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdLog.info(f"min_with_fill")
        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdLog.info(f"first_with_fill")
        tdSql.query(
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, '6', '6')"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # check double type values
        tdSql.checkData(0, 4, 0.000000000)

        tdLog.info(f"tdSql.getData(1,4) = {tdSql.getData(1,4)}")
        tdSql.checkData(1, 4, 6.000000000)
        tdSql.checkData(2, 4, 1.000000000)
        tdSql.checkData(3, 4, 6.000000000)
        tdSql.checkData(4, 4, 2.000000000)
        tdSql.checkData(5, 4, 6.000000000)
        tdSql.checkData(6, 4, 3.000000000)

        # check float type values
        tdLog.info(f"{tdSql.getData(0,3)} {tdSql.getData(1,3)}")
        tdSql.checkData(0, 3, 0.00000)
        tdSql.checkData(1, 3, 6.00000)
        tdSql.checkData(2, 3, 1.00000)
        tdSql.checkData(3, 3, 6.00000)
        tdSql.checkData(4, 3, 2.00000)
        tdSql.checkData(5, 3, 6.00000)
        tdSql.checkData(6, 3, 3.00000)
        tdSql.checkData(7, 3, 6.00000)
        tdSql.checkData(8, 3, 4.00000)

        # last_with_fill
        tdLog.info(f"last_with_fill")
        tdSql.query(
            f"select last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 6, 6, 6, 6, 6, 6, '6', '6')"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 6)
        tdSql.checkData(8, 1, 4)

        # fill_negative_values
        tdSql.query(
            f"select _wstart, sum(c1), avg(c2), max(c3), min(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, -1, -1, -1, -1, -1, -1, -1, -1)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, -1)

        # fill_char_values_to_arithmetic_fields
        tdSql.query(
            f"select sum(c1), avg(c2), max(c3), min(c4), avg(c4), count(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c')"
        )

        # fill_multiple_columns
        tdSql.error(
            f"select _wstart, sum(c1), avg(c2), min(c3), max(c4), count(c6), first(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99, 99, abc, abc)"
        )
        tdSql.query(
            f"select _wstart, sum(c1), avg(c2), min(c3), max(c4) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 99, 99, 99, 99)"
        )
        tdSql.checkRows(9)

        tdLog.info(f"tdSql.getData(0,1) = {tdSql.getData(0,1)}")
        tdLog.info(f"tdSql.getData(1,1) = {tdSql.getData(1,1)}")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 99)

        tdSql.query(f"select * from {tb}")
        # print tdSql.getData(0,8) = $tdSql.getData(0,8)
        tdSql.checkData(0, 8, "NCHAR")

        # return -1

        # fill_into_nonarithmetic_fieds
        tdSql.query(
            f"select _wstart, first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 20000000, 20000000, 20000000)"
        )

        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1, 1, 1)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1.1, 1.1, 1.1)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1e1, 1e1, 1e1)"
        )
        tdSql.query(
            f"select first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1')"
        )
        # fill quoted values into bool column will throw error unless the value is 'true' or 'false' Note:2018-10-24
        # fill values into binary or nchar columns will be set to null automatically Note:2018-10-24
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e', '1e1','1e1')"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, true, true, true)"
        )
        tdSql.query(
            f"select first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true', 'true','true')"
        )

        # fill nonarithmetic values into arithmetic fields
        tdSql.error(
            f"select count(*) where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, abc);"
        )
        tdSql.query(
            f"select count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 'true');"
        )

        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '1e1');"
        )

        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, 1e1);"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select _wstart, count(*) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(value, '10');"
        )

        ## linear fill
        # feature currently switched off 2018/09/29
        # sql select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(linear)

        ## previous fill
        tdLog.info(f"fill(prev)")
        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(8, 1, 1)

        # avg_with_fill
        tdSql.query(
            f"select _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, 0.000000000)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, 1.000000000)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, 2.000000000)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, 3.000000000)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdSql.query(
            f"select max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdSql.query(
            f"select min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdSql.query(
            f"select first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        # last_with_fill
        tdSql.query(
            f"select last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(prev)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, 3)
        tdSql.checkData(8, 1, 4)

        ## NULL fill
        tdLog.info(f"fill(value, NULL)")
        # count_with_fill
        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdLog.info(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.query(
            f"select _wstart, count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 1)

        tdSql.query(
            f"select count(c1), count(c2), count(c3), count(c4), count(c5), count(c6), count(c7), count(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(none)"
        )
        tdSql.checkRows(5)

        # avg_with_fill
        tdSql.query(
            f"select _wstart, avg(c1), avg(c2), avg(c3), avg(c4), avg(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2.000000000)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3.000000000)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4.000000000)

        # max_with_fill
        tdSql.query(
            f"select _wstart, max(c1), max(c2), max(c3), max(c4), max(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # min_with_fill
        tdSql.query(
            f"select _wstart, min(c1), min(c2), min(c3), min(c4), min(c5) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # first_with_fill
        tdSql.query(
            f"select _wstart, first(c1), first(c2), first(c3), first(c4), first(c5), first(c6), first(c7), first(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill( NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # last_with_fill
        tdSql.query(
            f"select _wstart, last(c1), last(c2), last(c3), last(c4), last(c5), last(c6), last(c7), last(c8) from {tb} where ts >= {ts0} and ts <= {tsu} interval(5m) fill(NULL)"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 1, 4)

        # desc fill query
        tdLog.info(f"desc fill query")
        tdSql.query(
            f"select count(*) from m_fl_tb0 where ts>='2018-9-17 9:0:0' and ts<='2018-9-17 9:11:00' interval(1m) fill(value,10);"
        )
        tdSql.checkRows(12)

        # print =============== clear
        # sql drop database $db
        # sql select * from information_schema.ins_databases
        # if $rows != 0 then
        #  return -1
        # endi

        ######################### us ##########################
        start = 1537146000000000  # 2018-09-17 09:00:"00+000000"
        delta = 600000000

        tdSql.execute(
            f"create table us_st (ts timestamp, c1 int, c2 double) tags(tgcol int)"
        )
        tdSql.execute(f"create table us_t1 using us_st tags( 1 )")

        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000001', 1 , 1)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000002', 2 , 2)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000003', 3 , 3)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000004', 4 , 4)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000005', 5 , 5)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000006', 6 , 6)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000007', 7 , 7)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000008', 8 , 8)")
        tdSql.execute(f"insert into us_t1 values ('2018-09-17 09:00:00.000009', 9 , 9)")

        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000015', 15 , 15)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000016', 16 , 16)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000017', 17 , 17)"
        )

        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000021', 21 , 21)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000022', 22 , 22)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000023', 23 , 23)"
        )

        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000027', 27 , 27)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000028', 28 , 28)"
        )
        tdSql.execute(
            f"insert into us_t1 values ('2018-09-17 09:00:00.000029', 29 , 29)"
        )

        tdSql.query(
            f"select avg(c1), avg(c2) from us_t1 where ts >= '2018-09-17 09:00:00.000002' and ts <= '2018-09-17 09:00:00.000021' interval(3u) fill(value, 999, 999)"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)
        tdSql.checkData(3, 1, 9.000000000)
        tdSql.checkData(4, 1, 999.000000000)
        tdSql.checkData(5, 1, 16.000000000)
        tdSql.checkData(6, 1, 999.000000000)
        tdSql.checkData(7, 1, 21.000000000)

        tdSql.query(
            f"select avg(c1), avg(c2) from us_t1 where ts >= '2018-09-17 09:00:00.000002' and ts <= '2018-09-17 09:00:00.000021' interval(3u) fill(none)"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)
        tdSql.checkData(3, 1, 9.000000000)
        tdSql.checkData(4, 1, 16.000000000)
        tdSql.checkData(5, 1, 21.000000000)

        tdSql.query(
            f"select avg(c1), avg(c2) from us_t1 where ts >= '2018-09-17 09:00:00.000002' and ts <= '2018-09-17 09:00:00.000021' interval(3u) fill(null)"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)
        tdSql.checkData(3, 1, 9.000000000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 1, 16.000000000)
        tdSql.checkData(6, 1, None)
        tdSql.checkData(7, 1, 21.000000000)

        tdSql.query(
            f"select avg(c1), avg(c2) from us_t1 where ts >= '2018-09-17 09:00:00.000002' and ts <= '2018-09-17 09:00:00.000021' interval(3u) fill(prev)"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)
        tdSql.checkData(3, 1, 9.000000000)
        tdSql.checkData(4, 1, 9.000000000)
        tdSql.checkData(5, 1, 16.000000000)
        tdSql.checkData(6, 1, 16.000000000)
        tdSql.checkData(7, 1, 21.000000000)

        tdSql.query(
            f"select _wstart, avg(c1), avg(c2) from us_t1 where ts >= '2018-09-17 09:00:00.000002' and ts <= '2018-09-17 09:00:00.000021' interval(3u) fill(linear)"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 2.000000000)
        tdSql.checkData(1, 1, 4.000000000)
        tdSql.checkData(2, 1, 7.000000000)
        tdSql.checkData(3, 1, 9.000000000)
        tdSql.checkData(4, 1, 12.500000000)
        tdSql.checkData(5, 1, 16.000000000)
        tdSql.checkData(6, 1, 18.500000000)
        tdSql.checkData(7, 1, 21.000000000)

        tdLog.info(f"======== fill_us.sim run end...... ================")

    def ForceFill(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 10;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 2.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 4.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 5.0, \"b\");")

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 8.800000000)
        tdSql.checkData(2, 0, 8.800000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(14400)
        tdSql.checkData(0, 0, 8.800000000)

        tdSql.query(
            f"select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(null_f);"
        )
        tdSql.checkRows(14400)
        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null_f);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 4.000000000)
        tdSql.checkData(5, 0, 5.000000000)
        tdSql.checkData(6, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value, 8.8);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 8.800000000)
        tdSql.checkData(2, 0, 8.800000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 8.800000000)
        tdSql.checkData(1, 0, 8.800000000)
        tdSql.checkData(2, 0, 8.800000000)
        tdSql.checkData(3, 0, 8.800000000)
        tdSql.checkData(4, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null_f);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(value_f, 8.8);"
        )
        tdSql.checkRows(14400)
        tdSql.checkData(0, 0, 8.800000000)

        tdSql.query(
            f"select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(null_f);"
        )
        tdSql.checkRows(14400)
        tdSql.checkData(0, 0, None)
