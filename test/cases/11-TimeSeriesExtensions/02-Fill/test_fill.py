from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFill:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fill(self):
        """Fill

        1. -

        Catalog:
            - Timeseries:Fill

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/fill.sim

        """

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
