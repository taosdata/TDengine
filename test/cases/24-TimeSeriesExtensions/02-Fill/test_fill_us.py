from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFillUs:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fill_us(self):
        """Fill Us

        1. -

        Catalog:
            - Timeseries:Fill

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/fill_us.sim

        """

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
