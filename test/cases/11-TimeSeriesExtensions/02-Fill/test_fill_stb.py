from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFillStb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fill_stb(self):
        """Fill Stb

        1. -

        Catalog:
            - Timeseries:Fill

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/fill_stb.sim

        """

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
