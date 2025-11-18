from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck

class TestArithmetic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_arithmetic(self):
        """Operator arithmetic

        1. Arithmetic operations between data columns
        2. Arithmetic operations between functions
        3. Filling when the operation result is null

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/parser/col_arithmetic_operation.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/fourArithmetic-basic.sim
            - 2025-4-28 Simon Guan Migrated from tsim/vector/metrics_query.sim

        """

        self.Operation()
        tdStream.dropAllStreamsAndDbs()
        self.Basic()
        tdStream.dropAllStreamsAndDbs()
        self.MetricsQuery()
        tdStream.dropAllStreamsAndDbs()
        
        # ========================================= setup environment ================================

    def Operation(self):
        dbPrefix = "ca_db"
        tbPrefix = "ca_tb"
        stbPrefix = "ca_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== col_arithmetic_operation.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfTbNum = tbNum / 2
        t1 = i + 1
        t2 = i + 2
        t3 = i + 3
        t4 = i + 4

        t5 = i + halfTbNum
        t6 = t5 + 1
        t7 = t6 + 1
        t8 = t7 + 1
        t9 = t8 + 1

        tb0 = tbPrefix + str(int(i))
        tb1 = tbPrefix + str(int(t1))
        tb2 = tbPrefix + str(int(t2))
        tb3 = tbPrefix + str(int(t3))
        tb4 = tbPrefix + str(int(t4))

        tb5 = tbPrefix + str(int(t5))
        tb6 = tbPrefix + str(int(t6))
        tb7 = tbPrefix + str(int(t7))
        tb8 = tbPrefix + str(int(t8))
        tb9 = tbPrefix + str(int(t9))

        tdSql.execute(f"create table {tb0} using {stb} tags( {i} )")
        tdSql.execute(f"create table {tb1} using {stb} tags( {t1} )")
        tdSql.execute(f"create table {tb2} using {stb} tags( {t2} )")
        tdSql.execute(f"create table {tb3} using {stb} tags( {t3} )")
        tdSql.execute(f"create table {tb4} using {stb} tags( {t4} )")

        tdSql.execute(f"create table {tb5} using {stb} tags( {t5} )")
        tdSql.execute(f"create table {tb6} using {stb} tags( {t6} )")
        tdSql.execute(f"create table {tb7} using {stb} tags( {t7} )")
        tdSql.execute(f"create table {tb8} using {stb} tags( {t8} )")
        tdSql.execute(f"create table {tb9} using {stb} tags( {t9} )")

        x = 0
        while x < rowNum:
            xs = int(x * delta)
            ts = int(ts0 + xs)
            c = x % 10
            binary = "'binary" + str(int(c)) + "'"
            nchar = "'nchar" + str(int(c)) + "'"
            tdSql.execute(
                f"insert into {tb0} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb1} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb2} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb3} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb4} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
            )
            # tdLog.info(f"insert into {tb0} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb1} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb2} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb3} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} ) {tb4} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )")
            x = x + 1

        x = 0
        while x < rowNum:
            xs = int(x * delta)
            ts = int(ts0 + xs)
            c = x % 10
            binary = "'binary" + str(int(c)) + "'"
            nchar = "'nchar" + str(int(c)) + "'"
            ts = ts + 1
            tdSql.execute(
                f"insert into {tb5} values ( {ts} , NULL , {c} , NULL , {c} , NULL , {c} , NULL, NULL , {nchar} ) {tb6} values ( {ts} , NULL , {c} , NULL , {c} , NULL , {c} , NULL, NULL , {nchar} ) {tb7} values ( {ts} , NULL , {c} , NULL , {c} , NULL , {c} , NULL, NULL , {nchar} ) {tb8} values ( {ts} , NULL , {c} , NULL , {c} , NULL , {c} , NULL, NULL , {nchar} ) {tb9} values ( {ts} , NULL , {c} , NULL , {c} , NULL , {c} , NULL, NULL , {nchar} )"
            )
            x = x + 1

        # =================================== above are setup test environment =============================
        self.col_arithmetic_query()

        # ======================================= all in files query =======================================
        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        self.col_arithmetic_query()

        # ================================================================================================

        tdLog.info(f"====================> crash")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select spread(ts )/(1000*3600*24) from {stb} interval(1y)")

        tdSql.error(f"select first(c1, c2) - last(c1, c2) from {stb} interval(1y)")
        tdSql.error(f"select first(ts) - last(ts) from {stb} interval(1y)")
        tdSql.error(f"select top(c1, 2) - last(c1) from {stb};")
        tdSql.execute(f"select stddev(c1) - last(c1) from {stb};")
        tdSql.error(f"select diff(c1) - last(c1) from {stb};")
        tdSql.execute(f"select first(c7) - last(c7) from {stb};")
        tdSql.execute(f"select first(c8) - last(c8) from {stb};")
        tdSql.execute(f"select first(c9) - last(c9) from {stb};")
        tdSql.execute(f"select max(c2*2) from {tb0}")
        tdSql.execute(f"select max(c1-c2) from {tb0}")

        # ========================================regression test cases====================================
        tdLog.info(f"=====================> td-1764")
        tdSql.query(
            f"select sum(c1)/count(*), sum(c1) as b, count(*) as b from {stb} interval(1y)"
        )

    def col_arithmetic_query(self):
        # ======================================= query test cases ========================================
        # select from table

        dbPrefix = "ca_db"
        tbPrefix = "ca_tb"
        stbPrefix = "ca_stb"
        rowNum = 1000

        i = 0
        db = dbPrefix + str(i)
        tdSql.execute(f"use {db}")

        tb = tbPrefix + "0"
        stb = stbPrefix + str(i)

        ## TBASE-344
        tdSql.query(f"select c1*2 from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.checkData(1, 0, 2.000000000)

        tdSql.checkData(2, 0, 4.000000000)

        tdSql.checkData(9, 0, 18.000000000)

        # asc/desc order [d.2] ======================================================
        tdSql.query(f"select c1 *( 2 / 3 ), c1/c1 from {tb} order by ts asc;")
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 0.000000000)

        # if $tdSql.getData(0,1) != -nan then
        #  print expect -nan, actual: $tdSql.getData(0,1)
        #  return -1
        # endi
        tdSql.checkData(1, 0, 0.666666667)

        tdSql.checkData(1, 1, 1.000000000)

        tdSql.checkData(9, 0, 6.000000000)

        tdSql.checkData(9, 1, 1.000000000)

        tdSql.query(
            f"select (c1 * 2) % 7.9, c1*1, c1*1*1, c1*c1, c1*c1*c1 from {tb} order by ts desc;"
        )
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 2.200000000)

        tdSql.checkData(0, 1, 9.000000000)

        tdSql.checkData(0, 2, 9.000000000)

        tdSql.checkData(0, 3, 81.000000000)

        tdSql.checkData(0, 4, 729.000000000)

        tdSql.checkData(1, 0, 0.200000000)

        tdSql.checkData(1, 1, 8.000000000)

        tdSql.checkData(1, 2, 8.000000000)

        tdSql.checkData(1, 3, 64.000000000)

        tdSql.checkData(1, 4, 512.000000000)

        tdSql.checkData(9, 0, 0.000000000)

        tdSql.checkData(9, 1, 0.000000000)

        tdSql.checkData(9, 2, 0.000000000)

        tdSql.checkData(9, 3, 0.000000000)

        tdSql.checkData(9, 4, 0.000000000)

        # [d.3]
        tdSql.query(
            f"select c1 * c2 /4 from {tb} where ts < 1537166000000 and ts > 1537156000000"
        )
        tdSql.checkRows(17)

        tdSql.checkData(0, 0, 12.250000000)

        tdSql.checkData(1, 0, 16.000000000)

        tdSql.checkData(2, 0, 20.250000000)

        tdSql.checkData(3, 0, 0.000000000)

        # no result return [d.3] ==============================================================
        tdSql.query(f"select c1 * 91- 7 from {tb} where ts < 1537146000000")
        tdSql.checkRows(0)

        # no result return [d.3]
        tdSql.query(
            f"select c2 - c2 from {tb} where ts > '2018-09-17 12:50:00.000' and ts<'2018-09-17 13:00:00.000'"
        )
        tdSql.checkRows(0)

        # single row result aggregation [d.4] =================================================
        # not available

        # error cases
        # not available

        # multi row result aggregation [d.4]
        tdSql.error(f"select top(c1, 1) - bottom(c1, 1) from {tb}")
        tdSql.error(f"select top(c1, 99) - bottom(c1, 99) from {tb}")
        tdSql.query(f"      select top(c1,1) - 88 from {tb}")

        # all data types [d.6] ================================================================
        tdSql.query(
            f"select c2-c1*1.1, c3/c2, c4*c3, c5%c4, (c6+c4)%22, c2-c2 from {tb}"
        )
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 0.000000000)

        # if $tdSql.getData(0,1) != -nan then
        #  return -1
        # endi
        tdSql.checkData(0, 2, 0.000000000)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, 0.000000000)

        tdSql.checkData(0, 5, 0.000000000)

        tdSql.checkData(9, 0, -0.900000000)

        tdSql.checkData(9, 1, 1.000000000)

        tdSql.checkData(9, 2, 81.000000000)

        tdSql.checkData(9, 3, 0.000000000)

        tdSql.checkData(9, 4, 18.000000000)

        # error case, ts/bool/binary/nchar not support arithmetic expression
        tdSql.error(f"select ts+ts from {tb}")
        tdSql.query(f"      select ts+22 from {tb}")
        tdSql.query(f"      select c7*12 from {tb}")
        tdSql.query(f"      select c8/55 from {tb}")
        tdSql.query(f"      select c9+c8 from {tb}")
        tdSql.query(f"      select c7-c8, c9-c8 from {tb}")
        tdSql.error(f"select ts-c9 from {tb}")
        tdSql.query(f"      select c8+c7, c9+c9+c8+c7/c6 from {tb}")

        # arithmetic expression in join [d.7]==================================================

        # arithmetic expression in union [d.8]=================================================

        # arithmetic expression in group by [d.9]==============================================
        # in group by tag, not support for normal table
        tdSql.error(f"select c5*99 from {tb} group by t1")

        # in group by column
        tdSql.error(f"select c6-(c6+c3)*12 from {tb} group by c3;")

        # limit offset [d.10]==================================================================
        tdSql.query(f"select c6 * c1 + 12 from {tb} limit 12 offset 99;")
        tdSql.checkRows(12)

        tdSql.checkData(0, 0, 93.000000000)

        tdSql.checkData(9, 0, 76.000000000)

        tdSql.query(f"select c4 / 99.123 from {tb} limit 10 offset 999;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.090796283)

        # slimit/soffset not support for normal table query. [d.11]============================
        tdSql.error(f"select sum(c1) from {tb} slimit 1 soffset 19;")

        # fill [d.12]==========================================================================
        tdSql.error(f"select c2-c2, c3-c4, c5%c3 from {tb} fill(value, 12);")

        # constant column. [d.13]==============================================================
        tdSql.query(f"select c1, c2+c6, 12.9876545678, 1, 1.1 from {tb}")
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(0, 2, 12.987654568)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 1.100000000)

        tdSql.checkData(1, 0, 1)

        # column value filter [d.14]===========================================================
        tdSql.query(f"select c1, c2+c6, 12.9876545678, 1, 1.1 from {tb} where c1<2")
        tdSql.checkRows(200)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(0, 1, 0.000000000)

        tdSql.checkData(0, 2, 12.987654568)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 0)

        # tag filter(not support for normal table). [d.15]=====================================
        tdSql.query(f"select c2+99 from {tb} where t1=12;")

        # multi-field output [d.16]============================================================
        tdSql.query(f"select c4*1+1/2,c4*1+1/2,c4*1+1/2,c4*1+1/2,c4*1+1/2 from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.checkData(0, 0, 0.500000000)

        tdSql.checkData(1, 0, 1.500000000)

        tdSql.checkData(9, 0, 9.500000000)

        # interval query [d.17]==================================================================
        tdSql.error(f"select c2*c2, c3-c3, c4+9 from {tb} interval(1s)")
        tdSql.error(f"select c7-c9 from {tb} interval(2y)")

        # aggregation query [d.18]===============================================================
        # see test cases below

        # first/last query  [d.19]===============================================================
        # see test cases below

        # multiple retrieve [d.20]===============================================================
        tdSql.query(f"select c2-c2, 911 from {tb}")

        # ======================================= aggregation function arithmetic query cases ===================================
        # on $tb percentile()   spread(ts) bug

        # asc/desc order [d.2]
        tdSql.query(f"select first(c1) * ( 2 / 3 ) from {stb} order by ts asc;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select first(c1) * (2/99) from {stb} order by ts desc;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            f"select (count(c1) * 2) % 7.9, (count(c1) * 2), ( count(1)*2) from {stb}"
        )
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 6.500000000)

        tdSql.checkData(0, 1, 10000.000000000)

        tdSql.checkData(0, 2, 20000.000000000)

        tdSql.query(f"select spread( c1 )/44, spread(c1), 0.204545455 * 44 from {stb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.204545455)

        tdSql.checkData(0, 1, 9.000000000)

        tdSql.checkData(0, 2, 9.000000020)

        # all possible function in the arithmetic expression, add more
        tdSql.query(
            f"select min(c1) * max(c2) /4, sum(c1) * apercentile(c2, 20), apercentile(c4, 33) + 52/9, spread(c5)/min(c2), count(1)/sum(c1), avg(c2)*count(c2) from {stb} where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-11-25 19:30:01.000';"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 0.000000000)

        tdSql.checkData(0, 1, 22500.000000000)

        tdSql.checkData(0, 2, 8.077777778)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, 0.444444444)

        tdSql.checkData(0, 5, 45000.000000000)

        # no result return [d.3]===============================================================
        tdSql.query(
            f"select first(c1) * 91 - 7, last(c3) from {stb} where ts < 1537146000000"
        )
        tdSql.checkRows(0)

        # no result return [d.3]
        tdSql.query(
            f"select sum(c2) - avg(c2) from {stb} where ts > '2018-11-25 19:30:01.000'"
        )
        tdSql.checkRows(0)

        # single row result aggregation [d.4]===================================================
        # all those cases are aggregation test cases.

        # error cases
        tdSql.error(f"select first(c1, c2) - last(c1, c2) from {stb}")
        tdSql.error(f"select top(c1, 5) - bottom(c1, 5) from {stb}")
        tdSql.error(f"select first(*) - 99 from {stb}")

        # multi row result aggregation [d.4]
        tdSql.error(f"select top(c1, 1) - bottom(c1, 1) from {stb}")
        tdSql.error(f"select top(c1, 99) - bottom(c1, 99) from {stb}")

        # query on super table [d.5]=============================================================
        # all cases in this part are query on super table

        # all data types [d.6]===================================================================
        tdSql.query(f"select c2-c1, c3/c2, c4*c3, c5%c4, c6+99%22 from {stb}")

        # error case, ts/bool/binary/nchar not support arithmetic expression
        tdSql.query(f"select first(c7)*12 from {stb}")
        tdSql.query(f"select last(c8)/55 from {stb}")
        tdSql.query(f"select last_row(c9) + last_row(c8) from {stb}")

        # arithmetic expression in join [d.7]===============================================================

        # arithmetic expression in union [d.8]===============================================================

        # arithmetic expression in group by [d.9]===============================================================
        # in group by tag
        tdSql.query(f"select avg(c4)*99, t1 from {stb} group by t1 order by t1")
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, 445.500000000)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(9, 0, 445.500000000)

        tdSql.checkData(9, 1, 9)

        # in group by column
        # sql select apercentile(c6, 50)-first(c6)+last(c5)*12, last(c5)*12 from ca_stb0 group by c2;
        # if $rows != 10 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(0,0) != 0.000000000 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(0,1) != 0.000000000 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(1,0) != 12.000000000 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(1,1) != 12.000000000 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(2,0) != 24.000000000 then
        #  return -1
        # endi
        #
        # if $tdSql.getData(2,1) != 24.000000000 then
        #  return -1
        # endi
        #
        tdSql.query(
            f"select first(c6) - last(c6) *12 / count(*) from {stb} group by c3;"
        )

        tdSql.query(
            f"select first(c6) - last(c6) *12 / count(*) from {stb} group by c5 order by c5;"
        )
        tdSql.checkRows(11)

        tdSql.checkData(1, 0, 0.000000000)

        tdSql.checkData(2, 0, 0.976000000)

        tdSql.checkData(9, 0, 7.808000000)

        # limit offset [d.10]===============================================================
        tdSql.query(f"select first(c6) - sum(c6) + 12 from {stb} limit 12 offset 0;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, -44988.000000000)

        tdSql.query(f"select apercentile(c4, 21) / 99.123 from {stb} limit 1 offset 1;")
        tdSql.checkRows(0)

        tdSql.query(
            f"select apercentile(c4, 21) / sum(c4) from {stb} interval(1s) limit 1 offset 1;"
        )
        tdSql.checkRows(1)

        # slimit/soffset not support for normal table query. [d.11]===============================================================
        tdSql.error(f"select sum(c1) from {stb} slimit 1 soffset 19;")

        tdSql.query(
            f"select sum(c1) from ca_stb0 partition by tbname interval(1s) slimit 1 soffset 1"
        )
        tdSql.query(
            f"select sum(c1) from ca_stb0 partition by tbname interval(1s) slimit 2 soffset 4 limit 10 offset 1"
        )

        # fill [d.12]===============================================================
        tdSql.error(
            f"select first(c1)-last(c1), sum(c3)*count(c3), spread(c5 ) % count(*) from ca_stb0 interval(1s) fill(prev);"
        )
        tdSql.error(f"select first(c1) from ca_stb0 fill(value, 20);")

        # constant column. [d.13]===============================================================

        # column value filter [d.14]===============================================================

        # tag filter. [d.15]===============================================================
        tdSql.query(f"select sum(c2)+99 from ca_stb0 where t1=12;")

        # multi-field output [d.16]===============================================================
        tdSql.query(
            f"select count(*), sum(c1)*avg(c2), avg(c3)*count(c3), sum(c3), sum(c4), first(c7), last(c8), first(c9), first(c7), last(c8) from {tb}"
        )

        tdSql.query(f"select c4*1+1/2 from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.checkData(0, 0, 0.500000000)

        tdSql.checkData(1, 0, 1.500000000)

        tdSql.checkData(9, 0, 9.500000000)

        # interval query [d.17]===============================================================
        tdSql.query(
            f"select avg(c2)*count(c2), sum(c3)-first(c3), last(c4)+9 from ca_stb0 interval(1s)"
        )
        tdSql.checkRows(1000)

        tdSql.query(f"select first(c7)- last(c1) from {tb} interval(2y)")

        # aggregation query [d.18]===============================================================
        # all cases in this part are aggregation query test.

        # first/last query  [d.19]===============================================================

        # multiple retrieve [d.20]===============================================================
        tdSql.query(f"select c2-c2 from {tb}")

        tdSql.query(
            f"select first(c1)-last(c1), spread(c2), max(c3) - min(c3), avg(c4)*count(c4) from {tb}"
        )

    def Basic(self):
        dbNamme = "d0"
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database {dbNamme} vgroups 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}')
        tdSql.checkRows(3)

        tdSql.execute(f"use {dbNamme}")

        tdLog.info(f'=============== create super table')
        tdSql.execute(f"create table if not exists stb (ts timestamp, c1 int, c2 bigint, c3 float, c4 double) tags (t1 int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f'=============== create child table')
        tdSql.execute(f"create table ct0 using stb tags(1000)")
#sql create table ct1 using stb tags(2000)
#sql create table ct3 using stb tags(3000)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f'=============== insert data')

        tbPrefix = "ct"
        tbNum = 1
        rowNum = 10
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            x = 0
            while x < rowNum:
                c2 = x + 10
                c3 = x * 10
                c4 = x - 10

                tdSql.execute(f"insert into {tb} values ({tstart} , {x} , {c2} ,  {c3} , {c4} )")
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 1640966400000

        tdSql.query(f"select ts, c2-c1, c3/c1, c4+c1, c1*9, c1%3 from ct0")
        tdLog.info(f'===> rows: {tdSql.getRows()})')
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, -10.000000000)
        tdSql.checkData(9, 1, 10.000000000)
        tdSql.checkData(9, 2, 10.000000000)
        tdSql.checkData(9, 3, 8.000000000)

        tdLog.info(f'=============== stop and restart taosd')
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        
        tdSql.query(f"select ts, c2-c1, c3/c1, c4+c1, c1*9, c1%3 from ct0")
        tdLog.info(f'===> rows: {tdSql.getRows()})')
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 10.000000000)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, -10.000000000)
        tdSql.checkData(9, 1, 10.000000000)
        tdSql.checkData(9, 2, 10.000000000)
        tdSql.checkData(9, 3, 8.000000000)
        
    def MetricsQuery(self):
        dbPrefix = "m_mq_db"
        tbPrefix = "m_mq_tb"
        mtPrefix = "m_mq_mt"

        tbNum = 10
        rowNum = 21
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, a int, b float, c smallint, d double, e tinyint, f bigint, g binary(10), h bool) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 1
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(
                    f"insert into {tb} values (now + {ms} , {x} , {x} , {x} , {x} ,  {x} , 10 , '11' , true )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 9.000000000)

        tdSql.query(f"select b - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 9.000000000)

        tdSql.query(f"select c - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select d - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select e - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -9.000000000)

        tdSql.query(f"select f - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select g - f from {mt}")
        tdSql.query(f"select h - f from {mt}")
        tdSql.query(f"select ts - f from {mt}")

        tdSql.query(f"select a - e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select d - e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select c - d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select a - b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select b - a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdLog.info(f"=============== step3")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f + a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select b + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f + b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select c + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select d + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select e + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 11.000000000)

        tdSql.query(f"select f + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(f"select a + e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b + e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select c + e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select d + e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a + d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b + d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select c + d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a + c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b + c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select a + b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select b + a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdLog.info(f"=============== step4")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select d * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select e * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select f * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.query(f"select a * e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c * e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select d * e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c * d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a * b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b * a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step5")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select a / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select b / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10.000000000)

        tdSql.query(f"select c / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select d / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select e / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.100000000)

        tdSql.query(f"select f / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select d / e from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select c / d from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / c from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select a / b from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdSql.query(f"select b / a from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"=============== step6")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select (a+ b+ c+ d+ e) / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select f / (a+ b+ c+ d+ e) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select (a+ b+ c+ d+ e) * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select f * (a+ b+ c+ d+ e) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select (a+ b+ c+ d+ e) - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select f - (a+ b+ c+ d+ e) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(f"select (f - (a+ b+ c+ d+ e)) / f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.500000000)

        tdSql.query(f"select (f - (a+ b+ c+ d+ e)) * f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50.000000000)

        tdSql.query(f"select (f - (a+ b+ c+ d+ e)) + f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 15.000000000)

        tdSql.query(f"select (f - (a+ b+ c+ d+ e)) - f from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, -5.000000000)

        tdSql.query(f"select (f - (a*b+ c)*a + d + e) * f  as zz from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100.000000000)

        tdSql.error(f"select (f - (a*b+ c)*a + d + e))) * f  as zz from {mt}")
        tdSql.error(f"select (f - (a*b+ c)*a + d + e))) * 2f  as zz from {mt}")
        tdSql.error(f"select (f - (a*b+ c)*a + d + e))) ** f  as zz from {mt}")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
