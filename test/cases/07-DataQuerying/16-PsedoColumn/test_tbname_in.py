from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTbnameIn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tbname_in(self):
        """tbname in

        1.

        Catalog:
            - PsedoColumn

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/tbnameIn.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn.sim")
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
                c = x / 10
                c = c * 10
                c = x - c
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        self.tbnameIn_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.tbnameIn_query()

    def tbnameIn_query(self):
        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tb = tbPrefix + "0"
        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        #### tbname in + other tag filtering
        ## [TBASE-362]
        # tbname in + tag filtering is allowed now!!!
        tdSql.query(
            f"select count(*) from {stb} where tbname in ('ti_tb1', 'ti_tb300') and t1 > 2"
        )

        # tbname in used on meter
        tdSql.query(f"select count(*) from {tb} where tbname in ('ti_tb1', 'ti_tb300')")

        ## tbname in + group by tag
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 300)

        ## duplicated tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb1', 'ti_tb1', 'ti_tb2', 'ti_tb2', 'ti_tb3') group by t1 order by t1 asc"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 0, rowNum)

        tdSql.checkData(2, 1, 3)

        ## wrong tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        ## tbname in + colummn filtering
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0', 'ti_tb2') and c8 like 'binary%' group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 2)

        ## tbname in can accpet Upper case table name
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb0', 'TI_tb1', 'TI_TB2') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 0)

        # multiple tbname in is not allowed NOW
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') and tbname in ('ti_tb5', 'ti_tb1000') group by t1 order by t1 asc"
        )


# if $rows != 4 then
#  return -1
# endi
# if $tdSql.getData(0,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(0,1, 1 then
#  return -1
# endi
# if $tdSql.getData(1,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(1,1, 5 then
#  return -1
# endi
# if $tdSql.getData(2,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(2,1, 300 then
#  return -1
# endi
# if $tdSql.getData(3,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(3,1, 1000 then
#  return -1
# endi

#### aggregation on stb with 6 tags + where + group by + limit offset
# $val1 = 1
# $val2 = $tbNum - 1
# sql select count(*) from $stb where t1 > $val1 and t1 < $val2 group by t1, t2, t3, t4, t5, t6 order by t1 asc limit 1 offset 0
# $val = $tbNum - 3
# if $rows != $val then
#  return -1
# endi
# if $tdSql.getData(0,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(0,1, 2 then
#  return -1
# endi
# if $tdSql.getData(0,2, tb2 then
#  return -1
# endi
# if $tdSql.getData(0,3, tb2 then
#  return -1
# endi
# if $tdSql.getData(0,4, 2 then
#  return -1
# endi
# if $tdSql.getData(0,5, 2 then
#  return -1
# endi
#
# sql select count(*) from $stb where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 1 offset 0
# if $rows != 2 then
#  return -1
# endi
# if $tdSql.getData(0,0, $rowNum then
#  return -1
# endi
# if $tdSql.getData(0,1, tb4 then
#  return -1
# endi
# if $tdSql.getData(0,2, 4 then
#  return -1
# endi
# if $tdSql.getData(1,1, tb3 then
#  return -1
# endi
# if $tdSql.getData(1,2, 3 then
#  return -1
# endi
# sql select count(*) from $stb where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 1 offset 1
# if $rows != 0 then
#  return -1
# sql_error select count(*) from $stb where t1 like 1
#
###### aggregation on tb + where + fill + limit offset
# sql select max(c1) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1, -2) limit 10 offset 1
# if $rows != 10 then
#  return -1
# endi
# if $tdSql.getData(0,0, @18-09-17 09:05:00.000@ then
#  return -1
# endi
# if $tdSql.getData(0,1, -1 then
#  return -1
# endi
# if $tdSql.getData(1,1, 1 then
#  return -1
# endi
# if $tdSql.getData(9,0, @18-09-17 09:50:00.000@ then
#  return -1
# endi
# if $tdSql.getData(9,1, 5 then
#  return -1
# endi
#
# $tb5 = $tbPrefix . 5
# sql select max(c1), min(c2) from $tb5 where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1, -2, -3, -4) limit 10 offset 1
# if $rows != 10 then
#  return -1
# endi
# if $tdSql.getData(0,0, @18-09-17 09:05:00.000@ then
#  return -1
# endi
# if $tdSql.getData(0,1, -1 then
#  return -1
# endi
# if $tdSql.getData(0,2, -2 then
#  return -1
# endi
# if $tdSql.getData(1,1, 1 then
#  return -1
# endi
# if $tdSql.getData(1,2, -2 then
#  return -1
# endi
# if $tdSql.getData(9,0, @18-09-17 09:50:00.000@ then
#  return -1
# endi
# if $tdSql.getData(9,1, 5 then
#  return -1
# endi
# if $tdSql.getData(9,2, -2 then
#  return -1
# endi
#
#### [TBASE-350]
### tb + interval + fill(value) + limit offset
# $tb = $tbPrefix . 0
# $limit = $rowNum
# $offset = $limit / 2
# sql select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1, -2) limit $limit offset $offset
# if $rows != $limit then
#  return -1
# endi
# if $tdSql.getData(0,1, 0 then
#  return -1
# endi
# if $tdSql.getData(1,1, -1 then
#  return -1
# endi
#
## tb + interval + fill(linear) + limit offset
# $limit = $rowNum
# $offset = $limit / 2
# sql select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(linear) limit $limit offset $offset
# if $rows != $limit then
#  return -1
# endi
# if $tdSql.getData(0,1, 0 then
#  return -1
# endi
# if $tdSql.getData(1,1, 0 then
#  return -1
# endi
# if $tdSql.getData(1,3, 0.500000000 then
#  return -1
# endi
# if $tdSql.getData(3,5, 0.000000000 then
#  return -1
# endi
# if $tdSql.getData(4,6, 0.000000000 then
#  return -1
# endi
# if $tdSql.getData(4,7, true then
#  return -1
# endi
# if $tdSql.getData(5,7, NULL then
#  return -1
# endi
# if $tdSql.getData(6,8, binary3 then
#  return -1
# endi
# if $tdSql.getData(7,9, NULL then
#  return -1
# endi
#
### tb + interval + fill(prev) + limit offset
# $limit = $rowNum
# $offset = $limit / 2
# sql select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(prev) limit $limit offset $offset
# if $rows != $limit then
#  return -1
# endi
#
#
# $limit = $rowNum
# $offset = $limit / 2
# $offset = $offset + 10
# sql select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from $tb where ts >= $ts0 and ts <= $tsu and c1 = 5 interval(5m) fill(value, -1, -2) limit $limit offset $offset
# if $rows != $limit then
#  return -1
# endi
# if $tdSql.getData(0,1, 5 then
#  return -1
# endi
# if $tdSql.getData(0,2, 5 then
#  return -1
# endi
# if $tdSql.getData(0,3, 5.000000000 then
#  return -1
# endi
# if $tdSql.getData(0,4, 5.000000000 then
#  return -1
# endi
# if $tdSql.getData(0,5, 0.000000000 then
#  return -1
# endi
# if $tdSql.getData(0,7, true then
#  return -1
# endi
# if $tdSql.getData(0,8, binary5 then
#  return -1
# endi
# if $tdSql.getData(0,9, nchar5 then
#  return -1
# endi
# if $tdSql.getData(1,8, NULL then
#  return -1
# endi
# if $tdSql.getData(1,9, NULL then
#  return -1
# endi
# if $tdSql.getData(1,6, -2.000000000 then
#  return -1
# endi
# if $tdSql.getData(1,7, true then
#  return -1
# endi
# if $tdSql.getData(1,1, -1 then
#  return -1
# endi
#
# $limit = $rowNum
# $offset = $limit * 2
# $offset = $offset - 11
# sql select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from $tb where ts >= $ts0 and ts <= $tsu and c1 = 5 interval(5m) fill(value, -1, -2) limit $limit offset $offset
# if $rows != 10 then
#  return -1
# endi
# if $tdSql.getData(0,1, -1 then
#  return -1
# endi
# if $tdSql.getData(0,2, -2 then
#  return -1
# endi
# if $tdSql.getData(1,1, 5 then
#  return -1
# endi
# if $tdSql.getData(1,2, 5 then
#  return -1
# endi
# if $tdSql.getData(1,3, 5.000000000 then
#  return -1
# endi
# if $tdSql.getData(1,5, 0.000000000 then
#  return -1
# endi
# if $tdSql.getData(1,6, 0.000000000 then
#  return -1
# endi
# if $tdSql.getData(1,8, binary5 then
#  return -1
# endi
# if $tdSql.getData(1,9, nchar5 then
#  return -1
# endi
# if $tdSql.getData(2,7, true then
#  return -1
# endi
# if $tdSql.getData(3,8, NULL then
#  return -1
# endi
# if $tdSql.getData(4,9, NULL then
#  return -1
# endi
#
#### [TBASE-350]
### stb + interval + fill + group by + limit offset
# sql select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from $stb where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1, -2) group by t1 limit 2 offset 10
# if $rows != 20 then
#  return -1
# endi
#
# $limit = 5
# $offset = $rowNum * 2
# $offset = $offset - 2
# sql select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from $stb where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1, -2) group by t1 order by t1 limit $limit offset $offset
# if $rows != $tbNum then
#  return -1
# endi
# if $tdSql.getData(0,0, @18-11-25 19:30:00.000@ then
#  return -1
# endi
# if $tdSql.getData(0,1, 9 then
#  return -1
# endi
# if $tdSql.getData(1,2, -2 then
#  return -1
# endi
# if $tdSql.getData(2,3, 9.000000000 then
#  return -1
# endi
# if $tdSql.getData(3,4, -2.000000000 then
#  return -1
# endi
# if $tdSql.getData(4,5, true then
#  return -1
# endi
# if $tdSql.getData(5,6, binary9 then
#  return -1
# endi
# if $tdSql.getData(6,8, 3 then
#  return -1
# endi
# if $tdSql.getData(7,2, 9 then
#  return -1
# endi
# if $tdSql.getData(8,4, 9.000000000 then
#  return -1
# endi
# if $tdSql.getData(9,8, 0 then
#  return -1
# endi
