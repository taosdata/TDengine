from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInterp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interp(self):
        """Interp

        1. -

        Catalog:
            - Query:Interp

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/interp.sim

        """

        dbPrefix = "intp_db"
        tbPrefix = "intp_tb"
        stbPrefix = "intp_stb"
        tbNum = 4
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== interp.sim")
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
            tbId = int(i + halfNum)
            tb = tbPrefix + str(int(i))
            tb1 = tbPrefix + str(tbId)
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

        tdSql.execute(f"create table ap1 (ts timestamp, pav float);")
        tdSql.execute(
            f"INSERT INTO ap1 VALUES ('2021-07-25 02:19:54.100',1) ('2021-07-25 02:19:54.200',2) ('2021-07-25 02:19:54.300',3) ('2021-07-25 02:19:56.500',4) ('2021-07-25 02:19:57.500',5) ('2021-07-25 02:19:57.600',6) ('2021-07-25 02:19:57.900',7) ('2021-07-25 02:19:58.100',8) ('2021-07-25 02:19:58.300',9) ('2021-07-25 02:19:59.100',10) ('2021-07-25 02:19:59.300',11) ('2021-07-25 02:19:59.500',12) ('2021-07-25 02:19:59.700',13) ('2021-07-25 02:19:59.900',14) ('2021-07-25 02:20:05.000', 20) ('2021-07-25 02:25:00.000', 10000);"
        )

        self.interp_test()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.interp_test()

        tdLog.info(f"================= TD-5931")
        tdSql.execute(f"create stable st5931(ts timestamp, f int) tags(t int)")
        tdSql.execute(f"create table ct5931 using st5931 tags(1)")
        tdSql.execute(f"create table nt5931(ts timestamp, f int)")
        tdSql.error(f"select interp(*) from nt5931 where ts=now")
        tdSql.error(f"select interp(*) from st5931 where ts=now")
        tdSql.error(f"select interp(*) from ct5931 where ts=now")

        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', -3.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 3.0, \"b\");")
        tdSql.query(
            f"select a from (select interp(f1) as a from tba1 where ts >= '2022-04-26 15:15:01' and ts <= '2022-04-26 15:15:05' range('2022-04-26 15:15:01','2022-04-26 15:15:05') every(1s) fill(linear)) where a > 0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.500000000)
        tdSql.checkData(1, 0, 3.000000000)

        tdSql.query(
            f"select a from (select interp(f1+1) as a from tba1 where ts >= '2022-04-26 15:15:01' and ts <= '2022-04-26 15:15:05' range('2022-04-26 15:15:01','2022-04-26 15:15:05') every(1s) fill(linear)) where a > 0;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.500000000)
        tdSql.checkData(2, 0, 4.000000000)

    def interp_test(self):
        dbPrefix = "intp_db"
        tbPrefix = "intp_tb"
        stbPrefix = "intp_stb"
        tbNum = 4
        rowNum = 10000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== intp_test.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        ##### select interp from table
        tdLog.info(f"====== select interp from table")
        tb = tbPrefix + str(0)
        ## interp(*) from tb
        tdSql.error(f"select interp(*) from {tb} where ts = {ts0}")

        ## interp + limit offset
        tdSql.error(f"select interp(*) from {tb} where ts = {ts0} limit 5 offset 1")

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0}"
        )

        ## intp + aggregation functions
        t = ts0 + delta
        t = t + delta
        tdSql.error(
            f"select interp(ts), max(c1), min(c2), count(c3), sum(c4), avg(c5), stddev(c6), first(c7), last(c8), interp(c9) from {tb} where ts = {t}"
        )
        tdSql.error(f"select interp(ts) from {tb} where ts={ts0} interval(1s)")

        ### illegal queries on a table
        tdSql.error(f"select interp(ts), c1 from {tb} where ts = {ts0}")
        tdSql.error(f"select interp(ts) from {tb} where ts >= {ts0}")
        tdSql.error(
            f"select interp(ts), max(c1), min(c2), count(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(NULL)"
        )

        ### interp from tb + fill
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t}"
        )

        ## fill(none)
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        ## fill(NULL)
        t = tsu - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, NULL) order by ts asc"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        ## fill(prev)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(prev)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {t} fill(prev)"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        ## fill(linear)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        # columns contain NULL values
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {t} fill(linear)"
        )

        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(linear)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(linear)"
        )

        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(linear)"
        )
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(linear)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        t = tsu + 1000
        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        ## fill(value)
        t = ts0 + 1000
        tdLog.info(f"91")
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(value, -1, -2, -3)"
        )

        # table has NULL columns
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(value, -1, -2, -3)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        ### select interp from stable
        ## interp(*) from stb
        tdLog.info(f"select interp(*) from {stb} where ts = {ts0}")
        tdSql.error(f"select interp(*) from {stb} where ts = {ts0}")

        tdSql.error(f"select interp(*) from {stb} where ts = {t}")

        ## interp(*) from stb + group by
        tdSql.error(
            f"select interp(ts, c1, c2, c3, c4, c5, c7, c9) from {stb} where ts = {ts0} group by tbname order by tbname asc"
        )
        tdLog.info(
            f"====== select interp(ts, c1, c2, c3, c4, c5, c7, c9) from {stb} where ts = {ts0} group by tbname order by tbname asc"
        )

        ## interp(*) from stb + group by + limit offset
        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} group by tbname limit 0"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} group by tbname limit 0 offset 1"
        )

        ## interp(*) from stb + group by + fill(none)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(none) group by tbname"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} fill(none) group by tbname"
        )

        ## interp(*) from stb + group by + fill(none)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(NULL) group by tbname"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} fill(NULL) group by tbname"
        )
        tdLog.info(f"{tdSql.getRows()})")

        ## interp(*) from stb + group by + fill(prev)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(prev) group by tbname"
        )

        ## interp(*) from stb + group by + fill(linear)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(linear) group by tbname"
        )

        ## interp(*) from stb + group by + fill(value)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(value, -1, -2) group by tbname"
        )

        tdSql.error(
            f"select interp(ts,c1) from intp_tb0 where ts>'2018-11-25 19:19:00' and ts<'2018-11-25 19:19:12';"
        )
        tdSql.error(
            f"select interp(ts,c1) from intp_tb0 where ts>'2018-11-25 19:19:00' and ts<'2018-11-25 19:19:12' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(c1) from intp_tb0 where ts>'2018-11-25 18:09:00' and ts<'2018-11-25 19:20:12' every(18m);"
        )

        tdSql.error(
            f"select interp(c1,c3,c4,ts) from intp_tb0 where ts>'2018-11-25 18:09:00' and ts<'2018-11-25 19:20:12' every(18m) fill(linear)"
        )

        tdSql.error(
            f"select interp(c1) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:42:00.000' every(1m) fill(linear);"
        )

        tdSql.error(
            f"select interp(c1) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:42:00.000' every(1m) fill(linear) order by ts desc;"
        )

        tdSql.error(
            f" select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(2m) fill(linear) order by ts;"
        )

        tdSql.error(
            f"select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(3m) fill(linear) order by ts;"
        )

        tdSql.error(
            f"select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(3m) fill(linear) order by ts desc;"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(value, 1);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(NULL);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:56' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:56' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:20:02' and ts<='2021-07-25 02:20:05' every(1s) fill(value, 1);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:20:02' and ts<='2021-07-25 02:20:05' every(1s) fill(null);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 03:25:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 03:25:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:07' every(1s);"
        )
