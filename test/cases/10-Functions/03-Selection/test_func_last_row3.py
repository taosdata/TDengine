from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLastRow3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_row3(self):
        """Last_Row 3

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow.sim

        """

        dbPrefix = "lr_db"
        tbPrefix = "lr_tb"
        stbPrefix = "lr_stb"
        tbNum = 8
        rowNum = 60 * 24
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 60000
        tdLog.info(f"========== lastrow.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = tbNum
        while i > 0:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            i = i - 1

        ts = ts0
        i = 1
        while i <= tbNum:
            x = 0
            tb = tbPrefix + str(i)
            while x < rowNum:
                ts = ts + delta
                c6 = x % 128
                c3 = "NULL"
                xr = x % 10
                if xr == 0:
                    c3 = x

                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {x} , NULL , {x} , {x} , {x} , {c6} , true, 'BINARY', 'NCHAR' )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"====== test data created")

        self.lastrow_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.lastrow_query()

    def lastrow_query(self):
        dbPrefix = "lr_db"
        tbPrefix = "lr_tb"
        stbPrefix = "lr_stb"
        tbNum = 8
        rowNum = 60 * 24
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 60000
        tdLog.info(f"========== lastrow_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"use {db}")

        tdLog.info(f"========>TD-3231 last_row with group by column error")
        tdSql.query(f"select last_row(c1) from {stb} group by c1;")

        ##### select lastrow from STable with two vnodes, timestamp decreases from tables in vnode0 to tables in vnode1
        tdSql.query(f"select last_row(*) from {stb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2018-09-25 09:00:00")

        tdSql.checkData(0, 1, 1439)

        tdSql.checkData(0, 2, None)

        tdSql.checkData(0, 3, 1439.00000)

        tdSql.checkData(0, 4, 1439.000000000)

        tdSql.checkData(0, 6, 31)

        tdSql.checkData(0, 7, 1)

        tdSql.checkData(0, 8, "BINARY")

        tdSql.checkData(0, 9, "NCHAR")

        # regression test case 1
        tdSql.query(
            f"select count(*) from lr_tb1 where ts>'2018-09-18 08:45:00.1' and ts<'2018-09-18 08:45:00.2'"
        )
        tdSql.checkRows(1)

        # regression test case 2
        tdSql.query(
            f"select count(*) from lr_db0.lr_stb0 where ts>'2018-9-18 8:00:00' and ts<'2018-9-18 14:00:00' interval(1s) fill(NULL);"
        )
        tdSql.checkRows(21600)

        # regression test case 3
        tdSql.query(
            f"select _wstart, t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 1"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 8)

        tdSql.checkData(0, 2, 8)

        tdSql.checkData(0, 3, None)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 9"
        )
        tdSql.checkRows(18)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 12"
        )
        tdSql.checkRows(24)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 25"
        )
        tdSql.checkRows(48)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 25 offset 1"
        )
        tdSql.checkRows(46)

        tdSql.query(
            f"select count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1d) fill(NULL) slimit 2"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1d) fill(NULL) slimit 2 soffset 1"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1d) fill(NULL) slimit 1"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1d) fill(NULL) slimit 1 soffset 1"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1d) fill(NULL) slimit 1 soffset 0"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select t1,t1,count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1s) fill(NULL) slimit 2 soffset 0 limit 250000 offset 1"
        )
        tdSql.checkRows(172798)

        tdSql.query(
            f"select t1,t1,count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1s) fill(NULL) slimit 1 soffset 0 limit 250000 offset 1"
        )
        tdSql.checkRows(86399)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 30"
        )
        tdSql.checkRows(48)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 2"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select t1,t1,count(*),tbname,t1,t1,tbname from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by tbname, t1 interval(1s) fill(NULL) slimit 1 soffset 1 limit 250000 offset 1"
        )
        tdSql.checkRows(86399)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 1"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select t1,t1,count(*),t1,t1 from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' partition by t1 interval(1h) fill(NULL) limit 25 offset 1"
        )
        tdSql.checkRows(46)
