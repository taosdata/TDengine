import time
from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestSelectLastRow:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_last_row(self):
        """Select: Last_row

        Test the Last_row function,
        1. Including time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        2. Set cacheModel = both and retest.

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow.sim
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow2.sim
            - 2025-4-28 Simon Guan Migrated from tsim/compute/last_row.sim

        """

        self.PareserLastRow()
        tdStream.dropAllStreamsAndDbs()
        self.PareserLastRow2()
        tdStream.dropAllStreamsAndDbs()
        self.ComputeLastRow()
        tdStream.dropAllStreamsAndDbs()

    def PareserLastRow(self):
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

    def PareserLastRow2(self):
        tdSql.execute(f"create database d1;")
        tdSql.execute(f"use d1;")

        tdLog.info(f"========>td-1317, empty table last_row query crashed")
        tdSql.execute(f"drop table if exists m1;")
        tdSql.execute(f"create table m1(ts timestamp, k int) tags (a int);")
        tdSql.execute(f"create table t1 using m1 tags(1);")
        tdSql.execute(f"create table t2 using m1 tags(2);")

        tdSql.query(f"select last_row(*) from t1")
        tdSql.checkRows(0)

        tdSql.query(f"select last_row(*) from m1")
        tdSql.checkRows(0)

        tdSql.query(f"select last_row(*) from m1 where tbname in ('t1')")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into t1 values('2019-1-1 1:1:1', 1);")
        tdLog.info(
            f"===================> last_row query against normal table along with ts/tbname"
        )
        tdSql.query(f"select last_row(*),ts,'k' from t1;")
        tdSql.checkRows(1)

        tdLog.info(
            f"===================> last_row + user-defined column + normal tables"
        )
        tdSql.query(f"select last_row(ts), 'abc', 1234.9384, ts from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "abc")
        tdSql.checkData(0, 2, 1234.938400000)
        tdSql.checkData(0, 3, "2019-01-01 01:01:01")

        tdLog.info(
            f"===================> last_row + stable + ts/tag column + condition + udf"
        )
        tdSql.query(f"select last_row(*), ts, 'abc', 123.981, tbname from m1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, "2019-01-01 01:01:01")
        tdSql.checkData(0, 3, "abc")
        tdSql.checkData(0, 4, 123.981000000)

        tdSql.execute(f"create table tu(ts timestamp, k int)")
        tdSql.query(f"select last_row(*) from tu")
        tdSql.checkRows(0)

        tdLog.info(f"=================== last_row + nested query")
        tdSql.execute(f"create table lr_nested(ts timestamp, f int)")
        tdSql.execute(f"insert into lr_nested values(now, 1)")
        tdSql.execute(f"insert into lr_nested values(now+1s, null)")
        tdSql.query(f"select last_row(*) from (select * from lr_nested)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

    def ComputeLastRow(self):
        dbPrefix = "m_la_db"
        tbPrefix = "m_la_tb"
        mtPrefix = "m_la_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select last_row(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"select last_row(tbcol) from {tb} where ts <= {ms}")
        tdSql.query(f"select last_row(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select last_row(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select last_row(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select last_row(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select last_row(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select last_row(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select last_row(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")

        cc = 1 * 3600000
        ms = 1601481600000 + cc
        tdSql.execute(f"insert into {tb} values( {ms} , 10)")

        cc = 3 * 3600000
        ms = 1601481600000 + cc
        tdSql.execute(f"insert into {tb} values( {ms} , null)")

        cc = 5 * 3600000
        ms = 1601481600000 + cc

        tdSql.execute(f"insert into {tb} values( {ms} , -1)")

        cc = 7 * 3600000
        ms = 1601481600000 + cc

        tdSql.execute(f"insert into {tb} values( {ms} , null)")

        ## for super table
        cc = 6 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, -1)

        cc = 8 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select last_row(*) from {mt}")
        tdSql.checkData(0, 1, None)

        cc = 4 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts < {ms}")
        tdSql.checkData(0, 1, None)

        cc = 1 * 3600000
        ms1 = 1601481600000 + cc
        cc = 4 * 3600000
        ms2 = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {mt} where ts > {ms1} and ts <= {ms2}")
        tdSql.checkData(0, 1, None)

        ## for table
        cc = 6 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, -1)

        cc = 8 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select last_row(*) from {tb}")
        tdSql.checkData(0, 1, None)

        cc = 4 * 3600000
        ms = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts <= {ms}")
        tdSql.checkData(0, 1, None)

        cc = 1 * 3600000
        ms1 = 1601481600000 + cc
        cc = 4 * 3600000
        ms2 = 1601481600000 + cc

        tdSql.query(f"select last_row(*) from {tb} where ts > {ms1} and ts <= {ms2}")
        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=======================> regresss bug in last_row query")
        tdSql.execute(f"drop database if exists db;")
        tdSql.prepare("db", cachemodel="both", vgroups=1)

        tdSql.execute(f"create table db.stb (ts timestamp, c0 bigint) tags(t1 int);")
        tdSql.execute(
            f"insert into db.stb_0 using db.stb tags(1) values ('2023-11-23 19:06:40.000', 491173569);"
        )
        tdSql.execute(
            f"insert into db.stb_2 using db.stb tags(3) values ('2023-11-25 19:30:00.000', 2080726142);"
        )
        tdSql.execute(
            f"insert into db.stb_3 using db.stb tags(4) values ('2023-11-26 06:48:20.000', 1907405128);"
        )
        tdSql.execute(
            f"insert into db.stb_4 using db.stb tags(5) values ('2023-11-24 22:56:40.000', 220783803);"
        )

        tdSql.execute(f"create table db.stb_1 using db.stb tags(2);")
        tdSql.execute(f"insert into db.stb_1 (ts) values('2023-11-26 13:11:40.000');")
        tdSql.execute(
            f"insert into db.stb_1 (ts, c0) values('2023-11-26 13:11:39.000', 11);"
        )

        tdSql.query(f"select tbname,ts,last_row(c0) from db.stb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb_1")
        tdSql.checkData(0, 1, "2023-11-26 13:11:40")
        tdSql.checkData(0, 2, None)

        tdSql.execute(f"alter database db cachemodel 'none';")
        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select tbname,last_row(c0, ts) from db.stb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb_1")
        tdSql.checkData(0, 2, "2023-11-26 13:11:40")
        tdSql.checkData(0, 1, None)
