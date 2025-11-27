import datetime
import inspect
import random
import sys
import taos
import time

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, etool

class TestFunLastRow:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ sim case ------------------
    #
    def do_sim_last_row(self):
        self.PareserLastRow()
        tdStream.dropAllStreamsAndDbs()
        self.PareserLastRow2()
        tdStream.dropAllStreamsAndDbs()
        self.ComputeLastRow()
        tdStream.dropAllStreamsAndDbs()

        print("\n")
        print("do_sim_last_row ....................... [passed]")


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
            sql = f"insert into {tb} values "
            while x < rowNum:
                ts = ts + delta
                c6 = x % 128
                c3 = "NULL"
                xr = x % 10
                if xr == 0:
                    c3 = x
                sql += f" ({ts},{x},NULL,{x},{x},{x},{c6},true,'BINARY','NCHAR')"
                x = x + 1 
            i = i + 1
            tdSql.execute(sql)

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
            sql = f"insert into {tb} values "
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql += f"({ms},{x})"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

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

        # init
        sql = f"insert into {tb} values"
        cc = 1 * 3600000
        ms = 1601481600000 + cc
        sql += f" ({ms},10)"

        cc = 3 * 3600000
        ms = 1601481600000 + cc
        sql += f" ({ms},NULL)"


        cc = 5 * 3600000
        ms = 1601481600000 + cc
        sql += f" ({ms},-1)"

        cc = 7 * 3600000
        ms = 1601481600000 + cc
        sql += f" ({ms},null)"

        # execute
        tdSql.execute(sql)

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

    #
    # ------------------ test_last_row.py ------------------
    #
    def insert_datas_and_check_abs(self, tbnums, rownums, time_step, cache_value, dbname="test"):
        tdSql.execute(f"drop database if exists {dbname} ")
        tdLog.info("prepare datas for auto check abs function ")

        tdSql.execute(f"create database {dbname} keep {self.keep_duration} cachemodel {cache_value} ")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(f"create stable {dbname}.stb (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")
        for tbnum in range(tbnums):
            tbname = f"{dbname}.sub_tb_{tbnum}"
            tdSql.execute(f"create table {tbname} using {dbname}.stb tags({tbnum}) ")

            ts = self.ts
            sql = f"insert into  {tbname} values"
            for row in range(rownums):
                ts = self.ts + time_step*row
                c1 = random.randint(0,10000)
                c2 = random.randint(0,100000)
                c3 = random.randint(0,125)
                c4 = random.randint(0,125)
                c5 = random.random()/1.0
                c6 = random.random()/1.0
                c7 = "'true'"
                c8 = "'binary_val'"
                c9 = "'nchar_val'"
                c10 = ts
                sql += f" ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})"
            tdSql.execute(sql)    

        tbnames = ["stb", "sub_tb_1"]
        support_types = ["BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "INT"]
        for tbname in tbnames:
            tdSql.query(f"desc {dbname}.{tbname}")
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                abs_sql = f"select abs({colname}) from {dbname}.{tbname} order by tbname "
                origin_sql = f"select {colname} from {dbname}.{tbname} order by tbname"
                if coltype[1] in support_types:
                    self.check_result_auto(origin_sql , abs_sql)

    def prepare_datas(self ,cache_value, dbname="db"):
        tdSql.execute(f"drop database if exists {dbname} ")
        create_db_sql = f"create database if not exists {dbname} keep 36500 duration 100 cachemodel {cache_value}"
        tdSql.execute(create_db_sql)

        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            '''
        )
        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

    def prepare_tag_datas(self,cache_value, dbname="testdb"):

        tdSql.execute(f"drop database if exists {dbname} ")
        # prepare datas
        tdSql.execute(f"create database if not exists {dbname} keep 36500 duration 100 cachemodel {cache_value}")

        tdSql.execute(f"use {dbname} ")

        tdSql.execute(f"create stable {dbname}.stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp , uc1 int unsigned,\
             uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags( t1 int , t2 bigint , t3 smallint , t4 tinyint , t5 float , t6 double , t7 bool , t8 binary(36)\
                 , t9 nchar(36) , t10 int unsigned , t11 bigint unsigned ,t12 smallint unsigned , t13 tinyint unsigned ,t14 timestamp  ) ")

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(
                f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" ,{111*i}, {1*i},{1*i},{1*i},now())')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a ,{111*i},{1111*i},{i},{i} )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a ,{111*i},{1111*i},{i},{i})"
            )
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a ,0,0,0,0)")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a , 999 , 9999 , 9 , 9)")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a , 999 , 99999 , 9 , 9)")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a ,999 , 99999 , 9 , 9)")

        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            '''
        )
        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

    def check_result_auto(self, origin_query, abs_query):
        abs_result = tdSql.getResult(abs_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result = []

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                elif elem >= 0:
                    elem = elem
                else:
                    elem = -elem
                row_check.append(elem)
            auto_result.append(row_check)

        check_status = True
        for row_index, row in enumerate(abs_result):
            for col_index, elem in enumerate(row):
                if auto_result[row_index][col_index] != elem:
                    check_status = False
        if not check_status:
            tdLog.notice(
                "abs function value has not as expected , sql is \"%s\" " % abs_query)
            sys.exit(1)
        else:
            tdLog.info(
                "abs value check pass , it work as expected ,sql is \"%s\"   " % abs_query)

    def check_errors(self, dbname="testdb"):
        # bug need fix
        tdSql.error(f"select last_row(c1 ,NULL) from {dbname}.t1")

        error_sql_lists = [
            f"select last_row from {dbname}.t1",
            f"select last_row(-+--+c1) from {dbname}.t1",
            f"select last_row(123--123)==1 from {dbname}.t1",
            f"select last_row(c1) as 'd1' from {dbname}.t1",
            #f"select last_row(c1 ,NULL) from {dbname}.t1",
            f"select last_row(,) from {dbname}.t1;",
            f"select last_row(abs(c1) ab from {dbname}.t1)",
            f"select last_row(c1) as int from {dbname}.t1",
            f"select last_row from {dbname}.stb1",
            f"select last_row(123--123)==1 from {dbname}.stb1",
            f"select last_row(c1) as 'd1' from {dbname}.stb1",
            #f"select last_row(c1 ,NULL) from {dbname}.stb1",
            f"select last_row(,) from {dbname}.stb1;",
            f"select last_row(abs(c1) ab from {dbname}.stb1)",
            f"select last_row(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self, dbname="testdb"):
        tdSql.execute(f"use {dbname}")
        tbnames = ["stb1", "t1", "ct1", "ct2"]

        for tbname in tbnames:
            tdSql.query(f"desc {dbname}.{tbname}")
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                col_note = coltype[-1]
                if col_note != "TAG":
                    abs_sql = f"select last_row({colname}) from {dbname}.{tbname}"
                    tdSql.query(abs_sql)

    def basic_abs_function(self, dbname="testdb"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select last_row(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(c6) from {dbname}.ct3")

        # used for regular table

        # bug need fix
        tdSql.query(f"select last_row(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.query(f"select last_row(c1) from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.query(f"select last_row(c1) from {dbname}.stb1")
        tdSql.checkData(0, 0, None)

        # support regular query about last ,first ,last_row
        tdSql.error(f"select last_row(c1,NULL) from {dbname}.t1")
        tdSql.error(f"select last_row(NULL) from {dbname}.t1")
        tdSql.error(f"select last(NULL) from {dbname}.t1")
        tdSql.error(f"select first(NULL) from {dbname}.t1")

        tdSql.query(f"select last_row(c1,123) from {dbname}.t1")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,123)

        tdSql.query(f"select last_row(123) from {dbname}.t1")
        tdSql.checkData(0,0,123)

        tdSql.error(f"select last(c1,NULL) from {dbname}.t1")

        tdSql.query(f"select last(c1,123) from {dbname}.t1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,123)

        tdSql.error(f"select first(c1,NULL) from {dbname}.t1")

        tdSql.query(f"select first(c1,123) from {dbname}.t1")
        tdSql.checkData(0,0,1)
        tdSql.checkData(0,1,123)

        tdSql.error(f"select last_row(c1,c2,c3,NULL,c4) from {dbname}.t1")

        tdSql.query(f"select last_row(c1,c2,c3,123,c4) from {dbname}.t1")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,123)
        tdSql.checkData(0,4,None)

        tdSql.error(f"select last_row(c1,c2,c3,NULL,c4,t1,t2) from {dbname}.ct1")

        tdSql.query(f"select last_row(c1,c2,c3,123,c4,t1,t2) from {dbname}.ct1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,-99999)
        tdSql.checkData(0,2,-999)
        tdSql.checkData(0,3,123)
        tdSql.checkData(0,4,None)
        tdSql.checkData(0,5,0)
        tdSql.checkData(0,5,0)

        # # bug need fix
        tdSql.query(f"select last_row(c1), c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        # # bug need fix
        tdSql.query(f"select last_row(c1), c2, c3 , c4, c5 from {dbname}.ct1")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, -99999)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4,-9.99000)
        
        tdSql.query(f"select last_row(c1), c2, c3 , c4, c5 from (select * from {dbname}.ct1)")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, -99999)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4,-9.99000)

        # bug need fix
        tdSql.query(f"select last_row(c1), c2, c3 , c4, c5 from {dbname}.stb1 where tbname='ct1'")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, -99999)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4,-9.99000)

        # bug fix
        tdSql.query(f"select last_row(abs(c1)) from {dbname}.ct1")
        tdSql.checkData(0,0,9)

        # # bug fix
        tdSql.query(f"select last_row(c1+1) from {dbname}.ct1")
        tdSql.query(f"select last_row(c1+1) from {dbname}.stb1")
        tdSql.query(f"select last_row(c1+1) from {dbname}.t1")

        # used for stable table
        tdSql.query(f"select last_row(c1 ,c2 ,c3) ,last_row(c4) from {dbname}.ct1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,-99999)
        tdSql.checkData(0,2,-999)
        tdSql.checkData(0,3,None)

        # bug need fix
        tdSql.query(f"select last_row(c1 ,c2 ,c3) from {dbname}.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)

        tdSql.query(f'select last_row(c1) from {dbname}.t1 where ts <"2022-12-31 01:01:36.000"')
        tdSql.checkData(0,0,8)
        # bug need fix
        tdSql.query(f"select abs(last_row(c1)-2)+max(c1),ceil(last_row(c4)-2) from {dbname}.stb1 where c4 is not null")
        tdSql.checkData(0,0,16.000000000)
        tdSql.checkData(0,1,-101.000000000)

        tdSql.query(f"select abs(last_row(c1)-2)+max(c1),ceil(last_row(c4)-2) from {dbname}.ct1 where c4<0")
        tdSql.checkData(0,0,16.000000000)
        tdSql.checkData(0,1,-101.000000000)

        tdSql.query(f"select last_row(ceil(c1+2)+floor(c1)-10) from {dbname}.stb1")
        tdSql.checkData(0,0,None)

        tdSql.query(f"select last_row(ceil(c1+2)+floor(c1)-10) from {dbname}.ct1")
        tdSql.checkData(0,0,10.000000000)

        # filter for last_row

        # bug need fix for all function

        tdSql.query(f"select last_row(ts ,c1 ) from {dbname}.ct4 where t1 = 1 ")
        tdSql.checkRows(0)

        tdSql.query(f"select count(c1) from {dbname}.ct4 where t1 = 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)

        tdSql.query(f"select last_row(c1) ,last(c1)  from {dbname}.stb1 where  c1 is null")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)

        tdSql.query(f"select last_row(c1) ,count(*)  from {dbname}.stb1 where  c1 is null")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)

        tdSql.query(f"select last_row(c1) ,count(c1)  from {dbname}.stb1 where  c1 is null")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,0)

        # bug need fix
        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1")
        tdSql.checkData(0,0,'ct4')
        tdSql.checkData(0,1,None)

        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 partition by tbname order by tbname ")
        tdSql.checkData(0,0,'ct1')
        tdSql.checkData(0,1,9)
        tdSql.checkData(1,0,'ct4')
        tdSql.checkData(1,1,None)

        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 group by tbname order by tbname ")
        tdSql.checkData(0,0,'ct1')
        tdSql.checkData(0,1,9)
        tdSql.checkData(1,0,'ct4')
        tdSql.checkData(1,1,None)

        tdSql.query(f"select t1 ,count(c1) from {dbname}.stb1 partition by t1 having count(c1)>0")
        tdSql.checkRows(2)

        # filter by tbname
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 where tbname = 'ct1' ")
        tdSql.checkData(0,0,9)

        # bug need fix
        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 where tbname = 'ct1' ")
        tdSql.checkData(0,1,9)
        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 partition by tbname order by tbname")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 group by tbname order by tbname")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        # last_row for only tag
        tdSql.query(f"select last_row(t1 ,t2 ,t3 , t4 ) from {dbname}.stb1")
        tdSql.checkData(0,0,3)
        tdSql.checkData(0,1,33333)
        tdSql.checkData(0,2,333)
        tdSql.checkData(0,3,3)

        tdSql.query(f"select last_row(abs(floor(t1)) ,t2 ,ceil(abs(t3)) , abs(ceil(t4)) ) from {dbname}.stb1")
        tdSql.checkData(0,0,3)
        tdSql.checkData(0,1,33333)
        tdSql.checkData(0,2,333)
        tdSql.checkData(0,3,3)
        tdSql.query(f"select last_row(abs(floor(t1)) ,t2 ,ceil(abs(t3)) , abs(ceil(t4)) ) from (select * from {dbname}.stb1)")
        tdSql.checkData(0,0,3)
        tdSql.checkData(0,1,33333)
        tdSql.checkData(0,2,333)
        tdSql.checkData(0,3,3)

        # filter by tag
        tdSql.query(f"select tbname ,last_row(c1) from {dbname}.stb1 where t1 =0 ")
        tdSql.checkData(0,1,9)
        tdSql.query(f"select tbname ,last_row(c1) ,t1 from {dbname}.stb1 partition by t1 order by t1")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        # filter by col

        tdSql.query(f"select tbname ,last_row(c1),abs(c1)from {dbname}.stb1 where c1 =1;")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 where abs(ceil(c1))*c1==1")
        tdSql.checkData(0,0,1)

        # mix with common functions
        tdSql.query(f"select last_row(*) ,last(*) from {dbname}.stb1  ")
        tdSql.checkRows(1)

        tdSql.query(f"select last_row(*) ,last(*) from {dbname}.stb1  ")
        tdSql.checkRows(1)

        tdSql.query(f"select last_row(c1+abs(c1)) from {dbname}.stb1 partition by tbname order by tbname")
        tdSql.query(f"select last(c1), max(c1+abs(c1)),last_row(c1+abs(c1)) from {dbname}.stb1 partition by tbname order by tbname")

        # # bug need fix ,taosd crash
        tdSql.error(f"select last_row(*) ,last(*) from {dbname}.stb1 partition by tbname order by last(*)")
        tdSql.error(f"select last_row(*) ,last(*) from {dbname}.stb1 partition by tbname order by last_row(*)")

        # mix with agg functions
        tdSql.query(f"select last(*), last_row(*),last(c1), last_row(c1) from {dbname}.stb1 ")
        tdSql.query(f"select last(*), last_row(*),last(c1), last_row(c1) from {dbname}.ct1 ")
        tdSql.query(f"select last(*), last_row(*),last(c1+1)*max(c1), last_row(c1+2)/2 from {dbname}.t1 ")
        tdSql.query(f"select last_row(*) ,abs(c1/2)+100 from {dbname}.stb1 where tbname =\"ct1\" ")
        tdSql.query(f"select c1, last_row(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, last_row(c5) ,last(c1) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select last(c1) , max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select last_row(c1) , max(c5), count(c5) from {dbname}.ct1")

        # bug fix for compute
        tdSql.query(f"select  last_row(c1) -0 ,last(c1)-0 ,last(c1)+last_row(c1) from {dbname}.ct4 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,0.000000000)
        tdSql.checkData(0,2,None)

        tdSql.query(f"select c1, abs(c1) -0 ,last_row(c1-0.1)-0.1 from {dbname}.ct1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,9.000000000)
        tdSql.checkData(0,2,8.800000000)

    def abs_func_filter(self, dbname="db"):
        tdSql.query(
            f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,last_row(log(c1,2)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 6.000000000)
        tdSql.checkData(0, 2, 6.000000000)
        tdSql.checkData(0, 3, 5.900000000)
        tdSql.checkData(0, 4, 2.084962501)

        tdSql.query(
            f"select last_row(c1,c2,c1+5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 55555)
        tdSql.checkData(0, 2, 10.000000000)

        tdSql.query(
            f"select last(c1,c2,c1+5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 55555)
        tdSql.checkData(0, 2, 10.000000000)

        tdSql.query(
            f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from {dbname}.ct4 where c1>log(c1,2) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, 88888)
        tdSql.checkData(0, 2, 8.000000000)
        tdSql.checkData(0, 3, 8.000000000)
        tdSql.checkData(0, 4, 7.900000000)
        tdSql.checkData(0, 5, 3.000000000)

    def abs_Arithmetic(self):
        pass

    def check_boundary_values(self, dbname="bound_test"):

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname} cachemodel 'LAST_ROW' ")

        time.sleep(3)
        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f"create table {dbname}.stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table {dbname}.sub1_bound using {dbname}.stb_bound tags ( 1 )')
        tdSql.execute(
            f"insert into {dbname}.sub1_bound values ( now()-10s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into {dbname}.sub1_bound values ( now()-5s, -2147483647, -9223372036854775807, -32767, -127, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into {dbname}.sub1_bound values ( now(), 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into {dbname}.sub1_bound values ( now()+5s, -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.error(
            f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )

        # check basic elem for table per row
        tdSql.query(
            f"select last(c1) ,last_row(c2), last_row(c3)+1 , last(c4)+1  from {dbname}.sub1_bound ")
        tdSql.checkData(0, 0, -2147483646)
        tdSql.checkData(0, 1, -9223372036854775806)
        tdSql.checkData(0, 2, -32765.000000000)
        tdSql.checkData(0, 3, -125.000000000)
        # check  + - * / in functions
        tdSql.query(
            f"select last_row(c1+1) ,last_row(c2) , last(c3*1) , last(c4/2)  from {dbname}.sub1_bound ")

    def check_tag_compute_for_scalar_function(self, dbname="testdb"):
        # bug need fix

        tdSql.query(f"select sum(c1) from {dbname}.stb1 where t1+10 >1; ")
        tdSql.query(f"select c1 ,t1 from {dbname}.stb1 where t1 =0 ")
        tdSql.checkRows(13)
        tdSql.query(f"select last_row(c1,t1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.query(f"select last_row(c1),t1 from {dbname}.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.query(f"select last_row(c1,t1),last(t1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.checkData(0,2,3)

        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where t1 >0 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,3)
        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where t1 =3 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,3)

        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where t1 =2")
        tdSql.checkRows(0)

        # nest query for last_row
        tdSql.query(f"select last_row(t1) from (select ts , c1 ,t1 from {dbname}.stb1)")
        tdSql.checkData(0,0,3)
        tdSql.query(f"select distinct(c1) ,t1 from {dbname}.stb1")
        tdSql.checkRows(20)
        tdSql.query(f"select last_row(c1) from (select _rowts , c1 ,t1 from {dbname}.stb1)")
        tdSql.checkData(0,0,None)

        tdSql.query(f"select last_row(c1) from (select ts , c1 ,t1 from {dbname}.stb1)")
        tdSql.checkData(0,0,None)

        tdSql.query(f"select ts , last_row(c1) ,c1  from (select ts , c1 ,t1 from {dbname}.stb1)")
        tdSql.checkData(0,1,None)

        tdSql.query(f"select ts , last_row(c1) ,c1  from (select ts , max(c1) c1  ,t1 from {dbname}.stb1 where ts >now -1h and ts <now+1h interval(10s) fill(value ,10, 10, 10))")
        tdSql.checkData(0,1,9)
        tdSql.checkData(0,1,9)

        tdSql.error(f"select ts , last_row(c1) ,c1  from (select count(c1) c1 from {dbname}.stb1 where ts >now -1h and ts <now+1h interval(10s) fill(value ,10, 10, 10))")

        tdSql.error(f"select  last_row(c1) ,c1  from (select  count(c1) c1 from {dbname}.stb1 where ts >now -1h and ts <now+1h interval(10s) fill(value ,10, 10))")

        # tag filter with last_row function
        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where abs(t1)=1")
        tdSql.checkRows(0)
        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where abs(t1)=0")
        tdSql.checkRows(1)
        tdSql.query(f"select last_row(t1),last_row(c1) from db.ct1 where abs(c1+t1)=1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,1)
        tdSql.checkData(0,1,0)

        tdSql.query(
            f"select last_row(c1+t1)*t1 from {dbname}.stb1 where abs(c1)/floor(abs(ceil(t1))) ==1")

    def group_test(self, dbname="testdb"):
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by t1 order by t1 ")
        tdSql.checkRows(2)

        # bug need fix
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by c1 order by c1,t1 ")
        tdSql.checkRows(10)
        tdSql.checkData(9,0,8)
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by c1 order by t1 ")
        tdSql.checkRows(10)
        tdSql.checkData(0,0,4)

        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by c1 order by t1")
        tdSql.checkRows(11)

        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by c1 order by c1,t1;")
        tdSql.checkRows(11)
        tdSql.checkData(10,0,9)

        # bug need fix , result is error
        tdSql.query(f"select last_row(c1) from {dbname}.ct4 group by c1 order by t1 ")
        tdSql.query(f"select last_row(t1) from {dbname}.ct4 group by c1 order by t1 ")

        tdSql.query(f"select last_row(t1) from {dbname}.stb1 group by t1 order by t1 ")
        tdSql.checkRows(2)
        tdSql.query(f"select last_row(c1) from {dbname}.stb1 group by c1 order by c1 ")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(10,0,9)

        tdSql.query(f"select ceil(abs(last_row(abs(c1)))) from {dbname}.stb1 group by abs(c1) order by abs(c1);")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(10,0,9)
        tdSql.query(f"select last_row(c1+c3) from {dbname}.stb1 group by abs(c1+c3) order by abs(c1+c3)")
        tdSql.checkRows(11)

        # bug need fix , taosd crash
        tdSql.query(f"select last_row(c1+c3)+c2 from {dbname}.stb1 group by abs(c1+c3)+c2 order by abs(c1+c3)+c2")
        tdSql.checkRows(11)
        tdSql.query(f"select last_row(c1+c3)+last_row(c2) from {dbname}.stb1 group by abs(c1+c3)+abs(c2) order by abs(c1+c3)+abs(c2)")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(2,0,11223.000000000)

        tdSql.query(f"select last_row(t1) from {dbname}.stb1 where abs(c1+t1)=1 partition by tbname")
        tdSql.checkData(0,0,1)

        tdSql.query(f"select tbname , last_row(c1) from {dbname}.stb1 partition by tbname order by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1,  9)
        tdSql.checkData(0, 2, 'ct4')
        tdSql.checkData(0, 3, None)

        tdSql.query(f"select tbname , last_row(c1) from {dbname}.stb1 partition by t1 order by t1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1,  9)
        tdSql.checkData(0, 2, 'ct4')
        tdSql.checkData(0, 3, None)

        # bug need fix
        tdSql.query(f"select tbname , last_row(c1) from {dbname}.stb1 partition by c2 order by c1")
        tdSql.checkRows(11)
        tdSql.checkData(10,1,9)

        tdSql.query(f"select tbname , last_row(c1) from {dbname}.stb1 partition by c2 order by c2")
        tdSql.checkRows(11)
        tdSql.checkData(10,1,88888)

        tdSql.query(f"select tbname , last_row(t1) from {dbname}.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)

        tdSql.query(f"select abs(c1) ,c2 ,t1, last_row(t1) from {dbname}.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)

        tdSql.query(f"select t1 ,last_row(t1) ,c2 from {dbname}.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)

        tdSql.query(f"select last_row(t1) ,last_row(t1) ,last_row(c2) from {dbname}.stb1 partition by c2 order by c2")
        tdSql.checkRows(11)

        tdSql.query(f"select abs(c1) , last_row(t1) ,c2 from {dbname}.stb1 partition by tbname order by tbname")
        tdSql.checkRows(2)

        tdSql.query(f"select last_row(c1) , ceil(t1) ,c2 from {dbname}.stb1 partition by t1 order by t1")
        tdSql.checkRows(2)

        tdSql.query(f"select last_row(c1) , abs(t1) ,c2 from {dbname}.stb1 partition by abs(c1) order by abs(c1)")
        tdSql.checkRows(11)

        tdSql.query(f"select abs(last_row(c1)) , abs(floor(t1)) ,floor(c2) from {dbname}.stb1 partition by abs(floor(c1)) order by abs(c1)")
        tdSql.checkRows(11)

        tdSql.query(f"select last_row(ceil(c1-2)) , abs(floor(t1+1)) ,floor(c2-c1) from {dbname}.stb1 partition by abs(floor(c1)) order by abs(c1)")
        tdSql.checkRows(11)

        tdSql.query(f"select max(c1) from {dbname}.stb1 interval(50s) sliding(30s)")
        tdSql.checkRows(13)

        tdSql.query(f"select unique(c1) from {dbname}.stb1 partition by tbname")

        # interval

        tdSql.query(f"select last_row(c1) from {dbname}.stb1 interval(50s) sliding(30s)")
        tdSql.checkRows(27)

        tdSql.query(f"select last_row(c1) from {dbname}.ct1 interval(50s) sliding(30s)")
        tdSql.checkRows(5)
        last_row_result = tdSql.queryResult
        tdSql.query(f"select last(c1) from {dbname}.ct1 interval(50s) sliding(30s)")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        # bug need fix
        tdSql.query(f'select max(c1) from {dbname}.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" interval(50d) sliding(30d) fill(NULL)')
        tdSql.checkRows(8)
        tdSql.checkData(7,0,None)

        tdSql.query(f'select last_row(c1) from {dbname}.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" interval(50d) sliding(30d) fill(value ,2 )')
        tdSql.checkRows(8)
        tdSql.checkData(7,0,2)

        tdSql.query(f'select last_row(c1) from {dbname}.stb1 where ts>="2022-07-06 16:00:00.000 " and ts < "2022-07-06 17:00:00.000 " interval(50s) sliding(30s)')
        tdSql.query(f'select last_row(c1) from (select ts ,  c1  from {dbname}.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" ) interval(10s) sliding(5s)')

        # join
        db1 = "test"
        tdSql.query(f"use {db1}")
        tdSql.query(f"select last(sub_tb_1.c1), last(sub_tb_2.c2) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        tdSql.checkCols(2)
        last_row_result = tdSql.queryResult
        tdSql.query(f"select last_row(sub_tb_1.c1), last_row(sub_tb_2.c2) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")

        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query(f"select last(*), last(*) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")

        last_row_result = tdSql.queryResult
        tdSql.query(f"select last_row(*), last_row(*) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query(f"select last(*), last_row(*) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query(f"select last_row(*), last(*) from {db1}.sub_tb_1 sub_tb_1, {db1}.sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

    def support_super_table_test(self, dbname="testdb"):
        self.check_result_auto( f"select c1 from {dbname}.stb1 order by ts " , f"select abs(c1) from {dbname}.stb1 order by ts" )
        self.check_result_auto( f"select c1 from {dbname}.stb1 order by tbname " , f"select abs(c1) from {dbname}.stb1 order by tbname" )
        self.check_result_auto( f"select c1 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select abs(c1) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( f"select c1 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select abs(c1) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto( f"select t1,c1 from {dbname}.stb1 order by ts " , f"select t1, abs(c1) from {dbname}.stb1 order by ts" )
        self.check_result_auto( f"select t2,c1 from {dbname}.stb1 order by tbname " , f"select t2 ,abs(c1) from {dbname}.stb1 order by tbname" )
        self.check_result_auto( f"select t3,c1 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select t3 ,abs(c1) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( f"select t4,c1 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select t4 , abs(c1) from {dbname}.stb1 where c1 > 0 order by tbname" )

    def basic_query(self):

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: abs basic query ============")

        self.basic_abs_function()

        tdLog.printNoPrefix("==========step5: abs boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step6: abs filter query ============")

        self.abs_func_filter()

        tdLog.printNoPrefix("==========step6: tag coumpute query ============")

        self.check_tag_compute_for_scalar_function()

        tdLog.printNoPrefix("==========step7: check result of query ============")

        tdLog.printNoPrefix("==========step8: check abs result of  stable query ============")

        self.support_super_table_test()

    def initLastRowDelayTest(self, dbname="db"):
        tdSql.execute(f"drop database if exists {dbname} ")
        create_db_sql = f"create database if not exists {dbname} keep 36500 duration 100 cachemodel 'NONE' REPLICA 1"
        tdSql.execute(create_db_sql)

        time.sleep(3)
        tdSql.execute(f"use {dbname}")
        tdSql.execute(f'create stable {dbname}.st(ts timestamp, v_int int, v_float float) TAGS (ctname varchar(32))')

        tdSql.execute(f"create table {dbname}.ct1 using {dbname}.st tags('ct1')")
        tdSql.execute(f"create table {dbname}.ct2 using {dbname}.st tags('ct2')")

        tdSql.execute(f"insert into {dbname}.st(tbname,ts,v_float, v_int) values('ct1',1630000000000,86,86)")
        tdSql.execute(f"insert into {dbname}.st(tbname,ts,v_float, v_int) values('ct1',1630000021255,59,59)")
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute(f'select last(*) from {dbname}.st')
        tdSql.execute(f'select last_row(*) from {dbname}.st')
        tdSql.execute(f"insert into {dbname}.st(tbname,ts) values('ct1',1630000091255)")
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute(f'select last(*) from {dbname}.st')
        tdSql.execute(f'select last_row(*) from {dbname}.st')
        tdSql.execute(f'alter database {dbname} cachemodel "both"')
        tdSql.query(f'select last(*) from {dbname}.st')
        tdSql.checkData(0 , 1 , 59)

        tdSql.query(f'select last_row(*) from {dbname}.st')
        tdSql.checkData(0 , 1 , None)
        tdSql.checkData(0 , 2 , None)

        tdLog.printNoPrefix("========== delay test init success ==============")

    def lastRowDelayTest(self, dbname="db"):
        tdLog.printNoPrefix("========== delay test start ==============")

        tdSql.execute(f"use {dbname}")

        tdSql.query(f'select last(*) from {dbname}.st')
        tdSql.checkData(0 , 1 , 59)

        tdSql.query(f'select last_row(*) from {dbname}.st')
        tdSql.checkData(0 , 1 , None)
        tdSql.checkData(0 , 2 , None)

    def lastrow_in_subquery(self, dbname="db"):
        tdSql.execute(f'create database if not exists {dbname};')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(f'drop table if exists {dbname}.meters')
        
        tdSql.execute(f'create table {dbname}.meters (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool) tags (t1 nchar(30))')
        tdSql.execute(f'create table {dbname}.d0 using {dbname}.meters tags("st1")')
        tdSql.execute(f'create table {dbname}.d1 using {dbname}.meters tags("st2")')
        tdSql.execute(f'insert into {dbname}.d0 values(1734574929000, 1, 1, "c2", true)')
        tdSql.execute(f'insert into {dbname}.d0 values(1734574929001, 2, 2, "bbbbbbbbb1", false)')
        tdSql.execute(f'insert into {dbname}.d0 values(1734574929002, 2, 2, "bbbbbbbbb1", false)')
        tdSql.execute(f'insert into {dbname}.d0 values(1734574929003, 3, 3, "a2", true)')
        tdSql.execute(f'insert into {dbname}.d0 values(1734574929004, 4, 4, "bbbbbbbbb2", false)')
        tdSql.execute(f'insert into {dbname}.d1 values(1734574929000, 1, 1, "c2", true)')
        tdSql.execute(f'use {dbname}')        
        tdSql.execute(f'Create table  {dbname}.normal_table (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool)')
        tdSql.execute(f'insert into {dbname}.normal_table (select * from {dbname}.d0)')
        
        tdSql.query(f'select count(1), last(ts), last_row(c0) from (select * from {dbname}.meters)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last(ts), last_row(c0) from (select * from {dbname}.meters order by ts desc)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last(ts), last_row(c0) from (select * from {dbname}.meters order by ts asc)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last(ts), last_row(c0) from (select * from {dbname}.meters order by c0 asc)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last_row(ts), last_row(c0) from (select * from {dbname}.meters)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last_row(ts), last_row(c0) from (select * from (select * from {dbname}.meters))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select tbname, last_row(ts), last_row(c0) from (select *, tbname from {dbname}.meters) group by tbname order by tbname')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'd0')
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 'd1')
        tdSql.checkData(1, 1, 1734574929000)
        tdSql.checkData(1, 2, 1)
        tdSql.query(f'select tbname, last_row(ts), last_row(c0) from (select * from  (select *, tbname from {dbname}.meters)) group by tbname order by tbname')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'd0')
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, 'd1')
        tdSql.checkData(1, 1, 1734574929000)
        tdSql.checkData(1, 2, 1)
        tdSql.query(f'select count(1), last_row(ts), last_row(c0) from (select * from {dbname}.d0)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        tdSql.query(f'select count(1), last_row(ts), last_row(c0) from (select * from {dbname}.normal_table)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 1734574929004)
        tdSql.checkData(0, 2, 4)
        
        sql = f"insert into {dbname}.d0 values"
        sql += f' (1734574930000, 1, 1, "c2", true)'
        sql += f' (1734574931000, 1, 1, "c2", true)'
        tdSql.execute(sql)
        tdSql.execute(f'insert into {dbname}.d0 values (1734574932000, 1, 1, "c2", true)')

        tdSql.query(f'select last_row(_wstart) from (select _wstart, _wend, count(1) from {dbname}.meters interval(1s))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.query(f'select last_row(_wstart), count(1) from (select _wstart, _wend, count(1) from {dbname}.meters interval(1s))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.checkData(0, 1, 4)
        tdSql.query(f'select last_row(_wstart) from (select _wstart, _wend, count(1) from {dbname}.meters partition by tbname interval(1s))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.query(f'select last_row(_wstart), count(1) from (select _wstart, _wend, count(1) from {dbname}.meters  partition by tbname interval(1s))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.checkData(0, 1, 5)
        tdSql.query(f'select first(_wstart), count(1) from (select _wstart, _wend, count(1) from {dbname}.meters interval(1s))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 4)
        
        tdSql.query(f'select last_row(_wstart) from (select * from (select _wstart, _wend, count(1) from {dbname}.meters interval(1s)))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.query(f'select last_row(_wstart), count(1) from (select * from (select _wstart, _wend, count(1) from {dbname}.meters interval(1s)))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.checkData(0, 1, 4)
        tdSql.query(f'select last_row(_wstart) from (select * from (select _wstart, _wend, count(1) from {dbname}.meters partition by tbname interval(1s)))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.query(f'select last_row(_wstart), count(1) from (select * from (select _wstart, _wend, count(1) from {dbname}.meters  partition by tbname interval(1s)))')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574932000)
        tdSql.checkData(0, 1, 5)  
        
    def do_last_row(self):
        # init
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000
        self.keep_duration = 36500

        # do
        tdLog.printNoPrefix("==========step1:create table ==============")
        self.initLastRowDelayTest("DELAYTEST")

        # cache_last 0
        self.prepare_datas("'NONE' ")
        self.prepare_tag_datas("'NONE'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'NONE'")
        self.basic_query()

        # cache_last 1
        self.prepare_datas("'LAST_ROW'")
        self.prepare_tag_datas("'LAST_ROW'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'LAST_ROW'")
        self.basic_query()

        # cache_last 2
        self.prepare_datas("'LAST_VALUE'")
        self.prepare_tag_datas("'LAST_VALUE'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'LAST_VALUE'")
        self.basic_query()

        # cache_last 3
        self.prepare_datas("'BOTH'")
        self.prepare_tag_datas("'BOTH'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'BOTH'")
        self.basic_query()
        self.lastRowDelayTest("DELAYTEST")        
        self.lastrow_in_subquery("db1")

        print("do_last_row ........................... [passed]")

    #
    # ----------------- test_func_paramnum.py  -----------------
    #        
    def create_table(self):
        etool.benchMark(command = "-l 1000 -n 1 -d ts_5798")

    def do_func_paramnum(self):
        self.create_table()

        tdSql.query("select last_row(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38, c39, c40, c41, c42, c43, c44, c45, c46, c47, c48, c49, c50, c51, c52, c53, c54, c55, c56, c57, c58, c59, c60, c61, c62, c63, c64, c65, c66, c67, c68, c69, c70, c71, c72, c73, c74, c75, c76, c77, c78, c79, c80, c81, c82, c83, c84, c85, c86, c87, c88, c89, c90, c91, c92, c93, c94, c95, c96, c97, c98, c99, c100, c101, c102, c103, c104, c105, c106, c107, c108, c109, c110, c111, c112, c113, c114, c115, c116, c117, c118, c119, c120, c121, c122, c123, c124, c125, c126, c127, c128, c129, c130, c131, c132, c133, c134, c135, c136, c137, c138, c139, c140, c141, c142, c143, c144, c145, c146, c147, c148, c149, c150, c151, c152, c153, c154, c155, c156, c157, c158, c159, c160, c161, c162, c163, c164, c165, c166, c167, c168, c169, c170, c171, c172, c173, c174, c175, c176, c177, c178, c179, c180, c181, c182, c183, c184, c185, c186, c187, c188, c189, c190, c191, c192, c193, c194, c195, c196, c197, c198, c199, c200, c201, c202, c203, c204, c205, c206, c207, c208, c209, c210, c211, c212, c213, c214, c215, c216, c217, c218, c219, c220, c221, c222, c223, c224, c225, c226, c227, c228, c229, c230, c231, c232, c233, c234, c235, c236, c237, c238, c239, c240, c241, c242, c243, c244, c245, c246, c247, c248, c249, c250, c251, c252, c253, c254, c255, c256, c257, c258, c259, c260, c261, c262, c263, c264, c265, c266, c267, c268, c269, c270, c271, c272, c273, c274, c275, c276, c277, c278, c279, c280, c281, c282, c283, c284, c285, c286, c287, c288, c289, c290, c291, c292, c293, c294, c295, c296, c297, c298, c299, c300, c301, c302, c303, c304, c305, c306, c307, c308, c309, c310, c311, c312, c313, c314, c315, c316, c317, c318, c319, c320, c321, c322, c323, c324, c325, c326, c327, c328, c329, c330, c331, c332, c333, c334, c335, c336, c337, c338, c339, c340, c341, c342, c343, c344, c345, c346, c347, c348, c349, c350, c351, c352, c353, c354, c355, c356, c357, c358, c359, c360, c361, c362, c363, c364, c365, c366, c367, c368, c369, c370, c371, c372, c373, c374, c375, c376, c377, c378, c379, c380, c381, c382, c383, c384, c385, c386, c387, c388, c389, c390, c391, c392, c393, c394, c395, c396, c397, c398, c399, c400, c401, c402, c403, c404, c405, c406, c407, c408, c409, c410, c411, c412, c413, c414, c415, c416, c417, c418, c419, c420, c421, c422, c423, c424, c425, c426, c427, c428, c429, c430, c431, c432, c433, c434, c435, c436, c437, c438, c439, c440, c441, c442, c443, c444, c445, c446, c447, c448, c449, c450, c451, c452, c453, c454, c455, c456, c457, c458, c459, c460, c461, c462, c463, c464, c465, c466, c467, c468, c469, c470, c471, c472, c473, c474, c475, c476, c477, c478, c479, c480, c481, c482, c483, c484, c485, c486, c487, c488, c489, c490, c491, c492, c493, c494, c495, c496, c497, c498, c499, c500, c501, c502, c503, c504, c505, c506, c507, c508, c509, c510, c511, c512, c513, c514, c515, c516, c517, c518, c519, c520)  from ts_5798.meters;")
        tdSql.checkRows(1)
        tdSql.query("select first(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38, c39, c40, c41, c42, c43, c44, c45, c46, c47, c48, c49, c50, c51, c52, c53, c54, c55, c56, c57, c58, c59, c60, c61, c62, c63, c64, c65, c66, c67, c68, c69, c70, c71, c72, c73, c74, c75, c76, c77, c78, c79, c80, c81, c82, c83, c84, c85, c86, c87, c88, c89, c90, c91, c92, c93, c94, c95, c96, c97, c98, c99, c100, c101, c102, c103, c104, c105, c106, c107, c108, c109, c110, c111, c112, c113, c114, c115, c116, c117, c118, c119, c120, c121, c122, c123, c124, c125, c126, c127, c128, c129, c130, c131, c132, c133, c134, c135, c136, c137, c138, c139, c140, c141, c142, c143, c144, c145, c146, c147, c148, c149, c150, c151, c152, c153, c154, c155, c156, c157, c158, c159, c160, c161, c162, c163, c164, c165, c166, c167, c168, c169, c170, c171, c172, c173, c174, c175, c176, c177, c178, c179, c180, c181, c182, c183, c184, c185, c186, c187, c188, c189, c190, c191, c192, c193, c194, c195, c196, c197, c198, c199, c200, c201, c202, c203, c204, c205, c206, c207, c208, c209, c210, c211, c212, c213, c214, c215, c216, c217, c218, c219, c220, c221, c222, c223, c224, c225, c226, c227, c228, c229, c230, c231, c232, c233, c234, c235, c236, c237, c238, c239, c240, c241, c242, c243, c244, c245, c246, c247, c248, c249, c250, c251, c252, c253, c254, c255, c256, c257, c258, c259, c260, c261, c262, c263, c264, c265, c266, c267, c268, c269, c270, c271, c272, c273, c274, c275, c276, c277, c278, c279, c280, c281, c282, c283, c284, c285, c286, c287, c288, c289, c290, c291, c292, c293, c294, c295, c296, c297, c298, c299, c300, c301, c302, c303, c304, c305, c306, c307, c308, c309, c310, c311, c312, c313, c314, c315, c316, c317, c318, c319, c320, c321, c322, c323, c324, c325, c326, c327, c328, c329, c330, c331, c332, c333, c334, c335, c336, c337, c338, c339, c340, c341, c342, c343, c344, c345, c346, c347, c348, c349, c350, c351, c352, c353, c354, c355, c356, c357, c358, c359, c360, c361, c362, c363, c364, c365, c366, c367, c368, c369, c370, c371, c372, c373, c374, c375, c376, c377, c378, c379, c380, c381, c382, c383, c384, c385, c386, c387, c388, c389, c390, c391, c392, c393, c394, c395, c396, c397, c398, c399, c400, c401, c402, c403, c404, c405, c406, c407, c408, c409, c410, c411, c412, c413, c414, c415, c416, c417, c418, c419, c420, c421, c422, c423, c424, c425, c426, c427, c428, c429, c430, c431, c432, c433, c434, c435, c436, c437, c438, c439, c440, c441, c442, c443, c444, c445, c446, c447, c448, c449, c450, c451, c452, c453, c454, c455, c456, c457, c458, c459, c460, c461, c462, c463, c464, c465, c466, c467, c468, c469, c470, c471, c472, c473, c474, c475, c476, c477, c478, c479, c480, c481, c482, c483, c484, c485, c486, c487, c488, c489, c490, c491, c492, c493, c494, c495, c496, c497, c498, c499, c500, c501, c502, c503, c504, c505, c506, c507, c508, c509, c510, c511, c512, c513, c514, c515, c516, c517, c518, c519, c520)  from ts_5798.meters;")
        tdSql.checkRows(1)
        tdSql.query("select last(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38, c39, c40, c41, c42, c43, c44, c45, c46, c47, c48, c49, c50, c51, c52, c53, c54, c55, c56, c57, c58, c59, c60, c61, c62, c63, c64, c65, c66, c67, c68, c69, c70, c71, c72, c73, c74, c75, c76, c77, c78, c79, c80, c81, c82, c83, c84, c85, c86, c87, c88, c89, c90, c91, c92, c93, c94, c95, c96, c97, c98, c99, c100, c101, c102, c103, c104, c105, c106, c107, c108, c109, c110, c111, c112, c113, c114, c115, c116, c117, c118, c119, c120, c121, c122, c123, c124, c125, c126, c127, c128, c129, c130, c131, c132, c133, c134, c135, c136, c137, c138, c139, c140, c141, c142, c143, c144, c145, c146, c147, c148, c149, c150, c151, c152, c153, c154, c155, c156, c157, c158, c159, c160, c161, c162, c163, c164, c165, c166, c167, c168, c169, c170, c171, c172, c173, c174, c175, c176, c177, c178, c179, c180, c181, c182, c183, c184, c185, c186, c187, c188, c189, c190, c191, c192, c193, c194, c195, c196, c197, c198, c199, c200, c201, c202, c203, c204, c205, c206, c207, c208, c209, c210, c211, c212, c213, c214, c215, c216, c217, c218, c219, c220, c221, c222, c223, c224, c225, c226, c227, c228, c229, c230, c231, c232, c233, c234, c235, c236, c237, c238, c239, c240, c241, c242, c243, c244, c245, c246, c247, c248, c249, c250, c251, c252, c253, c254, c255, c256, c257, c258, c259, c260, c261, c262, c263, c264, c265, c266, c267, c268, c269, c270, c271, c272, c273, c274, c275, c276, c277, c278, c279, c280, c281, c282, c283, c284, c285, c286, c287, c288, c289, c290, c291, c292, c293, c294, c295, c296, c297, c298, c299, c300, c301, c302, c303, c304, c305, c306, c307, c308, c309, c310, c311, c312, c313, c314, c315, c316, c317, c318, c319, c320, c321, c322, c323, c324, c325, c326, c327, c328, c329, c330, c331, c332, c333, c334, c335, c336, c337, c338, c339, c340, c341, c342, c343, c344, c345, c346, c347, c348, c349, c350, c351, c352, c353, c354, c355, c356, c357, c358, c359, c360, c361, c362, c363, c364, c365, c366, c367, c368, c369, c370, c371, c372, c373, c374, c375, c376, c377, c378, c379, c380, c381, c382, c383, c384, c385, c386, c387, c388, c389, c390, c391, c392, c393, c394, c395, c396, c397, c398, c399, c400, c401, c402, c403, c404, c405, c406, c407, c408, c409, c410, c411, c412, c413, c414, c415, c416, c417, c418, c419, c420, c421, c422, c423, c424, c425, c426, c427, c428, c429, c430, c431, c432, c433, c434, c435, c436, c437, c438, c439, c440, c441, c442, c443, c444, c445, c446, c447, c448, c449, c450, c451, c452, c453, c454, c455, c456, c457, c458, c459, c460, c461, c462, c463, c464, c465, c466, c467, c468, c469, c470, c471, c472, c473, c474, c475, c476, c477, c478, c479, c480, c481, c482, c483, c484, c485, c486, c487, c488, c489, c490, c491, c492, c493, c494, c495, c496, c497, c498, c499, c500, c501, c502, c503, c504, c505, c506, c507, c508, c509, c510, c511, c512, c513, c514, c515, c516, c517, c518, c519, c520)  from ts_5798.meters;")
        tdSql.checkRows(1)
        
        print("do max param num ...................... [passed]")
        

    #
    # ------------------ main ------------------
    #
    def test_func_select_last_row(self):
        """ Fun: last_row()

        1. Including time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        2. Set cacheModel = both and retest.
        3. Query on super/child/normal table
        4. Support types
        5. Error cases
        6. Query with filter conditions
        7. Query with group by
        8. Query with empty table
        9. Query with subquery
        10. Query with 512 parameters
        11. Check boundary values

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/last_row.sim
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow.sim
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow2.sim
            - 2025-9-28 Alex  Duan Migrated from uncatalog/system-test/2-query/test_last_row.py
            - 2025-10-20 Alex  Duan Migrated from uncatalog/army/query/function/test_func_paramnum.py
        """

        self.do_sim_last_row()
        self.do_last_row()
        self.do_func_paramnum()