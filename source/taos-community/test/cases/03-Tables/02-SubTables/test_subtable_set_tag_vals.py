from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubTableSetTagVals:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_set_subtable_set_tag_vals(self):
        """Set tag vals

        1. Modify tag values and insert data, including setting tags with NULL values
        2. Perform filtered queries on the super table using the modified tag values
        3. Modify multiple tag values simultaneously

        Catalog:
            - Table:SubTable

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/parser/set_tag_vals.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/alter1.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/set.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/change_multi_tag.sim

        """

        self.SetTagVals()
        tdStream.dropAllStreamsAndDbs()
        self.Alter1()
        tdStream.dropAllStreamsAndDbs()
        self.TagSet()
        tdStream.dropAllStreamsAndDbs()
        self.ChangeMultiTags()
        tdStream.dropAllStreamsAndDbs()

    def SetTagVals(self):
        dbPrefix = "db"
        tbPrefix = "tb"
        stbPrefix = "stb"
        tbNum = 100
        rowNum = 100
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== alter_tg.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create {tbNum} tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 binary(9), t2 binary(8))"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < tbNum:
            tb = tbPrefix + str(i)
            tgstr = "'tb" + str(i) + "'"
            tgstr1 = "'usr'"
            tdSql.execute(f"create table {tb} using {stb} tags( {tgstr} , {tgstr1} )")

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
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")
        tdSql.query(f"show tables")
        tdSql.checkRows(tbNum)

        tdLog.info(f"================== alter table add tags")
        tdSql.execute(f"alter table {stb} add tag t3 int")
        tdSql.execute(f"alter table {stb} add tag t4 binary(60)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} (ts, c1) values (now-100a, {i} )")
            tdSql.execute(f"alter table {tb} set tag t3 = {i}")
            tdSql.execute(f"insert into {tb} (ts, c1) values (now, {i} )")

            name = "'" + str(i) + "'"
            tdSql.execute(f"alter table {tb} set tag t4 = {name}")
            i = i + 1

        tdLog.info(f"================== all tags have been changed!")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select tbname from {stb} where t3 = 'NULL'")

        tdLog.info(f"================== set tag to NULL")
        tdSql.execute(
            f"create table stb1_tg (ts timestamp, c1 int) tags(t1 int,t2 bigint,t3 double,t4 float,t5 smallint,t6 tinyint)"
        )
        tdSql.execute(
            f"create table stb2_tg (ts timestamp, c1 int) tags(t1 bool,t2 binary(10), t3 nchar(10))"
        )
        tdSql.execute(f"create table tb1_tg1 using stb1_tg tags(1,2,3,4,5,6)")
        tdSql.execute(
            f"create table tb1_tg2 using stb2_tg tags(true, 'tb1_tg2', '表1标签2')"
        )
        tdSql.execute(f"insert into tb1_tg1 values (now,1)")
        tdSql.execute(f"insert into tb1_tg2 values (now,1)")

        tdSql.query(f"select * from stb1_tg")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 3.000000000)
        tdSql.checkData(0, 5, 4.00000)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 6)

        tdSql.query(f"select * from stb2_tg")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "tb1_tg2")
        tdSql.checkData(0, 4, "表1标签2")

        tdSql.execute(f"alter table tb1_tg1 set tag t1 = -1")
        tdSql.execute(f"alter table tb1_tg1 set tag t2 = -2")
        tdSql.execute(f"alter table tb1_tg1 set tag t3 = -3")
        tdSql.execute(f"alter table tb1_tg1 set tag t4 = -4")
        tdSql.execute(f"alter table tb1_tg1 set tag t5 = -5")
        tdSql.execute(f"alter table tb1_tg1 set tag t6 = -6")
        tdSql.execute(f"alter table tb1_tg2 set tag t1 = false")
        tdSql.execute(f"alter table tb1_tg2 set tag t2 = 'binary2'")
        tdSql.execute(f"alter table tb1_tg2 set tag t3 = '涛思'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from stb1_tg")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(0, 3, -2)
        tdSql.checkData(0, 4, -3.000000000)
        tdSql.checkData(0, 5, -4.00000)
        tdSql.checkData(0, 6, -5)
        tdSql.checkData(0, 7, -6)

        tdSql.query(f"select * from stb2_tg")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "binary2")
        tdSql.checkData(0, 4, "涛思")

        tdSql.execute(f"alter table tb1_tg1 set tag t1 = NULL")
        tdSql.execute(f"alter table tb1_tg1 set tag t2 = NULL")
        tdSql.execute(f"alter table tb1_tg1 set tag t3 = NULL")
        tdSql.execute(f"alter table tb1_tg1 set tag t4 = NULL")
        tdSql.execute(f"alter table tb1_tg1 set tag t5 = NULL")
        tdSql.execute(f"alter table tb1_tg1 set tag t6 = NULL")
        tdSql.execute(f"alter table tb1_tg2 set tag t1 = NULL")
        tdSql.execute(f"alter table tb1_tg2 set tag t2 = NULL")
        tdSql.execute(f"alter table tb1_tg2 set tag t3 = NULL")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from stb1_tg")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)
        tdSql.checkData(0, 7, None)

        tdSql.query(f"select * from stb2_tg")
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)

    def Alter1(self):
        dbPrefix = "alt1_db"

        tdLog.info(f"========== alter1.sim")
        db = dbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, speed double, mileage double) tags(carId int, carModel int)"
        )
        tdSql.execute(f"create table car1 using stb tags (1, 1)")
        tdSql.execute(f"create table car2 using stb tags (2, 1)")
        tdSql.execute(f"create table car3 using stb tags (3, 2)")
        tdSql.execute(f"insert into car1 values (now-1s, 100, 10000)")
        tdSql.execute(f"insert into car2 values (now, 100, 10000)")
        tdSql.execute(f"insert into car3 values (now, 100, 10000)")
        tdSql.execute(f"insert into car1 values (now, 120, 11000)")
        tdLog.info(f"================== add a column")
        tdSql.execute(f"alter table stb add column c1 int")
        tdSql.query(f"describe stb")
        tdSql.checkRows(6)

        tdSql.query(f"select * from stb")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 3, None)

        tdSql.query(f"select c1 from stb")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select c1+speed from stb")
        tdSql.checkRows(4)

        tdSql.query(f"select c1+speed from car1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select * from car1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)

        tdLog.info(f"================== insert values into table")
        tdSql.execute(
            f"insert into car1 values (now, 1, 1,1 ) (now +1s, 2,2,2) car2 values (now, 1,3,3)"
        )

        tdSql.query(f"select c1+speed from stb where c1 > 0")
        tdSql.checkRows(3)

        tdLog.info(f"================== add a tag")
        tdSql.execute(f"alter table stb add tag t1 int")
        tdSql.query(f"describe stb")
        tdSql.checkRows(7)
        tdSql.checkData(6, 0, "t1")

        tdLog.info(f"================== change a tag value")
        tdSql.execute(f"alter table car1 set tag carid=10")
        tdSql.query(f"select distinct carId, carmodel from car1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select * from stb where carid = 10")
        tdSql.checkRows(4)

        tdSql.execute(f"alter table car2 set tag carmodel = 2")
        tdSql.query(f"select * from stb where carmodel = 2")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)

    def TagSet(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_se_db"
        tbPrefix = "ta_se_tb"
        mtPrefix = "ta_se_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.error(f"alter table {tb} set tag tagcx 1")
        tdSql.execute(f"alter table {tb} set tag tgcol1=false")
        tdSql.execute(f"alter table {tb} set tag tgcol2=4")

        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol1 = false")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from {mt} where tgcol2 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"describe {tb}")
        tdLog.info(
            f"{tdSql.getData(2,1)} {tdSql.getData(2,3)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
        )
        tdSql.checkData(2, 1, "BOOL")
        tdSql.checkData(3, 1, "INT")
        tdSql.checkData(2, 3, "TAG")
        tdSql.checkData(3, 3, "TAG")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {tb} set tag tgcol1=3")
        tdSql.execute(f"alter table {tb} set tag tgcol2=4")

        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol1 = 3")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from {mt} where tgcol2 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {tb} set tag tgcol1=3")
        tdSql.execute(f"alter table {tb} set tag tgcol2=4")

        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol1 = 3")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from {mt} where tgcol2 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {tb} set tag tgcol1=3")
        tdSql.execute(f"alter table {tb} set tag tgcol2='4'")

        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol1 = 3")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from {mt} where tgcol2 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = '1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {tb} set tag tgcol1='7'")
        tdSql.execute(f"alter table {tb} set tag tgcol2=8")
        tdSql.execute(f"alter table {tb} set tag tgcol4='9'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=10")
        tdSql.execute(f"alter table {tb} set tag tgcol6='11'")

        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol1 = '7'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 11)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol2 = 8")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 11)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol4 = '9'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 11)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol5 = 10")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 11)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol6 = '11'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 11)
        tdSql.checkCols(7)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ChangeMultiTags(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ad_db"
        tbPrefix = "ta_ad_tb"
        mtPrefix = "ta_ad_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        j = 3
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tbj = tbPrefix + str(j)
        ntable = "tb_normal_table"

        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tagCol1 bool, tagCol2 tinyint, tagCol3 smallint, tagCol4 int, tagCol5 bigint, tagCol6 nchar(10), tagCol7 binary(8))"
        )
        tdSql.execute(
            f'create table {tb} using {mt} tags( 1, 2, 3, 5,7, "test", "test")'
        )
        tdSql.execute(
            f'create table {tbj} using {mt} tags( 2, 3, 4, 6,8, "testj", "testj")'
        )
        tdSql.execute(f"create table {ntable} (ts timestamp, f int)")

        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.execute(f"insert into {tb} values(now + 1s, 1)")

        # invalid sql
        tdSql.error(f" alter table {mt} set tag tgcol1 = 1,")
        tdSql.error(f" alter table {mt} set tag ,")
        tdSql.error(f" alter table {mt} set tag tgcol1=10,tagcol2=")
        # set tag value on supertable
        tdSql.error(f"alter table {mt} set tag tgcol1 = 1,tagcol2 = 2, tag3 = 4")
        # set normal table value
        tdSql.error(f"alter table {ntable} set tag f = 10")
        # duplicate tag name
        tdSql.error(f"alter table {tbj} set tag tagCol1=1,tagCol1 = 2")
        tdSql.error(f"alter table {tbj} set tag tagCol1=1,tagCol5=10, tagCol5=3")
        # not exist tag
        tdSql.error(f"alter table {tbj} set tag tagNotExist = 1,tagCol1 = 2")
        tdSql.error(f"alter table {tbj} set tag tagCol1 = 2, tagNotExist = 1")
        tdSql.error(f"alter table {tbj} set tagNotExist = 1")
        tdSql.error(f"alter table {tbj} set tagNotExist = NULL,")
        tdSql.error(
            f'alter table {tbj} set tag tagCol1 = 1, tagCol5="xxxxxxxxxxxxxxxx"'
        )

        tdSql.error(
            f'alter table {tbj} set tag tagCol1 = 1, tagCol5="xxxxxxxxxxxxxxxx", tagCol7="yyyyyyyyyyyyyyyyyyyyyyyyy"'
        )
        # invalid data type

        # escape
        tdSql.error(f"alter table {tbj} set tag `tagCol1`=true")
        tdSql.error(
            f"alter table {tbj} set tag `tagCol1`=true,`tagCol2`=1,`tagNotExist`=10"
        )
        tdSql.error(
            f"alter table {tbj} set tag `tagCol1`=true,`tagCol2`=1,tagcol1=true"
        )

        tdSql.execute(f"alter table {tbj} set tag tagCol1 = 100, tagCol2 = 100")

        tdSql.query(f"select * from {mt} where tagCol2 = 100")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol1 = 1")
        tdSql.checkRows(2)

        tdSql.execute(
            f'alter table {tbj} set tag tagCol1=true,tagCol2=-1,tagcol3=-10, tagcol4=-100,tagcol5=-1000,tagCol6="empty",tagCol7="empty1"'
        )
        tdSql.execute(f"alter table {tb} set tag tagCol1=0")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol2 = -1")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol3 = -10")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol4 = -100")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol5 = -1000")
        tdSql.checkRows(0)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty1"')
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tbj} values (now, 1)")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol2 = -1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol3 = -10")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol4 = -100")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol5 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol7 = "empty1"')
        tdSql.checkRows(1)

        tdSql.execute(f"alter table {tbj} set tag tagCol1=true")
        tdSql.execute(f"alter table {tb} set tag tagCol1=true")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(3)

        tdSql.execute(f"alter table {tb} set tag tagCol1=false")

        tdSql.execute(
            f'alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=-10000,tagCol6="empty1",tagCol7="empty2"'
        )

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol2 = -10")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol3 = -100")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol4 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol5 = -10000")
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty1"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol7 = "empty2"')
        tdSql.checkRows(1)

        tdSql.execute(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )
        tdSql.execute(
            f"alter table {tbj} set tag `tagcol1`=true,`tagcol2`=-10,`tagcol3`=-100, `tagcol4`=-1000,`tagcol5`=NULL,`tagcol6`=NULL,`tagcol7`=NULL"
        )

        tdSql.execute(f"alter table {mt} drop tag tagCol7")
        tdSql.execute(f"alter table {mt} drop tag tagCol3")

        tdSql.execute(f"alter table {mt} add tag tagCol8 int")

        # set not exist tag and value
        tdSql.error(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )
        tdSql.error(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )

        tdSql.execute(f"alter table {tbj} set tag tagCol8 = 8")

        tdSql.query(f"select * from {mt} where tagCol4 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol8 = 8")
        tdSql.checkRows(1)
