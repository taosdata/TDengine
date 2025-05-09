from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSetTagVals:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_set_tag_vals(self):
        """set tag vals

        1. -

        Catalog:
            - Table:SubTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/set_tag_vals.sim

        """

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
        tdSql.checkData(0, 4, '表1标签2')

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
        tdSql.checkData(0, 4, '涛思')

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
