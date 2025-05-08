from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_create(self):
        """创建标签列

        1. 创建包含多种标签类型的超级表
        2. 创建子表并写入数据
        3. 使用标签进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/create.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_cr_db"
        tbPrefix = "ta_cr_tb"
        mtPrefix = "ta_cr_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol smallint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bigint)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol float)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol double)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 smallint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step15")
        i = 15
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step16")
        i = 16
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 double)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step17")
        i = 17
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = true")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step18")
        i = 18
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step19")
        i = 19
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol tinyint, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step20")
        i = 20
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step21")
        i = 21
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step22")
        i = 22
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol float, tgcol2 double)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step23")
        i = 23
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step24")
        i = 24
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol3 = 3")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol5 = 5")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step25")
        i = 25
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step26")
        i = 26
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10), tgcol5 binary(10), tgcol6 binary(10))"
        )
        tdSql.execute(
            f"create table {tb} using {mt} tags( '1', '2', '3', '4', '5', '6' )"
        )
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol3 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step27")
        i = 27
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.error(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10), tgcol7)"
        )

        tdLog.info(f"=============== step28")
        i = 28
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags('1', '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step29")
        i = 29
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(25), tgcol2 binary(250))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags('1', '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step30")
        i = 30
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250), tgcol3 binary(30))"
        )

        tdLog.info(f"=============== step31")
        i = 31
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(5))"
        )
        tdSql.error(f"create table {tb} using {mt} tags('1234567')")
        tdSql.execute(f"create table {tb} using {mt} tags('12345')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt}")
        tdLog.info(f"sql select * from {mt}")
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 2, 12345)
