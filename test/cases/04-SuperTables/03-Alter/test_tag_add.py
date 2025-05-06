from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagAdd:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_add(self):
        """增加标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 增加标签列，确认生效
        4. 使用增加的标签值进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/add.sim

        """

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
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4 =4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 tinyint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.query(f"describe {tb}")
        tdLog.info(f"sql describe {tb}")
        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 float")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 smallint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol3 = '1'")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=false")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(0)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol6 tinyint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol6 = 6")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(17)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bool")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6='1'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"select * from {mt} where tgcol5 = 5")
        tdLog.info(
            f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 1)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2', '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")
        tdSql.execute(f"alter table {mt} add tag tgcol6 float")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=1")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', '2', '3', '4' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=false")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 0)

        tdSql.checkCols(5)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol7 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol8 smallint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"alter table {tb} set tag tgcol7=7")
        tdSql.execute(f"alter table {tb} set tag tgcol8=8")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 =5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6)

        tdSql.checkData(0, 6, 7)

        tdSql.checkData(0, 7, 8)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol9 = 1")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol3 int")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1=false")
        tdSql.execute(f"alter table {tb} set tag tgcol2='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=4")
        tdSql.execute(f"alter table {tb} set tag tgcol4='3'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=2")
        tdSql.execute(f"alter table {tb} set tag tgcol6='1'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '3'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 3)

        tdSql.checkData(0, 7, 2)

        tdSql.query(f"select * from {mt} where tgcol2 = '5'")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol3 = 4")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol5 = 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol6 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step13")
        i = 13
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

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1='7'")
        tdSql.execute(f"alter table {tb} set tag tgcol2='8'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=9")
        tdSql.execute(f"alter table {tb} set tag tgcol4=10")
        tdSql.execute(f"alter table {tb} set tag tgcol5=11")
        tdSql.execute(f"alter table {tb} set tag tgcol6=12")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol2 = '8'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 7)

        tdSql.checkData(0, 3, 9)

        tdSql.checkData(0, 4, 11)

        tdSql.checkData(0, 5, 8)

        tdSql.checkData(0, 6, 10)

        tdSql.checkData(0, 7, 12)

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")

        tdSql.execute(f"alter table {mt} add tag tgcol3 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")
        tdSql.execute(f"alter table {mt} add tag tgcol7 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol8 bigint")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
