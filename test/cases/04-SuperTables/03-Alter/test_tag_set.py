from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagSet:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_set(self):
        """修改标签值

        1. 创建包含 bool、int 类型标签的超级表
        2. 创建子表并写入数据
        3. 修改标签值，确认生效
        4. 使用修改后的标签值进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/set.sim

        """

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
