from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagChange:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_change(self):
        """修改标签名

        1. 创建包含 bool、int 类型标签的超级表
        2. 创建子表并写入数据
        3. 修改标签名，确认生效
        4. 使用修改后的标签名进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/change.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ch_db"
        tbPrefix = "ta_ch_tb"
        mtPrefix = "ta_ch_mt"
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

        tdSql.error(f"alter table {mt} rename tag tagcx tgcol3")
        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol2")

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")
        tdSql.error(f"alter table {mt} rename tag tgcol4 tgcol3")

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

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

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

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

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

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

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
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} rename tag tgcol4 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol7")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol8")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} rename tag tgcol3 tgcol9")
        tdSql.execute(f"alter table {mt} rename tag tgcol5 tgcol10")
        tdSql.execute(f"alter table {mt} rename tag tgcol6 tgcol11")

        tdSql.execute(f"reset query cache")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 < 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = '2'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdSql.query(f"select * from {mt} where tgcol7 = '1'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol8 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol9 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol10 = 5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol11 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
