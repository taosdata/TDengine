from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_drop(self):
        """删除标签列

        1. 创建超级表
        2. 创建子表并写入数据
        3. 删除标签列，确认生效
        4. 使用删除后的标签进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/delete.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_de_db"
        tbPrefix = "ta_de_tb"
        mtPrefix = "ta_de_mt"
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

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 < 3")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.query(f"describe {tb}")
        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(2, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.error(f"alter table {mt} drop tag tgcol1")

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
        tdSql.error(f"alter table {mt} drop tag tgcol1")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

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

        tdSql.query(f"describe {tb}")
        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 1, "VARCHAR")

        tdSql.checkData(2, 2, 2)

        tdSql.checkData(3, 2, 1)

        tdSql.checkData(4, 2, 10)

        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 3, "TAG")

        tdSql.checkData(4, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2', '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")

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
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")

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

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = '1'")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = '1'")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4=4")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkCols(4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkCols(4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 5)

        tdSql.checkCols(5)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")

        tdSql.error(f"alter table xxmt drop tag tag1")
        tdSql.error(f"alter table {tb} drop tag tag1")
        tdSql.error(f"alter table {mt} drop tag tag1")
        tdSql.error(f"alter table {mt} drop tag tagcol1")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.error(f"alter table {mt} drop tag tgcol1")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
