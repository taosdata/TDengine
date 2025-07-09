from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_column(self):
        """2 个标签列的超级表查询

        1. 创建包含 2 个标签的超级表
        2. 创建子表并写入数据
        3. 对超级表执行基于标签筛选条件的查询，包括投影查询、聚合查询和分组查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/column.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_co_db"
        tbPrefix = "ta_co_tb"
        mtPrefix = "ta_co_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        i = 0
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int, tbcol2 binary(10)) TAGS(tgcol int, tgcol2 binary(10))"
        )

        tdLog.info(f"=============== step2")

        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(  0,  '0' )")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(  1,   '1'  )")

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags( '2', '2' )")

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags( '3',  '3' )")

        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step3")

        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now,  0,  '0')")

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now,  1,   '1'  )")

        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now, '2', '2')")

        i = 3
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values(now, '3',  '3')")

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(4)

        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
