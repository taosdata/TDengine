from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestColumnBool:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_column_bool(self):
        """Bool 数据列的超级表查询

        1. 创建包含 1 个 Bool 类型普通数据列和 1 个 Bool 类型标签列的超级表
        2. 创建子表并写入数据
        3. 对超级表执行基于普通数据列筛选条件的查询，包括投影查询、聚合查询和分组查询

        Catalog:
            - SuperTable:Columns

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/field/bool.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol bool) TAGS(tgcol bool)")

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 0 )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , 1 )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt} where tbcol = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> true")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol = false")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tbcol <> false")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol = true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > 1626739440001 and tbcol <> true")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < 1626739440001 and tbcol <> false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol = false")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= 1626739440001 and tbcol <> false")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and ts < 1626739500001 and tbcol <> false"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > 1626739440001 and tbcol <> false and ts < 1626739500001"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol), first(tbcol), last(tbcol) from {mt}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step7")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where ts < 1626739440001 and tbcol = true group by tgcol"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true partition by tgcol interval(1d) order by tgcol desc"
        )
        tdLog.info(
            f"select count(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = true partition by tgcol interval(1d) order by tgcol desc"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
