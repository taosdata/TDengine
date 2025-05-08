from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagFilter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_filter(self):
        """1 个标签列的超级表查询

        1. 创建包含 1 个标签的超级表
        2. 创建子表并写入数据
        3. 对超级表执行基于标签筛选条件的查询，包括投影查询、聚合查询和分组查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/filter.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_fi_db"
        tbPrefix = "ta_fi_tb"
        mtPrefix = "ta_fi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10))"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '1' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol = '1'"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tg = '1'"
        )

        tdLog.info(f"=============== step3")
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where noexist = '1'"
        )

        tdLog.info(f"=============== step4")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tbcol = '1'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdLog.info(f"=============== step5")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step6")
        tdSql.error(
            f"select count(tbcol), avg(cc), sum(xx), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mtPrefix}"
        )

        tdLog.info(f"=============== step7")
        tdSql.error(
            f"select count(tgcol), avg(tgcol), sum(tgcol), min(tgcol), max(tgcol), first(tgcol), last(tgcol) from {mtPrefix}"
        )

        tdLog.info(f"=============== step8")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tbcol"
        )

        tdLog.info(f"=============== step9")
        tdSql.error(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by noexist"
        )

        tdLog.info(f"=============== step10")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step11")
        tdSql.query(f"select count(tbcol) as c from {mt} group by tbcol")

        tdLog.info(f"=============== step12")
        tdSql.error(f"select count(tbcol) as c from {mt} group by noexist")

        tdLog.info(f"=============== step13")
        tdSql.query(f"select count(tbcol) as c from {mt} group by tgcol")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select count(tbcol) as c from {mt} where ts > 1000 group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step15")
        tdSql.error(
            f"select count(tbcol) as c from {mt} where noexist < 1  group by tgcol"
        )

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol = '1' group by tgcol"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
