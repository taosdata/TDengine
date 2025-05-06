from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTag4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_4(self):
        """4 个标签列的超级表查询

        1. 创建一个包含 4 个标签的超级表
        2. 创建子表并写入数据
        3. 对超级表执行基于标签筛选条件的查询，包括投影查询、聚合查询和分组查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/4.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_4_db"
        tbPrefix = "ta_4_tb"
        mtPrefix = "ta_4_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdLog.info(f"=========db: {db}")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 bigint, tgcol3 float, tgcol4 double)"
        )

        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 0, 0, 0, 0 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1
        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( 1, 1, 1, 1 )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select * from {mt} where ts < now + 4m")
        tdSql.checkRows(50)

        tdSql.query(f"select * from {mt} where ts > now + 4m")
        tdSql.checkRows(150)

        tdSql.query(f"select * from {mt} where ts = now + 4m")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where ts > now + 4m and ts < now + 5m")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol1 <> 0")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol2 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt} where tgcol3 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol3 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step6")
        tdSql.query(f"select * from {mt} where tgcol4 = 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 0")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(100)

        tdSql.query(f"select * from {mt} where tgcol4 <> 1")
        tdSql.checkRows(100)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol1 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol2 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol3 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 = 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts > now + 4m and tgcol4 <> 1")
        tdSql.checkRows(75)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts < now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 = 0")
        tdSql.checkRows(25)

        tdSql.query(f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0")
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol2 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol2 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step12")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step13")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol2 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol2 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol2 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 = 1 and tgcol4 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 1 and tgcol4 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 = 0 and tgcol4 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol3 <> 0 and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol3 <> 0 and ts < now + 5m and ts < now + 5m and tgcol4 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step15")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol1 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step16")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step17")
        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol1 = 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 1 and tgcol2 <> 1  and tgcol3 <> 1 and tgcol1 <> 1"
        )
        tdSql.checkRows(75)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts < now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 = 0 and tgcol2 = 0 and tgcol3 = 0 and tgcol1 = 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts <= now + 4m and tgcol4 <> 0 and tgcol2 <> 0 and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and ts < now + 5m and tgcol4 <> 0 and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from {mt} where ts > now + 4m and tgcol4 <> 0 and ts < now + 5m and ts < now + 5m and tgcol2 <> 0  and tgcol3 <> 0 and tgcol1 <> 0"
        )
        tdSql.checkRows(5)

        tdLog.info(f"=============== step18")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt}"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step19")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1  and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step20")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 50)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step21")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol3"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} group by tgcol4"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step22")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1  group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol1"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== step23")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1  group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where ts < now + 4m and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 group by tgcol2"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step24")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol1 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol2 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 partition by tgcol3 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from {mt} where tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1 and tgcol4 = 1 partition by tgcol4 interval(1d)"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}"
        )
        tdSql.checkData(0, 0, 100)

        tdLog.info(f"=============== clear")
        tdLog.info(f"drop database {db}")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
