from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncPercentile:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_percentile(self):
        """Percentile 函数

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Percentile 查询，包括时间窗口、时间戳列筛选

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/percentile.sim

        """

        dbPrefix = "m_pe_db"
        tbPrefix = "m_pe_tb"
        mtPrefix = "m_pe_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select percentile(tbcol, 10) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.900000000)

        tdSql.query(f"select percentile(tbcol, 20) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 3.800000000)

        tdSql.query(f"select percentile(tbcol, 100) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19.000000000)

        tdSql.error(f"select percentile(tbcol, 110) from {tb}")

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 1) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.140000000)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 5) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.700000000)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 0) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.000000000)

        tdLog.info(f"=============== step4")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 1) as c from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.140000000)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 5) as c from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.700000000)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select percentile(tbcol, 0) as c from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select _wstart, percentile(tbcol, 10) as c from {tb} interval(1d)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2020-10-01 00:00:00")
        tdSql.checkData(0, 1, 1.900000000)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
