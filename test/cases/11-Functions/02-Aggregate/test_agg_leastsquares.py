from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLeastsquares:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_leastsquares(self):
        """Agg-basic: Leastsquares

        Test the LeastSquares function, including time windows, filtering on ordinary data columns, filtering on tag columns.

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/leastsquare.sim

        """

        dbPrefix = "m_le_db"
        tbPrefix = "m_le_tb"
        mtPrefix = "m_le_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True, keep=36500)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 2
            ms = 1000
            while x < rowNum:
                ms = ms + 1000
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select leastsquares(tbcol, 1, 1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdLog.info(f"=============== step3")
        tdSql.query(f"select leastsquares(tbcol, 1, 1) from {tb} where ts < now + 4m")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdLog.info(f"=============== step4")
        tdSql.query(f"select leastsquares(tbcol, 1, 1) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdLog.info(f"=============== step5")
        tdSql.query(f"select leastsquares(tbcol, 1, 1) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdSql.query(f"select leastsquares(tbcol, 1, 1) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select leastsquares(tbcol, 1, 1) as b from {tb} where ts < now + 4m interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "{slop:1.000000, intercept:1.000000}")

        tdSql.checkRows(1)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
