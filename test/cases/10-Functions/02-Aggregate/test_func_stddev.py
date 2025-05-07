from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncStddev:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_stddev(self):
        """Stddev 函数

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Stddev 查询，包括时间窗口、时间戳列筛选

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/stddev.sim

        """

        dbPrefix = "m_st_db"
        tbPrefix = "m_st_tb"
        mtPrefix = "m_st_mt"
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

        tdSql.query(f"select stddev(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.766281297)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select stddev(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1.414213562)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select stddev(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.766281297)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select stddev(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(f"select stddev(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5.766281297)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(
            f"select stddev(tbcol) as b from {tb} where ts <= {ms} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.checkRows(5)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
