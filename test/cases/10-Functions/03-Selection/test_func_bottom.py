from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncBottom:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_bottom(self):
        """Bottom 函数

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Bottom 查询

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/bottom.sim

        """

        dbPrefix = "m_bo_db"
        tbPrefix = "m_bo_tb"
        mtPrefix = "m_bo_mt"
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

        tdSql.query(f"select bottom(tbcol, 1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select bottom(tbcol, 1) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select bottom(tbcol, 1) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select bottom(tbcol, 2) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 0)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select bottom(tbcol, 2) as b from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 6)

        tdSql.checkData(1, 0, 5)

        tdSql.error(f"select bottom(tbcol, 122) as b from {tb}")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
