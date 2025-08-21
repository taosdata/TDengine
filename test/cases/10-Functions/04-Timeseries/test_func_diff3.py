from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncDiff:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_diff(self):
        """Diff 函数

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 对子表执行 Diff 查询

        Catalog:
            - Function:Timeseries

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/diff.sim

        """

        dbPrefix = "db"
        tbPrefix = "ctb"
        mtPrefix = "stb"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
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

        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb} where ts > {ms}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb} where ts > {ms}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb} where ts <= {ms}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(1, 1, 1)

        tdLog.info(f"=============== step4")
        tdLog.info(f"===> select _rowts, diff(tbcol) as b from {tb}")
        tdSql.query(f"select _rowts, diff(tbcol) as b from {tb}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        # print =============== step5
        # print ===> select diff(tbcol) as b from $tb interval(1m)
        # sql select diff(tbcol) as b from $tb interval(1m) -x step5
        #  return -1
        # step5:
        #
        # print =============== step6
        # $cc = 4 * 60000
        # $ms = 1601481600000 + $cc
        # print ===> select diff(tbcol) as b from $tb where ts <= $ms interval(1m)
        # sql select diff(tbcol) as b from $tb where ts <= $ms interval(1m) -x step6
        #  return -1

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
