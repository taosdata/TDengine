from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInterval:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval(self):
        """时间窗口基本查询

        1. 创建包含一个 Int 普通数据列的超级表
        2. 创建子表并写入数据
        3. 在 1m 窗口内进行 count/avg/max/min
        4. 增加时间戳筛选和 Fill 字句查询

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/interval.sim

        """

        dbPrefix = "m_in_db"
        tbPrefix = "m_in_tb"
        mtPrefix = "m_in_mt"
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

        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        # tdSql.checkRowsGreaterEqualThan(rowNum)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb}  where ts <= {ms} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        # tdSql.checkRowsLessEqualThan(10)
        # tdSql.checkRowsGreaterEqualThan(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"=============== step4")
        cc = 40 * 60000
        ms = 1601481600000 + cc

        cc = 1 * 60000
        ms2 = 1601481600000 - cc

        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb}  where ts <= {ms} and ts > {ms2} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        # tdSql.checkRowsLessEqualThan(22)
        # tdSql.checkRowsGreaterEqualThan(18)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"=============== step5")
        cc = 40 * 60000
        ms = 1601481600000 + cc

        cc = 1 * 60000
        ms2 = 1601481600000 - cc

        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {tb}  where ts <= {ms} and ts > {ms2} interval(1m) fill(value,0,0,0,0,0)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        # tdSql.checkRowsLessEqualThan(50)
        # tdSql.checkRowsGreaterEqualThan(30)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 4, 1)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {mt} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        tdSql.checkAssert(tdSql.getRows() >= 18)
        tdSql.checkAssert(tdSql.getRows() <= 22)
        tdSql.checkAssert(tdSql.getData(1, 0) >= 5)
        tdSql.checkAssert(tdSql.getData(1, 0) <= 15)

        tdLog.info(f"=============== step7")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {mt}  where ts <= {ms} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        tdSql.checkAssert(tdSql.getRows() >= 3)
        tdSql.checkAssert(tdSql.getRows() <= 7)
        tdSql.checkAssert(tdSql.getData(1, 0) >= 5)
        tdSql.checkAssert(tdSql.getData(1, 0) <= 15)

        tdLog.info(f"=============== step8")
        cc = 40 * 60000
        ms1 = 1601481600000 + cc

        cc = 1 * 60000
        ms2 = 1601481600000 - cc

        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {mt}  where ts <= {ms1} and ts > {ms2} interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getRows()}")
        tdSql.checkAssert(tdSql.getRows() >= 18)
        tdSql.checkAssert(tdSql.getRows() <= 22)
        tdSql.checkAssert(tdSql.getData(1, 0) >= 5)
        tdSql.checkAssert(tdSql.getData(1, 0) <= 15)

        tdLog.info(f"=============== step9")
        cc = 40 * 60000
        ms1 = 1601481600000 + cc

        cc = 1 * 60000
        ms2 = 1601481600000 - cc

        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from {mt}  where ts <= {ms1} and ts > {ms2} interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.checkAssert(tdSql.getRows() <= 50)
        tdSql.checkAssert(tdSql.getRows() >= 30)
        tdSql.checkAssert(tdSql.getData(1, 0) <= 15)
        tdSql.checkAssert(tdSql.getData(1, 0) >= 5)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
