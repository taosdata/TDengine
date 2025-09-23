from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTsDiff:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ts_diff(self):
        """Timeseries: Diff

        Test the DIFF function, including conditions such as LIKE, timestamp comparisons, and ordinary column comparisons.

        Catalog:
            - Function:Timeseries

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-26 Simon Guan Migrated from tsim/query/diff.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/diff.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/diff2.sim

        """

    def QueryDiff(self):
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

    def ComuteDiff(self):
        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
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

        tdSql.query(f"select diff(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select diff(tbcol) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select diff(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select diff(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step5")
        tdSql.error(f"select diff(tbcol) as b from {tb} interval(1m)")

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.error(f"select diff(tbcol) as b from {tb} where ts <= {ms} interval(1m)")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ComuteDiff2(self):
        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        tbNum = 2
        rowNum = 10000
        totalNum = 20000

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 nchar(5), c9 binary(10)) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tinyint = x / 128
                tdSql.execute(
                    f"insert into {tb} values ({ms} , {x} , {x} , {x} , {x} , {tinyint} , {x} , {x} , {x} , {x} )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select diff(c1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c2) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c3) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c4) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c5) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 0)

        tdSql.query(f"select diff(c6) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c7) from {tb}")
        tdSql.error(f"select diff(c8) from {tb}")
        tdSql.error(f"select diff(c9) from {tb}")
        tdSql.query(f"select diff(c1), diff(c2) from {tb}")

        tdSql.query(f"select 2+diff(c1) from {tb}")
        tdSql.query(f"select diff(c1+2) from {tb}")
        tdSql.error(
            f"select diff(c1) from {tb} where ts > 0 and ts < now + 100m interval(10m)"
        )
        # sql select diff(c1) from $mt
        tdSql.error(f"select diff(diff(c1)) from {tb}")
        tdSql.error(f"select diff(c1) from m_di_tb1 where c2 like '2%'")

        tdLog.info(f"=============== step3")
        tdSql.query(f"select diff(c1) from {tb} where c1 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c2) from {tb} where c2 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c3) from {tb} where c3 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c4) from {tb} where c4 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c5) from {tb} where c5 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 0)

        tdSql.query(f"select diff(c6) from {tb} where c6 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select diff(c1) from {tb} where c1 > 5 and c2 < {rowNum}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c1) from {tb} where c9 like '%9' and c1 <= 20")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"=============== step5")
        tdSql.error(f"select diff(c1) as b from {tb} interval(1m)")

        tdLog.info(f"=============== step6")
        tdSql.error(f"select diff(c1) as b from {tb} where ts < now + 4m interval(1m)")

        tdLog.info(f"=============== clear")
