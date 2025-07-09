from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestGroupByBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby_basic(self):
        """Group By Basic

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/groupby-basic.sim

        """

        # ========================================= setup environment ================================

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"
        tbNum = 8
        rowNum = 100
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== groupby.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"

        tdLog.info(f"==== create db, stable, ctables, insert data")
        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {x} , {x} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                # print ==== insert into $tb values ($tstart , $c , $c , $x , $x , $c , $c , $c , $binary , $nchar )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 1640966400000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        ts1 = tb1 + "ts"
        ts2 = tb2 + "ts"

        tdLog.info(f"===============================groupby_operation")
        tdLog.info(f"")
        tdLog.info(f"==== select count(*), c1 from group_tb0 group by c1 order by c1")
        tdSql.query(f"select count(*), c1 from group_tb0 group by c1 order by c1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(9, 0, 10)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(9, 1, 9)

        tdLog.info(f"==== select first(ts),c1 from group_tb0 group by c1 order by c1;")
        tdSql.query(f"select first(ts),c1 from group_tb0 group by c1 order by c1;")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2022-01-01 00:00:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(9, 0, "2022-01-01 00:00:00.009")
        tdSql.checkData(9, 1, 9)

        tdLog.info(f"==== select first(ts),c1 from interval(5m) group_tb0 group by c1;")
        tdSql.query(f"select first(ts),c1 from group_tb0 group by c1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
