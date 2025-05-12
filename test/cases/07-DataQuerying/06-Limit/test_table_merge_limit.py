from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTableMergeLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_table_merge_limit(self):
        """Table Merge Limit

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/table_merge_limit.sim

        """

        dbPrefix = "m_fl_db"
        tbPrefix = "m_fl_tb"
        mtPrefix = "m_fl_mt"
        tbNum = 2
        rowNum = 513
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== fill.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, c1 int) tags(tgcol int)")

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.query(f"select * from {mt} order by ts limit 10")
        tdSql.checkRows(10)
