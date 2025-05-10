from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSLimit1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_slimit1(self):
        """SLimit 1

        1.

        Catalog:
            - Query:SLimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/slimit1.sim

        """

        dbPrefix = "slm_alt_tg_db"

        tdLog.info(f"========== slimit_alter_tag.sim")
        # make sure the data in each table crosses a file block boundary
        rowNum = 300
        ts0 = 1537146000000
        delta = 600000
        db = dbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} maxrows 200")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 bigint, c3 double) tags(t1 int, t2 int)"
        )
        # If grouped by t2, some groups should have tables from different vnodes
        tdSql.execute(f"create table tb0 using stb tags (0, 0)")
        tdSql.execute(f"create table tb1 using stb tags (1, 0)")
        tdSql.execute(f"create table tb2 using stb tags (2, 0)")
        tdSql.execute(f"create table tb3 using stb tags (3, 1)")
        tdSql.execute(f"create table tb4 using stb tags (4, 1)")
        tdSql.execute(f"create table tb5 using stb tags (5, 1)")
        tdSql.execute(f"create table tb6 using stb tags (6, 2)")
        tdSql.execute(f"create table tb7 using stb tags (7, 2)")
        tdSql.execute(f"create table tb8 using stb tags (8, 2)")
        # tb9 is intentionally set the same tags with tb8
        tdSql.execute(f"create table tb9 using stb tags (8, 2)")

        i = 0
        tbNum = 10
        while i < tbNum:
            tb = "tb" + str(i)
            x = 0
            while x < rowNum:
                xs = int(x * delta)
                ts = ts0 + xs
                c1 = x * 10
                c1 = c1 + i
                tdSql.execute(f"insert into {tb} values ( {ts} , {c1} , {c1} , {c1} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== tables and data created")

        self.slimit1_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.slimit1_query()

    def slimit1_query(self):

        dbPrefix = "slm_alt_tg_db"

        tdLog.info(f"========== slimit1_query.sim")
        # make sure the data in each table crosses a file block boundary
        rowNum = 300
        ts0 = 1537146000000
        delta = 600000
        db = dbPrefix

        tdSql.execute(f"use {db}")

        #### partition by t2,t1 + slimit
        tdSql.query(f"select count(*) from stb partition by t2,t1 slimit 5 soffset 6")
        tdSql.checkRows(3)

        ## desc
        tdSql.query(
            f"select count(*),t2,t1 from stb partition by t2,t1 order by t2,t1 asc  slimit 5 soffset 0"
        )
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, 300)

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(1, 0, 300)

        tdSql.checkData(1, 1, 0)

        tdSql.checkData(1, 2, 1)

        tdSql.checkData(2, 0, 300)

        tdSql.checkData(2, 1, 0)

        tdSql.checkData(2, 2, 2)

        tdSql.checkData(3, 0, 300)

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 3)

        tdSql.checkData(4, 0, 300)

        tdSql.checkData(4, 1, 1)

        tdSql.checkData(4, 2, 4)

        ### empty result set
        tdSql.query(
            f"select count(*) from stb partition by t2,t1 order by t2 asc slimit 0 soffset 0"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(*) from stb partition by t2,t1 order by t2 asc slimit 5 soffset 10"
        )
        tdSql.checkRows(0)

        #### partition by t2 + slimit
        tdSql.query(
            f"select t2, count(*) from stb partition by t2 order by t2 asc slimit 2 soffset 0"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 2)

        tdSql.checkData(0, 1, 900)

        tdSql.checkData(1, 1, 900)

        tdSql.checkData(2, 1, 1200)

        tdSql.query(
            f"select t2, count(*) from stb partition by t2 order by t2 desc slimit 2 soffset 0"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 2)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 0)

        tdSql.checkData(0, 1, 1200)

        tdSql.checkData(1, 1, 900)

        tdSql.checkData(2, 1, 900)

        tdSql.query(
            f"select count(*) from stb partition by t2 order by t2 asc slimit 2 soffset 1"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(*) from stb partition by t2 order by t2 desc slimit 2 soffset 1"
        )
        tdSql.checkRows(0)
