from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSLimitAlterTags:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_slimit_alter_tags(self):
        """SLimit Alter Tags

        1.

        Catalog:
            - Query:SLimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/slimit_alter_tags.sim

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
                xs = x * delta
                ts = ts0 + xs
                c1 = x * 10
                c1 = c1 + i
                tdSql.execute(f"insert into {tb} values ( {ts} , {c1} , {c1} , {c1} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== tables and data created")

        tdLog.info(f"================== add a tag")
        tdSql.execute(f"alter table stb add tag tg_added binary(15)")
        tdSql.query(f"describe stb")
        tdSql.checkRows(7)

        tdSql.checkData(6, 0, "tg_added")

        tdSql.query(f"select count(*) from stb group by tg_added order by tg_added asc")
        tdSql.checkRows(1)

        res = rowNum * 10
        tdSql.checkData(0, 0, res)

        # if $tdSql.getData(0,1, NULL then
        #  return -1
        # endi

        tdLog.info(f"================== change tag values")
        i = 0
        while i < 10:
            tb = "tb" + str(i)
            tg_added = "'" + tb + "'"
            tdSql.execute(f"alter table {tb} set tag tg_added = {tg_added}")
            i = i + 1

        tdSql.query(f"select t1,t2,tg_added from tb0")
        tdSql.checkRows(300)

        tdSql.checkData(0, 2, "tb0")

        tdSql.execute(f"reset query cache")
        tdSql.query(
            f"select count(*), first(ts), tg_added from stb partition by tg_added slimit 5 soffset 3"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(2, 0, rowNum)

        tdSql.checkData(3, 0, rowNum)

        tdSql.checkData(4, 0, rowNum)

        tdSql.execute(f"alter table tb9 set tag t2 = 3")
        tdSql.query(
            f"select count(*), first(*) from stb partition by t2 slimit 6 soffset 1"
        )
        tdSql.checkRows(3)

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")
        ### repeat above queries
        tdSql.query(
            f"select count(*), first(ts) from stb partition by tg_added slimit 5 soffset 3;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(2, 0, rowNum)

        tdSql.checkData(3, 0, rowNum)

        tdSql.checkData(4, 0, rowNum)

        tdSql.query(
            f"select count(*), first(*) from stb partition by t2 slimit 6 soffset 1"
        )
        tdSql.checkRows(3)
