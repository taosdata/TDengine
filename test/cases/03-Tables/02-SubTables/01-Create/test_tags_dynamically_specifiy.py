from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagsDynamicallySpecifiy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tags_dynamically_specifiy(self):
        """tags dynamically specifiy

        1.

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/tags_dynamically_specifiy.sim

        """

        db = "dytag_db"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== tags_dynamically_specify.sim")
        i = 0

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )

        tdSql.execute(
            f"insert into tb1 using stb (t1) tags ('tag1') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tb2 using stb (t2) tags (2)      values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tb3 using stb (t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )

        tdSql.execute(
            f"insert into tb4 (ts, c1, c2) using stb (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4', 4)"
        )
        tdSql.execute(
            f"insert into tb5 (ts, c1, c3) using stb (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.execute(
            f"insert into tb6 (ts, c1, c3) using stb tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.execute(
            f"insert into tb7 (ts, c1, c2, c3) using stb tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )
        tdSql.query(f"select * from stb order by ts asc")
        tdSql.checkRows(7)

        tdSql.error(
            f"insert into tb11 using stb (t1) tags () values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb12 using stb (t1, t3) tags () values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb13 using stb (t1, t2, t3) tags (8, 9.13, 'ac') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb14 using stb () tags (2)  values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.error(
            f"insert into tb15 using stb (t2, t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )
        tdSql.error(
            f"insert into tb16 (ts, c1, c2) using stb (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4')"
        )
        tdSql.error(
            f"insert into tb17 (ts, c1, c3) using stb (t1, t3) tags ('tag5', 11.11, 5) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.error(
            f"insert into tb18 (ts, c1, c3) using stb tags ('tag5', 16) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.error(
            f"insert into tb19 (ts, c1, c2, c3) using stb tags (19, 'tag5', 91.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )

        tdSql.execute(
            f"create table stbx (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )
        tdSql.execute(
            f"insert into tb100 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag100', 100, 100.123456) values ( now + 10s, 'binary100', 100, 100.9) tb101 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag101', 101, 101.9) values ( now + 10s, 'binary101', 101, 101.9) tb102 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag102', 102, 102.9) values ( now + 10s, 'binary102', 102, 102.9)"
        )

        tdSql.query(f"select * from stbx order by t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 4, "tag100")

        tdSql.checkData(0, 5, 100)

        tdSql.checkData(0, 6, 100.12346)

        tdSql.execute(
            f"create table stby (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )
        tdSql.execute(f"reset query cache")
        tdSql.execute(
            f"insert into tby1 using stby (t1) tags ('tag1') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tby2 using stby (t2) tags (2)      values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tby3 using stby (t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )
        tdSql.execute(
            f"insert into tby4 (ts, c1, c2) using stby (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4', 4)"
        )
        tdSql.execute(
            f"insert into tby5 (ts, c1, c3) using stby (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.execute(
            f"insert into tby6 (ts, c1, c3) using stby tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.execute(
            f"insert into tby7 (ts, c1, c2, c3) using stby tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )
        tdSql.query(f"select * from stby order by ts asc")
        tdSql.checkRows(7)

        tdSql.execute(f"reset query cache")
        tdSql.execute(
            f"insert into tby1 using stby (t1) tags ('tag1') values ( now + 1s, 'binary1y', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tby2 using stby (t2) tags (2)      values ( now + 2s, 'binary2y', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tby3 using stby (t3) tags (3.3)    values ( now + 3s, 'binary3y', 3, 3.3)"
        )
        tdSql.execute(
            f"insert into tby4 (ts, c1, c2) using stby (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4y', 4)"
        )
        tdSql.execute(
            f"insert into tby5 (ts, c1, c3) using stby (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5y', 5.5)"
        )
        tdSql.execute(
            f"insert into tby6 (ts, c1, c3) using stby tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6y', 6.6)"
        )
        tdSql.execute(
            f"insert into tby7 (ts, c1, c2, c3) using stby tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7y', 7, 7.7)"
        )

        tdSql.query(f"select * from stby order by ts asc")
        tdSql.checkRows(14)

        tdLog.info(f"===============================> td-1765")
        tdSql.execute(
            f"create table m1(ts timestamp, k int) tags(a binary(4), b nchar(4));"
        )
        tdSql.execute(f"create table tm0 using m1 tags('abcd', 'abcd');")
        tdSql.error(f"alter table tm0 set tag b = 'abcd1';")
        tdSql.error(f"alter table tm0 set tag a = 'abcd1';")
