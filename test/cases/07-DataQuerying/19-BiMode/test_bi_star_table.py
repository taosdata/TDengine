from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestBiStarTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_bi_star_table(self):
        """Bi Star Table

        1.

        Catalog:
            - Query:BiMode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/bi_star_table.sim

        """

        tdSql.prepare(dbname="db1", vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f'insert into tba1 values(now, 1, "1");')
        tdSql.execute(f'insert into tba2 values(now + 1s, 2, "2");')
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")
        
        tdSql.prepare(dbname="db2", vgroups=3)
        tdSql.execute(f"use db2;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")

        # set_bi_mode 1
        tdSql.setConnMode(1)
        tdSql.query(f"select * from db1.sta order by ts;")
        tdSql.checkCols(7)

        tdSql.checkData(0, 6, "tba1")

        tdSql.query(f"select last(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select last_row(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select first(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba1")

        tdLog.info(f'"=====table star ====================="')

        tdSql.query(f"select b.* from db1.sta b order by ts;")
        tdSql.checkCols(7)

        tdSql.checkData(0, 6, "tba1")

        tdSql.query(f"select last(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select last_row(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select first(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba1")

        tdSql.query(f"select * from (select f1 from db1.sta);")
        tdSql.checkCols(1)

        # set_bi_mode 0
        tdSql.setConnMode(0)
        tdSql.query(f"select * from db1.sta order by ts;")
        tdSql.checkCols(6)
