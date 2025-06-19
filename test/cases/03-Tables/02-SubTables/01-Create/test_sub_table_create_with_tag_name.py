from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableCreateWithTagName:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_create_tb_with_tag_name(self):
        """create tb with tag name

        1.

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/create_tb_with_tag_name.sim

        """

        tdLog.info(f"======================== dnode1 start")
        db = "testdb"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int) tags (id int, t1 int, t2 nchar(4), t3 double)"
        )

        tdSql.execute(f"insert into tb1 using st2 (id, t1) tags(1,2) values (now, 1)")
        tdSql.query(f"select id,t1,t2,t3 from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, None)

        tdSql.checkData(0, 3, None)

        tdSql.execute(f'create table tb2 using st2 (t2,t3) tags ("12",22.0)')
        tdSql.query(f"show tags from tb2")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, None)

        tdSql.checkData(1, 5, None)

        tdSql.checkData(2, 5, 12)

        tdSql.checkData(3, 5, "22.000000000")

        tdSql.execute(f'create table tb3 using st2 tags (1,2,"3",33.0);')
        tdSql.query(f"show tags  from tb3;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, 3)

        tdSql.checkData(3, 5, "33.000000000")

        tdSql.execute(f'insert into tb4 using st2 tags(1,2,"33",44.0) values (now, 1);')
        tdSql.query(f"show tags from tb4;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, 33)

        tdSql.checkData(3, 5, "44.000000000")

        tdSql.error(f'create table tb5 using st2() tags (3,3,"3",33.0);')
        tdSql.error(f'create table tb6 using st2 (id,t1) tags (3,3,"3",33.0);')
        tdSql.error(f"create table tb7 using st2 (id,t1) tags (3);")
        tdSql.error(f"create table tb8 using st2 (ide) tags (3);")
        tdSql.error(f"create table tb9 using st2 (id);")
        tdSql.error(f"create table tb10 using st2 (id t1) tags (1,1);")
        tdSql.error(f"create table tb10 using st2 (id,,t1) tags (1,1,1);")
        tdSql.error(f"create table tb11 using st2 (id,t1,) tags (1,1,1);")

        tdSql.execute(f"create table tb12 using st2 (t1,id) tags (2,1);")
        tdSql.query(f"show tags from tb12;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, None)

        tdSql.checkData(3, 5, None)

        tdSql.execute(f"create table tb13 using st2 (t1,id) tags (2,1);")
        tdSql.query(f"show tags from tb13;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, None)

        tdSql.checkData(3, 5, None)
