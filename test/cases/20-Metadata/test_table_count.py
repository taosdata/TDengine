from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTableCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_table_count(self):
        """Table Count

        1.

        Catalog:
            - MetaData

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/tableCount.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.error(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tba3 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba4 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba5 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba6 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba7 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tba8 using sta tags(3, 3, 3);")
        tdSql.execute(f"create table tbb1 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb2 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb3 using stb tags(6, 6, 6);")
        tdSql.execute(f"create table tbb4 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb5 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb6 using stb tags(6, 6, 6);")
        tdSql.execute(f"create table tbb7 using stb tags(7, 7, 7);")
        tdSql.execute(f"create table tbb8 using stb tags(8, 8, 8);")
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")
        tdSql.execute(f"create database db2 vgroups 3;")
        tdSql.error(f"create database db2;")
        tdSql.execute(f"use db2;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f"create table tbb1 using stb tags(4, 4, 4);")
        tdSql.execute(f"create table tbb2 using stb tags(5, 5, 5);")
        tdSql.execute(f"create table tbb3 using stb tags(6, 6, 6);")

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by stable_name;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by db_name;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables group by db_name, stable_name;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select stable_name,count(table_name) from information_schema.ins_tables group by stable_name order by stable_name;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 45)

        tdSql.checkData(1, 1, 10)

        tdSql.checkData(2, 1, 11)

        tdSql.query(
            f"select db_name,count(table_name) from information_schema.ins_tables group by db_name order by db_name;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 17)

        tdSql.checkData(1, 1, 5)

        tdSql.checkData(2, 1, 39)

        tdSql.checkData(3, 1, 5)

        tdSql.query(
            f"select db_name,stable_name,count(table_name) from information_schema.ins_tables group by db_name, stable_name order by db_name, stable_name;"
        )
        tdSql.checkRows(7)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 2, 8)

        tdSql.checkData(2, 2, 8)

        tdSql.checkData(3, 2, 2)

        tdSql.checkData(4, 2, 3)

        tdSql.checkData(5, 2, 39)

        tdSql.checkData(6, 2, 5)

        tdSql.query(
            f"select count(table_name) from information_schema.ins_tables where db_name='db1' and stable_name='sta' group by stable_name"
        )
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 8)

        tdSql.query(f"select distinct db_name from information_schema.ins_tables;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)
