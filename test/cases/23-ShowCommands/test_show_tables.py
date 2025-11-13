from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestShowDbTableKind:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_sim_show_db_table_kind(self):
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

        tdSql.query(f"show user databases;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"show system databases;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"show databases;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)

        tdSql.execute(f"use db1")

        tdSql.query(f"show tables")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(17)

        tdSql.query(f"show normal tables;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "tbn1")

        tdSql.query(f"show child tables;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(16)

        tdSql.query(f"show db2.tables;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)

        tdSql.query(f"show normal db2.tables")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"show child db2.tables")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)

        tdSql.query(f"show child db2.tables like '%'")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)

        tdSql.query(f"show normal db2.tables like '%'")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(0)

        print("\n")
        print("do sim show tables  ................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_show_tables(self):
        """Show databases & tables

        1. Testing various show commands for database/table classification
        2. Verifying filtering capabilities with like clauses
        3. Checking metadata consistency across different database contexts

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/show_db_table_kind.sim

        """
        self.do_sim_show_db_table_kind()