from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableCreateMulti:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_createmulti(self):
        """create normal table (create multi)

        1. create normal table
        2. insert data
        3. drop table
        4. show tables

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/createmulti.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create table db.st1 (ts timestamp, i int) tags (j int)")
        tdSql.execute(
            f"create table db.st2 (ts timestamp, i int, j int) tags (t1 int, t2 int, t3 int)"
        )
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(2)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdLog.info(f"=============== create multiple child tables")
        tdSql.execute(
            f"create table db.ct1 using db.st1 tags(1) db.ct2 using db.st1 tags(2);"
        )

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(2)

        tdSql.execute(
            f"create table db.ct3 using db.st2 tags(1, 1, 1) db.ct4 using db.st2 tags(2, 2, 2);"
        )
        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(4)

        tdSql.execute(
            f"create table db.ct5 using db.st1 tags(3) db.ct6 using db.st2 tags(3, 3, 3);"
        )
        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(6)
