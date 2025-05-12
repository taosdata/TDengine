from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableAutoCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_auto_create(self):
        """auto create subtable

        1. create stable
        2. auto create sub table
        3. query from stable

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/autocreate.sim

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

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"insert into db.c1 using db.st1 tags(1) values(now, 1);")
        tdSql.execute(f"insert into db.c2 using db.st1 tags(2) values(now, 2);")
        tdSql.execute(f"insert into db.c3 using db.st1 tags(3) values(now, 3);")
        tdSql.execute(f"insert into db.c4 using db.st1 tags(4) values(now, 4);")
        tdSql.execute(f"insert into db.c1 using db.st1 tags(1) values(now+1s, 1);")
        tdSql.execute(f"insert into db.c2 using db.st1 tags(2) values(now+1s, 2);")
        tdSql.execute(f"insert into db.c3 using db.st1 tags(3) values(now+1s, 3);")
        tdSql.execute(f"insert into db.c4 using db.st1 tags(4) values(now+1s, 4);")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(4)

        tdSql.query(f"select * from db.c1")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.c2")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.c3")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.c4")
        tdSql.checkRows(2)

        tdSql.query(f"select * from db.st1")
        tdSql.checkRows(8)

        tdLog.info(f"=============== insert data")
        tdSql.execute(
            f"insert into db.s1 using db.st2 tags(1, 1, 1) values(now, 1, 2);"
        )
        tdSql.execute(
            f"insert into db.s2 using db.st2 tags(2, 2, 2) values(now, 2, 3);"
        )
        tdSql.execute(
            f"insert into db.s3 using db.st2 tags(3, 3, 3) values(now, 3, 4);"
        )
        tdSql.execute(
            f"insert into db.s4 using db.st2 tags(4, 4, 4) values(now, 4, 5);"
        )
        tdSql.execute(
            f"insert into db.s1 using db.st2 tags(1, 1, 1) values(now+1s, 1, 2);"
        )
        tdSql.execute(
            f"insert into db.s2 using db.st2 tags(2, 2, 2) values(now+1s, 2, 3);"
        )
        tdSql.execute(
            f"insert into db.s3 using db.st2 tags(3, 3, 3) values(now+1s, 3, 4);"
        )
        tdSql.execute(
            f"insert into db.s4 using db.st2 tags(4, 4, 4) values(now+1s, 4, 5);"
        )
        tdSql.execute(
            f"insert into db.s1 using db.st2 tags(1, 1, 1) values(now+2s, 1, 2);"
        )
        tdSql.execute(
            f"insert into db.s2 using db.st2 tags(2, 2, 2) values(now+2s, 2, 3);"
        )
        tdSql.execute(
            f"insert into db.s3 using db.st2 tags(3, 3, 3) values(now+2s, 3, 4);"
        )
        tdSql.execute(
            f"insert into db.s4 using db.st2 tags(4, 4, 4) values(now+2s, 4, 5);"
        )

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(8)

        tdSql.query(f"select * from db.s1")
        tdSql.checkRows(3)

        tdSql.query(f"select * from db.s2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from db.s3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from db.s4")
        tdSql.checkRows(3)
