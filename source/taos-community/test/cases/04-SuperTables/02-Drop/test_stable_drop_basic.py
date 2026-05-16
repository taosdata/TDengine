from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableDropBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_drop_basic(self):
        """Drop: basic

        1. Create a super table
        2. Create a child table
        3. Create a normal table
        4. Insert data
        5. Drop the super table

        Catalog:
            - SuperTable:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/table/basic3.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdLog.info(f"=============== create normal table")
        tdSql.execute(f"create table db.n1 (ts timestamp, i int)")
        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create table db.st (ts timestamp, i int) tags (j int)")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table db.c1 using db.st tags(1)")
        tdSql.execute(f"create table db.c2 using db.st tags(2)")
        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,2)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(1,1)} {tdSql.getData(2,2)}")

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"insert into db.n1 values(now+1s, 1)")
        tdSql.execute(f"insert into db.n1 values(now+2s, 2)")
        tdSql.execute(f"insert into db.n1 values(now+3s, 3)")

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from db.n1")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(1,1)}")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)

        tdLog.info(f"=============== drop stable")
        tdSql.execute(f"drop table db.st")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(0)
