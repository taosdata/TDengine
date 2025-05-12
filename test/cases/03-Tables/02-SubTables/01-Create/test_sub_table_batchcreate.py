from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableBatchCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_batch_create(self):
        """batch create subtable

        1. create stable
        2. batch create sub table
        3. query from stable

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/basic1.sim

        """

        # =========== TD-14042 start
        tdSql.prepare(dbname="db1")
        tdSql.prepare(dbname="db2")
        tdSql.prepare(dbname="db3")

        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table st1 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")

        tdSql.execute(f"use db2;")
        tdSql.execute(f"create table st2 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb2 using st2 tags(1);")

        tdSql.execute(f"use db3;")
        tdSql.execute(f"create table st3 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb3 using st3 tags(1);")

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        # =========== TD-14042 end

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(6)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use d1")

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create table st (ts timestamp, i int) tags (j int)")
        tdSql.execute(
            f"create table if not exists st (ts timestamp, i int) tags (j int)"
        )
        tdSql.execute(
            f"create table if not exists st (ts timestamp, i int) tags (j int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table st2 (ts timestamp, i float) tags (j int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table st2")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table c1 using st tags(1)")
        tdSql.execute(f"create table c2 using st tags(2)")

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.execute(
            f"create table c3 using st tags(3) c4 using st tags(4) c5 using st tags(5) c6 using st tags(6) c7 using st tags(7)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdLog.info(f"=============== create normal table")
        tdSql.prepare(dbname="ndb")
        tdSql.execute(f"use ndb")
        tdSql.execute(f"create table nt0 (ts timestamp, i int)")
        # sql create table if not exists nt0 (ts timestamp, i int)
        tdSql.execute(f"create table nt1 (ts timestamp, i int)")
        # sql create table if not exists nt1 (ts timestamp, i int)
        tdSql.execute(f"create table if not exists nt3 (ts timestamp, i int)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdSql.execute(f"insert into nt0 values(now+1s, 1)(now+2s, 2)(now+3s, 3)")
        tdSql.execute(f"insert into nt1 values(now+1s, 1)(now+2s, 2)(now+3s, 3)")

        tdSql.query(f"select * from nt1")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"use d1")
        tdSql.execute(f"insert into c1 values(now+1s, 1)")
        tdSql.execute(f"insert into c1 values(now+2s, 2)")
        tdSql.execute(f"insert into c1 values(now+3s, 3)")
        tdSql.execute(f"insert into c2 values(now+1s, 1)")
        tdSql.execute(f"insert into c2 values(now+2s, 2)")
        tdSql.execute(f"insert into c2 values(now+3s, 3)")
        tdSql.execute(f"insert into c3 values(now+1s, 1)")
        tdSql.execute(f"insert into c3 values(now+2s, 2)")
        tdSql.execute(f"insert into c3 values(now+3s, 3)")
        tdSql.execute(f"insert into c4 values(now+1s, 1)")
        tdSql.execute(f"insert into c4 values(now+2s, 2)")
        tdSql.execute(f"insert into c4 values(now+3s, 3)")
        tdSql.execute(f"insert into c5 values(now+1s, 1)")
        tdSql.execute(f"insert into c5 values(now+2s, 2)")
        tdSql.execute(f"insert into c5 values(now+3s, 3)")
        tdSql.execute(f"insert into c6 values(now+1s, 1)")
        tdSql.execute(f"insert into c6 values(now+2s, 2)")
        tdSql.execute(f"insert into c6 values(now+3s, 3)")
        tdSql.execute(f"insert into c7 values(now+1s, 1)")
        tdSql.execute(f"insert into c7 values(now+2s, 2)")
        tdSql.execute(f"insert into c7 values(now+3s, 3)")

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)

        tdSql.query(f"select * from c2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c4")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c5")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c6")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c7")
        tdSql.checkRows(3)

        tdLog.info(f"=============== query data from st")
        tdLog.info(f"==============select * against super will cause crash.")
        tdSql.query(f"select ts from st")
        tdSql.checkRows(21)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)

        tdSql.query(f"select * from c2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c4")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c5")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c6")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c7")
        tdSql.checkRows(3)

        tdLog.info(f"=============== query data from st")
        tdSql.query(f"select ts from st")
        tdSql.checkRows(21)

        tdLog.info(f"=============== query data from normal table after restart dnode")
        tdSql.execute(f"use ndb")
        tdSql.query(f"select * from nt1")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)
