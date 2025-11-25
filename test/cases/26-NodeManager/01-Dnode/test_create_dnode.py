import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestCreateDnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_create_dnode(self):
        """Dnode create

        1. Create dnode2
        2. Check system tables such as ins_dnodes and ins_mnodes
        3. Create database tables on these two dnodes and perform basic write and query operations

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/create_dnode.sim

        """

        clusterComCheck.checkDnodes(1)
        clusterComCheck.checkMnodeStatus(1)

        tdLog.info(f"=============== select * from information_schema.ins_dnodes")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, "leader")

        tdLog.info(f"=============== create dnodes")
        tdSql.execute(f"create dnode localhost port 6130")
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")

        tdSql.query(f"select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, "leader")

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d1 vgroups 4;")
        tdSql.execute(f"create database d2;")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.execute(f"use d1")
        tdSql.query(f"show vgroups;")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create table")
        tdSql.execute(f"use d1")

        tdSql.execute(f"create table st (ts timestamp, i int) tags (j int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table c1 using st tags(1)")
        tdSql.execute(f"create table c2 using st tags(2)")
        tdSql.execute(f"create table c3 using st tags(2)")
        tdSql.execute(f"create table c4 using st tags(2)")
        tdSql.execute(f"create table c5 using st tags(2)")

        tdSql.query(f"show tables")
        tdSql.checkRows(5)

        tdLog.info(f"=============== insert data")
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

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdSql.checkRows(3)
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

        tdSql.query(f"select ts, i from st")
        tdSql.checkRows(15)
