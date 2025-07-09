import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestSplitVgroupReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_split_vgroup_replica1(self):
        """split vgroup replica 1

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/split_vgroup_replica1.sim

        """

        clusterComCheck.checkDnodes(3)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "offline")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 1 replica 1")

        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"=============== step3: create database")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.execute(f"create table d1.c2 using st tags(2)")
        tdSql.execute(f"create table d1.c3 using st tags(3)")
        tdSql.execute(f"create table d1.c4 using st tags(4)")
        tdSql.execute(f"create table d1.c5 using st tags(5)")
        tdSql.execute(f"insert into d1.c1 values (now, 1);")
        tdSql.execute(f"insert into d1.c2 values (now, 2);")
        tdSql.execute(f"insert into d1.c3 values (now, 3);")
        tdSql.execute(f"insert into d1.c4 values (now, 4);")
        tdSql.execute(f"insert into d1.c5 values (now, 5);")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(5)

        tdSql.query(f"select * from d1.st")
        tdSql.checkRows(5)

        tdLog.info(f"=============== step4: split")
        tdLog.info(f"split vgroup 2")
        tdSql.execute("split vgroup 2")

        clusterComCheck.checkTransactions()
        tdSql.query(f"show transactions")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5: check split result")
        tdSql.query(f"show d1.tables")

        tdLog.info(f"=============== step6: create tables")
        tdSql.execute(f"create table d1.c6 using st tags(6)")
        tdSql.execute(f"create table d1.c7 using st tags(7)")
        tdSql.execute(f"create table d1.c8 using st tags(8)")
        tdSql.execute(f"create table d1.c9 using st tags(9)")
        tdSql.execute(f"insert into d1.c6 values (now, 6);")
        tdSql.execute(f"insert into d1.c7 values (now, 7);")
        tdSql.execute(f"insert into d1.c8 values (now, 8);")
        tdSql.execute(f"insert into d1.c9 values (now, 9);")
        tdSql.query(f"show d1.tables")
