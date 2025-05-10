import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestBalanceReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_balance_replica_1(self):
        """balance replica 1

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/balance_replica1.sim

        """

        clusterComCheck.checkDnodes(3)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")
        # no enough vnodes
        tdSql.error(f"balance vgroup")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "offline")

        tdLog.info(f"=============== step2 create database")
        tdSql.execute(f"create database d1 vgroups 2")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.execute(f"create table d1.c2 using st tags(1)")
        tdSql.execute(f"create table d1.c3 using st tags(1)")
        tdSql.execute(f"create table d1.c4 using st tags(1)")
        tdSql.execute(f"create table d1.c5 using st tags(1)")
        tdSql.execute(f"create table d1.c6 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(6)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(2)
        tdSql.checkKeyData(2, 3, 2)
        tdSql.checkKeyData(3, 3, 2)

        tdLog.info(f"=============== step3: balance vgroup")
        # has offline dnode
        tdSql.error(f"balance vgroup")
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"=============== step4: balance")
        tdSql.execute(f"balance vgroup")

        tdLog.info(f"show d1.vgroups")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(2)
        tdSql.checkKeyData(2, 3, 3)
        tdSql.checkKeyData(3, 3, 2)

        tdLog.info(f"=============== step7: select data")
        tdSql.query(f"show d1.tables")
        tdLog.info(f"rows {tdSql.getRows()})")
        tdSql.checkRows(6)
