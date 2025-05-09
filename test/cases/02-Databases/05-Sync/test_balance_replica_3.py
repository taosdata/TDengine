import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestBalanceReplica3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_balance_replica_3(self):
        """balance replica 3

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/balance_replica3.sim

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 4 replica 3")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step32 wait vgroup2")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step33 wait vgroup3")

        tdLog.info(f"=============== step34 wait vgroup4")

        tdLog.info(f"=============== step35 wait vgroup5")

        tdLog.info(f"=============== step36: create table")
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

        tdLog.info(f"=============== step4: start dnode5")
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(5)

        tdLog.info(f"=============== step5: balance")
        tdSql.execute(f"balance vgroup")

        tdLog.info(f"=============== step62 wait vgroup2")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step63 wait vgroup3")

        tdLog.info(f"=============== step64 wait vgroup4")

        tdLog.info(f"=============== step65 wait vgroup5")

        tdLog.info(f"=============== step7: select data")
        tdSql.query(f"show d1.tables")
        tdLog.info(f"rows {tdSql.getRows()})")
        tdSql.checkRows(6)
