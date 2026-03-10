import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasMultiVnodeReplica3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_multi_vnode_replica3(self):
        """Dnode drop with replca-3 vnodes

        Drop the dnode containing a three-replica vnode, and test the integrity of the data after vnode migration.

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_multi_vnode_replica3.sim

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"=============== step1 create dnode2 dnode3 dnode4 dnode 5")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "offline")

        tdLog.info(f"=============== step3 create database")
        tdSql.execute(f"create database d1 vgroups 4 replica 3")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step32 wait vgroup2")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step33 wait vgroup3")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step34 wait vgroup4")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step35 wait vgroup5")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdSql.query(f"show transactions")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step36: create table")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step4: drop dnode 2")
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(5)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "ready")

        tdLog.info(f"=============== step5: drop dnode2")
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step62 wait vgroup2")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step63 wait vgroup3")

        tdLog.info(f"=============== step64 wait vgroup4")

        tdLog.info(f"=============== step35 wait vgroup5")

        tdLog.info(f"=============== step7: select data")
        tdLog.info(f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)
