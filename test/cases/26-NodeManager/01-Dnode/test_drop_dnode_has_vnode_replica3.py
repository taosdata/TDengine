import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasVnodeReplica3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_vnode_replica3(self):
        """Dnode drop with replca-3 vnode

        Drop the dnode containing a three-replica vnode, and test the integrity of the data after vnode migration.

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_vnode_replica3.sim

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
        tdSql.execute(f"create database d1 vgroups 1 replica 3")
        clusterComCheck.checkDbReady("d1")

        leaderExist = 0
        leaderVnode = 0
        follower1 = 0
        follower2 = 0

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)

        if tdSql.getData(0, 4) == "leader":
            leaderExist = 1
            leaderVnode = 2
            follower1 = 3
            follower2 = 4

        if tdSql.getData(0, 7) == "leader":
            leaderExist = 1
            leaderVnode = 3
            follower1 = 2
            follower2 = 4

        if tdSql.getData(0, 10) == "leader":
            leaderExist = 1
            leaderVnode = 4
            follower1 = 2
            follower2 = 3

        tdLog.info(f"leader {leaderVnode}")
        tdLog.info(f"follower1 {follower1}")
        tdLog.info(f"follower2 {follower2}")

        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)

        tdSql.checkKeyData(2, 3, 2)
        tdSql.checkKeyData(2, 6, 3)
        tdSql.checkKeyData(2, 9, 4)

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

        tdLog.info(f"show d1.vgroups")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkKeyData(2, 3, 5)

        tdLog.info(f"=============== step6: select data")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)
        clusterComCheck.checkDbReady("d1")

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
