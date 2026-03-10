import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestRedistributeVgroupReplica2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_redistribute_vgroup_replica2(self):
        """RDST: replica-2

        1. Start a 3-dnode cluster with supportVnodes=0 on dnode1
        2. Create database d1 (1 vgroup, replica 2) and insert data
        3. Add dnode3 and dnode4 to the cluster
        4. Execute REDISTRIBUTE VGROUP to move the vnode to dnode4 dnode5; verify distribution & data integrity
        5. Execute REDISTRIBUTE VGROUP to move the vnode to dnode2 dnode3; verify distribution & data integrity

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-2 Dongming Chen Initial version

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(4)
        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 1 replica 2")

        sc.dnodeStart(4)
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(5)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "ready")

        tdLog.info(f"=============== step3: create table")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step40:")
        tdLog.info(f"redistribute vgroup 2 dnode 4 dnode 5")
        tdSql.execute(f"redistribute vgroup 2 dnode 4 dnode 5")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step41:")
        tdLog.info(f"redistribute vgroup 2 dnode 2 dnode 3")
        tdSql.execute(f"redistribute vgroup 2 dnode 2 dnode 3")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)
