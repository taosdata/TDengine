import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestRedistributeVgroupReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_redistribute_vgroup_replica1(self):
        """redistribute vgroup replica 1

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/redistribute_vgroup_replica1.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(3)
        sc.dnodeStop(4)
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(4)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 1 replica 1")

        sc.dnodeStart(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(4)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step3: create database")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step40:")
        tdLog.info(f"redistribute vgroup 2 dnode 3")
        tdSql.execute(f"redistribute vgroup 2 dnode 3")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step41:")
        tdLog.info(f"redistribute vgroup 2 dnode 4")
        tdSql.execute(f"redistribute vgroup 2 dnode 4")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step42:")
        tdLog.info(f"redistribute vgroup 2 dnode 2")
        tdSql.execute(f"redistribute vgroup 2 dnode 2")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step43:")
        tdLog.info(f"redistribute vgroup 2 dnode 3")
        tdSql.execute(f"redistribute vgroup 2 dnode 3")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step44:")
        tdLog.info(f"redistribute vgroup 2 dnode 4")
        tdSql.execute(f"redistribute vgroup 2 dnode 4")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step45:")
        tdLog.info(f"redistribute vgroup 2 dnode 2")
        tdSql.execute(f"redistribute vgroup 2 dnode 2")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step46:")
        tdLog.info(f"redistribute vgroup 2 dnode 3")
        tdSql.execute(f"redistribute vgroup 2 dnode 3")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step47:")
        tdLog.info(f"redistribute vgroup 2 dnode 2")
        tdSql.execute(f"redistribute vgroup 2 dnode 2")
        tdSql.query(f"show d1.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)
