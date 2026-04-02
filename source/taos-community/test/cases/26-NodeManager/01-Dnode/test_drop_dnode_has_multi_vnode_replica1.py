import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasMultiVnodeReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_multi_vnode_replica1(self):
        """Dnode drop with replca-1 vnodes

        Drop the dnode containing a single-replica vnode, and test the integrity of the data after vnode migration.

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_multi_vnode_replica1.sim

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

        tdLog.info(f"=============== step2 create database")
        tdSql.execute(f"create database d1 vgroups 4")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdSql.checkKeyData(2, 3, 2)
        tdSql.checkKeyData(3, 3, 2)
        tdSql.checkKeyData(4, 3, 2)
        tdSql.checkKeyData(5, 3, 2)

        tdLog.info(f"=============== step3: start dnode 3")
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"=============== step3: drop dnode2")
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(2)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdSql.checkKeyData(2, 3, 3)
        tdSql.checkKeyData(3, 3, 3)
        tdSql.checkKeyData(4, 3, 3)
        tdSql.checkKeyData(5, 3, 3)

        tdSql.execute(f"reset query cache")

        tdLog.info(f"=============== step4: select data")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)
