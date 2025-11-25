import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeForce:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_force(self):
        """Dnode drop force unsafe

        1. Create 5 dnodes, establish a three-replica database on them, and write data
        2. Create three mnodes
        3. Stop one dnode and verify that it cannot be deleted
        4. Use drop dnode force unsafe to forcibly delete this dnode

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_force.sim

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

        tdLog.info(f"=============== step2 create database")
        tdSql.execute(f"create database d1 vgroups 1 replica 3")
        clusterComCheck.checkDbReady("d1")
        tdSql.execute(f"use d1")

        tdSql.query(f"show transactions")
        tdSql.checkRows(0)

        tdSql.execute(f"create table d1.st0 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c0 using st0 tags(0)")
        tdSql.execute(f"create table d1.c1 using st0 tags(1)")
        tdSql.execute(f"create table d1.c2 using st0 tags(2)")
        tdSql.execute(f"create table d1.c3 using st0 tags(3)")
        tdSql.execute(f"create table d1.c4 using st0 tags(4)")
        tdSql.execute(f"create table d1.c5 using st0 tags(5)")
        tdSql.execute(f"create table d1.c6 using st0 tags(6)")
        tdSql.execute(f"create table d1.c7 using st0 tags(7)")
        tdSql.execute(f"create table d1.c8 using st0 tags(8)")
        tdSql.execute(f"create table d1.c9 using st0 tags(9)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(10)

        tdLog.info(f"d1.rows ===> {tdSql.getRows()})")
        tdSql.query(
            f"select * from information_schema.ins_tables where stable_name = 'st0' and db_name = 'd1'"
        )
        tdSql.checkRows(10)

        tdSql.execute(f"create database d2 vgroups 3 replica 1")
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table d2.st1 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c10 using st1 tags(0)")
        tdSql.execute(f"create table d2.c11 using st1 tags(1)")
        tdSql.execute(f"create table d2.c12 using st1 tags(2)")
        tdSql.execute(f"create table d2.c13 using st1 tags(3)")
        tdSql.execute(f"create table d2.c14 using st1 tags(4)")
        tdSql.execute(f"create table d2.c15 using st1 tags(5)")
        tdSql.execute(f"create table d2.c16 using st1 tags(6)")
        tdSql.execute(f"create table d2.c17 using st1 tags(7)")
        tdSql.execute(f"create table d2.c18 using st1 tags(8)")
        tdSql.execute(f"create table d2.c19 using st1 tags(9)")
        tdSql.execute(f"create table d2.c190 using st1 tags(9)")
        tdSql.query(f"show d2.tables")
        tdSql.checkRows(11)

        tdSql.execute(f"reset query cache")
        tdSql.query(
            f"select * from information_schema.ins_tables where stable_name = 'st1' and db_name = 'd2'"
        )
        tdLog.info(f"d2.st1.tables ===> {tdSql.getRows()})")
        tdSql.checkRows(11)

        tdSql.execute(f"create table d2.st2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c20 using st2 tags(0)")
        tdSql.execute(f"create table d2.c21 using st2 tags(1)")
        tdSql.execute(f"create table d2.c22 using st2 tags(2)")
        tdSql.execute(f"create table d2.c23 using st2 tags(3)")
        tdSql.execute(f"create table d2.c24 using st2 tags(4)")
        tdSql.execute(f"create table d2.c25 using st2 tags(5)")
        tdSql.execute(f"create table d2.c26 using st2 tags(6)")
        tdSql.execute(f"create table d2.c27 using st2 tags(7)")
        tdSql.execute(f"create table d2.c28 using st2 tags(8)")
        tdSql.execute(f"create table d2.c29 using st2 tags(9)")
        tdSql.execute(f"create table d2.c290 using st2 tags(9)")
        tdSql.execute(f"create table d2.c291 using st2 tags(9)")
        tdSql.query(f"show d2.tables")
        tdSql.checkRows(23)

        tdSql.execute(f"reset query cache")
        tdSql.query(
            f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'"
        )
        tdLog.info(f"d2.st2.tables ===> {tdSql.getRows()})")
        tdSql.checkRows(12)

        tdLog.info(f"=============== step3: create qnode snode on dnode 3")
        tdSql.execute(f"create qnode on dnode 3")
        tdSql.execute(f"create snode on dnode 3")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)

        tdSql.query(f"show snodes")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step4: create mnode on dnode 2")
        tdSql.execute(f"create mnode on dnode 3")
        tdSql.execute(f"create mnode on dnode 2")
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step5: create dnode 5")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(3)
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(4)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "offline")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "ready")

        tdLog.info(f"=============== step5a: drop dnode 3")
        tdSql.error(f"drop dnode 3")
        tdSql.error(f"drop dnode 3 force")
        tdSql.execute(f"drop dnode 3 unsafe")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(4)

        tdLog.info(f"select * from information_schema.ins_mnodes;")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)
        tdSql.checkKeyData(1, 2, "leader")

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6: check d1")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step7: check d2")
        tdSql.query(f"show d2.tables")
        tdLog.info(f"===> d2.tables: {tdSql.getRows()}) remained")
        tdSql.checkAssert(tdSql.getRows() <= 23)
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdLog.info(f"=============== step8: drop stable and recreate it")
        tdSql.query(
            f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'"
        )
        tdLog.info(f"d2.st2.tables ==> {tdSql.getRows()})")

        tdSql.execute(f"drop table d2.st2;")
        tdSql.execute(f"create table d2.st2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c20 using st2 tags(0)")
        tdSql.execute(f"create table d2.c21 using st2 tags(1)")
        tdSql.execute(f"create table d2.c22 using st2 tags(2)")
        tdSql.execute(f"create table d2.c23 using st2 tags(3)")
        tdSql.execute(f"create table d2.c24 using st2 tags(4)")
        tdSql.execute(f"create table d2.c25 using st2 tags(5)")
        tdSql.execute(f"create table d2.c26 using st2 tags(6)")
        tdSql.execute(f"create table d2.c27 using st2 tags(7)")
        tdSql.execute(f"create table d2.c28 using st2 tags(8)")
        tdSql.execute(f"create table d2.c29 using st2 tags(9)")
        tdSql.execute(f"create table d2.c30 using st2 tags(9)")
        tdSql.execute(f"create table d2.c31 using st2 tags(9)")
        tdSql.execute(f"create table d2.c32 using st2 tags(9)")

        tdSql.query(
            f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'"
        )
        tdLog.info(f"d2.st2.tables ==> {tdSql.getRows()})")
        tdSql.checkRows(13)

        tdLog.info(f"=============== step9: alter stable")
