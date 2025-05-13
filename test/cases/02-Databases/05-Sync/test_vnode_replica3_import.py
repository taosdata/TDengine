import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeReplica3Import:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_replica3_import(self):
        """vnode replica3 import

        1. -

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/replica3_import.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 2 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 3 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 4 'supportVnodes' '4'")

        clusterComCheck.checkDnodeSupportVnodes(1, 4)
        clusterComCheck.checkDnodeSupportVnodes(2, 4)
        clusterComCheck.checkDnodeSupportVnodes(3, 4)
        clusterComCheck.checkDnodeSupportVnodes(4, 4)

        tdLog.info(f"========== step1")

        tdSql.execute(f"create database ir3db replica 3 duration 7 vgroups 1")
        clusterComCheck.checkDbReady("ir3db")
        tdSql.execute(f"use ir3db")
        tdSql.execute(f"create table tb(ts timestamp, i bigint)")

        tdLog.info(f"================= step1")
        tdSql.execute(f"insert into tb values(1520000010000, 1520000010000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdLog.info(f"================= step2")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(2)

        tdLog.info(f"================= step3")
        tdSql.execute(f"insert into tb values(1520000020000, 1520000020000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)

        tdLog.info(f"================= step4")
        tdSql.execute(f"insert into tb values(1520000009000, 1520000009000)")
        tdSql.execute(f"insert into tb values(1520000015000, 1520000015000)")
        tdSql.execute(f"insert into tb values(1520000030000, 1520000030000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(6)

        tdLog.info(f"================= step5")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdSql.execute(f"insert into tb values(1520000014000, 1520000014000)")
        tdSql.execute(f"insert into tb values(1520000025000, 1520000025000)")
        tdSql.execute(f"insert into tb values(1520000040000, 1520000040000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        tdLog.info(f"================= step6")
        tdSql.execute(f"insert into tb values(1520000007000, 1520000007000)")
        tdSql.execute(f"insert into tb values(1520000012000, 1520000012000)")
        tdSql.execute(f"insert into tb values(1520000023000, 1520000023000)")
        tdSql.execute(f"insert into tb values(1520000034000, 1520000034000)")
        tdSql.execute(f"insert into tb values(1520000050000, 1520000050000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(14)

        tdLog.info(f"================== step7")
        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(3)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkDbReady("ir3db")

        tdLog.info(f"================= step7")
        tdSql.execute(f"insert into tb values(1520000007001, 1520000007001)")
        tdSql.execute(f"insert into tb values(1520000012001, 1520000012001)")
        tdSql.execute(f"insert into tb values(1520000023001, 1520000023001)")
        tdSql.execute(f"insert into tb values(1520000034001, 1520000034001)")
        tdSql.execute(f"insert into tb values(1520000050001, 1520000050001)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(19)

        tdLog.info(f"================= step8")
        tdSql.execute(f"insert into tb values(1520000008002, 1520000008002)")
        tdSql.execute(f"insert into tb values(1520000014002, 1520000014002)")
        tdSql.execute(f"insert into tb values(1520000025002, 1520000025002)")
        tdSql.execute(f"insert into tb values(1520000060000, 1520000060000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(23)

        tdLog.info(f"================= step9")
        tdSql.execute(f"insert into tb values(1517408000000, 1517408000000)")
        tdSql.execute(f"insert into tb values(1518272000000, 1518272000000)")
        tdSql.execute(f"insert into tb values(1519136000000, 1519136000000)")
        tdSql.execute(f"insert into tb values(1519568000000, 1519568000000)")
        tdSql.execute(f"insert into tb values(1519654400000, 1519654400000)")
        tdSql.execute(f"insert into tb values(1519827200000, 1519827200000)")
        tdSql.execute(f"insert into tb values(1520345600000, 1520345600000)")
        tdSql.execute(f"insert into tb values(1520691200000, 1520691200000)")
        tdSql.execute(f"insert into tb values(1520864000000, 1520864000000)")
        tdSql.execute(f"insert into tb values(1521900800000, 1521900800000)")
        tdSql.execute(f"insert into tb values(1523110400000, 1523110400000)")
        tdSql.execute(f"insert into tb values(1521382400000, 1521382400000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step10")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(3)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkDbReady("ir3db")

        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step11")
        tdSql.execute(
            f"insert into tb values(1515680000000, 1) (1515852800000, 2) (1516025600000, 3) (1516198400000, 4) (1516371200000, 5)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(40)

        tdLog.info(f"================= step12")
        tdSql.execute(
            f"insert into tb values(1518358400000, 6) (1518444800000, 7) (1518531200000, 8) (1518617600000, 9) (1518704000000, 10) (1518790400000, 11) (1518876800000, 12) (1518963200000, 13) (1519049600000, 14)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(49)

        tdLog.info(f"================= step13")

        sc.dnodeStop(4)
        clusterComCheck.checkDnodes(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkDbReady("ir3db")

        tdLog.info(f"================= step14")
        tdSql.execute(f"insert into tb values(1515852800001, -48)")
        tdSql.execute(f"insert into tb values(1516716800000, -38)")
        tdSql.execute(f"insert into tb values(1517580800000, -28)")

        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(52)

        tdLog.info(f"================= step15")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(3)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkDbReady("ir3db")

        tdSql.query(f"select * from tb;")
        tdSql.checkRows(52)
