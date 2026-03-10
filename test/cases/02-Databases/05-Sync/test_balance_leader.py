import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestBalanceLeader:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_balance_leader(self):
        """balance leader

        1. -

        Catalog:
            - Database:Sync

        Since: v3.3.7.0

        Labels: common,ci

        Jira: TS-6480

        History:
            - 2025-9-24 dmchen init

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        tdSql.execute(f"alter dnode 2 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 3 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 4 'supportVnodes' '4'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)
        clusterComCheck.checkDnodeSupportVnodes(2, 4)
        clusterComCheck.checkDnodeSupportVnodes(3, 4)
        clusterComCheck.checkDnodeSupportVnodes(4, 4)

        tdLog.info(f"========== step1")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(4)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdSql.execute(f"create database d1 replica 3 vgroups 3")
        clusterComCheck.checkDbReady("d1")

        tdSql.execute(f"balance vgroup leader")

        clusterComCheck.checkTransactions()
        tdSql.query(f"show transactions")
        tdSql.checkRows(0)

        clusterComCheck.checkDbReady("d1")
        
        tdSql.query(f"show d1.vgroups", show = True)
        c1 = 0
        if tdSql.checkDataV2(0, 4, "leader", True) == True:
            c1 = c1 + 1
        if tdSql.checkDataV2(1, 4, "leader", True) == True:
            c1 = c1 + 1
        if tdSql.checkDataV2(2, 4, "leader", True) == True:
            c1 = c1 + 1
        if c1 != 1:
            tdLog.exit("balance vgroup leader failed c1")

        c2 = 0
        if tdSql.checkDataV2(0, 7, "leader", True) == True:
            c2 = c2 + 1
        if tdSql.checkDataV2(1, 7, "leader", True) == True:
            c2 = c2 + 1
        if tdSql.checkDataV2(2, 7, "leader", True) == True:
            c2 = c2 + 1
        if c2 != 1:
            tdLog.exit("balance vgroup leader failed c2")

        c3 = 0
        if tdSql.checkDataV2(0, 10, "leader", True) == True:
            c3 = c3 + 1
        if tdSql.checkDataV2(1, 10, "leader", True) == True:
            c3 = c3 + 1
        if tdSql.checkDataV2(2, 10, "leader", True) == True:
            c3 = c3 + 1
        if c3 != 1:
            tdLog.exit("balance vgroup leader failed c3")
