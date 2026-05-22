from new_test_framework.utils import tdLog, tdSql, clusterComCheck


class TestBalanceLeaderReplica2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_balance_leader_replica2(self):
        """balance leader with replica 2

        1. Verify that balance vgroup leader works correctly for replica-2 databases.
           Previously this failed with "No VGroup's leader need to be balanced" because
           the election baseline loop hardcoded i < 3 instead of i < replica, causing
           an invalid dnode-0 lookup for the non-existent third vnodeGid slot.

        Catalog:
            - Database:Sync

        Since: v3.3.7.0

        Labels: common,ci

        Jira: TS-6480

        History:
            - 2026-5-22 dmchen add replica2 case

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

        tdLog.info(f"========== step1: create replica 2 database")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(4)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdSql.execute(f"create database d1 replica 2 vgroups 4")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"========== step2: balance vgroup leader on replica 2 db")
        tdSql.execute(f"balance vgroup leader")

        clusterComCheck.checkTransactions()
        tdSql.query(f"show transactions")
        tdSql.checkRows(0)

        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"========== step3: verify each vgroup has exactly one leader")
        tdSql.query(f"show d1.vgroups", show=True)
        for row in range(4):
            c = 0
            if tdSql.checkDataV2(row, 4, "leader", True) == True:
                c += 1
            if tdSql.checkDataV2(row, 7, "leader", True) == True:
                c += 1
            if c != 1:
                tdLog.exit(f"balance vgroup leader replica2 failed: row{row} leader count={c}, expected 1")
