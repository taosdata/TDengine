import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic5:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic5(self):
        """Mnode basic

        1. Create and delete mnodes on an offline dnode - expected to fail
        2. Create mnodes on a dnode that already has an mnode - expected to fail
        3. Use invalid mnode creation or deletion syntax
        4. Check the status of the dnode

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic5.sim

        """

        tdLog.info(f"=============== step1: create dnodes")
        clusterComCheck.checkDnodes(4)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        sc.dnodeStop(4)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2: create dnodes - with error")
        tdSql.error(f"create mnode on dnode 1;")
        tdSql.error(f"create mnode on dnode 2;")
        tdSql.error(f"create mnode on dnode 3;")
        tdSql.error(f"create mnode on dnode 4;")
        tdSql.error(f"create mnode on dnode 5;")
        tdSql.error(f"create mnode on dnode 6;")

        tdLog.info(f"=============== step3: create mnode 2 and 3")
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)

        tdSql.execute(f"create mnode on dnode 2")
        tdSql.execute(f"create mnode on dnode 3")
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step4: create dnodes - with error")
        tdSql.error(f"create mnode on dnode 1")
        tdSql.error(f"create mnode on dnode 2;")
        tdSql.error(f"create mnode on dnode 3;")
        tdSql.error(f"create mnode on dnode 4;")
        tdSql.error(f"create mnode on dnode 5;")
        tdSql.error(f"create mnode on dnode 6;")

        tdLog.info(f"=============== step5: drop mnodes - with error")
        tdSql.error(f"drop mnode on dnode 1")
        tdSql.error(f"drop mnode on dnode 4")
        tdSql.error(f"drop mnode on dnode 5")
        tdSql.error(f"drop mnode on dnode 6")

        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkMnodeStatus(3, False)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "offline")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(4)
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step6: stop mnode1")
        sc.dnodeStop(1)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkMnodeStatus(3, False)

        tdLog.info(f"=============== step7: start mnode1 and wait it online")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(4)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step8: stop mnode1 and drop it")
        sc.dnodeStop(1)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 4, "offline")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step9: start mnode1 and wait it dropped")
        tdLog.info(f"check mnode has leader step9a")
        clusterComCheck.checkMnodeStatus(3, False)

        tdLog.info(f"start dnode1 step9b")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"drop mnode step9d")
        tdSql.execute(f"drop mnode on dnode 1")
        clusterComCheck.checkMnodeStatus(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)

        tdLog.info(f"=============== stepa: create mnode1 again")
        tdSql.execute(f"create mnode on dnode 1")
        clusterComCheck.checkMnodeStatus(3)
        clusterComCheck.checkDnodes(4)
