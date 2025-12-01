import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic6:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic6(self):
        """Mnode repeatedly restart

        1. Create mnodes on dnode2 and dnode3
        2. Repeatedly restart dnode1 to dnode3 sequentially
        3. Check service availability

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic6.sim

        """
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

        tdSql.error(f"drop mnode on dnode 2")
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step6: stop mnode1")
        sc.dnodeStop(1)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkMnodeStatus(3, False)

        tdLog.info(f"=============== step7: start mnode1 and wait it online")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step8: stop mnode1 and drop it")
        sc.dnodeStop(1)
        clusterComCheck.checkDnodes(3)

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

        tdLog.info(f"check mnode leader")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)

        tdLog.info(f"=============== stepa: create mnode1 again")
        tdSql.execute(f"create mnode on dnode 1")
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"check mnode leader")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(3)
        clusterComCheck.checkDnodes(4)
