import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic4(self):
        """Mnode kill -9 then drop

        1. Create mnodes on the second and third dnodes
        2. Force stop dnode3
        3. Start dnode3
        4. Delete dnode3
        5. Check service availability

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic4.sim

        """

        clusterComCheck.checkDnodes(3)
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step2: create mnode 2")
        tdSql.execute(f"create mnode on dnode 2")
        tdSql.error(f"create mnode on dnode 3")

        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"=============== step4: create mnode 3")
        tdSql.execute(f"create mnode on dnode 3")
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step5: drop mnode 3 and stop dnode3")
        sc.dnodeForceStop(3)
        clusterComCheck.checkDnodes(2)
        tdSql.error(f"drop mnode on dnode 3")

        tdLog.info(f"=============== step6: start dnode3")
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        if tdSql.getData(0, 2) == "leader":
            tdLog.info(f"drop mnode on dnode {tdSql.getData(0,0)}")
            tdSql.error(f"drop mnode on dnode {tdSql.getData(0,0)}")
        elif tdSql.getData(1, 2) == "leader":
            tdLog.info(f"drop mnode on dnode {tdSql.getData(0,0)}")
            tdSql.error(f"drop mnode on dnode {tdSql.getData(0,0)}")
        elif tdSql.getData(2, 2) == "leader":
            tdLog.info(f"drop mnode on dnode {tdSql.getData(0,0)}")
            tdSql.error(f"drop mnode on dnode {tdSql.getData(0,0)}")
