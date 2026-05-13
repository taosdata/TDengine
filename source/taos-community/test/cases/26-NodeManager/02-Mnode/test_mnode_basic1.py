import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic1(self):
        """Mnode create on same dnode

        1. Create an mnode on the dnode2
        2. Delete mnode2
        3. Repeat the above operations

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic1.sim

        """

        clusterComCheck.checkDnodes(2)
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdSql.error(f"create mnode on dnode 1")
        tdSql.error(f"drop mnode on dnode 1")

        tdLog.info(f"=============== create mnode 2")
        tdSql.execute(f"create mnode on dnode 2")

        tdLog.info(f"=============== create mnode 2 finished")

        checkFailed = True
        for i in range(20):
            time.sleep(1)
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.checkRows(2)
            tdSql.checkKeyData(1, 0, 1)
            tdSql.checkKeyData(1, 2, "leader")
            tdSql.checkKeyData(2, 0, 2)
            if tdSql.expectKeyData(2, 2, "follower"):
                checkFailed = False
                break
        if checkFailed:
            tdSql.checkAssert(False)

        tdLog.info(f"============ drop mnode 2")
        tdSql.execute(f"drop mnode on dnode 2")

        tdLog.info(f"============ drop mnode 2 finished")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdSql.error(f"drop mnode on dnode 2")

        tdLog.info(f"=============== create mnodes")
        tdSql.execute(f"create mnode on dnode 2")

        tdLog.info(f"=============== create mnode 2 finished")
        checkFailed = True
        for i in range(20):
            time.sleep(1)
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.checkRows(2)
            tdSql.checkKeyData(1, 0, 1)
            tdSql.checkKeyData(1, 2, "leader")
            tdSql.checkKeyData(2, 0, 2)
            if tdSql.expectKeyData(2, 2, "follower"):
                checkFailed = False
                break
        if checkFailed:
            tdSql.checkAssert(False)
