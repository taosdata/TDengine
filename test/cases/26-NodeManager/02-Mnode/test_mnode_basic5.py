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
        5. Scale a 3-mnode cluster down to a single mnode on dnode 3 and restart
           it: the cluster must still have exactly one mnode (id 3) and it stays
           leader, i.e. no extra default mnode is re-created on restart
           (ensure-default gate regression guard).

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
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

        tdLog.info(f"=============== stepb: scale down to lone mnode on dnode 3")
        # 3 mnodes now live on dnode 1/2/3. We want a lone mnode on dnode 3.
        # The server refuses to drop the leader mnode outright (it never triggers
        # re-election), and once only 2 mnodes remain one of them is always the
        # leader with no quorum to re-elect the survivor alone. So first make
        # dnode 3 the leader while all 3 mnodes are still up (quorum is preserved
        # across a restart), then drop dnodes 1 and 2, which are followers and
        # drop cleanly while dnode 3 keeps leadership.
        self._make_mnode_leader(3)
        for drop_dnode in (1, 2):
            self._drop_mnode_until_gone(drop_dnode)
        clusterComCheck.checkMnodeStatus(1)

        tdSql.query(f"select id, `role` from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] == 3, (
            f"lone mnode id must be 3, got {tdSql.queryResult[0][0]}"
        )

        tdLog.info(f"=============== stepc: restart lone mnode, must stay 1 mnode")
        # Regression guard for the ensure-default gate. After scaling down, the
        # only mnode lives on dnode 3 and its metadata is already complete, so
        # restarting it must leave the cluster with exactly one mnode (id 3),
        # still the leader. The bug: ensure-default wrongly ran on restart and
        # re-created the default mnode with the hard-coded id 1, so the cluster
        # ended up with an extra, unexpected mnode and two nodes each believing
        # they were leader. The gate now also requires selfDnodeId <= 1, which
        # is false on dnode 3, so ensure-default no longer runs here.
        sc.dnodeStop(3)
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkClusterAlive(1)
        clusterComCheck.checkMnodeStatus(1)

        tdSql.query(f"select id, `role` from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] == 3, (
            "after restart there must still be exactly one mnode, on dnode 3. "
            f"An unexpected mnode id {tdSql.queryResult[0][0]} means "
            "ensure-default wrongly re-created a default mnode."
        )
        assert tdSql.queryResult[0][1] == "leader", (
            f"the only mnode (dnode 3) must be leader, got '{tdSql.queryResult[0][1]}'"
        )

    def _mnode_leader(self):
        """Return the dnode id of the current mnode leader, or None."""
        tdSql.query(f"select id, `role` from information_schema.ins_mnodes")
        for row in tdSql.queryResult:
            if row[1] == "leader":
                return row[0]
        return None

    def _make_mnode_leader(self, dnode_id, timeout=60):
        """Force the mnode on dnode_id to become leader by restarting whichever
        other node currently holds leadership. With 3 mnodes up, quorum survives
        the restart and the group re-elects; repeat until dnode_id wins."""
        for _ in range(timeout):
            leader = self._mnode_leader()
            if leader == dnode_id:
                tdLog.info(f"mnode on dnode {dnode_id} is leader")
                return
            if leader is not None:
                tdLog.info(f"leader is dnode {leader}, restart it to re-elect")
                sc.dnodeStop(leader)
                sc.dnodeStart(leader)
                clusterComCheck.checkDnodes(4)
                clusterComCheck.checkMnodeStatus(3)
            time.sleep(1)
        assert False, f"mnode on dnode {dnode_id} did not become leader in {timeout}s"

    def _drop_mnode_until_gone(self, dnode_id, timeout=30):
        """Drop the (follower) mnode on dnode_id, tolerating transient retries."""
        for _ in range(timeout):
            tdSql.query(f"select id from information_schema.ins_mnodes")
            ids = [row[0] for row in tdSql.queryResult]
            if dnode_id not in ids:
                tdLog.info(f"mnode on dnode {dnode_id} dropped")
                return
            try:
                tdSql.execute(f"drop mnode on dnode {dnode_id}")
            except Exception as e:
                tdLog.info(f"drop mnode on dnode {dnode_id} retry: {e}")
            time.sleep(1)
        assert False, f"failed to drop mnode on dnode {dnode_id} within {timeout}s"
