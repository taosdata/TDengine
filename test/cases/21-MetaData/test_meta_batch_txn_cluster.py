###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

"""
Cluster-mode integration tests for Batch Metadata Transaction (2PC) feature.

Requires multi-dnode deployment:
  ./ci/pytest.sh pytest cases/21-MetaData/test_meta_batch_txn_cluster.py -N 3 -M 3

Tests cover:
  - MNode leader switchover during active transaction
  - taos client disconnect → transaction auto-rollback
  - VNode leader dnode restart during active transaction
  - VNode dnode restart/recovery at each transaction stage
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time


class TestBatchMetaTxnCluster:
    """Cluster-mode batch meta txn tests (3 dnode, 3 mnode)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _reset_env(self, db_name="txn_cdb"):
        """Reset test database. Uses replica 3 for VNode HA tests."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 replica 3")
        tdSql.execute(f"use {db_name}")

    def _get_mnode_leader_dnode_id(self):
        """Get the dnode ID of the current MNode leader."""
        tdSql.query("select * from information_schema.ins_mnodes")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                return tdSql.queryResult[i][0]      # id column (1-based dnode id)
        return None

    def _get_vgroup_leader_dnode(self, db_name, vgId, timeout=30):
        """Get the dnode ID of the vgroup leader, with retry."""
        for attempt in range(timeout):
            tdSql.query(f"show {db_name}.vgroups")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == vgId:
                    row = tdSql.queryResult[i]
                    for j in range(len(row)):
                        if row[j] == 'leader':
                            return row[j - 1]           # dnode id is the column before status
            if attempt < timeout - 1:
                time.sleep(1)
        return None

    def _wait_mnode_leader_elected(self, timeout=30):
        """Wait for any MNode leader to be elected (ignoring offline nodes)."""
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for r in range(tdSql.queryRows):
                    if tdSql.queryResult[r][2] == 'leader':
                        tdLog.info(f"MNode leader found: dnode {tdSql.queryResult[r][0]} after {i+1}s")
                        return True
            except Exception:
                continue
        tdLog.exit(f"No MNode leader elected within {timeout}s")
        return False

    # =========================================================================
    # s40: MNode leader switch during active txn -> COMMIT succeeds
    # =========================================================================
    def s40_mnode_leader_switch_commit(self):
        self._reset_env()
        tdLog.info("======== s40_mnode_leader_switch_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Start transaction and create tables
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")

        # Find and kill MNode leader
        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it")
        sc.dnodeForceStop(leader_id)

        # Wait for new leader election (one node is offline)
        clusterComCheck.check3mnodeoff(leader_id)

        # COMMIT should succeed via new leader
        # (STxnObj is replicated in SDB, new leader can coordinate)
        tdSql.execute("COMMIT")

        # Restart killed dnode and wait for full cluster
        sc.dnodeStart(leader_id)
        time.sleep(3)
        clusterComCheck.checkDnodes(3)

        # Verify tables exist
        tdSql.query("show tables")
        tdSql.checkRows(2)

    # =========================================================================
    # s41: MNode leader switch during active txn -> ROLLBACK succeeds
    # =========================================================================
    def s41_mnode_leader_switch_rollback(self):
        self._reset_env()
        tdLog.info("======== s41_mnode_leader_switch_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")

        # Kill MNode leader
        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it")
        sc.dnodeForceStop(leader_id)
        clusterComCheck.check3mnodeoff(leader_id)

        # ROLLBACK via new leader
        tdSql.execute("ROLLBACK")

        sc.dnodeStart(leader_id)
        time.sleep(3)
        clusterComCheck.checkDnodes(3)

        # Tables should not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)

    def _poll_table_count(self, expected, db_name="txn_cdb", timeout=180):
        """Poll 'show tables' until expected row count or timeout."""
        last_count = -1
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.execute(f"use {db_name}")
                tdSql.query("show tables")
                last_count = tdSql.queryRows
                if last_count == expected:
                    tdLog.info(f"Table count reached {expected} after {i+1}s")
                    return True
            except Exception as e:
                tdLog.info(f"_poll_table_count: query failed at {i+1}s: {e}")
                continue
        tdLog.exit(f"Table count {last_count} != expected {expected} after {timeout}s")
        return False

    # =========================================================================
    # s42: Client disconnect -> txn auto-rollback after timeout
    # =========================================================================
    def s42_client_disconnect_auto_rollback(self):
        self._reset_env()
        tdLog.info("======== s42_client_disconnect_auto_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Use a separate connection that we can close
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_cdb")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_temp using stb tags(1)")

        # Close the connection without COMMIT/ROLLBACK
        tdSql2.close()

        # Poll until txn auto-rollback completes (ACTIVE timeout + timer scan)
        tdLog.info("Polling for txn timeout auto-rollback...")
        self._poll_table_count(0)

    # =========================================================================
    # s43: VNode follower restart during active txn -> COMMIT succeeds
    # =========================================================================
    def s43_vnode_follower_restart_commit(self):
        self._reset_env()
        tdLog.info("======== s43_vnode_follower_restart_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")
        tdSql.execute("create table ct2 using stb tags(2)")

        # Find a VNode follower dnode (not the leader)
        tdSql.query("show txn_cdb.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode("txn_cdb", vgId)

        # Find a follower dnode (any dnode that is not the leader of this vgroup)
        follower_dnode = None
        tdSql.query("select * from information_schema.ins_dnodes")
        for i in range(tdSql.queryRows):
            did = tdSql.queryResult[i][0]
            if did != leader_dnode:
                follower_dnode = did
                break
        assert follower_dnode is not None, "Could not find a follower dnode"

        tdLog.info(f"Vgroup {vgId} leader on dnode {leader_dnode}, killing follower dnode {follower_dnode}")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # COMMIT should succeed (quorum still met with 2/3 replicas)
        tdSql.execute("COMMIT")

        sc.dnodeStart(follower_dnode)
        time.sleep(3)
        clusterComCheck.checkDnodes(3)

        # Verify tables
        tdSql.query("show tables")
        tdSql.checkRows(2)

    # =========================================================================
    # s44: VNode leader dnode restart during active txn -> COMMIT after recovery
    # =========================================================================
    def s44_vnode_leader_restart_commit(self):
        self._reset_env()
        tdLog.info("======== s44_vnode_leader_restart_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct1 using stb tags(1)")

        # Kill VNode leader dnode
        tdSql.query("show txn_cdb.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode("txn_cdb", vgId)
        tdLog.info(f"Vgroup {vgId} leader on dnode {leader_dnode}, killing it")

        sc.dnodeForceStop(leader_dnode)
        time.sleep(5)

        # Restart it
        sc.dnodeStart(leader_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # COMMIT should succeed (VNode fencing will re-route to new leader)
        tdSql.execute("COMMIT")

        # Verify
        tdSql.query("show tables")
        tdSql.checkRows(1)

    # =========================================================================
    # s45: Full cluster restart during ACTIVE -> txn survives, COMMIT works
    #   After restart, MNode restores STxnObj from SDB, VNode rebuilds from
    #   txn.idx. The client auto-reconnects with the same txnId and sends
    #   heartbeat keepalives, so the 30s timeout does NOT fire. The txn
    #   stays ACTIVE and can be committed normally.
    # =========================================================================
    def s45_cluster_restart_txn_survives(self):
        self._reset_env()
        tdLog.info("======== s45_cluster_restart_txn_survives")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_pre using stb tags(1)")
        tdSql.execute("insert into ct_pre values(now, 100)")

        # Start txn on the main connection (its heartbeat will keep txn alive)
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_txn using stb tags(2)")

        # Full cluster restart — txn state persisted, client auto-reconnects
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # Client auto-reconnected, heartbeat keeps txn alive → COMMIT succeeds
        tdSql.execute("COMMIT")

        tdSql.execute("use txn_cdb")
        tdSql.query("show tables")
        tdSql.checkRows(2)    # ct_pre + ct_txn

        tdSql.query("select v from ct_pre")
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # s46: Full cluster restart after COMMIT -> data survives
    # =========================================================================
    def s46_mnode_restart_committing_recovery(self):
        self._reset_env()
        tdLog.info("======== s46_mnode_restart_committing_recovery")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Complete a full transaction successfully to verify recovery doesn't break things
        tdSql.execute("BEGIN")
        for i in range(5):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        # Verify committed
        tdSql.query("show tables")
        tdSql.checkRows(5)

        # Now test: full cluster restart after commit
        sc.dnodeStopAll()
        time.sleep(2)
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        tdSql.execute("use txn_cdb")

        # Data should survive restart
        tdSql.query("show tables")
        tdSql.checkRows(5)

        tdSql.execute("insert into ct_0 values(now, 42)")
        tdSql.query("select v from ct_0")
        tdSql.checkData(0, 0, 42)

    # =========================================================================
    # s47: Fencing — VNode leader switch (term change) during active txn
    #   When a VNode leader switches, the new leader has a higher Raft term.
    #   The fencing mechanism (vnodeTxnFencing) should handle old-term
    #   shadow entries. The MNode COMMIT re-broadcasts to new VNode leaders
    #   with the updated term. This tests that the txn can still COMMIT
    #   correctly after a leader switch.
    # =========================================================================
    def s47_fencing_vnode_leader_switch(self):
        self._reset_env()
        tdLog.info("======== s47_fencing_vnode_leader_switch")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create a table outside txn to verify it survives
        tdSql.execute("create table ct_pre using stb tags(0)")
        tdSql.execute("insert into ct_pre values(now, 100)")

        # Start transaction and create tables
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_fence1 using stb tags(1)")
        tdSql.execute("create table ct_fence2 using stb tags(2)")

        # Find VNode leader for our vgroup
        tdSql.query("show txn_cdb.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode("txn_cdb", vgId)
        tdLog.info(f"Vgroup {vgId} leader on dnode {leader_dnode}")

        # Kill VNode leader — triggers Raft election, new leader gets higher term
        # The fencing mechanism (vnodeTxnFencing) on new leader will see
        # term > pVnode->maxSeenTerm and abort old-term shadow entries
        tdLog.info(f"Killing VNode leader dnode {leader_dnode} to trigger fencing")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(5)

        # Wait for new VNode leader to be elected (different dnode)
        new_leader = self._get_vgroup_leader_dnode("txn_cdb", vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"
        assert new_leader != leader_dnode, f"Leader should have changed, still on dnode {leader_dnode}"
        tdLog.info(f"New VNode leader for vgroup {vgId}: dnode {new_leader}")

        # Restart the killed dnode
        sc.dnodeStart(leader_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # COMMIT — MNode sends TDMT_VND_TXN_COMMIT to all VNodes with current term
        # The new leader has higher term, so it processes the commit normally
        # The restarted old leader replays via Raft log
        tdSql.execute("COMMIT")

        # Verify: ct_fence1, ct_fence2, ct_pre should all exist
        tdSql.query("show tables")
        tdSql.checkRows(3)

        # Verify pre-existing data survived
        tdSql.query("select v from ct_pre")
        tdSql.checkData(0, 0, 100)

        # Verify new tables are usable
        tdSql.execute("insert into ct_fence1 values(now, 1)")
        tdSql.execute("insert into ct_fence2 values(now, 2)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 3)

    # =========================================================================
    # s48: Fencing — VNode leader switch during txn, then ROLLBACK
    # =========================================================================
    def s48_fencing_vnode_leader_switch_rollback(self):
        self._reset_env()
        tdLog.info("======== s48_fencing_vnode_leader_switch_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_pre using stb tags(0)")
        tdSql.execute("insert into ct_pre values(now, 200)")

        # Start txn and create tables
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_fence3 using stb tags(3)")

        # Kill VNode leader
        tdSql.query("show txn_cdb.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode("txn_cdb", vgId)
        tdLog.info(f"Killing VNode leader dnode {leader_dnode} to trigger fencing")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(5)

        # Wait for new leader
        new_leader = self._get_vgroup_leader_dnode("txn_cdb", vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"
        tdLog.info(f"New VNode leader: dnode {new_leader}")

        # Restart killed dnode
        sc.dnodeStart(leader_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # ROLLBACK — undo shadow entries via new leader
        tdSql.execute("ROLLBACK")

        # Verify: only ct_pre exists, ct_fence3 does not
        tdSql.query("show tables")
        tdSql.checkRows(1)

        tdSql.query("select v from ct_pre")
        tdSql.checkData(0, 0, 200)

    def test_meta_batch_txn_cluster(self):
        """Batch meta txn: cluster-mode tests

        40. MNode leader switch during active txn -> COMMIT
        41. MNode leader switch during active txn -> ROLLBACK
        42. Client disconnect -> auto-rollback
        43. VNode follower restart -> COMMIT succeeds
        44. VNode leader restart -> COMMIT after recovery
        45. Full cluster restart -> txn survives, COMMIT works
        46. Full cluster restart after COMMIT -> data survives
        47. Fencing: VNode leader switch (term change) -> COMMIT
        48. Fencing: VNode leader switch (term change) -> ROLLBACK


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-30 Created (cluster-mode txn robustness tests)
            - 2026-03-31 Added fencing (VNode leader switch) tests

        """
        self.s40_mnode_leader_switch_commit()
        self.s41_mnode_leader_switch_rollback()
        self.s42_client_disconnect_auto_rollback()
        self.s43_vnode_follower_restart_commit()
        self.s44_vnode_leader_restart_commit()
        self.s45_cluster_restart_txn_survives()
        self.s46_mnode_restart_committing_recovery()
        self.s47_fencing_vnode_leader_switch()
        self.s48_fencing_vnode_leader_switch_rollback()
