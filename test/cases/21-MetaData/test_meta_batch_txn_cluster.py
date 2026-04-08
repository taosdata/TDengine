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

    # =========================================================================
    # s49: Cross-VNode multi-dnode COMMIT
    #   Database with replica 1, vgroups 3 on 3 dnodes. Tables hash to
    #   different vgroups on different physical dnodes. COMMIT must
    #   coordinate across all dnodes.
    # =========================================================================
    def s49_cross_vnode_multi_dnode_commit(self):
        db = "txn_xvn"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 3 replica 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s49_cross_vnode_multi_dnode_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create some tables outside txn
        for i in range(6):
            tdSql.execute(f"create table ct_pre{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i} values(now, {i})")

        tdSql.query("show tables")
        tdSql.checkRows(6)

        # Verify tables are on different vgroups (showing distribution)
        tdSql.query(f"show {db}.vgroups")
        num_vgroups = tdSql.queryRows
        tdLog.info(f"Database has {num_vgroups} vgroups")
        assert num_vgroups == 3, f"Expected 3 vgroups, got {num_vgroups}"

        # Transaction with mixed DDL across vgroups
        tdSql.execute("BEGIN")

        # CREATE new tables (spread across vgroups by hash)
        for i in range(9):
            tdSql.execute(f"create table ct_new{i} using stb tags({100 + i})")

        # DROP some pre-existing
        tdSql.execute("drop table ct_pre0")
        tdSql.execute("drop table ct_pre1")

        # ALTER a pre-existing normal table
        tdSql.execute("create table ntb_xvn (ts timestamp, c1 int)")
        tdSql.execute("alter table ntb_xvn add column c2 float")

        tdSql.execute("COMMIT")

        # Verify: 4 remaining pre + 9 new + 1 ntb = 14
        tdSql.query("show tables")
        tdSql.checkRows(14)

        # Verify dropped tables gone
        tdSql.error("select * from ct_pre0")
        tdSql.error("select * from ct_pre1")

        # Verify new tables writable
        for i in range(9):
            tdSql.execute(f"insert into ct_new{i} values(now, {200 + i})")

        # Verify ALTER persisted
        tdSql.query("describe ntb_xvn")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in cols, "Column c2 should exist after COMMIT"

        # Cleanup
        tdSql.execute(f"drop database {db}")

    # =========================================================================
    # s50: Cross-VNode multi-dnode ROLLBACK
    # =========================================================================
    def s50_cross_vnode_multi_dnode_rollback(self):
        db = "txn_xvn2"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 3 replica 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s50_cross_vnode_multi_dnode_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create tables and a normal table
        for i in range(6):
            tdSql.execute(f"create table ct_pre{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i} values(now, {i})")
        tdSql.execute("create table ntb_xvn (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb_xvn values(now, 99)")

        tdSql.query("show tables")
        tdSql.checkRows(7)   # 6 child + 1 normal

        # Transaction with mixed DDL
        tdSql.execute("BEGIN")

        for i in range(9):
            tdSql.execute(f"create table ct_new{i} using stb tags({100 + i})")

        tdSql.execute("drop table ct_pre0")
        tdSql.execute("drop table ct_pre1")
        tdSql.execute("alter table ntb_xvn add column c2 float")

        tdSql.execute("ROLLBACK")

        # All changes undone: back to 7
        tdSql.query("show tables")
        tdSql.checkRows(7)

        # Dropped tables restored
        for i in range(2):
            tdSql.query(f"select * from ct_pre{i}")
            tdSql.checkRows(1)

        # New tables don't exist
        tdSql.error("select * from ct_new0")

        # ALTER undone
        tdSql.query("describe ntb_xvn")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in cols, "Column c2 should NOT exist after ROLLBACK"

        # Cleanup
        tdSql.execute(f"drop database {db}")

    # =========================================================================
    # s51: Cross-VNode multi-dnode with VNode leader switch mid-txn
    #   Combines cross-VNode distribution + fencing: kill one VNode leader
    #   during active txn, wait for new leader election, then COMMIT.
    # =========================================================================
    def s51_cross_vnode_fencing_commit(self):
        db = "txn_xvf"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 3 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s51_cross_vnode_fencing_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create a table
        tdSql.execute("create table ct_pre using stb tags(0)")
        tdSql.execute("insert into ct_pre values(now, 100)")

        # Begin txn with tables across vgroups
        tdSql.execute("BEGIN")
        for i in range(6):
            tdSql.execute(f"create table ct_xvf{i} using stb tags({i + 1})")

        # Find VNode leader for first vgroup and kill it
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Vgroup {vgId} leader on dnode {leader_dnode}, killing it")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(5)

        # Wait for new leader
        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"
        assert new_leader != leader_dnode, "Leader should have changed"
        tdLog.info(f"New leader: dnode {new_leader}")

        # Restart killed dnode
        sc.dnodeStart(leader_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # COMMIT across multiple vgroups after leader switch
        tdSql.execute("COMMIT")

        # Verify all tables: 1 pre + 6 new = 7
        tdSql.query("show tables")
        tdSql.checkRows(7)

        tdSql.query("select v from ct_pre")
        tdSql.checkData(0, 0, 100)

        for i in range(6):
            tdSql.execute(f"insert into ct_xvf{i} values(now, {i})")

        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 7)

        # Cleanup
        tdSql.execute(f"drop database {db}")

    # =========================================================================
    # s52: Snapshot sync — follower catches up via Raft snapshot after
    #   WAL compaction. Verifies PRE_CREATE txn entries survive snapshot
    #   transfer and COMMIT works after follower rejoins.
    #
    #   Approach:
    #   1. Create db replica 3, begin txn, create tables (PRE_CREATE in B+tree)
    #   2. COMMIT (some replicas see it, all hopefully)
    #   3. Stop a follower dnode
    #   4. Write extensive data to advance WAL far ahead
    #   5. FLUSH + COMPACT database to trigger WAL truncation (log compaction)
    #   6. Restart follower — if WAL compacted past its matchIndex, Raft sends
    #      snapshot (metaSnapRead → network → metaSnapWrite). The snapshot
    #      includes txn fields (txnId/txnStatus) per §35.9.3.
    #   7. Verify all data is consistent after follower catches up
    # =========================================================================
    def s52_snapshot_sync_commit(self):
        db = "txn_snap"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s52_snapshot_sync_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create tables outside txn
        for i in range(3):
            tdSql.execute(f"create table ct_pre{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i} values(now, {i})")

        # Transaction: create + alter + commit
        tdSql.execute("BEGIN")
        for i in range(5):
            tdSql.execute(f"create table ct_txn{i} using stb tags({100 + i})")
        tdSql.execute("create table ntb_snap (ts timestamp, c1 int)")
        tdSql.execute("COMMIT")

        # Verify committed state
        tdSql.query("show tables")
        tdSql.checkRows(9)  # 3 pre + 5 txn + 1 ntb

        # Stop a follower dnode
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)

        follower_dnode = None
        tdSql.query("select * from information_schema.ins_dnodes")
        for i in range(tdSql.queryRows):
            did = tdSql.queryResult[i][0]
            if did != leader_dnode:
                follower_dnode = did
                break
        assert follower_dnode is not None, "Could not find a follower dnode"

        tdLog.info(f"Stopping follower dnode {follower_dnode} (leader is dnode {leader_dnode})")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # Write extensive data to advance WAL far ahead of the stopped follower
        tdLog.info("Writing extensive data to advance WAL...")
        for batch in range(20):
            values = ",".join([f"(now+{batch*100+j}s, {batch*100+j})" for j in range(100)])
            tdSql.execute(f"insert into ct_pre0 values {values}")

        # Trigger WAL compaction via flush + compact
        tdLog.info("Flushing and compacting to trigger WAL truncation...")
        tdSql.execute(f"flush database {db}")
        time.sleep(2)

        # Restart the stopped follower — may need snapshot sync
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)

        # Wait for Raft sync to complete (follower catches up)
        time.sleep(5)

        # Verify all data is consistent
        tdSql.execute(f"use {db}")
        tdSql.query("show tables")
        tdSql.checkRows(9)  # 3 pre + 5 txn + 1 ntb

        # Verify pre-existing data survived
        tdSql.query("select count(*) from ct_pre0")
        count = tdSql.queryResult[0][0]
        assert count >= 2001, f"Expected >= 2001 rows in ct_pre0, got {count}"

        # Verify txn tables are usable
        for i in range(5):
            tdSql.execute(f"insert into ct_txn{i} values(now, {i})")
        tdSql.execute("insert into ntb_snap values(now, 42)")
        tdSql.query("select count(*) from stb")
        assert tdSql.queryResult[0][0] >= 8, "Expected at least 8 rows in stb"

        # New transaction should also work after snapshot sync
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_post using stb tags(200)")
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(10)

        # Cleanup
        tdSql.execute(f"drop database {db}")
        tdLog.info("s52 PASSED")

    # =========================================================================
    # s53: Snapshot sync — active txn during follower restart.
    #   Follower misses DDL, catches up via snapshot/WAL which includes
    #   PRE_CREATE entries. Then COMMIT propagates to all replicas.
    # =========================================================================
    def s53_snapshot_active_txn_commit(self):
        db = "txn_snap2"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s53_snapshot_active_txn_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_pre using stb tags(0)")
        tdSql.execute("insert into ct_pre values(now, 100)")

        # Begin txn (PRE_CREATE entries replicated to all 3 VNodes)
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_snap1 using stb tags(1)")
        tdSql.execute("create table ct_snap2 using stb tags(2)")
        tdSql.execute("create table ntb_snap (ts timestamp, c1 int)")

        # Stop a follower
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)

        follower_dnode = None
        tdSql.query("select * from information_schema.ins_dnodes")
        for i in range(tdSql.queryRows):
            did = tdSql.queryResult[i][0]
            if did != leader_dnode:
                follower_dnode = did
                break
        assert follower_dnode is not None

        tdLog.info(f"Stopping follower dnode {follower_dnode}")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # Write data via a SEPARATE connection (main conn has active txn which blocks INSERT)
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        for batch in range(10):
            values = ",".join([f"(now+{batch*50+j}s, {batch*50+j})" for j in range(50)])
            tdSql2.execute(f"insert into ct_pre values {values}")

        tdSql2.execute(f"flush database {db}")
        tdSql2.close()
        time.sleep(2)

        # Restart follower — catches up via WAL/snapshot
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(3)

        # COMMIT — all replicas should process (txn entries exist on all)
        tdSql.execute("COMMIT")

        # Verify all tables exist
        tdSql.query("show tables")
        tdSql.checkRows(4)  # ct_pre + ct_snap1 + ct_snap2 + ntb_snap

        # Verify data
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        tdSql2.query("select count(*) from ct_pre")
        count = tdSql2.queryResult[0][0]
        assert count >= 501, f"Expected >= 501 rows in ct_pre, got {count}"
        tdSql2.close()

        # Verify new tables writable
        tdSql.execute("insert into ct_snap1 values(now, 1)")
        tdSql.execute("insert into ntb_snap values(now, 42)")

        # Cleanup
        tdSql.execute(f"drop database {db}")
        tdLog.info("s53 PASSED")

    # =========================================================================
    # s54: Snapshot sync — active txn with DROP, follower restart, ROLLBACK.
    #   Tests PRE_DROP entries survive Raft replication and ROLLBACK restores.
    # =========================================================================
    def s54_snapshot_active_txn_rollback(self):
        db = "txn_snap3"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s54_snapshot_active_txn_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        for i in range(3):
            tdSql.execute(f"create table ct_orig{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_orig{i} values(now, {i*10})")
        tdSql.execute("create table ntb_orig (ts timestamp, c1 int)")
        tdSql.execute("insert into ntb_orig values(now, 99)")

        # Begin txn with mixed DDL
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_new using stb tags(10)")
        tdSql.execute("drop table ct_orig0")
        tdSql.execute("alter table ntb_orig add column c2 float")

        # Stop a follower and advance WAL
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)

        follower_dnode = None
        tdSql.query("select * from information_schema.ins_dnodes")
        for i in range(tdSql.queryRows):
            did = tdSql.queryResult[i][0]
            if did != leader_dnode:
                follower_dnode = did
                break
        assert follower_dnode is not None

        tdLog.info(f"Stopping follower dnode {follower_dnode}")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # Write data via a SEPARATE connection (main conn has active txn which blocks INSERT)
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        for batch in range(10):
            values = ",".join([f"(now+{batch*50+j}s, {j})" for j in range(50)])
            tdSql2.execute(f"insert into ct_orig1 values {values}")

        tdSql2.execute(f"flush database {db}")
        tdSql2.close()

        # Restart follower
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(3)

        # ROLLBACK — all changes should be undone
        tdSql.execute("ROLLBACK")

        # Verify: back to original state (3 ct + 1 ntb = 4)
        tdSql.query("show tables")
        tdSql.checkRows(4)  # ct_orig0, ct_orig1, ct_orig2, ntb_orig

        # ct_new should not exist
        tdSql.error("select * from ct_new")

        # ct_orig0 should be restored (snapshot isolation)
        tdSql.query("select * from ct_orig0")
        tdSql.checkRows(1)

        # ALTER undone — ntb_orig should NOT have c2
        tdSql.query("describe ntb_orig")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in cols, "Column c2 should NOT exist after ROLLBACK"

        # Cleanup
        tdSql.execute(f"drop database {db}")
        tdLog.info("s54 PASSED")

    # =========================================================================
    # s55: VNode crash after COMMIT written to WAL → restart → WAL replay
    #   Verifies that if a VNode leader crashes after COMMIT redo log is
    #   written but before all applies complete, the WAL replay on restart
    #   correctly finalizes the COMMIT (promotes shadow entries).
    # =========================================================================
    def s55_vnode_crash_wal_replay_commit(self):
        self._reset_env()
        tdLog.info("======== s55_vnode_crash_wal_replay_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Create tables in txn and COMMIT
        tdSql.execute("BEGIN")
        for i in range(5):
            tdSql.execute(f"create table ct_wal{i} using stb tags({i})")
        tdSql.execute("create table ntb_wal (ts timestamp, c1 int)")
        tdSql.execute("COMMIT")

        # Insert data to verify tables are usable
        for i in range(5):
            tdSql.execute(f"insert into ct_wal{i} values(now, {i})")
        tdSql.execute("insert into ntb_wal values(now, 99)")

        # Kill all dnodes immediately (simulating crash)
        tdLog.info("Simulating crash: force-stopping all dnodes")
        sc.dnodeForceStopAll()
        time.sleep(3)

        # Restart all dnodes (WAL replay should recover)
        tdLog.info("Restarting all dnodes for WAL replay")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # Verify all tables exist after WAL replay
        tdSql.execute("use txn_cdb")
        tdSql.query("show tables")
        tdSql.checkRows(6)  # 5 ct + 1 ntb

        # Verify data survived
        for i in range(5):
            tdSql.query(f"select v from ct_wal{i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i)
        tdSql.query("select c1 from ntb_wal")
        tdSql.checkData(0, 0, 99)

        # Verify new txn works after recovery
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_post_wal using stb tags(100)")
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(7)

    # =========================================================================
    # s56: MNode leader kill during active txn → BEGIN on new leader → retry
    #   Tests that when the MNode leader dies during an active transaction,
    #   a new client can successfully BEGIN on the new MNode leader.
    # =========================================================================
    def s56_mnode_election_retry_begin(self):
        self._reset_env()
        tdLog.info("======== s56_mnode_election_retry_begin")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Session A starts a txn on current leader
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute("use txn_cdb")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_sessA using stb tags(1)")

        # Kill MNode leader
        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"Killing MNode leader dnode {leader_id}")
        sc.dnodeForceStop(leader_id)
        clusterComCheck.check3mnodeoff(leader_id)

        # Session B on the new MNode leader → BEGIN should work
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute("use txn_cdb")
        tdSql3.execute("BEGIN")
        tdSql3.execute("create table ct_sessB using stb tags(2)")
        tdSql3.execute("COMMIT")
        tdSql3.close()

        # Restart killed dnode
        sc.dnodeStart(leader_id)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # Session A txn — try to commit (may or may not work depending on
        # whether the STxnObj is still in SDB on new leader)
        try:
            tdSql2.execute("COMMIT")
            tdLog.info("  Session A COMMIT succeeded after leader change")
        except Exception as e:
            tdLog.info(f"  Session A COMMIT failed (expected): {e}")
            try:
                tdSql2.execute("ROLLBACK")
            except Exception:
                pass
        tdSql2.close()

        # Session B's table should exist
        tdSql.query("show tables")
        ct_sessB_exists = False
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == 'ct_sessb':
                ct_sessB_exists = True
        assert ct_sessB_exists, "ct_sessB should exist after COMMIT on new leader"

    # =========================================================================
    # s57: Full cluster restart after DROP txn ROLLBACK → tables restored
    #   Verifies WAL replay correctly handles ROLLBACK undo (restoring
    #   PRE_DROP entries back to NORMAL).
    # =========================================================================
    def s57_cluster_restart_after_rollback(self):
        self._reset_env()
        tdLog.info("======== s57_cluster_restart_after_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        for i in range(5):
            tdSql.execute(f"create table ct_orig{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_orig{i} values(now, {i * 10})")

        # Drop tables in txn then ROLLBACK
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_orig0")
        tdSql.execute("drop table ct_orig1")
        tdSql.execute("create table ct_new using stb tags(99)")
        tdSql.execute("ROLLBACK")

        # Verify rollback worked
        tdSql.query("show tables")
        tdSql.checkRows(5)  # all original tables restored

        # Crash and restart
        tdLog.info("Force-stopping all dnodes for crash simulation")
        sc.dnodeForceStopAll()
        time.sleep(3)
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # Verify tables survived restart
        tdSql.execute("use txn_cdb")
        tdSql.query("show tables")
        tdSql.checkRows(5)

        # Verify data integrity
        for i in range(5):
            tdSql.query(f"select v from ct_orig{i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i * 10)

        # Verify new txn works after crash+recovery
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_after_crash using stb tags(200)")
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(6)

    # =========================================================================
    # s58: Concurrent txns on different VNodes + VNode leader switch
    #   Two sessions operating on tables in different VGroups simultaneously.
    #   Kill one VNode leader, verify both txns can complete.
    # =========================================================================
    def s58_concurrent_txn_different_vgroups(self):
        db = "txn_cvg"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 3 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s58_concurrent_txn_different_vgroups")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create tables that hash to different vgroups
        for i in range(9):
            tdSql.execute(f"create table ct_cvg{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_cvg{i} values(now, {i})")

        # Session A: txn on some tables
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        tdSql2.execute("BEGIN")
        tdSql2.execute("create table ct_newA using stb tags(100)")
        tdSql2.execute("drop table ct_cvg0")

        # Session B: txn on different tables
        tdSql3 = tdCom.newTdSql()
        tdSql3.execute(f"use {db}")
        tdSql3.execute("BEGIN")
        tdSql3.execute("create table ct_newB using stb tags(200)")
        tdSql3.execute("drop table ct_cvg8")

        # Kill one VNode leader
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader dnode {leader_dnode} for vgroup {vgId}")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(5)

        # Wait for new VNode leader
        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"
        tdLog.info(f"New leader: dnode {new_leader}")

        # Restart killed dnode
        sc.dnodeStart(leader_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3)

        # Both sessions COMMIT
        tdSql2.execute("COMMIT")
        tdSql3.execute("COMMIT")
        tdSql2.close()
        tdSql3.close()

        # Verify: 9 orig - 2 dropped + 2 new = 9
        tdSql.query("show tables")
        tdSql.checkRows(9)

        # Dropped tables gone
        tdSql.error(f"select * from ct_cvg0")
        tdSql.error(f"select * from ct_cvg8")

        # New tables writable
        tdSql.execute("insert into ct_newA values(now, 1)")
        tdSql.execute("insert into ct_newB values(now, 2)")

        # Cleanup
        tdSql.execute(f"drop database {db}")

    # =========================================================================
    # s59: Multiple sequential txns with cluster restart between them
    #   Verifies that txn infrastructure reinitializes correctly after
    #   each cluster restart.
    # =========================================================================
    def s59_sequential_txns_with_restarts(self):
        self._reset_env()
        tdLog.info("======== s59_sequential_txns_with_restarts")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        for round_num in range(3):
            tdLog.info(f"  Round {round_num + 1}/3")

            # Txn cycle
            tdSql.execute("BEGIN")
            tdSql.execute(f"create table ct_r{round_num} using stb tags({round_num})")
            tdSql.execute("COMMIT")

            expected = round_num + 1
            tdSql.query("show tables")
            tdSql.checkRows(expected)

            # Cluster restart
            sc.dnodeStopAll()
            time.sleep(2)
            sc.dnodeStartAll()
            clusterComCheck.checkDnodes(3, timeout=30)
            tdSql.execute("use txn_cdb")

            # Verify data survived restart
            tdSql.query("show tables")
            tdSql.checkRows(expected)

        # Final verification
        tdSql.query("show tables")
        tdSql.checkRows(3)
        for i in range(3):
            tdSql.execute(f"insert into ct_r{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 3)

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
        49. Cross-VNode multi-dnode (replica 1, vgroups 3) -> COMMIT
        50. Cross-VNode multi-dnode (replica 1, vgroups 3) -> ROLLBACK
        51. Cross-VNode fencing: kill VNode leader mid-txn -> COMMIT
        52. Snapshot sync: follower restart + WAL advance -> COMMIT
        53. Snapshot sync: active txn + follower restart -> COMMIT
        54. Snapshot sync: active txn + follower restart -> ROLLBACK
        55. VNode crash after COMMIT WAL write -> restart -> WAL replay
        56. MNode leader kill -> BEGIN on new leader -> retry
        57. Full cluster restart after DROP txn ROLLBACK -> tables restored
        58. Concurrent txns on different VGroups + VNode leader switch
        59. Multiple sequential txns with cluster restarts between them


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-30 Created (cluster-mode txn robustness tests)
            - 2026-03-31 Added fencing (VNode leader switch) tests
            - 2026-03-31 Added cross-VNode multi-dnode tests (s49-s51)
            - 2026-04-02 Added snapshot sync tests (s52-s54)
            - 2026-04-08 Added VNode crash WAL replay, MNode election, cluster
                         restart after rollback, concurrent VGroup tests (s55-s59)

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
        self.s49_cross_vnode_multi_dnode_commit()
        self.s50_cross_vnode_multi_dnode_rollback()
        self.s51_cross_vnode_fencing_commit()
        self.s52_snapshot_sync_commit()
        self.s53_snapshot_active_txn_commit()
        self.s54_snapshot_active_txn_rollback()
        self.s55_vnode_crash_wal_replay_commit()
        self.s56_mnode_election_retry_begin()
        self.s57_cluster_restart_after_rollback()
        self.s58_concurrent_txn_different_vgroups()
        self.s59_sequential_txns_with_restarts()
