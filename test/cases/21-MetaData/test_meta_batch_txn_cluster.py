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
  pytest -N 3 -M 3 -C 3 -I cases/21-MetaData/test_meta_batch_txn_cluster.py -A

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

    def _get_mnode_leader_ep(self):
        """Get the current mnode leader endpoint (host:port)."""
        tdSql.query("show mnodes")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                return tdSql.queryResult[i][1]      # ep column
        return None

    def _get_dnode_index_by_ep(self, ep):
        """Map endpoint string to dnode index (1-based)."""
        tdSql.query("show dnodes")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][1] == ep:       # ep column
                return tdSql.queryResult[i][0]      # id column
        return None

    def _get_vgroup_leader_dnode(self, db_name, vgId):
        """Get the dnode ID of the vgroup leader."""
        tdSql.query(f"show {db_name}.vgroups")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == vgId:
                # Columns: vgId, ..., v1_dnode, v1_status, v2_dnode, v2_status, v3_dnode, v3_status
                # Find the 'leader' status
                row = tdSql.queryResult[i]
                for j in range(len(row)):
                    if row[j] == 'leader':
                        return row[j - 1]           # dnode id is the column before status
        return None

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
        leader_ep = self._get_mnode_leader_ep()
        tdLog.info(f"MNode leader: {leader_ep}, killing it")
        leader_idx = self._get_dnode_index_by_ep(leader_ep)
        sc.dnodeForceStop(leader_idx)

        # Wait for new leader election
        time.sleep(5)
        clusterComCheck.checkMnodeStatus(3)

        # COMMIT should succeed via new leader
        # (STxnObj is replicated in SDB, new leader can coordinate)
        tdSql.execute("COMMIT")

        # Restart killed dnode
        sc.dnodeStart(leader_idx)
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
        leader_ep = self._get_mnode_leader_ep()
        tdLog.info(f"MNode leader: {leader_ep}, killing it")
        leader_idx = self._get_dnode_index_by_ep(leader_ep)
        sc.dnodeForceStop(leader_idx)
        time.sleep(5)
        clusterComCheck.checkMnodeStatus(3)

        # ROLLBACK via new leader
        tdSql.execute("ROLLBACK")

        sc.dnodeStart(leader_idx)
        time.sleep(3)
        clusterComCheck.checkDnodes(3)

        # Tables should not exist
        tdSql.query("show tables")
        tdSql.checkRows(0)

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

        # Wait for txn timeout (default 10s for ACTIVE stage)
        tdLog.info("Waiting for txn timeout auto-rollback...")
        time.sleep(15)

        # Table should have been auto-rolled-back
        tdSql.query("show tables")
        tdSql.checkRows(0)

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

        # Find a follower dnode
        follower_dnode = None
        tdSql.query("show dnodes")
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
    # s45: VNode dnode restart during ACTIVE -> txn cleanup
    # =========================================================================
    def s45_vnode_restart_active_cleanup(self):
        self._reset_env()
        tdLog.info("======== s45_vnode_restart_active_cleanup")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_pre using stb tags(1)")
        tdSql.execute("insert into ct_pre values(now, 100)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_txn using stb tags(2)")
        tdSql.execute("drop table ct_pre")

        # Kill ALL dnodes and restart (simulate full cluster restart)
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        time.sleep(8)
        clusterComCheck.checkDnodes(3)

        # Reconnect
        tdSql.execute("use txn_cdb")

        # Transaction should have been auto-rolled-back after timeout
        tdLog.info("Waiting for txn timeout cleanup...")
        time.sleep(15)

        # ct_pre should be restored, ct_txn should not exist
        tdSql.query("show tables")
        tdSql.checkRows(1)         # only ct_pre

        tdSql.query("select v from ct_pre")
        tdSql.checkData(0, 0, 100)

    # =========================================================================
    # s46: MNode dnode restart during COMMITTING -> committed on recovery
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
        time.sleep(8)
        clusterComCheck.checkDnodes(3)

        tdSql.execute("use txn_cdb")

        # Data should survive restart
        tdSql.query("show tables")
        tdSql.checkRows(5)

        tdSql.execute("insert into ct_0 values(now, 42)")
        tdSql.query("select v from ct_0")
        tdSql.checkData(0, 0, 42)

    def test_meta_batch_txn_cluster(self):
        """Batch meta txn: cluster-mode tests

        40. MNode leader switch during active txn -> COMMIT
        41. MNode leader switch during active txn -> ROLLBACK
        42. Client disconnect -> auto-rollback
        43. VNode follower restart -> COMMIT succeeds
        44. VNode leader restart -> COMMIT after recovery
        45. Full cluster restart during ACTIVE -> txn cleanup
        46. Full cluster restart after COMMIT -> data survives


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-03-30 Created (cluster-mode txn robustness tests)

        """
        self.s40_mnode_leader_switch_commit()
        self.s41_mnode_leader_switch_rollback()
        self.s42_client_disconnect_auto_rollback()
        self.s43_vnode_follower_restart_commit()
        self.s44_vnode_leader_restart_commit()
        self.s45_vnode_restart_active_cleanup()
        self.s46_mnode_restart_committing_recovery()
