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


"""Cluster batch meta txn: WAL replay & cluster restart tests (s55-s58).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Split from test_meta_batch_txn_cluster_snapshot.py to keep
per-file execution time manageable.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterSnapshotB:
    """Cluster batch meta txn: snapshot & WAL replay (s55-s58)."""

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


    def _get_table_name_set(self, db_name):
        """Return current table names as a set for exact object-level assertions."""
        tdSql.execute(f"use {db_name}")
        tdSql.query("show tables")
        return set(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    # =========================================================================
    # s42: Client disconnect -> txn auto-rollback after timeout
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
        for idx in range(1, 4):
            sc.dnodeForceStop(idx)
        time.sleep(3)

        # Restart all dnodes (WAL replay should recover)
        tdLog.info("Restarting all dnodes for WAL replay")
        for idx in range(1, 4):
            sc.dnodeStart(idx)
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
    # s56: MNode leader kill during active txn -> BEGIN on new leader -> retry
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

        # Session B on the new MNode leader -> BEGIN should work
        alive_port = 6030 + ((leader_id % 3)) * 100  # connect to a surviving dnode
        tdSql3 = tdCom.newTdSql(port=alive_port)
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
    # s57: Full cluster restart after DROP txn ROLLBACK -> tables restored
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
        for idx in range(1, 4):
            sc.dnodeForceStop(idx)
        time.sleep(3)
        for idx in range(1, 4):
            sc.dnodeStart(idx)
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


    def test_meta_batch_txn_cluster_snapshot_b(self):
        """Cluster batch meta txn: snapshot & WAL replay (s55-s58)
        55. vnode_crash_wal_replay_commit
        56. mnode_election_retry_begin
        57. cluster_restart_after_rollback
        58. concurrent_txn_different_vgroups

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s55_vnode_crash_wal_replay_commit()
        self.s56_mnode_election_retry_begin()
        self.s57_cluster_restart_after_rollback()
        self.s58_concurrent_txn_different_vgroups()
