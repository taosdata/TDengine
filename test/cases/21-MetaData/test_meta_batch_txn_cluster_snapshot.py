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


"""Cluster batch meta txn: snapshot & WAL replay tests (s52-s65).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  - Snapshot sync with active txn COMMIT/ROLLBACK (s52-s54)
  - VNode crash WAL replay COMMIT (s55)
  - MNode election retry BEGIN (s56)
  - Cluster restart after ROLLBACK (s57)
  - Concurrent txn on different vgroups (s58)
  - Sequential txns with restarts (s59)
  - Lazy vacuum snapshot COMMIT/ROLLBACK (s60-s61)
  - STB lifecycle with restart: CREATE/ALTER/DROP (s62-s65)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterSnapshot:
    """Cluster batch meta txn: snapshot & WAL replay (s52-s65)."""

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
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
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

        # Verify data — retry connection in case cluster needs extra settling time
        for _attempt in range(6):
            try:
                tdSql2 = tdCom.newTdSql()
                break
            except Exception as _e:
                if _attempt == 5:
                    raise
                tdLog.warning(f"  Connection attempt {_attempt + 1} failed ({_e}), retrying in 5s…")
                time.sleep(5)
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
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
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

    # =========================================================================
    # s60: Lazy vacuum × snapshot — large txn COMMIT, follower restart
    #   before vacuum drain, catch up via snapshot/WAL, verify tables visible
    #   and second txn works immediately.
    #   Covers: txn_final.idx in snapshot, Phase 2 rebuild, async vacuum resume.
    # =========================================================================

    def s60_lazy_vacuum_snapshot_commit(self):
        db = "txn_lv_snap"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s60_lazy_vacuum_snapshot_commit")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Large txn: 100 tables > TSDB_TXN_INLINE_THRESHOLD (64) → lazy vacuum path
        NUM_TABLES = 100
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 50):
            parts = [f"ct_{batch_start + j} using stb tags({batch_start + j})"
                     for j in range(min(50, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")
        # COMMIT returns immediately (lazy finalize O(1)); vacuum still in progress

        # Verify tables are visible on leader (via txn_final.idx visibility)
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(NUM_TABLES)

        # Stop a follower IMMEDIATELY — vacuum likely not yet drained
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

        # Write extensive data to advance WAL far ahead of the stopped follower.
        # Combined with low wal_retention_period + compact, follower should need snapshot.
        tdLog.info("Writing extensive data to advance WAL...")
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
        tdSql2.execute(f"use {db}")
        for batch in range(20):
            values = ",".join([f"(now+{batch*100+j}s, {batch*100+j})" for j in range(100)])
            tdSql2.execute(f"insert into ct_0 values {values}")
        tdSql2.execute(f"flush database {db}")
        tdSql2.execute(f"compact database {db}")
        time.sleep(4)  # let old WAL files age out under wal_retention_period=1
        tdSql2.close()
        time.sleep(2)

        # Restart the stopped follower — may need snapshot sync
        # The snapshot should include txn_final.idx entries (finalized but not vacuumed)
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(5)

        # Verify all tables are visible — follower should have recovered correctly
        # via vnodeTxnRebuildFromMeta Phase 2 (txn_final.idx → pFinalizedTxns)
        tdSql.execute(f"use {db}")
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(NUM_TABLES)

        # Verify data operations work (INSERT into committed tables)
        tdSql.execute("insert into ct_50 values(now, 50)")
        tdSql.execute("insert into ct_99 values(now, 99)")
        tdSql.query("select count(*) from stb")
        count = tdSql.queryResult[0][0]
        assert count >= 2002, f"Expected >= 2002 rows in stb, got {count}"

        # A second txn should work immediately (no conflict with finalized entries)
        tdSql.execute("BEGIN")
        tdSql.execute("drop table ct_0")
        tdSql.execute("create table ct_new using stb tags(999)")
        tdSql.execute("COMMIT")
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(NUM_TABLES)  # -1 dropped + 1 new = same count

        # Object-level assertions: prevent false positives from count-only checks
        current = self._get_table_name_set(db)
        assert "ct_0" not in current, "ct_0 should be dropped by second txn"
        assert "ct_new" in current, "ct_new should exist after second txn"
        tdSql.execute("insert into ct_new values(now, 1001)")

        tdSql.execute(f"drop database {db}")
        tdLog.info("s60 PASSED")

    # =========================================================================
    # s61: Lazy vacuum × snapshot — large txn ROLLBACK, follower restart
    #   before vacuum drain, verify PRE_CREATE entries fully cleaned.
    #   Covers: TXN_FINAL_ROLLEDBACK in snapshot, Phase 2 rebuild undo resume.
    # =========================================================================

    def s61_lazy_vacuum_snapshot_rollback(self):
        db = "txn_lv_snap_rb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s61_lazy_vacuum_snapshot_rollback")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Pre-create a few tables outside txn (to verify they survive)
        for i in range(3):
            tdSql.execute(f"create table ct_pre{i} using stb tags({i})")
            tdSql.execute(f"insert into ct_pre{i} values(now, {i})")

        # Large txn: 100 tables → lazy vacuum path
        NUM_TABLES = 100
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 50):
            parts = [f"ct_rb_{batch_start + j} using stb tags({100 + batch_start + j})"
                     for j in range(min(50, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("ROLLBACK")
        # ROLLBACK returns immediately (lazy finalize O(1)); vacuum undo in progress

        # Verify only pre-created tables remain (rollback PRE_CREATE → invisible)
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(3)
        expected_pre = {"ct_pre0", "ct_pre1", "ct_pre2"}
        assert self._get_table_name_set(db) == expected_pre, "Only pre-created tables should remain"

        # Stop a follower IMMEDIATELY — vacuum undo likely not yet drained
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

        # Advance WAL to trigger snapshot sync on follower restart
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
        tdSql2.execute(f"use {db}")
        for batch in range(20):
            values = ",".join([f"(now+{batch*100+j}s, {batch*100+j})" for j in range(100)])
            tdSql2.execute(f"insert into ct_pre0 values {values}")
        tdSql2.execute(f"flush database {db}")
        tdSql2.execute(f"compact database {db}")
        time.sleep(4)  # let old WAL files age out under wal_retention_period=1
        tdSql2.close()
        time.sleep(2)

        # Restart follower — snapshot includes txn_final.idx with TXN_FINAL_ROLLEDBACK
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(5)

        # Verify rolled-back tables are NOT visible
        tdSql.execute(f"use {db}")
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(3)
        assert self._get_table_name_set(db) == expected_pre, "Rollback should not leak ct_rb_* tables"
        tdSql.error("describe ct_rb_0")
        tdSql.error("describe ct_rb_99")

        # Verify pre-existing data survived
        tdSql.query("select count(*) from ct_pre0")
        count = tdSql.queryResult[0][0]
        assert count >= 2001, f"Expected >= 2001 rows in ct_pre0, got {count}"

        # A fresh txn after recovery should work — no stale finalized entries blocking
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_fresh using stb tags(500)")
        tdSql.execute("COMMIT")
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(4)  # 3 pre + 1 fresh
        assert self._get_table_name_set(db) == expected_pre | {"ct_fresh"}

        # Can also re-use the same names that were rolled back
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_rb_0 using stb tags(600)")
        tdSql.execute("COMMIT")
        tdSql.query(f"show {db}.tables")
        tdSql.checkRows(5)
        assert self._get_table_name_set(db) == expected_pre | {"ct_fresh", "ct_rb_0"}

        tdSql.execute(f"drop database {db}")
        tdLog.info("s61 PASSED")

    # =========================================================================
    # s62-s65: STB (super table) txn crash recovery tests
    #   These test MNode SDB persistence of STB txnId/txnStatus across restart.
    # =========================================================================


    def s62_stb_create_restart_commit(self):
        """STB created in txn → cluster restart → COMMIT → STB visible"""
        db = "txn_stb_rc"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s62_stb_create_restart_commit")

        # Create a pre-existing STB for reference
        tdSql.execute("create table stb_pre (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb_pre tags(0)")

        # Create STB within txn
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_txn (ts timestamp, v int, v2 float) tags (t1 int, t2 binary(16))")
        tdSql.execute("create table ct1 using stb_txn tags(1, 'hello')")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify STB exists and child table is usable
        tdSql.execute(f"use {db}")
        tdSql.query("show stables")
        stb_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
        assert "stb_txn" in stb_names, f"stb_txn should exist after COMMIT, got {stb_names}"

        tdSql.query("show tables")
        tdSql.checkRows(2)  # ct0 + ct1

        tdSql.execute("insert into ct1 values(now, 1, 1.5)")
        tdSql.query("select * from ct1")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s62 PASSED")


    def s63_stb_alter_restart_commit(self):
        """STB altered in txn → cluster restart → COMMIT → schema updated"""
        db = "txn_stb_ac"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s63_stb_alter_restart_commit")

        tdSql.execute("create table stb1 (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb1 tags(0)")
        tdSql.execute("insert into ct0 values(now, 100)")

        # ALTER STB within txn: add column
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column v2 float")
        tdSql.execute("create table ct1 using stb1 tags(1)")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify ALTER took effect
        tdSql.execute(f"use {db}")
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "v2" in col_names, f"v2 should exist after ALTER+COMMIT, got {col_names}"

        # Insert with new schema
        tdSql.execute("insert into ct0 values(now, 200, 3.14)")
        tdSql.query("select v2 from ct0 where v2 is not null")
        tdSql.checkRows(1)
        val = float(tdSql.queryResult[0][0])
        assert abs(val - 3.14) < 0.001, f"v2 should be ~3.14, got {val}"

        tdSql.execute(f"drop database {db}")
        tdLog.info("s63 PASSED")


    def s64_stb_drop_restart_rollback(self):
        """STB marked for DROP in txn → cluster restart → ROLLBACK → STB restored"""
        db = "txn_stb_dr"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s64_stb_drop_restart_rollback")

        tdSql.execute("create table stb1 (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb1 tags(0)")
        tdSql.execute("insert into ct0 values(now, 42)")

        # DROP STB within txn
        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb1")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # ROLLBACK after restart → STB should be restored
        tdSql.execute("ROLLBACK")

        tdSql.execute(f"use {db}")
        tdSql.query("show stables")
        stb_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
        assert "stb1" in stb_names, f"stb1 should be restored after ROLLBACK, got {stb_names}"

        # Child table and data should be intact
        tdSql.query("select v from ct0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        # Can still insert into the restored STB
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 99)")
        tdSql.query("select v from ct1")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s64 PASSED")


    def s65_stb_create_alter_restart_commit(self):
        """STB CREATE + ALTER chain in txn → cluster restart → COMMIT → final schema visible"""
        db = "txn_stb_cac"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s65_stb_create_alter_restart_commit")

        # Entire STB lifecycle in one txn
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_chain (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("alter table stb_chain add column v2 float")
        tdSql.execute("create table ct0 using stb_chain tags(0)")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify final schema includes ALTER
        tdSql.execute(f"use {db}")
        tdSql.query("describe stb_chain")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "v2" in col_names, f"v2 should exist after CREATE+ALTER+COMMIT, got {col_names}"

        # Verify child table usable with full schema
        tdSql.execute("insert into ct0 values(now, 1, 2.5)")
        tdSql.query("select v, v2 from ct0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        val = float(tdSql.queryResult[0][1])
        assert abs(val - 2.5) < 0.001, f"v2 should be ~2.5, got {val}"

        tdSql.execute(f"drop database {db}")
        tdLog.info("s65 PASSED")

    # =========================================================================
    # s66: Fault injection — Raft leader switch *during* vacuum
    #
    # COMMIT writes pTxnFinalIdx then triggers vacuum. Kill VNode leader
    # immediately. New leader must run vacuum exactly once (pTxnFinalIdx
    # guards double-vacuum if old leader had partially completed it).
    # =========================================================================

    def test_meta_batch_txn_cluster_snapshot(self):
        """Cluster batch meta txn: snapshot & WAL replay (s52-s65)

        52. snapshot_sync_commit
        53. snapshot_active_txn_commit
        54. snapshot_active_txn_rollback
        55. vnode_crash_wal_replay_commit
        56. mnode_election_retry_begin
        57. cluster_restart_after_rollback
        58. concurrent_txn_different_vgroups
        59. sequential_txns_with_restarts
        60. lazy_vacuum_snapshot_commit
        61. lazy_vacuum_snapshot_rollback
        62. stb_create_restart_commit
        63. stb_alter_restart_commit
        64. stb_drop_restart_rollback
        65. stb_create_alter_restart_commit

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        """
        self.s52_snapshot_sync_commit()
        self.s53_snapshot_active_txn_commit()
        self.s54_snapshot_active_txn_rollback()
        self.s55_vnode_crash_wal_replay_commit()
        self.s56_mnode_election_retry_begin()
        self.s57_cluster_restart_after_rollback()
        self.s58_concurrent_txn_different_vgroups()
        self.s59_sequential_txns_with_restarts()
        self.s60_lazy_vacuum_snapshot_commit()
        self.s61_lazy_vacuum_snapshot_rollback()
        self.s62_stb_create_restart_commit()
        self.s63_stb_alter_restart_commit()
        self.s64_stb_drop_restart_rollback()
        self.s65_stb_create_alter_restart_commit()
