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


"""Cluster batch meta txn: fault injection tests (s66-s71).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  - Leader switch during vacuum (s66)
  - Concurrent DROP during vacuum (s67)
  - VNode restart mid-vacuum (s68)
  - MNode leader switch before vacuum broadcast (s69)
  - PRE_ALTER snapshot COMMIT/ROLLBACK (s70-s71)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterFI:
    """Cluster batch meta txn: fault injection (s66-s71)."""

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

    def s66_fi_leader_switch_during_vacuum(self):
        db = "txn_fi66"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s66_fi_leader_switch_during_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) immediately after COMMIT")
        sc.dnodeForceStop(leader_dnode)

        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"

        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)

        self._poll_table_count(10, db_name=db)

        tdSql.execute(f"use {db}")
        for i in range(10):
            tdSql.execute(f"insert into ct_{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 10)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s66 PASSED")

    # =========================================================================
    # s67: Fault injection — Concurrent DROP on table being vacuumed (PRE_CREATE)
    #
    # COMMIT creates ct_target (PRE_CREATE). DROP arrives while vacuum is
    # promoting it. Both orderings (vacuum-first and drop-first) must not
    # corrupt pUidIdx/pTbDb.
    # =========================================================================

    def s67_fi_concurrent_drop_during_vacuum(self):
        db = "txn_fi67"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s67_fi_concurrent_drop_during_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_target using stb tags(99)")
        tdSql.execute("COMMIT")

        try:
            tdSql.execute("drop table ct_target")
        except Exception:
            pass  # PRE_CREATE still invisible — correct

        time.sleep(3)

        tdSql.execute(f"use {db}")
        tdSql.query("show tables")
        names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "ct_target" not in names, "ct_target must not exist after DROP"

        # Schema must be intact
        tdSql.execute("create table ct_safe using stb tags(1)")
        tdSql.execute("insert into ct_safe values(now, 1)")
        tdSql.query("select v from ct_safe")
        tdSql.checkData(0, 0, 1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s67 PASSED")

    # =========================================================================
    # s68: Fault injection — VNode restart mid-vacuum
    #
    # Large txn COMMIT, then immediately kill VNode leader. On restart
    # taosd finds txn.idx entries + pTxnFinalIdx COMMITTED and re-runs
    # vacuum. All tables must reach final promoted state.
    # =========================================================================

    def s68_fi_vnode_restart_mid_vacuum(self):
        db = "txn_fi68"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s68_fi_vnode_restart_mid_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        num_tables = 50
        tdSql.execute("BEGIN")
        for i in range(num_tables):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) mid-vacuum")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(1)

        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3, timeout=60)

        self._poll_table_count(num_tables, db_name=db, timeout=120)

        tdSql.execute(f"use {db}")
        tdSql.execute("insert into ct_0 values(now, 0)")
        tdSql.execute("insert into ct_49 values(now, 49)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 2)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s68 PASSED")

    # =========================================================================
    # s69: Fault injection — MNode leader switch between COMMIT and vacuum broadcast
    #
    # MNode writes pTxnFinalIdx (COMMITTED) then is killed before sending
    # vacuum broadcast. New MNode leader reads pTxnFinalIdx and re-broadcasts.
    # Vacuum must complete correctly.
    # =========================================================================

    def s69_fi_mnode_leader_switch_before_vacuum_broadcast(self):
        db = "txn_fi69"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s69_fi_mnode_leader_switch_before_vacuum_broadcast")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.query("select * from information_schema.ins_mnodes")
        mnode_leader_id = None
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                mnode_leader_id = tdSql.queryResult[i][0]
                break
        assert mnode_leader_id is not None, "No MNode leader found"
        tdLog.info(f"MNode leader: dnode {mnode_leader_id}")

        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")
        sc.dnodeForceStop(mnode_leader_id)
        tdLog.info(f"Killed MNode leader (dnode {mnode_leader_id}) after COMMIT")

        new_mnode_leader = None
        for _ in range(30):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for i in range(tdSql.queryRows):
                    if tdSql.queryResult[i][2] == 'leader' and \
                       tdSql.queryResult[i][0] != mnode_leader_id:
                        new_mnode_leader = tdSql.queryResult[i][0]
                        break
                if new_mnode_leader:
                    tdLog.info(f"New MNode leader: dnode {new_mnode_leader}")
                    break
            except Exception:
                continue
        assert new_mnode_leader is not None, "No new MNode leader elected"

        sc.dnodeStart(mnode_leader_id)
        clusterComCheck.checkDnodes(3)

        self._poll_table_count(10, db_name=db)

        tdSql.execute(f"use {db}")
        tdSql.execute("insert into ct_0 values(now, 100)")
        tdSql.query("select v from ct_0")
        tdSql.checkData(0, 0, 100)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s69 PASSED")

    # =========================================================================
    # s70: PRE_ALTER × snapshot rescue → COMMIT
    #   Covers the metaSnapshot.c "pPrevVerNeeded" rescue logic:
    #   1. Create table, write data (establishes "old" version in pTbDb)
    #   2. Stop a follower so it misses the ALTER
    #   3. While follower is down: BEGIN → ALTER table (adds column) → PRE_ALTER
    #      state now exists. The ALTER writes a NEW version row to pTbDb; old
    #      version row is BELOW the snapshot's sver window.
    #   4. Advance WAL with writes so WAL compacts, forcing snapshot sync.
    #   5. Restart follower: metaSnapRead detects PRE_ALTER uid whose
    #      txnPrevVer < sver → emits old-version row FIRST (rescue), then
    #      the PRE_ALTER new-version row. Both land on follower's pTbDb.
    #   6. COMMIT → promotes PRE_ALTER entry, follower has new schema.
    #   7. Verify the new column is usable.
    # =========================================================================

    def s70_pre_alter_snapshot_commit(self):
        db = "txn_palter_sc"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s70_pre_alter_snapshot_commit")

        # Phase 1: create tables and write initial data (baseline version in pTbDb)
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_a using stb tags(1)")
        tdSql.execute("create table ntb_b (ts timestamp, c1 int)")
        tdSql.execute("insert into ct_a values(now, 10)")
        tdSql.execute("insert into ntb_b values(now, 20)")
        # Flush to ensure WAL is advanced past table creation
        tdSql.execute(f"flush database {db}")
        time.sleep(2)

        # Phase 2: stop a follower — it will miss subsequent ALTER
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
        assert follower_dnode is not None, "Cannot find follower"
        tdLog.info(f"Stopping follower dnode {follower_dnode} (leader={leader_dnode})")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # Phase 3: BEGIN → ALTER TABLE (PRE_ALTER status on leader + remaining follower)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb_b add column c2 float")
        tdSql.execute("alter table stb add column v2 bigint")
        # The txn is still active — PRE_ALTER entries exist in pTbDb (new-version rows).
        # The old version row for ntb_b and stb will have version < sver for the follower.

        # Phase 4: advance WAL to force snapshot sync when follower restarts
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        for batch in range(20):
            values = ",".join([f"(now+{batch*100+j}s, {batch*100+j})" for j in range(100)])
            tdSql2.execute(f"insert into ct_a values {values}")
        tdSql2.execute(f"flush database {db}")
        tdSql2.execute(f"compact database {db}")
        time.sleep(4)  # age out old WAL files
        tdSql2.close()
        time.sleep(2)

        # Phase 5: restart follower — should use snapshot sync
        # metaSnapRead will emit the OLD version row for ntb_b/stb (via pPrevVerNeeded)
        # BEFORE the PRE_ALTER new-version row.
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(3)

        # Phase 6: COMMIT — promotes PRE_ALTER to committed state
        tdSql.execute("COMMIT")
        time.sleep(2)

        # Phase 7: verify new schema is active on all replicas
        tdSql.query("describe ntb_b")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in cols, f"Column c2 should exist after COMMIT, got: {cols}"

        tdSql.query("describe stb")
        cols_stb = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'v2' in cols_stb, f"Column v2 should exist on stb after COMMIT, got: {cols_stb}"

        # Verify data insert with new schema works
        tdSql.execute("insert into ntb_b values(now, 30, 3.14)")
        tdSql.execute("insert into ct_a values(now, 40, 42)")
        tdSql.query("select c2 from ntb_b where c2 is not null")
        tdSql.checkRows(1)
        tdSql.query("select v2 from ct_a where v2 is not null")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s70 PASSED")

    # =========================================================================
    # s71: PRE_ALTER × snapshot rescue → ROLLBACK
    #   Same setup as s70, but the txn is ROLLED BACK after follower catches up.
    #   On the leader, vnodeTxnRollbackShadowEntries needs txnPrevVer to exist
    #   in pTbDb. On the follower (which received data via snapshot), the rescue
    #   logic ensures the old-version row was sent alongside the PRE_ALTER row,
    #   so ROLLBACK correctly reverts to the original schema.
    #
    #   Key invariant being tested:
    #     After ROLLBACK, the ALTER is fully undone — old schema is restored
    #     on ALL replicas (including the one that received data via snapshot).
    # =========================================================================

    def s71_pre_alter_snapshot_rollback(self):
        db = "txn_palter_sr"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s71_pre_alter_snapshot_rollback")

        # Phase 1: create tables and baseline data
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct_a using stb tags(1)")
        tdSql.execute("create table ntb_b (ts timestamp, c1 int)")
        tdSql.execute("insert into ct_a values(now, 10)")
        tdSql.execute("insert into ntb_b values(now, 20)")
        tdSql.execute(f"flush database {db}")
        time.sleep(2)

        # Phase 2: stop a follower
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
        tdLog.info(f"Stopping follower dnode {follower_dnode} (leader={leader_dnode})")
        sc.dnodeForceStop(follower_dnode)
        time.sleep(2)

        # Phase 3: BEGIN → ALTER TABLE (PRE_ALTER)
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb_b add column c2 float")
        tdSql.execute("alter table stb add column v2 bigint")

        # Phase 4: advance WAL to force snapshot on follower restart
        tdSql2 = tdCom.newTdSql()
        tdSql2.execute(f"use {db}")
        for batch in range(20):
            values = ",".join([f"(now+{batch*100+j}s, {batch*100+j})" for j in range(100)])
            tdSql2.execute(f"insert into ct_a values {values}")
        tdSql2.execute(f"flush database {db}")
        tdSql2.execute(f"compact database {db}")
        time.sleep(4)
        tdSql2.close()
        time.sleep(2)

        # Phase 5: restart follower — snapshot sync includes prev-ver rescue rows
        tdLog.info(f"Restarting follower dnode {follower_dnode}")
        sc.dnodeStart(follower_dnode)
        time.sleep(5)
        clusterComCheck.checkDnodes(3, timeout=30)
        time.sleep(3)

        # Phase 6: ROLLBACK — should revert ALTER on all replicas
        tdSql.execute("ROLLBACK")
        time.sleep(2)

        # Phase 7: verify schema is reverted to original (NO c2, NO v2)
        # Uses the SAME connection — the client auto-retries on schema version
        # mismatch (TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER), which validates
        # that the server's pUidIdx.skmVer was correctly restored.
        tdSql.query("describe ntb_b")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' not in cols, f"Column c2 should NOT exist after ROLLBACK, got: {cols}"

        tdSql.query("describe stb")
        cols_stb = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'v2' not in cols_stb, f"Column v2 should NOT exist on stb after ROLLBACK, got: {cols_stb}"

        # Verify original schema still works (same connection, auto-refresh)
        tdSql.execute("insert into ntb_b values(now, 30)")
        tdSql.execute("insert into ct_a values(now, 40)")
        tdSql.query("select * from ntb_b")
        assert tdSql.queryRows >= 2, "ntb_b should have original data + new insert"
        tdSql.query("select * from ct_a")
        assert tdSql.queryRows >= 2, "ct_a should have original data + new insert"

        # Verify c2 column truly doesn't exist (INSERT with 3 cols should fail)
        tdSql.error("insert into ntb_b values(now, 50, 1.5)")

        # A new ALTER after ROLLBACK should work cleanly
        tdSql.execute("alter table ntb_b add column c2 double")
        tdSql.query("describe ntb_b")
        cols = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert 'c2' in cols, "New ALTER after ROLLBACK should succeed"
        tdSql.execute("insert into ntb_b values(now, 60, 2.718)")
        tdSql.query("select c2 from ntb_b where c2 is not null")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s71 PASSED")


    def test_meta_batch_txn_cluster_fi(self):
        """Cluster batch meta txn: fault injection (s66-s71)

        66. fi_leader_switch_during_vacuum
        67. fi_concurrent_drop_during_vacuum
        68. fi_vnode_restart_mid_vacuum
        69. fi_mnode_leader_switch_before_vacuum_broadcast
        70. pre_alter_snapshot_commit
        71. pre_alter_snapshot_rollback

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        """
        self.s66_fi_leader_switch_during_vacuum()
        self.s67_fi_concurrent_drop_during_vacuum()
        self.s68_fi_vnode_restart_mid_vacuum()
        self.s69_fi_mnode_leader_switch_before_vacuum_broadcast()
        self.s70_pre_alter_snapshot_commit()
        self.s71_pre_alter_snapshot_rollback()
