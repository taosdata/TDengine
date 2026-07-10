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


"""Cluster batch meta txn: fault injection tests (s70-s71).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  - PRE_ALTER snapshot rescue → COMMIT (s70)
  - PRE_ALTER snapshot rescue → ROLLBACK (s71)

Split from test_meta_batch_txn_cluster_fi.py to keep per-file execution
time manageable.  s66-s67 → cluster_fi.py, s68-s69 → cluster_fi_b.py.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterFIC:
    """Cluster batch meta txn: fault injection (s70-s71)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def _get_vgroup_leader_dnode(self, db_name, vgId, timeout=30):
        """Get the dnode ID of the vgroup leader, with retry."""
        for attempt in range(timeout):
            tdSql.query(f"show {db_name}.vgroups")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == vgId:
                    row = tdSql.queryResult[i]
                    for j in range(len(row)):
                        if row[j] == 'leader':
                            return row[j - 1]
            if attempt < timeout - 1:
                time.sleep(1)
        return None

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
    #      txnOrigVer < sver → emits old-version row FIRST (rescue), then
    #      the PRE_ALTER new-version row. Both land on follower's pTbDb.
    #   6. COMMIT → promotes PRE_ALTER entry, follower has new schema.
    #   7. Verify the new column is usable.
    # =========================================================================

    def s70_pre_alter_snapshot_commit(self):
        db = "txn_palter_sc"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1 keep 36500")
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
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
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
    #   On the leader, vnodeTxnRollbackShadowEntries needs txnOrigVer to exist
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
        tdSql.execute(f"create database {db} vgroups 1 replica 3 wal_retention_period 1 keep 36500")
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
        leader_port = 6030 + (leader_dnode - 1) * 100
        tdSql2 = tdCom.newTdSql(port=leader_port)
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


    def test_meta_batch_txn_cluster_fi_c(self):
        """Cluster batch meta txn: fault injection (s70-s71)

        70. pre_alter_snapshot_commit
        71. pre_alter_snapshot_rollback

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s70_pre_alter_snapshot_commit()
        self.s71_pre_alter_snapshot_rollback()
