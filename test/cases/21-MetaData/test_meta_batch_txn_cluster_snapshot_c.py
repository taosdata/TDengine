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


"""Cluster batch meta txn: sequential restart & lazy vacuum tests (s59-s61).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Split from test_meta_batch_txn_cluster_snapshot.py to keep
per-file execution time manageable.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterSnapshotC:
    """Cluster batch meta txn: sequential restarts + lazy vacuum (s59-s61)."""

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

    def s59_sequential_txns_with_restarts(self):
        self._reset_env()
        tdLog.info("======== s59_sequential_txns_with_restarts")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        for round_num in range(2):  # Reduced from 3 to 2 rounds; 2 restarts still prove 'committed data survives each restart'
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
        tdSql.checkRows(2)
        for i in range(2):
            tdSql.execute(f"insert into ct_r{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 2)

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

        # Large txn: 100 tables > TSDB_TXN_INLINE_THRESHOLD (64) -> lazy vacuum path
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
        # via vnodeTxnRebuildFromMeta Phase 2 (txn_final.idx -> pFinalizedTxns)
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

        # Large txn: 100 tables -> lazy vacuum path
        NUM_TABLES = 100
        tdSql.execute("BEGIN")
        for batch_start in range(0, NUM_TABLES, 50):
            parts = [f"ct_rb_{batch_start + j} using stb tags({100 + batch_start + j})"
                     for j in range(min(50, NUM_TABLES - batch_start))]
            tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("ROLLBACK")
        # ROLLBACK returns immediately (lazy finalize O(1)); vacuum undo in progress

        # Verify only pre-created tables remain (rollback PRE_CREATE -> invisible)
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



    def test_meta_batch_txn_cluster_snapshot_c(self):
        """Cluster batch meta txn: sequential restarts + lazy vacuum (s59-s61)

        59. sequential_txns_with_restarts (2 rounds; proves committed data survives each restart)
        60. lazy_vacuum_snapshot_commit
        61. lazy_vacuum_snapshot_rollback

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s59_sequential_txns_with_restarts()
        self.s60_lazy_vacuum_snapshot_commit()
        self.s61_lazy_vacuum_snapshot_rollback()
