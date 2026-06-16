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


"""Cluster batch meta txn: snapshot sync tests (s52-s54).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Split from test_meta_batch_txn_cluster_snapshot.py to keep
per-file execution time manageable.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterSnapshotA:
    """Cluster batch meta txn: snapshot & WAL replay (s52-s54)."""

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
    # s55: VNode crash after COMMIT written to WAL -> restart -> WAL replay
    #   Verifies that if a VNode leader crashes after COMMIT redo log is
    #   written but before all applies complete, the WAL replay on restart
    #   correctly finalizes the COMMIT (promotes shadow entries).
    # =========================================================================


    def test_meta_batch_txn_cluster_snapshot_a(self):
        """Cluster batch meta txn: snapshot & WAL replay (s52-s54)
        52. snapshot_sync_commit
        53. snapshot_active_txn_commit
        54. snapshot_active_txn_rollback

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s52_snapshot_sync_commit()
        self.s53_snapshot_active_txn_commit()
        self.s54_snapshot_active_txn_rollback()
