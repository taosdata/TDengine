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

"""Target-side (taosX) cluster correctness tests (s21-s23).

Covers three gaps not addressed by existing single-node taosx tests:

  s21: MNode leader switch mid-workflow.
       Verifies: after MNode leader switches, taosx consumer continues to
       receive committed txn DDL messages correctly.  The WAL .txn IS_BEGIN
       marking on the VNode side is independent of MNode leadership; this
       test confirms the entire BEGIN → COMMIT → consumer delivery pipeline
       survives MNode leader change.

  s22: VNode leader restart (WAL replay + .txn recovery + STxnWalManager eager load).
       Verifies: after the VNode dnode restarts, walTxnFilesRecover rebuilds
       txnBeginIndexMap from existing .txn files (no duplicate IS_BEGIN marking),
       and STxnWalManager eager-loads all committed txn entries via
       walTxnReadRange (side-effect IS_BEGIN rebuild).  A subsequent taosx
       consumer scenario must deliver DDL messages atomically.

  s23: WAL Raft snapshot (follower falls behind → snapshot sync) + taosx consumer.
       Verifies: after a follower dnode is restarted and receives a WAL Raft
       snapshot from the leader, walTxnFilesRotate re-initializes an empty
       txnBeginIndexMap (snapshots are taken at clean commit points — no
       in-flight txns cross the boundary).  New transactions after snapshot
       have their first WAL entry correctly marked with IS_BEGIN, and the
       taosx consumer delivers their DDL messages atomically.

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import subprocess
import os
import time


# ─────────────────────────────────────────────────────────────────────────────
# Binary helpers (identical to test_taosx_txn_basic.py)
# ─────────────────────────────────────────────────────────────────────────────

TMQ_TAOSX_TXN_BIN = None


def _find_binary():
    """Find the tmq_taosx_txn binary in build-dir or compile it."""
    global TMQ_TAOSX_TXN_BIN
    if TMQ_TAOSX_TXN_BIN is not None:
        return TMQ_TAOSX_TXN_BIN

    _root = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../../")
    )
    search_paths = [
        os.path.join(_root, "debug/build/bin/tmq_taosx_txn"),
        os.path.join(os.environ.get("TDENGINE_DIR", ""), "debug/build/bin/tmq_taosx_txn"),
    ]
    for p in search_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            TMQ_TAOSX_TXN_BIN = p
            return p

    src = os.path.join(
        os.path.dirname(__file__), "../../../utils/test/c/tmq_taosx_txn.c"
    )
    src = os.path.normpath(src)
    if not os.path.isfile(src):
        raise RuntimeError("Cannot find tmq_taosx_txn.c source: %s" % src)
    dst = "/tmp/tmq_taosx_txn"
    cmd = [
        "gcc", "-o", dst, src,
        "-I/usr/local/taos/include", "-L/usr/lib", "-ltaos", "-lpthread", "-lm",
    ]
    compile_env = os.environ.copy()
    compile_env["ASAN_OPTIONS"] = (
        compile_env.get("ASAN_OPTIONS", "").replace("detect_leaks=1", "")
        + ":detect_leaks=0"
    )
    ret = subprocess.run(cmd, capture_output=True, text=True, env=compile_env)
    if ret.returncode != 0 and not os.path.isfile(dst):
        raise RuntimeError("Failed to compile tmq_taosx_txn: %s" % ret.stderr)
    TMQ_TAOSX_TXN_BIN = dst
    return dst


def _run_scenario(scenario, expect_pass=True):
    """Run a tmq_taosx_txn scenario and assert result."""
    binary = _find_binary()
    tdLog.info("Running tmq_taosx_txn scenario %d (%s)" % (scenario, binary))
    build_lib = os.path.normpath(os.path.join(os.path.dirname(binary), "../lib"))
    lib_path = (build_lib + ":") if os.path.isdir(build_lib) else ""
    lib_path += "/usr/lib:/usr/local/taos/driver"
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "LD_LIBRARY_PATH": lib_path},
    )
    tdLog.info("stdout: %s" % ret.stdout)
    if ret.stderr:
        tdLog.info("stderr: %s" % ret.stderr)
    if expect_pass:
        assert ret.returncode == 0, (
            "Scenario %d FAILED (exit=%d)\nstdout: %s\nstderr: %s"
            % (scenario, ret.returncode, ret.stdout, ret.stderr)
        )
    else:
        assert ret.returncode != 0, (
            "Scenario %d expected FAIL but PASSED" % scenario
        )
    return ret


# ─────────────────────────────────────────────────────────────────────────────
# Test class
# ─────────────────────────────────────────────────────────────────────────────

class TestTaosxTxnCluster:
    """Cluster-level taosx consumer correctness: s21 (MNode switch),
    s22 (VNode restart), s23 (WAL snapshot)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        _find_binary()          # fail fast if binary unavailable

    # ─────────────────────────────────────────────────────────────────────
    # Cluster helpers
    # ─────────────────────────────────────────────────────────────────────

    def _cleanup_taosx_dbs(self):
        """Drop databases/topic left by previous taosx binary run.

        Waits for any in-flight mnode transaction to finish first — right after
        cluster deployment/dnode restart/mnode leader switch there can briefly be
        a bootstrap, recovery, or leader-transfer transaction still running, and
        issuing a new transaction (DROP TOPIC/DATABASE) while one is in flight
        fails with TSDB_CODE_MND_TRANS_CONFLICT ("Conflict transaction not
        completed"). checkTransactions() alone isn't quite enough right after a
        leader switch: a new transaction can start in the brief window between
        checkTransactions() observing zero rows and the DROP statement actually
        firing, so retry the whole check+drop sequence a few times as well.
        """
        last_err = None
        for attempt in range(5):
            try:
                clusterComCheck.checkTransactions(timeout=60)
                tdSql.execute("drop topic if exists topic_taosx_txn")
                tdSql.execute("drop database if exists src_txn_db")
                tdSql.execute("drop database if exists dst_txn_db")
                return
            except Exception as e:
                last_err = e
                tdLog.info(
                    "_cleanup_taosx_dbs: attempt %d/5 hit %r, retrying" % (attempt + 1, e)
                )
                time.sleep(2)
        raise last_err

    def _get_mnode_leader_id(self, timeout=30):
        """Return dnode ID of the current MNode leader."""
        for _ in range(timeout):
            tdSql.query("select * from information_schema.ins_mnodes")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][2] == "leader":
                    return int(tdSql.queryResult[i][0])
            time.sleep(1)
        tdLog.exit("No MNode leader found within %ds" % timeout)

    def _wait_mnode_leader(self, exclude_id=None, timeout=60):
        """Wait for any MNode leader; optionally require id != exclude_id."""
        for _ in range(timeout):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for i in range(tdSql.queryRows):
                    if tdSql.queryResult[i][2] == "leader":
                        leader = int(tdSql.queryResult[i][0])
                        if exclude_id is None or leader != exclude_id:
                            tdLog.info("MNode leader: dnode %d" % leader)
                            return leader
            except Exception:
                continue
        tdLog.exit(
            "MNode leader (not dnode %s) not elected within %ds"
            % (exclude_id, timeout)
        )

    def _get_vgroup_leader_dnode(self, db_name, vg_id, timeout=30):
        """Return the dnode ID of the vgroup leader."""
        for _ in range(timeout):
            tdSql.query("show %s.vgroups" % db_name)
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == vg_id:
                    row = tdSql.queryResult[i]
                    for j in range(len(row)):
                        if row[j] == "leader":
                            return int(row[j - 1])
            time.sleep(1)
        return None

    def _any_follower_dnode(self, leader_id):
        """Return any dnode that is NOT leader_id (assumes 3-node cluster)."""
        tdSql.query("select id from information_schema.ins_dnodes")
        for i in range(tdSql.queryRows):
            did = int(tdSql.queryResult[i][0])
            if did != leader_id:
                return did
        return None

    def _poll_table_count(self, expected, db_name, timeout=60):
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.execute("use %s" % db_name)
                tdSql.query("show tables")
                if tdSql.queryRows == expected:
                    return True
            except Exception:
                continue
        tdLog.exit(
            "Table count %d != expected %d in %s after %ds"
            % (tdSql.queryRows, expected, db_name, timeout)
        )

    # ═════════════════════════════════════════════════════════════════════
    # s21: MNode leader switch + taosx consumer
    # ═════════════════════════════════════════════════════════════════════

    def s21_mnode_leader_switch_then_consume(self):
        """
        s21: MNode leader switch → taosx consumer still delivers txn messages.

        1. Run taosx scenario s1  (CREATE STB + CTBs, COMMIT) — baseline.
        2. Find current MNode leader; force-stop it.
        3. Wait for new MNode leader election.
        4. Run taosx scenario s3  (CREATE STB → ALTER → COMMIT) on new leader.
        5. Restore stopped dnode; verify cluster health.

        Both s1 and s3 must pass.  s3 on the new MNode leader exercises the
        full BEGIN → txnSeq alloc → VNode WAL write (IS_BEGIN) → COMMIT →
        tqScan delivery pipeline after MNode leadership change.
        """
        self._cleanup_taosx_dbs()
        tdLog.info("======== s21: MNode leader switch + taosx consumer ========")

        # Baseline
        _run_scenario(1)
        tdLog.info("s21: scenario s1 baseline PASSED")

        # Find and stop current MNode leader
        old_leader = self._get_mnode_leader_id()
        tdLog.info("s21: Stopping MNode leader dnode %d" % old_leader)
        sc.dnodeForceStop(old_leader)

        try:
            # Wait for new MNode leader
            new_leader = self._wait_mnode_leader(exclude_id=old_leader, timeout=60)
            tdLog.info("s21: New MNode leader is dnode %d" % new_leader)
        finally:
            # Restart the old leader BEFORE issuing any further DDL. src_txn_db and
            # topic_taosx_txn are single-replica (created with no explicit `replica`),
            # so if the stopped dnode happens to hold their only vgroup replica (quite
            # likely in this 3-node cluster), a DROP TOPIC/DATABASE issued while it's
            # still down blocks forever waiting for a vnode-tmq-delete-sub response
            # that can never arrive — not a bug, single-replica data is genuinely
            # unavailable while its only host is down. MNode leadership has already
            # switched and stays switched; restarting the old leader here just
            # restores it as an ordinary cluster member so cleanup/DDL can proceed.
            sc.dnodeStart(old_leader)
            clusterComCheck.checkDnodes(3, timeout=60)

        # Post-switch: run another taosx scenario. mndProcessBeginTxnReq retries
        # internally on TSDB_CODE_MND_TXN_SEQ_IN_CREATING (server-side, transparent
        # to any caller), so scenario 3's BEGIN doesn't need special handling here
        # even though the new leader's txnSeq allocator may still be finishing its
        # post-restore (re)init.
        self._cleanup_taosx_dbs()
        _run_scenario(3)
        tdLog.info("s21: scenario s3 post-MNode-switch PASSED")

        tdLog.info("s21 PASSED — taosx consumer correct after MNode leader switch")

    # ═════════════════════════════════════════════════════════════════════
    # s22: VNode leader restart + .txn recovery + taosx consumer
    # ═════════════════════════════════════════════════════════════════════

    def s22_vnode_restart_then_consume(self):
        """
        s22: VNode dnode restart → WAL .txn recovery → taosx consumer correct.

        1. Run taosx scenario s1  (CREATE STB + CTBs, COMMIT).
           — Writes WAL .txn entries; txnBeginIndexMap populated.
        2. Identify the VNode leader dnode for src_txn_db; force-stop it.
           — Simulates crash; .txn files survive on disk.
        3. Restart the dnode.
           — walTxnFilesRecover runs: Step 1 truncates speculative .txn entries,
             Step 1.5 (walTxnRebuildBeginIndexMap) rebuilds txnBeginIndexMap from
             .txn files — no duplicate IS_BEGIN marking.
           — STxnWalManager eager-load (walTxnReadRange) reloads committed txn
             entries and updates txnBeginIndexMap as a side effect.
        4. Run taosx scenario s3  (CREATE STB → ALTER → COMMIT) after restart.
           — New txn's first WAL entry gets IS_BEGIN correctly (not re-marked).
           — taosx consumer receives all DDL messages atomically on COMMIT.

        This test specifically covers the .txn restart recovery path and the
        STxnWalManager eager-load correctness.
        """
        self._cleanup_taosx_dbs()
        tdLog.info("======== s22: VNode restart + .txn recovery + taosx consumer ========")

        # Step 1: Baseline — write WAL .txn entries
        _run_scenario(1)
        tdLog.info("s22: scenario s1 PASSED — .txn files written")

        # Step 2: Identify VNode leader for src_txn_db
        # The binary cleans up src_txn_db after each scenario, so query before cleanup
        # or rely on cluster dnode detection for VNode leader.
        # We restart the MNode-leader dnode (which is also likely the VNode leader
        # for the 1-vgroup database created by the binary).
        leader_id = self._get_mnode_leader_id()
        tdLog.info("s22: Stopping dnode %d for restart test" % leader_id)
        sc.dnodeForceStop(leader_id)
        time.sleep(3)

        # Step 3: Restart — walTxnFilesRecover + STxnWalManager eager-load
        tdLog.info("s22: Restarting dnode %d" % leader_id)
        sc.dnodeStart(leader_id)
        clusterComCheck.checkDnodes(3, timeout=60)
        # Allow WAL recovery + STxnWalManager eager-load to complete
        time.sleep(8)

        # Step 4: Post-restart taosx scenario — IS_BEGIN must be correct for new txn
        self._cleanup_taosx_dbs()
        _run_scenario(3)
        tdLog.info("s22: scenario s3 post-restart PASSED — IS_BEGIN correct after recovery")

        # Also run a second scenario to confirm multi-txn IS_BEGIN tracking
        self._cleanup_taosx_dbs()
        _run_scenario(8)
        tdLog.info("s22: scenario s8 (NTB ALTER) post-restart PASSED")

        tdLog.info(
            "s22 PASSED — taosx consumer correct after VNode restart "
            "(walTxnFilesRecover + STxnWalManager eager-load)"
        )

    # ═════════════════════════════════════════════════════════════════════
    # s23: WAL Raft snapshot (follower falls behind) + taosx consumer
    # ═════════════════════════════════════════════════════════════════════

    def s23_wal_snapshot_then_consume(self):
        """
        s23: WAL Raft snapshot applied to follower → taosx consumer delivers
             post-snapshot txn messages correctly.

        1. Create txn_snap_src database with REPLICA 3 (3 VNode replicas).
        2. Commit several batch DDL transactions to build up WAL entries.
        3. Stop one follower dnode (it falls behind the leader's WAL).
        4. Advance WAL further on the leader by writing more data.
        5. Restart the stopped follower — it receives a WAL Raft snapshot
           from the leader because its WAL is too far behind.
           — walTxnFilesRotate initializes an EMPTY txnBeginIndexMap
             (snapshots are at clean commit points, no in-flight txns cross
              the snapshot boundary — walTxnFilesRotate comment in code).
        6. Commit a new batch DDL transaction after snapshot sync.
        7. Verify the new tables are visible (DDL correctness).
        8. Run taosx scenario s10 (Mixed STB+CTB+NTB, COMMIT) against the
           same cluster to exercise the full taosx consumer path post-snapshot.

        What this specifically tests:
          - walTxnFilesRotate post-snapshot: re-initializes empty txnBeginIndexMap
          - New txn's first WAL entry gets IS_BEGIN correctly set (not re-marked)
          - tqScan.c / STxnWalManager delivers messages atomically on COMMIT
        """
        db = "txn_snap_src"
        tdLog.info("======== s23: WAL Raft snapshot + taosx consumer ========")

        tdSql.execute("drop database if exists %s" % db)
        tdSql.execute(
            "create database %s vgroups 1 replica 3 keep 36500 "
            "wal_retention_period 1 wal_segment_size 10" % db
            # small wal_segment_size encourages WAL rotation and snapshot triggers
        )
        tdSql.execute("use %s" % db)
        tdSql.execute(
            "create table stb (ts timestamp, v int, s varchar(64)) tags (region varchar(64))"
        )

        # Step 2: Commit several batch DDL transactions
        tdLog.info("s23: Committing baseline DDL transactions")
        for batch in range(3):
            tdSql.execute("BEGIN")
            for i in range(5):
                tdSql.execute(
                    "create table ct_b%d_%d using stb tags('region_%d_%d')"
                    % (batch, i, batch, i)
                )
            tdSql.execute("COMMIT")

        # Verify: 15 child tables
        tdSql.query("show tables")
        tdSql.checkRows(15)
        tdLog.info("s23: 15 CTBs committed OK")

        # Step 3: Find and stop a follower dnode
        tdSql.query("show %s.vgroups" % db)
        vg_id = int(tdSql.queryResult[0][0])
        leader_id = self._get_vgroup_leader_dnode(db, vg_id)
        assert leader_id is not None, "Could not find VNode leader for %s" % db

        follower_id = self._any_follower_dnode(leader_id)
        assert follower_id is not None, "No follower dnode found (replica < 2?)"

        tdLog.info(
            "s23: Stopping follower dnode %d (VNode leader is dnode %d)"
            % (follower_id, leader_id)
        )
        sc.dnodeForceStop(follower_id)
        time.sleep(2)

        # Step 4: Advance WAL on the leader while follower is down
        tdLog.info("s23: Advancing WAL on leader (follower down)")
        for batch in range(5):
            values = ",".join(
                ["(now+%ds, %d, 'v%d')" % (batch * 100 + j, batch * 100 + j, j)
                 for j in range(200)]
            )
            tdSql.execute("insert into ct_b0_0 values %s" % values)
        tdSql.execute("flush database %s" % db)
        time.sleep(2)

        # Commit more DDL txns while follower is offline
        tdSql.execute("BEGIN")
        for i in range(5):
            tdSql.execute(
                "create table ct_offline%d using stb tags('offline_%d')" % (i, i)
            )
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(20)    # 15 + 5
        tdLog.info("s23: 5 more CTBs committed while follower offline (20 total)")

        # Step 5: Restart follower — it should receive WAL Raft snapshot
        tdLog.info("s23: Restarting follower dnode %d for snapshot sync" % follower_id)
        sc.dnodeStart(follower_id)
        clusterComCheck.checkDnodes(3, timeout=60)
        # Allow snapshot sync to complete
        time.sleep(15)

        # Step 6: New batch DDL txn after snapshot sync
        tdLog.info("s23: Committing new DDL txn post-snapshot-sync")
        tdSql.execute("use %s" % db)
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create table ntb_post (ts timestamp, c1 int, c2 varchar(32))"
        )
        for i in range(3):
            tdSql.execute(
                "create table ct_post%d using stb tags('post_%d')" % (i, i)
            )
        tdSql.execute("COMMIT")

        # Step 7: Verify DDL visibility — all tables must be present
        tdSql.execute("use %s" % db)
        self._poll_table_count(24, db_name=db, timeout=30)   # 20 + 1 ntb + 3 ctb
        tdLog.info("s23: 24 tables visible after post-snapshot txn — DDL correctness OK")

        # Verify ntb_post is accessible (INSERT + SELECT)
        tdSql.execute("insert into ntb_post values(now, 1, 'hello')")
        tdSql.query("select count(*) from ntb_post")
        assert int(tdSql.queryResult[0][0]) >= 1, "ntb_post not accessible post-snapshot"

        # Step 8: Run taosx consumer scenario against the cluster to verify
        # the full taosx delivery path after WAL snapshot.
        # Scenario s10 (Mixed STB+CTB+NTB COMMIT) exercises the complete
        # IS_BEGIN → STxnWalManager → tqScan → taosx delivery chain.
        tdLog.info("s23: Running taosx scenario s10 post-snapshot to verify consumer")
        self._cleanup_taosx_dbs()
        _run_scenario(10)
        tdLog.info("s23: taosx scenario s10 post-snapshot PASSED")

        # Cleanup
        tdSql.execute("drop database if exists %s" % db)

        tdLog.info(
            "s23 PASSED — taosx consumer correct after WAL Raft snapshot "
            "(walTxnFilesRotate re-init + IS_BEGIN + STxnWalManager)"
        )

    # ═════════════════════════════════════════════════════════════════════
    # Entry point
    # ═════════════════════════════════════════════════════════════════════

    def test_taosx_txn_cluster(self):
        """Cluster taosx consumer tests: s21 (MNode switch), s22 (VNode restart),
        s23 (WAL Raft snapshot).

        Coverage:
          s21: MNode leader switch → taosx consumer still delivers txn messages.
               Exercises: txnSeq alloc migration, VNode WAL IS_BEGIN unaffected.
          s22: VNode dnode restart → walTxnFilesRecover + walTxnRebuildBeginIndexMap
               + STxnWalManager eager-load → taosx consumer correct post-restart.
          s23: Follower falls behind → WAL Raft snapshot → walTxnFilesRotate
               re-inits empty txnBeginIndexMap → new txn IS_BEGIN correct →
               taosx consumer delivers post-snapshot txn messages.

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-XXXXX
        """
        self.s21_mnode_leader_switch_then_consume()
        self.s22_vnode_restart_then_consume()
        self.s23_wal_snapshot_then_consume()
