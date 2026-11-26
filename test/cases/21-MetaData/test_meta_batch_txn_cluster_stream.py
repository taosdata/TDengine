# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-


"""Cluster batch meta txn: stream failover tests (cs72-cs80).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Stream objects live only in MNode SDB and reuse the same txn fields (txnId / txnStatus) and
shadow-op recovery path as super tables (DS 4.2 / 5.2 / 6.4). These tests cover the
failover behaviour the single-node stream tests cannot: MNode leader switch and full
cluster restart while a stream CREATE / DROP is still pending inside a transaction.

The cs-prefix distinguishes this file's case numbers from the s-prefixed cases in
test_meta_batch_txn_stream.py (s1-s13) and test_meta_batch_txn_stress{2}.py (s100+),
so a grep for "s72" does not collide across files.

Tests cover (DS 8 leader switch/reboot, stream-object dimension):
  - cs72: txn DROP STREAM + MNode leader switch + ROLLBACK -> stream kept and still running
  - cs73: txn CREATE STREAM + MNode leader switch + COMMIT -> stream deploys and runs
  - cs74: txn CREATE STREAM + MNode leader switch + ROLLBACK -> stream gone (no residue)
  - cs75: full cluster restart with an in-txn PRE_CREATE stream -> COMMIT deploys it
  - cs76: full cluster restart with an in-txn PRE_CREATE_DROP stream -> COMMIT removes
          it (DS §8 "PRE_CREATE_DROP finalize：rebuild shadow ops after reboot")
  - cs77: full cluster restart with an in-txn PRE_DROP stream -> ROLLBACK restores it
          (new leader keeps the stream running; DS 5.2's userDropped-is-not-persisted
          property, restart dimension)
  - cs78: txn STOP STREAM + MNode leader switch -> lock still enforced, COMMIT stops the stream
  - cs79: txn STOP STREAM + full cluster restart -> lock still enforced, ROLLBACK keeps the
          stream running and releases the lock (FS 4.16.2 persistence promise)
  - cs80: txn START STREAM + full cluster restart -> intent not applied early, COMMIT starts it
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time


class TestBatchMetaTxnClusterStream:
    """Cluster batch meta txn: stream failover (cs72-cs77, 3 dnode, 3 mnode)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    DEPLOY_WAIT_S = 30

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # Streams only deploy on a snode. It lives on dnode 1; a leader switch that kills
        # dnode 1 also briefly drops the snode, but the assertions only need the stream to
        # be *defined* and re-running after the cluster is restored.
        tdSql.execute_ignore_error("create snode on dnode 1")

    def _reset_env(self, db_name="txn_cdb"):
        """Reset test database. Uses replica 3 for VNode HA tests."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db_name}")
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")

    def _get_mnode_leader_dnode_id(self):
        """Get the dnode ID of the current MNode leader."""
        tdSql.query("select * from information_schema.ins_mnodes")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                return tdSql.queryResult[i][0]      # id column (1-based dnode id)
        return None

    def _wait_mnode_leader_elected(self, timeout=30):
        """Wait for any MNode leader to be elected (ignoring offline nodes)."""
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for r in range(tdSql.queryRows):
                    if tdSql.queryResult[r][2] == 'leader':
                        return True
            except Exception:
                continue
        tdLog.exit("No MNode leader elected within %ds" % timeout)
        return False

    def _stream_names(self):
        tdSql.query("show streams")
        return sorted(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    def _stream_status(self, name):
        tdSql.query("show streams")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == name:
                return tdSql.queryResult[i][1]
        return None

    def _wait_status(self, name, wanted, timeout=None):
        """Wait until `name` reports one of `wanted`. Returns the status seen."""
        timeout = timeout or self.DEPLOY_WAIT_S
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                tdSql.query("show streams")
            except Exception:
                time.sleep(1)
                continue
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == name:
                    last = tdSql.queryResult[i][1]
                    if last in wanted:
                        return last
            time.sleep(1)
        return last

    def _create_stream(self, name, out):
        tdSql.execute(
            f"create stream {name} interval(10s) sliding(10s) from ct1"
            f" into {out} as select _twstart ts, count(*) cnt from %%trows"
        )

    # =========================================================================
    # cs72: txn DROP STREAM, MNode leader switch, then ROLLBACK.
    #
    # DS 5.2: an in-txn DROP only marks txnStatus = PRE_DROP in SDB and does NOT set the
    # in-memory, non-persisted userDropped flag, nor undeploy the task. A leader switch must
    # therefore not lose the drop intent: the new leader reads txnStatus from SDB (userDropped
    # is 0 there) and keeps the stream running. ROLLBACK then clears the marker and the stream
    # must still be Running without a redeploy.
    # =========================================================================

    def cs72_txn_drop_stream_leader_switch_rollback(self):
        self._reset_env()
        tdLog.info("======== cs72_txn_drop_stream_leader_switch_rollback")

        self._create_stream("cs72", "o72")
        self._wait_status("cs72", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream cs72")
        assert self._stream_status("cs72") == "pending_drop", self._stream_status("cs72")

        # Kill MNode leader, wait for a new one.
        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it (in-txn stream DROP pending)")
        sc.dnodeForceStop(leader_id)
        try:
            clusterComCheck.check3mnodeoff(leader_id)

            # ROLLBACK via the new leader.
            tdSql.execute("ROLLBACK")
        finally:
            # Restore the killed dnode even if an assertion above fails, so the
            # remaining cases (cs73-cs77) keep a healthy 3-dnode cluster.
            sc.dnodeStart(leader_id)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)

        assert "cs72" in self._stream_names(), "stream lost after leader switch + ROLLBACK"
        status = self._wait_status("cs72", ("Running", "Idle"), timeout=90)
        assert status in ("Running", "Idle"), \
            f"cs72 should still run after leader switch + ROLLBACK, got {status}"
        tdLog.info(f"cs72 survived leader switch + ROLLBACK, still {status}")

    # =========================================================================
    # cs73: txn CREATE STREAM, MNode leader switch, then COMMIT.
    #
    # A PRE_CREATE stream must not deploy before COMMIT (m13 covers single-node); after a
    # leader switch the new leader reads PRE_CREATE from SDB and only deploys on COMMIT.
    # =========================================================================

    def cs73_txn_create_stream_leader_switch_commit(self):
        self._reset_env()
        tdLog.info("======== cs73_txn_create_stream_leader_switch_commit")

        tdSql.execute("BEGIN")
        self._create_stream("cs73", "o73")
        assert "cs73" in self._stream_names(), "PRE_CREATE stream should be visible in-txn"

        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it (in-txn stream CREATE pending)")
        sc.dnodeForceStop(leader_id)
        try:
            clusterComCheck.check3mnodeoff(leader_id)

            tdSql.execute("COMMIT")
        finally:
            # Restore the killed dnode even if an assertion above fails.
            sc.dnodeStart(leader_id)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)

        assert "cs73" in self._stream_names(), "stream lost after leader switch + COMMIT"
        status = self._wait_status("cs73", ("Running", "Idle"), timeout=90)
        assert status in ("Running", "Idle"), \
            f"cs73 should deploy after leader switch + COMMIT, got {status}"
        tdLog.info(f"cs73 deployed after leader switch + COMMIT, status={status}")

    # =========================================================================
    # cs74: txn CREATE STREAM, MNode leader switch, then ROLLBACK.
    #
    # The PRE_CREATE stream is never deployed, so ROLLBACK only removes the SDB object. After
    # a leader switch the new leader must still be able to undo it, leaving no residue.
    # =========================================================================

    def cs74_txn_create_stream_leader_switch_rollback(self):
        self._reset_env()
        tdLog.info("======== cs74_txn_create_stream_leader_switch_rollback")

        tdSql.execute("BEGIN")
        self._create_stream("cs74", "o74")
        assert "cs74" in self._stream_names(), "PRE_CREATE stream should be visible in-txn"

        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it (in-txn stream CREATE pending)")
        sc.dnodeForceStop(leader_id)
        try:
            clusterComCheck.check3mnodeoff(leader_id)

            tdSql.execute("ROLLBACK")
        finally:
            # Restore the killed dnode even if an assertion above fails.
            sc.dnodeStart(leader_id)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)

        assert "cs74" not in self._stream_names(), "PRE_CREATE stream residue after leader switch + ROLLBACK"
        tdLog.info("cs74 removed by leader switch + ROLLBACK, no residue")

    # =========================================================================
    # cs75: full cluster restart with an in-txn PRE_CREATE stream, then COMMIT.
    #
    # MNode restart rebuilds shadow ops from SDB_STREAM (txnId != 0), the client
    # auto-reconnects and its heartbeat keeps the txn alive, and COMMIT still deploys the
    # stream. Mirrors s45 (table DDL survives cluster restart) for the stream object.
    # =========================================================================

    def cs75_cluster_restart_stream_txn_survives(self):
        self._reset_env()
        tdLog.info("======== cs75_cluster_restart_stream_txn_survives")

        tdSql.execute("BEGIN")
        self._create_stream("cs75", "o75")
        assert "cs75" in self._stream_names(), "PRE_CREATE stream should be visible in-txn"

        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        tdSql.execute("COMMIT")

        tdSql.execute("use txn_cdb")
        assert "cs75" in self._stream_names(), "stream lost after cluster restart + COMMIT"
        status = self._wait_status("cs75", ("Running", "Idle"), timeout=60)
        assert status in ("Running", "Idle"), \
            f"cs75 should deploy after cluster restart + COMMIT, got {status}"
        tdLog.info(f"cs75 deployed after cluster restart + COMMIT, status={status}")

    # =========================================================================
    # cs76: full cluster restart with an in-txn PRE_CREATE_DROP stream, then COMMIT.
    #
    # CREATE STREAM + DROP STREAM in the same txn leaves the stream in
    # META_TXN_PRE_CREATE_DROP (DS 4.2). On restart the rebuild path emits BOTH a
    # CREATE_STREAM and a DROP_STREAM shadow op for it (mndTxn.c). COMMIT must then
    # net to "does not exist" — no residue. This is DS §8's "PRE_CREATE_DROP 终结：
    # 重启后影子操作重建正确" for the stream dimension.
    # =========================================================================

    def cs76_cluster_restart_pre_create_drop(self):
        self._reset_env()
        tdLog.info("======== cs76_cluster_restart_pre_create_drop")

        tdSql.execute("BEGIN")
        self._create_stream("cs76", "o76")
        assert "cs76" in self._stream_names(), "PRE_CREATE stream should be visible in-txn"
        tdSql.execute("drop stream cs76")
        # Created and dropped in the same txn: nobody sees it (mndRetrieveStream
        # hides PRE_CREATE_DROP from every connection).
        assert "cs76" not in self._stream_names(), \
            "PRE_CREATE_DROP stream must be invisible to its own txn"

        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        tdSql.execute("COMMIT")

        tdSql.execute("use txn_cdb")
        assert "cs76" not in self._stream_names(), \
            "PRE_CREATE_DROP stream residue after cluster restart + COMMIT"
        tdLog.info("cs76 no residue after cluster restart + COMMIT")

    # =========================================================================
    # cs77: full cluster restart with an in-txn PRE_DROP stream, then ROLLBACK.
    #
    # Same failure window as cs72, but the interruption is a full cluster restart
    # instead of a leader switch. The new leader rebuilds the stream's txnStatus
    # from SDB (PRE_DROP) and, because userDropped was never persisted, keeps the
    # task running. ROLLBACK clears the marker and the stream stays Running —
    # proving the drop intent lives in SDB, not in the lost in-memory flag.
    # =========================================================================

    def cs77_cluster_restart_pre_drop_rollback(self):
        self._reset_env()
        tdLog.info("======== cs77_cluster_restart_pre_drop_rollback")

        self._create_stream("cs77", "o77")
        self._wait_status("cs77", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream cs77")
        assert self._stream_status("cs77") == "pending_drop", self._stream_status("cs77")

        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        tdSql.execute("ROLLBACK")

        tdSql.execute("use txn_cdb")
        assert "cs77" in self._stream_names(), \
            "stream lost after cluster restart + ROLLBACK (drop intent not restored)"
        status = self._wait_status("cs77", ("Running", "Idle"), timeout=60)
        assert status in ("Running", "Idle"), \
            f"cs77 should still run after cluster restart + ROLLBACK, got {status}"
        tdLog.info(f"cs77 survived cluster restart + ROLLBACK, still {status}")

    # =========================================================================
    # cs78-cs80: START / STOP STREAM inside a txn, MNode leader switch / full restart.
    #
    # FS 4.16.2: the stream lock (txnId + PRE_ALTER + intent) and the pending intent are
    # persisted on the stream object, so they survive an MNode leader switch and a cluster
    # restart (rebuilt by mndTxnRebuildShadowOpsFromSdb). Three things must hold afterwards:
    #   - the lock is still enforced: a second session is refused with TXN_RESOURCE_BUSY (0x3317);
    #   - the intent is not applied early: the stream keeps its pre-txn run state until COMMIT;
    #   - COMMIT applies the intent, ROLLBACK discards it and releases the lock.
    # =========================================================================

    def _busy_on_other_session(self, sql):
        """`sql` from a separate session must fail with TSDB_CODE_TXN_RESOURCE_BUSY (0x3317)."""
        other = tdCom.newTdSql()
        try:
            other.execute("use txn_cdb")
            try:
                other.execute(sql)
            except Exception as e:
                assert "0x3317" in str(e) or "being modified by another transaction" in str(e), \
                    f"expected TXN_RESOURCE_BUSY for [{sql}], got: {e}"
                return
            raise AssertionError(f"[{sql}] must be refused while the txn holds the stream lock")
        finally:
            other.close()

    def cs78_stop_stream_lock_survives_leader_switch(self):
        self._reset_env()
        tdLog.info("======== cs78_stop_stream_lock_survives_leader_switch")

        self._create_stream("cs78", "o78")
        assert self._wait_status("cs78", ("Running", "Idle")) in ("Running", "Idle")

        tdSql.execute("BEGIN")
        tdSql.execute("stop stream cs78")
        assert self._stream_status("cs78") in ("Running", "Idle"), self._stream_status("cs78")

        leader_id = self._get_mnode_leader_dnode_id()
        tdLog.info(f"MNode leader: dnode {leader_id}, killing it (in-txn STOP STREAM pending)")
        sc.dnodeForceStop(leader_id)
        try:
            clusterComCheck.check3mnodeoff(leader_id)
            # New leader: lock still enforced, intent still not applied.
            self._busy_on_other_session("stop stream cs78")
            self._busy_on_other_session("start stream cs78")
            self._busy_on_other_session("drop stream cs78")
            # Only "not Stopped" is asserted. SHOW STREAMS reports Stopped as soon as the intent is applied
            # (userStopped wins over every runtime state), so this is exactly the "applied early" check.
            # Running/Idle must not be required: when the killed dnode also led the vgroup of the stream's
            # reader, the new MNode leader finds that task missing at the end of WATCH and marks the stream
            # Failed (Stream task lost) until its ~30s redeploy retry. That is the stream engine's own
            # recovery, unrelated to the txn.
            assert self._stream_status("cs78") != "Stopped", \
                f"STOP intent applied before COMMIT, status={self._stream_status('cs78')}"
            tdSql.execute("COMMIT")
        finally:
            sc.dnodeStart(leader_id)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)

        tdSql.execute("use txn_cdb")
        status = self._wait_status("cs78", ("Stopped",), timeout=60)
        assert status == "Stopped", f"cs78 should be stopped by COMMIT after a leader switch, got {status}"
        # Lock released by COMMIT: an ordinary START works again.
        tdSql.execute("start stream cs78")
        status = self._wait_status("cs78", ("Running", "Idle"), timeout=60)
        assert status in ("Running", "Idle"), f"cs78 did not restart after the lock was released, got {status}"

    def cs79_stop_stream_lock_survives_cluster_restart_rollback(self):
        self._reset_env()
        tdLog.info("======== cs79_stop_stream_lock_survives_cluster_restart_rollback")

        self._create_stream("cs79", "o79")
        assert self._wait_status("cs79", ("Running", "Idle")) in ("Running", "Idle")

        tdSql.execute("BEGIN")
        tdSql.execute("stop stream cs79")

        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # The lock was rebuilt from SDB: still refused for other sessions, intent not applied.
        self._busy_on_other_session("stop stream cs79")
        self._busy_on_other_session("drop stream cs79")

        tdSql.execute("ROLLBACK")
        tdSql.execute("use txn_cdb")
        status = self._wait_status("cs79", ("Running", "Idle"), timeout=60)
        assert status in ("Running", "Idle"), \
            f"cs79 must keep running after cluster restart + ROLLBACK, got {status}"

        # Lock released: a plain STOP from another session now succeeds.
        other = tdCom.newTdSql()
        try:
            other.execute("use txn_cdb")
            other.execute("stop stream cs79")
        finally:
            other.close()
        assert self._wait_status("cs79", ("Stopped",), timeout=60) == "Stopped", self._stream_status("cs79")

    def cs80_start_stream_lock_survives_cluster_restart_commit(self):
        self._reset_env()
        tdLog.info("======== cs80_start_stream_lock_survives_cluster_restart_commit")

        self._create_stream("cs80", "o80")
        assert self._wait_status("cs80", ("Running", "Idle")) in ("Running", "Idle")
        tdSql.execute("stop stream cs80")
        assert self._wait_status("cs80", ("Stopped",), timeout=60) == "Stopped"

        tdSql.execute("BEGIN")
        tdSql.execute("start stream cs80")
        assert self._stream_status("cs80") == "Stopped", self._stream_status("cs80")

        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        self._busy_on_other_session("start stream cs80")
        # The START intent was not applied by the restart: the stream is still stopped.
        # Right after a restart streamMgmt is still in WATCH state and SHOW STREAMS reports
        # "Undeployed" (Mnode may be unstable), so wait for it to settle instead of reading once.
        tdSql.execute("use txn_cdb")
        status = self._wait_status("cs80", ("Stopped",), timeout=60)
        assert status == "Stopped", status

        tdSql.execute("COMMIT")
        status = self._wait_status("cs80", ("Running", "Idle"), timeout=60)
        assert status in ("Running", "Idle"), f"cs80 must start at COMMIT after a cluster restart, got {status}"


    def test_meta_batch_txn_cluster_stream(self):
        """Cluster batch meta txn: stream failover (cs72-cs80)

        72. txn_drop_stream_leader_switch_rollback
        73. txn_create_stream_leader_switch_commit
        74. txn_create_stream_leader_switch_rollback
        75. cluster_restart_stream_txn_survives
        76. cluster_restart_pre_create_drop
        77. cluster_restart_pre_drop_rollback
        78. stop_stream_lock_survives_leader_switch
        79. stop_stream_lock_survives_cluster_restart_rollback
        80. start_stream_lock_survives_cluster_restart_commit

        Since: v3.4.3.0

        Labels: common,ci
        """
        self._wait_mnode_leader_elected()
        self.cs72_txn_drop_stream_leader_switch_rollback()
        self.cs73_txn_create_stream_leader_switch_commit()
        self.cs74_txn_create_stream_leader_switch_rollback()
        self.cs75_cluster_restart_stream_txn_survives()
        self.cs76_cluster_restart_pre_create_drop()
        self.cs77_cluster_restart_pre_drop_rollback()
        self.cs78_stop_stream_lock_survives_leader_switch()
        self.cs79_stop_stream_lock_survives_cluster_restart_rollback()
        self.cs80_start_stream_lock_survives_cluster_restart_commit()
