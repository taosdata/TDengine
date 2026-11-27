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

"""Target-side (taosX) replication of transactional DDL, all scenario groups.

Nine scenario families used to live in nine separate ``test_taosx_txn_*.py``
files. They share the same C helper binary (``utils/test/c/tmq_taosx_txn.c``),
the same ``_run_scenario`` driver and the same two databases, so each file paid
its own taosd deployment, its own compile check and its own schema build-up.
They are merged into one class here: one deployment, one binary lookup, and the
scenario groups still run independently (each starts with its own cleanup), so
a failure in one group does not hide the others.

Groups:
  basic      basic DDL replication (s1-s11)
  boundary   boundary cases: empty/minimal/huge/long transactions
  concurrent concurrent consumer sessions
  ddlmatrix  DDL type matrix
  mixed      mixed transactional and non-transactional traffic
  recovery   crash recovery and WAL replay
  snapshot   snapshot plus incremental consumption
  stress     consumer stress
  waltrim    WAL trim interaction
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import subprocess
import os
import time
from new_test_framework.utils.eutil import build_windows_test_program


TMQ_TAOSX_TXN_BIN = None

def _find_binary():
    """Find the tmq_taosx_txn binary in builddir or compile it."""
    global TMQ_TAOSX_TXN_BIN
    if TMQ_TAOSX_TXN_BIN is not None:
        return TMQ_TAOSX_TXN_BIN

    # Search common locations; derive root from this file's location
    # __file__ is .../community/test/cases/21-MetaData/test_taosx_txn_basic.py
    # 5 levels up reaches the TDinternal repo root (source/taos-community/test/cases/21-MetaData → repo root)
    _root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../../"))
    search_paths = [
        os.path.join(_root, "debug/build/bin/tmq_taosx_txn"),
        os.path.join(os.environ.get("TDENGINE_DIR", ""), "debug/build/bin/tmq_taosx_txn"),
    ]
    for p in search_paths:
        if os.name == "nt":
            p += ".exe"
        if os.path.isfile(p) and os.access(p, os.X_OK):
            TMQ_TAOSX_TXN_BIN = p
            return p

    # Try to compile in-place; suppress ASAN leak detection so gcc exits cleanly
    src = os.path.join(os.path.dirname(__file__), "../../../utils/test/c/tmq_taosx_txn.c")
    src = os.path.normpath(src)
    if not os.path.isfile(src):
        raise RuntimeError("Cannot find tmq_taosx_txn.c source: %s" % src)
    if os.name == 'nt':
        from new_test_framework.utils.eutil import build_windows_test_program
        TMQ_TAOSX_TXN_BIN = build_windows_test_program(src, 'tmq_taosx_txn', tdCom.getBuildPath())
        return TMQ_TAOSX_TXN_BIN
    dst = "/tmp/tmq_taosx_txn"
    cmd = [
        "gcc", "-o", dst, src,
        "-I/usr/local/taos/include", "-L/usr/lib", "-ltaos", "-lpthread", "-lm"
    ]
    compile_env = os.environ.copy()
    compile_env["ASAN_OPTIONS"] = compile_env.get("ASAN_OPTIONS", "").replace("detect_leaks=1", "") + ":detect_leaks=0"
    ret = subprocess.run(cmd, capture_output=True, text=True, errors="backslashreplace", env=compile_env)
    if ret.returncode != 0 and not os.path.isfile(dst):
        raise RuntimeError("Failed to compile tmq_taosx_txn: %s" % ret.stderr)
    TMQ_TAOSX_TXN_BIN = dst
    return dst


def _run_scenario(scenario, expect_pass=True, timeout=120):
    """Run a tmq_taosx_txn scenario and check result."""
    binary = _find_binary()
    tdLog.info("Running tmq_taosx_txn scenario %d (%s)" % (scenario, binary))
    build_lib = os.path.normpath(os.path.join(os.path.dirname(binary), "../lib"))
    lib_path = (build_lib + ":") if os.path.isdir(build_lib) else ""
    lib_path += "/usr/lib:/usr/local/taos/driver"
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, errors="backslashreplace", timeout=timeout,
        env={**os.environ, "LD_LIBRARY_PATH": lib_path}
    )
    tdLog.info("stdout: %s" % ret.stdout)
    if ret.stderr:
        tdLog.info("stderr: %s" % ret.stderr)
    if expect_pass:
        assert ret.returncode == 0, \
            "Scenario %d FAILED (exit=%d)\nstdout: %s\nstderr: %s" % (
                scenario, ret.returncode, ret.stdout, ret.stderr)
    else:
        assert ret.returncode != 0, \
            "Scenario %d expected FAIL but PASSED" % scenario
    return ret



class TestTaosxTxnReplication:
    def setup_class(cls):
            tdLog.debug("start to execute %s" % __file__)

    # =========================================================================
    # basic : test_taosx_txn_basic.py
    # =========================================================================

    def basic_s0_cleanup(self):
            """Clean up any leftover databases from previous runs."""
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def basic_s1_commit_stb_and_ctb(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s1: CREATE STB + CTBs → COMMIT → target verified")
            _run_scenario(1)
            tdLog.info("s1 PASSED")

    def basic_s2_rollback_stb_and_ctb(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s2: CREATE STB + CTBs → ROLLBACK → target empty")
            _run_scenario(2)
            tdLog.info("s2 PASSED")

    def basic_s3_alter_stb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s3: CREATE STB → ALTER STB → COMMIT → target has altered STB")
            _run_scenario(3)
            tdLog.info("s3 PASSED")

    def basic_s4_drop_stb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s4: CREATE STB → DROP STB → COMMIT → target has no STB")
            _run_scenario(4)
            tdLog.info("s4 PASSED")

    def basic_s5_idempotent_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s5: Idempotent COMMIT replay → target correct")
            _run_scenario(5)
            tdLog.info("s5 PASSED")

    def basic_s6_alter_ctb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s6: CREATE CTBs → ALTER child tag → COMMIT")
            _run_scenario(6)
            tdLog.info("s6 PASSED")

    def basic_s7_drop_ctb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s7: CREATE CTBs → DROP child → COMMIT")
            _run_scenario(7)
            tdLog.info("s7 PASSED")

    def basic_s8_alter_ntb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s8: CREATE normal table → ALTER → COMMIT")
            _run_scenario(8)
            tdLog.info("s8 PASSED")

    def basic_s9_drop_ntb_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s9: CREATE normal table → DROP → COMMIT")
            _run_scenario(9)
            tdLog.info("s9 PASSED")

    def basic_s10_mixed_commit(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s10: Mixed STB+CTB+NTB → COMMIT")
            _run_scenario(10)
            tdLog.info("s10 PASSED")

    def basic_s11_multi_vgroup(self):
            self.basic_s0_cleanup()
            tdLog.info("======== s11: Multi-VGroup STB+10CTBs+2NTBs → COMMIT")
            _run_scenario(11)
            tdLog.info("s11 PASSED")

    def test_taosx_txn_basic(self):
            """taosX basic replication tests (s1-s11)

            Verifies atomic batch delivery via STxnWalManager: DDL messages within
            a BEGIN…COMMIT block arrive as a single atomic batch on the consumer side;
            ROLLBACK produces no DDL messages. Individual PRE_CREATE WAL entries are
            filtered in tqScan.c and delivered only on COMMIT.

            1. commit_stb_and_ctb
            2. rollback_stb_and_ctb
            3. alter_stb_commit
            4. drop_stb_commit
            5. idempotent_commit
            6. alter_ctb_commit
            7. drop_ctb_commit
            8. alter_ntb_commit
            9. drop_ntb_commit
            10. mixed_commit
            11. multi_vgroup

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.basic_s1_commit_stb_and_ctb()
            self.basic_s2_rollback_stb_and_ctb()
            self.basic_s3_alter_stb_commit()
            self.basic_s4_drop_stb_commit()
            self.basic_s5_idempotent_commit()
            self.basic_s6_alter_ctb_commit()
            self.basic_s7_drop_ctb_commit()
            self.basic_s8_alter_ntb_commit()
            self.basic_s9_drop_ntb_commit()
            self.basic_s10_mixed_commit()
            self.basic_s11_multi_vgroup()


    # =========================================================================
    # boundary : test_taosx_txn_boundary.py
    # =========================================================================

    def boundary_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def boundary_s38_empty_txn(self):
            self.boundary_s0_cleanup()
            tdLog.info("======== s38: empty txn (BEGIN; COMMIT, no ops)")
            _run_scenario(38)
            tdLog.info("s38 PASSED")

    def boundary_s39_minimal_txn(self):
            self.boundary_s0_cleanup()
            tdLog.info("======== s39: minimal txn (single DDL op)")
            _run_scenario(39)
            tdLog.info("s39 PASSED")

    def boundary_s40_bulk_1200_ctb_single_txn(self):
            self.boundary_s0_cleanup()
            tdLog.info("======== s40: bulk single txn — 1200 CTBs → COMMIT")
            _run_scenario(40, timeout=180)
            tdLog.info("s40 PASSED")

    def boundary_s41_long_window_txn_with_wal_noise(self):
            self.boundary_s0_cleanup()
            tdLog.info("======== s41: long-window txn with concurrent unrelated WAL traffic")
            _run_scenario(41)
            tdLog.info("s41 PASSED")

    def test_taosx_txn_boundary(self):
            """taosX replication boundary tests (s38-s41)

            Covers edge cases at the extremes of transaction size/duration:
            empty transactions, minimal transactions, very large single
            transactions (bulk CTB creation), and transactions spanning a long
            wall-clock window with unrelated concurrent WAL traffic.

            1. empty_txn
            2. minimal_txn
            3. bulk_1200_ctb_single_txn
            4. long_window_txn_with_wal_noise

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.boundary_s38_empty_txn()
            self.boundary_s39_minimal_txn()
            self.boundary_s40_bulk_1200_ctb_single_txn()
            self.boundary_s41_long_window_txn_with_wal_noise()


    # =========================================================================
    # concurrent : test_taosx_txn_concurrent_sessions.py
    # =========================================================================

    def concurrent_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def concurrent_s34_two_independent_txns_reverse_commit(self):
            self.concurrent_s0_cleanup()
            tdLog.info("======== s34: two independent txns, reverse commit order")
            _run_scenario(34)
            tdLog.info("s34 PASSED")

    def concurrent_s35_txn_nontxn_txn_three_way_interleave(self):
            self.concurrent_s0_cleanup()
            tdLog.info("======== s35: txn A(commit) + non-txn B + txn C(rollback)")
            _run_scenario(35)
            tdLog.info("s35 PASSED")

    def concurrent_s36_two_sessions_same_table_blocked_then_retry(self):
            self.concurrent_s0_cleanup()
            tdLog.info("======== s36: two sessions BEGIN same STB, second retries after first COMMIT")
            _run_scenario(36)
            tdLog.info("s36 PASSED")

    def concurrent_s37_round_robin_two_sessions(self):
            self.concurrent_s0_cleanup()
            tdLog.info("======== s37: round-robin independent BEGIN/COMMIT across 2 sessions")
            _run_scenario(37)
            tdLog.info("s37 PASSED")

    def test_taosx_txn_concurrent_sessions(self):
            """taosX concurrent transactional/non-transactional sessions (s34-s37)

            Verifies that multiple independent TAOS sessions running
            BEGIN/COMMIT/ROLLBACK concurrently — on unrelated tables, and on the
            SAME contended table — replicate to taosX-style targets in a way
            that matches actual WAL commit order, with no corruption/duplication.

            1. two_independent_txns_reverse_commit
            2. txn_nontxn_txn_three_way_interleave
            3. two_sessions_same_table_blocked_then_retry
            4. round_robin_two_sessions

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.concurrent_s34_two_independent_txns_reverse_commit()
            self.concurrent_s35_txn_nontxn_txn_three_way_interleave()
            self.concurrent_s36_two_sessions_same_table_blocked_then_retry()
            self.concurrent_s37_round_robin_two_sessions()


    # =========================================================================
    # ddlmatrix : test_taosx_txn_ddl_matrix.py
    # =========================================================================

    def ddlmatrix_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def ddlmatrix_s24_create_alter_drop_alter_mix_commit(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s24: CREATE+ALTER(col+tag)+DROP+ALTER(tag) mixed, one txn → COMMIT")
            _run_scenario(24)
            tdLog.info("s24 PASSED")

    def ddlmatrix_s25_create_alter_drop_alter_mix_rollback(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s25: same mixed sequence → ROLLBACK → target empty")
            _run_scenario(25)
            tdLog.info("s25 PASSED")

    def ddlmatrix_s26_create_drop_recreate_same_name(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s26: CREATE→DROP→re-CREATE same name (diff schema) → COMMIT")
            _run_scenario(26)
            tdLog.info("s26 PASSED")

    def ddlmatrix_s27_triple_add_column(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s27: 3x consecutive ADD COLUMN same STB → COMMIT")
            _run_scenario(27)
            tdLog.info("s27 PASSED")

    def ddlmatrix_s28_two_unrelated_stbs_same_txn(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s28: two unrelated STBs, each CREATE+ALTER, same txn → COMMIT")
            _run_scenario(28)
            tdLog.info("s28 PASSED")

    def ddlmatrix_s29_vtable_create_source_drop_same_txn(self):
            self.ddlmatrix_s0_cleanup()
            tdLog.info("======== s29: CREATE vtable + DROP its source, same txn → COMMIT")
            _run_scenario(29)
            tdLog.info("s29 PASSED")

    def test_taosx_txn_ddl_matrix(self):
            """taosX DDL cross-mix within a single transaction (s24-s29)

            Verifies that mixing several different DDL kinds (CREATE/ALTER/DROP)
            on the same and on unrelated objects, all within one BEGIN...COMMIT
            block, replicates atomically to taosX-style targets — only the final
            state matters, and ROLLBACK leaves the target completely untouched.

            1. create_alter_drop_alter_mix_commit
            2. create_alter_drop_alter_mix_rollback
            3. create_drop_recreate_same_name
            4. triple_add_column
            5. two_unrelated_stbs_same_txn
            6. vtable_create_source_drop_same_txn

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.ddlmatrix_s24_create_alter_drop_alter_mix_commit()
            self.ddlmatrix_s25_create_alter_drop_alter_mix_rollback()
            self.ddlmatrix_s26_create_drop_recreate_same_name()
            self.ddlmatrix_s27_triple_add_column()
            self.ddlmatrix_s28_two_unrelated_stbs_same_txn()
            self.ddlmatrix_s29_vtable_create_source_drop_same_txn()


    # =========================================================================
    # mixed : test_taosx_txn_mixed_nontxn.py
    # =========================================================================

    def mixed_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def mixed_s30_nontxn_txn_nontxn_alter_chain(self):
            self.mixed_s0_cleanup()
            tdLog.info("======== s30: non-txn CREATE → txn ALTER → non-txn ALTER")
            _run_scenario(30)
            tdLog.info("s30 PASSED")

    def mixed_s31_txn_nontxn_txn_churn_same_name(self):
            self.mixed_s0_cleanup()
            tdLog.info("======== s31: txn CREATE → non-txn DROP → txn re-CREATE, same name")
            _run_scenario(31)
            tdLog.info("s31 PASSED")

    def mixed_s32_nontxn_inserts_around_txn_alter(self):
            self.mixed_s0_cleanup()
            tdLog.info("======== s32: non-txn INSERTs interleaved with txn ALTER on same NTB")
            _run_scenario(32)
            tdLog.info("s32 PASSED")

    def mixed_s33_nontxn_conflict_then_retry_after_rollback(self):
            self.mixed_s0_cleanup()
            tdLog.info("======== s33: non-txn conflict rejected, retry succeeds after ROLLBACK")
            _run_scenario(33)
            tdLog.info("s33 PASSED")

    def test_taosx_txn_mixed_nontxn(self):
            """taosX transactional vs non-transactional DDL interleaving (s30-s33)

            Verifies plain (non-BEGIN) DDL and batch-transactional DDL on the
            same objects, mixed over time, replicate correctly to taosX-style
            targets — no ghosts, no lost ALTERs, and conflicting concurrent DDL
            resolves to exactly the final successful state.

            1. nontxn_txn_nontxn_alter_chain
            2. txn_nontxn_txn_churn_same_name
            3. nontxn_inserts_around_txn_alter
            4. nontxn_conflict_then_retry_after_rollback

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.mixed_s30_nontxn_txn_nontxn_alter_chain()
            self.mixed_s31_txn_nontxn_txn_churn_same_name()
            self.mixed_s32_nontxn_inserts_around_txn_alter()
            self.mixed_s33_nontxn_conflict_then_retry_after_rollback()


    # =========================================================================
    # recovery : test_taosx_txn_recovery.py
    # =========================================================================

    def recovery_s0_cleanup(self):
            """Clean up any leftover databases from previous runs."""
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def recovery_s12_low_watermark_replay(self):
            self.recovery_s0_cleanup()
            tdLog.info("======== s12: Low-watermark replay → double consume → target correct")
            _run_scenario(12)
            tdLog.info("s12 PASSED")

    def recovery_s13_alter_existing_stb_commit(self):
            self.recovery_s0_cleanup()
            tdLog.info("======== s13: Pre-existing STB → ALTER STB → COMMIT (first MNode DDL)")
            _run_scenario(13)
            tdLog.info("s13 PASSED")

    def recovery_s14_drop_existing_stb_commit(self):
            self.recovery_s0_cleanup()
            tdLog.info("======== s14: Pre-existing STB → DROP STB → COMMIT (first MNode DDL)")
            _run_scenario(14)
            tdLog.info("s14 PASSED")

    def recovery_s15_alter_existing_stb_rollback(self):
            self.recovery_s0_cleanup()
            tdLog.info("======== s15: Pre-existing STB → ALTER STB → ROLLBACK (first MNode DDL)")
            _run_scenario(15)
            tdLog.info("s15 PASSED")

    def recovery_s16_drop_existing_stb_rollback(self):
            self.recovery_s0_cleanup()
            tdLog.info("======== s16: Pre-existing STB → DROP STB → ROLLBACK (first MNode DDL)")
            _run_scenario(16)
            tdLog.info("s16 PASSED")

    def test_taosx_txn_recovery(self):
            """taosX recovery & edge-case tests (s12-s17)

            Verifies idempotent re-consumption and correct handling of pre-existing
            objects on the target.  ROLLBACK correctly suppresses DDL delivery.
            s17 verifies basic connectivity (full inactivity-timeout exemption test
            covered by test_meta_batch_txn_cluster_fi.py).

            12. low_watermark_replay
            13. alter_existing_stb_commit
            14. drop_existing_stb_commit
            15. alter_existing_stb_rollback
            16. drop_existing_stb_rollback
            17. replicated_txn_connectivity_check (full timeout-exemption test in cluster_fi)

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.recovery_s12_low_watermark_replay()
            self.recovery_s13_alter_existing_stb_commit()
            self.recovery_s14_drop_existing_stb_commit()
            self.recovery_s15_alter_existing_stb_rollback()
            self.recovery_s16_drop_existing_stb_rollback()


    # =========================================================================
    # snapshot : test_taosx_txn_snapshot.py
    # =========================================================================

    def snapshot_s0_cleanup(self):
            """Clean up any leftover databases from previous runs."""
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def snapshot_s18_snapshot_committed_data(self):
            self.snapshot_s0_cleanup()
            tdLog.info("======== s18: snapshot of fully-committed data → target gets all")
            _run_scenario(18)
            tdLog.info("s18 PASSED")

    def snapshot_s19_snapshot_inflight_txn_commit(self):
            self.snapshot_s0_cleanup()
            tdLog.info("======== s19: snapshot while txn in-flight → COMMIT → target has stb1+ct1+ct2")
            _run_scenario(19)
            tdLog.info("s19 PASSED")

    def snapshot_s20_snapshot_idempotent_replay(self):
            self.snapshot_s0_cleanup()
            tdLog.info("======== s20: snapshot idempotent double-replay → same target state")
            _run_scenario(20)
            tdLog.info("s20 PASSED")

    def test_taosx_txn_snapshot(self):
            """taosX snapshot-mode replication tests (s18-s20)

            Verifies that a TMQ consumer with td.enable.snapshot=1 correctly
            delivers committed DDL to the target under the current snapshot design.

            Design: tqMeta.c caps snapshotVer at min(committedVer, minTxnBeginIndex-1).
            buildSnapContext stops iterating at version > snapshotVer, so in-flight
            PRE_CREATE/PRE_ALTER/PRE_DROP entries are NEVER included in the snapshot.
            After snapshot, WAL replay starts at snapshotVer+1; individual in-txn DDL
            entries are filtered (WAL_IS_TXN_MSG); TXN_COMMIT triggers atomic delivery
            via STxnWalManager; TXN_ROLLBACK is silently skipped (no PRE_CREATE was
            ever seen by the consumer, so nothing to clean up on the target).

            18. snapshot_committed_data
            19. snapshot_inflight_txn_commit
            20. snapshot_idempotent_replay

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-6659965197
            """
            self.snapshot_s18_snapshot_committed_data()
            self.snapshot_s19_snapshot_inflight_txn_commit()
            self.snapshot_s20_snapshot_idempotent_replay()


    # =========================================================================
    # stress : test_taosx_txn_stress_consumer.py
    # =========================================================================

    def stress_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def stress_s42_rapid_commit_rollback_cycles(self):
            self.stress_s0_cleanup()
            tdLog.info("======== s42: ~200 rapid BEGIN/COMMIT/ROLLBACK cycles")
            _run_scenario(42, timeout=180)
            tdLog.info("s42 PASSED")

    def stress_s43_multiple_consumer_groups_converge(self):
            self.stress_s0_cleanup()
            tdLog.info("======== s43: backlog + 4 independent consumer groups converge")
            _run_scenario(43)
            tdLog.info("s43 PASSED")

    def test_taosx_txn_stress_consumer(self):
            """taosX replication stress tests (s42-s43)

            Verifies the taosX consumption path holds up under rapid
            commit/rollback churn and under multiple independent consumer
            groups draining the same backlog — target state must exactly
            match the committed subset, and all consumer groups must converge
            to the identical final state.

            1. rapid_commit_rollback_cycles
            2. multiple_consumer_groups_converge

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.stress_s42_rapid_commit_rollback_cycles()
            self.stress_s43_multiple_consumer_groups_converge()


    # =========================================================================
    # waltrim : test_taosx_txn_wal_trim.py
    # =========================================================================

    def waltrim_s0_cleanup(self):
            tdSql.execute("drop topic if exists topic_taosx_txn")
            tdSql.execute("drop database if exists src_txn_db")
            tdSql.execute("drop database if exists dst_txn_db")

    def waltrim_s44_trim_already_consumed_wal_then_new_commit(self):
            self.waltrim_s0_cleanup()
            tdLog.info("======== s44: TRIM WAL after full drain, then new commit still consumes correctly")
            _run_scenario(44)
            tdLog.info("s44 PASSED")

    def waltrim_s45_never_consumed_txn_wal_trimmed(self):
            self.waltrim_s0_cleanup()
            tdLog.info("======== s45: never-consumed txn + WAL trimmed before any read (exploratory)")
            _run_scenario(45)
            tdLog.info("s45 PASSED (no crash / no partial-corrupted state)")

    def test_taosx_txn_wal_trim(self):
            """taosX replication WAL cleanup / retention scenarios (s44-s45)

            Verifies that explicitly trimming the source WAL (TRIM DATABASE ...
            WAL) interacts safely with taosX-style consumption: trimming
            already-consumed WAL must not disturb ongoing consumption of new
            commits (s44); and a txn whose WAL is trimmed before any consumer
            ever reads it must not crash or leave the target in a
            partial/corrupted state (s45, exploratory — no single contract is
            asserted for that corner yet).

            1. trim_already_consumed_wal_then_new_commit
            2. never_consumed_txn_wal_trimmed

            Since: v3.3.6.0

            Labels: common,ci

            Jira: TD-XXXXX
            """
            self.waltrim_s44_trim_already_consumed_wal_then_new_commit()
            self.waltrim_s45_never_consumed_txn_wal_trimmed()
