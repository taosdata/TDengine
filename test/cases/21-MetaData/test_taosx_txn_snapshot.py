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

"""Target-side (taosX) replication: snapshot-mode tests (s18-s20).

Design (current implementation):
  tqMeta.c caps snapshotVer = min(committedVer, minTxnBeginIndex-1).
  buildSnapContext stops iterating at version > snapshotVer, so in-flight
  PRE_CREATE/PRE_ALTER/PRE_DROP entries are NEVER included in the meta snapshot.
  Consumers always receive a "clean" snapshot containing only NORMAL entries.
  After snapshot, WAL replay starts at snapshotVer+1:
    - Individual in-txn DDL entries are filtered (WAL_IS_TXN_MSG skip).
    - TXN_COMMIT triggers atomic delivery via STxnWalManager.
    - TXN_ROLLBACK is silently skipped (no PRE_CREATE was ever seen by the
      consumer, so nothing to clean up on the target).

Scenarios:
  18. Snapshot + all data committed before subscribe → target gets everything.
  19. Snapshot while txn in-flight → snapshot capped before txn start →
      delivers only committed (stb1); WAL replay atomically delivers ct1+ct2
      on COMMIT.  Final target: stb1 + ct1 + ct2.
  20. Snapshot idempotent double-replay → two consumer groups, same result.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import subprocess
import os
import time


# Path to the C test binary
TMQ_TAOSX_TXN_BIN = None

def _find_binary():
    """Find the tmq_taosx_txn binary in builddir or compile it."""
    global TMQ_TAOSX_TXN_BIN
    if TMQ_TAOSX_TXN_BIN is not None:
        return TMQ_TAOSX_TXN_BIN

    # Search common locations; derive root from this file's location
    # __file__ is .../community/test/cases/21-MetaData/test_taosx_txn_snapshot.py
    # 5 levels up reaches the TDinternal repo root (file is under source/taos-community/test/cases/21-MetaData/)
    _root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../../"))
    search_paths = [
        os.path.join(_root, "debug/build/bin/tmq_taosx_txn"),
        os.path.join(os.environ.get("TDENGINE_DIR", ""), "debug/build/bin/tmq_taosx_txn"),
    ]
    for p in search_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            TMQ_TAOSX_TXN_BIN = p
            return p

    # Try to compile in-place; suppress ASAN leak detection so gcc exits cleanly
    src = os.path.join(os.path.dirname(__file__), "../../../utils/test/c/tmq_taosx_txn.c")
    src = os.path.normpath(src)
    if not os.path.isfile(src):
        raise RuntimeError("Cannot find tmq_taosx_txn.c source: %s" % src)
    dst = "/tmp/tmq_taosx_txn"
    cmd = [
        "gcc", "-o", dst, src,
        "-I/usr/local/taos/include", "-L/usr/lib", "-ltaos", "-lpthread", "-lm"
    ]
    compile_env = os.environ.copy()
    compile_env["ASAN_OPTIONS"] = compile_env.get("ASAN_OPTIONS", "").replace("detect_leaks=1", "") + ":detect_leaks=0"
    ret = subprocess.run(cmd, capture_output=True, text=True, env=compile_env)
    if ret.returncode != 0 and not os.path.isfile(dst):
        raise RuntimeError("Failed to compile tmq_taosx_txn: %s" % ret.stderr)
    TMQ_TAOSX_TXN_BIN = dst
    return dst


def _run_scenario(scenario, expect_pass=True):
    """Run a tmq_taosx_txn scenario and check result."""
    binary = _find_binary()
    tdLog.info("Running tmq_taosx_txn scenario %d (%s)" % (scenario, binary))
    build_lib = os.path.normpath(os.path.join(os.path.dirname(binary), "../lib"))
    lib_path = (build_lib + ":") if os.path.isdir(build_lib) else ""
    lib_path += "/usr/lib:/usr/local/taos/driver"
    env = {**os.environ, "LD_LIBRARY_PATH": lib_path}
    # Keep LD_PRELOAD if it contains ASAN runtime (needed for instrumented binaries)
    ld_preload = env.get("LD_PRELOAD", "")
    if "libasan" not in ld_preload:
        env.pop("LD_PRELOAD", None)
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, timeout=180,
        env=env
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


class TestTaosxTxnSnapshot:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        """Clean up any leftover databases from previous runs."""
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    # =========================================================================
    # s18: Snapshot mode — all data committed before subscribe → target gets all
    #
    # All DDL (stb1 + ct1 + ct2) is committed to the source database before the
    # consumer subscribes.  The consumer uses snapshot mode (td.enable.snapshot=1).
    # Because all transactions are fully committed and vacuumed, minTxnBeginIndex
    # does not constrain snapshotVer; the snapshot delivers everything.
    # Expected result: target has stb1 + ct1 + ct2.
    # =========================================================================

    def s18_snapshot_committed_data(self):
        self.s0_cleanup()
        tdLog.info("======== s18: snapshot of fully-committed data → target gets all")
        _run_scenario(18)
        tdLog.info("s18 PASSED")

    # =========================================================================
    # s19: Snapshot mode while txn in-flight → COMMIT → target gets full state
    #
    # stb1 is committed outside any transaction.  Then a BEGIN…CREATE ct1+ct2
    # transaction is opened but NOT yet committed.  The consumer subscribes in
    # snapshot mode while ct1 and ct2 are still PRE_CREATE (in-flight).
    #
    # snapshotVer = min(committedVer, minTxnBeginIndex-1) ensures the snapshot
    # stops before the in-flight transaction's first WAL entry.  The snapshot
    # therefore delivers only stb1 (NORMAL); ct1 and ct2 are excluded.
    #
    # After the consumer subscribes, the source commits the transaction.  WAL
    # replay from snapshotVer+1 filters individual in-txn DDL (WAL_IS_TXN_MSG),
    # then delivers ct1+ct2 atomically when TXN_COMMIT is encountered via
    # STxnWalManager.
    #
    # Expected result: target has stb1 + ct1 + ct2.
    # =========================================================================

    def s19_snapshot_inflight_txn_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s19: snapshot while txn in-flight → COMMIT → target has stb1+ct1+ct2")
        _run_scenario(19)
        tdLog.info("s19 PASSED")

    # =========================================================================
    # s20: Snapshot idempotent double-replay → same target state
    #
    # Two consumer groups (both with td.enable.snapshot=1) consume the same
    # topic from earliest offset.  The second group re-applies all messages;
    # TABLE_ALREADY_EXIST errors are tolerated (idempotent replay).
    # Expected result: target state is identical after both groups finish.
    # =========================================================================

    def s20_snapshot_idempotent_replay(self):
        self.s0_cleanup()
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
        self.s18_snapshot_committed_data()
        self.s19_snapshot_inflight_txn_commit()
        self.s20_snapshot_idempotent_replay()
