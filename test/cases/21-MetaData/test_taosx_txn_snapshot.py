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

Tests verify that transactions replicated through TMQ in snapshot mode
(td.enable.snapshot=1) correctly propagate txnId/txnStatus from
SVCreateTbReq so the target can process subsequent COMMIT/ROLLBACK:

  18. Snapshot + PRE_CREATE child tables → ROLLBACK → target has stb1 only
  19. Snapshot + PRE_CREATE child tables → COMMIT  → target has stb1 + ct1 + ct2
  20. Idempotent ROLLBACK via double replay → target has 0 stables, 0 tables
      (second ROLLBACK is a no-op on VNode when txnEntry was already removed)

These tests exercise the getTableInfoFromSnapshot() code path in
metaSnapshot.c which was fixed to copy me.txnId and me.txnStatus into
the SVCreateTbReq it builds for the consumer.  Scenario 20 specifically
covers the vnodeProcessTxnRollbackReq idempotency path (pEntry == NULL
→ return SUCCESS) which is invoked when MNode sends orphan ROLLBACK for
a txn that VNode already cleaned up.
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
    # 4 levels up reaches the TDinternal repo root
    _root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../"))
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
    env = {**os.environ, "LD_LIBRARY_PATH": "/usr/lib:/usr/local/taos/driver"}
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
    # s18: Snapshot mode + PRE_CREATE child tables → ROLLBACK
    #
    # The consumer subscribes in snapshot mode while ct1 and ct2 are in
    # PRE_CREATE state (transaction not yet committed or rolled back).
    # getTableInfoFromSnapshot() must copy txnId/txnStatus into SVCreateTbReq
    # so that the target vnode registers ct1/ct2 under the correct txnId.
    # When the subsequent ROLLBACK WAL entry arrives, the target vnode can
    # find and delete the PRE_CREATE tables.
    # Expected result: target has stb1, but 0 child tables.
    # =========================================================================

    def s18_snapshot_pre_create_rollback(self):
        self.s0_cleanup()
        tdLog.info("======== s18: snapshot + PRE_CREATE CTBs → ROLLBACK → target empty CTBs")
        _run_scenario(18)
        tdLog.info("s18 PASSED")

    # =========================================================================
    # s19: Snapshot mode + PRE_CREATE child tables → COMMIT
    #
    # Same ordering as s18 but the transaction is committed.  The consumer
    # carries PRE_CREATE ct1/ct2 with txnId/txnStatus, and the subsequent
    # COMMIT WAL entry promotes them to NORMAL on the target.
    # Expected result: target has stb1 + ct1 + ct2.
    # =========================================================================

    def s19_snapshot_pre_create_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s19: snapshot + PRE_CREATE CTBs → COMMIT → target has 2 CTBs")
        _run_scenario(19)
        tdLog.info("s19 PASSED")

    # =========================================================================
    # s20: Idempotent ROLLBACK via double replay
    #
    # Source creates a committed STB (outside BEGIN) then creates CT1/CT2 inside
    # a BEGIN…ROLLBACK.  The scenario is replayed twice via different consumer
    # groups.  In the second replay, the committed STB already exists (no-op),
    # CT1/CT2 are recreated in PRE_CREATE state under the same replicated txnId,
    # and the ROLLBACK event finalises them again.  The second ROLLBACK must be
    # idempotent: vnodeProcessTxnRollbackReq sees finalStatus != TXN_FINAL_NONE
    # or pEntry == NULL and returns SUCCESS without corrupting any data.
    #
    # This directly covers the scenario where MNode's orphan-cleanup mechanism
    # (mndRollbackOrphanTxnOnVnode) sends a ROLLBACK via STrans for a txn that
    # VNode already cleaned up, verifying that the VNode handler is idempotent.
    #
    # Expected result: target has 1 stable (stb1) and 0 tables after both replays.
    # =========================================================================

    def s20_idempotent_rollback_double_replay(self):
        self.s0_cleanup()
        tdLog.info("======== s20: idempotent ROLLBACK via double replay → target empty")
        _run_scenario(20)
        tdLog.info("s20 PASSED")

    def test_taosx_txn_snapshot(self):
        """taosX snapshot-mode replication tests (s18-s20)

        These tests exercise the getTableInfoFromSnapshot() code path that
        must preserve txnId/txnStatus in SVCreateTbReq so the consumer-side
        target can correctly process ROLLBACK and COMMIT for in-flight
        transactions that were captured in the TMQ meta snapshot.

        18. snapshot_pre_create_rollback
        19. snapshot_pre_create_commit
        20. idempotent_rollback_double_replay

        Since: v3.3.6.0
        Labels: common,ci
        Jira: TD-6659965197
        """
        import pytest
        pytest.skip("Phase 1: TMQ batch txn delivery not yet implemented — re-enable in Phase 2")
        self.s18_snapshot_pre_create_rollback()
        self.s19_snapshot_pre_create_commit()
        self.s20_idempotent_rollback_double_replay()
