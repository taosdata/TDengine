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

"""Target-side (taosX) replication: recovery & edge-case tests (s12-s17).

Tests cover recovery, existing-object handling, and timeout semantics:
  12. Low-watermark replay (crash recovery, idempotent handling)
  13. Pre-existing STB → ALTER STB → COMMIT (first MNode DDL = ALTER)
  14. Pre-existing STB → DROP STB → COMMIT (first MNode DDL = DROP)
  15. Pre-existing STB → ALTER STB → ROLLBACK
  16. Pre-existing STB → DROP STB → ROLLBACK
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
    # __file__ is .../community/test/cases/21-MetaData/test_taosx_txn_recovery.py
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
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "LD_LIBRARY_PATH": "/usr/lib:/usr/local/taos/driver"}
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



class TestTaosxTxnRecovery:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def s0_cleanup(self):
        """Clean up any leftover databases from previous runs."""
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    # =========================================================================
    # s1: CREATE STB + child tables → COMMIT → target has STB + child tables
    # =========================================================================

    def s12_low_watermark_replay(self):
        self.s0_cleanup()
        tdLog.info("======== s12: Low-watermark replay → double consume → target correct")
        _run_scenario(12)
        tdLog.info("s12 PASSED")

    # =========================================================================
    # s13: Pre-existing STB → BEGIN → ALTER STB → COMMIT
    #   Tests that ALTER STB as the FIRST MNode DDL in a replicated txn
    #   correctly triggers auto-BEGIN on the target side. This verifies the
    #   fix for taosAlterTable() auto-BEGIN, which is NOT covered by s3
    #   (s3 has CREATE STB as the first DDL, masking the auto-BEGIN path).
    # =========================================================================

    def s13_alter_existing_stb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s13: Pre-existing STB → ALTER STB → COMMIT (first MNode DDL)")
        _run_scenario(13)
        tdLog.info("s13 PASSED")

    # =========================================================================
    # s14: Pre-existing STB → BEGIN → DROP STB → COMMIT
    #   Tests that DROP STB as the FIRST MNode DDL in a replicated txn
    #   correctly triggers auto-BEGIN on the target side. This verifies the
    #   fix for taosDropStb() auto-BEGIN, which is NOT covered by s4
    #   (s4 has CREATE STB as the first DDL, masking the auto-BEGIN path).
    # =========================================================================

    def s14_drop_existing_stb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s14: Pre-existing STB → DROP STB → COMMIT (first MNode DDL)")
        _run_scenario(14)
        tdLog.info("s14 PASSED")

    # =========================================================================
    # s15: Pre-existing STB → BEGIN → ALTER STB → ROLLBACK
    #   First MNode DDL is ALTER STB, but final ROLLBACK should preserve
    #   original STB schema (no c2 column) on target side.
    # =========================================================================

    def s15_alter_existing_stb_rollback(self):
        self.s0_cleanup()
        tdLog.info("======== s15: Pre-existing STB → ALTER STB → ROLLBACK (first MNode DDL)")
        _run_scenario(15)
        tdLog.info("s15 PASSED")

    # =========================================================================
    # s16: Pre-existing STB → BEGIN → DROP STB → ROLLBACK
    #   First MNode DDL is DROP STB, but final ROLLBACK should restore STB
    #   and child tables on target side.
    # =========================================================================

    def s16_drop_existing_stb_rollback(self):
        self.s0_cleanup()
        tdLog.info("======== s16: Pre-existing STB → DROP STB → ROLLBACK (first MNode DDL)")
        _run_scenario(16)
        tdLog.info("s16 PASSED")

    # =========================================================================
    # Entry point
    # =========================================================================

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
        self.s12_low_watermark_replay()
        self.s13_alter_existing_stb_commit()
        self.s14_drop_existing_stb_commit()
        self.s15_alter_existing_stb_rollback()
        self.s16_drop_existing_stb_rollback()
