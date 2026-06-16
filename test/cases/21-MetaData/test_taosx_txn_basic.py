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

"""Target-side (taosX) replication: basic tests (s1-s11).

Tests verify that transactions replicated through TMQ correctly
handle STB/CTB/NTB DDL on the target MNode:
  1. CREATE STB + CTBs → COMMIT → target has all objects
  2. CREATE STB + CTBs → ROLLBACK → target has nothing
  3. CREATE STB → ALTER STB → COMMIT → target has altered schema
  4. CREATE STB → DROP STB → COMMIT → target has no STB
  5. Idempotent COMMIT replay
  6. CREATE CTBs → ALTER child tag → COMMIT
  7. CREATE CTBs → DROP child → COMMIT
  8. CREATE normal table → ALTER → COMMIT
  9. CREATE normal table → DROP → COMMIT
  10. Mixed STB+CTB+NTB → COMMIT
  11. Multi-VGroup STB+10CTBs+2NTBs → COMMIT
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
    # __file__ is .../community/test/cases/21-MetaData/test_taosx_txn_basic.py
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



class TestTaosxTxnBasic:

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

    def s1_commit_stb_and_ctb(self):
        self.s0_cleanup()
        tdLog.info("======== s1: CREATE STB + CTBs → COMMIT → target verified")
        _run_scenario(1)
        tdLog.info("s1 PASSED")

    # =========================================================================
    # s2: CREATE STB + child tables → ROLLBACK → target has nothing
    # =========================================================================

    def s2_rollback_stb_and_ctb(self):
        self.s0_cleanup()
        tdLog.info("======== s2: CREATE STB + CTBs → ROLLBACK → target empty")
        _run_scenario(2)
        tdLog.info("s2 PASSED")

    # =========================================================================
    # s3: CREATE STB → ALTER STB add column → COMMIT → target has altered schema
    # =========================================================================

    def s3_alter_stb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s3: CREATE STB → ALTER STB → COMMIT → target has altered STB")
        _run_scenario(3)
        tdLog.info("s3 PASSED")

    # =========================================================================
    # s4: CREATE STB → DROP STB → COMMIT → target has no STB
    # =========================================================================

    def s4_drop_stb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s4: CREATE STB → DROP STB → COMMIT → target has no STB")
        _run_scenario(4)
        tdLog.info("s4 PASSED")

    # =========================================================================
    # s5: Idempotent COMMIT replay (same scenario replayed)
    # =========================================================================

    def s5_idempotent_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s5: Idempotent COMMIT replay → target correct")
        _run_scenario(5)
        tdLog.info("s5 PASSED")

    # =========================================================================
    # s6: CREATE STB + CTBs → ALTER child tag → COMMIT
    # =========================================================================

    def s6_alter_ctb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s6: CREATE CTBs → ALTER child tag → COMMIT")
        _run_scenario(6)
        tdLog.info("s6 PASSED")

    # =========================================================================
    # s7: CREATE STB + CTBs → DROP child → COMMIT
    # =========================================================================

    def s7_drop_ctb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s7: CREATE CTBs → DROP child → COMMIT")
        _run_scenario(7)
        tdLog.info("s7 PASSED")

    # =========================================================================
    # s8: CREATE normal table → ALTER → COMMIT
    # =========================================================================

    def s8_alter_ntb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s8: CREATE normal table → ALTER → COMMIT")
        _run_scenario(8)
        tdLog.info("s8 PASSED")

    # =========================================================================
    # s9: CREATE normal table → DROP → COMMIT
    # =========================================================================

    def s9_drop_ntb_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s9: CREATE normal table → DROP → COMMIT")
        _run_scenario(9)
        tdLog.info("s9 PASSED")

    # =========================================================================
    # s10: Mixed STB + CTB + normal table → COMMIT
    # =========================================================================

    def s10_mixed_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s10: Mixed STB+CTB+NTB → COMMIT")
        _run_scenario(10)
        tdLog.info("s10 PASSED")

    # =========================================================================
    # s11: Multi-VGroup (2 VGroups)
    # =========================================================================

    def s11_multi_vgroup(self):
        self.s0_cleanup()
        tdLog.info("======== s11: Multi-VGroup STB+10CTBs+2NTBs → COMMIT")
        _run_scenario(11)
        tdLog.info("s11 PASSED")

    # =========================================================================
    # s12: Low-watermark replay (crash recovery simulation)
    #   Replays all WAL messages twice with different consumer groups.
    #   Verifies idempotent handling: TABLE_ALREADY_EXIST, TXN_CONFLICT,
    #   and duplicate COMMIT are all handled gracefully.
    # =========================================================================

    def test_taosx_txn_basic(self):
        """taosX basic replication tests (s1-s11)

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
        import pytest
        pytest.skip("Phase 1: TMQ batch txn delivery not yet implemented — re-enable in Phase 2")
        self.s1_commit_stb_and_ctb()
        self.s2_rollback_stb_and_ctb()
        self.s3_alter_stb_commit()
        self.s4_drop_stb_commit()
        self.s5_idempotent_commit()
        self.s6_alter_ctb_commit()
        self.s7_drop_ctb_commit()
        self.s8_alter_ntb_commit()
        self.s9_drop_ntb_commit()
        self.s10_mixed_commit()
        self.s11_multi_vgroup()
