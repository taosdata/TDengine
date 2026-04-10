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

"""
Target-side (taosX replication) tests for Batch Metadata Transaction.

Tests verify that transactions replicated through TMQ (tmq_get_raw/tmq_write_raw)
correctly handle STB DDL on the target MNode, including:
  - CREATE STB + child tables → COMMIT → target has all objects
  - CREATE STB + child tables → ROLLBACK → target has nothing
  - CREATE STB → ALTER STB → COMMIT → target has altered schema
  - CREATE STB → DROP STB → COMMIT → target has no STB

The C binary tmq_taosx_txn is used to perform actual TMQ replication since
the Python connector does not expose tmq_get_raw/tmq_write_raw.
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

    # Search common locations
    search_paths = [
        os.path.join(os.environ.get("TDENGINE_DIR", ""), "debug/build/bin/tmq_taosx_txn"),
        "/proj/github/3.ims/TDinternal/debug/build/bin/tmq_taosx_txn",
    ]
    for p in search_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            TMQ_TAOSX_TXN_BIN = p
            return p

    # Try to compile in-place
    src = os.path.join(os.path.dirname(__file__), "../../../utils/test/c/tmq_taosx_txn.c")
    src = os.path.normpath(src)
    if not os.path.isfile(src):
        raise RuntimeError("Cannot find tmq_taosx_txn.c source: %s" % src)
    dst = "/tmp/tmq_taosx_txn"
    cmd = [
        "gcc", "-o", dst, src,
        "-I/usr/local/taos/include", "-L/usr/lib", "-ltaos", "-lpthread", "-lm"
    ]
    ret = subprocess.run(cmd, capture_output=True, text=True)
    if ret.returncode != 0:
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


class TestTaosxTxn:

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
    # Entry point
    # =========================================================================
    def test_taosx_txn(self):
        """taosX target-side STB transaction replication via TMQ

        1. CREATE STB + child tables → COMMIT → target verified
        2. CREATE STB + child tables → ROLLBACK → target empty
        3. CREATE STB → ALTER STB → COMMIT → target has altered schema
        4. CREATE STB → DROP STB → COMMIT → target no STB
        5. Idempotent COMMIT replay
        12. Low-watermark replay (crash recovery)
        13. Pre-existing STB → ALTER STB → COMMIT (first MNode DDL = ALTER)
        14. Pre-existing STB → DROP STB → COMMIT (first MNode DDL = DROP)

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-04-01 Created — §35 target-side TMQ replication tests
            - 2026-04-02 Added s12 low-watermark replay test
            - 2026-04-10 Added s13-s14 first-MNode-DDL ALTER/DROP STB tests

        """
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
        self.s12_low_watermark_replay()
        self.s13_alter_existing_stb_commit()
        self.s14_drop_existing_stb_commit()
