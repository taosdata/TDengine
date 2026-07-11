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

"""Target-side (taosX) replication: multiple concurrent transactional and
non-transactional sessions interleaved (s34-s37).

Exercises two independent TAOS sessions (g_src / g_src2 in the C harness)
running BEGIN/COMMIT/ROLLBACK on unrelated or conflicting objects at the
same time, verifying the replicated target reflects the actual WAL commit
order and never corrupts/duplicates state:
  34. two independent txns (2 sessions), COMMIT in reverse BEGIN order
  35. txn A (commit) + non-txn B + txn C (rollback), 3-way interleave
  36. two sessions BEGIN on the same STB; second blocked, retries after first COMMITs
  37. round-robin independent BEGIN/COMMIT across 2 sessions, 5 rounds each
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import subprocess
import os
import time


TMQ_TAOSX_TXN_BIN = None


def _find_binary():
    """Find the tmq_taosx_txn binary in builddir or compile it."""
    global TMQ_TAOSX_TXN_BIN
    if TMQ_TAOSX_TXN_BIN is not None:
        return TMQ_TAOSX_TXN_BIN

    _root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../../"))
    search_paths = [
        os.path.join(_root, "debug/build/bin/tmq_taosx_txn"),
        os.path.join(os.environ.get("TDENGINE_DIR", ""), "debug/build/bin/tmq_taosx_txn"),
    ]
    for p in search_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            TMQ_TAOSX_TXN_BIN = p
            return p

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
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, timeout=120,
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


class TestTaosxTxnConcurrentSessions:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    def s34_two_independent_txns_reverse_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s34: two independent txns, reverse commit order")
        _run_scenario(34)
        tdLog.info("s34 PASSED")

    def s35_txn_nontxn_txn_three_way_interleave(self):
        self.s0_cleanup()
        tdLog.info("======== s35: txn A(commit) + non-txn B + txn C(rollback)")
        _run_scenario(35)
        tdLog.info("s35 PASSED")

    def s36_two_sessions_same_table_blocked_then_retry(self):
        self.s0_cleanup()
        tdLog.info("======== s36: two sessions BEGIN same STB, second retries after first COMMIT")
        _run_scenario(36)
        tdLog.info("s36 PASSED")

    def s37_round_robin_two_sessions(self):
        self.s0_cleanup()
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
        self.s34_two_independent_txns_reverse_commit()
        self.s35_txn_nontxn_txn_three_way_interleave()
        self.s36_two_sessions_same_table_blocked_then_retry()
        self.s37_round_robin_two_sessions()
