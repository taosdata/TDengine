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

"""Target-side (taosX) replication: boundary tests (s38-s41).

Covers edge cases at the extremes of transaction size/duration:
  38. Empty transaction (BEGIN; COMMIT, zero DDL ops) — no crash, no phantom messages
  39. Minimal transaction (single DDL op) — baseline distinct from no-txn-at-all
  40. Very large single transaction (1200 CTBs) — full atomic replication
  41. Long-wall-clock-window transaction with unrelated concurrent WAL traffic
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


def _run_scenario(scenario, expect_pass=True, timeout=120):
    """Run a tmq_taosx_txn scenario and check result."""
    binary = _find_binary()
    tdLog.info("Running tmq_taosx_txn scenario %d (%s)" % (scenario, binary))
    build_lib = os.path.normpath(os.path.join(os.path.dirname(binary), "../lib"))
    lib_path = (build_lib + ":") if os.path.isdir(build_lib) else ""
    lib_path += "/usr/lib:/usr/local/taos/driver"
    ret = subprocess.run(
        [binary, str(scenario)],
        capture_output=True, text=True, timeout=timeout,
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


class TestTaosxTxnBoundary:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    def s38_empty_txn(self):
        self.s0_cleanup()
        tdLog.info("======== s38: empty txn (BEGIN; COMMIT, no ops)")
        _run_scenario(38)
        tdLog.info("s38 PASSED")

    def s39_minimal_txn(self):
        self.s0_cleanup()
        tdLog.info("======== s39: minimal txn (single DDL op)")
        _run_scenario(39)
        tdLog.info("s39 PASSED")

    def s40_bulk_1200_ctb_single_txn(self):
        self.s0_cleanup()
        tdLog.info("======== s40: bulk single txn — 1200 CTBs → COMMIT")
        _run_scenario(40, timeout=180)
        tdLog.info("s40 PASSED")

    def s41_long_window_txn_with_wal_noise(self):
        self.s0_cleanup()
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
        self.s38_empty_txn()
        self.s39_minimal_txn()
        self.s40_bulk_1200_ctb_single_txn()
        self.s41_long_window_txn_with_wal_noise()
