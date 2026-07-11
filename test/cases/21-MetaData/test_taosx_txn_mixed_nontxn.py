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

"""Target-side (taosX) replication: transactional vs non-transactional DDL
interleaving (s30-s33).

Real usage mixes plain (non-BEGIN) DDL with batch-transactional DDL on the
same objects over time. These tests verify taosX replication stays correct
across that churn:
  30. non-txn CREATE → txn ALTER (COMMIT) → non-txn ALTER (no BEGIN)
  31. txn CREATE (COMMIT) → non-txn DROP → txn re-CREATE (COMMIT), same name
  32. non-txn INSERTs interleaved with a txn ALTER on the same NTB
  33. non-txn session's conflicting DDL rejected while a txn holds a table;
      retry succeeds after ROLLBACK
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


class TestTaosxTxnMixedNontxn:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    def s30_nontxn_txn_nontxn_alter_chain(self):
        self.s0_cleanup()
        tdLog.info("======== s30: non-txn CREATE → txn ALTER → non-txn ALTER")
        _run_scenario(30)
        tdLog.info("s30 PASSED")

    def s31_txn_nontxn_txn_churn_same_name(self):
        self.s0_cleanup()
        tdLog.info("======== s31: txn CREATE → non-txn DROP → txn re-CREATE, same name")
        _run_scenario(31)
        tdLog.info("s31 PASSED")

    def s32_nontxn_inserts_around_txn_alter(self):
        self.s0_cleanup()
        tdLog.info("======== s32: non-txn INSERTs interleaved with txn ALTER on same NTB")
        _run_scenario(32)
        tdLog.info("s32 PASSED")

    def s33_nontxn_conflict_then_retry_after_rollback(self):
        self.s0_cleanup()
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
        self.s30_nontxn_txn_nontxn_alter_chain()
        self.s31_txn_nontxn_txn_churn_same_name()
        self.s32_nontxn_inserts_around_txn_alter()
        self.s33_nontxn_conflict_then_retry_after_rollback()
