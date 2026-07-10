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

"""Target-side (taosX) replication: WAL cleanup / retention scenarios (s44-s45).

  44. Commit + fully drain a consumer group, THEN explicitly TRIM the WAL,
      THEN commit new data — the SAME consumer group must keep consuming
      correctly (trimming already-consumed WAL must not disturb ongoing
      consumption of new commits).
  45. EXPLORATORY: a txn commits but is never read by any consumer before
      its WAL is explicitly trimmed out from under it; a brand-new consumer
      group then tries to bootstrap. There's no single documented contract
      for this exact corner, so this scenario accepts either a full
      recovery or a clean failure — it only rejects a crash or a
      partial/corrupted target state, which would indicate silent data
      corruption.
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


class TestTaosxTxnWalTrim:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    def s44_trim_already_consumed_wal_then_new_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s44: TRIM WAL after full drain, then new commit still consumes correctly")
        _run_scenario(44)
        tdLog.info("s44 PASSED")

    def s45_never_consumed_txn_wal_trimmed(self):
        self.s0_cleanup()
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
        self.s44_trim_already_consumed_wal_then_new_commit()
        self.s45_never_consumed_txn_wal_trimmed()
