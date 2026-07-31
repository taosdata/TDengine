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

"""Target-side (taosX) replication: CREATE/DROP/ALTER cross-mix within a
single transaction (s24-s29).

Tests verify that a single BEGIN...COMMIT block mixing several different
DDL kinds on the SAME and on UNRELATED objects replicates atomically and
correctly to the target — only the final state matters, intermediate DDL
must not be replayed as independent events, and ROLLBACK must leave the
target completely untouched:
  24. CREATE STB+2CTB, ALTER(col+tag), DROP one CTB, ALTER remaining tag → COMMIT
  25. Same mixed sequence → ROLLBACK → target empty
  26. CREATE → DROP → re-CREATE same name (different schema) → COMMIT
  27. 3x consecutive ADD COLUMN on same STB → COMMIT
  28. Two unrelated STBs, each CREATE+ALTER, same txn → COMMIT
  29. CREATE vtable + DROP its source table, same txn → COMMIT (orphan replication)
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


class TestTaosxTxnDdlMatrix:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_cleanup(self):
        tdSql.execute("drop topic if exists topic_taosx_txn")
        tdSql.execute("drop database if exists src_txn_db")
        tdSql.execute("drop database if exists dst_txn_db")

    def s24_create_alter_drop_alter_mix_commit(self):
        self.s0_cleanup()
        tdLog.info("======== s24: CREATE+ALTER(col+tag)+DROP+ALTER(tag) mixed, one txn → COMMIT")
        _run_scenario(24)
        tdLog.info("s24 PASSED")

    def s25_create_alter_drop_alter_mix_rollback(self):
        self.s0_cleanup()
        tdLog.info("======== s25: same mixed sequence → ROLLBACK → target empty")
        _run_scenario(25)
        tdLog.info("s25 PASSED")

    def s26_create_drop_recreate_same_name(self):
        self.s0_cleanup()
        tdLog.info("======== s26: CREATE→DROP→re-CREATE same name (diff schema) → COMMIT")
        _run_scenario(26)
        tdLog.info("s26 PASSED")

    def s27_triple_add_column(self):
        self.s0_cleanup()
        tdLog.info("======== s27: 3x consecutive ADD COLUMN same STB → COMMIT")
        _run_scenario(27)
        tdLog.info("s27 PASSED")

    def s28_two_unrelated_stbs_same_txn(self):
        self.s0_cleanup()
        tdLog.info("======== s28: two unrelated STBs, each CREATE+ALTER, same txn → COMMIT")
        _run_scenario(28)
        tdLog.info("s28 PASSED")

    def s29_vtable_create_source_drop_same_txn(self):
        self.s0_cleanup()
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
        self.s24_create_alter_drop_alter_mix_commit()
        self.s25_create_alter_drop_alter_mix_rollback()
        self.s26_create_drop_recreate_same_name()
        self.s27_triple_add_column()
        self.s28_two_unrelated_stbs_same_txn()
        self.s29_vtable_create_source_drop_same_txn()
