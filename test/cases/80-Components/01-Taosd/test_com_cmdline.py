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

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import os
import shlex
import subprocess


class TestComCmdLine:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.tmpdir = "tmp"

    def _get_taosd_bin(self):
        candidates = []
        if tdDnodes.binPath:
            candidates.append(tdDnodes.binPath)

        taosd_bin = os.getenv("TAOSD_BIN")
        if taosd_bin:
            candidates.append(taosd_bin)

        taos_bin_path = os.getenv("TAOS_BIN_PATH")
        if taos_bin_path:
            candidates.append(os.path.join(taos_bin_path, "taosd"))

        for bin_path in candidates:
            if os.path.isfile(bin_path) and os.access(bin_path, os.X_OK):
                tdDnodes.binPath = bin_path
                tdLog.info("taosd found in %s" % bin_path)
                return bin_path

        tdLog.exit(
            "taosd not found! set TAOSD_BIN or TAOS_BIN_PATH when running this case standalone."
        )

    def _run_taosd(self, args):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path] + shlex.split(args)
        tdLog.info("run cmd: %s" % " ".join(cmd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", env=env
        )
        output = proc.stdout or ""
        tdLog.info("ret=%s output=%s" % (proc.returncode, output[:500].replace("\n", "\\n")))
        return proc.returncode, output

    def _assert_taosd_case(self, name, args, expected_code, expected_text):
        code, output = self._run_taosd(args)
        tdLog.info("verify case=%s" % name)
        tdSql.checkEqual(code, expected_code)
        tdSql.checkEqual(expected_text in output, True)

    def test_dumpsdb(self):
        """Taosd command line
        
        1. Verify taosd -s options to dump sdb.json

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_dumpsdb.py

        """
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = tdDnodes.binPath
        if binPath == "":
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % binPath)

        if os.path.exists("sdb.json"):
            os.system("rm -f sdb.json")

        os.system("%s -s" % binPath)

        if not os.path.exists("sdb.json"):
            tdLog.exit("taosd -s failed!")

    def test_repair_cmdline_phase1(self):
        """Taosd repair cmdline (phase1 freeze)

        1. Verify taosd -r parameter-layer behaviors frozen by phase1 doc.

        Since: v3.4.1.0

        Labels: common,ci
        """
        cases = [
            ("P01_legacy_r", "-r -V", 0, "version"),
            ("P02_repair_help", "-r --help", 0, "Usage: taosd -r"),
            (
                "P03_force_wal",
                "-r --node-type vnode --file-type wal --vnode-id 1234 --mode force -V",
                0,
                "version",
            ),
            (
                "P03_force_wal_no_version",
                "-r --node-type vnode --file-type wal --vnode-id 1 --mode force",
                0,
                "repair parameter validation succeeded (phase1)",
            ),
            (
                "P03_force_wal_equal_vnode_id",
                "-r --node-type vnode --file-type wal --vnode-id=1,2,3,4 --mode force",
                0,
                "repair parameter validation succeeded (phase1)",
            ),
            (
                "P04_force_tsdb_multi",
                "-r --node-type vnode --file-type tsdb --vnode-id 2,3 --mode force --backup-path /backup/vnode_tsdb -V",
                0,
                "version",
            ),
            (
                "P05_force_meta_none_backup",
                "-r --node-type vnode --file-type meta --vnode-id 1234 --mode force --backup-path none -V",
                0,
                "version",
            ),
            (
                "P05_force_meta_all_equals",
                "-r --node-type=vnode --file-type=meta --vnode-id=1234 --mode=force --backup-path=none -V",
                0,
                "version",
            ),
            (
                "E01_without_r",
                "--node-type vnode --file-type wal --vnode-id 1 --mode force -V",
                24,
                "repair options must be used with '-r'",
            ),
            ("E02_unknown", "-r --unknown-opt", 25, "invalid option"),
            (
                "E03_missing_node_type",
                "-r --file-type wal --vnode-id 1 --mode force -V",
                24,
                "missing '--node-type'",
            ),
            (
                "E03b_node_type_missing_value",
                "-r --node-type --file-type wal --vnode-id 1 --mode force -V",
                24,
                "'--node-type' requires a parameter",
            ),
            (
                "E04_non_vnode",
                "-r --node-type mnode --file-type wal --vnode-id 1 --mode force -V",
                1,
                "not supported in this phase",
            ),
            (
                "E05_missing_file_type",
                "-r --node-type vnode --vnode-id 1 --mode force -V",
                24,
                "missing '--file-type'",
            ),
            (
                "E06_file_type_tdb",
                "-r --node-type vnode --file-type tdb --vnode-id 1 --mode force -V",
                1,
                "not supported in this phase",
            ),
            (
                "E07_missing_vnode_id",
                "-r --node-type vnode --file-type wal --mode force -V",
                24,
                "missing '--vnode-id'",
            ),
            (
                "E08_vnode_id_format",
                "-r --node-type vnode --file-type wal --vnode-id 1,a --mode force -V",
                24,
                "invalid '--vnode-id' format",
            ),
            (
                "E09_missing_mode",
                "-r --node-type vnode --file-type wal --vnode-id 1 -V",
                24,
                "missing '--mode'",
            ),
            (
                "E10_deprecated_force",
                "-r --node-type vnode --file-type wal --vnode-id 1 --force 1 -V",
                24,
                "'--force' is deprecated",
            ),
            (
                "E10_deprecated_force_equals",
                "-r --node-type=vnode --file-type=wal --vnode-id=1 --force=1 -V",
                24,
                "'--force' is deprecated",
            ),
            (
                "E11_mode_copy",
                "-r --node-type vnode --file-type tsdb --vnode-id 1 --mode copy -V",
                1,
                "'--mode copy' is reserved and not supported in this phase",
            ),
            (
                "E11_mode_replica",
                "-r --node-type vnode --file-type wal --vnode-id 1 --mode replica -V",
                1,
                "'--mode replica' is reserved and not supported in this phase",
            ),
            (
                "E12_replica_node_in_force",
                "-r --node-type vnode --file-type tsdb --vnode-id 1 --mode force --replica-node 1.1.1.1:/d -V",
                24,
                "'--replica-node' is only valid when '--mode copy' is used",
            ),
        ]

        for name, args, expected_code, expected_text in cases:
            self._assert_taosd_case(name, args, expected_code, expected_text)
