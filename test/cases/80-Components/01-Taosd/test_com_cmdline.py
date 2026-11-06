# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import os
import shlex
import subprocess
import time


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
        if os.name == 'nt':
            # Windows preserves the full TSDB status; POSIX keeps its low byte.
            expected_code = {24: 0x80000118, 25: 0x80000119}.get(expected_code, expected_code)
        tdSql.checkEqual(code, expected_code)
        tdSql.checkEqual(expected_text in output, True)

    def _assert_taosd_cases(self, cases):
        for name, args, expected_code, expected_text in cases:
            self._assert_taosd_case(name, args, expected_code, expected_text)

    def test_dumpsdb(self):
        """Taosd command line
        
        1. Verify taosd -s options to dump sdb.json

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

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

    def test_arbgroup_sdb_export_import(self):
        """arbgroup sdb export/edit/import round-trip.

        Requires a 2-dnode cluster (run pytest with `-N 2 -C 2`): a 2-replica
        database creates an arbgroup. The conftest autouse fixture already deploys
        the cluster and connects tdSql, so this test drives it directly without
        re-initializing tdDnodes (which would drop the live tdSql connection).

        Steps:
        1. Join the 2nd dnode, create a 2-replica db, verify SHOW ARBGROUPS has rows.
        2. Stop mnode, taosd -s to dump sdb.json; assert the arbgroup record has
           editable fields.
        3. Flip the arbgroup isSync field in sdb.json.
        4. taosd -mSdb sdb.json + taosd -fMnode to rewrite sdb.
        5. Re-dump and assert the edited isSync persisted.

        Since: v3.5.0.0

        Labels: common,ci,integration,functional
        """
        import json as _json

        # wait until both dnodes are ready
        count = 0
        while count < 60:
            tdSql.query("select * from information_schema.ins_dnodes")
            ready = 0
            for r in tdSql.queryResult:
                if len(r) > 4 and r[4] == "ready":
                    ready += 1
            if ready >= 2:
                break
            time.sleep(1)
            count += 1

        tdSql.execute("create database db_import vgroups 1 replica 2;")
        tdSql.execute("use db_import;")
        # wait for the arbgroup to be created for the 2-replica db
        count = 0
        while count < 60:
            tdSql.query("show arbgroups;")
            if len(tdSql.queryResult) > 0:
                break
            time.sleep(1)
            count += 1
        tdSql.query("show arbgroups;")
        tdSql.checkEqual(len(tdSql.queryResult) > 0, True)

        binPath = self._get_taosd_bin()
        # The framework starts each dnode with a custom config dir; the offline
        # dump/modify tools must use the same -c so they read/write dnode 1's
        # (the mnode) real data directory rather than the default one.
        cfgDir = tdDnodes.dnodes[0].cfgDir
        # offline: stop the mnode (dnode 1) before dumping/modifying
        tdDnodes.stop(1)
        try:
            if os.path.exists("sdb.json"):
                os.system("rm -f sdb.json")
            os.system("%s -c %s -s" % (binPath, cfgDir))
            tdSql.checkEqual(os.path.exists("sdb.json"), True)

            with open("sdb.json") as f:
                data = _json.load(f)
            arb = None
            for rec in data["records"]:
                if rec.get("type") == "arb_group":
                    arb = rec
                    break
            tdSql.checkNotEqual(arb, None)
            tdSql.checkEqual("fields" in arb, True)
            tdSql.checkEqual("isSync" in arb["fields"], True)
            tdSql.checkEqual("member0.dnodeId" in arb["fields"], True)
            tdSql.checkEqual("assignedLeader.token" in arb["fields"], True)

            old_val = int(arb["fields"]["isSync"])
            new_val = 1 if old_val == 0 else 0
            # The dump encodes every numeric field as a JSON *string* (see
            # dumpArbGroupFields in mndDump.c / i642str). The offline overlay path
            # (OV_NUM -> tjsonGetBigIntValue -> cJSON_GetStringValue) only reads
            # string-encoded numbers, so the edited value MUST be written back as a
            # string; a Python int would serialize to a JSON number and be ignored.
            arb["fields"]["isSync"] = str(new_val)
            with open("sdb.json", "w") as f:
                _json.dump(data, f)

            with open("sdb.json") as f:
                data = _json.load(f)
            arb = None
            for rec in data["records"]:
                if rec.get("type") == "arb_group":
                    arb = rec
                    break
            tdSql.checkNotEqual(arb, None)
            tdSql.checkEqual("fields" in arb, True)
            tdSql.checkEqual("isSync" in arb["fields"], True)
            tdSql.checkEqual(int(arb["fields"]["isSync"]), new_val)

            os.system("%s -c %s -mSdb sdb.json" % (binPath, cfgDir))

            # re-dump and confirm the edit persisted in sdb
            os.system("rm -f sdb.json")
            os.system("%s -c %s -s" % (binPath, cfgDir))
            with open("sdb.json") as f:
                data2 = _json.load(f)
            arb2 = None
            for rec in data2["records"]:
                if rec.get("type") == "arb_group" and int(rec["fields"]["vgId"]) == int(arb["fields"]["vgId"]):
                    arb2 = rec
                    break
            tdSql.checkNotEqual(arb2, None)
            tdSql.checkEqual(int(arb2["fields"]["isSync"]), new_val)
        finally:
            # always restart the mnode so subsequent cases in this class are not polluted
            tdDnodes.start(1)

    def test_repair_cmdline_help_contract(self):
        """Repair cmdline help should expose the current repair-target interface.

        1. Verify `taosd -r --help` still prints force and copy mode usage.

        Since: v3.4.1.0

        Labels: common,ci,integration,functional
        """
        self._assert_taosd_case(
            "repair_help_contract",
            "-r --help",
            0,
            "Force mode (in-place repair)",
        )

    def test_repair_cmdline_accepts_valid_targets(self):
        """Repair cmdline should accept the current target grammar and strategy names.

        1. Verify valid `wal`, `meta`, and `tsdb` targets are accepted.
        2. Verify TSDB accepts the current explicit strategies.
        3. Verify mixed repair targets still parse together.

        Since: v3.4.1.0

        Labels: common,ci,integration,functional
        """
        cases = [
            (
                "valid_wal_target",
                "-r --mode force --node-type vnode --repair-target wal:vnode=1234 -V",
                0,
                "version",
            ),
            (
                "valid_meta_default_strategy",
                "-r --mode force --node-type vnode --repair-target meta:vnode=3 -V",
                0,
                "version",
            ),
            (
                "valid_meta_explicit_strategy",
                "-r --mode force --node-type vnode --repair-target meta:vnode=3:strategy=from_redo -V",
                0,
                "version",
            ),
            (
                "valid_tsdb_default_strategy",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809 --backup-path /backup/vnode_tsdb -V",
                0,
                "version",
            ),
            (
                "valid_tsdb_all_filesets_target",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=* -V",
                0,
                "version",
            ),
            (
                "valid_tsdb_head_only_rebuild_strategy",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809:strategy=head_only_rebuild -V",
                0,
                "version",
            ),
            (
                "valid_tsdb_full_rebuild_strategy",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809:strategy=full_rebuild -V",
                0,
                "version",
            ),
            (
                "valid_multi_target_mix",
                "-r --mode force --node-type vnode --backup-path /backup/vnode_mix --repair-target meta:vnode=3 --repair-target tsdb:vnode=5:fileid=1809:strategy=head_only_rebuild --repair-target wal:vnode=6 -V",
                0,
                "version",
            ),
        ]

        self._assert_taosd_cases(cases)

    def test_repair_cmdline_rejects_invalid_target_syntax(self):
        """Repair cmdline should reject invalid target syntax and invalid strategies.

        1. Verify parser-level grammar errors still fail with stable messages.
        2. Verify target-type-specific restrictions still hold.

        Since: v3.4.1.0

        Labels: common,ci,integration,functional
        """
        cases = [
            (
                "invalid_target_file_type",
                "-r --mode force --node-type vnode --repair-target foo:vnode=1 -V",
                24,
                "unknown file type 'foo'",
            ),
            (
                "duplicate_key_in_target",
                "-r --mode force --node-type vnode --repair-target meta:vnode=3:vnode=4 -V",
                24,
                "duplicated key 'vnode'",
            ),
            (
                "wal_strategy_not_supported",
                "-r --mode force --node-type vnode --repair-target wal:vnode=3:strategy=scan -V",
                24,
                "key 'strategy' is not supported for file type 'wal' in current phase",
            ),
            (
                "tsdb_missing_fileid",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5 -V",
                24,
                "missing required key 'fileid'",
            ),
            (
                "invalid_meta_strategy",
                "-r --mode force --node-type vnode --repair-target meta:vnode=3:strategy=foo -V",
                24,
                "invalid strategy 'foo' for file type 'meta'",
            ),
            (
                "invalid_tsdb_strategy",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809:strategy=deep_repair -V",
                24,
                "invalid strategy 'deep_repair' for file type 'tsdb'",
            ),
            (
                "duplicate_meta_target",
                "-r --mode force --node-type vnode --repair-target meta:vnode=3 --repair-target meta:vnode=3:strategy=from_redo -V",
                24,
                "duplicated repair target for meta vnode 3",
            ),
            (
                "duplicate_tsdb_target",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809 --repair-target tsdb:vnode=5:fileid=1809:strategy=full_rebuild -V",
                24,
                "duplicated repair target for tsdb vnode 5 fileid 1809",
            ),
            (
                "wildcard_then_explicit_tsdb_target_overlap",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=* --repair-target tsdb:vnode=5:fileid=1809 -V",
                24,
                "fileid=* overlaps existing tsdb repair targets",
            ),
            (
                "explicit_then_wildcard_tsdb_target_overlap",
                "-r --mode force --node-type vnode --repair-target tsdb:vnode=5:fileid=1809 --repair-target tsdb:vnode=5:fileid=* -V",
                24,
                "fileid=* overlaps existing tsdb repair targets",
            ),
        ]

        self._assert_taosd_cases(cases)

    def test_repair_cmdline_rejects_invalid_mode_and_legacy_args(self):
        """Repair cmdline should reject invalid mode usage and removed legacy options.

        1. Verify `--repair-target` cannot be used without `-r`.
        2. Verify required repair-mode options are enforced.
        3. Verify removed legacy args still fail as invalid options.

        Since: v3.4.1.0

        Labels: common,ci,integration,functional
        """
        cases = [
            (
                "target_without_r",
                "--mode force --node-type vnode --repair-target wal:vnode=1 -V",
                24,
                "'--repair-target' must be used with '-r'",
            ),
            (
                "unknown_option_under_r",
                "-r --unknown-opt",
                25,
                "invalid option",
            ),
            (
                "bare_r_missing_mode",
                "-r -V",
                24,
                "missing '--mode'",
            ),
            (
                "missing_node_type",
                "-r --mode force --repair-target wal:vnode=1 -V",
                24,
                "missing '--node-type'",
            ),
            (
                "non_vnode_node_type",
                "-r --mode force --node-type mnode --repair-target wal:vnode=1 -V",
                1,
                "currently only supports '--node-type vnode'",
            ),
            (
                "missing_repair_target",
                "-r --mode force --node-type vnode -V",
                24,
                "missing '--repair-target'",
            ),
            (
                "legacy_file_type_removed",
                "-r --mode force --node-type vnode --file-type wal -V",
                25,
                "invalid option",
            ),
            (
                "legacy_vnode_id_removed",
                "-r --mode force --node-type vnode --vnode-id 1 -V",
                25,
                "invalid option",
            ),
        ]

        self._assert_taosd_cases(cases)
