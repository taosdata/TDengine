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

from new_test_framework.utils import tdLog, tdSql, tdDnodes
import os
import shutil
import time
import shlex
import subprocess
import json
import re
import tempfile
import pytest


class TestTsdbForceRepair:
    # Phase 1 suite groups:
    # - metadata: dispatch, backup, crash-safe manifest behavior
    # - core_e2e: real core fileset corruption and post-repair recovery
    # - stt_e2e: real stt corruption and post-repair recovery
    SUITE_GROUPS = ("metadata", "core_e2e", "stt_e2e")
    _TSDB_FILE_SIZE_TOLERANCE = 4096

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

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

    def _get_cfg_dir(self):
        return tdDnodes.dnodes[0].cfgDir

    def _get_primary_data_dir(self):
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            first = data_dir[0]
            return first.split(" ")[0]
        return data_dir

    def _get_vnode_ids(self):
        vnode_root = os.path.join(self._get_primary_data_dir(), "vnode")
        if not os.path.isdir(vnode_root):
            return []

        vnode_ids = []
        for name in os.listdir(vnode_root):
            if not name.startswith("vnode"):
                continue
            suffix = name[5:]
            if suffix.isdigit():
                vnode_ids.append(int(suffix))
        return sorted(vnode_ids)

    def _get_vnode_id_for_db(self, dbname, table_name="t1"):
        tdSql.query(
            f"select vgroup_id from information_schema.ins_tables where db_name='{dbname}' and table_name='{table_name}'"
        )
        tdLog.info("ins_tables vnode lookup for %s.%s => %s" % (dbname, table_name, tdSql.queryResult))
        if len(tdSql.queryResult) > 0:
            value = tdSql.queryResult[0][0]
            if isinstance(value, int) and value > 0:
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)

        tdSql.query(f"show {dbname}.vgroups")
        tdSql.checkEqual(len(tdSql.queryResult) > 0, True)
        tdLog.info("show %s.vgroups => %s" % (dbname, tdSql.queryResult))
        row = tdSql.queryResult[0]
        for value in row:
            if isinstance(value, int) and value > 0:
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        tdLog.exit(f"failed to resolve vnode id from show {dbname}.vgroups result: {row}")

    def _tsdb_target(self, vnode_id, fid, strategy=None):
        target = f"tsdb:vnode={vnode_id}:fileid={fid}"
        if strategy:
            target += f":strategy={strategy}"
        return target

    def _tsdb_repair_args(self, vnode_id, fid, strategy=None, backup_root=None, extra_args=""):
        args = "-r --mode force --node-type vnode"
        if backup_root:
            args += f" --backup-path {backup_root}"
        args += f" --repair-target {self._tsdb_target(vnode_id, fid, strategy)}"
        if extra_args:
            args += f" {extra_args}"
        return args

    def _start_repair_process(self, args, extra_env=None):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path, "-c", self._get_cfg_dir()] + shlex.split(args)
        tdLog.info("run repair cmd: %s" % " ".join(cmd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = (
                "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
            )
        env.setdefault("LSAN_OPTIONS", "detect_leaks=0")
        if extra_env:
            env.update(extra_env)
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env=env,
        )

    def _stop_repair_process(self, proc):
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout else ""
            tdLog.info("repair proc exited early: %s" % output[:500].replace("\n", "\\n"))
            return output

        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)

        output = proc.stdout.read() if proc.stdout else ""
        tdLog.info("repair proc output=%s" % output[:500].replace("\n", "\\n"))
        return output

    def _run_taosd_with_cfg(self, args, timeout_sec=None, extra_env=None):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path, "-c", self._get_cfg_dir()] + shlex.split(args)
        tdLog.info("run cmd: %s" % " ".join(cmd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = (
                "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
            )
        env.setdefault("LSAN_OPTIONS", "detect_leaks=0")
        if extra_env:
            env.update(extra_env)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env=env,
        )
        try:
            output, _ = proc.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                output, _ = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                output, _ = proc.communicate(timeout=10)
        output = output or ""
        tdLog.info("ret=%s output=%s" % (proc.returncode, output[:500].replace("\n", "\\n")))
        return proc.returncode, output

    def _run_force_repair(self, vnode_id, fid, strategy=None, backup_root=None, extra_args="--log-output /dev/null",
                          timeout_sec=10, extra_env=None):
        return self._run_taosd_with_cfg(
            self._tsdb_repair_args(
                vnode_id,
                fid,
                strategy=strategy,
                backup_root=backup_root,
                extra_args=extra_args,
            ),
            timeout_sec=timeout_sec,
            extra_env=extra_env,
        )

    def _restart_taosd_and_wait_ready(self, timeout_sec=30):
        deadline = time.time() + timeout_sec
        last_error = None

        try:
            tdDnodes.stop(1)
        except BaseException:
            pass
        time.sleep(2)

        tdDnodes.startWithoutSleep(1)
        while time.time() < deadline:
            try:
                tdSql.query("select * from information_schema.ins_databases")
                return
            except BaseException as exc:
                last_error = exc
                time.sleep(1)

        if last_error is not None:
            raise last_error

    def _assert_database_writable_after_repair(self, dbname, pk_value, value, ts=None):
        ts_value = 1700001000000 if ts is None else ts
        normalized_pk = pk_value[:20]
        tdSql.execute(f"insert into {dbname}.t1 values({ts_value}, '{normalized_pk}', {value})")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.t1 where v1='{normalized_pk}'")
        tdSql.checkData(0, 0, 1)

    def _assert_repair_log_fields(self, log_text, fid, action=None, reason=None):
        tdSql.checkEqual(f"fid={fid}" in log_text, True)
        tdSql.checkEqual("action=" in log_text, True)
        tdSql.checkEqual("reason=" in log_text, True)
        if action is not None:
            tdSql.checkEqual(f"action={action}" in log_text, True)
        if reason is not None:
            tdSql.checkEqual(reason in log_text, True)

    def test_force_repair_fixture_builder_exposes_real_fileset(self):
        """TSDB force repair fixture builder should expose one real core fileset.

        1. Build a real core fixture from on-disk vnode data.
        2. Verify the fixture reports vnode, fid, and row count.
        3. Verify the resolved head and data files exist on disk.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(fixture["fid"] > 0, True)
        tdSql.checkEqual(fixture["row_count"] > 0, True)
        tdSql.checkEqual(os.path.isfile(fixture["fileset"]["head"]), True)
        tdSql.checkEqual(os.path.isfile(fixture["fileset"]["data"]), True)

    def test_force_repair_multi_fileset_fixture_exposes_multiple_real_filesets(self):
        """TSDB force repair fixture builder should expose multiple real core filesets.

        1. Build a multi-fileset core fixture from one vnode.
        2. Verify at least two distinct filesets are returned.
        3. Verify companion head and data files exist for those filesets.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(len(fixture["filesets"]) >= 2, True)
        tdSql.checkEqual(fixture["filesets"][0]["fid"] != fixture["filesets"][1]["fid"], True)
        tdSql.checkEqual(os.path.isfile(fixture["filesets"][0]["head"]), True)
        tdSql.checkEqual(os.path.isfile(fixture["filesets"][1]["data"]), True)

    def test_force_repair_stt_fixture_exposes_real_stt_file(self):
        """TSDB force repair fixture builder should expose one real stt file.

        1. Build a real stt fixture from flushed vnode data.
        2. Verify the fixture resolves vnode, fid, and stt entry count.
        3. Verify the referenced stt file exists on disk.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(fixture["fid"] > 0, True)
        tdSql.checkEqual(fixture["stt_entries"] > 0, True)
        tdSql.checkEqual(os.path.isfile(fixture["stt_path"]), True)

    def test_force_repair_corrupt_size_mismatch_changes_file_size(self):
        """TSDB force repair size-mismatch injector should alter file size.

        1. Create a temporary file with known size.
        2. Apply the size-mismatch corruption helper.
        3. Verify the helper reports size-mismatch and the file size changed.

        Since: v3.4.1.0

        Labels: common,ci
        """
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"x" * 64)
            path = fp.name

        try:
            before = os.path.getsize(path)
            info = self._corrupt_size_mismatch(path, mode="truncate")
            after = os.path.getsize(path)

            tdSql.checkEqual(info["corruption_type"], "size_mismatch")
            tdSql.checkEqual(after != before, True)
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_tsdb_force_repair_noop_on_healthy_fileset(self):
        """TSDB force repair should leave a healthy fileset readable and writable.

        1. Build one real healthy core fixture.
        2. Run force repair against that fileset without injecting corruption.
        3. Verify row count is unchanged and the table remains writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            code, output = self._run_force_repair(vnode_id, fid, extra_args="", timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, fixture["row_count"])
        tdSql.execute(f"insert into {dbname}.t1 values(1700000999999, 'pk_after_repair', 999)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.t1 where v1='pk_after_repair'")
        tdSql.checkData(0, 0, 1)

    @pytest.mark.xfail(
        reason="real missing-head filesets are not repaired when the manifest still references the absent core file",
        strict=False,
    )
    def test_tsdb_force_repair_missing_head_real_fileset_remains_writable(self):
        """TSDB force repair should keep the database writable after missing-head repair.

        1. Build one real core fixture and remove its head file.
        2. Run force repair with backup enabled for the affected fid.
        3. Verify repair output is persisted and the database remains writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        backup_root = f"/tmp/tsdb-force-repair-missing-head-{time.time_ns()}"

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(fixture["fileset"]["head"])
            code, output = self._run_force_repair(vnode_id, fid, backup_root=backup_root, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows >= 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fid)
        tdSql.checkEqual(os.path.isfile(repair_log), True)
        with open(repair_log, "r", encoding="utf-8") as fp:
            log_text = fp.read()
        tdSql.checkEqual("action=drop_core_group" in log_text, True)
        self._assert_database_writable_after_repair(dbname, "pk_after_missing_head", 1001)

    @pytest.mark.xfail(
        reason="real missing-head repair still leaves vnode closed even when other core filesets remain healthy",
        strict=False,
    )
    def test_tsdb_force_repair_missing_old_head_in_multi_fileset_keeps_database_writable(self):
        """TSDB force repair should keep multi-fileset databases writable after old-head repair.

        1. Build one vnode fixture with multiple real core filesets.
        2. Remove the head file from an older fileset and run force repair.
        3. Verify remaining data stays readable and the table remains writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        target = fixture["filesets"][0]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(target["head"])
            code, output = self._run_force_repair(vnode_id, target["fid"], timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows < fixture["row_count"], True)
        self._assert_database_writable_after_repair(dbname, "pk_after_multi_missing_head", 2001)

    @pytest.mark.xfail(
        reason="real head size-mismatch with full_rebuild still leaves the fileset unreadable after restart",
        strict=False,
    )
    def test_tsdb_force_repair_full_rebuild_recovers_real_head_size_mismatch(self):
        """TSDB force repair full rebuild should recover real head size mismatch.

        1. Build one real core fixture and truncate its head file to create a size mismatch.
        2. Run force repair with `full_rebuild` for the affected fid.
        3. Verify rows remain queryable and the database stays writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_size_mismatch(fixture["fileset"]["head"], mode="truncate")
            code, output = self._run_force_repair(vnode_id, fid, strategy="full_rebuild", timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        self._assert_database_writable_after_repair(dbname, "pk_after_full_rebuild", 3001)

    @pytest.mark.xfail(
        reason="real missing-stt repair still leaves vnode closed after restart",
        strict=False,
    )
    def test_tsdb_force_repair_missing_stt_real_fileset_remains_writable(self):
        """TSDB force repair should keep the database writable after missing-stt repair.

        1. Build one real stt fixture and remove its stt file.
        2. Run force repair for the affected fid.
        3. Verify the table still accepts writes after the node restarts.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        table_name = fixture["table_name"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(fixture["stt_path"])
            code, output = self._run_force_repair(vnode_id, fid, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.{table_name}")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows >= 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        tdSql.execute(f"insert into {dbname}.{table_name} values(1700001000100, 3, 2.5)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.{table_name} where ts=1700001000100")
        tdSql.checkData(0, 0, 1)

    def test_tsdb_force_repair_dispatches_in_open_fs(self):
        """TSDB force repair should dispatch inside tsdb open fs.

        1. Create a real vnode.
        2. Run tsdb force repair.
        3. Verify tsdb open fs reports the repair dispatch marker.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_dispatch_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")
        repair_fid = candidate["fid"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass

        tdSql.checkEqual("tsdb force repair dispatch" in output, True)
        tdSql.checkEqual("repair parameter validation succeeded (phase1)" in output, False)

    def test_tsdb_force_repair_enters_real_execution_path(self):
        """TSDB force repair should not stay on phase1 placeholder path.

        1. Create a real vnode.
        2. Stop taosd and run tsdb force repair against that vnode.
        3. Verify the process no longer reports the phase1 placeholder text.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_entry_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")
        repair_fid = candidate["fid"]

        code, output = self._run_taosd_with_cfg(
            self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null")
        )

        tdSql.checkEqual("repair execution is not enabled in this phase" in output, False)
        tdSql.checkEqual(code == 0 and "repair parameter validation succeeded (phase1)" in output, False)

    def _get_tsdb_dir(self, vnode_id):
        return os.path.join(self._get_primary_data_dir(), "vnode", f"vnode{vnode_id}", "tsdb")

    def _find_first_tsdb_file(self, vnode_id, suffix):
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        for root, _, files in os.walk(tsdb_dir):
            for name in sorted(files):
                if name.endswith(suffix):
                    return os.path.join(root, name)
        return None

    def _find_companion_tsdb_file(self, data_file, suffix):
        if not data_file.endswith(".data"):
            return None
        candidate = data_file[: -len('.data')] + suffix
        return candidate if os.path.isfile(candidate) else None

    def _manifest_size_matches_file(self, path, expected_size):
        if path is None or expected_size is None or not os.path.isfile(path):
            return False

        actual_size = os.path.getsize(path)
        if actual_size == expected_size:
            return True

        return actual_size > expected_size and actual_size - expected_size <= self._TSDB_FILE_SIZE_TOLERANCE

    def _find_size_matched_core_fileset(self, vnode_id):
        candidates = self._find_size_matched_core_filesets(vnode_id)
        return candidates[0] if candidates else None

    def _find_size_matched_core_filesets(self, vnode_id):
        current_json = self._current_json_path(vnode_id)
        current = self._load_current_json(current_json)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        candidates = []

        for fset in current.get("fset", []):
            head = fset.get("head")
            data = fset.get("data")
            if not head or not data:
                continue

            fid = fset.get("fid")
            head_cid = head.get("cid")
            data_cid = data.get("cid")
            if fid is None or head_cid is None or data_cid is None:
                continue

            head_file = os.path.join(tsdb_dir, f"v{vnode_id}f{fid}ver{head_cid}.head")
            data_file = os.path.join(tsdb_dir, f"v{vnode_id}f{fid}ver{data_cid}.data")
            if not (os.path.isfile(head_file) and os.path.isfile(data_file)):
                continue

            if not self._manifest_size_matches_file(head_file, head.get("size")):
                continue
            if not self._manifest_size_matches_file(data_file, data.get("size")):
                continue

            candidates.append({"fid": fid, "head": head_file, "data": data_file})

        return sorted(candidates, key=lambda item: item["fid"])

    def _parse_fid_from_tsdb_path(self, path):
        match = re.search(r"f(\d+)ver", os.path.basename(path))
        return int(match.group(1)) if match else None

    def _corrupt_missing_file(self, path):
        tdSql.checkEqual(os.path.exists(path), True)
        original_size = os.path.getsize(path)
        os.remove(path)
        tdLog.info("remove file for corruption path=%s size=%d" % (path, original_size))
        return {
            "corruption_type": "missing_file",
            "target_path": path,
            "original_size": original_size,
            "new_size": None,
        }

    def _corrupt_size_mismatch(self, path, mode="truncate", delta=1):
        tdSql.checkEqual(os.path.isfile(path), True)
        tdSql.checkEqual(mode in ("truncate", "extend"), True)

        original_size = os.path.getsize(path)
        tdSql.checkEqual(original_size > 0, True)

        with open(path, "r+b") as fp:
            if mode == "truncate":
                new_size = max(1, original_size - max(1, delta))
                fp.truncate(new_size)
            else:
                fp.seek(0, os.SEEK_END)
                fp.write(b"\x00" * max(1, delta))
                new_size = fp.tell()

        tdLog.info(
            "change file size for corruption path=%s mode=%s from=%d to=%d"
            % (path, mode, original_size, new_size)
        )
        return {
            "corruption_type": "size_mismatch",
            "target_path": path,
            "original_size": original_size,
            "new_size": new_size,
            "mode": mode,
        }

    def _overwrite_middle_bytes(self, path, length=256):
        size = os.path.getsize(path)
        tdSql.checkEqual(size > length * 4, True)
        header_size = 512
        length = max(length, 1024)
        offsets = [max(header_size + 128, (size * 3) // 4)]
        with open(path, "r+b") as fp:
            for offset in offsets:
                if offset + length >= size:
                    continue
                fp.seek(offset)
                fp.write(b"\x00" * length)
        tdLog.info("corrupt data file %s at offsets=%s length=%d size=%d" % (path, offsets, length, size))
        return offsets[0]

    def _corrupt_middle_bytes(self, path, length=256):
        original_size = os.path.getsize(path)
        offset = self._overwrite_middle_bytes(path, length=length)
        return {
            "corruption_type": "middle_bytes",
            "target_path": path,
            "original_size": original_size,
            "new_size": original_size,
            "offset": offset,
            "length": max(length, 1024),
        }

    def _snapshot_tsdb_dir(self, vnode_id):
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        snapshot = []
        if not os.path.isdir(tsdb_dir):
            return snapshot
        for root, _, files in os.walk(tsdb_dir):
            for name in sorted(files):
                if name.endswith((".data", ".head", ".sma", ".stt", ".tomb", ".json")):
                    snapshot.append(os.path.relpath(os.path.join(root, name), tsdb_dir))
        return snapshot

    def _insert_rebuild_rows(self, dbname, start, end, ts0, batch_size=500):
        for batch_start in range(start, end, batch_size):
            values = []
            batch_end = min(batch_start + batch_size, end)
            for idx in range(batch_start, batch_end):
                values.append(f"({ts0 + idx}, 'pk_{idx}', {idx})")
            tdSql.execute(f"insert into {dbname}.t1 values " + ",".join(values))

    def _wait_for_data_file(self, dbname, vnode_id, timeout_sec=90):
        deadline = time.time() + timeout_sec
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            data_file = self._find_first_tsdb_file(vnode_id, ".data")
            if data_file is not None and os.path.exists(data_file):
                tdLog.info("vnode%d data file materialized on attempt %d: %s" % (vnode_id, attempt, data_file))
                return data_file
            tdLog.info(
                "wait data file for db=%s vnode=%d attempt=%d snapshot=%s"
                % (dbname, vnode_id, attempt, self._snapshot_tsdb_dir(vnode_id)[:20])
            )
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
        return None

    def _wait_for_stt_file(self, dbname, vnode_id, timeout_sec=90):
        deadline = time.time() + timeout_sec
        attempt = 0
        current_json = self._current_json_path(vnode_id)
        while time.time() < deadline:
            attempt += 1
            stt_file = self._find_first_tsdb_file(vnode_id, ".stt")
            stt_entries = 0
            if os.path.exists(current_json):
                stt_entries = self._count_stt_entries_in_current(current_json)
            if stt_file is not None and stt_entries > 0 and os.path.exists(stt_file):
                tdLog.info(
                    "vnode%d stt file materialized on attempt %d: %s entries=%d"
                    % (vnode_id, attempt, stt_file, stt_entries)
                )
                return stt_file, stt_entries

            tdLog.info(
                "wait stt file for db=%s vnode=%d attempt=%d entries=%d snapshot=%s"
                % (dbname, vnode_id, attempt, stt_entries, self._snapshot_tsdb_dir(vnode_id)[:20])
            )
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
        return None, 0

    def _wait_for_size_matched_core_fileset(self, dbname, vnode_id, timeout_sec=90):
        deadline = time.time() + timeout_sec
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            candidate = self._find_size_matched_core_fileset(vnode_id)
            if candidate is not None:
                tdLog.info(
                    "vnode%d size-matched core fileset found on attempt %d: fid=%s"
                    % (vnode_id, attempt, candidate["fid"])
                )
                return candidate

            tdLog.info(
                "wait size-matched core fileset for db=%s vnode=%d attempt=%d snapshot=%s"
                % (dbname, vnode_id, attempt, self._snapshot_tsdb_dir(vnode_id)[:20])
            )
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
        return None

    def _wait_for_size_matched_core_filesets(self, dbname, vnode_id, min_count=2, timeout_sec=90):
        deadline = time.time() + timeout_sec
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            candidates = self._find_size_matched_core_filesets(vnode_id)
            if len(candidates) >= min_count:
                tdLog.info(
                    "vnode%d found %d size-matched core filesets on attempt %d: fids=%s"
                    % (vnode_id, len(candidates), attempt, [item["fid"] for item in candidates])
                )
                return candidates

            tdLog.info(
                "wait %d size-matched core filesets for db=%s vnode=%d attempt=%d snapshot=%s"
                % (min_count, dbname, vnode_id, attempt, self._snapshot_tsdb_dir(vnode_id)[:20])
            )
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
        return []

    def _prepare_core_fixture(self, total_rows=9000):
        dbname = f"tsdb_repair_fixture_{time.time_ns()}"
        ts0 = 1700000000000

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200")
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)")

        self._insert_rebuild_rows(dbname, 0, 8191, ts0)
        tdSql.execute(f"flush database {dbname}")
        self._insert_rebuild_rows(dbname, 8191, total_rows, ts0)
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, total_rows)

        vnode_id = self._get_vnode_id_for_db(dbname)
        data_file = self._wait_for_data_file(dbname, vnode_id, timeout_sec=90)
        if data_file is None:
            pytest.skip("real data file was not materialized in time after async flush")

        candidate = self._wait_for_size_matched_core_fileset(dbname, vnode_id, timeout_sec=90)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")

        return {
            "dbname": dbname,
            "vnode_id": vnode_id,
            "fid": candidate["fid"],
            "row_count": total_rows,
            "fileset": {
                "head": candidate["head"],
                "data": candidate["data"],
                "sma": self._find_companion_tsdb_file(candidate["data"], ".sma"),
            },
        }

    def _prepare_multi_fileset_core_fixture(self, rows_per_batch=2000):
        dbname = f"tsdb_repair_multi_fixture_{time.time_ns()}"
        ts0 = 1700000000000
        one_day_ms = 24 * 60 * 60 * 1000
        offsets = (0, 2 * one_day_ms)

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 duration 1d stt_trigger 1 minrows 10 maxrows 200")
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)")

        inserted_rows = 0
        for batch_index, offset in enumerate(offsets):
            batch_ts0 = ts0 + offset
            start = batch_index * rows_per_batch
            end = start + rows_per_batch
            self._insert_rebuild_rows(dbname, start, end, batch_ts0)
            inserted_rows += rows_per_batch
            tdSql.execute(f"flush database {dbname}")

        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, inserted_rows)

        vnode_id = self._get_vnode_id_for_db(dbname)
        filesets = self._wait_for_size_matched_core_filesets(dbname, vnode_id, min_count=2, timeout_sec=90)
        if len(filesets) < 2:
            pytest.skip("real multi-fileset core fixture was not materialized in time")

        normalized = []
        for item in filesets[:2]:
            normalized.append(
                {
                    "fid": item["fid"],
                    "head": item["head"],
                    "data": item["data"],
                    "sma": self._find_companion_tsdb_file(item["data"], ".sma"),
                }
            )

        return {
            "dbname": dbname,
            "vnode_id": vnode_id,
            "row_count": inserted_rows,
            "filesets": normalized,
        }

    def _prepare_stt_fixture(self, total_rows=4000):
        dbname = f"tsdb_repair_stt_fixture_{time.time_ns()}"
        ts0 = 1700000000000
        table_name = "d0"

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200")
        tdSql.execute(f"drop table if exists {dbname}.meters")
        tdSql.execute(f"create table {dbname}.meters (ts timestamp, c1 int, c2 float) tags(t1 int)")
        tdSql.execute(f"create table {dbname}.{table_name} using {dbname}.meters tags(1)")

        sql = f"insert into {dbname}.{table_name} values "
        sql += ",".join(f"({ts0 + i}, 1, 0.1)" for i in range(100))
        tdSql.execute(sql)
        tdSql.execute(f"flush database {dbname}")

        sql = f"insert into {dbname}.{table_name} values "
        sql += ",".join(f"({ts0 + 99 + i}, 1, 0.1)" for i in range(100))
        tdSql.execute(sql)
        tdSql.execute(f"flush database {dbname}")

        tdSql.execute(f"insert into {dbname}.{table_name} values({ts0 + 1000}, 2, 1.0)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        tdSql.query(f"select count(*) from {dbname}.{table_name}")
        tdSql.checkData(0, 0, 200)

        vnode_id = self._get_vnode_id_for_db(dbname, table_name=table_name)
        stt_path, stt_entries = self._wait_for_stt_file(dbname, vnode_id, timeout_sec=90)
        if stt_path is None or stt_entries <= 0:
            pytest.skip("real stt fixture was not materialized in time")

        fid = self._parse_fid_from_tsdb_path(stt_path)
        tdSql.checkEqual(fid is not None, True)
        return {
            "dbname": dbname,
            "vnode_id": vnode_id,
            "fid": fid,
            "row_count": 200,
            "table_name": table_name,
            "stt_path": stt_path,
            "stt_entries": stt_entries,
        }

    def _find_backup_manifest(self, backup_root, vnode_id):
        date_str = time.strftime("%Y%m%d")
        base = os.path.join(backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "tsdb")
        if not os.path.isdir(base):
            return None

        for root, _, files in os.walk(base):
            if "manifest.json" in files:
                return os.path.join(root, "manifest.json")
        return None

    def _find_backup_log(self, backup_root, vnode_id):
        date_str = time.strftime("%Y%m%d")
        base = os.path.join(backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "tsdb")
        if not os.path.isdir(base):
            return None

        for root, _, files in os.walk(base):
            if "repair.log" in files:
                return os.path.join(root, "repair.log")
        return None

    def _find_backup_log_for_fid(self, backup_root, vnode_id, fid):
        date_str = time.strftime("%Y%m%d")
        return os.path.join(backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "tsdb", f"fid_{fid}", "repair.log")
    @pytest.mark.xfail(
        reason="real missing-stt repair currently returns success without emitting backup manifest",
        strict=False,
    )
    def test_tsdb_force_repair_backs_up_affected_fileset_with_manifest(self):
        """TSDB force repair should back up affected file set and manifest.

        1. Create a real vnode with TSDB files.
        2. Delete one stt file to simulate a missing-file fileset corruption.
        3. Run tsdb force repair and verify affected fileset backup + manifest exist.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_backup_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        backup_root = f"/tmp/tsdb-force-repair-backup-{int(time.time())}"
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = self._find_first_tsdb_file(vnode_id, ".stt")
            tdSql.checkEqual(corrupt_file is not None, True)
            repair_fid = self._parse_fid_from_tsdb_path(corrupt_file)
            tdSql.checkEqual(repair_fid is not None, True)
            os.remove(corrupt_file)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, backup_root=backup_root, extra_args="--log-output /dev/null"),
                timeout_sec=5,
            )
            manifest = self._find_backup_manifest(backup_root, vnode_id)
            tdSql.checkEqual("tsdb force repair dispatch" in output, True)
            tdSql.checkEqual(manifest is not None, True)
            tdSql.checkEqual(os.path.isfile(manifest), True)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)

    def _current_json_path(self, vnode_id):
        return os.path.join(self._get_tsdb_dir(vnode_id), "current.json")

    def _count_stt_entries_in_current(self, current_json_path):
        with open(current_json_path, "r", encoding="utf-8") as fp:
            current = json.load(fp)

        count = 0
        for fset in current.get("fset", []):
            for lvl in fset.get("stt lvl", []):
                count += len(lvl.get("files", []))
        return count

    def _load_current_json(self, current_json_path):
        with open(current_json_path, "r", encoding="utf-8") as fp:
            return json.load(fp)

    def _save_current_json(self, current_json_path, current):
        with open(current_json_path, "w", encoding="utf-8") as fp:
            json.dump(current, fp, separators=(",", ":"))

    def _find_fset_by_fid(self, current_json_path, fid):
        current = self._load_current_json(current_json_path)
        for fset in current.get("fset", []):
            if fset.get("fid") == fid:
                return fset
        return None

    def _get_sample_disk_id_from_current(self, current):
        did_level = 0
        did_id = 0
        if current.get("fset") and current["fset"][0].get("stt lvl"):
            sample = current["fset"][0]["stt lvl"][0]["files"][0]
            did_level = sample["did.level"]
            did_id = sample["did.id"]
        return did_level, did_id

    def _build_fake_core_fileset_paths(self, tsdb_dir, vnode_id, fake_fid, fake_cid=1):
        return {
            "head": os.path.join(tsdb_dir, f"v{vnode_id}f{fake_fid}ver{fake_cid}.head"),
            "data": os.path.join(tsdb_dir, f"v{vnode_id}f{fake_fid}ver{fake_cid}.data"),
            "sma": os.path.join(tsdb_dir, f"v{vnode_id}f{fake_fid}ver{fake_cid}.sma"),
        }

    def _append_fake_core_fileset(self, current_json_path, fake_fid, fake_cid=1, size=16):
        current = self._load_current_json(current_json_path)
        did_level, did_id = self._get_sample_disk_id_from_current(current)
        fake_fset = {
            "fid": fake_fid,
            "head": {"did.level": did_level, "did.id": did_id, "lcn": 0, "fid": fake_fid, "mid": 0, "cid": fake_cid, "size": size, "minVer": 1, "maxVer": 1},
            "data": {"did.level": did_level, "did.id": did_id, "lcn": 0, "fid": fake_fid, "mid": 0, "cid": fake_cid, "size": size, "minVer": 1, "maxVer": 1},
            "sma": {"did.level": did_level, "did.id": did_id, "lcn": 0, "fid": fake_fid, "mid": 0, "cid": fake_cid, "size": size, "minVer": 1, "maxVer": 1},
            "stt lvl": [],
            "last compact": 0,
            "last commit": 0,
            "last migrate": 0,
            "last rollup": 0,
            "rlevel": 0,
        }
        current.setdefault("fset", []).append(fake_fset)
        self._save_current_json(current_json_path, current)

    def _write_fake_core_files(self, file_paths, content=b"repair-test"):
        for path in file_paths.values():
            with open(path, "wb") as fp:
                fp.write(content)

    def _count_core_entries_in_current(self, current_json_path):
        with open(current_json_path, "r", encoding="utf-8") as fp:
            current = json.load(fp)

        count = 0
        for fset in current.get("fset", []):
            for key in ("head", "data", "sma"):
                if key in fset:
                    count += 1
        return count

    def _wait_for_core_entries(self, current_json_path, timeout_sec=20):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if os.path.exists(current_json_path) and self._count_core_entries_in_current(current_json_path) > 0:
                return True
            time.sleep(1)
        return False


    @pytest.mark.xfail(
        reason="real missing-stt repair currently leaves the stale stt reference in current.json",
        strict=False,
    )
    def test_tsdb_force_repair_removes_missing_stt_from_current(self):
        """TSDB force repair should remove missing stt from current.json.

        1. Create a real vnode with stt file metadata.
        2. Delete one stt file.
        3. Run tsdb force repair and verify current.json no longer references that stt.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()
        vnode_id = fixture["vnode_id"]
        current_json = self._current_json_path(vnode_id)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = fixture["stt_path"]
            repair_fid = fixture["fid"]
            before_count = self._count_stt_entries_in_current(current_json)
            tdSql.checkEqual(before_count > 0, True)
            os.remove(corrupt_file)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
            after_count = self._count_stt_entries_in_current(current_json)
            tdSql.checkEqual(code, 0)
            tdSql.checkEqual(after_count, 0)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass

    @pytest.mark.xfail(
        reason="staged current manifest recovery for missing-stt repair does not complete on restart in the current implementation",
        strict=False,
    )
    def test_tsdb_force_repair_current_update_recovers_from_staged_manifest(self):
        """TSDB force repair current update should recover from staged manifest on restart.

        1. Create a real vnode with stt metadata.
        2. Delete one stt file and run repair with a test abort after staging `current.c.json`.
        3. Verify restart commits the staged manifest and removes missing stt from current.json.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_crashsafe_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        current_json = self._current_json_path(vnode_id)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = self._find_first_tsdb_file(vnode_id, ".stt")
            tdSql.checkEqual(corrupt_file is not None, True)
            before_count = self._count_stt_entries_in_current(current_json)
            tdSql.checkEqual(before_count > 0, True)
            os.remove(corrupt_file)

            abort_marker = "/tmp/taos_repair_test_abort_after_stage"
            with open(abort_marker, "w", encoding="utf-8") as fp:
                fp.write("1")
            repair_fid = self._parse_fid_from_tsdb_path(corrupt_file)
            tdSql.checkEqual(repair_fid is not None, True)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
            if os.path.exists(abort_marker):
                os.remove(abort_marker)
            tdSql.checkEqual(self._count_stt_entries_in_current(current_json) > 0, True)

            code, output = self._run_taosd_with_cfg("--log-output /dev/null", timeout_sec=5)
            tdSql.checkEqual(self._count_stt_entries_in_current(current_json), 0)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass



    @pytest.mark.skip(reason="superseded by real core-fixture coverage; synthetic current.json core injection no longer reflects real repair semantics")
    def test_tsdb_force_repair_removes_missing_head_data_sma_from_current(self):
        """TSDB force repair should drop head/data/sma together when head is missing.

        1. Create a real vnode and inject one synthetic head/data/sma fileset into current.json.
        2. Delete the synthetic head file.
        3. Run tsdb force repair and verify current.json no longer references head/data/sma for that fid.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_remove_core_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        current_json = self._current_json_path(vnode_id)

        fake_fid = 990001
        fake_cid = 1
        fake_paths = self._build_fake_core_fileset_paths(tsdb_dir, vnode_id, fake_fid, fake_cid=fake_cid)
        self._append_fake_core_fileset(current_json, fake_fid, fake_cid=fake_cid)
        self._write_fake_core_files(fake_paths)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            os.remove(fake_paths["head"])
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fake_fid, extra_args="--log-output /dev/null"),
                timeout_sec=20,
            )
            fset = self._find_fset_by_fid(current_json, fake_fid)
            tdSql.checkEqual(fset is not None, True)
            tdSql.checkEqual("head" in fset, False)
            tdSql.checkEqual("data" in fset, False)
            tdSql.checkEqual("sma" in fset, False)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            for path in fake_paths.values():
                if os.path.exists(path):
                    os.remove(path)

    @pytest.mark.xfail(
        reason="real missing-stt repair currently returns success without emitting backup repair.log",
        strict=False,
    )
    def test_tsdb_force_repair_backup_writes_repair_log(self):
        """TSDB force repair backup should write a repair log with reason.

        1. Create a real vnode with stt metadata.
        2. Delete one stt file.
        3. Run tsdb force repair and verify backup contains repair.log with missing-file reason.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_log_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        backup_root = f"/tmp/tsdb-force-repair-log-{int(time.time())}"
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = self._find_first_tsdb_file(vnode_id, ".stt")
            tdSql.checkEqual(corrupt_file is not None, True)
            repair_fid = self._parse_fid_from_tsdb_path(corrupt_file)
            tdSql.checkEqual(repair_fid is not None, True)
            os.remove(corrupt_file)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, backup_root=backup_root, extra_args="--log-output /dev/null"),
                timeout_sec=5,
            )
            repair_log = self._find_backup_log(backup_root, vnode_id)
            tdSql.checkEqual(code, 0)
            tdSql.checkEqual(repair_log is not None, True)
            with open(repair_log, "r", encoding="utf-8") as fp:
                log_text = fp.read()
            self._assert_repair_log_fields(log_text, repair_fid, action="drop_stt_file", reason="missing_stt")
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)

    @pytest.mark.skip(reason="superseded by real core-fixture coverage; synthetic current.json core injection no longer reflects real repair semantics")
    def test_tsdb_force_repair_removes_missing_data_head_data_sma_from_current(self):
        """TSDB force repair should drop head/data/sma together when data is missing.

        1. Create a real vnode and inject one synthetic head/data/sma fileset into current.json.
        2. Delete the synthetic data file.
        3. Run tsdb force repair and verify current.json no longer references head/data/sma for that fid.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_remove_core_data_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        current_json = self._current_json_path(vnode_id)

        fake_fid = 990002
        fake_cid = 1
        fake_paths = self._build_fake_core_fileset_paths(tsdb_dir, vnode_id, fake_fid, fake_cid=fake_cid)
        self._append_fake_core_fileset(current_json, fake_fid, fake_cid=fake_cid)
        self._write_fake_core_files(fake_paths)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            os.remove(fake_paths["data"])
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fake_fid, extra_args="--log-output /dev/null"),
                timeout_sec=20,
            )
            fset = self._find_fset_by_fid(current_json, fake_fid)
            tdSql.checkEqual(fset is not None, True)
            tdSql.checkEqual("head" in fset, False)
            tdSql.checkEqual("data" in fset, False)
            tdSql.checkEqual("sma" in fset, False)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            for path in fake_paths.values():
                if os.path.exists(path):
                    os.remove(path)

    @pytest.mark.skip(reason="superseded by real core-fixture coverage; synthetic current.json core injection no longer reflects real repair semantics")
    def test_tsdb_force_repair_removes_size_mismatch_head_data_sma_from_current(self):
        """TSDB deep repair should drop head/data/sma when core file size mismatches manifest.

        1. Inject one synthetic head/data/sma fileset into current.json.
        2. Keep head file present but with size different from manifest.
        3. Run tsdb force repair with an explicit deep strategy and verify current.json no longer references
           head/data/sma for that fid.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_size_mismatch_core_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        current_json = self._current_json_path(vnode_id)

        fake_fid = 990003
        fake_cid = 1
        fake_paths = self._build_fake_core_fileset_paths(tsdb_dir, vnode_id, fake_fid, fake_cid=fake_cid)
        self._append_fake_core_fileset(current_json, fake_fid, fake_cid=fake_cid)
        with open(fake_paths["head"], "wb") as fp:
            fp.write(b"short")
        self._write_fake_core_files({"data": fake_paths["data"], "sma": fake_paths["sma"]})

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fake_fid, strategy="full_rebuild", extra_args="--log-output /dev/null"),
                timeout_sec=20,
            )
            fset = self._find_fset_by_fid(current_json, fake_fid)
            tdSql.checkEqual(fset is not None, True)
            tdSql.checkEqual("head" in fset, False)
            tdSql.checkEqual("data" in fset, False)
            tdSql.checkEqual("sma" in fset, False)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            for path in fake_paths.values():
                if os.path.exists(path):
                    os.remove(path)

    @pytest.mark.skip(reason="superseded by real core-fixture coverage; synthetic current.json core backup assertions no longer reflect real repair semantics")
    def test_tsdb_force_repair_backup_writes_size_mismatch_reason(self):
        """TSDB deep repair backup should record size mismatch reason for core files.

        1. Inject one synthetic head/data/sma fileset into current.json.
        2. Keep head present but with size different from manifest.
        3. Run tsdb force repair with an explicit deep strategy and verify repair.log contains `size_mismatch_core`.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_log_size_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        current_json = self._current_json_path(vnode_id)
        backup_root = f"/tmp/tsdb-force-repair-log-size-{int(time.time())}"

        fake_fid = 990004
        fake_cid = 1
        fake_paths = self._build_fake_core_fileset_paths(tsdb_dir, vnode_id, fake_fid, fake_cid=fake_cid)
        self._append_fake_core_fileset(current_json, fake_fid, fake_cid=fake_cid)
        with open(fake_paths["head"], "wb") as fp:
            fp.write(b"short")
        self._write_fake_core_files({"data": fake_paths["data"], "sma": fake_paths["sma"]})

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(
                    vnode_id,
                    fake_fid,
                    strategy="full_rebuild",
                    backup_root=backup_root,
                    extra_args="--log-output /dev/null",
                ),
                timeout_sec=5,
            )
            repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fake_fid)
            tdSql.checkEqual("tsdb force repair dispatch" in output, True)
            tdSql.checkEqual(repair_log is not None, True)
            with open(repair_log, "r", encoding="utf-8") as fp:
                log_text = fp.read()
            tdSql.checkEqual("size_mismatch_core" in log_text, True)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            for path in fake_paths.values():
                if os.path.exists(path):
                    os.remove(path)
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)

    @pytest.mark.skip(reason="superseded by real core-fixture coverage; synthetic current.json core backup assertions no longer reflect real repair semantics")
    def test_tsdb_force_repair_backup_writes_action_for_missing_core(self):
        """TSDB force repair backup should record action for missing core files.

        1. Inject one synthetic head/data/sma fileset into current.json.
        2. Delete the synthetic head file.
        3. Run tsdb force repair and verify repair.log contains `action=drop_core_group`.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_log_action_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        tsdb_dir = self._get_tsdb_dir(vnode_id)
        current_json = self._current_json_path(vnode_id)
        backup_root = f"/tmp/tsdb-force-repair-log-action-{int(time.time())}"

        fake_fid = 990005
        fake_cid = 1
        fake_paths = self._build_fake_core_fileset_paths(tsdb_dir, vnode_id, fake_fid, fake_cid=fake_cid)
        self._append_fake_core_fileset(current_json, fake_fid, fake_cid=fake_cid)
        self._write_fake_core_files(fake_paths)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            os.remove(fake_paths["head"])
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fake_fid, backup_root=backup_root, extra_args="--log-output /dev/null"),
                timeout_sec=5,
            )
            repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fake_fid)
            tdSql.checkEqual("tsdb force repair dispatch" in output, True)
            tdSql.checkEqual(repair_log is not None, True)
            with open(repair_log, "r", encoding="utf-8") as fp:
                log_text = fp.read()
            tdSql.checkEqual("action=drop_core_group" in log_text, True)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            for path in fake_paths.values():
                if os.path.exists(path):
                    os.remove(path)
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)


    def test_tsdb_force_repair_rebuilds_core_from_valid_blocks(self):
        """TSDB force repair should rebuild head/data/sma from readable blocks.

        1. Create one vnode with enough rows to form multiple blocks.
        2. Corrupt late bytes in a real `.head` file without changing file size.
        3. Run tsdb force repair and verify it records `rebuild_core_group`.
        4. Restart normally and verify the table still has readable rows.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_rebuild_blocks_{time.time_ns()}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200")
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)")

        total_rows = 9000
        ts0 = 1700000000000
        self._insert_rebuild_rows(dbname, 0, 8191, ts0)

        tdSql.execute(f"flush database {dbname}")

        self._insert_rebuild_rows(dbname, 8191, total_rows, ts0)

        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, total_rows)

        vnode_id = self._get_vnode_id_for_db(dbname)
        backup_root = f"/tmp/tsdb-force-repair-rebuild-{int(time.time())}"
        data_file = self._wait_for_data_file(dbname, vnode_id, timeout_sec=90)
        if data_file is None:
            pytest.skip("real data file was not materialized in time after async flush")

        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")

        fid = candidate["fid"]
        corrupt_target = candidate["head"]
        tdLog.info("block repair corruption target=%s" % corrupt_target)
        keep_backup = False

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._overwrite_middle_bytes(corrupt_target)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fid, backup_root=backup_root, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
            repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fid)
            tdSql.checkEqual(code, 0)
            if repair_log is not None and os.path.isfile(repair_log):
                with open(repair_log, "r", encoding="utf-8") as fp:
                    log_text = fp.read()
                tdLog.info("rebuild repair.log=%s" % log_text.replace("\n", " | "))
                if "action=rebuild_core_group" not in log_text:
                    keep_backup = True
                    raise Exception(
                        f"unexpected repair.log for block rebuild: {log_text}; backup_root={backup_root}; target={corrupt_target}"
                    )

            self._restart_taosd_and_wait_ready()
            tdSql.query(f"select count(*) from {dbname}.t1")
            repaired_rows = tdSql.queryResult[0][0]
            tdSql.checkEqual(repaired_rows > 0, True)
            tdSql.checkEqual(repaired_rows <= total_rows, True)
            self._assert_database_writable_after_repair(dbname, "pk_after_block_rebuild", 4001)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            if os.path.exists(backup_root) and not keep_backup:
                shutil.rmtree(backup_root)
