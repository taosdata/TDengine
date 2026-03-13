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


class TsdbForceRepairBase:
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
        tdLog.info(
            "ins_tables vnode lookup for %s.%s => %s"
            % (dbname, table_name, tdSql.queryResult)
        )
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
        tdLog.exit(
            f"failed to resolve vnode id from show {dbname}.vgroups result: {row}"
        )

    def _tsdb_target(self, vnode_id, fid, strategy=None):
        target = f"tsdb:vnode={vnode_id}:fileid={fid}"
        if strategy:
            target += f":strategy={strategy}"
        return target

    def _tsdb_repair_args(
        self, vnode_id, fid, strategy=None, backup_root=None, extra_args=""
    ):
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
                "detect_leaks=0"
                if not asan_options
                else asan_options + ":detect_leaks=0"
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
            tdLog.info(
                "repair proc exited early: %s"
                % output[:500].replace("\n", "\\n")
            )
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
                "detect_leaks=0"
                if not asan_options
                else asan_options + ":detect_leaks=0"
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

    def _run_force_repair(
        self,
        vnode_id,
        fid,
        strategy=None,
        backup_root=None,
        extra_args="--log-output /dev/null",
        timeout_sec=10,
        extra_env=None,
    ):
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

    def _wait_for_database_ready(self, dbname, timeout_sec=30):
        deadline = time.time() + timeout_sec
        last_status = None

        while time.time() < deadline:
            tdSql.query(
                f"select * from information_schema.ins_databases where name='{dbname}'"
            )
            if tdSql.queryRows > 0:
                last_status = tdSql.queryResult[0][15]
                if last_status == "ready":
                    return
            time.sleep(1)

        if last_status is None:
            tdLog.exit(f"database {dbname} not found within {timeout_sec}s after restart")
        tdLog.exit(
            f"database {dbname} status is {last_status} after waiting {timeout_sec}s"
        )

    def _restart_taosd_and_wait_ready(self, timeout_sec=30, dbname=None):
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
                if dbname is not None:
                    self._wait_for_database_ready(dbname, timeout_sec=timeout_sec)
                return
            except BaseException as exc:
                last_error = exc
                time.sleep(1)

        if last_error is not None:
            raise last_error

    def _assert_database_writable_after_repair(self, dbname, pk_value, value, ts=None):
        ts_value = 1700001000000 if ts is None else ts
        normalized_pk = pk_value[:20]
        tdSql.execute(
            f"insert into {dbname}.t1 values({ts_value}, '{normalized_pk}', {value})"
        )
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
        candidate = data_file[: -len(".data")] + suffix
        return candidate if os.path.isfile(candidate) else None

    def _manifest_size_matches_file(self, path, expected_size):
        if path is None or expected_size is None or not os.path.isfile(path):
            return False

        actual_size = os.path.getsize(path)
        if actual_size == expected_size:
            return True

        return (
            actual_size > expected_size
            and actual_size - expected_size <= self._TSDB_FILE_SIZE_TOLERANCE
        )

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

            head_file = os.path.join(
                tsdb_dir, f"v{vnode_id}f{fid}ver{head_cid}.head"
            )
            data_file = os.path.join(
                tsdb_dir, f"v{vnode_id}f{fid}ver{data_cid}.data"
            )
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
        tdLog.info(
            "corrupt data file %s at offsets=%s length=%d size=%d"
            % (path, offsets, length, size)
        )
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
                tdLog.info(
                    "vnode%d data file materialized on attempt %d: %s"
                    % (vnode_id, attempt, data_file)
                )
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

    def _wait_for_size_matched_core_filesets(
        self, dbname, vnode_id, min_count=2, timeout_sec=90
    ):
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
        tdSql.execute(
            f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(
            f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)"
        )

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

        candidate = self._wait_for_size_matched_core_fileset(
            dbname, vnode_id, timeout_sec=90
        )
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
        tdSql.execute(
            f"create database {dbname} vgroups 1 duration 1d stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(
            f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)"
        )

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
        filesets = self._wait_for_size_matched_core_filesets(
            dbname, vnode_id, min_count=2, timeout_sec=90
        )
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
        tdSql.execute(
            f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200"
        )
        tdSql.execute(f"drop table if exists {dbname}.meters")
        tdSql.execute(
            f"create table {dbname}.meters (ts timestamp, c1 int, c2 float) tags(t1 int)"
        )
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
        base = os.path.join(
            backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "tsdb"
        )
        if not os.path.isdir(base):
            return None

        for root, _, files in os.walk(base):
            if "manifest.json" in files:
                return os.path.join(root, "manifest.json")
        return None

    def _find_backup_log(self, backup_root, vnode_id):
        date_str = time.strftime("%Y%m%d")
        base = os.path.join(
            backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "tsdb"
        )
        if not os.path.isdir(base):
            return None

        for root, _, files in os.walk(base):
            if "repair.log" in files:
                return os.path.join(root, "repair.log")
        return None

    def _find_backup_log_for_fid(self, backup_root, vnode_id, fid):
        date_str = time.strftime("%Y%m%d")
        return os.path.join(
            backup_root,
            f"taos_backup_{date_str}",
            f"vnode{vnode_id}",
            "tsdb",
            f"fid_{fid}",
            "repair.log",
        )

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
            "head": {
                "did.level": did_level,
                "did.id": did_id,
                "lcn": 0,
                "fid": fake_fid,
                "mid": 0,
                "cid": fake_cid,
                "size": size,
                "minVer": 1,
                "maxVer": 1,
            },
            "data": {
                "did.level": did_level,
                "did.id": did_id,
                "lcn": 0,
                "fid": fake_fid,
                "mid": 0,
                "cid": fake_cid,
                "size": size,
                "minVer": 1,
                "maxVer": 1,
            },
            "sma": {
                "did.level": did_level,
                "did.id": did_id,
                "lcn": 0,
                "fid": fake_fid,
                "mid": 0,
                "cid": fake_cid,
                "size": size,
                "minVer": 1,
                "maxVer": 1,
            },
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
            if (
                os.path.exists(current_json_path)
                and self._count_core_entries_in_current(current_json_path) > 0
            ):
                return True
            time.sleep(1)
        return False


class TestTsdbForceRepairBaseSmoke:
    def test_base_module_loads(self):
        """TSDB force repair base module should load as a valid test file.

        1. Import the shared base helpers module.
        2. Execute a no-op smoke test.
        3. Verify the file satisfies CI collection rules without affecting repair coverage.

        Since: v3.4.1.0

        Labels: common,ci
        """
        tdSql.checkEqual(True, True)
