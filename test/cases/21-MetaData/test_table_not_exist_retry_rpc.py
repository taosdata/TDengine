# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, etool, tdDnodes, tdCom

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


DB_NAME = "retry_meta_rpc"
TABLE_NAME = "stale_table"
USE_DB_LOG_MARKER = "process usedb req vgVersion:"
WORKER_RESULT_PREFIX = "TABLE_NOT_EXIST_WORKER_RESULT:"


WORKER_SCRIPT = r"""
import json
import os
import sys
import time

import taos


def write_json_atomic(path, value):
    temp_path = f"{path}.{os.getpid()}.tmp"
    with open(temp_path, "w", encoding="utf-8") as output:
        json.dump(value, output)
    os.replace(temp_path, path)


def wait_for_proceed(path):
    deadline = time.monotonic() + 60
    while not os.path.exists(path):
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for parent process")
        time.sleep(0.02)


mode = sys.argv[1]
client_cfg = sys.argv[2]
db_name = sys.argv[3]
table_name = sys.argv[4]
ready_path = sys.argv[5]
proceed_path = sys.argv[6]
worker_id = int(sys.argv[7])

conn = None
cursor = None
result = None
try:
    conn = taos.connect(config=client_cfg, database=db_name)
    cursor = conn.cursor()

    if mode == "stale-query":
        for _ in range(2):
            cursor.execute(f"select count(*) from {table_name}")
            cursor.fetchall()

        write_json_atomic(ready_path, {"worker_id": worker_id})
        wait_for_proceed(proceed_path)

        start_ns = time.perf_counter_ns()
        error_code = 0
        try:
            cursor.execute(f"select count(*) from {table_name}")
            cursor.fetchall()
        except Exception as err:
            error_code = getattr(err, "errno", -1)

        result = {
            "worker_id": worker_id,
            "elapsed_ns": time.perf_counter_ns() - start_ns,
            "error_code": error_code,
        }
    elif mode == "schema-refresh":
        cursor.execute(f"select old_v from {table_name}")
        write_json_atomic(ready_path, {"old_rows": cursor.fetchall()})
        wait_for_proceed(proceed_path)

        stale_error_code = 0
        try:
            cursor.execute(f"select old_v from {table_name}")
            cursor.fetchall()
        except Exception as err:
            stale_error_code = getattr(err, "errno", -1)

        query = (
            f"select new_v, note from {table_name} "
            "where new_v is not null"
        )
        cursor.execute(query)
        first_rows = cursor.fetchall()
        cursor.execute(query)
        cached_rows = cursor.fetchall()
        result = {
            "stale_error_code": stale_error_code,
            "first_rows": first_rows,
            "cached_rows": cached_rows,
        }
    else:
        raise ValueError(f"unsupported worker mode: {mode}")
finally:
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()

print("TABLE_NOT_EXIST_WORKER_RESULT:" + json.dumps(result), flush=True)
"""


class TestTableNotExistRetryRpc:
    updatecfgDict = {"mDebugFlag": 135, "asyncLog": 0}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    @staticmethod
    def log_prefix_digest(log_file, size):
        digest = hashlib.sha256()
        while size:
            chunk = log_file.read(min(size, 1024 * 1024))
            if not chunk:
                raise AssertionError("taosd log truncated while reading RPC evidence")
            digest.update(chunk)
            size -= len(chunk)
        return digest.digest()

    @classmethod
    def snapshot_logs(cls, log_dir):
        snapshot = {}
        for index in range(2):
            path = os.path.join(log_dir, f"taosdlog.{index}")
            try:
                log_file = open(path, "rb")
            except FileNotFoundError:
                snapshot[path] = None
                continue
            with log_file:
                stat = os.fstat(log_file.fileno())
                digest = cls.log_prefix_digest(log_file, stat.st_size)
                snapshot[path] = (stat.st_dev, stat.st_ino, stat.st_size, digest)
        if not any(value is not None for value in snapshot.values()):
            raise AssertionError(f"no taosd logs available for RPC counting in {log_dir}")
        return snapshot

    @classmethod
    def count_log_marker(cls, snapshot, marker):
        count = 0
        for path, baseline in snapshot.items():
            try:
                log_file = open(path, "rb")
            except FileNotFoundError:
                if baseline is not None:
                    raise AssertionError(f"taosd log disappeared; RPC evidence incomplete: {path}")
                continue
            with log_file:
                stat = os.fstat(log_file.fileno())
                offset = 0
                if baseline is not None:
                    device, inode, offset, digest = baseline
                    if (stat.st_dev, stat.st_ino) != (device, inode) or stat.st_size < offset:
                        raise AssertionError(f"taosd log replaced/truncated; RPC evidence incomplete: {path}")
                    if cls.log_prefix_digest(log_file, offset) != digest:
                        raise AssertionError(f"taosd log overwritten; RPC evidence incomplete: {path}")
                # A new numbered file is a rotation we can observe from its start.
                content = log_file.read(stat.st_size - offset)
                if len(content) != stat.st_size - offset:
                    raise AssertionError(f"taosd log truncated during RPC counting: {path}")
                count += content.count(marker.encode())
        return count

    @staticmethod
    def percentile(values, percent):
        ordered = sorted(values)
        index = min(len(ordered) - 1, int(len(ordered) * percent))
        return ordered[index]

    @classmethod
    def start_worker(cls, mode, client_cfg, ready_path, proceed_path, worker_id):
        return subprocess.Popen(
            [
                sys.executable,
                "-c",
                WORKER_SCRIPT,
                mode,
                client_cfg,
                DB_NAME,
                TABLE_NAME,
                ready_path,
                proceed_path,
                str(worker_id),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    @staticmethod
    def check_worker_diagnostics(stderr):
        if not stderr:
            return False

        # Preserve raw frames for the outer CI checker, including successful workers.
        sys.stderr.write(stderr)
        sys.stderr.flush()
        if any(marker in stderr for marker in (
            "ERROR: AddressSanitizer:", "AddressSanitizer:DEADLYSIGNAL",
            "UndefinedBehaviorSanitizer:", "LeakSanitizer has encountered a fatal error",
            "runtime error:", "Traceback (most recent call last):",
        )):
            raise AssertionError("worker reported a runtime error")

        if "ERROR: LeakSanitizer: detected memory leaks" not in stderr:
            return False

        # Python shutdown leaks can set a nonzero exit code. Apply the same
        # ownership filter as CI instead of disabling libtaos leak detection.
        test_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        checker = os.path.join(test_dir, "ci", "checkAsan.sh")
        with tempfile.TemporaryDirectory(prefix="retry-worker-asan-") as work_dir:
            asan_dir = os.path.join(work_dir, "asan")
            os.mkdir(asan_dir)
            with open(os.path.join(asan_dir, "worker.info"), "w", encoding="utf-8") as report:
                report.write(stderr)
            with open(os.path.join(asan_dir, "worker.asan"), "w", encoding="utf-8"):
                pass
            env = os.environ.copy()
            env.pop("LD_PRELOAD", None)
            env["WORK_DIR"] = work_dir
            checked = subprocess.run(
                ["bash", checker], env=env, capture_output=True, text=True, timeout=30,
            )
            if checked.returncode != 0:
                raise AssertionError(
                    f"worker sanitizer check failed: {checked.stdout}\n{checked.stderr}"
                )
        return True

    @staticmethod
    def wait_for_ready(workers, ready_paths):
        deadline = time.monotonic() + 90
        while not all(os.path.exists(path) for path in ready_paths):
            for worker, ready_path in zip(workers, ready_paths):
                if os.path.exists(ready_path) or worker.poll() is None:
                    continue
                stdout, stderr = worker.communicate()
                sys.stderr.write(stderr)
                sys.stderr.flush()
                worker.diagnostics_forwarded = True
                raise AssertionError(
                    f"worker {worker.pid} exited before ready, "
                    f"code={worker.returncode}, stdout={stdout!r}, "
                    f"stderr={stderr!r}"
                )

            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for worker ready signals")
            time.sleep(0.05)

        ready_values = []
        for ready_path in ready_paths:
            with open(ready_path, "r", encoding="utf-8") as ready_file:
                ready_values.append(json.load(ready_file))
        return ready_values

    @classmethod
    def receive_worker_result(cls, worker, timeout):
        try:
            stdout, stderr = worker.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as err:
            worker.terminate()
            try:
                stdout, stderr = worker.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                stdout, stderr = worker.communicate()
            sys.stderr.write(stderr)
            sys.stderr.flush()
            worker.diagnostics_forwarded = True
            raise TimeoutError(
                f"worker {worker.pid} timed out, stdout={stdout!r}, "
                f"stderr={stderr!r}"
            ) from err

        worker.diagnostics_forwarded = True
        ignored_python_leaks = cls.check_worker_diagnostics(stderr)
        if worker.returncode != 0 and not (worker.returncode > 0 and ignored_python_leaks):
            raise AssertionError(
                f"worker {worker.pid} exited with code {worker.returncode}, "
                f"stdout={stdout!r}, stderr={stderr!r}"
            )

        for line in reversed(stdout.splitlines()):
            if line.startswith(WORKER_RESULT_PREFIX):
                return json.loads(line[len(WORKER_RESULT_PREFIX) :])

        raise AssertionError(
            f"worker {worker.pid} returned no result, "
            f"stdout={stdout!r}, stderr={stderr!r}"
        )

    @staticmethod
    def signal_proceed(path):
        with open(path, "w", encoding="utf-8") as proceed_file:
            proceed_file.write("go")

    @staticmethod
    def stop_workers(workers):
        for worker in workers:
            if worker.poll() is None:
                worker.terminate()

        for worker in workers:
            try:
                _, stderr = worker.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                _, stderr = worker.communicate()
            if not getattr(worker, "diagnostics_forwarded", False):
                sys.stderr.write(stderr)
                sys.stderr.flush()
                worker.diagnostics_forwarded = True

    # --- impl ---
    def prepare(self):
        tdSql.execute(f"drop database if exists {DB_NAME}")
        tdSql.execute(f"create database {DB_NAME} vgroups 4")
        tdSql.execute(
            f"create table {DB_NAME}.{TABLE_NAME} (ts timestamp, old_v int)"
        )
        tdSql.execute(f"insert into {DB_NAME}.{TABLE_NAME} values(now, 1)")
        print("prepare stale table .................. [ passed ]")

    def do_table_not_exist_rpc_pressure(self):
        worker_count = int(os.getenv("TABLE_NOT_EXIST_WORKERS", "8"))
        if worker_count < 1 or worker_count > 64:
            raise ValueError(
                f"TABLE_NOT_EXIST_WORKERS out of range: {worker_count}"
            )
        expected_max_use_db = int(os.getenv("TABLE_NOT_EXIST_MAX_USE_DB", "0"))
        client_cfg = tdCom.getClientCfgPath()
        results = []
        with tempfile.TemporaryDirectory(prefix="table-not-exist-workers-") as work_dir:
            proceed_path = os.path.join(work_dir, "proceed")
            ready_paths = [
                os.path.join(work_dir, f"ready-{worker_id}.json")
                for worker_id in range(worker_count)
            ]
            workers = []
            try:
                for worker_id, ready_path in enumerate(ready_paths):
                    workers.append(
                        self.start_worker(
                            "stale-query",
                            client_cfg,
                            ready_path,
                            proceed_path,
                            worker_id,
                        )
                    )

                self.wait_for_ready(workers, ready_paths)
                tdSql.execute(f"drop table {DB_NAME}.{TABLE_NAME}")
                log_snapshot = self.snapshot_logs(tdDnodes.dnodes[0].logDir)
                self.signal_proceed(proceed_path)

                deadline = time.monotonic() + 90
                for worker in workers:
                    timeout = max(0.1, deadline - time.monotonic())
                    results.append(self.receive_worker_result(worker, timeout))
            finally:
                if not os.path.exists(proceed_path):
                    self.signal_proceed(proceed_path)
                self.stop_workers(workers)

        use_db_count = self.count_log_marker(
            log_snapshot, USE_DB_LOG_MARKER
        )
        latency_ms = [result["elapsed_ns"] / 1_000_000 for result in results]
        error_codes = [result["error_code"] for result in results]
        avg_ms = sum(latency_ms) / len(latency_ms)
        p95_ms = self.percentile(latency_ms, 0.95)

        tdLog.info(
            "table-not-exist retry metrics: workers=%d, use-db=%d, "
            "avg=%.3fms, p95=%.3fms",
            worker_count,
            use_db_count,
            avg_ms,
            p95_ms,
        )
        if any(code == 0 for code in error_codes):
            raise AssertionError(f"all stale queries must fail: {error_codes}")
        if use_db_count > expected_max_use_db:
            raise AssertionError(
                f"expected at most {expected_max_use_db} USE DB RPCs, "
                f"got {use_db_count} for {worker_count} stale queries"
            )

        print("table-only retry RPC pressure ........ [ passed ]")

    def do_recreated_table_meta_refresh(self):
        with tempfile.TemporaryDirectory(prefix="schema-refresh-worker-") as work_dir:
            proceed_path = os.path.join(work_dir, "proceed")
            ready_path = os.path.join(work_dir, "ready.json")
            worker = self.start_worker(
                "schema-refresh",
                tdCom.getClientCfgPath(),
                ready_path,
                proceed_path,
                0,
            )
            try:
                ready = self.wait_for_ready([worker], [ready_path])[0]
                if ready["old_rows"] != [[1]]:
                    raise AssertionError(
                        f"unexpected old table rows: {ready['old_rows']}"
                    )

                tdSql.execute(f"drop table {DB_NAME}.{TABLE_NAME}")
                tdSql.execute(
                    f"create table {DB_NAME}.{TABLE_NAME} "
                    "(ts timestamp, new_v bigint, note varchar(32))"
                )
                tdSql.execute(
                    f"insert into {DB_NAME}.{TABLE_NAME} "
                    "values(now, 42, 'new-meta')"
                )

                log_snapshot = self.snapshot_logs(tdDnodes.dnodes[0].logDir)
                self.signal_proceed(proceed_path)
                result = self.receive_worker_result(worker, 90)
            finally:
                if not os.path.exists(proceed_path):
                    self.signal_proceed(proceed_path)
                self.stop_workers([worker])

        stale_error_code = result["stale_error_code"]
        first_rows = result["first_rows"]
        cached_rows = result["cached_rows"]
        if stale_error_code == 0:
            raise AssertionError("query using removed old_v column must fail")
        expected_rows = [[42, "new-meta"]]
        if first_rows != expected_rows:
            raise AssertionError(f"new table meta was not applied: {first_rows}")
        if cached_rows != expected_rows:
            raise AssertionError(f"new table meta was not cached: {cached_rows}")

        use_db_count = self.count_log_marker(
            log_snapshot, USE_DB_LOG_MARKER
        )
        tdLog.info(
            "recreated-table meta refresh metrics: use-db=%d, "
            "stale-error=%d",
            use_db_count,
            stale_error_code,
        )
        if use_db_count != 0:
            raise AssertionError(
                f"expected no USE DB RPC while refreshing new meta, "
                f"got {use_db_count}"
            )

        print("recreated table meta refresh ........ [ passed ]")

    # --- main ---
    def do_log_rpc_observation(self):
        check_error = unittest.TestCase().assertRaisesRegex
        with tempfile.TemporaryDirectory(prefix="retry-rpc-logs-") as log_dir:
            log0 = Path(log_dir) / "taosdlog.0"
            log1 = Path(log_dir) / "taosdlog.1"
            marker = (USE_DB_LOG_MARKER + "\n").encode()
            with check_error(AssertionError, "no taosd logs"):
                self.snapshot_logs(log_dir)

            # .1 can be the only active log; old records must not be counted.
            log1.write_bytes(marker)
            snapshot = self.snapshot_logs(log_dir)
            assert self.count_log_marker(snapshot, USE_DB_LOG_MARKER) == 0
            with log1.open("ab") as output:
                output.write(marker)
            assert self.count_log_marker(snapshot, USE_DB_LOG_MARKER) == 1

            # Rotation into a new file preserves evidence in both numbered files.
            log0.write_bytes(marker)
            assert self.count_log_marker(snapshot, USE_DB_LOG_MARKER) == 2
            snapshot = self.snapshot_logs(log_dir)
            with log0.open("ab") as output:
                output.write(marker)
            assert self.count_log_marker(snapshot, USE_DB_LOG_MARKER) == 1

            # Truncate-and-regrow may reuse the inode and exceed the old offset.
            log0.write_bytes(b"x" * (len(marker) * 3))
            with check_error(AssertionError, "overwritten"):
                self.count_log_marker(snapshot, USE_DB_LOG_MARKER)
            log0.write_bytes(b"")
            with check_error(AssertionError, "truncated"):
                self.count_log_marker(snapshot, USE_DB_LOG_MARKER)

            log0.unlink()
            with check_error(AssertionError, "disappeared"):
                self.count_log_marker(snapshot, USE_DB_LOG_MARKER)
            snapshot = self.snapshot_logs(log_dir)
            replacement = Path(log_dir) / "replacement"
            replacement.write_bytes(marker * 2)
            replacement.replace(log1)
            with check_error(AssertionError, "replaced"):
                self.count_log_marker(snapshot, USE_DB_LOG_MARKER)

        print("RPC log observation ............................................. [ passed ]")

    def test_log_rpc_observation(self):
        """Observe RPC logs across rotation without false zero counts

        1. Count appended records in either numbered log and in a new rotation
        2. Reject missing, replaced, truncated, or overwritten log evidence

        Catalog:
            - MetaData

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-16 Codex Verify RPC log observation under rotation

        """
        self.do_log_rpc_observation()

    def test_table_not_exist_retry_rpc(self):
        """Avoid USE DB amplification during table-not-exist retry

        1. Warm table metadata in independent client processes
        2. Drop the table and trigger concurrent stale-cache retries
        3. Assert retries fail correctly without sending USE DB to mnode

        Catalog:
            - MetaData

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 Codex Add table-only retry RPC pressure coverage
            - 2026-08-21 Codex Isolate workers from Python LSAN noise

        """
        self.prepare()
        self.do_table_not_exist_rpc_pressure()

    def test_recreated_table_meta_refresh(self):
        """Refresh and cache new meta after recreating a stale table

        1. Warm old table metadata in an independent client process
        2. Drop and recreate the same table name with a different schema
        3. Verify stale-column failure and two successful new-schema queries
        4. Verify table-only refresh does not send USE DB to mnode

        Catalog:
            - MetaData

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 Codex Verify recreated table meta cache refresh
            - 2026-08-21 Codex Isolate workers from Python LSAN noise

        """
        self.prepare()
        self.do_recreated_table_meta_refresh()
