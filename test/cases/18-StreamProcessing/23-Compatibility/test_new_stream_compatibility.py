import importlib.util
import os
import platform
import re
import shutil
import subprocess
import time
from pathlib import Path

from new_test_framework.utils import StreamItem, tdCb, tdCom, tdLog, tdSql, tdStream


BASE_VERSIONS = ["3.3.7.9", "3.3.8.5", "3.3.8.6", "3.4.1.0"]
DATABASE = "test_stream_compatibility"

current_dir = os.path.dirname(os.path.realpath(__file__))
enterprise_downloader_path = os.path.abspath(
    os.path.join(current_dir, "../../../../../taos-internal/utils/download_enterprise_package.py")
)
if not os.path.exists(enterprise_downloader_path):
    import pytest

    pytest.skip("Enterprise package downloader not available (community-only CI)", allow_module_level=True)

spec = importlib.util.spec_from_file_location("download_enterprise_package", enterprise_downloader_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load enterprise package downloader: {enterprise_downloader_path}")
download_enterprise_package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(download_enterprise_package)
downloader = download_enterprise_package.EnterprisePackageDownloader()


class TestNewStreamCompatibility:
    def setup_class(self):
        tdLog.info(f"start to execute {__file__}")
        self.old_bin_dir = None
        self.old_lib_dir = None

    def test_stream_compatibility(self):
        """Upgrade streams and data written by supported old releases.

        1. Start each old release, create streams, and verify its output.
        2. Upgrade the preserved data directory to the current build.
        3. Verify that the current build reads and runs those streams.

        Catalog:
            - Streams:Compatibility:Backward

        Since: v3.3.8.7

        Labels: common,ci,integration,functional,compatibility

        Jira: TD-38416

        History:
            - 2025-11-17 Tony Zhang created this case
            - 2026-08-12 Codex isolated old-version client execution and diagnostics

        """
        if self._is_unsupported_platform():
            return

        build_path = tdCom.getBuildPath()
        config_path = self.get_cfg_path()
        for base_version in BASE_VERSIONS:
            tdLog.printNoPrefix(f"========== Stream upgrade: {base_version} -> current ==========")
            self.start_old_version(config_path, base_version)
            self.prepare_old_version_data(base_version)

            tdCb.stopTaosdCompletely()
            tdCb.updateNewVersion(build_path, cPaths=[config_path], upgrade=2)
            self.start_streams_on_current_version()
            self.verify_current_version_data()
            tdCb.stopTaosdCompletely()

        tdLog.printNoPrefix("stream compatibility ......................... [ passed ]")

    def _is_unsupported_platform(self):
        if platform.system().lower() == "windows":
            tdLog.info("Windows skips stream compatibility test")
            return True
        try:
            import distro

            if distro.id() == "alpine":
                tdLog.info("Alpine skips stream compatibility test")
                return True
        except ImportError:
            pass
        return False

    def get_cfg_path(self):
        return os.path.join(tdCom.getBuildPath(), "../sim/dnode1/cfg/")

    def old_client_env(self):
        if not self.old_bin_dir or not self.old_lib_dir:
            raise RuntimeError("Old version is not initialized")
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self.old_lib_dir
        env["LD_PRELOAD"] = os.path.join(self.old_lib_dir, "libtaos.so")
        return env

    def old_server_env(self):
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self.old_lib_dir
        env.pop("LD_PRELOAD", None)
        return env

    def run_old_taos(self, sql=None, database=None, timeout=15):
        command = [os.path.join(self.old_bin_dir, "taos")]
        if database:
            command.extend(["-d", database])
        if sql is not None:
            sql = sql.strip()
            if not sql.endswith(";"):
                sql += ";"
            command.extend(["-s", sql])
        try:
            result = subprocess.run(
                command, env=self.old_client_env(), text=True, capture_output=True, timeout=timeout
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"Old taos timed out: {command}; output: {error.output!r}") from error
        if result.returncode:
            raise RuntimeError(
                f"Old taos failed ({result.returncode}): {sql}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result.stdout

    def verify_old_client_version(self, base_version):
        result = subprocess.run(
            [os.path.join(self.old_bin_dir, "taos"), "-V"],
            env=self.old_client_env(), text=True, capture_output=True, timeout=10
        )
        if result.returncode:
            raise RuntimeError(f"Cannot get old taos version:\n{result.stderr}")
        output = result.stdout + result.stderr
        version_match = re.search(
            r"(?:Native Client )?Version:\s*([^\s]+)|\b(?:taos|TDengine)\s+version\s+([^\s]+)",
            output,
            re.IGNORECASE,
        )
        actual_version = next((value for value in version_match.groups() if value), "unknown") if version_match else "unknown"
        if not actual_version.startswith(base_version):
            raise RuntimeError(
                f"Old taos binary loaded wrong client library: expected {base_version}, got {actual_version}. "
                f"bin={self.old_bin_dir}, lib={self.old_lib_dir}\n{output}"
            )
        tdLog.info(f"Verified old client version: {actual_version}")

    def start_old_version(self, config_path, base_version):
        self.old_bin_dir, self.old_lib_dir = downloader.download_and_extract(base_version, "enterprise")
        self.verify_old_client_version(base_version)
        tdCb.stopTaosdCompletely()
        self.clean_data_directory(config_path)

        taosd = os.path.join(self.old_bin_dir, "taosd")
        tdLog.info(f"Starting old taosd {base_version}: {taosd}")
        with open(os.devnull, "w") as devnull:
            subprocess.Popen([taosd, "-c", config_path], env=self.old_server_env(), stdout=devnull, stderr=devnull)
        time.sleep(5)
        self.wait_for_old_server()

    def clean_data_directory(self, config_path):
        data_path = Path(config_path).parent / "data"
        if not data_path.is_dir():
            return
        for child in data_path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def wait_for_old_server(self, retries=30):
        output = ""
        for _ in range(retries):
            try:
                output = self.run_old_taos(
                    "select status from information_schema.ins_dnodes", timeout=5
                )
            except RuntimeError as error:
                output = str(error)
            else:
                if output.count("ready") >= 1:
                    return
            time.sleep(1)
        raise RuntimeError(f"Old taosd did not become ready:\n{output}")

    def prepare_old_version_data(self, base_version):
        tdLog.info(f"Preparing streams and data on old version {base_version}")
        setup_sql = [
            "create snode on dnode 1",
            f"drop database if exists {DATABASE}",
            f"create database {DATABASE}",
            f"create table {DATABASE}.stb (ts timestamp, v1 int, v2 float) tags (gid int)",
            f"create table {DATABASE}.ctb1 using {DATABASE}.stb tags (1)",
            f"create table {DATABASE}.ctb2 using {DATABASE}.stb tags (1)",
            f"create stream {DATABASE}.s_count count_window(3) from {DATABASE}.stb partition by tbname "
            f"into {DATABASE}.res_count as select _twstart as ts, _twend as te, sum(v1) as sum_v1, "
            "avg(v2) as avg_v2 from %%tbname where ts >= _twstart and ts <= _twend",
            f"create stream {DATABASE}.s_state state_window(v1) from {DATABASE}.stb partition by tbname "
            f"into {DATABASE}.res_state as select _twstart as ts, _twend as te, sum(v1) as sum_v1, "
            "avg(v2) as avg_v2 from %%tbname where ts >= _twstart and ts <= _twend",
            f"create stream {DATABASE}.s_inter interval(3s) sliding(3s) from {DATABASE}.stb "
            f"into {DATABASE}.res_inter as select _twstart as ts, _twend as te, sum(v1) as sum_v1, "
            f"avg(v2) as avg_v2 from {DATABASE}.stb where ts >= _twstart and ts < _twend",
        ]
        for sql in setup_sql:
            self.run_old_taos(sql)

        self.wait_for_old_streams()
        self.run_old_taos(
            f"insert into {DATABASE}.ctb1 values "
            '("2025-11-17 12:00:00", 1, 1.2) ("2025-11-17 12:00:01", 1, 1.3) '
            '("2025-11-17 12:00:02", 2, 1.5) ("2025-11-17 12:00:03", 2, 1.7) '
            '("2025-11-17 12:00:04", 2, 1.9) ("2025-11-17 12:00:05", 2, 2.2) '
            '("2025-11-17 12:00:06", 1, 3.2) ("2025-11-17 12:00:07", 1, 4.2) '
            '("2025-11-17 12:00:08", 1, 7.2) ("2025-11-17 12:00:09", 2, 9.2)'
        )
        time.sleep(10)
        for table in ("res_count", "res_state", "res_inter"):
            self.wait_for_old_row_count(table, 3)
        self.stop_old_streams()

    def wait_for_old_streams(self, timeout_seconds=300):
        deadline = time.monotonic() + timeout_seconds
        output = ""
        while time.monotonic() < deadline:
            remaining = max(1, int(deadline - time.monotonic()))
            try:
                output = self.run_old_taos(
                    "select status from information_schema.ins_streams",
                    timeout=min(5, remaining),
                )
                if output.count("Running") == 3:
                    return
            except RuntimeError as error:
                output = str(error)
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        raise RuntimeError(f"Old streams did not all reach Running state:\n{output}")

    def wait_for_old_row_count(self, table, expected_rows, timeout_seconds=300):
        deadline = time.monotonic() + timeout_seconds
        output = ""
        while time.monotonic() < deadline:
            remaining = max(1, int(deadline - time.monotonic()))
            try:
                output = self.run_old_taos(
                    f"select * from {DATABASE}.{table}",
                    timeout=min(5, remaining),
                )
                match = re.search(r"Query OK, (\d+) row\(s\) in set", output)
                if match and int(match.group(1)) == expected_rows:
                    return
            except RuntimeError as error:
                output = str(error)
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        raise RuntimeError(
            f"Old stream result {DATABASE}.{table} did not reach {expected_rows} rows:\n{output}"
        )

    def stop_old_streams(self):
        output = self.run_old_taos("select stream_name from information_schema.ins_streams")
        names = [name for name in re.findall(r"^\s*(\S+)\s*\|", output, re.MULTILINE) if name != "stream_name"]
        for name in names:
            self.run_old_taos(f"stop stream {name}", database=DATABASE)
        time.sleep(5)
        output = self.run_old_taos("select stream_name, status from information_schema.ins_streams")
        not_stopped = re.findall(r"^\s*(\S+)\s*\|\s*(?!Stopped\s*\|)(\S+)", output, re.MULTILINE)
        not_stopped = [(name, status) for name, status in not_stopped if name != "stream_name"]
        if not_stopped:
            raise RuntimeError(f"Old streams did not stop: {not_stopped}")

    def start_streams_on_current_version(self):
        tdSql.execute(f"use {DATABASE}")
        tdSql.query("select stream_name from information_schema.ins_streams")
        for (name,) in tdSql.queryResult:
            tdSql.execute(f"start stream {name}")
        tdStream.checkStreamStatus()

    def verify_current_version_data(self):
        streams = [
            StreamItem(0,
                f"create stream {DATABASE}.s_count count_window(3) from {DATABASE}.stb partition by tbname into {DATABASE}.res_count as select _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname where ts >= _twstart and ts <= _twend",
                f"select ts, te, sum_v1, avg_v2 from {DATABASE}.res_count",
                f"select _wstart, _wend, sum(v1), avg(v2) from {DATABASE}.ctb1 count_window(3) limit 3"),
            StreamItem(1,
                f"create stream {DATABASE}.s_state state_window(v1) from {DATABASE}.stb partition by tbname into {DATABASE}.res_state as select _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname where ts >= _twstart and ts <= _twend",
                f"select ts, te, sum_v1, avg_v2 from {DATABASE}.res_state",
                f"select _wstart, _wend, sum(v1), avg(v2) from {DATABASE}.ctb1 state_window(v1) limit 3"),
            StreamItem(2,
                f"create stream {DATABASE}.s_inter interval(3s) sliding(3s) from {DATABASE}.stb into {DATABASE}.res_inter as select _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as avg_v2 from {DATABASE}.stb where ts >= _twstart and ts < _twend",
                f"select ts, te, sum_v1, avg_v2 from {DATABASE}.res_inter",
                f"select _wstart, _wend, sum(v1), avg(v2) from {DATABASE}.ctb1 interval(3s) sliding(3s) limit 3"),
        ]
        tdStream.checkStreamStatus()
        for stream in streams:
            stream.checkResults()
