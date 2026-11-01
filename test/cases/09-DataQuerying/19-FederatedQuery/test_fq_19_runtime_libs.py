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

import glob
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import pytest
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from new_test_framework.utils import tdCom, tdDnodes, tdLog, tdSql

from federated_query_common import ExtSrcEnv, FederatedQueryTestMixin, _code, _fq_subprocess_env


TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT = _code("TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT")
_PROVIDER_RUNTIME_PATTERNS = {
    "mysql": ("libmariadb*", "libmysqlclient*"),
    "postgresql": ("libpq*",),
    "influxdb": ("libtaos_ext_influx_arrow*", "libarrow*", "libparquet*"),
    # SQL Server loads unixODBC at runtime and resolves the Microsoft ODBC
    # Driver 18 from the same runtime dir (both are staged like the other
    # providers' client libraries, DS 3.3).
    "sqlserver": ("libodbc*", "libmsodbcsql*"),
    # Oracle has the same shape: unixODBC plus the Oracle ODBC driver, which in
    # turn resolves the Instant Client base libraries by soname from that same
    # directory (Oracle DS 3.3 / 9.1.7).
    "oracle": ("libodbc*", "libsqora*", "libclntsh*", "libnnz*", "libons*",
               "libnfsodm*", "liborazt*", "libociei*"),
}
_PROVIDER_PLUGIN_PATTERNS = {
    "influxdb": ("libtaos_ext_influx_arrow*",),
}
_PROVIDER_RESOLVED_DEP_PREFIXES = {
    "influxdb": ("libarrow", "libparquet"),
}
_EXT_ENV_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ensure_ext_env.sh")


def _check_pymysql_dependency(tmp_path, module_available=True, python_available=True):
    module = tmp_path / "pymysql.py"
    module.write_text("" if module_available else "raise ImportError('PyMySQL unavailable')\n")
    env = _fq_subprocess_env()
    env["FQ_PYTHON"] = sys.executable if python_available else str(tmp_path / "missing-python")
    env["PYTHONPATH"] = str(tmp_path)
    tdLog.debug(
        f"checking PyMySQL dependency: module_available={module_available}, "
        f"python_available={python_available}"
    )
    return subprocess.run(
        [
            "bash",
            "-c",
            'FQ_SOURCE_ONLY=1 source "$1"; '
            '_check_pymysql_dependency',
            "bash",
            _EXT_ENV_SCRIPT,
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _apply_runtime_exports(output):
    for line in output.splitlines():
        if not line.startswith("export "):
            continue
        key, value = line[len("export "):].split("=", 1)
        if key == "DYLD_LIBRARY_PATH" and sys.platform != "darwin":
            continue
        os.environ[key] = value.replace(f"${key}", os.environ.get(key, ""))


def _prepare_runtime_libs():
    if sys.platform == "win32":
        # Windows stages the provider DLLs next to taosd at build time, so
        # there is nothing to prepare or export before the engine starts.
        return
    here = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(here, "ensure_fq_runtime_libs.sh")
    env = _fq_subprocess_env()
    result = subprocess.run(
        ["bash", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(result.stdout)
    result.check_returncode()
    _apply_runtime_exports(result.stdout)


def _try_prepare_runtime_libs():
    try:
        _prepare_runtime_libs()
        return True
    except subprocess.CalledProcessError as err:
        tdLog.info(
            "runtime libs unavailable; only checking CREATE failure paths: "
            f"{err}"
        )
        return False


def _check_provider_missing(src, sql):
    tdSql.execute(f"drop external source if exists {src}")
    tdSql.error(sql, expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT)


_ODBCINST_REGISTRY_ROOTS = (
    r"SOFTWARE\ODBC\ODBCINST.INI",
    r"SOFTWARE\WOW6432Node\ODBC\ODBCINST.INI",
)


def _windows_oracle_driver_registrations():
    """(name, driver_path) of every Oracle Instant Client registered on Windows.

    The driver manager resolves ``Driver={...}`` only through these entries,
    and the provider reads the same list back to learn the driver name and its
    directory, so this is the Windows equivalent of odbcinst.ini.
    """
    try:
        import winreg
    except ImportError:
        return []
    registrations = []
    for root_path in _ODBCINST_REGISTRY_ROOTS:
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, root_path, 0,
                                winreg.KEY_READ) as root:
                index = 0
                while True:
                    try:
                        name = winreg.EnumKey(root, index)
                    except OSError:
                        break
                    index += 1
                    try:
                        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                            root_path + "\\" + name, 0,
                                            winreg.KEY_READ) as key:
                            driver, _ = winreg.QueryValueEx(key, "Driver")
                    except OSError:
                        continue
                    base = os.path.basename(str(driver).replace("\\", "/")).lower()
                    if base.startswith("sqora") and base.endswith(".dll"):
                        registrations.append((name, str(driver)))
        except OSError:
            continue
    return registrations


def _odbcinst_registers_oracle_driver():
    """True when the host registers the Oracle ODBC driver in odbcinst.

    Mirrors ``_odbcinst_registers_driver18``: the provider resolves the driver
    by absolute path first and deliberately does not require a registration,
    but an existing one is used as the fallback driver name, so hiding the
    driver library alone does not make Oracle unreachable on such hosts.
    """
    if sys.platform == "win32":
        # Windows keeps the registrations in the registry, and there the
        # provider resolves the driver *only* through them.
        return bool(_windows_oracle_driver_registrations())
    for path in ("/etc/odbcinst.ini",
                 "/usr/local/etc/odbcinst.ini",
                 os.path.expanduser("~/.odbcinst.ini")):
        try:
            with open(path, encoding="utf-8", errors="ignore") as fh:
                text = fh.read()
        except OSError:
            continue
        if re.search(r"^\s*\[Oracle", text, re.M):
            return True
    return False


def _odbcinst_registers_driver18():
    """True when the host registers 'ODBC Driver 18 for SQL Server' in odbcinst.

    The provider resolves the driver by absolute path first and deliberately
    does not require a registration (DS 3.3); but when a registration exists it
    is used as the fallback driver name, so hiding libmsodbcsql alone does not
    make SQL Server unreachable on such hosts.
    """
    if sys.platform == "win32":
        # Windows keeps the driver registrations in the registry rather than
        # odbcinst.ini; the MSI installs 'ODBC Driver 18 for SQL Server' under
        # ODBCINST.INI\\ODBC Drivers.
        try:
            import winreg
        except ImportError:
            return False
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                with winreg.OpenKey(
                        hive, r"SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers") as key:
                    value, _ = winreg.QueryValueEx(
                        key, "ODBC Driver 18 for SQL Server")
                    if value:
                        return True
            except OSError:
                continue
        return False
    for path in ("/etc/odbcinst.ini",
                 "/usr/local/etc/odbcinst.ini",
                 os.path.expanduser("~/.odbcinst.ini")):
        try:
            with open(path, encoding="utf-8", errors="ignore") as fh:
                text = fh.read()
        except OSError:
            continue
        if re.search(r"^\s*\[ODBC Driver 18 for SQL Server\]", text, re.M):
            return True
    return False


class TestFq19RuntimeLibs(FederatedQueryTestMixin):
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.mark.parametrize("action", ["start", "stop", "kill"])
    @pytest.mark.parametrize("service,version", [("mysql", "8.0"), ("pg", "16"),
                                                ("influx", "3.0"), ("sqlserver", "2022"),
                                                ("oracle", "26ai")])
    def test_external_lifecycle_requires_lease_controller(self, monkeypatch, action,
                                                        service, version):
        """External fault operations fail explicitly without a lease owner.

        1. Reject start, stop and kill when no lifecycle controller is configured.

        Catalog:
            - FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-10 Simon Guan Add external lifecycle coverage

        """
        self.do_external_lifecycle_requires_lease_controller(monkeypatch, action, service, version)

    def do_external_lifecycle_requires_lease_controller(self, monkeypatch, action, service, version):
        monkeypatch.setenv("FQ_ENV_MODE", "external")
        monkeypatch.delenv("FQ_SERVICE_CONTROL", raising=False)
        monkeypatch.delenv("FQ_SERVICE_SOCKET", raising=False)
        with pytest.raises(RuntimeError, match="FQ_SERVICE_CONTROL"):
            getattr(ExtSrcEnv, f"{action}_{service}_instance")(version)
        print("external lifecycle guard ......................... [ passed ]")

    def test_external_environment_does_not_reset_local_services(self, monkeypatch):
        """Repeated external setup preserves the leased database environment.

        1. Probe configured databases without invoking the local reset installer.

        Catalog:
            - FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-10 Simon Guan Add external environment preservation coverage

        """
        self.do_external_environment_does_not_reset_local_services(monkeypatch)

    def do_external_environment_does_not_reset_local_services(self, monkeypatch):
        monkeypatch.setenv("FQ_ENV_MODE", "external")
        import pymysql
        import psycopg2
        import requests

        # This mode test must not require databases before the first full case
        # provisions them in local-mode CI. Real probes run in the full cases.
        monkeypatch.setattr(pymysql, "connect", MagicMock())
        monkeypatch.setattr(psycopg2, "connect", MagicMock())
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: MagicMock(status_code=200))
        # Oracle has no Python driver here: the probe runs a statement through
        # the lease controller, which this test deliberately does not configure.
        monkeypatch.setattr(ExtSrcEnv, "oracle_query_cfg", lambda *args, **kwargs: "1")
        # SQL Server is probed with a raw TCP connect. A leased environment is
        # not guaranteed to be reachable from this worker (and a previous case
        # may have stopped the local engine), while this test only exercises
        # the external-mode guards, never connectivity.
        monkeypatch.setattr(ExtSrcEnv, "_tcp_probe", lambda *args, **kwargs: True)
        monkeypatch.setattr(ExtSrcEnv, "ensure_qnode", lambda: None)
        monkeypatch.setattr(ExtSrcEnv, "purge_all_external_sources", lambda: None)
        def reject_local_script(*args, **kwargs):
            raise AssertionError("external mode invoked a local reset/teardown script")

        monkeypatch.setattr(subprocess, "call", reject_local_script)
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.teardown_env()
        print("external environment preservation ......................... [ passed ]")

    def test_external_baseline_preserves_result_and_endpoint_errors(self, monkeypatch):
        """Canonicalize deployment parameters while retaining regression evidence.

        1. Map fixture endpoints without masking wrong addresses or query values.

        Catalog:
            - FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-10 Simon Guan Cover deployment-independent baseline comparison

        """
        self.do_external_baseline_preserves_result_and_endpoint_errors(monkeypatch)

    def do_external_baseline_preserves_result_and_endpoint_errors(self, monkeypatch):
        monkeypatch.setenv("FQ_ENV_MODE", "external")
        monkeypatch.setattr(ExtSrcEnv, "MYSQL_HOST", "mysql")
        monkeypatch.setattr(ExtSrcEnv, "MYSQL_PORT", 3306)
        monkeypatch.setattr(ExtSrcEnv, "MYSQL_PASS", "lease-password")
        text = ("SQL: create external source s type='mysql' host='mysql' port=3306 "
                "password='lease-password'\n"
                "source_name|type|host|port|user|password|database\n"
                "s|mysql|mysql|3306|root|******|db\n---\n"
                "SQL: create external source bad host='wrong' port=3307 password='bad'\n"
                "SQL: select host from information_schema.ins_ext_sources\n"
                "host\nmysql\nwrong\n---\n"
                "      Source: mysql:3306 user=root\n"
                "      Source: wrong:3307 user=root\n"
                "SQL: select host from ordinary_table\n"
                "source_name|type|host\ns|mysql|mysql\n---\n"
                "RESULT:\nmysql\n3306\n")
        normalized = ExtSrcEnv.baseline_environment(text)
        assert "host='127.0.0.1' port=13306 password='taosdata'" in normalized
        assert "s|mysql|127.0.0.1|13306|root|******|db" in normalized
        assert "host='wrong' port=3307 password='bad'" in normalized
        assert "host\n127.0.0.1\nwrong\n" in normalized
        assert "Source: 127.0.0.1:13306 user=root" in normalized
        assert "Source: wrong:3307 user=root" in normalized
        assert "source_name|type|host\ns|mysql|mysql\n" in normalized
        assert normalized.endswith("RESULT:\nmysql\n3306\n")
        monkeypatch.setattr(ExtSrcEnv, "SQLSERVER_HOST", "sqlserver")
        monkeypatch.setattr(ExtSrcEnv, "SQLSERVER_PORT", 25003)
        monkeypatch.setattr(ExtSrcEnv, "SQLSERVER_PASS", "private-sqlserver-password")
        sqlserver_text = (
            "SQL: create external source s type='sqlserver' host='sqlserver' "
            "port=25003 password='private-sqlserver-password'\n"
            "SQL: create external source bad host='wrong' port=1434 password='bad'\n"
            "RESULT:\nsqlserver\n25003\n")
        sqlserver_normalized = ExtSrcEnv.baseline_environment(sqlserver_text)
        assert "host='127.0.0.1' port=1433 password='TaosFq!2026'" in sqlserver_normalized
        assert "host='wrong' port=1434 password='bad'" in sqlserver_normalized
        assert sqlserver_normalized.endswith("RESULT:\nsqlserver\n25003\n")
        monkeypatch.setenv("FQ_ENV_MODE", "local")
        assert ExtSrcEnv.baseline_environment(text) == text
        print("external baseline normalization ......................... [ passed ]")

    def test_external_lifecycle_delegates_without_shell(self, monkeypatch):
        """Pass the lifecycle operation to one explicit controller executable.

        1. Delegate the service operation and version without shell evaluation.

        Catalog:
            - FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-10 Simon Guan Add controller delegation coverage

        """
        self.do_external_lifecycle_delegates_without_shell(monkeypatch)

    def do_external_lifecycle_delegates_without_shell(self, monkeypatch):
        monkeypatch.setenv("FQ_ENV_MODE", "external")
        monkeypatch.setenv("FQ_SERVICE_CONTROL", "/runner/lease controller")
        monkeypatch.delenv("FQ_SERVICE_SOCKET", raising=False)
        calls = []
        monkeypatch.setattr(
            subprocess, "run", lambda args, **kwargs: calls.append((args, kwargs)))
        ExtSrcEnv.stop_mysql_instance("8.0")
        assert calls[0][0] == ["/runner/lease controller", "stop", "mysql", "8.0"]
        assert calls[0][1]["check"] is True
        assert not calls[0][1].get("shell", False)
        print("external lifecycle delegation ......................... [ passed ]")

    def test_prepare_runtime_libs_sanitizes_shell_helper_env(self, monkeypatch):
        """Sanitize inherited loader and sanitizer variables before setup.

        Verify that external-source setup does not inherit TDengine runtime
        overrides that could affect foreign binaries.

        Since: v3.4.2.0

        """
        if sys.platform == "win32":
            # The shell helper does not exist on Windows: the provider DLLs are
            # staged next to the engine and _prepare_runtime_libs() is a no-op.
            pytest.skip("shell-helper environment sanitising is Unix-only")
        for name in ("LD_PRELOAD", "ASAN_OPTIONS", "LSAN_OPTIONS", "UBSAN_OPTIONS"):
            monkeypatch.setenv(name, f"poison-{name}")
        for name in (
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
        ):
            monkeypatch.delenv(name, raising=False)

        captured = {}

        class Result:
            stdout = "export LD_LIBRARY_PATH=/tmp/build/lib:$LD_LIBRARY_PATH\n"

            def check_returncode(self):
                pass

        def fake_run(*args, **kwargs):
            captured["env"] = kwargs["env"]
            return Result()

        monkeypatch.setattr(subprocess, "run", fake_run)
        _prepare_runtime_libs()

        for name in ("LD_PRELOAD", "ASAN_OPTIONS", "LSAN_OPTIONS", "UBSAN_OPTIONS"):
            assert name not in captured["env"]

    def do_accept_available_pymysql(self, tmp_path):
        result = _check_pymysql_dependency(tmp_path)

        assert result.returncode == 0, result.stderr
        tdLog.info("accept available PyMySQL dependency ......... [ passed ]")

    def do_report_missing_pymysql(self, tmp_path):
        result = _check_pymysql_dependency(tmp_path, module_available=False)

        assert result.returncode != 0
        assert "MySQL setup requires the pymysql module" in result.stderr
        assert f"{sys.executable} -m pip install pymysql" in result.stderr
        tdLog.info("report missing PyMySQL dependency .......... [ passed ]")

    def do_report_missing_python_interpreter(self, tmp_path):
        result = _check_pymysql_dependency(tmp_path, python_available=False)

        assert result.returncode != 0
        assert "MySQL setup requires Python, but the interpreter was not found" in result.stderr
        assert "Set FQ_PYTHON to the Python interpreter used by pytest" in result.stderr
        tdLog.info("report missing Python interpreter .......... [ passed ]")

    def test_pymysql_dependency(self, tmp_path):
        """Check the Python dependencies used by MySQL setup

        1. Accept PyMySQL in the configured Python interpreter
        2. Report installation instructions when PyMySQL cannot be imported
        3. Report configuration instructions when the interpreter is missing

        Catalog:
            - FederatedQuery

        Since: v3.4.0.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-06 Wang Mingming Add MySQL client fallback coverage
            - 2026-09-22 Wang Mingming Cover the retained PyMySQL dependency path

        """
        if sys.platform == "win32":
            # The Linux scenarios drive the bash setup script; on Windows the
            # requirement is simply that the interpreter can import PyMySQL.
            import importlib.util
            assert importlib.util.find_spec("pymysql") is not None, (
                "PyMySQL is required for the MySQL fixtures: pip install pymysql")
            return
        self.do_accept_available_pymysql(tmp_path)
        self.do_report_missing_pymysql(tmp_path)
        self.do_report_missing_python_interpreter(tmp_path)

    def test_taos_cli_env_disables_leak_detection_for_asan_child(self, monkeypatch):
        """Disable leak detection for ASan child processes.

        Verify that the taos CLI environment preserves preload settings while
        disabling leak detection in ASan and LSan options.

        Since: v3.4.2.0

        """
        monkeypatch.setenv("CI_ASAN_BUILD", "1")
        monkeypatch.setenv("LD_PRELOAD", "/usr/lib/libasan.so /usr/lib/libstdc++.so")
        monkeypatch.setenv("ASAN_OPTIONS", "detect_odr_violation=0:detect_leaks=1")
        monkeypatch.setenv("LSAN_OPTIONS", "suppressions=/tmp/lsan.supp:detect_leaks=1")

        env = self._taos_cli_env()

        assert env["LD_PRELOAD"] == "/usr/lib/libasan.so /usr/lib/libstdc++.so"
        assert "detect_leaks=1" not in env["ASAN_OPTIONS"].split(":")
        assert "detect_leaks=0" in env["ASAN_OPTIONS"].split(":")
        assert "detect_leaks=1" not in env["LSAN_OPTIONS"].split(":")
        assert "detect_leaks=0" in env["LSAN_OPTIONS"].split(":")

    def test_missing_runtime_moves_normal_provider_libs(self, monkeypatch, tmp_path):
        """Restore normal provider libraries after a missing-runtime check.

        Verify that provider libraries are moved out of the runtime directory
        during the check and restored afterward.

        Since: v3.4.2.0

        """
        lib = tmp_path / "libmariadb.so.3"
        subdir = tmp_path / "mariadb"
        subdir.mkdir()
        nested_lib = subdir / "libmariadb.so.3"
        lib.write_text("mysql runtime")
        nested_lib.write_text("nested mysql runtime")
        monkeypatch.setattr(self, "_runtime_lib_dir", lambda: str(tmp_path))

        with self._without_provider_runtime_libs({"mysql"}):
            assert not lib.exists()
            assert not nested_lib.exists()

        assert lib.read_text() == "mysql runtime"
        assert nested_lib.read_text() == "nested mysql runtime"

    def test_missing_driver_moves_only_sqlserver_driver_libs(self, monkeypatch, tmp_path):
        """Hide only the Microsoft driver when a driver-scoped check is requested.

        Verify that the pattern-scoped hide used by the SQL Server
        driver-missing check (FQSS-LIB-003) moves libmsodbcsql* out of the
        runtime directory while leaving libodbc in place, and restores both.

        Since: v3.4.4.0

        """
        (tmp_path / "libodbc.so.2").write_text("driver manager")
        driver = tmp_path / "libmsodbcsql-18.so"
        driver.write_text("driver")
        monkeypatch.setattr(self, "_runtime_lib_dir", lambda: str(tmp_path))

        with self._without_provider_runtime_libs(
                {"sqlserver"}, patterns=["libmsodbcsql*"]):
            assert not driver.exists()
            assert (tmp_path / "libodbc.so.2").exists()

        assert driver.read_text() == "driver"
        assert (tmp_path / "libodbc.so.2").read_text() == "driver manager"

    def test_missing_influx_runtime_moves_arrow_third_party_libs(self, monkeypatch, tmp_path):
        """Restore InfluxDB plugin and Arrow dependencies after a runtime check.

        Verify that direct and resolved third-party libraries are hidden during
        the missing-runtime check and restored afterward.

        Since: v3.4.2.0

        """
        plugin = tmp_path / "libtaos_ext_influx_arrow.so"
        arrow = tmp_path / "libarrow.so.1900"
        parquet = tmp_path / "libparquet.so.1900"
        ext_arrow_dir = tmp_path / "ext_arrow"
        ext_arrow_dir.mkdir()
        resolved_arrow = ext_arrow_dir / "libarrow.so.1900"
        for lib in (plugin, arrow, parquet):
            lib.write_text(lib.name)
        resolved_arrow.write_text(resolved_arrow.name)
        monkeypatch.setattr(self, "_runtime_lib_dir", lambda: str(tmp_path))
        monkeypatch.setattr(
            self,
            "_resolved_runtime_dep_files",
            lambda provider: [str(resolved_arrow)] if provider == "influxdb" else [],
        )

        with self._without_provider_runtime_libs({"influxdb"}):
            assert not plugin.exists()
            assert not arrow.exists()
            assert not parquet.exists()
            assert not resolved_arrow.exists()

        assert plugin.read_text() == plugin.name
        assert arrow.read_text() == arrow.name
        assert parquet.read_text() == parquet.name
        assert resolved_arrow.read_text() == resolved_arrow.name

    def test_existing_source_query_missing_uses_provider_error(self, monkeypatch, tmp_path):
        """Report the provider error when an existing source lacks runtime libraries.

        Verify that source creation and query failures use the provider-specific
        error code and that the daemon is restarted with the original runtime.

        Since: v3.4.2.0

        """
        build = tmp_path / "debug" / "build"
        bin_dir = build / "bin"
        lib_dir = build / "lib"
        bin_dir.mkdir(parents=True)
        lib_dir.mkdir()
        taosd = bin_dir / "taosd"
        taosd.write_text("taosd")
        lib = lib_dir / "libmariadb.so.3"
        lib.write_text("mysql runtime")
        calls = []
        restarts = []

        def fake_error(sql, expectedErrno=None):
            calls.append((sql, expectedErrno))

        class Dnode:
            binPath = str(taosd)

        def runtime_dir():
            return getattr(self, "_runtime_lib_dir_override", str(lib_dir))

        def fake_restart():
            runtime_dir_path = runtime_dir()
            copy_lib = os.path.join(runtime_dir_path, "libmariadb.so.3")
            restarts.append((tdDnodes.dnodes[0].binPath, lib.exists(), os.path.exists(copy_lib)))

        monkeypatch.setattr(tdSql, "error", fake_error)
        monkeypatch.setattr(tdDnodes, "binPath", str(taosd), raising=False)
        monkeypatch.setattr(tdDnodes, "dnodes", [Dnode])
        monkeypatch.setattr(self, "_runtime_lib_dir", runtime_dir)
        monkeypatch.setattr(self, "_restart_taosd", fake_restart)

        self._check_runtime_missing_uses_provider_error(
            "mysql",
            "fq19_runtime_m",
            "src_t",
            "fq19_runtime_mysql_missing",
            "create external source fq19_runtime_mysql_missing type='mysql' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db'",
        )

        assert lib.read_text() == "mysql runtime"
        assert any(
            os.path.dirname(os.path.dirname(bin_path)) != str(build)
            and original_exists
            and not active_copy_exists
            for bin_path, original_exists, active_copy_exists in restarts
        )
        assert calls == [
            (
                "create external source fq19_runtime_mysql_missing type='mysql' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db'",
                TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
            ),
            (
                "select count(*) from fq19_runtime_m.src_t",
                TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
            ),
        ]

    def test_influx_http_uses_runtime_missing_query_check(self, monkeypatch):
        """Use the runtime-missing query check for HTTP InfluxDB sources.

        Verify that an HTTP InfluxDB source uses the same provider-error path as
        other external-source runtime checks.

        Since: v3.4.2.0

        """
        calls = []

        class Cfg:
            host = "127.0.0.1"
            port = 18086
            token = "tok"

        def fake_runtime_missing(provider, src, table, missing_create_src, missing_create_sql):
            calls.append((
                "runtime_missing",
                provider,
                src,
                table,
                missing_create_src,
                missing_create_sql,
            ))

        monkeypatch.setattr(self, "_influx_cfg", lambda: Cfg)
        monkeypatch.setattr(self, "_cleanup_src", lambda src: calls.append(("cleanup", src)))
        monkeypatch.setattr(self, "_check_remote_count", lambda src: calls.append(("remote_count", src)))
        monkeypatch.setattr(self, "_check_runtime_missing_uses_provider_error", fake_runtime_missing)
        monkeypatch.setattr(ExtSrcEnv, "influx_create_db_cfg", lambda cfg, db: calls.append(("create_db", db)))
        monkeypatch.setattr(ExtSrcEnv, "influx_write_cfg", lambda cfg, db, lines: calls.append(("write", db, lines)))
        monkeypatch.setattr(ExtSrcEnv, "influx_drop_db_cfg", lambda cfg, db: calls.append(("drop_db", db)))
        monkeypatch.setattr(tdSql, "execute", lambda sql: calls.append(("execute", sql)))

        self._with_influx_http_source(self._check_influx_http_missing_runtime)

        assert any(
            call[0] == "execute"
            and "type='influxdb'" in call[1]
            and "'protocol'='http'" in call[1]
            for call in calls
        )
        assert (
            "runtime_missing",
            "influxdb",
            "fq19_runtime_influx_http",
            "src_t",
            "fq19_runtime_influx_http_missing",
            "create external source fq19_runtime_influx_http_missing type='influxdb' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db' "
            "options('api_token'='tok','protocol'='http')",
        ) in calls

    def _taos_cli_env(self):
        env = os.environ.copy()
        taos_bin_path = os.environ.get("TAOS_BIN_PATH")
        if taos_bin_path:
            client_lib_dir = os.path.abspath(os.path.join(taos_bin_path, "..", "lib"))
        else:
            client_lib_dir = os.path.join(tdCom.getBuildPath(), "build", "lib")

        def disable_leak_detection(name):
            options = [
                option for option in env.get(name, "").split(":")
                if option and not option.startswith("detect_leaks=")
            ]
            options.append("detect_leaks=0")
            env[name] = ":".join(options)

        def prepend(name, path):
            if path and os.path.isdir(path):
                old = env.get(name, "")
                env[name] = f"{path}:{old}" if old else path

        prepend("LD_LIBRARY_PATH", client_lib_dir)
        if sys.platform == "darwin":
            prepend("DYLD_LIBRARY_PATH", client_lib_dir)
        if env.get("CI_ASAN_BUILD") == "1" or "libasan" in env.get("LD_PRELOAD", ""):
            disable_leak_detection("ASAN_OPTIONS")
            disable_leak_detection("LSAN_OPTIONS")
        return env

    def _run_taos_cli(self, sql):
        """Run one statement through a throw-away taos client; return stdout."""
        taos_bin = os.path.join(
            os.environ.get("TAOS_BIN_PATH", os.path.join(tdCom.getBuildPath(), "build", "bin")),
            "taos",
        )
        proc = subprocess.run(
            [taos_bin, "-r", "-c", tdCom.getClientCfgPath(), "-s", sql],
            capture_output=True,
            text=True,
            errors="ignore",
            env=self._taos_cli_env(),
        )
        output = proc.stdout + proc.stderr
        if proc.returncode != 0 or "DB error:" in output:
            raise AssertionError(output)
        return output

    def _query_count_with_taos(self, sql):
        output = self._run_taos_cli(sql)
        for line in output.splitlines():
            if "|" not in line or line.lstrip().startswith("="):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            for cell in cells:
                if cell.isdigit():
                    return int(cell)
        raise AssertionError(output)

    def _query_ints_with_taos(self, sql):
        """All integer cells of a result, in row order."""
        output = self._run_taos_cli(sql)
        values = []
        for line in output.splitlines():
            if "|" not in line or line.lstrip().startswith("="):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            for cell in cells:
                if cell.isdigit():
                    values.append(int(cell))
        if not values:
            raise AssertionError(output)
        return values

    def _check_remote_count(self, src):
        assert self._query_count_with_taos(f"select count(*) from {src}.src_t") == 5

    def _runtime_lib_dir_base(self):
        taos_bin_path = os.environ.get("TAOS_BIN_PATH")
        if taos_bin_path:
            return os.path.abspath(os.path.join(taos_bin_path, "..", "lib"))
        return os.path.abspath(os.path.join(tdCom.getBuildPath(), "build", "lib"))

    def _runtime_lib_dir(self):
        override = getattr(self, "_runtime_lib_dir_override", None)
        if override:
            return override
        return self._runtime_lib_dir_base()

    def _hidden_runtime_roots(self):
        """Directories a provider library can still be loaded from.

        The isolated copy only replaces LD_LIBRARY_PATH; taosd's RUNPATH still
        points at the original build lib directory (the path it was linked
        with), and the active client is also staged under the build tree's
        driver/ directory.  A hide that only touches the copy is therefore
        bypassed by those two, so the same files are hidden in every root.
        """
        roots = [self._runtime_lib_dir()]
        base = self._runtime_lib_dir_base()
        if os.path.realpath(base) not in {os.path.realpath(root) for root in roots}:
            roots.append(base)
        driver_root = os.path.join(os.path.dirname(base), "driver")
        if os.path.isdir(driver_root):
            roots.append(driver_root)
        return roots

    def _path_is_under(self, path, root):
        # Resolve symlinks as well: a search-path entry that is a link (or is
        # spelled through a linked directory) still reaches the original
        # runtime tree, and leaving it in LD_LIBRARY_PATH lets the isolated
        # taosd load the library the check just hid.
        try:
            real_path = os.path.realpath(path)
            real_root = os.path.realpath(root)
            return os.path.commonpath([real_path, real_root]) == real_root
        except ValueError:
            return False

    def _link_or_copy_file(self, src, dst):
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)

    @contextmanager
    def _isolated_runtime_copy(self):
        runtime_dir = self._runtime_lib_dir()
        original_taosd_paths = [dnode.binPath for dnode in tdDnodes.dnodes]
        original_global_bin_path = getattr(tdDnodes, "binPath", None)
        original_taosd = original_taosd_paths[0]
        # CI build trees are shared by concurrent workers. Keep the temporary
        # taosd outside them so another worker's build-path scan cannot select it.
        copy_root = tempfile.mkdtemp(prefix="fq-runtime-copy.")
        copy_build = os.path.join(copy_root, "build")
        copy_bin_dir = os.path.join(copy_build, "bin")
        copy_lib_dir = os.path.join(copy_build, "lib")
        old_runtime_override = getattr(self, "_runtime_lib_dir_override", None)
        # The dnode start command prepends the copy's lib dir and appends the
        # inherited search path, so anything hidden in the copy stays reachable
        # by soname through the original build tree.  Repoint the inherited
        # entries at the copy: it holds everything the original did except what
        # the check hides, which is exactly the state the check needs.
        lib_env = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
        original_lib_path = os.environ.get(lib_env)
        try:
            os.makedirs(copy_bin_dir)
            copy_taosd = os.path.join(copy_bin_dir, os.path.basename(original_taosd))
            self._link_or_copy_file(original_taosd, copy_taosd)
            # Dereference symlinks: a lib entry that is a link keeps the
            # original file's directory as its $ORIGIN (and RUNPATH), so a
            # copy that merely preserves the link still loads the Instant
            # Client next to the original file, not the (possibly hidden)
            # libraries in this copy.
            shutil.copytree(
                runtime_dir,
                copy_lib_dir,
                symlinks=False,
                copy_function=self._link_or_copy_file,
            )
            self._runtime_lib_dir_override = copy_lib_dir
            tdDnodes.binPath = copy_taosd
            for dnode in tdDnodes.dnodes:
                dnode.binPath = tdDnodes.binPath
            if original_lib_path is not None:
                inherited = [
                    entry for entry in original_lib_path.split(os.pathsep)
                    if entry and not self._path_is_under(entry, runtime_dir)
                ]
                os.environ[lib_env] = os.pathsep.join([copy_lib_dir] + inherited)
            yield
        finally:
            if original_lib_path is None:
                os.environ.pop(lib_env, None)
            else:
                os.environ[lib_env] = original_lib_path
            if old_runtime_override is None:
                if hasattr(self, "_runtime_lib_dir_override"):
                    delattr(self, "_runtime_lib_dir_override")
            else:
                self._runtime_lib_dir_override = old_runtime_override
            if original_global_bin_path is not None:
                tdDnodes.binPath = original_global_bin_path
            for dnode, bin_path in zip(tdDnodes.dnodes, original_taosd_paths):
                dnode.binPath = bin_path
            self._restart_taosd()
            shutil.rmtree(copy_root, ignore_errors=True)

    def _existing_runtime_files(self, paths):
        files = []
        seen = set()
        for path in paths:
            normalized = os.path.normpath(os.path.abspath(path))
            if normalized in seen or not os.path.lexists(normalized):
                continue
            seen.add(normalized)
            files.append(normalized)
        return sorted(files)

    def _provider_runtime_files(self, providers):
        runtime_dir = self._runtime_lib_dir()
        files = []
        for provider in providers:
            for pattern in _PROVIDER_RUNTIME_PATTERNS[provider]:
                files.extend(glob.glob(os.path.join(runtime_dir, pattern)))
                files.extend(glob.glob(os.path.join(runtime_dir, "**", pattern), recursive=True))
            files.extend(self._resolved_runtime_dep_files(provider))
        return self._existing_runtime_files(
            path for path in files if self._path_is_under(path, runtime_dir)
        )

    def _runtime_files_for_patterns(self, patterns):
        runtime_dir = self._runtime_lib_dir()
        files = []
        for pattern in patterns:
            files.extend(glob.glob(os.path.join(runtime_dir, pattern)))
            files.extend(glob.glob(os.path.join(runtime_dir, "**", pattern), recursive=True))
        return self._existing_runtime_files(
            path for path in files if self._path_is_under(path, runtime_dir)
        )

    def _provider_plugin_files(self, provider):
        runtime_dir = self._runtime_lib_dir()
        files = []
        for pattern in _PROVIDER_PLUGIN_PATTERNS.get(provider, ()):
            files.extend(glob.glob(os.path.join(runtime_dir, pattern)))
        return self._existing_runtime_files(files)

    def _parse_ldd_resolved_files(self, output, prefixes):
        files = []
        for line in output.splitlines():
            resolved = None
            if "=>" in line:
                fields = line.split("=>", 1)[1].strip().split()
                if fields:
                    resolved = fields[0]
            else:
                fields = line.strip().split()
                if fields:
                    resolved = fields[0]
            if not resolved or not os.path.isabs(resolved) or not os.path.lexists(resolved):
                continue
            if os.path.basename(resolved).startswith(prefixes):
                files.append(resolved)
        return files

    def _runtime_dep_search_dirs(self, plugin):
        plugin_dir = os.path.dirname(plugin)
        dirs = [plugin_dir]
        if sys.platform == "darwin":
            return dirs

        proc = subprocess.run(
            ["readelf", "-d", plugin],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="ignore",
        )
        for line in proc.stdout.splitlines():
            if "RUNPATH" not in line and "RPATH" not in line:
                continue
            start = line.find("[")
            end = line.find("]", start + 1)
            if start < 0 or end <= start:
                continue
            for raw_dir in line[start + 1:end].split(":"):
                dep_dir = raw_dir.replace("$ORIGIN", plugin_dir)
                dep_dir = os.path.normpath(os.path.abspath(dep_dir))
                if os.path.isdir(dep_dir):
                    dirs.append(dep_dir)
        return dirs

    def _resolved_runtime_dep_files(self, provider):
        prefixes = _PROVIDER_RESOLVED_DEP_PREFIXES.get(provider)
        if not prefixes or sys.platform == "darwin":
            return []

        files = []
        for plugin in self._provider_plugin_files(provider):
            for dep_dir in self._runtime_dep_search_dirs(plugin):
                for prefix in prefixes:
                    files.extend(glob.glob(os.path.join(dep_dir, f"{prefix}*")))
            proc = subprocess.run(
                ["ldd", plugin],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="ignore",
            )
            files.extend(self._parse_ldd_resolved_files(proc.stdout, prefixes))
        return files

    @staticmethod
    def _dir_is_readonly(path):
        """True when the directory lives on a read-only filesystem.

        The CI test artifact mounts build/lib read-only; root can still open
        files for writing on some read-only mounts, so ask statvfs for the
        mount flag instead of relying on os.access().
        """
        try:
            flags = os.statvfs(path).f_flag
        except OSError:
            return True
        return bool(flags & getattr(os, "ST_RDONLY", 1))

    @contextmanager
    def _without_provider_runtime_libs(self, providers, require_present=True, patterns=None):
        roots = self._hidden_runtime_roots()
        if patterns is None:
            patterns = [pattern
                        for provider in providers
                        for pattern in _PROVIDER_RUNTIME_PATTERNS.get(provider, ())]
        candidates = []
        for root in roots:
            for pattern in patterns:
                candidates.extend(glob.glob(os.path.join(root, pattern)))
                candidates.extend(glob.glob(os.path.join(root, "**", pattern),
                                            recursive=True))
        for provider in providers:
            candidates.extend(self._resolved_runtime_dep_files(provider))
        candidates = self._existing_runtime_files(
            path for path in candidates
            if any(self._path_is_under(path, root) for root in roots))
        # Hiding the libraries means moving them out of every resolution root.
        # A read-only root cannot be cleaned (EROFS/EXDEV), and a partial hide
        # would leave the provider resolvable, making the gate assertion
        # meaningless: skip those deployments instead of failing the case.
        probe_dirs = {os.path.dirname(path) for path in candidates}
        probe_dirs.add(self._runtime_lib_dir())
        readonly = sorted(dir_path for dir_path in probe_dirs
                          if self._dir_is_readonly(dir_path))
        if readonly:
            pytest.skip(
                "provider runtime libraries live on a read-only filesystem "
                f"({readonly[0]}); the hide/restore check needs a writable tree")
        hidden_dir = tempfile.mkdtemp(prefix=".fq-runtime-hidden.", dir=self._runtime_lib_dir())
        moved = []
        try:
            for path in candidates:
                hidden_path = os.path.join(hidden_dir, f"{len(moved)}-{os.path.basename(path)}")
                shutil.move(path, hidden_path)
                moved.append((hidden_path, path))
            if require_present and not moved:
                raise AssertionError(
                    f"no provider runtime libraries found in {self._runtime_lib_dir()} for {sorted(providers)}"
                )
            yield
        finally:
            for hidden_path, path in reversed(moved):
                if os.path.lexists(hidden_path) and not os.path.lexists(path):
                    shutil.move(hidden_path, path)
            try:
                os.rmdir(hidden_dir)
            except OSError:
                pass

    def _stop_direct_taosd(self):
        """Terminate the copy's taosd before the framework restarts dnode 1.

        The isolated start bypasses tdDnodes, so the framework's stop only
        knows the screen-managed process: the copy would keep listening on
        6030, the restart would wait for it to leave, and the case would run
        into its budget (CI: "wait too long for taosd start").
        """
        proc = getattr(self, "_direct_taosd", None)
        self._direct_taosd = None
        odbc_dir = getattr(self, "_isolated_odbc_dir", None)
        self._isolated_odbc_dir = None
        if odbc_dir:
            shutil.rmtree(odbc_dir, ignore_errors=True)
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass

    def _restart_taosd(self):
        self._stop_direct_taosd()
        tdDnodes.stop(1)
        for attempt in range(2):
            try:
                tdDnodes.start(1)
                return
            except SystemExit:
                if attempt == 1:
                    raise
                tdLog.info(
                    "taosd restart timed out after runtime-lib swap; retrying once"
                )
                tdDnodes.stop(1)

    def _restart_taosd_with_isolated_copy(self, isolate_odbc=False):
        """Start the copied taosd directly so it really runs from the copy.

        For a remote dnode (the CI sim) the framework's restart re-enters the
        start command in a fresh process whose binary and library directories
        are the ORIGINAL build tree; the engine would then load the driver from
        the original directory no matter what the isolated copy hides.  POSIX
        starts the copy's own binary here, with the isolated LD_LIBRARY_PATH
        already installed in the environment, so the hidden state is the state
        the engine sees.  Windows keeps the framework restart, which already
        launches the copied taosd.
        """
        if sys.platform == "win32":
            self._restart_taosd()
            return
        tdDnodes.stop(1)
        dnode = tdDnodes.dnodes[0]
        stderr = subprocess.DEVNULL
        if getattr(dnode, "asan", False):
            asan_dir = os.path.join(dnode.path, "asan")
            os.makedirs(asan_dir, exist_ok=True)
            stderr = open(os.path.join(asan_dir, "dnode%d.asan" % dnode.index), "ab")
        # unixODBC falls back to the host's driver registrations when the
        # path-based lookup fails.  Point the isolated engine at an empty
        # configuration so the missing-driver case is deterministic instead
        # of being skipped on hosts whose odbcinst.ini registers Oracle.
        engine_env = os.environ.copy()
        if isolate_odbc:
            odbc_dir = tempfile.mkdtemp(prefix="fq-odbc-isolated.")
            self._isolated_odbc_dir = odbc_dir
            open(os.path.join(odbc_dir, "odbcinst.ini"), "w").close()
            open(os.path.join(odbc_dir, "odbc.ini"), "w").close()
            engine_env["ODBCSYSINI"] = odbc_dir
            engine_env["ODBCINI"] = os.path.join(odbc_dir, "odbc.ini")
        try:
            self._direct_taosd = subprocess.Popen(
                [dnode.binPath, "-c", dnode.cfgDir],
                stdout=subprocess.DEVNULL, stderr=stderr,
                env=engine_env, start_new_session=True,
            )
        finally:
            if stderr is not subprocess.DEVNULL:
                stderr.close()
        port = 6030
        cfg_path = os.path.join(dnode.cfgDir, "taos.cfg")
        try:
            with open(cfg_path, encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    fields = line.split()
                    if len(fields) >= 2 and fields[0] == "serverPort":
                        port = int(fields[1])
                        break
        except (OSError, ValueError):
            pass
        deadline = time.time() + 120
        while time.time() < deadline:
            if self._direct_taosd.poll() is not None:
                raise AssertionError(
                    "isolated taosd exited before it accepted connections")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(0.5)
        raise AssertionError(
            "isolated taosd did not listen on port %d in time" % port)

    def _isolated_taosd_driver_log(self):
        """Driver-resolution lines from the isolated taosd, for diagnostics.

        The restart in the caller's finally block truncates the dnode log, so
        the only chance to see where the isolated engine resolved its driver
        (or why it still reached the Instant Client) is at the assertion.
        """
        try:
            dnode = tdDnodes.dnodes[0]
            path = os.path.join(dnode.path, "log", "taosdlog.0")
            with open(path, encoding="utf-8", errors="ignore") as fh:
                lines = fh.readlines()[-4000:]
        except (OSError, IndexError, AttributeError) as err:
            return f"[fq19-diag] taosd log unavailable: {err}"
        needles = ("Oracle", "sqora", "clntsh", "odbc", "driver")
        tail = [ln.rstrip() for ln in lines if any(n in ln for n in needles)]
        if not tail:
            return "[fq19-diag] no driver lines in the isolated taosd log"
        return "[fq19-diag] isolated taosd driver log:\n" + "\n".join(tail[-25:])

    def _missing_create_sql_for_src(self, src):
        missing_src = f"{src}_missing_create"
        if src.endswith("_m"):
            return (
                "mysql",
                missing_src,
                f"create external source {missing_src} type='mysql' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db'",
            )
        if src.endswith("_p"):
            return (
                "postgresql",
                missing_src,
                f"create external source {missing_src} type='postgresql' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db' schema='public'",
            )
        if src.endswith("_s"):
            return (
                "sqlserver",
                missing_src,
                f"create external source {missing_src} type='sqlserver' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db' schema='dbo'",
            )
        if src.endswith("_o"):
            # Oracle's DATABASE is the Easy Connect service name and its SCHEMA
            # is the login schema (DS 5.3.3).
            return (
                "oracle",
                missing_src,
                f"create external source {missing_src} type='oracle' "
                "host='127.0.0.1' port=9 user='u' password='p' database='FREEPDB1' "
                "schema='TAOSDATA'",
            )
        return (
            "influxdb",
            missing_src,
            f"create external source {missing_src} type='influxdb' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db' "
            "options('api_token'='tok','protocol'='flight_sql')",
        )

    def _check_runtime_missing_uses_provider_error(self, provider, src, table, missing_create_src, missing_create_sql):
        with self._isolated_runtime_copy():
            try:
                with self._without_provider_runtime_libs({provider}):
                    self._restart_taosd()
                    _check_provider_missing(missing_create_src, missing_create_sql)
                    tdSql.error(
                        f"select count(*) from {src}.{table}",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
            finally:
                self._restart_taosd()

    def _check_sqlserver_driver_missing(self, src):
        """Hide only libmsodbcsql*, keep libodbc: the driver-manager survives
        but the provider's path resolution finds no driver, so the query must
        fail with IM002 -> TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT (FQSS-LIB-003,
        DS 3.3 排障区分) instead of crashing."""
        self._check_remote_count(src)
        with self._isolated_runtime_copy():
            try:
                with self._without_provider_runtime_libs(
                        {"sqlserver"}, require_present=False,
                        patterns=["libmsodbcsql*"]):
                    self._restart_taosd()
                    tdSql.error(
                        f"select count(*) from {src}.src_t",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
            finally:
                self._restart_taosd()

    def _check_create_missing_with_runtime_hidden(self, provider, missing_create_src, missing_create_sql):
        with self._isolated_runtime_copy():
            try:
                with self._without_provider_runtime_libs({provider}, require_present=False):
                    self._restart_taosd()
                    _check_provider_missing(missing_create_src, missing_create_sql)
            finally:
                self._restart_taosd()

    def _check_remote_count_and_missing_runtime(self, src):
        self._check_remote_count(src)
        provider, missing_src, missing_sql = self._missing_create_sql_for_src(src)
        self._check_runtime_missing_uses_provider_error(provider, src, "src_t", missing_src, missing_sql)

    def _with_influx_http_source(self, callback):
        cfg = self._influx_cfg()
        src = "fq19_runtime_influx_http"
        db = "fq19_runtime_http_db"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(cfg, db)
        try:
            lines = [f"src_t val={i}i {1704067200000000000 + i * 60000000000}" for i in range(5)]
            ExtSrcEnv.influx_write_cfg(cfg, db, lines)
            tdSql.execute(
                f"create external source {src} type='influxdb' "
                f"host='{cfg.host}' port={cfg.port} user='u' password='' database={db} "
                f"options('api_token'='{cfg.token}','protocol'='http')"
            )
            callback(src)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(cfg, db)
            except Exception:
                pass

    def _check_influx_http_count(self, src):
        self._check_remote_count(src)

    def _check_influx_http_missing_runtime(self, src):
        self._check_runtime_missing_uses_provider_error(
            "influxdb",
            src,
            "src_t",
            "fq19_runtime_influx_http_missing",
            "create external source fq19_runtime_influx_http_missing type='influxdb' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db' "
            "options('api_token'='tok','protocol'='http')",
        )

    def _check_create_all_missing(self):
        self._check_create_missing_with_runtime_hidden(
            "mysql",
            "fq19_runtime_mysql",
            "create external source fq19_runtime_mysql type='mysql' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db'",
        )
        self._check_create_missing_with_runtime_hidden(
            "postgresql",
            "fq19_runtime_pg",
            "create external source fq19_runtime_pg type='postgresql' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db' schema='public'",
        )
        self._check_create_missing_with_runtime_hidden(
            "sqlserver",
            "fq19_runtime_sqlserver",
            "create external source fq19_runtime_sqlserver type='sqlserver' "
            "host='127.0.0.1' port=9 user='u' password='p' database='db' schema='dbo'",
        )
        self._check_create_missing_with_runtime_hidden(
            "oracle",
            "fq19_runtime_oracle",
            "create external source fq19_runtime_oracle type='oracle' "
            "host='127.0.0.1' port=9 user='u' password='p' database='FREEPDB1' "
            "schema='TAOSDATA'",
        )
        for protocol in ("http", "flight_sql"):
            self._check_create_missing_with_runtime_hidden(
                "influxdb",
                f"fq19_runtime_{protocol}",
                f"create external source fq19_runtime_{protocol} type='influxdb' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db' "
                f"options('api_token'='tok','protocol'='{protocol}')",
            )

    def test_fq_runtime_provider_libs_available(self):
        """Check federated-query provider runtime libraries work when present

        1. Stage provider runtime libraries before taosd startup
        2. Verify MySQL/PostgreSQL/InfluxDB flight SQL external-source queries
        3. Verify InfluxDB HTTP external-source queries

        Catalog:
            - FederatedQuery

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-04 Codex Add positive runtime library coverage

        """
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        self._with_std_sources("fq19_runtime", self._check_remote_count)
        self._with_influx_http_source(self._check_influx_http_count)

    def test_fq_runtime_provider_libs_missing(self):
        """Check federated-query provider runtime library gates

        1. Verify missing runtime libraries reject CREATE EXTERNAL SOURCE
        2. Verify existing external-source queries reject missing runtime libraries
        3. Verify InfluxDB HTTP also fails without its runtime plugin

        Catalog:
            - FederatedQuery

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-01 Codex Add runtime library gate coverage
            - 2026-07-04 Codex Verify queries fail after runtime libraries disappear

        """
        if sys.platform == "win32":
            # Hiding the provider libraries relies on the ELF/Mach-O runtime
            # directory layout (build/lib + LD_LIBRARY_PATH).  Windows loads
            # the DLLs from the engine's own directory and keeps extra copies
            # on PATH, so the hide cannot be made deterministic here; the
            # packaged-layout check lives in the virtual-table runtime suite.
            pytest.skip("runtime-library hiding is Unix-only; Windows ships "
                        "provider DLLs next to taosd")
        if _try_prepare_runtime_libs():
            ExtSrcEnv.ensure_env()
            self._with_std_sources("fq19_runtime", self._check_remote_count_and_missing_runtime)
            self._with_influx_http_source(self._check_influx_http_missing_runtime)
        self._check_create_all_missing()

    def test_oracle_missing_driver_uses_provider_error(self):
        """Check Oracle reports the missing-driver gate without losing unixODBC

        1. Build an Oracle external source with all dependencies staged
        2. Hide only the Oracle driver and its Instant Client libraries, keep libodbc
        3. Expect the driver-not-found error, not a crash or silent fallback

        The provider resolves ``libsqora.so.<ver>`` by path and only then falls
        back to the odbcinst driver name.  The isolated engine runs with an
        empty ODBC configuration, so a host that registers the Oracle driver
        cannot satisfy the fallback and the check stays deterministic
        (FQOR-LIB-004, Oracle DS 3.3).

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 wpan Add FQOR-LIB-004 driver-missing coverage

        """
        if sys.platform == "win32":
            pytest.skip("Windows hides the driver through its ODBC registration; "
                        "test_fq_23_runtime_missing_windows.py covers that path")
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        self._with_std_sources(
            "fq19_runtime_oradrv", self._check_oracle_driver_missing,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True)

    def _check_oracle_driver_missing(self, src):
        """Hide the Oracle driver and its Instant Client libraries, keep libodbc.

        The driver manager survives but the provider's path resolution finds no
        driver, so the query must fail with TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT
        instead of crashing or falling back to a local scan (FQOR-LIB-004)."""
        self._check_remote_count(src)
        with self._isolated_runtime_copy():
            try:
                with self._without_provider_runtime_libs(
                        {"oracle"}, require_present=False,
                        patterns=["libsqora*"]):
                    self._restart_taosd_with_isolated_copy(isolate_odbc=True)
                    tdSql.error(
                        f"select count(*) from {src}.src_t",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
            finally:
                self._restart_taosd()

    # ------------------------------------------------------------------
    # Oracle runtime dependency chain (FQOR-LIB-001/002/003) and pooling
    # (FQOR-LIB-005).  LIB-004 is the driver-missing case above.
    # ------------------------------------------------------------------

    def test_oracle_runtime_libs_available(self):
        """Check Oracle works when unixODBC, the driver and Instant Client are present

        1. Stage the provider runtime libraries before taosd starts
        2. Query an Oracle external table through the full dependency chain
        3. Assert every row exactly, not just the row count

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 dapan1121 Add FQOR-LIB-001 dependency-chain coverage
            - 2026-10-28 Wei Pan Check the Windows Instant Client and its registration

        """
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        self._with_std_sources(
            "fq19_runtime_oraok", self._check_oracle_runtime_available,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True)

    def _check_oracle_windows_runtime_layout(self):
        """Assert the staged Instant Client and its ODBC registration.

        Windows resolves the driver only through HKLM\\SOFTWARE\\ODBC\\
        ODBCINST.INI and the Driver keyword cannot carry a DLL path, so the
        provider needs both halves: the Instant Client DLLs next to taosd (or
        in the package's driver\\oracle) and a registration whose Driver value
        points at an existing sqora DLL in a directory that also carries the
        base libraries.
        """
        bin_dir = os.path.abspath(os.environ.get(
            "TAOS_BIN_PATH", os.path.join(tdCom.getBuildPath(), "build", "bin")))
        for pattern in ("sqora32.dll", "oraociei*.dll", "oci.dll", "odbc_install.exe"):
            matches = glob.glob(os.path.join(bin_dir, pattern))
            assert matches, (
                f"the Windows build did not stage {pattern} in {bin_dir}; "
                "FQ_ORACLE_DRIVER_DIR or INSTALL_FQ_RUNTIME_LIBS=ON is required")
        registrations = _windows_oracle_driver_registrations()
        assert registrations, (
            "no Oracle Instant Client driver is registered under "
            "HKLM\\SOFTWARE\\ODBC\\ODBCINST.INI; run odbc_install.exe from the "
            "Instant Client directory or the FQ setup script")
        loadable = []
        for name, driver in registrations:
            directory = os.path.dirname(driver)
            if not os.path.isfile(driver):
                continue
            if not glob.glob(os.path.join(directory, "oraociei*.dll")):
                continue
            loadable.append((name, driver))
        assert loadable, (
            "the registered Oracle drivers do not point at a loadable Instant "
            "Client: " + ", ".join(f"{name} -> {driver}"
                                   for name, driver in registrations))
        tdLog.info("Oracle ODBC registrations: "
                   + ", ".join(f"{name} -> {driver}" for name, driver in loadable))

    def _check_oracle_runtime_available(self, src):
        """FQOR-LIB-001: the staged chain (libodbc -> libsqora -> libclntsh)
        resolves, and the query returns the remote rows unchanged."""
        if sys.platform == "win32":
            self._check_oracle_windows_runtime_layout()
        self._check_remote_count(src)
        tdSql.query(f"select val, score, name from {src}.src_t order by val")
        tdSql.checkRows(5)
        expected = [
            (1, 1.5, "alpha"),
            (2, 2.5, "beta"),
            (3, 3.5, "gamma"),
            (4, 4.5, "delta"),
            (5, 5.5, "epsilon"),
        ]
        for idx, (val, score, name) in enumerate(expected):
            tdSql.checkData(idx, 0, val)
            tdSql.checkData(idx, 1, score)
            tdSql.checkData(idx, 2, name)

    def test_oracle_missing_unixodbc_uses_provider_error(self):
        """Check Oracle reports the missing driver-manager gate

        1. Build an Oracle external source with the full chain staged
        2. Hide only libodbc (keep the Oracle driver and Instant Client)
        3. Expect the provider gate on both CREATE and SELECT, never a crash

        Hiding the driver manager is what separates the "no unixODBC" state
        (FQOR-LIB-002) from the "no driver" state: with the driver and the
        Instant Client still present, the only component missing is the
        manager the provider links against.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 dapan1121 Add FQOR-LIB-002 unixODBC-missing coverage

        """
        if sys.platform == "win32":
            pytest.skip("odbc32.dll ships with Windows and cannot be hidden; "
                        "test_fq_23_runtime_missing_windows.py covers the "
                        "driver-registration path")
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        self._with_std_sources(
            "fq19_runtime_oraodbc", self._check_oracle_unixodbc_missing,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True)

    def _check_oracle_unixodbc_missing(self, src):
        self._check_oracle_dependency_missing(src, ["libodbc*"])

    def test_oracle_missing_instant_client_uses_provider_error(self):
        """Check Oracle reports the missing Instant Client gate

        1. Build an Oracle external source with the full chain staged
        2. Hide only libclntsh* (keep unixODBC and the Oracle driver)
        3. Expect the provider gate on the first data read, never a crash

        The driver library itself stays in place, so this is the "driver
        loads but its Instant Client dependency does not" state
        (FQOR-LIB-003).  The odbcinst fallback cannot mask it: the fallback
        driver needs the same Instant Client sonames.

        Only the qnode side sees the hidden bundle here: the client library
        that runs inside the test process keeps its own already-loaded copy.
        A missing Instant Client therefore surfaces on SELECT, while CREATE
        EXTERNAL SOURCE (metadata only, no connection) still succeeds.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 dapan1121 Add FQOR-LIB-003 Instant-Client-missing coverage

        """
        if sys.platform == "win32":
            pytest.skip("Windows resolves the Instant Client from the driver's "
                        "own directory while other copies stay on PATH, so the "
                        "hide is not deterministic")
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        self._with_std_sources(
            "fq19_runtime_oraic", self._check_oracle_instant_client_missing,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True)

    def _check_oracle_instant_client_missing(self, src):
        # The Instant Client is not needed to register a source, and the
        # client half of the engine already holds its copy, so only the data
        # path can report the gate.
        self._check_oracle_dependency_missing(src, ["libclntsh*"], create_gate=False)

    def _check_oracle_dependency_missing(self, src, patterns, create_gate=True):
        """Hide one layer of the Oracle runtime chain and assert the gate.

        ``require_present`` stays on: a host that does not actually stage the
        hidden library must fail the test instead of silently checking
        nothing.  ``create_gate`` is False for libraries that a qnode needs
        only when it connects (the Instant Client): CREATE is metadata-only
        there, so the gate must appear on SELECT instead.
        """
        self._check_remote_count(src)
        provider, missing_src, missing_sql = self._missing_create_sql_for_src(src)
        assert provider == "oracle"
        with self._isolated_runtime_copy():
            try:
                with self._without_provider_runtime_libs(
                        {provider}, patterns=patterns):
                    self._restart_taosd_with_isolated_copy(
                        isolate_odbc=provider in ("oracle", "sqlserver"))
                    if create_gate:
                        _check_provider_missing(missing_src, missing_sql)
                    else:
                        # CREATE must stay usable; it must also not steal the
                        # assertion from SELECT by failing first.
                        tdSql.execute(f"drop external source if exists {missing_src}")
                        tdSql.execute(missing_sql)
                    try:
                        tdSql.error(
                            f"select count(*) from {src}.src_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                        )
                    except AssertionError as err:
                        # The isolated taosd still reached the provider: record
                        # which driver it resolved, before the restart below
                        # truncates this process's log.
                        raise AssertionError(
                            f"{err}\n{self._isolated_taosd_driver_log()}"
                        ) from err
                    # The rest of the engine is unaffected by the broken chain.
                    tdSql.query("select server_version()")
                    tdSql.checkRows(1)
                    tdSql.execute(f"drop external source if exists {missing_src}")
            finally:
                self._restart_taosd()

    def test_oracle_pool_disconnect_reconnects(self):
        """FQOR-LIB-005: Oracle pooling is SQL_CP_OFF, so a drop really disconnects

        1. Query an Oracle source that logs in as the pool-exhaustion account
        2. Confirm the physical Oracle session appears in V$SESSION
        3. DROP the source and confirm the session goes away
        4. Recreate the source and get the same rows back

        The Oracle provider sets ``SQL_ATTR_CONNECTION_POOLING = SQL_CP_OFF``
        (extConnectorOracle.c) precisely so its own pool owns the physical
        connections: if the driver manager pooled them, SQLDisconnect would
        only return the connection to that pool and the Oracle session would
        survive the DROP.  V$SESSION is the observable difference, and the
        recreated source proves the state is recoverable.

        The rows are read through a one-shot CLI process on purpose: the
        Python test process runs the same client library, which pools its own
        metadata connection for 600 idle seconds, and that session would
        outlive the DROP no matter what the engine did.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 dapan1121 Add FQOR-LIB-005 pooling-disconnect coverage

        """
        _prepare_runtime_libs()
        ExtSrcEnv.ensure_env()
        cfg = self._oracle_cfg()
        src = "fq19_pool_o"
        table = "FQ19_POOL_T"
        schema = ExtSrcEnv.ORACLE_POOL_TEST_USER.upper()

        self._cleanup_src(src)
        ExtSrcEnv.oracle_exec_as_pool_user(cfg, [
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;",
            f"CREATE TABLE {table} (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
            f"INSERT INTO {table} VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 1)",
            f"INSERT INTO {table} VALUES (TIMESTAMP '2024-01-01 00:01:00.000', 2)",
            f"INSERT INTO {table} VALUES (TIMESTAMP '2024-01-01 00:02:00.000', 3)",
            f"INSERT INTO {table} VALUES (TIMESTAMP '2024-01-01 00:03:00.000', 4)",
            f"INSERT INTO {table} VALUES (TIMESTAMP '2024-01-01 00:04:00.000', 5)",
            "COMMIT",
        ])

        def open_source():
            self._mk_oracle_real(
                src, database=cfg.service, schema=schema,
                user=ExtSrcEnv.ORACLE_POOL_TEST_USER,
                password=ExtSrcEnv.ORACLE_POOL_TEST_PASS)

        def check_rows():
            # Read through a one-shot CLI: the test process itself keeps a
            # client-side pool (idle TTL 600 s) that would hold an Oracle
            # session of its own and make V$SESSION say the engine leaked one.
            values = self._query_ints_with_taos(
                f"select val from {src}.{table.lower()} order by val")
            assert values == [1, 2, 3, 4, 5], values

        try:
            # Another FQ case in a parallel shard may use this same Oracle
            # account, so the assertions are relative to the count seen before
            # this source was created.  An isolated run starts at 0, which
            # makes "back to baseline" mean "our session really went away".
            baseline = self._oracle_pool_session_count()
            open_source()
            check_rows()

            sessions = self._oracle_pool_session_count()
            assert sessions > baseline, (
                "the pooled Oracle connection is not visible in V$SESSION")
            tdSql.execute(f"drop external source {src}")

            # The disconnect is asynchronous on the Oracle side, so poll.
            deadline = time.monotonic() + 60
            while sessions > baseline and time.monotonic() < deadline:
                time.sleep(1)
                sessions = self._oracle_pool_session_count()
            assert sessions <= baseline, (
                f"DROP EXTERNAL SOURCE left {sessions - baseline} Oracle "
                "session(s) open; driver-manager connection pooling must "
                "stay off")

            # A recreated source connects afresh and answers with the same rows.
            open_source()
            check_rows()
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_exec_as_pool_user(cfg, [
                    f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
                    "EXCEPTION WHEN OTHERS THEN NULL; END;",
                ])
            except Exception:
                pass

    def _oracle_pool_session_count(self):
        """Number of live Oracle sessions for the pool-exhaustion account."""
        return self._oracle_admin_scalar(
            "SELECT COUNT(*) FROM V$SESSION WHERE USERNAME = "
            f"'{ExtSrcEnv.ORACLE_POOL_TEST_USER.upper()}'")

    def _oracle_admin_scalar(self, sql):
        """Return the first numeric cell of an admin query's output.

        ``oracle_admin_exec_cfg`` discards output, so a read-only query goes
        through the same statement channel with the admin credentials the
        provisioning path uses.
        """
        output = ExtSrcEnv._oracle_sql(
            self._oracle_cfg(), sql,
            user=ExtSrcEnv.ORACLE_ADMIN_USER,
            password=ExtSrcEnv.ORACLE_ADMIN_PASS)
        for line in output.splitlines():
            value = line.strip()
            if value.isdigit():
                return int(value)
        raise AssertionError(f"no numeric value in Oracle admin output: {output!r}")

    def test_sqlserver_missing_driver_uses_provider_error(self):
        """Check SQL Server reports the missing-driver gate without unixODBC loss

        1. Build a SQL Server external source with all dependencies staged
        2. Hide only libmsodbcsql* (the driver) and keep libodbc in place
        3. Expect the IM002-mapped error, not a crash or silent fallback

        The provider resolves the driver by path and only then falls back to
        the odbcinst driver name.  The isolated engine runs with an empty ODBC
        configuration, so a host that registers 'ODBC Driver 18 for SQL
        Server' cannot satisfy the fallback and the check stays deterministic.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-10 Claude Add FQSS-LIB-003 driver-missing coverage

        """
        ExtSrcEnv.ensure_env()
        self._with_std_sources(
            "fq19_runtime_ssdrv", self._check_sqlserver_driver_missing,
            skip_mysql=True, skip_pg=True, skip_influx=True)
