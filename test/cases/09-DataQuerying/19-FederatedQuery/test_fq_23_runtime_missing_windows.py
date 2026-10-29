# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import glob
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager

import pytest

from new_test_framework.utils import tdCom, tdDnodes, tdLog, tdSql

from federated_query_common import ExtSrcEnv, FederatedQueryTestMixin, _code

TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT = _code("TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT")

# Provider client DLLs staged next to taosd by the Windows build.
_PROVIDER_PATTERNS = {
    "mysql": ("libmariadb*.dll", "libmysqlclient*.dll"),
    "postgresql": ("libpq*.dll",),
    "influxdb": ("taos_ext_influx_arrow*.dll",),
}

_MISSING_CREATE_SQL = {
    "mysql": "create external source {src} type='mysql' "
             "host='127.0.0.1' port=9 user='u' password='p' database='db'",
    "postgresql": "create external source {src} type='postgresql' "
                  "host='127.0.0.1' port=9 user='u' password='p' database='db' schema='public'",
    "influxdb": "create external source {src} type='influxdb' "
                "host='127.0.0.1' port=9 user='u' password='p' database='db' "
                "options('api_token'='tok','protocol'='http')",
}

_SOURCE_PROVIDERS = {"_m": "mysql", "_p": "postgresql", "_i": "influxdb"}

_ODBC_DRIVER_NAME = "ODBC Driver 18 for SQL Server"
_ODBC_DRIVER_KEY = r"SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 18 for SQL Server"


def _is_admin():
    import ctypes

    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


@contextmanager
def _without_registered_odbc_driver():
    """Point the driver registration at a missing DLL and restore it.

    Windows resolves ODBC drivers through the registry, so this is the
    equivalent of hiding libmsodbcsql*.so on Linux.  The original value is
    always restored, including on failure.
    """
    import winreg

    key_flags = winreg.KEY_READ | winreg.KEY_SET_VALUE
    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _ODBC_DRIVER_KEY, 0, key_flags) as key:
        original, kind = winreg.QueryValueEx(key, "Driver")
        winreg.SetValueEx(key, "Driver", 0, kind, r"C:\nonexistent\msodbcsql18.dll")
    try:
        yield
    finally:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _ODBC_DRIVER_KEY, 0,
                            winreg.KEY_SET_VALUE) as key:
            winreg.SetValueEx(key, "Driver", 0, kind, original)
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _ODBC_DRIVER_KEY, 0,
                            winreg.KEY_READ) as key:
            restored, _ = winreg.QueryValueEx(key, "Driver")
        assert restored == original, (
            f"failed to restore the {_ODBC_DRIVER_NAME} registration: {restored!r}")


def _bin_dir():
    taos_bin_path = os.environ.get("TAOS_BIN_PATH")
    if taos_bin_path:
        return os.path.abspath(taos_bin_path)
    return os.path.abspath(os.path.join(tdCom.getBuildPath(), "build", "bin"))


class TestFq23RuntimeMissingWindows(FederatedQueryTestMixin):
    def _start_taosd(self):
        for attempt in range(3):
            try:
                tdDnodes.start(1)
                return
            except (SystemExit, Exception) as exc:
                if attempt == 2:
                    raise
                tdLog.info(
                    "taosd restart after the runtime-lib swap failed (%s); retrying"
                    % exc)
                try:
                    tdDnodes.stop(1)
                except Exception:
                    pass

    def _host_provider_files(self, provider):
        bin_dir = _bin_dir()
        files = []
        for pattern in _PROVIDER_PATTERNS[provider]:
            files.extend(glob.glob(os.path.join(bin_dir, pattern)))
        assert files, f"no {provider} runtime DLLs found in {bin_dir}"
        return sorted(set(files))

    @contextmanager
    def _without_provider_runtime_libs(self, provider):
        """Hide the provider DLLs and strip matching PATH entries.

        Windows also searches PATH for a bare DLL name, and the Python
        installation used by the tests keeps its own libpq/libmariadb copies,
        so hiding build/bin alone is not enough.
        """
        patterns = _PROVIDER_PATTERNS[provider]
        hidden_dir = tempfile.mkdtemp(prefix="fq23-hidden.")
        moved = []
        saved_path = os.environ.get("PATH", "")
        try:
            for path in self._host_provider_files(provider):
                destination = os.path.join(hidden_dir, os.path.basename(path))
                shutil.move(path, destination)
                moved.append((destination, path))
            keep = []
            for entry in saved_path.split(os.pathsep):
                if not entry:
                    continue
                try:
                    matches = any(glob.glob(os.path.join(entry, pattern))
                                  for pattern in patterns)
                except OSError:
                    matches = False
                if not matches:
                    keep.append(entry)
            os.environ["PATH"] = os.pathsep.join(keep)
            yield
        finally:
            os.environ["PATH"] = saved_path
            for destination, original in reversed(moved):
                if os.path.exists(destination) and not os.path.exists(original):
                    shutil.move(destination, original)
            shutil.rmtree(hidden_dir, ignore_errors=True)

    def _check_local_operations(self):
        db = "fq23_local"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {db}.t (ts timestamp, v int)")
        tdSql.execute(f"insert into {db}.t values (1704067200000, 1)")
        tdSql.query(f"select count(*) from {db}.t")
        tdSql.checkData(0, 0, 1)
        tdSql.execute(f"drop database {db}")

    def _check_source_without_runtime(self, provider, src):
        missing_src = f"{src}_missing"
        tdSql.query(f"select count(*) from {src}.src_t")
        tdSql.checkData(0, 0, 5)

        tdDnodes.stop(1)
        try:
            with self._without_provider_runtime_libs(provider):
                try:
                    self._start_taosd()
                    tdSql.execute(f"drop external source if exists {missing_src}")
                    tdSql.error(
                        _MISSING_CREATE_SQL[provider].format(src=missing_src),
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
                    tdSql.error(
                        f"select count(*) from {src}.src_t",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
                    self._check_local_operations()
                finally:
                    tdDnodes.stop(1)
        finally:
            # Always leave a running engine behind for the fixture teardown,
            # including when an assertion above fails.
            self._start_taosd()

        tdSql.query(f"select count(*) from {src}.src_t")
        tdSql.checkData(0, 0, 5)

    def test_missing_provider_dlls_disable_federated_query_only(self):
        """Check missing provider DLLs disable federated query only

        1. Create MySQL, PostgreSQL and InfluxDB external sources
        2. Hide each provider DLL, restart taosd and verify CREATE and SELECT
           fail with the external-source-not-supported error
        3. Verify local database operations keep working while the DLL is hidden
        4. Restore the DLL and verify federated queries recover

        SQL Server is covered separately below: Windows resolves odbc32.dll
        from System32, so only the driver registration can be made to point at
        a missing DLL.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Wei Pan Add the Windows missing-runtime behaviour check

        """
        if sys.platform != "win32":
            pytest.skip("Windows provider DLLs are staged next to taosd only on Windows")

        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        def body(src):
            provider = _SOURCE_PROVIDERS[src[-2:]]
            self._check_source_without_runtime(provider, src)

        self._with_std_sources(
            "fq23_missing", body, skip_sqlserver=True)

    def _check_sqlserver_without_driver(self, src):
        tdSql.query(f"select count(*) from {src}.src_t")
        tdSql.checkData(0, 0, 5)

        tdDnodes.stop(1)
        try:
            with _without_registered_odbc_driver():
                try:
                    self._start_taosd()
                    # CREATE only records metadata; like the Linux
                    # driver-missing case, the failure appears when the source
                    # is queried.
                    tdSql.error(
                        f"select count(*) from {src}.src_t",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                    )
                    self._check_local_operations()
                finally:
                    tdDnodes.stop(1)
        finally:
            # Always leave a running engine behind for the fixture teardown,
            # including when an assertion above fails.
            self._start_taosd()

        tdSql.query(f"select count(*) from {src}.src_t")
        tdSql.checkData(0, 0, 5)

    def test_missing_sqlserver_driver_disables_sqlserver_source_only(self):
        """Check a missing SQL Server driver disables SQL Server sources only

        1. Create a SQL Server external source and verify it works
        2. Point the ODBC Driver 18 registration at a missing DLL and restart
           taosd
        3. Verify the query fails with the external-source-not-supported error
           while local database operations keep working
        4. Restore the registration and verify the query recovers

        Windows resolves ODBC drivers through the registry and the driver is
        installed machine-wide, so this test temporarily rewrites
        HKLM\\SOFTWARE\\ODBC\\ODBCINST.INI and is opt-in: set
        FQ_SQLSERVER_DRIVER_MISSING_TEST=1 and run as Administrator.

        Catalog:
            - FederatedQuery

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Wei Pan Add the Windows missing-driver behaviour check

        """
        if sys.platform != "win32":
            pytest.skip("Windows resolves the SQL Server driver through the registry")
        if os.environ.get("FQ_SQLSERVER_DRIVER_MISSING_TEST") != "1":
            pytest.skip("set FQ_SQLSERVER_DRIVER_MISSING_TEST=1 to rewrite the "
                        "machine-wide ODBC driver registration")
        if not _is_admin():
            pytest.skip("administrator privileges are required to rewrite HKLM")
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _ODBC_DRIVER_KEY, 0,
                                winreg.KEY_READ) as key:
                winreg.QueryValueEx(key, "Driver")
        except OSError:
            pytest.skip(f"'{_ODBC_DRIVER_NAME}' is not registered")

        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        self._with_std_sources(
            "fq23_sqlserver", self._check_sqlserver_without_driver,
            skip_mysql=True, skip_pg=True, skip_influx=True)
