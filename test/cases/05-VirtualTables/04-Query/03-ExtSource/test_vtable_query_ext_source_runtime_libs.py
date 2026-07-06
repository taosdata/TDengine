###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-
import glob
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager

from new_test_framework.utils import tdCom, tdDnodes, tdLog, tdSql

_DDL_EXT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "01-DDL", "03-ExtSource")
)
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "09-DataQuerying", "19-FederatedQuery")
)
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_influx_measurement,
    create_mysql_table,
    create_pg_table,
    create_remote_db,
)
from federated_query_common import _code  # noqa: E402


TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT = _code("TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT")

_LOCAL_DB = "vt_runtime_libs_local"
_MYSQL_DB = "vt_runtime_libs_mysql_db"
_MYSQL_SRC = "vt_runtime_libs_mysql_src"
_PG_DB = "vt_runtime_libs_pg_db"
_PG_SRC = "vt_runtime_libs_pg_src"
_INFLUX_DB = "vt_runtime_libs_influx_db"
_INFLUX_SRC = "vt_runtime_libs_influx_src"
_INFLUX_HTTP_SRC = "vt_runtime_libs_influx_http_src"
_T0 = 1704067200000000000

_PROVIDER_RUNTIME_PATTERNS = {
    "mysql": ("libmariadb*", "libmysqlclient*"),
    "postgresql": ("libpq*",),
    "influxdb": ("libtaos_ext_influx_arrow*", "libarrow*", "libparquet*"),
}
_PROVIDER_PLUGIN_PATTERNS = {
    "influxdb": ("libtaos_ext_influx_arrow*",),
}
_PROVIDER_RESOLVED_DEP_PREFIXES = {
    "influxdb": ("libarrow", "libparquet"),
}


def _runtime_lib_dir():
    override = getattr(_runtime_lib_dir, "override", None)
    if override:
        return override
    taos_bin_path = os.environ.get("TAOS_BIN_PATH")
    if taos_bin_path:
        return os.path.abspath(os.path.join(taos_bin_path, "..", "lib"))
    return os.path.abspath(os.path.join(tdCom.getBuildPath(), "build", "lib"))


def _existing_runtime_files(paths):
    files = []
    seen = set()
    for path in paths:
        normalized = os.path.normpath(os.path.abspath(path))
        if normalized in seen or not os.path.lexists(normalized):
            continue
        seen.add(normalized)
        files.append(normalized)
    return sorted(files)


def _path_is_under(path, root):
    try:
        return os.path.commonpath(
            [os.path.abspath(path), os.path.abspath(root)]
        ) == os.path.abspath(root)
    except ValueError:
        return False


def _provider_runtime_files(provider):
    runtime_dir = _runtime_lib_dir()
    files = []
    for pattern in _PROVIDER_RUNTIME_PATTERNS[provider]:
        files.extend(glob.glob(os.path.join(runtime_dir, pattern)))
        files.extend(glob.glob(os.path.join(runtime_dir, "**", pattern), recursive=True))
    files.extend(_resolved_runtime_dep_files(provider))
    return _existing_runtime_files(
        path for path in files if _path_is_under(path, runtime_dir)
    )


def _provider_plugin_files(provider):
    runtime_dir = _runtime_lib_dir()
    files = []
    for pattern in _PROVIDER_PLUGIN_PATTERNS.get(provider, ()):
        files.extend(glob.glob(os.path.join(runtime_dir, pattern)))
    return _existing_runtime_files(
        path for path in files if _path_is_under(path, runtime_dir)
    )


def _parse_ldd_resolved_files(output, prefixes):
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


def _runtime_dep_search_dirs(plugin):
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


def _resolved_runtime_dep_files(provider):
    prefixes = _PROVIDER_RESOLVED_DEP_PREFIXES.get(provider)
    if not prefixes or sys.platform == "darwin":
        return []

    files = []
    for plugin in _provider_plugin_files(provider):
        for dep_dir in _runtime_dep_search_dirs(plugin):
            for prefix in prefixes:
                files.extend(glob.glob(os.path.join(dep_dir, f"{prefix}*")))
        proc = subprocess.run(
            ["ldd", plugin],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="ignore",
        )
        files.extend(_parse_ldd_resolved_files(proc.stdout, prefixes))
    return files


@contextmanager
def _without_provider_runtime_libs(provider):
    runtime_dir = _runtime_lib_dir()
    hidden_dir = tempfile.mkdtemp(prefix=".fq-runtime-hidden.", dir=runtime_dir)
    moved = []
    try:
        for path in _provider_runtime_files(provider):
            hidden_path = os.path.join(hidden_dir, f"{len(moved)}-{os.path.basename(path)}")
            shutil.move(path, hidden_path)
            moved.append((hidden_path, path))
        if not moved:
            raise AssertionError(f"no runtime libraries found in {runtime_dir} for {provider}")
        yield
    finally:
        for hidden_path, path in reversed(moved):
            if os.path.lexists(hidden_path) and not os.path.lexists(path):
                shutil.move(hidden_path, path)
        try:
            os.rmdir(hidden_dir)
        except OSError:
            pass


def _runtime_dir_is_writable():
    runtime_dir = _runtime_lib_dir()
    try:
        fd, path = tempfile.mkstemp(prefix=".fq-write-check.", dir=runtime_dir)
        os.close(fd)
        os.unlink(path)
        return True
    except OSError:
        return False


@contextmanager
def _writable_runtime_copy_if_needed():
    if _runtime_dir_is_writable():
        yield
        return

    runtime_dir = _runtime_lib_dir()
    build_dir = os.path.dirname(runtime_dir)
    original_taosd = tdDnodes.dnodes[0].binPath
    copy_root = tempfile.mkdtemp(prefix="fq-runtime-copy.")
    copy_build = os.path.join(copy_root, "build")
    copy_bin_dir = os.path.join(copy_build, "bin")
    copy_lib_dir = os.path.join(copy_build, "lib")
    old_override = getattr(_runtime_lib_dir, "override", None)
    try:
        shutil.copytree(os.path.join(build_dir, "bin"), copy_bin_dir, symlinks=True)
        shutil.copytree(runtime_dir, copy_lib_dir, symlinks=True)
        _runtime_lib_dir.override = copy_lib_dir
        tdDnodes.binPath = os.path.join(copy_bin_dir, os.path.basename(original_taosd))
        for dnode in tdDnodes.dnodes:
            dnode.binPath = tdDnodes.binPath
        yield
    finally:
        if old_override is None:
            if hasattr(_runtime_lib_dir, "override"):
                delattr(_runtime_lib_dir, "override")
        else:
            _runtime_lib_dir.override = old_override
        tdDnodes.binPath = original_taosd
        for dnode in tdDnodes.dnodes:
            dnode.binPath = original_taosd
        _restart_taosd()
        shutil.rmtree(copy_root, ignore_errors=True)


def _restart_taosd():
    tdDnodes.stop(1)
    tdDnodes.start(1)


def _check_count(sql, expected):
    tdSql.query(sql)
    tdSql.checkData(0, 0, expected)


def _create_influx_source(src, protocol):
    token = ExtSrcEnv._get_influx_token(ExtSrcEnv.INFLUX_VERSIONS[0])
    tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {src}")
    tdSql.execute(
        f"CREATE EXTERNAL SOURCE {src} TYPE='influxdb' "
        f"HOST='{ExtSrcEnv.INFLUX_HOST}' PORT={ExtSrcEnv.INFLUX_PORT} "
        f"API_TOKEN='{token}' "
        f"DATABASE={_INFLUX_DB} OPTIONS('protocol'='{protocol}')"
    )


class TestVtableQueryExtSourceRuntimeLibs:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        create_remote_db("mysql", _MYSQL_DB)
        create_mysql_table(
            _MYSQL_DB,
            "r_mysql",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01 00:00:00.000', 11)", "('2024-01-01 00:00:01.000', 12)"],
        )
        create_ext_source(_MYSQL_SRC, "mysql", _MYSQL_DB)

        create_remote_db("postgresql", _PG_DB)
        create_pg_table(
            _PG_DB,
            "r_pg",
            "ts TIMESTAMP PRIMARY KEY, v INTEGER",
            ["('2024-01-01 00:00:00', 21)", "('2024-01-01 00:00:01', 22)"],
        )
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)

        create_remote_db("influxdb", _INFLUX_DB)
        create_influx_measurement(
            _INFLUX_DB,
            [f"r_influx v=31i {_T0}", f"r_influx v=32i {_T0 + 1000000000}"],
        )
        create_ext_source(_INFLUX_SRC, "influxdb", _INFLUX_DB)
        _create_influx_source(_INFLUX_HTTP_SRC, "http")

        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(
            f"CREATE VTABLE v_mysql (ts timestamp, v int FROM {_MYSQL_SRC}.{_MYSQL_DB}.r_mysql.v)"
        )
        tdSql.execute(
            f"CREATE VTABLE v_pg (ts timestamp, v int FROM {_PG_SRC}.{_PG_DB}.r_pg.v)"
        )
        tdSql.execute(
            f"CREATE VTABLE v_influx (ts timestamp, v bigint FROM {_INFLUX_SRC}.{_INFLUX_DB}.r_influx.v)"
        )
        tdSql.execute(
            "CREATE VTABLE v_influx_http "
            f"(ts timestamp, v bigint FROM {_INFLUX_HTTP_SRC}.{_INFLUX_DB}.r_influx.v)"
        )

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_MYSQL_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INFLUX_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INFLUX_HTTP_SRC}")

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    def _check_vtable_fails_without_runtime_libs(self, provider, tables):
        if isinstance(tables, str):
            tables = (tables,)
        for table in tables:
            _check_count(f"SELECT count(*) FROM {table}", 2)
        with _writable_runtime_copy_if_needed():
            try:
                with _without_provider_runtime_libs(provider):
                    _restart_taosd()
                    tdSql.execute(f"USE {_LOCAL_DB}")
                    for table in tables:
                        tdSql.error(
                            f"SELECT count(*) FROM {table}",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT,
                        )
            finally:
                _restart_taosd()
                tdSql.execute(f"USE {_LOCAL_DB}")
        for table in tables:
            _check_count(f"SELECT count(*) FROM {table}", 2)

    def do_missing_runtime_libs_fail_vtable_queries(self):
        self._check_vtable_fails_without_runtime_libs("mysql", "v_mysql")
        self._check_vtable_fails_without_runtime_libs("postgresql", "v_pg")
        self._check_vtable_fails_without_runtime_libs(
            "influxdb", ("v_influx", "v_influx_http")
        )
        print("missing runtime libraries fail vtable queries ......................... [ passed ]")

    def test_missing_runtime_libs_fail_vtable_queries(self):
        """Check ext-source virtual table queries fail when provider runtime libraries are missing

        1. Create MySQL, PostgreSQL, and InfluxDB flight_sql/HTTP external-source virtual tables
        2. Verify each virtual table query succeeds while runtime libraries exist
        3. Hide the provider runtime libraries and restart taosd
        4. Verify each virtual table query fails with provider-not-initialized error
        5. Restore the runtime libraries and verify each virtual table query recovers

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-04 Codex Add ext-source virtual table missing runtime coverage

        """
        self.do_missing_runtime_libs_fail_vtable_queries()
