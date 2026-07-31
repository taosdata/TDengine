import glob
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager

from new_test_framework.utils import tdCom, tdDnodes, tdLog, tdSql

from federated_query_common import ExtSrcEnv, FederatedQueryTestMixin, _code, _fq_subprocess_env


TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT = _code("TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT")
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


def _apply_runtime_exports(output):
    for line in output.splitlines():
        if not line.startswith("export "):
            continue
        key, value = line[len("export "):].split("=", 1)
        if key == "DYLD_LIBRARY_PATH" and sys.platform != "darwin":
            continue
        os.environ[key] = value.replace(f"${key}", os.environ.get(key, ""))


def _prepare_runtime_libs():
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


class TestFq19RuntimeLibs(FederatedQueryTestMixin):
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_prepare_runtime_libs_sanitizes_shell_helper_env(self, monkeypatch):
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

    def test_taos_cli_env_disables_leak_detection_for_asan_child(self, monkeypatch):
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

    def test_missing_influx_runtime_moves_arrow_third_party_libs(self, monkeypatch, tmp_path):
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

    def _query_count_with_taos(self, sql):
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
        for line in output.splitlines():
            if "|" not in line or line.lstrip().startswith("="):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            for cell in cells:
                if cell.isdigit():
                    return int(cell)
        raise AssertionError(output)

    def _check_remote_count(self, src):
        assert self._query_count_with_taos(f"select count(*) from {src}.src_t") == 5

    def _runtime_lib_dir(self):
        override = getattr(self, "_runtime_lib_dir_override", None)
        if override:
            return override
        taos_bin_path = os.environ.get("TAOS_BIN_PATH")
        if taos_bin_path:
            return os.path.abspath(os.path.join(taos_bin_path, "..", "lib"))
        return os.path.abspath(os.path.join(tdCom.getBuildPath(), "build", "lib"))

    def _path_is_under(self, path, root):
        try:
            return os.path.commonpath(
                [os.path.abspath(path), os.path.abspath(root)]
            ) == os.path.abspath(root)
        except ValueError:
            return False

    @contextmanager
    def _isolated_runtime_copy(self):
        runtime_dir = self._runtime_lib_dir()
        build_dir = os.path.dirname(runtime_dir)
        original_taosd_paths = [dnode.binPath for dnode in tdDnodes.dnodes]
        original_global_bin_path = getattr(tdDnodes, "binPath", None)
        original_taosd = original_taosd_paths[0]
        copy_root = tempfile.mkdtemp(prefix="fq-runtime-copy.")
        copy_build = os.path.join(copy_root, "build")
        copy_bin_dir = os.path.join(copy_build, "bin")
        copy_lib_dir = os.path.join(copy_build, "lib")
        old_runtime_override = getattr(self, "_runtime_lib_dir_override", None)
        try:
            shutil.copytree(os.path.join(build_dir, "bin"), copy_bin_dir, symlinks=True)
            shutil.copytree(runtime_dir, copy_lib_dir, symlinks=True)
            self._runtime_lib_dir_override = copy_lib_dir
            tdDnodes.binPath = os.path.join(copy_bin_dir, os.path.basename(original_taosd))
            for dnode in tdDnodes.dnodes:
                dnode.binPath = tdDnodes.binPath
            yield
        finally:
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

    @contextmanager
    def _without_provider_runtime_libs(self, providers, require_present=True):
        hidden_dir = tempfile.mkdtemp(prefix=".fq-runtime-hidden.", dir=self._runtime_lib_dir())
        moved = []
        try:
            for path in self._provider_runtime_files(providers):
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

    def _restart_taosd(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)

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
        if _try_prepare_runtime_libs():
            ExtSrcEnv.ensure_env()
            self._with_std_sources("fq19_runtime", self._check_remote_count_and_missing_runtime)
            self._with_influx_http_source(self._check_influx_http_missing_runtime)
        self._check_create_all_missing()
