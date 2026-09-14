import os
import re
import shlex
import subprocess
import sys
import time

import pytest

from new_test_framework.utils import tdCb, tdCom, tdLog, tdSql, tdStream


_TDENGINE_VERSION_RE = re.compile(
    r"^(?P<core>\d+\.\d+\.\d+\.\d+)"
    r"(?:\.alpha)?(?:\.(?:community|enterprise))?$"
)


def _normalize_tdengine_version(version):
    match = _TDENGINE_VERSION_RE.fullmatch(version)
    if match is None:
        raise AssertionError(f"unsupported TDengine version format: {version!r}")
    return match.group("core")


def _parse_version_output(output):
    rows = []
    for line in output.splitlines():
        columns = [column.strip() for column in line.split("|")]
        if len(columns) < 2:
            continue
        client_version, server_version = columns[:2]
        if _TDENGINE_VERSION_RE.fullmatch(
            client_version
        ) and _TDENGINE_VERSION_RE.fullmatch(server_version):
            rows.append((client_version, server_version))
    if len(rows) != 1:
        raise AssertionError(f"expected one client/server version row: {output!r}")
    return rows[0]


def _require_supported_lifecycle(os_name, platform_name):
    if os_name != "posix" or not platform_name.startswith("linux"):
        raise RuntimeError(
            "offline baseline lifecycle requires Linux/POSIX process control; "
            f"got os.name={os_name!r}, sys.platform={platform_name!r}"
        )


def _require_baseline_artifacts(bin_dir, lib_dir):
    taos = os.path.join(bin_dir, "taos")
    libtaos = os.path.join(lib_dir, "libtaos.so")
    if not os.path.isfile(taos) or not os.access(taos, os.X_OK):
        raise AssertionError(
            f"baseline taos executable is missing or not executable: {taos}"
        )
    if not os.path.isfile(libtaos):
        raise AssertionError(f"baseline libtaos.so is missing: {libtaos}")
    return taos, libtaos


BASE_VERSION = os.environ.get("STREAM_TIME_CONFIG_BASELINE_VERSION")
if not BASE_VERSION:
    pytest.skip(
        "STREAM_TIME_CONFIG_BASELINE_VERSION is required for offline upgrade coverage",
        allow_module_level=True,
    )

try:
    _require_supported_lifecycle(os.name, sys.platform)
except RuntimeError as err:
    pytest.skip(str(err), allow_module_level=True)
EXPECTED_BASE_VERSION = _normalize_tdengine_version(BASE_VERSION)
if BASE_VERSION != EXPECTED_BASE_VERSION:
    raise RuntimeError(
        "STREAM_TIME_CONFIG_BASELINE_VERSION must be an exact four-component "
        f"package version, got {BASE_VERSION!r}"
    )

class TestStreamTimeConfigUpgrade:
    DB = "stream_time_config_upgrade"
    STREAM = "legacy_user"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _assert_fully_stopped(self):
        remaining = {}
        for process in ("taosd", "taos", "taosadapter"):
            pids = tdCb._getProcessPids(process)
            if pids:
                remaining[process] = pids
        assert not remaining, f"offline handoff still has live processes: {remaining}"

    def _old_client_env(self):
        _, libtaos = _require_baseline_artifacts(
            tdCb.old_bin_dir, tdCb.old_lib_dir
        )
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = tdCb.old_lib_dir
        env["LD_PRELOAD"] = libtaos
        return env

    def _run_old_sql(self, sql, check=True):
        taos, _ = _require_baseline_artifacts(
            tdCb.old_bin_dir, tdCb.old_lib_dir
        )
        command = [
            taos,
            "-c",
            self.cfg_path,
            "-w",
            "4096",
            "-s",
            sql,
        ]
        tdLog.info(f"run baseline client: {shlex.join(command)}")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=60,
            env=self._old_client_env(),
            check=False,
        )
        if check:
            if result.stdout:
                tdLog.info(result.stdout)
            if result.stderr:
                tdLog.info(result.stderr)
            assert result.returncode == 0, (
                f"baseline SQL failed with {result.returncode}: {sql}"
            )
        return result

    def _wait_for_baseline(self):
        last_error = "baseline client did not run"
        for _ in range(60):
            result = self._run_old_sql(
                "select client_version(), server_version();", check=False
            )
            if result.returncode == 0:
                client_version, server_version = _parse_version_output(result.stdout)
                normalized_client = _normalize_tdengine_version(client_version)
                normalized_server = _normalize_tdengine_version(server_version)
                if (
                    normalized_client != EXPECTED_BASE_VERSION
                    or normalized_server != EXPECTED_BASE_VERSION
                ):
                    raise AssertionError(
                        "baseline version mismatch: "
                        f"expected={EXPECTED_BASE_VERSION!r}, "
                        f"client={client_version!r}, server={server_version!r}"
                    )
                tdLog.info(
                    "verified baseline versions: "
                    f"client={client_version}, server={server_version}"
                )
                return
            last_error = result.stderr.strip() or result.stdout.strip()
            time.sleep(1)
        raise AssertionError(
            f"baseline server {BASE_VERSION} did not become ready: {last_error}"
        )

    def _wait_for_old_stream(self):
        for _ in range(120):
            result = self._run_old_sql(
                "select status from information_schema.ins_streams "
                f"where stream_name='{self.STREAM}';",
                check=False,
            )
            if result.returncode == 0 and "Running" in result.stdout:
                return
            time.sleep(1)
        raise AssertionError("baseline user stream did not become Running")

    def _old_stream_sqls(self):
        result = self._run_old_sql(
            "select stream_name, sql from information_schema.ins_streams "
            f"where db_name='{self.DB}' order by stream_name;"
        )
        rows = {}
        for line in result.stdout.splitlines():
            columns = [column.strip() for column in line.split("|")]
            if len(columns) < 3 or columns[0] == "stream_name":
                continue
            if columns[1].lower().startswith("create"):
                rows[columns[0]] = columns[1]
        assert len(rows) == 2, result.stdout
        assert self.STREAM in rows, rows
        tsma_rows = {
            name: sql
            for name, sql in rows.items()
            if sql.lower().startswith("create tsma")
        }
        assert len(tsma_rows) == 1, rows
        return rows[self.STREAM], next(iter(tsma_rows.values()))

    def _current_metadata(self):
        tdSql.query(
            "select stream_name, `timezone`, `first_day_of_week`, `sql` "
            "from information_schema.ins_streams "
            f"where db_name='{self.DB}' order by stream_name"
        )
        rows = list(tdSql.queryResult)
        assert len(rows) == 2, rows
        user_rows = [row for row in rows if row[0] == self.STREAM]
        tsma_rows = [
            row for row in rows if row[3].strip().lower().startswith("create tsma")
        ]
        assert len(user_rows) == 1, rows
        assert len(tsma_rows) == 1, rows
        return user_rows[0], tsma_rows[0]

    def _assert_legacy_metadata(self, old_user_sql, old_tsma_sql):
        user, tsma = self._current_metadata()
        assert user[1] is None, user
        assert user[2] is None, user
        assert user[3] == old_user_sql, user
        assert "config(" not in user[3].lower(), user[3]
        assert tsma[1] is None, tsma
        assert tsma[2] is None, tsma
        assert tsma[3] == old_tsma_sql, tsma
        assert tsma[3].lower().startswith("create tsma"), tsma[3]
        assert "config(" not in tsma[3].lower(), tsma[3]
        tdSql.query(f"show {self.DB}.tsmas")
        tdSql.checkRows(1)

    def _start_current(self, build_path):
        self._assert_fully_stopped()
        tdCb.updateNewVersion(
            build_path,
            cPaths=[self.cfg_path.rstrip("/") + "/"],
            upgrade=2,
        )
        tdSql.connect()
        tdSql.execute(f"use {self.DB}")
        tdStream.checkStreamStatus(self.STREAM)

    def _restart_current_with_timezone(self, build_path, timezone):
        tdSql.execute(f"alter all dnodes 'timezone {timezone}'")
        tdCb.stopTaosdCompletely()
        self._assert_fully_stopped()
        self._start_current(build_path)

    def _wait_for_boundary(self, start):
        tdSql.checkResultsByFunc(
            sql=(
                "select cast(wstart as bigint), cnt "
                f"from {self.STREAM}_out "
                f"where cast(wstart as bigint)={start}"
            ),
            func=lambda: list(tdSql.queryResult) == [(start, 1)],
            retry=90,
        )

    def test_stream_time_config_offline_upgrade(self):
        """Legacy stream keeps raw SQL, NULL config, and dynamic timezone fallback.

        1. Run only the supplied pre-feature new-stream baseline binary.
        2. Create a user stream, TSMA, and data, then stop it completely.
        3. Start the current binary on the same data with no version overlap.
        4. Verify legacy metadata and timezone-dependent boundaries after restarts.

        Catalog:
            - Streams:Compatibility:OfflineUpgrade

        Since: v3.4.3.0

        Labels: common,ci,stream,integration,functional,compatibility,timezone
        Feishu: None

        History:
            - 2026-09-04 OpenAI GPT-5 Created
        """
        build_path = tdCom.getBuildPath()
        old_user_sql = None
        old_tsma_sql = None

        try:
            tdCb.stopTaosdCompletely()
            self._assert_fully_stopped()
            tdCb.installTaosd(build_path, self.cfg_path, BASE_VERSION)
            _require_baseline_artifacts(tdCb.old_bin_dir, tdCb.old_lib_dir)
            self._wait_for_baseline()

            baseline_sql = (
                "create database stream_time_config_upgrade vgroups 1;"
                "create snode on dnode 1;"
                "create table stream_time_config_upgrade.src "
                "(ts timestamp, v int);"
                "create stream stream_time_config_upgrade.legacy_user "
                "interval(1d) sliding(1d) from stream_time_config_upgrade.src "
                "into stream_time_config_upgrade.legacy_user_out as "
                "select _twstart as wstart, count(*) as cnt "
                "from stream_time_config_upgrade.src "
                "where ts >= _twstart and ts < _twend;"
                "create tsma legacy_tsma on stream_time_config_upgrade.src "
                "function(avg(v), count(ts)) interval(1m);"
                "insert into stream_time_config_upgrade.src values "
                "(1746057600000, 1),(1746144000001, 2);"
                "flush database stream_time_config_upgrade;"
            )
            self._run_old_sql(baseline_sql)
            self._wait_for_old_stream()
            old_user_sql, old_tsma_sql = self._old_stream_sqls()
            assert "config(" not in old_user_sql.lower(), old_user_sql
            assert old_tsma_sql.lower().startswith("create tsma"), old_tsma_sql
            assert "config(" not in old_tsma_sql.lower(), old_tsma_sql

            tdCb.stopTaosdCompletely()
            self._assert_fully_stopped()
            self._start_current(build_path)
            self._assert_legacy_metadata(old_user_sql, old_tsma_sql)

            self._restart_current_with_timezone(build_path, "UTC")
            tdSql.execute(
                "insert into src values "
                "(1748737800000, 3),(1748822400001, 4)"
            )
            utc_start = 1748736000000
            self._wait_for_boundary(utc_start)

            self._restart_current_with_timezone(build_path, "Asia/Shanghai")
            tdSql.execute(
                "insert into src values "
                "(1751329800000, 5),(1751385600001, 6)"
            )
            shanghai_start = 1751299200000
            self._wait_for_boundary(shanghai_start)
            assert 1748737800000 - utc_start == 1800000
            assert 1751329800000 - shanghai_start == 30600000
            self._assert_legacy_metadata(old_user_sql, old_tsma_sql)
        finally:
            tdCb.stopTaosdCompletely()
            self._assert_fully_stopped()
