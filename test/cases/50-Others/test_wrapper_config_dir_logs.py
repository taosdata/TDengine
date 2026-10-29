# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

import os
import subprocess
import sys
from pathlib import Path

import pytest

# The case runs a child process of itself, which pytest's import path does not cover:
# put the test tree on sys.path before the framework import, so both work.
_TEST_ROOT = str(Path(__file__).resolve().parents[2])
if _TEST_ROOT not in sys.path:
    sys.path.insert(0, _TEST_ROOT)

from new_test_framework.utils import tdLog, tdSql, etool

# LeakSanitizer's exit code.  An ASAN build reports every interpreter object of a
# process that loaded the instrumented library as leaked, so a helper that did its job
# would still exit non-zero in the ASAN CI job; the values it printed are what this
# case is about, and the container-level ASAN check keeps its own leak accounting.
LSAN_LEAK_EXIT_CODE = 23

# Preferred charset for the option case; a machine that already runs with it gets the
# other one, so reading the default back can never make the option look effective.
CHARSET = "GB18030"
FALLBACK_CHARSET = "UTF-8"

# Two log families, one per library, sharing the configured directory: libtaos (the
# driver dispatcher) must not reopen a file of the driver, so it uses its own base name.
WRAPPER_LOG_PREFIX = "taosloglite"
ENGINE_LOG_PREFIX = "taoslog"


def read_charset(cursor):
    cursor.execute("show local variables like 'charset'")
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"unexpected SHOW LOCAL VARIABLES result: {rows}")
    return rows[0][1]


def pick_charset(before):
    return FALLBACK_CHARSET if before.strip().upper() == CHARSET else CHARSET


def run_client(config_dir):
    """Child process: point the client at another taos.cfg and report what it sees."""
    import taos

    # taos_options(TSDB_OPTION_CONFIGDIR) is how every connector hands its own config
    # file to the client library, and it has to happen before the first connection,
    # because that is when libtaos reads taos.cfg and loads the driver.
    taos.taos_options(taos.TaosOption.ConfigDir, str(config_dir))

    conn = taos.connect(user="root", password="taosdata")
    try:
        cursor = conn.cursor()
        before = read_charset(cursor)
        target = pick_charset(before)
        # The charset is forwarded to the driver, which is what answers SHOW LOCAL
        # VARIABLES and what getTsCharset() reports to the JNI callers afterwards.
        taos.taos_options(taos.TaosOption.Charset, target)
        after = read_charset(cursor)
        print(f"charset_before={before}")
        print(f"charset_target={target}")
        print(f"charset_after={after}")
    finally:
        conn.close()


class TestWrapperConfigDirLogs:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------ #
    # util
    # ------------------------------------------------------------------ #
    def prepare_config(self, root):
        """Write a taos.cfg whose logDir is outside the default log directory."""
        config_dir = root / "cfg"
        log_dir = root / "logs"
        if " " in str(root):
            # taos.cfg values are whitespace separated tokens; the engine has no quoting
            # for logDir, so a temporary path with a space cannot be expressed here.
            pytest.skip(f"taos.cfg cannot carry a logDir with a space: {root}")
        config_dir.mkdir()
        config_dir.joinpath("taos.cfg").write_text(f"logDir {log_dir}\n", encoding="utf-8")
        return config_dir, log_dir

    def run_client_process(self, config_dir):
        """Run the client in a child process, so its library state is fresh.

        taos_options() applies to the client library of the calling process, and the
        pytest process has connected to the server already -- it can neither point
        libtaos at another config file nor reload it.
        """
        environment = os.environ.copy()
        # No leak detection in the helper (the other ASAN options stay): a short-lived
        # child that loads an ASAN build reports the CPython interpreter's own objects
        # as leaked on exit, which says nothing about libtaos but would fail this case.
        asan = [o for o in environment.get("ASAN_OPTIONS", "").split(":") if o and not o.startswith("detect_leaks=")]
        asan.append("detect_leaks=0")
        environment["ASAN_OPTIONS"] = ":".join(asan)

        process = subprocess.run(
            [sys.executable, __file__, "--client", str(config_dir)],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
            env=environment,
        )

        values = {}
        for line in process.stdout.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()

        missing = [k for k in ("charset_before", "charset_target", "charset_after") if not values.get(k)]
        assert not missing, (
            f"the client helper reported no {missing} (rc={process.returncode})\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
        # 0 normally, LSAN_LEAK_EXIT_CODE when an ASAN runtime decided to report leaks
        # anyway -- the three values above prove the client work itself succeeded.
        assert process.returncode in (0, LSAN_LEAK_EXIT_CODE), (
            f"the client helper failed with rc={process.returncode}\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
        return values

    def list_files(self, log_dir):
        if not log_dir.is_dir():
            return []
        return sorted(p.name for p in log_dir.iterdir() if p.is_file())

    # ------------------------------------------------------------------ #
    # impl
    # ------------------------------------------------------------------ #
    def do_logs_follow_configured_log_dir(self, log_dir):
        names = self.list_files(log_dir)
        wrapper_logs = [n for n in names if n.startswith(WRAPPER_LOG_PREFIX)]
        engine_logs = [
            n for n in names if n.startswith(ENGINE_LOG_PREFIX) and not n.startswith(WRAPPER_LOG_PREFIX)
        ]
        assert wrapper_logs, f"libtaos wrote no {WRAPPER_LOG_PREFIX}* log into {log_dir}: {names}"
        assert engine_logs, f"the driver wrote no {ENGINE_LOG_PREFIX}* log into {log_dir}: {names}"
        engine_bytes = sum((log_dir / n).stat().st_size for n in engine_logs)
        assert engine_bytes > 0, f"the driver's log in {log_dir} is empty: {names}"
        tdLog.info(f"logDir from taos.cfg, files: {names}")
        print("logDir from taos.cfg used by libtaos and the driver ... [ passed ]")

    def do_charset_option_reaches_driver(self, values):
        before = values.get("charset_before")
        target = values.get("charset_target")
        after = values.get("charset_after")
        assert before and target and after, f"no charset was reported: {values}"
        assert before != target, f"the client already runs with {target} before the option"
        assert after == target, f"charset after taos_options(CHARSET) is {after!r}, expected {target!r}"
        tdLog.info(f"charset before/after taos_options: {before} -> {after}")
        print("charset option seen by the driver in SHOW LOCAL VARIABLES ... [ passed ]")

    # ------------------------------------------------------------------ #
    # main
    # ------------------------------------------------------------------ #
    def test_config_dir_log_dir_and_charset(self, tmp_path):
        """Client options that point libtaos at another config file

        1. Write a taos.cfg whose logDir is not the default log directory
        2. Point libtaos at it with taos_options(TSDB_OPTION_CONFIGDIR) before the
           first connection
        3. Verify both log families land there: taosloglite* from libtaos and
           taoslog* from the driver it loads
        4. Change the charset with taos_options(TSDB_OPTION_CHARSET) and read it back
           with SHOW LOCAL VARIABLES

        Catalog:
            - Client

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-23 qevolg Cover configDir logDir hand-over and charset forwarding
        """
        config_dir, log_dir = self.prepare_config(tmp_path)
        values = self.run_client_process(config_dir)
        self.do_logs_follow_configured_log_dir(log_dir)
        self.do_charset_option_reaches_driver(values)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--client":
        run_client(sys.argv[2])
