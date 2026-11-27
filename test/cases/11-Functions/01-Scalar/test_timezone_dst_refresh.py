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

from new_test_framework.utils import tdLog, tdSql, etool

import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class TestTimezoneDstRefresh:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def run_transition(self, zone, boundary, before, after, mode,
                       host_timezone=None, client_timezone=None):
        suffix = ".exe" if sys.platform == "win32" else ""
        binaries = Path(self.taos_bin_path)
        if binaries.is_file():
            binaries = binaries.parent
        client = binaries / ("timezoneDstClient" + suffix)
        assert client.is_file(), f"Build timezoneDstClient (BUILD_TEST=ON): {client}"
        epoch = int(datetime.strptime(boundary, "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=timezone.utc).timestamp())
        env = os.environ.copy()
        # Isolate client initialization from inherited connection/driver options.
        for key in ("TZ", "_TDENGINE_INTERNAL_TZ_RULE", "TAOS_TIMEZONE",
                    "TDENGINE_DRIVER", "TDENGINE_DSN", "TDENGINE_CLOUD_DSN"):
            env.pop(key, None)
        if host_timezone is not None:
            env["TZ"] = host_timezone
        with tempfile.TemporaryDirectory(prefix="taos-dst-refresh-") as directory:
            config = Path(directory)
            default_zone = client_timezone or (zone if mode == "default" else "UTC")
            (config / "taos.cfg").write_text(
                f"timezone {default_zone}\n"
                f"logDir {config.as_posix()}/log\n"
                "asyncLog 0\ntelemetryReporting 0\n",
                encoding="utf-8",
            )
            result = subprocess.run(
                [str(client), str(config), self.host, str(self.port),
                 str(epoch), f"{zone} ({before})", f"{zone} ({after})",
                 mode, zone],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, errors="replace", timeout=90, check=False,
            )
        tdLog.info(f"host TZ={host_timezone}, client timezone={default_zone}, "
                   f"mode={mode}, zone={zone}, boundary={boundary}\n{result.stdout}")
        assert result.returncode == 0, result.stdout

    # --- impl ---

    def do_dst_transitions(self, mode):
        cases = [
            ("UTC", "2024-03-10 07:00:00", "UTC, +0000", "UTC, +0000"),
            ("America/New_York", "2024-03-10 07:00:00",
             "EST, -0500", "EDT, -0400"),
            ("America/New_York", "2024-11-03 06:00:00",
             "EDT, -0400", "EST, -0500"),
            ("Australia/Lord_Howe", "2024-04-06 15:00:00",
             "+11, +1100", "+1030, +1030"),
            ("Australia/Lord_Howe", "2024-10-05 15:30:00",
             "+1030, +1030", "+11, +1100"),
        ]
        for zone, boundary, before, after in cases:
            self.run_transition(zone, boundary, before, after, mode)
        print(f"TIMEZONE() {mode} live DST refresh ........ [ passed ]")

    def do_host_timezone_isolation(self, mode):
        cases = [
            ("America/New_York", "2024-01-15 12:00:00",
             "EST, -0500", "EST, -0500"),
            ("America/New_York", "2024-07-15 12:00:00",
             "EDT, -0400", "EDT, -0400"),
            ("America/New_York", "2024-03-10 07:00:00",
             "EST, -0500", "EDT, -0400"),
            ("America/New_York", "2024-11-03 06:00:00",
             "EDT, -0400", "EST, -0500"),
            ("Australia/Lord_Howe", "2024-01-15 12:00:00",
             "+11, +1100", "+11, +1100"),
            ("Australia/Lord_Howe", "2024-07-15 12:00:00",
             "+1030, +1030", "+1030, +1030"),
            ("Australia/Lord_Howe", "2024-04-06 15:00:00",
             "+11, +1100", "+1030, +1030"),
            ("Australia/Lord_Howe", "2024-10-05 15:30:00",
             "+1030, +1030", "+11, +1100"),
        ]
        for zone, boundary, before, after in cases:
            # Keep the host CRT, client default and explicit session distinct.
            self.run_transition(
                zone, boundary, before, after, mode,
                host_timezone="Asia/Shanghai",
                client_timezone="UTC" if mode == "session" else zone,
            )
        print(f"Timezone {mode} ignores host offsets ........ [ passed ]")

    # --- main ---

    def test_default_timezone_dst_refresh(self):
        """TIMEZONE() refreshes the default zone across DST on one connection

        1. Initialize the native client before each spring/fall transition
        2. Check SELECT timezone() before, at, and after the transition
        3. Repeat with a server table and a half-hour DST zone

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Codex Added client-clock DST transition regression
        """
        self.do_dst_transitions("default")

    def test_connection_timezone_lifecycle(self):
        """Connection timezone options preserve isolated rules and old results

        1. Check named zones, invalid inputs, empty strings and NULL
        2. Change ALTER LOCAL defaults and verify other connections keep snapshots
        3. Clear pooled connection state and validate connect_with options

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Codex Added connection timezone lifecycle regression
        """
        self.run_transition("America/New_York", "2024-03-10 07:00:00",
                            "EST, -0500", "EDT, -0400", "behavior")

    def test_session_timezone_dst_refresh(self):
        """TIMEZONE() refreshes a session zone across DST on one connection

        1. Set a session zone once before each transition
        2. Advance only the test client's clock without reconnecting
        3. Verify one-hour and half-hour offset changes in SQL results

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Codex Added session-clock DST transition regression
        """
        self.do_dst_transitions("session")

    def test_default_timezone_ignores_host_timezone(self):
        """Client timezone formatting overrides the host CRT timezone

        1. Set the child process TZ to Shanghai and configure another client zone
        2. Check exact TIMEZONE, TO_CHAR and TO_ISO8601 text with and without FROM
        3. Cover winter, summer and one-hour and half-hour DST transitions

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-03 Codex Added host timezone isolation regression
        """
        self.do_host_timezone_isolation("default")

    def test_session_timezone_ignores_host_timezone(self):
        """Session timezone formatting overrides client and host CRT timezones

        1. Use Shanghai for the host TZ and UTC for the client configuration
        2. Set a named session zone and verify all three SQL display functions
        3. Keep the connection alive across one-hour and half-hour DST changes

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-03 Codex Added session and host timezone isolation regression
        """
        self.do_host_timezone_isolation("session")
