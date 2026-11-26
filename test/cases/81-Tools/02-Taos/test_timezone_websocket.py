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

from new_test_framework.utils import tdLog, tdSql, etool

import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class TestTimezoneWebsocket:
    updatecfgDict = {
        "timezone": "America/New_York",
        "clientCfg": {"timezone": "America/New_York"},
    }
    dbname = "tz_websocket"
    fmt = "YYYY-MM-DD HH24:MI:SS TZH TZ"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def epoch_ms(self, text):
        stamp = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return int(stamp.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def cli(self, sql, mode=1):
        env = os.environ.copy()
        for key in (
            "TAOS_TIMEZONE", "TDENGINE_DRIVER", "TDENGINE_DSN",
            "TDENGINE_CLOUD_DSN",
        ):
            env.pop(key, None)
        with tempfile.TemporaryDirectory(prefix="taos-tz-") as directory:
            config = Path(directory)
            (config / "taos.cfg").write_text(
                f"firstEp {self.host}:{self.port}\n"
                "timezone America/New_York\n"
                f"logDir {config.as_posix()}/log\n",
                encoding="utf-8",
            )
            command = [
                etool.taosFile(), "-Z", str(mode), "-c", str(config),
                "-u", "root", "-ptaosdata", "-w", "200", "-T", "10",
            ]
            if mode == 1:
                command += ["-X", f"http://{self.host}:6041"]
            else:
                command += ["-h", self.host, "-P", str(self.port)]
            result = subprocess.run(
                command + ["-s", sql], env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, errors="replace", timeout=90, check=False,
            )
        output = result.stdout
        assert result.returncode == 0, output
        assert "DB error:" not in output, output
        assert "failed to connect" not in output.lower(), output
        return output

    def expect_rows(self, sql, expected):
        for mode in (1, 0):
            output = self.cli(sql, mode)
            rows = [line.strip().strip("|").strip()
                    for line in output.splitlines()]
            actual = [row for row in rows if row.startswith("tzcase:")]
            assert actual == expected, (
                f"-Z {mode}: expected {expected}, got {actual}\n{output}"
            )

    def format_query(self, stamp, zone):
        return (
            "select concat('tzcase:', "
            f"to_char(cast({stamp} as timestamp), '{self.fmt}', "
            f"'{zone}')) as actual;"
        )

    # --- impl ---

    def do_configured_dst_roundtrip(self):
        cases = [
            ("2024-01-15 12:00:00", "2024-01-15 07:00:00 -05 -05:00"),
            ("2024-03-10 06:59:59", "2024-03-10 01:59:59 -05 -05:00"),
            ("2024-03-10 07:00:00", "2024-03-10 03:00:00 -04 -04:00"),
            ("2024-07-15 12:00:00", "2024-07-15 08:00:00 -04 -04:00"),
            ("2024-11-03 05:30:00", "2024-11-03 01:30:00 -04 -04:00"),
            ("2024-11-03 06:30:00", "2024-11-03 01:30:00 -05 -05:00"),
        ]
        self.cli(f"drop database if exists {self.dbname};")
        try:
            self.cli(
                f"create database {self.dbname} precision 'ms' "
                "keep 36500;"
                f"create table {self.dbname}.t (ts timestamp, v int);"
            )
            values = " ".join(
                f"({self.epoch_ms(utc)}, {index})"
                for index, (utc, _) in enumerate(cases)
            )
            self.cli(f"insert into {self.dbname}.t values {values};")
            self.expect_rows(
                "select concat('tzcase:', "
                f"to_char(ts, '{self.fmt}')) as actual "
                f"from {self.dbname}.t order by ts;",
                [f"tzcase:{local}" for _, local in cases],
            )
            self.cli(
                f"insert into {self.dbname}.t "
                "values ('2024-01-15 07:00:01', 99);"
            )
            self.expect_rows(
                "select concat('tzcase:', "
                "cast(cast(ts as bigint) as varchar)) as actual "
                f"from {self.dbname}.t where v = 99;",
                [f"tzcase:{self.epoch_ms('2024-01-15 12:00:01')}"],
            )
        finally:
            self.cli(f"drop database if exists {self.dbname};")
        print("WebSocket configured IANA/DST roundtrip ........ [ passed ]")

    def do_explicit_historical_timezones(self):
        cases = [
            ("1988-01-01 00:00:00", "Asia/Shanghai",
             "1988-01-01 08:00:00 +08 +08:00"),
            ("1988-07-01 00:00:00", "Asia/Shanghai",
             "1988-07-01 09:00:00 +09 +09:00"),
            ("2010-07-01 00:00:00", "Europe/Moscow",
             "2010-07-01 04:00:00 +04 +04:00"),
            ("2024-07-01 00:00:00", "Europe/Moscow",
             "2024-07-01 03:00:00 +03 +03:00"),
            ("2024-01-15 12:00:00", "Australia/Lord_Howe",
             "2024-01-15 23:00:00 +11 +11:00"),
            ("2024-07-15 12:00:00", "Australia/Lord_Howe",
             "2024-07-15 22:30:00 +10 +10:30"),
            ("2024-01-15 12:00:00", "UTC-8",
             "2024-01-15 20:00:00 +08 +08:00"),
            ("2024-01-15 12:00:00", "UTC+12",
             "2024-01-15 00:00:00 -12 -12:00"),
        ]
        self.expect_rows(
            "".join(self.format_query(self.epoch_ms(utc), zone)
                    for utc, zone, _ in cases),
            [f"tzcase:{local}" for _, _, local in cases],
        )
        print("WebSocket historical IANA and POSIX offsets .... [ passed ]")

    def do_negative_fractional_seconds(self):
        for precision, fraction in (
            ("ms", "999000000"), ("us", "999999000"),
            ("ns", "999999999"),
        ):
            database = f"{self.dbname}_{precision}"
            self.cli(f"drop database if exists {database};")
            try:
                self.cli(
                    f"create database {database} precision '{precision}' "
                    "keep 36500;"
                    f"create table {database}.t (ts timestamp, v int);"
                    f"insert into {database}.t values (-1, 1);"
                )
                self.expect_rows(
                    "select concat('tzcase:', "
                    "to_char(ts, 'YYYY-MM-DD HH24:MI:SS.NS TZH TZ', "
                    "'America/New_York')) as actual "
                    f"from {database}.t;",
                    [f"tzcase:1969-12-31 18:59:59.{fraction} -05 -05:00"],
                )
            finally:
                self.cli(f"drop database if exists {database};")
        print("WebSocket negative fractions in ms/us/ns ....... [ passed ]")

    # --- main ---

    def test_configured_dst_roundtrip(self):
        """IANA configuration applies to WebSocket writes and SQL formatting.

        1. Write numeric timestamps through taos -Z 1 and read via both drivers.
        2. Check winter, summer, spring gap and both occurrences of a fall fold.
        3. Write a local timestamp string and verify its stored UTC epoch.

        Catalog:
            - Tools:taos

        Since: v3.4.3.0

        Labels: common,ci,windows,timezone,websocket

        Jira: None

        History:
            - 2026-10-16 Add IANA timezone WebSocket regression coverage

        """
        self.do_configured_dst_roundtrip()

    def test_historical_and_negative_timestamps(self):
        """SQL timezone parameters preserve historical and fractional offsets.

        1. Check historical Shanghai/Moscow and half-hour Lord Howe rules.
        2. Check POSIX fixed offsets, including a colliding Windows zone name.
        3. Check pre-1970 fractional timestamps at all three precisions.

        Catalog:
            - Tools:taos

        Since: v3.4.3.0

        Labels: common,ci,windows,timezone,websocket

        Jira: None

        History:
            - 2026-10-16 Add historical and negative timestamp SQL coverage
            - 2026-10-28 Fix negative timestamp format and precision coverage

        """
        self.do_explicit_historical_timezones()
        self.do_negative_fractional_seconds()
