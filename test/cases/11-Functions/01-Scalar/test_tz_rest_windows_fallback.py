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

from new_test_framework.utils import tdLog, tdSql

import sys
import os

import pytest
import requests
import taosws
from urllib.parse import quote


pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="requires the Windows timezone implementation",
)


class TestTzRestWindowsFallback:
    updatecfgDict = {
        "timezone": "UTC-8",
        "clientCfg": {"timezone": "UTC-8"},
    }

    dbname = "db_tz_rest_windows"
    timestamp_ms = 1641038400000  # 2022-01-01 12:00:00 UTC

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        if not cls.restful:
            pytest.skip("requires REST mode (-R)")

    # --- util ---

    def prepare_data(self):
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(f"create database {self.dbname} precision 'ms'")
        tdSql.execute(f"create table {self.dbname}.t1 (ts timestamp, v int)")
        tdSql.execute(
            f"insert into {self.dbname}.t1 values ({self.timestamp_ms}, 1)"
        )

    # --- impl ---

    def do_windows_ws_connection_isolation(self):
        adapter_port = os.getenv("TAOS_ADAPTER_TEST_PORT", "6041")
        base = f"taosws://root:taosdata@{self.host}:{adapter_port}/"
        connections = []
        sql = "select to_iso8601(cast(1705320000000 as timestamp))"
        try:
            for zone, expected in (
                ("America/New_York", "2024-01-15T07:00:00.000-0500"),
                ("Asia/Shanghai", "2024-01-15T20:00:00.000+0800"),
                (None, "2024-01-15T20:00:00.000+0800"),
            ):
                dsn = base + ("?timezone=" + quote(zone, safe="") if zone else "")
                connection = taosws.connect(dsn)
                connections.append(connection)
                cursor = connection.cursor()
                cursor.execute(sql)
                assert cursor.fetchone()[0] == expected
                cursor.close()
            cursor = connections[0].cursor()
            cursor.execute("set timezone 'UTC'")
            cursor.execute(sql)
            assert cursor.fetchone()[0] in ("2024-01-15T12:00:00.000Z",
                                            "2024-01-15T12:00:00.000+0000")
            cursor.close()
            cursor = connections[1].cursor()
            cursor.execute(sql)
            assert cursor.fetchone()[0] == "2024-01-15T20:00:00.000+0800"
            cursor.close()
        finally:
            for connection in connections:
                connection.close()
        print("Windows WebSocket connection isolation ...... [ passed ]")

    def do_windows_rest_connection_reuse(self):
        adapter_port = os.getenv("TAOS_ADAPTER_TEST_PORT", "6041")
        url = f"http://{self.host}:{adapter_port}/rest/sql"
        sql = "select to_iso8601(cast(1705320000000 as timestamp))"
        with requests.Session() as session:
            session.auth = ("root", "taosdata")

            def query(statement, zone=None):
                params = {} if zone is None else {"conn_tz": zone}
                response = session.post(url, params=params, data=statement.encode("utf-8"), timeout=30)
                response.raise_for_status()
                payload = response.json()
                assert payload.get("code", 0) == 0, payload
                return payload.get("data", [])

            baseline = query(sql)[0][0]
            cases = [
                ("America/New_York", "2024-01-15T07:00:00.000-0500"),
                (None, baseline),
                ("Asia/Shanghai", "2024-01-15T20:00:00.000+0800"),
                (None, baseline),
                ("UTC", "2024-01-15T12:00:00.000Z"),
                (None, baseline),
            ]
            for zone, expected in cases:
                actual = query(sql, zone)[0][0]
                # UTC may be rendered with either supported zero-offset suffix.
                if zone == "UTC":
                    assert actual in (expected, expected[:-1] + "+0000"), actual
                else:
                    assert actual == expected, (zone, actual, expected)
        print("Windows REST connection reuse ............... [ passed ]")

    def do_windows_rest_timezone_fallback(self):
        self.prepare_data()
        try:
            tdSql.query(
                "select "
                "timezone(), "
                "to_iso8601(ts), "
                "to_iso8601(ts, '+0800'), "
                "to_char(ts, 'YYYY-MM-DD HH24:MI:SS'), "
                "to_char(ts, 'YYYY-MM-DD HH24:MI:SS', '-0800'), "
                "cast(timetruncate(ts, 1d) as bigint), "
                "cast(timetruncate(ts, 1d, '-0800') as bigint) "
                f"from {self.dbname}.t1"
            )
            tdSql.checkRows(1)

            # Windows config UTC-8 uses POSIX TZ=UTC-08:00, while the display
            # and TO_ISO8601 suffix use the ISO east-positive +0800 form.
            tdSql.checkData(0, 0, "UTC-8 (UTC, +0800)")
            tdSql.checkData(0, 1, "2022-01-01T20:00:00.000+0800")
            tdSql.checkData(0, 2, "2022-01-01T20:00:00.000+0800")
            tdSql.checkData(0, 3, "2022-01-01 20:00:00")
            tdSql.checkData(0, 4, "2022-01-01 20:00:00")

            # Local midnight at UTC+8 is 2021-12-31 16:00:00 UTC.
            tdSql.checkData(0, 5, 1640966400000)
            tdSql.checkData(0, 6, 1640966400000)
        finally:
            tdSql.execute(f"drop database if exists {self.dbname}")

        print("Windows REST timezone fallback .............. [ passed ]")

    def do_windows_rest_to_char_tzh(self):
        self.prepare_data()
        try:
            # Query from the defect report: over a REST connection the wall
            # clock was rendered in UTC while TZH still printed +08.
            tdSql.query(
                "select "
                "to_char(_wstart, 'YYYY-MM-DD HH24:MI:SS'), "
                "to_char(_wend, 'YYYY-MM-DD HH24:MI:SSTZH'), "
                "to_char(_wend, 'YYYY-MM-DD HH24:MI:SSTZH', '-0800'), "
                "to_iso8601(_wend) "
                f"from {self.dbname}.t1 interval(1d)"
            )
            tdSql.checkRows(1)

            # Windows align to local midnight at UTC+8, and _wend is the
            # exclusive end (window start + 1d).
            tdSql.checkData(0, 0, "2022-01-01 00:00:00")

            # The TZH offset and the wall clock in front of it must agree:
            # local time at UTC+8 followed by the east-positive offset.
            tdSql.checkData(0, 1, "2022-01-02 00:00:00+08")
            tdSql.checkData(0, 2, "2022-01-02 00:00:00+08")
            tdSql.checkData(0, 3, "2022-01-02T00:00:00.000+0800")
        finally:
            tdSql.execute(f"drop database if exists {self.dbname}")

        print("Windows REST TO_CHAR TZH .................... [ passed ]")

    # --- main ---

    def test_tz_rest_windows_fallback(self):
        """Windows REST connections use the configured fixed-offset timezone.

        1. Verify the Windows UTC-8 display uses ISO +0800.
        2. Verify implicit TO_ISO8601 matches explicit ISO +0800.
        3. Verify implicit TO_CHAR matches explicit POSIX -0800.
        4. Verify implicit TIMETRUNCATE matches explicit POSIX -0800.
        5. Verify TO_CHAR(_wend, '...TZH') prints local time plus +08.
        6. Alternate REST timezones and verify pooled requests without a timezone.
        7. Verify WebSocket connection parameters and SET TIMEZONE stay isolated.

        Catalog:
            - Function:timezone

        Since: v3.4.2.0

        Labels: common,ci,windows,rest,timezone

        Jira: None

        History:
            - 2026-07-28 Tony Zhang created
            - 2026-07-30 Tony Zhang add the TZH query from the defect report

        """
        self.do_windows_rest_timezone_fallback()
        self.do_windows_rest_to_char_tzh()
        self.do_windows_rest_connection_reuse()
        self.do_windows_ws_connection_isolation()
