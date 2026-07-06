###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""
Test querying virtual tables that reference different InfluxDB series
from the same measurement using the SERIES clause.

Scenario:
  - One InfluxDB measurement `machine_metrics` with fields: cpu, mem, disk
  - Multiple series distinguished by tags: (host, datacenter)
  - Virtual tables pin to specific series via SERIES clause
  - Each series uses UNIQUE timestamps (no overlap) so dedup doesn't mask data

Tests verified:
  1. CREATE VTABLE with SERIES clause succeeds (DDL)
  2. DESCRIBE shows correct series filter in ref column
  3. Virtual tables are queryable and return data
  4. Multi-SERIES vtable (columns from different series) works
  5. Series isolation: each vtable only returns its pinned series data
     (requires series filter push-down to InfluxDB query)
"""

import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import ExtSrcEnv


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INFLUX_DB = "test_series_query"
MEASUREMENT = "machine_metrics"
EXT_SOURCE_NAME = "influx_series_src"
TDENGINE_DB = "db_series_query"

BASE_TS = 1716000000  # 2024-05-18 approx, in seconds

# Series definitions: each series has UNIQUE timestamps (10s apart, offset
# between series by 100s) so there's no timestamp collision.
SERIES_DATA = {
    ("web01", "us-east"): [
        # (ts_offset_s, cpu, mem, disk)
        (100, 45.2, 72.1, 55.0),
        (110, 47.8, 73.5, 55.1),
        (120, 50.1, 71.0, 55.2),
        (130, 42.3, 70.8, 55.3),
        (140, 48.9, 74.2, 55.4),
    ],
    ("web02", "us-east"): [
        (200, 62.5, 85.3, 70.0),
        (210, 64.1, 86.0, 70.1),
        (220, 61.8, 84.7, 70.2),
        (230, 65.0, 87.1, 70.3),
        (240, 63.2, 85.9, 70.4),
    ],
    ("db01", "eu-west"): [
        (300, 30.0, 60.0, 80.0),
        (310, 31.5, 61.2, 80.5),
        (320, 29.8, 59.5, 81.0),
        (330, 32.1, 62.0, 81.5),
        (340, 30.9, 60.8, 82.0),
    ],
}


def _build_line_protocol():
    """Build InfluxDB line-protocol lines for all series."""
    lines = []
    for (host, dc), points in SERIES_DATA.items():
        for ts_offset, cpu, mem, disk in points:
            ts_ns = (BASE_TS + ts_offset) * 1_000_000_000
            lines.append(
                f"{MEASUREMENT},host={host},datacenter={dc} "
                f"cpu={cpu},mem={mem},disk={disk} {ts_ns}"
            )
    return lines


class TestVTableQueryExtSourceSeries:
    """Query tests for SERIES clause — same measurement, different series."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.start_influx_instance(ExtSrcEnv.INFLUX_VERSIONS[0])

    def setup_method(self, method):
        self._teardown()
        self._setup_influx_data()
        self._setup_tdengine()

    def teardown_method(self, method):
        self._teardown()

    def _setup_influx_data(self):
        """Write test data to InfluxDB (idempotent)."""
        ExtSrcEnv.influx_drop_db(INFLUX_DB)
        ExtSrcEnv.influx_create_db(INFLUX_DB)
        lines = _build_line_protocol()
        ExtSrcEnv.influx_write(INFLUX_DB, lines)
        # InfluxDB 3.x needs WAL→parquet snapshot for FlightSQL visibility
        tdLog.info("Waiting for InfluxDB WAL snapshot...")
        time.sleep(5)

    def _setup_tdengine(self):
        """Create TDengine DB, external source, and virtual tables with SERIES."""
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {TDENGINE_DB}")
        tdSql.execute(f"USE {TDENGINE_DB}")

        ExtSrcEnv.ensure_qnode()

        # External source
        influx_token = ExtSrcEnv._get_influx_token(ExtSrcEnv.INFLUX_VERSIONS[0])
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE IF NOT EXISTS {EXT_SOURCE_NAME} "
            f"TYPE='influxdb' "
            f"HOST='{ExtSrcEnv.INFLUX_HOST}' PORT={ExtSrcEnv.INFLUX_PORT} "
            f"USER='u' PASSWORD='' DATABASE={INFLUX_DB} "
            f"OPTIONS('api_token'='{influx_token}','protocol'='flight_sql')"
        )

        # VTable for web01/us-east series
        tdSql.execute(
            f"CREATE VTABLE IF NOT EXISTS vt_web01 ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s1.cpu,"
            f"  mem DOUBLE FROM s1.mem,"
            f"  disk DOUBLE FROM s1.disk"
            f") SERIES s1 AS {EXT_SOURCE_NAME}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='web01', datacenter='us-east')"
        )

        # VTable for web02/us-east series
        tdSql.execute(
            f"CREATE VTABLE IF NOT EXISTS vt_web02 ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s2.cpu,"
            f"  mem DOUBLE FROM s2.mem,"
            f"  disk DOUBLE FROM s2.disk"
            f") SERIES s2 AS {EXT_SOURCE_NAME}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='web02', datacenter='us-east')"
        )

        # VTable for db01/eu-west series
        tdSql.execute(
            f"CREATE VTABLE IF NOT EXISTS vt_db01 ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s3.cpu,"
            f"  mem DOUBLE FROM s3.mem,"
            f"  disk DOUBLE FROM s3.disk"
            f") SERIES s3 AS {EXT_SOURCE_NAME}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='db01', datacenter='eu-west')"
        )

        # VTable mixing columns from TWO different series (web01 cpu+mem,
        # db01 disk) — demonstrates multi-series in one vtable
        tdSql.execute(
            f"CREATE VTABLE IF NOT EXISTS vt_mixed_series ("
            f"  ts TIMESTAMP,"
            f"  web_cpu DOUBLE FROM sw.cpu,"
            f"  web_mem DOUBLE FROM sw.mem,"
            f"  db_disk DOUBLE FROM sd.disk"
            f") SERIES sw AS {EXT_SOURCE_NAME}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='web01', datacenter='us-east') "
            f"SERIES sd AS {EXT_SOURCE_NAME}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='db01', datacenter='eu-west')"
        )

    def _teardown(self):
        """Clean up TDengine objects."""
        tdSql.execute(f"DROP DATABASE IF EXISTS {TDENGINE_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {EXT_SOURCE_NAME}")

    # === Test methods ===

    def test_describe_shows_filter(self):
        """DESCRIBE shows the series filter in the ref column."""
        self._test_describe_shows_filter()

    def test_vtable_queryable(self):
        """Virtual tables with SERIES clause are queryable."""
        self._test_vtable_queryable()

    def test_mixed_series_queryable(self):
        """Multi-SERIES vtable is queryable."""
        self._test_mixed_series_queryable()

    def test_series_isolation(self):
        """Each vtable returns only its pinned series data."""
        self._test_series_isolation()

    def _test_describe_shows_filter(self):
        """DESCRIBE should show the series filter in the ref column."""
        tdLog.info("test_describe_shows_filter")
        tdSql.execute(f"USE {TDENGINE_DB}")
        tdSql.query("DESCRIBE vt_web01")
        # Find the cpu row and check its ref contains the filter
        found_filter = False
        for i in range(tdSql.queryRows):
            field = tdSql.getData(i, 0)
            if field and field.strip() == "cpu":
                ref = tdSql.getData(i, 4)
                assert ref is not None, "cpu column should have a ref"
                ref_str = str(ref).strip()
                assert "machine_metrics" in ref_str, \
                    f"ref should contain measurement name, got: {ref_str}"
                assert "host='web01'" in ref_str or "host=" in ref_str, \
                    f"ref should contain series filter, got: {ref_str}"
                found_filter = True
                break
        assert found_filter, "cpu column not found in DESCRIBE output"

    def _test_vtable_queryable(self):
        """Virtual tables with SERIES clause are queryable."""
        tdLog.info("test_vtable_queryable")
        tdSql.execute(f"USE {TDENGINE_DB}")

        # Each vtable should return data (at least 1 row)
        for vtable in ("vt_web01", "vt_web02", "vt_db01"):
            tdSql.query(f"SELECT COUNT(*) FROM {vtable}")
            count = tdSql.getData(0, 0)
            assert count > 0, f"{vtable} returned 0 rows"
            tdLog.info(f"  {vtable}: {count} rows")

        # Aggregation should work
        tdSql.query("SELECT AVG(cpu), MAX(cpu), MIN(cpu) FROM vt_web01")
        avg_val = tdSql.getData(0, 0)
        assert avg_val is not None and avg_val > 0, \
            f"AVG(cpu) should be positive, got {avg_val}"

        # WHERE filter should work
        tdSql.query("SELECT cpu FROM vt_web01 WHERE cpu > 40.0")
        assert tdSql.queryRows > 0, "WHERE filter returned no rows"

    def _test_mixed_series_queryable(self):
        """Multi-SERIES vtable (columns from 2 different series) is queryable.

        With series filter push-down active, sw (web01) and sd (db01) each
        return only their own rows.  web01 timestamps (offsets 100-140) and
        db01 timestamps (offsets 300-340) are disjoint, so the sort-merge
        produces 10 rows: the first 5 carry web_cpu/web_mem (db_disk NULL),
        the last 5 carry db_disk (web columns NULL).
        """
        tdLog.info("test_mixed_series_queryable")
        tdSql.execute(f"USE {TDENGINE_DB}")

        web_pts = SERIES_DATA[("web01", "us-east")]
        db_pts = SERIES_DATA[("db01", "eu-west")]
        expected_rows = len(web_pts) + len(db_pts)  # 10, timestamps disjoint

        tdSql.query("SELECT COUNT(*) FROM vt_mixed_series")
        count = tdSql.getData(0, 0)
        assert count == expected_rows, \
            f"vt_mixed_series: expected {expected_rows} rows, got {count}"
        tdLog.info(f"  vt_mixed_series: {count} rows")

        tdSql.query(
            "SELECT web_cpu, web_mem, db_disk FROM vt_mixed_series ORDER BY ts"
        )
        tdSql.checkRows(expected_rows)

        # First 5 rows: web01 series only (web columns set, db_disk NULL).
        for i, (_, cpu, mem, _disk) in enumerate(web_pts):
            tdSql.checkData(i, 0, cpu)
            tdSql.checkData(i, 1, mem)
            assert tdSql.getData(i, 2) is None, \
                f"row {i}: db_disk should be NULL for web01 ts, got {tdSql.getData(i, 2)}"

        # Last 5 rows: db01 series only (db_disk set, web columns NULL).
        for j, (_, _cpu, _mem, disk) in enumerate(db_pts):
            i = len(web_pts) + j
            assert tdSql.getData(i, 0) is None, \
                f"row {i}: web_cpu should be NULL for db01 ts, got {tdSql.getData(i, 0)}"
            assert tdSql.getData(i, 1) is None, \
                f"row {i}: web_mem should be NULL for db01 ts, got {tdSql.getData(i, 1)}"
            tdSql.checkData(i, 2, disk)

    def _test_series_isolation(self):
        """Each vtable must return ONLY its pinned series data.

        Series filter push-down emits the series tags as a WHERE clause in the
        external InfluxDB query, so each single-series vtable returns exactly
        its own 5 rows (not the full 15-row measurement).
        """
        tdLog.info("test_series_isolation")
        tdSql.execute(f"USE {TDENGINE_DB}")

        for vtable, (host, dc) in [
            ("vt_web01", ("web01", "us-east")),
            ("vt_web02", ("web02", "us-east")),
            ("vt_db01", ("db01", "eu-west")),
        ]:
            expected = SERIES_DATA[(host, dc)]
            tdSql.query(f"SELECT COUNT(*) FROM {vtable}")
            count = tdSql.getData(0, 0)
            assert count == len(expected), \
                f"{vtable}: expected {len(expected)} rows (series isolated), got {count}"

            tdSql.query(f"SELECT cpu, mem, disk FROM {vtable}")
            tdSql.checkRows(len(expected))
            actual = sorted(
                (tdSql.getData(i, 0), tdSql.getData(i, 1), tdSql.getData(i, 2))
                for i in range(tdSql.queryRows)
            )
            expected_values = sorted((cpu, mem, disk) for _, cpu, mem, disk in expected)
            assert actual == expected_values, \
                f"{vtable}: expected values {expected_values}, got {actual}"
            tdLog.info(f"  {vtable}: {count} rows, values isolated")
