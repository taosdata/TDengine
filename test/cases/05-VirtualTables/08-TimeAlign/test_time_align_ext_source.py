# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""TIME_ALIGN over a virtual table backed by an external (PostgreSQL) source.

The virtual-table rowset and column refs come from a real PostgreSQL table,
so this covers the federated scan path: PREV must align the remote values on
the merged virtual-table timeline exactly like a local source.

The raw external row is queried first and used as the reference for every
expected aligned value, so the case is independent of the session timezone
and of the remote timestamp precision.

Run with FQ_REQUIRED_SERVICES=pg (the CI task does); otherwise it skips.
"""

import os
import sys

import pytest

from new_test_framework.utils import tdLog, tdSql

_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
_TEST_DIR = os.path.abspath(os.path.join(_CASE_DIR, "..", "..", ".."))
sys.path.insert(0, os.path.join(_TEST_DIR, "cases", "09-DataQuerying", "19-FederatedQuery"))

from federated_query_common import ExtSrcEnv  # noqa: E402

PAR_INVALID_COLUMN = 0x80002602


class TestTimeAlignExtSource:
    VTABLE_DB = "ta_align_ext"
    SOURCE = "ta_align_pg"
    REMOTE_DB = "ta_align_ext_src"

    @classmethod
    def setup_class(cls):
        services = [item.strip() for item in os.environ.get("FQ_REQUIRED_SERVICES", "").split(",")]
        if "pg" not in services:
            pytest.skip("pg is not in FQ_REQUIRED_SERVICES")
        if not ExtSrcEnv._tcp_probe(ExtSrcEnv.PG_HOST, ExtSrcEnv.PG_PORT):
            ExtSrcEnv.start_pg_instance(ExtSrcEnv.PG_VERSIONS[0])
        ExtSrcEnv.ensure_qnode()

        ExtSrcEnv.pg_create_db(cls.REMOTE_DB)
        ExtSrcEnv.pg_exec(cls.REMOTE_DB, [
            "DROP TABLE IF EXISTS t_ext",
            "CREATE TABLE t_ext ("
            "ts timestamp(3) PRIMARY KEY, voltage double precision, "
            "current double precision, status integer)",
            "INSERT INTO t_ext VALUES "
            "('2024-01-01 00:00:00', 1, 10, 1),"
            "('2024-01-01 00:00:01', NULL, 20, NULL),"
            "('2024-01-01 00:00:02', 3, NULL, 3),"
            "('2024-01-01 00:00:04', 5, 50, 4)",
        ])

        tdSql.execute(f"drop database if exists {cls.VTABLE_DB}")
        tdSql.execute(f"create database {cls.VTABLE_DB}")
        tdSql.execute(f"use {cls.VTABLE_DB}")
        tdSql.execute(f"drop external source if exists {cls.SOURCE}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {cls.SOURCE} TYPE='postgresql' "
            f"HOST='{ExtSrcEnv.PG_HOST}' PORT={ExtSrcEnv.PG_PORT} "
            f"USER='{ExtSrcEnv.PG_USER}' PASSWORD='{ExtSrcEnv.PG_PASS}' "
            f"DATABASE={cls.REMOTE_DB} SCHEMA=public")
        tdSql.execute(
            f"create vtable vt_ext (ts timestamp, "
            f"voltage double from {cls.SOURCE}.{cls.REMOTE_DB}.t_ext.voltage, "
            f"current double from {cls.SOURCE}.{cls.REMOTE_DB}.t_ext.current, "
            f"status int from {cls.SOURCE}.{cls.REMOTE_DB}.t_ext.status)")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {cls.VTABLE_DB}")
        tdSql.execute(f"drop external source if exists {cls.SOURCE}")
        try:
            ExtSrcEnv.pg_drop_db(cls.REMOTE_DB)
        except Exception as e:
            tdLog.info(f"drop remote PG database skipped: {e}")

    @staticmethod
    def _raw_rows():
        tdSql.execute(f"use {TestTimeAlignExtSource.VTABLE_DB}")
        tdSql.query("select ts, voltage, current, status from vt_ext order by ts")
        return [list(row) for row in tdSql.queryResult]

    @staticmethod
    def _aligned_rows(raw_rows):
        last = [None, None, None]
        rows = []
        for row in raw_rows:
            current = [row[0]]
            for index in range(1, 4):
                if row[index] is not None:
                    last[index - 1] = row[index]
                current.append(last[index - 1])
            rows.append(current)
        return rows

    @staticmethod
    def _ts_ms(value):
        if isinstance(value, (int, float)):
            return int(value)
        return int(value.timestamp() * 1000)

    @classmethod
    def _window_start(cls, value, interval_ms=2000):
        return cls._ts_ms(value) // interval_ms * interval_ms

    def test_default_alignment(self):
        """PREV aligns remote columns exactly like local sources.

        The remote rows are merged on the vtable timeline, and every output row
        must carry the last non-NULL remote value for the aligned columns.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, postgres, ci
        """
        raw = self._raw_rows()
        tdSql.checkRows(4)
        expected = self._aligned_rows(raw)
        tdSql.query("select * from table(time_align(table vt_ext))")
        tdSql.checkRows(len(expected))
        assert tdSql.queryCols == 4, f"unexpected column count: {tdSql.queryCols}"
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def test_output_raw_mixed_aggregate(self):
        """Raw and aligned remote columns in one statement.

        Raw columns and their PREV-aligned counterparts are projected and
        aggregated side by side, checking both column order and aggregate
        semantics over the remote rowset.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, postgres, mixed, ci
        """
        raw = self._raw_rows()
        aligned = self._aligned_rows(raw)

        # output_raw=TRUE: raw columns then aligned columns.
        tdSql.query("select * from table(time_align(table vt_ext, TRUE, '*::_a'))")
        tdSql.checkRows(4)
        assert tdSql.queryCols == 7, f"unexpected column count: {tdSql.queryCols}"
        for row_index in range(4):
            for col_index in range(3):
                tdSql.checkData(row_index, 1 + col_index, raw[row_index][1 + col_index])
                tdSql.checkData(row_index, 4 + col_index, aligned[row_index][1 + col_index])

        # Raw vs aligned aggregates in the same SELECT.
        raw_voltage = [row[1] for row in raw if row[1] is not None]
        aligned_voltage = [row[1] for row in aligned]
        expected = [[4, len(raw_voltage), 4, sum(raw_voltage), sum(aligned_voltage)]]
        tdSql.query(
            "select count(*), count(voltage), count(voltage_a), "
            "sum(voltage), sum(voltage_a) from "
            "table(time_align(table vt_ext, TRUE, '*::_a'))")
        tdSql.checkRows(1)
        for col_index, value in enumerate(expected[0]):
            tdSql.checkData(0, col_index, value)

    def test_window_and_partition(self):
        """Windows and PARTITION BY on raw and aligned remote columns.

        INTERVAL windows and PARTITION BY run on both the raw and the aligned
        status column, including NULL partitions, and are compared against the
        reference model.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, postgres, window, ci
        """
        raw = self._raw_rows()
        aligned = self._aligned_rows(raw)

        # Interval window: raw and aligned counts/sums side by side.
        windows = {}
        for raw_row, aligned_row in zip(raw, aligned):
            key = self._window_start(raw_row[0])
            windows.setdefault(key, []).append((raw_row, aligned_row))
        expected = []
        for start in sorted(windows):
            rows = windows[start]
            raw_values = [row[0][1] for row in rows if row[0][1] is not None]
            aligned_values = [row[1][1] for row in rows]
            expected.append([
                start,
                len(rows),
                len(raw_values),
                sum(raw_values) if raw_values else None,
                len(aligned_values),
                sum(aligned_values),
            ])
        tdSql.query(
            "select _wstart, count(*), count(voltage), sum(voltage), "
            "count(voltage_a), sum(voltage_a) from "
            "table(time_align(table vt_ext, TRUE, '*::_a')) "
            "interval(2s) order by _wstart")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        # PARTITION BY the aligned status column.
        groups = {}
        for aligned_row in aligned:
            groups.setdefault(aligned_row[3], []).append(aligned_row)
        expected = []
        for status in sorted(groups):
            status_windows = {}
            for row in groups[status]:
                status_windows.setdefault(self._window_start(row[0]), []).append(row)
            for start in sorted(status_windows):
                rows = status_windows[start]
                expected.append([status, start, len(rows), sum(row[1] for row in rows)])
        tdSql.query(
            "select status_a, _wstart, count(*), sum(voltage_a) from "
            "table(time_align(table vt_ext, '*:PREV:_a')) "
            "partition by status_a interval(2s) order by status_a, _wstart")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        # PARTITION BY the raw status column (NULL is its own partition).
        groups = {}
        for raw_row, aligned_row in zip(raw, aligned):
            groups.setdefault(raw_row[3], []).append((raw_row, aligned_row))
        expected = []
        for status in sorted(groups, key=lambda value: (0, 0) if value is None else (1, value)):
            status_windows = {}
            for raw_row, aligned_row in groups[status]:
                status_windows.setdefault(self._window_start(raw_row[0]), []).append(aligned_row)
            for start in sorted(status_windows):
                rows = status_windows[start]
                expected.append([status, start, len(rows), sum(row[1] for row in rows)])
        tdSql.query(
            "select status, _wstart, count(*), sum(voltage_a) from "
            "table(time_align(table vt_ext, TRUE, '*::_a')) "
            "partition by status interval(2s) order by status, _wstart")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def test_default_named_alignment_and_leak(self):
        """Default names resolve to aligned columns; source columns are rejected.

        The default definition makes the plain column name the aligned column
        for ORDER BY and PARTITION BY, while referencing a source column that
        the output does not expose is an invalid column.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, postgres, negative, ci
        """
        raw = self._raw_rows()
        aligned = self._aligned_rows(raw)

        # Default arguments: the name 'voltage' is the aligned column and
        # PARTITION BY/ORDER BY use the same aligned values.
        groups = {}
        for row in aligned:
            groups.setdefault(row[3], []).append(row)
        expected = [
            [status, len(rows), sum(row[1] for row in rows)]
            for status, rows in sorted(groups.items())
        ]
        tdSql.query(
            "select status, count(*), sum(voltage) from "
            "table(time_align(table vt_ext)) partition by status order by status")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        tdSql.query(
            "select ts, voltage_a from table(time_align(table vt_ext, "
            "'voltage:PREV:_a')) order by voltage_a desc, ts limit 2")
        ordered = sorted([row for row in aligned if row[1] is not None],
                         key=lambda row: (-row[1], row[0]))[:2]
        tdSql.checkRows(len(ordered))
        for row_index, row in enumerate(ordered):
            tdSql.checkData(row_index, 0, row[0])
            tdSql.checkData(row_index, 1, row[1])

        # A source column that is not part of the output must not leak.
        tdSql.error(
            "select voltage from table(time_align(table vt_ext, 'voltage:PREV:_a'))",
            PAR_INVALID_COLUMN)
