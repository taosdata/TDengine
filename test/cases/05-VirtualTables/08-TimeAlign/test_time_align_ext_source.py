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
from datetime import datetime

import pytest

from new_test_framework.utils import tdLog, tdSql

_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
_TEST_DIR = os.path.abspath(os.path.join(_CASE_DIR, "..", "..", ".."))
sys.path.insert(0, os.path.join(_TEST_DIR, "cases", "09-DataQuerying", "19-FederatedQuery"))

from federated_query_common import ExtSrcEnv  # noqa: E402

PAR_SYNTAX_ERROR = 0x80002600
PAR_INVALID_COLUMN = 0x80002602
PAR_DUPLICATED_COLUMN = 0x8000263C
PAR_NOT_ALLOWED_FUNC = 0x8000264F


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
            "current double precision, status integer, "
            "metric numeric(10,2), note varchar(16))",
            "INSERT INTO t_ext VALUES "
            "('2024-01-01 00:00:00', 1, 10, 1, 10.50, 'aa'),"
            "('2024-01-01 00:00:01', NULL, 20, NULL, NULL, NULL),"
            "('2024-01-01 00:00:02', 3, NULL, 3, 20.50, ''),"
            "('2024-01-01 00:00:04', 5, 50, 4, NULL, 'bb')",
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

    def test_real_external_table(self):
        """TIME_ALIGN applies to a directly-qualified external table.

        The external table is projected above FederatedScan; WHERE on the
        aligned output runs after interpolation and does not truncate the
        remote input.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, phase2, ci
        """
        qualified = f"{self.SOURCE}.public.t_ext"
        tdSql.execute(f"use {self.VTABLE_DB}")

        tdSql.query(f"select ts, voltage from {qualified} order by ts")
        raw = [list(row) for row in tdSql.queryResult]
        values = [row[1] for row in raw]

        def prev(items):
            out, last = [], None
            for item in items:
                if item is not None:
                    last = item
                out.append(last)
            return out

        def nxt(items):
            out, following = [None] * len(items), None
            for i in range(len(items) - 1, -1, -1):
                if items[i] is not None:
                    following = items[i]
                out[i] = following
            return out

        expected = [[raw[i][0], prev(values)[i], nxt(values)[i]] for i in range(len(raw))]
        tdSql.query(
            f"select ts, voltage_p, voltage_n from table(time_align(table {qualified}, "
            f"'voltage:PREV:_p;voltage:NEXT:_n')) order by ts")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        tdSql.query(
            f"select ts, voltage_n from table(time_align(table {qualified}, 'voltage:NEXT:_n')) "
            f"where voltage_n = 3 order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

        # The align barrier keeps an explicit Projection above FederatedScan.
        tdSql.query(f"explain select ts, voltage_n from "
                    f"table(time_align(table {qualified}, 'voltage:NEXT:_n'))")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Projection" in plan and "Federated Scan" in plan, plan

    def test_external_types_view_and_negatives(self):
        """Remote types, an external-table view, the 4-part path and errors.

        PG numeric maps to DECIMAL and varchar to a variable-length column;
        both interpolate in the Projection above FederatedScan.  A view over
        the external table aligns on its output, a fourth path segment is
        rejected for this source, and definition errors are reported.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, phase2, negative, ci
        """
        qualified = f"{self.SOURCE}.public.t_ext"
        tdSql.execute(f"use {self.VTABLE_DB}")

        tdSql.query(f"select * from {qualified} order by ts")
        raw = [list(row) for row in tdSql.queryResult]
        tdSql.checkRows(4)
        assert tdSql.queryCols == 6, f"expected ts + 5 remote columns, got {tdSql.queryCols}"
        voltages = [row[1] for row in raw]
        currents = [row[2] for row in raw]
        statuses = [row[3] for row in raw]
        ratios = [row[4] for row in raw]
        notes = [row[5] for row in raw]

        def prev(items):
            out, last = [], None
            for item in items:
                if item is not None:
                    last = item
                out.append(last)
            return out

        def nxt(items):
            out, following = [None] * len(items), None
            for i in range(len(items) - 1, -1, -1):
                if items[i] is not None:
                    following = items[i]
                out[i] = following
            return out

        # DECIMAL and VARCHAR aligned together with DOUBLE; raw rows stay
        # available through the same Projection.
        expected = [[raw[i][0], nxt(voltages)[i], prev(ratios)[i], nxt(notes)[i]]
                    for i in range(4)]
        tdSql.query(
            f"select ts, voltage_n, metric_p, note_n from table(time_align(table {qualified}, "
            f"'voltage:NEXT:_n;metric:PREV:_p;note:NEXT:_n')) order by ts")
        tdSql.checkRows(4)
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        # output_raw=TRUE: ts + all remote columns + one aligned column each.
        tdSql.query(
            f"select * from table(time_align(table {qualified}, TRUE, '*::_a')) order by ts")
        tdSql.checkRows(4)
        assert tdSql.queryCols == 1 + 5 * 2, (
            f"expected {1 + 5 * 2} columns, got {tdSql.queryCols}")
        aligned_columns = [prev(voltages), prev(currents), prev(statuses), prev(ratios), prev(notes)]
        for row_index in range(4):
            for col_index in range(5):
                tdSql.checkData(row_index, 1 + col_index, raw[row_index][1 + col_index])
                tdSql.checkData(row_index, 1 + 5 + col_index,
                                aligned_columns[col_index][row_index])

        # View over the external table, aligned on the view's output.
        tdSql.execute(f"drop view if exists v_ext")
        tdSql.execute(f"create view v_ext as select ts, voltage, metric, note from {qualified}")
        expected = [[raw[i][0], nxt(voltages)[i], prev(ratios)[i]] for i in range(4)]
        tdSql.query("select ts, voltage_n, metric_p from "
                    "table(time_align(table v_ext, TRUE, 'voltage:NEXT:_n;metric:PREV:_p')) "
                    "order by ts")
        tdSql.checkRows(4)
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)
        tdSql.execute("drop view if exists v_ext")

        # This source binds the database: at most three path segments are
        # accepted, so the four-segment form is a syntax error.
        tdSql.error(
            f"select * from table(time_align(table {self.SOURCE}.{self.REMOTE_DB}.public.t_ext, "
            f"'voltage:PREV:_p'))",
            PAR_SYNTAX_ERROR)

        # Definition and visibility errors on the external Projection.
        tdSql.error(f"select * from table(time_align(table {qualified}, 'voltage:LINEAR:_x'))",
                    PAR_NOT_ALLOWED_FUNC)
        tdSql.error(f"select * from table(time_align(table {qualified}, 'nocol:PREV:_x'))",
                    PAR_INVALID_COLUMN)
        tdSql.error(f"select * from table(time_align(table {qualified}, TRUE))",
                    PAR_DUPLICATED_COLUMN)
        tdSql.error(f"select current from table(time_align(table {qualified}, 'voltage:PREV:_p'))",
                    PAR_INVALID_COLUMN)
        tdSql.error(f"select ts from table(time_align(table {qualified}, 'voltage:PREV:_p')) "
                    f"where current > 1", PAR_INVALID_COLUMN)

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

        # '*' combinations on the external input: NEXT fills, repeated '*'
        # (NEXT then PREV) and the raw+aligned form share one expansion.
        def nxt(items):
            out, following = [None] * len(items), None
            for i in range(len(items) - 1, -1, -1):
                if items[i] is not None:
                    following = items[i]
                out[i] = following
            return out

        def prev(items):
            out, last = [], None
            for item in items:
                if item is not None:
                    last = item
                out.append(last)
            return out

        voltages = [row[1] for row in raw]
        currents = [row[2] for row in raw]
        statuses = [row[3] for row in raw]

        tdSql.query("select * from table(time_align(table vt_ext, '*:NEXT:_n')) order by ts")
        assert tdSql.queryCols == 4, f"unexpected column count: {tdSql.queryCols}"
        expected_cols = [nxt(voltages), nxt(currents), nxt(statuses)]
        for row_index in range(4):
            for col_index in range(3):
                tdSql.checkData(row_index, 1 + col_index,
                                expected_cols[col_index][row_index])

        tdSql.query("select * from table(time_align(table vt_ext, "
                    "'*:NEXT:_a;*:PREV:_b')) order by ts")
        assert tdSql.queryCols == 7, f"unexpected column count: {tdSql.queryCols}"
        for row_index in range(4):
            tdSql.checkData(row_index, 1, nxt(voltages)[row_index])
            tdSql.checkData(row_index, 4, prev(voltages)[row_index])

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
    def test_external_time_range(self):
        """The optional time range works on external tables and their vtables.

        The time axis is the external table's primary timestamp; the range is
        measured in the Projection above FederatedScan exactly like a local
        table.  The row at 2s has a NULL current whose successor is 2s away, so
        NEXT(2s) fills it (closed interval) while NEXT(1s) does not.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, time_range, ci
        """
        qualified = f"{self.SOURCE}.public.t_ext"
        tdSql.execute(f"use {self.VTABLE_DB}")

        tdSql.query(
            f"select ts, current_n from table(time_align(table {qualified}, 'current:NEXT(2s):_n')) "
            f"order by ts")
        tdSql.checkRows(4)
        for row_index, value in enumerate([10, 20, 50, 50]):
            tdSql.checkData(row_index, 1, value)

        tdSql.query(
            f"select ts, current_n from table(time_align(table {qualified}, 'current:NEXT(1s):_n')) "
            f"order by ts")
        tdSql.checkRows(4)
        for row_index, value in enumerate([10, 20, None, 50]):
            tdSql.checkData(row_index, 1, value)

        tdSql.query(
            "select ts, current_p from table(time_align(table vt_ext, 'current:PREV(1s):_p')) "
            "order by ts")
        tdSql.checkRows(4)
        for row_index, value in enumerate([10, 20, 20, 50]):
            tdSql.checkData(row_index, 1, value)

        tdSql.query(
            "select ts, current_p from table(time_align(table vt_ext, 'current:PREV(0s):_p')) "
            "order by ts")
        tdSql.checkRows(4)
        for row_index, value in enumerate([10, 20, None, 50]):
            tdSql.checkData(row_index, 1, value)

        # Descending output sorts above the align Projection: the values match
        # the ascending result and only the row order changes.
        tdSql.query(
            f"select ts, current_n from table(time_align(table {qualified}, 'current:NEXT(2s):_n')) "
            f"order by ts desc")
        tdSql.checkRows(4)
        for row_index, value in enumerate([50, 50, 20, 10]):
            tdSql.checkData(row_index, 1, value)
        tdSql.query(
            "select ts, current_p from table(time_align(table vt_ext, 'current:PREV(1s):_p')) "
            "order by ts desc")
        tdSql.checkRows(4)
        for row_index, value in enumerate([50, 20, 20, 10]):
            tdSql.checkData(row_index, 1, value)
        # DESC + LIMIT: limit applies to the aligned output, not to the input.
        tdSql.query(
            f"select ts, current_n from table(time_align(table {qualified}, 'current:NEXT(2s):_n')) "
            f"order by ts desc limit 2")
        tdSql.checkRows(2)
        for row_index, value in enumerate([50, 50]):
            tdSql.checkData(row_index, 1, value)

    def test_external_no_primary_key(self):
        """External tables without a primary key follow view semantics.

        A remote table without a primary key cannot back a vtable, and TIME_ALIGN
        does not synthesize a timeline for the direct external reference: the
        input is aligned in output row order, no timestamp column is forced,
        and a range parameter is ignored.  This also covers a table with no
        TIMESTAMP column at all.  The positive control uses a primary-key table
        whose key is not part of the aligned output.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, ext_source, negative, ci
        """
        ExtSrcEnv.pg_exec(self.REMOTE_DB, [
            "DROP TABLE IF EXISTS t_nopk",
            "CREATE TABLE t_nopk (ts timestamp(3), voltage double precision, note varchar(16))",
            "INSERT INTO t_nopk VALUES ('2024-01-01 00:00:00', 1, 'a'), "
            "('2024-01-01 00:00:02', NULL, NULL)",
        ])
        tdSql.execute(f"use {self.VTABLE_DB}")
        tdSql.error(
            f"create vtable vt_nopk (ts timestamp, voltage double from "
            f"{self.SOURCE}.{self.REMOTE_DB}.t_nopk.voltage)",
            0x6406)
        # No primary key: view-like output, so the default output is only the
        # aligned columns.  The timestamp column is an ordinary source column,
        # not an implicit time axis.
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_nopk, "
            f"'voltage:PREV:_a;voltage:NEXT:_n'))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 2, f"expected aligned voltage columns only, got {tdSql.queryCols}"
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, None)  # no successor after the last value
        # The default *:PREV: alignment exposes only aligned columns; `*`
        # includes the TIMESTAMP column, which is an ordinary align source.
        tdSql.query(f"select * from table(time_align(table {self.SOURCE}.public.t_nopk))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 3, f"expected 3 aligned columns, got {tdSql.queryCols}"
        tdSql.checkData(0, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:00')))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 'a')
        tdSql.checkData(1, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:02')))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 'a')
        # output_raw=TRUE exposes the input columns in schema order, followed
        # by the aligned columns.
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_nopk, TRUE, "
            f"'voltage:PREV:_a'))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 4, f"expected raw columns + aligned voltage, got {tdSql.queryCols}"
        tdSql.checkData(0, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:00')))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 'a')
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:02')))
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, 1)
        # Explicit projection of the raw TIMESTAMP column uses the same
        # view-like resolution (it is not a primary timestamp).
        tdSql.query(
            f"select ts, voltage, note, voltage_a from table(time_align("
            f"table {self.SOURCE}.public.t_nopk, TRUE, 'voltage:PREV:_a'))")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:00')))
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, self._ts_ms(datetime.fromisoformat('2024-01-01 00:00:02')))
        tdSql.checkData(1, 3, 1)

        # A table without any TIMESTAMP column is also valid: row order is the
        # only alignment order, and a range parameter is ignored.
        ExtSrcEnv.pg_exec(self.REMOTE_DB, [
            "DROP TABLE IF EXISTS t_nots",
            "CREATE TABLE t_nots (voltage double precision, note varchar(16))",
            "INSERT INTO t_nots VALUES (1, 'a'), (NULL, NULL)",
        ])
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_nots, "
            f"'voltage:PREV:_a'))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 1, f"expected aligned voltage only, got {tdSql.queryCols}"
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_nots, "
            f"'voltage:PREV(2s):_a'))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 1, f"range must be ignored, got {tdSql.queryCols} columns"
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_nots, TRUE, "
            f"'voltage:PREV:_a'))")
        tdSql.checkRows(2)
        assert tdSql.queryCols == 3, f"expected raw columns + aligned voltage, got {tdSql.queryCols}"
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, 1)
        tdSql.error(
            f"create vtable vt_nots (ts timestamp, voltage double from "
            f"{self.SOURCE}.{self.REMOTE_DB}.t_nots.voltage)", 0x6406)
        # Positive control: the primary key is not named by the definition and
        # is not part of the aligned output, yet the row timeline still comes
        # from it.
        tdSql.query(
            f"select * from table(time_align(table {self.SOURCE}.public.t_ext, "
            f"'voltage:PREV:_a'))")
        tdSql.checkRows(4)
        assert tdSql.queryCols == 2, f"expected ts + aligned voltage, got {tdSql.queryCols}"
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 1, 5)
