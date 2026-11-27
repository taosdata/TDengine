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

import math
import time
import uuid


class TestStreamVtableExpressionPipeline:
    BASE_TS = 1767225600000
    WINDOW_MS = 10000
    OUTPUT_COLS = (
        "n,sum_flow,max_flow,sum_load,max_load,"
        "pressure_points,max_pressure,max_ratio"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def query(self, sql):
        return tdSql.query(sql, row_tag=True, queryTimes=1)

    def rows_for_window(self, window):
        rows = []
        for offset in range(1, 11):
            i = window * 10 + offset
            pressure = 11.8 if window >= 2 and offset not in (1, 5) else None
            rows.append(
                (
                    self.BASE_TS + i * 1000,
                    None if offset in (1, 4) else i,
                    None if offset == 3 else 100 + i,
                    None if offset in (1, 5) else 20 + i,
                    40 + i,
                    pressure,
                )
            )
        return rows

    def expected_window(self, window):
        previous = [None] * 5
        flow, load, pressures, ratios = [], [], [], []
        for row in self.rows_for_window(window):
            for col, value in enumerate(row[1:]):
                if value is not None:
                    previous[col] = value
            a, b, c, d, pressure = previous
            if a is not None and b is not None:
                flow.append(a + b)
            if c is not None and d is not None:
                load.append((c + d) / 2)
            if pressure is not None:
                pressures.append(pressure)
                if pressure > 0.1 and a is not None:
                    ratios.append(a / pressure)
        end = self.BASE_TS + (window + 1) * self.WINDOW_MS
        return (
            end,
            10,
            sum(flow),
            max(flow),
            sum(load),
            max(load),
            len(pressures),
            max(pressures) if pressures else None,
            max(ratios) if ratios else None,
        )

    def insert_window(self, window):
        values = []
        for row in self.rows_for_window(window):
            fields = ("NULL" if value is None else str(value) for value in row)
            values.append(f"({','.join(fields)})")
        tdSql.execute(
            f"insert into {self.db}.source_tb values {''.join(values)}", queryTimes=1
        )

    def select_body(self, start="_tprev_ts", end="_tcurrent_ts"):
        return (
            f"select cast({end} as timestamp) as ts, count(*) as n, "
            "sum(a+b) as sum_flow, max(a+b) as max_flow, "
            "sum((c+d)/2) as sum_load, max((c+d)/2) as max_load, "
            "count(p) as pressure_points, max(p) as max_pressure, "
            "max(case when p>0.1 then a/p else null end) as max_ratio "
            "from (select ts, fill_forward(a) as a, fill_forward(b) as b, "
            "fill_forward(c) as c, fill_forward(d) as d, fill_forward(p) as p "
            f"from {self.db}.vt where ts>{start} and ts<={end})"
        )

    def check_rows(self, actual, expected):
        assert len(actual) == len(expected), (actual, expected)
        for row, wanted in zip(actual, expected):
            assert len(row) == len(wanted), (row, wanted)
            for value, target in zip(row, wanted):
                if target is None:
                    assert value is None, (row, wanted)
                elif isinstance(target, int):
                    assert value == target, (row, wanted)
                else:
                    assert value is not None and math.isclose(
                        value, target, rel_tol=1e-9, abs_tol=1e-9
                    ), (row, wanted)

    def wait_status(self, expected):
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            rows = self.query(
                "select status,message from information_schema.ins_streams "
                f"where db_name='{self.db}' and stream_name='s_expr'"
            )
            assert not any(row[0] == "Failed" for row in rows), rows
            if len(rows) == 1 and rows[0][0] == expected:
                assert "Failed times" not in (rows[0][1] or ""), rows
                return
            time.sleep(0.2)
        raise AssertionError(f"stream did not become {expected}: {rows}")

    def wait_output(self, count):
        expected = [self.expected_window(window) for window in range(count)]
        deadline = time.monotonic() + 60
        actual = []
        while time.monotonic() < deadline:
            self.wait_status("Running")
            actual = self.query(
                f"select cast(ts as bigint),{self.OUTPUT_COLS} "
                f"from {self.db}.result order by ts"
            )
            if len(actual) >= count:
                self.check_rows(actual, expected)
                return
            time.sleep(0.2)
        raise AssertionError(f"missing output: actual={actual}, expected={expected}")

    def check_batch_query(self, window):
        start = self.BASE_TS + window * self.WINDOW_MS
        body = self.select_body(start, start + self.WINDOW_MS)
        actual = self.query(
            f"select cast(ts as bigint),{self.OUTPUT_COLS} from ({body})"
        )
        self.check_rows(actual, [self.expected_window(window)])

    # --- impl ---
    def do_vtable_expression_pipeline(self):
        self.db = f"stream_expr_{uuid.uuid4().hex[:8]}"
        try:
            tdSql.execute(f"create database {self.db} vgroups 1 keep 36500")
            tdSql.execute(
                f"create table {self.db}.source_tb "
                "(ts timestamp, a double, b double, c float, d float, p double)"
            )
            tdSql.execute(
                f"create vtable {self.db}.vt (ts timestamp, "
                f"a double from {self.db}.source_tb.a, "
                f"b double from {self.db}.source_tb.b, "
                f"c float from {self.db}.source_tb.c, "
                f"d float from {self.db}.source_tb.d, "
                f"p double from {self.db}.source_tb.p)"
            )
            tdSql.execute(
                f"create table {self.db}.result (ts timestamp, n bigint, "
                "sum_flow double, max_flow double, sum_load double, max_load double, "
                "pressure_points bigint, max_pressure double, max_ratio double)"
            )
            for window in (0, 1):
                self.insert_window(window)
                self.check_batch_query(window)
            if not self.query("show snodes"):
                tdSql.execute("create snode on dnode 1")
            tdSql.execute(
                f"create stream {self.db}.s_expr sliding(10s) from {self.db}.vt "
                "stream_options(ignore_disorder | fill_history_first) "
                f"into {self.db}.result as {self.select_body()}",
                queryTimes=1,
            )
            self.wait_output(2)
            print("history: two windows with NULL pressure [ passed ]")

            self.insert_window(2)
            self.check_batch_query(2)
            self.wait_output(3)
            print("new input: filled values and guarded ratio [ passed ]")

            tdSql.execute(f"stop stream {self.db}.s_expr", queryTimes=1)
            self.wait_status("Stopped")
            tdSql.execute(f"start stream {self.db}.s_expr", queryTimes=1)
            self.wait_status("Running")
            self.insert_window(3)
            self.check_batch_query(3)
            self.wait_output(4)
            print("restart: four exact persisted window results [ passed ]")
        finally:
            tdSql.execute(f"drop database if exists {self.db}")

    # --- main ---
    def test_vtable_expression_pipeline(self):
        """Stream: virtual-table expressions, NULLs, history and restart.

        1. Compare fill-forward expression aggregates with explicit Python values.
        2. Verify historical and newly inserted windows reach the output table.
        3. Stop/start the stream and verify all old and new window results.
        4. Use only TSDB SQL, without anomaly_window or anode dependencies.

        Catalog:
            - Stream:VirtualTable

        Since: v3.4.3.8

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-03 Codex Add pure stream expression pipeline regression.
        """
        self.do_vtable_expression_pipeline()
