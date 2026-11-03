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

import hashlib
import json
import os
import statistics
import time


class TestFunTsDiffPerf:
    DB = "diff_perf_v2"
    BASE_TS = 1790000000000
    LONG_ROWS = 12000
    SHORT_TABLES = 80
    SHORT_ROWS = 8
    BATCH_ROWS = 200
    DEFAULT_ROUNDS = 5
    BOUNDARY_ROWS = 16384
    # Fixed data shape and generation rule. Bump this when changing either.
    DATASET_VERSION = 2

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def insert_rows(self, table, rows):
        for start in range(0, len(rows), self.BATCH_ROWS):
            values = " ".join("(" + ",".join(str(v) for v in row) + ")" for row in rows[start:start + self.BATCH_ROWS])
            tdSql.execute(f"insert into {table} values {values}")

    def value(self, i, column):
        if column == 0:
            return i if i % 17 < 9 else 18 - i
        if column == 1:
            return "NULL" if i % 2 == 0 else (i * 7) % 101
        if column == 2:
            return "NULL" if i % 10 != 0 else i % 37
        return (i * (column + 3)) % 211

    def long_rows(self):
        return [
            (self.BASE_TS + i * 10, *(self.value(i, c) for c in range(8)),
             float(i if i % 17 < 9 else 18 - i) / 4, "NULL", i % 13)
            for i in range(self.LONG_ROWS)
        ]

    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.DB} vgroups 1 keep 36500d")
        tdSql.execute(f"use {self.DB}")
        columns = ", ".join(f"c{i} bigint" for i in range(8))
        columns += ", f double, empty_col bigint, aux int"
        tdSql.execute(f"create table if not exists long_run (ts timestamp, {columns})")
        tdSql.execute("create table if not exists empty_run (ts timestamp, c0 bigint)")
        tdSql.execute("create table if not exists one_run (ts timestamp, c0 bigint)")
        tdSql.execute(f"create stable if not exists short_runs (ts timestamp, {columns}) tags (device int)")
        tdSql.execute("create table if not exists keyed (ts timestamp, pk int primary key, c0 bigint, c1 bigint)")
        for child in range(self.SHORT_TABLES):
            tdSql.execute(f"create table if not exists short_{child} using short_runs tags ({child})")

        expected = {"long_run": self.LONG_ROWS, "short_runs": self.SHORT_TABLES * self.SHORT_ROWS, "keyed": 2 * self.LONG_ROWS, "empty_run": 0, "one_run": 1}
        counts = {}
        for table in expected:
            tdSql.query(f"select count(*) from {table}")
            counts[table] = tdSql.getData(0, 0)
        if counts["one_run"] == 0:
            tdSql.execute(f"insert into one_run values ({self.BASE_TS}, 7)")
            counts["one_run"] = 1
        if all(count == 0 for table, count in counts.items() if table != "one_run"):
            self.insert_rows("long_run", self.long_rows())
            for child in range(self.SHORT_TABLES):
                rows = [(self.BASE_TS + child * 1000000 + i * 10, *(self.value(i + child, c) for c in range(8)), float(i + child), "NULL", child) for i in range(self.SHORT_ROWS)]
                self.insert_rows(f"short_{child}", rows)
            for start in range(0, self.LONG_ROWS, self.BATCH_ROWS):
                rows = [(self.BASE_TS + i * 10, pk, self.value(i + pk, 0), self.value(i + pk, 1)) for i in range(start, min(start + self.BATCH_ROWS, self.LONG_ROWS)) for pk in (0, 1)]
                self.insert_rows("keyed", rows)
        elif counts != expected:
            raise AssertionError(f"incomplete or unexpected dataset: {counts}, expected {expected}; use a fresh database")
        for table, count in expected.items():
            tdSql.query(f"select count(*) from {table}")
            if tdSql.getData(0, 0) != count:
                raise AssertionError(f"unexpected row count in {table}; use a fresh database")
        # Counts alone cannot detect a same-size stale dataset. Check fixed anchor rows.
        tdSql.query(f"select c0, c1, c2, f, empty_col, aux from long_run where ts = {self.BASE_TS}")
        if tdSql.queryRows != 1 or tuple(tdSql.queryResult[0]) != (self.value(0, 0), None, self.value(0, 2), 0.0, None, 0):
            raise AssertionError(f"dataset anchor differs: {tdSql.queryResult!r}; use a fresh database")
        print(f"DIFF_DATASET version={self.DATASET_VERSION} db={self.DB} counts={json.dumps(expected, sort_keys=True)}")

    def cases(self):
        cols = lambda n: ", ".join(f"diff(c{i}, 3)" for i in range(n))
        return (
            ("single_long", "select ts, diff(c0) from long_run"),
            ("single_count", "select count(*) from (select diff(c0) as d from long_run)"),
            ("empty_input", "select ts, diff(c0) from empty_run"),
            ("one_input", "select ts, diff(c0) from one_run"),
            ("single_tiny", "select ts, diff(c0) from short_0"),
            ("single_short", "select ts, diff(c0) from short_runs partition by tbname"),
            ("option_zero", "select ts, diff(c0, 0) from long_run"),
            ("option_one", "select ts, diff(c0, 1) from long_run"),
            ("option_two", "select ts, diff(c0, 2) from long_run"),
            ("option_three", "select ts, diff(c0, 3) from long_run"),
            ("null_half", "select ts, diff(c1, 2) from long_run"),
            ("null_sparse", "select ts, diff(c2, 3) from long_run"),
            ("all_null", "select ts, diff(empty_col, 3) from long_run"),
            ("float_long", "select ts, diff(f) from long_run"),
            ("float_negative", "select ts, diff(f, 1) from long_run"),
            ("negative_ignore", "select ts, diff(c0, 1) from long_run"),
            ("multi_two", "select ts, " + cols(2) + " from long_run"),
            ("multi_count", "select count(*) from (select diff(c0, 3), diff(c1, 3) from long_run)"),
            ("multi_four", "select ts, " + cols(4) + " from long_run"),
            ("multi_eight", "select ts, " + cols(8) + " from long_run"),
            ("expression", "select ts, diff(c0 + 2) from long_run"),
            ("auxiliary", "select ts, aux, diff(c0) from long_run"),
            ("keyed_asc", "select ts, diff(c0) from keyed"),
            ("keyed_desc", "select ts, diff(c0) from keyed order by ts desc"),
            ("scan_control", "select ts, c0 from long_run"),
        )

    def check_known_results(self):
        tdSql.query("select ts, diff(c0) from long_run where ts < 1790000000050")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 1, 1)
        tdSql.query("select ts, diff(c1, 2) from long_run where ts < 1790000000060")
        tdSql.checkRows(2)
        for i in range(2):
            tdSql.checkData(i, 1, 14)
        tdSql.query("select diff(empty_col, 3) from long_run")
        tdSql.checkRows(0)
        tdSql.query("select diff(c0) from long_run where ts >= 1790000000000 and ts < 1790000000010")
        tdSql.checkRows(0)
        tdSql.query("select diff(c0, 1) from long_run where ts < 1790000000120")
        tdSql.checkRows(11)
        tdSql.checkData(9, 0, None)
        tdSql.query("select diff(c0, 3) from long_run where ts < 1790000000120")
        tdSql.checkRows(9)
        tdSql.query("select diff(c0) from one_run")
        tdSql.checkRows(0)
        print("DIFF_CORRECTNESS known_results=passed")

    def fingerprint(self, rows):
        digest = hashlib.sha256()
        for row in rows:
            digest.update(repr(tuple(row)).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    def measure(self):
        rounds = int(os.environ.get("DIFF_PERF_ROUNDS", str(self.DEFAULT_ROUNDS)))
        if not 1 <= rounds <= 50:
            raise ValueError("DIFF_PERF_ROUNDS must be between 1 and 50")
        label = os.environ.get("DIFF_PERF_LABEL", "unlabeled")
        for name, sql in self.cases():
            tdSql.query(sql)
            reference = (tdSql.queryRows, self.fingerprint(tdSql.queryResult))
            samples = []
            for _ in range(rounds):
                start = time.perf_counter()
                tdSql.query(sql)
                elapsed_ms = (time.perf_counter() - start) * 1000
                if (tdSql.queryRows, self.fingerprint(tdSql.queryResult)) != reference:
                    raise AssertionError(f"non-deterministic result: {name}")
                samples.append(round(elapsed_ms, 3))
            print("DIFF_PERF " + json.dumps({"dataset_version": self.DATASET_VERSION, "label": label, "name": name, "rows": reference[0], "sha256": reference[1], "samples_ms": samples, "median_ms": round(statistics.median(samples), 3)}, sort_keys=True))
        print("diff performance baseline ........... [passed]")

    def do_large_scale(self):
        target = int(os.environ.get("DIFF_PERF_LARGE_ROWS", "0"))
        if target == 0:
            return
        if target < 1000 or target > 10000000:
            raise ValueError("DIFF_PERF_LARGE_ROWS must be 1000..10000000")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists large_run (ts timestamp, c bigint)")
        tdSql.query("select count(*) from large_run")
        existing = tdSql.getData(0, 0)
        if existing > target:
            raise AssertionError(f"large_run already has {existing} rows; target is {target}")
        if existing:
            tdSql.query("select min(ts), max(ts) from large_run")
            if str(tdSql.getData(0, 0)) == str(tdSql.getData(0, 1)) and existing != 1:
                raise AssertionError("large_run timestamp range is invalid")
        for start in range(existing, target, 1000):
            end = min(start + 1000, target)
            values = " ".join(f"({self.BASE_TS + i * 10},{i})" for i in range(start, end))
            tdSql.execute(f"insert into large_run values {values}")
        tdSql.query("select count(*) from large_run")
        tdSql.checkData(0, 0, target)
        rounds = int(os.environ.get("DIFF_PERF_LARGE_ROUNDS", "3"))
        if not 1 <= rounds <= 10:
            raise ValueError("DIFF_PERF_LARGE_ROUNDS must be 1..10")
        cases = (
            ("diff_aggregate", "select count(d), sum(d) from (select diff(c) as d from large_run)", (target - 1, target - 1)),
            ("scan_aggregate", "select count(c), sum(c+1) from large_run", (target, target * (target + 1) // 2)),
        )
        if os.environ.get("DIFF_PERF_LARGE_FETCH") == "1":
            cases += (("diff_fetch", "select ts, diff(c) from large_run", None),)
        for name, sql, expected in cases:
            if os.environ.get("DIFF_PERF_EXPLAIN") == "1":
                tdSql.query("explain analyze verbose true " + sql, queryTimes=1)
                print("DIFF_EXPLAIN " + json.dumps({"label": os.environ.get("DIFF_PERF_LABEL", "unlabeled"), "name": name, "plan": tdSql.queryResult}, default=str, sort_keys=True))
            tdSql.query(sql, queryTimes=1)
            reference = (tdSql.queryRows, self.fingerprint(tdSql.queryResult))
            if expected is not None and (tdSql.queryRows != 1 or tuple(tdSql.queryResult[0]) != expected):
                raise AssertionError(f"unexpected result for {name}: {tdSql.queryResult!r}")
            if name == "diff_fetch" and tdSql.queryRows != target - 1:
                raise AssertionError(f"large_run returned {tdSql.queryRows}, expected {target - 1}")
            samples = []
            for _ in range(rounds):
                start = time.perf_counter()
                tdSql.query(sql, queryTimes=1)
                samples.append(round((time.perf_counter() - start) * 1000, 3))
                if (tdSql.queryRows, self.fingerprint(tdSql.queryResult)) != reference:
                    raise AssertionError(f"result changed for {name}")
            print("DIFF_LARGE " + json.dumps({"label": os.environ.get("DIFF_PERF_LABEL", "unlabeled"), "name": name, "input_rows": target, "output_rows": reference[0], "sha256": reference[1], "samples_ms": samples, "median_ms": round(statistics.median(samples), 3)}, sort_keys=True))
        print("large DIFF benchmark ........... [passed]")

    def check_numeric_types(self):
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists numeric_run (ts timestamp, c_int int, c_float float, c_double double)")
        target = int(os.environ.get("DIFF_PERF_NUMERIC_ROWS", "4096"))
        if target < 128 or target > 200000:
            raise ValueError("DIFF_PERF_NUMERIC_ROWS must be 128..200000")
        tdSql.query("select count(*) from numeric_run")
        existing = tdSql.getData(0, 0)
        if existing > target:
            raise AssertionError(f"numeric_run already has {existing} rows; target is {target}")
        for start in range(existing, target, 1000):
            rows = [(self.BASE_TS + i * 10, i if i % 97 < 50 else -i,
                     float(i % 41), float(i % 53)) for i in range(start, min(start + 1000, target))]
            self.insert_rows("numeric_run", rows)
        tdSql.query("select count(*) from numeric_run")
        tdSql.checkData(0, 0, target)
        label = os.environ.get("DIFF_PERF_LABEL", "unlabeled")
        rounds = int(os.environ.get("DIFF_PERF_NUMERIC_ROUNDS", "5"))
        if not 1 <= rounds <= 50:
            raise ValueError("DIFF_PERF_NUMERIC_ROUNDS must be between 1 and 50")
        for column in ("c_int", "c_float", "c_double"):
            for option in range(4):
                sql = f"select count(d) from (select diff({column}, {option}) as d from numeric_run)"
                if os.environ.get("DIFF_PERF_EXPLAIN") == "1":
                    tdSql.query("explain analyze verbose true " + sql, queryTimes=1)
                    print("DIFF_NUMERIC_EXPLAIN " + json.dumps({"label": label, "column": column,
                                                                "option": option, "plan": tdSql.queryResult}, default=str, sort_keys=True))
                tdSql.query(sql, queryTimes=1)
                reference = (tdSql.queryRows, self.fingerprint(tdSql.queryResult))
                samples = []
                for _ in range(rounds):
                    start = time.perf_counter()
                    tdSql.query(sql, queryTimes=1)
                    samples.append(round((time.perf_counter() - start) * 1000, 3))
                    if (tdSql.queryRows, self.fingerprint(tdSql.queryResult)) != reference:
                        raise AssertionError(f"numeric result changed: {column}, option={option}")
                print("DIFF_NUMERIC " + json.dumps({"label": label, "column": column, "option": option,
                                                    "rows": reference[0], "sha256": reference[1],
                                                    "samples_ms": samples, "median_ms": round(statistics.median(samples), 3)}, sort_keys=True))

    def check_int_boundaries(self):
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists int_boundary_run (ts timestamp, c int)")
        values = [None if i % 97 == 0 else ((1 << 31) - 1 if i % 2 else -(1 << 31))
                  for i in range(4096)]
        tdSql.query("select count(*) from int_boundary_run")
        if tdSql.getData(0, 0) == 0:
            self.insert_rows("int_boundary_run", [(self.BASE_TS + i * 10, v if v is not None else "NULL")
                                                  for i, v in enumerate(values)])
        tdSql.query("select c from int_boundary_run")
        if [row[0] for row in tdSql.queryResult] != values:
            raise AssertionError("int_boundary_run differs from deterministic input")
        for option in range(4):
            expected = []
            previous = None
            for i, value in enumerate(values):
                if i == 0:
                    previous = value
                elif value is None or previous is None:
                    if value is not None:
                        previous = value
                    if option & 2 == 0:
                        expected.append((None,))
                else:
                    delta = value - previous
                    previous = value
                    if delta < 0 and option & 1:
                        if option & 2 == 0:
                            expected.append((None,))
                    else:
                        expected.append((delta,))
            tdSql.query(f"select diff(c, {option}) from int_boundary_run", queryTimes=1)
            if tdSql.queryResult != expected:
                raise AssertionError(f"INT boundary result differs: option={option}")

    def check_integer_types(self):
        tdSql.execute(f"use {self.DB}")
        columns = ("c_tiny", "c_small", "c_int", "c_big", "c_utiny", "c_usmall", "c_uint", "c_ubig")
        types = ("tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned",
                 "int unsigned", "bigint unsigned")
        schema = ", ".join(f"{column} {type_name}" for column, type_name in zip(columns, types))
        tdSql.execute(f"create table if not exists integer_run (ts timestamp, {schema})")
        target = int(os.environ.get("DIFF_PERF_INTEGER_ROWS", "200000"))
        if target < 4096 or target > 200000:
            raise ValueError("DIFF_PERF_INTEGER_ROWS must be 4096..200000")
        tdSql.query("select count(*) from integer_run")
        existing = tdSql.getData(0, 0)
        if existing > target:
            raise AssertionError(f"integer_run already has {existing} rows; target is {target}")
        for start in range(existing, target, 1000):
            rows = []
            for i in range(start, min(start + 1000, target)):
                x = i % 256
                values = (x - 128, (i % 65536) - 32768, i if i % 97 < 50 else -i,
                          i * 100000, x, i % 65536, (i * 65537) % (1 << 32),
                          ((1 << 63) + i * 100000) % (1 << 64))
                rows.append((self.BASE_TS + i * 10, *("NULL" if i % 101 == 0 else v for v in values)))
            self.insert_rows("integer_run", rows)
        tdSql.query("select count(*) from integer_run")
        tdSql.checkData(0, 0, target)
        label = os.environ.get("DIFF_PERF_LABEL", "unlabeled")
        rounds = int(os.environ.get("DIFF_PERF_INTEGER_ROUNDS", "3"))
        if not 1 <= rounds <= 20:
            raise ValueError("DIFF_PERF_INTEGER_ROUNDS must be between 1 and 20")
        for column in columns:
            for option in range(4):
                for window_start in (0, 32768, 65536, 131072):
                    start_ts = self.BASE_TS + window_start * 10
                    end_ts = start_ts + 4096 * 10
                    correctness_sql = (f"select diff({column}, {option}) from integer_run "
                                       f"where ts >= {start_ts} and ts < {end_ts}")
                    tdSql.query(correctness_sql, queryTimes=1)
                    print("DIFF_INTEGER_RESULT " + json.dumps({"label": label, "column": column,
                                                              "option": option, "window_start": window_start,
                                                              "rows": tdSql.queryRows,
                                                              "sha256": self.fingerprint(tdSql.queryResult)},
                                                             sort_keys=True))
                if os.environ.get("DIFF_PERF_INTEGER_DIRECT", "1") == "1":
                    end_ts = self.BASE_TS + min(target, 16384) * 10
                    sql = (f"select diff({column}, {option}) from integer_run "
                           f"where ts >= {self.BASE_TS} and ts < {end_ts}")
                else:
                    sql = f"select count(d) from (select diff({column}, {option}) as d from integer_run)"
                if os.environ.get("DIFF_PERF_EXPLAIN") == "1":
                    tdSql.query("explain analyze verbose true " + sql, queryTimes=1)
                    print("DIFF_INTEGER_EXPLAIN " + json.dumps({"label": label, "column": column,
                                                               "option": option, "plan": tdSql.queryResult},
                                                              default=str, sort_keys=True))
                tdSql.query(sql, queryTimes=1)
                reference = (tdSql.queryRows, self.fingerprint(tdSql.queryResult))
                samples = []
                for _ in range(rounds):
                    start = time.perf_counter()
                    tdSql.query(sql, queryTimes=1)
                    samples.append(round((time.perf_counter() - start) * 1000, 3))
                    if (tdSql.queryRows, self.fingerprint(tdSql.queryResult)) != reference:
                        raise AssertionError(f"integer result changed: {column}, option={option}")
                print("DIFF_INTEGER " + json.dumps({"label": label, "column": column, "option": option,
                                                     "rows": reference[0], "sha256": reference[1], "sql": sql,
                                                     "samples_ms": samples,
                                                     "median_ms": round(statistics.median(samples), 3)},
                                                    sort_keys=True))

    def boundary_value(self, i):
        if i % 97 == 0 or i in (127, 4095, 4096):
            return None
        if i % 113 == 0 or i in (128, 129, 4097, 8191, 8192):
            return -i
        return i

    def check_boundary_results(self):
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists boundary_run (ts timestamp, c bigint)")
        tdSql.query("select count(*) from boundary_run")
        existing = tdSql.getData(0, 0)
        expected_input = [self.boundary_value(i) for i in range(self.BOUNDARY_ROWS)]
        if existing == 0:
            rows = [
                (self.BASE_TS + i * 10, value if value is not None else "NULL")
                for i, value in enumerate(expected_input)
            ]
            self.insert_rows("boundary_run", rows)
        elif existing != self.BOUNDARY_ROWS:
            raise AssertionError(f"unexpected boundary_run row count: {existing}")

        tdSql.query("select ts, c from boundary_run")
        actual_input = [row[1] for row in tdSql.queryResult]
        if actual_input != expected_input:
            raise AssertionError("boundary_run differs from deterministic input")

        for option in range(4):
            expected = []
            previous = None
            keep_null = option & 2 == 0
            for i, value in enumerate(expected_input):
                if i == 0:
                    previous = value
                elif value is None:
                    if keep_null:
                        expected.append((None,))
                elif previous is None:
                    previous = value
                    if keep_null:
                        expected.append((None,))
                else:
                    delta = value - previous
                    previous = value
                    if delta < 0 and option & 1:
                        if keep_null:
                            expected.append((None,))
                    else:
                        expected.append((delta,))
            tdSql.query(f"select diff(c, {option}) from boundary_run", queryTimes=1)
            if tdSql.queryResult != expected:
                mismatch = next((i for i, (got, want) in enumerate(zip(tdSql.queryResult, expected)) if got != want), None)
                raise AssertionError(f"boundary DIFF option={option}: rows={tdSql.queryRows}/{len(expected)}, first mismatch={mismatch}")
            print("DIFF_BOUNDARY " + json.dumps({"option": option, "input_rows": self.BOUNDARY_ROWS, "output_rows": len(expected), "sha256": self.fingerprint(tdSql.queryResult)}, sort_keys=True))

    def check_extreme_results(self):
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists extreme_run (ts timestamp, c bigint)")
        values = [-(1 << 63) if i % 2 == 0 else (1 << 63) - 1 for i in range(256)]
        tdSql.query("select count(*) from extreme_run")
        if tdSql.getData(0, 0) == 0:
            self.insert_rows("extreme_run", [(self.BASE_TS + i * 10, value) for i, value in enumerate(values)])
        tdSql.query("select c from extreme_run")
        if [row[0] for row in tdSql.queryResult] != values:
            raise AssertionError("extreme_run differs from deterministic input")
        expected = [(((values[i] - values[i - 1]) + (1 << 63)) % (1 << 64) - (1 << 63),)
                    for i in range(1, len(values))]
        tdSql.query("select diff(c, 0) from extreme_run", queryTimes=1)
        if tdSql.queryResult != expected:
            raise AssertionError("BIGINT DIFF overflow result differs from two's complement subtraction")

    def test_fun_ts_diff_perf(self):
        """Build reusable DIFF data and measure representative queries.

        1. Prepare deterministic long, short-group, and composite-key data.
        2. Check selected results and fingerprint every timed result.
        3. Emit repeated timings for paired baseline and candidate runs.

        Catalog:
            - Function

        Since: v3.5.0.0

        Labels: common

        Jira: None

        History:
            - 2026-10-23 Codex Add reusable DIFF performance workload.

        """
        self.prepare_data()
        self.check_known_results()
        self.measure()
        self.check_boundary_results()
        self.check_extreme_results()
        self.check_int_boundaries()
        if os.environ.get("DIFF_PERF_NUMERIC") == "1":
            self.check_numeric_types()
        if os.environ.get("DIFF_PERF_INTEGER") == "1":
            self.check_integer_types()
        self.do_large_scale()
