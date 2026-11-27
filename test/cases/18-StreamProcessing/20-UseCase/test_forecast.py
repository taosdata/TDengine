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

import json
import threading
import time
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class TestForecast:
    BASE_TS = 1767225600000  # Fixed epoch milliseconds, aligned to one minute.
    WINDOW_MS = 60000
    FORECAST_ROWS = 3
    FORECAST_EVERY_MS = 20000
    updatecfgDict = {"anodeAllowInternalUrl": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def input_columns(self, window):
        step = 1000 if self.input_row_count == 12 else 1
        indexes = [i for i in range(self.input_row_count) if (window, i) not in self.excluded_rows]
        return [
            [self.BASE_TS + window * self.WINDOW_MS + (i + 1) * step
             for i in indexes],
            [self.row_overrides.get((window, i), window * 100000 + i + self.value_offsets.get(window, 0))
             for i in indexes],
        ]

    def expected_input(self, window):
        columns = self.input_columns(window)
        if self.with_covariates:
            columns += [[value * 3 for value in columns[1]] + [7, 8, 9],
                        [value * 2 for value in columns[1]]]
        return tuple(tuple(column) for column in columns)

    def remember_input(self, window):
        snapshot = self.expected_input(window)
        with self.lock:
            self.input_versions.setdefault(window, set()).add(snapshot)

    def insert_window(self, db, window, start=0, end=None):
        timestamps, values = self.input_columns(window)
        end = len(timestamps) if end is None else end
        rows = []
        for i in range(start, end):
            fields = f"{timestamps[i]},{values[i]}"
            if self.with_covariates:
                fields += f",{values[i] * 2},{values[i] * 3}"
            rows.append(f"({fields})")
        # Publish each window atomically, including the 10001-row reuse regression.
        self.remember_input(window)
        tdSql.execute(f"insert into {db}.source_tb values " + " ".join(rows), queryTimes=1)

    def forecast_rows(self, window, columns=None):
        rows = []
        timestamps, values = self.input_columns(window) if columns is None else columns
        mean = sum(values) / len(values)
        for i in range(1, self.FORECAST_ROWS + 1):
            # Every training row affects the output, including middle-row mutations.
            prediction = mean + i
            rows.append((timestamps[-1] + i * self.FORECAST_EVERY_MS,
                         prediction, prediction - 0.5, prediction + 0.5))
        return rows

    def expected_rows(self, windows=(0, 1)):
        indexes = [0, 1]
        if self.bounds in ("both", "low"):
            indexes.append(2)
        if self.bounds in ("both", "high"):
            indexes.append(3)
        return [tuple(row[index] for index in indexes)
                for window in windows for row in self.forecast_rows(window)]

    def start_anode(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, payload):
                body = json.dumps(payload, allow_nan=False).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path == "/status":
                    self.reply(200, {"protocol": 1.0, "status": "ready"})
                elif self.path == "/list":
                    self.reply(200, {"protocol": 1.0, "version": 1.0, "details": [{
                        "type": "forecast",
                        "algo": [{"name": owner.algorithm, "status": "READY"}],
                    }]})
                else:
                    self.reply(404, {})

            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                try:
                    assert self.path == "/forecast", self.path
                    payload = json.loads(raw)
                    names = ["ts", "val"]
                    if owner.with_covariates:
                        names += ["dynamic_real_1", "past_dynamic_real_1"]
                    expected_schema = [[name, "TIMESTAMP" if name == "ts" else "DOUBLE", 8]
                                       for name in names]
                    assert payload["schema"] == expected_schema, payload["schema"]
                    assert len(payload["data"]) == len(names), payload["data"]
                    timestamps, values = payload["data"][:2]
                    window = (timestamps[0] - owner.BASE_TS) // owner.WINDOW_MS
                    snapshot = tuple(tuple(column) for column in payload["data"])
                    with owner.lock:
                        # A request already in flight may use a previously published
                        # version. It must still contain one complete, unmixed input.
                        assert snapshot in owner.input_versions.get(window, set()), window
                    assert payload["rows"] == len(timestamps), payload["rows"]
                    assert payload["algo"] == owner.algorithm, payload
                    assert payload["prec"] == "ms", payload
                    assert payload["forecast_rows"] == owner.FORECAST_ROWS, payload
                    assert payload["every"] == owner.FORECAST_EVERY_MS, payload
                    assert payload["start"] == timestamps[-1] + owner.FORECAST_EVERY_MS, payload
                    assert payload["return_conf"] == int(owner.bounds != "none"), payload
                    # Respond from the received input, independently of source mutations.
                    rows = owner.forecast_rows(window, (timestamps, values))
                    result = [list(column) for column in zip(*rows)]
                    if owner.bounds == "none":
                        result = result[:2]
                    with owner.lock:
                        owner.requests.append((window, snapshot))
                    self.reply(200, {"rows": len(rows), "res": result})
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(f"{exc}: {raw!r}")
                    self.reply(400, {"rows": -1, "msg": str(exc)})

        self.lock = threading.Lock()
        self.requests, self.errors = [], []
        self.input_versions = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def check_rows(self, expected):
        tdSql.checkRows(len(expected))
        if expected:
            tdSql.checkCols(len(expected[0]))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def forecast_select(self, source, start, end):
        names = ("v", "past", "future") if self.with_covariates else ("v",)
        columns = ",".join(f"%%tbname.{name}" if source == "%%tbname" else name for name in names)
        options = ",dynamic_real_1=[7 8 9],dynamic_real_1_col=future" if self.with_covariates else ""
        extra = ""
        if self.bounds in ("both", "low"):
            extra += ",_flow as low"
        if self.bounds in ("both", "high"):
            extra += ",_fhigh as high"
        return (
            "select _frowts as ts,"
            f"forecast({columns},'algo={self.algorithm},rows={self.FORECAST_ROWS},"
            f"every={self.FORECAST_EVERY_MS},wncheck=0{options}') as prediction{extra} "
            f"from {source} where ts >= {start} and ts <= {end}"
        )

    def output_query(self, source, condition=""):
        columns = "cast(ts as bigint),prediction"
        if self.bounds in ("both", "low"):
            columns += ",low"
        if self.bounds in ("both", "high"):
            columns += ",high"
        return f"select {columns} from {source} {condition} order by ts"

    def read_output(self, db):
        rows = tdSql.query(self.output_query(f"{db}.result"), row_tag=True, queryTimes=1)
        return [tuple(row) for row in rows]

    def query_forecast(self, db, window, query_times=1):
        start = self.BASE_TS + window * self.WINDOW_MS
        end = start + self.WINDOW_MS - 1
        body = self.forecast_select(self.query_source, start, end)
        rows = tdSql.query(self.output_query(f"({body})"), row_tag=True, queryTimes=query_times)
        self.check_rows(self.expected_rows((window,)))
        return [tuple(row) for row in rows]

    def compare_stream_with_query(self, db, phase, windows=(0, 1), outside_rows=()):
        reference = list(outside_rows)
        for window in windows:
            tdLog.info(f"{phase}: compare window {window} with a fresh ordinary FORECAST")
            expected = self.query_forecast(db, window)
            # Use prediction timestamps, which need not lie inside the input window.
            condition = f"where ts >= {expected[0][0]} and ts <= {expected[-1][0]}"
            tdSql.query(self.output_query(f"{db}.result", condition), queryTimes=1)
            self.check_rows(expected)
            reference.extend(expected)

        # Also reject extra output outside the timestamp ranges of all forecasts.
        self.read_output(db)
        self.check_rows(sorted(reference))
        print(f"{phase}: stream output equals ordinary FORECAST for {len(windows)} windows [ passed ]")

    def wait_output(self, db, phase, windows=(0, 1), expected=None):
        deadline = time.monotonic() + 60
        expected = self.expected_rows(windows) if expected is None else expected
        rows = []
        while time.monotonic() < deadline:
            with self.lock:
                assert not self.errors, self.errors
            status = tdSql.query(
                "select status,message from information_schema.ins_streams "
                f"where db_name='{db}' and stream_name='{self.stream_name}'",
                row_tag=True, queryTimes=1)
            assert not any(row[0] == "Failed" for row in status), (phase, status)
            rows = self.read_output(db)
            # Recalculation replaces values at existing keys, so row count alone
            # cannot distinguish old output from the newly computed result.
            # An empty realtime-only baseline must still wait for deployment;
            # writes before startup can belong to the history that is skipped.
            if status and all(row[0] == "Running" for row in status) and rows == expected:
                self.check_rows(expected)
                return
            time.sleep(0.2)
        raise AssertionError(f"{phase}: forecast output {rows}, expected {expected}; stream: {status}")

    def check_requests(self, start, windows=(0, 1), allowed_windows=None):
        required = {window: self.expected_input(window) for window in windows}
        with self.lock:
            assert not self.errors, self.errors
            requests = self.requests[start:]
            seen = {window for window, _ in requests}
            allowed = set(windows if allowed_windows is None else allowed_windows)
            assert set(windows) <= seen <= allowed, seen
            for window, snapshot in required.items():
                assert (window, snapshot) in requests, f"missing current input for window {window}"

    def recalc_rows(self):
        return tdSql.query(
            "select recalc_id,status,progress,message from information_schema.ins_stream_recalculates "
            f"where stream_name='{self.stream_name}'", row_tag=True, queryTimes=1)

    def wait_recalculation(self, previous):
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            rows = [row for row in self.recalc_rows() if row[0] not in previous]
            assert len(rows) <= 1, rows
            if rows:
                assert rows[0][1] != "Failed", rows
                if rows[0][1] == "Finished":
                    assert rows[0][2:] == ("100%", None), rows
                    return
            with self.lock:
                assert not self.errors, self.errors
            time.sleep(0.2)
        raise AssertionError(f"manual recalculation did not finish: {rows}")

    def recalculate(self, db, phase, windows, outside_rows=(), time_range=None):
        previous = {row[0] for row in self.recalc_rows()}
        lower = self.BASE_TS + min(windows) * self.WINDOW_MS
        upper = self.BASE_TS + (max(windows) + 1) * self.WINDOW_MS - 1
        lower, upper = (lower, upper) if time_range is None else time_range
        with self.lock:
            start = len(self.requests)
        tdSql.execute(f"recalculate stream {db}.{self.stream_name} from {lower} to {upper}", queryTimes=1)
        self.wait_recalculation(previous)
        expected = sorted(list(outside_rows) + self.expected_rows(windows))
        self.wait_output(db, phase, expected=expected)
        # Ordinary FORECAST requests must not satisfy this stream-only assertion.
        self.check_requests(start, windows)
        self.compare_stream_with_query(db, phase, windows, outside_rows)

    def ensure_snode(self):
        if not tdSql.query("show snodes", row_tag=True):
            tdSql.execute("create snode on dnode 1")

    def create_forecast_stream(self, db, history_mode, auto_recalc=False):
        options = ["delete_recalc" if auto_recalc else "ignore_disorder"]
        if history_mode:
            options.append(f"{history_mode}({self.BASE_TS})")
        body = self.forecast_select("%%tbname", "_twstart", "_twend")
        tdSql.execute(
            f"create stream {db}.{self.stream_name} interval(1m) sliding(1m) from {self.query_source} "
            f"partition by tbname stream_options({' | '.join(options)}) "
            f"into {db}.result as {body}", queryTimes=1)

    @contextmanager
    def forecast_environment(self, input_rows, with_covariates, virtual_source, bounds, excluded_rows=()):
        self.input_row_count = input_rows
        self.with_covariates = with_covariates
        self.bounds = bounds
        self.value_offsets = {}
        self.row_overrides = {}
        self.excluded_rows = set(excluded_rows)
        db = f"forecast_{uuid.uuid4().hex[:8]}"
        self.stream_name = f"s_{db}"
        self.algorithm = db
        self.start_anode()
        anode_id = None
        try:
            tdSql.execute(f"create anode '{self.url}'", queryTimes=1)
            anodes = tdSql.query("show anodes", row_tag=True)
            anode_id = next(row[0] for row in anodes if row[1] == self.url)
            tdSql.execute(f"create database {db} vgroups 1 precision 'ms' keep 36500")
            extra = ",past double,future double" if with_covariates else ""
            tdSql.execute(f"create table {db}.source_tb (ts timestamp,v double{extra})")
            self.query_source = f"{db}.source_tb"
            if virtual_source:
                names = ("v", "past", "future") if with_covariates else ("v",)
                columns = ",".join(f"{name} double from {db}.source_tb.{name}" for name in names)
                tdSql.execute(f"create vtable {db}.idmp_vt (ts timestamp,{columns})")
                self.query_source = f"{db}.idmp_vt"
            for window in (0, 1):
                self.insert_window(db, window)
            # Algorithm metadata reaches query executors asynchronously through heartbeats.
            self.query_forecast(db, 0, query_times=30)
            self.ensure_snode()
            yield db
        finally:
            try:
                tdSql.execute(f"drop database if exists {db}")
            finally:
                try:
                    if anode_id is not None:
                        tdSql.execute(f"drop anode {anode_id}")
                finally:
                    self.server.shutdown()
                    self.server.server_close()
                    self.thread.join(timeout=5)

    def check_automatic_result(self, db, phase, start, window, outside, allowed_windows):
        expected = sorted(list(outside) + self.expected_rows((window,)))
        self.wait_output(db, phase, expected=expected)
        # Automatic scheduling may also repair another completed window.
        # Require the changed window and reject calculations of unfinished windows.
        self.check_requests(start, (window,), allowed_windows)
        self.compare_stream_with_query(db, phase, (window,), outside)

    def outside_window(self, db, window):
        timestamps = {row[0] for row in self.expected_rows((window,))}
        return [row for row in self.read_output(db) if row[0] not in timestamps]

    def prime_realtime_reader(self, db):
        # Running only describes the Trigger. Advance complete probe windows
        # until this stream proves its Reader has consumed realtime WAL data.
        deadline = time.monotonic() + 60
        with self.lock:
            start = len(self.requests)
        probe = 2
        while time.monotonic() < deadline:
            self.insert_window(db, probe, start=0 if probe == 2 else 1)
            self.insert_window(db, probe + 1, end=1)
            poll_end = min(deadline, time.monotonic() + 2)
            while time.monotonic() < poll_end:
                with self.lock:
                    assert not self.errors, self.errors
                rows = self.read_output(db)
                complete = [window for window in range(2, probe + 1)
                            if all(row in rows for row in self.expected_rows((window,)))]
                if complete:
                    windows = tuple(range(min(complete), probe + 1))
                    self.wait_output(db, "realtime Reader readiness probe", windows=windows)
                    self.check_requests(start, windows, range(2, probe + 1))
                    self.compare_stream_with_query(db, "realtime Reader readiness probe", windows)
                    return windows
                time.sleep(0.2)
            probe += 1
        raise AssertionError(f"Reader did not consume realtime probe windows through {probe}")

    # --- impl ---
    def do_time_range_recalculation(self, db):
        before = self.read_output(db)
        target = self.expected_rows((1,))
        target_timestamps = {row[0] for row in target}
        outside = [row for row in before if row[0] not in target_timestamps]

        # Change all source windows. A single-window request must leave other
        # output stale, making accidental whole-history recalculation observable.
        for window in (0, 1, 2):
            self.value_offsets[window] = (window + 1) * 1000
            self.insert_window(db, window)
        self.read_output(db)
        self.check_rows(before)

        tdSql.execute(f"delete from {db}.result where ts >= {target[0][0]} and ts <= {target[-1][0]}")
        self.read_output(db)
        self.check_rows(outside)
        self.recalculate(db, "selected historical window", (1,), outside)

        # Repeating an interior range must recompute the complete affected
        # trigger window and preserve row count and the untouched output.
        lower = self.BASE_TS + self.WINDOW_MS + 5000
        upper = self.BASE_TS + self.WINDOW_MS + 10000
        tdSql.execute(f"delete from {db}.result where ts >= {target[0][0]} and ts <= {target[-1][0]}")
        self.read_output(db)
        self.check_rows(outside)
        self.recalculate(db, "repeated interior range", (1,), outside, (lower, upper))

        first_window = [row for row in before if row[0] < target[0][0]]
        self.recalculate(db, "overlapping historical and realtime range", (1, 2), first_window)

        with self.lock:
            start = len(self.requests)
        self.insert_window(db, 3, start=1)
        self.insert_window(db, 4, end=1)
        expected = sorted(first_window + self.expected_rows((1, 2, 3)))
        self.wait_output(db, "realtime after recalculation", expected=expected)
        self.check_requests(start, (3,))
        self.compare_stream_with_query(db, "realtime after recalculation", (1, 2, 3), first_window)
        print("bounded, repeated and overlapping recalculation preserves untouched windows [ passed ]")

    def do_interval_forecast_recalc(self, input_rows, with_covariates, virtual_source, bounds, history_mode):
        with self.forecast_environment(input_rows, with_covariates, virtual_source, bounds) as db:
            with self.lock:
                start = len(self.requests)
            self.create_forecast_stream(db, history_mode)
            self.wait_output(db, "initial history")
            # Check stream-only requests before ordinary queries can add matching requests.
            self.check_requests(start)
            self.compare_stream_with_query(db, "initial history")

            with self.lock:
                start = len(self.requests)
            self.insert_window(db, 2)
            self.insert_window(db, 3, end=1)
            self.wait_output(db, "realtime", windows=(0, 1, 2))
            self.check_requests(start, windows=(2,))
            self.compare_stream_with_query(db, "realtime", windows=(0, 1, 2))

            tdSql.execute(f"delete from {db}.result")
            tdSql.query(f"select count(*) from {db}.result")
            tdSql.checkData(0, 0, 0)
            self.recalculate(db, "full history restoration", (0, 1, 2))
            self.do_time_range_recalculation(db)
        print(f"manual recalculation with {history_mode} and {bounds} confidence output [ passed ]")

    def do_executor_reuse(self):
        with self.forecast_environment(10001, False, False, "both") as db:
            for window in (2, 3):
                self.insert_window(db, window)
            with self.lock:
                start = len(self.requests)
            self.create_forecast_stream(db, "fill_history_first")
            self.wait_output(db, "four large historical windows", windows=(0, 1, 2, 3))
            self.check_requests(start, (0, 1, 2, 3))
            # The mock validates all 40004 input rows. Only the first and last
            # windows need ordinary-query baselines for this counter regression.
            self.compare_stream_with_query(db, "four-window executor reuse", (3,),
                                           self.expected_rows((0, 1, 2)))
        print("four 10001-row windows reset the input counter independently [ passed ]")

    def do_automatic_forecast_recalc(self, with_covariates, virtual_source, bounds, history_mode):
        excluded = ((1, 5),) if history_mode else ()
        with self.forecast_environment(12, with_covariates, virtual_source, bounds,
                                       excluded_rows=excluded) as db:
            with self.lock:
                start = len(self.requests)
            self.create_forecast_stream(db, history_mode, auto_recalc=True)
            history_windows = (0, 1) if history_mode else ()
            self.wait_output(db, "automatic recalc initial history", windows=history_windows)
            self.check_requests(start, history_windows)
            self.compare_stream_with_query(db, "automatic recalc initial history", history_windows)

            live_window = 2
            baseline_windows = history_windows
            if not history_mode:
                baseline_windows = self.prime_realtime_reader(db)
                live_window = baseline_windows[-1] + 1
                self.excluded_rows.add((live_window, 5))
            late_window = 1 if history_mode else live_window
            with self.lock:
                start = len(self.requests)
            self.insert_window(db, live_window, start=0 if history_mode else 1)
            self.insert_window(db, live_window + 1, end=1)
            windows = baseline_windows + (live_window,)
            self.wait_output(db, "automatic recalc realtime baseline", windows=windows)
            self.check_requests(start, (live_window,), windows)
            self.compare_stream_with_query(db, "automatic recalc realtime baseline", windows)

            # Add a timestamp that was absent when this window first completed.
            outside = self.outside_window(db, late_window)
            with self.lock:
                start = len(self.requests)
            self.excluded_rows.remove((late_window, 5))
            self.insert_window(db, late_window, start=5, end=6)
            self.check_automatic_result(db, "late-insert automatic recalc", start, late_window, outside, windows)

            # Change a middle row, keeping prediction timestamps and row count stable.
            outside = self.outside_window(db, live_window)
            with self.lock:
                start = len(self.requests)
            self.row_overrides[(live_window, 3)] = live_window * 100000 + 50003
            self.insert_window(db, live_window, start=3, end=4)
            self.check_automatic_result(db, "update automatic recalc", start, live_window, outside, windows)

            # Delete an interior sample; enough rows remain for another forecast.
            outside = self.outside_window(db, late_window)
            timestamp = self.input_columns(late_window)[0][1]
            with self.lock:
                start = len(self.requests)
            self.excluded_rows.add((late_window, 1))
            self.remember_input(late_window)
            tdSql.execute(f"delete from {db}.source_tb where ts = {timestamp}", queryTimes=1)
            self.check_automatic_result(db, "delete automatic recalc", start, late_window, outside, windows)

            with self.lock:
                start = len(self.requests)
            self.insert_window(db, live_window + 1, start=1)
            self.insert_window(db, live_window + 2, end=1)
            self.wait_output(db, "realtime after automatic recalc", windows=windows + (live_window + 1,))
            self.check_requests(start, (live_window + 1,), windows + (live_window + 1,))
            self.compare_stream_with_query(db, "realtime after automatic recalc", windows + (live_window + 1,))
        print(f"automatic insert/update/delete recalculation with {history_mode or 'no history'} [ passed ]")

    # --- main ---
    def test_executor_reuse(self):
        """FORECAST stream: isolate input counters across four large windows.

        1. Calculate four consecutive historical windows with 10001 samples each.
        2. Validate each complete input and all forecast rows through the mock service.
        3. Compare the first and last windows with ordinary FORECAST without repeating range scenarios.

        Catalog: Streams:Recalculation

        Since: v3.4.3.12

        Labels: common,ci,stream,recalc,forecast

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7170501762

        History:
            - 2026-11-19 PengRongkun Isolate the cumulative row-count regression from range combinations.
        """
        self.do_executor_reuse()

    @pytest.mark.parametrize(
        "input_rows,with_covariates,virtual_source,bounds,history_mode",
        [(12, False, False, "both", "fill_history_first"),
         (12, True, False, "both", "fill_history_first"),
         (12, False, True, "both", "fill_history_first"),
         (12, True, True, "both", "fill_history_first"),
         (12, False, False, "low", "fill_history_first"),
         (12, False, False, "high", "fill_history_first"),
         (12, False, False, "none", "fill_history_first"),
         (12, False, False, "both", "fill_history"),
         (12, True, False, "low", "fill_history"),
         (12, False, True, "high", "fill_history"),
         (12, True, True, "none", "fill_history")],
        ids=["single-column", "future-covariate", "idmp-virtual-table",
             "idmp-virtual-covariate", "forecast-low", "forecast-high", "forecast-rowts",
             "fill-history-both", "fill-history-covariate-low", "fill-history-virtual-high",
             "fill-history-virtual-covariate-rowts"])
    def test_manual_recalculation(self, input_rows, with_covariates, virtual_source, bounds, history_mode):
        """FORECAST stream: reproduce history and manual recalculation with a local anode.

        1. Verify ordinary FORECAST with _frowts, _flow and _fhigh as a baseline.
        2. Calculate two historical windows and one newly written realtime window.
        3. Alternate ordinary FORECAST and stream-result queries for each input window.
        4. Compare timestamps, predictions and confidence bounds row by row.
        5. Delete all output, manually recalculate and repeat the ordinary-query comparisons.
        6. Verify stream-only anode requests before ordinary queries and reject extra output.
        7. Use small windows for the repeated range, overlap and ordinary-query comparisons.
        8. Exercise past and future covariate resources across stream executor resets.
        9. Resolve FORECAST inputs qualified with the %%tbname stream placeholder.
        10. Verify changed inputs and deleted output are refreshed only in the requested range.
        11. Repeat an interior range and overlap history/realtime ranges without duplicate output.
        12. Verify the recalculation reaches Finished with 100% progress and realtime resumes.
        13. Exercise IDMP-style virtual tables and each optional confidence-bound projection.
        14. Map FORECAST_ROWTS/LOW/HIGH to the SQL pseudo columns _frowts/_flow/_fhigh.
        15. Preserve forecasts whose timestamps extend past their source trigger windows.
        16. Exercise both FILL_HISTORY and FILL_HISTORY_FIRST before manual recalculation.

        Catalog: Streams:Recalculation

        Since: v3.4.3.12

        Labels: common,ci,stream,recalc,forecast

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7170140134

        History:
            - 2026-11-17 Codex Created minimal FORECAST reproduction.
            - 2026-11-17 Codex Added per-window ordinary-query comparisons in both phases.
            - 2026-11-17 Codex Added realtime, cumulative row-limit and covariate regressions.
            - 2026-11-17 Codex Added bounded recalculation, IDMP virtual tables and pseudo-column variants.
            - 2026-11-19 Codex Moved to forecast use cases and added FILL_HISTORY combinations.
            - 2026-11-19 Codex Made mock responses and request checks use immutable input versions.
            - 2026-11-19 PengRongkun Remove stale interior-range output and split the large-window regression.
        """
        self.do_interval_forecast_recalc(input_rows, with_covariates, virtual_source, bounds, history_mode)

    @pytest.mark.parametrize(
        "with_covariates,virtual_source,bounds,history_mode",
        [(False, False, "both", "fill_history"),
         (False, True, "low", "fill_history_first"),
         (True, False, "high", "fill_history"),
         (True, True, "none", None)],
        ids=["auto-both-fill-history", "auto-virtual-low-history-first",
             "auto-covariate-high-fill-history", "auto-virtual-covariate-rowts-no-history"])
    def test_automatic_recalculation(self, with_covariates, virtual_source, bounds, history_mode):
        """FORECAST stream: automatically recalculate late, updated and deleted source data.

        1. Exercise FILL_HISTORY, FILL_HISTORY_FIRST and realtime-only streams.
        2. Add a missing historical or realtime sample and verify automatic recalculation.
        3. Update an interior realtime sample without changing prediction timestamps.
        4. Delete an interior source sample with DELETE_RECALC and sufficient remaining rows.
        5. Check stream-only model requests and compare every changed result with ordinary FORECAST.
        6. Preserve output outside the affected window and continue realtime after automatic recalculation.
        7. Cover physical and virtual tables, past/future covariates and optional _flow/_fhigh outputs.
        8. Verify _frowts and all confidence projections without issuing RECALCULATE STREAM.

        Catalog: Streams:Recalculation

        Since: v3.4.3.12

        Labels: common,ci,stream,recalc,forecast

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7170140134

        History:
            - 2026-11-19 Codex Added automatic recalculation and history-mode coverage.
            - 2026-11-19 Codex Verify the current input version despite older in-flight requests.
            - 2026-11-19 PengRongkun Probe Reader startup and validate schema-to-data mappings.
        """
        self.do_automatic_forecast_recalc(with_covariates, virtual_source, bounds, history_mode)
