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
import math
import struct
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class TestStreamAnomalyRequest:
    BASE_TS = 1767225600000
    WINDOW_MS = 10000
    SINGLE_COLUMN_ALGOS = ("ksigma", "iqr", "grubbs", "shesd")

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def query(self, sql, query_times=1):
        return tdSql.query(sql, row_tag=True, queryTimes=query_times)

    def rows_for_window(self, window):
        rows = []
        for i in range(20):
            a, b = float(i), float(100 + i)
            if i in (0, 10):
                a = None
            if i == 3:
                b = None
            if i == 6:
                a, b = 1e308, 1e308  # Addition overflows in the scalar slot.
            if i in (7, 8, 9):
                a, b = {7: 1e70, 8: -1e70, 9: float.fromhex("0x1.fffffffffffffp+1023")}[i], 0.0
            p = None if i < 5 or i == 12 else float(10 + i)
            if i == 7:
                p = struct.unpack("f", struct.pack("f", 1e30))[0]
            material = None if a is None or b is None else a + b
            if material is not None and not math.isfinite(material):
                material = None
            rows.append((self.BASE_TS + window * self.WINDOW_MS + 100 + i * 400,
                         a, b, p, material))
        return rows

    def expected_payload(self, window):
        data = [[], [], []]
        previous = None
        for ts, a, b, p, material in self.rows_for_window(window):
            if p is not None:
                previous = p
            if material is not None and previous is not None:
                for column, value in zip(data, (ts, material, previous)):
                    column.append(value)
        return data

    def check_requests(self, start):
        with self.lock:
            assert not self.errors, self.errors
            requests = self.requests[start:]
        assert requests, "anomaly_window did not send a request"
        for raw, payload in requests:
            assert b"\x00" not in raw, raw
            data = payload["data"]
            window = (data[0][0] - self.BASE_TS) // self.WINDOW_MS
            expected = self.expected_payload(window)
            assert len(data) == len(expected), payload
            for col, (actual, wanted) in enumerate(zip(data, expected)):
                assert len(actual) == len(wanted), payload
                for value, target in zip(actual, wanted):
                    assert value is not None and math.isfinite(value), payload
                    if col < 2:
                        assert value == target, (payload, expected)
                    else:
                        assert math.isclose(value, target, rel_tol=1e-8), (payload, expected)

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
                        "type": "anomaly-detection",
                        "algo": [{"name": name, "status": "READY"}
                                 for name in (owner.algorithm, "lof") + owner.SINGLE_COLUMN_ALGOS]}]})
                else:
                    self.reply(404, {})

            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                try:
                    def reject_constant(value):
                        raise ValueError(f"non-finite JSON token: {value}")

                    assert self.path == "/anomaly-detect", self.path
                    payload = json.loads(raw, parse_constant=reject_constant)
                    data = payload["data"]
                    columns = 2 if payload["algo"].lower() in owner.SINGLE_COLUMN_ALGOS else 3
                    assert len(data) == columns and len(data[0]) >= 10, payload
                    assert len(payload["schema"]) == columns, payload
                    assert payload["rows"] == len(data[0]), payload
                    assert all(len(col) == len(data[0]) for col in data), payload
                    assert all(value is not None and math.isfinite(value)
                               for col in data for value in col), payload
                    with owner.lock:
                        owner.requests.append((raw, payload))
                    self.reply(200, {"rows": 1, "res": [[data[0][0], data[0][-1]]],
                                     "mask": [(1 << (columns - 1)) - 1]})
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(f"{exc}: {raw!r}")
                    self.reply(400, {"rows": -1, "msg": str(exc)})

        self.lock = threading.Lock()
        self.requests, self.errors = [], []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def insert_window(self, window):
        values = []
        for row in self.rows_for_window(window):
            values.append("(" + ",".join("NULL" if v is None else str(v) for v in row) + ")")
        tdSql.execute(f"insert into {self.db}.source_tb values {''.join(values)}", queryTimes=1)

    def select_body(self, key="(a+b)", source="vt", start="_tprev_ts", end="_tcurrent_ts"):
        return (
            "select _wend as ts, _wstart as ws, count(*) as n from "
            "(select ts,a,b,material,fill_forward(p) as p "
            f"from {self.db}.{source} where ts>{start} and ts<={end}) "
            f"anomaly_window({key},p,'algo={self.algorithm},wncheck=0')"
        )

    def expected_output(self, window):
        data = self.expected_payload(window)
        # Only analysis input is filtered; aggregate the original rows in the returned window.
        return (data[0][-1], data[0][0], 15)

    def wait_status(self, expected):
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            rows = self.query("select status,message from information_schema.ins_streams "
                              f"where db_name='{self.db}' and stream_name='s_anomaly'")
            assert not any(row[0] == "Failed" for row in rows), rows
            if len(rows) == 1 and rows[0][0] == expected:
                return
            time.sleep(0.2)
        raise AssertionError(f"stream did not become {expected}: {rows}")

    def wait_output(self, count):
        expected = [self.expected_output(w) for w in range(count)]
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            self.wait_status("Running")
            rows = self.query(f"select cast(ts as bigint),cast(ws as bigint),n "
                              f"from {self.db}.result order by ts")
            if len(rows) >= count:
                assert rows == expected, (rows, expected)
                return
            time.sleep(0.2)
        raise AssertionError(f"missing anomaly output: {rows}, expected {expected}")

    # --- impl ---
    def do_single_column_keys(self):
        tdSql.execute(f"create table {self.db}.single_tb "
                      "(ts timestamp,c1 double,c2 int,c3 double)")
        for scenario in ("all_null_extra", "partial_null_extra", "null_primary"):
            values, timestamps, primary = [], [], []
            for i in range(20):
                ts = self.BASE_TS + 100 + i * 400
                c1 = None if scenario == "null_primary" and i == 5 else float(i)
                c2 = i if scenario == "partial_null_extra" and i % 2 == 0 else None
                fields = (ts, c1, c2, 1e308)
                values.append("(" + ",".join("NULL" if v is None else str(v) for v in fields) + ")")
                if c1 is not None:
                    timestamps.append(ts)
                    primary.append(c1)
            tdSql.execute(f"insert into {self.db}.single_tb values {''.join(values)}", queryTimes=1)

            for algorithm in self.SINGLE_COLUMN_ALGOS:
                baseline = None
                for name in (algorithm, algorithm.upper(), algorithm.title()):
                    for keys in ("c1", "c1,c2", "c1,c2,(c3+c3)"):
                        start = len(self.requests)
                        body = ("select _wstart as ws,_wend as ts,count(*) as n "
                                f"from {self.db}.single_tb "
                                f"anomaly_window({keys},'algo={name},wncheck=0')")
                        actual = self.query(f"select cast(ws as bigint),cast(ts as bigint),n from ({body})",
                                            query_times=30)
                        if baseline is None:
                            baseline = actual
                        assert actual == baseline == [(timestamps[0], timestamps[-1], 20)], (scenario, name, keys, actual)
                        with self.lock:
                            assert not self.errors, self.errors
                            requests = self.requests[start:]
                        assert requests, (scenario, name, keys)
                        for raw, payload in requests:
                            assert payload["data"] == [timestamps, primary], (scenario, name, keys, raw)

        # Multivariate models must still reject rows missing any consumed key.
        start = len(self.requests)
        tdSql.error(f"select count(*) from {self.db}.single_tb "
                    "anomaly_window(c1,c2,'algo=lof,wncheck=0')",
                    expectErrInfo="not enough", fullMatched=False)
        assert len(self.requests) == start, "incomplete multivariate input reached the anode"

        # Ignoring extra keys must not bypass the minimum for the primary key.
        values = "".join(f"({self.BASE_TS + 100 + i * 400},NULL,NULL,1e308)" for i in range(11))
        tdSql.execute(f"insert into {self.db}.single_tb values {values}", queryTimes=1)
        for keys in ("c1", "c1,c2,(c3+c3)"):
            tdSql.error(f"select count(*) from {self.db}.single_tb "
                        f"anomaly_window({keys},'algo=ksigma,wncheck=0')",
                        expectErrInfo="not enough", fullMatched=False)
        assert len(self.requests) == start, "insufficient primary input reached the anode"
        print("single-column models ignore NULL and non-finite extra keys [ passed ]")

    def do_anomaly_request(self):
        self.db = f"anomaly_req_{uuid.uuid4().hex[:8]}"
        self.algorithm = self.db
        self.start_anode()
        anode_id = None
        try:
            tdSql.execute(f"create anode '{self.url}'")
            rows = self.query("show anodes")
            anode_id = next(row[0] for row in rows if row[1] == self.url)
            tdSql.execute(f"create database {self.db} vgroups 1 keep 36500")
            tdSql.execute(f"create table {self.db}.source_tb "
                          "(ts timestamp,a double,b double,p float,material double)")
            tdSql.execute(f"create vtable {self.db}.vt (ts timestamp, "
                          f"a double from {self.db}.source_tb.a, "
                          f"b double from {self.db}.source_tb.b, "
                          f"p float from {self.db}.source_tb.p, "
                          f"material double from {self.db}.source_tb.material)")
            for window in (0, 1):
                self.insert_window(window)
            self.do_single_column_keys()
            # Algorithm URLs reach executors asynchronously through dnode heartbeats.
            for key, source in (("(a+b)", "vt"), ("material", "vt"), ("(a+b)", "source_tb")):
                start = len(self.requests)
                body = self.select_body(key, source, self.BASE_TS, self.BASE_TS + self.WINDOW_MS)
                rows = self.query(f"select cast(ts as bigint),cast(ws as bigint),n from ({body})",
                                  query_times=30)
                assert rows == [self.expected_output(0)], rows
                self.check_requests(start)

            start = len(self.requests)
            body = self.select_body(start=self.BASE_TS, end=self.BASE_TS + 4500)
            tdSql.error(body, expectErrInfo="not enough", fullMatched=False)
            assert len(self.requests) == start, "insufficient valid rows reached the anode"

            if not self.query("show snodes"):
                tdSql.execute("create snode on dnode 1")
            tdSql.execute(f"create table {self.db}.result (ts timestamp,ws timestamp,n bigint)")
            start = len(self.requests)
            tdSql.execute(f"create stream {self.db}.s_anomaly sliding(10s) from {self.db}.vt "
                          f"stream_options(ignore_disorder | fill_history_first({self.BASE_TS})) "
                          f"into {self.db}.result as {self.select_body()}", queryTimes=1)
            self.wait_output(2)
            self.check_requests(start)
            tdSql.execute(f"stop stream {self.db}.s_anomaly", queryTimes=1)
            self.wait_status("Stopped")
            tdSql.execute(f"start stream {self.db}.s_anomaly", queryTimes=1)
            self.wait_status("Running")
            start = len(self.requests)
            self.insert_window(2)
            # Advance event time beyond the third window to trigger its calculation.
            tdSql.execute(f"insert into {self.db}.source_tb values "
                          f"({self.BASE_TS + 3 * self.WINDOW_MS + 1},NULL,NULL,NULL,NULL)",
                          queryTimes=1)
            self.wait_output(3)
            self.check_requests(start)
            print("anomaly expression, aligned finite input, history and restart [ passed ]")
        finally:
            try:
                tdSql.execute(f"drop database if exists {self.db}")
            finally:
                try:
                    if anode_id is not None:
                        tdSql.execute(f"drop anode {anode_id}")
                finally:
                    self.server.shutdown()
                    self.server.server_close()
                    self.thread.join(timeout=5)

    # --- main ---
    def test_anomaly_request(self):
        """Stream: validate actual anomaly requests with a strict local anode.

        1. Compare expression keys with materialized keys on physical and virtual tables.
        2. Assert aligned timestamps and values after NULL/non-finite row filtering.
        3. Check large FLOAT/DOUBLE values and reject requests below ten valid rows.
        4. Verify anomaly results persist through historical stream execution and restart.
        5. Verify ignored NULL/non-finite keys do not change single-column model input.
        6. Compare lowercase, uppercase and mixed-case names for all four single-column models.

        Catalog:
            - Stream:AnomalyWindow

        Since: v3.4.3.8

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Peng Rongkun Add anomaly request regression with a local anode.
            - 2026-11-05 Peng Rongkun Cover ignored keys for single-column models.
            - 2026-11-05 Peng Rongkun Cover case-insensitive single-column algorithm names.

        """
        self.do_anomaly_request()
