import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from new_test_framework.utils import tdLog, tdSql


class TestWindowQueryPartition:
    updatecfgDict = {"anodeAllowInternalUrl": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="window_query_partition", drop=True)
        tdSql.execute("create table t (ts timestamp, dev int, grp int, v int)")
        tdSql.execute(
            "insert into t values (1700000000000, 1, 0, 100)"
            "(1700000000001, 2, 0, 10)"
        )

    def check(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def test_single_row_partitions(self):
        """Query partitions isolate ranking and aggregation.

        1. Reproduce the reported two-device queries.
        2. Check unchanged global and explicit OVER partition behavior.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        self.check(
            "select dev, rank() over(order by v desc), sum(v) over() "
            "from t partition by dev order by dev",
            [(1, 1, 100), (2, 1, 10)],
        )
        self.check(
            "select dev, rank() over(order by v desc), sum(v) over() "
            "from t order by dev",
            [(1, 1, 110), (2, 2, 110)],
        )
        self.check(
            "select dev, rank() over(partition by dev order by v desc), "
            "sum(v) over(partition by dev) from t order by dev",
            [(1, 1, 100), (2, 1, 10)],
        )
        # The outer key must remain available even when it is not projected.
        self.check(
            "select sum(v) over() from t partition by dev order by v desc",
            [(100,), (10,)],
        )
        # Aggregation must expose the same query key to the later window stage.
        self.check(
            "select count(*), rank() over(order by dev) "
            "from t partition by dev order by dev",
            [(1, 1), (1, 1)],
        )
        self.check(
            "select count(*), sum(1) over() from t partition by dev + 1",
            [(1, 1), (1, 1)],
        )

    def test_nested_partitions_and_frames(self):
        """Window keys refine query partitions without merging devices.

        1. Exercise ties, independent window specifications and neighboring rows.
        2. Cover named windows, frames, null and computed query partition keys.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table multi (ts timestamp, dev int, grp int, v int)")
        tdSql.execute(
            "insert into multi values "
            "(1700000000000, 1, 0, 30)(1700000000001, 2, 0, 7)"
            "(1700000000002, 1, 0, 30)(1700000000003, 2, 0, 7)"
            "(1700000000004, 1, 1, 10)(1700000000005, 2, 1, 2)"
        )
        self.check(
            "select first(v), last(v), sum(1) over() from multi "
            "partition by dev + 1 order by first(v) desc",
            [(30, 10, 1), (7, 2, 1)],
        )
        self.check(
            "select count(*), sum(1) over() from multi partition by dev + 1 "
            "group by grp order by count(*)",
            [(1, 2), (1, 2), (2, 2), (2, 2)],
        )
        self.check(
            "select dev, v, rank() over(order by v desc), "
            "dense_rank() over(order by v desc), sum(v) over(), "
            "sum(v) over(partition by grp) from multi partition by dev "
            "order by dev, ts",
            [(1, 30, 1, 1, 70, 60), (1, 30, 1, 1, 70, 60),
             (1, 10, 3, 2, 70, 10), (2, 7, 1, 1, 16, 14),
             (2, 7, 1, 1, 16, 14), (2, 2, 3, 2, 16, 2)],
        )
        self.check(
            "select dev, lag(v) over w, lead(v) over w, "
            "sum(v) over(order by ts rows between 1 preceding and current row) "
            "from multi partition by dev "
            "window w as (partition by grp order by ts) order by dev, ts",
            [(1, None, 30, 30), (1, 30, None, 60), (1, None, None, 40),
             (2, None, 7, 7), (2, 7, None, 14), (2, None, None, 9)],
        )
        self.check(
            "select dev, sum(v) over(partition by grp) "
            "from multi partition by dev + 1 order by dev, ts",
            [(1, 60), (1, 60), (1, 10), (2, 14), (2, 14), (2, 2)],
        )
        self.check(
            "select dev, sum(v) over() from multi partition by dev, grp "
            "order by dev, ts",
            [(1, 60), (1, 60), (1, 10), (2, 14), (2, 14), (2, 2)],
        )
        tdSql.execute(
            "insert into multi values (1700000000006, null, 0, 5)"
            "(1700000000007, null, 0, 6)"
        )
        self.check(
            "select dev, sum(v) over() from multi partition by dev "
            "order by dev nulls first, ts",
            [(None, 11), (None, 11), (1, 70), (1, 70), (1, 70),
             (2, 16), (2, 16), (2, 16)],
        )

    @pytest.mark.parametrize("window", ["interval(1s)", "session(ts, 1s)"])
    def test_time_window_partitions(self, window):
        """Time window aggregation preserves hidden query partition keys.

        1. Combine INTERVAL and SESSION with OVER using an unprojected key.
        2. Repeat with a computed query partition expression.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        for key in ("dev", "dev + 1"):
            self.check(
                "select count(*), rank() over(order by 1) "
                f"from t partition by {key} {window}",
                [(1, 1), (1, 1)],
            )

    def test_external_window_partitions(self):
        """External window aggregates preserve hidden query partition keys.

        1. Check hidden ordinary and computed keys across two external windows.
        2. Compare global OVER, fill projections and all-null aggregate row counts.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table ext_data (ts timestamp, dev int, v int)")
        tdSql.execute("create table ext_bounds (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into ext_data values "
            "(1700600000000, 1, 10)(1700600000001, 2, 20)"
            "(1700600001000, 1, 30)(1700600001001, 1, 40)"
            "(1700600001002, 2, 50)"
        )
        tdSql.execute(
            "insert into ext_bounds values "
            "(1700600000000, 1700600000999)"
            "(1700600001000, 1700600001999)"
        )
        window = "external_window((select ts, end_ts from ext_bounds) w)"
        for key in ("dev", "dev + 1"):
            self.check(
                "select count(*), rank() over(order by 1) "
                f"from ext_data partition by {key} {window} order by count(*)",
                [(1, 1), (1, 1), (1, 1), (2, 1)],
            )
            self.check(
                "select count(*), rank() over(order by count(*)) "
                f"from ext_data partition by {key} {window} order by count(*)",
                [(1, 1), (1, 1), (1, 1), (2, 2)],
            )
        self.check(
            f"select count(*), rank() over(order by count(*)) from ext_data {window} "
            "order by count(*)",
            [(2, 1), (3, 2)],
        )
        for key in ("dev", "dev + 1"):
            self.check(
                f"select sum(v)+1 from ext_data partition by {key} {window} "
                "fill(value, 0) order by rank() over(order by 1), sum(v)+1",
                [(11,), (21,), (51,), (71,)],
            )
        tdSql.execute(
            "create stable ext_multi (ts timestamp, dev int, v int) tags (site int)"
        )
        for table in range(8):
            tdSql.execute(f"create table ext_child{table} using ext_multi tags({table})")
            tdSql.execute(
                f"insert into ext_child{table} values "
                "(1700600000000, 1, 10)(1700600001000, 1, 20)"
                "(1700600001001, 1, 30)"
            )
        for key in ("tbname, dev", "tbname, dev + 1"):
            self.check(
                "select count(*), rank() over(order by count(*)) "
                f"from ext_multi partition by {key} {window} order by count(*)",
                [(1, 1)] * 8 + [(2, 2)] * 8,
            )
            self.check(
                f"select sum(v)+1 from ext_multi partition by {key} {window} "
                "fill(value, 0) order by rank() over(order by 1), sum(v)+1",
                [(11,)] * 8 + [(51,)] * 8,
            )
        tdSql.execute("insert into ext_bounds values (1700600002000, 1700600002999)")
        for key in ("dev", "dev + 1"):
            for fill, empty in (("null", None), ("value, 0, 0", 0)):
                self.check(
                    "select count(*), rank() over(order by count(*)) "
                    f"from ext_data partition by {key} {window} fill({fill}) "
                    "order by count(*) nulls first",
                    [(empty, 1), (empty, 1), (1, 2), (1, 2), (1, 2), (2, 3)],
                )
        tdSql.execute("create table ext_nulls (ts timestamp, dev int, v int)")
        tdSql.execute(
            "insert into ext_nulls values "
            "(1700600000000, 1, NULL)(1700600000001, 2, NULL)"
        )
        for key in ("dev", "dev + 1"):
            for aggregate in ("first(v)", "last(v)"):
                self.check(
                    f"select {aggregate}, rank() over(order by 1) "
                    f"from ext_nulls partition by {key} {window}",
                    [],
                )

    def test_scalar_external_window_partitions(self):
        """Scalar external windows retain hidden keys for ORDER BY-only OVER.

        1. Preserve every input row with hidden ordinary, computed and NULL keys.
        2. Check partition-local ranking order against a global OVER control.
        3. Keep the same results when the query key is explicitly projected.
        4. Preserve composite table/tag keys across eight child tables.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table scalar_ext (ts timestamp, dev int)")
        tdSql.execute("create table scalar_bounds (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into scalar_ext values "
            "(1700700000000, 1)(1700700000001, 1)(1700700000002, 2)"
            "(1700700001000, 2)(1700700001001, NULL)(1700700001002, NULL)"
        )
        tdSql.execute(
            "insert into scalar_bounds values "
            "(1700700000000, 1700700000999)(1700700001000, 1700700001999)"
        )
        window = "external_window((select ts, end_ts from scalar_bounds) w)"
        tdSql.query("select ts from scalar_ext order by ts")
        expected = list(tdSql.queryResult)
        for order in ("1", "ts"):
            self.check(
                f"select ts from scalar_ext {window} "
                f"order by rank() over(order by {order}), ts",
                expected,
            )
        ranked = [expected[i] for i in (0, 2, 4, 1, 3, 5)]
        for key in ("dev", "dev + 1"):
            # ORDER BY 1 inside this OVER resolves to the first projection, ts.
            self.check(
                f"select ts from scalar_ext partition by {key} {window} "
                "order by rank() over(order by 1), ts",
                ranked,
            )
            self.check(
                f"select ts from scalar_ext partition by {key} {window} "
                "order by rank() over(order by ts), ts",
                ranked,
            )
        tdSql.query("select ts, dev from scalar_ext order by ts")
        projected = list(tdSql.queryResult)
        self.check(
            f"select ts, dev from scalar_ext partition by dev {window} "
            "order by rank() over(order by ts), ts",
            [projected[i] for i in (0, 2, 4, 1, 3, 5)],
        )
        tdSql.execute("create stable scalar_multi (ts timestamp, dev int) tags (site int)")
        for table in range(8):
            tdSql.execute(f"create table scalar_child{table} using scalar_multi tags({table})")
            ts = 1700700000000 + table * 10
            tdSql.execute(f"insert into scalar_child{table} values ({ts}, 1)({ts + 1}, 1)")
        tdSql.query("select ts from scalar_multi order by ts")
        multi = list(tdSql.queryResult)
        for key in ("tbname, dev", "site + 1, dev + 1"):
            self.check(
                f"select ts from scalar_multi partition by {key} {window} "
                "order by rank() over(order by ts), ts",
                multi[::2] + multi[1::2],
            )

    def test_time_window_null_aggregates(self):
        """Hidden partition keys do not produce aggregate result rows.

        1. Verify all-null FIRST/LAST rows match ordinary and time-window aggregation.
        2. Verify COUNT and LAST_ROW still produce their existing null rows.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table null_values (ts timestamp, dev int, v int)")
        tdSql.execute(
            "insert into null_values values (1700300000000, 1, NULL)"
            "(1700300000001, 2, NULL)"
        )
        for window in ("", "interval(1s)", "session(ts, 1s)"):
            for key in ("dev", "dev + 1"):
                for aggregates in ("last(v)", "first(v), sum(v)"):
                    null_row = (None, 1) if aggregates == "last(v)" else (None, None, 1)
                    self.check(
                        f"select {aggregates}, rank() over(order by 1) "
                        f"from null_values partition by {key} {window}",
                        [] if window else [null_row, null_row],
                    )
                self.check(
                    "select last(v), count(*), rank() over(order by 1) "
                    f"from null_values partition by {key} {window}",
                    [(None, 1, 1), (None, 1, 1)],
                )
                self.check(
                    f"select {key}, last(v), rank() over(order by 1) "
                    f"from null_values partition by {key} {window} order by {key}",
                    [(1 if key == "dev" else 2, None, 1),
                     (2 if key == "dev" else 3, None, 1)],
                )
                self.check(
                    "select first(v), last_row(v), rank() over(order by 1) "
                    f"from null_values partition by {key} {window}",
                    [(None, None, 1), (None, None, 1)],
                )

    def test_time_window_fill_partitions(self):
        """Filled time windows preserve query partition identity.

        1. Insert two intervals per device with a missing interval between them.
        2. Check ranking with hidden and computed keys after FILL.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table filled (ts timestamp, dev int, v int)")
        tdSql.execute(
            "insert into filled values "
            "(1700200000000, 1, 10)(1700200002000, 1, 20)"
            "(1700200000001, 2, 30)(1700200002001, 2, 40)"
        )
        for key in ("dev", "dev + 1"):
            self.check(
                "select count(*), row_number() over(order by 1) as rn from filled "
                "where ts >= 1700200000000 and ts <= 1700200002001 "
                f"partition by {key} interval(1s) fill(prev) order by rn",
                [(1, 1), (1, 1), (1, 2), (1, 2), (1, 3), (1, 3)],
            )

    def test_interp_partitions(self):
        """Interpolation preserves hidden query partition keys for OVER.

        1. Interpolate two devices with different first timestamps.
        2. Check hidden, computed and composite NULL keys keep ranking within each device.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table interpolated (ts timestamp, g int, dev int, v int)")
        tdSql.execute(
            "insert into interpolated values "
            "(1700400000000, NULL, 1, 10)(1700400000001, NULL, 2, 20)"
        )
        for key in ("dev", "dev + 1", "g, dev", "g, dev + 1"):
            self.check(
                "select interp(v) as iv, row_number() over(order by 1) as rn "
                f"from interpolated partition by {key} "
                "range(1700400000000, 1700400002000) every(1s) fill(prev) "
                "order by iv, rn",
                [(10, 1), (10, 2), (10, 3), (20, 1), (20, 2)],
            )

    def test_scalar_window_order_by_over(self):
        """Scalar time-window output retains keys consumed only by ORDER BY.

        1. Read scalar timestamps from partitions of different sizes through time windows.
        2. Check ORDER BY-only OVER ranks the larger hidden partition first.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table scalar_window (ts timestamp, dev int)")
        tdSql.execute(
            "insert into scalar_window values "
            "(1700500000000, 1)(1700500000002, 1)"
            "(1700500000001, 2)(1700500000003, 2)(1700500000004, 2)"
        )
        tdSql.query("select ts from scalar_window order by ts")
        expected = list(tdSql.queryResult)
        tdSql.query("select ts from scalar_window where dev=2 order by ts")
        larger_partition = list(tdSql.queryResult)
        tdSql.query("select ts from scalar_window where dev=1 order by ts")
        smaller_partition = list(tdSql.queryResult)
        for window in ("interval(1s)", "session(ts, 1s)"):
            for key in ("dev", "dev + 1"):
                tdSql.query(
                    f"select ts from scalar_window partition by {key} {window} "
                    "order by rank() over(order by 1)"
                )
                assert sorted(tdSql.queryResult) == expected
                tdSql.query(
                    f"select ts from scalar_window partition by {key} {window} "
                    "order by sum(1) over() desc"
                )
                assert sorted(tdSql.queryResult[:3]) == larger_partition
                assert sorted(tdSql.queryResult[3:]) == smaller_partition

    def test_scalar_window_fill_partitions(self):
        """Scalar FILL rows retain hidden partition identity for OVER.

        1. Fill missing intervals in two partitions with different value totals.
        2. Sort by the partition total and check filled rows stay with their real rows.
        3. Reject raw columns mixed with an aggregate introduced by ORDER BY.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create table scalar_fill (ts timestamp, dev int, v int)")
        tdSql.execute(
            "insert into scalar_fill values "
            "(1700600000000, 1, 10)(1700600003000, 1, 20)"
            "(1700600000001, 2, 30)(1700600002001, 2, 40)(1700600003001, 2, 50)"
        )
        for fill in ("null", "value,0,0"):
            for key in ("dev", "dev + 1"):
                tdSql.query(
                    "select _wstart, ts, v from scalar_fill "
                    "where ts >= 1700600000000 and ts <= 1700600003001 "
                    f"partition by {key} interval(1s) fill({fill}) "
                    "order by sum(v) over() desc"
                )
                tdSql.checkRows(8)
                # All four rows of dev=2 (sum 120), including its fill row,
                # precede all four rows of dev=1 (sum 30).
                assert sorted(row[2] for row in tdSql.queryResult[:4] if row[2]) == [30, 40, 50]
                assert sorted(row[2] for row in tdSql.queryResult[4:] if row[2]) == [10, 20]

        tdSql.error(
            "select ts from scalar_fill partition by dev interval(1s) "
            "order by rank() over(order by sum(v) + v)",
            expectErrInfo="Invalid usage of expr: v",
        )

    def test_analysis_partitions(self):
        """Analysis results retain hidden ordinary, computed and NULL partition keys.

        1. Use a local deterministic analysis endpoint with real query planning and execution.
        2. Check FORECAST and IMPUTATION reset OVER ranking for each input partition.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        requests = []

        class AnalysisHandler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self, data):
                body = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                self.reply({"protocol": 1.0, "version": 1.0, "details": [
                    {"type": kind, "algo": [{"name": algo, "status": "ready"}]}
                    for kind, algo in (("forecast", "partition_forecast"), ("imputation", "partition_imputation"))
                ]})

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(request)
                value = request["data"][1][0]
                self.reply({"rows": 2, "res": [[1700700100000, 1700700101000], [value, value]],
                            "ts": [1700700100000, 1700700101000], "target": [value, value], "mask": [0, 0]})

        tdSql.execute("create table analyzed (ts timestamp, g int, dev int, label varchar(16), v int, pv int)")
        for dev in (1, 2):
            rows = "".join(f"({1700700000000 + row * 1000 + dev}, NULL, {dev}, 'dev{dev}', {dev * 10}, {dev * 100})"
                           for row in range(40))
            tdSql.execute(f"insert into analyzed values {rows}")
        server = ThreadingHTTPServer(("127.0.0.1", 0), AnalysisHandler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        url = f"http://127.0.0.1:{server.server_port}"
        anode = None
        try:
            tdSql.execute(f"create anode '{url}'")
            tdSql.query("show anodes")
            anode = next(row[0] for row in tdSql.queryResult if url in row)
            for function, algo in (("forecast", "partition_forecast"), ("imputation", "partition_imputation")):
                expr = f"{function}(v, 'algo={algo},rows=2,timeout=2')"
                # Algorithm registration reaches query nodes through their heartbeat.
                self.check(f"select {expr} from analyzed where dev=1", [(10,), (10,)])
                for key in ("dev", "dev + 1", "g, dev", "label"):
                    self.check(
                        f"select {expr} as value, row_number() over(order by 1) as rn "
                        f"from analyzed partition by {key} order by value, rn",
                        [(10, 1), (10, 2), (20, 1), (20, 2)],
                    )
                if function == "forecast":
                    self.check(
                        f"select dev, {expr} as value from analyzed partition by dev order by dev, value",
                        [(1, 10), (1, 10), (2, 20), (2, 20)],
                    )
            self.check(
                "select forecast(v, dev, pv, 'algo=partition_forecast,rows=2,timeout=2,"
                "dynamic_real_1=[3 4],dynamic_real_1_col=dev') as value, "
                "row_number() over(order by 1) as rn from analyzed partition by dev order by value, rn",
                [(10, 1), (10, 2), (20, 1), (20, 2)],
            )
            for request in requests:
                assert len(request["data"][0]) == 40
                assert len(set(request["data"][1])) == 1
                if request["algo"] == "partition_forecast":
                    assert request["every"] == 1000
                    assert request["start"] == max(request["data"][0]) + 1000
                if len(request["data"]) == 4:
                    assert [column[0] for column in request["schema"]] == [
                        "ts", "val", "dynamic_real_1", "past_dynamic_real_1"
                    ]
                    assert len(request["data"][2]) == 42
                    assert request["data"][2][:40] == [request["data"][1][0] // 10] * 40
                    assert request["data"][2][-2:] == [3, 4]
                    assert request["data"][3] == [request["data"][1][0] * 10] * 40
            assert sum(len(request["data"]) == 4 for request in requests) == 2
        finally:
            if anode is not None:
                tdSql.execute(f"drop anode {anode}")
            server.shutdown()
            server.server_close()
            worker.join()

    def test_distributed_block_boundaries(self):
        """Query partitions stay isolated across vgroups and result blocks.

        1. Place each device in two different vgroups and insert 4100 rows per device.
        2. Check ranks, cumulative sums, inner partitions and neighboring-row boundaries.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7164874125

        History:
            - 2026-11-13 Created
        """
        tdSql.execute("create database window_partition_dist vgroups 2")
        tdSql.execute("use window_partition_dist")
        tdSql.execute("create stable s (ts timestamp, grp int, v int) tags (dev int)")
        for i in range(32):
            tdSql.execute(f"create table c{i} using s tags ({i % 2})")
        tdSql.query(
            "select table_name, vgroup_id from information_schema.ins_tables "
            "where db_name='window_partition_dist' and type='CHILD_TABLE'"
        )
        tables = {}
        for name, vgroup in tdSql.queryResult:
            tables.setdefault((int(name[1:]) % 2, vgroup), name)
        assert len(tables) == 4, "Both devices must have tables in both vgroups"
        for index, ((dev, _), name) in enumerate(sorted(tables.items())):
            for start in range(0, 2050, 500):
                values = "".join(
                    f"({1700100000000 + index * 2050 + row}, {row % 2}, {dev + 1})"
                    for row in range(start, min(start + 500, 2050))
                )
                tdSql.execute(f"insert into {name} values {values}")
        tdSql.execute("flush database window_partition_dist")
        self.check(
            "select dev, count(*), first(v), last(v), sum(1) over() "
            "from s partition by dev order by dev",
            [(0, 4100, 1, 1, 1), (1, 4100, 2, 2, 1)],
        )
        tdSql.query(
            "select dev, row_number() over(order by ts), "
            "csum(v) over(order by ts), lag(v) over(order by ts), "
            "lead(v) over(order by ts), sum(v) over(partition by grp) "
            "from s partition by dev order by dev, ts"
        )
        tdSql.checkRows(8200)
        for row in range(8200):
            dev, position = divmod(row, 4100)
            expected = (
                dev, position + 1, (position + 1) * (dev + 1),
                None if position == 0 else dev + 1,
                None if position == 4099 else dev + 1,
                2050 * (dev + 1),
            )
            for col, value in enumerate(expected):
                tdSql.checkData(row, col, value)
        # Exercise partial/merge aggregation with no output from FIRST/LAST.
        for index, name in enumerate(tables.values()):
            tdSql.execute(f"insert into {name} values ({1700300000000 + index}, 0, NULL)")
        for window in ("", "interval(1s)"):
            for key in ("dev", "dev + 1"):
                self.check(
                    "select last(v), sum(v), rank() over(order by 1) from s "
                    f"where ts >= 1700300000000 partition by {key} {window}",
                    [],
                )
                self.check(
                    "select last(v), count(*), rank() over(order by 1) from s "
                    f"where ts >= 1700300000000 partition by {key} {window}",
                    [(None, 2, 1), (None, 2, 1)],
                )
        tdSql.execute("use window_query_partition")
        tdSql.execute("drop database window_partition_dist")
