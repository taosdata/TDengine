from new_test_framework.utils import tdLog, tdSql

import csv
import json
import os
import statistics
import time

LEFT_VALUE_BASE = 10
RIGHT_VALUE_BASE = 20


class TestJoinHashPerformance:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_hash_performance_compare(self):
        """Hash Join performance comparison smoke and report generator.

        Run the configured comparison workloads over multiple rounds and write
        Hash Join performance measurements to report files.

        Since: v3.4.3.0

        Labels: common,integration,performance

        Jira: TD-29941

        History:
            - 2026-07-15 Added Hash Join performance comparison runner.
        """

        rows = int(os.environ.get("HJ_PERF_ROWS", "1000"))
        duplicate_mod = int(os.environ.get("HJ_PERF_DUP_MOD", "0"))
        rounds = max(1, int(os.environ.get("HJ_PERF_ROUNDS", "3")))
        report_dir = os.environ.get("HJ_REPORT_DIR", "/tmp/tdengine_hash_join_reports")
        os.makedirs(report_dir, exist_ok=True)

        tdSql.prepare(dbname="hj_perf", precision="ms")
        self.prepare_tables(rows, duplicate_mod)
        self.prepare_tracker_data()

        cases = self.build_cases(rows, duplicate_mod)
        results = []
        for case in cases:
            result = self.run_compare_case(case, rounds, rows)
            results.append(result)

        jsonl_path = os.path.join(report_dir, "join_hash_performance.jsonl")
        csv_path = os.path.join(report_dir, "join_hash_performance.csv")
        write_jsonl(jsonl_path, results)
        write_csv(csv_path, results)
        tdLog.info(f"hash join performance report jsonl: {jsonl_path}")
        tdLog.info(f"hash join performance report csv: {csv_path}")

    def prepare_tables(self, rows, duplicate_mod):
        tdSql.execute(
            "CREATE TABLE l_perf("
            "ts TIMESTAMP, k INT, k2 BINARY(32), v DOUBLE, payload BINARY(128))"
        )
        tdSql.execute(
            "CREATE TABLE r_perf("
            "ts TIMESTAMP, k INT, k2 BINARY(32), v DOUBLE, payload BINARY(128))"
        )
        self.insert_rows("l_perf", rows, duplicate_mod, 1704067200000, LEFT_VALUE_BASE)
        self.insert_rows("r_perf", rows, duplicate_mod, 1704067200000, RIGHT_VALUE_BASE)

    def insert_rows(self, table, rows, duplicate_mod, base_ts, base_value, batch_size=500):
        for start in range(0, rows, batch_size):
            values = []
            end = min(start + batch_size, rows)
            for i in range(start, end):
                key = i if duplicate_mod <= 0 else i % duplicate_mod
                key2 = f"k2_{i:08d}"
                payload = f"payload_{table}_{i:08d}"
                values.append(f"({base_ts + i}, {key}, '{key2}', {base_value + i}, '{payload}')")
            tdSql.execute(f"INSERT INTO {table} VALUES" + "".join(values))

    def prepare_tracker_data(self):
        tdSql.execute(
            "CREATE TABLE trackers("
            "ts TIMESTAMP, site BINARY(16), tracker BINARY(16), reg_pitch DOUBLE)"
        )
        rows = []
        for hour, base in ((1, 100), (2, 200), (3, 300), (4, 400)):
            for i in range(1, 9):
                rows.append(
                    f"('2023-01-01 {hour:02d}:10:{i:02d}.000', 'MN-01', 'MN01-T{i:02d}', {base + i})"
                )
            for i in range(1, 6):
                rows.append(
                    f"('2023-01-01 {hour:02d}:20:{i:02d}.000', 'MN-02', 'MN02-T{i:02d}', {base + 100 + i})"
                )
        for start in range(0, len(rows), 100):
            tdSql.execute("INSERT INTO trackers VALUES" + "".join(rows[start:start + 100]))

    def build_cases(self, rows, duplicate_mod):
        duplicate_expected = expected_inner_count(rows, duplicate_mod)
        tracker_body = (
            "FROM ("
            "  SELECT _wend AS window_end, site, tracker, LAST(reg_pitch) AS tracker_last_pitch "
            "  FROM trackers "
            "  WHERE ts >= '2023-01-01 01:00:00.000' "
            "    AND ts < '2023-01-01 03:00:00.000' "
            "  PARTITION BY site, tracker "
            "  INTERVAL(1h) SLIDING(1h)"
            ") a "
            "JOIN ("
            "  SELECT _wend AS window_end, site, AVG(reg_pitch) AS site_avg_pitch "
            "  FROM trackers "
            "  WHERE ts >= '2023-01-01 01:00:00.000' "
            "    AND ts < '2023-01-01 03:00:00.000' "
            "  PARTITION BY site "
            "  INTERVAL(1h) SLIDING(1h)"
            ") b "
            "ON a.window_end = b.window_end AND a.site = b.site"
        )
        tracker_sql = "SELECT COUNT(*) " + tracker_body
        tracker_hash_sql = "SELECT /*+ hash_join() */ COUNT(*) " + tracker_body
        return [
            {
                "case": "HJ-PERF-001",
                "scenario": "timestamp equality one-to-one",
                "hash_sql": (
                    "SELECT /*+ hash_join() */ COUNT(*) "
                    "FROM l_perf l JOIN r_perf r ON l.ts = r.ts"
                ),
                "normal_sql": "SELECT COUNT(*) FROM l_perf l JOIN r_perf r ON l.ts = r.ts",
                "expected": rows,
                "key_cardinality": rows,
            },
            {
                "case": "HJ-PERF-002",
                "scenario": "business key equality",
                "hash_sql": (
                    "SELECT /*+ hash_join() */ COUNT(*) "
                    "FROM l_perf l JOIN r_perf r ON l.k = r.k"
                ),
                "normal_sql": "SELECT COUNT(*) FROM l_perf l JOIN r_perf r ON l.k = r.k",
                "expected": duplicate_expected,
                "key_cardinality": rows if duplicate_mod <= 0 else duplicate_mod,
            },
            {
                "case": "HJ-PERF-003",
                "scenario": "composite key equality",
                "hash_sql": (
                    "SELECT /*+ hash_join() */ COUNT(*) "
                    "FROM l_perf l JOIN r_perf r ON l.k = r.k AND l.k2 = r.k2"
                ),
                "normal_sql": (
                    "SELECT COUNT(*) "
                    "FROM l_perf l JOIN r_perf r ON l.k = r.k AND l.k2 = r.k2"
                ),
                "expected": rows,
                "key_cardinality": rows,
            },
            {
                "case": "HJ-PERF-004",
                "scenario": "window aggregate subquery",
                "hash_sql": tracker_hash_sql,
                "normal_sql": tracker_sql,
                # Two hours across four sites produce 26 tracker-window matches.
                "expected": 26,
                "key_cardinality": 4,
            },
            {
                "case": "HJ-PERF-006",
                "scenario": "equality plus ON residual",
                "hash_sql": (
                    "SELECT /*+ hash_join() */ COUNT(*) "
                    "FROM l_perf l JOIN r_perf r ON l.k = r.k AND l.v < r.v"
                ),
                "normal_sql": "SELECT COUNT(*) FROM l_perf l JOIN r_perf r ON l.k = r.k AND l.v < r.v",
                "expected": expected_residual_count(rows, duplicate_mod),
                "key_cardinality": rows if duplicate_mod <= 0 else duplicate_mod,
            },
            {
                "case": "HJ-PERF-007",
                "scenario": "left semi existence",
                "hash_sql": (
                    "SELECT /*+ hash_join() */ COUNT(*) "
                    "FROM l_perf l LEFT SEMI JOIN r_perf r ON l.k = r.k"
                ),
                "normal_sql": "SELECT COUNT(*) FROM l_perf l LEFT SEMI JOIN r_perf r ON l.k = r.k",
                "expected": rows,
                "key_cardinality": rows if duplicate_mod <= 0 else duplicate_mod,
            },
        ]

    def run_compare_case(self, case, rounds, rows):
        hash_plan = explain_text(case["hash_sql"])
        if "Hash" not in hash_plan:
            tdLog.exit(f"{case['case']} did not use Hash Join plan: {hash_plan}")

        try:
            hash_measure = measure_sql(case["hash_sql"], rounds)
        except Exception as err:
            tdLog.exit(f"{case['case']} Hash Join failed: {err}")
        if hash_measure["status"] != "success":
            tdLog.exit(f"{case['case']} Hash Join failed: {hash_measure}")
        if int(hash_measure["output_rows"]) != case["expected"]:
            tdLog.exit(
                f"{case['case']} expected {case['expected']}, got {hash_measure['output_rows']}"
            )

        normal_plan = ""
        normal_measure = {"status": "not_run"}
        try:
            normal_plan = explain_text(case["normal_sql"])
            normal_measure = measure_sql(case["normal_sql"], rounds)
        except Exception as err:
            tdLog.exit(f"{case['case']} normal join path failed: {err}")

        consistency = self.is_consistent(hash_measure, normal_measure)
        if consistency is None:
            tdLog.exit(
                f"{case['case']} normal join path did not produce a result: {normal_measure}"
            )
        if not consistency:
            tdLog.exit(
                f"{case['case']} hash and normal join results differ: "
                f"hash={hash_measure.get('output_rows')}, normal={normal_measure.get('output_rows')}"
            )

        result = {
            "case": case["case"],
            "scenario": case["scenario"],
            "left_rows": rows,
            "right_rows": rows,
            "key_cardinality": case["key_cardinality"],
            "output_rows": hash_measure["output_rows"],
            "hash_plan": compact_plan(hash_plan),
            "normal_plan": compact_plan(normal_plan),
            "hash_status": hash_measure["status"],
            "normal_status": normal_measure["status"],
            "hash_median_ms": hash_measure.get("median_ms"),
            "hash_min_ms": hash_measure.get("min_ms"),
            "hash_p95_ms": hash_measure.get("p95_ms"),
            "normal_median_ms": normal_measure.get("median_ms"),
            "normal_min_ms": normal_measure.get("min_ms"),
            "normal_p95_ms": normal_measure.get("p95_ms"),
            "speedup": calc_speedup(normal_measure.get("median_ms"), hash_measure.get("median_ms")),
            "result_consistent": self.is_consistent(hash_measure, normal_measure),
        }
        tdLog.info(f"hash join performance result: {json.dumps(result, sort_keys=True)}")
        return result

    def is_consistent(self, hash_measure, normal_measure):
        if normal_measure.get("status") == "success":
            return int(hash_measure["output_rows"]) == int(normal_measure["output_rows"])
        return None


def measure_sql(sql, rounds):
    elapsed = []
    output_rows = None
    rounds_rows = []

    tdSql.cursor.execute(sql)
    warm_rows = tdSql.cursor.fetchall()
    output_rows = extract_count_result(warm_rows)

    for _ in range(rounds):
        started = time.perf_counter()
        tdSql.cursor.execute(sql)
        rows = tdSql.cursor.fetchall()
        elapsed.append((time.perf_counter() - started) * 1000)
        output_rows = extract_count_result(rows)
        rounds_rows.append(output_rows)

    elapsed_sorted = sorted(elapsed)
    if output_rows is None or len(set(rounds_rows)) != 1:
        raise ValueError(f"inconsistent row counts across rounds: {rounds_rows}")
    return {
        "status": "success",
        "output_rows": output_rows,
        "median_ms": round(statistics.median(elapsed_sorted), 3),
        "min_ms": round(min(elapsed_sorted), 3),
        "p95_ms": round(percentile(elapsed_sorted, 0.95), 3),
        "rounds": rounds,
    }


def extract_count_result(rows):
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError(f"expected one COUNT(*) row, got {rows}")
    return int(rows[0][0])


def explain_text(sql):
    tdSql.cursor.execute("EXPLAIN " + sql)
    rows = tdSql.cursor.fetchall()
    return "\n".join("|".join(str(v) for v in row) for row in rows)


def compact_plan(plan):
    if not plan:
        return ""
    lines = [line.strip() for line in plan.splitlines() if line.strip()]
    return " ; ".join(lines)[:1000]


def expected_inner_count(rows, duplicate_mod):
    if duplicate_mod <= 0:
        return rows
    base = rows // duplicate_mod
    rem = rows % duplicate_mod
    total = 0
    for key in range(duplicate_mod):
        count = base + (1 if key < rem else 0)
        total += count * count
    return total


def expected_residual_count(rows, duplicate_mod):
    if duplicate_mod <= 0:
        return rows

    # insert_rows assigns k=i%duplicate_mod and v=base+i, so adjacent
    # ordinals within one key group differ by duplicate_mod in v.
    max_left_ordinal_delta = (RIGHT_VALUE_BASE - LEFT_VALUE_BASE - 1) // duplicate_mod
    base = rows // duplicate_mod
    rem = rows % duplicate_mod
    total = 0
    for key in range(duplicate_mod):
        count = base + (1 if key < rem else 0)
        for left_ordinal in range(count):
            total += count - max(0, left_ordinal - max_left_ordinal_delta)
    return total


def percentile(values, ratio):
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    idx = int(round((len(values) - 1) * ratio))
    return values[min(idx, len(values) - 1)]


def calc_speedup(normal_ms, hash_ms):
    if normal_ms is None or hash_ms is None or hash_ms == 0:
        return None
    return round(normal_ms / hash_ms, 3)


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, sort_keys=True))
            fp.write("\n")


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
