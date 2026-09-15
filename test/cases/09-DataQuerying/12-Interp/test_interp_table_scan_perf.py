from new_test_framework.utils import tdLog, tdSql, etool

import copy
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from itertools import product
from pathlib import Path


class TestInterpTableScanPerf:
    """INTERP performance guard for existing Table Scan plans.

    Catalog:
        - Query:Interp

    Since: v3.4.3.0

    Labels: common,performance

    Jira: None

    History:
        - 2026-09-04 Tony Zhang created
    """

    updatecfgDict = {
        "asynclog": 0,
        "cDebugFlag": 131,
        "dDebugFlag": 131,
        "debugFlag": 131,
        "jniDebugFlag": 131,
        "mDebugFlag": 131,
        "qDebugFlag": 131,
        "rpcDebugFlag": 131,
        "sDebugFlag": 131,
        "smaDebugFlag": 131,
        "stDebugFlag": 131,
        "tmrDebugFlag": 131,
        "tqDebugFlag": 131,
        "uDebugFlag": 131,
        "vDebugFlag": 131,
        "wDebugFlag": 131,
    }

    DB = "interp_table_scan_perf_v3"
    STB = "meters"
    NEXT_START = datetime(2026, 1, 2, tzinfo=timezone.utc)
    MEDIAN_LIMIT_PCT = 5.0
    PROFILES = ("near", "far", "absent")

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def env_int(self, name, default, minimum):
        value = int(os.environ.get(name, default))
        if value < minimum:
            raise ValueError(f"{name} must be >= {minimum}")
        return value

    def read_settings(self):
        self.table_count = self.env_int("INTERP_PERF_TABLES", 8, 1)
        self.block_rows = 1024
        self.side_blocks = self.env_int("INTERP_PERF_SIDE_BLOCKS", 500, 3)
        self.far_block = self.env_int("INTERP_PERF_FAR_BLOCK", 100, 2)
        self.main_rows = self.env_int("INTERP_PERF_MAIN_ROWS", 8196, 3)
        assert self.far_block < self.side_blocks
        self.warmups = self.env_int("INTERP_PERF_WARMUPS", 5, 0)
        self.iterations = self.env_int("INTERP_PERF_ITERATIONS", 30, 1)

    def start_ms(self):
        # End MAIN at a fileset boundary so its tail cannot split NEXT blocks.
        return int(self.NEXT_START.timestamp() * 1000) - self.main_rows

    def format_timestamp(self, value):
        timestamp = datetime.fromtimestamp(value / 1000, timezone.utc)
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def reference_segments(self):
        block = self.block_rows
        return [
            ("left_outer", (self.side_blocks - self.far_block) * block),
            ("left_far", block),
            ("left_gap", (self.far_block - 2) * block),
            ("left_near", block),
            ("main", self.main_rows),
            ("right_near", block),
            ("right_gap", (self.far_block - 2) * block),
            ("right_far", block),
            ("right_outer", (self.side_blocks - self.far_block) * block),
        ]

    def build_insert_configs(self):
        path = Path(__file__).with_name("in") / "interp_table_scan_perf.json"
        template = json.loads(path.read_text(encoding="utf-8"))
        template["port"] = self.env_int("INTERP_PERF_PORT", 6030, 1)
        database = template["databases"][0]
        database["dbinfo"].update({
            "name": self.DB, "minrows": self.block_rows,
            "maxrows": self.block_rows,
        })
        stable = database["super_tables"][0]
        stable["childtable_count"] = self.table_count
        stable["batch_create_tbl_num"] = self.table_count

        configs = []
        offset = -self.side_blocks * self.block_rows
        for index, (name, rows) in enumerate(self.reference_segments()):
            if rows == 0:
                continue
            config = copy.deepcopy(template)
            db = config["databases"][0]
            db["dbinfo"].update({
                "name": self.DB, "minrows": self.block_rows,
                "maxrows": self.block_rows,
                "drop": "yes" if index == 0 else "no",
            })
            stb = db["super_tables"][0]
            stb.update({
                "name": self.STB, "childtable_prefix": "s",
                "child_table_exists": "no" if index == 0 else "yes",
                "insert_rows": rows,
                "timestamp_step": 1,
                "start_timestamp": self.format_timestamp(
                    self.start_ms() + offset
                ),
            })
            column_template = stb["columns"][0]
            stb["columns"] = []
            for prev, next_ in product(self.PROFILES, repeat=2):
                column = copy.deepcopy(column_template)
                column["name"] = f"v_{prev}_{next_}"
                column["all_null"] = name not in (
                    "main", f"left_{prev}", f"right_{next_}"
                )
                stb["columns"].append(column)
            configs.append((name, config))
            offset += rows
        return configs

    def load_segment(self, name, config):
        with tempfile.TemporaryDirectory() as temp_dir:
            config["result_file"] = str(Path(temp_dir) / "insert-result.txt")
            generated = Path(temp_dir) / "insert.json"
            generated.write_text(json.dumps(config), encoding="utf-8")
            process = subprocess.run(
                [etool.benchMarkFile(), "-f", str(generated)],
                capture_output=True, text=True,
                timeout=self.env_int("INTERP_PERF_LOAD_TIMEOUT", 900, 1),
                check=False,
            )
            if process.returncode != 0:
                raise RuntimeError(
                    f"taosBenchmark {name} failed: {process.returncode}\n"
                    f"{process.stdout}\n{process.stderr}"
                )

    def prepare_data(self):
        self.read_settings()
        self.insert_seconds = 0
        self.flush_seconds = 0
        if os.environ.get("INTERP_PERF_REUSE_DATA") != "1":
            for name, config in self.build_insert_configs():
                tdLog.info(f"loading INTERP data segment: {name}")
                started = time.perf_counter()
                self.load_segment(name, config)
                self.insert_seconds += time.perf_counter() - started
                started = time.perf_counter()
                # Keep side blocks separate from the partial MAIN block.
                db_name = config["databases"][0]["dbinfo"]["name"]
                tdSql.execute(f"flush database {db_name}")
                self.flush_seconds += time.perf_counter() - started
        self.layout_evidence = self.validate_data()

    def build_scenarios(self):
        scenarios = []
        start = self.start_ms()
        end = start + self.main_rows - 1
        for grouped in (False, True):
            prefix = "grouped" if grouped else "single"
            select = "tbname, " if grouped else ""
            partition = " partition by tbname" if grouped else ""
            tables = self.table_count if grouped else 1
            target = (
                f"{self.DB}.{self.STB}" if grouped else f"{self.DB}.s0"
            )
            for prev, next_, need_prev, need_next in product(
                self.PROFILES, self.PROFILES, (False, True), (False, True)
            ):
                predicates = []
                if need_prev:
                    predicates.append(f"ts != {start}")
                if need_next:
                    predicates.append(f"ts != {end}")
                where = (
                    " where " + " and ".join(predicates) if predicates else ""
                )
                first = start + int(prev == "absent" and need_prev)
                last = end - int(next_ == "absent" and need_next)
                holes = f"l{int(need_prev)}_r{int(need_next)}"
                scenarios.append({
                    "name": f"{prefix}_prev_{prev}_next_{next_}_{holes}",
                    "description": (
                        f"PREV={prev}, NEXT={next_}, "
                        f"left_hole={need_prev}, right_hole={need_next}"
                    ),
                    "grouped": grouped,
                    "prev_profile": prev, "next_profile": next_,
                    "needs_prev": need_prev, "needs_next": need_next,
                    "first_output_ms": first, "last_output_ms": last,
                    "expected_rows": tables * (last - first + 1),
                    "sample_repetitions": 1,
                    "sql": (
                        f"select {select}cast(_irowts as bigint), "
                        f"interp(v_{prev}_{next_}, 1) from {target}"
                        f"{where}{partition} "
                        f"range({start}, {end}) every(1a) fill(linear)"
                    ),
                })
        return scenarios

    def check_scalar(self, sql, expected):
        tdSql.query(sql)
        actual = tuple(tdSql.queryResult[0])
        assert actual == tuple(expected), (sql, actual, expected)
        self.data_checks.append({"sql": sql, "actual": actual})

    def validate_data(self):
        self.data_checks = []
        start = self.start_ms()
        main_rows = self.main_rows
        end = start + main_rows - 1
        evidence = {}
        tables = [
            (f"{self.DB}.s{i}", self.block_rows)
            for i in range(self.table_count)
        ]
        for table, block_rows in tables:
            side_rows = self.side_blocks * block_rows
            gap_rows = (self.far_block - 1) * block_rows
            # Evaluate NULL membership from rows, not column SMA counts.
            counts = "count(*), " + ", ".join(
                f"sum(case when {column} is null then 0 else 1 end)"
                for column in (
                    f"v_{prev}_{next_}"
                    for prev, next_ in product(self.PROFILES, repeat=2)
                )
            )
            self.check_scalar(
                f"select {counts} from {table} "
                f"where ts between {start} and {end}",
                (main_rows,) * (len(self.PROFILES) ** 2 + 1),
            )
            self.check_scalar(
                f"select count(*) from {table}",
                (main_rows + 2 * side_rows,),
            )
            for side, low, high, near, far in (
                ("prev", start - side_rows, start - 1,
                 start - 1, start - gap_rows - 1),
                ("next", end + 1, end + side_rows,
                 end + 1, end + gap_rows + 1),
            ):
                where = f"ts between {low} and {high}"
                self.check_scalar(
                    f"select {counts} from {table} where {where}",
                    (side_rows,) + tuple(
                        0 if profile == "absent" else block_rows
                        for prev, next_ in product(self.PROFILES, repeat=2)
                        for profile in (prev if side == "prev" else next_,)
                    ),
                )
                nearest = "last" if side == "prev" else "first"
                for prev, next_ in product(self.PROFILES, repeat=2):
                    profile = prev if side == "prev" else next_
                    if profile == "absent":
                        continue
                    timestamp = near if profile == "near" else far
                    self.check_scalar(
                        f"select cast({nearest}(ts) as bigint) from {table} "
                        f"where {where} and v_{prev}_{next_} is not null",
                        (timestamp,),
                    )
                direction = "desc" if side == "prev" else "asc"
                probes = (
                    ("all", low, high, self.side_blocks),
                    ("far_null_prefix",
                     start - gap_rows if side == "prev" else end + 1,
                     start - 1 if side == "prev" else end + gap_rows,
                     self.far_block - 1),
                )
                for label, probe_low, probe_high, blocks in probes:
                    sql = (
                        f"select ts, v_far_far from {table} where ts between "
                        f"{probe_low} and {probe_high} order by ts {direction}"
                    )
                    metrics = self.plan_evidence(sql)
                    assert metrics["total_blocks"] == blocks, (
                        table, side, label, blocks, metrics
                    )
                    assert metrics["check_rows"] == blocks * block_rows
                    evidence[f"{table}_{side}_{label}"] = metrics
        return evidence

    def validate_reference_scan(self, scenario, metrics):
        if not (scenario.get("needs_prev") and scenario.get("needs_next")):
            return
        block_rows = self.block_rows
        tables = self.table_count if scenario["grouped"] else 1
        main_rows = self.main_rows
        distances = {"near": 1, "far": self.far_block,
                     "absent": self.side_blocks}
        side_blocks = (
            distances[scenario["prev_profile"]]
            + distances[scenario["next_profile"]]
        )
        main_blocks = (main_rows + block_rows - 1) // block_rows
        expected_blocks = tables * (main_blocks + side_blocks)
        assert metrics["total_blocks"] == expected_blocks, (
            scenario["name"], metrics, expected_blocks
        )
        expected_rows = tables * (main_rows + side_blocks * block_rows)
        assert metrics["check_rows"] == expected_rows, (
            scenario["name"], metrics, expected_rows
        )

    def validate_scenario(self, scenario):
        tdSql.query(scenario["sql"])
        rows = [tuple(row) for row in tdSql.queryResult]
        assert len(rows) == scenario["expected_rows"], (
            scenario["name"], len(rows), scenario["expected_rows"]
        )
        if rows:
            groups = {}
            for row in rows:
                group = row[0] if scenario["grouped"] else "s0"
                timestamp, value = row[-2:]
                assert value == 1.0, (scenario["name"], row)
                groups.setdefault(group, []).append(timestamp)
            expected_groups = (
                {f"s{i}" for i in range(self.table_count)}
                if scenario["grouped"] else {"s0"}
            )
            assert set(groups) == expected_groups
            first, last = (
                scenario["first_output_ms"], scenario["last_output_ms"]
            )
            for timestamps in groups.values():
                assert sorted(timestamps) == list(range(first, last + 1)), (
                    scenario["name"], first, last
                )
        return self.result_signature(rows)

    def discover_modes(self):
        tdSql.query(
            "show local variables like 'tsInterpRangePrune'"
        )
        has_switch = any(
            str(row[0]).lower() == "tsinterprangeprune"
            for row in tdSql.queryResult
        )
        requested = os.environ.get("INTERP_PERF_MODE", "auto").lower()
        if requested == "auto":
            if has_switch:
                return ("legacy", "main_first"), has_switch
            return ("current",), has_switch
        if requested == "both":
            assert has_switch, "both modes require tsInterpRangePrune"
            return ("legacy", "main_first"), has_switch
        assert requested in ("legacy", "main_first", "current"), (
            f"unsupported INTERP_PERF_MODE={requested}"
        )
        if requested != "current":
            assert has_switch, (
                f"{requested} mode requires tsInterpRangePrune"
            )
        else:
            assert not has_switch, (
                "current mode requires a build without tsInterpRangePrune"
            )
        return (requested,), has_switch

    def set_mode(self, mode):
        if mode == "current":
            return
        enabled = 1 if mode == "main_first" else 0
        tdSql.execute(
            f"alter local 'tsInterpRangePrune' '{enabled}'"
        )

    def plan_evidence(self, sql):
        tdSql.query(f"explain {sql}")
        lines = [str(row[0]) for row in tdSql.queryResult]
        plan = "\n".join(lines)
        assert "Table Scan" in plan, f"Table Scan missing:\n{plan}"
        assert "Table Merge Scan" not in plan, (
            f"unexpected Table Merge Scan:\n{plan}"
        )

        tdSql.query(f"explain analyze verbose true {sql}")
        analyze = "\n".join(str(row[0]) for row in tdSql.queryResult)
        check_rows = self.scan_metric(analyze, "check_rows")
        total_blocks = self.scan_metric(analyze, "total_blocks")
        exchange = re.search(r"Data Exchange (\d+):1", analyze)
        scan_instances = int(exchange.group(1)) if exchange else 1
        return {
            "plan": lines,
            "check_rows": round(
                check_rows["average"] * scan_instances
            ),
            "check_rows_average": check_rows["average"],
            "check_rows_maximum": check_rows["maximum"],
            "total_blocks": round(
                total_blocks["average"] * scan_instances
            ),
            "total_blocks_average": total_blocks["average"],
            "total_blocks_maximum": total_blocks["maximum"],
            "scan_instances": scan_instances,
        }

    def scan_metric(self, analyze, name):
        pattern = rf"{name}=([0-9]+(?:\.[0-9]+)?)"
        pattern += r"(?:\(([0-9]+(?:\.[0-9]+)?)\))?"
        matches = re.findall(pattern, analyze)
        assert len(matches) == 1, (
            f"expected one {name} metric, found {len(matches)}:\n"
            f"{analyze}"
        )
        average, maximum = matches[0]
        return {
            "average": float(average),
            "maximum": float(maximum or average),
        }

    def result_signature(self, rows):
        normalized = []
        for row in rows:
            cells = []
            for value in row:
                if isinstance(value, bytes):
                    cells.append(value.hex())
                elif isinstance(value, datetime):
                    cells.append(value.isoformat())
                else:
                    cells.append(str(value))
            normalized.append(json.dumps(cells, ensure_ascii=True))
        normalized.sort()
        digest = hashlib.sha256()
        for row in normalized:
            digest.update(row.encode("utf-8"))
            digest.update(b"\n")
        return len(normalized), digest.hexdigest()

    def summarize(self, samples, row_count, digest, evidence):
        ordered = sorted(samples)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            median = ordered[middle]
        else:
            median = (ordered[middle - 1] + ordered[middle]) / 2
        return {
            "samples_ms": [round(value, 3) for value in samples],
            "median_ms": round(median, 3),
            "rows": row_count,
            "sha256": digest,
            **evidence,
        }

    def measure_scenario(self, scenario, modes):
        samples = {mode: [] for mode in modes}
        signatures = {}
        total_rounds = self.warmups + self.iterations

        for mode in modes:
            self.set_mode(mode)
            signatures[mode] = self.validate_scenario(scenario)

        for round_index in range(total_rounds):
            order = modes if round_index % 2 == 0 else tuple(reversed(modes))
            for mode in order:
                self.set_mode(mode)
                started = time.perf_counter_ns()
                for _ in range(scenario["sample_repetitions"]):
                    tdSql.query(scenario["sql"])
                elapsed_ms = (
                    time.perf_counter_ns() - started
                ) / 1000000 / scenario["sample_repetitions"]
                rows = [tuple(row) for row in tdSql.queryResult]
                assert len(rows) == scenario["expected_rows"], (
                    f"{scenario['name']} {mode}: expected "
                    f"{scenario['expected_rows']} rows, got {len(rows)}"
                )
                if mode not in signatures:
                    signatures[mode] = self.result_signature(rows)
                if round_index >= self.warmups:
                    samples[mode].append(elapsed_ms)

        expected_signature = signatures[modes[0]]
        for mode in modes[1:]:
            assert signatures[mode] == expected_signature, (
                f"{scenario['name']}: result changed between "
                f"{modes[0]} and {mode}"
            )

        result = {**scenario, "modes": {}}
        for mode in modes:
            self.set_mode(mode)
            evidence = self.plan_evidence(scenario["sql"])
            self.validate_reference_scan(scenario, evidence)
            row_count, digest = signatures[mode]
            result["modes"][mode] = self.summarize(
                samples[mode], row_count, digest, evidence
            )
        return result

    def compare_metrics(self, baseline, candidate, baseline_mode,
                        candidate_mode, enforced):
        assert candidate["rows"] == baseline["rows"]
        assert candidate["sha256"] == baseline["sha256"]

        median_delta = (
            candidate["median_ms"] / baseline["median_ms"] - 1
        ) * 100
        return {
            "baseline": baseline_mode,
            "candidate": candidate_mode,
            "median_delta_pct": round(median_delta, 2),
            "median_pass": median_delta <= self.MEDIAN_LIMIT_PCT,
            "enforced": enforced,
        }

    def compare_modes(self, scenario, baseline_mode, candidate_mode,
                      enforced):
        return self.compare_metrics(
            scenario["modes"][baseline_mode],
            scenario["modes"][candidate_mode],
            baseline_mode,
            candidate_mode,
            enforced,
        )

    def compare_external_baseline(self, result, baseline_path):
        baseline = json.loads(
            Path(baseline_path).read_text(encoding="utf-8")
        )
        assert baseline["schema_version"] == result["schema_version"], (
            "baseline uses a different performance matrix"
        )
        assert baseline["scale"] == result["scale"], (
            "baseline and candidate scales differ"
        )
        result["external_baseline"] = {
            "path": str(Path(baseline_path).resolve()),
            "binary": baseline["binary"],
        }
        first_scenario = next(iter(result["scenarios"].values()))
        candidate_modes = first_scenario["modes"]
        candidate_mode = next(
            (
                mode
                for mode in ("main_first", "current")
                if mode in candidate_modes
            ),
            None,
        )
        if candidate_mode is None:
            assert len(candidate_modes) == 1
            candidate_mode = next(iter(candidate_modes))

        failures = []
        for name, candidate_scenario in result["scenarios"].items():
            baseline_scenario = baseline["scenarios"][name]
            assert baseline_scenario["sql"] == candidate_scenario["sql"]
            assert candidate_mode in candidate_scenario["modes"]
            comparisons = candidate_scenario.setdefault(
                "comparisons", {}
            )
            candidate = candidate_scenario["modes"][candidate_mode]
            for baseline_mode, baseline_metrics in (
                baseline_scenario["modes"].items()
            ):
                enforced = baseline_mode in ("legacy", "current")
                key = f"{candidate_mode}_vs_external_{baseline_mode}"
                comparison = self.compare_metrics(
                    baseline_metrics,
                    candidate,
                    baseline_mode,
                    candidate_mode,
                    enforced,
                )
                comparisons[key] = comparison
                if enforced and not comparison["median_pass"]:
                    failures.append(f"{name}: {key} regressed")
        return failures

    def binary_metadata(self):
        bin_dir = Path(os.environ["TAOS_BIN_PATH"])
        binary = bin_dir / ("taosd.exe" if os.name == "nt" else "taosd")
        process = subprocess.run(
            [str(binary), "-V"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        version = process.stdout.strip()
        match = re.search(r"^git:\s*([0-9a-f]+)$", version, re.MULTILINE)
        return {
            "taosd": str(binary),
            "git_sha": match.group(1) if match else "unknown",
            "version": version,
        }

    def result_path(self, result):
        requested = os.environ.get("INTERP_PERF_RESULT")
        if requested:
            return Path(requested)
        short_sha = result["binary"]["git_sha"][:12]
        filename = f"interp_table_scan_perf_{short_sha}.json"
        return Path(tempfile.gettempdir()) / filename

    def run_performance_test(self):
        modes, has_switch = self.discover_modes()
        result = {
            "schema_version": 3,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "binary": self.binary_metadata(),
            "mode_switch_available": has_switch,
            "scale": {
                "tables": self.table_count,
                "main_rows": self.main_rows,
                "block_rows": self.block_rows,
                "side_blocks": self.side_blocks,
                "far_reference_block": self.far_block,
                "vgroups": 4,
                "warmups": self.warmups,
                "iterations": self.iterations,
            },
            "limits": {
                "median_regression_pct": self.MEDIAN_LIMIT_PCT,
            },
            "data_load": {
                "insert_seconds": round(self.insert_seconds, 3),
                "flush_seconds": round(self.flush_seconds, 3),
            },
            "layout_evidence": self.layout_evidence,
            "data_checks": self.data_checks,
            "scenarios": {},
        }
        failures = []

        try:
            for scenario in self.build_scenarios():
                tdLog.info(f"measuring {scenario['name']}: modes={modes}")
                measured = self.measure_scenario(scenario, modes)
                result["scenarios"][scenario["name"]] = measured
                if "legacy" in modes and "main_first" in modes:
                    comparison = self.compare_modes(
                        measured, "legacy", "main_first", True
                    )
                    measured["comparisons"] = {
                        "main_first_vs_legacy": comparison
                    }
                    if not comparison["median_pass"]:
                        failures.append(
                            f"{scenario['name']}: "
                            "main_first_vs_legacy regressed"
                        )

            baseline_path = os.environ.get(
                "INTERP_PERF_BASELINE_RESULT"
            )
            if baseline_path:
                failures.extend(
                    self.compare_external_baseline(
                        result, baseline_path
                    )
                )
        finally:
            if has_switch:
                self.set_mode("legacy")

        output_path = self.result_path(result)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        tdLog.info(f"INTERP performance result: {output_path}")
        for name, scenario in result["scenarios"].items():
            for mode, metrics in scenario["modes"].items():
                tdLog.info(
                    f"{name} {mode}: median={metrics['median_ms']} ms, "
                    f"check_rows={metrics['check_rows']}"
                )
        assert not failures, "; ".join(failures)

    def do_interp_table_scan_perf(self):
        self.prepare_data()
        if os.environ.get("INTERP_PERF_PREPARE_ONLY") == "1":
            self.validate_prepared_scenarios()
            print("INTERP performance data ................. [ prepared ]")
            return
        self.run_performance_test()
        print("INTERP Table Scan performance ................. [ passed ]")

    def validate_prepared_scenarios(self):
        modes, has_switch = self.discover_modes()
        result = {
            "schema_version": 3,
            "status": "data_prepared_not_performance_tested",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database": self.DB,
            "binary": self.binary_metadata(),
            "tables_per_stable": self.table_count,
            "block_rows": self.block_rows,
            "side_blocks": self.side_blocks,
            "far_reference_block": self.far_block,
            "main_rows": self.main_rows,
            "reference_rows_per_table": (
                self.main_rows + 2 * self.side_blocks * self.block_rows
            ),
            "layout_evidence": self.layout_evidence,
            "data_checks": self.data_checks,
            "scenarios": {},
        }
        try:
            for scenario in self.build_scenarios():
                validations = {}
                for mode in modes:
                    self.set_mode(mode)
                    rows, digest = self.validate_scenario(scenario)
                    metrics = self.plan_evidence(scenario["sql"])
                    self.validate_reference_scan(scenario, metrics)
                    validations[mode] = {
                        "rows": rows, "sha256": digest,
                        **metrics,
                    }
                result["scenarios"][scenario["name"]] = {
                    **scenario, "validation": validations,
                }
        finally:
            if has_switch:
                self.set_mode("legacy")
        output = self.result_path(result)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        tdLog.info(f"INTERP prepared data evidence: {output}")

    def test_interp_table_scan_perf(self):
        """Existing Table Scan INTERP plans have no performance regression.

        1. Cross independent PREV/NEXT distances and left/right MAIN holes.
        2. Run all 36 combinations for single-table and multi-group plans.
        3. Verify NULL layout and actual scan blocks before measuring median.

        Catalog:
            - Query:Interp

        Since: v3.4.3.0

        Labels: common,performance

        Jira: None

        History:
            - 2026-09-04 Tony Zhang created
            - 2026-09-07 Tony Zhang added reference-distance data matrix

        """
        self.do_interp_table_scan_perf()
