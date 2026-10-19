import importlib.util
import json
import tempfile
import unittest
from datetime import datetime
from itertools import product
from pathlib import Path


path = Path(__file__).parents[1] / "test_interp_table_scan_perf.py"
spec = importlib.util.spec_from_file_location("interp_perf", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TestInterpPerfLayout(unittest.TestCase):
    def setUp(self):
        self.case = module.TestInterpTableScanPerf()
        self.case.table_count = 8
        self.case.block_rows = 1024
        self.case.side_blocks = 500
        self.case.far_block = 100
        self.case.main_rows = 8196

    def test_matrix_covers_each_dependency_and_distance(self):
        scenarios = self.case.build_scenarios()
        self.assertEqual(len(scenarios), 72)
        self.assertEqual(len({s["name"] for s in scenarios}), 72)
        actual = {
            (s["grouped"], s["prev_profile"], s["next_profile"],
             s["needs_prev"], s["needs_next"])
            for s in scenarios
        }
        profiles = ("near", "far", "absent")
        expected = set(product(
            (False, True), profiles, profiles, (False, True), (False, True)
        ))
        self.assertEqual(actual, expected)

    def test_null_prefixes_place_far_reference_in_block_100(self):
        sides = {"left": [], "right": []}
        for name, config in self.case.build_insert_configs():
            stable = config["databases"][0]["super_tables"][0]
            valid = {
                c["name"] for c in stable["columns"] if not c["all_null"]
            }
            if name == "main":
                self.assertEqual(stable["insert_rows"], 8196)
                self.assertEqual(len(valid), 9)
                continue
            self.assertEqual(stable["insert_rows"] % 1024, 0)
            sides[name.split("_")[0]].extend(
                [valid] * (stable["insert_rows"] // 1024)
            )
        for side, blocks in sides.items():
            self.assertEqual(len(blocks), 500)
            if side == "left":
                blocks.reverse()
            for prev, next_ in product(
                ("near", "far", "absent"), repeat=2
            ):
                column = f"v_{prev}_{next_}"
                profile = prev if side == "left" else next_
                valid_indices = [
                    i for i, block in enumerate(blocks) if column in block
                ]
                expected = {"near": [0], "far": [99], "absent": []}
                self.assertEqual(valid_indices, expected[profile])

    def test_insert_segments_have_no_timestamp_gaps_or_overlap(self):
        next_start = self.case.start_ms() - 500 * 1024
        for name, config in self.case.build_insert_configs():
            stable = config["databases"][0]["super_tables"][0]
            timestamp = datetime.fromisoformat(
                stable["start_timestamp"].replace("Z", "+00:00")
            )
            self.assertEqual(int(timestamp.timestamp() * 1000), next_start)
            next_start += stable["insert_rows"]
        self.assertEqual(next_start, 1767312512000)

    def test_main_tail_ends_at_fileset_boundary(self):
        configs = dict(self.case.build_insert_configs())
        main = configs["main"]["databases"][0]
        self.assertEqual(main["dbinfo"]["duration"], 1)
        end = self.case.start_ms() + self.case.main_rows
        self.assertEqual(end % 86400000, 0)

    def test_missing_references_remove_only_unfillable_endpoints(self):
        for scenario in self.case.build_scenarios():
            missing = int(
                scenario["needs_prev"] and scenario["prev_profile"] == "absent"
            ) + int(
                scenario["needs_next"] and scenario["next_profile"] == "absent"
            )
            tables = 8 if scenario["grouped"] else 1
            self.assertEqual(
                scenario["expected_rows"], (8196 - missing) * tables
            )

    def test_queries_choose_both_profiles_and_only_filter_main_holes(self):
        start = self.case.start_ms()
        end = start + 8195
        for scenario in self.case.build_scenarios():
            sql = scenario["sql"]
            lowered = sql.lower()
            prev, next_ = scenario["prev_profile"], scenario["next_profile"]
            self.assertIn(f"interp(v_{prev}_{next_}, 1)", sql)
            self.assertEqual(f"ts != {start}" in sql, scenario["needs_prev"])
            self.assertEqual(f"ts != {end}" in sql, scenario["needs_next"])
            self.assertEqual(
                " where " in sql,
                scenario["needs_prev"] or scenario["needs_next"],
            )
            self.assertIn(f"range({start}, {end})", sql)
            self.assertNotIn("is not null", lowered)
            self.assertNotIn("case ", lowered)

    def test_external_baseline_keeps_candidate_modes_separate(self):
        def metrics(median_ms):
            return {
                "median_ms": median_ms,
                "rows": 10,
                "sha256": "same-result",
            }

        result = {
            "schema_version": 1,
            "scale": {"rows": 10},
            "scenarios": {
                "case": {
                    "sql": "select interp(v) from t",
                    "modes": {
                        "legacy": metrics(300),
                        "main_first": metrics(80),
                    },
                },
            },
        }
        baseline = {
            "schema_version": 1,
            "scale": {"rows": 10},
            "binary": {"version": "3.0"},
            "scenarios": {
                "case": {
                    "sql": "select interp(v) from t",
                    "modes": {"current": metrics(100)},
                },
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            path.write_text(json.dumps(baseline), encoding="utf-8")
            failures = self.case.compare_external_baseline(result, path)

        self.assertEqual(failures, [])
        scenario = result["scenarios"]["case"]
        self.assertEqual(set(scenario["modes"]), {"legacy", "main_first"})
        comparison = scenario["comparisons"][
            "main_first_vs_external_current"
        ]
        self.assertEqual(comparison["baseline"], "current")
        self.assertEqual(comparison["candidate"], "main_first")
        self.assertEqual(comparison["median_delta_pct"], -20.0)
        self.assertTrue(comparison["enforced"])


if __name__ == "__main__":
    unittest.main()
