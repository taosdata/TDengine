"""VST EXPAND Performance Benchmark.

Measures query latency across different scenarios to identify bottlenecks:
1. EXPAND depth impact (1-level vs 10-level)
2. VCT count scaling (5, 50, 200 VCTs)
3. Row count scaling (100, 10000, 100000 rows per VCT)
4. EXPAND(-1) vs individual queries (overhead measurement)
5. Column count impact (2 cols vs 10 cols)
"""

import taos
import time
import sys

from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    DB_NAME = "db_exp_perf"

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.results = []

    def _time_query(self, sql, label, warmup=1, runs=3):
        """Execute query multiple times, return avg latency in ms."""
        # Warmup
        for _ in range(warmup):
            tdSql.query(sql)
        # Timed runs
        times = []
        for _ in range(runs):
            start = time.time()
            tdSql.query(sql)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
        avg_ms = sum(times) / len(times)
        row_count = tdSql.queryRows
        self.results.append((label, avg_ms, row_count))
        tdLog.printNoPrefix(f"  [{label}] avg={avg_ms:.2f}ms rows={row_count}")
        return avg_ms

    # ============================================================
    # Scenario 1: EXPAND depth impact
    # ============================================================

    def setup_depth_test(self):
        """10-level chain, 1 VCT per level, 100 rows each."""
        tdLog.printNoPrefix("=== PERF: Setting up depth test (10 levels × 100 rows) ===")
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME} VGROUPS 2")
        tdSql.execute(f"USE {self.DB_NAME}")

        tdSql.execute(
            "CREATE STABLE depth_src (ts TIMESTAMP, c1 INT, c2 INT, c3 INT, "
            "c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT) TAGS (g INT)"
        )

        base_ts = 1700000000000
        for level in range(10):
            tbl = f"depth_src_l{level}"
            tdSql.execute(f"CREATE TABLE {tbl} USING depth_src TAGS({level})")
            batch_size = 50
            for batch_start in range(0, 100, batch_size):
                values = []
                for row in range(batch_start, batch_start + batch_size):
                    ts = base_ts + (level * 1000 + row) * 1000
                    v = level * 1000 + row
                    cols = [str(level * 100 + n * 10 + row) for n in range(2, 11)]
                    values.append(f"({ts}, {v}, {', '.join(cols)})")
                tdSql.execute(f"INSERT INTO {tbl} VALUES {' '.join(values)}")

        # VST chain
        tdSql.execute("CREATE STABLE depth_l0 (ts TIMESTAMP, val INT) TAGS (t0 INT) VIRTUAL 1")
        for level in range(1, 10):
            tdSql.execute(
                f"CREATE VIRTUAL STABLE depth_l{level} BASE ON depth_l{level-1} "
                f"(c{level} INT) TAGS (t{level} INT) VIRTUAL 1"
            )

        # VCTs
        for level in range(10):
            col_parts = [f"val FROM `{self.DB_NAME}`.`depth_src_l{level}`.`c1`"]
            for n in range(1, level + 1):
                col_parts.append(f"c{n} FROM `{self.DB_NAME}`.`depth_src_l{level}`.`c{n+1}`")
            tag_names = ", ".join([f"t{n}" for n in range(level + 1)])
            tag_vals = ", ".join([str(level * 10 + n) for n in range(level + 1)])
            tdSql.execute(
                f"CREATE VTABLE depth_vct_l{level} ({', '.join(col_parts)}) "
                f"USING depth_l{level} ({tag_names}) TAGS({tag_vals})"
            )

    def perf_depth_expand(self):
        """Measure EXPAND latency at different depths."""
        tdLog.printNoPrefix("=== PERF: Depth scaling ===")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(0)", "depth_expand(0)")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(1)", "depth_expand(1)")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(3)", "depth_expand(3)")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(5)", "depth_expand(5)")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(9)", "depth_expand(9)")
        self._time_query("SELECT COUNT(*) FROM depth_l0 EXPAND(-1)", "depth_expand(-1)")

        # Full scan with ORDER BY
        self._time_query("SELECT val FROM depth_l0 EXPAND(-1) ORDER BY val", "depth_full_scan_ordered")
        self._time_query("SELECT val FROM depth_l0 EXPAND(0) ORDER BY val", "depth_scan_l0_only")

    # ============================================================
    # Scenario 2: VCT count scaling
    # ============================================================

    def setup_vct_count_test(self):
        """2-level hierarchy, variable VCT counts per level."""
        tdLog.printNoPrefix("=== PERF: Setting up VCT count test ===")

        NUM_VCTS = 50  # 50 VCTs per level, 2 levels = 100 VCTs total
        ROWS_PER_VCT = 20

        tdSql.execute(
            "CREATE STABLE vctcnt_src (ts TIMESTAMP, c1 INT, c2 INT) TAGS (g INT)"
        )

        base_ts = 1700000000000
        for i in range(NUM_VCTS * 2):
            tbl = f"vctcnt_src_{i}"
            tdSql.execute(f"CREATE TABLE {tbl} USING vctcnt_src TAGS({i})")
            values = []
            for row in range(ROWS_PER_VCT):
                ts = base_ts + (i * 1000 + row) * 1000
                values.append(f"({ts}, {i * 100 + row}, {i * 200 + row})")
            tdSql.execute(f"INSERT INTO {tbl} VALUES {' '.join(values)}")

        # 2-level VST
        tdSql.execute("CREATE STABLE vctcnt_l0 (ts TIMESTAMP, val INT) TAGS (t0 INT) VIRTUAL 1")
        tdSql.execute(
            "CREATE VIRTUAL STABLE vctcnt_l1 BASE ON vctcnt_l0 "
            "(extra INT) TAGS (t1 INT) VIRTUAL 1"
        )

        # Create 50 VCTs at level 0
        for i in range(NUM_VCTS):
            src = f"vctcnt_src_{i}"
            tdSql.execute(
                f"CREATE VTABLE vctcnt_v0_{i} "
                f"(val FROM `{self.DB_NAME}`.`{src}`.`c1`) "
                f"USING vctcnt_l0 (t0) TAGS({i})"
            )

        # Create 50 VCTs at level 1
        for i in range(NUM_VCTS):
            src = f"vctcnt_src_{NUM_VCTS + i}"
            tdSql.execute(
                f"CREATE VTABLE vctcnt_v1_{i} "
                f"(val FROM `{self.DB_NAME}`.`{src}`.`c1`, "
                f"extra FROM `{self.DB_NAME}`.`{src}`.`c2`) "
                f"USING vctcnt_l1 (t0, t1) TAGS({100 + i}, {i})"
            )

    def perf_vct_count(self):
        """Measure latency with many VCTs."""
        tdLog.printNoPrefix("=== PERF: VCT count scaling (50 VCTs/level) ===")
        self._time_query("SELECT COUNT(*) FROM vctcnt_l0 EXPAND(0)", "50vct_expand(0)")
        self._time_query("SELECT COUNT(*) FROM vctcnt_l0 EXPAND(-1)", "50vct_expand(-1)")
        self._time_query("SELECT SUM(val) FROM vctcnt_l0 EXPAND(-1)", "50vct_sum_expand(-1)")
        self._time_query("SELECT val FROM vctcnt_l0 EXPAND(-1) ORDER BY val LIMIT 10", "50vct_top10")

    # ============================================================
    # Scenario 3: Row count scaling
    # ============================================================

    def setup_row_count_test(self):
        """2-level, 3 VCTs per level, 10000 rows each."""
        tdLog.printNoPrefix("=== PERF: Setting up row count test (6 VCTs × 10000 rows) ===")

        ROWS = 10000

        tdSql.execute("CREATE STABLE rowcnt_src (ts TIMESTAMP, c1 INT, c2 INT) TAGS (g INT)")

        base_ts = 1700000000000
        for i in range(6):
            tbl = f"rowcnt_src_{i}"
            tdSql.execute(f"CREATE TABLE {tbl} USING rowcnt_src TAGS({i})")
            batch_size = 500
            for batch_start in range(0, ROWS, batch_size):
                values = []
                for row in range(batch_start, min(batch_start + batch_size, ROWS)):
                    ts = base_ts + (i * 100000 + row) * 1000
                    values.append(f"({ts}, {i * 10000 + row}, {row * 2})")
                tdSql.execute(f"INSERT INTO {tbl} VALUES {' '.join(values)}")

        # 2-level VST
        tdSql.execute("CREATE STABLE rowcnt_l0 (ts TIMESTAMP, val INT) TAGS (t0 INT) VIRTUAL 1")
        tdSql.execute(
            "CREATE VIRTUAL STABLE rowcnt_l1 BASE ON rowcnt_l0 "
            "(extra INT) TAGS (t1 INT) VIRTUAL 1"
        )

        # 3 VCTs per level
        for i in range(3):
            src = f"rowcnt_src_{i}"
            tdSql.execute(
                f"CREATE VTABLE rowcnt_v0_{i} "
                f"(val FROM `{self.DB_NAME}`.`{src}`.`c1`) "
                f"USING rowcnt_l0 (t0) TAGS({i})"
            )
        for i in range(3):
            src = f"rowcnt_src_{3 + i}"
            tdSql.execute(
                f"CREATE VTABLE rowcnt_v1_{i} "
                f"(val FROM `{self.DB_NAME}`.`{src}`.`c1`, "
                f"extra FROM `{self.DB_NAME}`.`{src}`.`c2`) "
                f"USING rowcnt_l1 (t0, t1) TAGS({10 + i}, {i})"
            )

    def perf_row_count(self):
        """Measure latency with large row counts."""
        tdLog.printNoPrefix("=== PERF: Row count scaling (60000 total rows) ===")
        self._time_query("SELECT COUNT(*) FROM rowcnt_l0 EXPAND(0)", "10k×3_expand(0)")
        self._time_query("SELECT COUNT(*) FROM rowcnt_l0 EXPAND(-1)", "10k×6_expand(-1)")
        self._time_query("SELECT SUM(val) FROM rowcnt_l0 EXPAND(-1)", "10k×6_sum")
        self._time_query("SELECT val FROM rowcnt_l0 EXPAND(-1) ORDER BY val LIMIT 100", "10k×6_top100")
        self._time_query("SELECT val FROM rowcnt_l0 EXPAND(-1) WHERE val > 40000 ORDER BY val", "10k×6_filter")

    # ============================================================
    # Scenario 4: EXPAND overhead vs individual queries
    # ============================================================

    def perf_expand_overhead(self):
        """Compare EXPAND(-1) latency vs sum of individual level queries."""
        tdLog.printNoPrefix("=== PERF: EXPAND overhead comparison ===")

        # Using depth test data (10 levels × 100 rows)
        expand_time = self._time_query(
            "SELECT val FROM depth_l0 EXPAND(-1) ORDER BY val",
            "overhead_expand(-1)"
        )

        # Individual queries (simulate what user would do without EXPAND)
        start = time.time()
        all_vals = []
        for level in range(10):
            tdSql.query(f"SELECT val FROM depth_l{level} ORDER BY val")
            all_vals.extend([row[0] for row in tdSql.queryResult])
        all_vals.sort()
        individual_time = (time.time() - start) * 1000

        overhead = expand_time - individual_time
        ratio = expand_time / max(individual_time, 0.01)
        self.results.append(("overhead_individual_sum", individual_time, 1000))
        self.results.append(("overhead_ratio", ratio, 0))
        tdLog.printNoPrefix(
            f"  EXPAND(-1)={expand_time:.2f}ms vs "
            f"10×individual={individual_time:.2f}ms "
            f"(ratio={ratio:.2f}x)"
        )

    # ============================================================
    # Scenario 5: Catalog descendant lookup cost
    # ============================================================

    def perf_catalog_lookup(self):
        """Measure first-query vs cached-query latency (catalog cache effect)."""
        tdLog.printNoPrefix("=== PERF: Catalog cache effect ===")

        # First query (cold catalog for this specific EXPAND)
        start = time.time()
        tdSql.query("SELECT COUNT(*) FROM vctcnt_l0 EXPAND(-1)")
        cold_ms = (time.time() - start) * 1000

        # Second query (catalog should be cached)
        start = time.time()
        tdSql.query("SELECT COUNT(*) FROM vctcnt_l0 EXPAND(-1)")
        warm_ms = (time.time() - start) * 1000

        self.results.append(("catalog_cold", cold_ms, 0))
        self.results.append(("catalog_warm", warm_ms, 0))
        tdLog.printNoPrefix(f"  Cold={cold_ms:.2f}ms, Warm={warm_ms:.2f}ms")

    # ============================================================
    # Report
    # ============================================================

    def print_report(self):
        tdLog.printNoPrefix("\n" + "=" * 70)
        tdLog.printNoPrefix("PERFORMANCE REPORT")
        tdLog.printNoPrefix("=" * 70)
        tdLog.printNoPrefix(f"{'Test':<35} {'Avg(ms)':>10} {'Rows':>10}")
        tdLog.printNoPrefix("-" * 70)
        for label, avg_ms, rows in self.results:
            tdLog.printNoPrefix(f"{label:<35} {avg_ms:>10.2f} {rows:>10}")
        tdLog.printNoPrefix("=" * 70)

        # Identify potential bottlenecks
        tdLog.printNoPrefix("\nPOTENTIAL BOTTLENECKS:")
        depth_results = [(l, t) for l, t, _ in self.results if l.startswith("depth_expand")]
        if len(depth_results) >= 2:
            t0 = next((t for l, t in depth_results if "expand(0)" in l), None)
            tall = next((t for l, t in depth_results if "expand(-1)" in l), None)
            if t0 and tall:
                growth = tall / max(t0, 0.01)
                tdLog.printNoPrefix(
                    f"  - Depth scaling: EXPAND(-1)/EXPAND(0) = {growth:.1f}x "
                    f"({'LINEAR OK' if growth < 15 else 'SUPER-LINEAR — investigate!'})"
                )

        overhead_ratio = next((t for l, t, _ in self.results if l == "overhead_ratio"), None)
        if overhead_ratio:
            tdLog.printNoPrefix(
                f"  - EXPAND overhead vs individual: {overhead_ratio:.2f}x "
                f"({'ACCEPTABLE' if overhead_ratio < 3.0 else 'HIGH — catalog/plan overhead'})"
            )

        catalog_cold = next((t for l, t, _ in self.results if l == "catalog_cold"), None)
        catalog_warm = next((t for l, t, _ in self.results if l == "catalog_warm"), None)
        if catalog_cold and catalog_warm:
            tdLog.printNoPrefix(
                f"  - Catalog cache: cold={catalog_cold:.1f}ms warm={catalog_warm:.1f}ms "
                f"(delta={catalog_cold - catalog_warm:.1f}ms)"
            )

    def test_cleanup(self):
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB_NAME}")

    def run(self):
        self.setup_depth_test()
        self.perf_depth_expand()

        self.setup_vct_count_test()
        self.perf_vct_count()

        self.setup_row_count_test()
        self.perf_row_count()

        self.perf_expand_overhead()
        self.perf_catalog_lookup()

        self.print_report()
        self.test_cleanup()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
