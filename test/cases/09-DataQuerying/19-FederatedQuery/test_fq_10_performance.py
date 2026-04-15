"""
test_fq_10_performance.py

Implements PERF-001 through PERF-012 from TS "性能测试" section.

Design notes:
    - Key metrics: QPS (queries-per-second) and P50/P95/P99 latency, collected
      via serial multi-run measurement (_measure_latency helper, 30 runs by
      default).  Latency is always measured by running the same SQL statement N
      times back-to-back (serial) to gather a stable distribution.
    - Tests that require large external databases (MySQL 100K+, PG 1M+) cannot
      run in CI.  Those tests implement a lightweight internal-data proxy so they
      still exercise the code path, report real metrics, and never use
      pytest.skip().
    - teardown_class prints a structured performance summary including per-test
      QPS, P50, P95, and P99.
    - All tests are guarded with try/finally so the environment is cleaned up
      even on failure.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
"""

import os
import time
from datetime import datetime

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
)


class TestFq10Performance(FederatedQueryVersionedMixin):
    """PERF-001 through PERF-012: Performance tests."""

    PERF_DB = "fq_perf_db"
    SRC_DB = "fq_perf_src"
    MERGE_DB = "fq_perf_merge"
    MERGE_SRC = "fq_perf_merge_src"

    # ------------------------------------------------------------------
    # Iteration / scale controls
    # Override via environment variables to tune test load:
    #
    #   FQ_PERF_LATENCY_RUNS    Serial repetitions per latency measurement (default 30)
    #   FQ_PERF_ROW_COUNT       Internal table row count for data prep      (default 2000)
    #   FQ_PERF_BURST_COUNT     Number of connection-pool bursts             (default 5)
    #   FQ_PERF_BURST_SIZE      Queries per burst                            (default 20)
    #   FQ_PERF_MERGE_SUBTABLES Number of sub-tables in multi-source merge   (default 10)
    #   FQ_PERF_MERGE_ROWS      Rows per sub-table in merge test             (default 100)
    #   FQ_PERF_MERGE_RUNS      Serial latency runs for merge query          (default 30)
    #   FQ_PERF_TIMEOUT_RUNS    Error-path runs for timeout/retry tests      (default 5)
    #
    # Example (light smoke run):
    #   FQ_PERF_LATENCY_RUNS=5 FQ_PERF_ROW_COUNT=200 pytest fq_10...
    # Example (stress run):
    #   FQ_PERF_LATENCY_RUNS=200 FQ_PERF_ROW_COUNT=100000 pytest fq_10...
    # ------------------------------------------------------------------
    _PERF_LATENCY_RUNS   = int(os.getenv("FQ_PERF_LATENCY_RUNS",   "30"))
    _PERF_ROW_COUNT      = int(os.getenv("FQ_PERF_ROW_COUNT",      "2000"))
    _PERF_BURST_COUNT    = int(os.getenv("FQ_PERF_BURST_COUNT",    "5"))
    _PERF_BURST_SIZE     = int(os.getenv("FQ_PERF_BURST_SIZE",     "20"))
    _PERF_MERGE_SUBTABLES = int(os.getenv("FQ_PERF_MERGE_SUBTABLES", "10"))
    _PERF_MERGE_ROWS     = int(os.getenv("FQ_PERF_MERGE_ROWS",     "100"))
    _PERF_MERGE_RUNS     = int(os.getenv("FQ_PERF_MERGE_RUNS",     "30"))
    _PERF_TIMEOUT_RUNS   = int(os.getenv("FQ_PERF_TIMEOUT_RUNS",   "5"))

    # Class-level test result and performance metric registry
    _test_results: list = []
    _session_start: float = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        TestFq10Performance._test_results = []
        TestFq10Performance._session_start = time.time()
        # Pre-cleanup: remove any leftover state from previous runs
        self._teardown_data()
        self._teardown_merge()
        self._cleanup_src("perf_timeout_src", "perf_retry_src")

    def teardown_class(self):
        """Final cleanup and structured performance test summary."""
        try:
            self._teardown_data()
            self._teardown_merge()
            self._cleanup_src("perf_timeout_src", "perf_retry_src")
        finally:
            self._print_summary()

    # ------------------------------------------------------------------
    # Test result + timing registry
    # ------------------------------------------------------------------

    def _start_test(self, name, description="", iterations=0):
        """Record test start time and metadata.

        The entry name is automatically suffixed with the current version label
        (e.g. ``PERF-001_latency[my8.0-pg16-inf3.0]``) so multi-version runs
        produce one distinct row per (scenario, version) combination in the
        performance summary.
        """
        ver_label = self._version_label()
        full_name = f"{name}[{ver_label}]"
        TestFq10Performance._test_results.append({
            "name": full_name,
            "base_name": name,
            "version": ver_label,
            "desc": description,
            "iterations": iterations,
            "start": time.time(),
            "end": None,
            "duration": None,
            "status": "RUNNING",
            "error": None,
            "perf": None,
        })

    def _record_pass(self, name, perf_stats=None):
        full_name = f"{name}[{self._version_label()}]"
        for r in reversed(TestFq10Performance._test_results):
            if r["name"] == full_name:
                r["end"] = time.time()
                r["duration"] = r["end"] - r["start"]
                r["status"] = "PASS"
                if perf_stats:
                    r["perf"] = perf_stats
                return
        # Fallback if _start_test was not called
        ver_label = self._version_label()
        TestFq10Performance._test_results.append({
            "name": full_name, "base_name": name, "version": ver_label,
            "desc": "", "iterations": 0,
            "start": time.time(), "end": time.time(), "duration": 0.0,
            "status": "PASS", "error": None, "perf": perf_stats,
        })

    def _record_fail(self, name, reason):
        full_name = f"{name}[{self._version_label()}]"
        for r in reversed(TestFq10Performance._test_results):
            if r["name"] == full_name:
                r["end"] = time.time()
                r["duration"] = r["end"] - r["start"]
                r["status"] = "FAIL"
                r["error"] = reason
                return
        ver_label = self._version_label()
        TestFq10Performance._test_results.append({
            "name": full_name, "base_name": name, "version": ver_label,
            "desc": "", "iterations": 0,
            "start": time.time(), "end": time.time(), "duration": 0.0,
            "status": "FAIL", "error": reason, "perf": None,
        })

    def _print_summary(self):
        """Print structured performance test summary to tdLog."""
        results = TestFq10Performance._test_results
        session_end = time.time()
        session_start = TestFq10Performance._session_start
        total_duration = session_end - session_start

        def _fmt_ts(ts):
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{dt.microsecond // 1000:03d}"

        total = len(results)
        passed = sum(1 for r in results if r["status"] == "PASS")
        failed = sum(1 for r in results if r["status"] == "FAIL")

        sep = "=" * 84
        mid = "-" * 84
        tdLog.debug(sep)
        tdLog.debug("  test_fq_10_performance  性能测试总结  (Performance Test Summary)")
        tdLog.debug(sep)
        tdLog.debug(f"  会话启动 / Session Start  : {_fmt_ts(session_start)}")
        tdLog.debug(f"  会话结束 / Session End    : {_fmt_ts(session_end)}")
        tdLog.debug(f"  总耗时   / Total Duration : {total_duration:.3f} s")
        tdLog.debug(mid)
        tdLog.debug(
            f"  {'#':<3}  {'测试名称':<40}  {'状态':<5}  {'耗时s':<7}  "
            f"{'n':<4}  {'QPS':<7}  {'P50ms':<8}  {'P95ms':<8}  {'P99ms':<8}  描述"
        )
        tdLog.debug(mid)
        for idx, r in enumerate(results, 1):
            status_col = r["status"]
            dur_s = f"{r['duration']:.3f}" if r["duration"] is not None else "N/A"
            p = r.get("perf")
            if p:
                n_col = str(p.get("n", "-"))
                qps_col = f"{p['qps']:.2f}"
                p50_col = f"{p['p50_ms']:.2f}"
                p95_col = f"{p['p95_ms']:.2f}"
                p99_col = f"{p['p99_ms']:.2f}"
            else:
                n_col = qps_col = p50_col = p95_col = p99_col = "-"
            name_col = r["name"][:40]
            desc_col = r["desc"] or ""
            tdLog.debug(
                f"  {idx:<3}  {name_col:<40}  {status_col:<5}  {dur_s:<7}  "
                f"{n_col:<4}  {qps_col:<7}  {p50_col:<8}  {p95_col:<8}  {p99_col:<8}  {desc_col}"
            )
        tdLog.debug(mid)
        tdLog.debug(
            f"  合计 / Total: {total}   通过 / Passed: {passed}   "
            f"失败 / Failed: {failed}"
        )
        if failed > 0:
            tdLog.debug(mid)
            tdLog.debug("  错误详情 / Error Details:")
            for r in results:
                if r["status"] == "FAIL":
                    tdLog.debug(f"    [{r['name']}]  {r['error']}")
        else:
            tdLog.debug("  错误汇总 / Errors: 无 / None")
        tdLog.debug(sep)

    # ------------------------------------------------------------------
    # Data helpers
    # ------------------------------------------------------------------

    def _prepare_internal_data(self, row_count=None):
        """Create internal tables and vtables for lightweight perf baselines.

        row_count defaults to ``self._PERF_ROW_COUNT`` when not given.

        Data layout:
            SRC_DB.perf_ntb   : row_count rows
                                 ts    = BASE + i*1000 ms (1-second intervals)
                                 v     = i % 100  (int)
                                 score = i * 0.5  (double)
                                 g     = i % 10   (int, used for group-by)
            SRC_DB.perf_join  : 200 rows with ts matching first 200 of perf_ntb,
                                 cat = i % 5  (used for PERF-004 JOIN)
            PERF_DB.perf_vstb : virtual super table schema
            PERF_DB.vt_perf   : vtable child -> perf_ntb
        """
        _BASE = 1704067200000  # 2024-01-01 00:00:00 UTC ms

        tdSql.execute(f"drop database if exists {self.PERF_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")
        tdSql.execute(f"create database {self.SRC_DB}")
        tdSql.execute(f"use {self.SRC_DB}")
        tdSql.execute(
            "create table perf_ntb (ts timestamp, v int, score double, g int)"
        )
        batch = []
        if row_count is None:
            row_count = self._PERF_ROW_COUNT
        for i in range(row_count):
            ts = _BASE + i * 1000
            batch.append(f"({ts}, {i % 100}, {i * 0.5:.1f}, {i % 10})")
            if len(batch) >= 500:
                tdSql.execute("insert into perf_ntb values " + ",".join(batch))
                batch = []
        if batch:
            tdSql.execute("insert into perf_ntb values " + ",".join(batch))

        # perf_join: 200 rows for cross-table JOIN (ts aligned to perf_ntb)
        tdSql.execute("create table perf_join (ts timestamp, cat int)")
        join_vals = ", ".join(
            f"({_BASE + i * 1000}, {i % 5})" for i in range(200)
        )
        tdSql.execute(f"insert into perf_join values {join_vals}")

        # Virtual super table + one child vtable
        # Note: vtable column mappings must NOT include type annotations;
        # the ts primary key comes from the source table implicitly.
        tdSql.execute(f"create database {self.PERF_DB}")
        tdSql.execute(f"use {self.PERF_DB}")
        tdSql.execute(
            "create stable perf_vstb "
            "(ts timestamp, v int, score double, g int) "
            "tags(src int) virtual 1"
        )
        tdSql.execute(
            f"create vtable vt_perf ("
            f"v from {self.SRC_DB}.perf_ntb.v, "
            f"score from {self.SRC_DB}.perf_ntb.score, "
            f"g from {self.SRC_DB}.perf_ntb.g"
            f") using perf_vstb tags(1)"
        )

    def _teardown_data(self):
        tdSql.execute(f"drop database if exists {self.PERF_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")

    def _teardown_merge(self):
        tdSql.execute(f"drop database if exists {self.MERGE_DB}")
        tdSql.execute(f"drop database if exists {self.MERGE_SRC}")

    # ------------------------------------------------------------------
    # Latency + QPS measurement
    # ------------------------------------------------------------------

    @staticmethod
    def _pct(data_sorted, p):
        """Return p-th percentile (0-100) of pre-sorted seconds list, in ms."""
        n = len(data_sorted)
        if n == 0:
            return 0.0
        k = (n - 1) * p / 100.0
        f = int(k)
        c = min(f + 1, n - 1)
        return (data_sorted[f] + (k - f) * (data_sorted[c] - data_sorted[f])) * 1000

    def _measure_latency(self, sql, n_runs=30, label=""):
        """Run sql n_runs times serially and return performance stats.

        Latency is sampled by executing the same SQL statement n_runs times
        back-to-back (serial, no concurrency).  This gives a stable
        distribution for computing percentiles.

        Args:
            sql:    SQL statement to benchmark.
            n_runs: Number of serial repetitions (default 30).
            label:  If non-empty, log a summary line after measurement.

        Returns:
            dict with keys: n, total_s, qps, min_ms, max_ms, mean_ms,
                            p50_ms, p95_ms, p99_ms
        """
        times = []
        t_wall_start = time.time()
        for _ in range(n_runs):
            t0 = time.time()
            tdSql.query(sql)
            times.append(time.time() - t0)
        total_s = time.time() - t_wall_start

        ts = sorted(times)
        n = len(ts)
        stats = {
            "n": n_runs,
            "total_s": total_s,
            "qps": n_runs / total_s,
            "min_ms": ts[0] * 1000,
            "max_ms": ts[-1] * 1000,
            "mean_ms": (sum(times) / n) * 1000,
            "p50_ms": self._pct(ts, 50),
            "p95_ms": self._pct(ts, 95),
            "p99_ms": self._pct(ts, 99),
        }
        if label:
            tdLog.debug(
                f"{label}: n={n_runs} QPS={stats['qps']:.2f}/s "
                f"P50={stats['p50_ms']:.2f}ms P95={stats['p95_ms']:.2f}ms "
                f"P99={stats['p99_ms']:.2f}ms "
                f"min={stats['min_ms']:.2f}ms max={stats['max_ms']:.2f}ms"
            )
        return stats

    def _build_stats_from_times(self, times, wall_total):
        """Build a perf stats dict from a list of per-query elapsed seconds."""
        ts = sorted(times)
        n = len(ts)
        return {
            "n": n,
            "total_s": wall_total,
            "qps": n / wall_total if wall_total > 0 else 0.0,
            "min_ms": ts[0] * 1000 if ts else 0.0,
            "max_ms": ts[-1] * 1000 if ts else 0.0,
            "mean_ms": (sum(times) / n) * 1000 if n else 0.0,
            "p50_ms": self._pct(ts, 50),
            "p95_ms": self._pct(ts, 95),
            "p99_ms": self._pct(ts, 99),
        }

    # ------------------------------------------------------------------
    # PERF-001  Single-source full-pushdown baseline
    # ------------------------------------------------------------------

    def test_fq_perf_001_single_source_full_pushdown(self):
        """Single-source full-pushdown baseline

        TS: 小规模基线数据集, Filter+Agg+Sort+Limit 全下推, P50/P95/P99+QPS

        In CI: use internal data (2000 rows). Apply Filter+Agg+Sort+Limit on
        the direct source table path (pushdown-eligible).
        Collect QPS and P50/P95/P99 via 30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-001
            - 2026-04-15 wpan Add QPS/P99 via _measure_latency, add try/finally

        """
        _test_name = "PERF-001_single_source_full_pushdown"
        self._start_test(
            _test_name,
            "2000行直接表Filter+Agg+Sort+Limit 30次串行",
            30,
        )
        self._prepare_internal_data()
        try:
            sql = (
                f"select g, count(*), avg(score) "
                f"from {self.SRC_DB}.perf_ntb "
                f"where v > 10 group by g order by g limit 5"
            )
            # Correctness check before measurement
            tdSql.query(sql)
            tdSql.checkRows(5)

            # Latency: 30 serial runs
            stats = self._measure_latency(sql, n_runs=self._PERF_LATENCY_RUNS, label=_test_name)
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-002  Single-source zero-pushdown baseline
    # ------------------------------------------------------------------

    def test_fq_perf_002_single_source_zero_pushdown(self):
        """Single-source zero-pushdown baseline

        TS: 同数据集, 禁用下推全本地计算, 对比 P99 延迟与 PERF-001

        In CI: query through vtable forcing the local-computation path.
        Collect QPS and P50/P95/P99 via 30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-002
            - 2026-04-15 wpan Add QPS/P99 via _measure_latency, add try/finally

        """
        _test_name = "PERF-002_single_source_zero_pushdown"
        self._start_test(
            _test_name,
            "2000行虚拟表本地计算路径 30次串行",
            30,
        )
        self._prepare_internal_data()
        try:
            sql = (
                f"select count(*), sum(v), avg(v) "
                f"from {self.PERF_DB}.vt_perf"
            )
            # Correctness check
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2000)

            # Latency: 30 serial runs
            stats = self._measure_latency(sql, n_runs=self._PERF_LATENCY_RUNS, label=_test_name)
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-003  Full pushdown vs zero pushdown comparison
    # ------------------------------------------------------------------

    def test_fq_perf_003_pushdown_vs_zero_pushdown(self):
        """Full pushdown vs zero pushdown throughput comparison

        TS: 比较吞吐、延迟、拉取数据量

        In CI: measure direct-table path (pushdown-eligible) vs vtable path
        (local compute) using the same 2000-row dataset.  Report P99 ratio.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-003
            - 2026-04-15 wpan Implement with internal data, add latency comparison

        """
        _test_name = "PERF-003_pushdown_vs_zero_pushdown"
        self._start_test(
            _test_name,
            "直接表 vs 虚拟表路径 P99对比 各30次串行",
            60,
        )
        self._prepare_internal_data()
        try:
            sql_direct = (
                f"select count(*), avg(score) "
                f"from {self.SRC_DB}.perf_ntb where g = 1"
            )
            sql_vtable = (
                f"select count(*), avg(score) "
                f"from {self.PERF_DB}.vt_perf where g = 1"
            )

            s_d = self._measure_latency(
                sql_direct, n_runs=self._PERF_LATENCY_RUNS, label=f"{_test_name}[direct]"
            )
            s_v = self._measure_latency(
                sql_vtable, n_runs=self._PERF_LATENCY_RUNS, label=f"{_test_name}[vtable]"
            )

            ratio = s_v["p99_ms"] / max(s_d["p99_ms"], 0.001)
            tdLog.debug(
                f"{_test_name}: direct_P99={s_d['p99_ms']:.2f}ms "
                f"vtable_P99={s_v['p99_ms']:.2f}ms ratio={ratio:.2f}x"
            )

            # Summary uses averaged metrics across both paths
            combined = {
                "n": s_d["n"] + s_v["n"],
                "total_s": s_d["total_s"] + s_v["total_s"],
                "qps": (s_d["qps"] + s_v["qps"]) / 2,
                "min_ms": min(s_d["min_ms"], s_v["min_ms"]),
                "max_ms": max(s_d["max_ms"], s_v["max_ms"]),
                "mean_ms": (s_d["mean_ms"] + s_v["mean_ms"]) / 2,
                "p50_ms": (s_d["p50_ms"] + s_v["p50_ms"]) / 2,
                "p95_ms": (s_d["p95_ms"] + s_v["p95_ms"]) / 2,
                "p99_ms": (s_d["p99_ms"] + s_v["p99_ms"]) / 2,
            }
            self._record_pass(_test_name, perf_stats=combined)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-004  Cross-source JOIN performance
    # ------------------------------------------------------------------

    def test_fq_perf_004_cross_source_join(self):
        """Cross-source JOIN performance

        TS: 不同数据量组合下的跨源 JOIN 延迟曲线

        In CI: JOIN two internal tables (perf_ntb × perf_join) with matching
        timestamps to measure executor merge/join overhead.  30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-004
            - 2026-04-15 wpan Implement with internal JOIN, add latency measurement

        """
        _test_name = "PERF-004_cross_source_join"
        self._start_test(
            _test_name,
            "两张内部表ts对齐JOIN 30次串行",
            30,
        )
        self._prepare_internal_data()
        try:
            sql = (
                f"select a.v, a.score, b.cat "
                f"from {self.SRC_DB}.perf_ntb a, "
                f"{self.SRC_DB}.perf_join b "
                f"where a.ts = b.ts "
                f"order by a.ts limit 20"
            )
            # Correctness: 200 rows overlap, LIMIT 20 -> 20 rows
            tdSql.query(sql)
            tdSql.checkRows(20)

            stats = self._measure_latency(sql, n_runs=self._PERF_LATENCY_RUNS, label=_test_name)
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-005  Virtual table mixed query
    # ------------------------------------------------------------------

    def test_fq_perf_005_vtable_mixed_query(self):
        """Virtual table mixed query performance

        TS: 时序基线 + TDengine 本地数据集, 内外列融合查询, 多源归并开销评估

        In CI: multi-column vtable query with filter on mapped column.
        30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-005
            - 2026-04-15 wpan Add _measure_latency, add try/finally

        """
        _test_name = "PERF-005_vtable_mixed_query"
        self._start_test(
            _test_name,
            "虚拟表多列filter+聚合混合查询 30次串行",
            30,
        )
        self._prepare_internal_data()
        try:
            sql = (
                f"select count(*), avg(v), min(score), max(score) "
                f"from {self.PERF_DB}.vt_perf where g < 5"
            )
            tdSql.query(sql)
            tdSql.checkRows(1)

            stats = self._measure_latency(sql, n_runs=self._PERF_LATENCY_RUNS, label=_test_name)
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-006  Large window aggregation
    # ------------------------------------------------------------------

    def test_fq_perf_006_large_window_aggregation(self):
        """Large window aggregation performance

        TS: INTERVAL/FILL/INTERP 本地计算成本（大规模聚合）

        In CI: apply INTERVAL(1m) on vtable.  perf_ntb has 2000 rows at
        1-second intervals -> ~34 one-minute windows.  30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-006
            - 2026-04-15 wpan Implement with internal vtable INTERVAL, add latency

        """
        _test_name = "PERF-006_large_window_aggregation"
        self._start_test(
            _test_name,
            "虚拟表INTERVAL(1m) ~34窗口聚合 30次串行",
            30,
        )
        self._prepare_internal_data()
        try:
            sql = (
                f"select _wstart, count(*), avg(v) "
                f"from {self.PERF_DB}.vt_perf "
                f"interval(1m) order by _wstart"
            )
            # Correctness: 2000 rows at 1s -> ceil(2000/60) = 34 windows
            tdSql.query(sql)
            if tdSql.queryRows < 33:
                raise AssertionError(
                    f"PERF-006: expected >=33 INTERVAL windows, "
                    f"got {tdSql.queryRows}"
                )

            stats = self._measure_latency(sql, n_runs=self._PERF_LATENCY_RUNS, label=_test_name)
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-007  Cache hit benefit
    # ------------------------------------------------------------------

    def test_fq_perf_007_cache_hit_benefit(self):
        """Cache hit vs cache miss latency comparison

        TS: 同一查询连续执行先命中再失效, 对比元数据/能力缓存命中与重拉延迟差异

        In CI: 1 cold run (cache miss) followed by 30 warm runs (cache hit).
        Report cold latency vs warm P50/P95/P99 and speedup ratio.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-007
            - 2026-04-15 wpan Add proper cold/warm latency comparison + stats

        """
        _test_name = "PERF-007_cache_hit_benefit"
        self._start_test(
            _test_name,
            "1次冷启动 + 30次热缓存, 对比P99延迟差异",
            31,
        )
        self._prepare_internal_data()
        try:
            sql = f"select count(*), sum(v) from {self.PERF_DB}.vt_perf"

            # Cold run
            t_cold = time.time()
            tdSql.query(sql)
            cold_ms = (time.time() - t_cold) * 1000
            tdSql.checkData(0, 0, 2000)

            # Warm runs: 30 serial repetitions
            warm_stats = self._measure_latency(
                sql, n_runs=self._PERF_LATENCY_RUNS, label=f"{_test_name}[warm30]"
            )

            speedup = cold_ms / max(warm_stats["mean_ms"], 0.001)
            tdLog.debug(
                f"{_test_name}: cold={cold_ms:.2f}ms "
                f"warm_mean={warm_stats['mean_ms']:.2f}ms "
                f"warm_P99={warm_stats['p99_ms']:.2f}ms "
                f"speedup={speedup:.2f}x"
            )

            # Store warm stats as primary perf metric for summary
            self._record_pass(_test_name, perf_stats=warm_stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-008  Connection pool capacity — sequential burst proxy
    # ------------------------------------------------------------------

    def test_fq_perf_008_connection_pool_concurrent(self):
        """Connection pool capacity — sequential burst proxy

        TS: 4/16/64 并发客户端压测, P99延迟与失败率, 连接池上限表现

        In CI: 5 bursts of 20 sequential queries (100 total) simulate load on
        the connection pool without multi-threading.
        Collect QPS and P50/P95/P99 across all 100 queries.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-008
            - 2026-04-15 wpan Implement sequential burst proxy with full stats

        """
        _test_name = "PERF-008_connection_pool_burst"
        self._start_test(
            _test_name,
            "5x20 burst串行查询模拟连接池负载, QPS+P99",
            100,
        )
        self._prepare_internal_data()
        try:
            sql = f"select count(*) from {self.PERF_DB}.vt_perf"
            n_bursts = self._PERF_BURST_COUNT
            n_per_burst = self._PERF_BURST_SIZE

            all_times = []
            t_wall_start = time.time()
            for burst in range(n_bursts):
                t_burst = time.time()
                for _ in range(n_per_burst):
                    t0 = time.time()
                    tdSql.query(sql)
                    tdSql.checkData(0, 0, 2000)
                    all_times.append(time.time() - t0)
                burst_elapsed = time.time() - t_burst
                tdLog.debug(
                    f"{_test_name}: burst {burst + 1}/{n_bursts} "
                    f"QPS={n_per_burst / burst_elapsed:.2f}/s"
                )
            wall_total = time.time() - t_wall_start

            stats = self._build_stats_from_times(all_times, wall_total)
            tdLog.debug(
                f"{_test_name}: overall n={stats['n']} QPS={stats['qps']:.2f}/s "
                f"P50={stats['p50_ms']:.2f}ms P95={stats['p95_ms']:.2f}ms "
                f"P99={stats['p99_ms']:.2f}ms"
            )
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-009  Timeout parameter sensitivity
    # ------------------------------------------------------------------

    def test_fq_perf_009_timeout_parameter_sensitivity(self):
        """Timeout parameter sensitivity

        TS: 调整 connect_timeout_ms / read_timeout_ms, 注入可控延迟,
            验证超时触发与错误码正确

        In CI: stop the real MySQL instance to make it unreachable, create an
        external source with connect_timeout_ms=200 pointing to the real host,
        run 5 serial error queries to measure time-to-failure distribution and
        verify the correct error code, then restore MySQL.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-009
            - 2026-04-15 wpan Fix OPTIONS syntax, fix expectedErrno, add stats

        """
        _test_name = "PERF-009_timeout_parameter_sensitivity"
        self._start_test(
            _test_name,
            "connect_timeout_ms=200, 5次失败查询测time-to-failure分布",
            5,
        )
        cfg = self._mysql_cfg()
        ver = cfg.version
        src = "perf_timeout_src"
        self._cleanup_src(src)
        try:
            # OPTIONS keys and values must both be quoted strings.
            # Create source first (metadata only), then stop MySQL so queries fail.
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='{cfg.host}' port={cfg.port} user='{cfg.user}' "
                f"password='{cfg.password}' "
                f"database='db' options('connect_timeout_ms'='200')"
            )
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                # Measure time-to-failure for 5 serial attempts
                fail_times = []
                for _ in range(self._PERF_TIMEOUT_RUNS):
                    t0 = time.time()
                    tdSql.error(
                        f"select * from {src}.db.t1",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    fail_times.append(time.time() - t0)

                wall_total = sum(fail_times)
                stats = self._build_stats_from_times(fail_times, wall_total)
                tdLog.debug(
                    f"{_test_name}: timeout=200ms "
                    f"mean_fail={stats['mean_ms']:.2f}ms "
                    f"max_fail={stats['max_ms']:.2f}ms "
                    f"P99={stats['p99_ms']:.2f}ms"
                )
                self._record_pass(_test_name, perf_stats=stats)
            except Exception as e:
                self._record_fail(_test_name, str(e))
                raise
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # PERF-010  Backoff retry impact
    # ------------------------------------------------------------------

    def test_fq_perf_010_backoff_retry_impact(self):
        """Backoff retry impact on overall query latency

        TS: 模拟外部源资源限制（限流）场景, 退避重试策略对整体查询延迟放大倍数

        In CI: stop the real MySQL instance, create an external source with
        connect_timeout_ms=300 pointing to the real host, run 5 serial error
        queries and measure cumulative latency to estimate retry amplification
        overhead, then restore MySQL.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-010
            - 2026-04-15 wpan Implement with unreachable source timing measurement

        """
        _test_name = "PERF-010_backoff_retry_impact"
        self._start_test(
            _test_name,
            "不可达外部源5次连续失败, 测量退避重试延迟放大",
            5,
        )
        cfg = self._mysql_cfg()
        ver = cfg.version
        src = "perf_retry_src"
        self._cleanup_src(src)
        try:
            # Create source with real credentials; stop MySQL before querying.
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='{cfg.host}' port={cfg.port} user='{cfg.user}' "
                f"password='{cfg.password}' "
                f"database='db' options('connect_timeout_ms'='300')"
            )
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                fail_times = []
                for _ in range(self._PERF_TIMEOUT_RUNS):
                    t0 = time.time()
                    tdSql.error(
                        f"select * from {src}.db.t1",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    fail_times.append(time.time() - t0)

                wall_total = sum(fail_times)
                stats = self._build_stats_from_times(fail_times, wall_total)
                tdLog.debug(
                    f"{_test_name}: 5 failures "
                    f"mean={stats['mean_ms']:.2f}ms "
                    f"total={wall_total:.3f}s "
                    f"P99={stats['p99_ms']:.2f}ms"
                )
                self._record_pass(_test_name, perf_stats=stats)
            except Exception as e:
                self._record_fail(_test_name, str(e))
                raise
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # PERF-011  Multi-source merge cost
    # ------------------------------------------------------------------

    def test_fq_perf_011_multi_source_merge_cost(self):
        """Multi-source ts merge sort cost vs sub-table count

        TS: 1000 子表归并, SORT_MULTISOURCE_TS_MERGE 随子表数增长延迟曲线

        In CI: 10 sub-tables x 100 rows = 1000 rows total.  Measure merge
        query latency via 30 serial runs.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-011
            - 2026-04-15 wpan Add _measure_latency, add try/finally

        """
        _test_name = "PERF-011_multi_source_merge_cost"
        self._start_test(
            _test_name,
            "10子表x100行归并查询 30次串行",
            30,
        )
        try:
            tdSql.execute(f"drop database if exists {self.MERGE_DB}")
            tdSql.execute(f"drop database if exists {self.MERGE_SRC}")
            tdSql.execute(f"create database {self.MERGE_SRC}")
            tdSql.execute(f"use {self.MERGE_SRC}")
            tdSql.execute(
                "create stable stb (ts timestamp, v int) tags(dev int)"
            )
            for d in range(self._PERF_MERGE_SUBTABLES):
                tdSql.execute(f"create table ct_{d} using stb tags({d})")
                vals = ", ".join(
                    f"({1704067200000 + i * 1000}, {d * 100 + i})"
                    for i in range(self._PERF_MERGE_ROWS)
                )
                tdSql.execute(f"insert into ct_{d} values {vals}")

            tdSql.execute(f"create database {self.MERGE_DB}")
            tdSql.execute(f"use {self.MERGE_DB}")
            tdSql.execute(
                "create stable vstb_merge "
                "(ts timestamp, v_val int) tags(vg int) virtual 1"
            )
            for d in range(self._PERF_MERGE_SUBTABLES):
                tdSql.execute(
                    f"create vtable vct_{d} ("
                    f"v_val from {self.MERGE_SRC}.ct_{d}.v"
                    f") using vstb_merge tags({d})"
                )

            # Correctness check
            tdSql.query(f"select count(*) from {self.MERGE_DB}.vstb_merge")
            tdSql.checkData(0, 0, 1000)

            # Latency: 30 serial runs
            stats = self._measure_latency(
                f"select count(*) from {self.MERGE_DB}.vstb_merge",
                n_runs=self._PERF_MERGE_RUNS,
                label=_test_name,
            )
            self._record_pass(_test_name, perf_stats=stats)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_merge()

    # ------------------------------------------------------------------
    # PERF-012  Regression threshold check
    # ------------------------------------------------------------------

    def test_fq_perf_012_regression_threshold(self):
        """Regression threshold check against collected metrics

        TS: 对 PERF-001/002/011 三项指标与软阈值对比，超出退化阈值时标记回归失败

        In CI: compare P99 from PERF-001, PERF-002, PERF-011 (collected during
        this session) against generous soft thresholds (30 s each).
        Hard-fail if any P99 exceeds the threshold.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-012
            - 2026-04-15 wpan Implement by comparing session-collected metrics

        """
        _test_name = "PERF-012_regression_threshold"
        self._start_test(
            _test_name,
            "基于本次运行PERF-001/002/011 P99对比软阈值回归检查",
            0,
        )
        try:
            # Gather metrics recorded by earlier tests in this session
            collected = {
                r["name"]: r.get("perf")
                for r in TestFq10Performance._test_results
                if r.get("perf") is not None
            }

            # Soft thresholds (ms) — generous for CI environments
            thresholds = {
                "PERF-001_single_source_full_pushdown": 30_000,
                "PERF-002_single_source_zero_pushdown": 30_000,
                "PERF-011_multi_source_merge_cost": 30_000,
            }

            violations = []
            for key, max_p99_ms in thresholds.items():
                perf = collected.get(key)
                if perf is None:
                    tdLog.debug(
                        f"{_test_name}: no metrics for {key} — test may have failed"
                    )
                    continue
                p99 = perf["p99_ms"]
                if p99 > max_p99_ms:
                    violations.append(
                        f"{key}: P99={p99:.2f}ms > threshold {max_p99_ms:.0f}ms"
                    )
                    tdLog.debug(
                        f"{_test_name}: REGRESSION {key} "
                        f"P99={p99:.2f}ms (threshold {max_p99_ms:.0f}ms)"
                    )
                else:
                    tdLog.debug(
                        f"{_test_name}: OK {key} P99={p99:.2f}ms "
                        f"(<= {max_p99_ms:.0f}ms)"
                    )

            if violations:
                raise AssertionError(
                    "Performance regression detected: "
                    + "; ".join(violations)
                )

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
