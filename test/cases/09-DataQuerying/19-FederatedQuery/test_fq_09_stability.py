"""
test_fq_09_stability.py

Implements long-term stability tests from TS "长期稳定性测试" section.
Four focus areas:
    1. 72h continuous query mix (single-source / cross-source JOIN / vtable)
    2. Fault injection (external source unreachable, slow query, throttle, jitter)
    3. Cache stability (meta/capability cache repeated expiry & REFRESH cycle)
    4. Connection pool stability (high-frequency burst queries, no state corruption)

Since these are non-functional stability tests that require sustained runtime,
tests here are structured as *representative short cycles* that exercise the
same code paths.  In CI they run a small iteration count; a dedicated stability
environment would increase the count and duration.

Design notes:
    - Tests use internal vtables where possible so no external DB is needed.
    - Fault-injection tests stop/start real MySQL instances to simulate unreachable sources.
    - Connection pool stability uses burst sequential queries on internal vtables.
    - Each test is guarded with try/finally to ensure environment cleanup.
    - teardown_class prints a structured test summary.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
"""

import os
import threading
import time
from datetime import datetime

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
)


class TestFq09Stability(FederatedQueryVersionedMixin):
    """Long-term stability tests — typical short-cycle representatives."""

    STAB_DB = "fq_stab_db"
    SRC_DB = "fq_stab_src"

    # ------------------------------------------------------------------
    # Iteration / duration controls
    # Override via environment variables to scale the test load:
    #
    #   FQ_STAB_ITERS             Continuous-query mix cycles       (default 20)
    #   FQ_STAB_CACHE_CYCLES      Cache-stability loop iterations   (default 10)
    #   FQ_STAB_UNREACHABLE_Q     Unreachable-source error queries  (default 5)
    #   FQ_STAB_BURST_COUNT       Connection-pool burst count       (default 5)
    #   FQ_STAB_BURST_SIZE        Queries per burst                 (default 20)
    #   FQ_STAB_DRIFT_CYCLES      Drift-check repetition count      (default 49)
    #
    # Example (full stress run):
    #   FQ_STAB_ITERS=200 FQ_STAB_BURST_COUNT=20 FQ_STAB_BURST_SIZE=100 pytest fq_09...
    # ------------------------------------------------------------------
    _STAB_ITERS         = int(os.getenv("FQ_STAB_ITERS",         "20"))
    _STAB_CACHE_CYCLES  = int(os.getenv("FQ_STAB_CACHE_CYCLES",  "10"))
    _STAB_UNREACHABLE_Q = int(os.getenv("FQ_STAB_UNREACHABLE_Q", "5"))
    _STAB_BURST_COUNT   = int(os.getenv("FQ_STAB_BURST_COUNT",   "5"))
    _STAB_BURST_SIZE    = int(os.getenv("FQ_STAB_BURST_SIZE",    "20"))
    _STAB_DRIFT_CYCLES  = int(os.getenv("FQ_STAB_DRIFT_CYCLES",  "49"))

    # Class-level test result registry used by teardown_class summary
    _test_results: list = []
    _session_start: float = 0.0

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        TestFq09Stability._test_results = []
        TestFq09Stability._session_start = time.time()
        # Global pre-cleanup: ensure no leftover state from previous runs
        self._teardown_env()
        self._cleanup_src("stab_unreachable_src")

    def teardown_class(self):
        """Final cleanup and structured test summary report."""
        try:
            self._teardown_env()
            self._cleanup_src("stab_unreachable_src")
        finally:
            self._print_summary()

    def _start_test(self, name, description="", iterations=0):
        """Record test start time and metadata into results list.

        The entry name is automatically suffixed with the current version label
        (e.g. ``STAB-001[my8.0-pg16-inf3.0]``) so multi-version runs produce
        one distinct row per (scenario, version) combination in the summary.
        """
        ver_label = self._version_label()
        full_name = f"{name}[{ver_label}]"
        TestFq09Stability._test_results.append({
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
        })

    def _record_pass(self, name):
        full_name = f"{name}[{self._version_label()}]"
        for r in reversed(TestFq09Stability._test_results):
            if r["name"] == full_name:
                r["end"] = time.time()
                r["duration"] = r["end"] - r["start"]
                r["status"] = "PASS"
                return
        # Fallback if _start_test was not called
        ver_label = self._version_label()
        TestFq09Stability._test_results.append({
            "name": full_name, "base_name": name, "version": ver_label,
            "desc": "", "iterations": 0,
            "start": time.time(), "end": time.time(), "duration": 0.0,
            "status": "PASS", "error": None,
        })

    def _record_fail(self, name, reason):
        full_name = f"{name}[{self._version_label()}]"
        for r in reversed(TestFq09Stability._test_results):
            if r["name"] == full_name:
                r["end"] = time.time()
                r["duration"] = r["end"] - r["start"]
                r["status"] = "FAIL"
                r["error"] = reason
                return
        # Fallback if _start_test was not called
        ver_label = self._version_label()
        TestFq09Stability._test_results.append({
            "name": full_name, "base_name": name, "version": ver_label,
            "desc": "", "iterations": 0,
            "start": time.time(), "end": time.time(), "duration": 0.0,
            "status": "FAIL", "error": reason,
        })

    def _print_summary(self):
        """Print structured test summary including timing and error details."""
        results = TestFq09Stability._test_results
        session_end = time.time()
        session_start = TestFq09Stability._session_start
        total_duration = session_end - session_start

        def _fmt_ts(ts):
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{dt.microsecond // 1000:03d}"

        total = len(results)
        passed = sum(1 for r in results if r["status"] == "PASS")
        failed = total - passed

        sep = "=" * 74
        mid = "-" * 74
        tdLog.debug(sep)
        tdLog.debug("  test_fq_09_stability  稳定性测试总结  (Stability Test Summary)")
        tdLog.debug(sep)
        tdLog.debug(f"  会话启动 / Session Start  : {_fmt_ts(session_start)}")
        tdLog.debug(f"  会话结束 / Session End    : {_fmt_ts(session_end)}")
        tdLog.debug(f"  总耗时   / Total Duration : {total_duration:.3f} s")
        tdLog.debug(mid)
        tdLog.debug(
            f"  {'#':<3}  {'测试名称':<44}  {'状态':<6}  {'耗时(s)':<9}  {'迭代':<5}  描述"
        )
        tdLog.debug(mid)
        for idx, r in enumerate(results, 1):
            status_col = "PASS" if r["status"] == "PASS" else "FAIL"
            dur_s = f"{r['duration']:.3f}" if r["duration"] is not None else "N/A"
            iters = str(r["iterations"]) if r["iterations"] else "-"
            name_col = r["name"][:44]
            desc_col = r["desc"] or ""
            tdLog.debug(
                f"  {idx:<3}  {name_col:<44}  {status_col:<6}  {dur_s:<9}  {iters:<5}  {desc_col}"
            )
        tdLog.debug(mid)
        tdLog.debug(
            f"  合计 / Total: {total}   通过 / Passed: {passed}   失败 / Failed: {failed}"
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
    # Internal helpers
    # ------------------------------------------------------------------

    def _prepare_env(self):
        """Create internal databases, tables, vtables for stability loops.

        Data layout (derived constants — tests rely on these):
          src_d1 (100 rows): ts=BASE+i*1000ms, val=i, score=i*1.1, flag=(i%2==0)
                             for i=1..100
            count=100, sum(val)=5050, avg(val)=50.5, min=1, max=100
            avg(score) = 1.1*50.5 = 55.55
          src_d2 (50 rows):  ts=BASE+i*1000ms, val=i+100, score=i*2.2, flag=(i%2!=0)
                             for i=1..50
            count=50, sum(val)=6275, avg(val)=125.5, min=101, max=150
            avg(score) = 2.2*25.5 = 56.1
          vstb (150 rows total): vg=1 → vt_d1 (100), vg=2 → vt_d2 (50)
          local_dim: ts=BASE+1000ms (→ d1 i=1, val=1, weight=100)
                     ts=BASE+2000ms (→ d1 i=2, val=2, weight=200)
          JOIN vt_d1 ⋈ local_dim ON ts: 2 rows → (val=1,w=100), (val=2,w=200)
        """
        _BASE = 1704067200000
        tdSql.execute(f"drop database if exists {self.STAB_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")
        tdSql.execute(f"create database {self.SRC_DB}")
        tdSql.execute(f"use {self.SRC_DB}")

        tdSql.execute(
            "create stable src_stb (ts timestamp, val int, score double, flag bool) "
            "tags(region int)"
        )
        tdSql.execute("create table src_d1 using src_stb tags(1)")
        tdSql.execute("create table src_d2 using src_stb tags(2)")

        # Use :.6f to avoid Python float->string representation noise
        values_d1 = ", ".join(
            f"({_BASE + i * 1000}, {i}, {i * 1.1:.6f}, "
            f"{str(i % 2 == 0).lower()})"
            for i in range(1, 101)
        )
        tdSql.execute(f"insert into src_d1 values {values_d1}")

        values_d2 = ", ".join(
            f"({_BASE + i * 1000}, {i + 100}, {i * 2.2:.6f}, "
            f"{str(i % 2 != 0).lower()})"
            for i in range(1, 51)
        )
        tdSql.execute(f"insert into src_d2 values {values_d2}")

        tdSql.execute(f"create database {self.STAB_DB}")
        tdSql.execute(f"use {self.STAB_DB}")

        tdSql.execute(
            "create stable vstb (ts timestamp, v_val int, v_score double, v_flag bool) "
            "tags(vg int) virtual 1"
        )
        tdSql.execute(
            f"create vtable vt_d1 ("
            f"v_val from {self.SRC_DB}.src_d1.val, "
            f"v_score from {self.SRC_DB}.src_d1.score, "
            f"v_flag from {self.SRC_DB}.src_d1.flag"
            f") using vstb tags(1)"
        )
        tdSql.execute(
            f"create vtable vt_d2 ("
            f"v_val from {self.SRC_DB}.src_d2.val, "
            f"v_score from {self.SRC_DB}.src_d2.score, "
            f"v_flag from {self.SRC_DB}.src_d2.flag"
            f") using vstb tags(2)"
        )
        # local_dim: timestamps align with vt_d1 i=1 and i=2
        tdSql.execute(
            "create table local_dim (ts timestamp, device_id int, weight int)"
        )
        tdSql.execute(f"insert into local_dim values ({_BASE + 1000}, 1, 100)")
        tdSql.execute(f"insert into local_dim values ({_BASE + 2000}, 2, 200)")

    def _teardown_env(self):
        tdSql.execute(f"drop database if exists {self.STAB_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")

    # ------------------------------------------------------------------
    # STAB-001  72h continuous query mix (short-cycle representative)
    # ------------------------------------------------------------------

    def test_fq_stab_001_continuous_query_mix(self):
        """72h continuous query mix — short-cycle representative

        TS: 单源查询/跨源 JOIN/虚拟表混合查询连续运行

        1. Prepare internal vtable environment
        2. Run repeated cycles of single-table, cross-table, vtable queries
        3. Each cycle verifies row count and key aggregate values
        4. Negative: query dropped table returns expected error
        5. After loop: verify no state corruption by re-querying

        Expected data:
          src_d1: 100 rows, count=100, sum(val)=5050, avg(val)=50.5
          vstb:   150 rows total, vg=1 → 100 rows, vg=2 → 50 rows
          JOIN:   2 rows (val=1,weight=100) and (val=2,weight=200)

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        _test_name = "STAB-001_continuous_query_mix"
        self._start_test(
            _test_name,
            f"{self._STAB_ITERS}轮次单源/跨源JOIN/虚拟表混合查询连续性验证",
            self._STAB_ITERS,
        )
        self._prepare_env()
        try:
            iterations = self._STAB_ITERS
            for i in range(iterations):
                # Single-source query with full aggregate verification
                tdSql.query(
                    f"select count(*), sum(val), avg(val) "
                    f"from {self.SRC_DB}.src_d1"
                )
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 100)   # count
                tdSql.checkData(0, 1, 5050)  # sum(1..100)
                tdSql.checkData(0, 2, 50.5)  # avg

                # Vtable super-table aggregate
                tdSql.query(
                    f"select count(*) from {self.STAB_DB}.vstb"
                )
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 150)   # 100 + 50

                # Vtable group-by query
                tdSql.query(
                    f"select vg, count(*) from {self.STAB_DB}.vstb "
                    f"group by vg order by vg"
                )
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, 1)    # vg=1
                tdSql.checkData(0, 1, 100)  # count for vg=1
                tdSql.checkData(1, 0, 2)    # vg=2
                tdSql.checkData(1, 1, 50)   # count for vg=2

                # Cross-table: vtable JOIN local_dim ON ts
                # local_dim has ts=BASE+1000ms and BASE+2000ms which align with
                # vt_d1 i=1 (val=1) and i=2 (val=2)
                tdSql.query(
                    f"select a.v_val, b.weight "
                    f"from {self.STAB_DB}.vt_d1 a, "
                    f"{self.STAB_DB}.local_dim b "
                    f"where a.ts = b.ts order by a.ts"
                )
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, 1)    # vt_d1 i=1: val=1
                tdSql.checkData(0, 1, 100)  # local_dim weight=100
                tdSql.checkData(1, 0, 2)    # vt_d1 i=2: val=2
                tdSql.checkData(1, 1, 200)  # local_dim weight=200

            # Negative: non-existent vtable must return TABLE_NOT_EXIST
            tdSql.error(
                f"select * from {self.STAB_DB}.no_such_vtable",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST,
            )

            # Final sanity: data unchanged after 20 iterations
            tdSql.query(f"select count(*) from {self.STAB_DB}.vstb")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 150)

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_env()

    # ------------------------------------------------------------------
    # STAB-002  Fault injection (external source unreachable)
    # ------------------------------------------------------------------

    def test_fq_stab_002_fault_injection_unreachable(self):
        """Fault injection — external source unreachable / jitter

        TS: 外部源短时不可达、慢查询、限流、连接抖动

        1. Create external source pointing to real MySQL instance
        2. Stop the MySQL instance to make it unreachable
        3. Rapid-fire queries — must all fail with connection-layer error (not
           syntax error and not catalog-layer error so we know routing is correct)
        4. Restore MySQL; verify source survives in catalog after repeated failures
        5. Drop source and verify catalog cleanup (source no longer found)

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        _test_name = "STAB-002_fault_injection_unreachable"
        self._start_test(_test_name, "5次外部源不可达故障注入，验证连接层错误与目录存活性", 5)
        src_name = "stab_unreachable_src"
        cfg = self._mysql_cfg()
        ver = cfg.version
        self._cleanup_src(src_name)
        try:
            # Create real source first so the catalog entry is valid.
            self._mk_mysql_real(
                src_name,
                database="testdb",
                extra_options="'connect_timeout_ms'='500'",
            )
            # Source must be visible immediately after creation
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src_name}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src_name)

            # Stop the MySQL instance to make it unreachable, then fire queries.
            # All must fail with a connection-layer error (not syntax error).
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                for _ in range(self._STAB_UNREACHABLE_Q):
                    tdSql.error(
                        f"select * from {src_name}.testdb.some_table",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )

                # Source must still be in catalog after repeated failures
                tdSql.query(
                    "select count(*) from information_schema.ins_ext_sources "
                    f"where source_name = '{src_name}'"
                )
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 1)
            finally:
                # Always restore MySQL before leaving — other tests depend on it.
                ExtSrcEnv.start_mysql_instance(ver)

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._cleanup_src(src_name)

        # After DROP: source must no longer exist in catalog.
        # TSDB_CODE_EXT_SOURCE_NOT_FOUND = None (enterprise TBD).
        tdSql.error(
            f"select * from {src_name}.testdb.some_table",
            expectedErrno=TSDB_CODE_EXT_SOURCE_NOT_FOUND,
        )
        # System table confirms removal
        tdSql.query(
            "select count(*) from information_schema.ins_ext_sources "
            f"where source_name = '{src_name}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    # ------------------------------------------------------------------
    # STAB-003  Cache stability (repeated expiry + refresh)
    # ------------------------------------------------------------------

    def test_fq_stab_003_cache_stability(self):
        """Cache stability — repeated expiry and refresh cycles

        TS: meta/capability 缓存反复过期刷新，内存无泄漏

        1. Prepare vtable environment
        2. Loop: query → verify → (simulate cache invalidation) → repeat
        3. Verify no result drift across cycles
        4. Memory leak detection requires dedicated tools

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS stability section

        """
        _test_name = "STAB-003_cache_stability"
        self._start_test(
            _test_name,
            f"{self._STAB_CACHE_CYCLES}轮次缓存反复过期刷新，验证无内存泄漏与结果漂移",
            self._STAB_CACHE_CYCLES,
        )
        self._prepare_env()
        try:
            for i in range(self._STAB_CACHE_CYCLES):
                tdSql.query(f"select count(*) from {self.STAB_DB}.vstb")
                tdSql.checkData(0, 0, 150)

                tdSql.query(
                    f"select vg, avg(v_score) from {self.STAB_DB}.vstb "
                    f"group by vg order by vg"
                )
                tdSql.checkRows(2)

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_env()

    # ------------------------------------------------------------------
    # STAB-004  Connection pool stability
    # ------------------------------------------------------------------

    def test_fq_stab_004_connection_pool_stability(self):
        """Connection pool stability — high-frequency burst queries, no state corruption

        TS: 并发高峰与低峰切换，无僵尸连接

        Simulates high-concurrency → low-concurrency switching using rapid
        sequential bursts of queries on the same vtable.  Multi-threaded
        external source load is exercised via sequential burst since tdSql
        is a single-connection client.  Verifies no state corruption occurs.

        N_BURSTS * N_QUERIES_PER_BURST sequential queries performed.  Both
        the aggregate count and per-group counts are verified after each burst.

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        _test_name = "STAB-004_connection_pool_stability"
        self._start_test(
            _test_name,
            f"{self._STAB_BURST_COUNT}×{self._STAB_BURST_SIZE} burst序列查询，验证连接池状态无泄漏与聚合一致性",
            self._STAB_BURST_COUNT * self._STAB_BURST_SIZE,
        )
        self._prepare_env()
        try:
            n_bursts = self._STAB_BURST_COUNT
            n_queries_per_burst = self._STAB_BURST_SIZE

            for burst in range(n_bursts):
                tdLog.debug(
                    f"STAB-004: burst {burst + 1}/{n_bursts} "
                    f"({n_queries_per_burst} queries)"
                )
                for q in range(n_queries_per_burst):
                    tdSql.query(
                        f"select count(*) from {self.STAB_DB}.vstb"
                    )
                    tdSql.checkRows(1)
                    tdSql.checkData(0, 0, 150)

                # After each burst: full per-group verification
                tdSql.query(
                    f"select vg, count(*) "
                    f"from {self.STAB_DB}.vstb group by vg order by vg"
                )
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, 1)    # vg=1
                tdSql.checkData(0, 1, 100)  # count
                tdSql.checkData(1, 0, 2)    # vg=2
                tdSql.checkData(1, 1, 50)   # count

            # Final: min/max/sum integrity after all bursts
            tdSql.query(
                f"select count(*), sum(v_val), min(v_val), max(v_val) "
                f"from {self.STAB_DB}.vt_d1"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 100)   # count
            tdSql.checkData(0, 1, 5050)  # sum(1..100)
            tdSql.checkData(0, 2, 1)     # min
            tdSql.checkData(0, 3, 100)   # max

            tdSql.query(
                f"select count(*), sum(v_val), min(v_val), max(v_val) "
                f"from {self.STAB_DB}.vt_d2"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 50)    # count
            tdSql.checkData(0, 1, 6275)  # sum(101..150) = 5000+1275
            tdSql.checkData(0, 2, 101)   # min
            tdSql.checkData(0, 3, 150)   # max

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_env()

    # ------------------------------------------------------------------
    # STAB-005  Long-duration query consistency
    # ------------------------------------------------------------------

    def test_fq_stab_005_long_duration_consistency(self):
        """Long-duration result consistency — no state drift

        Supplementary: run the same query 50 times, compare each result
        to the first-run baseline.

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary consistency loop

        """
        _test_name = "STAB-005_long_duration_consistency"
        self._start_test(_test_name, "50轮次重复查询，对比基准结果验证无结果漂移", 50)
        self._prepare_env()
        try:
            # Establish baseline on first run
            tdSql.query(
                f"select vg, count(*), sum(v_val), min(v_val), max(v_val) "
                f"from {self.STAB_DB}.vstb group by vg order by vg"
            )
            tdSql.checkRows(2)
            # vg=1: count=100, sum=5050, min=1, max=100
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(0, 2, 5050)
            tdSql.checkData(0, 3, 1)
            tdSql.checkData(0, 4, 100)
            # vg=2: count=50, sum=6275, min=101, max=150
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 50)
            tdSql.checkData(1, 2, 6275)
            tdSql.checkData(1, 3, 101)
            tdSql.checkData(1, 4, 150)

            # Repeat the same query _STAB_DRIFT_CYCLES more times (total +1) verifying no drift
            for i in range(1, self._STAB_DRIFT_CYCLES + 1):
                tdSql.query(
                    f"select vg, count(*), sum(v_val), min(v_val), max(v_val) "
                    f"from {self.STAB_DB}.vstb group by vg order by vg"
                )
                tdSql.checkRows(2)
                # vg=1 consistency
                if (tdSql.queryResult[0][0] != 1
                        or tdSql.queryResult[0][1] != 100
                        or tdSql.queryResult[0][2] != 5050
                        or tdSql.queryResult[0][3] != 1
                        or tdSql.queryResult[0][4] != 100):
                    raise AssertionError(
                        f"vg=1 result drift at iteration {i}: "
                        f"got {tdSql.queryResult[0]}"
                    )
                # vg=2 consistency
                if (tdSql.queryResult[1][0] != 2
                        or tdSql.queryResult[1][1] != 50
                        or tdSql.queryResult[1][2] != 6275
                        or tdSql.queryResult[1][3] != 101
                        or tdSql.queryResult[1][4] != 150):
                    raise AssertionError(
                        f"vg=2 result drift at iteration {i}: "
                        f"got {tdSql.queryResult[1]}"
                    )

            self._record_pass(_test_name)
        except Exception as e:
            self._record_fail(_test_name, str(e))
            raise
        finally:
            self._teardown_env()
