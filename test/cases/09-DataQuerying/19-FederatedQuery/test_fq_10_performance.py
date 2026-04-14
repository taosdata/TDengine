"""
test_fq_10_performance.py

Implements PERF-001 through PERF-012 from TS "性能测试" section.

Design notes:
    - Most performance tests require pre-loaded external databases (MySQL 8.0,
      PostgreSQL 14, InfluxDB v3) with large datasets.  These are guarded by
      pytest.skip() in CI.
    - Tests that CAN run with internal data provide a lightweight baseline.
    - Metrics collection (P50/P95/P99 latency, QPS, CPU/memory) requires
      external tooling (Prometheus/Grafana); tests here validate the query
      path executes and capture elapsed time where possible.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - For full coverage: MySQL 8.0 (100K+ rows), PostgreSQL 14+ (1M+ rows),
      InfluxDB v3, TDengine local dataset.
"""

import time
import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    TSDB_CODE_PAR_SYNTAX_ERROR,
)


class TestFq10Performance:
    """PERF-001 through PERF-012: Performance tests."""

    PERF_DB = "fq_perf_db"
    SRC_DB = "fq_perf_src"

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _prepare_internal_data(self, row_count=2000):
        """Create internal tables + vtables for lightweight perf baselines."""
        tdSql.execute(f"drop database if exists {self.PERF_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")
        tdSql.execute(f"create database {self.SRC_DB}")
        tdSql.execute(f"use {self.SRC_DB}")
        tdSql.execute(
            "create table perf_ntb (ts timestamp, v int, score double, g int)"
        )
        batch = []
        for i in range(row_count):
            ts = 1704067200000 + i * 1000
            batch.append(f"({ts}, {i % 100}, {i * 0.5}, {i % 10})")
            if len(batch) >= 500:
                tdSql.execute(
                    "insert into perf_ntb values " + ",".join(batch)
                )
                batch = []
        if batch:
            tdSql.execute("insert into perf_ntb values " + ",".join(batch))

        tdSql.execute(f"create database {self.PERF_DB}")
        tdSql.execute(f"use {self.PERF_DB}")
        tdSql.execute(
            "create vtable vt_perf ("
            "ts timestamp, "
            f"v int from {self.SRC_DB}.perf_ntb.v, "
            f"score double from {self.SRC_DB}.perf_ntb.score, "
            f"g int from {self.SRC_DB}.perf_ntb.g"
            ")"
        )

    def _teardown_data(self):
        tdSql.execute(f"drop database if exists {self.PERF_DB}")
        tdSql.execute(f"drop database if exists {self.SRC_DB}")

    def _skip_external(self, msg="requires pre-loaded external databases"):
        pytest.skip(f"Full performance test {msg}")

    # ------------------------------------------------------------------
    # PERF-001  Single-source full-pushdown baseline
    # ------------------------------------------------------------------

    def test_fq_perf_001_single_source_full_pushdown(self):
        """Single-source full-pushdown baseline

        TS: 小规模基线数据集, Filter+Agg+Sort+Limit 全下推, P50/P95/P99+QPS

        In CI: use internal data, measure elapsed time only.
        Full test: external MySQL 100万行 with pushdown metrics.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-001

        """
        self._prepare_internal_data(2000)

        begin = time.time()
        tdSql.query(
            f"select g, count(*), avg(score) from {self.SRC_DB}.perf_ntb "
            f"where v > 10 group by g order by g limit 5"
        )
        elapsed = time.time() - begin

        tdSql.checkRows(5)
        tdLog.debug(f"PERF-001 internal baseline: {elapsed:.3f}s")

        if elapsed > 30:
            tdLog.exit(f"PERF-001 too slow: {elapsed:.3f}s (threshold 30s)")

        self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-002  Single-source zero-pushdown baseline
    # ------------------------------------------------------------------

    def test_fq_perf_002_single_source_zero_pushdown(self):
        """Single-source zero-pushdown baseline

        TS: 同数据集, 禁用下推全本地计算, 对比 P99 延迟与传输字节数

        In CI: execute a query that cannot be pushed down (proprietary function)
        and compare timing with PERF-001.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-002

        """
        self._prepare_internal_data(2000)

        # Use vtable path which forces local computation
        begin = time.time()
        tdSql.query(
            f"select count(*), sum(v), avg(v) from {self.PERF_DB}.vt_perf"
        )
        elapsed = time.time() - begin

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2000)
        tdLog.debug(f"PERF-002 zero-pushdown baseline: {elapsed:.3f}s")

        self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-003  Full pushdown vs zero pushdown comparison
    # ------------------------------------------------------------------

    def test_fq_perf_003_pushdown_vs_zero_pushdown(self):
        """Full pushdown vs zero pushdown throughput comparison

        TS: 小规模与大规模聚合数据集上对比吞吐/延迟/拉取数据量

        Requires external MySQL + PG with large datasets.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-003

        """
        self._skip_external("PERF-003: requires MySQL+PG large datasets")

    # ------------------------------------------------------------------
    # PERF-004  Cross-source JOIN performance
    # ------------------------------------------------------------------

    def test_fq_perf_004_cross_source_join(self):
        """Cross-source JOIN performance

        TS: MySQL×PG 各 1~10 张表组合, 不同数据量下延迟曲线

        Requires MySQL + PG with JOIN combination dataset.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-004

        """
        self._skip_external("PERF-004: requires MySQL+PG JOIN combination dataset")

    # ------------------------------------------------------------------
    # PERF-005  Virtual table mixed query
    # ------------------------------------------------------------------

    def test_fq_perf_005_vtable_mixed_query(self):
        """Virtual table mixed query performance

        TS: 时序基线 + TDengine 本地数据集, 内外列融合查询, 多源归并开销评估

        In CI: lightweight vtable mixed query with internal data.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-005

        """
        self._prepare_internal_data(2000)

        begin = time.time()
        for _ in range(10):
            tdSql.query(
                f"select count(*), avg(v), min(score), max(score) "
                f"from {self.PERF_DB}.vt_perf where g < 5"
            )
            tdSql.checkRows(1)
        elapsed = time.time() - begin

        tdLog.debug(f"PERF-005 vtable mixed 10 iterations: {elapsed:.3f}s")

        self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-006  Large window aggregation
    # ------------------------------------------------------------------

    def test_fq_perf_006_large_window_aggregation(self):
        """Large window aggregation performance

        TS: 大规模聚合数据集, INTERVAL 1h + FILL(PREV) + INTERP 本地计算成本

        Requires PG with 大规模聚合 dataset (1000万行).

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-006

        """
        self._skip_external("PERF-006: requires PG with 10M-row dataset")

    # ------------------------------------------------------------------
    # PERF-007  Cache hit benefit
    # ------------------------------------------------------------------

    def test_fq_perf_007_cache_hit_benefit(self):
        """Cache hit vs cache miss latency comparison

        TS: 同一查询连续执行先命中再失效, 对比元数据/能力缓存命中与重拉延迟差异

        In CI: run same vtable query twice, compare timing (cold vs warm).

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-007

        """
        self._prepare_internal_data(2000)

        sql = f"select count(*), sum(v) from {self.PERF_DB}.vt_perf"

        # Cold run
        t0 = time.time()
        tdSql.query(sql)
        cold_elapsed = time.time() - t0
        tdSql.checkData(0, 0, 2000)

        # Warm runs
        warm_times = []
        for _ in range(5):
            t0 = time.time()
            tdSql.query(sql)
            tdSql.checkRows(1)
            warm_times.append(time.time() - t0)

        avg_warm = sum(warm_times) / len(warm_times)
        tdLog.debug(
            f"PERF-007 cold={cold_elapsed:.3f}s, avg_warm={avg_warm:.3f}s"
        )

        self._teardown_data()

    # ------------------------------------------------------------------
    # PERF-008  Connection pool concurrent capability
    # ------------------------------------------------------------------

    def test_fq_perf_008_connection_pool_concurrent(self):
        """Connection pool concurrent capability

        TS: 4/16/64 并发客户端压测, P99延迟与失败率, 连接池上限表现

        Requires multi-threaded client and external databases.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-008

        """
        self._skip_external("PERF-008: requires multi-threaded client + external DBs")

    # ------------------------------------------------------------------
    # PERF-009  Timeout parameter sensitivity
    # ------------------------------------------------------------------

    def test_fq_perf_009_timeout_parameter_sensitivity(self):
        """Timeout parameter sensitivity

        TS: 调整 connect_timeout_ms / read_timeout_ms, 注入可控延迟,
            验证超时触发与错误码正确

        In CI: create source with low timeout, query non-routable → fast error.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-009

        """
        src = "perf_timeout_src"
        tdSql.execute(f"drop external source if exists {src}")
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='192.0.2.1' port=3306 user='u' password='p' database='db' "
            f"options(connect_timeout_ms=200)"
        )

        t0 = time.time()
        tdSql.error(
            f"select * from {src}.db.t1",
            expectedErrno=None,
        )
        elapsed = time.time() - t0

        tdLog.debug(f"PERF-009 timeout elapsed: {elapsed:.3f}s")
        # With 200ms timeout, should fail relatively quickly
        # Allow generous margin for CI environments
        if elapsed > 60:
            tdLog.exit(
                f"PERF-009 timeout too slow: {elapsed:.3f}s, expected <60s with 200ms timeout"
            )

        tdSql.execute(f"drop external source {src}")

    # ------------------------------------------------------------------
    # PERF-010  Backoff retry impact
    # ------------------------------------------------------------------

    def test_fq_perf_010_backoff_retry_impact(self):
        """Backoff retry impact on overall latency

        TS: 模拟外部源资源限制（限流）场景, 退避重试策略对整体查询延迟放大倍数

        Requires controlled latency injection on external source.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-010

        """
        self._skip_external("PERF-010: requires latency injection on external source")

    # ------------------------------------------------------------------
    # PERF-011  Multi-source merge cost
    # ------------------------------------------------------------------

    def test_fq_perf_011_multi_source_merge_cost(self):
        """Multi-source ts merge sort cost vs sub-table count

        TS: 1000 子表归并, SORT_MULTISOURCE_TS_MERGE 随子表数增长延迟曲线

        In CI: test with small sub-table count to validate merge path works.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-011

        """
        db = "fq_perf_merge"
        src = "fq_perf_merge_src"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(f"use {src}")

        tdSql.execute(
            "create stable stb (ts timestamp, v int) tags(dev int)"
        )
        # Create 10 sub-tables with 100 rows each
        for d in range(10):
            tdSql.execute(f"create table ct_{d} using stb tags({d})")
            vals = ", ".join(
                f"({1704067200000 + i * 1000}, {d * 100 + i})"
                for i in range(100)
            )
            tdSql.execute(f"insert into ct_{d} values {vals}")

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            "create stable vstb_merge (ts timestamp, v_val int) tags(vg int) virtual 1"
        )
        for d in range(10):
            tdSql.execute(
                f"create vtable vct_{d} (v_val from {src}.ct_{d}.v) "
                f"using vstb_merge tags({d})"
            )

        t0 = time.time()
        tdSql.query(f"select count(*) from {db}.vstb_merge")
        elapsed = time.time() - t0
        tdSql.checkData(0, 0, 1000)
        tdLog.debug(f"PERF-011 10-subtable merge: {elapsed:.3f}s")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"drop database if exists {src}")

    # ------------------------------------------------------------------
    # PERF-012  Regression threshold
    # ------------------------------------------------------------------

    def test_fq_perf_012_regression_threshold(self):
        """Regression threshold check

        TS: 对 PERF-001/002/008 三项指标与上一版本基线对比,
            超出退化阈值时标记回归失败

        This is a meta-test that compares collected metrics against stored
        baselines.  Requires baseline data from previous version.

        Catalog:
            - Query:FederatedPerformance

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS PERF-012

        """
        pytest.skip(
            "PERF-012: regression threshold check requires stored baseline "
            "metrics from previous version"
        )
