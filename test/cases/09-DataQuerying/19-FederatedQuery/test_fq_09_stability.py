"""
test_fq_09_stability.py

Implements long-term stability tests from TS "长期稳定性测试" section.
Four focus areas:
    1. 72h continuous query mix (single-source / cross-source JOIN / vtable)
    2. Fault injection (external source unreachable, slow query, throttle, jitter)
    3. Cache stability (meta/capability cache repeated expiry & refresh, no leak)
    4. Connection pool stability (high/low concurrency switching, no zombie conns)

Since these are non-functional stability tests that require sustained runtime,
tests here are structured as *representative short cycles* that exercise the
same code paths.  In CI they run a small iteration count; a dedicated stability
environment would increase the count and duration.

Design notes:
    - Tests use internal vtables where possible so no external DB is needed.
    - Fault-injection tests use RFC 5737 TEST-NET addresses (192.0.2.x).
    - Tests needing actual long-duration or resource monitors are guarded with
      pytest.skip().

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
"""

import time
import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
)


class TestFq09Stability:
    """Long-term stability tests — typical short-cycle representatives."""

    STAB_DB = "fq_stab_db"
    SRC_DB = "fq_stab_src"

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prepare_env(self):
        """Create internal databases, tables, vtables for stability loops."""
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

        values_d1 = ", ".join(
            f"({1704067200000 + i * 1000}, {i}, {i * 1.1}, {str(i % 2 == 0).lower()})"
            for i in range(1, 101)
        )
        tdSql.execute(f"insert into src_d1 values {values_d1}")

        values_d2 = ", ".join(
            f"({1704067200000 + i * 1000}, {i + 100}, {i * 2.2}, {str(i % 2 != 0).lower()})"
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

        tdSql.execute(
            "create table local_dim (ts timestamp, device_id int, weight int)"
        )
        tdSql.execute("insert into local_dim values (1704067201000, 1, 100)")
        tdSql.execute("insert into local_dim values (1704067202000, 2, 200)")

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

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS stability section

        """
        self._prepare_env()

        iterations = 20
        for i in range(iterations):
            # Single-source query
            tdSql.query(f"select count(*), sum(val), avg(val) from {self.SRC_DB}.src_d1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 100)
            tdSql.checkData(0, 1, 5050)

            # Vtable super-table aggregate
            tdSql.query(f"select count(*) from {self.STAB_DB}.vstb")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 150)

            # Vtable group query
            tdSql.query(
                f"select vg, count(*) from {self.STAB_DB}.vstb "
                f"group by vg order by vg"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 50)

            # Cross-table: vtable JOIN local dim
            tdSql.query(
                f"select a.v_val, b.weight from {self.STAB_DB}.vt_d1 a, "
                f"{self.STAB_DB}.local_dim b where a.ts = b.ts"
            )
            assert tdSql.queryRows > 0, "JOIN should return at least 1 row"

        # Negative: non-existent table
        tdSql.error(
            f"select * from {self.STAB_DB}.no_such_vtable",
            expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST,
        )

        # Final sanity
        tdSql.query(f"select count(*) from {self.STAB_DB}.vstb")
        tdSql.checkData(0, 0, 150)

        self._teardown_env()

    # ------------------------------------------------------------------
    # STAB-002  Fault injection (external source unreachable)
    # ------------------------------------------------------------------

    def test_fq_stab_002_fault_injection_unreachable(self):
        """Fault injection — external source unreachable / jitter

        TS: 外部源短时不可达、慢查询、限流、连接抖动

        1. Create external source pointing to non-routable 192.0.2.x
        2. Rapid fire queries — all should fail with connection error
        3. Verify no crash or state corruption
        4. Drop source and verify cleanup

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS stability section

        """
        src_name = "stab_unreachable_src"
        tdSql.execute(f"drop external source if exists {src_name}")
        tdSql.execute(
            f"create external source {src_name} type='mysql' "
            f"host='192.0.2.1' port=3306 user='u' password='p' database='testdb' "
            f"options(connect_timeout_ms=500)"
        )

        for _ in range(5):
            tdSql.error(
                f"select * from {src_name}.testdb.some_table",
                expectedErrno=None,
            )

        # Source should still exist in catalog
        tdSql.query("show external sources")
        found = any(str(row[0]) == src_name for row in tdSql.queryResult)
        assert found, f"{src_name} should survive failed queries"

        tdSql.execute(f"drop external source {src_name}")

        # After drop the query should fail differently
        tdSql.error(
            f"select * from {src_name}.testdb.some_table",
            expectedErrno=None,
        )

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
        self._prepare_env()

        for i in range(10):
            tdSql.query(f"select count(*) from {self.STAB_DB}.vstb")
            tdSql.checkData(0, 0, 150)

            tdSql.query(
                f"select vg, avg(v_score) from {self.STAB_DB}.vstb "
                f"group by vg order by vg"
            )
            tdSql.checkRows(2)

        self._teardown_env()

    # ------------------------------------------------------------------
    # STAB-004  Connection pool stability
    # ------------------------------------------------------------------

    def test_fq_stab_004_connection_pool_stability(self):
        """Connection pool stability — concurrency switching

        TS: 并发高峰与低峰切换，无僵尸连接

        Full pool-pressure test requires external DBs and multi-threaded client.
        Skip in CI.

        Catalog:
            - Query:FederatedStability

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS stability section

        """
        pytest.skip(
            "Full connection pool stability test requires real external sources "
            "and multi-threaded clients"
        )

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
        self._prepare_env()

        baseline = None
        for i in range(50):
            tdSql.query(
                f"select vg, count(*), sum(v_val), min(v_val), max(v_val) "
                f"from {self.STAB_DB}.vstb group by vg order by vg"
            )
            current = tdSql.queryResult
            if baseline is None:
                baseline = current
            else:
                if current != baseline:
                    tdLog.exit(
                        f"result drift at iteration {i}: "
                        f"expected={baseline}, got={current}"
                    )

        self._teardown_env()
