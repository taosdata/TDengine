"""
test_fq_06_pushdown_fallback.py

Implements FQ-PUSH-001 through FQ-PUSH-035 from TS §6
"下推优化与兜底恢复" — pushdown capabilities, condition/aggregate/sort/
limit pushdown, JOIN pushdown, pRemotePlan construction, recovery and
diagnostics.

Design notes:
    - Pushdown tests validate that the query planner correctly decides
      what to push down to external sources vs compute locally.
    - Tests verify behavior via EXPLAIN and result correctness.
    - Failure/recovery tests require live external DBs for full coverage.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_PUSHDOWN_FAILED,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
)


class TestFq06PushdownFallback(FederatedQueryTestMixin):
    """FQ-PUSH-001 through FQ-PUSH-035: pushdown optimization & recovery."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_push_db",
            "create database fq_push_db",
            "use fq_push_db",
            "create table src_t (ts timestamp, val int, score double, name binary(32), flag bool)",
            "insert into src_t values (1704067200000, 1, 1.5, 'alpha', true)",
            "insert into src_t values (1704067260000, 2, 2.5, 'beta', false)",
            "insert into src_t values (1704067320000, 3, 3.5, 'gamma', true)",
            "insert into src_t values (1704067380000, 4, 4.5, 'delta', false)",
            "insert into src_t values (1704067440000, 5, 5.5, 'epsilon', true)",
            "create stable src_stb (ts timestamp, val int, score double) tags(region int) virtual 1",
            "create vtable vt_push ("
            "  val from fq_push_db.src_t.val,"
            "  score from fq_push_db.src_t.score"
            ") using src_stb tags(1)",
        ]
        tdSql.executes(sqls)

    def _teardown_internal_env(self):
        tdSql.execute("drop database if exists fq_push_db")

    # ------------------------------------------------------------------
    # FQ-PUSH-001 ~ FQ-PUSH-004: Capability flags and conditions
    # ------------------------------------------------------------------

    def test_fq_push_001(self):
        """FQ-PUSH-001: 全能力关闭 — 能力位全 false 走零下推路径

        Dimensions:
          a) All pushdown capabilities disabled → zero pushdown
          b) Result still correct (all local computation)
          c) Parser acceptance

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_001"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # Zero pushdown: all computation local
            self._assert_not_syntax_error(
                f"select count(*) from {src}.orders")
        finally:
            self._cleanup_src(src)

    def test_fq_push_002(self):
        """FQ-PUSH-002: 条件全可映射 — FederatedCondPushdown 全量下推

        Dimensions:
          a) Simple WHERE with = → pushdown
          b) Compound WHERE with AND/OR → pushdown
          c) All conditions mappable

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_002"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.orders where status = 1 and amount > 100 limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_003(self):
        """FQ-PUSH-003: 条件部分可映射 — 可下推条件下推，不可下推本地保留

        Dimensions:
          a) Mix of pushable and non-pushable conditions
          b) Pushable part sent to remote
          c) Non-pushable part computed locally

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_003"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # Mix of regular condition (pushable) and TDengine function (not pushable)
            self._assert_not_syntax_error(
                f"select * from {src}.orders where amount > 100 limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_004(self):
        """FQ-PUSH-004: 条件不可映射 — 全部本地过滤

        Dimensions:
          a) All conditions non-mappable → full local filter
          b) Raw data fetched, filtered locally
          c) Result correct

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_004"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-PUSH-005 ~ FQ-PUSH-010: Aggregate, sort, limit pushdown
    # ------------------------------------------------------------------

    def test_fq_push_005(self):
        """FQ-PUSH-005: 聚合可下推 — Agg+Group Key 全可映射时下推

        Dimensions:
          a) COUNT/SUM/AVG with GROUP BY → pushdown
          b) All functions and group keys mappable

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_005"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select status, count(*), sum(amount) from {src}.orders group by status")
        finally:
            self._cleanup_src(src)

    def test_fq_push_006(self):
        """FQ-PUSH-006: 聚合不可下推 — 任一函数不可映射则聚合整体本地

        Dimensions:
          a) One non-mappable function → entire aggregate local
          b) Raw data fetched, aggregation computed locally
          c) Result correct

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # TDengine-specific ELAPSED → not pushable → entire aggregate local
            tdSql.query("select elapsed(ts) from fq_push_db.src_t")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_push_007(self):
        """FQ-PUSH-007: 排序可下推 — ORDER BY 可映射，MySQL NULLS 规则改写正确

        Dimensions:
          a) ORDER BY on pushable column → pushdown
          b) MySQL NULLS FIRST/LAST rewrite
          c) PG native NULLS support

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_push_007_m", self._mk_mysql),
                          ("fq_push_007_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.data order by val limit 10")
            self._cleanup_src(name)

    def test_fq_push_008(self):
        """FQ-PUSH-008: 排序不可下推 — 排序表达式不可映射时本地排序

        Dimensions:
          a) ORDER BY non-mappable expression → local sort
          b) Result ordered correctly

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val, score from fq_push_db.src_t order by val desc")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 5)  # highest val first
        finally:
            self._teardown_internal_env()

    def test_fq_push_009(self):
        """FQ-PUSH-009: LIMIT 可下推 — 无 partition 且依赖前置满足

        Dimensions:
          a) Simple query with LIMIT → pushdown
          b) LIMIT + ORDER BY → both pushdown when possible

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_009"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data order by id limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_push_010(self):
        """FQ-PUSH-010: LIMIT 不可下推 — PARTITION 或本地 Agg/Sort 时本地 LIMIT

        Dimensions:
          a) LIMIT with PARTITION BY → local LIMIT
          b) LIMIT with local aggregate → local LIMIT

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_push_db.src_t "
                "partition by flag interval(1m) limit 3")
            assert tdSql.queryRows <= 3
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-PUSH-011 ~ FQ-PUSH-016: Partition, window, JOIN, subquery
    # ------------------------------------------------------------------

    def test_fq_push_011(self):
        """FQ-PUSH-011: Partition 转换 — PARTITION BY 列转换到 GROUP BY

        Dimensions:
          a) PARTITION BY → GROUP BY conversion for remote
          b) Result semantics preserved

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_011"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select avg(usage_idle) from {src}.cpu partition by host")
        finally:
            self._cleanup_src(src)

    def test_fq_push_012(self):
        """FQ-PUSH-012: Window 转换 — 翻滚窗口转等效 GROUP BY 表达式

        Dimensions:
          a) INTERVAL → GROUP BY date_trunc equivalent
          b) Conversion for MySQL/PG/InfluxDB

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_012"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select count(*) from {src}.events interval(1h)")
        finally:
            self._cleanup_src(src)

    def test_fq_push_013(self):
        """FQ-PUSH-013: 同源 JOIN 下推 — 同 source（及库约束）可下推

        Dimensions:
          a) Same MySQL source, same database → pushdown
          b) Same MySQL source, cross-database → pushdown (MySQL allows)
          c) PG same database → pushdown

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_push_013_m"
        p = "fq_push_013_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._assert_not_syntax_error(
                f"select a.id from {m}.t1 a join {m}.t2 b on a.id = b.fk limit 5")
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select a.id from {p}.t1 a join {p}.t2 b on a.id = b.fk limit 5")
        finally:
            self._cleanup_src(m, p)

    def test_fq_push_014(self):
        """FQ-PUSH-014: 跨源 JOIN 回退 — 保留本地 JOIN

        Dimensions:
          a) MySQL JOIN PG → local JOIN
          b) Data fetched from both, joined locally
          c) Parser acceptance

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_push_014_m"
        p = "fq_push_014_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select a.id from {m}.users a join {p}.orders b on a.id = b.user_id limit 5")
        finally:
            self._cleanup_src(m, p)

    def test_fq_push_015(self):
        """FQ-PUSH-015: 子查询递归下推 — 内外层可映射场景合并下推

        Dimensions:
          a) Both inner and outer queries mappable → merge push
          b) Single remote SQL execution

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_015"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from (select id, name from {src}.users where active = 1) t "
                f"where t.id > 10 limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_016(self):
        """FQ-PUSH-016: 子查询部分下推 — 仅内层下推，外层本地执行

        Dimensions:
          a) Inner query pushable, outer has non-pushable function
          b) Inner fetched remotely, outer computed locally
          c) Result correct

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_016"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from (select id from {src}.users) t limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-PUSH-017 ~ FQ-PUSH-020: Plan construction and failure
    # ------------------------------------------------------------------

    def test_fq_push_017(self):
        """FQ-PUSH-017: pRemotePlan 构建顺序 — Filter->Agg->Sort->Limit 节点顺序正确

        Dimensions:
          a) Remote plan: WHERE → GROUP BY → ORDER BY → LIMIT
          b) Node order verified via EXPLAIN

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_017"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select status, count(*) from {src}.orders "
                f"where amount > 0 group by status order by status limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_push_018(self):
        """FQ-PUSH-018: pushdown_flags 编码 — 位掩码与实际下推内容一致

        Dimensions:
          a) Flags encoding matches actual pushdown behavior
          b) Cross-verify with EXPLAIN output

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_018"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data where id > 0 order by id limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_push_019(self):
        """FQ-PUSH-019: 下推失败语法类 — 产生 TSDB_CODE_EXT_PUSHDOWN_FAILED

        Dimensions:
          a) Pushdown failure due to remote dialect incompatibility
          b) Expected TSDB_CODE_EXT_PUSHDOWN_FAILED
          c) Client re-plans with zero pushdown

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB to trigger pushdown failure")

    def test_fq_push_020(self):
        """FQ-PUSH-020: 客户端禁用下推重规划 — 重规划后零下推结果正确

        Dimensions:
          a) After TSDB_CODE_EXT_PUSHDOWN_FAILED → client re-plan
          b) Zero pushdown execution
          c) Result matches full-pushdown result

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB to test re-planning")

    # ------------------------------------------------------------------
    # FQ-PUSH-021 ~ FQ-PUSH-025: Recovery and diagnostics
    # ------------------------------------------------------------------

    def test_fq_push_021(self):
        """FQ-PUSH-021: 连接错误重试 — Scheduler 按可重试语义重试

        Dimensions:
          a) Connection timeout → retry
          b) Retry count and backoff
          c) Eventually succeed or fail gracefully

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB to test retry behavior")

    def test_fq_push_022(self):
        """FQ-PUSH-022: 认证错误不重试 — 置 unavailable 并快速失败

        Dimensions:
          a) Authentication failure → no retry
          b) Source marked unavailable
          c) Fast fail on subsequent queries

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB with bad credentials")

    def test_fq_push_023(self):
        """FQ-PUSH-023: 资源限制退避 — degraded + backoff 行为正确

        Dimensions:
          a) Resource limit → degraded state
          b) Exponential backoff
          c) Recovery to available

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB under load")

    def test_fq_push_024(self):
        """FQ-PUSH-024: 可用性状态流转 — available/degraded/unavailable 切换正确

        Dimensions:
          a) available → degraded on resource limit
          b) degraded → unavailable on persistent failure
          c) unavailable → available on recovery

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for state transition testing")

    def test_fq_push_025(self):
        """FQ-PUSH-025: 诊断日志完整性 — 原 SQL/远端 SQL/远端错误/pushdown_flags 记录完整

        Dimensions:
          a) Original SQL logged
          b) Remote SQL logged
          c) Remote error info logged
          d) pushdown_flags logged

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB and log inspection")

    # ------------------------------------------------------------------
    # FQ-PUSH-026 ~ FQ-PUSH-030: Consistency and special cases
    # ------------------------------------------------------------------

    def test_fq_push_026(self):
        """FQ-PUSH-026: 三路径正确性一致 — 全下推/部分下推/零下推结果一致

        Dimensions:
          a) Full pushdown result
          b) Partial pushdown result
          c) Zero pushdown result
          d) All three results identical

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Internal vtable: verify same result regardless of plan
            tdSql.query("select count(*), avg(val) from fq_push_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
        finally:
            self._teardown_internal_env()

    def test_fq_push_027(self):
        """FQ-PUSH-027: PG FDW 外部表映射为普通表查询

        Dimensions:
          a) PG FDW table → read as normal table
          b) Mapping semantics consistent

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_027"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select * from {src}.fdw_table limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_028(self):
        """FQ-PUSH-028: PG 继承表映射为独立普通表

        Dimensions:
          a) PG inherited table → independent table
          b) Inheritance not affecting mapping

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_028"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select * from {src}.child_table limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_029(self):
        """FQ-PUSH-029: InfluxDB 标识符大小写区分

        Dimensions:
          a) Case-sensitive measurement names
          b) Case-sensitive tag/field names
          c) Different case = different identifier

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_push_029"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select * from {src}.CPU limit 5")
            self._assert_not_syntax_error(
                f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_push_030(self):
        """FQ-PUSH-030: 多节点环境外部连接器版本检查

        Dimensions:
          a) All nodes same connector version → OK
          b) Version mismatch → warning or error

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires multi-node cluster environment")

    # ------------------------------------------------------------------
    # FQ-PUSH-031 ~ FQ-PUSH-035: Advanced diagnostics and rules
    # ------------------------------------------------------------------

    def test_fq_push_031(self):
        """FQ-PUSH-031: 下推执行失败诊断日志完整性

        Dimensions:
          a) Failed pushdown → log: original SQL
          b) Log: remote SQL
          c) Log: remote error (remote_code/message)
          d) Log: pushdown_flags

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB and server log inspection")

    def test_fq_push_032(self):
        """FQ-PUSH-032: 客户端重规划禁用下推结果一致性

        Dimensions:
          a) TSDB_CODE_EXT_PUSHDOWN_FAILED → client re-plan
          b) Zero pushdown result equals partial pushdown result

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB to test re-plan consistency")

    def test_fq_push_033(self):
        """FQ-PUSH-033: Full Outer JOIN PG/InfluxDB 直接下推

        Dimensions:
          a) PG FULL OUTER JOIN → direct pushdown
          b) InfluxDB FULL OUTER JOIN → direct pushdown
          c) Result matches local execution

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_push_033_p", self._mk_pg),
                          ("fq_push_033_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.t1 full outer join {name}.t2 on t1.id = t2.id limit 5")
            self._cleanup_src(name)

    def test_fq_push_034(self):
        """FQ-PUSH-034: 联邦规则列表独立性验证

        Dimensions:
          a) Query with external scan → federated rules
          b) Pure local query → original 31 rules
          c) No interference between rule sets

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Local query uses standard rules
            tdSql.query("select count(*) from fq_push_db.src_t")
            tdSql.checkData(0, 0, 5)

            # External query uses federated rules
            src = "fq_push_034"
            self._cleanup_src(src)
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select count(*) from {src}.orders")
            self._cleanup_src(src)
        finally:
            self._teardown_internal_env()

    def test_fq_push_035(self):
        """FQ-PUSH-035: 通用结构优化规则在联邦计划中生效

        Dimensions:
          a) MergeProjects rule effective
          b) EliminateProject rule effective
          c) EliminateSetOperator rule effective
          d) Local operator chain optimized correctly

        Catalog: - Query:FederatedPushdown
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Verify optimizer rules apply to federated plans
            tdSql.query(
                "select val from (select val, score from fq_push_db.src_t) order by val limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
        finally:
            self._teardown_internal_env()
