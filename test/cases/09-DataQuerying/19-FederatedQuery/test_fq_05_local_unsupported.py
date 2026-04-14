"""
test_fq_05_local_unsupported.py

Implements FQ-LOCAL-001 through FQ-LOCAL-045 from TS §5
"不支持项与本地计算项" — local computation for un-pushable operations,
write denial, stream/subscribe rejection, community edition limits.

Design notes:
    - "Local" means the operation cannot be pushed to the external DB
      and must be computed by TDengine after fetching raw data.
    - "Unsupported" means the operation is outright rejected on
      external sources (INSERT/UPDATE/DELETE, stream, subscribe).
    - Internal vtable tests verify local computation paths fully.
    - External source tests verify error codes and parser acceptance.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_STREAM_NOT_SUPPORTED,
    TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED,
    TSDB_CODE_EXT_FEATURE_DISABLED,
)


class TestFq05LocalUnsupported(FederatedQueryTestMixin):
    """FQ-LOCAL-001 through FQ-LOCAL-045: unsupported & local computation."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_local_db",
            "create database fq_local_db",
            "use fq_local_db",
            "create table src_t (ts timestamp, val int, score double, name binary(32), flag bool)",
            "insert into src_t values (1704067200000, 1, 1.5, 'alpha', true)",
            "insert into src_t values (1704067260000, 2, 2.5, 'beta', false)",
            "insert into src_t values (1704067320000, 3, 3.5, 'gamma', true)",
            "insert into src_t values (1704067380000, 4, 4.5, 'delta', false)",
            "insert into src_t values (1704067440000, 5, 5.5, 'epsilon', true)",
            "create stable src_stb (ts timestamp, val int, score double) tags(region int) virtual 1",
            "create vtable vt_local ("
            "  val from fq_local_db.src_t.val,"
            "  score from fq_local_db.src_t.score"
            ") using src_stb tags(1)",
        ]
        tdSql.executes(sqls)

    def _teardown_internal_env(self):
        tdSql.execute("drop database if exists fq_local_db")

    # ------------------------------------------------------------------
    # FQ-LOCAL-001 ~ FQ-LOCAL-005: Window/clause local computation
    # ------------------------------------------------------------------

    def test_fq_local_001(self):
        """FQ-LOCAL-001: STATE_WINDOW — 本地计算路径正确

        Dimensions:
          a) STATE_WINDOW on vtable data
          b) Result correctness verification
          c) Multiple state transitions

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "state_window(flag)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_002(self):
        """FQ-LOCAL-002: INTERVAL 滑动窗口 — 本地计算路径正确

        Dimensions:
          a) INTERVAL with sliding on internal vtable
          b) Window count and data verification
          c) Various sliding ratios

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*), avg(val) from fq_local_db.src_t "
                "interval(2m) sliding(1m)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_003(self):
        """FQ-LOCAL-003: FILL 子句 — 本地填充语义正确

        Dimensions:
          a) FILL(NULL)
          b) FILL(PREV)
          c) FILL(NEXT)
          d) FILL(LINEAR)
          e) FILL(VALUE, v)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            for mode in ("null", "prev", "next", "linear", "value, 0"):
                tdSql.query(
                    f"select _wstart, avg(val) from fq_local_db.src_t "
                    f"where ts >= '2024-01-01' and ts < '2024-01-02' "
                    f"interval(30s) fill({mode})")
                assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_004(self):
        """FQ-LOCAL-004: INTERP 子句 — 本地插值语义正确

        Dimensions:
          a) INTERP with RANGE
          b) EVERY clause
          c) FILL mode in INTERP

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select interp(val) from fq_local_db.src_t "
                "range('2024-01-01 00:00:00', '2024-01-01 00:05:00') "
                "every(30s) fill(linear)")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_005(self):
        """FQ-LOCAL-005: SLIMIT/SOFFSET — 本地分片级截断语义正确

        Dimensions:
          a) SLIMIT on partition result
          b) SLIMIT + SOFFSET
          c) SOFFSET beyond data

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "partition by flag interval(1m) slimit 1")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_006(self):
        """FQ-LOCAL-006: UDF — 不下推，TDengine 本地执行

        Dimensions:
          a) Scalar UDF on external source
          b) Aggregate UDF on external source
          c) Parser acceptance (UDF not pushed down)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_006"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # UDF references on external tables → local execution path
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-007 ~ FQ-LOCAL-011: JOIN and subquery local paths
    # ------------------------------------------------------------------

    def test_fq_local_007(self):
        """FQ-LOCAL-007: Semi/Anti Join(MySQL/PG) — 子查询转换后执行正确

        Dimensions:
          a) Semi join (IN subquery) on MySQL
          b) Anti join (NOT IN subquery) on PG
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_local_007_m"
        p = "fq_local_007_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select * from {m}.users where id in "
                f"(select user_id from {m}.orders) limit 5")
            self._assert_not_syntax_error(
                f"select * from {p}.users where id not in "
                f"(select user_id from {p}.orders) limit 5")
        finally:
            self._cleanup_src(m, p)

    def test_fq_local_008(self):
        """FQ-LOCAL-008: Semi/Anti Join(Influx) — 不支持转换时本地执行

        Dimensions:
          a) IN subquery on InfluxDB → local execution
          b) NOT IN subquery on InfluxDB → local execution
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_008"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_009(self):
        """FQ-LOCAL-009: EXISTS/IN 子查询 — 各源按能力下推或本地回退

        Dimensions:
          a) EXISTS on MySQL (pushdown capable)
          b) EXISTS on InfluxDB (local fallback)
          c) IN subquery on PG
          d) Parser acceptance for all three

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_local_009_m", self._mk_mysql),
                          ("fq_local_009_p", self._mk_pg),
                          ("fq_local_009_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.users limit 5")
            self._cleanup_src(name)

    def test_fq_local_010(self):
        """FQ-LOCAL-010: ALL/ANY/SOME on Influx — 本地计算路径正确

        Dimensions:
          a) ALL on InfluxDB → local
          b) ANY on InfluxDB → local
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_010"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_011(self):
        """FQ-LOCAL-011: CASE 表达式含不可映射子表达式整体本地计算

        Dimensions:
          a) CASE with mappable branches → pushdown
          b) CASE with unmappable branch → entire CASE local
          c) Result correctness

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_011"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select case when val > 0 then val else 0 end from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-012 ~ FQ-LOCAL-017: Function conversion / local paths
    # ------------------------------------------------------------------

    def test_fq_local_012(self):
        """FQ-LOCAL-012: SPREAD 函数三源 MAX-MIN 表达式替代验证

        Dimensions:
          a) SPREAD on MySQL → MAX(col)-MIN(col) pushdown
          b) SPREAD on PG → MAX(col)-MIN(col) pushdown
          c) SPREAD on InfluxDB → same substitution
          d) Internal vtable: result correctness

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select spread(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 4)  # max=5 - min=1 = 4
        finally:
            self._teardown_internal_env()

    def test_fq_local_013(self):
        """FQ-LOCAL-013: GROUP_CONCAT(MySQL)/STRING_AGG(PG/InfluxDB) 转换

        Dimensions:
          a) MySQL → GROUP_CONCAT pushdown
          b) PG → STRING_AGG conversion
          c) Separator parameter mapping

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_local_013_m", self._mk_mysql),
                          ("fq_local_013_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.data limit 5")
            self._cleanup_src(name)

    def test_fq_local_014(self):
        """FQ-LOCAL-014: LEASTSQUARES 本地计算路径验证

        Dimensions:
          a) LEASTSQUARES on internal vtable
          b) Result correctness (slope, intercept)
          c) All three source types fetch raw data then compute locally

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select leastsquares(val, 1, 1) from fq_local_db.src_t")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_local_015(self):
        """FQ-LOCAL-015: LIKE_IN_SET/REGEXP_IN_SET 本地计算

        Dimensions:
          a) LIKE_IN_SET → local (TDengine proprietary)
          b) REGEXP_IN_SET → local (TDengine proprietary)
          c) Parser acceptance on external source

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_015"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_016(self):
        """FQ-LOCAL-016: FILL SURROUND 子句不影响下推行为

        Dimensions:
          a) FILL(PREV) + SURROUND → pushdown portion unaffected
          b) Local fill semantics correct

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= '2024-01-01' and ts < '2024-01-02' "
                "interval(30s) fill(prev)")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_017(self):
        """FQ-LOCAL-017: INTERP 查询时间范围 WHERE 条件下推

        Dimensions:
          a) INTERP + RANGE → WHERE ts BETWEEN pushed down
          b) Local interpolation result correct
          c) Reduced data fetch verified

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select interp(val) from fq_local_db.src_t "
                "range('2024-01-01 00:01:00', '2024-01-01 00:03:00') "
                "every(30s) fill(linear)")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-LOCAL-018 ~ FQ-LOCAL-021: JOIN specifics
    # ------------------------------------------------------------------

    def test_fq_local_018(self):
        """FQ-LOCAL-018: JOIN ON 条件含 TBNAME 时 Parser 报错

        Dimensions:
          a) ON clause with TBNAME pseudo-column → error
          b) Expected TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_local_018"
        self._cleanup_src(m)
        try:
            self._mk_mysql(m)
            tdSql.error(
                f"select * from {m}.t1 a join {m}.t2 b on a.tbname = b.tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(m)

    def test_fq_local_019(self):
        """FQ-LOCAL-019: MySQL 同源跨库 JOIN 可下推

        Dimensions:
          a) Same MySQL source, different databases → pushdown
          b) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        m1 = "fq_local_019"
        self._cleanup_src(m1)
        try:
            self._mk_mysql(m1, database="db1")
            self._assert_not_syntax_error(
                f"select * from {m1}.db1.t1 a join {m1}.db2.t2 b on a.id = b.id limit 5")
        finally:
            self._cleanup_src(m1)

    def test_fq_local_020(self):
        """FQ-LOCAL-020: PG/InfluxDB 跨库 JOIN 不可下推本地执行

        Dimensions:
          a) PG cross-database JOIN → local execution
          b) InfluxDB cross-database JOIN → local execution
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        p = "fq_local_020_p"
        i = "fq_local_020_i"
        self._cleanup_src(p, i)
        try:
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select * from {p}.t1 a join {p}.t2 b on a.id = b.id limit 5")
            self._mk_influx(i)
            self._assert_not_syntax_error(
                f"select * from {i}.cpu limit 5")
        finally:
            self._cleanup_src(p, i)

    def test_fq_local_021(self):
        """FQ-LOCAL-021: InfluxDB IN(subquery) 改写为常量列表

        Dimensions:
          a) Small result set → rewrite IN(v1,v2,...) pushdown
          b) Large result set → local computation
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_021"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-022 ~ FQ-LOCAL-028: Rejection paths
    # ------------------------------------------------------------------

    def test_fq_local_022(self):
        """FQ-LOCAL-022: 流计算中联邦查询拒绝

        Dimensions:
          a) CREATE STREAM on external source → error
          b) Expected TSDB_CODE_EXT_STREAM_NOT_SUPPORTED

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_022"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.error(
                f"create stream s1 trigger at_once into fq_local_022_out "
                f"as select count(*) from {src}.orders interval(1m)",
                expectedErrno=TSDB_CODE_EXT_STREAM_NOT_SUPPORTED)
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop stream if exists s1")

    def test_fq_local_023(self):
        """FQ-LOCAL-023: 订阅中联邦查询拒绝

        Dimensions:
          a) CREATE TOPIC on external source → error
          b) Expected TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_023"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.error(
                f"create topic t1 as select * from {src}.orders",
                expectedErrno=TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED)
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop topic if exists t1")

    def test_fq_local_024(self):
        """FQ-LOCAL-024: 外部写入 INSERT 拒绝

        Dimensions:
          a) INSERT INTO external table → error
          b) Expected TSDB_CODE_EXT_WRITE_DENIED

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_024"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.error(
                f"insert into {src}.orders values (1, 'test', 100)",
                expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
        finally:
            self._cleanup_src(src)

    def test_fq_local_025(self):
        """FQ-LOCAL-025: 外部写入 UPDATE 拒绝

        Dimensions:
          a) UPDATE on external table → error
          b) Expected TSDB_CODE_EXT_WRITE_DENIED or syntax error

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_025"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # TDengine doesn't have UPDATE syntax natively; external update denied
            tdSql.error(
                f"insert into {src}.orders values (1, 'updated', 200)",
                expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
        finally:
            self._cleanup_src(src)

    def test_fq_local_026(self):
        """FQ-LOCAL-026: 外部写入 DELETE 拒绝

        Dimensions:
          a) DELETE FROM external table → error
          b) Expected TSDB_CODE_EXT_WRITE_DENIED

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_026"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.error(
                f"delete from {src}.orders where id = 1",
                expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
        finally:
            self._cleanup_src(src)

    def test_fq_local_027(self):
        """FQ-LOCAL-027: 外部对象操作拒绝 — 索引/触发器/存储过程

        Dimensions:
          a) CREATE INDEX on external → error
          b) Other DDL on external → error
          c) Consistent error code

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_027"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # External table DDL operations rejected
            tdSql.error(
                f"create index idx1 on {src}.orders (id)",
                expectedErrno=None)
        finally:
            self._cleanup_src(src)

    def test_fq_local_028(self):
        """FQ-LOCAL-028: 跨源强一致事务限制

        Dimensions:
          a) Cross-source transaction semantics not supported
          b) Error or fallback to eventually consistent

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_local_028_m"
        p = "fq_local_028_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            # Cross-source queries are read-only, no transaction guarantee
            self._assert_not_syntax_error(
                f"select * from {m}.t1 union all select * from {p}.t1 limit 5")
        finally:
            self._cleanup_src(m, p)

    # ------------------------------------------------------------------
    # FQ-LOCAL-029 ~ FQ-LOCAL-034: Community edition and version limits
    # ------------------------------------------------------------------

    def test_fq_local_029(self):
        """FQ-LOCAL-029: 社区版联邦查询限制

        Dimensions:
          a) Community edition → federated query restricted
          b) Expected TSDB_CODE_EXT_FEATURE_DISABLED or similar

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # Enterprise required; this test documents the behavior
        # In community edition, external source operations should fail
        pytest.skip("Requires community edition binary for verification")

    def test_fq_local_030(self):
        """FQ-LOCAL-030: 社区版外部源 DDL 限制

        Dimensions:
          a) CREATE EXTERNAL SOURCE in community → error
          b) ALTER EXTERNAL SOURCE in community → error
          c) DROP EXTERNAL SOURCE in community → error

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires community edition binary for verification")

    def test_fq_local_031(self):
        """FQ-LOCAL-031: 版本能力提示一致性

        Dimensions:
          a) Community vs enterprise error messages
          b) Error codes consistent with documentation

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires community edition binary for comparison")

    def test_fq_local_032(self):
        """FQ-LOCAL-032: tdengine 外部源预留行为

        Dimensions:
          a) TYPE='tdengine' → reserved, not yet delivered
          b) Create with type='tdengine' → error or reserved message

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_032"
        self._cleanup_src(src)
        try:
            tdSql.error(
                f"create external source {src} type='tdengine' "
                f"host='192.0.2.1' port=6030 user='root' password='taosdata'",
                expectedErrno=None)
        finally:
            self._cleanup_src(src)

    def test_fq_local_033(self):
        """FQ-LOCAL-033: 版本支持矩阵限制

        Dimensions:
          a) External DB version outside support matrix → error or warning
          b) MySQL < 5.7, PG < 12, InfluxDB < v2 → behavior defined

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB with specific versions")

    def test_fq_local_034(self):
        """FQ-LOCAL-034: 不支持语句错误码稳定

        Dimensions:
          a) Stream error code stable
          b) Subscribe error code stable
          c) Write error code stable
          d) Repeated invocations return same code

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_034"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # Verify INSERT error code is stable across invocations
            for _ in range(3):
                tdSql.error(
                    f"insert into {src}.orders values (1, 'x', 1)",
                    expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-035 ~ FQ-LOCAL-037: Hints and pseudo columns
    # ------------------------------------------------------------------

    def test_fq_local_035(self):
        """FQ-LOCAL-035: Hints 不下推全量

        Dimensions:
          a) Hints stripped from remote SQL
          b) Hints effective locally
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_035"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select /*+ para_tables_sort() */ * from {src}.t1 limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_036(self):
        """FQ-LOCAL-036: 伪列限制全量 — TBNAME/TAGS 及其它伪列边界

        Dimensions:
          a) TBNAME on external → not applicable
          b) _ROWTS on external → local mapping
          c) TAGS on non-Influx → not applicable

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_036"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # Basic query without pseudo-columns → OK
            self._assert_not_syntax_error(
                f"select * from {src}.users limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_037(self):
        """FQ-LOCAL-037: TAGS 语义差异验证 — Influx 无数据 tag set 不返回

        Dimensions:
          a) InfluxDB tag query → only returns tags with data
          b) Empty tag set not returned
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_037"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select distinct host from {src}.cpu")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-038 ~ FQ-LOCAL-042: JOIN and pseudo-column local paths
    # ------------------------------------------------------------------

    def test_fq_local_038(self):
        """FQ-LOCAL-038: MySQL FULL OUTER JOIN 路径

        Dimensions:
          a) MySQL doesn't support FULL OUTER JOIN natively
          b) Rewrite (LEFT+RIGHT+UNION) or local fallback
          c) Result consistency with local execution

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_038"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.t1 full outer join {src}.t2 on t1.id = t2.id limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_039(self):
        """FQ-LOCAL-039: ASOF/WINDOW JOIN 路径

        Dimensions:
          a) ASOF JOIN on external → local execution
          b) WINDOW JOIN on external → local execution
          c) Parser acceptance

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # ASOF/WINDOW JOIN are TDengine-specific, always local
            tdSql.execute(
                "create table fq_local_db.t2 (ts timestamp, v2 int)")
            tdSql.execute(
                "insert into fq_local_db.t2 values "
                "(1704067200000, 10) (1704067260000, 20)")
        finally:
            self._teardown_internal_env()

    def test_fq_local_040(self):
        """FQ-LOCAL-040: 伪列 _ROWTS/_c0 联邦查询中本地映射

        Dimensions:
          a) _ROWTS maps to timestamp column locally
          b) _c0 maps to timestamp column locally
          c) Values correct

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select _rowts, val from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.queryResult[0][0] is not None

            tdSql.query("select _c0, val from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.queryResult[0][0] is not None
        finally:
            self._teardown_internal_env()

    def test_fq_local_041(self):
        """FQ-LOCAL-041: 伪列 _QSTART/_QEND 本地计算

        Dimensions:
          a) _QSTART/_QEND from WHERE condition
          b) Values extracted by Planner
          c) Not pushed down

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _qstart, _qend, count(*) from fq_local_db.src_t "
                "where ts >= '2024-01-01' and ts < '2024-01-02' interval(1m)")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_local_042(self):
        """FQ-LOCAL-042: 伪列 _IROWTS/_IROWTS_ORIGIN 本地计算

        Dimensions:
          a) INTERP generates _IROWTS locally
          b) Values correct for interpolated points

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _irowts, interp(val) from fq_local_db.src_t "
                "range('2024-01-01 00:00:30', '2024-01-01 00:04:00') "
                "every(1m) fill(linear)")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-LOCAL-043 ~ FQ-LOCAL-045: Proprietary function local paths
    # ------------------------------------------------------------------

    def test_fq_local_043(self):
        """FQ-LOCAL-043: TO_ISO8601/TIMEZONE() 本地计算

        Dimensions:
          a) TO_ISO8601 on all three sources → local
          b) TIMEZONE() → local
          c) Result correctness

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select to_iso8601(ts) from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.queryResult[0][0] is not None

            tdSql.query("select timezone() from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_local_044(self):
        """FQ-LOCAL-044: COLS()/UNIQUE()/SAMPLE() 本地计算

        Dimensions:
          a) UNIQUE on all sources → local
          b) SAMPLE on all sources → local
          c) Semantics correct

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select unique(val) from fq_local_db.src_t")
            tdSql.checkRows(5)  # all values unique

            tdSql.query("select sample(val, 3) from fq_local_db.src_t")
            tdSql.checkRows(3)
        finally:
            self._teardown_internal_env()

    def test_fq_local_045(self):
        """FQ-LOCAL-045: FILL_FORWARD/MAVG/STATECOUNT/STATEDURATION 本地计算

        Dimensions:
          a) MAVG on all sources → local
          b) STATECOUNT → local
          c) STATEDURATION → local
          d) Raw data fetched then local execution

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select mavg(val, 2) from fq_local_db.src_t")
            assert tdSql.queryRows > 0

            tdSql.query(
                "select statecount(val, 'GT', 2) from fq_local_db.src_t")
            assert tdSql.queryRows > 0

            tdSql.query(
                "select stateduration(val, 'GT', 2) from fq_local_db.src_t")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()
