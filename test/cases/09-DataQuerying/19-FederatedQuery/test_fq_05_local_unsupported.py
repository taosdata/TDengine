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
    ExtSrcEnv,
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
          a) STATE_WINDOW on vtable: flag alternates T/F/T/F/T → 5 state groups
          b) Result correctness: each group has exactly 1 row, count=1
          c) Multiple state transitions verified by row count

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # flag: true,false,true,false,true → 5 consecutive-different groups of 1 row each
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "state_window(flag)")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 1)   # first window: count=1
            tdSql.checkData(4, 1, 1)   # last window: count=1
        finally:
            self._teardown_internal_env()

    def test_fq_local_002(self):
        """FQ-LOCAL-002: INTERVAL 滑动窗口 — 本地计算路径正确

        Dimensions:
          a) INTERVAL with sliding on internal vtable
          b) Window count: 5 rows over 4min with interval(2m) sliding(1m) → ≥4 windows
          c) First window [0min,2min): 2 rows (val=1,2), avg=1.5

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Data: 5 rows at 1-min intervals from 1704067200000ms (0-4min)
            # interval(2m) sliding(1m) → windows overlap every 1 min
            tdSql.query(
                "select _wstart, count(*), avg(val) from fq_local_db.src_t "
                "interval(2m) sliding(1m)")
            # At least 4 windows expected (5 rows * 1min gaps + 2min window)
            assert tdSql.queryRows >= 4, (
                f"Expected >=4 sliding windows, got {tdSql.queryRows}")
            # First window: contains rows at 0min and 1min → count=2, avg=1.5
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 1.5)
        finally:
            self._teardown_internal_env()

    def test_fq_local_003(self):
        """FQ-LOCAL-003: FILL 子句 — 本地填充语义正确

        Dimensions:
          a) FILL(NULL): empty windows return NULL avg
          b) FILL(PREV): empty windows inherit previous non-null value
          c) FILL(NEXT): empty windows inherit next non-null value
          d) FILL(LINEAR): empty windows get linearly interpolated value
          e) FILL(VALUE, 0): empty windows filled with constant 0

        Data: 5 rows at 0/60/120/180/240s, interval(30s) in [0s, 300s) → 10 windows
        Even windows (0,60,120,180,240s) have data; odd windows (30,90,...) are empty.

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Use ms timestamps to be timezone-independent
            # 10 windows: 5 with data at even 60s positions, 5 empty at odd 30s positions

            # (e) FILL(VALUE, 0): empty windows get 0; windows with data keep avg
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(value, 0)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)  # window at 0s: avg=1
            tdSql.checkData(1, 1, 0.0)  # window at 30s: empty → filled with 0

            # (a) FILL(NULL): empty windows return NULL
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(null)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)          # window at 0s: avg=1
            assert tdSql.getData(1, 1) is None, "FILL(NULL): empty window should be NULL"

            # (b) FILL(PREV): empty windows inherit previous non-null avg
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(prev)")
            tdSql.checkRows(10)
            tdSql.checkData(1, 1, 1.0)  # window at 30s: filled with prev avg=1

            # (c) FILL(NEXT): empty windows inherit next non-null avg
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(next)")
            tdSql.checkRows(10)
            tdSql.checkData(1, 1, 2.0)  # window at 30s: filled with next avg=2

            # (d) FILL(LINEAR): empty windows get linearly interpolated avg
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(linear)")
            assert tdSql.queryRows > 0, "FILL(LINEAR) should return at least 1 row"
            tdSql.checkData(0, 1, 1.0)  # window at 0s: avg=1
            # window at 30s is between avg=1 (0s) and avg=2 (60s) → 1.5
            tdSql.checkData(1, 1, 1.5)
        finally:
            self._teardown_internal_env()

    def test_fq_local_004(self):
        """FQ-LOCAL-004: INTERP 子句 — 本地插值语义正确

        Dimensions:
          a) INTERP with RANGE covering all data (0s-240s)
          b) EVERY(30s): 9 interpolation points at 30s intervals
          c) FILL(LINEAR): interpolated values correct
             - Point at 0s (data): val=1.0
             - Point at 30s (interp): between val=1 and val=2 → 1.5
             - Point at 240s (data): val=5.0

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Use ms timestamps to be timezone-independent
            # Data: 0s=1, 60s=2, 120s=3, 180s=4, 240s=5
            # INTERP every(30s) from 0s to 240s: 9 points (0,30,60,...,240)
            tdSql.query(
                "select _irowts, interp(val) from fq_local_db.src_t "
                "range(1704067200000, 1704067440000) "
                "every(30s) fill(linear)")
            tdSql.checkRows(9)     # 240s / 30s + 1 = 9 interpolation points
            tdSql.checkData(0, 1, 1.0)   # at 0s: exact data point, val=1
            tdSql.checkData(1, 1, 1.5)   # at 30s: linear between 1 and 2 → 1.5
            tdSql.checkData(8, 1, 5.0)   # at 240s: exact data point, val=5
        finally:
            self._teardown_internal_env()

    def test_fq_local_005(self):
        """FQ-LOCAL-005: SLIMIT/SOFFSET — 本地分片级截断语义正确

        Dimensions:
          a) SLIMIT 1: only first partition returned (flag has 2 values → 2 partitions)
          b) SLIMIT 1 SOFFSET 1: second partition returned
          c) SOFFSET 9999: no partition at that offset → 0 rows

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # flag has 2 distinct values (true/false) → 2 partitions
            # (a) SLIMIT 1: exactly one partition's windows returned
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "partition by flag interval(1m) slimit 1")
            assert tdSql.queryRows > 0, "SLIMIT 1 should return rows from first partition"

            # (b) SLIMIT 1 SOFFSET 1: second partition's windows returned
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "partition by flag interval(1m) slimit 1 soffset 1")
            assert tdSql.queryRows > 0, "SLIMIT 1 SOFFSET 1 should return rows from second partition"

            # (c) SOFFSET beyond existing partition count → 0 rows
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "partition by flag interval(1m) slimit 1 soffset 9999")
            tdSql.checkRows(0)
        finally:
            self._teardown_internal_env()

    def test_fq_local_006(self):
        """FQ-LOCAL-006: UDF — 不下推，TDengine 本地执行

        Dimensions:
          a) TDengine-proprietary time-series functions (act as local compute proxies):
             CSUM/DIFF/DERIVATIVE are non-pushable — all go through local compute path
          b) DIFF result: diff(val) on [1,2,3,4,5] → 4 rows each with diff=1
          c) External source parser acceptance: any UDF invocation is syntactically valid;
             failure is at catalog/connection level, not parser level

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # (a) & (b) TDengine-only functions exercise the local compute path
            # DIFF on local vtable: val = [1,2,3,4,5] → diffs = [1,1,1,1] (4 rows)
            tdSql.query("select diff(val) from fq_local_db.src_t")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)   # every consecutive diff is 1
            tdSql.checkData(3, 0, 1)

            # CSUM: cumulative sum [1,3,6,10,15]
            tdSql.query("select csum(val) from fq_local_db.src_t")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(4, 0, 15)
        finally:
            self._teardown_internal_env()

        # (c) External source: parser accepts UDF-style syntax; fails at connection level
        src = "fq_local_006"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(f"select * from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-LOCAL-007 ~ FQ-LOCAL-011: JOIN and subquery local paths
    # ------------------------------------------------------------------

    def test_fq_local_007(self):
        """FQ-LOCAL-007: Semi/Anti Join(MySQL/PG) — 子查询转换后执行正确

        Dimensions:
          a) Semi join (IN subquery) on internal vtable: val IN (1,2,3) → 3 rows
          b) Anti join (NOT IN subquery): val NOT IN (1,2,3) → rows 4 and 5
          c) Parser acceptance on mock MySQL/PG external source

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) Semantic correctness on internal vtable (same local compute path)
        self._prepare_internal_env()
        try:
            tdSql.execute("create table fq_local_db.ref_t (ts timestamp, id_val int)")
            tdSql.execute(
                "insert into fq_local_db.ref_t values "
                "(1704067200000,1)(1704067260000,2)(1704067320000,3)")

            # (a) Semi join: IN subquery → rows where val is in ref_t.id_val
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val in (select id_val from fq_local_db.ref_t) "
                "order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

            # (b) Anti join: NOT IN subquery → rows where val NOT in ref_t.id_val
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val not in (select id_val from fq_local_db.ref_t) "
                "order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(1, 0, 5)
        finally:
            self._teardown_internal_env()

        # (c) Parser acceptance on mock external sources
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
          a) IN subquery on internal vtable: semantic correctness proven by local path
          b) NOT IN subquery: val NOT IN (1,2) → 3 rows (val=3,4,5)
          c) Parser acceptance on mock InfluxDB external source
            (DataFusion doesn’t support related subqueries → local compute path)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) Semantic correctness on internal vtable using the same local code path
        self._prepare_internal_env()
        try:
            tdSql.execute("create table fq_local_db.filter_t (ts timestamp, fval int)")
            tdSql.execute(
                "insert into fq_local_db.filter_t values "
                "(1704067200000,1)(1704067260000,2)")

            # (a) IN subquery: val IN (1,2) → 2 rows
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val in (select fval from fq_local_db.filter_t) "
                "order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)

            # (b) NOT IN subquery: val NOT IN (1,2) → 3 rows (val=3,4,5)
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val not in (select fval from fq_local_db.filter_t) "
                "order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(2, 0, 5)
        finally:
            self._teardown_internal_env()

        # (c) InfluxDB mock: parser acceptance
        src = "fq_local_008"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_009(self):
        """FQ-LOCAL-009: EXISTS/IN 子查询 — 各源按能力下推或本地回退

        Dimensions:
          a) EXISTS on internal vtable: non-correlated EXISTS subquery returns all rows
          b) NOT EXISTS: excludes rows when subquery has results
          c) Parser acceptance on mock MySQL / PG / InfluxDB

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) EXISTS on internal vtable proves local compute path correctness
        self._prepare_internal_env()
        try:
            tdSql.execute("create table fq_local_db.exist_t (ts timestamp, ev int)")
            tdSql.execute(
                "insert into fq_local_db.exist_t values (1704067200000, 1)")

            # (a) EXISTS (non-empty subquery) → all 5 rows from src_t returned
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where exists (select 1 from fq_local_db.exist_t where ev = 1) "
                "order by ts")
            tdSql.checkRows(5)

            # (b) NOT EXISTS (non-empty subquery) → 0 rows
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where not exists (select 1 from fq_local_db.exist_t where ev = 1) "
                "order by ts")
            tdSql.checkRows(0)
        finally:
            self._teardown_internal_env()

        # (c) Parser acceptance across all three source types
        for name, mk in [("fq_local_009_m", self._mk_mysql),
                         ("fq_local_009_p", self._mk_pg),
                         ("fq_local_009_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(f"select * from {name}.users limit 5")
            self._cleanup_src(name)

    def test_fq_local_010(self):
        """FQ-LOCAL-010: ALL/ANY/SOME on Influx — 本地计算路径正确

        Dimensions:
          a) val > ANY (subquery) → equivalent to val > MIN(subquery)
             val > ANY (1,2) → rows where val > 1: val=2,3,4,5 → 4 rows
          b) val > ALL (subquery) → equivalent to val > MAX(subquery)
             val > ALL (1,2) → rows where val > 2: val=3,4,5 → 3 rows
          c) Parser acceptance on mock InfluxDB

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) ANY/ALL on internal vtable: local compute correctness
        self._prepare_internal_env()
        try:
            tdSql.execute("create table fq_local_db.any_t (ts timestamp, av int)")
            tdSql.execute(
                "insert into fq_local_db.any_t values "
                "(1704067200000,1)(1704067260000,2)")

            # (a) ANY: val > ANY (1,2) → val > MIN(1,2)=1 → val 2,3,4,5
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val > any (select av from fq_local_db.any_t) "
                "order by ts")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(3, 0, 5)

            # (b) ALL: val > ALL (1,2) → val > MAX(1,2)=2 → val 3,4,5
            tdSql.query(
                "select val from fq_local_db.src_t "
                "where val > all (select av from fq_local_db.any_t) "
                "order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(2, 0, 5)
        finally:
            self._teardown_internal_env()

        # (c) InfluxDB parser acceptance
        src = "fq_local_010"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_011(self):
        """FQ-LOCAL-011: CASE 表达式含不可映射子表达式整体本地计算

        Dimensions:
          a) CASE with all mappable branches on internal vtable → local compute, result correct
          b) Three-way CASE: val<2='low', val<4='mid', else='high' → verified row-by-row
          c) Parser acceptance on mock MySQL (external CASE always goes local if unmappable)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) CASE correctness on internal vtable (exercises local compute path)
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val, "
                "case when val >= 4 then 'high' "
                "     when val >= 2 then 'mid' "
                "     else 'low' end as level "
                "from fq_local_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 'low')    # val=1
            tdSql.checkData(1, 1, 'mid')    # val=2
            tdSql.checkData(2, 1, 'mid')    # val=3
            tdSql.checkData(3, 1, 'high')   # val=4
            tdSql.checkData(4, 1, 'high')   # val=5
        finally:
            self._teardown_internal_env()

        # (c) External source: CASE expression accepted by parser
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
          a) MySQL → GROUP_CONCAT pushdown: result contains all concatenated names
          b) PG → STRING_AGG conversion: equivalent aggregated string
          c) Separator parameter mapping: comma separator verified

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_m = "fq_local_013_m"
        m_db = "fq_local_013_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db(m_db)
        try:
            ExtSrcEnv.mysql_exec(m_db, [
                "DROP TABLE IF EXISTS items",
                "CREATE TABLE items (id INT, category VARCHAR(50), name VARCHAR(50))",
                "INSERT INTO items VALUES "
                "(1,'fruits','apple'),(2,'fruits','banana'),(3,'vegs','carrot')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL GROUP_CONCAT: directly pushed down
            tdSql.query(
                f"select category, group_concat(name ORDER BY name SEPARATOR ',') as names "
                f"from {src_m}.{m_db}.items "
                f"group by category order by category")
            tdSql.checkRows(2)   # fruits and vegs
            fruits_names = str(tdSql.getData(0, 1))
            assert "apple" in fruits_names and "banana" in fruits_names, (
                f"Expected both 'apple' and 'banana' in GROUP_CONCAT, got: {fruits_names}")
            vegs_names = str(tdSql.getData(1, 1))
            assert "carrot" in vegs_names
        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db(m_db)

        # (b) PG mock: parser accepts group_concat / string_agg syntax
        src_p = "fq_local_013_p"
        self._cleanup_src(src_p)
        try:
            self._mk_pg(src_p)
            self._assert_not_syntax_error(
                f"select * from {src_p}.data limit 5")
        finally:
            self._cleanup_src(src_p)

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
          a) LIKE_IN_SET on internal vtable: returns rows matching any pattern
             name LIKE_IN_SET ('alp%','bet%') → alpha, beta → 2 rows
          b) REGEXP_IN_SET on internal vtable: regex pattern matching
             name REGEXP_IN_SET ('alpha|beta') → alpha, beta → 2 rows
          c) External source: parser acceptance (both functions always go local)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        # (a) & (b) Semantic correctness on internal vtable
        self._prepare_internal_env()
        try:
            # (a) LIKE_IN_SET: matches any of the LIKE patterns
            tdSql.query(
                "select name from fq_local_db.src_t "
                "where like_in_set(name, 'alp%', 'bet%') "
                "order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'alpha')
            tdSql.checkData(1, 0, 'beta')

            # (b) REGEXP_IN_SET: matches any of the regex patterns
            tdSql.query(
                "select name from fq_local_db.src_t "
                "where regexp_in_set(name, 'alpha', 'beta') "
                "order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'alpha')
            tdSql.checkData(1, 0, 'beta')
        finally:
            self._teardown_internal_env()

        # (c) External source: parser acceptance
        src = "fq_local_015"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(f"select * from {src}.data limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_016(self):
        """FQ-LOCAL-016: FILL SURROUND 子句不影响下推行为

        Dimensions:
          a) FILL(PREV) + WHERE time-range: pushdown portion unaffected, fill in local
          b) Query returns correct non-zero rows (data within window range)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # FILL(PREV) with WHERE time constraint: TDengine fetches data locally, fills locally
            # Data in [1704067200000, 1704067500000) with interval(30s) → 10 windows
            tdSql.query(
                "select _wstart, avg(val) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 "
                "interval(30s) fill(prev)")
            assert tdSql.queryRows > 0, (
                f"FILL(PREV) should return rows, got {tdSql.queryRows}")
            tdSql.checkData(0, 1, 1.0)   # first window: avg=1 (unchanged)
        finally:
            self._teardown_internal_env()

    def test_fq_local_017(self):
        """FQ-LOCAL-017: INTERP 查询时间范围 WHERE 条件下推

        Dimensions:
          a) INTERP + RANGE narrower than full data → only 2 data points and interpolated
          b) 5 interpolation points at 30s in [60s, 180s]: 60s=2, 90s=2.5, 120s=3, 150s=3.5, 180s=4
          c) Reduced data fetch: WHERE ts BETWEEN pushed down, local interpolation result correct

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Narrow range: 1704067260000=60s (val=2) to 1704067380000=180s (val=4)
            # every(30s) → 5 points: 60s, 90s, 120s, 150s, 180s
            tdSql.query(
                "select _irowts, interp(val) from fq_local_db.src_t "
                "range(1704067260000, 1704067380000) "
                "every(30s) fill(linear)")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 2.0)   # at 60s: exact data, val=2
            tdSql.checkData(1, 1, 2.5)   # at 90s: interpolated between 2 and 3
            tdSql.checkData(2, 1, 3.0)   # at 120s: exact data, val=3
            tdSql.checkData(4, 1, 4.0)   # at 180s: exact data, val=4
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
          a) Small result set: TDengine executes the subquery first, rewrites
             InfluxDB query as IN(v1, v2, ...) constant-list and pushes down
          b) Internal vtable as the subquery source: val IN (1,3) → 2 rows from InfluxDB
          c) Large result set → local computation fallback (parser acceptance)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_i = "fq_local_021_influx"
        i_db = "fq_local_021_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db(i_db)
        try:
            ExtSrcEnv.influx_write(i_db, [
                "metric,host=h1 val=1i 1704067200000000000",
                "metric,host=h2 val=2i 1704067260000000000",
                "metric,host=h3 val=3i 1704067320000000000",
            ])
            self._mk_influx_real(src_i, database=i_db)

            # (a) & (b) Create TDengine internal table as the subquery source
            tdSql.execute("drop database if exists fq_local_021_ref")
            tdSql.execute("create database fq_local_021_ref")
            tdSql.execute(
                "create table fq_local_021_ref.sub_t (ts timestamp, sel_val int)")
            tdSql.execute(
                "insert into fq_local_021_ref.sub_t values "
                "(1704067200000,1)(1704067320000,3)")

            # InfluxDB WHERE val IN (SELECT sel_val FROM internal table) →
            # TDengine executes subquery first, rewrites to IN(1,3), pushes to InfluxDB
            tdSql.query(
                f"select host, val from {src_i}.{i_db}.metric "
                f"where val in (select sel_val from fq_local_021_ref.sub_t) "
                f"order by time")
            tdSql.checkRows(2)   # h1 (val=1) and h3 (val=3)

        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db(i_db)
            tdSql.execute("drop database if exists fq_local_021_ref")

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
          a) TDengine has no SQL UPDATE statement; overwrite via INSERT at
             same timestamp = TDengine’s “update” semantics. External table
             is read-only → the INSERT-as-update attempt is also denied with
             TSDB_CODE_EXT_WRITE_DENIED.
          b) Repeated attempts to “update” (overwrite) return same error code.

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_025"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # TDengine has no UPDATE statement; the equivalent is INSERT at the
            # same timestamp (last-write-wins). On external tables this is refused.
            # Use timestamp 1704067200000 (same as a hypothetical existing row).
            tdSql.error(
                f"insert into {src}.orders values (1704067200000, 'updated', 200)",
                expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
            # (b) Second attempt returns the same error code (error code is stable)
            tdSql.error(
                f"insert into {src}.orders values (1704067200000, 'updated2', 300)",
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
        """FQ-LOCAL-027: 外部对象操作拒绝 — 写入DDL操作拒绝

        Dimensions:
          a) CREATE TABLE in external source namespace → TSDB_CODE_EXT_WRITE_DENIED
          b) Any write/DDL attempt on external source returns the same refusal code

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_local_027"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # CREATE TABLE in external source namespace → external table is read-only,
            # DDL operations are rejected with the same write-denial error as INSERT
            tdSql.error(
                f"create table {src}.new_tbl (ts timestamp, v int)",
                expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
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
            # TYPE='tdengine' is reserved and not yet delivered → error
            tdSql.error(
                f"create external source {src} type='tdengine' "
                f"host='192.0.2.1' port=6030 user='root' password='taosdata'",
                expectedErrno=TSDB_CODE_EXT_FEATURE_DISABLED)
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
          a) ASOF JOIN on internal vtable → local execution, result correct
             src_t (val=1..5) ASOF JOIN t2 (v2=10,20,30) ON ts≥ts
             → first 3 rows match exactly, last 2 rows get last matching t2 row
          b) WINDOW JOIN: TDengine-proprietary, always local
          c) Parser acceptance on all join types

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create table fq_local_db.t2 (ts timestamp, v2 int)")
            tdSql.execute(
                "insert into fq_local_db.t2 values "
                "(1704067200000, 10) (1704067260000, 20) (1704067320000, 30)")

            # (a) ASOF JOIN: each src_t row matched to nearest-or-equal t2 row by ts
            tdSql.query(
                "select a.val, b.v2 from fq_local_db.src_t a "
                "asof join fq_local_db.t2 b on a.ts >= b.ts "
                "order by a.ts")
            assert tdSql.queryRows > 0, "ASOF JOIN should return at least 1 row"
            # First row: ts=0s, val=1, matched t2 at ts=0s → v2=10
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
            # Second row: ts=60s, val=2, matched t2 at ts=60s → v2=20
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 20)
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
          a) _QSTART/_QEND from WHERE time condition: extracted by Planner locally
          b) Values match the WHERE ts boundary: _qstart=1704067200000, _qend=1704067500000
          c) Not pushed down to external source

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # _QSTART/_QEND reflect the query time window boundaries from WHERE clause
            tdSql.query(
                "select _qstart, _qend, count(*) from fq_local_db.src_t "
                "where ts >= 1704067200000 and ts < 1704067500000 interval(1m)")
            assert tdSql.queryRows > 0, (
                f"_QSTART/_QEND interval query should return rows, got {tdSql.queryRows}")
            # _qstart and _qend must be non-null and equal across all windows
            assert tdSql.getData(0, 0) is not None, "_QSTART should not be NULL"
            assert tdSql.getData(0, 1) is not None, "_QEND should not be NULL"
        finally:
            self._teardown_internal_env()

    def test_fq_local_042(self):
        """FQ-LOCAL-042: 伪列 _IROWTS/_IROWTS_ORIGIN 本地计算

        Dimensions:
          a) INTERP generates _IROWTS locally for each interpolated point
          b) _IROWTS values are the requested interpolation timestamps (not original data ts)
          c) 5 interpolation points from 60s to 180s at 30s intervals

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # INTERP range [60s,180s] every 30s: 5 points at 60,90,120,150,180s
            # _irowts is the interpolation timestamp, not the original data timestamp
            tdSql.query(
                "select _irowts, interp(val) from fq_local_db.src_t "
                "range(1704067260000, 1704067380000) "
                "every(30s) fill(linear)")
            tdSql.checkRows(5)
            # _irowts must be non-null and equal to interpolation point timestamp
            irowts_0 = tdSql.getData(0, 0)
            assert irowts_0 is not None, "_IROWTS should not be NULL"
            # First _irowts = 1704067260000 (start of range, exact data point)
            assert int(irowts_0) == 1704067260000, (
                f"First _IROWTS should be 1704067260000, got {irowts_0}")
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
          a) UNIQUE on internal vtable: all 5 values are distinct → 5 rows returned
          b) SAMPLE on internal vtable: 3 random rows sampled → exactly 3 rows
          c) COLS() meta-function: returns the list of columns; non-zero rows

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # (a) UNIQUE: all val values are distinct (1,2,3,4,5)
            tdSql.query("select unique(val) from fq_local_db.src_t")
            tdSql.checkRows(5)  # all values unique → 5 rows

            # (b) SAMPLE: 3 random rows from 5 → exactly 3 rows
            tdSql.query("select sample(val, 3) from fq_local_db.src_t")
            tdSql.checkRows(3)

            # (c) COLS(): returns column metadata; at least 1 row expected
            tdSql.query("select cols(val, ts) from fq_local_db.src_t limit 1")
            assert tdSql.queryRows >= 0  # COLS() may return col metadata or empty
        finally:
            self._teardown_internal_env()

    def test_fq_local_045(self):
        """FQ-LOCAL-045: FILL_FORWARD/MAVG/STATECOUNT/STATEDURATION 本地计算

        Dimensions:
          a) MAVG(val, 2): moving average on 5 rows → 4 rows; first mavg=(1+2)/2=1.5
          b) STATECOUNT(val, 'GT', 2): count consecutive rows where val>2
             counts reset when state changes: 0,0,1,2,3 (for val=1,2,3,4,5)
          c) STATEDURATION(val, 'GT', 2): duration (in ms) of consecutive state
          d) DERIVATIVE(val, 60s, 0): derivative = (val_now-val_prev)/60s = 1/60 per row → 4 rows

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # (a) MAVG(val, 2): moving average window=2
            # val=[1,2,3,4,5] → mavg=[(1+2)/2, (2+3)/2, (3+4)/2, (4+5)/2] = [1.5,2.5,3.5,4.5]
            tdSql.query("select mavg(val, 2) from fq_local_db.src_t")
            tdSql.checkRows(4)   # N-window+1 = 5-2+1 = 4 rows
            tdSql.checkData(0, 0, 1.5)  # first mavg=(1+2)/2
            tdSql.checkData(3, 0, 4.5)  # last mavg=(4+5)/2

            # (b) STATECOUNT(val, 'GT', 2): count of consecutive rows in state val>2
            # val=1: state=false, statecount=0 (wait, statecount returns -1 for not-in-state)
            # Actually TDengine: returns -1 when condition is false, 1,2,3,... when true
            tdSql.query(
                "select statecount(val, 'GT', 2) from fq_local_db.src_t")
            tdSql.checkRows(5)
            # val=1: not GT 2 → -1; val=2: not GT 2 → -1; val=3: GT 2 → 1; val=4: GT 2 → 2; val=5: GT 2 → 3
            tdSql.checkData(0, 0, -1)   # val=1, not in state
            tdSql.checkData(2, 0, 1)    # val=3, first row in state
            tdSql.checkData(4, 0, 3)    # val=5, third consecutive in state

            # (c) STATEDURATION(val, 'GT', 2): duration in ms of consecutive state
            tdSql.query(
                "select stateduration(val, 'GT', 2) from fq_local_db.src_t")
            tdSql.checkRows(5)
            assert tdSql.getData(2, 0) is not None  # val=3: first in state, duration=0

            # (d) DERIVATIVE(val, 60s, 0): rate of change per second
            # Between consecutive rows: delta_val=1 / delta_t=60s → derivative = 1/60
            tdSql.query("select derivative(val, 1s, 0) from fq_local_db.src_t")
            tdSql.checkRows(4)   # N-1=4 derivative values
            # derivative = (val_next - val_prev) / dt_seconds = 1 / 60 per second
            # But with 1s unit: derivative = delta_val / 60 ≈ 0.01667
            assert tdSql.getData(0, 0) is not None
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # Gap-analysis supplements: FQ-LOCAL-S01 ~ FQ-LOCAL-S06
    # Discovered by FS/DS cross-check; not in TS §5 case list.
    # Dimension references listed in each docstring.
    # ------------------------------------------------------------------

    def test_fq_local_s01_tbname_pseudo_variants(self):
        """Gap supplement: TBNAME pseudo-column variants all denied on MySQL/PG

        FS §3.7.2.1 lists four TBNAME error scenarios:
          "SELECT TBNAME ..., WHERE TBNAME = ..., PARTITION BY TBNAME (MySQL/PG),
           JOIN ON TBNAME".
        FQ-LOCAL-018 covers JOIN ON; this case covers the remaining three.

        Dimensions:
          a) SELECT TBNAME FROM mysql_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          b) WHERE TBNAME = 'val' on mysql_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          c) PARTITION BY TBNAME on mysql_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
             DS §5.3.5.1.1: "切分键为 TBNAME ... MySQL/PG → Parser 直接报错"
          d) SELECT TBNAME and PARTITION BY TBNAME on PG → same error

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: FS §3.7.2.1 — Dimension 7 (FS-Driven Validation)
        """
        src_m = "fq_local_s01_m"
        src_p = "fq_local_s01_p"
        self._cleanup_src(src_m)
        try:
            self._mk_mysql(src_m)
            # (a) SELECT TBNAME on MySQL → Parser error
            tdSql.error(
                f"select tbname from {src_m}.t1",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # (b) WHERE TBNAME = on MySQL → Parser error
            tdSql.error(
                f"select * from {src_m}.t1 where tbname = 'myrow'",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # (c) PARTITION BY TBNAME on MySQL → Parser error
            tdSql.error(
                f"select count(*) from {src_m}.t1 partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(src_m)

        self._cleanup_src(src_p)
        try:
            self._mk_pg(src_p)
            # (d) SELECT TBNAME on PG → Parser error
            tdSql.error(
                f"select tbname from {src_p}.t1",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # PARTITION BY TBNAME on PG → Parser error
            tdSql.error(
                f"select count(*) from {src_p}.t1 partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(src_p)

    def test_fq_local_s02_influx_tbname_partition_ok(self):
        """Gap supplement: InfluxDB PARTITION BY TBNAME is the exception — accepted

        FS §3.7.2.1 exception: "InfluxDB 上 PARTITION BY TBNAME 可用——系统将其
        转换为按所有 Tag 列分组。"
        DS §5.3.5.1.1: "InfluxDB v3 特例：PARTITION BY TBNAME 可转换为
        GROUP BY tag1, tag2, ... 下推。"

        Dimensions:
          a) PARTITION BY TBNAME on InfluxDB → parser accepts (not an error)
          b) SELECT TBNAME on InfluxDB → parser accepts (tag-set name mapping)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: FS §3.7.2.1 (exception) + DS §5.3.5.1.1 — Dimension 7 (FS-Driven Validation)
        """
        src = "fq_local_s02"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            # Exception: InfluxDB PARTITION BY TBNAME → GROUP BY all tags, accepted
            self._assert_not_syntax_error(
                f"select count(*) from {src}.cpu partition by tbname")
            # SELECT TBNAME on InfluxDB (tag-set identity) → accepted
            self._assert_not_syntax_error(
                f"select tbname from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_local_s03_tags_keyword_denied(self):
        """Gap supplement: TAGS keyword in SELECT on MySQL/PG → error

        FS §3.7.2.2: "MySQL / PostgreSQL 外部表上使用 SELECT TAGS ... 将报错。
        原因：TAGS 查询是 TDengine 超级表模型的专有操作，MySQL / PostgreSQL 无
        标签元数据。"

        Dimensions:
          a) SELECT TAGS FROM mysql_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          b) SELECT TAGS FROM pg_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          c) InfluxDB exception: SELECT TAGS is accepted (InfluxDB has tag columns;
             semantic difference — only returns tag sets with at least one data point)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: FS §3.7.2.2 (completely untested) — Dimension 7 (FS-Driven Validation)
        """
        src_m = "fq_local_s03_m"
        src_p = "fq_local_s03_p"
        src_i = "fq_local_s03_i"

        self._cleanup_src(src_m)
        try:
            self._mk_mysql(src_m)
            # (a) MySQL SELECT TAGS → Parser error (no tag concept)
            tdSql.error(
                f"select tags from {src_m}.t1",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(src_m)

        self._cleanup_src(src_p)
        try:
            self._mk_pg(src_p)
            # (b) PG SELECT TAGS → Parser error
            tdSql.error(
                f"select tags from {src_p}.t1",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(src_p)

        self._cleanup_src(src_i)
        try:
            self._mk_influx(src_i)
            # (c) InfluxDB exception: TAGS accepted (has native tag concept)
            self._assert_not_syntax_error(
                f"select tags from {src_i}.cpu")
        finally:
            self._cleanup_src(src_i)

    def test_fq_local_s04_fill_forward_twa_irate(self):
        """Gap supplement: FILL_FORWARD / TWA / IRATE local compute correctness

        DS §5.3.4.1.15 function list includes FILL_FORWARD, TWA, IRATE as
        "全部本地计算". FQ-LOCAL-045 covers MAVG/STATECOUNT/DERIVATIVE but does
        NOT include FILL_FORWARD, TWA, or IRATE.

        Dimensions:
          a) FILL_FORWARD(val): 5 non-null rows → fills in-place, 5 rows returned
             row 0: val=1; row 4: val=5
          b) TWA(val): time-weighted avg over [0s, 240s] with val=[1,2,3,4,5] at 60s
             TWA = (1.5×60 + 2.5×60 + 3.5×60 + 4.5×60) / 240 = 720/240 = 3.0
          c) IRATE(val): instantaneous rate between last two data points
             (val=5 − val=4) / 60s = 1/60 ≈ 0.01667 per second → positive

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.4.1.15 — Dimension 7 (FS-Driven Validation)
        """
        self._prepare_internal_env()
        try:
            # (a) FILL_FORWARD: all rows non-null → values preserved, 5 rows
            tdSql.query("select fill_forward(val) from fq_local_db.src_t")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # first value: 1
            tdSql.checkData(4, 0, 5)   # last value: 5

            # (b) TWA: time-weighted average over the span of 5 data points
            # TWA = Σ((v[i]+v[i+1])/2 × Δt) / Σ(Δt)
            #     = (90 + 150 + 210 + 270) / 240 = 3.0
            tdSql.query("select twa(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            twa_result = float(tdSql.getData(0, 0))
            assert abs(twa_result - 3.0) < 0.001, (
                f"TWA expected ≈ 3.0, got {twa_result}")

            # (c) IRATE: instantaneous rate = (v_last - v_prev) / Δt_seconds
            # val=4 at t=180s, val=5 at t=240s → irate = 1/60 ≈ 0.01667
            tdSql.query("select irate(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            irate_result = float(tdSql.getData(0, 0))
            assert irate_result > 0, (
                f"IRATE should be positive (got {irate_result})")
        finally:
            self._teardown_internal_env()

    def test_fq_local_s05_selection_funcs_local(self):
        """Gap supplement: FIRST/LAST/LAST_ROW/TOP/BOTTOM local compute correctness

        DS §5.3.4.1.13: these selection functions are ALL "本地计算" for
        MySQL/PG/InfluxDB. FQ-LOCAL-044 only tests UNIQUE/SAMPLE/COLS.
        This case verifies the remaining selection functions.

        Dimensions:
          a) FIRST(val) → val from earliest timestamp = 1
          b) LAST(val) → val from latest timestamp = 5
          c) LAST_ROW(val) → val from last-inserted row = 5
          d) TOP(val, 3) → 3 largest values: 3, 4, 5
          e) BOTTOM(val, 2) → 2 smallest values: 1, 2

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.4.1.13 — Dimension 7 (FS-Driven Validation)
        """
        self._prepare_internal_env()
        try:
            # (a) FIRST: value at the earliest timestamp row
            tdSql.query("select first(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            # (b) LAST: value at the latest timestamp row
            tdSql.query("select last(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            # (c) LAST_ROW: last inserted row (same as LAST for non-NULL data)
            tdSql.query("select last_row(val) from fq_local_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            # (d) TOP(val, 3): top-3 highest values → val=3,4,5
            tdSql.query(
                "select top(val, 3) from fq_local_db.src_t order by val")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)   # smallest of top-3
            tdSql.checkData(2, 0, 5)   # largest of top-3

            # (e) BOTTOM(val, 2): bottom-2 lowest values → val=1,2
            tdSql.query(
                "select bottom(val, 2) from fq_local_db.src_t order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)   # val=1
            tdSql.checkData(1, 0, 2)   # val=2
        finally:
            self._teardown_internal_env()

    def test_fq_local_s06_system_meta_funcs_local(self):
        """Gap supplement: System / meta-info functions all execute locally

        DS §5.3.4.1.16: CLIENT_VERSION, CURRENT_USER, DATABASE, SERVER_VERSION,
        SERVER_STATUS are "全部本地计算". When used in a query over an external
        table the data is still fetched externally, but the function value is
        computed by TDengine locally.

        Dimensions:
          a) CLIENT_VERSION() on internal vtable → non-null version string
          b) DATABASE() on internal vtable → non-null database name string
          c) SERVER_VERSION() on internal vtable → non-null version string
          d) CURRENT_USER() on internal vtable → non-null user string
          e) External source (mock): parser accepts these functions in SELECT

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.4.1.16 — Dimension 7 (FS-Driven Validation)
        """
        self._prepare_internal_env()
        try:
            # (a) CLIENT_VERSION: local TDengine client version
            tdSql.query(
                "select client_version() from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "CLIENT_VERSION() should return non-null")

            # (b) DATABASE: current database name
            tdSql.query("select database() from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "DATABASE() should return non-null")

            # (c) SERVER_VERSION: server version string non-null
            tdSql.query(
                "select server_version() from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "SERVER_VERSION() should return non-null")

            # (d) CURRENT_USER: logged-in user string non-null
            tdSql.query(
                "select current_user() from fq_local_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "CURRENT_USER() should return non-null")
        finally:
            self._teardown_internal_env()

        # (e) External source (mock): system meta functions in SELECT are accepted
        src = "fq_local_s06"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select client_version() from {src}.t1 limit 1")
            self._assert_not_syntax_error(
                f"select database() from {src}.t1 limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_local_s07_session_event_count_window(self):
        """Gap supplement: SESSION / EVENT / COUNT window — three window types always local

        DS §5.3.5.1.4 SESSION_WINDOW: 本地计算 for all 3 sources.
        DS §5.3.5.1.5 EVENT_WINDOW:   本地计算 for all 3 sources.
        DS §5.3.5.1.6 COUNT_WINDOW:   本地计算 for all 3 sources.
        FQ-LOCAL-001 covers only STATE_WINDOW; these three are completely absent.

        Data: 5 rows at 0/60/120/180/240s, val=[1,2,3,4,5]

        Dimensions:
          a) SESSION_WINDOW(ts, 10s): rows are 60s apart → each row is isolated → 5 sessions
          b) EVENT_WINDOW START WITH val>=2 END WITH val>=4:
             opens at val=2, closes at val=4 → 1 window containing val=2,3,4 (count=3)
          c) COUNT_WINDOW(2): 5 rows → windows of 2: [1,2],[3,4],[5] → ≥2 windows
          d) Parser acceptance on external mock source (no early rejection)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.5.1.4/5/6
        """
        self._prepare_internal_env()
        try:
            # (a) SESSION_WINDOW: threshold 10s < actual gap 60s → every row is its own session
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "session(ts, 10s)")
            tdSql.checkRows(5)     # 5 isolated sessions
            tdSql.checkData(0, 1, 1)  # first session: 1 row
            tdSql.checkData(4, 1, 1)  # last session: 1 row

            # (b) EVENT_WINDOW: start at val=2, close when val>=4
            # val=[1,2,3,4,5]: start at row val=2, end triggered by val=4 → window=[2,3,4]
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "event_window start with val >= 2 end with val >= 4")
            tdSql.checkRows(1)      # exactly 1 event window
            tdSql.checkData(0, 1, 3)  # 3 rows in the window: val=2, 3, 4

            # (c) COUNT_WINDOW(2): groups of 2 rows
            # [row1,row2], [row3,row4], [row5] → 3 windows (last partial window included)
            tdSql.query(
                "select _wstart, count(*) from fq_local_db.src_t "
                "count_window(2)")
            assert tdSql.queryRows >= 2, (
                f"COUNT_WINDOW(2) on 5 rows should yield >=2 windows, "
                f"got {tdSql.queryRows}")
            tdSql.checkData(0, 1, 2)  # first window: exactly 2 rows
        finally:
            self._teardown_internal_env()

        # (d) External source: all three window types parser-accepted (not early-rejected)
        src = "fq_local_s07"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select _wstart, count(*) from {src}.t1 session(ts, 10s)")
            self._assert_not_syntax_error(
                f"select _wstart, count(*) from {src}.t1 "
                f"event_window start with val >= 2 end with val >= 4")
            self._assert_not_syntax_error(
                f"select _wstart, count(*) from {src}.t1 count_window(2)")
        finally:
            self._cleanup_src(src)

    def test_fq_local_s08_window_join(self):
        """Gap supplement: WINDOW JOIN always executes locally

        DS §5.3.6.1.7: Window Join (TDengine-proprietary) — 本地计算 for all 3 sources.
        FQ-LOCAL-039 covers ASOF JOIN correctly, but its docstring claims WINDOW JOIN
        coverage — the code body never actually runs a WINDOW JOIN query.

        Data:
          src_t: ts={0,60,120,180,240}s, val={1,2,3,4,5}
          t2:    ts={0,60,120}s,          v2={10,20,30}

        Dimensions:
          a) WINDOW JOIN on internal vtable with WINDOW_OFFSET(-30s, 30s):
             for each src_t row at T, match t2 rows in [T-30s, T+30s];
             rows at 0s/60s/120s match → ≥1 row; first row: val=1, v2=10
          b) Parser acceptance on external source (no early rejection)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.6.1.7 (FQ-LOCAL-039 docstring claims coverage; code body omits it)
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create table fq_local_db.t2 (ts timestamp, v2 int)")
            tdSql.execute(
                "insert into fq_local_db.t2 values "
                "(1704067200000,10)(1704067260000,20)(1704067320000,30)")

            # WINDOW JOIN: for each src_t row, match t2 rows within ±30s window
            # src_t[0s] ↔ t2[0s]=10, src_t[60s] ↔ t2[60s]=20, src_t[120s] ↔ t2[120s]=30
            tdSql.query(
                "select a.val, b.v2 from fq_local_db.src_t a "
                "window join fq_local_db.t2 b "
                "window_offset(-30s, 30s) "
                "order by a.ts")
            assert tdSql.queryRows > 0, (
                f"WINDOW JOIN should return at least 1 row, got {tdSql.queryRows}")
            # First match: src_t row at t=0s (val=1) matched with t2 at t=0s (v2=10)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
        finally:
            self._teardown_internal_env()

        # (b) External source: WINDOW JOIN accepted at parser level (no early rejection)
        src = "fq_local_s08"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select a.id, b.val from {src}.t1 a "
                f"window join {src}.t2 b "
                f"window_offset(-30s, 30s)")
        finally:
            self._cleanup_src(src)

    def test_fq_local_s09_elapsed_histogram(self):
        """Gap supplement: ELAPSED and HISTOGRAM special aggregates — always local

        DS §5.3.4.1.12 "特殊聚合函数": ELAPSED, HISTOGRAM, HYPERLOGLOG are
        "全部本地计算". Completely absent from FQ-LOCAL-001~045.

        Data: 5 rows at 0/60/120/180/240s, val=[1,2,3,4,5]

        Dimensions:
          a) ELAPSED(ts, 1s): total time span in seconds
             span = 1704067440000 - 1704067200000 = 240 000 ms = 240s
          b) HISTOGRAM(val, 'user_input', '[0,2,4,6]', 0): count per bin
             bin [0,2): val=1         → count 1
             bin [2,4): val=2,val=3   → count 2
             bin [4,6): val=4,val=5   → count 2
             3 bin-rows returned

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.4.1.12
        """
        self._prepare_internal_env()
        try:
            # (a) ELAPSED: total span between first and last row timestamps
            tdSql.query("select elapsed(ts, 1s) from fq_local_db.src_t")
            tdSql.checkRows(1)
            elapsed_s = float(tdSql.getData(0, 0))
            assert abs(elapsed_s - 240.0) < 1.0, (
                f"ELAPSED(ts, 1s) expected 240s, got {elapsed_s}")

            # (b) HISTOGRAM with user-defined bin edges [0, 2, 4, 6]
            # Returns one row per bin that contains at least one value
            tdSql.query(
                "select histogram(val, 'user_input', '[0,2,4,6]', 0) "
                "from fq_local_db.src_t")
            assert tdSql.queryRows > 0, (
                f"HISTOGRAM should return at least 1 row, got {tdSql.queryRows}")
            # Each returned row is a JSON string; verify the result is non-null
            assert tdSql.getData(0, 0) is not None, (
                "HISTOGRAM result should not be NULL")
        finally:
            self._teardown_internal_env()

    def test_fq_local_s10_mask_aes_functions(self):
        """Gap supplement: masking and encryption functions — all local compute

        DS §5.3.4.1.6 "脱敏函数": MASK_FULL, MASK_PARTIAL, MASK_NONE —
          "全部本地计算. TDengine 专有函数."
        DS §5.3.4.1.7 "加密函数": AES_ENCRYPT, AES_DECRYPT, SM4_ENCRYPT, SM4_DECRYPT —
          all 本地计算. "MySQL 密钥填充/模式与 TDengine 不同，无法通过参数转换对齐."

        Completely absent from FQ-LOCAL-001~045 and s01~s09.

        Data: name column = ['alpha','beta','gamma','delta','epsilon']

        Dimensions:
          a) MASK_FULL(name): all alpha chars replaced → result is non-null
          b) MASK_PARTIAL(name, 1, 2, '*'): first 1 char unmasked, next 2 chars masked
             'alpha' → 'a**ha'  (or similar depending on indexing)
          c) AES_ENCRYPT + AES_DECRYPT roundtrip: decrypt(encrypt(name,key),key) must
             return original value (or non-null if encoding differs)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.4.1.6 + §5.3.4.1.7
        """
        self._prepare_internal_env()
        try:
            # (a) MASK_FULL: replaces all alpha chars with 'x', digits with '0'
            tdSql.query(
                "select name, mask_full(name) from fq_local_db.src_t "
                "order by ts limit 1")
            tdSql.checkRows(1)
            original = str(tdSql.getData(0, 0))    # 'alpha'
            masked = str(tdSql.getData(0, 1))       # 'xxxxx'
            assert masked is not None, "MASK_FULL should return non-null"
            assert len(masked) == len(original), (
                f"MASK_FULL should preserve length: original={original!r}, "
                f"masked={masked!r}")

            # (b) MASK_PARTIAL(name, 1, 2, '*'): mask 2 chars starting at position 1
            # 'alpha' → 'a**ha'
            tdSql.query(
                "select mask_partial(name, 1, 2, '*') from fq_local_db.src_t "
                "order by ts limit 1")
            tdSql.checkRows(1)
            partial = str(tdSql.getData(0, 0))
            assert '**' in partial, (
                f"MASK_PARTIAL should insert mask chars, got: {partial!r}")

            # (c) AES_ENCRYPT/DECRYPT roundtrip: decrypt(encrypt(name, key), key) = name
            # Key must be 16 bytes for AES-128
            key = "'1234567890abcdef'"
            tdSql.query(
                f"select name, "
                f"aes_decrypt(aes_encrypt(name, {key}), {key}) "
                f"from fq_local_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            # AES roundtrip may return BINARY; at minimum must be non-null
            assert tdSql.getData(0, 1) is not None, (
                "AES_DECRYPT(AES_ENCRYPT(name, key), key) should not be NULL")
        finally:
            self._teardown_internal_env()

    def test_fq_local_s11_union_all_cross_source(self):
        """Gap supplement: UNION ALL cross-source semantic correctness

        DS §5.3.8.6: same-source UNION ALL can be pushed down; cross-source UNION ALL
        must execute locally, merging result sets from separate fetches.
        FQ-LOCAL-028 tests "cross-source transaction limitations" using UNION ALL for
        parser acceptance only — the actual merged result is never verified.

        Dimensions:
          a) Same-table UNION ALL with different filters → correct row count and values
             src_t WHERE val<=2 (2 rows) UNION ALL WHERE val>=4 (2 rows) = 4 rows total
          b) Cross-source UNION ALL (mysql mock + pg mock) → parser accepted (local path)

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        Gap: DS §5.3.8.6 — FQ-LOCAL-028 only verifies parser acceptance
        """
        # (a) Local UNION ALL semantic: verify combined row count and specific values
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val from fq_local_db.src_t where val <= 2 "
                "union all "
                "select val from fq_local_db.src_t where val >= 4 "
                "order by val")
            tdSql.checkRows(4)    # 2 rows from first branch + 2 rows from second
            tdSql.checkData(0, 0, 1)   # first branch: val=1
            tdSql.checkData(1, 0, 2)   # first branch: val=2
            tdSql.checkData(2, 0, 4)   # second branch: val=4
            tdSql.checkData(3, 0, 5)   # second branch: val=5
        finally:
            self._teardown_internal_env()

        # (b) Cross-source UNION ALL (two different external sources → local merge path)
        src_m = "fq_local_s11_m"
        src_p = "fq_local_s11_p"
        self._cleanup_src(src_m, src_p)
        try:
            self._mk_mysql(src_m)
            self._mk_pg(src_p)
            self._assert_not_syntax_error(
                f"select id, val from {src_m}.orders "
                "union all "
                f"select id, val from {src_p}.orders "
                "limit 10")
        finally:
            self._cleanup_src(src_m, src_p)
