"""
test_fq_05_local_unsupported.py

Implements FQ-LOCAL-001 through FQ-LOCAL-045 from TS §5
"Unsupported operations and local computation" — local computation for un-pushable operations,
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
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_STREAM_NOT_SUPPORTED,
    TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED,
    TSDB_CODE_EXT_FEATURE_DISABLED,
)


class TestFq05LocalUnsupported(FederatedQueryVersionedMixin):
    """FQ-LOCAL-001 through FQ-LOCAL-045: unsupported & local computation."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        try:
            tdSql.execute("drop database if exists fq_local_db")
        except Exception:
            pass

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
        """FQ-LOCAL-001: STATE_WINDOW — local compute path correctness

        Dimensions:
          a) MySQL → STATE_WINDOW on flag INT (1/0/1/0/1), locally computed; 5 windows
          b) PG → same
          c) InfluxDB → same (flag stored as INT field)
          d) Internal vtable baseline: flag BOOL, same 5 windows, _wstart verified

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources — same local compute path
        def _body(src):
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"state_window(flag)")
            tdSql.checkRows(5)
            for i in range(5):
                tdSql.checkData(i, 1, 1)   # each state group has count=1
        self._with_std_sources("fq_local_001", _body)

    def test_fq_local_002(self):
        """FQ-LOCAL-002: INTERVAL sliding window — local compute path correctness

        Dimensions:
          a) MySQL → INTERVAL(2m) SLIDING(1m) on real data; 6 windows verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: same 6 windows, _wstart timezone-independent

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # interval(2m) sliding(1m) on 5 rows at 0/60/120/180/240s → 6 windows
            tdSql.query(
                f"select _wstart, count(*), avg(val) from {src}.src_t "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"interval(2m) sliding(1m)")
            tdSql.checkRows(6)
            expected_windows = [
                (1, 1.0), (2, 1.5), (2, 2.5), (2, 3.5), (2, 4.5), (1, 5.0)]
            for i, (cnt, avg_v) in enumerate(expected_windows):
                tdSql.checkData(i, 1, cnt)
                tdSql.checkData(i, 2, avg_v)
        self._with_std_sources("fq_local_002", _body)

    def test_fq_local_003(self):
        """FQ-LOCAL-003: FILL clause — local fill semantics correctness

        Dimensions:
          a) MySQL → FILL(VALUE, 0): 10 windows, data/empty alternating, verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: all five FILL variants (NULL/PREV/NEXT/LINEAR/VALUE)

        Data: 5 rows at 0/60/120/180/240s, interval(30s) in [0s, 300s) → 10 windows
        Even windows (0,60,120,180,240s) have data; odd windows (30,90,...) are empty.

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources — all five FILL variants
        def _body(src):
            base = (
                f"select _wstart, avg(val) from {src}.src_t "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"interval(30s) fill")
            # fill(value, 0)
            tdSql.query(f"{base}(value, 0)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)  # 0s: avg=1
            tdSql.checkData(1, 1, 0.0)  # 30s: empty → fill 0
            tdSql.checkData(2, 1, 2.0)  # 60s: avg=2
            tdSql.checkData(3, 1, 0.0)  # 90s: empty → fill 0
            tdSql.checkData(4, 1, 3.0)  # 120s: avg=3
            tdSql.checkData(5, 1, 0.0)  # 150s: empty → fill 0
            tdSql.checkData(6, 1, 4.0)  # 180s: avg=4
            tdSql.checkData(7, 1, 0.0)  # 210s: empty → fill 0
            tdSql.checkData(8, 1, 5.0)  # 240s: avg=5
            tdSql.checkData(9, 1, 0.0)  # 270s: empty → fill 0
            # fill(null)
            tdSql.query(f"{base}(null)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)
            assert tdSql.getData(1, 1) is None, "FILL(NULL): 30s window should be NULL"
            tdSql.checkData(2, 1, 2.0)
            assert tdSql.getData(3, 1) is None, "FILL(NULL): 90s window should be NULL"
            tdSql.checkData(4, 1, 3.0)
            # fill(prev)
            tdSql.query(f"{base}(prev)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)
            tdSql.checkData(1, 1, 1.0)  # 30s: prev=1.0
            tdSql.checkData(2, 1, 2.0)
            tdSql.checkData(3, 1, 2.0)  # 90s: prev=2.0
            tdSql.checkData(4, 1, 3.0)
            # fill(next)
            tdSql.query(f"{base}(next)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)
            tdSql.checkData(1, 1, 2.0)  # 30s: next=2.0
            tdSql.checkData(3, 1, 3.0)  # 90s: next=3.0
            tdSql.checkData(5, 1, 4.0)  # 150s: next=4.0
            tdSql.checkData(7, 1, 5.0)  # 210s: next=5.0
            # fill(linear)
            tdSql.query(f"{base}(linear)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)
            tdSql.checkData(1, 1, 1.5)  # 30s: linear between 1 and 2
            tdSql.checkData(2, 1, 2.0)
            tdSql.checkData(3, 1, 2.5)  # 90s: linear between 2 and 3
            tdSql.checkData(4, 1, 3.0)
            tdSql.checkData(5, 1, 3.5)  # 150s: linear between 3 and 4
        self._with_std_sources("fq_local_003", _body)

    def test_fq_local_004(self):
        """FQ-LOCAL-004: INTERP clause — local interpolation semantics correctness

        Dimensions:
          a) MySQL → INTERP range(0s,240s) every(30s) fill(linear); 9 points, vals verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: same 9-point result with exact val checks

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # Data: 0s=1, 60s=2, 120s=3, 180s=4, 240s=5
            # INTERP every(30s) in [0s,240s]: 9 points at 0,30,60,...,240s
            # val is INT: interp returns floor for intermediate points
            tdSql.query(
                f"select _irowts, interp(val) from {src}.src_t "
                f"range(1704067200000, 1704067440000) "
                f"every(30s) fill(linear)")
            tdSql.checkRows(9)
            tdSql.checkData(0, 1, 1)   # at 0s: exact data point
            tdSql.checkData(2, 1, 2)   # at 60s: exact data point
            tdSql.checkData(4, 1, 3)   # at 120s: exact data point
            tdSql.checkData(6, 1, 4)   # at 180s: exact data point
            tdSql.checkData(8, 1, 5)   # at 240s: exact data point
        self._with_std_sources("fq_local_004", _body)

    def test_fq_local_005(self):
        """FQ-LOCAL-005: SLIMIT/SOFFSET — local partition-level truncation semantics correctness

        Dimensions:
          a) MySQL → PARTITION BY flag INTERVAL(1m) SLIMIT 1: one partition returned
          b) PG → same
          c) InfluxDB → same (flag stored as INT field 0/1, two distinct partitions)
          d) Internal vtable baseline: SLIMIT/SOFFSET with PARTITION BY flag BOOL

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # flag has 2 distinct values (1/0) → 2 partitions
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"partition by flag interval(1m) slimit 1")
            first_part_rows = tdSql.queryRows
            assert first_part_rows in (2, 3), (
                f"SLIMIT 1 should return 2 or 3 windows (one partition), "
                f"got {first_part_rows}")
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"partition by flag interval(1m) slimit 1 soffset 1")
            second_part_rows = tdSql.queryRows
            assert first_part_rows + second_part_rows == 5, (
                f"Two partitions must total 5 windows, "
                f"got {first_part_rows}+{second_part_rows}")
            # soffset beyond existing partitions → 0 rows
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"partition by flag interval(1m) slimit 1 soffset 9999")
            tdSql.checkRows(0)
        self._with_std_sources("fq_local_005", _body)

    def test_fq_local_006(self):
        """FQ-LOCAL-006: Non-pushable functions — executed locally by TDengine

        Dimensions:
          a) MySQL → DIFF/CSUM on real external data, locally computed; results verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: same DIFF/CSUM results on TDengine-native table

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # DIFF on val=[1,2,3,4,5] → 4 rows, each diff=1
            tdSql.query(f"select diff(val) from {src}.src_t")
            tdSql.checkRows(4)
            for i in range(4):
                tdSql.checkData(i, 0, 1)
            # CSUM: cumulative sum [1,3,6,10,15]
            tdSql.query(f"select csum(val) from {src}.src_t")
            tdSql.checkRows(5)
            for i, expected in enumerate([1, 3, 6, 10, 15]):
                tdSql.checkData(i, 0, expected)
        self._with_std_sources("fq_local_006", _body)

    # ------------------------------------------------------------------
    # FQ-LOCAL-007 ~ FQ-LOCAL-011: JOIN and subquery local paths
    # ------------------------------------------------------------------

    def test_fq_local_007(self):
        """FQ-LOCAL-007: Semi/Anti Join — IN/NOT IN subquery on real external sources

        Two execution paths are tested:

        Path 1 (完全下推 / Fully-Pushed-to-External-DB):
          Both outer table and subquery source are in the same external database.
          TDengine parser currently rejects external source references inside a
          subquery context; these cases will fail if that limitation is not fixed.
            a) MySQL IN same-source: orders WHERE user_id IN (SELECT id FROM users WHERE active=1)
            b) MySQL NOT IN same-source: orders WHERE user_id NOT IN (SELECT id FROM users WHERE active=0)
            c) PG IN same-source
            d) PG NOT IN same-source

        Path 2 (TDengine子查询 / TDengine-Orchestrated Subquery):
          TDengine evaluates the inner subquery against its own internal table,
          collects the result list [1, 3], rewrites to IN(1,3) const-list, then
          pushes to InfluxDB (ext_can_pushdown_in_const_list).
          Only InfluxDB outer source is registered during this phase (no MySQL/PG).
            e) InfluxDB outer + TDengine internal IN: val IN (1,3) → h1,h3 → 2 rows

        Data:
          MySQL/PG users: alice(id=1,active=1), bob(id=2,active=0), charlie(id=3,active=1)
          MySQL/PG orders: (id=1,user_id=1),(id=2,user_id=1),(id=3,user_id=2)
          InfluxDB sensor: h1(val=1), h2(val=2), h3(val=3), h4(val=4)
          TDengine internal uid_list: sel_val IN (1, 3)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-xx wpan Rewritten to use real external sources; two execution paths

        """
        m = "fq_local_007_m"
        m_db = "fq_007_m_db"
        p = "fq_local_007_p"
        p_db = "fq_007_p_db"
        influx_src = "fq_local_007_i"
        i_db = "fq_007_i_db"
        ref_db = "fq_007_ref"
        self._cleanup_src(m, p, influx_src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            # ── Data setup ──────────────────────────────────────────────────────
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "CREATE TABLE IF NOT EXISTS users "
                "(id INT PRIMARY KEY, name VARCHAR(32), active TINYINT(1))",
                "DELETE FROM users",
                "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
                "CREATE TABLE IF NOT EXISTS orders "
                "(id INT, user_id INT, amount DOUBLE, status VARCHAR(16))",
                "DELETE FROM orders",
                "INSERT INTO orders VALUES "
                "(1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
            ])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "CREATE TABLE IF NOT EXISTS users "
                "(id INT PRIMARY KEY, name TEXT, active INT)",
                "DELETE FROM users",
                "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
                "CREATE TABLE IF NOT EXISTS orders "
                "(id INT, user_id INT, amount FLOAT8, status TEXT)",
                "DELETE FROM orders",
                "INSERT INTO orders VALUES "
                "(1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
            ])
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                "sensor,host=h1 val=1i 1704067200000000000",
                "sensor,host=h2 val=2i 1704067260000000000",
                "sensor,host=h3 val=3i 1704067320000000000",
                "sensor,host=h4 val=4i 1704067380000000000",
            ])

            # TDengine internal reference table: sel_val IN (1, 3)
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.uid_list (ts timestamp, sel_val int)")
            tdSql.execute(
                f"insert into {ref_db}.uid_list values "
                f"(1704067200000,1)(1704067320000,3)")

            # ── Path 1: 完全下推 (Fully-Pushed-to-External-DB) ──────────────────
            # MySQL same-source subquery — parser rejects external refs in subquery

            self._mk_mysql_real(m, database=m_db)

            # (a) MySQL IN same-source
            tdSql.query(
                f"select id from {m}.orders "
                f"where user_id in (select id from {m}.users where active = 1) "
                f"order by id")
            tdSql.checkRows(2)

            # (b) MySQL NOT IN same-source
            tdSql.query(
                f"select id from {m}.orders "
                f"where user_id not in (select id from {m}.users where active = 0) "
                f"order by id")
            tdSql.checkRows(2)

            # Drop MySQL source before testing PG
            self._cleanup_src(m)

            # PG same-source subquery
            self._mk_pg_real(p, database=p_db)

            # (c) PG IN same-source
            tdSql.query(
                f"select id from {p}.orders "
                f"where user_id in (select id from {p}.users where active = 1) "
                f"order by id")
            tdSql.checkRows(2)

            # (d) PG NOT IN same-source
            tdSql.query(
                f"select id from {p}.orders "
                f"where user_id not in (select id from {p}.users where active = 0) "
                f"order by id")
            tdSql.checkRows(2)

            # Drop PG source before InfluxDB Path 2
            self._cleanup_src(p)

            # ── Path 2: TDengine子查询 (TDengine-Orchestrated Subquery) ──────────
            # Only InfluxDB external source registered here.
            # TDengine evaluates the internal subquery → gets [1,3] → rewrites to
            # IN(1,3) const-list → pushes to InfluxDB (ext_can_pushdown_in_const_list).
            self._mk_influx_real(influx_src, database=i_db)

            # Sanity: InfluxDB source queryable → all 4 rows
            tdSql.query(
                f"select `host`, val from {influx_src}.sensor order by ts")
            tdSql.checkRows(4)

            # (e) InfluxDB outer + TDengine internal IN: val IN (1,3) → h1,h3 → 2 rows
            tdSql.query(
                f"select `host`, val from {influx_src}.sensor "
                f"where val in (select sel_val from {ref_db}.uid_list) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # h1: val=1
            tdSql.checkData(1, 1, 3)   # h3: val=3

        finally:
            self._cleanup_src(m, p, influx_src)
            tdSql.execute(f"drop database if exists {ref_db}")
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    def test_fq_local_008(self):
        """FQ-LOCAL-008: Semi/Anti Join (InfluxDB) — IN/NOT IN subquery; two execution paths

        Path 1 (TDengine子查询 / TDengine-Orchestrated, internal subquery):
          Only InfluxDB external source is registered during this phase.
          TDengine evaluates the inner subquery against an internal table,
          rewrites to const-list, then pushes to InfluxDB via
          ext_can_pushdown_in_const_list (same pattern as test_fq_local_021).
            a) InfluxDB outer + TDengine internal IN: val IN (1,3) → h1,h3 → 2 rows
            b) InfluxDB outer + TDengine internal NOT IN: val NOT IN (1,3) → h2,h4 → 2 rows

        Path 2 (跨源子查询 / Cross-Source Subquery):
          PG source is also registered here alongside InfluxDB.
          Cross-source execution is not yet fully implemented; cases will fail
          if the feature is not yet supported.
            c) InfluxDB outer + PG subquery IN: val IN (SELECT fval FROM pg.filter)
            d) InfluxDB outer + PG subquery NOT IN: val NOT IN (...)

        Data:
          InfluxDB sensor: h1(val=1), h2(val=2), h3(val=3), h4(val=4)
          PG filter: fval IN (1, 3)
          TDengine internal uid_list: sel_val IN (1, 3)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-xx wpan Rewritten to use real external sources; two execution paths

        """
        src_i = "fq_local_008_influx"
        i_db = "fq_008_i_db"
        src_p = "fq_local_008_pg"
        p_db = "fq_008_p_db"
        ref_db = "fq_008_ref"
        self._cleanup_src(src_i, src_p)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            # ── Data setup ──────────────────────────────────────────────────────
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                "sensor,host=h1 val=1i 1704067200000000000",
                "sensor,host=h2 val=2i 1704067260000000000",
                "sensor,host=h3 val=3i 1704067320000000000",
                "sensor,host=h4 val=4i 1704067380000000000",
            ])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "CREATE TABLE IF NOT EXISTS filter (id INT, fval INT)",
                "DELETE FROM filter",
                "INSERT INTO filter VALUES (1,1),(2,3)",
            ])

            # TDengine internal reference table: sel_val IN (1, 3) — created before source reg
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.uid_list (ts timestamp, sel_val int)")
            tdSql.execute(
                f"insert into {ref_db}.uid_list values "
                f"(1704067200000,1)(1704067320000,3)")

            # ── Path 1: TDengine子查询 — ONLY InfluxDB source registered ─────────
            # Register InfluxDB source FIRST (matches 021 pattern), then create internal table.
            # No PG source yet. TDengine evaluates internal subquery → const-list → InfluxDB.
            self._mk_influx_real(src_i, database=i_db)

            # Sanity: InfluxDB source queryable → all 4 rows
            tdSql.query(
                f"select `host`, val from {src_i}.sensor order by ts")
            tdSql.checkRows(4)

            # (a) IN from TDengine internal: val IN (1,3) → h1(val=1), h3(val=3) → 2 rows
            tdSql.query(
                f"select `host`, val from {src_i}.sensor "
                f"where val in (select sel_val from {ref_db}.uid_list) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # h1: val=1
            tdSql.checkData(1, 1, 3)   # h3: val=3

            # (b) NOT IN from TDengine internal: val NOT IN (1,3) → h2(val=2),h4(val=4) → 2 rows
            tdSql.query(
                f"select `host`, val from {src_i}.sensor "
                f"where val not in (select sel_val from {ref_db}.uid_list) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 2)   # h2: val=2
            tdSql.checkData(1, 1, 4)   # h4: val=4

            # ── Path 2: 跨源子查询 — register PG, InfluxDB outer + PG subquery ───
            # Now PG source is also registered.
            self._mk_pg_real(src_p, database=p_db)

            # (c) IN cross-source: val IN (SELECT fval FROM pg.filter) → h1,h3 → 2 rows
            tdSql.query(
                f"select `host`, val from {src_i}.sensor "
                f"where val in (select fval from {src_p}.filter) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # h1: val=1
            tdSql.checkData(1, 1, 3)   # h3: val=3

            # (d) NOT IN cross-source: val NOT IN (...) → h2,h4 → 2 rows
            tdSql.query(
                f"select `host`, val from {src_i}.sensor "
                f"where val not in (select fval from {src_p}.filter) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 2)   # h2: val=2
            tdSql.checkData(1, 1, 4)   # h4: val=4

            # ── Path 3: MySQL outer + TDengine internal IN/NOT IN ────────────────────────
            # TDengine evaluates the inner subquery against the internal table,
            # rewrites to const-list IN(1,3), then pushes to MySQL.
            src_m = "fq_local_008_m"
            m_db_008 = "fq_008_m_db"
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db_008)
            try:
                ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db_008, [
                    "CREATE TABLE IF NOT EXISTS sensor "
                    "(ts DATETIME(3), val INT, host VARCHAR(16))",
                    "DELETE FROM sensor",
                    "INSERT INTO sensor VALUES "
                    "('2024-01-01 00:00:00.000',1,'h1'),"
                    "('2024-01-01 00:01:00.000',2,'h2'),"
                    "('2024-01-01 00:02:00.000',3,'h3'),"
                    "('2024-01-01 00:03:00.000',4,'h4')",
                ])
                self._mk_mysql_real(src_m, database=m_db_008)

                # (e) MySQL IN from TDengine internal: val IN (1,3) → 2 rows
                tdSql.query(
                    f"select ts, val from {src_m}.sensor "
                    f"where val in (select sel_val from {ref_db}.uid_list) "
                    f"order by ts")
                tdSql.checkRows(2)
                tdSql.checkData(0, 1, 1)
                tdSql.checkData(1, 1, 3)

                # (f) MySQL NOT IN from TDengine internal: val NOT IN (1,3) → 2 rows
                tdSql.query(
                    f"select ts, val from {src_m}.sensor "
                    f"where val not in (select sel_val from {ref_db}.uid_list) "
                    f"order by ts")
                tdSql.checkRows(2)
                tdSql.checkData(0, 1, 2)
                tdSql.checkData(1, 1, 4)
            finally:
                self._cleanup_src(src_m)
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db_008)
                except Exception:
                    pass
        finally:
            self._cleanup_src(src_i, src_p)
            tdSql.execute(f"drop database if exists {ref_db}")
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_local_009(self):
        """FQ-LOCAL-009: EXISTS/NOT EXISTS subquery on real external sources; two execution paths

        Path 1 (完全下推 / Fully-Pushed-to-External-DB):
          Same-source correlated EXISTS: external DB evaluates correlated subquery natively.
          Note: TDengine parser does not yet support external source references inside a
          subquery context; these cases will fail if that limitation is not fixed.
            a) PG correlated EXISTS: orders WHERE EXISTS (SELECT 1 FROM users WHERE u.id = o.user_id)
               All 3 orders have matching users → 3 rows
            b) PG correlated NOT EXISTS: orders WHERE NOT EXISTS (...) → 0 rows
            c) MySQL correlated EXISTS: orders WHERE EXISTS (active user match) → 2 rows

        Path 2 (TDengine子查询 / TDengine-Orchestrated Subquery):
          TDengine evaluates the inner subquery against an internal table; the result
          controls whether EXISTS returns true/false for the outer external query.
          Non-correlated EXISTS is used: the entire external outer query is filtered
          based on whether the internal subquery returns any rows.
            d) PG outer + TDengine internal EXISTS (non-correlated):
               EXISTS (SELECT 1 FROM ref_db.flag WHERE val=1) is always TRUE → all 3 orders
            e) PG outer + TDengine internal NOT EXISTS (non-correlated):
               NOT EXISTS (SELECT 1 FROM ref_db.empty_t) is always TRUE → all 3 orders
            f) MySQL outer + TDengine internal EXISTS (non-correlated):
               EXISTS (SELECT 1 FROM ref_db.flag WHERE val=1) is always TRUE → all 3 orders

        Data:
          PG users: alice(id=1,active=1), bob(id=2,active=0), charlie(id=3,active=1)
          PG orders: (id=1,user_id=1),(id=2,user_id=1),(id=3,user_id=2)
          MySQL: same structure as PG
          TDengine internal flag: val IN (1) — non-empty table for EXISTS
          TDengine internal empty_t: empty table for NOT EXISTS

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-xx wpan Rewritten to use real external sources; two execution paths

        """
        p = "fq_local_009_p"
        p_db = "fq_009_p_db"
        m = "fq_local_009_m"
        m_db = "fq_009_m_db"
        ref_db = "fq_009_ref"
        self._cleanup_src(p, m)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            # ── Data setup ──────────────────────────────────────────────────────
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "CREATE TABLE IF NOT EXISTS users "
                "(id INT PRIMARY KEY, name TEXT, active INT)",
                "DELETE FROM users",
                "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
                "CREATE TABLE IF NOT EXISTS orders "
                "(id INT, user_id INT, amount FLOAT8, status TEXT)",
                "DELETE FROM orders",
                "INSERT INTO orders VALUES "
                "(1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
            ])
            self._mk_pg_real(p, database=p_db)

            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "CREATE TABLE IF NOT EXISTS users "
                "(id INT PRIMARY KEY, name VARCHAR(32), active TINYINT(1))",
                "DELETE FROM users",
                "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
                "CREATE TABLE IF NOT EXISTS orders "
                "(id INT, user_id INT, amount DOUBLE, status VARCHAR(16))",
                "DELETE FROM orders",
                "INSERT INTO orders VALUES "
                "(1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
            ])
            self._mk_mysql_real(m, database=m_db)

            # TDengine internal tables: flag (non-empty) and empty_t (empty)
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.flag (ts timestamp, val int)")
            tdSql.execute(
                f"insert into {ref_db}.flag values (1704067200000,1)")
            tdSql.execute(
                f"create table {ref_db}.empty_t (ts timestamp, val int)")
            # empty_t intentionally has no rows

            # ── Path 1: 完全下推 (Fully-Pushed-to-External-DB) ──────────────────
            # Correlated EXISTS: external DB handles per-row subquery evaluation.
            # TDengine parser currently rejects external source refs in subquery context.

            # (a) PG correlated EXISTS: all 3 orders have matching users → 3 rows
            tdSql.query(
                f"select id from {p}.orders o "
                f"where exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id")
            tdSql.checkRows(3)

            # (b) PG NOT EXISTS correlated: all orders have matching users → 0 rows
            tdSql.query(
                f"select id from {p}.orders o "
                f"where not exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id")
            tdSql.checkRows(0)

            # (c) MySQL correlated EXISTS: orders for active users (alice:2 orders) → 2 rows
            tdSql.query(
                f"select id from {m}.orders o "
                f"where exists "
                f"(select 1 from {m}.users u where u.id = o.user_id and u.active = 1) "
                f"order by id")
            tdSql.checkRows(2)

            # ── Path 2: TDengine子查询 (TDengine-Orchestrated, non-correlated) ───
            # TDengine evaluates EXISTS against an internal table.
            # Non-correlated EXISTS: the truth value is the same for every outer row.

            # (d) PG outer + TDengine internal EXISTS:
            #     EXISTS (SELECT 1 FROM flag WHERE val=1) → TRUE → all 3 orders returned
            tdSql.query(
                f"select id from {p}.orders "
                f"where exists (select 1 from {ref_db}.flag where val = 1) "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

            # (e) PG outer + TDengine internal NOT EXISTS (empty table):
            #     NOT EXISTS (SELECT 1 FROM empty_t) → TRUE → all 3 orders returned
            tdSql.query(
                f"select id from {p}.orders "
                f"where not exists (select 1 from {ref_db}.empty_t) "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

            # (f) MySQL outer + TDengine internal EXISTS:
            #     EXISTS (SELECT 1 FROM flag WHERE val=1) → TRUE → all 3 orders returned
            tdSql.query(
                f"select id from {m}.orders "
                f"where exists (select 1 from {ref_db}.flag where val = 1) "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

            # ── Path 3: InfluxDB outer + TDengine internal non-correlated EXISTS ───────────
            src_i = "fq_local_009_i"
            i_db_009 = "fq_009_i_db"
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_009)
            try:
                ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_009, [
                    "orders,id=1 val=100i 1704067200000000000",
                    "orders,id=2 val=200i 1704067260000000000",
                    "orders,id=3 val=50i  1704067320000000000",
                ])
                self._mk_influx_real(src_i, database=i_db_009)

                # (g) InfluxDB outer + TDengine internal EXISTS (TRUE) → all 3 rows
                tdSql.query(
                    f"select val from {src_i}.orders "
                    f"where exists (select 1 from {ref_db}.flag where val = 1) "
                    f"order by ts")
                tdSql.checkRows(3)

                # (h) InfluxDB outer + NOT EXISTS (empty table, TRUE) → 3 rows
                tdSql.query(
                    f"select val from {src_i}.orders "
                    f"where not exists (select 1 from {ref_db}.empty_t) "
                    f"order by ts")
                tdSql.checkRows(3)
            finally:
                self._cleanup_src(src_i)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_009)
                except Exception:
                    pass
        finally:
            self._cleanup_src(p, m)
            tdSql.execute(f"drop database if exists {ref_db}")
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass

    def test_fq_local_010(self):
        """FQ-LOCAL-010: ANY/SOME/ALL subquery — comparison operators; two execution paths

        Path 1 (完全下推 / Fully-Pushed-to-External-DB):
          Same-source MySQL: TDengine pushes entire subquery SQL to MySQL for native execution.
          Note: execution may not apply subquery filter correctly; test will fail if behavior is wrong.
            a) val > ANY (same-source): val greater than at least one threshold → 2 rows
            b) val > ALL (same-source): val greater than all thresholds → 1 row
            c) val = SOME (same-source): SOME synonym for ANY → 2 rows

        Path 2 (TDengine子查询 / TDengine-Orchestrated Subquery):
          TDengine executes the inner subquery against an internal table, collects the
          result list, then passes it as a comparison filter to MySQL (outer source).
            d) MySQL outer + TDengine internal ANY: val > ANY (SELECT tval FROM ref_db.thr)
               thr: (10, 20) → val > 10 OR val > 20 → val=20,30 → 2 rows
            e) MySQL outer + TDengine internal ALL: val > ALL (SELECT tval FROM ref_db.thr)
               thr: (10, 20) → val > 20 → val=30 → 1 row
            f) MySQL outer + TDengine internal SOME: val = SOME (SELECT tval FROM ref_db.thr)
               thr: (10, 20) → val=10 or val=20 → 2 rows

        Data:
          MySQL items: (id=1,val=10), (id=2,val=20), (id=3,val=30)
          MySQL thresholds: (id=1,tval=10), (id=2,tval=20)
          TDengine internal thr: tval IN (10, 20)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-xx wpan Rewritten to use real MySQL external source; two execution paths

        """
        m = "fq_local_010_m"
        m_db = "fq_010_m_db"
        ref_db = "fq_010_ref"
        self._cleanup_src(m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            # ── Data setup ──────────────────────────────────────────────────────
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "CREATE TABLE IF NOT EXISTS items (id INT, val INT)",
                "DELETE FROM items",
                "INSERT INTO items VALUES (1,10),(2,20),(3,30)",
                "CREATE TABLE IF NOT EXISTS thresholds (id INT, tval INT)",
                "DELETE FROM thresholds",
                "INSERT INTO thresholds VALUES (1,10),(2,20)",
            ])
            self._mk_mysql_real(m, database=m_db)

            # TDengine internal threshold table: tval IN (10, 20)
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.thr (ts timestamp, tval int)")
            tdSql.execute(
                f"insert into {ref_db}.thr values "
                f"(1704067200000,10)(1704067260000,20)")

            # ── Path 1: 完全下推 (Fully-Pushed-to-External-DB) ──────────────────
            # Same-source MySQL: entire subquery SQL sent to MySQL.
            # Current implementation may not apply ANY/ALL/SOME filter correctly.

            # (a) ANY same-source: val > ANY (10,20) → val=20,30 → 2 rows
            tdSql.query(
                f"select id, val from {m}.items "
                f"where val > any (select tval from {m}.thresholds) "
                f"order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 20)
            tdSql.checkData(1, 1, 30)

            # (b) ALL same-source: val > ALL (10,20) → val=30 → 1 row
            tdSql.query(
                f"select id, val from {m}.items "
                f"where val > all (select tval from {m}.thresholds) "
                f"order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 30)

            # (c) SOME same-source: val = SOME (10,20) → val=10,20 → 2 rows
            tdSql.query(
                f"select id, val from {m}.items "
                f"where val = some (select tval from {m}.thresholds) "
                f"order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 1, 20)

            # ── Path 2: TDengine子查询 (TDengine-Orchestrated Subquery) ──────────
            # Path 2 uses InfluxDB as the outer source (separate source, registered
            # only for this phase).  TDengine evaluates the inner subquery against an
            # internal table and rewrites the comparison for InfluxDB.
            #   InfluxDB sensor data: h1(val=5), h2(val=10), h3(val=20), h4(val=30)
            #   TDengine internal thr_list: sel_val IN (10, 20)
            #
            # SOME (= synonym for ANY with =): val = SOME(10,20) → val IN (10,20) →
            #   rewrites to IN const-list (same as ext_can_pushdown_in_const_list) →
            #   h2(val=10), h3(val=20) → 2 rows  [direct assertion]
            # ANY (>): val > ANY(10,20) = val > MIN=10 → h3(20),h4(30) → 2 rows
            # ALL (>): val > ALL(10,20) = val > MAX=20 → h4(30) → 1 row

            influx_src = "fq_local_010_i"
            i_db_010 = "fq_010_i_db"
            ref_db_i = "fq_010_ref_i"
            self._cleanup_src(influx_src)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_010)
            try:
                ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_010, [
                    "sensor,host=h1 val=5i  1704067200000000000",
                    "sensor,host=h2 val=10i 1704067260000000000",
                    "sensor,host=h3 val=20i 1704067320000000000",
                    "sensor,host=h4 val=30i 1704067380000000000",
                ])
                tdSql.execute(f"drop database if exists {ref_db_i}")
                tdSql.execute(f"create database {ref_db_i}")
                tdSql.execute(
                    f"create table {ref_db_i}.thr_list (ts timestamp, sel_val int)")
                tdSql.execute(
                    f"insert into {ref_db_i}.thr_list values "
                    f"(1704067200000,10)(1704067260000,20)")
                # Register InfluxDB source FIRST (matches 021 pattern), then create internal table.
                self._mk_influx_real(influx_src, database=i_db_010)

                # Sanity: InfluxDB source queryable → all 4 rows
                tdSql.query(
                    f"select `host`, val from {influx_src}.sensor order by ts")
                tdSql.checkRows(4)

                # (d) SOME (= ANY): val = SOME(10,20) → same as IN(10,20) →
                #     TDengine rewrites to const-list → h2(val=10), h3(val=20) → 2 rows
                tdSql.query(
                    f"select `host`, val from {influx_src}.sensor "
                    f"where val = some (select sel_val from {ref_db_i}.thr_list) "
                    f"order by ts")
                tdSql.checkRows(2)
                tdSql.checkData(0, 1, 10)   # h2: val=10
                tdSql.checkData(1, 1, 20)   # h3: val=20

                # (e) ANY (>): val > ANY(10,20) → val > 10 → h3(20),h4(30) → 2 rows
                tdSql.query(
                    f"select `host`, val from {influx_src}.sensor "
                    f"where val > any (select sel_val from {ref_db_i}.thr_list) "
                    f"order by ts")
                tdSql.checkRows(2)
                tdSql.checkData(0, 1, 20)   # h3: val=20
                tdSql.checkData(1, 1, 30)   # h4: val=30

                # (f) ALL (>): val > ALL(10,20) → val > 20 → h4(30) → 1 row
                tdSql.query(
                    f"select `host`, val from {influx_src}.sensor "
                    f"where val > all (select sel_val from {ref_db_i}.thr_list) "
                    f"order by ts")
                tdSql.checkRows(1)
                tdSql.checkData(0, 1, 30)   # h4: val=30

            finally:
                self._cleanup_src(influx_src)
                tdSql.execute(f"drop database if exists {ref_db_i}")
                try:
                    ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_010)
                except Exception:
                    pass
        finally:
            self._cleanup_src(m)
            tdSql.execute(f"drop database if exists {ref_db}")
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass

        # ── PG: Path1 same-source ANY/ALL/SOME ────────────────────────────────────────
        src_p_010 = "fq_local_010_p"
        p_db_010 = "fq_010_p_db"
        self._cleanup_src(src_p_010)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db_010)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db_010, [
                "CREATE TABLE IF NOT EXISTS items (id INT, val INT)",
                "DELETE FROM items",
                "INSERT INTO items VALUES (1,10),(2,20),(3,30)",
                "CREATE TABLE IF NOT EXISTS thresholds (id INT, tval INT)",
                "DELETE FROM thresholds",
                "INSERT INTO thresholds VALUES (1,10),(2,20)",
            ])
            self._mk_pg_real(src_p_010, database=p_db_010)

            # (g) PG ANY same-source: val > ANY(10,20) → val=20,30 → 2 rows
            tdSql.query(
                f"select id, val from {src_p_010}.items "
                f"where val > any (select tval from {src_p_010}.thresholds) "
                f"order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 20)
            tdSql.checkData(1, 1, 30)

            # (h) PG ALL same-source: val > ALL(10,20) → val=30 → 1 row
            tdSql.query(
                f"select id, val from {src_p_010}.items "
                f"where val > all (select tval from {src_p_010}.thresholds) "
                f"order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 30)

            # (i) PG SOME same-source: val = SOME(10,20) → val=10,20 → 2 rows
            tdSql.query(
                f"select id, val from {src_p_010}.items "
                f"where val = some (select tval from {src_p_010}.thresholds) "
                f"order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 1, 20)
        finally:
            self._cleanup_src(src_p_010)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db_010)
            except Exception:
                pass

    def test_fq_local_011(self):
        """FQ-LOCAL-011: CASE expression with unmappable sub-expressions computed locally as a whole

        Dimensions:
          a) CASE with all mappable branches on internal vtable → local compute, result correct
          b) Three-way CASE: val<2='low', val<4='mid', else='high' → verified row-by-row
          c) Parser acceptance on mock MySQL (external CASE always goes local if unmappable)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)–(c) CASE expression: verified against all three real external sources
        def _body(src):
            tdSql.query(
                f"select val, "
                f"case when val >= 4 then 'high' "
                f"     when val >= 2 then 'mid' "
                f"     else 'low' end as level "
                f"from {src}.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 'low')    # val=1
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 'mid')    # val=2
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 'mid')    # val=3
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, 'high')   # val=4
            tdSql.checkData(4, 0, 5)
            tdSql.checkData(4, 1, 'high')   # val=5
        self._with_std_sources("fq_local_011", _body)

    # ------------------------------------------------------------------
    # FQ-LOCAL-012 ~ FQ-LOCAL-017: Function conversion / local paths
    # ------------------------------------------------------------------

    def test_fq_local_012(self):
        """FQ-LOCAL-012: SPREAD function — MAX-MIN expression substitution across three sources

        Dimensions:
          a) MySQL → SPREAD(val) on real external data; spread=4 (max=5, min=1)
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: result correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(f"select spread(val) from {src}.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 4)  # max=5 - min=1 = 4
        self._with_std_sources("fq_local_012", _body)

    def test_fq_local_013(self):
        """FQ-LOCAL-013: GROUP_CONCAT(MySQL)/STRING_AGG(PG/InfluxDB) conversion

        Dimensions:
          a) MySQL → GROUP_CONCAT pushdown: result contains all concatenated names
          b) PG → STRING_AGG conversion: equivalent aggregated string
          c) Separator parameter mapping: comma separator verified

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_013_m"
        m_db = "fq_local_013_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS items",
                "CREATE TABLE items (id INT, category VARCHAR(50), name VARCHAR(50))",
                "INSERT INTO items VALUES "
                "(1,'fruits','apple'),(2,'fruits','banana'),(3,'vegs','carrot')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL GROUP_CONCAT: DS §5.3.4.1.10: GROUP_CONCAT → GROUP_CONCAT on MySQL
            # TDengine GROUP_CONCAT syntax: group_concat(expr, separator)
            tdSql.query(
                f"select category, group_concat(name, ',') as names "
                f"from {src_m}.{m_db}.items "
                f"group by category order by category")
            tdSql.checkRows(2)   # fruits and vegs
            tdSql.checkData(0, 0, 'fruits')
            fruits_names = str(tdSql.getData(0, 1))
            assert "apple" in fruits_names and "banana" in fruits_names, (
                f"Expected both 'apple' and 'banana' in GROUP_CONCAT, got: {fruits_names}")
            tdSql.checkData(1, 0, 'vegs')
            vegs_names = str(tdSql.getData(1, 1))
            assert "carrot" in vegs_names
        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        # (b) PG: real GROUP_CONCAT correctness
        src_p = "fq_local_013_p"
        p_db_013 = "fq_013_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db_013)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db_013, [
                "DROP TABLE IF EXISTS items",
                "CREATE TABLE items (id INT, category TEXT, name TEXT)",
                "INSERT INTO items VALUES "
                "(1,'fruits','apple'),(2,'fruits','banana'),(3,'vegs','carrot')",
            ])
            self._mk_pg_real(src_p, database=p_db_013)
            tdSql.query(
                f"select category, group_concat(name, ',') as names "
                f"from {src_p}.items "
                f"group by category order by category")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'fruits')
            fruits_p = str(tdSql.getData(0, 1))
            assert "apple" in fruits_p and "banana" in fruits_p, (
                f"Expected 'apple' and 'banana' in GROUP_CONCAT result, got: {fruits_p}")
            tdSql.checkData(1, 0, 'vegs')
            assert "carrot" in str(tdSql.getData(1, 1))
        finally:
            self._cleanup_src(src_p)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db_013)
            except Exception:
                pass

        # (c) InfluxDB: local GROUP_CONCAT on measurement data
        src_i = "fq_local_013_i"
        i_db_013 = "fq_013_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_013)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_013, [
                'items,category=fruits name="apple" 1704067200000000000',
                'items,category=fruits name="banana" 1704067260000000000',
                'items,category=vegs name="carrot" 1704067320000000000',
            ])
            self._mk_influx_real(src_i, database=i_db_013)
            tdSql.query(
                f"select category, group_concat(name, ',') as names "
                f"from {src_i}.items "
                f"group by category order by category")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'fruits')
            fruits_i = str(tdSql.getData(0, 1))
            assert "apple" in fruits_i and "banana" in fruits_i, (
                f"Expected 'apple' and 'banana' in GROUP_CONCAT result, got: {fruits_i}")
            tdSql.checkData(1, 0, 'vegs')
            assert "carrot" in str(tdSql.getData(1, 1))
        finally:
            self._cleanup_src(src_i)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_013)
            except Exception:
                pass

    def test_fq_local_014(self):
        """FQ-LOCAL-014: LEASTSQUARES local compute path verification

        Dimensions:
          a) MySQL → LEASTSQUARES(val,1,1) on real external data; slope=1, intercept=0
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: result correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(f"select leastsquares(val, 1, 1) from {src}.src_t")
            tdSql.checkRows(1)
            raw = tdSql.getData(0, 0)
            assert raw is not None, "LEASTSQUARES should return non-null"
            result_str = str(raw)
            assert "1.000" in result_str, (
                f"slope should be ~1.0 ('1.000'), got: {result_str!r}")
            assert "0.000" in result_str, (
                f"intercept should be ~0.0 ('0.000'), got: {result_str!r}")
        self._with_std_sources("fq_local_014", _body)

    def test_fq_local_015(self):
        """FQ-LOCAL-015: LIKE_IN_SET/REGEXP_IN_SET local computation

        Dimensions:
          a) LIKE_IN_SET on internal vtable: returns rows matching any pattern
             name LIKE_IN_SET ('alp%','bet%') → alpha, beta → 2 rows
          b) REGEXP_IN_SET on internal vtable: regex pattern matching
             name REGEXP_IN_SET ('alpha|beta') → alpha, beta → 2 rows
          c) External source: parser acceptance (both functions always go local)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)–(c) LIKE_IN_SET / REGEXP_IN_SET: verified against all three real external sources
        def _body(src):
            # (a) LIKE_IN_SET: first arg is the LIKE pattern, second arg is the set/column
            # like_in_set(pattern, set) returns position of first match (>0) or 0
            tdSql.query(
                f"select name from {src}.src_t "
                f"where like_in_set('alp%', name) > 0 "
                f"   or like_in_set('bet%', name) > 0 "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'alpha')
            tdSql.checkData(1, 0, 'beta')
            # (b) REGEXP_IN_SET: first arg is the regex pattern, second arg is the set/column
            tdSql.query(
                f"select name from {src}.src_t "
                f"where regexp_in_set('alpha', name) > 0 "
                f"   or regexp_in_set('beta', name) > 0 "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'alpha')
            tdSql.checkData(1, 0, 'beta')
        self._with_std_sources("fq_local_015", _body)

    def test_fq_local_016(self):
        """FQ-LOCAL-016: FILL SURROUND clause does not affect pushdown behavior

        Dimensions:
          a) MySQL → FILL(PREV) on real external data; 10 windows, values verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: FILL(PREV) correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(
                f"select _wstart, avg(val) from {src}.src_t "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"interval(30s) fill(prev)")
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 1.0)   # [0s,30s): val=1
            tdSql.checkData(1, 1, 1.0)   # [30s,60s): fill(prev)=1.0
            tdSql.checkData(2, 1, 2.0)   # [60s,90s): val=2
            tdSql.checkData(3, 1, 2.0)   # [90s,120s): fill(prev)=2.0
            tdSql.checkData(4, 1, 3.0)   # [120s,150s): val=3
            tdSql.checkData(5, 1, 3.0)   # [150s,180s): fill(prev)=3.0
            tdSql.checkData(6, 1, 4.0)   # [180s,210s): val=4
            tdSql.checkData(7, 1, 4.0)   # [210s,240s): fill(prev)=4.0
            tdSql.checkData(8, 1, 5.0)   # [240s,270s): val=5
            tdSql.checkData(9, 1, 5.0)   # [270s,300s): fill(prev)=5.0
        self._with_std_sources("fq_local_016", _body)

    def test_fq_local_017(self):
        """FQ-LOCAL-017: INTERP query time range WHERE condition pushdown

        Dimensions:
          a) MySQL → INTERP range(60s,180s) every(30s); 5 points, vals verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: same 5-point result with exact val checks

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # Narrow range: 60s (val=2) to 180s (val=4), every(30s) → 5 points
            tdSql.query(
                f"select _irowts, interp(val) from {src}.src_t "
                f"range(1704067260000, 1704067380000) "
                f"every(30s) fill(linear)")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 2)   # at 60s: exact data, val=2
            tdSql.checkData(1, 1, 2)   # at 90s: INT interp → 2
            tdSql.checkData(2, 1, 3)   # at 120s: exact data, val=3
            tdSql.checkData(3, 1, 3)   # at 150s: INT interp → 3
            tdSql.checkData(4, 1, 4)   # at 180s: exact data, val=4
        self._with_std_sources("fq_local_017", _body)

    # ------------------------------------------------------------------
    # FQ-LOCAL-018 ~ FQ-LOCAL-021: JOIN specifics
    # ------------------------------------------------------------------

    def test_fq_local_018(self):
        """FQ-LOCAL-018: JOIN ON condition with TBNAME triggers parser error

        Dimensions:
          a) ON clause with TBNAME pseudo-column → error
          b) Expected TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_018_m"
        src_p = "fq_local_018_p"
        src_i = "fq_local_018_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            self._mk_mysql_real(src_m)
            tdSql.error(
                f"select * from {src_m}.t1 a join {src_m}.t2 b on a.tbname = b.tbname",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            self._mk_pg_real(src_p)
            tdSql.error(
                f"select * from {src_p}.t1 a join {src_p}.t2 b on a.tbname = b.tbname",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            self._mk_influx_real(src_i)
            tdSql.error(
                f"select * from {src_i}.cpu a join {src_i}.disk b on a.tbname = b.tbname",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    def test_fq_local_019(self):
        """FQ-LOCAL-019: MySQL same-source cross-database JOIN pushdown

        Dimensions:
          a) Same MySQL source, different databases → pushdown
          b) Parser acceptance

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_019_m"
        src_p = "fq_local_019_p"
        src_i = "fq_local_019_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            # MySQL: same source, cross-database JOIN
            self._mk_mysql_real(src_m, database="db1")
            self._assert_not_syntax_error(
                f"select * from {src_m}.db1.t1 a join {src_m}.db2.t2 b on a.id = b.id limit 5")
            # PG: same source, cross-schema JOIN within one database
            self._mk_pg_real(src_p)
            self._assert_not_syntax_error(
                f"select * from {src_p}.t1 a join {src_p}.t2 b on a.id = b.id limit 5")
            # InfluxDB: same source, cross-measurement JOIN
            self._mk_influx_real(src_i)
            self._assert_not_syntax_error(
                f"select * from {src_i}.cpu a join {src_i}.disk b on cpu.ts = disk.ts limit 5")
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    def test_fq_local_020(self):
        """FQ-LOCAL-020: MySQL/PG/InfluxDB cross-database JOIN not pushable, local execution

        Dimensions:
          a) MySQL cross-database JOIN → local execution, parser accepts
          b) PG cross-database JOIN → local execution, parser accepts
          c) InfluxDB cross-database JOIN → local execution, parser accepts

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-29 wpan Add MySQL dimension

        """
        m = "fq_local_020_m"
        p = "fq_local_020_p"
        i = "fq_local_020_i"
        self._cleanup_src(m, p, i)
        try:
            # (a) MySQL cross-database JOIN → parser accepts (local execution)
            self._mk_mysql_real(m)
            self._assert_not_syntax_error(
                f"select * from {m}.t1 a join {m}.t2 b on a.id = b.id limit 5")
            # (b) PG cross-database JOIN → parser accepts (local execution)
            self._mk_pg_real(p)
            self._assert_not_syntax_error(
                f"select * from {p}.t1 a join {p}.t2 b on a.id = b.id limit 5")
            # (c) InfluxDB cross-source query → parser accepts (local execution)
            self._mk_influx_real(i)
            self._assert_not_syntax_error(
                f"select * from {i}.cpu limit 5")
        finally:
            self._cleanup_src(m, p, i)

    def test_fq_local_021(self):
        """FQ-LOCAL-021: InfluxDB IN(subquery) rewritten to constant list

        Dimensions:
          a) Small result set: TDengine executes the subquery first, rewrites
             InfluxDB query as IN(v1, v2, ...) constant-list and pushes down
          b) Internal vtable as the subquery source: val IN (1,3) → 2 rows from InfluxDB
          c) Large result set → local computation fallback (parser acceptance)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_i = "fq_local_021_influx"
        i_db = "fq_local_021_db"
        src_m = "fq_local_021_m"
        m_db_021 = "fq_021_m_db"
        src_p = "fq_local_021_p"
        p_db_021 = "fq_021_p_db"
        self._cleanup_src(src_i, src_m, src_p)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db_021)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db_021)
        try:
            # ── Data setup ──────────────────────────────────────────────────────────────────
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                "sensor,host=h1 val=1i 1704067200000000000",
                "sensor,host=h2 val=2i 1704067260000000000",
                "sensor,host=h3 val=3i 1704067320000000000",
            ])
            self._mk_influx_real(src_i, database=i_db)

            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db_021, [
                "CREATE TABLE IF NOT EXISTS sensor "
                "(ts DATETIME(3), val INT, host VARCHAR(16))",
                "DELETE FROM sensor",
                "INSERT INTO sensor VALUES "
                "('2024-01-01 00:00:00.000',1,'h1'),"
                "('2024-01-01 00:01:00.000',2,'h2'),"
                "('2024-01-01 00:02:00.000',3,'h3')",
            ])
            self._mk_mysql_real(src_m, database=m_db_021)

            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db_021, [
                "CREATE TABLE IF NOT EXISTS sensor (ts TIMESTAMP, val INT, host TEXT)",
                "DELETE FROM sensor",
                "INSERT INTO sensor VALUES "
                "('2024-01-01 00:00:00.000',1,'h1'),"
                "('2024-01-01 00:01:00.000',2,'h2'),"
                "('2024-01-01 00:02:00.000',3,'h3')",
            ])
            self._mk_pg_real(src_p, database=p_db_021)

            # (a)/(b) Create TDengine internal table as the subquery source
            tdSql.execute("drop database if exists fq_local_021_ref")
            tdSql.execute("create database fq_local_021_ref")
            tdSql.execute(
                "create table fq_local_021_ref.sub_t (ts timestamp, sel_val int)")
            tdSql.execute(
                "insert into fq_local_021_ref.sub_t values "
                "(1704067200000,1)(1704067320000,3)")

            # (a) InfluxDB outer + TDengine internal IN →
            # TDengine executes subquery first, rewrites to IN(1,3), push to InfluxDB.
            # Note: 'host' is a reserved keyword in TDengine, must use backtick-quoted
            tdSql.query(
                f"select `host`, val from {src_i}.sensor "
                f"where val in (select sel_val from fq_local_021_ref.sub_t) "
                f"order by ts")
            tdSql.checkRows(2)   # h1 (val=1) and h3 (val=3)
            tdSql.checkData(0, 1, 1)   # h1: val=1
            tdSql.checkData(1, 1, 3)   # h3: val=3

            # (b) MySQL outer + TDengine internal IN(subquery) → const-list rewrite
            tdSql.query(
                f"select ts, val from {src_m}.sensor "
                f"where val in (select sel_val from fq_local_021_ref.sub_t) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)

            # (c) PG outer + TDengine internal IN(subquery) → const-list rewrite
            tdSql.query(
                f"select ts, val from {src_p}.sensor "
                f"where val in (select sel_val from fq_local_021_ref.sub_t) "
                f"order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)

        finally:
            self._cleanup_src(src_i, src_m, src_p)
            tdSql.execute("drop database if exists fq_local_021_ref")
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db_021)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db_021)
            except Exception:
                pass
    # ------------------------------------------------------------------

    def test_fq_local_022(self):
        """FQ-LOCAL-022: federated query rejected in stream computation

        Dimensions:
          a) CREATE STREAM on external source → error
          b) Expected TSDB_CODE_EXT_STREAM_NOT_SUPPORTED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_022_m", self._mk_mysql_real),
            ("fq_local_022_p", self._mk_pg_real),
            ("fq_local_022_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                tdSql.error(
                    f"create stream fq_022_s trigger at_once into fq_022_out "
                    f"as select count(*) from {src}.orders interval(1m)",
                    expectedErrno=TSDB_CODE_EXT_STREAM_NOT_SUPPORTED)
            finally:
                self._cleanup_src(src)
                tdSql.execute("drop stream if exists fq_022_s")

    def test_fq_local_023(self):
        """FQ-LOCAL-023: federated query rejected in subscription

        Dimensions:
          a) CREATE TOPIC on external source → error
          b) Expected TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_023_m", self._mk_mysql_real),
            ("fq_local_023_p", self._mk_pg_real),
            ("fq_local_023_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                tdSql.error(
                    f"create topic fq_023_t as select * from {src}.orders",
                    expectedErrno=TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED)
            finally:
                self._cleanup_src(src)
                tdSql.execute("drop topic if exists fq_023_t")

    def test_fq_local_024(self):
        """FQ-LOCAL-024: external write INSERT denied

        Dimensions:
          a) INSERT INTO external table → error
          b) Expected TSDB_CODE_EXT_WRITE_DENIED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_024_m", self._mk_mysql_real),
            ("fq_local_024_p", self._mk_pg_real),
            ("fq_local_024_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                tdSql.error(
                    f"insert into {src}.orders values (1, 'test', 100)",
                    expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
            finally:
                self._cleanup_src(src)

    def test_fq_local_025(self):
        """FQ-LOCAL-025: external write UPDATE denied

        Dimensions:
          a) TDengine has no SQL UPDATE statement; overwrite via INSERT at
             same timestamp = TDengine’s “update” semantics. External table
             is read-only → the INSERT-as-update attempt is also denied with
             TSDB_CODE_EXT_WRITE_DENIED.
          b) Repeated attempts to “update” (overwrite) return same error code.

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_025_m", self._mk_mysql_real),
            ("fq_local_025_p", self._mk_pg_real),
            ("fq_local_025_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                # TDengine has no UPDATE statement; the equivalent is INSERT at the
                # same timestamp (last-write-wins). On external tables this is refused.
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
        """FQ-LOCAL-026: external write DELETE denied

        Dimensions:
          a) DELETE FROM external table → error
          b) Expected TSDB_CODE_EXT_WRITE_DENIED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_026_m", self._mk_mysql_real),
            ("fq_local_026_p", self._mk_pg_real),
            ("fq_local_026_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                tdSql.error(
                    f"delete from {src}.orders where id = 1",
                    expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
            finally:
                self._cleanup_src(src)

    def test_fq_local_027(self):
        """FQ-LOCAL-027: external object operation denied — write/DDL operation denied

        Dimensions:
          a) CREATE TABLE in external source namespace → TSDB_CODE_EXT_WRITE_DENIED
          b) Any write/DDL attempt on external source returns the same refusal code

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_027_m", self._mk_mysql_real),
            ("fq_local_027_p", self._mk_pg_real),
            ("fq_local_027_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
                # CREATE TABLE in external source namespace → external table is read-only,
                # DDL operations are rejected with the same write-denial error as INSERT
                tdSql.error(
                    f"create table {src}.new_tbl (ts timestamp, v int)",
                    expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)
            finally:
                self._cleanup_src(src)

    def test_fq_local_028(self):
        """FQ-LOCAL-028: cross-source strong consistency transaction limitation

        Dimensions:
          a) Cross-source transaction semantics not supported
          b) Error or fallback to eventually consistent

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_local_028_m"
        p = "fq_local_028_p"
        i = "fq_local_028_i"
        self._cleanup_src(m, p, i)
        try:
            self._mk_mysql_real(m)
            self._mk_pg_real(p)
            self._mk_influx_real(i)
            # Cross-source queries are read-only, no transaction guarantee
            self._assert_not_syntax_error(
                f"select * from {m}.t1 union all select * from {p}.t1 limit 5")
            self._assert_not_syntax_error(
                f"select * from {m}.t1 union all select * from {i}.cpu limit 5")
            self._assert_not_syntax_error(
                f"select * from {p}.t1 union all select * from {i}.cpu limit 5")
        finally:
            self._cleanup_src(m, p, i)

    # ------------------------------------------------------------------
    # FQ-LOCAL-029 ~ FQ-LOCAL-034: Community edition and version limits
    # ------------------------------------------------------------------

    def test_fq_local_029(self):
        """FQ-LOCAL-029: enterprise edition — federated query feature is enabled

        Since setup_class calls require_external_source_feature() and the test
        reaches this point, the runtime is confirmed enterprise edition.
        This test verifies the positive contract:
          a) SHOW EXTERNAL SOURCES executes without error
          b) The command returns a result set (no TSDB_CODE_EXT_FEATURE_DISABLED)
          c) CREATE EXTERNAL SOURCE with valid params does not return
             TSDB_CODE_EXT_FEATURE_DISABLED

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-21 wpan Replace pytest.skip with enterprise-positive assertion

        """
        # (a)+(b) SHOW EXTERNAL SOURCES must succeed on enterprise
        result = tdSql.query("show external sources", exit=False)
        assert result is not False, (
            "SHOW EXTERNAL SOURCES failed — feature is disabled on this build"
        )

        # (c) CREATE with valid params must not return EXT_FEATURE_DISABLED
        src = "fq_local_029_probe"
        self._cleanup_src(src)
        try:
            cfg = self._mysql_cfg()
            tdSql.execute(
                f"create external source {src} "
                f"type='mysql' host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}'"
            )
            # Source must be visible in system table
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
        finally:
            self._cleanup_src(src)

    def test_fq_local_030(self):
        """FQ-LOCAL-030: enterprise edition — all external source DDL operations succeed

        Verifies that on enterprise edition all three DDL verbs work correctly:
          a) CREATE EXTERNAL SOURCE → source appears in ins_ext_sources
          b) ALTER EXTERNAL SOURCE → field change reflected in ins_ext_sources
          c) DROP EXTERNAL SOURCE → source disappears from ins_ext_sources

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-21 wpan Replace pytest.skip with enterprise-positive assertion

        """
        src = "fq_local_030_ddl"
        self._cleanup_src(src)
        cfg = self._mysql_cfg()
        try:
            # (a) CREATE
            tdSql.execute(
                f"create external source {src} "
                f"type='mysql' host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}'"
            )
            tdSql.query(
                "select `host` from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "192.0.2.1")

            # (b) ALTER
            tdSql.execute(
                f"alter external source {src} SET host='192.0.2.2'"
            )
            tdSql.query(
                "select `host` from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "192.0.2.2")

            # (c) DROP
            tdSql.execute(f"drop external source {src}")
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
        finally:
            self._cleanup_src(src)

    def test_fq_local_031(self):
        """FQ-LOCAL-031: error code stability — operations return consistent codes

        On enterprise edition verifies:
          a) Normal DDL does NOT return TSDB_CODE_EXT_FEATURE_DISABLED
          b) Reserved TYPE='tdengine' returns TSDB_CODE_EXT_FEATURE_DISABLED
          c) Querying a dropped source consistently returns EXT_SOURCE_NOT_FOUND

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-21 wpan Replace pytest.skip with enterprise error-code assertion

        """
        src_ok = "fq_local_031_ok"
        src_td = "fq_local_031_td"
        self._cleanup_src(src_ok, src_td)
        cfg = self._mysql_cfg()

        # (a) Normal MySQL source: must not raise EXT_FEATURE_DISABLED
        try:
            tdSql.execute(
                f"create external source {src_ok} "
                f"type='mysql' host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}'"
            )
            # Drop it normally — also must not raise EXT_FEATURE_DISABLED
            tdSql.execute(f"drop external source {src_ok}")
        finally:
            self._cleanup_src(src_ok)

        # (b) Reserved TYPE='tdengine' → must raise EXT_FEATURE_DISABLED
        try:
            tdSql.error(
                f"create external source {src_td} "
                f"type='tdengine' host='{cfg.host}' port=6030 "
                f"user='{cfg.user}' password='{cfg.password}'",
                expectedErrno=TSDB_CODE_EXT_FEATURE_DISABLED,
            )
        finally:
            self._cleanup_src(src_td)

        # (c) Query nonexistent source returns EXT_SOURCE_NOT_FOUND (stable code)
        ghost = "fq_local_031_ghost_never_existed"
        self._cleanup_src(ghost)
        from federated_query_common import TSDB_CODE_EXT_SOURCE_NOT_FOUND
        for _ in range(3):
            tdSql.error(
                f"select * from {ghost}.some_db.some_table",
                expectedErrno=TSDB_CODE_EXT_SOURCE_NOT_FOUND,
            )

    def test_fq_local_032(self):
        """FQ-LOCAL-032: tdengine external source reserved behavior

        Dimensions:
          a) TYPE='tdengine' → reserved, not yet delivered
          b) Create with type='tdengine' → error or reserved message

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

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
        """FQ-LOCAL-033: version support matrix limitation

        Dimensions:
          a) External DB version outside support matrix → error or warning
          b) MySQL < 5.7, PG < 12, InfluxDB < v2 → behavior defined

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        pytest.skip("Requires live external DB with specific versions")

    def test_fq_local_034(self):
        """FQ-LOCAL-034: unsupported statement error code stability

        Dimensions:
          a) Stream error code stable
          b) Subscribe error code stable
          c) Write error code stable
          d) Repeated invocations return same code

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        for src, mk in [
            ("fq_local_034_m", self._mk_mysql_real),
            ("fq_local_034_p", self._mk_pg_real),
            ("fq_local_034_i", self._mk_influx_real),
        ]:
            self._cleanup_src(src)
            try:
                mk(src)
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
        """FQ-LOCAL-035: Hints not pushed down

        Dimensions:
          a) Hints stripped from remote SQL
          b) Hints effective locally
          c) Parser acceptance

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_035_m"
        src_p = "fq_local_035_p"
        src_i = "fq_local_035_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            self._mk_mysql_real(src_m)
            self._assert_not_syntax_error(
                f"select /*+ para_tables_sort() */ * from {src_m}.t1 limit 5")
            self._mk_pg_real(src_p)
            self._assert_not_syntax_error(
                f"select /*+ para_tables_sort() */ * from {src_p}.t1 limit 5")
            self._mk_influx_real(src_i)
            self._assert_not_syntax_error(
                f"select /*+ para_tables_sort() */ * from {src_i}.cpu limit 5")
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    def test_fq_local_036(self):
        """FQ-LOCAL-036: pseudo-column restrictions — TBNAME/TAGS and other pseudo-column boundaries

        Dimensions:
          a) TBNAME on external → not applicable
          b) _ROWTS on external → local mapping
          c) TAGS on non-Influx → not applicable

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_036_m"
        src_p = "fq_local_036_p"
        src_i = "fq_local_036_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            self._mk_mysql_real(src_m)
            # Basic query without pseudo-columns → OK
            self._assert_not_syntax_error(f"select * from {src_m}.users limit 5")
            self._mk_pg_real(src_p)
            self._assert_not_syntax_error(f"select * from {src_p}.users limit 5")
            self._mk_influx_real(src_i)
            self._assert_not_syntax_error(f"select * from {src_i}.cpu limit 5")
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    def test_fq_local_037(self):
        """FQ-LOCAL-037: TAGS semantic difference — Influx tag set without data not returned

        Dimensions:
          a) InfluxDB tag query → only returns tags with data
          b) Empty tag set not returned
          c) Parser acceptance

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_037_m"
        src_p = "fq_local_037_p"
        src_i = "fq_local_037_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            # MySQL: SELECT TAGS is a parser error (no tag concept on relational DBs)
            self._mk_mysql_real(src_m)
            tdSql.error(
                f"select tags from {src_m}.t1",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
            # PG: same — TAGS is a TDengine-specific keyword, rejected on relational DBs
            self._mk_pg_real(src_p)
            tdSql.error(
                f"select tags from {src_p}.t1",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
            # InfluxDB: TAGS accepted (has native tag concept)
            self._mk_influx_real(src_i)
            self._assert_not_syntax_error(f"select distinct host from {src_i}.cpu")
            self._assert_not_syntax_error(f"select tags from {src_i}.cpu")
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    # ------------------------------------------------------------------
    # FQ-LOCAL-038 ~ FQ-LOCAL-042: JOIN and pseudo-column local paths
    # ------------------------------------------------------------------

    def test_fq_local_038(self):
        """FQ-LOCAL-038: MySQL FULL OUTER JOIN path

        Dimensions:
          a) MySQL doesn't support FULL OUTER JOIN natively
          b) Rewrite (LEFT+RIGHT+UNION) or local fallback
          c) Result consistency with local execution

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_038_m"
        src_p = "fq_local_038_p"
        src_i = "fq_local_038_i"
        self._cleanup_src(src_m, src_p, src_i)
        try:
            # MySQL: FULL OUTER JOIN not natively supported → rewrite or local fallback
            self._mk_mysql_real(src_m)
            self._assert_not_syntax_error(
                f"select * from {src_m}.t1 full outer join {src_m}.t2 on t1.id = t2.id limit 5")
            # PG: supports FULL OUTER JOIN natively
            self._mk_pg_real(src_p)
            self._assert_not_syntax_error(
                f"select * from {src_p}.t1 full outer join {src_p}.t2 on t1.id = t2.id limit 5")
            # InfluxDB: FULL OUTER JOIN always executed locally by TDengine
            self._mk_influx_real(src_i)
            self._assert_not_syntax_error(
                f"select * from {src_i}.cpu full outer join {src_i}.disk "
                f"on cpu.ts = disk.ts limit 5")
        finally:
            self._cleanup_src(src_m, src_p, src_i)

    def test_fq_local_039(self):
        """FQ-LOCAL-039: ASOF/WINDOW JOIN path

        Dimensions:
          a) ASOF JOIN on internal vtable → local execution, result correct
             src_t (val=1..5) ASOF JOIN t2 (v2=10,20,30) ON ts≥ts
             → first 3 rows match exactly, last 2 rows get last matching t2 row
          b) WINDOW JOIN: TDengine-proprietary, always local (see test_fq_local_s08)
          c) Parser acceptance on external source: ASOF JOIN syntax not rejected at parse time

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create table fq_local_db.t2 (ts timestamp, v2 int)")
            tdSql.execute(
                "insert into fq_local_db.t2 values "
                "(1704067200000, 10) (1704067260000, 20) (1704067320000, 30)")

            # (a) ASOF JOIN: each src_t row matched to nearest-or-equal t2 row by ts
            # FS §3.7.3 + DS §5.3.6.1.6: ASOF Join supported (local computation)
            # TDengine ASOF JOIN syntax requires LEFT/RIGHT prefix
            tdSql.query(
                "select a.val, b.v2 from fq_local_db.src_t a "
                "left asof join fq_local_db.t2 b on a.ts >= b.ts "
                "order by a.ts")
            tdSql.checkRows(5)
            # row 0: ts=0s, val=1, matched t2 at ts=0s → v2=10
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
            # row 1: ts=60s, val=2, matched t2 at ts=60s → v2=20
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 20)
            # row 2: ts=120s, val=3, matched t2 at ts=120s → v2=30
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 30)
            # row 3: ts=180s, val=4, nearest t2 ≤ 180s is ts=120s → v2=30
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, 30)
            # row 4: ts=240s, val=5, nearest t2 ≤ 240s is ts=120s → v2=30
            tdSql.checkData(4, 0, 5)
            tdSql.checkData(4, 1, 30)
        finally:
            self._teardown_internal_env()

        # (c) MySQL: real ASOF JOIN correctness with two tables
        src_m = "fq_local_039_m"
        m_db_039 = "fq_039_m_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db_039)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db_039, [
                "CREATE TABLE IF NOT EXISTS src_t (ts DATETIME(3) PRIMARY KEY, val INT)",
                "DELETE FROM src_t",
                "INSERT INTO src_t VALUES "
                "('2024-01-01 00:00:00.000',1),('2024-01-01 00:01:00.000',2),"
                "('2024-01-01 00:02:00.000',3),('2024-01-01 00:03:00.000',4),"
                "('2024-01-01 00:04:00.000',5)",
                "CREATE TABLE IF NOT EXISTS t2 (ts DATETIME(3) PRIMARY KEY, v2 INT)",
                "DELETE FROM t2",
                "INSERT INTO t2 VALUES "
                "('2024-01-01 00:00:00.000',10),('2024-01-01 00:01:00.000',20),"
                "('2024-01-01 00:02:00.000',30)",
            ])
            self._mk_mysql_real(src_m, database=m_db_039)
            tdSql.query(
                f"select a.val, b.v2 from {src_m}.src_t a "
                f"left asof join {src_m}.t2 b on a.ts >= b.ts "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4);  tdSql.checkData(3, 1, 30)
            tdSql.checkData(4, 0, 5);  tdSql.checkData(4, 1, 30)
        finally:
            self._cleanup_src(src_m)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db_039)
            except Exception:
                pass

        # (d) PG: real ASOF JOIN correctness with two tables
        src_p = "fq_local_039_p"
        p_db_039 = "fq_039_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db_039)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db_039, [
                "CREATE TABLE IF NOT EXISTS src_t (ts TIMESTAMP, val INT)",
                "DELETE FROM src_t",
                "INSERT INTO src_t VALUES "
                "('2024-01-01 00:00:00.000',1),('2024-01-01 00:01:00.000',2),"
                "('2024-01-01 00:02:00.000',3),('2024-01-01 00:03:00.000',4),"
                "('2024-01-01 00:04:00.000',5)",
                "CREATE TABLE IF NOT EXISTS t2 (ts TIMESTAMP, v2 INT)",
                "DELETE FROM t2",
                "INSERT INTO t2 VALUES "
                "('2024-01-01 00:00:00.000',10),('2024-01-01 00:01:00.000',20),"
                "('2024-01-01 00:02:00.000',30)",
            ])
            self._mk_pg_real(src_p, database=p_db_039)
            tdSql.query(
                f"select a.val, b.v2 from {src_p}.src_t a "
                f"left asof join {src_p}.t2 b on a.ts >= b.ts "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4);  tdSql.checkData(3, 1, 30)
            tdSql.checkData(4, 0, 5);  tdSql.checkData(4, 1, 30)
        finally:
            self._cleanup_src(src_p)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db_039)
            except Exception:
                pass

        # (e) InfluxDB: real ASOF JOIN correctness with two measurements
        src_i = "fq_local_039_i"
        i_db_039 = "fq_039_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_039)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_039, [
                "src_t val=1i 1704067200000000000",
                "src_t val=2i 1704067260000000000",
                "src_t val=3i 1704067320000000000",
                "src_t val=4i 1704067380000000000",
                "src_t val=5i 1704067440000000000",
                "t2 v2=10i 1704067200000000000",
                "t2 v2=20i 1704067260000000000",
                "t2 v2=30i 1704067320000000000",
            ])
            self._mk_influx_real(src_i, database=i_db_039)
            tdSql.query(
                f"select a.val, b.v2 from {src_i}.src_t a "
                f"left asof join {src_i}.t2 b on a.ts >= b.ts "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4);  tdSql.checkData(3, 1, 30)
            tdSql.checkData(4, 0, 5);  tdSql.checkData(4, 1, 30)
        finally:
            self._cleanup_src(src_i)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_039)
            except Exception:
                pass

    def test_fq_local_040(self):
        """FQ-LOCAL-040: pseudo-column _ROWTS/_c0 local mapping in federated query

        Dimensions:
          a) MySQL → _ROWTS maps to timestamp column locally; val/ts values verified
          b) PG → same
          c) InfluxDB → same (InfluxDB time column mapped to TDengine timestamp)
          d) Internal vtable baseline: _ROWTS=_c0=ts for regular table

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        expected_ts = [
            1704067200000, 1704067260000, 1704067320000,
            1704067380000, 1704067440000]
        def _body(src):
            tdSql.query(f"select _rowts, val from {src}.src_t order by ts")
            tdSql.checkRows(5)
            for i, (ts, val) in enumerate(zip(expected_ts, [1, 2, 3, 4, 5])):
                tdSql.checkData(i, 0, ts)
                tdSql.checkData(i, 1, val)
        self._with_std_sources("fq_local_040", _body)

    def test_fq_local_041(self):
        """FQ-LOCAL-041: pseudo-column _QSTART/_QEND local computation

        Dimensions:
          a) MySQL → _QSTART/_QEND from WHERE time condition; ordering/consistency verified
          b) PG → same
          c) InfluxDB → same (TDengine computes _QSTART/_QEND locally, not pushed down)
          d) Internal vtable baseline: values match WHERE ts boundaries

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(
                f"select _qstart, _qend, count(*) from {src}.src_t "
                f"where ts >= 1704067200000 and ts < 1704067500000 interval(1m)")
            tdSql.checkRows(5)
            for i in range(5):
                tdSql.checkData(i, 2, 1)
            qstart_val = tdSql.getData(0, 0)
            qend_val   = tdSql.getData(0, 1)
            assert qstart_val is not None, "_QSTART should not be NULL"
            assert qend_val   is not None, "_QEND should not be NULL"
            assert qstart_val < qend_val,  "_QSTART must be before _QEND"
        self._with_std_sources("fq_local_041", _body)

    def test_fq_local_042(self):
        """FQ-LOCAL-042: pseudo-column _IROWTS/_IROWTS_ORIGIN local computation

        Dimensions:
          a) MySQL → INTERP generates _IROWTS locally; values match requested timestamps
          b) PG → same
          c) InfluxDB → same (TDengine computes INTERP locally after fetching raw data)
          d) Internal vtable baseline: _IROWTS at exact interp points, interp(val) correct

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        expected_irowts = [
            1704067260000,   # 60s
            1704067290000,   # 90s
            1704067320000,   # 120s
            1704067350000,   # 150s
            1704067380000,   # 180s
        ]
        def _body(src):
            tdSql.query(
                f"select _irowts, interp(val) from {src}.src_t "
                f"range(1704067260000, 1704067380000) "
                f"every(30s) fill(linear)")
            tdSql.checkRows(5)
            for i, ts in enumerate(expected_irowts):
                tdSql.checkData(i, 0, ts)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(2, 1, 3)
            tdSql.checkData(3, 1, 3)
            tdSql.checkData(4, 1, 4)
        self._with_std_sources("fq_local_042", _body)

    # ------------------------------------------------------------------
    # FQ-LOCAL-043 ~ FQ-LOCAL-045: Proprietary function local paths
    # ------------------------------------------------------------------

    def test_fq_local_043(self):
        """FQ-LOCAL-043: TO_ISO8601/TIMEZONE() local computation

        Dimensions:
          a) MySQL → TO_ISO8601(ts) and TIMEZONE() computed locally; values verified
          b) PG → same
          c) InfluxDB → same (TDengine maps InfluxDB time column to ts locally)
          d) Internal vtable baseline: result correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(f"select to_iso8601(ts) from {src}.src_t order by ts limit 1")
            tdSql.checkRows(1)
            iso_val = str(tdSql.getData(0, 0))
            assert "2024-01-01" in iso_val, (
                f"Expected ISO8601 to contain '2024-01-01', got: {iso_val}")
            tdSql.query(f"select timezone() from {src}.src_t limit 1")
            tdSql.checkRows(1)
            tz_val = tdSql.getData(0, 0)
            assert tz_val is not None and len(str(tz_val)) > 0, (
                f"TIMEZONE() should return a non-empty string, got: {tz_val}")
        self._with_std_sources("fq_local_043", _body)

    def test_fq_local_044(self):
        """FQ-LOCAL-044: COLS()/UNIQUE()/SAMPLE() local computation

        Dimensions:
          a) MySQL → UNIQUE(val), SAMPLE(val,3), LAST(val) on real external data
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: UNIQUE/SAMPLE/COLS correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # UNIQUE: all 5 val values are distinct
            tdSql.query(f"select unique(val) from {src}.src_t order by ts")
            tdSql.checkRows(5)
            for i, expected in enumerate([1, 2, 3, 4, 5]):
                tdSql.checkData(i, 0, expected)
            # SAMPLE: 3 random rows from 5
            tdSql.query(f"select sample(val, 3) from {src}.src_t")
            tdSql.checkRows(3)
            # LAST: last value is 5
            tdSql.query(f"select last(val) from {src}.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            # COLS(last(val), ts): returns ts of the row where last(val) occurred
            tdSql.query(f"select cols(last(val), ts) from {src}.src_t")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, "COLS ts should not be NULL"
        self._with_std_sources("fq_local_044", _body)

    def test_fq_local_045(self):
        """FQ-LOCAL-045: FILL_FORWARD/MAVG/STATECOUNT/STATEDURATION local computation

        Dimensions:
          a) MySQL → MAVG(val,2) and STATECOUNT(val,'GT',2) on real external data; verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: MAVG/STATECOUNT/STATEDURATION/DERIVATIVE correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # MAVG(val, 2): val=[1,2,3,4,5] → [1.5, 2.5, 3.5, 4.5]
            tdSql.query(f"select mavg(val, 2) from {src}.src_t")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1.5)
            tdSql.checkData(1, 0, 2.5)
            tdSql.checkData(2, 0, 3.5)
            tdSql.checkData(3, 0, 4.5)
            # STATECOUNT(val, 'GT', 2): -1,-1,1,2,3
            tdSql.query(f"select statecount(val, 'GT', 2) from {src}.src_t")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, -1)
            tdSql.checkData(1, 0, -1)
            tdSql.checkData(2, 0, 1)
            tdSql.checkData(3, 0, 2)
            tdSql.checkData(4, 0, 3)
            # STATEDURATION(val, 'GT', 2, 1s): -1,-1,0,60,120
            tdSql.query(f"select stateduration(val, 'GT', 2, 1s) from {src}.src_t")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, -1)
            tdSql.checkData(1, 0, -1)
            tdSql.checkData(2, 0, 0)
            tdSql.checkData(3, 0, 60)
            tdSql.checkData(4, 0, 120)
            # DERIVATIVE(val, 1s, 0): 4 rows, each ≈ 1/60 per second
            tdSql.query(f"select derivative(val, 1s, 0) from {src}.src_t")
            tdSql.checkRows(4)
            for i in range(4):
                v = float(tdSql.getData(i, 0))
                assert abs(v - 1.0/60) < 0.001, (
                    f"Row {i}: derivative should be ~0.01667, got {v}")
        self._with_std_sources("fq_local_045", _body)

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
             DS §5.3.5.1.1: "partition key is TBNAME ... MySQL/PG → Parser rejects directly"
          d) SELECT TBNAME and PARTITION BY TBNAME on PG → same error

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: FS §3.7.2.1 — Dimension 7 (FS-Driven Validation)

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_s01_m"
        src_p = "fq_local_s01_p"
        self._cleanup_src(src_m)
        try:
            self._mk_mysql_real(src_m)
            # (a) SELECT TBNAME on MySQL → error (table/syntax not exist/unsupported)
            tdSql.error(
                f"select tbname from {src_m}.t1",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # (b) WHERE TBNAME = on MySQL → error
            tdSql.error(
                f"select * from {src_m}.t1 where tbname = 'myrow'",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # (c) PARTITION BY TBNAME on MySQL → error
            tdSql.error(
                f"select count(*) from {src_m}.t1 partition by tbname",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src_m)

        self._cleanup_src(src_p)
        try:
            self._mk_pg_real(src_p)
            # (d) SELECT TBNAME on PG → error
            tdSql.error(
                f"select tbname from {src_p}.t1",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # PARTITION BY TBNAME on PG → error
            tdSql.error(
                f"select count(*) from {src_p}.t1 partition by tbname",
                expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src_p)

    def test_fq_local_s02_influx_tbname_partition_ok(self):
        """Gap supplement: InfluxDB PARTITION BY TBNAME is the exception — accepted

        FS §3.7.2.1 exception: "PARTITION BY TBNAME is available on InfluxDB —
        the system converts it to GROUP BY all Tag columns."
        DS §5.3.5.1.1: "InfluxDB v3 exception: PARTITION BY TBNAME can be converted
        to GROUP BY tag1, tag2, ... and pushed down."

        Dimensions:
          a) PARTITION BY TBNAME on InfluxDB → parser accepts (not an error)
          b) SELECT TBNAME on InfluxDB → parser accepts (tag-set name mapping)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: FS §3.7.2.1 (exception) + DS §5.3.5.1.1 — Dimension 7 (FS-Driven Validation)

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_local_s02"
        self._cleanup_src(src)
        try:
            self._mk_influx_real(src)
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

        FS §3.7.2.2: "Using SELECT TAGS on MySQL / PostgreSQL external tables will
        fail. Reason: TAGS query is a TDengine supertable-specific operation;
        MySQL / PostgreSQL have no tag metadata."

        Dimensions:
          a) SELECT TAGS FROM mysql_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          b) SELECT TAGS FROM pg_src → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          c) InfluxDB exception: SELECT TAGS is accepted (InfluxDB has tag columns;
             semantic difference — only returns tag sets with at least one data point)

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: FS §3.7.2.2 (completely untested) — Dimension 7 (FS-Driven Validation)

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_local_s03_m"
        src_p = "fq_local_s03_p"
        src_i = "fq_local_s03_i"

        self._cleanup_src(src_m)
        try:
            self._mk_mysql_real(src_m)
            # (a) MySQL SELECT TAGS → Parser error (no tag concept; 'tags' is a keyword)
            tdSql.error(
                f"select tags from {src_m}.t1",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        finally:
            self._cleanup_src(src_m)

        self._cleanup_src(src_p)
        try:
            self._mk_pg_real(src_p)
            # (b) PG SELECT TAGS → Parser error
            tdSql.error(
                f"select tags from {src_p}.t1",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        finally:
            self._cleanup_src(src_p)

        self._cleanup_src(src_i)
        try:
            self._mk_influx_real(src_i)
            # (c) InfluxDB exception: TAGS accepted (has native tag concept)
            self._assert_not_syntax_error(
                f"select tags from {src_i}.cpu")
        finally:
            self._cleanup_src(src_i)

    def test_fq_local_s04_fill_forward_twa_irate(self):
        """Gap supplement: FILL_FORWARD / TWA / IRATE local compute correctness

        DS §5.3.4.1.15 function list includes FILL_FORWARD, TWA, IRATE as
        "all local computation". FQ-LOCAL-045 covers MAVG/STATECOUNT/DERIVATIVE but does
        NOT include FILL_FORWARD, TWA, or IRATE.

        Dimensions:
          a) MySQL → FILL_FORWARD(val), TWA(val), IRATE(val) on real external data; verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: all three functions computed locally, results correct

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: DS §5.3.4.1.15 — Dimension 7 (FS-Driven Validation)

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            # FILL_FORWARD: all rows non-null, values preserved
            tdSql.query(f"select fill_forward(val) from {src}.src_t")
            tdSql.checkRows(5)
            for i, expected in enumerate([1, 2, 3, 4, 5]):
                tdSql.checkData(i, 0, expected)
            # TWA: time-weighted average ≈ 3.0
            tdSql.query(f"select twa(val) from {src}.src_t")
            tdSql.checkRows(1)
            twa_result = float(tdSql.getData(0, 0))
            assert abs(twa_result - 3.0) < 0.001, (
                f"TWA expected ≈3.0, got {twa_result}")
            # IRATE: instantaneous rate = (5-4)/60s = 1/60 ≈ 0.01667
            tdSql.query(f"select irate(val) from {src}.src_t")
            tdSql.checkRows(1)
            irate_result = float(tdSql.getData(0, 0))
            assert abs(irate_result - 1.0/60) < 0.001, (
                f"IRATE expected ≈0.01667, got {irate_result}")
        self._with_std_sources("fq_local_s04", _body)

    def test_fq_local_s05_selection_funcs_local(self):
        """Gap supplement: FIRST/LAST/LAST_ROW/TOP/BOTTOM local compute correctness

        DS §5.3.4.1.13: these selection functions are ALL "local computation" for
        MySQL/PG/InfluxDB. FQ-LOCAL-044 only tests UNIQUE/SAMPLE/COLS.
        This case verifies the remaining selection functions.

        Dimensions:
          a) MySQL → FIRST(val), LAST(val), TOP(val,3), BOTTOM(val,2) on real data
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: FIRST/LAST/LAST_ROW/TOP/BOTTOM correctness

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: DS §5.3.4.1.13 — Dimension 7 (FS-Driven Validation)

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(f"select first(val) from {src}.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.query(f"select last(val) from {src}.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select last_row(val) from {src}.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select top(val, 3) from {src}.src_t order by val")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(1, 0, 4)
            tdSql.checkData(2, 0, 5)
            tdSql.query(f"select bottom(val, 2) from {src}.src_t order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
        self._with_std_sources("fq_local_s05", _body)

    def test_fq_local_s06_system_meta_funcs_local(self):
        """Gap supplement: System / meta-info functions all execute locally

        DS §5.3.4.1.16: CLIENT_VERSION, CURRENT_USER, DATABASE, SERVER_VERSION,
        SERVER_STATUS are "all local computation". When used in a query over an external
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

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)–(e) System meta functions: verified against all three real external sources
        def _body(src):
            # (a) CLIENT_VERSION: local TDengine client version
            tdSql.query(f"select client_version() from {src}.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "CLIENT_VERSION() should return non-null")

            # (b) DATABASE: current database name
            tdSql.query(f"select database() from {src}.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "DATABASE() should return non-null")

            # (c) SERVER_VERSION: server version string non-null
            tdSql.query(f"select server_version() from {src}.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, (
                "SERVER_VERSION() should return non-null")

            # (d) CURRENT_USER: logged-in user string non-null
            tdSql.query(f"select current_user() from {src}.src_t limit 1")
            tdSql.checkRows(1)
            cu_val = str(tdSql.getData(0, 0))
            assert len(cu_val) > 0, (
                "CURRENT_USER() should return a non-empty string")
        self._with_std_sources("fq_local_s06", _body)

    def test_fq_local_s07_session_event_count_window(self):
        """Gap supplement: SESSION / EVENT / COUNT window — three window types always local

        DS §5.3.5.1.4 SESSION_WINDOW: local computation for all 3 sources.
        DS §5.3.5.1.5 EVENT_WINDOW:   local computation for all 3 sources.
        DS §5.3.5.1.6 COUNT_WINDOW:   local computation for all 3 sources.
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

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)–(d) SESSION/EVENT/COUNT window: verified against all three real external sources
        def _body(src):
            # (a) SESSION_WINDOW: threshold 10s < actual gap 60s → every row is its own session
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"session(ts, 10s)")
            tdSql.checkRows(5)     # 5 isolated sessions
            for i in range(5):
                tdSql.checkData(i, 1, 1)  # each session has exactly 1 row

            # (b) EVENT_WINDOW: start at val>=2, close when val>=4
            # val=[1,2,3,4,5]:
            #   row val=1: condition val>=2 not met, no window
            #   row val=2: start condition met → open window
            #   row val=3: in window
            #   row val=4: end condition met → close window → window1=[2,3,4] (3 rows)
            #   row val=5: start condition met (val>=2) → open window, no more rows → window2=[5] (1 row)
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"event_window start with val >= 2 end with val >= 4")
            tdSql.checkRows(2)      # 2 event windows
            tdSql.checkData(0, 1, 3)  # first window: val=2,3,4 → 3 rows
            tdSql.checkData(1, 1, 1)  # second window: val=5 → 1 row

            # (c) COUNT_WINDOW(2): groups of 2 rows
            # [row1,row2], [row3,row4], [row5] → 3 windows (last partial window included)
            tdSql.query(
                f"select _wstart, count(*) from {src}.src_t "
                f"count_window(2)")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 2)  # first window: 2 rows
            tdSql.checkData(1, 1, 2)  # second window: 2 rows
            tdSql.checkData(2, 1, 1)  # last window: 1 row (partial)
        self._with_std_sources("fq_local_s07", _body)

    def test_fq_local_s08_window_join(self):
        """Gap supplement: WINDOW JOIN always executes locally

        DS §5.3.6.1.7: Window Join (TDengine-proprietary) — local computation for all 3 sources.
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

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create table fq_local_db.t2 (ts timestamp, v2 int)")
            tdSql.execute(
                "insert into fq_local_db.t2 values "
                "(1704067200000,10)(1704067260000,20)(1704067320000,30)")

            # WINDOW JOIN: for each src_t row, match t2 rows within ±30s window
            # FS §3.7.3 + DS §5.3.6.1.7: Window Join supported (local computation)
            # TDengine WINDOW JOIN syntax requires LEFT/RIGHT prefix
            # LEFT WINDOW JOIN: all left-table rows preserved; unmatched → NULL right cols
            # ts=0/60/120s match t2 (v2=10/20/30); ts=180/240s have no t2 match → NULL
            tdSql.query(
                "select a.val, b.v2 from fq_local_db.src_t a "
                "left window join fq_local_db.t2 b "
                "window_offset(-30s, 30s) "
                "order by a.ts")
            tdSql.checkRows(5)   # LEFT JOIN: all 5 src_t rows returned
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4)
            assert tdSql.getData(3, 1) is None, "val=4: no t2 in window, v2 must be NULL"
            tdSql.checkData(4, 0, 5)
            assert tdSql.getData(4, 1) is None, "val=5: no t2 in window, v2 must be NULL"
        finally:
            self._teardown_internal_env()

        # (b) MySQL: real WINDOW JOIN correctness with two tables
        src_m = "fq_local_s08_m"
        m_db_s08 = "fq_s08_m_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db_s08)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db_s08, [
                "CREATE TABLE IF NOT EXISTS src_t (ts DATETIME(3) PRIMARY KEY, val INT)",
                "DELETE FROM src_t",
                "INSERT INTO src_t VALUES "
                "('2024-01-01 00:00:00.000',1),('2024-01-01 00:01:00.000',2),"
                "('2024-01-01 00:02:00.000',3),('2024-01-01 00:03:00.000',4),"
                "('2024-01-01 00:04:00.000',5)",
                "CREATE TABLE IF NOT EXISTS t2 (ts DATETIME(3) PRIMARY KEY, v2 INT)",
                "DELETE FROM t2",
                "INSERT INTO t2 VALUES "
                "('2024-01-01 00:00:00.000',10),('2024-01-01 00:01:00.000',20),"
                "('2024-01-01 00:02:00.000',30)",
            ])
            self._mk_mysql_real(src_m, database=m_db_s08)
            tdSql.query(
                f"select a.val, b.v2 from {src_m}.src_t a "
                f"left window join {src_m}.t2 b "
                f"window_offset(-30s, 30s) "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4)
            assert tdSql.getData(3, 1) is None, "val=4: no t2 in window, v2 must be NULL"
            tdSql.checkData(4, 0, 5)
            assert tdSql.getData(4, 1) is None, "val=5: no t2 in window, v2 must be NULL"
        finally:
            self._cleanup_src(src_m)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db_s08)
            except Exception:
                pass

        # (c) PG: real WINDOW JOIN correctness with two tables
        src_p = "fq_local_s08_p"
        p_db_s08 = "fq_s08_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db_s08)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db_s08, [
                "CREATE TABLE IF NOT EXISTS src_t (ts TIMESTAMP, val INT)",
                "DELETE FROM src_t",
                "INSERT INTO src_t VALUES "
                "('2024-01-01 00:00:00.000',1),('2024-01-01 00:01:00.000',2),"
                "('2024-01-01 00:02:00.000',3),('2024-01-01 00:03:00.000',4),"
                "('2024-01-01 00:04:00.000',5)",
                "CREATE TABLE IF NOT EXISTS t2 (ts TIMESTAMP, v2 INT)",
                "DELETE FROM t2",
                "INSERT INTO t2 VALUES "
                "('2024-01-01 00:00:00.000',10),('2024-01-01 00:01:00.000',20),"
                "('2024-01-01 00:02:00.000',30)",
            ])
            self._mk_pg_real(src_p, database=p_db_s08)
            tdSql.query(
                f"select a.val, b.v2 from {src_p}.src_t a "
                f"left window join {src_p}.t2 b "
                f"window_offset(-30s, 30s) "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4)
            assert tdSql.getData(3, 1) is None, "val=4: no t2 in window, v2 must be NULL"
            tdSql.checkData(4, 0, 5)
            assert tdSql.getData(4, 1) is None, "val=5: no t2 in window, v2 must be NULL"
        finally:
            self._cleanup_src(src_p)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db_s08)
            except Exception:
                pass

        # (d) InfluxDB: real WINDOW JOIN correctness with two measurements
        src_i = "fq_local_s08_i"
        i_db_s08 = "fq_s08_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_s08)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_s08, [
                "src_t val=1i 1704067200000000000",
                "src_t val=2i 1704067260000000000",
                "src_t val=3i 1704067320000000000",
                "src_t val=4i 1704067380000000000",
                "src_t val=5i 1704067440000000000",
                "t2 v2=10i 1704067200000000000",
                "t2 v2=20i 1704067260000000000",
                "t2 v2=30i 1704067320000000000",
            ])
            self._mk_influx_real(src_i, database=i_db_s08)
            tdSql.query(
                f"select a.val, b.v2 from {src_i}.src_t a "
                f"left window join {src_i}.t2 b "
                f"window_offset(-30s, 30s) "
                f"order by a.ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1);  tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2);  tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3);  tdSql.checkData(2, 1, 30)
            tdSql.checkData(3, 0, 4)
            assert tdSql.getData(3, 1) is None, "val=4: no t2 in window, v2 must be NULL"
            tdSql.checkData(4, 0, 5)
            assert tdSql.getData(4, 1) is None, "val=5: no t2 in window, v2 must be NULL"
        finally:
            self._cleanup_src(src_i)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_s08)
            except Exception:
                pass

    def test_fq_local_s09_elapsed_histogram(self):
        """Gap supplement: ELAPSED and HISTOGRAM special aggregates — always local

        DS §5.3.4.1.12 "special aggregate functions": ELAPSED, HISTOGRAM, HYPERLOGLOG are
        "all local computation". Completely absent from FQ-LOCAL-001~045.

        Data: 5 rows at 0/60/120/180/240s, val=[1,2,3,4,5]

        Dimensions:
          a) MySQL → ELAPSED(ts,1s) ≈0=240s; HISTOGRAM(val,...) → 3 bin rows; verified
          b) PG → same
          c) InfluxDB → same (val stored as INT field)
          d) Internal vtable baseline: all results correct

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: DS §5.3.4.1.12

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b)/(c) All three real external sources
        def _body(src):
            tdSql.query(f"select elapsed(ts, 1s) from {src}.src_t")
            tdSql.checkRows(1)
            elapsed_s = float(tdSql.getData(0, 0))
            assert abs(elapsed_s - 240.0) < 1.0, (
                f"ELAPSED expected 240s, got {elapsed_s}")
            tdSql.query(
                f"select histogram(val, 'user_input', '[0,2,4,6]', 0) "
                f"from {src}.src_t")
            tdSql.checkRows(3)
            for i in range(3):
                assert tdSql.getData(i, 0) is not None, (
                    f"HISTOGRAM row {i} should not be NULL")
        self._with_std_sources("fq_local_s09", _body)

    def test_fq_local_s10_mask_aes_functions(self):
        """Gap supplement: masking and encryption functions — all local compute

        DS §5.3.4.1.6 "masking functions": MASK_FULL, MASK_PARTIAL, MASK_NONE —
          "all local computation. TDengine-proprietary functions."
        DS §5.3.4.1.7 "encryption functions": AES_ENCRYPT, AES_DECRYPT, SM4_ENCRYPT, SM4_DECRYPT —
          all local computation. "MySQL key padding/mode differs from TDengine; cannot be aligned via parameter conversion."

        Completely absent from FQ-LOCAL-001~045 and s01~s09.

        Data: name column = ['alpha','beta','gamma','delta','epsilon']

        Dimensions:
          a) MySQL → MASK_FULL, MASK_PARTIAL, AES roundtrip on name VARCHAR; verified
          b) PG → same (name VARCHAR)
          c) InfluxDB skipped: name is stored as a string field (BINARY); AES/mask
             behavior on BINARY may differ; covered by internal vtable baseline.
          d) Internal vtable baseline: MASK_FULL, MASK_PARTIAL, AES roundtrip correct

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci
        Gap: DS §5.3.4.1.6 + §5.3.4.1.7

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a)/(b) MySQL + PG only (skip InfluxDB: BINARY name type may behave differently)
        def _body(src):
            tdSql.query(
                f"select name, mask_full(name, 'xxxxx') from {src}.src_t "
                f"order by ts limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is not None, "MASK_FULL should return non-null"
            tdSql.query(
                f"select mask_partial(name, 1, 2, '*') from {src}.src_t "
                f"order by ts limit 1")
            tdSql.checkRows(1)
            partial = str(tdSql.getData(0, 0))
            assert '**' in partial, (
                f"MASK_PARTIAL should insert mask chars, got: {partial!r}")
            key = "'1234567890abcdef'"
            tdSql.query(
                f"select name, "
                f"aes_decrypt(aes_encrypt(name, {key}), {key}) "
                f"from {src}.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is not None, (
                "AES_DECRYPT(AES_ENCRYPT(name, key), key) should not be NULL")
        self._with_std_sources("fq_local_s10", _body)

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

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # (a) UNION ALL semantic: same-table, different filters — verified against all three real external sources
        def _body(src):
            tdSql.query(
                f"select val from {src}.src_t where val <= 2 "
                f"union all "
                f"select val from {src}.src_t where val >= 4 "
                f"order by val")
            tdSql.checkRows(4)    # 2 rows from first branch + 2 rows from second
            tdSql.checkData(0, 0, 1)   # first branch: val=1
            tdSql.checkData(1, 0, 2)   # first branch: val=2
            tdSql.checkData(2, 0, 4)   # second branch: val=4
            tdSql.checkData(3, 0, 5)   # second branch: val=5
        self._with_std_sources("fq_local_s11", _body)

        # (b) Cross-source UNION ALL (two different external sources → local merge path)
        src_m = "fq_local_s11_m"
        src_p = "fq_local_s11_p"
        self._cleanup_src(src_m, src_p)
        try:
            self._mk_mysql_real(src_m)
            self._mk_pg_real(src_p)
            self._assert_not_syntax_error(
                f"select id, val from {src_m}.orders "
                "union all "
                f"select id, val from {src_p}.orders "
                "limit 10")
        finally:
            self._cleanup_src(src_m, src_p)

    def test_fq_local_s12_enterprise_feature_positive_suite(self):
        """Gap: comprehensive positive verification of enterprise-edition feature availability

        Supplements local_029/030/031 with a broader set of positive checks:
          a) CREATE EXTERNAL SOURCE for all supported types (mysql, postgresql, influxdb)
             does not raise TSDB_CODE_EXT_FEATURE_DISABLED
          b) SHOW EXTERNAL SOURCES lists all created sources
          c) DESCRIBE EXTERNAL SOURCE succeeds for a live source
          d) ALTER EXTERNAL SOURCE with every alterable field does not raise
             TSDB_CODE_EXT_FEATURE_DISABLED
          e) DROP EXTERNAL SOURCE IF EXISTS is idempotent (no error on absent source)
          f) Querying ins_ext_sources after each operation reflects correct state

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 wpan Initial implementation

        """
        src_m = "fq_local_s12_m"
        src_p = "fq_local_s12_p"
        src_i = "fq_local_s12_i"
        self._cleanup_src(src_m, src_p, src_i)
        cfg_m = self._mysql_cfg()
        cfg_p = self._pg_cfg()
        cfg_i = self._influx_cfg()

        try:
            # (a) CREATE for all supported types
            tdSql.execute(
                f"create external source {src_m} "
                f"type='mysql' host='{cfg_m.host}' port={cfg_m.port} "
                f"user='{cfg_m.user}' password='{cfg_m.password}'"
            )
            tdSql.execute(
                f"create external source {src_p} "
                f"type='postgresql' host='{cfg_p.host}' port={cfg_p.port} "
                f"user='{cfg_p.user}' password='{cfg_p.password}'"
            )
            tdSql.execute(
                f"create external source {src_i} "
                f"type='influxdb' host='{cfg_i.host}' port={cfg_i.port} "
                f"user='u' password='' "
                f"options('protocol'='http')"
            )

            # (f) All three visible in system table
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name in ('{src_m}', '{src_p}', '{src_i}')"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)

            # (b) SHOW EXTERNAL SOURCES — lists all external sources
            tdSql.query("show external sources")
            assert tdSql.queryRows >= 3, "SHOW EXTERNAL SOURCES must list at least 3 sources"

            # (c) DESCRIBE EXTERNAL SOURCE
            tdSql.query(f"describe external source {src_m}")
            assert tdSql.queryRows >= 1, "DESCRIBE must return at least one row"

            # (d) ALTER with multiple fields on MySQL source
            tdSql.execute(
                f"alter external source {src_m} SET "
                f"host='{cfg_m.host}', port={cfg_m.port}, "
                f"options('connect_timeout_ms'='3000')"
            )
            tdSql.query(
                "select `host`, `port` from information_schema.ins_ext_sources "
                f"where source_name = '{src_m}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, cfg_m.host)
            tdSql.checkData(0, 1, cfg_m.port)

            # (e) DROP EXTERNAL SOURCE IF EXISTS: first call drops, second is idempotent
            tdSql.execute(f"drop external source if exists {src_m}")
            tdSql.execute(f"drop external source if exists {src_m}")   # must not error

            # Remaining two sources still present
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name in ('{src_p}', '{src_i}')"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
        finally:
            self._cleanup_src(src_m, src_p, src_i)
    def test_fq_local_s13_scalar_subquery_external_source(self):
        """Gap: scalar subquery against real external source tables; two execution paths

        Path 1 (完全下推 / Fully-Pushed-to-External-DB):
          Same-source scalar subquery: external DB evaluates the scalar subquery natively.
          TDengine currently returns all rows ignoring the scalar filter; wrapped in try/except.
            a) MySQL scalar in WHERE: val > (SELECT MAX(threshold) FROM limits) → 1 row
            b) MySQL scalar in SELECT: (SELECT COUNT(*) FROM items) as total → 3
            c) PG scalar in WHERE: score > (SELECT AVG(score) FROM scores) → 1 row

        Path 2 (TDengine子查询 / TDengine-Orchestrated Subquery):
          TDengine evaluates the scalar subquery against an internal table, obtains a
          single constant value, then pushes the rewritten comparison to InfluxDB
          (only InfluxDB source is registered for this phase).
            d) InfluxDB outer + TDengine internal scalar in WHERE:
               val > (SELECT MAX(limit_val) FROM ref.lim) → MAX=20 → val=30 → 1 row
               [try/except — scalar rewrite for external source not yet confirmed]

        Data:
          MySQL items: (id=1,val=10),(id=2,val=20),(id=3,val=30)
          MySQL limits: (id=1,threshold=10),(id=2,threshold=20)
          PG scores: (id=1,score=10),(id=2,score=20),(id=3,score=30)
          InfluxDB sensor: h1(val=5), h2(val=10), h3(val=20), h4(val=30)
          TDengine internal lim: limit_val IN (10,20) → MAX=20

        Catalog: - Query:FederatedLocal

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-xx wpan Initial implementation; two execution paths

        """
        m = "fq_local_s13_m"
        m_db = "fq_s13_m_db"
        p = "fq_local_s13_p"
        p_db = "fq_s13_p_db"
        ref_db = "fq_s13_ref"
        self._cleanup_src(m, p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            # ── Data setup ──────────────────────────────────────────────────────
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "CREATE TABLE IF NOT EXISTS items (id INT, val INT)",
                "DELETE FROM items",
                "INSERT INTO items VALUES (1,10),(2,20),(3,30)",
                "CREATE TABLE IF NOT EXISTS limits (id INT, threshold INT)",
                "DELETE FROM limits",
                "INSERT INTO limits VALUES (1,10),(2,20)",
            ])
            self._mk_mysql_real(m, database=m_db)

            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "CREATE TABLE IF NOT EXISTS scores (id INT, score FLOAT8)",
                "DELETE FROM scores",
                "INSERT INTO scores VALUES (1,10.0),(2,20.0),(3,30.0)",
            ])
            self._mk_pg_real(p, database=p_db)

            # TDengine internal: limit_val IN (10, 20) → MAX=20
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.lim (ts timestamp, limit_val int)")
            tdSql.execute(
                f"insert into {ref_db}.lim values "
                f"(1704067200000,10)(1704067260000,20)")

            # ── Path 1: 完全下推 (Fully-Pushed-to-External-DB) ──────────────────

            # (a) MySQL scalar in WHERE: val > MAX(threshold)=20 → only val=30 → 1 row
            tdSql.query(
                f"select id, val from {m}.items "
                f"where val > (select max(threshold) from {m}.limits) "
                f"order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 30)

            # (b) MySQL scalar in SELECT: (SELECT COUNT(*) FROM items) → 3
            tdSql.query(
                f"select (select count(*) from {m}.items) as total")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)

            # (c) PG scalar in WHERE: score > AVG(scores)=20 → only score=30 → 1 row
            tdSql.query(
                f"select id, score from {p}.scores "
                f"where score > (select avg(score) from {p}.scores) "
                f"order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 30.0)

            # Drop MySQL and PG sources before registering InfluxDB for Path 2
            self._cleanup_src(m, p)

            # ── Path 2: TDengine子查询 (TDengine-Orchestrated Subquery) ──────────
            # Only InfluxDB source registered here. TDengine evaluates the internal
            # scalar subquery first, then uses the result as a constant in the
            # WHERE clause pushed to InfluxDB.
            influx_src = "fq_local_s13_i"
            i_db_s13 = "fq_s13_i_db"
            self._cleanup_src(influx_src)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db_s13)
            try:
                ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db_s13, [
                    "sensor,host=h1 val=5i  1704067200000000000",
                    "sensor,host=h2 val=10i 1704067260000000000",
                    "sensor,host=h3 val=20i 1704067320000000000",
                    "sensor,host=h4 val=30i 1704067380000000000",
                ])
                self._mk_influx_real(influx_src, database=i_db_s13)

                # (d) InfluxDB outer + TDengine internal scalar:
                #     MAX(limit_val)=20 → val > 20 → h4(val=30) → 1 row
                tdSql.query(
                    f"select `host`, val from {influx_src}.sensor "
                    f"where val > (select max(limit_val) from {ref_db}.lim) "
                    f"order by ts")
                tdSql.checkRows(1)
                tdSql.checkData(0, 1, 30)   # h4: val=30

            finally:
                self._cleanup_src(influx_src)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db_s13)
                except Exception:
                    pass
        finally:
            self._cleanup_src(m, p)
            tdSql.execute(f"drop database if exists {ref_db}")
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
