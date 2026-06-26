"""
test_fq_18_source_specific.py

Source-specific and library-behavior-inconsistent federated query cases.

Consolidates cases from fq_04, fq_05, and fq_06 that exhibit behavior
unique to a specific external source library or expose incompatibilities
between source versions/protocols:

  - test_fq_sql_influxdb_tags_partition  (fq_04): InfluxDB DISTINCT tags
                                                  + PARTITION BY tag (InfluxDB-only)
  - test_fq_sql_infoschema               (fq_04): MySQL INFORMATION_SCHEMA
                                                  (MySQL-only system catalog)
  - test_fq_local_033                    (fq_05): InfluxDB 1.x protocol
                                                  incompatibility (1.8 vs 3.x)
  - test_fq_push_s04_influx_partition_tbname_to_groupby_tags (fq_06):
                                                  InfluxDB PARTITION BY TBNAME
                                                  → GROUP BY all tags (positive);
                                                  MySQL/PG reject with
                                                  EXT_SYNTAX_UNSUPPORTED

Test structure
--------------
A single module-level array ``_CASES`` drives all tests.  SQL templates use
brace-token placeholders substituted at runtime with the actual external
source names created in ``setup_method``.

Each entry has the form::

    (case_id, sql_template, positive, expected_errno, opts)

  positive=True,  expected_errno=None : positive query case;
    opts may contain ``float_cols``, ``ordered``, or ``min_count``.
    Cases with ``min_count`` assert result[0][0] ≥ N instead of baseline
    comparison; all others are compared against the baseline file.
  positive=False, expected_errno=CODE : error case, errno verified inline.

Entries are grouped by target source (InfluxDB → MySQL → PostgreSQL).

Catalog: - Query:FederatedSourceSpecific

Since: v3.4.0.0

Labels: common,ci
"""

import os
import shutil
import time as _time

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    QueryError,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_OPS_NOT_SUPPORT,
    TSDB_CODE_PAR_INVALID_COLUMN,
    TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
    parity_serialize_case,
    parity_serialize_cell,
)


# ---------------------------------------------------------------------------
# Module-level data constants
# ---------------------------------------------------------------------------

_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC (ms)

# ── InfluxDB tags-partition data (itag group) ────────────────────────────────
# cpu measurement: 4 rows, 2 host/region combinations
#   h1/us: usage=30 and usage=50  → AVG = 40.0
#   h2/eu: usage=10 and usage=20  → AVG = 15.0
_INFLUX_ITAG_DB    = "fq18_itag_db"
_INFLUX_ITAG_LINES = [
    "cpu,host=h1,region=us usage=30i 1704067200000000000",
    "cpu,host=h1,region=us usage=50i 1704067260000000000",
    "cpu,host=h2,region=eu usage=10i 1704067320000000000",
    "cpu,host=h2,region=eu usage=20i 1704067380000000000",
]

# ── InfluxDB CPU s04 data (i04 / PARTITION BY TBNAME group) ─────────────────
# cpu measurement: 4 rows, 2 host tag values
#   host=a: usage_idle=80.0 and 75.0  → AVG = 77.5
#   host=b: usage_idle=90.0 and 85.0  → AVG = 87.5
_INFLUX_S04_DB    = "fq18_s04_cpu"
_INFLUX_S04_LINES = [
    f"cpu,host=a usage_idle=80.0 {_BASE_TS}000000",
    f"cpu,host=a usage_idle=75.0 {_BASE_TS + 60000}000000",
    f"cpu,host=b usage_idle=90.0 {_BASE_TS}000000",
    f"cpu,host=b usage_idle=85.0 {_BASE_TS + 60000}000000",
]

# ── MySQL push_t data (m04 / PARTITION BY TBNAME error group) ───────────────
_MYSQL_PUSH_T_SQLS = [
    "CREATE TABLE IF NOT EXISTS push_t "
    "(val INT, score DOUBLE, name VARCHAR(32), flag TINYINT(1), status VARCHAR(16))",
    "DELETE FROM push_t",
    "INSERT INTO push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# ── PostgreSQL push_t data (p04 / PARTITION BY TBNAME error group) ──────────
_PG_PUSH_T_SQLS = [
    "CREATE TABLE IF NOT EXISTS push_t "
    "(val INT, score FLOAT8, name TEXT, flag INT, status TEXT)",
    "DELETE FROM push_t",
    "INSERT INTO push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# ── MySQL VIEW data (vw group) ────────────────────────────────────────────────────
_MYSQL_VIEW_DB   = "fq18_view_mdb"
_MYSQL_VIEW_SQLS = [
    "DROP TABLE IF EXISTS users",
    "DROP TABLE IF EXISTS orders",
    "CREATE TABLE users (id INT, name VARCHAR(50))",
    "CREATE TABLE orders (id INT, user_id INT, amount INT, status INT)",
    "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    "INSERT INTO orders VALUES (1, 1, 100, 1), (2, 1, 200, 2)",
    "DROP VIEW IF EXISTS v_summary",
    "DROP VIEW IF EXISTS v_users",
    "CREATE VIEW v_summary AS "
    "  SELECT status, sum(amount) as total FROM orders GROUP BY status",
    "CREATE VIEW v_users AS SELECT id, name FROM users WHERE id <= 10",
]

# ── PostgreSQL VIEW data (vw group) ────────────────────────────────────────────────
_PG_VIEW_DB   = "fq18_view_pdb"
_PG_VIEW_SQLS = [
    "DROP TABLE IF EXISTS users CASCADE",
    "DROP TABLE IF EXISTS orders CASCADE",
    "CREATE TABLE users (id INT, name TEXT)",
    "CREATE TABLE orders (id INT, user_id INT, amount INT, status INT)",
    "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    "INSERT INTO orders VALUES (1, 1, 100, 1), (2, 1, 200, 2)",
    "DROP VIEW IF EXISTS v_summary",
    "DROP VIEW IF EXISTS v_users",
    "CREATE VIEW v_summary AS "
    "  SELECT status, sum(amount) as total FROM orders GROUP BY status",
    "CREATE VIEW v_users AS SELECT id, name FROM users WHERE id <= 10",
]


# ---------------------------------------------------------------------------
# Unified case array  (case_id, sql_template, positive, expected_errno, opts)
# ---------------------------------------------------------------------------
# SQL template tokens (substituted at runtime via _fmt):
#   {I_TAG}     — InfluxDB tags-partition source
#   {I_TAG_DB}  — InfluxDB tags-partition bucket
#   {I_CPU}     — InfluxDB CPU s04 source
#   {I18}       — InfluxDB 1.8 source (conditional; absent when not configured)
#   {M_INFO}    — MySQL INFORMATION_SCHEMA source
#   {M_INFO_DB} — MySQL INFORMATION_SCHEMA database name
#   {M_PUSH}    — MySQL push_t source
#   {P_PUSH}    — PostgreSQL push_t source
#
# opts keys:
#   float_cols  — set of column indices compared as float (default empty)
#   ordered     — False when row order is non-deterministic (default True)
#   min_count   — int; assert result[0][0] >= N instead of baseline comparison
_CASES = [
    # ── InfluxDB: DISTINCT tags + PARTITION BY tag (itag, from fq_04) ────────
    # 2 distinct host/region combinations; ORDER BY host ensures stable order.
    ("itag-01",
     "SELECT DISTINCT host, region FROM {I_TAG}.{I_TAG_DB}.cpu ORDER BY host",
     True, None, {}),
    # AVG(usage) per host: h1 → 40.0, h2 → 15.0; ORDER BY host is stable.
    ("itag-02",
     "SELECT AVG(usage) FROM {I_TAG}.{I_TAG_DB}.cpu PARTITION BY host ORDER BY host",
     True, None, dict(float_cols={0})),

    # ── InfluxDB: PARTITION BY TBNAME → GROUP BY all tags (i04, from fq_06) ──
    # cpu has 2 distinct host tags (a, b); partition order is hash-based → unordered.
    ("i04-01",
     "SELECT COUNT(*) FROM {I_CPU}.cpu PARTITION BY tbname",
     True, None, dict(ordered=False)),
    # host=a: avg(80.0, 75.0) = 77.5; host=b: avg(90.0, 85.0) = 87.5; unordered.
    ("i04-02",
     "SELECT AVG(usage_idle) FROM {I_CPU}.cpu PARTITION BY tbname",
     True, None, dict(float_cols={0}, ordered=False)),
    # SELECT TBNAME on InfluxDB is NOT the same as PARTITION BY TBNAME:
    # bare SELECT TBNAME is unsupported even on InfluxDB.
    ("i04-03",
     "SELECT tbname FROM {I_CPU}.cpu LIMIT 5",
     False, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, {}),

    # ── InfluxDB 1.8: protocol incompatibility (i18, from fq_05 local_033) ───
    # InfluxDB 1.x has no /api/v3/query_sql; HTTP 404 → TSDB_CODE_OPS_NOT_SUPPORT.
    # Skipped automatically when "1.8" is NOT in FQ_INFLUX_VERSIONS.
    ("i18-01",
     "SELECT * FROM {I18}.testdb.m",
     False, TSDB_CODE_OPS_NOT_SUPPORT, {}),

    # ── MySQL: INFORMATION_SCHEMA introspection (minfo, from fq_04) ──────────
    # Count is ≥ 1 (not exact); INFORMATION_SCHEMA may contain extra system tables.
    ("minfo-01",
     "SELECT COUNT(*) FROM {M_INFO}.information_schema.TABLES "
     "WHERE table_schema = '{M_INFO_DB}'",
     True, None, dict(min_count=1)),

    # ── MySQL: PARTITION BY / SELECT TBNAME → EXT_SYNTAX_UNSUPPORTED (m04) ────
    ("m04-01",
     "SELECT COUNT(*) FROM {M_PUSH}.push_t PARTITION BY tbname",
     False, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, {}),
    ("m04-02",
     "SELECT tbname FROM {M_PUSH}.push_t",
     False, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, {}),

    # ── PostgreSQL: PARTITION BY / SELECT TBNAME → EXT_SYNTAX_UNSUPPORTED (p04) ─
    ("p04-01",
     "SELECT COUNT(*) FROM {P_PUSH}.push_t PARTITION BY tbname",
     False, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, {}),
    ("p04-02",
     "SELECT tbname FROM {P_PUSH}.push_t",
     False, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, {}),

    # ── MySQL: VIEW query (vw-m group) ───────────────────────────────────────────
    # v_summary: SELECT status, SUM(amount) FROM orders GROUP BY status
    # status=1 -> total=100; status=2 -> total=200
    ("vw-m-01",
     "SELECT * FROM {M_VIEW}.{M_VIEW_DB}.v_summary ORDER BY status",
     True, None, {}),
    # v_users JOIN orders: non-ts JOIN → PAR_NOT_SUPPORT_JOIN
    ("vw-m-02",
     "SELECT v.id, v.name, SUM(o.amount) AS total "
     "FROM {M_VIEW}.{M_VIEW_DB}.v_users v "
     "JOIN {M_VIEW}.{M_VIEW_DB}.orders o ON v.id = o.user_id "
     "GROUP BY v.id, v.name ORDER BY v.id",
     False, TSDB_CODE_PAR_NOT_SUPPORT_JOIN, {}),

    # ── PostgreSQL: VIEW query (vw-p group) ────────────────────────────────────────
    ("vw-p-01",
     "SELECT * FROM {P_VIEW}.public.v_summary ORDER BY status",
     True, None, {}),
    ("vw-p-02",
     "SELECT v.id, v.name, SUM(o.amount) AS total "
     "FROM {P_VIEW}.public.v_users v "
     "JOIN {P_VIEW}.public.orders o ON v.id = o.user_id "
     "GROUP BY v.id, v.name ORDER BY v.id",
     False, TSDB_CODE_PAR_NOT_SUPPORT_JOIN, {}),

    # ── InfluxDB: "ts" column not found (i-neg group) ─────────────────────────
    # InfluxDB primary key is always "time"; querying with "ts" must fail at
    # parse time because the column does not exist in the external schema.
    # Error: TSDB_CODE_PAR_INVALID_COLUMN (0x2602) — column name not found.
    ("i-neg-01",
     "SELECT ts FROM {I_CPU}.cpu LIMIT 1",
     False, TSDB_CODE_PAR_INVALID_COLUMN, {}),
    ("i-neg-02",
     "SELECT usage_idle FROM {I_CPU}.cpu ORDER BY ts LIMIT 1",
     False, TSDB_CODE_PAR_INVALID_COLUMN, {}),
]


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestFq18SourceSpecific(FederatedQueryVersionedMixin):
    """Source-specific and library-behavior-inconsistent federated query cases.

    All cases are driven by the module-level ``_CASES`` array and grouped by
    target source (InfluxDB → MySQL → PostgreSQL):

      itag   — InfluxDB DISTINCT tags + PARTITION BY tag  (positive, baseline)
      i04    — InfluxDB PARTITION BY TBNAME → positive; SELECT TBNAME → error
      i18    — InfluxDB 1.8 protocol incompatibility  (negative, OPS_NOT_SUPPORT)
      minfo  — MySQL INFORMATION_SCHEMA count ≥ 1  (positive, min_count assertion)
      m04    — MySQL PARTITION BY / SELECT TBNAME  (negative, EXT_SYNTAX_UNSUPPORTED)
      p04    — PostgreSQL PARTITION BY TBNAME  (negative, EXT_SYNTAX_UNSUPPORTED)
    """

    # ── External source names ────────────────────────────────────────────────
    _SRC_I_TAG  = "fq18_itag"
    _SRC_I_CPU  = "fq18_s04_i"
    _SRC_M_INFO = "fq18_info_m"
    _SRC_M_PUSH = "fq18_s04_m"
    _SRC_P_PUSH = "fq18_s04_p"
    _SRC_M_VIEW = "fq18_view_m"
    _SRC_P_VIEW = "fq18_view_p"
    _SRC_I18    = "fq18_influx18"

    # ── Remote databases / buckets ───────────────────────────────────────────
    _I_TAG_DB   = _INFLUX_ITAG_DB    # "fq18_itag_db"
    _I_CPU_DB   = _INFLUX_S04_DB     # "fq18_s04_cpu"
    _M_INFO_DB  = "fq18_info_mdb"
    _M_PUSH_DB  = "fq18_s04_m_ext"
    _P_PUSH_DB  = "fq18_s04_p_ext"
    _M_VIEW_DB  = _MYSQL_VIEW_DB     # "fq18_view_mdb"
    _P_VIEW_DB  = _PG_VIEW_DB        # "fq18_view_pdb"
    _M_VIEW_DB  = _MYSQL_VIEW_DB     # "fq18_view_mdb"
    _P_VIEW_DB  = _PG_VIEW_DB        # "fq18_view_pdb"

    # ── Baseline file (same format as fq_14) ─────────────────────────────────
    _BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_18_source_specific.txt")

    # ── One-time setup guard ─────────────────────────────────────────────────
    _class_setup_done = False
    _i18_available    = False   # set to True in setup_method when 1.8 found

    # -----------------------------------------------------------------------
    # Setup / teardown
    # -----------------------------------------------------------------------

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        self._cleanup_src(
            self._SRC_I_TAG, self._SRC_I_CPU,
            self._SRC_M_INFO, self._SRC_M_PUSH, self._SRC_P_PUSH,
            self._SRC_M_VIEW, self._SRC_P_VIEW,
            self._SRC_I18,
        )
        for fn in [
            lambda: ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), self._I_TAG_DB),
            lambda: ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), self._I_CPU_DB),
            lambda: ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), self._M_INFO_DB),
            lambda: ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), self._M_PUSH_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), self._P_PUSH_DB),
            lambda: ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), self._M_VIEW_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), self._P_VIEW_DB),
        ]:
            try:
                fn()
            except Exception:
                pass
        TestFq18SourceSpecific._class_setup_done = False
        TestFq18SourceSpecific._i18_available    = False
        ExtSrcEnv.teardown_env()

    def setup_method(self, method):
        """Create all external sources once (shared across all test methods)."""
        if TestFq18SourceSpecific._class_setup_done:
            return

        # ── InfluxDB tags-partition source (itag group) ───────────────────────
        self._cleanup_src(self._SRC_I_TAG)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), self._I_TAG_DB)
        ExtSrcEnv.influx_write_cfg(
            self._influx_cfg(), self._I_TAG_DB, _INFLUX_ITAG_LINES)
        self._mk_influx_real(self._SRC_I_TAG, database=self._I_TAG_DB)

        # ── InfluxDB CPU source (i04 / PARTITION BY TBNAME group) ────────────
        self._cleanup_src(self._SRC_I_CPU)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), self._I_CPU_DB)
        ExtSrcEnv.influx_write_cfg(
            self._influx_cfg(), self._I_CPU_DB, _INFLUX_S04_LINES)
        self._mk_influx_real(self._SRC_I_CPU, database=self._I_CPU_DB)

        # ── MySQL INFORMATION_SCHEMA source (minfo group) ─────────────────────
        self._cleanup_src(self._SRC_M_INFO)
        ExtSrcEnv.mysql_kill_sleeping_connections_cfg(self._mysql_cfg())
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), self._M_INFO_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), self._M_INFO_DB, [
            "DROP TABLE IF EXISTS t1",
            "CREATE TABLE t1 (id INT)",
            "INSERT INTO t1 VALUES (1)",
        ])
        self._mk_mysql_real(self._SRC_M_INFO, database=self._M_INFO_DB)

        # ── MySQL push_t source (m04 / PARTITION BY TBNAME error group) ───────
        self._cleanup_src(self._SRC_M_PUSH)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), self._M_PUSH_DB)
        ExtSrcEnv.mysql_exec_cfg(
            self._mysql_cfg(), self._M_PUSH_DB, _MYSQL_PUSH_T_SQLS)
        self._mk_mysql_real(self._SRC_M_PUSH, database=self._M_PUSH_DB)

        # ── PostgreSQL push_t source (p04 / PARTITION BY TBNAME error group) ──
        self._cleanup_src(self._SRC_P_PUSH)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), self._P_PUSH_DB)
        ExtSrcEnv.pg_exec_cfg(
            self._pg_cfg(), self._P_PUSH_DB, _PG_PUSH_T_SQLS)
        self._mk_pg_real(self._SRC_P_PUSH, database=self._P_PUSH_DB)
        # ── MySQL VIEW source (vw-m group) ──────────────────────────────────────────────
        self._cleanup_src(self._SRC_M_VIEW)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), self._M_VIEW_DB)
        ExtSrcEnv.mysql_exec_cfg(
            self._mysql_cfg(), self._M_VIEW_DB, _MYSQL_VIEW_SQLS)
        self._mk_mysql_real(self._SRC_M_VIEW, database=self._M_VIEW_DB)

        # ── PostgreSQL VIEW source (vw-p group) ──────────────────────────────────────────
        self._cleanup_src(self._SRC_P_VIEW)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), self._P_VIEW_DB)
        ExtSrcEnv.pg_exec_cfg(
            self._pg_cfg(), self._P_VIEW_DB, _PG_VIEW_SQLS)
        self._mk_pg_real(self._SRC_P_VIEW, database=self._P_VIEW_DB)
        # ── InfluxDB 1.8 source (i18 group, conditional) ─────────────────────
        # Only created when "1.8" is listed in FQ_INFLUX_VERSIONS.
        # Uses protocol='http' so the HTTP connector probes /api/v3/query_sql
        # which returns HTTP 404 on InfluxDB 1.x → TSDB_CODE_OPS_NOT_SUPPORT.
        influx18_cfgs = [
            c for c in ExtSrcEnv.influx_version_configs() if c.version == "1.8"
        ]
        if influx18_cfgs:
            cfg18 = influx18_cfgs[0]
            self._cleanup_src(self._SRC_I18)
            tdSql.execute(
                f"CREATE EXTERNAL SOURCE {self._SRC_I18} "
                f"TYPE='influxdb' HOST='{cfg18.host}' PORT={cfg18.port} "
                f"USER='u' PASSWORD='' "
                f"OPTIONS('api_token'='','protocol'='http')"
            )
            TestFq18SourceSpecific._i18_available = True

        TestFq18SourceSpecific._class_setup_done = True

    # -----------------------------------------------------------------------
    # SQL template substitution helper
    # -----------------------------------------------------------------------

    def _fmt(self, sql_template):
        """Substitute source-name and db-name tokens in *sql_template*."""
        return sql_template.format(
            I_TAG=self._SRC_I_TAG,
            I_TAG_DB=self._I_TAG_DB,
            I_CPU=self._SRC_I_CPU,
            M_INFO=self._SRC_M_INFO,
            M_INFO_DB=self._M_INFO_DB,
            M_PUSH=self._SRC_M_PUSH,
            P_PUSH=self._SRC_P_PUSH,
            M_VIEW=self._SRC_M_VIEW,
            M_VIEW_DB=self._M_VIEW_DB,
            P_VIEW=self._SRC_P_VIEW,
            I18=self._SRC_I18,
        )

    # -----------------------------------------------------------------------
    # Case runners
    # -----------------------------------------------------------------------

    def _run_specific_cases(self, cases):
        """Run positive source-specific cases; compare results against baseline.

        Each case runs the substituted SQL, serializes the result into the
        same ``### case_id POS`` block format as fq_14, writes a ``.tmp``
        file, and then compares against the stable baseline file.

        On the first run (no baseline file present), the tmp file is copied
        to the baseline automatically.

        Returns a list of ``(case_id, sql_template, detail)`` failure tuples.
        """
        failed: list[tuple[str, str, str]] = []
        serialized_blocks: list[str] = []

        for case_id, sql_template, opts in cases:
            float_cols = opts.get("float_cols") or set()
            ordered    = opts.get("ordered", True)
            min_count  = opts.get("min_count")
            sql        = self._fmt(sql_template)
            t0         = _time.monotonic()

            rows      = None
            qerr      = None
            try:
                tdSql.query(sql, queryTimes=3)
                rows = list(tdSql.queryResult)
                if min_count is not None:
                    actual = int(rows[0][0]) if rows else 0
                    if actual < min_count:
                        det = f"Expected count >= {min_count}, got {actual}"
                        failed.append((case_id, sql_template, det))
                        tdLog.info(
                            f"[{case_id:<9s} POS] FAIL  {sql_template[:70]}  "
                            f"[{_time.monotonic() - t0:.2f}s]  {det}"
                        )
                    else:
                        tdLog.info(
                            f"[{case_id:<9s} POS] PASS  {sql_template[:70]}  "
                            f"[{_time.monotonic() - t0:.2f}s]  count={actual} >= {min_count}"
                        )
                    continue
                if not ordered:
                    rows = sorted(rows, key=lambda r: [str(x) for x in r])
                elapsed = _time.monotonic() - t0
                tdLog.info(
                    f"[{case_id:<9s} POS] PASS  {sql_template[:70]}  "
                    f"[{elapsed:.2f}s]  rows={len(rows)}"
                )
            except Exception as exc:
                elapsed = _time.monotonic() - t0
                _ea = getattr(exc, "args", ())
                errno = (
                    _ea[-1] if len(_ea) >= 2 and isinstance(_ea[-1], int) else
                    getattr(exc, "errno", None)
                )
                qerr = QueryError(errno, str(_ea[0]) if _ea else str(exc), sql, exc)
                tdLog.info(
                    f"[{case_id:<9s} POS] FAIL  {sql_template[:70]}  "
                    f"[{elapsed:.2f}s]  {qerr.err_info}"
                )
                failed.append((case_id, sql_template, str(qerr)))

            serialized = parity_serialize_case(
                case_id, sql_template, True, rows, qerr, float_cols, ordered)
            serialized_blocks.append(serialized)

        # ── baseline comparison ──────────────────────────────────────────────
        baseline_file = self._BASELINE_FILE
        if baseline_file:
            tmp_file    = baseline_file + ".tmp"
            tmp_content = "\n".join(serialized_blocks) + "\n"
            os.makedirs(os.path.dirname(baseline_file), exist_ok=True)
            with open(tmp_file, "w") as f:
                f.write(tmp_content)
            tdLog.info(f"Temp result file written: {tmp_file}")

            if os.path.isfile(baseline_file):
                with open(baseline_file, "r") as f:
                    baseline_content = f.read()
                if tmp_content != baseline_content:
                    tmp_lines  = tmp_content.splitlines()
                    base_lines = baseline_content.splitlines()
                    diff_line  = -1
                    for li in range(max(len(tmp_lines), len(base_lines))):
                        tl = tmp_lines[li]  if li < len(tmp_lines)  else "<EOF>"
                        bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                        if tl != bl:
                            diff_line = li + 1
                            break
                    baseline_err = (
                        f"Regression baseline mismatch!\n"
                        f"  baseline: {baseline_file}\n"
                        f"  actual  : {tmp_file}\n"
                        f"  first diff at line {diff_line}:\n"
                        f"    baseline: {bl!r}\n"
                        f"    actual  : {tl!r}\n"
                        f"  Run: diff {baseline_file} {tmp_file}"
                    )
                    tdLog.info(f"BASELINE MISMATCH: {baseline_err}")
                    failed.append(("<baseline>", "<baseline>", baseline_err))
                else:
                    tdLog.info("Baseline comparison: OK (matches static baseline)")
                    try:
                        os.remove(tmp_file)
                    except OSError:
                        pass
            else:
                shutil.copy(tmp_file, baseline_file)
                tdLog.info(f"Baseline file created: {baseline_file}")
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

        return failed

    def _run_error_cases(self, cases):
        """Run negative (error-expected) source-specific cases.

        i18 cases are skipped automatically when InfluxDB 1.8 is not available.

        Returns a list of ``(case_id, sql_template, detail)`` failure tuples.
        """
        failed: list[tuple[str, str, str]] = []
        for case_id, sql_template, expected_errno in cases:
            # i18 cases are conditional on InfluxDB 1.8 availability
            if "{I18}" in sql_template and not self._i18_available:
                tdLog.info(
                    f"[{case_id:<9s} NEG] SKIP  InfluxDB 1.8 not configured "
                    f"(set FQ_INFLUX_VERSIONS=1.8 to enable)"
                )
                continue

            sql = self._fmt(sql_template)
            t0  = _time.monotonic()
            try:
                tdSql.error(sql, expectedErrno=expected_errno)
                elapsed = _time.monotonic() - t0
                tdLog.info(
                    f"[{case_id:<9s} NEG] PASS  {sql_template[:70]}  "
                    f"[{elapsed:.2f}s]"
                )
            except Exception as exc:
                elapsed = _time.monotonic() - t0
                tdLog.info(
                    f"[{case_id:<9s} NEG] FAIL  {sql_template[:70]}  "
                    f"[{elapsed:.2f}s]  {exc}"
                )
                failed.append((case_id, sql_template, str(exc)))
        return failed

    # -----------------------------------------------------------------------
    # Test methods
    # -----------------------------------------------------------------------

    def test_fq_source_specific_queries(self):
        """Source-specific positive query cases.

        Runs SQL templates against their respective external sources and compares
        results against a stable baseline file (ans/test_fq_18_source_specific.txt).
        The baseline is created automatically on the first run.
        Cases with ``min_count`` use threshold assertion instead of baseline.

        Cases:
          itag-01   DISTINCT host, region from InfluxDB cpu → 2 rows (h1/us, h2/eu)
          itag-02   AVG(usage) PARTITION BY host from InfluxDB → 40.0 / 15.0
          i04-01    COUNT(*) PARTITION BY tbname from InfluxDB cpu → 2 rows of 2
          i04-02    AVG(usage_idle) PARTITION BY tbname → {77.5, 87.5} (unordered)
          minfo-01  MySQL INFORMATION_SCHEMA.TABLES count ≥ 1
          vw-m-01   MySQL VIEW v_summary → 2 rows
          vw-p-01   PostgreSQL VIEW v_summary → 2 rows

        Catalog: - Query:FederatedSourceSpecific

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-06-xx wpan  Migrated from fq_04 test_fq_sql_influxdb_tags_partition,
                                fq_04 test_fq_sql_infoschema,
                                and fq_06 test_fq_push_s04_influx_partition_tbname_to_groupby_tags
        """
        failed = self._run_specific_cases(
            [(cid, sql, opts)
             for cid, sql, pos, _, opts in _CASES
             if pos]
        )
        if failed:
            all_errors = "\n".join(
                f"\n[{cid}] {sql}\n  {det}" for cid, sql, det in failed
            )
            raise AssertionError(
                f"{len(failed)} case(s) failed:\n{all_errors}"
            )

    def test_fq_source_specific_errors(self):
        """Source-specific error cases.

        Cases:
          i-neg-01 InfluxDB SELECT ts (column not found)      → error
          i-neg-02 InfluxDB ORDER BY ts (column not found)    → error
          m04-01   MySQL PARTITION BY TBNAME → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          m04-02   MySQL SELECT TBNAME       → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          p04-01   PG PARTITION BY TBNAME    → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          p04-02   PG SELECT TBNAME          → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          i04-03   InfluxDB SELECT TBNAME    → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          i18-01   InfluxDB 1.8 query        → TSDB_CODE_OPS_NOT_SUPPORT
                   (skipped when "1.8" is not in FQ_INFLUX_VERSIONS)

        Catalog: - Query:FederatedSourceSpecific

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-06-xx wpan  Migrated from fq_06 test_fq_push_s04_influx_partition_tbname_to_groupby_tags
                                and fq_05 test_fq_local_033
        """
        failed = self._run_error_cases(
            [(cid, sql, errno)
             for cid, sql, pos, errno, _ in _CASES
             if not pos]
        )
        if failed:
            all_errors = "\n".join(
                f"\n[{cid}] {sql}\n  {det}" for cid, sql, det in failed
            )
            raise AssertionError(
                f"{len(failed)} error case(s) failed:\n{all_errors}"
            )
