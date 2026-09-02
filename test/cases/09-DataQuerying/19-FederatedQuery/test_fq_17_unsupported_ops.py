"""
test_fq_17_unsupported_ops.py

Unsupported features and operations in federated query.

Consolidates all "error-expected" cases from test_fq_04, test_fq_05 and
test_fq_06 that involve features or statements not supported on external
sources:
  - test_fq_sql_tbname_pseudo_column  (fq_04): TBNAME pseudo-column
  - test_fq_local_022                 (fq_05): CREATE STREAM denied
  - test_fq_local_023                 (fq_05): CREATE TOPIC denied
  - test_fq_local_024                 (fq_05): INSERT denied
  - test_fq_local_025                 (fq_05): overwrite-INSERT denied
  - test_fq_local_026                 (fq_05): DELETE denied
  - test_fq_local_027                 (fq_05): CREATE TABLE denied
  - test_fq_local_032                 (fq_05): reserved type='tdengine'
  - test_fq_local_034                 (fq_05): write error-code stability
  - test_fq_local_s01                 (fq_05): TBNAME pseudo-column variants
  - test_fq_local_s03                 (fq_05): TAGS keyword denied

Test structure
--------------
Two module-level case arrays drive the tests.  SQL templates contain
``{M}``, ``{P}``, and ``{I}`` as placeholder tokens for the MySQL,
PostgreSQL, and InfluxDB external source names respectively.  The test
methods substitute the real names at runtime.

  _QUERY_CASES — SELECT / query-like statements that must fail.
  _STMT_CASES  — Non-query statements (DML / DDL) that must fail.

Each entry:  (case_id, sql_template, expected_errno)

Minimal data setup
------------------
A dedicated database is created in each external system with a single
``orders`` table/measurement (3 rows) so that:
  * TBNAME tests target a real table and receive EXT_SYNTAX_UNSUPPORTED
    (not EXT_TABLE_NOT_EXIST).
  * Write / stream / topic tests just need a live connection — the write
    guard fires before any table lookup.
  * Parser-level errors (non-ts JOIN, TAGS, correlated subquery) need no
    real data at all.

Catalog: - Query:FederatedLocal
Since: v3.4.0.0
Labels: common,ci
"""

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
    TSDB_CODE_PAR_INVALID_EXPR_SUBQ,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_STREAM_NOT_SUPPORTED,
    TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED,
    TSDB_CODE_EXT_FEATURE_DISABLED,
)

# ---------------------------------------------------------------------------
# Case array 1: unsupported query statements (SELECT-like, must fail)
# ---------------------------------------------------------------------------
# Placeholders: {M}=MySQL source, {P}=PostgreSQL source, {I}=InfluxDB source.
# All three sources have an `orders` table with 3 rows so that TBNAME tests
# get EXT_SYNTAX_UNSUPPORTED (not EXT_TABLE_NOT_EXIST).
_QUERY_CASES = [
    # -- TBNAME pseudo-column (FQ-SQL-TBNAME, FQ-LOCAL-s01) -----------------
    # SELECT TBNAME on MySQL → syntax unsupported on external source
    ("uq-01-m", "SELECT tbname FROM {M}.orders ORDER BY ts",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # GROUP BY TBNAME on MySQL
    ("uq-02-m", "SELECT tbname, COUNT(*) FROM {M}.orders GROUP BY tbname",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # WHERE TBNAME on MySQL
    ("uq-03-m", "SELECT COUNT(*) FROM {M}.orders WHERE tbname = 'orders'",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # SELECT TBNAME on PostgreSQL
    ("uq-01-p", "SELECT tbname FROM {P}.orders ORDER BY ts",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # GROUP BY TBNAME on PostgreSQL
    ("uq-02-p", "SELECT tbname, COUNT(*) FROM {P}.orders GROUP BY tbname",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # WHERE TBNAME on PostgreSQL
    ("uq-03-p", "SELECT COUNT(*) FROM {P}.orders WHERE tbname = 'orders'",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # SELECT TBNAME on InfluxDB
    ("uq-01-i", "SELECT tbname FROM {I}.orders ORDER BY time",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # WHERE TBNAME on InfluxDB
    ("uq-03-i", "SELECT COUNT(*) FROM {I}.orders WHERE tbname = 'orders'",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),

    # -- TAGS keyword on relational sources (FQ-LOCAL-s03) ------------------
    # TAGS is a TDengine super-table concept; MySQL/PG/InfluxDB have no
    # tag metadata → rejected at parser level (parser error)
    ("uq-04-m", "SELECT tags FROM {M}.orders",
     TSDB_CODE_PAR_SYNTAX_ERROR),
    ("uq-04-p", "SELECT tags FROM {P}.orders",
     TSDB_CODE_PAR_SYNTAX_ERROR),
    ("uq-04-i", "SELECT tags FROM {I}.orders",
     TSDB_CODE_PAR_SYNTAX_ERROR),

    # -- Non-timestamp JOIN (parser-level, no table lookup needed) ----------
    # TDengine JOIN must use timestamp-equality or ASOF/WINDOW semantics.
    # Any JOIN with a non-ts ON-condition is rejected before table lookup.
    ("uq-05-m", "SELECT a.ts FROM {M}.orders a JOIN {M}.orders b ON a.id = b.id",
     TSDB_CODE_PAR_NOT_SUPPORT_JOIN),
    ("uq-05-p", "SELECT a.ts FROM {P}.orders a JOIN {P}.orders b ON a.id = b.id",
     TSDB_CODE_PAR_NOT_SUPPORT_JOIN),
    ("uq-05-i", "SELECT a.time FROM {I}.orders a JOIN {I}.orders b ON a.id = b.id",
     TSDB_CODE_PAR_NOT_SUPPORT_JOIN),
    # Cross-source non-ts JOIN (MySQL ↔ PostgreSQL)
    ("uq-06-mp", "SELECT a.ts FROM {M}.orders a JOIN {P}.orders b ON a.id = b.id",
     TSDB_CODE_PAR_NOT_SUPPORT_JOIN),
    # Cross-source non-ts JOIN (MySQL ↔ InfluxDB)
    ("uq-06-mi", "SELECT a.ts FROM {M}.orders a JOIN {I}.orders b ON a.id = b.id",
     TSDB_CODE_PAR_NOT_SUPPORT_JOIN),

    # -- Correlated subquery (planner-level, no table lookup needed) --------
    # EXISTS correlated subquery not supported on external sources
    ("uq-07-m",
     "SELECT id FROM {M}.orders o WHERE EXISTS "
     "(SELECT 1 FROM {M}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
    ("uq-07-p",
     "SELECT id FROM {P}.orders o WHERE EXISTS "
     "(SELECT 1 FROM {P}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
    # NOT EXISTS correlated subquery
    ("uq-08-m",
     "SELECT id FROM {M}.orders o WHERE NOT EXISTS "
     "(SELECT 1 FROM {M}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
]

# ---------------------------------------------------------------------------
# Case array 2: unsupported non-query statements (DML / DDL, must fail)
# ---------------------------------------------------------------------------
_STMT_CASES = [
    # -- INSERT denied (FQ-LOCAL-024, FQ-LOCAL-025, FQ-LOCAL-034) -----------
    # External tables are read-only; write guard fires before table lookup.
    ("us-01-m", "INSERT INTO {M}.orders VALUES (1704067200000, 1, 100.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-01-p", "INSERT INTO {P}.orders VALUES (1704067200000, 1, 100.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-01-i", "INSERT INTO {I}.orders VALUES (1704067200000, 1, 100.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    # Overwrite-style INSERT (same-timestamp "update" semantics) also denied
    ("us-02-m", "INSERT INTO {M}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-p", "INSERT INTO {P}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-i", "INSERT INTO {I}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),

    # -- DELETE denied (FQ-LOCAL-026) ---------------------------------------
    ("us-03-m", "DELETE FROM {M}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-p", "DELETE FROM {P}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-i", "DELETE FROM {I}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),

    # -- CREATE TABLE denied (FQ-LOCAL-027) ---------------------------------
    # DDL write attempt on external namespace → same write-denial code
    ("us-04-m", "CREATE TABLE {M}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-p", "CREATE TABLE {P}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-i", "CREATE TABLE {I}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),

    # -- CREATE STREAM denied (FQ-LOCAL-022) --------------------------------
    ("us-05-m",
     "CREATE STREAM fq17_s05m TRIGGER AT_ONCE INTO fq17_o05m "
     "AS SELECT COUNT(*) FROM {M}.orders INTERVAL(1m)",
     TSDB_CODE_EXT_STREAM_NOT_SUPPORTED),
    ("us-05-p",
     "CREATE STREAM fq17_s05p TRIGGER AT_ONCE INTO fq17_o05p "
     "AS SELECT COUNT(*) FROM {P}.orders INTERVAL(1m)",
     TSDB_CODE_EXT_STREAM_NOT_SUPPORTED),
    ("us-05-i",
     "CREATE STREAM fq17_s05i TRIGGER AT_ONCE INTO fq17_o05i "
     "AS SELECT COUNT(*) FROM {I}.orders INTERVAL(1m)",
     TSDB_CODE_EXT_STREAM_NOT_SUPPORTED),

    # -- CREATE TOPIC denied (FQ-LOCAL-023) ---------------------------------
    ("us-06-m", "CREATE TOPIC fq17_t06m AS SELECT * FROM {M}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-p", "CREATE TOPIC fq17_t06p AS SELECT * FROM {P}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-i", "CREATE TOPIC fq17_t06i AS SELECT * FROM {I}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),

    # -- Reserved source type (FQ-LOCAL-032) --------------------------------
    # TYPE='tdengine' is reserved and not yet delivered → feature disabled
    ("us-07",
     "CREATE EXTERNAL SOURCE fq17_td_src TYPE='tdengine' "
     "HOST='192.0.2.1' PORT=6030 USER='root' PASSWORD='taosdata'",
     TSDB_CODE_EXT_FEATURE_DISABLED),
]


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestFq17UnsupportedOps(FederatedQueryVersionedMixin):
    """FQ-LOCAL-022~027/032/034/s01/s03, FQ-SQL-TBNAME: unsupported ops."""

    # Dedicated external source and database names
    _SRC_M         = "fq17_src_m"
    _SRC_P         = "fq17_src_p"
    _SRC_I         = "fq17_src_i"
    _MYSQL_DB      = "fq17_m"
    _PG_DB         = "fq17_p"
    _INFLUX_BUCKET = "fq17_i"

    _class_setup_done = False
    _sql_retry_patched = False
    _orig_tdsql_execute = None
    _orig_tdsql_query = None
    _orig_tdsql_query_success_failed = None
    _orig_tdsql_querySuccessailed = None

    @classmethod
    def _disable_sql_retries_for_this_file(cls):
        """Disable tdSql retry loops for this test file only.

        We force queryTimes=1 on retry-capable methods so any transient
        external-env issue surfaces immediately instead of being retried.
        """
        if cls._sql_retry_patched:
            return

        cls._orig_tdsql_execute = tdSql.execute
        cls._orig_tdsql_query = tdSql.query
        cls._orig_tdsql_query_success_failed = tdSql.query_success_failed
        cls._orig_tdsql_querySuccessailed = tdSql.querySuccessailed

        def _execute_no_retry(sql, queryTimes=10, show=False):
            # Single-shot execute: no retry loop, no "Try to execute ... again" log.
            tdSql.sql = sql
            if show:
                tdLog.info(sql)
            try:
                tdSql.affectedRows = tdSql.cursor.execute(sql)
                return tdSql.affectedRows
            except Exception as e:
                raise Exception(repr(e))

        def _query_no_retry(sql, row_tag=None, queryTimes=10,
                            count_expected_res=None, show=False, exit=True):
            return cls._orig_tdsql_query(
                sql,
                row_tag=row_tag,
                queryTimes=1,
                count_expected_res=count_expected_res,
                show=show,
                exit=exit,
            )

        def _query_success_failed_no_retry(sql, row_tag=None, queryTimes=10,
                                           count_expected_res=None,
                                           expectErrInfo=None,
                                           fullMatched=True):
            return cls._orig_tdsql_query_success_failed(
                sql,
                row_tag=row_tag,
                queryTimes=1,
                count_expected_res=count_expected_res,
                expectErrInfo=expectErrInfo,
                fullMatched=fullMatched,
            )

        def _querySuccessailed_no_retry(sql, row_tag=None, queryTimes=10,
                                        count_expected_res=None,
                                        expectErrInfo=None,
                                        fullMatched=True):
            return cls._orig_tdsql_querySuccessailed(
                sql,
                row_tag=row_tag,
                queryTimes=1,
                count_expected_res=count_expected_res,
                expectErrInfo=expectErrInfo,
                fullMatched=fullMatched,
            )

        tdSql.execute = _execute_no_retry
        tdSql.query = _query_no_retry
        tdSql.query_success_failed = _query_success_failed_no_retry
        tdSql.querySuccessailed = _querySuccessailed_no_retry
        cls._sql_retry_patched = True

    @classmethod
    def _restore_sql_retries_for_this_file(cls):
        """Restore tdSql retry behavior after this test class finishes."""
        if not cls._sql_retry_patched:
            return

        if cls._orig_tdsql_execute is not None:
            tdSql.execute = cls._orig_tdsql_execute
        if cls._orig_tdsql_query is not None:
            tdSql.query = cls._orig_tdsql_query
        if cls._orig_tdsql_query_success_failed is not None:
            tdSql.query_success_failed = cls._orig_tdsql_query_success_failed
        if cls._orig_tdsql_querySuccessailed is not None:
            tdSql.querySuccessailed = cls._orig_tdsql_querySuccessailed

        cls._sql_retry_patched = False

    # -------------------------------------------------------------------
    # Setup / teardown
    # -------------------------------------------------------------------

    def setup_class(self):
        self._disable_sql_retries_for_this_file()
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        self._cleanup_src(self._SRC_M, self._SRC_P, self._SRC_I)
        try:
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), self._MYSQL_DB)
        except Exception:
            pass
        try:
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), self._PG_DB)
        except Exception:
            pass
        try:
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), self._INFLUX_BUCKET)
        except Exception:
            pass
        # Clean up any stray stream/topic objects left by failed assertions
        for s in ["fq17_s05m", "fq17_s05p", "fq17_s05i"]:
            try:
                tdSql.execute(f"DROP STREAM IF EXISTS {s}")
            except Exception:
                pass
        for t in ["fq17_t06m", "fq17_t06p", "fq17_t06i"]:
            try:
                tdSql.execute(f"DROP TOPIC IF EXISTS {t}")
            except Exception:
                pass
        try:
            tdSql.execute("DROP EXTERNAL SOURCE IF EXISTS fq17_td_src")
        except Exception:
            pass
        TestFq17UnsupportedOps._class_setup_done = False
        ExtSrcEnv.teardown_env()
        self._restore_sql_retries_for_this_file()

    def setup_method(self, method):
        """Create external sources once (shared across all test methods)."""
        if TestFq17UnsupportedOps._class_setup_done:
            return

        m_src = self._SRC_M
        p_src = self._SRC_P
        i_src = self._SRC_I
        m_db  = self._MYSQL_DB
        p_db  = self._PG_DB
        i_bkt = self._INFLUX_BUCKET

        # -- MySQL -----------------------------------------------------------
        self._cleanup_src(m_src)
        ExtSrcEnv.mysql_kill_sleeping_connections_cfg(self._mysql_cfg())
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
            "DROP TABLE IF EXISTS `orders`",
            "CREATE TABLE `orders` ("
            "  ts DATETIME(3) PRIMARY KEY, id INT, amount FLOAT)",
            "INSERT INTO `orders` VALUES "
            "('2024-01-01 00:00:00.000', 1, 100.0),"
            "('2024-01-01 00:01:00.000', 2, 200.0),"
            "('2024-01-01 00:02:00.000', 3, 300.0)",
        ])
        self._mk_mysql_real(m_src, database=m_db)

        # -- PostgreSQL ------------------------------------------------------
        self._cleanup_src(p_src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
            "DROP TABLE IF EXISTS orders",
            "CREATE TABLE orders ("
            "  ts TIMESTAMP PRIMARY KEY, id INT, amount FLOAT8)",
            "INSERT INTO orders VALUES "
            "('2024-01-01 00:00:00', 1, 100.0),"
            "('2024-01-01 00:01:00', 2, 200.0),"
            "('2024-01-01 00:02:00', 3, 300.0)",
        ])
        self._mk_pg_real(p_src, database=p_db, schema="public")

        # -- InfluxDB --------------------------------------------------------
        self._cleanup_src(i_src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_bkt)
        _T0 = 1704067200_000_000_000   # 2024-01-01 00:00:00 UTC in ns
        _M  = 60_000_000_000           # 1 minute in ns
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_bkt, [
            f"orders id=1i,amount=100.0 {_T0 + 0 * _M}",
            f"orders id=2i,amount=200.0 {_T0 + 1 * _M}",
            f"orders id=3i,amount=300.0 {_T0 + 2 * _M}",
        ])
        self._mk_influx_real(i_src, database=i_bkt)

        TestFq17UnsupportedOps._class_setup_done = True

    # -------------------------------------------------------------------
    # Helper: substitute {M}, {P}, {I} in an SQL template
    # -------------------------------------------------------------------

    def _expand(self, sql_template):
        return (sql_template
                .replace("{M}", self._SRC_M)
                .replace("{P}", self._SRC_P)
                .replace("{I}", self._SRC_I))

    # -------------------------------------------------------------------
    # Test 1: unsupported query statements
    # -------------------------------------------------------------------

    def test_fq_uns_queries(self):
        """FQ-17-QUERY: all unsupported SELECT/query statements must fail.

        Iterates _QUERY_CASES.  Each entry (case_id, sql_template,
        expected_errno) is expanded with the real source names and then
        checked via tdSql.error().

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        for case_id, sql_tpl, errno in _QUERY_CASES:
            sql = self._expand(sql_tpl)
            tdLog.debug(f"[{case_id}] {sql}")
            tdSql.error(sql, expectedErrno=errno)

    # -------------------------------------------------------------------
    # Test 2: unsupported non-query statements
    # -------------------------------------------------------------------

    def test_fq_uns_stmts(self):
        """FQ-17-STMT: all unsupported DML/DDL statements must fail.

        Iterates _STMT_CASES.  Each entry (case_id, sql_template,
        expected_errno) is expanded with the real source names and then
        checked via tdSql.error().

        Catalog: - Query:FederatedLocal
        Since: v3.4.0.0
        Labels: common,ci
        """
        for case_id, sql_tpl, errno in _STMT_CASES:
            sql = self._expand(sql_tpl)
            tdLog.debug(f"[{case_id}] {sql}")
            tdSql.error(sql, expectedErrno=errno)
