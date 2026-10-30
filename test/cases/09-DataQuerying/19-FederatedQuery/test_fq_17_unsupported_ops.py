# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
  _JOIN_CASES  — Same-source and cross-source joins that must succeed.

Entry format:  ``_QUERY_CASES`` / ``_STMT_CASES`` use
``(case_id, sql_template, expected_errno)``; ``_JOIN_CASES`` uses
``(case_id, sql_template, expected_rows)``.

Minimal data setup
------------------
A dedicated database is created in each external system with a single
``orders`` table/measurement (3 rows) so that:
  * TBNAME tests target a real table and receive EXT_SYNTAX_UNSUPPORTED
    (not EXT_TABLE_NOT_EXIST).
  * Write / stream / topic tests just need a live connection — the write
    guard fires before any table lookup.
  * Parser-level TAGS errors and planner-level correlated-subquery errors need
    no real data at all.

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
    TSDB_CODE_PAR_INVALID_EXPR_SUBQ,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_STREAM_NOT_SUPPORTED,
    TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED,
    TSDB_CODE_EXT_FEATURE_DISABLED,
    TSDB_CODE_INVALID_PARA,
    _code,
)

# Concrete codes the Oracle-only rows below need that the shared module does
# not re-export.  Both names exist in taoserror.h; resolving them here keeps
# the assertion concrete instead of accepting any error the way some of the
# generic rows below do.
TSDB_CODE_EXT_SOURCE_WRITE_NOT_SUPPORTED = _code(
    'TSDB_CODE_EXT_SOURCE_WRITE_NOT_SUPPORTED')
TSDB_CODE_TXN_NOT_IN_PROGRESS = _code('TSDB_CODE_TXN_NOT_IN_PROGRESS')

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
    # SELECT tbname / GROUP BY tbname / WHERE tbname on SQL Server
    ("uq-01-s", "SELECT tbname FROM {S}.orders ORDER BY ts",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    ("uq-02-s", "SELECT tbname, COUNT(*) FROM {S}.orders GROUP BY tbname",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    ("uq-03-s", "SELECT COUNT(*) FROM {S}.orders WHERE tbname = 'orders'",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # Same three on Oracle (FQOR-UNSUP-001).  Oracle has no pseudo-columns
    # with these names, so the rejection must come from the federation layer
    # and must not be turned into a remote "invalid identifier" error.
    ("uq-01-o", "SELECT tbname FROM {O}.orders ORDER BY ts",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    ("uq-02-o", "SELECT tbname, COUNT(*) FROM {O}.orders GROUP BY tbname",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    ("uq-03-o", "SELECT COUNT(*) FROM {O}.orders WHERE tbname = 'orders'",
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
    ("uq-04-s", "SELECT tags FROM {S}.orders",
     TSDB_CODE_PAR_SYNTAX_ERROR),
    ("uq-04-o", "SELECT tags FROM {O}.orders",
     TSDB_CODE_PAR_SYNTAX_ERROR),

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
    ("uq-07-s",
     "SELECT id FROM {S}.orders o WHERE EXISTS "
     "(SELECT 1 FROM {S}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
    ("uq-08-s",
     "SELECT id FROM {S}.orders o WHERE NOT EXISTS "
     "(SELECT 1 FROM {S}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
    ("uq-07-o",
     "SELECT id FROM {O}.orders o WHERE EXISTS "
     "(SELECT 1 FROM {O}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
    ("uq-08-o",
     "SELECT id FROM {O}.orders o WHERE NOT EXISTS "
     "(SELECT 1 FROM {O}.orders b WHERE b.id = o.id)",
     TSDB_CODE_PAR_INVALID_EXPR_SUBQ),
]

# Ordinary-column equi-joins are supported for external sources. Keep these
# separate from _QUERY_CASES because that list is reserved for error cases.
_JOIN_CASES = [
    ("uq-05-m", "SELECT a.ts FROM {M}.orders a JOIN {M}.orders b ON a.id = b.id", 3),
    ("uq-05-p", "SELECT a.ts FROM {P}.orders a JOIN {P}.orders b ON a.id = b.id", 3),
    ("uq-05-i", "SELECT a.time FROM {I}.orders a JOIN {I}.orders b ON a.id = b.id", 3),
    ("uq-05-s", "SELECT a.ts FROM {S}.orders a JOIN {S}.orders b ON a.id = b.id", 3),
    ("uq-06-mp", "SELECT a.ts FROM {M}.orders a JOIN {P}.orders b ON a.id = b.id", 3),
    ("uq-06-mi", "SELECT a.ts FROM {M}.orders a JOIN {I}.orders b ON a.id = b.id", 3),
    ("uq-06-ms", "SELECT a.ts FROM {M}.orders a JOIN {S}.orders b ON a.id = b.id", 3),
    ("uq-06-ps", "SELECT a.ts FROM {P}.orders a JOIN {S}.orders b ON a.id = b.id", 3),
    ("uq-06-is", "SELECT a.time FROM {I}.orders a JOIN {S}.orders b ON a.id = b.id", 3),
    ("uq-05-o", "SELECT a.ts FROM {O}.orders a JOIN {O}.orders b ON a.id = b.id", 3),
    ("uq-06-mo", "SELECT a.ts FROM {M}.orders a JOIN {O}.orders b ON a.id = b.id", 3),
    ("uq-06-po", "SELECT a.ts FROM {P}.orders a JOIN {O}.orders b ON a.id = b.id", 3),
    ("uq-06-so", "SELECT a.ts FROM {S}.orders a JOIN {O}.orders b ON a.id = b.id", 3),
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
    ("us-01-s", "INSERT INTO {S}.orders VALUES (1704067200000, 1, 100.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    # Overwrite-style INSERT (same-timestamp "update" semantics) also denied
    ("us-02-m", "INSERT INTO {M}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-p", "INSERT INTO {P}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-i", "INSERT INTO {I}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-s", "INSERT INTO {S}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    # Oracle: read-only guard fires the same way (FQOR-UNSUP-002).
    ("us-01-o", "INSERT INTO {O}.orders VALUES (1704067200000, 1, 100.0)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-02-o", "INSERT INTO {O}.orders VALUES (1704067200000, 99, 999.0)",
     TSDB_CODE_EXT_WRITE_DENIED),

    # -- DELETE denied (FQ-LOCAL-026) ---------------------------------------
    ("us-03-m", "DELETE FROM {M}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-p", "DELETE FROM {P}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-i", "DELETE FROM {I}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-s", "DELETE FROM {S}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-03-o", "DELETE FROM {O}.orders WHERE id = 1",
     TSDB_CODE_EXT_WRITE_DENIED),

    # -- CREATE TABLE denied (FQ-LOCAL-027) ---------------------------------
    # DDL write attempt on external namespace → same write-denial code
    ("us-04-m", "CREATE TABLE {M}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-p", "CREATE TABLE {P}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-i", "CREATE TABLE {I}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-s", "CREATE TABLE {S}.new_table (ts TIMESTAMP, v INT)",
     TSDB_CODE_EXT_WRITE_DENIED),
    ("us-04-o", "CREATE TABLE {O}.new_table (ts TIMESTAMP, v INT)",
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
    ("us-05-s",
     "CREATE STREAM fq17_s05s TRIGGER AT_ONCE INTO fq17_o05s "
     "AS SELECT COUNT(*) FROM {S}.orders INTERVAL(1m)",
     TSDB_CODE_EXT_STREAM_NOT_SUPPORTED),
    ("us-05-o",
     "CREATE STREAM fq17_s05o TRIGGER AT_ONCE INTO fq17_o05o "
     "AS SELECT COUNT(*) FROM {O}.orders INTERVAL(1m)",
     TSDB_CODE_EXT_STREAM_NOT_SUPPORTED),

    # -- CREATE TOPIC denied (FQ-LOCAL-023) ---------------------------------
    ("us-06-m", "CREATE TOPIC fq17_t06m AS SELECT * FROM {M}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-p", "CREATE TOPIC fq17_t06p AS SELECT * FROM {P}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-i", "CREATE TOPIC fq17_t06i AS SELECT * FROM {I}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-s", "CREATE TOPIC fq17_t06s AS SELECT * FROM {S}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),
    ("us-06-o", "CREATE TOPIC fq17_t06o AS SELECT * FROM {O}.orders",
     TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED),

    # -- Reserved source type (FQ-LOCAL-032) --------------------------------
    # TYPE='tdengine' is reserved and not yet delivered → feature disabled
    ("us-07",
     "CREATE EXTERNAL SOURCE fq17_td_src TYPE='tdengine' "
     "HOST='192.0.2.1' PORT=6030 USER='root' PASSWORD='taosdata'",
     TSDB_CODE_EXT_FEATURE_DISABLED),
]

# ---------------------------------------------------------------------------
# Case array 3: Oracle-only unsupported statements (FQOR-UNSUP-003/004)
# ---------------------------------------------------------------------------
# Unlike most rows above, these pin the exact engine decision instead of
# accepting any error.  Every code below is produced locally, so it does not
# depend on how Oracle happens to phrase a message.
_ORACLE_PLSQL_CASES = [
    # CALL is not part of the TDengine grammar -- the only CALL* token is the
    # user option CALL_PER_SESSION -- so a stored-procedure call dies in the
    # parser before an external source is even resolved.
    ("uo-03-1", "CALL FQ17_SIDE_EFFECT_PROC()", TSDB_CODE_PAR_SYNTAX_ERROR),
    # An anonymous PL/SQL block fails at its first PL/SQL-only keyword for the
    # same reason: nothing is translated and nothing is forwarded.
    ("uo-03-2",
     "BEGIN EXECUTE IMMEDIATE "
     "'INSERT INTO FQ17_SIDE_EFFECT_LOG VALUES (1)'; END;",
     TSDB_CODE_PAR_SYNTAX_ERROR),
    # COMMIT / ROLLBACK without BEGIN are stopped by the client-side
    # transaction state check (parTranslater.c translateTransStmt), so
    # transaction control never reaches the remote session either.
    ("uo-03-3", "COMMIT", TSDB_CODE_TXN_NOT_IN_PROGRESS),
    ("uo-03-4", "ROLLBACK", TSDB_CODE_TXN_NOT_IN_PROGRESS),
]

_ORACLE_STREAM_TOPIC_CASES = [
    # A single-window external stream is a supported shape in this branch, so
    # the rejected form is the nested (multi-layer) window plan: mnode cannot
    # give an external trigger the local vgroup / trigger-task identity the
    # multi-layer runtime needs, and tValidateStreamWindowPlan() rejects every
    # external trigger with TSDB_CODE_INVALID_PARA (streamWindowPlan.c, called
    # from mndStreamRejectAuthoritativeExtTrigger).
    ("uo-04-1",
     "CREATE STREAM fq17_uo04o WINDOW ("
     "INTERVAL(1m) SLIDING(1m) AS w_outer, COUNT_WINDOW(2,1)) "
     "FROM {O}.ORDERS "
     "INTO {DB}.fq17_uo04_sink AS SELECT _twstart, count(*) FROM %%trows",
     TSDB_CODE_INVALID_PARA),
    # A topic is a subscription over local vnodes; the external table
    # translator rejects the reference before any remote SQL is built.
    ("uo-04-2", "CREATE TOPIC fq17_uo04_t AS SELECT * FROM {O}.ORDERS",
     TSDB_CODE_EXT_SYNTAX_UNSUPPORTED),
    # INSERT ... SELECT is rejected only when the *written* table is the
    # external one: the write guard runs while the target is resolved
    # (translateInsertTable), before the query part is translated.
    ("uo-04-3",
     "INSERT INTO {O}.ORDERS SELECT 1704067200000, 9, 900.0 "
     "FROM {DB}.fq17_uo04_local",
     TSDB_CODE_EXT_SOURCE_WRITE_NOT_SUPPORTED),
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
    _SRC_S         = "fq17_src_s"
    _SRC_O         = "fq17_src_o"
    _MYSQL_DB      = "fq17_m"
    _PG_DB         = "fq17_p"
    _INFLUX_BUCKET = "fq17_i"
    _SQLSERVER_DB  = "fq17_s"

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
        self._cleanup_src(self._SRC_M, self._SRC_P, self._SRC_I, self._SRC_S,
                          self._SRC_O)
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
        try:
            ExtSrcEnv.sqlserver_drop_db(self._SQLSERVER_DB)
        except Exception:
            pass
        try:
            ExtSrcEnv.oracle_drop_table_cfg(self._oracle_cfg(), "ORDERS")
        except Exception:
            pass
        # Clean up any stray stream/topic objects left by failed assertions
        for s in ["fq17_s05m", "fq17_s05p", "fq17_s05i", "fq17_s05s",
                  "fq17_s05o"]:
            try:
                tdSql.execute(f"DROP STREAM IF EXISTS {s}")
            except Exception:
                pass
        for t in ["fq17_t06m", "fq17_t06p", "fq17_t06i", "fq17_t06s",
                  "fq17_t06o"]:
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

        # -- SQL Server ------------------------------------------------------
        s_src = self._SRC_S
        s_db  = self._SQLSERVER_DB
        self._cleanup_src(s_src)
        ExtSrcEnv.sqlserver_create_db(s_db)
        ExtSrcEnv.sqlserver_exec(s_db, [
            "IF OBJECT_ID('dbo.orders') IS NOT NULL DROP TABLE dbo.orders",
            "CREATE TABLE dbo.orders (ts datetime2(3) NOT NULL PRIMARY KEY, id INT, amount FLOAT)",
            "INSERT INTO dbo.orders VALUES "
            "('2024-01-01 00:00:00.000', 1, 100.0),"
            "('2024-01-01 00:01:00.000', 2, 200.0),"
            "('2024-01-01 00:02:00.000', 3, 300.0)",
        ])
        self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")

        # -- Oracle ----------------------------------------------------------
        # Same three-row `orders` shape; the table name is unquoted in the
        # remote DDL so it lands upper-cased in the data dictionary.
        o_src = self._SRC_O
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        ExtSrcEnv.oracle_exec_cfg(o_cfg, [
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ORDERS'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;",
            "CREATE TABLE ORDERS ("
            " TS TIMESTAMP(3) PRIMARY KEY, ID NUMBER(10,0), AMOUNT BINARY_DOUBLE)",
            "INSERT INTO ORDERS VALUES "
            " (TIMESTAMP '2024-01-01 00:00:00.000', 1, 100.0)",
            "INSERT INTO ORDERS VALUES "
            " (TIMESTAMP '2024-01-01 00:01:00.000', 2, 200.0)",
            "INSERT INTO ORDERS VALUES "
            " (TIMESTAMP '2024-01-01 00:02:00.000', 3, 300.0)",
            "COMMIT",
        ])
        self._mk_oracle_real(o_src, database=o_cfg.service,
                             schema=ExtSrcEnv.ORACLE_USER.upper())

        TestFq17UnsupportedOps._class_setup_done = True

    # -------------------------------------------------------------------
    # Helper: substitute {M}, {P}, {I} in an SQL template
    # -------------------------------------------------------------------

    def _expand(self, sql_template, local_db=None):
        sql = (sql_template
               .replace("{M}", self._SRC_M)
               .replace("{P}", self._SRC_P)
               .replace("{I}", self._SRC_I)
               .replace("{S}", self._SRC_S)
               .replace("{O}", self._SRC_O))
        if local_db is not None:
            sql = sql.replace("{DB}", local_db)
        return sql

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

    def test_fq_ordinary_column_joins(self):
        """Ordinary-column equi-joins succeed for same-source and cross-source FQ.

        Expand each same-source and cross-source ordinary-column join case and verify
        that the returned row count matches its expected value.

        Catalog:
            - Query:FederatedLocal

        Since: v3.4.3.0

        Labels: common,ci
        """
        for case_id, sql_tpl, expected_rows in _JOIN_CASES:
            sql = self._expand(sql_tpl)
            tdLog.debug(f"[{case_id}] {sql}")
            tdSql.query(sql)
            tdSql.checkRows(expected_rows)

    # -------------------------------------------------------------------
    # Test 1b: rejected writes leave the remote object untouched
    # -------------------------------------------------------------------

    def test_fq_uns_oracle_no_side_effects(self):
        """FQOR-UNSUP-002/003: a rejected write never reaches the remote object.

        TDengine must reject INSERT / UPDATE / DELETE against an Oracle
        external table *locally*; Oracle is never sent the statement, so the
        row count and the stored values are unchanged afterwards.  This is the
        property the error code alone cannot prove.

        Catalog: - Query:FederatedLocal

        Since: v3.4.4.0

        Labels: common,ci

        """
        src = self._SRC_O

        def remote_rows():
            return ExtSrcEnv.oracle_query_cfg(
                self._oracle_cfg(), "SELECT COUNT(*) FROM ORDERS").strip()

        before = remote_rows()
        assert before == "3", f"unexpected ORDERS row count: {before}"

        for sql in [
            f"INSERT INTO {src}.ORDERS VALUES (1704067200000, 9, 900.0)",
            f"DELETE FROM {src}.ORDERS WHERE ID = 1",
            f"UPDATE {src}.ORDERS SET AMOUNT = 1.0 WHERE ID = 1",
        ]:
            tdSql.error(sql)

        # The rejection is local, so three rows with the original values remain.
        assert remote_rows() == "3", "a rejected write reached the remote table"
        tdSql.query(f"SELECT ID, AMOUNT FROM {src}.ORDERS ORDER BY ID")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 100.0)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, 300.0)

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

    # -------------------------------------------------------------------
    # Test 3: Oracle PL/SQL and transaction control (FQOR-UNSUP-003)
    # -------------------------------------------------------------------

    def test_fq_uns_oracle_plsql_and_transaction(self):
        """FQOR-UNSUP-003: PL/SQL and transaction control never reach Oracle.

        ``CALL`` and PL/SQL blocks are not part of the TDengine grammar, and
        COMMIT/ROLLBACK without BEGIN trip the client-side transaction state
        check, so each statement is rejected with a concrete parser-level
        code and nothing is forwarded to the remote instance.  The procedure
        below really exists and would append a row if the call leaked
        through, so an empty side-effect log is the property the code alone
        cannot prove.

        Catalog: - Query:FederatedLocal

        Since: v3.4.4.0

        Labels: common,ci

        """
        src = self._SRC_O
        cfg = self._oracle_cfg()
        log_tbl = "FQ17_SIDE_EFFECT_LOG"
        proc = "FQ17_SIDE_EFFECT_PROC"
        login_schema = ExtSrcEnv.ORACLE_USER.upper()

        ExtSrcEnv.oracle_exec_cfg(cfg, [
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {log_tbl}'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;",
            f"CREATE TABLE {log_tbl} (ID NUMBER(10,0))",
        ])
        try:
            # The login schema may not create procedures, so SYSTEM owns the
            # procedure while the log table stays in the login schema (SYSTEM
            # may write there); EXECUTE is granted to the login account so a
            # leaked call would be accepted remotely instead of failing.
            ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
                f"CREATE OR REPLACE PROCEDURE {proc} AS\n"
                "BEGIN\n"
                f"  INSERT INTO {login_schema}.{log_tbl} VALUES (1);\n"
                "END;",
                f"GRANT EXECUTE ON {proc} TO {login_schema}",
            ])

            for case_id, sql_tpl, errno in _ORACLE_PLSQL_CASES:
                tdLog.debug(f"[{case_id}] {sql_tpl}")
                tdSql.error(sql_tpl, expectedErrno=errno)

            # No remote side effect: the log is still empty and the external
            # table the block tried to write still holds its three rows.
            count = ExtSrcEnv.oracle_query_cfg(
                cfg, f"SELECT COUNT(*) FROM {log_tbl}").strip()
            assert count == "0", \
                f"a rejected PL/SQL statement reached Oracle ({count} log rows)"
            tdSql.query(f"SELECT ID, AMOUNT FROM {src}.ORDERS ORDER BY ID")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 300.0)
        finally:
            try:
                ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
                    f"BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE {proc}'; "
                    "EXCEPTION WHEN OTHERS THEN NULL; END;",
                ])
            except Exception:
                pass
            ExtSrcEnv.oracle_drop_table_cfg(cfg, log_tbl)

    # -------------------------------------------------------------------
    # Test 4: Oracle stream / topic / INSERT ... SELECT (FQOR-UNSUP-004)
    # -------------------------------------------------------------------

    def test_fq_uns_oracle_stream_topic_insert(self):
        """FQOR-UNSUP-004: stream and topic rejected, local tables unaffected.

        A stream (nested-window form, see the array comment), a topic and an
        INSERT whose written table is the Oracle source are all rejected with
        a concrete engine code, and ordinary local DDL/DML keeps working.

        The Oracle TS also lists ``INSERT INTO <local> SELECT ... FROM
        <oracle>`` as rejected.  That is not what this branch implements: the
        write guard fires only when the *written* table resolves to an
        external source, and the read-remote/write-local ETL direction is the
        supported shape for every provider (test_fq_04's ins-* cases assert it
        positively).  That direction is therefore asserted with exact rows
        here rather than against an invented error.

        Catalog: - Query:FederatedLocal

        Since: v3.4.4.0

        Labels: common,ci

        """
        src = self._SRC_O
        db = "fq17_local"
        sink = f"{db}.fq17_uo04_local"

        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")
        try:
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            # Oracle NUMBER(10,0) arrives as BIGINT, so the local sink uses
            # BIGINT too: INSERT ... SELECT is type-checked, not cast.
            tdSql.execute(f"CREATE TABLE {sink} (ts TIMESTAMP, id BIGINT, amount DOUBLE)")

            for case_id, sql_tpl, errno in _ORACLE_STREAM_TOPIC_CASES:
                sql = self._expand(sql_tpl, local_db=db)
                tdLog.debug(f"[{case_id}] {sql}")
                tdSql.error(sql, expectedErrno=errno)

            # Supported direction: the remote rows land in the local table.
            tdSql.execute(
                f"INSERT INTO {sink} SELECT TS, ID, AMOUNT FROM {src}.ORDERS")
            tdSql.query(f"SELECT id, amount FROM {sink} ORDER BY ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 100.0)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 200.0)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 300.0)

            # Local tables are unaffected by the rejections above.
            tdSql.execute(f"INSERT INTO {sink} VALUES (1704067800000, 9, 900.0)")
            tdSql.query(f"SELECT id FROM {sink} WHERE id = 9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 9)
        finally:
            try:
                tdSql.execute("DROP STREAM IF EXISTS fq17_uo04o")
            except Exception:
                pass
            try:
                tdSql.execute("DROP TOPIC IF EXISTS fq17_uo04_t")
            except Exception:
                pass
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
