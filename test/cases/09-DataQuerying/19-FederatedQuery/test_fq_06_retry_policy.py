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
test_fq_06_retry_policy.py

Implements FQ-06-001 through FQ-06-004 from TS §6
"Error Classification & Retry Policy" — connection-error retry semantics,
auth-error fast-fail, resource-limit backoff, and availability state-machine
transitions (available / degraded / unavailable).

Design notes:
    - Tests focus on *logical* retry/error-classification behavior, not
      wall-clock latency bounds (those live in test_fq_15_service_disruption).
    - Each test injects a specific failure mode and validates the scheduler's
      response: retry count, state transition, and final query outcome.
    - SQL Server (4th external source; zero pushdown in step 1, so the remote
      SQL is a base scan) adds a parallel path to tests 001-005 and 007-009
      using the shared SQL Server helpers (sqlserver_exec /
      stop|start_sqlserver_instance / _mk_sqlserver_real) plus the lock-based
      slow-query substitute _sqlserver_hold_table_lock — T-SQL has no
      SELECT-embedded SLEEP().  Tests keep the existing MySQL/PG/InfluxDB
      paths unchanged and add the SQL Server path at the end of each test.
    - FQ-06-006 stays MySQL-only: SQL Server native error 916 maps to
      EXT_ACCESS_DENIED, but a table-level permission error (native 229) falls
      through to EXT_REMOTE_INTERNAL, which FQ-06-009 covers.
    - Oracle (5th external source) adds a parallel path to tests 001-005 and
      007-009 plus the FQOR-ERR-xxx methods at the end of the class.  FQ-06-006
      stays MySQL-only for Oracle too: ORA-00942 is raised both for a missing
      object and for a missing privilege, so it cannot carry the table-level
      denial the test needs (its ORA-01031 case is FQOR-ERR-007).  Oracle
      has no SELECT-embedded SLEEP(), so a SYSTEM-owned wrapper around
      DBMS_SESSION.SLEEP is exposed through a view (see
      _oracle_sleep_fixtures); scan-stage errors are injected through
      SYSTEM-owned AUTHID CURRENT_USER helpers, which raise the ORA error under
      the caller's privileges.  Oracle error classification has two shapes that
      the assertions must respect:
        * errors raised while connecting/authenticating surface only the TSDB
          error code to the client — the ORA number never reaches the client
          message, so those cases assert the code (and that the catalog entry
          survives), not the text;
        * errors raised while scanning carry remote_code / remote_sqlstate /
          remote_message (the ORA text is preserved verbatim) and the demotion
          classification tags, so those cases assert both.
      FQOR-ERR-005 (ORA-03113/03114/03135) is not injectable here: the only
      inducible link loss is ORA-00028 from ALTER SYSTEM KILL SESSION, which
      maps to EXT_REMOTE_INTERNAL, and real link-loss recovery is covered by
      test_fq_09 and test_fq_15.  FQOR-ERR-009 (ORA-01795) is not covered: the
      renderer splits IN lists into 1000-item chunks (FQOR-PUSH-012) and 23ai+
      lifts the limit altogether, so the pre-split form cannot be produced.
      FQOR-ERR-011 (IM002 / 01000 / ORA-12557 → EXT_SOURCE_TYPE_NOT_SUPPORT)
      belongs to the dependency-matrix suite test_fq_19_runtime_libs.
"""

import time

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_EXT_PUSHDOWN_FAILED,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_REMOTE_INTERNAL,
    TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
    TSDB_CODE_EXT_CONNECT_FAILED,
    TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
    TSDB_CODE_EXT_AUTH_FAILED,
    TSDB_CODE_EXT_ACCESS_DENIED,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_SOURCE_CHANGED,
    TSDB_CODE_EXT_DB_NOT_EXIST,
    TSDB_CODE_EXT_QUERY_TIMEOUT,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    _code,
)


# The connector's row-level conversion code is defined in taoserror.h but has
# no exported constant in federated_query_common, so resolve it here.
TSDB_CODE_EXT_TYPE_CONVERT_FAILED = _code("TSDB_CODE_EXT_TYPE_CONVERT_FAILED")


# ---------------------------------------------------------------------------
# Module-level constants for external test data
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# Standard 5-row MySQL push_t table
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

# MySQL users + orders for JOIN tests
_MYSQL_JOIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS users "
    "(id INT PRIMARY KEY, name VARCHAR(32), active TINYINT(1))",
    "DELETE FROM users",
    "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
    "CREATE TABLE IF NOT EXISTS orders "
    "(id INT, user_id INT, amount DOUBLE, status VARCHAR(16))",
    "DELETE FROM orders",
    "INSERT INTO orders VALUES (1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
]

# Standard 5-row PG push_t table
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

# PG users + orders for JOIN tests
_PG_JOIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS users "
    "(id INT PRIMARY KEY, name TEXT, active INT)",
    "DELETE FROM users",
    "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
    "CREATE TABLE IF NOT EXISTS orders "
    "(id INT, user_id INT, amount FLOAT8, status TEXT)",
    "DELETE FROM orders",
    "INSERT INTO orders VALUES (1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
]

# PG two tables for FULL OUTER JOIN (t1.id / t2.fk = 1,2,3 vs 1,2,4 → 4 result rows)
_PG_FOJ_SQLS = [
    "CREATE TABLE IF NOT EXISTS t1 (id INT, name TEXT)",
    "DELETE FROM t1",
    "INSERT INTO t1 VALUES (1,'alice'),(2,'bob'),(3,'charlie')",
    "CREATE TABLE IF NOT EXISTS t2 (fk INT, value TEXT)",
    "DELETE FROM t2",
    "INSERT INTO t2 VALUES (1,'x'),(2,'y'),(4,'z')",
]

# Standard 5-row SQL Server push_t table (T-SQL type mapping used across the
# FQ suites: INT / FLOAT / NVARCHAR / BIT)
_SQLSERVER_PUSH_T_SQLS = [
    "IF OBJECT_ID('dbo.push_t') IS NOT NULL DROP TABLE dbo.push_t",
    "CREATE TABLE dbo.push_t "
    "(val INT, score FLOAT, name NVARCHAR(32), flag BIT, status NVARCHAR(16))",
    "INSERT INTO dbo.push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]


def _sqlserver_hold_table_lock(database, seconds):
    """Block remote readers of ``dbo.push_t`` for *seconds*.

    SQL Server has no SELECT-embedded SLEEP(), so the row-locking waits used by
    the MySQL paths cannot be translated literally.  A background sqlcmd
    session opens a transaction, takes an exclusive table lock with
    ``WITH (TABLOCKX)`` and holds it via ``WAITFOR DELAY`` before committing;
    a concurrent plain SELECT (no locking hint) blocks for the whole window.
    This is used to keep the single ext-connector pool slot busy (test 003)
    and to make a remote scan deterministically slow.
    """
    ExtSrcEnv.sqlserver_exec(database, [
        "BEGIN TRANSACTION; "
        "SELECT COUNT(*) FROM dbo.push_t WITH (TABLOCKX); "
        # WAITFOR takes HH:MM:SS; the seconds field must stay below 60, so
        # anything from a minute upwards has to carry into the minutes field.
        f"WAITFOR DELAY '{int(seconds) // 3600:02d}:{(int(seconds) % 3600) // 60:02d}:{int(seconds) % 60:02d}'; "
        "COMMIT TRANSACTION",
    ])


def _wait_dnode_var(param, value, timeout=30.0):
    """Poll SHOW DNODE 1 VARIABLES until the dnode reports ``value``.

    ALTER ALL DNODES is acknowledged by the mnode before the dnode has
    applied the change: it is carried by a transaction redo action and lands
    on the dnode a few hundred milliseconds after the client ack (measured
    on Windows: mnode appended the redo action at 51.990, the dnode applied
    the value at 52.592).  A fixed sleep therefore races the propagation.
    When the sleep loses, the source's pool is created from the previous
    module config and the pool-cap dimensions below test the old size
    instead of the configured one.  ins_dnode_variables is served by the
    dnode itself, so a matching row proves the new value is live in the
    dnode's config and in the ext-connector module config derived from it.
    """
    pattern = param if len(param) <= 30 else param[:28] + '%'
    deadline = time.monotonic() + timeout
    while True:
        tdSql.query(f"SHOW DNODE 1 VARIABLES LIKE '{pattern}'", queryTimes=1)
        tdSql.checkRows(1)
        got = str(tdSql.getData(0, 2))
        if got == str(value):
            return
        if time.monotonic() >= deadline:
            tdLog.exit(f"dnode 1 did not apply {param}={value} within "
                       f"{timeout:.0f}s (still {got})")
        time.sleep(0.2)


def _alter_max_pool_size(value):
    """Set federatedQueryMaxPoolSizePerSource and wait for the dnode to apply it."""
    tdSql.execute(f"ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '{value}'")
    _wait_dnode_var("federatedQueryMaxPoolSizePerSource", value)


# ---------------------------------------------------------------------------
# Oracle test data — the same 5 rows as the MySQL/SQL Server push_t fixture,
# with the Oracle type mapping.  19c has no BOOLEAN, so the bit-like column is
# NUMBER(1); rows are inserted one statement at a time because a multi-row
# VALUES list is not accepted before 23ai.
# ---------------------------------------------------------------------------
_ORACLE_PUSH_T_SQLS = [
    "CREATE TABLE PUSH_T "
    "(VAL NUMBER(10,0), SCORE BINARY_DOUBLE, NAME VARCHAR2(32), "
    "FLAG NUMBER(1), STATUS VARCHAR2(16))",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (1,1.5,'alpha',1,'active')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (2,2.5,'beta',0,'idle')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (3,3.5,'gamma',1,'active')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (4,4.5,'delta',0,'idle')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (5,5.5,'epsilon',1,'active')",
    "COMMIT",
]

# Dedicated account for FQOR-ERR-002 (account lock / password expiry).  It is
# created by the instance's SYSTEM account and has no privileges beyond
# CREATE SESSION and SELECT on the fixture table.
_FQOR_LOCK_USER = "FQOR_LOCK_U"
_FQOR_LOCK_PASS = "FqorLock1"
_FQOR_LOCK_PASS2 = "FqorLock2"


def _oracle_sleep_fixtures(cfg, table="PUSH_T"):
    """Make one Oracle base scan take a known number of seconds.

    Oracle has no SELECT-embedded SLEEP(): DBMS_SESSION.SLEEP is a
    definer-rights call a plain test schema may not execute, so SYSTEM owns a
    one-line wrapper and grants EXECUTE to PUBLIC.  The wrapper is exposed
    through a view, the same shape as the MySQL SLEEP() view and the SQL Server
    TABLOCKX window.

    FQ06_SLOW_V sleeps 2 s (long enough to exceed a 1 s read timeout);
    FQ06_SLOW10_V sleeps 10 s, which outlasts the pool-acquire wait so the
    pool-exhaustion case deterministically sees EXT_RESOURCE_EXHAUSTED instead
    of queueing for the single slot.  The cast to NUMBER(10,0) is required: a
    bare NUMBER return has no precision and the column would be rejected as
    unmappable.
    """
    ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
        *ExtSrcEnv.oracle_sleep_function_sql("FQ06_SLEEP", 2),
        *ExtSrcEnv.oracle_sleep_function_sql("FQ06_SLEEP10", 10),
    ])
    ExtSrcEnv._oracle_sql(cfg, [
        f"CREATE OR REPLACE VIEW FQ06_SLOW_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_SLEEP AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_SLOW10_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_SLEEP10 AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
    ])


def _oracle_drop_sleep_fixtures(cfg):
    """Drop the sleep views (idempotent; the helpers may not exist)."""
    for view in ("FQ06_SLOW_V", "FQ06_SLOW10_V"):
        try:
            ExtSrcEnv._oracle_sql(cfg, f"DROP VIEW {view}")
        except Exception:
            pass


def _oracle_error_fixtures(cfg, table="PUSH_T"):
    """Install the ORA-error injection helpers and the views that raise them.

    Oracle raises a compile-time error for a broken statement inside a view,
    so a plain view cannot carry an invalid query.  The indirection used here
    is a SYSTEM-owned AUTHID CURRENT_USER function that opens a cursor over a
    SQL string passed at run time: the string is only parsed when the function
    executes, at scan time, and the raised ORA error reaches the connector like
    any other remote error.  AUTHID CURRENT_USER means the privileges are the
    caller's, which is what makes ORA-01031 (no ALTER SYSTEM privilege)
    reproducible for the test schema.

    Views installed (all over *table*, one row):
      * FQ06_E904_V — ORA-00904 invalid identifier (vague demotion class)
      * FQ06_E937_V — ORA-00937 not a single-group group function (precise)
      * FQ06_E979_V — ORA-00979 not a GROUP BY expression (precise)
      * FQ06_E900_V — ORA-00900 invalid SQL statement (vague)
      * FQ06_DENY_V — ORA-01031 insufficient privileges (access denied)
      * FQ06_DIV0_V — ORA-01476 divisor is equal to zero (conversion failure)
      * FQ06_1722_V — ORA-01722 invalid number (conversion failure)
      * FQ06_1858_V — ORA-01858 non-numeric character (conversion failure)
      * FQ06_E1426_V — ORA-01426 numeric overflow (default mapping)
    """
    ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
        "CREATE OR REPLACE FUNCTION SYSTEM.FQ06_RAISE(p_sql VARCHAR2) "
        "RETURN NUMBER AUTHID CURRENT_USER IS\n"
        "  c SYS_REFCURSOR;\n"
        "  x NUMBER;\n"
        "BEGIN\n"
        "  OPEN c FOR p_sql;\n"
        "  FETCH c INTO x;\n"
        "  CLOSE c;\n"
        "  RETURN NVL(x, 0);\n"
        "END;",
        "GRANT EXECUTE ON SYSTEM.FQ06_RAISE TO PUBLIC",
        "CREATE OR REPLACE FUNCTION SYSTEM.FQ06_BOOM "
        "RETURN NUMBER AUTHID CURRENT_USER IS\n"
        "BEGIN\n"
        "  EXECUTE IMMEDIATE 'SELEKT 1 FROM DUAL';\n"
        "  RETURN 1;\n"
        "END;",
        "GRANT EXECUTE ON SYSTEM.FQ06_BOOM TO PUBLIC",
        "CREATE OR REPLACE FUNCTION SYSTEM.FQ06_DENY_FN "
        "RETURN NUMBER AUTHID CURRENT_USER IS\n"
        "BEGIN\n"
        "  EXECUTE IMMEDIATE 'ALTER SYSTEM FLUSH SHARED_POOL';\n"
        "  RETURN 1;\n"
        "END;",
        "GRANT EXECUTE ON SYSTEM.FQ06_DENY_FN TO PUBLIC",
    ])
    ExtSrcEnv._oracle_sql(cfg, [
        f"CREATE OR REPLACE VIEW FQ06_E904_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_RAISE('SELECT NO_SUCH_COL_ZZ FROM DUAL') "
        f"AS NUMBER(10,0)) AS BOOM FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_E937_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_RAISE('SELECT COUNT(*), DUMMY FROM DUAL') "
        f"AS NUMBER(10,0)) AS BOOM FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_E979_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_RAISE('SELECT DUMMY, COUNT(*) FROM DUAL "
        f"GROUP BY SYSDATE') AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_E900_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_BOOM AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_DENY_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ06_DENY_FN AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ06_DIV0_V AS SELECT VAL, "
        f"CAST(VAL / 0 AS NUMBER(10,0)) AS BOOM FROM {table}",
        f"CREATE OR REPLACE VIEW FQ06_1722_V AS SELECT VAL, "
        f"CAST(DUMMY + 1 AS NUMBER(10,0)) AS BOOM FROM {table}, DUAL",
        f"CREATE OR REPLACE VIEW FQ06_1858_V AS SELECT VAL, "
        f"TO_DATE('2024-' || NAME || '-01','YYYY-MM-DD') AS BOOM "
        f"FROM {table}",
        f"CREATE OR REPLACE VIEW FQ06_E1426_V AS SELECT VAL, "
        f"CAST(POWER(10, VAL * 400) AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
    ])


def _oracle_drop_error_fixtures(cfg):
    """Drop the error-injection views (idempotent)."""
    for view in ("FQ06_E904_V", "FQ06_E937_V", "FQ06_E979_V", "FQ06_E900_V",
                 "FQ06_DENY_V", "FQ06_DIV0_V", "FQ06_1722_V", "FQ06_1858_V",
                 "FQ06_E1426_V"):
        try:
            ExtSrcEnv._oracle_sql(cfg, f"DROP VIEW {view}")
        except Exception:
            pass


def _plan_text(sql):
    """Return the full ``EXPLAIN VERBOSE TRUE`` output for *sql* as one string."""
    tdSql.query(f"explain verbose true {sql}")
    return "\n".join(
        str(col) for row in tdSql.queryResult for col in row if col is not None
    )


def _fresh_execute(sql):
    """Run *sql* on a brand-new connection, i.e. in a fresh session.

    Demotion state is attached to the session, so the cases need a second
    connection that does not share the planner state of ``tdSql``.  Returns
    ``(rows, None)`` on success and ``(None, exception)`` on failure.
    """
    import taos  # noqa: PLC0415  (local import: the client may be absent)

    try:
        conn = taos.connect()
    except Exception as exc:
        return None, exc
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall(), None
        except Exception as exc:
            return None, exc
        finally:
            cur.close()
    finally:
        conn.close()


def _oracle_drop_user(cfg, user):
    """DROP USER ... CASCADE, tolerating an account that never existed."""
    try:
        ExtSrcEnv.oracle_admin_exec_cfg(cfg, [f"DROP USER {user} CASCADE"])
    except Exception:
        pass


# InfluxDB line-protocol data for push tests
_INFLUX_BUCKET_S04 = "fq_push_s04_i"   # s04 uses its own bucket to avoid race with fq_push_029 drop
_INFLUX_LINES_CPU = [
    f"cpu,host=a usage_idle=80.0 {_BASE_TS}000000",       # ns-precision
    f"cpu,host=a usage_idle=75.0 {_BASE_TS + 60000}000000",
    f"cpu,host=b usage_idle=90.0 {_BASE_TS}000000",
    f"cpu,host=b usage_idle=85.0 {_BASE_TS + 60000}000000",
]

# InfluxDB push_t equivalent (5 rows: status as TAG, val/score/flag/name as fields)
# Matches MySQL/PG push_t schema: val=1..5, score=1.5..5.5, flag=1/0, status=active/idle
_INFLUX_PUSH_T_LINES = [
    f'push_t,status=active val=1i,score=1.5,flag=1i,name="alpha" {_BASE_TS}000000',
    f'push_t,status=idle val=2i,score=2.5,flag=0i,name="beta" {_BASE_TS + 60000}000000',
    f'push_t,status=active val=3i,score=3.5,flag=1i,name="gamma" {_BASE_TS + 120000}000000',
    f'push_t,status=idle val=4i,score=4.5,flag=0i,name="delta" {_BASE_TS + 180000}000000',
    f'push_t,status=active val=5i,score=5.5,flag=1i,name="epsilon" {_BASE_TS + 240000}000000',
]


class TestFq06RetryPolicy(FederatedQueryVersionedMixin):
    """FQ-06-001 through FQ-06-004: error classification & retry policy."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        pass

    # ------------------------------------------------------------------
    # FQ-06-001 ~ FQ-06-004: Error retry and diagnostics
    # ------------------------------------------------------------------

    def test_fq_06_001(self):
        """FQ-06-001: Connection error retry — Scheduler retries per retryable semantics

        Dimensions:
          a) Stop external service → EXT_SOURCE_UNAVAILABLE (connection-error class,
             retryable per DS §5.3.10.3.5; scheduler exhausts retries and surfaces error)
          b) Error is NOT a syntax error (parser accepted the SQL)
          c) Source persists in catalog after failed query (not removed by scheduler)
          d) Recovery: restart service → subsequent query succeeds (source returns available)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-06-30 wpan Add dimension d: recovery verification after service restart

        """
        # Real MySQL: create source, verify works, STOP instance → connection error,
        # catalog persistence verified, then RESTART + verify recovery.
        src = "fq_06_001"
        ext_db = "fq_06_001_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify works before stop
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → connection error (retryable)
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Dimension c) Source still in catalog after failed query
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
                # Dimension d) Recovery: query succeeds again after service restart
                tdSql.query(f"select count(*) from {src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # --- PG path ---
        p_src = "fq_06_001_p"
        p_db = "fq_06_001_p_ext"
        pg_ver = getattr(self, "_active_pg_ver", None) or ExtSrcEnv.PG_VERSIONS[0]
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            ExtSrcEnv.stop_pg_instance(pg_ver)
            try:
                tdSql.error(f"select * from {p_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{p_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_pg_instance(pg_ver)
                # Dimension d) Recovery: query succeeds again after service restart
                tdSql.query(f"select count(*) from {p_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # --- InfluxDB path ---
        i_src = "fq_06_001_i"
        i_db = "fq_06_001_i_ext"
        influx_ver = getattr(self, "_active_influx_ver", None) or ExtSrcEnv.INFLUX_VERSIONS[0]
        self._cleanup_src(i_src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, _INFLUX_PUSH_T_LINES)
            self._mk_influx_real(i_src, database=i_db)
            tdSql.query(f"select count(*) from {i_src}.push_t")
            tdSql.checkData(0, 0, 5)
            ExtSrcEnv.stop_influx_instance(influx_ver)
            try:
                tdSql.error(f"select * from {i_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{i_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_influx_instance(influx_ver)
                # Dimension d) Recovery: query succeeds again after service restart
                tdSql.query(f"select count(*) from {i_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
        # --- SQL Server path ---
        s_src = "fq_06_001_s"
        s_db = "fq_06_001_s_ext"
        ss_ver = getattr(self, "_active_sqlserver_ver", None) or ExtSrcEnv.SQLSERVER_VERSIONS[0]
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Verify works before stop.  Same pushdown expectation as the
            # MySQL/PG/InfluxDB legs above: phase-2 SQL Server capability bits
            # are enabled, so COUNT must reach the remote for this dialect too.
            self._verify_pushdown_explain(
                f"select count(*) from {s_src}.push_t", "COUNT")
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension a/b) Stop instance → connection error, retry class.
            # SQL Server reports 08001/08S01 on a stopped instance →
            # EXT_CONNECT_FAILED.  The test asserts an error (and the same
            # helper path) rather than pinning the dialect-specific errno.
            ExtSrcEnv.stop_sqlserver_instance(ss_ver)
            try:
                tdSql.error(f"select * from {s_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Dimension c) Source still in catalog after failed query
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{s_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ss_ver)
                # Dimension d) Recovery: query succeeds again after service restart
                tdSql.query(f"select count(*) from {s_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass
        # --- Oracle path ---
        o_src = "fq_06_001_o"
        o_cfg = self._oracle_cfg()
        o_ver = o_cfg.version
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            # connect_timeout_ms=500 keeps the unreachable-instance window short;
            # the value is a per-source OPTIONS entry only Oracle exposes.
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 extra_options="'connect_timeout_ms'='500'")
            # Same pushdown expectation as the legs above.
            self._verify_pushdown_explain(
                f"select count(*) from {o_src}.push_t", "COUNT")
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension a/b) Stop the instance → the listener is gone, so the
            # source fails with the connection-error code (a stopped instance
            # means a refused connection, so the class is EXT_CONNECT_FAILED).
            # Wait for the TCP port to close before querying so the failure is
            # not a race against the shutdown.
            ExtSrcEnv.stop_oracle_instance(o_ver)
            try:
                deadline = time.time() + 60
                while time.time() < deadline and ExtSrcEnv._tcp_probe(
                        o_cfg.host, o_cfg.port):
                    time.sleep(0.5)
                tdSql.error(f"select * from {o_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
                # Dimension c) Source still in catalog after failed query
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{o_src}'")
                tdSql.checkRows(1)
            finally:
                o_started = ExtSrcEnv.start_oracle_instance(o_ver)
            if not o_started:
                raise AssertionError(
                    f"Oracle {o_ver} did not become ready after restart")
            # Dimension d) Recovery: query succeeds again after service restart
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_002(self):
        """FQ-06-002: Auth error fast-fail — wrong credentials produce EXT_AUTH_FAILED

        DS §5.3.10.3.5: authentication failures are non-retryable.  The scheduler
        surfaces EXT_AUTH_FAILED immediately without retrying, and the source
        remains reachable (service is up) — only the credentials are wrong.

        Dimensions:
          a) Wrong password → EXT_AUTH_FAILED (not EXT_CONNECT_FAILED / not retried)
          b) Service is up; failure is purely credential-based (not network fault)
          c) Source persists in catalog after auth failure (scheduler does not remove it)
          d) Recovery: DROP wrong-credential source, recreate with correct password,
             subsequent query succeeds

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation (was stop-instance — incorrect)
            - 2026-06-30 wpan Complete rewrite: inject real auth failure via wrong
                               password; verify EXT_AUTH_FAILED + recovery

        """
        # --- MySQL path ---
        src = "fq_06_002"
        ext_db = "fq_06_002_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            # Dimension a/b) Create source with WRONG password → auth error, no retry
            # Service is running; only the credential is wrong.
            # MySQL: wrong password → errno 1045 → extConnectorMySQL maps to
            # EXT_AUTH_FAILED, which propagates through ctgAsync → catalog cache
            # → translateExternalTableImpl → client.
            self._mk_mysql_real(src, database=ext_db,
                                password="WRONG_PW_FQ06_b3k9x")
            tdSql.error(f"select * from {src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            # Dimension c) Source still in catalog despite auth failure
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Dimension d) Recovery: recreate with correct password → query succeeds
            self._mk_mysql_real(src, database=ext_db)  # correct password
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # --- PG path ---
        p_src = "fq_06_002_p"
        p_db = "fq_06_002_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            # Build raw DDL with wrong password (_mk_pg_real has no password override)
            cfg = self._pg_cfg()
            tdSql.execute(f"drop external source if exists {p_src}")
            tdSql.execute(
                f"create external source {p_src} "
                f"type='postgresql' host='{cfg.host}' port={cfg.port} "
                f"user='totally_nonexistent_role_fq06_y9c8x' password='irrelevant' "
                f"database={p_db} schema=public")
            # Dimension a/b) PG auth failure → EXT_AUTH_FAILED
            # pg_hba.conf for this test instance uses trust mode, so password
            # checks are bypassed.  Instead we use a completely non-existent role
            # name: PG rejects unknown roles even in trust mode, returning:
            #   FATAL: role "<name>" does not exist   (SQLSTATE 28000)
            # extConnectorPG maps any isFatal-and-not-SSL error to EXT_AUTH_FAILED.
            tdSql.error(f"select * from {p_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            # Dimension c)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'")
            tdSql.checkRows(1)
            # Dimension d) Recovery
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # --- InfluxDB path ---
        i_src = "fq_06_002_i"
        i_db = "fq_06_002_i_ext"
        influx_ver = getattr(self, "_active_influx_ver", None) or ExtSrcEnv.INFLUX_VERSIONS[0]
        self._cleanup_src(i_src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, _INFLUX_PUSH_T_LINES)
            self._mk_influx_real(
                i_src,
                database=i_db,
                api_token="TOTALLY_WRONG_TOKEN_FQ06_xr2p7",
            )
            # Dimension a/b) Wrong token → EXT_AUTH_FAILED
            # Generic Influx auth-consumption check: if api_token handling breaks,
            # the query path should not authenticate successfully.
            tdSql.error(f"select * from {i_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            # Dimension c)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{i_src}'")
            tdSql.checkRows(1)
            # Dimension d) Recovery: recreate with correct token → query succeeds
            self._mk_influx_real(i_src, database=i_db)
            tdSql.query(f"select count(*) from {i_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
        # --- SQL Server path ---
        s_src = "fq_06_002_s"
        s_db = "fq_06_002_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            # Dimension a/b) Wrong password → native error 18456 (login failed),
            # service up, only the credential wrong → EXT_AUTH_FAILED
            # (extConnectorSQLServer maps SQLSTATE 28* / 18456).
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo",
                                    password="WRONG_PW_FQ06_b3k9x")
            tdSql.error(f"select * from {s_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            # Dimension c) Source still in catalog despite auth failure
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'")
            tdSql.checkRows(1)
            # Dimension d) Recovery: recreate with correct password → query succeeds
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass
        # --- Oracle path ---
        o_src = "fq_06_002_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            # Dimension a/b) Wrong password → ORA-01017 → EXT_AUTH_FAILED.
            # The login error is raised by the driver before any statement runs,
            # so only the TSDB code reaches the client; the ORA text stays in
            # the taosd log (the client message is "authentication failed").
            t0 = time.time()
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 password="WRONG_PW_FQ06_b3k9x")
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            elapsed = time.time() - t0
            # Auth failures are non-retryable: a retry chain would multiply the
            # per-attempt cost, so a single digit-second bound proves fast-fail.
            assert elapsed < 15, (
                f"[FQ-06-002-O] wrong password was not fast-failed: "
                f"{elapsed:.1f}s")
            tdLog.info(f"[FQ-06-002-O] wrong password → EXT_AUTH_FAILED "
                       f"after {elapsed:.1f}s")
            # Dimension c) Source still in catalog despite auth failure
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            # Dimension d) Recovery: recreate with correct password → query succeeds
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_003(self):
        """FQ-06-003: Resource exhaustion — EXT_RESOURCE_EXHAUSTED (real verification)

        DS §5.3.10.3.5: resource-limit errors trigger backoff/retry.

        Production trigger mechanism (EXT_RESOURCE_EXHAUSTED):
          extConnector.c: live = idleCount + inUseCount >= maxPoolSize → error.
          maxPoolSize = gExtConnCfg.max_pool_size_per_source (global server cfg,
          default 64). Via ALTER ALL DNODES SET 'federatedQueryMaxPoolSizePerSource'='1',
          the pool size is reduced dynamically; extConnectorUpdateModuleCfg()
          propagates the update so newly-created pools pick up the new value.

        Test strategy (MySQL path, dimension a–c):
          1. SET federatedQueryMaxPoolSizePerSource=1 via ALTER ALL DNODES SET.
             extConnectorUpdateModuleCfg() propagates this to gExtConnCfg.
          2. CREATE EXTERNAL SOURCE → pool created lazily on first query
             with maxPoolSize=1.
          3. Thread A runs a slow MySQL query (VIEW using SLEEP(2)) holding the
             single connection slot for ~2 seconds.
          4. Main thread queries push_t → RESOURCE_EXHAUSTED (pool full).
          5. Verify source remains in catalog after the error.
          6. After Thread A completes, pool slot freed; next query succeeds.
          7. Restore federatedQueryMaxPoolSizePerSource=64.

        Dimensions:
          a) EXT_RESOURCE_EXHAUSTED returned when maxPoolSize=1 and slot is in use
          b) Source persists in catalog after resource exhaustion error
          c) Recovery: query succeeds after pool slot is freed
          d) PG and InfluxDB source creation + basic query (non-exhausted pool path)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-06-30 wpan Rewrite: max_pool_size is global-only; sanity-check
                               for all engines; add TODO for per-source pool config
            - 2026-07-01 wpan Full exhaustion test: extConnectorUpdateModuleCfg()
                               propagates ALTER DNODE SET to pool creation; threading
                               with MySQL SLEEP() VIEW holds slot for RESOURCE_EXHAUSTED.

        """
        import threading
        import time

        # ----------------------------------------------------------------
        # MySQL path — real RESOURCE_EXHAUSTED verification (dimensions a–c)
        # ----------------------------------------------------------------
        src = "fq_06_003"
        ext_db = "fq_06_003_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            # slow_v: SELECT with SLEEP(10) holds the ext-connector pool slot for ~10s.
            # Using SLEEP(10) (not SLEEP(2)) because EXT_RESOURCE_EXHAUSTED is retryable:
            # the client retries ~3 times at ~1s intervals.  SLEEP(10) ensures all retries
            # see an exhausted pool before the slot is finally freed.
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP VIEW IF EXISTS slow_v",
                "CREATE VIEW slow_v AS "
                "SELECT val, SLEEP(10) AS delay FROM push_t LIMIT 1",
            ])

            # Set pool size = 1 dynamically; extConnectorUpdateModuleCfg() ensures
            # the next pool created by extConnectorOpen picks up maxPoolSize=1.
            _alter_max_pool_size("1")

            # Create source AFTER the ALTER so the first extConnectorOpen call
            # creates a new pool with maxPoolSize=1.
            self._mk_mysql_real(src, database=ext_db)

            # Dimension d-subset) Normal query works (pool created with maxPoolSize=1)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            # --- Exhaust the single pool slot via a background thread ---
            # Thread A: query slow_v → connection in-use for ~2s via SLEEP(2).
            bg_errors: list = []

            def _run_slow():
                import taos  # noqa: PLC0415  (local import for thread safety)
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                f"select val, delay from {src}.slow_v limit 1"
                            )
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            t = threading.Thread(target=_run_slow, daemon=True)
            t.start()
            # Give thread A time to acquire the pool slot and start MySQL SLEEP(2).
            time.sleep(1.0)

            # Dimension a) Second query hits pool cap → RESOURCE_EXHAUSTED
            # (maxPoolSize=1, one slot in-use by thread A's SLEEP query)
            tdSql.error(
                f"select count(*) from {src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )

            # Dimension b) Source still in catalog after the resource error
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)

            # Wait for background thread (SLEEP(10) completes; total ~10s from t.start())
            t.join(timeout=30)
            if t.is_alive():
                tdLog.warning("Background slow-query thread did not finish in time")
            if bg_errors:
                tdLog.warning(
                    f"Background slow-query thread had error: {bg_errors[0]}"
                )

            # Dimension c) Recovery: slot freed → next query succeeds
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            # Restore original pool size regardless of test outcome
            try:
                _alter_max_pool_size("64")
            except Exception:
                pass

        # ----------------------------------------------------------------
        # PG path — sanity-check (pool size now restored to 8)
        # ----------------------------------------------------------------
        p_src = "fq_06_003_p"
        p_db = "fq_06_003_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'"
            )
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # InfluxDB path — sanity-check
        # ----------------------------------------------------------------
        i_src = "fq_06_003_i"
        i_db = "fq_06_003_i_ext"
        self._cleanup_src(i_src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, _INFLUX_PUSH_T_LINES)
            self._mk_influx_real(i_src, database=i_db)
            tdSql.query(f"select count(*) from {i_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{i_src}'"
            )
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # SQL Server path — real RESOURCE_EXHAUSTED verification
        #
        # SQL Server has no SELECT-embedded SLEEP(): the single pool slot is
        # held by a federated base scan that blocks on an exclusive table lock
        # (TABLOCKX) held by a background sqlcmd session.  The 12s lock window
        # outlasts the client's retry loop for the retryable
        # EXT_RESOURCE_EXHAUSTED error, as with the MySQL SLEEP(10) above.
        # ----------------------------------------------------------------
        s_src = "fq_06_003_s"
        s_db = "fq_06_003_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)

            _alter_max_pool_size("1")
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")

            # Normal query works (pool created with maxPoolSize=1)
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)

            lock_errors: list = []
            bg_errors: list = []

            def _hold_lock():
                try:
                    _sqlserver_hold_table_lock(s_db, 12)
                except Exception as exc:
                    lock_errors.append(exc)

            def _run_blocked():
                import taos  # noqa: PLC0415  (local import for thread safety)
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(f"select count(*) from {s_src}.push_t")
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            lt = threading.Thread(target=_hold_lock, daemon=True)
            lt.start()
            time.sleep(1.5)  # let the lock holder acquire TABLOCKX
            # Thread B: the blocked scan keeps the single pool slot busy.
            bt = threading.Thread(target=_run_blocked, daemon=True)
            bt.start()
            time.sleep(1.5)  # blocked scan now occupies the pool slot

            # Dimension a) Second query hits pool cap → RESOURCE_EXHAUSTED
            tdSql.error(
                f"select count(*) from {s_src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )

            # Dimension b) Source still in catalog after the resource error
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'"
            )
            tdSql.checkRows(1)

            # Wait for the lock window to elapse and the blocked scan to finish
            bt.join(timeout=60)
            lt.join(timeout=30)
            if bt.is_alive() or lt.is_alive():
                tdLog.warning("Background SQL Server threads did not finish in time")
            if lock_errors:
                tdLog.warning(f"Lock-holder thread error: {lock_errors[0]}")
            if bg_errors:
                tdLog.warning(f"Blocked-query thread error: {bg_errors[0]}")

            # Dimension c) Recovery: slot freed → next query succeeds
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)

        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass
            # Restore original pool size regardless of test outcome
            try:
                _alter_max_pool_size("64")
            except Exception:
                pass

        # ----------------------------------------------------------------
        # Oracle path — real RESOURCE_EXHAUSTED verification
        #
        # Oracle has no SELECT-embedded SLEEP(): the single pool slot is held
        # by a federated base scan over a view that calls DBMS_SESSION.SLEEP
        # through a SYSTEM-owned wrapper.  The 10 s sleep outlasts the client's
        # retry loop for the retryable EXT_RESOURCE_EXHAUSTED error, as with
        # the MySQL SLEEP(10) and the SQL Server 12 s lock above.  The pool
        # acquire itself waits about the connect timeout (5 s) before giving
        # up, so the exhausted error arrives well inside the sleep window.
        # ----------------------------------------------------------------
        o_src = "fq_06_003_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_sleep_fixtures(o_cfg)

            _alter_max_pool_size("1")
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())

            # Normal query works (pool created with maxPoolSize=1)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            bg_errors: list = []

            def _run_slow_oracle():
                import taos  # noqa: PLC0415  (local import for thread safety)
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                f"select val, boom from {o_src}.fq06_slow10_v"
                            )
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            ot = threading.Thread(target=_run_slow_oracle, daemon=True)
            ot.start()
            time.sleep(1.5)  # slow scan now occupies the single pool slot

            # Dimension a) Second query hits pool cap → RESOURCE_EXHAUSTED
            t0 = time.time()
            tdSql.error(
                f"select count(*) from {o_src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )
            elapsed = time.time() - t0
            assert elapsed < 60, (
                f"[FQ-06-003-O] pool cap not enforced: error took {elapsed:.1f}s")

            # Dimension b) Source still in catalog after the resource error
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'"
            )
            tdSql.checkRows(1)

            # Wait for the 10 s sleep to finish and the slot to be released
            ot.join(timeout=60)
            if ot.is_alive():
                tdLog.warning("Background Oracle slow-query thread did not finish")
            if bg_errors:
                tdLog.warning(
                    f"Background Oracle slow-query thread error: {bg_errors[0]}")

            # Dimension c) Recovery: slot freed → next query succeeds
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_sleep_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass
            # Restore original pool size regardless of test outcome
            try:
                _alter_max_pool_size("64")
            except Exception:
                pass

    def test_fq_06_004(self):
        """FQ-06-004: Availability state transitions — available/degraded/unavailable switching correct

        Dimensions:
          a) After CREATE: source is tracked in ins_ext_sources
          b) After successful query: source is available (query returns results)
          c) After failed query (service stopped): source remains in catalog; queries fail
          d) After DROP: source removed from catalog
          e) Recovery: restart service → query succeeds again (source available)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-06-30 wpan Add dimension e: recovery verification after service restart

        """
        src = "fq_06_004"
        ext_db = "fq_06_004_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a) Source available → in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Dimension b) Verify query works (available state)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension c) Stop instance → state transitions to degraded/unavailable
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Source still in catalog despite failed state
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
                # Dimension e) Recovery: source transitions back to available
                tdSql.query(f"select count(*) from {src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # Dimension d) After DROP: source removed from catalog
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(0)
        # --- PG path ---
        p_src = "fq_06_004_p"
        p_db = "fq_06_004_p_ext"
        pg_ver = getattr(self, "_active_pg_ver", None) or ExtSrcEnv.PG_VERSIONS[0]
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'")
            tdSql.checkRows(1)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            ExtSrcEnv.stop_pg_instance(pg_ver)
            try:
                tdSql.error(f"select * from {p_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{p_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_pg_instance(pg_ver)
                # Dimension e) Recovery
                tdSql.query(f"select count(*) from {p_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{p_src}'")
        tdSql.checkRows(0)
        # --- InfluxDB path ---
        i_src = "fq_06_004_i"
        i_db = "fq_06_004_i_ext"
        influx_ver = getattr(self, "_active_influx_ver", None) or ExtSrcEnv.INFLUX_VERSIONS[0]
        self._cleanup_src(i_src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, _INFLUX_PUSH_T_LINES)
            self._mk_influx_real(i_src, database=i_db)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{i_src}'")
            tdSql.checkRows(1)
            tdSql.query(f"select count(*) from {i_src}.push_t")
            tdSql.checkData(0, 0, 5)
            ExtSrcEnv.stop_influx_instance(influx_ver)
            try:
                tdSql.error(f"select * from {i_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{i_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_influx_instance(influx_ver)
                # Dimension e) Recovery
                tdSql.query(f"select count(*) from {i_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{i_src}'")
        tdSql.checkRows(0)
        # --- SQL Server path ---
        s_src = "fq_06_004_s"
        s_db = "fq_06_004_s_ext"
        ss_ver = getattr(self, "_active_sqlserver_ver", None) or ExtSrcEnv.SQLSERVER_VERSIONS[0]
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Dimension a) Source available → in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'")
            tdSql.checkRows(1)
            # Dimension b) Verify query works (available state); same pushdown
            # expectation as the MySQL/PG/InfluxDB legs above.
            self._verify_pushdown_explain(
                f"select count(*) from {s_src}.push_t", "COUNT")
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension c) Stop instance → state transitions to degraded/unavailable;
            # 08001/08S01 on a stopped instance → EXT_CONNECT_FAILED class
            ExtSrcEnv.stop_sqlserver_instance(ss_ver)
            try:
                tdSql.error(f"select * from {s_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Source still in catalog despite failed state
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{s_src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ss_ver)
                # Dimension e) Recovery: source transitions back to available
                tdSql.query(f"select count(*) from {s_src}.push_t")
                tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass
        # Dimension d) After DROP: source removed from catalog
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{s_src}'")
        tdSql.checkRows(0)
        # --- Oracle path ---
        o_src = "fq_06_004_o"
        o_cfg = self._oracle_cfg()
        o_ver = o_cfg.version
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 extra_options="'connect_timeout_ms'='500'")
            # Dimension a) Source available → in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            # Dimension b) Verify query works (available state); same pushdown
            # expectation as the legs above.
            self._verify_pushdown_explain(
                f"select count(*) from {o_src}.push_t", "COUNT")
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension c) Stop the instance → degraded/unavailable.  The
            # listener is gone, so the connection is refused: the documented
            # code for that class is EXT_CONNECT_FAILED.
            ExtSrcEnv.stop_oracle_instance(o_ver)
            try:
                deadline = time.time() + 60
                while time.time() < deadline and ExtSrcEnv._tcp_probe(
                        o_cfg.host, o_cfg.port):
                    time.sleep(0.5)
                tdSql.error(f"select * from {o_src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
                # Source still in catalog despite failed state
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{o_src}'")
                tdSql.checkRows(1)
            finally:
                o_started = ExtSrcEnv.start_oracle_instance(o_ver)
            if not o_started:
                raise AssertionError(
                    f"Oracle {o_ver} did not become ready after restart")
            # Dimension e) Recovery: source transitions back to available
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass
        # Dimension d) After DROP: source removed from catalog
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{o_src}'")
        tdSql.checkRows(0)

    def test_fq_06_005(self):
        """FQ-06-005: EXT_TABLE_NOT_EXIST — remote table dropped while cached, triggers REFRESH path (catalogRemoveExtSource + restartAsyncQuery), exhausts retries, returns EXT_TABLE_NOT_EXIST to user.

        DS §5.3.10 NEED_CLIENT_REFRESH_EXT_SOURCE_ERROR:
          On EXT_TABLE_NOT_EXIST the client:
            1. Removes ext source from catalog cache.
            2. Retries the full query pipeline (up to REQUEST_TOTAL_EXEC_TIMES=2
               additional attempts).
            3. Each retry re-fetches remote schema; if the table is still absent
               the error repeats until retries are exhausted.
            4. User ultimately receives TSDB_CODE_EXT_TABLE_NOT_EXIST.

        extConnectorIsRetryable(EXT_TABLE_NOT_EXIST) = false — no
        executor-level retry; the error propagates immediately to the client
        retry layer.

        Dimensions:
          a) MySQL: normal query → drop remote table → EXT_TABLE_NOT_EXIST
          b) PG:    normal query → drop remote table → EXT_TABLE_NOT_EXIST
          c) After receiving the error, source still listed in ins_ext_sources
             (REFRESH path does NOT remove the source from mnode; only cache
             is cleared)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-07-xx wpan Initial implementation

        """
        # ----------------------------------------------------------------
        # MySQL path
        # ----------------------------------------------------------------
        src = "fq_06_005_m"
        ext_db = "fq_06_005_m_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a-pre) Verify normal query works — schema gets cached
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Drop the remote table directly — TDengine cache still has the schema
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db,
                                     ["DROP TABLE IF EXISTS push_t"])
            # Dimension a) Query after remote table gone → REFRESH exhausted
            # → user sees EXT_TABLE_NOT_EXIST
            tdSql.error(f"select * from {src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # Dimension c) Source remains in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # PG path
        # ----------------------------------------------------------------
        p_src = "fq_06_005_p"
        p_db = "fq_06_005_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            # Verify normal query works
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Drop the remote table directly in PG
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db,
                                  ["DROP TABLE IF EXISTS push_t"])
            # Dimension b) EXT_TABLE_NOT_EXIST after REFRESH exhausted
            tdSql.error(f"select * from {p_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # Dimension c) Source remains
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # SQL Server path
        # ----------------------------------------------------------------
        s_src = "fq_06_005_s"
        s_db = "fq_06_005_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Verify normal query works — schema gets cached
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Drop the remote table directly — TDengine cache still has the schema
            ExtSrcEnv.sqlserver_exec(s_db, ["DROP TABLE IF EXISTS dbo.push_t"])
            # Dimension c) EXT_TABLE_NOT_EXIST after REFRESH exhausted
            # (native 208 "invalid object name" → EXT_TABLE_NOT_EXIST)
            tdSql.error(f"select * from {s_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # Source remains in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # Oracle path
        #
        # Oracle reports every "cannot see the object" condition as
        # ORA-00942 (table or view does not exist), whether the table is gone
        # or the schema lacks the privilege on it.  The connector maps 942 to
        # EXT_TABLE_NOT_EXIST without trying to tell the two apart — the TS
        # requires that ambiguity to be preserved rather than guessed.  The
        # client message is the generic "external table not found" text; the
        # ORA number is retained on the taosd side.
        # ----------------------------------------------------------------
        o_src = "fq_06_005_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Verify normal query works — schema gets cached
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Drop the remote table directly — TDengine cache still has the schema
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            # Dimension c) EXT_TABLE_NOT_EXIST after REFRESH exhausted
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            # Source remains in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            # Recovery: recreate the table → the next query re-fetches the
            # schema and succeeds (the REFRESH path only cleared the cache).
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_006(self):
        """FQ-06-006: EXT_ACCESS_DENIED — insufficient table-level privileges, non-retryable.

        DS §5.3.10 NEED_CLIENT_RETURN_EXT_SOURCE_ERROR:
          EXT_ACCESS_DENIED is returned directly to the user without retry.
          extConnectorIsRetryable(EXT_ACCESS_DENIED) = false.

        MySQL error mapping (extConnectorMySQL.c):
          errno 1142 (SELECT command denied to user) → EXT_ACCESS_DENIED
          errno 1044 (access denied for database)    → EXT_ACCESS_DENIED

        PG note: query-time permission denied (SQLSTATE 42501) is mapped to
          EXT_REMOTE_INTERNAL by pgMapError (which only inspects PQstatus).
          Therefore this test covers MySQL only for EXT_ACCESS_DENIED.
          See FQ-06-002 for PG auth-failure coverage.

        SQL Server note: only native error 916 (no permission on the database)
          maps to EXT_ACCESS_DENIED in extConnectorSQLServer; a table-level
          permission error (native 229) falls through to EXT_REMOTE_INTERNAL.
          EXT_ACCESS_DENIED is therefore not covered for SQL Server here; the
          default-mapping behaviour is covered by FQ-06-009.

        Oracle note: Oracle reports a missing object and a missing privilege on
          an existing object with the same ORA-00942, so this test cannot
          separate a table-level denial from a dropped table (FQ-06-005).  A
          genuine ORA-01031 raised while scanning is covered by FQOR-ERR-007.

        Dimensions:
          a) MySQL user with SELECT denied on specific table → EXT_ACCESS_DENIED
          b) Source persists in catalog after access denied (no source removal)
          c) Recovery: recreate source with privileged user → query succeeds

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-07-xx wpan Initial implementation

        """
        src = "fq_06_006_m"
        ext_db = "fq_06_006_m_ext"
        restricted_user = "fq06_noaccess"
        restricted_pass = "Fq06NoAcc3ss!"
        self._cleanup_src(src)
        cfg = self._mysql_cfg()
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _MYSQL_PUSH_T_SQLS)
            # Create a user with INSERT-only privilege on ext_db (no SELECT).
            # Having INSERT means MySQL allows the user to USE/connect to ext_db
            # (has some privilege on the DB) but denies SELECT on any table with
            # errno 1142 (ER_TABLEACCESS_DENIED_ERROR) → EXT_ACCESS_DENIED.
            # Using GRANT INSERT avoids the MySQL 8.0 REVOKE-from-DB-grant limitation
            # (errno 1147: "There is no such grant defined on table").
            ExtSrcEnv.mysql_exec_cfg(cfg, None, [
                f"DROP USER IF EXISTS '{restricted_user}'@'%'",
                f"CREATE USER '{restricted_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{restricted_pass}'",
                f"GRANT INSERT ON `{ext_db}`.* TO '{restricted_user}'@'%'",
                "FLUSH PRIVILEGES",
            ])
            # Dimension a) Source with restricted user → EXT_ACCESS_DENIED
            self._mk_mysql_real(src, database=ext_db,
                                user=restricted_user, password=restricted_pass)
            tdSql.error(f"select * from {src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_ACCESS_DENIED)
            # Dimension b) Source still in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Dimension c) Recovery: recreate source with privileged user
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
        finally:
            try:
                ExtSrcEnv.mysql_exec_cfg(cfg, None, [
                    f"DROP USER IF EXISTS '{restricted_user}'@'%'",
                    "FLUSH PRIVILEGES",
                ])
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    def test_fq_06_007(self):
        """FQ-06-007: EXT_SOURCE_NOT_FOUND — DROP external source, then query returns error.

        DS §5.3.10 NEED_CLIENT_RM_EXT_SOURCE_ERROR:
          On EXT_SOURCE_NOT_FOUND the client removes the source from catalog
          cache and returns the error to the user without retry.

        Observable path: DROP EXTERNAL SOURCE invalidates mnode + catalog.
          Subsequent queries get an error from the parser/mnode stage because
          the source no longer exists.  The final error seen by the user may be
          TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST (mnode path) or
          TSDB_CODE_EXT_SOURCE_NOT_FOUND (executor path in race conditions).
          Both indicate source-not-found; this test accepts either.

        Dimensions:
          a) MySQL: create + query → DROP source → query returns error
          b) PG:    same as a)
          c) After DROP, source absent from ins_ext_sources

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-07-xx wpan Initial implementation

        """
        # ----------------------------------------------------------------
        # MySQL path
        # ----------------------------------------------------------------
        src = "fq_06_007_m"
        ext_db = "fq_06_007_m_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Populate catalog cache
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # DROP the external source
            tdSql.execute(f"drop external source if exists {src}")
            # Dimension c) Catalog entry removed
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(0)
            # Dimension a) Query deleted source → error
            # Error is MND_EXTERNAL_SOURCE_NOT_EXIST from mnode (source not in catalog)
            tdSql.error(f"select * from {src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST)
        finally:
            # Source already dropped; just guard against partial cleanup
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # PG path
        # ----------------------------------------------------------------
        p_src = "fq_06_007_p"
        p_db = "fq_06_007_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.execute(f"drop external source if exists {p_src}")
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'")
            tdSql.checkRows(0)
            # Dimension b) Query deleted source → error
            tdSql.error(f"select * from {p_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # SQL Server path
        # ----------------------------------------------------------------
        s_src = "fq_06_007_s"
        s_db = "fq_06_007_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Populate catalog cache
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # DROP the external source
            tdSql.execute(f"drop external source if exists {s_src}")
            # Dimension c) Catalog entry removed
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'")
            tdSql.checkRows(0)
            # Dimension a) Query deleted source → MND_EXTERNAL_SOURCE_NOT_EXIST
            tdSql.error(f"select * from {s_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST)
        finally:
            # Source already dropped; just guard against partial cleanup
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # Oracle path
        # ----------------------------------------------------------------
        o_src = "fq_06_007_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Populate catalog cache
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # DROP the external source
            tdSql.execute(f"drop external source if exists {o_src}")
            # Dimension c) Catalog entry removed
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(0)
            # Dimension a) Query deleted source → EXT_SOURCE_NOT_FOUND
            # (0x6407; the mnode/catalog path for a source that no longer
            # exists — the "MND_EXTERNAL_SOURCE_NOT_EXIST" name used by the
            # legs above does not exist in taoserror.h).
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_NOT_FOUND)
        finally:
            # Source already dropped; just guard against partial cleanup
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_008(self):
        """FQ-06-008: EXT_SOURCE_CHANGED — ALTER EXTERNAL SOURCE triggers REFRESH path; client clears catalog cache and re-plans; query succeeds with updated metadata.

        DS §5.3.10 NEED_CLIENT_REFRESH_EXT_SOURCE_ERROR:
          EXT_SOURCE_CHANGED causes the client to:
            1. Remove the stale ext source from catalog cache.
            2. Restart the query pipeline, re-fetching metadata from mnode.
          If the source is still accessible with the new config, the re-plan
          succeeds transparently and the user sees a normal result.

        TDengine behaviour: schedulerExecCallback calls catalogRemoveExtSource
          after ALTER EXTERNAL SOURCE succeeds, invalidating the local cache.
          The next query re-fetches source metadata from mnode and succeeds.

        Dimensions:
          a) MySQL: query → ALTER source options → query again → success
          b) PG:    same as a)
          c) Source still in ins_ext_sources with updated attributes after ALTER

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-07-xx wpan Initial implementation

        """
        # ----------------------------------------------------------------
        # MySQL path
        # ----------------------------------------------------------------
        src = "fq_06_008_m"
        ext_db = "fq_06_008_m_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a-pre) Baseline query (populates cache)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # ALTER the external source to bump mnode version → cache invalidated
            # Correct syntax: ALTER EXTERNAL SOURCE name SET options(...)
            tdSql.execute(
                f"alter external source {src} "
                f"set options('connect_timeout_ms'='5000')")
            # Dimension a) Query after ALTER → REFRESH + re-plan → success
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension c) Source still in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # PG path
        # ----------------------------------------------------------------
        p_src = "fq_06_008_p"
        p_db = "fq_06_008_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.execute(
                f"alter external source {p_src} "
                f"set options('connect_timeout_ms'='5000')")
            # Dimension b) Query after ALTER → REFRESH → success
            tdSql.query(f"select count(*) from {p_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{p_src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # SQL Server path
        # ----------------------------------------------------------------
        s_src = "fq_06_008_s"
        s_db = "fq_06_008_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS)
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Baseline query (populates cache)
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # ALTER the external source to bump the mnode version → cache invalidated
            tdSql.execute(
                f"alter external source {s_src} "
                f"set options('connect_timeout_ms'='5000')")
            # Query after ALTER → REFRESH + re-plan → success
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Source still in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s_src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # Oracle path
        #
        # The ALTER carries the per-source OPTIONS pair Oracle exposes
        # (connect_timeout_ms / read_timeout_ms); the DDL bumps the mnode
        # version, which invalidates the client catalog cache, and the next
        # query re-fetches metadata and succeeds.
        # ----------------------------------------------------------------
        o_src = "fq_06_008_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Baseline query (populates cache)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # ALTER the external source to bump the mnode version → cache invalidated
            tdSql.execute(
                f"alter external source {o_src} "
                f"set options('connect_timeout_ms'='5000',"
                f"'read_timeout_ms'='10000')")
            # Query after ALTER → REFRESH + re-plan → success
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Source still in catalog, with the new OPTIONS persisted
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            tdSql.query(f"describe external source {o_src}")
            describe_text = "\n".join(
                str(col) for row in tdSql.queryResult
                for col in row if col is not None)
            assert "10000" in describe_text, (
                "[FQ-06-008-O] read_timeout_ms not persisted after ALTER:\n"
                f"{describe_text}")
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_009(self):
        """FQ-06-009: EXT_REMOTE_INTERNAL — unrecognised remote error, non-retryable.

        DS §5.3.10 NEED_CLIENT_RETURN_EXT_SOURCE_ERROR:
          EXT_REMOTE_INTERNAL is returned directly to the user without retry.
          extConnectorIsRetryable(EXT_REMOTE_INTERNAL) = false.

        MySQL connector default mapping (extConnectorMySQL.c):
          Any MySQL errno not in the explicit switch cases maps to
          TSDB_CODE_EXT_REMOTE_INTERNAL.

        Injection mechanism: a MySQL virtual generated column with expression
          `val DIV 0` causes errno 1365 (ER_DIVISION_BY_ZERO) at SELECT time.
          MySQL errno 1365 is not in the extConnectorMySQL switch → mapped to
          EXT_REMOTE_INTERNAL.

        Note: MySQL strict mode may raise the division-by-zero at INSERT time.
          In that case the generated column raises the error at SELECT when
          MySQL is in non-strict mode (ANSI/traditional mode difference).
          If the test environment runs MySQL in strict mode, this injection
          may not produce EXT_REMOTE_INTERNAL at SELECT time; the test catches
          unexpected outcomes and logs a notice for investigation.

        Dimensions:
          a) Normal push_t query succeeds (baseline)
          b) Query on error-injected table returns EXT_REMOTE_INTERNAL (or
             documents injection limitation for strict-mode MySQL environments)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-07-xx wpan Initial implementation

        """
        src = "fq_06_009_m"
        ext_db = "fq_06_009_m_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            # Injection: stored function that raises SIGNAL (errno 1644,
            # ER_SIGNAL_EXCEPTION) exposed through a VIEW.  The function
            # fires at mysql_stmt_execute time → not in the MySQL error switch
            # → EXT_REMOTE_INTERNAL.
            _INJECT_SQLS = [
                "DROP VIEW  IF EXISTS remote_err_v",
                "DROP TABLE IF EXISTS remote_err_t",
                "DROP FUNCTION IF EXISTS boom",
                ("CREATE FUNCTION boom() RETURNS INT DETERMINISTIC "
                 "BEGIN "
                 "  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected remote error'; "
                 "  RETURN 0; "
                 "END"),
                "CREATE TABLE remote_err_t (id INT, val INT)",
                "INSERT INTO remote_err_t VALUES (1, 10), (2, 20)",
                "CREATE VIEW remote_err_v AS "
                "SELECT id, val, boom() AS boom_col FROM remote_err_t",
            ]
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _INJECT_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a) Normal push_t query succeeds
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension b) Query error-injected view → mysql_stmt_execute fires
            # boom() → errno 1644 (ER_SIGNAL_EXCEPTION) → not in switch →
            # EXT_REMOTE_INTERNAL (DS §5.3.10 NEED_CLIENT_RETURN_EXT_SOURCE_ERROR)
            tdSql.error(f"select * from {src}.remote_err_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
        finally:
            try:
                ExtSrcEnv.mysql_exec_cfg(
                    self._mysql_cfg(), ext_db, [
                        "DROP VIEW  IF EXISTS remote_err_v",
                        "DROP TABLE IF EXISTS remote_err_t",
                        "DROP FUNCTION IF EXISTS boom",
                    ])
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # SQL Server path
        #
        # Injection mechanism: a view whose projection CASTs the non-numeric
        # NVARCHAR column `status` to INT.  The expression depends on a column,
        # so it cannot be constant-folded and view creation succeeds; at SELECT
        # time SQL Server raises native error 245 (conversion failed, SQLSTATE
        # 22018), which is not in the extConnectorSQLServer switch →
        # EXT_REMOTE_INTERNAL.  (CREATE VIEW is sent as its own sqlcmd batch,
        # as T-SQL requires.)
        # ----------------------------------------------------------------
        s_src = "fq_06_009_s"
        s_db = "fq_06_009_s_ext"
        self._cleanup_src(s_src)
        try:
            ExtSrcEnv.sqlserver_create_db(s_db)
            ExtSrcEnv.sqlserver_exec(s_db, _SQLSERVER_PUSH_T_SQLS + [
                "CREATE VIEW dbo.remote_err_v AS "
                "SELECT val, CAST(status AS INT) AS boom FROM dbo.push_t",
            ])
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")
            # Dimension a) Normal push_t query succeeds
            tdSql.query(f"select count(*) from {s_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension b) Query error-injected view → conversion error (245)
            # → not in the connector switch → EXT_REMOTE_INTERNAL
            tdSql.error(f"select * from {s_src}.remote_err_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
        finally:
            try:
                ExtSrcEnv.sqlserver_exec(s_db, [
                    "DROP VIEW IF EXISTS dbo.remote_err_v",
                ])
            except Exception:
                pass
            self._cleanup_src(s_src)
            try:
                ExtSrcEnv.sqlserver_drop_db(s_db)
            except Exception:
                pass

        # ----------------------------------------------------------------
        # Oracle path
        #
        # Injection mechanism: a view whose projection evaluates
        # POWER(10, VAL * 400).  The expression depends on a column, so it
        # cannot be constant-folded and view creation succeeds; at SELECT time
        # Oracle raises ORA-01426 (numeric overflow, SQLSTATE 22003), which is
        # not in the extConnectorOracle switch → EXT_REMOTE_INTERNAL.  Because
        # the error happens while scanning, the client message keeps
        # remote_code / remote_sqlstate / remote_message.
        # ----------------------------------------------------------------
        o_src = "fq_06_009_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Dimension a) Normal push_t query succeeds
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension b) Query error-injected view → ORA-01426
            # → not in the connector switch → EXT_REMOTE_INTERNAL
            tdSql.error(f"select * from {o_src}.fq06_e1426_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL,
                        expectErrInfo="remote_code=1426",
                        fullMatched=False)
            # taosd must still serve local queries (no crash)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    # ==================================================================
    # FQOR-ERR-xxx — Oracle error classification and demotion
    #
    # These cases exercise the Oracle-specific mapping documented in the
    # Oracle TS §6.8.  Two observable shapes are used:
    #
    #   * connect/auth-stage failures (wrong password, locked or expired
    #     account, wrong service name, closed port) surface only the TSDB
    #     error code to the client — the ORA number stays in the taosd log,
    #     so the assertion is the exact code plus the catalog/engine state;
    #   * scan-stage failures carry remote_code / remote_sqlstate /
    #     remote_message and the demotion tags, so those cases assert the
    #     code, the ORA number and the classification wording.
    #
    # A connection is a "session" for demotion purposes: the session overlay
    # of a vague error is dropped on the next connection, while the precise
    # (clause-level) demotion is visible to every connection of the client
    # process.  Both directions are asserted below.
    # ==================================================================

    def test_fq_06_fqor_err_001_002(self):
        """FQOR-ERR-001/002: Oracle auth failures — wrong password, locked and expired accounts.

        ORA-01017 (invalid username/password), ORA-28000 (account locked) and
        ORA-28001 (password expired) all map to EXT_AUTH_FAILED and are not
        retried.  The login error happens before any statement is sent, so the
        client sees only the TSDB code ("External source authentication
        failed"); the ORA number is preserved in the taosd log.

        Dimensions:
          a) Wrong password → EXT_AUTH_FAILED, fast-fail (no retry chain),
             source stays in the catalog
          b) Recovery with the correct password → correct results
          c) ACCOUNT LOCK → EXT_AUTH_FAILED; UNLOCK → recovery
          d) PASSWORD EXPIRE → EXT_AUTH_FAILED; password reset → recovery

        Oracle resolves credentials at login only, so a pooled connection
        would keep working across a lock.  Every credential change therefore
        recreates the source, which forces a fresh login.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err1_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        _oracle_drop_user(o_cfg, _FQOR_LOCK_USER)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)

            # --- FQOR-ERR-001: wrong password ---
            t0 = time.time()
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 password="WRONG_PW_FQOR_ERR1_x7")
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            elapsed = time.time() - t0
            assert elapsed < 15, (
                f"[FQOR-ERR-001] wrong password was not fast-failed: "
                f"{elapsed:.1f}s")
            tdLog.info(f"[FQOR-ERR-001] wrong password → EXT_AUTH_FAILED "
                       f"after {elapsed:.1f}s")
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            # Recovery with the correct password
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # --- FQOR-ERR-002: account lock / password expiry ---
            try:
                ExtSrcEnv.oracle_admin_exec_cfg(o_cfg, [
                    f'CREATE USER {_FQOR_LOCK_USER} '
                    f'IDENTIFIED BY "{_FQOR_LOCK_PASS}"',
                ])
            except Exception:
                # A previous run may have left the account behind: DROP USER
                # fails while a pooled session is still attached.  Reusing it
                # is equivalent because the first assertion below sets the
                # password and clears any lock anyway.
                pass
            ExtSrcEnv.oracle_admin_exec_cfg(o_cfg, [
                f'ALTER USER {_FQOR_LOCK_USER} IDENTIFIED BY '
                f'"{_FQOR_LOCK_PASS}" ACCOUNT UNLOCK',
                f"GRANT CREATE SESSION TO {_FQOR_LOCK_USER}",
                f"GRANT SELECT ON TAOSDATA.PUSH_T TO {_FQOR_LOCK_USER}",
            ])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 user=_FQOR_LOCK_USER,
                                 password=_FQOR_LOCK_PASS)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # ACCOUNT LOCK → ORA-28000 → EXT_AUTH_FAILED
            ExtSrcEnv.oracle_admin_exec_cfg(
                o_cfg, [f"ALTER USER {_FQOR_LOCK_USER} ACCOUNT LOCK"])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 user=_FQOR_LOCK_USER,
                                 password=_FQOR_LOCK_PASS)
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            tdLog.info("[FQOR-ERR-002] locked account → EXT_AUTH_FAILED  OK")
            # UNLOCK → recovery
            ExtSrcEnv.oracle_admin_exec_cfg(
                o_cfg, [f"ALTER USER {_FQOR_LOCK_USER} ACCOUNT UNLOCK"])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 user=_FQOR_LOCK_USER,
                                 password=_FQOR_LOCK_PASS)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # PASSWORD EXPIRE → ORA-28001 → EXT_AUTH_FAILED
            ExtSrcEnv.oracle_admin_exec_cfg(
                o_cfg, [f"ALTER USER {_FQOR_LOCK_USER} PASSWORD EXPIRE"])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 user=_FQOR_LOCK_USER,
                                 password=_FQOR_LOCK_PASS)
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_AUTH_FAILED)
            tdLog.info("[FQOR-ERR-002] expired password → EXT_AUTH_FAILED  OK")
            # Resetting the password also clears the expiry → recovery
            ExtSrcEnv.oracle_admin_exec_cfg(
                o_cfg, [f'ALTER USER {_FQOR_LOCK_USER} '
                        f'IDENTIFIED BY "{_FQOR_LOCK_PASS2}"'])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper(),
                                 user=_FQOR_LOCK_USER,
                                 password=_FQOR_LOCK_PASS2)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # taosd stayed alive throughout
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            _oracle_drop_user(o_cfg, _FQOR_LOCK_USER)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_003_004(self):
        """FQOR-ERR-003/004: Oracle service-name error vs listener failure.

        ORA-12514 (service not registered with the listener) maps to
        EXT_DB_NOT_EXIST, while a refused connection (SQLSTATE 08001 on a
        closed port) maps to EXT_CONNECT_FAILED.  The two classes must be
        distinguishable, and both must recover once a valid endpoint is used
        again.  Both errors are raised while connecting, so the client sees
        the TSDB code only.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err3_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)

            # --- FQOR-ERR-003: DATABASE names a service the listener does not
            # serve → ORA-12514 → EXT_DB_NOT_EXIST ---
            tdSql.execute(
                f"create external source {o_src} type='oracle' "
                f"host='{o_cfg.host}' port={o_cfg.port} "
                f"user='{o_cfg.user}' password='{o_cfg.password}' "
                f"database=NOSUCHSVC schema='{o_cfg.user.upper()}'")
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_DB_NOT_EXIST)
            tdLog.info("[FQOR-ERR-003] unknown service → EXT_DB_NOT_EXIST  OK")
            # Recovery: the same source pointed at the real service
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # --- FQOR-ERR-004: closed port → connection refused →
            # EXT_CONNECT_FAILED (a different class from ERR-003) ---
            self._cleanup_src(o_src)
            tdSql.execute(
                f"create external source {o_src} type='oracle' "
                f"host='127.0.0.1' port=1 "
                f"user='{o_cfg.user}' password='{o_cfg.password}' "
                f"database={o_cfg.service} schema='{o_cfg.user.upper()}' "
                f"options('connect_timeout_ms'='500')")
            t0 = time.time()
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
            elapsed = time.time() - t0
            assert elapsed < 60, (
                f"[FQOR-ERR-004] connection refusal took too long to "
                f"surface: {elapsed:.1f}s")
            tdLog.info(f"[FQOR-ERR-004] closed port → EXT_CONNECT_FAILED "
                       f"after {elapsed:.1f}s  OK")
            # Recovery with the real endpoint
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # taosd stayed alive
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_006_007(self):
        """FQOR-ERR-006/007: Oracle object-visibility vs privilege errors.

        Oracle reports a missing object and a missing privilege on an existing
        object with the same ORA-00942, so the connector maps both to
        EXT_TABLE_NOT_EXIST and keeps the raw text; it must not claim more
        than Oracle said.  A genuine privilege violation raised inside an
        executed statement is ORA-01031 and maps to EXT_ACCESS_DENIED — the two
        codes have to stay distinguishable.

        Dimensions:
          a) Cached schema, remote table dropped → EXT_TABLE_NOT_EXIST, source
             stays in the catalog; recreating the table recovers
          b) ORA-01031 at scan time → EXT_ACCESS_DENIED with remote_code=1031
          c) The two codes differ (00942 is not reported as access denied)

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err6_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Cache the schema first
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # --- FQOR-ERR-006: remote table gone → ORA-00942 ---
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            tdSql.error(f"select * from {o_src}.push_t limit 1",
                        expectedErrno=TSDB_CODE_EXT_TABLE_NOT_EXIST)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{o_src}'")
            tdSql.checkRows(1)
            # Recovery: recreate the table and the fixture views over it
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # --- FQOR-ERR-007: ORA-01031 at scan time → EXT_ACCESS_DENIED ---
            tdSql.error(f"select * from {o_src}.fq06_deny_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_ACCESS_DENIED)
            msg = tdSql.error_info
            assert "remote_code=1031" in msg, msg
            assert "ORA-01031" in msg, msg
            tdLog.info(f"[FQOR-ERR-007] ORA-01031 → EXT_ACCESS_DENIED: {msg}")
            # The codes of (a) and (b) are distinct — 00942 must not be
            # reported as an access-denied error.
            assert TSDB_CODE_EXT_ACCESS_DENIED != TSDB_CODE_EXT_TABLE_NOT_EXIST

            # Results still correct afterwards
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_008(self):
        """FQOR-ERR-008: Oracle syntax/identifier errors are demotion classes.

        ORA-00904 / ORA-00900 are in the connector's syntax switch, so they do
        not reach the client as a raw failure: the planner classifies them as
        SYNTAX_UNSUPPORTED, demotes the pushdown and reports the demotion.
        ORA-00904 is deliberately in the *vague* class (session overlay), so a
        column-rendering mistake cannot be persisted as a permanent capability
        change.  The client-visible code for a demoted statement is
        EXT_REMOTE_INTERNAL with "demoted with session overlay" and
        diag_kind=0 / reason=10; the ORA number is still in remote_code.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err8_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # ORA-00904 → vague demotion
            tdSql.error(f"select * from {o_src}.fq06_e904_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
            msg = tdSql.error_info
            assert "External pushdown demoted with session overlay" in msg, msg
            assert "remote_code=904" in msg, msg
            assert "diag_kind=0" in msg and "reason=10" in msg, msg
            tdLog.info(f"[FQOR-ERR-008] ORA-00904 demoted (vague): {msg}")

            # ORA-00900 → vague demotion as well
            tdSql.error(f"select * from {o_src}.fq06_e900_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
            msg = tdSql.error_info
            assert "External pushdown demoted with session overlay" in msg, msg
            assert "remote_code=900" in msg, msg
            assert "diag_kind=0" in msg and "reason=10" in msg, msg
            tdLog.info(f"[FQOR-ERR-008] ORA-00900 demoted (vague): {msg}")

            # taosd kept serving and the source still answers
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_010(self):
        """FQOR-ERR-010: Oracle type-conversion failures — no truncation, no silent NULL.

        ORA-01476 (divisor is zero), ORA-01722 (invalid number) and ORA-01858
        (non-numeric character) map to EXT_TYPE_CONVERT_FAILED.  The statement
        must fail — a conversion error must never be swallowed into a NULL or a
        truncated value — and the client message must keep the ORA number.
        ORA-00932 cannot be produced inside a view on
        this release (Oracle rejects the view definition itself), so 00932 is
        covered only by the connector mapping.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err10_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            for view, ora in (
                ("fq06_div0_v", "1476"),
                ("fq06_1722_v", "1722"),
                ("fq06_1858_v", "1858"),
            ):
                tdSql.error(f"select * from {o_src}.{view} limit 1",
                            expectedErrno=TSDB_CODE_EXT_TYPE_CONVERT_FAILED)
                msg = tdSql.error_info
                assert f"remote_code={ora}" in msg, (view, msg)
                assert f"ORA-0{ora}" in msg, (view, msg)
                # The failing rows must not come back as NULL: reaching
                # tdSql.error at all proves the statement failed.
                tdLog.info(f"[FQOR-ERR-010] {view} → "
                           f"EXT_TYPE_CONVERT_FAILED ({ora}): {msg}")

            # Normal rows are unaffected and taosd stayed alive
            tdSql.query(f"select count(*), sum(val) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.checkData(0, 1, 15)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_012(self):
        """FQOR-ERR-012: Oracle read timeout — HYT00 → EXT_QUERY_TIMEOUT, connection recovers.

        The per-source OPTIONS('read_timeout_ms') is the only timeout knob
        Oracle exposes beyond the global setting; the driver reports the
        cancelled statement as ORA-01013 / SQLSTATE HYT00, which the connector
        maps to EXT_QUERY_TIMEOUT.  A fast query immediately afterwards proves
        the pool connection was replaced rather than poisoned.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err12_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_sleep_fixtures(o_cfg)
            self._mk_oracle_real(
                o_src, database=o_cfg.service, schema=o_cfg.user.upper(),
                extra_options="'connect_timeout_ms'='5000',"
                              "'read_timeout_ms'='1000'")
            # The fast path still works under the 1 s timeout
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # The 2 s sleep exceeds the 1 s read timeout
            t0 = time.time()
            tdSql.error(f"select val, boom from {o_src}.fq06_slow_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_QUERY_TIMEOUT)
            elapsed = time.time() - t0
            assert elapsed < 60, (
                f"[FQOR-ERR-012] read timeout not effective: {elapsed:.1f}s")
            msg = tdSql.error_info
            assert "remote_code=1013" in msg, msg
            assert "HYT00" in msg, msg
            tdLog.info(f"[FQOR-ERR-012] read_timeout_ms=1000 → "
                       f"EXT_QUERY_TIMEOUT in {elapsed:.1f}s: {msg}")

            # Recovery: the timed-out connection is replaced
            tdSql.query(f"select count(*), sum(val) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.checkData(0, 1, 15)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_sleep_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_013(self):
        """FQOR-ERR-013: Oracle precise demotion — clause-level, catalog-scoped.

        ORA-00937 (not a single-group group function) and ORA-00979 (not a
        GROUP BY expression) are precise: they implicate one clause, so the
        planner demotes that clause (AGG) instead of the whole statement.  The
        demotion is recorded in the client catalog, which means every
        connection of the process sees the local Aggregate over a base-scan
        Remote SQL, and the results stay correct.  The failing view itself is
        permanently broken (its definition calls a helper that raises), so the
        correctness assertion uses the real table: the aggregate is computed
        locally from the raw VALUES.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err13_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            # Baseline: COUNT is pushed down in this session
            plan = _plan_text(f"select count(*) from {o_src}.push_t")
            assert "COUNT(VAL)" in plan, plan

            # ORA-00937 → precise demotion, AGG clause
            tdSql.error(f"select * from {o_src}.fq06_e937_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
            msg = tdSql.error_info
            assert "External pushdown demoted at clause level" in msg, msg
            assert "remote_code=937" in msg, msg
            assert "diag_kind=3" in msg and "reason=2" in msg, msg
            assert "clause=2" in msg, msg
            tdLog.info(f"[FQOR-ERR-013] ORA-00937 demoted (precise): {msg}")

            # Same session: the AGG clause is no longer pushed
            plan = _plan_text(f"select count(*) from {o_src}.push_t")
            assert "Aggregate" in plan, plan
            assert "COUNT(VAL)" not in plan, plan
            assert "Remote SQL: SELECT VAL" in plan, plan
            tdSql.query(f"select count(*) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select max(val) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)

            # ORA-00979 → same precise class
            tdSql.error(f"select * from {o_src}.fq06_e979_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
            msg = tdSql.error_info
            assert "External pushdown demoted at clause level" in msg, msg
            assert "remote_code=979" in msg, msg
            assert "diag_kind=3" in msg and "reason=2" in msg, msg
            tdLog.info(f"[FQOR-ERR-013] ORA-00979 demoted (precise): {msg}")

            # Catalog-scoped: a new connection inherits the demoted plan and
            # still returns correct aggregate results.
            rows, err = _fresh_execute(
                f"explain verbose true select count(*) from {o_src}.push_t")
            assert err is None, err
            fresh_plan = "\n".join(
                str(col) for row in rows for col in row if col is not None)
            assert "Aggregate" in fresh_plan, fresh_plan
            assert "COUNT(VAL)" not in fresh_plan, fresh_plan
            rows, err = _fresh_execute(
                f"select count(*), sum(val) from {o_src}.push_t")
            assert err is None, err
            assert rows[0][0] == 5 and rows[0][1] == 15, rows
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass

    def test_fq_06_fqor_err_014(self):
        """FQOR-ERR-014: Oracle vague demotion — session overlay only.

        ORA-00904 is vague: an invalid identifier can come from a rendering
        mistake anywhere in the statement, so the demotion is applied only as
        a session overlay and is never written into the persistent capability
        state.  Two consequences are asserted:

          a) the plan of an unrelated aggregate in the same session is
             unchanged (COUNT still pushed down), unlike the precise class;
          b) a new connection re-attempts the pushdown — it raises the same
             demotion error again rather than silently running a local plan.

        Results of normal queries stay correct in both sessions.

        Catalog: - Query:FederatedPushdown

        Since: v3.5.0.0

        Labels: common,ci

        """
        o_src = "fq_06_err14_o"
        o_cfg = self._oracle_cfg()
        self._cleanup_src(o_src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(o_cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_error_fixtures(o_cfg)
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())
            plan = _plan_text(f"select count(*) from {o_src}.push_t")
            assert "COUNT(VAL)" in plan, plan

            # ORA-00904 → vague demotion
            tdSql.error(f"select * from {o_src}.fq06_e904_v limit 1",
                        expectedErrno=TSDB_CODE_EXT_REMOTE_INTERNAL)
            msg = tdSql.error_info
            assert "External pushdown demoted with session overlay" in msg, msg
            assert "remote_code=904" in msg, msg
            assert "diag_kind=0" in msg and "reason=10" in msg, msg
            tdLog.info(f"[FQOR-ERR-014] ORA-00904 demoted (vague): {msg}")

            # (a) The same session's plan is unchanged: COUNT is still pushed.
            plan = _plan_text(f"select count(*) from {o_src}.push_t")
            assert "COUNT(VAL)" in plan, plan
            assert "Aggregate" not in plan, plan
            tdSql.query(f"select count(*), sum(val) from {o_src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdSql.checkData(0, 1, 15)

            # (b) A new connection re-attempts the pushdown: the overlay is
            # not persisted, so the same statement raises the same error.
            rows, err = _fresh_execute(
                f"select * from {o_src}.fq06_e904_v limit 1")
            assert err is not None, (
                "[FQOR-ERR-014] a new session reused the old overlay instead "
                "of re-attempting the pushdown")
            text = str(err)
            assert "demoted with session overlay" in text, text
            assert "remote_code=904" in text, text
            # And its plan still pushes COUNT down.
            rows, err = _fresh_execute(
                f"explain verbose true select count(*) from {o_src}.push_t")
            assert err is None, err
            fresh_plan = "\n".join(
                str(col) for row in rows for col in row if col is not None)
            assert "COUNT(VAL)" in fresh_plan, fresh_plan
            rows, err = _fresh_execute(f"select count(*) from {o_src}.push_t")
            assert err is None, err
            assert rows[0][0] == 5, rows
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(o_src)
            try:
                _oracle_drop_error_fixtures(o_cfg)
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, "PUSH_T")
            except Exception:
                pass
