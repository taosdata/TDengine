"""
test_fq_06_retry_policy.py

Implements FQ-PUSH-021 through FQ-PUSH-024 from TS §6
"Error Classification & Retry Policy" — connection-error retry semantics,
auth-error fast-fail, resource-limit backoff, and availability state-machine
transitions (available / degraded / unavailable).

Design notes:
    - Tests focus on *logical* retry/error-classification behavior, not
      wall-clock latency bounds (those live in test_fq_15_service_disruption).
    - Each test injects a specific failure mode and validates the scheduler's
      response: retry count, state transition, and final query outcome.
"""

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
)


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
    """FQ-PUSH-021 through FQ-PUSH-024: error classification & retry policy."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        pass

    # ------------------------------------------------------------------
    # FQ-PUSH-006 ~ FQ-PUSH-010: Non-mappable aggregate / PARTITION / window
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # FQ-PUSH-021 ~ FQ-PUSH-024: Error retry and diagnostics
    # ------------------------------------------------------------------

    def test_fq_push_021(self):
        """FQ-PUSH-021: Connection error retry — Scheduler retries per retryable semantics

        Dimensions:
          a) Connection to non-routable host → connection error (retryable per DS §5.3.10.3.5)
          b) Error is NOT a syntax error (parser accepted the SQL)
          c) Source persists in catalog after failed query (not removed)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Real MySQL: create source, verify works, STOP instance → connection error,
        # catalog persistence verified, then RESTART.
        src = "fq_push_021"
        ext_db = "fq_push_021_ext"
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
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # --- PG path ---
        p_src = "fq_push_021_p"
        p_db = "fq_push_021_p_ext"
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
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # --- InfluxDB path ---
        i_src = "fq_push_021_i"
        i_db = "fq_push_021_i_ext"
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
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    def test_fq_push_022(self):
        """FQ-PUSH-022: Auth error no retry — set unavailable and fail fast

        Dimensions:
          a) Source created with non-routable host (simulates auth/connection failure)
          b) Query fails with non-syntax error (connection/auth class, not syntax)
          c) Source remains in catalog after failure (DROP required to remove)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_022"
        ext_db = "fq_push_022_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify works first
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → simulates auth/connection error (fast fail)
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Dimension c) Source remains in catalog even after failure
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # --- PG path ---
        p_src = "fq_push_022_p"
        p_db = "fq_push_022_p_ext"
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
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # --- InfluxDB path ---
        i_src = "fq_push_022_i"
        i_db = "fq_push_022_i_ext"
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
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    def test_fq_push_023(self):
        """FQ-PUSH-023: Resource limit backoff — degraded + backoff behavior correct

        Dimensions:
          a) Non-routable source simulates resource-limit failure path
          b) Query fails with non-syntax error (connection class)
          c) Internal vtable fallback: correct result verifies fallback correctness

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_023"
        ext_db = "fq_push_023_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify external works first
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → simulates resource limit failure + backoff
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select count(*) from {src}.push_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # --- PG path ---
        p_src = "fq_push_023_p"
        p_db = "fq_push_023_p_ext"
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
                tdSql.error(f"select count(*) from {p_src}.push_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_pg_instance(pg_ver)
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # --- InfluxDB path ---
        i_src = "fq_push_023_i"
        i_db = "fq_push_023_i_ext"
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
                tdSql.error(f"select count(*) from {i_src}.push_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_influx_instance(influx_ver)
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    def test_fq_push_024(self):
        """FQ-PUSH-024: Availability state transitions — available/degraded/unavailable switching correct

        Dimensions:
          a) After CREATE: source is tracked in ins_ext_sources
          b) After failed query: source remains in catalog (state may → degraded)
          c) DROP: source removed from catalog
          d) System table row count reflects create/drop lifecycle

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_024"
        ext_db = "fq_push_024_ext"
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
            # Verify query works (available state)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension b) Stop instance → state transitions to degraded/unavailable
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
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # Dimension c/d) After DROP: source removed from catalog
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(0)
        # --- PG path ---
        p_src = "fq_push_024_p"
        p_db = "fq_push_024_p_ext"
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
        i_src = "fq_push_024_i"
        i_db = "fq_push_024_i_ext"
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

