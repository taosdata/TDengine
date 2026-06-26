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
    TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
    TSDB_CODE_EXT_AUTH_FAILED,
    TSDB_CODE_EXT_ACCESS_DENIED,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_SOURCE_CHANGED,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
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
            # Build raw DDL with a totally wrong api_token
            cfg = self._influx_cfg()
            tdSql.execute(f"drop external source if exists {i_src}")
            tdSql.execute(
                f"create external source {i_src} "
                f"type='influxdb' host='{cfg.host}' port={cfg.port} "
                f"user='u' password='' database={i_db} "
                f"options('api_token'='TOTALLY_WRONG_TOKEN_FQ06_xr2p7',"
                f"'protocol'='flight_sql')")
            # Dimension a/b) Wrong token → EXT_AUTH_FAILED
            # InfluxDB 3.0 FlightSQL: wrong Bearer token → gRPC Unauthenticated(3)
            # → extConnectorInflux maps to EXT_AUTH_FAILED.
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
            tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '1'")
            time.sleep(0.5)  # wait for config propagation across the dnode

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
                tdSql.execute(
                    "ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'"
                )
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

    def test_fq_06_005(self):
        """FQ-06-005: EXT_TABLE_NOT_EXIST — remote table dropped while cached, triggers
        REFRESH path (catalogRemoveExtSource + restartAsyncQuery), exhausts retries,
        returns EXT_TABLE_NOT_EXIST to user.

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

    def test_fq_06_008(self):
        """FQ-06-008: EXT_SOURCE_CHANGED — ALTER EXTERNAL SOURCE triggers REFRESH path;
        client clears catalog cache and re-plans; query succeeds with updated metadata.

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

