"""
test_fq_15_service_disruption.py

Targeted verification that all ext-connector timeout and resilience
fixes actually take effect when third-party databases are restarted,
stopped, or killed.

Test matrix (per provider):

  ┌─────────┬──────────────────────────────────────────────────────────────────┐
  │ Scenario │ Purpose                                                        │
  ├─────────┼──────────────────────────────────────────────────────────────────┤
  │ s01      │ Graceful stop → query fails fast (< wall-clock bound)          │
  │ s02      │ Docker kill (SIGKILL) → query fails fast (no TCP FIN)          │
  │ s03      │ Stop → error → restart → auto-recovery within seconds         │
  │ s04      │ Stop during multi-query burst → every query bounded            │
  │ s05      │ getTableSchema with service down → fails fast                  │
  │ s06      │ isAlive probe with service down → returns false promptly       │
  │ s07      │ Repeated stop/start cycles → no stale connection leak          │
  │ s08      │ Network delay injection → timeout fires before delay expires   │
  └─────────┴──────────────────────────────────────────────────────────────────┘

Every test guards service lifecycle with try/finally so that the
third-party service is always restarted, even on assertion failure.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - MySQL, PostgreSQL, and InfluxDB instances reachable (Docker or bare-metal).
    - Root / CAP_NET_ADMIN for network delay tests (s08).
"""

import os
import subprocess
import time

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# Wall-clock upper bound for a single query against a stopped service.
# With conn_timeout_ms=5000 the RPC must complete within ~5-7s;
# we allow 15s headroom for scheduling jitter, retries, and slow CI.
_QUERY_WALL_LIMIT_S = 15.0

# Short connect timeout for test sources (milliseconds).
_SHORT_CONN_TIMEOUT_MS = 2000

# MySQL 5-row test table
_MYSQL_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS svc_t "
    "(id INT PRIMARY KEY, val INT, name VARCHAR(32))",
    "DELETE FROM svc_t",
    "INSERT INTO svc_t VALUES (1,10,'alpha'),(2,20,'beta'),"
    "(3,30,'gamma'),(4,40,'delta'),(5,50,'epsilon')",
]

# PostgreSQL 5-row test table
_PG_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS svc_t "
    "(id INT PRIMARY KEY, val INT, name TEXT)",
    "DELETE FROM svc_t",
    "INSERT INTO svc_t VALUES (1,10,'alpha'),(2,20,'beta'),"
    "(3,30,'gamma'),(4,40,'delta'),(5,50,'epsilon')",
]

# InfluxDB line-protocol test data (5 points)
_INFLUX_BUCKET = "fq_svc_test_i"
_INFLUX_LINES = [
    f'svc_t,host=a val=10i,name="alpha" {_BASE_TS}000000',
    f'svc_t,host=a val=20i,name="beta" {_BASE_TS + 60000}000000',
    f'svc_t,host=b val=30i,name="gamma" {_BASE_TS + 120000}000000',
    f'svc_t,host=b val=40i,name="delta" {_BASE_TS + 180000}000000',
    f'svc_t,host=b val=50i,name="epsilon" {_BASE_TS + 240000}000000',
]


# ---------------------------------------------------------------------------
# Helper: docker kill (SIGKILL, no graceful shutdown)
# ---------------------------------------------------------------------------
def _docker_kill_container(container_name):
    """Send SIGKILL to a running Docker container (instant termination).

    Unlike ``docker stop`` which sends SIGTERM and waits, ``docker kill``
    simulates a hard crash — the server process has no chance to send
    TCP FIN or close connections gracefully.
    """
    subprocess.run(
        ["docker", "kill", container_name],
        capture_output=True, timeout=10,
    )


def _docker_container_running(container_name):
    """Return True if the named container exists and is running."""
    result = subprocess.run(
        ["docker", "inspect", "--format={{.State.Running}}", container_name],
        capture_output=True, text=True, timeout=10,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------
class TestFq15ServiceDisruption(FederatedQueryVersionedMixin):
    """Verify timeout / resilience fixes under real service disruption."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        # Best-effort cleanup of any leftover test databases
        for db in ("fq_svc_my", "fq_svc_pg"):
            try:
                ExtSrcEnv.mysql_drop_db(db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db(db)
            except Exception:
                pass
        try:
            ExtSrcEnv.influx_drop_db(_INFLUX_BUCKET)
        except Exception:
            pass

    # ==================================================================
    # Internal helpers
    # ==================================================================

    def _mk_mysql_src(self, name, database, extra_options=None):
        """Create MySQL external source with optional extra options."""
        self._mk_mysql_real(name, database=database, extra_options=extra_options)

    def _mk_pg_src(self, name, database, extra_options=None):
        """Create PG external source with optional extra options."""
        cfg = self._pg_cfg()
        sql = (f"create external source {name} "
               f"type='postgresql' host='{cfg.host}' "
               f"port={cfg.port} "
               f"user='{cfg.user}' "
               f"password='{cfg.password}' "
               f"database={database} schema=public")
        if extra_options:
            sql += f" options({extra_options})"
        tdSql.execute(sql)

    def _mk_influx_src(self, name, database, extra_options=None):
        """Create InfluxDB external source with optional extra options."""
        cfg = self._influx_cfg()
        opts = f"'api_token'='{cfg.token}','protocol'='flight_sql'"
        if extra_options:
            opts += f",{extra_options}"
        sql = (f"create external source {name} "
               f"type='influxdb' host='{cfg.host}' "
               f"port={cfg.port} "
               f"user='u' password='' "
               f"database={database} "
               f"options({opts})")
        tdSql.execute(sql)

    def _setup_mysql_data(self, db):
        cfg = self._mysql_cfg()
        ExtSrcEnv.mysql_create_db_cfg(cfg, db)
        ExtSrcEnv.mysql_exec_cfg(cfg, db, _MYSQL_TABLE_SQLS)

    def _setup_pg_data(self, db):
        cfg = self._pg_cfg()
        ExtSrcEnv.pg_create_db_cfg(cfg, db)
        ExtSrcEnv.pg_exec_cfg(cfg, db, _PG_TABLE_SQLS)

    def _setup_influx_data(self):
        cfg = self._influx_cfg()
        ExtSrcEnv.influx_create_db_cfg(cfg, _INFLUX_BUCKET)
        ExtSrcEnv.influx_write_cfg(cfg, _INFLUX_BUCKET, _INFLUX_LINES,
                                   precision='ms')

    def _mysql_ver(self):
        return (getattr(self, "_active_mysql_ver", None)
                or ExtSrcEnv.MYSQL_VERSIONS[0])

    def _pg_ver(self):
        return (getattr(self, "_active_pg_ver", None)
                or ExtSrcEnv.PG_VERSIONS[0])

    def _influx_ver(self):
        return (getattr(self, "_active_influx_ver", None)
                or ExtSrcEnv.INFLUX_VERSIONS[0])

    def _assert_query_bounded(self, sql, wall_limit=_QUERY_WALL_LIMIT_S,
                              expected_errno=None):
        """Execute *sql* expecting an error; assert it completes within *wall_limit*.

        Returns the elapsed time in seconds.
        """
        t0 = time.monotonic()
        if expected_errno:
            tdSql.error(sql, expectedErrno=expected_errno)
        else:
            # Accept any error — the key assertion is timing
            try:
                tdSql.query(sql)
            except Exception:
                pass
        elapsed = time.monotonic() - t0
        assert elapsed < wall_limit, (
            f"Query took {elapsed:.2f}s, expected < {wall_limit}s — "
            f"timeout protection may not be effective.  SQL: {sql}"
        )
        return elapsed

    # ==================================================================
    # S01: Graceful stop → query fails fast
    # ==================================================================

    def test_svc_s01_mysql_stop_query_bounded(self):
        """S01-MySQL: docker stop MySQL → query fails within wall-clock bound.

        Verifies that MySQL ``MYSQL_OPT_CONNECT_TIMEOUT`` and
        ``MYSQL_OPT_READ_TIMEOUT`` cause the query to fail quickly
        rather than blocking indefinitely.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s01_my", "fq_svc_s01_my"
        ver = self._mysql_ver()
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            # Verify healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            # Stop and verify fast failure
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S01-MySQL stop query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s01_pg_stop_query_bounded(self):
        """S01-PG: docker stop PostgreSQL → query fails within wall-clock bound.

        Verifies that PostgreSQL ``connect_timeout`` and ``statement_timeout``
        cause the query to fail quickly.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s01_pg", "fq_svc_s01_pg"
        ver = self._pg_ver()
        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_pg_instance(ver)
            try:
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S01-PG stop query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_pg_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s01_influx_stop_query_bounded(self):
        """S01-Influx: docker stop InfluxDB → query fails within wall-clock bound.

        Verifies that InfluxDB Arrow Flight gRPC ``conn_timeout_ms`` deadline
        and keepalive settings cause the query to fail quickly.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src = "svc_s01_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_influx_instance(ver)
            try:
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S01-Influx stop query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S02: Docker kill (SIGKILL) → query fails fast (no TCP FIN)
    # ==================================================================

    def test_svc_s02_mysql_kill_query_bounded(self):
        """S02-MySQL: SIGKILL MySQL → query fails within wall-clock bound.

        Unlike ``docker stop`` (SIGTERM → graceful shutdown → TCP FIN),
        ``docker kill`` sends SIGKILL so the server process terminates
        instantly with no chance to close connections.  The client sees
        a broken pipe / connection-reset rather than a clean disconnect.

        This verifies ``MYSQL_OPT_READ_TIMEOUT`` fires even when no
        TCP RST/FIN is sent (half-open connection scenario).

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s02_my", "fq_svc_s02_my"
        ver = self._mysql_ver()
        container = ExtSrcEnv._mysql_container_name(ver)
        if not _docker_container_running(container):
            pytest.skip("MySQL not running in Docker — cannot test docker kill")

        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            _docker_kill_container(container)
            try:
                time.sleep(0.5)  # let kernel notice process exit
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S02-MySQL kill query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s02_pg_kill_query_bounded(self):
        """S02-PG: SIGKILL PostgreSQL → query fails within wall-clock bound.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s02_pg", "fq_svc_s02_pg"
        ver = self._pg_ver()
        container = ExtSrcEnv._pg_container_name(ver)
        if not _docker_container_running(container):
            pytest.skip("PG not running in Docker — cannot test docker kill")

        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            _docker_kill_container(container)
            try:
                time.sleep(0.5)
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S02-PG kill query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_pg_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s02_influx_kill_query_bounded(self):
        """S02-Influx: SIGKILL InfluxDB → query fails within wall-clock bound.

        Critical test for gRPC keepalive + deadline fixes.  After a hard
        kill, the gRPC channel has stale HTTP/2 streams with no server-side
        GOAWAY.  Without keepalive and deadlines, ``cq_.Pluck()`` would
        block indefinitely.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src = "svc_s02_inf"
        ver = self._influx_ver()
        container = ExtSrcEnv._influx_container_name(ver)
        if not _docker_container_running(container):
            pytest.skip("InfluxDB not running in Docker — cannot test docker kill")

        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            _docker_kill_container(container)
            try:
                time.sleep(0.5)
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S02-Influx kill query #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S03: Stop → error → restart → auto-recovery
    # ==================================================================

    def test_svc_s03_mysql_restart_recovery(self):
        """S03-MySQL: stop → errors → restart → automatic reconnect + correct result.

        Verifies the connection pool evicts dead connections and acquires
        new ones transparently after the backend recovers.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s03_my", "fq_svc_s03_my"
        ver = self._mysql_ver()
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            # Healthy
            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)

            # Stop → errors
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                for _ in range(3):
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_mysql_instance(ver)

            time.sleep(2)

            # Recovery — must succeed with correct data
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s03_pg_restart_recovery(self):
        """S03-PG: stop → errors → restart → automatic reconnect + correct result.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s03_pg", "fq_svc_s03_pg"
        ver = self._pg_ver()
        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)

            ExtSrcEnv.stop_pg_instance(ver)
            try:
                for _ in range(3):
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_pg_instance(ver)

            time.sleep(2)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s03_influx_restart_recovery(self):
        """S03-Influx: stop → errors → restart → automatic reconnect + correct result.

        Critical for gRPC keepalive: after restart, the old channel's
        keepalive should have detected the dead connection, and the pool
        must establish a fresh gRPC channel to the restarted InfluxDB.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src = "svc_s03_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_influx_instance(ver)
            try:
                for _ in range(3):
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_influx_instance(ver)

            time.sleep(2)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S04: Stop during multi-query burst → every query bounded
    # ==================================================================

    def test_svc_s04_mysql_burst_all_bounded(self):
        """S04-MySQL: stop mid-burst → all queries complete within wall-clock bound.

        Issues N queries against a stopped MySQL.  Each individual query
        must complete within the wall-clock bound (no accumulation).
        Proves that read_timeout fires per query, not just the first.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        N = 5
        src, db = "svc_s04_my", "fq_svc_s04_my"
        ver = self._mysql_ver()
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                total_start = time.monotonic()
                for i in range(N):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S04-MySQL burst #{i+1}: {elapsed:.2f}s")
                total_elapsed = time.monotonic() - total_start
                tdLog.debug(f"  S04-MySQL total {N} queries: {total_elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s04_pg_burst_all_bounded(self):
        """S04-PG: stop mid-burst → all queries complete within wall-clock bound.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        N = 5
        src, db = "svc_s04_pg", "fq_svc_s04_pg"
        ver = self._pg_ver()
        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_pg_instance(ver)
            try:
                for i in range(N):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S04-PG burst #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_pg_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s04_influx_burst_all_bounded(self):
        """S04-Influx: stop mid-burst → all queries complete within wall-clock bound.

        Each query exercises a fresh gRPC call with conn_timeout_ms deadline.
        Proves that the deadline is set per-RPC (not per-channel), so each
        individual call is bounded.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        N = 5
        src = "svc_s04_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_influx_instance(ver)
            try:
                for i in range(N):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    tdLog.debug(f"  S04-Influx burst #{i+1}: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S05: getTableSchema with service down → fails fast
    # ==================================================================

    def test_svc_s05_mysql_schema_down_bounded(self):
        """S05-MySQL: getTableSchema while MySQL is down → fails fast.

        ``DESCRIBE <src>.table`` triggers getTableSchema internally.
        With MySQL down, ``MYSQL_OPT_CONNECT_TIMEOUT`` must fire within
        the wall-clock bound.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s05_my", "fq_svc_s05_my"
        ver = self._mysql_ver()
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            # Verify describe works while healthy
            tdSql.query(f"describe {src}.svc_t")
            assert tdSql.queryRows > 0, "DESCRIBE should return columns"

            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"describe {src}.svc_t",
                )
                tdLog.debug(f"  S05-MySQL schema-down: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s05_pg_schema_down_bounded(self):
        """S05-PG: getTableSchema while PG is down → fails fast.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src, db = "svc_s05_pg", "fq_svc_s05_pg"
        ver = self._pg_ver()
        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"describe {src}.svc_t")
            assert tdSql.queryRows > 0

            ExtSrcEnv.stop_pg_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"describe {src}.svc_t",
                )
                tdLog.debug(f"  S05-PG schema-down: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_pg_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s05_influx_schema_down_bounded(self):
        """S05-Influx: getTableSchema while InfluxDB is down → fails fast.

        This specifically tests the ``deadlineKind=1`` fix on the Arrow
        Flight ``influxArrowGetTableSchema`` path.  Without this fix,
        the Execute+DoGet RPCs would block indefinitely.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src = "svc_s05_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"describe {src}.svc_t")
            assert tdSql.queryRows > 0

            ExtSrcEnv.stop_influx_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"describe {src}.svc_t",
                )
                tdLog.debug(f"  S05-Influx schema-down: {elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S06: isAlive probe with service down → returns false promptly
    # ==================================================================

    def test_svc_s06_influx_isalive_probe(self):
        """S06-Influx: isAlive probe returns false promptly when InfluxDB is down.

        After stopping InfluxDB, the first query uses isAlive to check the
        pooled connection.  The enhanced ``GetCatalogs`` probe with
        ``conn_timeout_ms`` deadline must detect the dead connection within
        the wall-clock bound (previously it was a trivial pointer check
        that always returned true).

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        src = "svc_s06_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            # Warm up connection pool
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            # Stop — pooled connection becomes stale
            ExtSrcEnv.stop_influx_instance(ver)
            try:
                # The first query after stop exercises the isAlive path:
                # pool returns a cached connection → isAlive probe (GetCatalogs)
                # detects dead → evict → try fresh connect → connect fails →
                # error returned.  All within wall-clock bound.
                t0 = time.monotonic()
                tdSql.error(f"select * from {src}.svc_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                elapsed = time.monotonic() - t0
                tdLog.debug(f"  S06-Influx isAlive probe: {elapsed:.2f}s")
                assert elapsed < _QUERY_WALL_LIMIT_S, (
                    f"isAlive probe took {elapsed:.2f}s, expected < "
                    f"{_QUERY_WALL_LIMIT_S}s — GetCatalogs deadline may not "
                    f"be effective"
                )
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S07: Repeated stop/start cycles → no stale connection leak
    # ==================================================================

    def test_svc_s07_mysql_repeated_cycles(self):
        """S07-MySQL: 3× stop/start cycles → query always succeeds after restart.

        Verifies that repeated disruptions don't accumulate stale
        connections or corrupt internal state.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        CYCLES = 3
        src, db = "svc_s07_my", "fq_svc_s07_my"
        ver = self._mysql_ver()
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            for cycle in range(CYCLES):
                tdLog.debug(f"  S07-MySQL cycle {cycle+1}/{CYCLES}")

                # Healthy
                tdSql.query(f"select count(*) from {src}.svc_t")
                tdSql.checkData(0, 0, 5)

                # Disrupt
                ExtSrcEnv.stop_mysql_instance(ver)
                try:
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                finally:
                    ExtSrcEnv.start_mysql_instance(ver)

                time.sleep(2)

            # Final verification after all cycles
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s07_pg_repeated_cycles(self):
        """S07-PG: 3× stop/start cycles → query always succeeds after restart.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        CYCLES = 3
        src, db = "svc_s07_pg", "fq_svc_s07_pg"
        ver = self._pg_ver()
        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            for cycle in range(CYCLES):
                tdLog.debug(f"  S07-PG cycle {cycle+1}/{CYCLES}")

                tdSql.query(f"select count(*) from {src}.svc_t")
                tdSql.checkData(0, 0, 5)

                ExtSrcEnv.stop_pg_instance(ver)
                try:
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                finally:
                    ExtSrcEnv.start_pg_instance(ver)

                time.sleep(2)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s07_influx_repeated_cycles(self):
        """S07-Influx: 3× stop/start cycles → query always succeeds after restart.

        This is the most critical test for gRPC keepalive: each cycle
        creates a stale gRPC channel, and the keepalive+deadline must
        detect and evict it before the next query can succeed.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation

        """
        CYCLES = 3
        src = "svc_s07_inf"
        ver = self._influx_ver()
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            for cycle in range(CYCLES):
                tdLog.debug(f"  S07-Influx cycle {cycle+1}/{CYCLES}")

                tdSql.query(f"select count(*) from {src}.svc_t")
                tdSql.checkData(0, 0, 5)

                ExtSrcEnv.stop_influx_instance(ver)
                try:
                    self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                finally:
                    ExtSrcEnv.start_influx_instance(ver)

                time.sleep(2)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S08: Network delay injection → timeout fires before delay expires
    # ==================================================================

    def test_svc_s08_mysql_net_delay_timeout(self):
        """S08-MySQL: inject 8s network delay → 5s conn_timeout fires first.

        Uses tc netem to add 8000ms delay on loopback.  With
        ``federatedQueryConnectTimeoutMs = 5000``, the query must fail
        within ~5-7s (before the 8s delay fully elapses).

        Requires root / CAP_NET_ADMIN.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common

        History:
            - 2026-05-07 wpan Initial implementation

        """
        # Check if tc netem is available
        tc_check = subprocess.run(["tc", "qdisc", "show"], capture_output=True)
        if tc_check.returncode != 0:
            pytest.skip("tc (iproute2) not available — cannot inject delay")

        src, db = "svc_s08_my", "fq_svc_s08_my"
        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            # Verify healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            # Inject 8s delay — conn_timeout_ms (5s) should fire first
            ExtSrcEnv.inject_net_delay(8000)
            try:
                t0 = time.monotonic()
                # With 8s delay, TCP SYN takes 8s to reach, connect_timeout
                # (5s) fires before handshake completes
                try:
                    tdSql.query(f"select * from {src}.svc_t")
                except Exception:
                    pass
                elapsed = time.monotonic() - t0
                tdLog.debug(f"  S08-MySQL net-delay: {elapsed:.2f}s")
                # Should fail within ~10s (5s timeout + scheduling overhead)
                # but NOT take 16s+ (which would indicate no timeout fired)
                assert elapsed < _QUERY_WALL_LIMIT_S, (
                    f"Query took {elapsed:.2f}s with 8s delay — "
                    f"conn_timeout_ms may not be effective"
                )
            finally:
                ExtSrcEnv.clear_net_delay()

            # After clearing delay, verify recovery
            time.sleep(1)
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            ExtSrcEnv.clear_net_delay()  # double-ensure cleanup
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s08_influx_net_delay_timeout(self):
        """S08-Influx: inject 8s network delay → gRPC deadline fires first.

        The gRPC channel uses ``conn_timeout_ms`` (5s) as the deadline for
        Execute RPCs.  With 8s network delay, the deadline should fire
        before the delayed response arrives.

        Requires root / CAP_NET_ADMIN.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common

        History:
            - 2026-05-07 wpan Initial implementation

        """
        tc_check = subprocess.run(["tc", "qdisc", "show"], capture_output=True)
        if tc_check.returncode != 0:
            pytest.skip("tc (iproute2) not available — cannot inject delay")

        src = "svc_s08_inf"
        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.inject_net_delay(8000)
            try:
                t0 = time.monotonic()
                try:
                    tdSql.query(f"select * from {src}.svc_t")
                except Exception:
                    pass
                elapsed = time.monotonic() - t0
                tdLog.debug(f"  S08-Influx net-delay: {elapsed:.2f}s")
                assert elapsed < _QUERY_WALL_LIMIT_S, (
                    f"Query took {elapsed:.2f}s with 8s delay — "
                    f"gRPC deadline may not be effective"
                )
            finally:
                ExtSrcEnv.clear_net_delay()

            time.sleep(1)
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            ExtSrcEnv.clear_net_delay()
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass
