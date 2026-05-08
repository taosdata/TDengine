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
  │ s02      │ SIGKILL → query fails fast (no TCP FIN)                       │
  │ s03      │ Stop → error → restart → auto-recovery within seconds         │
  │ s04      │ Stop during multi-query burst → every query bounded            │
  │ s05      │ getTableSchema with service down → fails fast                  │
  │ s06      │ isAlive probe with service down → returns false promptly       │
  │ s07      │ Repeated stop/start cycles → no stale connection leak          │
  │ s08      │ Black-hole listener → timeout fires (cross-platform)          │
  └─────────┴──────────────────────────────────────────────────────────────────┘

Every test guards service lifecycle with try/finally so that the
third-party service is always restarted, even on assertion failure.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - MySQL, PostgreSQL, and InfluxDB instances reachable.
    - Root / CAP_NET_ADMIN for network delay tests (s08).
"""

import os
import socket
import subprocess
import threading
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
# Helper: black-hole TCP listener (cross-platform timeout testing)
# ---------------------------------------------------------------------------
class _BlackHoleListener:
    """TCP listener that accepts connections but never sends data.

    Simulates a service that is reachable at the TCP layer but never
    responds at the application protocol level (no MySQL greeting, no
    PG startup response, no HTTP/2 preface).  Clients block until their
    read/connect timeout fires.

    Works on all platforms (Linux, macOS, Windows) without root privileges
    or kernel modules.  Use as a context manager::

        with _BlackHoleListener() as bh:
            # bh.port is the listening port on 127.0.0.1
            ...
    """

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen(32)
        self.port = self._sock.getsockname()[1]
        self._clients = []
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self):
        self._sock.settimeout(0.5)
        while self._running:
            try:
                client, _ = self._sock.accept()
                self._clients.append(client)  # hold open, never send
            except socket.timeout:
                continue
            except OSError:
                break

    def close(self):
        self._running = False
        for c in self._clients:
            try:
                c.close()
            except Exception:
                pass
        try:
            self._sock.close()
        except Exception:
            pass
        self._thread.join(timeout=2)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------
class TestFq15ServiceDisruption(FederatedQueryVersionedMixin):
    """Verify timeout / resilience fixes under real service disruption."""

    _timing_records: list = []  # [(label, elapsed_s), ...]

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        # ---- Timing summary ----
        # NOTE: pytest calls teardown_class with cls (the class itself) as self,
        # so self._timing_records is correct; self.__class__ would be `type`.
        records = self._timing_records
        if records:
            tdLog.info("")
            tdLog.info("=" * 65)
            tdLog.info("  Disruption → query-failure latency summary")
            tdLog.info("-" * 65)
            for label, elapsed in records:
                tdLog.info(f"  {label:<50s} {elapsed:5.2f}s")
            tdLog.info("-" * 65)
            avg = sum(e for _, e in records) / len(records)
            mx  = max(e for _, e in records)
            mn  = min(e for _, e in records)
            tdLog.info(f"  {'Count':<50s} {len(records):5d}")
            tdLog.info(f"  {'Min':<50s} {mn:5.2f}s")
            tdLog.info(f"  {'Max':<50s} {mx:5.2f}s")
            tdLog.info(f"  {'Avg':<50s} {avg:5.2f}s")
            tdLog.info("=" * 65)
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
            # expected_errno unknown — verify timing only; do NOT retry
            # (queryTimes=1 + exit=False prevents the default 10-retry loop
            # which would take ~90 s and always exceed the wall limit)
            tdSql.query(sql, queryTimes=1, exit=False)
        elapsed = time.monotonic() - t0
        assert elapsed < wall_limit, (
            f"Query took {elapsed:.2f}s, expected < {wall_limit}s — "
            f"timeout protection may not be effective.  SQL: {sql}"
        )
        return elapsed

    def _record_timing(self, label: str, elapsed: float):
        """Log a query-failure latency and record it for the end-of-run summary."""
        tdLog.info(f"  [TIMING] {label}: {elapsed:.2f}s")
        self.__class__._timing_records.append((label, elapsed))

    # S01: Graceful stop → query fails fast
    # ==================================================================

    def test_svc_s01_mysql_stop_query_bounded(self):
        """S01-MySQL: stop MySQL → query fails within wall-clock bound.

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
                    self._record_timing(f"S01-MySQL stop #{i+1}", elapsed)
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    def test_svc_s01_pg_stop_query_bounded(self):
        """S01-PG: stop PostgreSQL → query fails within wall-clock bound.

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
                    self._record_timing(f"S01-PG stop #{i+1}", elapsed)
            finally:
                ExtSrcEnv.start_pg_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass

    def test_svc_s01_influx_stop_query_bounded(self):
        """S01-Influx: stop InfluxDB → query fails within wall-clock bound.

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
                    self._record_timing(f"S01-Influx stop #{i+1}", elapsed)
            finally:
                ExtSrcEnv.start_influx_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
            except Exception:
                pass

    # ==================================================================
    # S02: SIGKILL → query fails fast (no TCP FIN)
    # ==================================================================

    def test_svc_s02_mysql_kill_query_bounded(self):
        """S02-MySQL: SIGKILL MySQL → query fails within wall-clock bound.

        SIGKILL terminates the server process instantly with no chance to
        close connections.  The client sees a broken pipe / connection-reset
        rather than a clean disconnect.

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

        self._cleanup_src(src)
        try:
            self._setup_mysql_data(db)
            self._mk_mysql_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.kill_mysql_instance(ver)
            try:
                time.sleep(0.5)  # let kernel notice process exit
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S02-MySQL kill #{i+1}", elapsed)
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

        self._cleanup_src(src)
        try:
            self._setup_pg_data(db)
            self._mk_pg_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.kill_pg_instance(ver)
            try:
                time.sleep(0.5)
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S02-PG kill #{i+1}", elapsed)
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

        self._cleanup_src(src)
        try:
            self._setup_influx_data()
            self._mk_influx_src(src, database=_INFLUX_BUCKET)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.kill_influx_instance(ver)
            try:
                time.sleep(0.5)
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S02-Influx kill #{i+1}", elapsed)
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
                for i in range(3):
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S03-MySQL stop error #{i+1}", time.monotonic() - t0)
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
                # ensure_ext_env.sh drops all test databases on bare-metal restart;
                # re-create the test database and table so the recovery query works.
                self._setup_mysql_data(db)

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
                for i in range(3):
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S03-PG stop error #{i+1}", time.monotonic() - t0)
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
                for i in range(3):
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S03-Influx stop error #{i+1}", time.monotonic() - t0)
            finally:
                ExtSrcEnv.start_influx_instance(ver)
                # ensure_ext_env.sh resets InfluxDB buckets on bare-metal restart;
                # re-create the bucket and data so the recovery query works.
                self._setup_influx_data()

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
                    self._record_timing(f"S04-MySQL burst #{i+1}", elapsed)
                total_elapsed = time.monotonic() - total_start
                tdLog.info(f"  [TIMING] S04-MySQL total {N} queries: {total_elapsed:.2f}s")
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
                    self._record_timing(f"S04-PG burst #{i+1}", elapsed)
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
                    self._record_timing(f"S04-Influx burst #{i+1}", elapsed)
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

            # Verify schema/data access works while healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t",
                )
                self._record_timing("S05-MySQL schema-down (SELECT)", elapsed)
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
                self._setup_mysql_data(db)
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

            # Verify schema/data access works while healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_pg_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t",
                )
                self._record_timing("S05-PG schema-down (SELECT)", elapsed)
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

            # Verify schema/data access works while healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_influx_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t",
                )
                self._record_timing("S05-Influx schema-down (SELECT)", elapsed)
            finally:
                ExtSrcEnv.start_influx_instance(ver)
                self._setup_influx_data()
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
                self._record_timing("S06-Influx isAlive probe", elapsed)
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
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S07-MySQL cycle-{cycle+1} error", time.monotonic() - t0)
                finally:
                    ExtSrcEnv.start_mysql_instance(ver)
                    # ensure_ext_env.sh resets MySQL databases on bare-metal restart;
                    # re-create test data so the next cycle's healthy check works.
                    self._setup_mysql_data(db)

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
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S07-PG cycle-{cycle+1} error", time.monotonic() - t0)
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
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S07-Influx cycle-{cycle+1} error", elapsed)
                finally:
                    ExtSrcEnv.start_influx_instance(ver)
                    # ensure_ext_env.sh resets InfluxDB buckets on bare-metal restart;
                    # re-create bucket and data so the next cycle's healthy check works.
                    self._setup_influx_data()

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
    # S08: Unresponsive service → timeout fires (cross-platform)
    # ==================================================================

    def test_svc_s08_mysql_black_hole_timeout(self):
        """S08-MySQL: black-hole listener → read_timeout fires within wall-clock bound.

        A TCP listener that accepts connections but never sends data
        simulates a service that is reachable but unresponsive.  The
        MySQL client library waits for the server greeting packet; with
        ``MYSQL_OPT_READ_TIMEOUT`` set to ``conn_timeout_ms / 1000``,
        the query must fail quickly.

        Cross-platform: works on Linux, macOS, and Windows without root
        privileges or kernel modules.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation
            - 2026-05-08 wpan Replace tc netem with cross-platform black-hole listener

        """
        src = "svc_s08_my"
        self._cleanup_src(src)
        with _BlackHoleListener() as bh:
            try:
                cfg = self._mysql_cfg()
                tdSql.execute(
                    f"create external source {src} type='mysql' "
                    f"host='127.0.0.1' port={bh.port} "
                    f"user='{cfg.user}' password='{cfg.password}' "
                    f"database='test'"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                )
                self._record_timing("S08-MySQL black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)

    def test_svc_s08_pg_black_hole_timeout(self):
        """S08-PG: black-hole listener → connect_timeout fires within wall-clock bound.

        A TCP listener that never responds to the PostgreSQL startup
        message.  The ``connect_timeout`` parameter must cause the query
        to fail quickly.

        Cross-platform: works on Linux, macOS, and Windows without root
        privileges or kernel modules.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-08 wpan Initial implementation (cross-platform black-hole)

        """
        src = "svc_s08_pg"
        self._cleanup_src(src)
        with _BlackHoleListener() as bh:
            try:
                cfg = self._pg_cfg()
                tdSql.execute(
                    f"create external source {src} type='postgresql' "
                    f"host='127.0.0.1' port={bh.port} "
                    f"user='{cfg.user}' password='{cfg.password}' "
                    f"database='test' schema=public"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                )
                self._record_timing("S08-PG black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)

    def test_svc_s08_influx_black_hole_timeout(self):
        """S08-Influx: black-hole listener → gRPC deadline fires within wall-clock bound.

        A TCP listener that never responds to HTTP/2 preface or gRPC
        frames.  The ``conn_timeout_ms`` gRPC deadline must cause the
        query to fail quickly.

        Cross-platform: works on Linux, macOS, and Windows without root
        privileges or kernel modules.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-07 wpan Initial implementation
            - 2026-05-08 wpan Replace tc netem with cross-platform black-hole listener

        """
        src = "svc_s08_inf"
        self._cleanup_src(src)
        with _BlackHoleListener() as bh:
            try:
                cfg = self._influx_cfg()
                opts = f"'api_token'='{cfg.token}','protocol'='flight_sql'"
                tdSql.execute(
                    f"create external source {src} type='influxdb' "
                    f"host='127.0.0.1' port={bh.port} "
                    f"user='u' password='' "
                    f"database='test' "
                    f"options({opts})"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                )
                self._record_timing("S08-Influx black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)
