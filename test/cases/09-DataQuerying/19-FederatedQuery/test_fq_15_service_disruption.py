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
    - MySQL, PostgreSQL, InfluxDB, SQL Server, and Oracle instances
      reachable.
    - Root / CAP_NET_ADMIN for network delay tests (s08).

SQL Server runs as the fourth provider in the same per-scenario form (s01,
s02, s03, s04, s05, s07, s08) on top of the step-1 zero-pushdown contract:
the remote side only ever executes a base scan, so a stopped/killed instance
must surface a bounded connection error and a restarted instance must serve
the same rows again via start_sqlserver_instance()'s readiness probe.

Oracle is the fifth provider and repeats the same seven scenarios, so the
matrix grows 29 → 36 (s06 stays Influx-only).  Oracle's DATABASE is the Easy
Connect service name and is mandatory; there is no database level, so the
test tables live in the login user's schema and are addressed as
``<source>.<table>`` through the source's default schema.  Connection-layer
failures are asserted exactly as TSDB_CODE_EXT_CONNECT_FAILED (Oracle's
08xxx and ORA-12541 / 12545 / 12170 / 12560 / 03113 / 03114 / 03135
diagnostics, DS 5.3.8).  The black-hole case is a login timeout instead; it
accepts exactly the two documented classifications of that diagnostic -
TSDB_CODE_EXT_QUERY_TIMEOUT (SQLSTATE HYT00) and
TSDB_CODE_EXT_CONNECT_FAILED (ORA-12170) - so a syntax-class or catalog-class
error still fails the case.  Oracle start-up takes minutes, not seconds, so
every restart goes through start_oracle_instance()'s readiness probe and a
failed start fails the case instead of surfacing later as a recovery
mismatch.
"""

import os
import socket
import subprocess
import sys
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
    TSDB_CODE_EXT_CONNECT_FAILED,
    TSDB_CODE_EXT_QUERY_TIMEOUT,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# Wall-clock upper bound for a single query against a stopped service.
# With conn_timeout_ms=5000 the RPC must complete within ~5-7s;
# we allow 15s headroom for scheduling jitter, retries, and slow CI.
_QUERY_WALL_LIMIT_S = 15.0
# A SIGKILL leaves the client with an established socket whose peer is gone.
# The Oracle ODBC driver only gives up on that dead connection after its own
# network timeout (~55 s observed), which it applies regardless of
# SQL_ATTR_LOGIN_TIMEOUT / SQL_ATTR_QUERY_TIMEOUT because the TCP connection
# itself succeeded.  The bound still proves the query terminates instead of
# hanging forever.
_ORACLE_KILL_WALL_LIMIT_S = 90.0

# Short connect timeout for test sources (milliseconds).
_SHORT_CONN_TIMEOUT_MS = 2000

# Oracle statement timeout for test sources (milliseconds).  The connect
# timeout stays short so a downed instance fails fast; the statement timeout
# is larger because a cold Oracle instance can need more than two seconds for
# the first scan, while the wall bound below still holds.
_ORACLE_READ_TIMEOUT_MS = 5000

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

# SQL Server 5-row test table (dbo schema; T-SQL idempotent re-create)
_SQLSERVER_TABLE_SQLS = [
    "IF OBJECT_ID('dbo.svc_t') IS NOT NULL DROP TABLE dbo.svc_t",
    "CREATE TABLE dbo.svc_t "
    "(id INT NOT NULL PRIMARY KEY, val INT, name NVARCHAR(32))",
    "INSERT INTO dbo.svc_t VALUES (1,10,'alpha'),(2,20,'beta'),"
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

# Oracle 5-row test table contents.  Oracle has a single namespace level (the
# login user's schema) and every external table needs a timestamp-mappable
# column, so all Oracle fixtures use a TS TIMESTAMP(3) first column.
_ORACLE_EXPECTED_ROWS = [
    (1, 10, "alpha"),
    (2, 20, "beta"),
    (3, 30, "gamma"),
    (4, 40, "delta"),
    (5, 50, "epsilon"),
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


class _SynDropPort:
    """TCP port on 127.0.0.1 whose incoming SYNs are silently dropped.

    Reproduces a service that is unreachable at the packet level - a
    stopped container behind a published port, or a firewall DROP rule -
    without iptables, root or kernel modules.  A listener with a backlog
    of zero holds one connection that is never accepted, which fills its
    accept queue; once the queue is full the kernel discards further
    SYNs instead of answering them (``tcp_conn_request`` drops whenever
    ``sk_acceptq_is_full()``), so a client stays in SYN_SENT through the
    whole SYN retransmission backoff (~60 s) exactly as it does against a
    black-holed port.

    The constructor waits until the kernel reports the accept queue full
    and then probes the port once; a platform that answers that probe
    does not drop SYNs and raises ``RuntimeError`` instead of pretending
    the scenario is reproducible.  Linux-only in practice (the queue
    depth is read from /proc/net/tcp).

    Use as a context manager; ``port`` listens on 127.0.0.1::

        with _SynDropPort() as syn:
            # connect to syn.port hangs in SYN_SENT
            ...
    """

    _PROBE_TIMEOUT_S = 1.5

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(0)  # accept queue capacity: a single connection
        self.port = self._sock.getsockname()[1]
        self._filler = socket.create_connection(
            ("127.0.0.1", self.port), timeout=2)
        self._wait_until_queue_full()
        self._verify_syn_dropped()

    def _wait_until_queue_full(self):
        # For a LISTEN socket /proc/net/tcp reports sk_ack_backlog as the
        # rx_queue column; waiting for it removes the race between the
        # filler's final ACK and the query's first SYN.  Where the file
        # does not exist the probe below still tells us the truth.
        deadline = time.time() + 2
        while time.time() < deadline:
            try:
                with open("/proc/net/tcp") as fh:
                    backlog = None
                    for line in fh:
                        fields = line.split()
                        if len(fields) < 5 or fields[0] == "sl":
                            continue
                        _, port_hex = fields[1].split(":")
                        if (fields[3] == "0A"
                                and int(port_hex, 16) == self.port):
                            backlog = int(fields[4].split(":")[1], 16)
                            break
            except OSError:
                time.sleep(0.1)
                return
            if backlog is not None and backlog >= 1:
                return
            time.sleep(0.01)
        time.sleep(0.05)

    def _verify_syn_dropped(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(self._PROBE_TIMEOUT_S)
        try:
            try:
                probe.connect(("127.0.0.1", self.port))
            except socket.timeout:
                return
            raise RuntimeError(
                "this platform answers SYNs on a saturated accept queue; "
                "the SYN-drop scenario cannot be reproduced")
        finally:
            probe.close()

    def close(self):
        for s in (self._filler, self._sock):
            try:
                s.close()
            except OSError:
                pass
        self._filler = self._sock = None

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

    def setup_method(self, method):
        """Pre-test safety net: if a previous test left MySQL/PG/InfluxDB/SQL Server/Oracle down, restart them."""
        import pymysql, psycopg2, requests
        mysql_cfg  = self._mysql_cfg()
        pg_cfg     = self._pg_cfg()
        influx_cfg = self._influx_cfg()
        mysql_ver  = self._mysql_ver()
        pg_ver     = self._pg_ver()
        influx_ver = self._influx_ver()

        if "mysql" in ExtSrcEnv.required_services():
            try:
                conn = pymysql.connect(
                    host=mysql_cfg.host, port=mysql_cfg.port,
                    user=mysql_cfg.user, password=mysql_cfg.password,
                    connect_timeout=3)
                conn.close()
            except Exception:
                tdLog.info("[fq15 setup_method] MySQL is down — restarting ...")
                ExtSrcEnv.start_mysql_instance(mysql_ver)

        if "pg" in ExtSrcEnv.required_services():
            try:
                conn = psycopg2.connect(
                    host=pg_cfg.host, port=pg_cfg.port,
                    user=pg_cfg.user, password=pg_cfg.password,
                    connect_timeout=3)
                conn.close()
            except Exception:
                tdLog.info("[fq15 setup_method] PostgreSQL is down — restarting ...")
                ExtSrcEnv.start_pg_instance(pg_ver)

        if "influx" in ExtSrcEnv.required_services():
            try:
                r = requests.get(f"http://{influx_cfg.host}:{influx_cfg.port}/health",
                                 timeout=3)
                if r.status_code not in (200, 204, 401):
                    raise RuntimeError(f"InfluxDB health check returned {r.status_code}")
            except Exception:
                tdLog.info("[fq15 setup_method] InfluxDB is down — restarting ...")
                ExtSrcEnv.start_influx_instance(influx_ver)

        if "sqlserver" in ExtSrcEnv.required_services():
            try:
                ExtSrcEnv.sqlserver_query_cfg(self._sqlserver_cfg(), "master", "SELECT 1")
            except Exception:
                tdLog.info("[fq15 setup_method] SQL Server is down — restarting ...")
                ExtSrcEnv.start_sqlserver_instance(self._sqlserver_ver())

        if "oracle" in ExtSrcEnv.required_services():
            try:
                ExtSrcEnv.oracle_query_cfg(self._oracle_cfg(), "SELECT 1 FROM DUAL")
            except Exception:
                tdLog.info("[fq15 setup_method] Oracle is down — restarting ...")
                ExtSrcEnv.start_oracle_instance(self._oracle_ver())


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
        # Each grouped CI task owns only its declared provider.
        services = ExtSrcEnv.required_services()
        for db in ("fq_svc_my", "fq_svc_pg"):
            for service, drop in (("mysql", ExtSrcEnv.mysql_drop_db),
                                  ("pg", ExtSrcEnv.pg_drop_db)):
                if service in services:
                    try:
                        drop(db)
                    except Exception:
                        pass
        if "sqlserver" in services:
            for db in ("fq_svc_s01_ss", "fq_svc_s02_ss", "fq_svc_s03_ss",
                       "fq_svc_s04_ss", "fq_svc_s05_ss", "fq_svc_s07_ss"):
                try:
                    ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
                except Exception:
                    pass
        if "influx" in services:
            try:
                ExtSrcEnv.influx_drop_db(_INFLUX_BUCKET)
            except Exception:
                pass
        if "oracle" in services:
            # Oracle has no database level to drop, so each scenario owns a
            # table in the login user's schema instead of its own database.
            # teardown_class receives the class rather than an instance, so
            # the version configs are taken directly instead of through the
            # instance helper _oracle_cfg().
            for cfg_o in ExtSrcEnv.oracle_version_configs():
                for table in ("SVC_S01_T", "SVC_S02_T", "SVC_S03_T",
                              "SVC_S04_T", "SVC_S05_T", "SVC_S07_T"):
                    try:
                        ExtSrcEnv.oracle_drop_table_cfg(cfg_o, table)
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
        """Create InfluxDB external source with optional extra options.

        Generic service-disruption coverage follows the common Influx protocol
        resolver so this file can run against HTTP as well as Flight SQL.  The
        normal disruption cases must also set short deadlines; otherwise they
        exercise the connector's production default timeout instead of the
        bounded-failure contract asserted by this suite.
        """
        if extra_options is None:
            extra_options = (
                f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                f"'read_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}'"
            )
        self._mk_influx_real(
            name,
            database=database,
            extra_options=extra_options,
        )

    def _mk_sqlserver_src(self, name, database, extra_options=None):
        """Create SQL Server external source with optional extra options.

        Like the other providers, the disruption cases must set short
        deadlines; otherwise they would exercise the connector's production
        default timeout instead of the bounded-failure contract asserted by
        this suite.  SQL Server's TLS keys (tls_enabled /
        trust_server_certificate) are unrelated to the timeout axis, and the
        test environment connects with trust_server_certificate.
        """
        if extra_options is None:
            extra_options = (
                f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                f"'read_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}'"
            )
        self._mk_sqlserver_real(
            name,
            database=database,
            schema="dbo",
            extra_options=extra_options,
        )

    def _mk_oracle_src(self, name, extra_options=None, database=None,
                       schema=None):
        """Create Oracle external source with optional extra options.

        Oracle's DATABASE field is the Easy Connect service name and is
        mandatory; there is no database level, so ``schema`` (the login user
        that owns the test tables) is the only namespace.  A two-segment
        ``<source>.<table>`` path resolves against that default schema, so
        both values default to the active instance.  As with the other
        providers the disruption cases set short deadlines, otherwise they
        would exercise the connector's production defaults instead of the
        bounded-failure contract asserted by this suite.
        """
        if extra_options is None:
            extra_options = (
                f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                f"'read_timeout_ms'='{_ORACLE_READ_TIMEOUT_MS}'"
            )
        cfg = self._oracle_cfg()
        self._mk_oracle_real(
            name,
            database=database if database is not None else cfg.service,
            schema=schema if schema is not None else cfg.user.upper(),
            extra_options=extra_options,
        )

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

    def _setup_sqlserver_data(self, db):
        cfg = self._sqlserver_cfg()
        ExtSrcEnv.sqlserver_create_db_cfg(cfg, db)
        ExtSrcEnv.sqlserver_exec_cfg(cfg, db, _SQLSERVER_TABLE_SQLS)

    def _setup_oracle_data(self, table):
        """(Re)create the 5-row Oracle test table in the login user's schema.

        Oracle has no database level and no per-scenario database to isolate
        the fixtures, so each scenario passes its own table name instead.
        The table carries a TIMESTAMP column, which every external table
        needs as its time key, and is created unquoted so the name folds to
        upper case exactly like the paths the tests use.
        """
        cfg = self._oracle_cfg()
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        ExtSrcEnv.oracle_exec_cfg(cfg, [
            f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10) NOT NULL, "
            "VAL NUMBER(10), NAME VARCHAR2(32))",
            f"INSERT INTO {table} VALUES "
            "(TIMESTAMP '2024-01-01 00:00:00.000', 1, 10, 'alpha')",
            f"INSERT INTO {table} VALUES "
            "(TIMESTAMP '2024-01-01 00:01:00.000', 2, 20, 'beta')",
            f"INSERT INTO {table} VALUES "
            "(TIMESTAMP '2024-01-01 00:02:00.000', 3, 30, 'gamma')",
            f"INSERT INTO {table} VALUES "
            "(TIMESTAMP '2024-01-01 00:03:00.000', 4, 40, 'delta')",
            f"INSERT INTO {table} VALUES "
            "(TIMESTAMP '2024-01-01 00:04:00.000', 5, 50, 'epsilon')",
            "COMMIT",
        ])

    def _mysql_ver(self):
        return (getattr(self, "_active_mysql_ver", None)
                or ExtSrcEnv.MYSQL_VERSIONS[0])

    def _pg_ver(self):
        return (getattr(self, "_active_pg_ver", None)
                or ExtSrcEnv.PG_VERSIONS[0])

    def _influx_ver(self):
        return (getattr(self, "_active_influx_ver", None)
                or ExtSrcEnv.INFLUX_VERSIONS[0])

    def _sqlserver_ver(self):
        # The versioned fixture parametrizes MySQL/PG/InfluxDB only (SQL Server
        # is deliberately not a fourth axis); fall back to the primary version.
        return (getattr(self, "_active_sqlserver_ver", None)
                or ExtSrcEnv.SQLSERVER_VERSIONS[0])

    def _oracle_ver(self):
        # Oracle follows SQL Server as a fixed provider: not part of the
        # versioned fixture, so select the primary configured version.
        return (getattr(self, "_active_oracle_ver", None)
                or ExtSrcEnv.ORACLE_VERSIONS[0])

    def _assert_oracle_catalog(self, src):
        """Assert the Oracle source survives in the catalog with its address.

        Called while the instance is down: the catalog is local metadata, so
        a stopped/killed Oracle must not remove or rewrite the source.
        """
        cfg = self._oracle_cfg()
        # Reserved-looking column names are quoted, as in fq_07/fq_08.
        tdSql.query(
            "select source_name, `type`, host, `port`, `user`, `database`, "
            "`schema` from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, src)
        tdSql.checkData(0, 1, "oracle")
        tdSql.checkData(0, 2, cfg.host)
        tdSql.checkData(0, 3, cfg.port)
        tdSql.checkData(0, 4, cfg.user)
        tdSql.checkData(0, 5, cfg.service)
        tdSql.checkData(0, 6, cfg.user.upper())

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

        S02-PG: SIGKILL PostgreSQL → query fails within wall-clock bound.

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

        S03-PG: stop → errors → restart → automatic reconnect + correct result.

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

        S04-PG: stop mid-burst → all queries complete within wall-clock bound.

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

        S05-PG: getTableSchema while PG is down → fails fast.

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

        S07-PG: 3× stop/start cycles → query always succeeds after restart.

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
                    # start_influx_instance() may fall back to a hard reset, which
                    # recreates the admin token. Refresh the external source after
                    # restart so it never keeps a stale token from the previous cycle.
                    self._cleanup_src(src)
                    self._mk_influx_src(src, database=_INFLUX_BUCKET)
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
                opts = (
                    f"'api_token'='{cfg.token}','protocol'='flight_sql',"
                    f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                    f"'read_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}'"
                )
                tdSql.execute(
                    self._mk_influx_source_sql(
                        src,
                        "127.0.0.1",
                        bh.port,
                        cfg.token,
                        database="test",
                        extra_options=opts,
                    )
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                )
                self._record_timing("S08-Influx black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)

    # ==================================================================
    # SQL Server additions (4th provider)
    #
    # Same per-scenario form as the MySQL / PG / InfluxDB cases above:
    # s01 stop, s02 kill, s03 restart recovery, s04 burst, s05 schema-down,
    # s07 repeated cycles, s08 black-hole.  s06 (isAlive probe) stays
    # Influx-only by design, so the matrix grows 22 → 29.
    #
    # SQL Server timeout plumbing: connect_timeout_ms → SQL_ATTR_LOGIN_TIMEOUT
    # and read_timeout_ms → SQL_ATTR_QUERY_TIMEOUT.  Queries follow the step-1
    # zero-pushdown contract (remote side runs a base scan only), so a downed
    # instance must surface a bounded connection error and a restarted one
    # must serve the same rows again.
    # ==================================================================

    def test_svc_s01_sqlserver_stop_query_bounded(self):
        """S01-SQLServer: stop SQL Server → query fails within wall-clock bound.

        Verifies that ``connect_timeout_ms`` / ``read_timeout_ms`` (mapped to
        the ODBC login/query timeouts) cause the query to fail quickly rather
        than blocking indefinitely.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        src, db = "svc_s01_ss", "fq_svc_s01_ss"
        ver = self._sqlserver_ver()
        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            # Verify healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            # Stop and verify fast failure
            ExtSrcEnv.stop_sqlserver_instance(ver)
            try:
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S01-SQLServer stop #{i+1}", elapsed)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s02_sqlserver_kill_query_bounded(self):
        """S02-SQLServer: SIGKILL SQL Server → query fails within wall-clock bound.

        A hard container kill leaves the client with a broken connection and
        no clean disconnect; the ODBC query timeout must still fire so the
        federated query stays bounded.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        src, db = "svc_s02_ss", "fq_svc_s02_ss"
        ver = self._sqlserver_ver()

        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.kill_sqlserver_instance(ver)
            try:
                time.sleep(0.5)  # let kernel notice process exit
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t limit 1",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S02-SQLServer kill #{i+1}", elapsed)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s03_sqlserver_restart_recovery(self):
        """S03-SQLServer: stop → errors → restart → reconnect + correct result.

        start_sqlserver_instance() already waits for a probe query on master,
        so after it returns the source must transparently serve the same rows
        through a fresh connection.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        src, db = "svc_s03_ss", "fq_svc_s03_ss"
        ver = self._sqlserver_ver()
        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            # Healthy
            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)

            # Stop → errors
            ExtSrcEnv.stop_sqlserver_instance(ver)
            try:
                for i in range(3):
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S03-SQLServer stop error #{i+1}", time.monotonic() - t0)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ver)
                # Re-create the test database/table in case the restart path
                # reset it, so the recovery query below is deterministic.
                self._setup_sqlserver_data(db)

            time.sleep(2)

            # Recovery — must succeed with correct data
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query(f"select val from {src}.svc_t where id=1")
            tdSql.checkData(0, 0, 10)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s04_sqlserver_burst_all_bounded(self):
        """S04-SQLServer: stop mid-burst → all queries complete within wall-clock bound.

        Issues N queries against a stopped SQL Server.  Each individual query
        must complete within the wall-clock bound (no accumulation), proving
        the ODBC timeout fires per query, not just the first.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        N = 5
        src, db = "svc_s04_ss", "fq_svc_s04_ss"
        ver = self._sqlserver_ver()
        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_sqlserver_instance(ver)
            try:
                total_start = time.monotonic()
                for i in range(N):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_t",
                        expected_errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                    )
                    self._record_timing(f"S04-SQLServer burst #{i+1}", elapsed)
                total_elapsed = time.monotonic() - total_start
                tdLog.info(f"  [TIMING] S04-SQLServer total {N} queries: {total_elapsed:.2f}s")
            finally:
                ExtSrcEnv.start_sqlserver_instance(ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s05_sqlserver_schema_down_bounded(self):
        """S05-SQLServer: schema access while SQL Server is down → fails fast.

        With the instance down, resolving the remote table (the connector's
        pre-query schema/metadata step) must fail within the wall-clock bound
        instead of hanging on the ODBC connection attempt.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        src, db = "svc_s05_ss", "fq_svc_s05_ss"
        ver = self._sqlserver_ver()
        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            # Verify schema/data access works while healthy
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_sqlserver_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t",
                )
                self._record_timing("S05-SQLServer schema-down (SELECT)", elapsed)
            finally:
                ExtSrcEnv.start_sqlserver_instance(ver)
                self._setup_sqlserver_data(db)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s07_sqlserver_repeated_cycles(self):
        """S07-SQLServer: 3× stop/start cycles → query always succeeds after restart.

        Verifies that repeated disruptions don't accumulate stale ODBC
        handles or corrupt internal state.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        CYCLES = 3
        src, db = "svc_s07_ss", "fq_svc_s07_ss"
        ver = self._sqlserver_ver()
        self._cleanup_src(src)
        try:
            self._setup_sqlserver_data(db)
            self._mk_sqlserver_src(src, database=db)

            for cycle in range(CYCLES):
                tdLog.debug(f"  S07-SQLServer cycle {cycle+1}/{CYCLES}")

                # Healthy
                tdSql.query(f"select count(*) from {src}.svc_t")
                tdSql.checkData(0, 0, 5)

                # Disrupt
                ExtSrcEnv.stop_sqlserver_instance(ver)
                try:
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_t",
                                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                    self._record_timing(f"S07-SQLServer cycle-{cycle+1} error", time.monotonic() - t0)
                finally:
                    ExtSrcEnv.start_sqlserver_instance(ver)
                    # Re-create test data so the next cycle's healthy check works.
                    self._setup_sqlserver_data(db)

                time.sleep(2)

            # Final verification after all cycles
            tdSql.query(f"select count(*) from {src}.svc_t")
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db_cfg(self._sqlserver_cfg(), db)
            except Exception:
                pass

    def test_svc_s08_sqlserver_black_hole_timeout(self):
        """S08-SQLServer: black-hole listener → login timeout fires within wall-clock bound.

        A TCP listener that accepts connections but never sends the pre-login
        response.  The ``connect_timeout_ms`` → SQL_ATTR_LOGIN_TIMEOUT mapping
        must cause the query to fail quickly.

        Cross-platform: works on Linux, macOS, and Windows without root
        privileges or kernel modules.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-10-09 wpan Add SQL Server to the per-provider matrix

        """
        src = "svc_s08_ss"
        self._cleanup_src(src)
        with _BlackHoleListener() as bh:
            try:
                cfg = self._sqlserver_cfg()
                opts = (
                    f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                    f"'read_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}'"
                )
                tdSql.execute(
                    f"create external source {src} type='sqlserver' "
                    f"host='127.0.0.1' port={bh.port} "
                    f"user='{cfg.user}' password='{cfg.password}' "
                    f"database='master' schema='dbo' "
                    f"options({opts})"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                )
                self._record_timing("S08-SQLServer black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)

    # ==================================================================
    # Oracle additions (5th provider)
    #
    # Same per-scenario form as the SQL Server block above: s01 stop,
    # s02 kill, s03 restart recovery, s04 burst, s05 schema-down,
    # s07 repeated cycles, s08 black-hole.  s06 (isAlive probe) stays
    # Influx-only by design, so the matrix grows 29 → 36.
    #
    # Oracle timeout plumbing: connect_timeout_ms → SQL_ATTR_LOGIN_TIMEOUT
    # and read_timeout_ms → SQL_ATTR_QUERY_TIMEOUT.  Queries follow the
    # step-1 zero-pushdown contract (remote side runs a base scan only),
    # so a downed instance must surface a bounded connection error and a
    # restarted one must serve the same rows again.
    #
    # Oracle has no database level: DATABASE carries the Easy Connect
    # service name and the tables live in the login user's schema, which a
    # two-segment <source>.<table> path resolves against the source's
    # default schema.  Each scenario therefore owns a table name instead of
    # a database name, and the fixtures always set database=<service> and
    # schema=<login user>.
    # ==================================================================

    def test_svc_s01_oracle_stop_query_bounded(self):
        """S01-Oracle: stop Oracle → query fails within wall-clock bound.

        Verifies that ``connect_timeout_ms`` / ``read_timeout_ms`` (mapped to
        the ODBC login/query timeouts) cause the query to fail quickly rather
        than blocking indefinitely.  A stopped instance is a connection-layer
        failure: the 08xxx / ORA-12541 / ORA-12560 diagnostics map to
        TSDB_CODE_EXT_CONNECT_FAILED (DS 5.3.8).

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        src, table = "svc_s01_or", "SVC_S01_T"
        ver = self._oracle_ver()
        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            # Verify healthy
            tdSql.query(f"select count(*) from {src}.svc_s01_t")
            tdSql.checkData(0, 0, 5)

            # Stop and verify fast failure
            ExtSrcEnv.stop_oracle_instance(ver)
            try:
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_s01_t limit 1",
                        expected_errno=TSDB_CODE_EXT_CONNECT_FAILED,
                    )
                    self._record_timing(f"S01-Oracle stop #{i+1}", elapsed)

                # The source must survive in the catalog while it is down.
                self._assert_oracle_catalog(src)

                # taosd itself must keep serving local queries.
                tdSql.query("select server_version()")
                tdSql.checkRows(1)
            finally:
                started = ExtSrcEnv.start_oracle_instance(ver)
            if not started:
                raise AssertionError(
                    f"Oracle {ver} did not become ready after restart")
        finally:
            self._cleanup_src(src)

    def test_svc_s02_oracle_kill_query_bounded(self):
        """S02-Oracle: SIGKILL Oracle → query fails within wall-clock bound.

        A hard kill leaves the client with a broken connection and no clean
        disconnect; the ODBC timeout must still fire so the federated query
        stays bounded.  ORA-03113 / ORA-03114 / ORA-03135 (or a fresh
        ORA-12541) all classify as TSDB_CODE_EXT_CONNECT_FAILED.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        src, table = "svc_s02_or", "SVC_S02_T"
        ver = self._oracle_ver()

        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            tdSql.query(f"select count(*) from {src}.svc_s02_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.kill_oracle_instance(ver)
            try:
                # The platform kill does not wait for the listener socket to
                # close; wait for it deterministically before asserting the
                # failure mode, then let the kernel finish the teardown.
                cfg = self._oracle_cfg()
                deadline = time.time() + 30
                while time.time() < deadline and ExtSrcEnv._tcp_probe(
                        cfg.host, cfg.port):
                    time.sleep(0.5)
                time.sleep(0.5)
                for i in range(3):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_s02_t limit 1",
                        wall_limit=_ORACLE_KILL_WALL_LIMIT_S,
                        expected_errno=TSDB_CODE_EXT_CONNECT_FAILED,
                    )
                    self._record_timing(f"S02-Oracle kill #{i+1}", elapsed)

                self._assert_oracle_catalog(src)
                tdSql.query("select server_version()")
                tdSql.checkRows(1)
            finally:
                started = ExtSrcEnv.start_oracle_instance(ver)
            if not started:
                raise AssertionError(
                    f"Oracle {ver} did not become ready after restart")
        finally:
            self._cleanup_src(src)

    def test_svc_s03_oracle_restart_recovery(self):
        """S03-Oracle: stop → errors → restart → reconnect + identical rows.

        start_oracle_instance() already waits for a probe query on DUAL, so
        after it returns the source must transparently serve exactly the
        rows read before the outage through a fresh connection.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        src, table = "svc_s03_or", "SVC_S03_T"
        ver = self._oracle_ver()
        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            # Healthy: capture the exact pre-outage rows
            tdSql.query(
                f"select id, val, name from {src}.svc_s03_t order by id")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, "alpha")
            before = [tuple(row) for row in tdSql.queryResult]
            assert before == _ORACLE_EXPECTED_ROWS, (
                f"unexpected pre-outage rows: {before!r}")

            # Stop → errors
            ExtSrcEnv.stop_oracle_instance(ver)
            try:
                for i in range(3):
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_s03_t",
                                expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
                    self._record_timing(
                        f"S03-Oracle stop error #{i+1}",
                        time.monotonic() - t0)
            finally:
                started = ExtSrcEnv.start_oracle_instance(ver)
                if started:
                    # A quick restart reuses the managed instance's data
                    # directory, but recreate the fixture so the recovery
                    # assertion does not depend on what a stop path left
                    # behind.
                    self._setup_oracle_data(table)
            if not started:
                raise AssertionError(
                    f"Oracle {ver} did not become ready after restart")

            time.sleep(2)

            # Recovery — must succeed with exactly the same rows
            tdSql.query(
                f"select id, val, name from {src}.svc_s03_t order by id")
            tdSql.checkRows(5)
            after = [tuple(row) for row in tdSql.queryResult]
            assert after == before, (
                f"rows changed across restart: before={before!r}, "
                f"after={after!r}")
        finally:
            self._cleanup_src(src)

    def test_svc_s04_oracle_burst_all_bounded(self):
        """S04-Oracle: stop mid-burst → all queries complete within wall-clock bound.

        Issues N queries against a stopped Oracle.  Each individual query
        must complete within the wall-clock bound (no accumulation), proving
        the ODBC timeout fires per query, not just the first.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        N = 5
        src, table = "svc_s04_or", "SVC_S04_T"
        ver = self._oracle_ver()
        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            tdSql.query(f"select count(*) from {src}.svc_s04_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_oracle_instance(ver)
            try:
                total_start = time.monotonic()
                for i in range(N):
                    elapsed = self._assert_query_bounded(
                        f"select * from {src}.svc_s04_t",
                        expected_errno=TSDB_CODE_EXT_CONNECT_FAILED,
                    )
                    self._record_timing(f"S04-Oracle burst #{i+1}", elapsed)
                total_elapsed = time.monotonic() - total_start
                tdLog.info(
                    f"  [TIMING] S04-Oracle total {N} queries: "
                    f"{total_elapsed:.2f}s")
                tdSql.query("select server_version()")
                tdSql.checkRows(1)
            finally:
                started = ExtSrcEnv.start_oracle_instance(ver)
            if not started:
                raise AssertionError(
                    f"Oracle {ver} did not become ready after restart")
        finally:
            self._cleanup_src(src)

    def test_svc_s05_oracle_schema_down_bounded(self):
        """S05-Oracle: schema access while Oracle is down → fails fast.

        Resolving the remote table (the connector's pre-query schema step)
        needs a connection, so with the instance down it must fail within the
        wall-clock bound and with the connection-layer code, not with a
        syntax-class or table-not-exist code.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        src, table = "svc_s05_or", "SVC_S05_T"
        ver = self._oracle_ver()
        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            # Verify schema/data access works while healthy
            tdSql.query(f"select count(*) from {src}.svc_s05_t")
            tdSql.checkData(0, 0, 5)

            ExtSrcEnv.stop_oracle_instance(ver)
            try:
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_s05_t",
                    expected_errno=TSDB_CODE_EXT_CONNECT_FAILED,
                )
                self._record_timing(
                    "S05-Oracle schema-down (SELECT)", elapsed)
            finally:
                started = ExtSrcEnv.start_oracle_instance(ver)
                if started:
                    self._setup_oracle_data(table)
            if not started:
                raise AssertionError(
                    f"Oracle {ver} did not become ready after restart")
        finally:
            self._cleanup_src(src)

    def test_svc_s07_oracle_repeated_cycles(self):
        """S07-Oracle: 3× stop/start cycles → query always succeeds after restart.

        Verifies that repeated disruptions don't accumulate stale ODBC
        handles or corrupt internal state, and that the source, the catalog
        entry and the data all survive the cycles.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        CYCLES = 3
        src, table = "svc_s07_or", "SVC_S07_T"
        ver = self._oracle_ver()
        self._cleanup_src(src)
        try:
            self._setup_oracle_data(table)
            self._mk_oracle_src(src)

            for cycle in range(CYCLES):
                tdLog.debug(f"  S07-Oracle cycle {cycle+1}/{CYCLES}")

                # Healthy
                tdSql.query(f"select count(*) from {src}.svc_s07_t")
                tdSql.checkData(0, 0, 5)

                # Disrupt
                ExtSrcEnv.stop_oracle_instance(ver)
                try:
                    t0 = time.monotonic()
                    tdSql.error(f"select * from {src}.svc_s07_t",
                                expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
                    self._record_timing(
                        f"S07-Oracle cycle-{cycle+1} error",
                        time.monotonic() - t0)
                finally:
                    started = ExtSrcEnv.start_oracle_instance(ver)
                    if started:
                        # Re-create test data so the next cycle's healthy
                        # check works regardless of the stop path.
                        self._setup_oracle_data(table)
                if not started:
                    raise AssertionError(
                        f"Oracle {ver} did not become ready in cycle "
                        f"{cycle+1}")

                time.sleep(2)

            # Final verification: catalog entry, taosd liveness, exact rows
            self._assert_oracle_catalog(src)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
            tdSql.query(
                f"select id, val, name from {src}.svc_s07_t order by id")
            tdSql.checkRows(5)
            after = [tuple(row) for row in tdSql.queryResult]
            assert after == _ORACLE_EXPECTED_ROWS, (
                f"rows changed across {CYCLES} cycles: {after!r}")
        finally:
            self._cleanup_src(src)

    def test_svc_s08_oracle_black_hole_timeout(self):
        """S08-Oracle: black-hole listener → login timeout fires within wall-clock bound.

        A TCP listener that accepts connections but never answers the Oracle
        Net handshake.  ``connect_timeout_ms`` → SQL_ATTR_LOGIN_TIMEOUT must
        make the query fail quickly.  The expired login is classified as a
        timeout by both documented routes (DS 5.3.8): SQLSTATE HYT00 →
        TSDB_CODE_EXT_QUERY_TIMEOUT, ORA-12170 → TSDB_CODE_EXT_CONNECT_FAILED.
        Any other code (syntax / catalog / type) fails the case.

        Cross-platform: works on Linux, macOS, and Windows without root
        privileges or kernel modules.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-26 wpan Add Oracle to the per-provider matrix

        """
        src = "svc_s08_or"
        self._cleanup_src(src)
        with _BlackHoleListener() as bh:
            try:
                cfg = self._oracle_cfg()
                opts = (
                    f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                    f"'read_timeout_ms'='{_ORACLE_READ_TIMEOUT_MS}'"
                )
                tdSql.execute(
                    f"create external source {src} type='oracle' "
                    f"host='127.0.0.1' port={bh.port} "
                    f"user='{cfg.user}' password='{cfg.password}' "
                    f"database={cfg.service} schema={cfg.user.upper()} "
                    f"options({opts})"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_s08_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                    expected_errno=[TSDB_CODE_EXT_QUERY_TIMEOUT,
                                    TSDB_CODE_EXT_CONNECT_FAILED],
                )
                self._record_timing("S08-Oracle black-hole timeout", elapsed)
            finally:
                self._cleanup_src(src)

    def test_svc_s08_oracle_syn_drop_bounded(self):
        """S08-Oracle (SYN drop): a black-holed connect is deadline-bounded.

        When the Oracle container is stopped but its published port stays
        behind the docker proxy, new SYNs to that port are dropped instead
        of refused: the kernel retries for the full SYN backoff (~60 s) and
        SQL_ATTR_LOGIN_TIMEOUT never fires.  The provider must enforce
        connect_timeout_ms itself, or one query blocks for a minute.
        _SynDropPort reproduces the black hole by saturating a local accept
        queue, so the case needs neither iptables nor root and really runs
        in CI instead of skipping for lack of privileges.

        Catalog: - Query:FederatedServiceDisruption

        Since: v3.5.0.0

        Labels: common,ci

        History:
            - 2026-10-30 dapan1121 Add the SYN-drop connect bound
            - 2026-10-30 dapan1121 Drop the iptables dependency: saturate
              the accept queue so the check runs in every CI job

        """
        if sys.platform != "linux":
            pytest.skip("the SYN-drop check needs the Linux accept queue")
        src = "svc_s08s_or"
        self._cleanup_src(src)
        with _SynDropPort() as syn:
            try:
                cfg = self._oracle_cfg()
                opts = (
                    f"'connect_timeout_ms'='{_SHORT_CONN_TIMEOUT_MS}',"
                    f"'read_timeout_ms'='{_ORACLE_READ_TIMEOUT_MS}'"
                )
                tdSql.execute(
                    f"create external source {src} type='oracle' "
                    f"host='127.0.0.1' port={syn.port} "
                    f"user='{cfg.user}' password='{cfg.password}' "
                    f"database={cfg.service} schema={cfg.user.upper()} "
                    f"options({opts})"
                )
                elapsed = self._assert_query_bounded(
                    f"select * from {src}.svc_s08s_t limit 1",
                    wall_limit=_QUERY_WALL_LIMIT_S,
                    expected_errno=TSDB_CODE_EXT_CONNECT_FAILED,
                )
                # A dropped SYN cannot fail before the first retransmit;
                # require most of the connect deadline to have passed so a
                # fast unrelated error cannot masquerade as the bound.
                assert elapsed >= _SHORT_CONN_TIMEOUT_MS / 1000.0 * 0.75, (
                    f"query failed in {elapsed:.2f}s, before the "
                    f"{_SHORT_CONN_TIMEOUT_MS} ms connect deadline could "
                    "fire - the port did not behave like a SYN black hole")
                self._record_timing("S08-Oracle SYN-drop bound", elapsed)
            finally:
                self._cleanup_src(src)
