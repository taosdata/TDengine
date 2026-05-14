"""
test_ext_env_lifecycle.py

Lifecycle validation for ensure_ext_env.sh: verifies that every
stop / kill / start operation leaves the external DB in a confirmed
state before returning.  Uses the same ExtSrcEnv class methods that
FQ test cases call — not raw shell — so the test exercises the exact
code path used in production.

Covered operations per provider (MySQL, PostgreSQL, InfluxDB):
  - stop  → verify port unreachable → start → verify SQL/HTTP ready
  - kill  → verify port unreachable → start → verify SQL/HTTP ready
  - 3-round repeated stop/start (stability)

This test does NOT query TDengine.  It is self-contained and can be
run as part of the FQ test suite or standalone.
"""

import socket
import time

import pymysql
import psycopg2
import requests

from new_test_framework.utils import tdLog

from federated_query_common import ExtSrcEnv


# ---------------------------------------------------------------------------
# Port reachability helpers
# ---------------------------------------------------------------------------
def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except (ConnectionRefusedError, OSError, TimeoutError):
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def _wait_port_gone(host: str, port: int, max_s: float = 15.0) -> bool:
    """Poll every 0.2 s until the TCP port becomes unreachable.

    Returns True if the port closed within max_s seconds.
    """
    deadline = time.monotonic() + max_s
    while time.monotonic() < deadline:
        if not _port_open(host, port, timeout=0.5):
            return True
        time.sleep(0.2)
    return False


def _wait_port_ready(host: str, port: int, max_s: float = 30.0) -> bool:
    """Poll every 0.2 s until the TCP port becomes reachable.

    Returns True if the port opened within max_s seconds.
    """
    deadline = time.monotonic() + max_s
    while time.monotonic() < deadline:
        if _port_open(host, port, timeout=0.5):
            return True
        time.sleep(0.2)
    return False


# ---------------------------------------------------------------------------
# Application-level probe helpers (SQL / HTTP — not just TCP port open)
# ---------------------------------------------------------------------------
def _mysql_ready(cfg, max_s: float = 20.0) -> bool:
    """Return True if MySQL accepts a real connection within max_s seconds."""
    deadline = time.monotonic() + max_s
    while time.monotonic() < deadline:
        try:
            conn = pymysql.connect(
                host=cfg.host, port=cfg.port,
                user=cfg.user, password=cfg.password,
                connect_timeout=2,
            )
            conn.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def _pg_ready(cfg, max_s: float = 20.0) -> bool:
    """Return True if PostgreSQL accepts a real connection within max_s seconds."""
    deadline = time.monotonic() + max_s
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(
                host=cfg.host, port=cfg.port,
                user=cfg.user, password=cfg.password,
                connect_timeout=2, dbname="postgres",
            )
            conn.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def _influx_ready(cfg, max_s: float = 20.0) -> bool:
    """Return True if InfluxDB /health returns HTTP 200 within max_s seconds."""
    deadline = time.monotonic() + max_s
    while time.monotonic() < deadline:
        try:
            r = requests.get(
                f"http://{cfg.host}:{cfg.port}/health",
                timeout=2,
            )
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------
class TestExtEnvLifecycle:
    """Lifecycle validation for ensure_ext_env.sh stop/kill/start operations.

    Each test uses ExtSrcEnv.{stop,kill,start}_*_instance() — the same
    interfaces used by the FQ integration tests — wrapped in try/finally
    so the service is always restored even on assertion failure.
    """

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()

    # -----------------------------------------------------------------------
    # MySQL
    # -----------------------------------------------------------------------
    def test_mysql_stop_start(self):
        """Graceful stop → verify port closed → start → verify SQL ready."""
        for cfg in ExtSrcEnv.mysql_version_configs():
            ver = cfg.version
            tdLog.info(f"[MySQL {ver}] stop/start lifecycle test")

            assert _mysql_ready(cfg, max_s=10), \
                f"MySQL {ver} must be reachable before stop"

            try:
                ExtSrcEnv.stop_mysql_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"MySQL {ver} port {cfg.port} still open after stop"
            finally:
                ExtSrcEnv.start_mysql_instance(ver)

            assert _mysql_ready(cfg, max_s=30), \
                f"MySQL {ver} not SQL-ready after start_mysql_instance"
            tdLog.info(f"[MySQL {ver}] stop/start PASSED")

    def test_mysql_kill_start(self):
        """SIGKILL → verify port closed → start → verify SQL ready."""
        for cfg in ExtSrcEnv.mysql_version_configs():
            ver = cfg.version
            tdLog.info(f"[MySQL {ver}] kill/start lifecycle test")

            assert _mysql_ready(cfg, max_s=10), \
                f"MySQL {ver} must be reachable before kill"

            try:
                ExtSrcEnv.kill_mysql_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"MySQL {ver} port {cfg.port} still open after kill"
            finally:
                ExtSrcEnv.start_mysql_instance(ver)

            assert _mysql_ready(cfg, max_s=30), \
                f"MySQL {ver} not SQL-ready after start_mysql_instance"
            tdLog.info(f"[MySQL {ver}] kill/start PASSED")

    def test_mysql_repeated_stop_start(self):
        """3 consecutive stop/start rounds — verify stability."""
        for cfg in ExtSrcEnv.mysql_version_configs():
            ver = cfg.version
            tdLog.info(f"[MySQL {ver}] repeated stop/start (3 rounds)")

            for i in range(3):
                try:
                    ExtSrcEnv.stop_mysql_instance(ver)
                    assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                        f"MySQL {ver} round {i + 1}: port still open after stop"
                finally:
                    ExtSrcEnv.start_mysql_instance(ver)
                assert _mysql_ready(cfg, max_s=30), \
                    f"MySQL {ver} round {i + 1}: not SQL-ready after restart"
                tdLog.info(f"[MySQL {ver}] round {i + 1}/3 PASSED")

    # -----------------------------------------------------------------------
    # PostgreSQL
    # -----------------------------------------------------------------------
    def test_pg_stop_start(self):
        """Graceful stop → verify port closed → start → verify SQL ready."""
        for cfg in ExtSrcEnv.pg_version_configs():
            ver = cfg.version
            tdLog.info(f"[PostgreSQL {ver}] stop/start lifecycle test")

            assert _pg_ready(cfg, max_s=10), \
                f"PostgreSQL {ver} must be reachable before stop"

            try:
                ExtSrcEnv.stop_pg_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"PostgreSQL {ver} port {cfg.port} still open after stop"
            finally:
                ExtSrcEnv.start_pg_instance(ver)

            assert _pg_ready(cfg, max_s=30), \
                f"PostgreSQL {ver} not SQL-ready after start_pg_instance"
            tdLog.info(f"[PostgreSQL {ver}] stop/start PASSED")

    def test_pg_kill_start(self):
        """SIGKILL → verify port closed → start → verify SQL ready."""
        for cfg in ExtSrcEnv.pg_version_configs():
            ver = cfg.version
            tdLog.info(f"[PostgreSQL {ver}] kill/start lifecycle test")

            assert _pg_ready(cfg, max_s=10), \
                f"PostgreSQL {ver} must be reachable before kill"

            try:
                ExtSrcEnv.kill_pg_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"PostgreSQL {ver} port {cfg.port} still open after kill"
            finally:
                ExtSrcEnv.start_pg_instance(ver)

            assert _pg_ready(cfg, max_s=30), \
                f"PostgreSQL {ver} not SQL-ready after start_pg_instance"
            tdLog.info(f"[PostgreSQL {ver}] kill/start PASSED")

    def test_pg_repeated_stop_start(self):
        """3 consecutive stop/start rounds — verify stability."""
        for cfg in ExtSrcEnv.pg_version_configs():
            ver = cfg.version
            tdLog.info(f"[PostgreSQL {ver}] repeated stop/start (3 rounds)")

            for i in range(3):
                try:
                    ExtSrcEnv.stop_pg_instance(ver)
                    assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                        f"PostgreSQL {ver} round {i + 1}: port still open after stop"
                finally:
                    ExtSrcEnv.start_pg_instance(ver)
                assert _pg_ready(cfg, max_s=30), \
                    f"PostgreSQL {ver} round {i + 1}: not SQL-ready after restart"
                tdLog.info(f"[PostgreSQL {ver}] round {i + 1}/3 PASSED")

    # -----------------------------------------------------------------------
    # InfluxDB
    # -----------------------------------------------------------------------
    def test_influx_stop_start(self):
        """Graceful stop → verify port closed → start → verify HTTP ready."""
        for cfg in ExtSrcEnv.influx_version_configs():
            ver = cfg.version
            tdLog.info(f"[InfluxDB {ver}] stop/start lifecycle test")

            assert _influx_ready(cfg, max_s=10), \
                f"InfluxDB {ver} must be reachable before stop"

            try:
                ExtSrcEnv.stop_influx_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"InfluxDB {ver} port {cfg.port} still open after stop"
            finally:
                ExtSrcEnv.start_influx_instance(ver)

            assert _influx_ready(cfg, max_s=30), \
                f"InfluxDB {ver} /health not ready after start_influx_instance"
            tdLog.info(f"[InfluxDB {ver}] stop/start PASSED")

    def test_influx_kill_start(self):
        """SIGKILL → verify port closed → start → verify HTTP ready."""
        for cfg in ExtSrcEnv.influx_version_configs():
            ver = cfg.version
            tdLog.info(f"[InfluxDB {ver}] kill/start lifecycle test")

            assert _influx_ready(cfg, max_s=10), \
                f"InfluxDB {ver} must be reachable before kill"

            try:
                ExtSrcEnv.kill_influx_instance(ver)
                assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                    f"InfluxDB {ver} port {cfg.port} still open after kill"
            finally:
                ExtSrcEnv.start_influx_instance(ver)

            assert _influx_ready(cfg, max_s=30), \
                f"InfluxDB {ver} /health not ready after start_influx_instance"
            tdLog.info(f"[InfluxDB {ver}] kill/start PASSED")

    def test_influx_repeated_stop_start(self):
        """3 consecutive stop/start rounds — verify stability."""
        for cfg in ExtSrcEnv.influx_version_configs():
            ver = cfg.version
            tdLog.info(f"[InfluxDB {ver}] repeated stop/start (3 rounds)")

            for i in range(3):
                try:
                    ExtSrcEnv.stop_influx_instance(ver)
                    assert _wait_port_gone(cfg.host, cfg.port, max_s=15), \
                        f"InfluxDB {ver} round {i + 1}: port still open after stop"
                finally:
                    ExtSrcEnv.start_influx_instance(ver)
                assert _influx_ready(cfg, max_s=30), \
                    f"InfluxDB {ver} round {i + 1}: /health not ready after restart"
                tdLog.info(f"[InfluxDB {ver}] round {i + 1}/3 PASSED")
