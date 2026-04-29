import os
import re
import pytest
from collections import namedtuple
from itertools import zip_longest

from new_test_framework.utils import tdLog, tdSql, tdCom


# =====================================================================
# Dynamic error code loader — parses taoserror.h at import time
#
# Instead of hardcoding hex values that drift when the source changes,
# we read the authoritative header file and resolve every TSDB_CODE_*
# to its current integer value.  Codes not yet defined in the header
# (e.g. enterprise-only codes that haven't shipped) resolve to None,
# which causes tdSql.error() to check only that *some* error occurs.
# =====================================================================

def _parse_taoserror_header():
    """Parse taoserror.h and return {name: int_value} for all TSDB_CODE_* macros."""
    # Locate taoserror.h relative to this file:
    #   .../community/test/cases/09-DataQuerying/19-FederatedQuery/ → 4 levels up → community/
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(_this_dir, '..', '..', '..', '..', 'include', 'util', 'taoserror.h'),
    ]
    env_path = os.environ.get('TAOSERROR_HEADER')
    if env_path:
        candidates.insert(0, env_path)

    for candidate in candidates:
        path = os.path.normpath(candidate)
        if os.path.isfile(path):
            return _do_parse(path)
    return {}


# =====================================================================
# Diagnostic helpers — produce human-readable failure messages
# =====================================================================

def _fmt_result_table(actual_rows, expected_rows):
    """Format actual vs expected query results as a side-by-side text table.

    Returns a multi-line string suitable for embedding in AssertionError
    messages so the developer can see at a glance which cells diverge.

    Args:
        actual_rows:   Iterable of tuples (from tdSql.queryResult).
        expected_rows: Iterable of iterables (test-specified expected values).

    Returns:
        str: A formatted table string, prefixed with two newlines.
    """
    actual   = [tuple(r) for r in actual_rows]
    expected = [tuple(r) for r in expected_rows]
    max_rows = max(len(actual), len(expected), 1)
    max_cols = max(
        (len(r) for r in actual),
        default=max((len(r) for r in expected), default=0),
    )

    lines = ["  actual vs expected:"]
    for r in range(max_rows):
        arow = actual[r]   if r < len(actual)   else ()
        erow = expected[r] if r < len(expected) else ()
        cells = []
        for c in range(max_cols):
            av = arow[c]   if c < len(arow) else "<missing>"
            ev = erow[c]   if c < len(erow) else "<missing>"
            mark = "" if av == ev else " ✗"
            cells.append(f"col{c}={av!r}(exp={ev!r}){mark}")
        lines.append(f"  row{r}: " + ", ".join(cells))
    return "\n".join(lines)


# =====================================================================
# Diagnostic helpers — produce human-readable failure messages
# =====================================================================

def _fmt_result_table(actual_rows, expected_rows):
    """Format actual vs expected query results as a side-by-side text table.

    Returns a multi-line string suitable for embedding in AssertionError
    messages so the developer can see at a glance which cells diverge.

    Args:
        actual_rows:   Iterable of tuples (from tdSql.queryResult).
        expected_rows: Iterable of iterables (test-specified expected values).

    Returns:
        str: A formatted table string, prefixed with two newlines.
    """
    actual   = [tuple(r) for r in actual_rows]
    expected = [tuple(r) for r in expected_rows]
    max_rows = max(len(actual), len(expected), 1)
    max_cols = max(
        (len(r) for r in actual),
        default=max((len(r) for r in expected), default=0),
    )

    lines = ["  actual vs expected:"]
    for r in range(max_rows):
        arow = actual[r]   if r < len(actual)   else ()
        erow = expected[r] if r < len(expected) else ()
        cells = []
        for c in range(max_cols):
            av = arow[c]   if c < len(arow) else "<missing>"
            ev = erow[c]   if c < len(erow) else "<missing>"
            mark = "" if av == ev else " ✗"
            cells.append(f"col{c}={av!r}(exp={ev!r}){mark}")
        lines.append(f"  row{r}: " + ", ".join(cells))
    return "\n".join(lines)


def _do_parse(path):
    """Parse a single taoserror.h and extract all TSDB_CODE_* defines."""
    codes = {}
    # Matches:  #define TSDB_CODE_XXX  TAOS_DEF_ERROR_CODE(mod, 0xHEX)  // optional comment
    pattern = re.compile(
        r'#define\s+(TSDB_CODE_\w+)\s+TAOS_DEF_ERROR_CODE\s*\(\s*(\d+)\s*,\s*0x([0-9a-fA-F]+)\s*\)'
    )
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            m = pattern.search(line)
            if m:
                name = m.group(1)
                mod = int(m.group(2))
                code = int(m.group(3), 16)
                codes[name] = int(0x80000000 | (mod << 16) | code)
    return codes


_ERROR_CODES = _parse_taoserror_header()


def _code(name):
    """Resolve a TSDB_CODE_* name to its integer value, or None if not yet defined."""
    return _ERROR_CODES.get(name)


# === Error codes — resolved dynamically from taoserror.h =============
# If a code is not yet in the header (e.g. unreleased enterprise codes),
# the value will be None and tdSql.error() only checks that an error occurs.

# --- Standard community codes ---
TSDB_CODE_PAR_SYNTAX_ERROR                     = _code('TSDB_CODE_PAR_SYNTAX_ERROR')
TSDB_CODE_PAR_TABLE_NOT_EXIST                  = _code('TSDB_CODE_PAR_TABLE_NOT_EXIST')
TSDB_CODE_PAR_INVALID_REF_COLUMN               = _code('TSDB_CODE_PAR_INVALID_REF_COLUMN')
TSDB_CODE_MND_DB_NOT_EXIST                     = _code('TSDB_CODE_MND_DB_NOT_EXIST')
TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH          = _code('TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH')

# --- External Source Management (enterprise) ---
TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS   = _code('TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS')
TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST        = _code('TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST')
TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT    = _code('TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT')
TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED = _code('TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED')
TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT             = _code('TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT')
TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG          = _code('TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG')

# --- Path resolution / type mapping / pushdown ---
TSDB_CODE_EXT_SOURCE_NOT_FOUND                 = _code('TSDB_CODE_EXT_SOURCE_NOT_FOUND')
TSDB_CODE_EXT_DB_NOT_EXIST                     = _code('TSDB_CODE_EXT_DB_NOT_EXIST')
TSDB_CODE_EXT_DEFAULT_NS_MISSING               = _code('TSDB_CODE_EXT_DEFAULT_NS_MISSING')
TSDB_CODE_EXT_INVALID_PATH                     = _code('TSDB_CODE_EXT_INVALID_PATH')
TSDB_CODE_EXT_TYPE_NOT_MAPPABLE                = _code('TSDB_CODE_EXT_TYPE_NOT_MAPPABLE')
TSDB_CODE_EXT_NO_TS_KEY                        = _code('TSDB_CODE_EXT_NO_TS_KEY')
TSDB_CODE_EXT_SYNTAX_UNSUPPORTED               = _code('TSDB_CODE_EXT_SYNTAX_UNSUPPORTED')
TSDB_CODE_EXT_TABLE_NOT_EXIST                  = _code('TSDB_CODE_EXT_TABLE_NOT_EXIST')
TSDB_CODE_EXT_PUSHDOWN_FAILED                  = _code('TSDB_CODE_EXT_PUSHDOWN_FAILED')
TSDB_CODE_EXT_SOURCE_UNAVAILABLE               = _code('TSDB_CODE_EXT_SOURCE_UNAVAILABLE')
TSDB_CODE_EXT_RESOURCE_EXHAUSTED               = _code('TSDB_CODE_EXT_RESOURCE_EXHAUSTED')
TSDB_CODE_EXT_WRITE_DENIED                     = _code('TSDB_CODE_EXT_WRITE_DENIED')
TSDB_CODE_EXT_STREAM_NOT_SUPPORTED             = _code('TSDB_CODE_EXT_STREAM_NOT_SUPPORTED')
TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED          = _code('TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED')

# --- VTable DDL ---
TSDB_CODE_FOREIGN_SERVER_NOT_EXIST             = _code('TSDB_CODE_FOREIGN_SERVER_NOT_EXIST')
TSDB_CODE_FOREIGN_DB_NOT_EXIST                 = _code('TSDB_CODE_FOREIGN_DB_NOT_EXIST')
TSDB_CODE_FOREIGN_TABLE_NOT_EXIST              = _code('TSDB_CODE_FOREIGN_TABLE_NOT_EXIST')
TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST             = _code('TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST')
TSDB_CODE_FOREIGN_TYPE_MISMATCH                = _code('TSDB_CODE_FOREIGN_TYPE_MISMATCH')
TSDB_CODE_FOREIGN_NO_TS_KEY                    = _code('TSDB_CODE_FOREIGN_NO_TS_KEY')

# --- System / feature toggle ---
TSDB_CODE_EXT_CONFIG_PARAM_INVALID             = _code('TSDB_CODE_EXT_CONFIG_PARAM_INVALID')
TSDB_CODE_EXT_FEATURE_DISABLED                 = _code('TSDB_CODE_EXT_FEATURE_DISABLED')


# =====================================================================
# TLS certificate paths
#
# Certificates are generated by ensure_ext_env.sh into FQ_CERT_DIR
# (default: /opt/taostest/fq/certs).  All paths here must match what
# the script writes so test cases can reference them directly.
#
# Layout:
#   FQ_CERT_DIR/
#     ca.pem                — shared CA cert
#     mysql/
#       ca.pem  (symlink)   — CA cert (also accessible via FQ_CA_CERT)
#       server.pem          — MySQL server cert
#       server-key.pem      — MySQL server private key
#       client.pem          — client cert for mTLS
#       client-key.pem      — client private key for mTLS
#     pg/
#       ca.pem  (symlink)
#       server.pem
#       server.key          — PG requires the file be named .key and mode 600
#       client.pem
#       client-key.pem
# =====================================================================

_FQ_CERT_DIR = os.getenv(
    "FQ_CERT_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs"),
)

# Shared
FQ_CA_CERT        = os.path.join(_FQ_CERT_DIR, "ca.pem")

# MySQL TLS files
FQ_MYSQL_CA_CERT        = os.path.join(_FQ_CERT_DIR, "mysql", "ca.pem")
FQ_MYSQL_SERVER_CERT    = os.path.join(_FQ_CERT_DIR, "mysql", "server.pem")
FQ_MYSQL_SERVER_KEY     = os.path.join(_FQ_CERT_DIR, "mysql", "server-key.pem")
FQ_MYSQL_CLIENT_CERT    = os.path.join(_FQ_CERT_DIR, "mysql", "client.pem")
FQ_MYSQL_CLIENT_KEY     = os.path.join(_FQ_CERT_DIR, "mysql", "client-key.pem")

# PostgreSQL TLS files
FQ_PG_CA_CERT           = os.path.join(_FQ_CERT_DIR, "pg", "ca.pem")
FQ_PG_SERVER_CERT       = os.path.join(_FQ_CERT_DIR, "pg", "server.pem")
FQ_PG_SERVER_KEY        = os.path.join(_FQ_CERT_DIR, "pg", "server.key")
FQ_PG_CLIENT_CERT       = os.path.join(_FQ_CERT_DIR, "pg", "client.pem")
FQ_PG_CLIENT_KEY        = os.path.join(_FQ_CERT_DIR, "pg", "client-key.pem")


# =====================================================================
# Version-configuration namedtuples used by ExtSrcEnv.*_version_configs
# and by tests that iterate over multiple database versions.
# =====================================================================

_MySQLVerCfg  = namedtuple("_MySQLVerCfg",  ["version", "host", "port", "user", "password"])
_PGVerCfg     = namedtuple("_PGVerCfg",     ["version", "host", "port", "user", "password"])
_InfluxVerCfg = namedtuple("_InfluxVerCfg", ["version", "host", "port", "token", "org"])


# =====================================================================
# External source direct-connection helpers
# =====================================================================

class ExtSrcEnv:
    """Direct connections to external databases for test data setup/teardown.

    Connection parameters are configurable via environment variables.
    Each test case uses these helpers to prepare test data in the real
    external source BEFORE querying via TDengine federated query.
    """

    # ------------------------------------------------------------------
    # Version lists — override via comma-separated env vars.
    # Default: one reference version per engine.
    # Supported:  MySQL 5.7/8.x | PostgreSQL 14+  | InfluxDB 3.x
    # CI-tested:  MySQL 5.7/8.0/8.4 | pg 14/15/16/17 | InfluxDB 3.0/3.5
    #   FQ_MYSQL_VERSIONS   e.g. "5.7,8.0,8.4"   (default "8.0")
    #   FQ_PG_VERSIONS      e.g. "14,15,16,17"   (default "16")
    #   FQ_INFLUX_VERSIONS  e.g. "3.0,3.5"       (default "3.0")
    # ------------------------------------------------------------------
    MYSQL_VERSIONS  = [v.strip() for v in
                       os.getenv("FQ_MYSQL_VERSIONS", "8.0").split(",")
                       if v.strip()]
    PG_VERSIONS     = [v.strip() for v in
                       os.getenv("FQ_PG_VERSIONS", "16").split(",")
                       if v.strip()]
    INFLUX_VERSIONS = [v.strip() for v in
                       os.getenv("FQ_INFLUX_VERSIONS", "3.0").split(",")
                       if v.strip()]

    # Per-version port assignments — non-default, test-dedicated ports so
    # multiple versions can run simultaneously alongside any production instance.
    # Override individually via FQ_*_PORT_<ver-without-dots> env vars.
    _MYSQL_VERSION_PORTS = {
        "5.7": int(os.getenv("FQ_MYSQL_PORT_57", "13305")),
        "8.0": int(os.getenv("FQ_MYSQL_PORT_80", "13306")),
        "8.4": int(os.getenv("FQ_MYSQL_PORT_84", "13307")),
    }
    _PG_VERSION_PORTS = {
        "14":  int(os.getenv("FQ_PG_PORT_14", "15433")),
        "15":  int(os.getenv("FQ_PG_PORT_15", "15435")),
        "16":  int(os.getenv("FQ_PG_PORT_16", "15434")),
        "17":  int(os.getenv("FQ_PG_PORT_17", "15436")),
    }
    _INFLUX_VERSION_PORTS = {
        "3.0": int(os.getenv("FQ_INFLUX_PORT_30", "18086")),
        "3.5": int(os.getenv("FQ_INFLUX_PORT_35", "18087")),
    }

    # ------------------------------------------------------------------
    # Primary connection params — derived from the first configured version.
    # All existing helpers (mysql_exec, pg_exec, …) continue to work
    # unchanged and target this primary version.
    # ------------------------------------------------------------------
    MYSQL_HOST = os.getenv("FQ_MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = _MYSQL_VERSION_PORTS.get(
        MYSQL_VERSIONS[0], int(os.getenv("FQ_MYSQL_PORT", "13306")))
    MYSQL_USER = os.getenv("FQ_MYSQL_USER", "root")
    MYSQL_PASS = os.getenv("FQ_MYSQL_PASS", "taosdata")

    PG_HOST    = os.getenv("FQ_PG_HOST", "127.0.0.1")
    PG_PORT    = _PG_VERSION_PORTS.get(
        PG_VERSIONS[0], int(os.getenv("FQ_PG_PORT", "15434")))
    PG_USER    = os.getenv("FQ_PG_USER", "postgres")
    PG_PASS    = os.getenv("FQ_PG_PASS", "taosdata")

    INFLUX_HOST  = os.getenv("FQ_INFLUX_HOST",  "127.0.0.1")
    INFLUX_PORT  = _INFLUX_VERSION_PORTS.get(
        INFLUX_VERSIONS[0], int(os.getenv("FQ_INFLUX_PORT", "18086")))
    INFLUX_TOKEN = os.getenv("FQ_INFLUX_TOKEN", "test-token")
    INFLUX_ORG   = os.getenv("FQ_INFLUX_ORG",   "test-org")

    # Pool-exhaustion test user — created by ensure_ext_env.sh with
    # MAX_USER_CONNECTIONS limited to FQ_POOL_TEST_MAX_CONN (default 1).
    # Tests use this user to saturate the per-user connection limit and
    # trigger TSDB_CODE_EXT_RESOURCE_EXHAUSTED.
    POOL_TEST_USER     = os.getenv("FQ_POOL_TEST_USER",     "fq_pool_test")
    POOL_TEST_PASS     = os.getenv("FQ_POOL_TEST_PASS",     "taosdata")
    POOL_TEST_MAX_CONN = int(os.getenv("FQ_POOL_TEST_MAX_CONN", "1"))

    _env_checked = False

    @classmethod
    def ensure_env(cls):
        """Start and verify all external test databases.

        Step 1 — run ensure_ext_env.sh (Linux/macOS) or ensure_ext_env.ps1
        (Windows) — idempotent — with the configured version lists passed as
        env vars so the script can start the correct per-version instances on
        their dedicated non-default ports.

        Step 2 — probe every configured version for connectivity so any
        startup failure is reported with a clear error rather than a cryptic
        connection refusal later inside a test.

        Call once per process from setup_class.
        Raises RuntimeError (not pytest.skip) so failures are clearly visible.
        """
        if cls._env_checked:
            return

        # ------------------------------------------------------------------
        # Step 1: run platform-appropriate setup script
        # ------------------------------------------------------------------
        import subprocess
        import sys

        here = os.path.dirname(os.path.abspath(__file__))
        env = os.environ.copy()
        env["FQ_MYSQL_VERSIONS"]  = ",".join(cls.MYSQL_VERSIONS)
        env["FQ_PG_VERSIONS"]     = ",".join(cls.PG_VERSIONS)
        env["FQ_INFLUX_VERSIONS"] = ",".join(cls.INFLUX_VERSIONS)

        if sys.platform == "win32":
            ps1 = os.path.join(here, "ensure_ext_env.ps1")
            if os.path.exists(ps1):
                cmd = [
                    "powershell.exe",
                    "-ExecutionPolicy", "Bypass",
                    "-NoProfile",
                    "-File", ps1,
                ]
                ret = subprocess.call(cmd, env=env)
                if ret != 0:
                    raise RuntimeError(
                        f"ensure_ext_env.ps1 failed (exit={ret}). "
                        f"Check that MySQL/PG/InfluxDB test instances can start.")
        else:
            sh = os.path.join(here, "ensure_ext_env.sh")
            if os.path.exists(sh):
                ret = subprocess.call(["bash", sh], env=env)
                if ret != 0:
                    raise RuntimeError(
                        f"ensure_ext_env.sh failed (exit={ret}). "
                        f"Check that MySQL/PG/InfluxDB test instances can start.")

        # ------------------------------------------------------------------
        # Step 2: connectivity probe — verify every configured version
        # ------------------------------------------------------------------
        errors = []

        # --- MySQL (all configured versions) ---
        import pymysql
        for cfg in cls.mysql_version_configs():
            try:
                conn = pymysql.connect(
                    host=cfg.host, port=cfg.port,
                    user=cfg.user, password=cfg.password,
                    connect_timeout=5, autocommit=True)
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.close()
            except Exception as e:
                errors.append(
                    f"  MySQL {cfg.version} @ {cfg.host}:{cfg.port} — {e}")

        # --- PostgreSQL (all configured versions) ---
        import psycopg2
        for cfg in cls.pg_version_configs():
            try:
                conn = psycopg2.connect(
                    host=cfg.host, port=cfg.port,
                    user=cfg.user, password=cfg.password,
                    dbname="postgres", connect_timeout=5)
                conn.close()
            except Exception as e:
                errors.append(
                    f"  PostgreSQL {cfg.version} @ {cfg.host}:{cfg.port} — {e}")

        # --- InfluxDB (all configured versions) ---
        import requests
        for cfg in cls.influx_version_configs():
            try:
                r = requests.get(
                    f"http://{cfg.host}:{cfg.port}/health",
                    timeout=5)
                if r.status_code not in (200, 204):
                    errors.append(
                        f"  InfluxDB {cfg.version} @ {cfg.host}:{cfg.port} — "
                        f"health endpoint returned HTTP {r.status_code}")
            except Exception as e:
                errors.append(
                    f"  InfluxDB {cfg.version} @ {cfg.host}:{cfg.port} — {e}")

        if errors:
            raise RuntimeError(
                "External test databases not reachable after ensure_ext_env.sh.\n"
                "(Override hosts/ports via FQ_MYSQL_HOST/FQ_PG_HOST/"
                "FQ_INFLUX_HOST env vars)\n"
                + "\n".join(errors))

        cls._env_checked = True

    # ---- Version iteration helpers ----

    @classmethod
    def mysql_version_configs(cls):
        """Yield one _MySQLVerCfg per configured MySQL version."""
        for ver in cls.MYSQL_VERSIONS:
            port = cls._MYSQL_VERSION_PORTS.get(ver, cls.MYSQL_PORT)
            yield _MySQLVerCfg(ver, cls.MYSQL_HOST, port,
                               cls.MYSQL_USER, cls.MYSQL_PASS)

    @classmethod
    def pg_version_configs(cls):
        """Yield one _PGVerCfg per configured PostgreSQL version."""
        for ver in cls.PG_VERSIONS:
            port = cls._PG_VERSION_PORTS.get(ver, cls.PG_PORT)
            yield _PGVerCfg(ver, cls.PG_HOST, port,
                            cls.PG_USER, cls.PG_PASS)

    @classmethod
    def influx_version_configs(cls):
        """Yield one _InfluxVerCfg per configured InfluxDB version."""
        for ver in cls.INFLUX_VERSIONS:
            port = cls._INFLUX_VERSION_PORTS.get(ver, cls.INFLUX_PORT)
            yield _InfluxVerCfg(ver, cls.INFLUX_HOST, port,
                                cls.INFLUX_TOKEN, cls.INFLUX_ORG)

    # ---- Container lifecycle helpers (for unreachability tests) ----
    #
    # Container names are resolved via env vars with sensible defaults:
    #   FQ_MYSQL_CONTAINER_57  (default: fq-mysql-5.7)
    #   FQ_MYSQL_CONTAINER_80  (default: fq-mysql-8.0)
    #   FQ_MYSQL_CONTAINER_84  (default: fq-mysql-8.4)
    #   FQ_PG_CONTAINER_14     (default: fq-pg-14)   etc.
    #   FQ_PG_CONTAINER_15     (default: fq-pg-15)
    #   FQ_PG_CONTAINER_16     (default: fq-pg-16)
    #   FQ_PG_CONTAINER_17     (default: fq-pg-17)
    #   FQ_INFLUX_CONTAINER_30 (default: fq-influx-3.0)
    #   FQ_INFLUX_CONTAINER_35 (default: fq-influx-3.5)
    #
    # Tests that need to stop/start a real instance call these helpers and
    # wrap the body with try/finally to guarantee the instance is restarted.

    @classmethod
    def _mysql_container_name(cls, ver):
        tag = ver.replace(".", "")
        return os.getenv(f"FQ_MYSQL_CONTAINER_{tag}", f"fq-mysql-{ver}")

    @classmethod
    def _pg_container_name(cls, ver):
        tag = ver.replace(".", "")
        return os.getenv(f"FQ_PG_CONTAINER_{tag}", f"fq-pg-{ver}")

    @classmethod
    def _influx_container_name(cls, ver):
        tag = ver.replace(".", "")
        return os.getenv(f"FQ_INFLUX_CONTAINER_{tag}", f"fq-influx-{ver}")

    @classmethod
    def _docker_container_running(cls, container_name):
        """Return True iff docker is available and the named container is running."""
        import subprocess, shutil
        if not shutil.which("docker"):
            return False
        try:
            r = subprocess.run(
                ["docker", "inspect", "--format={{.State.Running}}", container_name],
                capture_output=True, text=True, timeout=10,
            )
            return r.returncode == 0 and r.stdout.strip() == "true"
        except Exception:
            return False

    @classmethod
    def _docker_container_exists(cls, container_name):
        """Return True iff docker is available and the named container exists (any state)."""
        import subprocess, shutil
        if not shutil.which("docker"):
            return False
        try:
            r = subprocess.run(
                ["docker", "inspect", "--format={{.State.Status}}", container_name],
                capture_output=True, text=True, timeout=10,
            )
            return r.returncode == 0
        except Exception:
            return False

    @classmethod
    def _kill_process_by_pidfile(cls, pidfile, wait_s=30):
        """SIGTERM a process identified by pidfile; SIGKILL if it lingers."""
        import os, signal, time
        with open(pidfile) as _pf:
            pid = int(_pf.read().strip())
        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + wait_s
        while time.time() < deadline:
            try:
                os.kill(pid, 0)
                time.sleep(0.3)
            except ProcessLookupError:
                return
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    @classmethod
    def stop_mysql_instance(cls, ver):
        """Stop the MySQL instance for the given version.

        Supports both Docker-based and bare-metal deployments.
        After this call the MySQL port for 'ver' is unreachable.
        Always pair with start_mysql_instance() in a try/finally block.
        """
        import subprocess
        container = cls._mysql_container_name(ver)
        if cls._docker_container_running(container):
            subprocess.run(["docker", "stop", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: kill mysqld via its pidfile.
            fq_base = os.getenv("FQ_BASE_DIR", "/opt/taostest/fq")
            pidfile = os.path.join(fq_base, "mysql", ver, "run", "mysqld.pid")
            cls._kill_process_by_pidfile(pidfile)

    @classmethod
    def start_mysql_instance(cls, ver, wait_s=30):
        """Start the MySQL instance for the given version and wait until ready.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess, time
        container = cls._mysql_container_name(ver)
        if cls._docker_container_exists(container):
            subprocess.run(["docker", "start", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: restart via ensure_ext_env.sh which handles
            # "installed but stopped" and is fully idempotent.
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "ensure_ext_env.sh")
            subprocess.run(["bash", script],
                           check=True, capture_output=False, timeout=120)
        # Wait for the port to become accepting connections
        cfg = next(c for c in cls.mysql_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        import pymysql
        while time.time() < deadline:
            try:
                conn = pymysql.connect(host=cfg.host, port=cfg.port,
                                       user=cfg.user, password=cfg.password,
                                       connect_timeout=2)
                conn.close()
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError(
            f"MySQL {ver} did not become ready within {wait_s}s")

    @classmethod
    def stop_pg_instance(cls, ver):
        """Stop the PostgreSQL instance for the given version.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess
        container = cls._pg_container_name(ver)
        if cls._docker_container_running(container):
            subprocess.run(["docker", "stop", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: stop via pg_ctl.
            fq_base = os.getenv("FQ_BASE_DIR", "/opt/taostest/fq")
            datadir = os.path.join(fq_base, "pg", ver, "data")
            subprocess.run(["pg_ctl", "stop", "-D", datadir, "-m", "fast"],
                           check=True, capture_output=True, timeout=30)

    @classmethod
    def start_pg_instance(cls, ver, wait_s=10):
        """Start the PostgreSQL instance for the given version and wait until ready.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess, time
        container = cls._pg_container_name(ver)
        if cls._docker_container_exists(container):
            subprocess.run(["docker", "start", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: start via ensure_ext_env.sh.
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "ensure_ext_env.sh")
            subprocess.run(["bash", script],
                           check=True, capture_output=False, timeout=120)
        cfg = next(c for c in cls.pg_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        import psycopg2
        while time.time() < deadline:
            try:
                conn = psycopg2.connect(host=cfg.host, port=cfg.port,
                                        user=cfg.user, password=cfg.password,
                                        connect_timeout=2)
                conn.close()
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError(
            f"PostgreSQL {ver} did not become ready within {wait_s}s")

    @classmethod
    def stop_influx_instance(cls, ver):
        """Stop the InfluxDB instance for the given version.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess
        container = cls._influx_container_name(ver)
        if cls._docker_container_running(container):
            subprocess.run(["docker", "stop", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: kill influxd via its pidfile.
            fq_base = os.getenv("FQ_BASE_DIR", "/opt/taostest/fq")
            pidfile = os.path.join(fq_base, "influxdb", ver, "influxd.pid")
            cls._kill_process_by_pidfile(pidfile)

    @classmethod
    def start_influx_instance(cls, ver, wait_s=10):
        """Start the InfluxDB instance for the given version and wait until ready.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess, time, requests
        container = cls._influx_container_name(ver)
        if cls._docker_container_exists(container):
            subprocess.run(["docker", "start", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: start via ensure_ext_env.sh.
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "ensure_ext_env.sh")
            subprocess.run(["bash", script],
                           check=True, capture_output=False, timeout=120)
        cfg = next(c for c in cls.influx_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        while time.time() < deadline:
            try:
                r = requests.get(f"http://{cfg.host}:{cfg.port}/health",
                                 timeout=2)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"InfluxDB {ver} did not become ready within {wait_s}s")

    @classmethod
    def stop_influx_instance(cls, ver):
        """Stop the InfluxDB instance for the given version.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess
        container = cls._influx_container_name(ver)
        if cls._docker_container_running(container):
            subprocess.run(["docker", "stop", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: kill influxd via its pidfile.
            fq_base = os.getenv("FQ_BASE_DIR", "/opt/taostest/fq")
            pidfile = os.path.join(fq_base, "influxdb", ver, "influxd.pid")
            cls._kill_process_by_pidfile(pidfile)

    @classmethod
    def start_influx_instance(cls, ver, wait_s=10):
        """Start the InfluxDB instance for the given version and wait until ready.

        Supports both Docker-based and bare-metal deployments.
        """
        import subprocess, time, requests
        container = cls._influx_container_name(ver)
        if cls._docker_container_exists(container):
            subprocess.run(["docker", "start", container],
                           check=True, capture_output=True, timeout=30)
        else:
            # Bare-metal: start via ensure_ext_env.sh.
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "ensure_ext_env.sh")
            subprocess.run(["bash", script],
                           check=True, capture_output=False, timeout=120)
        cfg = next(c for c in cls.influx_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        while time.time() < deadline:
            try:
                r = requests.get(f"http://{cfg.host}:{cfg.port}/health",
                                 timeout=2)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"InfluxDB {ver} did not become ready within {wait_s}s")

    # ---- Network delay injection (for timeout/latency tests) ----
    #
    # Uses Linux tc(8) netem to add outgoing delay on the loopback interface.
    # Requires: iproute2 installed and CAP_NET_ADMIN (or root).
    # The delay applies globally to loopback, so tests using this must be
    # run serially and must always call clear_net_delay() in finally blocks.
    #
    # Alternative: override FQ_NETEM_IFACE to target a specific interface.

    _NETEM_IFACE = os.getenv("FQ_NETEM_IFACE", "lo")

    @classmethod
    def inject_net_delay(cls, delay_ms, jitter_ms=0):
        """Add tc netem delay on loopback (or FQ_NETEM_IFACE).

        Example: inject_net_delay(200) → every outgoing packet delayed 200ms.
        Always call clear_net_delay() in a finally block.
        """
        import subprocess
        iface = cls._NETEM_IFACE
        # Remove any existing qdisc first (ignore error if none exists)
        subprocess.run(["tc", "qdisc", "del", "dev", iface, "root"],
                       capture_output=True)
        netem_args = ["tc", "qdisc", "add", "dev", iface, "root",
                      "netem", "delay", f"{delay_ms}ms"]
        if jitter_ms:
            netem_args += [f"{jitter_ms}ms"]
        subprocess.run(netem_args, check=True, capture_output=True)

    @classmethod
    def clear_net_delay(cls):
        """Remove tc netem delay added by inject_net_delay()."""
        import subprocess
        iface = cls._NETEM_IFACE
        subprocess.run(["tc", "qdisc", "del", "dev", iface, "root"],
                       capture_output=True)  # ignore error if already absent

    # ---- Version combo helpers (used by FederatedQueryVersionedMixin) ----

    @classmethod
    def _version_combos(cls):
        """Return list of (mysql_ver, pg_ver, influx_ver) tuples for pytest parametrize.

        Uses zip_longest over the three configured version lists so that all
        versions of the longest list get covered; shorter lists are padded with
        their last element.  When only default single versions are configured
        this returns exactly one tuple — same behavior as before.
        """
        raw = list(zip_longest(cls.MYSQL_VERSIONS, cls.PG_VERSIONS, cls.INFLUX_VERSIONS))
        return [
            (m or cls.MYSQL_VERSIONS[-1],
             p or cls.PG_VERSIONS[-1],
             i or cls.INFLUX_VERSIONS[-1])
            for m, p, i in raw
        ]

    @classmethod
    def _version_combo_ids(cls):
        """Human-readable pytest IDs for version combos."""
        return [f"my{m}-pg{p}-inf{i}" for m, p, i in cls._version_combos()]

    # ---- MySQL helpers ----

    @classmethod
    def mysql_exec(cls, database, sqls):
        """Execute SQL statements on MySQL. database=None for server-level."""
        import pymysql
        conn = pymysql.connect(
            host=cls.MYSQL_HOST, port=cls.MYSQL_PORT,
            user=cls.MYSQL_USER, password=cls.MYSQL_PASS,
            database=database, autocommit=True, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def mysql_query(cls, database, sql):
        """Query MySQL, return list of row-tuples."""
        import pymysql
        conn = pymysql.connect(
            host=cls.MYSQL_HOST, port=cls.MYSQL_PORT,
            user=cls.MYSQL_USER, password=cls.MYSQL_PASS,
            database=database, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    @classmethod
    def mysql_open_connection(cls, user=None, password=None, database=None):
        """Open and return a raw pymysql connection (caller must close it).

        Used by pool-exhaustion tests to hold a connection open while a
        TDengine federated query is issued, thereby saturating the per-user
        connection limit and triggering TSDB_CODE_EXT_RESOURCE_EXHAUSTED.
        """
        import pymysql
        return pymysql.connect(
            host=cls.MYSQL_HOST, port=cls.MYSQL_PORT,
            user=user if user is not None else cls.MYSQL_USER,
            password=password if password is not None else cls.MYSQL_PASS,
            database=database, autocommit=True, charset="utf8mb4")

    @classmethod
    def mysql_create_db(cls, db):
        """Create MySQL database (idempotent)."""
        cls.mysql_exec(None, [
            f"CREATE DATABASE IF NOT EXISTS `{db}` "
            f"CHARACTER SET utf8mb4"])

    @classmethod
    def mysql_drop_db(cls, db):
        """Drop MySQL database (idempotent)."""
        cls.mysql_exec(None, [f"DROP DATABASE IF EXISTS `{db}`"])

    @classmethod
    def mysql_query_cfg(cls, cfg, database, sql):
        """Query a specific MySQL version instance, return the first column of the first row."""
        import pymysql
        conn = pymysql.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            database=database, autocommit=True, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()

    @classmethod
    def mysql_exec_cfg(cls, cfg, database, sqls):
        """Execute SQL on a specific MySQL version instance."""
        import pymysql
        conn = pymysql.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            database=database, autocommit=True, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def mysql_create_db_cfg(cls, cfg, db):
        """Create MySQL database on a specific version instance (idempotent)."""
        cls.mysql_exec_cfg(cfg, None, [
            f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4"])

    @classmethod
    def mysql_drop_db_cfg(cls, cfg, db):
        """Drop MySQL database on a specific version instance (idempotent)."""
        cls.mysql_exec_cfg(cfg, None, [f"DROP DATABASE IF EXISTS `{db}`"])

    # ---- PostgreSQL helpers ----

    @classmethod
    def pg_exec(cls, database, sqls):
        """Execute SQL statements on PG. database=None uses 'postgres'."""
        import psycopg2
        conn = psycopg2.connect(
            host=cls.PG_HOST, port=cls.PG_PORT,
            user=cls.PG_USER, password=cls.PG_PASS,
            dbname=database or "postgres")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def pg_query(cls, database, sql):
        """Query PG, return list of row-tuples."""
        import psycopg2
        conn = psycopg2.connect(
            host=cls.PG_HOST, port=cls.PG_PORT,
            user=cls.PG_USER, password=cls.PG_PASS,
            dbname=database or "postgres")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    @classmethod
    def pg_create_db(cls, db):
        """Create PG database (idempotent)."""
        rows = cls.pg_query(
            "postgres",
            f"SELECT 1 FROM pg_database WHERE datname='{db}'")
        if not rows:
            cls.pg_exec("postgres", [f'CREATE DATABASE "{db}"'])

    @classmethod
    def pg_drop_db(cls, db):
        """Drop PG database — terminates active connections first."""
        cls.pg_exec("postgres", [
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname='{db}' AND pid <> pg_backend_pid()",
            f'DROP DATABASE IF EXISTS "{db}"',
        ])

    @classmethod
    def pg_exec_cfg(cls, cfg, database, sqls):
        """Execute SQL on a specific PostgreSQL version instance."""
        import psycopg2
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            dbname=database or "postgres")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def pg_query_cfg(cls, cfg, database, sql):
        """Query a specific PostgreSQL version instance, return list of row-tuples."""
        import psycopg2
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            dbname=database or "postgres")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    @classmethod
    def pg_create_db_cfg(cls, cfg, db):
        """Create PG database on a specific version instance (idempotent)."""
        rows = cls.pg_query_cfg(
            cfg, "postgres",
            f"SELECT 1 FROM pg_database WHERE datname='{db}'")
        if not rows:
            cls.pg_exec_cfg(cfg, "postgres", [f'CREATE DATABASE "{db}"'])

    @classmethod
    def pg_drop_db_cfg(cls, cfg, db):
        """Drop PG database on a specific version instance."""
        cls.pg_exec_cfg(cfg, "postgres", [
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname='{db}' AND pid <> pg_backend_pid()",
            f'DROP DATABASE IF EXISTS "{db}"',
        ])

    # ---- InfluxDB helpers ----

    @classmethod
    def influx_create_db(cls, bucket):
        """Create InfluxDB v3 database (idempotent)."""
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v3/configure/database"
        r = requests.get(url, params={"format": "json"}, timeout=5)
        if r.status_code == 200:
            if any(d.get("iox::database") == bucket for d in r.json()):
                return  # already exists
        elif r.status_code not in (404,):
            r.raise_for_status()
        r_create = requests.post(url, json={"db": bucket}, timeout=5)
        if r_create.status_code not in (200, 201):
            r_create.raise_for_status()

    @classmethod
    def influx_drop_db(cls, bucket):
        """Drop InfluxDB v3 database (idempotent)."""
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v3/configure/database"
        r = requests.delete(url, params={"db": bucket}, timeout=5)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()

    @classmethod
    def influx_write(cls, bucket, lines):
        """Write line-protocol data to InfluxDB.

        Uses /api/v2/write which InfluxDB 3.x retains for backward
        compatibility. Uses bucket= parameter (v2 compat name) and no
        auth header (running with --without-auth).

        lines: list of line-protocol strings, or a single pre-joined string.
        """
        import requests
        data = lines if isinstance(lines, str) else "\n".join(lines)
        if not data.strip():
            return  # nothing to write
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v2/write"
        params = {"bucket": bucket, "precision": "ns"}
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        r = requests.post(url, params=params, headers=headers,
                          data=data.encode('utf-8'))
        r.raise_for_status()

    @classmethod
    def influx_query_sql(cls, bucket, sql, fmt="json"):
        """Run a SQL query against an InfluxDB v3 database, return parsed JSON.

        InfluxDB 3.x dropped Flux support; use /api/v3/query_sql instead.
        fmt: 'json' (default) | 'csv' | 'pretty'
        """
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v3/query_sql"
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        payload = {"db": bucket, "q": sql, "format": fmt}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    @classmethod
    def influx_create_db_cfg(cls, cfg, bucket):
        """Create InfluxDB v3 database on a specific version instance (idempotent)."""
        import requests
        url = f"http://{cfg.host}:{cfg.port}/api/v3/configure/database"
        r = requests.get(url, params={"format": "json"}, timeout=5)
        if r.status_code == 200:
            if any(d.get("iox::database") == bucket for d in r.json()):
                return
        elif r.status_code not in (404,):
            r.raise_for_status()
        r_create = requests.post(url, json={"db": bucket}, timeout=5)
        if r_create.status_code not in (200, 201):
            r_create.raise_for_status()

    @classmethod
    def influx_drop_db_cfg(cls, cfg, bucket):
        """Drop InfluxDB v3 database on a specific version instance (idempotent)."""
        import requests
        url = f"http://{cfg.host}:{cfg.port}/api/v3/configure/database"
        r = requests.delete(url, params={"db": bucket}, timeout=5)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()

    @classmethod
    def influx_write_cfg(cls, cfg, bucket, lines):
        """Write line-protocol data to a specific InfluxDB v3 instance.

        lines: list of line-protocol strings, or a single pre-joined string.
        """
        import requests
        data = lines if isinstance(lines, str) else "\n".join(lines)
        if not data.strip():
            return  # nothing to write
        url = f"http://{cfg.host}:{cfg.port}/api/v2/write"
        params = {"bucket": bucket, "precision": "ms"}
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        r = requests.post(url, params=params, headers=headers,
                          data=data.encode('utf-8'))
        r.raise_for_status()

    @classmethod
    def influx_query_sql_cfg(cls, cfg, bucket, sql, fmt="json"):
        """Run a SQL query against a specific InfluxDB v3 instance, return parsed JSON."""
        import requests
        url = f"http://{cfg.host}:{cfg.port}/api/v3/query_sql"
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        payload = {"db": bucket, "q": sql, "format": fmt}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()


# =====================================================================
# Shared test mixin — eliminates duplicated helpers across test files
# =====================================================================

class FederatedQueryTestMixin:
    """Mixin providing common helper methods for federated query tests.

    Test classes can inherit from this mixin to get:
      - External source creation/cleanup shortcuts
      - Assertion helpers with proper verification
    """

    # Request the test framework to start taosd with federatedQueryEnable=1
    # so that SHOW/CREATE/ALTER/DROP EXTERNAL SOURCE are available.
    # clientCfg entry ensures psim/cfg/taos.cfg also gets the flag (CFG_SCOPE_BOTH).
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "clientCfg": {"federatedQueryEnable": 1},
    }

    # ------------------------------------------------------------------
    # Source lifecycle helpers
    # ------------------------------------------------------------------

    def _cleanup_src(self, *names):
        """Drop external sources by name (idempotent)."""
        for n in names:
            tdSql.execute(f"drop external source if exists {n}")

    # Alias used by some files
    _cleanup = _cleanup_src

    # ------------------------------------------------------------------
    # Real external source creation (connects to actual databases)
    # ------------------------------------------------------------------

    def _mk_mysql_real(self, name, database="testdb", extra_options=None,
                       user=None, password=None):
        """Create MySQL external source pointing to the configured primary test MySQL.

        Args:
            name:          External source name.
            database:      Remote database name passed in the DDL.
            extra_options: Optional raw options string inserted into OPTIONS(...),
                           e.g. ``"'connect_timeout_ms'='500'"`` or
                           ``"'connect_timeout_ms'='500','max_pool_size'='1'"``.
                           The caller is responsible for proper quoting.
            user:          Override the MySQL user (default: cfg.user).
            password:      Override the MySQL password (default: cfg.password).
        """
        cfg = self._mysql_cfg()
        _user = user if user is not None else cfg.user
        _pass = password if password is not None else cfg.password
        sql = (f"create external source {name} "
               f"type='mysql' host='{cfg.host}' "
               f"port={cfg.port} "
               f"user='{_user}' "
               f"password='{_pass}'")
        if database:
            sql += f" database={database}"
        if extra_options:
            sql += f" options({extra_options})"
        tdSql.execute(sql)

    def _mk_pg_real(self, name, database="pgdb", schema="public"):
        """Create PG external source pointing to the configured primary test PostgreSQL."""
        cfg = self._pg_cfg()
        sql = (f"create external source {name} "
               f"type='postgresql' host='{cfg.host}' "
               f"port={cfg.port} "
               f"user='{cfg.user}' "
               f"password='{cfg.password}'")
        if database:
            sql += f" database={database}"
        if schema:
            sql += f" schema={schema}"
        tdSql.execute(sql)

    def _mk_influx_real(self, name, database="telegraf"):
        """Create InfluxDB external source pointing to the configured primary test InfluxDB."""
        cfg = self._influx_cfg()
        sql = (f"create external source {name} "
               f"type='influxdb' host='{cfg.host}' "
               f"port={cfg.port} "
               f"user='u' password=''")
        if database:
            sql += f" database={database}"
        sql += (f" options('api_token'='{cfg.token}',"
                f"'protocol'='flight_sql')")
        tdSql.execute(sql)

    # ------------------------------------------------------------------
    # Real external source creation (version-specific)
    # ------------------------------------------------------------------

    def _mysql_cfg(self):
        """Return MySQL config for the currently active test version.

        When running under FederatedQueryVersionedMixin the active version is
        set by the per-test fixture; otherwise falls back to the first
        configured version.
        """
        ver = getattr(self, '_active_mysql_ver', None)
        if ver is None:
            return next(ExtSrcEnv.mysql_version_configs())
        for cfg in ExtSrcEnv.mysql_version_configs():
            if cfg.version == ver:
                return cfg
        return next(ExtSrcEnv.mysql_version_configs())

    def _pg_cfg(self):
        """Return PG config for the currently active test version."""
        ver = getattr(self, '_active_pg_ver', None)
        if ver is None:
            return next(ExtSrcEnv.pg_version_configs())
        for cfg in ExtSrcEnv.pg_version_configs():
            if cfg.version == ver:
                return cfg
        return next(ExtSrcEnv.pg_version_configs())

    def _influx_cfg(self):
        """Return InfluxDB config for the currently active test version."""
        ver = getattr(self, '_active_influx_ver', None)
        if ver is None:
            return next(ExtSrcEnv.influx_version_configs())
        for cfg in ExtSrcEnv.influx_version_configs():
            if cfg.version == ver:
                return cfg
        return next(ExtSrcEnv.influx_version_configs())

    def _for_each_mysql_version(self, body_fn):
        """Call body_fn(ver_cfg) once for each configured MySQL version."""
        for cfg in ExtSrcEnv.mysql_version_configs():
            body_fn(cfg)

    def _for_each_pg_version(self, body_fn):
        """Call body_fn(ver_cfg) once for each configured PostgreSQL version."""
        for cfg in ExtSrcEnv.pg_version_configs():
            body_fn(cfg)

    def _for_each_influx_version(self, body_fn):
        """Call body_fn(ver_cfg) once for each configured InfluxDB version."""
        for cfg in ExtSrcEnv.influx_version_configs():
            body_fn(cfg)

    def _mk_mysql_real_ver(self, name, ver_cfg, database="testdb"):
        """Create MySQL external source pointing to a specific version instance."""
        sql = (f"create external source {name} "
               f"type='mysql' host='{ver_cfg.host}' "
               f"port={ver_cfg.port} "
               f"user='{ver_cfg.user}' "
               f"password='{ver_cfg.password}'")
        if database:
            sql += f" database={database}"
        tdSql.execute(sql)

    def _mk_pg_real_ver(self, name, ver_cfg, database="pgdb", schema="public"):
        """Create PostgreSQL external source pointing to a specific version instance."""
        sql = (f"create external source {name} "
               f"type='postgresql' host='{ver_cfg.host}' "
               f"port={ver_cfg.port} "
               f"user='{ver_cfg.user}' "
               f"password='{ver_cfg.password}'")
        if database:
            sql += f" database={database}"
        if schema:
            sql += f" schema={schema}"
        tdSql.execute(sql)

    def _mk_influx_real_ver(self, name, ver_cfg, database="telegraf"):
        """Create InfluxDB external source pointing to a specific version instance."""
        sql = (f"create external source {name} "
               f"type='influxdb' host='{ver_cfg.host}' "
               f"port={ver_cfg.port} "
               f"user='u' password=''")
        if database:
            sql += f" database={database}"
        sql += (f" options('api_token'='{ver_cfg.token}',"
                f"'protocol'='flight_sql')")
        tdSql.execute(sql)

    # ------------------------------------------------------------------
    # Assertion helpers
    # ------------------------------------------------------------------

    def _assert_error_not_syntax(self, sql, queryTimes=10):
        """Execute *sql* expecting an error; assert it is NOT a syntax error.

        Proves the parser accepted the SQL; the failure is expected at
        catalog/connection level (source unreachable, etc.).

        queryTimes: passed to tdSql.query; set to 1 when testing timeouts to
        avoid retry overhead masking the real connection latency.
        """
        ok = tdSql.query(sql, exit=False, queryTimes=queryTimes)
        if ok is not False:
            return  # query succeeded (possible in future builds)
        errno = getattr(tdSql, 'errno', None)
        error_info = getattr(tdSql, 'error_info', None)
        if (TSDB_CODE_PAR_SYNTAX_ERROR is not None
                and errno == TSDB_CODE_PAR_SYNTAX_ERROR):
            raise AssertionError(
                f"Expected non-syntax error for SQL, but got PAR_SYNTAX_ERROR\n"
                f"  sql:        {sql}\n"
                f"  errno:      {errno:#010x}\n"
                f"  error_info: {error_info}"
            )

    # Alias used by some files
    _assert_not_syntax_error = _assert_error_not_syntax

    def _assert_external_context(self, table_name="meters"):
        """Assert current context is external after USE external_source.

        A 1-seg query on *table_name* must NOT return PAR_TABLE_NOT_EXIST
        (which would indicate local resolution) or SYNTAX_ERROR.  Instead
        it should produce a connection/catalog-level error proving the
        context is external.

        Prerequisite: a local table with the same *table_name* must exist
        in the current (local) database, so that PAR_TABLE_NOT_EXIST can
        only mean "resolved locally and not found" vs "resolved externally".
        """
        ok = tdSql.query(f"select * from {table_name} limit 1", exit=False)
        if ok is not False:
            return  # query succeeded — may happen if real external DB is up
        errno = getattr(tdSql, 'errno', None)
        error_info = getattr(tdSql, 'error_info', None)
        if (TSDB_CODE_PAR_TABLE_NOT_EXIST is not None
                and errno == TSDB_CODE_PAR_TABLE_NOT_EXIST):
            raise AssertionError(
                f"After USE external, '{table_name}' resolved locally (PAR_TABLE_NOT_EXIST)\n"
                f"  errno:      {errno:#010x}\n"
                f"  error_info: {error_info}"
            )
        if (TSDB_CODE_PAR_SYNTAX_ERROR is not None
                and errno == TSDB_CODE_PAR_SYNTAX_ERROR):
            raise AssertionError(
                f"After USE external, '{table_name}' got SYNTAX_ERROR\n"
                f"  errno:      {errno:#010x}\n"
                f"  error_info: {error_info}"
            )

    def _assert_local_context(self, db, table_name, expected_val):
        """Assert current context is local *db* by verifying data.

        A 1-seg query on *table_name* returns *expected_val* at row 0 col 1,
        proving USE local_db took effect.
        """
        tdSql.query(f"select * from {table_name} order by ts limit 1")
        tdSql.checkData(0, 1, expected_val)

    def _assert_describe_field(self, source_name, field, expected):
        """DESCRIBE external source and assert *field* equals *expected*.

        Useful for verifying ALTER operations actually took effect.
        """
        tdSql.query(f"describe external source {source_name}")
        desc = {str(r[0]).lower(): str(r[1]) for r in tdSql.queryResult}
        actual = desc.get(field.lower(), "")
        assert actual == str(expected), (
            f"Expected {field}={expected} for source '{source_name}', "
            f"got '{actual}'. Full desc: {desc}"
        )


# =====================================================================
# Versioned test mixin — per-version parametrization for fq_01 ~ fq_05
# =====================================================================

class FederatedQueryVersionedMixin(FederatedQueryTestMixin):
    """Extends FederatedQueryTestMixin with automatic per-version parametrization.

    Each test method in a subclass runs **once per version combo** determined by
    FQ_MYSQL_VERSIONS / FQ_PG_VERSIONS / FQ_INFLUX_VERSIONS (zip_longest).
    Pytest serialises the fixture parameters so versions are always tested one
    at a time, back-to-back.

    At the start of every test the ``_version_combo`` autouse fixture sets
    ``self._active_mysql_ver`` etc., so that ``self._mysql_cfg()`` /
    ``self._pg_cfg()`` / ``self._influx_cfg()`` return the correct connection
    details automatically — no changes to test bodies needed.

    ``self._version_label()`` returns a human-readable string such as
    ``'my8.0-pg16-inf3.0'`` that test result helpers can append to scenario
    names so the final summary shows per-scenario × per-version rows.

    When only default single versions are configured each test runs exactly
    once, identical to the pre-versioning behavior.

    Usage::

        class TestFqXX(FederatedQueryVersionedMixin):
            ...

    Do NOT use for fq_12 (which iterates versions explicitly inside test bodies).
    """

    @pytest.fixture(autouse=True,
                    params=ExtSrcEnv._version_combos(),
                    ids=ExtSrcEnv._version_combo_ids())
    def _version_combo(self, request):
        mysql_ver, pg_ver, influx_ver = request.param
        self._active_mysql_ver = mysql_ver
        self._active_pg_ver = pg_ver
        self._active_influx_ver = influx_ver
        yield
        self._active_mysql_ver = None
        self._active_pg_ver = None
        self._active_influx_ver = None

    def _version_label(self):
        """Return the current version-combo label, e.g. ``'my8.0-pg16-inf3.0'``.

        Call from ``_start_test`` / ``_record_pass`` / ``_record_fail`` to tag
        every result record with the version under test, so the final summary
        shows one row per (scenario, version) combination.
        """
        mysql_ver = getattr(self, '_active_mysql_ver', None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        pg_ver = getattr(self, '_active_pg_ver', None) or ExtSrcEnv.PG_VERSIONS[0]
        influx_ver = getattr(self, '_active_influx_ver', None) or ExtSrcEnv.INFLUX_VERSIONS[0]
        return f"my{mysql_ver}-pg{pg_ver}-inf{influx_ver}"


class FederatedQueryCaseHelper:
    BASE_DB = "fq_case_db"
    SRC_DB = "fq_src_db"

    def __init__(self, case_file: str):
        self.case_dir = os.path.dirname(os.path.abspath(case_file))
        self.in_dir = os.path.join(self.case_dir, "in")
        self.ans_dir = os.path.join(self.case_dir, "ans")
        os.makedirs(self.in_dir, exist_ok=True)
        os.makedirs(self.ans_dir, exist_ok=True)

    def prepare_shared_data(self):
        sqls = [
            f"drop database if exists {self.SRC_DB}",
            f"drop database if exists {self.BASE_DB}",
            f"create database {self.SRC_DB}",
            f"create database {self.BASE_DB}",
            f"use {self.SRC_DB}",
            "create table src_ntb (ts timestamp, c_int int, c_double double, c_bool bool, c_str binary(16))",
            "insert into src_ntb values (1704067200000, 1, 1.5, true, 'alpha')",
            "insert into src_ntb values (1704067260000, 2, 2.5, false, 'beta')",
            "insert into src_ntb values (1704067320000, 3, 3.5, true, 'gamma')",
            "create stable src_stb (ts timestamp, val int, extra float, flag bool) tags(region int, owner nchar(16))",
            "create table src_ctb_a using src_stb tags(1, 'north')",
            "create table src_ctb_b using src_stb tags(2, 'south')",
            "insert into src_ctb_a values (1704067200000, 11, 1.1, true)",
            "insert into src_ctb_a values (1704067260000, 12, 1.2, false)",
            "insert into src_ctb_b values (1704067200000, 21, 2.1, true)",
            "insert into src_ctb_b values (1704067260000, 22, 2.2, true)",
            f"use {self.BASE_DB}",
            "create table local_dim (ts timestamp, sensor_id int, weight int, owner binary(16))",
            "insert into local_dim values (1704067200000, 11, 100, 'team_a')",
            "insert into local_dim values (1704067260000, 21, 200, 'team_b')",
            "create stable vstb_fq (ts timestamp, v_int int, v_float float, v_status bool) tags(vg int) virtual 1",
            (
                "create vtable vctb_fq ("
                "v_int from fq_src_db.src_ctb_a.val, "
                "v_float from fq_src_db.src_ctb_a.extra, "
                "v_status from fq_src_db.src_ctb_a.flag"
                ") using vstb_fq tags(1)"
            ),
            (
                "create vtable vctb_fq_b ("
                "v_int from fq_src_db.src_ctb_b.val, "
                "v_float from fq_src_db.src_ctb_b.extra, "
                "v_status from fq_src_db.src_ctb_b.flag"
                ") using vstb_fq tags(2)"
            ),
            (
                "create vtable vntb_fq ("
                "ts timestamp, "
                "v_int int from fq_src_db.src_ntb.c_int, "
                "v_float double from fq_src_db.src_ntb.c_double, "
                "v_status bool from fq_src_db.src_ntb.c_bool"
                ")"
            ),
        ]
        tdSql.executes(sqls)

    def require_external_source_feature(self):
        if tdSql.query("show external sources", exit=False) is False:
            pytest.skip("external source feature is unavailable in current build")
        # Ensure federatedQueryEnable is active in this client process.
        # taos_init reads psim/cfg/taos.cfg (which has federatedQueryEnable 1),
        # but call alter local as a belt-and-suspenders guarantee.
        try:
            tdSql.execute('alter local "federatedQueryEnable" "1"')
        except Exception as e:
            tdLog.warning(f"alter local federatedQueryEnable failed: {e}")

    def assert_query_result(self, sql: str, expected_rows):
        """Execute *sql* and assert results match *expected_rows*.

        On any mismatch the error message shows:
          - the SQL that was executed
          - the actual error (if execution failed)
          - a side-by-side actual vs expected table for data mismatches
        """
        try:
            tdSql.query(sql)
        except Exception as e:
            raise AssertionError(
                f"Query execution failed\n"
                f"  sql:   {sql}\n"
                f"  error: {e}"
            ) from e

        actual_rows_list = list(tdSql.queryResult)
        actual_count    = len(actual_rows_list)
        expected_count  = len(expected_rows)

        if actual_count != expected_count:
            raise AssertionError(
                f"Row count mismatch\n"
                f"  sql:      {sql}\n"
                f"  expected: {expected_count} rows\n"
                f"  actual:   {actual_count} rows\n"
                f"{_fmt_result_table(actual_rows_list, expected_rows)}"
            )

        for row_idx, row_data in enumerate(expected_rows):
            for col_idx, expected in enumerate(row_data):
                actual = actual_rows_list[row_idx][col_idx]
                if actual != expected:
                    raise AssertionError(
                        f"Data mismatch at row {row_idx}, col {col_idx}\n"
                        f"  sql:      {sql}\n"
                        f"  expected: {expected!r}\n"
                        f"  actual:   {actual!r}\n"
                        f"{_fmt_result_table(actual_rows_list, expected_rows)}"
                    )

    def assert_error_code(self, sql: str, expected_errno: int):
        tdSql.error(sql, expectedErrno=expected_errno)

    def batch_query_and_check(self, sql_list, expected_result_list):
        tdSql.queryAndCheckResult(sql_list, expected_result_list)

    def compare_sql_files(self, case_name: str, uut_sql_list, ref_sql_list, db_name=None):
        if db_name is None:
            db_name = self.BASE_DB

        uut_sql_file = os.path.join(self.in_dir, f"{case_name}.sql")
        ref_sql_file = os.path.join(self.in_dir, f"{case_name}.ref.sql")
        expected_result_file = ""

        try:
            self._write_sql_file(uut_sql_file, db_name, uut_sql_list)
            self._write_sql_file(ref_sql_file, db_name, ref_sql_list)

            expected_result_file = tdCom.generate_query_result(ref_sql_file, f"{case_name}_ref")
            tdCom.compare_testcase_result(uut_sql_file, expected_result_file, f"{case_name}_uut")
        finally:
            for path in (uut_sql_file, ref_sql_file, expected_result_file):
                if path and os.path.exists(path):
                    os.remove(path)

    @staticmethod
    def _write_sql_file(file_path: str, db_name: str, sql_lines):
        with open(file_path, "w", encoding="utf-8") as fout:
            fout.write(f"use {db_name};\n")
            for sql in sql_lines:
                stmt = sql.strip().rstrip(";") + ";"
                fout.write(stmt + "\n")

    @staticmethod
    def assert_plan_contains(sql: str, keyword: str):
        """Assert *keyword* appears in ``EXPLAIN VERBOSE TRUE`` output.

        On failure the full plan is shown so the caller can see what the
        planner actually produced.
        """
        tdSql.query(f"explain verbose true {sql}")
        plan_lines = []
        for row in tdSql.queryResult:
            for col in row:
                if col is not None:
                    plan_lines.append(str(col))
                    if keyword in str(col):
                        return
        plan_dump = "\n    ".join(
            f"[{i:02d}] {l}" for i, l in enumerate(plan_lines)
        )
        raise AssertionError(
            f"expected keyword '{keyword}' not found in plan\n"
            f"  sql: {sql}\n"
            f"  plan ({len(plan_lines)} lines):\n"
            f"    {plan_dump}"
        )
