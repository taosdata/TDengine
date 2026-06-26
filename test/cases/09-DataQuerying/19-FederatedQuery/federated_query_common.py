import os
import re
import sys
import subprocess
import datetime as _datetime
import shutil
import time as _time
import pytest
from collections import namedtuple
from itertools import zip_longest
from typing import Dict, List, Optional, Set, Tuple

from new_test_framework.utils import tdLog, tdSql, tdCom


def _fq_subprocess_env():
    env = os.environ.copy()
    for key in ("LD_PRELOAD", "LD_LIBRARY_PATH", "ASAN_OPTIONS", "LSAN_OPTIONS", "UBSAN_OPTIONS"):
        env.pop(key, None)
    return env


# =====================================================================
# Dynamic error code loader — parses taoserror.h at import time
#
# Instead of hardcoding hex values that drift when the source changes,
# we read the authoritative header file and resolve every TSDB_CODE_*
# to its current integer value.  Codes not yet defined in the header
# (e.g. enterprise-only codes that haven't shipped) resolve to None,
# which causes tdSql.error() to check only that *some* error occurs.
# =====================================================================

# ---------------------------------------------------------------------------
# Standard 5-row test dataset used by FederatedQueryTestMixin._with_std_sources().
# Mirrors the data inserted by TestFq05LocalUnsupported._prepare_internal_env().
# Columns: (ts_ms, val, score, name, flag_int)
# Timestamps: 0/60/120/180/240 seconds from 2024-01-01T00:00:00 UTC
# ---------------------------------------------------------------------------
_STD_ROWS = [
    (1704067200000, 1, 1.5, 'alpha',   1),
    (1704067260000, 2, 2.5, 'beta',    0),
    (1704067320000, 3, 3.5, 'gamma',   1),
    (1704067380000, 4, 4.5, 'delta',   0),
    (1704067440000, 5, 5.5, 'epsilon', 1),
]


def _ms_to_dt(ms_ts):
    """Return 'YYYY-MM-DD HH:MM:SS.mmm' (local time) for a millisecond timestamp.

    MySQL DATETIME and PG TIMESTAMP (without tz) are timezone-naive: they store
    the literal string and are NOT affected by SET time_zone / SET TIME ZONE.
    The TDengine ext-connector parses these strings using the taosd system
    timezone, so the inserted strings must be in *local* time so that the
    round-trip epoch is correct.
    """
    dt = _datetime.datetime.fromtimestamp(ms_ts / 1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms_ts % 1000:03d}"


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
TSDB_CODE_PAR_INVALID_COLUMN                   = _code('TSDB_CODE_PAR_INVALID_COLUMN')
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
TSDB_CODE_EXT_CONNECT_FAILED                   = _code('TSDB_CODE_EXT_CONNECT_FAILED')
TSDB_CODE_EXT_AUTH_FAILED                      = _code('TSDB_CODE_EXT_AUTH_FAILED')
TSDB_CODE_EXT_ACCESS_DENIED                    = _code('TSDB_CODE_EXT_ACCESS_DENIED')
TSDB_CODE_EXT_QUERY_TIMEOUT                    = _code('TSDB_CODE_EXT_QUERY_TIMEOUT')
TSDB_CODE_EXT_SOURCE_UNAVAILABLE               = _code('TSDB_CODE_EXT_SOURCE_UNAVAILABLE')
TSDB_CODE_EXT_RESOURCE_EXHAUSTED               = _code('TSDB_CODE_EXT_RESOURCE_EXHAUSTED')
TSDB_CODE_EXT_FETCH_FAILED                     = _code('TSDB_CODE_EXT_FETCH_FAILED')
TSDB_CODE_EXT_SOURCE_CHANGED                   = _code('TSDB_CODE_EXT_SOURCE_CHANGED')
TSDB_CODE_EXT_SCHEMA_CHANGED                   = _code('TSDB_CODE_EXT_SCHEMA_CHANGED')
TSDB_CODE_EXT_CAPABILITY_CHANGED               = _code('TSDB_CODE_EXT_CAPABILITY_CHANGED')
TSDB_CODE_EXT_WRITE_DENIED                     = _code('TSDB_CODE_EXT_WRITE_DENIED')
TSDB_CODE_EXT_STREAM_NOT_SUPPORTED             = _code('TSDB_CODE_EXT_STREAM_NOT_SUPPORTED')
TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED          = _code('TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED')
TSDB_CODE_EXT_REMOTE_INTERNAL                  = _code('TSDB_CODE_EXT_REMOTE_INTERNAL')
TSDB_CODE_PAR_NOT_SUPPORT_JOIN                 = _code('TSDB_CODE_PAR_NOT_SUPPORT_JOIN')
TSDB_CODE_PAR_INVALID_COL_JSON                 = _code('TSDB_CODE_PAR_INVALID_COL_JSON')
TSDB_CODE_PAR_INVALID_EXPR_SUBQ                = _code('TSDB_CODE_PAR_INVALID_EXPR_SUBQ')
TSDB_CODE_OPS_NOT_SUPPORT                      = _code('TSDB_CODE_OPS_NOT_SUPPORT')
TSDB_CODE_TSC_NO_EXEC_NODE                     = _code('TSDB_CODE_TSC_NO_EXEC_NODE')
TSDB_CODE_INVALID_PARA                         = _code('TSDB_CODE_INVALID_PARA')

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
TSDB_CODE_EXT_FEDERATED_DISABLED               = _code('TSDB_CODE_EXT_FEDERATED_DISABLED')

# --- Mnode (general) ---
TSDB_CODE_MND_DB_ALREADY_EXIST                 = _code('TSDB_CODE_MND_DB_ALREADY_EXIST')
TSDB_CODE_MND_FUNC_NOT_EXIST                   = _code('TSDB_CODE_MND_FUNC_NOT_EXIST')
TSDB_CODE_EXT_SOURCE_EXISTS                    = _code('TSDB_CODE_EXT_SOURCE_EXISTS')

# --- Qnode scheduling ---
TSDB_CODE_QNODE_NOT_FOUND                      = _code('TSDB_CODE_QNODE_NOT_FOUND')
TSDB_CODE_QNODE_ALREADY_DEPLOYED               = _code('TSDB_CODE_QNODE_ALREADY_DEPLOYED')

# --- Function errors ---
TSDB_CODE_FUNC_FUNTION_PARA_TYPE               = _code('TSDB_CODE_FUNC_FUNTION_PARA_TYPE')
TSDB_CODE_FUNC_FUNTION_PARA_NUM                = _code('TSDB_CODE_FUNC_FUNTION_PARA_NUM')


# =====================================================================
# TLS certificate paths
#
# Certificates are generated by ensure_ext_env.sh into FQ_CERT_DIR
# (default: <case-dir>/certs; FQ_BASE_DIR defaults to /opt/taostest/fq on
# Linux and ~/taostest/fq on macOS).  All paths here must match what
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

def _default_fq_base_dir() -> str:
    """Return the default external-source base dir for the current platform."""
    return os.getenv(
        "FQ_BASE_DIR",
        os.path.join(os.path.expanduser("~"), "taostest", "fq")
        if sys.platform == "darwin"
        else "/opt/taostest/fq",
    )


def _read_influx_token_file(ver: str) -> Optional[str]:
    """Read InfluxDB admin token from the file written by ensure_ext_env.sh.

    Returns the token string, or None if the file is missing/empty.
    """
    try:
        fq_base = _default_fq_base_dir()
        tok_file = os.path.join(fq_base, "influxdb", ver, "admin_token.txt")
        with open(tok_file) as _tf:
            tok = _tf.read().strip()
        return tok if tok else None
    except OSError:
        return None


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
        "1.8": int(os.getenv("FQ_INFLUX_PORT_18", "18085")),
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
    # Priority: FQ_INFLUX_TOKEN env var → token file written by ensure_ext_env.sh
    # → "test-token" fallback (used when InfluxDB runs without auth).
    INFLUX_TOKEN = (
        os.getenv("FQ_INFLUX_TOKEN")
        or _read_influx_token_file(INFLUX_VERSIONS[0])
        or "test-token"
    )
    INFLUX_ORG   = os.getenv("FQ_INFLUX_ORG",   "test-org")

    # Pool-exhaustion test user — created by ensure_ext_env.sh with
    # MAX_USER_CONNECTIONS limited to FQ_POOL_TEST_MAX_CONN (default 1).
    # Tests use this user to saturate the per-user connection limit and
    # trigger TSDB_CODE_EXT_RESOURCE_EXHAUSTED.
    POOL_TEST_USER     = os.getenv("FQ_POOL_TEST_USER",     "fq_pool_test")
    POOL_TEST_PASS     = os.getenv("FQ_POOL_TEST_PASS",     "taosdata")
    POOL_TEST_MAX_CONN = int(os.getenv("FQ_POOL_TEST_MAX_CONN", "1"))

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

        Call once per test file from setup_class (re-runs every invocation to
        guarantee a clean env — no caching across test files).
        Raises RuntimeError (not pytest.skip) so failures are clearly visible.
        """

        # ------------------------------------------------------------------
        # Step 1: run platform-appropriate setup script
        # ------------------------------------------------------------------
        import subprocess

        here = os.path.dirname(os.path.abspath(__file__))

        env = _fq_subprocess_env()
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
        cls._revive_attempts = 0   # reset mid-test recovery counter
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
                # InfluxDB 1.x uses /ping (HTTP 204); 3.x uses /health (JSON)
                if cfg.version.startswith("1."):
                    r = requests.get(
                        f"http://{cfg.host}:{cfg.port}/ping",
                        timeout=5)
                    if r.status_code != 204:
                        errors.append(
                            f"  InfluxDB {cfg.version} @ {cfg.host}:{cfg.port} — "
                            f"/ping returned HTTP {r.status_code}")
                else:
                    r = requests.get(
                        f"http://{cfg.host}:{cfg.port}/health",
                        timeout=5)
                    # 200 = running without auth (pass/ok body expected)
                    # 401 = running with auth enabled — server is up and reachable
                    if r.status_code not in (200, 204, 401):
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

        # ------------------------------------------------------------------
        # Step 3: ensure a qnode is deployed on dnode 1.
        # Pure federated queries require qnode for MERGE plan execution;
        # without one the client returns TSDB_CODE_QNODE_NOT_FOUND.
        # This step is idempotent — repeated CREATE QNODE is silently ignored.
        # ------------------------------------------------------------------
        cls.ensure_qnode()

        # ------------------------------------------------------------------
        # Step 4: drop all stale external sources from previous test runs.
        # The WORK_DIR (taosd data dir) persists across test sessions, so
        # external sources created by earlier test files accumulate.  Purge
        # them here so every test class starts from a clean state.
        # ------------------------------------------------------------------
        cls.purge_all_external_sources()

    @classmethod
    def purge_all_external_sources(cls):
        """Drop every external source currently registered in this taosd.

        Called automatically from ensure_env() before each test class runs.
        This prevents cross-test contamination when the WORK_DIR is reused
        between sessions (e.g. fq_17/fq_18 sources leaking into fq_01/fq_08).
        """
        try:
            rows = tdSql.query(
                "select source_name from information_schema.ins_ext_sources",
                row_tag=True)
            names = [r[0] for r in rows] if rows else []
        except Exception as e:
            tdLog.info(f"[FQ env] purge_all_external_sources: query failed ({e}), skipping")
            return
        for name in names:
            try:
                tdSql.execute(f"drop external source if exists {name}", queryTimes=1)
                tdLog.info(f"[FQ env] purge: dropped stale external source '{name}'")
            except Exception as e:
                tdLog.info(f"[FQ env] purge: drop '{name}' failed ({e}), ignored")

    @classmethod
    def ensure_qnode(cls):
        """Idempotently create a qnode on dnode 1.

        Pure federated queries (SUBPLAN_TYPE_MERGE) must execute on a qnode.
        The scheduler picks qnode from the nodeList; if no qnode is deployed,
        the client returns TSDB_CODE_QNODE_NOT_FOUND (by design — mnode
        fallback is explicitly blocked for federated queries).

        This method is called automatically from ensure_env() and may also
        be called directly to restore qnode after a no-qnode negative test.
        """
        try:
            tdSql.execute("CREATE QNODE ON DNODE 1", queryTimes=1)
            tdLog.info("[FQ env] created qnode on dnode 1")
        except Exception as e:
            # QNODE_ALREADY_DEPLOYED is expected on repeated calls — ignore.
            tdLog.info(f"[FQ env] qnode already exists or create skipped: {e}")

    @classmethod
    def drop_qnode(cls):
        """Drop the qnode on dnode 1 (for negative-test teardown).

        After DROP, the qnode cache in connected clients is invalidated via
        heartbeat (within ~1-3 s).  Callers must wait for invalidation before
        querying.  Always follow with ensure_qnode() in a finally block to
        restore the environment for subsequent tests.
        """
        try:
            tdSql.execute("DROP QNODE ON DNODE 1", queryTimes=1)
            tdLog.info("[FQ env] dropped qnode on dnode 1")
        except Exception as e:
            tdLog.info(f"[FQ env] qnode drop skipped: {e}")

    # Number of mid-test ext-DB recovery attempts already consumed.
    # Reset to 0 by each ensure_env() call.
    _revive_attempts = 0
    _REVIVE_MAX = 2          # at most 2 mid-test recoveries per file

    @classmethod
    def revive_if_dead(cls):
        """Re-start ext DBs if they died mid-test; return True if revived.

        Checks TCP connectivity to every configured ext-DB port.  If any
        is unreachable, only restarts the dead services using quick-restart
        mode (preserving existing test data) and then re-verifies.  Returns
        True if a recovery was performed, False if all ext DBs were already
        alive.

        Raises RuntimeError if recovery fails or the maximum number of
        mid-test recoveries has been exceeded (to prevent infinite loops
        masking a real environment issue).

        Callers should invoke this when they observe ext-DB connection
        failures, then retry the failed query.  The method is idempotent
        and safe to call when ext DBs are healthy (fast TCP probe, no
        subprocess overhead).
        """
        dead = []
        dead_mysql = []
        dead_pg = []
        dead_influx = []
        for cfg in cls.mysql_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                dead.append(f"MySQL {cfg.version} @ {cfg.host}:{cfg.port}")
                dead_mysql.append(cfg.version)
        for cfg in cls.pg_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                dead.append(f"PG {cfg.version} @ {cfg.host}:{cfg.port}")
                dead_pg.append(cfg.version)
        for cfg in cls.influx_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                dead.append(f"InfluxDB {cfg.version} @ {cfg.host}:{cfg.port}")
                dead_influx.append(cfg.version)

        if not dead:
            return False      # all alive — nothing to do

        cls._revive_attempts += 1
        if cls._revive_attempts > cls._REVIVE_MAX:
            raise RuntimeError(
                f"[FQ revive] ext DBs died again (attempt {cls._revive_attempts}/"
                f"{cls._REVIVE_MAX}); giving up.  Dead: {dead}")

        tdLog.info(
            f"[FQ revive] ext-DB processes dead mid-test (attempt "
            f"{cls._revive_attempts}/{cls._REVIVE_MAX}): {dead}")
        tdLog.info("[FQ revive] restarting dead services (quick restart) ...")

        here = os.path.dirname(os.path.abspath(__file__))
        env = _fq_subprocess_env()
        env["FQ_MYSQL_VERSIONS"]  = ",".join(cls.MYSQL_VERSIONS)
        env["FQ_PG_VERSIONS"]     = ",".join(cls.PG_VERSIONS)
        env["FQ_INFLUX_VERSIONS"] = ",".join(cls.INFLUX_VERSIONS)
        # Use quick restart for InfluxDB: preserve the data directory so that
        # the existing admin token stays valid.  revive_if_dead is a mid-test
        # recovery; hard reset would generate a new token, making _I_TOKEN
        # (cached at import time) stale and breaking all subsequent auth.
        env["FQ_INFLUX_QUICK_RESTART"] = "1"
        sh = os.path.join(here, "ensure_ext_env.sh")
        ret = subprocess.call(["bash", sh], env=env)
        if ret != 0:
            raise RuntimeError(
                f"[FQ revive] ensure_ext_env.sh failed (exit={ret})")

        # Re-verify
        still_dead = []
        for cfg in cls.mysql_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                still_dead.append(f"MySQL {cfg.version}")
        for cfg in cls.pg_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                still_dead.append(f"PG {cfg.version}")
        for cfg in cls.influx_version_configs():
            if not cls._tcp_probe(cfg.host, cfg.port):
                still_dead.append(f"InfluxDB {cfg.version}")
        if still_dead:
            raise RuntimeError(
                f"[FQ revive] ext DBs still dead after restart: {still_dead}")

        tdLog.info("[FQ revive] ext DBs restored successfully.")
        return True

    @staticmethod
    def _tcp_probe(host: str, port: int, timeout: float = 2.0) -> bool:
        """Return True if *host*:*port* accepts a TCP connection."""
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError, TimeoutError):
            return False
        finally:
            s.close()

    @staticmethod
    def _is_retriable_ext_conn_error(exc: Exception) -> bool:
        """Return True when *exc* looks like a transient external DB connection failure."""
        msg = str(exc).lower()
        needles = (
            "connection refused",
            "can't connect",
            "could not connect to server",
            "server closed the connection unexpectedly",
            "lost connection",
            "connection reset by peer",
            "failed to establish a new connection",
            "timed out",
        )
        return any(n in msg for n in needles)

    @classmethod
    def teardown_env(cls):
        """Stop all external DB processes and rotate oversized log files.

        Must be called unconditionally at the end of every FQ test class
        (teardown_class / fixture teardown), regardless of pass/fail.
        Uses ensure_ext_env.sh --teardown which is a no-op if processes are
        already stopped.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        env = _fq_subprocess_env()
        env["FQ_MYSQL_VERSIONS"]  = ",".join(cls.MYSQL_VERSIONS)
        env["FQ_PG_VERSIONS"]     = ",".join(cls.PG_VERSIONS)
        env["FQ_INFLUX_VERSIONS"] = ",".join(cls.INFLUX_VERSIONS)

        if sys.platform != "win32":
            sh = os.path.join(here, "ensure_ext_env.sh")
            if os.path.exists(sh):
                ret = subprocess.call(["bash", sh, "--teardown"], env=env)
                if ret != 0:
                    # Non-fatal: log but do not raise — teardown must not mask
                    # the original test failure.
                    import logging
                    logging.getLogger(__name__).warning(
                        "ensure_ext_env.sh --teardown exited with %d", ret)

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
                                cls._get_influx_token(ver), cls.INFLUX_ORG)

    @classmethod
    def _get_influx_token(cls, ver):
        """Return the InfluxDB admin token.

        Reads from the token file written by ensure_ext_env.sh after each
        hard reset (data wipe + restart).  Falls back to FQ_INFLUX_TOKEN env
        var or the 'test-token' default (used when InfluxDB runs without auth).
        """
        fq_base = _default_fq_base_dir()
        token_file = os.path.join(fq_base, "influxdb", ver, "admin_token.txt")
        try:
            with open(token_file) as _tf:
                tok = _tf.read().strip()
            if tok:
                return tok
        except OSError:
            pass
        return cls.INFLUX_TOKEN

    # ---- Service lifecycle helpers (for unreachability tests) ----
    #
    # Tests that need to stop/start a real instance call these helpers and
    # wrap the body with try/finally to guarantee the instance is restarted.

    @classmethod
    def _kill_process_by_pidfile(cls, pidfile, wait_s=30):
        """SIGTERM a process identified by pidfile; SIGKILL if it lingers."""
        import os, signal, time
        with open(pidfile) as _pf:
            pid = int(_pf.read().strip())
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return  # already exited
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
    def _kill_process_sigkill(cls, pidfile, pid_first_line=False):
        """Send SIGKILL immediately to a process identified by pidfile."""
        import os, signal
        with open(pidfile) as _pf:
            content = _pf.read().strip()
            pid = int(content.split('\n')[0] if pid_first_line else content)
        os.kill(pid, signal.SIGKILL)

    @classmethod
    def kill_mysql_instance(cls, ver):
        """Send SIGKILL to the MySQL process."""
        fq_base = _default_fq_base_dir()
        pidfile = os.path.join(fq_base, "mysql", ver, "run", "mysqld.pid")
        cls._kill_process_sigkill(pidfile)

    @classmethod
    def kill_pg_instance(cls, ver):
        """Send SIGKILL to the PostgreSQL process."""
        # postmaster.pid: first line is PID
        fq_base = _default_fq_base_dir()
        pidfile = os.path.join(fq_base, "pg", ver, "data", "postmaster.pid")
        cls._kill_process_sigkill(pidfile, pid_first_line=True)

    @classmethod
    def kill_influx_instance(cls, ver):
        """Send SIGKILL to the InfluxDB process."""
        fq_base = _default_fq_base_dir()
        pidfile = os.path.join(fq_base, "influxdb", ver, "run", "influxd.pid")
        cls._kill_process_sigkill(pidfile)

    @classmethod
    def stop_mysql_instance(cls, ver):
        """Stop the MySQL instance for the given version.

        After this call the MySQL port for 'ver' is unreachable.
        Always pair with start_mysql_instance() in a try/finally block.
        """
        fq_base = _default_fq_base_dir()
        pidfile = os.path.join(fq_base, "mysql", ver, "run", "mysqld.pid")
        cls._kill_process_by_pidfile(pidfile)

    @classmethod
    def start_mysql_instance(cls, ver, wait_s=30):
        """Start the MySQL instance for the given version and wait until ready."""
        import subprocess, time
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "ensure_ext_env.sh")
        # Strip ASAN/TDengine library vars from the bash subprocess environment.
        # ensure_ext_env.sh uses _fq_env_clean internally, but removing them at
        # the Python level is cleaner: apt-get, dpkg, and the mysql binary itself
        # can run cleanly without LD_PRELOAD=libasan.so or TDengine's LD_LIBRARY_PATH.
        env = _fq_subprocess_env()
        fq_base = _default_fq_base_dir()
        mysql_base = os.path.join(fq_base, "mysql", ver)
        has_mysql_install = os.path.isfile(
            os.path.join(mysql_base, "bin", "mysqld"))
        has_mysql_data = (
            os.path.isdir(os.path.join(mysql_base, "data", "mysql"))
            or os.path.isfile(os.path.join(mysql_base, "data", "ibdata1"))
        )
        env["FQ_SERVICES_TO_RESET"] = "mysql"
        if has_mysql_install and has_mysql_data:
            env["FQ_MYSQL_QUICK_RESTART"] = "1"
        script_timeout = int(os.getenv("FQ_MYSQL_SCRIPT_TIMEOUT_S", "600"))
        script_error = None
        try:
            subprocess.run(["bash", script],
                           env=env, check=True, capture_output=False,
                           timeout=script_timeout)
        except subprocess.CalledProcessError as exc:
            script_error = exc
        cfg = next(c for c in cls.mysql_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        import pymysql
        while time.time() < deadline:
            try:
                conn = pymysql.connect(host=cfg.host, port=cfg.port,
                                       user=cfg.user, password=cfg.password,
                                       connect_timeout=2)
                conn.close()
                if script_error is not None:
                    tdLog.info(
                        f"[FQ env] ensure_ext_env.sh exited "
                        f"{script_error.returncode} for MySQL {ver}, "
                        f"but MySQL is accepting connections; continuing.")
                return
            except Exception:
                time.sleep(0.5)
        if script_error is not None:
            raise script_error
        raise RuntimeError(
            f"MySQL {ver} did not become ready within {wait_s}s")

    @classmethod
    def stop_pg_instance(cls, ver):
        """Stop the PostgreSQL instance for the given version."""
        import subprocess
        import shutil
        fq_base = _default_fq_base_dir()
        datadir = os.path.join(fq_base, "pg", ver, "data")
        pg_ctl_bin = os.path.join(fq_base, "pg", ver, "bin", "pg_ctl")
        import pwd as _pwd
        try:
            pg_owner = _pwd.getpwuid(os.stat(datadir).st_uid).pw_name
            runuser = shutil.which("runuser")
            if runuser:
                cmd = [runuser, "-u", pg_owner, "--",
                       pg_ctl_bin, "stop", "-D", datadir, "-m", "fast"]
            else:
                cmd = [pg_ctl_bin, "stop", "-D", datadir, "-m", "fast"]
        except (KeyError, PermissionError):
            cmd = [pg_ctl_bin, "stop", "-D", datadir, "-m", "fast"]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)

    @classmethod
    def start_pg_instance(cls, ver, wait_s=10):
        """Start the PostgreSQL instance for the given version and wait until ready."""
        import subprocess, time
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "ensure_ext_env.sh")
        # Strip ASAN/TDengine library vars so dpkg, apt-get and postgres run cleanly.
        env = _fq_subprocess_env()
        fq_base = _default_fq_base_dir()
        pg_base = os.path.join(fq_base, "pg", ver)
        has_pg_data = (
            os.path.isfile(os.path.join(pg_base, "data", "PG_VERSION"))
            or os.path.isfile(os.path.join(pg_base, "data", "postgresql.conf"))
        )
        env["FQ_SERVICES_TO_RESET"] = "pg"
        if has_pg_data:
            env["FQ_PG_QUICK_RESTART"] = "1"
        script_timeout = int(os.getenv("FQ_PG_SCRIPT_TIMEOUT_S", "600"))
        script_error = None
        try:
            subprocess.run(["bash", script],
                           env=env, check=True, capture_output=False,
                           timeout=script_timeout)
        except subprocess.CalledProcessError as exc:
            script_error = exc
        cfg = next(c for c in cls.pg_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        import psycopg2
        while time.time() < deadline:
            try:
                conn = psycopg2.connect(host=cfg.host, port=cfg.port,
                                        user=cfg.user, password=cfg.password,
                                        connect_timeout=2)
                conn.close()
                if script_error is not None:
                    tdLog.info(
                        f"[FQ env] ensure_ext_env.sh exited "
                        f"{script_error.returncode} for PostgreSQL {ver}, "
                        f"but PG is accepting connections; continuing.")
                return
            except Exception:
                time.sleep(0.5)
        if script_error is not None:
            raise script_error
        raise RuntimeError(
            f"PostgreSQL {ver} did not become ready within {wait_s}s")

    @classmethod
    def stop_influx_instance(cls, ver):
        """Stop the InfluxDB instance for the given version."""
        import signal, time
        fq_base = _default_fq_base_dir()
        pidfile = os.path.join(fq_base, "influxdb", ver, "run", "influxd.pid")
        # Try pidfile first.
        try:
            cls._kill_process_by_pidfile(pidfile)
        except (FileNotFoundError, ValueError, ProcessLookupError):
            pass
        # Fallback: kill any influxdb3 process matching this version's data dir.
        data_dir = os.path.join(fq_base, "influxdb", ver, "data")
        import subprocess
        try:
            result = subprocess.run(
                ["pgrep", "-f", f"influxdb3.*{data_dir}"],
                capture_output=True, text=True, timeout=5)
            for line in result.stdout.strip().splitlines():
                pid = int(line.strip())
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        # Wait up to 5s for the port to become unreachable.
        cfg = next((c for c in cls.influx_version_configs() if c.version == ver), None)
        if cfg:
            import socket
            deadline = time.time() + 5
            while time.time() < deadline:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(0.5)
                try:
                    s.connect((cfg.host, cfg.port))
                    s.close()
                    time.sleep(0.3)
                except (ConnectionRefusedError, OSError):
                    s.close()
                    return
            # Last resort: SIGKILL
            try:
                result = subprocess.run(
                    ["pgrep", "-f", f"influxdb3.*{data_dir}"],
                    capture_output=True, text=True, timeout=5)
                for line in result.stdout.strip().splitlines():
                    pid = int(line.strip())
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

    @classmethod
    def start_influx_instance(cls, ver, wait_s=10):
        """Start the InfluxDB instance for the given version and wait until ready."""
        import subprocess, time, requests
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "ensure_ext_env.sh")
        # Strip ASAN/TDengine library vars so influxdb3 runs cleanly.
        env = _fq_subprocess_env()
        fq_base = _default_fq_base_dir()
        influx_bin_dir = os.path.join(fq_base, "influxdb", ver, "bin")
        has_influx_install = (
            os.path.isfile(os.path.join(influx_bin_dir, "influxdb3"))
            or os.path.isfile(os.path.join(influx_bin_dir, "influxd"))
        )
        # Reset only InfluxDB (not MySQL/PG) to avoid disrupting other
        # services and to prevent accidental signal delivery to taosd.
        # InfluxDB 3.x still gets a full hard reset (kill → wipe → restart)
        # because its append-only IOx catalog overflows without a data wipe.
        env["FQ_SERVICES_TO_RESET"] = "influx"
        if has_influx_install:
            # Quick restart: skip the IOx data-dir wipe so InfluxDB is started
            # in-place.  This avoids the double-restart that ensure_influx's
            # hard-reset path causes when InfluxDB is already stopped:
            # (start → wait → kill → wipe → restart) was sometimes leaving
            # the second instance in a state where it crashed shortly after
            # startup, causing cascade failures in subsequent tests.
            env["FQ_INFLUX_QUICK_RESTART"] = "1"
        script_timeout = int(os.getenv("FQ_INFLUX_SCRIPT_TIMEOUT_S", "600"))
        script_error = None
        try:
            subprocess.run(["bash", script],
                           env=env, check=True, capture_output=False,
                           timeout=script_timeout)
        except subprocess.CalledProcessError as exc:
            script_error = exc
        cfg = next(c for c in cls.influx_version_configs() if c.version == ver)
        deadline = time.time() + wait_s
        while time.time() < deadline:
            try:
                r = requests.get(f"http://{cfg.host}:{cfg.port}/health",
                                 timeout=2)
                # Accept 200 (no-auth) or 401 (auth-enabled — server is up
                # but requires credentials).  Mirrors ensure_ext_env.sh logic.
                if r.status_code in (200, 401):
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"InfluxDB {ver} did not become ready within {wait_s}s")

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
        """Execute SQL on a specific MySQL version instance.

        The session timezone is NOT overridden here; MySQL uses its server
        default (SYSTEM, typically matching the taosd host timezone, e.g. CST).
        This keeps TIMESTAMP storage consistent with what the TDengine
        ext-connector reads back: the FQ connector sets UTC on its own
        connection, so a CST-stored TIMESTAMP is returned as UTC string, then
        taosParseTime uses the taosd local timezone (CST) to re-interpret it,
        and the resulting TDengine epoch matches what callers expect when they
        insert a local-time literal.
        Note: DATETIME columns are timezone-naive and always round-trip as
        their literal string value regardless of session timezone.
        """
        import pymysql
        for attempt in range(2):
            try:
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
                return
            except Exception as e:
                if attempt == 0 and cls._is_retriable_ext_conn_error(e):
                    tdLog.info(f"[FQ revive] mysql_exec_cfg transient failure, retry after revive: {e}")
                    try:
                        cls.revive_if_dead()
                    except RuntimeError:
                        pass
                    continue
                raise

    @classmethod
    def mysql_create_db_cfg(cls, cfg, db):
        """Create MySQL database on a specific version instance (idempotent)."""
        cls.mysql_exec_cfg(cfg, None, [
            f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4"])

    @classmethod
    def mysql_drop_db_cfg(cls, cfg, db):
        """Drop MySQL database on a specific version instance (idempotent)."""
        cls.mysql_exec_cfg(cfg, None, [f"DROP DATABASE IF EXISTS `{db}`"])

    @classmethod
    def mysql_kill_sleeping_connections_cfg(cls, cfg):
        """Kill all sleeping (idle) connections on a specific MySQL instance.

        TDengine keeps external-source connections open after queries; calling
        this before each test prevents 'Too many connections' errors.
        """
        import pymysql
        conn = pymysql.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            connect_timeout=10)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM information_schema.processlist "
                    "WHERE command = 'Sleep' AND id <> CONNECTION_ID()")
                rows = cur.fetchall()
            for (pid,) in rows:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"KILL CONNECTION {pid}")
                except Exception:
                    pass
        finally:
            conn.close()

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
        import re
        for attempt in range(2):
            try:
                conn = psycopg2.connect(
                    host=cfg.host, port=cfg.port,
                    user=cfg.user, password=cfg.password,
                    dbname=database or "postgres")
                conn.autocommit = True
                try:
                    with conn.cursor() as cur:
                        for sql in sqls:
                            try:
                                cur.execute(sql)
                            except psycopg2.errors.ObjectInUse:
                                # Federated PG sessions may still be alive for a short
                                # window after source cleanup; force-terminate and retry
                                # DROP DATABASE to keep tests deterministic.
                                m = re.search(r'DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?"([^"]+)"', sql, re.IGNORECASE)
                                if not m:
                                    raise
                                db_name = m.group(1)
                                cur.execute(
                                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                    "WHERE datname=%s AND pid <> pg_backend_pid()",
                                    (db_name,),
                                )
                                cur.execute(sql)
                finally:
                    conn.close()
                return
            except Exception as e:
                if attempt == 0 and cls._is_retriable_ext_conn_error(e):
                    tdLog.info(f"[FQ revive] pg_exec_cfg transient failure, retry after revive: {e}")
                    try:
                        cls.revive_if_dead()
                    except RuntimeError:
                        pass
                    continue
                raise

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
        tok = cls._get_influx_token(cls.INFLUX_VERSIONS[0])
        auth = {"Authorization": f"Bearer {tok}"} if tok else {}
        r = requests.get(url, headers=auth, params={"format": "json"}, timeout=5)
        if r.status_code == 200:
            if any(d.get("iox::database") == bucket for d in r.json()):
                return  # already exists
        elif r.status_code not in (404,):
            r.raise_for_status()
        r_create = requests.post(url, headers=auth, json={"db": bucket}, timeout=5)
        if r_create.status_code not in (200, 201):
            r_create.raise_for_status()

    @classmethod
    def influx_drop_db(cls, bucket):
        """Drop InfluxDB v3 database (idempotent)."""
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v3/configure/database"
        tok = cls._get_influx_token(cls.INFLUX_VERSIONS[0])
        auth = {"Authorization": f"Bearer {tok}"} if tok else {}
        r = requests.delete(url, headers=auth, params={"db": bucket}, timeout=5)
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
        tok = cls._get_influx_token(cls.INFLUX_VERSIONS[0])
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if tok:
            headers["Authorization"] = f"Bearer {tok}"
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
        tok = cls._get_influx_token(cls.INFLUX_VERSIONS[0])
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        if tok:
            headers["Authorization"] = f"Bearer {tok}"
        payload = {"db": bucket, "q": sql, "format": fmt}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    @classmethod
    def influx_create_db_cfg(cls, cfg, bucket):
        """Create InfluxDB v3 database on a specific version instance (idempotent)."""
        import requests
        url = f"http://{cfg.host}:{cfg.port}/api/v3/configure/database"
        headers = {"Authorization": f"Bearer {cfg.token}"} if cfg.token else {}
        r = requests.get(url, headers=headers, params={"format": "json"}, timeout=5)
        if r.status_code == 200:
            if any(d.get("iox::database") == bucket for d in r.json()):
                return
        elif r.status_code not in (404,):
            r.raise_for_status()
        r_create = requests.post(url, headers=headers, json={"db": bucket}, timeout=5)
        if r_create.status_code not in (200, 201):
            r_create.raise_for_status()

    @classmethod
    def influx_drop_db_cfg(cls, cfg, bucket):
        """Drop InfluxDB v3 database on a specific version instance (idempotent).

        Note: InfluxDB 3.x (IOx) may exit shortly after responding to a
        DELETE /api/v3/configure/database request.  This method detects such
        crashes and automatically restarts InfluxDB so that subsequent tests
        can continue without a cascade failure.
        """
        import requests, time
        url = f"http://{cfg.host}:{cfg.port}/api/v3/configure/database"
        headers = {"Authorization": f"Bearer {cfg.token}"} if cfg.token else {}
        _crashed = False
        try:
            r = requests.delete(url, headers=headers, params={"db": bucket}, timeout=5)
            if r.status_code not in (200, 204, 404):
                r.raise_for_status()
        except requests.exceptions.ConnectionError:
            _crashed = True  # already not responding when DELETE was sent
        if not _crashed:
            # InfluxDB 3.x (IOx) sometimes crashes *after* responding to the
            # DELETE.  Give it a brief moment to settle, then verify health.
            time.sleep(0.5)
            try:
                requests.get(f"http://{cfg.host}:{cfg.port}/health", timeout=3)
            except requests.exceptions.ConnectionError:
                _crashed = True
        if _crashed:
            # Restart InfluxDB in-place (quick restart, no data wipe) so that
            # the next operation against InfluxDB succeeds.
            try:
                cls.start_influx_instance(cfg.version)
            except Exception:
                pass

    @classmethod
    def influx_write_cfg(cls, cfg, bucket, lines, precision='ns'):
        """Write line-protocol data to a specific InfluxDB v3 instance.

        lines: list of line-protocol strings, or a single pre-joined string.
        precision: timestamp precision in line protocol ('ns', 'us', 'ms', 's').
                   Defaults to 'ns' to match standard InfluxDB line protocol.
        """
        import requests
        data = lines if isinstance(lines, str) else "\n".join(lines)
        if not data.strip():
            return  # nothing to write
        url = f"http://{cfg.host}:{cfg.port}/api/v2/write"
        params = {"bucket": bucket, "precision": precision}
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if cfg.token:
            headers["Authorization"] = f"Bearer {cfg.token}"
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
        if cfg.token:
            headers["Authorization"] = f"Bearer {cfg.token}"
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

    # Maps a Remote SQL keyword to the local TDengine plan operator that must be
    # ABSENT from the local plan when that keyword is confirmed pushed to remote.
    # Absence proves the operator was not retained locally for re-computation.
    # "Sort " uses a trailing space to avoid matching compound names like
    # "FederatedSortScan".  "Filter" / "Agg" / "Join" are TDengine operator prefixes.
    _PUSHDOWN_LOCAL_OP_MAP = {
        "WHERE":    "Filter",
        "ORDER BY": "Sort ",   # trailing space avoids false matches
        "GROUP BY": "Agg",
        "COUNT":    "Agg",
        "SUM":      "Agg",
        "AVG":      "Agg",
        "MIN":      "Agg",
        "MAX":      "Agg",
        "HAVING":   "Agg",
        "JOIN":     "Join",
    }

    # ------------------------------------------------------------------
    # Source lifecycle helpers
    # ------------------------------------------------------------------

    def _cleanup_src(self, *names):
        """Drop external sources by name (idempotent)."""
        for n in names:
            tdSql.execute(f"drop database if exists {n}")
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
        tdSql.execute(f"drop database if exists {name}")
        tdSql.execute(f"drop external source if exists {name}")
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
        tdSql.execute(f"drop database if exists {name}")
        tdSql.execute(f"drop external source if exists {name}")
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
        tdSql.execute(f"drop database if exists {name}")
        tdSql.execute(f"drop external source if exists {name}")
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

    def _with_std_sources(self, prefix, body_fn, *,
                          table="src_t",
                          skip_mysql=False, skip_pg=False, skip_influx=False):
        """Create standard 5-row test data in MySQL / PG / InfluxDB; call body_fn(src) for each.

        Standard table schema (``src_t`` by default):
            MySQL  : ts DATETIME(3) PK, val INT, score DOUBLE, name VARCHAR(32), flag TINYINT(1)
            PG     : ts TIMESTAMP PK,   val INT, score DOUBLE PRECISION, name VARCHAR(32), flag INT
            InfluxDB: measurement with val/score/flag as numeric fields, name as string field.

        ``body_fn(src_name: str) -> None`` receives the external source name and should
        execute the same SQL/assertions against ``{src_name}.{table}``.

        Each source is created, tested, and cleaned up sequentially.
        """
        m_src = f"{prefix}_m"
        p_src = f"{prefix}_p"
        i_src = f"{prefix}_i"
        m_db  = f"{prefix}_mdb"
        p_db  = f"{prefix}_pdb"
        i_db  = f"{prefix}_idb"
        rows_sql = ", ".join(
            f"('{_ms_to_dt(ts)}', {val}, {score}, '{name}', {flag})"
            for ts, val, score, name, flag in _STD_ROWS
        )

        # ----- MySQL -----
        if not skip_mysql:
            self._cleanup_src(m_src)
            ExtSrcEnv.mysql_kill_sleeping_connections_cfg(self._mysql_cfg())
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            try:
                ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                    f"DROP TABLE IF EXISTS `{table}`",
                    f"CREATE TABLE `{table}` ("
                    f"  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
                    f"  name VARCHAR(32), flag TINYINT(1))",
                    f"INSERT INTO `{table}` VALUES {rows_sql}",
                ])
                self._mk_mysql_real(m_src, database=m_db)
                body_fn(m_src)
            finally:
                self._cleanup_src(m_src)
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
                except Exception:
                    pass

        # ----- PostgreSQL -----
        if not skip_pg:
            self._cleanup_src(p_src)
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            try:
                ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                    f"DROP TABLE IF EXISTS public.{table}",
                    f"CREATE TABLE public.{table} ("
                    f"  ts TIMESTAMP PRIMARY KEY, val INT,"
                    f"  score DOUBLE PRECISION, name VARCHAR(32), flag INT)",
                    f"INSERT INTO public.{table} VALUES {rows_sql}",
                ])
                self._mk_pg_real(p_src, database=p_db, schema="public")
                body_fn(p_src)
            finally:
                self._cleanup_src(p_src)
                try:
                    ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
                except Exception:
                    pass

        # ----- InfluxDB -----
        if not skip_influx:
            self._cleanup_src(i_src)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            try:
                lines = [
                    f'{table} val={val}i,score={score},name="{name}",flag={flag}i '
                    f'{ts * 1_000_000}'
                    for ts, val, score, name, flag in _STD_ROWS
                ]
                ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, lines)
                self._mk_influx_real(i_src, database=i_db)
                body_fn(i_src)
            finally:
                self._cleanup_src(i_src)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
                except Exception:
                    pass

    def _with_custom_sources(self, prefix, body_fn, *,
                             mysql_setup=None, pg_setup=None, influx_lines=None,
                             mysql_table=None, pg_table=None, influx_table=None,
                             skip_mysql=False, skip_pg=False, skip_influx=False):
        """Create custom data in MySQL / PG / InfluxDB; call body_fn(src, db_type) for each.

        Unlike ``_with_std_sources`` which writes a fixed 5-row ``src_t`` table,
        this method lets the caller specify arbitrary DDL/DML per source.

        Args:
            prefix:       Unique name prefix for external sources and databases.
            body_fn:      ``body_fn(src_name: str, db_type: str) -> None``.
                          *db_type* is ``'mysql'``, ``'pg'``, or ``'influx'``.
            mysql_setup:  List of SQL strings to execute in MySQL (DDL + INSERT).
            pg_setup:     List of SQL strings to execute in PostgreSQL.
            influx_lines: List of InfluxDB line-protocol strings.
            mysql_table:  Table name used when querying MySQL (for docs only).
            pg_table:     Table name used when querying PG.
            influx_table: Measurement name used when querying InfluxDB.
            skip_mysql / skip_pg / skip_influx: Skip specific sources.
        """
        m_src = f"{prefix}_m"
        p_src = f"{prefix}_p"
        i_src = f"{prefix}_i"
        m_db  = f"{prefix}_mdb"
        p_db  = f"{prefix}_pdb"
        i_db  = f"{prefix}_idb"

        # ----- MySQL -----
        if not skip_mysql and mysql_setup:
            self._cleanup_src(m_src)
            ExtSrcEnv.mysql_kill_sleeping_connections_cfg(self._mysql_cfg())
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            try:
                ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, mysql_setup)
                self._mk_mysql_real(m_src, database=m_db)
                body_fn(m_src, 'mysql')
            finally:
                self._cleanup_src(m_src)
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
                except Exception:
                    pass

        # ----- PostgreSQL -----
        if not skip_pg and pg_setup:
            self._cleanup_src(p_src)
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            try:
                ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, pg_setup)
                self._mk_pg_real(p_src, database=p_db, schema="public")
                body_fn(p_src, 'pg')
            finally:
                self._cleanup_src(p_src)
                try:
                    ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
                except Exception:
                    pass

        # ----- InfluxDB -----
        if not skip_influx and influx_lines:
            self._cleanup_src(i_src)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
            try:
                ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, influx_lines)
                self._mk_influx_real(i_src, database=i_db)
                body_fn(i_src, 'influx')
            finally:
                self._cleanup_src(i_src)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
                except Exception:
                    pass

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

    def _verify_pushdown_explain(self, sql, *remote_kws):
        """EXPLAIN *sql* and verify that pushdown occurred via three checks:

        1. **FederatedScan** is present in the plan — proves the query reached
           the external source (a prerequisite for any meaningful pushdown check).

        2. **Remote SQL keywords** — every keyword in *remote_kws* must appear
           (case-insensitively) inside the ``Remote SQL: …`` line.  This confirms
           each clause was included in the query sent to the external database.

        3. **Absent local operators** — for each keyword that maps to a TDengine
           local plan operator via ``_PUSHDOWN_LOCAL_OP_MAP``, that operator name
           must NOT appear in any plan line *outside* the ``Remote SQL:`` line.
           Its absence proves the clause was not retained locally for re-execution
           by TDengine — i.e., the pushdown actually took effect.

        **Why not check Remote SQL existence?**  Remote SQL always appears for any
        federated query, even when zero clauses are pushed down (TDengine still
        must fetch raw rows from the external source).  Its mere presence carries
        no information about pushdown success.

        Behavior is controlled by the ``FQ_EXPLAIN_STRICT`` environment variable
        (read on every call so it can be changed between tests without reloading):

        * ``FQ_EXPLAIN_STRICT=1`` / ``true`` / ``yes`` — strict mode: any failure
          raises ``AssertionError`` and fails the test immediately.
        * Anything else (default) — non-strict mode: failures are logged as
          warnings and the calling test continues.

        Args:
            sql:        Query string to EXPLAIN (without the leading ``explain``).
            *remote_kws: Keywords that must appear in Remote SQL.  Each keyword
                         also triggers an absent-local-operator check when a
                         mapping exists in ``_PUSHDOWN_LOCAL_OP_MAP``.
        """
        strict = os.getenv("FQ_EXPLAIN_STRICT", "0").strip().lower() in (
            "1", "true", "yes"
        )

        def _fail(msg):
            if strict:
                raise AssertionError(msg)
            # Non-strict mode: silently skip (Phase 2 pushdown not yet implemented)

        ok = tdSql.query(f"explain verbose true {sql}", exit=False)
        if ok is False:
            _fail(f"[EXPLAIN] query returned error for: {sql!r}")
            return

        # Flatten all plan output into a list of strings (one per EXPLAIN row).
        lines = [
            str(col)
            for row in (tdSql.queryResult or [])
            for col in row
            if col is not None
        ]

        # 1. FederatedScan must be present — proof the external source was queried.
        if not any("FederatedScan" in line or "Federated Scan" in line for line in lines):
            _fail(
                f"[EXPLAIN] FederatedScan not found in plan — not a federated query?\n"
                f"  SQL:  {sql!r}\n"
                f"  Plan: {lines}"
            )
            return

        # 2. Remote SQL line (always present in federated queries).
        remote_sql_line = next((l for l in lines if "Remote SQL:" in l), "")
        if not remote_sql_line:
            _fail(
                f"[EXPLAIN] 'Remote SQL:' line missing — unexpected for a federated query\n"
                f"  SQL:  {sql!r}\n"
                f"  Plan: {lines}"
            )
            return

        # Local-plan lines: everything except the Remote SQL line itself.
        # An operator keyword appearing here means TDengine is executing it locally.
        local_plan_lines = [l for l in lines if "Remote SQL:" not in l]

        # 3. Per-keyword: Remote SQL content check + corresponding local operator check.
        for kw in remote_kws:
            if kw.upper() not in remote_sql_line.upper():
                _fail(
                    f"[EXPLAIN] Remote SQL missing expected keyword '{kw}'\n"
                    f"  SQL:        {sql!r}\n"
                    f"  Remote SQL: {remote_sql_line}"
                )
                # Continue to remaining keywords even in non-strict mode.

            local_op = self._PUSHDOWN_LOCAL_OP_MAP.get(kw.upper())
            if local_op:
                offending = [l for l in local_plan_lines if local_op in l]
                if offending:
                    _fail(
                        f"[EXPLAIN] Local plan still has '{local_op}' operator — "
                        f"'{kw}' was not fully pushed to remote\n"
                        f"  SQL:             {sql!r}\n"
                        f"  Offending lines: {offending}\n"
                        f"  Remote SQL:      {remote_sql_line}"
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
            pytest.fail("external source feature is unavailable in current build")
        # Ensure federatedQueryEnable is active in this client process.
        # taos_init reads psim/cfg/taos.cfg (which has federatedQueryEnable 1),
        # but call alter local as a belt-and-suspenders guarantee.
        try:
            tdSql.execute('alter local "federatedQueryEnable" "1"')
        except Exception as e:
            tdLog.info(f"[WARN] alter local federatedQueryEnable failed: {e}")

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


# =====================================================================
# Parity test framework
# =====================================================================

class QueryError(AssertionError):
    """Carries structured error information from a failed query."""
    def __init__(self, errno: Optional[int], err_info: Optional[str], sql: str, raw: Exception):
        self.qerrno   = errno
        self.err_info = err_info
        self.sql      = sql
        detail = ""
        if errno is not None:
            detail += f"\n  errno:      {errno:#010x}"
        if err_info:
            detail += f"\n  error_info: {err_info}"
        super().__init__(
            f"Query execution failed{detail}\n"
            f"  sql: {sql}\n"
            f"  raw exception: {raw}"
        )


def parity_sql_val(v):
    """Format a Python value as a SQL literal (NULL for None, quoted for str)."""
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return f"'{v}'"
    return str(v)


def _ns_int_to_datetime(ns_val, tz=None):
    """Convert a nanosecond epoch integer to datetime.

    *tz*: if given, produce a tz-aware datetime then strip tzinfo so the
    result is comparable with the naive datetimes returned by the TDengine
    connector.  When *tz* is ``None`` (default), use the OS local timezone
    (backward compatible).
    """
    try:
        if tz is not None:
            return _datetime.datetime.fromtimestamp(ns_val / 1e9, tz=tz).replace(tzinfo=None)
        return _datetime.datetime.fromtimestamp(ns_val / 1e9)
    except (OSError, OverflowError, ValueError):
        return None


def _us_int_to_datetime(us_val, tz=None):
    """Convert a microsecond epoch integer to datetime.

    See :func:`_ns_int_to_datetime` for the *tz* parameter semantics.
    """
    try:
        if tz is not None:
            return _datetime.datetime.fromtimestamp(us_val / 1e6, tz=tz).replace(tzinfo=None)
        return _datetime.datetime.fromtimestamp(us_val / 1e6)
    except (OSError, OverflowError, ValueError):
        return None


def parity_ts_eq(a, b, tz=None):
    """Compare two timestamp values that may differ in representation.

    Handles: datetime vs datetime, datetime vs raw ns/µs int, int vs int.
    The Python connector returns raw int for nanosecond-precision timestamps.

    *tz*: timezone for epoch→datetime conversion (see :func:`_ns_int_to_datetime`).
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if str(a) == str(b):
        return True
    # One is datetime, other is int (ns or µs from external source)
    dt_val = int_val = None
    if isinstance(a, _datetime.datetime) and isinstance(b, int):
        dt_val, int_val = a, b
    elif isinstance(b, _datetime.datetime) and isinstance(a, int):
        dt_val, int_val = b, a
    else:
        return False
    # Try ns first (InfluxDB), then µs (MySQL/PG)
    # Strip tzinfo from dt_val so it can be compared with the naive datetime
    # returned by _ns_int_to_datetime / _us_int_to_datetime (which already
    # call .replace(tzinfo=None)).  The UTC-aware datetime returned by taospy
    # when the server runs in UTC mode would otherwise never match the naive
    # converted value even when the wall-clock time is identical.
    dt_val_naive = dt_val.replace(tzinfo=None) if dt_val.tzinfo is not None else dt_val
    for conv in (_ns_int_to_datetime, _us_int_to_datetime):
        converted = conv(int_val, tz=tz)
        if converted is not None and dt_val_naive == converted:
            return True
    return False


def parity_float_eq(a, b, tol=1e-4):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(str(a)) - float(str(b))) <= tol
    except (TypeError, ValueError):
        return str(a) == str(b)


# Precision scaling factors relative to local TDengine (ms precision=0).
# MySQL/PG use µs (precision=1) → factor 1000; InfluxDB uses ns (precision=2) → factor 1000000.
_EXT_PRECISION_FACTOR = {"MySQL": 1000, "PG": 1000, "InfluxDB": 1000000}

import re as _re


def parity_precision_eq(lv, ev, label, tz=None):
    """Compare values expected to differ by precision scaling (DS §5.3.12.3).

    Handles:
    - Integers/floats scaled by precision factor (_wduration, ELAPSED, CAST BIGINT)
    - ISO-8601 strings with different fractional digits (TO_ISO8601)
    - Datetimes differing at sub-precision boundary (_QEND)
    - Datetime vs raw int (InfluxDB ns-precision timestamps)

    *tz*: timezone for epoch→datetime conversion (see :func:`_ns_int_to_datetime`).
    """
    if lv is None and ev is None:
        return True
    if lv is None or ev is None:
        return False
    if str(lv) == str(ev):
        return True
    factor = _EXT_PRECISION_FACTOR.get(label, 1)

    # ── Integer/float scaling: ext_val == local_val * factor ──
    try:
        lf = float(str(lv))
        ef = float(str(ev))
        if lf == 0.0 and ef == 0.0:
            return True
        if lf != 0.0:
            ratio = ef / lf
            if abs(ratio - factor) < 1e-6:
                return True
    except (TypeError, ValueError):
        pass

    # ── ISO-8601 fractional-seconds normalization ──
    # e.g. ".000+0000" vs ".000000+0000" vs ".000000000+0000"
    ls, es = str(lv), str(ev)
    m1 = _re.match(r'^(.+\.\d*?)0*([+-Z].*)$', ls)
    m2 = _re.match(r'^(.+\.\d*?)0*([+-Z].*)$', es)
    if m1 and m2:
        if m1.group(1) + m1.group(2) == m2.group(1) + m2.group(2):
            return True

    # ── Datetime: truncate both to ms precision and compare ──
    # Strip tzinfo so that offset-aware and offset-naive datetimes that represent
    # the same wall-clock instant compare equal (UTC server returns aware values).
    if isinstance(lv, _datetime.datetime) and isinstance(ev, _datetime.datetime):
        lv_ms = lv.replace(microsecond=(lv.microsecond // 1000) * 1000, tzinfo=None)
        ev_ms = ev.replace(microsecond=(ev.microsecond // 1000) * 1000, tzinfo=None)
        if lv_ms == ev_ms:
            return True

    # ── Datetime vs raw int (InfluxDB ns) ──
    # Use fromtimestamp to get datetime matching TDengine client timezone output.
    if isinstance(lv, _datetime.datetime) and isinstance(ev, int):
        try:
            if factor >= 1000000:  # ns
                sec, nsec = divmod(ev, 1_000_000_000)
                usec = nsec // 1000
            else:  # µs
                sec, usec = divmod(ev, 1_000_000)
            if tz is not None:
                ev_dt = _datetime.datetime.fromtimestamp(sec, tz=tz).replace(
                    tzinfo=None, microsecond=usec)
            else:
                ev_dt = _datetime.datetime.fromtimestamp(sec).replace(
                    microsecond=usec)
            # Strip tzinfo from lv so offset-aware (UTC server) compares with naive ev_dt.
            lv_ms = lv.replace(microsecond=(lv.microsecond // 1000) * 1000, tzinfo=None)
            ev_ms = ev_dt.replace(microsecond=(ev_dt.microsecond // 1000) * 1000)
            if lv_ms == ev_ms:
                return True
        except (OSError, OverflowError, ValueError):
            pass

    return False


def parity_serialize_cell(val, col_idx, float_cols, dynamic_cols=None):
    """Serialize a single cell value to a stable string representation."""
    if val is None:
        return "NULL"
    if dynamic_cols and col_idx in dynamic_cols:
        return "<DYNAMIC>"
    if col_idx in float_cols:
        try:
            return f"{float(str(val)):.6g}"
        except (TypeError, ValueError):
            pass
    # Strip tzinfo from datetime so the serialized string matches the baseline
    # format generated with naive datetimes (UTC server returns offset-aware values).
    if isinstance(val, _datetime.datetime) and val.tzinfo is not None:
        val = val.replace(tzinfo=None)
    return str(val)


def parity_serialize_case(case_id, sql_template, positive, ref_rows, local_qerr, float_cols, ordered, dynamic_cols=None):
    """Serialize local result of one parity case to a canonical text block."""
    kind_tag = "POS" if positive else "NEG"
    lines = [f"### {case_id} {kind_tag}", f"SQL: {sql_template}"]
    if local_qerr is not None:
        errno = local_qerr.qerrno
        err_info = local_qerr.err_info or ""
        lines.append(f"ERROR {errno if errno is not None else 0:#010x}: {err_info}")
    else:
        lines.append("RESULT")
        for row in ref_rows:
            cells = [parity_serialize_cell(v, ci, float_cols, dynamic_cols) for ci, v in enumerate(row)]
            lines.append("|".join(cells))
    lines.append("---")
    return "\n".join(lines)


def parity_make_insert_sqls(rows_dt, *, table="parity_t", schema=""):
    """Generate INSERT statements for a parity table given rows in any order."""
    tbl = f"{schema}.{table}" if schema else table
    return [
        f"INSERT INTO {tbl} VALUES ({', '.join(parity_sql_val(x) for x in r)})"
        for r in rows_dt
    ]


class ParityTestBase(FederatedQueryTestMixin):
    """Base class for parity tests that compare local TDengine results
    against one or more external sources.

    Subclasses must define:
      - ``_local_tbl`` property: fully-qualified local table path
      - ``_ext_sources()`` method: returns ``[(label, tbl_expr), ...]``
      - ``_BASELINE_FILE``: path to baseline file (mandatory)
      - ``_FLOAT_TOL``: float comparison tolerance (default 1e-4)
      - ``_PARITY_CASES``: flat list of ``(case_id, sql_template, opts)``
    """

    _FLOAT_TOL = 1e-4
    _BASELINE_FILE: str  # subclasses MUST set to a real path
    _PARITY_CASES: List[Tuple[str, str, dict]] = []
    _PARITY_TZ = None  # Fixed timezone for epoch→datetime; None = OS local tz

    @property
    def _local_tbl(self):
        raise NotImplementedError

    def _ext_sources(self):
        """Return list of (label, tbl_expr) for external sources."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_col_names():
        """Return current cursor column names as a list (best effort)."""
        desc = getattr(tdSql.cursor, "description", None)
        if not desc:
            return []
        cols = []
        for d in desc:
            try:
                cols.append(str(d[0]))
            except Exception:
                cols.append("")
        return cols

    def _get_rows(self, sql):
        """Execute *sql* once and return results as a list of tuples.

        On failure raises QueryError.
        """
        try:
            tdSql.cursor.execute(sql)
            tdSql.queryResult = tdSql.cursor.fetchall()
            tdSql.queryRows = len(tdSql.queryResult)
            tdSql.queryCols = len(tdSql.cursor.description)
        except Exception as e:
            _eargs = getattr(e, 'args', ())
            errno = None
            if len(_eargs) >= 2 and isinstance(_eargs[-1], int):
                errno = _eargs[-1]
            if errno is None:
                errno = getattr(e, 'errno', None)
            err_info = None
            if _eargs:
                err_info = str(_eargs[0])
            tdLog.debug(f"expected SQL failure: {sql!r} → {err_info!r} (errno={errno})")
            raise QueryError(errno, err_info, sql, e) from e
        return list(tdSql.queryResult)

    # ------------------------------------------------------------------
    # Result comparison helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fmt_result_tables(ref_rows, ext_rows, ref_sql, cmp_sql, label):
        """Return a formatted side-by-side diff of *ref_rows* vs *ext_rows*."""
        lines = [
            f"  local_sql  : {ref_sql}",
            f"  {label}_sql    : {cmp_sql}",
            f"  local rows : {len(ref_rows)}  {label} rows: {len(ext_rows)}",
        ]
        n_rows = max(len(ref_rows), len(ext_rows))
        for r in range(n_rows):
            lr = tuple(ref_rows[r]) if r < len(ref_rows) else ()
            er = tuple(ext_rows[r]) if r < len(ext_rows) else ()
            n_cols = max(len(lr), len(er))
            cells = []
            for c in range(n_cols):
                lv = lr[c] if c < len(lr) else "<missing>"
                ev = er[c] if c < len(er) else "<missing>"
                mark = "" if str(lv) == str(ev) else " \u2717"
                cells.append(f"col{c}[local={lv!r} {label}={ev!r}]{mark}")
            lines.append(f"  row[{r:02d}]: " + "  ".join(cells))
        return "\n".join(lines)

    def _compare_rows(self, ref, rows, ref_sql, cmp_sql, label, float_cols,
                      precision_cols=None, dynamic_cols=None):
        """Row-by-row comparison with full diff on mismatch."""
        precision_cols = precision_cols or set()
        dynamic_cols = dynamic_cols or set()
        if len(ref) != len(rows):
            raise AssertionError(
                f"{label} row count mismatch: local={len(ref)} {label}={len(rows)}\n"
                + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
            )
        for ri, (lr, er) in enumerate(zip(ref, rows)):
            if len(lr) != len(er):
                raise AssertionError(
                    f"{label} col count mismatch at row {ri}: "
                    f"local={len(lr)} {label}={len(er)}\n"
                    + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
                )
            for ci, (lv, ev) in enumerate(zip(lr, er)):
                if ci in dynamic_cols:
                    # Convert nanosecond int to datetime for comparison
                    _tz = self._PARITY_TZ
                    def _to_dt(v, tz=_tz):
                        if isinstance(v, _datetime.datetime):
                            return v.replace(tzinfo=None)
                        if isinstance(v, (int, float)) and v > 1e15:
                            if tz is not None:
                                return _datetime.datetime.fromtimestamp(v / 1e9, tz=tz).replace(tzinfo=None)
                            return _datetime.datetime.fromtimestamp(v / 1e9)
                        return None
                    lv_dt, ev_dt = _to_dt(lv), _to_dt(ev)
                    if lv_dt is not None and ev_dt is not None:
                        if abs((lv_dt - ev_dt).total_seconds()) <= 120:
                            continue
                    ok = (lv is None and ev is None) or (str(lv) == str(ev))
                elif ci in float_cols:
                    ok = parity_float_eq(lv, ev, self._FLOAT_TOL)
                elif ci in precision_cols:
                    ok = parity_precision_eq(lv, ev, label, tz=self._PARITY_TZ)
                else:
                    ok = (str(lv) == str(ev)) or (lv is None and ev is None)
                    if not ok:
                        ok = parity_ts_eq(lv, ev, tz=self._PARITY_TZ)
                if not ok:
                    raise AssertionError(
                        f"{label} value mismatch at row={ri} col={ci}: "
                        f"local={lv!r} {label}={ev!r}\n"
                        + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
                    )

    def _assert_parity_all(self, sql_template, *, float_cols=None, ordered=True):
        """Execute *sql_template* against all sources and compare."""
        float_cols = float_cols or set()
        local_sql = sql_template.format(tbl=self._local_tbl)
        ref = self._get_rows(local_sql)
        if not ordered:
            ref = sorted(ref, key=lambda r: [str(x) for x in r])
        for lbl, tbl in self._ext_sources():
            sql = sql_template.format(tbl=tbl)
            rows = self._get_rows(sql)
            if not ordered:
                rows = sorted(rows, key=lambda r: [str(x) for x in r])
            self._compare_rows(ref, rows, local_sql, sql, lbl, float_cols)

    # ------------------------------------------------------------------
    # Case runner
    # ------------------------------------------------------------------

    def _run_one_case(self, case_id: str, sql_template: str, **kwargs) -> tuple:
        """Run one parity case and return ``(passed, details, serialized)``."""
        positive        = kwargs.get("positive", True)
        reason          = kwargs.get("reason", "")
        float_cols      = kwargs.get("float_cols") or set()
        ordered         = kwargs.get("ordered", True)
        source_expected = kwargs.get("source_expected") or {}
        validate_in     = kwargs.get("validate_in")
        precision_cols  = kwargs.get("precision_cols") or set()
        dynamic_cols    = set(kwargs.get("dynamic_cols") or set())
        day_start_cols  = set(kwargs.get("day_start_cols") or set())
        dynamic_cols   |= day_start_cols
        kind_tag   = "POS" if positive else "NEG"
        sql_short  = sql_template if len(sql_template) <= 90 else sql_template[:87] + "..."
        prefix     = f"[{case_id:<9s} {kind_tag}]"
        t0 = _time.monotonic()
        if not positive and reason:
            tdLog.info(f"{prefix}  reason: {reason}")

        # ── local reference ──
        local_sql        = sql_template.format(tbl=self._local_tbl)
        local_qerr: Optional[QueryError] = None
        ref = None
        try:
            ref = self._get_rows(local_sql)
            if not ordered:
                ref = sorted(ref, key=lambda r: [str(x) for x in r])
        except QueryError as exc:
            local_qerr = exc

        # ── validate_in mode ──
        if validate_in is not None:
            return self._run_validate_in(
                case_id, sql_template, prefix, sql_short, t0,
                ref, local_qerr, validate_in,
            )

        # ── dynamic time-range validation (NOW / TODAY) ──
        if dynamic_cols and ref is not None and local_qerr is None:
            t_ref = _datetime.datetime.now()
            for ri, row in enumerate(ref):
                for ci in dynamic_cols:
                    if ci < len(row) and row[ci] is not None:
                        v = row[ci]
                        if isinstance(v, _datetime.datetime):
                            v_naive = v.replace(tzinfo=None) if v.tzinfo is not None else v
                            delta = (t_ref - v_naive).total_seconds()
                            if ci in day_start_cols:
                                in_range = -120 <= delta <= 24 * 3600 + 120
                            else:
                                in_range = abs(delta) <= 120 or v_naive.date() == t_ref.date()
                            if not in_range:
                                elapsed = _time.monotonic() - t0
                                tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
                                tdLog.info(f"  [time-range] col {ci} row {ri}: "
                                           f"value {v} is {abs(delta):.0f}s from current time {t_ref}")
                                serialized = parity_serialize_case(
                                    case_id, sql_template, positive, ref, local_qerr,
                                    float_cols, ordered, dynamic_cols)
                                return False, f"dynamic col {ci} out of time range", serialized

        # ── serialize local result for baseline ──
        serialized = parity_serialize_case(
            case_id, sql_template, positive, ref, local_qerr, float_cols, ordered, dynamic_cols)

        # ── negative-case early-fail ──
        if not positive and local_qerr is None:
            elapsed = _time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            tdLog.info(f"  [neg-expected] local unexpectedly succeeded (expected error)")
            if reason:
                tdLog.info(f"  reason: {reason}")
            return False, "local unexpectedly succeeded", serialized

        # ── compare each external source ──
        _EXT_CONN_FAILED = 0x80006400
        src_failures: List[Tuple[str, str, str]] = []   # (label, err, sql)
        for lbl, tbl in self._ext_sources():
            sql = sql_template.format(tbl=tbl)
            ext_qerr: Optional[QueryError] = None
            rows = None
            try:
                rows = self._get_rows(sql)
                if not ordered:
                    rows = sorted(rows, key=lambda r: [str(x) for x in r])
            except QueryError as exc:
                ext_qerr = exc

            if local_qerr is not None:
                if ext_qerr is None:
                    _le = local_qerr.qerrno
                    src_failures.append((
                        lbl,
                        f"BUG: local errored but [{lbl}] succeeded\n"
                        f"  local errno : {_le if _le is not None else 0:#010x} — {local_qerr.err_info}\n"
                        f"  {lbl} sql   : {sql}",
                        sql,
                    ))
                elif ext_qerr.qerrno != local_qerr.qerrno:
                    _le = local_qerr.qerrno
                    _ee = ext_qerr.qerrno
                    src_failures.append((
                        lbl,
                        f"BUG: errno mismatch\n"
                        f"  local  errno: {_le if _le is not None else 0:#010x} — {local_qerr.err_info}\n"
                        f"  {lbl}   errno: {_ee if _ee is not None else 0:#010x} — {ext_qerr.err_info}\n"
                        f"  {lbl} sql   : {sql}",
                        sql,
                    ))
            else:
                if ext_qerr is not None:
                    src_failures.append((lbl, str(ext_qerr), sql))
                    continue
                try:
                    if lbl in source_expected:
                        expected_rows = list(source_expected[lbl])
                        self._compare_rows(expected_rows, rows, f"expected({lbl})", sql, lbl, float_cols, precision_cols, dynamic_cols)
                    else:
                        self._compare_rows(ref, rows, local_sql, sql, lbl, float_cols, precision_cols, dynamic_cols)
                except AssertionError as exc:
                    src_failures.append((lbl, str(exc), sql))

        if local_qerr is not None and not src_failures:
            elapsed = _time.monotonic() - t0
            _le = local_qerr.qerrno
            tag = "PASS" if not positive else "PASS(err-parity)"
            tdLog.info(f"{prefix} {tag}  {sql_short}  errno={_le if _le is not None else 0:#010x}  [{elapsed:.2f}s]")
            return True, "", serialized

        if local_qerr is not None and src_failures:
            elapsed = _time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            if not positive and reason:
                tdLog.info(f"  [neg-expected] {reason}")
            _le = local_qerr.qerrno
            tdLog.info(f"  [local] errno={_le if _le is not None else 0:#010x} — {local_qerr.err_info}")
            for lbl, err, esql in src_failures:
                tdLog.info(f"  [{lbl}] SQL: {esql}")
                for line in err.splitlines()[:5]:
                    tdLog.info(f"    {line}")
            summary = "; ".join(f"[{lbl}] {err.splitlines()[0]}" for lbl, err, _ in src_failures)
            return False, summary, serialized

        if src_failures:
            elapsed = _time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            if not positive and reason:
                tdLog.info(f"  [neg-expected] {reason}")
            for lbl, err, esql in src_failures:
                tdLog.info(f"  [{lbl}] SQL: {esql}")
                err_lines = err.split("\n")
                tdLog.info(f"    {err_lines[0]}")
                for line in err_lines[1:10]:
                    tdLog.info(f"    {line}")
            summary = "; ".join(
                f"[{lbl}] {err.split(chr(10))[0]}" for lbl, err, _ in src_failures
            )
            return False, summary, serialized

        elapsed = _time.monotonic() - t0
        tdLog.info(f"{prefix} PASS  {sql_short}  [{elapsed:.2f}s]")
        return True, "", serialized

    def _run_validate_in(self, case_id, sql_template, prefix, sql_short, t0,
                          ref, local_qerr, valid_values):
        """Validate every returned value is in *valid_values* (non-deterministic funcs)."""
        # Serialize for baseline — sorted valid_values for deterministic output
        sorted_vals = sorted(valid_values, key=lambda x: (type(x).__name__, x))
        serialized = "\n".join([
            f"### {case_id} POS",
            f"SQL: {sql_template}",
            f"VALIDATE_IN: {sorted_vals}",
            "---",
        ])

        if local_qerr is not None:
            elapsed = _time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            return False, f"local query error: {local_qerr}", serialized

        errors: List[str] = []
        for ri, row in enumerate(ref):
            for ci, v in enumerate(row):
                if v not in valid_values:
                    errors.append(f"local row[{ri}] col[{ci}]={v!r} not in {valid_values}")
        for lbl, tbl in self._ext_sources():
            sql = sql_template.format(tbl=tbl)
            try:
                rows = self._get_rows(sql)
            except QueryError as exc:
                errors.append(f"[{lbl}] query error: {exc}")
                continue
            if len(rows) != len(ref):
                errors.append(f"[{lbl}] row count {len(rows)} != local {len(ref)}")
            for ri, row in enumerate(rows):
                for ci, v in enumerate(row):
                    if v not in valid_values:
                        errors.append(f"[{lbl}] row[{ri}] col[{ci}]={v!r} not in {valid_values}")

        elapsed = _time.monotonic() - t0
        if errors:
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            for e in errors[:10]:
                tdLog.info(f"  {e}")
            return False, "; ".join(errors[:3]), serialized

        tdLog.info(f"{prefix} PASS  {sql_short}  (validate_in)  [{elapsed:.2f}s]")
        return True, "", serialized

    # ------------------------------------------------------------------
    # Orchestrators
    # ------------------------------------------------------------------

    def run_parity_cases(self, parity_cases, parity_groups=None):
        """Run all parity cases with PARITY_IDX filtering and baseline comparison.

        *parity_cases*: flat list of (case_id, sql_template, opts).
        *parity_groups*: optional dict of group_name → entries for PARITY_IDX
                         group expansion.  If None, only exact IDs are supported.
        """
        raw = os.environ.get("PARITY_IDX", "").strip()
        if raw:
            selected_ids: Set[str] = set()
            for part in raw.split(","):
                part = part.strip()
                if parity_groups and part in parity_groups:
                    for i in range(1, len(parity_groups[part]) + 1):
                        selected_ids.add(f"{part}-{i:02d}")
                else:
                    selected_ids.add(part)
            all_ids = {c[0] for c in parity_cases}
            invalid = selected_ids - all_ids
            if invalid:
                grp_names = list(parity_groups) if parity_groups else []
                raise ValueError(
                    f"Unknown PARITY_IDX entries: {sorted(invalid)!r}\n"
                    f"  Valid groups: {grp_names}\n"
                    f"  Example IDs: {list(all_ids)[:3]}"
                )
            selected_cases = [c for c in parity_cases if c[0] in selected_ids]
        else:
            selected_cases = parity_cases

        total  = len(parity_cases)
        n_run  = len(selected_cases)
        n_pos  = sum(1 for _, _, kw in selected_cases if kw.get("positive", True))
        n_neg  = n_run - n_pos
        tdLog.info(f"\nParity run: {n_run} case(s) of {total} total  (pos={n_pos} neg={n_neg})")

        failed: List[Tuple[str, str, str]] = []
        serialized_blocks: List[str] = []

        for case_id, sql_template, kwargs in selected_cases:
            passed, details, serialized = self._run_one_case(case_id, sql_template, **kwargs)
            if serialized:
                serialized_blocks.append(serialized)
            if not passed:
                failed.append((case_id, sql_template, details))

        # ── baseline comparison ──
        # Normalize non-deterministic values (e.g. root@<container_id>)
        _baseline_norm_re = re.compile(r'(root@)[0-9a-f]{12}\b')
        def _normalize_baseline(text: str) -> str:
            return _baseline_norm_re.sub(r'\1<HOST>', text)

        baseline_file = self._BASELINE_FILE
        if not baseline_file:
            raise ValueError(
                "_BASELINE_FILE is not set. "
                "Subclasses MUST define _BASELINE_FILE to a valid baseline path."
            )
        run_all = (not raw) or (len(selected_cases) == total)
        if not os.path.isfile(baseline_file):
            raise FileNotFoundError(
                f"Baseline file not found: {baseline_file}\n"
                f"  The baseline file must be checked into the repository."
            )
        if run_all:
            tmp_file = os.path.join("/tmp", os.path.basename(baseline_file) + ".tmp")
            tmp_content = "\n".join(serialized_blocks) + "\n"
            with open(tmp_file, "w") as f:
                f.write(tmp_content)
            tdLog.info(f"Temp result file written: {tmp_file}")

            with open(baseline_file, "r") as f:
                baseline_content = f.read()
            if _normalize_baseline(tmp_content) != _normalize_baseline(baseline_content):
                tmp_lines = tmp_content.splitlines()
                base_lines = baseline_content.splitlines()
                diff_line = -1
                for li in range(max(len(tmp_lines), len(base_lines))):
                    tl = tmp_lines[li] if li < len(tmp_lines) else "<EOF>"
                    bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                    if tl != bl:
                        diff_line = li + 1
                        break
                baseline_err = (
                    f"Regression baseline mismatch!\n"
                    f"  baseline: {baseline_file}\n"
                    f"  actual  : {tmp_file}\n"
                    f"  first diff at line {diff_line}:\n"
                    f"    baseline: {bl!r}\n"
                    f"    actual  : {tl!r}\n"
                    f"  Run: diff {baseline_file} {tmp_file}"
                )
                tdLog.info(f"BASELINE MISMATCH: {baseline_err}")
                failed.append(("<baseline>", "<baseline>", baseline_err))
            else:
                tdLog.info("Baseline comparison: OK (matches static baseline)")
        else:
            # Subset run
            with open(baseline_file, "r") as f:
                baseline_content = f.read()
            baseline_blocks: Dict[str, str] = {}
            current_id = None
            current_lines: List[str] = []
            for line in baseline_content.splitlines():
                if line.startswith("### "):
                    if current_id is not None:
                        baseline_blocks[current_id] = "\n".join(current_lines)
                    parts = line.split()
                    current_id = parts[1] if len(parts) >= 2 else None
                    current_lines = [line]
                else:
                    current_lines.append(line)
            if current_id is not None:
                baseline_blocks[current_id] = "\n".join(current_lines)

            selected_ids_ordered = [c[0] for c in selected_cases]
            expected_parts = []
            for cid in selected_ids_ordered:
                if cid in baseline_blocks:
                    expected_parts.append(baseline_blocks[cid])
            if not expected_parts:
                raise ValueError(
                    f"No matching baseline blocks found for selected cases: "
                    f"{selected_ids_ordered!r}"
                )
            expected_content = "\n".join(expected_parts) + "\n"
            actual_content = "\n".join(serialized_blocks) + "\n"
            if _normalize_baseline(actual_content) != _normalize_baseline(expected_content):
                act_lines = actual_content.splitlines()
                exp_lines = expected_content.splitlines()
                diff_line = -1
                for li in range(max(len(act_lines), len(exp_lines))):
                    tl = act_lines[li] if li < len(act_lines) else "<EOF>"
                    bl = exp_lines[li] if li < len(exp_lines) else "<EOF>"
                    if tl != bl:
                        diff_line = li + 1
                        break
                baseline_err = (
                    f"Subset baseline mismatch!\n"
                    f"  baseline: {baseline_file} (subset of {len(selected_ids_ordered)} cases)\n"
                    f"  first diff at line {diff_line}:\n"
                    f"    baseline: {bl!r}\n"
                    f"    actual  : {tl!r}"
                )
                tdLog.info(f"BASELINE MISMATCH: {baseline_err}")
                failed.append(("<baseline>", "<baseline>", baseline_err))
            else:
                tdLog.info(f"Baseline comparison: OK (subset of {len(selected_ids_ordered)} cases matches)")

        # ── summary ──
        n_pass = n_run - len(failed)
        sep    = "─" * 72
        tdLog.info(f"\n{sep}")
        tdLog.info(f"  Parity summary: {n_pass}/{n_run} passed  |  {len(failed)} failed  (pos={n_pos} neg={n_neg})")
        if failed:
            tdLog.info("  Failed cases:")
            for case_id, sql, det in failed:
                kw = next((kw for cid, _, kw in parity_cases if cid == case_id), {})
                kind_tag = "POS" if kw.get("positive", True) else "NEG"
                tdLog.info(f"    [{kind_tag}  {case_id}]  {sql[:70]}")
                tdLog.info(f"            {det[:130]}")
        tdLog.info(sep)

        # ── cleanup temp file ──
        if run_all:
            tmp_file_path = os.path.join("/tmp", os.path.basename(baseline_file) + ".tmp")
            if failed:
                tdLog.info(f"Temp result file kept for debugging: {tmp_file_path}")
            elif os.path.isfile(tmp_file_path):
                os.remove(tmp_file_path)
                tdLog.info(f"Temp result file removed (all passed).")

        if failed:
            all_errors = "\n".join(
                f"\n[{case_id}] {sql}\n  {det}" for case_id, sql, det in failed
            )
            raise AssertionError(
                f"{len(failed)} of {n_run} case(s) failed:\n{all_errors}"
            )

    def run_parity_disorder(self, parity_cases, rewrite_data_fn, restore_data_fn):
        """Run positive parity cases after disorder data rewrite.

        *rewrite_data_fn*: callable that re-inserts data in shuffled order.
        *restore_data_fn*: callable that restores original ordered data.
        """
        pos_cases = [
            (cid, sql, kw) for cid, sql, kw in parity_cases
            if kw.get("positive", True)
        ]
        n_run = len(pos_cases)
        tdLog.info(f"[disorder] Running {n_run} positive parity case(s) "
                   f"with disorder data …")

        failed: List[Tuple[str, str, str]] = []
        for case_id, sql_template, kwargs in pos_cases:
            passed, details, _ = self._run_one_case(
                case_id, sql_template, **kwargs)
            if not passed:
                failed.append((case_id, sql_template, details))

        # ── restore ──
        tdLog.info("[disorder] Restoring original ordered data …")
        restore_data_fn()

        # ── summary ──
        n_pass = n_run - len(failed)
        sep    = "─" * 72
        tdLog.info(f"\n{sep}")
        tdLog.info(f"  Disorder parity: {n_pass}/{n_run} passed  |  "
                   f"{len(failed)} failed")
        if failed:
            tdLog.info("  Failed cases:")
            for case_id, sql, det in failed:
                tdLog.info(f"    [{case_id}]  {sql[:70]}")
                tdLog.info(f"            {det[:130]}")
        tdLog.info(sep)

        if failed:
            all_errors = "\n".join(
                f"\n[{case_id}] {sql}\n  {det}"
                for case_id, sql, det in failed
            )
            raise AssertionError(
                f"[disorder] {len(failed)} of {n_run} case(s) failed:\n"
                f"{all_errors}"
            )

    # ------------------------------------------------------------------
    # Epoch parity: taos -r shell execution for timestamp precision
    # ------------------------------------------------------------------

    @staticmethod
    def _epoch_query(sql):
        """Execute one SQL via ``taos -r -s`` and return raw output lines.

        Uses ``-r`` (raw-time) so TIMESTAMP columns are printed as epoch
        integers instead of formatted strings.  This lets us compare exact
        epoch values across sources with different precisions (ms/µs/ns).
        """
        cfgPath = tdCom.getClientCfgPath()
        taos_bin = os.path.join(tdCom.getBuildPath(), "build", "bin", "taos")
        proc = subprocess.run(
            [taos_bin, "-r", "-c", cfgPath, "-s", sql],
            capture_output=True, text=True, errors="ignore",
        )
        lines = proc.stdout.splitlines()
        ignore_pats = [
            "Query OK", "Copyright", "Welcome to the TDengine",
            "Exec cost:", "Database changed",
        ]
        result = []
        for line in lines:
            if any(pat in line for pat in ignore_pats):
                continue
            # strip taos> prompt prefix
            if line.startswith("taos> "):
                continue
            # strip trailing timing info e.g. (0.001234s)
            line = re.sub(r'\s*\(\d+\.\d+s\)\s*$', '', line)
            stripped = line.rstrip()
            if stripped:
                result.append(stripped)
        return result

    @staticmethod
    def _epoch_query_batch(sqls):
        """Execute multiple SQLs in one ``taos -r -f`` call.

        Returns a list of output-line-lists, one per input SQL.
        Dramatically faster than calling ``_epoch_query`` per statement
        because it avoids per-query process startup overhead.
        """
        if not sqls:
            return []
        cfgPath = tdCom.getClientCfgPath()
        taos_bin = os.path.join(tdCom.getBuildPath(), "build", "bin", "taos")
        import tempfile
        fd, tmpfile = tempfile.mkstemp(suffix='.sql', prefix='epoch_')
        try:
            with os.fdopen(fd, 'w') as f:
                for sql in sqls:
                    f.write(sql.rstrip().rstrip(';') + ';\n')
            proc = subprocess.run(
                [taos_bin, "-r", "-c", cfgPath, "-f", tmpfile],
                capture_output=True, text=True, errors="ignore",
            )
        finally:
            try:
                os.unlink(tmpfile)
            except OSError:
                pass

        lines = proc.stdout.splitlines()
        skip_pats = ("Copyright", "Welcome to the TDengine", "Exec cost:",
                     "Query OK", "Database changed", "DB error:")

        blocks: List[List[str]] = []
        current: Optional[List[str]] = None

        for line in lines:
            if line.startswith("taos> "):
                # new query block
                if current is not None:
                    blocks.append(current)
                current = []
                continue
            if current is None:
                continue
            if any(pat in line for pat in skip_pats):
                continue
            cleaned = re.sub(r'\s*\(\d+\.\d+s\)\s*$', '', line).rstrip()
            if cleaned:
                current.append(cleaned)

        if current is not None:
            blocks.append(current)

        # Pad / trim to match input count
        while len(blocks) < len(sqls):
            blocks.append([])
        return blocks[:len(sqls)]

    @staticmethod
    def _parse_epoch_data_rows(output_lines):
        """Parse ``taos -r`` output lines into a list of cell-string lists.

        Skips header and separator (``===``) lines; extracts pipe-delimited
        data rows.  Returns ``[(cell0, cell1, ...), ...]``.
        """
        rows = []
        in_data = False
        for line in output_lines:
            if line.startswith("="):
                in_data = True
                continue
            if in_data and "|" in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                rows.append(tuple(cells))
            elif in_data:
                # Blank or non-data line after separator ends this block
                in_data = False
        return rows

    def run_epoch_parity_cases(self, cases, baseline_file):
        """Run window cases via ``taos -r`` for per-source epoch precision.

        Each *case* is ``(case_id, sql_template, opts)`` where *opts* may
        contain:

        - ``ts_cols``: set of column indices that hold timestamps —
          these columns are expected to differ across sources due to
          precision and are **excluded** from cross-source validation.
        - ``float_cols``: set of column indices with float values —
          compared with tolerance during cross-source validation.

        **Baseline format** — single file, all sources per case::

            ### <case_id> <label>
            SQL: <substituted_sql>
            <header_line>
            <separator_line>
            <data_rows ...>
            ---

        On **first run** (baseline absent) the file is auto-generated.
        Subsequent runs compare against the checked-in baseline.
        Additionally, non-timestamp columns are cross-validated across
        all sources to ensure identical results.

        All queries for a given source are batched into a single
        ``taos -r -f`` call to avoid per-query subprocess overhead.
        """
        sources = [("local", self._local_tbl)]
        sources.extend(self._ext_sources())

        # ── Phase 1: batch-execute per source ──
        per_source_outputs: Dict[str, List[List[str]]] = {}
        per_source_data: Dict[str, List[List[tuple]]] = {}

        for label, tbl in sources:
            sqls = [sql_template.format(tbl=tbl)
                    for _, sql_template, _ in cases]
            # When _PARITY_TZ is set, prepend ALTER LOCAL so the taos -r
            # subprocess interprets datetime literals in the same timezone
            # as the Python connector session.
            n_setup = 0
            if self._PARITY_TZ is not None:
                tz_name = self._PARITY_TZ.tzname(None)
                sqls.insert(0, f'ALTER LOCAL "timezone" "{tz_name}"')
                n_setup = 1
            batch_out = self._epoch_query_batch(sqls)
            if n_setup:
                batch_out = batch_out[n_setup:]
            per_source_outputs[label] = batch_out
            per_source_data[label] = [
                self._parse_epoch_data_rows(out) for out in batch_out
            ]

        # ── Phase 2: serialize + cross-validate ──
        serialized_blocks: List[str] = []
        failed: List[Tuple[str, str]] = []

        for i, (case_id, sql_template, opts) in enumerate(cases):
            ts_cols = opts.get("ts_cols", set())
            float_cols = opts.get("float_cols", set())

            # Serialize each source's output for baseline
            for label, tbl in sources:
                sql = sql_template.format(tbl=tbl)
                output = per_source_outputs[label][i]
                block = [f"### {case_id} {label}", f"SQL: {sql}"]
                block.extend(output)
                block.append("---")
                serialized_blocks.append("\n".join(block))

            # Cross-validate non-ts columns
            local_rows = per_source_data["local"][i]
            for label, _ in sources:
                if label == "local":
                    continue
                ext_rows = per_source_data[label][i]
                if len(local_rows) != len(ext_rows):
                    err = (f"row count mismatch: local={len(local_rows)} "
                           f"{label}={len(ext_rows)}")
                    tdLog.info(f"  [{case_id}] [{label}] {err}")
                    failed.append((case_id, err))
                    continue
                for ri, (lr, er) in enumerate(zip(local_rows, ext_rows)):
                    ncols = min(len(lr), len(er))
                    for ci in range(ncols):
                        if ci in ts_cols:
                            continue
                        lv, ev = lr[ci], er[ci]
                        if ci in float_cols:
                            try:
                                if abs(float(lv) - float(ev)) > self._FLOAT_TOL:
                                    err = (f"float mismatch row={ri} col={ci} "
                                           f"local={lv} {label}={ev}")
                                    tdLog.info(f"  [{case_id}] [{label}] {err}")
                                    failed.append((case_id, err))
                                continue
                            except (TypeError, ValueError):
                                pass
                        if lv != ev:
                            err = (f"value mismatch row={ri} col={ci} "
                                   f"local={lv!r} {label}={ev!r}")
                            tdLog.info(f"  [{case_id}] [{label}] {err}")
                            failed.append((case_id, err))

        # ── baseline comparison ──
        tmp_content = "\n".join(serialized_blocks) + "\n"
        tmp_file = baseline_file + ".tmp"
        os.makedirs(os.path.dirname(baseline_file), exist_ok=True)
        with open(tmp_file, "w") as f:
            f.write(tmp_content)

        if not os.path.isfile(baseline_file):
            # First run — save as baseline
            shutil.copy2(tmp_file, baseline_file)
            tdLog.info(f"Epoch baseline generated (first run): {baseline_file}")
        else:
            with open(baseline_file, "r") as f:
                baseline_content = f.read()
            if tmp_content != baseline_content:
                # Find first diff
                tmp_lines = tmp_content.splitlines()
                base_lines = baseline_content.splitlines()
                diff_line = -1
                bl = tl = ""
                for li in range(max(len(tmp_lines), len(base_lines))):
                    tl = tmp_lines[li] if li < len(tmp_lines) else "<EOF>"
                    bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                    if tl != bl:
                        diff_line = li + 1
                        break
                err = (
                    f"Epoch baseline mismatch!\n"
                    f"  baseline: {baseline_file}\n"
                    f"  actual  : {tmp_file}\n"
                    f"  first diff at line {diff_line}:\n"
                    f"    baseline: {bl!r}\n"
                    f"    actual  : {tl!r}\n"
                    f"  Run: diff {baseline_file} {tmp_file}"
                )
                tdLog.info(f"EPOCH BASELINE MISMATCH: {err}")
                failed.append(("<baseline>", err))
            else:
                tdLog.info("Epoch baseline comparison: OK")
                if os.path.isfile(tmp_file):
                    os.remove(tmp_file)

        # ── summary ──
        n_run = len(cases)
        n_pass = n_run - sum(1 for cid, _ in failed if cid != "<baseline>")
        sep = "─" * 72
        tdLog.info(f"\n{sep}")
        tdLog.info(f"  Epoch parity: {n_pass}/{n_run} passed  |  "
                   f"{len(failed)} failure(s)")
        if failed:
            tdLog.info("  Failures:")
            for cid, det in failed:
                tdLog.info(f"    [{cid}] {det[:140]}")
        tdLog.info(sep)

        if failed:
            all_errors = "\n".join(
                f"\n[{cid}] {det}" for cid, det in failed
            )
            raise AssertionError(
                f"Epoch parity: {len(failed)} failure(s):\n{all_errors}"
            )
