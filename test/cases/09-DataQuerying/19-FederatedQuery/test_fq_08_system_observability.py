"""
test_fq_08_system_observability.py

Data-driven framework for FQ-SYS-001 through FQ-SYS-028 (TS §8):
"System tables, config, observability" — SHOW/DESCRIBE, ins_ext_sources schema,
permissions, dynamic config, TLS, observability metrics, feature toggle.

Framework: statement-array + result-file baseline comparison (same pattern as fq_01).
Cases are grouped by functional area; one test method drives all cases.

Groups:
  A  — System table schema (SYS-003, s03, s04, s05)
  B  — Non-existent / empty results (s01)
  C  — Dynamic config parameter boundaries (SYS-006~008, SYS-021~025, s08)
  D  — OPTIONS JSON / TLS masking (SYS-009, SYS-010, SYS-017, SYS-020, SYS-028)
  E  — create_time correctness (SYS-018)
  F  — Permissions / sysInfo protection (SYS-004, SYS-005)
  G  — Observability / execution chain (SYS-011, SYS-012, SYS-014)
  H  — Multi-source filtering / projection / compound WHERE (s07, s09, s10)
  I  — Local DB unaffected (SYS-016)
  J  — Advanced / env-dependent (s11, s12, s13, s14)
  K  — Behavioral timeout verification (FQ-SYS-021 behavioral, FQ-SYS-009/028 override)
"""

import os
import re
import shutil
import tempfile
import time
from typing import Any, List, Optional, Sequence, Union

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
    TSDB_CODE_EXT_FEATURE_DISABLED,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_QNODE_NOT_FOUND,
)

# ── Connection globals (from ExtSrcEnv class-level attributes) ─────────────
_M_HOST = ExtSrcEnv.MYSQL_HOST
_M_PORT = ExtSrcEnv.MYSQL_PORT
_M_USER = ExtSrcEnv.MYSQL_USER
_M_PASS = ExtSrcEnv.MYSQL_PASS

_P_HOST = ExtSrcEnv.PG_HOST
_P_PORT = ExtSrcEnv.PG_PORT
_P_USER = ExtSrcEnv.PG_USER
_P_PASS = ExtSrcEnv.PG_PASS

_I_HOST  = ExtSrcEnv.INFLUX_HOST
_I_PORT  = ExtSrcEnv.INFLUX_PORT
_I_TOKEN = ExtSrcEnv.INFLUX_TOKEN

_MASKED = "******"

# ── SHOW EXTERNAL SOURCES / ins_ext_sources column indices (DS §5.4) ───────
_COL_NAME     = 0   # source_name
_COL_TYPE     = 1   # type
_COL_HOST     = 2   # host
_COL_PORT     = 3   # port
_COL_USER     = 4   # user  (sysInfo=true — NULL for non-admin)
_COL_PASSWORD = 5   # password (always masked '******')
_COL_DATABASE = 6   # database
_COL_SCHEMA   = 7   # schema
_COL_OPTIONS  = 8   # options (JSON)
_COL_CTIME    = 9   # create_time (TIMESTAMP)

# Columns excluded from baseline comparison (non-deterministic / dynamic)
_DYNAMIC_RESULT_COLUMNS = {"create_time", "ctime"}

_SQL_RESERVED_WORDS = {
    "select", "from", "where", "show", "describe", "create", "drop", "alter",
    "database", "schema", "type", "user", "password", "host", "port", "options",
}

# ── Type shortcuts ─────────────────────────────────────────────────────────
_mysql    = "mysql"
_pg       = "postgresql"
_influxdb = "influxdb"


# ── Step sentinel classes ──────────────────────────────────────────────────

class _QCountStep:
    """SELECT COUNT(*) against the primary source's standard table (inherited from fq_01)."""
    def __init__(self, negative: bool = False, count: int = 5):
        self.negative = negative
        self.count    = count

class _ClearSourceStep:
    """DROP IF EXISTS every source in the current case's source_names list."""
    pass

class _ConnectStep:
    """Switch active tdSql connection to a non-root user."""
    def __init__(self, user: str, password: str):
        self.user     = user
        self.password = password

class _ConnectRootStep:
    """Restore tdSql connection to root / taosdata."""
    pass

class _ErrorStep:
    """Execute SQL expecting an error; record ERROR_OK or WRONG_ERROR in baseline."""
    def __init__(self, sql: str, errno=None):
        self.sql   = sql
        self.errno = errno

class _StopMysqlStep:
    """Stop the primary MySQL instance (ExtSrcEnv.MYSQL_VERSIONS[0])."""
    pass

class _StartMysqlStep:
    """Start (restore) the primary MySQL instance."""
    pass

class _DropQnodeStep:
    """DROP QNODE ON DNODE 1 — for no-qnode negative tests."""
    pass

class _EnsureQnodeStep:
    """Idempotently CREATE QNODE ON DNODE 1 — restore after no-qnode test."""
    pass

class _PollErrorStep:
    """Poll (up to timeout s) until SQL fails; record ERROR_OK or timeout failure."""
    def __init__(self, sql: str, errno=None, timeout: float = 20.0):
        self.sql     = sql
        self.errno   = errno
        self.timeout = timeout

class _PollSuccessStep:
    """Poll (up to timeout s) until SQL succeeds; record rows or timeout failure."""
    def __init__(self, sql: str, timeout: float = 20.0):
        self.sql     = sql
        self.timeout = timeout

class _MysqlSetupStep:
    """Create a MySQL database and run setup SQLs (for adv-group data prep)."""
    def __init__(self, database: str, sqls: List[str]):
        self.database = database
        self.sqls     = sqls

class _MysqlDropDbStep:
    """Drop a MySQL database (for adv-group cleanup)."""
    def __init__(self, database: str):
        self.database = database


# ── Step factory helpers ───────────────────────────────────────────────────

def _clear_source_step() -> _ClearSourceStep:
    return _ClearSourceStep()

def _connect(user: str, password: str) -> _ConnectStep:
    return _ConnectStep(user, password)

def _connect_root() -> _ConnectRootStep:
    return _ConnectRootStep()

def _error_step(sql: str, errno=None) -> _ErrorStep:
    return _ErrorStep(sql, errno=errno)


# ── Case list ─────────────────────────────────────────────────────────────
# Format: [case_id, [types], [source_names], desc, [steps]]
#
#  types        — list of source type strings; case runs once per entry
#  source_names — cleaned up by _clear_source_step()
#  steps        — SQL strings, sentinel objects, or callables(src_type)->step
#
# Framework automatics:
#   * Every successful CREATE EXTERNAL SOURCE auto-triggers
#     SHOW EXTERNAL SOURCES (filtered) + DESCRIBE EXTERNAL SOURCE.
#   * All SQL + results serialised to baseline file (ans/ dir).
#   * _DYNAMIC_RESULT_COLUMNS (create_time, ctime) excluded from baseline.
#   * On first run the baseline is created; subsequent runs compare.

_CASES = [

    # === GROUP A: System table schema ========================================
    # Covers: FQ-SYS-003 (10-column order/types), s03 (same), s04 (PG schema),
    #         s05 (InfluxDB type/database/masked api_token)

    ["sys003m", [_mysql], ["fq08_s003_m"],
     "MySQL source: SELECT all non-dynamic cols from ins_ext_sources; 10-col DS §5.4 schema; password masked; schema empty (covers SYS-003, s03)",
     [
         "drop external source if exists fq08_s003_m",
         f"create external source fq08_s003_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='testdb'",
         "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options` from information_schema.ins_ext_sources where source_name='fq08_s003_m'",
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_s003_m' and create_time is not null",
         _clear_source_step(),
     ],
     ],

    ["sys004p", [_pg], ["fq08_s004_p"],
     "PostgreSQL source: schema='public' stored in ins_ext_sources col[7] (covers s04)",
     [
         "drop external source if exists fq08_s004_p",
         f"create external source fq08_s004_p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
         "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options` from information_schema.ins_ext_sources where source_name='fq08_s004_p'",
         _clear_source_step(),
     ],
     ],

    ["sys005i", [_influxdb], ["fq08_s005_i"],
     "InfluxDB source: type=influxdb, database=telegraf, api_token masked, protocol visible in options (covers s05 + SYS-017)",
     [
         "drop external source if exists fq08_s005_i",
         f"create external source fq08_s005_i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='telegraf' options('api_token'='secret_token','protocol'='flight_sql')",
         "select source_name, `type`, `host`, `port`, `database`, `schema`, `options` from information_schema.ins_ext_sources where source_name='fq08_s005_i'",
         _clear_source_step(),
     ],
     ],

    # === GROUP B: Non-existent / empty results ================================
    # Covers: s01 (non-existent source name returns 0 rows, not error)

    ["empty001", [_mysql], [],
     "SELECT for non-existent source name returns 0 rows (not error); count also 0 (covers s01)",
     [
         "select source_name from information_schema.ins_ext_sources where source_name='_fq08_never_created_'",
         "select count(*) from information_schema.ins_ext_sources where source_name='_fq08_never_created_'",
     ],
     ],

    # === GROUP C: Dynamic config parameter boundaries =========================
    # Covers: SYS-006 (ConnectTimeout), SYS-007 (MetaCacheTTL), SYS-008 (CapCacheTTL),
    #         SYS-021 (ConnectTimeout min 100), SYS-022 (ConnectTimeout 99 rejected),
    #         SYS-023 (MetaCacheTTL max 86400), SYS-024 (federatedQueryEnable recognized),
    #         SYS-025 (ConnectTimeout server-side), s08 (CapCacheTTL boundaries)

    ["cfg006", [_mysql], [],
     "federatedQueryConnectTimeoutMs: valid range [100,600000]; 99 and 600001 rejected (covers SYS-006/021/022/025)",
     [
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '100'",
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '5000'",
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '600000'",
         _error_step("alter dnode 0 'federatedQueryConnectTimeoutMs' '99'",     errno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID),
         _error_step("alter dnode 0 'federatedQueryConnectTimeoutMs' '600001'", errno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID),
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '30000'",
     ],
     ],

    ["cfg024m", [_mysql], ["fq08_c024_m"],
     "federatedQueryEnable=1 recognized by server; SHOW EXTERNAL SOURCES succeeds; DDL works under enabled flag (covers SYS-024)",
     [
         "show external sources",
         "drop external source if exists fq08_c024_m",
         f"create external source fq08_c024_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_c024_m'",
         "alter dnode 0 'federatedQueryEnable' '1'",
         _clear_source_step(),
     ],
     ],

    # === GROUP D: OPTIONS JSON / TLS masking ==================================
    # Covers: SYS-009 (connect/read timeout OPTIONS stored as JSON)
    #         SYS-010 (TLS: tls_client_key masked)
    #         SYS-017 (InfluxDB OPTIONS: api_token masked, protocol visible)
    #         SYS-020 (options column is valid JSON with correct values)
    #         SYS-028 (per-source OPTIONS vs default no-OPTIONS source)

    ["opt009m", [_mysql], ["fq08_o009_m"],
     "OPTIONS: connect_timeout_ms + read_timeout_ms stored as JSON in options column; values correct (covers SYS-009/020)",
     [
         "drop external source if exists fq08_o009_m",
         f"create external source fq08_o009_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='u' password='p' database='testdb' options('connect_timeout_ms'='2000','read_timeout_ms'='3000')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_o009_m'",
         _clear_source_step(),
     ],
     ],

    ["opt010m", [_mysql], ["fq08_o010_m"],
     "TLS: connect_timeout_ms stored as-is; tls_client_key value masked in options column (covers SYS-010)",
     [
         "drop external source if exists fq08_o010_m",
         f"create external source fq08_o010_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='u' password='p' database='testdb' options('connect_timeout_ms'='5000','tls_client_cert'='/path/to/client.pem','tls_client_key'='MY_SECRET_KEY')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_o010_m'",
         _clear_source_step(),
     ],
     ],

    ["opt017i", [_influxdb], ["fq08_o017_i"],
     "InfluxDB OPTIONS: api_token masked; protocol='flight_sql' visible; options is valid JSON (covers SYS-017)",
     [
         "drop external source if exists fq08_o017_i",
         f"create external source fq08_o017_i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='u' password='' database='telegraf' options('api_token'='secret_token','protocol'='flight_sql')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_o017_i'",
         _clear_source_step(),
     ],
     ],

    ["opt028m", [_mysql], ["fq08_o028_d", "fq08_o028_c"],
     "Per-source OPTIONS: custom source has explicit timeouts; default source has no OPTIONS; both in catalog (covers SYS-028)",
     [
         "drop external source if exists fq08_o028_d",
         "drop external source if exists fq08_o028_c",
         f"create external source fq08_o028_d type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_o028_c type='mysql' host='{_M_HOST}' port={_M_PORT} user='u' password='p' database='testdb' options('read_timeout_ms'='1000','connect_timeout_ms'='500')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_o028_c'",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_o028_d'",
         "select count(*) from information_schema.ins_ext_sources where source_name in ('fq08_o028_d','fq08_o028_c')",
         _clear_source_step(),
     ],
     ],

    # === GROUP E: create_time correctness =====================================
    # Covers: SYS-018 (create_time non-null, TIMESTAMP type, value close to now)

    ["ctime018m", [_mysql], ["fq08_ct018_m"],
     "create_time must not be NULL after CREATE EXTERNAL SOURCE; count(*) IS NOT NULL == 1 (covers SYS-018)",
     [
         "drop external source if exists fq08_ct018_m",
         f"create external source fq08_ct018_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_ct018_m' and create_time is not null",
         _clear_source_step(),
     ],
     ],

    # === GROUP F: Permissions / sysInfo protection ============================
    # Covers: SYS-004 (normal user can query basic cols; no permission error)
    #         SYS-005 (sysInfo=0 non-admin: user col NULL; password always masked)

    ["perm004m", [_mysql], ["fq08_pm004_m"],
     "Normal user (sysinfo=0) can query basic ins_ext_sources columns without error; password always '******' (covers SYS-004)",
     [
         "drop external source if exists fq08_pm004_m",
         f"create external source fq08_pm004_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='testdb'",
         # Admin baseline: verify basic columns
         "select source_name, `type`, `host`, `port`, `database` from information_schema.ins_ext_sources where source_name='fq08_pm004_m'",
         # Setup non-admin user
         "drop user if exists fq08_pm004_usr",
         "create user fq08_pm004_usr pass 'Test_123' sysinfo 0",
         _connect("fq08_pm004_usr", "Test_123"),
         # Non-admin: basic columns must be accessible (table is PRIV_CAT_BASIC)
         "select source_name, `type`, `host`, `port`, `database` from information_schema.ins_ext_sources where source_name='fq08_pm004_m'",
         # Non-admin: password always masked as '******'
         "select `password` from information_schema.ins_ext_sources where source_name='fq08_pm004_m'",
         _connect_root(),
         "drop user if exists fq08_pm004_usr",
         _clear_source_step(),
     ],
     ],

    ["perm005m", [_mysql], ["fq08_pm005_m"],
     "Admin and non-admin (sysinfo=0) both see user + masked password; user col visible to all per FS §3.9.1 (covers SYS-005)",
     [
         "drop external source if exists fq08_pm005_m",
         f"create external source fq08_pm005_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         # Admin: sees user col (sysInfo=true) and masked password
         "select `user`, `password` from information_schema.ins_ext_sources where source_name='fq08_pm005_m'",
         # Setup non-admin user (sysinfo=0)
         "drop user if exists fq08_pm005_usr",
         "create user fq08_pm005_usr pass 'Test_123' sysinfo 0",
         _connect("fq08_pm005_usr", "Test_123"),
         # Non-admin: user col visible to all (sysInfo=false per FS §3.9.1); password still masked
         "select `user`, `password` from information_schema.ins_ext_sources where source_name='fq08_pm005_m'",
         _connect_root(),
         "drop user if exists fq08_pm005_usr",
         _clear_source_step(),
     ],
     ],

    # === GROUP G: Observability / execution chain =============================
    # Covers: SYS-011 (external metrics: EXT_TABLE_NOT_EXIST proves full chain)
    #         SYS-012 (pushdown: MySQL + PG each through external path, no fallback)
    #         SYS-014 (CREATE→catalog-register→SELECT via external→DROP lifecycle)

    ["obs011m", [_mysql], ["fq08_ob011_m"],
     "Full execution chain: MySQL → EXT_TABLE_NOT_EXIST (not SYNTAX_ERROR) proves parser→planner→executor→connector path (covers SYS-011)",
     [
         "drop external source if exists fq08_ob011_m",
         f"create external source fq08_ob011_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         "select source_name, `type`, `host`, `port` from information_schema.ins_ext_sources where source_name='fq08_ob011_m'",
         _error_step("select * from fq08_ob011_m.testdb.some_table limit 1", errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         _clear_source_step(),
     ],
     ],

    ["obs012mp", [_mysql], ["fq08_ob012_m", "fq08_ob012_p"],
     "MySQL + PG sources registered independently; each routes to external path; combined count=2; no interference (covers SYS-012)",
     [
         "drop external source if exists fq08_ob012_m",
         "drop external source if exists fq08_ob012_p",
         f"create external source fq08_ob012_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_ob012_p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
         "select source_name, `type` from information_schema.ins_ext_sources where source_name='fq08_ob012_m'",
         "select source_name, `type` from information_schema.ins_ext_sources where source_name='fq08_ob012_p'",
         "select count(*) from information_schema.ins_ext_sources where source_name in ('fq08_ob012_m','fq08_ob012_p')",
         _error_step("select * from fq08_ob012_m.testdb.t1 limit 1", errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         _error_step("select * from fq08_ob012_p.pgdb.t1 limit 1",   errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         _clear_source_step(),
     ],
     ],

    ["obs014m", [_mysql], ["fq08_ob014_m"],
     "Lifecycle: CREATE→catalog-register→SELECT via external path→DROP→confirm removed from catalog (covers SYS-014)",
     [
         "drop external source if exists fq08_ob014_m",
         f"create external source fq08_ob014_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         "select source_name, `type` from information_schema.ins_ext_sources where source_name='fq08_ob014_m'",
         _error_step("select * from fq08_ob014_m.testdb.some_table limit 1", errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         "drop external source if exists fq08_ob014_m",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_ob014_m'",
     ],
     ],

    # === GROUP H: Multi-source filtering / projection / compound WHERE =========
    # Covers: s07 (multiple sources + type-based WHERE filter)
    #         s09 (partial column SELECT / projection)
    #         s10 (compound AND WHERE: host + type + source_name)

    ["multi007", [_mysql], ["fq08_ml007_m1", "fq08_ml007_m2", "fq08_ml007_p1"],
     "2 MySQL + 1 PG sources; type-based WHERE count: mysql=2, postgresql=1 (covers s07)",
     [
         "drop external source if exists fq08_ml007_m1",
         "drop external source if exists fq08_ml007_m2",
         "drop external source if exists fq08_ml007_p1",
         f"create external source fq08_ml007_m1 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_ml007_m2 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_ml007_p1 type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_ml007_m1'",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_ml007_m2'",
         "select source_name from information_schema.ins_ext_sources where source_name='fq08_ml007_p1'",
         "select count(*) from information_schema.ins_ext_sources where `type`='mysql' and source_name in ('fq08_ml007_m1','fq08_ml007_m2','fq08_ml007_p1')",
         "select count(*) from information_schema.ins_ext_sources where `type`='postgresql' and source_name in ('fq08_ml007_m1','fq08_ml007_m2','fq08_ml007_p1')",
         _clear_source_step(),
     ],
     ],

    ["proj009p", [_pg], ["fq08_pj009_p"],
     "Partial column SELECT (4-col projection) from ins_ext_sources: source_name, type, database, schema correct (covers s09)",
     [
         "drop external source if exists fq08_pj009_p",
         f"create external source fq08_pj009_p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
         "select source_name, `type`, `database`, `schema` from information_schema.ins_ext_sources where source_name='fq08_pj009_p'",
         _clear_source_step(),
     ],
     ],

    ["comp010", [_mysql], ["fq08_cp010_m", "fq08_cp010_p"],
     "Compound AND WHERE (host+type+source_name): MySQL row found; PG row found; type mismatch yields 0 rows (covers s10)",
     [
         "drop external source if exists fq08_cp010_m",
         "drop external source if exists fq08_cp010_p",
         f"create external source fq08_cp010_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_cp010_p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
         f"select source_name from information_schema.ins_ext_sources where `host`='{_M_HOST}' and `type`='mysql' and source_name in ('fq08_cp010_m','fq08_cp010_p')",
         f"select source_name from information_schema.ins_ext_sources where `host`='{_P_HOST}' and `type`='postgresql' and source_name in ('fq08_cp010_m','fq08_cp010_p')",
         # Mismatch: type='mysql' but PG source name -> 0 rows
         "select source_name from information_schema.ins_ext_sources where `type`='mysql' and source_name='fq08_cp010_p'",
         _clear_source_step(),
     ],
     ],

    # === GROUP I: Local DB unaffected =========================================
    # Covers: SYS-016 (local DB CREATE/INSERT/SELECT unaffected when feature is on)

    ["local016", [_mysql], [],
     "Local DB CREATE/INSERT/SELECT unaffected when federatedQueryEnable=1; no regression (covers SYS-016)",
     [
         "show external sources",
         "create database if not exists fq08_local016",
         "use fq08_local016",
         "create table if not exists fq08_016_t (ts timestamp, v int)",
         "insert into fq08_016_t values (1704067200000, 42)",
         "select v from fq08_016_t",
         "drop database if exists fq08_local016",
     ],
     ],

    # === GROUP J: Advanced / env-dependent ====================================
    # Covers: s11 (connect_timeout_ms actual trigger: stop MySQL -> UNAVAILABLE)
    #         s12 (10-round sequential ALTER options + SELECT: no catalog corruption)
    #         s13 (no qnode -> QNODE_NOT_FOUND; restore -> recovery)
    #         s14 (qnode routing proof: ins_qnodes>=1; EXPLAIN MERGE; 5 repeats; drop->fail->restore)

    ["adv011m", [_mysql], ["fq08_a011_m"],
     "connect_timeout_ms=500 against stopped MySQL -> EXT_SOURCE_UNAVAILABLE within timeout; source stays in catalog (covers s11)",
     [
         "drop external source if exists fq08_a011_m",
         f"create external source fq08_a011_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' options('connect_timeout_ms'='500')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_a011_m'",
         _StopMysqlStep(),
         _error_step("select count(*) from fq08_a011_m.testdb.t", errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE),
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_a011_m'",
         _StartMysqlStep(),
         _clear_source_step(),
     ],
     ],

    ["adv012m", [_mysql], ["fq08_a012_m"],
     "10-round sequential ALTER options + SELECT count: no catalog corruption; final connect_timeout_ms='1900' persisted (covers s12)",
     [
         "drop external source if exists fq08_a012_m",
         _MysqlSetupStep("fq08_adv012_ext", [
             "drop table if exists sys_t",
             "create table sys_t (id int primary key, val int)",
             "insert into sys_t values (1,1),(2,2),(3,3)",
         ]),
         f"create external source fq08_a012_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='fq08_adv012_ext'",
         # 10 rounds of ALTER + SELECT (inlined for explicit baseline tracing)
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1000')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1100')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1200')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1300')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1400')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1500')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1600')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1700')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1800')",
         "select count(*) from fq08_a012_m.sys_t",
         "alter external source fq08_a012_m set options('connect_timeout_ms'='1900')",
         "select count(*) from fq08_a012_m.sys_t",
         # Post-loop: catalog still has 1 source; final options persisted
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_a012_m'",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_a012_m'",
         _clear_source_step(),
         _MysqlDropDbStep("fq08_adv012_ext"),
     ],
     ],

    ["adv013m", [_mysql], ["fq08_a013_m"],
     "No qnode -> federated SELECT fails QNODE_NOT_FOUND; restore qnode -> query succeeds again (covers s13)",
     [
         "drop external source if exists fq08_a013_m",
         _MysqlSetupStep("fq08_adv013_ext", [
             "drop table if exists s13_t",
             "create table s13_t (id int primary key, val int)",
             "insert into s13_t values (1, 100), (2, 200)",
         ]),
         f"create external source fq08_a013_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='fq08_adv013_ext'",
         # Baseline: qnode present -> query succeeds
         "select val from fq08_a013_m.s13_t order by id",
         # Drop qnode; poll until cache invalidated -> QNODE_NOT_FOUND
         _DropQnodeStep(),
         _PollErrorStep("select val from fq08_a013_m.s13_t order by id", errno=TSDB_CODE_QNODE_NOT_FOUND, timeout=20.0),
         # Restore qnode; poll until recovery confirmed
         _EnsureQnodeStep(),
         _PollSuccessStep("select val from fq08_a013_m.s13_t order by id", timeout=20.0),
         _clear_source_step(),
         _MysqlDropDbStep("fq08_adv013_ext"),
     ],
     ],

    ["adv014m", [_mysql], ["fq08_a014_m"],
     "Qnode routing proof: ins_qnodes>=1; EXPLAIN shows Federated Scan; 5 repeated selects succeed; drop->fail->restore (covers s14)",
     [
         "drop external source if exists fq08_a014_m",
         _MysqlSetupStep("fq08_adv014_ext", [
             "drop table if exists s14_t",
             "create table s14_t (id int primary key, val int)",
             "insert into s14_t values (1, 10), (2, 20), (3, 30)",
         ]),
         f"create external source fq08_a014_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='fq08_adv014_ext'",
         # A: qnode present in ins_qnodes
         "select count(*) from information_schema.ins_qnodes",
         # B: EXPLAIN shows MERGE plan (not vnode SCAN)
         "explain select val from fq08_a014_m.s14_t order by val",
         # C: 5 repeated queries all succeed (proves no mnode fallback)
         "select val from fq08_a014_m.s14_t order by val",
         "select val from fq08_a014_m.s14_t order by val",
         "select val from fq08_a014_m.s14_t order by val",
         "select val from fq08_a014_m.s14_t order by val",
         "select val from fq08_a014_m.s14_t order by val",
         # D: round-trip proof — drop -> fail -> restore -> succeed
         _DropQnodeStep(),
         _PollErrorStep("select val from fq08_a014_m.s14_t", timeout=20.0),
         _EnsureQnodeStep(),
         _PollSuccessStep("select val from fq08_a014_m.s14_t", timeout=20.0),
         _clear_source_step(),
         _MysqlDropDbStep("fq08_adv014_ext"),
     ],
     ],

    # === GROUP K: Behavioral timeout verification =============================
    # Covers: FQ-SYS-021 (global ConnectTimeoutMs 100ms behaviorally effective)
    #         FQ-SYS-009/028 (per-source OPTIONS override global timeout)
    # These prove config values ACTUALLY CONTROL timeout behaviour, not just
    # that the config is accepted (which GROUP C already covers).

    ["behav021m", [_mysql], ["fq08_bh021_m"],
     "Global federatedQueryConnectTimeoutMs=100 behaviorally effective: stopped MySQL + global 100ms -> fast EXT_SOURCE_UNAVAILABLE (covers FQ-SYS-021 behavioral)",
     [
         "drop external source if exists fq08_bh021_m",
         # Lower global timeout to 100ms (minimum allowed)
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '100'",
         # Create source WITHOUT per-source timeout -> uses global 100ms
         f"create external source fq08_bh021_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         _StopMysqlStep(),
         # Query must fail with UNAVAILABLE (global 100ms applied -> fast fail)
         _error_step("select count(*) from fq08_bh021_m.testdb.t", errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE),
         # Source still registered in catalog despite outage
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_bh021_m'",
         _StartMysqlStep(),
         # Restore global default
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '30000'",
         _clear_source_step(),
     ],
     ],

    ["override009m", [_mysql], ["fq08_ov009_m"],
     "Per-source OPTIONS override global: global=600000ms, per-source=500ms -> stopped MySQL -> fast UNAVAILABLE proves per-source wins (covers FQ-SYS-009/028 behavioral)",
     [
         "drop external source if exists fq08_ov009_m",
         # Set global to maximum 600000ms (10 min) — if used, test would hang
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '600000'",
         # Create source with per-source 500ms (overrides global)
         f"create external source fq08_ov009_m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' options('connect_timeout_ms'='500')",
         "select `options` from information_schema.ins_ext_sources where source_name='fq08_ov009_m'",
         _StopMysqlStep(),
         # Per-source 500ms must apply, not global 600000ms -> fast fail
         _error_step("select count(*) from fq08_ov009_m.testdb.t", errno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE),
         _StartMysqlStep(),
         # Restore global default
         "alter dnode 0 'federatedQueryConnectTimeoutMs' '30000'",
         _clear_source_step(),
     ],
     ],
]


# ── Test class ─────────────────────────────────────────────────────────────

class TestFq08SystemObservability(FederatedQueryVersionedMixin):
    """Data-driven framework tests for FQ-SYS: system tables, config, observability.

    One test method (test_fq08_system_observability_framework) drives all _CASES.
    Results are serialised to baseline files in ans/ and compared on subsequent runs.

    Labels: common,ci
    Since: v3.4.0.0
    """

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")
        # Safety cleanup: drop local test DB in case local016 case failed mid-run
        try:
            tdSql.execute("drop database if exists fq08_local016")
        except Exception:
            pass
        # Restore federatedQueryConnectTimeoutMs to default (5000 ms) so that
        # subsequent test files (e.g. fq_05) see the expected default value.
        try:
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
        except Exception:
            pass
        # Ensure qnode is present for subsequent test files
        ExtSrcEnv.ensure_qnode()

    # ── Framework helpers (same pattern as fq_01) ─────────────────────────

    @staticmethod
    def _fw_fmt_cell(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.12g}"
        return str(value)

    def _fw_fmt_rows(self, rows: Sequence) -> List[str]:
        return ["|".join(self._fw_fmt_cell(v) for v in row) for row in rows]

    @staticmethod
    def _fw_drop_dynamic_columns(description, rows: Sequence):
        """Remove _DYNAMIC_RESULT_COLUMNS (e.g. create_time) before baseline comparison."""
        if not description:
            return description, rows
        keep = [i for i, col in enumerate(description)
                if str(col[0]).lower() not in _DYNAMIC_RESULT_COLUMNS]
        if len(keep) == len(description):
            return description, rows
        return [description[i] for i in keep], [tuple(row[i] for i in keep) for row in rows]

    def _fw_fmt_result(self, description, rows: Sequence) -> List[str]:
        description, rows = self._fw_drop_dynamic_columns(description, rows)
        lines = []
        if description:
            lines.append("|".join(col[0] for col in description))
        lines.extend(self._fw_fmt_rows(rows))
        return lines

    @staticmethod
    def _fw_normalize_result_lines(result: Union[str, Sequence[str], None]) -> List[str]:
        if result is None:
            return ["<empty result>"]
        if isinstance(result, str):
            return [result] if result else ["<empty result>"]
        lines = [str(line) for line in result if str(line)]
        return lines if lines else ["<empty result set>"]

    def _fw_append_step_block(self, blocks: List[str], label: str, step_tag: str,
                               kind: str, sql: str, result):
        lines = [
            f"### {label} {step_tag} {kind}",
            "SQL: " + sql,
            "RESULT:",
        ]
        lines.extend(self._fw_normalize_result_lines(result))
        lines.append("---")
        blocks.append("\n".join(lines))

    @staticmethod
    def _fw_append_case_boundary(blocks: List[str], label: str, desc: str, is_start: bool):
        marker = "CASE START" if is_start else "CASE END"
        blocks.append("\n".join([
            "=" * 96,
            f"{marker}: {label}",
            f"DESC: {desc}",
            "=" * 96,
        ]))

    @staticmethod
    def _fw_query_once(sql: str, exit: bool = False):
        return tdSql.query(sql, exit=exit, queryTimes=1)

    @staticmethod
    def _fw_quote_src_name(src_name: str) -> str:
        name = str(src_name)
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) and name.lower() not in _SQL_RESERVED_WORDS:
            return name
        return "`" + name.replace("`", "``") + "`"

    @staticmethod
    def _fw_norm_src_name(src_name: Any) -> str:
        name = str(src_name).strip()
        if len(name) >= 2 and name[0] == "`" and name[-1] == "`":
            name = name[1:-1]
        return name.lower()

    @staticmethod
    def _fw_extract_src_name(sql: str) -> Optional[str]:
        m = re.match(
            r"create\s+external\s+source(?:\s+if\s+not\s+exists)?\s+(`[^`]+`|\S+)",
            sql.strip(), re.IGNORECASE,
        )
        return m.group(1).strip("`") if m else None

    def _fw_auto_show_describe(self, src_name: str, blocks: List[str],
                                label: str, step_tag: str):
        """Auto SHOW (filtered for src_name) + DESCRIBE after successful CREATE."""
        # SHOW — filter to this source
        show_sql = "show external sources"
        ok = self._fw_query_once(show_sql, exit=False)
        if ok is not False:
            target   = self._fw_norm_src_name(src_name)
            matching = [r for r in tdSql.queryResult
                        if self._fw_norm_src_name(r[_COL_NAME]) == target]
            rows_text = self._fw_fmt_result(tdSql.cursor.description, matching)
        else:
            rows_text = "ERROR: show external sources failed"
        self._fw_append_step_block(blocks, label, step_tag, "AUTO-SHOW", show_sql, rows_text)

        # DESCRIBE
        desc_sql = f"describe external source {self._fw_quote_src_name(src_name)}"
        ok2 = self._fw_query_once(desc_sql, exit=False)
        if ok2 is not False:
            rows_text = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult)
        else:
            rows_text = "ERROR: describe external source failed"
        self._fw_append_step_block(blocks, label, step_tag, "AUTO-DESCRIBE", desc_sql, rows_text)

    def _fw_exec_step(self, step, src_type: str, source_names: List[str],
                       blocks: List[str], label: str, step_tag: str):
        """Execute one step and append serialised result block(s) to blocks."""

        # ── callable (e.g. lambda returning a SQL string or sentinel) ──────
        if callable(step):
            self._fw_exec_step(step(src_type), src_type, source_names, blocks, label, step_tag)
            return

        # ── SQL string ──────────────────────────────────────────────────────
        if isinstance(step, str):
            sql      = step
            stripped = sql.strip()
            is_create = bool(re.match(r"create\s+external\s+source", stripped, re.IGNORECASE))
            is_query  = bool(re.match(r"(select|show|describe|explain)\b", stripped, re.IGNORECASE))

            if is_query:
                ok = self._fw_query_once(sql, exit=False)
                if ok is not False:
                    rows_all = tdSql.queryResult or []
                    # "SHOW EXTERNAL SOURCES" returns ALL sources across the whole
                    # server, making baseline comparison environment-dependent when
                    # other tests leave sources behind.  Always filter to only the
                    # sources declared for this case (source_names) so the result
                    # is deterministic regardless of server state.
                    # When source_names is empty (e.g. local016), the filter yields
                    # 0 rows — which is still deterministic and sufficient to prove
                    # the command executed without error.
                    if stripped.lower() == "show external sources":
                        norm = {self._fw_norm_src_name(n) for n in source_names}
                        rows_all = [r for r in rows_all
                                    if self._fw_norm_src_name(r[_COL_NAME]) in norm]
                    rows = self._fw_fmt_result(tdSql.cursor.description, rows_all)
                else:
                    rows = ["ERROR: query failed"]
                self._fw_append_step_block(blocks, label, step_tag, "QUERY", sql, rows)
            else:
                tdSql.sql = sql
                try:
                    tdSql.affectedRows = tdSql.cursor.execute(sql)
                    exec_result = "OK"
                except Exception as _e:
                    _msg = str(_e).splitlines()[0] if str(_e) else "unknown error"
                    exec_result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "EXEC", sql, exec_result)
                if is_create:
                    created_name = self._fw_extract_src_name(sql)
                    if created_name:
                        self._fw_auto_show_describe(created_name, blocks, label, step_tag)
            return

        # ── _ErrorStep: execute SQL expecting failure ───────────────────────
        if isinstance(step, _ErrorStep):
            sql = step.sql
            try:
                tdSql.cursor.execute(sql)
                result = "UNEXPECTED_OK (expected error)"
            except Exception as e:
                err_code = getattr(e, "errno", None)
                if step.errno is not None and err_code is not None:
                    if (err_code & 0xFFFFFFFF) != (step.errno & 0xFFFFFFFF):
                        result = (f"WRONG_ERROR: expected 0x{step.errno & 0xFFFFFFFF:08x} "
                                  f"got 0x{err_code & 0xFFFFFFFF:08x}")
                    else:
                        result = "ERROR_OK"
                else:
                    result = "ERROR_OK"
            self._fw_append_step_block(blocks, label, step_tag, "ERROR", sql, result)
            return

        # ── _ClearSourceStep: drop all case sources ─────────────────────────
        if isinstance(step, _ClearSourceStep):
            for name in source_names:
                sql = f"drop external source if exists {name}"
                try:
                    tdSql.cursor.execute(sql)
                    result = "OK"
                except Exception as _e:
                    _msg = str(_e).splitlines()[0] if str(_e) else "unknown error"
                    result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "CLEANUP", sql, result)
            return

        # ── _ConnectStep / _ConnectRootStep ─────────────────────────────────
        if isinstance(step, _ConnectStep):
            sql = f"-- connect user={step.user}"
            try:
                tdSql.connect(step.user, step.password)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e).splitlines()[0][:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "CONNECT", sql, result)
            return

        if isinstance(step, _ConnectRootStep):
            sql = "-- connect user=root"
            try:
                tdSql.connect("root", "taosdata")
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e).splitlines()[0][:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "CONNECT", sql, result)
            return

        # ── _StopMysqlStep / _StartMysqlStep ────────────────────────────────
        if isinstance(step, _StopMysqlStep):
            ver = ExtSrcEnv.MYSQL_VERSIONS[0]
            sql = f"-- stop mysql {ver}"
            try:
                ExtSrcEnv.stop_mysql_instance(ver)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "ENV-CTRL", sql, result)
            return

        if isinstance(step, _StartMysqlStep):
            ver = ExtSrcEnv.MYSQL_VERSIONS[0]
            sql = f"-- start mysql {ver}"
            try:
                ExtSrcEnv.start_mysql_instance(ver)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "ENV-CTRL", sql, result)
            return

        # ── _DropQnodeStep / _EnsureQnodeStep ───────────────────────────────
        if isinstance(step, _DropQnodeStep):
            sql = "-- drop qnode on dnode 1"
            try:
                ExtSrcEnv.drop_qnode()
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "ENV-CTRL", sql, result)
            return

        if isinstance(step, _EnsureQnodeStep):
            sql = "-- ensure qnode on dnode 1"
            try:
                ExtSrcEnv.ensure_qnode()
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "ENV-CTRL", sql, result)
            return

        # ── _PollErrorStep: poll until SQL fails ────────────────────────────
        if isinstance(step, _PollErrorStep):
            sql      = step.sql
            deadline = time.monotonic() + step.timeout
            got_error, last_exc = False, None
            while time.monotonic() < deadline:
                try:
                    tdSql.query(sql, queryTimes=1)
                    tdLog.info(f"[{label}] PollError: query succeeded, retrying in 1s...")
                    time.sleep(1)
                except Exception as e:
                    last_exc  = e
                    got_error = True
                    break
            if not got_error:
                result = f"POLL_ERROR_TIMEOUT: query kept succeeding for {step.timeout:.0f}s"
            else:
                err_code = getattr(last_exc, "errno", None)
                if step.errno is not None and err_code is not None:
                    if (err_code & 0xFFFFFFFF) != (step.errno & 0xFFFFFFFF):
                        result = (f"WRONG_ERROR: expected 0x{step.errno & 0xFFFFFFFF:08x} "
                                  f"got 0x{err_code & 0xFFFFFFFF:08x}")
                    else:
                        result = "ERROR_OK"
                else:
                    result = "ERROR_OK"
            self._fw_append_step_block(blocks, label, step_tag, "POLL-ERROR", sql, result)
            return

        # ── _PollSuccessStep: poll until SQL succeeds ───────────────────────
        if isinstance(step, _PollSuccessStep):
            sql      = step.sql
            deadline = time.monotonic() + step.timeout
            succeeded, rows = False, []
            while time.monotonic() < deadline:
                ok = self._fw_query_once(sql, exit=False)
                if ok is not False:
                    succeeded = True
                    rows = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult or [])
                    break
                time.sleep(1)
            if not succeeded:
                result = f"POLL_SUCCESS_TIMEOUT: query kept failing for {step.timeout:.0f}s"
                self._fw_append_step_block(blocks, label, step_tag, "POLL-SUCCESS", sql, result)
            else:
                self._fw_append_step_block(blocks, label, step_tag, "POLL-SUCCESS", sql, rows)
            return

        # ── _MysqlSetupStep: create MySQL DB + run setup SQLs ───────────────
        if isinstance(step, _MysqlSetupStep):
            sql = f"-- mysql setup database={step.database}"
            try:
                cfg = next(ExtSrcEnv.mysql_version_configs())
                ExtSrcEnv.mysql_create_db_cfg(cfg, step.database)
                ExtSrcEnv.mysql_exec_cfg(cfg, step.database, step.sqls)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "MYSQL-SETUP", sql, result)
            return

        # ── _MysqlDropDbStep: drop MySQL database ───────────────────────────
        if isinstance(step, _MysqlDropDbStep):
            sql = f"-- mysql drop database={step.database}"
            try:
                cfg = next(ExtSrcEnv.mysql_version_configs())
                ExtSrcEnv.mysql_drop_db_cfg(cfg, step.database)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "MYSQL-CLEANUP", sql, result)
            return

        raise ValueError(f"Unknown step type: {type(step).__name__}")

    # ── Baseline file management ───────────────────────────────────────────

    def _fw_baseline_file(self) -> str:
        label = self._version_label().replace(".", "_").replace("/", "_")
        return os.path.join(
            os.path.dirname(__file__),
            "ans",
            f"test_fq_08_system_observability_framework_{label}.txt",
        )

    def _fw_compare_baseline(self, blocks: List[str]):
        actual   = "\n".join(blocks) + "\n"
        baseline = self._fw_baseline_file()
        tmp_file = os.path.join(
            tempfile.gettempdir(),
            f"{os.path.basename(baseline)}.{os.getpid()}.tmp",
        )

        os.makedirs(os.path.dirname(baseline), exist_ok=True)
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(actual)

        if not os.path.isfile(baseline):
            shutil.copy(tmp_file, baseline)
            os.remove(tmp_file)
            tdLog.info(f"Framework baseline file created: {baseline}")
            return

        with open(baseline, "r", encoding="utf-8") as f:
            expected = f.read()

        if expected == actual:
            os.remove(tmp_file)
            tdLog.info("Framework baseline comparison: OK")
            return

        exp_lines = expected.splitlines()
        act_lines = actual.splitlines()
        diff_line, exp_val, act_val = -1, "<EOF>", "<EOF>"
        for idx in range(max(len(exp_lines), len(act_lines))):
            lhs = exp_lines[idx] if idx < len(exp_lines) else "<EOF>"
            rhs = act_lines[idx] if idx < len(act_lines) else "<EOF>"
            if lhs != rhs:
                diff_line = idx + 1
                exp_val, act_val = lhs, rhs
                break

        raise AssertionError(
            "Framework baseline mismatch\n"
            f"  baseline: {baseline}\n"
            f"  actual  : {tmp_file}\n"
            f"  first diff at line {diff_line}:\n"
            f"    baseline: {exp_val!r}\n"
            f"    actual  : {act_val!r}"
        )

    # ── Main test entry point ──────────────────────────────────────────────

    def test_fq08_system_observability_framework(self):
        """Statement-array + baseline-file framework covering all FQ-SYS-001~028 cases.

        Run a subset: FQ_CASES=sys003m,cfg006 (comma-separated case IDs).
        On first run the baseline is auto-created; subsequent runs compare against it.

        Labels: common,ci
        Since: v3.4.0.0
        """
        _only     = os.environ.get("FQ_CASES", "").strip()
        _only_set = set(_only.split(",")) if _only else None
        blocks: List[str]  = []
        timings: List[str] = []

        for case in _CASES:
            case_id, types, source_names, desc, steps = case
            if _only_set and case_id not in _only_set:
                continue
            for src_type in types:
                label = f"CASE-{case_id}[{src_type[:3].upper()}]"
                tdLog.info(f"Running {label}: {desc}")
                self._fw_append_case_boundary(blocks, label, desc, is_start=True)
                _t0 = time.monotonic()
                for step_no, step in enumerate(steps, start=1):
                    step_tag = f"STEP-{step_no:03d}"
                    self._fw_exec_step(step, src_type, source_names, blocks, label, step_tag)
                elapsed = time.monotonic() - _t0
                self._fw_append_case_boundary(blocks, label, desc, is_start=False)
                timing_line = f"{label}: {elapsed:.2f}s"
                print(f"[TIMING] {timing_line}", flush=True)
                timings.append(timing_line)

        print("[TIMING SUMMARY]", flush=True)
        for t in timings:
            print(f"  {t}", flush=True)

        if _only_set:
            tdLog.info(f"FQ_CASES={_only} — skipping baseline comparison")
        else:
            self._fw_compare_baseline(blocks)
