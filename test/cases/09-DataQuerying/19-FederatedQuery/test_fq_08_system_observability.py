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
  O  — Oracle observability (FQOR-SYS-001~005: catalog row, EXPLAIN namespace,
       USE-selected schema, log masking, distinguishable error classes)
"""

import glob
import os
import re
import shutil
import tempfile
import time
from typing import Any, List, Optional, Sequence, Union

from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.srvCtl import sc

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_EXT_ACCESS_DENIED,
    TSDB_CODE_EXT_AUTH_FAILED,
    TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
    TSDB_CODE_EXT_CONNECT_FAILED,
    TSDB_CODE_EXT_DB_NOT_EXIST,
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

_S_HOST = ExtSrcEnv.SQLSERVER_HOST
_S_PORT = ExtSrcEnv.SQLSERVER_PORT
_S_USER = ExtSrcEnv.SQLSERVER_USER
_S_PASS = ExtSrcEnv.SQLSERVER_PASS

# Oracle: the service name is the source's DATABASE and a schema is a login
# user (DS 5.3.3), so the namespace columns carry those two values.
_O_HOST    = ExtSrcEnv.ORACLE_HOST
_O_PORT    = ExtSrcEnv.ORACLE_PORT
_O_USER    = ExtSrcEnv.ORACLE_USER
_O_PASS    = ExtSrcEnv.ORACLE_PASS
_O_SERVICE = ExtSrcEnv.ORACLE_SERVICE
_O_SCHEMA  = ExtSrcEnv.ORACLE_USER.upper()
# The second Oracle schema the USE cases switch to.  A schema is an account,
# so this is the provisioned alternate user, with its own namespace.
_O_SCHEMA2 = ExtSrcEnv.ORACLE_ALT_USER.upper()

# A password that is not any real credential.  The log-masking case can then
# assert its absence without colliding with a value that also appears as a
# legitimate user name.
_O_BAD_PASS = "fq08_wrong_password_9d31"

# Oracle object names are folded to upper case by unquoted DDL.  The fixture
# SQL below uses unquoted (folded) names throughout, so drops, CREATEs and
# queries all agree without quoting.
_LOG_TAIL_BYTES = 8 * 1024 * 1024

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
_sqlserver = "sqlserver"
_oracle    = "oracle"


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
    """Execute SQL expecting an error; record ERROR_OK or WRONG_ERROR in baseline.

    ``required`` / ``forbidden`` hold text fragments that the error message must
    / must not contain; they are asserted (not just recorded) so a message that
    changes class fails the case instead of silently updating the baseline.
    """
    def __init__(self, sql: str, errno=None, required: Sequence[str] = (),
                 forbidden: Sequence[str] = ()):
        self.sql       = sql
        self.errno     = errno
        self.required  = tuple(required)
        self.forbidden = tuple(forbidden)

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

class _SqlserverSetupStep:
    """Create a SQL Server database and run setup SQLs."""
    def __init__(self, database: str, sqls: List[str]):
        self.database = database
        self.sqls     = sqls

class _SqlserverDropDbStep:
    """Drop a SQL Server database (cleanup)."""
    def __init__(self, database: str):
        self.database = database

# ── Oracle observability helpers (FQOR-SYS-002~005) ───────────────────────

class _OracleFixtureStep:
    """Create the Oracle-side fixtures one observability case needs.

    Profiles: s002 (EXPLAIN namespace), s003 (two schemas, same table name),
    s004 (table behind a bad-password source), s005 (table plus views that
    raise ORA-00942 / ORA-01031 only when scanned).
    """
    def __init__(self, profile: str):
        self.profile = profile

class _OracleTeardownStep:
    """Drop the Oracle-side fixtures of a profile (idempotent, best effort)."""
    def __init__(self, profile: str):
        self.profile = profile

class _AssertQueryStep:
    """Run a query and assert concrete rows and/or text fragments of its result.

    ``rows``       — exact formatted rows (same format as recorded results).
    ``required``   — fragments the result text must contain (EXPLAIN plans).
    ``forbidden``  — fragments it must not contain (e.g. the other schema).
    """
    def __init__(self, sql: str, rows=None, required: Sequence[str] = (),
                 forbidden: Sequence[str] = (), ignore_case: bool = True):
        self.sql         = sql
        self.rows        = list(rows) if rows is not None else None
        self.required    = tuple(required)
        self.forbidden   = tuple(forbidden)
        self.ignore_case = ignore_case

class _LogAssertStep:
    """Assert fragments present/absent in the dnode-1 taosd log tail.

    Used by FQOR-SYS-004: connect-stage ORA text stays in the log, and the
    password used to build the Oracle DSN must never reach it.
    """
    def __init__(self, required: Sequence[str] = (), forbidden: Sequence[str] = (),
                 timeout: float = 15.0):
        self.required  = tuple(required)
        self.forbidden = tuple(forbidden)
        self.timeout   = timeout


# ── Step factory helpers ───────────────────────────────────────────────────

def _clear_source_step() -> _ClearSourceStep:
    return _ClearSourceStep()

def _connect(user: str, password: str) -> _ConnectStep:
    return _ConnectStep(user, password)

def _connect_root() -> _ConnectRootStep:
    return _ConnectRootStep()

def _error_step(sql: str, errno=None, required: Sequence[str] = (),
                forbidden: Sequence[str] = ()) -> _ErrorStep:
    return _ErrorStep(sql, errno=errno, required=required, forbidden=forbidden)


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

    ["sys006s", [_sqlserver], ["fq08_s006_s"],
     "SQL Server source: type=sqlserver, schema='dbo' stored in col[7], OPTIONS visible, password masked (covers SYS-001/SYS-005)",
     [
         "drop external source if exists fq08_s006_s",
         f"create external source fq08_s006_s type='sqlserver' host='{_S_HOST}' port={_S_PORT} user='{_S_USER}' password='{_S_PASS}' database='sqlsrvdb' schema='dbo' options('trust_server_certificate'='true','connect_timeout_ms'='3000')",
         "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options` from information_schema.ins_ext_sources where source_name='fq08_s006_s'",
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_s006_s' and create_time is not null",
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
     "InfluxDB OPTIONS: api_token masked; protocol value visible; options is valid JSON (covers SYS-017)",
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

    ["obs012ms", [_sqlserver], ["fq08_ob012m2", "fq08_ob012s"],
     "MySQL + SQL Server sources registered independently; each routes to external path; combined count=2; no interference (covers SYS-012, SQL Server leg)",
     [
         "drop external source if exists fq08_ob012m2",
         "drop external source if exists fq08_ob012s",
         _SqlserverSetupStep("fq08_ob012_ss_ext", [
             "IF OBJECT_ID('dbo.t1') IS NOT NULL DROP TABLE dbo.t1",
             "CREATE TABLE dbo.t1 (id INT PRIMARY KEY)",
         ]),
         f"create external source fq08_ob012m2 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'",
         f"create external source fq08_ob012s type='sqlserver' host='{_S_HOST}' port={_S_PORT} user='{_S_USER}' password='{_S_PASS}' database='fq08_ob012_ss_ext' schema='dbo'",
         "select source_name, `type` from information_schema.ins_ext_sources where source_name='fq08_ob012m2'",
         "select source_name, `type` from information_schema.ins_ext_sources where source_name='fq08_ob012s'",
         "select count(*) from information_schema.ins_ext_sources where source_name in ('fq08_ob012m2','fq08_ob012s')",
         _error_step("select * from fq08_ob012m2.testdb.t1 limit 1", errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         # Default database + schema resolve to fq08_ob012_ss_ext/dbo (created
         # above); the missing table must surface as TABLE_NOT_EXIST, proving
         # the query reached the SQL Server path rather than failing locally.
         _error_step("select * from fq08_ob012s.missing_t limit 1", errno=TSDB_CODE_EXT_TABLE_NOT_EXIST),
         _clear_source_step(),
         _SqlserverDropDbStep("fq08_ob012_ss_ext"),
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

    # === GROUP O: Oracle observability ======================================
    # Covers: FQOR-SYS-001 (type filter and field fidelity, password masked),
    # FQOR-SYS-002 (EXPLAIN names source, schema and table for both the
    # 3-segment and the 2-segment path), FQOR-SYS-003 (the schema effective
    # after USE is the one USE selected, not the source default), FQOR-SYS-004
    # (the ORA text and the source name reach the taosd log while the password
    # never does), FQOR-SYS-005 (namespace, object, privilege, auth and connect
    # failures stay distinguishable by code and text, and ORA-00942 keeps its
    # text).

    ["sys001o", [_oracle], ["fq08_s001_o"],
     "Oracle source: type='oracle' filter finds it; host/port/database/schema match the DDL and the password is masked (covers FQOR-SYS-001)",
     [
         "drop external source if exists fq08_s001_o",
         f"create external source fq08_s001_o type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options` "
         "from information_schema.ins_ext_sources where `type`='oracle' and source_name='fq08_s001_o'",
         # The stored row must equal the DDL values, with the password masked.
         _AssertQueryStep(
             "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema` "
             "from information_schema.ins_ext_sources where source_name='fq08_s001_o'",
             rows=[f"fq08_s001_o|oracle|{_O_HOST}|{_O_PORT}|{_O_USER}"
                   f"|******|{_O_SERVICE}|{_O_SCHEMA}"]),
         "select count(*) from information_schema.ins_ext_sources where source_name='fq08_s001_o' and create_time is not null",
         _clear_source_step(),
     ],
     ],

    ["sys002o", [_oracle], ["fq08_s002_o"],
     "Oracle EXPLAIN names source, schema and table; the 3-segment and the 2-segment path agree on the resolved schema and return the same rows (covers FQOR-SYS-002)",
     [
         _OracleFixtureStep("s002"),
         "drop external source if exists fq08_s002_o",
         f"create external source fq08_s002_o type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         # 3-segment path: source, schema and table all appear in the plan.
         _AssertQueryStep(
             f"explain select val from fq08_s002_o.{_O_SCHEMA}.fq08_s002_t order by val",
             required=(f"Federated Scan on fq08_s002_o.{_O_SCHEMA}.fq08_s002_t (oracle)",),
             forbidden=("Federated Scan on fq08_s002_o.fq08_s002_t (oracle)",)),
         _AssertQueryStep(
             f"select val from fq08_s002_o.{_O_SCHEMA}.fq08_s002_t order by val",
             rows=["10", "20"]),
         # 2-segment path: the plan must still name the effective (default) schema.
         _AssertQueryStep(
             "explain select val from fq08_s002_o.fq08_s002_t order by val",
             required=(f"Federated Scan on fq08_s002_o.{_O_SCHEMA}.fq08_s002_t (oracle)",)),
         _AssertQueryStep(
             "select val from fq08_s002_o.fq08_s002_t order by val",
             rows=["10", "20"]),
         _clear_source_step(),
         _OracleTeardownStep("s002"),
     ],
     ],

    ["sys003o", [_oracle], ["fq08_s003_o"],
     "Oracle USE: 'USE source' selects the configured schema, 'USE source.<schema>' switches the effective schema, and each context reads its own same-named table (covers FQOR-SYS-003)",
     [
         _OracleFixtureStep("s003"),
         "drop external source if exists fq08_s003_o",
         # Created before any USE so the local DDL never runs under an
         # external-source namespace.
         "create database if not exists fq08_local016",
         f"create external source fq08_s003_o type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         # Default namespace: USE source resolves to the configured schema.
         "use fq08_s003_o",
         _AssertQueryStep("select val from fq08_s003_t", rows=["100"]),
         _AssertQueryStep(
             "explain select val from fq08_s003_t",
             required=(f"Federated Scan on fq08_s003_o.{_O_SCHEMA}.fq08_s003_t (oracle)",),
             forbidden=(f"Federated Scan on fq08_s003_o.{_O_SCHEMA2}.fq08_s003_t (oracle)",)),
         # Explicit namespace: the same unqualified name now reads the other schema.
         f"use fq08_s003_o.{_O_SCHEMA2}",
         _AssertQueryStep("select val from fq08_s003_t", rows=["200"]),
         _AssertQueryStep(
             "explain select val from fq08_s003_t",
             required=(f"Federated Scan on fq08_s003_o.{_O_SCHEMA2}.fq08_s003_t (oracle)",),
             forbidden=(f"Federated Scan on fq08_s003_o.{_O_SCHEMA}.fq08_s003_t (oracle)",)),
         # Leave the session on a local database before the source is dropped.
         "use fq08_local016",
         _clear_source_step(),
         _OracleTeardownStep("s003"),
     ],
     ],

    ["sys004o", [_oracle], ["fq08_s004_o", "fq08_s004_o_auth"],
     "Oracle connect failure is logged with the source name and the raw ORA text, and the log never carries the DSN password (covers FQOR-SYS-004)",
     [
         _OracleFixtureStep("s004"),
         "drop external source if exists fq08_s004_o",
         f"create external source fq08_s004_o type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         # The valid source proves the instance is reachable: only the password
         # of the second source differs.
         _AssertQueryStep("select val from fq08_s004_o.fq08_s004_t", rows=["10"]),
         # The DDL stores the credentials without validating them; the first
         # query is what makes the connector build the DSN and fail to log in.
         "drop external source if exists fq08_s004_o_auth",
         f"create external source fq08_s004_o_auth type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_BAD_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         _error_step(
             "select count(*) from fq08_s004_o_auth.fq08_s004_t",
             errno=TSDB_CODE_EXT_AUTH_FAILED,
             required=("External source authentication failed",)),
         # The connector logs only the first diagnostic record (one
         # SQLGetDiagRec call into a 512-byte buffer), so the ORA number and its
         # first line are kept; the leak forms below must never appear.
         _LogAssertStep(
             required=("Oracle: connect failed source=fq08_s004_o_auth",
                       "nativeErr=1017",
                       "ORA-01017"),
             # The client debug log echoes the user's own CREATE statement,
             # so the bare password necessarily appears there for every
             # source type; what must never appear is the driver-level form
             # the connector would build (PWD=...), nor any DSN/wallet text.
             forbidden=(f"PWD={_O_PASS}", f"PWD={_O_BAD_PASS}",
                        "DBQ=", "jdbc:oracle:thin", "cwallet.sso")),
         _clear_source_step(),
         _OracleTeardownStep("s004"),
     ],
     ],

    ["sys005o", [_oracle], ["fq08_s005_o", "fq08_s005_o_auth", "fq08_s005_o_conn"],
     "Oracle error classes stay distinguishable (namespace vs object vs privilege vs auth vs connect) and a scan-time ORA-00942 keeps its raw text (covers FQOR-SYS-005)",
     [
         _OracleFixtureStep("s005"),
         "drop external source if exists fq08_s005_o",
         f"create external source fq08_s005_o type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         # a) Namespace missing: USE validates the schema against ALL_USERS.
         _error_step("use fq08_s005_o.NO_SUCH_SCHEMA",
                     errno=TSDB_CODE_EXT_DB_NOT_EXIST,
                     required=("External source database or schema not found",)),
         # b) Object missing: same source, existing schema, absent table.
         _error_step(f"select count(*) from fq08_s005_o.{_O_SCHEMA}.FQ08_S005_MISSING",
                     errno=TSDB_CODE_EXT_TABLE_NOT_EXIST,
                     required=("External table not found on remote source",)),
         # c) ORA-00942 raised while scanning is passed through verbatim: the
         #    view's column list resolves, the missing object surfaces only when
         #    the statement runs.
         _error_step("select * from fq08_s005_o.fq08_e942_v limit 1",
                     errno=TSDB_CODE_EXT_TABLE_NOT_EXIST,
                     required=("remote_code=942", "ORA-00942")),
         # d) Privilege missing: ORA-01031 raised while scanning is a distinct
         #    class from the ORA-00942 above.
         _error_step("select * from fq08_s005_o.fq08_deny_v limit 1",
                     errno=TSDB_CODE_EXT_ACCESS_DENIED,
                     required=("remote_code=1031", "ORA-01031")),
         # e) Authentication: wrong password, existing object.
         "drop external source if exists fq08_s005_o_auth",
         f"create external source fq08_s005_o_auth type='oracle' host='{_O_HOST}' port={_O_PORT} "
         f"user='{_O_USER}' password='{_O_BAD_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}'",
         _error_step("select count(*) from fq08_s005_o_auth.fq08_s005_t",
                     errno=TSDB_CODE_EXT_AUTH_FAILED,
                     required=("External source authentication failed",)),
         # f) Connect failure: closed port, bounded by the per-source timeout.
         "drop external source if exists fq08_s005_o_conn",
         f"create external source fq08_s005_o_conn type='oracle' host='127.0.0.1' port=1 "
         f"user='{_O_USER}' password='{_O_PASS}' database='{_O_SERVICE}' schema='{_O_SCHEMA}' "
         f"options('connect_timeout_ms'='500')",
         _error_step("select count(*) from fq08_s005_o_conn.fq08_s005_t",
                     errno=TSDB_CODE_EXT_CONNECT_FAILED,
                     required=("External source connection failed",)),
         _clear_source_step(),
         _OracleTeardownStep("s005"),
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

    def _fw_runtime_sql(self, sql: str, src_type: str) -> str:
        """Patch generic import-time Influx source SQL to runtime protocol."""
        if src_type != _influxdb:
            return sql
        return self._rewrite_influx_source_runtime_sql(sql)

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

    # ── Oracle fixture management (FQOR-SYS-002~005) ───────────────────────

    @staticmethod
    def _fw_oracle_cfg():
        return next(ExtSrcEnv.oracle_version_configs())

    @staticmethod
    def _fw_drop_oracle_object(kind: str, name: str) -> str:
        """Idempotent DROP: Oracle has no DROP <object> IF EXISTS."""
        return (f"BEGIN EXECUTE IMMEDIATE 'DROP {kind} {name}'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;")

    def _fw_oracle_fixtures(self, profile: str):
        """Create the Oracle objects a case queries; raises on failure."""
        cfg = self._fw_oracle_cfg()
        if profile == "s002":
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                self._fw_drop_oracle_object("TABLE", "FQ08_S002_T"),
                "CREATE TABLE FQ08_S002_T (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
                "INSERT INTO FQ08_S002_T VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 10)",
                "INSERT INTO FQ08_S002_T VALUES (TIMESTAMP '2024-01-01 00:00:01.000', 20)",
                "COMMIT",
            ])
            return
        if profile == "s003":
            # The same table name in two schemas: the row value identifies the
            # schema the query actually read.
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                self._fw_drop_oracle_object("TABLE", "FQ08_S003_T"),
                "CREATE TABLE FQ08_S003_T (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
                "INSERT INTO FQ08_S003_T VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 100)",
                "COMMIT",
            ])
            # A schema is an account, so the alternate namespace is created
            # while logged in as that account.
            ExtSrcEnv._oracle_sql(cfg, [
                self._fw_drop_oracle_object("TABLE", "FQ08_S003_T"),
                "CREATE TABLE FQ08_S003_T (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
                "INSERT INTO FQ08_S003_T VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 200)",
                "COMMIT",
            ], user=ExtSrcEnv.ORACLE_ALT_USER,
               password=ExtSrcEnv.ORACLE_ALT_PASS)
            # Cross-schema reads need an explicit grant.
            ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
                f'GRANT SELECT ON "{_O_SCHEMA2}"."FQ08_S003_T" TO "{_O_SCHEMA}"',
            ])
            return
        if profile == "s004":
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                self._fw_drop_oracle_object("TABLE", "FQ08_S004_T"),
                "CREATE TABLE FQ08_S004_T (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
                "INSERT INTO FQ08_S004_T VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 10)",
                "COMMIT",
            ])
            return
        if profile == "s005":
            # A SYSTEM-owned AUTHID CURRENT_USER function parses its argument
            # only when the statement runs.  The view over it therefore
            # compiles (its columns resolve from the dictionary) and raises the
            # error of the embedded statement at scan time.
            ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
                "CREATE OR REPLACE FUNCTION SYSTEM.FQ08_RAISE(p_sql VARCHAR2) "
                "RETURN NUMBER AUTHID CURRENT_USER IS\n"
                "  c SYS_REFCURSOR;\n"
                "  x NUMBER;\n"
                "BEGIN\n"
                "  OPEN c FOR p_sql;\n"
                "  FETCH c INTO x;\n"
                "  CLOSE c;\n"
                "  RETURN NVL(x, 0);\n"
                "END;",
                "GRANT EXECUTE ON SYSTEM.FQ08_RAISE TO PUBLIC",
                # ALTER SYSTEM is not a query, so it cannot be opened as a
                # cursor; EXECUTE IMMEDIATE is what makes the test account hit
                # ORA-01031 (insufficient privileges) at scan time.
                "CREATE OR REPLACE FUNCTION SYSTEM.FQ08_DENY_FN "
                "RETURN NUMBER AUTHID CURRENT_USER IS\n"
                "BEGIN\n"
                "  EXECUTE IMMEDIATE 'ALTER SYSTEM FLUSH SHARED_POOL';\n"
                "  RETURN 1;\n"
                "END;",
                "GRANT EXECUTE ON SYSTEM.FQ08_DENY_FN TO PUBLIC",
            ])
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                self._fw_drop_oracle_object("VIEW", "FQ08_E942_V"),
                self._fw_drop_oracle_object("VIEW", "FQ08_DENY_V"),
                self._fw_drop_oracle_object("TABLE", "FQ08_S005_T"),
                "CREATE TABLE FQ08_S005_T (TS TIMESTAMP(3) PRIMARY KEY, VAL NUMBER(10,0))",
                "INSERT INTO FQ08_S005_T VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 10)",
                "COMMIT",
                "CREATE OR REPLACE VIEW FQ08_E942_V AS SELECT VAL, "
                "CAST(SYSTEM.FQ08_RAISE('SELECT VAL FROM FQ08_S005_MISSING') "
                "AS NUMBER(10,0)) AS BOOM FROM FQ08_S005_T WHERE ROWNUM <= 1",
                "CREATE OR REPLACE VIEW FQ08_DENY_V AS SELECT VAL, "
                "CAST(SYSTEM.FQ08_DENY_FN AS NUMBER(10,0)) AS BOOM "
                "FROM FQ08_S005_T WHERE ROWNUM <= 1",
            ])
            return
        raise ValueError(f"Unknown Oracle fixture profile: {profile}")

    def _fw_oracle_fixture_cleanup(self, profile: str):
        """Drop a profile's Oracle objects; failures are logged, not raised."""
        cfg = self._fw_oracle_cfg()
        try:
            if profile == "s002":
                ExtSrcEnv.oracle_exec_cfg(
                    cfg, [self._fw_drop_oracle_object("TABLE", "FQ08_S002_T")])
            elif profile == "s003":
                ExtSrcEnv.oracle_exec_cfg(
                    cfg, [self._fw_drop_oracle_object("TABLE", "FQ08_S003_T")])
                ExtSrcEnv._oracle_sql(
                    cfg, [self._fw_drop_oracle_object("TABLE", "FQ08_S003_T")],
                    user=ExtSrcEnv.ORACLE_ALT_USER,
                    password=ExtSrcEnv.ORACLE_ALT_PASS)
            elif profile == "s004":
                ExtSrcEnv.oracle_exec_cfg(
                    cfg, [self._fw_drop_oracle_object("TABLE", "FQ08_S004_T")])
            elif profile == "s005":
                ExtSrcEnv.oracle_exec_cfg(cfg, [
                    self._fw_drop_oracle_object("VIEW", "FQ08_E942_V"),
                    self._fw_drop_oracle_object("VIEW", "FQ08_DENY_V"),
                    self._fw_drop_oracle_object("TABLE", "FQ08_S005_T"),
                ])
                ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
                    self._fw_drop_oracle_object("FUNCTION", "SYSTEM.FQ08_RAISE"),
                    self._fw_drop_oracle_object("FUNCTION", "SYSTEM.FQ08_DENY_FN"),
                ])
            else:
                raise ValueError(f"Unknown Oracle fixture profile: {profile}")
        except Exception as e:  # best effort: the case result is authoritative
            tdLog.info(f"[fq08] Oracle fixture cleanup {profile} failed: {e}")

    @staticmethod
    def _fw_read_client_log_tail(log_dir: str) -> str:
        """Read the client-side log tail.

        Connector calls made while the statement is translated run in the
        client library (libtaosnative), so their diagnostics land in the psim
        log, not in the dnode log.  The psim log directory is derived from the
        dnode log path (<sim>/dnode<N>/log -> <sim>/psim/log).
        """
        sim_root = os.path.dirname(os.path.dirname(log_dir))
        client_dir = os.path.join(sim_root, "psim", "log")
        if not os.path.isdir(client_dir):
            return ""
        return TestFq08SystemObservability._fw_read_dnode_log_tail(client_dir)

    @staticmethod
    def _fw_read_dnode_log_tail(log_dir: str) -> str:
        """Read the tail of every taosd log file of one dnode."""
        chunks = []
        # taosd log files are taosdlog*; the client library writes taoslog*.
        # Reading both patterns keeps this helper usable for either directory.
        for pattern in ("taosdlog*", "taoslog*"):
            for path in sorted(glob.glob(os.path.join(log_dir, pattern))):
                try:
                    size = os.path.getsize(path)
                    with open(path, "rb") as f:
                        if size > _LOG_TAIL_BYTES:
                            f.seek(size - _LOG_TAIL_BYTES)
                        chunks.append(f.read().decode("utf-8", "replace"))
                except OSError:
                    continue
        return "\n".join(chunks)

    def _fw_exec_step(self, step, src_type: str, source_names: List[str],
                       blocks: List[str], label: str, step_tag: str):
        """Execute one step and append serialised result block(s) to blocks."""

        # ── callable (e.g. lambda returning a SQL string or sentinel) ──────
        if callable(step):
            self._fw_exec_step(step(src_type), src_type, source_names, blocks, label, step_tag)
            return

        # ── SQL string ──────────────────────────────────────────────────────
        if isinstance(step, str):
            sql      = self._fw_runtime_sql(step, src_type)
            stripped = sql.strip()
            is_create = bool(re.match(r"create\s+external\s+source", stripped, re.IGNORECASE))
            is_query  = bool(re.match(r"(select|show|describe|explain)\b", stripped, re.IGNORECASE))

            if is_query:
                ok = self._fw_query_once(sql, exit=False)
                rows = (self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult or [])
                        if ok is not False else ["ERROR: query failed"])
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
            sql = self._fw_runtime_sql(step.sql, src_type)
            err_text = ""
            try:
                tdSql.cursor.execute(sql)
                result = "UNEXPECTED_OK (expected error)"
            except Exception as e:
                err_text = str(e)
                err_code = getattr(e, "errno", None)
                if step.errno is not None and err_code is not None:
                    if (err_code & 0xFFFFFFFF) != (step.errno & 0xFFFFFFFF):
                        result = (f"WRONG_ERROR: expected 0x{step.errno & 0xFFFFFFFF:08x} "
                                  f"got 0x{err_code & 0xFFFFFFFF:08x}")
                    else:
                        result = "ERROR_OK"
                else:
                    result = "ERROR_OK"
            if step.required or step.forbidden:
                missing = [f for f in step.required if f.lower() not in err_text.lower()]
                leaked  = [f for f in step.forbidden if f.lower() in err_text.lower()]
                if missing or leaked:
                    self._fw_append_step_block(blocks, label, step_tag, "ERROR", sql, result)
                    raise AssertionError(
                        f"[{label} {step_tag}] error message mismatch: "
                        f"missing={missing!r} forbidden={leaked!r} "
                        f"message={err_text!r}")
                result += "; TEXT-OK"
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

        # ── _SqlserverSetupStep: create SQL Server DB + run setup SQLs ──────
        if isinstance(step, _SqlserverSetupStep):
            sql = f"-- sqlserver setup database={step.database}"
            try:
                cfg = next(ExtSrcEnv.sqlserver_version_configs())
                ExtSrcEnv.sqlserver_create_db_cfg(cfg, step.database)
                ExtSrcEnv.sqlserver_exec_cfg(cfg, step.database, step.sqls)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "SQLSERVER-SETUP", sql, result)
            return

        # ── _SqlserverDropDbStep: drop SQL Server database ──────────────────
        if isinstance(step, _SqlserverDropDbStep):
            sql = f"-- sqlserver drop database={step.database}"
            try:
                cfg = next(ExtSrcEnv.sqlserver_version_configs())
                ExtSrcEnv.sqlserver_drop_db_cfg(cfg, step.database)
                result = "OK"
            except Exception as _e:
                result = f"ERROR: {str(_e)[:200]}"
            self._fw_append_step_block(blocks, label, step_tag, "SQLSERVER-CLEANUP", sql, result)
            return

        # ── _OracleFixtureStep: create the Oracle-side fixtures ─────────────
        if isinstance(step, _OracleFixtureStep):
            sql = f"-- oracle fixtures profile={step.profile}"
            self._fw_oracle_fixtures(step.profile)
            self._fw_append_step_block(blocks, label, step_tag, "ORACLE-SETUP", sql, "OK")
            return

        # ── _OracleTeardownStep: drop the Oracle-side fixtures ──────────────
        if isinstance(step, _OracleTeardownStep):
            sql = f"-- oracle fixture cleanup profile={step.profile}"
            self._fw_oracle_fixture_cleanup(step.profile)
            self._fw_append_step_block(blocks, label, step_tag, "ORACLE-CLEANUP", sql, "OK")
            return

        # ── _AssertQueryStep: assert exact rows / result text ───────────────
        if isinstance(step, _AssertQueryStep):
            sql = self._fw_runtime_sql(step.sql, src_type)
            ok  = self._fw_query_once(sql, exit=False)
            if ok is False:
                self._fw_append_step_block(blocks, label, step_tag, "ASSERT", sql,
                                           "ERROR: query failed")
                raise AssertionError(f"[{label} {step_tag}] assert query failed: {sql}")
            rows = self._fw_fmt_rows(tdSql.queryResult or [])
            text = "\n".join(rows)
            probe = text.lower() if step.ignore_case else text
            missing = [f for f in step.required
                       if (f.lower() if step.ignore_case else f) not in probe]
            leaked  = [f for f in step.forbidden
                       if (f.lower() if step.ignore_case else f) in probe]
            problem = []
            if step.rows is not None and rows != step.rows:
                problem.append(f"rows {rows!r} != expected {step.rows!r}")
            if missing:
                problem.append(f"missing fragments {missing!r}")
            if leaked:
                problem.append(f"forbidden fragments {leaked!r}")
            self._fw_append_step_block(blocks, label, step_tag, "ASSERT", sql, rows)
            if problem:
                raise AssertionError(
                    f"[{label} {step_tag}] " + "; ".join(problem) + f"\nSQL: {sql}")
            return

        # ── _LogAssertStep: assert dnode log fragments ──────────────────────
        if isinstance(step, _LogAssertStep):
            deadline = time.monotonic() + step.timeout
            log_dir = sc.dnodeLogPath(1)
            missing, leaked = list(step.required), []
            while True:
                text = (self._fw_read_dnode_log_tail(log_dir) + "\n" +
                        self._fw_read_client_log_tail(log_dir))
                missing = [f for f in step.required if f.lower() not in text.lower()]
                leaked  = [f for f in step.forbidden if f.lower() in text.lower()]
                if leaked or not missing or time.monotonic() >= deadline:
                    break
                time.sleep(1.0)
            sql = f"-- scan taosd and client logs under {log_dir}"
            result = "LOG-OK"
            if missing or leaked:
                result = (f"LOG-MISMATCH: missing={missing!r} "
                          f"forbidden={leaked!r}")
            self._fw_append_step_block(blocks, label, step_tag, "LOG-ASSERT", sql, result)
            if missing or leaked:
                raise AssertionError(
                    f"[{label} {step_tag}] dnode log assertion failed: {result}")
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
        actual   = ExtSrcEnv.baseline_environment("\n".join(blocks) + "\n")
        import re as _re
        _PROTOCOL_SQL_NORM = _re.compile(
            r"('protocol'\s*=\s*')(?:flight_sql|http)(')",
            _re.IGNORECASE,
        )
        _PROTOCOL_JSON_NORM = _re.compile(
            r'("protocol"\s*:\s*")(?:flight_sql|http)(")',
            _re.IGNORECASE,
        )
        def _norm_protocol(text: str) -> str:
            text = _PROTOCOL_SQL_NORM.sub(r"\1<PROTO>\2", text)
            text = _PROTOCOL_JSON_NORM.sub(r'\1<PROTO>\2', text)
            text = text.replace("protocol value visible", "protocol visible")
            return _re.sub(r"protocol='(?:flight_sql|http)' visible",
                           "protocol visible", text, flags=_re.IGNORECASE)
        # The dnode log directory is the one harness-owned path in the
        # transcript: the checked-in baseline recorded the Linux tree
        # (/local/tsdb/sim/dnode1/log) and Windows runs from
        # D:\local\TDinternal\sim\dnode1\log.  The step asserts the same log
        # content on both, so only the directory spelling is normalized.
        _DNODE_LOG_DIR_NORM = _re.compile(
            r"(?<=scan taosd and client logs under )[^\r\n]+")
        def _norm_paths(text: str) -> str:
            return _DNODE_LOG_DIR_NORM.sub("<DNODE_LOG>", text)
        actual = _norm_paths(_norm_protocol(actual))
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
        expected = _norm_paths(_norm_protocol(expected))

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
