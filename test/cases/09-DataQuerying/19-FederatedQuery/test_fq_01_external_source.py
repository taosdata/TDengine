"""
test_fq_01_external_source.py

Simplified data-driven framework for external source lifecycle tests.
Case format: [types, source_names, desc, steps]

Original legacy tests are preserved in:
  test_fq_01_external_source.py.bak
"""

import os
import re
import tempfile
import time
from typing import Any, List, Optional, Sequence, Tuple, Union

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
)

# ── Connection globals (from ExtSrcEnv class-level attributes) ─────────────
_M_HOST = ExtSrcEnv.MYSQL_HOST
_M_PORT = ExtSrcEnv.MYSQL_PORT
_M_USER = ExtSrcEnv.MYSQL_USER
_M_PASS = ExtSrcEnv.MYSQL_PASS
_M_DB   = "fq01_fw_mdb"

_P_HOST   = ExtSrcEnv.PG_HOST
_P_PORT   = ExtSrcEnv.PG_PORT
_P_USER   = ExtSrcEnv.PG_USER
_P_PASS   = ExtSrcEnv.PG_PASS
_P_DB     = "fq01_fw_pdb"
_P_SCHEMA = "public"

_I_HOST  = ExtSrcEnv.INFLUX_HOST
_I_PORT  = ExtSrcEnv.INFLUX_PORT
_I_TOKEN = ExtSrcEnv.INFLUX_TOKEN
_I_DB    = "fq01_fw_idb"

_FW_TABLE = "src_t"
_MASKED   = "******"
_BASE_TS  = 1_704_067_200_000  # 2024-01-01T00:00:00Z in ms

_FW_ROWS_DT = [
    ("2024-01-01 00:00:00.000", 1, 1.5, "alpha",   1),
    ("2024-01-01 00:01:00.000", 2, 2.5, "beta",    0),
    ("2024-01-01 00:02:00.000", 3, 3.5, "gamma",   1),
    ("2024-01-01 00:03:00.000", 4, 4.5, "delta",   0),
    ("2024-01-01 00:04:00.000", 5, 5.5, "epsilon", 1),
]

# SHOW EXTERNAL SOURCES column indices
_COL_NAME     = 0
_COL_TYPE     = 1
_COL_HOST     = 2
_COL_PORT     = 3
_COL_USER     = 4
_COL_PASSWORD = 5
_COL_DATABASE = 6
_COL_SCHEMA   = 7
_COL_OPTIONS  = 8
_COL_CTIME    = 9

_DYNAMIC_RESULT_COLUMNS = {"create_time", "ctime"}
_SQL_RESERVED_WORDS = {
    "select", "from", "where", "show", "describe", "create", "drop", "alter", "refresh",
    "database", "schema", "type", "user", "password", "host", "port", "options",
}

# ── Step sentinel classes ──────────────────────────────────────────────────

class _QCountStep:
    """SELECT COUNT(*) against the primary source's standard table.
    Positive (negative=False): retries every 0.5 s up to 20 s until count matches.
    Negative (negative=True):  single attempt, no retry; failure is the expected outcome."""
    def __init__(self, negative: bool = False, count: int = 5):
        self.negative = negative
        self.count    = count

class _ClearSourceStep:
    """DROP all sources listed in the current case's source_names."""
    pass

class _ConnectStep:
    """Switch the active tdSql connection."""
    def __init__(self, user: str, password: str):
        self.user     = user
        self.password = password

class _ConnectRootStep:
    """Restore tdSql connection to root / taosdata."""
    pass

# ── Step helper functions ──────────────────────────────────────────────────

def _q_count_step(negative: bool = False, count: int = 5) -> _QCountStep:
    return _QCountStep(negative=negative, count=count)

def _clear_source_step() -> _ClearSourceStep:
    return _ClearSourceStep()

def _connect(user: str, password: str) -> _ConnectStep:
    return _ConnectStep(user, password)

def _connect_root() -> _ConnectRootStep:
    return _ConnectRootStep()

def _mysql_exec_step(database: Optional[str], sqls: Union[str, Sequence[str]], marker_sql: str = "select 1"):
    """Run side-effect SQL directly on MySQL and return a local marker query step.

    This is used for REFRESH/schema-effect scenarios where remote DDL must be
    applied between federated queries.
    """
    if isinstance(sqls, str):
        statements = [sqls]
    else:
        statements = [str(s) for s in sqls]

    def _step(src_type: str):
        if src_type == _mysql:
            ExtSrcEnv.mysql_exec(database, statements)
        return marker_sql

    return _step

def _pg_exec_step(database: Optional[str], sqls: Union[str, Sequence[str]], marker_sql: str = "select 1"):
    """Run side-effect SQL directly on PostgreSQL and return a local marker query step.

    This is used for REFRESH/schema-effect scenarios where remote DDL must be
    applied between federated queries.
    """
    if isinstance(sqls, str):
        statements = [sqls]
    else:
        statements = [str(s) for s in sqls]

    def _step(src_type: str):
        if src_type == _pg:
            ExtSrcEnv.pg_exec(database, statements)
        return marker_sql

    return _step

# ── Type shortcuts ─────────────────────────────────────────────────────────
_mysql    = "mysql"
_pg       = "postgresql"
_influxdb = "influxdb"
# ── Case list ──────────────────────────────────────────────────────────────
# Format: [types, source_names, desc, steps]
#
#  source_names – pool of source names used by this case (usually 1)
#  desc         – plain-text description
#  steps        – list of SQL strings, lambdas, _q_count_step(), etc.
#
# Framework automatic behaviour:
#  • Every successful CREATE EXTERNAL SOURCE SQL auto-triggers
#    SHOW EXTERNAL SOURCES (filtered) + DESCRIBE EXTERNAL SOURCE.
#  • All SQL + results are serialised to the result file.
#  • _q_count_step() retries until count == 5 (positive) or runs once (negative).
#  • _clear_source_step() drops every name in source_names, ignoring errors.
_CASES = [

    ## 001m
    ["001m", [_mysql], ["fq01_src001m"], "basic create / show (auto) / describe (auto) / query / drop", [
            "drop external source if exists fq01_src001m",
            f"create external source fq01_src001m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 001p
    ["001p", [_pg], ["fq01_src001p"], "basic create / show (auto) / describe (auto) / query / drop", [
            "drop external source if exists fq01_src001p",
            f"create external source fq01_src001p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 001i
    ["001i", [_influxdb], ["fq01_src001i"], "basic create / show (auto) / describe (auto) / query / drop", [
            "drop external source if exists fq01_src001i",
            f"create external source fq01_src001i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 002
    ["002", [_mysql], ["fq01_src002"], "CREATE IF NOT EXISTS is idempotent; second create with different params is ignored", [
            "drop external source if exists fq01_src002",
            f"create external source fq01_src002 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source if not exists fq01_src002 type='mysql' host='{_M_HOST}' port=13307 user='alt_user' password='alt_pass' database='alt_db'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 003
    ["003", [_mysql], ["fq01_src003"], "duplicate CREATE raises ALREADY_EXISTS", [
            "drop external source if exists fq01_src003",
            f"create external source fq01_src003 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src003 type='mysql' host='{_M_HOST}' port=13307 user='alt_user' password='alt_pass' database='alt_db'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 004
    ["004", [_mysql], ["fq01_src004"], "ALTER options then re-query", [
            "drop external source if exists fq01_src004",
            f"create external source fq01_src004 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "alter external source fq01_src004 set options('connect_timeout_ms'='1500')",
            "describe external source fq01_src004",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 005
    ["005", [_mysql], ["fq01_src005"], "REFRESH is idempotent", [
            "drop external source if exists fq01_src005",
            f"create external source fq01_src005 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "refresh external source fq01_src005",
            _q_count_step(),
            "refresh external source fq01_src005",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 006
    ["006", [_mysql], ["fq01_src006"], "DROP IF EXISTS is idempotent", [
            "drop external source if exists fq01_src006",
            f"create external source fq01_src006 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "drop external source if exists fq01_src006",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006'",
            "drop external source if exists fq01_src006",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006'",
        ],
    ],

    ## 007
    ["007", [_mysql], ["fq01_src007"], "DROP without IF EXISTS on absent source raises NOT_EXIST", [
            "drop external source if exists fq01_src007",
            "drop external source fq01_src007",
        ],
    ],

    ## 008m
    ["008m", [_mysql], ["fq01_src008m"], "ALTER TYPE is always denied; each type attempts to change to a different target", [
            "drop external source if exists fq01_src008m",
            f"create external source fq01_src008m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "alter external source fq01_src008m set type='postgresql'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 008p
    ["008p", [_pg], ["fq01_src008p"], "ALTER TYPE is always denied; each type attempts to change to a different target", [
            "drop external source if exists fq01_src008p",
            f"create external source fq01_src008p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "alter external source fq01_src008p set type='influxdb'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 008i
    ["008i", [_influxdb], ["fq01_src008i"], "ALTER TYPE is always denied; each type attempts to change to a different target", [
            "drop external source if exists fq01_src008i",
            f"create external source fq01_src008i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "alter external source fq01_src008i set type='mysql'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 009
    ["009", [_mysql], ["fq01_src009"], "unknown option key is a syntax error", [
            "drop external source if exists fq01_src009",
            f"create external source fq01_src009 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('unknown_fw_opt'='1')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src009'",
        ],
    ],

    ## 010
    ["010", [_mysql], ["fq01_src010"], "source name conflicts with an existing local database name", [
            "drop external source if exists fq01_src010",
            "drop database if exists fq01_src010",
            "create database fq01_src010",
            f"create external source fq01_src010 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop database fq01_src010",
            f"create external source fq01_src010 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 011
    ["011", [_mysql], ["fq01_src011"], "type= value is case-insensitive (Mysql, MYSQL both accepted)", [
            "drop external source if exists fq01_src011",
            f"create external source fq01_src011 type='Mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
            f"create external source fq01_src011 type='MYSQL' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 012
    ["012", [_mysql], ["fq01_src012"], "reserved type 'tdengine' is rejected at create time", [
            "drop external source if exists fq01_src012",
            f"create external source fq01_src012 type='tdengine' host='{_M_HOST}' port=6030 user='root' password='taosdata' database='{_M_DB}'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src012'",
        ],
    ],

    ## 013
    ["013", [_mysql], ["fq01_src013"], "missing mandatory fields each produce a syntax error", [
            "drop external source if exists fq01_src013",
            # missing type=
            f"create external source fq01_src013 host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            # missing host=
            f"create external source fq01_src013 type='mysql' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            # missing port=
            f"create external source fq01_src013 type='mysql' host='{_M_HOST}' user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            # missing user=
            f"create external source fq01_src013 type='mysql' host='{_M_HOST}' port={_M_PORT} password='{_M_PASS}' database='{_M_DB}'",
            # missing password= (errno varies by implementation; just check any error)
            f"create external source fq01_src013 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' database='{_M_DB}'",
            # bare name only
            "create external source fq01_src013",
            # valid create at end confirms parser is not stuck
            f"create external source fq01_src013 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 014
    ["014", [_mysql], ["fq01_src014"], "cross-db option (e.g. PG's sslmode= used in mysql source) is syntax error", [
            "drop external source if exists fq01_src014",
            f"create external source fq01_src014 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('sslmode'='require')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src014'",
            f"create external source fq01_src014 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 015
        ["015", [_mysql], ["fq01_src015"], "TLS: cert-only or key-only raises CONFLICT; cert+key+disabled=false is accepted", [
            "drop external source if exists fq01_src015",
            f"create external source fq01_src015 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='true', 'tls_client_cert'='FAKECERT')",
            f"create external source fq01_src015 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='true', 'tls_client_key'='FAKEKEY')",
            f"create external source fq01_src015 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='false', 'tls_client_cert'='FAKECERT', 'tls_client_key'='FAKEKEY')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 016m
    ["016m", [_mysql], ["fq01_src016m"], "type-specific options persist and alter", [
            "drop external source if exists fq01_src016m",
            f"create external source fq01_src016m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('charset'='utf8mb4', 'ssl_mode'='preferred')",
            "describe external source fq01_src016m",
            _q_count_step(),
            "alter external source fq01_src016m set options('ssl_mode'='required')",
            "describe external source fq01_src016m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 016p
    ["016p", [_pg], ["fq01_src016p"], "type-specific options persist and alter", [
            "drop external source if exists fq01_src016p",
            f"create external source fq01_src016p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('sslmode'='prefer')",
            "describe external source fq01_src016p",
            _q_count_step(),
            "alter external source fq01_src016p set options('sslmode'='require')",
            "describe external source fq01_src016p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 016i
    ["016i", [_influxdb], ["fq01_src016i"], "type-specific options persist and alter", [
            "drop external source if exists fq01_src016i",
            f"create external source fq01_src016i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "describe external source fq01_src016i",
            _q_count_step(),
            f"alter external source fq01_src016i set options('api_token'='{_I_TOKEN}', 'protocol'='http')",
            "describe external source fq01_src016i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 017m
    ["017m", [_mysql], ["fq01_src017m"], "sensitive fields (password / api_token) are always masked in SHOW and DESCRIBE", [
            "drop external source if exists fq01_src017m",
            # Create with a deliberately wrong secret – connectivity not the goal
            f"create external source fq01_src017m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='AlwaysMask!987' database='{_M_DB}'",
            "describe external source fq01_src017m",
            # Alter to a different secret
            "alter external source fq01_src017m set password='NewMask!654'",
            "describe external source fq01_src017m",
            _clear_source_step(),
        ],
    ],

    ## 017p
    ["017p", [_pg], ["fq01_src017p"], "sensitive fields (password / api_token) are always masked in SHOW and DESCRIBE", [
            "drop external source if exists fq01_src017p",
            # Create with a deliberately wrong secret – connectivity not the goal
            f"create external source fq01_src017p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='AlwaysMask!987' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "describe external source fq01_src017p",
            # Alter to a different secret
            "alter external source fq01_src017p set password='NewMask!654'",
            "describe external source fq01_src017p",
            _clear_source_step(),
        ],
    ],

    ## 017i
    ["017i", [_influxdb], ["fq01_src017i"], "sensitive fields (password / api_token) are always masked in SHOW and DESCRIBE", [
            "drop external source if exists fq01_src017i",
            # Create with a deliberately wrong secret – connectivity not the goal
            f"create external source fq01_src017i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='tok_secret_123456', 'protocol'='flight_sql')",
            "describe external source fq01_src017i",
            # Alter to a different secret
            "alter external source fq01_src017i set options('api_token'='tok_secret_new_654321', 'protocol'='flight_sql')",
            "describe external source fq01_src017i",
            _clear_source_step(),
        ],
    ],

    ## 018
    ["018", [_mysql], ["fq01_src018"], "ALTER with multiple fields combined", [
            "drop external source if exists fq01_src018",
            f"create external source fq01_src018 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            f"alter external source fq01_src018 set host='{_M_HOST}', port={_M_PORT}, user='{_M_USER}', password='{_M_PASS}'",
            "describe external source fq01_src018",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 019m
    ["019m", [_mysql], ["fq01_src019m"], "ALTER database then revert (mysql; no schema clause)", [
            "drop external source if exists fq01_src019m",
            f"create external source fq01_src019m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "alter external source fq01_src019m set database='fw_alt_mdb'",
            "describe external source fq01_src019m",
            f"alter external source fq01_src019m set database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 019p
    ["019p", [_pg], ["fq01_src019p"], "ALTER database + schema then revert (postgresql)", [
            "drop external source if exists fq01_src019p",
            f"create external source fq01_src019p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "alter external source fq01_src019p set database='fw_alt_pdb', schema='alt_schema'",
            "describe external source fq01_src019p",
            f"alter external source fq01_src019p set database='{_P_DB}', schema='{_P_SCHEMA}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 020
    ["020", [_mysql], ["fq01_src020"], "repeated drop / re-create semantics", [
            "drop external source if exists fq01_src020",
            f"create external source fq01_src020 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "drop external source if exists fq01_src020",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020'",
            "drop external source if exists fq01_src020",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020'",
            f"create external source fq01_src020 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "drop external source fq01_src020",
            "drop external source fq01_src020",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020'",
        ],
    ],

    ## 021
    ["021", [_mysql], ["fq01_src021"], "DESCRIBE on non-existent source raises NOT_EXIST", [
            "drop external source if exists fq01_src021",
            "describe external source fq01_src021_ghost",
            f"create external source fq01_src021 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
            "describe external source fq01_src021",
        ],
    ],

    ## 022
    ["022", [_mysql], ["fq01_src022"], "REFRESH on non-existent source raises NOT_EXIST", [
            "drop external source if exists fq01_src022",
            "refresh external source fq01_src022_ghost",
            f"create external source fq01_src022 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "refresh external source fq01_src022",
            _clear_source_step(),
            "refresh external source fq01_src022",
        ],
    ],

    ## 023
    ["023", [_mysql], ["fq01_src023"], "ALTER on non-existent source raises NOT_EXIST", [
            "drop external source if exists fq01_src023",
            "alter external source fq01_src023_ghost set password='x'",
            "alter external source fq01_src023_ghost set host='127.0.0.1'",
            f"create external source fq01_src023 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
            "alter external source fq01_src023 set password='x'",
        ],
    ],

    ## 024
    ["024", [_mysql], ["fq01_src024"], "source name identifier length boundary: 64 chars ok, 65 rejected", [
            f"drop external source if exists {'x' * 64}",
            f"create external source {'x' * 64} type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"select count(*) from {'x' * 64}.{_M_DB}.{_FW_TABLE}",
            f"drop external source {'x' * 64}",
            f"create external source {'x' * 65} type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source 12345 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source '' type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
        ],
    ],

    ## 025
    ["025", [_mysql], ["fq01_src025"], "host / user / password / database too long each raise NAME_OR_PASSWD_TOO_LONG", [
            "drop external source if exists fq01_src025",
            f"create external source fq01_src025 type='mysql' host='{'h' * 257}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src025 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{'u' * 129}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src025 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{'p' * 129}' database='{_M_DB}'",
            f"create external source fq01_src025 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{'d' * 65}'",
            f"create external source fq01_src025 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 026
    ["026", [_mysql], ["fq01_src026"], "port boundary: 0 and 65536 rejected; then a valid port create succeeds", [
            "drop external source if exists fq01_src026",
            f"create external source fq01_src026 type='mysql' host='{_M_HOST}' port=0 user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src026 type='mysql' host='{_M_HOST}' port=65536 user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src026 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 027
    ["027", [_mysql], ["fq01_src027"], "option key/value length and total JSON length boundary", [
            "drop external source if exists fq01_src027",
            # key 65 chars → too long
            f"create external source fq01_src027 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('{'k' * 65}'='v')",
            # key 64 chars → unknown key (syntax error)
            f"create external source fq01_src027 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('{'k' * 64}'='v')",
            # value 4095 chars → max valid value length
            f"create external source fq01_src027 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_ca_cert'='{'v' * 4095}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src027'",
            _q_count_step(),
            _clear_source_step(),
            # value 4096 chars → over max value length, must be rejected
            f"create external source fq01_src027 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_ca_cert'='{'v' * 4096}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src027'",
        ],
    ],

    ## 028
    ["028", [_mysql], ["fq01_src028"], "ins_ext_sources reflects CREATE / ALTER / DROP lifecycle", [
            "drop external source if exists fq01_src028",
            f"create external source fq01_src028 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "select source_name, host, `type` from information_schema.ins_ext_sources where source_name='fq01_src028'",
            "alter external source fq01_src028 set host='10.0.0.2'",
            "select source_name, host, `type` from information_schema.ins_ext_sources where source_name='fq01_src028'",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src028'",
        ],
    ],

    ## 029
    ["029", [_mysql], ["fq01_src029m"], "querying a non-existent source raises EXT_SOURCE_NOT_FOUND", [
            f"select * from fq01_src029m_ghost.{_M_DB}.{_FW_TABLE}",
            f"select * from fq01_src029m_ghost.{_M_DB}.{_FW_TABLE}",
        ],
    ],

    ## 029
    ["029", [_pg], ["fq01_src029p"], "querying a non-existent source raises EXT_SOURCE_NOT_FOUND", [
            f"select * from fq01_src029p_ghost.{_P_SCHEMA}.{_FW_TABLE}",
            f"select * from fq01_src029p_ghost.{_P_SCHEMA}.{_FW_TABLE}",
        ],
    ],

    ## 029
    ["029", [_influxdb], ["fq01_src029i"], "querying a non-existent source raises EXT_SOURCE_NOT_FOUND", [
            f"select * from fq01_src029i_ghost.{_FW_TABLE}",
            f"select * from fq01_src029i_ghost.{_FW_TABLE}",
        ],
    ],

    ## 030
    ["030", [_mysql], ["fq01_src030m"], "create with unreachable host, then recover via ALTER + REFRESH", [
            "drop external source if exists fq01_src030m",
            f"create external source fq01_src030m type='mysql' host='192.0.2.200' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='500')",
            _q_count_step(negative=True),
            f"alter external source fq01_src030m set host='{_M_HOST}', port={_M_PORT}",
            "describe external source fq01_src030m",
            "refresh external source fq01_src030m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 030
    ["030", [_pg], ["fq01_src030p"], "create with unreachable host, then recover via ALTER + REFRESH", [
            "drop external source if exists fq01_src030p",
            f"create external source fq01_src030p type='postgresql' host='192.0.2.200' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='500')",
            _q_count_step(negative=True),
            f"alter external source fq01_src030p set host='{_P_HOST}', port={_P_PORT}",
            "describe external source fq01_src030p",
            "refresh external source fq01_src030p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 030
    ["030", [_influxdb], ["fq01_src030i"], "create with unreachable host, then recover via ALTER + REFRESH", [
            "drop external source if exists fq01_src030i",
            f"create external source fq01_src030i type='influxdb' host='192.0.2.200' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'connect_timeout_ms'='500')",
            _q_count_step(negative=True),
            f"alter external source fq01_src030i set host='{_I_HOST}', port={_I_PORT}",
            "describe external source fq01_src030i",
            "refresh external source fq01_src030i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 031
    ["031", [_mysql], ["fq01_src031"], "query against an unreachable source fails with connect_timeout_ms configured", [
            "drop external source if exists fq01_src031",
            f"create external source fq01_src031 type='mysql' host='192.0.2.200' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='1000')",
            f"select * from fq01_src031.{_M_DB}.ghost_table",
            _clear_source_step(),
        ],
    ],

    ## 032
    ["032", [_mysql], ["fq01_src032"], "doc-style CREATE IF NOT EXISTS (as shown in user docs)", [
            "drop external source if exists fq01_src032",
            f"create external source if not exists fq01_src032 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            f"create external source if not exists fq01_src032 type='mysql' host='{_M_HOST}' port=13307 user='alt_user' password='alt_pass' database='alt_db'",
            _q_count_step(),
            "describe external source fq01_src032",
            _clear_source_step(),
        ],
    ],

    ## 033
    ["033", [_mysql], ["fq01_src033"], "SHOW EXTERNAL SOURCES includes non-null create_time", [
            "drop external source if exists fq01_src033",
            f"create external source fq01_src033 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            # auto-SHOW after CREATE already records ctime; explicit query for clarity
            "select source_name, `type`, create_time from information_schema.ins_ext_sources where source_name='fq01_src033'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 034m
    ["034m", [_mysql], ["fq01_src034m"], "all-types positive summary: CREATE / type opts / count / ALTER opts / count / DROP", [
            "drop external source if exists fq01_src034m",
            f"create external source fq01_src034m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('charset'='utf8mb4', 'ssl_mode'='preferred')",
            _q_count_step(),
            "select count(*) from information_schema.ins_ext_sources",
            "describe external source fq01_src034m",
            "alter external source fq01_src034m set options('ssl_mode'='required')",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src034m'",
        ],
    ],

    ## 034p
    ["034p", [_pg], ["fq01_src034p"], "all-types positive summary: CREATE / type opts / count / ALTER opts / count / DROP", [
            "drop external source if exists fq01_src034p",
            f"create external source fq01_src034p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('sslmode'='prefer')",
            _q_count_step(),
            "select count(*) from information_schema.ins_ext_sources",
            "describe external source fq01_src034p",
            "alter external source fq01_src034p set options('sslmode'='require')",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src034p'",
        ],
    ],

    ## 034i
    ["034i", [_influxdb], ["fq01_src034i"], "all-types positive summary: CREATE / type opts / count / ALTER opts / count / DROP", [
            "drop external source if exists fq01_src034i",
            f"create external source fq01_src034i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "select count(*) from information_schema.ins_ext_sources",
            "describe external source fq01_src034i",
            f"alter external source fq01_src034i set options('api_token'='{_I_TOKEN}', 'protocol'='http')",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src034i'",
        ],
    ],

    ## 035
    ["035", [_mysql], ["fq01_src035"], "non-admin user sees SHOW + DESCRIBE with masked password", [
            "drop external source if exists fq01_src035",
            f"create external source fq01_src035 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "drop user if exists fq01fw035usr",
            "create user fq01fw035usr pass 'FqFrame!2026'",
            _connect("fq01fw035usr", "FqFrame!2026"),
            "show external sources",
            "describe external source fq01_src035",
            _connect_root(),
            "drop user if exists fq01fw035usr",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src035'",
        ],
    ],

    ## 038
    ["038", [_mysql], ["fq01_src038"], "valid boundary values: host 256, port 1, port 65535, user 128, pass 128, db 64", [
            "drop external source if exists fq01_src038",
            # host 256 chars
            f"create external source fq01_src038 type='mysql' host='{'h' * 256}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "describe external source fq01_src038",
            _clear_source_step(),
            # port 1
            f"create external source fq01_src038 type='mysql' host='{_M_HOST}' port=1 user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _clear_source_step(),
            # port 65535
            f"create external source fq01_src038 type='mysql' host='{_M_HOST}' port=65535 user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _clear_source_step(),
            # user 128 chars
            f"create external source fq01_src038 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{'u' * 128}' password='{_M_PASS}' database='{_M_DB}'",
            _clear_source_step(),
            # password 128 chars
            f"create external source fq01_src038 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{'p' * 128}' database='{_M_DB}'",
            _clear_source_step(),
            # database 64 chars
            f"create external source fq01_src038 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{'d' * 64}'",
            _clear_source_step(),
        ],
    ],

    ## 039
    ["039", [_mysql], ["fq01_src039"], "backtick-quoted and special-character source names", [
            "drop external source if exists _fq01fw039u",
            f"create external source _fq01fw039u type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop external source if exists _fq01fw039u",
            "drop external source if exists `fq01-fw-039-hyp`",
            f"create external source `fq01-fw-039-hyp` type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop external source if exists `fq01-fw-039-hyp`",
            "drop external source if exists `fq01 fw 039 sp`",
            f"create external source `fq01 fw 039 sp` type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop external source if exists `fq01 fw 039 sp`",
            "drop external source if exists `select`",
            f"create external source `select` type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop external source if exists `select`",
            f"create external source `` type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
        ],
    ],

    ## 040
    ["040", [_mysql], ["fq01_src040"], "source name / db name conflict is case-insensitive", [
            "drop external source if exists FQ01FW040DB",
            "drop database if exists FQ01FW040DB",
            # Uppercase DB then try same caseless source name → CONFLICT
            "create database FQ01FW040DB",
            f"create external source FQ01FW040DB type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop database FQ01FW040DB",
            # Lowercase source then uppercase same caseless name → ALREADY_EXISTS
            "drop external source if exists fq01fw040src",
            f"create external source fq01fw040src type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01fw040src'",
            f"create external source FQ01FW040SRC type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "drop external source if exists fq01fw040src",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01fw040src'",
        ],
    ],

    ## 041
    ["041", [_mysql], ["fq01_src041"], "ALTER USER-only and ALTER PASSWORD-only each update independently", [
            "drop external source if exists fq01_src041",
            f"create external source fq01_src041 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            # ALTER USER only — password unchanged
            "alter external source fq01_src041 set user='alt_user'",
            "describe external source fq01_src041",
            "show external sources",
            # ALTER PASSWORD only — user unchanged
            "alter external source fq01_src041 set password='AltPass!999'",
            "describe external source fq01_src041",
            "show external sources",
            _clear_source_step(),
        ],
    ],

    ## 042
    ["042", [_mysql], ["fq01_src042"], "ALTER OPTIONS patch-merge: adding a new key retains existing keys", [
            "drop external source if exists fq01_src042",
            f"create external source fq01_src042 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='1000')",
            "describe external source fq01_src042",
            # Add read_timeout_ms; connect_timeout_ms must still be present
            "alter external source fq01_src042 set options('read_timeout_ms'='3000')",
            "describe external source fq01_src042",
            # Multi-key merge; all three keys must survive
            "alter external source fq01_src042 set options('connect_timeout_ms'='500', 'charset'='utf8mb4')",
            "describe external source fq01_src042",
            _clear_source_step(),
        ],
    ],

    ## 043
    ["043", [_mysql], ["fq01_src043"], "ALTER OPTIONS value='' removes that key while preserving others", [
            "drop external source if exists fq01_src043",
            f"create external source fq01_src043 type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='1000', 'read_timeout_ms'='2000')",
            "describe external source fq01_src043",
            # Empty value removes read_timeout_ms; connect_timeout_ms must survive
            "alter external source fq01_src043 set options('read_timeout_ms'='')",
            "describe external source fq01_src043",
            _clear_source_step(),
        ],
    ],

    ## 044p
    ["044p", [_pg], ["fq01_src044p"], "PG TLS: tls_enabled+sslmode=disable conflicts; 6 valid sslmode combos accepted", [
            "drop external source if exists fq01_src044p",
            # tls=true + sslmode=disable → TLS conflict error
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='disable')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src044p'",
            # valid: tls=false, sslmode=disable
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='false', 'sslmode'='disable')",
            _clear_source_step(),
            # valid: tls=true, sslmode=allow
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='allow')",
            _clear_source_step(),
            # valid: tls=true, sslmode=prefer
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='prefer')",
            _clear_source_step(),
            # valid: tls=true, sslmode=require
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='require')",
            _clear_source_step(),
            # valid: tls=true, sslmode=verify-ca
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='verify-ca')",
            _clear_source_step(),
            # valid: tls=true, sslmode=verify-full
            f"create external source fq01_src044p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' options('tls_enabled'='true', 'sslmode'='verify-full')",
            _clear_source_step(),
        ],
    ],

    ## 045m
    ["045m", [_mysql], ["fq01_src045m"], "ALTER DATABASE='' clears database field; subsequent set restores it", [
            "drop external source if exists fq01_src045m",
            f"create external source fq01_src045m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='mydb'",
            "describe external source fq01_src045m",
            # clear DATABASE
            "alter external source fq01_src045m set database=''",
            "describe external source fq01_src045m",
            # restore to a valid value
            "alter external source fq01_src045m set database='restored_db'",
            "describe external source fq01_src045m",
            _clear_source_step(),
        ],
    ],

    ## 045p
    ["045p", [_pg], ["fq01_src045p"], "ALTER SCHEMA='' clears schema; ALTER DATABASE+SCHEMA='' clears both", [
            "drop external source if exists fq01_src045p",
            f"create external source fq01_src045p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='pgdb' schema='public'",
            "describe external source fq01_src045p",
            # clear SCHEMA only
            "alter external source fq01_src045p set schema=''",
            "describe external source fq01_src045p",
            # clear both DATABASE + SCHEMA
            "alter external source fq01_src045p set database='', schema=''",
            "describe external source fq01_src045p",
            _clear_source_step(),
        ],
    ],

    ## 046m
    ["046m", [_mysql], ["fq01_src046m", "fq01_src046m_bad"], "MySQL ssl_mode enum transitions are persisted; invalid enum is rejected with no metadata residue", [
            "drop external source if exists fq01_src046m",
            "drop external source if exists fq01_src046m_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src046m_bad'",
            f"create external source fq01_src046m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='disabled')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "describe external source fq01_src046m",
            "alter external source fq01_src046m set options('ssl_mode'='verify_ca')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "describe external source fq01_src046m",
            "alter external source fq01_src046m set options('ssl_mode'='verify_identity')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "describe external source fq01_src046m",
            "alter external source fq01_src046m set options('ssl_mode'='preferred')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "describe external source fq01_src046m",
            _q_count_step(),
            f"create external source fq01_src046m_bad type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='not_a_mode')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src046m_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src046m'",
            "select source_name, options from information_schema.ins_ext_sources where source_name in ('fq01_src046m', 'fq01_src046m_bad') order by source_name",
            "describe external source fq01_src046m_bad",
            _clear_source_step(),
        ],
    ],

    ## 047m
    ["047m", [_mysql], ["fq01_src047m", "fq01_src047m_bad"], "timeout option boundary and invalid values are enforced; failed ALTER leaves source usable", [
            "drop external source if exists fq01_src047m",
            "drop external source if exists fq01_src047m_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m_bad'",
            f"create external source fq01_src047m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='0', 'read_timeout_ms'='0')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "describe external source fq01_src047m",
            _q_count_step(),
            "alter external source fq01_src047m set options('connect_timeout_ms'='100', 'read_timeout_ms'='600000')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "describe external source fq01_src047m",
            _q_count_step(),
            "alter external source fq01_src047m set options('connect_timeout_ms'='99')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "describe external source fq01_src047m",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            _q_count_step(),
            "alter external source fq01_src047m set options('read_timeout_ms'='600001')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "describe external source fq01_src047m",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            _q_count_step(),
            f"create external source fq01_src047m_bad type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='600001')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047m'",
            "select source_name, options from information_schema.ins_ext_sources where source_name in ('fq01_src047m', 'fq01_src047m_bad') order by source_name",
            "describe external source fq01_src047m_bad",
            _clear_source_step(),
        ],
    ],

    ## 048m
    ["048m", [_mysql], ["fq01_src048m", "fq01_src048m_bad"], "OPTIONS total length budget: valid combined payload accepted, oversized payload rejected", [
            "drop external source if exists fq01_src048m",
            "drop external source if exists fq01_src048m_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048m'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048m_bad'",
            f"create external source fq01_src048m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='false', 'tls_client_cert'='{'a' * 1500}', 'tls_client_key'='{'b' * 1500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048m'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048m'",
            "describe external source fq01_src048m",
            _q_count_step(),
            f"create external source fq01_src048m_bad type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='false', 'tls_client_cert'='{'a' * 2500}', 'tls_client_key'='{'b' * 2500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048m_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048m'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048m'",
            "describe external source fq01_src048m_bad",
            _clear_source_step(),
        ],
    ],

    ## 049m
    ["049m", [_mysql], ["fq01_src049m"], "ALTER DATABASE changes 2-segment path resolution; REFRESH after restore makes default path usable again", [
            "drop external source if exists fq01_src049m",
            f"create external source fq01_src049m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from fq01_src049m.src_t",
            "alter external source fq01_src049m set database='fw_alt_mdb'",
            "describe external source fq01_src049m",
            "select count(*) from fq01_src049m.src_t",
            f"alter external source fq01_src049m set database='{_M_DB}'",
            "refresh external source fq01_src049m",
            "describe external source fq01_src049m",
            "select count(*) from fq01_src049m.src_t",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 050m
    ["050m", [_mysql], ["fq01_src050m"], "REFRESH reflects external MySQL schema change (add/drop column) with observable query behavior", [
            "drop external source if exists fq01_src050m",
            f"create external source fq01_src050m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from fq01_src050m.src_t",
            "select extra_note from fq01_src050m.src_t where val=5",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` add column extra_note VARCHAR(32) NULL",
                f"update `{_FW_TABLE}` set extra_note='after_refresh' where val=5",
            ]),
            "select extra_note from fq01_src050m.src_t where val=5",
            "refresh external source fq01_src050m",
            "select extra_note from fq01_src050m.src_t where val=5",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` drop column extra_note",
            ]),
            "refresh external source fq01_src050m",
            "select extra_note from fq01_src050m.src_t where val=5",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 051m
    ["051m", [_mysql], ["fq01_src051m"], "charset with multilingual data and ssl_mode combination remains queryable after ALTER", [
            "drop external source if exists fq01_src051m",
            f"create external source fq01_src051m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('charset'='utf8mb4', 'ssl_mode'='preferred')",
            _mysql_exec_step(_M_DB, [
                f"replace into `{_FW_TABLE}` values ('2024-01-01 00:10:00.000', 66, 6.6, '中文字符', 1)",
            ]),
            "select count(*) from fq01_src051m.src_t where name='中文字符'",
            "alter external source fq01_src051m set options('charset'='utf8mb4', 'ssl_mode'='required')",
            "describe external source fq01_src051m",
            "select count(*) from fq01_src051m.src_t where name='中文字符'",
            _mysql_exec_step(_M_DB, [
                f"delete from `{_FW_TABLE}` where val=66",
            ]),
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 052m
    ["052m", [_mysql], ["fq01_src052m"], "single ALTER combining base fields and OPTIONS supports add/remove patch semantics", [
            "drop external source if exists fq01_src052m",
            f"create external source fq01_src052m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('read_timeout_ms'='2000', 'ssl_mode'='preferred')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052m'",
            "select source_name, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src052m'",
            _q_count_step(),
            f"alter external source fq01_src052m set host='{_M_HOST}', port={_M_PORT}, user='{_M_USER}', password='{_M_PASS}', database='{_M_DB}', options('read_timeout_ms'='', 'connect_timeout_ms'='350', 'ssl_mode'='required')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052m'",
            "select source_name, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src052m'",
            "describe external source fq01_src052m",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052m'",
        ],
    ],

    ## 053p
    ["053p", [_pg], ["fq01_src053p", "fq01_src053p_bad"], "PostgreSQL sslmode invalid values are rejected on CREATE/ALTER with no metadata residue", [
            "drop external source if exists fq01_src053p",
            "drop external source if exists fq01_src053p_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p_bad'",
            f"create external source fq01_src053p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('sslmode'='prefer')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            _q_count_step(),
            "alter external source fq01_src053p set options('sslmode'='not_a_mode')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            "describe external source fq01_src053p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            _q_count_step(),
            f"create external source fq01_src053p_bad type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('sslmode'='not_a_mode')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src053p'",
            _clear_source_step(),
        ],
    ],

    ## 054i
    ["054i", [_influxdb], ["fq01_src054i", "fq01_src054i_bad"], "InfluxDB protocol invalid values are rejected on CREATE/ALTER with no metadata residue", [
            "drop external source if exists fq01_src054i",
            "drop external source if exists fq01_src054i_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i_bad'",
            f"create external source fq01_src054i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            _q_count_step(),
            "alter external source fq01_src054i set options('protocol'='http')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            "describe external source fq01_src054i",
            _q_count_step(),
            "alter external source fq01_src054i set options('protocol'='not_a_protocol')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            "describe external source fq01_src054i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            _q_count_step(),
            f"create external source fq01_src054i_bad type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='not_a_protocol')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src054i'",
            _clear_source_step(),
        ],
    ],

    # ══════════════════════════════════════════════════════════════════════
    # InfluxDB gap-fill cases — cover MySQL/PG dimensions for InfluxDB
    # ══════════════════════════════════════════════════════════════════════

    ## 002i
    ["002i", [_influxdb], ["fq01_src002i"], "CREATE IF NOT EXISTS is idempotent; second create with different params is ignored (InfluxDB)", [
            "drop external source if exists fq01_src002i",
            f"create external source fq01_src002i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            f"create external source if not exists fq01_src002i type='influxdb' host='{_I_HOST}' port=18087 user='alt_user' database='alt_db' options('api_token'='alt_token', 'protocol'='http')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 003i
    ["003i", [_influxdb], ["fq01_src003i"], "duplicate CREATE raises ALREADY_EXISTS (InfluxDB)", [
            "drop external source if exists fq01_src003i",
            f"create external source fq01_src003i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            f"create external source fq01_src003i type='influxdb' host='{_I_HOST}' port=18087 user='alt_user' database='alt_db' options('api_token'='alt_token', 'protocol'='http')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 004i
    ["004i", [_influxdb], ["fq01_src004i"], "ALTER options (protocol) then re-query (InfluxDB)", [
            "drop external source if exists fq01_src004i",
            f"create external source fq01_src004i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            f"alter external source fq01_src004i set options('api_token'='{_I_TOKEN}', 'protocol'='http')",
            "describe external source fq01_src004i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 005i
    ["005i", [_influxdb], ["fq01_src005i"], "REFRESH is idempotent (InfluxDB)", [
            "drop external source if exists fq01_src005i",
            f"create external source fq01_src005i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "refresh external source fq01_src005i",
            _q_count_step(),
            "refresh external source fq01_src005i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 006i
    ["006i", [_influxdb], ["fq01_src006i"], "DROP IF EXISTS is idempotent (InfluxDB)", [
            "drop external source if exists fq01_src006i",
            f"create external source fq01_src006i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "drop external source if exists fq01_src006i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006i'",
            "drop external source if exists fq01_src006i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006i'",
        ],
    ],

    ## 013i
    ["013i", [_influxdb], ["fq01_src013i"], "missing mandatory fields each produce a syntax error (InfluxDB: DATABASE optional per FS §3.4.1.2)", [
            "drop external source if exists fq01_src013i",
            # missing type=
            f"create external source fq01_src013i host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # missing host=
            f"create external source fq01_src013i type='influxdb' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # missing port=
            f"create external source fq01_src013i type='influxdb' host='{_I_HOST}' user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # missing user=
            f"create external source fq01_src013i type='influxdb' host='{_I_HOST}' port={_I_PORT} database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # bare name only
            "create external source fq01_src013i",
            # valid create at end confirms parser is not stuck
            f"create external source fq01_src013i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 018i
    ["018i", [_influxdb], ["fq01_src018i"], "ALTER with multiple fields combined (InfluxDB: user + database)", [
            "drop external source if exists fq01_src018i",
            f"create external source fq01_src018i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            f"alter external source fq01_src018i set host='{_I_HOST}', port={_I_PORT}, user='admin', database='{_I_DB}'",
            "describe external source fq01_src018i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 019i
    ["019i", [_influxdb], ["fq01_src019i"], "ALTER database then revert (InfluxDB; no schema clause)", [
            "drop external source if exists fq01_src019i",
            f"create external source fq01_src019i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "alter external source fq01_src019i set database='alt_idb'",
            "describe external source fq01_src019i",
            f"alter external source fq01_src019i set database='{_I_DB}'",
            "describe external source fq01_src019i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 020i
    ["020i", [_influxdb], ["fq01_src020i"], "repeated drop / re-create semantics (InfluxDB)", [
            "drop external source if exists fq01_src020i",
            f"create external source fq01_src020i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "drop external source fq01_src020i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020i'",
            f"create external source fq01_src020i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "drop external source fq01_src020i",
            f"create external source fq01_src020i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 025i
    ["025i", [_influxdb], ["fq01_src025i"], "host / user / password / database too long each raise error (InfluxDB; no schema field)", [
            "drop external source if exists fq01_src025i",
            # host too long (>256)
            f"create external source fq01_src025i type='influxdb' host='{'h' * 257}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # user too long (>128)
            f"create external source fq01_src025i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='{'u' * 129}' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # password too long (>128)
            f"create external source fq01_src025i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' password='{'p' * 129}' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            # database too long (>64)
            f"create external source fq01_src025i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{'d' * 65}' options('api_token'='{_I_TOKEN}')",
            # all within limits → valid
            f"create external source fq01_src025i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 028i
    ["028i", [_influxdb], ["fq01_src028i"], "ins_ext_sources reflects CREATE / ALTER / DROP lifecycle (InfluxDB)", [
            "drop external source if exists fq01_src028i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src028i'",
            f"create external source fq01_src028i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "select source_name, `type`, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src028i'",
            _q_count_step(),
            f"alter external source fq01_src028i set options('api_token'='{_I_TOKEN}', 'protocol'='http')",
            "select source_name, `type`, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src028i'",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src028i'",
        ],
    ],

    ## 031i
    ["031i", [_influxdb], ["fq01_src031i"], "query against an unreachable InfluxDB source fails with connect_timeout_ms configured (gRPC)", [
            "drop external source if exists fq01_src031i",
            f"create external source fq01_src031i type='influxdb' host='192.0.2.200' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'connect_timeout_ms'='1000')",
            f"select * from fq01_src031i.{_I_DB}.ghost_table",
            _clear_source_step(),
        ],
    ],

    ## 032i
    ["032i", [_influxdb], ["fq01_src032i"], "doc-style CREATE IF NOT EXISTS (InfluxDB with api_token)", [
            "drop external source if exists fq01_src032i",
            f"create external source if not exists fq01_src032i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            f"create external source if not exists fq01_src032i type='influxdb' host='{_I_HOST}' port=18087 user='alt_user' database='alt_db' options('api_token'='alt_token', 'protocol'='http')",
            _q_count_step(),
            "describe external source fq01_src032i",
            _clear_source_step(),
        ],
    ],

    ## 033i
    ["033i", [_influxdb], ["fq01_src033i"], "SHOW EXTERNAL SOURCES includes non-null create_time (InfluxDB)", [
            "drop external source if exists fq01_src033i",
            f"create external source fq01_src033i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "select source_name, `type`, create_time from information_schema.ins_ext_sources where source_name='fq01_src033i'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 035i
    ["035i", [_influxdb], ["fq01_src035i"], "non-admin user sees SHOW + DESCRIBE with masked api_token (InfluxDB)", [
            "drop external source if exists fq01_src035i",
            f"create external source fq01_src035i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "drop user if exists fq01fw035iusr",
            "create user fq01fw035iusr pass 'FqFrame!2026'",
            _connect("fq01fw035iusr", "FqFrame!2026"),
            "show external sources",
            "describe external source fq01_src035i",
            _connect_root(),
            "drop user if exists fq01fw035iusr",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src035i'",
        ],
    ],

    ## 038i
    ["038i", [_influxdb], ["fq01_src038i"], "valid boundary values: host 256, port 1, port 65535, user 128, db 64 (InfluxDB; no schema)", [
            "drop external source if exists fq01_src038i",
            # host=256 chars, port=1
            f"create external source fq01_src038i type='influxdb' host='{'h' * 256}' port=1 user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            "describe external source fq01_src038i",
            "drop external source if exists fq01_src038i",
            # port=65535, user=128 chars
            f"create external source fq01_src038i type='influxdb' host='{_I_HOST}' port=65535 user='{'u' * 128}' database='{_I_DB}' options('api_token'='{_I_TOKEN}')",
            "describe external source fq01_src038i",
            "drop external source if exists fq01_src038i",
            # database=64 chars
            f"create external source fq01_src038i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{'d' * 64}' options('api_token'='{_I_TOKEN}')",
            "describe external source fq01_src038i",
            "drop external source if exists fq01_src038i",
        ],
    ],

    ## 041i
    ["041i", [_influxdb], ["fq01_src041i"], "ALTER USER-only and ALTER api_token-only each update independently (InfluxDB)", [
            "drop external source if exists fq01_src041i",
            f"create external source fq01_src041i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            # ALTER USER only — api_token unchanged
            "alter external source fq01_src041i set user='alt_user'",
            "describe external source fq01_src041i",
            "show external sources",
            # ALTER api_token only — user unchanged
            f"alter external source fq01_src041i set options('api_token'='{_I_TOKEN}')",
            "describe external source fq01_src041i",
            "show external sources",
            # restore user for connectivity
            "alter external source fq01_src041i set user='admin'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 042i
    ["042i", [_influxdb], ["fq01_src042i"], "ALTER OPTIONS patch-merge: adding a new key retains existing keys (InfluxDB: protocol + api_token)", [
            "drop external source if exists fq01_src042i",
            f"create external source fq01_src042i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "describe external source fq01_src042i",
            # Add connect_timeout_ms; api_token + protocol must still be present
            "alter external source fq01_src042i set options('connect_timeout_ms'='1000')",
            "describe external source fq01_src042i",
            # Multi-key merge; all keys must survive
            f"alter external source fq01_src042i set options('connect_timeout_ms'='500', 'read_timeout_ms'='3000')",
            "describe external source fq01_src042i",
            _clear_source_step(),
        ],
    ],

    ## 043i
    ["043i", [_influxdb], ["fq01_src043i"], "ALTER OPTIONS value='' removes that key while preserving others (InfluxDB)", [
            "drop external source if exists fq01_src043i",
            f"create external source fq01_src043i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'connect_timeout_ms'='1000', 'read_timeout_ms'='2000')",
            "describe external source fq01_src043i",
            # Empty value removes read_timeout_ms; other keys must survive
            "alter external source fq01_src043i set options('read_timeout_ms'='')",
            "describe external source fq01_src043i",
            _clear_source_step(),
        ],
    ],

    ## 045i
    ["045i", [_influxdb], ["fq01_src045i"], "ALTER DATABASE='' clears database field; subsequent set restores it (InfluxDB)", [
            "drop external source if exists fq01_src045i",
            f"create external source fq01_src045i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='mydb' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "describe external source fq01_src045i",
            # clear DATABASE
            "alter external source fq01_src045i set database=''",
            "describe external source fq01_src045i",
            # restore to a valid value
            "alter external source fq01_src045i set database='restored_db'",
            "describe external source fq01_src045i",
            _clear_source_step(),
        ],
    ],

    ## 047i
    ["047i", [_influxdb], ["fq01_src047i", "fq01_src047i_bad"], "timeout option boundary and invalid values are enforced (InfluxDB gRPC); failed ALTER leaves source usable", [
            "drop external source if exists fq01_src047i",
            "drop external source if exists fq01_src047i_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i_bad'",
            f"create external source fq01_src047i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'connect_timeout_ms'='0', 'read_timeout_ms'='0')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "describe external source fq01_src047i",
            _q_count_step(),
            "alter external source fq01_src047i set options('connect_timeout_ms'='100', 'read_timeout_ms'='600000')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "describe external source fq01_src047i",
            _q_count_step(),
            # invalid: connect_timeout_ms < 100
            "alter external source fq01_src047i set options('connect_timeout_ms'='99')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "describe external source fq01_src047i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            _q_count_step(),
            # invalid: read_timeout_ms > 600000
            "alter external source fq01_src047i set options('read_timeout_ms'='600001')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "describe external source fq01_src047i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            _q_count_step(),
            # invalid on CREATE
            f"create external source fq01_src047i_bad type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'connect_timeout_ms'='600001')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047i'",
            "select source_name, options from information_schema.ins_ext_sources where source_name in ('fq01_src047i', 'fq01_src047i_bad') order by source_name",
            "describe external source fq01_src047i_bad",
            _clear_source_step(),
        ],
    ],

    ## 048i
    ["048i", [_influxdb], ["fq01_src048i", "fq01_src048i_bad"], "OPTIONS total length budget: valid combined payload accepted, oversized payload rejected (InfluxDB)", [
            "drop external source if exists fq01_src048i",
            "drop external source if exists fq01_src048i_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048i'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048i_bad'",
            f"create external source fq01_src048i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'tls_enabled'='false', 'tls_client_cert'='{'a' * 1500}', 'tls_client_key'='{'b' * 1500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048i'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048i'",
            "describe external source fq01_src048i",
            _q_count_step(),
            f"create external source fq01_src048i_bad type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'tls_enabled'='false', 'tls_client_cert'='{'a' * 2500}', 'tls_client_key'='{'b' * 2500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048i_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048i'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048i'",
            "describe external source fq01_src048i_bad",
            _clear_source_step(),
        ],
    ],

    ## 049i
    ["049i", [_influxdb], ["fq01_src049i"], "ALTER DATABASE changes 2-segment path resolution; REFRESH after restore makes default path usable again (InfluxDB)", [
            "drop external source if exists fq01_src049i",
            f"create external source fq01_src049i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            f"select count(*) from fq01_src049i.{_FW_TABLE}",
            "alter external source fq01_src049i set database='fw_alt_idb'",
            "describe external source fq01_src049i",
            f"select count(*) from fq01_src049i.{_FW_TABLE}",
            f"alter external source fq01_src049i set database='{_I_DB}'",
            "refresh external source fq01_src049i",
            "describe external source fq01_src049i",
            f"select count(*) from fq01_src049i.{_FW_TABLE}",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 052i
    ["052i", [_influxdb], ["fq01_src052i"], "single ALTER combining base fields and OPTIONS supports add/remove patch semantics (InfluxDB)", [
            "drop external source if exists fq01_src052i",
            f"create external source fq01_src052i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'read_timeout_ms'='2000')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052i'",
            "select source_name, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src052i'",
            _q_count_step(),
            f"alter external source fq01_src052i set host='{_I_HOST}', port={_I_PORT}, user='admin', database='{_I_DB}', options('read_timeout_ms'='', 'connect_timeout_ms'='350', 'protocol'='http')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052i'",
            "select source_name, host, port, user, database, options from information_schema.ins_ext_sources where source_name='fq01_src052i'",
            "describe external source fq01_src052i",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052i'",
        ],
    ],

    ## 055i
    ["055i", [_influxdb], ["fq01_src055i"], "3-segment path source.database.table queries InfluxDB table with explicit database (FS §3.5)", [
            "drop external source if exists fq01_src055i",
            f"create external source fq01_src055i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            # 2-segment path: source.table (uses default database)
            f"select count(*) from fq01_src055i.{_FW_TABLE}",
            # 3-segment path: source.database.table (explicit database per FS §3.5.4)
            f"select count(*) from fq01_src055i.{_I_DB}.{_FW_TABLE}",
            # Both paths should return same result
            f"select time, val, score, name, flag from fq01_src055i.{_FW_TABLE} order by time limit 1",
            f"select time, val, score, name, flag from fq01_src055i.{_I_DB}.{_FW_TABLE} order by time limit 1",
            # ALTER database to empty, then 3-segment path should still work with explicit database
            "alter external source fq01_src055i set database=''",
            "describe external source fq01_src055i",
            f"select count(*) from fq01_src055i.{_I_DB}.{_FW_TABLE}",
            # Restore default database
            f"alter external source fq01_src055i set database='{_I_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ══════════════════════════════════════════════════════════════════════
    # InfluxDB-specific cases — dimensions unique to InfluxDB per FS
    # ══════════════════════════════════════════════════════════════════════

    ## 057i — api_token is primary auth; USER/PASSWORD can be empty strings
    ["057i", [_influxdb], ["fq01_src057i"], "api_token is primary auth; USER='' PASSWORD='' with valid api_token still queries OK (FS §3.4.1.4)", [
            "drop external source if exists fq01_src057i",
            # Create with USER='' PASSWORD='' but valid api_token — DDL succeeds (no connectivity check)
            f"create external source fq01_src057i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='' password='' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "describe external source fq01_src057i",
            # Query should succeed — api_token is sufficient for InfluxDB v3
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 058i — arbitrary api_token value: DDL succeeds (no connectivity check), query with wrong token fails auth
    ["058i", [_influxdb], ["fq01_src058i"], "arbitrary api_token → DDL succeeds (no connectivity check, FS §3.4.1.3); query with wrong token fails auth (EXT_AUTH_FAILED); ALTER updates token; post-ALTER query succeeds", [
            "drop external source if exists fq01_src058i",
            # DDL accepts any token value — no connectivity check
            f"create external source fq01_src058i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='INVALID_TOKEN_12345', 'protocol'='flight_sql')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src058i'",
            # Auth is enforced: wrong token → EXT_AUTH_FAILED (query is expected to fail)
            _q_count_step(negative=True),
            # ALTER to change token value, verify query still works
            f"alter external source fq01_src058i set options('api_token'='{_I_TOKEN}')",
            "refresh external source fq01_src058i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 059i — omit api_token: DDL succeeds (no connectivity check), query with no token fails auth
    ["059i", [_influxdb], ["fq01_src059i"], "no api_token → DDL succeeds (no connectivity check, FS §3.4.1.3); query with no token fails auth (EXT_AUTH_FAILED); ALTER adds token; post-ALTER query succeeds", [
            "drop external source if exists fq01_src059i",
            # Create without api_token — DDL succeeds (no connectivity check per FS §3.4.1.3)
            f"create external source fq01_src059i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('protocol'='flight_sql')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src059i'",
            # Auth is enforced: no token → EXT_AUTH_FAILED (query is expected to fail)
            _q_count_step(negative=True),
            # Add api_token via ALTER, verify query still works
            f"alter external source fq01_src059i set options('api_token'='{_I_TOKEN}')",
            "refresh external source fq01_src059i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 060i — cross-type option rejection: non-Influx options on InfluxDB source
    ["060i", [_influxdb], ["fq01_src060i"], "Non-Influx options are rejected on InfluxDB source (FS §3.4.1.4)", [
            "drop external source if exists fq01_src060i",
            # MySQL charset on InfluxDB → error
            f"create external source fq01_src060i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'charset'='utf8mb4')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src060i'",
            # MySQL ssl_mode on InfluxDB → error
            f"create external source fq01_src060i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'ssl_mode'='preferred')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src060i'",
            # PG sslmode on InfluxDB → error
            f"create external source fq01_src060i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'sslmode'='require')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src060i'",
            # Generic unknown key on InfluxDB → error
            f"create external source fq01_src060i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'unknown_opt'='x')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src060i'",
            # Valid create to confirm parser is not stuck
            f"create external source fq01_src060i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 061i — protocol=http on initial CREATE, query succeeds
    ["061i", [_influxdb], ["fq01_src061i"], "CREATE with protocol=http works; query succeeds via HTTP API (FS §3.4.1.4)", [
            "drop external source if exists fq01_src061i",
            # Create with protocol=http directly
            f"create external source fq01_src061i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='http')",
            "describe external source fq01_src061i",
            _q_count_step(),
            # Switch to flight_sql, verify query still works
            "alter external source fq01_src061i set options('protocol'='flight_sql')",
            "describe external source fq01_src061i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ══════════════════════════════════════════════════════════════════════
    # PG gap-fill cases — added to cover MySQL-only dimensions for PostgreSQL
    # ══════════════════════════════════════════════════════════════════════

    ## 002p
    ["002p", [_pg], ["fq01_src002p"], "CREATE IF NOT EXISTS is idempotent; second create with different params is ignored", [
            "drop external source if exists fq01_src002p",
            f"create external source fq01_src002p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"create external source if not exists fq01_src002p type='postgresql' host='{_P_HOST}' port=15435 user='alt_user' password='alt_pass' database='alt_db' schema='alt_schema'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 003p
    ["003p", [_pg], ["fq01_src003p"], "duplicate CREATE raises ALREADY_EXISTS", [
            "drop external source if exists fq01_src003p",
            f"create external source fq01_src003p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"create external source fq01_src003p type='postgresql' host='{_P_HOST}' port=15435 user='alt_user' password='alt_pass' database='alt_db' schema='alt_schema'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 004p
    ["004p", [_pg], ["fq01_src004p"], "ALTER options then re-query (PG: sslmode)", [
            "drop external source if exists fq01_src004p",
            f"create external source fq01_src004p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "alter external source fq01_src004p set options('connect_timeout_ms'='1500')",
            "describe external source fq01_src004p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 005p
    ["005p", [_pg], ["fq01_src005p"], "REFRESH is idempotent (PG with schema)", [
            "drop external source if exists fq01_src005p",
            f"create external source fq01_src005p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "refresh external source fq01_src005p",
            _q_count_step(),
            "refresh external source fq01_src005p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 006p
    ["006p", [_pg], ["fq01_src006p"], "DROP IF EXISTS is idempotent (PG)", [
            "drop external source if exists fq01_src006p",
            f"create external source fq01_src006p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "drop external source if exists fq01_src006p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006p'",
            "drop external source if exists fq01_src006p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src006p'",
        ],
    ],

    ## 013p
    ["013p", [_pg], ["fq01_src013p"], "missing mandatory fields each produce a syntax error (PG: DATABASE is mandatory per FS §3.4.1.2)", [
            "drop external source if exists fq01_src013p",
            # missing type=
            f"create external source fq01_src013p host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # missing host=
            f"create external source fq01_src013p type='postgresql' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # missing port=
            f"create external source fq01_src013p type='postgresql' host='{_P_HOST}' user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # missing user=
            f"create external source fq01_src013p type='postgresql' host='{_P_HOST}' port={_P_PORT} password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # missing password=
            f"create external source fq01_src013p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # missing database= (PG mandatory per FS §3.4.1.2)
            f"create external source fq01_src013p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' schema='{_P_SCHEMA}'",
            # bare name only
            "create external source fq01_src013p",
            # valid create at end confirms parser is not stuck
            f"create external source fq01_src013p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 018p
    ["018p", [_pg], ["fq01_src018p"], "ALTER with multiple fields combined (PG includes schema)", [
            "drop external source if exists fq01_src018p",
            f"create external source fq01_src018p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            f"alter external source fq01_src018p set host='{_P_HOST}', port={_P_PORT}, user='{_P_USER}', password='{_P_PASS}', schema='{_P_SCHEMA}'",
            "describe external source fq01_src018p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 020p
    ["020p", [_pg], ["fq01_src020p"], "repeated drop / re-create semantics (PG)", [
            "drop external source if exists fq01_src020p",
            f"create external source fq01_src020p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "drop external source if exists fq01_src020p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020p'",
            "drop external source if exists fq01_src020p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020p'",
            f"create external source fq01_src020p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "drop external source fq01_src020p",
            "drop external source fq01_src020p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src020p'",
        ],
    ],

    ## 025p
    ["025p", [_pg], ["fq01_src025p"], "host / user / password / database / schema too long each raise error (PG adds schema≤64)", [
            "drop external source if exists fq01_src025p",
            f"create external source fq01_src025p type='postgresql' host='{'h' * 257}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"create external source fq01_src025p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{'u' * 129}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"create external source fq01_src025p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{'p' * 129}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"create external source fq01_src025p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{'d' * 65}' schema='{_P_SCHEMA}'",
            # PG-specific: schema too long (>64)
            f"create external source fq01_src025p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{'s' * 65}'",
            # valid create confirms parser recovery
            f"create external source fq01_src025p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 028p
    ["028p", [_pg], ["fq01_src028p"], "ins_ext_sources reflects CREATE / ALTER / DROP lifecycle (PG with schema column)", [
            "drop external source if exists fq01_src028p",
            f"create external source fq01_src028p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "select source_name, host, `type`, `schema` from information_schema.ins_ext_sources where source_name='fq01_src028p'",
            "alter external source fq01_src028p set host='10.0.0.2'",
            "select source_name, host, `type`, `schema` from information_schema.ins_ext_sources where source_name='fq01_src028p'",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src028p'",
        ],
    ],

    ## 031p
    ["031p", [_pg], ["fq01_src031p"], "query against an unreachable PG source fails with connect_timeout_ms configured", [
            "drop external source if exists fq01_src031p",
            f"create external source fq01_src031p type='postgresql' host='192.0.2.200' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='1000')",
            f"select * from fq01_src031p.{_P_SCHEMA}.ghost_table",
            _clear_source_step(),
        ],
    ],

    ## 032p
    ["032p", [_pg], ["fq01_src032p"], "doc-style CREATE IF NOT EXISTS (PG with schema)", [
            "drop external source if exists fq01_src032p",
            f"create external source if not exists fq01_src032p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            f"create external source if not exists fq01_src032p type='postgresql' host='{_P_HOST}' port=15435 user='alt_user' password='alt_pass' database='alt_db' schema='alt_schema'",
            _q_count_step(),
            "describe external source fq01_src032p",
            _clear_source_step(),
        ],
    ],

    ## 033p
    ["033p", [_pg], ["fq01_src033p"], "SHOW EXTERNAL SOURCES includes non-null create_time (PG)", [
            "drop external source if exists fq01_src033p",
            f"create external source fq01_src033p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "select source_name, `type`, create_time from information_schema.ins_ext_sources where source_name='fq01_src033p'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 035p
    ["035p", [_pg], ["fq01_src035p"], "non-admin user sees SHOW + DESCRIBE with masked password (PG)", [
            "drop external source if exists fq01_src035p",
            f"create external source fq01_src035p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "drop user if exists fq01fw035pusr",
            "create user fq01fw035pusr pass 'FqFrame!2026'",
            _connect("fq01fw035pusr", "FqFrame!2026"),
            "show external sources",
            "describe external source fq01_src035p",
            _connect_root(),
            "drop user if exists fq01fw035pusr",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src035p'",
        ],
    ],

    ## 038p
    ["038p", [_pg], ["fq01_src038p"], "valid boundary values: host 256, port 1, port 65535, user 128, pass 128, db 64, schema 64 (PG)", [
            "drop external source if exists fq01_src038p",
            # host 256 chars
            f"create external source fq01_src038p type='postgresql' host='{'h' * 256}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "describe external source fq01_src038p",
            _clear_source_step(),
            # port 1
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port=1 user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _clear_source_step(),
            # port 65535
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port=65535 user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _clear_source_step(),
            # user 128 chars
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{'u' * 128}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _clear_source_step(),
            # password 128 chars
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{'p' * 128}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _clear_source_step(),
            # database 64 chars
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{'d' * 64}' schema='{_P_SCHEMA}'",
            _clear_source_step(),
            # PG-specific: schema 64 chars
            f"create external source fq01_src038p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{'s' * 64}'",
            _clear_source_step(),
        ],
    ],

    ## 041p
    ["041p", [_pg], ["fq01_src041p"], "ALTER USER-only and ALTER PASSWORD-only each update independently (PG)", [
            "drop external source if exists fq01_src041p",
            f"create external source fq01_src041p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            # ALTER USER only — password unchanged
            "alter external source fq01_src041p set user='alt_user'",
            "describe external source fq01_src041p",
            "show external sources",
            # ALTER PASSWORD only — user unchanged
            "alter external source fq01_src041p set password='AltPass!999'",
            "describe external source fq01_src041p",
            "show external sources",
            _clear_source_step(),
        ],
    ],

    ## 042p
    ["042p", [_pg], ["fq01_src042p"], "ALTER OPTIONS patch-merge: adding a new key retains existing keys (PG: sslmode)", [
            "drop external source if exists fq01_src042p",
            f"create external source fq01_src042p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='1000')",
            "describe external source fq01_src042p",
            # Add read_timeout_ms; connect_timeout_ms must still be present
            "alter external source fq01_src042p set options('read_timeout_ms'='3000')",
            "describe external source fq01_src042p",
            # Multi-key merge with PG-specific options; all keys must survive
            "alter external source fq01_src042p set options('connect_timeout_ms'='500', 'sslmode'='prefer')",
            "describe external source fq01_src042p",
            _clear_source_step(),
        ],
    ],

    ## 043p
    ["043p", [_pg], ["fq01_src043p"], "ALTER OPTIONS value='' removes that key while preserving others (PG)", [
            "drop external source if exists fq01_src043p",
            f"create external source fq01_src043p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='1000', 'read_timeout_ms'='2000')",
            "describe external source fq01_src043p",
            # Empty value removes read_timeout_ms; connect_timeout_ms must survive
            "alter external source fq01_src043p set options('read_timeout_ms'='')",
            "describe external source fq01_src043p",
            _clear_source_step(),
        ],
    ],

    ## 047p
    ["047p", [_pg], ["fq01_src047p", "fq01_src047p_bad"], "timeout option boundary and invalid values are enforced (PG); failed ALTER leaves source usable", [
            "drop external source if exists fq01_src047p",
            "drop external source if exists fq01_src047p_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p_bad'",
            f"create external source fq01_src047p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='0', 'read_timeout_ms'='0')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "describe external source fq01_src047p",
            _q_count_step(),
            "alter external source fq01_src047p set options('connect_timeout_ms'='100', 'read_timeout_ms'='600000')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "describe external source fq01_src047p",
            _q_count_step(),
            # below min (99 < 100)
            "alter external source fq01_src047p set options('connect_timeout_ms'='99')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "describe external source fq01_src047p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            _q_count_step(),
            # above max (600001 > 600000)
            "alter external source fq01_src047p set options('read_timeout_ms'='600001')",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "describe external source fq01_src047p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            _q_count_step(),
            f"create external source fq01_src047p_bad type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='600001')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src047p'",
            "select source_name, options from information_schema.ins_ext_sources where source_name in ('fq01_src047p', 'fq01_src047p_bad') order by source_name",
            "describe external source fq01_src047p_bad",
            _clear_source_step(),
        ],
    ],

    ## 049p
    ["049p", [_pg], ["fq01_src049p"], "ALTER SCHEMA changes 2-segment path resolution (PG uses schema as middle segment per FS §3.5)", [
            "drop external source if exists fq01_src049p",
            f"create external source fq01_src049p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"select count(*) from fq01_src049p.{_FW_TABLE}",
            "alter external source fq01_src049p set schema='nonexistent_schema'",
            "describe external source fq01_src049p",
            f"select count(*) from fq01_src049p.{_FW_TABLE}",
            f"alter external source fq01_src049p set schema='{_P_SCHEMA}'",
            "refresh external source fq01_src049p",
            "describe external source fq01_src049p",
            f"select count(*) from fq01_src049p.{_FW_TABLE}",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 052p
    ["052p", [_pg], ["fq01_src052p"], "single ALTER combining base fields and OPTIONS supports add/remove patch semantics (PG with schema)", [
            "drop external source if exists fq01_src052p",
            f"create external source fq01_src052p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('read_timeout_ms'='2000', 'sslmode'='prefer')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052p'",
            "select source_name, host, port, user, database, `schema`, options from information_schema.ins_ext_sources where source_name='fq01_src052p'",
            _q_count_step(),
            f"alter external source fq01_src052p set host='{_P_HOST}', port={_P_PORT}, user='{_P_USER}', password='{_P_PASS}', database='{_P_DB}', schema='{_P_SCHEMA}', options('read_timeout_ms'='', 'connect_timeout_ms'='350', 'sslmode'='require')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052p'",
            "select source_name, host, port, user, database, `schema`, options from information_schema.ins_ext_sources where source_name='fq01_src052p'",
            "describe external source fq01_src052p",
            _q_count_step(),
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src052p'",
        ],
    ],

    # ══════════════════════════════════════════════════════════════════════
    # PG gap-fill cases v2 — remaining MySQL/PG-exclusive dimensions
    # ══════════════════════════════════════════════════════════════════════

    ## 048p
    ["048p", [_pg], ["fq01_src048p", "fq01_src048p_bad"], "OPTIONS total length budget: valid combined payload accepted, oversized payload rejected (PG with schema)", [
            "drop external source if exists fq01_src048p",
            "drop external source if exists fq01_src048p_bad",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048p'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048p_bad'",
            f"create external source fq01_src048p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('tls_enabled'='false', 'tls_client_cert'='{'a' * 1500}', 'tls_client_key'='{'b' * 1500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048p'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048p'",
            "describe external source fq01_src048p",
            _q_count_step(),
            f"create external source fq01_src048p_bad type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('tls_enabled'='false', 'tls_client_cert'='{'a' * 2500}', 'tls_client_key'='{'b' * 2500}')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048p_bad'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src048p'",
            "select source_name, length(options), options from information_schema.ins_ext_sources where source_name='fq01_src048p'",
            "describe external source fq01_src048p_bad",
            _clear_source_step(),
        ],
    ],

    ## 050p
    ["050p", [_pg], ["fq01_src050p"], "REFRESH reflects external PG schema change (add/drop column) with observable query behavior", [
            "drop external source if exists fq01_src050p",
            f"create external source fq01_src050p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"select count(*) from fq01_src050p.{_FW_TABLE}",
            f"select extra_note from fq01_src050p.{_FW_TABLE} where val=5",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} add column extra_note VARCHAR(32) NULL",
                f"update {_P_SCHEMA}.{_FW_TABLE} set extra_note='after_refresh' where val=5",
            ]),
            f"select extra_note from fq01_src050p.{_FW_TABLE} where val=5",
            "refresh external source fq01_src050p",
            f"select extra_note from fq01_src050p.{_FW_TABLE} where val=5",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} drop column extra_note",
            ]),
            "refresh external source fq01_src050p",
            f"select extra_note from fq01_src050p.{_FW_TABLE} where val=5",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 055p
    ["055p", [_pg], ["fq01_src055p"], "3-segment path source.schema.table queries PG table with explicit schema (FS §3.5)", [
            "drop external source if exists fq01_src055p",
            f"create external source fq01_src055p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            # 2-segment path: source.table (uses default schema)
            f"select count(*) from fq01_src055p.{_FW_TABLE}",
            # 3-segment path: source.schema.table (explicit schema per FS §3.5.1)
            f"select count(*) from fq01_src055p.{_P_SCHEMA}.{_FW_TABLE}",
            # Both paths should return same result
            f"select ts, val, score, name, flag from fq01_src055p.{_FW_TABLE} order by ts limit 1",
            f"select ts, val, score, name, flag from fq01_src055p.{_P_SCHEMA}.{_FW_TABLE} order by ts limit 1",
            # ALTER schema to empty, then 3-segment path should still work with explicit schema
            "alter external source fq01_src055p set schema=''",
            "describe external source fq01_src055p",
            f"select count(*) from fq01_src055p.{_P_SCHEMA}.{_FW_TABLE}",
            # Restore default schema
            f"alter external source fq01_src055p set schema='{_P_SCHEMA}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ## 056p
    ["056p", [_pg], ["fq01_src056p"], "ALTER tls_enabled=true + sslmode=disable conflict detected on ALTER (not just CREATE)", [
            "drop external source if exists fq01_src056p",
            # Create valid source with sslmode=prefer
            f"create external source fq01_src056p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('sslmode'='prefer')",
            _q_count_step(),
            "describe external source fq01_src056p",
            # ALTER to tls_enabled=true + sslmode=disable → TLS conflict error
            "alter external source fq01_src056p set options('tls_enabled'='true', 'sslmode'='disable')",
            # Source should remain usable with original options
            "describe external source fq01_src056p",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src056p'",
            _q_count_step(),
            # ALTER to valid combination should work
            "alter external source fq01_src056p set options('tls_enabled'='true', 'sslmode'='require')",
            "describe external source fq01_src056p",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src056p'",
            _clear_source_step(),
        ],
    ],

    # ══════════════════════════════════════════════════════════════════════
    # Gap-fill cases — missing coverage identified via FS/TS gap analysis
    # ══════════════════════════════════════════════════════════════════════

    # ── FQ-EXT-016: DROP while source is referenced by an active query ────
    # FS §3.4.5.2: "若存在活跃查询或被对象引用，删除有可能造成当前查询失败。"
    # We verify: (a) DROP succeeds even if the source was recently queried,
    # (b) a subsequent query against the dropped source fails.
    ["062m", [_mysql], ["fq01_src062m"], "DROP after recent query succeeds; subsequent query fails (FS §3.4.5.2)", [
            "drop external source if exists fq01_src062m",
            f"create external source fq01_src062m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            # DROP the source immediately after a successful query
            "drop external source fq01_src062m",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src062m'",
            # Query against dropped source must fail
            f"select count(*) from fq01_src062m.{_M_DB}.{_FW_TABLE}",
        ],
    ],

    ["062p", [_pg], ["fq01_src062p"], "DROP after recent query succeeds; subsequent query fails (FS §3.4.5.2)", [
            "drop external source if exists fq01_src062p",
            f"create external source fq01_src062p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _q_count_step(),
            "drop external source fq01_src062p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src062p'",
            f"select count(*) from fq01_src062p.{_P_SCHEMA}.{_FW_TABLE}",
        ],
    ],

    ["062i", [_influxdb], ["fq01_src062i"], "DROP after recent query succeeds; subsequent query fails (FS §3.4.5.2)", [
            "drop external source if exists fq01_src062i",
            f"create external source fq01_src062i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            _q_count_step(),
            "drop external source fq01_src062i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src062i'",
            f"select count(*) from fq01_src062i.{_I_DB}.{_FW_TABLE}",
        ],
    ],

    # ── FQ-EXT-016 variant: DROP + re-create cycle, then query works ──────
    ["063m", [_mysql], ["fq01_src063m"], "DROP then re-create with same name; query succeeds on new source (FS §3.4.5)", [
            "drop external source if exists fq01_src063m",
            f"create external source fq01_src063m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "drop external source fq01_src063m",
            f"select count(*) from fq01_src063m.{_M_DB}.{_FW_TABLE}",
            # Re-create with same name
            f"create external source fq01_src063m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-017: OPTIONS with unrecognized key → CREATE rejected ───────
    # FS §3.4.1.4: "未识别 key：DDL 阶段直接报 TSDB_CODE_PAR_SYNTAX_ERROR 并拒绝本次变更。"
    # This case keeps a three-type regression guard consistent with case 009/SEC-021.
    ["064m", [_mysql], ["fq01_src064m"], "OPTIONS with unrecognized key on CREATE is rejected (FS §3.4.1.4)", [
            "drop external source if exists fq01_src064m",
            # Unknown option key must be rejected.
            f"create external source fq01_src064m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('totally_unknown_key'='value123')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src064m'",
            # Rejection path keeps metadata clean.
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src064m'",
            _clear_source_step(),
        ],
    ],

    ["064p", [_pg], ["fq01_src064p"], "OPTIONS with unrecognized key on CREATE is rejected (FS §3.4.1.4)", [
            "drop external source if exists fq01_src064p",
            f"create external source fq01_src064p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('mystery_option'='42')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src064p'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src064p'",
            _clear_source_step(),
        ],
    ],

    ["064i", [_influxdb], ["fq01_src064i"], "OPTIONS with unrecognized key on CREATE is rejected (FS §3.4.1.4)", [
            "drop external source if exists fq01_src064i",
            f"create external source fq01_src064i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'bogus_key'='bogus_val')",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src064i'",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src064i'",
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-017 variant: ALTER with unrecognized key ───────────────────
    ["065m", [_mysql], ["fq01_src065m"], "ALTER OPTIONS with unrecognized key is rejected (FS §3.4.1.4)", [
            "drop external source if exists fq01_src065m",
            f"create external source fq01_src065m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            # Attempt ALTER with unknown option key
            "alter external source fq01_src065m set options('nonexistent_option'='val')",
            # Verify source is still intact and usable
            "describe external source fq01_src065m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-018: MySQL tls_enabled=true + ssl_mode=disabled conflict ───
    # FS §3.4.1.2: "当同时设置 tls_enabled = true 时，ssl_mode 不得为 disabled。"
    ["066m", [_mysql], ["fq01_src066m"], "CREATE with tls_enabled=true + ssl_mode=disabled → conflict error (FS §3.4.1.2)", [
            "drop external source if exists fq01_src066m",
            # This should fail: tls_enabled=true conflicts with ssl_mode=disabled
            f"create external source fq01_src066m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('tls_enabled'='true', 'ssl_mode'='disabled')",
            # Source should NOT exist after failed CREATE
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src066m'",
        ],
    ],

    ["067m", [_mysql], ["fq01_src067m"], "ALTER to tls_enabled=true + ssl_mode=disabled → conflict error; source remains usable (FS §3.4.1.2)", [
            "drop external source if exists fq01_src067m",
            f"create external source fq01_src067m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='preferred')",
            _q_count_step(),
            # ALTER to conflicting combination should fail
            "alter external source fq01_src067m set options('tls_enabled'='true', 'ssl_mode'='disabled')",
            # Source should remain usable with original options
            "describe external source fq01_src067m",
            "select source_name, options from information_schema.ins_ext_sources where source_name='fq01_src067m'",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-019: PG tls_enabled=true + sslmode=disable conflict (CREATE) ──
    # FS §3.4.1.3: "当同时设置 tls_enabled = true 时，sslmode 不得为 disable。"
    # 056p only tests ALTER path; this tests CREATE path.
    ["068p", [_pg], ["fq01_src068p"], "CREATE with tls_enabled=true + sslmode=disable → conflict error (FS §3.4.1.3)", [
            "drop external source if exists fq01_src068p",
            # This should fail: tls_enabled=true conflicts with sslmode=disable
            f"create external source fq01_src068p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('tls_enabled'='true', 'sslmode'='disable')",
            # Source should NOT exist after failed CREATE
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src068p'",
        ],
    ],

    # ── FQ-EXT-027: REFRESH on unreachable source ──────────────────────────
    # FS §3.4.6: REFRESH is a metadata cache invalidation; it does NOT
    # attempt to connect — always returns OK regardless of reachability.
    ["069m", [_mysql], ["fq01_src069m"], "REFRESH on unreachable MySQL source succeeds (metadata-only, FS §3.4.6)", [
            "drop external source if exists fq01_src069m",
            # Create source pointing to non-routable IP
            f"create external source fq01_src069m type='mysql' host='192.0.2.200' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('connect_timeout_ms'='1000')",
            # REFRESH succeeds — metadata-only, no connectivity check
            "refresh external source fq01_src069m",
            # Source object still exists after REFRESH
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src069m'",
            _clear_source_step(),
        ],
    ],

    ["069p", [_pg], ["fq01_src069p"], "REFRESH on unreachable PG source succeeds (metadata-only, FS §3.4.6)", [
            "drop external source if exists fq01_src069p",
            f"create external source fq01_src069p type='postgresql' host='192.0.2.200' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}' options('connect_timeout_ms'='1000')",
            "refresh external source fq01_src069p",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src069p'",
            _clear_source_step(),
        ],
    ],

    ["069i", [_influxdb], ["fq01_src069i"], "REFRESH on unreachable InfluxDB source succeeds (metadata-only, FS §3.4.6)", [
            "drop external source if exists fq01_src069i",
            f"create external source fq01_src069i type='influxdb' host='192.0.2.200' port={_I_PORT} user='admin' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql', 'connect_timeout_ms'='1000')",
            "refresh external source fq01_src069i",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src069i'",
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-027 variant: REFRESH on reachable → ALTER to unreachable → REFRESH still OK ──
    ["070m", [_mysql], ["fq01_src070m"], "REFRESH OK on reachable; ALTER to unreachable; REFRESH still OK; restore + verify (FS §3.4.6)", [
            "drop external source if exists fq01_src070m",
            f"create external source fq01_src070m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _q_count_step(),
            "refresh external source fq01_src070m",
            _q_count_step(),
            # Make source unreachable
            "alter external source fq01_src070m set host='192.0.2.200', options('connect_timeout_ms'='1000')",
            "refresh external source fq01_src070m",
            # Restore to working state
            f"alter external source fq01_src070m set host='{_M_HOST}'",
            "refresh external source fq01_src070m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-032: FS doc CREATE examples runnable (adapted to test env) ─
    # FS §3.4.1.5 examples: MySQL / PG / InfluxDB CREATE statements.
    # We adapt host/port/user/password to test env but keep the structure.
    ["071m", [_mysql], ["fq01_src071m"], "FS §3.4.1.5 MySQL CREATE example adapted to test env — structure matches doc", [
            "drop external source if exists fq01_src071m",
            # FS example structure: TYPE HOST PORT USER PASSWORD DATABASE
            f"create external source fq01_src071m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "show external sources",
            "describe external source fq01_src071m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ["071p", [_pg], ["fq01_src071p"], "FS §3.4.1.5 PG CREATE example adapted to test env — structure matches doc (with SCHEMA)", [
            "drop external source if exists fq01_src071p",
            # FS example structure: TYPE HOST PORT USER PASSWORD DATABASE SCHEMA
            f"create external source fq01_src071p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "show external sources",
            "describe external source fq01_src071p",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ["071i", [_influxdb], ["fq01_src071i"], "FS §3.4.1.5 InfluxDB CREATE IF NOT EXISTS example adapted to test env", [
            "drop external source if exists fq01_src071i",
            # FS example structure: CREATE IF NOT EXISTS ... TYPE HOST PORT USER PASSWORD DATABASE OPTIONS(api_token, protocol)
            f"create external source if not exists fq01_src071i type='influxdb' host='{_I_HOST}' port={_I_PORT} user='admin' password='' database='{_I_DB}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')",
            "show external sources",
            "describe external source fq01_src071i",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # ── FQ-EXT-018 variant: MySQL ssl_mode valid values accepted ──────────
    ["072m", [_mysql], ["fq01_src072m", "fq01_src072m_b"], "MySQL ssl_mode valid values each accepted on CREATE (FS §3.4.1.2)", [
            "drop external source if exists fq01_src072m",
            "drop external source if exists fq01_src072m_b",
            # ssl_mode=disabled (no conflict since tls_enabled defaults to false)
            f"create external source fq01_src072m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='disabled')",
            "describe external source fq01_src072m",
            _q_count_step(),
            "drop external source fq01_src072m",
            # ssl_mode=preferred (default value, should work)
            f"create external source fq01_src072m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='preferred')",
            "describe external source fq01_src072m",
            _q_count_step(),
            "drop external source fq01_src072m",
            # ssl_mode=required
            f"create external source fq01_src072m_b type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}' options('ssl_mode'='required')",
            "describe external source fq01_src072m_b",
            _clear_source_step(),
        ],
    ],

    # ── DDL lifecycle and catalog visibility ──────────────────────────────
    # Migrated from fq_08 (SYS-001/002/013/015/019/026/027/s06) to keep
    # all DDL lifecycle verification in fq_01's data-driven framework.

    # SYS-001: SHOW EXTERNAL SOURCES rewrites to ins_ext_sources
    ["073m", [_mysql], ["fq01_src073m"], "SHOW EXTERNAL SOURCES rewrites to ins_ext_sources — both expose same row for same source (DS §5.4)", [
            "drop external source if exists fq01_src073m",
            f"create external source fq01_src073m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "show external sources",
            "select * from information_schema.ins_ext_sources where source_name='fq01_src073m'",
            _clear_source_step(),
        ],
    ],

    # SYS-002: DESCRIBE EXTERNAL SOURCE rewrites to WHERE source_name= on ins_ext_sources
    ["074m", [_mysql], ["fq01_src074m"], "DESCRIBE EXTERNAL SOURCE rewrites to WHERE source_name= — DESCRIBE and ins_ext_sources return identical data (DS §5.4)", [
            "drop external source if exists fq01_src074m",
            f"create external source fq01_src074m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='testdb'",
            "describe external source fq01_src074m",
            "select * from information_schema.ins_ext_sources where source_name='fq01_src074m'",
            _clear_source_step(),
        ],
    ],

    # SYS-013: REFRESH clears cache; DESCRIBE before and after REFRESH is consistent
    ["075m", [_mysql], ["fq01_src075m"], "REFRESH rebuilds metadata cache — DESCRIBE before and after REFRESH returns consistent data (FS §3.4.6)", [
            "drop external source if exists fq01_src075m",
            f"create external source fq01_src075m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "describe external source fq01_src075m",
            "refresh external source fq01_src075m",
            "describe external source fq01_src075m",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    # SYS-015: REFRESH does not remove source; source still in ins_ext_sources and DESCRIBE still works
    ["076m", [_mysql], ["fq01_src076m"], "Source remains in ins_ext_sources after REFRESH; DESCRIBE still works — REFRESH does not remove source (FS §3.4.6)", [
            "drop external source if exists fq01_src076m",
            f"create external source fq01_src076m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src076m'",
            "refresh external source fq01_src076m",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src076m'",
            "describe external source fq01_src076m",
            _clear_source_step(),
        ],
    ],

    # SYS-019: DESCRIBE and ins_ext_sources all 10 columns projection consistent
    ["077m", [_mysql], ["fq01_src077m"], "DESCRIBE output and ins_ext_sources row cover all 10 DS §5.4 columns with consistent values (MySQL source)", [
            "drop external source if exists fq01_src077m",
            f"create external source fq01_src077m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='testdb'",
            "describe external source fq01_src077m",
            "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options`, create_time from information_schema.ins_ext_sources where source_name='fq01_src077m'",
            _clear_source_step(),
        ],
    ],

    ["077p", [_pg], ["fq01_src077p"], "DESCRIBE output and ins_ext_sources row cover all 10 DS §5.4 columns — PostgreSQL source includes schema field", [
            "drop external source if exists fq01_src077p",
            f"create external source fq01_src077p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            "describe external source fq01_src077p",
            "select source_name, `type`, `host`, `port`, `user`, `password`, `database`, `schema`, `options`, create_time from information_schema.ins_ext_sources where source_name='fq01_src077p'",
            _clear_source_step(),
        ],
    ],

    # SYS-026: DROP all N sources → ins_ext_sources shows 0 rows (models pre-downgrade clean state)
    ["078m", [_mysql], ["fq01_src078ma", "fq01_src078mb", "fq01_src078mc"], "DROP all N sources → ins_ext_sources shows 0 rows for those names (models zero-data pre-downgrade state) (DS §5.4)", [
            "drop external source if exists fq01_src078ma",
            "drop external source if exists fq01_src078mb",
            "drop external source if exists fq01_src078mc",
            f"create external source fq01_src078ma type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src078mb type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            f"create external source fq01_src078mc type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from information_schema.ins_ext_sources where source_name in ('fq01_src078ma','fq01_src078mb','fq01_src078mc')",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name in ('fq01_src078ma','fq01_src078mb','fq01_src078mc')",
        ],
    ],

    # SYS-027: ALTER persists across re-queries; DROP permanently removes from catalog
    ["079m", [_mysql], ["fq01_src079m"], "ALTER host change survives re-query; DROP permanently removes source from ins_ext_sources (FS §3.4.5)", [
            "drop external source if exists fq01_src079m",
            f"create external source fq01_src079m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src079m'",
            "alter external source fq01_src079m set host='altered.example.com'",
            "describe external source fq01_src079m",
            _clear_source_step(),
            "select count(*) from information_schema.ins_ext_sources where source_name='fq01_src079m'",
        ],
    ],

    # s06: ALTER immediately reflected in ins_ext_sources (no stale cache)
    ["080m", [_mysql], ["fq01_src080m"], "ALTER host immediately visible in ins_ext_sources — no stale cache delay (FS §3.4.5)", [
            "drop external source if exists fq01_src080m",
            f"create external source fq01_src080m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select `host` from information_schema.ins_ext_sources where source_name='fq01_src080m'",
            "alter external source fq01_src080m set host='altered.example.com'",
            "select `host` from information_schema.ins_ext_sources where source_name='fq01_src080m'",
            _clear_source_step(),
        ],
    ],

    # Schema-change retry semantics: explicit stale-column lookup should retry via cache refresh,
    # while SELECT * only reflects new columns after an explicit REFRESH.
    ["081m", [_mysql], ["fq01_src081m"], "DROP COLUMN without explicit REFRESH: dropped-column query fails, then SELECT * succeeds without the removed column", [
            "drop external source if exists fq01_src081m",
            f"create external source fq01_src081m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` add column extra_note VARCHAR(32) NULL",
                f"update `{_FW_TABLE}` set extra_note='after_refresh' where val=5",
            ]),
            "refresh external source fq01_src081m",
            "select extra_note from fq01_src081m.src_t where val=5",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` drop column extra_note",
            ]),
            "select extra_note from fq01_src081m.src_t where val=5",
            "select * from fq01_src081m.src_t where val=5",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ["081p", [_pg], ["fq01_src081p"], "DROP COLUMN without explicit REFRESH: dropped-column query fails, then SELECT * succeeds without the removed column", [
            "drop external source if exists fq01_src081p",
            f"create external source fq01_src081p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} add column extra_note VARCHAR(32) NULL",
                f"update {_P_SCHEMA}.{_FW_TABLE} set extra_note='after_refresh' where val=5",
            ]),
            "refresh external source fq01_src081p",
            f"select extra_note from fq01_src081p.{_FW_TABLE} where val=5",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} drop column extra_note",
            ]),
            f"select extra_note from fq01_src081p.{_FW_TABLE} where val=5",
            f"select * from fq01_src081p.{_FW_TABLE} where val=5",
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ["082m", [_mysql], ["fq01_src082m"], "ADD COLUMN without explicit REFRESH: immediate SELECT * stays on cached columns; REFRESH then exposes the new column", [
            "drop external source if exists fq01_src082m",
            f"create external source fq01_src082m type='mysql' host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' database='{_M_DB}'",
            "select * from fq01_src082m.src_t where val=5",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` add column extra_note VARCHAR(32) NULL",
                f"update `{_FW_TABLE}` set extra_note='after_refresh' where val=5",
            ]),
            "select * from fq01_src082m.src_t where val=5",
            "refresh external source fq01_src082m",
            "select * from fq01_src082m.src_t where val=5",
            _mysql_exec_step(_M_DB, [
                f"alter table `{_FW_TABLE}` drop column extra_note",
            ]),
            _q_count_step(),
            _clear_source_step(),
        ],
    ],

    ["082p", [_pg], ["fq01_src082p"], "ADD COLUMN without explicit REFRESH: immediate SELECT * stays on cached columns; REFRESH then exposes the new column", [
            "drop external source if exists fq01_src082p",
            f"create external source fq01_src082p type='postgresql' host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' database='{_P_DB}' schema='{_P_SCHEMA}'",
            f"select * from fq01_src082p.{_FW_TABLE} where val=5",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} add column extra_note VARCHAR(32) NULL",
                f"update {_P_SCHEMA}.{_FW_TABLE} set extra_note='after_refresh' where val=5",
            ]),
            f"select * from fq01_src082p.{_FW_TABLE} where val=5",
            "refresh external source fq01_src082p",
            f"select * from fq01_src082p.{_FW_TABLE} where val=5",
            _pg_exec_step(_P_DB, [
                f"alter table {_P_SCHEMA}.{_FW_TABLE} drop column extra_note",
            ]),
            _q_count_step(),
            _clear_source_step(),
        ],
    ],
]


# ── Test class ─────────────────────────────────────────────────────────────

class TestFq01ExternalSource(FederatedQueryVersionedMixin):
    _fw_data_prepared = False

    # Override parent updatecfgDict to also reset the client-side timezone to
    # Asia/Shanghai (CST).  When the full suite is run, a preceding test class
    # (e.g. fq_03 / fq_04) may have issued ALTER LOCAL "timezone" "UTC" which
    # persists in the C library for the whole Python process.  Declaring
    # clientCfg.timezone here causes before_test.get_taos_conn() to apply
    # ALTER LOCAL "timezone" "Asia/Shanghai" at connection time, restoring the
    # expected timezone so that taospy datetime values match the CST baseline.
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "timezone": "Asia/Shanghai",
        "clientCfg": {
            "federatedQueryEnable": 1,
            "timezone": "Asia/Shanghai",
        },
    }

    # ──────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        # Drop any external sources left by other test files from a previous
        # pytest session (external sources persist across taosd restarts via
        # metadata).  Keep only sources whose names start with our own prefix
        # so that SHOW EXTERNAL SOURCES results are not polluted.
        try:
            tdSql.query("show external sources", queryTimes=1)
            foreign = [
                row[0] for row in (tdSql.queryResult or [])
                if not str(row[0]).startswith("fq01_")
            ]
            for _src in foreign:
                try:
                    tdSql.execute(f"drop database if exists {_src}", queryTimes=1)
                    tdSql.execute(f"drop external source if exists {_src}", queryTimes=1)
                except Exception:
                    pass
        except Exception:
            pass

    def setup_method(self, method):
        if TestFq01ExternalSource._fw_data_prepared:
            return
        self._fw_prepare_shared_data()
        TestFq01ExternalSource._fw_data_prepared = True

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")
        TestFq01ExternalSource._fw_data_prepared = False

    # ──────────────────────────────────────────────────────────────────────
    # Shared test data setup
    # ──────────────────────────────────────────────────────────────────────

    def _fw_prepare_shared_data(self):
        mysql_cfg  = self._mysql_cfg()
        pg_cfg     = self._pg_cfg()
        influx_cfg = self._influx_cfg()

        mysql_values = ", ".join(
            f"('{ts}', {val}, {score}, '{name}', {flag})"
            for ts, val, score, name, flag in _FW_ROWS_DT
        )

        ExtSrcEnv.mysql_create_db_cfg(mysql_cfg, _M_DB)
        ExtSrcEnv.mysql_exec_cfg(mysql_cfg, _M_DB, [
            f"drop table if exists `{_FW_TABLE}`",
            (
                f"create table `{_FW_TABLE}` ("
                "ts DATETIME(3) PRIMARY KEY, "
                "val INT, score DOUBLE, name VARCHAR(32), flag TINYINT(1))"
            ),
            f"insert into `{_FW_TABLE}` values {mysql_values}",
        ])

        ExtSrcEnv.pg_create_db_cfg(pg_cfg, _P_DB)
        ExtSrcEnv.pg_exec_cfg(pg_cfg, _P_DB, [
            f"drop table if exists {_P_SCHEMA}.{_FW_TABLE}",
            (
                f"create table {_P_SCHEMA}.{_FW_TABLE} ("
                "ts TIMESTAMP PRIMARY KEY, "
                "val INT, score DOUBLE PRECISION, name VARCHAR(32), flag INT)"
            ),
            f"insert into {_P_SCHEMA}.{_FW_TABLE} values {mysql_values}",
        ])

        ExtSrcEnv.influx_create_db_cfg(influx_cfg, _I_DB)
        influx_lines = [
            (
                f"{_FW_TABLE} val={val}i,score={score},flag={flag}i,name=\"{name}\" "
                f"{_BASE_TS + idx * 60000}000000"
            )
            for idx, (_, val, score, name, flag) in enumerate(_FW_ROWS_DT)
        ]
        ExtSrcEnv.influx_write_cfg(influx_cfg, _I_DB, influx_lines)

    # ──────────────────────────────────────────────────────────────────────
    # Framework helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fw_std_query_table(src_name: str, src_type: str) -> str:
        """Return the fully-qualified query path for the standard test table."""
        if src_type == _mysql:
            return f"{src_name}.{_M_DB}.{_FW_TABLE}"
        elif src_type == _pg:
            return f"{src_name}.{_P_SCHEMA}.{_FW_TABLE}"
        else:  # influxdb
            return f"{src_name}.{_FW_TABLE}"

    @staticmethod
    def _fw_extract_src_name(sql: str) -> Optional[str]:
        """Extract the source name from a CREATE EXTERNAL SOURCE SQL string."""
        m = re.match(
            r"create\s+external\s+source(?:\s+if\s+not\s+exists)?\s+(`[^`]+`|\S+)",
            sql.strip(),
            re.IGNORECASE,
        )
        if m:
            return m.group(1).strip("`")
        return None

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
        """Drop dynamic result columns (e.g. create_time) before serializing ans/tmp output."""
        if not description:
            return description, rows

        keep_indices = [
            i for i, col in enumerate(description)
            if str(col[0]).lower() not in _DYNAMIC_RESULT_COLUMNS
        ]

        if len(keep_indices) == len(description):
            return description, rows

        filtered_desc = [description[i] for i in keep_indices]
        filtered_rows = [tuple(row[i] for i in keep_indices) for row in rows]
        return filtered_desc, filtered_rows

    def _fw_fmt_result(self, description, rows: Sequence) -> List[str]:
        """Format query result with column header line followed by data rows."""
        description, rows = self._fw_drop_dynamic_columns(description, rows)
        lines = []
        if description:
            lines.append("|".join(col[0] for col in description))
        lines.extend(self._fw_fmt_rows(rows))
        return lines

    @staticmethod
    def _fw_normalize_result_lines(result: Union[str, Sequence[str], None]) -> List[str]:
        """Normalize result payload to non-empty lines for stable ans/tmp serialization."""
        if result is None:
            return ["<empty result>"]
        if isinstance(result, str):
            return [result] if result else ["<empty result>"]

        lines = [str(line) for line in result if str(line)]
        return lines if lines else ["<empty result set>"]

    def _fw_append_step_block(
        self,
        blocks: List[str],
        label: str,
        step_tag: str,
        kind: str,
        sql: str,
        result: Union[str, Sequence[str], None],
    ):
        """Append one serialized block that always contains SQL and RESULT sections."""
        lines = [
            f"### {label} {step_tag} {kind}",
            "SQL: " + sql,
            "RESULT:",
        ]
        lines.extend(self._fw_normalize_result_lines(result))
        lines.append("---")
        blocks.append("\n".join(lines))

    @staticmethod
    def _fw_append_case_boundary(
        blocks: List[str],
        label: str,
        desc: str,
        is_start: bool,
    ):
        """Append a visual case boundary that is clearly different from step separators."""
        marker = "CASE START" if is_start else "CASE END"
        blocks.append(
            "\n".join([
                "=" * 96,
                f"{marker}: {label}",
                f"DESC: {desc}",
                "=" * 96,
            ])
        )

    @staticmethod
    def _fw_query_once(sql: str, exit: bool = False):
        """Execute SQL exactly once via tdSql.query (no internal retry)."""
        return tdSql.query(sql, exit=exit, queryTimes=1)

    def _fw_runtime_sql(self, sql: str, src_type: str) -> str:
        """Patch import-time Influx token placeholders with runtime token.

        These framework SQL strings are assembled at module import time. InfluxDB
        token may rotate after `ensure_env()` hard reset, so refresh only the
        `api_token='<import-time-token>'` part for influxdb steps.
        """
        if src_type != _influxdb:
            return sql
        runtime_token = self._influx_cfg().token
        if not runtime_token or runtime_token == _I_TOKEN:
            return sql
        pattern = re.compile(
            r"(api_token'\s*=\s*')" + re.escape(_I_TOKEN) + r"(')",
            re.IGNORECASE,
        )
        return pattern.sub(r"\1" + runtime_token + r"\2", sql)

    def _fw_auto_show_describe(
        self,
        src_name: str,
        blocks: List[str],
        label: str,
        step_tag: str,
    ):
        """Run SHOW (filtered for src_name) and DESCRIBE after a successful CREATE."""
        # SHOW – find the row for this source
        show_sql = "show external sources"
        ok = self._fw_query_once(show_sql, exit=False)
        if ok is not False:
            target_name = self._fw_norm_src_name(src_name)
            matching = [
                row for row in tdSql.queryResult
                if self._fw_norm_src_name(row[_COL_NAME]) == target_name
            ]
            rows_text = self._fw_fmt_result(tdSql.cursor.description, matching)
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "AUTO-SHOW",
                show_sql,
                rows_text,
            )
        else:
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "AUTO-SHOW",
                show_sql,
                "ERROR: show external sources failed",
            )

        # DESCRIBE
        desc_sql = f"describe external source {self._fw_quote_src_name(src_name)}"
        ok2 = self._fw_query_once(desc_sql, exit=False)
        if ok2 is not False:
            rows_text = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult)
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "AUTO-DESCRIBE",
                desc_sql,
                rows_text,
            )
        else:
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "AUTO-DESCRIBE",
                desc_sql,
                "ERROR: describe external source failed",
            )

    def _fw_exec_step(
        self,
        step,
        src_type: str,
        source_names: List[str],
        blocks: List[str],
        label: str,
        step_tag: str,
    ):
        """Recursively execute one step; append serialised result to blocks."""
        primary_src = source_names[0]

        # ── callable step (e.g. lambda t: ...) ────────────────────────────────
        if callable(step):
            self._fw_exec_step(step(src_type), src_type, source_names, blocks, label, step_tag)
            return

        if isinstance(step, str):
            sql = self._fw_runtime_sql(step, src_type)
            stripped = sql.strip()
            is_create = bool(
                re.match(r"create\s+external\s+source", stripped, re.IGNORECASE)
            )
            is_query = bool(
                re.match(
                    r"(select|show|describe|explain)\b",
                    stripped,
                    re.IGNORECASE,
                )
            )
            if is_query:
                ok = self._fw_query_once(sql, exit=False)
                if ok is not False:
                    rows = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult or [])
                else:
                    rows = ["ERROR: query failed"]
                self._fw_append_step_block(
                    blocks,
                    label,
                    step_tag,
                    "QUERY",
                    sql,
                    rows,
                )
            else:
                tdSql.sql = sql
                try:
                    tdSql.affectedRows = tdSql.cursor.execute(sql)
                    exec_result = "OK"
                except Exception as _exec_err:
                    _msg = str(_exec_err).splitlines()[0] if str(_exec_err) else "unknown error"
                    exec_result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(
                    blocks,
                    label,
                    step_tag,
                    "EXEC",
                    sql,
                    exec_result,
                )
                if is_create:
                    created_name = self._fw_extract_src_name(sql)
                    if created_name:
                        self._fw_auto_show_describe(created_name, blocks, label, step_tag)
            return

        # ── count query with retry ─────────────────────────────────────────
        if isinstance(step, _QCountStep):
            table = self._fw_std_query_table(primary_src, src_type)
            sql   = f"select count(*) from {table}"
            tdLog.info(f"[DEBUG] {label} QCountStep: src_type={src_type!r} primary_src={primary_src!r} table={table!r} sql={sql!r}")
            if step.negative:
                # Negative: single attempt, no retry; failure is expected.
                ok = self._fw_query_once(sql, exit=False)
                if ok is False:
                    self._fw_append_step_block(
                        blocks,
                        label,
                        step_tag,
                        "COUNT",
                        sql,
                        "ERROR: count query failed (expected in negative case)",
                    )
                else:
                    rows = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult)
                    self._fw_append_step_block(
                        blocks,
                        label,
                        step_tag,
                        "COUNT",
                        sql,
                        rows,
                    )
            else:
                # Positive: retry every 0.5 s up to 10 s until count matches.
                deadline = time.monotonic() + 10
                while True:
                    self._fw_query_once(sql, exit=True)
                    actual = tdSql.queryResult[0][0] if tdSql.queryResult else None
                    if actual == step.count:
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError(
                            f"{label}: count query timed out; "
                            f"expected {step.count}, last got {actual!r}\n"
                            f"SQL: {sql}"
                        )
                    time.sleep(0.5)
                rows = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult)
                self._fw_append_step_block(
                    blocks,
                    label,
                    step_tag,
                    "COUNT",
                    sql,
                    rows,
                )
            return

        # ── drop all sources in pool ───────────────────────────────────────
        if isinstance(step, _ClearSourceStep):
            for name in source_names:
                sql = f"drop external source if exists {name}"
                try:
                    tdSql.cursor.execute(sql)
                    result = "OK"
                except Exception as _drop_err:
                    _msg = str(_drop_err).splitlines()[0] if str(_drop_err) else "unknown error"
                    result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(
                    blocks,
                    label,
                    step_tag,
                    "CLEANUP",
                    sql,
                    result,
                )
            return

        # ── connection switch ──────────────────────────────────────────────
        if isinstance(step, _ConnectStep):
            sql = f"-- connect user={step.user}"
            try:
                tdSql.connect(step.user, step.password)
                result = "OK"
            except Exception as _conn_err:
                _msg = str(_conn_err).splitlines()[0] if str(_conn_err) else "unknown error"
                result = f"ERROR: {_msg[:200]}"
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "CONNECT",
                sql,
                result,
            )
            return

        if isinstance(step, _ConnectRootStep):
            sql = "-- connect user=root"
            try:
                tdSql.connect("root", "taosdata")
                result = "OK"
            except Exception as _conn_err:
                _msg = str(_conn_err).splitlines()[0] if str(_conn_err) else "unknown error"
                result = f"ERROR: {_msg[:200]}"
            self._fw_append_step_block(
                blocks,
                label,
                step_tag,
                "CONNECT",
                sql,
                result,
            )
            return

        # ── plain query (non-exec): record results ─────────────────────────
        raise ValueError(f"Unknown step type: {type(step).__name__}")

    # ──────────────────────────────────────────────────────────────────────
    # Result file management
    # ──────────────────────────────────────────────────────────────────────

    def _fw_baseline_file(self) -> str:
        label = self._version_label().replace(".", "_").replace("/", "_")
        return os.path.join(
            os.path.dirname(__file__),
            "ans",
            f"test_fq_01_external_source_framework_{label}.txt",
        )

    def _fw_compare_baseline(self, blocks: List[str]):
        actual   = "\n".join(blocks) + "\n"
        # Normalise dynamic influxdb3 admin tokens (apiv3_...) to the stable
        # placeholder "test-token" so that the baseline comparison is stable
        # across test runs even though influxdb3 generates a new random token
        # on each hard reset.  The baseline was captured with "test-token".
        import re as _re
        actual = _re.sub(r"apiv3_[A-Za-z0-9_\-]+", "test-token", actual)
        # TDengine includes the offending SQL text in syntax-error messages.
        # When the real apiv3 token is long the SQL string is longer than with
        # the short "test-token" placeholder, causing TDengine to truncate the
        # error message before the closing ')" chars.  After the token
        # normalisation above both actual and baseline should agree on the
        # token spelling, so strip everything AFTER "test-token" on syntax-
        # error lines in BOTH texts so truncation differences don't matter.
        _SYNTAX_ERR_NORM = _re.compile(
            r'(ERROR: \[0x2600\]: syntax error near "[^\n]*test-token)[^\n]*',
            _re.MULTILINE,
        )
        def _norm_synerr(text):
            return _SYNTAX_ERR_NORM.sub(r"\1", text)

        actual = _norm_synerr(actual)
        baseline = self._fw_baseline_file()
        tmp_file = os.path.join(
            tempfile.gettempdir(),
            f"{os.path.basename(baseline)}.{os.getpid()}.tmp",
        )

        os.makedirs(os.path.dirname(baseline), exist_ok=True)
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(actual)

        if not os.path.isfile(baseline):
            raise AssertionError(
                "Framework baseline file not found\n"
                f"  baseline: {baseline}\n"
                f"  actual  : {tmp_file}\n"
                "  Baseline files must be committed to repository; auto-create/rebuild is forbidden.\n"
                "  Do not delete old ans to regenerate."
            )

        with open(baseline, "r", encoding="utf-8") as f:
            expected = f.read()
        # Apply the same syntax-error normalisation to the baseline so that
        # differences caused by TDengine error-message truncation are absorbed.
        expected = _norm_synerr(expected)

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

        def _case_by_line(lines: List[str], line_no_1based: int) -> str:
            if line_no_1based <= 0:
                return "<unknown-case>"
            i = min(line_no_1based - 1, len(lines) - 1)
            while i >= 0:
                m = re.match(r"CASE START:\s*(.+)$", lines[i])
                if m:
                    return m.group(1)
                i -= 1
            return "<unknown-case>"

        case_hint = _case_by_line(act_lines, diff_line)

        raise AssertionError(
            "Framework baseline mismatch\n"
            f"  baseline: {baseline}\n"
            f"  actual  : {tmp_file}\n"
            f"  case    : {case_hint}\n"
            f"  first diff at line {diff_line}:\n"
            f"    baseline: {exp_val!r}\n"
            f"    actual  : {act_val!r}\n"
            "  Policy: update only the mismatched CASE result lines in ans;\n"
            "          deleting old ans and rebuilding is not allowed."
        )

    # ──────────────────────────────────────────────────────────────────────
    # Main test entry point
    # ──────────────────────────────────────────────────────────────────────

    def test_fq_ext_src_framework(self):
        # FQ_CASES="009,012" → only run CASE-009 and CASE-012
        _only = os.environ.get("FQ_CASES", "").strip()
        _only_set = set(_only.split(",")) if _only else None
        blocks: List[str] = []
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
