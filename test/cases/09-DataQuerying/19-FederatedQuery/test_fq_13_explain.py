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
test_fq_13_explain.py  —  Data-driven EXPLAIN federated query tests

=== DESIGN ===

Every query in ``_EXPLAIN_CASES`` is executed against **all three external
sources** (MySQL, PostgreSQL, InfluxDB).
Cross-source queries are expanded across source-pair combinations.  There are
exactly **four test methods**, one per EXPLAIN mode:

    test_explain                 →  EXPLAIN <sql>
    test_explain_verbose         →  EXPLAIN VERBOSE TRUE <sql>
    test_explain_analyze         →  EXPLAIN ANALYZE <sql>
    test_explain_analyze_verbose →  EXPLAIN ANALYZE VERBOSE TRUE <sql>

Each test iterates ALL cases, expands per-source, executes the SQL in its
mode, runs inline validators, serialises the EXPLAIN output to a temp result
file, and compares against a golden baseline.

=== BASELINE MAINTENANCE POLICY ===

Baseline files live in ``ans/test_fq_13_explain/``.

    **DO NOT** regenerate baseline files by copying tmp output to ans/.

When a baseline diff occurs:
  1. Inspect the tmp file to understand what changed.
  2. Determine whether the change is correct (product improvement) or a bug.
  3. If correct, **manually edit** only the affected entry in the baseline.
  4. If a bug, fix the product — NEVER fix the test or baseline to hide it.

Tmp files are stored in ``tmp/test_fq_13_explain/`` for debugging only.

=== CASE DEFINITION ===

Cases are defined in ``_EXPLAIN_GROUPS``, a dict of category → list of tuples:

    Simple (SQL only — defaults apply):
        ("select * from {SRC}.sensor",),

    With options:
        ("select ... from {SRC}.sensor", dict(remote_has=["WHERE"])),

Defaults for each case:
    sources     = "single"         (expand across all 3 sources)
    checks      = ["federated_scan", "analyze_metrics"]
    check_args  = {}

Recognized option keys:
    checks         list[str]   — OVERRIDE full checks list
    checks_add     list[str]   — ADD to default checks
    remote_has     list[str]   — keywords expected in Remote SQL (hard)
    remote_any_has list[str]   — keywords expected across all Remote SQL lines
    soft_remote_has list[str]  — keywords expected in Remote SQL (soft/warn)
    remote_not_has list[str]   — keywords NOT expected in Remote SQL
    remote_any_not_has list[str] — keywords NOT expected across all Remote SQL lines
    remote_any_count_eq list[tuple[str,int]] — exact keyword count across Remote SQL lines
    remote_any_count_ge list[tuple[str,int]] — minimum keyword count across Remote SQL lines
    local_has      list[str]   — local plan markers that must remain local
    analyze_local_has list[str] — analyze-only local plan markers that must
                                 appear concretely in the local plan
    plain_local_has list[str] — plain-explain-only local plan markers
    local_not_has  list[str]   — local operators / markers that should be absent
    expect_err_contains list[str] — required substrings in expected EXPLAIN error
    no_data_leak   list[str]   — values that must NOT appear in plan text
    output_cols_eq int         — expected first "Output: columns=" value
    min_scans      int         — minimum Federated Scan count (default 2)
    sources        str         — "single" or "cross"
    pre_sql        str|list    — execute before EXPLAIN for each expanded case
    post_sql       str|list    — execute after EXPLAIN for each expanded case

=== VALIDATORS ===

    federated_scan       — plan contains "Federated Scan"                     (all modes)
    federated_scan_on    — plan contains "Federated Scan on <SRC>..."         (all modes)
    remote_sql           — plan contains "Remote SQL:"                        (verbose only)
    type_mapping         — plan contains "Type Mapping:" + "<-"               (verbose only)
    output_cols_eq       — first output column count equals expected value     (verbose only)
    pushdown_flags       — plan contains "Pushdown:"                          (verbose only)
    columns_list         — plan contains "columns="                           (verbose only)
    analyze_metrics      — plan contains execution metrics (rows=/time=/...)  (analyze only)
    remote_has           — Remote SQL contains keyword(s)                     (verbose only)
    remote_any_has       — any Remote SQL line set contains keyword(s)        (verbose only)
    remote_not_has       — Remote SQL does NOT contain keyword(s)             (verbose only)
    remote_any_not_has   — all Remote SQL lines do NOT contain keyword(s)     (verbose only)
    remote_any_count_eq  — Remote SQL(all) contains keyword exactly N times   (verbose only)
    remote_any_count_ge  — Remote SQL(all) contains keyword at least N times  (verbose only)
    local_has            — local plan retains marker(s). When
                           analyze_local_has is present, this check is
                           applied to non-analyze modes only                 (all modes)
    analyze_local_has    — local plan retains concrete marker(s) in analyze
                           modes                                              (analyze only)
    local_not_has       — local plan does NOT contain operator / marker(s)    (all modes)
    no_data_leak         — plan does NOT contain listed data values           (all modes)
    dialect_backtick     — Remote SQL uses backtick quoting                   (verbose only)
    dialect_double_quote — Remote SQL uses double-quote quoting               (verbose only)
    dialect_bracket      — Remote SQL uses [bracket] quoting (SQL Server)     (verbose only)
    min_federated_scans  — plan has >= N "Federated Scan" occurrences         (all modes)
    single_federated_scan  — plan has exactly 1 "Federated Scan" occurrence    (all modes)
    no_table_scan        — plan does NOT contain "Table Scan"                 (all modes)
    compound_where       — Remote SQL contains AND or OR                      (verbose only)
    projection_no_star   — Remote SQL does not use "SELECT *"                 (verbose only)
    join_remote_or_multi — JOIN pushed to remote, or >=2 FederatedScans       (all modes)
    subquery_handling    — plan has Federated Scan and no local TableScan (all data sourced from remote)   (verbose only)
All validators are HARD by default — every assertion must pass or the test fails.
This ensures DS-mandated pushdown is actually implemented; missing pushdown
surfaces immediately as a test failure rather than a silent skip.
"""

import os
import re
import sys
import time as _time

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
)


# ---------------------------------------------------------------------------
# EXPLAIN mode constants
# ---------------------------------------------------------------------------
EXPLAIN = "explain"
EXPLAIN_VERBOSE = "explain verbose true"
EXPLAIN_ANALYZE = "explain analyze"
EXPLAIN_ANALYZE_VERBOSE = "explain analyze verbose true"

ALL_MODES = [EXPLAIN, EXPLAIN_VERBOSE, EXPLAIN_ANALYZE, EXPLAIN_ANALYZE_VERBOSE]
VERBOSE_MODES = {EXPLAIN_VERBOSE, EXPLAIN_ANALYZE_VERBOSE}
ANALYZE_MODES = {EXPLAIN_ANALYZE, EXPLAIN_ANALYZE_VERBOSE}

_MODE_LABELS = {
    EXPLAIN: "plain",
    EXPLAIN_VERBOSE: "verbose",
    EXPLAIN_ANALYZE: "analyze",
    EXPLAIN_ANALYZE_VERBOSE: "analyze_verbose",
}

_MODE_BASELINE_NAMES = {
    EXPLAIN: "test_explain",
    EXPLAIN_VERBOSE: "test_explain_verbose",
    EXPLAIN_ANALYZE: "test_explain_analyze",
    EXPLAIN_ANALYZE_VERBOSE: "test_explain_analyze_verbose",
}

# Legacy group metadata defined above still references this sentinel, but the
# executed _EXPLAIN_CASES table below no longer relies on sentinel checks.
_LOCAL_FALLBACK_SENTINEL = "<LOCAL_FALLBACK>"

# ---------------------------------------------------------------------------
# External test data
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

_MYSQL_DB = "fq_explain_m"
_MYSQL_SETUP_SQLS = [
    "DROP TABLE IF EXISTS sensor",
    "CREATE TABLE IF NOT EXISTS sensor "
    "(`time` DATETIME NOT NULL, voltage DOUBLE, current FLOAT, region VARCHAR(32), "
    "date_str VARCHAR(32), json_str VARCHAR(64), b64_str VARCHAR(32), geom_wkt VARCHAR(64))",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00',220.5,1.2,'north','2024-01-01','{\"k\":1}','aGVsbG8=','POINT(0 0)'),"
    "('2024-01-01 00:01:00',221.0,1.3,'south','2024-01-02','{\"k\":2}','d29ybGQ=','POINT(1 1)'),"
    "('2024-01-01 00:02:00',219.8,1.1,'north','2024-01-03','{\"k\":3}','Zm9v','POINT(2 2)'),"
    "('2024-01-01 00:03:00',222.0,1.4,'south','2024-01-04','{\"k\":4}','YmFy','POINT(3 3)'),"
    "('2024-01-01 00:04:00',220.0,1.0,'north','2024-01-05','{\"k\":5}','YmF6','POINT(4 4)')",
    "DROP TABLE IF EXISTS region_info",
    "CREATE TABLE IF NOT EXISTS region_info "
    "(`time` DATETIME NOT NULL, region VARCHAR(32), area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES "
    "('2024-01-01 00:00:00','north',1),"
    "('2024-01-01 00:01:00','south',2)",
]

_MYSQL_ASCII_DB = "fq_explain_m_ascii"
_MYSQL_ASCII_SETUP_SQLS = [
    "DROP TABLE IF EXISTS sensor",
    "CREATE TABLE IF NOT EXISTS sensor "
    "(`time` DATETIME NOT NULL, region VARCHAR(32), "
    "region_ascii VARCHAR(32) CHARACTER SET ascii, "
    "b64_ascii VARCHAR(32) CHARACTER SET ascii)",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00','north','north','aGVsbG8='),"
    "('2024-01-01 00:01:00','south','south','d29ybGQ='),"
    "('2024-01-01 00:02:00','north','north','Zm9v'),"
    "('2024-01-01 00:03:00','south','south','YmFy'),"
    "('2024-01-01 00:04:00','north','north','YmF6')",
    "DROP TABLE IF EXISTS region_info",
    "CREATE TABLE IF NOT EXISTS region_info "
    "(`time` DATETIME NOT NULL, region VARCHAR(32), "
    "region_ascii VARCHAR(32) CHARACTER SET ascii, area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES "
    "('2024-01-01 00:00:00','north','north',1),"
    "('2024-01-01 00:01:00','south','south',2)",
]

_PG_DB = "fq_explain_p"
_PG_SETUP_SQLS = [
    "ALTER DATABASE fq_explain_p SET timezone TO 'Asia/Shanghai'",
    "SET TIME ZONE 'Asia/Shanghai'",
    "CREATE EXTENSION IF NOT EXISTS postgis",
    "DROP TABLE IF EXISTS sensor",
    "CREATE TABLE IF NOT EXISTS sensor "
    "(\"time\" TIMESTAMPTZ NOT NULL, voltage FLOAT8, current REAL, region TEXT, "
    "date_str TEXT, json_str TEXT, b64_str TEXT, geom_wkt TEXT)",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00+00',220.5,1.2,'north','2024-01-01','{\"k\":1}','aGVsbG8=','POINT(0 0)'),"
    "('2024-01-01 00:01:00+00',221.0,1.3,'south','2024-01-02','{\"k\":2}','d29ybGQ=','POINT(1 1)'),"
    "('2024-01-01 00:02:00+00',219.8,1.1,'north','2024-01-03','{\"k\":3}','Zm9v','POINT(2 2)'),"
    "('2024-01-01 00:03:00+00',222.0,1.4,'south','2024-01-04','{\"k\":4}','YmFy','POINT(3 3)'),"
    "('2024-01-01 00:04:00+00',220.0,1.0,'north','2024-01-05','{\"k\":5}','YmF6','POINT(4 4)')",
    "DROP TABLE IF EXISTS region_info",
    "CREATE TABLE IF NOT EXISTS region_info "
    "(\"time\" TIMESTAMPTZ NOT NULL, region TEXT, area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES "
    "('2024-01-01 00:00:00+00','north',1),"
    "('2024-01-01 00:01:00+00','south',2)",
]

_PG_ASCII_DB = "fq_explain_p_ascii"
_PG_ASCII_SETUP_SQLS = [
    "ALTER DATABASE fq_explain_p_ascii SET timezone TO 'Asia/Shanghai'",
    "SET TIME ZONE 'Asia/Shanghai'",
    "DROP TABLE IF EXISTS sensor",
    "CREATE TABLE IF NOT EXISTS sensor "
    "(\"time\" TIMESTAMPTZ NOT NULL, region VARCHAR(32), region_ascii VARCHAR(32), "
    "b64_ascii VARCHAR(32))",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00+00','north','north','aGVsbG8='),"
    "('2024-01-01 00:01:00+00','south','south','d29ybGQ='),"
    "('2024-01-01 00:02:00+00','north','north','Zm9v'),"
    "('2024-01-01 00:03:00+00','south','south','YmFy'),"
    "('2024-01-01 00:04:00+00','north','north','YmF6')",
    "DROP TABLE IF EXISTS region_info",
    "CREATE TABLE IF NOT EXISTS region_info "
    "(\"time\" TIMESTAMPTZ NOT NULL, region VARCHAR(32), "
    "region_ascii VARCHAR(32), area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES "
    "('2024-01-01 00:00:00+00','north','north',1),"
    "('2024-01-01 00:01:00+00','south','south',2)",
]

_INFLUX_BUCKET = "fq_explain_i"
_INFLUX_LINES = [
    f"sensor,region=north voltage=220.5,current=1.2,date_str=\"2024-01-01\",json_str=\"v1\",b64_str=\"aGVsbG8=\",geom_wkt=\"POINT(0 0)\" {_BASE_TS}000000",
    f"sensor,region=south voltage=221.0,current=1.3,date_str=\"2024-01-02\",json_str=\"v2\",b64_str=\"d29ybGQ=\",geom_wkt=\"POINT(1 1)\" {_BASE_TS + 60_000}000000",
    f"sensor,region=north voltage=219.8,current=1.1,date_str=\"2024-01-03\",json_str=\"v3\",b64_str=\"Zm9v\",geom_wkt=\"POINT(2 2)\" {_BASE_TS + 120_000}000000",
    f"sensor,region=south voltage=222.0,current=1.4,date_str=\"2024-01-04\",json_str=\"v4\",b64_str=\"YmFy\",geom_wkt=\"POINT(3 3)\" {_BASE_TS + 180_000}000000",
    f"sensor,region=north voltage=220.0,current=1.0,date_str=\"2024-01-05\",json_str=\"v5\",b64_str=\"YmF6\",geom_wkt=\"POINT(4 4)\" {_BASE_TS + 240_000}000000",
    # region_info — identical schema/data to MySQL/PG (time, region, area).
    # region is a tag (mirrors sensor), area an integer field.
    f"region_info,region=north area=1i {_BASE_TS}000000",
    f"region_info,region=south area=2i {_BASE_TS + 60_000}000000",
]

# Source names
_SRC_MYSQL = "fq_exp_mysql"
_SRC_PG = "fq_exp_pg"
_SRC_INFLUX = "fq_exp_influx"
_SRC_MYSQL_ASCII = "fq_exp_mysql_ascii"
_SRC_PG_ASCII = "fq_exp_pg_ascii"
_SRC_SQLSERVER = "fq_exp_sqlserver"
_SQLSERVER_DB  = "fq_exp_ssdb"

# Per-source metadata for expansion
_SINGLE_SOURCES = [
    {"label": "mysql",  "src": _SRC_MYSQL,  "db": _MYSQL_DB},
    {"label": "pg",     "src": _SRC_PG,     "db": _PG_DB},
    {"label": "influx", "src": _SRC_INFLUX, "db": _INFLUX_BUCKET},
    # SQL Server joined phase 2 milestone by milestone (M1 operators +
    # FILTER/PROJECTION ... M4 time family / windows / subqueries).  All four
    # capability faces are enabled now, so the leg runs the full check set;
    # checks whose expectation dicts carry no "sqlserver" key stay no-ops for
    # it via _per_src().
    {"label": "sqlserver", "src": _SRC_SQLSERVER, "db": _SQLSERVER_DB},
]

_ASCII_LENGTH_SOURCES = [
    {"label": "mysql_ascii", "src": _SRC_MYSQL_ASCII, "db": _MYSQL_ASCII_DB},
    {"label": "pg_ascii", "src": _SRC_PG_ASCII, "db": _PG_ASCII_DB},
]

_ASCII_VARCHAR_SOURCES = _ASCII_LENGTH_SOURCES

# Cross-source pairs for JOINs across different external DBs
_CROSS_PAIRS = [
    {"label": "mysql_pg",     "src": _SRC_MYSQL,  "db": _MYSQL_DB,
     "src2": _SRC_PG,     "db2": _PG_DB},
    {"label": "mysql_influx", "src": _SRC_MYSQL,  "db": _MYSQL_DB,
     "src2": _SRC_INFLUX, "db2": _INFLUX_BUCKET},
    {"label": "pg_influx",    "src": _SRC_PG,     "db": _PG_DB,
     "src2": _SRC_INFLUX, "db2": _INFLUX_BUCKET},
    # Cross pairs with the SQL Server leg.
    {"label": "mysql_sqlserver", "src": _SRC_MYSQL, "db": _MYSQL_DB,
     "src2": _SRC_SQLSERVER, "db2": _SQLSERVER_DB},
    {"label": "pg_sqlserver",    "src": _SRC_PG,    "db": _PG_DB,
     "src2": _SRC_SQLSERVER, "db2": _SQLSERVER_DB},
    {"label": "influx_sqlserver", "src": _SRC_INFLUX, "db": _INFLUX_BUCKET,
     "src2": _SRC_SQLSERVER, "db2": _SQLSERVER_DB},
]


# =========================================================================
# Query cases — grouped by category, tuple-based definition
#
# Format: (sql_template,) or (sql_template, opts_dict)
# Defaults: sources="single", checks=["federated_scan","analyze_metrics"]
# =========================================================================

_DEFAULT_CHECKS = ["federated_scan", "analyze_metrics"]

# =========================================================================
# DS-aligned comprehensive pushdown matrix
# -------------------------------------------------------------------------
# Each case is one of three classes:
#   1. PUSHDOWN  — DS §5.3.x declares the construct must be sent to the
#                   external source.  Asserted via ``remote_has`` (HARD).
#   2. LOCAL     — DS declares TDengine-specific or unsupported in target.
#                   Asserted via ``remote_not_has`` (HARD): the keyword
#                   must NOT appear in Remote SQL.
#   3. STRUCTURAL — Plan-level invariants (FederatedScan presence, no
#                   data leak, dialect quoting, etc.).
#
# Strict mode is the default (see _is_strict): every assertion is hard.
# A test failure here means a DS-mandated pushdown is NOT actually
# emitted by the planner — that is the bug the test is designed to find.
# =========================================================================

_EXPLAIN_GROUPS = {

    # ====================================================================
    # §5.3.3 Operators
    # ====================================================================

    # §5.3.3.1 Arithmetic operators (+ - * / %)
    "op_arith": [
        ("select voltage + 1.0 from {SRC}.sensor",
         dict(remote_has=["+"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select voltage - 1.0 from {SRC}.sensor",
         dict(remote_has=["-"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select voltage * 2.0 from {SRC}.sensor",
         dict(remote_has=["*"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select voltage / 2.0 from {SRC}.sensor",
         dict(remote_has=["/"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        # Modulo: MySQL/PG keep MOD or %, Influx converts to % — all push %
        ("select area % 2 from {SRC}.region_info",
         dict(remote_has=["%"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.3.2 Comparison operators
    "op_compare_eq": [
        ("select * from {SRC}.sensor where voltage = 220.0",
         dict(remote_has=["WHERE", "="],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_ne": [
        ("select * from {SRC}.sensor where voltage <> 220.0",
         dict(remote_has=["WHERE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_lt": [
        ("select * from {SRC}.sensor where voltage < 222.0",
         dict(remote_has=["WHERE", "<"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_gt": [
        ("select * from {SRC}.sensor where voltage > 219.0",
         dict(remote_has=["WHERE", ">"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_between": [
        ("select * from {SRC}.sensor where voltage between 219.0 and 222.0",
         dict(remote_has=["WHERE", "BETWEEN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_in": [
        ("select * from {SRC}.sensor where voltage in (220.0, 221.0)",
         dict(remote_has=["WHERE", "IN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_like": [
        ("select * from {SRC}.sensor where region like 'no%'",
         dict(remote_has=["WHERE", "LIKE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_compare_isnull": [
        ("select * from {SRC}.sensor where region is null",
         dict(remote_has=["WHERE", "NULL"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select * from {SRC}.sensor where region is not null",
         dict(remote_has=["WHERE", "NULL"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.3.3 Logical operators
    "op_logical_and": [
        ("select * from {SRC}.sensor where voltage > 219.0 and region = 'north'",
         dict(remote_has=["WHERE", "AND"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_logical_or": [
        ("select * from {SRC}.sensor where voltage > 222.0 or region = 'north'",
         dict(remote_has=["WHERE", "OR"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "op_logical_not": [
        ("select * from {SRC}.sensor where not (voltage > 222.0)",
         dict(remote_has=["WHERE", "NOT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.3.4 Bitwise operators — push on MySQL/PG, LOCAL on InfluxDB v3
    "op_bitwise": [
        ("select area & 1 from {SRC}.region_info",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["&"], "pg": ["&"]},
              remote_not_has={"influx": [" & "]})),
        ("select area | 1 from {SRC}.region_info",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["|"], "pg": ["|"]},
              remote_not_has={"influx": [" | "]})),
    ],
    # InfluxDB v3 has no bitwise operators — must NOT appear in Remote SQL.
    # Distinct statement (cast + bitwise in WHERE); runs on all sources.
    "op_bitwise_cast": [
        ("select voltage from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["WHERE", "CAST", "&"],
                          "pg": ["WHERE", "CAST", "&"]},
              remote_not_has={"influx": [" & ", " | "]})),
    ],

    # ====================================================================
    # §5.3.4 Functions
    # ====================================================================

    # §5.3.4.1 Math functions
    "func_math_abs": [
        ("select abs(voltage - 220.0) from {SRC}.sensor",
         dict(remote_has=["ABS"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_math_ceil": [
        ("select ceil(voltage) from {SRC}.sensor",
         dict(remote_has=["CEIL"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_math_floor": [
        ("select floor(voltage) from {SRC}.sensor",
         dict(remote_has=["FLOOR"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_math_round": [
        ("select round(voltage, 1) from {SRC}.sensor",
         dict(remote_has=["ROUND"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_math_sqrt": [
        ("select sqrt(voltage) from {SRC}.sensor",
         dict(remote_has=["SQRT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_math_pow": [
        ("select pow(voltage, 2) from {SRC}.sensor",
         dict(remote_has=["POW"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.2 String functions
    "func_str_upper": [
        ("select upper(region) from {SRC}.sensor",
         dict(remote_has=["UPPER"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_str_lower": [
        ("select lower(region) from {SRC}.sensor",
         dict(remote_has=["LOWER"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_str_concat": [
        ("select concat(region, '_x') from {SRC}.sensor",
         dict(remote_has=["CONCAT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_str_trim": [
        ("select trim(region) from {SRC}.sensor",
         dict(remote_has=["TRIM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_str_length": [
        # Multi-byte VARCHAR/TEXT are mapped to TDengine NCHAR, so LENGTH must
        # preserve UCS-4 byte semantics via CHAR_LENGTH(expr) * 4.
        ("select length(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["CHAR_LENGTH", "* 4"],
                          "pg": ["CHAR_LENGTH", "* 4"],
                          "influx": ["CHAR_LENGTH", "* 4"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        # ASCII-only VARCHAR must stay on the VARCHAR/OCTET_LENGTH branch.
        ("select length(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_LENGTH_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["LENGTH"],
                          "pg_ascii": ["OCTET_LENGTH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select length(region_ascii) from {SRC}.region_info",
         dict(sources="custom",
              source_infos=_ASCII_LENGTH_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["LENGTH"],
                          "pg_ascii": ["OCTET_LENGTH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_str_substring": [
        ("select substring(region, 1, 2) from {SRC}.sensor",
         dict(remote_has=["SUBSTRING"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.x Encoding / hashing functions — source-specific availability.
    # MySQL pushes TO_BASE64 / SHA1 / CRC32 directly; PG/InfluxDB have no
    # native equivalent so they run locally (keyword must NOT reach Remote SQL).
    "func_str_to_base64": [
        ("select to_base64(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["TO_BASE64", "ENCODE"],
                              "pg": ["TO_BASE64", "ENCODE"],
                              "influx": ["TO_BASE64", "ENCODE"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select to_base64(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["TO_BASE64"],
                          "pg_ascii": ["ENCODE"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_hash_sha1": [
        ("select sha1(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["SHA1"],
                              "pg": ["SHA1"],
                              "influx": ["SHA1"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select sha1(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["SHA1"]},
              remote_not_has={"pg_ascii": ["SHA1"]},
              local_has={"pg_ascii": ["Projection"]},
              local_not_has={"mysql_ascii": ["Projection"]})),
    ],
    "func_hash_crc32": [
        ("select crc32(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["CRC32"],
                              "pg": ["CRC32"],
                              "influx": ["CRC32"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select crc32(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["CRC32"]},
              remote_not_has={"pg_ascii": ["CRC32"]},
              local_has={"pg_ascii": ["Projection"]},
              local_not_has={"mysql_ascii": ["Projection"]})),
    ],

    # §5.3.4.3 Type / cast functions
    "func_cast": [
        ("select cast(voltage as varchar(32)) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has=["CAST"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.4 Time / date functions
    # now() is constant-folded at plan time and pushed as a timestamp literal,
    # e.g. SELECT '2026-06-05 07:15:26.375' ... — NOT as NOW() function call.
    # Verify the query is pushed down (remote SELECT/FROM present), not the
    # NOW() keyword itself.
    "func_time_now": [
        ("select now() from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.5 Hash functions
    "func_hash_md5": [
        ("select md5(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["MD5"],
                              "pg": ["MD5"],
                              "influx": ["MD5"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select md5(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["MD5"],
                          "pg_ascii": ["MD5"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.6 Aggregate functions
    "func_agg_count": [
        ("select count(*) from {SRC}.sensor",
         dict(remote_has=["COUNT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_sum": [
        ("select sum(voltage) from {SRC}.sensor",
         dict(remote_has=["SUM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_avg": [
        ("select avg(voltage) from {SRC}.sensor",
         dict(remote_has=["AVG"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_min": [
        ("select min(voltage) from {SRC}.sensor",
         dict(remote_has=["MIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_max": [
        ("select max(voltage) from {SRC}.sensor",
         dict(remote_has=["MAX"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_stddev": [
        ("select stddev_pop(voltage) from {SRC}.sensor",
         dict(remote_has=["STDDEV_POP"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_agg_var": [
        ("select var_pop(voltage) from {SRC}.sensor",
         dict(remote_has=["VAR_POP"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.7 Comparison / null-handling functions
    "func_coalesce": [
        ("select coalesce(region, 'unknown') from {SRC}.sensor",
         dict(remote_has=["COALESCE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "func_nullif": [
        ("select nullif(voltage, 220.0) from {SRC}.sensor",
         dict(remote_has=["NULLIF"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.4.x TDengine-specific functions — must NOT push down
    "func_local_csum": [
        ("select csum(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["CSUM"])),
    ],
    "func_local_diff": [
        ("select diff(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["DIFF"])),
    ],
    "func_local_derivative": [
        ("select derivative(voltage, 1s, 0) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["DERIVATIVE"])),
    ],
    "func_local_twa": [
        ("select twa(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["TWA"])),
    ],
    "func_local_first": [
        ("select first(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["FIRST("])),
    ],
    "func_local_last": [
        ("select last(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["LAST("])),
    ],
    "func_local_top": [
        ("select top(voltage, 2) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["TOP("])),
    ],
    "func_local_bottom": [
        ("select bottom(voltage, 2) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["BOTTOM("])),
    ],
    "func_local_elapsed": [
        ("select elapsed(`time`) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["ELAPSED"])),
    ],

    # ====================================================================
    # §5.3.5 Windows
    # ====================================================================

    # §5.3.5.1 INTERVAL — must convert to GROUP BY + bucket expression.
    # The TDengine literal `INTERVAL(...)` keyword must NOT appear in
    # Remote SQL; the converted query must contain `GROUP BY`.
    "win_interval": [
        ("select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m)",
         dict(checks_add=["remote_sql"],
              remote_has=["GROUP BY"],
              remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.5.x TDengine-specific windows — must NOT push as keyword
    "win_local_state": [
        ("select count(*), region from {SRC}.sensor state_window(region)",
         dict(checks_add=["remote_sql"], remote_not_has=["STATE_WINDOW"])),
    ],
    "win_local_session": [
        ("select count(*) from {SRC}.sensor session(`time`, 1m)",
         dict(checks_add=["remote_sql"], remote_not_has=["SESSION("])),
    ],
    "win_local_count": [
        ("select count(*) from {SRC}.sensor count_window(2)",
         dict(checks_add=["remote_sql"], remote_not_has=["COUNT_WINDOW"])),
    ],
    "win_local_event": [
        ("select count(*) from {SRC}.sensor "
         "event_window start with voltage > 220.0 end with voltage < 220.0",
         dict(checks_add=["remote_sql"], remote_not_has=["EVENT_WINDOW"])),
    ],

    # ====================================================================
    # §5.3.6 JOINs
    # ====================================================================

    # Same-source INNER JOIN — push entire join (all sources have region_info)
    "join_inner": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "inner join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["JOIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s, "
         "{SRC}.region_info r where s.`time` = r.`time` "
         "and s.region = r.region",
         dict(checks_add=["remote_sql", "single_federated_scan",
                          "no_table_scan"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # Same-source LEFT OUTER JOIN
    "join_left": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "left join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["LEFT", "JOIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # Same-source RIGHT OUTER JOIN
    "join_right": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "right join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["RIGHT", "JOIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # Same-source FULL OUTER JOIN — PG/InfluxDB native; MySQL has no FULL OUTER
    # JOIN, so it runs locally (multi-scan, no remote FULL JOIN keyword).
    "join_full": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "full join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has={"pg": ["FULL", "JOIN"], "influx": ["FULL", "JOIN"]},
              remote_not_has={"mysql": ["FULL"]})),
    ],

    # Cross-source JOIN — must split into multiple Federated Scans, no JOIN
    "join_cross_source": [
        ("select m.`time`, m.voltage, p.current from {SRC}.sensor m "
         "join {SRC2}.sensor p on m.`time` = p.`time` "
         "and m.region = p.region",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics"],
              min_scans=2)),
    ],

    # §5.3.6 JOIN — branch-condition coverage
    # Both sides pushable + WHERE filter on each side → JOIN + WHERE in remote
    "join_inner_with_where": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "inner join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region "
         "where s.voltage > 220.0",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["JOIN", "WHERE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # Both sides pushable + ORDER BY + LIMIT on outer result → full pushdown
    "join_inner_order_limit": [
        ("select s.`time`, s.voltage, r.area from {SRC}.sensor s "
         "inner join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region "
         "order by s.`time` limit 5",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["JOIN", "ORDER BY", "LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # Not-pushable JOIN type: cross-source (both sides point to different external sources)
    # produces multiple FedScans; no remote JOIN keyword
    # Note: ASOF/SEMI/ANTI JOIN syntax is not supported for external tables (parser error),
    # so cross-source JOIN is the canonical "local-only JOIN" test case.
    # (join_cross_source already covers this; this group is intentionally left empty to
    # preserve the structural placeholder — the coverage is in join_cross_source above.)

    # Left join + GROUP BY / aggregation over joined result
    # JOIN is pushed to remote; GROUP BY is evaluated locally (AGG+JOIN pushdown
    # not yet implemented — JOIN alone is the first-phase behavior).
    "join_left_with_groupby": [
        ("select r.area, count(s.voltage) from {SRC}.sensor s "
         "left join {SRC}.region_info r on s.`time` = r.`time` "
         "and s.region = r.region "
         "group by r.area",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["LEFT", "JOIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # ====================================================================
    # §5.3.7 Subqueries
    # ====================================================================

    # §5.3.7.1.1 Nested FROM subquery — all 3 sources push down
    "subq_nested": [
        ("select * from "
         "(select `time`, voltage from {SRC}.sensor where voltage > 220.0) t "
         "order by t.`time`",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_has=["WHERE", "ORDER BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.7.1.2 Scalar uncorrelated subquery — all 3 sources push down
    "subq_scalar": [
        ("select `time`, voltage from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor)",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_has=["AVG("],
              local_not_has=["InitPlan 1"])),
    ],

    # §5.3.7.1.3 IN (subquery) over a sibling table (region_info).
    # MySQL/PG push the IN subquery; InfluxDB runs it locally (no IN-subquery
    # pushdown in the matrix) but must still execute successfully.
    "subq_in": [
        ("select * from {SRC}.sensor "
         "where region in (select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    # IN (subquery) over the SAME table (self-referencing inner SELECT).
    # Distinct statement from subq_in (different inner query); same per-source
    # split: MySQL/PG push, InfluxDB local.
    "subq_in_self": [
        ("select * from {SRC}.sensor "
         "where region in (select region from {SRC}.sensor where voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_in": [
        ("select * from {SRC}.sensor "
         "where region not in (select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_in_self": [
        ("select * from {SRC}.sensor "
         "where region not in (select region from {SRC}.sensor where voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],

    # §5.3.7.1.4 EXISTS (correlated) — TDengine does not support subquery as
    # expression with an outer correlation ([0x26a6]); EXPLAIN fails at parse
    # for ALL sources (source-independent parser limitation).
    "subq_exists": [
        ("select * from {SRC}.sensor s where exists "
         "(select 1 from {SRC}.region_info r where r.region = s.region)",
         dict(expect_err=True)),
        # Non-correlated EXISTS is still valid and should execute through FQ.
        ("select * from {SRC}.sensor s where exists "
         "(select 1 from {SRC}.sensor r where r.voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["EXISTS"], "pg": ["EXISTS"]},
              remote_not_has={"influx": ["EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1", "Projection"]})),
    ],
    "subq_not_exists": [
        ("select * from {SRC}.sensor s where not exists "
         "(select 1 from {SRC}.region_info r where r.region = s.region)",
         dict(expect_err=True)),
        # Non-correlated NOT EXISTS is also valid; only the correlated form is
        # blocked by TDengine's subquery-as-expression limitation.
        ("select * from {SRC}.sensor s where not exists "
         "(select 1 from {SRC}.sensor r where r.voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT", "EXISTS"], "pg": ["NOT", "EXISTS"]},
              remote_not_has={"influx": ["NOT EXISTS", "NOT  EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1", "Projection"]})),
    ],
    # EXISTS (non-correlated) — runs on all sources.  MySQL/PG push [NOT]
    # EXISTS; InfluxDB runs locally (no EXISTS pushdown) but must still execute.
    "subq_exists_nocorr": [
        ("select * from {SRC}.sensor s where exists "
         "(select 1 from {SRC}.sensor r where r.voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["EXISTS"], "pg": ["EXISTS"]},
              remote_not_has={"influx": ["EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_exists_nocorr": [
        ("select * from {SRC}.sensor s where not exists "
         "(select 1 from {SRC}.sensor r where r.voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT", "EXISTS"],
                          "pg": ["NOT", "EXISTS"]},
              remote_not_has={"influx": ["NOT EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    # §5.3.7.1.5 ALL/ANY/SOME — MySQL/PG push the quantified comparison;
    # InfluxDB runs locally but must still execute.
    "subq_any": [
        ("select * from {SRC}.sensor where voltage > any "
         "(select voltage from {SRC}.sensor where region = 'south')",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_all": [
        ("select * from {SRC}.sensor where voltage > all "
         "(select voltage from {SRC}.sensor where region = 'south')",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ALL"], "pg": ["ALL"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_some": [
        ("select * from {SRC}.sensor where voltage > some "
         "(select voltage from {SRC}.sensor where region = 'south')",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    # DIRECT_SQL variants: same subquery forms, but the inner SQL uses bare
    # table names under USE <source> context instead of source-qualified paths.
    # These cases run on all 3 sources and are the explicit non-recursive
    # coverage for the DIRECT_SQL render shape.
    "subq_nested_direct": [
        ("select * from "
         "(select `time`, voltage from sensor where voltage > 220.0) t "
         "order by t.`time`",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql", "subquery_handling"],
              remote_has=["WHERE", "ORDER BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "subq_scalar_direct": [
        ("select `time`, voltage from sensor "
         "where voltage > (select avg(voltage) from sensor)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql", "subquery_handling"],
              remote_has=["AVG("],
              local_not_has=["InitPlan 1"])),
    ],
    "subq_in_direct": [
        ("select * from sensor "
         "where region in (select region from region_info)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_in_self_direct": [
        ("select * from sensor "
         "where region in (select region from sensor where voltage > 220)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_in_direct": [
        ("select * from sensor "
         "where region not in (select region from region_info)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_in_self_direct": [
        ("select * from sensor "
         "where region not in (select region from sensor where voltage > 220)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_exists_nocorr_direct": [
        ("select * from sensor s where exists "
         "(select 1 from sensor r where r.voltage > 220)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["EXISTS"], "pg": ["EXISTS"]},
              remote_not_has={"influx": ["EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_not_exists_nocorr_direct": [
        ("select * from sensor s where not exists "
         "(select 1 from sensor r where r.voltage > 220)",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT", "EXISTS"],
                          "pg": ["NOT", "EXISTS"]},
              remote_not_has={"influx": ["NOT EXISTS"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_any_direct": [
        ("select * from sensor where voltage > any "
         "(select voltage from sensor where region = 'south')",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_all_direct": [
        ("select * from sensor where voltage > all "
         "(select voltage from sensor where region = 'south')",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["ALL"], "pg": ["ALL"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],
    "subq_some_direct": [
        ("select * from sensor where voltage > some "
         "(select voltage from sensor where region = 'south')",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]})),
    ],

    # ====================================================================
    # §5.3.8 Other clauses
    # ====================================================================

    # §5.3.8.1 DISTINCT — all 3 sources push down
    "distinct": [
        ("select distinct region from {SRC}.sensor",
         dict(remote_has=["DISTINCT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.2 GROUP BY / HAVING — all 3 sources push down
    "group_by": [
        ("select region, count(*) from {SRC}.sensor group by region",
         dict(remote_has=["GROUP BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "having": [
        ("select region, count(*) as cnt from {SRC}.sensor "
         "group by region having count(*) > 1",
         dict(remote_has=["GROUP BY", "HAVING"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.3 ORDER BY — all 3 sources push down
    "order_by": [
        ("select `time`, voltage from {SRC}.sensor order by voltage desc",
         dict(remote_has=["ORDER BY"], local_not_has=["Sort "])),
    ],
    # §5.3.8.3 NULLS FIRST/LAST — PG/Influx native; MySQL converts to ISNULL().
    "order_by_nulls_last": [
        ("select `time`, voltage from {SRC}.sensor "
         "order by voltage desc nulls last",
         dict(checks_add=["remote_sql"],
              remote_has={"pg": ["ORDER BY", "NULLS LAST"],
                          "influx": ["ORDER BY", "NULLS LAST"],
                          "mysql": ["ORDER BY", "ISNULL"]},
              remote_not_has={"mysql": ["NULLS LAST"]},
              local_not_has=["Sort "])),
    ],
    "order_by_nulls_first": [
        # MySQL has no NULLS FIRST/LAST; planner must convert to ISNULL(...)
        ("select `time`, voltage from {SRC}.sensor "
         "order by voltage asc nulls first",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ORDER BY", "ISNULL"],
                          "pg": ["ORDER BY", "NULLS FIRST"],
                          "influx": ["ORDER BY", "NULLS FIRST"]},
              remote_not_has={"mysql": ["NULLS FIRST", "NULLS LAST"]},
              local_not_has=["Sort "])),
    ],

    # §5.3.8.4 LIMIT / OFFSET — push down when no PARTITION BY
    "limit_only": [
        ("select `time`, voltage from {SRC}.sensor limit 3",
         dict(remote_has=["LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "limit_offset": [
        ("select `time`, voltage from {SRC}.sensor "
         "order by `time` limit 2 offset 1",
         dict(remote_has=["ORDER BY", "LIMIT", "OFFSET"],
              local_not_has=["Sort "])),
    ],
    # §5.3.8.4 LIMIT with PARTITION BY — per-partition semantics, must NOT push
    "limit_partition_local": [
        ("select `time`, voltage from {SRC}.sensor "
         "partition by region limit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["LIMIT"],
              local_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.5 SLIMIT / SOFFSET — TDengine-specific, never push
    "slimit_local": [
        ("select `time`, voltage from {SRC}.sensor "
         "partition by region slimit 1",
         dict(checks_add=["remote_sql"], remote_not_has=["SLIMIT"])),
    ],

    # §5.3.8.6 UNION ALL same-source — push as single SQL
    "union_all": [
        ("select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select `time`, voltage from {SRC}.sensor where voltage < 220.0",
         dict(checks_add=["remote_sql"], remote_has=["UNION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source + LIMIT on the UNION statement itself.
    # This LIMIT belongs to the set-op statement, not to an outer derived-table
    # query.  Remote SQL should therefore keep LIMIT in the UNION statement
    # itself instead of wrapping the UNION in an extra outer SELECT solely to
    # host the LIMIT.
    "union_all_stmt_limit": [
        ("select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select `time`, voltage from {SRC}.sensor where voltage < 220.0 "
         "limit 10",
         dict(checks_add=["remote_sql", "remote_any_not_has"],
              remote_has=["UNION", "LIMIT"],
              remote_any_not_has=["_sq LIMIT 10"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source + LIMIT on the outer query.
    # This LIMIT belongs to the outer derived-table query, so remote SQL should
    # preserve the extra outer SELECT ... FROM (<union>) _sq LIMIT ...
    "union_all_outer_limit": [
        ("select * from "
         "(select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select `time`, voltage from {SRC}.sensor where voltage < 220.0) t "
         "limit 10",
         dict(checks_add=["remote_sql", "remote_any_has"],
              remote_has=["UNION", "LIMIT"],
              remote_any_has=["_sq LIMIT 10"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source + ORDER BY on outer result
    # The UNION ALL branches and the outer ORDER BY/LIMIT are pushed together.
    # Remote SQL therefore keeps the UNION inside a derived-table wrapper and
    # attaches ORDER BY/LIMIT on the outer statement.
    "union_all_outer_order_limit": [
        ("select `time`, voltage from "
         "(select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select `time`, voltage from {SRC}.sensor where voltage < 220.0) t "
         "order by t.`time` limit 10",
         dict(checks_add=["remote_sql", "remote_any_has"],
              remote_has=["UNION", "ORDER BY", "LIMIT"],
              remote_any_has=["_sq ORDER BY", "LIMIT 10"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source + GROUP BY on both branches.
    # Both branches are fully pushable single-source aggregations, so by the
    # "push as much as possible" rule the whole UNION ALL is rendered as one
    # remote statement and each branch keeps its own GROUP BY remotely.
    "union_all_both_groupby": [
        ("select region, count(*) from {SRC}.sensor where voltage > 221.0 "
         "group by region "
         "union all "
         "select region, count(*) from {SRC}.sensor where voltage < 220.0 "
         "group by region",
         dict(checks_add=["remote_sql"], remote_has=["UNION", "GROUP BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source — one branch is an INTERVAL aggregation.
    # After the interval remote-plan build was aligned with the generic set-op
    # whole-push path, the planner can push the whole UNION ALL as one remote
    # statement, with the interval branch rendered as a remote subquery.
    "union_all_one_branch_local": [
        ("select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' interval(1m)",
         dict(checks_add=["remote_sql"], remote_has=["UNION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL same-source — both branches are INTERVAL aggregations.
    # The planner now preserves both branch remote subqueries under one
    # same-source UNION ALL remote root, so the whole set-op is pushable.
    "union_all_both_local": [
        ("select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-01 01:00:00' interval(1m) "
         "union all "
         "select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-02' and `time` <= '2024-01-02 01:00:00' interval(1m)",
         dict(checks_add=["remote_sql"], remote_has=["UNION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION ALL cross-source — each branch pushed independently;
    # the merge happens locally so each leg becomes its own Federated Scan
    # and no single Remote SQL contains UNION.
    "union_all_cross": [
        ("select `time`, voltage from {SRC}.sensor "
         "union all "
         "select `time`, voltage from {SRC2}.sensor",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics", "remote_not_has"],
              min_scans=2,
              remote_not_has=["UNION"])),
    ],

    # §5.3.8.6 UNION (dedup) same-source — DS requires the entire UNION to be pushed
    # to the remote source so deduplication happens there.  All supported
    # external sources keep the dedup UNION remote when both branches are
    # otherwise fully pushable.
    "union_dedup": [
        ("select region from {SRC}.sensor where voltage > 221.0 "
         "union "
         "select region from {SRC}.sensor where voltage < 220.0",
         dict(checks_add=["remote_sql"], remote_has=["UNION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION (dedup) same-source + GROUP BY on both branches.
    # When both branches are fully pushable aggregations, the whole dedup UNION
    # stays remote and each branch keeps its GROUP BY remotely.
    "union_dedup_groupby": [
        ("select region, count(*) as cnt from {SRC}.sensor where voltage > 221.0 "
         "group by region "
         "union "
         "select region, count(*) as cnt from {SRC}.sensor where voltage < 220.0 "
         "group by region",
         dict(checks_add=["remote_sql"], remote_has=["UNION", "GROUP BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION (dedup) same-source — one branch is an INTERVAL aggregate.
    # The branch is still remote-expressible on all 3 sources, so the whole
    # dedup UNION stays remote and preserves the aggregate branch shape.
    "union_dedup_one_branch_local": [
        ("select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union "
         "select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' interval(1m)",
         dict(checks_add=["remote_sql"], remote_has=["UNION", "GROUP BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.6 UNION (dedup) cross-source — local dedup after fetching each side
    "union_dedup_cross": [
        ("select region from {SRC}.sensor "
         "union "
         "select region from {SRC2}.sensor",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics", "remote_not_has"],
              min_scans=2,
              remote_not_has=["UNION"])),
    ],

    # §5.3.8.7 CASE expression — all 3 sources push down
    "case_expr": [
        ("select case when voltage > 220.0 then 'high' else 'low' end "
         "from {SRC}.sensor",
         dict(remote_has={"mysql": ["IF("],
                          "pg": ["CASE", "WHEN"],
                          "influx": ["CASE", "WHEN"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # §5.3.8.8 FILL — FILL itself stays local, but INTERVAL aggregation can
    # push to the remote source and feed the local fill node.
    "fill_local": [
        ("select _wstart, avg(voltage) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m) fill(value, 0)",
         dict(checks_add=["remote_sql"],
              remote_has=["AVG", "GROUP BY"],
              remote_not_has=["FILL", "INTERVAL("],
              local_has=["Fill"],
              local_not_has=["Interval"])),
    ],

    # §5.3.8.9 INTERP — TDengine-specific, never push
    "interp_local": [
        ("select interp(voltage) from {SRC}.sensor "
         "range('2024-01-01 00:00:00', '2024-01-01 00:05:00') "
         "every(1m) fill(linear)",
         dict(checks_add=["remote_sql"], remote_not_has=["INTERP"])),
    ],

    # ====================================================================
    # Structural / baseline groups (independent of DS section numbering)
    # ====================================================================

    "basic": [
        ("select * from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select `time`, voltage from {SRC}.sensor where `time` > '2024-01-01'",
         dict(checks_add=["remote_sql"], remote_has=["WHERE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select * from {SRC}.sensor",
         dict(checks_add=["federated_scan_on"])),
    ],

    "verbose": [
        ("select `time`, voltage from {SRC}.sensor",
         dict(checks_add=["remote_sql", "columns_list", "projection_no_star",
                          "output_cols_eq"],
              output_cols_eq=2,
              remote_has={"mysql": ["`time`", "`voltage`"],
                          "pg": ["\"time\"", "\"voltage\""],
                          "influx": ["\"time\"", "\"voltage\""]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select `time`, voltage from {SRC}.sensor "
         "where `time` > '2024-01-01' order by `time` limit 10",
         dict(checks_add=["pushdown_flags"])),
        ("select `time`, voltage from {SRC}.sensor",
         dict(checks_add=["columns_list"])),
    ],

    "dialect": [
        # Same statement on all sources; quoting check self-gates by dialect
        # (backtick on MySQL, double-quote on PG/InfluxDB, [brackets] on
        # SQL Server).
        ("select `time`, voltage from {SRC}.sensor",
         dict(checks=["federated_scan", "dialect_backtick",
                      "dialect_double_quote", "dialect_bracket"])),
    ],

    "typemap": [
        ("select `time`, voltage from {SRC}.sensor",
         dict(checks_add=["remote_sql", "columns_list", "projection_no_star",
                          "output_cols_eq"],
              output_cols_eq=2,
              remote_has={"mysql": ["`time`", "`voltage`"],
                          "pg": ["\"time\"", "\"voltage\""],
                          "influx": ["\"time\"", "\"voltage\""]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select * from {SRC}.sensor",
         dict(checks_add=["remote_sql", "columns_list", "projection_no_star",
                          "output_cols_eq"],
              output_cols_eq=8,
              remote_has={"mysql": ["`time`", "`voltage`", "`current`",
                                    "`region`", "`date_str`", "`json_str`",
                                    "`b64_str`", "`geom_wkt`"],
                          "pg": ["\"time\"", "\"voltage\"", "\"current\"",
                                 "\"region\"", "\"date_str\"", "\"json_str\"",
                                 "\"b64_str\"", "\"geom_wkt\""],
                          "influx": ["\"time\"", "\"voltage\"", "\"current\"",
                                     "\"region\"", "\"date_str\"", "\"json_str\"",
                                     "\"b64_str\"", "\"geom_wkt\""]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    "security": [
        ("select * from {SRC}.sensor",
         dict(no_data_leak=["220.5", "221.0", "219.8", "222.0", "220.0"])),
    ],

    "projection": [
        ("select `time`, voltage from {SRC}.sensor",
         dict(checks_add=["projection_no_star"])),
        ("select * from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=["Sort ", "Agg"])),
    ],

    "compound_where": [
        ("select `time`, voltage from {SRC}.sensor "
         "where (voltage > 220.0 and region = 'north') or current < 1.2",
         dict(remote_has=["WHERE"], checks_add=["compound_where"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    "full_pushdown": [
        ("select `time`, voltage from {SRC}.sensor "
         "where voltage > 220.0 order by `time` limit 3",
         dict(remote_has=["WHERE", "ORDER BY", "LIMIT"],
              local_not_has=["Sort "])),
        ("select `time`, voltage from {SRC}.sensor "
         "where `time` >= '2024-01-01' order by `time` limit 3",
         dict(remote_has=["WHERE", "ORDER BY", "LIMIT"],
              local_not_has=["Sort "])),
    ],

    "where_only": [
        ("select `time`, voltage from {SRC}.sensor where voltage > 220.0",
         dict(remote_has=["WHERE"], remote_not_has=["LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    "order_only": [
        ("select `time`, voltage from {SRC}.sensor order by voltage desc",
         dict(remote_has=["ORDER BY"], remote_not_has=["LIMIT"],
              local_not_has=["Sort "])),
    ],

    # WHERE + ORDER BY without LIMIT — LIMIT must NOT be invented
    "where_order_no_limit": [
        ("select `time`, voltage from {SRC}.sensor "
         "where voltage > 219.0 order by `time`",
         dict(remote_has=["WHERE", "ORDER BY"],
              remote_not_has=["LIMIT"], local_not_has=["Sort "])),
    ],

    # ====================================================================
    # CARTESIAN CROSS-DIMENSION COMBOS
    # --------------------------------------------------------------------
    # Naming:  combo_<dim1><dim2>_<state1><state2>_<note>
    #   dimensions: F=Function  O=Operator  W=Window  C=Clause  S=Subquery
    #   states:     P=Pushdown  L=Local
    # Each combo asserts BOTH halves simultaneously: the pushdown half via
    # remote_has, the local half via remote_not_has — strict mode guarantees
    # that the planner emits the supported part to the source AND keeps the
    # TDengine-specific part local in the SAME query plan.
    # ====================================================================

    # ---------- F × F : Function × Function ----------
    "combo_FF_PP_select": [
        # Two pushdown functions in same SELECT
        ("select abs(voltage), upper(region) from {SRC}.sensor",
         dict(remote_has=["ABS", "UPPER"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FF_PL_select": [
        # Pushdown ABS + TDengine-only CSUM in same SELECT
        ("select abs(voltage - 220.0), csum(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["CSUM"])),
    ],
    "combo_FF_LL_select": [
        # Two TDengine-only functions — TDengine rejects this combination
        # ([0x264f] Multiple indefinite rows functions) before reaching FQ.
        ("select csum(voltage), diff(voltage) from {SRC}.sensor",
         dict(expect_err=True)),
        # Two TDengine-only scalar functions stay local but should still
        # execute successfully through FQ.
        ("select MASK_FULL(region, '*'), to_iso8601(`time`) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["MASK_FULL", "TO_ISO8601"],
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select MASK_PARTIAL(region, 1, 1, '#'), MASK_NONE(region) from "
         "{SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["MASK_PARTIAL", "MASK_NONE"],
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
    ],
    "combo_FF_PP_nested": [
        # Nested pushdown funcs: round(abs(...))
        ("select round(abs(voltage - 220.0), 2) from {SRC}.sensor",
         dict(remote_has=["ROUND", "ABS"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FF_PP_agg_scalar": [
        # Pushdown agg over pushdown scalar
        ("select avg(abs(voltage - 220.0)) from {SRC}.sensor",
         dict(remote_has=["AVG", "ABS"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FF_PL_agg_local_inside": [
        # Aggregate containing TDengine-only func: TDengine rejects this
        # ([0x2627] Aggregate functions do not support nesting).
        ("select avg(twa(voltage)) from {SRC}.sensor",
         dict(expect_err=True)),
        # DS §5.3.10.3.4 requires AGG to stay local when its argument contains
        # a non-mappable local scalar such as TO_ISO8601.
        ("select count(to_iso8601(`time`)) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["COUNT", "TO_ISO8601"],
              local_has={"mysql": ["Aggregate"],
                         "pg": ["Aggregate"],
                         "influx": ["Aggregate"]})),
    ],

    # ---------- F × O : Function × Operator ----------
    "combo_FO_PP_func_select_op_where": [
        ("select abs(voltage - 220.0) from {SRC}.sensor "
         "where voltage between 219.0 and 222.0",
         dict(remote_has=["ABS", "WHERE", "BETWEEN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FO_PP_func_in_where": [
        # Pushdown function used inside WHERE expression
        ("select `time`, voltage from {SRC}.sensor "
         "where abs(voltage - 220.0) < 2.0",
         dict(remote_has=["WHERE", "ABS", "<"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FO_LP_localfunc_select_op_where": [
        # TDengine-only func in SELECT + pushdown WHERE
        ("select csum(voltage) from {SRC}.sensor where voltage > 220.0",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", ">"], remote_not_has=["CSUM"])),
    ],
    "combo_FO_PL_pushfunc_op_local_bitwise_influx": [
        # InfluxDB has no bitwise &, so the WHERE stays local there.  Once the
        # scan WHERE is local, we do not try to keep pushing SELECT project
        # expressions remotely; the remote scan should fetch only raw columns
        # required by the local filter/project path.
        ("select abs(voltage) from {SRC}.sensor "
         "where (cast(voltage as bigint) & 1) = 1",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ABS", "&"], "pg": ["ABS", "&"]},
              remote_not_has={"influx": ["ABS", " & "]},
              local_has={"influx": ["Projection"]})),
    ],
    "combo_FO_LL_localfunc_op_unsupported": [
        # Mixed-source coverage: CSUM is local on all 3 sources, while the
        # bitwise filter still pushes on MySQL/PG but stays local on InfluxDB.
        # Keep this statement to cover that split behavior explicitly.
        ("select csum(voltage) from {SRC}.sensor "
         "where (cast(voltage as bigint) | 1) > 0",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["CSUM"], "pg": ["CSUM"],
                              "influx": ["CSUM"]})),
    ],

    # ---------- F × W : Function × Window ----------
    "combo_FW_PP_pushfunc_intervalwin": [
        # AVG over INTERVAL window — converts to remote GROUP BY
        ("select _wstart, avg(voltage) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m)",
         dict(remote_has=["AVG", "GROUP BY"],
              remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FW_LP_localfunc_intervalwin": [
        # CSUM in SELECT + INTERVAL — both the func and the literal
        # INTERVAL keyword must NOT appear in Remote SQL
        ("select csum(voltage) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["CSUM", "INTERVAL("])),
    ],
    "combo_FW_PL_pushfunc_statewin": [
        # COUNT(*) + STATE_WINDOW: window stays local; STATE_WINDOW
        # keyword must never appear in Remote SQL
        ("select count(*), region from {SRC}.sensor state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["STATE_WINDOW"])),
    ],
    "combo_FW_LL_localfunc_localwin": [
        ("select csum(voltage) from {SRC}.sensor state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["CSUM", "STATE_WINDOW"])),
    ],

    # ---------- F × C : Function × Clause ----------
    "combo_FC_PP_pushfunc_full_clauses": [
        # ABS + WHERE + ORDER BY + LIMIT
        ("select abs(voltage - 220.0) av from {SRC}.sensor "
         "where voltage > 219.0 order by `time` limit 3",
         dict(remote_has=["ABS", "WHERE", "ORDER BY", "LIMIT"],
              local_not_has=["Sort "])),
    ],
    "combo_FC_PP_agg_groupby_having": [
        # SUM/AVG + GROUP BY + HAVING all push
        ("select region, sum(voltage), avg(voltage) cnt "
         "from {SRC}.sensor group by region having sum(voltage) > 100",
         dict(remote_has=["SUM", "AVG", "GROUP BY", "HAVING"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FC_PP_distinct_pushfunc": [
        ("select distinct upper(region) from {SRC}.sensor",
         dict(remote_has=["DISTINCT", "UPPER"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_FC_LP_localfunc_clauses": [
        # CSUM + WHERE + ORDER BY: WHERE pushes (constant filter), CSUM local
        ("select csum(voltage) from {SRC}.sensor "
         "where voltage > 220.0 order by `time`",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE"], remote_not_has=["CSUM"])),
    ],
    "combo_FC_LL_localfunc_localclause": [
        # TWA + SLIMIT + PARTITION BY — none push
        ("select twa(voltage) from {SRC}.sensor "
         "partition by region slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TWA", "SLIMIT", "PARTITION"])),
    ],
    "combo_FC_PL_case_with_localfunc": [
        # DS §5.3.8.7: CASE containing TDengine-only func — entire CASE
        # is non-pushdown
        ("select case when twa(voltage) > 220.0 then 'high' else 'low' end "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["CASE", "TWA"])),
    ],
    "combo_FC_PP_case_with_pushfunc": [
        # CASE with pushdown function inside — whole expression pushes
        ("select case when abs(voltage - 220.0) < 1.0 then 'normal' "
         "else 'abnormal' end from {SRC}.sensor",
         dict(remote_has={"mysql": ["IF(", "ABS"],
                          "pg": ["CASE", "ABS"],
                          "influx": ["CASE", "ABS"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # ---------- O × W : Operator × Window ----------
    "combo_OW_PP_op_intervalwin": [
        # WHERE + INTERVAL — both pushed (interval converts to GROUP BY)
        ("select _wstart, avg(voltage) from {SRC}.sensor "
         "where region = 'north' and `time` >= '2024-01-01' "
         "and `time` <= '2024-01-02' interval(1m)",
         dict(remote_has=["WHERE", "GROUP BY", "AVG"],
              remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_OW_PL_op_statewin": [
        # WHERE pushes; STATE_WINDOW stays local
        ("select count(*), region from {SRC}.sensor "
         "where voltage > 219.0 state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE"], remote_not_has=["STATE_WINDOW"])),
    ],
    "combo_OW_LL_op_local_win_local_influx": [
        # Mixed-source coverage: STATE_WINDOW is local on all 3 sources, while
        # the bitwise filter is local only on InfluxDB and remains pushable on
        # MySQL/PG.
        ("select count(*) from {SRC}.sensor "
         "where (cast(voltage as bigint) & 1) = 1 state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["STATE_WINDOW"], "pg": ["STATE_WINDOW"],
                              "influx": [" & ", "STATE_WINDOW"]})),
    ],

    # ---------- O × C : Operator × Clause ----------
    "combo_OC_PP_in_orderby_limit": [
        ("select * from {SRC}.sensor where voltage in (220.0, 221.0) "
         "order by `time` limit 5",
         dict(remote_has=["WHERE", "IN", "ORDER BY", "LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_OC_PP_compound_groupby": [
        ("select region, count(*) from {SRC}.sensor "
         "where voltage > 219.0 and region <> 'east' group by region",
         dict(remote_has=["WHERE", "AND", "GROUP BY"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_OC_PP_like_distinct": [
        ("select distinct region from {SRC}.sensor where region like 'no%'",
         dict(remote_has=["DISTINCT", "WHERE", "LIKE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_OC_PL_op_partition_limit": [
        # WHERE pushes; LIMIT stays local because PARTITION BY
        ("select `time`, voltage from {SRC}.sensor "
         "where voltage > 219.0 partition by region limit 1",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE"], remote_not_has=["LIMIT"],
              local_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],

    # ---------- W × C : Window × Clause ----------
    "combo_WC_PP_interval_orderby_limit": [
        # INTERVAL converts + ORDER BY/LIMIT push
        ("select _wstart, avg(voltage) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m) order by _wstart limit 5",
         dict(remote_has=["GROUP BY"], remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_WC_PL_interval_fill": [
        # FILL still executes locally, but the windowed AVG is now pushed as a
        # remote grouped scan. The local plan should retain only the fill step.
        ("select _wstart, avg(voltage) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m) fill(value, 0)",
        dict(checks_add=["remote_sql"],
             remote_has=["AVG", "GROUP BY"],
             remote_not_has=["FILL", "INTERVAL("],
             local_has=["Fill"],
             local_not_has=["Interval"])),
    ],
    "combo_WC_PL_extwin_fill": [
        # The EXTERNAL_WINDOW boundary subquery uses local INTERVAL+FILL to
        # materialise empty windows, while the outer EXTERNAL_WINDOW FILL
        # drives emission/fill of aggregate outputs. Remote SQL must remain a
        # pair of raw ordered source scans without window/fill/agg leakage.
        ("select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' "
         "interval(30s) fill(null)) w) fill(null) order by _wstart",
         dict(checks_add=["remote_sql", "remote_any_not_has", "local_has", "min_federated_scans"],
              remote_any_not_has=["GROUP BY", "COUNT(", "SUM(", "FILL", "INTERVAL(", "EXTERNAL_WINDOW"],
              local_has=["Sort", "External", "Fill", "Interval"],
              min_scans=2)),
        ("select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' "
         "interval(30s) fill(null)) w) fill(value, 0, 0) order by _wstart",
         dict(checks_add=["remote_sql", "remote_any_not_has", "local_has", "min_federated_scans"],
              remote_any_not_has=["GROUP BY", "COUNT(", "SUM(", "FILL", "INTERVAL(", "EXTERNAL_WINDOW"],
              local_has=["Sort", "External", "Fill", "Interval"],
              min_scans=2)),
        ("select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' "
         "interval(30s) fill(null)) w) fill(prev) order by _wstart",
         dict(checks_add=["remote_sql", "remote_any_not_has", "local_has", "min_federated_scans"],
              remote_any_not_has=["GROUP BY", "COUNT(", "SUM(", "FILL", "INTERVAL(", "EXTERNAL_WINDOW"],
              local_has=["Sort", "External", "Fill", "Interval"],
              min_scans=2)),
    ],
    "combo_WC_LP_statewin_orderby": [
        # STATE_WINDOW local; ORDER BY only on local result
        ("select count(*), region from {SRC}.sensor "
         "state_window(region) order by region",
        dict(checks_add=["remote_sql"],
              remote_not_has=["STATE_WINDOW"])),
    ],
    "combo_WC_LL_localwin_slimit": [
        ("select count(*) from {SRC}.sensor "
         "partition by region session(`time`, 1m) slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SESSION(", "SLIMIT", "PARTITION"])),
    ],

    # ---------- C × C : Clause × Clause ----------
    "combo_CC_PP_distinct_orderby_limit": [
        ("select distinct region from {SRC}.sensor "
         "order by region limit 3",
         dict(remote_has=["DISTINCT", "ORDER BY", "LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_CC_PP_groupby_having_orderby_limit": [
        ("select region, count(*) cnt from {SRC}.sensor "
         "group by region having count(*) > 0 "
         "order by cnt desc limit 2",
         dict(remote_has=["GROUP BY", "HAVING", "ORDER BY", "LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_CC_PP_union_orderby": [
        # Same-source UNION ALL with outer ORDER BY — single remote SQL
        ("select * from ("
         "select `time`, voltage from {SRC}.sensor where voltage > 221.0 "
         "union all "
         "select `time`, voltage from {SRC}.sensor where voltage < 220.0"
         ") t order by voltage",
         dict(checks_add=["remote_sql"],
              remote_has=["UNION"],
              local_has=["Sort "])),
    ],
    "combo_CC_LL_slimit_partition_limit": [
        # SLIMIT + LIMIT under PARTITION BY — both stay local
        ("select * from {SRC}.sensor "
         "partition by region slimit 1 limit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SLIMIT", "LIMIT"])),
    ],
    "combo_CC_PL_groupby_fill": [
        # PARTITION BY + INTERVAL aggregation is pushed to the remote source,
        # while the outer fill remains local on the grouped rows.
        ("select _wstart, region, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "partition by region interval(1m) fill(null)",
         dict(checks_add=["remote_sql"],
              remote_has=["COUNT(", "GROUP BY"],
              remote_not_has=["FILL", "INTERVAL("],
              local_has=["Fill"],
              local_not_has=["Interval", "Partition"])),
    ],

    # ---------- S × X : Subquery cross-cuts ----------
    "combo_S_PP_outer_pushP_inner_pushP": [
        # Inner & outer both push — entire query foldable
        ("select * from ("
         "select abs(voltage - 220.0) av from {SRC}.sensor "
         "where voltage > 219.0) t order by t.av limit 3",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_has=["ABS", "WHERE"],
              local_has=["Projection", "Sort "])),
    ],
    "combo_S_LP_outer_pushP_inner_localL": [
        # Inner has CSUM (local); outer cannot fold over local result remotely
        ("select * from (select csum(voltage) v from {SRC}.sensor) t "
         "order by t.v limit 3",
         dict(checks_add=["remote_sql"],
              remote_not_has=["CSUM"],
              local_has=["Indefinite Rows Function"])),
    ],
    "combo_S_LP_scalar_inner_localL": [
        # Scalar subquery uses TDengine func — inner runs local, outer
        # gets a constant; TWA must NOT appear in Remote SQL
        ("select `time`, voltage from {SRC}.sensor "
         "where voltage > (select twa(voltage) from {SRC}.sensor)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TWA"],
              local_has=["InitPlan 1"])),
    ],
    "combo_S_PL_in_subq_localfunc_inner": [
        # IN-subquery with a TDengine-only function (twa) inside the inner
        # SELECT.  twa cannot be pushed to any dialect, so it must never appear
        # in Remote SQL on any source.  Runs on all 3 sources.
        ("select * from {SRC}.sensor where voltage in "
         "(select twa(voltage) from {SRC}.sensor)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TWA"],
              local_has=["InitPlan 1"])),
    ],

    # ---------- 3-way combos ----------
    "combo_3W_FOC_pushP": [
        # Func + Op + Clause all push together
        ("select abs(voltage - 220.0) av from {SRC}.sensor "
         "where voltage between 219.0 and 222.0 "
         "order by av limit 3",
         dict(remote_has=["ABS", "BETWEEN", "ORDER BY", "LIMIT"],
              local_not_has=["Sort "])),
    ],
    "combo_3W_FWC_pushP": [
        # Func + Window + Clause: AVG(ABS) over INTERVAL with ORDER BY/LIMIT
        ("select _wstart, avg(abs(voltage - 220.0)) av from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m) order by _wstart limit 5",
         dict(remote_has=["AVG", "ABS", "GROUP BY"],
              remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_3W_FWC_LLP": [
        # Local func + Local window + Pushdown clause:
        # WHERE pushes; CSUM and STATE_WINDOW stay local
        ("select csum(voltage) from {SRC}.sensor "
         "where voltage > 219.0 state_window(region) order by voltage",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE"],
              remote_not_has=["CSUM", "STATE_WINDOW"])),
    ],
    "combo_3W_FOC_LPP": [
        # Local func + Pushdown op + Pushdown clause
        ("select diff(voltage) from {SRC}.sensor "
         "where region = 'north' and voltage > 219.0 order by `time`",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "AND"],
              remote_not_has=["DIFF"])),
    ],
    "combo_3W_FOC_LLP": [
        # Mixed-source coverage: CSUM stays local on all 3 sources; the
        # bitwise predicate stays local only on InfluxDB; ORDER BY/LIMIT remain
        # the pushdown clause dimension for the remote-capable sources.
        ("select csum(voltage) from {SRC}.sensor "
         "where (cast(voltage as bigint) & 1) = 1 "
         "order by `time` limit 3",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["CSUM"], "pg": ["CSUM"],
                              "influx": ["CSUM", " & "]})),
    ],
    "combo_3W_FWC_LLL": [
        # Triple-local: TWA + SESSION + SLIMIT
        ("select twa(voltage) from {SRC}.sensor "
         "partition by region session(`time`, 1m) slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TWA", "SESSION(", "SLIMIT", "PARTITION"])),
    ],

    # ---------- 4-way mega-combo (smoke) ----------
    "combo_4W_FOWC_PPPP": [
        # When the whole statement is remotely expressible, PARTITION BY +
        # INTERVAL should remain in the same remote grouped query after rewrite.
        ("select _wstart, region, avg(abs(voltage - 220.0)) av "
         "from {SRC}.sensor "
         "where region in ('north', 'south') "
         "and `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "partition by region interval(1m) "
         "order by _wstart limit 10",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "GROUP BY", "ORDER BY", "LIMIT"],
              remote_not_has=["PARTITION", "INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
    ],
    "combo_4W_FOWC_LLLL": [
        # Mixed-source coverage: TWA / SESSION / SLIMIT / PARTITION are local
        # on all 3 sources; the bitwise predicate is additionally local only
        # on InfluxDB.
        ("select twa(voltage) from {SRC}.sensor "
         "where (cast(voltage as bigint) & 1) = 1 "
         "partition by region session(`time`, 1m) slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["TWA", "SESSION(", "SLIMIT", "PARTITION"],
                              "pg": ["TWA", "SESSION(", "SLIMIT", "PARTITION"],
                              "influx": ["TWA", " & ", "SESSION(", "SLIMIT",
                                         "PARTITION"]})),
    ],

    # ---------- Cross-source × X ----------
    "combo_XS_join_pushfunc": [
        # Cross-source JOIN with pushdown func on each leg
        ("select abs(m.voltage - 220.0), p.current "
         "from {SRC}.sensor m join {SRC2}.sensor p "
         "on m.`time` = p.`time` and m.region = p.region",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics"],
              min_scans=2)),
    ],
    "combo_XS_join_localfunc": [
        # Cross-source JOIN with TDengine-only func — neither leg may
        # contain CSUM/TWA in its Remote SQL
        ("select csum(m.voltage), p.current "
         "from {SRC}.sensor m join {SRC2}.sensor p "
         "on m.`time` = p.`time` and m.region = p.region",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics", "remote_not_has"],
              min_scans=2,
              remote_not_has=["CSUM"])),
    ],
    "combo_XS_join_with_where": [
        # Cross-source JOIN — each leg pushes its own WHERE independently
        ("select m.voltage, p.current "
         "from {SRC}.sensor m join {SRC2}.sensor p "
         "on m.`time` = p.`time` and m.region = p.region "
         "where m.voltage > 219.0 and p.current > 1.0",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics", "remote_has"],
              min_scans=2,
              remote_has=["WHERE"])),
    ],
}


# =========================================================================
# DS §5.3 programmatic generators
# -------------------------------------------------------------------------
# Below tables enumerate EVERY function / operator / window / clause / JOIN
# listed in DS §5.3.3-§5.3.8.  For each entry we generate:
#
#   - one PUSH case per source where DS marks the construct as
#     "直接下推" or "转换下推", asserting remote_has=[expected_keyword].
#   - one LOCAL case per source where DS marks "本地计算",
#     asserting remote_not_has=[forbidden_keyword].
#
# Each generated group key has prefix ``ds_`` and is merged into the
# hand-written ``_EXPLAIN_GROUPS`` above via update().  Hand-written
# groups continue to provide rich combo and structural coverage; these
# generators provide the breadth across DS catalogue.
# =========================================================================


def _ds_emit(group_dict):
    """Emit DS-driven function/operator/window/clause cases by source."""

    # -------------------------------------------------------------
    # §5.3.4.1.1 Math functions — default PUSH on all 3 sources
    # -------------------------------------------------------------
    # (td_func, sql_args)  — both the function name and the keyword
    #                        expected in Remote SQL are the same.
    # NOTE: args must reference a column so the expression is not constant-folded
    # at plan time (constant-only args are evaluated at planning, producing a
    # literal in Remote SQL instead of the function call).
    math_default = [
        ("ABS",     "(voltage - 220.0)"),
        # ACOS/ASIN domain is [-1, 1]; use SIN(voltage)/1000 to stay in range
        # and keep a column reference so the expression is NOT constant-folded.
        ("ACOS",    "(SIN(voltage) / 1000.0)"),
        ("ASIN",    "(SIN(voltage) / 1000.0)"),
        ("ATAN",    "(voltage / 1000.0)"),
        ("CEIL",    "(voltage)"),
        ("COS",     "(voltage)"),
        ("DEGREES", "(voltage)"),
        ("EXP",     "(1.0)"),
        ("FLOOR",   "(voltage)"),
        ("LN",      "(voltage)"),
        # PI is handled separately below (no-arg function, always constant-folded)
        ("POW",     "(voltage, 2)"),
        ("RADIANS", "(voltage)"),
        ("ROUND",   "(voltage, 2)"),
        ("SIGN",    "(voltage)"),
        ("SIN",     "(voltage)"),
        ("SQRT",    "(voltage)"),
        ("TAN",     "(voltage)"),
    ]
    for fn, args in math_default:
        # Pushed identically on all 3 sources — one group, no per-source split.
        group_dict[f"dsM_{fn.lower()}"] = [
            (f"select {fn}{args} from {{SRC}}.sensor",
             dict(remote_has=[fn],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))
        ]
    # PI() has no arguments and is always constant-folded to 3.14159...
    # Correct behaviour: the literal value is pushed, not the PI keyword.
    # Verify remote SELECT/FROM are still pushed; do not require PI() keyword.
    group_dict["dsM_pi"] = [
        ("select PI() from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))
    ]

    # §5.3.4.1.1 Math special-mapping
    # LOG: MySQL keeps LOG; PG/Influx convert to LN
    group_dict["dsM_log"] = [
        ("select log(voltage) from {SRC}.sensor",
         dict(remote_has={"mysql": ["LOG"], "pg": ["LN"], "influx": ["LN"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # TRUNCATE: MySQL keeps TRUNCATE; PG/Influx convert to TRUNC
    group_dict["dsM_truncate"] = [
        ("select truncate(voltage, 1) from {SRC}.sensor",
         dict(remote_has={"mysql": ["TRUNCATE"], "pg": ["TRUNC"],
                          "influx": ["TRUNC"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # MOD(): MySQL/PG push MOD; InfluxDB has no MOD() → local (% expression).
    group_dict["dsM_mod"] = [
        ("select mod(area, 2) from {SRC}.region_info",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["MOD"], "pg": ["MOD"], "influx": ["%"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # CORR: MySQL local (no native CORR); PG/Influx push CORR.
    group_dict["dsM_corr"] = [
        ("select corr(voltage, current) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"pg": ["CORR"], "influx": ["CORR"]},
              remote_not_has={"mysql": ["CORR"]}))]
    # GREATEST / LEAST — push on MySQL/PG.  InfluxDB 3.0's DataFusion only added
    # GREATEST (v42.0) / LEAST (v44.0); our InfluxDB rejects them ("Invalid
    # function"), so the capability matrix marks them supported and the runtime
    # function-level fallback (extClassifyRemoteError → FUNC demote) re-plans them
    # to local execution → remote SQL must NOT contain the function (analyze),
    # though the pre-fallback verbose plan still shows it pushed.
    for fn in ("GREATEST", "LEAST"):
        group_dict[f"dsM_{fn.lower()}"] = [
            (f"select {fn}(voltage, 220) from {{SRC}}.sensor",
             dict(remote_has={"mysql": [fn], "pg": [fn]},
                  verbose_remote_has={"influx": [fn]},
                  analyze_remote_not_has={"influx": [fn]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.2 String functions — default PUSH on all 3
    # -------------------------------------------------------------
    str_default = [
        ("ASCII",       "(region)"),
        ("CHAR_LENGTH", "(region)"),
        ("CONCAT",      "(region, '_x')"),
        ("CONCAT_WS",   "(',', region, region)"),
        ("LOWER",       "(region)"),
        ("LTRIM",       "(region)"),
        ("REPEAT",      "(region, 2)"),
        ("REPLACE",     "(region, 'n', 'x')"),
        ("RTRIM",       "(region)"),
        ("TRIM",        "(region)"),
        ("UPPER",       "(region)"),
    ]
    for fn, args in str_default:
        # Pushed identically on all 3 sources — one group, no per-source split.
        group_dict[f"dsS_{fn.lower()}"] = [
            (f"select {fn}{args} from {{SRC}}.sensor",
             dict(remote_has=[fn],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # CHAR: MySQL CHAR; PG/Influx CHR
    # char(65) is constant-folded to 'A' at plan time; use ASCII(region) as
    # argument to keep a column reference and force the function into Remote SQL.
    group_dict["dsS_char"] = [
        ("select char(ascii(region)) from {SRC}.sensor",
         dict(remote_has={"mysql": ["CHAR"], "pg": ["CHR"], "influx": ["CHR"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # LENGTH over multi-byte VARCHAR/TEXT follows the NCHAR branch.
    group_dict["dsS_length"] = [
        ("select length(region) from {SRC}.sensor",
         dict(remote_has={"mysql": ["CHAR_LENGTH", "* 4"],
                          "pg": ["CHAR_LENGTH", "* 4"],
                          "influx": ["CHAR_LENGTH", "* 4"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select length(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_LENGTH_SOURCES,
              remote_has={"mysql_ascii": ["LENGTH"],
                          "pg_ascii": ["OCTET_LENGTH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL])),
        ("select length(region_ascii) from {SRC}.region_info",
         dict(sources="custom",
              source_infos=_ASCII_LENGTH_SOURCES,
              remote_has={"mysql_ascii": ["LENGTH"],
                          "pg_ascii": ["OCTET_LENGTH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # POSITION: MySQL/PG keep; Influx STRPOS
    group_dict["dsS_position"] = [
        ("select position('n' in region) from {SRC}.sensor",
         dict(remote_has={"mysql": ["POSITION"], "pg": ["POSITION"],
                          "influx": ["STRPOS"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # SUBSTRING: MySQL/PG keep; Influx SUBSTR
    group_dict["dsS_substring"] = [
        ("select substring(region, 1, 2) from {SRC}.sensor",
         dict(remote_has={"mysql": ["SUBSTRING"], "pg": ["SUBSTRING"],
                          "influx": ["SUBSTR"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # SUBSTRING_INDEX: MySQL push; PG/Influx local
    group_dict["dsS_substring_index"] = [
        ("select substring_index(region, '_', 1) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["SUBSTRING_INDEX"]},
              remote_not_has={"pg": ["SUBSTRING_INDEX"],
                              "influx": ["SUBSTRING_INDEX"]}))]
    # FIND_IN_SET: MySQL push; PG/Influx local
    group_dict["dsS_find_in_set"] = [
        ("select find_in_set('north', region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["FIND_IN_SET"]},
              remote_not_has={"pg": ["FIND_IN_SET"], "influx": ["FIND_IN_SET"]}))]
    # LIKE_IN_SET / REGEXP_IN_SET — TDengine-only, all local
    for fn in ("LIKE_IN_SET", "REGEXP_IN_SET"):
        group_dict[f"dsS_{fn.lower()}_local_all"] = [
            (f"select {fn}(region, 'no%|so%') from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn]))]

    # -------------------------------------------------------------
    # §5.3.4.1.3 Encoding — NCHAR local; ASCII/VARCHAR can push on MySQL/PG
    # -------------------------------------------------------------
    group_dict["dsE_to_base64"] = [
        ("select to_base64(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["TO_BASE64", "ENCODE"],
                              "pg": ["TO_BASE64", "ENCODE"],
                              "influx": ["TO_BASE64", "ENCODE"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select to_base64(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["TO_BASE64"],
                          "pg_ascii": ["ENCODE"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsE_from_base64"] = [
        ("select from_base64(b64_str) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["FROM_BASE64", "DECODE"],
                              "pg": ["FROM_BASE64", "DECODE"],
                              "influx": ["FROM_BASE64", "DECODE"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select from_base64(b64_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["FROM_BASE64"],
                          "pg_ascii": ["DECODE"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # -------------------------------------------------------------
    # §5.3.4.1.4 Hash — NCHAR local; ASCII/VARCHAR follows dialect matrix
    # -------------------------------------------------------------
    group_dict["dsH_md5"] = [
        ("select md5(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["MD5"],
                              "pg": ["MD5"],
                              "influx": ["MD5"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select md5(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["MD5"],
                          "pg_ascii": ["MD5"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsH_sha1"] = [
        ("select sha1(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["SHA1"],
                              "pg": ["SHA1"],
                              "influx": ["SHA1"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select sha1(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["SHA1"]},
              remote_not_has={"pg_ascii": ["SHA1"]},
              local_has={"pg_ascii": ["Projection"]},
              local_not_has={"mysql_ascii": ["Projection"]}))]
    group_dict["dsH_sha2"] = [
        ("select sha2(region, 256) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["SHA2", "SHA256", "SHA512"],
                              "pg": ["SHA2", "SHA256", "SHA512"],
                              "influx": ["SHA2", "SHA256", "SHA512"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select sha2(region_ascii, 256) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["SHA2"]},
              remote_not_has={"pg_ascii": ["SHA2", "SHA256", "SHA512"]},
              local_has={"pg_ascii": ["Projection"]},
              local_not_has={"mysql_ascii": ["Projection"]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.5 Bitwise function CRC32 — NCHAR local; ASCII/VARCHAR MySQL push
    # -------------------------------------------------------------
    group_dict["dsBF_crc32"] = [
        ("select crc32(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["CRC32"],
                              "pg": ["CRC32"],
                              "influx": ["CRC32"]},
              local_has={"mysql": ["Projection"],
                         "pg": ["Projection"],
                         "influx": ["Projection"]})),
        ("select crc32(region_ascii) from {SRC}.sensor",
         dict(sources="custom",
              source_infos=_ASCII_VARCHAR_SOURCES,
              checks_add=["remote_sql"],
              remote_has={"mysql_ascii": ["CRC32"]},
              remote_not_has={"pg_ascii": ["CRC32"]},
              local_has={"pg_ascii": ["Projection"]},
              local_not_has={"mysql_ascii": ["Projection"]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.6 脱敏 — TDengine-only, all local
    # -------------------------------------------------------------
    for fn in ("MASK_FULL", "MASK_PARTIAL", "MASK_NONE"):
        group_dict[f"dsMask_{fn.lower()}_local"] = [
            (f"select {fn}(region) from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn]))]

    # -------------------------------------------------------------
    # §5.3.4.1.7 加密 — all local
    # -------------------------------------------------------------
    group_dict["dsCrypto_aes_encrypt_local"] = [
        ("select aes_encrypt(region, 'k') from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["AES_ENCRYPT"]))]
    group_dict["dsCrypto_aes_decrypt_local"] = [
        ("select aes_decrypt(region, 'k') from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["AES_DECRYPT"]))]
    group_dict["dsCrypto_sm4_encrypt_local"] = [
        ("select sm4_encrypt(region, 'k') from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["SM4_ENCRYPT"]))]
    group_dict["dsCrypto_sm4_decrypt_local"] = [
        ("select sm4_decrypt(region, 'k') from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["SM4_DECRYPT"]))]

    # -------------------------------------------------------------
    # §5.3.4.1.8 类型转换函数
    # -------------------------------------------------------------
    # CAST: all 3 push (target type translated)
    group_dict["dsT_cast"] = [
        ("select cast(voltage as varchar(32)) from {SRC}.sensor",
         dict(remote_has=["CAST"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # TO_CHAR: MySQL DATE_FORMAT; PG/Influx TO_CHAR
    group_dict["dsT_to_char"] = [
        ("select to_char(`time`, 'yyyy-mm-dd') from {SRC}.sensor",
         dict(remote_has={"mysql": ["DATE_FORMAT"], "pg": ["TO_CHAR"],
                          "influx": ["TO_CHAR"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # TO_TIMESTAMP: MySQL STR_TO_DATE; PG/Influx TO_TIMESTAMP
    # Both arguments are string literals → constant-folded at plan time.
    # Use the `region` column as the first argument so the function is NOT
    # constant-folded and appears in the Remote SQL.
    group_dict["dsT_to_timestamp"] = [
        ("select to_timestamp(date_str, 'yyyy-mm-dd') from {SRC}.sensor",
         dict(remote_has={"mysql": ["STR_TO_DATE"], "pg": ["TO_TIMESTAMP"],
                          "influx": ["TO_TIMESTAMP"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # TO_UNIXTIMESTAMP on sensor.time follows each source column's real type:
    #   MySQL DATETIME   -> CONVERT_TZ + TIMESTAMPDIFF
    #   PG TIMESTAMPTZ   -> direct EXTRACT EPOCH
    #   Influx time      -> direct EXTRACT EPOCH
    group_dict["dsT_to_unix"] = [
        ("select to_unixtimestamp(`time`) from {SRC}.sensor",
         dict(remote_has={"mysql": ["TIMESTAMPDIFF", "CONVERT_TZ"],
                          "pg": ["EXTRACT", "EPOCH"],
                          "influx": ["EXTRACT", "EPOCH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # TO_ISO8601: all local
    group_dict["dsT_to_iso8601_local"] = [
        ("select to_iso8601(`time`) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["TO_ISO8601"]))]
    # TO_JSON: current FQ capability matrix keeps it local for all dialects
    group_dict["dsT_to_json"] = [
        ("select to_json(json_str) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["TO_JSON"], "pg": ["TO_JSON"], "influx": ["TO_JSON"]},
              local_has={"mysql": ["Projection"], "pg": ["Projection"], "influx": ["Projection"]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.9 时间日期函数
    # -------------------------------------------------------------
    # now() is constant-folded at plan time and pushed as a timestamp literal —
    # NOT as the NOW() keyword. Verify remote SELECT/FROM are present.
    group_dict["dsTime_now_all"] = [
        ("select now() from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # today() is constant-folded at plan time and pushed as a date literal,
    # e.g. SELECT '<TODAY>' ... — NOT as CURDATE()/CURRENT_DATE keyword.
    # Verify remote SELECT/FROM are present; do not require function keyword.
    group_dict["dsTime_today_all"] = [
        ("select today() from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_has=["SELECT", "FROM"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_timezone_local"] = [
        ("select timezone() from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["TIMEZONE("]))]
    group_dict["dsTime_date"] = [
        ("select date(`time`) from {SRC}.sensor",
         dict(remote_has={"mysql": ["DATE"], "pg": ["DATE"],
                          "influx": ["CAST", "DATE"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_dayofweek"] = [
        ("select dayofweek(`time`) from {SRC}.sensor",
         dict(remote_has={"mysql": ["DAYOFWEEK"], "pg": ["EXTRACT", "DOW"],
                          "influx": ["EXTRACT", "DOW"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_weekday"] = [
        ("select weekday(`time`) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["WEEKDAY"], "pg": ["EXTRACT", "ISODOW"]},
              remote_not_has={"influx": ["WEEKDAY", "ISODOW"]}))]
    group_dict["dsTime_week_mysql"] = [
        ("select week(`time`, 0) from {SRC}.sensor",
         dict(remote_has={"mysql": ["WEEK"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_week_pg_influx"] = [
        ("select week(`time`) from {SRC}.sensor",
         dict(remote_has={"pg": ["EXTRACT", "WEEK"],
                          "influx": ["EXTRACT", "WEEK"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_weekofyear_mysql"] = [
        ("select weekofyear(`time`) from {SRC}.sensor",
         dict(remote_has={"mysql": ["WEEKOFYEAR"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_weekofyear_pg_influx"] = [
        ("select weekofyear(`time`) from {SRC}.sensor",
         dict(remote_has={"pg": ["EXTRACT", "WEEK"],
                          "influx": ["EXTRACT", "WEEK"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_timediff_mysql"] = [
        ("select timediff(`time`, `time`, 1s) from {SRC}.sensor",
         dict(remote_has={"mysql": ["TIMESTAMPDIFF"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_timediff_pg_influx"] = [
        ("select timediff(`time`, `time`) from {SRC}.sensor",
         dict(remote_has={"pg": ["EXTRACT", "EPOCH"],
                          "influx": ["EXTRACT", "EPOCH"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsTime_timetruncate"] = [
        ("select timetruncate(`time`, 1m) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"pg": ["DATE_TRUNC"], "influx": ["DATE_TRUNC"]},
              remote_not_has={"mysql": ["TIMETRUNCATE"]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.10 基础聚合 — default PUSH on all 3
    # -------------------------------------------------------------
    agg_default = ["AVG", "COUNT", "SUM", "STDDEV_SAMP", "VAR_POP",
                   "VAR_SAMP"]
    for fn in agg_default:
        group_dict[f"dsA_{fn.lower()}"] = [
            (f"select {fn}(voltage) from {{SRC}}.sensor",
             dict(remote_has=[fn],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # STDDEV → STDDEV_POP on all 3
    group_dict["dsA_stddev"] = [
        ("select stddev(voltage) from {SRC}.sensor",
         dict(remote_has=["STDDEV_POP"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # VARIANCE → VAR_POP on all 3
    group_dict["dsA_variance"] = [
        ("select variance(voltage) from {SRC}.sensor",
         dict(remote_has=["VAR_POP"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # SPREAD → MAX-MIN expression on all 3
    group_dict["dsA_spread"] = [
        ("select spread(voltage) from {SRC}.sensor",
         dict(remote_has=["MAX", "MIN"], remote_not_has=["SPREAD"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # GROUP_CONCAT: MySQL keeps; PG/Influx STRING_AGG
    group_dict["dsA_group_concat"] = [
        ("select group_concat(region) from {SRC}.sensor",
         dict(remote_has={"mysql": ["GROUP_CONCAT"], "pg": ["STRING_AGG"],
                          "influx": ["STRING_AGG"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # LEASTSQUARES — all local
    group_dict["dsA_leastsquares_local"] = [
        ("select leastsquares(voltage, 0, 1) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["LEASTSQUARES"]))]

    # -------------------------------------------------------------
    # §5.3.4.1.11 分位数与近似统计
    # -------------------------------------------------------------
    group_dict["dsP_percentile"] = [
        ("select percentile(voltage, 50) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"pg": ["PERCENTILE_CONT", "WITHIN GROUP"]},
              remote_not_has={"mysql": ["PERCENTILE"], "influx": ["PERCENTILE"]}))]
    group_dict["dsP_apercentile"] = [
        ("select apercentile(voltage, 50) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has={"mysql": ["APERCENTILE"], "pg": ["APERCENTILE"],
                              "influx": ["APPROX_PERCENTILE_CONT", "APERCENTILE"]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.12 特殊聚合 — all local
    # -------------------------------------------------------------
    group_dict["dsAS_elapsed_local"] = [
        ("select elapsed(`time`) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["ELAPSED"]))]
    group_dict["dsAS_histogram_local"] = [
        ("select histogram(voltage, 'linear_bin', "
         "'{{\"start\":0, \"width\":10, \"count\":10}}', 0) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["HISTOGRAM"]))]
    group_dict["dsAS_hyperloglog_local"] = [
        ("select hyperloglog(region) from {SRC}.sensor",
         dict(checks_add=["remote_sql"], remote_not_has=["HYPERLOGLOG"]))]

    # -------------------------------------------------------------
    # §5.3.4.1.13 选择函数
    # -------------------------------------------------------------
    # MAX/MIN — push on all 3
    for fn in ("MAX", "MIN"):
        group_dict[f"dsSel_{fn.lower()}"] = [
            (f"select {fn}(voltage) from {{SRC}}.sensor",
             dict(remote_has=[fn],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # FIRST/LAST/LAST_ROW/TOP/BOTTOM/TAIL/COLS/UNIQUE — all local
    for fn in ("FIRST", "LAST", "LAST_ROW", "TAIL", "UNIQUE"):
        group_dict[f"dsSel_{fn.lower()}_local"] = [
            (f"select {fn}(voltage) from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn + "("]))]
    for fn, args in [("TOP", "(voltage, 2)"), ("BOTTOM", "(voltage, 2)")]:
        group_dict[f"dsSel_{fn.lower()}_local"] = [
            (f"select {fn}{args} from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn + "("]))]
    # LAG/LEAD — push on all 3 (with implicit OVER)
    for fn in ("LAG", "LEAD"):
        group_dict[f"dsSel_{fn.lower()}"] = [
            (f"select {fn}(voltage, 1) from {{SRC}}.sensor",
             dict(remote_has=[fn, "OVER"],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # MODE: PG push, others local
    group_dict["dsSel_mode"] = [
        ("select mode(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"pg": ["MODE", "WITHIN GROUP"]},
              remote_not_has={"mysql": ["MODE("], "influx": ["MODE("]}))]

    # -------------------------------------------------------------
    # §5.3.4.1.14 比较函数
    # -------------------------------------------------------------
    # IF: MySQL keeps; PG/Influx convert to CASE
    group_dict["dsCmp_if"] = [
        ("select if(voltage > 220, 1, 0) from {SRC}.sensor",
         dict(remote_has={"mysql": ["IF("], "pg": ["CASE", "WHEN"],
                          "influx": ["CASE", "WHEN"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # IFNULL: MySQL keeps; PG/Influx COALESCE
    group_dict["dsCmp_ifnull"] = [
        ("select ifnull(region, 'x') from {SRC}.sensor",
         dict(remote_has={"mysql": ["IFNULL"], "pg": ["COALESCE"],
                          "influx": ["COALESCE"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # NULLIF: all 3 push
    group_dict["dsCmp_nullif"] = [
        ("select nullif(region, 'x') from {SRC}.sensor",
         dict(remote_has=["NULLIF"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # NVL2: MySQL IF; PG/Influx CASE WHEN
    group_dict["dsCmp_nvl2"] = [
        ("select nvl2(region, 1, 0) from {SRC}.sensor",
         dict(remote_has={"mysql": ["IF("], "pg": ["CASE", "WHEN"],
                          "influx": ["CASE", "WHEN"]},
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # -------------------------------------------------------------
    # §5.3.4.1.15 时序函数 — all local
    # -------------------------------------------------------------
    timeseries_local = [
        ("CSUM",        "(voltage)"),
        ("DERIVATIVE",  "(voltage, 1s, 0)"),
        ("DIFF",        "(voltage)"),
        ("IRATE",       "(voltage)"),
        ("MAVG",        "(voltage, 3)"),
        ("STATECOUNT",  "(voltage, 'GT', 220)"),
        ("STATEDURATION","(voltage, 'GT', 220, 1s)"),
        ("TWA",         "(voltage)"),
    ]
    for fn, args in timeseries_local:
        group_dict[f"dsTS_{fn.lower()}_local"] = [
            (f"select {fn}{args} from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn]))]

    # -------------------------------------------------------------
    # §5.3.4.1.16 系统函数 — all local
    # -------------------------------------------------------------
    for fn in ("CLIENT_VERSION", "CURRENT_USER", "DATABASE",
               "SERVER_VERSION", "SERVER_STATUS"):
        group_dict[f"dsSys_{fn.lower()}_local"] = [
            (f"select {fn}() from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"], remote_not_has=[fn]))]

    # -------------------------------------------------------------
    # §5.3.4.1.17 地理函数 — MySQL/PG push, Influx local
    # -------------------------------------------------------------
    # Geometry inputs are built from the non-constant geom_wkt column via
    # ST_GeomFromText so the expression is NOT constant-folded away at plan time
    # (a constant WKT argument folds to a literal and never reaches the remote
    # SQL).  All three sources carry a geom_wkt text column.
    geo_funcs = [
        ("ST_GEOMFROMTEXT", "(geom_wkt)"),
        ("ST_ASTEXT",       "(st_geomfromtext(geom_wkt))"),
    ]
    for fn, args in geo_funcs:
        group_dict[f"dsGeo_{fn.lower()}"] = [
            (f"select {fn}{args} from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"],
                  remote_has={"mysql": [fn], "pg": [fn]},
                  remote_not_has={"influx": [fn]}))]
    # ST_ContainsProperly / ST_Covers — only PG; MySQL/Influx local
    for fn in ("ST_CONTAINSPROPERLY", "ST_COVERS"):
        group_dict[f"dsGeo_{fn.lower()}"] = [
            (f"select {fn}(st_geomfromtext(geom_wkt), "
             f"st_geomfromtext(geom_wkt)) from {{SRC}}.sensor",
             dict(checks_add=["remote_sql"],
                  remote_has={"pg": [fn]},
                  remote_not_has={"mysql": [fn], "influx": [fn]}))]

    # -------------------------------------------------------------
    # §5.3.3 OPERATORS — exhaustive coverage
    # -------------------------------------------------------------
    # Arithmetic unary +/-
    group_dict["dsOpA_unary_neg"] = [
        ("select -voltage from {SRC}.sensor",
         dict(remote_has=["-"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # Comparison: =, <>, !=, >, <, >=, <=
    for op, kw in [("=", "="), ("<>", "<>"), ("!=", "!="),
                   (">", ">"), ("<", "<"), (">=", ">="), ("<=", "<=")]:
        sid = op.replace("=", "eq").replace("<", "lt").replace(">", "gt").replace("!", "ne")
        group_dict[f"dsOpC_{sid}"] = [
            (f"select * from {{SRC}}.sensor where voltage {op} 220.0",
             dict(remote_has=["WHERE", kw],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # IS NULL / IS NOT NULL — all push
    group_dict["dsOpC_isnull"] = [
        ("select * from {SRC}.sensor where region is null",
         dict(remote_has=["WHERE", "NULL"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsOpC_isnotnull"] = [
        ("select * from {SRC}.sensor where region is not null",
         dict(remote_has=["WHERE", "NULL"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # ISNULL / ISNOTNULL function-form — convert to IS [NOT] NULL
    group_dict["dsOpC_isnull_fn"] = [
        ("select * from {SRC}.sensor where isnull(region)",
         dict(remote_has=["IS NULL"], remote_not_has=["ISNULL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsOpC_isnotnull_fn"] = [
        ("select * from {SRC}.sensor where isnotnull(region)",
         dict(remote_has=["IS NOT NULL"], remote_not_has=["ISNOTNULL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # NOT BETWEEN / NOT IN / NOT LIKE — all push
    group_dict["dsOpC_not_between"] = [
        ("select * from {SRC}.sensor where voltage not between 219 and 222",
         dict(remote_has=["WHERE", "BETWEEN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsOpC_not_in"] = [
        ("select * from {SRC}.sensor where voltage not in (220, 221)",
         dict(remote_has=["WHERE", "IN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    group_dict["dsOpC_not_like"] = [
        ("select * from {SRC}.sensor where region not like 'no%'",
         dict(remote_has=["WHERE", "LIKE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # MATCH / NMATCH: MySQL REGEXP / NOT REGEXP; PG ~ / !~; Influx local
    group_dict["dsOpC_match"] = [
        ("select * from {SRC}.sensor where region match 'no.*'",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["REGEXP"], "pg": ["~"]},
              remote_not_has={"influx": ["MATCH"]}))]
    group_dict["dsOpC_nmatch"] = [
        ("select * from {SRC}.sensor where region nmatch 'no.*'",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT REGEXP"], "pg": ["!~"]},
              remote_not_has={"influx": ["NMATCH"]}))]

    # Bitwise: & | — MySQL/PG push, Influx local.  All sources use region_info.
    for op, kw in [("&", "&"), ("|", "|")]:
        sid = "and" if op == "&" else "or"
        group_dict[f"dsOpBit_{sid}"] = [
            (f"select area {op} 1 from {{SRC}}.region_info",
             dict(checks_add=["remote_sql"],
                  remote_has={"mysql": [kw], "pg": [kw]},
                  remote_not_has={"influx": [f" {op} "]}))]

    # UNION (with dedup) same-source — DS §5.3.8.6 requires push-down as UNION.
    group_dict["dsOpSet_union_dedup"] = [
        ("select region from {SRC}.sensor "
         "union "
         "select region from {SRC}.sensor where voltage > 220",
         dict(checks_add=["remote_sql"], remote_has=["UNION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # -------------------------------------------------------------
    # §5.3.5 WINDOWS — exhaustive
    # -------------------------------------------------------------
    # PARTITION BY without window + agg — converts to GROUP BY
    group_dict["dsWin_partition_groupby"] = [
        ("select region, count(*) from {SRC}.sensor partition by region",
         dict(remote_has=["GROUP BY"], remote_not_has=["PARTITION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # PARTITION BY + pushable aggregate — same rewrite path as COUNT(*):
    # convert to remote GROUP BY and keep PARTITION out of Remote SQL.
    partition_push_aggs = [
        ("AVG", "avg(voltage)"),
        ("SUM", "sum(voltage)"),
        ("MIN", "min(voltage)"),
        ("MAX", "max(voltage)"),
    ]
    group_dict["dsWin_partition_groupby_pushagg"] = [
        (f"select region, {aexpr} from {{SRC}}.sensor partition by region",
         dict(remote_has=[aname, "GROUP BY"], remote_not_has=["PARTITION"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))
        for aname, aexpr in partition_push_aggs
    ]
    # PARTITION BY + INTERVAL — when the whole shape is remotely expressible,
    # keep it in one remote grouped query.
    group_dict["dsWin_partition_interval_local"] = [
        ("select _wstart, region, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "partition by region interval(1m)",
         dict(checks_add=["remote_sql"],
              remote_has=["GROUP BY"],
              remote_not_has=["PARTITION", "INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # INTERVAL tumbling — push, no SLIDING
    group_dict["dsWin_interval"] = [
        ("select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(1m)",
         dict(remote_has=["GROUP BY"], remote_not_has=["INTERVAL("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # INTERVAL with SLIDING — local
    group_dict["dsWin_interval_sliding_local"] = [
        ("select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "interval(2m) sliding(1m)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["INTERVAL(", "SLIDING"]))]
    # STATE/SESSION/EVENT/COUNT windows — all local
    group_dict["dsWin_state_local"] = [
        ("select count(*), region from {SRC}.sensor state_window(region)",
         dict(checks_add=["remote_sql"], remote_not_has=["STATE_WINDOW"]))]
    group_dict["dsWin_session_local"] = [
        ("select count(*) from {SRC}.sensor session(`time`, 1m)",
         dict(checks_add=["remote_sql"], remote_not_has=["SESSION("]))]
    group_dict["dsWin_count_local"] = [
        ("select count(*) from {SRC}.sensor count_window(2)",
         dict(checks_add=["remote_sql"], remote_not_has=["COUNT_WINDOW"]))]
    group_dict["dsWin_event_local"] = [
        ("select count(*) from {SRC}.sensor "
         "event_window start with voltage > 220 end with voltage < 220",
         dict(checks_add=["remote_sql"], remote_not_has=["EVENT_WINDOW"]))]

    # -------------------------------------------------------------
    # §5.3.6 JOINs — full matrix
    # -------------------------------------------------------------
    base_join = ("select s.`time`, s.voltage, r.area "
                 "from {SRC}.sensor s {JOIN} {SRC}.region_info r "
                 "on s.`time` = r.`time` and s.region = r.region")
    # Inner / Left / Right — push on all 3 sources
    for kind, kw in [("inner join", "JOIN"),
                     ("left join", "LEFT"),
                     ("right join", "RIGHT")]:
        sid = kind.split()[0]
        group_dict[f"dsJoin_{sid}"] = [
            (base_join.replace("{JOIN}", kind),
             dict(checks_add=["join_remote_or_multi", "remote_sql"],
                  remote_has=[kw, "JOIN"],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # Full Outer Join: PG/Influx push; MySQL local (no FULL OUTER JOIN)
    group_dict["dsJoin_full"] = [
        (base_join.replace("{JOIN}", "full join"),
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has={"pg": ["FULL", "JOIN"], "influx": ["FULL", "JOIN"]},
              remote_not_has={"mysql": ["FULL"]}))]
    # JOIN + WHERE on both sides → WHERE pushed into remote JOIN SQL
    group_dict["dsJoin_inner_where"] = [
        ("select s.`time`, s.voltage, r.area "
         "from {SRC}.sensor s inner join {SRC}.region_info r "
         "on s.`time` = r.`time` and s.region = r.region "
         "where s.voltage > 220.0 and r.area = 1",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["JOIN", "WHERE"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # JOIN + ORDER BY + LIMIT → full pushdown
    group_dict["dsJoin_inner_order_limit"] = [
        ("select s.`time`, s.voltage, r.area "
         "from {SRC}.sensor s inner join {SRC}.region_info r "
         "on s.`time` = r.`time` and s.region = r.region "
         "order by s.`time` limit 5",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["JOIN", "ORDER BY", "LIMIT"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # JOIN + GROUP BY aggregation on joined result
    # JOIN is pushed to remote; GROUP BY evaluated locally (AGG+JOIN not yet implemented)
    group_dict["dsJoin_left_groupby"] = [
        ("select r.area, count(s.voltage) "
         "from {SRC}.sensor s left join {SRC}.region_info r "
         "on s.`time` = r.`time` and s.region = r.region "
         "group by r.area",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has=["LEFT", "JOIN"],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # Non-pushable JOIN type (ASOF) — ASOF JOIN syntax is not supported for
    # external tables (parser error), so this group is intentionally omitted.
    # Cross-source JOIN coverage is in join_cross_source above.
    # Semi/Anti Semi via correlated EXISTS/NOT EXISTS — TDengine subquery-as-
    # expression unsupported ([0x26a6]); EXPLAIN fails at parse for ALL sources.
    group_dict["dsJoin_semi_via_exists"] = [
        ("select * from {SRC}.sensor s where exists "
         "(select 1 from {SRC}.region_info r "
         "where r.`time` = s.`time` and r.region = s.region)",
         dict(expect_err=True))]
    group_dict["dsJoin_antisemi_via_notexists"] = [
        ("select * from {SRC}.sensor s where not exists "
         "(select 1 from {SRC}.region_info r "
         "where r.`time` = s.`time` and r.region = s.region)",
         dict(expect_err=True))]

    # -------------------------------------------------------------
    # §5.3.7 SUBQUERIES — full matrix
    # -------------------------------------------------------------
    # §5.3.7 SUBQUERIES — full matrix.  Every statement runs on all 3 sources;
    # MySQL/PG push the subquery, InfluxDB runs locally but must still execute.
    # -------------------------------------------------------------
    # NOT IN over sibling table (region_info).
    group_dict["dsSubq_not_in"] = [
        ("select * from {SRC}.sensor where region not in "
         "(select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # NOT IN over the SAME table (distinct statement: self-referencing inner).
    group_dict["dsSubq_not_in_self"] = [
        ("select * from {SRC}.sensor where region not in "
         "(select region from {SRC}.sensor where voltage > 220)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["NOT IN"], "pg": ["NOT IN"]},
              remote_not_has={"influx": ["NOT IN (SELECT", "NOT IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # NOT EXISTS (correlated) — TDengine subquery-as-expression unsupported
    # ([0x26a6]); EXPLAIN fails at parse for ALL sources.
    group_dict["dsSubq_not_exists"] = [
        ("select * from {SRC}.sensor s where not exists "
         "(select 1 from {SRC}.region_info r where r.region = s.region)",
         dict(expect_err=True))]
    # NOT EXISTS (non-correlated) — runs on all sources.
    group_dict["dsSubq_not_exists_nocorr"] = [
        ("select * from {SRC}.sensor s where not exists "
        "(select 1 from {SRC}.sensor r where r.voltage > 220)",
        dict(checks_add=["remote_sql"],
             remote_has={"mysql": ["NOT", "EXISTS"],
                         "pg": ["NOT", "EXISTS"]},
             remote_not_has={"influx": ["NOT EXISTS"]},
             local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
             local_has={"influx": ["InitPlan 1"]}))]
    # ALL/SOME — MySQL/PG push.  SOME is normalized to ANY at parse time
    # (both map to QU_TYPE_ANY), so the rendered SQL uses ANY for SOME input.
    # InfluxDB runs locally but must still execute.
    for kw in ("ALL", "SOME"):
        _expect_kw = "ANY" if kw == "SOME" else kw
        group_dict[f"dsSubq_{kw.lower()}"] = [
            (f"select * from {{SRC}}.sensor where voltage > {kw} "
             f"(select voltage from {{SRC}}.sensor where region = 'south')",
             dict(checks_add=["remote_sql"],
                  remote_has={"mysql": [_expect_kw], "pg": [_expect_kw]},
                  remote_not_has={"influx": ["ANY (", "SOME (", "ALL ("]},
                  local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
                  local_has={"influx": ["InitPlan 1"]}))]

    # =============================================================
    # 子查询覆盖补强 — 对照本地表用例 09-DataQuerying/08-SubQuery
    # (test_quantified_sub1 / test_scalar_sub1 / test_in_subq1)
    # 本地表支持的 {算子}×{量词}、标量包裹聚合、子句位置(HAVING/投影/
    # GROUP BY)、NULL 语义,联邦查询都应有对应覆盖。每条语句对所有 3 源
    # 执行;MySQL/PG 下推量词谓词,InfluxDB 本地执行但必须成功。
    # =============================================================

    # ---- (A) ALL/ANY/SOME × 比较算子全矩阵 ----
    # 本地表 compareExprs = {=, !=, <, <=, >, >=}; quantifiedExprs = {ALL, ANY, SOME}.
    # 已有 dsSubq_all/some/any 覆盖 ">"; 这里补齐其余 5 个算子 × 3 量词.
    # 关键: "<"/"<=" 触发渲染器 MIN→ALL / MAX→ANY 反向分支, 必须覆盖.
    # InfluxDB: 量词谓词本地, remote SQL 不得含 ALL(/ANY(/SOME(.
    # 注: TDengine parser 接受 <> 但不接受 != 作为 quantified 比较算子
    # (原生表实测: f1 != ALL(...) → syntax error; f1 <> ALL(...) → OK).
    _Q_OPS = [("=", "eq"), ("<>", "ne"), ("<", "lt"),
              ("<=", "le"), (">=", "ge")]
    for _op, _ops_id in _Q_OPS:
        for _q in ("ALL", "ANY", "SOME"):
            if _op == "=" and _q in ("ANY", "SOME"):
                _exp = "IN"
            elif _op == "<>" and _q == "ALL":
                _exp = "NOT IN"
            else:
                _exp = "ANY" if _q == "SOME" else _q
            group_dict[f"dsSubq_q_{_ops_id}_{_q.lower()}"] = [
                (f"select * from {{SRC}}.sensor where voltage {_op} {_q} "
                 f"(select voltage from {{SRC}}.sensor where region = 'south')",
                 dict(checks_add=["remote_sql"],
                      remote_has={"mysql": [_exp], "pg": [_exp]},
                      remote_not_has={"influx": ["ALL (", "ANY (", "SOME ("]},
                      local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
                      local_has={"influx": ["InitPlan 1"]}))]

    # ---- (A2) 量词子查询出现在 SELECT 投影位置 (本地表 querySql 模板) ----
    # select <expr> <op> <quantifier> (subq) from ... — 谓词在投影列.
    group_dict["dsSubq_q_proj_any"] = [
        ("select voltage > any "
         "(select voltage from {SRC}.sensor where region = 'south') "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ALL (", "ANY (", "SOME ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    group_dict["dsSubq_q_proj_all"] = [
        ("select voltage <= all "
         "(select voltage from {SRC}.sensor where region = 'south') "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ALL"], "pg": ["ALL"]},
              remote_not_has={"influx": ["ALL (", "ANY (", "SOME ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]

    # ---- (B) 标量子查询 × 比较算子全矩阵 ----
    # 已有 subq_scalar 覆盖 "> (select avg...)"; 补齐其余算子.
    # DS §5.3.7.1.2: 三库在“原文即可直接表达”的非相关标量子查询场景都应
    # 直接下推，因此这里要显式钉住远端 SQL 中的标量聚合。
    _S_OPS = [("=", "eq"), ("!=", "ne"), ("<", "lt"),
              ("<=", "le"), (">=", "ge")]
    for _op, _ops_id in _S_OPS:
        group_dict[f"dsSubq_s_{_ops_id}"] = [
            (f"select `time`, voltage from {{SRC}}.sensor "
             f"where voltage {_op} "
             f"(select avg(voltage) from {{SRC}}.sensor)",
             dict(checks_add=["remote_sql", "subquery_handling"],
                  remote_any_has=["AVG("],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---- (B2) 标量子查询内层聚合变体 (本地表 scalarSqls: count/sum/min/max) ----
    # 子查询本体用不同聚合, 验证内层 SELECT 渲染 (函数映射 2b) 正确.
    for _agg in ("min", "max", "count", "sum"):
        _inner_col = "*" if _agg == "count" else "voltage"
        group_dict[f"dsSubq_s_agg_{_agg}"] = [
            (f"select `time`, voltage from {{SRC}}.sensor "
             f"where voltage > "
             f"(select {_agg}({_inner_col}) from {{SRC}}.sensor)",
             dict(checks_add=["remote_sql", "subquery_handling"],
                  remote_any_has=[_agg.upper() + "("],
                  local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---- (B3) 标量子查询出现在 SELECT 投影位置 (本地表 subSqls 模板) ----
    # select (subq) ... from ... — 标量子查询作为投影列, 非 WHERE.
    group_dict["dsSubq_s_proj"] = [
        ("select voltage, "
         "(select max(voltage) from {SRC}.sensor) "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_any_has=["MAX("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # =============================================================
    # (C) 子句位置全覆盖 — 对照本地表 subSqls 模板的每一种位置.
    # 本地表 (scalar/in/quantified) 把子查询放在以下位置都做了测试:
    #   SELECT 投影(裸/别名/distinct/多次/expr,*) / 聚合包裹 / CASE 内 /
    #   GROUP BY 键 / HAVING / PARTITION BY / 窗口(interval/session/state/
    #   event/count) / JOIN ON / WHERE(各算子/and/or/between/cast) /
    #   UNION·UNION ALL / ORDER BY.
    # 联邦语义分两类:
    #   * 直接下推位置:
    #       - 标量 / FROM 子查询按 DS §5.3.7.1.1/1.2 三库都应 remote_has；
    #       - IN / EXISTS / ALL-ANY-SOME 仍按能力矩阵做方言分流。
    #   * 本地算子位置(窗口 interval/session/state/event/count、partition by):
    #     该子句在 FQ 中本地执行, remote_not_has 窗口/PARTITION 关键字;
    #     子查询随之本地. 每条对 3 源都执行且必须成功.
    # =============================================================

    # ---- (C-SELECT) 投影位置变体 ----
    # 标量子查询带别名.
    group_dict["dsSubq_c_proj_alias"] = [
        ("select (select max(voltage) from {SRC}.sensor) mx "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_any_has=["MAX("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # IN 子查询在投影.
    group_dict["dsSubq_c_proj_in"] = [
        ("select region in (select region from {SRC}.region_info) "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # 量词子查询在投影 — 见上方 (A2) dsSubq_q_proj_any / dsSubq_q_proj_all.
    # DISTINCT + 标量子查询在投影.
    group_dict["dsSubq_c_proj_distinct"] = [
        ("select distinct region in (select region from {SRC}.region_info) "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["DISTINCT", "IN"],
                          "pg": ["DISTINCT", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # 标量子查询投影 + ORDER BY (本地表 "select 1,{subq} ... order by ts").
    group_dict["dsSubq_c_proj_order"] = [
        ("select voltage, (select max(voltage) from {SRC}.sensor) mx "
        "from {SRC}.sensor order by voltage",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_any_has=["MAX("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # 标量子查询 + 星号投影，覆盖 "expr,*" 形态。
    group_dict["dsSubq_c_proj_star"] = [
        ("select (select max(voltage) from {SRC}.sensor) mx, * "
         "from {SRC}.sensor order by voltage",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_any_has=["MAX("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # 同一层投影中出现多个标量子查询，覆盖 "多次子查询投影"。
    group_dict["dsSubq_c_proj_multi_scalar"] = [
        ("select (select max(voltage) from {SRC}.sensor) mx, "
         "(select min(voltage) from {SRC}.sensor) mn "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql", "subquery_handling"],
              remote_any_has=["MAX(", "MIN("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---- (C-AGG) 聚合包裹标量子查询 (本地表 count/sum/avg(subq)) ----
    # 外层聚合包裹一个标量子查询 (uncorrelated → 常量), 验证可下推.
    group_dict["dsSubq_c_agg_wrap"] = [
        ("select sum((select count(*) from {SRC}.region_info)) "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_any_has={
                  "mysql": ["SUM(", "COUNT("],
                  "pg": ["SUM(", "COUNT("],
              },
              remote_any_not_has={
                  "influx": ["SUM(", "COUNT("],
              },
              local_has={
                  "influx": ["Aggregate", "InitPlan 1"],
              }))]

    # ---- (C-CASE) CASE 表达式内含子查询 (本地表 case when {subq}) ----
    # CASE WHEN <标量子查询比较>.
    group_dict["dsSubq_c_case_scalar"] = [
        ("select case when voltage > "
         "(select avg(voltage) from {SRC}.sensor) then 'h' else 'l' end "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IF("], "pg": ["CASE"], "influx": ["CASE"]},
              remote_any_has=["AVG("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # CASE WHEN <IN 子查询>.
    group_dict["dsSubq_c_case_in"] = [
        ("select case when region in "
         "(select region from {SRC}.region_info) then 1 else 0 end "
         "from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IF(", "IN"], "pg": ["CASE", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_has={"influx": ["InitPlan 1"]}))]

    # ---- (C-GROUPBY) 子查询作为 GROUP BY 键 (本地表 group by {subq}) ----
    group_dict["dsSubq_c_groupby_key"] = [
        # 本地子查询基线已确认：`GROUP BY <in-subquery-expr>` 语法本身
        # 当前就不被 TDengine 接受，因此这里应明确校验 expected error，
        # 而不是把它当成联邦下推能力用例。
        ("select count(*) from sensor "
         "group by 1 in (select 1 from region_info)",
         dict(pre_sql="use {SRC}", expect_err=True))]

    # ---- (C-HAVING) 子查询在 HAVING (本地表 group by f1 having(...subq)) ----
    # HAVING + 标量子查询比较.
    group_dict["dsSubq_c_having_scalar"] = [
        ("select region, count(*) cnt from {SRC}.sensor "
         "group by region "
         "having count(*) > (select count(*) from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has=["HAVING"],
              remote_any_has=["COUNT("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # HAVING + IN 子查询.
    group_dict["dsSubq_c_having_in"] = [
        ("select region, count(*) cnt from {SRC}.sensor "
         "group by region "
         "having region in (select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["WHERE", "GROUP BY", "IN"],
                          "pg": ["WHERE", "GROUP BY", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_has={"influx": ["InitPlan 1"]}))]

    # ---- (C-JOIN) 子查询在 JOIN ON 条件 (本地表 on a.ts=b.ts and ...subq) ----
    # 同源 JOIN, ON 含 IN 子查询. JOIN 下推时子查询随 ON 一起下推.
    group_dict["dsSubq_c_join_on_in"] = [
        ("select s.`time`, s.voltage from {SRC}.sensor s "
         "inner join {SRC}.region_info r "
         "on s.`time` = r.`time` and s.region in "
         "(select region from {SRC}.region_info)",
         dict(checks_add=["join_remote_or_multi", "remote_sql"],
              remote_has={"mysql": ["JOIN", "IN"], "pg": ["JOIN", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_has={"influx": ["InitPlan 1"]}))]

    # ---- (C-WHERE) WHERE 复合条件含子查询 (本地表 and/or/between/cast) ----
    # IN 子查询 AND 普通条件.
    group_dict["dsSubq_c_where_and"] = [
        ("select * from {SRC}.sensor where voltage > 219.0 "
         "and region in (select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["WHERE", "AND", "IN"],
                          "pg": ["WHERE", "AND", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # IN 子查询 OR 普通条件.
    group_dict["dsSubq_c_where_or"] = [
        ("select * from {SRC}.sensor where voltage > 221.0 "
         "or region in (select region from {SRC}.region_info)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["WHERE", "OR", "IN"],
                          "pg": ["WHERE", "OR", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # 标量子查询 BETWEEN (本地表 between {subq} and {subq}+1).
    group_dict["dsSubq_c_where_between"] = [
        ("select * from {SRC}.sensor where voltage between "
         "(select min(voltage) from {SRC}.sensor) "
         "and (select max(voltage) from {SRC}.sensor)",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "BETWEEN"],
              remote_any_has=["MIN(", "MAX("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # 标量子查询先进入 CAST，再参与 WHERE 比较，覆盖 where(cast(subq)).
    group_dict["dsSubq_c_where_cast_scalar"] = [
        ("select * from {SRC}.sensor where voltage > "
         "cast((select avg(voltage) from {SRC}.sensor) as double)",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "CAST"],
              remote_any_has=["AVG("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---- (C-UNION) 子查询出现在 UNION 分支 (本地表 union select {subq}) ----
    # 两分支都含 IN 子查询, UNION ALL 同源下推为单条 SQL.
    group_dict["dsSubq_c_union_in"] = [
        ("select region in (select region from region_info) "
         "from sensor where voltage > 221.0 "
         "union all "
         "select region in (select region from region_info) "
         "from sensor where voltage < 220.0",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["UNION", "IN"], "pg": ["UNION", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # UNION 去重分支也要覆盖含 IN 子查询的分支形态。
    group_dict["dsSubq_c_union_dedup_in"] = [
        ("select region in (select region from region_info) "
         "from sensor where voltage > 221.0 "
         "union "
         "select region in (select region from region_info) "
         "from sensor where voltage < 220.0",
         dict(pre_sql="use {SRC}",
              checks_add=["remote_sql"],
              remote_has={"mysql": ["UNION", "IN"], "pg": ["UNION", "IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              plain_local_has={"influx": ["InitPlan 1", "InitPlan 2"]},
              local_has={"influx": ["InitPlan 1", "InitPlan 2", "Projection"]}))]

    # ---- (C-WINDOW) 子查询 + 窗口子句 ----
    # INTERVAL 本身是可下推窗口，Remote SQL 应看到标量子查询聚合和
    # INTERVAL→GROUP BY 的转换结果；STATE_WINDOW 仍是本地窗口。
    group_dict["dsSubq_c_win_interval"] = [
        ("select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         "and voltage > (select avg(voltage) from {SRC}.sensor) "
         "interval(1m)",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "GROUP BY"],
              remote_not_has=["INTERVAL("],
              remote_any_has=["AVG("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # STATE_WINDOW + WHERE 标量子查询 (窗口本地, 子查询比较随 WHERE).
    group_dict["dsSubq_c_win_state"] = [
        ("select count(*), region from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor) "
         "state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["STATE_WINDOW"],
              remote_any_has=["AVG("],
              local_has=[_LOCAL_FALLBACK_SENTINEL]))]
    # SESSION 窗口本地，前置 WHERE 的标量子查询仍应单独验证其渲染路径。
    group_dict["dsSubq_c_win_session"] = [
        ("select count(*) from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor) "
         "session(`time`, 1m)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SESSION("],
              remote_any_has=["AVG("],
              local_has=["Session"],
              local_not_has=["InitPlan 1", "Filter:"]))]
    # EVENT_WINDOW 本地，子查询位于窗口前的 WHERE 条件。
    group_dict["dsSubq_c_win_event"] = [
        ("select count(*) from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor) "
         "event_window start with voltage > 220 end with voltage < 220",
         dict(checks_add=["remote_sql"],
              remote_not_has=["EVENT_WINDOW"],
              remote_any_has=["AVG("],
              local_has=["Event"],
              local_not_has=["InitPlan 1", "Filter:"]))]
    # COUNT_WINDOW 本地，子查询位于窗口前的 WHERE 条件。
    group_dict["dsSubq_c_win_count"] = [
        ("select count(*) from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor) "
         "count_window(2)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["COUNT_WINDOW"],
              remote_any_has=["AVG("],
              local_has=["Count"],
              local_not_has=["InitPlan 1", "Filter:"]))]

    # ---- (C-PARTITION) 子查询 + PARTITION BY ----
    # 这里是“PARTITION BY + 聚合”场景，按 DS 可转换为 remote GROUP BY。
    group_dict["dsSubq_c_partition"] = [
        ("select region, count(*) from {SRC}.sensor "
         "where voltage > (select avg(voltage) from {SRC}.sensor) "
         "partition by region",
         dict(checks_add=["remote_sql"],
              remote_has=["WHERE", "GROUP BY"],
              remote_any_has=["AVG("],
              local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---- (D) NULL 语义: ALL/ANY 子查询结果含 NULL (本地表 filterExprs IS NULL) ----
    # ALL/ANY 在子查询含 NULL 行时有特殊 _has_null 语义.
    # 子查询 WHERE region is null 时结果可能为空/含 NULL.
    group_dict["dsSubq_d_all_isnull"] = [
        ("select * from {SRC}.sensor where voltage >= all "
         "(select voltage from {SRC}.sensor where region is null)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ALL"], "pg": ["ALL"]},
              remote_not_has={"influx": ["ALL (", "ANY (", "SOME ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    group_dict["dsSubq_d_any_isnull"] = [
        ("select * from {SRC}.sensor where voltage < any "
         "(select voltage from {SRC}.sensor where region is null)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["ANY"], "pg": ["ANY"]},
              remote_not_has={"influx": ["ALL (", "ANY (", "SOME ("]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]
    # NULL 语义: IN 子查询结果含 NULL.
    group_dict["dsSubq_d_in_isnull"] = [
        ("select * from {SRC}.sensor where region in "
         "(select region from {SRC}.region_info where area is null)",
         dict(checks_add=["remote_sql"],
              remote_has={"mysql": ["IN"], "pg": ["IN"]},
              remote_not_has={"influx": ["IN (SELECT", "IN(SELECT"]},
              local_not_has={"mysql": ["InitPlan 1"], "pg": ["InitPlan 1"]},
              local_has={"influx": ["InitPlan 1"]}))]

    # -------------------------------------------------------------
    # §5.3.8 OTHER CLAUSES — exhaustive
    # -------------------------------------------------------------
    # SOFFSET — TDengine local
    group_dict["dsClause_soffset_local"] = [
        ("select `time`, voltage from {SRC}.sensor "
         "partition by region slimit 1 soffset 0",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SLIMIT", "SOFFSET"]))]
    # UNION (dedup) cross-source — local
    group_dict["dsClause_union_cross_local"] = [
        ("select region from {SRC}.sensor "
         "union "
         "select region from {SRC2}.sensor",
         dict(sources="cross",
              checks=["min_federated_scans", "no_table_scan",
                      "analyze_metrics", "remote_not_has"],
              min_scans=2, remote_not_has=["UNION"]))]
    # Hints — must be stripped, never appear in Remote SQL
    group_dict["dsClause_hint_local"] = [
        ("select /*+ batch_scan() */ `time`, voltage from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["/*+", "BATCH_SCAN"]))]

    return group_dict


# Bucketing: collapse fine-grained per-item keys into coarse category groups.
# Each call site below uses keys like ``dsM_abs_mysql`` purely for readability;
# at merge time we map every key onto a small set of canonical buckets.
_DS_BUCKET_MAP = [
    ("dsM_",     "ds_math"),
    ("dsS_",     "ds_string"),
    ("dsE_",     "ds_encoding"),
    ("dsH_",     "ds_hash"),
    ("dsBF_",    "ds_bitwise_func"),
    ("dsMask_",  "ds_mask"),
    ("dsCrypto_","ds_crypto"),
    ("dsT_",     "ds_typeconv"),
    ("dsTime_",  "ds_time"),
    ("dsA_",     "ds_agg_basic"),
    ("dsP_",     "ds_quantile"),
    ("dsAS_",    "ds_agg_special"),
    ("dsSel_",   "ds_selection"),
    ("dsCmp_",   "ds_compare"),
    ("dsTS_",    "ds_timeseries"),
    ("dsSys_",   "ds_system"),
    ("dsGeo_",   "ds_geo"),
    ("dsOpA_",   "ds_op_arith"),
    ("dsOpC_",   "ds_op_compare"),
    ("dsOpBit_", "ds_op_bitwise"),
    ("dsOpSet_", "ds_op_set"),
    ("dsWin_",   "ds_window"),
    ("dsJoin_",  "ds_join"),
    ("dsSubq_",  "ds_subquery"),
    ("dsClause_","ds_clause"),
    ("genFF_",   "gen_F_F"),
    ("genFO_",   "gen_F_O"),
    ("genLF_O_", "gen_LF_O"),
    ("genFW_",   "gen_F_W"),
    ("genLLFW_", "gen_LF_W"),
    ("genFC_",   "gen_F_C"),
    ("genLLFC_", "gen_LF_C"),
    ("genOC_",   "gen_O_C"),
    ("genWC_",   "gen_W_C"),
    ("gen3FOC_", "gen_FOC"),
    ("gen3LOC_", "gen_LFOC"),
]


class _BucketDict(dict):
    """dict subclass that funnels assignments into coarse buckets.

    `bucket_dict[fine_key] = [entry]` appends `entry` to the list keyed
    by the coarse bucket determined from ``fine_key`` via prefix match.
    Falls back to the original key when no prefix matches.
    """

    def __setitem__(self, key, value):
        bucket = key
        for prefix, target in _DS_BUCKET_MAP:
            if key.startswith(prefix):
                bucket = target
                break
        existing = super().get(bucket)
        if existing is None:
            super().__setitem__(bucket, list(value))
        else:
            existing.extend(value)


_EXPLAIN_GROUPS.update(_ds_emit(_BucketDict()))


# =========================================================================
# DS §5.3 cross-dimension cartesian generators
# -------------------------------------------------------------------------
# Generate combos as cartesian products over enumerated members:
#   PUSH funcs × PUSH operators × PUSH clauses
#   LOCAL funcs × LOCAL windows × ...
# Hand-written combos earlier give specific high-coverage scenarios;
# generators below give breadth — every {push function} × {push window}
# × {push clause}, etc.
# =========================================================================


def _ds_combo_emit(group_dict):

    # Reduced lists used in combos to keep case explosion bounded but
    # still touch every category.

    # ---- Pushable scalar functions (expressible in all remote SQL dialects) ----
    PUSH_SCALAR_FUNCS = [
        ("ABS",    "abs(voltage - 220.0)"),
        ("UPPER",  "upper(region)"),
        ("ROUND",  "round(voltage, 1)"),
        ("LENGTH", "length(region)"),
        ("MD5",    "md5(region)"),
        ("CAST",   "cast(voltage as bigint)"),
    ]

    # ---- Non-pushable scalar functions (TDengine-specific, stay local) ----
    LOCAL_SCALAR_FUNCS = [
        ("MASK_FULL",        "MASK_FULL(region, '*')"),
        ("TO_ISO8601",       "to_iso8601(`time`)"),
        ("MASK_PARTIAL",     "MASK_PARTIAL(region, 1, 1, '#')"),
        ("MASK_NONE",        "MASK_NONE(region)"),
    ]

    # ---- Pushable aggregate functions ----
    PUSH_AGG_FUNCS = [
        ("COUNT", "count(*)"),
        ("AVG",   "avg(voltage)"),
        ("SUM",   "sum(voltage)"),
        ("MIN",   "min(voltage)"),
        ("MAX",   "max(voltage)"),
    ]

    # ---- Non-pushable aggregate functions (TDengine-specific, stay local) ----
    LOCAL_AGG_FUNCS = [
        ("TWA",        "twa(voltage)"),
        ("IRATE",      "irate(voltage)"),
        ("STATECOUNT", "statecount(voltage, 'GT', 220)"),
    ]

    # ---- Indefinite-rows functions (local-only, neither scalar nor aggregate) ----
    LOCAL_INDEF_FUNCS = [
        ("CSUM",       "csum(voltage)"),
        ("DIFF",       "diff(voltage)"),
        ("DERIVATIVE", "derivative(voltage, 1s, 0)"),
        ("MAVG",       "mavg(voltage, 3)"),
        ("ELAPSED",    "elapsed(`time`)"),
        ("HISTOGRAM",  "histogram(voltage, 'linear_bin', "
                       "'{{\"start\":0,\"width\":10,\"count\":10}}', 0)"),
    ]

    # Backward-compat aliases used by the remaining loops below
    PUSH_FUNCS = PUSH_SCALAR_FUNCS
    LOCAL_FUNCS = LOCAL_INDEF_FUNCS + LOCAL_SCALAR_FUNCS
    PUSH_OPS = [
        ("BETWEEN", "voltage between 219 and 222"),
        ("IN",      "voltage in (220, 221)"),
        ("LIKE",    "region like 'no%'"),
        ("AND",     "voltage > 220 and region = 'north'"),
        ("OR",      "voltage > 222 or region = 'south'"),
    ]
    PUSH_CLAUSES = [
        ("WHERE",    "where voltage > 219", ["WHERE"]),
        ("ORDER",    "order by `time`", ["ORDER BY"]),
        ("LIMIT",    "limit 3", ["LIMIT"]),
        ("DISTINCT", "(distinct)", ["DISTINCT"]),  # special handled below
        ("GROUP",    "group by region", ["GROUP BY"]),
        ("HAVING",   "group by region having count(*) > 0",
                     ["GROUP BY", "HAVING"]),
        ("CASE",     "(case)", ["CASE", "WHEN"]),  # special
    ]
    LOCAL_WINS = [
        ("STATE_WINDOW",  "state_window(region)",  "STATE_WINDOW"),
        ("SESSION",       "session(`time`, 1m)",   "SESSION("),
        ("COUNT_WINDOW",  "count_window(2)",       "COUNT_WINDOW"),
        ("EVENT_WINDOW",  "event_window start with voltage > 220 "
                          "end with voltage < 220",  "EVENT_WINDOW"),
    ]
    LOCAL_CLAUSES = [
        ("SLIMIT",   "partition by region slimit 1",
                     ["SLIMIT", "PARTITION"]),
        ("FILL",     "fill",  # see filler below; uses interval
                     ["FILL"]),
        ("INTERP",   "interp",  # special form below
                     ["INTERP"]),
    ]

    # ---------- F_push_scalar × F_local_scalar (标量下推 + 标量不下推) ----------
    for pname, pexpr in PUSH_SCALAR_FUNCS:
        for lname, lexpr in LOCAL_SCALAR_FUNCS[:2]:
            key = f"genFF_{pname.lower()}_{lname.lower()}"
            group_dict[key] = [
                (f"select {pexpr}, {lexpr} from {{SRC}}.sensor",
                 dict(checks_add=["remote_sql"],
                      remote_has=[pname],
                      remote_not_has=[lname]))]

    # ---------- F_push × O_push (in WHERE) ----------
    for pname, pexpr in PUSH_FUNCS[:4]:
        for opname, oexpr in PUSH_OPS:
            key = f"genFO_{pname.lower()}_{opname.lower()}"
            group_dict[key] = [
                (f"select {pexpr} from {{SRC}}.sensor where {oexpr}",
                 dict(remote_has=[pname, "WHERE"],
                      local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- F_local_indef × O_push (local indefinite-rows func + push WHERE) ----------
    for lname, lexpr in LOCAL_INDEF_FUNCS[:4]:
        for opname, oexpr in PUSH_OPS[:3]:
            key = f"genLF_O_{lname.lower()}_{opname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor where {oexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_has=["WHERE"], remote_not_has=[lname]))]

    # ---------- LOCAL_SCALAR × O_push (local scalar + push WHERE) ----------
    for lname, lexpr in LOCAL_SCALAR_FUNCS[:2]:
        for opname, oexpr in PUSH_OPS[:3]:
            key = f"genLS_O_{lname.lower()}_{opname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor where {oexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_has=["WHERE"], remote_not_has=[lname]))]

    # ---------- LOCAL_AGG × O_push (local agg + push WHERE) ----------
    for lname, lexpr in LOCAL_AGG_FUNCS[:2]:
        for opname, oexpr in PUSH_OPS[:3]:
            key = f"genLA_O_{lname.lower()}_{opname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor where {oexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_has=["WHERE"], remote_not_has=[lname]))]

    # ---------- F_push_scalar × W_local (标量下推 + 本地窗口) ----------
    # Window clauses require at least one aggregate; use count(*) as the aggregate.
    # TDengine only allows a non-aggregated scalar alongside an aggregate in a window
    # when the scalar expression uses the window's partition column.  STATE_WINDOW
    # partitions by `region`, so upper(region) / length(region) are valid there, but
    # abs(voltage) / round(voltage) are not.  SESSION / COUNT_WINDOW / EVENT_WINDOW
    # do not partition at all, so all four scalars raise [0x2667].
    # Valid combos: (UPPER, STATE_WINDOW), (LENGTH, STATE_WINDOW).
    _FW_VALID = {("UPPER", "STATE_WINDOW"), ("LENGTH", "STATE_WINDOW")}
    for pname, pexpr in PUSH_SCALAR_FUNCS[:4]:
        for wname, wexpr, wkw in LOCAL_WINS:
            key = f"genFW_{pname.lower()}_{wname.lower()}"
            if (pname, wname) in _FW_VALID:
                opts = dict(checks_add=["remote_sql"], remote_not_has=[pname, wkw])
            else:
                # TDengine [0x2667]: scalar expression not valid in this window context
                opts = dict(expect_err=True)
            group_dict[key] = [
                (f"select count(*), {pexpr} from {{SRC}}.sensor {wexpr}", opts)]

    # ---------- F_local_indef × W_local (double-local: indef-rows + window) ----------
    for lname, lexpr in LOCAL_INDEF_FUNCS[:4]:
        for wname, wexpr, wkw in LOCAL_WINS[:2]:
            key = f"genLLFW_{lname.lower()}_{wname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor {wexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[lname, wkw]))]

    # ---------- LOCAL_SCALAR × W_local ----------
    # Window requires aggregate; count(*) satisfies the requirement, local_scalar stays local.
    # Exception: MASK_FULL + STATE_WINDOW is valid ([0x2667] only for SESSION/COUNT/EVENT
    # or non-partition-key scalars).  All others get [0x2667] and must be expect_err.
    _LS_W_VALID = {("MASK_FULL", "STATE_WINDOW")}
    for lname, lexpr in LOCAL_SCALAR_FUNCS[:2]:
        for wname, wexpr, wkw in LOCAL_WINS[:2]:
            key = f"genLS_W_{lname.lower()}_{wname.lower()}"
            if (lname, wname) in _LS_W_VALID:
                opts = dict(checks_add=["remote_sql"],
                            remote_not_has=[lname, wkw])
            else:
                # TDengine [0x2667]: scalar expr alongside count(*) in window context
                opts = dict(expect_err=True)
            group_dict[key] = [
                (f"select count(*), {lexpr} from {{SRC}}.sensor {wexpr}", opts)]

    # Positive contrasts for the negative-only LOCAL_SCALAR × W_local buckets:
    # window pseudo columns are legal scalar inputs even when raw row columns are not.
    group_dict["genLS_W_mask_full_session"].append(
        ("select count(*), MASK_FULL(to_iso8601(_wstart), '*') "
         "from {SRC}.sensor session(`time`, 1m)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["MASK_FULL", "TO_ISO8601", "SESSION("],
              local_has={"mysql": ["Projection", "Session"],
                         "pg": ["Projection", "Session"],
                         "influx": ["Projection", "Session"]})))
    group_dict["genLS_W_to_iso8601_state_window"].append(
        ("select count(*), to_iso8601(_wstart) "
         "from {SRC}.sensor state_window(region)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TO_ISO8601", "STATE_WINDOW"],
              local_has={"mysql": ["Projection", "StateWindow"],
                         "pg": ["Projection", "StateWindow"],
                         "influx": ["Projection", "StateWindow"]})))
    group_dict["genLS_W_to_iso8601_session"].append(
        ("select count(*), to_iso8601(_wstart) "
         "from {SRC}.sensor session(`time`, 1m)",
         dict(checks_add=["remote_sql"],
              remote_not_has=["TO_ISO8601", "SESSION("],
              local_has={"mysql": ["Projection", "Session"],
                         "pg": ["Projection", "Session"],
                         "influx": ["Projection", "Session"]})))

    # ---------- LOCAL_AGG × W_local ----------
    for lname, lexpr in LOCAL_AGG_FUNCS[:2]:
        for wname, wexpr, wkw in LOCAL_WINS[:2]:
            key = f"genLA_W_{lname.lower()}_{wname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor {wexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[lname, wkw]))]

    # ---------- A_push × A_local (聚合下推 + 聚合不下推) ----------
    # 只要 SELECT 中有任何一个不可下推的聚合（如 TWA、IRATE），整个聚合就必须
    # 在本地执行（下推聚合会改变行数，导致另一个本地聚合无法正确计算）。
    # 因此远端只拉取原始行，pname 和 lname 都不应出现在 Remote SQL 中。
    for pname, pexpr in PUSH_AGG_FUNCS[:3]:
        for lname, lexpr in LOCAL_AGG_FUNCS[:2]:
            key = f"genAA_{pname.lower()}_{lname.lower()}"
            group_dict[key] = [
                (f"select {pexpr}, {lexpr} from {{SRC}}.sensor",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[pname, lname]))]

    # ---------- 跨类型 SELECT 列表: scalar × agg 组合 ----------
    # TDengine disallows bare scalar columns alongside aggregate functions without
    # GROUP BY (0x2764).  We express this dimension using GROUP BY (scalar as key)
    # or WHERE (scalar as filter), both of which are valid.

    # PUSH_SCALAR × PUSH_AGG (全下推): scalar as GROUP BY key + push_agg in SELECT
    for pname, pexpr in PUSH_SCALAR_FUNCS[:3]:
        for aname, aexpr in PUSH_AGG_FUNCS[:2]:
            key = f"genSA_pp_{pname.lower()}_{aname.lower()}"
            group_dict[key] = [
                (f"select {pexpr}, {aexpr} from {{SRC}}.sensor group by {pexpr}",
                 dict(remote_has=[pname, aname, "GROUP BY"],
                      local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # PUSH_SCALAR × LOCAL_AGG: scalar in WHERE (pushed), local_agg in SELECT (local).
    # Can't put both in SELECT side-by-side without GROUP BY in TDengine.
    _PUSH_SCALAR_WHERE = [
        ("ABS",   "abs(voltage - 220.0) < 5.0"),
        ("UPPER", "upper(region) = 'NORTH'"),
    ]
    for pname, pwhere in _PUSH_SCALAR_WHERE:
        for lname, lexpr in LOCAL_AGG_FUNCS[:2]:
            key = f"genSA_pl_{pname.lower()}_{lname.lower()}"
            group_dict[key] = [
                (f"select {lexpr} from {{SRC}}.sensor where {pwhere}",
                 dict(checks_add=["remote_sql"],
                      remote_has=["WHERE", pname],
                      remote_not_has=[lname]))]

    # LOCAL_SCALAR × LOCAL_AGG (全不下推): scalar as GROUP BY key, both stay local
    for lsname, lsexpr in LOCAL_SCALAR_FUNCS[:2]:
        for laname, laexpr in LOCAL_AGG_FUNCS[:2]:
            key = f"genSA_ll_{lsname.lower()}_{laname.lower()}"
            group_dict[key] = [
                (f"select {lsexpr}, {laexpr} from {{SRC}}.sensor group by {lsexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[lsname, laname]))]

    # ---------- F_push_scalar × F_push_scalar (全标量下推) ----------
    for i, (n1, e1) in enumerate(PUSH_SCALAR_FUNCS[:4]):
        for n2, e2 in PUSH_SCALAR_FUNCS[i + 1:i + 2]:
            key = f"genPP_{n1.lower()}_{n2.lower()}"
            group_dict[key] = [
                (f"select {e1}, {e2} from {{SRC}}.sensor",
                 dict(remote_has=[n1, n2],
                      local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- F_local_scalar × F_local_scalar (全标量不下推) ----------
    for i, (n1, e1) in enumerate(LOCAL_SCALAR_FUNCS):
        for n2, e2 in LOCAL_SCALAR_FUNCS[i + 1:i + 2]:
            key = f"genLL_{n1.lower()}_{n2.lower()}"
            group_dict[key] = [
                (f"select {e1}, {e2} from {{SRC}}.sensor",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[n1, n2]))]

    # ---------- A_push × A_push (全聚合下推) ----------
    for i, (n1, e1) in enumerate(PUSH_AGG_FUNCS[:4]):
        for n2, e2 in PUSH_AGG_FUNCS[i + 1:i + 2]:
            key = f"genPA_{n1.lower()}_{n2.lower()}"
            group_dict[key] = [
                (f"select {e1}, {e2} from {{SRC}}.sensor",
                 dict(remote_has=[n1, n2],
                      local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- A_local × A_local (全聚合不下推) ----------
    # IRATE + STATECOUNT raises [0x264f] (indefinite-rows mixed with multi-row).
    _LA_LA_INCOMPATIBLE = {("IRATE", "STATECOUNT")}
    for i, (n1, e1) in enumerate(LOCAL_AGG_FUNCS):
        for n2, e2 in LOCAL_AGG_FUNCS[i + 1:i + 2]:
            key = f"genLA_{n1.lower()}_{n2.lower()}"
            if (n1, n2) in _LA_LA_INCOMPATIBLE:
                opts = dict(expect_err=True)
            else:
                opts = dict(checks_add=["remote_sql"], remote_not_has=[n1, n2])
            group_dict[key] = [
                (f"select {e1}, {e2} from {{SRC}}.sensor", opts)]
    group_dict["genLA_irate_statecount"].append(
        ("select irate(voltage), twa(voltage) from {SRC}.sensor",
         dict(checks_add=["remote_sql"],
              remote_not_has=["IRATE", "TWA"],
              local_has={"mysql": ["Aggregate"],
                         "pg": ["Aggregate"],
                         "influx": ["Aggregate"]})))

    # ---------- A_push × W_local (聚合 + 本地窗口: 验证聚合不被下推) ----------
    for pname, pexpr in PUSH_AGG_FUNCS[:3]:
        for wname, wexpr, wkw in LOCAL_WINS:
            key = f"genAW_{pname.lower()}_{wname.lower()}"
            group_dict[key] = [
                (f"select {pexpr} from {{SRC}}.sensor {wexpr}",
                 dict(checks_add=["remote_sql"],
                      remote_not_has=[pname, wkw]))]

    # ---------- F_push × C_push ----------
    for pname, pexpr in PUSH_FUNCS:
        for cname, cexpr, ckws in PUSH_CLAUSES:
            if cname == "DISTINCT":
                sql = f"select distinct {pexpr} from {{SRC}}.sensor"
            elif cname == "CASE":
                sql = (f"select case when {pexpr} > 0 then 'a' "
                       f"else 'b' end from {{SRC}}.sensor")
            elif cname in ("GROUP", "HAVING"):
                sql = (f"select region, count({pexpr}) cnt "
                       f"from {{SRC}}.sensor {cexpr}")
            else:
                sql = f"select {pexpr} from {{SRC}}.sensor {cexpr}"
            key = f"genFC_{pname.lower()}_{cname.lower()}"
            opts = {}
            if cname == "CASE":
                if pname in {"UPPER", "MD5"}:
                    opts = dict(
                        remote_has={"mysql": [pname, "IF("],
                                    "influx": [pname, "CASE", "WHEN"]},
                        remote_not_has={"pg": [pname, "CASE"]},
                        local_not_has={
                            "mysql": [_LOCAL_FALLBACK_SENTINEL],
                            "influx": [_LOCAL_FALLBACK_SENTINEL],
                        },
                        local_has={"pg": ["Projection"]},
                    )
                else:
                    opts = dict(
                        remote_has={"mysql": [pname, "IF("],
                                    "pg": [pname, "CASE", "WHEN"],
                                    "influx": [pname, "CASE", "WHEN"]},
                        local_not_has=[_LOCAL_FALLBACK_SENTINEL],
                    )
            else:
                opts = dict(remote_has=[pname] + list(ckws),
                            local_not_has=[_LOCAL_FALLBACK_SENTINEL])
            group_dict[key] = [
                (sql, opts)]

    # ---------- F_local_indef × C_local ----------
    for lname, lexpr in LOCAL_INDEF_FUNCS[:5]:
        for cname, cexpr, ckws in LOCAL_CLAUSES:
            if cname == "SLIMIT":
                sql = f"select {lexpr} from {{SRC}}.sensor {cexpr}"
                forb = list(ckws) + [lname]
                opts = dict(checks_add=["remote_sql"], remote_not_has=forb)
            elif cname == "FILL":
                sql = (f"select _wstart, count({lexpr}) from {{SRC}}.sensor "
                       f"where `time` >= '2024-01-01' and "
                       f"`time` <= '2024-01-02' interval(1m) fill(value, 0)")
                # TDengine rejects aggregate nesting ([0x2627]) for indef funcs.
                opts = dict(expect_err=True)
            else:  # INTERP
                sql = (f"select interp({lexpr}) from {{SRC}}.sensor "
                       f"range('2024-01-01 00:00:00', "
                       f"'2024-01-01 00:05:00') every(1m) fill(linear)")
                # TDengine rejects interp with multi-row functions ([0x264f]).
                opts = dict(expect_err=True)
            key = f"genLLFC_{lname.lower()}_{cname.lower()}"
            group_dict[key] = [(sql, opts)]

    # ---------- LOCAL_SCALAR × C_local (SLIMIT only; FILL/INTERP need agg/ts col) ----------
    for lname, lexpr in LOCAL_SCALAR_FUNCS[:2]:
        sql = f"select {lexpr} from {{SRC}}.sensor partition by region slimit 1"
        key = f"genLS_Cslimit_{lname.lower()}"
        group_dict[key] = [
            (sql, dict(checks_add=["remote_sql"],
                       remote_not_has=["SLIMIT", "PARTITION", lname]))]

    # ---------- LOCAL_AGG × C_local (SLIMIT only) ----------
    for lname, lexpr in LOCAL_AGG_FUNCS[:2]:
        sql = f"select {lexpr} from {{SRC}}.sensor partition by region slimit 1"
        key = f"genLA_Cslimit_{lname.lower()}"
        group_dict[key] = [
            (sql, dict(checks_add=["remote_sql"],
                       remote_not_has=["SLIMIT", "PARTITION", lname]))]

    # ---------- STATECOUNT / HISTOGRAM × C_local (SLIMIT only) ----------
    # STATECOUNT is categorized with LOCAL_AGG_FUNCS for some combo dimensions,
    # but the physical plan uses an indefinite-rows node. HISTOGRAM is a local
    # special aggregate. Add explicit PARTITION BY coverage for both.
    group_dict["genTS_Cslimit_statecount"] = [
        ("select statecount(voltage, 'GT', 220) from {SRC}.sensor partition by region slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SLIMIT", "PARTITION", "STATECOUNT"]))]
    group_dict["genAS_Cslimit_histogram"] = [
        ("select histogram(voltage, 'linear_bin', "
         "'{{\"start\":219,\"width\":1,\"count\":4}}', 0) "
         "from {SRC}.sensor partition by region slimit 1",
         dict(checks_add=["remote_sql"],
              remote_not_has=["SLIMIT", "PARTITION", "HISTOGRAM"]))]

    # ---------- O_push × C_push ----------
    for opname, oexpr in PUSH_OPS:
        for cname, cexpr, ckws in PUSH_CLAUSES[:3]:  # WHERE/ORDER/LIMIT
            if cname == "WHERE":
                # Base SQL already has WHERE {oexpr}; append extra filter
                # with AND instead of a second WHERE keyword.
                extra_cond = cexpr[len("where "):].strip()  # strip "where "
                sql = (f"select * from {{SRC}}.sensor "
                       f"where {oexpr} and {extra_cond}")
            else:
                sql = f"select * from {{SRC}}.sensor where {oexpr} {cexpr}"
            key = f"genOC_{opname.lower()}_{cname.lower()}"
            group_dict[key] = [
                (sql, dict(remote_has=["WHERE"] + list(ckws),
                           local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- W_push (INTERVAL) × C_push ----------
    for cname, cexpr, ckws in [("ORDER", "order by _wstart", ["ORDER BY"]),
                               ("LIMIT", "limit 5",          ["LIMIT"])]:
        sql = (f"select _wstart, count(*) from {{SRC}}.sensor "
               f"where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
               f"interval(1m) {cexpr}")
        key = f"genWC_interval_{cname.lower()}"
        group_dict[key] = [
            (sql, dict(remote_has=["GROUP BY"] + list(ckws),
                       remote_not_has=["INTERVAL("],
                       local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- W_local × C_push ----------
    # STATE_WINDOW partitions by region so "region" in SELECT is valid.
    # SESSION / COUNT_WINDOW / EVENT_WINDOW do not partition by region, so
    # "select count(*), region ... order by region" raises [0x2667].
    _WC_ORDER_VALID = {"STATE_WINDOW"}
    for wname, wexpr, wkw in LOCAL_WINS:
        sql = (f"select count(*), region from {{SRC}}.sensor {wexpr} "
               f"order by region")
        key = f"genWC_{wname.lower()}_order"
        if wname in _WC_ORDER_VALID:
            opts = dict(checks_add=["remote_sql"], remote_not_has=[wkw])
        else:
            # TDengine [0x2667]: region not valid as non-grouped scalar in this window
            opts = dict(expect_err=True)
        group_dict[key] = [(sql, opts)]

    # ---------- triple combos: F_push × O_push × C_push ----------
    for pname, pexpr in PUSH_FUNCS[:3]:
        for opname, oexpr in PUSH_OPS[:3]:
            for cname, cexpr, ckws in [("ORDER", "order by `time`",
                                        ["ORDER BY"]),
                                       ("LIMIT", "limit 3", ["LIMIT"])]:
                sql = (f"select {pexpr} from {{SRC}}.sensor "
                       f"where {oexpr} {cexpr}")
                key = (f"gen3FOC_{pname.lower()}_{opname.lower()}_"
                       f"{cname.lower()}")
                group_dict[key] = [
                    (sql, dict(remote_has=[pname, "WHERE"] + list(ckws),
                               local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # ---------- triple combos: F_local_indef × O_push × C_push ----------
    for lname, lexpr in LOCAL_INDEF_FUNCS[:3]:
        for opname, oexpr in PUSH_OPS[:2]:
            for cname, cexpr, ckws in [("ORDER", "order by `time`",
                                        ["ORDER BY"])]:
                sql = (f"select {lexpr} from {{SRC}}.sensor "
                       f"where {oexpr} {cexpr}")
                key = (f"gen3LOC_{lname.lower()}_{opname.lower()}_"
                       f"{cname.lower()}")
                group_dict[key] = [
                    (sql, dict(checks_add=["remote_sql"],
                               remote_has=["WHERE"] + list(ckws),
                               remote_not_has=[lname]))]

    # ==========================================================================
    # Function nesting dimension
    # Covers: scalar(scalar), agg(scalar), scalar(agg) × (push/local) combos
    # ==========================================================================

    # scalar(scalar) — push_scalar(translatable time scalar): both layers push.
    # to_unixtimestamp(time) is translated per dialect based on the source type:
    #   MySQL   -> CONVERT_TZ(time, client_tz, UTC) then TIMESTAMPDIFF(...)/1000
    #   PG      -> EXTRACT(EPOCH FROM time) * 1000 (sensor.time is TIMESTAMPTZ)
    #   Influx  -> EXTRACT(EPOCH FROM time) * 1000
    group_dict["genNest_push_s_local_s"] = [(
        "select abs(to_unixtimestamp(`time`)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_has={"mysql": ["ABS", "TIMESTAMPDIFF", "CONVERT_TZ"],
                         "pg": ["ABS", "EXTRACT", "EPOCH"],
                         "influx": ["ABS", "EXTRACT", "EPOCH"]},
             local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # scalar(scalar) — local_scalar(push_scalar): outer is local → whole stays local
    group_dict["genNest_local_s_push_s"] = [(
        "select MASK_FULL(upper(region), '*') from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["MASK_FULL", "UPPER"]))]

    # scalar(scalar) — local_scalar(local_scalar): both local
    group_dict["genNest_local_s_local_s"] = [(
        "select MASK_FULL(to_iso8601(`time`), '*') from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["MASK_FULL", "ISO8601"]))]

    # agg(scalar) — push_agg(translatable time scalar): both layers push.
    group_dict["genNest_push_a_local_s"] = [(
        "select avg(to_unixtimestamp(`time`)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_has={"mysql": ["AVG", "TIMESTAMPDIFF", "CONVERT_TZ"],
                         "pg": ["AVG", "EXTRACT", "EPOCH"],
                         "influx": ["AVG", "EXTRACT", "EPOCH"]},
             local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # scalar(agg) — push_scalar(push_agg): both pushed → fully rendered in remote SQL.
    # ABS and AVG are both pushable per DS §5.3.4.1 / §5.3.4.6.
    group_dict["genNest_push_s_push_a"] = [(
        "select abs(avg(voltage)) from {SRC}.sensor",
        dict(remote_has=["ABS", "AVG"],
             local_not_has=[_LOCAL_FALLBACK_SENTINEL]))]

    # scalar(agg) — push_scalar(local_agg): local_agg → scalar cannot be pushed
    group_dict["genNest_push_s_local_a"] = [(
        "select abs(twa(voltage)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["ABS", "TWA"]))]

    # scalar(agg) — local_scalar(push_agg): outer scalar (to_iso8601) is local;
    # inner MIN is pushable per DS §5.3.4.13 and must appear in Remote SQL.
    group_dict["genNest_local_s_push_a"] = [(
        "select to_iso8601(min(`time`)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_has=["MIN"],
             local_has=["Projection"]))]

    # ---------- Missing nesting combinations ----------

    # agg(scalar) — local_agg(push_scalar): local agg wraps a push-able scalar
    group_dict["genNest_local_a_push_s"] = [(
        "select twa(abs(voltage - 220.0)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["TWA"]))]

    # agg(scalar) — local_agg(local_scalar): both local.
    # twa accepts numeric; to_unixtimestamp(time) returns int64 → type-compatible.
    group_dict["genNest_local_a_local_s"] = [(
        "select twa(to_unixtimestamp(`time`)) from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["TWA", "TO_UNIXTIMESTAMP"]))]

    # scalar(agg) — local_scalar(local_agg): both local.
    # first(region) returns varchar; MASK_FULL accepts varchar → type-compatible.
    # first() is TDengine-specific (not in pushable list) so stays local.
    # Use "FIRST(" to avoid false-positive match on "NULLS FIRST" in ORDER BY.
    group_dict["genNest_local_s_local_a"] = [(
        "select MASK_FULL(first(region), '*') from {SRC}.sensor",
        dict(checks_add=["remote_sql"],
             remote_not_has=["MASK_FULL", "FIRST("]))]

    return group_dict


_EXPLAIN_GROUPS.update(_ds_combo_emit(_BucketDict()))


def _recursive_combo_flags():
    """Yield the full 3-layer pushability matrix."""
    for bits in range(8):
        yield (
            (bits & 0b100) != 0,
            (bits & 0b010) != 0,
            (bits & 0b001) != 0,
        )


def _recursive_combo_name(flags):
    return "".join("p" if flag else "n" for flag in flags)


def _dedupe_keep_order(items):
    seen = set()
    result = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _recursive_table_ref(render, table):
    return table if render == "direct" else f"{{SRC}}.{table}"


def _recursive_voltage_chain(flags, render, layer=0):
    # Keep the recursive scalar/quantified generator aligned with the region
    # chain so every layer has a unique, observable push/local marker.
    return _recursive_region_chain(flags, render, layer)


def _recursive_region_chain(flags, render, layer=0):
    alias = f"rr{layer + 1}"
    if layer == 2:
        table = _recursive_table_ref(render, "region_info")
        if flags[layer]:
            expr = "lower(region)"
        else:
            expr = "mask_none(region)"
        return (f"select {expr} as region_l2 from {table} "
                f"where area >= 1 and region is not null")

    inner = _recursive_region_chain(flags, render, layer + 1)
    prev_col = f"region_l{layer + 1}"
    curr_col = f"region_l{layer}"
    if layer == 1:
        if flags[layer]:
            expr = f"trim({prev_col})"
        else:
            expr = f"mask_partial({prev_col}, 1, 1, '#')"
    else:
        if flags[layer]:
            expr = f"substring({prev_col}, 1, length({prev_col}))"
        else:
            expr = f"mask_full({prev_col}, '*')"
    return (f"select {expr} as {curr_col} from ({inner}) {alias} "
            f"where {prev_col} is not null")


def _recursive_from_sql(flags, render):
    inner = _recursive_region_chain(flags, render)
    return (f"select * from "
            f"(select region_l0 as region from ({inner}) fq0 "
            f"where region_l0 is not null) fq1 "
            f"order by fq1.region")


def _recursive_scalar_sql(flags, render):
    outer = _recursive_table_ref(render, "region_info")
    inner = _recursive_region_chain(flags, render)
    return (f"select * from {outer} where region > "
            f"(select max(region_l0) from ({inner}) rs0)")


def _recursive_in_sql(flags, render, negated):
    outer = _recursive_table_ref(render, "sensor")
    inner = _recursive_region_chain(flags, render)
    op = "not in" if negated else "in"
    return (f"select * from {outer} where region {op} "
            f"(select region_l0 from ({inner}) ri0)")


def _recursive_exists_sql(flags, render, negated):
    outer = _recursive_table_ref(render, "sensor")
    inner = _recursive_region_chain(flags, render)
    prefix = "not exists" if negated else "exists"
    return (f"select * from {outer} where {prefix} "
            f"(select 1 from ({inner}) re0 where re0.region_l0 is not null)")


def _recursive_quantified_sql(flags, render, quantifier):
    outer = _recursive_table_ref(render, "region_info")
    inner = _recursive_region_chain(flags, render)
    cmp_op = ">" if quantifier in ("any", "some") else "<="
    return (f"select * from {outer} where region {cmp_op} {quantifier} "
            f"(select region_l0 from ({inner}) rq0)")


_RECURSIVE_FORM_BUILDERS = {
    "from": lambda flags, render: _recursive_from_sql(flags, render),
    "scalar": lambda flags, render: _recursive_scalar_sql(flags, render),
    "in": lambda flags, render: _recursive_in_sql(flags, render, False),
    "not_in": lambda flags, render: _recursive_in_sql(flags, render, True),
    "exists": lambda flags, render: _recursive_exists_sql(flags, render, False),
    "not_exists": lambda flags, render: _recursive_exists_sql(flags, render, True),
    "any": lambda flags, render: _recursive_quantified_sql(flags, render, "any"),
    "all": lambda flags, render: _recursive_quantified_sql(flags, render, "all"),
    "some": lambda flags, render: _recursive_quantified_sql(flags, render, "some"),
}

_RECURSIVE_FORM_KWS = {
    "scalar": ["MAX("],
    "in": [" IN ("],
    "not_in": [" NOT IN ("],
    "exists": ["EXISTS ("],
    "not_exists": ["NOT EXISTS ("],
    "any": [" ANY ("],
    "all": [" ALL ("],
    "some": [" ANY ("],
}

_RECURSIVE_LOCAL_MARKERS = (
    "MASK_FULL(",
    "MASK_PARTIAL(",
    "MASK_NONE(",
)

_RECURSIVE_PUSH_MARKERS = (
    "SUBSTRING(",
    "TRIM(",
    "LOWER(",
)


def _recursive_remote_markers(flags):
    markers = []
    for layer in (2, 1, 0):
        if flags[layer]:
            markers.append(_RECURSIVE_PUSH_MARKERS[layer])
        else:
            break
    return list(reversed(markers))


def _recursive_local_markers(flags):
    remote_markers = set(_recursive_remote_markers(flags))
    return [
        marker for marker in _recursive_chain_markers(flags)
        if marker not in remote_markers
    ]


def _recursive_chain_markers(flags):
    markers = []
    for layer, pushed in enumerate(flags):
        markers.append(_RECURSIVE_PUSH_MARKERS[layer]
                       if pushed else _RECURSIVE_LOCAL_MARKERS[layer])
    return _dedupe_keep_order(markers)


def _recursive_local_plan_kws(form):
    if form == "from":
        return ["Sort ", "Projection"]
    if form == "scalar":
        return ["InitPlan 1", "Aggregate", "Projection"]
    if form in ("in", "not_in", "exists", "not_exists"):
        return ["InitPlan 1", "Projection"]
    return ["InitPlan 1", "Aggregate", "Projection"]


def _recursive_local_not_has_kws(form):
    if form == "from":
        return ["Projection"]
    if form == "scalar":
        return ["InitPlan 1", "Filter:"]
    if form in ("in", "not_in", "exists", "not_exists"):
        return ["InitPlan 1", "Projection"]
    return ["InitPlan 1", "Aggregate", "Projection"]

def _recursive_pushable_labels(form):
    if form == "from":
        return ("mysql", "pg", "influx")
    if form == "scalar":
        return ("mysql", "pg", "influx")
    return ("mysql", "pg")


def _recursive_case_opts(form, flags, render):
    all_push = all(flags)
    opts = {"checks_add": ["remote_sql"]}
    if render == "direct":
        opts["pre_sql"] = ["use {SRC}"]

    kws = list(_RECURSIVE_FORM_KWS.get(form, []))
    remote_markers = _recursive_remote_markers(flags)
    local_markers = _recursive_local_markers(flags)
    local_plan_kws = _recursive_local_plan_kws(form)
    local_not_has_kws = _recursive_local_not_has_kws(form)
    pushable_labels = _recursive_pushable_labels(form)
    nonpush_labels = tuple(
        label for label in ("mysql", "pg", "influx")
        if label not in pushable_labels
    )
    remote_any_has = {}
    remote_not_has = {}
    remote_any_not_has = {}
    local_has = {}
    local_not_has = {}

    if all_push:
        if form == "from":
            remote_any_has = {
                label: list(_RECURSIVE_PUSH_MARKERS) for label in pushable_labels
            }
            local_has = {
                label: ["Sort "] for label in pushable_labels
            }
            local_not_has = list(local_not_has_kws)
        elif kws:
            remote_any_has = {
                label: _dedupe_keep_order(list(kws) + list(_RECURSIVE_PUSH_MARKERS))
                for label in pushable_labels
            }
            local_not_has = {
                label: list(local_not_has_kws)
                for label in pushable_labels
            }
            for label in nonpush_labels:
                remote_any_has[label] = list(_RECURSIVE_PUSH_MARKERS)
                remote_any_not_has[label] = list(kws)
                local_has[label] = list(local_plan_kws)
    else:
        if remote_markers:
            remote_any_has = {
                label: list(remote_markers)
                for label in ("mysql", "pg", "influx")
            }
        if kws:
            remote_any_not_has = {
                label: list(kws) + list(local_markers)
                for label in ("mysql", "pg", "influx")
            }
        else:
            remote_any_not_has = {
                label: list(local_markers)
                for label in ("mysql", "pg", "influx")
            }
        local_has = {
            label: list(local_plan_kws)
            for label in ("mysql", "pg", "influx")
        }
        opts["plain_local_has"] = {
            label: list(local_plan_kws)
            for label in ("mysql", "pg", "influx")
        }
        opts["analyze_local_has"] = {
            label: list(local_plan_kws)
            for label in ("mysql", "pg", "influx")
        }
        if form != "from":
            opts["min_scans"] = 2

    if form == "from" and not all_push:
        opts["remote_any_not_has"] = {
            label: list(local_markers)
            for label in ("mysql", "pg", "influx")
        }

    if remote_any_has:
        opts["remote_any_has"] = remote_any_has
    if remote_not_has:
        opts["remote_not_has"] = remote_not_has
    if remote_any_not_has:
        opts["remote_any_not_has"] = remote_any_not_has
    if local_has:
        opts["local_has"] = local_has
    if local_not_has:
        opts["local_not_has"] = local_not_has

    if form in ("scalar", "from") and all_push:
        opts["checks_add"].append("subquery_handling")
    if local_not_has and "local_not_has" not in opts["checks_add"]:
        opts["checks_add"].append("local_not_has")
    if "analyze_local_has" in opts and "analyze_local_has" not in opts["checks_add"]:
        opts["checks_add"].append("analyze_local_has")
    if "min_scans" in opts and "min_federated_scans" not in opts["checks_add"]:
        opts["checks_add"].append("min_federated_scans")
    return opts


def _emit_recursive_subquery_cases():
    """Build recursive subquery cases with bare-table vs qualified-path coverage."""
    group_dict = _BucketDict()
    forms = (
        "from", "scalar", "in", "not_in", "exists", "not_exists", "any", "all", "some",
    )

    for flags in _recursive_combo_flags():
        combo_name = _recursive_combo_name(flags)
        for render in ("direct", "qualified"):
            for form in forms:
                sql = _RECURSIVE_FORM_BUILDERS[form](flags, render)
                key = f"dsSubq_recursive_{form}_{combo_name}_{render}"
                group_dict[key] = [(sql, _recursive_case_opts(form, flags, render))]

    return group_dict


def _extend_group_entries(dst, src):
    for grp, entries in src.items():
        dst.setdefault(grp, []).extend(entries)


_extend_group_entries(_EXPLAIN_GROUPS, _emit_recursive_subquery_cases())


def _per_src(val, label):
    """Resolve a per-source expectation value for the given source label.

    Expectation fields (remote_has / remote_not_has / soft_remote_has /
    local_not_has / expect_err / expect_err_contains) accept two shapes:
      * list / bool  — same expectation for all three sources (legacy).
      * dict {"mysql": [...], "pg": [...], "influx": [...]} — per-source;
        a missing source key means "no expectation" for that source
        ([] for keyword lists, False for expect_err).

    Since all SQL is TDengine syntax executed against every source, the only
    legitimate cross-source difference is the EXPLAIN result, expressed via
    this per-source dict.
    """
    if isinstance(val, dict):
        if label in val:
            return val[label]
        # missing key → empty expectation; bool fields default False
        return False if any(isinstance(v, bool) for v in val.values()) else []
    return val


# SQL Server character semantics: TDengine compares NCHAR as UCS4 code points
# and VARCHAR as raw bytes, while T-SQL uses the column collation, UTF-16 code
# units and the code page.  The semantic-equivalence gate keeps these
# operations local (or rewrites equality/LIKE exactly); their placement and
# result parity are covered by test_fq_20_sqlserver_semantics.py, so the generic
# pushdown matrix skips the SQL Server leg for these SQL shapes instead of
# asserting the old (semantically unsafe) remote form.
_SS_LOCAL_CHAR_RE = re.compile(
    r"\b(?:upper|lower|char_length|substr|substring|trim|ltrim|rtrim|replace|"
    r"position|group_concat|repeat)\s*\("
    r"|\blength\s*\(\s*(?:region|json_str|date_str|b64_str|geom_wkt)\s*\)"
    r"|\bascii\s*\(\s*(?:region|json_str|date_str|b64_str|geom_wkt)\s*\)"
    r"|\b(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\s+"
    r"(?:not\s+)?(?:in|between)\b"
    r"|\b(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\s*"
    r"(?:<|>|<=|>=)"
    r"|\bgroup\s+by\s+(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\b"
    r"|\border\s+by\s+(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\b"
    r"|\bdistinct\s+(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\b"
    r"|\b(?:min|max)\s*\(\s*(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\s*\)"
    r"|\bpartition\s+by\s+(?:region|region_ascii|json_str|date_str|b64_str|geom_wkt)\b"
    r"|\bcast\s*\([^)]*\bas\s+(?:varchar|nchar|nvarchar)\b"
    r"|\blike\s+'[^']*_[^']*'"
)

_EXPLAIN_CASES = [{'id': 'EXP-op_arith-01',
  'sql': 'select voltage + 1.0 from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['+'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-op_arith-02',
  'sql': 'select voltage - 1.0 from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['-'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-op_arith-03',
  'sql': 'select voltage * 2.0 from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['*'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-op_arith-04',
  'sql': 'select voltage / 2.0 from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['/'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-op_arith-05',
  'sql': 'select area % 2 from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['%'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-op_compare_eq-01',
  'sql': 'select * from {SRC}.sensor where voltage = 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '='], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_ne-01',
  'sql': 'select * from {SRC}.sensor where voltage <> 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_lt-01',
  'sql': 'select * from {SRC}.sensor where voltage < 222.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '<'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_gt-01',
  'sql': 'select * from {SRC}.sensor where voltage > 219.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '>'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_between-01',
  'sql': 'select * from {SRC}.sensor where voltage between 219.0 and 222.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'BETWEEN'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_in-01',
  'sql': 'select * from {SRC}.sensor where voltage in (220.0, 221.0)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'IN'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_like-01',
  'sql': "select * from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIKE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_isnull-01',
  'sql': 'select * from {SRC}.sensor where region is null',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'NULL'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_compare_isnull-02',
  'sql': 'select * from {SRC}.sensor where region is not null',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'NULL'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_logical_and-01',
  'sql': "select * from {SRC}.sensor where voltage > 219.0 and region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'AND'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_logical_or-01',
  'sql': "select * from {SRC}.sensor where voltage > 222.0 or region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'OR'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_logical_not-01',
  'sql': 'select * from {SRC}.sensor where not (voltage > 222.0)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'NOT'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-op_bitwise-01',
  'sql': 'select area & 1 from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['&'], 'pg': ['&'], 'sqlserver': ['&']},
                 'remote_not_has': {'influx': [' & ']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'],
                                   'sqlserver': ['Projection']}}},
 {'id': 'EXP-op_bitwise-02',
  'sql': 'select area | 1 from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['|'], 'pg': ['|'], 'sqlserver': ['|']},
                 'remote_not_has': {'influx': [' | ']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'],
                                   'sqlserver': ['Projection']}}},
 {'id': 'EXP-op_bitwise_cast-01',
  'sql': 'select voltage from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['WHERE', 'CAST', '&'], 'pg': ['WHERE', 'CAST', '&'],
                                'sqlserver': ['WHERE', 'CAST', '&']},
                 'remote_not_has': {'influx': [' & ', ' | ']},
                 'local_has': {'influx': []},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'],
                                   'sqlserver': ['Projection']}}},
 {'id': 'EXP-func_math_abs-01',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_math_ceil-01',
  'sql': 'select ceil(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CEIL'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_math_floor-01',
  'sql': 'select floor(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['FLOOR'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_math_round-01',
  'sql': 'select round(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_math_sqrt-01',
  'sql': 'select sqrt(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SQRT'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_math_pow-01',
  'sql': 'select pow(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['POW'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_upper-01',
  'sql': 'select upper(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_lower-01',
  'sql': 'select lower(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LOWER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_concat-01',
  'sql': "select concat(region, '_x') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CONCAT'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_trim-01',
  'sql': 'select trim(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['TRIM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_length-01',
  'sql': 'select length(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['CHAR_LENGTH', '* 4'],
                                'pg': ['CHAR_LENGTH', '* 4'],
                                'influx': ['CHAR_LENGTH', '* 4'], 'sqlserver': ['LEN(', '* 4']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_length-02',
  'sql': 'select length(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_LENGTH_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['LENGTH'],
                                'pg_ascii': ['OCTET_LENGTH'], 'sqlserver': ['DATALENGTH']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_length-03',
  'sql': 'select length(region_ascii) from {SRC}.region_info',
  'sources': 'custom',
  'source_infos': _ASCII_LENGTH_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['LENGTH'],
                                'pg_ascii': ['OCTET_LENGTH'], 'sqlserver': ['DATALENGTH']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_substring-01',
  'sql': 'select substring(region, 1, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUBSTRING'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_str_to_base64-01',
  'sql': 'select to_base64(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['TO_BASE64', 'ENCODE'],
                                    'pg': ['TO_BASE64', 'ENCODE'],
                                    'influx': ['TO_BASE64', 'ENCODE'], 'sqlserver': ['TO_BASE64', 'ENCODE']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-func_str_to_base64-02',
  'sql': 'select to_base64(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'local_has': {'sqlserver': ['Projection']}, 'remote_not_has': {'sqlserver': ['TO_BASE64', 'ENCODE']}, 'remote_has': {'mysql_ascii': ['TO_BASE64'],
                                'pg_ascii': ['ENCODE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_hash_sha1-01',
  'sql': 'select sha1(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['SHA1'],
                                    'pg': ['SHA1'],
                                    'influx': ['SHA1'], 'sqlserver': ['SHA1', 'HASHBYTES']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-func_hash_sha1-02',
  'sql': 'select sha1(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['SHA1'], 'sqlserver': ['HASHBYTES', 'SHA1']},
                 'remote_not_has': {'pg_ascii': ['SHA1']},
                 'local_has': {'pg_ascii': ['Projection']},
                 'local_not_has': {'mysql_ascii': ['Projection']}}},
 {'id': 'EXP-func_hash_crc32-01',
  'sql': 'select crc32(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['CRC32'],
                                    'pg': ['CRC32'],
                                    'influx': ['CRC32'], 'sqlserver': ['CRC32', 'CHECKSUM']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-func_hash_crc32-02',
  'sql': 'select crc32(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['CRC32']},
                 'remote_not_has': {'pg_ascii': ['CRC32'], 'sqlserver': ['CRC32', 'CHECKSUM']},
                 'local_has': {'pg_ascii': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql_ascii': ['Projection']}}},
 {'id': 'EXP-func_cast-01',
  'sql': 'select cast(voltage as varchar(32)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_time_now-01',
  'sql': 'select now() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_hash_md5-01',
  'sql': 'select md5(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['MD5'],
                                    'pg': ['MD5'],
                                    'influx': ['MD5'], 'sqlserver': ['MD5', 'HASHBYTES']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-func_hash_md5-02',
  'sql': 'select md5(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['MD5'],
                                'pg_ascii': ['MD5'], 'sqlserver': ['HASHBYTES', 'MD5']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_agg_count-01',
  'sql': 'select count(*) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['COUNT'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_sum-01',
  'sql': 'select sum(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUM'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_avg-01',
  'sql': 'select avg(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_min-01',
  'sql': 'select min(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MIN'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_max-01',
  'sql': 'select max(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MAX'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_stddev-01',
  'sql': 'select stddev_pop(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['STDDEV_POP'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_agg_var-01',
  'sql': 'select var_pop(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['VAR_POP'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-func_coalesce-01',
  'sql': "select coalesce(region, 'unknown') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['COALESCE'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_nullif-01',
  'sql': 'select nullif(voltage, 220.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['NULLIF'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-func_local_csum-01',
  'sql': 'select csum(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-func_local_diff-01',
  'sql': 'select diff(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-func_local_derivative-01',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-func_local_twa-01',
  'sql': 'select twa(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-func_local_first-01',
  'sql': 'select first(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['FIRST('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-func_local_last-01',
  'sql': 'select last(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LAST('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-func_local_top-01',
  'sql': 'select top(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TOP('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-func_local_bottom-01',
  'sql': 'select bottom(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['BOTTOM('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-func_local_elapsed-01',
  'sql': 'select elapsed(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['ELAPSED'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-win_interval-01',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-win_local_state-01',
  'sql': 'select count(*), region from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-win_local_session-01',
  'sql': 'select count(*) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-win_local_count-01',
  'sql': 'select count(*) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'], 'pg': ['Count'], 'influx': ['Count']}}},
 {'id': 'EXP-win_local_event-01',
  'sql': 'select count(*) from {SRC}.sensor event_window start with voltage > 220.0 end with '
         'voltage < 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'], 'pg': ['Event'], 'influx': ['Event']}}},
 {'id': 'EXP-join_inner-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-join_inner-02',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s, {SRC}.region_info r where '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'single_federated_scan',
             'no_table_scan',
             'local_not_has'],
  'check_args': {'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-join_left-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s left join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['LEFT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-join_right-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s right join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['RIGHT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-join_full-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s full join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['FULL', 'JOIN'], 'influx': ['FULL', 'JOIN'], 'sqlserver': ['FULL', 'JOIN']},
                 'remote_not_has': {'mysql': ['FULL']},
                 'local_has': {'mysql': ['Projection', 'Full Join']},
                 'local_not_has': {'influx': ['Projection', 'Full Join', 'Data Exchange'],
                                   'pg': ['Projection', 'Full Join', 'Data Exchange'], 'sqlserver': ['Projection', 'Full Join', 'Data Exchange']}}},
 {'id': 'EXP-join_cross_source-01',
  'sql': 'select m.`time`, m.voltage, p.current from {SRC}.sensor m join {SRC2}.sensor p on '
         'm.`time` = p.`time` and m.region = p.region',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_any_count_eq',
             'remote_any_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'min_scans': 2,
                 'remote_any_count_eq': [('Remote SQL:', 2)],
                 'remote_any_not_has': ['JOIN'],
                 'local_has': {'mysql_pg': ['Projection', 'Inner Join', 'Data Exchange'],
                               'mysql_influx': ['Projection', 'Inner Join'],
                               'pg_influx': ['Projection', 'Inner Join'], 'mysql_sqlserver': ['Projection', 'Inner Join', 'Data Exchange'], 'pg_sqlserver': ['Projection', 'Inner Join', 'Data Exchange'], 'influx_sqlserver': ['Projection', 'Inner Join']},
                 'local_not_has': {'mysql_pg': [],
                                   'mysql_influx': ['Data Exchange'],
                                   'pg_influx': ['Data Exchange'], 'mysql_sqlserver': [], 'pg_sqlserver': [], 'influx_sqlserver': ['Data Exchange']}}},
 {'id': 'EXP-join_inner_with_where-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region where s.voltage > 220.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN', 'WHERE'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Filter:', 'Projection']}},
 {'id': 'EXP-join_inner_order_limit-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region order by s.`time` limit 5',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Sort', 'limit=', 'Projection']}},
 {'id': 'EXP-join_left_with_groupby-01',
  'sql': 'select r.area, count(s.voltage) from {SRC}.sensor s left join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region group by r.area',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['LEFT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'GroupAggregate', 'Projection']}},
 {'id': 'EXP-subq_nested-01',
  'sql': 'select * from (select `time`, voltage from {SRC}.sensor where voltage > 220.0) t order '
         'by t.`time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Sort']}},
 {'id': 'EXP-subq_scalar-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['AVG('], 'local_not_has': ['InitPlan 1']}},
 {'id': 'EXP-subq_in-01',
  'sql': 'select * from {SRC}.sensor where region in (select region from {SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_in_self-01',
  'sql': 'select * from {SRC}.sensor where region in (select region from {SRC}.sensor where '
         'voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_not_in-01',
  'sql': 'select * from {SRC}.sensor where region not in (select region from {SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_not_in_self-01',
  'sql': 'select * from {SRC}.sensor where region not in (select region from {SRC}.sensor where '
         'voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
{'id': 'EXP-subq_exists-01',
  'sql': 'select * from {SRC}.sensor s where exists (select 1 from {SRC}.region_info r where '
         'r.region = s.region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Not supported subquery as expr']},
  'expect_err': True},
 {'id': 'EXP-subq_exists-02',
  'sql': 'select * from {SRC}.sensor s where exists (select 1 from {SRC}.sensor r where r.voltage '
         '> 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['EXISTS'], 'pg': ['EXISTS'], 'sqlserver': ['EXISTS']},
                 'remote_not_has': {'influx': ['EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
{'id': 'EXP-subq_not_exists-01',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.region_info r where '
         'r.region = s.region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Not supported subquery as expr']},
  'expect_err': True},
 {'id': 'EXP-subq_not_exists-02',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.sensor r where '
         'r.voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT', 'EXISTS'], 'pg': ['NOT', 'EXISTS'], 'sqlserver': ['NOT', 'EXISTS']},
                 'remote_not_has': {'influx': ['NOT EXISTS', 'NOT  EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_exists_nocorr-01',
  'sql': 'select * from {SRC}.sensor s where exists (select 1 from {SRC}.sensor r where r.voltage '
         '> 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['EXISTS'], 'pg': ['EXISTS'], 'sqlserver': ['EXISTS']},
                 'remote_not_has': {'influx': ['EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_not_exists_nocorr-01',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.sensor r where '
         'r.voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT', 'EXISTS'], 'pg': ['NOT', 'EXISTS'], 'sqlserver': ['NOT', 'EXISTS']},
                 'remote_not_has': {'influx': ['NOT EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_any-01',
  'sql': 'select * from {SRC}.sensor where voltage > any (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_all-01',
  'sql': 'select * from {SRC}.sensor where voltage > all (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_some-01',
  'sql': 'select * from {SRC}.sensor where voltage > some (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-subq_nested_direct-01',
  'sql': 'select * from (select `time`, voltage from sensor where voltage > 220.0) t order by '
         't.`time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Sort']},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_scalar_direct-01',
  'sql': 'select `time`, voltage from sensor where voltage > (select avg(voltage) from sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['AVG('], 'local_not_has': ['InitPlan 1']},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_in_direct-01',
  'sql': 'select * from sensor where region in (select region from region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_in_self_direct-01',
  'sql': 'select * from sensor where region in (select region from sensor where voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_not_in_direct-01',
  'sql': 'select * from sensor where region not in (select region from region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_not_in_self_direct-01',
  'sql': 'select * from sensor where region not in (select region from sensor where voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_exists_nocorr_direct-01',
  'sql': 'select * from sensor s where exists (select 1 from sensor r where r.voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['EXISTS'], 'pg': ['EXISTS'], 'sqlserver': ['EXISTS']},
                 'remote_not_has': {'influx': ['EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_not_exists_nocorr_direct-01',
  'sql': 'select * from sensor s where not exists (select 1 from sensor r where r.voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT', 'EXISTS'], 'pg': ['NOT', 'EXISTS'], 'sqlserver': ['NOT', 'EXISTS']},
                 'remote_not_has': {'influx': ['NOT EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_any_direct-01',
  'sql': 'select * from sensor where voltage > any (select voltage from sensor where region = '
         "'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_all_direct-01',
  'sql': 'select * from sensor where voltage > all (select voltage from sensor where region = '
         "'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-subq_some_direct-01',
  'sql': 'select * from sensor where voltage > some (select voltage from sensor where region = '
         "'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-distinct-01',
  'sql': 'select distinct region from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['DISTINCT'], 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-group_by-01',
  'sql': 'select region, count(*) from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'], 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-having-01',
  'sql': 'select region, count(*) as cnt from {SRC}.sensor group by region having count(*) > 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-order_by-01',
  'sql': 'select `time`, voltage from {SRC}.sensor order by voltage desc',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ORDER BY'], 'local_not_has': ['Sort ']}},
 {'id': 'EXP-order_by_nulls_last-01',
  'sql': 'select `time`, voltage from {SRC}.sensor order by voltage desc nulls last',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': {'pg': ['ORDER BY', 'NULLS LAST'],
                                'influx': ['ORDER BY', 'NULLS LAST'],
                                'mysql': ['ORDER BY', 'ISNULL'], 'sqlserver': ['ORDER BY [VOLTAGE] DESC']},
                 'remote_not_has': {'mysql': ['NULLS LAST'], 'sqlserver': ['NULLS LAST']},
                 'local_not_has': ['Sort ']}},
 {'id': 'EXP-order_by_nulls_first-01',
  'sql': 'select `time`, voltage from {SRC}.sensor order by voltage asc nulls first',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ORDER BY', 'ISNULL'],
                                'pg': ['ORDER BY', 'NULLS FIRST'],
                                'influx': ['ORDER BY', 'NULLS FIRST'], 'sqlserver': ['ORDER BY [VOLTAGE] ASC']},
                 'remote_not_has': {'mysql': ['NULLS FIRST', 'NULLS LAST'], 'sqlserver': ['NULLS FIRST', 'CASE WHEN']},
                 'local_not_has': ['Sort ']}},
 {'id': 'EXP-limit_only-01',
  'sql': 'select `time`, voltage from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-limit_offset-01',
  'sql': 'select `time`, voltage from {SRC}.sensor order by `time` limit 2 offset 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ORDER BY', 'LIMIT', 'OFFSET'], 'local_not_has': ['Sort ']}},
 {'id': 'EXP-limit_partition_local-01',
  'sql': 'select `time`, voltage from {SRC}.sensor partition by region limit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LIMIT'], 'local_has': ['Projection', 'Partition']}},
 {'id': 'EXP-slimit_local-01',
  'sql': 'select `time`, voltage from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT'],
                 'local_has': {'mysql': ['Projection', 'Partition'],
                               'pg': ['Projection', 'Partition'],
                               'influx': ['Projection', 'Partition']}}},
 {'id': 'EXP-union_all-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 221.0 union all select `time`, '
         'voltage from {SRC}.sensor where voltage < 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION'],
                 'local_not_has': ['Data Exchange', 'Filter:', 'Projection']}},
 {'id': 'EXP-union_all_stmt_limit-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 221.0 union all select `time`, '
         'voltage from {SRC}.sensor where voltage < 220.0 limit 10',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_not_has',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['UNION', 'LIMIT'], 'pg': ['UNION', 'LIMIT'],
                                'influx': ['UNION', 'LIMIT'],
                                # The TOP form of a LIMIT counts per UNION
                                # branch, so SQL Server keeps it local.
                                'sqlserver': ['UNION']},
                 'remote_any_not_has': ['_sq LIMIT 10'],
                 'local_not_has': {'mysql': ['Data Exchange', 'Filter:', 'limit=', 'Projection'],
                                   'pg': ['Data Exchange', 'Filter:', 'limit=', 'Projection'],
                                   'influx': ['Data Exchange', 'Filter:', 'limit=', 'Projection'],
                                   'sqlserver': ['Data Exchange', 'Filter:', 'Projection']}}},
 {'id': 'EXP-union_all_outer_limit-01',
  'sql': 'select * from (select `time`, voltage from {SRC}.sensor where voltage > 221.0 union all '
         'select `time`, voltage from {SRC}.sensor where voltage < 220.0) t limit 10',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['UNION', 'LIMIT'], 'pg': ['UNION', 'LIMIT'],
                                'influx': ['UNION', 'LIMIT'],
                                # TOP-style LIMIT over a set-op stays local.
                                'sqlserver': ['UNION']},
                 'remote_any_has': {'mysql': ['_sq LIMIT 10'], 'pg': ['_sq LIMIT 10'],
                                    'influx': ['_sq LIMIT 10'], 'sqlserver': []},
                 'local_not_has': {'mysql': ['InitPlan 1', 'Data Exchange', 'Filter:', 'limit='],
                                   'pg': ['InitPlan 1', 'Data Exchange', 'Filter:', 'limit='],
                                   'influx': ['InitPlan 1', 'Data Exchange', 'Filter:', 'limit='],
                                   'sqlserver': ['InitPlan 1', 'Data Exchange', 'Filter:']}}},
 {'id': 'EXP-union_all_outer_order_limit-01',
  'sql': 'select `time`, voltage from (select `time`, voltage from {SRC}.sensor where voltage > '
         '221.0 union all select `time`, voltage from {SRC}.sensor where voltage < 220.0) t order '
         'by t.`time` limit 10',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['UNION', 'ORDER BY', 'LIMIT'],
                 # SQL Server renders the pushed LIMIT as
                 # OFFSET n ROWS FETCH NEXT m ROWS ONLY.
                 'remote_any_has': {'mysql': ['_sq ORDER BY', 'LIMIT 10'],
                                    'pg': ['_sq ORDER BY', 'LIMIT 10'],
                                    'influx': ['_sq ORDER BY', 'LIMIT 10'],
                                    'sqlserver': ['_sq ORDER BY', 'FETCH NEXT 10']},
                 'local_not_has': ['InitPlan 1',
                                   'Data Exchange',
                                   'Filter:',
                                   'Sort',
                                   'limit=',
                                   'Projection']}},
 {'id': 'EXP-union_all_both_groupby-01',
  'sql': 'select region, count(*) from {SRC}.sensor where voltage > 221.0 group by region union '
         'all select region, count(*) from {SRC}.sensor where voltage < 220.0 group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION', 'GROUP BY'],
                 'local_not_has': ['Data Exchange', 'Filter:', 'GroupAggregate', 'Projection']}},
 {'id': 'EXP-union_all_one_branch_local-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 221.0 union all select _wstart, '
         "count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         'interval(1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  # SQL Server at the M1 stage: the INTERVAL branch is not pushable (AGG /
  # INTERVAL clauses land in M2/M4), so the set-op cannot merge; each branch
  # still pushes what M1 allows - the filter on branch 1, and UNION stays out.
  'check_args': {'remote_has': {'mysql': ['UNION'], 'pg': ['UNION'], 'influx': ['UNION'],
                                'sqlserver': ['WHERE ([voltage] > 221.0)']},
                 'remote_not_has': {'sqlserver': ['UNION']},
                 'local_not_has': ['Data Exchange', 'Filter:', 'Interval', 'Projection']}},
 {'id': 'EXP-union_all_both_local-01',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-01 01:00:00' interval(1m) union all select _wstart, count(*) from {SRC}.sensor "
         "where `time` >= '2024-01-02' and `time` <= '2024-01-02 01:00:00' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  # Both branches are INTERVAL aggregates: for SQL Server at M1 they stay
  # local (AGG/INTERVAL land in M2/M4), so only the pushed ts range reaches
  # the remote statement - no UNION.
  'check_args': {'remote_has': {'mysql': ['UNION'], 'pg': ['UNION'], 'influx': ['UNION'],
                                'sqlserver': ['BETWEEN']},
                 'remote_not_has': {'sqlserver': ['UNION', 'GROUP BY']},
                 'local_not_has': ['Data Exchange',
                                   'Filter:',
                                   'Interval',
                                   'Aggregate',
                                   'Projection']}},
 {'id': 'EXP-union_all_cross-01',
  'sql': 'select `time`, voltage from {SRC}.sensor union all select `time`, voltage from '
         '{SRC2}.sensor',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['UNION'],
                 'min_scans': 2,
                 'local_has': {'mysql_pg': ['Data Exchange'],
                               'mysql_influx': ['Data Exchange', 'Projection'],
                               'pg_influx': ['Data Exchange', 'Projection']}}},
 {'id': 'EXP-union_dedup-01',
  'sql': 'select region from {SRC}.sensor where voltage > 221.0 union select region from '
         '{SRC}.sensor where voltage < 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION'],
                 'local_not_has': ['Data Exchange', 'GroupAggregate', 'Filter:', 'Projection']}},
 {'id': 'EXP-union_dedup_groupby-01',
  'sql': 'select region, count(*) as cnt from {SRC}.sensor where voltage > 221.0 group by region '
         'union select region, count(*) as cnt from {SRC}.sensor where voltage < 220.0 group by '
         'region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION', 'GROUP BY'],
                 'local_not_has': ['Data Exchange', 'GroupAggregate', 'Filter:', 'Projection']}},
 {'id': 'EXP-union_dedup_one_branch_local-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 221.0 union select _wstart, '
         "count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= '2024-01-02' "
         'interval(1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION', 'GROUP BY'],
                 'local_not_has': ['Data Exchange',
                                   'GroupAggregate',
                                   'Filter:',
                                   'Interval',
                                   'Projection']}},
 {'id': 'EXP-union_dedup_cross-01',
  'sql': 'select region from {SRC}.sensor union select region from {SRC2}.sensor',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['UNION'],
                 'min_scans': 2,
                 'local_has': {'mysql_pg': ['GroupAggregate', 'Data Exchange'],
                               'mysql_influx': ['GroupAggregate', 'Data Exchange'],
                               'pg_influx': ['GroupAggregate', 'Data Exchange']}}},
 {'id': 'EXP-case_expr-01',
  'sql': "select case when voltage > 220.0 then 'high' else 'low' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IF('],
                                'pg': ['CASE', 'WHEN'],
                                'influx': ['CASE', 'WHEN'], 'sqlserver': ['CASE', 'WHEN']},
                 'local_not_has': ['Projection']}},
{'id': 'EXP-fill_local-01',
  'sql': "select _wstart, avg(voltage) from {SRC}.sensor where `time` >= '2024-01-01' and `time` "
         "<= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['SELECT `time`, `voltage`', 'ORDER BY `time` ASC'],
                                'pg': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'],
                                'influx': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME], [VOLTAGE]', 'ORDER BY [TIME] ASC']},
                 'remote_not_has': ['AVG(', 'GROUP BY', 'FILL', 'INTERVAL('],
                 'local_has': ['Fill', 'Interval']}},
 {'id': 'EXP-interp_local-01',
  'sql': "select interp(voltage) from {SRC}.sensor range('2024-01-01 00:00:00', '2024-01-01 "
         "00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['INTERP'],
                 'local_has': {'mysql': ['Interp'], 'pg': ['Interp'], 'influx': ['Interp']}}},
 {'id': 'EXP-basic-01',
  'sql': 'select * from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'],
                 'local_not_has': ['Projection', 'Aggregate', 'Sort', 'Data Exchange']}},
 {'id': 'EXP-basic-02',
  'sql': "select `time`, voltage from {SRC}.sensor where `time` > '2024-01-01'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-basic-03',
  'sql': 'select * from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'federated_scan_on',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'],
                 'local_not_has': ['Projection', 'Aggregate', 'Sort', 'Data Exchange']}},
 {'id': 'EXP-verbose-01',
  'sql': 'select `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'columns_list',
             'projection_no_star',
             'output_cols_eq',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['`time`', '`voltage`'],
                                'pg': ['"time"', '"voltage"'],
                                'influx': ['"time"', '"voltage"'], 'sqlserver': ['[time]', '[voltage]']},
                 'local_not_has': ['Projection'],
                 'output_cols_eq': 2}},
{'id': 'EXP-verbose-02',
  'sql': "select `time`, voltage from {SRC}.sensor where `time` > '2024-01-01' order by `time` "
         'limit 10',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Filter:', 'Sort', 'limit=', 'Projection']}},
 {'id': 'EXP-verbose-03',
  'sql': 'select `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'columns_list',
             'output_cols_eq',
             'remote_has',
             'local_not_has'],
  'check_args': {'output_cols_eq': 2,
                 'remote_has': {'mysql': ['`time`', '`voltage`'],
                                'pg': ['"time"', '"voltage"'],
                                'influx': ['"time"', '"voltage"'], 'sqlserver': ['[time]', '[voltage]']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-dialect-01',
  'sql': 'select `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'dialect_backtick',
             'dialect_double_quote',
             'dialect_bracket',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['`time`', '`voltage`'],
                                'pg': ['"time"', '"voltage"'],
                                'influx': ['"time"', '"voltage"'],
                                'sqlserver': ['[time]', '[voltage]']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-typemap-01',
  'sql': 'select `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'columns_list',
             'projection_no_star',
             'output_cols_eq',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['`time`', '`voltage`'],
                                'pg': ['"time"', '"voltage"'],
                                'influx': ['"time"', '"voltage"'], 'sqlserver': ['[time]', '[voltage]']},
                 'local_not_has': ['Projection'],
                 'output_cols_eq': 2}},
 {'id': 'EXP-typemap-02',
  'sql': 'select * from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'columns_list',
             'projection_no_star',
             'output_cols_eq',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['`time`',
                                          '`voltage`',
                                          '`current`',
                                          '`region`',
                                          '`date_str`',
                                          '`json_str`',
                                          '`b64_str`',
                                          '`geom_wkt`'],
                                'pg': ['"time"',
                                       '"voltage"',
                                       '"current"',
                                       '"region"',
                                       '"date_str"',
                                       '"json_str"',
                                       '"b64_str"',
                                       '"geom_wkt"'],
                                'influx': ['"time"',
                                           '"voltage"',
                                           '"current"',
                                           '"region"',
                                           '"date_str"',
                                           '"json_str"',
                                           '"b64_str"',
                                           '"geom_wkt"'], 'sqlserver': ['[time]', '[voltage]', '[current]', '[region]', '[date_str]', '[json_str]', '[b64_str]', '[geom_wkt]']},
                 'local_not_has': ['Projection', 'Aggregate', 'Sort', 'Data Exchange'],
                 'output_cols_eq': 8}},
 {'id': 'EXP-security-01',
  'sql': 'select * from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'no_data_leak', 'remote_has', 'local_not_has'],
  'check_args': {'no_data_leak': ['220.5', '221.0', '219.8', '222.0', '220.0'],
                 'remote_has': ['SELECT', 'FROM'],
                 'local_not_has': ['Projection', 'Aggregate', 'Sort', 'Data Exchange']}},
 {'id': 'EXP-projection-01',
  'sql': 'select `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'projection_no_star',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['`time`', '`voltage`'],
                                'pg': ['"time"', '"voltage"'],
                                'influx': ['"time"', '"voltage"'], 'sqlserver': ['[time]', '[voltage]']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-projection-02',
  'sql': 'select * from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'], 'local_not_has': ['Sort ', 'Agg']}},
 {'id': 'EXP-compound_where-01',
  'sql': "select `time`, voltage from {SRC}.sensor where (voltage > 220.0 and region = 'north') or "
         'current < 1.2',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'compound_where', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-full_pushdown-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 220.0 order by `time` limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY', 'LIMIT'], 'local_not_has': ['Sort ']}},
 {'id': 'EXP-full_pushdown-02',
  'sql': "select `time`, voltage from {SRC}.sensor where `time` >= '2024-01-01' order by `time` "
         'limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY', 'LIMIT'], 'local_not_has': ['Sort ']}},
 {'id': 'EXP-where_only-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['LIMIT'],
                 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-order_only-01',
  'sql': 'select `time`, voltage from {SRC}.sensor order by voltage desc',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['ORDER BY'],
                 'remote_not_has': ['LIMIT'],
                 'local_not_has': ['Sort ']}},
 {'id': 'EXP-where_order_no_limit-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 219.0 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['LIMIT'],
                 'local_not_has': ['Sort ']}},
 {'id': 'EXP-combo_FF_PP_select-01',
  'sql': 'select abs(voltage), upper(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'UPPER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-combo_FF_PL_select-01',
  'sql': 'select abs(voltage - 220.0), csum(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function'],
                               'pg': ['Projection', 'Indefinite Rows Function'],
                               'influx': ['Projection', 'Indefinite Rows Function']}}},
{'id': 'EXP-combo_FF_LL_select-01',
  'sql': 'select csum(voltage), diff(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Multiple indefinite rows functions with different return rows']},
  'expect_err': True},
 {'id': 'EXP-combo_FF_LL_select-02',
  'sql': "select MASK_FULL(region, '*'), to_iso8601(`time`) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-combo_FF_LL_select-03',
  'sql': "select MASK_PARTIAL(region, 1, 1, '#'), MASK_NONE(region) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['MASK_PARTIAL', 'MASK_NONE'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-combo_FF_PP_nested-01',
  'sql': 'select round(abs(voltage - 220.0), 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'ABS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-combo_FF_PP_agg_scalar-01',
  'sql': 'select avg(abs(voltage - 220.0)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG', 'ABS'], 'local_not_has': ['Aggregate', 'Projection']}},
{'id': 'EXP-combo_FF_PL_agg_local_inside-01',
  'sql': 'select avg(twa(voltage)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
 {'id': 'EXP-combo_FF_PL_agg_local_inside-02',
  'sql': 'select count(to_iso8601(`time`)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-combo_FO_PP_func_select_op_where-01',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage between 219.0 and 222.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'BETWEEN'],
                 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-combo_FO_PP_func_in_where-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where abs(voltage - 220.0) < 2.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ABS', '<'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-combo_FO_LP_localfunc_select_op_where-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage > 220.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', '>'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_FO_PL_pushfunc_op_local_bitwise_influx-01',
  'sql': 'select abs(voltage) from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['ABS', '&'], 'pg': ['ABS', '&'], 'sqlserver': ['ABS', '&']},
                 'remote_not_has': {'influx': ['ABS', ' & ']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-combo_FO_LL_localfunc_op_unsupported-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where (cast(voltage as bigint) | 1) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['CSUM'], 'pg': ['CSUM'], 'influx': ['CSUM'], 'sqlserver': ['CSUM']},
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function'], 'sqlserver': ['Indefinite Rows Function']}}},
 {'id': 'EXP-combo_FW_PP_pushfunc_intervalwin-01',
  'sql': "select _wstart, avg(voltage) from {SRC}.sensor where `time` >= '2024-01-01' and `time` "
         "<= '2024-01-02' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG', 'GROUP BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-combo_FW_LP_localfunc_intervalwin-01',
  'sql': "select csum(voltage) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM', 'INTERVAL('],
                 'local_has': {'mysql': ['Interval'], 'pg': ['Interval'], 'influx': ['Interval']}}},
 {'id': 'EXP-combo_FW_PL_pushfunc_statewin-01',
  'sql': 'select count(*), region from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-combo_FW_LL_localfunc_localwin-01',
  'sql': 'select csum(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-combo_FC_PP_pushfunc_full_clauses-01',
  'sql': 'select abs(voltage - 220.0) av from {SRC}.sensor where voltage > 219.0 order by `time` '
         'limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'ORDER BY', 'LIMIT'], 'local_not_has': ['Sort ']}},
 {'id': 'EXP-combo_FC_PP_agg_groupby_having-01',
  'sql': 'select region, sum(voltage), avg(voltage) cnt from {SRC}.sensor group by region having '
         'sum(voltage) > 100',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUM', 'AVG', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-combo_FC_PP_distinct_pushfunc-01',
  'sql': 'select distinct upper(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['DISTINCT', 'UPPER'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-combo_FC_LP_localfunc_clauses-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage > 220.0 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_FC_LL_localfunc_localclause-01',
  'sql': 'select twa(voltage) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'SLIMIT', 'PARTITION'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-combo_FC_PL_case_with_localfunc-01',
  'sql': "select case when twa(voltage) > 220.0 then 'high' else 'low' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CASE', 'TWA'],
                 'local_has': {'mysql': ['Projection', 'Aggregate'],
                               'pg': ['Projection', 'Aggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-combo_FC_PP_case_with_pushfunc-01',
  'sql': "select case when abs(voltage - 220.0) < 1.0 then 'normal' else 'abnormal' end from "
         '{SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IF(', 'ABS'],
                                'pg': ['CASE', 'ABS'],
                                'influx': ['CASE', 'ABS'], 'sqlserver': ['CASE', 'ABS']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-combo_OW_PP_op_intervalwin-01',
  'sql': "select _wstart, avg(voltage) from {SRC}.sensor where region = 'north' and `time` >= "
         "'2024-01-01' and `time` <= '2024-01-02' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'GROUP BY', 'AVG'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-combo_OW_PL_op_statewin-01',
  'sql': 'select count(*), region from {SRC}.sensor where voltage > 219.0 state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_OW_LL_op_local_win_local_influx-01',
  'sql': 'select count(*) from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1 '
         'state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['STATE_WINDOW'],
                                    'pg': ['STATE_WINDOW'],
                                    'influx': [' & ', 'STATE_WINDOW'], 'sqlserver': ['STATE_WINDOW']},
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow'], 'sqlserver': ['StateWindow']}}},
 {'id': 'EXP-combo_OC_PP_in_orderby_limit-01',
  'sql': 'select * from {SRC}.sensor where voltage in (220.0, 221.0) order by `time` limit 5',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'IN', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Filter:', 'Sort', 'limit=']}},
 {'id': 'EXP-combo_OC_PP_compound_groupby-01',
  'sql': "select region, count(*) from {SRC}.sensor where voltage > 219.0 and region <> 'east' "
         'group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'AND', 'GROUP BY'],
                 'local_not_has': ['Filter:', 'GroupAggregate', 'Projection']}},
 {'id': 'EXP-combo_OC_PP_like_distinct-01',
  'sql': "select distinct region from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['DISTINCT', 'WHERE', 'LIKE'],
                 'local_not_has': ['Filter:', 'GroupAggregate', 'Projection']}},
 {'id': 'EXP-combo_OC_PL_op_partition_limit-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > 219.0 partition by region limit '
         '1',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['LIMIT'],
                 'local_has': ['Projection', 'Partition'],
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_WC_PP_interval_orderby_limit-01',
  'sql': "select _wstart, avg(voltage) from {SRC}.sensor where `time` >= '2024-01-01' and `time` "
         "<= '2024-01-02' interval(1m) order by _wstart limit 5",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:',
                                   'Sort',
                                   'limit=',
                                   'Interval',
                                   'Aggregate',
                                   'Projection']}},
{'id': 'EXP-combo_WC_PL_interval_fill-01',
  'sql': "select _wstart, avg(voltage) from {SRC}.sensor where `time` >= '2024-01-01' and `time` "
         "<= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
 'check_args': {'remote_has': {'mysql': ['SELECT `time`, `voltage`', 'ORDER BY `time` ASC'],
                               'pg': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'],
                               'influx': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME], [VOLTAGE]', 'ORDER BY [TIME] ASC']},
                'remote_not_has': ['AVG(', 'GROUP BY', 'FILL', 'INTERVAL('],
                'local_has': ['Fill', 'Interval']}},
 {'id': 'EXP-combo_WC_PL_extwin_fill-01',
  'sql': "select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' interval(30s) "
         "fill(null)) w) fill(null) order by _wstart",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_not_has',
             'local_has',
             'min_federated_scans'],
  'check_args': {'remote_any_not_has': ['GROUP BY',
                                        'COUNT(',
                                        'SUM(',
                                        'FILL',
                                        'INTERVAL(',
                                        'EXTERNAL_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'External', 'Fill', 'Interval'],
                               'pg': ['Projection', 'External', 'Fill', 'Interval'],
                               'influx': ['Projection', 'External', 'Fill', 'Interval']},
                 'min_scans': 2}},
 {'id': 'EXP-combo_WC_PL_extwin_fill-02',
  'sql': "select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' interval(30s) "
         "fill(null)) w) fill(value, 0, 0) order by _wstart",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_not_has',
             'local_has',
             'min_federated_scans'],
  'check_args': {'remote_any_not_has': ['GROUP BY',
                                        'COUNT(',
                                        'SUM(',
                                        'FILL',
                                        'INTERVAL(',
                                        'EXTERNAL_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'External', 'Fill', 'Interval'],
                               'pg': ['Projection', 'External', 'Fill', 'Interval'],
                               'influx': ['Projection', 'External', 'Fill', 'Interval']},
                 'min_scans': 2}},
 {'id': 'EXP-combo_WC_PL_extwin_fill-03',
  'sql': "select _wstart, count(*), sum(voltage) from {SRC}.sensor "
         "external_window((select _wstart, _wend from {SRC}.sensor "
         "where `time` >= '2024-01-01' and `time` < '2024-01-01 00:05:00' interval(30s) "
         "fill(null)) w) fill(prev) order by _wstart",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_not_has',
             'local_has',
             'min_federated_scans'],
  'check_args': {'remote_any_not_has': ['GROUP BY',
                                        'COUNT(',
                                        'SUM(',
                                        'FILL',
                                        'INTERVAL(',
                                        'EXTERNAL_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'External', 'Fill', 'Interval'],
                               'pg': ['Projection', 'External', 'Fill', 'Interval'],
                               'influx': ['Projection', 'External', 'Fill', 'Interval']},
                 'min_scans': 2}},
 {'id': 'EXP-combo_WC_LP_statewin_orderby-01',
  'sql': 'select count(*), region from {SRC}.sensor state_window(region) order by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['Sort', 'StateWindow'],
                               'pg': ['Sort', 'StateWindow'],
                               'influx': ['Sort', 'StateWindow']}}},
 {'id': 'EXP-combo_WC_LL_localwin_slimit-01',
  'sql': 'select count(*) from {SRC}.sensor partition by region session(`time`, 1m) slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SESSION(', 'SLIMIT', 'PARTITION'],
                 'local_has': {'mysql': ['Projection', 'Session', 'Partition'],
                               'pg': ['Projection', 'Session', 'Partition'],
                               'influx': ['Projection', 'Session', 'Partition']}}},
 {'id': 'EXP-combo_CC_PP_distinct_orderby_limit-01',
  'sql': 'select distinct region from {SRC}.sensor order by region limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['DISTINCT', 'ORDER BY', 'LIMIT'],
                                'pg': ['DISTINCT', 'ORDER BY', 'LIMIT'],
                                'influx': ['DISTINCT', 'ORDER BY', 'LIMIT'],
                                # T-SQL error 145: ORDER BY over a DISTINCT must
                                # be built from the select list, so sort and
                                # limit stay local - only DISTINCT is remote.
                                'sqlserver': ['DISTINCT']},
                 'local_not_has': {'mysql': ['Sort', 'limit=', 'GroupAggregate', 'Projection'],
                                   'pg': ['Sort', 'limit=', 'GroupAggregate', 'Projection'],
                                   'influx': ['Sort', 'limit=', 'GroupAggregate', 'Projection'],
                                   # with the sort kept local the limit and the
                                   # projection stay local as well.
                                   'sqlserver': ['GroupAggregate']}}},
 {'id': 'EXP-combo_CC_PP_groupby_having_orderby_limit-01',
  'sql': 'select region, count(*) cnt from {SRC}.sensor group by region having count(*) > 0 order '
         'by cnt desc limit 2',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Sort', 'limit=', 'GroupAggregate', 'Projection']}},
 {'id': 'EXP-combo_CC_PP_union_orderby-01',
  'sql': 'select * from (select `time`, voltage from {SRC}.sensor where voltage > 221.0 union all '
         'select `time`, voltage from {SRC}.sensor where voltage < 220.0) t order by voltage',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_has'],
  'check_args': {'remote_has': ['UNION'], 'local_has': ['Sort '], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_CC_LL_slimit_partition_limit-01',
  'sql': 'select * from {SRC}.sensor partition by region slimit 1 limit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'LIMIT'],
                 'local_has': {'mysql': ['Projection', 'Partition'],
                               'pg': ['Projection', 'Partition'],
                               'influx': ['Projection', 'Partition']}}},
{'id': 'EXP-combo_CC_PL_groupby_fill-01',
  'sql': "select _wstart, region, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and "
         "`time` <= '2024-01-02' partition by region interval(1m) fill(null)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['SELECT `time`, `region`', 'ORDER BY `time` ASC'],
                                'pg': ['SELECT "time", "region"', 'ORDER BY "time" ASC'],
                                'influx': ['SELECT "time", "region"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME], [REGION]', 'ORDER BY [TIME] ASC']},
                 'remote_not_has': ['COUNT(', 'GROUP BY', 'FILL', 'INTERVAL('],
                 'local_has': ['Fill', 'Interval', 'Partition']}},
 {'id': 'EXP-combo_S_PP_outer_pushP_inner_pushP-01',
  'sql': 'select * from (select abs(voltage - 220.0) av from {SRC}.sensor where voltage > 219.0) t '
         'order by t.av limit 3',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_has',
             'local_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'],
                 'local_has': ['Projection', 'Sort '],
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_S_LP_outer_pushP_inner_localL-01',
  'sql': 'select * from (select csum(voltage) v from {SRC}.sensor) t order by t.v limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM'], 'local_has': ['Indefinite Rows Function']}},
 {'id': 'EXP-combo_S_LP_scalar_inner_localL-01',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select twa(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'min_federated_scans',
             'local_has'],
  'check_args': {'remote_not_has': ['TWA'],
                 'min_scans': 2,
                 'local_has': ['InitPlan 1', 'Aggregate']}},
 {'id': 'EXP-combo_S_PL_in_subq_localfunc_inner-01',
  'sql': 'select * from {SRC}.sensor where voltage in (select twa(voltage) from {SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'min_federated_scans',
             'local_has'],
  'check_args': {'remote_not_has': ['TWA'],
                 'min_scans': 2,
                 'local_has': ['InitPlan 1', 'Aggregate']}},
 {'id': 'EXP-combo_3W_FOC_pushP-01',
  'sql': 'select abs(voltage - 220.0) av from {SRC}.sensor where voltage between 219.0 and 222.0 '
         'order by av limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'BETWEEN', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Sort ']}},
 {'id': 'EXP-combo_3W_FWC_pushP-01',
  'sql': 'select _wstart, avg(abs(voltage - 220.0)) av from {SRC}.sensor where `time` >= '
         "'2024-01-01' and `time` <= '2024-01-02' interval(1m) order by _wstart limit 5",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG', 'ABS', 'GROUP BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:',
                                   'Sort',
                                   'limit=',
                                   'Interval',
                                   'Aggregate',
                                   'Projection']}},
 {'id': 'EXP-combo_3W_FWC_LLP-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage > 219.0 state_window(region) order '
         'by voltage',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['CSUM', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['Sort', 'StateWindow'],
                               'pg': ['Sort', 'StateWindow'],
                               'influx': ['Sort', 'StateWindow']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_3W_FOC_LPP-01',
  'sql': "select diff(voltage) from {SRC}.sensor where region = 'north' and voltage > 219.0 order "
         'by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'AND'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-combo_3W_FOC_LLP-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1 order by '
         '`time` limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['CSUM'], 'pg': ['CSUM'], 'influx': ['CSUM', ' & '], 'sqlserver': ['CSUM']},
                 'local_has': {'mysql': ['Projection', 'Sort', 'Indefinite Rows Function'],
                               'pg': ['Projection', 'Sort', 'Indefinite Rows Function'],
                               'influx': ['Projection', 'Sort', 'Indefinite Rows Function'], 'sqlserver': ['Projection', 'Sort', 'Indefinite Rows Function']}}},
 {'id': 'EXP-combo_3W_FWC_LLL-01',
  'sql': 'select twa(voltage) from {SRC}.sensor partition by region session(`time`, 1m) slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'SESSION(', 'SLIMIT', 'PARTITION'],
                 'local_has': {'mysql': ['Projection', 'Session', 'Partition'],
                               'pg': ['Projection', 'Session', 'Partition'],
                               'influx': ['Projection', 'Session', 'Partition']}}},
 {'id': 'EXP-combo_4W_FOWC_PPPP-01',
  'sql': 'select _wstart, region, avg(abs(voltage - 220.0)) av from {SRC}.sensor where region in '
         "('north', 'south') and `time` >= '2024-01-01' and `time` <= '2024-01-02' partition by "
         'region interval(1m) order by _wstart limit 10',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT'],
                 'remote_not_has': ['PARTITION', 'INTERVAL('],
                 'local_not_has': ['Filter:',
                                   'Sort',
                                   'limit=',
                                   'Partition',
                                   'Interval',
                                   'Aggregate',
                                   'Projection']}},
 {'id': 'EXP-combo_4W_FOWC_LLLL-01',
  'sql': 'select twa(voltage) from {SRC}.sensor where (cast(voltage as bigint) & 1) = 1 partition '
         'by region session(`time`, 1m) slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['TWA', 'SESSION(', 'SLIMIT', 'PARTITION'],
                                    'pg': ['TWA', 'SESSION(', 'SLIMIT', 'PARTITION'],
                                    'influx': ['TWA', ' & ', 'SESSION(', 'SLIMIT', 'PARTITION'], 'sqlserver': ['TWA', 'SESSION(', 'SLIMIT', 'PARTITION']},
                 'local_has': {'mysql': ['Projection', 'Session', 'Partition'],
                               'pg': ['Projection', 'Session', 'Partition'],
                               'influx': ['Projection', 'Session', 'Partition'], 'sqlserver': ['Projection', 'Session', 'Partition']}}},
 {'id': 'EXP-combo_XS_join_pushfunc-01',
  'sql': 'select abs(m.voltage - 220.0), p.current from {SRC}.sensor m join {SRC2}.sensor p on '
         'm.`time` = p.`time` and m.region = p.region',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_any_count_eq',
             'remote_any_not_has',
             'local_has'],
  'check_args': {'min_scans': 2,
                 'remote_any_count_eq': [('Remote SQL:', 2)],
                 'remote_any_not_has': ['ABS', 'JOIN'],
                 'local_has': {'mysql_pg': ['Projection', 'Inner Join'],
                               'mysql_influx': ['Projection', 'Inner Join'],
                               'pg_influx': ['Projection', 'Inner Join']}}},
 {'id': 'EXP-combo_XS_join_localfunc-01',
  'sql': 'select csum(m.voltage), p.current from {SRC}.sensor m join {SRC2}.sensor p on m.`time` = '
         'p.`time` and m.region = p.region',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['CSUM'],
                 'min_scans': 2,
                 'local_has': {'mysql_pg': ['Indefinite Rows Function',
                                            'Inner Join'],
                               'mysql_influx': ['Indefinite Rows Function',
                                                'Inner Join'],
                               'pg_influx': ['Indefinite Rows Function',
                                             'Inner Join']}}},
 {'id': 'EXP-combo_XS_join_with_where-01',
  'sql': 'select m.voltage, p.current from {SRC}.sensor m join {SRC2}.sensor p on m.`time` = '
         'p.`time` and m.region = p.region where m.voltage > 219.0 and p.current > 1.0',
  'sources': 'cross',
  'checks': ['min_federated_scans', 'no_table_scan', 'analyze_metrics', 'remote_has', 'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'min_scans': 2,
                 'local_has': {'mysql_pg': ['Projection', 'Inner Join'],
                               'mysql_influx': ['Projection', 'Inner Join'],
                               'pg_influx': ['Projection', 'Inner Join']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_math-01',
  'sql': 'select ABS(voltage - 220.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-02',
  'sql': 'select ACOS(SIN(voltage) / 1000.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ACOS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-03',
  'sql': 'select ASIN(SIN(voltage) / 1000.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ASIN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-04',
  'sql': 'select ATAN(voltage / 1000.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ATAN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-05',
  'sql': 'select CEIL(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CEIL'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-06',
  'sql': 'select COS(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['COS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-07',
  'sql': 'select DEGREES(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['DEGREES'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-08',
  'sql': 'select EXP(1.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['EXP'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-09',
  'sql': 'select FLOOR(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['FLOOR'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-10',
  'sql': 'select LN(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-11',
  'sql': 'select POW(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['POW'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-12',
  'sql': 'select RADIANS(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['RADIANS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-13',
  'sql': 'select ROUND(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-14',
  'sql': 'select SIGN(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SIGN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-15',
  'sql': 'select SIN(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SIN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-16',
  'sql': 'select SQRT(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SQRT'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-17',
  'sql': 'select TAN(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['TAN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-18',
  'sql': 'select PI() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-19',
  'sql': 'select log(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['LOG'], 'pg': ['LN'], 'influx': ['LN'], 'sqlserver': ['LOG(']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-20',
  'sql': 'select truncate(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['TRUNCATE'], 'pg': ['TRUNC'], 'influx': ['TRUNC'], 'sqlserver': ['ROUND(']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-21',
  'sql': 'select mod(area, 2) from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['MOD'], 'pg': ['MOD'], 'influx': ['%'], 'sqlserver': ['%']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_math-22',
  'sql': 'select corr(voltage, current) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['CORR'], 'influx': ['CORR']},
                 'remote_not_has': {'mysql': ['CORR'], 'sqlserver': ['CORR']},
                 'local_has': {'mysql': ['Aggregate'], 'sqlserver': ['Aggregate']},
                 'local_not_has': {'influx': ['Aggregate'], 'pg': ['Aggregate']}}},
 {'id': 'EXP-ds_math-23',
  'sql': 'select GREATEST(voltage, 220) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'verbose_remote_has', 'analyze_local_has'],
  'check_args': {'remote_has': {'mysql': ['GREATEST'], 'pg': ['GREATEST'], 'sqlserver': []},
                 'plain_local_has': [],
                 'verbose_remote_has': {'influx': ['GREATEST']},
                 'analyze_local_has': {'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_math-24',
  'sql': 'select LEAST(voltage, 220) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'verbose_remote_has', 'analyze_local_has'],
  'check_args': {'remote_has': {'mysql': ['LEAST'], 'pg': ['LEAST'], 'sqlserver': []},
                 'plain_local_has': [],
                 'verbose_remote_has': {'influx': ['LEAST']},
                 'analyze_local_has': {'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_string-01',
  'sql': 'select ASCII(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ASCII'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-02',
  'sql': 'select CHAR_LENGTH(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-03',
  'sql': "select CONCAT(region, '_x') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CONCAT'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-04',
  'sql': "select CONCAT_WS(',', region, region) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CONCAT_WS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-05',
  'sql': 'select LOWER(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LOWER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-06',
  'sql': 'select LTRIM(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LTRIM'], 'local_not_has': ['Projection']}},
 # PG TEXT and Influx tags map to the maximum field length; keep REPEAT within that limit.
 {'id': 'EXP-ds_string-07',
  'sql': 'select REPEAT(region, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['REPEAT'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-08',
  'sql': "select REPLACE(region, 'n', 'x') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['REPLACE'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-09',
  'sql': 'select RTRIM(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['RTRIM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-10',
  'sql': 'select TRIM(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['TRIM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-11',
  'sql': 'select UPPER(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-12',
  'sql': 'select char(ascii(region)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['CHAR'], 'pg': ['CHR'], 'influx': ['CHR']},
                 # SQL Server keeps CHAR(ASCII(...)) local, so its local plan
                 # legitimately retains the projection.
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'],
                                   'influx': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_string-13',
  'sql': 'select length(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['CHAR_LENGTH', '* 4'],
                                'pg': ['CHAR_LENGTH', '* 4'],
                                'influx': ['CHAR_LENGTH', '* 4'], 'sqlserver': ['LEN(', '* 4']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-13a',
  'sql': 'select length(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_LENGTH_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['LENGTH'],
                                'pg_ascii': ['OCTET_LENGTH'], 'sqlserver': ['DATALENGTH']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-13b',
  'sql': 'select length(region_ascii) from {SRC}.region_info',
  'sources': 'custom',
  'source_infos': _ASCII_LENGTH_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['LENGTH'],
                                'pg_ascii': ['OCTET_LENGTH'], 'sqlserver': ['DATALENGTH']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-14',
  'sql': "select position('n' in region) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['POSITION'], 'pg': ['POSITION'], 'influx': ['STRPOS'], 'sqlserver': ['CHARINDEX']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-15',
  'sql': 'select substring(region, 1, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['SUBSTRING'], 'pg': ['SUBSTRING'], 'influx': ['SUBSTR'], 'sqlserver': ['SUBSTRING']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_string-16',
  'sql': "select substring_index(region, '_', 1) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['SUBSTRING_INDEX']},
                 'remote_not_has': {'pg': ['SUBSTRING_INDEX'], 'influx': ['SUBSTRING_INDEX'], 'sqlserver': ['SUBSTRING_INDEX', 'STRING_SPLIT']},
                 'local_has': {'pg': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection']}}},
 {'id': 'EXP-ds_string-17',
  'sql': "select find_in_set('north', region) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['FIND_IN_SET']},
                 'remote_not_has': {'pg': ['FIND_IN_SET'], 'influx': ['FIND_IN_SET'], 'sqlserver': ['FIND_IN_SET']},
                 'local_has': {'pg': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection']}}},
 {'id': 'EXP-ds_string-18',
  'sql': "select LIKE_IN_SET(region, 'no%|so%') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LIKE_IN_SET'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_string-19',
  'sql': "select REGEXP_IN_SET(region, 'no%|so%') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['REGEXP_IN_SET'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_encoding-01',
  'sql': 'select to_base64(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['TO_BASE64', 'ENCODE'],
                                    'pg': ['TO_BASE64', 'ENCODE'],
                                    'influx': ['TO_BASE64', 'ENCODE'], 'sqlserver': ['TO_BASE64', 'ENCODE']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_encoding-01a',
  'sql': 'select to_base64(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'local_has': {'sqlserver': ['Projection']}, 'remote_not_has': {'sqlserver': ['TO_BASE64', 'ENCODE']}, 'remote_has': {'mysql_ascii': ['TO_BASE64'],
                                'pg_ascii': ['ENCODE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_encoding-02',
  'sql': 'select from_base64(b64_str) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['FROM_BASE64', 'DECODE'],
                                    'pg': ['FROM_BASE64', 'DECODE'],
                                    'influx': ['FROM_BASE64', 'DECODE'], 'sqlserver': ['FROM_BASE64', 'DECODE']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_encoding-02a',
  'sql': 'select from_base64(b64_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'local_has': {'sqlserver': ['Projection']}, 'remote_not_has': {'sqlserver': ['FROM_BASE64', 'DECODE']}, 'remote_has': {'mysql_ascii': ['FROM_BASE64'],
                                'pg_ascii': ['DECODE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_hash-01',
  'sql': 'select md5(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['MD5'],
                                    'pg': ['MD5'],
                                    'influx': ['MD5'], 'sqlserver': ['MD5', 'HASHBYTES']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_hash-01a',
  'sql': 'select md5(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['MD5'],
                                'pg_ascii': ['MD5'], 'sqlserver': ['HASHBYTES', 'MD5']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_hash-02',
  'sql': 'select sha1(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['SHA1'],
                                    'pg': ['SHA1'],
                                    'influx': ['SHA1'], 'sqlserver': ['SHA1', 'HASHBYTES']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_hash-02a',
  'sql': 'select sha1(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['SHA1'], 'sqlserver': ['HASHBYTES', 'SHA1']},
                 'remote_not_has': {'pg_ascii': ['SHA1']},
                 'local_has': {'pg_ascii': ['Projection']},
                 'local_not_has': {'mysql_ascii': ['Projection']}}},
 {'id': 'EXP-ds_hash-03',
  'sql': 'select sha2(region, 256) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['SHA2', 'SHA256', 'SHA512'],
                                    'pg': ['SHA2', 'SHA256', 'SHA512'],
                                    'influx': ['SHA2', 'SHA256', 'SHA512'], 'sqlserver': ['SHA2', 'SHA256', 'SHA512', 'HASHBYTES']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_hash-03a',
  'sql': 'select sha2(region_ascii, 256) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['SHA2'], 'sqlserver': ['HASHBYTES', 'SHA2_256']},
                 'remote_not_has': {'pg_ascii': ['SHA2', 'SHA256', 'SHA512']},
                 'local_has': {'pg_ascii': ['Projection']},
                 'local_not_has': {'mysql_ascii': ['Projection']}}},
 {'id': 'EXP-ds_bitwise_func-01',
  'sql': 'select crc32(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['CRC32'],
                                    'pg': ['CRC32'],
                                    'influx': ['CRC32'], 'sqlserver': ['CRC32', 'CHECKSUM']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_bitwise_func-01a',
  'sql': 'select crc32(region_ascii) from {SRC}.sensor',
  'sources': 'custom',
  'source_infos': _ASCII_VARCHAR_SOURCES,
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql_ascii': ['CRC32']},
                 'remote_not_has': {'pg_ascii': ['CRC32'], 'sqlserver': ['CRC32', 'CHECKSUM']},
                 'local_has': {'pg_ascii': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql_ascii': ['Projection']}}},
 {'id': 'EXP-ds_mask-01',
  'sql': "select MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_mask-02',
  'sql': "select MASK_PARTIAL(region, 1, 1, '#') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_PARTIAL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_mask-03',
  'sql': 'select MASK_NONE(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_NONE'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_crypto-01',
  'sql': "select aes_encrypt(region, 'k') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AES_ENCRYPT'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_crypto-02',
  'sql': "select aes_decrypt(aes_encrypt(region, 'k'), 'k') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AES_DECRYPT'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_crypto-03',
  'sql': "select sm4_encrypt(region, 'k') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SM4_ENCRYPT'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_crypto-04',
  'sql': "select sm4_decrypt(sm4_encrypt(region, 'k'), 'k') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SM4_DECRYPT'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_typeconv-01',
  'sql': 'select cast(voltage as varchar(32)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_typeconv-02',
  'sql': "select to_char(`time`, 'yyyy-mm-dd') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['DATE_FORMAT'], 'pg': ['TO_CHAR'], 'influx': ['TO_CHAR'], 'sqlserver': ['FORMAT(']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_typeconv-03',
  'sql': "select to_timestamp(date_str, 'yyyy-mm-dd') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['STR_TO_DATE'],
                                'pg': ['TO_TIMESTAMP'],
                                'influx': ['TO_TIMESTAMP']},
                 # TO_TIMESTAMP stays local on SQL Server (no T-SQL equivalent),
                 # so its local projection remains.
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'],
                                   'influx': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_typeconv-04',
  'sql': 'select to_unixtimestamp(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['TIMESTAMPDIFF', 'CONVERT_TZ'],
                                'pg': ['EXTRACT', 'EPOCH'],
                                'influx': ['EXTRACT', 'EPOCH'], 'sqlserver': ['DATEDIFF_BIG']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_typeconv-05',
  'sql': 'select to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-ds_typeconv-06',
  'sql': 'select to_json(json_str) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['TO_JSON'],
                                    'pg': ['TO_JSON'],
                                    'influx': ['TO_JSON'], 'sqlserver': ['TO_JSON', 'JSON_QUERY']},
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_time-01',
  'sql': 'select now() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-02',
  'sql': 'select today() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SELECT', 'FROM'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-03',
  'sql': 'select timezone() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['TIMEZONE('], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-04',
  'sql': 'select date(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['DATE_FORMAT'], 'pg': ['TO_CHAR', 'YYYY-MM-DD'], 'influx': ['CAST', 'DATE'], 'sqlserver': ['CONVERT(VARCHAR(10)']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-05',
  'sql': 'select dayofweek(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['DAYOFWEEK'],
                                'pg': ['EXTRACT', 'DOW'],
                                'influx': ['EXTRACT', 'DOW'], 'sqlserver': ['DATEPART(WEEKDAY']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-06',
  'sql': 'select weekday(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['WEEKDAY'], 'pg': ['EXTRACT', 'ISODOW'], 'sqlserver': ['DATEPART(WEEKDAY']},
                 'remote_not_has': {'influx': ['WEEKDAY', 'ISODOW']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_time-07',
  'sql': 'select week(`time`, 0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_has'],
  'check_args': {'remote_has': {'mysql': ['WEEK'], 'sqlserver': []},
                 'remote_not_has': {'pg': ['EXTRACT', 'WEEK'], 'influx': ['EXTRACT', 'WEEK'], 'sqlserver': ['WEEK', 'DATEPART']},
                 'local_has': {'pg': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_time-08',
  'sql': 'select week(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_has'],
  'check_args': {'remote_has': {'mysql': ['WEEK'], 'sqlserver': []},
                 'remote_not_has': {'pg': ['EXTRACT', 'WEEK'], 'influx': ['EXTRACT', 'WEEK'], 'sqlserver': ['WEEK', 'DATEPART']},
                 'local_has': {'pg': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_time-09',
  'sql': 'select weekofyear(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['WEEKOFYEAR'], 'sqlserver': ['DATEPART(ISO_WEEK']}, 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-10',
  'sql': 'select weekofyear(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'pg': ['EXTRACT', 'WEEK'], 'influx': ['EXTRACT', 'WEEK'], 'sqlserver': ['DATEPART(ISO_WEEK']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-11',
  'sql': 'select timediff(`time`, `time`, 1s) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['TIMESTAMPDIFF'], 'sqlserver': ['DATEDIFF_BIG']}, 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-12',
  'sql': 'select timediff(`time`, `time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'pg': ['EXTRACT', 'EPOCH'],
                                'influx': ['CAST("time" AS BIGINT)', 'AS BIGINT'], 'sqlserver': ['DATEDIFF_BIG']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_time-13',
  'sql': 'select timetruncate(`time`, 1m) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['DATE_TRUNC'], 'influx': ['DATE_TRUNC']},
                 'remote_not_has': {'mysql': ['TIMETRUNCATE'], 'sqlserver': ['TIMETRUNCATE', 'DATETRUNC']},
                 'local_has': {'mysql': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'influx': ['Projection'], 'pg': ['Projection']}}},
 {'id': 'EXP-ds_agg_basic-01',
  'sql': 'select AVG(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-02',
  'sql': 'select COUNT(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['COUNT'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-03',
  'sql': 'select SUM(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUM'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-04',
  'sql': 'select STDDEV_SAMP(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['STDDEV_SAMP'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-05',
  'sql': 'select VAR_POP(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['VAR_POP'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-06',
  'sql': 'select VAR_SAMP(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['VAR_SAMP'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-07',
  'sql': 'select stddev(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['STDDEV_POP'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_agg_basic-08',
  'sql': 'select variance(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['VAR_POP'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_agg_basic-09',
  'sql': 'select spread(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['MAX', 'MIN'],
                 'remote_not_has': ['SPREAD'],
                 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_agg_basic-10',
  'sql': 'select group_concat(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['GROUP_CONCAT'],
                                'pg': ['STRING_AGG'],
                                'influx': ['STRING_AGG'], 'sqlserver': ['STRING_AGG']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_agg_basic-11',
  'sql': 'select leastsquares(voltage, 0, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LEASTSQUARES'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_quantile-01',
  'sql': 'select percentile(voltage, 50) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['PERCENTILE_CONT', 'WITHIN GROUP']},
                 'remote_not_has': {'mysql': ['PERCENTILE'], 'influx': ['PERCENTILE'], 'sqlserver': ['PERCENTILE']},
                 'local_has': {'mysql': ['Aggregate'], 'influx': ['Aggregate'], 'sqlserver': ['Aggregate']},
                 'local_not_has': {'pg': ['Aggregate']}}},
 {'id': 'EXP-ds_quantile-02',
  'sql': 'select apercentile(voltage, 50) from {SRC}.sensor',
  'sources': 'single',
 'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': {'mysql': ['APERCENTILE'],
                                    'pg': ['APERCENTILE'],
                                    'influx': ['APPROX_PERCENTILE_CONT', 'APERCENTILE'], 'sqlserver': ['APERCENTILE', 'APPROX_PERCENTILE_CONT']},
                 'local_has': {'mysql': ['Aggregate'], 'pg': ['Aggregate'], 'influx': ['Aggregate'], 'sqlserver': ['Aggregate']}}},
 {'id': 'EXP-ds_agg_special-01',
  'sql': 'select elapsed(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['ELAPSED'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_agg_special-02',
  'sql': 'select histogram(voltage, \'linear_bin\', \'{{"start":0, "width":10, "count":10}}\', 0) '
         'from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['HISTOGRAM'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_agg_special-03',
  'sql': 'select hyperloglog(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['HYPERLOGLOG'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-01',
  'sql': 'select MAX(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MAX'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_selection-02',
  'sql': 'select MIN(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MIN'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-ds_selection-03',
  'sql': 'select FIRST(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['FIRST('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-04',
  'sql': 'select LAST(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LAST('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-05',
  'sql': 'select LAST_ROW(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LAST_ROW('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-06',
  'sql': 'select TAIL(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has',
             'local_not_has', 'verbose_remote_has'],
  'check_args': {'remote_not_has': ['SELECT TAIL('],
                 'local_not_has': ['Indefinite Rows Function'],
                 'verbose_remote_has': ['ORDER BY', 'DESC', 'LIMIT 1']}},
 {'id': 'EXP-ds_selection-07',
  'sql': 'select UNIQUE(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has',
             'local_not_has', 'verbose_remote_has',
             'analyze_local_has'],
  'check_args': {'remote_not_has': ['UNIQUE('],
                 'local_not_has': ['Indefinite Rows Function'],
                 'verbose_remote_has': ['TIME'],
                 'analyze_local_has': {'mysql': ['GroupAggregate'],
                                       'pg': ['GroupAggregate'],
                                       'influx': ['GroupAggregate']}}},
 {'id': 'EXP-ds_selection-08',
  'sql': 'select TOP(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TOP('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-09',
  'sql': 'select BOTTOM(voltage, 2) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['BOTTOM('],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_selection-10',
  'sql': 'select LAG(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LAG', 'OVER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_selection-11',
  'sql': 'select LEAD(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LEAD', 'OVER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_selection-12',
  'sql': 'select mode(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['MODE', 'WITHIN GROUP']},
                 'remote_not_has': {'mysql': ['MODE('], 'influx': ['MODE('], 'sqlserver': ['MODE(']},
                 'local_has': {'mysql': ['Aggregate'], 'influx': ['Aggregate'], 'sqlserver': ['Aggregate']},
                 'local_not_has': {'pg': ['Aggregate']}}},
 {'id': 'EXP-ds_compare-01',
  'sql': 'select if(voltage > 220, 1, 0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IF('],
                                'pg': ['CASE', 'WHEN'],
                                'influx': ['CASE', 'WHEN'], 'sqlserver': ['CASE', 'WHEN']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_compare-02',
  'sql': "select ifnull(region, 'x') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IFNULL'], 'pg': ['COALESCE'], 'influx': ['COALESCE'], 'sqlserver': ['COALESCE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_compare-03',
  'sql': "select nullif(region, 'x') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['NULLIF'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_compare-04',
  'sql': 'select nvl2(region, 1, 0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IF('],
                                'pg': ['CASE', 'WHEN'],
                                'influx': ['CASE', 'WHEN'], 'sqlserver': ['CASE', 'WHEN']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_timeseries-01',
  'sql': 'select CSUM(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-02',
  'sql': 'select DERIVATIVE(voltage, 1s, 0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-03',
  'sql': 'select DIFF(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-04',
  'sql': 'select IRATE(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_timeseries-05',
  'sql': 'select MAVG(voltage, 3) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MAVG'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-06',
  'sql': "select STATECOUNT(voltage, 'GT', 220) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATECOUNT'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-07',
  'sql': "select STATEDURATION(voltage, 'GT', 220, 1s) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATEDURATION'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']}}},
 {'id': 'EXP-ds_timeseries-08',
  'sql': 'select TWA(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-ds_system-01',
  'sql': 'select CLIENT_VERSION() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['CLIENT_VERSION'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_system-02',
  'sql': 'select CURRENT_USER() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['CURRENT_USER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_system-03',
  'sql': 'select DATABASE() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['DATABASE'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_system-04',
  'sql': 'select SERVER_VERSION() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['SERVER_VERSION'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_system-05',
  'sql': 'select SERVER_STATUS() from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['SERVER_STATUS'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_geo-01',
  'sql': 'select ST_GEOMFROMTEXT(geom_wkt) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['ST_GEOMFROMTEXT'], 'pg': ['ST_GEOMFROMTEXT'], 'sqlserver': []},
                 'remote_not_has': {'influx': ['ST_GEOMFROMTEXT'], 'sqlserver': ['ST_GEOMFROMTEXT']},
                 'local_has': {'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_geo-02',
  'sql': 'select ST_ASTEXT(st_geomfromtext(geom_wkt)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['ST_ASTEXT'], 'pg': ['ST_ASTEXT'], 'sqlserver': []},
                 'remote_not_has': {'influx': ['ST_ASTEXT'], 'sqlserver': ['ST_ASTEXT']},
                 'local_has': {'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_geo-03',
  'sql': 'select ST_CONTAINSPROPERLY(st_geomfromtext(geom_wkt), st_geomfromtext(geom_wkt)) from '
         '{SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['ST_CONTAINSPROPERLY']},
                 'remote_not_has': {'mysql': ['ST_CONTAINSPROPERLY'],
                                    'influx': ['ST_CONTAINSPROPERLY'], 'sqlserver': ['ST_CONTAINSPROPERLY']},
                 'local_has': {'mysql': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_geo-04',
  'sql': 'select ST_COVERS(st_geomfromtext(geom_wkt), st_geomfromtext(geom_wkt)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['ST_COVERS']},
                 'remote_not_has': {'mysql': ['ST_COVERS'], 'influx': ['ST_COVERS'], 'sqlserver': ['ST_COVERS']},
                 'local_has': {'mysql': ['Projection'], 'influx': ['Projection'], 'sqlserver': ['Projection']},
                 'local_not_has': {'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_op_arith-01',
  'sql': 'select -voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['-'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-ds_op_compare-01',
  'sql': 'select * from {SRC}.sensor where voltage = 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '='], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-02',
  'sql': 'select * from {SRC}.sensor where voltage <> 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '<>'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-03',
  'sql': 'select * from {SRC}.sensor where voltage != 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '!='], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-04',
  'sql': 'select * from {SRC}.sensor where voltage > 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '>'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-05',
  'sql': 'select * from {SRC}.sensor where voltage < 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '<'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-06',
  'sql': 'select * from {SRC}.sensor where voltage >= 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '>='], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-07',
  'sql': 'select * from {SRC}.sensor where voltage <= 220.0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', '<='], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-08',
  'sql': 'select * from {SRC}.sensor where region is null',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'NULL'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-09',
  'sql': 'select * from {SRC}.sensor where region is not null',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'NULL'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-10',
  'sql': 'select * from {SRC}.sensor where isnull(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['IS NULL'],
                 'remote_not_has': ['ISNULL('],
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-11',
  'sql': 'select * from {SRC}.sensor where isnotnull(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['IS NOT NULL'],
                 'remote_not_has': ['ISNOTNULL('],
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-12',
  'sql': 'select * from {SRC}.sensor where voltage not between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'BETWEEN'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-13',
  'sql': 'select * from {SRC}.sensor where voltage not in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'IN'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-14',
  'sql': "select * from {SRC}.sensor where region not like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIKE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-ds_op_compare-15',
  'sql': "select * from {SRC}.sensor where region match 'no.*'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['REGEXP'], 'pg': ['~']},
                 'remote_not_has': {'influx': ['MATCH'], 'sqlserver': ['MATCH', 'REGEXP']},
                 'local_has': {'influx': [], 'sqlserver': []},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_op_compare-16',
  'sql': "select * from {SRC}.sensor where region nmatch 'no.*'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['NOT REGEXP'], 'pg': ['!~']},
                 'remote_not_has': {'influx': ['NMATCH'], 'sqlserver': ['NMATCH', 'NOT REGEXP']},
                 'local_has': {'influx': [], 'sqlserver': []},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': []}}},
 {'id': 'EXP-ds_op_bitwise-01',
  'sql': 'select area & 1 from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['&'], 'pg': ['&'], 'sqlserver': ['&']},
                 'remote_not_has': {'influx': [' & ']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_op_bitwise-02',
  'sql': 'select area | 1 from {SRC}.region_info',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['|'], 'pg': ['|'], 'sqlserver': ['|']},
                 'remote_not_has': {'influx': [' | ']},
                 'local_has': {'influx': ['Projection']},
                 'local_not_has': {'mysql': ['Projection'], 'pg': ['Projection'], 'sqlserver': ['Projection']}}},
 {'id': 'EXP-ds_op_set-01',
  'sql': 'select region from {SRC}.sensor union select region from {SRC}.sensor where voltage > '
         '220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UNION'],
                 'local_not_has': ['Data Exchange', 'GroupAggregate', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_window-01',
  'sql': 'select region, count(*) from {SRC}.sensor partition by region',
  'sources': 'single',
 'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'],
                 'remote_not_has': ['PARTITION'],
                 'local_not_has': ['Partition', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window_partition_agg-01',
  'sql': 'select region, avg(voltage) from {SRC}.sensor partition by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG', 'GROUP BY'],
                 'remote_not_has': ['PARTITION'],
                 'local_not_has': ['Partition', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window_partition_agg-02',
  'sql': 'select region, sum(voltage) from {SRC}.sensor partition by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUM', 'GROUP BY'],
                 'remote_not_has': ['PARTITION'],
                 'local_not_has': ['Partition', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window_partition_agg-03',
  'sql': 'select region, min(voltage) from {SRC}.sensor partition by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['MIN', 'GROUP BY'],
                 'remote_not_has': ['PARTITION'],
                 'local_not_has': ['Partition', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window_partition_agg-04',
  'sql': 'select region, max(voltage) from {SRC}.sensor partition by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['MAX', 'GROUP BY'],
                 'remote_not_has': ['PARTITION'],
                 'local_not_has': ['Partition', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window-02',
  'sql': "select _wstart, region, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and "
         "`time` <= '2024-01-02' partition by region interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'],
                 'remote_not_has': ['PARTITION', 'INTERVAL('],
                 'local_not_has': ['Filter:', 'Partition', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window-03',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_window-04',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(2m) sliding(1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['INTERVAL(', 'SLIDING'],
                 'local_has': {'mysql': ['Interval'], 'pg': ['Interval'], 'influx': ['Interval']}}},
 {'id': 'EXP-ds_window-05',
  'sql': 'select count(*), region from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-ds_window-06',
  'sql': 'select count(*) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-ds_window-07',
  'sql': 'select count(*) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'], 'pg': ['Count'], 'influx': ['Count']}}},
 {'id': 'EXP-ds_window-08',
  'sql': 'select count(*) from {SRC}.sensor event_window start with voltage > 220 end with voltage '
         '< 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'], 'pg': ['Event'], 'influx': ['Event']}}},
 {'id': 'EXP-ds_join-01',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-ds_join-02',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s left join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['LEFT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-ds_join-03',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s right join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['RIGHT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Projection']}},
 {'id': 'EXP-ds_join-04',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s full join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': {'pg': ['FULL', 'JOIN'], 'influx': ['FULL', 'JOIN'], 'sqlserver': ['FULL', 'JOIN']},
                 'remote_not_has': {'mysql': ['FULL']},
                 'local_has': {'mysql': ['Projection', 'Full Join']},
                 'local_not_has': {'influx': ['Projection', 'Full Join', 'Data Exchange'],
                                   'pg': ['Projection', 'Full Join', 'Data Exchange'], 'sqlserver': ['Projection', 'Full Join', 'Data Exchange']}}},
 {'id': 'EXP-ds_join-05',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region where s.voltage > 220.0 and r.area = 1',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN', 'WHERE'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_join-06',
  'sql': 'select s.`time`, s.voltage, r.area from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region order by s.`time` limit 5',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['JOIN', 'ORDER BY', 'LIMIT'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'Sort', 'limit=', 'Projection']}},
 {'id': 'EXP-ds_join-07',
  'sql': 'select r.area, count(s.voltage) from {SRC}.sensor s left join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region = r.region group by r.area',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'local_not_has'],
  'check_args': {'remote_has': ['LEFT', 'JOIN'],
                 'local_not_has': ['Inner Join', 'Data Exchange', 'GroupAggregate', 'Projection']}},
{'id': 'EXP-ds_join-08',
  'sql': 'select * from {SRC}.sensor s where exists (select 1 from {SRC}.region_info r where '
         'r.`time` = s.`time` and r.region = s.region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Not supported subquery as expr']},
  'expect_err': True},
{'id': 'EXP-ds_join-09',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.region_info r where '
         'r.`time` = s.`time` and r.region = s.region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Not supported subquery as expr']},
  'expect_err': True},
 {'id': 'EXP-ds_subquery-01',
  'sql': 'select * from {SRC}.sensor where region not in (select region from {SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-02',
  'sql': 'select * from {SRC}.sensor where region not in (select region from {SRC}.sensor where '
         'voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['NOT IN (SELECT', 'NOT IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
{'id': 'EXP-ds_subquery-03',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.region_info r where '
         'r.region = s.region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Not supported subquery as expr']},
  'expect_err': True},
 {'id': 'EXP-ds_subquery-04',
  'sql': 'select * from {SRC}.sensor s where not exists (select 1 from {SRC}.sensor r where '
         'r.voltage > 220)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT', 'EXISTS'], 'pg': ['NOT', 'EXISTS'], 'sqlserver': ['NOT', 'EXISTS']},
                 'remote_not_has': {'influx': ['NOT EXISTS']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-05',
  'sql': 'select * from {SRC}.sensor where voltage > ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-06',
  'sql': 'select * from {SRC}.sensor where voltage > SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ANY (', 'SOME (', 'ALL (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-07',
  'sql': 'select * from {SRC}.sensor where voltage = ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-08',
  'sql': 'select * from {SRC}.sensor where voltage = ANY (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-09',
  'sql': 'select * from {SRC}.sensor where voltage = SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-10',
  'sql': 'select * from {SRC}.sensor where voltage <> ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['NOT IN'], 'pg': ['NOT IN'], 'sqlserver': ['NOT IN']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-11',
  'sql': 'select * from {SRC}.sensor where voltage <> ANY (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-12',
  'sql': 'select * from {SRC}.sensor where voltage <> SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-13',
  'sql': 'select * from {SRC}.sensor where voltage < ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-14',
  'sql': 'select * from {SRC}.sensor where voltage < ANY (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-15',
  'sql': 'select * from {SRC}.sensor where voltage < SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-16',
  'sql': 'select * from {SRC}.sensor where voltage <= ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-17',
  'sql': 'select * from {SRC}.sensor where voltage <= ANY (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-18',
  'sql': 'select * from {SRC}.sensor where voltage <= SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-19',
  'sql': 'select * from {SRC}.sensor where voltage >= ALL (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-20',
  'sql': 'select * from {SRC}.sensor where voltage >= ANY (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-21',
  'sql': 'select * from {SRC}.sensor where voltage >= SOME (select voltage from {SRC}.sensor where '
         "region = 'south')",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-22',
  'sql': "select voltage > any (select voltage from {SRC}.sensor where region = 'south') from "
         '{SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['CASE', 'ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-23',
  'sql': "select voltage <= all (select voltage from {SRC}.sensor where region = 'south') from "
         '{SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['CASE', 'ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-24',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage = (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-25',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage != (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-26',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage < (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-27',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage <= (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-28',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage >= (select avg(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-29',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select min(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MIN('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-30',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select max(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-31',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select count(*) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': {'mysql': ['COUNT('], 'pg': ['COUNT('],
                                    'influx': ['COUNT('], 'sqlserver': ['COUNT_BIG(']},
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-32',
  'sql': 'select `time`, voltage from {SRC}.sensor where voltage > (select sum(voltage) from '
         '{SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['SUM('],
                 'local_not_has': ['InitPlan 1', 'Filter:', 'Projection']}},
 {'id': 'EXP-ds_subquery-33',
  'sql': 'select voltage, (select max(voltage) from {SRC}.sensor) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX('],
                 'local_not_has': ['InitPlan 1', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-34',
  'sql': 'select (select max(voltage) from {SRC}.sensor) mx from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX('],
                 'local_not_has': ['InitPlan 1', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-35',
  'sql': 'select region in (select region from {SRC}.region_info) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['CASE', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-36',
  'sql': 'select distinct region in (select region from {SRC}.region_info) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['DISTINCT', 'IN'], 'pg': ['DISTINCT', 'IN'], 'sqlserver': ['CASE', 'IN', 'DISTINCT']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'GroupAggregate', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-37',
  'sql': 'select voltage, (select max(voltage) from {SRC}.sensor) mx from {SRC}.sensor order by '
         'voltage',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX('],
                 'local_not_has': ['InitPlan 1', 'Sort', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-38',
  'sql': 'select sum((select count(*) from {SRC}.region_info)) from {SRC}.sensor',
  'sources': 'single',
 'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'min_federated_scans',
             'verbose_remote_has',
             'analyze_local_has',
             'analyze_remote_not_has'],
  'check_args': {'remote_any_has': {'mysql': ['SUM(', 'COUNT('],
                                    'pg': ['SUM(', 'COUNT(']},
                 # SQL Server: the scalar-subquery SUM pushes pre-execution, but
                 # the runtime fallback demotes it to a base scan (see
                 # analyze_remote_not_has below).
                 'verbose_remote_has': {'influx': ['SUM(', 'COUNT('],
                                        'sqlserver': ['SUM(', 'COUNT_BIG(']},
                 'analyze_remote_not_has': {'sqlserver': ['SUM(']},
                 'plain_local_has': {'influx': []},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1},
                 'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate'],
                                   'pg': ['InitPlan 1', 'Aggregate']}}},
 {'id': 'EXP-ds_subquery-39',
  'sql': "select case when voltage > (select avg(voltage) from {SRC}.sensor) then 'h' else 'l' end "
         'from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IF('], 'pg': ['CASE'], 'influx': ['CASE'], 'sqlserver': ['CASE', 'WHEN']},
                 'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-40',
  'sql': 'select case when region in (select region from {SRC}.region_info) then 1 else 0 end from '
         '{SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['IF(', 'IN'], 'pg': ['CASE', 'IN'], 'sqlserver': ['CASE', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Projection']},
                 'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                   'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']}}},
{'id': 'EXP-ds_subquery-41',
  'sql': 'select count(*) from sensor group by 1 in (select 1 from region_info)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['syntax error near "in (select 1 from region_info)"']},
  'expect_err': True,
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-ds_subquery-42',
  'sql': 'select region, count(*) cnt from {SRC}.sensor group by region having count(*) > (select '
         'count(*) from {SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_has': ['HAVING'],
                 'remote_any_has': ['COUNT('],
                 'local_not_has': ['InitPlan 1', 'GroupAggregate', 'Projection']}},
{'id': 'EXP-ds_subquery-43',
  'sql': 'select region, count(*) cnt from {SRC}.sensor group by region having region in (select '
         'region from {SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has'],
  'check_args': {'remote_has': {'mysql': ['HAVING', 'GROUP BY', 'IN'],
                                'pg': ['HAVING', 'GROUP BY', 'IN'], 'sqlserver': ['HAVING', 'GROUP BY', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'GroupAggregate', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1', 'GroupAggregate', 'Aggregate'],
                                   'pg': ['InitPlan 1', 'GroupAggregate', 'Aggregate'], 'sqlserver': ['InitPlan 1', 'GroupAggregate', 'Aggregate']}}},
 {'id': 'EXP-ds_subquery-44',
  'sql': 'select s.`time`, s.voltage from {SRC}.sensor s inner join {SRC}.region_info r on '
         's.`time` = r.`time` and s.region in (select region from {SRC}.region_info)',
  'sources': 'single',
 'checks': ['federated_scan',
             'analyze_metrics',
             'join_remote_or_multi',
             'remote_sql',
             'remote_has',
             'min_federated_scans',
             'verbose_remote_has',
             'analyze_local_has'],
  'check_args': {'remote_has': {'mysql': ['JOIN', 'IN'],
                                'pg': ['JOIN', 'IN'], 'sqlserver': ['JOIN', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'verbose_remote_has': {'influx': ['JOIN']},
                 'plain_local_has': {'influx': []},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 1, 'sqlserver': 1},
                 'analyze_local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1', 'Inner Join', 'Data Exchange'],
                                   'pg': ['InitPlan 1', 'Inner Join', 'Data Exchange'],
                                   'influx': ['Inner Join', 'Data Exchange'], 'sqlserver': ['InitPlan 1', 'Inner Join', 'Data Exchange']}}},
 {'id': 'EXP-ds_subquery-45',
  'sql': 'select * from {SRC}.sensor where voltage > 219.0 and region in (select region from '
         '{SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['WHERE', 'AND', 'IN'], 'pg': ['WHERE', 'AND', 'IN'], 'sqlserver': ['WHERE', 'AND', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-46',
  'sql': 'select * from {SRC}.sensor where voltage > 221.0 or region in (select region from '
         '{SRC}.region_info)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['WHERE', 'OR', 'IN'], 'pg': ['WHERE', 'OR', 'IN'], 'sqlserver': ['WHERE', 'OR', 'IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-47',
  'sql': 'select * from {SRC}.sensor where voltage between (select min(voltage) from {SRC}.sensor) '
         'and (select max(voltage) from {SRC}.sensor)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'local_not_has'],
  # SQL Server at M1: BETWEEN's bounds are scalar subqueries (M4), so the
  # whole predicate stays local and the remote statement is a bare scan.
  'check_args': {'remote_has': {'mysql': ['WHERE', 'BETWEEN'], 'pg': ['WHERE', 'BETWEEN'],
                                'influx': ['WHERE', 'BETWEEN'], 'sqlserver': []},
                 'remote_any_has': ['MIN(', 'MAX('],
                 'remote_not_has': {'sqlserver': ['WHERE']},
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
 {'id': 'EXP-ds_subquery-48',
  'sql': 'select region in (select region from region_info) from sensor where voltage > 221.0 '
         'union all select region in (select region from region_info) from sensor where voltage < '
         '220.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['UNION', 'IN'], 'pg': ['UNION', 'IN'], 'sqlserver': ['UNION', 'CASE', 'IN']},
                 'plain_local_has': {'influx': ['InitPlan 1', 'InitPlan 2']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1',
                                          'InitPlan 2']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-ds_subquery-49',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' and voltage > (select avg(voltage) from {SRC}.sensor) interval(1m)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'remote_not_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'GROUP BY'],
                 'remote_any_has': ['AVG('],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['InitPlan 1',
                                   'Filter:',
                                   'Interval',
                                   'Aggregate',
                                   'Projection']}},
 {'id': 'EXP-ds_subquery-50',
  'sql': 'select count(*), region from {SRC}.sensor where voltage > (select avg(voltage) from '
         '{SRC}.sensor) state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'remote_not_has': ['STATE_WINDOW'],
                 'local_has': ['StateWindow'],
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
 {'id': 'EXP-ds_subquery-51',
  'sql': 'select region, count(*) from {SRC}.sensor where voltage > (select avg(voltage) from '
         '{SRC}.sensor) partition by region',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'GROUP BY'],
                 'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1',
                                   'Filter:',
                                   'Partition',
                                   'Aggregate',
                                   'Projection']}},
 {'id': 'EXP-ds_subquery-52',
  'sql': 'select * from {SRC}.sensor where voltage >= all (select voltage from {SRC}.sensor where '
         'region is null)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ALL'], 'pg': ['ALL'], 'sqlserver': ['ALL']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-53',
  'sql': 'select * from {SRC}.sensor where voltage < any (select voltage from {SRC}.sensor where '
         'region is null)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ANY'], 'pg': ['ANY'], 'sqlserver': ['ANY']},
                 'remote_not_has': {'influx': ['ALL (', 'ANY (', 'SOME (']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
 {'id': 'EXP-ds_subquery-54',
  'sql': 'select * from {SRC}.sensor where region in (select region from {SRC}.region_info where '
         'area is null)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['IN'], 'pg': ['IN'], 'sqlserver': ['IN']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}}},
{'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'mysql': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'pg': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-55',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is '
        'not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['MAX(',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['MAX(',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-56',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' IN (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' IN (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-57',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' NOT IN (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' NOT IN (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-58',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['EXISTS (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['EXISTS (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-59',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where exists (select 1 from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-60',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where not exists (select 1 from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-61',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ALL (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ALL (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-62',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-63',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'mysql': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'pg': ['MASK_FULL(', 'MASK_PARTIAL(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-64',
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where '
        'region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['MAX(',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['MAX(',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-65',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' IN (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' IN (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-66',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' NOT IN (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' NOT IN (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-67',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['EXISTS (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['EXISTS (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-68',
 'sources': 'single',
 'sql': "select * from {SRC}.sensor where exists (select 1 from (select mask_full(region_l1, '*') "
        "as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-69',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-70',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ALL (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ALL (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-71',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'MASK_FULL(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'MASK_FULL(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'MASK_FULL(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-72',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-73',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is not '
        'null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-74',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-75',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-76',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-77',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where exists (select 1 from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is '
        'not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-78',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where not exists (select 1 from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is '
        'not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-79',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-80',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-81',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-82',
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        "region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is '
        'not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['MAX(', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-83',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' IN (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-84',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' NOT IN (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-85',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-86',
 'sources': 'single',
 'sql': "select * from {SRC}.sensor where exists (select 1 from (select mask_full(region_l1, '*') "
        "as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from (select "
        'lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': ['NOT EXISTS (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-87',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-88',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ALL (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-89',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-90',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-91',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select mask_none(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) fq0 where region_l0 is not null) fq1 order by '
        'fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-92',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-93',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select trim(region_l2) as region_l1 from (select "
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-94',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-95',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where exists (select 1 from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select mask_none(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'MASK_FULL(',
                                                  'TRIM(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'MASK_FULL(',
                                                 'TRIM(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'MASK_FULL(',
                                              'TRIM(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-96',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where not exists (select 1 from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select mask_none(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-97',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-98',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-99',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-100',
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select mask_none(region) as '
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is not null) '
        'fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MAX(', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-101',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-102',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' NOT IN (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-103',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['EXISTS (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-104',
 'sources': 'single',
 'sql': "select * from {SRC}.sensor where exists (select 1 from (select mask_full(region_l1, '*') "
        'as region_l0 from (select trim(region_l2) as region_l1 from (select mask_none(region) as '
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not '
        'null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'MASK_FULL(',
                                                  'TRIM(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'MASK_FULL(',
                                                 'TRIM(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'MASK_FULL(',
                                              'TRIM(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-105',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select trim(region_l2) as region_l1 from (select "
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-106',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ALL (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-107',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'MASK_FULL(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-108',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region '
        'is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['MASK_FULL('],
                                       'mysql': ['MASK_FULL('],
                                       'pg': ['MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-109',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) fq0 where region_l0 is not null) fq1 order by '
        'fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL('],
                                       'mysql': ['MAX(', 'MASK_FULL('],
                                       'pg': ['MAX(', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-110',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL('],
                                       'mysql': [' IN (', 'MASK_FULL('],
                                       'pg': [' IN (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-111',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL('],
                                       'mysql': [' NOT IN (', 'MASK_FULL('],
                                       'pg': [' NOT IN (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-112',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL('],
                                       'mysql': ['EXISTS (', 'MASK_FULL('],
                                       'pg': ['EXISTS (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-113',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where exists (select 1 from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'MASK_FULL('],
                                       'mysql': ['NOT EXISTS (', 'MASK_FULL('],
                                       'pg': ['NOT EXISTS (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-114',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': "select * from sensor where not exists (select 1 from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as '
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL('],
                                       'mysql': [' ANY (', 'MASK_FULL('],
                                       'pg': [' ANY (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-115',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL('],
                                       'mysql': [' ALL (', 'MASK_FULL('],
                                       'pg': [' ALL (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-116',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL('],
                                       'mysql': [' ANY (', 'MASK_FULL('],
                                       'pg': [' ANY (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-117',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['MASK_FULL('],
                                       'mysql': ['MASK_FULL('],
                                       'pg': ['MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-118',
 'sources': 'single',
 'sql': "select * from (select region_l0 as region from (select mask_full(region_l1, '*') as "
        'region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as '
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is not null) '
        'fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'MASK_FULL('],
                                       'mysql': ['MAX(', 'MASK_FULL('],
                                       'pg': ['MAX(', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-119',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'MASK_FULL('],
                                       'mysql': [' IN (', 'MASK_FULL('],
                                       'pg': [' IN (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-120',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'MASK_FULL('],
                                       'mysql': [' NOT IN (', 'MASK_FULL('],
                                       'pg': [' NOT IN (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-121',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'MASK_FULL('],
                                       'mysql': ['EXISTS (', 'MASK_FULL('],
                                       'pg': ['EXISTS (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-122',
 'sources': 'single',
 'sql': "select * from {SRC}.sensor where exists (select 1 from (select mask_full(region_l1, '*') "
        'as region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as '
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not '
        'null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'MASK_FULL('],
                                       'mysql': ['NOT EXISTS (', 'MASK_FULL('],
                                       'pg': ['NOT EXISTS (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-123',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select mask_full(region_l1, '
        "'*') as region_l0 from (select trim(region_l2) as region_l1 from (select lower(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is not '
        'null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL('],
                                       'mysql': [' ANY (', 'MASK_FULL('],
                                       'pg': [' ANY (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-124',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'MASK_FULL('],
                                       'mysql': [' ALL (', 'MASK_FULL('],
                                       'pg': [' ALL (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-125',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['TRIM(', 'LOWER('],
                                   'mysql': ['TRIM(', 'LOWER('],
                                   'pg': ['TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'MASK_FULL('],
                                       'mysql': [' ANY (', 'MASK_FULL('],
                                       'pg': [' ANY (', 'MASK_FULL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-126',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        "mask_full(region_l1, '*') as region_l0 from (select trim(region_l2) as region_l1 from "
        '(select lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is '
        'not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'mysql': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'pg': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-127',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'fq0 where region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['MAX(',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['MAX(',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-128',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' IN (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' IN (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-129',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select substring(region_l1, '
        "1, length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' NOT IN (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' NOT IN (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-130',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['EXISTS (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['EXISTS (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-131',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        're0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-132',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where not exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        're0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-133',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ALL (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ALL (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-134',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-135',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from region_info where area >= 1 and region is not null) rr2 where region_l2 is '
        'not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'mysql': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE('],
                                       'pg': ['SUBSTRING(', 'MASK_PARTIAL(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-136',
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) fq0 where region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['MAX(',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['MAX(',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-137',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' IN (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' IN (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-138',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' NOT IN (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' NOT IN (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-139',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['EXISTS (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['EXISTS (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-140',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-141',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select substring(region_l1, '
        "1, length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) re0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-142',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ALL (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ALL (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-143',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (',
                                                  'SUBSTRING(',
                                                  'MASK_PARTIAL(',
                                                  'MASK_NONE('],
                                       'mysql': [' ANY (',
                                                 'SUBSTRING(',
                                                 'MASK_PARTIAL(',
                                                 'MASK_NONE('],
                                       'pg': [' ANY (',
                                              'SUBSTRING(',
                                              'MASK_PARTIAL(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-144',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select mask_none(region) as "
        'region_l2 from {SRC}.region_info where area >= 1 and region is not null) rr2 where '
        'region_l2 is not null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-145',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 '
        'where region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-146',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from region_info where area >= 1 and region is not null) rr2 where region_l2 is not null) '
        'rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-147',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select substring(region_l1, '
        "1, length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-148',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from region_info where area >= 1 and region is not null) rr2 where region_l2 is not null) '
        'rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-149',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 '
        'where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-150',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where not exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 '
        'where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-151',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from region_info where area >= 1 and region is not null) rr2 where region_l2 is not null) '
        'rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-152',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from region_info where area >= 1 and region is not null) rr2 where region_l2 is not null) '
        'rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-153',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from region_info where area >= 1 and region is not null) rr2 where region_l2 is not null) '
        'rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-154',
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'fq0 where region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['MAX(', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-155',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' IN (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-156',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' NOT IN (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-157',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-158',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where exists (select 1 from (select substring(region_l1, 1, '
        "length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        're0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': ['NOT EXISTS (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-159',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select substring(region_l1, '
        "1, length(region_l1)) as region_l0 from (select mask_partial(region_l2, 1, 1, '#') as "
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        're0 where re0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-160',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ALL (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-161',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_has': {'influx': ['LOWER('], 'mysql': ['LOWER('], 'pg': ['LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'MASK_PARTIAL(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-162',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select '
        "mask_partial(region_l2, 1, 1, '#') as region_l1 from (select lower(region) as region_l2 "
        'from {SRC}.region_info where area >= 1 and region is not null) rr2 where region_l2 is not '
        'null) rr1 where region_l1 is not null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-163',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is '
        'not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-164',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-165',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select substring(region_l1, '
        '1, length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-166',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-167',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'SUBSTRING(',
                                                  'TRIM(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'SUBSTRING(',
                                                 'TRIM(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'SUBSTRING(',
                                              'TRIM(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-168',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where not exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-169',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-170',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-171',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['Sort ', 'Projection'],
                                      'mysql': ['Sort ', 'Projection'],
                                      'pg': ['Sort ', 'Projection']},
                'local_has': {'influx': ['Sort ', 'Projection'],
                              'mysql': ['Sort ', 'Projection'],
                              'pg': ['Sort ', 'Projection']},
                'plain_local_has': {'influx': ['Sort ', 'Projection'],
                                    'mysql': ['Sort ', 'Projection'],
                                    'pg': ['Sort ', 'Projection']},
                'remote_any_not_has': {'influx': ['SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has'],
 'id': 'EXP-ds_subquery-172',
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where '
        'region_l0 is not null) fq1 order by fq1.region'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['MAX(', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-173',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rs0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-174',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-175',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) ri0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-176',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Projection'],
                                      'pg': ['InitPlan 1', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Projection'],
                              'mysql': ['InitPlan 1', 'Projection'],
                              'pg': ['InitPlan 1', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Projection'],
                                    'pg': ['InitPlan 1', 'Projection']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (',
                                                  'SUBSTRING(',
                                                  'TRIM(',
                                                  'MASK_NONE('],
                                       'mysql': ['NOT EXISTS (',
                                                 'SUBSTRING(',
                                                 'TRIM(',
                                                 'MASK_NONE('],
                                       'pg': ['NOT EXISTS (',
                                              'SUBSTRING(',
                                              'TRIM(',
                                              'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-177',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select substring(region_l1, '
        '1, length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'mask_none(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not '
        'null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where '
        're0.region_l0 is not null)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-178',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ALL (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-179',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'analyze_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                      'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                              'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': 2,
                'plain_local_has': {'influx': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                    'pg': ['InitPlan 1', 'Aggregate', 'Projection']},
                'remote_any_not_has': {'influx': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE('],
                                       'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'MASK_NONE(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_not_has',
            'local_has',
            'analyze_local_has',
            'min_federated_scans'],
 'id': 'EXP-ds_subquery-180',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select mask_none(region) as region_l2 from {SRC}.region_info where area '
        '>= 1 and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not '
        'null) rq0)'},
 {'check_args': {'local_has': {'influx': ['Sort '], 'mysql': ['Sort '], 'pg': ['Sort ']},
                'local_not_has': ['Projection'],
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['SUBSTRING(', 'TRIM(', 'LOWER(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'subquery_handling',
            'remote_any_has',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-181',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is not '
        'null) fq1 order by fq1.region'},
 {'check_args': {'local_not_has': {'influx': ['InitPlan 1', 'Filter:'],
                                  'mysql': ['InitPlan 1', 'Filter:'],
                                  'pg': ['InitPlan 1', 'Filter:'], 'sqlserver': ['InitPlan 1', 'Filter:']},
                'remote_any_has': {'influx': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'subquery_handling',
            'remote_any_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-182',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' IN (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-183',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region in (select region_l0 from (select substring(region_l1, '
        '1, length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) ri0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-184',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Projection']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-185',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is '
        'not null)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Projection']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-186',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from sensor where not exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from region_info where area >= 1 and region is not null) rr2 '
        'where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 is '
        'not null)'},
{'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-187',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-188',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-189',
 'pre_sql': ['use {SRC}'],
 'sources': 'single',
 'sql': 'select * from region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from region_info where area >= 1 and '
        'region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'local_has': {'influx': ['Sort '], 'mysql': ['Sort '], 'pg': ['Sort ']},
                'local_not_has': ['Projection'],
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['SUBSTRING(', 'TRIM(', 'LOWER(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'subquery_handling',
            'remote_any_has',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-190',
 'sources': 'single',
 'sql': 'select * from (select region_l0 as region from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) fq0 where region_l0 is '
        'not null) fq1 order by fq1.region'},
 {'check_args': {'local_not_has': {'influx': ['InitPlan 1', 'Filter:'],
                                  'mysql': ['InitPlan 1', 'Filter:'],
                                  'pg': ['InitPlan 1', 'Filter:'], 'sqlserver': ['InitPlan 1', 'Filter:']},
                'remote_any_has': {'influx': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['MAX(', 'SUBSTRING(', 'TRIM(', 'LOWER(']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'subquery_handling',
            'remote_any_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-191',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > (select max(region_l0) from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rs0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' IN (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' IN (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-192',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' NOT IN (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' NOT IN (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-193',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where region not in (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'ri0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Projection']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['EXISTS (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-194',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where exists (select 1 from (select substring(region_l1, 1, '
        'length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Projection']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Projection'],
                                  'pg': ['InitPlan 1', 'Projection'], 'sqlserver': ['InitPlan 1', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': ['NOT EXISTS (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': ['NOT EXISTS (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-195',
 'sources': 'single',
 'sql': 'select * from {SRC}.sensor where not exists (select 1 from (select substring(region_l1, '
        '1, length(region_l1)) as region_l0 from (select trim(region_l2) as region_l1 from (select '
        'lower(region) as region_l2 from {SRC}.region_info where area >= 1 and region is not null) '
        'rr2 where region_l2 is not null) rr1 where region_l1 is not null) re0 where re0.region_l0 '
        'is not null)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-196',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > any (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ALL (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ALL (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-197',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region <= all (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'check_args': {'local_has': {'influx': ['InitPlan 1', 'Aggregate']},
                'local_not_has': {'mysql': ['InitPlan 1', 'Aggregate', 'Projection'],
                                  'pg': ['InitPlan 1', 'Aggregate', 'Projection'], 'sqlserver': ['InitPlan 1', 'Aggregate', 'Projection']},
                'min_scans': {'influx': 2, 'mysql': 1, 'pg': 1, 'sqlserver': 1},
                'remote_any_has': {'influx': ['SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'mysql': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('],
                                   'pg': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER('], 'sqlserver': [' ANY (', 'SUBSTRING(', 'TRIM(', 'LOWER(']},
                'remote_any_not_has': {'influx': [' ANY (']}},
 'checks': ['federated_scan',
            'analyze_metrics',
            'remote_sql',
            'remote_any_has',
            'remote_any_not_has',
            'min_federated_scans',
            'local_has',
            'local_not_has'],
 'id': 'EXP-ds_subquery-198',
 'sources': 'single',
 'sql': 'select * from {SRC}.region_info where region > some (select region_l0 from (select '
        'substring(region_l1, 1, length(region_l1)) as region_l0 from (select trim(region_l2) as '
        'region_l1 from (select lower(region) as region_l2 from {SRC}.region_info where area >= 1 '
        'and region is not null) rr2 where region_l2 is not null) rr1 where region_l1 is not null) '
        'rq0)'},
 {'id': 'EXP-ds_subquery-199',
  'sql': 'select (select max(voltage) from {SRC}.sensor) mx, * from {SRC}.sensor order by voltage',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX('],
                 'local_not_has': ['InitPlan 1', 'Sort', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-200',
  'sql': 'select (select max(voltage) from {SRC}.sensor) mx, (select min(voltage) from '
         '{SRC}.sensor) mn from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'subquery_handling',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['MAX(', 'MIN('],
                 'local_not_has': ['InitPlan 1', 'Aggregate', 'Projection']}},
 {'id': 'EXP-ds_subquery-201',
  'sql': 'select * from {SRC}.sensor where voltage > cast((select avg(voltage) from {SRC}.sensor) '
         'as double)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_any_has',
             'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'CAST'],
                 'remote_any_has': ['AVG('],
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
 {'id': 'EXP-ds_subquery-202',
  'sql': 'select region in (select region from region_info) from sensor where voltage > 221.0 '
         'union select region in (select region from region_info) from sensor where voltage < 220.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'min_federated_scans',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['UNION', 'IN'], 'pg': ['UNION', 'IN'], 'sqlserver': ['UNION', 'CASE', 'IN']},
                 'plain_local_has': {'influx': ['InitPlan 1', 'InitPlan 2']},
                 'remote_not_has': {'influx': ['IN (SELECT', 'IN(SELECT']},
                 'min_scans': {'mysql': 1, 'pg': 1, 'influx': 2, 'sqlserver': 1},
                 'local_has': {'influx': ['InitPlan 1',
                                          'InitPlan 2']},
                 'local_not_has': {'mysql': ['InitPlan 1'], 'pg': ['InitPlan 1'], 'sqlserver': ['InitPlan 1']}},
  'pre_sql': 'use {SRC}'},
 {'id': 'EXP-ds_subquery-203',
  'sql': 'select count(*) from {SRC}.sensor where voltage > (select avg(voltage) from {SRC}.sensor) '
         'session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'remote_not_has': ['SESSION('],
                 'local_has': ['Session'],
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
 {'id': 'EXP-ds_subquery-204',
  'sql': 'select count(*) from {SRC}.sensor where voltage > (select avg(voltage) from {SRC}.sensor) '
         'event_window start with voltage > 220 end with voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'remote_not_has': ['EVENT_WINDOW'],
                 'local_has': ['Event'],
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
 {'id': 'EXP-ds_subquery-205',
  'sql': 'select count(*) from {SRC}.sensor where voltage > (select avg(voltage) from {SRC}.sensor) '
         'count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_any_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_any_has': ['AVG('],
                 'remote_not_has': ['COUNT_WINDOW'],
                 'local_has': ['Count'],
                 'local_not_has': ['InitPlan 1', 'Filter:']}},
{'id': 'EXP-ds_clause-01',
  'sql': 'select `time`, voltage from {SRC}.sensor partition by region slimit 1 soffset 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'SOFFSET'],
                 'local_has': {'mysql': ['Projection', 'Partition'],
                               'pg': ['Projection', 'Partition'],
                               'influx': ['Projection', 'Partition']}}},
 {'id': 'EXP-ds_clause-02',
  'sql': 'select region from {SRC}.sensor union select region from {SRC2}.sensor',
  'sources': 'cross',
  'checks': ['min_federated_scans',
             'no_table_scan',
             'analyze_metrics',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['UNION'],
                 'min_scans': 2,
                 'local_has': {'mysql_pg': ['GroupAggregate', 'Data Exchange'],
                               'mysql_influx': ['GroupAggregate', 'Data Exchange'],
                               'pg_influx': ['GroupAggregate', 'Data Exchange']}}},
 {'id': 'EXP-ds_clause-03',
  'sql': 'select /*+ batch_scan() */ `time`, voltage from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_not_has': ['/*+', 'BATCH_SCAN'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-gen_F_F-01',
  'sql': "select abs(voltage - 220.0), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['ABS', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-02',
  'sql': 'select abs(voltage - 220.0), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['ABS', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-03',
  'sql': "select upper(region), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['UPPER', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-04',
  'sql': 'select upper(region), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['UPPER', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-05',
  'sql': "select round(voltage, 1), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['ROUND', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-06',
  'sql': 'select round(voltage, 1), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['ROUND', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-07',
  'sql': "select length(region), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['CHAR_LENGTH', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-08',
  'sql': 'select length(region), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['CHAR_LENGTH', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-09',
  'sql': "select md5(region), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-10',
  'sql': 'select md5(region), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-11',
  'sql': "select cast(voltage as bigint), MASK_FULL(region, '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['CAST', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_F-12',
  'sql': 'select cast(voltage as bigint), to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['CAST', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_O-01',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-02',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-03',
  'sql': "select abs(voltage - 220.0) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-04',
  'sql': "select abs(voltage - 220.0) from {SRC}.sensor where voltage > 220 and region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-05',
  'sql': "select abs(voltage - 220.0) from {SRC}.sensor where voltage > 222 or region = 'south'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-06',
  'sql': 'select upper(region) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-07',
  'sql': 'select upper(region) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-08',
  'sql': "select upper(region) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-09',
  'sql': "select upper(region) from {SRC}.sensor where voltage > 220 and region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-10',
  'sql': "select upper(region) from {SRC}.sensor where voltage > 222 or region = 'south'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-11',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-12',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-13',
  'sql': "select round(voltage, 1) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-14',
  'sql': "select round(voltage, 1) from {SRC}.sensor where voltage > 220 and region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-15',
  'sql': "select round(voltage, 1) from {SRC}.sensor where voltage > 222 or region = 'south'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-16',
  'sql': 'select length(region) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-17',
  'sql': 'select length(region) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-18',
  'sql': "select length(region) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-19',
  'sql': "select length(region) from {SRC}.sensor where voltage > 220 and region = 'north'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_O-20',
  'sql': "select length(region) from {SRC}.sensor where voltage > 222 or region = 'south'",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_LF_O-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-02',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-03',
  'sql': "select csum(voltage) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-04',
  'sql': 'select diff(voltage) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-05',
  'sql': 'select diff(voltage) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-06',
  'sql': "select diff(voltage) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-07',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-08',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-09',
  'sql': "select derivative(voltage, 1s, 0) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-10',
  'sql': 'select mavg(voltage, 3) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MAVG'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-11',
  'sql': 'select mavg(voltage, 3) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MAVG'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LF_O-12',
  'sql': "select mavg(voltage, 3) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MAVG'],
                 'local_has': {'mysql': ['Indefinite Rows Function'],
                               'pg': ['Indefinite Rows Function'],
                               'influx': ['Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_mask_full_between-01',
  'sql': "select MASK_FULL(region, '*') from {SRC}.sensor where voltage between 219 and 222",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_mask_full_in-01',
  'sql': "select MASK_FULL(region, '*') from {SRC}.sensor where voltage in (220, 221)",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_mask_full_like-01',
  'sql': "select MASK_FULL(region, '*') from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['MASK_FULL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_to_iso8601_between-01',
  'sql': 'select to_iso8601(`time`) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_to_iso8601_in-01',
  'sql': 'select to_iso8601(`time`) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLS_O_to_iso8601_like-01',
  'sql': "select to_iso8601(`time`) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_twa_between-01',
  'sql': 'select twa(voltage) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_twa_in-01',
  'sql': 'select twa(voltage) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_twa_like-01',
  'sql': "select twa(voltage) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_irate_between-01',
  'sql': 'select irate(voltage) from {SRC}.sensor where voltage between 219 and 222',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_irate_in-01',
  'sql': 'select irate(voltage) from {SRC}.sensor where voltage in (220, 221)',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genLA_O_irate_like-01',
  'sql': "select irate(voltage) from {SRC}.sensor where region like 'no%'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE'],
                 'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
{'id': 'EXP-gen_F_W-01',
  'sql': 'select count(*), abs(voltage - 220.0) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-02',
  'sql': 'select count(*), abs(voltage - 220.0) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-03',
  'sql': 'select count(*), abs(voltage - 220.0) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-04',
  'sql': 'select count(*), abs(voltage - 220.0) from {SRC}.sensor event_window start with voltage '
         '> 220 end with voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
 {'id': 'EXP-gen_F_W-05',
  'sql': 'select count(*), upper(region) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['UPPER', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'StateWindow'],
                               'pg': ['Projection', 'StateWindow'],
                               'influx': ['Projection', 'StateWindow']}}},
 {'id': 'EXP-gen_F_W_session_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SESSION('],
                 'local_has': {'mysql': ['Session'],
                               'pg': ['Session'],
                               'influx': ['Session']}}},
{'id': 'EXP-gen_F_W-06',
  'sql': 'select count(*), upper(region) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_F_W_count_window_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'],
                               'pg': ['Count'],
                               'influx': ['Count']}}},
{'id': 'EXP-gen_F_W-07',
  'sql': 'select count(*), upper(region) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_F_W_event_window_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor event_window start with voltage > 220 end with '
         'voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'],
                               'pg': ['Event'],
                               'influx': ['Event']}}},
{'id': 'EXP-gen_F_W-08',
  'sql': 'select count(*), upper(region) from {SRC}.sensor event_window start with voltage > 220 '
         'end with voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-09',
  'sql': 'select count(*), round(voltage, 1) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-10',
  'sql': 'select count(*), round(voltage, 1) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-11',
  'sql': 'select count(*), round(voltage, 1) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-12',
  'sql': 'select count(*), round(voltage, 1) from {SRC}.sensor event_window start with voltage > '
         '220 end with voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: voltage']},
  'expect_err': True},
 {'id': 'EXP-gen_F_W-13',
  'sql': 'select count(*), length(region) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['LENGTH', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'StateWindow'],
                               'pg': ['Projection', 'StateWindow'],
                               'influx': ['Projection', 'StateWindow']}}},
{'id': 'EXP-gen_F_W-14',
  'sql': 'select count(*), length(region) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-15',
  'sql': 'select count(*), length(region) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
{'id': 'EXP-gen_F_W-16',
  'sql': 'select count(*), length(region) from {SRC}.sensor event_window start with voltage > 220 '
         'end with voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_LF_W-01',
  'sql': 'select csum(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-gen_LF_W-02',
  'sql': 'select csum(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CSUM', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-gen_LF_W-03',
  'sql': 'select diff(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DIFF', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-gen_LF_W-04',
  'sql': 'select diff(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DIFF', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-gen_LF_W-05',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DERIVATIVE', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-gen_LF_W-06',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['DERIVATIVE', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-gen_LF_W-07',
  'sql': 'select mavg(voltage, 3) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MAVG', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-gen_LF_W-08',
  'sql': 'select mavg(voltage, 3) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MAVG', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genLS_W_mask_full_state_window-01',
  'sql': "select count(*), MASK_FULL(region, '*') from {SRC}.sensor state_window(region)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'StateWindow'],
                               'pg': ['Projection', 'StateWindow'],
                               'influx': ['Projection', 'StateWindow']}}},
{'id': 'EXP-genLS_W_mask_full_session-01',
  'sql': "select count(*), MASK_FULL(region, '*') from {SRC}.sensor session(`time`, 1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-genLS_W_mask_full_session-02',
  'sql': "select count(*), MASK_FULL(to_iso8601(_wstart), '*') from {SRC}.sensor session(`time`, 1m)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'TO_ISO8601', 'SESSION('],
                 'local_has': {'mysql': ['Projection', 'Session'],
                               'pg': ['Projection', 'Session'],
                               'influx': ['Projection', 'Session']}}},
 {'id': 'EXP-genLS_W_to_iso8601_plain-01',
  'sql': 'select to_iso8601(`time`) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
{'id': 'EXP-genLS_W_to_iso8601_state_window-01',
  'sql': 'select count(*), to_iso8601(`time`) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: time']},
  'expect_err': True},
 {'id': 'EXP-genLS_W_to_iso8601_state_window-02',
  'sql': 'select count(*), to_iso8601(_wstart) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['Projection', 'StateWindow'],
                               'pg': ['Projection', 'StateWindow'],
                               'influx': ['Projection', 'StateWindow']}}},
{'id': 'EXP-genLS_W_to_iso8601_session-01',
  'sql': 'select count(*), to_iso8601(`time`) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: time']},
  'expect_err': True},
 {'id': 'EXP-genLS_W_to_iso8601_session-02',
  'sql': 'select count(*), to_iso8601(_wstart) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601', 'SESSION('],
                 'local_has': {'mysql': ['Projection', 'Session'],
                               'pg': ['Projection', 'Session'],
                               'influx': ['Projection', 'Session']}}},
 {'id': 'EXP-genLA_W_twa_state_window-01',
  'sql': 'select twa(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-genLA_W_twa_session-01',
  'sql': 'select twa(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genLA_W_irate_state_window-01',
  'sql': 'select irate(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['IRATE', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-genLA_W_irate_session-01',
  'sql': 'select irate(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['IRATE', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genAA_count_twa-01',
  'sql': 'select count(*), twa(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAA_count_irate-01',
  'sql': 'select count(*), irate(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAA_avg_twa-01',
  'sql': 'select avg(voltage), twa(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAA_avg_irate-01',
  'sql': 'select avg(voltage), irate(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAA_sum_twa-01',
  'sql': 'select sum(voltage), twa(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAA_sum_irate-01',
  'sql': 'select sum(voltage), irate(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genSA_pp_abs_count-01',
  'sql': 'select abs(voltage - 220.0), count(*) from {SRC}.sensor group by abs(voltage - 220.0)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'COUNT', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pp_abs_avg-01',
  'sql': 'select abs(voltage - 220.0), avg(voltage) from {SRC}.sensor group by abs(voltage - '
         '220.0)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'AVG', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pp_upper_count-01',
  'sql': 'select upper(region), count(*) from {SRC}.sensor group by upper(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'COUNT', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pp_upper_avg-01',
  'sql': 'select upper(region), avg(voltage) from {SRC}.sensor group by upper(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'AVG', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pp_round_count-01',
  'sql': 'select round(voltage, 1), count(*) from {SRC}.sensor group by round(voltage, 1)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'COUNT', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pp_round_avg-01',
  'sql': 'select round(voltage, 1), avg(voltage) from {SRC}.sensor group by round(voltage, 1)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'AVG', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-genSA_pl_abs_twa-01',
  'sql': 'select twa(voltage) from {SRC}.sensor where abs(voltage - 220.0) < 5.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ABS'],
                 'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genSA_pl_abs_irate-01',
  'sql': 'select irate(voltage) from {SRC}.sensor where abs(voltage - 220.0) < 5.0',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ABS'],
                 'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genSA_pl_upper_twa-01',
  'sql': "select twa(voltage) from {SRC}.sensor where upper(region) = 'NORTH'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'UPPER'],
                 'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genSA_pl_upper_irate-01',
  'sql': "select irate(voltage) from {SRC}.sensor where upper(region) = 'NORTH'",
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'UPPER'],
                 'remote_not_has': ['IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genSA_ll_mask_full_twa-01',
  'sql': "select MASK_FULL(region, '*'), twa(voltage) from {SRC}.sensor group by MASK_FULL(region, '*')",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'TWA'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-genSA_ll_mask_full_irate-01',
  'sql': "select MASK_FULL(region, '*'), irate(voltage) from {SRC}.sensor group by MASK_FULL(region, '*')",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'IRATE'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-genSA_ll_to_iso8601_twa-01',
  'sql': 'select to_iso8601(`time`), twa(voltage) from {SRC}.sensor group by to_iso8601(`time`)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601', 'TWA'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-genSA_ll_to_iso8601_irate-01',
  'sql': 'select to_iso8601(`time`), irate(voltage) from {SRC}.sensor group by to_iso8601(`time`)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601', 'IRATE'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-genPP_abs_upper-01',
  'sql': 'select abs(voltage - 220.0), upper(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'UPPER'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-genPP_upper_round-01',
  'sql': 'select upper(region), round(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'ROUND'], 'local_not_has': ['Projection']}},
 {'id': 'EXP-genPP_round_length-01',
  'sql': 'select round(voltage, 1), length(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'LENGTH'], 'local_not_has': ['Projection']}},
{'id': 'EXP-genPP_length_md5-01',
  'sql': 'select length(region), md5(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['CHAR_LENGTH', 'MD5'],
                 'local_has': ['Projection']}},
 {'id': 'EXP-genLL_mask_full_to_iso8601-01',
  'sql': "select MASK_FULL(region, '*'), to_iso8601(`time`) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-genLL_to_iso8601_mask_partial-01',
  'sql': "select to_iso8601(`time`), MASK_PARTIAL(region, 1, 1, '#') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TO_ISO8601', 'MASK_PARTIAL'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-genLL_mask_partial_mask_none-01',
  'sql': "select MASK_PARTIAL(region, 1, 1, '#'), MASK_NONE(region) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_PARTIAL', 'MASK_NONE'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-genPA_count_avg-01',
  'sql': 'select count(*), avg(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['COUNT', 'AVG'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genPA_avg_sum-01',
  'sql': 'select avg(voltage), sum(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['AVG', 'SUM'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genPA_sum_min-01',
  'sql': 'select sum(voltage), min(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['SUM', 'MIN'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genPA_min_max-01',
  'sql': 'select min(voltage), max(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MIN', 'MAX'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genLA_twa_irate-01',
  'sql': 'select twa(voltage), irate(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'IRATE'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
{'id': 'EXP-genLA_irate_statecount-01',
  'sql': "select irate(voltage), statecount(voltage, 'GT', 220) from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Indefinite rows function 'statecount' mixed with other multi-row functions"]},
  'expect_err': True},
 {'id': 'EXP-genLA_irate_statecount-02',
  'sql': 'select irate(voltage), twa(voltage) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['IRATE', 'TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genAW_count_state_window-01',
  'sql': 'select count(*) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-genAW_count_session-01',
  'sql': 'select count(*) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genAW_count_count_window-01',
  'sql': 'select count(*) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'], 'pg': ['Count'], 'influx': ['Count']}}},
 {'id': 'EXP-genAW_count_event_window-01',
  'sql': 'select count(*) from {SRC}.sensor event_window start with voltage > 220 end with voltage '
         '< 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['COUNT', 'EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'], 'pg': ['Event'], 'influx': ['Event']}}},
 {'id': 'EXP-genAW_avg_state_window-01',
  'sql': 'select avg(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-genAW_avg_session-01',
  'sql': 'select avg(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genAW_avg_count_window-01',
  'sql': 'select avg(voltage) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'], 'pg': ['Count'], 'influx': ['Count']}}},
 {'id': 'EXP-genAW_avg_event_window-01',
  'sql': 'select avg(voltage) from {SRC}.sensor event_window start with voltage > 220 end with '
         'voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['AVG', 'EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'], 'pg': ['Event'], 'influx': ['Event']}}},
 {'id': 'EXP-genAW_sum_state_window-01',
  'sql': 'select sum(voltage) from {SRC}.sensor state_window(region)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'STATE_WINDOW'],
                 'local_has': {'mysql': ['StateWindow'],
                               'pg': ['StateWindow'],
                               'influx': ['StateWindow']}}},
 {'id': 'EXP-genAW_sum_session-01',
  'sql': 'select sum(voltage) from {SRC}.sensor session(`time`, 1m)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'SESSION('],
                 'local_has': {'mysql': ['Session'], 'pg': ['Session'], 'influx': ['Session']}}},
 {'id': 'EXP-genAW_sum_count_window-01',
  'sql': 'select sum(voltage) from {SRC}.sensor count_window(2)',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'COUNT_WINDOW'],
                 'local_has': {'mysql': ['Count'], 'pg': ['Count'], 'influx': ['Count']}}},
 {'id': 'EXP-genAW_sum_event_window-01',
  'sql': 'select sum(voltage) from {SRC}.sensor event_window start with voltage > 220 end with '
         'voltage < 220',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SUM', 'EVENT_WINDOW'],
                 'local_has': {'mysql': ['Event'], 'pg': ['Event'], 'influx': ['Event']}}},
 {'id': 'EXP-gen_F_C-01',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-02',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-03',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-04',
  'sql': 'select distinct abs(voltage - 220.0) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-05',
  'sql': 'select region, count(abs(voltage - 220.0)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-06',
  'sql': 'select region, count(abs(voltage - 220.0)) cnt from {SRC}.sensor group by region having '
         'count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-07',
  'sql': "select case when abs(voltage - 220.0) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ABS', 'IF('],
                                'pg': ['ABS', 'CASE', 'WHEN'],
                                'influx': ['ABS', 'CASE', 'WHEN'], 'sqlserver': ['ABS', 'CASE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-gen_F_C-08',
  'sql': 'select upper(region) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-09',
  'sql': 'select upper(region) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-10',
  'sql': 'select upper(region) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-11',
  'sql': 'select distinct upper(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-12',
  'sql': 'select region, count(upper(region)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-13',
  'sql': 'select region, count(upper(region)) cnt from {SRC}.sensor group by region having '
         'count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
{'id': 'EXP-gen_F_C-14',
  'sql': "select case when upper(region) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
 'checks': ['federated_scan',
             'analyze_metrics',
             'remote_has',
             'local_not_has',
             'verbose_remote_has',
             'analyze_remote_not_has'],
  'check_args': {'remote_has': {'mysql': ['UPPER', 'IF('],
                                'pg': ['UPPER', 'CASE', 'WHEN'],
                                'influx': ['UPPER', 'CASE', 'WHEN']},
                 # SQL Server: UPPER pushes pre-execution; the runtime
                 # type-clash fallback demotes it to a base scan (see
                 # analyze_remote_not_has below).
                 'verbose_remote_has': {'sqlserver': ['UPPER', 'CASE']},
                 'analyze_remote_not_has': {'sqlserver': ['UPPER']},
                 'local_not_has': {'mysql': ['Projection'],
                                   'pg': ['Projection'],
                                   'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_C-15',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-16',
  'sql': 'select round(voltage, 1) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-17',
  'sql': 'select round(voltage, 1) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-18',
  'sql': 'select distinct round(voltage, 1) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-19',
  'sql': 'select region, count(round(voltage, 1)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-20',
  'sql': 'select region, count(round(voltage, 1)) cnt from {SRC}.sensor group by region having '
         'count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-21',
  'sql': "select case when round(voltage, 1) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ROUND', 'IF('],
                                'pg': ['ROUND', 'CASE', 'WHEN'],
                                'influx': ['ROUND', 'CASE', 'WHEN'], 'sqlserver': ['ROUND', 'CASE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-gen_F_C-22',
  'sql': 'select length(region) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-23',
  'sql': 'select length(region) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-24',
  'sql': 'select length(region) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-25',
  'sql': 'select distinct length(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
{'id': 'EXP-gen_F_C-26',
  'sql': 'select region, count(length(region)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-27',
  'sql': 'select region, count(length(region)) cnt from {SRC}.sensor group by region having '
         'count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CHAR_LENGTH', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-28',
  'sql': "select case when length(region) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['CHAR_LENGTH', 'IF('],
                                'pg': ['CHAR_LENGTH', 'CASE', 'WHEN'],
                                'influx': ['CHAR_LENGTH', 'CASE', 'WHEN'], 'sqlserver': ['LEN(', 'CASE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-gen_F_C-29',
  'sql': 'select md5(region) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5'],
                 'remote_has': ['WHERE'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_C-30',
  'sql': 'select md5(region) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5'],
                 'remote_has': ['ORDER BY'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_C-31',
  'sql': 'select md5(region) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_C-32',
  'sql': 'select distinct md5(region) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'DISTINCT'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-gen_F_C-33',
  'sql': 'select region, count(md5(region)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'GROUP BY'],
                 'local_has': {'mysql': ['GroupAggregate'],
                               'pg': ['GroupAggregate'],
                               'influx': ['GroupAggregate']}}},
 {'id': 'EXP-gen_F_C-34',
  'sql': 'select region, count(md5(region)) cnt from {SRC}.sensor group by region having count(*) '
         '> 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'HAVING'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'GroupAggregate']}}},
 {'id': 'EXP-gen_F_C-35',
  'sql': "select case when md5(region) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
 'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_not_has': ['MD5', 'IF(', 'CASE', 'WHEN'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-gen_F_C-43',
  'sql': 'select md5(cast(voltage as varchar(32))) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-44',
  'sql': 'select md5(cast(voltage as varchar(32))) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-45',
  'sql': 'select md5(cast(voltage as varchar(32))) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-46',
  'sql': 'select distinct md5(cast(voltage as varchar(32))) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-47',
  'sql': 'select region, count(md5(cast(voltage as varchar(32)))) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-48',
  'sql': 'select region, count(md5(cast(voltage as varchar(32)))) cnt from {SRC}.sensor group by region having count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['MD5', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-49',
  'sql': "select case when md5(cast(voltage as varchar(32))) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['MD5', 'IF('],
                                'pg': ['MD5', 'CASE', 'WHEN'],
                                'influx': ['MD5', 'CASE', 'WHEN']},
                 # The whole md5 CASE is pushed to SQL Server, but the remote
                 # returns a single expr_1 that the local plan must re-project
                 # (aliasing) — a thin top Projection is expected there.  The
                 # other three sources surface the leaf directly, so they keep
                 # the no-Projection guarantee.
                 'local_not_has': {'mysql': ['Projection'],
                                   'pg': ['Projection'],
                                   'influx': ['Projection'],
                                   'sqlserver': []}}},
 {'id': 'EXP-gen_F_C-36',
  'sql': 'select cast(voltage as bigint) from {SRC}.sensor where voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'WHERE'], 'local_not_has': ['Filter:', 'Projection']}},
 {'id': 'EXP-gen_F_C-37',
  'sql': 'select cast(voltage as bigint) from {SRC}.sensor order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'ORDER BY'], 'local_not_has': ['Sort', 'Projection']}},
 {'id': 'EXP-gen_F_C-38',
  'sql': 'select cast(voltage as bigint) from {SRC}.sensor limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'LIMIT'], 'local_not_has': ['limit=', 'Projection']}},
 {'id': 'EXP-gen_F_C-39',
  'sql': 'select distinct cast(voltage as bigint) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'DISTINCT'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-40',
  'sql': 'select region, count(cast(voltage as bigint)) cnt from {SRC}.sensor group by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'GROUP BY'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-41',
  'sql': 'select region, count(cast(voltage as bigint)) cnt from {SRC}.sensor group by region '
         'having count(*) > 0',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['CAST', 'GROUP BY', 'HAVING'],
                 'local_not_has': ['GroupAggregate', 'Projection']}},
 {'id': 'EXP-gen_F_C-42',
  'sql': "select case when cast(voltage as bigint) > 0 then 'a' else 'b' end from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['CAST', 'IF('],
                                'pg': ['CAST', 'CASE', 'WHEN'],
                                'influx': ['CAST', 'CASE', 'WHEN'], 'sqlserver': ['CAST', 'CASE']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-gen_LF_C-01',
  'sql': 'select csum(voltage) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'CSUM'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'pg': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'influx': ['Projection', 'Indefinite Rows Function', 'Partition']}}},
{'id': 'EXP-gen_LF_C-02',
  'sql': "select _wstart, count(csum(voltage)) from {SRC}.sensor where `time` >= '2024-01-01' and "
         "`time` <= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
{'id': 'EXP-gen_LF_C-03',
  'sql': "select interp(csum(voltage)) from {SRC}.sensor range('2024-01-01 00:00:00', '2024-01-01 "
         "00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Interp function 'interp' is not allowed with aggregate, multi-rows, or indefinite rows functions"]},
  'expect_err': True},
 {'id': 'EXP-gen_LF_C-04',
  'sql': 'select diff(voltage) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'DIFF'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'pg': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'influx': ['Projection', 'Indefinite Rows Function', 'Partition']}}},
{'id': 'EXP-gen_LF_C-05',
  'sql': "select _wstart, count(diff(voltage)) from {SRC}.sensor where `time` >= '2024-01-01' and "
         "`time` <= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
{'id': 'EXP-gen_LF_C-06',
  'sql': "select interp(diff(voltage)) from {SRC}.sensor range('2024-01-01 00:00:00', '2024-01-01 "
         "00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Interp function 'interp' is not allowed with aggregate, multi-rows, or indefinite rows functions"]},
  'expect_err': True},
 {'id': 'EXP-gen_LF_C-07',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'DERIVATIVE'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'pg': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'influx': ['Projection', 'Indefinite Rows Function', 'Partition']}}},
{'id': 'EXP-gen_LF_C-08',
  'sql': 'select _wstart, count(derivative(voltage, 1s, 0)) from {SRC}.sensor where `time` >= '
         "'2024-01-01' and `time` <= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
{'id': 'EXP-gen_LF_C-09',
  'sql': "select interp(derivative(voltage, 1s, 0)) from {SRC}.sensor range('2024-01-01 00:00:00', "
         "'2024-01-01 00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Interp function 'interp' is not allowed with aggregate, multi-rows, or indefinite rows functions"]},
  'expect_err': True},
 {'id': 'EXP-gen_LF_C-10',
  'sql': 'select mavg(voltage, 3) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'MAVG'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'pg': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'influx': ['Projection', 'Indefinite Rows Function', 'Partition']}}},
{'id': 'EXP-gen_LF_C-11',
  'sql': "select _wstart, count(mavg(voltage, 3)) from {SRC}.sensor where `time` >= '2024-01-01' "
         "and `time` <= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
{'id': 'EXP-gen_LF_C-12',
  'sql': "select interp(mavg(voltage, 3)) from {SRC}.sensor range('2024-01-01 00:00:00', "
         "'2024-01-01 00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Interp function 'interp' is not allowed with aggregate, multi-rows, or indefinite rows functions"]},
  'expect_err': True},
 {'id': 'EXP-gen_LF_C-13',
  'sql': 'select elapsed(`time`) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'ELAPSED'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
{'id': 'EXP-gen_LF_C-14',
  'sql': "select _wstart, count(elapsed(`time`)) from {SRC}.sensor where `time` >= '2024-01-01' "
         "and `time` <= '2024-01-02' interval(1m) fill(value, 0)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Aggregate functions do not support nesting']},
  'expect_err': True},
{'id': 'EXP-gen_LF_C-15',
  'sql': "select interp(elapsed(`time`)) from {SRC}.sensor range('2024-01-01 00:00:00', "
         "'2024-01-01 00:05:00') every(1m) fill(linear)",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ["Interp function 'interp' is not allowed with aggregate, multi-rows, or indefinite rows functions"]},
  'expect_err': True},
 {'id': 'EXP-genLS_Cslimit_mask_full-01',
  'sql': "select MASK_FULL(region, '*') from {SRC}.sensor partition by region slimit 1",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'MASK_FULL'],
                 'local_has': {'mysql': ['Projection', 'Partition'],
                               'pg': ['Projection', 'Partition'],
                               'influx': ['Projection', 'Partition']}}},
 {'id': 'EXP-genLS_Cslimit_to_iso8601-01',
  'sql': 'select to_iso8601(`time`) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'TO_ISO8601'],
                 'local_has': {'mysql': ['Projection', 'Partition'],
                               'pg': ['Projection', 'Partition'],
                               'influx': ['Projection', 'Partition']}}},
 {'id': 'EXP-genLA_Cslimit_twa-01',
  'sql': 'select twa(voltage) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'TWA'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-genLA_Cslimit_irate-01',
  'sql': 'select irate(voltage) from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
 'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'IRATE'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-genTS_Cslimit_statecount-01',
  'sql': "select statecount(voltage, 'GT', 220) from {SRC}.sensor partition by region slimit 1",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'STATECOUNT'],
                 'local_has': {'mysql': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'pg': ['Projection', 'Indefinite Rows Function', 'Partition'],
                               'influx': ['Projection', 'Indefinite Rows Function', 'Partition']}}},
 {'id': 'EXP-genAS_Cslimit_histogram-01',
  'sql': 'select histogram(voltage, \'linear_bin\', \'{{"start":219,"width":1,"count":4}}\', 0) '
         'from {SRC}.sensor partition by region slimit 1',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['SLIMIT', 'PARTITION', 'HISTOGRAM'],
                 'local_has': {'mysql': ['Projection', 'GroupAggregate'],
                               'pg': ['Projection', 'GroupAggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-gen_O_C-01',
  'sql': 'select * from {SRC}.sensor where voltage between 219 and 222 and voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_O_C-02',
  'sql': 'select * from {SRC}.sensor where voltage between 219 and 222 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'], 'local_not_has': ['Filter:', 'Sort']}},
 {'id': 'EXP-gen_O_C-03',
  'sql': 'select * from {SRC}.sensor where voltage between 219 and 222 limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIMIT'], 'local_not_has': ['Filter:', 'limit=']}},
 {'id': 'EXP-gen_O_C-04',
  'sql': 'select * from {SRC}.sensor where voltage in (220, 221) and voltage > 219',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_O_C-05',
  'sql': 'select * from {SRC}.sensor where voltage in (220, 221) order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'], 'local_not_has': ['Filter:', 'Sort']}},
 {'id': 'EXP-gen_O_C-06',
  'sql': 'select * from {SRC}.sensor where voltage in (220, 221) limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIMIT'], 'local_not_has': ['Filter:', 'limit=']}},
 {'id': 'EXP-gen_O_C-07',
  'sql': "select * from {SRC}.sensor where region like 'no%' and voltage > 219",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_O_C-08',
  'sql': "select * from {SRC}.sensor where region like 'no%' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'], 'local_not_has': ['Filter:', 'Sort']}},
 {'id': 'EXP-gen_O_C-09',
  'sql': "select * from {SRC}.sensor where region like 'no%' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIMIT'], 'local_not_has': ['Filter:', 'limit=']}},
 {'id': 'EXP-gen_O_C-10',
  'sql': "select * from {SRC}.sensor where voltage > 220 and region = 'north' and voltage > 219",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_O_C-11',
  'sql': "select * from {SRC}.sensor where voltage > 220 and region = 'north' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'], 'local_not_has': ['Filter:', 'Sort']}},
 {'id': 'EXP-gen_O_C-12',
  'sql': "select * from {SRC}.sensor where voltage > 220 and region = 'north' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIMIT'], 'local_not_has': ['Filter:', 'limit=']}},
 {'id': 'EXP-gen_O_C-13',
  'sql': "select * from {SRC}.sensor where voltage > 222 or region = 'south' and voltage > 219",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'WHERE'], 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_O_C-14',
  'sql': "select * from {SRC}.sensor where voltage > 222 or region = 'south' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'], 'local_not_has': ['Filter:', 'Sort']}},
 {'id': 'EXP-gen_O_C-15',
  'sql': "select * from {SRC}.sensor where voltage > 222 or region = 'south' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['WHERE', 'LIMIT'], 'local_not_has': ['Filter:', 'limit=']}},
 {'id': 'EXP-gen_W_C-01',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(1m) order by _wstart",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY', 'ORDER BY'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'Sort', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-gen_W_C-02',
  'sql': "select _wstart, count(*) from {SRC}.sensor where `time` >= '2024-01-01' and `time` <= "
         "'2024-01-02' interval(1m) limit 5",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'remote_not_has', 'local_not_has'],
  'check_args': {'remote_has': ['GROUP BY', 'LIMIT'],
                 'remote_not_has': ['INTERVAL('],
                 'local_not_has': ['Filter:', 'limit=', 'Interval', 'Aggregate', 'Projection']}},
 {'id': 'EXP-gen_W_C-03',
  'sql': 'select count(*), region from {SRC}.sensor state_window(region) order by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['STATE_WINDOW'],
                 'local_has': {'mysql': ['Sort', 'StateWindow'],
                               'pg': ['Sort', 'StateWindow'],
                               'influx': ['Sort', 'StateWindow']}}},
 {'id': 'EXP-gen_W_C_session_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor session(`time`, 1m) order by _wstart',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['SELECT `time`', 'ORDER BY `time` ASC'],
                                'pg': ['SELECT "time"', 'ORDER BY "time" ASC'],
                                'influx': ['SELECT "time"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME]', 'ORDER BY [TIME] ASC']},
                 'remote_not_has': ['SESSION('],
                 'local_has': ['Session'],
                 'local_not_has': ['Sort']}},
{'id': 'EXP-gen_W_C-04',
  'sql': 'select count(*), region from {SRC}.sensor session(`time`, 1m) order by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_W_C_count_window_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor count_window(2) order by _wstart',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['SELECT `time`', 'ORDER BY `time` ASC'],
                                'pg': ['SELECT "time"', 'ORDER BY "time" ASC'],
                                'influx': ['SELECT "time"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME]', 'ORDER BY [TIME] ASC']},
                 'remote_not_has': ['COUNT_WINDOW'],
                 'local_has': ['Count'],
                 'local_not_has': ['Sort']}},
{'id': 'EXP-gen_W_C-05',
  'sql': 'select count(*), region from {SRC}.sensor count_window(2) order by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_W_C_event_window_anchor-01',
  'sql': 'select _wstart, count(*) from {SRC}.sensor event_window start with voltage > 220 end with '
         'voltage < 220 order by _wstart',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has',
             'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['SELECT `time`, `voltage`', 'ORDER BY `time` ASC'],
                                'pg': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'],
                                'influx': ['SELECT "time", "voltage"', 'ORDER BY "time" ASC'], 'sqlserver': ['SELECT [TIME], [VOLTAGE]', 'ORDER BY [TIME] ASC']},
                 'remote_not_has': ['EVENT_WINDOW'],
                 'local_has': ['Event'],
                 'local_not_has': ['Sort']}},
{'id': 'EXP-gen_W_C-06',
  'sql': 'select count(*), region from {SRC}.sensor event_window start with voltage > 220 end with '
         'voltage < 220 order by region',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics'],
  'check_args': {'expect_err_contains': ['Invalid usage of expr: region']},
  'expect_err': True},
 {'id': 'EXP-gen_FOC-01',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage between 219 and 222 order by '
         '`time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-02',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage between 219 and 222 limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-03',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage in (220, 221) order by '
         '`time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-04',
  'sql': 'select abs(voltage - 220.0) from {SRC}.sensor where voltage in (220, 221) limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-05',
  'sql': "select abs(voltage - 220.0) from {SRC}.sensor where region like 'no%' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-06',
  'sql': "select abs(voltage - 220.0) from {SRC}.sensor where region like 'no%' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-07',
  'sql': 'select upper(region) from {SRC}.sensor where voltage between 219 and 222 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-08',
  'sql': 'select upper(region) from {SRC}.sensor where voltage between 219 and 222 limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-09',
  'sql': 'select upper(region) from {SRC}.sensor where voltage in (220, 221) order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-10',
  'sql': 'select upper(region) from {SRC}.sensor where voltage in (220, 221) limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-11',
  'sql': "select upper(region) from {SRC}.sensor where region like 'no%' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-12',
  'sql': "select upper(region) from {SRC}.sensor where region like 'no%' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['UPPER', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-13',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage between 219 and 222 order by '
         '`time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-14',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage between 219 and 222 limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-15',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage in (220, 221) order by `time`',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-16',
  'sql': 'select round(voltage, 1) from {SRC}.sensor where voltage in (220, 221) limit 3',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_FOC-17',
  'sql': "select round(voltage, 1) from {SRC}.sensor where region like 'no%' order by `time`",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'ORDER BY'],
                 'local_not_has': ['Filter:', 'Sort', 'Projection']}},
 {'id': 'EXP-gen_FOC-18',
  'sql': "select round(voltage, 1) from {SRC}.sensor where region like 'no%' limit 3",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ROUND', 'WHERE', 'LIMIT'],
                 'local_not_has': ['Filter:', 'limit=', 'Projection']}},
 {'id': 'EXP-gen_LFOC-01',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage between 219 and 222 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LFOC-02',
  'sql': 'select csum(voltage) from {SRC}.sensor where voltage in (220, 221) order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['CSUM'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LFOC-03',
  'sql': 'select diff(voltage) from {SRC}.sensor where voltage between 219 and 222 order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LFOC-04',
  'sql': 'select diff(voltage) from {SRC}.sensor where voltage in (220, 221) order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['DIFF'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LFOC-05',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor where voltage between 219 and 222 '
         'order by `time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-gen_LFOC-06',
  'sql': 'select derivative(voltage, 1s, 0) from {SRC}.sensor where voltage in (220, 221) order by '
         '`time`',
  'sources': 'single',
  'checks': ['federated_scan',
             'analyze_metrics',
             'remote_sql',
             'remote_has',
             'remote_not_has',
             'local_has'],
  'check_args': {'remote_has': ['WHERE', 'ORDER BY'],
                 'remote_not_has': ['DERIVATIVE'],
                 'local_has': {'mysql': ['Sort', 'Indefinite Rows Function'],
                               'pg': ['Sort', 'Indefinite Rows Function'],
                               'influx': ['Sort', 'Indefinite Rows Function']},
                 'local_not_has': ['Filter:']}},
 {'id': 'EXP-genNest_push_s_local_s-01',
  'sql': 'select abs(to_unixtimestamp(`time`)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['ABS', 'TIMESTAMPDIFF', 'CONVERT_TZ'],
                                'pg': ['ABS', 'EXTRACT', 'EPOCH'],
                                'influx': ['ABS', 'EXTRACT', 'EPOCH'], 'sqlserver': ['ABS', 'DATEDIFF_BIG']},
                 'local_not_has': ['Projection']}},
 {'id': 'EXP-genNest_local_s_push_s-01',
  'sql': "select MASK_FULL(upper(region), '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'UPPER'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-genNest_local_s_local_s-01',
  'sql': "select MASK_FULL(to_iso8601(`time`), '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'ISO8601'],
                 'local_has': {'mysql': ['Projection'],
                               'pg': ['Projection'],
                               'influx': ['Projection']}}},
 {'id': 'EXP-genNest_push_a_local_s-01',
  'sql': 'select avg(to_unixtimestamp(`time`)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': {'mysql': ['AVG', 'TIMESTAMPDIFF', 'CONVERT_TZ'],
                                'pg': ['AVG', 'EXTRACT', 'EPOCH'],
                                'influx': ['AVG', 'EXTRACT', 'EPOCH'], 'sqlserver': ['AVG', 'DATEDIFF_BIG']},
                 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genNest_push_s_push_a-01',
  'sql': 'select abs(avg(voltage)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_has', 'local_not_has'],
  'check_args': {'remote_has': ['ABS', 'AVG'], 'local_not_has': ['Aggregate', 'Projection']}},
 {'id': 'EXP-genNest_push_s_local_a-01',
  'sql': 'select abs(twa(voltage)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['ABS', 'TWA'],
                 'local_has': {'mysql': ['Projection', 'Aggregate'],
                               'pg': ['Projection', 'Aggregate'],
                               'influx': ['Projection', 'Aggregate']}}},
 {'id': 'EXP-genNest_local_s_push_a-01',
  'sql': 'select to_iso8601(min(`time`)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_has', 'local_has'],
  'check_args': {'remote_has': ['MIN'],
                 'local_has': ['Projection'],
                 'local_not_has': ['GroupAggregate', 'Aggregate']}},
 {'id': 'EXP-genNest_local_a_push_s-01',
  'sql': 'select twa(abs(voltage - 220.0)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genNest_local_a_local_s-01',
  'sql': 'select twa(to_unixtimestamp(`time`)) from {SRC}.sensor',
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['TWA', 'TO_UNIXTIMESTAMP'],
                 'local_has': {'mysql': ['Aggregate'],
                               'pg': ['Aggregate'],
                               'influx': ['Aggregate']}}},
 {'id': 'EXP-genNest_local_s_local_a-01',
  'sql': "select MASK_FULL(first(region), '*') from {SRC}.sensor",
  'sources': 'single',
  'checks': ['federated_scan', 'analyze_metrics', 'remote_sql', 'remote_not_has', 'local_has'],
  'check_args': {'remote_not_has': ['MASK_FULL', 'FIRST('],
                 'local_has': {'mysql': ['Projection', 'Aggregate'],
                               'pg': ['Projection', 'Aggregate'],
                               'influx': ['Projection', 'Aggregate']}}}]



def _max_subquery_depth(sql):
    """Return the maximum nested ``(select`` depth in *sql*.

    This is a lightweight test-run classifier used only to split file-13
    execution into flat vs recursive subquery buckets.  It does not attempt a
    full SQL parse; it only tracks parenthesized ``select`` nesting depth,
    which is sufficient for the hand-authored explain cases in this file.
    """
    lower = sql.lower()
    n = len(lower)
    depth = 0
    max_depth = 0
    stack = []
    i = 0
    while i < n:
        ch = lower[i]
        if ch == '(':
            j = i + 1
            while j < n and lower[j].isspace():
                j += 1
            is_subquery = lower.startswith("select", j)
            stack.append(is_subquery)
            if is_subquery:
                depth += 1
                if depth > max_depth:
                    max_depth = depth
            i += 1
            continue
        if ch == ')' and stack:
            if stack.pop():
                depth -= 1
        i += 1
    return max_depth


# =========================================================================
# Test class
# =========================================================================

class TestFq13Explain(FederatedQueryVersionedMixin):
    """Data-driven EXPLAIN federated query tests.

    Four test methods — one per EXPLAIN mode.  Each iterates all query cases,
    expands across external sources, validates inline, serialises output,
    and compares against golden baseline files.
    """

    # Keep EXPLAIN output deterministic regardless of prior tests.
    # The pytest framework reads class-level updatecfgDict, not cfg.
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "queryPlannerTrace": 1,
        "timezone": "UTC",
        "clientCfg": {
            "federatedQueryEnable": 1,
            "queryPlannerTrace": 1,
            "timezone": "UTC",
        },
    }

    _class_setup_done = False

    # Baseline / temp directories
    _ANS_DIR = os.path.join(os.path.dirname(__file__),
                            "ans", "test_fq_13_explain")
    _TMP_DIR = os.path.join(os.path.dirname(__file__),
                            "tmp", "test_fq_13_explain")

    # ------------------------------------------------------------------
    # Setup / teardown
    # ------------------------------------------------------------------

    @classmethod
    def _reset_td_session(cls):
        """Reconnect tdSql after external-env hard reset to clear stale client caches."""
        connect_args = {}
        if getattr(cls, "host", None):
            connect_args["host"] = cls.host
        if getattr(cls, "port", None):
            connect_args["port"] = cls.port
        tdSql.connect("root", "taosdata", **connect_args)
        tdSql.execute('ALTER LOCAL "federatedQueryEnable" "1"')
        tdSql.execute('ALTER LOCAL "queryPlannerTrace" "1"')
        tdSql.execute('ALTER LOCAL "timezone" "UTC"')

    def _recreate_pg_ascii_db(self):
        ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), _PG_ASCII_DB)
        ExtSrcEnv.pg_exec_cfg(
            self._pg_cfg(),
            "postgres",
            [f'CREATE DATABASE "{_PG_ASCII_DB}" WITH ENCODING \'SQL_ASCII\' TEMPLATE template0'],
        )

    def _setup_ascii_length_sources(self):
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_ASCII_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_ASCII_DB, _MYSQL_ASCII_SETUP_SQLS)
        self._cleanup_src(_SRC_MYSQL_ASCII)
        self._mk_mysql_real(_SRC_MYSQL_ASCII, database=_MYSQL_ASCII_DB)

        self._recreate_pg_ascii_db()
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_ASCII_DB, _PG_ASCII_SETUP_SQLS)
        self._cleanup_src(_SRC_PG_ASCII)
        self._mk_pg_real(_SRC_PG_ASCII, database=_PG_ASCII_DB)

    def _rebuild_sources(self):
        """Rebuild external-source metadata after a taosd/qnode/session flap."""
        ExtSrcEnv.ensure_env()
        self._reset_td_session()

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, _MYSQL_SETUP_SQLS)
        self._cleanup_src(_SRC_MYSQL)
        self._mk_mysql_real(_SRC_MYSQL, database=_MYSQL_DB)

        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _PG_SETUP_SQLS)
        self._cleanup_src(_SRC_PG)
        self._mk_pg_real(_SRC_PG, database=_PG_DB)

        ExtSrcEnv.influx_create_db(_INFLUX_BUCKET)
        ExtSrcEnv.influx_write(_INFLUX_BUCKET, _INFLUX_LINES)
        self._cleanup_src(_SRC_INFLUX)
        self._mk_influx_real(_SRC_INFLUX, database=_INFLUX_BUCKET)

        self._setup_ascii_length_sources()

    @staticmethod
    def _is_env_recoverable_error(sql, err_text):
        err_upper = err_text.upper()
        sql_upper = sql.upper()
        env_needles = (
            "EXTERNAL SOURCE NOT FOUND",
            "DATABASE NOT EXIST",
            "QNODE NOT FOUND",
            "EXTERNAL SOURCE AUTHENTICATION FAILED",
            "EXTERNAL SOURCE INTERNAL ERROR",
            "UNABLE TO ESTABLISH CONNECTION",
        )
        fq_needles = ("FQ_EXP_", "FQ_EXPLAIN_", "REGION_INFO", "SENSOR")
        return (
            any(needle in err_upper for needle in env_needles)
            and any(needle in sql_upper for needle in fq_needles)
        )

    def _maybe_recover_case(self, expanded_id, sql, exc, attempt):
        if attempt > 0:
            return False
        err_text = str(exc)
        if not self._is_env_recoverable_error(sql, err_text):
            return False
        tdLog.info(
            f"[{expanded_id:<24s}] recover env after transient failure: {err_text}"
        )
        self._rebuild_sources()
        return True

    def setup_method(self, method):
        if TestFq13Explain._class_setup_done:
            return
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        self._reset_td_session()

        # -- MySQL (sensor + region_info) --
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB,
                                 _MYSQL_SETUP_SQLS)
        self._cleanup_src(_SRC_MYSQL)
        self._mk_mysql_real(_SRC_MYSQL, database=_MYSQL_DB)

        # -- PostgreSQL (sensor + region_info) --
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _PG_SETUP_SQLS)
        self._cleanup_src(_SRC_PG)
        self._mk_pg_real(_SRC_PG, database=_PG_DB)

        # -- InfluxDB (sensor measurement) --
        ExtSrcEnv.influx_create_db(_INFLUX_BUCKET)
        ExtSrcEnv.influx_write(_INFLUX_BUCKET, _INFLUX_LINES)
        self._cleanup_src(_SRC_INFLUX)
        self._mk_influx_real(_SRC_INFLUX, database=_INFLUX_BUCKET)

        # -- Dedicated ASCII/VARCHAR sources used to validate the non-NCHAR
        #    LENGTH rewrite branch without perturbing the shared 3-source corpus.
        self._setup_ascii_length_sources()

        # -- SQL Server (sensor + region_info): the 4th source every matrix
        #    case now runs against (zero pushdown in step 1).
        self._setup_sqlserver_explain_source()

        TestFq13Explain._class_setup_done = True

    def teardown_class(self):
        tmp = TestFq13Explain()
        for src in [_SRC_MYSQL, _SRC_PG, _SRC_INFLUX, _SRC_MYSQL_ASCII,
                    _SRC_PG_ASCII, _SRC_SQLSERVER]:
            tmp._cleanup_src(src)
        for drop_fn, drop_args in [
            (ExtSrcEnv.mysql_drop_db_cfg, (tmp._mysql_cfg(), _MYSQL_DB)),
            (ExtSrcEnv.mysql_drop_db_cfg, (tmp._mysql_cfg(), _MYSQL_ASCII_DB)),
            (ExtSrcEnv.pg_drop_db_cfg, (tmp._pg_cfg(), _PG_DB)),
            (ExtSrcEnv.pg_drop_db_cfg, (tmp._pg_cfg(), _PG_ASCII_DB)),
            (ExtSrcEnv.influx_drop_db, (_INFLUX_BUCKET,)),
            (ExtSrcEnv.sqlserver_drop_db, (_SQLSERVER_DB,)),
        ]:
            try:
                drop_fn(*drop_args)
            except Exception:
                pass
        TestFq13Explain._class_setup_done = False

    # ------------------------------------------------------------------
    # Baseline infrastructure
    # ------------------------------------------------------------------

    def _baseline_path(self, name):
        return os.path.join(self._ANS_DIR, f"{name}.txt")

    @staticmethod
    def _serialize_explain(case_id, sql_display, lines):
        """Serialize one EXPLAIN result block for baseline comparison."""
        parts = [f"### {case_id} POS", f"SQL: {sql_display}", "RESULT"]
        for line in lines:
            parts.append(line)
        parts.append("---")
        return "\n".join(parts)

    @staticmethod
    def _serialize_err(case_id, sql_display, err):
        """Serialize one EXPLAIN error block for baseline comparison."""
        return "\n".join([
            f"### {case_id} ERR",
            f"SQL: {sql_display}",
            f"ERROR: {err}",
            "---",
        ])

    def _compare_baseline(self, output, baseline_file, failed):
        """Compare serialised output against baseline; report diff.

        Baseline files store **normalised** content: every run-dependent
        value (cost, elapsed, timing metrics …) is replaced by a
        placeholder so that baselines are deterministic.  Field names and
        plan structure are kept verbatim.
        """
        _norm_re = re.compile(r'(root@)[0-9a-f]{12}\b')
        _cost_re = re.compile(r'cost=[\d.]+\.\.[\d.]+')
        _fetch_cost_re = re.compile(r'fetch_cost=[\d.]+(\([\d.]+\))?')
        _plan_time_re = re.compile(r'Planning Time: [\d.]+ ms')
        _exec_time_re = re.compile(r'Execution Time: [\d.]+ ms')
        _elapsed_re = re.compile(r'elapsed=[\d.]+ms')
        _remote_blocks_re = re.compile(r'(Remote rows=\d+, )blocks=\d+(, elapsed=<TIME>ms)')
        _exec_cost_line_re = re.compile(r'Exec cost: .+')
        _exec_cost_dt_re = re.compile(
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+')
        _exec_cost_num_re = re.compile(r'(=)[\d.]+')
        # NOW() pushed to remote: literal timestamp with non-zero fractional seconds.
        # e.g. '2026-06-05 05:42:59.980', '2026-06-05 06:36:40.050'
        # Test data constants use all-zero ms ('2024-01-01 00:00:00.000') or no ms
        # at all — excluded by requiring at least one non-zero digit: \d*[1-9]\d*
        # The separator accepts both ' ' and 'T': SQL Server renders ISO-8601
        # (N'2026-10-12T01:40:52.709784') per DS 5.3.6 R4.
        _remote_ts_re = re.compile(
            r"'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.\d*[1-9]\d*'")
        # TODAY() pushed to remote: timestamp with all-zero fractional seconds,
        # year >= 2025 (test data uses only fixed 2024-xx-xx literals).
        # e.g. '2026-06-12 00:00:00.000' (local TZ) or '2026-06-11 16:00:00.000' (UTC)
        # Keep 2024 literals intact so Remote SQL baselines preserve the original
        # fixed SQL predicates instead of being normalized as dynamic TODAY().
        _remote_today_re = re.compile(
            r"'20(?:2[5-9]|[3-9]\d)-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.0+'")
        # TIMEZONE() pushed to remote as a constant string, e.g. 'Asia/Shanghai (CST, +0800)'
        _remote_tz_re = re.compile(r"'[A-Za-z/_]+ \([A-Z]+, [+-]\d{4}\)'")
        # CLIENT_VERSION()/SERVER_VERSION() may be constant-folded into remote SQL.
        _remote_version_re = re.compile(
            r"'\d+\.\d+\.\d+(?:\.[A-Za-z0-9_-]+)*\.[A-Za-z][A-Za-z0-9_.-]*'")
        # Remote SQL may synthesize per-run expression aliases such as
        # expr_3401e7e7 / EXPR_EC06414E. Keep short ordinal aliases like
        # expr_2 / expr_3 intact by requiring a longer hex suffix.
        _remote_expr_alias_re = re.compile(r'\bexpr_[0-9a-f]{5,}\b',
                                           re.IGNORECASE)
        _curr_user_sql_re = re.compile(
            r'^SQL: select (?:CURRENT_USER|USER)\(\) from .+$',
            re.IGNORECASE | re.MULTILINE)
        # T-SQL renders the pushed literal as N'<user>@<host>'; the optional
        # N prefix must be part of the match, otherwise the machine-specific
        # user@host leaks into the baseline and only compares equal on the
        # host that generated it.
        _curr_user_remote_sql_re = re.compile(
            r"(Remote SQL: SELECT )N?'[^']+'(\s+AS\s+)",
            re.IGNORECASE)
        _curr_user_width_re = re.compile(r'(width=)\d+')
        _version_sql_re = re.compile(
            r'^SQL: select (?:CLIENT_VERSION|SERVER_VERSION)\(\) from .+$',
            re.IGNORECASE | re.MULTILINE)
        _version_width_re = re.compile(r'(width=)\d+')
        # Windows reaches InfluxDB through the HTTP/JSON transport (no Arrow
        # Flight SQL), so the connector delivers the whole response as a single
        # block, while the Linux FlightSQL transport splits the same rows into
        # smaller batches.  EXPLAIN ANALYZE reports the rows the operator
        # actually processed, so a downstream LIMIT may stop part-way through a
        # differently sized block (e.g. 4 vs 5 of 5 rows).  The result set is
        # identical; only these block-boundary statistics differ.
        _influx_scan_rows_re = re.compile(r'(\(cost=[^)]*? rows=)\d+')
        _remote_rows_re = re.compile(r'(Remote rows=)\d+')

        def _mask_exec_cost(m):
            s = m.group(0)
            s = _exec_cost_dt_re.sub('<TIME>', s)
            s = _exec_cost_num_re.sub(r'\1<NUM>', s)
            return s

        def _mask_current_user_block(block):
            block = _curr_user_remote_sql_re.sub(
                r"\1'<CURRENT_USER>'\2", block)
            block = _curr_user_width_re.sub(
                r'\1<CURRENT_USER_WIDTH>', block)
            return block

        def _mask_version_block(block):
            return _version_width_re.sub(r'\1<VERSION_WIDTH>', block)

        def _mask_influx_rows(block):
            block = _influx_scan_rows_re.sub(r'\1<ROWS>', block)
            return _remote_rows_re.sub(r'\1<ROWS>', block)

        def normalize(text):
            t = _norm_re.sub(r'\1<HOST>', text)
            t = _cost_re.sub('cost=<COST>', t)
            t = _fetch_cost_re.sub('fetch_cost=<COST>', t)
            t = _plan_time_re.sub('Planning Time: <TIME> ms', t)
            t = _exec_time_re.sub('Execution Time: <TIME> ms', t)
            t = _elapsed_re.sub('elapsed=<TIME>ms', t)
            t = _remote_blocks_re.sub(r'\1blocks=<BLOCKS>\2', t)
            t = _exec_cost_line_re.sub(_mask_exec_cost, t)
            t = _remote_ts_re.sub("'<NOW>'", t)
            t = _remote_today_re.sub("'<TODAY>'", t)
            t = _remote_tz_re.sub("'<TIMEZONE>'", t)
            t = _remote_version_re.sub("'<VERSION>'", t)
            t = _remote_expr_alias_re.sub('<REMOTE_EXPR>', t)
            # Runtime converts final MySQL/PG timestamp outputs to epoch values
            # for connector transport. Baselines compare the logical remote
            # plan, so canonicalize that wrapper back to its timestamp expr.
            t = self._canonicalize_timestamp_transport(t)
            t = ''.join(
                _mask_current_user_block(block) if _curr_user_sql_re.search(block)
                else _mask_version_block(block) if _version_sql_re.search(block)
                else _mask_influx_rows(block)
                if sys.platform == 'win32' and '(influxdb)' in block
                else block
                for block in re.split(r'(?=### )', t))
            return t

        normalized_output = normalize(output)

        os.makedirs(self._TMP_DIR, exist_ok=True)
        tmp_file = os.path.join(
            self._TMP_DIR,
            f"{os.path.basename(baseline_file)}.{os.getpid()}.tmp",
        )
        with open(tmp_file, "w") as f:
            f.write(normalized_output)
        tdLog.info(f"Temp result: {tmp_file}")

        # Explicit, env-gated baseline update.  Used when a new source is added
        # to the matrix: the shared blocks must stay byte-identical, which is
        # checked with `git diff` (additions only) before the change is kept.
        if os.environ.get("FQ13_WRITE_BASELINE") == "1":
            with open(baseline_file, "w", encoding="utf-8") as f:
                f.write(normalized_output)
            tdLog.info(f"FQ13_WRITE_BASELINE: rewrote {baseline_file}")
            return

        if not os.path.isfile(baseline_file):
            msg = (
                f"Baseline file not found: {baseline_file}\n"
                f"  Actual output saved to: {tmp_file}\n"
                f"  Copy tmp to ans/ after manual inspection."
            )
            tdLog.info(f"BASELINE MISSING: {msg}")
            failed.append(("<baseline>", "<baseline>", msg))
            return

        with open(baseline_file, "r") as f:
            baseline = f.read()
        normalized_baseline = normalize(baseline)
        if normalized_output != normalized_baseline:
            out_lines = normalized_output.splitlines()
            base_lines = normalized_baseline.splitlines()
            diff_line = -1
            bl = tl = ""
            for li in range(max(len(out_lines), len(base_lines))):
                tl = out_lines[li] if li < len(out_lines) else "<EOF>"
                bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                if tl != bl:
                    diff_line = li + 1
                    break
            msg = (
                f"Baseline mismatch!\n"
                f"  baseline: {baseline_file}\n"
                f"  actual:   {tmp_file}\n"
                f"  first diff at line {diff_line}:\n"
                f"    baseline: {bl!r}\n"
                f"    actual:   {tl!r}\n"
                f"  Run: diff {baseline_file} {tmp_file}"
            )
            tdLog.info(f"BASELINE MISMATCH: {msg}")
            failed.append(("<baseline>", "<baseline>", msg))
        else:
            tdLog.info(f"Baseline comparison: OK ({baseline_file})")

    def _compare_baseline_filtered(self, output, baseline_file, failed, executed_ids):
        """Compare only matching blocks when FQ_CASE_FILTER is active.

        Extracts the blocks from the baseline whose expanded_id is in executed_ids,
        then compares against the actual (filtered) output block-by-block.
        This avoids full-file mismatch when only a subset of cases ran.
        """
        if not os.path.isfile(baseline_file):
            self._compare_baseline(output, baseline_file, failed)
            return

        with open(baseline_file, "r") as f:
            baseline_text = f.read()

        # Extract blocks from text, keeping only those whose header ID is in executed_ids.
        def extract_blocks(text, id_set):
            result = []
            current = []
            for line in text.splitlines():
                if line.startswith("### "):
                    if current:
                        result.append("\n".join(current))
                        current = []
                    current = [line]
                elif current:
                    current.append(line)
            if current:
                result.append("\n".join(current))
            if id_set is None:
                return result
            return [b for b in result
                    if b.split("\n")[0][4:].split(" ")[0] in id_set]

        baseline_blocks = extract_blocks(baseline_text, executed_ids)
        actual_blocks   = extract_blocks(output, None)

        if not baseline_blocks:
            # No matching blocks in baseline — fall back to full comparison.
            self._compare_baseline(output, baseline_file, failed)
            return

        filtered_baseline = "\n".join(baseline_blocks) + "\n"
        filtered_actual   = "\n".join(actual_blocks)   + "\n"

        # Reuse existing compare logic via a temporary wrapper.
        import tempfile, os as _os
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tf:
            tf.write(filtered_baseline)
            tmp_baseline = tf.name
        try:
            self._compare_baseline(filtered_actual, tmp_baseline, failed)
        finally:
            _os.unlink(tmp_baseline)

    # ------------------------------------------------------------------
    # EXPLAIN execution
    # ------------------------------------------------------------------

    @staticmethod
    def _exec_explain(sql, mode):
        """Execute EXPLAIN and return output lines.  Raises on failure."""
        full_sql = f"{mode} {sql}"
        # tdSql keeps errno/error_info as mutable instance attributes and does not
        # reset them on every query failure path. Clear them here so any appended
        # detail belongs to the current EXPLAIN only, instead of leaking from a
        # previous negative-path case such as auth-failure tests.
        tdSql.errno = None
        tdSql.error_info = None
        try:
            tdSql.query(full_sql, queryTimes=1)
        except Exception as e:
            errno = getattr(tdSql, 'errno', None)
            err_info = getattr(tdSql, 'error_info', None)
            detail = ""
            if errno is not None:
                detail += f" errno={errno:#x}"
            if err_info:
                detail += f" error_info={err_info}"
            raise AssertionError(
                f"EXPLAIN failed: {full_sql}{detail}\n  Exception: {e}"
            ) from e
        lines = []
        if tdSql.queryResult:
            for row in tdSql.queryResult:
                if row[0] is not None:
                    lines.append(str(row[0]))
        return lines

    # ------------------------------------------------------------------
    # Remote SQL helper
    # ------------------------------------------------------------------

    @staticmethod
    def _get_remote_sql_lines(lines):
        return [line for line in lines if "Remote SQL:" in line]

    @staticmethod
    def _get_remote_sql_line(lines):
        """Extract the first 'Remote SQL:' line, or '' if absent."""
        remote_lines = [line for line in lines if "Remote SQL:" in line]
        return remote_lines[0] if remote_lines else ""

    @classmethod
    def _get_remote_sql_text(cls, lines):
        return " ".join(cls._get_remote_sql_lines(lines))

    # T-SQL spellings of expectations written for the other three sources
    # (DS 5.3.6 / 5.3.13): the SQL Server leg satisfies a MySQL/PG token with
    # any of the listed T-SQL equivalents.
    _SS_TOKEN_EQUIV = {
        "LIMIT": ("ROWS FETCH NEXT", "TOP ("),
        "STDDEV_POP": ("STDEVP(",),
        "STDDEV_SAMP": ("STDEV(",),
        "VAR_POP": ("VARP(",),
        "VAR_SAMP": ("VAR(",),
        "LN": ("LOG(",),
        "REPEAT": ("REPLICATE(",),
        "CHAR_LENGTH": ("LEN(",),
        "LENGTH": ("LEN(",),
    }

    @classmethod
    def _remote_token_present(cls, kw, remote_upper, is_ss_leg):
        if kw.upper() in remote_upper:
            return True
        if is_ss_leg:
            if any(alias in remote_upper
                   for alias in cls._SS_TOKEN_EQUIV.get(kw.upper(), ())):
                return True
            # `LIMIT <n>` has no single T-SQL spelling: without an ORDER BY it
            # renders as TOP (<n>), with one as OFFSET ... FETCH NEXT <n> ROWS
            # ONLY.  Match the rendered count rather than the keyword.
            m = re.fullmatch(r"LIMIT\s+(\d+)", kw.strip(), re.IGNORECASE)
            if m:
                n = m.group(1)
                return (f"TOP ({n})" in remote_upper or
                        f"FETCH NEXT {n} ROWS ONLY" in remote_upper)
        return False

    @staticmethod
    def _find_sql_call_end(sql, open_paren):
        """Return the matching ')' while respecting SQL quoted strings."""
        depth = 0
        quote = None
        i = open_paren
        while i < len(sql):
            ch = sql[i]
            if quote is not None:
                if ch == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                elif ch == "\\" and quote == "'":
                    i += 2
                    continue
            elif ch in ("'", '"', "`"):
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
            i += 1
        return -1

    @classmethod
    def _first_sql_arg(cls, args):
        """Extract the first top-level function argument."""
        wrapped = "(" + args + ")"
        end = cls._find_sql_call_end(wrapped, 0)
        if end < 0:
            return None

        depth = 0
        quote = None
        i = 1
        while i < end:
            ch = wrapped[i]
            if quote is not None:
                if ch == quote:
                    if i + 1 < end and wrapped[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                elif ch == "\\" and quote == "'":
                    i += 2
                    continue
            elif ch in ("'", '"', "`"):
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                return wrapped[1:i].strip()
            i += 1
        return wrapped[1:end].strip()

    @classmethod
    def _canonicalize_timestamp_transport(cls, sql):
        """Replace runtime epoch transport wrappers with their timestamp expr.

        These wrappers are a connector transport detail, not scalar-function
        pushdown. Keeping them out of semantic checks avoids treating MySQL's
        TIMESTAMPDIFF or PG's EXTRACT as a pushed TDengine function.
        """
        mysql_prefix = (
            "CAST(TIMESTAMPDIFF(MICROSECOND, "
            "'1970-01-01 00:00:00', CONVERT_TZ("
        )
        mysql_aware_prefix = "CAST(UNIX_TIMESTAMP("
        pg_prefix = "CAST(EXTRACT(EPOCH FROM "
        prefixes = (mysql_prefix, mysql_aware_prefix, pg_prefix)

        result = sql
        search_from = 0
        while search_from < len(result):
            upper = result.upper()
            matches = [
                (upper.find(prefix, search_from), prefix)
                for prefix in prefixes
            ]
            matches = [(pos, prefix) for pos, prefix in matches if pos >= 0]
            if not matches:
                break
            start, prefix = min(matches, key=lambda item: item[0])
            cast_open = result.find("(", start)
            cast_end = cls._find_sql_call_end(result, cast_open)
            if cast_end < 0:
                search_from = start + 1
                continue

            alias_match = re.match(
                r"\s+AS\s+(`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)",
                result[cast_end + 1:], re.IGNORECASE)
            alias = alias_match.group(1) if alias_match is not None else None
            replace_end = (cast_end + 1 + alias_match.end()
                           if alias_match is not None else cast_end + 1)

            inner = None
            if prefix == mysql_prefix:
                convert_pos = upper.find("CONVERT_TZ(", start, cast_end)
                if convert_pos >= 0:
                    convert_open = result.find("(", convert_pos)
                    convert_end = cls._find_sql_call_end(result, convert_open)
                    if 0 <= convert_end <= cast_end:
                        inner = cls._first_sql_arg(
                            result[convert_open + 1:convert_end])
            elif prefix == mysql_aware_prefix:
                unix_pos = upper.find("UNIX_TIMESTAMP(", start, cast_end)
                if unix_pos >= 0:
                    unix_open = result.find("(", unix_pos)
                    unix_end = cls._find_sql_call_end(result, unix_open)
                    if 0 <= unix_end <= cast_end:
                        inner = cls._first_sql_arg(
                            result[unix_open + 1:unix_end])
            else:
                extract_pos = upper.find("EXTRACT(", start, cast_end)
                if extract_pos >= 0:
                    extract_open = result.find("(", extract_pos)
                    extract_end = cls._find_sql_call_end(result, extract_open)
                    epoch_from = extract_open + len("(EPOCH FROM ")
                    if 0 <= extract_end <= cast_end:
                        inner = result[epoch_from:extract_end].strip()

            if not inner:
                search_from = cast_end + 1
                continue

            def identifier_name(value):
                identifier = (
                    r'(?:`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)'
                )
                match = re.fullmatch(
                    rf'\s*(?:{identifier}\s*\.\s*)*'
                    rf'({identifier})\s*', value)
                if match is None:
                    return None
                name = match.group(1)
                if name[0] in ('`', '"'):
                    name = name[1:-1]
                return name.lower()

            replacement = inner
            if (alias is not None and
                    identifier_name(inner) != identifier_name(alias)):
                replacement += " AS " + alias
            result = result[:start] + replacement + result[replace_end:]
            search_from = start + len(replacement)

        return cls._canonicalize_mysql_epoch_group_alias(result)

    @classmethod
    def _canonicalize_mysql_epoch_group_alias(cls, sql):
        """Restore logical interval expressions hidden by a MySQL alias.

        Runtime epoch transport makes SELECT and GROUP BY use different
        expressions under ONLY_FULL_GROUP_BY. The renderer therefore groups by
        the emitted SELECT alias; baselines keep comparing the logical bucket
        expression used before transport conversion.
        """
        result = sql
        search_from = 0
        marker = "Remote SQL: SELECT "
        while True:
            line_start = result.find(marker, search_from)
            if line_start < 0:
                break
            line_end = result.find("\n", line_start)
            if line_end < 0:
                line_end = len(result)
            line = result[line_start:line_end]
            upper = line.upper()
            group_pos = upper.find(" GROUP BY ")
            bucket_pos = upper.find("FROM_UNIXTIME(")
            if bucket_pos < 0 or group_pos < 0 or bucket_pos >= group_pos:
                search_from = line_end + 1
                continue

            bucket_open = line.find("(", bucket_pos)
            bucket_end = cls._find_sql_call_end(line, bucket_open)
            if bucket_end < 0 or bucket_end >= group_pos:
                search_from = line_end + 1
                continue
            alias_match = re.match(
                r"\s+AS\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)",
                line[bucket_end + 1:], re.IGNORECASE)
            if alias_match is None:
                search_from = line_end + 1
                continue

            alias = alias_match.group(1)
            group_value_start = group_pos + len(" GROUP BY ")
            clause_end = len(line)
            for suffix in (" HAVING ", " ORDER BY ", " LIMIT "):
                pos = upper.find(suffix, group_value_start)
                if 0 <= pos < clause_end:
                    clause_end = pos
            group_clause = line[group_value_start:clause_end]
            if alias.startswith("`"):
                alias_pattern = re.escape(alias)
            else:
                alias_pattern = (
                    rf"(?<![A-Za-z0-9_]){re.escape(alias)}"
                    r"(?![A-Za-z0-9_])"
                )
            bucket_expr = line[bucket_pos:bucket_end + 1]
            group_clause, replacement_count = re.subn(
                alias_pattern, lambda _: bucket_expr, group_clause,
                flags=re.IGNORECASE)
            if replacement_count == 0:
                search_from = line_end + 1
                continue

            line = (line[:group_value_start] + group_clause +
                    line[clause_end:])
            result = result[:line_start] + line + result[line_end:]
            search_from = line_start + len(line) + 1

        return result

    @staticmethod
    def _get_plan_operator_lines(lines):
        return [line.strip() for line in lines if re.match(r"^\s*->\s+", line)]

    @staticmethod
    def _get_local_plan_text(lines):
        return " ".join(
            line for line in lines
            if "Remote SQL:" not in line and not line.startswith("SQL:")
        )

    # ------------------------------------------------------------------
    # Strict-mode toggle
    # ------------------------------------------------------------------

    @staticmethod
    def _is_strict():
        """Return True unless ``FQ_EXPLAIN_STRICT`` is explicitly disabled.

        Strict mode is the DEFAULT behaviour for validators that
        assert concrete pushdown/rendering behaviour
        (pushdown_flags / columns_list / soft_remote_has). This is
        what the DS spec requires — each declared pushdown must
        actually be emitted in the Remote SQL.

        ``type_mapping`` is intentionally excluded from strict mode.
        The current engine output does not emit a "Type Mapping:"
        section, so that check remains informational-only until the
        explain text grows a stable type-mapping block.

        Set ``FQ_EXPLAIN_STRICT=0`` (or ``false`` / ``no``) to demote
        these checks to warnings while debugging locally.
        """
        return os.getenv("FQ_EXPLAIN_STRICT", "1").strip().lower() not in (
            "0", "false", "no",
        )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    def _validate(self, case_id, mode, lines, case_def, src_info):
        """Run all validators in case_def['checks'] for the given mode.

        Returns list of (case_id, check_name, error_msg) tuples on failure.
        Validators are mode-aware: verbose-only checks skip in plain mode,
        analyze-only checks skip in non-analyze mode.

        Validators for concrete pushdown/rendering behaviour
        (pushdown_flags, columns_list, soft_remote_has) become hard
        failures by default; ``FQ_EXPLAIN_STRICT=0`` demotes them to
        warnings. ``type_mapping`` stays informational because the
        current explain output does not emit that section.
        """
        checks = case_def.get("checks", [])
        raw_args = case_def.get("check_args", {})
        # Resolve per-source expectations: a check-arg value may be a plain
        # list (same for all sources) or a dict{label:list} (per-source).
        src_label = src_info.get("label", "")
        args = {k: _per_src(v, src_label) for k, v in raw_args.items()}
        errors = []
        strict = self._is_strict()
        is_ss_leg = "sqlserver" in src_label.lower()
        is_verbose = mode in VERBOSE_MODES
        is_analyze = mode in ANALYZE_MODES
        mode_label = _MODE_LABELS[mode]
        text = " ".join(lines)
        src_name = src_info.get("src", "")
        db_name = src_info.get("db", "")

        for chk in checks:
            try:
                if chk == "federated_scan":
                    assert "Federated Scan" in text, \
                        f"[{mode_label}] 'Federated Scan' not found in plan"

                elif chk == "federated_scan_on":
                    has_full = (f"Federated Scan on "
                                f"{src_name}.{db_name}.sensor") in text
                    has_short = (f"Federated Scan on "
                                 f"{src_name}.sensor") in text
                    assert has_full or has_short, (
                        f"[{mode_label}] 'Federated Scan on "
                        f"{src_name}[.{db_name}].sensor' not found")

                elif chk == "remote_sql":
                    if is_verbose:
                        assert "Remote SQL:" in text, \
                            f"[{mode_label}] 'Remote SQL:' not found"

                elif chk == "type_mapping":
                    if is_verbose:
                        ok = "Type Mapping:" in text and "<-" in text
                        if not ok:
                            msg = (f"[{mode_label}] 'Type Mapping:' "
                                   f"not found (SOFT)")
                            tdLog.info(f"SOFT SKIP: {case_id} {msg}")

                elif chk == "output_cols_eq":
                    if is_verbose:
                        expected = args.get("output_cols_eq", None)
                        if isinstance(expected, list):
                            # No per-source expectation for this leg.
                            continue
                        assert expected is not None, (
                            f"[{mode_label}] output_cols_eq requires "
                            f"check_args['output_cols_eq']"
                        )
                        m = re.search(r"Output: columns=(\d+)\b", text)
                        assert m, (
                            f"[{mode_label}] first 'Output: columns=' "
                            f"not found"
                        )
                        actual = int(m.group(1))
                        assert actual == expected, (
                            f"[{mode_label}] output columns mismatch: "
                            f"expected {expected}, got {actual}"
                        )

                elif chk == "pushdown_flags":
                    if is_verbose:
                        if "Pushdown:" not in text:
                            msg = (f"[{mode_label}] 'Pushdown:' "
                                   f"not found (SOFT)")
                            if strict:
                                raise AssertionError(msg)
                            tdLog.info(f"SOFT SKIP: {case_id} {msg}")

                elif chk == "columns_list":
                    if is_verbose:
                        if "columns=" not in text:
                            msg = (f"[{mode_label}] 'columns=' "
                                   f"not found (SOFT)")
                            if strict:
                                raise AssertionError(msg)
                            tdLog.info(f"SOFT SKIP: {case_id} {msg}")

                elif chk == "analyze_metrics":
                    if is_analyze:
                        metric_kws = ["rows=", "time=", "loops=",
                                      "actual", "elapsed", "duration",
                                      "cost="]
                        has = any(k in text.lower() for k in metric_kws)
                        assert has, (
                            f"[{mode_label}] No execution metrics found "
                            f"in ANALYZE output")

                elif chk == "remote_has":
                    if is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        assert remote, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_has): {remote}")
                        logical_remote = self._canonicalize_timestamp_transport(
                            remote)
                        for kw in args.get("remote_has", []):
                            assert (self._remote_token_present(kw, remote.upper(), is_ss_leg) or
                                    kw.upper() in logical_remote.upper()), \
                                (f"[{mode_label}] Remote SQL missing "
                                 f"'{kw}': {remote}")

                elif chk == "remote_any_has":
                    if is_verbose:
                        remote_text = self._get_remote_sql_text(lines)
                        assert remote_text, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote_text.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_any_has): {remote_text}")
                        logical_remote = self._canonicalize_timestamp_transport(
                            remote_text)
                        for kw in args.get("remote_any_has", []):
                            assert (self._remote_token_present(kw, remote_text.upper(), is_ss_leg) or
                                    kw.upper() in logical_remote.upper()), \
                                (f"[{mode_label}] Remote SQL(all) missing "
                                 f"'{kw}': {remote_text}")

                elif chk == "soft_remote_has":
                    if is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        if not remote:
                            msg = (f"[{mode_label}] 'Remote SQL:' "
                                   f"line not found (SOFT)")
                            if strict:
                                raise AssertionError(msg)
                            tdLog.info(f"SOFT SKIP: {case_id} {msg}")
                        elif "generation failed" in remote.lower():
                            msg = (f"[{mode_label}] Remote SQL generation "
                                   f"failed (SOFT): {remote}")
                            if strict:
                                raise AssertionError(msg)
                            tdLog.info(f"SOFT SKIP: {case_id} {msg}")
                        else:
                            logical_remote = \
                                self._canonicalize_timestamp_transport(remote)
                            for kw in args.get("soft_remote_has", []):
                                if (not self._remote_token_present(kw, remote.upper(), is_ss_leg) and
                                        kw.upper() not in logical_remote.upper()):
                                    msg = (
                                        f"[{mode_label}] Remote SQL "
                                        f"missing '{kw}' (SOFT): "
                                        f"{remote}")
                                    if strict:
                                        raise AssertionError(msg)
                                    tdLog.info(
                                        f"SOFT SKIP: {case_id} {msg}")

                elif chk == "remote_not_has":
                    if is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        assert remote, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_not_has): {remote}")
                        semantic_remote = self._canonicalize_timestamp_transport(
                            remote)
                        for kw in args.get("remote_not_has", []):
                            assert kw.upper() not in semantic_remote.upper(), \
                                (f"[{mode_label}] Remote SQL should NOT "
                                 f"contain '{kw}': {remote}")

                elif chk == "remote_any_not_has":
                    if is_verbose:
                        remote_text = self._get_remote_sql_text(lines)
                        assert remote_text, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote_text.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_any_not_has): {remote_text}")
                        semantic_remote = self._canonicalize_timestamp_transport(
                            remote_text)
                        for kw in args.get("remote_any_not_has", []):
                            assert kw.upper() not in semantic_remote.upper(), \
                                (f"[{mode_label}] Remote SQL(all) should NOT "
                                 f"contain '{kw}': {remote_text}")

                elif chk == "remote_any_count_eq":
                    if is_verbose:
                        remote_text = self._get_remote_sql_text(lines)
                        assert remote_text, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote_text.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_any_count_eq): {remote_text}")
                        remote_upper = remote_text.upper()
                        for kw, expected in args.get("remote_any_count_eq", []):
                            actual = remote_upper.count(str(kw).upper())
                            assert actual == expected, (
                                f"[{mode_label}] Remote SQL(all) keyword '{kw}' "
                                f"count mismatch: expected {expected}, got {actual}: "
                                f"{remote_text}")

                elif chk == "remote_any_count_ge":
                    if is_verbose:
                        remote_text = self._get_remote_sql_text(lines)
                        assert remote_text, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        assert "generation failed" not in remote_text.lower(), \
                            (f"[{mode_label}] Remote SQL generation failed "
                             f"(cannot check remote_any_count_ge): {remote_text}")
                        remote_upper = remote_text.upper()
                        for kw, expected in args.get("remote_any_count_ge", []):
                            actual = remote_upper.count(str(kw).upper())
                            assert actual >= expected, (
                                f"[{mode_label}] Remote SQL(all) keyword '{kw}' "
                                f"count too small: expected >= {expected}, got {actual}: "
                                f"{remote_text}")

                elif chk == "local_not_has":
                    for op in args.get("local_not_has", []):
                        for line in lines:
                            if "Remote SQL:" in line:
                                continue
                            assert op not in line, (
                                f"[{mode_label}] Local plan should not "
                                f"contain '{op}': {line}")

                elif chk == "local_has":
                    if is_analyze and args.get("analyze_local_has"):
                        continue
                    local_has_args = args.get("local_has", [])
                    if (not is_analyze) and (not is_verbose) and \
                            ("plain_local_has" in args):
                        local_has_args = args.get("plain_local_has", [])
                        if not local_has_args:
                            continue
                    # Positive local-retention check: when a construct is not
                    # pushed to the remote source, the corresponding operator
                    # must remain in the local plan (outside Remote SQL).
                    local_text = self._get_local_plan_text(lines)
                    for kw in local_has_args:
                        assert kw.upper() in local_text.upper(), (
                            f"[{mode_label}] Local plan should retain "
                            f"'{kw}' (not pushed to remote): {local_text}")

                elif chk == "analyze_local_has":
                    if is_analyze:
                        # Non-verbose analyze output often does not preserve the
                        # original expression text, so keep analyze-only local
                        # retention checks separate from verbose/local_has.
                        local_text = self._get_local_plan_text(lines)
                        for kw in args.get("analyze_local_has", []):
                            assert kw.upper() in local_text.upper(), (
                                f"[{mode_label}] Local plan should retain "
                                f"'{kw}' in analyze output: {local_text}")

                elif chk == "verbose_remote_has":
                    # Mode-conditional: checked ONLY in explain_verbose (no
                    # execution).  For functions marked supported in the
                    # capability matrix but rejected by the running target at
                    # runtime (version-dependent, e.g. DataFusion GREATEST/LEAST),
                    # the plan pushes them down — visible here pre-fallback.
                    if is_verbose and not is_analyze:
                        remote = self._get_remote_sql_line(lines)
                        assert remote, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        for kw in args.get("verbose_remote_has", []):
                            assert self._remote_token_present(kw, remote.upper(), is_ss_leg), \
                                (f"[{mode_label}] Remote SQL should contain "
                                 f"'{kw}' (pre-fallback pushdown): {remote}")

                elif chk == "analyze_remote_not_has":
                    # Mode-conditional: checked ONLY in the verbose analyze mode
                    # (real execution with the re-planned plan).  The runtime
                    # function-level fallback demotes the unsupported function to
                    # local, so the re-planned Remote SQL must NOT contain it.
                    # The plain analyze mode has no Remote SQL line at all, so
                    # the check is skipped there.
                    if is_analyze and is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        assert remote, \
                            f"[{mode_label}] 'Remote SQL:' line not found"
                        semantic_remote = self._canonicalize_timestamp_transport(
                            remote)
                        for kw in args.get("analyze_remote_not_has", []):
                            assert kw.upper() not in semantic_remote.upper(), \
                                (f"[{mode_label}] Remote SQL should NOT contain "
                                 f"'{kw}' (function-level fallback to local): {remote}")

                elif chk == "no_data_leak":
                    for val in args.get("no_data_leak", []):
                        for line in lines:
                            assert val not in line, (
                                f"[{mode_label}] Plan should not contain "
                                f"data value '{val}': {line}")

                elif chk == "dialect_backtick":
                    # MySQL uses backtick quoting; only assert on MySQL source.
                    if is_verbose and src_label == "mysql":
                        remote = self._get_remote_sql_line(lines)
                        assert "`" in remote, (
                            f"[{mode_label}] MySQL Remote SQL should "
                            f"use backtick quoting: {remote}")

                elif chk == "dialect_double_quote":
                    # PG/InfluxDB use double-quote quoting; assert on those.
                    if is_verbose and src_label in ("pg", "influx"):
                        remote = self._get_remote_sql_line(lines)
                        assert '"' in remote, (
                            f"[{mode_label}] {src_label} Remote SQL should "
                            f"use double-quote quoting: {remote}")

                elif chk == "dialect_bracket":
                    # SQL Server uses [bracket] quoting; assert on that source.
                    if is_verbose and src_label == "sqlserver":
                        remote = self._get_remote_sql_line(lines)
                        assert "[" in remote, (
                            f"[{mode_label}] sqlserver Remote SQL should "
                            f"use bracket quoting: {remote}")

                elif chk == "min_federated_scans":
                    n = args.get("min_scans", 2)
                    if isinstance(n, list):
                        # A per-source dict without a key for this leg means
                        # "no expectation here" (see _per_src).
                        n = None
                    if n is not None:
                        cnt = sum(1 for l in lines
                                  if "Federated Scan" in l)
                        assert cnt >= n, (
                            f"[{mode_label}] Expected >={n} "
                            f"'Federated Scan', got {cnt}")

                elif chk == "single_federated_scan":
                    cnt = sum(1 for l in lines
                              if "Federated Scan" in l)
                    assert cnt == 1, (
                        f"[{mode_label}] Expected exactly 1 "
                        f"'Federated Scan', got {cnt}")

                elif chk == "no_table_scan":
                    assert "Table Scan" not in text, (
                        f"[{mode_label}] Plan should not contain "
                        f"'Table Scan'")

                elif chk == "compound_where":
                    if is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        has_logic = ("AND" in remote.upper()
                                     or "OR" in remote.upper())
                        assert has_logic, (
                            f"[{mode_label}] Compound WHERE should "
                            f"have AND/OR in Remote SQL: {remote}")

                elif chk == "projection_no_star":
                    if is_verbose:
                        remote = self._get_remote_sql_line(lines)
                        r_upper = remote.upper()
                        assert ("SELECT *" not in r_upper
                                or "time" in remote.lower()), (
                            f"[{mode_label}] Projection should push "
                            f"specific columns: {remote}")

                elif chk == "join_remote_or_multi":
                    remote_lines = [l for l in lines
                                    if "Remote SQL:" in l]
                    has_join = any("JOIN" in l.upper()
                                   for l in remote_lines)
                    scan_cnt = sum(1 for l in lines
                                   if "Federated Scan" in l)
                    # In verbose mode the Remote SQL is visible: require JOIN in it
                    # or >= 2 FedScans (two-side local join fallback).
                    # In plain mode there is no Remote SQL line; a single FedScan
                    # means the full JOIN was pushed into that scan, which is correct.
                    if is_verbose:
                        assert has_join or scan_cnt >= 2, (
                            f"[{mode_label}] JOIN not pushed and only "
                            f"{scan_cnt} Federated Scans")
                    else:
                        assert scan_cnt >= 1, (
                            f"[{mode_label}] Expected at least 1 Federated Scan, "
                            f"got {scan_cnt}")

                elif chk == "subquery_handling":
                    if is_verbose:
                        # 校验子查询被推到远端：
                        # 查询计划里有 Federated Scan 且没有本地 TableScan，
                        # 说明所有数据都来自远端（子查询内容成功推下去）。
                        fed_cnt = sum(1 for l in lines
                                      if "Federated Scan" in l)
                        local_scan_cnt = sum(1 for l in lines
                                             if "TableScan" in l or
                                             "SystemTableScan" in l)
                        assert fed_cnt >= 1 and local_scan_cnt == 0, (
                            f"[{mode_label}] Subquery not pushed to remote — "
                            f"fed_cnt={fed_cnt}, local_scan_cnt={local_scan_cnt}")

            except AssertionError as e:
                errors.append((case_id, chk, str(e)))

        return errors

    # ------------------------------------------------------------------
    # Source expansion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_real_sql(sql_tpl, src_info, sources_type):
        """Substitute source names/DBs into SQL template."""
        if sources_type == "cross":
            return sql_tpl.format(
                SRC=src_info["src"], DB=src_info["db"],
                SRC2=src_info["src2"], DB2=src_info["db2"],
            )
        return sql_tpl.format(
            SRC=src_info["src"], DB=src_info["db"],
        )

    def _expand_sources(self, case_def):
        """Yield (expanded_id, src_info) for a case definition.

        Every statement runs against ALL external sources.
        Any legitimate cross-source difference in the EXPLAIN result is
        expressed via per-source expectation dicts (see ``_per_src``).
        """
        sources_type = case_def.get("sources", "single")

        if sources_type == "single":
            for si in _SINGLE_SOURCES:
                yield f"{case_def['id']}_{si['label']}", si, sources_type
        elif sources_type == "custom":
            for si in case_def.get("source_infos", []):
                yield f"{case_def['id']}_{si['label']}", si, "single"
        elif sources_type == "cross":
            for pair in _CROSS_PAIRS:
                yield f"{case_def['id']}_{pair['label']}", pair, sources_type

    @staticmethod
    def _iter_case_sqls(case_def, key):
        sqls = case_def.get(key)
        if not sqls:
            return []
        if isinstance(sqls, str):
            return [sqls]
        return list(sqls)

    def _exec_case_sqls(self, case_def, key, src_info, src_type, case_id):
        for idx, sql_tpl in enumerate(self._iter_case_sqls(case_def, key), 1):
            sql = self._make_real_sql(sql_tpl, src_info, src_type)
            tdLog.info(f"[{case_id:<24s}] {key}#{idx}: {sql}")
            tdSql.execute(sql)

    # ------------------------------------------------------------------
    # Core dispatcher — run ALL cases in ONE mode
    # ------------------------------------------------------------------

    def _run_mode(self, mode, baseline_name):
        """Execute every case in *mode*, serialize, compare baseline."""
        os.makedirs(self._ANS_DIR, exist_ok=True)
        baseline_file = self._baseline_path(baseline_name)
        blocks = []
        failed = []
        mode_label = _MODE_LABELS[mode]

        # Two-level case filtering (both optional, both substring match):
        #   FQ_CASE_FILTER — matches case_def["id"] (group level, e.g. "ds_subquery"
        #                    runs every expanded case+source in that bucket).
        #   FQ_ID_FILTER   — matches the per-source expanded_id (single-statement
        #                    level, e.g. "ds_subquery-43_influx" runs exactly one
        #                    statement on one source; "_influx" runs only InfluxDB).
        # When FQ_ID_FILTER is set the case_def-level filter is skipped so the
        # expanded_id match alone decides what runs.
        _case_filter = os.environ.get("FQ_CASE_FILTER", "")
        _id_filter = os.environ.get("FQ_ID_FILTER", "")
        _id_filters = [part.strip() for part in _id_filter.split(",") if part.strip()]
        _subq_scope = os.environ.get("FQ_SUBQUERY_SCOPE", "").strip().lower()
        # Two phases: every non-SQL-Server block first (byte-for-byte the
        # previous execution order, so the shared baselines keep matching), then
        # the SQL Server blocks.  SQL Server blocks carry a `use <src>` pre-SQL
        # for the unqualified-reference cases, and execising that context
        # changes how a following MySQL interval expression is rendered - the
        # blocks are therefore run last instead of interleaved.
        # Each mode starts from a fresh session: a previous mode test can end
        # with a SQL Server USE context (the unqualified-reference cases in
        # phase 2 issue `use <src>`), and execising that context changes how a
        # later MySQL interval expression is rendered (an extra FROM_UNIXTIME
        # wrap).  Reconnecting restores the state the shared baselines were
        # produced in.
        self._reset_td_session()
        for phase in (0, 1):
            for case_def in _EXPLAIN_CASES:
                if not _id_filter and _case_filter and _case_filter not in case_def["id"]:
                    continue
                if _subq_scope in ("flat", "non_recursive", "nonrecursive"):
                    if _max_subquery_depth(case_def["sql"]) > 1:
                        continue
                elif _subq_scope == "recursive":
                    if _max_subquery_depth(case_def["sql"]) <= 1:
                        continue
                sql_tpl = case_def["sql"]
                expect_err_spec = case_def.get("expect_err", False)
                raw_args = case_def.get("check_args", {})

                for expanded_id, src_info, src_type in \
                        self._expand_sources(case_def):
                    if _id_filters and not any(part in expanded_id for part in _id_filters):
                        continue
                    is_ss_block = "sqlserver" in expanded_id.lower()
                    if is_ss_block and _SS_LOCAL_CHAR_RE.search(sql_tpl.lower()):
                        tdLog.info(
                            f"[{expanded_id:<24s}] SKIP  SQL Server character "
                            f"semantics covered by test_fq_20_sqlserver_semantics")
                        continue
                    if (phase == 1) != is_ss_block:
                        continue
                    # Per-source expect_err: accepts bool (all sources) or
                    # dict{label:bool} (per-source).  Resolve for this source.
                    src_label = src_info.get("label", "")
                    expect_err = _per_src(expect_err_spec, src_label)
                    expect_err_contains = _per_src(
                        raw_args.get("expect_err_contains", []), src_label)
                    if isinstance(expect_err_contains, str):
                        expect_err_contains = [expect_err_contains]
                    sql = self._make_real_sql(sql_tpl, src_info, src_type)
                    for attempt in range(2):
                        t0 = _time.monotonic()
                        try:
                            self._exec_case_sqls(case_def, "pre_sql", src_info, src_type, expanded_id)
                            lines = self._exec_explain(sql, mode)
                            elapsed = _time.monotonic() - t0
                            if expect_err:
                                # EXPLAIN succeeded but we expected it to fail —
                                # this is an unexpected success (regression).
                                msg = (f"[{_MODE_LABELS[mode]}] EXPLAIN unexpectedly "
                                       f"succeeded (expect_err=True): {sql}")
                                tdLog.info(
                                    f"[{expanded_id:<24s}] FAIL  expect_err: {msg}  "
                                    f"[{elapsed:.2f}s]")
                                failed.append((expanded_id, "expect_err", msg))
                                # Still serialize the result for the baseline record.
                                blocks.append(self._serialize_explain(
                                    expanded_id, sql, lines))
                            else:
                                errs = self._validate(expanded_id, mode, lines,
                                                      case_def, src_info)
                                blocks.append(self._serialize_explain(
                                    expanded_id, sql, lines))
                                if errs:
                                    for eid, chk, msg in errs:
                                        tdLog.info(
                                            f"[{eid:<24s}] FAIL  {chk}: {msg}  "
                                            f"[{elapsed:.2f}s]")
                                        failed.append((eid, chk, msg))
                                else:
                                    tdLog.info(
                                        f"[{expanded_id:<24s}] PASS  "
                                        f"{sql_tpl[:55]}  [{elapsed:.2f}s]")
                            break
                        except Exception as e:
                            elapsed = _time.monotonic() - t0
                            if self._maybe_recover_case(expanded_id, sql, e, attempt):
                                continue
                            if expect_err:
                                err_text = str(e)
                                missing = [
                                    kw for kw in expect_err_contains
                                    if kw not in err_text
                                ]
                                if missing:
                                    msg = (
                                        f"[{_MODE_LABELS[mode]}] Expected error "
                                        f"missing {missing}: {err_text}"
                                    )
                                    tdLog.info(
                                        f"[{expanded_id:<24s}] FAIL  "
                                        f"expect_err_contains: {msg}  "
                                        f"[{elapsed:.2f}s]")
                                    failed.append(
                                        (expanded_id, "expect_err_contains", msg))
                                # Expected failure — record as POS block (error text
                                # captured in baseline for change detection).
                                tdLog.info(
                                    f"[{expanded_id:<24s}] PASS(expected-err)  "
                                    f"{sql_tpl[:55]}  [{elapsed:.2f}s]")
                                blocks.append(self._serialize_err(
                                    expanded_id, sql, str(e)))
                            else:
                                # Unexpected failure — this is a real test failure.
                                tdLog.info(
                                    f"[{expanded_id:<24s}] FAIL(exec-err)  "
                                    f"{sql_tpl[:55]}  [{elapsed:.2f}s]  {e}")
                                blocks.append(self._serialize_err(
                                    expanded_id, sql, str(e)))
                                failed.append((expanded_id, "exec_err", str(e)))
                            break
                    try:
                        self._exec_case_sqls(case_def, "post_sql", src_info, src_type, expanded_id)
                    except Exception as e:
                        tdLog.info(
                            f"[{expanded_id:<24s}] FAIL(post-sql)  "
                            f"{sql_tpl[:55]}  {e}")
                        failed.append((expanded_id, "post_sql", str(e)))

        output = ExtSrcEnv.baseline_environment("\n".join(blocks) + "\n")
        # When either filter is active, extract only the matching blocks from
        # the baseline so we compare like-for-like (filtered subset vs subset).
        if _case_filter or _id_filter:
            # Collect the expanded_ids that were actually executed.
            executed_ids = set()
            for b in blocks:
                first_line = b.split("\n")[0]
                if first_line.startswith("### EXP-"):
                    executed_ids.add(first_line[4:].split(" ")[0])  # e.g. "EXP-ds_typeconv-04_mysql"
            self._compare_baseline_filtered(output, baseline_file, failed, executed_ids)
        else:
            self._compare_baseline(output, baseline_file, failed)

        if failed:
            summary = "\n".join(
                f"  {cid}: {msg}" for cid, _, msg in failed)
            pytest.fail(
                f"{len(failed)} check(s) failed in "
                f"{baseline_name} ({mode_label}):\n{summary}")

    # ==================================================================
    # test_* entry points — exactly 4, one per EXPLAIN mode
    # ==================================================================

    def test_explain(self):
        """EXPLAIN — basic plan: FederatedScan, operators, data isolation.

        Runs every query in ``_EXPLAIN_CASES`` using ``EXPLAIN <sql>``,
        validates plan structure, serialises output, compares baseline.

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-28 wpan Refactor to data-driven per-mode structure

        """
        self._run_mode(EXPLAIN, "test_explain")

    def test_explain_verbose(self):
        """EXPLAIN VERBOSE — Remote SQL, type mapping, pushdown, dialect.

        Runs every query using ``EXPLAIN VERBOSE TRUE <sql>``, validates
        Remote SQL content, pushdown flags, type mapping, dialect quoting,
        column projection, JOIN handling, and subquery decomposition.

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-28 wpan Refactor to data-driven per-mode structure

        """
        self._run_mode(EXPLAIN_VERBOSE, "test_explain_verbose")

    def test_explain_analyze(self):
        """EXPLAIN ANALYZE — execution metrics (rows=, time=, loops=).

        Runs every query using ``EXPLAIN ANALYZE <sql>``, validates
        that execution metric keywords appear in output.

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-28 wpan Refactor to data-driven per-mode structure

        """
        self._run_mode(EXPLAIN_ANALYZE, "test_explain_analyze")

    def test_explain_analyze_verbose(self):
        """EXPLAIN ANALYZE VERBOSE — full validation, all checks active.

        Runs every query using ``EXPLAIN ANALYZE VERBOSE TRUE <sql>``,
        validates everything: operators, Remote SQL, pushdown, type mapping,
        dialect, execution metrics, data isolation.

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-05-28 wpan Refactor to data-driven per-mode structure

        """
        self._run_mode(EXPLAIN_ANALYZE_VERBOSE,
                       "test_explain_analyze_verbose")

    # ------------------------------------------------------------------
    # SQL Server EXPLAIN — phase 2 pushdown (M1 stage, evolved per milestone)
    # ------------------------------------------------------------------

    _SS_DB = _SQLSERVER_DB
    _SS_SRC = _SRC_SQLSERVER

    def _setup_sqlserver_explain_source(self):
        ExtSrcEnv.sqlserver_create_db(self._SS_DB)
        # Same schema and rows as the MySQL/PG fixtures, so every matrix SQL
        # (written against `time`/`voltage`/`region`/...) runs unchanged.
        ExtSrcEnv.sqlserver_exec(self._SS_DB, [
            "IF OBJECT_ID('dbo.sensor') IS NOT NULL DROP TABLE dbo.sensor",
            "CREATE TABLE dbo.sensor ("
            # `time`/`current` are T-SQL keywords and must stay bracketed.
            " [time] datetime2(3) NOT NULL PRIMARY KEY, [voltage] float, [current] real,"
            " [region] nvarchar(32), [date_str] nvarchar(32), [json_str] nvarchar(64),"
            " [b64_str] nvarchar(32), [geom_wkt] nvarchar(64))",
            "INSERT INTO dbo.sensor VALUES "
            " ('2024-01-01T00:00:00.000',220.5,1.2,N'north',N'2024-01-01',N'{\"k\":1}',N'aGVsbG8=',N'POINT(0 0)'),"
            " ('2024-01-01T00:01:00.000',221.0,1.3,N'south',N'2024-01-02',N'{\"k\":2}',N'd29ybGQ=',N'POINT(1 1)'),"
            " ('2024-01-01T00:02:00.000',219.8,1.1,N'north',N'2024-01-03',N'{\"k\":3}',N'Zm9v',N'POINT(2 2)'),"
            " ('2024-01-01T00:03:00.000',222.0,1.4,N'south',N'2024-01-04',N'{\"k\":4}',N'YmFy',N'POINT(3 3)'),"
            " ('2024-01-01T00:04:00.000',220.0,1.0,N'north',N'2024-01-05',N'{\"k\":5}',N'YmF6',N'POINT(4 4)')",
            "IF OBJECT_ID('dbo.region_info') IS NOT NULL DROP TABLE dbo.region_info",
            "CREATE TABLE dbo.region_info ("
            " [time] datetime2(3) NOT NULL PRIMARY KEY, [region] nvarchar(32), [area] int)",
            "INSERT INTO dbo.region_info VALUES "
            " ('2024-01-01T00:00:00.000',N'north',1),"
            " ('2024-01-01T00:01:00.000',N'south',2)",
        ])
        self._cleanup_src(self._SS_SRC)
        self._mk_sqlserver_real(self._SS_SRC, database=self._SS_DB, schema="dbo")

    @staticmethod
    def _ss_plan_text(sql, mode="verbose"):
        if mode == "verbose":
            tdSql.query(f"explain verbose true {sql}")
        elif mode == "analyze":
            # `explain analyze` does not take the verbose flag.
            tdSql.query(f"explain analyze {sql}")
        else:
            tdSql.query(f"explain {sql}")
        return "\n".join(
            str(cell) for row in (tdSql.queryResult or ()) for cell in row if cell is not None
        )

    def test_explain_sqlserver_pushdown(self):
        """FQSS-SYS-002/003 + phase-2 pushdown EXPLAIN contract (M1-M4).

        Phase 2 turned SQL Server into a pushdown source milestone by
        milestone: M1 operators + FILTER/PROJECTION, M2 functions and
        aggregates, M3 ORDER BY / LIMIT / JOIN / windows, M4 the time family,
        tumbling windows and subqueries.  All four capability faces are
        enabled now, so the cases below assert the T-SQL rewrites actually
        emitted per DS 5.3.12-5.3.14:

        - predicate context stays bare, value context wraps in
          ``(CASE WHEN <pred> THEN 1 ELSE 0 END)`` (R1), string literals use
          N'...', timestamps never render as ``TIMESTAMP`` (R4);
        - LIMIT becomes ``TOP (n)`` without a sort and
          ``OFFSET 0 ROWS FETCH NEXT n ROWS ONLY`` with one;
        - aggregates render natively (COUNT/AVG), the time family uses the
          JUDGED rewrites and the tumbling bucket the DATEADD + DATEDIFF_BIG
          remainder form.

        Catalog: - Query:FederatedExplain

        Since: v3.4.4.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation (step-1 zero pushdown)
            - 2026-10-12 wpan Phase-2 M1: filter/projection pushdown contract
            - 2026-10-12 wpan Phase-2 M4: full pushdown contract (functions,
                               aggregates, ORDER/LIMIT, time family, buckets)

        """
        self._setup_sqlserver_explain_source()
        try:
            base = f"{self._SS_SRC}.{self._SS_DB}.dbo.sensor"
            area_base = f"{self._SS_SRC}.{self._SS_DB}.dbo.region_info"

            # (sql, remote_has, remote_not_has, local_has, local_not_has).
            # Keyword matching is case-insensitive against the joined Remote
            # SQL lines / the whole plan text.
            cases = [
                # PROJECTION: the scan prunes to the referenced columns.
                (f"select voltage from {base}",
                 ["[voltage]", "from [fq_exp_ssdb].[dbo].[sensor]"],
                 ["where", "case when"],
                 [], ["filter", "sort", "aggregate", "limit"]),
                # FILTER: M1 comparison pushes natively (predicate context,
                # no CASE wrapping).
                (f"select voltage from {base} where voltage > 220.0",
                 ["where ([voltage] > 220.0)"],
                 ["case when", "group by", "limit"],
                 [], ["filter"]),
                # SORT + LIMIT: T-SQL has no LIMIT; with a sort the statement
                # uses the OFFSET ... FETCH NEXT form (DS 5.3.6).
                (f"select voltage from {base} order by voltage limit 1",
                 ["order by [voltage]", "offset 0 rows fetch next 1 rows only"],
                 [" limit "],
                 [], ["sort"]),
                # LIMIT without ORDER BY is rendered structurally as TOP (n).
                (f"select voltage from {base} limit 2",
                 ["select top (2) [voltage] from [fq_exp_ssdb].[dbo].[sensor]"],
                 [" limit ", "offset", "order by"],
                 [], ["limit"]),
                # AGGREGATE: COUNT renders as COUNT_BIG (TDengine COUNT is
                # BIGINT); AVG over a DOUBLE column renders natively.
                (f"select count(*), avg(voltage) from {base}",
                 ["count_big(", "avg("],
                 ["group by", "having"],
                 [], ["aggregate"]),
                # Value-context boolean: CASE WHEN wrap (no boolean scalar in
                # T-SQL).
                (f"select voltage > 220.0 from {base}",
                 ["(case when ([voltage] > 220.0) then 1 else 0 end)"],
                 [],
                 [], ["filter"]),
                # IS NULL pushes natively.
                (f"select voltage from {base} where voltage is null",
                 ["where ([voltage] is null)"],
                 ["is true", "is false"],
                 [], ["filter"]),
                # LIKE: TDengine folds ASCII letters only.  Both sides are
                # uppercased with TRANSLATE and compared under a binary
                # collation; '_' patterns stay local (UTF-16 vs UCS4 units).
                (f"select voltage from {base} where region like 'no%'",
                 ["translate(", "collate latin1_general_bin2", "like n'no%'",
                  "escape '\\'"],
                 [],
                 [], ["filter"]),
                (f"select voltage from {base} where region not like 'no%'",
                 ["translate(", "not like n'no%'", "escape '\\'"],
                 [],
                 [], ["filter"]),
                # IN list over a numeric column pushes.
                (f"select voltage from {base} where voltage in (220.0, 221.0)",
                 ["where [voltage] in (220.0, 221.0)"],
                 [],
                 [], ["filter"]),
                # AND with a string equality: the equality pushes with the
                # exact BIN2 + DATALENGTH rewrite; the numeric comparison is
                # unchanged.
                (f"select voltage from {base} where region = 'north' and voltage > 219.0",
                 ["collate latin1_general_bin2", "n'north'", "len(",
                  "[voltage] > 219.0"],
                 [],
                 [], ["filter"]),
                # Integer divisor: `/` must evaluate as DOUBLE on both sides
                # and map a zero divisor to NULL (TDengine semantics).
                (f"select area / 2 from {area_base}",
                 ["(cast([area] as float) / nullif(cast(2 as float), 0))"],
                 [],
                 [], ["filter"]),
                # Integer bitwise AND pushes, widened to BIGINT.
                (f"select area & 1 from {area_base}",
                 ["(cast([area] as bigint) & cast(1 as bigint))"],
                 [],
                 [], ["filter"]),
                # CAST renders the T-SQL target type (DS 5.3.13), so the
                # predicate pushes.
                (f"select voltage from {base} where cast(voltage as bigint) > 1",
                 ["cast(", "where"],
                 [],
                 [], ["filter"]),
                # Character functions depend on the remote code page / UTF-16
                # units, so LENGTH(region) stays local.
                (f"select voltage from {base} where length(region) > 3",
                 [],
                 ["len("],
                 ["filter"], []),
                # M3: T-SQL sorts NULLs first for ASC / last for DESC, so
                # only the other two combos need a CASE prefix key; the
                # native combos render bare and never emit a NULLS clause.
                (f"select voltage from {base} order by voltage asc nulls last",
                 ["order by case when [voltage] is null then 1 else 0 end asc, [voltage] asc"],
                 ["nulls last"],
                 [], ["sort"]),
                (f"select voltage from {base} order by voltage asc nulls first",
                 ["order by [voltage] asc"],
                 ["case when", "nulls first"],
                 [], ["sort"]),
                # M4: the time family renders its judged rewrite; a tz-naive
                # date() column becomes CONVERT(VARCHAR(10), ..., 23) and the
                # statement never emits a T-SQL TIMESTAMP literal (R4).
                (f"select date(time) from {base}",
                 ["convert(varchar(10)"],
                 ["timestamp "],
                 [], ["filter"]),
                # M4: the tumbling bucket uses the DATEADD + DATEDIFF_BIG
                # remainder form (T-SQL has no DATE_BIN).
                (f"select count(*) from {base} where voltage > 220.0 "
                 f"interval(10s)",
                 ["dateadd(", "datediff_big(", "group by"],
                 ["date_bin", " limit "],
                 [], []),
                # M4: an IN subquery executes on the source side.
                (f"select voltage from {base} where voltage in "
                 f"(select voltage from {base} where voltage > 220.0)",
                 [" in ("],
                 [],
                 [], ["filter"]),
            ]
            for sql, remote_has, remote_not_has, local_has, local_not_has in cases:
                text = self._ss_plan_text(sql)
                lower = text.lower()
                assert "federated scan" in lower, f"no federated scan for {sql!r}: {text!r}"
                remote = "\n".join(
                    line for line in text.splitlines() if "remote sql:" in line.lower())
                assert remote, f"no Remote SQL for {sql!r}: {text!r}"
                remote_lower = remote.lower()
                for token in remote_has:
                    assert token.lower() in remote_lower, (
                        f"remote SQL lacks {token!r} for {sql!r}: {remote!r}")
                for token in remote_not_has:
                    assert token.lower() not in remote_lower, (
                        f"remote SQL must not carry {token!r} at M1 for {sql!r}: {remote!r}")
                for token in local_has:
                    assert token.lower() in lower, (
                        f"local plan lacks {token!r} for {sql!r}: {text!r}")
                for token in local_not_has:
                    assert token.lower() not in lower, (
                        f"local plan must not retain {token!r} for {sql!r}: {text!r}")
                # Analyze mode must still produce metrics for the federated scan.
                analyze = self._ss_plan_text(sql, mode="analyze")
                assert "federated" in analyze.lower(), (
                    f"analyze plan lacks the federated scan for {sql!r}: {analyze!r}")
        finally:
            self._cleanup_src(self._SS_SRC)
            try:
                ExtSrcEnv.sqlserver_drop_db(self._SS_DB)
            except Exception:
                pass
