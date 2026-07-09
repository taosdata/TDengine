###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Test data preparation helpers for the 05-VirtualTables suite.

The file is organized top-down into six self-contained sections:

  1. Column / table schemas           — pure data
  2. Column → origin-table maps       — pure data
  3. SQL builders                     — pure functions
  4. TDengine origin-table builders   — small helpers
  5. Backend adapters                 — PG / MySQL / Influx
  6. VtableQueryUtil orchestrator     — preserved public API

External-source lifecycle (start/stop/connection params/exec helpers)
is delegated to `ExtSrcEnv` in `federated_query_common.py` — see the
backend adapter classes for the integration points.
"""

import os
import sys
import time
from datetime import datetime, timedelta

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

# ---- Shared FederatedQuery env helpers ------------------------------------
# ExtSrcEnv exposes connection params, exec/query helpers, and selective
# start_*_instance / stop_*_instance methods for each backend. We import it
# once here so adapter classes can delegate cleanly.
sys.path.insert(0, os.path.join(
    os.path.dirname(__file__), '..', '09-DataQuerying', '19-FederatedQuery'))

# Convenience: propagate generic FQ_{PG,MYSQL,INFLUX}_PORT to the
# versioned variants that ExtSrcEnv reads (FQ_PG_PORT_16 etc.) so users
# can point at a locally-running instance with a single env var.
for _proto, _ver_tag in (
        ("PG",     "16"),
        ("MYSQL",  "80"),
        ("INFLUX", "30")):
    _generic = os.environ.get(f"FQ_{_proto}_PORT")
    _versioned_key = f"FQ_{_proto}_PORT_{_ver_tag}"
    if _generic and not os.environ.get(_versioned_key):
        os.environ[_versioned_key] = _generic

from federated_query_common import ExtSrcEnv  # noqa: E402


# ============================================================================
# Section 1.  Column / table schemas
# ============================================================================

# Base list of (column_name, tdengine_type) used by VTABLE column definitions.
# Backend variants are derived by applying small override dicts.
VTABLE_COLUMN_TYPES = (
    ("u_tinyint_col", "tinyint unsigned"),
    ("u_smallint_col", "smallint unsigned"),
    ("u_int_col", "int unsigned"),
    ("u_bigint_col", "bigint unsigned"),
    ("tinyint_col", "tinyint"),
    ("smallint_col", "smallint"),
    ("int_col", "int"),
    ("bigint_col", "bigint"),
    ("float_col", "float"),
    ("double_col", "double"),
    ("bool_col", "bool"),
    ("binary_16_col", "binary(16)"),
    ("binary_32_col", "binary(32)"),
    ("nchar_16_col", "nchar(16)"),
    ("nchar_32_col", "nchar(32)"),
)


def _override_column_types(overrides):
    """Return a new column-types tuple with selected types replaced."""
    return tuple((col, overrides.get(col, t)) for col, t in VTABLE_COLUMN_TYPES)


# PG: same numeric types as TDengine; binary stays binary in the VTABLE.
# Used for both ext-source and mixed-source PG vtables.
VTABLE_COLUMN_TYPES_PG = _override_column_types({
    "u_tinyint_col": "smallint",
    "u_smallint_col": "int",
    "u_int_col": "bigint",
    "u_bigint_col": "bigint",
    "tinyint_col": "smallint",
    "smallint_col": "smallint",
    "float_col": "float",
    "double_col": "double",
    "bool_col": "bool",
    "binary_16_col": "nchar(16)",
    "binary_32_col": "nchar(32)",
    "nchar_16_col": "nchar(16)",
    "nchar_32_col": "nchar(32)",
})

# MySQL: no unsigned types — promoted to next signed size.
# binary_* columns are declared in MySQL with ASCII charset → VARCHAR;
# nchar_*  columns are declared in MySQL with utf8mb4 charset → NCHAR.
VTABLE_COLUMN_TYPES_MYSQL = _override_column_types({
    "u_tinyint_col": "smallint",
    "u_smallint_col": "int",
    "u_int_col": "bigint",
    "u_bigint_col": "bigint",
    "tinyint_col": "smallint",
    "smallint_col": "smallint",
    "binary_16_col": "varchar(16)",
    "binary_32_col": "varchar(32)",
    "nchar_16_col": "nchar(16)",
    "nchar_32_col": "nchar(32)",
})

# InfluxDB: every integer is i64 (→ bigint); every float is f64 (→ double).
# Strings only exist as Utf8 (→ nchar).
VTABLE_COLUMN_TYPES_INFLUX = _override_column_types({
    "u_tinyint_col": "bigint",
    "u_smallint_col": "bigint",
    "u_int_col": "bigint",
    "u_bigint_col": "bigint",
    "tinyint_col": "bigint",
    "smallint_col": "bigint",
    "int_col": "bigint",
    "float_col": "double",
    "binary_16_col": "nchar(16)",
    "binary_32_col": "nchar(32)",
})

# Mixed-mode column types are computed per adapter via _build_mixed_column_types
# below, using 3-way positional alternation between external (src 1/2) and
# internal TDengine sources. No predetermined column-to-source mapping.


def _build_mixed_column_buckets():
    """Return {col_name: bucket} where bucket is 'ext' or 'internal'.

    Decides per-column ownership in mixed mode by position in
    VTABLE_COLUMN_TYPES (every 3rd data column goes to the internal
    TDengine source). Used by both _build_mixed_column_types (which sets
    declared types on the vtable) and VtableQueryUtil._mixed_map (which
    picks the actual data source ref), so the two stay in sync regardless
    of source-map key order.
    """
    buckets = {}
    data_idx = 0
    for col, _ in VTABLE_COLUMN_TYPES:
        if col == "ts":
            continue
        buckets[col] = "internal" if data_idx % 3 == 2 else "ext"
        data_idx += 1
    return buckets


_MIXED_COLUMN_BUCKETS = _build_mixed_column_buckets()


def _build_mixed_column_types(ext_types):
    """Compute per-position column types for mixed (ext1/ext2/internal) mode.

    Data columns at positions where (data_idx % 3) in {0, 1} come from the
    external source — use the adapter's external column types. Data columns
    at positions where (data_idx % 3) == 2 come from the local TDengine
    table — use the base VTABLE_COLUMN_TYPES.
    """
    ext_map  = dict(ext_types)
    base_map = dict(VTABLE_COLUMN_TYPES)
    out = []
    for col, base_type in VTABLE_COLUMN_TYPES:
        if col == "ts":
            out.append((col, base_type))
            continue
        if _MIXED_COLUMN_BUCKETS[col] == "internal":
            out.append((col, base_map[col]))
        else:
            out.append((col, ext_map[col]))
    return tuple(out)


# --- External-source virtual-reference mode ------------------------------
#
# The top vtables reference an intermediate virtual table. That intermediate
# table intentionally mixes three reference kinds across columns:
#   virtual  → another virtual table layer
#   internal → a local TDengine normal table
#   ext      → an external source table
_EXT_VIRTUAL_REF_CYCLE = ("virtual", "internal", "ext")


def _build_ext_virtual_ref_column_buckets():
    """Return {col_name: bucket} for ext-source virtual-ref mode."""
    buckets = {}
    data_idx = 0
    for col, _ in VTABLE_COLUMN_TYPES:
        if col == "ts":
            continue
        buckets[col] = _EXT_VIRTUAL_REF_CYCLE[
            data_idx % len(_EXT_VIRTUAL_REF_CYCLE)
        ]
        data_idx += 1
    return buckets


_EXT_VIRTUAL_REF_COLUMN_BUCKETS = _build_ext_virtual_ref_column_buckets()


def _build_ext_virtual_ref_column_types(ext_types):
    """Declared types for ext-source virtual-ref mode.

    Columns directly or indirectly backed by the external source use the
    adapter's vtable-safe external types. Columns directly backed by TDengine
    retain the base TDengine types.
    """
    ext_map = dict(ext_types)
    base_map = dict(VTABLE_COLUMN_TYPES)
    out = []
    for col, base_type in VTABLE_COLUMN_TYPES:
        if col == "ts":
            out.append((col, base_type))
            continue
        if _EXT_VIRTUAL_REF_COLUMN_BUCKETS[col] in ("internal", "virtual"):
            out.append((col, base_map[col]))
        else:
            out.append((col, ext_map[col]))
    return tuple(out)


# --- All-sources (7-way) mode --------------------------------------------
#
# A single virtual table whose columns are split across 7 sources at once:
#   bucket 0 → internal TDengine
#   buckets 1,2 → PG sources       (pg_test_1, pg_test_2)
#   buckets 3,4 → MySQL sources    (mysql_test_1, mysql_test_2)
#   buckets 5,6 → Influx sources   (influx_test_1, influx_test_2)
#
# Bucket assignment is by each column's POSITION in VTABLE_COLUMN_TYPES,
# so the same column always lands in the same source across every vtable.
_ALL_SOURCE_CYCLE = (
    ('internal', 0),
    ('pg',       0),
    ('pg',       1),
    ('mysql',    0),
    ('mysql',    1),
    ('influx',   0),
    ('influx',   1),
)


def _build_all_column_buckets():
    """Return {col_name: (backend, src_idx)} using 7-way positional cycle."""
    buckets = {}
    data_idx = 0
    for col, _ in VTABLE_COLUMN_TYPES:
        if col == "ts":
            continue
        buckets[col] = _ALL_SOURCE_CYCLE[data_idx % len(_ALL_SOURCE_CYCLE)]
        data_idx += 1
    return buckets


_ALL_COLUMN_BUCKETS = _build_all_column_buckets()


def _build_all_column_types():
    """Per-column declared type matches each column's assigned backend."""
    type_map_by_backend = {
        'internal': dict(VTABLE_COLUMN_TYPES),
        'pg':       dict(VTABLE_COLUMN_TYPES_PG),
        'mysql':    dict(VTABLE_COLUMN_TYPES_MYSQL),
        'influx':   dict(VTABLE_COLUMN_TYPES_INFLUX),
    }
    out = []
    for col, base_type in VTABLE_COLUMN_TYPES:
        if col == "ts":
            out.append((col, base_type))
            continue
        backend, _ = _ALL_COLUMN_BUCKETS[col]
        out.append((col, type_map_by_backend[backend][col]))
    return tuple(out)


VTABLE_COLUMN_TYPES_ALL = _build_all_column_types()


# Backend-side table column declarations (used when creating origin tables in
# the external DBs).
PG_TABLE_COLUMNS = (
    'ts TIMESTAMP NOT NULL PRIMARY KEY',
    'u_tinyint_col SMALLINT', 'u_smallint_col INTEGER',
    'u_int_col BIGINT', 'u_bigint_col BIGINT',
    'tinyint_col SMALLINT', 'smallint_col SMALLINT',
    'int_col INTEGER', 'bigint_col BIGINT',
    'float_col REAL', 'double_col DOUBLE PRECISION', 'bool_col BOOLEAN',
    'binary_16_col VARCHAR(16)', 'binary_32_col VARCHAR(32)',
    'nchar_16_col TEXT', 'nchar_32_col TEXT',
    'groupid INTEGER', 'location TEXT',
)

PG_COPY_COLUMN_LIST = (
    'ts',
    'u_tinyint_col', 'u_smallint_col', 'u_int_col', 'u_bigint_col',
    'tinyint_col', 'smallint_col', 'int_col', 'bigint_col',
    'float_col', 'double_col', 'bool_col',
    'binary_16_col', 'binary_32_col', 'nchar_16_col', 'nchar_32_col',
    'groupid', 'location',
)

MYSQL_TABLE_COLUMNS = (
    'ts DATETIME(3) NOT NULL PRIMARY KEY',
    'u_tinyint_col SMALLINT', 'u_smallint_col INT',
    'u_int_col BIGINT', 'u_bigint_col BIGINT',
    'tinyint_col SMALLINT', 'smallint_col SMALLINT',
    'int_col INT', 'bigint_col BIGINT',
    'float_col FLOAT', 'double_col DOUBLE', 'bool_col TINYINT(1)',
    # binary_*  → declared as ASCII so connector maps to VARCHAR
    # nchar_*   → declared as utf8mb4 so connector maps to NCHAR
    'binary_16_col VARCHAR(16) CHARACTER SET ascii',
    'binary_32_col VARCHAR(32) CHARACTER SET ascii',
    'nchar_16_col VARCHAR(16) CHARACTER SET utf8mb4',
    'nchar_32_col VARCHAR(32) CHARACTER SET utf8mb4',
    'groupid INT', 'location VARCHAR(64) CHARACTER SET utf8mb4',
)

# InfluxDB field columns: every vtable data column is a field — the 11 numeric
# columns plus the 4 string columns (binary_*/nchar_*). Strings are Utf8 fields
# and map to nchar in the vtable.
INFLUX_FIELD_COLUMNS = (
    'u_tinyint_col', 'u_smallint_col', 'u_int_col', 'u_bigint_col',
    'tinyint_col', 'smallint_col', 'int_col', 'bigint_col',
    'float_col', 'double_col', 'bool_col',
    'binary_16_col', 'binary_32_col', 'nchar_16_col', 'nchar_32_col',
)
# Field columns whose line-protocol value must be a quoted/escaped string.
INFLUX_STRING_FIELD_COLUMNS = (
    'binary_16_col', 'binary_32_col', 'nchar_16_col', 'nchar_32_col',
)
# InfluxDB tag columns: low-cardinality dimensions, constant per origin group.
# groupid/location are not vtable data columns; they only give the measurement
# realistic tags. The data4-9 CSVs omit them, so a deterministic constant is
# synthesized per group (see _InfluxAdapter._group_tags) — this keeps every
# group's tag set identical, which the SERIES exact-tag-match requires in
# series mode.
INFLUX_TAG_COLUMNS = (
    'groupid', 'location',
)


# TDengine origin normal-table column block.
_TDE_ORIGIN_NTB_COLUMNS = (
    "ts timestamp, "
    "u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, "
    "u_int_col int unsigned, u_bigint_col bigint unsigned, "
    "tinyint_col tinyint, smallint_col smallint, "
    "int_col int, bigint_col bigint, "
    "float_col float, double_col double, bool_col bool, "
    "binary_16_col binary(16), binary_32_col binary(32), "
    "nchar_16_col nchar(16), nchar_32_col nchar(32)"
)
_TDE_ORIGIN_SMA_COLS = (
    "u_tinyint_col, u_smallint_col, u_int_col, u_bigint_col, "
    "tinyint_col, smallint_col, int_col, bigint_col, "
    "float_col, double_col, bool_col, "
    "binary_16_col, binary_32_col, nchar_16_col, nchar_32_col"
)

# VSTABLE tag clause used by every CREATE STABLE … VIRTUAL 1 statement here.
_VSTB_TAGS = (
    "int_tag int, "
    "bool_tag bool, "
    "float_tag float, "
    "double_tag double, "
    "nchar_32_tag nchar(32), "
    "binary_32_tag binary(32)"
)


# ============================================================================
# Section 2.  Column → origin-table source maps
# ============================================================================
#
# Each map describes which origin (column → table) each VTABLE column reads
# from. They are rendered into SQL by combining with a ref-builder appropriate
# for the deployment (same-db / cross-db / external).

# --- Same-database / mode-1 / NTB & CTB maps ------------------------------
NTB_FULL_SOURCE_TABLES = {
    'u_tinyint_col':  'vtb_org_tb_0',
    'u_smallint_col': 'vtb_org_tb_1',
    'u_int_col':      'vtb_org_tb_2',
    'u_bigint_col':   'vtb_org_tb_0',
    'tinyint_col':    'vtb_org_tb_1',
    'smallint_col':   'vtb_org_tb_2',
    'int_col':        'vtb_org_tb_0',
    'bigint_col':     'vtb_org_tb_1',
    'float_col':      'vtb_org_tb_2',
    'double_col':     'vtb_org_tb_0',
    'bool_col':       'vtb_org_tb_1',
    'binary_16_col':  'vtb_org_tb_2',
    'binary_32_col':  'vtb_org_tb_0',
    'nchar_16_col':   'vtb_org_tb_1',
    'nchar_32_col':   'vtb_org_tb_2',
}

NTB_HALF_SOURCE_TABLES = {
    'u_tinyint_col':  'vtb_org_tb_0',
    'u_smallint_col': 'vtb_org_tb_1',
    'u_int_col':      'vtb_org_tb_2',
    'int_col':        'vtb_org_tb_0',
    'bigint_col':     'vtb_org_tb_1',
    'float_col':      'vtb_org_tb_2',
    'binary_32_col':  'vtb_org_tb_0',
    'nchar_16_col':   'vtb_org_tb_1',
    'nchar_32_col':   'vtb_org_tb_2',
}

# CTB half references tables 3/4/5 (data4/5/6.csv) in the original test.
CTB_HALF_SOURCE_TABLES = {
    'u_tinyint_col':  'vtb_org_tb_3',
    'u_smallint_col': 'vtb_org_tb_4',
    'u_int_col':      'vtb_org_tb_5',
    'int_col':        'vtb_org_tb_3',
    'bigint_col':     'vtb_org_tb_4',
    'float_col':      'vtb_org_tb_5',
    'binary_32_col':  'vtb_org_tb_3',
    'nchar_16_col':   'vtb_org_tb_4',
    'nchar_32_col':   'vtb_org_tb_5',
}

# CTB mix references child tables 6/7/8 (instead of normal_*).
CTB_MIX_SOURCE_TABLES = {
    'u_tinyint_col':  'vtb_org_tb_6',
    'u_smallint_col': 'vtb_org_tb_7',
    'u_int_col':      'vtb_org_tb_8',
    'u_bigint_col':   'vtb_org_tb_6',
    'tinyint_col':    'vtb_org_tb_7',
    'smallint_col':   'vtb_org_tb_8',
    'int_col':        'vtb_org_tb_6',
    'bigint_col':     'vtb_org_tb_7',
    'float_col':      'vtb_org_tb_8',
    'double_col':     'vtb_org_tb_6',
    'bool_col':       'vtb_org_tb_7',
    'binary_16_col':  'vtb_org_tb_8',
    'binary_32_col':  'vtb_org_tb_6',
    'nchar_16_col':   'vtb_org_tb_7',
    'nchar_32_col':   'vtb_org_tb_8',
}


def _make_full_source_map(base_idx):
    """Three-cyclic full map starting at vtb_org_tb_{base_idx}."""
    t = [f'vtb_org_tb_{base_idx + i}' for i in range(3)]
    return {
        'u_tinyint_col': t[0], 'u_smallint_col': t[1], 'u_int_col':    t[2],
        'u_bigint_col':  t[0], 'tinyint_col':    t[1], 'smallint_col': t[2],
        'int_col':       t[0], 'bigint_col':     t[1], 'float_col':    t[2],
        'double_col':    t[0], 'bool_col':       t[1], 'binary_16_col':t[2],
        'binary_32_col': t[0], 'nchar_16_col':   t[1], 'nchar_32_col': t[2],
    }


# --- Mode-2 half-child column subsets (same shape as same_db mode-2) ------
MODE2_HALF_0_MAP = {
    'u_tinyint_col':  'vtb_org_tb_9',
    'u_smallint_col': 'vtb_org_tb_10',
    'u_int_col':      'vtb_org_tb_11',
    'int_col':        'vtb_org_tb_9',
    'bigint_col':     'vtb_org_tb_10',
    'float_col':      'vtb_org_tb_11',
    'binary_32_col':  'vtb_org_tb_9',
    'nchar_16_col':   'vtb_org_tb_10',
    'nchar_32_col':   'vtb_org_tb_11',
}
MODE2_HALF_1_MAP = {
    'tinyint_col':    'vtb_org_tb_12',
    'smallint_col':   'vtb_org_tb_13',
    'int_col':        'vtb_org_tb_14',
    'bigint_col':     'vtb_org_tb_12',
    'float_col':      'vtb_org_tb_13',
    'double_col':     'vtb_org_tb_14',
    'bool_col':       'vtb_org_tb_12',
    'binary_16_col':  'vtb_org_tb_13',
    'binary_32_col':  'vtb_org_tb_14',
}
MODE2_HALF_2_MAP = {
    'u_int_col':      'vtb_org_tb_15',
    'u_bigint_col':   'vtb_org_tb_16',
    'tinyint_col':    'vtb_org_tb_17',
    'smallint_col':   'vtb_org_tb_15',
    'int_col':        'vtb_org_tb_16',
    'bigint_col':     'vtb_org_tb_17',
    'float_col':      'vtb_org_tb_15',
    'binary_32_col':  'vtb_org_tb_16',
    'nchar_16_col':   'vtb_org_tb_17',
}

# --- Cross-DB db-index cycles --------------------------------------------
# Cross-DB tests spread the 4 origin databases across columns in a fixed
# periodic pattern. Each map below is derived from a same-db map by
# attaching a (db_idx, table) tuple to each entry via `_with_db_cycle`.
_CROSS_DB_MODE1_CYCLE = [0, 1, 2, 3, 2]
_CROSS_DB_MODE2_CYCLE = [1, 0, 2]


def _with_db_cycle(table_map, cycle):
    """Promote {col: table} → {col: (db_idx, table)} using a cyclic db pattern."""
    return {col: (cycle[i % len(cycle)], table)
            for i, (col, table) in enumerate(table_map.items())}


CROSS_DB_NTB_FULL_MODE1 = _with_db_cycle(NTB_FULL_SOURCE_TABLES, _CROSS_DB_MODE1_CYCLE)
CROSS_DB_NTB_HALF_MODE1 = _with_db_cycle(NTB_HALF_SOURCE_TABLES, _CROSS_DB_MODE1_CYCLE)
CROSS_DB_CTB_FULL_MODE1 = CROSS_DB_NTB_FULL_MODE1
CROSS_DB_CTB_HALF_MODE1 = _with_db_cycle(CTB_HALF_SOURCE_TABLES, _CROSS_DB_MODE1_CYCLE)
CROSS_DB_CTB_MIX_MODE1  = _with_db_cycle(CTB_MIX_SOURCE_TABLES,  _CROSS_DB_MODE1_CYCLE)


# ============================================================================
# Section 3.  SQL builders (pure functions)
# ============================================================================

def build_origin_ntable_sql(table_name):
    return (f"CREATE TABLE `{table_name}` ({_TDE_ORIGIN_NTB_COLUMNS}) "
            f"SMA({_TDE_ORIGIN_SMA_COLS})")


def build_vstable_sql(column_types=VTABLE_COLUMN_TYPES, stb="vtb_virtual_stb"):
    cols = ['ts timestamp']
    cols.extend(f'{c} {t}' for c, t in column_types)
    return (f"CREATE STABLE `{stb}` ({', '.join(cols)}) "
            f"TAGS ({_VSTB_TAGS}) VIRTUAL 1")


def build_vtable_normal_sql(name, source_map, column_types=VTABLE_COLUMN_TYPES,
                            series_clause=""):
    """`CREATE VTABLE name (ts timestamp, col type [from ref], …)`.

    @param series_clause Optional trailing `SERIES … (tag='value')` clause used
                         by the InfluxDB series-mode path; empty otherwise.
    """
    parts = ['ts timestamp']
    for col, ctype in column_types:
        ref = source_map.get(col)
        if ref:
            parts.append(f'{col} {ctype} from {ref}')
        else:
            parts.append(f'{col} {ctype}')
    return f"CREATE VTABLE `{name}` ({', '.join(parts)}){series_clause}"


def build_vtable_child_sql(name, source_map, tags,
                           column_types=VTABLE_COLUMN_TYPES,
                           stb="vtb_virtual_stb", series_clause=""):
    """`CREATE VTABLE name (col from ref, …) USING vstb TAGS (…)`.

    @param series_clause Optional trailing `SERIES … (tag='value')` clause used
                         by the InfluxDB series-mode path; empty otherwise.
    """
    parts = [f'{col} from {source_map[col]}'
             for col, _ in column_types if col in source_map]
    if parts:
        return (f"CREATE VTABLE `{name}` ({', '.join(parts)}) "
                f"USING `{stb}` TAGS ({tags}){series_clause}")
    return f"CREATE VTABLE `{name}` USING `{stb}` TAGS ({tags})"


# ============================================================================
# Section 4.  TDengine origin-table builders
# ============================================================================

def _create_vtable_database(db_name):
    """Create a clean empty TDengine database to hold virtual tables.

    Used for ext-source mode where all columns come from the external source —
    no local origin tables are needed, only the database itself.
    """
    tdSql.execute(f"drop database if exists {db_name};")
    tdSql.execute(f"create database {db_name} vgroups 2;")
    tdSql.execute(f"use {db_name};")


def _prepare_one_origin_database(db_name, sma=False, debug_select_database=False):
    """Build the standard 18-child / 18-normal origin schema in one TDengine DB.

    Loads CSV data/{1..9}.csv into tables 0..8 and replays them +3 days for
    tables 9..17. Idempotent: drops and recreates the database first.
    `debug_select_database` emits a `select database();` after `use` (original
    same-db behavior; harmless no-op).
    """
    tdSql.execute(f"drop database if exists {db_name};")
    if sma:
        tdSql.execute(f"create database {db_name} vgroups 2 minrows 10 "
                      f"maxrows 200 stt_trigger 1;")
    else:
        tdSql.execute(f"create database {db_name} vgroups 2;")
    tdSql.execute(f"use {db_name};")
    if debug_select_database:
        tdSql.execute("select database();")

    tdLog.info(f"prepare origin normal tables in {db_name}.")
    for i in range(18):
        tdSql.execute(build_origin_ntable_sql(f"vtb_org_tb_{i}"))

    for i in range(9):
        datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
        tdSql.execute(f"insert into vtb_org_tb_{i} file'{datafile}';")

    # Tables 9..17 = tables 0..8 shifted +3 days.
    cols_csv = ("u_tinyint_col, u_smallint_col, u_int_col, u_bigint_col, "
                "tinyint_col, smallint_col, int_col, bigint_col, "
                "float_col, double_col, bool_col, "
                "binary_16_col, binary_32_col, nchar_16_col, nchar_32_col")
    for i in range(9, 18):
        tdSql.execute(f"insert into vtb_org_tb_{i} select ts + 3d, "
                      f"{cols_csv} from vtb_org_tb_{i-9};")

    tdSql.execute(f"flush database {db_name};")


# ============================================================================
# Section 5.  External backend adapters
# ============================================================================
#
# Each adapter owns the lifecycle of one external backend (PG / MySQL /
# Influx). It exposes a uniform interface used by the orchestrator:
#
#     adapter.column_types         — VTABLE column types for ext-source mode
#     adapter.mixed_column_types   — VTABLE column types for mixed mode
#     adapter.ensure_running()     — probe; start instance if needed
#     adapter.create_external_source()
#     adapter.prepare_origin_tables()
#     adapter.ref(table, col)      — full ref for vtable DDL
#     adapter.ref_short(table, col)
#
# Connection params, exec helpers (mysql_exec / pg_exec / influx_write /
# influx_query_sql), TCP probe and selective start_*_instance are delegated
# to ExtSrcEnv to avoid duplication.


# --- PostgreSQL adapter ----------------------------------------------------
class _PgAdapter:
    BACKEND       = "pg"
    SOURCE_NAMES  = ("pg_test_1", "pg_test_2")
    DB_NAMES      = ("test_vtable_ext_1", "test_vtable_ext_2")
    SCHEMA_NAME   = "public"

    column_types       = VTABLE_COLUMN_TYPES_PG
    mixed_column_types = _build_mixed_column_types(VTABLE_COLUMN_TYPES_PG)

    # ----- connection params (delegated to ExtSrcEnv) ----------------------
    @property
    def host(self):    return ExtSrcEnv.PG_HOST
    @property
    def port(self):    return ExtSrcEnv.PG_PORT
    @property
    def user(self):    return ExtSrcEnv.PG_USER
    @property
    def password(self):return ExtSrcEnv.PG_PASS
    @property
    def version(self): return ExtSrcEnv.PG_VERSIONS[0]

    # ----- env lifecycle ---------------------------------------------------
    def ensure_running(self):
        if ExtSrcEnv._tcp_probe(self.host, self.port, timeout=5):
            return
        if os.environ.get("FQ_SKIP_ENV_RESET") == "1":
            raise RuntimeError(
                f"FQ_SKIP_ENV_RESET=1 but PostgreSQL {self.version} not "
                f"reachable at {self.host}:{self.port}")
        tdLog.info(f"Starting PG {self.version} via ExtSrcEnv.start_pg_instance ...")
        ExtSrcEnv.start_pg_instance(self.version)
        if not ExtSrcEnv._tcp_probe(self.host, self.port, timeout=10):
            raise RuntimeError(
                f"PostgreSQL {self.version} not reachable at "
                f"{self.host}:{self.port}")

    # ----- ref builders (idx selects which of the two same-type sources) --
    def ref(self, table, col, idx):
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    def ref_short(self, table, col, idx):
        # PostgreSQL external source references MUST include the database name even in
        # child-vtable (short) form.  A 3-component "source.table.col" is ambiguous:
        # TDengine cannot distinguish it from a local "db.table.col" and reports
        # "External source not found".  Use the same 4-component format as ref() —
        # "source.db.table.col" — which TDengine resolves correctly.
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    # ----- external-source DDL --------------------------------------------
    def create_external_source(self):
        ExtSrcEnv.ensure_qnode()
        for src, db in zip(self.SOURCE_NAMES, self.DB_NAMES):
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {src}")
            tdSql.execute(
                f"CREATE EXTERNAL SOURCE IF NOT EXISTS {src} TYPE='postgresql' "
                f"HOST='{self.host}' PORT={self.port} "
                f"USER='{self.user}' PASSWORD='{self.password}' "
                f"DATABASE={db} SCHEMA={self.SCHEMA_NAME}")

    # ----- origin-table preparation ---------------------------------------
    def _copy_csv(self, database, table, columns, csv_path):
        """Bulk-load a CSV via psycopg2's copy_expert (no subprocess needed)."""
        import psycopg2
        conn = psycopg2.connect(
            host=self.host, port=self.port,
            user=self.user, password=self.password, dbname=database)
        conn.autocommit = True
        try:
            with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as fp:
                cur.copy_expert(
                    f"COPY {table} ({columns}) FROM STDIN "
                    f"WITH (FORMAT csv, HEADER true)", fp)
        finally:
            conn.close()

    def prepare_origin_tables(self):
        """Build identical origin tables in BOTH external DBs."""
        tdLog.info(
            f"prepare PostgreSQL origin tables in {self.DB_NAMES[0]} & "
            f"{self.DB_NAMES[1]}.")
        for db in self.DB_NAMES:
            ExtSrcEnv.pg_drop_db(db)
            ExtSrcEnv.pg_create_db(db)

        cols_sql = ", ".join(PG_TABLE_COLUMNS)
        create_stmts = [f"CREATE TABLE vtb_org_tb_{i} ({cols_sql})"
                        for i in range(18)]
        for db in self.DB_NAMES:
            ExtSrcEnv.pg_exec(db, create_stmts)

        # Load CSVs (tables 0..8) via COPY into both DBs.
        for i in range(9):
            datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
            with open(datafile, "r", encoding="utf-8") as fp:
                copy_cols = ", ".join(fp.readline().strip().split(","))
            for db in self.DB_NAMES:
                self._copy_csv(db, f"vtb_org_tb_{i}", copy_cols, datafile)

        # Tables 9..17 = 0..8 shifted +3 days.
        insert_cols = ", ".join(PG_COPY_COLUMN_LIST)
        select_cols = ", ".join(PG_COPY_COLUMN_LIST[1:])
        offset_stmts = [
            f"INSERT INTO vtb_org_tb_{i} ({insert_cols}) "
            f"SELECT ts + INTERVAL '3 day', {select_cols} "
            f"FROM vtb_org_tb_{i-9}"
            for i in range(9, 18)]
        for db in self.DB_NAMES:
            ExtSrcEnv.pg_exec(db, offset_stmts)


# --- MySQL adapter ---------------------------------------------------------
class _MysqlAdapter:
    BACKEND      = "mysql"
    SOURCE_NAMES = ("mysql_test_1", "mysql_test_2")
    DB_NAMES     = ("test_vtable_ext_1", "test_vtable_ext_2")

    column_types       = VTABLE_COLUMN_TYPES_MYSQL
    mixed_column_types = _build_mixed_column_types(VTABLE_COLUMN_TYPES_MYSQL)

    @property
    def host(self):    return ExtSrcEnv.MYSQL_HOST
    @property
    def port(self):    return ExtSrcEnv.MYSQL_PORT
    @property
    def user(self):    return ExtSrcEnv.MYSQL_USER
    @property
    def password(self):return ExtSrcEnv.MYSQL_PASS
    @property
    def version(self): return ExtSrcEnv.MYSQL_VERSIONS[0]

    def ensure_running(self):
        if ExtSrcEnv._tcp_probe(self.host, self.port, timeout=5):
            return
        if os.environ.get("FQ_SKIP_ENV_RESET") == "1":
            raise RuntimeError(
                f"FQ_SKIP_ENV_RESET=1 but MySQL {self.version} not "
                f"reachable at {self.host}:{self.port}")
        tdLog.info(f"Starting MySQL {self.version} via ExtSrcEnv.start_mysql_instance ...")
        ExtSrcEnv.start_mysql_instance(self.version)
        if not ExtSrcEnv._tcp_probe(self.host, self.port, timeout=10):
            raise RuntimeError(
                f"MySQL {self.version} not reachable at {self.host}:{self.port}")

    def ref(self, table, col, idx):
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    def ref_short(self, table, col, idx):
        # MySQL external source references MUST include the database name even in
        # child-vtable (short) form. A 3-component "source.table.col" is ambiguous:
        # TDengine cannot distinguish it from a local "db.table.col" and reports
        # "External source not found". Use the same 4-component format as ref() —
        # "source.db.table.col" — which TDengine resolves correctly.
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    def create_external_source(self):
        ExtSrcEnv.ensure_qnode()
        for src, db in zip(self.SOURCE_NAMES, self.DB_NAMES):
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {src}")
            tdSql.execute(
                f"CREATE EXTERNAL SOURCE IF NOT EXISTS {src} TYPE='mysql' "
                f"HOST='{self.host}' PORT={self.port} "
                f"USER='{self.user}' PASSWORD='{self.password}' "
                f"DATABASE={db}")

    def _load_csv(self, database, table_name, csv_path):
        """Bulk-load a CSV into a MySQL table via batched executemany."""
        import csv
        col_list = list(PG_COPY_COLUMN_LIST)
        col_str = ", ".join(f"`{c}`" for c in col_list)
        placeholders = ", ".join(["%s"] * len(col_list))
        insert_sql = (f"INSERT INTO `{table_name}` ({col_str}) "
                      f"VALUES ({placeholders})")
        conn = ExtSrcEnv.mysql_open_connection(database=database)
        conn.autocommit(False)
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                with conn.cursor() as cur:
                    for row in reader:
                        values = []
                        for col in col_list:
                            v = row.get(col, "").strip()
                            values.append(None if v == "" or v.lower() == "null" else v)
                        batch.append(tuple(values))
                        if len(batch) >= 1000:
                            cur.executemany(insert_sql, batch)
                            batch = []
                    if batch:
                        cur.executemany(insert_sql, batch)
            conn.commit()
        finally:
            conn.close()

    def prepare_origin_tables(self):
        """Build identical origin tables in BOTH external DBs."""
        tdLog.info(
            f"prepare MySQL origin tables in {self.DB_NAMES[0]} & "
            f"{self.DB_NAMES[1]}.")
        cols_sql = ", ".join(MYSQL_TABLE_COLUMNS)
        create_stmts = [f"CREATE TABLE `vtb_org_tb_{i}` ({cols_sql})"
                        for i in range(18)]
        select_cols = ", ".join(PG_COPY_COLUMN_LIST[1:])
        offset_stmts = [
            f"INSERT INTO `vtb_org_tb_{i}` "
            f"SELECT ts + INTERVAL 3 DAY, {select_cols} "
            f"FROM `vtb_org_tb_{i-9}`"
            for i in range(9, 18)]

        for db in self.DB_NAMES:
            ExtSrcEnv.mysql_drop_db(db)
            ExtSrcEnv.mysql_create_db(db)
            ExtSrcEnv.mysql_exec(db, create_stmts)
            for i in range(9):
                datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
                self._load_csv(db, f"vtb_org_tb_{i}", datafile)
            ExtSrcEnv.mysql_exec(db, offset_stmts)


# --- InfluxDB adapter ------------------------------------------------------
class _InfluxAdapter:
    BACKEND      = "influx"
    SOURCE_NAMES = ("influx_test_1", "influx_test_2")
    DB_NAMES     = ("test_vtable_ext_1", "test_vtable_ext_2")
    SNAPSHOT_WAIT_S = 30  # WAL→parquet snapshot wait so FlightSQL sees data

    column_types       = VTABLE_COLUMN_TYPES_INFLUX
    mixed_column_types = _build_mixed_column_types(VTABLE_COLUMN_TYPES_INFLUX)

    # Series-mode shared measurement: in this mode every origin "table"
    # (vtb_org_tb_N) is written into ONE measurement, distinguished only by the
    # `org_tb` tag, and vtable columns reference it via a SERIES tagCond clause.
    SHARED_MEASUREMENT = "vtb_org_shared"

    def __init__(self):
        # series_mode is enabled only on the basic prepare_ext_source_vtables
        # ("influx") path; the mixed / all paths keep the separate-measurement
        # layout. `_series_registry` maps each SERIES alias to its source target.
        self.series_mode = False
        self._series_registry = {}  # alias -> (source, db, org_tb_table)

    def enable_series_mode(self):
        self.series_mode = True
        self._series_registry = {}

    @property
    def host(self):    return ExtSrcEnv.INFLUX_HOST
    @property
    def port(self):    return ExtSrcEnv.INFLUX_PORT
    @property
    def token(self):
        # Read the token from file each time: INFLUX_TOKEN is a stale class
        # variable set at import time (before ensure_ext_env.sh runs and writes
        # the real admin token).  _get_influx_token() reads the file on demand.
        return ExtSrcEnv._get_influx_token(self.version)
    @property
    def version(self): return ExtSrcEnv.INFLUX_VERSIONS[0]

    def ensure_running(self):
        if ExtSrcEnv._tcp_probe(self.host, self.port, timeout=5):
            return
        if os.environ.get("FQ_SKIP_ENV_RESET") == "1":
            raise RuntimeError(
                f"FQ_SKIP_ENV_RESET=1 but InfluxDB {self.version} not "
                f"reachable at {self.host}:{self.port}")
        tdLog.info(f"Starting InfluxDB {self.version} via "
                   f"ExtSrcEnv.start_influx_instance ...")
        ExtSrcEnv.start_influx_instance(self.version)
        if not ExtSrcEnv._tcp_probe(self.host, self.port, timeout=10):
            raise RuntimeError(
                f"InfluxDB {self.version} not reachable at "
                f"{self.host}:{self.port}")

    def _register_series(self, db_idx, table):
        """Register a SERIES alias for (db_idx, org_tb table) and return it."""
        n = table.rsplit('_', 1)[-1]
        alias = f"s{db_idx}_{n}"
        self._series_registry[alias] = (
            self.SOURCE_NAMES[db_idx], self.DB_NAMES[db_idx], table)
        return alias

    def ref(self, table, col, idx):
        if self.series_mode:
            return f"{self._register_series(idx % 2, table)}.{col}"
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    def ref_short(self, table, col, idx):
        if self.series_mode:
            return f"{self._register_series(idx % 2, table)}.{col}"
        # InfluxDB external source references MUST include the database name even in
        # child-vtable (short) form.  A 3-component "source.measurement.col" is
        # ambiguous: TDengine cannot distinguish it from a local "db.table.col" and
        # reports "External source not found".  Use the same 4-component format as
        # ref() — "source.db.measurement.col" — which TDengine resolves correctly.
        s = self.SOURCE_NAMES[idx % 2]
        d = self.DB_NAMES[idx % 2]
        return f"{s}.{d}.{table}.{col}"

    @staticmethod
    def _group_tags(org_tb):
        """Synthesized constant (groupid, location) tag values for an origin
        group. The CSV groupid/location are intentionally ignored so that every
        group — including data4-9 which omit those columns — carries an
        identical, deterministic tag set. Returns string values (InfluxDB tags
        are always strings)."""
        n = org_tb.rsplit('_', 1)[-1]
        return n, f"loc{n}"

    @classmethod
    def _build_field_list(cls, row):
        """Build the line-protocol field list for one CSV row: numeric columns
        as typed fields, string columns as quoted/escaped fields."""
        fields = []
        for col in INFLUX_FIELD_COLUMNS:
            v = row.get(col, "").strip().strip('"')
            if not v or v.lower() == "null":
                continue
            if col == "bool_col":
                fields.append(f"{col}={v.lower() in ('1', 'true')}")
            elif col in ("float_col", "double_col"):
                fields.append(f"{col}={float(v)}")
            elif col in INFLUX_STRING_FIELD_COLUMNS:
                esc = v.replace("\\", "\\\\").replace('"', '\\"')
                fields.append(f'{col}="{esc}"')
            else:
                fields.append(f"{col}={int(float(v))}i")
        return fields

    def series_clause(self, source_map):
        """Build the trailing SERIES clause for one vtable's source map.

        Collects the distinct aliases referenced by `source_map` (values look
        like `alias.col`) and emits one `SERIES alias AS src.db.measurement
        (org_tb='table', groupid='G', location='L')` declaration per alias. The
        tag cond names every tag of the shared measurement, as required by the
        SERIES exact-tag-match. Returns "" outside series mode.
        """
        if not self.series_mode:
            return ""
        seen = []
        for v in source_map.values():
            alias = v.split('.', 1)[0]
            if alias not in seen:
                seen.append(alias)
        parts = []
        for alias in seen:
            src, db, table = self._series_registry[alias]
            gid, loc = self._group_tags(table)
            parts.append(
                f"SERIES {alias} AS {src}.{db}.{self.SHARED_MEASUREMENT} "
                f"(org_tb='{table}', groupid='{gid}', location='{loc}')")
        return (" " + " ".join(parts)) if parts else ""

    def create_external_source(self):
        ExtSrcEnv.ensure_qnode()
        for src, db in zip(self.SOURCE_NAMES, self.DB_NAMES):
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {src}")
            tdSql.execute(
                f"CREATE EXTERNAL SOURCE IF NOT EXISTS {src} TYPE='influxdb' "
                f"HOST='{self.host}' PORT={self.port} "
                f"USER='u' PASSWORD='' DATABASE={db} "
                f"OPTIONS('api_token'='{self.token}','protocol'='flight_sql')")

    def _row_count(self, database, measurement):
        """Return the row count for a measurement, or 0 on any error."""
        try:
            data = ExtSrcEnv.influx_query_sql(
                database, f'SELECT count(*) FROM "{measurement}"')
            if data and isinstance(data, list) and "count(*)" in data[0]:
                return data[0]["count(*)"]
        except Exception:
            pass
        return 0

    def _row_count_series(self, database, org_tb):
        """Return the row count of one org_tb group in the shared measurement."""
        try:
            data = ExtSrcEnv.influx_query_sql(
                database,
                f'SELECT count(*) FROM "{self.SHARED_MEASUREMENT}" '
                f"WHERE org_tb='{org_tb}'")
            if data and isinstance(data, list) and "count(*)" in data[0]:
                return data[0]["count(*)"]
        except Exception:
            pass
        return 0

    @classmethod
    def _csv_to_lines(cls, measurement, csv_path, ts_offset_ms=0):
        """Convert a CSV file into a list of InfluxDB line-protocol records.

        Tags are the synthesized constant groupid/location for this group; all
        vtable data columns (numeric + string) are written as fields.
        """
        import csv
        gid, loc = cls._group_tags(measurement)
        tag_str = f",groupid={gid},location={loc}"
        lines = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_str = row["ts"].strip().strip('"')
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                # ExtSrcEnv.influx_write forces precision=ns, so emit ns.
                ts_ns = (int(ts_dt.timestamp() * 1000) + ts_offset_ms) * 1_000_000

                fields = cls._build_field_list(row)
                if not fields:
                    continue

                lines.append(f"{measurement}{tag_str} {','.join(fields)} {ts_ns}")
        return lines

    @classmethod
    def _csv_to_lines_series(cls, org_tb, csv_path, ts_offset_ms=0):
        """Series-mode line protocol: ONE shared measurement whose tags are
        `org_tb` (the group discriminator) plus the synthesized constant
        groupid/location. All vtable data columns are written as fields so a
        SERIES `(org_tb=…, groupid=…, location=…)` clause can pin the group.
        """
        import csv
        gid, loc = cls._group_tags(org_tb)
        tag_str = f",org_tb={org_tb},groupid={gid},location={loc}"
        lines = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_str = row["ts"].strip().strip('"')
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                ts_ns = (int(ts_dt.timestamp() * 1000) + ts_offset_ms) * 1_000_000

                fields = cls._build_field_list(row)
                if not fields:
                    continue

                lines.append(f"{cls.SHARED_MEASUREMENT}{tag_str} "
                             f"{','.join(fields)} {ts_ns}")
        return lines

    def _write_batched(self, database, lines, batch_size=5000):
        for start in range(0, len(lines), batch_size):
            ExtSrcEnv.influx_write(database, lines[start:start + batch_size])

    def prepare_origin_tables(self):
        """Write identical line-protocol data into BOTH external DBs."""
        if self.series_mode:
            return self._prepare_origin_tables_series()

        tdLog.info(
            f"prepare InfluxDB measurements in {self.DB_NAMES[0]} & "
            f"{self.DB_NAMES[1]}.")
        offset_ms = 3 * 24 * 3600 * 1000

        for db in self.DB_NAMES:
            # Idempotency: skip reload only if data already populated AND has
            # the expected 2020+ timestamps. Stale data (from prior runs that
            # used wrong precision) must be cleared because InfluxDB upserts
            # by timestamp and can't overwrite ts in a different range.
            if self._row_count(db, "vtb_org_tb_0") >= 1000 and \
                    self._data_looks_valid(db):
                tdLog.info(f"InfluxDB data already exists with valid ts in "
                           f"{db}/vtb_org_tb_0 — skipping reload.")
                continue

            tdLog.info(f"Dropping & recreating InfluxDB database {db} to "
                       f"ensure clean ts range.")
            ExtSrcEnv.influx_drop_db(db)
            ExtSrcEnv.influx_create_db(db)

            for i in range(9):
                datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
                self._write_batched(
                    db, self._csv_to_lines(f"vtb_org_tb_{i}", datafile))
            for i in range(9):
                datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
                self._write_batched(db, self._csv_to_lines(
                    f"vtb_org_tb_{i+9}", datafile, ts_offset_ms=offset_ms))

            count = self._row_count(db, "vtb_org_tb_0")
            if count < 1000:
                tdLog.info(f"WARNING: InfluxDB {db}/vtb_org_tb_0 has only "
                           f"{count} rows, expected 1000. Waiting more...")
                time.sleep(self.SNAPSHOT_WAIT_S)
                count = self._row_count(db, "vtb_org_tb_0")
                tdLog.info(f"After additional wait: {count} rows in {db}")

    def _prepare_origin_tables_series(self):
        """Series-mode origin data: the 18 origin groups are written into ONE
        shared measurement per DB, distinguished by the `org_tb` tag, so the
        vtables can pin each group via a SERIES tagCond (same data → same
        in/ans results, but exercising the tagCond-pushdown code path).
        """
        tdLog.info(
            f"prepare InfluxDB series-mode shared measurement "
            f"'{self.SHARED_MEASUREMENT}' in {self.DB_NAMES[0]} & "
            f"{self.DB_NAMES[1]}.")
        offset_ms = 3 * 24 * 3600 * 1000

        for db in self.DB_NAMES:
            if self._row_count_series(db, "vtb_org_tb_0") >= 1000 and \
                    self._data_looks_valid_series(db):
                tdLog.info(f"InfluxDB series data already exists with valid ts "
                           f"in {db}/{self.SHARED_MEASUREMENT} — skipping reload.")
                continue

            tdLog.info(f"Dropping & recreating InfluxDB database {db} to "
                       f"ensure clean ts range.")
            ExtSrcEnv.influx_drop_db(db)
            ExtSrcEnv.influx_create_db(db)

            for i in range(9):
                datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
                self._write_batched(
                    db, self._csv_to_lines_series(f"vtb_org_tb_{i}", datafile))
            for i in range(9):
                datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
                self._write_batched(db, self._csv_to_lines_series(
                    f"vtb_org_tb_{i+9}", datafile, ts_offset_ms=offset_ms))

            count = self._row_count_series(db, "vtb_org_tb_0")
            if count < 1000:
                tdLog.info(f"WARNING: InfluxDB {db}/{self.SHARED_MEASUREMENT} "
                           f"org_tb=vtb_org_tb_0 has only {count} rows, expected "
                           f"1000. Waiting more...")
                time.sleep(self.SNAPSHOT_WAIT_S)
                count = self._row_count_series(db, "vtb_org_tb_0")
                tdLog.info(f"After additional wait: {count} rows in {db}")

    @staticmethod
    def _data_looks_valid(db):
        """Probe a single row from vtb_org_tb_0 to confirm ts is in the
        expected 2020+ range (i.e., not from a precision-bug write)."""
        try:
            data = ExtSrcEnv.influx_query_sql(
                db, 'SELECT "time" FROM "vtb_org_tb_0" LIMIT 1')
            if data and isinstance(data, list):
                ts_str = str(data[0].get("time", ""))
                return ts_str.startswith("20")  # 2020+
        except Exception:
            pass
        return False

    @classmethod
    def _data_looks_valid_series(cls, db):
        """Series-mode ts sanity probe on the shared measurement."""
        try:
            data = ExtSrcEnv.influx_query_sql(
                db, f'SELECT "time" FROM "{cls.SHARED_MEASUREMENT}" '
                    f"WHERE org_tb='vtb_org_tb_0' LIMIT 1")
            if data and isinstance(data, list):
                ts_str = str(data[0].get("time", ""))
                return ts_str.startswith("20")  # 2020+
        except Exception:
            pass
        return False


# ============================================================================
# Section 6.  VtableQueryUtil  (orchestrator — public API)
# ============================================================================

VTABLE_VIRTUAL_REF_COLUMN_DEFS = [
    ("u_tinyint_col", "tinyint unsigned"),
    ("u_smallint_col", "smallint unsigned"),
    ("u_int_col", "int unsigned"),
    ("u_bigint_col", "bigint unsigned"),
    ("tinyint_col", "tinyint"),
    ("smallint_col", "smallint"),
    ("int_col", "int"),
    ("bigint_col", "bigint"),
    ("float_col", "float"),
    ("double_col", "double"),
    ("bool_col", "bool"),
    ("binary_16_col", "binary(16)"),
    ("binary_32_col", "binary(32)"),
    ("nchar_16_col", "nchar(16)"),
    ("nchar_32_col", "nchar(32)"),
]

VTABLE_VIRTUAL_REF_NORMAL_FULL_REFS = [
    ("u_tinyint_col", "vtb_org_normal_0", "u_tinyint_col"),
    ("u_smallint_col", "vtb_org_normal_1", "u_smallint_col"),
    ("u_int_col", "vtb_org_normal_2", "u_int_col"),
    ("u_bigint_col", "vtb_org_normal_0", "u_bigint_col"),
    ("tinyint_col", "vtb_org_normal_1", "tinyint_col"),
    ("smallint_col", "vtb_org_normal_2", "smallint_col"),
    ("int_col", "vtb_org_normal_0", "int_col"),
    ("bigint_col", "vtb_org_normal_1", "bigint_col"),
    ("float_col", "vtb_org_normal_2", "float_col"),
    ("double_col", "vtb_org_normal_0", "double_col"),
    ("bool_col", "vtb_org_normal_1", "bool_col"),
    ("binary_16_col", "vtb_org_normal_2", "binary_16_col"),
    ("binary_32_col", "vtb_org_normal_0", "binary_32_col"),
    ("nchar_16_col", "vtb_org_normal_1", "nchar_16_col"),
    ("nchar_32_col", "vtb_org_normal_2", "nchar_32_col"),
]

VTABLE_VIRTUAL_REF_NORMAL_HALF_REFS = [
    ("u_tinyint_col", "vtb_org_normal_0", "u_tinyint_col"),
    ("u_smallint_col", "vtb_org_normal_1", "u_smallint_col"),
    ("u_int_col", "vtb_org_normal_2", "u_int_col"),
    ("int_col", "vtb_org_normal_0", "int_col"),
    ("bigint_col", "vtb_org_normal_1", "bigint_col"),
    ("float_col", "vtb_org_normal_2", "float_col"),
    ("binary_32_col", "vtb_org_normal_0", "binary_32_col"),
    ("nchar_16_col", "vtb_org_normal_1", "nchar_16_col"),
    ("nchar_32_col", "vtb_org_normal_2", "nchar_32_col"),
]

VTABLE_VIRTUAL_REF_MODE2_CHILD_REFS = [
    (
        "vtb_virtual_ctb_full_0",
        "vtb_virtual_ref_ctb_full_0",
        [
            ("u_tinyint_col", "vtb_org_normal_0", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_normal_1", "u_smallint_col"),
            ("u_int_col", "vtb_org_normal_2", "u_int_col"),
            ("u_bigint_col", "vtb_org_normal_0", "u_bigint_col"),
            ("tinyint_col", "vtb_org_normal_1", "tinyint_col"),
            ("smallint_col", "vtb_org_normal_2", "smallint_col"),
            ("int_col", "vtb_org_normal_0", "int_col"),
            ("bigint_col", "vtb_org_normal_1", "bigint_col"),
            ("float_col", "vtb_org_normal_2", "float_col"),
            ("double_col", "vtb_org_normal_0", "double_col"),
            ("bool_col", "vtb_org_normal_1", "bool_col"),
            ("binary_16_col", "vtb_org_normal_2", "binary_16_col"),
            ("binary_32_col", "vtb_org_normal_0", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_1", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_normal_2", "nchar_32_col"),
        ],
        "0, false, 0, 0, 'full', 'child0'",
    ),
    (
        "vtb_virtual_ctb_full_1",
        "vtb_virtual_ref_ctb_full_1",
        [
            ("u_tinyint_col", "vtb_org_normal_3", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_normal_4", "u_smallint_col"),
            ("u_int_col", "vtb_org_normal_5", "u_int_col"),
            ("u_bigint_col", "vtb_org_normal_3", "u_bigint_col"),
            ("tinyint_col", "vtb_org_normal_4", "tinyint_col"),
            ("smallint_col", "vtb_org_normal_5", "smallint_col"),
            ("int_col", "vtb_org_normal_3", "int_col"),
            ("bigint_col", "vtb_org_normal_4", "bigint_col"),
            ("float_col", "vtb_org_normal_5", "float_col"),
            ("double_col", "vtb_org_normal_3", "double_col"),
            ("bool_col", "vtb_org_normal_4", "bool_col"),
            ("binary_16_col", "vtb_org_normal_5", "binary_16_col"),
            ("binary_32_col", "vtb_org_normal_3", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_4", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_normal_5", "nchar_32_col"),
        ],
        "0, false, 0, 0, 'full', 'child1'",
    ),
    (
        "vtb_virtual_ctb_full_2",
        "vtb_virtual_ref_ctb_full_2",
        [
            ("u_tinyint_col", "vtb_org_normal_6", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_normal_7", "u_smallint_col"),
            ("u_int_col", "vtb_org_normal_8", "u_int_col"),
            ("u_bigint_col", "vtb_org_normal_6", "u_bigint_col"),
            ("tinyint_col", "vtb_org_normal_7", "tinyint_col"),
            ("smallint_col", "vtb_org_normal_8", "smallint_col"),
            ("int_col", "vtb_org_normal_6", "int_col"),
            ("bigint_col", "vtb_org_normal_7", "bigint_col"),
            ("float_col", "vtb_org_normal_8", "float_col"),
            ("double_col", "vtb_org_normal_6", "double_col"),
            ("bool_col", "vtb_org_normal_7", "bool_col"),
            ("binary_16_col", "vtb_org_normal_8", "binary_16_col"),
            ("binary_32_col", "vtb_org_normal_6", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_7", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_normal_8", "nchar_32_col"),
        ],
        "0, false, 0, 0, 'full', 'child2'",
    ),
    (
        "vtb_virtual_ctb_half_full_0",
        "vtb_virtual_ref_ctb_half_0",
        [
            ("u_tinyint_col", "vtb_org_normal_9", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_normal_10", "u_smallint_col"),
            ("u_int_col", "vtb_org_normal_11", "u_int_col"),
            ("int_col", "vtb_org_normal_9", "int_col"),
            ("bigint_col", "vtb_org_normal_10", "bigint_col"),
            ("float_col", "vtb_org_normal_11", "float_col"),
            ("binary_32_col", "vtb_org_normal_9", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_10", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_normal_11", "nchar_32_col"),
        ],
        "1, false, 1, 1, 'half', 'child0'",
    ),
    (
        "vtb_virtual_ctb_half_full_1",
        "vtb_virtual_ref_ctb_half_1",
        [
            ("tinyint_col", "vtb_org_normal_12", "tinyint_col"),
            ("smallint_col", "vtb_org_normal_13", "smallint_col"),
            ("int_col", "vtb_org_normal_14", "int_col"),
            ("bigint_col", "vtb_org_normal_12", "bigint_col"),
            ("float_col", "vtb_org_normal_13", "float_col"),
            ("double_col", "vtb_org_normal_14", "double_col"),
            ("bool_col", "vtb_org_normal_12", "bool_col"),
            ("binary_16_col", "vtb_org_normal_13", "binary_16_col"),
            ("binary_32_col", "vtb_org_normal_14", "binary_32_col"),
        ],
        "1, false, 1, 1, 'half', 'child1'",
    ),
    (
        "vtb_virtual_ctb_half_full_2",
        "vtb_virtual_ref_ctb_half_2",
        [
            ("u_int_col", "vtb_org_normal_15", "u_int_col"),
            ("u_bigint_col", "vtb_org_normal_16", "u_bigint_col"),
            ("tinyint_col", "vtb_org_normal_17", "tinyint_col"),
            ("smallint_col", "vtb_org_normal_15", "smallint_col"),
            ("int_col", "vtb_org_normal_16", "int_col"),
            ("bigint_col", "vtb_org_normal_17", "bigint_col"),
            ("float_col", "vtb_org_normal_15", "float_col"),
            ("binary_32_col", "vtb_org_normal_16", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_17", "nchar_16_col"),
        ],
        "1, false, 1, 1, 'half', 'child2'",
    ),
]

VTABLE_VIRTUAL_REF_MODE2_EMPTY_CHILD_TAGS = [
    ("vtb_virtual_ctb_empty_0", "2, true, 2, 2, 'empty', 'child0'"),
    ("vtb_virtual_ctb_empty_1", "2, true, 2, 2, 'empty', 'child1'"),
    ("vtb_virtual_ctb_empty_2", "2, true, 2, 2, 'empty', 'child2'"),
]

VTABLE_VIRTUAL_REF_MODE1_CHILD_REFS = [
    (
        "vtb_virtual_ctb_full",
        "vtb_virtual_ref_ctb_full",
        VTABLE_VIRTUAL_REF_NORMAL_FULL_REFS,
        "0, false, 0, 0, 'child0', 'child0'",
    ),
    (
        "vtb_virtual_ctb_half_full",
        "vtb_virtual_ref_ctb_half_full",
        [
            ("u_tinyint_col", "vtb_org_normal_3", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_normal_4", "u_smallint_col"),
            ("u_int_col", "vtb_org_normal_5", "u_int_col"),
            ("int_col", "vtb_org_normal_3", "int_col"),
            ("bigint_col", "vtb_org_normal_4", "bigint_col"),
            ("float_col", "vtb_org_normal_5", "float_col"),
            ("binary_32_col", "vtb_org_normal_3", "binary_32_col"),
            ("nchar_16_col", "vtb_org_normal_4", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_normal_5", "nchar_32_col"),
        ],
        "1, false, 1, 1, 'child1', 'child1'",
    ),
    (
        "vtb_virtual_ctb_mix",
        "vtb_virtual_ref_ctb_mix",
        [
            ("u_tinyint_col", "vtb_org_child_6", "u_tinyint_col"),
            ("u_smallint_col", "vtb_org_child_7", "u_smallint_col"),
            ("u_int_col", "vtb_org_child_8", "u_int_col"),
            ("u_bigint_col", "vtb_org_child_6", "u_bigint_col"),
            ("tinyint_col", "vtb_org_child_7", "tinyint_col"),
            ("smallint_col", "vtb_org_child_8", "smallint_col"),
            ("int_col", "vtb_org_child_6", "int_col"),
            ("bigint_col", "vtb_org_child_7", "bigint_col"),
            ("float_col", "vtb_org_child_8", "float_col"),
            ("double_col", "vtb_org_child_6", "double_col"),
            ("bool_col", "vtb_org_child_7", "bool_col"),
            ("binary_16_col", "vtb_org_child_8", "binary_16_col"),
            ("binary_32_col", "vtb_org_child_6", "binary_32_col"),
            ("nchar_16_col", "vtb_org_child_7", "nchar_16_col"),
            ("nchar_32_col", "vtb_org_child_8", "nchar_32_col"),
        ],
        "3, false, 3, 3, 'child3', 'child3'",
    ),
]

VTABLE_VIRTUAL_REF_MODE1_EMPTY_CHILD_TAGS = [
    ("vtb_virtual_ctb_empty", "2, false, 2, 2, 'child2', 'child2'"),
]

VTABLE_VIRTUAL_REF_CROSS_ROOT_DB = "test_vtable_select"
VTABLE_VIRTUAL_REF_CROSS_RAW_DB = "test_vtable_select_virtual_ref_raw"
VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS = [
    "test_vtable_select_ref_1",
    "test_vtable_select_ref_2",
    "test_vtable_select_ref_3",
    "test_vtable_select_ref_4",
]

class VtableQueryUtil:
    """Public façade used by all 05-VirtualTables test files.

    Method signatures preserved from the previous monolithic implementation —
    do not rename or change parameters.
    """

    VTABLE_DB_NAME = "test_vtable_select"

    # Class-level constants kept for backward compatibility with any external
    # references (none in this repo today, but cheap insurance).
    VTABLE_COLUMN_TYPES        = VTABLE_COLUMN_TYPES
    VTABLE_COLUMN_TYPES_MYSQL  = VTABLE_COLUMN_TYPES_MYSQL
    VTABLE_COLUMN_TYPES_INFLUX = VTABLE_COLUMN_TYPES_INFLUX

    # ------------------------------------------------------------------
    # Ref builders for built-in (TDengine-local) sources
    # ------------------------------------------------------------------
    def _same_db_ref(self, table, col, idx=0):
        return f"{table}.{col}"

    def _internal_ref(self, table, col, idx=0):
        return f"{self.VTABLE_DB_NAME}.{table}.{col}"

    @staticmethod
    def _cross_db_ref(db_idx, table, col):
        return f"test_vtable_select_{db_idx}.{table}.{col}"

    # ------------------------------------------------------------------
    # Source-map factories
    # ------------------------------------------------------------------
    @staticmethod
    def _map(table_map, ref_builder):
        """Render {col: table} → {col: ref-string} via a position-aware ref
        builder. `ref_builder(table, col, idx)` decides which external source
        the column comes from by `idx % 2` (alternation between the two
        same-type external sources)."""
        return {col: ref_builder(table, col, idx)
                for idx, (col, table) in enumerate(table_map.items())}

    @staticmethod
    def _cross_db_map(db_table_map):
        """Render {col: (db_idx, table)} → {col: 'db.table.col'}."""
        return {col: VtableQueryUtil._cross_db_ref(db, table, col)
                for col, (db, table) in db_table_map.items()}

    def _mixed_map(self, table_map, external_ref_builder):
        """Build a mixed source map: each column goes to ext or internal
        based on _MIXED_COLUMN_BUCKETS (same rule used by
        _build_mixed_column_types so declared types match actual sources).
        For ext columns, alternation between ext_src1/ext_src2 is driven
        by a running ext-only index via the adapter's `ref` callable.
        """
        out = {}
        ext_idx = 0
        for col, table in table_map.items():
            if _MIXED_COLUMN_BUCKETS.get(col) == "internal":
                out[col] = self._internal_ref(table, col)
            else:
                out[col] = external_ref_builder(table, col, ext_idx)
                ext_idx += 1
        return out

    def _ext_virtual_ref_maps(self, table_map, external_ref_builder):
        """Build raw and direct maps for ext-source virtual-ref chains.

        raw_ref_map feeds the lower virtual chain. direct_ref_map feeds only
        the top intermediate layer, leaving omitted columns to reference the
        next virtual layer.
        """
        raw_ref_map = {}
        direct_ref_map = {}
        ext_idx = 0
        for col, table in table_map.items():
            bucket = _EXT_VIRTUAL_REF_COLUMN_BUCKETS.get(col)
            if bucket in ("internal", "virtual"):
                ref = self._internal_ref(table, col)
                raw_ref_map[col] = ref
                if bucket == "internal":
                    direct_ref_map[col] = ref
            else:
                ref = external_ref_builder(table, col, ext_idx)
                ext_idx += 1
                raw_ref_map[col] = ref
                if bucket == "ext":
                    direct_ref_map[col] = ref
        return raw_ref_map, direct_ref_map

    def _all_map(self, table_map, adapters_by_backend):
        """Build the 7-way source map (internal + 2*pg + 2*mysql + 2*influx).

        Each column's (backend, src_idx) is fixed by _ALL_COLUMN_BUCKETS, so
        the same column always reads from the same physical source across
        every vtable. Declared types come from VTABLE_COLUMN_TYPES_ALL.
        """
        out = {}
        for col, table in table_map.items():
            bucket = _ALL_COLUMN_BUCKETS.get(col)
            if bucket is None:
                continue
            backend, src_idx = bucket
            if backend == 'internal':
                out[col] = self._internal_ref(table, col)
            else:
                out[col] = adapters_by_backend[backend].ref_short(
                    table, col, src_idx)
        return out

    # ==================================================================
    # Same-database vtables
    # ==================================================================

    def prepare_same_db_vtables(self, mode=1, sma=False,
                                ref_mode="no_virtual_ref"):
        """Origin tables + virtual tables, everything inside `test_vtable_select`."""
        tdSql.execute("alter all dnodes 'debugflag 131';")

        if ref_mode == "virtual_ref":
            tdLog.info("prepare same-db virtual_ref tables.")
            tdSql.execute(f"drop database if exists {self.VTABLE_DB_NAME};")
            if sma:
                tdSql.execute(f"create database {self.VTABLE_DB_NAME} "
                              f"vgroups 2 minrows 10 maxrows 200 stt_trigger 1;")
            else:
                tdSql.execute(f"create database {self.VTABLE_DB_NAME} vgroups 2;")
            self._prepare_virtual_ref_source_tables(self.VTABLE_DB_NAME, sma=sma)
            self.prepare_same_db_virtual_normal_table_virtual_ref()
            if mode == 2:
                self.prepare_same_db_virtual_super_child_table_mode_2_virtual_ref()
            else:
                self.prepare_same_db_virtual_super_child_table_mode_1_virtual_ref()
            return

        tdLog.info("prepare origin tables.")
        _prepare_one_origin_database(self.VTABLE_DB_NAME, sma=sma,
                                     debug_select_database=True)

        self._prepare_same_db_virtual_ntb()
        if mode == 2:
            self._prepare_same_db_virtual_ctb_mode2()
        else:
            self._prepare_same_db_virtual_ctb_mode1()

    def clean_up_same_db_vtables(self):
        tdLog.info("clean up same db vtables.")
        tdSql.execute(f"drop database if exists {self.VTABLE_DB_NAME};")

    def _prepare_same_db_virtual_ntb(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual normal table.")
        ref = self._same_db_ref
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_full",      self._map(NTB_FULL_SOURCE_TABLES, ref)))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_half_full", self._map(NTB_HALF_SOURCE_TABLES, ref)))
        tdSql.execute(build_vtable_normal_sql("vtb_virtual_ntb_empty", {}))

    def _prepare_same_db_virtual_ctb_mode1(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual super table.")
        tdSql.execute(build_vstable_sql())

        tdLog.info("prepare virtual child table.")
        ref = self._same_db_ref
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_full",      self._map(NTB_FULL_SOURCE_TABLES, ref),
            "0, false, 0, 0, 'child0', 'child0'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_half_full", self._map(CTB_HALF_SOURCE_TABLES, ref),
            "1, false, 1, 1, 'child1', 'child1'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_empty",     {},
            "2, false, 2, 2, 'child2', 'child2'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_mix",       self._map(CTB_MIX_SOURCE_TABLES, ref),
            "3, false, 3, 3, 'child3', 'child3'"))

    def _prepare_same_db_virtual_ctb_mode2(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual super table.")
        tdSql.execute(build_vstable_sql())

        tdLog.info("prepare virtual child table.")
        ref = self._same_db_ref
        for i in range(3):
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_full_{i}",
                self._map(_make_full_source_map(i * 3), ref),
                f"0, false, 0, 0, 'full', 'child{i}'"))
        for i, half in enumerate(
                (MODE2_HALF_0_MAP, MODE2_HALF_1_MAP, MODE2_HALF_2_MAP)):
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_half_full_{i}", self._map(half, ref),
                f"1, false, 1, 1, 'half', 'child{i}'"))
        for i in range(3):
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_empty_{i}", {},
                f"2, true, 2, 2, 'empty', 'child{i}'"))

    # ==================================================================
    # Cross-database vtables
    # ==================================================================

    def prepare_cross_db_vtables(self, mode=1, sma=False,
                                 ref_mode="no_virtual_ref"):
        """Spread origin tables across 4 dbs; vtables live in a 5th db."""
        if ref_mode == "virtual_ref":
            self.prepare_cross_db_virtual_ref_vtables(mode=mode, sma=sma)
            return

        tdSql.execute("alter all dnodes 'debugflag 131';")
        tdLog.info("prepare org tables.")
        for i in range(4):
            _prepare_one_origin_database(f"test_vtable_select_{i}", sma=sma)

        tdSql.execute(f"drop database if exists {self.VTABLE_DB_NAME};")
        tdSql.execute(f"create database {self.VTABLE_DB_NAME} vgroups 2;")

        self._prepare_cross_db_virtual_ntb()
        if mode == 2:
            self._prepare_cross_db_virtual_ctb_mode2()
        else:
            self._prepare_cross_db_virtual_ctb_mode1()

    def clean_up_cross_db_vtables(self):
        tdLog.info("clean up cross db vtables.")
        tdSql.execute(f"drop database if exists {self.VTABLE_DB_NAME};")
        for i in range(4):
            tdSql.execute(f"drop database if exists test_vtable_select_{i};")
        tdSql.execute(f"drop database if exists {VTABLE_VIRTUAL_REF_CROSS_RAW_DB};")
        for db_name in VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS:
            tdSql.execute(f"drop database if exists {db_name};")

    def _prepare_cross_db_virtual_ntb(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual normal table.")
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_full",      self._cross_db_map(CROSS_DB_NTB_FULL_MODE1)))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_half_full", self._cross_db_map(CROSS_DB_NTB_HALF_MODE1)))
        tdSql.execute(build_vtable_normal_sql("vtb_virtual_ntb_empty", {}))

    def _prepare_cross_db_virtual_ctb_mode1(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual super table.")
        tdSql.execute(build_vstable_sql())

        tdLog.info("prepare virtual child table.")
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_full",      self._cross_db_map(CROSS_DB_CTB_FULL_MODE1),
            "0, false, 0, 0, 'child0', 'child0'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_half_full", self._cross_db_map(CROSS_DB_CTB_HALF_MODE1),
            "1, false, 1, 1, 'child1', 'child1'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_empty",     {},
            "2, false, 2, 2, 'child2', 'child2'"))
        tdSql.execute(build_vtable_child_sql(
            "vtb_virtual_ctb_mix",       self._cross_db_map(CROSS_DB_CTB_MIX_MODE1),
            "3, false, 3, 3, 'child3', 'child3'"))

    def _prepare_cross_db_virtual_ctb_mode2(self):
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdLog.info("prepare virtual super table.")
        tdSql.execute(build_vstable_sql())

        tdLog.info("prepare virtual child table.")
        for i in range(3):
            full_map = _with_db_cycle(_make_full_source_map(i * 3),
                                      _CROSS_DB_MODE2_CYCLE)
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_full_{i}", self._cross_db_map(full_map),
                f"0, false, 0, 0, 'full', 'child{i}'"))
        for i, half in enumerate(
                (MODE2_HALF_0_MAP, MODE2_HALF_1_MAP, MODE2_HALF_2_MAP)):
            half_map = _with_db_cycle(half, _CROSS_DB_MODE2_CYCLE)
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_half_full_{i}", self._cross_db_map(half_map),
                f"1, false, 1, 1, 'half', 'child{i}'"))
        for i in range(3):
            tdSql.execute(build_vtable_child_sql(
                f"vtb_virtual_ctb_empty_{i}", {},
                f"2, true, 2, 2, 'empty', 'child{i}'"))

    # ==================================================================
    # External-source vtables  (PG / MySQL / Influx)
    # ==================================================================

    def _prepare_ext_source_vtables(self, adapter, mode):
        """Shared body for prepare_ext_source_vtables_*.

        All vtable columns come from the external source, so we only need:
        empty TDengine DB to hold vtables → external origin tables → EXTERNAL
        SOURCE → virtual tables. No local TDengine origin tables.
        """
        adapter.ensure_running()
        # InfluxDB basic path: route every column through the shared measurement
        # via a SERIES tagCond so the federated-scan tagCond-pushdown code path
        # is exercised by the existing in/ans suite. Other backends are no-ops.
        if getattr(adapter, "BACKEND", "") == "influx":
            adapter.enable_series_mode()
        _create_vtable_database(self.VTABLE_DB_NAME)
        adapter.prepare_origin_tables()
        adapter.create_external_source()

        ct = adapter.column_types
        self._emit_virtual_tables(
            ntb_full_src=self._map(NTB_FULL_SOURCE_TABLES, adapter.ref),
            ntb_half_src=self._map(NTB_HALF_SOURCE_TABLES, adapter.ref),
            ctb_full_src=self._map(NTB_FULL_SOURCE_TABLES, adapter.ref_short),
            ctb_half_src=self._map(CTB_HALF_SOURCE_TABLES, adapter.ref_short),
            ctb_mix_src =self._map(CTB_MIX_SOURCE_TABLES,  adapter.ref_short),
            mode2_builder=lambda m: self._map(m, adapter.ref_short),
            column_types=ct, mode=mode,
            series_clause_builder=getattr(adapter, "series_clause", None))

    def _prepare_mixed_source_vtables(self, adapter, mode):
        """Shared body for prepare_mixed_source_vtables_*."""
        adapter.ensure_running()
        _prepare_one_origin_database(self.VTABLE_DB_NAME, debug_select_database=True)
        adapter.prepare_origin_tables()
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        adapter.create_external_source()

        ct = adapter.mixed_column_types
        self._emit_virtual_tables(
            ntb_full_src=self._mixed_map(NTB_FULL_SOURCE_TABLES, adapter.ref),
            ntb_half_src=self._mixed_map(NTB_HALF_SOURCE_TABLES, adapter.ref),
            ctb_full_src=self._mixed_map(NTB_FULL_SOURCE_TABLES, adapter.ref_short),
            ctb_half_src=self._mixed_map(CTB_HALF_SOURCE_TABLES, adapter.ref_short),
            ctb_mix_src =self._mixed_map(CTB_MIX_SOURCE_TABLES,  adapter.ref_short),
            mode2_builder=lambda m: self._mixed_map(m, adapter.ref_short),
            column_types=ct, mode=mode)

    def _prepare_all_ext_source_vtables(self, mode):
        """Shared body for prepare_all_ext_source_vtables.

        Builds a single TDengine database that hosts both internal origin
        tables AND virtual tables whose columns are split 7-ways across:
        internal + 2*PG + 2*MySQL + 2*Influx. All three external backends
        plus a 2-source-per-backend EXTERNAL SOURCE setup are created.
        """
        pg     = _PgAdapter()
        mysql  = _MysqlAdapter()
        influx = _InfluxAdapter()
        adapters = (pg, mysql, influx)
        adapters_by_backend = {'pg': pg, 'mysql': mysql, 'influx': influx}

        for a in adapters:
            a.ensure_running()

        _prepare_one_origin_database(self.VTABLE_DB_NAME, debug_select_database=True)
        for a in adapters:
            a.prepare_origin_tables()

        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        for a in adapters:
            a.create_external_source()

        ct = VTABLE_COLUMN_TYPES_ALL
        _map_all = lambda tm: self._all_map(tm, adapters_by_backend)
        self._emit_virtual_tables(
            ntb_full_src=_map_all(NTB_FULL_SOURCE_TABLES),
            ntb_half_src=_map_all(NTB_HALF_SOURCE_TABLES),
            ctb_full_src=_map_all(NTB_FULL_SOURCE_TABLES),
            ctb_half_src=_map_all(CTB_HALF_SOURCE_TABLES),
            ctb_mix_src =_map_all(CTB_MIX_SOURCE_TABLES),
            mode2_builder=_map_all,
            column_types=ct, mode=mode)

    def _prepare_ext_source_virtual_ref_vtables(self, adapter, mode):
        """Shared body for vtable → vtable → ext/internal source chains."""
        adapter.ensure_running()
        _prepare_one_origin_database(self.VTABLE_DB_NAME,
                                     debug_select_database=True)
        adapter.prepare_origin_tables()
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        adapter.create_external_source()

        column_types = _build_ext_virtual_ref_column_types(
            adapter.column_types)
        layer_dbs = [self.VTABLE_DB_NAME] * 4

        def chained_map(table_map, chain_prefix):
            raw_ref_map, direct_ref_map = self._ext_virtual_ref_maps(
                table_map, adapter.ref)
            return self._create_virtual_ref_chain(
                layer_dbs, chain_prefix, raw_ref_map,
                column_types=column_types,
                layer1_ref_map=direct_ref_map)

        tdLog.info("prepare virtual normal table through virtual references.")
        ntb_full_src = chained_map(
            NTB_FULL_SOURCE_TABLES, "vtb_ext_virtual_ref_ntb_full")
        ntb_half_src = chained_map(
            NTB_HALF_SOURCE_TABLES, "vtb_ext_virtual_ref_ntb_half_full")

        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_full", ntb_full_src, column_types))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_half_full", ntb_half_src, column_types))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_empty", {}, column_types))

        tdLog.info("prepare virtual super table.")
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdSql.execute(build_vstable_sql(column_types))

        tdLog.info("prepare virtual child table through virtual references.")
        if mode == 2:
            for i in range(3):
                full_map = chained_map(
                    _make_full_source_map(i * 3),
                    f"vtb_ext_virtual_ref_ctb_full_{i}")
                tdSql.execute(build_vtable_child_sql(
                    f"vtb_virtual_ctb_full_{i}", full_map,
                    f"0, false, 0, 0, 'full', 'child{i}'",
                    column_types))
            for i, half in enumerate(
                    (MODE2_HALF_0_MAP, MODE2_HALF_1_MAP, MODE2_HALF_2_MAP)):
                half_map = chained_map(
                    half, f"vtb_ext_virtual_ref_ctb_half_full_{i}")
                tdSql.execute(build_vtable_child_sql(
                    f"vtb_virtual_ctb_half_full_{i}", half_map,
                    f"1, false, 1, 1, 'half', 'child{i}'",
                    column_types))
            for i in range(3):
                tdSql.execute(
                    f"CREATE VTABLE `vtb_virtual_ctb_empty_{i}` "
                    f"USING `vtb_virtual_stb` "
                    f"TAGS (2, true, 2, 2, 'empty', 'child{i}')")
        else:
            ctb_full_src = chained_map(
                NTB_FULL_SOURCE_TABLES, "vtb_ext_virtual_ref_ctb_full")
            ctb_half_src = chained_map(
                CTB_HALF_SOURCE_TABLES, "vtb_ext_virtual_ref_ctb_half_full")
            ctb_mix_src = chained_map(
                CTB_MIX_SOURCE_TABLES, "vtb_ext_virtual_ref_ctb_mix")
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_full", ctb_full_src,
                "0, false, 0, 0, 'child0', 'child0'", column_types))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_half_full", ctb_half_src,
                "1, false, 1, 1, 'child1', 'child1'", column_types))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_empty", {},
                "2, false, 2, 2, 'child2', 'child2'", column_types))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_mix", ctb_mix_src,
                "3, false, 3, 3, 'child3', 'child3'", column_types))

    def _emit_virtual_tables(self, *, ntb_full_src, ntb_half_src,
                             ctb_full_src, ctb_half_src, ctb_mix_src,
                             mode2_builder, column_types, mode,
                             series_clause_builder=None):
        """Emit the standard normal + stable + child vtable set.

        @param series_clause_builder Optional callable mapping a source map to a
                                     trailing SERIES clause (InfluxDB series
                                     mode); no clause is appended when None.
        """
        scb = series_clause_builder or (lambda m: "")
        tdLog.info("prepare virtual normal table.")
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_full",      ntb_full_src, column_types,
            series_clause=scb(ntb_full_src)))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_half_full", ntb_half_src, column_types,
            series_clause=scb(ntb_half_src)))
        tdSql.execute(build_vtable_normal_sql(
            "vtb_virtual_ntb_empty",     {},           column_types))

        tdLog.info("prepare virtual super table.")
        tdSql.execute(f"use {self.VTABLE_DB_NAME};")
        tdSql.execute(build_vstable_sql(column_types))

        tdLog.info("prepare virtual child table.")
        if mode == 2:
            for i in range(3):
                full_map = mode2_builder(_make_full_source_map(i * 3))
                tdSql.execute(build_vtable_child_sql(
                    f"vtb_virtual_ctb_full_{i}", full_map,
                    f"0, false, 0, 0, 'full', 'child{i}'", column_types,
                    series_clause=scb(full_map)))
            for i, half in enumerate(
                    (MODE2_HALF_0_MAP, MODE2_HALF_1_MAP, MODE2_HALF_2_MAP)):
                half_map = mode2_builder(half)
                tdSql.execute(build_vtable_child_sql(
                    f"vtb_virtual_ctb_half_full_{i}", half_map,
                    f"1, false, 1, 1, 'half', 'child{i}'", column_types,
                    series_clause=scb(half_map)))
            for i in range(3):
                tdSql.execute(
                    f"CREATE VTABLE `vtb_virtual_ctb_empty_{i}` "
                    f"USING `vtb_virtual_stb` "
                    f"TAGS (2, true, 2, 2, 'empty', 'child{i}')")
        else:
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_full",      ctb_full_src,
                "0, false, 0, 0, 'child0', 'child0'", column_types,
                series_clause=scb(ctb_full_src)))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_half_full", ctb_half_src,
                "1, false, 1, 1, 'child1', 'child1'", column_types,
                series_clause=scb(ctb_half_src)))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_empty",     {},
                "2, false, 2, 2, 'child2', 'child2'", column_types))
            tdSql.execute(build_vtable_child_sql(
                "vtb_virtual_ctb_mix",       ctb_mix_src,
                "3, false, 3, 3, 'child3', 'child3'", column_types,
                series_clause=scb(ctb_mix_src)))

    # Public entry points (preserved signatures) ----------------------------

    _EXT_SOURCE_ADAPTERS = {
        "postgres": _PgAdapter,
        "mysql": _MysqlAdapter,
        "influx": _InfluxAdapter,
    }

    def prepare_ext_source_vtables(self, source, mode=1):
        adapter_cls = self._EXT_SOURCE_ADAPTERS.get(source)
        if adapter_cls is None:
            raise ValueError(f"Unknown source: {source}, expected one of {list(self._EXT_SOURCE_ADAPTERS)}")
        self._prepare_ext_source_vtables(adapter_cls(), mode)

    def prepare_ext_source_virtual_ref_vtables(self, source, mode=1):
        adapter_cls = self._EXT_SOURCE_ADAPTERS.get(source)
        if adapter_cls is None:
            raise ValueError(f"Unknown source: {source}, expected one of {list(self._EXT_SOURCE_ADAPTERS)}")
        self._prepare_ext_source_virtual_ref_vtables(adapter_cls(), mode)

    _MIXED_SOURCE_ADAPTERS = {
        "postgres": _PgAdapter,
        "mysql": _MysqlAdapter,
        "influx": _InfluxAdapter,
    }

    def prepare_mixed_source_vtables(self, source, mode=1):
        adapter_cls = self._MIXED_SOURCE_ADAPTERS.get(source)
        if adapter_cls is None:
            raise ValueError(f"Unknown source: {source}, expected one of {list(self._MIXED_SOURCE_ADAPTERS)}")
        self._prepare_mixed_source_vtables(adapter_cls(), mode)

    def prepare_all_ext_source_vtables(self, mode=1):
        """Build vtables whose columns are split across all four sources
        (internal TDengine + PG + MySQL + Influx), with two EXTERNAL SOURCE
        instances per external backend (7 distinct sources total).
        """
        self._prepare_all_ext_source_vtables(mode)

    # ==================================================================
    # Standalone test environment — ts subquery pushdown
    # ==================================================================

    def prepare_ts_subquery_pushdown_env(self):
        """Build the small fixed dataset used by the ts-subquery-pushdown test.

        Unrelated to the source-map / adapter machinery above — preserved
        verbatim from the original implementation.
        """
        tdLog.info("prepare origin tables for vtable ts pushdown test.")
        tdSql.execute("drop database if exists test_vtable_ts_pushdown_origin;")
        tdSql.execute("create database test_vtable_ts_pushdown_origin;")
        tdSql.execute("use test_vtable_ts_pushdown_origin;")

        base_ts = datetime(2020, 10, 10, 9, 59, 45)
        ntb_0_rows, ntb_1_rows, ctb_0_rows = [], [], []
        for i in range(35):
            ts = (base_ts + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
            v = i - 14
            ntb_0_rows.append(f"('{ts}', {v})")
            ntb_1_rows.append(f"('{ts}', {100 + v})")
            ctb_0_rows.append(f"('{ts}', {10 + v})")

        tdSql.execute("create table ntb_0(event_time timestamp, value_col int);")
        tdSql.execute("insert into ntb_0 values " + " ".join(ntb_0_rows) + ";")
        tdSql.execute("create table ntb_1(event_time timestamp, value_col int);")
        tdSql.execute("insert into ntb_1 values " + " ".join(ntb_1_rows) + ";")
        tdSql.execute("create stable stb_0(event_time timestamp, value_col int) "
                      "tags (group_id int);")
        tdSql.execute("create table ctb_0 using stb_0 tags (1);")
        tdSql.execute("insert into ctb_0 values " + " ".join(ctb_0_rows) + ";")

        tdSql.execute(
            "create table bound_t(ts timestamp, lower_ts timestamp, "
            "upper_ts timestamp, exact_ts timestamp, mid_ts timestamp);")
        tdSql.execute(
            "insert into bound_t values "
            "('2020-10-10 09:59:59', '2020-10-10 10:00:01', "
            "'2020-10-10 10:00:03', '2020-10-10 10:00:02', "
            "'2020-10-10 10:00:02');")

        tdSql.execute(
            "create table bound_filter_t(ts timestamp, group_id int, "
            "lower_ts timestamp, upper_ts timestamp, exact_ts timestamp);")
        tdSql.execute(
            "insert into bound_filter_t values "
            "('2020-10-10 09:59:58', 0, '2020-10-10 10:00:00', "
            "'2020-10-10 10:00:02', '2020-10-10 10:00:01') "
            "('2020-10-10 09:59:59', 1, '2020-10-10 10:00:01', "
            "'2020-10-10 10:00:03', '2020-10-10 10:00:02') "
            "('2020-10-10 10:00:00', 1, '2020-10-10 10:00:02', "
            "'2020-10-10 10:00:04', '2020-10-10 10:00:03');")

        tdLog.info("prepare virtual tables for vtable ts pushdown test.")
        tdSql.execute("drop database if exists test_vtable_ts_pushdown_vtb;")
        tdSql.execute("create database test_vtable_ts_pushdown_vtb;")
        tdSql.execute("use test_vtable_ts_pushdown_vtb;")

        tdSql.execute("create vtable ntb_0_vtb("
                      "ts timestamp, "
                      "value_col int from test_vtable_ts_pushdown_origin.ntb_0.value_col);")

        tdSql.execute("create vtable ntb_multi_vtb("
                      "ts timestamp, "
                      "left_value int from test_vtable_ts_pushdown_origin.ntb_0.value_col, "
                      "right_value int from test_vtable_ts_pushdown_origin.ntb_1.value_col);")

        tdSql.execute("create stable vstb_0(ts timestamp, value_col int) tags (group_id int) virtual 1;")
        tdSql.execute("create vtable ctb_0_vtb("
                      "value_col from test_vtable_ts_pushdown_origin.ctb_0.value_col) "
                      "using vstb_0 tags (1);")

    def _build_virtual_ref_map(self, db_name, ref_specs):
        ref_map = {}
        for col_name, table_name, src_col_name in ref_specs:
            ref_map[col_name] = f"{db_name}.{table_name}.{src_col_name}"
        return ref_map

    def _build_virtual_ref_normal_columns(
            self, ref_map, column_types=VTABLE_COLUMN_TYPES):
        columns = ["ts timestamp"]
        for col_name, col_type in column_types:
            if col_name in ref_map:
                columns.append(f"{col_name} {col_type} from {ref_map[col_name]}")
            else:
                columns.append(f"{col_name} {col_type}")
        return ", ".join(columns)

    def _build_virtual_ref_child_columns(
            self, ref_map, column_types=VTABLE_COLUMN_TYPES):
        columns = []
        for col_name, _ in column_types:
            if col_name in ref_map:
                columns.append(f"{col_name} from {ref_map[col_name]}")
        return ", ".join(columns)

    def _create_virtual_ref_normal_table(
            self, db_name, table_name, ref_map,
            column_types=VTABLE_COLUMN_TYPES):
        tdSql.execute(f"use {db_name};")
        columns = self._build_virtual_ref_normal_columns(
            ref_map, column_types)
        tdSql.execute(f"CREATE VTABLE `{table_name}` ({columns})")

    def _create_virtual_ref_child_table(
            self, db_name, table_name, ref_map, tags_sql,
            column_types=VTABLE_COLUMN_TYPES):
        tdSql.execute(f"use {db_name};")
        child_columns = self._build_virtual_ref_child_columns(
            ref_map, column_types)
        if child_columns:
            tdSql.execute(f"CREATE VTABLE `{table_name}` ({child_columns}) USING `vtb_virtual_stb` TAGS ({tags_sql})")
        else:
            tdSql.execute(f"CREATE VTABLE `{table_name}` USING `vtb_virtual_stb` TAGS ({tags_sql})")

    def _create_virtual_ref_chain(
            self, layer_dbs, chain_prefix, raw_ref_map,
            column_types=VTABLE_COLUMN_TYPES, layer1_ref_map=None):
        next_ref_map = dict(raw_ref_map)

        for level_idx in range(4, 0, -1):
            db_name = layer_dbs[level_idx - 1]
            table_name = f"{chain_prefix}_l{level_idx}"

            table_ref_map = next_ref_map
            if level_idx == 1 and layer1_ref_map is not None:
                table_ref_map = {
                    col_name: f"{layer_dbs[1]}.{chain_prefix}_l2.{col_name}"
                    for col_name in raw_ref_map
                }
                table_ref_map.update(layer1_ref_map)

            self._create_virtual_ref_normal_table(
                db_name, table_name, table_ref_map, column_types)
            next_ref_map = {col_name: f"{db_name}.{table_name}.{col_name}" for col_name in raw_ref_map}

        return next_ref_map

    def _create_virtual_ref_stable(
            self, db_name, column_types=VTABLE_COLUMN_TYPES):
        tdSql.execute(f"use {db_name};")
        tdSql.execute(build_vstable_sql(column_types))

    def _prepare_virtual_ref_normal_tables(
            self, root_db, layer_dbs, raw_db,
            column_types=VTABLE_COLUMN_TYPES):
        full_ref_map = self._build_virtual_ref_map(raw_db, VTABLE_VIRTUAL_REF_NORMAL_FULL_REFS)
        full_root_ref_map = self._create_virtual_ref_chain(
            layer_dbs, "vtb_virtual_ref_ntb_full", full_ref_map,
            column_types=column_types)
        self._create_virtual_ref_normal_table(
            root_db, "vtb_virtual_ntb_full", full_root_ref_map, column_types)

        half_ref_map = self._build_virtual_ref_map(raw_db, VTABLE_VIRTUAL_REF_NORMAL_HALF_REFS)
        half_root_ref_map = self._create_virtual_ref_chain(
            layer_dbs, "vtb_virtual_ref_ntb_half_full", half_ref_map,
            column_types=column_types)
        self._create_virtual_ref_normal_table(
            root_db, "vtb_virtual_ntb_half_full", half_root_ref_map,
            column_types)

        self._create_virtual_ref_normal_table(
            root_db, "vtb_virtual_ntb_empty", {}, column_types)

    def _prepare_virtual_ref_child_tables(
            self, root_db, layer_dbs, raw_db, child_ref_defs,
            empty_child_defs, column_types=VTABLE_COLUMN_TYPES):
        self._create_virtual_ref_stable(root_db, column_types)

        for table_name, chain_prefix, ref_specs, tags_sql in child_ref_defs:
            raw_ref_map = self._build_virtual_ref_map(raw_db, ref_specs)
            root_ref_map = self._create_virtual_ref_chain(
                layer_dbs, chain_prefix, raw_ref_map,
                column_types=column_types)
            self._create_virtual_ref_child_table(
                root_db, table_name, root_ref_map, tags_sql, column_types)

        for table_name, tags_sql in empty_child_defs:
            self._create_virtual_ref_child_table(
                root_db, table_name, {}, tags_sql, column_types)

    def _prepare_virtual_ref_source_tables(self, db_name, sma=False):
        tdSql.execute(f"use {db_name};")

        tdLog.info("prepare org super table for virtual_ref.")
        tdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32),"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))")

        tdLog.info("prepare org child table for virtual_ref.")
        for i in range(18):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

        tdLog.info("prepare org normal table for virtual_ref.")
        for i in range(18):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32)) SMA(u_tinyint_col, u_smallint_col, u_int_col, u_bigint_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, bool_col, binary_16_col, binary_32_col, nchar_16_col, nchar_32_col)")

        for i in range(9):
            datafile = etool.getFilePath(__file__, "data", f"data{i+1}.csv")
            tdSql.execute(f"insert into vtb_org_normal_{i} file" + "'%s';" % datafile)
            tdSql.execute(f"insert into vtb_org_child_{i} file" + "'%s';" % datafile)

        for i in range(9, 18):
            tdSql.execute(f"insert into vtb_org_normal_{i} select ts + 3d, u_tinyint_col, u_smallint_col, u_int_col, u_bigint_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, bool_col, binary_16_col, binary_32_col, nchar_16_col, nchar_32_col from vtb_org_normal_{i-9};")
            tdSql.execute(f"insert into vtb_org_child_{i} select ts + 3d, u_tinyint_col, u_smallint_col, u_int_col, u_bigint_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, bool_col, binary_16_col, binary_32_col, nchar_16_col, nchar_32_col from vtb_org_child_{i-9};")

        tdSql.execute(f"flush database {db_name};")

    def prepare_same_db_virtual_normal_table_virtual_ref(self):
        same_db_layers = ["test_vtable_select"] * 4
        self._prepare_virtual_ref_normal_tables("test_vtable_select", same_db_layers, "test_vtable_select")

    def prepare_same_db_virtual_super_child_table_mode_1_virtual_ref(self):
        same_db_layers = ["test_vtable_select"] * 4
        self._prepare_virtual_ref_child_tables("test_vtable_select", same_db_layers, "test_vtable_select",
                                               VTABLE_VIRTUAL_REF_MODE1_CHILD_REFS,
                                               VTABLE_VIRTUAL_REF_MODE1_EMPTY_CHILD_TAGS)

    def prepare_same_db_virtual_super_child_table_mode_2_virtual_ref(self):
        same_db_layers = ["test_vtable_select"] * 4
        self._prepare_virtual_ref_child_tables("test_vtable_select", same_db_layers, "test_vtable_select",
                                               VTABLE_VIRTUAL_REF_MODE2_CHILD_REFS,
                                               VTABLE_VIRTUAL_REF_MODE2_EMPTY_CHILD_TAGS)

    def prepare_cross_db_virtual_ref_vtables(self, mode=1, sma=False):
        tdSql.execute(f"alter all dnodes 'debugflag 131';")
        tdLog.info("prepare cross db virtual_ref tables.")

        self.clean_up_cross_db_vtables()
        tdSql.execute(f"drop database if exists {VTABLE_VIRTUAL_REF_CROSS_RAW_DB};")
        for db_name in VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS:
            tdSql.execute(f"drop database if exists {db_name};")

        if sma:
            tdSql.execute(f"create database {VTABLE_VIRTUAL_REF_CROSS_RAW_DB} vgroups 2 minrows 10 maxrows 200 stt_trigger 1;")
        else:
            tdSql.execute(f"create database {VTABLE_VIRTUAL_REF_CROSS_RAW_DB} vgroups 2;")
        self._prepare_virtual_ref_source_tables(VTABLE_VIRTUAL_REF_CROSS_RAW_DB, sma)

        for db_name in VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS:
            tdSql.execute(f"create database {db_name} vgroups 2;")

        tdSql.execute(f"create database {VTABLE_VIRTUAL_REF_CROSS_ROOT_DB} vgroups 2;")
        self._prepare_virtual_ref_normal_tables(VTABLE_VIRTUAL_REF_CROSS_ROOT_DB,
                                                VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS,
                                                VTABLE_VIRTUAL_REF_CROSS_RAW_DB)

        if mode == 2:
            self._prepare_virtual_ref_child_tables(VTABLE_VIRTUAL_REF_CROSS_ROOT_DB,
                                                   VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS,
                                                   VTABLE_VIRTUAL_REF_CROSS_RAW_DB,
                                                   VTABLE_VIRTUAL_REF_MODE2_CHILD_REFS,
                                                   VTABLE_VIRTUAL_REF_MODE2_EMPTY_CHILD_TAGS)
        else:
            self._prepare_virtual_ref_child_tables(VTABLE_VIRTUAL_REF_CROSS_ROOT_DB,
                                                   VTABLE_VIRTUAL_REF_CROSS_LAYER_DBS,
                                                   VTABLE_VIRTUAL_REF_CROSS_RAW_DB,
                                                   VTABLE_VIRTUAL_REF_MODE1_CHILD_REFS,
                                                   VTABLE_VIRTUAL_REF_MODE1_EMPTY_CHILD_TAGS)
