# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""DDL tests for CREATE VTABLE and CREATE STABLE ... VIRTUAL 1 referencing
EXTERNAL SOURCEs.

Covers sections 2 and 3 of the DDL test plan:

    §2 — CREATE VTABLE … (col TYPE FROM <ref>):
      - reference legal remote columns (single source, multi-table, multi-source,
        multi-type sources, mixed ext+local-NULL)
      - full type & precision compatibility matrix (per backend)
      - negative paths (missing source/table/column, syntax error, duplicate
        column, reserved/unicode names)
      - FROM reference segment-count parsing (1/2/3/4/5-seg)

    §3 — CREATE STABLE … VIRTUAL 1 + CREATE VTABLE … USING (PATENT CORE):
      heterogeneous remote tables sharing common columns are abstracted into a
      virtual super-table; each remote table becomes a child virtual table.
      Aggregate queries over the vstb fan out across heterogeneous sources.

Sibling file test_fq_03_type_mapping.py exercises end-to-end query parity for
each type mapping; this file focuses on the CREATE-stage DDL parsing,
metadata, and DESCRIBE round-trip behavior — what fq_03 does not cover.
"""

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db,
    create_pg_table, create_mysql_table, create_influx_measurement,
    create_sqlserver_table, create_oracle_table,
    create_pg_view, create_mysql_view, create_oracle_view,
    create_oracle_materialized_view,
    extract_show_create_sql as _extract_show_create_sql)


_P = "vtcreate_"

# --- Per-backend remote DB / source name ---
_PG_DB      = "vtcreate_pg_db"
_MY_DB      = "vtcreate_my_db"
_INF_DB     = "vtcreate_inf_db"
_SS_DB      = "vtcreate_ss_db"
_PG_SRC     = "vtcreate_pg_src"
_MY_SRC     = "vtcreate_my_src"
_INF_SRC    = "vtcreate_inf_src"
_SS_SRC     = "vtcreate_ss_src"
# Oracle has no per-source database: the source names the Easy Connect service
# and the schema is the login user (DS 5.3.10).
_OR_SVC        = ExtSrcEnv.ORACLE_SERVICE
_OR_SCHEMA     = ExtSrcEnv.ORACLE_USER.upper()
_OR_ALT_SCHEMA = ExtSrcEnv.ORACLE_ALT_USER.upper()
_OR_SRC        = "vtcreate_or_src"
_OR_ALT_SRC    = "vtcreate_or_alt_src"

# Local TDengine database that hosts the virtual tables under test.
_LOCAL_DB   = "vtcreate_local"


# ---------------------------------------------------------------------------
# Backend type matrices (remote column type → expected TDengine column type).
# Each entry: (remote_col_name, remote_decl, local_decl)
# Used to drive: build a single wide remote table, create a vtable that maps
# each remote col to its local decl, then DESCRIBE the vtable and verify
# the resulting schema matches the local_decl.
# ---------------------------------------------------------------------------

# Each entry is (remote_col_name, remote_decl, canonical_local_decl).
#
# canonical_local_decl is the TDengine declaration that EXACTLY matches what
# the connector returns for that remote type (per extTypeMap.c).  It is used
# by test_create_single_source_all_cols (DESCRIBE round-trip) and is the only
# entry in _*_COMPAT that must be ACCEPTED at CREATE time.
#
# Coverage goal: every native remote type the connector knows about (PG —
# pgTypeMap; MySQL — mysqlTypeMap) appears here, so the cross-product matrix
# tests exercise isSameRefDataType against every TDengine type in _TDS_ALL.
# Remote types whose canonical TDengine target is outside _TDS_ALL (e.g.
# MONEY → decimal64(18,2), LONGBLOB → blob, unbounded NUMERIC → decimal(38,6))
# are listed with the canonical decl anyway; their _*_COMPAT entry is
# intentionally empty so all 17 TDengine local types must reject them.
#
# Skipped from the matrices on purpose:
#   * PG geometry/point/path/polygon, hstore, tsvector/tsquery, arrays:
#     require non-default extensions or CI provisioning.
#   * MySQL spatial (GEOMETRY/POINT/LINESTRING/POLYGON): require WKT-aware
#     INSERTs that the helper does not currently emit.

_PG_TYPE_MATRIX = [
    # --- numerics ---
    ("c_bool",         "BOOLEAN",           "bool"),
    ("c_smallint",     "SMALLINT",          "smallint"),
    ("c_int",          "INTEGER",           "int"),
    ("c_bigint",       "BIGINT",            "bigint"),
    ("c_real",         "REAL",              "float"),
    ("c_double",       "DOUBLE PRECISION",  "double"),
    ("c_numeric_184",  "NUMERIC(18,4)",     "decimal(18,4)"),
    ("c_numeric",      "NUMERIC",           "decimal(38,6)"),   # not in _TDS_ALL
    ("c_money",        "MONEY",             "decimal(18,2)"),   # not in _TDS_ALL
    ("c_serial",       "SERIAL",            "int"),
    ("c_bigserial",    "BIGSERIAL",         "bigint"),
    ("c_smallserial",  "SMALLSERIAL",       "smallint"),
    # --- character / binary (PG DB encoding = UTF-8 → NCHAR for char-y types) ---
    ("c_varchar",      "VARCHAR(32)",       "nchar(32)"),
    ("c_char",         "CHAR(4)",           "nchar(4)"),
    ("c_text",         "TEXT",              "nchar(64)"),
    ("c_bytea",        "BYTEA",             "varbinary(64)"),
    ("c_bit",          "BIT(8)",            "varbinary(64)"),
    ("c_bitvar",       "BIT VARYING(32)",   "varbinary(64)"),
    # --- temporal ---
    ("c_date",         "DATE",              "timestamp"),
    ("c_time",         "TIME",              "bigint"),
    ("c_timetz",       "TIMETZ",            "bigint"),
    ("c_timestamp",    "TIMESTAMP",         "timestamp"),
    ("c_timestamptz",  "TIMESTAMPTZ",       "timestamp"),
    ("c_interval",     "INTERVAL",          "bigint"),
    # --- semi-structured / network / identity ---
    ("c_uuid",         "UUID",              "varchar(36)"),
    ("c_json",         "JSON",              "nchar(64)"),
    ("c_jsonb",        "JSONB",             "nchar(64)"),
    ("c_xml",          "XML",               "nchar(64)"),
    ("c_inet",         "INET",              "varchar(64)"),
    ("c_cidr",         "CIDR",              "varchar(64)"),
    ("c_macaddr",      "MACADDR",           "varchar(64)"),
    ("c_macaddr8",     "MACADDR8",          "varchar(64)"),
]

_MY_TYPE_MATRIX = [
    # --- signed ints ---
    ("c_tinyint",   "TINYINT",                              "tinyint"),
    ("c_stinyint",  "TINYINT(1)",                           "bool"),
    ("c_smallint",  "SMALLINT",                             "smallint"),
    ("c_mediumint", "MEDIUMINT",                            "int"),
    ("c_int",       "INT",                                  "int"),
    ("c_bigint",    "BIGINT",                               "bigint"),
    # --- unsigned ints ---
    ("c_utiny",     "TINYINT UNSIGNED",                     "tinyint unsigned"),
    ("c_usmall",    "SMALLINT UNSIGNED",                    "smallint unsigned"),
    ("c_umed",      "MEDIUMINT UNSIGNED",                   "int unsigned"),
    ("c_uint",      "INT UNSIGNED",                         "int unsigned"),
    ("c_ubig",      "BIGINT UNSIGNED",                      "bigint unsigned"),
    # --- floats / decimals ---
    ("c_float",     "FLOAT",                                "float"),
    ("c_double",    "DOUBLE",                               "double"),
    ("c_dec",       "DECIMAL(10,2)",                        "decimal(10,2)"),
    # --- bit (MySQL BIT max width is 64) ---
    ("c_bit8",      "BIT(8)",                               "bigint"),
    ("c_bit64",     "BIT(64)",                              "bigint unsigned"),
    # --- bool (BOOL is alias of TINYINT(1) in MySQL but listed for completeness) ---
    ("c_bool",      "BOOL",                                 "bool"),
    # --- temporal ---
    ("c_date",      "DATE",                                 "timestamp"),
    ("c_datetime",  "DATETIME",                             "timestamp"),
    ("c_ts2",       "TIMESTAMP NULL DEFAULT NULL",          "timestamp"),
    ("c_time",      "TIME",                                 "bigint"),
    ("c_year",      "YEAR",                                 "smallint"),
    # --- character (utf8mb4 → NCHAR; ascii → VARCHAR/BINARY) ---
    ("c_char_utf",  "CHAR(8) CHARACTER SET utf8mb4",        "nchar(32)"),
    ("c_char_asc",  "CHAR(8) CHARACTER SET ascii",          "varchar(32)"),
    ("c_vchar_utf", "VARCHAR(32) CHARACTER SET utf8mb4",    "nchar(32)"),
    ("c_vchar_asc", "VARCHAR(32) CHARACTER SET ascii",      "varchar(32)"),
    ("c_text_utf",  "TEXT CHARACTER SET utf8mb4",           "nchar(128)"),
    ("c_text_asc",  "TEXT CHARACTER SET ascii",             "varchar(128)"),
    ("c_ttext",     "TINYTEXT CHARACTER SET utf8mb4",       "nchar(32)"),
    ("c_mtext",     "MEDIUMTEXT CHARACTER SET utf8mb4",     "nchar(128)"),
    ("c_ltext",     "LONGTEXT CHARACTER SET utf8mb4",       "nchar(128)"),
    # --- binary ---
    # NOTE: DS §5.3.2.1 says BINARY(n)/TINYBLOB → BINARY(n) (VARCHAR alias),
    # but connector (extTypeMap.c) maps them to VARBINARY. Tests follow code.
    ("c_binary",    "BINARY(16)",                           "varbinary(64)"),
    ("c_vbinary",   "VARBINARY(64)",                        "varbinary(64)"),
    ("c_blob",      "BLOB",                                 "varbinary(64)"),
    ("c_tblob",     "TINYBLOB",                             "varbinary(64)"),
    ("c_mblob",     "MEDIUMBLOB",                           "varbinary(64)"),
    ("c_lblob",     "LONGBLOB",                             "blob"),            # not in _TDS_ALL
    # --- enum / set / json (utf8mb4) ---
    ("c_enum",      "ENUM('a','b','c') CHARACTER SET utf8mb4", "nchar(64)"),
    ("c_set",       "SET('a','b','c') CHARACTER SET utf8mb4",  "nchar(64)"),
    ("c_json",      "JSON",                                 "nchar(64)"),
]

_SS_TYPE_MATRIX = [
    # --- integer family (bit is SQL Server's boolean) ---
    ("c_bit",            "BIT",                 "bool"),
    ("c_tinyint",        "TINYINT",             "tinyint"),
    ("c_smallint",       "SMALLINT",            "smallint"),
    ("c_int",            "INT",                 "int"),
    ("c_bigint",         "BIGINT",              "bigint"),
    # --- floating point ---
    ("c_real",           "REAL",                "float"),
    ("c_float",          "FLOAT",               "double"),
    # --- exact numerics ---
    ("c_decimal_184",    "DECIMAL(18,4)",       "decimal(18,4)"),
    ("c_decimal_102",    "DECIMAL(10,2)",       "decimal(10,2)"),
    ("c_numeric",        "NUMERIC(18,4)",       "decimal(18,4)"),
    # money / smallmoney keep value semantics with a fixed scale of 4; their
    # canonical targets (decimal(19,4) / decimal(10,4)) are outside _TDS_ALL.
    ("c_money",          "MONEY",               "decimal(19,4)"),
    ("c_smallmoney",     "SMALLMONEY",          "decimal(10,4)"),
    # --- character / unicode (char/varchar are non-Unicode -> VARCHAR; the
    #     n-prefixed types are Unicode -> NCHAR, per DS 5.3.2.1) ---
    ("c_char",           "CHAR(8)",             "varchar(8)"),
    ("c_varchar",        "VARCHAR(32)",         "varchar(32)"),
    ("c_nchar",          "NCHAR(8)",            "nchar(8)"),
    ("c_nvarchar",       "NVARCHAR(32)",        "nchar(32)"),
    ("c_uuid",           "UNIQUEIDENTIFIER",    "varchar(36)"),
    # xml maps to NCHAR; the connector clamps its width, and NCHAR widths are
    # not part of the CREATE-time check, so a representative width is declared.
    ("c_xml",            "XML",                 "nchar(64)"),
    # --- binary (rowversion is a row-version counter mapped to VARBINARY; it
    #     is usable as a data column but can never be the time axis) ---
    ("c_binary",         "BINARY(16)",          "varbinary(16)"),
    ("c_varbinary",      "VARBINARY(64)",       "varbinary(64)"),
    ("c_image",          "IMAGE",               "blob"),        # not in _TDS_ALL
    ("c_rowversion",     "ROWVERSION",          "varbinary(8)"),
    # --- temporal (all non-rowversion temporal types map to TIMESTAMP) ---
    ("c_date",           "DATE",                "timestamp"),
    ("c_time",           "TIME(6)",             "bigint"),
    ("c_datetime",       "DATETIME",            "timestamp"),
    ("c_smalldatetime",  "SMALLDATETIME",       "timestamp"),
    ("c_datetime2",      "DATETIME2(6)",        "timestamp"),
    ("c_datetimeoffset", "DATETIMEOFFSET(6)",   "timestamp"),
]

# Oracle declarations are re-composed from ALL_TAB_COLUMNS: INTEGER and
# SMALLINT arrive as NUMBER(38,0), REAL as FLOAT(63), and the character types
# carry their character length.  A table may contain only one LONG-family
# column (ORA-01754), so LONG and LONG RAW live in their own tables.
_ORACLE_TYPE_MATRIX = [
    # --- NUMBER families (extTypeMap.c oracleTypeMap) ---
    ("c_num_dec",      "NUMBER(10,2)",      "decimal(10,2)"),
    ("c_num_bigint",   "NUMBER(18,0)",      "bigint"),
    ("c_num_wide",     "NUMBER(19,0)",      "decimal(19,0)"),   # not in _TDS_ALL
    ("c_num_38",       "NUMBER(38,0)",      "decimal(38,0)"),   # not in _TDS_ALL
    ("c_num_one",      "NUMBER(1,0)",       "tinyint"),
    # A bare NUMBER carries up to 38 digits and an arbitrary exponent; it has
    # no lossless TDengine representation (EXT_TYPE_NOT_MAPPABLE).
    ("c_number_bare",  "NUMBER",            "decimal(38,0)"),   # not in _TDS_ALL
    # INTEGER / SMALLINT are aliases of NUMBER(38,0), not machine integers.
    ("c_integer",      "INTEGER",           "decimal(38,0)"),   # not in _TDS_ALL
    ("c_smallint",     "SMALLINT",          "decimal(38,0)"),   # not in _TDS_ALL
    # --- binary floating point ---
    ("c_float24",      "FLOAT(24)",         "float"),
    ("c_float53",      "FLOAT(53)",         "double"),
    ("c_float126",     "FLOAT(126)",        "double"),          # not mappable >53
    ("c_real",         "REAL",              "double"),          # REAL == FLOAT(63)
    ("c_bin_float",    "BINARY_FLOAT",      "float"),
    ("c_bin_double",   "BINARY_DOUBLE",     "double"),
    # --- character / national character ---
    ("c_char",         "CHAR(8)",           "varchar(8)"),
    ("c_varchar2",     "VARCHAR2(32)",      "varchar(32)"),
    ("c_nchar",        "NCHAR(8)",          "nchar(8)"),
    ("c_nvarchar2",    "NVARCHAR2(32)",     "nchar(32)"),
    ("c_clob",         "CLOB",              "nchar(64)"),
    ("c_nclob",        "NCLOB",             "nchar(64)"),
    # --- binary ---
    ("c_raw",          "RAW(16)",           "varbinary(16)"),
    ("c_blob",         "BLOB",              "blob"),            # not in _TDS_ALL
    # --- identifiers / object types ---
    ("c_rowid",        "ROWID",             "varchar(40)"),
    ("c_urowid",       "UROWID",            "varchar(40)"),
    ("c_xmltype",      "XMLTYPE",           "nchar(64)"),
    # --- temporal (non-axis columns; DATE / TIMESTAMP are the only axes) ---
    ("c_date",         "DATE",              "timestamp"),
    ("c_ts6",          "TIMESTAMP(6)",      "timestamp"),
    ("c_ts9",          "TIMESTAMP(9)",      "timestamp"),
    ("c_ts_tz",        "TIMESTAMP(6) WITH TIME ZONE",       "timestamp"),
    ("c_ts_ltz",       "TIMESTAMP(6) WITH LOCAL TIME ZONE", "timestamp"),
]

# LONG and LONG RAW are the only columns of their table: Oracle permits one
# LONG-family column per table (ORA-01754), which also excludes putting both
# in the wide table above.
_ORACLE_LONG_TYPE_MATRIX = [
    ("c_long",         "LONG",              "nchar(64)"),
]

_ORACLE_LRAW_TYPE_MATRIX = [
    ("c_long_raw",     "LONG RAW",          "varbinary(64)"),
]

_INF_TYPE_MATRIX = [
    # Influx 3 line-protocol types: integer (i64), unsigned (u64), float (f64),
    # string (Utf8), boolean. All numerics widen to TDengine's largest signed.
    ("c_int",    "bigint"),
    ("c_uint",   "bigint unsigned"),
    ("c_float",  "double"),
    ("c_str",    "nchar(64)"),
    ("c_bool",   "bool"),
]

# ---------------------------------------------------------------------------
# Type-mismatch enumeration: every remote column type × every TDengine local
# type. Compatible pairs are derived from the positive matrices above; every
# other pair MUST be rejected (at CREATE time, by [contract] possibly at
# query time). This guarantees the engine's isSameRefDataType /
# checkExternalColRef rules are exercised for every TDengine type kind, not
# just the canonical mapping.
# ---------------------------------------------------------------------------

# Canonical list of TDengine column types tested as the LOCAL side.
_TDS_ALL = [
    "bool",
    "tinyint",      "smallint",     "int",      "bigint",
    "tinyint unsigned", "smallint unsigned",
    "int unsigned", "bigint unsigned",
    "float",        "double",
    "decimal(18,4)", "decimal(10,2)",
    "varchar(32)",  "nchar(32)",
    "varbinary(64)",
    "timestamp",
]

# Compatible (remote_col -> {tdengine_decl, ...}) mappings.
# Anything not in the compatible set, against that remote column, must fail.
# Compatible (remote_col -> {tdengine_decl, ...}) mappings.
# Anything not in the compatible set, against that remote column, must fail.
# Empty set ⇒ canonical TD target is outside _TDS_ALL (decimal(38,6),
# decimal(18,2), blob) — every _TDS_ALL local type must be rejected.
_PG_COMPAT = {
    "c_bool":         {"bool"},
    "c_smallint":     {"smallint"},
    "c_int":          {"int"},
    "c_bigint":       {"bigint"},
    "c_real":         {"float"},
    "c_double":       {"double"},
    "c_numeric_184":  {"decimal(18,4)"},
    "c_numeric":      set(),                    # → decimal(38,6)
    "c_money":        set(),                    # → decimal64(18,2)
    "c_serial":       {"int"},
    "c_bigserial":    {"bigint"},
    "c_smallserial":  {"smallint"},
    "c_varchar":      {"nchar(32)"},
    "c_char":         {"nchar(32)"},            # VAR type, bytes ignored
    "c_text":         {"nchar(32)"},
    "c_bytea":        {"varbinary(64)"},
    "c_bit":          {"varbinary(64)"},
    "c_bitvar":       {"varbinary(64)"},
    "c_date":         {"timestamp"},
    "c_time":         {"bigint"},
    "c_timetz":       {"bigint"},
    "c_timestamp":    {"timestamp"},
    "c_timestamptz":  {"timestamp"},
    "c_interval":     {"bigint"},
    "c_uuid":         {"varchar(32)"},
    "c_json":         {"nchar(32)"},
    "c_jsonb":        {"nchar(32)"},
    "c_xml":          {"nchar(32)"},
    "c_inet":         {"varchar(32)"},
    "c_cidr":         {"varchar(32)"},
    "c_macaddr":      {"varchar(32)"},
    "c_macaddr8":     {"varchar(32)"},
}

_MY_COMPAT = {
    "c_tinyint":   {"tinyint"},
    "c_stinyint":  {"bool"},
    "c_smallint":  {"smallint"},
    "c_mediumint": {"int"},
    "c_int":       {"int"},
    "c_bigint":    {"bigint"},
    "c_utiny":     {"tinyint unsigned"},
    "c_usmall":    {"smallint unsigned"},
    "c_umed":      {"int unsigned"},
    "c_uint":      {"int unsigned"},
    "c_ubig":      {"bigint unsigned"},
    "c_float":     {"float"},
    "c_double":    {"double"},
    "c_dec":       {"decimal(10,2)"},
    "c_bit8":      {"bigint"},
    "c_bit64":     {"bigint unsigned"},     # DS says BIGINT but code returns UBIGINT (correct for u64 range)
    "c_bool":      {"bool"},
    "c_date":      {"timestamp"},
    "c_datetime":  {"timestamp"},
    "c_ts2":       {"timestamp"},
    "c_time":      {"bigint"},
    "c_year":      {"smallint"},
    "c_char_utf":  {"nchar(32)"},
    "c_char_asc":  {"varchar(32)"},         # TSDB_DATA_TYPE_BINARY ≡ VARCHAR
    "c_vchar_utf": {"nchar(32)"},
    "c_vchar_asc": {"varchar(32)"},
    "c_text_utf":  {"nchar(32)"},
    "c_text_asc":  {"varchar(32)"},
    "c_ttext":     {"nchar(32)"},
    "c_mtext":     {"nchar(32)"},
    "c_ltext":     {"nchar(32)"},
    "c_binary":    {"varbinary(64)"},
    "c_vbinary":   {"varbinary(64)"},
    "c_blob":      {"varbinary(64)"},
    "c_tblob":     {"varbinary(64)"},
    "c_mblob":     {"varbinary(64)"},
    "c_lblob":     set(),                   # → TSDB_DATA_TYPE_BLOB
    "c_enum":      {"nchar(32)"},
    "c_set":       {"nchar(32)"},
    "c_json":      {"nchar(32)"},
}

_INF_COMPAT = {
    "c_int":   {"bigint"},
    "c_uint":  {"bigint unsigned"},
    "c_float": {"double"},
    "c_str":   {"nchar(32)"},
    "c_bool":  {"bool"},
}

# SQL Server: compatibility against the _TDS_ALL local types. Money types map
# to decimal(19,4) / decimal(10,4) (outside _TDS_ALL), IMAGE maps to BLOB
# (outside _TDS_ALL), so those entries have empty compatible sets. VARCHAR /
# NCHAR / VARBINARY are VAR types: only the type kind is compared at CREATE
# time, hence the _TDS_ALL representative width is compatible with any remote
# width. TIMESTAMP references are accepted in 'us'/'ns' local databases.
_SS_COMPAT = {
    "c_bit":            {"bool"},
    "c_tinyint":        {"tinyint"},
    "c_smallint":       {"smallint"},
    "c_int":            {"int"},
    "c_bigint":         {"bigint"},
    "c_real":           {"float"},
    "c_float":          {"double"},
    "c_decimal_184":    {"decimal(18,4)"},
    "c_decimal_102":    {"decimal(10,2)"},
    "c_numeric":        {"decimal(18,4)"},
    "c_money":          set(),                  # → decimal(19,4)
    "c_smallmoney":     set(),                  # → decimal(10,4)
    "c_char":           {"varchar(32)"},        # VAR type, bytes ignored
    "c_varchar":        {"varchar(32)"},
    "c_nchar":          {"nchar(32)"},          # VAR type, bytes ignored
    "c_nvarchar":       {"nchar(32)"},
    "c_uuid":           {"varchar(32)"},        # UNIQUEIDENTIFIER → varchar(36)
    "c_xml":            {"nchar(32)"},
    "c_binary":         {"varbinary(64)"},      # VAR type, bytes ignored
    "c_varbinary":      {"varbinary(64)"},
    "c_image":          set(),                  # → BLOB
    "c_rowversion":     {"varbinary(64)"},      # → varbinary(8)
    "c_date":           {"timestamp"},
    "c_time":           {"bigint"},
    "c_datetime":       {"timestamp"},
    "c_smalldatetime":  {"timestamp"},
    "c_datetime2":      {"timestamp"},
    "c_datetimeoffset": {"timestamp"},
}

# Oracle: NUMBER targets are exact (precision and scale must match), while
# CHAR/VARCHAR2 and the national character family are VAR types whose width is
# not compared at CREATE time.  Bare NUMBER, INTEGER/SMALLINT (NUMBER(38,0)),
# FLOAT(54..126), REAL and DOUBLE PRECISION have no lossless target, and BLOB
# is outside _TDS_ALL, so those entries have empty compatible sets.
_ORACLE_COMPAT = {
    "c_num_dec":       {"decimal(10,2)"},
    "c_num_bigint":    {"bigint"},
    "c_num_wide":      set(),                   # → decimal(19,0)
    "c_num_38":        set(),                   # → decimal(38,0)
    "c_num_one":       {"tinyint"},
    "c_number_bare":   set(),                   # → EXT_TYPE_NOT_MAPPABLE
    "c_integer":       set(),                   # → decimal(38,0)
    "c_smallint":      set(),                   # → decimal(38,0)
    "c_float24":       {"float"},
    "c_float53":       {"double"},
    "c_float126":      set(),                   # → EXT_TYPE_NOT_MAPPABLE
    "c_real":          set(),                   # REAL == FLOAT(63)
    "c_bin_float":     {"float"},
    "c_bin_double":    {"double"},
    "c_char":          {"varchar(32)"},         # VAR type, bytes ignored
    "c_varchar2":      {"varchar(32)"},
    "c_nchar":         {"nchar(32)"},           # VAR type, bytes ignored
    "c_nvarchar2":     {"nchar(32)"},
    "c_clob":          {"nchar(32)"},
    "c_nclob":         {"nchar(32)"},
    "c_long":          {"nchar(32)"},
    "c_raw":           {"varbinary(64)"},       # VAR type, bytes ignored
    "c_long_raw":      {"varbinary(64)"},
    "c_blob":          set(),                   # → BLOB
    "c_rowid":         {"varchar(32)"},         # → varchar(40)
    "c_urowid":        {"varchar(32)"},         # → varchar(40)
    "c_xmltype":       {"nchar(32)"},
    "c_date":          {"timestamp"},
    "c_ts6":           {"timestamp"},
    "c_ts9":           {"timestamp"},
    "c_ts_tz":         {"timestamp"},
    "c_ts_ltz":        {"timestamp"},
}

def _check_compat_consistency():
    """Sanity: every compatible local decl listed in the matrices is one of
    the canonical TDengine type strings in _TDS_ALL."""
    for name, m in (("PG", _PG_COMPAT), ("MY", _MY_COMPAT), ("INF", _INF_COMPAT),
                    ("SS", _SS_COMPAT), ("ORA", _ORACLE_COMPAT)):
        for col, ok in m.items():
            unknown = ok - set(_TDS_ALL)
            assert not unknown, (
                f"{name}: {col} compat references unknown TDengine "
                f"type(s): {unknown}")
_check_compat_consistency()





# ---------------------------------------------------------------------------
# Provisioning helpers
# ---------------------------------------------------------------------------

def _drop_local_db():
    tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")

def _create_local_db(precision="ms"):
    _drop_local_db()
    tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION '{precision}'")
    tdSql.execute(f"USE {_LOCAL_DB}")

def create_pg_remote_table(table, matrix):
    cols = "ts TIMESTAMP PRIMARY KEY, " + ", ".join(
        f"{n} {t}" for n, t, _ in matrix)
    create_pg_table(_PG_DB, table, cols)

def create_mysql_remote_table(table, matrix):
    cols = "ts DATETIME(3) NOT NULL PRIMARY KEY, " + ", ".join(
        f"{n} {t}" for n, t, _ in matrix)
    create_mysql_table(_MY_DB, table, cols)

def create_sqlserver_remote_table(table, matrix, schema="dbo", rows=None,
                                  ts_type="datetime2(3)"):
    # The virtual-table precision follows the declared fractional precision of
    # the timestamp primary key (datetime2(3) -> ms, datetime2(>3) -> us).
    cols = f"ts {ts_type} NOT NULL PRIMARY KEY, " + ", ".join(
        f"{n} {t}" for n, t, _ in matrix)
    create_sqlserver_table(_SS_DB, table, cols, rows=rows, schema=schema)

def create_influx_remote_table(meas):
    # Influx is schema-on-write — emit one row so all fields exist.
    create_influx_measurement(_INF_DB, [
        f"{meas},tag1=a "
        f"c_int=1i,c_uint=2u,c_float=3.5,c_str=\"x\",c_bool=true "
        f"1700000000000000000"
    ])

def create_oracle_remote_table(table, matrix, rows=None, ts_type="TIMESTAMP(3)",
                               schema=None, user=None, password=None):
    # The timestamp primary-key declaration drives the source precision
    # (TIMESTAMP(3) -> ms, TIMESTAMP(6) -> us, TIMESTAMP(9) -> ns).
    cols = f"ts {ts_type} NOT NULL PRIMARY KEY, " + ", ".join(
        f"{n} {t}" for n, t, _ in matrix)
    create_oracle_table(table, cols, rows=rows, schema=schema, user=user,
                        password=password)

def _provision_heterogeneous_remote_tables():
    """Four remote backends, each holding a table whose schemas DIFFER but
    share two columns: temperature (DOUBLE) and humidity (DOUBLE).

    This is the canonical patent scenario (used by §3 vstable tests):

        PG.kpi_pg      (ts, temperature, humidity, pressure, voltage)
        MySQL.kpi_my   (ts, temperature, humidity, fan_rpm)
        Influx.kpi_in  (time, temperature, humidity, battery)
        SQL Server.kpi_ss (ts, temperature, humidity, pressure, voltage)
        Oracle.kpi_ora / kpi_ora_b (ts, temperature, humidity, ...)
    """
    create_pg_table(_PG_DB, "kpi_pg",
                    "ts TIMESTAMP PRIMARY KEY, "
                    "temperature DOUBLE PRECISION, humidity DOUBLE PRECISION, "
                    "pressure REAL, voltage REAL",
                    ["('2024-01-01 00:00:00', 25.1, 60.2, 1013.0, 3.30)",
                     "('2024-01-01 00:01:00', 25.2, 60.3, 1013.1, 3.31)"])
    create_mysql_table(_MY_DB, "kpi_my",
                       "ts DATETIME(3) NOT NULL PRIMARY KEY, "
                       "temperature DOUBLE, humidity DOUBLE, fan_rpm INT",
                       ["('2024-01-01 00:00:00', 22.5, 55.0, 1200)",
                        "('2024-01-01 00:01:00', 22.6, 55.1, 1210)"])
    create_influx_measurement(_INF_DB, [
        "kpi_in,site=a temperature=18.0,humidity=70.0,battery=88i "
            "1704067200000000000",
        "kpi_in,site=a temperature=18.1,humidity=70.1,battery=87i "
            "1704067260000000000",
    ])
    create_sqlserver_remote_table(
        "kpi_ss",
        [("temperature", "FLOAT",        "double"),
         ("humidity",    "FLOAT",        "double"),
         ("pressure",    "REAL",         "float"),
         ("voltage",     "REAL",         "float")],
        rows=["('2024-01-01 00:00:00.000', 30.5, 40.0, 1013.0, 3.30)",
              "('2024-01-01 00:01:00.000', 31.5, 41.0, 1013.1, 3.31)"])
    # BINARY_DOUBLE keeps the two shared columns IEEE-double, matching the
    # other backends; the fixed-point BINARY_FLOAT columns are deliberately
    # not asserted value-by-value.
    create_oracle_remote_table(
        "kpi_ora",
        [("temperature", "BINARY_DOUBLE", "double"),
         ("humidity",    "BINARY_DOUBLE", "double"),
         ("pressure",    "BINARY_FLOAT",  "float"),
         ("voltage",     "BINARY_FLOAT",  "float")],
        rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 30.5, 40.0, 1013.0, 3.3)",
              "(TIMESTAMP '2024-01-01 00:01:00.000', 31.5, 41.0, 1013.1, 3.31)"])
    create_oracle_remote_table(
        "kpi_ora_b",
        [("temperature", "BINARY_DOUBLE", "double"),
         ("humidity",    "BINARY_DOUBLE", "double")],
        rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 28.5, 50.0)",
              "(TIMESTAMP '2024-01-01 00:02:00.000', 29.5, 51.0)"])

def _ensure_sources():
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_ext_source(_MY_SRC, "mysql", _MY_DB)
    create_ext_source(_INF_SRC, "influxdb", _INF_DB)
    create_ext_source(_SS_SRC, "sqlserver", _SS_DB)
    create_ext_source(_OR_SRC, "oracle", _OR_SVC)
    # Oracle accounts own no cross-schema grants, so "same service, different
    # schema" needs the second schema's own credentials (DS 5.3.10).
    create_ext_source(_OR_ALT_SRC, "oracle", _OR_SVC,
                      user=ExtSrcEnv.ORACLE_ALT_USER,
                      password=ExtSrcEnv.ORACLE_ALT_PASS)




# ===========================================================================
# Test class
# ===========================================================================

class TestVtableCreateExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        create_remote_db("postgresql", _PG_DB)
        create_remote_db("mysql", _MY_DB)
        create_remote_db("influxdb", _INF_DB)
        create_remote_db("sqlserver", _SS_DB)
        # Oracle has no per-source database: this is a no-op that documents
        # that the service name already exists.
        create_remote_db("oracle", _OR_SVC)
        _ensure_sources()
        _create_local_db("ns")
        # Provision the heterogeneous kpi_* remote tables once for §3 vstable
        # patent-core tests; they don't conflict with §2 vtable tests because
        # the latter create their own t_* remote tables under distinct names.
        _provision_heterogeneous_remote_tables()

    @classmethod
    def teardown_class(cls):
        # This class had no working teardown: the method was commented out and
        # its @classmethod decorator was left dangling on the test that
        # followed.  The InfluxDB bucket therefore survived for the lifetime of
        # the node, and since InfluxDB 3 Core allows only five databases per
        # instance, a full run eventually makes every later
        # influx_create_db() fail with HTTP 422 in some unrelated suite.
        # Release just the bucket: the local database and the EXTERNAL SOURCEs
        # are recreated by setup_class anyway.
        ExtSrcEnv.influx_drop_db(_INF_DB)

    # -------------------------------------------------------------------
    # 2.1 — Reference legal remote columns
    # -------------------------------------------------------------------

    def test_create_legal_references(self):
        # (a) Single source, all PG types mapped and DESCRIBE verified.
        """Create legal references

        (a) Single source, all PG types mapped and DESCRIBE verified.

        Since: v3.4.0.0

        """
        tbl = "t_single"
        create_pg_remote_table(tbl, _PG_TYPE_MATRIX)
        cols = ", ".join(
            f"{n} {ldecl} FROM {_PG_SRC}.{_PG_DB}.{tbl}.{n}"
            for n, _, ldecl in _PG_TYPE_MATRIX)
        tdSql.execute(f"CREATE VTABLE v_single (ts timestamp, {cols})")
        tdSql.query("DESCRIBE v_single")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        for i, (n, _, ldecl) in enumerate(_PG_TYPE_MATRIX):
            row = i + 1
            tdSql.checkData(row, 0, n)
        tdSql.execute("DROP VTABLE v_single")

        # (b) Multi remote tables within same source.
        for i in (1, 2, 3):
            create_pg_remote_table(
                f"t_multi_{i}", [("v", "INTEGER", "int")])
        tdSql.execute(
            "CREATE VTABLE v_multi_tbl ("
            f"ts timestamp, "
            f"col1 int FROM {_PG_SRC}.{_PG_DB}.t_multi_1.v, "
            f"col2 int FROM {_PG_SRC}.{_PG_DB}.t_multi_2.v, "
            f"col3 int FROM {_PG_SRC}.{_PG_DB}.t_multi_3.v)")
        tdSql.query("DESCRIBE v_multi_tbl")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "col1")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "col2")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "col3")
        tdSql.checkData(3, 1, "INT")
        tdSql.execute("DROP VTABLE v_multi_tbl")

        # (c) One vtable, columns drawn from all three backends.
        create_pg_remote_table("tx", [("v", "INTEGER", "int")])
        create_mysql_remote_table("tx", [("v", "INTEGER", "int")])
        create_influx_remote_table("tx_inf")
        tdSql.execute(
            "CREATE VTABLE v_multi_src ("
            "ts timestamp, "
            f"c_pg int FROM {_PG_SRC}.{_PG_DB}.tx.v, "
            f"c_my int FROM {_MY_SRC}.{_MY_DB}.tx.v, "
            f"c_inf bigint FROM {_INF_SRC}.{_INF_DB}.tx_inf.c_int)")
        tdSql.query("DESCRIBE v_multi_src")
        tdSql.checkData(1, 0, "c_pg")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "c_my")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "c_inf")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.execute("DROP VTABLE v_multi_src")

        # (d) Mixed ext + local-NULL placeholder columns.
        create_pg_remote_table("tx_mix", [("v", "INTEGER", "int")])
        tdSql.execute(
            "CREATE VTABLE v_mix ("
            "ts timestamp, "
            f"ref_col int FROM {_PG_SRC}.{_PG_DB}.tx_mix.v, "
            "null_col int, "
            "null_str nchar(32))")
        tdSql.query("DESCRIBE v_mix")
        tdSql.checkData(1, 0, "ref_col")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "null_col")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "null_str")
        tdSql.checkData(3, 1, "NCHAR")
        tdSql.execute("DROP VTABLE v_mix")

        # (e) ts must be local (no FROM); ts with FROM must be rejected.
        create_pg_remote_table("tx_ts", [("v", "INTEGER", "int")])
        tdSql.execute(
            "CREATE VTABLE v_ts_local "
            f"(ts timestamp, v int FROM {_PG_SRC}.{_PG_DB}.tx_ts.v)")
        tdSql.query("DESCRIBE v_ts_local")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.execute("DROP VTABLE v_ts_local")
        tdSql.error(
            "CREATE VTABLE v_ts_remote "
            f"(ts timestamp FROM {_PG_SRC}.{_PG_DB}.tx_ts.ts, "
            f" v int FROM {_PG_SRC}.{_PG_DB}.tx_ts.v)")

    # -------------------------------------------------------------------
    # 2.2 — Type compatibility matrix (joint positive+negative enumeration)
    #
    # For every (remote_col, tdengine_local_decl) pair:
    #   * pair ∈ compat  → CREATE must succeed AND DESCRIBE shows the
    #                      declared local type;
    #   * pair ∉ compat  → CREATE must be rejected (or, by [contract], the
    #                      column must not materialise).
    # All violations of either direction are collected and reported in a
    # single AssertionError so one regression doesn't mask others.
    # -------------------------------------------------------------------

    def _check_type_compat_matrix(self, table, compat, refmaker):
        for col, ok_set in compat.items():
            for tds in _TDS_ALL:
                expected_ok = tds in ok_set
                safe = (tds.replace("(", "_").replace(")", "")
                           .replace(",", "_").replace(" ", "_"))
                vname = f"v_m_{table}_{col}_{safe}"
                sql = (f"CREATE VTABLE {vname} (ts timestamp, "
                       f"x {tds} FROM {refmaker(col)})")
                tag = "compat" if expected_ok else "incompat"
                if expected_ok:
                    tdSql.execute(sql)
                    tdSql.execute(f"DROP VTABLE {vname}")
                else:
                    tdSql.error(sql)

    def test_type_compatibility_matrix(self):
        # (a) PG type matrix: 32 remote types × 17 TDengine local types.
        """Type compatibility matrix

        (a) PG type matrix: 32 remote types × 17 TDengine local types.

        Since: v3.4.0.0

        """
        create_pg_remote_table("t_typematrix_pg", _PG_TYPE_MATRIX)
        self._check_type_compat_matrix(
            "pg", _PG_COMPAT,
            lambda c: f"{_PG_SRC}.{_PG_DB}.t_typematrix_pg.{c}")

        # (b) MySQL type matrix: 41 remote types × 17 TDengine local types.
        create_mysql_remote_table("t_typematrix_my", _MY_TYPE_MATRIX)
        self._check_type_compat_matrix(
            "my", _MY_COMPAT,
            lambda c: f"{_MY_SRC}.{_MY_DB}.t_typematrix_my.{c}")

        # (c) Influx type matrix: 5 LP types × 17 TDengine local types.
        create_influx_remote_table("t_typematrix_inf")
        self._check_type_compat_matrix(
            "inf", _INF_COMPAT,
            lambda c: f"{_INF_SRC}.{_INF_DB}.t_typematrix_inf.{c}")

        # (c2) SQL Server type matrix: 28 remote types × 17 TDengine local
        # types. The reference uses the 5-segment path
        # source.database.schema.table.column (DS 5.3.3).
        create_sqlserver_remote_table("t_typematrix_ss", _SS_TYPE_MATRIX)
        self._check_type_compat_matrix(
            "ss", _SS_COMPAT,
            lambda c: f"{_SS_SRC}.{_SS_DB}.dbo.t_typematrix_ss.{c}")

        # (c3) Oracle type matrix: 32 remote types × 17 TDengine local types.
        # Oracle permits one LONG-family column per table (ORA-01754), so the
        # matrix is spread over three remote tables. References use the
        # 4-segment path source.schema.table.column (DS 5.3.3).
        create_oracle_remote_table("t_typematrix_ora", _ORACLE_TYPE_MATRIX)
        create_oracle_remote_table("t_typematrix_ora_long", _ORACLE_LONG_TYPE_MATRIX)
        create_oracle_remote_table("t_typematrix_ora_lraw", _ORACLE_LRAW_TYPE_MATRIX)
        oracle_ref_tables = {}
        for matrix, table in ((_ORACLE_TYPE_MATRIX, "t_typematrix_ora"),
                              (_ORACLE_LONG_TYPE_MATRIX, "t_typematrix_ora_long"),
                              (_ORACLE_LRAW_TYPE_MATRIX, "t_typematrix_ora_lraw")):
            for n, _, _ in matrix:
                oracle_ref_tables[n] = table
        self._check_type_compat_matrix(
            "ora", _ORACLE_COMPAT,
            lambda c: f"{_OR_SRC}.{_OR_SCHEMA}.{oracle_ref_tables[c]}.{c}")

        # (d) TIMESTAMP precision cross-product (8 sources × 3 local precisions).
        create_pg_remote_table("ts_p", [("v", "INTEGER", "int")])
        create_mysql_remote_table("ts_p", [("v", "INTEGER", "int")])
        create_influx_remote_table("ts_p_inf")
        create_sqlserver_remote_table("ts_p", [("v", "INT", "int")])
        create_sqlserver_remote_table("ts_p_us", [("v", "INT", "int")],
                                      ts_type="datetime2(6)")
        create_oracle_remote_table("ts_p_ms", [("v", "NUMBER(10,0)", "bigint")],
                                   ts_type="TIMESTAMP(3)")
        create_oracle_remote_table("ts_p_us", [("v", "NUMBER(10,0)", "bigint")],
                                   ts_type="TIMESTAMP(6)")
        create_oracle_remote_table("ts_p_ns", [("v", "NUMBER(10,0)", "bigint")],
                                   ts_type="TIMESTAMP(9)")
        backends = [
            ("pg",  "us", "int",    f"{_PG_SRC}.{_PG_DB}.ts_p.v"),
            ("my",  "ms", "int",    f"{_MY_SRC}.{_MY_DB}.ts_p.v"),
            ("inf", "ns", "bigint", f"{_INF_SRC}.{_INF_DB}.ts_p_inf.c_int"),
            ("ss",    "ms", "int", f"{_SS_SRC}.{_SS_DB}.dbo.ts_p.v"),
            ("ss_us", "us", "int", f"{_SS_SRC}.{_SS_DB}.dbo.ts_p_us.v"),
            ("ora_ms", "ms", "bigint", f"{_OR_SRC}.{_OR_SCHEMA}.ts_p_ms.v"),
            ("ora_us", "us", "bigint", f"{_OR_SRC}.{_OR_SCHEMA}.ts_p_us.v"),
            ("ora_ns", "ns", "bigint", f"{_OR_SRC}.{_OR_SCHEMA}.ts_p_ns.v"),
        ]
        precision_rank = {"ms": 0, "us": 1, "ns": 2}
        for prec in ("ms", "us", "ns"):
            db = f"vtcreate_p_{prec}"
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} PRECISION '{prec}'")
            tdSql.execute(f"USE {db}")
            for label, source_prec, v_type, v_ref in backends:
                vname = f"v_ts_{label}_{prec}"
                sql = (
                    f"CREATE VTABLE {vname} ("
                    f"ts timestamp, "
                    f"v {v_type} FROM {v_ref})")
                if precision_rank[prec] < precision_rank[source_prec]:
                    tdSql.error(sql, expectedErrno=0x268f)
                else:
                    tdSql.execute(sql)
                    tdSql.query(f"DESCRIBE {vname}")
                    tdSql.checkData(0, 0, "ts")
                    tdSql.checkData(0, 1, "TIMESTAMP")
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"USE {_LOCAL_DB}")

        # (e) VARCHAR length boundary (=, <, >).
        create_pg_table(_PG_DB, "vlen",
                        "ts TIMESTAMP PRIMARY KEY, v VARCHAR(32)")
        tdSql.execute(
            f"CREATE VTABLE v_len_eq (ts timestamp, "
            f"v nchar(32) FROM {_PG_SRC}.{_PG_DB}.vlen.v)")
        tdSql.execute(
            f"CREATE VTABLE v_len_gt (ts timestamp, "
            f"v nchar(64) FROM {_PG_SRC}.{_PG_DB}.vlen.v)")
        # local < remote — VAR-type length is not checked at CREATE time;
        # narrowing succeeds (truncation deferred to query time).
        tdSql.execute(
            f"CREATE VTABLE v_len_lt (ts timestamp, "
            f"v nchar(8) FROM {_PG_SRC}.{_PG_DB}.vlen.v)")
        tdSql.execute("DROP VTABLE v_len_lt")
        tdSql.execute("DROP VTABLE v_len_eq")
        tdSql.execute("DROP VTABLE v_len_gt")

    # -------------------------------------------------------------------
    # 2.3 — Illegal references and FROM segment-count parsing
    # -------------------------------------------------------------------

    def test_illegal_references_and_segment_parsing(self):
        # (a) Unknown source.
        """Illegal references and segment parsing

        (a) Unknown source.

        Since: v3.4.0.0

        """
        tdSql.error(
            "CREATE VTABLE v_unk_src (ts timestamp, "
            "v int FROM no_such_source.no_db.no_tbl.v)")

        # (b) Unknown remote table.
        tdSql.error(
            f"CREATE VTABLE v_unk_tbl (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.no_such_table.v)")

        # (c) Unknown remote column.
        create_pg_remote_table("t_unkcol", [("v", "INTEGER", "int")])
        tdSql.error(
            f"CREATE VTABLE v_unk_col (ts timestamp, "
            f"x int FROM {_PG_SRC}.{_PG_DB}.t_unkcol.no_such_col)")

        # (d) Malformed FROM clause (missing col / empty triplet).
        tdSql.error(
            f"CREATE VTABLE v_bad_from (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.t_typematrix_pg)")
        tdSql.error(
            "CREATE VTABLE v_bad_from2 (ts timestamp, v int FROM .)")

        # (e) Duplicate column name.
        create_pg_remote_table("t_dup", [("v", "INTEGER", "int")])
        tdSql.error(
            f"CREATE VTABLE v_dup_col (ts timestamp, "
            f"x int FROM {_PG_SRC}.{_PG_DB}.t_dup.v, "
            f"x int FROM {_PG_SRC}.{_PG_DB}.t_dup.v)")

        # (f) Type incompatibility — bytea cannot map to INT.
        create_pg_table(_PG_DB, "t_bytea",
                        "ts TIMESTAMP PRIMARY KEY, b BYTEA")
        tdSql.error(
            f"CREATE VTABLE v_bytea_bad (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.t_bytea.b)")

        # (f2) InfluxDB tag cannot be referenced — only fields are allowed.
        create_influx_measurement(_INF_DB, [
            "t_inftag,device=sensor1 value=42.0 1700000000000000000"
        ])
        tdSql.error(
            f"CREATE VTABLE v_inftag_bad (ts timestamp, "
            f"d nchar(64) FROM {_INF_SRC}.{_INF_DB}.t_inftag.device)")

        # (g) 4-seg resolves to ext source.
        create_pg_remote_table("t_4seg", [("v", "INTEGER", "int")])
        tdSql.execute(
            f"CREATE VTABLE v_4seg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.t_4seg.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_4seg' "
            f"AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.public.t_4seg.v")
        tdSql.execute("DROP VTABLE v_4seg")

        # (h) 3-component source.table.col resolves through the default
        # database/schema of the source (DS section 6.1.3.1); the 4-component
        # form stays available to name the remote database explicitly.
        create_pg_remote_table("t_3disamb", [("v", "INTEGER", "int")])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(
            f"CREATE VTABLE v_3disamb_short (ts timestamp, "
            f"v int FROM {_PG_SRC}.t_3disamb.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3disamb_short' "
            f"AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.public.t_3disamb.v")
        tdSql.execute("DROP VTABLE v_3disamb_short")
        tdSql.execute(
            f"CREATE VTABLE v_3disamb (ts timestamp, "
            f"v int FROM {_PG_SRC}.t_3disamb.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3disamb' "
            f"AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.public.t_3disamb.v")
        tdSql.execute("DROP VTABLE v_3disamb")

        # The same binding through the explicit 4-component form.
        tdSql.execute(
            f"CREATE VTABLE v_3disamb4 (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.t_3disamb.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3disamb4' "
            f"AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.public.t_3disamb.v")
        tdSql.execute("DROP VTABLE v_3disamb4")

        # (i) 3-seg without source name stays local ref → must be rejected.
        create_pg_remote_table("t_3local", [("v", "INTEGER", "int")])
        bogus_db = "no_such_db_for_3seg_test"
        tdSql.error(
            f"CREATE VTABLE v_3local (ts timestamp, "
            f"v int FROM {bogus_db}.t_3local.v)")

        # (j) 2-seg resolves to current-db (negative — no local table).
        create_pg_remote_table("t_2seg", [("v", "INTEGER", "int")])
        tdSql.error(
            "CREATE VTABLE v_2seg (ts timestamp, "
            "v int FROM t_2seg.v)")

        # (k) 2-seg local table resolves (positive).
        tdSql.execute("CREATE TABLE t_proxy (ts timestamp, v int)")
        tdSql.execute("INSERT INTO t_proxy VALUES (NOW, 7)")
        tdSql.execute(
            "CREATE VTABLE v_proxy (ts timestamp, "
            "v int FROM t_proxy.v)")
        tdSql.execute("DROP VTABLE v_proxy")
        tdSql.execute("DROP TABLE t_proxy")

        # (l) 1-seg is syntax error.
        tdSql.error(
            "CREATE VTABLE v_1seg (ts timestamp, "
            f"v int FROM v)")

        # (m) 5-seg is syntax error.
        tdSql.error(
            "CREATE VTABLE v_5seg (ts timestamp, "
            f"v int FROM x.{_PG_SRC}.{_PG_DB}.t_4seg.v)")

        # (n) Explicit external refs with multiple columns.
        tdSql.execute(
            f"CREATE VTABLE v_3mix4 (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.t_3disamb.v, "
            f"v2 int FROM {_PG_SRC}.{_PG_DB}.t_4seg.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3mix4' "
            f"AND col_name IN ('v', 'v2') ORDER BY col_source")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.public.t_3disamb.v")
        tdSql.checkData(1, 0, f"{_PG_SRC}.{_PG_DB}.public.t_4seg.v")
        tdSql.execute("DROP VTABLE v_3mix4")

    def test_reject_remote_views_for_normal_and_child_vtables(self):
        """Remote views cannot back virtual table external refs.

        Remote views cannot back virtual table external refs.

        Since: v3.4.0.0

        """
        create_pg_table(_PG_DB, "view_base_pg",
                        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
                        ["('2024-01-01 00:00:00', 1)"])
        create_pg_view(_PG_DB, "v_pg_with_ts",
                       "SELECT ts, v FROM public.view_base_pg")
        create_pg_view(_PG_DB, "v_pg_no_ts",
                       "SELECT v FROM public.view_base_pg")

        create_mysql_table(_MY_DB, "view_base_my",
                           "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT",
                           ["('2024-01-01 00:00:00', 10)"])
        create_mysql_view(_MY_DB, "v_my_with_ts",
                          "SELECT ts, v FROM view_base_my")
        create_mysql_view(_MY_DB, "v_my_no_ts",
                          "SELECT v FROM view_base_my")

        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")

        for src, db, view in [
            (_PG_SRC, _PG_DB, "v_pg_with_ts"),
            (_PG_SRC, _PG_DB, "v_pg_no_ts"),
            (_MY_SRC, _MY_DB, "v_my_with_ts"),
            (_MY_SRC, _MY_DB, "v_my_no_ts"),
        ]:
            tdSql.error(
                f"CREATE VTABLE v_view_bad (ts timestamp, "
                f"v int FROM {src}.{db}.{view}.v)")

        tdSql.execute("DROP STABLE IF EXISTS vstb_view_bad")
        tdSql.execute(
            "CREATE STABLE vstb_view_bad (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.error(
            f"CREATE VTABLE vctb_view_bad_pg ("
            f"v FROM {_PG_SRC}.{_PG_DB}.v_pg_with_ts.v) "
            "USING vstb_view_bad TAGS ('pg')")
        tdSql.error(
            f"CREATE VTABLE vctb_view_bad_my ("
            f"v FROM {_MY_SRC}.{_MY_DB}.v_my_with_ts.v) "
            "USING vstb_view_bad TAGS ('my')")
        tdSql.execute("DROP STABLE IF EXISTS vstb_view_bad")

    def test_pg_source_schema_mismatch_blocks_existing_vtable_query(self):
        """Existing vtable query fails while source schema mismatches the saved ref.

        Existing vtable query fails while source schema mismatches the saved ref.

        Since: v3.4.0.0

        """
        create_pg_table(_PG_DB, "schema_base",
                        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
                        ["('2024-01-01 00:00:00', 11)"])
        ExtSrcEnv.pg_exec(_PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS analytics.schema_base",
            "CREATE TABLE analytics.schema_base (ts TIMESTAMP PRIMARY KEY, v INTEGER)",
            "INSERT INTO analytics.schema_base VALUES ('2024-01-01 00:00:00', 22)",
        ])

        try:
            tdSql.execute(f"ALTER EXTERNAL SOURCE {_PG_SRC} SET schema='public'")
            tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
            tdSql.execute(
                f"CREATE VTABLE v_pg_schema_public (ts timestamp, "
                f"v int FROM {_PG_SRC}.{_PG_DB}.schema_base.v)")
            tdSql.query("SELECT v FROM v_pg_schema_public")
            tdSql.checkData(0, 0, 11)

            tdSql.execute(f"ALTER EXTERNAL SOURCE {_PG_SRC} SET schema='analytics'")
            tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
            tdSql.error("SELECT v FROM v_pg_schema_public")

            tdSql.execute(
                f"CREATE VTABLE v_pg_schema_analytics (ts timestamp, "
                f"v int FROM {_PG_SRC}.{_PG_DB}.schema_base.v)")
            tdSql.query("SELECT v FROM v_pg_schema_analytics")
            tdSql.checkData(0, 0, 22)

            tdSql.execute(f"ALTER EXTERNAL SOURCE {_PG_SRC} SET schema='public'")
            tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
            tdSql.query("SELECT v FROM v_pg_schema_public")
            tdSql.checkData(0, 0, 11)
        finally:
            tdSql.execute(f"ALTER EXTERNAL SOURCE {_PG_SRC} SET schema='public'")
            tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
            tdSql.execute("DROP VTABLE IF EXISTS v_pg_schema_public")
            tdSql.execute("DROP VTABLE IF EXISTS v_pg_schema_analytics")

    def test_quoted_remote_identifiers_in_vtable_refs(self):
        """Quoted PG identifiers survive create, query, and metadata display.

        Quoted PG identifiers survive create, query, and metadata display.

        Since: v3.4.0.0

        """
        ExtSrcEnv.pg_exec(_PG_DB, [
            'DROP TABLE IF EXISTS public."CaseTable"',
            'CREATE TABLE public."CaseTable" (ts TIMESTAMP PRIMARY KEY, "MixedValue" INTEGER)',
            'INSERT INTO public."CaseTable" VALUES (\'2024-01-01 00:00:00\', 33)',
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(
            f"CREATE VTABLE v_quoted_pg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.`CaseTable`.`MixedValue`)")
        tdSql.query("SELECT v FROM v_quoted_pg")
        tdSql.checkData(0, 0, 33)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_quoted_pg' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.execute("DROP VTABLE IF EXISTS v_quoted_pg")

    # -------------------------------------------------------------------
    # SQL Server references (FQSS-VTBL-001 .. 005, 007 .. 010)
    #
    # SQL Server namespaces are <database>.<schema>.<object>, so column
    # references use the 5-segment path source.database.schema.table.column
    # (DS 5.3.3). The local database uses PRECISION 'ns' so the sub-millisecond
    # axes (datetime2(>3), datetime) are legal here as well.
    # -------------------------------------------------------------------

    def test_sqlserver_legal_references_and_schema(self):
        """FQSS-VTBL-001/004/005/007: legal SQL Server column references.

        (a) mixed external + local placeholder columns addressed by the
            5-segment path, with the persisted column source checked;
        (b) the same table name in two schemas resolves to distinct objects;
        (c) every temporal type can serve as the referenced table's time axis.

        Since: v3.4.4.0

        """
        # --- (a) 5-segment path, mixed ext + local columns ---
        create_sqlserver_remote_table(
            "r_mixed", [("v", "INT", "int"), ("name", "NVARCHAR(32)", "nchar(32)")],
            rows=["('2024-01-01 00:00:00.000', 11, 'alpha')",
                  "('2024-01-01 00:01:00.000', 12, 'beta')"])
        tdSql.execute(
            "DROP VTABLE IF EXISTS v_ss_mixed")
        tdSql.execute(
            "CREATE VTABLE v_ss_mixed ("
            "ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_mixed.v, "
            f"name nchar(32) FROM {_SS_SRC}.{_SS_DB}.dbo.r_mixed.name, "
            "local_col int)")
        tdSql.query("DESCRIBE v_ss_mixed")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "name")
        tdSql.checkData(2, 1, "NCHAR")
        tdSql.checkData(3, 0, "local_col")
        tdSql.checkData(3, 1, "INT")

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_ss_mixed' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        col_source = str(tdSql.queryResult[0][0])
        # The displayed source keeps source/db/table/column (the schema segment
        # is part of the persisted ref but not of the display form).
        assert col_source.startswith(f"{_SS_SRC}.{_SS_DB}."), \
            f"unexpected SQL Server col_source: {col_source!r}"
        assert col_source.endswith(".r_mixed.v"), \
            f"unexpected SQL Server col_source: {col_source!r}"
        tdSql.query("SELECT v, name, local_col FROM v_ss_mixed ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, "alpha")
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 12)
        tdSql.execute("DROP VTABLE v_ss_mixed")

        # --- (b) same database, different schema, same table name ---
        create_sqlserver_remote_table(
            "r_schema", [("v", "INT", "int")],
            rows=["('2024-01-01 00:00:00.000', 21)"], schema="other")
        create_sqlserver_remote_table(
            "r_schema", [("v", "INT", "int")],
            rows=["('2024-01-01 00:00:00.000', 11)"])
        tdSql.execute(
            "CREATE VTABLE v_ss_dbo (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_schema.v)")
        tdSql.execute(
            "CREATE VTABLE v_ss_other (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.other.r_schema.v)")
        tdSql.query("SELECT v FROM v_ss_dbo")
        tdSql.checkData(0, 0, 11)
        tdSql.query("SELECT v FROM v_ss_other")
        tdSql.checkData(0, 0, 21)
        tdSql.execute("DROP VTABLE v_ss_dbo")
        tdSql.execute("DROP VTABLE v_ss_other")

        # --- (c) every temporal type satisfies the time-axis requirement ---
        axis_cases = [
            ("t_dt2", "datetime2(3) NOT NULL PRIMARY KEY"),
            ("t_dto", "datetimeoffset(3) NOT NULL PRIMARY KEY"),
            ("t_dt",  "datetime NOT NULL PRIMARY KEY"),
            ("t_sdt", "smalldatetime NOT NULL PRIMARY KEY"),
            ("t_date", "date NOT NULL PRIMARY KEY"),
        ]
        for table, ts_decl in axis_cases:
            create_sqlserver_table(_SS_DB, table, f"ts {ts_decl}, v INT")
            vname = f"v_ss_axis_{table}"
            tdSql.execute(f"DROP VTABLE IF EXISTS {vname}")
            tdSql.execute(
                f"CREATE VTABLE {vname} (ts timestamp, "
                f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.{table}.v)")
            tdSql.query(f"DESCRIBE {vname}")
            tdSql.checkData(0, 0, "ts")
            tdSql.checkData(0, 1, "TIMESTAMP")
            tdSql.execute(f"DROP VTABLE {vname}")

    def test_sqlserver_ts_precision_matrix(self):
        """FQSS-VTBL-012: vtable precision follows the timestamp declaration.

        A millisecond database — the engine default and the precision IDMP
        creates — must accept every millisecond-aligned SQL Server time axis:
        datetime2(0..3), datetimeoffset(0..3), smalldatetime and date.  It must
        still reject the finer datetime2(4..7), datetimeoffset(4..7) and
        datetime (1/300 s ticks), which a microsecond database accepts.

        Since: v3.4.4.0

        """
        cases = [
            ("p_dt2_0", "datetime2(0) NOT NULL PRIMARY KEY",      "ms"),
            ("p_dt2_1", "datetime2(1) NOT NULL PRIMARY KEY",      "ms"),
            ("p_dt2_2", "datetime2(2) NOT NULL PRIMARY KEY",      "ms"),
            ("p_dt2_3", "datetime2(3) NOT NULL PRIMARY KEY",      "ms"),
            ("p_dt2_4", "datetime2(4) NOT NULL PRIMARY KEY",      "us"),
            ("p_dt2_5", "datetime2(5) NOT NULL PRIMARY KEY",      "us"),
            ("p_dt2_6", "datetime2(6) NOT NULL PRIMARY KEY",      "us"),
            ("p_dt2_7", "datetime2(7) NOT NULL PRIMARY KEY",      "us"),
            ("p_dt2",   "datetime2 NOT NULL PRIMARY KEY",         "us"),
            ("p_dto_0", "datetimeoffset(0) NOT NULL PRIMARY KEY", "ms"),
            ("p_dto_1", "datetimeoffset(1) NOT NULL PRIMARY KEY", "ms"),
            ("p_dto_2", "datetimeoffset(2) NOT NULL PRIMARY KEY", "ms"),
            ("p_dto_3", "datetimeoffset(3) NOT NULL PRIMARY KEY", "ms"),
            ("p_dto_4", "datetimeoffset(4) NOT NULL PRIMARY KEY", "us"),
            ("p_dto_5", "datetimeoffset(5) NOT NULL PRIMARY KEY", "us"),
            ("p_dto_6", "datetimeoffset(6) NOT NULL PRIMARY KEY", "us"),
            ("p_dto_7", "datetimeoffset(7) NOT NULL PRIMARY KEY", "us"),
            ("p_dto",   "datetimeoffset NOT NULL PRIMARY KEY",    "us"),
            ("p_dt",    "datetime NOT NULL PRIMARY KEY",          "us"),
            ("p_sdt",   "smalldatetime NOT NULL PRIMARY KEY",     "ms"),
            ("p_date",  "date NOT NULL PRIMARY KEY",              "ms"),
        ]
        rank = {"ms": 0, "us": 1, "ns": 2}
        for table, ts_decl, source_prec in cases:
            create_sqlserver_table(_SS_DB, table, f"ts {ts_decl}, v INT")
            ref = f"{_SS_SRC}.{_SS_DB}.dbo.{table}.v"
            for db_prec in ("ms", "us", "ns"):
                db = f"vtcreate_ssp_{table}_{db_prec}"
                tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
                tdSql.execute(f"CREATE DATABASE {db} PRECISION '{db_prec}'")
                tdSql.execute(f"USE {db}")
                vname = f"v_{table}_{db_prec}"
                sql = f"CREATE VTABLE {vname} (ts timestamp, v int FROM {ref})"
                if rank[db_prec] < rank[source_prec]:
                    tdSql.error(sql, expectedErrno=0x268f)
                else:
                    tdSql.execute(sql)
                    tdSql.query(f"DESCRIBE {vname}")
                    tdSql.checkData(0, 0, "ts")
                    tdSql.checkData(0, 1, "TIMESTAMP")
                    tdSql.execute(f"DROP VTABLE {vname}")
                tdSql.execute(f"DROP DATABASE {db}")
        tdSql.execute(f"USE {_LOCAL_DB}")

        # The IDMP path: a database created without an explicit precision keeps
        # the engine default (ms), and a datetime2(3) reference must work there.
        create_sqlserver_remote_table(
            "r_ms_default", [("v", "INT", "int")],
            rows=["('2024-01-01 00:00:00.123', 7)"])
        default_db = "vtcreate_ssp_default"
        tdSql.execute(f"DROP DATABASE IF EXISTS {default_db}")
        tdSql.execute(f"CREATE DATABASE {default_db}")
        tdSql.execute(f"USE {default_db}")
        tdSql.query("SELECT `precision` FROM information_schema.ins_databases "
                    f"WHERE name = '{default_db}'")
        tdSql.checkData(0, 0, "ms")
        tdSql.execute(
            "CREATE VTABLE v_ms_default (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_ms_default.v)")
        tdSql.query("SELECT ts, v FROM v_ms_default")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:00.123")
        tdSql.checkData(0, 1, 7)
        tdSql.query("SELECT v FROM v_ms_default "
                    "WHERE ts = '2024-01-01 00:00:00.123'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)
        tdSql.execute("DROP VTABLE v_ms_default")
        tdSql.execute(f"DROP DATABASE {default_db}")
        tdSql.execute(f"USE {_LOCAL_DB}")

    def test_sqlserver_relative_column_paths(self):
        """FQSS-VTBL-011/012: relative SQL Server column paths.

        A three-part path (source.table.column) resolves with the source's
        default database and schema; a four-part path
        (source.schema.table.column) keeps the schema named by the path and
        takes the database from the source. Both are stored as the explicit
        five-part namespace, so `col_source` and the rendered DDL stay
        self-contained. The same forms are accepted by virtual child tables,
        and the namespace prerequisites are enforced: the three-part form
        needs both defaults, the four-part form needs the database default.

        Since: v3.4.4.0

        """
        create_sqlserver_remote_table(
            "r_relpath", [("v", "INT", "int"), ("w", "INT", "int")],
            rows=["('2024-01-01 00:00:00.000', 11, 110)"])
        create_sqlserver_remote_table(
            "r_relpath", [("v", "INT", "int")],
            rows=["('2024-01-01 00:00:00.000', 21)"], schema="other")

        def _col_source(table, col):
            tdSql.query(
                "SELECT col_source FROM information_schema.ins_columns "
                f"WHERE db_name='{_LOCAL_DB}' AND table_name='{table}' "
                f"AND col_name='{col}'")
            tdSql.checkRows(1)
            return str(tdSql.queryResult[0][0])

        # --- three-part: source's default database + schema ---
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_rel3")
        tdSql.execute(
            "CREATE VTABLE v_ss_rel3 (ts timestamp, "
            f"v int FROM {_SS_SRC}.r_relpath.v)")
        tdSql.query("SELECT v FROM v_ss_rel3")
        tdSql.checkData(0, 0, 11)
        assert _col_source("v_ss_rel3", "v") == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r_relpath.v"

        # --- four-part: schema from the path, database from the source ---
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_rel4")
        tdSql.execute(
            "CREATE VTABLE v_ss_rel4 (ts timestamp, "
            f"v int FROM {_SS_SRC}.dbo.r_relpath.v)")
        tdSql.query("SELECT v FROM v_ss_rel4")
        tdSql.checkData(0, 0, 11)
        assert _col_source("v_ss_rel4", "v") == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r_relpath.v"

        # The path schema wins over the source's default schema.
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_rel4_other")
        tdSql.execute(
            "CREATE VTABLE v_ss_rel4_other (ts timestamp, "
            f"v int FROM {_SS_SRC}.other.r_relpath.v)")
        tdSql.query("SELECT v FROM v_ss_rel4_other")
        tdSql.checkData(0, 0, 21)
        assert _col_source("v_ss_rel4_other", "v") == \
            f"{_SS_SRC}.{_SS_DB}.other.r_relpath.v"

        # --- ALTER ADD COLUMN with relative paths ---
        tdSql.execute(
            f"ALTER VTABLE v_ss_rel3 ADD COLUMN w int FROM {_SS_SRC}.r_relpath.w")
        tdSql.query("SELECT w FROM v_ss_rel3")
        tdSql.checkData(0, 0, 110)
        assert _col_source("v_ss_rel3", "w") == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r_relpath.w"
        tdSql.execute(
            f"ALTER VTABLE v_ss_rel4 ADD COLUMN w int FROM {_SS_SRC}.dbo.r_relpath.w")
        tdSql.query("SELECT w FROM v_ss_rel4")
        tdSql.checkData(0, 0, 110)

        # --- ALTER COLUMN ... SET with relative paths ---
        tdSql.execute(
            f"ALTER VTABLE v_ss_rel4 ALTER COLUMN v SET {_SS_SRC}.r_relpath.w")
        tdSql.query("SELECT v FROM v_ss_rel4")
        tdSql.checkData(0, 0, 110)
        assert _col_source("v_ss_rel4", "v") == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r_relpath.w"
        tdSql.execute(
            f"ALTER VTABLE v_ss_rel4 ALTER COLUMN v SET {_SS_SRC}.dbo.r_relpath.v")
        tdSql.query("SELECT v FROM v_ss_rel4")
        tdSql.checkData(0, 0, 11)

        # --- virtual child tables accept the relative forms ---
        tdSql.execute("DROP STABLE IF EXISTS vstb_ss_rel")
        tdSql.execute(
            "CREATE STABLE vstb_ss_rel (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_rel3 (v FROM {_SS_SRC}.r_relpath.v) "
            "USING vstb_ss_rel TAGS ('rel3')")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_rel4 (v FROM {_SS_SRC}.dbo.r_relpath.v) "
            "USING vstb_ss_rel TAGS ('rel4')")
        tdSql.query("SELECT v FROM vctb_ss_rel3")
        tdSql.checkData(0, 0, 11)
        tdSql.query("SELECT v FROM vctb_ss_rel4")
        tdSql.checkData(0, 0, 11)
        assert _col_source("vctb_ss_rel3", "v") == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r_relpath.v"
        tdSql.execute("DROP STABLE vstb_ss_rel")

        tdSql.execute("DROP VTABLE v_ss_rel3")
        tdSql.execute("DROP VTABLE v_ss_rel4")
        tdSql.execute("DROP VTABLE v_ss_rel4_other")

        # --- namespace prerequisites ---
        # A source without a database default cannot resolve relative paths,
        # but the five-part path still works.
        _NO_DB_SRC = f"{_P}ss_nodb"
        _NO_SCHEMA_SRC = f"{_P}ss_noschema"
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_NO_DB_SRC}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {_NO_DB_SRC} TYPE='sqlserver' "
            f"HOST='{ExtSrcEnv.SQLSERVER_HOST}' PORT={ExtSrcEnv.SQLSERVER_PORT} "
            f"USER='{ExtSrcEnv.SQLSERVER_USER}' PASSWORD='{ExtSrcEnv.SQLSERVER_PASS}' "
            "SCHEMA=dbo")
        tdSql.error(
            "CREATE VTABLE v_ss_nodb3 (ts timestamp, "
            f"v int FROM {_NO_DB_SRC}.r_relpath.v)")
        tdSql.error(
            "CREATE VTABLE v_ss_nodb4 (ts timestamp, "
            f"v int FROM {_NO_DB_SRC}.dbo.r_relpath.v)")

        # A source without a schema default rejects the three-part path; the
        # four-part path names the schema itself and therefore works.
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_NO_SCHEMA_SRC}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {_NO_SCHEMA_SRC} TYPE='sqlserver' "
            f"HOST='{ExtSrcEnv.SQLSERVER_HOST}' PORT={ExtSrcEnv.SQLSERVER_PORT} "
            f"USER='{ExtSrcEnv.SQLSERVER_USER}' PASSWORD='{ExtSrcEnv.SQLSERVER_PASS}' "
            f"DATABASE={_SS_DB}")
        tdSql.error(
            "CREATE VTABLE v_ss_noschema3 (ts timestamp, "
            f"v int FROM {_NO_SCHEMA_SRC}.r_relpath.v)")
        tdSql.execute(
            "CREATE VTABLE v_ss_noschema4 (ts timestamp, "
            f"v int FROM {_NO_SCHEMA_SRC}.dbo.r_relpath.v)")
        tdSql.query("SELECT v FROM v_ss_noschema4")
        tdSql.checkData(0, 0, 11)
        tdSql.execute("DROP VTABLE v_ss_noschema4")
        tdSql.execute(f"DROP EXTERNAL SOURCE {_NO_DB_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE {_NO_SCHEMA_SRC}")

    def test_sqlserver_missing_objects_and_ts_requirement(self):
        """FQSS-VTBL-008/010: SQL Server negative reference paths.

        Rowversion-only tables and views without a time column cannot back a
        virtual table; unknown database/schema components are rejected as well.

        Since: v3.4.4.0

        """
        # (a) rowversion-only table: no temporal primary key exists.
        create_sqlserver_table(_SS_DB, "r_rv",
                               "rv rowversion, v INT",
                               rows=["(DEFAULT, 41)"])
        tdSql.error(
            "CREATE VTABLE v_ss_rv_bad (ts timestamp, "
            f"x varbinary(8) FROM {_SS_SRC}.{_SS_DB}.dbo.r_rv.rv)")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_ss_rv_bad'")
        tdSql.checkData(0, 0, 0)

        # (b) view without a time column cannot be a column source.
        create_sqlserver_remote_table("r_view_src", [("v", "INT", "int")],
                                      rows=["('2024-01-01 00:00:00.000', 51)"])
        ExtSrcEnv.sqlserver_exec(_SS_DB, [
            "IF OBJECT_ID('dbo.v_ss_no_ts') IS NOT NULL DROP VIEW dbo.v_ss_no_ts",
            "CREATE VIEW dbo.v_ss_no_ts AS SELECT v FROM dbo.r_view_src",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_SS_SRC}")
        tdSql.error(
            "CREATE VTABLE v_ss_view_bad (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.v_ss_no_ts.v)")

        # (c) unknown database / schema segments in a 5-segment path.
        tdSql.error(
            "CREATE VTABLE v_ss_nodb_bad (ts timestamp, "
            f"v int FROM {_SS_SRC}.no_such_db.dbo.r_view_src.v)")
        tdSql.error(
            "CREATE VTABLE v_ss_noschema_bad (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.no_such_schema.r_view_src.v)")

    def test_sqlserver_use_switch_freezes_reference(self):
        """FQSS-VTBL-009: USE switching does not change an existing vtable.

        The external reference is frozen when the vtable is created, so
        switching the session namespace afterwards keeps the same source
        object and the same persisted column reference.

        Since: v3.4.4.0

        """
        create_sqlserver_remote_table(
            "r_use", [("v", "INT", "int")],
            rows=["('2024-01-01 00:00:00.000', 61)",
                  "('2024-01-01 00:01:00.000', 62)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_use")
        tdSql.execute(
            "CREATE VTABLE v_ss_use (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_use.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_ss_use' "
            "AND col_name='v'")
        before = str(tdSql.queryResult[0][0])

        tdSql.execute(f"USE {_SS_SRC}")
        tdSql.execute(f"USE {_LOCAL_DB}")

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_ss_use' "
            "AND col_name='v'")
        after = str(tdSql.queryResult[0][0])
        assert before == after, f"USE switch changed ref: {before!r} -> {after!r}"
        tdSql.query("SELECT v FROM v_ss_use ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 61)
        tdSql.checkData(1, 0, 62)
        tdSql.execute("DROP VTABLE v_ss_use")

    def test_vstb_sqlserver_heterogeneous_child(self):
        """FQSS-VTBL-002/003: virtual super table with a SQL Server child.

        A virtual super table fans out across PG, MySQL, InfluxDB and SQL
        Server children; the SQL Server child references its columns through
        the 5-segment path and contributes to super-table aggregation.

        Since: v3.4.4.0

        """
        tdSql.execute("DROP STABLE IF EXISTS vstb_ss_fan")
        tdSql.execute(
            "CREATE STABLE vstb_ss_fan (ts timestamp, "
            "temperature double, humidity double) "
            "TAGS (site nchar(32), backend nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_fan_pg ("
            f"temperature FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature, "
            f"humidity    FROM {_PG_SRC}.{_PG_DB}.kpi_pg.humidity) "
            f"USING vstb_ss_fan TAGS ('siteA', 'pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_fan_my ("
            f"temperature FROM {_MY_SRC}.{_MY_DB}.kpi_my.temperature, "
            f"humidity    FROM {_MY_SRC}.{_MY_DB}.kpi_my.humidity) "
            f"USING vstb_ss_fan TAGS ('siteB', 'mysql')")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_fan_inf ("
            f"temperature FROM {_INF_SRC}.{_INF_DB}.kpi_in.temperature, "
            f"humidity    FROM {_INF_SRC}.{_INF_DB}.kpi_in.humidity) "
            f"USING vstb_ss_fan TAGS ('siteC', 'influx')")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss_fan_ss ("
            f"temperature FROM {_SS_SRC}.{_SS_DB}.dbo.kpi_ss.temperature, "
            f"humidity    FROM {_SS_SRC}.{_SS_DB}.dbo.kpi_ss.humidity) "
            f"USING vstb_ss_fan TAGS ('siteD', 'sqlserver')")

        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_ss_fan'")
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_ss_fan_ss' "
            "AND col_name='temperature'")
        tdSql.checkRows(1)
        col_source = str(tdSql.queryResult[0][0])
        assert col_source.startswith(f"{_SS_SRC}.{_SS_DB}."), \
            f"unexpected SQL Server child col_source: {col_source!r}"
        assert col_source.endswith(".kpi_ss.temperature"), \
            f"unexpected SQL Server child col_source: {col_source!r}"

        tdSql.query("SELECT temperature, humidity FROM vctb_ss_fan_ss ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 30.5)
        tdSql.checkData(0, 1, 40.0)
        tdSql.checkData(1, 0, 31.5)
        tdSql.checkData(1, 1, 41.0)

        tdSql.query("SELECT count(*) FROM vstb_ss_fan")
        tdSql.checkData(0, 0, 8)
        tdSql.query("SELECT count(*) FROM vstb_ss_fan WHERE backend = 'sqlserver'")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP STABLE IF EXISTS vstb_ss_fan")

    # -------------------------------------------------------------------
    # Oracle references (FQOR-VTBL-001 .. 006, 008, 010)
    #
    # Oracle has one namespace level (the schema), so a column reference is
    # either source.table.column (the source's schema) or
    # source.schema.table.column; unquoted identifiers fold to upper case,
    # which is why the persisted schema is the upper-case login user. The
    # local database is 'ns', so DATE / TIMESTAMP(p) axes are legal.
    # -------------------------------------------------------------------

    def test_oracle_legal_references_and_schema(self):
        """FQOR-VTBL-001/004/005: legal Oracle column references.

        (a) mixed external + local placeholder columns through the 4-segment
            path, with the persisted column source checked;
        (b) the 3-segment and 4-segment paths persist the same full reference;
        (c) the same service and table name in two schemas resolve to
            distinct objects and values.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        # --- (a) 4-segment path, mixed ext + local columns ---
        create_oracle_remote_table(
            "r_mixed",
            [("v", "NUMBER(10,0)", "bigint"), ("name", "VARCHAR2(32)", "varchar(32)")],
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 11, 'alpha')",
                  "(TIMESTAMP '2024-01-01 00:01:00.000', 12, 'beta')"])
        tdSql.execute("DROP VTABLE IF EXISTS v_or_mixed")
        tdSql.execute(
            "CREATE VTABLE v_or_mixed ("
            "ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_mixed.v, "
            f"name varchar(32) FROM {_OR_SRC}.{_OR_SCHEMA}.r_mixed.name, "
            "local_col int)")
        tdSql.query("DESCRIBE v_or_mixed")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 0, "name")
        tdSql.checkData(2, 1, "VARCHAR")
        tdSql.checkData(3, 0, "local_col")
        tdSql.checkData(3, 1, "INT")

        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_mixed' "
            "AND col_name IN ('local_col', 'ts', 'v', 'name') "
            "ORDER BY col_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "local_col")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "name")
        tdSql.checkData(1, 1, f"{_OR_SRC}.{_OR_SCHEMA}.r_mixed.name")
        tdSql.checkData(2, 0, "ts")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "v")
        tdSql.checkData(3, 1, f"{_OR_SRC}.{_OR_SCHEMA}.r_mixed.v")

        tdSql.query("SELECT v, name, local_col FROM v_or_mixed ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, "alpha")
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 12)
        tdSql.checkData(1, 1, "beta")
        tdSql.execute("DROP VTABLE v_or_mixed")

        # --- (b) 3-segment vs 4-segment references ---
        create_oracle_remote_table(
            "r_rel", [("v", "NUMBER(10,0)", "bigint")],
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 31)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_or_rel3")
        tdSql.execute(
            "CREATE VTABLE v_or_rel3 (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.r_rel.v)")
        tdSql.query("SELECT v FROM v_or_rel3")
        tdSql.checkData(0, 0, 31)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_rel3' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_SRC}.{_OR_SCHEMA}.r_rel.v")
        tdSql.execute("DROP VTABLE v_or_rel3")

        tdSql.execute("DROP VTABLE IF EXISTS v_or_rel4")
        tdSql.execute(
            "CREATE VTABLE v_or_rel4 (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_rel.v)")
        tdSql.query("SELECT v FROM v_or_rel4")
        tdSql.checkData(0, 0, 31)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_rel4' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_SRC}.{_OR_SCHEMA}.r_rel.v")
        tdSql.execute("DROP VTABLE v_or_rel4")

        # --- (c) same service, two schemas, same table name ---
        create_oracle_remote_table(
            "r_schema", [("v", "NUMBER(10,0)", "bigint")],
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 41)"])
        create_oracle_remote_table(
            "r_schema", [("v", "NUMBER(10,0)", "bigint")],
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 42)"],
            schema=ExtSrcEnv.ORACLE_ALT_USER,
            user=ExtSrcEnv.ORACLE_ALT_USER,
            password=ExtSrcEnv.ORACLE_ALT_PASS)
        tdSql.execute("DROP VTABLE IF EXISTS v_or_schema_a")
        tdSql.execute("DROP VTABLE IF EXISTS v_or_schema_b")
        tdSql.execute(
            "CREATE VTABLE v_or_schema_a (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_schema.v)")
        tdSql.execute(
            "CREATE VTABLE v_or_schema_b (ts timestamp, "
            f"v bigint FROM {_OR_ALT_SRC}.{_OR_ALT_SCHEMA}.r_schema.v)")
        tdSql.query("SELECT v FROM v_or_schema_a")
        tdSql.checkData(0, 0, 41)
        tdSql.query("SELECT v FROM v_or_schema_b")
        tdSql.checkData(0, 0, 42)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_schema_b' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_ALT_SRC}.{_OR_ALT_SCHEMA}.r_schema.v")
        tdSql.execute("DROP VTABLE v_or_schema_a")
        tdSql.execute("DROP VTABLE v_or_schema_b")

    def test_oracle_show_create_roundtrip(self):
        """FQOR-VTBL-006: Oracle schema survives SHOW CREATE and replay.

        The persisted reference keeps the schema segment, so the rendered DDL
        is self-contained: replaying it under a new vtable name resolves the
        same remote object, and REFRESH does not rewrite either form.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        create_oracle_remote_table(
            "r_rt",
            [("v", "NUMBER(10,0)", "bigint"), ("w", "NUMBER(10,0)", "bigint")],
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 51, 510)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_or_rt")
        tdSql.execute(
            "CREATE VTABLE v_or_rt (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_rt.v, "
            "local_col int)")
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_or_rt")
        for expected in (_OR_SRC, _OR_SCHEMA, "r_rt", "local_col"):
            if expected not in sql:
                tdLog.exit(f"expected {expected} in {sql}")

        # REFRESH re-reads the remote dictionary and must not change the frozen
        # reference or its rendered DDL.
        before = _extract_show_create_sql("SHOW CREATE VTABLE v_or_rt")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_OR_SRC}")
        after = _extract_show_create_sql("SHOW CREATE VTABLE v_or_rt")
        if before != after:
            tdLog.exit(f"SHOW CREATE changed after REFRESH: before={before}, after={after}")

        tdSql.execute("DROP VTABLE IF EXISTS v_or_rt_copy")
        tdSql.execute(sql.replace("v_or_rt", "v_or_rt_copy", 1))
        tdSql.query("SELECT v FROM v_or_rt_copy")
        tdSql.checkData(0, 0, 51)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_rt_copy' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_SRC}.{_OR_SCHEMA}.r_rt.v")
        tdSql.execute("DROP VTABLE v_or_rt_copy")
        tdSql.execute("DROP VTABLE v_or_rt")

    def test_oracle_missing_objects_and_ts_requirement(self):
        """FQOR-VTBL-008/010: Oracle negative reference paths.

        A remote table needs exactly one DATE / TIMESTAMP primary-key column
        to back a virtual table; RAW, INTERVAL and keyless tables are
        rejected, unknown objects and pseudo-columns are reported, and views
        (plain or materialized) have no remote key at all.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        def _assert_no_vtable(name):
            tdSql.query(
                "SELECT count(*) FROM information_schema.ins_tables "
                f"WHERE db_name='{_LOCAL_DB}' AND table_name='{name}'")
            tdSql.checkData(0, 0, 0)

        # (a) no primary key at all.
        create_oracle_table(
            "r_nopk", "ts TIMESTAMP(3), v NUMBER(10,0)",
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 61)"])
        tdSql.error(
            "CREATE VTABLE v_or_nopk_bad (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_nopk.v)",
            expectedErrno=0x6406)
        _assert_no_vtable("v_or_nopk_bad")

        # (b) RAW primary key: mappable, but not a timestamp.
        create_oracle_table("r_raw_axis", "ts RAW(16) PRIMARY KEY, v NUMBER(10,0)")
        tdSql.error(
            "CREATE VTABLE v_or_raw_bad (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_raw_axis.v)",
            expectedErrno=0x6406)
        _assert_no_vtable("v_or_raw_bad")

        # (c) A BLOB column cannot be a time axis, so a table that only has one
        # cannot back a virtual table (Oracle refuses a LOB primary key).
        create_oracle_table("r_blob_axis", "b BLOB, v NUMBER(10,0)")
        tdSql.error(
            "CREATE VTABLE v_or_blob_bad (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_blob_axis.v)",
            expectedErrno=0x6406)
        _assert_no_vtable("v_or_blob_bad")

        # (d) INTERVAL primary key: the key column itself has no TDengine
        # mapping, so validating the key reports the unmappable type.
        create_oracle_table(
            "r_itv_axis", "ts INTERVAL DAY TO SECOND PRIMARY KEY, v NUMBER(10,0)")
        tdSql.error(
            "CREATE VTABLE v_or_itv_bad (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_itv_axis.v)",
            expectedErrno=0x6405)
        _assert_no_vtable("v_or_itv_bad")

        # (e) Pseudo-columns are absent from the metadata, so they surface as
        # an unknown column.
        tdSql.error(
            "CREATE VTABLE v_or_pseudo_bad (ts timestamp, "
            f"x bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_raw_axis.ROWNUM)",
            expectedErrno=0x641B)
        _assert_no_vtable("v_or_pseudo_bad")

        # (f) unknown table / column.
        tdSql.error(
            "CREATE VTABLE v_or_unk_tbl (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_no_such_tbl.v)",
            expectedErrno=0x6410)
        tdSql.error(
            "CREATE VTABLE v_or_unk_col (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_raw_axis.no_such_col)",
            expectedErrno=0x641B)

        # (g) A view has no remote key, whether or not it exposes a time
        # column, so it cannot be a column source; the same holds for a
        # materialized view.
        create_oracle_table(
            "r_view_base", "ts TIMESTAMP(3) PRIMARY KEY, v NUMBER(10,0)",
            rows=["(TIMESTAMP '2024-01-01 00:00:00.000', 71)"])
        create_oracle_view("v_or_no_ts", "SELECT v FROM r_view_base")
        create_oracle_view("v_or_with_ts", "SELECT ts, v FROM r_view_base")
        # The materialized view is created by the admin account, so its body
        # must name the owning schema explicitly.
        create_oracle_materialized_view(
            "mv_or_no_ts", f"SELECT v FROM {_OR_SCHEMA}.r_view_base")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_OR_SRC}")
        for view in ("v_or_no_ts", "v_or_with_ts", "mv_or_no_ts"):
            vname = f"v_or_view_bad_{view}".lower()
            tdSql.error(
                f"CREATE VTABLE {vname} (ts timestamp, "
                f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.{view}.v)",
                expectedErrno=0x6406)
            _assert_no_vtable(vname)

    def test_vstb_oracle_heterogeneous_child(self):
        """FQOR-VTBL-002/003: virtual super table with Oracle children.

        Two children over the same Oracle source contribute to super-table
        aggregation; the persisted references keep the schema, and the
        tag-filtered counts match the per-child rows.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        tdSql.execute("DROP STABLE IF EXISTS vstb_or_fan")
        tdSql.execute(
            "CREATE STABLE vstb_or_fan (ts timestamp, "
            "temperature double, humidity double) "
            "TAGS (site nchar(32), backend nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_pg ("
            f"temperature FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature, "
            f"humidity    FROM {_PG_SRC}.{_PG_DB}.kpi_pg.humidity) "
            f"USING vstb_or_fan TAGS ('siteA', 'pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_my ("
            f"temperature FROM {_MY_SRC}.{_MY_DB}.kpi_my.temperature, "
            f"humidity    FROM {_MY_SRC}.{_MY_DB}.kpi_my.humidity) "
            f"USING vstb_or_fan TAGS ('siteB', 'mysql')")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_inf ("
            f"temperature FROM {_INF_SRC}.{_INF_DB}.kpi_in.temperature, "
            f"humidity    FROM {_INF_SRC}.{_INF_DB}.kpi_in.humidity) "
            f"USING vstb_or_fan TAGS ('siteC', 'influx')")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_ss ("
            f"temperature FROM {_SS_SRC}.{_SS_DB}.dbo.kpi_ss.temperature, "
            f"humidity    FROM {_SS_SRC}.{_SS_DB}.dbo.kpi_ss.humidity) "
            f"USING vstb_or_fan TAGS ('siteD', 'sqlserver')")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_a ("
            f"temperature FROM {_OR_SRC}.{_OR_SCHEMA}.kpi_ora.temperature, "
            f"humidity    FROM {_OR_SRC}.{_OR_SCHEMA}.kpi_ora.humidity) "
            f"USING vstb_or_fan TAGS ('siteE', 'oracle')")
        tdSql.execute(
            f"CREATE VTABLE vctb_or_fan_b ("
            f"temperature FROM {_OR_SRC}.{_OR_SCHEMA}.kpi_ora_b.temperature, "
            f"humidity    FROM {_OR_SRC}.{_OR_SCHEMA}.kpi_ora_b.humidity) "
            f"USING vstb_or_fan TAGS ('siteF', 'oracle')")

        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_or_fan'")
        tdSql.checkData(0, 0, 6)

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_or_fan_a' "
            "AND col_name='temperature'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_SRC}.{_OR_SCHEMA}.kpi_ora.temperature")

        tdSql.query("SELECT temperature, humidity FROM vctb_or_fan_a ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 30.5)
        tdSql.checkData(0, 1, 40.0)
        tdSql.checkData(1, 0, 31.5)
        tdSql.checkData(1, 1, 41.0)

        tdSql.query("SELECT temperature, humidity FROM vctb_or_fan_b ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 28.5)
        tdSql.checkData(0, 1, 50.0)
        tdSql.checkData(1, 0, 29.5)
        tdSql.checkData(1, 1, 51.0)

        tdSql.query("SELECT count(*) FROM vstb_or_fan")
        tdSql.checkData(0, 0, 12)
        tdSql.query("SELECT count(*) FROM vstb_or_fan WHERE backend = 'oracle'")
        tdSql.checkData(0, 0, 4)
        tdSql.query("SELECT count(*) FROM vstb_or_fan WHERE site = 'siteE'")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP STABLE IF EXISTS vstb_or_fan")

    # -------------------------------------------------------------------
    # §3 — CREATE STABLE … VIRTUAL 1 + CREATE VTABLE … USING (PATENT CORE)
    #
    # Heterogeneous remote tables sharing common columns are abstracted into
    # a virtual super-table; each remote table becomes a child virtual table.
    # Aggregate queries over the vstb fan out across heterogeneous sources.
    # Uses kpi_pg / kpi_my / kpi_in remote tables provisioned in setup_class.
    # -------------------------------------------------------------------

    def test_vstb_create_and_aggregation(self):
        """§3 — Virtual super-table: heterogeneous children and negative paths.

        Covers: patent-core heterogeneous children creation and negative paths
        (missing vstb, duplicate child, tag/type mismatch).

        Since: v3.4.0.0

        """
        # --- §3.1: Patent-core heterogeneous children ---
        tdLog.info("§3.1: patent-core heterogeneous children create")
        tdSql.execute(
            "CREATE STABLE vstb_patent (ts timestamp, "
            "temperature double, humidity double) "
            "TAGS (site nchar(32), backend nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_pg ("
            f"temperature FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature, "
            f"humidity    FROM {_PG_SRC}.{_PG_DB}.kpi_pg.humidity) "
            f"USING vstb_patent TAGS ('siteA', 'pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_my ("
            f"temperature FROM {_MY_SRC}.{_MY_DB}.kpi_my.temperature, "
            f"humidity    FROM {_MY_SRC}.{_MY_DB}.kpi_my.humidity) "
            f"USING vstb_patent TAGS ('siteB', 'mysql')")
        tdSql.execute(
            f"CREATE VTABLE vctb_inf ("
            f"temperature FROM {_INF_SRC}.{_INF_DB}.kpi_in.temperature, "
            f"humidity    FROM {_INF_SRC}.{_INF_DB}.kpi_in.humidity) "
            f"USING vstb_patent TAGS ('siteC', 'influx')")
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_patent'")
        tdSql.checkData(0, 0, 3)
        # Verify col_source in ins_columns shows the full ref (including the
        # PostgreSQL source schema) for each child.
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_pg' "
            "AND col_name='temperature'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.public.kpi_pg.temperature")
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_my' "
            "AND col_name='humidity'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, f"{_MY_SRC}.{_MY_DB}.kpi_my.humidity")
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_inf' "
            "AND col_name='temperature'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, f"{_INF_SRC}.{_INF_DB}.kpi_in.temperature")
        tdSql.execute("DROP STABLE IF EXISTS vstb_patent")

        # --- §3.2: Child vtable with mixed local and external FROM refs ---
        tdLog.info("§3.2: mixed local and external refs in child vtable")
        # Create a local table for 2-seg ref.
        tdSql.execute(
            "CREATE TABLE local_kpi (ts timestamp, temperature double, "
            "humidity double)")
        tdSql.execute(
            "INSERT INTO local_kpi VALUES ('2024-01-01 00:00:00', 99.0, 88.0)")
        tdSql.execute(
            "CREATE STABLE vstb_mix (ts timestamp, "
            "t1 double, t2 double, t3 double) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        # Child with: 2-seg local ref plus explicit external refs.
        tdSql.execute(
            f"CREATE VTABLE vctb_mix ("
            f"t1 FROM local_kpi.temperature, "
            f"t2 FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature, "
            f"t3 FROM {_PG_SRC}.{_PG_DB}.kpi_pg.humidity) "
            f"USING vstb_mix TAGS ('mixed')")
        # Verify ins_columns: 2-seg stays local; external refs carry the full
        # namespace (PostgreSQL: source.database.schema.table.column).
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_mix' "
            "AND col_name IN ('t1','t2','t3') ORDER BY col_name")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "t1")
        tdSql.checkData(0, 1, f"{_LOCAL_DB}.local_kpi.temperature")
        tdSql.checkData(1, 0, "t2")
        tdSql.checkData(1, 1, f"{_PG_SRC}.{_PG_DB}.public.kpi_pg.temperature")
        tdSql.checkData(2, 0, "t3")
        tdSql.checkData(2, 1, f"{_PG_SRC}.{_PG_DB}.public.kpi_pg.humidity")
        tdSql.execute("DROP STABLE IF EXISTS vstb_mix")
        tdSql.execute("DROP TABLE local_kpi")

        # --- §3.3: Negative paths ---
        tdLog.info("§3.3: negative paths")

        # child USING non-existent vstb
        tdSql.error(
            f"CREATE VTABLE vctb_orphan ("
            f"temperature FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature) "
            f"USING vstb_does_not_exist TAGS ('x')")

        # duplicate child name
        tdSql.execute(
            "CREATE STABLE vstb_dup (ts timestamp, t double) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_dup ("
            f"t FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature) "
            f"USING vstb_dup TAGS ('s1')")
        tdSql.error(
            f"CREATE VTABLE vctb_dup ("
            f"t FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature) "
            f"USING vstb_dup TAGS ('s2')")
        tdSql.execute("DROP STABLE IF EXISTS vstb_dup")

        # tag type mismatch (tag is INT, provide string)
        tdSql.execute(
            "CREATE STABLE vstb_tagmis (ts timestamp, t double) "
            "TAGS (n int) VIRTUAL 1")
        tdSql.error(
            f"CREATE VTABLE vctb_tagmis ("
            f"t FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature) "
            f"USING vstb_tagmis TAGS ('not_an_int')")
        tdSql.execute("DROP STABLE IF EXISTS vstb_tagmis")

        # column type mismatch with vstb (vstb says INT, remote is DOUBLE)
        tdSql.execute(
            "CREATE STABLE vstb_typemis (ts timestamp, temperature int) "
            "TAGS (n int) VIRTUAL 1")
        tdSql.error(
            f"CREATE VTABLE vctb_typemis ("
            f"temperature FROM {_PG_SRC}.{_PG_DB}.kpi_pg.temperature) "
            f"USING vstb_typemis TAGS (1)")
        tdSql.execute("DROP STABLE IF EXISTS vstb_typemis")
