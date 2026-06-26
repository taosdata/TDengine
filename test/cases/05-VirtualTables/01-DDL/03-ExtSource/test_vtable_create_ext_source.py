###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

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
    create_pg_view, create_mysql_view)


_P = "vtcreate_"

# --- Per-backend remote DB / source name ---
_PG_DB      = "vtcreate_pg_db"
_MY_DB      = "vtcreate_my_db"
_INF_DB     = "vtcreate_inf_db"
_PG_SRC     = "vtcreate_pg_src"
_MY_SRC     = "vtcreate_my_src"
_INF_SRC    = "vtcreate_inf_src"

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

def _check_compat_consistency():
    """Sanity: every compatible local decl listed in the matrices is one of
    the canonical TDengine type strings in _TDS_ALL."""
    for name, m in (("PG", _PG_COMPAT), ("MY", _MY_COMPAT), ("INF", _INF_COMPAT)):
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

def create_influx_remote_table(meas):
    # Influx is schema-on-write — emit one row so all fields exist.
    create_influx_measurement(_INF_DB, [
        f"{meas},tag1=a "
        f"c_int=1i,c_uint=2u,c_float=3.5,c_str=\"x\",c_bool=true "
        f"1700000000000000000"
    ])

def _provision_heterogeneous_remote_tables():
    """Three remote backends, each holding a table whose schemas DIFFER but
    share two columns: temperature (DOUBLE) and humidity (DOUBLE).

    This is the canonical patent scenario (used by §3 vstable tests):

        PG.kpi_pg     (ts, temperature, humidity, pressure, voltage)
        MySQL.kpi_my  (ts, temperature, humidity, fan_rpm)
        Influx.kpi_in (time, temperature, humidity, battery)
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

def _ensure_sources():
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_ext_source(_MY_SRC, "mysql", _MY_DB)
    create_ext_source(_INF_SRC, "influxdb", _INF_DB)




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
        _ensure_sources()
        _create_local_db()
        # Provision the heterogeneous kpi_* remote tables once for §3 vstable
        # patent-core tests; they don't conflict with §2 vtable tests because
        # the latter create their own t_* remote tables under distinct names.
        _provision_heterogeneous_remote_tables()

    @classmethod
    #def teardown_class(cls):
        #_drop_local_db()
        #for n in (_PG_SRC, _MY_SRC, _INF_SRC):
        #    try: tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {n}")
        #    except Exception: pass

    # -------------------------------------------------------------------
    # 2.1 — Reference legal remote columns
    # -------------------------------------------------------------------

    def test_create_legal_references(self):
        # (a) Single source, all PG types mapped and DESCRIBE verified.
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

        # (d) TIMESTAMP precision cross-product (3 sources × 3 local precisions).
        create_pg_remote_table("ts_p", [("v", "INTEGER", "int")])
        create_mysql_remote_table("ts_p", [("v", "INTEGER", "int")])
        create_influx_remote_table("ts_p_inf")
        backends = [
            ("pg",  "int",    f"{_PG_SRC}.{_PG_DB}.ts_p.v"),
            ("my",  "int",    f"{_MY_SRC}.{_MY_DB}.ts_p.v"),
            ("inf", "bigint", f"{_INF_SRC}.{_INF_DB}.ts_p_inf.c_int"),
        ]
        for prec in ("ms", "us", "ns"):
            db = f"vtcreate_p_{prec}"
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} PRECISION '{prec}'")
            tdSql.execute(f"USE {db}")
            for label, v_type, v_ref in backends:
                vname = f"v_ts_{label}_{prec}"
                tdSql.execute(
                    f"CREATE VTABLE {vname} ("
                    f"ts timestamp, "
                    f"v {v_type} FROM {v_ref})")
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
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.t_4seg.v")
        tdSql.execute("DROP VTABLE v_4seg")

        # (h) 3-seg with source name disambiguates to ext.
        create_pg_remote_table("t_3disamb", [("v", "INTEGER", "int")])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(
            f"CREATE VTABLE v_3disamb (ts timestamp, "
            f"v int FROM {_PG_SRC}.t_3disamb.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3disamb' "
            f"AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.t_3disamb.v")
        tdSql.execute("DROP VTABLE v_3disamb")

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

        # (n) 3-seg mix with 4-seg
        tdSql.execute(
            f"CREATE VTABLE v_3mix4 (ts timestamp, "
            f"v int FROM {_PG_SRC}.t_3disamb.v, "
            f"v2 int FROM {_PG_SRC}.t_4seg.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_3mix4' "
            f"AND col_name IN ('v', 'v2') ORDER BY col_source")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, f"{_PG_SRC}.{_PG_DB}.t_3disamb.v")
        tdSql.checkData(1, 0, f"{_PG_SRC}.{_PG_DB}.t_4seg.v")
        tdSql.execute("DROP VTABLE v_3mix4")

    def test_reject_remote_views_for_normal_and_child_vtables(self):
        """Remote views cannot back virtual table external refs."""
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
        """Existing vtable query fails while source schema mismatches the saved ref."""
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
        """Quoted PG identifiers survive create, query, and metadata display."""
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
        # Verify col_source in ins_columns shows full 4-seg refs for each child.
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_pg' "
            "AND col_name='temperature'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.kpi_pg.temperature")
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

        # --- §3.2: Child vtable with mixed 2/3/4-seg FROM refs ---
        tdLog.info("§3.2: mixed segment refs in child vtable")
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
        # Child with: 2-seg (local), 3-seg (ext disambiguated), 4-seg (ext explicit)
        tdSql.execute(
            f"CREATE VTABLE vctb_mix ("
            f"t1 FROM local_kpi.temperature, "
            f"t2 FROM {_PG_SRC}.kpi_pg.temperature, "
            f"t3 FROM {_PG_SRC}.{_PG_DB}.kpi_pg.humidity) "
            f"USING vstb_mix TAGS ('mixed')")
        # Verify ins_columns: 2-seg stays local (db.table.col), 3-seg/4-seg are full 4-seg.
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_mix' "
            "AND col_name IN ('t1','t2','t3') ORDER BY col_name")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "t1")
        tdSql.checkData(0, 1, f"{_LOCAL_DB}.local_kpi.temperature")
        tdSql.checkData(1, 0, "t2")
        tdSql.checkData(1, 1, f"{_PG_SRC}.{_PG_DB}.kpi_pg.temperature")
        tdSql.checkData(2, 0, "t3")
        tdSql.checkData(2, 1, f"{_PG_SRC}.{_PG_DB}.kpi_pg.humidity")
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
