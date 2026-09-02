"""
test_fq_03_type_mapping.py

Simplified data-driven framework for federated query type mapping tests.
Covers FQ-TYPE-001 through FQ-TYPE-060 and supplementary S01–S32.

Original legacy tests are preserved in:
  test_fq_03_type_mapping.py.bak
"""

import datetime
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
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
    TSDB_CODE_PAR_INVALID_COL_JSON,
)

# ── Connection globals ─────────────────────────────────────────────────────
_M_HOST = ExtSrcEnv.MYSQL_HOST
_M_PORT = ExtSrcEnv.MYSQL_PORT
_M_USER = ExtSrcEnv.MYSQL_USER
_M_PASS = ExtSrcEnv.MYSQL_PASS
_M_DB   = "fq03_type_mdb"

_P_HOST   = ExtSrcEnv.PG_HOST
_P_PORT   = ExtSrcEnv.PG_PORT
_P_USER   = ExtSrcEnv.PG_USER
_P_PASS   = ExtSrcEnv.PG_PASS
_P_DB     = "fq03_type_pdb"
_P_SCHEMA = "public"

_I_HOST  = ExtSrcEnv.INFLUX_HOST
_I_PORT  = ExtSrcEnv.INFLUX_PORT
_I_TOKEN = ExtSrcEnv.INFLUX_TOKEN
_I_DB    = "fq03_type_idb"

_DYNAMIC_RESULT_COLUMNS = {"create_time", "ctime"}

# ── Type shortcuts ─────────────────────────────────────────────────────────
_mysql    = "mysql"
_pg       = "postgresql"
_influxdb = "influxdb"

# ── Source names ───────────────────────────────────────────────────────────
_SRC_M = "fq03_src_m"
_SRC_P = "fq03_src_p"
_SRC_I = "fq03_src_i"

# ── Step sentinel classes ──────────────────────────────────────────────────

class _ClearSourceStep:
    """DROP all sources listed in the current case's source_names."""
    pass


class _ExpectErrorStep:
    """Execute SQL and assert it fails with a specific error code."""
    def __init__(self, sql: str, errno, err_info: Optional[str] = None):
        self.sql      = sql
        self.errno    = errno
        self.err_info = err_info


def _clear_source_step() -> _ClearSourceStep:
    return _ClearSourceStep()


def _expect_error(sql: str, errno, err_info: Optional[str] = None) -> _ExpectErrorStep:
    return _ExpectErrorStep(sql, errno, err_info)


# ── MySQL side-effect step ─────────────────────────────────────────────────

def _mysql_exec_step(sqls: Union[str, Sequence[str]], marker_sql: str = "select 1"):
    """Run side-effect SQL directly on MySQL and return a marker query step."""
    if isinstance(sqls, str):
        statements = [sqls]
    else:
        statements = [str(s) for s in sqls]

    def _step(src_type: str):
        ExtSrcEnv.mysql_exec(_M_DB, statements)
        return marker_sql

    return _step


def _pg_exec_step(sqls: Union[str, Sequence[str]], marker_sql: str = "select 1"):
    """Run side-effect SQL directly on PostgreSQL and return a marker query step."""
    if isinstance(sqls, str):
        statements = [sqls]
    else:
        statements = [str(s) for s in sqls]

    def _step(src_type: str):
        ExtSrcEnv.pg_exec(_P_DB, statements)
        return marker_sql

    return _step


# ── Helper: detect MySQL version for unmappable type selection ─────────────

def _mysql_version_ge_9():
    """Return True if the connected MySQL version is >= 9.0."""
    try:
        ver_str = ExtSrcEnv.mysql_query_cfg(
            {"host": _M_HOST, "port": _M_PORT, "user": _M_USER, "password": _M_PASS},
            "mysql", "SELECT VERSION()"
        )
        m = re.match(r"(\d+)\.(\d+)", str(ver_str or ""))
        return m and (int(m.group(1)), int(m.group(2))) >= (9, 0)
    except Exception:
        return False


def _mysql_vector_test_step(src_type):
    """Callable step: MySQL VECTOR type test (S17 VECTOR branch).

    If MySQL >= 9.0, creates vector_type_test table with VECTOR(3),
    verifies known-type columns work and VECTOR column is unmappable.
    If MySQL < 9.0, returns empty list (MULTILINESTRING already tested).
    """
    if not _mysql_version_ge_9():
        # MySQL < 9.0: VECTOR not available; MULTILINESTRING tested separately
        return []
    # MySQL 9.0+: prepare VECTOR(3) table
    cfg = {"host": _M_HOST, "port": _M_PORT, "user": _M_USER, "password": _M_PASS}
    ExtSrcEnv.mysql_exec_cfg(cfg, _M_DB, [
        "DROP TABLE IF EXISTS vector_type_test",
        "CREATE TABLE vector_type_test ("
        "  ts  DATETIME(3) NOT NULL,"
        "  val INT,"
        "  emb VECTOR(3),"
        "  PRIMARY KEY (ts))",
        "INSERT INTO vector_type_test VALUES "
        "('2024-01-01 00:00:00.000', 7, TO_VECTOR('[1.0, 2.0, 3.0]'))",
    ])
    return [
        f"select ts, val from {_SRC_M}.vector_type_test",
        _expect_error(
            f"select emb from {_SRC_M}.vector_type_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),
    ]


# ══════════════════════════════════════════════════════════════════════════
# Data preparation SQL
# ══════════════════════════════════════════════════════════════════════════

def _mysql_setup_sqls():
    """Return list of SQL statements to create all MySQL test tables."""
    sqls = []

    # ── all_types: main type coverage table ──
    sqls.append("DROP TABLE IF EXISTS all_types")
    sqls.append(
        "CREATE TABLE all_types ("
        "  ts             DATETIME(6)       PRIMARY KEY,"
        # integers
        "  c_tinyint      TINYINT,"
        "  c_tinyint_u    TINYINT UNSIGNED,"
        "  c_smallint     SMALLINT,"
        "  c_smallint_u   SMALLINT UNSIGNED,"
        "  c_mediumint    MEDIUMINT,"
        "  c_mediumint_u  MEDIUMINT UNSIGNED,"
        "  c_int          INT,"
        "  c_int_u        INT UNSIGNED,"
        "  c_bigint       BIGINT,"
        "  c_bigint_u     BIGINT UNSIGNED,"
        # float/decimal
        "  c_float        FLOAT,"
        "  c_double       DOUBLE,"
        "  c_decimal      DECIMAL(18,4),"
        "  c_decimal_big  DECIMAL(38,10),"
        "  c_decimal_trunc DECIMAL(65,30),"
        # boolean
        "  c_bool         BOOLEAN,"
        # strings utf8mb4
        "  c_char         CHAR(20),"
        "  c_varchar      VARCHAR(100),"
        "  c_tinytext     TINYTEXT,"
        "  c_text         TEXT,"
        "  c_mediumtext   MEDIUMTEXT,"
        "  c_longtext     LONGTEXT,"
        # binary
        "  c_binary       BINARY(16),"
        "  c_varbinary    VARBINARY(100),"
        "  c_tinyblob     TINYBLOB,"
        "  c_blob         BLOB,"
        "  c_mediumblob   MEDIUMBLOB,"
        "  c_longblob     LONGBLOB,"
        # datetime
        "  c_date         DATE,"
        "  c_time         TIME,"
        "  c_datetime     DATETIME,"
        "  c_timestamp    TIMESTAMP NULL,"
        "  c_year         YEAR,"
        # json
        "  c_json         JSON,"
        # enum/set
        "  c_enum         ENUM('red','green','blue'),"
        "  c_set          SET('a','b','c','d'),"
        # geometry
        "  c_geometry     GEOMETRY,"
        "  c_point        POINT"
        ") CHARACTER SET utf8mb4"
    )
    # Row 1: normal values
    sqls.append(
        "INSERT INTO all_types VALUES ("
        "'2024-01-01 00:00:00.123456',"
        " -128, 0, -32768, 0, -8388608, 0,"
        " 42, 100, -9223372036854775808, 0,"
        " 3.14, 2.718281828, 12345.6789, 1234567890.1234567890,"
        " 123456789012345678.123456789012345678,"
        " TRUE,"
        " 'hello', '中文测试UTF8', 'tiny', 'text content',"
        " 'medium text', 'long text',"
        " X'48454C4C4F0000000000000000000000', X'DEADBEEF',"
        " X'AA', X'BBCC', X'DDEEFF', X'112233',"
        " '2024-06-15', '10:30:00', '2024-01-01 12:00:00',"
        " '2024-06-15 12:30:00', 2024,"
        " '{\"name\":\"test\",\"value\":42}',"
        " 'red', 'a,b',"
        " ST_GeomFromText('POINT(116.39 39.91)'),"
        " ST_PointFromText('POINT(121.47 31.23)')"
        ")"
    )
    # Row 2: boundary values
    sqls.append(
        "INSERT INTO all_types VALUES ("
        "'2024-01-01 00:01:00.000000',"
        " 127, 255, 32767, 65535, 8388607, 16777215,"
        " 2147483647, 4294967295, 9223372036854775807, 18446744073709551615,"
        " -3.14, -2.718281828, -99999999999999.9999, -9999999999999999999999999999.9999999999,"
        " -123456789012345678.123456789012345678,"
        " FALSE,"
        " '', '', '', '', '', '',"
        " X'00000000000000000000000000000000', X'',"
        " X'', X'', X'', X'',"
        " '2023-12-31', '00:00:01', '2024-01-02 00:00:00',"
        " '2024-01-02 00:00:00', 1901,"
        " '[]',"
        " 'blue', 'a,b,c,d',"
        " ST_GeomFromText('LINESTRING(0 0, 1 1)'),"
        " ST_PointFromText('POINT(0 0)')"
        ")"
    )
    # Row 3: all NULL except PK
    sqls.append(
        "INSERT INTO all_types (ts) VALUES ('2024-01-01 00:02:00.000000')"
    )

    # ── ascii_types: ASCII charset branch ──
    sqls.append("DROP TABLE IF EXISTS ascii_types")
    sqls.append(
        "CREATE TABLE ascii_types ("
        "  ts              DATETIME PRIMARY KEY,"
        "  c_char_asc      CHAR(20) CHARACTER SET ascii,"
        "  c_varchar_asc   VARCHAR(100) CHARACTER SET ascii,"
        "  c_tinytext_asc  TINYTEXT CHARACTER SET ascii,"
        "  c_text_asc      TEXT CHARACTER SET ascii,"
        "  c_enum_asc      ENUM('x','y','z') CHARACTER SET ascii,"
        "  c_set_asc       SET('p','q','r') CHARACTER SET ascii"
        ") CHARACTER SET ascii"
    )
    sqls.append(
        "INSERT INTO ascii_types VALUES ("
        "'2024-01-01 00:00:00', 'ascii_char', 'ascii_varchar',"
        " 'ascii_tiny', 'ascii_text', 'x', 'p,q')"
    )

    # ── special_types: BIT, NCHAR, NVARCHAR, type aliases ──
    sqls.append("DROP TABLE IF EXISTS special_types")
    sqls.append(
        "CREATE TABLE special_types ("
        "  ts            DATETIME PRIMARY KEY,"
        "  c_bit1        BIT(1),"
        "  c_bit8        BIT(8),"
        "  c_bit64       BIT(64),"
        "  c_nchar       NCHAR(20),"
        "  c_nvarchar    NVARCHAR(50),"
        "  c_dbl_prec    DOUBLE PRECISION,"
        "  c_real        REAL,"
        "  c_integer     INTEGER,"
        "  c_integer_u   INTEGER UNSIGNED"
        ")"
    )
    sqls.append(
        "INSERT INTO special_types VALUES ("
        "'2024-01-01 00:00:00',"
        " b'1', b'10101010', b'1111111111111111111111111111111111111111111111111111111111111111',"
        " '中文NCHAR', '日本語NVarchar',"
        " 3.14159, 2.71828, -2147483648, 4294967295)"
    )
    sqls.append(
        "INSERT INTO special_types VALUES ("
        "'2024-01-02 00:00:00',"
        " b'0', b'00000001', b'0000000000000000000000000000000000000000000000000000000000000001',"
        " 'abc', 'def',"
        " -1.5, 0.0, 2147483647, 0)"
    )

    # ── ts PK variants ──
    sqls.append("DROP TABLE IF EXISTS ts_pk_timestamp")
    sqls.append(
        "CREATE TABLE ts_pk_timestamp ("
        "  ts TIMESTAMP PRIMARY KEY, val INT)"
    )
    sqls.append(
        "INSERT INTO ts_pk_timestamp VALUES ('2024-01-01 00:00:00', 1)"
    )

    sqls.append("DROP TABLE IF EXISTS multi_ts")
    sqls.append(
        "CREATE TABLE multi_ts ("
        "  ts_pk DATETIME PRIMARY KEY, ts_extra DATETIME, val INT)"
    )
    sqls.append(
        "INSERT INTO multi_ts VALUES ("
        "'2024-01-01 00:00:00', '2024-06-15 12:00:00', 42)"
    )

    sqls.append("DROP TABLE IF EXISTS no_ts_table")
    sqls.append(
        "CREATE TABLE no_ts_table (id INT PRIMARY KEY, val INT)"
    )
    sqls.append("INSERT INTO no_ts_table VALUES (1, 100), (2, 200)")

    # ── views ──
    sqls.append("DROP VIEW IF EXISTS v_no_ts")
    sqls.append("DROP VIEW IF EXISTS v_with_ts")
    sqls.append("DROP VIEW IF EXISTS v_mixed")
    sqls.append("DROP TABLE IF EXISTS view_base")
    sqls.append(
        "CREATE TABLE view_base ("
        "  ts DATETIME PRIMARY KEY,"
        "  id INT, val INT,"
        "  c_int INT, c_str VARCHAR(50), c_bool BOOLEAN)"
    )
    sqls.append(
        "INSERT INTO view_base VALUES "
        "('2024-01-01 00:00:00', 1, 10, 42, 'test', TRUE),"
        "('2024-01-02 00:00:00', 2, 20, -1, 'hello', FALSE)"
    )
    sqls.append("CREATE VIEW v_no_ts AS SELECT id, val FROM view_base")
    sqls.append("CREATE VIEW v_with_ts AS SELECT ts, id, val FROM view_base")
    sqls.append("CREATE VIEW v_mixed AS SELECT ts, c_int, c_str, c_bool FROM view_base")

    # ── blob_overflow: LONGBLOB boundary ──
    sqls.append("DROP TABLE IF EXISTS blob_overflow")
    sqls.append(
        "CREATE TABLE blob_overflow ("
        "  ts DATETIME PRIMARY KEY, data LONGBLOB, val INT)"
    )
    small_hex = 'AA' * 100
    sqls.append(
        f"INSERT INTO blob_overflow VALUES ("
        f"'2024-01-01 00:00:00', X'{small_hex}', 1)"
    )

    # ── unmappable type table (MySQL MULTILINESTRING / VECTOR) ──
    sqls.append("DROP TABLE IF EXISTS unmappable_test")
    # Use MULTILINESTRING as default; MySQL < 9.0 has no VECTOR.
    # MySQL >= 9.0 VECTOR is tested separately via a callable step.
    sqls.append(
        "CREATE TABLE unmappable_test ("
        "  ts   DATETIME(3) PRIMARY KEY,"
        "  val  INT,"
        "  shape MULTILINESTRING)"
    )
    sqls.append(
        "INSERT INTO unmappable_test VALUES ("
        "'2024-01-01 00:00:00.000', 7,"
        " ST_GeomFromText('MULTILINESTRING((0 0, 1 1),(2 2, 3 3))'))"
    )

    # ── JSON operator test table ──
    sqls.append("DROP TABLE IF EXISTS json_op_test")
    sqls.append(
        "CREATE TABLE json_op_test ("
        "  ts DATETIME PRIMARY KEY,"
        "  doc JSON,"
        "  val INT)"
    )
    sqls.append(
        """INSERT INTO json_op_test VALUES ('2024-01-01 00:00:00', '{"k":"v"}', 1)"""
    )

    # ── geometric aliases (POLYGON / LINESTRING) ──
    sqls.append("DROP TABLE IF EXISTS geo_alias_test")
    sqls.append(
        "CREATE TABLE geo_alias_test ("
        "  ts    DATETIME PRIMARY KEY,"
        "  poly  POLYGON,"
        "  lstr  LINESTRING,"
        "  val   INT)"
    )
    sqls.append(
        "INSERT INTO geo_alias_test VALUES ("
        "'2024-01-01 00:00:00',"
        " ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),"
        " ST_GeomFromText('LINESTRING(0 0, 1 1, 2 0)'),"
        " 1)"
    )

    # ── SET multi-value combination ──
    sqls.append("DROP TABLE IF EXISTS set_combo")
    sqls.append(
        "CREATE TABLE set_combo ("
        "  ts   DATETIME PRIMARY KEY,"
        "  tag_val SET('a','b','c','d'))"
    )
    sqls.append(
        "INSERT INTO set_combo VALUES "
        "('2024-01-01 00:00:00', 'a'),"
        "('2024-01-02 00:00:00', 'a,b'),"
        "('2024-01-03 00:00:00', 'a,b,c,d')"
    )

    # ── TEXT charset variants ──
    sqls.append("DROP TABLE IF EXISTS text_charset")
    sqls.append(
        "CREATE TABLE text_charset ("
        "  ts       DATETIME PRIMARY KEY,"
        "  c_text   TEXT CHARACTER SET utf8mb4,"
        "  c_mtext  MEDIUMTEXT CHARACTER SET utf8mb4,"
        "  c_ltext  LONGTEXT CHARACTER SET utf8mb4,"
        "  c_ttext  TINYTEXT CHARACTER SET utf8mb4)"
    )
    sqls.append(
        "INSERT INTO text_charset VALUES ("
        "'2024-01-01 00:00:00', '中文text', '中文mtext', '中文ltext', '中文ttext')"
    )

    return sqls


def _pg_setup_sqls():
    """Return list of SQL statements to create all PG test tables."""
    sqls = []

    # ── install extensions ──
    sqls.append("CREATE EXTENSION IF NOT EXISTS hstore")
    sqls.append("CREATE EXTENSION IF NOT EXISTS postgis")

    # ── create user-defined types ──
    sqls.append("DROP TABLE IF EXISTS udt_type_test")
    sqls.append("DROP TABLE IF EXISTS domain_type_test")
    sqls.append("DROP TABLE IF EXISTS custom_range_test")
    sqls.append("DROP TYPE IF EXISTS mood_enum CASCADE")
    sqls.append("DROP TYPE IF EXISTS my_point CASCADE")
    sqls.append("DROP DOMAIN IF EXISTS positive_int CASCADE")
    sqls.append("DROP TYPE IF EXISTS float8range_custom CASCADE")
    sqls.append("CREATE TYPE mood_enum AS ENUM ('happy', 'sad', 'neutral')")
    sqls.append("CREATE TYPE my_point AS (x DOUBLE PRECISION, y DOUBLE PRECISION)")
    sqls.append("CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)")
    sqls.append("CREATE TYPE float8range_custom AS RANGE (subtype = float8)")

    # ── all_types: main type coverage table ──
    sqls.append("DROP TABLE IF EXISTS all_types")
    sqls.append(
        "CREATE TABLE all_types ("
        "  ts              TIMESTAMP       PRIMARY KEY,"
        # integers
        "  c_smallint      SMALLINT,"
        "  c_integer       INTEGER,"
        "  c_bigint        BIGINT,"
        # float
        "  c_real          REAL,"
        "  c_double_prec   DOUBLE PRECISION,"
        "  c_numeric       NUMERIC(18,4),"
        "  c_numeric_big   NUMERIC(38,10),"
        # boolean
        "  c_boolean       BOOLEAN,"
        # strings
        "  c_char          CHAR(20),"
        "  c_varchar       VARCHAR(100),"
        "  c_text          TEXT,"
        # binary
        "  c_bytea         BYTEA,"
        # datetime
        "  c_date          DATE,"
        "  c_time          TIME,"
        "  c_timetz        TIMETZ,"
        "  c_timestamp     TIMESTAMP,"
        "  c_timestamptz   TIMESTAMPTZ,"
        "  c_interval      INTERVAL,"
        # money
        "  c_money         MONEY,"
        # network
        "  c_inet          INET,"
        "  c_cidr          CIDR,"
        "  c_macaddr       MACADDR,"
        "  c_macaddr8      MACADDR8,"
        # uuid
        "  c_uuid          UUID,"
        # text search
        "  c_tsvector      TSVECTOR,"
        "  c_tsquery       TSQUERY,"
        # json
        "  c_json          JSON,"
        "  c_jsonb         JSONB,"
        # xml
        "  c_xml           XML,"
        # native geometry
        "  c_point         POINT,"
        # bit string
        "  c_bit           BIT(8),"
        "  c_bit_varying   BIT VARYING(64),"
        # hstore
        "  c_hstore        HSTORE,"
        # PostGIS
        "  c_geom          GEOMETRY(POINT, 4326),"
        # enum UDT
        "  c_mood          mood_enum,"
        # type aliases
        "  c_float4        FLOAT4,"
        "  c_float8        FLOAT8,"
        "  c_int2          INT2,"
        "  c_int4          INT4,"
        "  c_int8          INT8,"
        # character aliases
        "  c_character     CHARACTER(10),"
        "  c_char_varying  CHARACTER VARYING(50)"
        ")"
    )
    # Row 1: normal values
    sqls.append(
        "INSERT INTO all_types VALUES ("
        "'2024-01-01 00:00:00',"
        " -32768, 42, -9223372036854775808,"
        " 3.14, 2.718281828, 12345.6789, 1234567890.1234567890,"
        " TRUE,"
        " 'hello', '中文测试UTF8', 'text content',"
        " E'\\\\xDEADBEEF',"
        " '2024-06-15', '10:30:00', '13:45:30+00',"
        " '2024-01-01 12:00:00', '2024-06-15 12:30:00+00',"
        " '1 year 2 months 3 days',"
        " '$12.34',"
        " '192.168.1.1', '10.0.0.0/8',"
        " '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',"
        " 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',"
        " 'the quick brown fox'::tsvector, 'fox & dog'::tsquery,"
        " '{\"name\":\"test\",\"value\":42}', '{\"key\":\"val\"}',"
        " '<root><item>1</item></root>'::xml,"
        " '(116.39, 39.91)'::point,"
        " B'10101010', B'110011',"
        " '\"key\"=>\"value\"'::hstore,"
        " ST_SetSRID(ST_MakePoint(116.39, 39.91), 4326),"
        " 'happy',"
        " 1.5, 2.718281828, -32768, -2147483648, -9223372036854775808,"
        " 'char10', 'varying50'"
        ")"
    )
    # Row 2: boundary values
    sqls.append(
        "INSERT INTO all_types VALUES ("
        "'2024-01-01 00:01:00',"
        " 32767, 2147483647, 9223372036854775807,"
        " -3.14, -2.718281828, -99999999999999.9999, -9999999999999999999999999999.9999999999,"
        " FALSE,"
        " '', '', '',"
        " E'\\\\x00',"
        " '2023-12-31', '00:00:01', '00:00:01+08',"
        " '2024-01-02 00:00:00', '2024-01-02 00:00:00+08',"
        " '0 seconds',"
        " '$0.00',"
        " '::1', '::0/0',"
        " 'ff:ff:ff:ff:ff:ff', 'ff:ff:ff:ff:ff:ff:ff:ff',"
        " '00000000-0000-0000-0000-000000000000',"
        " ''::tsvector, ''::tsquery,"
        " '[]', '{}',"
        " '<empty/>'::xml,"
        " '(0, 0)'::point,"
        " B'00000001', B'1',"
        " '\"a\"=>\"b\"'::hstore,"
        " ST_SetSRID(ST_MakePoint(0, 0), 4326),"
        " 'sad',"
        " -0.5, 0.0, 32767, 2147483647, 9223372036854775807,"
        " 'x', 'y'"
        ")"
    )
    # Row 3: all NULL except PK
    sqls.append(
        "INSERT INTO all_types (ts) VALUES ('2024-01-01 00:02:00')"
    )

    # ── degrade_types: arrays, ranges, native geometry ──
    sqls.append("DROP TABLE IF EXISTS degrade_types")
    sqls.append(
        "CREATE TABLE degrade_types ("
        "  ts          TIMESTAMP PRIMARY KEY,"
        "  c_int_arr   INTEGER[],"
        "  c_text_arr  TEXT[],"
        "  c_int4range INT4RANGE,"
        "  c_tsrange   TSRANGE,"
        "  c_path      PATH,"
        "  c_polygon   POLYGON)"
    )
    sqls.append(
        "INSERT INTO degrade_types VALUES ("
        "'2024-01-01 00:00:00',"
        " '{1,2,3}', '{\"hello\",\"world\"}',"
        " '[1,5)', '[2024-01-01 00:00:00, 2024-01-02 00:00:00)',"
        " '((0,0),(1,1),(2,0))'::path,"
        " '((0,0),(1,1),(2,0),(0,0))'::polygon)"
    )

    # ── UDT: composite type ──
    sqls.append(
        "CREATE TABLE udt_type_test ("
        "  ts   TIMESTAMP   PRIMARY KEY,"
        "  val  INT,"
        "  loc  my_point)"
    )
    sqls.append(
        "INSERT INTO udt_type_test VALUES ("
        "'2024-01-01 00:00:00', 99, ROW(1.0, 2.0)::my_point)"
    )

    # ── DOMAIN ──
    sqls.append(
        "CREATE TABLE domain_type_test ("
        "  ts    TIMESTAMP    PRIMARY KEY,"
        "  val   INT,"
        "  score positive_int)"
    )
    sqls.append(
        "INSERT INTO domain_type_test VALUES ("
        "'2024-01-01 00:00:00', 42, 10)"
    )

    # ── user-defined RANGE ──
    sqls.append(
        "CREATE TABLE custom_range_test ("
        "  ts   TIMESTAMP          PRIMARY KEY,"
        "  val  INT,"
        "  rng  float8range_custom)"
    )
    sqls.append(
        "INSERT INTO custom_range_test VALUES ("
        "'2024-01-01 00:00:00', 7,"
        " '[1.5,3.14)'::float8range_custom)"
    )

    # ── ts PK variants ──
    sqls.append("DROP TABLE IF EXISTS ts_pk_tstz")
    sqls.append(
        "CREATE TABLE ts_pk_tstz ("
        "  ts TIMESTAMPTZ PRIMARY KEY, val INT)"
    )
    sqls.append(
        "INSERT INTO ts_pk_tstz VALUES ('2024-01-01 00:00:00+00', 1)"
    )

    sqls.append("DROP TABLE IF EXISTS no_ts_table")
    sqls.append(
        "CREATE TABLE no_ts_table (id SERIAL PRIMARY KEY, val INT)"
    )
    sqls.append("INSERT INTO no_ts_table (val) VALUES (100), (200)")

    # ── serial types ──
    sqls.append("DROP TABLE IF EXISTS serial_types")
    sqls.append(
        "CREATE TABLE serial_types ("
        "  ts           TIMESTAMP PRIMARY KEY,"
        "  c_smallserial SMALLSERIAL,"
        "  c_serial     SERIAL,"
        "  c_bigserial  BIGSERIAL)"
    )
    sqls.append(
        "INSERT INTO serial_types (ts) VALUES ('2024-01-01 00:00:00')"
    )

    # ── views ──
    sqls.append("DROP VIEW IF EXISTS v_no_ts")
    sqls.append("DROP TABLE IF EXISTS view_base")
    sqls.append(
        "CREATE TABLE view_base ("
        "  ts TIMESTAMP PRIMARY KEY, id INT, val INT)"
    )
    sqls.append(
        "INSERT INTO view_base VALUES "
        "('2024-01-01 00:00:00', 1, 10),"
        "('2024-01-02 00:00:00', 2, 20)"
    )
    sqls.append("CREATE VIEW v_no_ts AS SELECT id, val FROM view_base")

    # ── JSON operator test table ──
    sqls.append("DROP TABLE IF EXISTS json_op_test")
    sqls.append(
        "CREATE TABLE json_op_test ("
        "  ts TIMESTAMP PRIMARY KEY,"
        "  doc_json json,"
        "  doc_jsonb jsonb,"
        "  val INT)"
    )
    sqls.append(
        """INSERT INTO json_op_test VALUES ('2024-01-01 00:00:00', '{"k":"v"}', '{"k":"v"}', 1)"""
    )

    # ── timestamptz multi-timezone ──
    sqls.append("DROP TABLE IF EXISTS tstz_multi")
    sqls.append(
        "CREATE TABLE tstz_multi ("
        "  ts TIMESTAMP PRIMARY KEY,"
        "  c_tstz TIMESTAMPTZ)"
    )
    sqls.append(
        "INSERT INTO tstz_multi VALUES "
        "('2024-01-01 00:00:00', '2024-06-15 12:00:00+00'),"
        "('2024-01-01 00:01:00', '2024-06-15 20:00:00+08'),"
        "('2024-01-01 00:02:00', '2024-06-15 07:00:00-05')"
    )

    # ── timetz + long-form timestamp variants ──
    sqls.append("DROP TABLE IF EXISTS ts_variants")
    sqls.append(
        "CREATE TABLE ts_variants ("
        "  ts      TIMESTAMP PRIMARY KEY,"
        "  c_ttz   TIME WITH TIME ZONE,"
        "  c_tstz  TIMESTAMP WITH TIME ZONE,"
        "  c_tsno  TIMESTAMP WITHOUT TIME ZONE)"
    )
    sqls.append(
        "INSERT INTO ts_variants VALUES ("
        "'2024-01-01 00:00:00',"
        " '13:45:30+00'::timetz,"
        " '2024-06-15 12:00:00+00'::timestamptz,"
        " '2024-06-15 15:30:00'::timestamp)"
    )

    return sqls


def _influx_setup_lines():
    """Return (lines, precision) tuples for InfluxDB test data."""
    ns_lines = [
        # scalar_types: int, float, bool, string + tags (Dictionary-encoded)
        'scalar_types,host=srv01,region=east f_int=42i,f_float=3.14,f_bool=true,f_str="hello" 1704067200000000000',
        'scalar_types,host=srv02,region=west f_int=-100i,f_float=-0.5,f_bool=false,f_str="world" 1704067260000000000',
        'scalar_types,host=srv03,region=east f_int=0i,f_float=0.0,f_bool=true,f_str="" 1704067320000000000',
        # complex_test: JSON-like string simulating Struct/Map (050)
        'complex_test,host=s1 data="[1,2,3]",meta="{\\\"key\\\":\\\"val\\\"}" 1704067200000000000',
        # time_of_day: Time32/64 → BIGINT (060)
        'time_of_day,host=s1 tod_us=49530000000i 1704067200000000000',
        'time_of_day,host=s2 tod_us=1000000i 1704067260000000000',
        # bool_test: Boolean exact mapping (S09)
        'bool_test,host=s1 flag=true 1704067200000000000',
        'bool_test,host=s2 flag=false 1704067260000000000',
        # uint_test: UInt64 exact mapping (S10) — note 'u' suffix for unsigned
        'uint_test,host=s1 counter=100u 1704067200000000000',
        'uint_test,host=s2 counter=0u 1704067260000000000',
        # str_test: String + UTF-8 encoding (S15)
        'str_test,host=s1 msg="hello world",code="UTF-8\u4e2d\u6587" 1704067200000000000',
        # duration_test: Duration/Interval → BIGINT ns (034)
        'duration_test,host=s1 dur_ns=3600000000000i 1704067200000000000',
        'duration_test,host=s2 dur_ns=60000000000i 1704067260000000000',
        # decimal_test: high-precision float proxy for Decimal128 (033)
        'decimal_test,host=s1 high_prec=123456789.123456789 1704067200000000000',
        # dict_test: Dictionary-encoded tags (057)
        'dict_test,category=electronics name="laptop" 1704067200000000000',
        'dict_test,category=clothing name="shirt" 1704067260000000000',
        'dict_test,category=electronics name="phone" 1704067320000000000',
        # struct_test: Struct/Map → JSON string (058)
        'struct_test,host=s1 config="{\\\"timeout\\\":30,\\\"retries\\\":3}" 1704067200000000000',
    ]
    ms_lines = [
        # date_test: Date32/64 midnight zero-fill (059)
        'date_test,host=s1 value=1i 1705276800000',
        'date_test,host=s2 value=2i 1718409600000',
        # cpu: object mapping — measurement/tag/field (003)
        'cpu,host=server01,region=east usage_idle=95.5,usage_system=3.2 1704067200000',
        'cpu,host=server02,region=west usage_idle=88.1,usage_system=5.0 1704067260000',
    ]
    return ns_lines, ms_lines


# ══════════════════════════════════════════════════════════════════════════
# CASE definitions
# ══════════════════════════════════════════════════════════════════════════

_CASES = [
    # ── CASE-01m: MySQL full type mapping ─────────────────────────────────
    ["01m", [_mysql], [_SRC_M], "MySQL full type mapping", [
        f"drop external source if exists {_SRC_M}",
        (
            f"create external source {_SRC_M} type='mysql' "
            f"host='{_M_HOST}' port={_M_PORT} "
            f"user='{_M_USER}' password='{_M_PASS}' "
            f"database='{_M_DB}'"
        ),

        # ── Integer family (10 columns) ──
        f"select ts, c_tinyint from {_SRC_M}.all_types order by ts",
        f"select ts, c_tinyint_u from {_SRC_M}.all_types order by ts",
        f"select ts, c_smallint from {_SRC_M}.all_types order by ts",
        f"select ts, c_smallint_u from {_SRC_M}.all_types order by ts",
        f"select ts, c_mediumint from {_SRC_M}.all_types order by ts",
        f"select ts, c_mediumint_u from {_SRC_M}.all_types order by ts",
        f"select ts, c_int from {_SRC_M}.all_types order by ts",
        f"select ts, c_int_u from {_SRC_M}.all_types order by ts",
        f"select ts, c_bigint from {_SRC_M}.all_types order by ts",
        f"select ts, c_bigint_u from {_SRC_M}.all_types order by ts",

        # ── Integer group query ──
        f"select c_tinyint, c_smallint, c_mediumint, c_int, c_bigint from {_SRC_M}.all_types order by ts",

        # ── Float/Decimal (4 columns) ──
        f"select ts, c_float from {_SRC_M}.all_types order by ts",
        f"select ts, c_double from {_SRC_M}.all_types order by ts",
        f"select ts, c_decimal from {_SRC_M}.all_types order by ts",
        f"select ts, c_decimal_big from {_SRC_M}.all_types order by ts",
        # ── DECIMAL(65,30) precision>38 truncation (014) ──
        f"select ts, c_decimal_trunc from {_SRC_M}.all_types order by ts",

        # ── Boolean ──
        f"select ts, c_bool from {_SRC_M}.all_types order by ts",

        # ── Strings utf8mb4 (6 columns) ──
        f"select ts, c_char from {_SRC_M}.all_types order by ts",
        f"select ts, c_varchar from {_SRC_M}.all_types order by ts",
        f"select ts, c_tinytext from {_SRC_M}.all_types order by ts",
        f"select ts, c_text from {_SRC_M}.all_types order by ts",
        f"select ts, c_mediumtext from {_SRC_M}.all_types order by ts",
        f"select ts, c_longtext from {_SRC_M}.all_types order by ts",

        # ── Binary (6 columns) ──
        f"select ts, c_binary from {_SRC_M}.all_types order by ts",
        f"select ts, c_varbinary from {_SRC_M}.all_types order by ts",
        f"select ts, c_tinyblob from {_SRC_M}.all_types order by ts",
        f"select ts, c_blob from {_SRC_M}.all_types order by ts",
        f"select ts, c_mediumblob from {_SRC_M}.all_types order by ts",
        f"select ts, c_longblob from {_SRC_M}.all_types order by ts",

        # ── Datetime (5 columns) ──
        f"select ts, c_date from {_SRC_M}.all_types order by ts",
        f"select ts, c_time from {_SRC_M}.all_types order by ts",
        f"select ts, c_datetime from {_SRC_M}.all_types order by ts",
        f"select ts, c_timestamp from {_SRC_M}.all_types order by ts",
        f"select ts, c_year from {_SRC_M}.all_types order by ts",

        # ── JSON ──
        f"select ts, c_json from {_SRC_M}.all_types order by ts",

        # ── ENUM/SET ──
        f"select ts, c_enum from {_SRC_M}.all_types order by ts",
        f"select ts, c_set from {_SRC_M}.all_types order by ts",

        # ── Geometry ──
        f"select ts, c_geometry from {_SRC_M}.all_types order by ts",
        f"select ts, c_point from {_SRC_M}.all_types order by ts",

        # ── NULL row verification ──
        f"select * from {_SRC_M}.all_types where c_int is null",

        # ── UTF-8 encoding fidelity ──
        f"select c_varchar from {_SRC_M}.all_types where c_varchar like '%中文%'",

        # ── ASCII charset branch (S04, S31, S32) ──
        f"select c_char_asc, c_varchar_asc from {_SRC_M}.ascii_types order by ts",
        f"select c_tinytext_asc, c_text_asc from {_SRC_M}.ascii_types order by ts",
        f"select c_enum_asc, c_set_asc from {_SRC_M}.ascii_types order by ts",

        # ── BIT boundary (023-024) ──
        f"select c_bit1, c_bit8, c_bit64 from {_SRC_M}.special_types order by ts",

        # ── NCHAR/NVARCHAR (S28) ──
        f"select c_nchar, c_nvarchar from {_SRC_M}.special_types order by ts",

        # ── Type aliases (S19) ──
        f"select c_dbl_prec, c_real, c_integer, c_integer_u from {_SRC_M}.special_types order by ts",

        # ── ts PK variants (005-007) ──
        f"select ts, val from {_SRC_M}.ts_pk_timestamp",
        f"select ts_pk, ts_extra, val from {_SRC_M}.multi_ts",

        # ── No ts PK → count works (004 partial) ──
        f"select count(*) from {_SRC_M}.no_ts_table",

        # ── View tests (004, 052) ──
        f"select count(*) from {_SRC_M}.v_no_ts",
        f"select id, val from {_SRC_M}.v_with_ts order by id",
        f"select ts, c_int, c_str, c_bool from {_SRC_M}.v_mixed order by ts",

        # ── LONGBLOB boundary (026) ──
        f"select data, val from {_SRC_M}.blob_overflow",

        # ── SET multi-value serialization (S06) ──
        f"select tag_val from {_SRC_M}.set_combo order by ts",

        # ── TEXT charset variants (S13) ──
        f"select c_text, c_mtext, c_ltext, c_ttext from {_SRC_M}.text_charset",

        # ── Geometric aliases POLYGON/LINESTRING (S23) ──
        f"select poly, lstr, val from {_SRC_M}.geo_alias_test",

        # ── DATETIME fractional seconds (S11) ──
        f"select ts from {_SRC_M}.all_types order by ts limit 1",

        # ── Unmappable type: known-type columns work (S17) ──
        f"select ts, val from {_SRC_M}.unmappable_test",
        # ── Unmappable type column → error (S17) ──
        _expect_error(
            f"select shape from {_SRC_M}.unmappable_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),

        # ── MySQL VECTOR type — version-conditional (S17 VECTOR branch) ──
        _mysql_vector_test_step,

        # ── JSON operator rejection (S24) ──
        _expect_error(
            f"select doc->'$.k' from {_SRC_M}.json_op_test",
            TSDB_CODE_PAR_INVALID_COL_JSON,
            "Only tag can be json type",
        ),

        _clear_source_step(),
    ]],

    # ── CASE-01p: PG full type mapping ────────────────────────────────────
    ["01p", [_pg], [_SRC_P], "PG full type mapping", [
        f"drop external source if exists {_SRC_P}",
        (
            f"create external source {_SRC_P} type='postgresql' "
            f"host='{_P_HOST}' port={_P_PORT} "
            f"user='{_P_USER}' password='{_P_PASS}' "
            f"database='{_P_DB}'"
        ),

        # ── Integer (3 columns) ──
        f"select ts, c_smallint from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_integer from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_bigint from {_SRC_P}.public.all_types order by ts",

        # ── Float/Numeric (4 columns) ──
        f"select ts, c_real from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_double_prec from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_numeric from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_numeric_big from {_SRC_P}.public.all_types order by ts",

        # ── Boolean (S03) ──
        f"select ts, c_boolean from {_SRC_P}.public.all_types order by ts",

        # ── Strings ──
        f"select ts, c_char from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_varchar from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_text from {_SRC_P}.public.all_types order by ts",

        # ── Binary (bytea) ──
        f"select ts, c_bytea from {_SRC_P}.public.all_types order by ts",

        # ── Datetime (6 columns) ──
        f"select ts, c_date from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_time from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_timetz from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_timestamp from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_timestamptz from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_interval from {_SRC_P}.public.all_types order by ts",

        # ── Money ──
        f"select ts, c_money from {_SRC_P}.public.all_types order by ts",

        # ── Network (inet, cidr, macaddr, macaddr8) (S22 partial, 054) ──
        f"select ts, c_inet from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_cidr from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_macaddr from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_macaddr8 from {_SRC_P}.public.all_types order by ts",

        # ── UUID ──
        f"select ts, c_uuid from {_SRC_P}.public.all_types order by ts",

        # ── Text search (tsvector, tsquery) ──
        f"select ts, c_tsvector from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_tsquery from {_SRC_P}.public.all_types order by ts",

        # ── JSON (json, jsonb) (S07) ──
        f"select ts, c_json from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_jsonb from {_SRC_P}.public.all_types order by ts",

        # ── XML (053) ──
        f"select ts, c_xml from {_SRC_P}.public.all_types order by ts",

        # ── Native PG geometry (point) ──
        f"select ts, c_point from {_SRC_P}.public.all_types order by ts",

        # ── Bit strings (055) ──
        f"select ts, c_bit from {_SRC_P}.public.all_types order by ts",
        f"select ts, c_bit_varying from {_SRC_P}.public.all_types order by ts",

        # ── hstore (031) ──
        f"select ts, c_hstore from {_SRC_P}.public.all_types order by ts",

        # ── PostGIS GEOMETRY (036) ──
        f"select ts, c_geom from {_SRC_P}.public.all_types order by ts",

        # ── Enum UDT (056) ──
        f"select ts, c_mood from {_SRC_P}.public.all_types order by ts",

        # ── Type aliases (S20): float4/float8/int2/int4/int8 ──
        f"select c_float4, c_float8, c_int2, c_int4, c_int8 from {_SRC_P}.public.all_types order by ts",

        # ── Character aliases (S29): CHARACTER(n), CHARACTER VARYING(n) ──
        f"select c_character, c_char_varying from {_SRC_P}.public.all_types order by ts",

        # ── NULL row verification ──
        f"select * from {_SRC_P}.public.all_types where c_integer is null",

        # ── UTF-8 encoding fidelity ──
        f"select c_varchar from {_SRC_P}.public.all_types where c_varchar like '%中文%'",

        # ── Array/Range degradation (016, S16, 048) ──
        f"select c_int_arr, c_text_arr from {_SRC_P}.public.degrade_types",
        f"select c_int4range, c_tsrange from {_SRC_P}.public.degrade_types",

        # ── Native PG geometry degradation: path, polygon (S22) ──
        f"select c_path, c_polygon from {_SRC_P}.public.degrade_types",

        # ── Serial types (028, S08, S30) ──
        f"select c_smallserial, c_serial, c_bigserial from {_SRC_P}.public.serial_types",

        # ── ts PK variant: timestamptz PK (005) ──
        f"select ts, val from {_SRC_P}.public.ts_pk_tstz",

        # ── No ts PK → count works ──
        f"select count(*) from {_SRC_P}.public.no_ts_table",

        # ── View test (052) ──
        f"select count(*) from {_SRC_P}.public.v_no_ts",

        # ── Timestamptz multi-timezone normalization (S12, 018) ──
        f"select c_tstz from {_SRC_P}.public.tstz_multi order by ts",

        # ── Timetz + long-form timestamp variants (S21) ──
        f"select c_ttz, c_tstz, c_tsno from {_SRC_P}.public.ts_variants",

        # ── Text no length limit (S14) ──
        f"select c_text from {_SRC_P}.public.all_types where c_text != '' order by ts limit 1",

        # ── UDT composite: known-type columns work (S18) ──
        f"select ts, val from {_SRC_P}.public.udt_type_test",
        # ── UDT composite column → error (S18) ──
        _expect_error(
            f"select loc from {_SRC_P}.public.udt_type_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),
        # ── UDT composite: SELECT * → error (S18) ──
        _expect_error(
            f"select * from {_SRC_P}.public.udt_type_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),

        # ── DOMAIN: known-type columns work (S26) ──
        f"select ts, val from {_SRC_P}.public.domain_type_test",
        # ── DOMAIN column → error (S26) ──
        _expect_error(
            f"select score from {_SRC_P}.public.domain_type_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),

        # ── User-defined RANGE: known-type columns work (S27) ──
        f"select ts, val from {_SRC_P}.public.custom_range_test",
        # ── User-defined RANGE column → error (S27) ──
        _expect_error(
            f"select rng from {_SRC_P}.public.custom_range_test",
            TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
        ),

        # ── JSON operator rejection (S25) ──
        _expect_error(
            f"select doc_json->'k' from {_SRC_P}.public.json_op_test",
            TSDB_CODE_PAR_INVALID_COL_JSON,
            "Only tag can be json type",
        ),
        _expect_error(
            f"select doc_jsonb->'k' from {_SRC_P}.public.json_op_test",
            TSDB_CODE_PAR_INVALID_COL_JSON,
            "Only tag can be json type",
        ),

        _clear_source_step(),
    ]],

    # ── CASE-01i: InfluxDB full type mapping ──────────────────────────────
    ["01i", [_influxdb], [_SRC_I], "InfluxDB full type mapping", [
        f"drop external source if exists {_SRC_I}",
        (
            f"create external source {_SRC_I} type='influxdb' "
            f"host='{_I_HOST}' port={_I_PORT} "
            f"user='admin' "
            f"database='{_I_DB}' "
            f"options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')"
        ),

        # ── Scalar type mapping (049, S09, S10, S15) ──
        f"select f_int, f_float, f_bool, f_str from {_SRC_I}.scalar_types order by f_int",

        # ── Tags — Dictionary encoded (057, 013) ──
        f"select host, region from {_SRC_I}.scalar_types order by time",

        # ── NULL / missing field verification (019) ──
        # All three scalar_types rows have all fields → no NULL expected here
        f"select f_int, f_float, f_bool, f_str from {_SRC_I}.scalar_types where f_int = 0",

        # ── Complex type degradation: JSON-like string (050, 058) ──
        f"select data, meta from {_SRC_I}.complex_test",

        # ── Date midnight zero-fill (059) ──
        f"select time, value from {_SRC_I}.date_test order by value",

        # ── Time-of-day integer (060) ──
        f"select tod_us from {_SRC_I}.time_of_day order by tod_us",

        # ── Boolean exact mapping (S09) ──
        f"select flag from {_SRC_I}.bool_test order by flag",

        # ── UInt64 mapping (S10) ──
        f"select counter from {_SRC_I}.uint_test order by counter",

        # ── String + UTF-8 (S15) ──
        f"select msg, code from {_SRC_I}.str_test",

        # ── Duration/Interval → BIGINT ns (034) ──
        f"select dur_ns from {_SRC_I}.duration_test order by dur_ns",

        # ── Decimal128 high-precision float (033) ──
        f"select high_prec from {_SRC_I}.decimal_test",

        # ── Dictionary-encoded tags — dedicated measurement (057) ──
        f"select category, name from {_SRC_I}.dict_test order by name",

        # ── Struct/Map → JSON string (058) ──
        f"select config from {_SRC_I}.struct_test",

        # ── Object mapping: cpu measurement (003) ──
        f"select usage_idle, usage_system from {_SRC_I}.cpu order by usage_idle",

        # ── 3-segment path: src.bucket.measurement (003d) ──
        f"select host, region from {_SRC_I}.{_I_DB}.cpu order by host",

        _clear_source_step(),
    ]],
]


# ══════════════════════════════════════════════════════════════════════════
# Test class
# ══════════════════════════════════════════════════════════════════════════

class TestFq03TypeMapping(FederatedQueryVersionedMixin):
    """FQ-TYPE-001~060 + S01~S32: concept and type mapping."""

    _fw_data_prepared = False

    # Force UTC timezone so TIMESTAMPTZ columns from PostgreSQL (which are stored
    # as absolute epoch values) are always displayed in UTC, regardless of the
    # host machine's system timezone.  Without this, a machine in Asia/Shanghai
    # (UTC+8) would format the same epoch as "+8h", causing baseline mismatches.
    #
    # "timezone" (top-level) → written to taosd's taos.cfg via deploy_taos.
    # "clientCfg.timezone"   → applied via ALTER LOCAL after the connection is
    #                          established (see before_test.py:get_taos_conn).
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "timezone": "UTC",
        "clientCfg": {
            "federatedQueryEnable": 1,
            "timezone": "UTC",
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

    def setup_method(self, method):
        if TestFq03TypeMapping._fw_data_prepared:
            return
        self._fw_prepare_shared_data()
        TestFq03TypeMapping._fw_data_prepared = True

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")
        TestFq03TypeMapping._fw_data_prepared = False

    # ──────────────────────────────────────────────────────────────────────
    # Shared test data setup
    # ──────────────────────────────────────────────────────────────────────

    def _fw_prepare_shared_data(self):
        mysql_cfg  = self._mysql_cfg()
        pg_cfg     = self._pg_cfg()
        influx_cfg = self._influx_cfg()

        # ── MySQL ──
        ExtSrcEnv.mysql_create_db_cfg(mysql_cfg, _M_DB)
        ExtSrcEnv.mysql_exec_cfg(mysql_cfg, _M_DB, _mysql_setup_sqls())

        # ── PostgreSQL ──
        ExtSrcEnv.pg_create_db_cfg(pg_cfg, _P_DB)
        ExtSrcEnv.pg_exec_cfg(pg_cfg, _P_DB, _pg_setup_sqls())

        # ── InfluxDB ──
        ExtSrcEnv.influx_create_db_cfg(influx_cfg, _I_DB)
        ns_lines, ms_lines = _influx_setup_lines()
        ExtSrcEnv.influx_write_cfg(influx_cfg, _I_DB, ns_lines)
        if ms_lines:
            ExtSrcEnv.influx_write_cfg(influx_cfg, _I_DB, ms_lines, precision='ms')

    # ──────────────────────────────────────────────────────────────────────
    # Framework helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fw_fmt_cell(value) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.12g}"
        if isinstance(value, datetime.datetime):
            # The Python taos driver returns naive datetimes in the local
            # timezone.  Normalize to UTC for a stable, timezone-independent
            # baseline (ALTER LOCAL "timezone" has no effect on the driver).
            return str(datetime.datetime.utcfromtimestamp(value.timestamp()))
        return str(value)

    def _fw_fmt_rows(self, rows: Sequence) -> List[str]:
        return ["|".join(self._fw_fmt_cell(v) for v in row) for row in rows]

    @staticmethod
    def _fw_drop_dynamic_columns(description, rows: Sequence):
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
        description, rows = self._fw_drop_dynamic_columns(description, rows)
        lines = []
        if description:
            lines.append("|".join(col[0] for col in description))
        lines.extend(self._fw_fmt_rows(rows))
        return lines

    @staticmethod
    def _fw_normalize_result_lines(result) -> List[str]:
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
        result,
    ):
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
        return tdSql.query(sql, exit=exit, queryTimes=1)

    def _fw_runtime_sql(self, sql: str, src_type: str) -> str:
        """Patch import-time Influx token placeholders with runtime token."""
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

    def _fw_exec_step(
        self,
        step,
        src_type: str,
        source_names: List[str],
        blocks: List[str],
        label: str,
        step_tag: str,
    ):
        # ── list of sub-steps ─────────────────────────────────────────
        if isinstance(step, list):
            for sub_no, sub in enumerate(step, start=1):
                sub_tag = f"{step_tag}.{sub_no:02d}"
                self._fw_exec_step(sub, src_type, source_names, blocks, label, sub_tag)
            return

        # ── callable step ─────────────────────────────────────────────
        if callable(step):
            self._fw_exec_step(step(src_type), src_type, source_names, blocks, label, step_tag)
            return

        # ── string SQL step ───────────────────────────────────────────
        if isinstance(step, str):
            sql = self._fw_runtime_sql(step, src_type)
            stripped = sql.strip()
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
                self._fw_append_step_block(blocks, label, step_tag, "QUERY", sql, rows)
            else:
                is_create = bool(
                    re.match(r"create\s+external\s+source", stripped, re.IGNORECASE)
                )
                tdSql.sql = sql
                try:
                    tdSql.affectedRows = tdSql.cursor.execute(sql)
                    exec_result = "OK"
                except Exception as _exec_err:
                    _msg = str(_exec_err).splitlines()[0] if str(_exec_err) else "unknown error"
                    exec_result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "EXEC", sql, exec_result)
                if is_create:
                    created_name = self._fw_extract_src_name(sql)
                    if created_name:
                        self._fw_auto_show_describe(created_name, blocks, label, step_tag)
            return

        # ── expect error step ─────────────────────────────────────────
        if isinstance(step, _ExpectErrorStep):
            sql = self._fw_runtime_sql(step.sql, src_type)
            try:
                tdSql.error(
                    sql,
                    expectedErrno=step.errno,
                    expectErrInfo=step.err_info,
                )
                err_name = getattr(step.errno, '__name__', str(step.errno))
                self._fw_append_step_block(
                    blocks, label, step_tag, "EXPECT-ERROR",
                    sql,
                    f"ERROR_EXPECTED: {err_name}",
                )
            except Exception as _e:
                self._fw_append_step_block(
                    blocks, label, step_tag, "EXPECT-ERROR",
                    sql,
                    f"UNEXPECTED: {str(_e)[:200]}",
                )
            return

        # ── drop all sources in pool ──────────────────────────────────
        if isinstance(step, _ClearSourceStep):
            for name in source_names:
                sql = f"drop external source if exists {name}"
                try:
                    tdSql.cursor.execute(sql)
                    result = "OK"
                except Exception as _drop_err:
                    _msg = str(_drop_err).splitlines()[0] if str(_drop_err) else "unknown error"
                    result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "CLEANUP", sql, result)
            return

        raise ValueError(f"Unknown step type: {type(step).__name__}")

    # ──────────────────────────────────────────────────────────────────────
    # Auto show/describe after CREATE
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fw_extract_src_name(sql: str) -> Optional[str]:
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
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            return name
        return "`" + name.replace("`", "``") + "`"

    @staticmethod
    def _fw_norm_src_name(src_name) -> str:
        name = str(src_name).strip()
        if len(name) >= 2 and name[0] == "`" and name[-1] == "`":
            name = name[1:-1]
        return name.lower()

    def _fw_auto_show_describe(
        self,
        src_name: str,
        blocks: List[str],
        label: str,
        step_tag: str,
    ):
        # SHOW – find the row for this source
        show_sql = "show external sources"
        ok = self._fw_query_once(show_sql, exit=False)
        if ok is not False:
            target_name = self._fw_norm_src_name(src_name)
            matching = [
                row for row in tdSql.queryResult
                if self._fw_norm_src_name(row[0]) == target_name
            ]
            rows_text = self._fw_fmt_result(tdSql.cursor.description, matching)
            self._fw_append_step_block(blocks, label, step_tag, "AUTO-SHOW", show_sql, rows_text)
        else:
            self._fw_append_step_block(blocks, label, step_tag, "AUTO-SHOW", show_sql, "ERROR: show external sources failed")

        # DESCRIBE
        desc_sql = f"describe external source {self._fw_quote_src_name(src_name)}"
        ok2 = self._fw_query_once(desc_sql, exit=False)
        if ok2 is not False:
            rows_text = self._fw_fmt_result(tdSql.cursor.description, tdSql.queryResult)
            self._fw_append_step_block(blocks, label, step_tag, "AUTO-DESCRIBE", desc_sql, rows_text)
        else:
            self._fw_append_step_block(blocks, label, step_tag, "AUTO-DESCRIBE", desc_sql, "ERROR: describe external source failed")

    # ──────────────────────────────────────────────────────────────────────
    # Result file management
    # ──────────────────────────────────────────────────────────────────────

    def _fw_baseline_file(self) -> str:
        label = self._version_label().replace(".", "_").replace("/", "_")
        return os.path.join(
            os.path.dirname(__file__),
            "ans",
            f"test_fq_03_type_mapping_framework_{label}.txt",
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
        # error message before the closing ')"' chars.  After the token
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
            shutil.copy(tmp_file, baseline)
            os.remove(tmp_file)
            tdLog.info(f"Framework baseline file created: {baseline}")
            return

        with open(baseline, "r", encoding="utf-8") as f:
            expected = f.read()
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

        raise AssertionError(
            "Framework baseline mismatch\n"
            f"  baseline: {baseline}\n"
            f"  actual  : {tmp_file}\n"
            f"  first diff at line {diff_line}:\n"
            f"    baseline: {exp_val!r}\n"
            f"    actual  : {act_val!r}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Main test entry point
    # ──────────────────────────────────────────────────────────────────────

    def test_fq_type_mapping_framework(self):
        """FQ-TYPE-001~060 + S01~S32: complete type mapping coverage.

        Catalog: - Query:FederatedTypeMapping

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-23 wpan Refactored to data-driven framework

        """
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
