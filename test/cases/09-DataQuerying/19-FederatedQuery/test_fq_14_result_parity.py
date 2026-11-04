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
test_fq_14_result_parity.py

Result-parity test framework for federated query.

ALL features are tested against ALL five database types:
  1. Local TDengine (reference)
  2. MySQL external source
  3. PostgreSQL external source
  4. InfluxDB external source
  5. Oracle external source

For each SQL statement, the same logical query is executed against all
sources and results are compared row-by-row against the local TDengine
reference.  A test only omits an external source when the source's SQL
dialect physically cannot express the query (e.g. MySQL has no FULL
OUTER JOIN or NULLS FIRST syntax; PostgreSQL has no FIND_IN_SET).
All other features — including functions, operators, window queries,
JOINs, UNION, subqueries, NULLS FIRST/LAST, etc. — are tested on every
supported source.

Logical schema (all five sources identical):
  Local TDengine:  time TIMESTAMP PK, id INT, val INT, Mapped_Val INT,
                   score DOUBLE, label NCHAR(32),
                   nullable_val INT, lat DOUBLE, lon DOUBLE,
                   wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32)
  MySQL:           time DATETIME(3) NOT NULL PK  (enables TDengine window queries)
  PostgreSQL:      time TIMESTAMP NOT NULL PK
  InfluxDB:        time (built-in timestamp); label tag; Mapped_Val field;
                   remaining values are fields
  Oracle:          time TIMESTAMP(3) PRIMARY KEY; numbers are NUMBER(10,0)
                   (BIGINT) / BINARY_DOUBLE; text is NVARCHAR2 so that it maps
                   to NCHAR like the other providers

Parity cases use provider-independent lowercase columns.  The dedicated
mixed-case test verifies provider-specific metadata and binding rules.

Environment:
  Enterprise edition, federatedQueryEnable=1
  MySQL 8.0+, PostgreSQL 14+, InfluxDB v3, Oracle 19c+
  Python: pymysql, psycopg2, requests
"""
from typing import Dict, List, Tuple


import datetime as _datetime
import os
import re
import tempfile
import time
import pytest

from new_test_framework.utils import tdLog, tdSql


import federated_query_common
from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    ParityTestBase,
    QueryError,
    parity_sql_val,
    parity_make_insert_sqls,
)

# Oracle's transport precision is MICRO (DS 5.3.7: the fixture has no
# TIMESTAMP(7-9) column), so epoch counts read through an Oracle scan differ
# from the local millisecond reference by the same 1000 factor as MySQL/PG.
# The factor table is owned by the shared module; register the entry here
# instead of duplicating the scaling rule in this file.
federated_query_common._EXT_PRECISION_FACTOR.setdefault("Oracle", 1000)

# ── Process-timezone helpers ─────────────────────────────────────────
# The parity case requires UTC for both its epoch conversions and the taospy
# cached datetime epoch.  Do not change TZ at module import time: pytest
# imports every FQ module during collection, so doing so leaks UTC into tests
# that run before this class.
import taos.field as _taos_field

_ORIG_TZ = None
_ORIG_DATETIME_EPOCH = None
_TZ_PINNED = False


def _refresh_process_tz():
    """Make a TZ change visible to libc (and the client's own conversions).

    POSIX exposes time.tzset(); the Windows CRT keeps the zone cached until
    _tzset() is called, and the client formats result timestamps through libc
    localtime(), so without the refresh a pinned TZ never reaches the output
    even though the TZ environment variable itself was updated.
    """
    if hasattr(time, 'tzset'):
        time.tzset()
        return
    try:
        import ctypes

        ctypes.CDLL('ucrtbase')._tzset()
    except OSError:
        pass


def _pin_process_timezone_utc():
    global _ORIG_TZ, _ORIG_DATETIME_EPOCH, _TZ_PINNED
    if _TZ_PINNED:
        return

    _ORIG_TZ = os.environ.get('TZ')
    _ORIG_DATETIME_EPOCH = _taos_field._datetime_epoch
    os.environ['TZ'] = 'UTC'
    _refresh_process_tz()
    _taos_field._datetime_epoch = _datetime.datetime.fromtimestamp(0)
    _TZ_PINNED = True


def _restore_process_timezone():
    global _TZ_PINNED
    if not _TZ_PINNED:
        return

    if _ORIG_TZ is not None:
        os.environ['TZ'] = _ORIG_TZ
    else:
        os.environ.pop('TZ', None)
    _refresh_process_tz()
    _taos_field._datetime_epoch = _ORIG_DATETIME_EPOCH
    _TZ_PINNED = False

# Fixed timezone for this test — all epoch→datetime conversions use UTC so
# the test produces identical results regardless of the host OS timezone.
# The TDengine client timezone is also fixed to UTC via ALTER LOCAL in setup.
_FIXED_TZ = _datetime.timezone.utc

def _ms_to_dt(ms_ts):
    """Convert epoch ms to UTC 'YYYY-MM-DD HH:MM:SS.mmm' string.

    The TDengine client timezone is pinned to UTC by setup_method, so
    MySQL DATETIME / PG TIMESTAMP strings must also be in UTC for the
    epoch round-trip to be correct.
    """
    dt = _datetime.datetime.fromtimestamp(ms_ts / 1000.0, tz=_FIXED_TZ)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms_ts % 1000:03d}"


_MYSQL_DB      = "fq_parity_m"
_PG_DB         = "fq_parity_p"
_SQLSERVER_DB  = "fq_parity_s"
_INFLUX_BUCKET = "fq_parity_i"
_LOCAL_DB      = "fq_parity_local"
_LOCAL_US_DB   = "fq_parity_local_us"
_LOCAL_TBL     = "parity_t"
_FLOAT_TOL     = 1e-4

# Precision-independent timestamp strings for WHERE / RANGE clauses.
# Raw epoch integers (e.g. 1704067200000) are interpreted at the *table's*
# precision — which is µs for MySQL/PG and ns for InfluxDB — so the same
# integer means a different wall-clock instant for each source.  Using
# ISO-8601 strings avoids this mismatch entirely.
_TS0  = _ms_to_dt(1704067200000)   # row[0] — data start
_TS5M = _ms_to_dt(1704067500000)   # row[0] + 5 min
_TS4M = _ms_to_dt(1704067440000)   # row[4] — data end

# Pre-built clause fragments for SQL templates (embed at module load time
# so that .format(tbl=...) only needs to substitute the table name).
_W5M  = f"WHERE time >= '{_TS0}' AND time < '{_TS5M}'"   # 5-minute window
_R4M  = f"RANGE('{_TS0}', '{_TS4M}')"                     # INTERP range

# ── Case registry ────────────────────────────────────────────────────────────────
# FQOR-LOCAL-001 … FQOR-LOCAL-016 (TS §5.7.1) are carried by the existing
# groups and by the dedicated methods below; no separate file is added.
#   001  basic query + column pruning   sort-23 (all columns) and
#                                       test_fq_oracle_projection_pruning
#   002  filter / sort                  whr-*, sort-*
#   003  aggregate                      agg-*
#   004  time windows                   win-*, _WIN_EPOCH_CASES
#   005  local-table JOIN               join-*
#   006  cross-source JOIN              _JOIN_TS_EXT_SORT_MATRIX oracle/* legs
#                                       and test_fq_oracle_cross_source_join
#   007  subquery incl. NULL 3VL        sub-34 … sub-39
#   008  window functions               lag-03 … lag-10
#   009  zero pushdown                  test_fq_oracle_zero_pushdown
#   010  three-path consistency         test_fq_oracle_three_paths
#   011  repeat execution               test_fq_oracle_repeat_execution
#   012  view / materialized view       test_fq_oracle_views
#   013  large result set               test_fq_oracle_large_result_set
#   014  derived-table projections      join-39 … join-41
#   015  expression projection          arith-13
#   016  zero-pushdown JOIN shape       test_fq_oracle_zero_pushdown
# Every entry: (sql_template,) or (sql_template, opts_dict)
# opts keys: positive (bool, default True), reason (str),
#             float_cols (set of col indices), ordered (bool, default True),
#             source_expected (dict[str, list])
# positive=True  → results must match local reference (正向用例)
# positive=False → all sources must error with same errno  (负向用例，reason必填)
#
# IDs are auto-generated as  <group>-<NN>  (e.g. whr-01, win-19).
# To add a case: append to the relevant group — only that group's last IDs shift.
# To add a group: add a new key — all existing IDs are unaffected.
# ─────────────────────────────────────────────────────────────────────────────────
_PARITY_GROUPS: Dict[str, list] = {
    # ── whr: WHERE 条件（比较 / 字符串 / 逻辑组合）───────────────────────────
    "whr": [
        ("SELECT id, val FROM {tbl} WHERE val > 20 ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val = 30 ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val <> 30 ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val <= 30 ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val >= 30 ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val BETWEEN 20 AND 40 ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val NOT BETWEEN 20 AND 40 ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label IN ('north', 'east') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label NOT IN ('east') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label LIKE 'n%' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label LIKE '%th' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label NOT LIKE 'n%' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label LIKE '_orth' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label LIKE '____' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label IS NOT NULL ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label IS NULL ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val > 20 AND val < 50 ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val < 15 OR val > 45 ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE NOT (val > 30) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE (val < 20 OR val > 40) AND label <> 'east' ORDER BY id",),
        ("SELECT val FROM {tbl} WHERE val > 30 ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val = 10 ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val != 30 ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val < 30 ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val IN (10, 30, 50) ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE label LIKE 'n%' ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val = 10 OR val = 50 ORDER BY val",),
        ("SELECT COUNT(*) FROM {tbl} WHERE val IS NOT NULL", dict(ordered=False)),
        ("SELECT val FROM {tbl} WHERE val > 20 AND id > 3 ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE NOT (id = 1) ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE label NOT LIKE 'n%' ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val NOT IN (10, 30, 50) ORDER BY val",),
        ("SELECT val, ISNULL(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT val FROM {tbl} WHERE val BETWEEN 20 AND 40 ORDER BY val",),
        ("SELECT id, val FROM {tbl} WHERE label MATCH '^n' ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE label NMATCH '^n' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label MATCH '.*th$' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label NMATCH '.*th$' ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE label->'key' = 'val' ORDER BY time",
         dict(positive=False, reason="'->' on NCHAR column in WHERE → PAR_INVALID_COL_JSON")),
        ("SELECT id FROM {tbl} WHERE label CONTAINS 'north'",
         dict(positive=False, reason="CONTAINS on NCHAR column in WHERE → PAR_INVALID_COL_JSON")),
        ("SELECT a.id, a.val FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time WHERE a.val > 20 ORDER BY a.id",),
        ("SELECT a.id, a.val FROM {tbl} a, {tbl} b WHERE a.time = b.time and a.val > 20 ORDER BY a.id",),
        ("SELECT a.id, a.val, b.label FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time WHERE a.val > 20 AND b.label = 'south' ORDER BY a.id",),
        ("SELECT id, val FROM {tbl} WHERE val > (SELECT AVG(val) FROM {tbl}) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val = (SELECT MAX(val) FROM {tbl}) ORDER BY time",),
        ("SELECT id, val FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE label = 'north') ORDER BY time",),
        ("SELECT id, val FROM {tbl} t1 WHERE val > ANY (SELECT val FROM {tbl} t2 WHERE t2.label = t1.label) ORDER BY time",
         dict(positive=False, reason="correlated quantified subquery in WHERE not supported (0x800026A6)")),
        ("SELECT id FROM {tbl} t1 WHERE EXISTS (SELECT 1 FROM {tbl} t2 WHERE t2.id = t1.id AND t2.val > 30) ORDER BY time",
         dict(positive=False, reason="correlated EXISTS subquery as expr not supported (0x800026A6)")),
        # WHERE + 聚合 / ORDER / LIMIT 组合
        ("SELECT COUNT(*) FROM {tbl} WHERE val > 20", dict(ordered=False)),
        ("SELECT COUNT(*) FROM {tbl} WHERE val > 10 AND id > 2", dict(ordered=False)),
        ("SELECT COUNT(*) FROM {tbl} WHERE val <= 30", dict(ordered=False)),
        ("SELECT val FROM {tbl} WHERE val > 0 ORDER BY val LIMIT 3",),
        ("SELECT val FROM {tbl} WHERE val > 0 ORDER BY val ASC LIMIT 3",),
        ("SELECT COUNT(*) FROM {tbl} WHERE score > 0 AND val > 0", dict(ordered=False)),
        ("SELECT id, val, score FROM {tbl} WHERE val >= 20 ORDER BY val", dict(float_cols={2})),
    ],
    # ── cond: 条件表达式（COALESCE / NULLIF / IF / IFNULL / NVL2 / CASE WHEN）─
    "cond": [
        ("SELECT id, COALESCE(label, 'unknown') FROM {tbl} ORDER BY time",),
        ("SELECT id, NULLIF(val, 30) FROM {tbl} ORDER BY time",),
        ("SELECT id, IF(val > 30, 'high', 'low') AS cat FROM {tbl} ORDER BY time",),
        ("SELECT id, IFNULL(label, 'none') FROM {tbl} ORDER BY time",),
        ("SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {tbl} ORDER BY time",),
        ("SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {tbl} ORDER BY time",),
        ("SELECT id, NULLIF(val, 30) FROM {tbl} ORDER BY time",),
        ("SELECT id, COALESCE(NULL, val) FROM {tbl} ORDER BY time",),
        ("SELECT id, NVL2(label, 'has_val', 'no_val') AS nv FROM {tbl} ORDER BY time",),
        ("SELECT id, NULLIF(val, 30) AS v FROM {tbl} ORDER BY v ASC NULLS FIRST",),
        ("SELECT id, NULLIF(val, 30) AS v FROM {tbl} ORDER BY v DESC NULLS LAST",),
        ("SELECT val, CASE WHEN val > 30 THEN 'high' ELSE 'low' END AS lvl FROM {tbl} ORDER BY val",),
        ("SELECT SUM(CASE WHEN id = 1 THEN val ELSE 0 END) AS s1 FROM {tbl}", dict(ordered=False)),
        ("SELECT IFNULL(val, 0) FROM {tbl} WHERE val = 10",),
        ("SELECT COALESCE(val, 0) FROM {tbl} WHERE val = 10",),
        ("SELECT val, val / NULLIF(0, 0) FROM {tbl} WHERE val = 10",),
        ("SELECT val, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {tbl} ORDER BY val",),
    ],
    # ── arith: 算术运算符（+  -  *  /  %  &  |）──────────────────────────────
    "arith": [
        ("SELECT id, val * 2 + 1 FROM {tbl} ORDER BY time",),
        ("SELECT id, val * 3 FROM {tbl} ORDER BY time",),
        ("SELECT id, val / 4.0 FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, val % 3 FROM {tbl} ORDER BY time",),
        ("SELECT id, val & 3 FROM {tbl} ORDER BY time",),
        ("SELECT id, val | 1 FROM {tbl} ORDER BY time",),
        ("SELECT id, val * 2 AS dbl FROM {tbl} ORDER BY dbl",),
        ("SELECT val, val+1, val-1, val*2, val%3 FROM {tbl} ORDER BY val",),
        ("SELECT val, val / 2.0 FROM {tbl} ORDER BY val", dict(float_cols={1})),
        ("SELECT val, val * 2 AS doubled FROM {tbl} ORDER BY val",),
        ("SELECT val, val & 3 FROM {tbl} WHERE val = 50 ORDER BY time",),
        ("SELECT val, val | 8 FROM {tbl} WHERE val = 50 ORDER BY time",),
        # Expression projection (FQOR-LOCAL-015): a constant, a division by a
        # NULL divisor and a bigint cast are all evaluated locally on the
        # external scan; the remote SQL stays a bare scan.
        ("SELECT id, val + 1, val / NULLIF(0, 0), CAST(val AS BIGINT) FROM {tbl} ORDER BY time",),
    ],
    # ── math: 数学函数（ABS CEIL FLOOR ROUND SQRT POW MOD SIGN GREATEST LEAST
    #                    PI TRUNCATE DEGREES RADIANS EXP LN LOG）────────────────
    "math": [
        ("SELECT id, GREATEST(id, val) FROM {tbl} ORDER BY time",),
        ("SELECT id, LEAST(id, val) FROM {tbl} ORDER BY time",),
        ("SELECT id, TRUNCATE(PI(), 5) AS pi5 FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, ABS(20 - val) FROM {tbl} ORDER BY time",),
        ("SELECT id, CEIL(score) FROM {tbl} ORDER BY time",),
        ("SELECT id, FLOOR(score) FROM {tbl} ORDER BY time",),
        ("SELECT id, ROUND(score, 1) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, ROUND(score, 0) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, SQRT(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, POW(id, 2) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, MOD(val, 3) FROM {tbl} ORDER BY time",),
        ("SELECT id, SIGN(val - 25) FROM {tbl} ORDER BY time",),
        ("SELECT id, EXP(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, LN(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, LOG(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, LOG(val, 10) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, TRUNCATE(score, 1) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, DEGREES(score) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, RADIANS(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT ABS(val - 30) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT CEIL(score) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT FLOOR(score) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT ROUND(score, 0) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0})),
        ("SELECT SQRT(val) FROM {tbl} WHERE val = 40 ORDER BY time", dict(float_cols={0})),
        ("SELECT POW(val, 2) FROM {tbl} WHERE val = 30 ORDER BY time", dict(float_cols={0})),
        ("SELECT SIGN(val) FROM {tbl} WHERE val = 30 ORDER BY time",),
        ("SELECT LOG(val, 2) FROM {tbl} WHERE val = 40 ORDER BY time", dict(float_cols={0})),
        ("SELECT TRUNCATE(score, 1) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0})),
        ("SELECT LOG(val) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0})),
        ("SELECT MOD(val, 3) FROM {tbl} WHERE val = 50 ORDER BY time",),
        ("SELECT GREATEST(val, 3), LEAST(val, 3) FROM {tbl} WHERE val = 50 ORDER BY time",),
        ("SELECT GREATEST(val, 3), LEAST(val, 3) FROM {tbl} WHERE val = 10 ORDER BY time",),
    ],
    # ── trig: 三角函数（SIN COS TAN ASIN ACOS ATAN）──────────────────────────
    "trig": [
        ("SELECT id, SIN(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, COS(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, TAN(score) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, ASIN(score / 10.0) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, ACOS(score / 10.0) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, ATAN(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT COS(val - 10), SIN(val - 10), TAN(val - 10) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0, 1, 2})),
        ("SELECT ACOS(val - 10), ASIN(val / 10.0), ATAN(val / 10.0) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0, 1, 2})),
        ("SELECT DEGREES(PI()), RADIANS(180), PI(), EXP(val - 10) FROM {tbl} WHERE val = 10 ORDER BY time", dict(float_cols={0, 1, 2, 3})),
    ],
    # ── sfn: 字符串函数（LOWER UPPER LENGTH TRIM CONCAT SUBSTRING REPLACE
    #                     POSITION REPEAT ASCII CHAR FIND_IN_SET）──────────────
    "sfn": [
        ("SELECT id, LOWER(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, UPPER(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, CHAR_LENGTH(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, LENGTH(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, LTRIM(CONCAT(' ', label)) FROM {tbl} ORDER BY time",),
        ("SELECT id, RTRIM(CONCAT(label, ' ')) FROM {tbl} ORDER BY time",),
        ("SELECT id, TRIM(CONCAT(' ', label, ' ')) FROM {tbl} ORDER BY time",),
        ("SELECT id, CONCAT(label, '-', LOWER(label)) FROM {tbl} ORDER BY time",),
        ("SELECT id, CONCAT_WS('-', label, LOWER(label)) FROM {tbl} ORDER BY time",),
        ("SELECT id, SUBSTRING(label, 1, 3) FROM {tbl} ORDER BY time",),
        ("SELECT id, SUBSTR(label, -3, 3) FROM {tbl} ORDER BY time",),
        ("SELECT id, REPLACE(label, 'north', 'n') FROM {tbl} ORDER BY time",),
        ("SELECT id, POSITION('o' IN label) FROM {tbl} ORDER BY time",),
        ("SELECT id, REPEAT('x', id) FROM {tbl} ORDER BY time",),
        ("SELECT id, ASCII(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, CHAR(65) FROM {tbl} ORDER BY time",),
        ("SELECT id, FIND_IN_SET(label, 'north,south,east') AS pos FROM {tbl} ORDER BY time",),
        ("SELECT id, SUBSTRING_INDEX(label, 'o', 1) AS si FROM {tbl} ORDER BY time",),
        ("SELECT CONCAT(label, '_x') FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT UPPER(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT LOWER(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT REPLACE(label, 'north', 'omega') FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT LENGTH(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT CHAR_LENGTH(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT ASCII(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT LTRIM(label), RTRIM(label), TRIM(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT CONCAT_WS('-', label, 'b') FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT REPEAT('x', 3) FROM {tbl} LIMIT 1",),
        ("SELECT SUBSTRING(label, 1, 3) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT CHAR(65) FROM {tbl} LIMIT 1",),
        ("SELECT POSITION('lp' IN label) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT FIND_IN_SET('B', 'A,B,C') FROM {tbl} LIMIT 1",),
        ("SELECT SUBSTRING_INDEX('www.taosdata.com', '.', 2) FROM {tbl} LIMIT 1",),
        ("SELECT LIKE_IN_SET(label, 'no%,so%') FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT REGEXP_IN_SET(label, '^n.*,^s.*') FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT MASK_NONE(label) FROM {tbl} WHERE val = 10 ORDER BY time",),
    ],
    # ── hash: 哈希 / 编码函数（MD5 SHA1 SHA2 CRC32 TO_BASE64 FROM_BASE64）────
    "hash": [
        ("SELECT id, MD5(CAST(label AS VARCHAR(32))) FROM {tbl} ORDER BY time",),
        ("SELECT id, TO_BASE64(label) FROM {tbl} ORDER BY time",),
        ("SELECT id, FROM_BASE64(TO_BASE64(label)) AS decoded FROM {tbl} ORDER BY time",),
        ("SELECT id, SHA1(CAST(label AS VARCHAR(32))) FROM {tbl} ORDER BY time",),
        ("SELECT id, SHA2(CAST(label AS VARCHAR(32)), 256) FROM {tbl} ORDER BY time",),
        ("SELECT id, CRC32(label) FROM {tbl} ORDER BY time",),
        ("SELECT SHA1(CAST(label AS VARCHAR(32))) FROM {tbl} WHERE val = 10 ORDER BY time",),
        ("SELECT SHA2(CAST(label AS VARCHAR(32)), 256) FROM {tbl} WHERE val = 10 ORDER BY time",),
    ],
    # ── agg: 聚合函数（COUNT SUM MIN MAX AVG STDDEV VARIANCE COUNT DISTINCT）──
    "agg": [
        ("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {tbl}", dict(ordered=False)),
        ("SELECT AVG(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT STDDEV(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT VARIANCE(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT STDDEV_SAMP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT VAR_SAMP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT COUNT(DISTINCT val) FROM {tbl}", dict(ordered=False)),
        ("SELECT COUNT(label) FROM {tbl}", dict(ordered=False)),
        ("SELECT SUM(CASE WHEN label = 'north' THEN val ELSE 0 END) FROM {tbl}", dict(ordered=False)),
        ("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM {tbl}", dict(float_cols={2}, ordered=False)),
        ("SELECT PERCENTILE(val, 50) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT APERCENTILE(val, 50) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT SPREAD(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT LEASTSQUARES(val, 0, 1) FROM {tbl}", dict(ordered=False)),
        ("SELECT VAR_POP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT STDDEV_POP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT STD(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT GROUP_CONCAT(label, ',') FROM {tbl}", dict(ordered=False)),
        ("SELECT COLS(MAX(val), label) FROM {tbl}", dict(ordered=False)),
        ("SELECT COLS(MIN(val), label) FROM {tbl}", dict(ordered=False)),
        ("SELECT COUNT(*), SUM(val) FROM {tbl} WHERE val BETWEEN 20 AND 40", dict(ordered=False)),
        ("SELECT COUNT(*), AVG(score) FROM {tbl}", dict(float_cols={1}, ordered=False)),
        ("SELECT COUNT(*) FROM {tbl} WHERE score > 0", dict(ordered=False)),
        ("SELECT AVG(score) FROM {tbl} WHERE score > 0", dict(float_cols={0}, ordered=False)),
        ("SELECT COUNT(*), AVG(val) FROM {tbl} WHERE val > 0", dict(float_cols={1}, ordered=False)),
    ],
    # ── grp: GROUP BY / HAVING ────────────────────────────────────────────────
    "grp": [
        ("SELECT label, COUNT(*) FROM {tbl} GROUP BY label ORDER BY label",),
        ("SELECT label, COUNT(*) FROM {tbl} GROUP BY label HAVING COUNT(*) > 1 ORDER BY label",),
        ("SELECT label, SUM(val) FROM {tbl} GROUP BY label ORDER BY label",),
        ("SELECT label, AVG(val) FROM {tbl} GROUP BY label ORDER BY label", dict(float_cols={1})),
        ("SELECT label, MAX(val) FROM {tbl} GROUP BY label ORDER BY label",),
        ("SELECT label, MIN(val) FROM {tbl} GROUP BY label ORDER BY label",),
        ("SELECT label, COUNT(id) FROM {tbl} GROUP BY label ORDER BY label",),
        ("SELECT label, SUM(val) FROM {tbl} GROUP BY label HAVING SUM(val) > 30 ORDER BY label",),
        ("SELECT label, AVG(score) FROM {tbl} GROUP BY label HAVING AVG(score) > 2.0 ORDER BY label",
         dict(float_cols={1})),
        ("SELECT val / 20 AS bucket, COUNT(*) FROM {tbl} GROUP BY val / 20 ORDER BY bucket",),
        ("SELECT label, COUNT(*) AS cnt FROM {tbl} GROUP BY label ORDER BY 2 DESC", dict(ordered=False)),
        ("SELECT label, COUNT(*), SUM(val), AVG(score) FROM {tbl} WHERE val >= 20 GROUP BY label ORDER BY label",
         dict(float_cols={3})),
        ("SELECT id, COUNT(*) AS cnt FROM {tbl} GROUP BY id ORDER BY id",),
        ("SELECT id, COUNT(*) AS cnt FROM {tbl} GROUP BY id HAVING COUNT(*) > 0 ORDER BY id",),
        ("SELECT label, COUNT(*) FROM {tbl} WHERE val > 0 GROUP BY label ORDER BY label LIMIT 10",),
        ("SELECT label, SUM(val) FROM {tbl} WHERE val > 0 GROUP BY label ORDER BY label",),
        ("SELECT label, COUNT(*), AVG(val) FROM {tbl} GROUP BY label ORDER BY label", dict(float_cols={2})),
        ("SELECT label, COUNT(*) FROM {tbl} GROUP BY label HAVING COUNT(*) >= 2 ORDER BY label",),
        # GROUP BY position syntax
        ("SELECT label, COUNT(*) FROM {tbl} GROUP BY 1 ORDER BY 1",),
        # GROUP BY alias (result-set column name)
        ("SELECT label AS lb, COUNT(*) FROM {tbl} GROUP BY lb ORDER BY lb",),
        # PARTITION BY expression
        ("SELECT val / 10 AS bucket, COUNT(*) FROM {tbl} PARTITION BY val / 10 ORDER BY bucket",),
        # PARTITION BY position syntax
        ("SELECT label, SUM(val) FROM {tbl} PARTITION BY 1 ORDER BY label",),
        # ORDER BY ordinal with deterministic tie-breaker
        ("SELECT label, COUNT(*) AS cnt FROM {tbl} GROUP BY label ORDER BY 2 DESC, label",),
    ],
    # ── cast: 类型转换（CAST）────────────────────────────────────────────────
    "cast": [
        ("SELECT id, CAST(val AS DOUBLE) FROM {tbl} ORDER BY time", dict(float_cols={1})),
        ("SELECT id, CHAR_LENGTH(CAST(val AS VARCHAR(10))) FROM {tbl} ORDER BY time",),
        ("SELECT id, CAST(score AS BIGINT) FROM {tbl} ORDER BY time",),
        ("SELECT CAST(val AS DOUBLE) FROM {tbl} WHERE val = 30 ORDER BY time", dict(float_cols={0})),
        ("SELECT CAST(score AS BINARY(16)) FROM {tbl} WHERE val = 10 ORDER BY time",),
    ],
    # ── sort: ORDER BY / LIMIT / DISTINCT / NULLS FIRST/LAST ─────────────────
    "sort": [
        ("SELECT id, val - 5 FROM {tbl} ORDER BY time",),
        ("SELECT id, val, score, label FROM {tbl} ORDER BY time", dict(float_cols={2})),
        ("SELECT id, val FROM {tbl} ORDER BY time LIMIT 3 OFFSET 1",),
        ("SELECT DISTINCT label FROM {tbl} ORDER BY label",),
        ("SELECT id, val FROM {tbl} ORDER BY val DESC",),
        ("SELECT id, -val FROM {tbl} ORDER BY time",),
        ("SELECT id, label, val FROM {tbl} ORDER BY label, val",),
        ("SELECT DISTINCT label, val FROM {tbl} ORDER BY label, val",),
        ("SELECT val, score FROM {tbl} ORDER BY val", dict(float_cols={1})),
        ("SELECT val FROM {tbl} ORDER BY val DESC",),
        ("SELECT val FROM {tbl} ORDER BY val LIMIT 2 OFFSET 2",),
        ("SELECT val FROM {tbl} ORDER BY val LIMIT 2 OFFSET 3",),
        ("SELECT val FROM {tbl} ORDER BY val LIMIT 10 OFFSET 100",),
        ("SELECT DISTINCT id FROM {tbl} ORDER BY id",),
        ("SELECT DISTINCT val, id FROM {tbl} ORDER BY val",),
        ("SELECT val FROM {tbl} ORDER BY val ASC LIMIT 2",),
        ("SELECT val FROM {tbl} ORDER BY val DESC LIMIT 2",),
        ("SELECT label, val FROM {tbl} ORDER BY LENGTH(label), val",),
        ("SELECT val FROM {tbl} ORDER BY val",),
        ("SELECT nullable_val FROM {tbl} ORDER BY nullable_val ASC NULLS LAST",),
        ("SELECT nullable_val FROM {tbl} ORDER BY nullable_val DESC NULLS FIRST",),
        ("SELECT /*+ para_tables_sort() */ val FROM {tbl} ORDER BY val",),
        # LIMIT shorthand: LIMIT offset, count
        ("SELECT val FROM {tbl} ORDER BY val LIMIT 1, 3",),
        # SELECT with explicit columns (ORDER BY time, LIMIT 2) - validates all-column scan in schema order.
        # Note: SELECT * is not used here because external sources (e.g. InfluxDB auto-discovered tables)
        # may return columns in their own discovery order rather than TDengine schema order.
        ("SELECT time, id, val, score, label, nullable_val, lat, lon, wkt_point, wkt_poly, ts_str"
         " FROM {tbl} ORDER BY time LIMIT 2", dict(precision_cols={0}, float_cols={3, 6, 7})),
        # Hint: SORT_FOR_GROUP
        ("SELECT /*+ SORT_FOR_GROUP() */ label, COUNT(*) FROM {tbl} PARTITION BY label ORDER BY label",),
        # Hint: PARTITION_FIRST
        ("SELECT /*+ PARTITION_FIRST() */ label, SUM(val) FROM {tbl} PARTITION BY label ORDER BY label",),
    ],
    # ── sub: 子查询（IN / NOT IN / EXISTS / ALL / ANY / SOME / scalar / derived）
    "sub": [
        ("SELECT id FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE label = 'north') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val NOT IN (SELECT val FROM {tbl} WHERE label = 'east') ORDER BY time",),
        ("SELECT id FROM {tbl} t1 WHERE EXISTS (SELECT 1 FROM {tbl} t2 WHERE t2.id = t1.id AND t2.label = 'north') ORDER BY time",
         dict(positive=False, reason="correlated EXISTS subquery as expr not supported (0x800026A6)")),
        ("SELECT id FROM {tbl} t1 WHERE NOT EXISTS (SELECT 1 FROM {tbl} t2 WHERE t2.id = t1.id AND t2.label = 'south') ORDER BY time",
         dict(positive=False, reason="correlated NOT EXISTS subquery as expr not supported (0x800026A6)")),
        ("SELECT id FROM {tbl} WHERE val > ALL (SELECT val FROM {tbl} WHERE val < 20) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val > ANY (SELECT val FROM {tbl} WHERE val < 30) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE val >= SOME (SELECT val FROM {tbl} WHERE val >= 30) ORDER BY time",),
        ("SELECT id, val, (SELECT AVG(val) FROM {tbl}) AS avg_val FROM {tbl} ORDER BY time", dict(float_cols={2})),
        ("SELECT id, val FROM {tbl} WHERE val > (SELECT AVG(val) FROM {tbl}) ORDER BY time",),
        ("SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {tbl} GROUP BY label) sub",
         dict(float_cols={0}, ordered=False)),
        ("SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {tbl}) sub ORDER BY id",),
        ("SELECT id FROM {tbl} WHERE id IN (SELECT id FROM {tbl} WHERE val > 30) ORDER BY id",),
        ("SELECT id FROM {tbl} WHERE id NOT IN (SELECT id FROM {tbl} WHERE val > 30) ORDER BY id",),
        ("SELECT AVG(v) FROM (SELECT val AS v FROM {tbl} WHERE val > 10) sub", dict(float_cols={0}, ordered=False)),
        ("SELECT val, (SELECT MAX(val) FROM {tbl}) AS mx FROM {tbl} ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val > ALL (SELECT val FROM {tbl} WHERE val < 20) ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val < ANY (SELECT val FROM {tbl} WHERE val = 50) ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE id = 1) ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE val NOT IN (SELECT val FROM {tbl} WHERE id = 1) ORDER BY val",),
        ("SELECT id FROM {tbl} WHERE EXISTS (SELECT 1 FROM {tbl} WHERE val > 10) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE NOT EXISTS (SELECT 1 FROM {tbl} WHERE val > 999) ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE NOT EXISTS (SELECT 1 FROM {tbl} WHERE val > 10) ORDER BY time",),
        ("SELECT val FROM {tbl} WHERE val > SOME (SELECT val FROM {tbl} WHERE label = 'south') ORDER BY val",),
        ("SELECT val FROM (SELECT val, label FROM {tbl}) t ORDER BY val",),
        # ALL with <= operator (val <= ALL values >= 40 → val <= 40)
        ("SELECT id FROM {tbl} WHERE val <= ALL (SELECT val FROM {tbl} WHERE val >= 40) ORDER BY time",),
        # ALL with <> operator (val <> east's val=50 → ids 1,2,3,4)
        ("SELECT id FROM {tbl} WHERE val <> ALL (SELECT val FROM {tbl} WHERE label = 'east') ORDER BY time",),
        # ANY with = operator (val equals any north val → 10 or 30)
        ("SELECT id FROM {tbl} WHERE val = ANY (SELECT val FROM {tbl} WHERE label = 'north') ORDER BY time",),
        # scalar subquery in arithmetic expression
        ("SELECT id, val - (SELECT MIN(val) FROM {tbl}) AS diff FROM {tbl} ORDER BY time",),
        # IN with UNION subquery
        ("SELECT id FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE id = 1 UNION SELECT val FROM {tbl} WHERE id = 5) ORDER BY time",),
        # CASE + scalar subquery
        ("SELECT id, CASE WHEN val > (SELECT AVG(val) FROM {tbl}) THEN 'high' ELSE 'low' END AS cat FROM {tbl} ORDER BY time",),
        # scalar subquery in HAVING
        ("SELECT label, SUM(val) AS s FROM {tbl} GROUP BY label HAVING SUM(val) > (SELECT AVG(val) FROM {tbl}) ORDER BY label",),
        # multi-level nesting (subquery inside IN inside FROM)
        ("SELECT id FROM (SELECT id, val FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE label = 'north')) sub ORDER BY id",),
        # EXISTS in CASE WHEN — non-correlated EXISTS subquery evaluated TDengine-side
        ("SELECT id, CASE WHEN EXISTS (SELECT 1 FROM {tbl} WHERE val > 40) THEN 'yes' ELSE 'no' END AS flag FROM {tbl} ORDER BY time",),
        # NULL three-valued logic (FQOR-LOCAL-007).  nullable_val is NULL for
        # id=2 and id=4, and label 'south' selects exactly those two rows, so
        # the subquery below is the single-element set {NULL}: every IN / NOT
        # IN / = ANY / > ALL comparison against it is UNKNOWN and matches no
        # row on any source.
        ("SELECT id FROM {tbl} WHERE nullable_val IN (SELECT nullable_val FROM {tbl} WHERE label = 'south') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE nullable_val NOT IN (SELECT nullable_val FROM {tbl} WHERE label = 'south') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE nullable_val = ANY (SELECT nullable_val FROM {tbl} WHERE label = 'south') ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE nullable_val > ALL (SELECT nullable_val FROM {tbl} WHERE label = 'south') ORDER BY time",),
        # A NULL in the value list must not create a spurious match.
        ("SELECT id FROM {tbl} WHERE id IN (SELECT nullable_val FROM {tbl}) ORDER BY time",),
        # Control: the 'north' list is NULL-free yet the two NULL rows still
        # never qualify, so only id 5 (50) survives.
        ("SELECT id, nullable_val FROM {tbl} WHERE nullable_val NOT IN (SELECT nullable_val FROM {tbl} WHERE label = 'north') ORDER BY time",),
    ],
    # ── union: UNION / UNION ALL ──────────────────────────────────────────────
    "union": [
        ("SELECT val FROM {tbl} WHERE id <= 2 UNION ALL SELECT val FROM {tbl} WHERE id <= 2 ORDER BY val",),
        ("SELECT label FROM {tbl} WHERE id IN (1,3) UNION SELECT label FROM {tbl} WHERE id IN (1,4) ORDER BY label",),
        ("SELECT id, label FROM {tbl} WHERE id <= 2 UNION ALL SELECT id, label FROM {tbl} WHERE id >= 4 ORDER BY id",),
        # 3-way UNION ALL（源自 fq_04 cross-source 三路 UNION）
        ("SELECT id, val FROM {tbl} WHERE id <= 2 UNION ALL SELECT id, val FROM {tbl} WHERE id BETWEEN 2 AND 4 UNION ALL SELECT id, val FROM {tbl} WHERE id >= 4 ORDER BY id, val",),
        ("SELECT val FROM {tbl} WHERE val > 0 UNION ALL SELECT val FROM {tbl} WHERE val < 0 ORDER BY val",),
    ],
    # ── join: JOIN（INNER / LEFT / RIGHT / FULL OUTER / CROSS / 3-way）────────
    "join": [
        ("SELECT a.id, a.val, b.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",),
        ("SELECT a.id AS aid, b.id AS bid FROM {tbl} a FULL OUTER JOIN {tbl} b ON a.id = b.id + 3 ORDER BY a.id, b.id",),
        ("SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {tbl} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {tbl} WHERE id >= 4) b ORDER BY a.id, b.id",),
        ("SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",),
        ("SELECT a.id, b.val, c.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id INNER JOIN {tbl} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",),
        # SEMI / ANTI JOIN — ordinary-column Hash Join positive cases
        ("SELECT a.id, a.val FROM {tbl} a LEFT SEMI JOIN {tbl} b ON a.id = b.id ORDER BY a.id",),
        ("SELECT a.id, a.val FROM {tbl} a LEFT ANTI JOIN {tbl} b ON a.id = b.id ORDER BY a.id",),
        ("SELECT b.id, b.val FROM {tbl} a RIGHT SEMI JOIN {tbl} b ON a.id = b.id ORDER BY b.id",),
        ("SELECT b.id, b.val FROM {tbl} a RIGHT ANTI JOIN {tbl} b ON a.id = b.id ORDER BY b.id",),
        # ts-pk 自连接 — 正例（源自 fq_06 013/033/s03）
        ("SELECT a.id, a.val, b.label FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a FULL OUTER JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        # RIGHT JOIN with ts-pk
        ("SELECT b.id, a.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.time = b.time ORDER BY b.id",),
        # LEFT OUTER JOIN — explicit OUTER keyword
        ("SELECT a.id, b.val FROM {tbl} a LEFT OUTER JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        # LEFT SEMI JOIN with ts-pk — all rows match in self-join
        ("SELECT a.id, a.val FROM {tbl} a LEFT SEMI JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        # RIGHT SEMI JOIN with ts-pk
        ("SELECT b.id, b.val FROM {tbl} a RIGHT SEMI JOIN {tbl} b ON a.time = b.time ORDER BY b.id",),
        # LEFT ANTI JOIN with ts-pk — self-join yields 0 rows
        ("SELECT a.id, a.val FROM {tbl} a LEFT ANTI JOIN {tbl} b ON a.time = b.time ORDER BY a.id",),
        # RIGHT ANTI JOIN with ts-pk — self-join yields 0 rows
        ("SELECT b.id, b.val FROM {tbl} a RIGHT ANTI JOIN {tbl} b ON a.time = b.time ORDER BY b.id",),
        # INNER JOIN + additional AND condition in ON clause
        ("SELECT a.id, b.val FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time AND a.val >= 30 ORDER BY a.id",),
        # LEFT JOIN + WHERE filter
        ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.time = b.time WHERE a.val > 20 ORDER BY a.id",),
        # INNER JOIN + aggregation with ts-pk
        ("SELECT COUNT(*) AS cnt, SUM(a.val) AS sv FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time WHERE a.val > 20", dict(ordered=False)),
        # 3-way INNER JOIN with ts-pk
        ("SELECT a.id, b.val, c.label FROM {tbl} a INNER JOIN {tbl} b ON a.time = b.time INNER JOIN {tbl} c ON b.time = c.time WHERE a.id <= 3 ORDER BY a.id",),
        # OUTER JOIN + ts-pk equality + one-sided predicate in ON
        ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.time = b.time AND b.val > 30 ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.time = b.time AND a.val < 30 ORDER BY b.id",),
        # RIGHT JOIN + nullable-side WHERE; unmatched rows must be filtered out.
        ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.time = b.time WHERE a.val < 30 ORDER BY b.id",),
        # RIGHT JOIN + preserved-side WHERE filter
        ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.time = b.time WHERE b.val > 20 ORDER BY b.id",),
        # JOIN TIMETRUNCATE sort-compatibility with external sources on both
        # branches.  These mirror the internal-domain coverage in
        # test_join_ts_sort.py, but validate result parity for MySQL/PG/Influx
        # federated scans.
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl}) a "
         "JOIN (SELECT time, id, val FROM {tbl}) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl} ORDER BY time DESC) a "
         "JOIN {tbl} b ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY time DESC) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl} ORDER BY time) a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY time) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl} ORDER BY val DESC) a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY time) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl} ORDER BY val DESC LIMIT 3) a "
         "JOIN {tbl} b ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM {tbl} a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY val LIMIT 2) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT time, id, val FROM {tbl} ORDER BY time DESC LIMIT 2) a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY time DESC LIMIT 2) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.time DESC LIMIT 2",),
        ("SELECT a.id, b.id FROM (SELECT time, id, val FROM {tbl} ORDER BY val DESC LIMIT 3) a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY val DESC LIMIT 3) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",),
        ("SELECT a.id, b.id FROM (SELECT time, id, val FROM {tbl} ORDER BY TIMETRUNCATE(time, 1m)) a "
         "JOIN (SELECT time, id, val FROM {tbl} ORDER BY val DESC) b "
         "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id LIMIT 3",),
        # Derived-table projection shapes (FQOR-LOCAL-014): partial columns,
        # SELECT * and an expression projection on both join inputs.  The outer
        # query projects named columns, so the star never depends on a source's
        # own column-discovery order.
        ("SELECT a.id, b.val FROM (SELECT id FROM {tbl}) a "
         "INNER JOIN (SELECT id, val FROM {tbl}) b ON a.id = b.id ORDER BY a.id",),
        ("SELECT a.id, b.val FROM (SELECT * FROM {tbl}) a "
         "INNER JOIN (SELECT * FROM {tbl}) b ON a.id = b.id ORDER BY a.id",),
        ("SELECT a.id, b.doubled FROM (SELECT id FROM {tbl}) a "
         "INNER JOIN (SELECT id, val * 2 AS doubled FROM {tbl}) b "
         "ON a.id = b.id ORDER BY a.id",),
    ],
    # ── asof: ASOF JOIN（源自 fq_04 asof_join）───────────────────────────────
    "asof": [
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time >= b.time ORDER BY a.time",),
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time >= b.time JLIMIT 1 ORDER BY a.time",),
        ("SELECT a.val, b.id, b.val AS b_val FROM {tbl} a RIGHT ASOF JOIN {tbl} b ON b.time >= a.time ORDER BY b.time",),
        # LEFT ASOF without ON clause — default >= behavior
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ORDER BY a.time",),
        # LEFT ASOF with > (strict: match right row with largest ts < left ts)
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time > b.time ORDER BY a.time",),
        # LEFT ASOF with = (exact match only)
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time = b.time ORDER BY a.time",),
        # LEFT ASOF with < (match right row with smallest ts > left ts)
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time < b.time ORDER BY a.time",),
        # LEFT ASOF with <= (match right row with smallest ts >= left ts)
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time <= b.time ORDER BY a.time",),
        # LEFT ASOF + WHERE filter
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time >= b.time WHERE a.val > 20 ORDER BY a.time",),
        # LEFT ASOF + JLIMIT 2
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT ASOF JOIN {tbl} b ON a.time >= b.time JLIMIT 2 ORDER BY a.time", dict(ordered=False)),
    ],
    # ── wjoin: WINDOW JOIN（源自 fq_04 window_join）─────────────────────────
    "wjoin": [
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-2m, 2m) ORDER BY a.time, b.time",),
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-2m, 2m) JLIMIT 1 ORDER BY a.time",),
        ("SELECT a.val, b.id, b.val AS b_val FROM {tbl} a RIGHT WINDOW JOIN {tbl} b WINDOW_OFFSET(-2m, 2m) ORDER BY b.time, a.time",),
        # LEFT WINDOW JOIN + aggregation (COUNT + AVG in window)
        ("SELECT a.id, a.val, COUNT(b.val) AS b_cnt, AVG(b.val) AS b_avg FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-1m, 1m) ORDER BY a.time", dict(float_cols={3})),
        # LEFT WINDOW JOIN + HAVING (aggregate filter per window)
        ("SELECT a.id, a.val, AVG(b.val) AS b_avg FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-1m, 1m) HAVING AVG(b.val) > 20 ORDER BY a.time", dict(float_cols={2})),
        # LEFT WINDOW JOIN + WHERE filter
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-2m, 2m) WHERE a.val > 20 ORDER BY a.time, b.time",),
        # LEFT WINDOW JOIN — asymmetric offset (past only)
        ("SELECT a.id, a.val, b.val AS b_val FROM {tbl} a LEFT WINDOW JOIN {tbl} b WINDOW_OFFSET(-1m, 0s) ORDER BY a.time, b.time",),
    ],
    # ── win: 时间窗口（INTERVAL / FILL / PARTITION BY / SESSION /
    #                   STATE_WINDOW / EVENT_WINDOW / COUNT_WINDOW）────────────
    "win": [
        ("SELECT COUNT(*) AS cnt FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),
        ("SELECT AVG(score) AS avg_s FROM {tbl} INTERVAL(1m) ORDER BY _wstart", dict(float_cols={0})),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} INTERVAL(2m) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart", dict(float_cols={0})),
        ("SELECT label, COUNT(*) AS cnt FROM {tbl} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart",),
        ("SELECT COUNT(*) AS cnt FROM {tbl} SESSION(time, 30s) ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} SESSION(time, 2m) ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} STATE_WINDOW(val >= 30) ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt FROM {tbl} STATE_WINDOW(label) ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} EVENT_WINDOW START WITH val >= 30 END WITH val >= 50 ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} COUNT_WINDOW(2) ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} COUNT_WINDOW(3) ORDER BY _wstart",),
        ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",),
        ("SELECT COUNT(*) AS cnt FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} SESSION(time, 2m)",),
        ("SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 20 END WITH val < 40 ORDER BY _wstart",),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} COUNT_WINDOW(2) ORDER BY _wstart",),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NULL) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(PREV) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NEXT) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, COUNT(*) FROM {tbl} " + _W5M + " PARTITION BY id INTERVAL(1m) ORDER BY id, _wstart",),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(id) ORDER BY _wstart",),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(CASE WHEN val > 30 THEN 1 ELSE 0 END) ORDER BY _wstart",),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NONE) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NEAR) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NULL_F) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(VALUE_F, 0) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, _wend, _wduration, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart", dict(precision_cols={2})),
        ("SELECT _wstart, _wend, w.win_max, COUNT(*), AVG(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend, MAX(val) win_max FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart", dict(float_cols={4})),
        ("SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} EVENT_WINDOW START WITH val >= 20 END WITH val >= 40) w) ORDER BY _wstart",),
        ("SELECT _wstart, _wend, id, COUNT(*), SUM(val) FROM {tbl} PARTITION BY id EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY id, _wstart",),
        ("SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) HAVING COUNT(*) >= 2 ORDER BY _wstart",),
        ("SELECT _wstart, _wend, COUNT(*), MAX(val), MIN(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart",),
        ("SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 20 END WITH val < 40 TRUE_FOR(1m)", dict(ordered=False)),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} " + _W5M + " INTERVAL(2m) SLIDING(1m) ORDER BY _wstart",),
        # PARTITION BY + INTERVAL + LIMIT（非下推 LIMIT，源自 fq_06 010）
        ("SELECT label, COUNT(*) AS cnt FROM {tbl} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart LIMIT 3",),
        ("SELECT _QSTART, _QEND, COUNT(*) FROM {tbl} " + _W5M + " INTERVAL(1m) ORDER BY _wstart", dict(precision_cols={1})),
        # _WEND + _WDURATION pseudo-columns in plain INTERVAL context
        ("SELECT _wstart, _wend, _wduration, COUNT(*), SUM(val) FROM {tbl} INTERVAL(1m) ORDER BY _wstart", dict(precision_cols={0, 1, 2})),
        # INTERVAL with offset
        ("SELECT _wstart, _wend, COUNT(*) FROM {tbl} " + _W5M + " INTERVAL(2m, 30s) ORDER BY _wstart",),
        # COUNT_WINDOW with sliding
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} COUNT_WINDOW(3, 2) ORDER BY _wstart",),
        # PARTITION BY + SESSION
        ("SELECT label, _wstart, COUNT(*) FROM {tbl} PARTITION BY label SESSION(time, 30s) ORDER BY label, _wstart",),
        # PARTITION BY + EVENT_WINDOW
        ("SELECT label, _wstart, COUNT(*), SUM(val) FROM {tbl} PARTITION BY label EVENT_WINDOW START WITH val >= 20 END WITH val >= 40 ORDER BY label, _wstart",),
        # PARTITION BY + COUNT_WINDOW
        ("SELECT label, _wstart, COUNT(*) FROM {tbl} PARTITION BY label COUNT_WINDOW(2) ORDER BY label, _wstart",),
        # PARTITION BY + STATE_WINDOW
        ("SELECT label, _wstart, COUNT(*), SUM(val) FROM {tbl} PARTITION BY label STATE_WINDOW(val >= 30) ORDER BY label, _wstart",),
        # EXTERNAL_WINDOW from SESSION
        ("SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} SESSION(time, 30s)) w) ORDER BY _wstart",),
        # STATE_WINDOW + TRUE_FOR (duration filter)
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(val >= 30) TRUE_FOR(2m)", dict(ordered=False)),
        # EVENT_WINDOW + TRUE_FOR(COUNT n) (count filter)
        ("SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 10 END WITH val >= 40 TRUE_FOR(COUNT 2)", dict(ordered=False)),
        # FILL SURROUND with INTERVAL (surrounding time window limit)
        ("SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(PREV) SURROUND(1m, 0) ORDER BY _wstart", dict(float_cols={1})),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} "
         "EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} " + _W5M + " "
         "INTERVAL(30s) FILL(NULL)) w) FILL(NULL) ORDER BY _wstart",
         dict()),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} "
         "EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} " + _W5M + " "
         "INTERVAL(30s) FILL(NULL)) w) FILL(VALUE, 0, 0) ORDER BY _wstart",
         dict()),
        ("SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} "
         "EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} " + _W5M + " "
         "INTERVAL(30s) FILL(NULL)) w) FILL(PREV) ORDER BY _wstart",
         dict()),
    ],
    # ── regex: MATCH/NMATCH/REGEXP/NOT REGEXP 正则匹配
    "regex": [
        ("SELECT val FROM {tbl} WHERE label MATCH '^n' ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE label NMATCH '^n' ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE label REGEXP '^s' ORDER BY val",),
        ("SELECT val FROM {tbl} WHERE label NOT REGEXP '^s' ORDER BY val",),
    ],
    # ── dt: 日期时间函数（TIMEDIFF/TIMETRUNCATE/TO_ISO8601/WEEKOFYEAR）
    "dt": [
        ("SELECT TIMEDIFF('2024-01-01', '2024-01-01') FROM {tbl} LIMIT 1",),
        ("SELECT TIMETRUNCATE(time, 1h) FROM {tbl} ORDER BY time LIMIT 1",),
        ("SELECT CAST(time AS BIGINT) FROM {tbl} ORDER BY time LIMIT 1", dict(precision_cols={0})),
        ("SELECT TO_ISO8601(time) FROM {tbl} ORDER BY time LIMIT 1", dict(precision_cols={0})),
        ("SELECT WEEKOFYEAR(time) FROM {tbl} ORDER BY time LIMIT 1",),
        ("SELECT TIMEZONE() FROM {tbl} LIMIT 1",),
    ],
    # ── ts: TDengine 时序函数（FIRST/LAST/TOP/BOTTOM/DIFF/CSUM 等）
    "ts": [
        ("SELECT FIRST(val) FROM {tbl}",),
        ("SELECT LAST(val) FROM {tbl}",),
        ("SELECT TOP(val, 2) FROM {tbl}",),
        ("SELECT BOTTOM(val, 2) FROM {tbl}",),
        ("SELECT ELAPSED(time) FROM {tbl}", dict(precision_cols={0})),
        ("SELECT ELAPSED(time, 1s) FROM {tbl}",),
        ("SELECT HYPERLOGLOG(val) FROM {tbl}", dict(ordered=False)),
        ("SELECT DIFF(val) FROM {tbl}",),
        ("SELECT CSUM(val) FROM {tbl}",),
        ("SELECT LAST_ROW(val) FROM {tbl}",),
        ("SELECT TAIL(val, 2) FROM {tbl}",),
        ("SELECT TWA(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT HISTOGRAM(val, 'user_input', '[0, 60, 100]', 0) FROM {tbl}",),
        ("SELECT * FROM (SELECT time, DIFF(val) AS d FROM {tbl})",),
        ("SELECT DERIVATIVE(val, 1m, 0) FROM {tbl}", dict(float_cols={0})),
        ("SELECT IRATE(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),
        ("SELECT MAVG(val, 2) FROM {tbl}", dict(float_cols={0})),
        ("SELECT STATECOUNT(val, 'GT', 20) FROM {tbl}",),
        ("SELECT STATEDURATION(val, 'GT', 20, 1s) FROM {tbl}",),
        ("SELECT _ROWTS, val FROM {tbl} ORDER BY time",),
        ("SELECT UNIQUE(val) FROM {tbl}", dict(ordered=False)),
        ("SELECT MODE(label) FROM {tbl} WHERE id <= 3", dict(ordered=False)),
        ("SELECT MODE(val) FROM {tbl}", dict(validate_in={10, 20, 30, 40, 50})),
        ("SELECT SAMPLE(val, 3) FROM {tbl}", dict(validate_in={10, 20, 30, 40, 50})),
    ],
    # ── lag: 窗口函数（LAG/LEAD 与 ROW_NUMBER/RANK/DENSE_RANK/聚合窗口）──────
    # Window functions are evaluated by TDengine on top of the external scan
    # (FQOR-LOCAL-008), so partitioning, ordering and the NULL position must
    # match the local reference exactly.
    "lag": [
        ("SELECT val, LAG(val, 1) FROM {tbl} ORDER BY time",),
        ("SELECT val, LEAD(val, 1) FROM {tbl} ORDER BY time",),
        ("SELECT id, ROW_NUMBER() OVER (PARTITION BY label ORDER BY time) AS rn FROM {tbl} ORDER BY id",),
        ("SELECT id, RANK() OVER (PARTITION BY label ORDER BY val) AS rk FROM {tbl} ORDER BY id",),
        ("SELECT id, DENSE_RANK() OVER (ORDER BY label) AS dr FROM {tbl} ORDER BY id",),
        ("SELECT id, SUM(val) OVER (PARTITION BY label ORDER BY time) AS running FROM {tbl} ORDER BY id",),
        ("SELECT id, COUNT(*) OVER (PARTITION BY label) AS cnt FROM {tbl} ORDER BY id",),
        # NULL ordering inside OVER(): NULLS LAST is explicit so the rank of
        # the two NULL rows does not depend on a provider default.
        ("SELECT id, RANK() OVER (PARTITION BY label ORDER BY nullable_val NULLS LAST) AS rk FROM {tbl} ORDER BY id",),
        ("SELECT id, LAG(nullable_val, 1) OVER (PARTITION BY label ORDER BY time) AS prev FROM {tbl} ORDER BY id",),
        ("SELECT id, FIRST_VALUE(val) OVER (PARTITION BY label ORDER BY time) AS fv FROM {tbl} ORDER BY id",),
    ],
    # ── pscol: 伪列测试（_C0 / TAGS / TBNAME）
    # Note: tbname-based queries succeed on local TDengine but fail on external sources
    # (non-parity behavior) — only SELECT TAGS errors on all sources including local
    "pscol": [
        ("SELECT tags FROM {tbl}", dict(positive=False, reason="SELECT TAGS pseudo-column not supported (TSDB_CODE_PAR_SYNTAX_ERROR)")),
        # _C0 pseudo-column (alias of primary timestamp _ROWTS)
        ("SELECT _C0, val FROM {tbl} ORDER BY time", dict(precision_cols={0})),
    ],
    # ── mask: 数据脱敏函数（MASK_FULL/MASK_PARTIAL）
    "mask": [
        ("SELECT MASK_FULL(label) FROM {tbl} ORDER BY val LIMIT 1", dict(positive=False, reason="MASK_FULL with 1 argument invalid - requires (string, mask_char) (TSDB_CODE_FUNC_FUNTION_PARA_NUM)")),
        ("SELECT MASK_FULL(label, 'X') FROM {tbl} ORDER BY val LIMIT 1",),
        ("SELECT MASK_PARTIAL(label, 2, 'X') FROM {tbl} WHERE val = 10", dict(positive=False, reason="MASK_PARTIAL with 3 args invalid - requires 4 (TSDB_CODE_FUNC_FUNTION_PARA_NUM)")),
        ("SELECT label, MASK_PARTIAL(label, 0, 2, 'X') FROM {tbl} WHERE val = 10 ORDER BY time",),
    ],
    # ── slimit: SLIMIT/SOFFSET 分区限制
    "slimit": [
        ("SELECT id, AVG(val) FROM {tbl} PARTITION BY id ORDER BY id SLIMIT 5", dict(float_cols={1})),
        # SLIMIT with SOFFSET
        ("SELECT id, AVG(val) FROM {tbl} PARTITION BY id ORDER BY id SLIMIT 3 SOFFSET 1", dict(float_cols={1})),
    ],
    # ── null: nullable_val 列的 NULL 处理（IS NULL / IFNULL / NVL / FILL_FORWARD 等）
    # nullable_val: 10, NULL, 30, NULL, 50  (id=2 and id=4 are NULL)
    "null": [
        ("SELECT id, IFNULL(nullable_val, 99) FROM {tbl} ORDER BY time",),
        ("SELECT id, NVL(nullable_val, 99) FROM {tbl} ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE nullable_val IS NULL ORDER BY time",),
        ("SELECT id FROM {tbl} WHERE nullable_val IS NOT NULL ORDER BY time",),
        ("SELECT id, COALESCE(nullable_val, 0) FROM {tbl} ORDER BY time",),
        ("SELECT id, NVL2(nullable_val, 'has', 'no') FROM {tbl} ORDER BY time",),
        ("SELECT id, NULLIF(nullable_val, 30) FROM {tbl} WHERE id IN (1, 3) ORDER BY time",),
        ("SELECT id, IF(nullable_val > 0, 'positive', 'other') FROM {tbl} ORDER BY time",),
        ("SELECT id, ISNOTNULL(nullable_val) FROM {tbl} ORDER BY time",),
        ("SELECT FILL_FORWARD(nullable_val) FROM {tbl} ORDER BY time",),
        # The two NULL rows tie on nullable_val; the id key keeps the order
        # deterministic across sources (a tie order is not specified, and
        # Oracle's sort puts the tie in a different order than a table scan).
        ("SELECT id, nullable_val FROM {tbl} ORDER BY nullable_val NULLS FIRST, id",),
        ("SELECT id, nullable_val FROM {tbl} ORDER BY nullable_val NULLS LAST, id",),
    ],
    # ── corr: CORR 相关系数（val/score 完全线性相关 → CORR = 1.0）
    "corr": [
        ("SELECT CORR(val, score) FROM {tbl}", dict(float_cols={0}, ordered=False)),
    ],
    # ── geo: 坐标距离（SQRT + POW 欧几里得距离）
    # lat/lon: (116.4,39.9), (121.5,31.2), (104.0,30.6), (108.0,34.2), (113.0,28.1)
    "geo": [
        ("SELECT id, SQRT(POW(lat - lat, 2) + POW(lon - lon, 2)) AS dist FROM {tbl} ORDER BY time",
         dict(float_cols={1})),
        ("SELECT SQRT(POW(lat - 116.4, 2) + POW(lon - 39.9, 2)) AS dist FROM {tbl} WHERE id = 1",
         dict(float_cols={0}, ordered=False)),
        ("SELECT id, SQRT(POW(lat - 116.4, 2) + POW(lon - 39.9, 2)) AS dist FROM {tbl} WHERE id = 2 ORDER BY time",
         dict(float_cols={1})),
    ],
    # ── geost: GeoS2 几何函数（ST_Contains / ST_Intersects / ST_Equals / ST_Covers / ST_Touches）
    # wkt_point: POINT(5 5), POINT(15 15), POINT(3 3), POINT(12 12), POINT(8 8)
    # wkt_poly : POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))  (same for all rows)
    "geost": [
        ("SELECT id, ST_Contains(ST_GeomFromText(wkt_poly), ST_GeomFromText(wkt_point)) FROM {tbl} ORDER BY time",),
        ("SELECT id, ST_ContainsProperly(ST_GeomFromText(wkt_poly), ST_GeomFromText(wkt_point)) FROM {tbl} ORDER BY time",),
        ("SELECT id, ST_Intersects(ST_GeomFromText(wkt_poly), ST_GeomFromText(wkt_point)) FROM {tbl} ORDER BY time",),
        ("SELECT id, ST_Equals(ST_GeomFromText(wkt_poly), ST_GeomFromText(wkt_poly)) FROM {tbl} ORDER BY time",),
        ("SELECT id, ST_Covers(ST_GeomFromText(wkt_poly), ST_GeomFromText(wkt_point)) FROM {tbl} ORDER BY time",),
        ("SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(0 0)')) FROM {tbl} LIMIT 1",),
        ("SELECT ST_AsText(ST_GeomFromText(wkt_point)) FROM {tbl} WHERE id = 1",),
        ("SELECT id, ST_AsText(ST_GeomFromText(wkt_poly)) FROM {tbl} ORDER BY time",),
    ],
    # ── enc: AES/SM4 加解密 + MD5/CRC32；NCHAR 输入是否支持按函数当前实际行为校验
    "enc": [
        ("SELECT MD5(label) FROM {tbl} WHERE id = 1",),
        ("SELECT AES_ENCRYPT(label, 'mykeystring12345') FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="AES_ENCRYPT on NCHAR input is rejected by current engine behavior")),
        ("SELECT AES_DECRYPT(AES_ENCRYPT(CAST(label AS VARCHAR(32)), 'mykeystring12345'), 'mykeystring12345') FROM {tbl} WHERE id = 1",),
        ("SELECT SM4_DECRYPT(SM4_ENCRYPT(CAST(label AS VARCHAR(32)), 'mykeystring12345'), 'mykeystring12345') FROM {tbl} WHERE id = 1",),
        ("SELECT CRC32(CAST(label AS VARCHAR(32))) FROM {tbl} WHERE id = 1", dict(ordered=False)),
    ],
    # ── dtf: 日期函数（DATE / DAYOFWEEK / WEEK / WEEKDAY）
    # 2024-01-01 is a Monday: DAYOFWEEK=2, WEEK=0, WEEKDAY=0
    # YEAR/HOUR/MINUTE are not TDengine functions → error on all sources
    "dtf": [
        ("SELECT DATE(time) FROM {tbl} WHERE id = 1 ORDER BY time",),
        ("SELECT DAYOFWEEK(time) FROM {tbl} WHERE id = 1 ORDER BY time",),
        ("SELECT WEEK(time) FROM {tbl} WHERE id = 1 ORDER BY time",),
        ("SELECT WEEKDAY(time) FROM {tbl} WHERE id = 1 ORDER BY time",),
        ("SELECT YEAR(time) FROM {tbl} LIMIT 1",
         dict(positive=False, reason="YEAR() is not a TDengine function (TSDB_CODE_MND_FUNC_NOT_EXIST)")),
        ("SELECT HOUR(time) FROM {tbl} LIMIT 1",
         dict(positive=False, reason="HOUR() is not a TDengine function (TSDB_CODE_MND_FUNC_NOT_EXIST)")),
        ("SELECT MINUTE(time) FROM {tbl} LIMIT 1",
         dict(positive=False, reason="MINUTE() is not a TDengine function (TSDB_CODE_MND_FUNC_NOT_EXIST)")),
    ],
    # ── tochar: TO_CHAR / TO_TIMESTAMP / TO_UNIXTIMESTAMP
    "tochar": [
        ("SELECT TO_CHAR(time, 'yyyy-MM-dd') FROM {tbl} WHERE id = 1 ORDER BY time",),
        ("SELECT TO_TIMESTAMP(ts_str, 'YYYY-MM-DD HH24:mi:ss') FROM {tbl} WHERE id = 1 ORDER BY time",),
        # TO_UNIXTIMESTAMP returns the epoch in the column's own precision.
        # MySQL/PG/InfluxDB push the conversion to the remote source (rendered as
        # epoch * 1000 -> ms, see DS 5.3.x TO_UNIXTIMESTAMP); SQL Server runs the
        # baseline (no pushdown) so the function is evaluated locally against the
        # microsecond-precision external column and yields us.  Accept the source
        # precision factor for that column only.
        ("SELECT TO_UNIXTIMESTAMP(time) FROM {tbl} WHERE id = 1", dict(precision_cols={0})),
    ],
    # ── interp: INTERP 时间序列插值（RANGE / EVERY / FILL）
    # Data: val=10,20,30,40,50 at 1-minute intervals from 2024-01-01 00:00:00
    "interp": [
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(1m) FILL(linear)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(prev)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(next)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(null)",),
        ("SELECT interp(val) FROM {tbl} RANGE('2024-01-01 00:01:30.000') FILL(prev) SURROUND(1m, NULL)",),
        ("SELECT _IROWTS, interp(val) FROM {tbl} " + _R4M + " EVERY(1m) FILL(linear)",),
        # _IROWTS_ORIGIN pseudo-column with FILL(PREV)
        ("SELECT _IROWTS_ORIGIN, interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(PREV)", dict(precision_cols={0})),
        # _IROWTS_ORIGIN pseudo-column with FILL(NEXT)
        ("SELECT _IROWTS_ORIGIN, interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(NEXT)", dict(precision_cols={0})),
        # INTERP with FILL(NEAR)
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(NEAR)",),
        # INTERP with FILL(VALUE)
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(VALUE, 0)",),
    ],
    # ── sysfn: 系统信息函数（CLIENT_VERSION / SERVER_VERSION / CURRENT_USER / DATABASE）
    # 这些函数由 TDengine 本地计算，不推送到外部源，4库结果应一致
    "sysfn": [
        # CLIENT_VERSION / SERVER_VERSION return the build version string, which
        # changes on every version bump — mask it so the baseline stays stable.
        ("SELECT CLIENT_VERSION() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        ("SELECT SERVER_VERSION() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        ("SELECT CURRENT_USER() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        ("SELECT DATABASE() FROM {tbl} LIMIT 1",),
        # SERVER_STATUS() — returns 1 if server is running
        ("SELECT SERVER_STATUS() FROM {tbl} LIMIT 1",),
        # USER() — alias for CURRENT_USER()
        ("SELECT USER() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        # NOW() — returns current timestamp; dynamic_cols masks baseline, time-range validates value
        ("SELECT NOW() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        # TODAY() — returns beginning of today (00:00:00); stable within a day
        ("SELECT TODAY() FROM {tbl} LIMIT 1", dict(dynamic_cols={0})),
        # NOW() in expression — verify arithmetic with NOW()
        ("SELECT CASE WHEN NOW() > '2024-01-01 00:00:00' THEN 1 ELSE 0 END FROM {tbl} LIMIT 1",),
        # TODAY() in expression — verify arithmetic with TODAY()
        ("SELECT CASE WHEN TODAY() >= '2024-01-01' THEN 1 ELSE 0 END FROM {tbl} LIMIT 1",),
    ],
    # ── json: JSON 操作符拒绝 + LIKE 变通 + TO_JSON 当前实际返回
    "json": [
        ("SELECT label->'k' FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="'->' on NCHAR column → PAR_INVALID_COL_JSON")),
        ("SELECT id FROM {tbl} WHERE label CONTAINS 'num'",
         dict(positive=False, reason="CONTAINS on NCHAR column → PAR_INVALID_COL_JSON")),
        ("SELECT id FROM {tbl} WHERE label LIKE '%orth%' ORDER BY time",),
        ("SELECT TO_JSON(label) FROM {tbl} WHERE id = 1",),
    ],
    # ── grpconcat: GROUP_CONCAT 函数映射（MySQL GROUP_CONCAT / PG STRING_AGG / InfluxDB 本地执行）
    # 每个 id 仅对应一行 → GROUP_CONCAT 结果唯一确定（单元素无二义性）
    "grpconcat": [
        ("SELECT id, GROUP_CONCAT(label, ',') AS g FROM {tbl} GROUP BY id ORDER BY id",),
        ("SELECT id, GROUP_CONCAT(CAST(val AS VARCHAR(8)), '') AS g FROM {tbl} GROUP BY id ORDER BY id",),
    ],
}

# Flat list derived from _PARITY_GROUPS; each entry: (case_id, sql_template, opts)
# case_id format: "<group>-<NN>"  e.g. "whr-01", "win-19"
_parity_group_items = list(_PARITY_GROUPS.items())

_PARITY_CASES: List[Tuple[str, str, dict]] = [
    (f"{grp}-{i:02d}", entry[0], entry[1] if len(entry) > 1 else {})
    for grp, entries in _parity_group_items
    for i, entry in enumerate(entries, 1)
]

# ── Epoch parity cases: window queries with _wstart/_wend in SELECT ──
# These are run via `taos -r` (raw epoch output) to validate that
# timestamp precision (ms/µs/ns) propagates correctly from each
# external source.  Each entry: (case_id, sql_template, opts)
# where opts["ts_cols"] marks column indices that hold timestamps.
_WIN_EPOCH_CASES: List[Tuple[str, str, dict]] = [
    # SESSION — _wstart at col 0
    ("ewin-01", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} SESSION(time, 2m)", dict(ts_cols={0})),
    # EVENT_WINDOW — _wstart at col 0
    ("ewin-02", "SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 20 END WITH val < 40 ORDER BY _wstart", dict(ts_cols={0})),
    # COUNT_WINDOW — _wstart at col 0
    ("ewin-03", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} COUNT_WINDOW(2) ORDER BY _wstart", dict(ts_cols={0})),
    # FILL variants — _wstart at col 0
    ("ewin-04", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NULL) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-05", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-06", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(PREV) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-07", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NEXT) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-08", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    # PARTITION BY + INTERVAL — _wstart at col 0
    ("ewin-09", "SELECT _wstart, COUNT(*) FROM {tbl} " + _W5M + " PARTITION BY id INTERVAL(1m) ORDER BY id, _wstart", dict(ts_cols={0})),
    # STATE_WINDOW — _wstart at col 0
    ("ewin-10", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(id) ORDER BY _wstart", dict(ts_cols={0})),
    ("ewin-11", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(CASE WHEN val > 30 THEN 1 ELSE 0 END) ORDER BY _wstart", dict(ts_cols={0})),
    # FILL(NONE/NEAR/NULL_F/VALUE_F) — _wstart at col 0
    ("ewin-12", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NONE) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-13", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NEAR) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-14", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(NULL_F) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    ("ewin-15", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(VALUE_F, 0) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
    # EXTERNAL_WINDOW — _wstart/_wend at col 0,1
    ("ewin-16", "SELECT _wstart, _wend, _wduration, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart", dict(ts_cols={0, 1, 2})),
    ("ewin-17", "SELECT _wstart, _wend, w.win_max, COUNT(*), AVG(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend, MAX(val) win_max FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart", dict(ts_cols={0, 1}, float_cols={4})),
    ("ewin-18", "SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} EVENT_WINDOW START WITH val >= 20 END WITH val >= 40) w) ORDER BY _wstart", dict(ts_cols={0, 1})),
    ("ewin-19", "SELECT _wstart, _wend, id, COUNT(*), SUM(val) FROM {tbl} PARTITION BY id EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY id, _wstart", dict(ts_cols={0, 1})),
    ("ewin-20", "SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) HAVING COUNT(*) >= 2 ORDER BY _wstart", dict(ts_cols={0, 1})),
    ("ewin-21", "SELECT _wstart, _wend, COUNT(*), MAX(val), MIN(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(2m)) w) ORDER BY _wstart", dict(ts_cols={0, 1})),
    # EVENT_WINDOW + TRUE_FOR — _wstart at col 0
    ("ewin-22", "SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 20 END WITH val < 40 TRUE_FOR(1m)", dict(ts_cols={0})),
    # SLIDING — _wstart at col 0
    ("ewin-23", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} " + _W5M + " INTERVAL(2m) SLIDING(1m) ORDER BY _wstart", dict(ts_cols={0})),
    # _QSTART, _QEND — ts at col 0,1
    ("ewin-24", "SELECT _QSTART, _QEND, COUNT(*) FROM {tbl} " + _W5M + " INTERVAL(1m) ORDER BY _wstart", dict(ts_cols={0, 1})),
    # _wend + _wduration in plain INTERVAL — ts at col 0,1
    ("ewin-25", "SELECT _wstart, _wend, _wduration, COUNT(*), SUM(val) FROM {tbl} INTERVAL(1m) ORDER BY _wstart", dict(ts_cols={0, 1, 2})),
    # INTERVAL with offset — ts at col 0,1
    ("ewin-26", "SELECT _wstart, _wend, COUNT(*) FROM {tbl} " + _W5M + " INTERVAL(2m, 30s) ORDER BY _wstart", dict(ts_cols={0, 1})),
    # COUNT_WINDOW with sliding — _wstart at col 0
    ("ewin-27", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} COUNT_WINDOW(3, 2) ORDER BY _wstart", dict(ts_cols={0})),
    # PARTITION BY + SESSION — label at col 0, _wstart at col 1
    ("ewin-28", "SELECT label, _wstart, COUNT(*) FROM {tbl} PARTITION BY label SESSION(time, 30s) ORDER BY label, _wstart", dict(ts_cols={1})),
    # PARTITION BY + EVENT_WINDOW — label at col 0, _wstart at col 1
    ("ewin-29", "SELECT label, _wstart, COUNT(*), SUM(val) FROM {tbl} PARTITION BY label EVENT_WINDOW START WITH val >= 20 END WITH val >= 40 ORDER BY label, _wstart", dict(ts_cols={1})),
    # PARTITION BY + COUNT_WINDOW — label at col 0, _wstart at col 1
    ("ewin-30", "SELECT label, _wstart, COUNT(*) FROM {tbl} PARTITION BY label COUNT_WINDOW(2) ORDER BY label, _wstart", dict(ts_cols={1})),
    # PARTITION BY + STATE_WINDOW — label at col 0, _wstart at col 1
    ("ewin-31", "SELECT label, _wstart, COUNT(*), SUM(val) FROM {tbl} PARTITION BY label STATE_WINDOW(val >= 30) ORDER BY label, _wstart", dict(ts_cols={1})),
    # EXTERNAL_WINDOW from SESSION — ts at col 0,1
    ("ewin-32", "SELECT _wstart, _wend, COUNT(*), SUM(val) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} SESSION(time, 30s)) w) ORDER BY _wstart", dict(ts_cols={0, 1})),
    # STATE_WINDOW + TRUE_FOR — _wstart at col 0
    ("ewin-33", "SELECT _wstart, COUNT(*), SUM(val) FROM {tbl} STATE_WINDOW(val >= 30) TRUE_FOR(2m)", dict(ts_cols={0})),
    # EVENT_WINDOW + TRUE_FOR(COUNT n) — _wstart at col 0
    ("ewin-34", "SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 10 END WITH val >= 40 TRUE_FOR(COUNT 2)", dict(ts_cols={0})),
    # FILL SURROUND — _wstart at col 0
    ("ewin-35", "SELECT _wstart, AVG(val) FROM {tbl} " + _W5M + " INTERVAL(30s) FILL(PREV) SURROUND(1m, 0) ORDER BY _wstart", dict(ts_cols={0}, float_cols={1})),
]


# Explicit-source matrix for JOIN TIMETRUNCATE sort compatibility.  Unlike
# _PARITY_GROUPS, these templates can mix local/MySQL/PG in one SQL.
# Each entry: (case_id, sql_template, opts).  The local reference SQL is built
# by replacing every source placeholder with the local TDengine table.
#
# Keep this matrix on same-precision sources (MySQL/PG/Oracle are all us).
# InfluxDB is still covered by _PARITY_GROUPS single-source parity cases;
# mixing it with MySQL/PG/local under TIMETRUNCATE produces cross-precision
# join semantics that are outside this sort-compatibility regression.
# Oracle × InfluxDB is covered by test_fq_oracle_cross_source_join, which joins
# on the timestamp column instead of a TIMETRUNCATE key.
_JOIN_TS_EXT_SORT_MATRIX: list[tuple[str, str, dict]] = [
    ("mx-01",
     "SELECT a.id, b.val FROM (SELECT time, id, val FROM {mysql}) a "
     "JOIN (SELECT time, id, val FROM {pg}) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",
     {}),
    ("mx-02",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY time DESC LIMIT 3) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY time ASC LIMIT 3) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("mx-03",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY val DESC LIMIT 3) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY val ASC LIMIT 3) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("tz-01",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY TIMETRUNCATE(time, 1d, 0)) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY TIMETRUNCATE(time, 1d, 1)) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("tz-02",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY TIMETRUNCATE(time, 1d, 0)) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY time) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("tz-03",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {pg} ORDER BY TIMETRUNCATE(time, 1d, 1)) a "
     "JOIN (SELECT time, id, val FROM {mysql} ORDER BY time DESC) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("gran-01",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY TIMETRUNCATE(time, 1h)) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY val DESC) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("gran-02",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {pg} ORDER BY TIMETRUNCATE(time, 1w)) a "
     "JOIN (SELECT time, id, val FROM {mysql} ORDER BY time DESC) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("nlim-01",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY time DESC LIMIT 2) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY time DESC LIMIT 2) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) LIMIT 2",
     dict(ordered=False)),
    ("left-01",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {mysql} ORDER BY val DESC LIMIT 3) a "
     "LEFT JOIN (SELECT time, id, val FROM {pg} WHERE id <= 2 ORDER BY time) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
    ("right-01",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {pg} WHERE id <= 2 ORDER BY time) a "
     "RIGHT JOIN (SELECT time, id, val FROM {mysql} ORDER BY val DESC LIMIT 3) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY b.id, a.id",
     {}),
    ("local-01",
     "SELECT a.id, b.val FROM (SELECT time, id, val FROM {local_us} ORDER BY time DESC LIMIT 3) a "
     "JOIN (SELECT time, id, val FROM {mysql}) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",
     {}),
    ("local-02",
     "SELECT a.id, b.val FROM (SELECT time, id, val FROM {pg} ORDER BY val DESC LIMIT 3) a "
     "JOIN (SELECT time, id, val FROM {local_us} ORDER BY time) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id",
     {}),
    ("local-03",
     "SELECT a.id, b.id FROM (SELECT time, id, val FROM {local_us} ORDER BY time DESC LIMIT 2) a "
     "JOIN (SELECT time, id, val FROM {pg} ORDER BY time ASC LIMIT 5) b "
     "ON TIMETRUNCATE(a.time, 1m) = TIMETRUNCATE(b.time, 1m) ORDER BY a.id, b.id",
     {}),
]


# 5 rows, 2024-01-01 00:00-04:00 UTC, 1-minute spacing
# Columns: ts_ms, id, val, score, label, nullable_val, lat, lon, wkt_point, wkt_poly, ts_str
_POLY = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"
_ROWS = [
    (1704067200000, 1, 10, 1.5, "north", 10,   116.4, 39.9, "POINT(5 5)",   _POLY, "2024-01-01 00:00:00"),
    (1704067260000, 2, 20, 2.5, "south", None, 121.5, 31.2, "POINT(15 15)", _POLY, "2024-01-01 00:01:00"),
    (1704067320000, 3, 30, 3.5, "north", 30,   104.0, 30.6, "POINT(3 3)",   _POLY, "2024-01-01 00:02:00"),
    (1704067380000, 4, 40, 4.5, "south", None, 108.0, 34.2, "POINT(12 12)", _POLY, "2024-01-01 00:03:00"),
    (1704067440000, 5, 50, 5.5, "east",  50,   113.0, 28.1, "POINT(8 8)",   _POLY, "2024-01-01 00:04:00"),
]

# Dynamically compute datetime strings from epoch using UTC (matching the
# ALTER LOCAL "timezone" "UTC" in setup_method), so the test is fully
# timezone-independent.
_ROWS_DT = [(_ms_to_dt(row[0]), *row[1:]) for row in _ROWS]


# MySQL: DATETIME(3) PRIMARY KEY — TDengine recognises as time axis for window queries
_MYSQL_SETUP = [
    "DROP TABLE IF EXISTS parity_t",
    "CREATE TABLE parity_t ("
    "  time DATETIME(3) NOT NULL, id INT, val INT, `Mapped_Val` INT, score DOUBLE, label VARCHAR(32),"
    "  nullable_val INT, lat DOUBLE, lon DOUBLE,"
    "  wkt_point VARCHAR(64), wkt_poly VARCHAR(256), ts_str VARCHAR(32),"
    "  PRIMARY KEY (time)"
    ")",
] + [
    "INSERT INTO parity_t VALUES ({})".format(
        ", ".join(parity_sql_val(x) for x in (ts, i, v, 100 + i, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

# PostgreSQL: TIMESTAMP PRIMARY KEY
_PG_SETUP = [
    "CREATE EXTENSION IF NOT EXISTS postgis",
    "DROP TABLE IF EXISTS public.parity_t",
    "CREATE TABLE public.parity_t ("
    "  time TIMESTAMP NOT NULL PRIMARY KEY,"
    '  id INT, val INT, "Mapped_Val" INT, score DOUBLE PRECISION, label VARCHAR(32),'
    "  nullable_val INT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,"
    "  wkt_point VARCHAR(64), wkt_poly VARCHAR(256), ts_str VARCHAR(32)"
    ")",
] + [
    "INSERT INTO public.parity_t VALUES ({})".format(
        ", ".join(parity_sql_val(x) for x in (ts, i, v, 100 + i, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

_SQLSERVER_SETUP = [
    "IF OBJECT_ID('dbo.parity_t') IS NOT NULL DROP TABLE dbo.parity_t",
    "CREATE TABLE dbo.parity_t ("
    "  time datetime2(3) NOT NULL,"
    "  id int, val int, Mapped_Val int, score float, label nvarchar(32),"
    "  nullable_val int, lat float, lon float,"
    "  wkt_point nvarchar(64), wkt_poly nvarchar(256), ts_str nvarchar(32)"
    ")",
] + [
    "INSERT INTO dbo.parity_t (time, id, val, Mapped_Val, score, label,"
    " nullable_val, lat, lon, wkt_point, wkt_poly, ts_str) VALUES ({})".format(
        ", ".join(parity_sql_val(x) for x in (ts, i, v, 100 + i, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

# InfluxDB line-protocol: label tag; numeric/string fields; timestamp in ns.
# nullable_val is omitted for NULL rows (InfluxDB has no NULL — omitting a field
# makes it NULL when read back via TDengine federated query).
def _influx_line(ts, i, v, s, l, nv, lat, lon, wp, wy, tss):
    fields = [f"id={i}i", f"val={v}i", f"Mapped_Val={100 + i}i", f"score={s}"]
    if nv is not None:
        fields.append(f"nullable_val={nv}i")
    fields += [
        f"lat={lat}", f"lon={lon}",
        f'wkt_point="{wp}"', f'wkt_poly="{wy}"', f'ts_str="{tss}"',
    ]
    return f"parity_t,label={l} {','.join(fields)} {ts}000000"

_INFLUX_LINES = [_influx_line(*row) for row in _ROWS]

_LOCAL_SETUP = [
    f"DROP DATABASE IF EXISTS {_LOCAL_DB}",
    f"CREATE DATABASE {_LOCAL_DB}",
    f"USE {_LOCAL_DB}",
    f"CREATE TABLE {_LOCAL_TBL} ("
    f"  time TIMESTAMP, id INT, val INT, `Mapped_Val` INT, score DOUBLE, label NCHAR(32),"
    f"  nullable_val INT, lat DOUBLE, lon DOUBLE,"
    f"  wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32)"
    f")",
] + [
    "INSERT INTO {} VALUES ({})".format(
        _LOCAL_TBL,
        ", ".join(parity_sql_val(x) for x in (ts, i, v, 100 + i, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS
]

_LOCAL_US_SETUP = [
    f"DROP DATABASE IF EXISTS {_LOCAL_US_DB}",
    f"CREATE DATABASE {_LOCAL_US_DB} PRECISION 'us'",
    f"USE {_LOCAL_US_DB}",
    f"CREATE TABLE {_LOCAL_TBL} ("
    f"  time TIMESTAMP, id INT, val INT, `Mapped_Val` INT, score DOUBLE, label NCHAR(32),"
    f"  nullable_val INT, lat DOUBLE, lon DOUBLE,"
    f"  wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32)"
    f")",
] + [
    "INSERT INTO {} VALUES ({})".format(
        _LOCAL_TBL,
        ", ".join(parity_sql_val(x) for x in (ts, i, v, 100 + i, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

# ── Oracle fixtures ─────────────────────────────────────────────────
# Oracle has no per-test database: the login schema is the namespace
# (DS 5.3.2), so every fixture is a table in ORACLE_USER's own schema.  The
# names carry this file's own prefix so the fixtures never collide with the
# identifiers other FQ files create on the same instance.
_ORACLE_TABLE      = "PARITY14_T"
_ORACLE_SEM_TABLE  = "PARITY14_SEM"
_ORACLE_BULK_TABLE = "PARITY14_BULK"
_ORACLE_VIEW_SUM   = "PARITY14_V_SUM"
_ORACLE_VIEW_FILT  = "PARITY14_V_FILT"

# Types are picked so the TDengine mapping matches the other providers:
#   NUMBER(10,0)  -> BIGINT        (id / val / Mapped_Val / nullable_val)
#   BINARY_DOUBLE -> DOUBLE        (score / lat / lon)
#   NVARCHAR2     -> NCHAR         (text; VARCHAR2 would map to VARCHAR and
#                                   change the '->' / CONTAINS / AES_ENCRYPT
#                                   behaviour that the NCHAR negative cases pin)
#   TIMESTAMP(3)  -> TIMESTAMP with MICRO transport precision (DS 5.3.7: no
#                                   TIMESTAMP(7-9) column in the table)
_ORACLE_COLUMNS = (
    '"TIME" TIMESTAMP(3) PRIMARY KEY,'
    ' "ID" NUMBER(10,0), "VAL" NUMBER(10,0), "MAPPED_VAL" NUMBER(10,0),'
    ' "SCORE" BINARY_DOUBLE, "LABEL" NVARCHAR2(32), "NULLABLE_VAL" NUMBER(10,0),'
    ' "LAT" BINARY_DOUBLE, "LON" BINARY_DOUBLE,'
    ' "WKT_POINT" NVARCHAR2(64), "WKT_POLY" NVARCHAR2(256), "TS_STR" NVARCHAR2(32)'
)


def _oracle_ts_literal(dt_str):
    """Render a 'YYYY-MM-DD HH:MM:SS.mmm' string as a TIMESTAMP(3) literal.

    The format model is explicit: a bare string is parsed with the session's
    NLS_TIMESTAMP_FORMAT, which differs between the sqlplus client and the
    server.
    """
    return f"TO_TIMESTAMP('{dt_str}', 'YYYY-MM-DD HH24:MI:SS.FF3')"


def _oracle_insert_all(table, rows_dt):
    """Build one INSERT ALL statement (Oracle 19c has no multi-row VALUES).

    Mapped_Val is derived (100 + id) when the caller passes the source rows,
    and already present when the disorder rewrite hands over the expanded
    rows, so the Oracle table always carries the same 12 columns.
    """
    branches = []
    for row in rows_dt:
        if len(row) == 11:
            ts, i, v, score, label, nv, lat, lon, wp, wy, tss = row
            mapped = 100 + i
        else:
            ts, i, v, mapped, score, label, nv, lat, lon, wp, wy, tss = row
        values = ", ".join(
            [_oracle_ts_literal(ts)] +
            [parity_sql_val(x) for x in (i, v, mapped, score, label, nv,
                                         lat, lon, wp, wy, tss)]
        )
        branches.append(f'INTO "{table}" VALUES ({values})')
    # Newline-separated: SQL*Plus silently ignores input lines longer than
    # 4999 characters (SP2-0027) while still exiting 0.
    return "INSERT ALL\n" + "\n".join(branches) + "\nSELECT 1 FROM DUAL"


_ORACLE_SETUP = [
    f'CREATE TABLE "{_ORACLE_TABLE}" ({_ORACLE_COLUMNS})',
    _oracle_insert_all(_ORACLE_TABLE, _ROWS_DT),
    "COMMIT",
]

# Oracle semantics fixtures (FQOR-LOCAL-010 / -012 / -015), with a local
# mirror that already encodes Oracle's storage rules:
#   D_STR   empty string: Oracle stores '' as NULL, so the mirror row is NULL
#   D_DATE  DATE: second precision, padded to the table's MICRO transport
#   D_DEC   NUMBER(10,2) -> DECIMAL(10,2), compared as exact decimal strings
#   D_NUM1  NUMBER(1,0): numeric, never BOOL; the value 2 must survive
_ORACLE_SEM_ROWS = [
    ("2024-01-01 00:00:05", "12345678.91", 1, "alpha"),
    ("2024-01-01 00:00:06", "-0.25", 0, ""),
    ("2024-01-01 00:00:07", "0.75", 2, "beta"),
]
_ORACLE_SEM_SETUP = [
    f'CREATE TABLE "{_ORACLE_SEM_TABLE}" ('
    f' "D_DATE" DATE, "D_DEC" NUMBER(10,2), "D_NUM1" NUMBER(1,0),'
    f' "D_STR" NVARCHAR2(16))',
] + [
    # DATE is a second-precision timestamp in Oracle; the ANSI DATE literal
    # only takes YYYY-MM-DD and raises ORA-01861 for a time component, so
    # the value goes through TO_DATE with an explicit format model.
    f'INSERT INTO "{_ORACLE_SEM_TABLE}" VALUES '
    f"(TO_DATE('{d_date}', 'YYYY-MM-DD HH24:MI:SS'), "
    f"{d_dec}, {d_num1}, {parity_sql_val(d_str)})"
    for d_date, d_dec, d_num1, d_str in _ORACLE_SEM_ROWS
] + ["COMMIT"]

_LOCAL_SEM_TABLE = "parity_sem"
_LOCAL_SEM_SETUP = [
    f"DROP TABLE IF EXISTS {_LOCAL_SEM_TABLE}",
    f"CREATE TABLE {_LOCAL_SEM_TABLE} ("
    f"  d_date TIMESTAMP, d_dec DECIMAL(10, 2), d_num1 TINYINT, d_str NCHAR(16))",
] + [
    "INSERT INTO {} VALUES ('{}.000', {}, {}, {})".format(
        _LOCAL_SEM_TABLE, d_date, d_dec, d_num1,
        "NULL" if d_str == "" else parity_sql_val(d_str))
    for d_date, d_dec, d_num1, d_str in _ORACLE_SEM_ROWS
]

# Views (FQOR-LOCAL-012).  Oracle views carry no primary key, so TDengine
# exposes them as ordinary non-timestamp tables: the projection-only queries
# below never need a time axis.
_ORACLE_VIEWS = [
    # A bare COUNT(*)/SUM() column is an unconstrained NUMBER, which the type
    # map rejects (DS 5.3.2.1: 未声明精度的 NUMBER 不支持), so the view casts
    # its aggregate columns to the precision the values really need.
    f'CREATE OR REPLACE VIEW "{_ORACLE_VIEW_SUM}" AS '
    f'SELECT "LABEL", CAST(COUNT(*) AS NUMBER(18,0)) AS "CNT", '
    f'CAST(SUM("VAL") AS NUMBER(19,0)) AS "SV" '
    f'FROM "{_ORACLE_TABLE}" GROUP BY "LABEL"',
    f'CREATE OR REPLACE VIEW "{_ORACLE_VIEW_FILT}" AS '
    f'SELECT "ID", "VAL" FROM "{_ORACLE_TABLE}" WHERE "VAL" > 20',
]

# Large result set (FQOR-LOCAL-013): one row per second, K = 1..5000, V = 2*K.
# CONNECT BY LEVEL generates the rows on the Oracle side in a single statement.
_ORACLE_BULK_ROWS = 5000
_ORACLE_BULK_SETUP = [
    f'CREATE TABLE "{_ORACLE_BULK_TABLE}" ('
    f' "TS" TIMESTAMP(3) PRIMARY KEY, "K" NUMBER(10,0), "V" NUMBER(10,0))',
    f'INSERT INTO "{_ORACLE_BULK_TABLE}" ("TS", "K", "V") '
    f"SELECT TIMESTAMP '2024-02-01 00:00:00' + NUMTODSINTERVAL(LEVEL - 1, 'SECOND'),"
    f" LEVEL, LEVEL * 2 FROM DUAL CONNECT BY LEVEL <= {_ORACLE_BULK_ROWS}",
    "COMMIT",
]

_ORACLE_DROP_OBJECTS = (
    ("VIEW", _ORACLE_VIEW_SUM),
    ("VIEW", _ORACLE_VIEW_FILT),
    ("TABLE", _ORACLE_BULK_TABLE),
    ("TABLE", _ORACLE_SEM_TABLE),
    ("TABLE", _ORACLE_TABLE),
)


def _oracle_drop_fixture(cfg):
    """Drop the Oracle fixtures, ignoring the ones that are not there.

    Oracle has no ``DROP ... IF EXISTS``, so a first run raises ORA-00942 for
    every object; that is the expected state, not a failure.
    """
    for kind, name in _ORACLE_DROP_OBJECTS:
        try:
            ExtSrcEnv._oracle_sql(cfg, f'DROP {kind} "{name}"')
        except RuntimeError as e:
            if "ORA-00942" not in str(e):
                raise


# 乱序写入：刻意打乱插入顺序（row-3, row-1, row-5, row-2, row-4），
# 验证无论插入顺序如何，查询结果都与顺序写入一致。
_DISORDER_IDX = [2, 0, 4, 1, 3]


class TestFq14ResultParity(ParityTestBase):
    """Result-parity: local TDengine == MySQL == PostgreSQL == InfluxDB == Oracle.

    Every test executes the same logical query against all five sources
    and asserts row-by-row equality.  A source is only omitted when its
    SQL dialect physically lacks the required syntax.
    """

    _SRC_MYSQL  = "fq_parity_src_m"
    _SRC_PG     = "fq_parity_src_p"
    _SRC_INFLUX = "fq_parity_src_i"
    _SRC_SQLSERVER = "fq_parity_src_s"
    _SRC_ORACLE = "fq_parity_src_o"
    _class_setup_done = False
    _FLOAT_TOL = _FLOAT_TOL
    _BASELINE_FILE = os.path.join(os.path.dirname(__file__), "ans", "test_fq_14_result_parity.txt")
    _EPOCH_BASELINE_FILE = os.path.join(os.path.dirname(__file__), "ans", "test_fq_14_win_epoch.txt")
    _PUSHDOWN_BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_14_join_pushdown.txt"
    )
    _PARITY_CASES = _PARITY_CASES
    _PARITY_TZ = _FIXED_TZ  # Pin epoch→datetime to UTC (matches ALTER LOCAL below)
    # Capability clauses demoted to reach the zero-pushdown baseline:
    # extCapShouldUseBaseline() is true only when all six are disabled.
    _COARSE_CLAUSES = ("FILTER", "PROJECTION", "AGG", "ORDER", "LIMIT", "JOIN")

    @property
    def _local_tbl(self):
        return f"{_LOCAL_DB}.{_LOCAL_TBL}"

    def _ext_sources(self):
        return [
            ("MySQL",     f"{self._SRC_MYSQL}.parity_t"),
            ("PG",        f"{self._SRC_PG}.parity_t"),
            ("InfluxDB",  f"{self._SRC_INFLUX}.parity_t"),
            ("SQLServer", f"{self._SRC_SQLSERVER}.parity_t"),
            # Oracle folds unquoted identifiers to upper case, so the lowercase
            # reference resolves to PARITY14_T.
            ("Oracle",    f"{self._SRC_ORACLE}.parity14_t"),
        ]

    def _join_ts_source_map(self, *, local_ref=False):
        local_tbl = self._local_tbl
        local_us_tbl = f"{_LOCAL_US_DB}.{_LOCAL_TBL}"
        if local_ref:
            return {
                "local": local_us_tbl,
                "local_us": local_us_tbl,
                "mysql": local_us_tbl,
                "pg": local_us_tbl,
                "influx": local_us_tbl,
                "sqlserver": local_us_tbl,
                "oracle": local_us_tbl,
            }
        return {
            "local": local_tbl,
            "local_us": local_us_tbl,
            "mysql": f"{self._SRC_MYSQL}.parity_t",
            "pg": f"{self._SRC_PG}.parity_t",
            "influx": f"{self._SRC_INFLUX}.parity_t",
            "sqlserver": f"{self._SRC_SQLSERVER}.parity_t",
            "oracle": f"{self._SRC_ORACLE}.parity14_t",
        }

    def _run_join_ts_external_sort_matrix(self):
        failed = []
        # The matrix was authored for the MySQL/PG pair.  Execute it once per
        # external pair so every same-precision provider is covered with the
        # identical sort / TIMETRUNCATE shapes: substitute SQL Server and
        # Oracle for the MySQL leg, and SQL Server / Oracle for the PG leg.
        # Cases that do not use the leg being substituted are skipped (the
        # other pass covers them).
        leg_bindings = [
            ("mysql/pg", {}),
            ("sqlserver/pg", {"mysql": "sqlserver"}),
            ("mysql/sqlserver", {"pg": "sqlserver"}),
            ("oracle/pg", {"mysql": "oracle"}),
            ("mysql/oracle", {"pg": "oracle"}),
            ("oracle/sqlserver", {"mysql": "oracle", "pg": "sqlserver"}),
        ]
        total = 0
        for pair_label, remap in leg_bindings:
            for case_id, sql_template, opts in _JOIN_TS_EXT_SORT_MATRIX:
                if remap and not any(f"{{{k}}}" in sql_template for k in remap):
                    continue
                ordered = opts.get("ordered", True)
                ref_sql = sql_template.format(**self._join_ts_source_map(local_ref=True))
                src_map = self._join_ts_source_map()
                for k, v in remap.items():
                    src_map[k] = src_map[v]
                mix_sql = sql_template.format(**src_map)
                label = f"{case_id}[{pair_label}]"
                total += 1
                try:
                    ref = self._get_rows(ref_sql)
                    rows = self._get_rows(mix_sql)
                    if not ordered:
                        ref = sorted(ref, key=lambda r: [str(x) for x in r])
                        rows = sorted(rows, key=lambda r: [str(x) for x in r])
                    self._compare_rows(ref, rows, ref_sql, mix_sql, label, set())
                    tdLog.info(f"[join-ts-ext {label}] PASS")
                except (AssertionError, QueryError) as exc:
                    failed.append((label, sql_template, str(exc)))
                    tdLog.info(f"[join-ts-ext {label}] FAIL: {str(exc).splitlines()[0]}")

        if failed:
            all_errors = "\n".join(
                f"\n[{case_id}] {sql}\n  {detail}" for case_id, sql, detail in failed
            )
            raise AssertionError(
                f"{len(failed)} of {total} "
                f"JOIN TIMETRUNCATE external sort case(s) failed:{all_errors}"
            )

    def setup_method(self, method):
        if TestFq14ResultParity._class_setup_done:
            return
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

        _pin_process_timezone_utc()

        # Pin client timezone to UTC so all datetime conversions are
        # deterministic regardless of the host OS timezone setting.
        tdSql.execute('ALTER LOCAL "timezone" "UTC"')

        ExtSrcEnv.ensure_env()

        tdSql.executes(_LOCAL_SETUP)
        tdSql.executes(_LOCAL_US_SETUP)
        tdSql.execute(f"USE {_LOCAL_DB}")
        # Local mirror of the Oracle semantics fixture (empty string -> NULL,
        # DATE second precision, NUMBER(10,2) -> DECIMAL(10,2)).
        tdSql.executes(_LOCAL_SEM_SETUP)

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, _MYSQL_SETUP)
        self._cleanup_src(self._SRC_MYSQL)
        self._mk_mysql_real(self._SRC_MYSQL, database=_MYSQL_DB)

        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        # Keep PostgreSQL timezone-sensitive functions such as TO_TIMESTAMP()
        # aligned with the UTC-pinned TDengine client session used by this test.
        ExtSrcEnv.pg_exec_cfg(
            self._pg_cfg(),
            "postgres",
            [f'ALTER DATABASE "{_PG_DB}" SET timezone TO \'UTC\''],
        )
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _PG_SETUP)
        self._cleanup_src(self._SRC_PG)
        self._mk_pg_real(self._SRC_PG, database=_PG_DB, schema="public")

        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), _INFLUX_BUCKET, _INFLUX_LINES)
        self._cleanup_src(self._SRC_INFLUX)
        self._mk_influx_real(self._SRC_INFLUX, database=_INFLUX_BUCKET)

        ExtSrcEnv.sqlserver_create_db(_SQLSERVER_DB)
        ExtSrcEnv.sqlserver_exec(_SQLSERVER_DB, _SQLSERVER_SETUP)
        self._cleanup_src(self._SRC_SQLSERVER)
        self._mk_sqlserver_real(self._SRC_SQLSERVER, database=_SQLSERVER_DB, schema="dbo")

        # Oracle has no per-test database (DS 5.3.2): the login schema is the
        # namespace, so the fixtures are rebuilt in place.  Every statement is
        # idempotent against a first run that has no objects yet.
        oracle_cfg = self._oracle_cfg()
        _oracle_drop_fixture(oracle_cfg)
        ExtSrcEnv.oracle_exec_cfg(
            oracle_cfg,
            _ORACLE_SETUP + _ORACLE_SEM_SETUP + _ORACLE_VIEWS + _ORACLE_BULK_SETUP,
        )
        self._cleanup_src(self._SRC_ORACLE)
        self._mk_oracle_real(self._SRC_ORACLE, database=oracle_cfg.service,
                             schema=ExtSrcEnv.ORACLE_USER.upper())

        TestFq14ResultParity._class_setup_done = True

    def teardown_class(self):
        tmp = TestFq14ResultParity()
        tmp._cleanup_src(tmp._SRC_MYSQL, tmp._SRC_PG, tmp._SRC_INFLUX,
                         tmp._SRC_SQLSERVER, tmp._SRC_ORACLE)
        try:
            _oracle_drop_fixture(tmp._oracle_cfg())
        except Exception:
            pass
        try:
            ExtSrcEnv.sqlserver_drop_db(_SQLSERVER_DB)
        except Exception:
            pass
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_US_DB}")
        for drop in [
            lambda: ExtSrcEnv.mysql_drop_db_cfg(tmp._mysql_cfg(), _MYSQL_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(tmp._pg_cfg(), _PG_DB),
            lambda: ExtSrcEnv.influx_drop_db_cfg(tmp._influx_cfg(), _INFLUX_BUCKET),
        ]:
            try:
                drop()
            except Exception:
                pass
        TestFq14ResultParity._class_setup_done = False
        ExtSrcEnv.teardown_env()

        _restore_process_timezone()

    def test_fq_parity_all_cases(self):
        """All result-parity cases driven by _PARITY_GROUPS.

        By default every entry in _PARITY_CASES is executed.  Set the
        environment variable ``PARITY_IDX`` to a comma-separated list of
        case IDs (``grp-NN``) or group names to run only those entries::

            PARITY_IDX=whr-01 pytest ...::test_fq_parity_all_cases
            PARITY_IDX=whr-01,win-03 pytest ...
            PARITY_IDX=whr,win pytest ...       # entire groups

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
            - 2026-05-14 wpan Switch to group-based case IDs (grp-NN format)
            - 2026-08-25 wpan Verify outer-join WHERE predicates reach the remote scan
        """

        selected = {
            part.strip()
            for part in os.getenv("PARITY_IDX", "").split(",")
            if part.strip()
        }
        auxiliary_ids = {"full-flat"}
        standard_ids = selected - auxiliary_ids
        run_standard = not selected or bool(standard_ids)
        run_full_flat = not selected or "join" in selected or "full-flat" in selected

        original_selector = os.environ.get("PARITY_IDX")
        try:
            if run_standard:
                if selected:
                    os.environ["PARITY_IDX"] = ",".join(sorted(standard_ids))
                self.run_parity_cases(_PARITY_CASES, parity_groups=_PARITY_GROUPS)
                self.do_verify_outer_join_where_scan_pushdown()
        finally:
            if original_selector is None:
                os.environ.pop("PARITY_IDX", None)
            else:
                os.environ["PARITY_IDX"] = original_selector

        if run_full_flat:
            self.do_verify_filtered_full_join()

    def test_fq_temporal_join_filtered_input_order(self):
        """Temporal JOIN preserves remote input order after filtering.

        1. Check explicit ASOF and WINDOW results for both preserved sides.
        2. Cover aliases, descending inputs, LIMIT and OFFSET.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-28 Tony Zhang Verify filtered temporal input ordering
        """
        self.do_verify_temporal_join_filtered_input_order()

    def do_verify_temporal_join_filtered_input_order(self):
        asof_rows = [(3, 30, 30), (4, 40, 40), (5, 50, 50)]
        window_rows = [
            (3, 30, 10), (3, 30, 20), (3, 30, 30),
            (3, 30, 40), (3, 30, 50),
            (4, 40, 20), (4, 40, 30), (4, 40, 40), (4, 40, 50),
            (5, 50, 30), (5, 50, 40), (5, 50, 50),
        ]
        cases = []
        for side, preserved, other in [("LEFT", "a", "b"),
                                        ("RIGHT", "b", "a")]:
            for kind, clause, expected in [
                ("ASOF", f"ON {preserved}.time >= {other}.time",
                 asof_rows),
                ("WINDOW", "WINDOW_OFFSET(-2m, 2m)", window_rows),
            ]:
                sql = (
                    f"SELECT {preserved}.id, {preserved}.val, {other}.val "
                    f"FROM {{tbl}} a {side} {kind} JOIN {{tbl}} b "
                    f"{clause} WHERE {preserved}.val > 20 "
                    f"ORDER BY {preserved}.time, {other}.time"
                )
                cases.append((sql, expected))
                cases.append((sql.replace(" > 20 ", " > 50 "), []))

        limited_input = (
            "(SELECT event_time, id, val FROM "
            "(SELECT time AS event_time, id, val FROM {tbl} "
            "ORDER BY time DESC LIMIT 3 OFFSET 1) s)"
        )
        for kind, clause, expected in [
            ("ASOF", "ON a.event_time >= b.time", asof_rows[:2]),
            ("WINDOW", "WINDOW_OFFSET(-2m, 2m)", window_rows[:9]),
        ]:
            sql = (
                "SELECT a.id, a.val, b.val FROM " + limited_input +
                f" a LEFT {kind} JOIN {{tbl}} b {clause} "
                "WHERE a.val > 20 ORDER BY a.event_time, b.time"
            )
            cases.append((sql, expected))

        sources = [("local", self._local_tbl)] + self._ext_sources()
        for label, table in sources:
            for template, expected in cases:
                sql = template.format(tbl=table)
                tdSql.query(sql)
                actual = [tuple(row) for row in tdSql.queryResult]
                assert actual == expected, (
                    f"[{label}] temporal input order mismatch: {sql}\n"
                    f"expected={expected!r}\nactual={actual!r}"
                )

    def do_verify_filtered_full_join(self):
        selected = {
            part.strip()
            for part in os.getenv("PARITY_IDX", "").split(",")
            if part.strip()
        }
        valid_ids = {"full-flat"}
        if selected and "join" not in selected and not (selected & valid_ids):
            return

        # The post-join predicate keeps FULL JOIN local while both external inputs
        # remain remote scans. Derived-leg SQL rendering is covered by nodesTest.
        sql_template = (
            "SELECT a.id, b.val FROM {tbl} a FULL OUTER JOIN {tbl} b "
            "ON a.time = b.time AND a.id < 4 "
            "WHERE a.val > 20 OR a.id IS NULL ORDER BY a.id, b.val"
        )
        self._assert_parity_all(sql_template)
        for label, table in self._ext_sources():
            sql = sql_template.format(tbl=table)
            tdSql.query(f"EXPLAIN VERBOSE TRUE {sql}")
            plan_lines = [
                str(cell)
                for row in (tdSql.queryResult or [])
                for cell in row
                if cell is not None
            ]
            remote_lines = [line for line in plan_lines if "remote sql:" in line.lower()]
            local_lines = [line for line in plan_lines if "remote sql:" not in line.lower()]
            local_full_join = any("full join" in line.lower() for line in local_lines)
            remote_full_join = any(" full join " in line.lower() or
                                   " full outer join " in line.lower()
                                   for line in remote_lines)
            if local_full_join:
                assert len(remote_lines) == 2, (
                    f"[full-flat {label}] expected two remote input scans: {remote_lines!r}"
                )
                assert all(" join " not in line.lower() for line in remote_lines), (
                    f"[full-flat {label}] JOIN leaked into a remote input scan: {remote_lines!r}"
                )
            else:
                assert len(remote_lines) == 1 and remote_full_join, (
                    f"[full-flat {label}] expected either a local FULL JOIN with two remote "
                    f"scans or one pushed-down FULL JOIN: local={local_lines!r} "
                    f"remote={remote_lines!r}"
                )
            assert all(" select " in line.lower() and " from " in line.lower()
                       for line in remote_lines), (
                f"[full-flat {label}] malformed Remote SQL: {remote_lines!r}"
            )

    def do_verify_outer_join_where_scan_pushdown(self):
        selected = {
            part.strip()
            for part in os.getenv("PARITY_IDX", "").split(",")
            if part.strip()
        }
        plan_cases = [
            (
                "join-22",
                "SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b "
                "ON a.time = b.time WHERE a.val > 20 ORDER BY a.id",
            ),
            (
                "join-27",
                "SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b "
                "ON a.time = b.time WHERE a.val < 30 ORDER BY b.id",
            ),
            (
                "join-28",
                "SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b "
                "ON a.time = b.time WHERE b.val > 20 ORDER BY b.id",
            ),
        ]
        if selected and "join" not in selected and not any(case[0] in selected for case in plan_cases):
            return

        compare_full_baseline = not selected or "join" in selected
        if selected and "join" not in selected:
            plan_cases = [case for case in plan_cases if case[0] in selected]

        result_blocks = []
        for case_id, sql_template in plan_cases:
            for label, table in self._ext_sources():
                sql = sql_template.format(tbl=table)
                tdSql.query(f"EXPLAIN VERBOSE TRUE {sql}")
                plan_lines = [
                    str(cell)
                    for row in (tdSql.queryResult or [])
                    for cell in row
                    if cell is not None
                ]
                remote_lines = [line for line in plan_lines if "remote sql:" in line.lower()]
                assert remote_lines, f"[{case_id} {label}] EXPLAIN has no Remote SQL: {plan_lines!r}"

                local_plan = "\n".join(
                    line for line in plan_lines if "remote sql:" not in line.lower()
                )
                remote_sql = re.sub(r"\s+", " ", " ".join(remote_lines)).strip()
                remote_sql = re.sub(r"^remote sql:\s*", "", remote_sql, flags=re.IGNORECASE)
                plan_details = [f"REMOTE SQL: {remote_sql}"]
                normalized_sql = remote_sql.upper()
                outer_tail = normalized_sql.rsplit(" ON ", 1)[-1]
                outer_tail = outer_tail.split(" ORDER BY ", 1)[0]
                outer_where = " WHERE " in outer_tail
                if case_id in ("join-22", "join-28"):
                    if case_id == "join-22":
                        derived_pattern = r"FROM \(SELECT .+ WHERE .+\) [`\"\[]?a[`\"\]]? LEFT JOIN"
                    else:
                        derived_pattern = r"RIGHT JOIN \(SELECT .+ WHERE .+\) [`\"\[]?b[`\"\]]? ON"
                    preserved_derived = re.search(
                        derived_pattern, remote_sql, flags=re.IGNORECASE
                    ) is not None
                    predicate_count = len(
                        re.findall(r"[`\"\[]?val[`\"\]]?\s*>\s*20", remote_sql, re.IGNORECASE)
                    )
                    assert preserved_derived, (
                        f"[{case_id} {label}] preserved side is not a filtered derived table: "
                        f"{remote_sql}"
                    )
                    assert not outer_where, (
                        f"[{case_id} {label}] preserved-side predicate leaked to outer WHERE: "
                        f"{remote_sql}"
                    )
                    assert predicate_count == 1, (
                        f"[{case_id} {label}] expected one preserved-side predicate, got "
                        f"{predicate_count}: {remote_sql}"
                    )
                    plan_details = [
                        "PRESERVED DERIVED: YES",
                        "OUTER WHERE: NO",
                        "PREDICATE COUNT: 1",
                    ]
                elif case_id == "join-27":
                    predicate_count = len(
                        re.findall(r"[`\"\[]?val[`\"\]]?\s*<\s*30", remote_sql, re.IGNORECASE)
                    )
                    assert outer_where, (
                        f"[{case_id} {label}] nullable-side predicate is not in outer WHERE: "
                        f"{remote_sql}"
                    )
                    assert predicate_count == 1, (
                        f"[{case_id} {label}] expected one nullable-side predicate, got "
                        f"{predicate_count}: {remote_sql}"
                    )
                    plan_details = [
                        "NULLABLE OUTER WHERE: YES",
                        "PREDICATE COUNT: 1",
                    ]
                result_blocks.append(
                    "\n".join(
                        [
                            f"### {case_id} {label}",
                            f"SQL: {sql}",
                            *plan_details,
                            f"LOCAL FILTER: {'YES' if 'filter' in local_plan.lower() else 'NO'}",
                            "---",
                        ]
                    )
                )

        if not compare_full_baseline:
            return

        actual = "\n".join(result_blocks) + "\n"
        baseline_file = self._PUSHDOWN_BASELINE_FILE
        tmp_file = os.path.join(
            tempfile.gettempdir(),
            f"{os.path.basename(baseline_file)}.{os.getpid()}.tmp",
        )
        with open(tmp_file, "w") as f:
            f.write(actual)

        if not os.path.isfile(baseline_file):
            raise FileNotFoundError(
                f"Pushdown baseline not found: {baseline_file}; actual: {tmp_file}"
            )
        with open(baseline_file, "r") as f:
            expected = f.read()
        if actual != expected:
            actual_lines = actual.splitlines()
            expected_lines = expected.splitlines()
            diff_line = next(
                (
                    i + 1
                    for i in range(max(len(actual_lines), len(expected_lines)))
                    if (actual_lines[i] if i < len(actual_lines) else "<EOF>")
                    != (expected_lines[i] if i < len(expected_lines) else "<EOF>")
                ),
                -1,
            )
            raise AssertionError(
                "JOIN pushdown baseline mismatch:\n"
                f"  baseline: {baseline_file}\n"
                f"  actual:   {tmp_file}\n"
                f"  first diff at line {diff_line}"
            )
        os.remove(tmp_file)
        print("OUTER JOIN WHERE scan pushdown ..................... [ passed ]")

    def test_fq_parity_epoch_window(self):
        """Epoch-precision parity for window queries with _wstart/_wend.

        Runs window queries via ``taos -r`` (raw epoch output) against all
        four sources.  This catches precision propagation bugs that the
        connector-based test masks — e.g. ms vs µs vs ns epoch values for
        _wstart/_wend columns.

        Non-timestamp columns are cross-validated to match across sources.
        The full output (all sources) is compared against a checked-in
        baseline file.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-06-10 wpan Epoch precision parity test for window queries
        """
        self.run_epoch_parity_cases(_WIN_EPOCH_CASES, self._EPOCH_BASELINE_FILE)

    def test_fq_parity_mixed_case_identifiers(self):
        """External column names follow each provider's identifier rules.

        MySQL lowercases mixed-case columns, so DESC and SELECT treat
        Mapped_Val as mapped_val. PostgreSQL and InfluxDB preserve the
        original case, so unquoted mapped_val is rejected and quoted
        Mapped_Val is required for metadata and binding. SQL Server keeps the
        stored case but matches names case-insensitively, like MySQL. Oracle
        folds the unquoted declaration to upper case, so the stored column is
        MAPPED_VAL; an unquoted or all-lowercase reference folds onto it, while
        a quoted reference is matched exactly (DS 5.3.6 R1).

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-09-01 xsren Mixed-case identifier parity for MySQL/PG/InfluxDB
            - 2026-10-16 wpan Add the SQL Server identifier leg
            - 2026-10-26 wpan Add the Oracle identifier-folding leg
        """
        tables = dict(self._ext_sources())

        tdSql.query(f"DESC {tables['MySQL']}")
        mysql_fields = [str(row[0]) for row in tdSql.queryResult]
        assert "mapped_val" in mysql_fields and "Mapped_Val" not in mysql_fields
        for name in ("mapped_val", "Mapped_Val", "`mapped_val`", "`Mapped_Val`"):
            tdSql.query(f"SELECT id, {name} FROM {tables['MySQL']} WHERE {name} > 103 ORDER BY id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(0, 1, 104)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(1, 1, 105)

        for provider in ("PG", "InfluxDB"):
            tdSql.query(f"DESC {tables[provider]}")
            fields = [str(row[0]) for row in tdSql.queryResult]
            assert "Mapped_Val" in fields and "mapped_val" not in fields
            tdSql.error(f"SELECT mapped_val FROM {tables[provider]}")
            tdSql.query(f"SELECT id, `Mapped_Val` FROM {tables[provider]} WHERE `Mapped_Val` > 103 ORDER BY id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(0, 1, 104)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(1, 1, 105)

        # SQL Server keeps the stored case in the metadata but exposes a
        # case-insensitive catalog (its default collation), exactly like MySQL:
        # every name variant folds onto the stored column.
        tdSql.query(f"DESC {tables['SQLServer']}")
        ss_fields = [str(row[0]) for row in tdSql.queryResult]
        assert "Mapped_Val" in ss_fields and "mapped_val" not in ss_fields
        for name in ("Mapped_Val", "mapped_val", "`Mapped_Val`", "`mapped_val`"):
            tdSql.query(
                f"SELECT id, {name} FROM {tables['SQLServer']} "
                f"WHERE {name} > 103 ORDER BY id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(0, 1, 104)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(1, 1, 105)

        # Oracle folds the unquoted column declaration to upper case (DS
        # 5.3.6 R1): the metadata reports MAPPED_VAL.  An unquoted reference
        # is lower-cased by TDengine and then folds onto it, and the quoted
        # all-lower-case form is indistinguishable from an unquoted one, so
        # all of those spellings resolve.
        tdSql.query(f"DESC {tables['Oracle']}")
        oracle_fields = [str(row[0]) for row in tdSql.queryResult]
        assert "MAPPED_VAL" in oracle_fields and "Mapped_Val" not in oracle_fields
        for name in ("MAPPED_VAL", "mapped_val", "Mapped_Val",
                     "`MAPPED_VAL`", "`mapped_val`"):
            tdSql.query(
                f"SELECT id, {name} FROM {tables['Oracle']} "
                f"WHERE {name} > 103 ORDER BY id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(0, 1, 104)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(1, 1, 105)

        # A quoted reference keeps its spelling and is matched exactly, so the
        # quoted mixed-case form names a column that does not exist: the
        # table declares MAPPED_VAL, not "Mapped_Val".
        tdSql.error(f"SELECT id, `Mapped_Val` FROM {tables['Oracle']}")

    def test_fq_join_timetruncate_external_sort_matrix(self):
        """JOIN TIMETRUNCATE sort compatibility with explicit external sources.

        1. Mixed MySQL/PostgreSQL source joins in one SQL
        2. TIMETRUNCATE timezone and granularity sort-compatibility cases
        3. Opposite ASC/DESC subquery ordering with inner LIMIT
        4. Outer LIMIT without outer ORDER BY when the join result is stable
        5. LEFT/RIGHT JOIN paths with external-source branches
        6. Local TDengine branch joined with external-source branches

        Catalog: - Query:FederatedResultParity

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-20 Codex Add explicit external-source JOIN TIMETRUNCATE sort matrix
        """
        self._run_join_ts_external_sort_matrix()

    # ── FQOR-LOCAL dedicated Oracle coverage ─────────────────────────────
    @property
    def _oracle_tbl(self):
        return f"{self._SRC_ORACLE}.parity14_t"

    @property
    def _oracle_sem_tbl(self):
        return f"{self._SRC_ORACLE}.{_ORACLE_SEM_TABLE.lower()}"

    @property
    def _oracle_bulk_tbl(self):
        return f"{self._SRC_ORACLE}.{_ORACLE_BULK_TABLE.lower()}"

    @property
    def _oracle_view_sum(self):
        return f"{self._SRC_ORACLE}.{_ORACLE_VIEW_SUM.lower()}"

    @property
    def _oracle_view_filt(self):
        return f"{self._SRC_ORACLE}.{_ORACLE_VIEW_FILT.lower()}"

    @property
    def _local_sem_tbl(self):
        return f"{_LOCAL_DB}.{_LOCAL_SEM_TABLE}"

    @staticmethod
    def _plan_text(rows):
        return [str(cell) for row in (rows or []) for cell in row if cell is not None]

    def _explain(self, sql):
        """Return ``(plan_lines, remote_lines)`` for one EXPLAIN VERBOSE."""
        tdSql.query(f"EXPLAIN VERBOSE TRUE {sql}")
        plan_lines = self._plan_text(tdSql.queryResult)
        remote_lines = [line for line in plan_lines if "remote sql:" in line.lower()]
        return plan_lines, remote_lines

    @staticmethod
    def _set_clause_pushdown(src, clause, enable):
        """Force one clause local (or restore it) for an external source.

        The demotion is catalog state, so callers restore it in a ``finally``
        block.
        """
        action = "ENABLE" if enable else "DISABLE"
        tdSql.execute(f"alter external source {src} {action} clause '{clause}'")

    def test_fq_oracle_projection_pruning(self):
        """Oracle basic query and column pruning (FQOR-LOCAL-001).

        1. A partial projection returns the same rows as the local mirror.
        2. The remote SQL carries only the projected columns.
        3. A full-column projection matches the local mirror column by column.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_tbl
        local = self._local_tbl

        partial_sql = f"SELECT id, val FROM {src} ORDER BY time"
        ref_sql = f"SELECT id, val FROM {local} ORDER BY time"
        ref = self._get_rows(ref_sql)
        rows = self._get_rows(partial_sql)
        self._compare_rows(ref, rows, ref_sql, partial_sql, "Oracle", set())

        _, remote_lines = self._explain(partial_sql)
        assert len(remote_lines) == 1, (
            f"[oracle-prune] expected one remote scan: {remote_lines!r}"
        )
        remote_sql = " ".join(remote_lines).lower()
        assert " select " in remote_sql and " from " in remote_sql, (
            f"[oracle-prune] malformed Remote SQL: {remote_sql}"
        )
        for pruned in ("label", "score", "wkt_poly", "wkt_point", "ts_str"):
            assert pruned not in remote_sql, (
                f"[oracle-prune] unprojected column '{pruned}' reached the remote "
                f"scan: {remote_sql}"
            )

        # The local mirror declares the mixed-case column quoted, while the
        # Oracle source reports it folded to MAPPED_VAL.  A quoted reference is
        # matched exactly (DS 5.3.6 R1), so each side keeps its own spelling:
        # the local side reads `Mapped_Val`, the Oracle side MAPPED_VAL.
        local_cols = ("time, id, val, `Mapped_Val`, score, label, nullable_val, lat,"
                      " lon, wkt_point, wkt_poly, ts_str")
        remote_cols = ("time, id, val, MAPPED_VAL, score, label, nullable_val, lat,"
                       " lon, wkt_point, wkt_poly, ts_str")
        full_sql = f"SELECT {remote_cols} FROM {src} ORDER BY time"
        ref_full_sql = f"SELECT {local_cols} FROM {local} ORDER BY time"
        ref_full = self._get_rows(ref_full_sql)
        rows_full = self._get_rows(full_sql)
        # score / lat / lon are columns 4 / 7 / 8 in this projection (Mapped_Val
        # is the fourth column and shifts them by one against the sort-* case).
        self._compare_rows(ref_full, rows_full, ref_full_sql, full_sql, "Oracle",
                           {4, 7, 8})

    def test_fq_oracle_zero_pushdown(self):
        """Oracle zero-pushdown plan shape (FQOR-LOCAL-009 and FQOR-LOCAL-016).

        With FILTER / PROJECTION / AGG / ORDER / LIMIT / JOIN all demoted the
        planner builds the baseline plan, which is what "every pushdown flag
        is false" means observably: EXPLAIN does not print the flags
        themselves, so the assertion is made on their consequence.

        1. Single-table query: one Remote SQL line, no WHERE / JOIN / GROUP BY.
        2. Local-table JOIN: two Remote SQL lines, no JOIN in either, and a
           local Filter node.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_tbl
        single_sql = f"SELECT id, val FROM {src} WHERE val > 20"
        join_sql = (f"SELECT a.id, b.val FROM {src} a INNER JOIN {src} b "
                    f"ON a.id = b.id WHERE a.val > 20")
        try:
            for clause in self._COARSE_CLAUSES:
                self._set_clause_pushdown(self._SRC_ORACLE, clause, False)

            plan_lines, remote_lines = self._explain(single_sql)
            assert len(remote_lines) == 1, (
                f"[oracle-zero] expected one baseline remote scan: {remote_lines!r}"
            )
            remote_sql = " ".join(remote_lines).lower()
            for clause in (" where ", " join ", " group by ", " having "):
                assert clause not in remote_sql, (
                    f"[oracle-zero] '{clause.strip()}' leaked into the baseline "
                    f"Remote SQL: {remote_sql}"
                )
            local_plan = "\n".join(
                line for line in plan_lines if "remote sql:" not in line.lower()
            )
            assert "filter" in local_plan.lower(), (
                f"[oracle-zero] predicate is not a local Filter node: {local_plan}"
            )

            # A user-forced demotion is client-side catalog state; a catalog
            # refresh that lands between the two plans silently reinstates the
            # server capability (observed as a JOIN pushdown flake on CI).
            # Re-assert immediately before the second plan so the assertion
            # measures the demoted plan rather than the refresh timing.
            for clause in self._COARSE_CLAUSES:
                self._set_clause_pushdown(self._SRC_ORACLE, clause, False)

            plan_lines, remote_lines = self._explain(join_sql)
            assert len(remote_lines) == 2, (
                f"[oracle-zero] expected two baseline remote scans for the JOIN: "
                f"{remote_lines!r}"
            )
            for line in remote_lines:
                assert " join " not in line.lower(), (
                    f"[oracle-zero] JOIN leaked into a baseline remote scan: {line}"
                )
            local_plan = "\n".join(
                line for line in plan_lines if "remote sql:" not in line.lower()
            )
            assert "filter" in local_plan.lower(), (
                f"[oracle-zero] JOIN predicate is not a local Filter node: {local_plan}"
            )

            # The baseline plan must still return the same rows as the local
            # mirror (FQOR-LOCAL-010 path 3).
            ref_sql = f"SELECT id, val FROM {self._local_tbl} WHERE val > 20"
            ref = self._get_rows(ref_sql)
            rows = self._get_rows(single_sql)
            self._compare_rows(ref, rows, ref_sql, single_sql, "Oracle", set())
        finally:
            for clause in self._COARSE_CLAUSES:
                self._set_clause_pushdown(self._SRC_ORACLE, clause, True)

    def test_fq_oracle_three_paths(self):
        """Oracle pushdown / local mirror / forced-local agreement (FQOR-LOCAL-010).

        The same SQL runs on the Oracle external table, on the local TDengine
        mirror and on the external table with every capability clause demoted;
        all three result sets must agree row by row.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_tbl
        local = self._local_tbl
        cases = [
            ("filter+projection",
             "SELECT id, val, label FROM {tbl} WHERE val >= 20 ORDER BY val, id",
             set()),
            ("aggregate",
             "SELECT COUNT(*), SUM(val), AVG(val) FROM {tbl} WHERE val > 20",
             {2}),
        ]
        for label, template, float_cols in cases:
            ref_sql = template.format(tbl=local)
            ref = self._get_rows(ref_sql)
            pushed_sql = template.format(tbl=src)
            pushed = self._get_rows(pushed_sql)
            self._compare_rows(ref, pushed, ref_sql, pushed_sql,
                               f"Oracle-push[{label}]", float_cols)
            try:
                for clause in self._COARSE_CLAUSES:
                    self._set_clause_pushdown(self._SRC_ORACLE, clause, False)
                baseline = self._get_rows(pushed_sql)
            finally:
                for clause in self._COARSE_CLAUSES:
                    self._set_clause_pushdown(self._SRC_ORACLE, clause, True)
            self._compare_rows(ref, baseline, ref_sql, pushed_sql,
                               f"Oracle-local[{label}]", float_cols)

    def test_fq_oracle_repeat_execution(self):
        """Oracle repeated execution returns identical results (FQOR-LOCAL-011).

        The same aggregate query runs ten times; every result must equal the
        first one and the local mirror.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_tbl
        template = ("SELECT label, COUNT(*) AS cnt, SUM(val) AS sv, AVG(val) AS av "
                    "FROM {tbl} WHERE val > 0 GROUP BY label ORDER BY label")
        ref_sql = template.format(tbl=self._local_tbl)
        ref = self._get_rows(ref_sql)
        sql = template.format(tbl=src)
        first = self._get_rows(sql)
        self._compare_rows(ref, first, ref_sql, sql, "Oracle", {3})
        for i in range(1, 10):
            rows = self._get_rows(sql)
            assert rows == first, (
                f"[oracle-repeat] execution {i + 1} differs from the first:\n"
                f"  first={first!r}\n  now  ={rows!r}"
            )

    def test_fq_oracle_views(self):
        """Oracle view results match the base-table queries (FQOR-LOCAL-012).

        1. An aggregate view versus the equivalent GROUP BY query.
        2. A filtering view versus the equivalent WHERE query.

        A materialized view is not covered: the provisioned test account only
        holds CREATE SESSION / CREATE TABLE / CREATE VIEW / CREATE SEQUENCE /
        UNLIMITED TABLESPACE (ensure_ext_env.sh), so CREATE MATERIALIZED VIEW
        is not permitted.  No skip is registered for it; the plain view legs
        exercise the same remote metadata path (ALL_OBJECTS has no PK).

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        base = self._oracle_tbl
        cases = [
            (f"SELECT label, cnt, sv FROM {self._oracle_view_sum} ORDER BY label",
             f"SELECT label, COUNT(*) AS cnt, SUM(val) AS sv FROM {base} "
             f"GROUP BY label ORDER BY label"),
            (f"SELECT id, val FROM {self._oracle_view_filt} ORDER BY id",
             f"SELECT id, val FROM {base} WHERE val > 20 ORDER BY id"),
        ]
        for view_sql, base_sql in cases:
            expected = self._get_rows(base_sql)
            rows = self._get_rows(view_sql)
            self._compare_rows(expected, rows, base_sql, view_sql, "Oracle-view",
                               set())

    def test_fq_oracle_large_result_set(self):
        """Oracle large result set is neither duplicated nor truncated (FQOR-LOCAL-013).

        The fixture holds 5000 rows (K = 1..5000, V = 2*K).  Both the row-by-row
        fetch and a pushed aggregate are checked against explicit expected
        values.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_bulk_tbl
        rows = self._get_rows(f"SELECT k, v FROM {src} ORDER BY k")
        assert len(rows) == _ORACLE_BULK_ROWS, (
            f"[oracle-bulk] expected {_ORACLE_BULK_ROWS} rows, got {len(rows)}"
        )
        actual = [tuple(int(x) for x in row) for row in rows]
        expected = [(i, 2 * i) for i in range(1, _ORACLE_BULK_ROWS + 1)]
        assert actual == expected, (
            f"[oracle-bulk] row set differs: first mismatch at "
            f"{next((i for i, (a, e) in enumerate(zip(actual, expected)) if a != e), -1)}"
        )
        assert len({k for k, _ in actual}) == _ORACLE_BULK_ROWS, (
            "[oracle-bulk] duplicate K values in the result"
        )

        agg = self._get_rows(
            f"SELECT COUNT(*), MIN(k), MAX(k), SUM(v) FROM {src}")
        expected_agg = (len(expected), expected[0][0], expected[-1][0],
                        sum(v for _, v in expected))
        assert tuple(int(x) for x in agg[0]) == expected_agg, (
            f"[oracle-bulk] aggregate mismatch: {agg[0]!r} != {expected_agg!r}"
        )

    def test_fq_oracle_type_semantics(self):
        """Oracle storage semantics match the local mirror.

        1. Metadata: DATE -> TIMESTAMP, NUMBER(10,2) -> DECIMAL(10,2),
           NUMBER(1,0) stays numeric (never BOOL), NVARCHAR2 -> NCHAR.
        2. Values: DECIMAL compares as an exact decimal string, DATE keeps
           second precision, and the numeric value 2 survives NUMBER(1,0).
        3. An empty string is stored as NULL and reads back as NULL.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        src = self._oracle_sem_tbl
        tdSql.query(f"DESC {src}")
        desc = {str(row[0]).upper(): str(row[1]).upper()
                for row in tdSql.queryResult}
        assert desc["D_DATE"].startswith("TIMESTAMP"), desc
        assert desc["D_DEC"].startswith("DECIMAL(10") and "2" in desc["D_DEC"], desc
        assert "BOOL" not in desc["D_NUM1"], desc
        assert desc["D_NUM1"] in ("TINYINT", "SMALLINT", "INT", "BIGINT") or \
            desc["D_NUM1"].startswith("DECIMAL"), desc
        assert desc["D_STR"].startswith("NCHAR"), desc

        sql = f"SELECT d_date, d_dec, d_num1, d_str FROM {src} ORDER BY d_date"
        ref_sql = (f"SELECT d_date, d_dec, d_num1, d_str FROM {self._local_sem_tbl} "
                   f"ORDER BY d_date")
        ref = self._get_rows(ref_sql)
        rows = self._get_rows(sql)
        self._compare_rows(ref, rows, ref_sql, sql, "Oracle-sem", set())
        assert [str(row[1]) for row in rows] == [r[1] for r in _ORACLE_SEM_ROWS], (
            f"[oracle-sem] DECIMAL values are not exact: {[str(r[1]) for r in rows]!r}"
        )
        assert [int(row[2]) for row in rows] == [r[2] for r in _ORACLE_SEM_ROWS], (
            f"[oracle-sem] NUMBER(1,0) values changed: {[r[2] for r in rows]!r}"
        )
        # Row 2 was inserted with '' and must read back as NULL, exactly like
        # the local mirror row that stores NULL.
        assert rows[1][3] is None and ref[1][3] is None, (
            f"[oracle-sem] empty string did not become NULL: oracle={rows[1][3]!r} "
            f"local={ref[1][3]!r}"
        )

    def test_fq_oracle_cross_source_join(self):
        """Oracle JOINs every other source and the local table (FQOR-LOCAL-006).

        Each leg joins the Oracle table with one other source on the timestamp
        column; the reference is the same join over the local microsecond
        table.  A three-way Oracle x MySQL x PostgreSQL join and an aggregate
        over it complete the matrix.

        Catalog: - Query:FederatedResultParity

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle parity leg
        """
        oracle = self._oracle_tbl
        local_us = f"{_LOCAL_US_DB}.{_LOCAL_TBL}"
        mysql = f"{self._SRC_MYSQL}.parity_t"
        pg = f"{self._SRC_PG}.parity_t"
        others = [
            ("MySQL", mysql),
            ("PG", pg),
            ("InfluxDB", f"{self._SRC_INFLUX}.parity_t"),
            ("SQLServer", f"{self._SRC_SQLSERVER}.parity_t"),
            ("local", local_us),
        ]
        ref_sql = (f"SELECT a.id, b.val FROM {local_us} a JOIN {local_us} b "
                   f"ON a.time = b.time ORDER BY a.id")
        ref = self._get_rows(ref_sql)
        for label, other in others:
            sql = (f"SELECT a.id, b.val FROM {oracle} a JOIN {other} b "
                   f"ON a.time = b.time ORDER BY a.id")
            rows = self._get_rows(sql)
            self._compare_rows(ref, rows, ref_sql, sql, f"Oracle×{label}", set())

        three_ref_sql = (
            f"SELECT COUNT(*) AS cnt, SUM(a.val + b.val + c.val) AS sv "
            f"FROM {local_us} a JOIN {local_us} b ON a.time = b.time "
            f"JOIN {local_us} c ON b.time = c.time")
        three_sql = (
            f"SELECT COUNT(*) AS cnt, SUM(a.val + b.val + c.val) AS sv "
            f"FROM {oracle} a JOIN {mysql} b ON a.time = b.time "
            f"JOIN {pg} c ON b.time = c.time")
        three_ref = self._get_rows(three_ref_sql)
        three = self._get_rows(three_sql)
        self._compare_rows(three_ref, three, three_ref_sql, three_sql,
                           "Oracle×MySQL×PG", set())

    def _run_ext_op_with_recover(self, op_name, fn):
        """Run one external-source operation — no retry, failures surface immediately."""
        return fn()

    # ── 乱序数据写入 ─────────────────────────────────────────────────────
    def _rewrite_all_data(self, row_indices):
        """Drop and re-insert parity data in all DBs using *row_indices* order."""
        ordered_rows    = [_ROWS[i] for i in row_indices]
        ordered_rows_dt = [_ROWS_DT[i] for i in row_indices]
        mapped_rows = [row[:3] + (100 + row[1],) + row[3:] for row in ordered_rows]
        mapped_rows_dt = [row[:3] + (100 + row[1],) + row[3:] for row in ordered_rows_dt]

        # --- Local TDengine ---
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(f"DROP TABLE IF EXISTS {_LOCAL_TBL}")
        tdSql.execute(
            f"CREATE TABLE {_LOCAL_TBL} ("
            f"  time TIMESTAMP, id INT, val INT, `Mapped_Val` INT, score DOUBLE, label NCHAR(32),"
            f"  nullable_val INT, lat DOUBLE, lon DOUBLE,"
            f"  wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32))")
        for row in mapped_rows:
            vals = ", ".join(parity_sql_val(x) for x in row)
            tdSql.execute(f"INSERT INTO {_LOCAL_TBL} VALUES ({vals})")

        # --- MySQL ---
        mysql_sqls = ["DELETE FROM parity_t"] + parity_make_insert_sqls(mapped_rows_dt)
        self._run_ext_op_with_recover(
            "mysql rewrite",
            lambda: ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, mysql_sqls),
        )

        # --- PostgreSQL ---
        pg_sqls = ["DELETE FROM public.parity_t"] + parity_make_insert_sqls(
            mapped_rows_dt, schema="public")
        self._run_ext_op_with_recover(
            "pg rewrite",
            lambda: ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, pg_sqls),
        )

        # --- SQL Server ---
        ss_sqls = ["DELETE FROM dbo.parity_t"] + parity_make_insert_sqls(
            mapped_rows_dt, schema="dbo")
        self._run_ext_op_with_recover(
            "sqlserver rewrite",
            lambda: ExtSrcEnv.sqlserver_exec(_SQLSERVER_DB, ss_sqls),
        )

        # --- Oracle ---
        oracle_sqls = [
            f'DELETE FROM "{_ORACLE_TABLE}"',
            _oracle_insert_all(_ORACLE_TABLE, mapped_rows_dt),
            "COMMIT",
        ]
        self._run_ext_op_with_recover(
            "oracle rewrite",
            lambda: ExtSrcEnv.oracle_exec_cfg(self._oracle_cfg(), oracle_sqls),
        )

        # --- InfluxDB (no DELETE; drop + recreate bucket) ---
        try:
            self._run_ext_op_with_recover(
                "influx drop bucket",
                lambda: ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET),
            )
        except Exception:
            pass
        self._run_ext_op_with_recover(
            "influx create bucket",
            lambda: ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET),
        )
        disorder_lines = [_influx_line(*_ROWS[i]) for i in row_indices]
        self._run_ext_op_with_recover(
            "influx rewrite",
            lambda: ExtSrcEnv.influx_write_cfg(self._influx_cfg(), _INFLUX_BUCKET, disorder_lines),
        )
        # Bucket recreation can invalidate the existing external-source binding.
        # Recreate the source so subsequent connector and taos -r queries see
        # the freshly written disorder dataset consistently.
        self._cleanup_src(self._SRC_INFLUX)
        self._mk_influx_real(self._SRC_INFLUX, database=_INFLUX_BUCKET)

    def test_fq_parity_disorder(self):
        """Re-run ALL parity cases after inserting data in deliberately shuffled order.

        Verifies that insertion order does not affect query results:
        same SQL, same data, different write order → identical results.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-15 wpan Disorder data parity test (s10 migration)
        """
        tdLog.info("\n[disorder] Re-inserting data in shuffled order …")
        self._rewrite_all_data(_DISORDER_IDX)
        self.run_parity_disorder(
            _PARITY_CASES,
            rewrite_data_fn=lambda: None,       # already rewritten above
            restore_data_fn=lambda: self._rewrite_all_data(list(range(len(_ROWS)))),
        )

    def test_fq_parity_epoch_window_disorder(self):
        """Epoch-precision parity for window queries with disordered data.

        Same as ``test_fq_parity_epoch_window`` but data is inserted in
        deliberately shuffled order first.  Verifies that insertion order
        does not affect window aggregation results or epoch precision.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-05-30 wpan Epoch window parity under disorder dimension
        """
        tdLog.info("\n[epoch-disorder] Re-inserting data in shuffled order …")
        self._rewrite_all_data(_DISORDER_IDX)
        try:
            self.run_epoch_parity_cases(_WIN_EPOCH_CASES, self._EPOCH_BASELINE_FILE)
        finally:
            tdLog.info("[epoch-disorder] Restoring original ordered data …")
            self._rewrite_all_data(list(range(len(_ROWS))))
