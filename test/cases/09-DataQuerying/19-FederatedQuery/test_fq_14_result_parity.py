"""
test_fq_14_result_parity.py

Result-parity test framework for federated query.

ALL features are tested against ALL four database types:
  1. Local TDengine (reference)
  2. MySQL external source
  3. PostgreSQL external source
  4. InfluxDB external source

For each SQL statement, the same logical query is executed against all
sources and results are compared row-by-row against the local TDengine
reference.  A test only omits an external source when the source's SQL
dialect physically cannot express the query (e.g. MySQL has no FULL
OUTER JOIN or NULLS FIRST syntax; PostgreSQL has no FIND_IN_SET).
All other features — including functions, operators, window queries,
JOINs, UNION, subqueries, NULLS FIRST/LAST, etc. — are tested on every
supported source.

Schema (all four sources identical):
  Local TDengine:  time TIMESTAMP PK, id INT, val INT, score DOUBLE, label NCHAR(32),
                   nullable_val INT, lat DOUBLE, lon DOUBLE,
                   wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32)
  MySQL:           time DATETIME(3) NOT NULL PK  (enables TDengine window queries)
  PostgreSQL:      time TIMESTAMP NOT NULL PK
  InfluxDB:        time (built-in timestamp); label tag; id/val/score/nullable_val fields

All four sources share the same column names (time, id, val, score, label, …),
so every SQL statement is identical across all sources with no per-source adaptation.

Environment:
  Enterprise edition, federatedQueryEnable=1
  MySQL 8.0+, PostgreSQL 14+, InfluxDB v3
  Python: pymysql, psycopg2, requests
"""

import datetime as _datetime
import os
import time
import pytest

from new_test_framework.utils import tdLog, tdSql


from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    ParityTestBase,
    QueryError,
    parity_sql_val,
    parity_make_insert_sqls,
)

# ── Pin process timezone to UTC ──────────────────────────────────────
# All epoch↔datetime conversions in this module and in the taospy connector
# must use the same timezone.  Setting TZ=UTC *before* any datetime
# computation makes the test produce identical results regardless of the
# host OS timezone.
#
# The taospy connector caches ``datetime.fromtimestamp(0)`` at import time
# (in ``taos.field._datetime_epoch``).  By the time this test module is
# loaded, the parent conftest.py has already imported taospy, so the cached
# epoch reflects the original OS timezone.  After changing TZ we must
# re-compute the epoch so the connector returns UTC datetimes.
import taos.field as _taos_field

_ORIG_TZ = os.environ.get('TZ')
_ORIG_DATETIME_EPOCH = _taos_field._datetime_epoch

os.environ['TZ'] = 'UTC'
time.tzset()
_taos_field._datetime_epoch = _datetime.datetime.fromtimestamp(0)
# ─────────────────────────────────────────────────────────────────────

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
_INFLUX_BUCKET = "fq_parity_i"
_LOCAL_DB      = "fq_parity_local"
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
_PARITY_GROUPS: dict[str, list] = {
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
        ("SELECT id, val FROM {tbl} t1 WHERE val > (SELECT AVG(val) FROM {tbl} t2 WHERE t2.label = t1.label) ORDER BY time",
         dict(positive=False, reason="correlated scalar subquery in WHERE not supported (0x800026A6)")),
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
        ("SELECT label, COUNT(*) AS cnt FROM {tbl} GROUP BY label ORDER BY 2 DESC",),
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
        ("SELECT a.id, a.val, b.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id ORDER BY a.id",
         dict(positive=False, reason="INNER JOIN without primary timestamp equal condition in ON clause not supported")),
        ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
         dict(positive=False, reason="LEFT JOIN without primary timestamp equal condition in ON clause not supported")),
        ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
         dict(positive=False, reason="RIGHT JOIN without primary timestamp equal condition in ON clause not supported")),
        ("SELECT a.id AS aid, b.id AS bid FROM {tbl} a FULL OUTER JOIN {tbl} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
         dict(positive=False, reason="FULL OUTER JOIN without primary timestamp equal condition in ON clause not supported")),
        ("SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {tbl} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {tbl} WHERE id >= 4) b ORDER BY a.id, b.id",
         dict(positive=False, reason="CROSS JOIN syntax not supported (0x80002600)")),
        ("SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",
         dict(positive=False, reason="INNER JOIN without primary timestamp equal condition in ON clause not supported")),
        ("SELECT a.id, b.val, c.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id INNER JOIN {tbl} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
         dict(positive=False, reason="3-way INNER JOIN without primary timestamp equal condition not supported")),
        # SEMI / ANTI JOIN — 非 ts-pk（负例，源自 fq_04 join_types）
        ("SELECT a.id, a.val FROM {tbl} a LEFT SEMI JOIN {tbl} b ON a.id = b.id ORDER BY a.id",
         dict(positive=False, reason="LEFT SEMI JOIN without primary timestamp equal condition not supported")),
        ("SELECT a.id, a.val FROM {tbl} a LEFT ANTI JOIN {tbl} b ON a.id = b.id ORDER BY a.id",
         dict(positive=False, reason="LEFT ANTI JOIN without primary timestamp equal condition not supported")),
        ("SELECT b.id, b.val FROM {tbl} a RIGHT SEMI JOIN {tbl} b ON a.id = b.id ORDER BY b.id",
         dict(positive=False, reason="RIGHT SEMI JOIN without primary timestamp equal condition not supported")),
        ("SELECT b.id, b.val FROM {tbl} a RIGHT ANTI JOIN {tbl} b ON a.id = b.id ORDER BY b.id",
         dict(positive=False, reason="RIGHT ANTI JOIN without primary timestamp equal condition not supported")),
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
    #                   STATE_WINDOW / EVENT_WINDOW / COUNT_WINDOW / EXTERNAL_WINDOW）────────────
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
    # ── lag: LAG/LEAD 窗口函数
    "lag": [
        ("SELECT val, LAG(val, 1) FROM {tbl} ORDER BY time",),
        ("SELECT val, LEAD(val, 1) FROM {tbl} ORDER BY time",),
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
        ("SELECT id, nullable_val FROM {tbl} ORDER BY nullable_val NULLS FIRST",),
        ("SELECT id, nullable_val FROM {tbl} ORDER BY nullable_val NULLS LAST",),
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
    # ── enc: AES/SM4 加解密 + MD5/CRC32 负向（NCHAR 类型错误）
    "enc": [
        ("SELECT MD5(label) FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="MD5 requires VARCHAR; all sources see label as NCHAR → type error")),
        ("SELECT AES_ENCRYPT(label, 'mykeystring12345') FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="AES_ENCRYPT requires VARCHAR; NCHAR input → type error")),
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
        ("SELECT TO_UNIXTIMESTAMP(time) FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="TO_UNIXTIMESTAMP requires string input; TIMESTAMP column → type error")),
    ],
    # ── interp: INTERP 时间序列插值（RANGE / EVERY / FILL）
    # Data: val=10,20,30,40,50 at 1-minute intervals from 2024-01-01 00:00:00
    "interp": [
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(1m) FILL(linear)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(prev)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(next)",),
        ("SELECT interp(val) FROM {tbl} " + _R4M + " EVERY(30s) FILL(null)",),
        ("SELECT interp(val) FROM {tbl} RANGE('2024-01-01 00:01:30.000') FILL(linear) SURROUND(1)",),
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
        ("SELECT TODAY() FROM {tbl} LIMIT 1", dict(day_start_cols={0})),
        # NOW() in expression — verify arithmetic with NOW()
        ("SELECT CASE WHEN NOW() > '2024-01-01 00:00:00' THEN 1 ELSE 0 END FROM {tbl} LIMIT 1",),
        # TODAY() in expression — verify arithmetic with TODAY()
        ("SELECT CASE WHEN TODAY() >= '2024-01-01' THEN 1 ELSE 0 END FROM {tbl} LIMIT 1",),
    ],
    # ── json: JSON 操作符拒绝 + LIKE 变通 + TO_JSON 类型错误
    "json": [
        ("SELECT label->'k' FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="'->' on NCHAR column → PAR_INVALID_COL_JSON")),
        ("SELECT id FROM {tbl} WHERE label CONTAINS 'num'",
         dict(positive=False, reason="CONTAINS on NCHAR column → PAR_INVALID_COL_JSON")),
        ("SELECT id FROM {tbl} WHERE label LIKE '%orth%' ORDER BY time",),
        ("SELECT TO_JSON(label) FROM {tbl} WHERE id = 1",
         dict(positive=False, reason="TO_JSON requires JSON type; NCHAR column → FUNC_FUNTION_PARA_TYPE")),
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

_PARITY_CASES: list[tuple[str, str, dict]] = [
    (f"{grp}-{i:02d}", entry[0], entry[1] if len(entry) > 1 else {})
    for grp, entries in _parity_group_items
    for i, entry in enumerate(entries, 1)
]

# ── Epoch parity cases: window queries with _wstart/_wend in SELECT ──
# These are run via `taos -r` (raw epoch output) to validate that
# timestamp precision (ms/µs/ns) propagates correctly from each
# external source.  Each entry: (case_id, sql_template, opts)
# where opts["ts_cols"] marks column indices that hold timestamps.
_WIN_EPOCH_CASES: list[tuple[str, str, dict]] = [
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
    "  time DATETIME(3) NOT NULL, id INT, val INT, score DOUBLE, label VARCHAR(32),"
    "  nullable_val INT, lat DOUBLE, lon DOUBLE,"
    "  wkt_point VARCHAR(64), wkt_poly VARCHAR(256), ts_str VARCHAR(32),"
    "  PRIMARY KEY (time)"
    ")",
] + [
    "INSERT INTO parity_t VALUES ({})".format(
        ", ".join(parity_sql_val(x) for x in (ts, i, v, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

# PostgreSQL: TIMESTAMP PRIMARY KEY
_PG_SETUP = [
    "DROP TABLE IF EXISTS public.parity_t",
    "CREATE TABLE public.parity_t ("
    "  time TIMESTAMP NOT NULL PRIMARY KEY,"
    "  id INT, val INT, score DOUBLE PRECISION, label VARCHAR(32),"
    "  nullable_val INT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,"
    "  wkt_point VARCHAR(64), wkt_poly VARCHAR(256), ts_str VARCHAR(32)"
    ")",
] + [
    "INSERT INTO public.parity_t VALUES ({})".format(
        ", ".join(parity_sql_val(x) for x in (ts, i, v, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS_DT
]

# InfluxDB line-protocol: label tag; numeric/string fields; timestamp in ns.
# nullable_val is omitted for NULL rows (InfluxDB has no NULL — omitting a field
# makes it NULL when read back via TDengine federated query).
def _influx_line(ts, i, v, s, l, nv, lat, lon, wp, wy, tss):
    fields = [f"id={i}i", f"val={v}i", f"score={s}"]
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
    f"  time TIMESTAMP, id INT, val INT, score DOUBLE, label NCHAR(32),"
    f"  nullable_val INT, lat DOUBLE, lon DOUBLE,"
    f"  wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32)"
    f")",
] + [
    "INSERT INTO {} VALUES ({})".format(
        _LOCAL_TBL,
        ", ".join(parity_sql_val(x) for x in (ts, i, v, s, l, nv, lat, lon, wp, wy, tss))
    )
    for ts, i, v, s, l, nv, lat, lon, wp, wy, tss in _ROWS
]

# 乱序写入：刻意打乱插入顺序（row-3, row-1, row-5, row-2, row-4），
# 验证无论插入顺序如何，查询结果都与顺序写入一致。
_DISORDER_IDX = [2, 0, 4, 1, 3]


class TestFq14ResultParity(ParityTestBase):
    """Result-parity: local TDengine == MySQL == PostgreSQL == InfluxDB.

    Every test executes the same logical query against all four sources
    and asserts row-by-row equality.  A source is only omitted when its
    SQL dialect physically lacks the required syntax.
    """

    _SRC_MYSQL  = "fq_parity_src_m"
    _SRC_PG     = "fq_parity_src_p"
    _SRC_INFLUX = "fq_parity_src_i"
    _class_setup_done = False
    _FLOAT_TOL = _FLOAT_TOL
    _BASELINE_FILE = os.path.join(os.path.dirname(__file__), "ans", "test_fq_14_result_parity.txt")
    _EPOCH_BASELINE_FILE = os.path.join(os.path.dirname(__file__), "ans", "test_fq_14_win_epoch.txt")
    _PARITY_CASES = _PARITY_CASES
    _PARITY_TZ = _FIXED_TZ  # Pin epoch→datetime to UTC (matches ALTER LOCAL below)

    @property
    def _local_tbl(self):
        return f"{_LOCAL_DB}.{_LOCAL_TBL}"

    def _ext_sources(self):
        return [
            ("MySQL",    f"{self._SRC_MYSQL}.parity_t"),
            ("PG",       f"{self._SRC_PG}.parity_t"),
            ("InfluxDB", f"{self._SRC_INFLUX}.parity_t"),
        ]

    def setup_method(self, method):
        if TestFq14ResultParity._class_setup_done:
            return
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

        # Pin client timezone to UTC so all datetime conversions are
        # deterministic regardless of the host OS timezone setting.
        tdSql.execute('ALTER LOCAL "timezone" "UTC"')

        ExtSrcEnv.ensure_env()

        tdSql.executes(_LOCAL_SETUP)

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, _MYSQL_SETUP)
        self._cleanup_src(self._SRC_MYSQL)
        self._mk_mysql_real(self._SRC_MYSQL, database=_MYSQL_DB)

        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _PG_SETUP)
        self._cleanup_src(self._SRC_PG)
        self._mk_pg_real(self._SRC_PG, database=_PG_DB, schema="public")

        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), _INFLUX_BUCKET, _INFLUX_LINES)
        self._cleanup_src(self._SRC_INFLUX)
        self._mk_influx_real(self._SRC_INFLUX, database=_INFLUX_BUCKET)

        TestFq14ResultParity._class_setup_done = True

    def teardown_class(self):
        tmp = TestFq14ResultParity()
        tmp._cleanup_src(tmp._SRC_MYSQL, tmp._SRC_PG, tmp._SRC_INFLUX)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
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

        # Restore original timezone so subsequent tests in the same process
        # are not affected by the UTC pin.
        if _ORIG_TZ is not None:
            os.environ['TZ'] = _ORIG_TZ
        else:
            os.environ.pop('TZ', None)
        time.tzset()
        _taos_field._datetime_epoch = _ORIG_DATETIME_EPOCH

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
        """

        self.run_parity_cases(_PARITY_CASES, parity_groups=_PARITY_GROUPS)

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

    def _run_ext_op_with_recover(self, op_name, fn):
        """Run one external-source operation — no retry, failures surface immediately."""
        return fn()

    # ── 乱序数据写入 ─────────────────────────────────────────────────────
    def _rewrite_all_data(self, row_indices):
        """Drop and re-insert parity data in all 4 DBs using *row_indices* order."""
        ordered_rows    = [_ROWS[i] for i in row_indices]
        ordered_rows_dt = [_ROWS_DT[i] for i in row_indices]

        # --- Local TDengine ---
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(f"DROP TABLE IF EXISTS {_LOCAL_TBL}")
        tdSql.execute(
            f"CREATE TABLE {_LOCAL_TBL} ("
            f"  time TIMESTAMP, id INT, val INT, score DOUBLE, label NCHAR(32),"
            f"  nullable_val INT, lat DOUBLE, lon DOUBLE,"
            f"  wkt_point NCHAR(64), wkt_poly NCHAR(256), ts_str NCHAR(32))")
        for row in ordered_rows:
            vals = ", ".join(parity_sql_val(x) for x in row)
            tdSql.execute(f"INSERT INTO {_LOCAL_TBL} VALUES ({vals})")

        # --- MySQL ---
        mysql_sqls = ["DELETE FROM parity_t"] + parity_make_insert_sqls(ordered_rows_dt)
        self._run_ext_op_with_recover(
            "mysql rewrite",
            lambda: ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, mysql_sqls),
        )

        # --- PostgreSQL ---
        pg_sqls = ["DELETE FROM public.parity_t"] + parity_make_insert_sqls(
            ordered_rows_dt, schema="public")
        self._run_ext_op_with_recover(
            "pg rewrite",
            lambda: ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, pg_sqls),
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

