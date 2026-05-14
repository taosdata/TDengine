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
  Local TDengine:  time TIMESTAMP PK, id INT, val INT, score DOUBLE, label NCHAR(32)
  MySQL:           time DATETIME(3) NOT NULL PK  (enables TDengine window queries)
  PostgreSQL:      time TIMESTAMP NOT NULL PK
  InfluxDB:        time (built-in timestamp); label tag; id/val/score fields

All four sources share the same column names (time, id, val, score, label),
so every SQL statement is identical across all sources with no per-source adaptation.

Environment:
  Enterprise edition, federatedQueryEnable=1
  MySQL 8.0+, PostgreSQL 14+, InfluxDB v3
  Python: pymysql, psycopg2, requests
"""

import math
import os
import re
import time
import pytest

from new_test_framework.utils import tdLog, tdSql


class QueryError(AssertionError):
    """Carries structured error information from a failed query."""
    def __init__(self, errno: int | None, err_info: str | None, sql: str, raw: Exception):
        self.qerrno   = errno      # raw integer errno, e.g. -2147473820
        self.err_info = err_info
        self.sql      = sql
        detail = ""
        if errno is not None:
            detail += f"\n  errno:      {errno:#010x}"
        if err_info:
            detail += f"\n  error_info: {err_info}"
        super().__init__(
            f"Query execution failed{detail}\n"
            f"  sql: {sql}\n"
            f"  raw exception: {raw}"
        )

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
)

_MYSQL_DB      = "fq_parity_m"
_PG_DB         = "fq_parity_p"
_INFLUX_BUCKET = "fq_parity_i"
_LOCAL_DB      = "fq_parity_local"
_LOCAL_TBL     = "parity_t"
_FLOAT_TOL     = 1e-4

# Each entry: (sql_template, opts)
# opts keys: positive (bool, default True), reason (str, default ""),
#             float_cols, ordered, source_expected, skip_sources
# positive=True  → results must match local reference (正向用例)
# positive=False → all sources must error with the same errno (负向用例，reason必填)
_PARITY_CASES = [
    # ── WHERE 比较条件（=  <>  <  <=  >  >=  BETWEEN  NOT BETWEEN）
    ("SELECT id, val FROM {tbl} WHERE val > 20 ORDER BY time",),  # #1
    ("SELECT id, val FROM {tbl} WHERE val = 30 ORDER BY time",),  # #2
    ("SELECT id FROM {tbl} WHERE val <> 30 ORDER BY time",),  # #3
    ("SELECT id, val FROM {tbl} WHERE val <= 30 ORDER BY time",),  # #4
    ("SELECT id, val FROM {tbl} WHERE val >= 30 ORDER BY time",),  # #5
    ("SELECT id, val FROM {tbl} WHERE val BETWEEN 20 AND 40 ORDER BY time",),  # #6
    ("SELECT id FROM {tbl} WHERE val NOT BETWEEN 20 AND 40 ORDER BY time",),  # #7
    # ── WHERE 字符串条件（IN  NOT IN  LIKE  NOT LIKE  IS NULL  IS NOT NULL）
    ("SELECT id FROM {tbl} WHERE label IN ('north', 'east') ORDER BY time",),  # #8
    ("SELECT id FROM {tbl} WHERE label NOT IN ('east') ORDER BY time",),  # #9
    ("SELECT id FROM {tbl} WHERE label LIKE 'n%' ORDER BY time",),  # #10
    ("SELECT id FROM {tbl} WHERE label LIKE '%th' ORDER BY time",),  # #11
    ("SELECT id FROM {tbl} WHERE label NOT LIKE 'n%' ORDER BY time",),  # #12
    ("SELECT id FROM {tbl} WHERE label IS NOT NULL ORDER BY time",),  # #13
    ("SELECT id FROM {tbl} WHERE label IS NULL ORDER BY time",),  # #14
    # ── WHERE 逻辑组合（AND  OR  NOT）
    ("SELECT id, val FROM {tbl} WHERE val > 20 AND val < 50 ORDER BY time",),  # #15
    ("SELECT id, val FROM {tbl} WHERE val < 15 OR val > 45 ORDER BY time",),  # #16
    ("SELECT id FROM {tbl} WHERE NOT (val > 30) ORDER BY time",),  # #17
    ("SELECT id FROM {tbl} WHERE (val < 20 OR val > 40) AND label <> 'east' ORDER BY id",),  # #18
    # ── 条件表达式（COALESCE  NULLIF  IF  IFNULL  NVL2  CASE WHEN）
    ("SELECT id, COALESCE(label, 'unknown') FROM {tbl} ORDER BY time",),  # #19
    ("SELECT id, NULLIF(val, 30) FROM {tbl} ORDER BY time",),  # #20
    ("SELECT id, IF(val > 30, 'high', 'low') AS cat FROM {tbl} ORDER BY time",),  # #21
    ("SELECT id, IFNULL(label, 'none') FROM {tbl} ORDER BY time",),  # #22
    ("SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {tbl} ORDER BY time",),  # #23
    ("SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {tbl} ORDER BY time",),  # #24
    ("SELECT id, NULLIF(val, 30) FROM {tbl} ORDER BY time",),  # #25
    ("SELECT id, COALESCE(NULL, val) FROM {tbl} ORDER BY time",),  # #26
    ("SELECT id, NVL2(label, 'has_val', 'no_val') AS nv FROM {tbl} ORDER BY time",),  # #27
    ("SELECT id, NULLIF(val, 30) AS v FROM {tbl} ORDER BY v ASC NULLS FIRST",),  # #28
    ("SELECT id, NULLIF(val, 30) AS v FROM {tbl} ORDER BY v DESC NULLS LAST",),  # #29
    # ── 算术运算符（一元 -  +  -  *  /  %  &  |）
    ("SELECT id, val * 2 + 1 FROM {tbl} ORDER BY time",),  # #30
    ("SELECT id, val * 3 FROM {tbl} ORDER BY time",),  # #31
    ("SELECT id, val / 4.0 FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #32
    ("SELECT id, val % 3 FROM {tbl} ORDER BY time",),  # #33
    ("SELECT id, val & 3 FROM {tbl} ORDER BY time",),  # #34
    ("SELECT id, val | 1 FROM {tbl} ORDER BY time",),  # #35
    ("SELECT id, val * 2 AS dbl FROM {tbl} ORDER BY dbl",),  # #36
    # ── 数学函数（ABS  CEIL  FLOOR  ROUND  SQRT  POW  MOD  SIGN  GREATEST  LEAST  PI  TRUNCATE  DEGREES  RADIANS  EXP  LN  LOG）
    ("SELECT id, GREATEST(id, val) FROM {tbl} ORDER BY time",),  # #37
    ("SELECT id, LEAST(id, val) FROM {tbl} ORDER BY time",),  # #38
    ("SELECT id, TRUNCATE(PI(), 5) AS pi5 FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #39
    ("SELECT id, ABS(20 - val) FROM {tbl} ORDER BY time",),  # #40
    ("SELECT id, CEIL(score) FROM {tbl} ORDER BY time",),  # #41
    ("SELECT id, FLOOR(score) FROM {tbl} ORDER BY time",),  # #42
    ("SELECT id, ROUND(score, 1) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #43
    ("SELECT id, ROUND(score, 0) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #44
    ("SELECT id, SQRT(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #45
    ("SELECT id, POW(id, 2) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #46
    ("SELECT id, MOD(val, 3) FROM {tbl} ORDER BY time",),  # #47
    ("SELECT id, SIGN(val - 25) FROM {tbl} ORDER BY time",),  # #48
    ("SELECT id, EXP(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #49
    ("SELECT id, LN(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #50
    ("SELECT id, LOG(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #51
    ("SELECT id, LOG(val, 10) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #52
    ("SELECT id, TRUNCATE(score, 1) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #53
    ("SELECT id, DEGREES(score) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #54
    ("SELECT id, RADIANS(val) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #55
    # ── 三角函数（SIN  COS  TAN  ASIN  ACOS  ATAN）
    ("SELECT id, SIN(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #56
    ("SELECT id, COS(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #57
    ("SELECT id, TAN(score) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #58
    ("SELECT id, ASIN(score / 10.0) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #59
    ("SELECT id, ACOS(score / 10.0) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #60
    ("SELECT id, ATAN(id) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #61
    # ── 字符串函数（LOWER  UPPER  LENGTH  TRIM  CONCAT  SUBSTRING  REPLACE  POSITION  REPEAT  ASCII  CHAR  FIND_IN_SET）
    ("SELECT id, LOWER(label) FROM {tbl} ORDER BY time",),  # #62
    ("SELECT id, UPPER(label) FROM {tbl} ORDER BY time",),  # #63
    ("SELECT id, CHAR_LENGTH(label) FROM {tbl} ORDER BY time",),  # #64
    ("SELECT id, LENGTH(label) FROM {tbl} ORDER BY time",),  # #65
    ("SELECT id, LTRIM(CONCAT(' ', label)) FROM {tbl} ORDER BY time",),  # #66
    ("SELECT id, RTRIM(CONCAT(label, ' ')) FROM {tbl} ORDER BY time",),  # #67
    ("SELECT id, TRIM(CONCAT(' ', label, ' ')) FROM {tbl} ORDER BY time",),  # #68
    ("SELECT id, CONCAT(label, '-', LOWER(label)) FROM {tbl} ORDER BY time",),  # #69
    ("SELECT id, CONCAT_WS('-', label, LOWER(label)) FROM {tbl} ORDER BY time",),  # #70
    ("SELECT id, SUBSTRING(label, 1, 3) FROM {tbl} ORDER BY time",),  # #71
    ("SELECT id, SUBSTR(label, -3, 3) FROM {tbl} ORDER BY time",),  # #72
    ("SELECT id, REPLACE(label, 'north', 'n') FROM {tbl} ORDER BY time",),  # #73
    ("SELECT id, POSITION('o' IN label) FROM {tbl} ORDER BY time",),  # #74
    ("SELECT id, REPEAT('x', id) FROM {tbl} ORDER BY time",),  # #75
    ("SELECT id, ASCII(label) FROM {tbl} ORDER BY time",),  # #76
    ("SELECT id, CHAR(65) FROM {tbl} ORDER BY time",),  # #77
    ("SELECT id, FIND_IN_SET(label, 'north,south,east') AS pos FROM {tbl} ORDER BY time",),  # #78
    ("SELECT id, SUBSTRING_INDEX(label, 'o', 1) AS si FROM {tbl} ORDER BY time",),  # #79
    # ── 哈希 / 编码函数（MD5  SHA1  SHA2  CRC32  TO_BASE64  FROM_BASE64）
    ("SELECT id, MD5(CAST(label AS VARCHAR(32))) FROM {tbl} ORDER BY time",),  # #80
    ("SELECT id, TO_BASE64(label) FROM {tbl} ORDER BY time",),  # #81
    ("SELECT id, FROM_BASE64(TO_BASE64(label)) AS decoded FROM {tbl} ORDER BY time",),  # #82
    ("SELECT id, SHA1(CAST(label AS VARCHAR(32))) FROM {tbl} ORDER BY time",),  # #83
    ("SELECT id, SHA2(CAST(label AS VARCHAR(32)), 256) FROM {tbl} ORDER BY time",),  # #84
    ("SELECT id, CRC32(label) FROM {tbl} ORDER BY time",),  # #85
    # ── 聚合函数（COUNT  SUM  MIN  MAX  AVG  STDDEV  VARIANCE  COUNT DISTINCT）
    ("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {tbl}", dict(ordered=False)),  # #86
    ("SELECT AVG(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),  # #87
    ("SELECT STDDEV(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),  # #88
    ("SELECT VARIANCE(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),  # #89
    ("SELECT STDDEV_SAMP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),  # #90
    ("SELECT VAR_SAMP(val) FROM {tbl}", dict(float_cols={0}, ordered=False)),  # #91
    ("SELECT COUNT(DISTINCT val) FROM {tbl}",
     dict(ordered=False, positive=False, reason="COUNT(DISTINCT col) syntax not supported (0x80002600); use a subquery workaround")),  # #92
    ("SELECT COUNT(label) FROM {tbl}", dict(ordered=False)),  # #93
    ("SELECT SUM(CASE WHEN label = 'north' THEN val ELSE 0 END) FROM {tbl}", dict(ordered=False)),  # #94
    # ── GROUP BY / HAVING
    ("SELECT label, COUNT(*) FROM {tbl} GROUP BY label ORDER BY label",),  # #95
    ("SELECT label, COUNT(*) FROM {tbl} GROUP BY label HAVING COUNT(*) > 1 ORDER BY label",),  # #96
    ("SELECT label, SUM(val) FROM {tbl} GROUP BY label ORDER BY label",),  # #97
    ("SELECT label, AVG(val) FROM {tbl} GROUP BY label ORDER BY label", dict(float_cols={1})),  # #98
    ("SELECT label, MAX(val) FROM {tbl} GROUP BY label ORDER BY label",),  # #99
    ("SELECT label, MIN(val) FROM {tbl} GROUP BY label ORDER BY label",),  # #100
    ("SELECT label, COUNT(id) FROM {tbl} GROUP BY label ORDER BY label",),  # #101
    ("SELECT label, SUM(val) FROM {tbl} GROUP BY label HAVING SUM(val) > 30 ORDER BY label",),  # #102
    ("SELECT label, AVG(score) FROM {tbl} GROUP BY label HAVING AVG(score) > 2.0 ORDER BY label", dict(float_cols={1})),  # #103
    ("SELECT val / 20 AS bucket, COUNT(*) FROM {tbl} GROUP BY val / 20 ORDER BY bucket",),  # #104
    ("SELECT label, COUNT(*) AS cnt FROM {tbl} GROUP BY label ORDER BY 2 DESC",),  # #105
    ("SELECT label, COUNT(*), SUM(val), AVG(score) FROM {tbl} WHERE val >= 20 GROUP BY label ORDER BY label", dict(float_cols={3})),  # #106
    # ── 类型转换（CAST）
    ("SELECT id, CAST(val AS DOUBLE) FROM {tbl} ORDER BY time", dict(float_cols={1})),  # #107
    ("SELECT id, CHAR_LENGTH(CAST(val AS VARCHAR(10))) FROM {tbl} ORDER BY time",),  # #108
    ("SELECT id, CAST(score AS BIGINT) FROM {tbl} ORDER BY time",),  # #109
    # ── ORDER BY / LIMIT / OFFSET / DISTINCT / NULLS FIRST/LAST
    ("SELECT id, val - 5 FROM {tbl} ORDER BY time",),  # #110
    ("SELECT id, val, score, label FROM {tbl} ORDER BY time", dict(float_cols={2})),  # #111
    ("SELECT id, val FROM {tbl} ORDER BY time LIMIT 3 OFFSET 1",),  # #112
    ("SELECT DISTINCT label FROM {tbl} ORDER BY label",),  # #113
    ("SELECT id, val FROM {tbl} ORDER BY val DESC",),  # #114
    ("SELECT id, -val FROM {tbl} ORDER BY time",),  # #115
    ("SELECT id, label, val FROM {tbl} ORDER BY label, val",),  # #116
    ("SELECT DISTINCT label, val FROM {tbl} ORDER BY label, val",),  # #117
    # ── 子查询（IN  NOT IN  EXISTS  ALL  ANY  SOME  标量子查询  派生表）
    ("SELECT id FROM {tbl} WHERE val IN (SELECT val FROM {tbl} WHERE label = 'north') ORDER BY time",),  # #118
    ("SELECT id FROM {tbl} WHERE val NOT IN (SELECT val FROM {tbl} WHERE label = 'east') ORDER BY time",),  # #119
    ("SELECT id FROM {tbl} t1 WHERE EXISTS (SELECT 1 FROM {tbl} t2 WHERE t2.id = t1.id AND t2.label = 'north') ORDER BY time",
     dict(positive=False, reason="correlated EXISTS subquery as expr not supported (0x800026A6)")),  # #120
    ("SELECT id FROM {tbl} t1 WHERE NOT EXISTS (SELECT 1 FROM {tbl} t2 WHERE t2.id = t1.id AND t2.label = 'south') ORDER BY time",
     dict(positive=False, reason="correlated NOT EXISTS subquery as expr not supported (0x800026A6)")),  # #121
    ("SELECT id FROM {tbl} WHERE val > ALL (SELECT val FROM {tbl} WHERE val < 20) ORDER BY time",),  # #122
    ("SELECT id FROM {tbl} WHERE val > ANY (SELECT val FROM {tbl} WHERE val < 30) ORDER BY time",),  # #123
    ("SELECT id FROM {tbl} WHERE val >= SOME (SELECT val FROM {tbl} WHERE val >= 30) ORDER BY time",),  # #124
    ("SELECT id, val, (SELECT AVG(val) FROM {tbl}) AS avg_val FROM {tbl} ORDER BY time", dict(float_cols={2})),  # #125
    ("SELECT id, val FROM {tbl} WHERE val > (SELECT AVG(val) FROM {tbl}) ORDER BY time",),  # #126
    ("SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {tbl} GROUP BY label) sub", dict(float_cols={0}, ordered=False)),  # #127
    ("SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {tbl}) sub ORDER BY id",),  # #128
    ("SELECT id FROM {tbl} WHERE id IN (SELECT id FROM {tbl} WHERE val > 30) ORDER BY id",),  # #129
    ("SELECT id FROM {tbl} WHERE id NOT IN (SELECT id FROM {tbl} WHERE val > 30) ORDER BY id",),  # #130
    # ── UNION / UNION ALL
    ("SELECT val FROM {tbl} WHERE id <= 2 UNION ALL SELECT val FROM {tbl} WHERE id <= 2 ORDER BY val",),  # #131
    ("SELECT label FROM {tbl} WHERE id IN (1,3) UNION SELECT label FROM {tbl} WHERE id IN (1,4) ORDER BY label",),  # #132
    # ── JOIN（INNER  LEFT  RIGHT  FULL OUTER  CROSS  3-way）
    ("SELECT a.id, a.val, b.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id ORDER BY a.id",
     dict(positive=False, reason="INNER JOIN without primary timestamp equal condition in ON clause not supported")),  # #133
    ("SELECT a.id, b.val FROM {tbl} a LEFT JOIN {tbl} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
     dict(positive=False, reason="LEFT JOIN without primary timestamp equal condition in ON clause not supported")),  # #134
    ("SELECT a.id, b.val FROM {tbl} a RIGHT JOIN {tbl} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
     dict(positive=False, reason="RIGHT JOIN without primary timestamp equal condition in ON clause not supported")),  # #135
    ("SELECT a.id AS aid, b.id AS bid FROM {tbl} a FULL OUTER JOIN {tbl} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
     dict(positive=False, reason="FULL OUTER JOIN without primary timestamp equal condition in ON clause not supported")),  # #136
    ("SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {tbl} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {tbl} WHERE id >= 4) b ORDER BY a.id, b.id",
     dict(positive=False, reason="CROSS JOIN syntax not supported (0x80002600)")),  # #137
    ("SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",
     dict(positive=False, reason="INNER JOIN without primary timestamp equal condition in ON clause not supported")),  # #138
    ("SELECT a.id, b.val, c.label FROM {tbl} a INNER JOIN {tbl} b ON a.id = b.id INNER JOIN {tbl} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
     dict(positive=False, reason="3-way INNER JOIN without primary timestamp equal condition not supported")),  # #139
    # ── 时间窗口（INTERVAL  FILL  PARTITION BY  SESSION  STATE_WINDOW  EVENT_WINDOW  COUNT_WINDOW）
    ("SELECT COUNT(*) AS cnt FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),  # #140
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),  # #141
    ("SELECT AVG(score) AS avg_s FROM {tbl} INTERVAL(1m) ORDER BY _wstart", dict(float_cols={0})),  # #142
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} INTERVAL(2m) ORDER BY _wstart",),  # #143
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",),  # #144
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",),  # #145
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",),  # #146
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",),  # #147
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart", dict(float_cols={0})),  # #148
    ("SELECT label, COUNT(*) AS cnt FROM {tbl} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart",),  # #149
    ("SELECT COUNT(*) AS cnt FROM {tbl} SESSION(time, 30s) ORDER BY _wstart",),  # #150
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} SESSION(time, 2m) ORDER BY _wstart",),  # #151
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} STATE_WINDOW(val >= 30) ORDER BY _wstart",),  # #152
    ("SELECT COUNT(*) AS cnt FROM {tbl} STATE_WINDOW(label) ORDER BY _wstart",),  # #153
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} EVENT_WINDOW START WITH val >= 30 END WITH val >= 50 ORDER BY _wstart",),  # #154
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} COUNT_WINDOW(2) ORDER BY _wstart",),  # #155
    ("SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {tbl} COUNT_WINDOW(3) ORDER BY _wstart",),  # #156
    ("SELECT SUM(val) AS sv FROM {tbl} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",),  # #157
    ("SELECT COUNT(*) AS cnt FROM {tbl} INTERVAL(1m) ORDER BY _wstart",),  # #158
]


# 5 rows, 2024-01-01 00:00-04:00 UTC, 1-minute spacing
_ROWS = [
    (1704067200000, 1, 10, 1.5, "north"),
    (1704067260000, 2, 20, 2.5, "south"),
    (1704067320000, 3, 30, 3.5, "north"),
    (1704067380000, 4, 40, 4.5, "south"),
    (1704067440000, 5, 50, 5.5, "east"),
]

_ROWS_DT = [
    ("2024-01-01 00:00:00.000", 1, 10, 1.5, "north"),
    ("2024-01-01 00:01:00.000", 2, 20, 2.5, "south"),
    ("2024-01-01 00:02:00.000", 3, 30, 3.5, "north"),
    ("2024-01-01 00:03:00.000", 4, 40, 4.5, "south"),
    ("2024-01-01 00:04:00.000", 5, 50, 5.5, "east"),
]

# MySQL: DATETIME(3) PRIMARY KEY — TDengine recognises as time axis for window queries
_MYSQL_SETUP = [
    "DROP TABLE IF EXISTS parity_t",
    "CREATE TABLE parity_t ("
    "  time DATETIME(3) NOT NULL, id INT, val INT, score DOUBLE, label VARCHAR(32),"
    "  PRIMARY KEY (time)"
    ")",
] + [
    f"INSERT INTO parity_t VALUES ('{ts}', {i}, {v}, {s}, '{l}')"
    for ts, i, v, s, l in _ROWS_DT
]

# PostgreSQL: TIMESTAMP PRIMARY KEY
_PG_SETUP = [
    "DROP TABLE IF EXISTS public.parity_t",
    "CREATE TABLE public.parity_t ("
    "  time TIMESTAMP NOT NULL PRIMARY KEY,"
    "  id INT, val INT, score DOUBLE PRECISION, label VARCHAR(32)"
    ")",
] + [
    f"INSERT INTO public.parity_t VALUES ('{ts}', {i}, {v}, {s}, '{l}')"
    for ts, i, v, s, l in _ROWS_DT
]

# InfluxDB line-protocol: label tag; id/val/score fields; timestamp in ns
_INFLUX_LINES = [
    f"parity_t,label={l} id={i}i,val={v}i,score={s} {ts}000000"
    for ts, i, v, s, l in _ROWS
]

_LOCAL_SETUP = [
    f"DROP DATABASE IF EXISTS {_LOCAL_DB}",
    f"CREATE DATABASE {_LOCAL_DB}",
    f"USE {_LOCAL_DB}",
    f"CREATE TABLE {_LOCAL_TBL} ("
    f"  time TIMESTAMP, id INT, val INT, score DOUBLE, label NCHAR(32)"
    f")",
] + [
    f"INSERT INTO {_LOCAL_TBL} VALUES ({ts}, {i}, {v}, {s}, '{l}')"
    for ts, i, v, s, l in _ROWS
]


def _float_eq(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(str(a)) - float(str(b))) <= _FLOAT_TOL
    except (TypeError, ValueError):
        return str(a) == str(b)


_BASELINE_FILE = os.path.join(os.path.dirname(__file__), "ans", "test_fq_14_result_parity.txt")


def _serialize_cell(val, col_idx, float_cols):
    """Serialize a single cell value to a stable string representation."""
    if val is None:
        return "NULL"
    if col_idx in float_cols:
        try:
            return f"{float(str(val)):.6g}"
        except (TypeError, ValueError):
            pass
    return str(val)


def _serialize_case(idx, total, sql_template, positive, ref_rows, local_qerr, float_cols, ordered):
    """Serialize local result of one parity case to a canonical text block."""
    kind_tag = "POS" if positive else "NEG"
    lines = [f"### #{idx:03d} {kind_tag}", f"SQL: {sql_template}"]
    if local_qerr is not None:
        errno = local_qerr.qerrno
        err_info = local_qerr.err_info or ""
        lines.append(f"ERROR {errno if errno is not None else 0:#010x}: {err_info}")
    else:
        lines.append("RESULT")
        for row in ref_rows:
            cells = [_serialize_cell(v, ci, float_cols) for ci, v in enumerate(row)]
            lines.append("|".join(cells))
    lines.append("---")
    return "\n".join(lines)


class TestFq14ResultParity(FederatedQueryTestMixin):
    """Result-parity: local TDengine == MySQL == PostgreSQL == InfluxDB.

    Every test executes the same logical query against all four sources
    and asserts row-by-row equality.  A source is only omitted when its
    SQL dialect physically lacks the required syntax.
    """

    _SRC_MYSQL  = "fq_parity_src_m"
    _SRC_PG     = "fq_parity_src_p"
    _SRC_INFLUX = "fq_parity_src_i"
    _class_setup_done = False

    @property
    def _L(self):
        return f"{_LOCAL_DB}.{_LOCAL_TBL}"

    @property
    def _M(self):
        return f"{self._SRC_MYSQL}.parity_t"

    @property
    def _P(self):
        return f"{self._SRC_PG}.parity_t"

    @property
    def _I(self):
        return f"{self._SRC_INFLUX}.parity_t"

    def setup_method(self, method):
        if TestFq14ResultParity._class_setup_done:
            return
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
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

    def _get_rows(self, sql, no_retry=False):
        """Execute *sql* and return results as a list of tuples.

        On failure raises QueryError that includes the SQL text,
        errno, and error_info so the failing query is immediately
        identifiable in the test report without re-running.

        Pass no_retry=True (for negative cases) to skip the 10-attempt
        retry loop and fail immediately, saving ~9 seconds per source.
        When no_retry=True the cursor is called directly to avoid the
        tdLog.error() that tdSql.query() emits on failure — expected
        failures should be debug info, not errors.
        """
        try:
            if no_retry:
                # Bypass tdSql.query() so that expected SQL failures are logged
                # at DEBUG level instead of ERROR level.
                tdSql.cursor.execute(sql)
                tdSql.queryResult = tdSql.cursor.fetchall()
                tdSql.queryRows = len(tdSql.queryResult)
                tdSql.queryCols = len(tdSql.cursor.description)
            else:
                tdSql.query(sql, queryTimes=10)
        except Exception as e:
            # Extract errno directly from the exception.  tdSql.query() does NOT
            # update tdSql.errno on failure, so tdSql.errno may hold a stale value
            # from a prior tdSql.error() call in a different test class (e.g. the
            # preceding fq_05 class sets tdSql.errno = TSDB_CODE_EXT_TABLE_NOT_EXIST
            # and that value persists into this class, corrupting baseline comparisons).
            _eargs = getattr(e, 'args', ())
            errno = None
            if len(_eargs) >= 2 and isinstance(_eargs[-1], int):
                errno = _eargs[-1]
            if errno is None:
                errno = getattr(e, 'errno', None)
            err_info = None
            if _eargs:
                err_info = str(_eargs[0])
            tdLog.debug(f"expected SQL failure: {sql!r} → {err_info!r} (errno={errno})")
            raise QueryError(errno, err_info, sql, e) from e
        return list(tdSql.queryResult)

    @staticmethod
    def _fmt_result_tables(ref_rows, ext_rows, ref_sql, cmp_sql, label):
        """Return a formatted side-by-side diff of *ref_rows* vs *ext_rows*.

        Every row is shown; mismatched cells are marked with ✗ so the
        developer can see at a glance which values differ.
        """
        lines = [
            f"  local_sql  : {ref_sql}",
            f"  {label}_sql    : {cmp_sql}",
            f"  local rows : {len(ref_rows)}  {label} rows: {len(ext_rows)}",
        ]
        n_rows = max(len(ref_rows), len(ext_rows))
        for r in range(n_rows):
            lr = tuple(ref_rows[r]) if r < len(ref_rows) else ()
            er = tuple(ext_rows[r]) if r < len(ext_rows) else ()
            n_cols = max(len(lr), len(er))
            cells = []
            for c in range(n_cols):
                lv = lr[c] if c < len(lr) else "<missing>"
                ev = er[c] if c < len(er) else "<missing>"
                mark = "" if str(lv) == str(ev) else " \u2717"
                cells.append(f"col{c}[local={lv!r} {label}={ev!r}]{mark}")
            lines.append(f"  row[{r:02d}]: " + "  ".join(cells))
        return "\n".join(lines)

    def _compare_rows(self, ref, rows, ref_sql, cmp_sql, label, float_cols):
        """Row-by-row comparison of *ref* (local) vs *rows* (external source).

        On any mismatch shows the FULL side-by-side result table so the
        developer can immediately see which rows and cells diverge.
        """
        if len(ref) != len(rows):
            raise AssertionError(
                f"{label} row count mismatch: local={len(ref)} {label}={len(rows)}\n"
                + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
            )
        for ri, (lr, er) in enumerate(zip(ref, rows)):
            if len(lr) != len(er):
                raise AssertionError(
                    f"{label} col count mismatch at row {ri}: "
                    f"local={len(lr)} {label}={len(er)}\n"
                    + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
                )
            for ci, (lv, ev) in enumerate(zip(lr, er)):
                if ci in float_cols:
                    ok = _float_eq(lv, ev)
                else:
                    ok = (str(lv) == str(ev)) or (lv is None and ev is None)
                if not ok:
                    raise AssertionError(
                        f"{label} value mismatch at row={ri} col={ci}: "
                        f"local={lv!r} {label}={ev!r}\n"
                        + self._fmt_result_tables(ref, rows, ref_sql, cmp_sql, label)
                    )

    def _assert_parity_all(
        self,
        sql_template,
        *,
        float_cols=None,
        ordered=True,
    ):
        """Execute *sql_template* against all four sources and compare results.

        *sql_template* must contain ``{tbl}`` as the table-name placeholder.
        The same template is instantiated for Local TDengine, MySQL, PG and
        InfluxDB; results from each external source are compared row-by-row
        against the local reference.
        """
        float_cols = float_cols or set()
        local_sql = sql_template.format(tbl=self._L)
        ref = self._get_rows(local_sql)
        if not ordered:
            ref = sorted(ref, key=lambda r: [str(x) for x in r])
        for lbl, tbl in [
            ("MySQL",    self._M),
            ("PG",       self._P),
            ("InfluxDB", self._I),
        ]:
            sql = sql_template.format(tbl=tbl)
            rows = self._get_rows(sql)
            if not ordered:
                rows = sorted(rows, key=lambda r: [str(x) for x in r])
            self._compare_rows(ref, rows, local_sql, sql, lbl, float_cols)

    def _run_one_case(self, idx: int, total: int, sql_template: str, **kwargs) -> tuple:
        """Run one parity case and return ``(passed, details, serialized)``.

        Returns a 3-tuple:
          - passed (bool): whether this case passed
          - details (str): one-line failure summary (empty on pass)
          - serialized (str): canonical text block of the local result
            for regression baseline comparison
        """
        positive        = kwargs.get("positive", True)
        reason          = kwargs.get("reason", "")
        float_cols      = kwargs.get("float_cols") or set()
        ordered         = kwargs.get("ordered", True)
        # source_expected: dict[str, list[tuple]] — for sources whose results
        # legitimately differ from local (e.g. type-mapping semantic differences),
        # supply the *exact* expected rows instead of comparing against local.
        # Sources absent from this dict (and not in skip_sources) are compared
        # against local as usual.  skip_sources is still supported for sources
        # where no meaningful assertion is possible at all.
        source_expected = kwargs.get("source_expected") or {}
        skip_sources    = kwargs.get("skip_sources") or set()
        kind_tag   = "POS" if positive else "NEG"
        sql_short  = sql_template if len(sql_template) <= 90 else sql_template[:87] + "..."
        prefix     = f"[{kind_tag} #{idx:03d}/{total}]"
        t0 = time.monotonic()
        if not positive and reason:
            tdLog.info(f"{prefix}  reason: {reason}")

        # ── local reference ──────────────────────────────────────────────────
        local_sql        = sql_template.format(tbl=self._L)
        local_qerr: QueryError | None = None   # non-None when local query errors out
        ref = None
        try:
            ref = self._get_rows(local_sql, no_retry=not positive)
            if not ordered:
                ref = sorted(ref, key=lambda r: [str(x) for x in r])
        except QueryError as exc:
            local_qerr = exc

        # ── serialize local result for baseline comparison ────────────────
        serialized = _serialize_case(idx, total, sql_template, positive, ref, local_qerr, float_cols, ordered)

        # ── negative-case early-fail: if local succeeded but we expected error ─
        if not positive and local_qerr is None:
            elapsed = time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            tdLog.info(f"  [neg-expected] local unexpectedly succeeded (expected error)")
            if reason:
                tdLog.info(f"  reason: {reason}")
            return False, "local unexpectedly succeeded", serialized

        # ── compare each external source ─────────────────────────────────────
        # When local errors we still run all external sources:
        #   • external source also errors with same errno → parity OK
        #   • external source errors with different errno → BUG (wrong error code)
        #   • external source succeeds                   → BUG (should have errored)
        src_failures: list[tuple[str, str]] = []   # (label, full_error_str)
        for lbl, tbl in [("MySQL", self._M), ("PG", self._P), ("InfluxDB", self._I)]:
            if lbl in skip_sources:
                continue
            sql = sql_template.format(tbl=tbl)
            ext_qerr: QueryError | None = None
            rows = None
            try:
                rows = self._get_rows(sql, no_retry=not positive)
                if not ordered:
                    rows = sorted(rows, key=lambda r: [str(x) for x in r])
            except QueryError as exc:
                ext_qerr = exc

            if local_qerr is not None:
                if ext_qerr is None:
                    # External succeeded but local errored — that's a bug.
                    _le = local_qerr.qerrno
                    src_failures.append((
                        lbl,
                        f"BUG: local errored but [{lbl}] succeeded\n"
                        f"  local errno : {_le if _le is not None else 0:#010x} — {local_qerr.err_info}\n"
                        f"  {lbl} sql   : {sql}",
                    ))
                elif ext_qerr.qerrno != local_qerr.qerrno:
                    # Both errored but with different errno — that's a bug.
                    _le = local_qerr.qerrno
                    _ee = ext_qerr.qerrno
                    src_failures.append((
                        lbl,
                        f"BUG: errno mismatch\n"
                        f"  local  errno: {_le if _le is not None else 0:#010x} — {local_qerr.err_info}\n"
                        f"  {lbl}   errno: {_ee if _ee is not None else 0:#010x} — {ext_qerr.err_info}\n"
                        f"  {lbl} sql   : {sql}",
                    ))
                # else: both errored with same errno — parity satisfied
            else:
                # Local succeeded — compare results normally.
                if ext_qerr is not None:
                    src_failures.append((lbl, str(ext_qerr)))
                    continue
                try:
                    if lbl in source_expected:
                        expected_rows = list(source_expected[lbl])
                        self._compare_rows(expected_rows, rows, f"expected({lbl})", sql, lbl, float_cols)
                    else:
                        self._compare_rows(ref, rows, local_sql, sql, lbl, float_cols)
                except AssertionError as exc:
                    src_failures.append((lbl, str(exc)))

        if local_qerr is not None and not src_failures:
            # All sources errored with same errno — parity OK.
            # For positive cases this is unexpected but still consistent (err-parity).
            # For negative cases this is the expected outcome → PASS.
            elapsed = time.monotonic() - t0
            _le = local_qerr.qerrno
            tag = "PASS" if not positive else "PASS(err-parity)"
            tdLog.info(f"{prefix} {tag}  {sql_short}  errno={_le if _le is not None else 0:#010x}  [{elapsed:.2f}s]")
            return True, "", serialized

        if local_qerr is not None and src_failures:
            # Some external sources had wrong errno or succeeded — bug.
            elapsed = time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            if not positive and reason:
                tdLog.info(f"  [neg-expected] {reason}")
            _le = local_qerr.qerrno
            tdLog.info(f"  [local] errno={_le if _le is not None else 0:#010x} — {local_qerr.err_info}")
            for lbl, err in src_failures:
                for line in err.splitlines()[:5]:
                    tdLog.info(f"  {line}")
            summary = "; ".join(f"[{lbl}] {err.splitlines()[0]}" for lbl, err in src_failures)
            return False, summary, serialized

        if src_failures:
            elapsed = time.monotonic() - t0
            tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
            if not positive and reason:
                tdLog.info(f"  [neg-expected] {reason}")
            for lbl, err in src_failures:
                err_lines = err.split("\n")
                tdLog.info(f"  [{lbl}] {err_lines[0]}")
                for line in err_lines[1:10]:        # up to 9 detail lines
                    tdLog.info(f"    {line}")
            summary = "; ".join(
                f"[{lbl}] {err.split(chr(10))[0]}" for lbl, err in src_failures
            )
            return False, summary, serialized

        elapsed = time.monotonic() - t0
        tdLog.info(f"{prefix} PASS  {sql_short}  [{elapsed:.2f}s]")
        return True, "", serialized

    def test_fq_parity_all_cases(self):
        """All result-parity cases (parametrized via _PARITY_CASES).

        By default every entry in _PARITY_CASES is executed.  Set the
        environment variable ``PARITY_IDX`` to a comma-separated list of
        1-based indices to run only those entries, e.g.::

            PARITY_IDX=1 pytest test_fq_14_result_parity.py::...::test_fq_parity_all_cases
            PARITY_IDX=1,3,5-8 pytest ...

        Ranges (``a-b``) are inclusive on both ends.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """

        raw = os.environ.get("PARITY_IDX", "").strip()
        if raw:
            selected: set[int] = set()
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    lo, hi = part.split("-", 1)
                    selected.update(range(int(lo), int(hi) + 1))
                else:
                    selected.add(int(part))
            indices = sorted(selected)
        else:
            indices = list(range(1, len(_PARITY_CASES) + 1))

        total  = len(_PARITY_CASES)
        n_run  = len(indices)
        n_pos  = sum(1 for i in indices if (_PARITY_CASES[i - 1][1] if len(_PARITY_CASES[i - 1]) > 1 else {}).get("positive", True))
        n_neg  = n_run - n_pos
        tdLog.info(f"\nParity run: {n_run} case(s) of {total} total  (pos={n_pos} neg={n_neg})")

        failed: list[tuple[int, str, str]] = []   # (idx, sql_template, details)
        serialized_blocks: list[str] = []          # per-case baseline text

        for idx in indices:
            if idx < 1 or idx > total:
                raise ValueError(f"PARITY_IDX {idx} out of range 1..{total}")
            entry        = _PARITY_CASES[idx - 1]
            sql_template = entry[0]
            kwargs       = entry[1] if len(entry) > 1 else {}
            passed, details, serialized = self._run_one_case(idx, total, sql_template, **kwargs)
            serialized_blocks.append(serialized)
            if not passed:
                failed.append((idx, sql_template, details))

        # ── write temp result file and compare with static baseline ────────
        run_all = (not raw) or (set(indices) == set(range(1, total + 1)))
        if run_all:
            tmp_file = _BASELINE_FILE + ".tmp"
            tmp_content = "\n".join(serialized_blocks) + "\n"
            with open(tmp_file, "w") as f:
                f.write(tmp_content)
            tdLog.info(f"Temp result file written: {tmp_file}")

            if os.path.isfile(_BASELINE_FILE):
                with open(_BASELINE_FILE, "r") as f:
                    baseline_content = f.read()
                if tmp_content != baseline_content:
                    # Find first diff line for diagnostic
                    tmp_lines = tmp_content.splitlines()
                    base_lines = baseline_content.splitlines()
                    diff_line = -1
                    for li in range(max(len(tmp_lines), len(base_lines))):
                        tl = tmp_lines[li] if li < len(tmp_lines) else "<EOF>"
                        bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                        if tl != bl:
                            diff_line = li + 1
                            break
                    baseline_err = (
                        f"Regression baseline mismatch!\n"
                        f"  baseline: {_BASELINE_FILE}\n"
                        f"  actual  : {tmp_file}\n"
                        f"  first diff at line {diff_line}:\n"
                        f"    baseline: {bl!r}\n"
                        f"    actual  : {tl!r}\n"
                        f"  Run: diff {_BASELINE_FILE} {tmp_file}"
                    )
                    tdLog.info(f"BASELINE MISMATCH: {baseline_err}")
                    failed.append((0, "<baseline>", baseline_err))
                else:
                    tdLog.info("Baseline comparison: OK (matches static baseline)")
            else:
                tdLog.info(f"No baseline file found at {_BASELINE_FILE}, skipping baseline comparison")

        # ── summary ────────────────────────────────────────────────────────
        n_pass = n_run - len(failed)
        sep    = "─" * 72
        tdLog.info(f"\n{sep}")
        tdLog.info(f"  Parity summary: {n_pass}/{n_run} passed  |  {len(failed)} failed  (pos={n_pos} neg={n_neg})")
        if failed:
            tdLog.info("  Failed cases:")
            for i, sql, det in failed:
                opts     = _PARITY_CASES[i - 1][1] if i > 0 and len(_PARITY_CASES[i - 1]) > 1 else {}
                kind_tag = "POS" if opts.get("positive", True) else "NEG"
                tdLog.info(f"    [{kind_tag} #{i:03d}]  {sql[:70]}")
                tdLog.info(f"           {det[:130]}")
        tdLog.info(sep)

        # ── cleanup temp file: keep on failure, remove on success ─────────
        if run_all:
            tmp_file_path = _BASELINE_FILE + ".tmp"
            if failed:
                tdLog.info(f"Temp result file kept for debugging: {tmp_file_path}")
            elif os.path.isfile(tmp_file_path):
                os.remove(tmp_file_path)
                tdLog.info(f"Temp result file removed (all passed).")

        if failed:
            all_errors = "\n".join(
                f"\n[#{i:03d}] {sql}\n  {det}" for i, sql, det in failed
            )
            raise AssertionError(
                f"{len(failed)} of {n_run} case(s) failed:\n{all_errors}"
            )
