"""
test_fq_16_multi_table_parity.py

Multi-table result-parity and cross-source query tests for federated query.

Consolidates multi-table cases from test_fq_04 (UNION, subquery, JOINs),
test_fq_05 (IN/NOT IN, EXISTS, ANY/SOME/ALL, ASOF/WINDOW JOIN, scalar subquery),
and test_fq_06 (TS/non-TS JOIN, FULL OUTER JOIN, semi/anti JOIN) into a single
unified test file.

Schema (two tables in each data source):
  - ta:  ts TIMESTAMP, id INT, val INT, label NCHAR(16)
  - tb:  ts TIMESTAMP, bid INT, ref_id INT, val2 INT, flag INT
  - ref_t (local only): ts TIMESTAMP, val INT  -- for cross-source subqueries
  - empty_t (local only): ts TIMESTAMP, val INT  -- for NOT EXISTS tests

Data:
  ta (6 rows): 1-minute intervals from 2024-01-01 00:00:00 UTC
    (00:00, 1, 10, 'a'), (00:01, 2, 20, 'b'), (00:02, 3, 30, 'a'),
    (00:03, 4, 40, 'b'), (00:04, 5, 50, 'a'), (00:05, 6, 10, 'b')

  tb (5 rows): partial ts overlap with ta
    (00:00, 1, 1, 15, 1), (00:01, 2, 2, 25, 0), (00:02, 3, 1, 30, 1),
    (00:05, 4, 3, 45, 1), (00:06, 5, 5, 50, 0)
    Note: ts 00:03, 00:04 missing in tb -> LEFT JOIN shows NULLs
          ts 00:06 only in tb -> FULL OUTER JOIN has 7 rows

  ref_t (2 rows, local only): (00:00, 1), (00:01, 3)
  empty_t (0 rows, local only): intentionally empty

Environment:
  Enterprise edition, federatedQueryEnable=1
  MySQL 8.0+, PostgreSQL 14+, InfluxDB v3
"""

import os
import shutil
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
    parity_serialize_case,
    parity_serialize_cell,
    TSDB_CODE_PAR_INVALID_COLUMN,
    TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
    TSDB_CODE_PAR_INVALID_EXPR_SUBQ,
)


# -- Source & DB names ---------------------------------------------------------
_MYSQL_DB      = "fq_mt_parity_m"
_PG_DB         = "fq_mt_parity_p"
_INFLUX_BUCKET = "fq_mt_parity_i"
_LOCAL_DB      = "fq_mt_parity_local"
_FLOAT_TOL     = 1e-4

# -- Timestamps ----------------------------------------------------------------
_TS_BASE_MS = 1704067200000             # 2024-01-01 00:00:00 UTC in ms
_M = 60_000                             # 1 minute in ms

# -- Data rows -----------------------------------------------------------------
# ta: (ts_ms, id, val, label)
_TA_ROWS = [
    (_TS_BASE_MS + 0 * _M, 1, 10, 'a'),
    (_TS_BASE_MS + 1 * _M, 2, 20, 'b'),
    (_TS_BASE_MS + 2 * _M, 3, 30, 'a'),
    (_TS_BASE_MS + 3 * _M, 4, 40, 'b'),
    (_TS_BASE_MS + 4 * _M, 5, 50, 'a'),
    (_TS_BASE_MS + 5 * _M, 6, 10, 'b'),
]

# tb: (ts_ms, bid, ref_id, val2, flag)
_TB_ROWS = [
    (_TS_BASE_MS + 0 * _M, 1, 1, 15, 1),
    (_TS_BASE_MS + 1 * _M, 2, 2, 25, 0),
    (_TS_BASE_MS + 2 * _M, 3, 1, 30, 1),
    (_TS_BASE_MS + 5 * _M, 4, 3, 45, 1),
    (_TS_BASE_MS + 6 * _M, 5, 5, 50, 0),
]

# ref_t: (ts_ms, val) -- local only, for cross-source subqueries
_REF_ROWS = [
    (_TS_BASE_MS + 0 * _M, 1),
    (_TS_BASE_MS + 1 * _M, 3),
]

# Datetime strings for MySQL/PG
_TA_ROWS_DT = [
    ('2024-01-01 00:00:00.000', 1, 10, 'a'),
    ('2024-01-01 00:01:00.000', 2, 20, 'b'),
    ('2024-01-01 00:02:00.000', 3, 30, 'a'),
    ('2024-01-01 00:03:00.000', 4, 40, 'b'),
    ('2024-01-01 00:04:00.000', 5, 50, 'a'),
    ('2024-01-01 00:05:00.000', 6, 10, 'b'),
]

_TB_ROWS_DT = [
    ('2024-01-01 00:00:00.000', 1, 1, 15, 1),
    ('2024-01-01 00:01:00.000', 2, 2, 25, 0),
    ('2024-01-01 00:02:00.000', 3, 1, 30, 1),
    ('2024-01-01 00:05:00.000', 4, 3, 45, 1),
    ('2024-01-01 00:06:00.000', 5, 5, 50, 0),
]

# -- Disorder indices ----------------------------------------------------------
_TA_DISORDER_IDX = [3, 0, 5, 2, 4, 1]
_TB_DISORDER_IDX = [2, 4, 0, 3, 1]


# ==============================================================================
# Parity case registry (same-source multi-table queries)
# ==============================================================================
#
# SQL templates use {tbl} as the source/database prefix.
#   Local:    {tbl} = "fq_mt_parity_local"  -> "{tbl}.ta" = "fq_mt_parity_local.ta"
#   External: {tbl} = "fq_mt_parity_src_m"  -> "{tbl}.ta" = "fq_mt_parity_src_m.ta"

_PARITY_GROUPS: dict[str, list] = {
    # -- xjoin: Cross-table JOINs (ts-based equality) -------------------------
    "xjoin": [
        # 01: INNER JOIN on ts -> 4 rows
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.time = b.time "
         "ORDER BY a.time",),
        # 02: LEFT JOIN on ts -> 6 rows (NULL tb at 00:03, 00:04)
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a LEFT JOIN {tbl}.tb b ON a.time = b.time "
         "ORDER BY a.time",),
        # 03: FULL OUTER JOIN on ts -> 7 rows (unordered comparison)
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a FULL JOIN {tbl}.tb b ON a.time = b.time",
         {"ordered": False}),
        # 04: LEFT ASOF JOIN -> 6 rows
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT ASOF JOIN {tbl}.tb b ON a.time >= b.time "
         "ORDER BY a.time",),
        # 05: LEFT ASOF JOIN JLIMIT 1 -> 6 rows
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT ASOF JOIN {tbl}.tb b ON a.time >= b.time JLIMIT 1 "
         "ORDER BY a.time",),
        # 06: RIGHT ASOF JOIN -> 5 rows
        ("SELECT a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a RIGHT ASOF JOIN {tbl}.tb b ON b.time >= a.time "
         "ORDER BY b.time",),
        # 07: LEFT WINDOW JOIN (-1m, 1m)
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT WINDOW JOIN {tbl}.tb b WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY a.time, b.time",),
        # 08: LEFT WINDOW JOIN (-1m, 1m) JLIMIT 1
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT WINDOW JOIN {tbl}.tb b "
         "WINDOW_OFFSET(-1m, 1m) JLIMIT 1 "
         "ORDER BY a.time",),
        # 09: RIGHT WINDOW JOIN (-1m, 1m)
        ("SELECT a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a RIGHT WINDOW JOIN {tbl}.tb b "
         "WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY b.time, a.time",),
    ],
    # -- xsub: Cross-table subqueries -----------------------------------------
    "xsub": [
        # 01: IN subquery -> 4 rows (id in {1,2,3,5})
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE id IN (SELECT ref_id FROM {tbl}.tb) ORDER BY id",),
        # 02: NOT IN subquery -> 2 rows (id in {4,6})
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE id NOT IN (SELECT ref_id FROM {tbl}.tb) ORDER BY id",),
        # 03: > ANY (flag=1 -> val2 in {15,30,45}, min=15) -> 4 rows (val>15)
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE val > ANY (SELECT val2 FROM {tbl}.tb WHERE flag = 1) "
         "ORDER BY id",),
        # 04: > ALL (flag=1 -> max=45) -> 1 row (val=50)
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE val > ALL (SELECT val2 FROM {tbl}.tb WHERE flag = 1) "
         "ORDER BY id",),
        # 05: = SOME -> 2 rows (val in {15,25,30,45,50} -> val=30,50)
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE val = SOME (SELECT val2 FROM {tbl}.tb) ORDER BY id",),
        # 06: Scalar subquery: val > MAX(val2 WHERE flag=1)=45 -> 1 row
        ("SELECT id, val FROM {tbl}.ta "
         "WHERE val > (SELECT MAX(val2) FROM {tbl}.tb WHERE flag = 1) "
         "ORDER BY id",),
    ],
    # -- xunion: Cross-table UNION ---------------------------------------------
    "xunion": [
        # 01: UNION ALL -> 11 rows (6 + 5)
        ("SELECT id, val FROM {tbl}.ta "
         "UNION ALL "
         "SELECT bid, val2 FROM {tbl}.tb "
         "ORDER BY 1, 2",),
        # 02: UNION dedup -> 9 rows (overlap: (3,30) and (5,50))
        ("SELECT id, val FROM {tbl}.ta "
         "UNION "
         "SELECT bid, val2 FROM {tbl}.tb "
         "ORDER BY 1, 2",),
    ],
    # -- xagg: Cross-table aggregation (JOIN + GROUP BY) -----------------------
    "xagg": [
        # 01: JOIN + COUNT + GROUP BY label
        ("SELECT a.label, COUNT(*) AS cnt "
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.time = b.time "
         "GROUP BY a.label ORDER BY a.label",),
        # 02: JOIN + SUM(val2) + GROUP BY label
        ("SELECT a.label, SUM(b.val2) AS total "
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.time = b.time "
         "GROUP BY a.label ORDER BY a.label",),
    ],
}

_PARITY_CASES: list[tuple[str, str, dict]] = [
    (f"{grp}-{i:02d}", entry[0], entry[1] if len(entry) > 1 else {})
    for grp, entries in _PARITY_GROUPS.items()
    for i, entry in enumerate(entries, 1)
]


# ==============================================================================
# Cross-source and negative test cases
# ==============================================================================
#
# SQL templates use:
#   {M} = MySQL external source   {P} = PG external source
#   {I} = InfluxDB external source {L} = local TDengine database
#
# Each entry: (case_id, sql_template, opts)
#   opts["rows"]   : expected row count (positive cases)
#   opts["data"]   : [(row, col, val), ...] optional spot-check values
#   opts["ordered"]: whether result order matters (default True)
#   opts["error"]  : expected errno (negative cases)
#
# For negative cases that apply to each source individually, use {src}
# placeholder and "per_source": True — the runner expands to M/P/I.

_CROSS_CASES: list[tuple[str, str, dict]] = [
    # ── Cross-source UNION ────────────────────────────────────────────────
    # xsrc-01: MySQL + PG UNION ALL -> 12 rows (6+6, identical data)
    ("xsrc-01",
     "SELECT id, val FROM {M}.ta "
     "UNION ALL "
     "SELECT id, val FROM {P}.ta "
     "ORDER BY 1, 2",
     {"rows": 12}),
    # xsrc-02: MySQL + InfluxDB UNION ALL -> 12 rows
    ("xsrc-02",
     "SELECT id, val FROM {M}.ta "
     "UNION ALL "
     "SELECT id, val FROM {I}.ta "
     "ORDER BY 1, 2",
     {"rows": 12}),
    # xsrc-03: PG + InfluxDB UNION ALL -> 12 rows
    ("xsrc-03",
     "SELECT id, val FROM {P}.ta "
     "UNION ALL "
     "SELECT id, val FROM {I}.ta "
     "ORDER BY 1, 2",
     {"rows": 12}),
    # xsrc-04: MySQL + PG UNION (dedup) -> 6 rows
    ("xsrc-04",
     "SELECT id, val FROM {M}.ta "
     "UNION "
     "SELECT id, val FROM {P}.ta "
     "ORDER BY 1, 2",
     {"rows": 6}),
    # xsrc-05: MySQL + InfluxDB UNION -> 6 rows
    ("xsrc-05",
     "SELECT id, val FROM {M}.ta "
     "UNION "
     "SELECT id, val FROM {I}.ta "
     "ORDER BY 1, 2",
     {"rows": 6}),
    # xsrc-06: 3-source UNION ALL -> 18 rows (6x3)
    ("xsrc-06",
     "SELECT id, val FROM {M}.ta "
     "UNION ALL "
     "SELECT id, val FROM {P}.ta "
     "UNION ALL "
     "SELECT id, val FROM {I}.ta "
     "ORDER BY 1, 2",
     {"rows": 18}),
    # xsrc-07: 4-source UNION ALL -> 24 rows (6x4)
    ("xsrc-07",
     "SELECT id, val FROM {M}.ta "
     "UNION ALL "
     "SELECT id, val FROM {P}.ta "
     "UNION ALL "
     "SELECT id, val FROM {I}.ta "
     "UNION ALL "
     "SELECT id, val FROM {L}.ta "
     "ORDER BY 1, 2",
     {"rows": 24}),

    # ── Cross-source IN subquery ──────────────────────────────────────────
    # xsrc-08: MySQL outer WHERE id IN (PG subquery) -> 4 rows
    ("xsrc-08",
     "SELECT id, val FROM {M}.ta "
     "WHERE id IN (SELECT ref_id FROM {P}.tb) "
     "ORDER BY id",
     {"rows": 4}),
    # xsrc-09: MySQL outer WHERE id IN (local ref_t) -> 2 rows
    # ref_t.val = {1, 3} → ta 中 id IN (1,3) → 2行
    ("xsrc-09",
     "SELECT id, val FROM {M}.ta "
     "WHERE id IN (SELECT val FROM {L}.ref_t) "
     "ORDER BY id",
     {"rows": 2,
      "data": [(0, 0, 1), (0, 1, 10), (1, 0, 3), (1, 1, 30)]}),
    # xsrc-10: InfluxDB outer WHERE id IN (local ref_t) -> 2 rows
    ("xsrc-10",
     "SELECT id, val FROM {I}.ta "
     "WHERE id IN (SELECT val FROM {L}.ref_t) "
     "ORDER BY time",
     {"rows": 2,
      "data": [(0, 0, 1), (0, 1, 10), (1, 0, 3), (1, 1, 30)]}),
    # xsrc-11: MySQL outer WHERE id NOT IN (local ref_t) -> 4 rows
    ("xsrc-11",
     "SELECT id, val FROM {M}.ta "
     "WHERE id NOT IN (SELECT val FROM {L}.ref_t) "
     "ORDER BY id",
     {"rows": 4}),

    # ── Cross-source EXISTS (non-correlated) ──────────────────────────────
    # xsrc-12: MySQL outer + local EXISTS -> 6 rows
    ("xsrc-12",
     "SELECT id FROM {M}.ta "
     "WHERE EXISTS (SELECT 1 FROM {L}.ref_t WHERE val = 1) "
     "ORDER BY id",
     {"rows": 6}),
    # xsrc-13: PG outer + local NOT EXISTS (empty_t) -> 6 rows
    ("xsrc-13",
     "SELECT id FROM {P}.ta "
     "WHERE NOT EXISTS (SELECT 1 FROM {L}.empty_t) "
     "ORDER BY id",
     {"rows": 6}),
    # xsrc-14: InfluxDB outer + local EXISTS -> 6 rows
    ("xsrc-14",
     "SELECT id FROM {I}.ta "
     "WHERE EXISTS (SELECT 1 FROM {L}.ref_t WHERE val = 1) "
     "ORDER BY time",
     {"rows": 6}),

    # ── Cross-source scalar subquery ──────────────────────────────────────
    # xsrc-15: InfluxDB outer WHERE val > MAX(val2 from PG WHERE flag=1) -> 1 row
    ("xsrc-15",
     "SELECT id, val FROM {I}.ta "
     "WHERE val > (SELECT MAX(val2) FROM {P}.tb WHERE flag = 1) "
     "ORDER BY time",
     {"rows": 1,
      "data": [(0, 0, 5), (0, 1, 50)]}),

    # ── Cross-source ANY/ALL ──────────────────────────────────────────────
    # xsrc-16: MySQL outer WHERE val > ANY (PG.tb val2 where flag=1) -> 4 rows
    ("xsrc-16",
     "SELECT id, val FROM {M}.ta "
     "WHERE val > ANY (SELECT val2 FROM {P}.tb WHERE flag = 1) "
     "ORDER BY id",
     {"rows": 4}),
    # xsrc-17: InfluxDB outer WHERE val > ALL (local ref_t) -> 6 rows
    ("xsrc-17",
     "SELECT id, val FROM {I}.ta "
     "WHERE val > ALL (SELECT val FROM {L}.ref_t) "
     "ORDER BY time",
     {"rows": 6}),

    # ── Cross-source TS-PK JOIN ───────────────────────────────────────────
    # xsrc-18: MySQL.ta JOIN PG.tb ON ts -> 4 rows
    ("xsrc-18",
     "SELECT a.id, a.val, b.bid, b.val2 "
     "FROM {M}.ta a INNER JOIN {P}.tb b ON a.time = b.time "
     "ORDER BY a.time",
     {"rows": 4}),
    # xsrc-19: MySQL.ta JOIN InfluxDB.tb ON ts -> 4 rows
    ("xsrc-19",
     "SELECT a.id, a.val, b.bid, b.val2 "
     "FROM {M}.ta a INNER JOIN {I}.tb b ON a.time = b.time "
     "ORDER BY a.time",
     {"rows": 4}),

    # ── Cross-source ASOF/WINDOW JOIN ─────────────────────────────────────
    # xsrc-20: LEFT ASOF JOIN (MySQL.ta × PG.tb) -> 6 rows
    ("xsrc-20",
     "SELECT a.id, a.val, b.val2 "
     "FROM {M}.ta a LEFT ASOF JOIN {P}.tb b ON a.time >= b.time "
     "ORDER BY a.time",
     {"rows": 6,
      "data": [(0, 0, 1), (0, 1, 10), (0, 2, 15),
               (5, 0, 6), (5, 1, 10), (5, 2, 45)]}),
    # xsrc-21: LEFT WINDOW JOIN (MySQL.ta × InfluxDB.tb, ±1m) -> 11 rows
    ("xsrc-21",
     "SELECT a.id, a.val, b.val2 "
     "FROM {M}.ta a LEFT WINDOW JOIN {I}.tb b WINDOW_OFFSET(-1m, 1m) "
     "ORDER BY a.time, b.time",
     {"rows": 11}),
    # xsrc-22: FULL OUTER JOIN (PG.ta × InfluxDB.tb ON ts) -> 7 rows
    ("xsrc-22",
     "SELECT a.id, a.val, b.bid, b.val2 "
     "FROM {P}.ta a FULL JOIN {I}.tb b ON a.time = b.time",
     {"rows": 7, "ordered": False}),
    # xsrc-23: PG outer WHERE val > (MySQL.tb MAX scalar) -> 1 row
    ("xsrc-23",
     "SELECT id, val FROM {P}.ta "
     "WHERE val > (SELECT MAX(val2) FROM {M}.tb WHERE flag = 1) "
     "ORDER BY id",
     {"rows": 1,
      "data": [(0, 0, 5), (0, 1, 50)]}),
    # xsrc-24: RIGHT ASOF JOIN (InfluxDB.ta × PG.tb) -> 5 rows
    ("xsrc-24",
     "SELECT a.val, b.bid, b.val2 "
     "FROM {I}.ta a RIGHT ASOF JOIN {P}.tb b ON b.time >= a.time "
     "ORDER BY b.time",
     {"rows": 5,
      "data": [(0, 0, 10), (0, 1, 1), (0, 2, 15)]}),

        # ── Negative: InfluxDB uses time as primary timestamp column ──────────
        ("xsrc-25-neg",
         "SELECT a.id, a.val, b.val2 "
         "FROM {M}.ta a LEFT WINDOW JOIN {I}.tb b WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY a.time, b.ts",
         {"error": TSDB_CODE_PAR_INVALID_COLUMN}),
        ("xsrc-26-neg",
         "SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {P}.ta a FULL JOIN {I}.tb b ON a.time = b.ts",
         {"error": TSDB_CODE_PAR_INVALID_COLUMN}),
        ("xsrc-27-neg",
         "SELECT a.val, b.bid, b.val2 "
         "FROM {I}.ta a RIGHT ASOF JOIN {P}.tb b ON b.time >= a.ts "
         "ORDER BY b.time",
         {"error": TSDB_CODE_PAR_INVALID_COLUMN}),

    # ── Negative: unsupported JOIN types (per source) ─────────────────────
    # neg-01 ~ neg-06: Non-TS JOIN types on each external source
    # MySQL
    ("neg-m-01",
     "SELECT a.id, b.bid FROM {M}.ta a "
     "JOIN {M}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-m-02",
     "SELECT a.id, b.bid FROM {M}.ta a "
     "FULL JOIN {M}.tb b ON a.id = b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-m-03",
     "SELECT a.id FROM {M}.ta a "
     "LEFT SEMI JOIN {M}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-m-04",
     "SELECT a.id FROM {M}.ta a "
     "LEFT ANTI JOIN {M}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-m-05",
     "SELECT b.bid FROM {M}.ta a "
     "RIGHT SEMI JOIN {M}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-m-06",
     "SELECT b.bid FROM {M}.ta a "
     "RIGHT ANTI JOIN {M}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    # PostgreSQL
    ("neg-p-01",
     "SELECT a.id, b.bid FROM {P}.ta a "
     "JOIN {P}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-p-02",
     "SELECT a.id, b.bid FROM {P}.ta a "
     "FULL JOIN {P}.tb b ON a.id = b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-p-03",
     "SELECT a.id FROM {P}.ta a "
     "LEFT SEMI JOIN {P}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-p-04",
     "SELECT a.id FROM {P}.ta a "
     "LEFT ANTI JOIN {P}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-p-05",
     "SELECT b.bid FROM {P}.ta a "
     "RIGHT SEMI JOIN {P}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-p-06",
     "SELECT b.bid FROM {P}.ta a "
     "RIGHT ANTI JOIN {P}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    # InfluxDB
    ("neg-i-01",
     "SELECT a.id, b.bid FROM {I}.ta a "
     "JOIN {I}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-i-02",
     "SELECT a.id, b.bid FROM {I}.ta a "
     "FULL JOIN {I}.tb b ON a.id = b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-i-03",
     "SELECT a.id FROM {I}.ta a "
     "LEFT SEMI JOIN {I}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-i-04",
     "SELECT a.id FROM {I}.ta a "
     "LEFT ANTI JOIN {I}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-i-05",
     "SELECT b.bid FROM {I}.ta a "
     "RIGHT SEMI JOIN {I}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),
    ("neg-i-06",
     "SELECT b.bid FROM {I}.ta a "
     "RIGHT ANTI JOIN {I}.tb b ON a.id = b.bid ORDER BY b.bid",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),

    # Negative: cross-source non-TS JOIN
    ("neg-x-01",
     "SELECT a.id FROM {M}.ta a "
     "JOIN {P}.tb b ON a.id = b.bid ORDER BY a.id",
     {"error": TSDB_CODE_PAR_NOT_SUPPORT_JOIN}),

    # Negative: correlated subqueries on external sources
    ("neg-csub-m-01",
     "SELECT id FROM {M}.ta a "
     "WHERE EXISTS "
     "(SELECT 1 FROM {M}.tb b WHERE b.ref_id = a.id) "
     "ORDER BY id",
     {"error": TSDB_CODE_PAR_INVALID_EXPR_SUBQ}),
    ("neg-csub-m-02",
     "SELECT id FROM {M}.ta a "
     "WHERE NOT EXISTS "
     "(SELECT 1 FROM {M}.tb b WHERE b.ref_id = a.id) "
     "ORDER BY id",
     {"error": TSDB_CODE_PAR_INVALID_EXPR_SUBQ}),
    ("neg-csub-p-01",
     "SELECT id FROM {P}.ta a "
     "WHERE EXISTS "
     "(SELECT 1 FROM {P}.tb b WHERE b.ref_id = a.id) "
     "ORDER BY id",
     {"error": TSDB_CODE_PAR_INVALID_EXPR_SUBQ}),
    ("neg-csub-p-02",
     "SELECT id FROM {P}.ta a "
     "WHERE NOT EXISTS "
     "(SELECT 1 FROM {P}.tb b WHERE b.ref_id = a.id) "
     "ORDER BY id",
     {"error": TSDB_CODE_PAR_INVALID_EXPR_SUBQ}),
]


# ==============================================================================
# External DB setup SQL builders
# ==============================================================================

def _build_mysql_setup():
    sqls = [
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        ("CREATE TABLE ta "
         "(time DATETIME(3) PRIMARY KEY, id INT, val INT, label VARCHAR(16))"),
        ("CREATE TABLE tb "
         "(time DATETIME(3) PRIMARY KEY, bid INT, ref_id INT, val2 INT, flag INT)"),
    ]
    vals = ", ".join(
        f"('{dt}', {id_}, {val}, '{label}')"
        for dt, id_, val, label in _TA_ROWS_DT)
    sqls.append(f"INSERT INTO ta VALUES {vals}")
    vals = ", ".join(
        f"('{dt}', {bid}, {ref_id}, {val2}, {flag})"
        for dt, bid, ref_id, val2, flag in _TB_ROWS_DT)
    sqls.append(f"INSERT INTO tb VALUES {vals}")
    return sqls


def _build_pg_setup():
    sqls = [
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        ("CREATE TABLE ta "
         "(time TIMESTAMP PRIMARY KEY, id INT, val INT, label TEXT)"),
        ("CREATE TABLE tb "
         "(time TIMESTAMP PRIMARY KEY, bid INT, ref_id INT, val2 INT, flag INT)"),
    ]
    vals = ", ".join(
        f"('{dt}', {id_}, {val}, '{label}')"
        for dt, id_, val, label in _TA_ROWS_DT)
    sqls.append(f"INSERT INTO ta VALUES {vals}")
    vals = ", ".join(
        f"('{dt}', {bid}, {ref_id}, {val2}, {flag})"
        for dt, bid, ref_id, val2, flag in _TB_ROWS_DT)
    sqls.append(f"INSERT INTO tb VALUES {vals}")
    return sqls


def _build_influx_lines():
    lines = []
    for ts_ms, id_, val, label in _TA_ROWS:
        ns = ts_ms * 1_000_000
        lines.append(f"ta,label={label} id={id_}i,val={val}i {ns}")
    for ts_ms, bid, ref_id, val2, flag in _TB_ROWS:
        ns = ts_ms * 1_000_000
        lines.append(
            f"tb bid={bid}i,ref_id={ref_id}i,val2={val2}i,flag={flag}i {ns}")
    return lines


def _build_local_setup():
    sqls = [
        f"DROP DATABASE IF EXISTS {_LOCAL_DB}",
        f"CREATE DATABASE {_LOCAL_DB}",
        f"USE {_LOCAL_DB}",
        "CREATE TABLE ta (time TIMESTAMP, id INT, val INT, label NCHAR(16))",
        "CREATE TABLE tb (time TIMESTAMP, bid INT, ref_id INT, val2 INT, flag INT)",
        "CREATE TABLE ref_t (time TIMESTAMP, val INT)",
        "CREATE TABLE empty_t (time TIMESTAMP, val INT)",
    ]
    vals = " ".join(
        f"({ts}, {id_}, {val}, '{label}')"
        for ts, id_, val, label in _TA_ROWS)
    sqls.append(f"INSERT INTO ta VALUES {vals}")
    vals = " ".join(
        f"({ts}, {bid}, {ref_id}, {val2}, {flag})"
        for ts, bid, ref_id, val2, flag in _TB_ROWS)
    sqls.append(f"INSERT INTO tb VALUES {vals}")
    vals = " ".join(f"({ts}, {val})" for ts, val in _REF_ROWS)
    sqls.append(f"INSERT INTO ref_t VALUES {vals}")
    return sqls


# -- Disorder helpers ----------------------------------------------------------

def _build_mysql_disorder():
    sqls = ["DELETE FROM ta", "DELETE FROM tb"]
    for i in _TA_DISORDER_IDX:
        dt, id_, val, label = _TA_ROWS_DT[i]
        sqls.append(f"INSERT INTO ta VALUES ('{dt}', {id_}, {val}, '{label}')")
    for i in _TB_DISORDER_IDX:
        dt, bid, ref_id, val2, flag = _TB_ROWS_DT[i]
        sqls.append(
            f"INSERT INTO tb VALUES ('{dt}', {bid}, {ref_id}, {val2}, {flag})")
    return sqls


def _build_pg_disorder():
    sqls = ["DELETE FROM ta", "DELETE FROM tb"]
    for i in _TA_DISORDER_IDX:
        dt, id_, val, label = _TA_ROWS_DT[i]
        sqls.append(f"INSERT INTO ta VALUES ('{dt}', {id_}, {val}, '{label}')")
    for i in _TB_DISORDER_IDX:
        dt, bid, ref_id, val2, flag = _TB_ROWS_DT[i]
        sqls.append(
            f"INSERT INTO tb VALUES ('{dt}', {bid}, {ref_id}, {val2}, {flag})")
    return sqls


def _build_influx_disorder_lines():
    lines = []
    for i in _TA_DISORDER_IDX:
        ts_ms, id_, val, label = _TA_ROWS[i]
        ns = ts_ms * 1_000_000
        lines.append(f"ta,label={label} id={id_}i,val={val}i {ns}")
    for i in _TB_DISORDER_IDX:
        ts_ms, bid, ref_id, val2, flag = _TB_ROWS[i]
        ns = ts_ms * 1_000_000
        lines.append(
            f"tb bid={bid}i,ref_id={ref_id}i,val2={val2}i,flag={flag}i {ns}")
    return lines


def _build_local_disorder():
    sqls = [
        f"DELETE FROM {_LOCAL_DB}.ta WHERE time >= 0",
        f"DELETE FROM {_LOCAL_DB}.tb WHERE time >= 0",
    ]
    for i in _TA_DISORDER_IDX:
        ts, id_, val, label = _TA_ROWS[i]
        sqls.append(
            f"INSERT INTO {_LOCAL_DB}.ta VALUES ({ts}, {id_}, {val}, '{label}')")
    for i in _TB_DISORDER_IDX:
        ts, bid, ref_id, val2, flag = _TB_ROWS[i]
        sqls.append(
            f"INSERT INTO {_LOCAL_DB}.tb VALUES "
            f"({ts}, {bid}, {ref_id}, {val2}, {flag})")
    return sqls


# ==============================================================================
# Cross-case serialization helpers
# ==============================================================================

def _cross_serialize_case(case_id, sql, positive, rows, qerr, float_cols, ordered):
    """Serialize one cross-source case result to a text block for baseline comparison."""
    kind_tag = "POS" if positive else "NEG"
    lines = [f"### {case_id} {kind_tag}", f"SQL: {sql}"]
    if qerr is not None:
        errno = qerr.qerrno
        err_info = qerr.err_info or ""
        lines.append(f"ERROR {errno if errno is not None else 0:#010x}: {err_info}")
    else:
        lines.append("RESULT")
        for row in rows:
            cells = [parity_serialize_cell(v, ci, float_cols) for ci, v in enumerate(row)]
            lines.append("|".join(cells))
    lines.append("---")
    return "\n".join(lines)


# ==============================================================================
# Test class
# ==============================================================================

class TestFq16MultiTableParity(ParityTestBase):
    """Multi-table result-parity and cross-source query tests.

    Exercises cross-table operations (JOINs, subqueries, UNIONs, aggregation)
    across all four data sources, plus cross-source and negative test cases.

    Migrated from:
      - test_fq_04: UNION cross-source, cross-source subquery, JOIN types,
                     ASOF JOIN, WINDOW JOIN
      - test_fq_05: IN/NOT IN, EXISTS/NOT EXISTS, ANY/SOME/ALL, ASOF JOIN,
                     WINDOW JOIN, scalar subquery, cross-source UNION
      - test_fq_06: same/cross-source JOIN, FULL OUTER JOIN, semi/anti JOIN
    """

    # The OS timezone is Asia/Shanghai (CST, +0800), so taosd defaults to CST.
    # MySQL DATETIME(3) stores UTC strings like '2024-01-01 00:00:00.000'
    # without timezone metadata.  When taosd reads them in CST mode it
    # interprets them as local (CST) → unix-ms 1704038400000, which is 8 h
    # behind InfluxDB's UTC nanoseconds (→ unix-ms 1704067200000).  The
    # cross-source JOINs (xsrc-19/21/22/24) therefore return 0 matches.
    # Forcing UTC makes both sources resolve to unix-ms 1704067200000.
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "timezone": "UTC",
        "clientCfg": {
            "federatedQueryEnable": 1,
            "timezone": "UTC",
        },
    }

    _SRC_MYSQL  = "fq_mt_parity_src_m"
    _SRC_PG     = "fq_mt_parity_src_p"
    _SRC_INFLUX = "fq_mt_parity_src_i"
    _class_setup_done = False
    _FLOAT_TOL = _FLOAT_TOL
    _BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_16_multi_table_parity.txt")
    _CROSS_BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_16_multi_table_parity_cross.txt")
    _PARITY_CASES = _PARITY_CASES

    @property
    def _local_tbl(self):
        return _LOCAL_DB

    def _ext_sources(self):
        return [
            ("MySQL",    self._SRC_MYSQL),
            ("PG",       self._SRC_PG),
            ("InfluxDB", self._SRC_INFLUX),
        ]

    def setup_method(self, method):
        if TestFq16MultiTableParity._class_setup_done:
            return
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

        # --- Local TDengine ---
        tdSql.executes(_build_local_setup())

        # --- MySQL ---
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(
            self._mysql_cfg(), _MYSQL_DB, _build_mysql_setup())
        self._cleanup_src(self._SRC_MYSQL)
        self._mk_mysql_real(self._SRC_MYSQL, database=_MYSQL_DB)

        # --- PostgreSQL ---
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _build_pg_setup())
        self._cleanup_src(self._SRC_PG)
        self._mk_pg_real(self._SRC_PG, database=_PG_DB, schema="public")

        # --- InfluxDB ---
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET)
        ExtSrcEnv.influx_write_cfg(
            self._influx_cfg(), _INFLUX_BUCKET, _build_influx_lines())
        self._cleanup_src(self._SRC_INFLUX)
        self._mk_influx_real(self._SRC_INFLUX, database=_INFLUX_BUCKET)

        TestFq16MultiTableParity._class_setup_done = True

    def teardown_class(self):
        tmp = TestFq16MultiTableParity()
        tmp._cleanup_src(tmp._SRC_MYSQL, tmp._SRC_PG, tmp._SRC_INFLUX)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        for drop in [
            lambda: ExtSrcEnv.mysql_drop_db_cfg(tmp._mysql_cfg(), _MYSQL_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(tmp._pg_cfg(), _PG_DB),
            lambda: ExtSrcEnv.influx_drop_db_cfg(
                tmp._influx_cfg(), _INFLUX_BUCKET),
        ]:
            try:
                drop()
            except Exception:
                pass
        TestFq16MultiTableParity._class_setup_done = False
        ExtSrcEnv.teardown_env()

    # ------------------------------------------------------------------
    # Cross-source case runner
    # ------------------------------------------------------------------

    def _run_cross_cases(self, cross_cases):
        """Run cross-source and negative cases with baseline file comparison.

        Returns (n_pass, n_fail, serialized_blocks, failed_list).
        """
        import time as _time

        M = self._SRC_MYSQL
        P = self._SRC_PG
        I = self._SRC_INFLUX
        L = _LOCAL_DB

        failed: list[tuple[str, str, str]] = []
        serialized_blocks: list[str] = []

        for case_id, sql_tmpl, opts in cross_cases:
            sql = sql_tmpl.format(M=M, P=P, I=I, L=L)
            expected_error = opts.get("error")
            positive = expected_error is None
            ordered = opts.get("ordered", True)
            float_cols = opts.get("float_cols") or set()
            kind_tag = "POS" if positive else "NEG"
            sql_short = sql if len(sql) <= 90 else sql[:87] + "..."
            prefix = f"[{case_id:<14s} {kind_tag}]"
            t0 = _time.monotonic()

            qerr = None
            rows = None
            try:
                if positive:
                    tdSql.query(sql, queryTimes=10)
                else:
                    tdSql.cursor.execute(sql)
                    tdSql.queryResult = tdSql.cursor.fetchall()
                    tdSql.queryRows = len(tdSql.queryResult)
                    tdSql.queryCols = len(tdSql.cursor.description)
                rows = list(tdSql.queryResult)
                if not ordered and rows:
                    rows = sorted(rows, key=lambda r: [str(x) for x in r])
            except Exception as e:
                _eargs = getattr(e, 'args', ())
                errno = None
                if len(_eargs) >= 2 and isinstance(_eargs[-1], int):
                    errno = _eargs[-1]
                if errno is None:
                    errno = getattr(e, 'errno', None)
                err_info = str(_eargs[0]) if _eargs else None
                qerr = QueryError(errno, err_info, sql, e)

            # Serialize
            serialized = _cross_serialize_case(
                case_id, sql_tmpl, positive, rows, qerr, float_cols, ordered)
            serialized_blocks.append(serialized)

            elapsed = _time.monotonic() - t0

            if expected_error is not None:
                # Negative case: expect error
                if qerr is None:
                    tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
                    tdLog.info(f"  Expected error {expected_error:#010x} but query succeeded")
                    failed.append((case_id, sql_tmpl, "expected error but succeeded"))
                elif (qerr.qerrno & 0xFFFFFFFF) != (expected_error & 0xFFFFFFFF):
                    tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
                    tdLog.info(
                        f"  Expected errno {expected_error:#010x} "
                        f"got {qerr.qerrno:#010x}: {qerr.err_info}")
                    failed.append((case_id, sql_tmpl,
                                   f"errno mismatch: expected {expected_error:#010x} "
                                   f"got {qerr.qerrno:#010x}"))
                else:
                    tdLog.info(f"{prefix} PASS  {sql_short}  "
                               f"errno={qerr.qerrno:#010x}  [{elapsed:.2f}s]")
            else:
                # Positive case
                if qerr is not None:
                    tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
                    tdLog.info(f"  Unexpected error: {qerr.err_info}")
                    failed.append((case_id, sql_tmpl, f"unexpected error: {qerr.err_info}"))
                else:
                    # Check row count
                    expected_rows = opts.get("rows")
                    if expected_rows is not None and len(rows) != expected_rows:
                        tdLog.info(f"{prefix} FAIL  {sql_short}  [{elapsed:.2f}s]")
                        tdLog.info(
                            f"  Row count mismatch: expected {expected_rows} got {len(rows)}")
                        failed.append((case_id, sql_tmpl,
                                       f"row count: expected {expected_rows} got {len(rows)}"))
                    else:
                        tdLog.info(f"{prefix} PASS  {sql_short}  [{elapsed:.2f}s]")

        return len(cross_cases) - len(failed), len(failed), serialized_blocks, failed

    def _compare_cross_baseline(self, serialized_blocks, failed):
        """Compare serialized cross-case results against baseline file."""
        baseline_file = self._CROSS_BASELINE_FILE
        if not baseline_file:
            return failed

        tmp_file = baseline_file + ".tmp"
        tmp_content = "\n".join(serialized_blocks) + "\n"
        with open(tmp_file, "w") as f:
            f.write(tmp_content)
        tdLog.info(f"Cross-case temp result file: {tmp_file}")

        if os.path.isfile(baseline_file):
            with open(baseline_file, "r") as f:
                baseline_content = f.read()
            if tmp_content != baseline_content:
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
                    f"Cross-case baseline mismatch!\n"
                    f"  baseline: {baseline_file}\n"
                    f"  actual  : {tmp_file}\n"
                    f"  first diff at line {diff_line}:\n"
                    f"    baseline: {bl!r}\n"
                    f"    actual  : {tl!r}\n"
                    f"  Run: diff {baseline_file} {tmp_file}")
                tdLog.info(f"BASELINE MISMATCH: {baseline_err}")
                failed.append(("<cross-baseline>", "<cross-baseline>", baseline_err))
            else:
                tdLog.info("Cross-case baseline comparison: OK")
        else:
            shutil.copy(tmp_file, baseline_file)
            tdLog.info(f"Cross-case baseline file created: {baseline_file}")

        return failed

    # --- Disorder helpers -----------------------------------------------------

    def _rewrite_all_data(self, disorder=True):
        if disorder:
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), _MYSQL_DB, _build_mysql_disorder())
            ExtSrcEnv.pg_exec_cfg(
                self._pg_cfg(), _PG_DB, _build_pg_disorder())
            ExtSrcEnv.influx_drop_db_cfg(
                self._influx_cfg(), _INFLUX_BUCKET)
            ExtSrcEnv.influx_create_db_cfg(
                self._influx_cfg(), _INFLUX_BUCKET)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET,
                _build_influx_disorder_lines())
            tdSql.executes(_build_local_disorder())
        else:
            restore_m = ["DELETE FROM ta", "DELETE FROM tb"]
            restore_m += _build_mysql_setup()[-2:]
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), _MYSQL_DB, restore_m)
            restore_p = ["DELETE FROM ta", "DELETE FROM tb"]
            restore_p += _build_pg_setup()[-2:]
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, restore_p)
            ExtSrcEnv.influx_drop_db_cfg(
                self._influx_cfg(), _INFLUX_BUCKET)
            ExtSrcEnv.influx_create_db_cfg(
                self._influx_cfg(), _INFLUX_BUCKET)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET, _build_influx_lines())
            ta_vals = " ".join(
                f"({ts}, {id_}, {val}, '{label}')"
                for ts, id_, val, label in _TA_ROWS)
            tb_vals = " ".join(
                f"({ts}, {bid}, {ref_id}, {val2}, {flag})"
                for ts, bid, ref_id, val2, flag in _TB_ROWS)
            tdSql.execute(f"DELETE FROM {_LOCAL_DB}.ta WHERE time >= 0")
            tdSql.execute(f"DELETE FROM {_LOCAL_DB}.tb WHERE time >= 0")
            tdSql.execute(f"INSERT INTO {_LOCAL_DB}.ta VALUES {ta_vals}")
            tdSql.execute(f"INSERT INTO {_LOCAL_DB}.tb VALUES {tb_vals}")

    # ==========================================================================
    # Test 1: All parity + cross-source + negative cases
    # ==========================================================================

    def test_fq_mt_parity_all_cases(self):
        """All multi-table parity and cross-source cases.

        Part 1: Same-source multi-table parity (local == MySQL == PG == InfluxDB).
        Tests INNER/LEFT/FULL JOIN, ASOF/WINDOW JOIN, IN/NOT IN/ANY/ALL/SOME
        subqueries, scalar subquery, UNION, and JOIN aggregation.

        Part 2: Cross-source queries (mixing different external sources in one
        statement). UNION, IN/NOT IN, EXISTS/NOT EXISTS, ANY/ALL, scalar subquery,
        TS-PK JOIN, ASOF JOIN, WINDOW JOIN, FULL OUTER JOIN.

        Part 3: Negative tests — unsupported JOIN types and correlated subqueries.

        By default every entry is executed.  Set ``PARITY_IDX`` to filter:
            PARITY_IDX=xjoin-01 pytest ...
            PARITY_IDX=xjoin,xsub pytest ...   # entire groups

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-06-03 wpan Consolidated multi-table parity from fq_04/05/06
        """
        # Part 1: same-source parity (4-source comparison + baseline file)
        self.run_parity_cases(_PARITY_CASES, parity_groups=_PARITY_GROUPS)

        # Part 2 + 3: cross-source and negative (baseline file comparison)
        n_cross = len(_CROSS_CASES)
        n_pos = sum(1 for _, _, o in _CROSS_CASES if "error" not in o)
        n_neg = n_cross - n_pos
        tdLog.info(f"\nCross-source run: {n_cross} case(s)  (pos={n_pos} neg={n_neg})")

        n_pass, n_fail, serialized_blocks, failed = self._run_cross_cases(
            _CROSS_CASES)
        failed = self._compare_cross_baseline(serialized_blocks, failed)

        sep = "─" * 72
        tdLog.info(f"\n{sep}")
        tdLog.info(f"  Cross-source summary: {n_pass}/{n_cross} passed  |  "
                   f"{n_fail} failed  (pos={n_pos} neg={n_neg})")
        if failed:
            tdLog.info("  Failed cases:")
            for case_id, sql, det in failed:
                tdLog.info(f"    [{case_id}]  {sql[:70]}")
                tdLog.info(f"            {det[:130]}")
        tdLog.info(sep)

        # ── cleanup cross temp file ──
        cross_tmp = self._CROSS_BASELINE_FILE + ".tmp"
        if failed:
            tdLog.info(f"Cross temp result file kept for debugging: {cross_tmp}")
        elif os.path.isfile(cross_tmp):
            os.remove(cross_tmp)
            tdLog.info("Cross temp result file removed (all passed).")

        if failed:
            all_errors = "\n".join(
                f"\n[{case_id}] {sql}\n  {det}" for case_id, sql, det in failed)
            raise AssertionError(
                f"{len(failed)} cross-source case(s) failed:\n{all_errors}")

    # ==========================================================================
    # Test 2: Disorder parity
    # ==========================================================================

    def test_fq_mt_parity_disorder(self):
        """Re-run all positive cases after inserting data in shuffled order.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci
        """
        # Filter positive parity cases
        pos_parity = [(cid, sql, kw) for cid, sql, kw in _PARITY_CASES
                      if kw.get("positive", True)]
        # Filter positive cross-source cases
        pos_cross = [(cid, sql, opts) for cid, sql, opts in _CROSS_CASES
                     if "error" not in opts]

        if not pos_parity and not pos_cross:
            pytest.skip("no positive cases to test")

        try:
            self._rewrite_all_data(disorder=True)

            # Part 1: same-source parity disorder
            if pos_parity:
                self.run_parity_disorder(
                    pos_parity,
                    rewrite_data_fn=lambda: None,
                    restore_data_fn=lambda: None,
                )

            # Part 2: cross-source disorder
            if pos_cross:
                n_pass, n_fail, _, failed = self._run_cross_cases(pos_cross)
                sep = "─" * 72
                tdLog.info(f"\n{sep}")
                tdLog.info(f"  Cross-source disorder: {n_pass}/{len(pos_cross)} passed  |  "
                           f"{n_fail} failed")
                if failed:
                    tdLog.info("  Failed cases:")
                    for case_id, sql, det in failed:
                        tdLog.info(f"    [{case_id}]  {sql[:70]}")
                        tdLog.info(f"            {det[:130]}")
                tdLog.info(sep)
                if failed:
                    all_errors = "\n".join(
                        f"\n[{case_id}] {sql}\n  {det}"
                        for case_id, sql, det in failed)
                    raise AssertionError(
                        f"[disorder] {len(failed)} cross-source case(s) failed:\n"
                        f"{all_errors}")
        finally:
            self._rewrite_all_data(disorder=False)
