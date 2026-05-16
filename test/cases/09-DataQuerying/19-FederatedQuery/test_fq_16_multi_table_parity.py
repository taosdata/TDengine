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
# Parity case registry
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
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.ts = b.ts "
         "ORDER BY a.ts",),
        # 02: LEFT JOIN on ts -> 6 rows (NULL tb at 00:03, 00:04)
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a LEFT JOIN {tbl}.tb b ON a.ts = b.ts "
         "ORDER BY a.ts",),
        # 03: FULL OUTER JOIN on ts -> 7 rows (unordered comparison)
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a FULL JOIN {tbl}.tb b ON a.ts = b.ts",
         {"ordered": False}),
        # 04: LEFT ASOF JOIN -> 6 rows
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT ASOF JOIN {tbl}.tb b ON a.ts >= b.ts "
         "ORDER BY a.ts",),
        # 05: LEFT ASOF JOIN JLIMIT 1 -> 6 rows
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT ASOF JOIN {tbl}.tb b ON a.ts >= b.ts JLIMIT 1 "
         "ORDER BY a.ts",),
        # 06: RIGHT ASOF JOIN -> 5 rows
        ("SELECT a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a RIGHT ASOF JOIN {tbl}.tb b ON b.ts >= a.ts "
         "ORDER BY b.ts",),
        # 07: LEFT WINDOW JOIN (-1m, 1m)
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT WINDOW JOIN {tbl}.tb b WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY a.ts, b.ts",),
        # 08: LEFT WINDOW JOIN (-1m, 1m) JLIMIT 1
        ("SELECT a.id, a.val, b.val2 "
         "FROM {tbl}.ta a LEFT WINDOW JOIN {tbl}.tb b "
         "WINDOW_OFFSET(-1m, 1m) JLIMIT 1 "
         "ORDER BY a.ts",),
        # 09: RIGHT WINDOW JOIN (-1m, 1m)
        ("SELECT a.val, b.bid, b.val2 "
         "FROM {tbl}.ta a RIGHT WINDOW JOIN {tbl}.tb b "
         "WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY b.ts, a.ts",),
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
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.ts = b.ts "
         "GROUP BY a.label ORDER BY a.label",),
        # 02: JOIN + SUM(val2) + GROUP BY label
        ("SELECT a.label, SUM(b.val2) AS total "
         "FROM {tbl}.ta a INNER JOIN {tbl}.tb b ON a.ts = b.ts "
         "GROUP BY a.label ORDER BY a.label",),
    ],
}

_PARITY_CASES: list[tuple[str, str, dict]] = [
    (f"{grp}-{i:02d}", entry[0], entry[1] if len(entry) > 1 else {})
    for grp, entries in _PARITY_GROUPS.items()
    for i, entry in enumerate(entries, 1)
]


# ==============================================================================
# Cross-source query cases  (不要求三库完整结果对比，仅验证跨源查询正确性)
#
# SQL 模板占位符：
#   {M} = MySQL 外部源名称   {P} = PG 外部源名称
#   {I} = InfluxDB 外部源名称  {L} = 本地 TDengine 数据库名
#
# 每条 entry: (case_id, sql_template, opts)
#   opts["rows"]  : 期望行数
#   opts["data"]  : [(row, col, val), ...] 可选精确值校验
#
# ta (6行): ts=00:00-00:05, id=1-6, val=10/20/30/40/50/10, label=a/b/a/b/a/b
# tb (5行): ts=00:00/01/02/05/06, bid=1-5, val2=15/25/30/45/50, flag=1/0/1/1/0
# ref_t (2行, 仅本地): val=1, val=3
# ==============================================================================
_CROSS_CASES: list[tuple[str, str, dict]] = [
    # xsrc-01: 跨源 LEFT ASOF JOIN (MySQL.ta × PG.tb)
    # 对每行 ta 找最近一条 ts <= a.ts 的 tb 行
    # 结果: 6行 (ta共6行，00:03/00:04 匹配 tb(00:02,val2=30))
    (
        "xsrc-01",
        ("SELECT a.id, a.val, b.val2 "
         "FROM {M}.ta a LEFT ASOF JOIN {P}.tb b ON a.ts >= b.ts "
         "ORDER BY a.ts"),
        {"rows": 6,
         "data": [(0, 0, 1), (0, 1, 10), (0, 2, 15),    # ts=00:00
                  (5, 0, 6), (5, 1, 10), (5, 2, 45)]},  # ts=00:05
    ),
    # xsrc-02: 跨源 LEFT WINDOW JOIN (MySQL.ta × InfluxDB.tb, ±1m)
    # 对每行 ta 包含 [a.ts-1m, a.ts+1m] 内的所有 tb 行
    # 各行匹配数: 2+3+2+1+1+2 = 11行
    (
        "xsrc-02",
        ("SELECT a.id, a.val, b.val2 "
         "FROM {M}.ta a LEFT WINDOW JOIN {I}.tb b WINDOW_OFFSET(-1m, 1m) "
         "ORDER BY a.ts, b.ts"),
        {"rows": 11},
    ),
    # xsrc-03: 跨源 FULL OUTER JOIN (PG.ta × InfluxDB.tb ON ts)
    # ta: 6行(00:00-00:05); tb: 5行(00:00-00:02,00:05,00:06)
    # 匹配4行 + 左独2行(00:03,00:04) + 右独1行(00:06) = 7行（无序）
    (
        "xsrc-03",
        ("SELECT a.id, a.val, b.bid, b.val2 "
         "FROM {P}.ta a FULL JOIN {I}.tb b ON a.ts = b.ts"),
        {"rows": 7},
    ),
    # xsrc-04: 跨源 IN 子查询 (MySQL 外表, 本地 ref_t 子查询)
    # ref_t.val = {1, 3} → ta 中 id IN (1,3) → 2行
    (
        "xsrc-04",
        ("SELECT id, val FROM {M}.ta "
         "WHERE id IN (SELECT val FROM {L}.ref_t) "
         "ORDER BY id"),
        {"rows": 2,
         "data": [(0, 0, 1), (0, 1, 10), (1, 0, 3), (1, 1, 30)]},
    ),
    # xsrc-05: 跨源标量子查询 (PG 外表, MySQL.tb MAX 子查询)
    # MAX(val2 WHERE flag=1) = 45 → val > 45 → id=5 (val=50) → 1行
    (
        "xsrc-05",
        ("SELECT id, val FROM {P}.ta "
         "WHERE val > (SELECT MAX(val2) FROM {M}.tb WHERE flag = 1) "
         "ORDER BY id"),
        {"rows": 1,
         "data": [(0, 0, 5), (0, 1, 50)]},
    ),
    # xsrc-06: 跨源 RIGHT ASOF JOIN (InfluxDB.ta × PG.tb)
    # 对每行 tb 找最近一条 ts <= b.ts 的 ta 行
    # tb有5行(00:00/01/02/05/06)，每行都能匹配 ta → 5行
    (
        "xsrc-06",
        ("SELECT a.val, b.bid, b.val2 "
         "FROM {I}.ta a RIGHT ASOF JOIN {P}.tb b ON b.ts >= a.ts "
         "ORDER BY b.ts"),
        {"rows": 5,
         "data": [(0, 0, 10), (0, 1, 1), (0, 2, 15)]},   # b(00:00)→a(00:00,val=10)
    ),
]


# ==============================================================================
# External DB setup SQL builders
# ==============================================================================

def _build_mysql_setup():
    """Build MySQL DDL + INSERT for ta and tb."""
    sqls = [
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        ("CREATE TABLE ta "
         "(ts DATETIME(3) PRIMARY KEY, id INT, val INT, label VARCHAR(16))"),
        ("CREATE TABLE tb "
         "(ts DATETIME(3) PRIMARY KEY, bid INT, ref_id INT, val2 INT, flag INT)"),
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
    """Build PostgreSQL DDL + INSERT for ta and tb."""
    sqls = [
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        ("CREATE TABLE ta "
         "(ts TIMESTAMP PRIMARY KEY, id INT, val INT, label TEXT)"),
        ("CREATE TABLE tb "
         "(ts TIMESTAMP PRIMARY KEY, bid INT, ref_id INT, val2 INT, flag INT)"),
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
    """Build InfluxDB line-protocol for ta and tb measurements."""
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
    """Build local TDengine DDL + INSERT for ta, tb, ref_t, empty_t."""
    sqls = [
        f"DROP DATABASE IF EXISTS {_LOCAL_DB}",
        f"CREATE DATABASE {_LOCAL_DB}",
        f"USE {_LOCAL_DB}",
        "CREATE TABLE ta (ts TIMESTAMP, id INT, val INT, label NCHAR(16))",
        "CREATE TABLE tb (ts TIMESTAMP, bid INT, ref_id INT, val2 INT, flag INT)",
        "CREATE TABLE ref_t (ts TIMESTAMP, val INT)",
        "CREATE TABLE empty_t (ts TIMESTAMP, val INT)",
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
        f"DELETE FROM {_LOCAL_DB}.ta WHERE ts >= 0",
        f"DELETE FROM {_LOCAL_DB}.tb WHERE ts >= 0",
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

    _SRC_MYSQL  = "fq_mt_parity_src_m"
    _SRC_PG     = "fq_mt_parity_src_p"
    _SRC_INFLUX = "fq_mt_parity_src_i"
    _class_setup_done = False
    _FLOAT_TOL = _FLOAT_TOL
    _BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_16_multi_table_parity.txt")
    _PARITY_CASES = _PARITY_CASES

    @property
    def _local_tbl(self):
        """Source/DB prefix for local TDengine tables."""
        return _LOCAL_DB

    def _ext_sources(self):
        """Source prefixes for external databases."""
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

    # --- Disorder helpers -----------------------------------------------------

    def _rewrite_all_data(self, disorder=True):
        """Re-insert data in disorder order (or restore original)."""
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
            tdSql.execute(f"DELETE FROM {_LOCAL_DB}.ta WHERE ts >= 0")
            tdSql.execute(f"DELETE FROM {_LOCAL_DB}.tb WHERE ts >= 0")
            tdSql.execute(f"INSERT INTO {_LOCAL_DB}.ta VALUES {ta_vals}")
            tdSql.execute(f"INSERT INTO {_LOCAL_DB}.tb VALUES {tb_vals}")

    # ==========================================================================
    # Test 1: Same-source multi-table result parity
    # ==========================================================================

    def test_fq_mt_parity_all_cases(self):
        """Same-source multi-table parity: local == MySQL == PG == InfluxDB.

        Tests INNER/LEFT/FULL JOIN, ASOF/WINDOW JOIN, IN/NOT IN/ANY/ALL/SOME
        subqueries, scalar subquery, UNION, and JOIN aggregation.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-06-03 wpan Consolidated multi-table parity from fq_04/05/06
        """
        self.run_parity_cases(_PARITY_CASES, parity_groups=_PARITY_GROUPS)

    # ==========================================================================
    # Test 2: Disorder parity
    # ==========================================================================

    def test_fq_mt_parity_disorder(self):
        """Re-run positive parity cases after inserting data in shuffled order.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci
        """
        pos_cases = [(cid, sql, kw) for cid, sql, kw in _PARITY_CASES
                     if kw.get("positive", True)]
        if not pos_cases:
            pytest.skip("no positive cases to test")
        try:
            self._rewrite_all_data(disorder=True)
            self.run_parity_cases(pos_cases, parity_groups=_PARITY_GROUPS)
        finally:
            self._rewrite_all_data(disorder=False)

    # ==========================================================================
    # Test 3: Cross-source operations
    # ==========================================================================

    def test_fq_mt_cross_source(self):
        """Cross-source multi-table: UNION, IN subquery, EXISTS, JOIN.

        Migrated from:
          - test_fq_04: cross-source UNION, cross-source IN subquery
          - test_fq_05: cross-source IN/NOT IN, EXISTS/NOT EXISTS, scalar, ANY/ALL
          - test_fq_06: cross-source TS-PK JOIN

        Catalog: - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci
        """
        M = self._SRC_MYSQL
        P = self._SRC_PG
        I = self._SRC_INFLUX

        # -- Cross-source UNION ------------------------------------------------
        # MySQL + PG UNION ALL -> 12 rows (6+6, identical data from 2 sources)
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {P}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(12)

        # MySQL + InfluxDB UNION ALL -> 12 rows (6+6)
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {I}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(12)

        # PG + InfluxDB UNION ALL -> 12 rows (6+6)
        tdSql.query(
            f"SELECT id, val FROM {P}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {I}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(12)

        # MySQL + PG UNION (dedup) -> 6 rows
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION "
            f"SELECT id, val FROM {P}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(6)

        # MySQL + InfluxDB UNION -> 6 rows
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION "
            f"SELECT id, val FROM {I}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(6)

        # 3-source UNION ALL -> 18 rows (6x3)
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {P}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {I}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(18)

        # 4-source UNION ALL: MySQL + PG + InfluxDB + local -> 24 rows (6x4)
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {P}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {I}.ta "
            f"UNION ALL "
            f"SELECT id, val FROM {_LOCAL_DB}.ta "
            f"ORDER BY 1, 2")
        tdSql.checkRows(24)

        # -- Cross-source IN subquery ------------------------------------------
        # MySQL outer WHERE id IN (PG subquery) -> 4 rows
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"WHERE id IN (SELECT ref_id FROM {P}.tb) "
            f"ORDER BY id")
        tdSql.checkRows(4)

        # InfluxDB outer WHERE id IN (local TDengine subquery)
        # ref_t.val = {1,3} -> id in {1,3} -> 2 rows
        tdSql.query(
            f"SELECT id, val FROM {I}.ta "
            f"WHERE id IN (SELECT val FROM {_LOCAL_DB}.ref_t) "
            f"ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        # MySQL outer WHERE id NOT IN (local subquery) -> 4 rows
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"WHERE id NOT IN (SELECT val FROM {_LOCAL_DB}.ref_t) "
            f"ORDER BY id")
        tdSql.checkRows(4)

        # -- Cross-source EXISTS (non-correlated) ------------------------------
        # MySQL outer + local EXISTS (TRUE -> all 6 rows)
        tdSql.query(
            f"SELECT id FROM {M}.ta "
            f"WHERE EXISTS (SELECT 1 FROM {_LOCAL_DB}.ref_t WHERE val = 1) "
            f"ORDER BY id")
        tdSql.checkRows(6)

        # PG outer + local NOT EXISTS (empty_t -> TRUE -> all 6 rows)
        tdSql.query(
            f"SELECT id FROM {P}.ta "
            f"WHERE NOT EXISTS (SELECT 1 FROM {_LOCAL_DB}.empty_t) "
            f"ORDER BY id")
        tdSql.checkRows(6)

        # InfluxDB outer + local EXISTS
        tdSql.query(
            f"SELECT id FROM {I}.ta "
            f"WHERE EXISTS (SELECT 1 FROM {_LOCAL_DB}.ref_t WHERE val = 1) "
            f"ORDER BY ts")
        tdSql.checkRows(6)

        # -- Cross-source scalar subquery --------------------------------------
        # InfluxDB outer WHERE val > MAX(val2 from PG WHERE flag=1)
        # MAX(val2 where flag=1) = 45 -> val > 45 -> 1 row (id=5, val=50)
        tdSql.query(
            f"SELECT id, val FROM {I}.ta "
            f"WHERE val > (SELECT MAX(val2) FROM {P}.tb WHERE flag = 1) "
            f"ORDER BY ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 50)

        # -- Cross-source ANY/ALL ----------------------------------------------
        # MySQL outer WHERE val > ANY (PG.tb val2 where flag=1)
        # min val2 where flag=1 = 15 -> val > 15 -> 4 rows
        tdSql.query(
            f"SELECT id, val FROM {M}.ta "
            f"WHERE val > ANY (SELECT val2 FROM {P}.tb WHERE flag = 1) "
            f"ORDER BY id")
        tdSql.checkRows(4)

        # InfluxDB outer WHERE val > ALL (local ref_t)
        # ref_t.val = {1,3}, max=3 -> val > 3 -> all 6 rows
        tdSql.query(
            f"SELECT id, val FROM {I}.ta "
            f"WHERE val > ALL (SELECT val FROM {_LOCAL_DB}.ref_t) "
            f"ORDER BY ts")
        tdSql.checkRows(6)

        # -- Cross-source TS-PK JOIN -------------------------------------------
        # MySQL.ta JOIN PG.tb ON ts -> 4 rows
        tdSql.query(
            f"SELECT a.id, a.val, b.bid, b.val2 "
            f"FROM {M}.ta a INNER JOIN {P}.tb b ON a.ts = b.ts "
            f"ORDER BY a.ts")
        tdSql.checkRows(4)

        # MySQL.ta JOIN InfluxDB.tb ON ts -> 4 rows
        tdSql.query(
            f"SELECT a.id, a.val, b.bid, b.val2 "
            f"FROM {M}.ta a INNER JOIN {I}.tb b ON a.ts = b.ts "
            f"ORDER BY a.ts")
        tdSql.checkRows(4)

    # ==========================================================================
    # Test 4: Negative tests -- unsupported JOIN types
    # ==========================================================================

    def test_fq_mt_negative_join(self):
        """Non-TS JOIN and unsupported JOIN types produce errors.

        Migrated from:
          - test_fq_04: RIGHT/FULL/SEMI/ANTI JOIN (all non-TS -> error)
          - test_fq_06: same/cross-source non-TS JOIN -> error

        Catalog: - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci
        """
        for src in [self._SRC_MYSQL, self._SRC_PG, self._SRC_INFLUX]:
            # Non-TS INNER JOIN -> error
            tdSql.error(
                f"SELECT a.id, b.bid FROM {src}.ta a "
                f"JOIN {src}.tb b ON a.id = b.bid ORDER BY a.id",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # Non-TS FULL OUTER JOIN -> error
            tdSql.error(
                f"SELECT a.id, b.bid FROM {src}.ta a "
                f"FULL JOIN {src}.tb b ON a.id = b.bid",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # LEFT SEMI JOIN -> error
            tdSql.error(
                f"SELECT a.id FROM {src}.ta a "
                f"LEFT SEMI JOIN {src}.tb b ON a.id = b.bid ORDER BY a.id",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # LEFT ANTI JOIN -> error
            tdSql.error(
                f"SELECT a.id FROM {src}.ta a "
                f"LEFT ANTI JOIN {src}.tb b ON a.id = b.bid ORDER BY a.id",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # RIGHT SEMI JOIN -> error
            tdSql.error(
                f"SELECT b.bid FROM {src}.ta a "
                f"RIGHT SEMI JOIN {src}.tb b ON a.id = b.bid ORDER BY b.bid",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # RIGHT ANTI JOIN -> error
            tdSql.error(
                f"SELECT b.bid FROM {src}.ta a "
                f"RIGHT ANTI JOIN {src}.tb b ON a.id = b.bid ORDER BY b.bid",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

        # Cross-source non-TS JOIN -> error
        tdSql.error(
            f"SELECT a.id FROM {self._SRC_MYSQL}.ta a "
            f"JOIN {self._SRC_PG}.tb b ON a.id = b.bid ORDER BY a.id",
            expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

    # ==========================================================================
    # Test 5: Negative tests -- correlated subqueries
    # ==========================================================================

    def test_fq_mt_negative_subquery(self):
        """Correlated subqueries on external sources produce errors.

        Migrated from:
          - test_fq_05: correlated EXISTS/NOT EXISTS -> parser error

        Catalog: - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci
        """
        for src in [self._SRC_MYSQL, self._SRC_PG]:
            # Correlated EXISTS -> parser error
            tdSql.error(
                f"SELECT id FROM {src}.ta a "
                f"WHERE EXISTS "
                f"(SELECT 1 FROM {src}.tb b WHERE b.ref_id = a.id) "
                f"ORDER BY id",
                expectedErrno=TSDB_CODE_PAR_INVALID_EXPR_SUBQ)

            # Correlated NOT EXISTS -> parser error
            tdSql.error(
                f"SELECT id FROM {src}.ta a "
                f"WHERE NOT EXISTS "
                f"(SELECT 1 FROM {src}.tb b WHERE b.ref_id = a.id) "
                f"ORDER BY id",
                expectedErrno=TSDB_CODE_PAR_INVALID_EXPR_SUBQ)

    # ==========================================================================
    # Test 6: Cross-source structured cases  (_CROSS_CASES)
    # ==========================================================================

    def test_fq_mt_cross_source_cases(self):
        """Cross-source structured query cases: ASOF/WINDOW JOIN, IN, scalar subquery.

        Iterates _CROSS_CASES — cross-source queries that mix different external
        sources in a single statement.  Unlike the parity tests, these cases do
        NOT require identical results across all three sources; they only verify
        that the cross-source query produces the expected row count and spot-check
        values against the shared ta/tb/ref_t data.

        Migrated from:
          - test_fq_05: cross-source IN subquery (021), same-source cross-DB JOIN (019/020)
          - test_fq_06: cross-source ASOF/WINDOW JOIN (s06)

        Catalog: - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci
        """
        M = self._SRC_MYSQL
        P = self._SRC_PG
        I = self._SRC_INFLUX
        L = _LOCAL_DB

        for (case_id, sql_tmpl, opts) in _CROSS_CASES:
            sql = sql_tmpl.format(M=M, P=P, I=I, L=L)
            tdLog.debug(f"[{case_id}] {sql}")
            tdSql.query(sql)
            tdSql.checkRows(opts["rows"])
            for (row, col, val) in opts.get("data", []):
                tdSql.checkData(row, col, val)
