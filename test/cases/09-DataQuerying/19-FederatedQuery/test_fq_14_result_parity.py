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

Schema:
  Local TDengine:  ts TIMESTAMP PK, id INT, val INT, score DOUBLE, label NCHAR(32)
  MySQL:           ts DATETIME(3) NOT NULL PK  (enables TDengine window queries)
  PostgreSQL:      ts TIMESTAMP NOT NULL PK
  InfluxDB:        native _time column; region tag = label; id/val/score fields

InfluxDB query adaptations:
  - label column → region tag
  - ORDER BY ts  → ORDER BY time
  - SESSION(ts,  → SESSION(time,
  - PARTITION BY label → PARTITION BY region
  Non-native functions fall back to TDengine local compute after fetch.

Environment:
  Enterprise edition, federatedQueryEnable=1
  MySQL 8.0+, PostgreSQL 14+, InfluxDB v3
  Python: pymysql, psycopg2, requests
"""

import math
import pytest

from new_test_framework.utils import tdLog, tdSql

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
    "  ts DATETIME(3) NOT NULL, id INT, val INT, score DOUBLE, label VARCHAR(32),"
    "  PRIMARY KEY (ts)"
    ")",
] + [
    f"INSERT INTO parity_t VALUES ('{ts}', {i}, {v}, {s}, '{l}')"
    for ts, i, v, s, l in _ROWS_DT
]

# PostgreSQL: TIMESTAMP PRIMARY KEY
_PG_SETUP = [
    "DROP TABLE IF EXISTS public.parity_t",
    "CREATE TABLE public.parity_t ("
    "  ts TIMESTAMP NOT NULL PRIMARY KEY,"
    "  id INT, val INT, score DOUBLE PRECISION, label VARCHAR(32)"
    ")",
] + [
    f"INSERT INTO public.parity_t VALUES ('{ts}', {i}, {v}, {s}, '{l}')"
    for ts, i, v, s, l in _ROWS_DT
]

# InfluxDB line-protocol: region tag = label; id/val/score fields; ts in ns
_INFLUX_LINES = [
    f"parity_t,region={l} id={i}i,val={v}i,score={s} {ts}000000"
    for ts, i, v, s, l in _ROWS
]

_LOCAL_SETUP = [
    f"DROP DATABASE IF EXISTS {_LOCAL_DB}",
    f"CREATE DATABASE {_LOCAL_DB}",
    f"USE {_LOCAL_DB}",
    f"CREATE TABLE {_LOCAL_TBL} ("
    f"  ts TIMESTAMP, id INT, val INT, score DOUBLE, label NCHAR(32)"
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


class TestFq14ResultParity(FederatedQueryTestMixin):
    """Result-parity: local TDengine == MySQL == PostgreSQL == InfluxDB.

    Every test executes the same logical query against all four sources
    and asserts row-by-row equality.  A source is only omitted when its
    SQL dialect physically lacks the required syntax.
    """

    _SRC_MYSQL  = "fq_parity_src_m"
    _SRC_PG     = "fq_parity_src_p"
    _SRC_INFLUX = "fq_parity_src_i"

    @property
    def _L(self):
        return f"{_LOCAL_DB}.{_LOCAL_TBL}"

    @property
    def _M(self):
        return f"{self._SRC_MYSQL}.{_MYSQL_DB}.parity_t"

    @property
    def _P(self):
        return f"{self._SRC_PG}.{_PG_DB}.parity_t"

    @property
    def _I(self):
        return f"{self._SRC_INFLUX}.{_INFLUX_BUCKET}.parity_t"

    def setup_class(self):
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

    def teardown_class(self):
        self._cleanup_src(self._SRC_MYSQL, self._SRC_PG, self._SRC_INFLUX)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        for drop in [
            lambda: ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), _PG_DB),
            lambda: ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET),
        ]:
            try:
                drop()
            except Exception:
                pass

    def _get_rows(self, sql):
        """Execute *sql* and return results as a list of tuples.

        On failure raises AssertionError that includes the SQL text,
        errno, and error_info so the failing query is immediately
        identifiable in the test report without re-running.
        """
        try:
            tdSql.query(sql)
        except Exception as e:
            errno    = getattr(tdSql, 'errno',      None)
            err_info = getattr(tdSql, 'error_info', None)
            detail = ""
            if errno is not None:
                detail += f"\n  errno:      {errno:#010x}"
            if err_info:
                detail += f"\n  error_info: {err_info}"
            raise AssertionError(
                f"Query execution failed{detail}\n"
                f"  sql: {sql}\n"
                f"  raw exception: {e}"
            ) from e
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
        local_sql,
        mysql_sql=None,
        pg_sql=None,
        influx_sql=None,
        *,
        float_cols=None,
        ordered=True,
    ):
        """Compare local TDengine result against MySQL, PG and InfluxDB.

        Pass None to skip a source.  Any non-None source must return
        identical results to the local reference.
        """
        float_cols = float_cols or set()
        ref = self._get_rows(local_sql)
        if not ordered:
            ref = sorted(ref, key=lambda r: [str(x) for x in r])
        for lbl, sql in [
            ("MySQL",    mysql_sql),
            ("PG",       pg_sql),
            ("InfluxDB", influx_sql),
        ]:
            if sql is None:
                continue
            rows = self._get_rows(sql)
            if not ordered:
                rows = sorted(rows, key=lambda r: [str(x) for x in r])
            self._compare_rows(ref, rows, local_sql, sql, lbl, float_cols)


    def test_fq_parity_001_basic_select_id_val_score_label_order_by_ts(self):
        """basic SELECT id val score label ORDER BY ts

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val, score, label FROM {L} ORDER BY ts",
            f"SELECT id, val, score, label FROM {M} ORDER BY ts",
            f"SELECT id, val, score, label FROM {P} ORDER BY ts",
            f"SELECT id, val, score, region AS label FROM {I} ORDER BY time",
            float_cols={2},
        )

    def test_fq_parity_002_where_val_20_order_by_ts(self):
        """WHERE val > 20 ORDER BY ts

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val > 20 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val > 20 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val > 20 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val > 20 ORDER BY time",
        )

    def test_fq_parity_003_count_sum_min_max_aggregate(self):
        """COUNT SUM MIN MAX aggregate

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {L}",
            f"SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {M}",
            f"SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {P}",
            f"SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM {I}",
            ordered=False,
        )

    def test_fq_parity_004_avg_val_float(self):
        """AVG val float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT AVG(val) FROM {L}",
            f"SELECT AVG(val) FROM {M}",
            f"SELECT AVG(val) FROM {P}",
            f"SELECT AVG(val) FROM {I}",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_005_group_by_label_count(self):
        """GROUP BY label COUNT

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(*) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(*) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(*) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, COUNT(*) FROM {I} GROUP BY region ORDER BY region",
        )

    def test_fq_parity_006_group_by_having_count_gt_1(self):
        """GROUP BY HAVING COUNT gt 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(*) FROM {L} GROUP BY label HAVING COUNT(*) > 1 ORDER BY label",
            f"SELECT label, COUNT(*) FROM {M} GROUP BY label HAVING COUNT(*) > 1 ORDER BY label",
            f"SELECT label, COUNT(*) FROM {P} GROUP BY label HAVING COUNT(*) > 1 ORDER BY label",
            f"SELECT region AS label, COUNT(*) FROM {I} GROUP BY region HAVING COUNT(*) > 1 ORDER BY region",
        )

    def test_fq_parity_007_limit_3_offset_1(self):
        """LIMIT 3 OFFSET 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} ORDER BY ts LIMIT 3 OFFSET 1",
            f"SELECT id, val FROM {M} ORDER BY ts LIMIT 3 OFFSET 1",
            f"SELECT id, val FROM {P} ORDER BY ts LIMIT 3 OFFSET 1",
            f"SELECT id, val FROM {I} ORDER BY time LIMIT 3 OFFSET 1",
        )

    def test_fq_parity_008_distinct_label(self):
        """DISTINCT label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT DISTINCT label FROM {L} ORDER BY label",
            f"SELECT DISTINCT label FROM {M} ORDER BY label",
            f"SELECT DISTINCT label FROM {P} ORDER BY label",
            f"SELECT DISTINCT region FROM {I} ORDER BY region",
        )

    def test_fq_parity_009_arithmetic_val_2_1(self):
        """arithmetic val * 2 + 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val * 2 + 1 FROM {L} ORDER BY ts",
            f"SELECT id, val * 2 + 1 FROM {M} ORDER BY ts",
            f"SELECT id, val * 2 + 1 FROM {P} ORDER BY ts",
            f"SELECT id, val * 2 + 1 FROM {I} ORDER BY time",
        )

    def test_fq_parity_010_order_by_val_desc(self):
        """ORDER BY val DESC

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} ORDER BY val DESC",
            f"SELECT id, val FROM {M} ORDER BY val DESC",
            f"SELECT id, val FROM {P} ORDER BY val DESC",
            f"SELECT id, val FROM {I} ORDER BY val DESC",
        )

    def test_fq_parity_011_where_val_30_equality(self):
        """WHERE val = 30 equality

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val = 30 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val = 30 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val = 30 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val = 30 ORDER BY time",
        )

    def test_fq_parity_012_where_val_30_inequality(self):
        """WHERE val <> 30 inequality

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val <> 30 ORDER BY ts",
            f"SELECT id FROM {M} WHERE val <> 30 ORDER BY ts",
            f"SELECT id FROM {P} WHERE val <> 30 ORDER BY ts",
            f"SELECT id FROM {I} WHERE val <> 30 ORDER BY time",
        )

    def test_fq_parity_013_where_val_30(self):
        """WHERE val <= 30

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val <= 30 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val <= 30 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val <= 30 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val <= 30 ORDER BY time",
        )

    def test_fq_parity_014_where_val_30(self):
        """WHERE val >= 30

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val >= 30 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val >= 30 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val >= 30 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val >= 30 ORDER BY time",
        )

    def test_fq_parity_015_where_val_between_20_and_40(self):
        """WHERE val BETWEEN 20 AND 40

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val BETWEEN 20 AND 40 ORDER BY time",
        )

    def test_fq_parity_016_where_val_not_between_20_and_40(self):
        """WHERE val NOT BETWEEN 20 AND 40

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val NOT BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id FROM {M} WHERE val NOT BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id FROM {P} WHERE val NOT BETWEEN 20 AND 40 ORDER BY ts",
            f"SELECT id FROM {I} WHERE val NOT BETWEEN 20 AND 40 ORDER BY time",
        )

    def test_fq_parity_017_where_label_in_list(self):
        """WHERE label IN list

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label IN ('north', 'east') ORDER BY ts",
            f"SELECT id FROM {M} WHERE label IN ('north', 'east') ORDER BY ts",
            f"SELECT id FROM {P} WHERE label IN ('north', 'east') ORDER BY ts",
            f"SELECT id FROM {I} WHERE region IN ('north', 'east') ORDER BY time",
        )

    def test_fq_parity_018_where_label_not_in_list(self):
        """WHERE label NOT IN list

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label NOT IN ('east') ORDER BY ts",
            f"SELECT id FROM {M} WHERE label NOT IN ('east') ORDER BY ts",
            f"SELECT id FROM {P} WHERE label NOT IN ('east') ORDER BY ts",
            f"SELECT id FROM {I} WHERE region NOT IN ('east') ORDER BY time",
        )

    def test_fq_parity_019_where_label_like_prefix(self):
        """WHERE label LIKE prefix

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {M} WHERE label LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {P} WHERE label LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {I} WHERE region LIKE 'n%' ORDER BY time",
        )

    def test_fq_parity_020_where_label_like_suffix(self):
        """WHERE label LIKE suffix

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label LIKE '%th' ORDER BY ts",
            f"SELECT id FROM {M} WHERE label LIKE '%th' ORDER BY ts",
            f"SELECT id FROM {P} WHERE label LIKE '%th' ORDER BY ts",
            f"SELECT id FROM {I} WHERE region LIKE '%th' ORDER BY time",
        )

    def test_fq_parity_021_where_label_not_like(self):
        """WHERE label NOT LIKE

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label NOT LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {M} WHERE label NOT LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {P} WHERE label NOT LIKE 'n%' ORDER BY ts",
            f"SELECT id FROM {I} WHERE region NOT LIKE 'n%' ORDER BY time",
        )

    def test_fq_parity_022_label_is_not_null(self):
        """label IS NOT NULL

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label IS NOT NULL ORDER BY ts",
            f"SELECT id FROM {M} WHERE label IS NOT NULL ORDER BY ts",
            f"SELECT id FROM {P} WHERE label IS NOT NULL ORDER BY ts",
            f"SELECT id FROM {I} WHERE region IS NOT NULL ORDER BY time",
        )

    def test_fq_parity_023_label_is_null_returns_zero_rows(self):
        """label IS NULL returns zero rows

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE label IS NULL ORDER BY ts",
            f"SELECT id FROM {M} WHERE label IS NULL ORDER BY ts",
            f"SELECT id FROM {P} WHERE label IS NULL ORDER BY ts",
            f"SELECT id FROM {I} WHERE region IS NULL ORDER BY time",
        )

    def test_fq_parity_024_where_val_20_and_val_50(self):
        """WHERE val > 20 AND val < 50

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val > 20 AND val < 50 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val > 20 AND val < 50 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val > 20 AND val < 50 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val > 20 AND val < 50 ORDER BY time",
        )

    def test_fq_parity_025_where_val_15_or_val_45(self):
        """WHERE val < 15 OR val > 45

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val < 15 OR val > 45 ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val < 15 OR val > 45 ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val < 15 OR val > 45 ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val < 15 OR val > 45 ORDER BY time",
        )

    def test_fq_parity_026_where_not_val_30(self):
        """WHERE NOT val > 30

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE NOT (val > 30) ORDER BY ts",
            f"SELECT id FROM {M} WHERE NOT (val > 30) ORDER BY ts",
            f"SELECT id FROM {P} WHERE NOT (val > 30) ORDER BY ts",
            f"SELECT id FROM {I} WHERE NOT (val > 30) ORDER BY time",
        )

    def test_fq_parity_027_coalesce_label_fallback(self):
        """COALESCE label fallback

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, COALESCE(label, 'unknown') FROM {L} ORDER BY ts",
            f"SELECT id, COALESCE(label, 'unknown') FROM {M} ORDER BY ts",
            f"SELECT id, COALESCE(label, 'unknown') FROM {P} ORDER BY ts",
            f"SELECT id, COALESCE(region, 'unknown') FROM {I} ORDER BY time",
        )

    def test_fq_parity_028_nullif_val_30(self):
        """NULLIF val 30

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, NULLIF(val, 30) FROM {L} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {M} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {P} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {I} ORDER BY time",
        )

    def test_fq_parity_029_if_case_conditional_val(self):
        """IF CASE conditional val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, IF(val > 30, 'high', 'low') AS cat FROM {L} ORDER BY ts",
            f"SELECT id, IF(val > 30, 'high', 'low') AS cat FROM {M} ORDER BY ts",
            f"SELECT id, CASE WHEN val > 30 THEN 'high' ELSE 'low' END AS cat FROM {P} ORDER BY ts",
            f"SELECT id, IF(val > 30, 'high', 'low') AS cat FROM {I} ORDER BY time",
        )

    def test_fq_parity_030_ifnull_coalesce_null_substitution(self):
        """IFNULL COALESCE null substitution

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, IFNULL(label, 'none') FROM {L} ORDER BY ts",
            f"SELECT id, IFNULL(label, 'none') FROM {M} ORDER BY ts",
            f"SELECT id, COALESCE(label, 'none') FROM {P} ORDER BY ts",
            f"SELECT id, IFNULL(region, 'none') FROM {I} ORDER BY time",
        )

    def test_fq_parity_031_unary_minus_val(self):
        """unary minus -val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, -val FROM {L} ORDER BY ts",
            f"SELECT id, -val FROM {M} ORDER BY ts",
            f"SELECT id, -val FROM {P} ORDER BY ts",
            f"SELECT id, -val FROM {I} ORDER BY time",
        )

    def test_fq_parity_032_subtraction_val_5(self):
        """subtraction val - 5

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val - 5 FROM {L} ORDER BY ts",
            f"SELECT id, val - 5 FROM {M} ORDER BY ts",
            f"SELECT id, val - 5 FROM {P} ORDER BY ts",
            f"SELECT id, val - 5 FROM {I} ORDER BY time",
        )

    def test_fq_parity_033_multiplication_val_3(self):
        """multiplication val * 3

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val * 3 FROM {L} ORDER BY ts",
            f"SELECT id, val * 3 FROM {M} ORDER BY ts",
            f"SELECT id, val * 3 FROM {P} ORDER BY ts",
            f"SELECT id, val * 3 FROM {I} ORDER BY time",
        )

    def test_fq_parity_034_division_val_4_0_float(self):
        """division val / 4.0 float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val / 4.0 FROM {L} ORDER BY ts",
            f"SELECT id, val / 4.0 FROM {M} ORDER BY ts",
            f"SELECT id, val / 4.0 FROM {P} ORDER BY ts",
            f"SELECT id, val / 4.0 FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_035_modulo_val_3(self):
        """modulo val % 3

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val % 3 FROM {L} ORDER BY ts",
            f"SELECT id, val % 3 FROM {M} ORDER BY ts",
            f"SELECT id, val % 3 FROM {P} ORDER BY ts",
            f"SELECT id, val % 3 FROM {I} ORDER BY time",
        )

    def test_fq_parity_036_bitwise_and_val_3(self):
        """bitwise AND val & 3

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val & 3 FROM {L} ORDER BY ts",
            f"SELECT id, val & 3 FROM {M} ORDER BY ts",
            f"SELECT id, val & 3 FROM {P} ORDER BY ts",
            f"SELECT id, val & 3 FROM {I} ORDER BY time",
        )

    def test_fq_parity_037_bitwise_or_val_1(self):
        """bitwise OR val | 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val | 1 FROM {L} ORDER BY ts",
            f"SELECT id, val | 1 FROM {M} ORDER BY ts",
            f"SELECT id, val | 1 FROM {P} ORDER BY ts",
            f"SELECT id, val | 1 FROM {I} ORDER BY time",
        )

    def test_fq_parity_038_greatest_id_val(self):
        """GREATEST id val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, GREATEST(id, val) FROM {L} ORDER BY ts",
            f"SELECT id, GREATEST(id, val) FROM {M} ORDER BY ts",
            f"SELECT id, GREATEST(id, val) FROM {P} ORDER BY ts",
            f"SELECT id, GREATEST(id, val) FROM {I} ORDER BY time",
        )

    def test_fq_parity_039_least_id_val(self):
        """LEAST id val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LEAST(id, val) FROM {L} ORDER BY ts",
            f"SELECT id, LEAST(id, val) FROM {M} ORDER BY ts",
            f"SELECT id, LEAST(id, val) FROM {P} ORDER BY ts",
            f"SELECT id, LEAST(id, val) FROM {I} ORDER BY time",
        )

    def test_fq_parity_040_pi_constant(self):
        """PI constant

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, TRUNCATE(PI(), 5) AS pi5 FROM {L} ORDER BY ts",
            f"SELECT id, TRUNCATE(PI(), 5) AS pi5 FROM {M} ORDER BY ts",
            f"SELECT id, TRUNC(PI()::NUMERIC, 5) AS pi5 FROM {P} ORDER BY ts",
            f"SELECT id, TRUNCATE(PI(), 5) AS pi5 FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_041_abs_20_val(self):
        """ABS 20 - val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ABS(20 - val) FROM {L} ORDER BY ts",
            f"SELECT id, ABS(20 - val) FROM {M} ORDER BY ts",
            f"SELECT id, ABS(20 - val) FROM {P} ORDER BY ts",
            f"SELECT id, ABS(20 - val) FROM {I} ORDER BY time",
        )

    def test_fq_parity_042_ceil_score(self):
        """CEIL score

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CEIL(score) FROM {L} ORDER BY ts",
            f"SELECT id, CEIL(score) FROM {M} ORDER BY ts",
            f"SELECT id, CEIL(score) FROM {P} ORDER BY ts",
            f"SELECT id, CEIL(score) FROM {I} ORDER BY time",
        )

    def test_fq_parity_043_floor_score(self):
        """FLOOR score

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, FLOOR(score) FROM {L} ORDER BY ts",
            f"SELECT id, FLOOR(score) FROM {M} ORDER BY ts",
            f"SELECT id, FLOOR(score) FROM {P} ORDER BY ts",
            f"SELECT id, FLOOR(score) FROM {I} ORDER BY time",
        )

    def test_fq_parity_044_round_score_1_decimal(self):
        """ROUND score 1 decimal

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ROUND(score, 1) FROM {L} ORDER BY ts",
            f"SELECT id, ROUND(score, 1) FROM {M} ORDER BY ts",
            f"SELECT id, ROUND(score::NUMERIC, 1) FROM {P} ORDER BY ts",
            f"SELECT id, ROUND(score, 1) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_045_round_score_integer(self):
        """ROUND score integer

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ROUND(score, 0) FROM {L} ORDER BY ts",
            f"SELECT id, ROUND(score, 0) FROM {M} ORDER BY ts",
            f"SELECT id, ROUND(score::NUMERIC, 0) FROM {P} ORDER BY ts",
            f"SELECT id, ROUND(score, 0) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_046_sqrt_val_float(self):
        """SQRT val float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SQRT(val) FROM {L} ORDER BY ts",
            f"SELECT id, SQRT(val) FROM {M} ORDER BY ts",
            f"SELECT id, SQRT(val::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, SQRT(val) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_047_pow_power_id_squared(self):
        """POW POWER id squared

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, POW(id, 2) FROM {L} ORDER BY ts",
            f"SELECT id, POW(id, 2) FROM {M} ORDER BY ts",
            f"SELECT id, POWER(id, 2) FROM {P} ORDER BY ts",
            f"SELECT id, POW(id, 2) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_048_mod_function_val_3(self):
        """MOD function val 3

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, MOD(val, 3) FROM {L} ORDER BY ts",
            f"SELECT id, MOD(val, 3) FROM {M} ORDER BY ts",
            f"SELECT id, MOD(val, 3) FROM {P} ORDER BY ts",
            f"SELECT id, MOD(val, 3) FROM {I} ORDER BY time",
        )

    def test_fq_parity_049_sign_val_25(self):
        """SIGN val - 25

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SIGN(val - 25) FROM {L} ORDER BY ts",
            f"SELECT id, SIGN(val - 25) FROM {M} ORDER BY ts",
            f"SELECT id, SIGN(val - 25) FROM {P} ORDER BY ts",
            f"SELECT id, SIGN(val - 25) FROM {I} ORDER BY time",
        )

    def test_fq_parity_050_sin_id_float(self):
        """SIN id float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SIN(id) FROM {L} ORDER BY ts",
            f"SELECT id, SIN(id) FROM {M} ORDER BY ts",
            f"SELECT id, SIN(id::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, SIN(id) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_051_cos_id_float(self):
        """COS id float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, COS(id) FROM {L} ORDER BY ts",
            f"SELECT id, COS(id) FROM {M} ORDER BY ts",
            f"SELECT id, COS(id::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, COS(id) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_052_tan_score_float(self):
        """TAN score float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, TAN(score) FROM {L} ORDER BY ts",
            f"SELECT id, TAN(score) FROM {M} ORDER BY ts",
            f"SELECT id, TAN(score::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, TAN(score) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_053_asin_score_10_float(self):
        """ASIN score / 10 float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ASIN(score / 10.0) FROM {L} ORDER BY ts",
            f"SELECT id, ASIN(score / 10.0) FROM {M} ORDER BY ts",
            f"SELECT id, ASIN((score / 10.0)::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, ASIN(score / 10.0) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_054_acos_score_10_float(self):
        """ACOS score / 10 float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ACOS(score / 10.0) FROM {L} ORDER BY ts",
            f"SELECT id, ACOS(score / 10.0) FROM {M} ORDER BY ts",
            f"SELECT id, ACOS((score / 10.0)::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, ACOS(score / 10.0) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_055_atan_id_float(self):
        """ATAN id float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ATAN(id) FROM {L} ORDER BY ts",
            f"SELECT id, ATAN(id) FROM {M} ORDER BY ts",
            f"SELECT id, ATAN(id::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, ATAN(id) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_056_exp_id_float(self):
        """EXP id float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, EXP(id) FROM {L} ORDER BY ts",
            f"SELECT id, EXP(id) FROM {M} ORDER BY ts",
            f"SELECT id, EXP(id::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, EXP(id) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_057_ln_val_natural_log_float(self):
        """LN val natural log float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LN(val) FROM {L} ORDER BY ts",
            f"SELECT id, LN(val) FROM {M} ORDER BY ts",
            f"SELECT id, LN(val::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, LN(val) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_058_log_single_arg_natural_log(self):
        """LOG single arg natural log

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LOG(val) FROM {L} ORDER BY ts",
            f"SELECT id, LOG(val) FROM {M} ORDER BY ts",
            f"SELECT id, LN(val::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, LOG(val) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_059_log_base_10(self):
        """LOG base-10

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LOG(val, 10) FROM {L} ORDER BY ts",
            f"SELECT id, LOG(10, val) FROM {M} ORDER BY ts",
            f"SELECT id, LOG(val::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, LOG(val, 10) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_060_truncate_trunc_score_1(self):
        """TRUNCATE TRUNC score 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, TRUNCATE(score, 1) FROM {L} ORDER BY ts",
            f"SELECT id, TRUNCATE(score, 1) FROM {M} ORDER BY ts",
            f"SELECT id, TRUNC(score::NUMERIC, 1) FROM {P} ORDER BY ts",
            f"SELECT id, TRUNCATE(score, 1) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_061_degrees_score_float(self):
        """DEGREES score float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, DEGREES(score) FROM {L} ORDER BY ts",
            f"SELECT id, DEGREES(score) FROM {M} ORDER BY ts",
            f"SELECT id, DEGREES(score::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, DEGREES(score) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_062_radians_val_float(self):
        """RADIANS val float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, RADIANS(val) FROM {L} ORDER BY ts",
            f"SELECT id, RADIANS(val) FROM {M} ORDER BY ts",
            f"SELECT id, RADIANS(val::DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, RADIANS(val) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_063_lower_label(self):
        """LOWER label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LOWER(label) FROM {L} ORDER BY ts",
            f"SELECT id, LOWER(label) FROM {M} ORDER BY ts",
            f"SELECT id, LOWER(label) FROM {P} ORDER BY ts",
            f"SELECT id, LOWER(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_064_upper_label(self):
        """UPPER label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, UPPER(label) FROM {L} ORDER BY ts",
            f"SELECT id, UPPER(label) FROM {M} ORDER BY ts",
            f"SELECT id, UPPER(label) FROM {P} ORDER BY ts",
            f"SELECT id, UPPER(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_065_char_length_label(self):
        """CHAR_LENGTH label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CHAR_LENGTH(label) FROM {L} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(label) FROM {M} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(label) FROM {P} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_066_length_label(self):
        """LENGTH label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LENGTH(label) FROM {L} ORDER BY ts",
            f"SELECT id, LENGTH(label) FROM {M} ORDER BY ts",
            f"SELECT id, LENGTH(label) FROM {P} ORDER BY ts",
            f"SELECT id, LENGTH(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_067_ltrim_leading_spaces(self):
        """LTRIM leading spaces

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, LTRIM(CONCAT(' ', label)) FROM {L} ORDER BY ts",
            f"SELECT id, LTRIM(CONCAT(' ', label)) FROM {M} ORDER BY ts",
            f"SELECT id, LTRIM(' ' || label) FROM {P} ORDER BY ts",
            f"SELECT id, LTRIM(CONCAT(' ', region)) FROM {I} ORDER BY time",
        )

    def test_fq_parity_068_rtrim_trailing_spaces(self):
        """RTRIM trailing spaces

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, RTRIM(CONCAT(label, ' ')) FROM {L} ORDER BY ts",
            f"SELECT id, RTRIM(CONCAT(label, ' ')) FROM {M} ORDER BY ts",
            f"SELECT id, RTRIM(label || ' ') FROM {P} ORDER BY ts",
            f"SELECT id, RTRIM(CONCAT(region, ' ')) FROM {I} ORDER BY time",
        )

    def test_fq_parity_069_trim_both_sides(self):
        """TRIM both sides

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, TRIM(CONCAT(' ', label, ' ')) FROM {L} ORDER BY ts",
            f"SELECT id, TRIM(CONCAT(' ', label, ' ')) FROM {M} ORDER BY ts",
            f"SELECT id, TRIM(' ' || label || ' ') FROM {P} ORDER BY ts",
            f"SELECT id, TRIM(CONCAT(' ', region, ' ')) FROM {I} ORDER BY time",
        )

    def test_fq_parity_070_concat_label_and_id(self):
        """CONCAT label and id

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CONCAT(label, '-', id) FROM {L} ORDER BY ts",
            f"SELECT id, CONCAT(label, '-', id) FROM {M} ORDER BY ts",
            f"SELECT id, label || '-' || id::TEXT FROM {P} ORDER BY ts",
            f"SELECT id, CONCAT(region, '-', id) FROM {I} ORDER BY time",
        )

    def test_fq_parity_071_concat_ws_sep_id_val(self):
        """CONCAT_WS sep id val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CONCAT_WS('-', id, val) FROM {L} ORDER BY ts",
            f"SELECT id, CONCAT_WS('-', id, val) FROM {M} ORDER BY ts",
            f"SELECT id, CONCAT_WS('-', id::TEXT, val::TEXT) FROM {P} ORDER BY ts",
            f"SELECT id, CONCAT_WS('-', id, val) FROM {I} ORDER BY time",
        )

    def test_fq_parity_072_substring_label_1_3(self):
        """SUBSTRING label 1 3

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SUBSTRING(label, 1, 3) FROM {L} ORDER BY ts",
            f"SELECT id, SUBSTRING(label, 1, 3) FROM {M} ORDER BY ts",
            f"SELECT id, SUBSTRING(label FROM 1 FOR 3) FROM {P} ORDER BY ts",
            f"SELECT id, SUBSTRING(region, 1, 3) FROM {I} ORDER BY time",
        )

    def test_fq_parity_073_substr_negative_offset(self):
        """SUBSTR negative offset

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SUBSTR(label, -3, 3) FROM {L} ORDER BY ts",
            f"SELECT id, SUBSTR(label, -3, 3) FROM {M} ORDER BY ts",
            f"SELECT id, SUBSTR(label, -3, 3) FROM {P} ORDER BY ts",
            f"SELECT id, SUBSTR(region, -3, 3) FROM {I} ORDER BY time",
        )

    def test_fq_parity_074_replace_label_north_n(self):
        """REPLACE label north n

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, REPLACE(label, 'north', 'n') FROM {L} ORDER BY ts",
            f"SELECT id, REPLACE(label, 'north', 'n') FROM {M} ORDER BY ts",
            f"SELECT id, REPLACE(label, 'north', 'n') FROM {P} ORDER BY ts",
            f"SELECT id, REPLACE(region, 'north', 'n') FROM {I} ORDER BY time",
        )

    def test_fq_parity_075_position_o_in_label(self):
        """POSITION o IN label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, POSITION('o' IN label) FROM {L} ORDER BY ts",
            f"SELECT id, POSITION('o' IN label) FROM {M} ORDER BY ts",
            f"SELECT id, POSITION('o' IN label) FROM {P} ORDER BY ts",
            f"SELECT id, POSITION('o' IN region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_076_repeat_x_id_times(self):
        """REPEAT x id times

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, REPEAT('x', id) FROM {L} ORDER BY ts",
            f"SELECT id, REPEAT('x', id) FROM {M} ORDER BY ts",
            f"SELECT id, REPEAT('x', id) FROM {P} ORDER BY ts",
            f"SELECT id, REPEAT('x', id) FROM {I} ORDER BY time",
        )

    def test_fq_parity_077_ascii_first_char_of_label(self):
        """ASCII first char of label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, ASCII(label) FROM {L} ORDER BY ts",
            f"SELECT id, ASCII(label) FROM {M} ORDER BY ts",
            f"SELECT id, ASCII(label) FROM {P} ORDER BY ts",
            f"SELECT id, ASCII(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_078_char_chr_65_returns_a(self):
        """CHAR CHR 65 returns A

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CHAR(65) FROM {L} ORDER BY ts",
            f"SELECT id, CHAR(65) FROM {M} ORDER BY ts",
            f"SELECT id, CHR(65) FROM {P} ORDER BY ts",
            f"SELECT id, CHAR(65) FROM {I} ORDER BY time",
        )

    def test_fq_parity_079_find_in_set_label(self):
        """FIND_IN_SET label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, FIND_IN_SET(label, 'north,south,east') AS pos FROM {L} ORDER BY ts",
            f"SELECT id, FIND_IN_SET(label, 'north,south,east') AS pos FROM {M} ORDER BY ts",
            f"SELECT id, FIND_IN_SET(label, 'north,south,east') AS pos FROM {P} ORDER BY ts",
            f"SELECT id, FIND_IN_SET(region, 'north,south,east') AS pos FROM {I} ORDER BY time",
        )

    def test_fq_parity_080_substring_index_label_o_1(self):
        """SUBSTRING_INDEX label o 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SUBSTRING_INDEX(label, 'o', 1) AS si FROM {L} ORDER BY ts",
            f"SELECT id, SUBSTRING_INDEX(label, 'o', 1) AS si FROM {M} ORDER BY ts",
            f"SELECT id, SUBSTRING_INDEX(label, 'o', 1) AS si FROM {P} ORDER BY ts",
            f"SELECT id, SUBSTRING_INDEX(region, 'o', 1) AS si FROM {I} ORDER BY time",
        )

    def test_fq_parity_081_md5_label_hash(self):
        """MD5 label hash

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, MD5(label) FROM {L} ORDER BY ts",
            f"SELECT id, MD5(label) FROM {M} ORDER BY ts",
            f"SELECT id, MD5(label) FROM {P} ORDER BY ts",
            f"SELECT id, MD5(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_082_to_base64_label(self):
        """TO_BASE64 label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, TO_BASE64(label) FROM {L} ORDER BY ts",
            f"SELECT id, TO_BASE64(label) FROM {M} ORDER BY ts",
            f"SELECT id, TO_BASE64(label) FROM {P} ORDER BY ts",
            f"SELECT id, TO_BASE64(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_083_from_base64_round_trip(self):
        """FROM_BASE64 round trip

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, FROM_BASE64(TO_BASE64(label)) AS decoded FROM {L} ORDER BY ts",
            f"SELECT id, FROM_BASE64(TO_BASE64(label)) AS decoded FROM {M} ORDER BY ts",
            f"SELECT id, FROM_BASE64(TO_BASE64(label)) AS decoded FROM {P} ORDER BY ts",
            f"SELECT id, FROM_BASE64(TO_BASE64(region)) AS decoded FROM {I} ORDER BY time",
        )

    def test_fq_parity_084_sha1_label_hash(self):
        """SHA1 label hash

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SHA1(label) FROM {L} ORDER BY ts",
            f"SELECT id, SHA1(label) FROM {M} ORDER BY ts",
            f"SELECT id, encode(sha1(label::bytea), 'hex') FROM {P} ORDER BY ts",
            f"SELECT id, SHA1(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_085_sha2_256_label_hash(self):
        """SHA2 256 label hash

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, SHA2(label, 256) FROM {L} ORDER BY ts",
            f"SELECT id, SHA2(label, 256) FROM {M} ORDER BY ts",
            f"SELECT id, encode(sha256(label::bytea), 'hex') FROM {P} ORDER BY ts",
            f"SELECT id, SHA2(region, 256) FROM {I} ORDER BY time",
        )

    def test_fq_parity_086_crc32_label(self):
        """CRC32 label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CRC32(label) FROM {L} ORDER BY ts",
            f"SELECT id, CRC32(label) FROM {M} ORDER BY ts",
            f"SELECT id, CRC32(label) FROM {P} ORDER BY ts",
            f"SELECT id, CRC32(region) FROM {I} ORDER BY time",
        )

    def test_fq_parity_087_stddev_population_val(self):
        """STDDEV population val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT STDDEV(val) FROM {L}",
            f"SELECT STDDEV(val) FROM {M}",
            f"SELECT STDDEV_POP(val) FROM {P}",
            f"SELECT STDDEV(val) FROM {I}",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_088_variance_population_val(self):
        """VARIANCE population val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT VARIANCE(val) FROM {L}",
            f"SELECT VARIANCE(val) FROM {M}",
            f"SELECT VAR_POP(val) FROM {P}",
            f"SELECT VARIANCE(val) FROM {I}",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_089_stddev_samp_sample_val(self):
        """STDDEV_SAMP sample val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT STDDEV_SAMP(val) FROM {L}",
            f"SELECT STDDEV_SAMP(val) FROM {M}",
            f"SELECT STDDEV_SAMP(val) FROM {P}",
            f"SELECT STDDEV_SAMP(val) FROM {I}",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_090_var_samp_sample_val(self):
        """VAR_SAMP sample val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT VAR_SAMP(val) FROM {L}",
            f"SELECT VAR_SAMP(val) FROM {M}",
            f"SELECT VAR_SAMP(val) FROM {P}",
            f"SELECT VAR_SAMP(val) FROM {I}",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_091_count_distinct_val(self):
        """COUNT DISTINCT val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(DISTINCT val) FROM {L}",
            f"SELECT COUNT(DISTINCT val) FROM {M}",
            f"SELECT COUNT(DISTINCT val) FROM {P}",
            f"SELECT COUNT(DISTINCT val) FROM {I}",
            ordered=False,
        )

    def test_fq_parity_092_group_by_label_sum_val(self):
        """GROUP BY label SUM val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, SUM(val) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, SUM(val) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, SUM(val) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, SUM(val) FROM {I} GROUP BY region ORDER BY region",
        )

    def test_fq_parity_093_group_by_label_avg_val_float(self):
        """GROUP BY label AVG val float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, AVG(val) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, AVG(val) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, AVG(val) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, AVG(val) FROM {I} GROUP BY region ORDER BY region",
            float_cols={1},
        )

    def test_fq_parity_094_group_by_label_max_val(self):
        """GROUP BY label MAX val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, MAX(val) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, MAX(val) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, MAX(val) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, MAX(val) FROM {I} GROUP BY region ORDER BY region",
        )

    def test_fq_parity_095_group_by_label_min_val(self):
        """GROUP BY label MIN val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, MIN(val) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, MIN(val) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, MIN(val) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, MIN(val) FROM {I} GROUP BY region ORDER BY region",
        )

    def test_fq_parity_096_group_by_label_count_id(self):
        """GROUP BY label COUNT id

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(id) FROM {L} GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(id) FROM {M} GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(id) FROM {P} GROUP BY label ORDER BY label",
            f"SELECT region AS label, COUNT(id) FROM {I} GROUP BY region ORDER BY region",
        )

    def test_fq_parity_097_having_sum_val_30(self):
        """HAVING SUM val > 30

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, SUM(val) FROM {L} GROUP BY label HAVING SUM(val) > 30 ORDER BY label",
            f"SELECT label, SUM(val) FROM {M} GROUP BY label HAVING SUM(val) > 30 ORDER BY label",
            f"SELECT label, SUM(val) FROM {P} GROUP BY label HAVING SUM(val) > 30 ORDER BY label",
            f"SELECT region AS label, SUM(val) FROM {I} GROUP BY region HAVING SUM(val) > 30 ORDER BY region",
        )

    def test_fq_parity_098_having_avg_score_float(self):
        """HAVING AVG score float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, AVG(score) FROM {L} GROUP BY label HAVING AVG(score) > 2.0 ORDER BY label",
            f"SELECT label, AVG(score) FROM {M} GROUP BY label HAVING AVG(score) > 2.0 ORDER BY label",
            f"SELECT label, AVG(score) FROM {P} GROUP BY label HAVING AVG(score) > 2.0 ORDER BY label",
            f"SELECT region AS label, AVG(score) FROM {I} GROUP BY region HAVING AVG(score) > 2.0 ORDER BY region",
            float_cols={1},
        )

    def test_fq_parity_099_count_non_null_label(self):
        """COUNT non-null label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(label) FROM {L}",
            f"SELECT COUNT(label) FROM {M}",
            f"SELECT COUNT(label) FROM {P}",
            f"SELECT COUNT(region) FROM {I}",
            ordered=False,
        )

    def test_fq_parity_100_sum_case_conditional_aggregation(self):
        """SUM CASE conditional aggregation

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(CASE WHEN label = 'north' THEN val ELSE 0 END) FROM {L}",
            f"SELECT SUM(CASE WHEN label = 'north' THEN val ELSE 0 END) FROM {M}",
            f"SELECT SUM(CASE WHEN label = 'north' THEN val ELSE 0 END) FROM {P}",
            f"SELECT SUM(CASE WHEN region = 'north' THEN val ELSE 0 END) FROM {I}",
            ordered=False,
        )

    def test_fq_parity_101_case_when_multi_branch_classification(self):
        """CASE WHEN multi-branch classification

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {L} ORDER BY ts",
            f"SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {M} ORDER BY ts",
            f"SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {P} ORDER BY ts",
            f"SELECT id, CASE WHEN val >= 40 THEN 'high' WHEN val >= 20 THEN 'mid' ELSE 'low' END AS cat FROM {I} ORDER BY time",
        )

    def test_fq_parity_102_case_value_form_val_10_then_ten(self):
        """CASE value form val 10 THEN ten

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {L} ORDER BY ts",
            f"SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {M} ORDER BY ts",
            f"SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {P} ORDER BY ts",
            f"SELECT id, CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS lbl FROM {I} ORDER BY time",
        )

    def test_fq_parity_103_nullif_val_30_returns_null(self):
        """NULLIF val 30 returns NULL

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, NULLIF(val, 30) FROM {L} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {M} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {P} ORDER BY ts",
            f"SELECT id, NULLIF(val, 30) FROM {I} ORDER BY time",
        )

    def test_fq_parity_104_coalesce_null_val_fallback(self):
        """COALESCE NULL val fallback

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, COALESCE(NULL, val) FROM {L} ORDER BY ts",
            f"SELECT id, COALESCE(NULL, val) FROM {M} ORDER BY ts",
            f"SELECT id, COALESCE(NULL, val) FROM {P} ORDER BY ts",
            f"SELECT id, COALESCE(NULL, val) FROM {I} ORDER BY time",
        )

    def test_fq_parity_105_nvl2_label_not_null_and_null_branches(self):
        """NVL2 label not-null and null branches

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, NVL2(label, 'has_val', 'no_val') AS nv FROM {L} ORDER BY ts",
            f"SELECT id, CASE WHEN label IS NOT NULL THEN 'has_val' ELSE 'no_val' END AS nv FROM {M} ORDER BY ts",
            f"SELECT id, CASE WHEN label IS NOT NULL THEN 'has_val' ELSE 'no_val' END AS nv FROM {P} ORDER BY ts",
            f"SELECT id, NVL2(region, 'has_val', 'no_val') AS nv FROM {I} ORDER BY time",
        )

    def test_fq_parity_106_cast_val_as_double_float(self):
        """CAST val AS DOUBLE float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CAST(val AS DOUBLE) FROM {L} ORDER BY ts",
            f"SELECT id, CAST(val AS DOUBLE) FROM {M} ORDER BY ts",
            f"SELECT id, CAST(val AS DOUBLE PRECISION) FROM {P} ORDER BY ts",
            f"SELECT id, CAST(val AS DOUBLE) FROM {I} ORDER BY time",
            float_cols={1},
        )

    def test_fq_parity_107_char_length_cast_val_as_varchar(self):
        """CHAR_LENGTH CAST val AS VARCHAR

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CHAR_LENGTH(CAST(val AS VARCHAR(10))) FROM {L} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(CAST(val AS CHAR(10))) FROM {M} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(val::TEXT) FROM {P} ORDER BY ts",
            f"SELECT id, CHAR_LENGTH(CAST(val AS VARCHAR(10))) FROM {I} ORDER BY time",
        )

    def test_fq_parity_108_cast_score_as_bigint_truncates_decimal(self):
        """CAST score AS BIGINT truncates decimal

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, CAST(score AS BIGINT) FROM {L} ORDER BY ts",
            f"SELECT id, CAST(score AS SIGNED) FROM {M} ORDER BY ts",
            f"SELECT id, CAST(score AS BIGINT) FROM {P} ORDER BY ts",
            f"SELECT id, CAST(score AS BIGINT) FROM {I} ORDER BY time",
        )

    def test_fq_parity_109_in_subquery_val_in_north_vals(self):
        """IN subquery val in north vals

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val IN (SELECT val FROM {L} WHERE label = 'north') ORDER BY ts",
            f"SELECT id FROM {M} WHERE val IN (SELECT val FROM {M} WHERE label = 'north') ORDER BY ts",
            f"SELECT id FROM {P} WHERE val IN (SELECT val FROM {P} WHERE label = 'north') ORDER BY ts",
            f"SELECT id FROM {I} WHERE val IN (SELECT val FROM {I} WHERE region = 'north') ORDER BY time",
        )

    def test_fq_parity_110_not_in_subquery_exclude_east_vals(self):
        """NOT IN subquery exclude east vals

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val NOT IN (SELECT val FROM {L} WHERE label = 'east') ORDER BY ts",
            f"SELECT id FROM {M} WHERE val NOT IN (SELECT val FROM {M} WHERE label = 'east') ORDER BY ts",
            f"SELECT id FROM {P} WHERE val NOT IN (SELECT val FROM {P} WHERE label = 'east') ORDER BY ts",
            f"SELECT id FROM {I} WHERE val NOT IN (SELECT val FROM {I} WHERE region = 'east') ORDER BY time",
        )

    def test_fq_parity_111_exists_subquery_north_rows(self):
        """EXISTS subquery north rows

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} t1 WHERE EXISTS (SELECT 1 FROM {L} t2 WHERE t2.id = t1.id AND t2.label = 'north') ORDER BY ts",
            f"SELECT id FROM {M} t1 WHERE EXISTS (SELECT 1 FROM {M} t2 WHERE t2.id = t1.id AND t2.label = 'north') ORDER BY ts",
            f"SELECT id FROM {P} t1 WHERE EXISTS (SELECT 1 FROM {P} t2 WHERE t2.id = t1.id AND t2.label = 'north') ORDER BY ts",
            f"SELECT id FROM {I} t1 WHERE EXISTS (SELECT 1 FROM {I} t2 WHERE t2.id = t1.id AND t2.region = 'north') ORDER BY time",
        )

    def test_fq_parity_112_not_exists_subquery_exclude_south_rows(self):
        """NOT EXISTS subquery exclude south rows

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} t1 WHERE NOT EXISTS (SELECT 1 FROM {L} t2 WHERE t2.id = t1.id AND t2.label = 'south') ORDER BY ts",
            f"SELECT id FROM {M} t1 WHERE NOT EXISTS (SELECT 1 FROM {M} t2 WHERE t2.id = t1.id AND t2.label = 'south') ORDER BY ts",
            f"SELECT id FROM {P} t1 WHERE NOT EXISTS (SELECT 1 FROM {P} t2 WHERE t2.id = t1.id AND t2.label = 'south') ORDER BY ts",
            f"SELECT id FROM {I} t1 WHERE NOT EXISTS (SELECT 1 FROM {I} t2 WHERE t2.id = t1.id AND t2.region = 'south') ORDER BY time",
        )

    def test_fq_parity_113_all_subquery_val_all_low_vals(self):
        """ALL subquery val > ALL low vals

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val > ALL (SELECT val FROM {L} WHERE val < 20) ORDER BY ts",
            f"SELECT id FROM {M} WHERE val > ALL (SELECT val FROM {M} WHERE val < 20) ORDER BY ts",
            f"SELECT id FROM {P} WHERE val > ALL (SELECT val FROM {P} WHERE val < 20) ORDER BY ts",
            f"SELECT id FROM {I} WHERE val > ALL (SELECT val FROM {I} WHERE val < 20) ORDER BY time",
        )

    def test_fq_parity_114_any_subquery_val_any_low_vals(self):
        """ANY subquery val > ANY low vals

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val > ANY (SELECT val FROM {L} WHERE val < 30) ORDER BY ts",
            f"SELECT id FROM {M} WHERE val > ANY (SELECT val FROM {M} WHERE val < 30) ORDER BY ts",
            f"SELECT id FROM {P} WHERE val > ANY (SELECT val FROM {P} WHERE val < 30) ORDER BY ts",
            f"SELECT id FROM {I} WHERE val > ANY (SELECT val FROM {I} WHERE val < 30) ORDER BY time",
        )

    def test_fq_parity_115_some_subquery_val_some_mid_vals(self):
        """SOME subquery val >= SOME mid vals

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE val >= SOME (SELECT val FROM {L} WHERE val >= 30) ORDER BY ts",
            f"SELECT id FROM {M} WHERE val >= SOME (SELECT val FROM {M} WHERE val >= 30) ORDER BY ts",
            f"SELECT id FROM {P} WHERE val >= SOME (SELECT val FROM {P} WHERE val >= 30) ORDER BY ts",
            f"SELECT id FROM {I} WHERE val >= SOME (SELECT val FROM {I} WHERE val >= 30) ORDER BY time",
        )

    def test_fq_parity_116_scalar_subquery_avg_in_select(self):
        """scalar subquery AVG in SELECT

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val, (SELECT AVG(val) FROM {L}) AS avg_val FROM {L} ORDER BY ts",
            f"SELECT id, val, (SELECT AVG(val) FROM {M}) AS avg_val FROM {M} ORDER BY ts",
            f"SELECT id, val, (SELECT AVG(val) FROM {P}) AS avg_val FROM {P} ORDER BY ts",
            f"SELECT id, val, (SELECT AVG(val) FROM {I}) AS avg_val FROM {I} ORDER BY time",
            float_cols={2},
        )

    def test_fq_parity_117_scalar_subquery_avg_in_where_above_avg(self):
        """scalar subquery AVG in WHERE above avg

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val FROM {L} WHERE val > (SELECT AVG(val) FROM {L}) ORDER BY ts",
            f"SELECT id, val FROM {M} WHERE val > (SELECT AVG(val) FROM {M}) ORDER BY ts",
            f"SELECT id, val FROM {P} WHERE val > (SELECT AVG(val) FROM {P}) ORDER BY ts",
            f"SELECT id, val FROM {I} WHERE val > (SELECT AVG(val) FROM {I}) ORDER BY time",
        )

    def test_fq_parity_118_nested_subquery_avg_of_label_sums(self):
        """nested subquery AVG of label sums

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {L} GROUP BY label) sub",
            f"SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {M} GROUP BY label) sub",
            f"SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {P} GROUP BY label) sub",
            f"SELECT AVG(s) AS avg_sum FROM (SELECT SUM(val) AS s FROM {I} GROUP BY region) sub",
            float_cols={0},
            ordered=False,
        )

    def test_fq_parity_119_order_by_multiple_cols_label_val(self):
        """ORDER BY multiple cols label val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, label, val FROM {L} ORDER BY label, val",
            f"SELECT id, label, val FROM {M} ORDER BY label, val",
            f"SELECT id, label, val FROM {P} ORDER BY label, val",
            f"SELECT id, region AS label, val FROM {I} ORDER BY region, val",
        )

    def test_fq_parity_120_group_by_expression_val_div_20(self):
        """GROUP BY expression val div 20

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT val / 20 AS bucket, COUNT(*) FROM {L} GROUP BY val / 20 ORDER BY bucket",
            f"SELECT val / 20 AS bucket, COUNT(*) FROM {M} GROUP BY val / 20 ORDER BY bucket",
            f"SELECT val / 20 AS bucket, COUNT(*) FROM {P} GROUP BY val / 20 ORDER BY bucket",
            f"SELECT val / 20 AS bucket, COUNT(*) FROM {I} GROUP BY val / 20 ORDER BY bucket",
        )

    def test_fq_parity_121_union_all_duplicates_preserved(self):
        """UNION ALL duplicates preserved

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT val FROM {L} WHERE id <= 2 UNION ALL SELECT val FROM {L} WHERE id <= 2 ORDER BY val",
            f"SELECT val FROM {M} WHERE id <= 2 UNION ALL SELECT val FROM {M} WHERE id <= 2 ORDER BY val",
            f"SELECT val FROM {P} WHERE id <= 2 UNION ALL SELECT val FROM {P} WHERE id <= 2 ORDER BY val",
            f"SELECT val FROM {I} WHERE id <= 2 UNION ALL SELECT val FROM {I} WHERE id <= 2 ORDER BY val",
        )

    def test_fq_parity_122_union_deduplicated(self):
        """UNION deduplicated

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label FROM {L} WHERE id IN (1,3) UNION SELECT label FROM {L} WHERE id IN (1,4) ORDER BY label",
            f"SELECT label FROM {M} WHERE id IN (1,3) UNION SELECT label FROM {M} WHERE id IN (1,4) ORDER BY label",
            f"SELECT label FROM {P} WHERE id IN (1,3) UNION SELECT label FROM {P} WHERE id IN (1,4) ORDER BY label",
            f"SELECT region FROM {I} WHERE id IN (1,3) UNION SELECT region FROM {I} WHERE id IN (1,4) ORDER BY region",
        )

    def test_fq_parity_123_distinct_multi_col_label_val(self):
        """DISTINCT multi-col label val

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT DISTINCT label, val FROM {L} ORDER BY label, val",
            f"SELECT DISTINCT label, val FROM {M} ORDER BY label, val",
            f"SELECT DISTINCT label, val FROM {P} ORDER BY label, val",
            f"SELECT DISTINCT region, val FROM {I} ORDER BY region, val",
        )

    def test_fq_parity_124_column_alias_in_order_by(self):
        """column alias in ORDER BY

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, val * 2 AS dbl FROM {L} ORDER BY dbl",
            f"SELECT id, val * 2 AS dbl FROM {M} ORDER BY dbl",
            f"SELECT id, val * 2 AS dbl FROM {P} ORDER BY dbl",
            f"SELECT id, val * 2 AS dbl FROM {I} ORDER BY dbl",
        )

    def test_fq_parity_125_order_by_nulls_first(self):
        """ORDER BY NULLS FIRST

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, NULLIF(val, 30) AS v FROM {L} ORDER BY v ASC NULLS FIRST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {M} ORDER BY v ASC NULLS FIRST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {P} ORDER BY v ASC NULLS FIRST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {I} ORDER BY v ASC NULLS FIRST",
        )

    def test_fq_parity_126_order_by_nulls_last(self):
        """ORDER BY NULLS LAST

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, NULLIF(val, 30) AS v FROM {L} ORDER BY v DESC NULLS LAST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {M} ORDER BY v DESC NULLS LAST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {P} ORDER BY v DESC NULLS LAST",
            f"SELECT id, NULLIF(val, 30) AS v FROM {I} ORDER BY v DESC NULLS LAST",
        )

    def test_fq_parity_127_subquery_in_from_derived_table(self):
        """subquery in FROM derived table

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {L}) sub ORDER BY id",
            f"SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {M}) sub ORDER BY id",
            f"SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {P}) sub ORDER BY id",
            f"SELECT id, doubled FROM (SELECT id, val * 2 AS doubled FROM {I}) sub ORDER BY id",
        )

    def test_fq_parity_128_order_by_ordinal_position_1(self):
        """ORDER BY ordinal position 1

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(*) AS cnt FROM {L} GROUP BY label ORDER BY 2 DESC",
            f"SELECT label, COUNT(*) AS cnt FROM {M} GROUP BY label ORDER BY 2 DESC",
            f"SELECT label, COUNT(*) AS cnt FROM {P} GROUP BY label ORDER BY 2 DESC",
            f"SELECT region AS label, COUNT(*) AS cnt FROM {I} GROUP BY region ORDER BY 2 DESC",
        )

    def test_fq_parity_129_inner_join_same_table_on_id(self):
        """INNER JOIN same table on id

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id, a.val, b.label FROM {L} a INNER JOIN {L} b ON a.id = b.id ORDER BY a.id",
            f"SELECT a.id, a.val, b.label FROM {M} a INNER JOIN {M} b ON a.id = b.id ORDER BY a.id",
            f"SELECT a.id, a.val, b.label FROM {P} a INNER JOIN {P} b ON a.id = b.id ORDER BY a.id",
            f"SELECT a.id, a.val, b.region AS label FROM {I} a INNER JOIN {I} b ON a.id = b.id ORDER BY a.id",
        )

    def test_fq_parity_130_left_join_filtered(self):
        """LEFT JOIN filtered

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id, b.val FROM {L} a LEFT JOIN {L} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
            f"SELECT a.id, b.val FROM {M} a LEFT JOIN {M} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
            f"SELECT a.id, b.val FROM {P} a LEFT JOIN {P} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
            f"SELECT a.id, b.val FROM {I} a LEFT JOIN {I} b ON a.id = b.id AND b.val > 30 ORDER BY a.id",
        )

    def test_fq_parity_131_right_join_filtered(self):
        """RIGHT JOIN filtered

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id, b.val FROM {L} a RIGHT JOIN {L} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
            f"SELECT a.id, b.val FROM {M} a RIGHT JOIN {M} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
            f"SELECT a.id, b.val FROM {P} a RIGHT JOIN {P} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
            f"SELECT a.id, b.val FROM {I} a RIGHT JOIN {I} b ON a.id = b.id AND a.val < 30 ORDER BY b.id",
        )

    def test_fq_parity_132_full_outer_join(self):
        """FULL OUTER JOIN

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id AS aid, b.id AS bid FROM {L} a FULL OUTER JOIN {L} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM {M} a FULL OUTER JOIN {M} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM {P} a FULL OUTER JOIN {P} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM {I} a FULL OUTER JOIN {I} b ON a.id = b.id + 3 ORDER BY a.id, b.id",
        )

    def test_fq_parity_133_cross_join(self):
        """CROSS JOIN

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {L} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {L} WHERE id >= 4) b ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {M} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {M} WHERE id >= 4) b ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {P} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {P} WHERE id >= 4) b ORDER BY a.id, b.id",
            f"SELECT a.id AS aid, b.id AS bid FROM (SELECT id FROM {I} WHERE id <= 2) a CROSS JOIN (SELECT id FROM {I} WHERE id >= 4) b ORDER BY a.id, b.id",
        )

    def test_fq_parity_134_join_with_group_by_aggregate(self):
        """JOIN with GROUP BY aggregate

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {L} a INNER JOIN {L} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",
            f"SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {M} a INNER JOIN {M} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",
            f"SELECT a.label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {P} a INNER JOIN {P} b ON a.id = b.id GROUP BY a.label ORDER BY a.label",
            f"SELECT a.region AS label, COUNT(*) AS cnt, SUM(b.val) AS sv FROM {I} a INNER JOIN {I} b ON a.id = b.id GROUP BY a.region ORDER BY a.region",
        )

    def test_fq_parity_135_semi_join_via_in_subquery(self):
        """SEMI JOIN via IN subquery

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE id IN (SELECT id FROM {L} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {M} WHERE id IN (SELECT id FROM {M} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {P} WHERE id IN (SELECT id FROM {P} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {I} WHERE id IN (SELECT id FROM {I} WHERE val > 30) ORDER BY time",
        )

    def test_fq_parity_136_anti_join_via_not_in_subquery(self):
        """ANTI JOIN via NOT IN subquery

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE id NOT IN (SELECT id FROM {L} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {M} WHERE id NOT IN (SELECT id FROM {M} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {P} WHERE id NOT IN (SELECT id FROM {P} WHERE val > 30) ORDER BY id",
            f"SELECT id FROM {I} WHERE id NOT IN (SELECT id FROM {I} WHERE val > 30) ORDER BY time",
        )

    def test_fq_parity_137_3_way_join_self_triple(self):
        """3-way JOIN self triple

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT a.id, b.val, c.label FROM {L} a INNER JOIN {L} b ON a.id = b.id INNER JOIN {L} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
            f"SELECT a.id, b.val, c.label FROM {M} a INNER JOIN {M} b ON a.id = b.id INNER JOIN {M} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
            f"SELECT a.id, b.val, c.label FROM {P} a INNER JOIN {P} b ON a.id = b.id INNER JOIN {P} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
            f"SELECT a.id, b.val, c.region AS label FROM {I} a INNER JOIN {I} b ON a.id = b.id INNER JOIN {I} c ON b.id = c.id WHERE a.id <= 3 ORDER BY a.id",
        )

    def test_fq_parity_138_interval_1m_count_per_window(self):
        """INTERVAL 1m COUNT per window

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt FROM {L} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {M} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {P} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {I} INTERVAL(1m) ORDER BY _wstart",
        )

    def test_fq_parity_139_interval_1m_sum_val_per_window(self):
        """INTERVAL 1m SUM val per window

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(1m) ORDER BY _wstart",
        )

    def test_fq_parity_140_interval_1m_avg_score_per_window_float(self):
        """INTERVAL 1m AVG score per window float

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT AVG(score) AS asc FROM {L} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT AVG(score) AS asc FROM {M} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT AVG(score) AS asc FROM {P} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT AVG(score) AS asc FROM {I} INTERVAL(1m) ORDER BY _wstart",
            float_cols={0},
        )

    def test_fq_parity_141_interval_2m_count_and_sum(self):
        """INTERVAL 2m COUNT and SUM

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} INTERVAL(2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} INTERVAL(2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} INTERVAL(2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} INTERVAL(2m) ORDER BY _wstart",
        )

    def test_fq_parity_142_interval_30s_fill_null_shows_gaps(self):
        """INTERVAL 30s FILL NULL shows gaps

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(30s) FILL(NULL) ORDER BY _wstart",
        )

    def test_fq_parity_143_interval_30s_fill_value_0_fills_with_zero(self):
        """INTERVAL 30s FILL VALUE 0 fills with zero

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(30s) FILL(VALUE, 0) ORDER BY _wstart",
        )

    def test_fq_parity_144_interval_30s_fill_prev_forward_fill(self):
        """INTERVAL 30s FILL PREV forward fill

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(30s) FILL(PREV) ORDER BY _wstart",
        )

    def test_fq_parity_145_interval_30s_fill_next_backward_fill(self):
        """INTERVAL 30s FILL NEXT backward fill

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(30s) FILL(NEXT) ORDER BY _wstart",
        )

    def test_fq_parity_146_interval_30s_fill_linear_interpolation(self):
        """INTERVAL 30s FILL LINEAR interpolation

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(30s) FILL(LINEAR) ORDER BY _wstart",
            float_cols={0},
        )

    def test_fq_parity_147_interval_1m_partition_by_label(self):
        """INTERVAL 1m PARTITION BY label

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(*) AS cnt FROM {L} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart",
            f"SELECT label, COUNT(*) AS cnt FROM {M} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart",
            f"SELECT label, COUNT(*) AS cnt FROM {P} PARTITION BY label INTERVAL(1m) ORDER BY label, _wstart",
            f"SELECT region AS label, COUNT(*) AS cnt FROM {I} PARTITION BY region INTERVAL(1m) ORDER BY region, _wstart",
        )

    def test_fq_parity_148_session_window_30s_gap_5_sessions(self):
        """SESSION_WINDOW 30s gap 5 sessions

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt FROM {L} SESSION(ts, 30s) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {M} SESSION(ts, 30s) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {P} SESSION(ts, 30s) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {I} SESSION(time, 30s) ORDER BY _wstart",
        )

    def test_fq_parity_149_session_window_2m_gap_1_session(self):
        """SESSION_WINDOW 2m gap 1 session

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} SESSION(ts, 2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} SESSION(ts, 2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} SESSION(ts, 2m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} SESSION(time, 2m) ORDER BY _wstart",
        )

    def test_fq_parity_150_state_window_val_gte_30_two_states(self):
        """STATE_WINDOW val gte 30 two states

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} STATE_WINDOW(val >= 30) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} STATE_WINDOW(val >= 30) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} STATE_WINDOW(val >= 30) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} STATE_WINDOW(val >= 30) ORDER BY _wstart",
        )

    def test_fq_parity_151_state_window_label_per_label_group(self):
        """STATE_WINDOW label per label group

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt FROM {L} STATE_WINDOW(label) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {M} STATE_WINDOW(label) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {P} STATE_WINDOW(label) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {I} STATE_WINDOW(region) ORDER BY _wstart",
        )

    def test_fq_parity_152_event_window_start_val_gte_30_close_gte_50(self):
        """EVENT_WINDOW start val gte 30 close gte 50

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} EVENT_WINDOW START WHEN val >= 30 CLOSE WHEN val >= 50 ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} EVENT_WINDOW START WHEN val >= 30 CLOSE WHEN val >= 50 ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} EVENT_WINDOW START WHEN val >= 30 CLOSE WHEN val >= 50 ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} EVENT_WINDOW START WHEN val >= 30 CLOSE WHEN val >= 50 ORDER BY _wstart",
        )

    def test_fq_parity_153_count_window_2_rows_per_window(self):
        """COUNT_WINDOW 2 rows per window

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} COUNT_WINDOW(2) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} COUNT_WINDOW(2) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} COUNT_WINDOW(2) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} COUNT_WINDOW(2) ORDER BY _wstart",
        )

    def test_fq_parity_154_count_window_3_rows_per_window(self):
        """COUNT_WINDOW 3 rows per window

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {L} COUNT_WINDOW(3) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {M} COUNT_WINDOW(3) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {P} COUNT_WINDOW(3) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt, SUM(val) AS sv FROM {I} COUNT_WINDOW(3) ORDER BY _wstart",
        )

    def test_fq_parity_155_interval_1m_having_sum_filter(self):
        """INTERVAL 1m HAVING SUM filter

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT SUM(val) AS sv FROM {L} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {M} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {P} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",
            f"SELECT SUM(val) AS sv FROM {I} INTERVAL(1m) HAVING SUM(val) > 25 ORDER BY _wstart",
        )

    def test_fq_parity_156_interval_1m_wstart_wend_present_correct_count(self):
        """INTERVAL 1m wstart wend present correct count

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT COUNT(*) AS cnt FROM {L} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {M} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {P} INTERVAL(1m) ORDER BY _wstart",
            f"SELECT COUNT(*) AS cnt FROM {I} INTERVAL(1m) ORDER BY _wstart",
        )

    def test_fq_parity_157_combined_and_or_precedence(self):
        """combined AND OR precedence

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT id FROM {L} WHERE (val < 20 OR val > 40) AND label <> 'east' ORDER BY id",
            f"SELECT id FROM {M} WHERE (val < 20 OR val > 40) AND label <> 'east' ORDER BY id",
            f"SELECT id FROM {P} WHERE (val < 20 OR val > 40) AND label <> 'east' ORDER BY id",
            f"SELECT id FROM {I} WHERE (val < 20 OR val > 40) AND region <> 'east' ORDER BY id",
        )

    def test_fq_parity_158_aggregate_with_where_filter_and_order(self):
        """aggregate with WHERE filter and ORDER

        Catalog: - Query:FederatedResultParity

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-21 wpan Initial implementation
        """
        L, M, P, I = self._L, self._M, self._P, self._I
        self._assert_parity_all(
            f"SELECT label, COUNT(*), SUM(val), AVG(score) FROM {L} WHERE val >= 20 GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(*), SUM(val), AVG(score) FROM {M} WHERE val >= 20 GROUP BY label ORDER BY label",
            f"SELECT label, COUNT(*), SUM(val), AVG(score) FROM {P} WHERE val >= 20 GROUP BY label ORDER BY label",
            f"SELECT region AS label, COUNT(*), SUM(val), AVG(score) FROM {I} WHERE val >= 20 GROUP BY region ORDER BY region",
            float_cols={3},
        )
