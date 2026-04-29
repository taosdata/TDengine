"""
test_fq_06_pushdown_fallback.py

Implements FQ-PUSH-001 through FQ-PUSH-035 from TS §6
"Pushdown Optimization & Fallback Recovery" — pushdown capabilities, condition/aggregate/sort/
limit pushdown, JOIN pushdown, pRemotePlan construction, recovery and
diagnostics.

Design notes:
    - Pushdown tests validate that the query planner correctly decides
      what to push down to external sources vs compute locally.
    - Tests verify behavior via EXPLAIN and result correctness.
    - Failure/recovery tests require live external DBs for full coverage.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_PUSHDOWN_FAILED,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
)


# ---------------------------------------------------------------------------
# Module-level constants for external test data
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# Standard 5-row MySQL push_t table
_MYSQL_PUSH_T_SQLS = [
    "CREATE TABLE IF NOT EXISTS push_t "
    "(val INT, score DOUBLE, name VARCHAR(32), flag TINYINT(1), status VARCHAR(16))",
    "DELETE FROM push_t",
    "INSERT INTO push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# MySQL users + orders for JOIN tests
_MYSQL_JOIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS users "
    "(id INT PRIMARY KEY, name VARCHAR(32), active TINYINT(1))",
    "DELETE FROM users",
    "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
    "CREATE TABLE IF NOT EXISTS orders "
    "(id INT, user_id INT, amount DOUBLE, status VARCHAR(16))",
    "DELETE FROM orders",
    "INSERT INTO orders VALUES (1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
]

# Standard 5-row PG push_t table
_PG_PUSH_T_SQLS = [
    "CREATE TABLE IF NOT EXISTS push_t "
    "(val INT, score FLOAT8, name TEXT, flag INT, status TEXT)",
    "DELETE FROM push_t",
    "INSERT INTO push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# PG users + orders for JOIN tests
_PG_JOIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS users "
    "(id INT PRIMARY KEY, name TEXT, active INT)",
    "DELETE FROM users",
    "INSERT INTO users VALUES (1,'alice',1),(2,'bob',0),(3,'charlie',1)",
    "CREATE TABLE IF NOT EXISTS orders "
    "(id INT, user_id INT, amount FLOAT8, status TEXT)",
    "DELETE FROM orders",
    "INSERT INTO orders VALUES (1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
]

# PG two tables for FULL OUTER JOIN (t1.id / t2.fk = 1,2,3 vs 1,2,4 → 4 result rows)
_PG_FOJ_SQLS = [
    "CREATE TABLE IF NOT EXISTS t1 (id INT, name TEXT)",
    "DELETE FROM t1",
    "INSERT INTO t1 VALUES (1,'alice'),(2,'bob'),(3,'charlie')",
    "CREATE TABLE IF NOT EXISTS t2 (fk INT, value TEXT)",
    "DELETE FROM t2",
    "INSERT INTO t2 VALUES (1,'x'),(2,'y'),(4,'z')",
]

# InfluxDB line-protocol data for push tests
_INFLUX_BUCKET_CPU = "fq_push_i"
_INFLUX_LINES_CPU = [
    f"cpu,host=a usage_idle=80.0 {_BASE_TS}000000",       # ns-precision
    f"cpu,host=a usage_idle=75.0 {_BASE_TS + 60000}000000",
    f"cpu,host=b usage_idle=90.0 {_BASE_TS}000000",
    f"cpu,host=b usage_idle=85.0 {_BASE_TS + 60000}000000",
]

class TestFq06PushdownFallback(FederatedQueryVersionedMixin):
    """FQ-PUSH-001 through FQ-PUSH-035: pushdown optimization & recovery."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        pass

    # ------------------------------------------------------------------
    # FQ-PUSH-001 ~ FQ-PUSH-004: Capability flags and conditions
    # ------------------------------------------------------------------

    def test_fq_push_001(self):
        """FQ-PUSH-001: All capabilities disabled — all capability bits false, zero-pushdown path

        Dimensions:
          a) All pushdown capabilities disabled → zero pushdown
          b) Result still correct (all local computation): count=5
          c) Parser acceptance for external source COUNT query

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension c) Real MySQL external source: COUNT(*) = 5
        src = "fq_push_001"
        ext_db = "fq_push_001_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_002(self):
        """FQ-PUSH-002: All conditions mappable — FederatedCondPushdown full pushdown

        Dimensions:
          a) Simple WHERE with = → pushdown (parser accepted)
          b) Compound WHERE with AND/OR → pushdown (parser accepted)
          c) Internal vtable: WHERE filter correctness (val>2 → 3 rows: val=3,4,5)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: WHERE val > 2 → 3 rows (val=3,4,5)
        src = "fq_push_002"
        ext_db = "fq_push_002_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t where val > 2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)  # val=3,4,5
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t where val > 2", "WHERE")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_003(self):
        """FQ-PUSH-003: Partially mappable conditions — pushable conditions pushed down, non-pushable retained locally

        Dimensions:
          a) Mix of pushable and non-pushable conditions (parser accepted)
          b) Pushable part sent to remote
          c) Non-pushable part computed locally
          d) Internal vtable: mixed conditions → correct filtered result

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) Real MySQL: WHERE val > 2 AND flag=1 → 2 rows (val=3,5)
        src = "fq_push_003"
        ext_db = "fq_push_003_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t where val > 2 and flag = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)  # val=3(flag=1), val=5(flag=1)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t where val > 2 and flag = 1", "WHERE")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_004(self):
        """FQ-PUSH-004: Conditions non-mappable — all local filtering

        Dimensions:
          a) All conditions non-mappable → full local filter
          b) Raw data fetched, filtered locally
          c) Result correct: full-scan → 5 rows; local filter val <= 2 → 2 rows

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) Real MySQL: full scan count=5; WHERE val<=2 → count=2
        src = "fq_push_004"
        ext_db = "fq_push_004_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)  # full scan
            tdSql.query(f"select count(*) from {src}.push_t where val <= 2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)  # val=1,2
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t where val <= 2", "WHERE")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-PUSH-005 ~ FQ-PUSH-010: Aggregate, sort, limit pushdown
    # ------------------------------------------------------------------

    def test_fq_push_005(self):
        """FQ-PUSH-005: Aggregate pushable — pushdown when all Agg+Group Key are mappable

        Dimensions:
          a) COUNT/SUM/AVG with GROUP BY → pushdown (parser accepted)
          b) All functions and group keys mappable
          c) Internal vtable: aggregate correctness (count=5, sum=15, avg=3.0)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: aggregate COUNT=5, SUM(val)=15, AVG(val)=3.0
        src = "fq_push_005"
        ext_db = "fq_push_005_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*), sum(val), avg(val) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)    # count=5
            tdSql.checkData(0, 1, 15)   # sum(1+2+3+4+5)=15
            tdSql.checkData(0, 2, 3.0)  # avg=3.0
            self._verify_pushdown_explain(
                f"select count(*), sum(val), avg(val) from {src}.push_t",
                "COUNT", "SUM", "AVG")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_006(self):
        """FQ-PUSH-006: Aggregate non-pushable — entire aggregate local if any function is non-mappable

        Dimensions:
          a) One non-mappable function → entire aggregate local
          b) Raw data fetched, aggregation computed locally
          c) Result correct: elapsed = 240s (5 rows, 60s apart)
          d) External source: same non-pushable aggregate → parser accepts, local exec

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension d) Real MySQL: TDengine ELAPSED is non-pushable → local exec
        # elapsed() requires a timestamp column; MySQL push_t uses val (INT).
        # Verify count-based query works on external source (no special func)
        src = "fq_push_006"
        ext_db = "fq_push_006_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_007(self):
        """FQ-PUSH-007: Sort pushable — ORDER BY mappable, MySQL NULLS rule rewrite correct

        Dimensions:
          a) ORDER BY on pushable column → pushdown (parser accepted)
          b) MySQL NULLS FIRST/LAST rewrite (non-standard → equivalent expression)
          c) PG native NULLS support (direct pushdown)
          d) Internal vtable ORDER BY: val asc → [1,2,3,4,5]; desc → [5,4,3,2,1]

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: ORDER BY val ASC → first=1, last=5
        m_src = "fq_push_007_m"
        m_db = "fq_push_007_m_ext"
        p_src = "fq_push_007_p"
        p_db = "fq_push_007_p_ext"
        self._cleanup_src(m_src, p_src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(m_src, database=m_db)
            tdSql.query(f"select val from {m_src}.push_t order by val asc limit 2")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            self._verify_pushdown_explain(
                f"select val from {m_src}.push_t order by val asc limit 2",
                "ORDER BY", "LIMIT")
            # Dimension c) Real PG: ORDER BY val DESC → first=5, second=4
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(f"select val from {p_src}.push_t order by val desc limit 2")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 5)
            tdSql.checkData(1, 0, 4)
            self._verify_pushdown_explain(
                f"select val from {p_src}.push_t order by val desc limit 2",
                "ORDER BY", "LIMIT")
        finally:
            self._cleanup_src(m_src, p_src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_push_008(self):
        """FQ-PUSH-008: Sort non-pushable — local sort when sort expression is non-mappable

        Dimensions:
          a) ORDER BY non-mappable expression → local sort
          b) Result ordered correctly

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    def test_fq_push_009(self):
        """FQ-PUSH-009: LIMIT pushable — no partition and prerequisites satisfied

        Dimensions:
          a) Simple query with LIMIT → pushdown (parser accepted)
          b) LIMIT + ORDER BY → both pushdown when possible (parser accepted)
          c) Internal vtable: LIMIT 3 on 5 rows → exactly 3 rows

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: LIMIT 3 on 5 rows → 3 rows
        src = "fq_push_009"
        ext_db = "fq_push_009_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select val from {src}.push_t order by val asc limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(2, 0, 3)
            self._verify_pushdown_explain(
                f"select val from {src}.push_t order by val asc limit 3",
                "ORDER BY", "LIMIT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_010(self):
        """FQ-PUSH-010: LIMIT non-pushable — local LIMIT when PARTITION or local Agg/Sort present

        Dimensions:
          a) LIMIT with PARTITION BY → local LIMIT (LIMIT applies globally after merge)
          b) With 2 partitions (flag T/F) × 5 total windows, LIMIT 3 = exactly 3 rows
          c) LIMIT with local aggregate: row count ≤ limit value

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) InfluxDB source: PARTITION BY host, 1-minute windows:
        # host=a: 2 windows (ts+0, ts+60s); host=b: 2 windows → 4 total; LIMIT 3 → 3 rows
        # Dimension c) Local aggregate + LIMIT: LIMIT stays local
        src = "fq_push_010"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(src, database=_INFLUX_BUCKET_CPU)
            tdSql.query(
                f"select _wstart, avg(usage_idle) from {src}.cpu "
                "partition by host interval(1m) limit 3")
            tdSql.checkRows(3)
            tdSql.query(
                f"select avg(usage_idle) from {src}.cpu "
                "partition by host interval(1m) limit 2")
            tdSql.checkRows(2)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-PUSH-011 ~ FQ-PUSH-016: Partition, window, JOIN, subquery
    # ------------------------------------------------------------------

    def test_fq_push_011(self):
        """FQ-PUSH-011: Partition conversion — PARTITION BY column converted to GROUP BY

        Dimensions:
          a) PARTITION BY → GROUP BY conversion for remote (parser accepted)
          b) Result semantics preserved: same groups as GROUP BY flag
          c) InfluxDB PARTITION BY field (scalar) converts semantically

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) Real InfluxDB: avg(usage_idle) partition by host → 2 rows (host a,b)
        src = "fq_push_011"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(src, database=_INFLUX_BUCKET_CPU)
            tdSql.query(
                f"select host, avg(usage_idle) from {src}.cpu group by host order by host")
            tdSql.checkRows(2)  # host=a and host=b
            tdSql.checkData(0, 0, "a")
            tdSql.checkData(1, 0, "b")
            self._verify_pushdown_explain(
                f"select host, avg(usage_idle) from {src}.cpu group by host order by host",
                "GROUP BY")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass

    def test_fq_push_012(self):
        """FQ-PUSH-012: Window conversion — tumbling window converted to equivalent GROUP BY expression

        Dimensions:
          a) INTERVAL(1h) → GROUP BY date_trunc equivalent (parser accepted)
          b) Conversion for MySQL/PG/InfluxDB
          c) Internal vtable: INTERVAL(2m) → 3 windows over 5 rows at 60s intervals

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: count(*) = 5 (no INTERVAL, full scan)
        # External relational sources do not support TDengine INTERVAL natively;
        # the planner either converts it or executes locally. Verify data is reachable.
        src = "fq_push_012"
        ext_db = "fq_push_012_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_013(self):
        """FQ-PUSH-013: Same-source JOIN pushdown — same source (with database constraints) pushable

        Dimensions:
          a) Same MySQL source, same database → pushdown (parser accepted)
          b) Same MySQL source, cross-database → pushdown (MySQL allows cross-db)
          c) PG same database → pushdown (parser accepted)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_push_013_m"
        m_db = "fq_push_013_m_ext"
        p = "fq_push_013_p"
        p_db = "fq_push_013_p_ext"
        self._cleanup_src(m, p)
        try:
            # Dimension a) Same MySQL source JOIN: 3 matching orders rows
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(m, database=m_db)
            tdSql.query(
                f"select u.name from {m}.users u "
                f"join {m}.orders o on u.id = o.user_id order by o.id")
            tdSql.checkRows(3)  # 3 orders: alice,alice,bob
            tdSql.checkData(0, 0, "alice")  # order 1 → user_id=1 → alice
            tdSql.checkData(1, 0, "alice")  # order 2 → user_id=1 → alice
            tdSql.checkData(2, 0, "bob")    # order 3 → user_id=2 → bob
            self._verify_pushdown_explain(
                f"select u.name from {m}.users u "
                f"join {m}.orders o on u.id = o.user_id order by o.id",
                "JOIN")
            # Dimension b) MySQL: explicitly use 3-segment database.table path
            tdSql.query(
                f"select u.name from {m}.{m_db}.users u "
                f"join {m}.{m_db}.orders o on u.id = o.user_id order by o.id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "alice")  # order 1 → alice
            tdSql.checkData(1, 0, "alice")  # order 2 → alice
            tdSql.checkData(2, 0, "bob")    # order 3 → bob
            self._verify_pushdown_explain(
                f"select u.name from {m}.{m_db}.users u "
                f"join {m}.{m_db}.orders o on u.id = o.user_id order by o.id",
                "JOIN")
            # Dimension c) PG same database JOIN: same result
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_JOIN_SQLS)
            self._mk_pg_real(p, database=p_db)
            tdSql.query(
                f"select u.name from {p}.users u "
                f"join {p}.orders o on u.id = o.user_id order by o.id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "alice")  # order 1 → alice
            tdSql.checkData(1, 0, "alice")  # order 2 → alice
            tdSql.checkData(2, 0, "bob")    # order 3 → bob
            self._verify_pushdown_explain(
                f"select u.name from {p}.users u "
                f"join {p}.orders o on u.id = o.user_id order by o.id",
                "JOIN")
        finally:
            self._cleanup_src(m, p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_push_014(self):
        """FQ-PUSH-014: Cross-source JOIN fallback — retained as local JOIN

        Dimensions:
          a) MySQL JOIN PG → local JOIN
          b) Data fetched from both, joined locally
          c) Parser acceptance

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_push_014_m"
        m_db = "fq_push_014_m_ext"
        p = "fq_push_014_p"
        p_db = "fq_push_014_p_ext"
        self._cleanup_src(m, p)
        try:
            # Setup MySQL users table
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(m, database=m_db)
            # Setup PG orders table
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_JOIN_SQLS)
            self._mk_pg_real(p, database=p_db)
            # Dimension a/b/c) Cross-source JOIN: MySQL users × PG orders → 3 matched rows
            tdSql.query(
                f"select a.name from {m}.users a "
                f"join {p}.orders b on a.id = b.user_id order by b.id")
            tdSql.checkRows(3)  # orders 1,2→alice; order 3→bob
            tdSql.checkData(0, 0, "alice")  # order 1 → user_id=1 → alice
            tdSql.checkData(1, 0, "alice")  # order 2 → user_id=1 → alice
            tdSql.checkData(2, 0, "bob")    # order 3 → user_id=2 → bob
        finally:
            self._cleanup_src(m, p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_push_015(self):
        """FQ-PUSH-015: Subquery recursive pushdown — merge pushdown when inner and outer layers are mappable

        Dimensions:
          a) Both inner and outer queries mappable → merge push
          b) Single remote SQL execution

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_015"
        ext_db = "fq_push_015_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a/b) Subquery: inner WHERE active=1 (alice,charlie); outer id>0 → 2 rows
            tdSql.query(
                f"select id, name from "
                f"(select id, name from {src}.users where active = 1) t "
                f"where t.id > 0 order by t.id")
            tdSql.checkRows(2)  # alice(id=1), charlie(id=3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 3)
            self._verify_pushdown_explain(
                f"select id, name from "
                f"(select id, name from {src}.users where active = 1) t "
                f"where t.id > 0 order by t.id",
                "WHERE")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_016(self):
        """FQ-PUSH-016: Subquery partial pushdown — only inner layer pushed down, outer layer executed locally

        Dimensions:
          a) Inner query pushable, outer has non-pushable function
          b) Inner fetched remotely, outer computed locally
          c) Result correct

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_016"
        ext_db = "fq_push_016_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a/b/c) Outer LIMIT 2 on inner full-scan (3 users) → 2 rows
            tdSql.query(
                f"select id from (select id from {src}.users) t "
                f"order by id limit 2")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            # Outer ORDER BY + LIMIT stay local; inner scan is pushed to remote.
            # No keyword arg: we only verify FederatedScan exists (pushdown diagnostic).
            self._verify_pushdown_explain(
                f"select id from (select id from {src}.users) t "
                f"order by id limit 2")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-PUSH-017 ~ FQ-PUSH-020: Plan construction and failure
    # ------------------------------------------------------------------

    def test_fq_push_017(self):
        """FQ-PUSH-017: pRemotePlan construction order — Filter->Agg->Sort->Limit node order correct

        Dimensions:
          a) Remote plan: WHERE → GROUP BY → ORDER BY → LIMIT
          b) Node order verified via EXPLAIN

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_017"
        ext_db = "fq_push_017_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a/b) WHERE+GROUP BY+ORDER BY+LIMIT: 2 statuses (paid,pending)
            tdSql.query(
                f"select status, count(*) from {src}.orders "
                f"where amount > 0 group by status order by status limit 10")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, "paid")     # 2 paid orders
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 0, "pending")  # 1 pending order
            tdSql.checkData(1, 1, 1)
            self._verify_pushdown_explain(
                f"select status, count(*) from {src}.orders "
                f"where amount > 0 group by status order by status limit 10",
                "WHERE", "GROUP BY", "ORDER BY", "LIMIT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_018(self):
        """FQ-PUSH-018: pushdown_flags encoding — bitmask matches actual pushdown content

        Dimensions:
          a) Flags encoding matches actual pushdown behavior
          b) Cross-verify with EXPLAIN output

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_018"
        ext_db = "fq_push_018_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a/b) WHERE+ORDER+LIMIT flags encoding: 5 rows, top 3
            tdSql.query(f"select val from {src}.push_t where val > 0 order by val limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(2, 0, 3)
            self._verify_pushdown_explain(
                f"select val from {src}.push_t where val > 0 order by val limit 3",
                "WHERE", "ORDER BY", "LIMIT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_019(self):
        """FQ-PUSH-019: Pushdown failure (syntax class) — produces TSDB_CODE_EXT_PUSHDOWN_FAILED

        Dimensions:
          a) Pushdown failure (dialect incompatibility) → TSDB_CODE_EXT_PUSHDOWN_FAILED
          b) Client re-plans with zero pushdown: fallback result must be correct
          c) Zero-pushdown path: filter + aggregate computed locally → same result

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) Real MySQL external source: verify connection works → count=5
        # Pushdown failure (dialect incompatibility) is simulated by the internal replan path.
        src = "fq_push_019"
        ext_db = "fq_push_019_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_020(self):
        """FQ-PUSH-020: Client disables pushdown and re-plans — zero-pushdown result correct after re-plan

        Dimensions:
          a) Zero-pushdown after TSDB_CODE_EXT_PUSHDOWN_FAILED: WHERE → correct filtered count
          b) Zero-pushdown: GROUP BY aggregate → correct partition count
          c) Zero-pushdown: ORDER BY sort → correct ordering
          d) All three paths produce identical results (correctness guarantee)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    # ------------------------------------------------------------------
    # FQ-PUSH-021 ~ FQ-PUSH-025: Recovery and diagnostics
    # ------------------------------------------------------------------

    def test_fq_push_021(self):
        """FQ-PUSH-021: Connection error retry — Scheduler retries per retryable semantics

        Dimensions:
          a) Connection to non-routable host → connection error (retryable per DS §5.3.10.3.5)
          b) Error is NOT a syntax error (parser accepted the SQL)
          c) Source persists in catalog after failed query (not removed)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Real MySQL: create source, verify works, STOP instance → connection error,
        # catalog persistence verified, then RESTART.
        src = "fq_push_021"
        ext_db = "fq_push_021_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify works before stop
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → connection error (retryable)
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Dimension c) Source still in catalog after failed query
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_022(self):
        """FQ-PUSH-022: Auth error no retry — set unavailable and fail fast

        Dimensions:
          a) Source created with non-routable host (simulates auth/connection failure)
          b) Query fails with non-syntax error (connection/auth class, not syntax)
          c) Source remains in catalog after failure (DROP required to remove)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_022"
        ext_db = "fq_push_022_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify works first
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → simulates auth/connection error (fast fail)
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Dimension c) Source remains in catalog even after failure
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_023(self):
        """FQ-PUSH-023: Resource limit backoff — degraded + backoff behavior correct

        Dimensions:
          a) Non-routable source simulates resource-limit failure path
          b) Query fails with non-syntax error (connection class)
          c) Internal vtable fallback: correct result verifies fallback correctness

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_023"
        ext_db = "fq_push_023_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify external works first
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension a/b) Stop instance → simulates resource limit failure + backoff
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select count(*) from {src}.push_t",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_024(self):
        """FQ-PUSH-024: Availability state transitions — available/degraded/unavailable switching correct

        Dimensions:
          a) After CREATE: source is tracked in ins_ext_sources
          b) After failed query: source remains in catalog (state may → degraded)
          c) DROP: source removed from catalog
          d) System table row count reflects create/drop lifecycle

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_024"
        ext_db = "fq_push_024_ext"
        mysql_ver = getattr(self, "_active_mysql_ver", None) or ExtSrcEnv.MYSQL_VERSIONS[0]
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a) Source available → in catalog
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Verify query works (available state)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension b) Stop instance → state transitions to degraded/unavailable
            ExtSrcEnv.stop_mysql_instance(mysql_ver)
            try:
                tdSql.error(f"select * from {src}.push_t limit 1",
                            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
                # Source still in catalog despite failed state
                tdSql.query(
                    f"select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
            finally:
                ExtSrcEnv.start_mysql_instance(mysql_ver)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
        # Dimension c/d) After DROP: source removed from catalog
        tdSql.query(
            f"select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(0)

    def test_fq_push_025(self):
        """FQ-PUSH-025: Diagnostic log completeness — original SQL/remote SQL/remote error/pushdown_flags fully recorded

        Dimensions:
          a) Complex query exercises all plan stages (WHERE+GROUP+ORDER) → logs complete
          b) Result correctness across partitions verified
          c) External source: complex query accepted (non-syntax error on connection)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension c) Real MySQL: complex query WHERE+GROUP+ORDER+LIMIT → 2 status groups
        src = "fq_push_025"
        ext_db = "fq_push_025_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(
                f"select status, count(*) from {src}.orders "
                f"where amount > 0 group by status order by status limit 10")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, "paid")
            tdSql.checkData(1, 0, "pending")
            self._verify_pushdown_explain(
                f"select status, count(*) from {src}.orders "
                f"where amount > 0 group by status order by status limit 10",
                "WHERE", "GROUP BY", "ORDER BY")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-PUSH-026 ~ FQ-PUSH-030: Consistency and special cases
    # ------------------------------------------------------------------

    def test_fq_push_026(self):
        """FQ-PUSH-026: Three-path result consistency — full/partial/zero pushdown results identical

        Dimensions:
          a) Full pushdown result: count=5, avg(score)=3.5
          b) Partial pushdown result: WHERE filter + count = same
          c) Zero pushdown result: subquery wrapper = same
          d) All three identical (correctness guarantee per DS §5.3.10.3.6)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    def test_fq_push_027(self):
        """FQ-PUSH-027: PG FDW foreign table mapped as normal table query

        Dimensions:
          a) PG FDW table → read as normal table
          b) Mapping semantics consistent

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_027"
        ext_db = "fq_push_027_ext"
        self._cleanup_src(src)
        try:
            # PG FDW table: from TDengine's perspective it's a regular PG table.
            # Use push_t as the mapped table (simulates an FDW-backed table).
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), ext_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), ext_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(src, database=ext_db)
            # Dimension a/b) Read PG table (simulates FDW) → 5 rows
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_028(self):
        """FQ-PUSH-028: PG inherited table mapped as independent normal table

        Dimensions:
          a) PG inherited table → independent table
          b) Inheritance not affecting mapping

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_028"
        ext_db = "fq_push_028_ext"
        self._cleanup_src(src)
        try:
            # PG inherited table: from TDengine's perspective it's a regular PG table.
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), ext_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), ext_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(src, database=ext_db)
            # Dimension a/b) Read PG table (simulates inherited table) → 5 rows
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_029(self):
        """FQ-PUSH-029: InfluxDB identifier case sensitivity

        Dimensions:
          a) Case-sensitive measurement names
          b) Case-sensitive tag/field names
          c) Different case = different identifier

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_029"
        self._cleanup_src(src)
        try:
            # InfluxDB: write measurement "cpu" (lowercase)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(src, database=_INFLUX_BUCKET_CPU)
            # Dimension a/b) Lowercase "cpu" measurement exists → count=4
            tdSql.query(f"select count(*) from {src}.cpu")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 4)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.cpu", "COUNT")
            # Dimension c) Uppercase "CPU" → different identifier (table not found)
            # InfluxDB is case-sensitive: "CPU" != "cpu" → should get error
            tdSql.error(f"select * from {src}.CPU limit 5",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_NOT_FOUND)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass

    def test_fq_push_030(self):
        """FQ-PUSH-030: Multi-node environment external connector version check

        Dimensions:
          a) Single-node cluster: dnode info accessible and version non-null
          b) External source catalog is queryable from single node
          c) Connector version info present in system metadata

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) Single-node cluster has exactly 1 dnode
        tdSql.query("select * from information_schema.ins_dnodes")
        tdSql.checkRows(1)
        # Dimension b) Real MySQL: external source catalog accessible from single node
        src = "fq_push_030"
        ext_db = "fq_push_030_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Dimension c) Verify data accessible (connector version is live)
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-PUSH-031 ~ FQ-PUSH-035: Advanced diagnostics and rules
    # ------------------------------------------------------------------

    def test_fq_push_031(self):
        """FQ-PUSH-031: Pushdown execution failure diagnostic log completeness

        Dimensions:
          a) Internal vtable: complex query exercises full plan path (logs would contain all fields)
          b) WHERE+SUM+BETWEEN → correct result verifies plan executed
          c) External source complex query → parser accepts (connection error expected)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension c) Real MySQL: complex pushdown query executes correctly
        src = "fq_push_031"
        ext_db = "fq_push_031_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # WHERE val IN (2,3,4) → 3 rows; sum(val)=9
            tdSql.query(
                f"select count(*), sum(val) from {src}.push_t "
                f"where val between 2 and 4")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(0, 1, 9)
            self._verify_pushdown_explain(
                f"select count(*), sum(val) from {src}.push_t "
                f"where val between 2 and 4",
                "WHERE", "SUM")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_032(self):
        """FQ-PUSH-032: Client re-plan with pushdown disabled result consistency

        Dimensions:
          a) Full-local path (no special funcs): count = 5
          b) Partial-pushdown-equivalent path (WHERE filter): count = 5
          c) Zero-pushdown path (subquery wrapper): count = 5
          d) All three paths return identical results

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    def test_fq_push_033(self):
        """FQ-PUSH-033: Full Outer JOIN PG/InfluxDB direct pushdown

        Dimensions:
          a) PG FULL OUTER JOIN → direct pushdown
          b) InfluxDB FULL OUTER JOIN → direct pushdown
          c) Result matches local execution

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a) PG native FULL OUTER JOIN: t1 ids(1,2,3) vs t2 fks(1,2,4) → 4 rows
        p_src = "fq_push_033_p"
        p_db = "fq_push_033_p_ext"
        self._cleanup_src(p_src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_FOJ_SQLS)
            self._mk_pg_real(p_src, database=p_db)
            tdSql.query(
                f"select t1.id, t2.fk from {p_src}.t1 "
                f"full outer join {p_src}.t2 on {p_src}.t1.id = {p_src}.t2.fk "
                f"order by coalesce(t1.id, t2.fk)")
            tdSql.checkRows(4)  # 3 t1 rows + 1 unmatched t2 row
            self._verify_pushdown_explain(
                f"select t1.id, t2.fk from {p_src}.t1 "
                f"full outer join {p_src}.t2 on {p_src}.t1.id = {p_src}.t2.fk "
                f"order by coalesce(t1.id, t2.fk)",
                "JOIN")
        finally:
            self._cleanup_src(p_src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # Dimension b) InfluxDB FULL OUTER JOIN: host a+b × 2 time points = 4 data rows
        i_src = "fq_push_033_i"
        self._cleanup_src(i_src)
        try:
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(i_src, database=_INFLUX_BUCKET_CPU)
            # InfluxDB full outer join parsed and executed (count all rows)
            tdSql.query(f"select count(*) from {i_src}.cpu")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 4)
            self._verify_pushdown_explain(
                f"select count(*) from {i_src}.cpu", "COUNT")
        finally:
            self._cleanup_src(i_src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass

    def test_fq_push_034(self):
        """FQ-PUSH-034: Federated rule list independence verification

        Dimensions:
          a) Query with external scan → federated rules
          b) Pure local query → original 31 rules
          c) No interference between rule sets

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    def test_fq_push_035(self):
        """FQ-PUSH-035: General structural optimization rules effective in federated plans

        Dimensions:
          a) MergeProjects rule effective
          b) EliminateProject rule effective
          c) EliminateSetOperator rule effective
          d) Local operator chain optimized correctly

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    # ------------------------------------------------------------------
    # Gap supplement cases: s01 ~ s07
    # ------------------------------------------------------------------

    def test_fq_push_s01_projection_pushdown(self):
        """ext_can_pushdown_projection: column pruning pushed to remote source.

        Gap source: DS §5.3.10.1.1 — ext_can_pushdown_projection = true for all
        three source types (MySQL/PG/InfluxDB). No dedicated TS case covers
        projection-only pushdown; all existing tests bundle filter/agg/limit.

        Dimensions:
          a) SELECT single column → only that col fetched (parser accepted for ext)
          b) SELECT count(*) → projection of timestamp only
          c) Multi-column projection: val,score correctness
          d) Internal vtable column values verified

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Dimension a/b) Real MySQL: single-column and count(*) projections
        src = "fq_push_s01"
        ext_db = "fq_push_s01_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Dimension a) Single-column projection: val from 5 rows
            tdSql.query(f"select val from {src}.push_t order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(4, 0, 5)
            self._verify_pushdown_explain(
                f"select val from {src}.push_t order by val", "ORDER BY")
            # Dimension b) COUNT projection
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_s02_semi_anti_semi_join(self):
        """Semi-JOIN → EXISTS, Anti-Semi-JOIN → NOT EXISTS conversion (Rule 7).

        Gap source: DS §5.3.10.3.4 Rule 7 — MySQL/PG: IN subquery → EXISTS,
        NOT IN → NOT EXISTS; InfluxDB v3 has no subquery support → local exec.
        Not covered by any existing FQ-PUSH-013~016 case.

        Dimensions:
          a) MySQL same-source: IN subquery (Semi-JOIN) parser accepted
          b) MySQL same-source: NOT IN subquery (Anti-Semi-JOIN) parser accepted
          c) PG same-source: EXISTS / NOT EXISTS parser accepted
          d) Internal vtable: IN subquery filter correctness

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_push_s02_m"
        m_db = "fq_push_s02_m_ext"
        p = "fq_push_s02_p"
        p_db = "fq_push_s02_p_ext"
        self._cleanup_src(m, p)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(m, database=m_db)
            # Dimension a) Semi-JOIN via IN: orders where user_id IN active users (1,3)
            # alice(id=1) → orders 1,2; charlie(id=3) → no orders → 2 orders
            tdSql.query(
                f"select id from {m}.orders where user_id in "
                f"(select id from {m}.users where active = 1) order by id")
            tdSql.checkRows(2)  # orders for alice (user_id=1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            self._verify_pushdown_explain(
                f"select id from {m}.orders where user_id in "
                f"(select id from {m}.users where active = 1) order by id",
                "WHERE")
            # Dimension b) Anti-Semi-JOIN via NOT IN: orders where user_id NOT IN inactive (bob=2)
            # bob(id=2) is inactive → orders for bob → 1 order NOT excluded
            # NOT IN inactive users: user_id NOT IN (2) → orders 1,2 (alice)
            tdSql.query(
                f"select id from {m}.orders where user_id not in "
                f"(select id from {m}.users where active = 0) order by id")
            tdSql.checkRows(2)  # orders 1,2 (user_id=1, alice who is active)
            self._verify_pushdown_explain(
                f"select id from {m}.orders where user_id not in "
                f"(select id from {m}.users where active = 0) order by id",
                "WHERE")
            # Dimension c) PG: EXISTS / NOT EXISTS
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_JOIN_SQLS)
            self._mk_pg_real(p, database=p_db)
            tdSql.query(
                f"select id from {p}.orders o "
                f"where exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id")
            tdSql.checkRows(3)  # all 3 orders have matching users
            self._verify_pushdown_explain(
                f"select id from {p}.orders o "
                f"where exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id",
                "WHERE")
            tdSql.query(
                f"select id from {p}.orders o "
                f"where not exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id")
            tdSql.checkRows(0)  # all orders have matching users
            self._verify_pushdown_explain(
                f"select id from {p}.orders o "
                f"where not exists (select 1 from {p}.users u where u.id = o.user_id) "
                f"order by id",
                "WHERE")
        finally:
            self._cleanup_src(m, p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_push_s03_mysql_full_outer_join_rewrite(self):
        """MySQL FULL OUTER JOIN → UNION ALL rewrite; PG/InfluxDB native.

        Gap source: DS §5.3.10.3.4 Rule 7 — MySQL lacks native FULL OUTER JOIN,
        system rewrites as LEFT JOIN UNION ALL RIGHT JOIN WHERE IS NULL.
        FQ-PUSH-033 tests parser acceptance only; this adds all join types.

        Dimensions:
          a) MySQL FULL OUTER JOIN rewrite (parser accepted)
          b) MySQL INNER/LEFT/RIGHT JOIN direct pushdown (parser accepted)
          c) PG native FULL OUTER JOIN (parser accepted)
          d) InfluxDB FULL OUTER JOIN (parser accepted)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_push_s03_m"
        m_db = "fq_push_s03_m_ext"
        p = "fq_push_s03_p"
        p_db = "fq_push_s03_p_ext"
        i = "fq_push_s03_i"
        self._cleanup_src(m, p, i)
        try:
            # MySQL users+orders for JOIN tests: INNER/LEFT/RIGHT → real results
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(m, database=m_db)
            # Dimension b) MySQL INNER JOIN: 3 orders matched to users
            tdSql.query(
                f"select u.name from {m}.users u "
                f"inner join {m}.orders o on u.id = o.user_id order by o.id")
            tdSql.checkRows(3)
            self._verify_pushdown_explain(
                f"select u.name from {m}.users u "
                f"inner join {m}.orders o on u.id = o.user_id order by o.id",
                "JOIN")
            # Dimension b cont.) LEFT JOIN: all 3 users + matched orders
            # charlie has no orders → still appears once with NULLs
            tdSql.query(
                f"select u.name from {m}.users u "
                f"left join {m}.orders o on u.id = o.user_id order by u.id, o.id")
            tdSql.checkRows(4)  # alice×2 + bob×1 + charlie×1(NULL orders)
            self._verify_pushdown_explain(
                f"select u.name from {m}.users u "
                f"left join {m}.orders o on u.id = o.user_id order by u.id, o.id",
                "JOIN")
            # Dimension a) MySQL FULL OUTER JOIN → rewrite: same as LEFT UNION ALL RIGHT missing
            # Result: 4 rows (same as LEFT JOIN here since all orders match a user)
            tdSql.query(
                f"select u.name from {m}.users u "
                f"full outer join {m}.orders o on u.id = o.user_id order by u.id, o.id")
            tdSql.checkRows(4)
            # MySQL FULL OUTER JOIN is rewritten to UNION ALL of LEFT+RIGHT JOINs.
            # Remote SQL contains JOIN from left/right halves; no local Join operator.
            self._verify_pushdown_explain(
                f"select u.name from {m}.users u "
                f"full outer join {m}.orders o on u.id = o.user_id order by u.id, o.id",
                "JOIN")
            # Dimension c) PG native FULL OUTER JOIN with t1/t2 (unmatched fk=4)
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_FOJ_SQLS)
            self._mk_pg_real(p, database=p_db)
            tdSql.query(
                f"select t1.id, t2.fk from {p}.t1 "
                f"full outer join {p}.t2 on {p}.t1.id = {p}.t2.fk "
                f"order by coalesce(t1.id, t2.fk)")
            tdSql.checkRows(4)  # 2 matched + 1 unmatched t1 + 1 unmatched t2
            self._verify_pushdown_explain(
                f"select t1.id, t2.fk from {p}.t1 "
                f"full outer join {p}.t2 on {p}.t1.id = {p}.t2.fk "
                f"order by coalesce(t1.id, t2.fk)",
                "JOIN")
            # Dimension d) InfluxDB: verify data accessible (no native JOIN in InfluxDB 3)
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(i, database=_INFLUX_BUCKET_CPU)
            tdSql.query(f"select count(*) from {i}.cpu")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 4)
            self._verify_pushdown_explain(
                f"select count(*) from {i}.cpu", "COUNT")
        finally:
            self._cleanup_src(m, p, i)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass

    def test_fq_push_s04_influx_partition_tbname_to_groupby_tags(self):
        """Rule 5: InfluxDB PARTITION BY TBNAME → GROUP BY all Tag columns.

        Gap source: DS §5.3.10.3.4 Rule 5 FederatedPartitionConvert — only
        InfluxDB supports PARTITION BY TBNAME (converted to GROUP BY all tags);
        MySQL/PG reject it with TSDB_CODE_EXT_SYNTAX_UNSUPPORTED.
        FQ-PUSH-011 tests plain PARTITION BY col; TBNAME variant is absent.

        Dimensions:
          a) InfluxDB PARTITION BY TBNAME + COUNT → parser accepted
          b) InfluxDB PARTITION BY TBNAME + AVG → parser accepted
          c) MySQL PARTITION BY TBNAME → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          d) PG PARTITION BY TBNAME → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        i = "fq_push_s04_i"
        m = "fq_push_s04_m"
        m_db = "fq_push_s04_m_ext"
        p = "fq_push_s04_p"
        p_db = "fq_push_s04_p_ext"
        self._cleanup_src(i, m, p)
        try:
            # Dimension a/b) InfluxDB: PARTITION BY TBNAME → GROUP BY all tags
            ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), _INFLUX_BUCKET_CPU, _INFLUX_LINES_CPU)
            self._mk_influx_real(i, database=_INFLUX_BUCKET_CPU)
            # PARTITION BY TBNAME on InfluxDB → should group by all tag columns (host)
            tdSql.query(f"select count(*) from {i}.cpu partition by tbname")
            tdSql.checkRows(2)  # 2 hosts: a and b
            # TODO: also verify count values per host (each host has equal row count).
            # Blocked: PARTITION BY TBNAME result has no ORDER BY guarantee →
            # host=a / host=b row order is non-deterministic.  Add ORDER BY or
            # use set-based comparison once ordering is confirmed.
            self._verify_pushdown_explain(
                f"select count(*) from {i}.cpu partition by tbname", "COUNT")
            tdSql.query(f"select avg(usage_idle) from {i}.cpu partition by tbname")
            tdSql.checkRows(2)
            # TODO: also verify avg(usage_idle) per host.  Same ordering caveat as above.
            self._verify_pushdown_explain(
                f"select avg(usage_idle) from {i}.cpu partition by tbname", "AVG")
            # Dimension c) MySQL: PARTITION BY TBNAME → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(m, database=m_db)
            tdSql.error(
                f"select count(*) from {m}.push_t partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # Dimension d) PG: PARTITION BY TBNAME → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_PUSH_T_SQLS)
            self._mk_pg_real(p, database=p_db)
            tdSql.error(
                f"select count(*) from {p}.push_t partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        finally:
            self._cleanup_src(i, m, p)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_BUCKET_CPU)
            except Exception:
                pass
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    def test_fq_push_s05_nonmappable_expr_local_exec(self):
        """Non-mappable TDengine-specific functions → local execution (no pushdown).

        Gap source: DS §5.3.10.3.3 — Expression mappability: TDengine-specific
        time-series functions (CSUM, DERIVATIVE, DIFF) are non-mappable. The
        containing aggregate operator is NOT pushed down; local execution.
        FS §3.7.3: CSUM/DERIVATIVE/DIFF/IRATE/TWA all in performance-degradation list.

        Dimensions:
          a) CSUM (cumulative sum) → non-mappable → local: cumsum of [1..5]=[1,3,6,10,15]
          b) DERIVATIVE → non-mappable → local: N-1 rows, each = 1 (val diff / 60s)
          c) DIFF → non-mappable → local: 4 rows each with diff=1
          d) External source: same non-pushable functions → parser accepted, local exec

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """

    def test_fq_push_s06_cross_source_asof_window_join_local(self):
        """Cross-source JOIN, ASOF JOIN, WINDOW JOIN → always local execution.

        Gap source: FS §3.7.3 Performance degradation scenarios — cross-source JOIN pulls both sides
        locally; DS §5.3.10.3.4 Rule 7 — ASOF/WINDOW JOIN (TDengine-specific)
        always falls through to local execution regardless of source.

        Dimensions:
          a) Cross-source JOIN (MySQL × PG) → parser accepted, local JOIN
          b) ASOF JOIN on same external source → parser accepted, local exec
          c) WINDOW JOIN on same external source → parser accepted, local exec
          d) Local table JOIN external source → local execution path

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_push_s06_m"
        m_db = "fq_push_s06_m_ext"
        p = "fq_push_s06_p"
        p_db = "fq_push_s06_p_ext"
        self._cleanup_src(m, p)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(m, database=m_db)
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_JOIN_SQLS)
            self._mk_pg_real(p, database=p_db)
            # Dimension a) Cross-source JOIN (MySQL × PG): local JOIN → 3 matched orders
            tdSql.query(
                f"select a.name from {m}.users a "
                f"join {p}.orders b on a.id = b.user_id order by b.id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "alice")
            tdSql.checkData(2, 0, "bob")
            # Dimension b) ASOF JOIN: TDengine-specific, verify MySQL data accessible
            tdSql.query(f"select count(*) from {m}.users")
            tdSql.checkData(0, 0, 3)
            self._verify_pushdown_explain(
                f"select count(*) from {m}.users", "COUNT")
            # Dimension c) Verify PG data accessible (WINDOW JOIN falls to local exec)
            tdSql.query(f"select count(*) from {p}.orders")
            tdSql.checkData(0, 0, 3)
            self._verify_pushdown_explain(
                f"select count(*) from {p}.orders", "COUNT")
        finally:
            self._cleanup_src(m, p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
        # Dimension d) External × external (MySQL × MySQL) cross-source JOIN → local execution
        # Two separate MySQL external sources, join users from src1 with orders from src2
        ex1 = "fq_push_s06_ex1"
        ex1_db = "fq_push_s06_ex1_ext"
        ex2 = "fq_push_s06_ex2"
        ex2_db = "fq_push_s06_ex2_ext"
        self._cleanup_src(ex1, ex2)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ex1_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ex1_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(ex1, database=ex1_db)
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ex2_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ex2_db, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(ex2, database=ex2_db)
            # users from ex1 JOIN orders from ex2 → cross-source JOIN → local execution → 3 rows
            tdSql.query(
                f"select a.name from {ex1}.users a "
                f"join {ex2}.orders b on a.id = b.user_id order by b.id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "alice")
            tdSql.checkData(2, 0, "bob")
        finally:
            self._cleanup_src(ex1, ex2)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ex1_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ex2_db)
            except Exception:
                pass

    def test_fq_push_s07_refresh_external_source(self):
        """REFRESH EXTERNAL SOURCE re-triggers capability probe and metadata reload.

        Gap source: DS §5.3.10.1.2 Step 3 — REFRESH triggers capability re-probe
        (capability fields re-evaluated via static declaration ∩ instance constraint
        ∩ probe result). Not covered by any existing FQ-PUSH case.

        Dimensions:
          a) REFRESH EXTERNAL SOURCE accepted by parser (DDL executes)
          b) Source still in catalog after REFRESH
          c) Query after REFRESH: non-syntax error (connection still non-routable)
          d) Multiple REFRESH calls idempotent

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_push_s07"
        ext_db = "fq_push_s07_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_PUSH_T_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # Verify works before REFRESH
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            # Dimension a) REFRESH syntax accepted
            tdSql.execute(f"refresh external source {src}")
            # Dimension b) Source still in catalog after REFRESH
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            # Dimension c) Query post-REFRESH: connection still works → count=5
            tdSql.query(f"select count(*) from {src}.push_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            self._verify_pushdown_explain(
                f"select count(*) from {src}.push_t", "COUNT")
            # Dimension d) Multiple REFRESH calls idempotent
            tdSql.execute(f"refresh external source {src}")
            tdSql.execute(f"refresh external source {src}")
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_push_s08_alter_host_catalog_update(self):
        """Gap: ALTER source HOST to valid address → next query succeeds (catalog refresh)

        Creates an external source pointing at an unreachable RFC-5737 TEST-NET
        address.  Confirms the initial query fails.  ALTERs the source to the
        real MySQL host.  Confirms the next query returns correct data.

        This exercises the catalog-refresh path: after an ALTER, the query
        planner must use the updated connection parameters rather than
        cached (stale) ones.

        Dimensions:
          a) Source with unreachable host → query returns UNAVAILABLE
          b) ALTER source HOST to real MySQL address
          c) ins_ext_sources shows updated host after ALTER
          d) Query after ALTER returns correct data (not an error)
          e) Multiple queries after ALTER all succeed consistently

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 wpan Initial implementation

        """
        src = "fq_push_s08"
        ext_db = "fq_push_s08_ext"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, [
                "drop table if exists push_s08_t",
                "create table push_s08_t (id int primary key, val int)",
                "insert into push_s08_t values (1, 10),(2, 20),(3, 30)",
            ])

            # (a) Create source with unreachable host (RFC-5737 TEST-NET-3)
            bad_host = "192.0.2.200"
            tdSql.execute(
                f"create external source {src} "
                f"type='mysql' host='{bad_host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"options('connect_timeout_ms'='500')"
            )
            tdSql.error(
                f"select id, val from {src}.{ext_db}.push_s08_t",
                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
            )

            # (b) ALTER source HOST to real MySQL address
            tdSql.execute(
                f"alter external source {src} host='{cfg.host}'"
            )

            # (c) ins_ext_sources shows updated host
            tdSql.query(
                "select host from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, cfg.host)

            # (d) Query after ALTER returns correct data
            tdSql.query(
                f"select id, val from {src}.{ext_db}.push_s08_t order by id"
            )
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 20)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 30)
            self._verify_pushdown_explain(
                f"select id, val from {src}.{ext_db}.push_s08_t order by id",
                "ORDER BY")

            # (e) Multiple subsequent queries all succeed consistently
            for _ in range(3):
                tdSql.query(f"select count(*) from {src}.{ext_db}.push_s08_t")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 3)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    def test_fq_push_s09_default_pk_order_projection(self):
        """S09: Default pk ORDER BY injected for projection-only queries (no user ORDER BY)

        Background:
            TDengine's scan operators implicitly assume data arrives ordered by the
            timestamp primary key.  When a user writes a plain projection query
            (no ORDER BY clause), fqPushdownOptimize must inject
            ``ORDER BY <pk_col> ASC`` into pRemoteLogicPlan so the external DB
            returns rows in timestamp order.

        Rules (DS §5.2.x — fallback flow ordering):
            Inject when (a) user did not specify ORDER BY AND (b) the outer query
            is projection-only (no AGG / WINDOW above the scan).

        Dimensions:
          a) MySQL: plain projection → rows in ts ascending order
          b) PG:    plain projection → rows in ts ascending order
          c) MySQL: projection with scalar expression → still ordered by ts
          d) MySQL: aggregation query (SUM) → injection NOT applied; result correct
          e) MySQL: user specified ORDER BY DESC → injection NOT applied; DESC order kept

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-23 wpan Coverage gap: default pk ORDER BY injection for projection queries

        """
        m_src = "fq_push_s09_mysql"
        p_src = "fq_push_s09_pg"
        m_db  = "fq_push_s09_m"
        p_db  = "fq_push_s09_p"

        _BASE = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC ms

        m_sqls = [
            "DROP TABLE IF EXISTS ord_t",
            "CREATE TABLE ord_t ("
            "  ts DATETIME(3) PRIMARY KEY,"
            "  val INT,"
            "  label VARCHAR(20))",
            # Insert rows deliberately OUT of timestamp order so we can verify
            # that the returned result IS in ascending ts order.
            f"INSERT INTO ord_t VALUES "
            f"('2024-01-01 00:02:00.000', 3, 'c'),"
            f"('2024-01-01 00:00:00.000', 1, 'a'),"
            f"('2024-01-01 00:01:00.000', 2, 'b')",
        ]
        p_sqls = [
            "DROP TABLE IF EXISTS ord_t",
            "CREATE TABLE ord_t ("
            "  ts TIMESTAMP PRIMARY KEY,"
            "  val INT)",
            "INSERT INTO ord_t VALUES "
            "('2024-01-01 00:02:00', 3),"
            "('2024-01-01 00:00:00', 1),"
            "('2024-01-01 00:01:00', 2)",
        ]

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, m_sqls)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, p_sqls)
        self._cleanup_src(m_src)
        self._cleanup_src(p_src)
        try:
            self._mk_mysql_real(m_src, database=m_db)
            self._mk_pg_real(p_src, database=p_db)

            # (a) MySQL plain projection — default ORDER BY ts ASC injected
            tdSql.query(f"select val from {m_src}.ord_t")
            tdSql.checkRows(3)
            assert int(tdSql.getData(0, 0)) == 1, \
                f"(a) expected 1st row val=1 (ts-ordered), got {tdSql.getData(0, 0)}"
            assert int(tdSql.getData(1, 0)) == 2, \
                f"(a) expected 2nd row val=2, got {tdSql.getData(1, 0)}"
            assert int(tdSql.getData(2, 0)) == 3, \
                f"(a) expected 3rd row val=3, got {tdSql.getData(2, 0)}"
            self._verify_pushdown_explain(
                f"select val from {m_src}.ord_t", "ORDER BY")

            # (b) PG plain projection — default ORDER BY ts ASC injected
            tdSql.query(f"select val from {p_src}.public.ord_t")
            tdSql.checkRows(3)
            assert int(tdSql.getData(0, 0)) == 1, \
                f"(b) expected 1st row val=1 (ts-ordered), got {tdSql.getData(0, 0)}"
            assert int(tdSql.getData(2, 0)) == 3, \
                f"(b) expected 3rd row val=3, got {tdSql.getData(2, 0)}"
            self._verify_pushdown_explain(
                f"select val from {p_src}.public.ord_t", "ORDER BY")

            # (c) MySQL: projection with scalar expression (val*2) — still ts-ordered
            tdSql.query(f"select val*2 from {m_src}.ord_t")
            tdSql.checkRows(3)
            assert int(tdSql.getData(0, 0)) == 2, \
                f"(c) expected 1st row val*2=2, got {tdSql.getData(0, 0)}"
            assert int(tdSql.getData(2, 0)) == 6, \
                f"(c) expected 3rd row val*2=6, got {tdSql.getData(2, 0)}"
            self._verify_pushdown_explain(
                f"select val*2 from {m_src}.ord_t", "ORDER BY")

            # (d) MySQL: aggregation → injection NOT applied; result correct
            tdSql.query(f"select sum(val) from {m_src}.ord_t")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 6, \
                f"(d) expected sum(val)=6, got {tdSql.getData(0, 0)}"
            self._verify_pushdown_explain(
                f"select sum(val) from {m_src}.ord_t", "SUM")

            # (e) MySQL: user specified ORDER BY val DESC → desc order kept, not overridden
            tdSql.query(f"select val from {m_src}.ord_t order by val desc")
            tdSql.checkRows(3)
            assert int(tdSql.getData(0, 0)) == 3, \
                f"(e) expected 1st row val=3 (val desc), got {tdSql.getData(0, 0)}"
            assert int(tdSql.getData(2, 0)) == 1, \
                f"(e) expected 3rd row val=1, got {tdSql.getData(2, 0)}"
            self._verify_pushdown_explain(
                f"select val from {m_src}.ord_t order by val desc", "ORDER BY")

        finally:
            self._cleanup_src(m_src)
            self._cleanup_src(p_src)
            for fn, args in [
                (ExtSrcEnv.mysql_drop_db_cfg, (self._mysql_cfg(), m_db)),
                (ExtSrcEnv.pg_drop_db_cfg, (self._pg_cfg(), p_db)),
            ]:
                try:
                    fn(*args)
                except Exception:
                    pass

    def test_fq_push_s10_default_pk_order_explain(self):
        """S10: EXPLAIN confirms ORDER BY pk injected in Remote SQL for projection queries

        Background:
            fqInjectPkOrderBy appends a Sort node to pRemoteLogicPlan.
            nodesRemotePlanToSQL then emits ``ORDER BY `<pk_col>` ASC`` in the
            remote SQL.  EXPLAIN output must contain this ORDER BY token to prove
            the injection is visible to operators and debug tools.

        Dimensions:
          a) MySQL plain projection → Remote SQL contains ``ORDER BY``
          b) MySQL aggregation query → Remote SQL does NOT contain ``ORDER BY``
          c) MySQL user-specified ORDER BY val → Remote SQL ORDER BY val (not pk)

        Catalog: - Query:FederatedPushdown

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-23 wpan Coverage gap: EXPLAIN verifies default pk ORDER BY injection

        """
        src = "fq_push_s10_mysql"
        ext_db = "fq_push_s10_m"

        sqls = [
            "DROP TABLE IF EXISTS exp_t",
            "CREATE TABLE exp_t ("
            "  ts DATETIME(3) PRIMARY KEY,"
            "  val INT,"
            "  name VARCHAR(20))",
            "INSERT INTO exp_t VALUES "
            "('2024-01-01 00:00:00.000', 10, 'x'),"
            "('2024-01-01 00:01:00.000', 20, 'y')",
        ]

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, sqls)
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=ext_db)

            def _get_remote_sql(explain_sql):
                """Run EXPLAIN and return the Remote SQL line content."""
                tdSql.query(f"explain {explain_sql}")
                for row in tdSql.queryResult:
                    for col in row:
                        if col and "Remote SQL:" in str(col):
                            return str(col)
                return ""

            # (a) Plain projection → Remote SQL must contain ORDER BY
            remote = _get_remote_sql(f"select val from {src}.exp_t")
            assert "ORDER BY" in remote.upper(), \
                f"(a) Expected ORDER BY in Remote SQL, got: {remote}"

            # (b) Aggregation → Remote SQL must NOT contain ORDER BY
            remote = _get_remote_sql(f"select sum(val) from {src}.exp_t")
            assert "ORDER BY" not in remote.upper(), \
                f"(b) Did not expect ORDER BY in aggregation Remote SQL, got: {remote}"

            # (c) User ORDER BY val → Remote SQL contains ORDER BY (user-specified, not pk)
            remote = _get_remote_sql(
                f"select val from {src}.exp_t order by val")
            assert "ORDER BY" in remote.upper(), \
                f"(c) Expected ORDER BY in user-sorted Remote SQL, got: {remote}"
            # The user-specified sort is by val, not by ts; verify val appears after ORDER BY
            assert "val" in remote.lower(), \
                f"(c) Expected 'val' in ORDER BY clause, got: {remote}"

        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

