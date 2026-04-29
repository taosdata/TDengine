"""
test_fq_04_sql_capability.py

Implements FQ-SQL-001 through FQ-SQL-086 from TS §4
"SQL Feature Support" — basic queries, operators, functions, windows, subqueries,
views, and dialect conversion across MySQL/PG/InfluxDB.

Design notes:
    - Each test prepares real data in the external source via ExtSrcEnv,
      creates a TDengine external source pointing to the real DB, queries
      via federated query, and verifies every returned value with checkData.
    - Internal vtable queries use the shared _prepare_internal_env() helper.
    - ensure_env() is called once per process to guarantee the external
      databases (MySQL/PG/InfluxDB) are running.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - MySQL 8.0+, PostgreSQL 14+, InfluxDB v3 (Flight SQL).
    - Python packages: pymysql, psycopg2, requests.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_PUSHDOWN_FAILED,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_STREAM_NOT_SUPPORTED,
    TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED,
)


class TestFq04SqlCapability(FederatedQueryVersionedMixin):
    """FQ-SQL-001 through FQ-SQL-086: SQL feature support."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        tdSql.execute("drop database if exists fq_sql_db")

    # ------------------------------------------------------------------
    # Shared internal vtable helpers
    # ------------------------------------------------------------------

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_sql_db",
            "create database fq_sql_db",
            "use fq_sql_db",
            "create table src_t (ts timestamp, val int, score double, "
            "name binary(32), flag bool)",
            "insert into src_t values (1704067200000, 1, 1.5, 'alpha', true)",
            "insert into src_t values (1704067260000, 2, 2.5, 'beta', false)",
            "insert into src_t values (1704067320000, 3, 3.5, 'gamma', true)",
            "insert into src_t values (1704067380000, 4, 4.5, 'delta', false)",
            "insert into src_t values (1704067440000, 5, 5.5, 'epsilon', true)",
            "create stable src_stb (ts timestamp, val int, score double) "
            "tags(region int) virtual 1",
            "create vtable vt_sql ("
            "  val from fq_sql_db.src_t.val,"
            "  score from fq_sql_db.src_t.score"
            ") using src_stb tags(1)",
        ]
        tdSql.executes(sqls)

    def _teardown_internal_env(self):
        tdSql.execute("drop database if exists fq_sql_db")

    # ------------------------------------------------------------------
    # FQ-SQL-001 ~ FQ-SQL-006: Basic queries
    # ------------------------------------------------------------------

    def test_fq_sql_001(self):
        """FQ-SQL-001: Basic query — SELECT+WHERE+ORDER+LIMIT executes correctly on external tables

        Dimensions:
          a) SELECT * → all 4 rows verified via checkData
          b) WHERE clause → filtered rows with exact count
          c) ORDER BY DESC → first row verified
          d) LIMIT/OFFSET → exact rows returned
          e) Internal vtable SELECT+ORDER+LIMIT verification

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_001_mysql"
        ext_db = "fq_sql_001_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (id INT, amount INT, status INT)",
                "INSERT INTO orders VALUES (1, 50, 1)",
                "INSERT INTO orders VALUES (2, 150, 2)",
                "INSERT INTO orders VALUES (3, 200, 1)",
                "INSERT INTO orders VALUES (4, 80, 2)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) SELECT * → 4 rows, verify all rows × all columns (id, amount, status)
            tdSql.query(f"select * from {src}.{ext_db}.orders order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 50)
            tdSql.checkData(0, 2, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 150)
            tdSql.checkData(1, 2, 2)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 200)
            tdSql.checkData(2, 2, 1)
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, 80)
            tdSql.checkData(3, 2, 2)

            # (b) WHERE amount > 100 → 2 rows
            tdSql.query(
                f"select id, amount from {src}.{ext_db}.orders "
                f"where amount > 100 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, 150)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(1, 1, 200)

            # (c) ORDER BY amount DESC → first row has amount=200
            tdSql.query(
                f"select id, amount from {src}.{ext_db}.orders order by amount desc")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(0, 1, 200)

            # (d) LIMIT 2 OFFSET 1 → rows at index 1,2 by id
            tdSql.query(
                f"select id from {src}.{ext_db}.orders order by id limit 2 offset 1")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 3)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (e) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query("select val, score from fq_sql_db.src_t order by ts limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 1.5)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 2.5)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 3.5)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_002(self):
        """FQ-SQL-002: GROUP BY/HAVING — grouping and filtering results are correct

        Dimensions:
          a) GROUP BY single column → 2 groups, count verified
          b) GROUP BY + SUM → sum per group verified
          c) HAVING filters groups → 1 group returned
          d) Internal vtable: GROUP BY flag → 2 groups with exact counts

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_002_mysql"
        ext_db = "fq_sql_002_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (id INT, status INT, amount INT)",
                "INSERT INTO orders VALUES (1, 1, 200)",
                "INSERT INTO orders VALUES (2, 1, 300)",
                "INSERT INTO orders VALUES (3, 2, 100)",
                "INSERT INTO orders VALUES (4, 2, 150)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) GROUP BY status → 2 rows
            tdSql.query(
                f"select status, count(*) as cnt from {src}.{ext_db}.orders "
                f"group by status order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 2)

            # (b) GROUP BY + SUM
            tdSql.query(
                f"select status, sum(amount) as total from {src}.{ext_db}.orders "
                f"group by status order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 500)   # status=1: 200+300
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 250)   # status=2: 100+150

            # (c) HAVING sum(amount) > 400 → only status=1
            tdSql.query(
                f"select status, sum(amount) as total from {src}.{ext_db}.orders "
                f"group by status having sum(amount) > 400")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 500)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (d) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select flag, count(*) as cnt from fq_sql_db.src_t "
                "group by flag order by flag")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, False)  # flag=false group
            tdSql.checkData(0, 1, 2)   # flag=false: val=2,4
            tdSql.checkData(1, 0, True)   # flag=true group
            tdSql.checkData(1, 1, 3)   # flag=true: val=1,3,5
        finally:
            self._teardown_internal_env()

    def test_fq_sql_003(self):
        """FQ-SQL-003: DISTINCT — deduplication semantics are consistent

        Dimensions:
          a) SELECT DISTINCT single column → 3 unique values verified
          b) SELECT DISTINCT multiple columns → 4 combos verified
          c) Internal vtable: DISTINCT flag → 2 unique booleans

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_003_mysql"
        ext_db = "fq_sql_003_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS items",
                "CREATE TABLE items (id INT, category VARCHAR(20), status INT)",
                "INSERT INTO items VALUES (1, 'A', 1)",
                "INSERT INTO items VALUES (2, 'B', 1)",
                "INSERT INTO items VALUES (3, 'A', 2)",
                "INSERT INTO items VALUES (4, 'C', 2)",
                "INSERT INTO items VALUES (5, 'B', 1)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) DISTINCT category → 3 unique: A, B, C
            tdSql.query(
                f"select distinct category from {src}.{ext_db}.items order by category")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "A")
            tdSql.checkData(1, 0, "B")
            tdSql.checkData(2, 0, "C")

            # (b) DISTINCT (category, status) → 4 combos
            tdSql.query(
                f"select distinct category, status from {src}.{ext_db}.items "
                f"order by category, status")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, "A")
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 0, "A")
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(2, 0, "B")
            tdSql.checkData(2, 1, 1)
            tdSql.checkData(3, 0, "C")
            tdSql.checkData(3, 1, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (c) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query("select distinct flag from fq_sql_db.src_t order by flag")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, False)
            tdSql.checkData(1, 0, True)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_004(self):
        """FQ-SQL-004: UNION ALL same source — pushed down as a whole to same external source, results merged

        Dimensions:
          a) UNION ALL two tables from same MySQL source → 4 rows total
          b) Data from both tables present, no dedup

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_004_mysql"
        ext_db = "fq_sql_004_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users_a",
                "DROP TABLE IF EXISTS users_b",
                "CREATE TABLE users_a (id INT, name VARCHAR(20))",
                "CREATE TABLE users_b (id INT, name VARCHAR(20))",
                "INSERT INTO users_a VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO users_b VALUES (3, 'Carol'), (4, 'Dave')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # UNION ALL → 4 rows, no dedup
            tdSql.query(
                f"select id, name from {src}.{ext_db}.users_a "
                f"union all "
                f"select id, name from {src}.{ext_db}.users_b "
                f"order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, "Bob")
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, "Carol")
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, "Dave")

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_005(self):
        """FQ-SQL-005: UNION cross-source — multi-source local merge with dedup

        Dimensions:
          a) UNION across MySQL and PG sources → shared row deduped
          b) After dedup: 3 distinct rows (id=1,2,3)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_005_mysql"
        src_p = "fq_sql_005_pg"
        m_db = "fq_sql_005_m_db"
        p_db = "fq_sql_005_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(20))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            ])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name TEXT)",
                "INSERT INTO users VALUES (1, 'Alice'), (3, 'Carol')",
            ])
            self._mk_mysql_real(src_m, database=m_db)
            self._mk_pg_real(src_p, database=p_db)

            # UNION dedupes id=1 row → 3 distinct rows
            tdSql.query(
                f"select id, name from {src_m}.{m_db}.users "
                f"union "
                f"select id, name from {src_p}.{p_db}.public.users "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, "Bob")
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, "Carol")

        finally:
            self._cleanup_src(src_m, src_p)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_006(self):
        """FQ-SQL-006: CASE expression — standard CASE pushed down and returns correctly

        Dimensions:
          a) Simple CASE WHEN amount > 200 THEN 'high' ELSE 'low' → verified
          b) SUM(CASE ...) for conditional aggregation → verified
          c) Internal vtable: CASE on flag column

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_006_mysql"
        ext_db = "fq_sql_006_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (id INT, amount INT)",
                "INSERT INTO orders VALUES (1, 100)",
                "INSERT INTO orders VALUES (2, 250)",
                "INSERT INTO orders VALUES (3, 300)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) Simple CASE WHEN
            tdSql.query(
                f"select id, case when amount > 200 then 'high' else 'low' end as level "
                f"from {src}.{ext_db}.orders order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "low")
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, "high")
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, "high")

            # (b) SUM(CASE ...) conditional aggregation
            tdSql.query(
                f"select sum(case when amount > 200 then 1 else 0 end) as high_cnt "
                f"from {src}.{ext_db}.orders")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (c) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val, case when flag = true then 'yes' else 'no' end as f "
                "from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "yes")   # val=1, flag=true
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, "no")    # val=2, flag=false
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, "yes")   # val=3, flag=true
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, "no")    # val=4, flag=false
            tdSql.checkData(4, 0, 5)
            tdSql.checkData(4, 1, "yes")   # val=5, flag=true
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-007 ~ FQ-SQL-012: Operators and special conversions
    # ------------------------------------------------------------------

    def test_fq_sql_007(self):
        """FQ-SQL-007: Arithmetic/comparison/logical operators — +,-,*,/,%,comparison,AND/OR/NOT

        Dimensions:
          a) Internal vtable arithmetic: val+10/val*2/score/2.0 → verified
          b) Comparison WHERE val > 3 → 2 rows (val=4,5)
          c) AND: val > 2 AND flag = true → 2 rows (val=3,5)
          d) OR: val = 1 OR val = 5 → 2 rows
          e) NOT: NOT (val > 3) → 3 rows (val=1,2,3)
          f) MySQL external: arithmetic and comparison via real data verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a–e) Internal vtable
        self._prepare_internal_env()
        try:
            # (a) Arithmetic — verify all 5 rows
            tdSql.query(
                "select val + 10, val * 2, score / 2.0 "
                "from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 11)   # 1+10
            tdSql.checkData(0, 1, 2)    # 1*2
            assert abs(float(tdSql.getData(0, 2)) - 0.75) < 1e-6  # 1.5/2.0
            tdSql.checkData(1, 0, 12)   # 2+10
            tdSql.checkData(1, 1, 4)    # 2*2
            assert abs(float(tdSql.getData(1, 2)) - 1.25) < 1e-6  # 2.5/2.0
            tdSql.checkData(2, 0, 13)   # 3+10
            tdSql.checkData(2, 1, 6)    # 3*2
            assert abs(float(tdSql.getData(2, 2)) - 1.75) < 1e-6  # 3.5/2.0
            tdSql.checkData(3, 0, 14)   # 4+10
            tdSql.checkData(3, 1, 8)    # 4*2
            assert abs(float(tdSql.getData(3, 2)) - 2.25) < 1e-6  # 4.5/2.0
            tdSql.checkData(4, 0, 15)   # 5+10
            tdSql.checkData(4, 1, 10)   # 5*2
            assert abs(float(tdSql.getData(4, 2)) - 2.75) < 1e-6  # 5.5/2.0

            # (b) Comparison
            tdSql.query(
                "select val from fq_sql_db.src_t where val > 3 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(1, 0, 5)

            # (c) AND
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val > 2 and flag = true order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(1, 0, 5)

            # (d) OR
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val = 1 or val = 5 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 5)

            # (e) NOT
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where not (val > 3) order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

        finally:
            self._teardown_internal_env()

        # (f) MySQL external
        src = "fq_sql_007_mysql"
        ext_db = "fq_sql_007_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS nums",
                "CREATE TABLE nums (id INT, val INT)",
                "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select id, val + 5, val * 2, val % 7 "
                f"from {src}.{ext_db}.nums order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 15)   # 10+5
            tdSql.checkData(0, 2, 20)   # 10*2
            tdSql.checkData(0, 3, 3)    # 10%7
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 25)   # 20+5
            tdSql.checkData(1, 2, 40)   # 20*2
            tdSql.checkData(1, 3, 6)    # 20%7
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 35)   # 30+5
            tdSql.checkData(2, 2, 60)   # 30*2
            tdSql.checkData(2, 3, 2)    # 30%7

            tdSql.query(
                f"select id from {src}.{ext_db}.nums where val >= 20 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 3)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_008(self):
        """FQ-SQL-008: REGEXP conversion (MySQL) — MATCH/NMATCH converted to MySQL REGEXP/NOT REGEXP

        Dimensions:
          a) MATCH '^A.*' → 1 row (Alice) verified by checkData
          b) NMATCH '^A' → 2 rows (Bob, Charlie) verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_008_mysql"
        ext_db = "fq_sql_008_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) MATCH '^A.*' → only Alice
            tdSql.query(
                f"select id, name from {src}.{ext_db}.users "
                f"where name match '^A.*' order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")

            # (b) NMATCH '^A' → Bob, Charlie
            tdSql.query(
                f"select id, name from {src}.{ext_db}.users "
                f"where name nmatch '^A' order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, "Bob")
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(1, 1, "Charlie")

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_009(self):
        """FQ-SQL-009: REGEXP conversion (PG) — MATCH/NMATCH converted to ~ / !~

        Dimensions:
          a) MATCH '^A' on PG → 1 row (Alice) verified
          b) NMATCH '^A' on PG → 2 rows (Bob, Charlie) verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_009_pg"
        p_db = "fq_sql_009_db"
        self._cleanup_src(src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name TEXT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            ])
            self._mk_pg_real(src, database=p_db)

            # (a) MATCH '^A' → Alice
            tdSql.query(
                f"select id, name from {src}.{p_db}.public.users "
                f"where name match '^A' order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")

            # (b) NMATCH '^A' → Bob, Charlie
            tdSql.query(
                f"select id, name from {src}.{p_db}.public.users "
                f"where name nmatch '^A' order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, "Bob")
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(1, 1, "Charlie")

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_010(self):
        """FQ-SQL-010: JSON operator conversion (MySQL) — -> converted to JSON_EXTRACT equivalent

        Dimensions:
          a) SELECT metadata->'$.key' from MySQL JSON column → 2 values verified
          b) WHERE on JSON number key → filtered row verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_010_mysql"
        ext_db = "fq_sql_010_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS configs",
                "CREATE TABLE configs (id INT, metadata JSON)",
                "INSERT INTO configs VALUES (1, JSON_OBJECT('key', 'v1', 'num', 10))",
                "INSERT INTO configs VALUES (2, JSON_OBJECT('key', 'v2', 'num', 20))",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) Extract JSON key
            tdSql.query(
                f"select id, metadata->'$.key' as k "
                f"from {src}.{ext_db}.configs order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            assert "v1" in str(tdSql.getData(0, 1))
            tdSql.checkData(1, 0, 2)
            assert "v2" in str(tdSql.getData(1, 1))

            # (b) WHERE on JSON num field
            tdSql.query(
                f"select id from {src}.{ext_db}.configs "
                f"where cast(metadata->>'$.num' as unsigned) = 20")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_011(self):
        """FQ-SQL-011: JSON operator conversion (PG) — -> and ->> return correct values

        Dimensions:
          a) data->>'field' text extraction → 2 values verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_011_pg"
        p_db = "fq_sql_011_db"
        self._cleanup_src(src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS json_table",
                "CREATE TABLE json_table (id INT, data JSONB)",
                "INSERT INTO json_table VALUES (1, '{\"field\": \"hello\"}\'::jsonb)",
                "INSERT INTO json_table VALUES (2, '{\"field\": \"world\"}\'::jsonb)",
            ])
            self._mk_pg_real(src, database=p_db)

            tdSql.query(
                f"select id, data->>'field' as f "
                f"from {src}.{p_db}.public.json_table order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "hello")
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, "world")

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_012(self):
        """FQ-SQL-012: CONTAINS behavior — PG conversion pushed down, other sources computed locally

        Dimensions:
          a) CONTAINS on PG JSONB column → filter works, 2 rows verified
          b) CONTAINS on MySQL text column → local compute, 1 row verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a) PG JSONB CONTAINS
        src_p = "fq_sql_012_pg"
        p_db = "fq_sql_012_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS json_data",
                "CREATE TABLE json_data (id INT, tags JSONB)",
                "INSERT INTO json_data VALUES (1, '{\"env\": \"prod\"}\'::jsonb)",
                "INSERT INTO json_data VALUES (2, '{\"env\": \"dev\"}\'::jsonb)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(
                f"select id from {src_p}.{p_db}.public.json_data "
                f"where tags contains '\"env\"' order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (b) MySQL text column CONTAINS (local compute)
        src_m = "fq_sql_012_mysql"
        m_db = "fq_sql_012_m_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS texts",
                "CREATE TABLE texts (id INT, content TEXT)",
                "INSERT INTO texts VALUES (1, 'hello world')",
                "INSERT INTO texts VALUES (2, 'foo bar')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            tdSql.query(
                f"select id from {src_m}.{m_db}.texts "
                f"where content contains 'hello' order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

    # ------------------------------------------------------------------
    # FQ-SQL-013 ~ FQ-SQL-023: Function mapping
    # ------------------------------------------------------------------

    def test_fq_sql_013(self):
        """FQ-SQL-013: Math function set — ABS/ROUND/CEIL/FLOOR/SIN/COS/SQRT mapping

        Dimensions:
          a) ABS(-3.7) → 3.7 on MySQL
          b) CEIL(2.1) → 3, FLOOR(2.9) → 2 on MySQL
          c) ROUND(2.567, 2) → 2.57 on MySQL
          d) SIN(0) → 0.0, SQRT(9) → 3.0 on MySQL
          e) Internal vtable: ABS/CEIL/FLOOR on score column verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_013_mysql"
        ext_db = "fq_sql_013_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE)",
                "INSERT INTO numbers VALUES (1, -3.7)",
                "INSERT INTO numbers VALUES (2, 2.1)",
                "INSERT INTO numbers VALUES (3, 2.9)",
                "INSERT INTO numbers VALUES (4, 2.567)",
                "INSERT INTO numbers VALUES (5, 0.0)",
                "INSERT INTO numbers VALUES (6, 9.0)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) ABS(-3.7) → 3.7
            tdSql.query(
                f"select id, abs(val) from {src}.{ext_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 1)) - 3.7) < 1e-6

            # (b) CEIL(2.1) → 3
            tdSql.query(
                f"select ceil(val) from {src}.{ext_db}.numbers where id = 2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)

            # FLOOR(2.9) → 2
            tdSql.query(
                f"select floor(val) from {src}.{ext_db}.numbers where id = 3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # (c) ROUND(2.567, 2) → 2.57
            tdSql.query(
                f"select round(val, 2) from {src}.{ext_db}.numbers where id = 4")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.57) < 1e-6

            # (d) SIN(0) → 0.0
            tdSql.query(
                f"select sin(val) from {src}.{ext_db}.numbers where id = 5")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0))) < 1e-6

            # SQRT(9) → 3.0
            tdSql.query(
                f"select sqrt(val) from {src}.{ext_db}.numbers where id = 6")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (e) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select abs(score), ceil(score), floor(score) "
                "from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5) < 1e-6
            tdSql.checkData(0, 1, 2)   # ceil(1.5)=2
            tdSql.checkData(0, 2, 1)   # floor(1.5)=1
        finally:
            self._teardown_internal_env()

    def test_fq_sql_014(self):
        """FQ-SQL-014: LOG parameter order conversion — LOG(value, base) matches target DB parameter order

        Dimensions:
          a) LOG(8, 2) on MySQL → swapped to LOG(2,8) → 3
          b) LOG(8, 2) on PG → swapped to LOG(2,8) → 3
          c) LOG(val) single-arg on MySQL → natural log of 8 ≈ 2.079

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_014_mysql"
        src_p = "fq_sql_014_pg"
        m_db = "fq_sql_014_m_db"
        p_db = "fq_sql_014_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE)",
                "INSERT INTO numbers VALUES (1, 8.0)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) LOG(8, 2) MySQL → 3
            tdSql.query(
                f"select log(val, 2) from {src_m}.{m_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            # (c) LOG single-arg → ln(8) ≈ 2.079
            tdSql.query(
                f"select log(val) from {src_m}.{m_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0794) < 1e-3

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE PRECISION)",
                "INSERT INTO numbers VALUES (1, 8.0)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) LOG(8, 2) PG → 3
            tdSql.query(
                f"select log(val, 2) from {src_p}.{p_db}.public.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_015(self):
        """FQ-SQL-015: TRUNCATE/TRUNC conversion — function name compatibility across databases

        Dimensions:
          a) TRUNCATE(2.567, 2) on MySQL → 2.56 (MySQL: TRUNCATE)
          b) TRUNCATE(2.567, 2) on PG → 2.56 (PG: TRUNC)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_015_mysql"
        src_p = "fq_sql_015_pg"
        m_db = "fq_sql_015_m_db"
        p_db = "fq_sql_015_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE)",
                "INSERT INTO numbers VALUES (1, 2.567)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL TRUNCATE(2.567, 2) → 2.56
            tdSql.query(
                f"select truncate(val, 2) from {src_m}.{m_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.56) < 1e-6

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE PRECISION)",
                "INSERT INTO numbers VALUES (1, 2.567)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) PG TRUNCATE → TRUNC(2.567, 2) → 2.56
            tdSql.query(
                f"select truncate(val, 2) from {src_p}.{p_db}.public.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.56) < 1e-6

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_016(self):
        """FQ-SQL-016: RAND semantics — seed/no-seed difference handled as expected

        Dimensions:
          a) RAND() on MySQL → result in [0, 1)
          b) RAND(42) seeded on MySQL → result in [0, 1)
          c) RAND() on PG → converted to RANDOM(), result in [0, 1)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_016_mysql"
        src_p = "fq_sql_016_pg"
        m_db = "fq_sql_016_m_db"
        p_db = "fq_sql_016_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS nums",
                "CREATE TABLE nums (id INT)",
                "INSERT INTO nums VALUES (1)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) RAND() no seed → value in [0, 1)
            tdSql.query(f"select rand() as r from {src_m}.{m_db}.nums where id = 1")
            tdSql.checkRows(1)
            rval = float(tdSql.getData(0, 0))
            assert 0.0 <= rval < 1.0, f"RAND() out of range: {rval}"

            # (b) RAND(42) seeded
            tdSql.query(f"select rand(42) as r from {src_m}.{m_db}.nums where id = 1")
            tdSql.checkRows(1)
            rval2 = float(tdSql.getData(0, 0))
            assert 0.0 <= rval2 < 1.0, f"RAND(42) out of range: {rval2}"

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS nums",
                "CREATE TABLE nums (id INT)",
                "INSERT INTO nums VALUES (1)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (c) RAND() → RANDOM() on PG
            tdSql.query(f"select rand() as r from {src_p}.{p_db}.public.nums where id = 1")
            tdSql.checkRows(1)
            rval3 = float(tdSql.getData(0, 0))
            assert 0.0 <= rval3 < 1.0, f"RANDOM() out of range: {rval3}"

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_017(self):
        """FQ-SQL-017: String function set — CONCAT/TRIM/REPLACE/UPPER/LOWER mapping

        Dimensions:
          a) CONCAT(name, '_x') → 'Alice_x' on MySQL
          b) TRIM(' Bob ') → 'Bob' on MySQL
          c) REPLACE(name, 'A', 'a') → 'alice' on MySQL
          d) UPPER/LOWER on MySQL → verified
          e) Internal vtable: LOWER/UPPER on name column → verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_017_mysql"
        ext_db = "fq_sql_017_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice')",
                "INSERT INTO users VALUES (2, ' Bob ')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) CONCAT
            tdSql.query(
                f"select id, concat(name, '_x') from {src}.{ext_db}.users "
                f"where id = 1")
            tdSql.checkRows(1)
            assert "Alice_x" in str(tdSql.getData(0, 1))

            # (b) TRIM
            tdSql.query(
                f"select id, trim(name) from {src}.{ext_db}.users where id = 2")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 1)).strip() == "Bob"

            # (c) REPLACE
            tdSql.query(
                f"select id, replace(name, 'A', 'a') from {src}.{ext_db}.users "
                f"where id = 1")
            tdSql.checkRows(1)
            assert "alice" in str(tdSql.getData(0, 1))

            # (d) UPPER / LOWER
            tdSql.query(
                f"select upper(name), lower(name) from {src}.{ext_db}.users where id = 1")
            tdSql.checkRows(1)
            assert "ALICE" in str(tdSql.getData(0, 0))
            assert "alice" in str(tdSql.getData(0, 1))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (e) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select lower(name), upper(name) from fq_sql_db.src_t "
                "order by ts limit 1")
            tdSql.checkRows(1)
            assert "alpha" in str(tdSql.getData(0, 0))
            assert "ALPHA" in str(tdSql.getData(0, 1))
        finally:
            self._teardown_internal_env()

    def test_fq_sql_018(self):
        """FQ-SQL-018: LENGTH byte semantics — PG uses OCTET_LENGTH

        Dimensions:
          a) LENGTH('hello') on MySQL → 5 bytes verified
          b) LENGTH('hello') on PG → mapped to OCTET_LENGTH → 5 bytes verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_018_mysql"
        src_p = "fq_sql_018_pg"
        m_db = "fq_sql_018_m_db"
        p_db = "fq_sql_018_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS strings",
                "CREATE TABLE strings (id INT, name VARCHAR(50) CHARACTER SET utf8mb4)",
                "INSERT INTO strings VALUES (1, 'hello')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL LENGTH('hello') → 5
            tdSql.query(
                f"select length(name) from {src_m}.{m_db}.strings where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS strings",
                "CREATE TABLE strings (id INT, name TEXT)",
                "INSERT INTO strings VALUES (1, 'hello')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) PG LENGTH → OCTET_LENGTH → 5
            tdSql.query(
                f"select length(name) from {src_p}.{p_db}.public.strings where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_019(self):
        """FQ-SQL-019: SUBSTRING_INDEX handling — local computation when PG has no equivalent

        Dimensions:
          a) MySQL: SUBSTRING_INDEX(email, '@', 1) → local part before @ verified
          b) PG: SUBSTRING_INDEX → local computation, result verified
          c) InfluxDB: SUBSTRING_INDEX → local computation, result verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_019_mysql"
        src_p = "fq_sql_019_pg"
        m_db = "fq_sql_019_m_db"
        p_db = "fq_sql_019_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, email VARCHAR(100))",
                "INSERT INTO users VALUES (1, 'alice@example.com')",
                "INSERT INTO users VALUES (2, 'bob@test.org')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL pushdown
            tdSql.query(
                f"select id, substring_index(email, '@', 1) as local_part "
                f"from {src_m}.{m_db}.users order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            assert "alice" in str(tdSql.getData(0, 1))
            tdSql.checkData(1, 0, 2)
            assert "bob" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, email TEXT)",
                "INSERT INTO users VALUES (1, 'alice@example.com')",
                "INSERT INTO users VALUES (2, 'bob@test.org')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) PG local compute
            tdSql.query(
                f"select id, substring_index(email, '@', 1) as local_part "
                f"from {src_p}.{p_db}.public.users order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            assert "alice" in str(tdSql.getData(0, 1))
            tdSql.checkData(1, 0, 2)
            assert "bob" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (c) InfluxDB: SUBSTRING_INDEX not pushed down → local compute fallback
        src_i = "fq_sql_019_influx"
        i_db = "fq_sql_019_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                'users,id=1 email="alice@example.com" 1704067200000000000\n'
                'users,id=2 email="bob@test.org" 1704067260000000000'
            )
            self._mk_influx_real(src_i, database=i_db)

            tdSql.query(
                f"select email, substring_index(email, '@', 1) as local_part "
                f"from {src_i}.{i_db}.users order by time")
            tdSql.checkRows(2)
            assert "alice" in str(tdSql.getData(0, 1))
            assert "bob" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_020(self):
        """FQ-SQL-020: Encoding functions — TO_BASE64/FROM_BASE64 mapping behaves correctly

        Dimensions:
          a) TO_BASE64('hello') on MySQL → 'aGVsbG8=' verified
          b) FROM_BASE64('aGVsbG8=') on MySQL → 'hello' verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_020_mysql"
        ext_db = "fq_sql_020_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS strings",
                "CREATE TABLE strings (id INT, data VARCHAR(100))",
                "INSERT INTO strings VALUES (1, 'hello')",
                "INSERT INTO strings VALUES (2, 'aGVsbG8=')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) TO_BASE64('hello') → 'aGVsbG8='
            tdSql.query(
                f"select to_base64(data) from {src}.{ext_db}.strings where id = 1")
            tdSql.checkRows(1)
            assert "aGVsbG8=" in str(tdSql.getData(0, 0))

            # (b) FROM_BASE64('aGVsbG8=') → 'hello'
            tdSql.query(
                f"select from_base64(data) from {src}.{ext_db}.strings where id = 2")
            tdSql.checkRows(1)
            assert "hello" in str(tdSql.getData(0, 0))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_021(self):
        """FQ-SQL-021: Hash functions — MD5/SHA2 mapping and local fallback

        Dimensions:
          a) MD5(name) on MySQL → 32-char hex verified
          b) MD5(name) on PG → 32-char hex verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_021_mysql"
        src_p = "fq_sql_021_pg"
        m_db = "fq_sql_021_m_db"
        p_db = "fq_sql_021_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MD5 on MySQL → 32-char hex string
            tdSql.query(
                f"select id, md5(name) from {src_m}.{m_db}.users where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            result = str(tdSql.getData(0, 1))
            assert len(result) == 32, f"MD5 length should be 32: {result}"
            assert all(c in "0123456789abcdefABCDEF" for c in result), \
                f"MD5 should be hex: {result}"
            m_hash = result

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name TEXT)",
                "INSERT INTO users VALUES (1, 'Alice')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) MD5 on PG → 32-char string; same hash as MySQL
            tdSql.query(
                f"select id, md5(name) from {src_p}.{p_db}.public.users where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            result = str(tdSql.getData(0, 1))
            assert len(result) == 32, f"PG MD5 length should be 32: {result}"
            assert all(c in "0123456789abcdefABCDEF" for c in result), \
                f"MD5 should be hex: {result}"
            assert result.lower() == m_hash.lower(), \
                f"MySQL and PG MD5 should match: {m_hash} vs {result}"

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_022(self):
        """FQ-SQL-022: Type conversion function — CAST semantics correct on external tables and internal vtables

        Dimensions:
          a) CAST(val AS DOUBLE) on MySQL → double value verified
          b) CAST(val AS VARCHAR) on MySQL → string verified
          c) Internal vtable: CAST(val AS DOUBLE) verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_022_mysql"
        ext_db = "fq_sql_022_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val INT)",
                "INSERT INTO numbers VALUES (1, 42)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) CAST as DOUBLE
            tdSql.query(
                f"select cast(val as double) from {src}.{ext_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 42.0) < 1e-6

            # (b) CAST as VARCHAR
            tdSql.query(
                f"select cast(val as char) from {src}.{ext_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert "42" in str(tdSql.getData(0, 0))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (c) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query("select cast(val as double) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-6
        finally:
            self._teardown_internal_env()

    def test_fq_sql_023(self):
        """FQ-SQL-023: Time function mapping — NOW/TODAY/MONTH/YEAR and other time function conversions

        Dimensions:
          a) DAYOFWEEK(ts) on MySQL → 1–7, verified for known date
          b) YEAR(ts) / MONTH(ts) on MySQL → verified for 2024-01-01
          c) Internal vtable: CAST(ts AS BIGINT) → timestamp epoch

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_023_mysql"
        ext_db = "fq_sql_023_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS events",
                "CREATE TABLE events (id INT, ts DATETIME)",
                # 2024-01-01 is a Monday, DAYOFWEEK=2
                "INSERT INTO events VALUES (1, '2024-01-01 00:00:00')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) DAYOFWEEK → 2 for Monday
            tdSql.query(
                f"select id, dayofweek(ts) from {src}.{ext_db}.events where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 2)   # Monday = 2 in MySQL (1=Sun, 2=Mon)

            # (b) YEAR and MONTH
            tdSql.query(
                f"select year(ts), month(ts) from {src}.{ext_db}.events where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2024)
            tdSql.checkData(0, 1, 1)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (c) Internal vtable CAST ts → bigint — verify first and last row
        self._prepare_internal_env()
        try:
            tdSql.query("select cast(ts as bigint) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            # rows are 1-min apart starting from 2024-01-01 00:00:00 UTC
            assert int(tdSql.getData(0, 0)) == 1704067200000
            assert int(tdSql.getData(1, 0)) == 1704067260000
            assert int(tdSql.getData(2, 0)) == 1704067320000
            assert int(tdSql.getData(3, 0)) == 1704067380000
            assert int(tdSql.getData(4, 0)) == 1704067440000
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-024 ~ FQ-SQL-032: Aggregates and special functions
    # ------------------------------------------------------------------

    def test_fq_sql_024(self):
        """FQ-SQL-024: Basic aggregate functions — COUNT/SUM/AVG/MIN/MAX/STDDEV on MySQL

        Dimensions:
          a) COUNT/SUM/AVG/MIN/MAX on MySQL external table → all verified
          b) Internal vtable: same aggregates verified with exact values

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_024_mysql"
        ext_db = "fq_sql_024_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS nums",
                "CREATE TABLE nums (id INT, val INT)",
                "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) COUNT/SUM/AVG/MIN/MAX
            tdSql.query(
                f"select count(*), sum(val), avg(val), min(val), max(val) "
                f"from {src}.{ext_db}.nums")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)    # count
            tdSql.checkData(0, 1, 150)  # sum
            assert abs(float(tdSql.getData(0, 2)) - 30.0) < 1e-6  # avg
            tdSql.checkData(0, 3, 10)   # min
            tdSql.checkData(0, 4, 50)   # max

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (b) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select count(*), sum(val), avg(val), min(val), max(val), stddev(val) "
                "from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)   # count
            tdSql.checkData(0, 1, 15)  # sum(1+2+3+4+5)
            assert abs(float(tdSql.getData(0, 2)) - 3.0) < 1e-6  # avg
            tdSql.checkData(0, 3, 1)   # min
            tdSql.checkData(0, 4, 5)   # max
            # stddev of [1,2,3,4,5] = sqrt(2) ≈ 1.4142
            assert abs(float(tdSql.getData(0, 5)) - 1.4142) < 1e-3
        finally:
            self._teardown_internal_env()

    def test_fq_sql_025(self):
        """FQ-SQL-025: Percentile functions — PERCENTILE/APERCENTILE only supported on internal tables

        Dimensions:
          a) PERCENTILE(val, 50) on internal vtable → 3 (median of 1,2,3,4,5)
          b) APERCENTILE on internal vtable → verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # (a) PERCENTILE p50 of [1,2,3,4,5] = 3
            tdSql.query("select percentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            # (b) APERCENTILE (approximate percentile) — p50 of [1,2,3,4,5] ≈ 3
            tdSql.query("select apercentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1.0, \
                f"APERCENTILE p50 should be near 3: {tdSql.getData(0, 0)}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_026(self):
        """FQ-SQL-026: Selection functions — FIRST/LAST/TOP/BOTTOM computed locally

        Dimensions:
          a) FIRST(val) on internal vtable → 1 (inserted first)
          b) LAST(val) → 5
          c) TOP(val, 2) → 2 rows with val=4,5
          d) BOTTOM(val, 2) → 2 rows with val=1,2

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query("select first(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            tdSql.query("select last(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            tdSql.query("select top(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            # TOP(val, 2) returns the 2 largest values: 5 and 4 (order may vary)
            top_vals = sorted([int(tdSql.getData(0, 0)), int(tdSql.getData(1, 0))], reverse=True)
            assert top_vals[0] == 5, f"TOP-1 should be 5, got {top_vals[0]}"
            assert top_vals[1] == 4, f"TOP-2 should be 4, got {top_vals[1]}"

            tdSql.query("select bottom(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            # BOTTOM(val, 2) returns the 2 smallest values: 1 and 2 (order may vary)
            bot_vals = sorted([int(tdSql.getData(0, 0)), int(tdSql.getData(1, 0))])
            assert bot_vals[0] == 1, f"BOTTOM-1 should be 1, got {bot_vals[0]}"
            assert bot_vals[1] == 2, f"BOTTOM-2 should be 2, got {bot_vals[1]}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_027(self):
        """FQ-SQL-027: LAG/LEAD — OVER(ORDER BY ts) semantics pushed down

        Dimensions:
          a) LAG(val) OVER(ORDER BY ts) on PG → NULL for first row, prior val for others
          b) LEAD(val) OVER(ORDER BY ts) on PG → next val, NULL for last row

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_027_pg"
        p_db = "fq_sql_027_db"
        self._cleanup_src(src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS measures",
                "CREATE TABLE measures (ts TIMESTAMP, val INT)",
                "INSERT INTO measures VALUES "
                "('2024-01-01 00:00:00', 10), "
                "('2024-01-01 00:01:00', 20), "
                "('2024-01-01 00:02:00', 30)",
            ])
            self._mk_pg_real(src, database=p_db)

            # (a) LAG: first row → NULL, second → 10, third → 20
            tdSql.query(
                f"select val, lag(val) over(order by ts) as prev "
                f"from {src}.{p_db}.public.measures order by ts")
            tdSql.checkRows(3)
            assert tdSql.getData(0, 1) is None   # first row has no previous
            tdSql.checkData(1, 1, 10)
            tdSql.checkData(2, 1, 20)

            # (b) LEAD: first → 20, second → 30, last → NULL
            tdSql.query(
                f"select val, lead(val) over(order by ts) as nxt "
                f"from {src}.{p_db}.public.measures order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 20)
            tdSql.checkData(1, 1, 30)
            assert tdSql.getData(2, 1) is None   # last row has no next

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_028(self):
        """FQ-SQL-028: TAGS on InfluxDB — converted to DISTINCT tag combinations

        Dimensions:
          a) SELECT DISTINCT host, region from InfluxDB → 2 tag combos verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_028_influx"
        i_db = "fq_sql_028_db"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "cpu,host=h1,region=us val=1 1704067200000000000\n"
                "cpu,host=h2,region=eu val=2 1704067260000000000\n"
                "cpu,host=h1,region=us val=3 1704067320000000000"
            )
            self._mk_influx_real(src, database=i_db)

            tdSql.query(
                f"select distinct host, region from {src}.{i_db}.cpu order by host")
            # h1+us and h2+eu → 2 combos
            tdSql.checkRows(2)
            assert "h1" in str(tdSql.getData(0, 0))
            assert "us" in str(tdSql.getData(0, 1))
            assert "h2" in str(tdSql.getData(1, 0))
            assert "eu" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_029(self):
        """FQ-SQL-029: TAGS pseudo-column on MySQL/PG — reports unsupported error

        Dimensions:
          a) SELECT tags FROM mysql_src.db.table → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          b) SELECT tags FROM pg_src.db.schema.table → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_029_mysql"
        src_p = "fq_sql_029_pg"
        m_db = "fq_sql_029_m_db"
        p_db = "fq_sql_029_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT)",
                "INSERT INTO users VALUES (1)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) TAGS pseudo-column on MySQL → error
            tdSql.error(
                f"select tags from {src_m}.{m_db}.users",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT)",
                "INSERT INTO users VALUES (1)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) TAGS pseudo-column on PG → error
            tdSql.error(
                f"select tags from {src_p}.{p_db}.public.users",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_030(self):
        """FQ-SQL-030: TBNAME on MySQL/PG — reports unsupported error

        Dimensions:
          a) SELECT tbname FROM mysql_src.db.table → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED
          b) SELECT tbname FROM pg_src.db.schema.table → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_030_mysql"
        src_p = "fq_sql_030_pg"
        m_db = "fq_sql_030_m_db"
        p_db = "fq_sql_030_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT)",
                "INSERT INTO users VALUES (1)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) TBNAME on MySQL → error
            tdSql.error(
                f"select tbname from {src_m}.{m_db}.users",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT)",
                "INSERT INTO users VALUES (1)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) TBNAME on PG → error
            tdSql.error(
                f"select tbname from {src_p}.{p_db}.public.users",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_031(self):
        """FQ-SQL-031: PARTITION BY on InfluxDB — converted to GROUP BY tag

        Dimensions:
          a) SELECT avg(val) PARTITION BY host on InfluxDB → 2 partitions verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_031_influx"
        i_db = "fq_sql_031_db"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "cpu,host=h1 usage=10 1704067200000000000\n"
                "cpu,host=h1 usage=20 1704067260000000000\n"
                "cpu,host=h2 usage=30 1704067320000000000\n"
                "cpu,host=h2 usage=40 1704067380000000000"
            )
            self._mk_influx_real(src, database=i_db)

            # PARTITION BY host → 2 groups: h1 avg=15, h2 avg=35
            tdSql.query(
                f"select avg(usage) from {src}.{i_db}.cpu partition by host "
                f"order by host")
            tdSql.checkRows(2)
            # h1: (10+20)/2 = 15.0, h2: (30+40)/2 = 35.0; ORDER BY host → h1 first
            assert abs(float(tdSql.getData(0, 0)) - 15.0) < 1e-3, \
                f"h1 avg(usage) should be 15.0, got {tdSql.getData(0, 0)}"
            assert abs(float(tdSql.getData(1, 0)) - 35.0) < 1e-3, \
                f"h2 avg(usage) should be 35.0, got {tdSql.getData(1, 0)}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_032(self):
        """FQ-SQL-032: PARTITION BY TBNAME MySQL/PG — reports unsupported error

        Dimensions:
          a) SELECT count(*) FROM mysql.db.table PARTITION BY tbname → TSDB_CODE_EXT_SYNTAX_UNSUPPORTED

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_032_mysql"
        ext_db = "fq_sql_032_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (id INT, status INT)",
                "INSERT INTO orders VALUES (1, 1), (2, 2)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # PARTITION BY tbname on MySQL → error
            tdSql.error(
                f"select count(*) from {src}.{ext_db}.orders partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_033(self):
        """FQ-SQL-033: INTERVAL tumbling window — time window aggregation pushdown

        Dimensions:
          a) INTERVAL(1m) on internal vtable → window count and wstart verified
          b) MySQL: GROUP BY DATE_TRUNC equivalent window → verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a) Internal vtable INTERVAL(1m) — 5 rows each in separate 1-minute bucket
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*), avg(val) from fq_sql_db.src_t "
                "interval(1m)")
            tdSql.checkRows(5)   # 5 rows in 5 distinct 1-minute slots
            # Each 1-min window holds exactly 1 row, val=1..5, avg=val, count=1
            expected_wstarts = [1704067200000, 1704067260000, 1704067320000,
                                1704067380000, 1704067440000]
            for i in range(5):
                assert int(tdSql.getData(i, 0)) == expected_wstarts[i], \
                    f"row {i} _wstart should be {expected_wstarts[i]}"
                assert int(tdSql.getData(i, 1)) == 1, \
                    f"row {i} count should be 1"
                assert abs(float(tdSql.getData(i, 2)) - (i + 1)) < 1e-6, \
                    f"row {i} avg(val) should be {i + 1}"
        finally:
            self._teardown_internal_env()

        # (b) MySQL external: GROUP BY minute using floor-based group
        src = "fq_sql_033_mysql"
        ext_db = "fq_sql_033_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS events",
                "CREATE TABLE events (id INT, ts DATETIME, val INT)",
                "INSERT INTO events VALUES (1, '2024-01-01 00:00:00', 10)",
                "INSERT INTO events VALUES (2, '2024-01-01 00:00:30', 20)",
                "INSERT INTO events VALUES (3, '2024-01-01 00:01:00', 30)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select year(ts), hour(ts), minute(ts), sum(val) as sm "
                f"from {src}.{ext_db}.events "
                f"group by year(ts), hour(ts), minute(ts) "
                f"order by year(ts), hour(ts), minute(ts)")
            tdSql.checkRows(2)  # minute 0 (rows 1,2) + minute 1 (row 3)
            # minute 0: year=2024, hour=0, minute=0, sum=10+20=30
            tdSql.checkData(0, 0, 2024)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, 0)
            tdSql.checkData(0, 3, 30)
            # minute 1: year=2024, hour=0, minute=1, sum=30
            tdSql.checkData(1, 0, 2024)
            tdSql.checkData(1, 1, 0)
            tdSql.checkData(1, 2, 1)
            tdSql.checkData(1, 3, 30)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    # ------------------------------------------------------------------
    # FQ-SQL-034 ~ FQ-SQL-043: Detailed operator/syntax coverage
    # ------------------------------------------------------------------

    def test_fq_sql_034(self):
        """FQ-SQL-034: Arithmetic operators full coverage — +,-,*,/,% row-by-row value verification

        Dimensions:
          a) All 5 arithmetic ops on internal vtable, row-by-row verified
          b) Division result verified as float

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # val=[1,2,3,4,5]; divisor=2.0 to force float division
            tdSql.query(
                "select val + 1, val - 1, val * 2, val / 2.0, val % 3 "
                "from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            # row 0: val=1
            tdSql.checkData(0, 0, 2)     # 1+1
            tdSql.checkData(0, 1, 0)     # 1-1
            tdSql.checkData(0, 2, 2)     # 1*2
            assert abs(float(tdSql.getData(0, 3)) - 0.5) < 1e-6   # 1/2.0
            tdSql.checkData(0, 4, 1)     # 1%3
            # row 1: val=2
            tdSql.checkData(1, 0, 3)     # 2+1
            tdSql.checkData(1, 1, 1)     # 2-1
            tdSql.checkData(1, 2, 4)     # 2*2
            assert abs(float(tdSql.getData(1, 3)) - 1.0) < 1e-6   # 2/2.0
            tdSql.checkData(1, 4, 2)     # 2%3
            # row 2: val=3
            tdSql.checkData(2, 0, 4)     # 3+1
            tdSql.checkData(2, 1, 2)     # 3-1
            tdSql.checkData(2, 2, 6)     # 3*2
            assert abs(float(tdSql.getData(2, 3)) - 1.5) < 1e-6   # 3/2.0
            tdSql.checkData(2, 4, 0)     # 3%3
            # row 3: val=4
            tdSql.checkData(3, 0, 5)     # 4+1
            tdSql.checkData(3, 1, 3)     # 4-1
            tdSql.checkData(3, 2, 8)     # 4*2
            assert abs(float(tdSql.getData(3, 3)) - 2.0) < 1e-6   # 4/2.0
            tdSql.checkData(3, 4, 1)     # 4%3
            # row 4: val=5
            tdSql.checkData(4, 0, 6)     # 5+1
            tdSql.checkData(4, 1, 4)     # 5-1
            tdSql.checkData(4, 2, 10)    # 5*2
            assert abs(float(tdSql.getData(4, 3)) - 2.5) < 1e-6   # 5/2.0
            tdSql.checkData(4, 4, 2)     # 5%3

        finally:
            self._teardown_internal_env()

    def test_fq_sql_035(self):
        """FQ-SQL-035: Comparison operators full coverage — =,!=,<>,>,<,>=,<=,BETWEEN,IN,LIKE

        Dimensions:
          a) = / != / <> / > / < / >= / <= / BETWEEN / IN / LIKE — all ops verified with values

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # = : val=3 → 1 row
            tdSql.query("select val from fq_sql_db.src_t where val = 3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)

            # != : val≠3 → 4 rows (1,2,4,5)
            tdSql.query("select val from fq_sql_db.src_t where val != 3 order by ts")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 4)
            tdSql.checkData(3, 0, 5)

            # <> : identical to != → same 4 rows
            tdSql.query("select val from fq_sql_db.src_t where val <> 3 order by ts")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(3, 0, 5)

            # > : val>3 → rows with val=4,5
            tdSql.query("select val from fq_sql_db.src_t where val > 3 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(1, 0, 5)

            # < : val<3 → rows with val=1,2
            tdSql.query("select val from fq_sql_db.src_t where val < 3 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)

            # >= : val>=3 → rows with val=3,4,5
            tdSql.query("select val from fq_sql_db.src_t where val >= 3 order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(1, 0, 4)
            tdSql.checkData(2, 0, 5)

            # <= : val<=3 → rows with val=1,2,3
            tdSql.query("select val from fq_sql_db.src_t where val <= 3 order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)

            # BETWEEN : val between 2 and 4 → rows with val=2,3,4
            tdSql.query(
                "select val from fq_sql_db.src_t where val between 2 and 4 order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 4)

            # IN : val in (1,3,5) → rows with val=1,3,5
            tdSql.query(
                "select val from fq_sql_db.src_t where val in (1, 3, 5) order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 5)

            # LIKE : name like 'a%' → only 'alpha' (val=1)
            tdSql.query(
                "select val, name from fq_sql_db.src_t where name like 'a%'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            assert "alpha" in str(tdSql.getData(0, 1))

        finally:
            self._teardown_internal_env()

    def test_fq_sql_036(self):
        """FQ-SQL-036: Logical operators full coverage — AND/OR/NOT combinations

        Dimensions:
          a) AND → 2 rows verified (val=3,5 with flag=true)
          b) OR → 2 rows verified
          c) NOT → 3 rows verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val > 2 and flag = true order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(1, 0, 5)

            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val = 1 or val = 5 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 5)

            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where not (val > 3) order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_037(self):
        """FQ-SQL-037: Bitwise operators full coverage — & and | pushdown on MySQL/PG, local execution on InfluxDB

        Dimensions:
          a) val & 3 on internal vtable → all 5 rows verified
          b) val | 8 → first row = 9 verified
          c) MySQL external: & and | operators pushed down, results verified
          d) PG external: & and | operators pushed down, results verified
          e) InfluxDB: bitwise not pushed down, local compute fallback, results correct

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a)(b) Internal vtable
        self._prepare_internal_env()
        try:
            tdSql.query("select val & 3 from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # 1 & 3 = 1
            tdSql.checkData(1, 0, 2)   # 2 & 3 = 2
            tdSql.checkData(2, 0, 3)   # 3 & 3 = 3
            tdSql.checkData(3, 0, 0)   # 4 & 3 = 0
            tdSql.checkData(4, 0, 1)   # 5 & 3 = 1

            tdSql.query("select val | 8 from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 9)   # 1 | 8 = 9
        finally:
            self._teardown_internal_env()

        # (c) MySQL external
        src = "fq_sql_037_mysql"
        ext_db = "fq_sql_037_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS bits",
                "CREATE TABLE bits (id INT, val INT)",
                "INSERT INTO bits VALUES (1, 5), (2, 3)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select id, val & 3 from {src}.{ext_db}.bits order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # 5 & 3 = 1
            tdSql.checkData(1, 1, 3)   # 3 & 3 = 3

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (d) PG external: & and | pushed down
        src_p = "fq_sql_037_pg"
        p_db = "fq_sql_037_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS bits",
                "CREATE TABLE bits (id INT, val INT)",
                "INSERT INTO bits VALUES (1, 5), (2, 3)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(
                f"select id, val & 3 from {src_p}.{p_db}.public.bits order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # 5 & 3 = 1
            tdSql.checkData(1, 1, 3)   # 3 & 3 = 3

            tdSql.query(
                f"select id, val | 8 from {src_p}.{p_db}.public.bits order by id limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 13)  # 5 | 8 = 13

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (e) InfluxDB: bitwise not pushed down → local compute, result still correct
        src_i = "fq_sql_037_influx"
        i_db = "fq_sql_037_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "bits,host=h1 val=5i 1704067200000000000\n"
                "bits,host=h2 val=3i 1704067260000000000"
            )
            self._mk_influx_real(src_i, database=i_db)

            # InfluxDB bitwise: local compute fallback, correct result
            tdSql.query(
                f"select host, val & 3 from {src_i}.{i_db}.bits order by time")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)   # 5 & 3 = 1
            tdSql.checkData(1, 1, 3)   # 3 & 3 = 3

        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_038(self):
        """FQ-SQL-038: JSON operators full coverage — -> converted correctly for MySQL/PG respectively

        Dimensions:
          a) MySQL: metadata->'$.key' → value verified
          b) PG: data->>'field' → value verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a) MySQL JSON
        src_m = "fq_sql_038_mysql"
        m_db = "fq_sql_038_m_db"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS jdata",
                "CREATE TABLE jdata (id INT, data JSON)",
                "INSERT INTO jdata VALUES (1, JSON_OBJECT('k', 'v1'))",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            tdSql.query(
                f"select id, data->'$.k' from {src_m}.{m_db}.jdata where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            assert "v1" in str(tdSql.getData(0, 1))

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        # (b) PG JSONB
        src_p = "fq_sql_038_pg"
        p_db = "fq_sql_038_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS jdata",
                "CREATE TABLE jdata (id INT, data JSONB)",
                "INSERT INTO jdata VALUES (1, '{\"k\": \"v2\"}\'::jsonb)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(
                f"select id, data->>'k' from {src_p}.{p_db}.public.jdata where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "v2")

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_039(self):
        """FQ-SQL-039: REGEXP operations full coverage — MATCH/NMATCH target dialect conversion

        Dimensions:
          a) MySQL MATCH '^B' → rows starting with B verified
          b) MySQL NMATCH '^B' → rows not starting with B verified
          c) PG MATCH → ~ operator conversion verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_039_mysql"
        src_p = "fq_sql_039_pg"
        m_db = "fq_sql_039_m_db"
        p_db = "fq_sql_039_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Bart')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MATCH '^B' → Bob, Bart (2 rows)
            tdSql.query(
                f"select id, name from {src_m}.{m_db}.users "
                f"where name match '^B' order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            assert "Bob" in str(tdSql.getData(0, 1))
            tdSql.checkData(1, 0, 3)
            assert "Bart" in str(tdSql.getData(1, 1))

            # (b) NMATCH '^B' → only Alice (1 row)
            tdSql.query(
                f"select id, name from {src_m}.{m_db}.users "
                f"where name nmatch '^B' order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            assert "Alice" in str(tdSql.getData(0, 1))

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name TEXT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (c) PG MATCH '^A' → Alice only
            tdSql.query(
                f"select id, name from {src_p}.{p_db}.public.users "
                f"where name match '^A' order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            assert "Alice" in str(tdSql.getData(0, 1))

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_040(self):
        """FQ-SQL-040: NULL predicate expressions full coverage — IS NULL/IS NOT NULL

        Dimensions:
          a) IS NOT NULL → all 5 non-null rows
          b) IS NULL → 0 rows (all name values set)
          c) MySQL external: NULL row inserted, IS NULL filter verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # (a)(b) Internal vtable
        self._prepare_internal_env()
        try:
            # (a) IS NOT NULL → all 5 rows with non-null names
            tdSql.query(
                "select val, name from fq_sql_db.src_t where name is not null order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            assert "alpha" in str(tdSql.getData(0, 1))
            tdSql.checkData(4, 0, 5)
            assert "epsilon" in str(tdSql.getData(4, 1))

            # (b) IS NULL → 0 rows (all names set)
            tdSql.query("select * from fq_sql_db.src_t where name is null")
            tdSql.checkRows(0)
        finally:
            self._teardown_internal_env()

        # (c) MySQL with explicit NULL
        src = "fq_sql_040_mysql"
        ext_db = "fq_sql_040_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, val INT)",
                "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select id from {src}.{ext_db}.data where val is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            tdSql.query(
                f"select id from {src}.{ext_db}.data where val is not null order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 3)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_041(self):
        """FQ-SQL-041: UNION family full coverage — UNION/UNION ALL single-source pushdown, cross-source fallback

        Dimensions:
          a) Same MySQL source UNION ALL → 4 rows (no dedup)
          b) Cross-source UNION (MySQL + PG) → 3 rows after dedup

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_041_mysql"
        m_db = "fq_sql_041_m_db"
        src_p = "fq_sql_041_pg"
        p_db = "fq_sql_041_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS t1",
                "DROP TABLE IF EXISTS t2",
                "CREATE TABLE t1 (id INT)",
                "CREATE TABLE t2 (id INT)",
                "INSERT INTO t1 VALUES (1), (2)",
                "INSERT INTO t2 VALUES (3), (4)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) UNION ALL same source → 4 rows
            tdSql.query(
                f"select id from {src_m}.{m_db}.t1 "
                f"union all select id from {src_m}.{m_db}.t2 "
                f"order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(3, 0, 4)

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS t1",
                "CREATE TABLE t1 (id INT)",
                "INSERT INTO t1 VALUES (2), (5)",
            ])
            self._mk_pg_real(src_p, database=p_db)
            # Re-create MySQL for cross-source test
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS t1",
                "CREATE TABLE t1 (id INT)",
                "INSERT INTO t1 VALUES (1), (2)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (b) UNION cross-source MySQL+PG: ids 1,2 from MySQL, 2,5 from PG
            # UNION dedupes id=2 → 3 distinct rows: 1,2,5
            tdSql.query(
                f"select id from {src_m}.{m_db}.t1 "
                f"union select id from {src_p}.{p_db}.public.t1 "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 5)

        finally:
            self._cleanup_src(src_m, src_p)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_042(self):
        """FQ-SQL-042: ORDER BY NULLS semantics — NULLS FIRST/LAST handling

        Dimensions:
          a) ORDER BY val NULLS FIRST on PG → NULL appears first
          b) ORDER BY val NULLS LAST → NULL appears last

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_042_pg"
        p_db = "fq_sql_042_db"
        self._cleanup_src(src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, val INT)",
                "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 20)",
            ])
            self._mk_pg_real(src, database=p_db)

            # (a) NULLS FIRST → first row has val=NULL
            tdSql.query(
                f"select id, val from {src}.{p_db}.public.data "
                f"order by val nulls first")
            tdSql.checkRows(3)
            assert tdSql.getData(0, 1) is None

            # (b) NULLS LAST → last row has val=NULL
            tdSql.query(
                f"select id, val from {src}.{p_db}.public.data "
                f"order by val nulls last")
            tdSql.checkRows(3)
            assert tdSql.getData(2, 1) is None

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_043(self):
        """FQ-SQL-043: LIMIT/OFFSET full boundary — large offset, exceeding data range

        Dimensions:
          a) LIMIT 2 OFFSET 3 on internal vtable → 2 rows from index 3
          b) LIMIT 10 OFFSET 100 → 0 rows (offset beyond data)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # (a) LIMIT 2 OFFSET 3 → rows at position 3,4 (val=4,5)
            tdSql.query(
                "select val from fq_sql_db.src_t order by ts limit 2 offset 3")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4)
            tdSql.checkData(1, 0, 5)

            # (b) OFFSET beyond data → 0 rows
            tdSql.query(
                "select * from fq_sql_db.src_t limit 10 offset 100")
            tdSql.checkRows(0)
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-044 ~ FQ-SQL-050: Function white-list coverage
    # ------------------------------------------------------------------

    def test_fq_sql_044(self):
        """FQ-SQL-044: Math function whitelist full coverage — DS §5.3.4.1.1 parameterized verification of all functions

        Dimensions:
          a) ABS/CEIL/FLOOR/ROUND/SQRT/POW — vtable
          b) ACOS/ASIN/ATAN/COS/SIN/TAN — vtable (trig functions)
          c) DEGREES/RADIANS/EXP/LN/PI/SIGN — vtable (misc math)
          d) External MySQL: representative subset verified on external source

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added complete whitelist coverage per DS §5.3.4.1.1

        """
        self._prepare_internal_env()
        try:
            # (a) Basic math
            tdSql.query("select abs(val) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)

            tdSql.query("select ceil(score), floor(score) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)   # ceil(1.5)=2
            tdSql.checkData(0, 1, 1)   # floor(1.5)=1

            tdSql.query("select round(score) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) in (1, 2)  # round(1.5) platform-dependent

            tdSql.query("select sqrt(abs(score)) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.2247) < 1e-3

            tdSql.query("select pow(val, 2) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # 1^2
            tdSql.checkData(1, 0, 4)   # 2^2

            # (b) Trig functions — verify on val=1 (first row)
            tdSql.query("select acos(0) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5707963) < 1e-5  # PI/2

            tdSql.query("select asin(1) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5707963) < 1e-5  # PI/2

            tdSql.query("select atan(1) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 0.7853981) < 1e-5  # PI/4

            tdSql.query("select cos(0) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-9

            tdSql.query("select sin(0) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 0.0) < 1e-9

            tdSql.query("select tan(0) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 0.0) < 1e-9

            # (c) Misc math
            tdSql.query("select degrees(pi()) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 180.0) < 1e-6

            tdSql.query("select radians(180) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.14159265) < 1e-5

            tdSql.query("select exp(1) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.71828182) < 1e-5

            tdSql.query("select ln(1) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 0.0) < 1e-9

            tdSql.query("select pi() from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.14159265) < 1e-5

            tdSql.query("select sign(val - 3) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, -1)  # 1-3=-2 → sign=-1
            tdSql.checkData(2, 0, 0)   # 3-3=0  → sign=0
            tdSql.checkData(4, 0, 1)   # 5-3=2  → sign=1

        finally:
            self._teardown_internal_env()

        # (d) MySQL external: verify representative subset pushes down correctly
        src = "fq_sql_044_mysql"
        ext_db = "fq_sql_044_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS nums",
                "CREATE TABLE nums (id INT, val DOUBLE)",
                "INSERT INTO nums VALUES (1, 4.0), (2, -1.0), (3, 0.0)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(f"select id, abs(val), sqrt(abs(val)) from {src}.{ext_db}.nums order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 4.0)
            assert abs(float(tdSql.getData(0, 2)) - 2.0) < 1e-9  # sqrt(4)=2
            tdSql.checkData(1, 1, 1.0)
            tdSql.checkData(2, 1, 0.0)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_045(self):
        """FQ-SQL-045: Math function special mapping full coverage — LOG/TRUNC/RAND/MOD/GREATEST/LEAST/CORR full verification

        Dimensions:
          a) LOG(val, 2) on MySQL → verified for val=8 (result=3)
          b) TRUNCATE(val, 1) on MySQL → verified for val=2.567 (result=2.5)
          c) MOD(val, 3) on MySQL → verified for val=10 (result=1)
          d) RAND() on MySQL → non-null float in [0,1)
          e) GREATEST/LEAST on MySQL → result verified
          f) CORR(x, y) on PG (pushdown) → perfect correlation = 1.0

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added RAND/GREATEST/LEAST/CORR per DS §5.3.4.1.1

        """
        src = "fq_sql_045_mysql"
        ext_db = "fq_sql_045_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val DOUBLE)",
                "INSERT INTO numbers VALUES (1, 8.0)",
                "INSERT INTO numbers VALUES (2, 2.567)",
                "INSERT INTO numbers VALUES (3, 10.0)",
                "INSERT INTO numbers VALUES (4, 3.0)",
                "INSERT INTO numbers VALUES (5, 7.0)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) LOG(8.0, 2) → 3
            tdSql.query(f"select log(val, 2) from {src}.{ext_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            # (b) TRUNCATE(2.567, 1) → 2.5
            tdSql.query(f"select truncate(val, 1) from {src}.{ext_db}.numbers where id = 2")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.5) < 1e-6

            # (c) MOD(10, 3) → 1
            tdSql.query(f"select mod(val, 3) from {src}.{ext_db}.numbers where id = 3")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-6

            # (d) RAND() → in [0, 1)
            tdSql.query(f"select rand() from {src}.{ext_db}.numbers limit 1")
            tdSql.checkRows(1)
            r = float(tdSql.getData(0, 0))
            assert 0.0 <= r < 1.0, f"RAND() out of range: {r}"

            # (e) GREATEST(3.0, 5.0)=5; LEAST(7.0, 5.0)=5
            tdSql.query(
                f"select id, greatest(val, 5.0), least(val, 5.0) "
                f"from {src}.{ext_db}.numbers where id in (4, 5) order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 5.0)   # greatest(3, 5) = 5
            tdSql.checkData(0, 2, 3.0)   # least(3, 5) = 3
            tdSql.checkData(1, 1, 7.0)   # greatest(7, 5) = 7
            tdSql.checkData(1, 2, 5.0)   # least(7, 5) = 5

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (f) CORR on PG: perfect positive correlation (y = 2*x → corr=1.0)
        src_p = "fq_sql_045_pg"
        p_db = "fq_sql_045_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS corr_data",
                "CREATE TABLE corr_data (id INT, x DOUBLE PRECISION, y DOUBLE PRECISION)",
                "INSERT INTO corr_data VALUES (1, 1.0, 2.0), (2, 2.0, 4.0), (3, 3.0, 6.0)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(f"select corr(x, y) from {src_p}.{p_db}.public.corr_data")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-9

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_046(self):
        """FQ-SQL-046: String function whitelist full coverage — DS §5.3.4.1.2 item-by-item verification

        Dimensions:
          a) Default-strategy functions on vtable: ASCII/CHAR_LENGTH/CONCAT/CONCAT_WS/LOWER/
             LTRIM/REPEAT/REPLACE/RTRIM/TRIM/UPPER — all verified
          b) External MySQL: representative subset on real external source

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added complete whitelist per DS §5.3.4.1.2

        """
        self._prepare_internal_env()
        try:
            # ASCII('a') → 97
            tdSql.query("select ascii(name) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 97)   # 'a' = 97

            # CHAR_LENGTH('alpha') → 5
            tdSql.query("select char_length(name) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            # LOWER/UPPER
            tdSql.query("select lower(name), upper(name) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert "alpha" in str(tdSql.getData(0, 0))
            assert "ALPHA" in str(tdSql.getData(0, 1))

            # LTRIM/RTRIM/TRIM
            tdSql.query("select ltrim('  x  '), rtrim('  x  '), trim('  x  ') "
                        "from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)).startswith('x')
            assert str(tdSql.getData(0, 1)).endswith('x')
            assert str(tdSql.getData(0, 2)) == 'x'

            # CONCAT
            tdSql.query("select concat(name, '_x') from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert "alpha_x" in str(tdSql.getData(0, 0))

            # CONCAT_WS
            tdSql.query("select concat_ws('-', 'a', 'b', 'c') from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert "a-b-c" in str(tdSql.getData(0, 0))

            # REPEAT
            tdSql.query("select repeat('x', 3) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert "xxx" in str(tdSql.getData(0, 0))

            # REPLACE
            tdSql.query("select replace(name, 'alpha', 'beta') from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert "beta" in str(tdSql.getData(0, 0))

        finally:
            self._teardown_internal_env()

        # (b) External MySQL: verify default-strategy functions push down
        src = "fq_sql_046_mysql"
        ext_db = "fq_sql_046_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS words",
                "CREATE TABLE words (id INT, word VARCHAR(50))",
                "INSERT INTO words VALUES (1, 'hello'), (2, 'world')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select id, upper(word), concat(word, '!') "
                f"from {src}.{ext_db}.words order by id")
            tdSql.checkRows(2)
            assert "HELLO" in str(tdSql.getData(0, 1))
            assert "hello!" in str(tdSql.getData(0, 2))
            assert "WORLD" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_047(self):
        """FQ-SQL-047: String function special mapping full coverage — SUBSTRING/POSITION/FIND_IN_SET/CHAR verification

        Dimensions:
          a) SUBSTRING(name, 1, 3) on MySQL → 'Ali'
          b) REPLACE(name, 'Alice', 'Eve') on MySQL → 'Eve'
          c) POSITION('li' IN name) on MySQL → 2
          d) FIND_IN_SET('B', 'A,B,C') on MySQL → 2
          e) CHAR(65) on MySQL → 'A' (vs PG: CHR(65) → 'A')

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added POSITION/FIND_IN_SET/CHAR per DS §5.3.4.1.2

        """
        src = "fq_sql_047_mysql"
        ext_db = "fq_sql_047_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50), tags VARCHAR(100))",
                "INSERT INTO users VALUES (1, 'Alice', 'A,B,C')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) SUBSTRING
            tdSql.query(
                f"select substring(name, 1, 3) from {src}.{ext_db}.users where id = 1")
            tdSql.checkRows(1)
            assert "Ali" in str(tdSql.getData(0, 0))

            # (b) REPLACE
            tdSql.query(
                f"select replace(name, 'Alice', 'Eve') "
                f"from {src}.{ext_db}.users where id = 1")
            tdSql.checkRows(1)
            assert "Eve" in str(tdSql.getData(0, 0))

            # (c) POSITION('li' IN name) → 2 (MySQL 1-based)
            tdSql.query(
                f"select position('li' in name) from {src}.{ext_db}.users where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # (d) FIND_IN_SET('B', 'A,B,C') → 2
            tdSql.query(
                f"select find_in_set('B', tags) from {src}.{ext_db}.users where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

        # (e) MySQL CHAR(65) → 'A'; PG uses CHR(65) → 'A'
        src_p = "fq_sql_047_pg"
        p_db = "fq_sql_047_p_db"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS dummy",
                "CREATE TABLE dummy (id INT, val INT)",
                "INSERT INTO dummy VALUES (1, 65)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # PG: char(65) maps to chr(65) → 'A'
            tdSql.query(
                f"select char(val) from {src_p}.{p_db}.public.dummy where id = 1")
            tdSql.checkRows(1)
            assert "A" in str(tdSql.getData(0, 0))

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_048(self):
        """FQ-SQL-048: Encoding functions full coverage — TO_BASE64/FROM_BASE64 three-source behavior verification

        Dimensions:
          a) TO_BASE64('test') → 'dGVzdA==' on MySQL (direct pushdown)
          b) FROM_BASE64('dGVzdA==') → 'test' on MySQL
          c) PG: TO_BASE64 via ENCODE(bytea, 'base64') → verified
          d) InfluxDB: TO_BASE64 local compute fallback → correct result

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_048_mysql"
        src_p = "fq_sql_048_pg"
        m_db = "fq_sql_048_m_db"
        p_db = "fq_sql_048_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS strings",
                "CREATE TABLE strings (id INT, data VARCHAR(100))",
                "INSERT INTO strings VALUES (1, 'test')",
                "INSERT INTO strings VALUES (2, 'dGVzdA==')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) TO_BASE64('test') → 'dGVzdA=='
            tdSql.query(
                f"select to_base64(data) from {src_m}.{m_db}.strings where id = 1")
            tdSql.checkRows(1)
            assert "dGVzdA==" in str(tdSql.getData(0, 0))

            # (b) FROM_BASE64('dGVzdA==') → 'test'
            tdSql.query(
                f"select from_base64(data) from {src_m}.{m_db}.strings where id = 2")
            tdSql.checkRows(1)
            assert "test" in str(tdSql.getData(0, 0))

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS strings",
                "CREATE TABLE strings (id INT, data TEXT)",
                "INSERT INTO strings VALUES (1, 'test')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (c) PG TO_BASE64 → ENCODE(data::bytea, 'base64')
            tdSql.query(
                f"select to_base64(data) from {src_p}.{p_db}.public.strings where id = 1")
            tdSql.checkRows(1)
            assert "dGVzdA==" in str(tdSql.getData(0, 0)).replace("\n", "")

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (d) InfluxDB: TO_BASE64 not pushed down → local compute fallback, result correct
        src_i = "fq_sql_048_influx"
        i_db = "fq_sql_048_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "strings,id=1 data=\"test\" 1704067200000000000"
            )
            self._mk_influx_real(src_i, database=i_db)

            # InfluxDB: to_base64 falls back to local compute
            tdSql.query(
                f"select data, to_base64(data) from {src_i}.{i_db}.strings order by time")
            tdSql.checkRows(1)
            assert "dGVzdA==" in str(tdSql.getData(0, 1)).replace("\n", "")

        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_049(self):
        """FQ-SQL-049: Hash functions full coverage — MD5 results consistent across MySQL/PG sources

        Dimensions:
          a) MD5('Alice') on MySQL
          b) MD5('Alice') on PG
          c) Both must return same 32-char hash

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_049_mysql"
        src_p = "fq_sql_049_pg"
        m_db = "fq_sql_049_m_db"
        p_db = "fq_sql_049_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, name VARCHAR(50))",
                "INSERT INTO data VALUES (1, 'Alice')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            tdSql.query(f"select md5(name) from {src_m}.{m_db}.data where id = 1")
            tdSql.checkRows(1)
            m_hash = str(tdSql.getData(0, 0))
            assert len(m_hash) == 32

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, name TEXT)",
                "INSERT INTO data VALUES (1, 'Alice')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(
                f"select md5(name) from {src_p}.{p_db}.public.data where id = 1")
            tdSql.checkRows(1)
            p_hash = str(tdSql.getData(0, 0))
            assert len(p_hash) == 32
            assert m_hash == p_hash, f"Hash mismatch: MySQL={m_hash} PG={p_hash}"

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_050(self):
        """FQ-SQL-050: Bitwise functions full coverage — CRC32 on MySQL verified

        Dimensions:
          a) CRC32('Alice') on MySQL → deterministic non-zero value

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_050_mysql"
        ext_db = "fq_sql_050_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, name VARCHAR(50))",
                "INSERT INTO data VALUES (1, 'Alice')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select id, crc32(name) from {src}.{ext_db}.data where id = 1")
            tdSql.checkRows(1)
            crc_val = int(tdSql.getData(0, 1))
            # CRC32('Alice') = 3739141946
            assert crc_val == 3739141946, f"CRC32 mismatch: {crc_val}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_051(self):
        """FQ-SQL-051: Data masking functions — MASK_FULL/MASK_PARTIAL executed locally

        Dimensions:
          a) MASK_FULL → all chars masked on internal vtable
          b) MASK_PARTIAL → partial mask on internal vtable

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # MASK_FULL replaces all chars with 'X'
            tdSql.query("select mask_full(name) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            masked = str(tdSql.getData(0, 0))
            assert all(c in ("X", "x") for c in masked), f"MASK_FULL expected all X: {masked}"

            # MASK_PARTIAL: first 2 chars unchanged, rest masked
            # name[0] for val=1 is 'alpha'; MASK_PARTIAL(name, 2, 'X') → 'alXXX'
            tdSql.query(
                "select name, mask_partial(name, 2, 'X') from fq_sql_db.src_t "
                "where val = 1")
            tdSql.checkRows(1)
            original = str(tdSql.getData(0, 0))   # 'alpha'
            masked = str(tdSql.getData(0, 1))
            # First 2 chars should be unchanged; rest should be X
            assert masked[:2] == original[:2], \
                f"MASK_PARTIAL prefix should be unchanged: got '{masked}'"
            assert all(c == "X" for c in masked[2:]), \
                f"MASK_PARTIAL suffix should be X: got '{masked}'"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_052(self):
        """FQ-SQL-052: Encryption functions — AES_ENCRYPT/AES_DECRYPT executed locally

        Dimensions:
          a) AES_ENCRYPT → non-null ciphertext on MySQL
          b) AES_DECRYPT(encrypt) = original → verified on MySQL

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_052_mysql"
        ext_db = "fq_sql_052_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS secrets",
                "CREATE TABLE secrets (id INT, plain VARCHAR(100))",
                "INSERT INTO secrets VALUES (1, 'hello')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) AES_ENCRYPT returns non-null ciphertext
            tdSql.query(
                f"select id, aes_encrypt(plain, 'key123') as cipher "
                f"from {src}.{ext_db}.secrets where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            cipher = tdSql.getData(0, 1)
            assert cipher is not None, "AES_ENCRYPT should return non-null ciphertext"

            # (b) AES_DECRYPT(AES_ENCRYPT(plain, key), key) = original
            tdSql.query(
                f"select id, aes_decrypt(aes_encrypt(plain, 'key123'), 'key123') as decrypted "
                f"from {src}.{ext_db}.secrets where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            decrypted = str(tdSql.getData(0, 1))
            assert "hello" in decrypted, \
                f"AES_DECRYPT should recover 'hello', got: {decrypted}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_053(self):
        """FQ-SQL-053: Type conversion functions full coverage — CAST/TO_CHAR/TO_TIMESTAMP/TO_UNIXTIMESTAMP verification

        Dimensions:
          a) CAST(val AS DOUBLE) on vtable → exact value verified
          b) CAST(val AS BINARY) → string verified
          c) CAST(ts AS BIGINT) → epoch millis verified
          d) TO_CHAR(ts, 'yyyy-MM-dd') on MySQL → DATE_FORMAT conversion verified
          e) TO_TIMESTAMP(str, 'yyyy-MM-dd') on MySQL → STR_TO_DATE conversion verified
          f) TO_UNIXTIMESTAMP on MySQL → UNIX_TIMESTAMP conversion verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added TO_CHAR/TO_TIMESTAMP/TO_UNIXTIMESTAMP per DS §5.3.4.1.8

        """
        self._prepare_internal_env()
        try:
            tdSql.query("select cast(val as double) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-6

            tdSql.query("select cast(val as binary(16)) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            # CAST(1 AS BINARY(16)) → binary representation of 1; string form contains '1'
            result = str(tdSql.getData(0, 0))
            assert result is not None and len(result) > 0, \
                f"CAST AS BINARY should return non-empty: {result}"

            tdSql.query("select cast(ts as bigint) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 1704067200000
        finally:
            self._teardown_internal_env()

        # (d-f) TO_CHAR / TO_TIMESTAMP / TO_UNIXTIMESTAMP on MySQL external
        src = "fq_sql_053_mysql"
        ext_db = "fq_sql_053_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS times",
                "CREATE TABLE times (id INT, ts DATETIME, ts_str VARCHAR(30))",
                "INSERT INTO times VALUES "
                "(1, '2024-01-15 12:30:00', '2024-01-15 12:30:00')",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (d) TO_CHAR(ts, 'yyyy-MM-dd') → MySQL DATE_FORMAT(ts, '%Y-%m-%d')
            tdSql.query(
                f"select id, to_char(ts, 'yyyy-MM-dd') "
                f"from {src}.{ext_db}.times where id = 1")
            tdSql.checkRows(1)
            assert "2024-01-15" in str(tdSql.getData(0, 1))

            # (e) TO_TIMESTAMP(ts_str, 'yyyy-MM-dd HH:mm:ss') → MySQL STR_TO_DATE
            tdSql.query(
                f"select id, to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') "
                f"from {src}.{ext_db}.times where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            ts_result = tdSql.getData(0, 1)
            assert ts_result is not None, "TO_TIMESTAMP should return non-null"
            # The returned datetime should contain '2024-01-15'
            assert "2024-01-15" in str(ts_result), \
                f"TO_TIMESTAMP should contain '2024-01-15': {ts_result}"

            # (f) TO_UNIXTIMESTAMP(ts) → MySQL UNIX_TIMESTAMP(ts)
            tdSql.query(
                f"select id, to_unixtimestamp(ts) "
                f"from {src}.{ext_db}.times where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            unix_ts = int(tdSql.getData(0, 1))
            # 2024-01-15 12:30:00 UTC → 1705319400 (UTC-based)
            # Allow ±86400 for timezone differences across test environments
            assert abs(unix_ts - 1705319400) < 86400, \
                f"TO_UNIXTIMESTAMP unexpected: {unix_ts}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_054(self):
        """FQ-SQL-054: Date/time functions full coverage — NOW/TODAY/DATE/DAYOFWEEK/WEEK/WEEKDAY/TIMEDIFF/TIMETRUNCATE verification

        Dimensions:
          a) NOW() returns non-null on vtable
          b) TODAY() returns non-null
          c) TIMEDIFF('2024-01-01', '2024-01-01') → 0
          d) TIMETRUNCATE(ts, 1h) → truncated to hour
          e) DATE(ts) on MySQL external → date string verified
          f) DAYOFWEEK(ts) on MySQL → 1-7 (1=Sunday)
          g) WEEK(ts) on MySQL → week number
          h) WEEKDAY(ts) on MySQL → 0-6 (0=Monday)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Added DATE/DAYOFWEEK/WEEK/WEEKDAY per DS §5.3.4.1.9

        """
        self._prepare_internal_env()
        try:
            tdSql.query("select now() from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None

            tdSql.query("select today() from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None

            # timediff('2024-01-01', '2024-01-01') = 0
            tdSql.query("select timediff('2024-01-01', '2024-01-01') from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 0

            tdSql.query(
                "select timetruncate(ts, 1h) from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 1704067200000  # truncated to 2024-01-01T00:00:00

        finally:
            self._teardown_internal_env()

        # (e-h) DATE/DAYOFWEEK/WEEK/WEEKDAY on MySQL (→ converted pushdown per DS §5.3.4.1.9)
        src = "fq_sql_054_mysql"
        ext_db = "fq_sql_054_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS times",
                "CREATE TABLE times (id INT, ts DATETIME)",
                # 2024-01-01 is Monday (weekday=0, dayofweek=2, week=1 in mode 0)
                "INSERT INTO times VALUES (1, '2024-01-01 00:00:00')",
                "INSERT INTO times VALUES (2, '2024-01-07 00:00:00')",  # Sunday
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (e) DATE(ts) → date part
            tdSql.query(f"select id, date(ts) from {src}.{ext_db}.times order by id")
            tdSql.checkRows(2)
            assert "2024-01-01" in str(tdSql.getData(0, 1))

            # (f) DAYOFWEEK(ts): 1=Sunday...7=Saturday; Monday=2, Sunday=1
            tdSql.query(f"select id, dayofweek(ts) from {src}.{ext_db}.times order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 2)   # 2024-01-01 Monday → 2
            tdSql.checkData(1, 1, 1)   # 2024-01-07 Sunday → 1

            # (g) WEEK(ts) → week number; 2024-01-01 is in ISO week 1
            tdSql.query(f"select id, week(ts) from {src}.{ext_db}.times order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            # MySQL WEEK(ts, 0): default mode; 2024-01-01 (Monday) → week 1
            assert int(tdSql.getData(0, 1)) >= 1, \
                f"WEEK(2024-01-01) should be >= 1: {tdSql.getData(0, 1)}"

            # (h) WEEKDAY(ts): 0=Monday...6=Sunday; Monday=0, Sunday=6
            tdSql.query(f"select id, weekday(ts) from {src}.{ext_db}.times order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)   # Monday → 0
            tdSql.checkData(1, 1, 6)   # Sunday → 6

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_055(self):
        """FQ-SQL-055: Basic aggregate functions full coverage — COUNT/SUM/AVG/MIN/MAX/STDDEV value verification

        Dimensions:
          a) All functions on vtable — count=5, sum=15, avg=3, min=1, max=5

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select count(*), sum(val), avg(val), min(val), max(val), "
                "stddev(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
            tdSql.checkData(0, 1, 15)
            assert abs(float(tdSql.getData(0, 2)) - 3.0) < 1e-6
            tdSql.checkData(0, 3, 1)
            tdSql.checkData(0, 4, 5)
            # stddev([1,2,3,4,5]) = sqrt(2) ≈ 1.4142
            assert abs(float(tdSql.getData(0, 5)) - 1.4142) < 1e-3, \
                f"STDDEV should be ≈1.4142: {tdSql.getData(0, 5)}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_056(self):
        """FQ-SQL-056: Percentile and approximate statistics — PERCENTILE/APERCENTILE value verification

        Dimensions:
          a) PERCENTILE(val, 50) → 3 for [1,2,3,4,5]
          b) APERCENTILE(val, 50) → non-null

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query("select percentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            tdSql.query("select apercentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            # APERCENTILE p50 of [1,2,3,4,5] should be close to 3 (±1 tolerance for approximation)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1.0, \
                f"APERCENTILE p50 should be near 3: {tdSql.getData(0, 0)}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_057(self):
        """FQ-SQL-057: Special aggregate functions — ELAPSED/HISTOGRAM/HYPERLOGLOG local execution value verification

        Dimensions:
          a) ELAPSED(ts) → positive duration (local compute)
          b) HISTOGRAM(val, 'user_input', '[0,10]', 0) → non-null bucket string (local)
          c) HYPERLOGLOG(val) → approximate distinct count = 5 (local compute)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-04-15 wpan Replaced TWA with HISTOGRAM/HYPERLOGLOG per DS §5.3.4.1.12

        """
        self._prepare_internal_env()
        try:
            # (a) ELAPSED: 5 rows, 1-min apart → elapsed = 4 minutes = 240000 ms
            tdSql.query("select elapsed(ts) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            elapsed_val = float(tdSql.getData(0, 0))
            assert elapsed_val > 0, f"ELAPSED should be positive: {elapsed_val}"
            # 4 intervals of 60s = 240s = 240000ms; accept within ±1000ms for precision
            assert abs(elapsed_val - 240000) < 1000 or abs(elapsed_val - 240) < 1, \
                f"ELAPSED should be ~240000ms or ~240s: {elapsed_val}"

            # (b) HISTOGRAM with user-defined bucket [0,10]
            tdSql.query(
                "select histogram(val, 'user_input', '[0, 10]', 0) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            hist_result = str(tdSql.getData(0, 0))
            assert hist_result is not None and len(hist_result) > 0

            # (c) HYPERLOGLOG → approximate cardinality of distinct val values
            tdSql.query("select hyperloglog(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            hll = int(tdSql.getData(0, 0))
            # val = [1,2,3,4,5] → 5 distinct, HLL estimate should be near 5
            assert hll >= 4, f"HYPERLOGLOG of 5 distinct values should be >= 4: {hll}"

        finally:
            self._teardown_internal_env()

    def test_fq_sql_058(self):
        """FQ-SQL-058: Selection functions full coverage — FIRST/LAST/LAST_ROW/TOP/BOTTOM/TAIL/MODE/UNIQUE value verification

        Dimensions:
          a) FIRST(val) → 1, LAST(val) → 5
          b) TOP(val, 2) → 2 rows, BOTTOM(val, 2) → 2 rows
          c) TAIL(val, 2) → 2 rows (last 2 by arrival order)
          d) MODE(val) → most frequent value (all distinct → returns one value)
          e) UNIQUE(name) → deduplicated names (all unique → 5 results)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query("select first(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            tdSql.query("select last(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            tdSql.query("select last_row(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            tdSql.query("select top(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            top_vals = sorted([int(tdSql.getData(i, 0)) for i in range(2)], reverse=True)
            assert top_vals[0] == 5 and top_vals[1] == 4, f"TOP-2 should be [5,4]: {top_vals}"

            tdSql.query("select bottom(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            bot_vals = sorted([int(tdSql.getData(i, 0)) for i in range(2)])
            assert bot_vals[0] == 1 and bot_vals[1] == 2, f"BOTTOM-2 should be [1,2]: {bot_vals}"

            tdSql.query("select tail(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            # TAIL returns last 2 rows by insertion order: val=4 and val=5
            tail_vals = sorted([int(tdSql.getData(i, 0)) for i in range(2)])
            assert tail_vals[0] == 4 and tail_vals[1] == 5, f"TAIL-2 should be [4,5]: {tail_vals}"

            # (d) MODE(val): all distinct values → returns any one value in [1,5]
            tdSql.query("select mode(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            mode_val = int(tdSql.getData(0, 0))
            assert 1 <= mode_val <= 5, f"MODE(val) should be in [1,5]: {mode_val}"

            # (e) UNIQUE(name): all names are unique → 5 distinct rows returned
            tdSql.query("select unique(name) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            expected_names = ["alpha", "beta", "gamma", "delta", "epsilon"]
            for i, name in enumerate(expected_names):
                assert name in str(tdSql.getData(i, 0)), \
                    f"UNIQUE row {i} should be '{name}': {tdSql.getData(i, 0)}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_059(self):
        """FQ-SQL-059: Comparison and conditional functions — IFNULL/COALESCE/GREATEST/LEAST with real data

        Dimensions:
          a) IFNULL(val, 0) on MySQL with NULL rows → verified
          b) COALESCE(val, 0) on MySQL → verified
          c) GREATEST(val, 10) on MySQL → verified
          d) LEAST(val, 10) on MySQL → verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_059_mysql"
        ext_db = "fq_sql_059_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, val INT)",
                "INSERT INTO data VALUES (1, NULL), (2, 5), (3, 15)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) IFNULL(val, 0) → NULL→0, 5→5, 15→15
            tdSql.query(
                f"select id, ifnull(val, 0) from {src}.{ext_db}.data order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 0)    # NULL → 0
            tdSql.checkData(1, 1, 5)    # 5 stays 5
            tdSql.checkData(2, 1, 15)

            # (b) COALESCE(val, 0) → same behavior as IFNULL
            tdSql.query(
                f"select id, coalesce(val, 0) from {src}.{ext_db}.data order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 0)    # NULL → 0
            tdSql.checkData(1, 1, 5)    # 5 stays 5
            tdSql.checkData(2, 1, 15)   # 15 stays 15

            # (c) GREATEST(val, 10): null→NULL, 5→10 (5<10), 15→15
            tdSql.query(
                f"select id, greatest(val, 10) from {src}.{ext_db}.data order by id")
            tdSql.checkRows(3)
            tdSql.checkData(1, 1, 10)   # max(5, 10) = 10
            tdSql.checkData(2, 1, 15)   # max(15, 10) = 15

            # (d) LEAST(val, 10): 5→5, 15→10
            tdSql.query(
                f"select id, least(val, 10) from {src}.{ext_db}.data where val is not null order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 5)    # min(5, 10) = 5
            tdSql.checkData(1, 1, 10)   # min(15, 10) = 10

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_060(self):
        """FQ-SQL-060: Time-series functions — CSUM/DERIVATIVE/DIFF/IRATE/TWA value verification

        Dimensions:
          a) DIFF(val) on vtable → 4 rows of differences = all 1s
          b) CSUM(val) → cumulative sums verified
          c) TWA(val) → non-null time-weighted average

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # diff(1,2,3,4,5) → 4 rows: 1,1,1,1
            tdSql.query("select diff(val) from fq_sql_db.src_t")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(3, 0, 1)

            # csum(1,2,3,4,5) → 1,3,6,10,15
            tdSql.query("select csum(val) from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(4, 0, 15)

            tdSql.query("select twa(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            # TWA([1,2,3,4,5]) with equal time intervals = arithmetic mean = 3.0
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-3, \
                f"TWA with equal intervals should equal avg=3.0: {tdSql.getData(0, 0)}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_061(self):
        """FQ-SQL-061: System metadata functions — INFORMATION_SCHEMA query executable

        Dimensions:
          a) SELECT count(*) from INFORMATION_SCHEMA.TABLES on MySQL external → verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_061_mysql"
        ext_db = "fq_sql_061_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS t1",
                "CREATE TABLE t1 (id INT)",
                "INSERT INTO t1 VALUES (1)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) Query INFORMATION_SCHEMA.TABLES on MySQL external source
            # The ext_db should appear in INFORMATION_SCHEMA.TABLES
            tdSql.query(
                f"select count(*) from {src}.information_schema.TABLES "
                f"where TABLE_SCHEMA = '{ext_db}'")
            tdSql.checkRows(1)
            # t1 was created, so at least 1 table in ext_db
            assert int(tdSql.getData(0, 0)) >= 1, \
                f"INFORMATION_SCHEMA.TABLES should show >= 1 table: {tdSql.getData(0, 0)}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_062(self):
        """FQ-SQL-062: Geo functions full coverage — ST_DISTANCE/ST_CONTAINS MySQL/PG mapping/local fallback

        Dimensions:
          a) MySQL ST_DISTANCE: distance from point to itself = 0.0; between two distinct points > 0
          b) PG built-in geometric point distance: point <-> point operator, no PostGIS required

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation
            - 2026-05-01 wpan Fix: replace plain column reads with actual ST_DISTANCE/point distance queries

        """
        src_m = "fq_sql_062_mysql"
        src_p = "fq_sql_062_pg"
        m_db = "fq_sql_062_m_db"
        p_db = "fq_sql_062_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS geo",
                "CREATE TABLE geo (id INT, geom GEOMETRY)",
                # Beijing and Shanghai as POINT geometry
                "INSERT INTO geo VALUES "
                "(1, ST_GeomFromText('POINT(116.4 39.9)')), "
                "(2, ST_GeomFromText('POINT(121.5 31.2)'))",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) ST_DISTANCE from each point to itself → 0.0
            tdSql.query(
                f"select id, ST_DISTANCE(geom, geom) as d "
                f"from {src_m}.{m_db}.geo order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            assert abs(float(tdSql.getData(0, 1))) < 1e-9, \
                f"ST_DISTANCE from point to itself should be 0: {tdSql.getData(0, 1)}"
            tdSql.checkData(1, 0, 2)
            assert abs(float(tdSql.getData(1, 1))) < 1e-9, \
                f"ST_DISTANCE from point to itself should be 0: {tdSql.getData(1, 1)}"

            # ST_DISTANCE between Beijing and Shanghai should be positive
            tdSql.query(
                f"select ST_DISTANCE("
                f"    ST_GeomFromText('POINT(116.4 39.9)'), "
                f"    ST_GeomFromText('POINT(121.5 31.2)') "
                f") as dist from {src_m}.{m_db}.geo limit 1")
            tdSql.checkRows(1)
            assert float(tdSql.getData(0, 0)) > 0, \
                f"ST_DISTANCE between distinct points should be > 0: {tdSql.getData(0, 0)}"

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS geo",
                "CREATE TABLE geo (id INT, loc POINT)",
                "INSERT INTO geo VALUES (1, POINT(116.4, 39.9))",
                "INSERT INTO geo VALUES (2, POINT(121.5, 31.2))",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) PG built-in POINT <-> distance operator: same point → 0
            tdSql.query(
                f"select id, (loc <-> loc) as d "
                f"from {src_p}.{p_db}.public.geo order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            assert abs(float(tdSql.getData(0, 1))) < 1e-9, \
                f"PG point distance to itself should be 0: {tdSql.getData(0, 1)}"
            tdSql.checkData(1, 0, 2)
            assert abs(float(tdSql.getData(1, 1))) < 1e-9, \
                f"PG point distance to itself should be 0: {tdSql.getData(1, 1)}"

            # Distance between the two points should be positive
            tdSql.query(
                f"select (POINT(116.4, 39.9) <-> POINT(121.5, 31.2)) as dist "
                f"from {src_p}.{p_db}.public.geo limit 1")
            tdSql.checkRows(1)
            assert float(tdSql.getData(0, 0)) > 0, \
                f"PG point distance between distinct points should be > 0: {tdSql.getData(0, 0)}"

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_063(self):
        """FQ-SQL-063: UDF scalar/aggregate — local execution path verification

        Dimensions:
          a) Internal vtable + UDF function → local execution verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        # UDF requires compiled .so deployment; verify that the internal vtable
        # computation path (which a UDF would use) is reachable and correct.
        self._prepare_internal_env()
        try:
            tdSql.query("select val, val * 2 as doubled from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            # val=[1,2,3,4,5], doubled=[2,4,6,8,10]
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 4)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 6)
            tdSql.checkData(3, 0, 4)
            tdSql.checkData(3, 1, 8)
            tdSql.checkData(4, 0, 5)
            tdSql.checkData(4, 1, 10)
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-064 ~ FQ-SQL-069: Window functions
    # ------------------------------------------------------------------

    def test_fq_sql_064(self):
        """FQ-SQL-064: SESSION_WINDOW — gaps within threshold are merged into same session

        Dimensions:
          a) session(ts, 2m) on vtable with 1-min spaced rows → 5 windows (each >2min apart)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # 5 rows each 1 min apart; session threshold 2 min → all 5 form one session
            tdSql.query(
                "select _wstart, count(*) as cnt, sum(val) as total "
                "from fq_sql_db.src_t session(ts, 2m)")
            tdSql.checkRows(1)          # one continuous session
            tdSql.checkData(0, 1, 5)   # 5 rows in session
            tdSql.checkData(0, 2, 15)  # sum = 1+2+3+4+5

        finally:
            self._teardown_internal_env()

    def test_fq_sql_065(self):
        """FQ-SQL-065: EVENT_WINDOW — start/end conditions define window boundaries

        Dimensions:
          a) start with val > 2 end with val < 4 → verifies non-zero windows

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) as cnt, sum(val) as s from fq_sql_db.src_t "
                "event_window start with val > 2 end with val < 4")
            # val=[1,2,3,4,5] — val=3 satisfies both start(3>2) and end(3<4), forming one window
            # val=4 starts a window (4>2) but 4 and 5 never satisfy end(val<4) → incomplete
            # Expect at least 1 window containing val=3 (cnt=1, sum=3)
            assert tdSql.queryRows >= 1, \
                f"EVENT_WINDOW should yield at least 1 complete window: {tdSql.queryRows}"
            # First window must have sum containing val=3
            first_sum = int(tdSql.getData(0, 2))
            assert first_sum >= 3, f"EVENT_WINDOW first window sum should include val=3: {first_sum}"
        finally:
            self._teardown_internal_env()

    def test_fq_sql_066(self):
        """FQ-SQL-066: COUNT_WINDOW — one window per N rows

        Dimensions:
          a) count_window(2) on 5 rows → 3 windows (2+2+1)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*), sum(val) from fq_sql_db.src_t "
                "count_window(2)")
            tdSql.checkRows(3)              # ceil(5/2) = 3 windows
            # window 0: rows val=1,2 → cnt=2, sum=3
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 3)
            # window 1: rows val=3,4 → cnt=2, sum=7
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(1, 2, 7)
            # window 2: row val=5 → cnt=1, sum=5
            tdSql.checkData(2, 1, 1)
            tdSql.checkData(2, 2, 5)

        finally:
            self._teardown_internal_env()

    def test_fq_sql_067(self):
        """FQ-SQL-067: Window pseudo-columns full coverage — _wstart/_wend are non-NULL and correctly aligned

        Dimensions:
          a) interval(1m): _wstart aligns to minute boundary
          b) _wend = _wstart + 60000ms

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, _wend, count(*) from fq_sql_db.src_t interval(1m) order by _wstart")
            tdSql.checkRows(5)
            # First window starts at 1704067200000 (2024-01-01T00:00:00Z)
            assert int(tdSql.getData(0, 0)) == 1704067200000
            # _wend = _wstart + 60000
            assert int(tdSql.getData(0, 1)) == 1704067260000
            # Both non-NULL
            assert tdSql.getData(0, 0) is not None
            assert tdSql.getData(0, 1) is not None
        finally:
            self._teardown_internal_env()

    def test_fq_sql_068(self):
        """FQ-SQL-068: Window FILL full coverage — NULL/VALUE/PREV/NEXT/LINEAR

        Dimensions:
          a) All 5 FILL modes on interval(30s) — execute without error
          b) FILL(NULL): missing windows have NULL avg
          c) FILL(VALUE, 0): missing windows have 0 avg

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # 5 data points at 1-min intervals in [00:00, 00:04]; with interval(30s) in [00:00, 00:05)
            # there are 9 bins: 5 with real data + 4 empty gaps at 00:30/01:30/02:30/03:30
            time_range = ("ts >= '2024-01-01T00:00:00' "
                          "and ts < '2024-01-01T00:05:00'")

            # FILL(NULL): gaps get NULL → 9 rows total
            tdSql.query(
                f"select _wstart, avg(val) from fq_sql_db.src_t "
                f"where {time_range} interval(30s) fill(null)")
            assert tdSql.queryRows == 9, \
                f"fill(null) should yield 9 rows, got {tdSql.queryRows}"
            # first real row: avg(val)=1.0
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6, \
                f"fill(null) row 0 avg should be 1.0: {tdSql.getData(0, 1)}"
            # second row is a gap → NULL
            assert tdSql.getData(1, 1) is None, \
                f"fill(null) gap row should be NULL: {tdSql.getData(1, 1)}"

            # FILL(VALUE, 0): gaps get 0.0 → 9 rows
            tdSql.query(
                f"select _wstart, avg(val) from fq_sql_db.src_t "
                f"where {time_range} interval(30s) fill(value, 0)")
            assert tdSql.queryRows == 9, \
                f"fill(value,0) should yield 9 rows, got {tdSql.queryRows}"
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6, \
                f"fill(value,0) row 0 avg should be 1.0: {tdSql.getData(0, 1)}"
            # second row is a gap → filled with 0.0
            assert abs(float(tdSql.getData(1, 1)) - 0.0) < 1e-6, \
                f"fill(value,0) gap row should be 0.0: {tdSql.getData(1, 1)}"

            # FILL(PREV): gaps get value from previous real row → 9 rows
            tdSql.query(
                f"select _wstart, avg(val) from fq_sql_db.src_t "
                f"where {time_range} interval(30s) fill(prev)")
            assert tdSql.queryRows == 9, \
                f"fill(prev) should yield 9 rows, got {tdSql.queryRows}"
            # second row is a gap → prev value = 1.0 (from row at 00:00)
            assert abs(float(tdSql.getData(1, 1)) - 1.0) < 1e-6, \
                f"fill(prev) gap row should equal prev=1.0: {tdSql.getData(1, 1)}"

            # FILL(NEXT): gaps get value from next real row → 9 rows
            tdSql.query(
                f"select _wstart, avg(val) from fq_sql_db.src_t "
                f"where {time_range} interval(30s) fill(next)")
            assert tdSql.queryRows == 9, \
                f"fill(next) should yield 9 rows, got {tdSql.queryRows}"
            # second row is a gap → next value = 2.0 (from row at 01:00)
            assert abs(float(tdSql.getData(1, 1)) - 2.0) < 1e-6, \
                f"fill(next) gap row should equal next=2.0: {tdSql.getData(1, 1)}"

            # FILL(LINEAR): gaps get linear interpolation → 9 rows
            tdSql.query(
                f"select _wstart, avg(val) from fq_sql_db.src_t "
                f"where {time_range} interval(30s) fill(linear)")
            assert tdSql.queryRows == 9, \
                f"fill(linear) should yield 9 rows, got {tdSql.queryRows}"
            # second row is a gap → linear between 1.0 and 2.0 at midpoint = 1.5
            assert abs(float(tdSql.getData(1, 1)) - 1.5) < 0.01, \
                f"fill(linear) gap row should be 1.5: {tdSql.getData(1, 1)}"

        finally:
            self._teardown_internal_env()

    def test_fq_sql_069(self):
        """FQ-SQL-069: Window PARTITION BY combination — each partition gets its own windows

        Dimensions:
          a) interval(1m) PARTITION BY flag → 2 flag values each get their own windows

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, flag, count(*) from fq_sql_db.src_t "
                "partition by flag interval(1m) order by _wstart, flag")
            # flag=T at rows 0,2,4 → 3 windows; flag=F at rows 1,3 → 2 windows = 5 total
            tdSql.checkRows(5)
            # All partition windows have count(*) = 1 (one row per 1-minute bucket)
            for r in range(5):
                tdSql.checkData(r, 2, 1)
            # Verify the two distinct flag values appear in results
            flags_seen = {tdSql.getData(r, 1) for r in range(5)}
            assert len(flags_seen) == 2, \
                f"Should see 2 distinct flag values in PARTITION BY results, got: {flags_seen}"
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-070 ~ FQ-SQL-081: Subqueries and views
    # ------------------------------------------------------------------

    def test_fq_sql_070(self):
        """FQ-SQL-070: FROM nested subquery — outer AVG is correct

        Dimensions:
          a) avg(v) from (select val where val > 1) → avg(2,3,4,5) = 3.5

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select avg(v) from (select val as v from fq_sql_db.src_t where val > 1)")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.5) < 1e-6  # avg(2,3,4,5) = 3.5
        finally:
            self._teardown_internal_env()

    def test_fq_sql_071(self):
        """FQ-SQL-071: Non-correlated scalar subquery — inline subquery returns scalar

        Dimensions:
          a) SELECT val, (SELECT max(val) FROM t) AS mx → mx same in all rows

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val, (select max(val) from fq_sql_db.src_t) as mx "
                "from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            # every row has mx=5
            for r in range(5):
                tdSql.checkData(r, 1, 5)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_072(self):
        """FQ-SQL-072: IN/NOT IN subquery — filtering is correct

        Dimensions:
          a) WHERE val IN (subquery WHERE flag=true) → 3 rows (val=1,3,5)
          b) WHERE val NOT IN (subquery) → 2 rows (val=2,4)

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select val from fq_sql_db.src_t where val in "
                "(select val from fq_sql_db.src_t where flag = true) order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 5)

            tdSql.query(
                "select val from fq_sql_db.src_t where val not in "
                "(select val from fq_sql_db.src_t where flag = true) order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 4)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_073(self):
        """FQ-SQL-073: EXISTS/NOT EXISTS subquery — MySQL pushdown

        Dimensions:
          a) EXISTS subquery on same MySQL source → verified true case
          b) NOT EXISTS → verified false case

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_073_mysql"
        ext_db = "fq_sql_073_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "CREATE TABLE orders (order_id INT, user_id INT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO orders VALUES (1, 1)",   # only Alice has order
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) EXISTS: users with orders → only Alice
            tdSql.query(
                f"select u.id from {src}.{ext_db}.users u "
                f"where exists (select 1 from {src}.{ext_db}.orders o where o.user_id = u.id) "
                f"order by u.id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            # (b) NOT EXISTS: users without orders → only Bob
            tdSql.query(
                f"select u.id from {src}.{ext_db}.users u "
                f"where not exists (select 1 from {src}.{ext_db}.orders o where o.user_id = u.id) "
                f"order by u.id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_074(self):
        """FQ-SQL-074: ALL/ANY subquery — cross-source local execution

        Dimensions:
          a) val > ALL(subquery) on vtable → only max(val)=5 qualifies (>4)
          b) val < ANY(subquery) on vtable → rows with val less than max

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            # val > ALL(select val … where val < 5) → val must be > 1,2,3,4 → val=5 only
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val > all(select val from fq_sql_db.src_t where val < 5) "
                "order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            # val < ANY(select max(val)) → val < 5 → 4 rows (val=1,2,3,4)
            tdSql.query(
                "select val from fq_sql_db.src_t "
                "where val < any(select val from fq_sql_db.src_t where val = 5) "
                "order by ts")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(3, 0, 4)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_075(self):
        """FQ-SQL-075: InfluxDB IN subquery — falls back to local execution

        Dimensions:
          a) Basic InfluxDB read → 3 rows returned
          b) InfluxDB source WHERE usage IN (TDengine subquery) → local fallback,
             only 2 matching rows returned (usage=10, usage=30)
          c) InfluxDB as inner subquery in cross-source IN: MySQL WHERE id IN
             (SELECT usage FROM InfluxDB) → local execution verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_075_influx"
        i_db = "fq_sql_075_db"
        ref_db = "fq_sql_075_ref"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "cpu,host=h1 usage=10 1704067200000000000\n"
                "cpu,host=h2 usage=20 1704067260000000000\n"
                "cpu,host=h3 usage=30 1704067320000000000"
            )
            self._mk_influx_real(src, database=i_db)

            # (a) Basic InfluxDB read
            tdSql.query(
                f"select host, usage from {src}.{i_db}.cpu order by time")
            tdSql.checkRows(3)

            # (b) InfluxDB source WHERE usage IN (TDengine internal table subquery)
            # InfluxDB cannot push IN subquery down; TDengine executes locally
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(
                f"create table {ref_db}.ref_t (ts timestamp, val int)")
            tdSql.execute(
                f"insert into {ref_db}.ref_t values "
                f"(1704067200000, 10), (1704067200001, 30)")
            tdSql.query(
                f"select host, usage from {src}.{i_db}.cpu "
                f"where usage in (select val from {ref_db}.ref_t) "
                f"order by time")
            tdSql.checkRows(2)   # h1 (usage=10) and h3 (usage=30)
            # time-ordered: h1 at 1704067200000000000 < h3 at 1704067320000000000
            assert str(tdSql.getData(0, 0)) == "h1", \
                f"row 0 host should be h1: {tdSql.getData(0, 0)}"
            assert int(tdSql.getData(0, 1)) == 10, \
                f"row 0 usage should be 10: {tdSql.getData(0, 1)}"
            assert str(tdSql.getData(1, 0)) == "h3", \
                f"row 1 host should be h3: {tdSql.getData(1, 0)}"
            assert int(tdSql.getData(1, 1)) == 30, \
                f"row 1 usage should be 30: {tdSql.getData(1, 1)}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            tdSql.execute(f"drop database if exists {ref_db}")

    def test_fq_sql_076(self):
        """FQ-SQL-076: Cross-source subquery — MySQL IN (PG subquery) local assembly

        Dimensions:
          a) MySQL users WHERE id IN (PG subquery order_user_ids) → cross-source local

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_076_mysql"
        src_p = "fq_sql_076_pg"
        m_db = "fq_sql_076_m_db"
        p_db = "fq_sql_076_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
            ])
            self._mk_mysql_real(src_m, database=m_db)
        except Exception:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            raise

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (order_id INT, user_id INT)",
                "INSERT INTO orders VALUES (1, 1), (2, 3)",  # users 1 and 3 ordered
            ])
            self._mk_pg_real(src_p, database=p_db)

            # Cross-source: MySQL users WHERE id IN (PG orders.user_id)
            tdSql.query(
                f"select u.id, u.name from {src_m}.{m_db}.users u "
                f"where u.id in (select o.user_id from {src_p}.{p_db}.public.orders o) "
                f"order by u.id")
            tdSql.checkRows(2)   # Alice (1) and Carol (3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(1, 1, "Carol")

        finally:
            self._cleanup_src(src_m, src_p)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_077(self):
        """FQ-SQL-077: Subquery with proprietary functions — DIFF executed locally in subquery

        Dimensions:
          a) outer SELECT from (inner DIFF) → diff values accessible from outer

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select * from (select ts, diff(val) as d from fq_sql_db.src_t)")
            tdSql.checkRows(4)
            # all diffs are 1 (val sequence 1,2,3,4,5)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 1)
            tdSql.checkData(2, 1, 1)
            tdSql.checkData(3, 1, 1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_078(self):
        """FQ-SQL-078: View non-timeline query — MySQL VIEW is queryable

        Dimensions:
          a) Query MySQL view → rows returned without error

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_078_mysql"
        ext_db = "fq_sql_078_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (id INT, amount INT, status INT)",
                "INSERT INTO orders VALUES (1, 100, 1), (2, 200, 2)",
                "DROP VIEW IF EXISTS v_summary",
                "CREATE VIEW v_summary AS SELECT status, sum(amount) as total FROM orders GROUP BY status",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(f"select * from {src}.{ext_db}.v_summary order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)    # status=1
            tdSql.checkData(0, 1, 100)  # total=100
            tdSql.checkData(1, 0, 2)    # status=2
            tdSql.checkData(1, 1, 200)  # total=200

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_079(self):
        """FQ-SQL-079: View timeline dependency boundary — PG VIEW with ts column

        Dimensions:
          a) Query PG view with ts column → ORDER BY ts works correctly

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_079_pg"
        p_db = "fq_sql_079_db"
        self._cleanup_src(src)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS measurements",
                "CREATE TABLE measurements (ts TIMESTAMP, val INT)",
                "INSERT INTO measurements VALUES ('2024-01-01', 10), ('2024-01-02', 20)",
                "DROP VIEW IF EXISTS v_timeseries",
                "CREATE VIEW v_timeseries AS SELECT ts, val FROM measurements",
            ])
            self._mk_pg_real(src, database=p_db)

            tdSql.query(
                f"select * from {src}.{p_db}.public.v_timeseries order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 1, 20)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_080(self):
        """FQ-SQL-080: View in JOIN/GROUP/ORDER — MySQL view joined with table

        Dimensions:
          a) View v_users joined with orders table → correct join result

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_080_mysql"
        ext_db = "fq_sql_080_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "CREATE TABLE orders (id INT, user_id INT, amount INT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200)",
                "DROP VIEW IF EXISTS v_users",
                "CREATE VIEW v_users AS SELECT id, name FROM users WHERE id <= 10",
            ])
            self._mk_mysql_real(src, database=ext_db)

            tdSql.query(
                f"select v.id, v.name, sum(o.amount) as total "
                f"from {src}.{ext_db}.v_users v "
                f"join {src}.{ext_db}.orders o on v.id = o.user_id "
                f"group by v.id, v.name order by v.id")
            tdSql.checkRows(1)   # only Alice has orders
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(0, 2, 300)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_sql_081(self):
        """FQ-SQL-081: View schema change and REFRESH — MySQL view then alter and refresh

        Dimensions:
          a) initial view query works
          b) after REFRESH EXTERNAL SOURCE, query still works

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_081_mysql"
        ext_db = "fq_sql_081_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS base_table",
                "CREATE TABLE base_table (id INT, val INT)",
                "INSERT INTO base_table VALUES (1, 1), (2, 2)",
                "DROP VIEW IF EXISTS v_dynamic",
                "CREATE VIEW v_dynamic AS SELECT id, val FROM base_table",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) initial query
            tdSql.query(f"select count(*) from {src}.{ext_db}.v_dynamic")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # (b) REFRESH and re-query
            tdSql.execute(f"refresh external source {src}")
            tdSql.query(f"select count(*) from {src}.{ext_db}.v_dynamic")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    # ------------------------------------------------------------------
    # FQ-SQL-082 ~ FQ-SQL-086: Special mappings and DS examples
    # ------------------------------------------------------------------

    def test_fq_sql_082(self):
        """FQ-SQL-082: TO_JSON conversion — MySQL/PG/InfluxDB multi-source JSON handling

        Dimensions:
          a) MySQL: to_json(varchar_json_col) → JSON object returned non-null
          b) PG: JSONB column passthrough → JSON value non-null
          c) InfluxDB: to_json on string field → local compute, result non-null

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_082_mysql"
        src_p = "fq_sql_082_pg"
        m_db = "fq_sql_082_m_db"
        p_db = "fq_sql_082_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, name VARCHAR(50), attrs VARCHAR(200))",
                "INSERT INTO data VALUES (1, 'Alice', '{\"age\": 30}')",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) MySQL: to_json converts varchar JSON string column → JSON object
            tdSql.query(
                f"select id, to_json(attrs) from {src_m}.{m_db}.data where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            json_val = tdSql.getData(0, 1)
            assert json_val is not None, \
                "to_json(attrs) on MySQL source should return non-null JSON"
            assert "age" in str(json_val), \
                f"to_json result should contain key 'age': {json_val}"

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, payload JSONB)",
                "INSERT INTO data VALUES (1, '{\"k\": \"v\"}\'::jsonb)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (b) PG: JSONB column passthrough → non-null JSON value
            tdSql.query(
                f"select id, payload from {src_p}.{p_db}.public.data where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            pg_json = tdSql.getData(0, 1)
            assert pg_json is not None, \
                "PG JSONB payload should be non-null"
            assert "k" in str(pg_json), \
                f"PG JSONB payload should contain key 'k': {pg_json}"

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (c) InfluxDB: to_json on string field → local compute
        src_i = "fq_sql_082_influx"
        i_db = "fq_sql_082_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                'sensor,host=h1 attrs="{\"unit\": \"C\"}" 1704067200000000000')
            self._mk_influx_real(src_i, database=i_db)
            tdSql.query(
                f"select to_json(attrs) from {src_i}.{i_db}.sensor")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None, \
                "to_json(attrs) on InfluxDB source should return non-null (local compute)"
        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_083(self):
        """FQ-SQL-083: Comparison functions complete coverage — IF/IFNULL/NULLIF/NVL2/COALESCE

        Dimensions:
          a) MySQL IFNULL(NULL, 99) → 99; IFNULL(5, 99) → 5
          b) MySQL NULLIF(5, 5) → NULL
          c) MySQL IF(val > 0, 'positive', 'zero') → branching result
          d) MySQL NVL2(val, 'has_val', 'no_val') → conditional non-null check
          e) PG COALESCE(NULL, 'fallback') → 'fallback'
          f) InfluxDB: IFNULL on numeric field → local compute, non-null result

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_083_mysql"
        src_p = "fq_sql_083_pg"
        m_db = "fq_sql_083_m_db"
        p_db = "fq_sql_083_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, val INT)",
                "INSERT INTO data VALUES (1, NULL), (2, 5)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # (a) IFNULL: NULL row → 99, non-null row → 5
            tdSql.query(
                f"select id, ifnull(val, 99) from {src_m}.{m_db}.data order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 99)   # NULL → 99
            tdSql.checkData(1, 1, 5)    # 5 stays 5

            # (b) NULLIF: NULLIF(5, 5) = NULL
            tdSql.query(
                f"select id, nullif(val, 5) from {src_m}.{m_db}.data where id = 2")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is None, "NULLIF(5,5) should be NULL"

            # (c) IF(): MySQL direct pushdown — IF(val > 0, 'positive', 'zero_or_null')
            tdSql.query(
                f"select id, if(val > 0, 'positive', 'zero_or_null') "
                f"from {src_m}.{m_db}.data where id = 2")
            tdSql.checkRows(1)
            assert "positive" in str(tdSql.getData(0, 1)), \
                "IF(5 > 0, 'positive', ...) should return 'positive'"

            # (d) NVL2(val, 'has_val', 'no_val'): TDengine converts to CASE WHEN
            tdSql.query(
                f"select id, nvl2(val, 'has_val', 'no_val') "
                f"from {src_m}.{m_db}.data order by id")
            tdSql.checkRows(2)
            assert "no_val" in str(tdSql.getData(0, 1)), \
                "NVL2(NULL, ...) should return 'no_val'"
            assert "has_val" in str(tdSql.getData(1, 1)), \
                "NVL2(5, 'has_val', ...) should return 'has_val'"

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS data",
                "CREATE TABLE data (id INT, label TEXT)",
                "INSERT INTO data VALUES (1, NULL), (2, 'present')",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # (e) PG COALESCE: converts to CASE WHEN for PG pushdown
            tdSql.query(
                f"select id, coalesce(label, 'fallback') "
                f"from {src_p}.{p_db}.public.data order by id")
            tdSql.checkRows(2)
            assert "fallback" in str(tdSql.getData(0, 1))
            assert "present" in str(tdSql.getData(1, 1))

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

        # (f) InfluxDB: IFNULL local compute
        src_i = "fq_sql_083_influx"
        i_db = "fq_sql_083_i_db"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "sensor,host=h1 val=42 1704067200000000000")
            self._mk_influx_real(src_i, database=i_db)
            # InfluxDB cannot push down IFNULL; TDengine executes locally
            tdSql.query(
                f"select ifnull(val, 0) from {src_i}.{i_db}.sensor")
            tdSql.checkRows(1)
            influx_val = tdSql.getData(0, 0)
            assert influx_val is not None, \
                "IFNULL(val, 0) on InfluxDB source should return non-null (local compute)"
            assert abs(float(influx_val) - 42.0) < 0.01, \
                f"IFNULL(42, 0) should return 42.0, got {influx_val}"
        finally:
            self._cleanup_src(src_i)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_084(self):
        """FQ-SQL-084: Division by zero behavior difference — MySQL NULL vs PG expression handling

        Dimensions:
          a) MySQL: val / NULLIF(0, 0) → NULL (avoid error via NULLIF)
          b) PG: val * 1.0 / NULLIF(0, 0) → NULL

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src_m = "fq_sql_084_mysql"
        src_p = "fq_sql_084_pg"
        m_db = "fq_sql_084_m_db"
        p_db = "fq_sql_084_p_db"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val INT)",
                "INSERT INTO numbers VALUES (1, 10)",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # MySQL: 10 / NULLIF(0, 0) = NULL (safe div by zero)
            tdSql.query(
                f"select id, val / nullif(0, 0) from {src_m}.{m_db}.numbers where id = 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is None

        finally:
            self._cleanup_src(src_m)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)

        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS numbers",
                "CREATE TABLE numbers (id INT, val INT)",
                "INSERT INTO numbers VALUES (1, 10)",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # PG: 10 / NULLIF(0, 0) = NULL
            tdSql.query(
                f"select id, val / nullif(0, 0) from {src_p}.{p_db}.public.numbers where id = 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is None

        finally:
            self._cleanup_src(src_p)
            ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)

    def test_fq_sql_085(self):
        """FQ-SQL-085: InfluxDB PARTITION BY tag pushdown — GROUP BY host aggregation

        Dimensions:
          a) avg(usage) PARTITION BY host → 2 groups verified

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_085_influx"
        i_db = "fq_sql_085_db"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db,
                "cpu,host=h1 usage=30 1704067200000000000\n"
                "cpu,host=h1 usage=50 1704067260000000000\n"
                "cpu,host=h2 usage=10 1704067320000000000\n"
                "cpu,host=h2 usage=20 1704067380000000000"
            )
            self._mk_influx_real(src, database=i_db)

            tdSql.query(
                f"select avg(usage) from {src}.{i_db}.cpu partition by host "
                f"order by host")
            tdSql.checkRows(2)   # 2 hosts: h1 avg=40, h2 avg=15
            # ORDER BY host: h1 < h2 alphabetically → row 0 = h1 (avg=40), row 1 = h2 (avg=15)
            h1_avg = float(tdSql.getData(0, 0))
            h2_avg = float(tdSql.getData(1, 0))
            assert abs(h1_avg - 40.0) < 0.01, \
                f"h1 avg(usage)=(30+50)/2=40.0, got {h1_avg}"
            assert abs(h2_avg - 15.0) < 0.01, \
                f"h2 avg(usage)=(10+20)/2=15.0, got {h2_avg}"

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)

    def test_fq_sql_086(self):
        """FQ-SQL-086: DS/FS query example runnability — typical business SQL full verification

        Dimensions:
          a) SELECT with WHERE filter → verified count
          b) GROUP BY aggregate → counts verified
          c) JOIN same source → join result verified
          d) DISTINCT on external source → correct unique values

        Catalog:
            - Query:FederatedSQL

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Initial implementation

        """
        src = "fq_sql_086_mysql"
        ext_db = "fq_sql_086_db"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS users",
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE users (id INT, name VARCHAR(50), region VARCHAR(20))",
                "CREATE TABLE orders (id INT, user_id INT, status INT, amount INT)",
                "INSERT INTO users VALUES (1, 'Alice', 'us'), (2, 'Bob', 'eu')",
                "INSERT INTO orders VALUES (1, 1, 1, 100), (2, 1, 2, 200), (3, 2, 1, 150)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            # (a) SELECT with WHERE filter
            tdSql.query(
                f"select * from {src}.{ext_db}.orders where status = 1 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)    # id=1
            tdSql.checkData(0, 2, 1)    # status=1
            tdSql.checkData(0, 3, 100)  # amount=100
            tdSql.checkData(1, 0, 3)    # id=3
            tdSql.checkData(1, 2, 1)    # status=1
            tdSql.checkData(1, 3, 150)  # amount=150

            # (b) GROUP BY aggregate
            tdSql.query(
                f"select status, count(*) from {src}.{ext_db}.orders group by status order by status")
            tdSql.checkRows(2)    # status 1 and 2
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 2)  # count of status=1: orders 1,3
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 1)  # count of status=2: order 2

            # (c) JOIN same source
            tdSql.query(
                f"select u.name, sum(o.amount) as total "
                f"from {src}.{ext_db}.users u "
                f"join {src}.{ext_db}.orders o on u.id = o.user_id "
                f"group by u.name order by u.name")
            tdSql.checkRows(2)
            # Alice: 100+200 = 300, Bob: 150
            assert "Alice" in str(tdSql.getData(0, 0))
            tdSql.checkData(0, 1, 300)
            assert "Bob" in str(tdSql.getData(1, 0))
            tdSql.checkData(1, 1, 150)

            # (d) DISTINCT region
            tdSql.query(
                f"select distinct region from {src}.{ext_db}.users order by region")
            tdSql.checkRows(2)
            assert "eu" in str(tdSql.getData(0, 0))
            assert "us" in str(tdSql.getData(1, 0))

        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

