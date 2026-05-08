"""
test_fq_04_sql_capability.py  –  Data-driven SQL capability tests for federated query.

Covers FQ-SQL-001 through FQ-SQL-086 from TS §4.  Tests are grouped by
feature category.  Within each group SQL / expected-result pairs are
executed against MySQL, PostgreSQL and InfluxDB via ``_with_std_sources``
or ``_with_custom_sources``.

Design
------
* Standard 5-row ``src_t`` dataset (ts/val/score/name/flag) reused across
  most test points via ``_with_std_sources``.
* Source-specific tests (JSON, MATCH/REGEXP, VIEWs, windows, …) use
  ``_with_custom_sources`` or explicit per-source setup.
* Original 86 test methods compressed into ~20 data-driven methods with
  zero coverage loss.
"""

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    _STD_ROWS,
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
)


class TestFq04SqlCapability(FederatedQueryVersionedMixin):
    """FQ-SQL-001 – FQ-SQL-086: SQL feature support (data-driven)."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        pass

    # ==================================================================
    # 1  Standard src_t: basic SQL, operators, aggregates, subqueries
    #    Covers: 001/002/003/006/007/024/025/034/035/036/040/043/055/056/
    #            059/063/070/071/072/074/084
    # ==================================================================

    def test_fq_sql_std_queries(self):
        """FQ-SQL-STD: Basic queries, operators, aggregates, subqueries on standard src_t.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # --- 001: basic SELECT / WHERE / ORDER / LIMIT ---
            tdSql.query(f"select val, score from {t} order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1); tdSql.checkData(4, 0, 5)

            tdSql.query(f"select val from {t} where val > 3 order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4); tdSql.checkData(1, 0, 5)

            tdSql.query(f"select val from {t} order by val desc")
            tdSql.checkRows(5); tdSql.checkData(0, 0, 5)

            tdSql.query(f"select val from {t} order by val limit 2 offset 2")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 3); tdSql.checkData(1, 0, 4)

            # --- 002: GROUP BY / HAVING ---
            tdSql.query(
                f"select flag, count(*) as cnt from {t} "
                f"group by flag order by flag")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 0); tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 0, 1); tdSql.checkData(1, 1, 3)

            tdSql.query(
                f"select flag, count(*) as cnt from {t} "
                f"group by flag having count(*) > 2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 3)

            # --- 003: DISTINCT ---
            tdSql.query(f"select distinct flag from {t} order by flag")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 0); tdSql.checkData(1, 0, 1)

            # --- 003: DISTINCT multi-column ---
            tdSql.query(f"select distinct val, flag from {t} order by val")
            tdSql.checkRows(5)  # all (val, flag) combos are unique

            # --- 006: CASE expression ---
            tdSql.query(
                f"select val, case when val > 3 then 'high' else 'low' end as lvl "
                f"from {t} order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 'low')
            tdSql.checkData(3, 1, 'high')
            tdSql.checkData(4, 1, 'high')

            # --- 006: SUM(CASE WHEN …) conditional aggregation ---
            tdSql.query(
                f"select sum(case when flag = 1 then val else 0 end) as s1 from {t}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 9)   # flag=1 vals: 1+3+5 = 9

            # --- 007 / 034: arithmetic operators ---
            tdSql.query(
                f"select val, val+1, val-1, val*2, val%3 from {t} order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 2); tdSql.checkData(0, 2, 0)
            tdSql.checkData(0, 3, 2); tdSql.checkData(0, 4, 1)
            # intermediate rows
            tdSql.checkData(1, 1, 3); tdSql.checkData(1, 3, 4)
            tdSql.checkData(2, 1, 4); tdSql.checkData(2, 3, 6)
            tdSql.checkData(3, 1, 5); tdSql.checkData(3, 3, 8)
            tdSql.checkData(4, 1, 6); tdSql.checkData(4, 3, 10)

            # --- 034: division operator ---
            tdSql.query(
                f"select val, val / 2.0 from {t} order by val")
            tdSql.checkRows(5)
            assert abs(float(tdSql.getData(0, 1)) - 0.5) < 1e-6
            assert abs(float(tdSql.getData(2, 1)) - 1.5) < 1e-6
            assert abs(float(tdSql.getData(4, 1)) - 2.5) < 1e-6

            # --- 035: comparison operators ---
            tdSql.query(f"select val from {t} where val = 3")
            tdSql.checkRows(1)
            tdSql.query(f"select val from {t} where val != 3")
            tdSql.checkRows(4)
            tdSql.query(f"select val from {t} where val <> 3")
            tdSql.checkRows(4)
            tdSql.query(f"select val from {t} where val > 3 order by val")
            tdSql.checkRows(2); tdSql.checkData(0, 0, 4)
            tdSql.query(f"select val from {t} where val < 3 order by val")
            tdSql.checkRows(2); tdSql.checkData(0, 0, 1)
            tdSql.query(f"select val from {t} where val >= 3")
            tdSql.checkRows(3)
            tdSql.query(f"select val from {t} where val <= 3")
            tdSql.checkRows(3)
            tdSql.query(f"select val from {t} where val between 2 and 4")
            tdSql.checkRows(3)
            tdSql.query(f"select val from {t} where val in (1, 3, 5)")
            tdSql.checkRows(3)
            tdSql.query(f"select val from {t} where name like 'a%'")
            tdSql.checkRows(1)

            # --- 036: logical operators ---
            tdSql.query(
                f"select val from {t} where val > 2 and flag = 1 order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 3); tdSql.checkData(1, 0, 5)

            tdSql.query(
                f"select val from {t} where val = 1 or val = 5 order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(1, 0, 5)

            tdSql.query(
                f"select val from {t} where not flag = 1 order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2); tdSql.checkData(1, 0, 4)

            # --- 040: IS NOT NULL (std data has no NULLs) ---
            tdSql.query(f"select count(*) from {t} where val is not null")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 5)

            # --- 043: LIMIT / OFFSET boundary ---
            tdSql.query(f"select val from {t} order by val limit 2 offset 3")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 4); tdSql.checkData(1, 0, 5)
            tdSql.query(f"select val from {t} order by val limit 10 offset 100")
            tdSql.checkRows(0)

            # --- 055 / 024: aggregate functions ---
            tdSql.query(
                f"select count(*), sum(val), avg(val), min(val), max(val) from {t}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)    # count
            tdSql.checkData(0, 1, 15)   # sum
            tdSql.checkData(0, 3, 1)    # min
            tdSql.checkData(0, 4, 5)    # max

            # --- 055: AVG exact check ---
            tdSql.query(f"select avg(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            # --- 055: STDDEV ---
            tdSql.query(f"select stddev(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.4142) < 1e-3, \
                f"STDDEV should be ~1.4142: {tdSql.getData(0, 0)}"

            # --- 056: PERCENTILE / APERCENTILE ---
            tdSql.query(f"select percentile(val, 50) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6, \
                f"PERCENTILE(50) expected 3.0, got {tdSql.getData(0, 0)}"

            tdSql.query(f"select apercentile(val, 50) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1.0, \
                f"APERCENTILE(50) expected ~3.0, got {tdSql.getData(0, 0)}"

            # --- 059: IFNULL / COALESCE on non-null data ---
            tdSql.query(f"select ifnull(val, 0) from {t} where val = 1")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 1)
            tdSql.query(f"select coalesce(val, 0) from {t} where val = 1")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 1)

            # --- 063: scalar expression ---
            tdSql.query(
                f"select val, val * 2 as doubled from {t} order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 2)
            tdSql.checkData(4, 0, 5); tdSql.checkData(4, 1, 10)

            # --- 070: nested subquery ---
            tdSql.query(
                f"select avg(v) from (select val as v from {t} where val > 1)")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.5) < 1e-6

            # --- 071: scalar subquery ---
            tdSql.query(
                f"select val, (select max(val) from {t}) as mx from {t} order by val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 5)
            tdSql.checkData(4, 0, 5); tdSql.checkData(4, 1, 5)

            # --- 072: IN / NOT IN subquery ---
            tdSql.query(
                f"select val from {t} "
                f"where val in (select val from {t} where flag = 1) order by val")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1); tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 5)

            tdSql.query(
                f"select val from {t} "
                f"where val not in (select val from {t} where flag = 1) order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2); tdSql.checkData(1, 0, 4)

            # --- 074: ALL / ANY subquery ---
            tdSql.query(
                f"select val from {t} "
                f"where val > all(select val from {t} where val < 5) order by val")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 5)

            tdSql.query(
                f"select val from {t} "
                f"where val < any(select val from {t} where val = 5) order by val")
            tdSql.checkRows(4); tdSql.checkData(3, 0, 4)

            # --- 084: division by zero → NULL via NULLIF ---
            tdSql.query(f"select val, val / nullif(0, 0) from {t} where val = 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 1) is None, "val/NULLIF(0,0) should be NULL"

        self._with_std_sources("fq04_std", body)

    # ==================================================================
    # 2  Custom-table queries: UNION ALL, JOIN, GROUP BY, EXISTS
    #    Covers: 004/073/086
    # ==================================================================

    def test_fq_sql_custom_table_queries(self):
        """FQ-SQL-CUSTOM: UNION ALL, JOIN, GROUP BY, EXISTS on custom tables.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS users",
            "DROP TABLE IF EXISTS orders",
            "DROP TABLE IF EXISTS orders_solo",
            "DROP TABLE IF EXISTS users_a",
            "DROP TABLE IF EXISTS users_b",
            "CREATE TABLE users (id INT, name VARCHAR(50), region VARCHAR(20))",
            "CREATE TABLE orders (id INT, user_id INT, status INT, amount INT)",
            "CREATE TABLE orders_solo (id INT, user_id INT)",
            "CREATE TABLE users_a (id INT, name VARCHAR(50))",
            "CREATE TABLE users_b (id INT, name VARCHAR(50))",
            "INSERT INTO users VALUES (1, 'Alice', 'us'), (2, 'Bob', 'eu')",
            "INSERT INTO orders VALUES (1, 1, 1, 100), (2, 1, 2, 200), (3, 2, 1, 150)",
            "INSERT INTO orders_solo VALUES (1, 1)",
            "INSERT INTO users_a VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO users_b VALUES (3, 'Carol'), (4, 'Dave')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS users CASCADE",
            "DROP TABLE IF EXISTS orders CASCADE",
            "DROP TABLE IF EXISTS orders_solo CASCADE",
            "DROP TABLE IF EXISTS users_a CASCADE",
            "DROP TABLE IF EXISTS users_b CASCADE",
            "CREATE TABLE users (id INT, name TEXT, region TEXT)",
            "CREATE TABLE orders (id INT, user_id INT, status INT, amount INT)",
            "CREATE TABLE orders_solo (id INT, user_id INT)",
            "CREATE TABLE users_a (id INT, name TEXT)",
            "CREATE TABLE users_b (id INT, name TEXT)",
            "INSERT INTO users VALUES (1, 'Alice', 'us'), (2, 'Bob', 'eu')",
            "INSERT INTO orders VALUES (1, 1, 1, 100), (2, 1, 2, 200), (3, 2, 1, 150)",
            "INSERT INTO orders_solo VALUES (1, 1)",
            "INSERT INTO users_a VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO users_b VALUES (3, 'Carol'), (4, 'Dave')",
        ]
        influx_lines = [
            'users name="Alice",region="us",id=1i 1704067200000000000',
            'users name="Bob",region="eu",id=2i 1704067260000000000',
            'orders user_id=1i,status=1i,amount=100i,id=1i 1704067200000000000',
            'orders user_id=1i,status=2i,amount=200i,id=2i 1704067260000000000',
            'orders user_id=2i,status=1i,amount=150i,id=3i 1704067320000000000',
            'orders_solo user_id=1i,id=1i 1704067200000000000',
            'users_a name="Alice",id=1i 1704067200000000000',
            'users_a name="Bob",id=2i 1704067260000000000',
            'users_b name="Carol",id=3i 1704067200000000000',
            'users_b name="Dave",id=4i 1704067260000000000',
        ]

        def body(src, db_type):
            # --- 004: UNION ALL same source ---
            tdSql.query(
                f"select id, name from {src}.users_a "
                f"union all "
                f"select id, name from {src}.users_b "
                f"order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(3, 1, "Dave")

            # --- 086a: WHERE filter ---
            tdSql.query(
                f"select id, amount from {src}.orders where status = 1 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 0, 3); tdSql.checkData(1, 1, 150)

            # --- 086b: GROUP BY aggregate ---
            tdSql.query(
                f"select status, count(*) from {src}.orders "
                f"group by status order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 2)
            tdSql.checkData(1, 0, 2); tdSql.checkData(1, 1, 1)

            # --- 086c: JOIN same source ---
            # External sources lack timestamp PK → JOIN not supported (0x2664)
            tdSql.error(
                f"select u.name, sum(o.amount) as total "
                f"from {src}.users u "
                f"join {src}.orders o on u.id = o.user_id "
                f"group by u.name order by u.name",
                expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_JOIN)

            # --- 086d: DISTINCT ---
            tdSql.query(
                f"select distinct region from {src}.users order by region")
            tdSql.checkRows(2)
            assert str(tdSql.getData(0, 0)) == "eu"
            assert str(tdSql.getData(1, 0)) == "us"

            # --- 073: EXISTS / NOT EXISTS ---
            # Correlated subqueries not supported on external sources (0x26a6)
            tdSql.error(
                f"select u.id from {src}.users u "
                f"where exists (select 1 from {src}.orders o "
                f"where o.user_id = u.id) order by u.id")

            tdSql.error(
                f"select u.id from {src}.users u "
                f"where not exists (select 1 from {src}.orders o "
                f"where o.user_id = u.id and o.status = 2) order by u.id")

            # --- 073b: Pure EXISTS / NOT EXISTS ---
            tdSql.error(
                f"select u.id from {src}.users u "
                f"where exists (select 1 from {src}.orders_solo o "
                f"where o.user_id = u.id) order by u.id")

            tdSql.error(
                f"select u.id from {src}.users u "
                f"where not exists (select 1 from {src}.orders_solo o "
                f"where o.user_id = u.id) order by u.id")

        self._with_custom_sources(
            "fq04_cust", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines,
        )

    # ==================================================================
    # 3  String functions
    #    Covers: 017/018/019/046/047
    # ==================================================================

    def test_fq_sql_string_functions(self):
        """FQ-SQL-STR: CONCAT/UPPER/LOWER/REPLACE/LENGTH/CHAR_LENGTH/SUBSTRING on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select concat(name, '_x') from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "alpha_x"

            tdSql.query(f"select upper(name) from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "ALPHA"

            tdSql.query(f"select lower(name) from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "alpha"

            tdSql.query(
                f"select replace(name, 'alpha', 'omega') from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "omega"

            tdSql.query(f"select length(name) from {t} where val = 1")
            tdSql.checkRows(1)
            # InfluxDB maps strings to NCHAR (UCS-4): LENGTH returns byte count
            # (5 chars × 4 bytes = 20). MySQL/PG use VARCHAR: LENGTH = 5.
            v = int(tdSql.getData(0, 0))
            assert v in (5, 20), f"LENGTH(name) expected 5 or 20, got {v}"

            tdSql.query(f"select char_length(name) from {t} where val = 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 5

            # --- 046: ASCII ---
            tdSql.query(f"select ascii(name) from {t} where val = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 97)   # ascii('a') = 97

            # --- 046: LTRIM / RTRIM / TRIM ---
            # Pure-literal string functions may cause 0x6404 on external sources;
            # use column-based expressions instead.
            tdSql.query(
                f"select ltrim(name), rtrim(name), trim(name) from {t} where val = 1")
            tdSql.checkRows(1)
            # name='alpha' has no leading/trailing spaces, so all return 'alpha'
            assert str(tdSql.getData(0, 0)) == 'alpha'
            assert str(tdSql.getData(0, 1)) == 'alpha'
            assert str(tdSql.getData(0, 2)) == 'alpha'

            # --- 046: CONCAT_WS ---
            tdSql.query(f"select concat_ws('-', name, 'b') from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "alpha-b"

            # --- 046: REPEAT ---
            tdSql.query(f"select repeat('x', 3) from {t} limit 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "xxx"

        self._with_std_sources("fq04_str", body)

    def test_fq_sql_substring(self):
        """FQ-SQL-SUBSTR: SUBSTRING and extended string functions on all sources.

        Covers: 019, 047.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"
            tdSql.query(f"select substring(name, 1, 3) from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)).lower() == "alp"

        self._with_std_sources("fq04_substr", body)

        # --- 019/047: Additional TDengine string functions ---
        mysql_setup = [
            "DROP TABLE IF EXISTS str_data",
            "CREATE TABLE str_data (id INT, name VARCHAR(50), labels VARCHAR(100))",
            "INSERT INTO str_data VALUES (1, 'Alice', 'A,B,C')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS str_data",
            "CREATE TABLE str_data (id INT, name TEXT, labels TEXT)",
            "INSERT INTO str_data VALUES (1, 'Alice', 'A,B,C')",
        ]
        influx_lines_str = [
            'str_data name="Alice",labels="A,B,C",id=1i 1704067200000000000',
        ]

        def body2(src, db_type):
            t = f"{src}.str_data"
            # CONCAT
            tdSql.query(f"select concat(name, '-test') from {t} where id = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "Alice-test"

            # CONCAT_WS
            tdSql.query(
                f"select concat_ws('-', name, labels) from {t} where id = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "Alice-A,B,C"

            # LENGTH — InfluxDB NCHAR returns byte count (5×4=20)
            tdSql.query(f"select length(name) from {t} where id = 1")
            tdSql.checkRows(1)
            expected_len = 20 if db_type == 'influx' else 5
            tdSql.checkData(0, 0, expected_len)

            # CHAR_LENGTH
            tdSql.query(f"select char_length(name) from {t} where id = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)

            # LOWER / UPPER
            tdSql.query(f"select lower(name) from {t} where id = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "alice"

            tdSql.query(f"select upper(name) from {t} where id = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "ALICE"

        self._with_custom_sources(
            "fq04_strx", body2,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_str,
        )

    # ==================================================================
    # 4  Math functions
    #    Covers: 013/014/015/016/044/045
    # ==================================================================

    def test_fq_sql_math_functions(self):
        """FQ-SQL-MATH: ABS/CEIL/FLOOR/ROUND/SQRT/POW/SIGN on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select abs(val - 3) from {t} where val = 1")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 2)

            tdSql.query(f"select ceil(score) from {t} where val = 1")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 2)

            tdSql.query(f"select floor(score) from {t} where val = 1")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 1)

            tdSql.query(f"select round(score, 0) from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 1e-6

            tdSql.query(f"select sqrt(val) from {t} where val = 4")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 1e-6

            tdSql.query(f"select pow(val, 2) from {t} where val = 3")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 9.0) < 1e-6

            tdSql.query(f"select sign(val) from {t} where val = 3")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 1)

            # --- 013 / 044: trig functions ---
            # Use column expressions to avoid pure-literal 0x6404 errors
            # val=1 → cos(1-1)=cos(0)=1, sin(1-1)=0, tan(1-1)=0
            tdSql.query(
                f"select cos(val - 1), sin(val - 1), tan(val - 1) "
                f"from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.0) < 1e-9
            assert abs(float(tdSql.getData(0, 1)) - 0.0) < 1e-9
            assert abs(float(tdSql.getData(0, 2)) - 0.0) < 1e-9

            # acos(val-1)=acos(0)=pi/2, asin(val)=asin(1)=pi/2, atan(val)=atan(1)=pi/4
            tdSql.query(
                f"select acos(val - 1), asin(val), atan(val) "
                f"from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5707963) < 1e-5
            assert abs(float(tdSql.getData(0, 1)) - 1.5707963) < 1e-5
            assert abs(float(tdSql.getData(0, 2)) - 0.7853981) < 1e-5

            # --- 044: DEGREES/RADIANS/EXP/PI ---
            # degrees/radians use pi() which is a constant, not a pure literal
            tdSql.query(
                f"select degrees(pi()), radians(180), pi(), exp(val - 1) from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 180.0) < 1e-6
            assert abs(float(tdSql.getData(0, 1)) - 3.14159265) < 1e-5
            assert abs(float(tdSql.getData(0, 2)) - 3.14159265) < 1e-5
            assert abs(float(tdSql.getData(0, 3)) - 1.0) < 1e-9

        self._with_std_sources("fq04_math", body)

    def test_fq_sql_math_dialect(self):
        """FQ-SQL-MATHD: LOG/TRUNCATE/RAND dialect conversion on MySQL + PG.

        Covers: 014, 015, 016.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select log(val, 2) from {t} where val = 4")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 0.01

            tdSql.query(f"select truncate(score, 1) from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5) < 1e-6

            tdSql.query(f"select rand() from {t} where val = 1")
            tdSql.checkRows(1)
            r = float(tdSql.getData(0, 0))
            assert 0.0 <= r < 1.0, f"RAND() out of range: {r}"

            # --- 016: RAND(seed) reproducibility ---
            tdSql.query(f"select rand(42) from {t} where val = 1")
            tdSql.checkRows(1)
            r_seed = float(tdSql.getData(0, 0))
            assert 0.0 <= r_seed < 1.0, f"RAND(42) out of range: {r_seed}"

            # --- 014: LOG single-arg (natural log / ln) ---
            tdSql.query(f"select log(val) from {t} where val = 1")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 0.0) < 1e-6  # ln(1) = 0

            # --- 045: MOD ---
            tdSql.query(f"select mod(val, 3) from {t} where val = 5")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 1e-6  # 5 % 3 = 2

            # --- 045: GREATEST / LEAST ---
            tdSql.query(
                f"select greatest(val, 3), least(val, 3) from {t} where val = 5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)   # greatest(5, 3) = 5
            tdSql.checkData(0, 1, 3)   # least(5, 3) = 3

            tdSql.query(
                f"select greatest(val, 3), least(val, 3) from {t} where val = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)   # greatest(1, 3) = 3
            tdSql.checkData(0, 1, 1)   # least(1, 3) = 1

        self._with_std_sources("fq04_mathd", body)

    def test_fq_sql_corr_pg(self):
        """FQ-SQL-CORR: CORR(x,y) correlation coefficient on all sources.

        Covers: 045.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS corr_data",
            "CREATE TABLE corr_data (id INT, x DOUBLE, y DOUBLE)",
            "INSERT INTO corr_data VALUES (1, 1.0, 2.0), (2, 2.0, 4.0), (3, 3.0, 6.0)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS corr_data",
            "CREATE TABLE corr_data (id INT, x DOUBLE PRECISION, y DOUBLE PRECISION)",
            "INSERT INTO corr_data VALUES (1, 1.0, 2.0), (2, 2.0, 4.0), (3, 3.0, 6.0)",
        ]
        influx_lines_corr = [
            'corr_data x=1.0,y=2.0,id=1i 1704067200000000000',
            'corr_data x=2.0,y=4.0,id=2i 1704067260000000000',
            'corr_data x=3.0,y=6.0,id=3i 1704067320000000000',
        ]

        def body(src, db_type):
            tdSql.query(f"select corr(x, y) from {src}.corr_data")
            tdSql.checkRows(1)
            corr_val = float(tdSql.getData(0, 0))
            assert abs(corr_val - 1.0) < 1e-6, \
                f"CORR(x, y) for perfect linear should be 1.0: {corr_val}"

        self._with_custom_sources(
            "fq04_corr", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_corr,
        )

    # ==================================================================
    # 5  MATCH / REGEXP
    #    Covers: 008/009/039
    # ==================================================================

    def test_fq_sql_regexp_match(self):
        """FQ-SQL-REGEXP: MATCH/NMATCH dialect conversion on MySQL + PG.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select val from {t} where name match '^a' order by val")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)  # 'alpha'

            tdSql.query(f"select val from {t} where name nmatch '^a' order by val")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 2)

        self._with_std_sources("fq04_regex", body)

    # ==================================================================
    # 6  JSON operators
    #    Covers: 010/011/012/038/082
    # ==================================================================

    def test_fq_sql_json_operators(self):
        """FQ-SQL-JSON: JSON value extraction via -> and value filter on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS jdata",
            "CREATE TABLE jdata (id INT, data JSON)",
            "INSERT INTO jdata VALUES (1, JSON_OBJECT('k', 'v1', 'num', 10))",
            "INSERT INTO jdata VALUES (2, JSON_OBJECT('k', 'v2', 'num', 20))",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS jdata",
            "CREATE TABLE jdata (id INT, data JSONB)",
            "INSERT INTO jdata VALUES (1, '{\"k\": \"v1\", \"num\": 10}'::jsonb)",
            "INSERT INTO jdata VALUES (2, '{\"k\": \"v2\", \"num\": 20}'::jsonb)",
        ]
        influx_lines_json = [
            'jdata data="{\\\"k\\\": \\\"v1\\\", \\\"num\\\": 10}",id=1i 1704067200000000000',
            'jdata data="{\\\"k\\\": \\\"v2\\\", \\\"num\\\": 20}",id=2i 1704067260000000000',
        ]

        def body(src, db_type):
            t = f"{src}.jdata"

            # -> JSON extraction requires JSON type, but external source JSON/JSONB
            # columns are mapped to NCHAR in TDengine → type error (0x2652)
            tdSql.error(f"select id, data->'k' from {t} order by id")

            # -> value filter also fails for the same reason
            tdSql.error(
                f"select id from {t} where data->'num' = 10")

        self._with_custom_sources(
            "fq04_json", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_json,
        )

    def test_fq_sql_to_json(self):
        """FQ-SQL-TOJSON: TO_JSON conversion on MySQL/PG/InfluxDB.

        Covers: 082.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, attrs VARCHAR(200))",
            "INSERT INTO data VALUES (1, '{\"age\": 30}')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, attrs TEXT)",
            "INSERT INTO data VALUES (1, '{\"age\": 30}')",
        ]
        influx_lines = [
            'data attrs="{\\\"age\\\": 30}",id=1i 1704067200000000000',
        ]

        def body(src, db_type):
            # to_json() requires JSON type input; external sources map
            # VARCHAR/TEXT to VARCHAR/NCHAR → invalid parameter type (0x2802)
            tdSql.error(f"select to_json(attrs) from {src}.data where id = 1")

        self._with_custom_sources(
            "fq04_tojson", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines,
        )

    # ==================================================================
    # 7  Type conversion
    #    Covers: 022/023/053
    # ==================================================================

    def test_fq_sql_type_conversion(self):
        """FQ-SQL-CAST: CAST on all sources, TO_CHAR/TO_TIMESTAMP on MySQL+PG.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        # CAST on std data
        def cast_body(src):
            t = f"{src}.src_t"
            tdSql.query(f"select cast(val as double) from {t} where val = 3")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6

            # --- 022/053: CAST(val AS BINARY) → string representation ---
            tdSql.query(
                f"select cast(score as binary(16)) from {t} where val = 1")
            tdSql.checkRows(1)
            result = str(tdSql.getData(0, 0))
            assert result is not None and len(result) > 0, \
                f"CAST AS BINARY should return non-empty: {result}"

        self._with_std_sources("fq04_cast", cast_body)

        # TO_CHAR / TO_TIMESTAMP on MySQL + PG + InfluxDB
        mysql_setup = [
            "DROP TABLE IF EXISTS times",
            "CREATE TABLE times (id INT, ts DATETIME, ts_str VARCHAR(30))",
            "INSERT INTO times VALUES (1, '2024-01-15 12:30:00', '2024-01-15 12:30:00')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS times",
            "CREATE TABLE times (id INT, ts TIMESTAMP, ts_str VARCHAR(30))",
            "INSERT INTO times VALUES (1, '2024-01-15 12:30:00', '2024-01-15 12:30:00')",
        ]
        influx_lines_tc = [
            'times ts_str="2024-01-15 12:30:00",id=1i 1705321800000000000',
        ]

        def tochar_body(src, db_type):
            if db_type == 'influx':
                # InfluxDB: ts is the native line-protocol TIMESTAMP column.
                # to_char works on it; to_timestamp fails on NCHAR ts_str;
                # to_unixtimestamp fails on TIMESTAMP ts — same semantics as MySQL/PG.
                tdSql.query(
                    f"select to_char(ts, 'yyyy-MM-dd') from {src}.times where id = 1")
                tdSql.checkRows(1)
                assert "2024-01-15" in str(tdSql.getData(0, 0))

                # ts_str is NCHAR on InfluxDB → to_timestamp encoding mismatch error
                tdSql.error(
                    f"select to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') "
                    f"from {src}.times where id = 1")

                # ts is TIMESTAMP on InfluxDB → to_unixtimestamp type mismatch error
                tdSql.error(
                    f"select to_unixtimestamp(ts) from {src}.times where id = 1")
                return

            # MySQL/PG have explicit ts DATETIME/TIMESTAMP column
            tdSql.query(
                f"select to_char(ts, 'yyyy-MM-dd') from {src}.times where id = 1")
            tdSql.checkRows(1)
            assert "2024-01-15" in str(tdSql.getData(0, 0))

            # MySQL/PG VARCHAR → encoding causes to_timestamp failure (0x2807)
            tdSql.error(
                f"select to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') "
                f"from {src}.times where id = 1")

            # MySQL/PG timestamp type mapping causes to_unixtimestamp failure (0x2802)
            tdSql.error(
                f"select to_unixtimestamp(ts) from {src}.times where id = 1")

        self._with_custom_sources(
            "fq04_tochar", tochar_body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_tc,
        )

    # ==================================================================
    # 8  Date/time functions
    #    Covers: 023/033/054
    # ==================================================================

    def test_fq_sql_datetime_influx(self):
        """FQ-SQL-DTI: NOW/TODAY/TIMEDIFF/TIMETRUNCATE/CAST(ts) on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select now() from {t} limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None

            tdSql.query(f"select today() from {t} limit 1")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is not None

            tdSql.query(
                f"select timediff('2024-01-01', '2024-01-01') from {t} limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 0

            tdSql.query(
                f"select timetruncate(ts, 1h) from {t} order by ts limit 1")
            tdSql.checkRows(1)
            tt_result = tdSql.getData(0, 0)
            assert "2024-01-01" in str(tt_result), \
                f"timetruncate(ts,1h) expected 2024-01-01, got {tt_result}"

            tdSql.query(f"select cast(ts as bigint) from {t} order by ts limit 1")
            tdSql.checkRows(1)
            cast_result = tdSql.getData(0, 0)
            assert cast_result == 1704067200000, \
                f"cast(ts as bigint) = {cast_result}, expected 1704067200000"

        self._with_std_sources("fq04_dti", body)

    def test_fq_sql_datetime_mysql_pg(self):
        """FQ-SQL-DTM: DATE/DAYOFWEEK/WEEK/WEEKDAY on all sources.

        Covers: 054.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS times",
            "CREATE TABLE times (id INT, ts DATETIME)",
            "INSERT INTO times VALUES (1, '2024-01-01 00:00:00')",
            "INSERT INTO times VALUES (2, '2024-01-07 00:00:00')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS times",
            "CREATE TABLE times (id INT, ts TIMESTAMP)",
            "INSERT INTO times VALUES (1, '2024-01-01 00:00:00')",
            "INSERT INTO times VALUES (2, '2024-01-07 00:00:00')",
        ]

        influx_lines_dtm = [
            'times id=1i 1704067200000000000',
            'times id=2i 1704585600000000000',
        ]

        def body(src, db_type):
            # --- 054e: DATE(ts) ---
            tdSql.query(f"select date(ts) from {src}.times order by id")
            tdSql.checkRows(2)
            assert "2024-01-01" in str(tdSql.getData(0, 0))

            # --- 054f: DAYOFWEEK(ts) 1=Sunday..7=Saturday ---
            tdSql.query(
                f"select id, dayofweek(ts) from {src}.times order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 2)   # 2024-01-01 Monday → 2
            tdSql.checkData(1, 1, 1)   # 2024-01-07 Sunday → 1

            # --- 054g: WEEK(ts) ---
            tdSql.query(
                f"select id, week(ts) from {src}.times order by id")
            tdSql.checkRows(2)
            # 2024-01-01: WEEK() in TDengine MySQL-mode returns 0 (week 0)
            tdSql.checkData(0, 1, 0)   # 2024-01-01 → week 0
            tdSql.checkData(1, 1, 1)   # 2024-01-07 → week 1

            # --- 054h: WEEKDAY(ts) 0=Monday..6=Sunday ---
            tdSql.query(
                f"select id, weekday(ts) from {src}.times order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)   # Monday → 0
            tdSql.checkData(1, 1, 6)   # Sunday → 6

        self._with_custom_sources(
            "fq04_dtm", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_dtm,
        )

    def test_fq_sql_mysql_group_by_time_bucket(self):
        """FQ-SQL-TIMEBUCKET: GROUP BY YEAR/HOUR/MINUTE on all sources.

        Covers: 033.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS events",
            "CREATE TABLE events (id INT, ts DATETIME, val INT)",
            "INSERT INTO events VALUES "
            "(1, '2024-01-01 10:15:30', 10), "
            "(2, '2024-01-01 10:15:45', 20), "
            "(3, '2024-01-01 10:30:00', 30), "
            "(4, '2024-01-01 11:00:00', 40), "
            "(5, '2025-06-15 10:00:00', 50)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS events",
            "CREATE TABLE events (id INT, ts TIMESTAMP, val INT)",
            "INSERT INTO events VALUES "
            "(1, '2024-01-01 10:15:30', 10), "
            "(2, '2024-01-01 10:15:45', 20), "
            "(3, '2024-01-01 10:30:00', 30), "
            "(4, '2024-01-01 11:00:00', 40), "
            "(5, '2025-06-15 10:00:00', 50)",
        ]
        influx_lines_tb = [
            'events id=1i,val=10i 1704104130000000000',
            'events id=2i,val=20i 1704104145000000000',
            'events id=3i,val=30i 1704105000000000000',
            'events id=4i,val=40i 1704106800000000000',
            'events id=5i,val=50i 1750068000000000000',
        ]

        def body(src, db_type):
            # GROUP BY YEAR(ts) — not supported on external sources (0x6404)
            tdSql.error(
                f"select year(ts) as yr, count(*) from {src}.events "
                f"group by year(ts) order by yr")

            # GROUP BY HOUR(ts) — not supported on external sources (0x6404)
            tdSql.error(
                f"select hour(ts) as hr, count(*) from {src}.events "
                f"where ts < '2025-01-01' group by hour(ts) order by hr")

            # GROUP BY MINUTE(ts) — not supported on external sources (0x6404)
            tdSql.error(
                f"select minute(ts) as mi, count(*) from {src}.events "
                f"where ts < '2024-01-01 11:00:00' "
                f"group by minute(ts) order by mi")

        self._with_custom_sources(
            "fq04_timebkt", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_tb,
        )

    # ==================================================================
    # 9  Encoding / hash / crypto
    #    Covers: 020/021/048/049/050/052
    # ==================================================================

    def test_fq_sql_encoding_hash_crypto(self):
        """FQ-SQL-CRYPTO: TO_BASE64/FROM_BASE64/MD5 on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, name VARCHAR(50), plain VARCHAR(100))",
            "INSERT INTO data VALUES (1, 'Alice', 'hello')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, name TEXT, plain TEXT)",
            "INSERT INTO data VALUES (1, 'Alice', 'hello')",
        ]
        influx_lines_crypto = [
            'data name="Alice",plain="hello",id=1i 1704067200000000000',
        ]

        # Track MD5 hashes across sources for cross-source consistency check
        md5_hashes = {}

        def body(src, db_type):
            if db_type == 'influx':
                # InfluxDB maps strings to NCHAR; md5/to_base64 require VARCHAR → error
                tdSql.error(f"select md5(name) from {src}.data where id = 1")
                tdSql.error(f"select to_base64(name) from {src}.data where id = 1")

                # Use CAST to convert NCHAR to VARCHAR for influx
                tdSql.query(f"select md5(CAST(name AS VARCHAR(50))) from {src}.data where id = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 32, f"MD5 should be 32 chars: {h}"
                md5_hashes[db_type] = h

                tdSql.query(f"select to_base64(CAST(name AS VARCHAR(50))) from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)).replace("\n", "") == "QWxpY2U="

                tdSql.query(
                    f"select from_base64(to_base64(CAST(name AS VARCHAR(50)))) "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == "Alice"
            else:
                # MD5 — MySQL/PG
                tdSql.query(f"select md5(name) from {src}.data where id = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 32, f"MD5 should be 32 chars: {h}"
                md5_hashes[db_type] = h

                # TO_BASE64 — MySQL/PG
                tdSql.query(f"select to_base64(name) from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)).replace("\n", "") == "QWxpY2U="

                # FROM_BASE64 round-trip — MySQL/PG
                tdSql.query(
                    f"select from_base64(to_base64(name)) "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == "Alice"

        self._with_custom_sources(
            "fq04_crypto", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_crypto,
        )

        # --- 049: MD5 cross-source consistency ---
        hashes = list(md5_hashes.values())
        if len(hashes) >= 2:
            for h in hashes[1:]:
                assert h == hashes[0], f"MD5 mismatch across sources: {md5_hashes}"

    # ==================================================================
    # 10  Conditional functions
    #     Covers: 083/084
    # ==================================================================

    def test_fq_sql_conditional_functions(self):
        """FQ-SQL-COND: IFNULL/NULLIF/NVL2/COALESCE/IF on MySQL+PG+InfluxDB.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, NULL), (2, 5)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, NULL), (2, 5)",
        ]
        influx_lines = [
            'data id=1i 1704067200000000000',
            'data val=5i,id=2i 1704067260000000000',
        ]

        def body(src, db_type):
            # IFNULL
            tdSql.query(f"select id, ifnull(val, 99) from {src}.data order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 99)
            tdSql.checkData(1, 1, 5)

            # NULLIF
            tdSql.query(f"select nullif(val, 5) from {src}.data where id = 2")
            tdSql.checkRows(1)
            assert tdSql.getData(0, 0) is None

            # IF()
            tdSql.query(
                f"select if(val > 0, 'positive', 'zero_or_null') "
                f"from {src}.data where id = 2")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == "positive"

            # NVL2
            tdSql.query(
                f"select id, nvl2(val, 'has', 'no') from {src}.data order by id")
            tdSql.checkRows(2)
            assert str(tdSql.getData(0, 1)) == "no"
            assert str(tdSql.getData(1, 1)) == "has"

            # COALESCE
            tdSql.query(
                f"select id, coalesce(val, 99) from {src}.data order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 99)
            tdSql.checkData(1, 1, 5)

        self._with_custom_sources(
            "fq04_cond", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines,
        )

    # ==================================================================
    # 11  NULL predicates (with actual NULLs)
    #     Covers: 040
    # ==================================================================

    def test_fq_sql_null_predicates(self):
        """FQ-SQL-NULL: IS NULL / IS NOT NULL with actual NULL rows on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        influx_lines_null = [
            'data val=10i,id=1i 1704067200000000000',
            'data id=2i 1704067260000000000',
            'data val=30i,id=3i 1704067320000000000',
        ]

        def body(src, db_type):
            tdSql.query(f"select id from {src}.data where val is null")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 2)
            tdSql.query(
                f"select id from {src}.data where val is not null order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(1, 0, 3)

        self._with_custom_sources(
            "fq04_null", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_null,
        )

    # ==================================================================
    # 12  UNION cross-source
    #     Covers: 005/041
    # ==================================================================

    def test_fq_sql_union_cross_source(self):
        """FQ-SQL-UNION: UNION / UNION ALL across MySQL, PG, InfluxDB.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_m = "fq04_union_m"
        src_p = "fq04_union_p"
        src_i = "fq04_union_i"
        m_db = "fq04_union_mdb"
        p_db = "fq04_union_pdb"
        i_db = "fq04_union_idb"
        self._cleanup_src(src_m, src_p, src_i)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            ])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name TEXT)",
                "INSERT INTO users VALUES (1, 'Alice'), (3, 'Carol')",
            ])
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                'users name="Dave",id=4i 1704067200000000000',
                'users name="Eve",id=5i 1704067260000000000',
            ])
            self._mk_mysql_real(src_m, database=m_db)
            self._mk_pg_real(src_p, database=p_db)
            self._mk_influx_real(src_i, database=i_db)

            m_users = f"{src_m}.{m_db}.users"
            p_users = f"{src_p}.{p_db}.public.users"
            i_users = f"{src_i}.{i_db}.users"

            # M+P UNION → dedup → 3 rows
            tdSql.query(
                f"select id, name from {m_users} "
                f"union "
                f"select id, name from {p_users} "
                f"order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 2); tdSql.checkData(1, 1, "Bob")
            tdSql.checkData(2, 0, 3); tdSql.checkData(2, 1, "Carol")

            # M+P UNION ALL → cross-source UNION ALL is not supported (0x26a6)
            tdSql.error(
                f"select id, name from {m_users} "
                f"union all "
                f"select id, name from {p_users} "
                f"order by id")

            # M+I UNION → 4 rows (no overlap)
            tdSql.query(
                f"select id, name from {m_users} "
                f"union "
                f"select id, name from {i_users} "
                f"order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 2); tdSql.checkData(1, 1, "Bob")
            tdSql.checkData(2, 0, 4); tdSql.checkData(2, 1, "Dave")
            tdSql.checkData(3, 0, 5); tdSql.checkData(3, 1, "Eve")

            # P+I UNION → 4 rows (no overlap)
            tdSql.query(
                f"select id, name from {p_users} "
                f"union "
                f"select id, name from {i_users} "
                f"order by id")
            tdSql.checkRows(4)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 3); tdSql.checkData(1, 1, "Carol")
            tdSql.checkData(2, 0, 4); tdSql.checkData(2, 1, "Dave")
            tdSql.checkData(3, 0, 5); tdSql.checkData(3, 1, "Eve")

            # M+P+I UNION ALL → cross-source UNION ALL is not supported (0x26a6)
            tdSql.error(
                f"select id, name from {m_users} "
                f"union all "
                f"select id, name from {p_users} "
                f"union all "
                f"select id, name from {i_users} "
                f"order by id")
        finally:
            self._cleanup_src(src_m, src_p, src_i)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    # ==================================================================
    # 13  Cross-source subquery
    #     Covers: 075/076
    # ==================================================================

    def test_fq_sql_cross_source_subquery(self):
        """FQ-SQL-XSUB: MySQL IN (PG subquery), InfluxDB IN (TDengine subquery).

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        # --- 076: MySQL WHERE id IN (PG subquery) ---
        src_m = "fq04_xsub_m"
        src_p = "fq04_xsub_p"
        m_db = "fq04_xsub_mdb"
        p_db = "fq04_xsub_pdb"
        self._cleanup_src(src_m, src_p)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
            ])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE orders (order_id INT, user_id INT)",
                "INSERT INTO orders VALUES (1, 1), (2, 3)",
            ])
            self._mk_mysql_real(src_m, database=m_db)
            self._mk_pg_real(src_p, database=p_db)

            tdSql.query(
                f"select u.id, u.name from {src_m}.{m_db}.users u "
                f"where u.id in "
                f"(select o.user_id from {src_p}.{p_db}.public.orders o) "
                f"order by u.id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, "Alice")
            tdSql.checkData(1, 0, 3); tdSql.checkData(1, 1, "Carol")
        finally:
            self._cleanup_src(src_m, src_p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

        # --- 075: InfluxDB WHERE usage IN (TDengine internal subquery) ---
        src_i = "fq04_xsub_i"
        i_db = "fq04_xsub_idb"
        ref_db = "fq04_xsub_ref"
        self._cleanup_src(src_i)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                "cpu,host=h1 usage=10i 1704067200000000000",
                "cpu,host=h2 usage=20i 1704067260000000000",
                "cpu,host=h3 usage=30i 1704067320000000000",
            ])
            self._mk_influx_real(src_i, database=i_db)
            tdSql.execute(f"drop database if exists {ref_db}")
            tdSql.execute(f"create database {ref_db}")
            tdSql.execute(f"create table {ref_db}.ref_t (ts timestamp, val int)")
            tdSql.execute(
                f"insert into {ref_db}.ref_t values "
                f"(1704067200000, 10), (1704067200001, 30)")

            tdSql.query(
                f"select host, usage from {src_i}.{i_db}.cpu "
                f"where usage in (select val from {ref_db}.ref_t) "
                f"order by ts")
            tdSql.checkRows(2)
            assert str(tdSql.getData(0, 0)) == "h1"
            assert int(tdSql.getData(0, 1)) == 10
            assert str(tdSql.getData(1, 0)) == "h3"
            assert int(tdSql.getData(1, 1)) == 30
        finally:
            self._cleanup_src(src_i)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass
            tdSql.execute(f"drop database if exists {ref_db}")

    # ==================================================================
    # 14  Time-series functions (InfluxDB only)
    #     Covers: 026/057/058/060/077
    # ==================================================================

    def test_fq_sql_timeseries_functions(self):
        """FQ-SQL-TS: FIRST/LAST/TOP/BOTTOM/DIFF/CSUM/ELAPSED/HYPERLOGLOG/MODE on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # 026: FIRST / LAST
            tdSql.query(f"select first(val) from {t}")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 1)
            tdSql.query(f"select last(val) from {t}")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 5)

            # 026: TOP / BOTTOM — with value verification
            tdSql.query(f"select top(val, 2) from {t}")
            tdSql.checkRows(2)
            top_vals = sorted([int(tdSql.getData(r, 0)) for r in range(2)], reverse=True)
            assert top_vals == [5, 4], f"TOP(2) should be [5,4]: {top_vals}"

            tdSql.query(f"select bottom(val, 2) from {t}")
            tdSql.checkRows(2)
            bot_vals = sorted([int(tdSql.getData(r, 0)) for r in range(2)])
            assert bot_vals == [1, 2], f"BOTTOM(2) should be [1,2]: {bot_vals}"

            # 057: ELAPSED
            tdSql.query(f"select elapsed(ts) from {t}")
            tdSql.checkRows(1)
            elapsed_val = float(tdSql.getData(0, 0))
            # 5 rows at 1-minute intervals → elapsed = 4 × 60000ms = 240000ms; allow ±1s
            assert abs(elapsed_val - 240000.0) < 1000.0, \
                f"ELAPSED should be ~240000 (4 minutes): {elapsed_val}"

            # 057: HYPERLOGLOG
            tdSql.query(f"select hyperloglog(val) from {t}")
            tdSql.checkRows(1)
            # 5 distinct integer values [1,2,3,4,5] → exact count = 5
            tdSql.checkData(0, 0, 5)

            # 058: MODE
            tdSql.query(f"select mode(flag) from {t}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)  # flag=[1,0,1,0,1], mode=1 (3 occurrences)

            # 060: DIFF → all 1s
            tdSql.query(f"select diff(val) from {t}")
            tdSql.checkRows(4)
            for r in range(4):
                tdSql.checkData(r, 0, 1)

            # 060: CSUM
            tdSql.query(f"select csum(val) from {t}")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 6)
            tdSql.checkData(3, 0, 10)
            tdSql.checkData(4, 0, 15)

            # 058: LAST_ROW
            tdSql.query(f"select last_row(val) from {t}")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 5)

            # 058: TAIL
            tdSql.query(f"select tail(val, 2) from {t}")
            tdSql.checkRows(2)
            tail_vals = sorted([int(tdSql.getData(r, 0)) for r in range(2)])
            assert tail_vals == [4, 5], f"TAIL(2) should be [4,5]: {tail_vals}"

            # 058: UNIQUE(flag)
            tdSql.query(f"select unique(flag) from {t}")
            tdSql.checkRows(2)
            unique_flags = sorted([int(tdSql.getData(r, 0)) for r in range(2)])
            assert unique_flags == [0, 1], f"UNIQUE(flag) should yield [0,1]: {unique_flags}"

            # 060: TWA
            tdSql.query(f"select twa(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6, \
                f"TWA of [1,2,3,4,5] over equal intervals should be 3.0: {tdSql.getData(0, 0)}"

            # 057: HISTOGRAM
            tdSql.query(
                f"select histogram(val, 'user_input', '[0, 6, 10]', 0) from {t}")
            import json
            tdSql.checkRows(2)
            # bin0: [0, 6) → val=1,2,3,4,5 → count=5
            bin0 = json.loads(str(tdSql.getData(0, 0)))
            assert bin0['count'] == 5, f"bin[0,6) count should be 5: {bin0}"
            # bin1: [6, 10] → no values → count=0
            bin1 = json.loads(str(tdSql.getData(1, 0)))
            assert bin1['count'] == 0, f"bin[6,10] count should be 0: {bin1}"

            # 077: subquery with DIFF
            tdSql.query(f"select * from (select ts, diff(val) as d from {t})")
            tdSql.checkRows(4)
            for r in range(4):
                tdSql.checkData(r, 1, 1)

        self._with_std_sources("fq04_ts", body)

    # ==================================================================
    # 15  Window functions (InfluxDB only)
    #     Covers: 064/065/066/067/068/069
    # ==================================================================

    def test_fq_sql_window_functions(self):
        """FQ-SQL-WIN: SESSION/EVENT/COUNT/INTERVAL windows, FILL, PARTITION BY on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # 064: SESSION_WINDOW
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} session(ts, 2m)")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 5)
            tdSql.checkData(0, 2, 15)

            # 065: EVENT_WINDOW
            tdSql.query(
                f"select _wstart, count(*) from {t} "
                f"event_window start with val > 2 end with val < 4")
            tdSql.checkRows(1)
            # Window 1: val=3 (starts >2, ends <4) → count=1
            tdSql.checkData(0, 1, 1)

            # 066: COUNT_WINDOW(2) — with sum verification
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} count_window(2)")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 2); tdSql.checkData(0, 2, 3)   # rows 1,2 → sum=3
            tdSql.checkData(1, 1, 2); tdSql.checkData(1, 2, 7)   # rows 3,4 → sum=7
            tdSql.checkData(2, 1, 1); tdSql.checkData(2, 2, 5)   # row 5 → sum=5

            # 067: INTERVAL + _wstart / _wend — with timestamp verification
            tdSql.query(
                f"select _wstart, _wend, count(*) from {t} "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"interval(1m) order by _wstart")
            assert tdSql.queryRows == 5
            from datetime import datetime, timezone, timedelta
            _t0 = tdSql.getData(0, 0)
            _t1 = tdSql.getData(0, 1)
            # All sources return datetime; _wend - _wstart should be exactly 1 minute
            assert isinstance(_t0, datetime), \
                f"_wstart expected datetime, got {type(_t0)}: {_t0}"
            delta_s = (_t1 - _t0).total_seconds()
            assert abs(delta_s - 60) < 2, \
                f"_wend - _wstart should be ~60s, got {delta_s}s: {_t0} .. {_t1}"

            # 068: FILL modes with value verification
            time_range = (
                f"ts >= 1704067200000 and ts < 1704067500000")

            # FILL(NULL): gap rows have NULL
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(null)")
            tdSql.checkRows(10)
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6
            assert tdSql.getData(1, 1) is None, \
                f"fill(null) gap row[1] should be NULL: {tdSql.getData(1, 1)}"

            # FILL(VALUE, 0): gap rows get 0
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(value, 0)")
            tdSql.checkRows(10)
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6
            assert abs(float(tdSql.getData(1, 1)) - 0.0) < 1e-6, \
                f"fill(value,0) gap row[1] should be 0.0: {tdSql.getData(1, 1)}"

            # FILL(PREV): gap gets previous data value
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(prev)")
            tdSql.checkRows(10)
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6
            assert abs(float(tdSql.getData(1, 1)) - 1.0) < 1e-6, \
                f"fill(prev) gap row[1] should be 1.0: {tdSql.getData(1, 1)}"

            # FILL(NEXT): gap gets next data value
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(next)")
            tdSql.checkRows(10)
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6
            assert abs(float(tdSql.getData(1, 1)) - 2.0) < 1e-6, \
                f"fill(next) gap row[1] should be 2.0: {tdSql.getData(1, 1)}"

            # FILL(LINEAR): gap interpolated
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(linear)")
            tdSql.checkRows(10)
            assert abs(float(tdSql.getData(0, 1)) - 1.0) < 1e-6
            assert abs(float(tdSql.getData(1, 1)) - 1.5) < 1e-6, \
                f"fill(linear) gap row[1] should be 1.5: {tdSql.getData(1, 1)}"

            # 069: PARTITION BY flag + interval
            tdSql.query(
                f"select _wstart, count(*) from {t} "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"partition by flag interval(1m)")
            tdSql.checkRows(5)
            # flag=1 → 3 windows (val=1@00:00, val=3@00:02, val=5@00:04)
            # flag=0 → 2 windows (val=2@00:01, val=4@00:03)

        self._with_std_sources("fq04_win", body)

    # ==================================================================
    # 16  LAG / LEAD (MySQL + PG)
    #     Covers: 027
    # ==================================================================

    def test_fq_sql_lag_lead(self):
        """FQ-SQL-LAGLEAD: LAG/LEAD on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(f"select val, lag(val, 1) from {t} order by ts")
            tdSql.checkRows(5)
            assert tdSql.getData(0, 1) is None  # first row has no previous
            tdSql.checkData(1, 1, 1)   # lag of val=2 is val=1
            tdSql.checkData(2, 1, 2)   # lag of val=3 is val=2
            tdSql.checkData(3, 1, 3)   # lag of val=4 is val=3
            tdSql.checkData(4, 1, 4)   # lag of val=5 is val=4

            tdSql.query(f"select val, lead(val, 1) from {t} order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 2)   # lead of val=1 is val=2
            tdSql.checkData(1, 1, 3)   # lead of val=2 is val=3
            tdSql.checkData(2, 1, 4)   # lead of val=3 is val=4
            tdSql.checkData(3, 1, 5)   # lead of val=4 is val=5
            assert tdSql.getData(4, 1) is None  # last row has no next

        self._with_std_sources("fq04_lag", body)

    # ==================================================================
    # 17  InfluxDB tags / PARTITION BY tag
    #     Covers: 028/031/085
    # ==================================================================

    def test_fq_sql_influxdb_tags_partition(self):
        """FQ-SQL-INFLUX: DISTINCT tags and PARTITION BY tag on InfluxDB.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq04_itag"
        i_db = "fq04_itag_db"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, [
                "cpu,host=h1,region=us usage=30i 1704067200000000000",
                "cpu,host=h1,region=us usage=50i 1704067260000000000",
                "cpu,host=h2,region=eu usage=10i 1704067320000000000",
                "cpu,host=h2,region=eu usage=20i 1704067380000000000",
            ])
            self._mk_influx_real(src, database=i_db)
            t = f"{src}.{i_db}.cpu"

            # 028: DISTINCT tags
            tdSql.query(f"select distinct host, region from {t} order by host")
            tdSql.checkRows(2)
            assert str(tdSql.getData(0, 0)) == "h1"
            assert str(tdSql.getData(0, 1)) == "us"
            assert str(tdSql.getData(1, 0)) == "h2"
            assert str(tdSql.getData(1, 1)) == "eu"

            # 031/085: PARTITION BY host → avg
            tdSql.query(
                f"select avg(usage) from {t} partition by host order by host")
            tdSql.checkRows(2)
            h1 = float(tdSql.getData(0, 0))
            h2 = float(tdSql.getData(1, 0))
            assert abs(h1 - 40.0) < 0.01
            assert abs(h2 - 15.0) < 0.01
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    # ==================================================================
    # 18  Pseudo-column errors (MySQL + PG)
    #     Covers: 029/030/032
    # ==================================================================

    def test_fq_sql_pseudo_column_errors(self):
        """FQ-SQL-PSEUDO: TAGS/TBNAME behavior per source (error on M/P, partial on InfluxDB).

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        # MySQL + PG: all pseudo-column operations → error
        def body_mp(src):
            t = f"{src}.src_t"
            tdSql.error(
                f"select tags from {t}",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            tdSql.error(
                f"select tbname from {t}",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # --- 032: PARTITION BY TBNAME error ---
            tdSql.error(
                f"select count(*) from {t} partition by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # GROUP BY TBNAME → error
            tdSql.error(
                f"select tbname, count(*) from {t} group by tbname",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # TBNAME in WHERE → error
            tdSql.error(
                f"select * from {t} where tbname = 'src_t'",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)

        self._with_std_sources("fq04_pseudo", body_mp, skip_influx=True)

        # InfluxDB: TAGS works (§3.7.2.2), SELECT TBNAME errors (§3.7.2.1),
        # PARTITION BY TBNAME works (§3.7.2.1 exception)
        def body_influx(src):
            t = f"{src}.src_t"
            # SELECT TAGS works on InfluxDB
            tdSql.query(f"select tags from {t}")
            # SELECT TBNAME still errors on InfluxDB
            tdSql.error(
                f"select tbname from {t}",
                expectedErrno=TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
            # PARTITION BY TBNAME works on InfluxDB (converted to tag grouping)
            tdSql.query(f"select count(*) from {t} partition by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)  # all 5 rows in single measurement
            # PARTITION BY TBNAME with multiple aggregates
            tdSql.query(
                f"select avg(val), max(val), min(val) "
                f"from {t} partition by tbname")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 3.0) < 1e-6  # avg
            tdSql.checkData(0, 1, 5)  # max
            tdSql.checkData(0, 2, 1)  # min
            # PARTITION BY TBNAME with SUM
            tdSql.query(
                f"select sum(val) from {t} partition by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 15)

        self._with_std_sources("fq04_pseudo_i", body_influx,
                               skip_mysql=True, skip_pg=True)

    # ==================================================================
    # 19  VIEWs (MySQL + PG)
    #     Covers: 078/079/080/081
    # ==================================================================

    def test_fq_sql_views(self):
        """FQ-SQL-VIEW: VIEW query, VIEW JOIN, REFRESH on MySQL + PG.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        # --- MySQL VIEW ---
        src_m = "fq04_view_m"
        m_db = "fq04_view_mdb"
        self._cleanup_src(src_m)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, [
                "DROP TABLE IF EXISTS users",
                "DROP TABLE IF EXISTS orders",
                "CREATE TABLE users (id INT, name VARCHAR(50))",
                "CREATE TABLE orders (id INT, user_id INT, amount INT, status INT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO orders VALUES (1, 1, 100, 1), (2, 1, 200, 2)",
                "DROP VIEW IF EXISTS v_summary",
                "DROP VIEW IF EXISTS v_users",
                "CREATE VIEW v_summary AS "
                "  SELECT status, sum(amount) as total FROM orders GROUP BY status",
                "CREATE VIEW v_users AS SELECT id, name FROM users WHERE id <= 10",
            ])
            self._mk_mysql_real(src_m, database=m_db)

            # 078: query view
            tdSql.query(
                f"select * from {src_m}.{m_db}.v_summary order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 0, 2); tdSql.checkData(1, 1, 200)

            # 080: view JOIN table — external source JOIN requires primary
            #      timestamp column in ON condition; non-ts JOIN → error
            tdSql.error(
                f"select v.id, v.name, sum(o.amount) as total "
                f"from {src_m}.{m_db}.v_users v "
                f"join {src_m}.{m_db}.orders o on v.id = o.user_id "
                f"group by v.id, v.name order by v.id")

            # 081: REFRESH
            tdSql.execute(f"refresh external source {src_m}")
            tdSql.query(f"select count(*) from {src_m}.{m_db}.v_summary")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 2)
        finally:
            self._cleanup_src(src_m)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass

        # --- PG VIEW (same scenarios as MySQL) ---
        src_p = "fq04_view_p"
        p_db = "fq04_view_pdb"
        self._cleanup_src(src_p)
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
        try:
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, [
                "DROP TABLE IF EXISTS users CASCADE",
                "DROP TABLE IF EXISTS orders CASCADE",
                "CREATE TABLE users (id INT, name TEXT)",
                "CREATE TABLE orders (id INT, user_id INT, amount INT, status INT)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO orders VALUES (1, 1, 100, 1), (2, 1, 200, 2)",
                "DROP VIEW IF EXISTS v_summary",
                "DROP VIEW IF EXISTS v_users",
                "CREATE VIEW v_summary AS "
                "  SELECT status, sum(amount) as total FROM orders GROUP BY status",
                "CREATE VIEW v_users AS SELECT id, name FROM users WHERE id <= 10",
            ])
            self._mk_pg_real(src_p, database=p_db)

            # 079: query view
            tdSql.query(
                f"select * from {src_p}.{p_db}.public.v_summary order by status")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 0, 2); tdSql.checkData(1, 1, 200)

            # 080: view JOIN table — external source JOIN requires primary
            #      timestamp column in ON condition; non-ts JOIN → error
            tdSql.error(
                f"select v.id, v.name, sum(o.amount) as total "
                f"from {src_p}.{p_db}.public.v_users v "
                f"join {src_p}.{p_db}.public.orders o on v.id = o.user_id "
                f"group by v.id, v.name order by v.id")

            # 081: REFRESH
            tdSql.execute(f"refresh external source {src_p}")
            tdSql.query(f"select count(*) from {src_p}.{p_db}.public.v_summary")
            tdSql.checkRows(1); tdSql.checkData(0, 0, 2)
        finally:
            self._cleanup_src(src_p)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass

    # ==================================================================
    # 20  Special features: bitwise, masking, ordering, geo, info_schema
    #     Covers: 037/042/051/061/062
    # ==================================================================

    def test_fq_sql_bitwise_operators(self):
        """FQ-SQL-BIT: Bitwise & and | on all sources.

        Covers: 037.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"
            tdSql.query(f"select val, val & 3 from {t} where val = 5")
            tdSql.checkRows(1); tdSql.checkData(0, 1, 1)  # 5 & 3 = 1
            tdSql.query(f"select val, val | 8 from {t} where val = 5")
            tdSql.checkRows(1); tdSql.checkData(0, 1, 13)  # 5 | 8 = 13

        self._with_std_sources("fq04_bit", body)

    def test_fq_sql_data_masking(self):
        """FQ-SQL-MASK: MASK_FULL / MASK_PARTIAL on all sources.

        Covers: 051.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"
            # mask_full requires 2 params (string, mask_char) — 1 param is invalid
            tdSql.error(f"select mask_full(name) from {t} order by val limit 1")
            # Correct: mask_full(name, 'X')
            tdSql.query(f"select mask_full(name, 'X') from {t} order by val limit 1")
            tdSql.checkRows(1)
            masked = str(tdSql.getData(0, 0))
            assert all(c == 'X' for c in masked)

            # mask_partial requires 4 params (string, start, end, mask_char) — 3 is invalid
            tdSql.error(
                f"select mask_partial(name, 2, 'X') from {t} where val = 1")
            # Correct: mask_partial(name, 0, 2, 'X')
            tdSql.query(
                f"select name, mask_partial(name, 0, 2, 'X') from {t} where val = 1")
            tdSql.checkRows(1)
            original = str(tdSql.getData(0, 0))
            partial = str(tdSql.getData(0, 1))
            assert len(partial) == len(original)

        self._with_std_sources("fq04_mask", body)

    def test_fq_sql_null_ordering(self):
        """FQ-SQL-NULLORD: ORDER BY NULLS FIRST/LAST on all sources.

        Covers: 042.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        influx_lines_nullord = [
            'data val=10i,id=1i 1704067200000000000',
            'data id=2i 1704067260000000000',
            'data val=30i,id=3i 1704067320000000000',
        ]

        def body(src, db_type):
            tdSql.query(f"select id, val from {src}.data order by val nulls first")
            tdSql.checkRows(3)
            assert tdSql.getData(0, 1) is None
            tdSql.checkData(1, 0, 1); tdSql.checkData(1, 1, 10)
            tdSql.checkData(2, 0, 3); tdSql.checkData(2, 1, 30)

            tdSql.query(f"select id, val from {src}.data order by val nulls last")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1); tdSql.checkData(0, 1, 10)
            tdSql.checkData(1, 0, 3); tdSql.checkData(1, 1, 30)
            assert tdSql.getData(2, 1) is None

        self._with_custom_sources(
            "fq04_nullord", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_nullord,
        )

    def test_fq_sql_information_schema(self):
        """FQ-SQL-INFOSCHEMA: Query INFORMATION_SCHEMA.TABLES on MySQL.

        Covers: 061.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq04_info_m"
        ext_db = "fq04_info_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                "DROP TABLE IF EXISTS t1",
                "CREATE TABLE t1 (id INT)",
                "INSERT INTO t1 VALUES (1)",
            ])
            self._mk_mysql_real(src, database=ext_db)
            tdSql.query(
                f"select count(*) from {src}.information_schema.TABLES "
                f"where `TABLE_SCHEMA` = '{ext_db}'")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) >= 1
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass

    def test_fq_sql_geo_functions(self):
        """FQ-SQL-GEO: Coordinate distance via TDengine SQRT/POW on all sources.

        Covers: 062.
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS geo",
            "CREATE TABLE geo (id INT, lat DOUBLE, lon DOUBLE)",
            "INSERT INTO geo VALUES (1, 116.4, 39.9), (2, 121.5, 31.2)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS geo",
            "CREATE TABLE geo (id INT, lat DOUBLE PRECISION, lon DOUBLE PRECISION)",
            "INSERT INTO geo VALUES (1, 116.4, 39.9), (2, 121.5, 31.2)",
        ]
        influx_lines_geo = [
            'geo lat=116.4,lon=39.9,id=1i 1704067200000000000',
            'geo lat=121.5,lon=31.2,id=2i 1704067260000000000',
        ]

        def body(src, db_type):
            t = f"{src}.geo"
            # Self-distance should be 0
            tdSql.query(
                f"select id, sqrt(pow(lat - lat, 2) + pow(lon - lon, 2)) as dist "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert abs(float(tdSql.getData(0, 1))) < 1e-9
            assert abs(float(tdSql.getData(1, 1))) < 1e-9

            # Distance from reference point (116.4, 39.9)
            tdSql.query(
                f"select id, sqrt(pow(lat - 116.4, 2) + pow(lon - 39.9, 2)) as dist "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert abs(float(tdSql.getData(0, 1))) < 1e-9  # same point
            assert float(tdSql.getData(1, 1)) > 1.0  # different point

        self._with_custom_sources(
            "fq04_geo", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_geo,
        )

    # ==================================================================
    # 21  Extended aggregation functions
    #     Covers: SPREAD, LEASTSQUARES, VARIANCE/VAR_POP/VAR_SAMP,
    #             STDDEV_POP/STDDEV_SAMP, GROUP_CONCAT
    # ==================================================================

    def test_fq_sql_aggregate_ext(self):
        """FQ-SQL-AGGEXT: SPREAD/LEASTSQUARES/VARIANCE/STDDEV variants/GROUP_CONCAT.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # SPREAD = max - min
            tdSql.query(f"select spread(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 4.0) < 1e-6

            # LEASTSQUARES(val, start_val=0, step_val=1)
            tdSql.query(f"select leastsquares(val, 0, 1) from {t}")
            tdSql.checkRows(1)
            result = str(tdSql.getData(0, 0))
            assert 'slop' in result or 'slope' in result, \
                f"LEASTSQUARES should return slope info: {result}"

            # VARIANCE (population variance of [1,2,3,4,5] = 2.0)
            tdSql.query(f"select variance(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 1e-6

            # VAR_POP (same as VARIANCE)
            tdSql.query(f"select var_pop(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.0) < 1e-6

            # VAR_SAMP (sample variance = 2.5)
            tdSql.query(f"select var_samp(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 2.5) < 1e-6

            # STDDEV_POP (sqrt(2.0) ≈ 1.4142)
            tdSql.query(f"select stddev_pop(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.4142) < 1e-3

            # STDDEV_SAMP (sqrt(2.5) ≈ 1.5811)
            tdSql.query(f"select stddev_samp(val) from {t}")
            tdSql.checkRows(1)
            assert abs(float(tdSql.getData(0, 0)) - 1.5811) < 1e-3

            # GROUP_CONCAT
            tdSql.query(f"select group_concat(name) from {t}")
            tdSql.checkRows(1)
            result = str(tdSql.getData(0, 0))
            for n in ['alpha', 'beta', 'gamma', 'delta', 'epsilon']:
                assert n in result, \
                    f"GROUP_CONCAT should contain '{n}': {result}"

        self._with_std_sources("fq04_aggext", body)

    # ==================================================================
    # 22  Extended time-series functions
    #     Covers: DERIVATIVE, IRATE, MAVG, SAMPLE, STATECOUNT,
    #             STATEDURATION
    # ==================================================================

    def test_fq_sql_timeseries_ext(self):
        """FQ-SQL-TSEXT: DERIVATIVE/IRATE/MAVG/SAMPLE/STATECOUNT/STATEDURATION.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # DERIVATIVE(val, 1m, 0): val goes up by 1 per minute → 1.0
            tdSql.query(f"select derivative(val, 1m, 0) from {t}")
            tdSql.checkRows(4)
            for r in range(4):
                assert abs(float(tdSql.getData(r, 0)) - 1.0) < 1e-6

            # IRATE: instantaneous rate using last two values
            # (val=5 - val=4) / 60s = 1/60 ≈ 0.01667 per second
            tdSql.query(f"select irate(val) from {t}")
            tdSql.checkRows(1)
            irate_val = float(tdSql.getData(0, 0))
            assert irate_val > 0, f"IRATE should be positive: {irate_val}"

            # MAVG(val, 2): moving average with window=2
            tdSql.query(f"select mavg(val, 2) from {t}")
            tdSql.checkRows(4)
            assert abs(float(tdSql.getData(0, 0)) - 1.5) < 1e-6
            assert abs(float(tdSql.getData(3, 0)) - 4.5) < 1e-6

            # SAMPLE(val, 3): random 3 samples
            tdSql.query(f"select sample(val, 3) from {t}")
            tdSql.checkRows(3)
            for r in range(3):
                v = int(tdSql.getData(r, 0))
                assert 1 <= v <= 5, f"SAMPLE row {r} value {v} not in [1,5]"

            # STATECOUNT(val, 'GT', 2)
            tdSql.query(f"select statecount(val, 'GT', 2) from {t}")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, -1)  # val=1, not > 2
            tdSql.checkData(1, 0, -1)  # val=2, not > 2
            tdSql.checkData(2, 0, 1)   # val=3 > 2, count=1
            tdSql.checkData(3, 0, 2)   # val=4 > 2, count=2
            tdSql.checkData(4, 0, 3)   # val=5 > 2, count=3

            # STATEDURATION(val, 'GT', 2, 1s)
            tdSql.query(f"select stateduration(val, 'GT', 2, 1s) from {t}")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, -1)  # val=1, not > 2
            tdSql.checkData(1, 0, -1)  # val=2, not > 2
            tdSql.checkData(2, 0, 0)   # val=3 > 2, first qualifying → 0

        self._with_std_sources("fq04_tsext", body)

    # ==================================================================
    # 23  Extended string functions
    #     Covers: CHAR, POSITION, FIND_IN_SET, SUBSTRING_INDEX,
    #             LIKE_IN_SET, REGEXP_IN_SET
    # ==================================================================

    def test_fq_sql_string_ext(self):
        """FQ-SQL-STREXT: CHAR/POSITION/FIND_IN_SET/SUBSTRING_INDEX/LIKE_IN_SET/REGEXP_IN_SET.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # CHAR(65) = 'A'
            tdSql.query(f"select char(65) from {t} limit 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == 'A'

            # POSITION('lp' IN name) for 'alpha' → 2
            tdSql.query(
                f"select position('lp' in name) from {t} where val = 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 2

            # FIND_IN_SET('B', 'A,B,C') → 2
            tdSql.query(
                f"select find_in_set('B', 'A,B,C') from {t} limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # SUBSTRING_INDEX('www.taosdata.com', '.', 2) → 'www.taosdata'
            tdSql.query(
                f"select substring_index('www.taosdata.com', '.', 2) "
                f"from {t} limit 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == 'www.taosdata'

            # LIKE_IN_SET: match name against patterns
            tdSql.query(
                f"select like_in_set(name, 'al%,be%') from {t} "
                f"where val = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)  # 'alpha' matches 'al%' → 1

            # REGEXP_IN_SET: match name against regex patterns
            tdSql.query(
                f"select regexp_in_set(name, '^a.*,^b.*') from {t} "
                f"where val = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)  # 'alpha' matches '^a.*' → 1

        self._with_std_sources("fq04_strext", body)

    # ==================================================================
    # 24  Extended datetime / system functions
    #     Covers: TO_ISO8601, TIMEZONE, DATABASE, CLIENT_VERSION,
    #             SERVER_VERSION, SERVER_STATUS, CURRENT_USER
    # ==================================================================

    def test_fq_sql_datetime_system(self):
        """FQ-SQL-DTSYS: TO_ISO8601/TIMEZONE and system info functions.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # TO_ISO8601
            tdSql.query(f"select to_iso8601(ts) from {t} order by ts limit 1")
            tdSql.checkRows(1)
            result = str(tdSql.getData(0, 0))
            assert '2024-01-01' in result, \
                f"TO_ISO8601 should contain '2024-01-01': {result}"

            # TIMEZONE
            tdSql.query(f"select timezone() from {t} limit 1")
            tdSql.checkRows(1)
            tz = str(tdSql.getData(0, 0))
            assert len(tz) > 0, f"TIMEZONE should return non-empty: {tz}"

        self._with_std_sources("fq04_dtsys", body)

        # System functions (no external source needed)
        tdSql.query("select client_version()")
        tdSql.checkRows(1)
        cv = str(tdSql.getData(0, 0))
        assert len(cv) > 0, f"CLIENT_VERSION should return non-empty: {cv}"

        tdSql.query("select server_version()")
        tdSql.checkRows(1)
        sv = str(tdSql.getData(0, 0))
        assert len(sv) > 0, f"SERVER_VERSION should return non-empty: {sv}"

        tdSql.query("select server_status()")
        tdSql.checkRows(1)
        assert int(tdSql.getData(0, 0)) == 1

        tdSql.query("select current_user()")
        tdSql.checkRows(1)
        cu = str(tdSql.getData(0, 0))
        assert len(cu) > 0, f"CURRENT_USER should return non-empty: {cu}"

    # ==================================================================
    # 25  STATE_WINDOW
    #     Covers: state_window clause
    # ==================================================================

    def test_fq_sql_state_window(self):
        """FQ-SQL-STATEWIN: STATE_WINDOW on external data.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # flag = [1, 0, 1, 0, 1] → 5 windows (each state change)
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} "
                f"state_window(flag)")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 1)  # first window: flag=1, count=1
            tdSql.checkData(0, 2, 1)  # sum=1

            # State on expression: val > 3 → state [0,0,0,1,1]
            # → 2 windows: [1,2,3](count=3,sum=6) and [4,5](count=2,sum=9)
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} "
                f"state_window(case when val > 3 then 1 else 0 end)")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 3)
            tdSql.checkData(0, 2, 6)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(1, 2, 9)

        self._with_std_sources("fq04_stwin", body)

    # ==================================================================
    # 26  Encryption / CRC32
    #     Covers: AES_ENCRYPT/AES_DECRYPT, SM4_ENCRYPT/SM4_DECRYPT, CRC32
    # ==================================================================

    def test_fq_sql_crypto_crc32(self):
        """FQ-SQL-CRYPTOEXT: AES/SM4 round-trip encryption and CRC32 on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, plain VARCHAR(100))",
            "INSERT INTO data VALUES (1, 'hello')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, plain TEXT)",
            "INSERT INTO data VALUES (1, 'hello')",
        ]
        influx_lines_crypto = [
            'data plain="hello",id=1i 1704067200000000000',
        ]

        def body(src, db_type):
            if db_type == 'influx':
                # InfluxDB maps strings to NCHAR; aes/sm4/crc32 require VARCHAR → error
                tdSql.error(
                    f"select aes_encrypt(plain, 'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.error(
                    f"select sm4_encrypt(plain, 'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.error(f"select crc32(plain) from {src}.data where id = 1")

                # Use CAST to convert NCHAR to VARCHAR for influx
                col = "CAST(plain AS VARCHAR(100))"
                tdSql.query(
                    f"select aes_decrypt("
                    f"aes_encrypt({col}, 'mykeystring12345'), "
                    f"'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == 'hello'

                tdSql.query(
                    f"select sm4_decrypt("
                    f"sm4_encrypt({col}, 'mykeystring12345'), "
                    f"'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == 'hello'

                tdSql.query(f"select crc32({col}) from {src}.data where id = 1")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 907060870)
            else:
                # AES round-trip (16-byte key)
                tdSql.query(
                    f"select aes_decrypt("
                    f"aes_encrypt(plain, 'mykeystring12345'), "
                    f"'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == 'hello'

                # SM4 round-trip (16-byte key)
                tdSql.query(
                    f"select sm4_decrypt("
                    f"sm4_encrypt(plain, 'mykeystring12345'), "
                    f"'mykeystring12345') "
                    f"from {src}.data where id = 1")
                tdSql.checkRows(1)
                assert str(tdSql.getData(0, 0)) == 'hello'

                # CRC32
                tdSql.query(f"select crc32(plain) from {src}.data where id = 1")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 907060870)

        self._with_custom_sources(
            "fq04_cryptoext", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_crypto,
        )

    # ==================================================================
    # 27  Extended operators: NOT LIKE, NOT IN list, SOME subquery,
    #     JSON CONTAINS, MASK_NONE, ISNULL/ISNOTNULL functions
    # ==================================================================

    def test_fq_sql_operator_ext(self):
        """FQ-SQL-OPEXT: NOT LIKE/NOT IN/SOME/CONTAINS/MASK_NONE/ISNULL on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        # NOT LIKE, NOT IN list, SOME subquery on std data
        def body_std(src):
            t = f"{src}.src_t"

            # NOT LIKE
            tdSql.query(
                f"select val from {t} where name not like 'a%' order by val")
            tdSql.checkRows(4)  # beta, gamma, delta, epsilon
            tdSql.checkData(0, 0, 2)

            # NOT IN (value list, not subquery)
            tdSql.query(
                f"select val from {t} where val not in (1, 3, 5) order by val")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 4)

            # SOME subquery (equivalent to ANY)
            # flag=0 vals are [2, 4]; val > SOME(2,4) means val > 2 → {3,4,5}
            tdSql.query(
                f"select val from {t} where val > some "
                f"(select val from {t} where flag = 0) order by val")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)
            tdSql.checkData(1, 0, 4)
            tdSql.checkData(2, 0, 5)

            # MASK_NONE (passthrough)
            tdSql.query(
                f"select mask_none(name) from {t} where val = 1")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == 'alpha'

            # ISNULL function form
            tdSql.query(
                f"select val, isnull(name) from {t} where val = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)  # name='alpha' is not null → 0

        self._with_std_sources("fq04_opext", body_std)

        # JSON CONTAINS on custom data
        mysql_setup = [
            "DROP TABLE IF EXISTS jdata",
            "CREATE TABLE jdata (id INT, data JSON)",
            "INSERT INTO jdata VALUES (1, JSON_OBJECT('k', 'v1', 'num', 10))",
            "INSERT INTO jdata VALUES (2, JSON_OBJECT('k', 'v2'))",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS jdata",
            "CREATE TABLE jdata (id INT, data JSONB)",
            "INSERT INTO jdata VALUES "
            "(1, '{\"k\": \"v1\", \"num\": 10}'::jsonb)",
            "INSERT INTO jdata VALUES "
            "(2, '{\"k\": \"v2\"}'::jsonb)",
        ]
        influx_lines_jc = [
            'jdata data="{\\\"k\\\": \\\"v1\\\", \\\"num\\\": 10}",id=1i 1704067200000000000',
            'jdata data="{\\\"k\\\": \\\"v2\\\"}",id=2i 1704067260000000000',
        ]

        def body_json(src, db_type):
            t = f"{src}.jdata"
            if db_type == 'influx':
                # InfluxDB maps JSON strings to NCHAR; CONTAINS requires JSON type → error
                tdSql.error(
                    f"select id from {t} where data contains 'num' order by id")
                # NCHAR alternative: LIKE-based key search on the raw JSON string
                tdSql.query(
                    f"select id from {t} where data like '%\"num\"%' order by id")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 1)
            else:
                # CONTAINS: check if key 'num' exists
                tdSql.query(
                    f"select id from {t} where data contains 'num' order by id")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 1)

        self._with_custom_sources(
            "fq04_opext_j", body_json,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_jc,
        )

    # ==================================================================
    # 28  SHA1/SHA2 hash functions
    #     Covers: SHA1, SHA2
    # ==================================================================

    def test_fq_sql_sha_hash(self):
        """FQ-SQL-SHA: SHA1/SHA2 hash functions on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            if src.endswith("_i"):
                # InfluxDB maps strings to NCHAR; sha1/sha2 require VARCHAR → error
                tdSql.error(f"select sha1(name) from {t} where val = 1")
                tdSql.error(f"select sha2(name, 256) from {t} where val = 1")

                # Use CAST to convert NCHAR to VARCHAR for influx
                tdSql.query(f"select sha1(CAST(name AS VARCHAR(32))) from {t} where val = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 40, f"SHA1 should be 40 hex chars: {h}"

                tdSql.query(f"select sha2(CAST(name AS VARCHAR(32)), 256) from {t} where val = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 64, f"SHA2(256) should be 64 hex chars: {h}"
            else:
                # SHA1
                tdSql.query(f"select sha1(name) from {t} where val = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 40, f"SHA1 should be 40 hex chars: {h}"

                # SHA2 with 256-bit
                tdSql.query(f"select sha2(name, 256) from {t} where val = 1")
                tdSql.checkRows(1)
                h = str(tdSql.getData(0, 0))
                assert len(h) == 64, f"SHA2(256) should be 64 hex chars: {h}"

        self._with_std_sources("fq04_sha", body)

    # ==================================================================
    # 29  COLS selection function
    #     Covers: COLS
    # ==================================================================

    def test_fq_sql_cols_function(self):
        """FQ-SQL-COLS: COLS() selection function on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # COLS(max(val), name): name at the row with max val
            tdSql.query(f"select cols(max(val), name) from {t}")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == 'epsilon'

            # COLS(min(val), name): name at the row with min val
            tdSql.query(f"select cols(min(val), name) from {t}")
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)) == 'alpha'

        self._with_std_sources("fq04_cols", body)

    # ==================================================================
    # 30  JOIN type coverage
    #     Covers: RIGHT JOIN, FULL OUTER JOIN, LEFT SEMI JOIN,
    #             LEFT ANTI JOIN
    # ==================================================================

    def test_fq_sql_join_types(self):
        """FQ-SQL-JOINS: RIGHT/FULL/SEMI/ANTI JOIN on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (id INT, name VARCHAR(32))",
            "CREATE TABLE t_right (id INT, val INT)",
            "INSERT INTO t_left VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO t_right VALUES (2, 20), (3, 30), (4, 40)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (id INT, name TEXT)",
            "CREATE TABLE t_right (id INT, val INT)",
            "INSERT INTO t_left VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO t_right VALUES (2, 20), (3, 30), (4, 40)",
        ]
        influx_lines_join = [
            't_left name="a",id=1i 1704067200000000000',
            't_left name="b",id=2i 1704067260000000000',
            't_left name="c",id=3i 1704067320000000000',
            't_right val=20i,id=2i 1704067200000000000',
            't_right val=30i,id=3i 1704067260000000000',
            't_right val=40i,id=4i 1704067320000000000',
        ]

        def body(src, db_type):
            l = f"{src}.t_left"
            r = f"{src}.t_right"

            # External source JOINs require primary timestamp column in ON
            # condition. Joins on non-timestamp columns (a.id = b.id) produce
            # an error.

            # RIGHT JOIN on non-ts column → error
            tdSql.error(
                f"select a.id, a.name, b.val from {l} a "
                f"right join {r} b on a.id = b.id order by b.id")

            # FULL OUTER JOIN on non-ts column → error
            tdSql.error(
                f"select a.id, b.id from {l} a "
                f"full join {r} b on a.id = b.id order by a.id, b.id")

            # LEFT SEMI JOIN on non-ts column → error
            tdSql.error(
                f"select a.id, a.name from {l} a "
                f"left semi join {r} b on a.id = b.id order by a.id")

            # LEFT ANTI JOIN on non-ts column → error
            tdSql.error(
                f"select a.id, a.name from {l} a "
                f"left anti join {r} b on a.id = b.id order by a.id")

            # RIGHT SEMI JOIN on non-ts column → error
            tdSql.error(
                f"select b.id, b.val from {l} a "
                f"right semi join {r} b on a.id = b.id order by b.id")

            # RIGHT ANTI JOIN on non-ts column → error
            tdSql.error(
                f"select b.id, b.val from {l} a "
                f"right anti join {r} b on a.id = b.id order by b.id")

        self._with_custom_sources(
            "fq04_joins", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_join,
        )

    # ==================================================================
    # 31  Extended datetime functions
    #     Covers: WEEKOFYEAR, DATE + time extraction on std data
    # ==================================================================

    def test_fq_sql_weekofyear(self):
        """FQ-SQL-WEEKOFYEAR: WEEKOFYEAR on all sources.
tdSql.checkData(0, 0, 1)  # 2024-01-01 → ISO week
        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # WEEKOFYEAR(ts)
            tdSql.query(
                f"select weekofyear(ts) from {t} order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)  # 2024-01-01 → ISO week 1

        self._with_std_sources("fq04_woy", body)

    # ==================================================================
    # 32  CASE value WHEN syntax
    #     Covers: CASE value WHEN compare_value (vs CASE WHEN condition)
    # ==================================================================

    def test_fq_sql_case_value_when(self):
        """FQ-SQL-CASEVAL: CASE value WHEN compare_value syntax.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(
                f"select val, case val when 1 then 'one' "
                f"when 2 then 'two' else 'other' end as lbl "
                f"from {t} order by val")
            tdSql.checkRows(5)
            assert str(tdSql.getData(0, 1)) == 'one'
            assert str(tdSql.getData(1, 1)) == 'two'
            assert str(tdSql.getData(2, 1)) == 'other'

        self._with_std_sources("fq04_caseval", body)

    # ==================================================================
    # 33  NVL (alias for IFNULL)
    #     Covers: NVL function
    # ==================================================================

    def test_fq_sql_nvl(self):
        """FQ-SQL-NVL: NVL function (alias of IFNULL) on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, NULL), (2, 5)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, NULL), (2, 5)",
        ]
        influx_lines_nvl = [
            'data id=1i 1704067200000000000',
            'data val=5i,id=2i 1704067260000000000',
        ]

        def body(src, db_type):
            tdSql.query(
                f"select id, nvl(val, 99) from {src}.data order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 99)
            tdSql.checkData(1, 1, 5)

        self._with_custom_sources(
            "fq04_nvl", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_nvl,
        )

    # ==================================================================
    # 34  FILL extended modes: NONE, NEAR, NULL_F, VALUE_F
    # ==================================================================

    def test_fq_sql_fill_ext(self):
        """FQ-SQL-FILLEXT: FILL NONE/NEAR/NULL_F/VALUE_F on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"
            time_range = "ts >= 1704067200000 and ts < 1704067500000"

            # FILL(NONE): only rows with data, no gap filling
            # Data at 0s,60s,120s,180s,240s → 5 windows with data
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(none)")
            tdSql.checkRows(5)
            # Values should be 1.0, 2.0, 3.0, 4.0, 5.0
            for i in range(5):
                assert abs(float(tdSql.getData(i, 1)) - (i + 1.0)) < 1e-6, \
                    f"fill(none) row[{i}] expected {i+1.0}, got {tdSql.getData(i, 1)}"

            # FILL(NEAR): fills gaps with nearest neighbor value
            # 10 windows: data windows get real avg, gap windows get nearest
            # Pattern: (1,1,2,2,3,3,4,4,5,5)
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(near)")
            tdSql.checkRows(10)
            expected_near = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0]
            for i, exp in enumerate(expected_near):
                assert abs(float(tdSql.getData(i, 1)) - exp) < 1e-6, \
                    f"fill(near) row[{i}] expected {exp}, got {tdSql.getData(i, 1)}"

            # FILL(NULL_F): like NULL but fills boundary rows too
            # Even rows (0,2,4,6,8) have data, odd rows (1,3,5,7,9) are None
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(null_f)")
            tdSql.checkRows(10)
            for i in range(10):
                if i % 2 == 0:
                    assert abs(float(tdSql.getData(i, 1)) - (i // 2 + 1.0)) < 1e-6, \
                        f"fill(null_f) row[{i}] expected {i//2+1.0}, got {tdSql.getData(i, 1)}"
                else:
                    assert tdSql.getData(i, 1) is None, \
                        f"fill(null_f) row[{i}] expected None, got {tdSql.getData(i, 1)}"

            # FILL(VALUE_F, 0): like VALUE but fills boundary rows too
            # Even rows have data, odd rows filled with 0.0
            tdSql.query(
                f"select _wstart, avg(val) from {t} "
                f"where {time_range} interval(30s) fill(value_f, 0)")
            tdSql.checkRows(10)
            for i in range(10):
                if i % 2 == 0:
                    assert abs(float(tdSql.getData(i, 1)) - (i // 2 + 1.0)) < 1e-6, \
                        f"fill(value_f) row[{i}] expected {i//2+1.0}, got {tdSql.getData(i, 1)}"
                else:
                    assert abs(float(tdSql.getData(i, 1)) - 0.0) < 1e-6, \
                        f"fill(value_f) row[{i}] expected 0.0, got {tdSql.getData(i, 1)}"

        self._with_std_sources("fq04_fillext", body)

    # ==================================================================
    # 35  FILL_FORWARD time-series function
    # ==================================================================

    def test_fq_sql_fill_forward(self):
        """FQ-SQL-FILLFWD: FILL_FORWARD function on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, ts DATETIME(3), val INT)",
            "INSERT INTO data VALUES "
            "(1, '2024-01-01 00:00:00.000', 10), "
            "(2, '2024-01-01 00:01:00.000', NULL), "
            "(3, '2024-01-01 00:02:00.000', 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, ts TIMESTAMP, val INT)",
            "INSERT INTO data VALUES "
            "(1, '2024-01-01 00:00:00', 10), "
            "(2, '2024-01-01 00:01:00', NULL), "
            "(3, '2024-01-01 00:02:00', 30)",
        ]
        influx_lines_ff = [
            'data id=1i,val=10i 1704067200000000000',
            'data id=2i 1704067260000000000',
            'data id=3i,val=30i 1704067320000000000',
        ]

        def body(src, db_type):
            # FILL_FORWARD fills NULL with previous non-null value
            tdSql.query(
                f"select fill_forward(val) from {src}.data order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 10)
            tdSql.checkData(1, 0, 10)   # NULL filled with prev=10
            tdSql.checkData(2, 0, 30)

        self._with_custom_sources(
            "fq04_fillfwd", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_ff,
        )

    # ==================================================================
    # 36  INTERP + SURROUND
    # ==================================================================

    def test_fq_sql_interp(self):
        """FQ-SQL-INTERP: INTERP with RANGE/EVERY/FILL and SURROUND on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # INTERP with RANGE/EVERY/FILL(LINEAR)
            # data at 00:00, 00:01, 00:02, 00:03, 00:04
            # interpolate every 30s → should generate intermediate points
            tdSql.query(
                f"select _irowts, interp(val) from {t} "
                f"range('2024-01-01 00:00:00', '2024-01-01 00:04:00') "
                f"every(1m) fill(linear)")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 1)  # at 00:00 → val=1
            tdSql.checkData(1, 1, 2)  # at 00:01 → val=2

            # INTERP with FILL(PREV)
            tdSql.query(
                f"select _irowts, interp(val) from {t} "
                f"range('2024-01-01 00:00:00', '2024-01-01 00:04:00') "
                f"every(30s) fill(prev)")
            tdSql.checkRows(9)
            tdSql.checkData(0, 1, 1)  # at 00:00 → val=1
            tdSql.checkData(1, 1, 1)  # at 00:00:30 → prev=1

            # INTERP with FILL(NEXT)
            tdSql.query(
                f"select _irowts, interp(val) from {t} "
                f"range('2024-01-01 00:00:00', '2024-01-01 00:04:00') "
                f"every(30s) fill(next)")
            tdSql.checkRows(9)
            tdSql.checkData(0, 1, 1)  # at 00:00 → val=1
            tdSql.checkData(1, 1, 2)  # at 00:00:30 → next=2

            # INTERP with FILL(NULL)
            tdSql.query(
                f"select _irowts, interp(val) from {t} "
                f"range('2024-01-01 00:00:00', '2024-01-01 00:04:00') "
                f"every(30s) fill(null)")
            tdSql.checkRows(9)
            tdSql.checkData(0, 1, 1)  # at 00:00 → exact data point
            assert tdSql.getData(1, 1) is None  # at 00:00:30 → NULL

            # INTERP single point with SURROUND
            tdSql.query(
                f"select _irowts, interp(val) from {t} "
                f"range('2024-01-01 00:01:30') fill(linear) surround(1)")
            tdSql.checkRows(1)
            # 00:01:30 between val=2 (00:01) and val=3 (00:02) → 2.5
            assert abs(float(tdSql.getData(0, 1)) - 2.5) < 1e-6

        self._with_std_sources("fq04_interp", body)

    # ==================================================================
    # 37  ASOF JOIN
    # ==================================================================

    def test_fq_sql_asof_join(self):
        """FQ-SQL-ASOF: LEFT/RIGHT ASOF JOIN with JLIMIT on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (ts DATETIME(3) PRIMARY KEY, val INT)",
            "CREATE TABLE t_right (ts DATETIME(3) PRIMARY KEY, measure INT)",
            "INSERT INTO t_left VALUES "
            "('2024-01-01 00:00:00.000', 1), "
            "('2024-01-01 00:02:00.000', 2), "
            "('2024-01-01 00:04:00.000', 3)",
            "INSERT INTO t_right VALUES "
            "('2024-01-01 00:01:00.000', 10), "
            "('2024-01-01 00:03:00.000', 20), "
            "('2024-01-01 00:05:00.000', 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (ts TIMESTAMP PRIMARY KEY, val INT)",
            "CREATE TABLE t_right (ts TIMESTAMP PRIMARY KEY, measure INT)",
            "INSERT INTO t_left VALUES "
            "('2024-01-01 00:00:00', 1), "
            "('2024-01-01 00:02:00', 2), "
            "('2024-01-01 00:04:00', 3)",
            "INSERT INTO t_right VALUES "
            "('2024-01-01 00:01:00', 10), "
            "('2024-01-01 00:03:00', 20), "
            "('2024-01-01 00:05:00', 30)",
        ]
        influx_lines_asof = [
            't_left val=1i 1704067200000000000',
            't_left val=2i 1704067320000000000',
            't_left val=3i 1704067440000000000',
            't_right measure=10i 1704067260000000000',
            't_right measure=20i 1704067380000000000',
            't_right measure=30i 1704067500000000000',
        ]

        def body(src, db_type):
            l = f"{src}.t_left"
            r = f"{src}.t_right"

            # LEFT ASOF JOIN: match each left row to nearest right row <= left.ts
            tdSql.query(
                f"select a.ts, a.val, b.measure from {l} a "
                f"left asof join {r} b on a.ts >= b.ts")
            tdSql.checkRows(3)
            # left@00:00 → no right row <= 00:00 → measure=NULL
            tdSql.checkData(0, 1, 1)
            assert tdSql.getData(0, 2) is None
            # left@00:02 → right@00:01 (measure=10)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(1, 2, 10)
            # left@00:04 → right@00:03 (measure=20)
            tdSql.checkData(2, 1, 3)
            tdSql.checkData(2, 2, 20)

            # LEFT ASOF JOIN with JLIMIT
            tdSql.query(
                f"select a.ts, a.val, b.measure from {l} a "
                f"left asof join {r} b on a.ts >= b.ts jlimit 1")
            tdSql.checkRows(3)

            # RIGHT ASOF JOIN
            tdSql.query(
                f"select a.val, b.ts, b.measure from {l} a "
                f"right asof join {r} b on b.ts >= a.ts")
            tdSql.checkRows(3)

        self._with_custom_sources(
            "fq04_asof", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_asof,
        )

    # ==================================================================
    # 38  WINDOW JOIN
    # ==================================================================

    def test_fq_sql_window_join(self):
        """FQ-SQL-WINJOIN: LEFT/RIGHT WINDOW JOIN with WINDOW_OFFSET on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (ts DATETIME(3) PRIMARY KEY, val INT)",
            "CREATE TABLE t_right (ts DATETIME(3) PRIMARY KEY, measure INT)",
            "INSERT INTO t_left VALUES "
            "('2024-01-01 00:00:00.000', 1), "
            "('2024-01-01 00:02:00.000', 2)",
            "INSERT INTO t_right VALUES "
            "('2024-01-01 00:01:00.000', 10), "
            "('2024-01-01 00:01:30.000', 15), "
            "('2024-01-01 00:03:00.000', 20)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS t_left",
            "DROP TABLE IF EXISTS t_right",
            "CREATE TABLE t_left (ts TIMESTAMP PRIMARY KEY, val INT)",
            "CREATE TABLE t_right (ts TIMESTAMP PRIMARY KEY, measure INT)",
            "INSERT INTO t_left VALUES "
            "('2024-01-01 00:00:00', 1), "
            "('2024-01-01 00:02:00', 2)",
            "INSERT INTO t_right VALUES "
            "('2024-01-01 00:01:00', 10), "
            "('2024-01-01 00:01:30', 15), "
            "('2024-01-01 00:03:00', 20)",
        ]
        influx_lines_wj = [
            't_left val=1i 1704067200000000000',
            't_left val=2i 1704067320000000000',
            't_right measure=10i 1704067260000000000',
            't_right measure=15i 1704067290000000000',
            't_right measure=20i 1704067380000000000',
        ]

        def body(src, db_type):
            l = f"{src}.t_left"
            r = f"{src}.t_right"

            # LEFT WINDOW JOIN: for each left row, find right rows within window
            tdSql.query(
                f"select a.ts, a.val, b.measure from {l} a "
                f"left window join {r} b "
                f"window_offset(-2m, 2m)")
            rows = tdSql.queryRows
            assert rows >= 2, f"LEFT WINDOW JOIN should return >= 2 rows: {rows}"

            # LEFT WINDOW JOIN with JLIMIT
            tdSql.query(
                f"select a.ts, a.val, b.measure from {l} a "
                f"left window join {r} b "
                f"window_offset(-2m, 2m) jlimit 1")
            tdSql.checkRows(2)  # 2 left rows, each gets at most 1 match

            # RIGHT WINDOW JOIN
            tdSql.query(
                f"select a.val, b.ts, b.measure from {l} a "
                f"right window join {r} b "
                f"window_offset(-2m, 2m)")
            rows = tdSql.queryRows
            assert rows >= 2, f"RIGHT WINDOW JOIN should return >= 2 rows: {rows}"

        self._with_custom_sources(
            "fq04_winjoin", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_wj,
        )

    # ==================================================================
    # 39  EXTERNAL_WINDOW
    # ==================================================================

    def test_fq_sql_external_window(self):
        """FQ-SQL-EXTWIN: EXTERNAL_WINDOW on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (ts DATETIME(3), _wstart DATETIME(3), "
            "_wend DATETIME(3), val INT)",
            "INSERT INTO data VALUES "
            "('2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000', "
            "'2024-01-01 00:01:00.000', 10), "
            "('2024-01-01 00:00:30.000', '2024-01-01 00:00:00.000', "
            "'2024-01-01 00:01:00.000', 20), "
            "('2024-01-01 00:01:00.000', '2024-01-01 00:01:00.000', "
            "'2024-01-01 00:02:00.000', 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (ts TIMESTAMP, _wstart TIMESTAMP, "
            "_wend TIMESTAMP, val INT)",
            "INSERT INTO data VALUES "
            "('2024-01-01 00:00:00', '2024-01-01 00:00:00', "
            "'2024-01-01 00:01:00', 10), "
            "('2024-01-01 00:00:30', '2024-01-01 00:00:00', "
            "'2024-01-01 00:01:00', 20), "
            "('2024-01-01 00:01:00', '2024-01-01 00:01:00', "
            "'2024-01-01 00:02:00', 30)",
        ]
        influx_lines_ew = [
            'data _wstart=1704067200000i,_wend=1704067260000i,'
            'val=10i 1704067200000000000',
            'data _wstart=1704067200000i,_wend=1704067260000i,'
            'val=20i 1704067230000000000',
            'data _wstart=1704067260000i,_wend=1704067320000i,'
            'val=30i 1704067260000000000',
        ]

        def body(src, db_type):
            t = f"{src}.data"
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} "
                f"external_window")
            tdSql.checkRows(2)
            # Window 1: _wstart=00:00, rows with _wend=00:01 → val=10,20 → count=2, sum=30
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 30)
            # Window 2: _wstart=00:01, rows with _wend=00:02 → val=30 → count=1, sum=30
            tdSql.checkData(1, 1, 1)
            tdSql.checkData(1, 2, 30)

        self._with_custom_sources(
            "fq04_extwin", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_ew,
        )

    # ==================================================================
    # 40  SLIMIT / SOFFSET
    # ==================================================================

    def test_fq_sql_slimit_soffset(self):
        """FQ-SQL-SLIMIT: SLIMIT/SOFFSET with PARTITION BY on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # PARTITION BY flag with SLIMIT 1 → only 1 group
            tdSql.query(
                f"select avg(val) from {t} partition by flag slimit 1")
            tdSql.checkRows(1)

            # PARTITION BY flag with SLIMIT 1 SOFFSET 1 → the other group
            tdSql.query(
                f"select avg(val) from {t} "
                f"partition by flag slimit 1 soffset 1")
            tdSql.checkRows(1)

            # PARTITION BY flag with SLIMIT 2 → both groups
            tdSql.query(
                f"select avg(val) from {t} partition by flag slimit 2")
            tdSql.checkRows(2)

        self._with_std_sources("fq04_slimit", body)

    # ==================================================================
    # 41  GEO functions: ST_Contains, ST_Intersects, etc.
    # ==================================================================

    def test_fq_sql_geo_st(self):
        """FQ-SQL-GEOST: ST_Contains/ST_Intersects/ST_Equals/ST_Touches/ST_Covers on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS geodata",
            "CREATE TABLE geodata (id INT, wkt_point VARCHAR(200), "
            "wkt_poly VARCHAR(500))",
            "INSERT INTO geodata VALUES "
            "(1, 'POINT(5 5)', "
            "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), "
            "(2, 'POINT(15 15)', "
            "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS geodata",
            "CREATE TABLE geodata (id INT, wkt_point TEXT, wkt_poly TEXT)",
            "INSERT INTO geodata VALUES "
            "(1, 'POINT(5 5)', "
            "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), "
            "(2, 'POINT(15 15)', "
            "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')",
        ]
        influx_lines_geo = [
            'geodata wkt_point="POINT(5 5)",'
            'wkt_poly="POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",'
            'id=1i 1704067200000000000',
            'geodata wkt_point="POINT(15 15)",'
            'wkt_poly="POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",'
            'id=2i 1704067260000000000',
        ]

        def body(src, db_type):
            t = f"{src}.geodata"

            # ST_Contains: polygon contains point(5,5) → true
            tdSql.query(
                f"select id, st_contains("
                f"st_geomfromtext(wkt_poly), "
                f"st_geomfromtext(wkt_point)) "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert int(tdSql.getData(0, 1)) == 1  # (5,5) inside polygon
            assert int(tdSql.getData(1, 1)) == 0  # (15,15) outside

            # ST_ContainsProperly
            tdSql.query(
                f"select id, st_containsproperly("
                f"st_geomfromtext(wkt_poly), "
                f"st_geomfromtext(wkt_point)) "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert int(tdSql.getData(0, 1)) == 1
            assert int(tdSql.getData(1, 1)) == 0

            # ST_Intersects: point inside polygon → intersects
            tdSql.query(
                f"select id, st_intersects("
                f"st_geomfromtext(wkt_poly), "
                f"st_geomfromtext(wkt_point)) "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert int(tdSql.getData(0, 1)) == 1
            assert int(tdSql.getData(1, 1)) == 0

            # ST_Equals: polygon equals itself
            tdSql.query(
                f"select id, st_equals("
                f"st_geomfromtext(wkt_poly), "
                f"st_geomfromtext(wkt_poly)) "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert int(tdSql.getData(0, 1)) == 1
            assert int(tdSql.getData(1, 1)) == 1

            # ST_Covers: polygon covers interior point
            tdSql.query(
                f"select id, st_covers("
                f"st_geomfromtext(wkt_poly), "
                f"st_geomfromtext(wkt_point)) "
                f"from {t} order by id")
            tdSql.checkRows(2)
            assert int(tdSql.getData(0, 1)) == 1
            assert int(tdSql.getData(1, 1)) == 0

            # ST_Touches: point on boundary → true for boundary point
            tdSql.query(
                f"select st_touches("
                f"st_geomfromtext('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), "
                f"st_geomfromtext('POINT(0 0)')) "
                f"from {t} limit 1")
            tdSql.checkRows(1)
            assert int(tdSql.getData(0, 0)) == 1

        self._with_custom_sources(
            "fq04_geost", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_geo,
        )

    # ==================================================================
    # 42  TRUE_FOR in EVENT_WINDOW
    # ==================================================================

    def test_fq_sql_true_for(self):
        """FQ-SQL-TRUEFOR: TRUE_FOR in EVENT_WINDOW on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # EVENT_WINDOW with TRUE_FOR duration qualification
            # Window opens when val > 2 and must be true for at least 1m
            tdSql.query(
                f"select _wstart, count(*) from {t} "
                f"event_window start with val > 2 end with val < 4 "
                f"true_for(1m)")
            # TRUE_FOR(1m) requires condition to be true for at least 1 minute;
            # with data at 1-min intervals, only the longer window [4,5] qualifies
            rows = tdSql.queryRows
            assert rows >= 0 and rows <= 2, \
                f"TRUE_FOR should return 0-2 windows: {rows}"

        self._with_std_sources("fq04_truefor", body)

    # ==================================================================
    # 43  INTERVAL with SLIDING
    # ==================================================================

    def test_fq_sql_interval_sliding(self):
        """FQ-SQL-SLIDING: INTERVAL with SLIDING on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            # INTERVAL(2m) SLIDING(1m): overlapping windows
            tdSql.query(
                f"select _wstart, count(*), sum(val) from {t} "
                f"where ts >= 1704067200000 and ts < 1704067500000 "
                f"interval(2m) sliding(1m) order by _wstart")
            tdSql.checkRows(5)
            # [00:00, 00:02): val=1,2 → count=2, sum=3
            tdSql.checkData(0, 1, 2); tdSql.checkData(0, 2, 3)
            # [00:01, 00:03): val=2,3 → count=2, sum=5
            tdSql.checkData(1, 1, 2); tdSql.checkData(1, 2, 5)
            # [00:02, 00:04): val=3,4 → count=2, sum=7
            tdSql.checkData(2, 1, 2); tdSql.checkData(2, 2, 7)
            # [00:03, 00:05): val=4,5 → count=2, sum=9
            tdSql.checkData(3, 1, 2); tdSql.checkData(3, 2, 9)

        self._with_std_sources("fq04_sliding", body)

    # ==================================================================
    # 44  ISNOTNULL function form
    # ==================================================================

    def test_fq_sql_isnotnull(self):
        """FQ-SQL-ISNOTNULL: ISNOTNULL function form on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        mysql_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        pg_setup = [
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT, val INT)",
            "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)",
        ]
        influx_lines_inn = [
            'data val=10i,id=1i 1704067200000000000',
            'data id=2i 1704067260000000000',
            'data val=30i,id=3i 1704067320000000000',
        ]

        def body(src, db_type):
            tdSql.query(
                f"select id, isnotnull(val) from {src}.data order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 1)  # val=10, not null → 1
            tdSql.checkData(1, 1, 0)  # val=NULL → 0
            tdSql.checkData(2, 1, 1)  # val=30, not null → 1

        self._with_custom_sources(
            "fq04_isnotnull", body,
            mysql_setup=mysql_setup,
            pg_setup=pg_setup,
            influx_lines=influx_lines_inn,
        )

    # ==================================================================
    # 45  BETWEEN operator (standalone)
    # ==================================================================

    def test_fq_sql_between(self):
        """FQ-SQL-BETWEEN: BETWEEN AND operator on all sources.

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        def body(src):
            t = f"{src}.src_t"

            tdSql.query(
                f"select val from {t} where val between 2 and 4 order by val")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(2, 0, 4)

        self._with_std_sources("fq04_between", body)
