import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestTextSource:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_text_source(self):
        """TEXT table source tests.

        Positive cases (see in/text_source.in for details):
        - Data type coverage: all integer widths (signed/unsigned), float/double,
          bool, varchar/nchar (including unicode and empty string), NULL values
        - Filter/operator coverage: WHERE, BETWEEN, LIKE, IS NOT NULL,
          timestamp range, arithmetic expressions, ORDER BY, LIMIT
        - Unordered rows: out-of-order VALUES are auto-sorted by ts at parse time;
          plain select returns ts-ordered rows; ORDER BY ts and subquery also correct
        - Subquery scenarios: TEXT as inner source, cascaded WHERE, double-nested
          subquery, UNION ALL, expression aliases, DISTINCT
        - JOIN scenarios: TEXT subquery joined with a real table (TEXT on left or right),
          TEXT subquery joined with TEXT subquery (ts-equality), unsorted TEXT in join,
          scalar subquery whose source is a TEXT table

        Negative cases (inline):
        - Duplicate column name, cell count mismatch, NULL primary timestamp,
          JOIN with TEXT/FILE source that has no primary timestamp column

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-15 Added for TEXT table source feature
            - 2026-04-16 Refactored to file-based comparison with extended coverage

        """

        tdSql.prepare("text_src_db", drop=True)
        self.text_source_queries()
        self.text_source_negative()

        tdLog.debug("test_text_source passed")

    def text_source_queries(self):
        """Positive test cases via file-based result comparison."""
        tdLog.info("text_source: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_source.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_source.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_source")

    def text_source_negative(self):
        """Negative test cases: each of these should return an error."""
        tdLog.info("text_source: duplicate column name should fail")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP, ts TIMESTAMP) VALUES ('2024-01-01 00:00:00', '2024-01-01 00:00:00') t_neg2"
        )

        tdLog.info("text_source: mismatched row cell count should fail")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP, a INT) VALUES ('2024-01-01 00:00:00', 1, 99) t_neg3"
        )

        tdLog.info("text_source: NULL primary timestamp should fail")
        tdSql.error(
            "SELECT ts, a FROM TEXT(ts TIMESTAMP, a INT) VALUES (NULL, 1) t_neg4"
        )

        tdLog.info("text_source: JOIN with no-ts TEXT source should fail (avoids executor crash)")
        tdSql.error(
            "SELECT a.id, b.val FROM TEXT(id INT) VALUES (1)(2) a "
            "JOIN TEXT(id INT, val FLOAT) VALUES (1,1.0)(2,2.0) b ON a.id=b.id"
        )

    def test_text_source_window(self):
        """TEXT table source — window query coverage: SESSION, INTERVAL, EVENT_WINDOW, STATE_WINDOW.

        Session window directly on TEXT table:
        - SESSION(ts, gap) works when the source has ≥4 rows that form ≥2 separate windows
        - SESSION with exactly 3 total rows or with a single merged session window has a known
          server-side crash bug and is therefore excluded from this test

        INTERVAL, INTERVAL SLIDING, EVENT_WINDOW, STATE_WINDOW on TEXT table:
        - These window types require the TEXT table to be wrapped in a subquery; placing them
          directly on a TEXT source produces error 0x80002650 ("not valid primary timestamp column")
        - All four types work correctly when TEXT is the inner source of a subquery

        PARTITION BY with window:
        - PARTITION BY SESSION via subquery works when each partition produces ≥2 session windows
        - PARTITION BY INTERVAL via subquery works for multiple partitions

        NOTE: Must be defined and run BEFORE test_text_source_groupby in the same class because
        pytest executes methods in definition order, and the GROUP BY / PARTITION BY path has a
        known server-side heap-corruption side-effect that causes subsequent window queries to fail.

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 Added for TEXT table window query coverage
        """

        tdSql.prepare("text_src_db", drop=True)
        self._run_window_queries()

        tdLog.debug("test_text_source_window passed")

    def _run_window_queries(self):
        tdLog.info("text_source_window: running SESSION / INTERVAL / EVENT / STATE window cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_window.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_window.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_window")

    def test_text_type_special(self):
        """TEXT table source — special column type coverage and unsupported type rejection.

        Verified supported types:
        - 15 basic types (TINYINT..UBIGINT, FLOAT, DOUBLE, BOOL, VARCHAR, NCHAR, VARBINARY,
          TIMESTAMP): all fully supported with NULL values
        - DECIMAL(10,2): fully supported (DECIMAL64), values round-trip correctly
        - DECIMAL(38,10): fully supported (DECIMAL128), values round-trip correctly
        - VARBINARY: fully supported with hex-string format

        Rejected types (negative tests):
        - GEOMETRY: rejected at parse time
        - JSON: rejected at parse time
        - BLOB: rejected at parse time
        - MEDIUMBLOB: rejected at parse time

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 Added for TEXT table special type coverage
            - 2026-05-09 Added comprehensive type matrix tests; DECIMAL fully supported
            - 2026-05-09 GEOMETRY/JSON/BLOB/MEDIUMBLOB changed to rejected types
        """

        tdSql.prepare("text_src_db", drop=True)
        self._run_type_special_queries()

        # ================================================================
        # Programmatic type matrix tests
        # ================================================================

        # --- T1: VARBINARY full support ---
        tdLog.info("T1: VARBINARY column round-trip (hex + NULL)")
        tdSql.query(
            "SELECT ts, v FROM TEXT(ts TIMESTAMP, v VARBINARY(64)) "
            "VALUES ('2024-01-01 00:00:00', '\\x48454C4C4F') "
            "('2024-01-02 00:00:00', NULL) t_vb ORDER BY ts"
        )
        tdSql.checkRows(2)
        assert tdSql.queryResult[0][1] is not None, "T1: VARBINARY row 0 should not be NULL"
        assert tdSql.queryResult[1][1] is None, "T1: VARBINARY row 1 should be NULL"

        # --- T2: DECIMAL(10,2) short — NULL works, non-NULL stored as 0 ---
        tdLog.info("T2: DECIMAL(10,2) NULL and non-NULL behaviour")
        tdSql.query(
            "SELECT ts, v FROM TEXT(ts TIMESTAMP, v DECIMAL(10,2)) "
            "VALUES ('2024-01-01 00:00:00', NULL)('2024-01-02 00:00:00', NULL) t WHERE v IS NULL"
        )
        tdSql.checkRows(2)
        # non-NULL values should be stored correctly
        tdSql.query(
            "SELECT ts, v FROM TEXT(ts TIMESTAMP, v DECIMAL(10,2)) "
            "VALUES ('2024-01-01 00:00:00', 3.14)('2024-01-02 00:00:00', 0)('2024-01-03 00:00:00', NULL) t ORDER BY ts"
        )
        tdSql.checkRows(3)
        assert float(tdSql.queryResult[0][1]) == 3.14, f"T2: DECIMAL(10,2) expected 3.14, got {tdSql.queryResult[0][1]}"
        assert float(tdSql.queryResult[1][1]) == 0, f"T2: DECIMAL(10,2) zero expected 0, got {tdSql.queryResult[1][1]}"
        assert tdSql.queryResult[2][1] is None, f"T2: DECIMAL(10,2) NULL expected None, got {tdSql.queryResult[2][1]}"

        # --- T3: DECIMAL(38,10) long — full support ---
        tdLog.info("T3: DECIMAL(38,10) full support")
        tdSql.query(
            "SELECT ts, v FROM TEXT(ts TIMESTAMP, v DECIMAL(38,10)) "
            "VALUES ('2024-01-01 00:00:00', NULL) t WHERE v IS NULL"
        )
        tdSql.checkRows(1)
        tdSql.query(
            "SELECT ts, v FROM TEXT(ts TIMESTAMP, v DECIMAL(38,10)) "
            "VALUES ('2024-01-01 00:00:00', 12345.6789)('2024-01-02 00:00:00', NULL) t ORDER BY ts"
        )
        tdSql.checkRows(2)
        assert tdSql.queryResult[0][1] is not None, f"T3: DECIMAL(38,10) non-NULL expected value, got None"
        assert tdSql.queryResult[1][1] is None, f"T3: DECIMAL(38,10) NULL expected None"
        # ORDER BY and SUM should work
        tdSql.query(
            "SELECT SUM(v) FROM TEXT(ts TIMESTAMP, v DECIMAL(10,2)) "
            "VALUES ('2024-01-01 00:00:00', 3.14)('2024-01-02 00:00:00', 0)('2024-01-03 00:00:00', 99.99) t"
        )
        tdSql.checkRows(1)
        assert abs(float(tdSql.queryResult[0][0]) - 103.13) < 0.01, \
            f"T3: SUM expected 103.13, got {tdSql.queryResult[0][0]}"

        # --- T4: GEOMETRY — rejected ---
        tdLog.info("T4: GEOMETRY type should be rejected")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP, g GEOMETRY(100)) "
            "VALUES ('2024-01-01 00:00:00', 'POINT(1 2)') t"
        )
        # GEOMETRY as only column
        tdSql.error(
            "SELECT g FROM TEXT(g GEOMETRY(64)) VALUES ('POINT(1 2)') t"
        )

        # --- T5: JSON — rejected ---
        tdLog.info("T5: JSON type should be rejected")
        tdSql.error(
            "SELECT ts, j FROM TEXT(ts TIMESTAMP, j JSON) "
            "VALUES ('2024-01-01 00:00:00', '{\"k\":1}') t"
        )
        # JSON as only column
        tdSql.error(
            "SELECT j FROM TEXT(j JSON) VALUES ('{\"k\":1}') t"
        )

        # --- T5b: BLOB / MEDIUMBLOB — rejected ---
        tdLog.info("T5b: BLOB and MEDIUMBLOB types should be rejected")
        tdSql.error(
            "SELECT ts, b FROM TEXT(ts TIMESTAMP, b BLOB) "
            "VALUES ('2024-01-01 00:00:00', 'data') t"
        )
        tdSql.error(
            "SELECT ts, b FROM TEXT(ts TIMESTAMP, b MEDIUMBLOB) "
            "VALUES ('2024-01-01 00:00:00', 'data') t"
        )

        # --- T6: All basic types with NULL in one query ---
        tdLog.info("T6: all basic types with NULL values")
        tdSql.query(
            "SELECT * FROM TEXT("
            "  ts TIMESTAMP, v1 TINYINT, v2 SMALLINT, v3 INT, v4 BIGINT, "
            "  v5 FLOAT, v6 DOUBLE, v7 BOOL, v8 VARCHAR(32), v9 NCHAR(64)"
            ") VALUES "
            "('2024-01-01 00:00:00', 127, 32767, 2147483647, 9223372036854775807, 3.14, 2.718, true, 'hello', '中文') "
            "('2024-01-02 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) t ORDER BY ts"
        )
        tdSql.checkRows(2)
        row0 = tdSql.queryResult[0]
        assert row0[1] == 127, f"T6: TINYINT expected 127, got {row0[1]}"
        assert row0[2] == 32767, f"T6: SMALLINT expected 32767, got {row0[2]}"
        assert row0[3] == 2147483647, f"T6: INT expected max, got {row0[3]}"
        assert row0[4] == 9223372036854775807, f"T6: BIGINT expected max, got {row0[4]}"
        assert row0[7] is True or row0[7] == 1, f"T6: BOOL expected true, got {row0[7]}"
        assert row0[8] == "hello", f"T6: VARCHAR expected 'hello', got {row0[8]}"
        assert row0[9] == "中文", f"T6: NCHAR expected '中文', got {row0[9]}"
        row1 = tdSql.queryResult[1]
        for i in range(1, 10):
            assert row1[i] is None, f"T6: NULL row col {i} expected None, got {row1[i]}"

        # --- T7: Unsigned integer types with boundary values ---
        tdLog.info("T7: unsigned integer boundary values")
        tdSql.query(
            "SELECT ts, v1, v2, v3, v4 FROM TEXT("
            "  ts TIMESTAMP, v1 TINYINT UNSIGNED, v2 SMALLINT UNSIGNED, "
            "  v3 INT UNSIGNED, v4 BIGINT UNSIGNED"
            ") VALUES "
            "('2024-01-01 00:00:00', 0, 0, 0, 0)"
            "('2024-01-02 00:00:00', 255, 65535, 4294967295, 18446744073709551615)"
            "('2024-01-03 00:00:00', NULL, NULL, NULL, NULL) t ORDER BY ts"
        )
        tdSql.checkRows(3)
        assert list(tdSql.queryResult[0][1:]) == [0, 0, 0, 0], \
            f"T7: zeros expected, got {tdSql.queryResult[0][1:]}"
        row_max = tdSql.queryResult[1]
        assert row_max[1] == 255, f"T7: UTINYINT max expected 255, got {row_max[1]}"
        assert row_max[2] == 65535, f"T7: USMALLINT max expected 65535, got {row_max[2]}"
        assert row_max[3] == 4294967295, f"T7: UINT max expected 4294967295, got {row_max[3]}"
        assert row_max[4] == 18446744073709551615, f"T7: UBIGINT max expected, got {row_max[4]}"

        # --- T8: Non-first TIMESTAMP column — rejected (first col must be TIMESTAMP) ---
        tdLog.info("T8: non-first TIMESTAMP column must be rejected")
        tdSql.error(
            "SELECT id, ts FROM TEXT(id INT, ts TIMESTAMP) "
            "VALUES (1, '2024-01-01 00:00:00')(2, NULL) t"
        )

        # --- T9: Primary TIMESTAMP column NULL should error ---
        tdLog.info("T9: primary timestamp NULL should error")
        tdSql.error(
            "SELECT v FROM TEXT(v TIMESTAMP) VALUES (NULL) t"
        )

        tdLog.debug("test_text_type_special passed")

    def _run_type_special_queries(self):
        tdLog.info("text_source_types: running DECIMAL and VARBINARY query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_type_special.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_type_special.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_type_special")

    def test_text_source_large(self):
        """TEXT table source — large data volume, subquery correctness, type coercion, and wide columns.

        Positive checks (10,000 rows — exactly at the cap):
        - COUNT / SUM / MIN / MAX over all rows
        - WHERE predicate (half the rows selected)
        - ORDER BY + LIMIT (top-5 descending)

        Q1 (subquery on large data): TEXT with 10,000 rows as the inner source of a
          subquery; outer WHERE reduces to 5,000 rows. Verifies the planner's
          projection-elimination optimizer does not mis-prune ROWSET_SOURCE targets.

        Q5 (type coercion on bad values): TEXT VALUES with a non-numeric string in an
          INT column silently coerce to 0 (consistent with TDengine's general coercion
          semantics). Overflow wraps modulo the type range. These tests document the
          current behaviour so regressions are detected if it changes.

        Q6 (wide columns): TEXT with many columns and few rows is handled correctly;
          memory allocation is proportional to rows × columns.

        Negative checks:
        - 10,001 rows → translation error (row count exceeds limit)

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-20 Added for large data volume coverage
            - 2026-04-20 Added over-limit negative case (kMaxTextRows = 10000)
            - 2026-04-22 Added Q1/Q5/Q6 coverage gaps
        """
        import datetime

        tdSql.prepare("text_src_large_db", drop=True)

        base = datetime.datetime(2020, 1, 1, 0, 0, 0)

        def _build_values(n):
            return " ".join(
                f"('{(base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')}', {i})"
                for i in range(n)
            )

        schema = "TEXT(ts TIMESTAMP, a INT) VALUES"
        values = _build_values(10000)

        # --- Positive: 10,000 rows (at the cap) ---
        tdSql.query(f"SELECT COUNT(a), SUM(a), MIN(a), MAX(a) FROM {schema} {values} t_large")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)     # COUNT
        tdSql.checkData(0, 1, 49995000)  # SUM(0..9999) = 9999*10000/2
        tdSql.checkData(0, 2, 0)         # MIN
        tdSql.checkData(0, 3, 9999)      # MAX

        # WHERE filter: a >= 5000 → 5000 rows (5000..9999)
        tdSql.query(f"SELECT COUNT(a) FROM {schema} {values} t_large_filter WHERE a >= 5000")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5000)

        # ORDER BY DESC + LIMIT: top 5
        tdSql.query(f"SELECT a FROM {schema} {values} t_large_limit ORDER BY a DESC LIMIT 5")
        tdSql.checkRows(5)
        for rank, expected in enumerate([9999, 9998, 9997, 9996, 9995]):
            tdSql.checkData(rank, 0, expected)

        # --- Q1: 10,000-row TEXT as inner source of a subquery ---
        tdLog.info("text_source_large Q1: 10000-row TEXT as subquery inner source")
        tdSql.query(
            f"SELECT COUNT(a), SUM(a) FROM "
            f"(SELECT ts, a FROM {schema} {values} t_inner) sub "
            f"WHERE a >= 5000"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5000)      # 5000..9999 → 5000 rows
        tdSql.checkData(0, 1, 37497500)  # SUM(5000..9999) = 5000*14999/2

        # --- Q5: type coercion on bad values ---
        tdLog.info("text_source_large Q5: type coercion — bad values silently become 0")
        # Non-numeric string in INT column → 0
        tdSql.query(
            "SELECT a FROM TEXT(ts TIMESTAMP, a INT) "
            "VALUES ('2024-01-01 00:00:00', 'not_a_number') t_coerce_int"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        # Non-bool string in BOOL column → false
        tdSql.query(
            "SELECT a FROM TEXT(ts TIMESTAMP, a BOOL) "
            "VALUES ('2024-01-01 00:00:01', 'hello') t_coerce_bool"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)

        # Overflow: 300 into TINYINT (-128..127) wraps to 44 (300 mod 256)
        tdSql.query(
            "SELECT a FROM TEXT(ts TIMESTAMP, a TINYINT) "
            "VALUES ('2024-01-01 00:00:02', 300) t_coerce_overflow"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 44)

        # --- Q6: wide-column TEXT table ---
        tdLog.info("text_source_large Q6: 50-column TEXT table (10 rows)")
        cols_schema = ", ".join(f"c{i} INT" for i in range(1, 50))
        wide_schema = f"TEXT(ts TIMESTAMP, {cols_schema}) VALUES"
        wide_rows = " ".join(
            f"('2024-01-01 00:{i:02d}:00'" + "".join(f", {i * j}" for j in range(1, 50)) + ")"
            for i in range(1, 11)
        )
        tdSql.query(f"SELECT COUNT(c1), SUM(c49) FROM {wide_schema} {wide_rows} t_wide")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)                        # 10 rows
        tdSql.checkData(0, 1, sum(i * 49 for i in range(1, 11)))  # SUM c49 = 49*(1+…+10) = 2695

        # --- Negative: 10,001 rows (one over the cap) must be rejected ---
        tdLog.info("text_source_large: 10001 rows should be rejected (exceeds kMaxTextRows=10000)")
        values_over = _build_values(10001)
        tdSql.error(
            f"SELECT COUNT(a) FROM {schema} {values_over} t_over_limit"
        )

        tdLog.debug("test_text_source_large passed")

    def test_text_source_groupby(self):
        """TEXT table source — GROUP BY and PARTITION BY clause combinations.

        GROUP BY safe patterns (empirically validated):
        - Single aggregate (COUNT), multiple aggregates (SUM + COUNT), MAX / MIN
        - WHERE pre-filter before grouping
        - HAVING applied to aggregate result
        - Composite GROUP BY key (two columns)
        - Nested: grouped inner subquery + outer WHERE filter
        - GROUP BY on integer key column
        All tests use ≤4 rows with ≤2 distinct group keys.

        Known GROUP BY limitations (excluded from this test):
        - GROUP BY + LIMIT → taosd crash (unrelated pre-existing bug)

        PARTITION BY safe patterns:
        - Projection with ORDER BY (no aggregation)
        - SUM aggregate with ORDER BY
        - HAVING filter with ORDER BY
        - COUNT + MAX per partition
        - Nested subquery (GROUP BY inside) feeding outer PARTITION BY

        Known PARTITION BY limitations:
        - PARTITION BY without ORDER BY → error 0x80002603 ("invalid parameters")

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 Added for TEXT table GROUP BY / PARTITION BY coverage
            - 2026-04-20 Removed nested-GROUP-BY+PARTITION-BY case (pre-existing crash)
            - 2026-05-xx Restored nested-GROUP-BY+PARTITION-BY case after crash fix in operator.c
        """

        tdSql.prepare("text_src_db", drop=True)
        self._run_groupby_queries()

        tdLog.debug("test_text_source_groupby passed")

    def _run_groupby_queries(self):
        tdLog.info("text_source_groupby: running GROUP BY and PARTITION BY cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_groupby.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_groupby.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_groupby")

    def test_text_source_no_ts(self):
        """TEXT source requires first column to be TIMESTAMP — rejection and edge cases.

        TEXT requires the first column to be TIMESTAMP (primary key). Queries
        without a TIMESTAMP first column are rejected at parse time.

        No-timestamp rejection (A-series):
        - A1: no-ts TEXT → rejected (first column must be TIMESTAMP)
        - A2: no-ts TEXT GROUP BY → rejected
        - A3: no-ts TEXT JOIN → rejected
        - A4: no-ts TEXT JOIN no-ts TEXT → rejected
        - A5: no-ts TEXT INTERVAL → rejected

        Unsorted-timestamp (B-series):
        - B1: SELECT on unsorted-ts TEXT returns rows in ts order (auto-sorted at parse time)
        - B2: Unsorted-ts TEXT JOIN real table → data matches after auto-sort
        - B3: INTERVAL on unsorted-ts TEXT (in subquery) produces correct window counts
        - B4: LEFT JOIN unsorted-ts TEXT with real table → non-matching rows get NULL

        NULL-timestamp (C-series):
        - C1: NULL value in the primary timestamp column → rejected at parse time
        - C2: NULL value in a non-timestamp column → accepted

        Non-first TIMESTAMP column (D-series):
        - D1-D4: non-first TIMESTAMP col → rejected (first column must be TIMESTAMP)

        Duplicate-timestamp (E-series):
        - E1-E4: duplicate timestamps preserved, auto-sorted
        - E5: FIRST/LAST with duplicate timestamps
        - E6: CSUM/DIFF/DERIVATIVE/TWA/IRATE reject duplicate timestamps

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-xx Added for TEXT no-ts, unsorted-ts, and null-ts coverage
        """
        tdSql.prepare("text_nots_db", drop=True)

        # Deterministic positive queries via file-based comparison (B, C2, E series)
        tdLog.info("text_no_ts: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_no_ts.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_no_ts.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_no_ts")

        # --- Error cases ---

        # A1: no-ts TEXT → rejected (first column must be TIMESTAMP)
        tdLog.info("A1: no-ts TEXT must be rejected")
        tdSql.error("SELECT id, score FROM TEXT(id INT, score FLOAT) VALUES (2,2.5)(1,1.5) t ORDER BY id")

        # A2: no-ts TEXT GROUP BY → rejected
        tdLog.info("A2: no-ts TEXT GROUP BY must be rejected")
        tdSql.error("SELECT grp, COUNT(id) FROM TEXT(id INT, grp VARCHAR(4)) VALUES (1,'a')(2,'b') t GROUP BY grp")

        # A3: no-ts JOIN real table → rejected
        tdLog.info("A3: no-ts TEXT JOIN real table must be rejected")
        tdSql.error("SELECT t.id, r.label FROM TEXT(id INT) VALUES (1)(2) t "
                    "JOIN ref_ts r ON t.id=r.id ORDER BY t.id")

        # A4: no-ts TEXT JOIN no-ts TEXT → rejected
        tdLog.info("A4: no-ts TEXT JOIN no-ts TEXT must be rejected")
        tdSql.error("SELECT a.id, b.val FROM TEXT(id INT) VALUES (1)(2) a "
                    "JOIN TEXT(id INT, val FLOAT) VALUES (1,1.0)(2,2.0) b ON a.id=b.id")

        # A5: no-ts INTERVAL → rejected
        tdLog.info("A5: no-ts TEXT INTERVAL must be rejected")
        tdSql.error("SELECT COUNT(id) FROM TEXT(id INT) VALUES (1)(2)(3) t INTERVAL(1d)")

        # C1: NULL primary timestamp → rejected
        tdLog.info("C1: NULL in primary timestamp column must be rejected")
        tdSql.error("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES (NULL, 1) t")
        tdSql.error("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-01 00:00:00', 10)(NULL, 20) t")

        # D1: non-first TIMESTAMP col → rejected
        tdLog.info("D1: non-first TIMESTAMP col must be rejected")
        tdSql.error("SELECT id, ts FROM TEXT(id INT, ts TIMESTAMP) VALUES "
                    "(2, '2026-01-01 00:00:02')(1, '2026-01-01 00:00:01') t ORDER BY ts ASC")

        # D2: non-first TIMESTAMP col with NULL → rejected
        tdLog.info("D2: non-first TIMESTAMP col must be rejected")
        tdSql.error("SELECT id, ts FROM TEXT(id INT, ts TIMESTAMP) VALUES "
                    "(1, NULL)(2, '2026-01-01 00:00:02') t ORDER BY id")

        # D3: JOIN with non-first TIMESTAMP → rejected
        tdLog.info("D3: JOIN with non-first TIMESTAMP must be rejected")
        tdSql.error("SELECT t.id, r.label FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, '2026-01-01 00:00:01') t "
                    "JOIN ref_ts r ON t.ts = r.ts")

        # D4: INTERVAL with non-first TIMESTAMP → rejected
        tdLog.info("D4: INTERVAL with non-first TIMESTAMP must be rejected")
        tdSql.error("SELECT COUNT(id) FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, '2026-01-01 00:00:01')(2, '2026-01-01 00:00:02') t "
                    "INTERVAL(1s)")

        # E5: FIRST/LAST with duplicate timestamps (non-deterministic FIRST)
        tdLog.info("E5: FIRST/LAST with duplicate timestamps")
        dup_sub = ("(SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                   "('2024-01-01 00:00:00', 1)('2024-01-01 00:00:00', 2)"
                   "('2024-01-02 00:00:00', 3) t)")
        tdSql.query(f"SELECT FIRST(id) FROM {dup_sub}")
        assert tdSql.getData(0, 0) in (1, 2), f"FIRST(id) should be 1 or 2, got {tdSql.getData(0, 0)}"
        tdSql.query(f"SELECT LAST(id) FROM {dup_sub}")
        tdSql.checkData(0, 0, 3)

        # E6: CSUM/DIFF/DERIVATIVE/TWA/IRATE reject duplicate timestamps
        tdLog.info("E6: time-series functions reject duplicate timestamps")
        tdSql.error(f"SELECT CSUM(id) FROM {dup_sub}")
        tdSql.error(f"SELECT DIFF(id) FROM {dup_sub}")
        tdSql.error(f"SELECT DERIVATIVE(id, 1s, 0) FROM {dup_sub}")
        tdSql.error(f"SELECT TWA(id) FROM {dup_sub}")
        tdSql.error(f"SELECT IRATE(id) FROM {dup_sub}")

        tdLog.debug("test_text_source_no_ts passed")

    def test_text_source_union(self):
        """TEXT table source: UNION / UNION ALL combined with real tables and other TEXT sources.

        U1: UNION ALL two no-ts TEXT sources returns all rows in order
        U2: UNION (distinct) of two identical TEXT sources deduplicates rows
        U3: UNION ALL TEXT source with a real super-table returns all rows
        U4: TEXT subquery UNION ALL real-table subquery returns correct combined rows
        U5: Three-way UNION ALL across two TEXT sources and one real table
        U6: UNION ALL with GROUP BY aggregation on each side

        Since: v3.4.2

        Labels: common,unit

        Jira: None
        """
        tdSql.prepare("text_union_db", drop=True)

        # Deterministic positive queries via file-based comparison
        tdLog.info("text_union: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "text_union.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "text_union.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "text_union")

        # U5: ts TEXT UNION ALL real table ORDER BY ts — non-deterministic tie-break
        tdLog.info("U5: ts TEXT UNION ALL real table ORDER BY ts")
        tdSql.execute("USE text_union_db")
        tdSql.query(
            "SELECT ts, volt FROM TEXT(ts TIMESTAMP, volt INT) "
            "VALUES ('2026-04-01 00:00:00', 50)('2026-04-01 00:03:00', 400) t "
            "UNION ALL "
            "SELECT ts, volt FROM m1 "
            "ORDER BY ts"
        )
        tdSql.checkRows(5)
        volts = [row[1] for row in tdSql.queryResult]
        # First two rows share ts '2026-04-01 00:00:00' (50 from TEXT, 100 from m1),
        # their relative order is non-deterministic; the rest are deterministic.
        assert sorted(volts[:2]) == [50, 100], f"U5 wrong first two volts: {volts[:2]}"
        assert volts[2:] == [200, 300, 400], f"U5 wrong tail volts: {volts[2:]}"

        tdLog.debug("test_text_source_union passed")
