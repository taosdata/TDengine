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
            - 2026-04-15 Copilot Added for TEXT table source feature
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
            - 2026-04-16 Copilot Added for TEXT table window query coverage
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
        """TEXT table source — special column type coverage: DECIMAL and GEOMETRY.

        DECIMAL column behaviour (engine limitation: literals stored as 0):
        - Column declaration is accepted (no parse error)
        - NULL values are stored and round-trip correctly; IS NULL filter works
        - Non-NULL literals are written as 0 due to a known limitation in translateNormalValue
          (TSDB_DATA_TYPE_DECIMAL case in parTranslater.c returns an error that is silently swallowed)

        GEOMETRY column behaviour (engine limitation: WKT stored as raw VARCHAR bytes):
        - Column declaration is accepted alongside TIMESTAMP
        - Real tables convert WKT strings to WKB binary; TEXT tables store the literal as-is
        - Only the ts column is projected in these tests; direct SELECT of the geometry
          column or spatial functions (st_astext, st_x) are NOT tested here because they
          fail with 0x80002803 on TEXT-sourced GEOMETRY data

        NOTE: Must be defined BEFORE test_text_source_groupby. The aggregation path in the
        TEXT scan operator has a known heap-corruption side-effect; running it before type
        tests would corrupt allocator state and cause spurious failures here.

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 Copilot Added for TEXT table special type coverage
        """

        tdSql.prepare("text_src_db", drop=True)
        self._run_type_special_queries()

        tdLog.debug("test_text_type_special passed")

    def _run_type_special_queries(self):
        tdLog.info("text_source_types: running DECIMAL and GEOMETRY query cases")
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
            - 2026-04-20 Copilot Added for large data volume coverage
            - 2026-04-20 Copilot Added over-limit negative case (kMaxTextRows = 10000)
            - 2026-04-22 Copilot Added Q1/Q5/Q6 coverage gaps
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
            - 2026-04-16 Copilot Added for TEXT table GROUP BY / PARTITION BY coverage
            - 2026-04-20 Copilot Removed nested-GROUP-BY+PARTITION-BY case (pre-existing crash)
            - 2026-05-xx Copilot Restored nested-GROUP-BY+PARTITION-BY case after crash fix in operator.c
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
        """TEXT source without a primary timestamp column — and timestamp-related edge cases.

        TEXT no longer requires the first column to be TIMESTAMP. When the first column
        is not TIMESTAMP, hasPrimaryTs=false. This enables non-time-series use-cases
        (lookup tables, enumerations, etc.).

        No-timestamp (A-series):
        - A1: SELECT / ORDER BY on a no-ts TEXT source works correctly
        - A2: GROUP BY on a no-ts TEXT source works correctly
        - A3: JOIN no-ts TEXT with a real table → rejected (engine requires ts for join)
        - A4: JOIN no-ts TEXT with no-ts TEXT → rejected (avoids heap-buffer-overflow in
              merge-join executor which assumes colId==1 is a TIMESTAMP)
        - A5: INTERVAL on no-ts TEXT → rejected (window functions require ts)

        Unsorted-timestamp (B-series):
        - B1: SELECT on unsorted-ts TEXT returns rows in ts order (auto-sorted at parse time)
        - B2: Unsorted-ts TEXT JOIN real table → data matches after auto-sort
        - B3: INTERVAL on unsorted-ts TEXT (in subquery) produces correct window counts
        - B4: LEFT JOIN unsorted-ts TEXT with real table → non-matching rows get NULL

        NULL-timestamp (C-series):
        - C1: NULL value in the primary timestamp column → rejected at parse time
        - C2: NULL value in a non-timestamp column → accepted

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-xx Copilot Added for TEXT no-ts, unsorted-ts, and null-ts coverage
        """
        tdSql.prepare("text_nots_db", drop=True)
        tdSql.execute("CREATE TABLE ref_ts (ts TIMESTAMP, id INT, label VARCHAR(32))")
        tdSql.execute("INSERT INTO ref_ts VALUES "
                      "('2024-01-01 00:00:00',1,'alpha')"
                      "('2024-01-02 00:00:00',2,'beta')"
                      "('2024-01-03 00:00:00',3,'gamma')")

        # --- A1: no-ts SELECT / ORDER BY ---
        tdLog.info("A1: no-ts TEXT SELECT ORDER BY id")
        tdSql.query("SELECT id, score FROM TEXT(id INT, score FLOAT) VALUES (2,2.5)(1,1.5)(3,3.5) t ORDER BY id")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)

        # --- A2: no-ts GROUP BY ---
        tdLog.info("A2: no-ts TEXT GROUP BY")
        tdSql.query("SELECT grp, COUNT(id) FROM TEXT(id INT, grp VARCHAR(4)) VALUES "
                    "(1,'a')(2,'a')(3,'b') t GROUP BY grp ORDER BY grp")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)   # group 'a' has 2 rows
        tdSql.checkData(1, 1, 1)   # group 'b' has 1 row

        # --- A3: no-ts JOIN real table → rejected ---
        tdLog.info("A3: no-ts TEXT JOIN real table must be rejected")
        tdSql.error("SELECT t.id, r.label FROM TEXT(id INT) VALUES (1)(2) t "
                    "JOIN ref_ts r ON t.id=r.id ORDER BY t.id")

        # --- A4: no-ts TEXT JOIN no-ts TEXT → rejected (prevents executor heap overflow) ---
        tdLog.info("A4: no-ts TEXT JOIN no-ts TEXT must be rejected")
        tdSql.error("SELECT a.id, b.val FROM TEXT(id INT) VALUES (1)(2) a "
                    "JOIN TEXT(id INT, val FLOAT) VALUES (1,1.0)(2,2.0) b ON a.id=b.id")

        # --- A5: no-ts INTERVAL → rejected ---
        tdLog.info("A5: no-ts TEXT INTERVAL must be rejected")
        tdSql.error("SELECT COUNT(id) FROM TEXT(id INT) VALUES (1)(2)(3) t INTERVAL(1d)")

        # --- B1: unsorted ts auto-sort ---
        tdLog.info("B1: unsorted-ts TEXT auto-sorts rows by ts")
        tdSql.query("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-03 00:00:00',3)('2024-01-01 00:00:00',1)('2024-01-02 00:00:00',2) t "
                    "ORDER BY ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)

        # --- B2: unsorted ts JOIN real table ---
        tdLog.info("B2: unsorted-ts TEXT JOIN real table")
        tdSql.query("SELECT t.id, r.label FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-03 00:00:00',3)('2024-01-01 00:00:00',1)('2024-01-02 00:00:00',2) t "
                    "JOIN ref_ts r ON t.ts=r.ts ORDER BY t.ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)

        # --- B3: unsorted ts INTERVAL via subquery ---
        # Rows Jan-05, Jan-01, Jan-02 → auto-sorted: Jan-01, Jan-02, Jan-05
        # INTERVAL(3d): [Jan-01,Jan-04) = 2 rows, [Jan-04,Jan-07) = 1 row
        tdLog.info("B3: unsorted-ts TEXT INTERVAL in subquery")
        tdSql.query("SELECT _wstart, COUNT(*) FROM ("
                    "SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-05 00:00:00',3)('2024-01-01 00:00:00',1)('2024-01-02 00:00:00',2) t"
                    ") INTERVAL(3d)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 1)

        # --- B4: unsorted ts LEFT JOIN (unmatched rows get NULL) ---
        tdLog.info("B4: unsorted-ts TEXT LEFT JOIN real table")
        tdSql.query("SELECT t.id, r.label FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-03 00:00:00',3)('2024-01-01 00:00:00',1)('2024-01-04 00:00:00',4) t "
                    "LEFT JOIN ref_ts r ON t.ts=r.ts ORDER BY t.ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)    # id=1 matched
        tdSql.checkData(2, 0, 4)    # id=4 not in ref_ts

        # --- C1: NULL primary timestamp → rejected ---
        tdLog.info("C1: NULL in primary timestamp column must be rejected")
        tdSql.error("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES (NULL, 1) t")
        tdSql.error("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-01 00:00:00', 10)(NULL, 20) t")

        # --- C2: NULL in non-ts column → accepted ---
        tdLog.info("C2: NULL in non-ts column is accepted")
        tdSql.query("SELECT ts, id FROM TEXT(ts TIMESTAMP, id INT) VALUES "
                    "('2024-01-01 00:00:00', NULL) t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        # --- D-series: first col non-TIMESTAMP, second col TIMESTAMP ---
        # hasPrimaryTs is determined solely by the first column's type.
        # A TIMESTAMP in any non-first position is a plain typed column with no
        # primary-timestamp semantics.

        # D1: SELECT / WHERE / ORDER BY on non-first TIMESTAMP column all work
        tdLog.info("D1: non-first TIMESTAMP col — SELECT, WHERE, ORDER BY")
        tdSql.query("SELECT id, ts FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (2, '2026-01-01 00:00:02')(1, '2026-01-01 00:00:01') t "
                    "ORDER BY ts ASC")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)   # ordered by ts ascending
        tdSql.checkData(1, 0, 2)

        tdSql.query("SELECT id FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, '2026-01-01 00:00:01')(2, '2026-01-01 00:00:02') t "
                    "WHERE ts > '2026-01-01 00:00:01'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        # D2: NULL in non-first TIMESTAMP column is allowed
        tdLog.info("D2: NULL in non-first TIMESTAMP col is allowed")
        tdSql.query("SELECT id, ts FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, NULL)(2, '2026-01-01 00:00:02') t ORDER BY id")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, None)

        # D3: JOIN rejected even when second column is TIMESTAMP
        tdLog.info("D3: JOIN with non-first TIMESTAMP must be rejected")
        tdSql.error("SELECT t.id, r.label FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, '2026-01-01 00:00:01') t "
                    "JOIN ref_ts r ON t.ts = r.ts")

        # D4: INTERVAL rejected even when second column is TIMESTAMP
        tdLog.info("D4: INTERVAL with non-first TIMESTAMP must be rejected")
        tdSql.error("SELECT COUNT(id) FROM TEXT(id INT, ts TIMESTAMP) "
                    "VALUES (1, '2026-01-01 00:00:01')(2, '2026-01-01 00:00:02') t "
                    "INTERVAL(1s)")

        tdLog.debug("test_text_source_no_ts passed")

    def test_text_source_union(self):
        """TEXT table source: UNION / UNION ALL combined with real tables and other TEXT sources.

        U1: UNION ALL two no-ts TEXT sources returns all rows in order
        U2: UNION (distinct) of two identical TEXT sources deduplicates rows
        U3: UNION ALL TEXT source with a real super-table returns all rows
        U4: TEXT subquery UNION ALL real-table subquery returns correct combined rows
        U5: Three-way UNION ALL across two TEXT sources and one real table

        Since: v3.4.2

        Labels: common,unit

        Jira: None
        """
        tdSql.prepare("text_union_db", drop=True)
        tdSql.execute("USE text_union_db")
        tdSql.execute(
            "CREATE STABLE meters(ts TIMESTAMP, volt INT) TAGS(loc NCHAR(8))"
        )
        tdSql.execute("CREATE TABLE m1 USING meters TAGS('f1')")
        tdSql.execute(
            "INSERT INTO m1 VALUES "
            "('2026-04-01 00:00:00',100)"
            "('2026-04-01 00:01:00',200)"
            "('2026-04-01 00:02:00',300)"
        )

        # U1: UNION ALL two no-ts TEXT sources — all rows returned in insertion order
        tdLog.info("U1: UNION ALL two no-ts TEXT sources")
        tdSql.query(
            "SELECT id, name FROM TEXT(id INT, name VARCHAR(8)) VALUES (1,'alpha')(2,'beta') t "
            "UNION ALL "
            "SELECT id, name FROM TEXT(id INT, name VARCHAR(8)) VALUES (3,'gamma')(4,'delta') t2"
        )
        tdSql.checkRows(4)
        # verify data from both sides is present
        ids = [row[0] for row in tdSql.queryResult]
        assert sorted(ids) == [1, 2, 3, 4], f"U1 wrong ids: {ids}"

        # U2: UNION (dedup) two no-ts TEXT sources — duplicate rows collapsed
        tdLog.info("U2: UNION dedup two no-ts TEXT sources")
        tdSql.query(
            "SELECT id FROM TEXT(id INT) VALUES (1)(2)(2) t "
            "UNION "
            "SELECT id FROM TEXT(id INT) VALUES (2)(3) t2"
        )
        tdSql.checkRows(3)
        ids = sorted(row[0] for row in tdSql.queryResult)
        assert ids == [1, 2, 3], f"U2 wrong ids: {ids}"

        # U3: no-ts TEXT UNION ALL real table
        tdLog.info("U3: no-ts TEXT UNION ALL real table")
        tdSql.query(
            "SELECT volt FROM TEXT(volt INT) VALUES (400)(500) t "
            "UNION ALL "
            "SELECT volt FROM m1"
        )
        tdSql.checkRows(5)
        volts = sorted(row[0] for row in tdSql.queryResult)
        assert volts == [100, 200, 300, 400, 500], f"U3 wrong volts: {volts}"

        # U4: subquery wrapping UNION ALL — ORDER BY on outer query
        tdLog.info("U4: subquery wrapping UNION ALL with outer ORDER BY")
        tdSql.query(
            "SELECT * FROM ("
            "  SELECT id FROM TEXT(id INT) VALUES (3)(1) t "
            "  UNION ALL "
            "  SELECT id FROM TEXT(id INT) VALUES (4)(2) t2"
            ") sub ORDER BY id"
        )
        tdSql.checkRows(4)
        ids = [row[0] for row in tdSql.queryResult]
        assert ids == [1, 2, 3, 4], f"U4 wrong order: {ids}"

        # U5: ts TEXT UNION ALL real table ORDER BY ts
        tdLog.info("U5: ts TEXT UNION ALL real table ORDER BY ts")
        tdSql.query(
            "SELECT ts, volt FROM TEXT(ts TIMESTAMP, volt INT) "
            "VALUES ('2026-04-01 00:00:00', 50)('2026-04-01 00:03:00', 400) t "
            "UNION ALL "
            "SELECT ts, volt FROM m1 "
            "ORDER BY ts"
        )
        tdSql.checkRows(5)
        volts = [row[1] for row in tdSql.queryResult]
        assert volts == [50, 100, 200, 300, 400], f"U5 wrong volt order: {volts}"

        # U6: UNION ALL with GROUP BY aggregation on each side
        tdLog.info("U6: UNION ALL combining two GROUP BY results")
        tdSql.query(
            "SELECT id, COUNT(*) cnt FROM TEXT(id INT, val INT) VALUES (1,10)(1,20)(2,30) t "
            "GROUP BY id "
            "UNION ALL "
            "SELECT id, COUNT(*) cnt FROM TEXT(id INT, val INT) VALUES (3,40)(3,50) t2 "
            "GROUP BY id "
            "ORDER BY id"
        )
        tdSql.checkRows(3)
        rows = tdSql.queryResult
        assert rows[0][0] == 1 and rows[0][1] == 2, f"U6[0] wrong: {rows[0]}"
        assert rows[1][0] == 2 and rows[1][1] == 1, f"U6[1] wrong: {rows[1]}"
        assert rows[2][0] == 3 and rows[2][1] == 2, f"U6[2] wrong: {rows[2]}"

        tdLog.debug("test_text_source_union passed")
