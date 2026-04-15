import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestTextSource:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_text_source(self):
        """TEXT table source tests (first column must be TIMESTAMP)

        Positive cases (via file-based comparison in in/text_source.in):
        --- Data type coverage ---
        1.  Basic SELECT: TIMESTAMP + BIGINT, 2 rows
        2.  Multiple data types: INT, DOUBLE, BOOL, VARCHAR in one row
        3.  Single-column TIMESTAMP table (3 rows)
        4.  NULL value in a non-ts INT column
        5.  TEXT table as subquery source (SELECT * outer)
        6.  TEXT table without explicit alias (auto-generated alias)
        7.  WHERE clause filtering on INT column
        8.  Signed integer boundary values: TINYINT, SMALLINT, INT, BIGINT max
        9.  Floating-point types: FLOAT and DOUBLE (positive, negative, zero)
        10. String types: VARCHAR and NCHAR in same table
        11. BOOL column with NULL in third row
        12. ORDER BY non-timestamp column DESC
        13. LIMIT clause (first 3 of 5 rows)
        14. Multi-column NULL: INT NULL and VARCHAR NULL in different rows
        15. Nested subquery with WHERE on inner TEXT table
        16. Unsigned integer boundary values: TINYINT/SMALLINT/INT/BIGINT UNSIGNED max
        17. Negative and zero boundary values for signed INT and BIGINT
        18. NCHAR with unicode (CJK) characters
        19. VARCHAR with empty string vs non-empty
        --- Filter / operator coverage ---
        20. WHERE timestamp range (>= AND <=)
        21. WHERE IS NOT NULL filter
        22. WHERE BETWEEN filter
        23. WHERE LIKE filter on VARCHAR
        24. Arithmetic expression columns (multiply, add)
        --- Subquery scenario coverage ---
        25. Subquery projects an expression; outer selects the alias with ORDER BY
        26. Cascaded WHERE: inner TEXT WHERE a>1, outer WHERE a<5
        27. Double-nested subquery, each level applies its own WHERE
        28. UNION ALL of two TEXT tables, result ordered by ts
        29. Column alias in inner TEXT query, outer selects alias with ORDER BY
        30. DISTINCT inside subquery, outer ORDER BY for deterministic output

        Negative cases (inline error assertions):
        31. Error: first column is not TIMESTAMP
        32. Error: duplicate column name in TEXT definition
        33. Error: row cell count does not match column count
        34. Error: rows not in ascending timestamp order

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-15 Copilot Added for TEXT table source feature
            - 2026-04-16 Refactored to use file-based comparison + added type/op coverage
            - 2026-04-16 Expanded to 30 positive cases: full type coverage + subquery scenarios

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
        tdLog.info("text_source: non-TIMESTAMP first column should fail")
        tdSql.error(
            "SELECT a FROM TEXT(a BIGINT, b VARCHAR(20)) VALUES (1, 'hello') (2, 'world') t_neg1"
        )

        tdLog.info("text_source: duplicate column name should fail")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP, ts TIMESTAMP) VALUES ('2024-01-01 00:00:00', '2024-01-01 00:00:00') t_neg2"
        )

        tdLog.info("text_source: mismatched row cell count should fail")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP, a INT) VALUES ('2024-01-01 00:00:00', 1, 99) t_neg3"
        )

        tdLog.info("text_source: unsorted rows should fail")
        tdSql.error(
            "SELECT ts FROM TEXT(ts TIMESTAMP) "
            "VALUES ('2024-01-03 00:00:00') ('2024-01-01 00:00:00') t_neg4"
        )

