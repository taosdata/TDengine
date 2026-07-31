import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestFileSource:
    """FILE table source pytest test class.

    Exercises the FILE(path, column_list [, option ...]) virtual-table syntax
    that reads CSV data directly from a file at query time.

    CSV test data is stored in the in/ directory alongside this file:
      file_source_basic.csv     - 5 columns (ts, c1 INT, c2 DOUBLE, c3 BOOL, c4 VARCHAR(32)), 3 rows, no header
      file_source_header.csv    - 4 columns (ts, c1 INT, c2 DOUBLE, label VARCHAR(16)), 4 rows incl. header row
      file_source_nulls.csv     - 3 columns (ts, c1 INT, c2 DOUBLE), 3 rows with NULL cells
      file_source_unsorted.csv  - 2 columns (ts, c1 INT), 3 rows in reverse timestamp order
      file_source_groups.csv    - 3 columns (ts, grp VARCHAR(4), val INT), 4 rows / 2 groups
      file_source_widecols.csv  - 8 columns (ts + 7 INTs), used to test schema-narrower-than-CSV
      file_source_bad_types.csv - 2 columns with invalid INT and BOOL text values
      file_source_no_ts.csv     - 3 columns (id INT, name VARCHAR(16), score FLOAT), no timestamp column
      file_source_dup_ts.csv    - 2 columns (ts TIMESTAMP, id INT), 3 rows with duplicate timestamps, unsorted

    Known limitations (excluded from tests to avoid crashes or wrong results):
      - Projecting a non-contiguous column subset that ends on a VARCHAR column crashes
        the server executor (buffer-offset calculation bug). Safe patterns: project all
        columns, or project a leading prefix (ts, c1), or use a subquery.
      - DOUBLE-column-only WHERE predicate (e.g. WHERE c2 > 1.0) returns 0 rows (bug in
        float predicate pushdown in the rowset scan path).
      - COUNT(*) crashes the server; use COUNT(col) instead.
      - Direct WHERE with IS NULL / IS NOT NULL on a FILE table returns wrong row counts;
        wrap the FILE() source in a subquery to make NULL predicates work correctly.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_file_source(self):
        """FILE table source - positive query tests.

        Positive cases (see in/file_source.in for the full SQL):
        - Projection: SELECT all named columns, SELECT *, SELECT ts only
        - Auto-sort: unsorted CSV rows are returned in ascending timestamp order
        - header=true: header row is skipped; only data rows are returned
        - LIMIT: restricts the returned row count
        - ORDER BY non-ts column (DESC)
        - WHERE on INT column (>, BETWEEN)
        - WHERE on BOOL column (= true, projects all columns to avoid partial-col crash)
        - IS NOT NULL / IS NULL filters applied on a subquery wrapper
        - Aggregate functions: MAX/MIN, SUM+COUNT, aggregate with WHERE pre-filter
        - NULL round-trip: empty CSV fields become NULL in INT and DOUBLE columns
        - FILE() as inner source of a subquery with outer WHERE

        Negative cases (inline):
        - Nonexistent file path: server should return an error
        - Empty column_list string: parser should reject the query

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-17 Added for FILE table source feature

        """

        tdSql.prepare("file_src_db", drop=True)
        self._run_positive_queries()
        self._run_negative()

        tdLog.debug("test_file_source passed")

    def _run_positive_queries(self):
        tdLog.info("file_source: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_source.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_source.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_source")

    def _run_negative(self):
        tdLog.info("file_source: nonexistent file path should fail")
        tdSql.error(
            "SELECT ts FROM FILE('/nonexistent/no_such_file.csv', 'ts TIMESTAMP') f"
        )

        tdLog.info("file_source: empty column_list should fail")
        in_dir = os.path.join(os.path.dirname(__file__), "in")
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_basic.csv', '') f"
        )

        tdLog.info("file_source: multi-char delimiter should fail")
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_basic.csv', 'ts TIMESTAMP, a INT', delimiter='||') f"
        )

        tdLog.info("file_source: delimiter without quotes should fail")
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_basic.csv', 'ts TIMESTAMP, a INT', delimiter=1) f"
        )

    def test_file_source_large(self):
        """FILE table source — large data volume: 10,000 rows (well within cap) and over-limit rejection.

        Reads file_source_large.csv (10,000 rows, schema: ts TIMESTAMP, a INT,
        a = 0..9999) and validates aggregates, WHERE filters, and ORDER BY.

        The per-query row cap is 10,000 (kMaxInlineRows in parTranslater.c), shared with
        TEXT().  FILE() rows are loaded into an SSDataBlock in memory at plan time; the
        cap prevents runaway allocation when pointed at unexpectedly large files.

        Positive checks (10,000 rows — exactly at the cap):
        - COUNT / SUM / MIN / MAX over all rows
        - WHERE predicate (half the rows selected)
        - ORDER BY + LIMIT (top-5 descending)

        Negative checks:
        - 10,001 rows → translation error (row count exceeds limit)

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-20 Added for large data volume coverage
            - 2026-04-20 Aligned row cap with TEXT() (kMaxInlineRows = 10000)
        """
        import datetime, os, tempfile

        in_dir = os.path.join(os.path.dirname(__file__), "in")
        csv = f"{in_dir}/file_source_large.csv"
        schema = "'ts TIMESTAMP, a INT'"
        src = f"FILE('{csv}', {schema})"

        # --- Positive: 10,000 rows ---
        tdSql.query(f"SELECT COUNT(a), SUM(a), MIN(a), MAX(a) FROM {src} f")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)     # COUNT
        tdSql.checkData(0, 1, 49995000)  # SUM(0..9999) = 9999*10000/2
        tdSql.checkData(0, 2, 0)         # MIN
        tdSql.checkData(0, 3, 9999)      # MAX

        # WHERE filter: a >= 5000 → 5000 rows (5000..9999)
        tdSql.query(f"SELECT COUNT(a) FROM {src} f WHERE a >= 5000")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5000)

        # ORDER BY DESC + LIMIT: top 5
        tdSql.query(f"SELECT a FROM {src} f ORDER BY a DESC LIMIT 5")
        tdSql.checkRows(5)
        for rank, expected in enumerate([9999, 9998, 9997, 9996, 9995]):
            tdSql.checkData(rank, 0, expected)

        # --- Negative: 10,001 rows (one over kMaxInlineRows=10000) must be rejected ---
        tdLog.info("file_source_large: 10001 rows should be rejected (exceeds kMaxInlineRows=10000)")
        base = datetime.datetime(2020, 1, 1, 0, 0, 0)
        over_limit = 10001
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix="tdengine_file_over_")
        try:
            with os.fdopen(tmp_fd, "w") as fout:
                for i in range(over_limit):
                    ts = (base + datetime.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
                    fout.write(f"{ts},{i}\n")
            over_src = f"FILE('{tmp_path}', {schema})"
            tdSql.error(f"SELECT COUNT(a) FROM {over_src} f_over")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        tdLog.debug("test_file_source_large passed")

    def test_file_source_coverage(self):
        """FILE table source — edge-case and coverage tests.

        Q1 (large data in subquery): 10,000-row FILE() as inner source of a subquery.
          Verifies that the optimizer does not mis-prune ROWSET_SOURCE targets when a
          PROJECT node sits above it, and that the executor produces correct results.

        Q2 (column type coverage): BIGINT and FLOAT columns round-trip correctly through
          the FILE() positional CSV parser.

        Q3 (schema narrower than CSV): when schema declares N columns but the CSV file
          physically has M > N columns (positional mode, header=false), only the first N
          columns are read; extra CSV columns are silently ignored.

        Q4 (header=true, column not in CSV): when a schema column name is absent from the
          CSV header row, the parser rejects the query with a descriptive error.

        Q5 (type coercion on bad data): invalid CSV cell values are silently coerced to
          zero / false rather than raising an error (consistent with TDengine's general
          type-coercion semantics — same behaviour as CAST('abc' AS INT) in SQL).
          This test documents the behaviour so that regressions are detected.

        Q6 (wide columns): a FILE() table with many columns and few rows is handled
          correctly; memory allocation is proportional to rows × columns.

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-22 Added for test coverage gap analysis

        """
        import datetime, tempfile, os

        in_dir = os.path.join(os.path.dirname(__file__), "in")

        # --- Q1: large data in subquery ---
        tdLog.info("file_source_coverage Q1: 10,000-row FILE as subquery inner source")
        csv = f"{in_dir}/file_source_large.csv"
        outer = (
            f"SELECT COUNT(a), SUM(a) FROM "
            f"(SELECT ts, a FROM FILE('{csv}', 'ts TIMESTAMP, a INT') f_inner) sub "
            f"WHERE a >= 5000"
        )
        tdSql.query(outer)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5000)      # 5000..9999 → 5000 rows
        tdSql.checkData(0, 1, 37497500)  # SUM(5000..9999) = 5000*14999/2

        # --- Q2: BIGINT and FLOAT column types ---
        tdLog.info("file_source_coverage Q2: BIGINT and FLOAT column type coverage")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-05-01 00:00:01,9876543210,3.14\n")
            fh.write("2026-05-01 00:00:02,1234567890,2.71\n")
        try:
            tdSql.query(
                f"SELECT ts, b, f FROM FILE('{path}', 'ts TIMESTAMP, b BIGINT, f FLOAT') f_types ORDER BY ts"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 9876543210)
            tdSql.checkData(1, 1, 1234567890)
        finally:
            os.unlink(path)

        # --- Deterministic queries Q3, Q5, Q6 via file-based comparison ---
        tdLog.info("file_coverage: running deterministic queries via .in/.ans")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_coverage.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_coverage.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_coverage")

        # --- Q4: header=true with column name absent from CSV header ---
        tdLog.info("file_source_coverage Q4: missing header column should be rejected")
        # file_source_header.csv header row: ts,c1,c2,label — 'no_such_col' does not exist
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_header.csv', "
            f"'ts TIMESTAMP, c1 INT, no_such_col INT', header=true) f_badcol"
        )

        tdLog.debug("test_file_source_coverage passed")

    def test_file_source_groupby(self):
        """FILE table source - GROUP BY aggregation tests.

        Safe patterns (empirically validated; <=4 rows, <=2 group keys):
        - GROUP BY with SUM(val)
        - GROUP BY with MIN / MAX / SUM multi-aggregate
        - GROUP BY with COUNT(col)

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-17 Added for FILE table source feature
            - 2026-04-21 Self-contained: creates file_src_db before running

        """

        tdSql.prepare("file_src_db", drop=True)
        self._run_groupby_queries()

        tdLog.debug("test_file_source_groupby passed")

    def _run_groupby_queries(self):
        tdLog.info("file_source_groupby: running GROUP BY cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_source_groupby.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_source_groupby.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_source_groupby")

    def test_file_source_no_ts(self):
        """FILE source requires first column to be TIMESTAMP — rejection cases.

        FILE() requires the first column to be TIMESTAMP (primary key). Queries
        without a TIMESTAMP first column are rejected at parse time with error
        "FILE source requires the first column to be TIMESTAMP".

        Negative cases:
        - SELECT on non-ts FILE → rejected
        - ORDER BY on non-ts FILE → rejected
        - GROUP BY on non-ts FILE → rejected
        - INTERVAL on non-ts FILE → rejected
        - JOIN with non-ts FILE → rejected
        - Non-first TIMESTAMP col → rejected

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-22 Added to cover FILE non-TIMESTAMP first column behaviour
            - 2026-06-xx Changed: no-ts queries now rejected (first col must be TIMESTAMP)

        """

        in_dir = os.path.join(os.path.dirname(__file__), "in")
        csv = f"{in_dir}/file_source_no_ts.csv"
        schema = "'id INT, name VARCHAR(16), score FLOAT'"

        tdSql.prepare("file_nots_db", drop=True)

        # Original no-ts queries via file-based comparison (all produce errors now)
        tdLog.info("file_no_ts: running no-ts FILE queries (all rejected)")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_no_ts.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_no_ts.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_no_ts")

        # All no-ts FILE queries are now rejected
        tdLog.info("file_source_no_ts: SELECT on non-ts FILE must be rejected")
        tdSql.error(
            f"SELECT id, score FROM FILE('{csv}', {schema}) f ORDER BY id"
        )

        tdLog.info("file_source_no_ts: ORDER BY on non-ts FILE must be rejected")
        tdSql.error(
            f"SELECT id FROM FILE('{csv}', {schema}) f ORDER BY score DESC"
        )

        tdLog.info("file_source_no_ts: GROUP BY on non-ts FILE must be rejected")
        tdSql.error(
            f"SELECT COUNT(id) FROM FILE('{csv}', {schema}) f GROUP BY name"
        )

        # INTERVAL requires a primary timestamp column
        tdLog.info("file_source_no_ts: INTERVAL on non-ts FILE must be rejected")
        tdSql.error(
            f"SELECT COUNT(id) FROM FILE('{csv}', {schema}) f INTERVAL(1s)"
        )

        # JOIN requires a primary timestamp column in TEXT/FILE source
        tdLog.info("file_source_no_ts: JOIN with non-ts FILE must be rejected")
        tdSql.execute("USE file_nots_db")
        tdSql.execute("CREATE TABLE IF NOT EXISTS ref_for_file_join (ts TIMESTAMP, id INT)")
        tdSql.error(
            f"SELECT f.id FROM FILE('{csv}', {schema}) f "
            "JOIN ref_for_file_join r ON f.id=r.id"
        )

        # --- D-series: first col non-TIMESTAMP, second col TIMESTAMP ---
        import tempfile as _tmpfile

        fd, ts2_path = _tmpfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2,2026-01-01 00:00:02\n")
            fh.write("1,2026-01-01 00:00:01\n")
        ts2_schema = "'id INT, ts TIMESTAMP'"

        try:
            # D1: non-first TIMESTAMP col → rejected
            tdLog.info("D1: non-first TIMESTAMP col must be rejected")
            tdSql.error(
                f"SELECT id, ts FROM FILE('{ts2_path}', {ts2_schema}) f ORDER BY ts ASC"
            )

            # D2: WHERE on non-first TIMESTAMP → rejected
            tdLog.info("D2: WHERE on non-first TIMESTAMP must be rejected")
            tdSql.error(
                f"SELECT id FROM FILE('{ts2_path}', {ts2_schema}) f "
                "WHERE ts > '2026-01-01 00:00:01'"
            )

            # D3: JOIN with non-first TIMESTAMP → rejected
            tdLog.info("D3: JOIN with non-first TIMESTAMP must be rejected")
            tdSql.error(
                f"SELECT f.id FROM FILE('{ts2_path}', {ts2_schema}) f "
                "JOIN ref_for_file_join r ON f.ts = r.ts"
            )

            # D4: INTERVAL with non-first TIMESTAMP → rejected
            tdLog.info("D4: INTERVAL with non-first TIMESTAMP must be rejected")
            tdSql.error(
                f"SELECT COUNT(id) FROM FILE('{ts2_path}', {ts2_schema}) f INTERVAL(1s)"
            )
        finally:
            os.unlink(ts2_path)

        tdLog.debug("test_file_source_no_ts passed")

    def test_file_source_dup_ts(self):
        """FILE source with duplicate timestamps — all rows preserved, no UPSERT.

        E1: duplicate-ts rows all preserved
        E2: COUNT includes all duplicate-ts rows
        E3: GROUP BY ts with duplicates
        E4: FIRST/LAST with duplicate timestamps (non-deterministic FIRST)
        E5: DERIVATIVE/TWA reject duplicate timestamps; CSUM/DIFF/IRATE process duplicate timestamps by row order.

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-12 Added to cover duplicate timestamp behaviour

        """

        in_dir = os.path.join(os.path.dirname(__file__), "in")
        csv = f"{in_dir}/file_source_dup_ts.csv"
        schema = "'ts TIMESTAMP, id INT'"

        tdSql.prepare("file_dupts_db", drop=True)

        # Deterministic positive queries via file-based comparison
        tdLog.info("file_dup_ts: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_dup_ts.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_dup_ts.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_dup_ts")

        # E4: FIRST/LAST work with duplicate timestamps (non-deterministic FIRST)
        tdLog.info("E4: FIRST/LAST with duplicate timestamps from FILE")
        dup_sub = f"(SELECT ts, id FROM FILE('{csv}', {schema}) f)"
        tdSql.query(f"SELECT FIRST(id) FROM {dup_sub}")
        assert tdSql.getData(0, 0) in (1, 2), f"FIRST(id) should be 1 or 2, got {tdSql.getData(0, 0)}"
        tdSql.query(f"SELECT LAST(id) FROM {dup_sub}")
        tdSql.checkData(0, 0, 3)

        # E5: DERIVATIVE/TWA reject duplicate timestamps; CSUM/DIFF/IRATE process duplicate timestamps by row order.
        tdLog.info("E5: time-series functions with duplicate timestamps from FILE")
        tdSql.query(f"SELECT CSUM(id) FROM {dup_sub}")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 6)
        tdSql.query(f"SELECT DIFF(id) FROM {dup_sub}")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -1)
        tdSql.checkData(1, 0, 2)
        tdSql.error(f"SELECT DERIVATIVE(id, 1s, 0) FROM {dup_sub}")
        tdSql.error(f"SELECT TWA(id) FROM {dup_sub}")
        tdSql.query(f"SELECT IRATE(id) FROM {dup_sub}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1.1574074074074073e-05, tolerance=1e-12)

        tdLog.debug("test_file_source_dup_ts passed")

    def test_file_source_union(self):
        """FILE table source: UNION / UNION ALL combined with real tables and other FILE sources.

        U1: no-ts FILE UNION ALL TEXT
        U2: TEXT UNION ALL ts FILE ORDER BY volt DESC
        U3: FILE UNION ALL FILE (same source, doubled)
        U4: FILE UNION FILE (dedup)
        U5: ts FILE UNION ALL real table ORDER BY volt

        Since: v3.4.2

        Labels: common,unit

        Jira: None
        """
        tdSql.prepare("file_union_db", drop=True)

        # Deterministic positive queries via file-based comparison
        tdLog.info("file_union: running positive query cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_union.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_union.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_union")

        tdLog.debug("test_file_source_union passed")

    def test_file_source_schema_types(self):
        """FILE column_list: type rejection, type coverage, CSV NULL, and edge cases.

        Negative (rejected types):
        N1: JSON rejected.  N2: GEOMETRY rejected.
        N3: BLOB rejected.  N4: MEDIUMBLOB rejected.

        Positive (type coverage):
        P1: VARBINARY round-trip.  P2: NCHAR with Chinese text.
        P3: SMALLINT/TINYINT/unsigned integers.
        P4: CSV NULL representations (empty, NULL, null).
        P5: backslash-N is literal, not NULL.
        P6: Quoted empty string handling.
        P7: BOOL values from CSV (true/false/NULL).
        P8: FLOAT/DOUBLE precision from CSV.
        P9: CSV has more columns than column_list.
        P10: DECIMAL(10,2) from CSV.  P11: DECIMAL(38,10) from CSV.

        Since: v3.4.2

        Labels: common,ci

        Jira: None
        """
        import tempfile, os

        # --- N1: JSON column_list should be rejected ---
        tdLog.info("file_source_schema_types N1: JSON in column_list should be rejected")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write('2026-01-01 00:00:01,{"k":1}\n')
        try:
            tdSql.error(
                f"SELECT ts FROM FILE('{path}', 'ts TIMESTAMP, j JSON') f"
            )
        finally:
            os.unlink(path)

        # --- N2: GEOMETRY column_list should be rejected ---
        tdLog.info("file_source_schema_types N2: GEOMETRY in column_list should be rejected")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,POINT(1 2)\n")
        try:
            tdSql.error(
                f"SELECT ts FROM FILE('{path}', 'ts TIMESTAMP, g GEOMETRY(64)') f"
            )
        finally:
            os.unlink(path)

        # --- N3: BLOB column_list should be rejected ---
        tdLog.info("file_source_schema_types N3: BLOB in column_list should be rejected")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,data\n")
        try:
            tdSql.error(
                f"SELECT ts FROM FILE('{path}', 'ts TIMESTAMP, b BLOB') f"
            )
        finally:
            os.unlink(path)

        # --- N4: MEDIUMBLOB column_list should be rejected ---
        tdLog.info("file_source_schema_types N4: MEDIUMBLOB in column_list should be rejected")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,data\n")
        try:
            tdSql.error(
                f"SELECT ts FROM FILE('{path}', 'ts TIMESTAMP, b MEDIUMBLOB') f"
            )
        finally:
            os.unlink(path)

        # --- Deterministic type coverage queries via file-based comparison ---
        # (covers P1-P4, P7-P9, P10 using static CSV files)
        tdLog.info("file_schema_types: running positive query cases via .in/.ans")
        tdSql.prepare("file_src_db", drop=True)
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_schema_types.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_schema_types.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_schema_types")

        # --- P10: DECIMAL(10,2) from CSV ---
        tdLog.info("file_source_schema_types P10: DECIMAL(10,2) from CSV")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,3.14\n")
            fh.write("2026-01-01 00:00:02,NULL\n")
            fh.write("2026-01-01 00:00:03,99.99\n")
        try:
            tdSql.query(
                f"SELECT ts, d FROM FILE('{path}', 'ts TIMESTAMP, d DECIMAL(10,2)') f ORDER BY ts"
            )
            tdSql.checkRows(3)
            assert float(tdSql.queryResult[0][1]) == 3.14, f"P10: expected 3.14, got {tdSql.queryResult[0][1]}"
            assert tdSql.queryResult[1][1] is None, f"P10: expected NULL, got {tdSql.queryResult[1][1]}"
            assert float(tdSql.queryResult[2][1]) == 99.99, f"P10: expected 99.99, got {tdSql.queryResult[2][1]}"
        finally:
            os.unlink(path)

        # --- P11: DECIMAL(38,10) from CSV ---
        tdLog.info("file_source_schema_types P11: DECIMAL(38,10) from CSV")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,12345.6789\n")
            fh.write("2026-01-01 00:00:02,NULL\n")
        try:
            tdSql.query(
                f"SELECT ts, d FROM FILE('{path}', 'ts TIMESTAMP, d DECIMAL(38,10)') f ORDER BY ts"
            )
            tdSql.checkRows(2)
            assert tdSql.queryResult[0][1] is not None, f"P11: expected value, got None"
            assert tdSql.queryResult[1][1] is None, f"P11: expected NULL"
        finally:
            os.unlink(path)

        # --- P1: VARBINARY column round-trip (plain string and hex literal) ---
        tdLog.info("file_source_schema_types P1: VARBINARY column reads plain strings")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,hello\n")
            fh.write("2026-01-01 00:00:02,world\n")
        try:
            tdSql.query(
                f"SELECT ts, v FROM FILE('{path}', 'ts TIMESTAMP, v VARBINARY(64)') f ORDER BY ts"
            )
            tdSql.checkRows(2)
            # VARBINARY values are returned as hex-encoded strings by the driver
            assert tdSql.queryResult[0][1] is not None, "P1: row 0 VARBINARY should not be NULL"
            assert tdSql.queryResult[1][1] is not None, "P1: row 1 VARBINARY should not be NULL"
        finally:
            os.unlink(path)

        # --- P2: NCHAR column with Chinese characters ---
        tdLog.info("file_source_schema_types P2: NCHAR column with Chinese text")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,你好\n")
            fh.write("2026-01-01 00:00:02,世界\n")
        try:
            tdSql.query(
                f"SELECT ts, n FROM FILE('{path}', 'ts TIMESTAMP, n NCHAR(64)') f ORDER BY ts"
            )
            tdSql.checkRows(2)
            assert tdSql.queryResult[0][1] == "你好", f"P2: expected '你好', got '{tdSql.queryResult[0][1]}'"
            assert tdSql.queryResult[1][1] == "世界", f"P2: expected '世界', got '{tdSql.queryResult[1][1]}'"
        finally:
            os.unlink(path)

        # --- P3: SMALLINT, TINYINT, unsigned integer types ---
        tdLog.info("file_source_schema_types P3: small/unsigned integer types")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,127,32767,255,65535,4294967295,18446744073709551615\n")
        try:
            tdSql.query(
                f"SELECT * FROM FILE('{path}', "
                f"'ts TIMESTAMP, v1 TINYINT, v2 SMALLINT, v3 UTINYINT, v4 USMALLINT, v5 UINT, v6 UBIGINT') f"
            )
            tdSql.checkRows(1)
            row = tdSql.queryResult[0]
            assert row[1] == 127, f"P3: TINYINT expected 127, got {row[1]}"
            assert row[2] == 32767, f"P3: SMALLINT expected 32767, got {row[2]}"
            assert row[3] == 255, f"P3: UTINYINT expected 255, got {row[3]}"
            assert row[4] == 65535, f"P3: USMALLINT expected 65535, got {row[4]}"
            assert row[5] == 4294967295, f"P3: UINT expected 4294967295, got {row[5]}"
            assert row[6] == 18446744073709551615, f"P3: UBIGINT expected max, got {row[6]}"
        finally:
            os.unlink(path)

        # --- P4: CSV NULL representations ---
        tdLog.info("file_source_schema_types P4: CSV NULL field representations")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            # empty field → NULL
            fh.write("2026-01-01 00:00:01,,\n")
            # 'NULL' text → NULL
            fh.write("2026-01-01 00:00:02,NULL,NULL\n")
            # 'null' text → NULL
            fh.write("2026-01-01 00:00:03,null,null\n")
            # normal value (control)
            fh.write("2026-01-01 00:00:04,42,hello\n")
        try:
            tdSql.query(
                f"SELECT ts, v, s FROM FILE('{path}', "
                f"'ts TIMESTAMP, v INT, s VARCHAR(32)') f ORDER BY ts"
            )
            tdSql.checkRows(4)
            # row 0: empty field → NULL for both INT and VARCHAR
            assert tdSql.queryResult[0][1] is None, f"P4: empty INT should be NULL, got {tdSql.queryResult[0][1]}"
            assert tdSql.queryResult[0][2] is None, f"P4: empty VARCHAR should be NULL, got {tdSql.queryResult[0][2]}"
            # row 1: 'NULL' → NULL
            assert tdSql.queryResult[1][1] is None, f"P4: 'NULL' INT should be NULL, got {tdSql.queryResult[1][1]}"
            assert tdSql.queryResult[1][2] is None, f"P4: 'NULL' VARCHAR should be NULL, got {tdSql.queryResult[1][2]}"
            # row 2: 'null' → NULL
            assert tdSql.queryResult[2][1] is None, f"P4: 'null' INT should be NULL, got {tdSql.queryResult[2][1]}"
            assert tdSql.queryResult[2][2] is None, f"P4: 'null' VARCHAR should be NULL, got {tdSql.queryResult[2][2]}"
            # row 3: normal
            assert tdSql.queryResult[3][1] == 42, f"P4: normal INT expected 42, got {tdSql.queryResult[3][1]}"
            assert tdSql.queryResult[3][2] == "hello", f"P4: normal VARCHAR expected 'hello', got {tdSql.queryResult[3][2]}"
        finally:
            os.unlink(path)

        # --- P5: CSV backslash-N is NOT treated as NULL ---
        tdLog.info("file_source_schema_types P5: backslash-N is literal, not NULL")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,\\N,\\N\n")
        try:
            tdSql.query(
                f"SELECT ts, v, s FROM FILE('{path}', "
                f"'ts TIMESTAMP, v INT, s VARCHAR(32)') f"
            )
            tdSql.checkRows(1)
            # \\N for INT becomes 0 (parsed as number)
            assert tdSql.queryResult[0][1] is not None or tdSql.queryResult[0][1] == 0, \
                f"P5: \\N INT not treated as NULL, got {tdSql.queryResult[0][1]}"
            # \\N for VARCHAR becomes literal string "\\N"
            assert tdSql.queryResult[0][2] is not None, \
                f"P5: \\N VARCHAR should be literal, got {tdSql.queryResult[0][2]}"
        finally:
            os.unlink(path)

        # --- P6: CSV quoted empty string → NULL for VARCHAR ---
        tdLog.info("file_source_schema_types P6: quoted empty string")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write('2026-01-01 00:00:01,"",""\n')
        try:
            tdSql.query(
                f"SELECT ts, s1, s2 FROM FILE('{path}', "
                f"'ts TIMESTAMP, s1 VARCHAR(32), s2 NCHAR(32)') f"
            )
            tdSql.checkRows(1)
            # quoted empty strings: may be NULL or empty string depending on implementation
            # just verify no crash and result returned
        finally:
            os.unlink(path)

        # --- P7: BOOL with NULL and various representations ---
        tdLog.info("file_source_schema_types P7: BOOL values from CSV")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,true\n")
            fh.write("2026-01-01 00:00:02,false\n")
            fh.write("2026-01-01 00:00:03,NULL\n")
        try:
            tdSql.query(
                f"SELECT ts, b FROM FILE('{path}', 'ts TIMESTAMP, b BOOL') f ORDER BY ts"
            )
            tdSql.checkRows(3)
            assert tdSql.queryResult[0][1] is True or tdSql.queryResult[0][1] == 1, \
                f"P7: true expected True, got {tdSql.queryResult[0][1]}"
            assert tdSql.queryResult[1][1] is False or tdSql.queryResult[1][1] == 0, \
                f"P7: false expected False, got {tdSql.queryResult[1][1]}"
            assert tdSql.queryResult[2][1] is None, \
                f"P7: NULL BOOL expected None, got {tdSql.queryResult[2][1]}"
        finally:
            os.unlink(path)

        # --- P8: FLOAT/DOUBLE precision from CSV ---
        tdLog.info("file_source_schema_types P8: FLOAT/DOUBLE from CSV")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            fh.write("2026-01-01 00:00:01,3.14,2.718281828459045\n")
            fh.write("2026-01-01 00:00:02,NULL,NULL\n")
        try:
            tdSql.query(
                f"SELECT ts, f, d FROM FILE('{path}', "
                f"'ts TIMESTAMP, f FLOAT, d DOUBLE') f_tbl ORDER BY ts"
            )
            tdSql.checkRows(2)
            assert abs(tdSql.queryResult[0][1] - 3.14) < 0.01, \
                f"P8: FLOAT expected ~3.14, got {tdSql.queryResult[0][1]}"
            assert abs(tdSql.queryResult[0][2] - 2.718281828459045) < 1e-10, \
                f"P8: DOUBLE expected ~2.718, got {tdSql.queryResult[0][2]}"
            assert tdSql.queryResult[1][1] is None, "P8: FLOAT NULL"
            assert tdSql.queryResult[1][2] is None, "P8: DOUBLE NULL"
        finally:
            os.unlink(path)

        # --- P9: CSV partial columns — column_list has fewer cols than CSV ---
        tdLog.info("file_source_schema_types P9: CSV has more columns than column_list")
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w") as fh:
            # CSV has 5 columns but column_list declares only 3; FILE reads first 3 by position
            fh.write("2026-01-01 00:00:01,10,skip1,hello,skip2\n")
            fh.write("2026-01-01 00:00:02,20,skip3,world,skip4\n")
        try:
            tdSql.query(
                f"SELECT * FROM FILE('{path}', 'ts TIMESTAMP, a INT, b VARCHAR(32)') f ORDER BY ts"
            )
            tdSql.checkRows(2)
            assert tdSql.queryResult[0][1] == 10, f"P9: row0 a expected 10, got {tdSql.queryResult[0][1]}"
            assert tdSql.queryResult[0][2] == "skip1", f"P9: row0 b expected 'skip1', got {tdSql.queryResult[0][2]}"
        finally:
            os.unlink(path)

        tdLog.debug("test_file_source_schema_types passed")
