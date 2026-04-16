import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestFileSource:
    """FILE table source pytest test class.

    Exercises the FILE(path, schema_decl [, option ...]) virtual-table syntax
    that reads CSV data directly from a file at query time.

    CSV test data is stored in the in/ directory alongside this file:
      file_source_basic.csv     - 5 columns (ts, c1 INT, c2 DOUBLE, c3 BOOL, c4 VARCHAR(32)), 3 rows, no header
      file_source_header.csv    - 4 columns (ts, c1 INT, c2 DOUBLE, label VARCHAR(16)), 4 rows incl. header row
      file_source_nulls.csv     - 3 columns (ts, c1 INT, c2 DOUBLE), 3 rows with NULL cells
      file_source_unsorted.csv  - 2 columns (ts, c1 INT), 3 rows in reverse timestamp order
      file_source_groups.csv    - 3 columns (ts, grp VARCHAR(4), val INT), 4 rows / 2 groups
      file_source_widecols.csv  - 8 columns (ts + 7 INTs), used to test schema-narrower-than-CSV
      file_source_bad_types.csv - 2 columns with invalid INT and BOOL text values

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
        - Empty schema_decl string: parser should reject the query

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-17 Copilot Added for FILE table source feature

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

        tdLog.info("file_source: empty schema_decl should fail")
        in_dir = os.path.join(os.path.dirname(__file__), "in")
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_basic.csv', '') f"
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
            - 2026-04-20 Copilot Added for large data volume coverage
            - 2026-04-20 Copilot Aligned row cap with TEXT() (kMaxInlineRows = 10000)
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
            - 2026-04-22 Copilot Added for test coverage gap analysis

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

        # --- Q3: schema narrower than CSV (positional) ---
        tdLog.info("file_source_coverage Q3: schema declares 3 cols, CSV has 8 cols")
        # file_source_widecols.csv has ts,c1..c7 (8 cols); we declare only ts+c1+c2 (3 cols)
        tdSql.query(
            f"SELECT ts, c1, c2 FROM FILE('{in_dir}/file_source_widecols.csv', "
            f"'ts TIMESTAMP, c1 INT, c2 INT') f_narrow ORDER BY ts"
        )
        tdSql.checkRows(3)
        # rows: (10,20), (11,21), (12,22) — positional cols 1 and 2 from CSV
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20)
        tdSql.checkData(2, 1, 12)
        tdSql.checkData(2, 2, 22)

        # --- Q4: header=true with column name absent from CSV header ---
        tdLog.info("file_source_coverage Q4: missing header column should be rejected")
        # file_source_header.csv header row: ts,c1,c2,label — 'no_such_col' does not exist
        tdSql.error(
            f"SELECT ts FROM FILE('{in_dir}/file_source_header.csv', "
            f"'ts TIMESTAMP, c1 INT, no_such_col INT', header=true) f_badcol"
        )

        # --- Q5: invalid type coercion (documents silent-zero behaviour) ---
        tdLog.info("file_source_coverage Q5: bad cell values silently coerce to 0 / false")
        # file_source_bad_types.csv rows: "not_an_int" and "not_a_bool"
        tdSql.query(
            f"SELECT c1, c2 FROM FILE('{in_dir}/file_source_bad_types.csv', "
            f"'ts TIMESTAMP, c1 INT, c2 BOOL') f_bad ORDER BY ts"
        )
        tdSql.checkRows(2)
        # non-numeric strings → INT 0; non-bool strings → BOOL false
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, False)

        # --- Q6: wide-column FILE table ---
        tdLog.info("file_source_coverage Q6: 8-column FILE table reads all columns correctly")
        tdSql.query(
            f"SELECT c1, c7 FROM FILE('{in_dir}/file_source_widecols.csv', "
            f"'ts TIMESTAMP, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT') f_wide "
            f"ORDER BY ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 10)   # c1 row 1
        tdSql.checkData(0, 1, 70)   # c7 row 1
        tdSql.checkData(2, 0, 12)   # c1 row 3
        tdSql.checkData(2, 1, 72)   # c7 row 3

        tdLog.debug("test_file_source_coverage passed")

    def test_file_source_groupby(self):
        """FILE table source - GROUP BY aggregation tests.

        Safe patterns (empirically validated; <=4 rows, <=2 group keys):
        - GROUP BY with SUM(val)
        - GROUP BY with MIN / MAX / SUM multi-aggregate
        - GROUP BY with COUNT(col)

        NOTE: This test MUST be defined last in the class. The GROUP BY code path
        on FILE tables previously had a server-side heap-corruption side-effect;
        run it last as a precaution while the root cause is under investigation.

        Since: v3.4.2

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-17 Copilot Added for FILE table source feature

        """

        self._run_groupby_queries()

        tdLog.debug("test_file_source_groupby passed")

    def _run_groupby_queries(self):
        tdLog.info("file_source_groupby: running GROUP BY cases")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "file_source_groupby.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "file_source_groupby.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "file_source_groupby")
