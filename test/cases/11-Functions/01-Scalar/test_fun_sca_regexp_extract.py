from new_test_framework.utils import tdLog, tdSql
import datetime


class TestFunRegexpExtract:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_tb(self, dbname="db"):
        tdSql.execute(f"""CREATE STABLE {dbname}.st (
            ts TIMESTAMP, vc VARCHAR(128), nc NCHAR(64), iv INT
        ) TAGS (t INT)""")
        tdSql.execute(f"CREATE TABLE {dbname}.ct1 USING {dbname}.st TAGS(1)")
        tdSql.execute(f"CREATE TABLE {dbname}.ct2 USING {dbname}.st TAGS(2)")
        tdSql.execute(f"""CREATE TABLE {dbname}.nt (
            ts TIMESTAMP, vc VARCHAR(128), nc NCHAR(64), iv INT
        )""")

    def _insert_data(self, dbname="db"):
        now = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        # ct1: log-style rows + one NULL row
        ct1_rows = [
            (now - 4000, "'code=42,type=DISK_FULL'", "'code=42,type=DISK_FULL'", 42),
            (now - 3000, "'code=7,type=LOW_MEM'",   "'code=7,type=LOW_MEM'",     7),
            (now - 2000, "'code=0,type=OK'",         "'code=0,type=OK'",          0),
            (now - 1000, "NULL",                     "NULL",                      "NULL"),
        ]
        for ts, vc, nc, iv in ct1_rows:
            tdSql.execute(f"INSERT INTO {dbname}.ct1 VALUES({ts}, {vc}, {nc}, {iv})")
        # ct2: URL-style rows
        ct2_rows = [
            (now - 3000, "'https://example.com'", "'https://example.com'", 1),
            (now - 2000, "'http://api.example.org'", "'http://api.example.org'", 2),
            (now - 1000, "'ftp://files.example.net'", "'ftp://files.example.net'", 3),
        ]
        for ts, vc, nc, iv in ct2_rows:
            tdSql.execute(f"INSERT INTO {dbname}.ct2 VALUES({ts}, {vc}, {nc}, {iv})")
        # nt: same as ct1
        for ts, vc, nc, iv in ct1_rows:
            tdSql.execute(f"INSERT INTO {dbname}.nt VALUES({ts}, {vc}, {nc}, {iv})")

    def _check_basic(self, dbname="db"):
        # -----------------------------------------------------------------
        # §1  Default group_idx=1 — no-table queries
        # -----------------------------------------------------------------
        # RXE-BASIC-001: single capture group → group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-BASIC-002: multiple capture groups, default → group 1 only
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-BASIC-003: no capture group, default group_idx=1 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'b')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §2  group_idx=0 whole match
        # -----------------------------------------------------------------
        # RXE-GRP0-001: no capture group, group_idx=0 → whole match
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'b', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-GRP0-002: with capture group, group_idx=0 → whole match ≠ group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)c', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'bc')

        # RXE-GRP0-003: no match, group_idx=0 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'x+', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §3  Multiple capture groups by explicit index
        # -----------------------------------------------------------------
        # RXE-GRP-001: explicit group_idx=1 → group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-GRP-002: explicit group_idx=2 → group 2
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'c')

        # RXE-GRP-003: group_idx out of range → NULL, no error
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 3)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §4  NULL and no-match
        # -----------------------------------------------------------------
        # RXE-NULL-001: str=NULL → NULL
        tdSql.query("SELECT REGEXP_EXTRACT(NULL, '(a+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-002: no match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(x+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-003: multiple matches, only first (leftmost) returned
        tdSql.query("SELECT REGEXP_EXTRACT('a1b2', '([0-9])')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1')

        # RXE-NULL-004: str=NULL with group_idx=0 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT(NULL, 'a+', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-005: explicit NULL group_idx → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)', NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-006: non-participating group in alternation → NULL
        # pattern '(a)|(b)' matches 'b' via group 2; group 1 did not participate
        tdSql.query("SELECT REGEXP_EXTRACT('b', '(a)|(b)', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-007: participating group 2 returns matched content
        tdSql.query("SELECT REGEXP_EXTRACT('b', '(a)|(b)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # -----------------------------------------------------------------
        # §5  Empty string scenarios
        # -----------------------------------------------------------------
        # RXE-EMPTY-001: capture group matches empty string → '' (not NULL)
        tdSql.query("SELECT REGEXP_EXTRACT('ac', '(b?)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        # RXE-EMPTY-002: empty input str with zero-length match → ''
        tdSql.query("SELECT REGEXP_EXTRACT('', '(a*)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        # -----------------------------------------------------------------
        # §6  Table queries — per-row scalar behavior
        # -----------------------------------------------------------------
        # RXE-TBL-001: extract numeric code — multiple rows match 'code=([0-9]+)';
        # verify row-by-row extraction for 42, 7, 0, and NULL propagation
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, 'code=([0-9]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '42')
        tdSql.checkData(1, 0, '7')
        tdSql.checkData(2, 0, '0')
        tdSql.checkData(3, 0, None)   # NULL row → NULL

        # RXE-TBL-002: NULL column row → NULL; non-NULL rows → extracted value
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, 'type=([A-Z_]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'DISK_FULL')
        tdSql.checkData(1, 0, 'LOW_MEM')
        tdSql.checkData(2, 0, 'OK')
        tdSql.checkData(3, 0, None)

        # RXE-TBL-003: empty table → 0 rows, no error
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS {dbname}.empty_t (ts TIMESTAMP, vc VARCHAR(64))")
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, '([0-9]+)') FROM {dbname}.empty_t")
        tdSql.checkRows(0)

        # -----------------------------------------------------------------
        # §7  WHERE clause
        # -----------------------------------------------------------------
        # RXE-WHERE-001: IS NOT NULL filters to rows with a match
        tdSql.query(f"SELECT vc FROM {dbname}.ct1 "
                    "WHERE REGEXP_EXTRACT(vc, 'code=([4-9][0-9]+)') IS NOT NULL "
                    "ORDER BY ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'code=42,type=DISK_FULL')

        # RXE-WHERE-002: equality on extracted scheme value
        tdSql.query(f"SELECT vc FROM {dbname}.ct2 "
                    "WHERE REGEXP_EXTRACT(vc, '(https?)://') = 'https' "
                    "ORDER BY ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'https://example.com')

        # -----------------------------------------------------------------
        # §8  NCHAR column: extraction result equals VARCHAR equivalent
        # -----------------------------------------------------------------
        # RXE-NCHAR-001: NCHAR input yields same extracted value as VARCHAR
        tdSql.query(f"SELECT REGEXP_EXTRACT(nc, 'code=([0-9]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '42')
        tdSql.checkData(1, 0, '7')
        tdSql.checkData(2, 0, '0')
        tdSql.checkData(3, 0, None)

        # -----------------------------------------------------------------
        # §9  Subquery with GROUP BY
        # -----------------------------------------------------------------
        # RXE-SUB-001: group by extracted URL scheme
        tdSql.query(f"""SELECT scheme, COUNT(*) AS cnt
            FROM (SELECT REGEXP_EXTRACT(vc, '(https?)://') AS scheme FROM {dbname}.ct2) t
            WHERE scheme IS NOT NULL
            GROUP BY scheme
            ORDER BY scheme""")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'http')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 'https')
        tdSql.checkData(1, 1, 1)

        # -----------------------------------------------------------------
        # §10  ERE features (character class, anchors, case sensitivity)
        # -----------------------------------------------------------------
        # RXE-RE-001: character class extracts decimal number
        tdSql.query("SELECT REGEXP_EXTRACT('v=3.14', '([0-9]+\\.[0-9]+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '3.14')

        # RXE-RE-002a: anchor ^ matches at start → 'a'
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '^(a)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        # RXE-RE-002b: anchor ^ requires position 0; 'x' blocks match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('xabc', '^(a)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-RE-003a: case-sensitive by default → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('ABC', '(abc)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-RE-003b: LOWER() enables case-insensitive extraction → 'abc'
        tdSql.query("SELECT REGEXP_EXTRACT(LOWER('ABC'), '(abc)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abc')

    def _check_error(self, dbname="db"):
        # -----------------------------------------------------------------
        # §11  Error cases
        # -----------------------------------------------------------------
        # RXE-ERR-001: too few arguments (1)
        tdSql.error("SELECT REGEXP_EXTRACT('abc')")

        # RXE-ERR-002: too many arguments (4)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', 1, 0)")

        # RXE-ERR-003: str is non-string type (INT column)
        tdSql.error(f"SELECT REGEXP_EXTRACT(iv, '([0-9]+)') FROM {dbname}.ct1")

        # RXE-ERR-004: pattern is a column reference (not a constant)
        tdSql.error(f"SELECT REGEXP_EXTRACT(vc, vc) FROM {dbname}.ct1")

        # RXE-ERR-005: negative group_idx → translation-phase error
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', -1)")

        # RXE-ERR-006: invalid regex (unmatched parenthesis)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b', 1)")

        # RXE-ERR-007: group_idx exceeds maximum (512)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', 513)")

    def _check_doc_examples(self):
        # -----------------------------------------------------------------
        # §12  Doc examples — verify the three queries from the user manual
        # -----------------------------------------------------------------
        # RXE-DOC-001: date string, group 1 → year
        tdSql.query("SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2026')

        # RXE-DOC-002: date string, group 0 → whole match
        tdSql.query("SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2026-04-22')

        # RXE-DOC-003: no match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('no-digits-here', '[0-9]+', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def all_test(self, dbname="db"):
        self._check_basic(dbname)
        self._check_error(dbname)
        self._check_doc_examples()

    def test_fun_sca_regexp_extract(self):
        """Fun: regexp_extract()

        1. regexp_extract default group_idx=1 returns first capture group
        2. regexp_extract group_idx=0 returns whole match substring
        3. regexp_extract with explicit group index (1, 2, out-of-range)
        4. regexp_extract NULL input and no-match return NULL
        5. regexp_extract capture group matching empty string returns ''
        6. regexp_extract on table columns with per-row scalar semantics
        7. regexp_extract in WHERE clause for row filtering
        8. regexp_extract on NCHAR column (return type NCHAR)
        9. regexp_extract in subquery with GROUP BY
        10. regexp_extract POSIX ERE features: character class, anchors, case sensitivity
        11. regexp_extract invalid parameter error cases (including group_idx > 512)
        12. regexp_extract user-manual doc examples

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-20 Stephen Created
        """
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self._create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self._insert_data()

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute("flush database db")

        tdLog.printNoPrefix("==========step4:after wal, all check again")
        self.all_test()
