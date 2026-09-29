from new_test_framework.utils import tdLog, tdSql, etool, tdCom

# ──────────────────────────────────────────────────────────────────────────────
# Database names used by this file
# ──────────────────────────────────────────────────────────────────────────────
DB_TCOL = "test_join_tcol"       # for t_col (non-primary timestamp) tests
DB_SORT = "test_join_tt_sort"    # for TIMETRUNCATE sort-compatibility tests
DB_CROSS_MS = "test_join_tt_sort_ms"
DB_CROSS_US = "test_join_tt_sort_us"


class TestJoinTsSort:
    """Consolidated test file for timestamp-related JOIN features.

    Covers:
    1. Non-primary timestamp column (t_col) as join condition — current behavior
       (sections A–H, from test_join_time_col.py)
    2. TIMETRUNCATE sort-compatibility optimization — expected behavior after optimization
       (groups 1–9, from test_join_timetruncate_sort.py)
    3. Additional unique scenarios converted from merged_all.in tests
       (section M)
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _check_desc_order(self, col_idx=0):
        assert tdSql.queryRows > 0, "no rows returned; cannot validate DESC order"
        prev_ts = None
        for i in range(tdSql.queryRows):
            cur_ts = tdSql.getData(i, col_idx)
            if prev_ts is not None:
                assert cur_ts <= prev_ts, (
                    f"Row {i}: ts={cur_ts} > prev ts={prev_ts}, not DESC order"
                )
            prev_ts = cur_ts

    def _check_asc_order(self, col_idx=0):
        assert tdSql.queryRows > 0, "no rows returned; cannot validate ASC order"
        prev_ts = None
        for i in range(tdSql.queryRows):
            cur_ts = tdSql.getData(i, col_idx)
            if prev_ts is not None:
                assert cur_ts >= prev_ts, (
                    f"Row {i}: ts={cur_ts} < prev ts={prev_ts}, not ASC order"
                )
            prev_ts = cur_ts

    # ══════════════════════════════════════════════════════════════════════════
    # Data preparation helpers
    # ══════════════════════════════════════════════════════════════════════════

    def _prepare_tcol_data(self):
        """Create DB_TCOL with stable stb(ts, t_col timestamp, val) + ct1/ct2."""
        tdSql.execute(f"drop database if exists {DB_TCOL};")
        tdSql.execute(f"create database {DB_TCOL} vgroups 2;")
        tdSql.execute(f"use {DB_TCOL};")
        tdSql.execute(
            "create stable stb (ts timestamp, t_col timestamp, val int) tags(g int);"
        )
        tdSql.execute("create table ct1 using stb tags(1);")
        tdSql.execute("create table ct2 using stb tags(2);")
        # ct1: 4 rows, t_col has a duplicate (10:00 appears at row 0 and row 3)
        tdSql.execute(
            "insert into ct1 values "
            "('2024-01-01 00:00:00', '2024-01-01 10:00:00', 1) "
            "('2024-01-01 01:00:00', '2024-01-01 11:00:00', 2) "
            "('2024-01-01 02:00:00', '2024-01-01 12:00:00', 3) "
            "('2024-01-01 03:00:00', '2024-01-01 10:00:00', 4);"
        )
        # ct2: 4 rows, t_col partially overlaps ct1
        tdSql.execute(
            "insert into ct2 values "
            "('2024-01-01 00:00:00', '2024-01-01 10:00:00', 10) "
            "('2024-01-01 01:00:00', '2024-01-01 11:00:00', 20) "
            "('2024-01-01 02:00:00', '2024-01-01 13:00:00', 30) "
            "('2024-01-01 04:00:00', '2024-01-01 12:00:00', 40);"
        )

    def _prepare_sort_data(self):
        """Create DB_SORT with stb1(ts, val) + ct1/ct2/ct3 (3 vgroup child tables)
        and regular tables t1/t2.  Timestamps span UTC/local day boundary."""
        tdSql.execute(f"drop database if exists {DB_SORT};")
        tdSql.execute(f"create database {DB_SORT} vgroups 2;")
        tdSql.execute(f"use {DB_SORT};")
        tdSql.execute(
            "create stable stb1 (ts timestamp, val int) tags(t1 int);"
        )
        tdSql.execute("create table ct1 using stb1 tags(1);")
        tdSql.execute("create table ct2 using stb1 tags(2);")
        tdSql.execute("create table ct3 using stb1 tags(3);")
        # ct1: 5 rows
        tdSql.execute(
            "insert into ct1 values "
            "('2024-01-01 00:00:00', 1) "
            "('2024-01-01 06:00:00', 2) "
            "('2024-01-01 12:00:00', 3) "
            "('2024-01-01 18:00:00', 4) "
            "('2024-01-02 00:00:00', 5);"
        )
        # ct2: 5 rows offset by 3h
        tdSql.execute(
            "insert into ct2 values "
            "('2024-01-01 03:00:00', 10) "
            "('2024-01-01 09:00:00', 20) "
            "('2024-01-01 15:00:00', 30) "
            "('2024-01-01 21:00:00', 40) "
            "('2024-01-02 06:00:00', 50);"
        )
        # ct3: 5 rows offset by 1h
        tdSql.execute(
            "insert into ct3 values "
            "('2024-01-01 01:00:00', 100) "
            "('2024-01-01 07:00:00', 200) "
            "('2024-01-01 13:00:00', 300) "
            "('2024-01-01 19:00:00', 400) "
            "('2024-01-02 03:00:00', 500);"
        )
        # regular tables
        tdSql.execute("create table t1 (ts timestamp, val int);")
        tdSql.execute("create table t2 (ts timestamp, val int);")
        tdSql.execute(
            "insert into t1 values "
            "('2024-01-01 00:00:00', 1) "
            "('2024-01-01 06:00:00', 2) "
            "('2024-01-01 12:00:00', 3) "
            "('2024-01-01 18:00:00', 4) "
            "('2024-01-02 00:00:00', 5);"
        )
        tdSql.execute(
            "insert into t2 values "
            "('2024-01-01 03:00:00', 10) "
            "('2024-01-01 09:00:00', 20) "
            "('2024-01-01 15:00:00', 30) "
            "('2024-01-01 21:00:00', 40) "
            "('2024-01-02 06:00:00', 50);"
        )

    def _daily_join_count(self, left_flag=None, right_flag=None):
        from collections import Counter

        def keys(flag):
            suffix = "" if flag is None else f", {flag}"
            tdSql.query(f"select timetruncate(ts, 1d{suffix}) from stb1")
            return Counter(row[0] for row in tdSql.queryResult if row[0] is not None)

        left = keys(left_flag)
        right = keys(right_flag)
        return sum(count * right.get(key, 0) for key, count in left.items())

    def _prepare_cross_precision_data(self):
        """Create two databases with different timestamp precision for cross-precision join."""
        tdSql.execute(f"drop database if exists {DB_CROSS_MS};")
        tdSql.execute(f"drop database if exists {DB_CROSS_US};")
        tdSql.execute(f"create database {DB_CROSS_MS} vgroups 2 precision 'ms';")
        tdSql.execute(f"create database {DB_CROSS_US} vgroups 2 precision 'us';")
        tdSql.execute(f"use {DB_CROSS_MS};")
        tdSql.execute("create table t_ms (ts timestamp, val int);")
        tdSql.execute(
            "insert into t_ms values "
            "('2024-01-01 00:00:00.000', 10) "
            "('2024-01-01 06:00:00.000', 30) "
            "('2024-01-02 00:00:00.000', 20);"
        )
        tdSql.execute(f"use {DB_CROSS_US};")
        tdSql.execute("create table t_us (ts timestamp, val int);")
        tdSql.execute(
            "insert into t_us values "
            "('2024-01-01 00:00:00.000000', 20) "
            "('2024-01-01 06:00:00.000000', 10) "
            "('2024-01-02 00:00:00.000000', 30);"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # A. Baseline: standard ts join (always works)
    # ══════════════════════════════════════════════════════════════════════════

    def test_A1_inner_join_on_ts(self):
        """Baseline: INNER JOIN ON a.ts = b.ts

        Verify that an inner join on primary timestamps returns three matching rows.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(3)

    def test_A2_left_join_on_ts(self):
        """Baseline: LEFT JOIN ON a.ts = b.ts

        Verify that a left join on primary timestamps preserves all four left rows.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a left join ct2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(4)

    def test_A3_full_join_on_ts(self):
        """Baseline: FULL JOIN ON a.ts = b.ts

        Verify that a full join on primary timestamps returns five rows including unmatched inputs.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a full join ct2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(5)

    def test_A4_asof_join_on_ts(self):
        """Baseline: LEFT ASOF JOIN ON a.ts >= b.ts

        Verify that an ASOF join on primary timestamps returns four left-side rows.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a left asof join ct2 b on a.ts >= b.ts;"
        )
        tdSql.checkRows(4)

    def test_A5_window_join_on_ts(self):
        """Baseline: LEFT WINDOW JOIN (default, no ON condition)

        Verify that a one-hour window on either side of each timestamp returns nine rows.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a left window join ct2 b "
            "window_offset(-1h, 1h);"
        )
        tdSql.checkRows(9)

    # ══════════════════════════════════════════════════════════════════════════
    # B. Table-to-table join with t_col as sole primary condition — all rejected
    # ══════════════════════════════════════════════════════════════════════════

    def test_B1_inner_join_on_tcol_supported(self):
        """INNER JOIN ON a.t_col = b.t_col — supported

        Verify that an inner equi-join on ordinary timestamp columns returns four rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b on a.t_col = b.t_col;"
        )
        tdSql.checkRows(4)

    def test_B2_left_join_on_tcol_supported(self):
        """LEFT JOIN ON a.t_col = b.t_col — supported

        Verify that a left equi-join on ordinary timestamp columns returns four rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a left join ct2 b on a.t_col = b.t_col;"
        )
        tdSql.checkRows(4)

    def test_B3_asof_join_on_tcol_rejected(self):
        """LEFT ASOF JOIN ON a.t_col >= b.t_col — rejected

        Verify that ASOF JOIN rejects an ordinary timestamp column as its time condition.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.error(
            "select a.ts, a.val, b.val from ct1 a left asof join ct2 b "
            "on a.t_col >= b.t_col;"
        )

    def test_B4_timetruncate_tcol_supported(self):
        """JOIN ON timetruncate(a.t_col) = timetruncate(b.t_col) is supported

        Truncate ordinary timestamp columns to hours and verify four matching join rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b "
            "on timetruncate(a.t_col,1h) = timetruncate(b.t_col,1h) "
            "order by a.ts, a.val, b.val;"
        )
        tdSql.checkRows(4)

    # ══════════════════════════════════════════════════════════════════════════
    # C. Table-to-table: t_col as secondary condition (ts is primary)
    # ══════════════════════════════════════════════════════════════════════════

    def test_C1_ts_primary_tcol_equal_secondary(self):
        """JOIN ON a.ts = b.ts AND a.t_col = b.t_col — ts primary, t_col filters

        Combine primary-timestamp equality with ordinary timestamp equality and verify two matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b "
            "on a.ts = b.ts and a.t_col = b.t_col;"
        )
        # ts matches at 00:00/01:00/02:00; t_col match: 00:00(10=10)✓ 01:00(11=11)✓ 02:00(12≠13)✗
        tdSql.checkRows(2)

    def test_C2_ts_primary_tcol_gt_secondary(self):
        """JOIN ON a.ts = b.ts AND a.t_col > b.t_col — inequality secondary condition

        Combine primary-timestamp equality with an ordinary timestamp inequality and verify no matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b "
            "on a.ts = b.ts and a.t_col > b.t_col;"
        )
        # 00:00(10>10?no) 01:00(11>11?no) 02:00(12>13?no) => 0 rows
        tdSql.checkRows(0)

    # ══════════════════════════════════════════════════════════════════════════
    # D. WINDOW JOIN with t_col as ON condition (works for table-to-table)
    # ══════════════════════════════════════════════════════════════════════════

    def test_D1_window_join_on_tcol(self):
        """LEFT WINDOW JOIN ON a.t_col = b.t_col — accepted, t_col match

        Apply ordinary timestamp equality inside a one-hour window and verify four rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.t_col, a.val, b.ts, b.t_col, b.val "
            "from ct1 a left window join ct2 b "
            "on a.t_col = b.t_col window_offset(-1h, 1h);"
        )
        tdSql.checkRows(4)

    def test_D2_window_join_on_tcol_wider_offset(self):
        """LEFT WINDOW JOIN ON t_col with wider offset

        Expand the window to two hours while retaining the ordinary-column filter and four rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.t_col, a.val, b.ts, b.t_col, b.val "
            "from ct1 a left window join ct2 b "
            "on a.t_col = b.t_col window_offset(-2h, 2h);"
        )
        tdSql.checkRows(4)

    def test_D3_window_join_on_tcol_zero_offset(self):
        """LEFT WINDOW JOIN ON t_col with zero offset (exact match)

        Use a zero-width window with ordinary timestamp equality and verify four rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.t_col, a.val, b.ts, b.t_col, b.val "
            "from ct1 a left window join ct2 b "
            "on a.t_col = b.t_col window_offset(0h, 0h);"
        )
        tdSql.checkRows(4)

    def test_D4_window_join_on_tcol_with_null(self):
        """LEFT WINDOW JOIN ON t_col when t_col has NULL values

        Include a NULL ordinary timestamp and verify that the left window join preserves both left rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.execute("create table ct3 using stb tags(3);")
        tdSql.execute(
            "insert into ct3 values "
            "('2024-01-01 00:00:00', NULL, 100) "
            "('2024-01-01 01:00:00', '2024-01-01 10:00:00', 200);"
        )
        tdSql.query(
            "select a.ts, a.t_col, a.val, b.val "
            "from ct3 a left window join ct2 b "
            "on a.t_col = b.t_col window_offset(-1h, 1h);"
        )
        tdSql.checkRows(2)

    def test_D5_window_join_on_ts_with_on_rejected(self):
        """LEFT WINDOW JOIN ON a.ts = b.ts — rejected (ts ON condition not allowed)

        Verify that WINDOW JOIN rejects explicit primary-timestamp equality in its ON clause.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.error(
            "select a.ts, a.val, b.val from ct1 a left window join ct2 b "
            "on a.ts = b.ts window_offset(-1h, 1h);"
        )

    def test_D6_window_join_on_timetruncate_tcol_rejected(self):
        """LEFT WINDOW JOIN ON timetruncate(t_col) — rejected

        Verify that WINDOW JOIN rejects TIMETRUNCATE expressions in its ON clause.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.error(
            "select a.ts, a.val, b.val from ct1 a left window join ct2 b "
            "on timetruncate(a.t_col,1h) = timetruncate(b.t_col,1h) "
            "window_offset(-1h, 1h);"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # E. Subquery join with fallback timeline — all rejected for non-WINDOW
    # ══════════════════════════════════════════════════════════════════════════

    def test_E1_subquery_no_ts_join_on_tcol_supported(self):
        """Subquery without ts, JOIN ON t_col — supported

        Join subqueries projecting only ordinary timestamps and values and verify four matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.t_col, a.val, b.val "
            "from (select t_col, val from ct1) a "
            "join (select t_col, val from ct2) b "
            "on a.t_col = b.t_col;"
        )
        tdSql.checkRows(4)

    def test_E2_subquery_no_ts_timetruncate_tcol_supported(self):
        """Subquery without ts supports JOIN ON timetruncate(t_col)

        Join hour-truncated ordinary timestamps without projecting the primary timestamp and verify four matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.t_col, a.val, b.val "
            "from (select t_col, val from ct1) a "
            "join (select t_col, val from ct2) b "
            "on timetruncate(a.t_col,1h) = timetruncate(b.t_col,1h) "
            "order by a.t_col, a.val, b.val;"
        )
        tdSql.checkRows(4)

    def test_E3_subquery_with_ts_join_on_tcol_supported(self):
        """Subquery with ts in projection, but JOIN ON t_col — supported

        Project primary timestamps but join on ordinary timestamps and verify four matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.t_col, a.val, b.val "
            "from (select ts, t_col, val from ct1) a "
            "join (select ts, t_col, val from ct2) b "
            "on a.t_col = b.t_col;"
        )
        tdSql.checkRows(4)

    def test_E4_mixed_subquery_table_on_tcol_supported(self):
        """One side subquery (no ts), other side table, ON t_col — supported

        Join a subquery without a primary timestamp to a child table and verify four matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.t_col, a.val, b.val "
            "from (select t_col, val from ct1) a "
            "join ct2 b on a.t_col = b.t_col;"
        )
        tdSql.checkRows(4)

    def test_E5_subquery_window_join_on_tcol_rejected(self):
        """Subquery WINDOW JOIN ON t_col — rejected (WINDOW only between tables)

        Verify that WINDOW JOIN rejects subquery inputs with an ordinary timestamp condition.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.error(
            "select a.t_col, a.val, b.val "
            "from (select t_col, val from ct1) a "
            "left window join (select t_col, val from ct2) b "
            "on a.t_col = b.t_col window_offset(-1h, 1h);"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # F. Supertable join with t_col
    # ══════════════════════════════════════════════════════════════════════════

    def test_F1_supertable_join_on_ts(self):
        """Baseline: supertable INNER JOIN ON ts with tag filter

        Filter supertable inputs by tag and verify three primary-timestamp matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from stb a join stb b "
            "on a.ts = b.ts and a.g = 1 and b.g = 2;"
        )
        tdSql.checkRows(3)

    def test_F2_supertable_join_on_tcol_supported(self):
        """Supertable JOIN ON t_col — supported

        Filter supertable inputs by tag and verify four ordinary-timestamp matches.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from stb a join stb b "
            "on a.t_col = b.t_col and a.g = 1 and b.g = 2;"
        )
        tdSql.checkRows(4)

    # ══════════════════════════════════════════════════════════════════════════
    # G. Fallback timeline verification (confirms timeline works without join)
    # ══════════════════════════════════════════════════════════════════════════

    def test_G1_fallback_timeline_first_last(self):
        """Subquery without ts: first()/last() work via fallback timeline

        Query first and last values from a subquery without ts and verify the fallback timeline endpoints.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select first(val), last(val) from (select t_col, val from ct1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 3)

    def test_G2_fallback_timeline_order_by(self):
        """Subquery without ts: ORDER BY t_col works

        Sort a subquery without ts by its ordinary timestamp column and verify four result rows.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select t_col, val from (select t_col, val from ct1) order by t_col;"
        )
        tdSql.checkRows(4)

    def test_G3_tcol_stable_results_ans(self):
        """Stable t_col JOIN and fallback timeline results are covered by ans file

        Compare ordinary-timestamp join and fallback timeline results with the expected answer file.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Added stable result-file coverage
        """
        self._prepare_tcol_data()
        sql_file = etool.curFile(__file__, "in/test_join_ts_sort_tcol_results.in")
        ans_file = etool.curFile(__file__, "ans/test_join_ts_sort_tcol_results.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, "test_join_ts_sort_tcol_results")

    # ══════════════════════════════════════════════════════════════════════════
    # H. Mixed conditions edge cases
    # ══════════════════════════════════════════════════════════════════════════

    def test_H1_ts_plus_timetruncate_tcol_supported(self):
        """JOIN ON a.ts = b.ts AND timetruncate(a.t_col,1d) = timetruncate(b.t_col,1d)

        — primary timestamp key plus secondary timestamp residual condition

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
        """
        self._prepare_tcol_data()
        tdSql.query(
            "select a.ts, a.val, b.val from ct1 a join ct2 b "
            "on a.ts = b.ts and "
            "timetruncate(a.t_col,1d) = timetruncate(b.t_col,1d);"
        )
        tdSql.checkRows(3)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 1: TIMETRUNCATE tz mismatch (FS 场景1)
    # ══════════════════════════════════════════════════════════════════════════

    def test_1_1_baseline_order_by_ts(self):
        """TC-1.1 Baseline: ORDER BY ts, result N1 used as reference for Group 1

        Establish a positive join-count baseline using subqueries ordered by their primary timestamps.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1);"
        )
        tdSql.checkRows(1)
        assert tdSql.getData(0, 0) > 0, "Baseline N1 must be > 0"

    def test_1_2_order_by_tt_tz0_join_on_tz1(self):
        """TC-1.2 ORDER BY TIMETRUNCATE(ts,1d,0) + JOIN ON tz=1 — correct result

        Compare timezone-0 truncation ordering with raw timestamp ordering when the join uses timezone 1.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 0)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 0)) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_1_3_order_by_tt_tz1_join_on_tz0(self):
        """TC-1.3 ORDER BY TIMETRUNCATE(ts,1d,1) + JOIN ON tz=0 — correct result

        Compare timezone-1 truncation ordering with an unordered baseline when the join uses timezone 0.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1) a "
            "join "
            "(select ts, val from stb1) b "
            "on timetruncate(a.ts, 1d, 0) = timetruncate(b.ts, 1d, 0);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 1)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 1)) b "
            "on timetruncate(a.ts, 1d, 0) = timetruncate(b.ts, 1d, 0);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 2: ON condition two-sides tz different (FS 场景2)
    # ══════════════════════════════════════════════════════════════════════════

    def test_2_1_cross_tz_on_condition(self):
        """TC-2.1 ON TIMETRUNCATE(a.ts,1d,0) = TIMETRUNCATE(b.ts,1d,1) — correct result

        Execute a join whose two TIMETRUNCATE keys use different timezone flags and verify one aggregate row.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        expected_count = self._daily_join_count(0, 1)
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 0)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 1)) b "
            "on timetruncate(a.ts, 1d, 0) = timetruncate(b.ts, 1d, 1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected_count)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 3: Granularity mismatch (FS 场景3)
    # ══════════════════════════════════════════════════════════════════════════

    def test_3_1_order_by_1w_join_on_1d(self):
        """TC-3.1 ORDER BY TIMETRUNCATE(ts,1w) + JOIN ON 1d — correct result

        Compare weekly input ordering with raw timestamp ordering for joins on daily timestamp buckets.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by timetruncate(ts, 1w)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1w)) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_3_2_order_by_1h_join_on_1d(self):
        """TC-3.2 ORDER BY TIMETRUNCATE(ts,1h) + JOIN ON 1d — 1h < 1d, naturally correct

        Compare hourly input ordering with raw timestamp ordering for joins on daily timestamp buckets.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by timetruncate(ts, 1h)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1h)) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 4: ts scalar expression ORDER BY (FS 场景4)
    # ══════════════════════════════════════════════════════════════════════════

    def test_4_1_order_by_ts_plus_1(self):
        """TC-4.1 ORDER BY ts + 1 — correct (was accidentally correct before)

        Compare input ordering by ts + 1 with raw timestamp ordering and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts + 1) a "
            "join "
            "(select ts, val from stb1 order by ts + 1) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_4_2_order_by_cast_ts_bigint(self):
        """TC-4.2 ORDER BY CAST(ts AS BIGINT) — no longer rejected after optimization

        Compare input ordering by CAST(ts AS BIGINT) with raw timestamp ordering and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by cast(ts as bigint)) a "
            "join "
            "(select ts, val from stb1 order by cast(ts as bigint)) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_4_3_order_by_ts_mul_1(self):
        """TC-4.3 ORDER BY ts * 1 — no longer rejected after optimization

        Compare input ordering by ts * 1 with raw timestamp ordering and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts * 1) a "
            "join "
            "(select ts, val from stb1 order by ts * 1) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 5: Non-ts column ORDER BY (FS 场景5)
    # ══════════════════════════════════════════════════════════════════════════

    def test_5_1_order_by_val(self):
        """TC-5.1 ORDER BY val — no longer rejected after optimization

        Compare ascending value ordering on both inputs with timestamp ordering and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by val) a "
            "join "
            "(select ts, val from stb1 order by val) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_5_2_order_by_val_desc(self):
        """TC-5.2 ORDER BY val DESC — no longer rejected

        Compare descending value ordering on both inputs with timestamp ordering and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by val desc) a "
            "join "
            "(select ts, val from stb1 order by val desc) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_5_3_mixed_one_side_val_other_ts(self):
        """TC-5.3 Mixed: one side ORDER BY val, other ORDER BY ts

        Order one input by value and the other by timestamp and compare the count with the timestamp baseline.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by val) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_5_4_mixed_val_desc_and_ts(self):
        """TC-5.4 Mixed: one side ORDER BY val DESC, other ORDER BY ts

        Mix descending value ordering and timestamp ordering and compare the count with the timestamp baseline.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by val desc) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from stb1 order by val desc) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d) "
            "order by a.ts;"
        )
        self._check_asc_order(0)

    # ══════════════════════════════════════════════════════════════════════════
    # Group 6: Sort direction (FS §4.4)
    # ══════════════════════════════════════════════════════════════════════════

    def test_6_1_outer_order_by_desc_limit(self):
        """TC-6.1 Outer ORDER BY ts DESC LIMIT — result rows in DESC order

        Apply descending timestamp ordering and LIMIT 5 after the join and check row count and ordering.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 0)) a "
            "join "
            "(select ts, val from stb1 order by timetruncate(ts, 1d, 0)) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1) "
            "order by a.ts desc "
            "limit 5;"
        )
        tdSql.checkRows(5)
        self._check_desc_order(0)

    def test_6_2_outer_no_order_limit(self):
        """TC-6.2 No outer ORDER BY + LIMIT 10 — returns detail rows without error

        Apply LIMIT 10 without outer ordering and verify that the join returns at most ten rows.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from stb1 order by val) a "
            "join "
            "(select ts, val from stb1 order by val) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d) "
            "limit 10;"
        )
        assert tdSql.queryRows <= 10

    def test_6_3_outer_order_desc_overrides_child_val_order(self):
        """TC-6.3 Outer ORDER BY DESC drives both JOIN input sort directions

        Order inputs by value, then verify that outer timestamp ordering returns five descending rows.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Added for parent inputTsOrder priority
        """
        self._prepare_sort_data()
        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from stb1 order by val) a "
            "join "
            "(select ts, val from stb1 order by val) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d) "
            "order by a.ts desc "
            "limit 5;"
        )
        tdSql.checkRows(5)
        self._check_desc_order(0)

    def test_6_3_1_inner_desc_limit_outer_order_limit(self):
        """TC-6.3.1 Inner DESC LIMIT is preserved before outer ORDER BY LIMIT

        Limit descending subquery inputs before the join and verify the endpoints selected by outer ordering.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-15 Added for opposite inner/outer sort direction with LIMIT
        """
        self._prepare_sort_data()
        tdSql.query("select count(*) from t1;")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from t2;")
        tdSql.checkData(0, 0, 5)

        # t1 DESC LIMIT 2 must keep the last two t1 rows before JOIN.
        # The outer ORDER BY only chooses opposite endpoints from that limited set.
        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from t1 order by ts desc limit 2) a "
            "join "
            "(select ts, val from t2 order by ts desc limit 2) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d) "
            "order by a.ts asc "
            "limit 1;"
        )
        tdSql.checkRows(1)
        asc_ts = tdSql.getData(0, 0)
        tdSql.checkData(0, 0, "2024-01-01 18:00:00")

        tdSql.query(
            "select a.ts, b.ts from "
            "(select ts, val from t1 order by ts desc limit 2) a "
            "join "
            "(select ts, val from t2 order by ts desc limit 2) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d) "
            "order by a.ts desc "
            "limit 1;"
        )
        tdSql.checkRows(1)
        desc_ts = tdSql.getData(0, 0)
        tdSql.checkData(0, 0, "2024-01-02 00:00:00")

        assert asc_ts < desc_ts, (
            f"Outer ORDER BY LIMIT did not select opposite endpoints: "
            f"asc_ts={asc_ts}, desc_ts={desc_ts}"
        )

    def test_6_4_explain_join_sort_uses_raw_ts(self):
        """TC-6.4 EXPLAIN result for JOIN input sort is covered by ans file

        Compare EXPLAIN output for join input sorting with the expected plan answer file.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Added for DS EXPLAIN coverage
        """
        self._prepare_sort_data()
        sql_file = etool.curFile(__file__, "in/test_join_ts_sort_explain.in")
        ans_file = etool.curFile(__file__, "ans/test_join_ts_sort_explain.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, "test_join_ts_sort_explain")

    def test_6_5_stable_sort_results_ans(self):
        """Stable TIMETRUNCATE JOIN sort results are covered by ans file

        Compare TIMETRUNCATE join sorting results with the expected answer file.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Added stable result-file coverage
        """
        self._prepare_sort_data()
        sql_file = etool.curFile(__file__, "in/test_join_ts_sort_results.in")
        ans_file = etool.curFile(__file__, "ans/test_join_ts_sort_results.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, "test_join_ts_sort_results")

    # Groups 7 (ASOF) and 8 (WINDOW) with subquery inputs are out of scope for this
    # optimization. checkJoinTable() (parTranslater.c) requires both ASOF/WINDOW sides
    # to be QUERY_NODE_REAL_TABLE; lifting that restriction is a separate future task.
    # The FS §4.3 subquery example has been corrected to reflect this scope boundary.

    def test_7_1_asof_subquery_input_rejected(self):
        """TC-7.1 ASOF JOIN with subquery input remains out of scope

        Verify that ASOF JOIN rejects subqueries ordered by value or timestamp as inputs.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in reject case
        """
        self._prepare_sort_data()
        tdSql.error(
            "select count(*) from "
            "(select ts, val from t1 order by val) a "
            "left asof join "
            "(select ts, val from t2 order by ts) b "
            "on a.ts >= b.ts;"
        )

    def test_8_1_window_subquery_input_rejected(self):
        """TC-8.1 WINDOW JOIN with subquery input remains out of scope

        Verify that WINDOW JOIN rejects ordered subquery inputs even with a primary-timestamp condition.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in reject case
        """
        self._prepare_sort_data()
        tdSql.error(
            "select count(*) from "
            "(select ts, val from t1 order by val) a "
            "left window join "
            "(select ts, val from t2 order by ts) b "
            "on a.ts = b.ts window_offset(-1h, 1h);"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Group 9: Regression (FS §4.2 unchanged scenarios)
    # ══════════════════════════════════════════════════════════════════════════

    def test_9_1_no_order_by_supertable(self):
        """TC-9.1 No ORDER BY, multi-vgroup supertable — result unchanged by optimization

        Compare unordered supertable inputs with timestamp-ordered inputs and verify equal join counts.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1) a "
            "join "
            "(select ts, val from stb1) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_9_2_order_by_ts_regression(self):
        """TC-9.2 ORDER BY ts — regression, result unchanged

        Execute a daily TIMETRUNCATE join over timestamp-ordered supertable inputs and verify one aggregate row.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        expected_count = self._daily_join_count()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected_count)

    def test_order_by_alias_collision_is_rejected(self):
        """Duplicate projection aliases must not select an arbitrary sort key.

        Verify that ORDER BY reports an error when a timestamp and a value column
        share the same projection alias.

        Since: v3.4.2.0
        """
        self._prepare_sort_data()
        tdSql.error(
            "select ts as dup, val as dup from ct1 order by dup"
        )

    def test_self_join_primary_timestamp_aliases(self):
        """Self-join aliases must remain bound to their respective input sides.

        Join two subqueries that alias their primary timestamps as join_ts and
        check that the self-join returns all five matching rows.

        Since: v3.4.2.0
        """
        self._prepare_sort_data()
        tdSql.query(
            "select a.join_ts, b.join_ts from "
            "(select ts as join_ts, val from ct1 order by join_ts) a join "
            "(select ts as join_ts, val from ct1 order by join_ts) b "
            "on a.join_ts = b.join_ts order by a.join_ts"
        )
        tdSql.checkRows(5)

    def test_expression_alias_is_not_primary_timestamp_order(self):
        """An expression alias must not be treated as a raw primary timestamp.

        Join two subqueries ordered by an alias of ts + 0 and check that all five
        rows match without relying on raw primary-timestamp ordering.

        Since: v3.4.2.0
        """
        self._prepare_sort_data()
        tdSql.query(
            "select a.expr_ts, b.expr_ts from "
            "(select ts + 0 as expr_ts, val from ct1 order by expr_ts) a join "
            "(select ts + 0 as expr_ts, val from ct1 order by expr_ts) b "
            "on a.expr_ts = b.expr_ts order by a.expr_ts"
        )
        tdSql.checkRows(5)

    def test_9_3_plain_ts_join_regression(self):
        """TC-9.3 Plain a.ts = b.ts join — 15 self-match rows (3 child tables x 5 rows)

        Verify that a primary-timestamp self-join returns fifteen matches across three child tables.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        # stb1 has 15 unique timestamps; self-join on ts = ts yields 15 rows
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on a.ts = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 15)

    def test_9_4_single_table_join_regression(self):
        """TC-9.4 Single regular tables — naturally sorted, optimization does not affect

        Compare mismatched truncation timezones with raw timestamp ordering for two regular tables.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by ts) a "
            "join "
            "(select ts, val from t2 order by ts) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by timetruncate(ts, 1d, 0)) a "
            "join "
            "(select ts, val from t2 order by timetruncate(ts, 1d, 0)) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

    def test_9_5_hash_join_not_affected(self):
        """TC-9.5 hash_join hint — sort order is irrelevant, result is correct

        Hash join (when the hint is accepted) does not rely on sort order, so
        ORDER BY val subqueries should produce the same count as ORDER BY ts.
        If the hint is silently ignored, the optimized merge-join path is taken
        instead and the result is still correct.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Created for JOIN TIMETRUNCATE sort optimization
        """
        self._prepare_sort_data()
        # Reference count with ORDER BY ts
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from stb1 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        # hash_join hint + ORDER BY val — result must equal baseline regardless
        # of whether the hint is accepted or silently ignored
        tdSql.query(
            "select /*+ hash_join(a, b) */ count(*) from "
            "(select ts, val from stb1 order by val) a "
            "join "
            "(select ts, val from stb1 order by val) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)

        # The hint may be ignored by the optimizer path used in this suite.
        # This regression only requires the query result to remain stable.

    def test_9_6_cross_precision_subquery_order_by_val(self):
        """TC-9.6 Cross-precision join with subquery ORDER BY val stays stable

        Compare cross-precision joins with subquery ordering and limits against the expected answer file.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-16 Added cross-precision ORDER BY/LIMIT result coverage
        """
        self._prepare_cross_precision_data()

        sql_file = etool.curFile(__file__, "in/test_join_ts_sort_cross_precision.in")
        ans_file = etool.curFile(__file__, "ans/test_join_ts_sort_cross_precision.ans")
        tdCom.compare_testcase_result(
            sql_file, ans_file, "test_join_ts_sort_cross_precision"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # M. Additional scenarios converted from merged_all.in
    # ══════════════════════════════════════════════════════════════════════════

    def test_M1_upgrade_to_ts_row_detail(self):
        """TC-M1 Verify per-row data after tz-mismatch fix (ORDER BY vs ON tz differ)

        After optimization the sort key is upgraded to ts, so result rows must match
        those from an equivalent ORDER BY ts + same ON condition query.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Converted from merged_all.in test_upgrade_to_ts
        """
        self._prepare_sort_data()
        # Reference: ORDER BY ts + ON tz=1 same-side
        tdSql.query(
            "select a.ts, a.val, b.val from "
            "(select ts, val from t1 order by ts) a "
            "join "
            "(select ts, val from t2 order by ts) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1) "
            "order by a.ts, b.ts;"
        )
        ref_rows = tdSql.queryRows
        ref_data = [(tdSql.getData(i, 0), tdSql.getData(i, 1), tdSql.getData(i, 2))
                    for i in range(ref_rows)]

        # Test: ORDER BY tt(tz=0) + ON tz=1 cross-tz
        tdSql.query(
            "select a.ts, a.val, b.val from "
            "(select ts, val from t1 order by timetruncate(ts, 1d, 0)) a "
            "join "
            "(select ts, val from t2 order by timetruncate(ts, 1d, 1)) b "
            "on timetruncate(a.ts, 1d, 1) = timetruncate(b.ts, 1d, 1) "
            "order by a.ts, b.ts;"
        )
        tdSql.checkRows(ref_rows)
        for i in range(ref_rows):
            tdSql.checkData(i, 0, ref_data[i][0])
            tdSql.checkData(i, 1, ref_data[i][1])
            tdSql.checkData(i, 2, ref_data[i][2])

    def test_M2_dir_limit_desc_subquery(self):
        """TC-M2 Subquery ORDER BY ts DESC LIMIT N + join — direction preserved

        Execute a join over two descending subqueries limited to two rows and verify one aggregate row.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Converted from merged_all.in test_dir_limit_extra
        """
        self._prepare_sort_data()
        # Both sides DESC LIMIT 2 — optimization inserts ts sort preserving DESC
        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by ts desc limit 2) a "
            "join "
            "(select ts, val from t2 order by ts desc limit 2) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)

    def test_M2_1_dir_limit_mixed_desc_asc_subquery(self):
        """TC-M2.1 Mixed DESC/ASC subquery LIMIT + join — result set preserved

        Combine descending and ascending subquery limits and verify a join count of two.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in mixed direction LIMIT case
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by ts desc limit 2) a "
            "join "
            "(select ts, val from t2 order by ts limit 2) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def test_M3_groupby_subquery_join(self):
        """TC-M3 GROUP BY subquery join — aggregated subquery can participate in join

        Aggregate each input by daily timestamp buckets, join the aggregates, and verify one count row.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Converted from merged_all.in test_groupby_join
        """
        self._prepare_sort_data()
        # Aggregate to day-level, then join on day.
        # Note: TDengine GROUP BY does not accept aliases defined in the same SELECT;
        # use the full expression instead.
        tdSql.query(
            "select count(*) from "
            "(select timetruncate(ts, 1d) as gts, sum(val) as s "
            " from ct1 group by timetruncate(ts, 1d) order by gts) a "
            "join "
            "(select timetruncate(ts, 1d) as gts, sum(val) as s "
            " from ct2 group by timetruncate(ts, 1d) order by gts) b "
            "on a.gts = b.gts;"
        )
        tdSql.checkRows(1)

    def test_M4_no_ts_condition_in_on_supported(self):
        """TC-M4 JOIN ON x.val = y.val — Hash Join accepts column equality

        Verify that ordinary value equality is accepted as a join condition without primary-timestamp equality.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-30 Converted from merged_all.in test_non_child_time_in_on
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from ct1 order by ts) a "
            "join "
            "(select ts, val from ct2 order by ts) b "
            "on a.val = b.val;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    def test_M5_supertable_to_child_timetruncate_join(self):
        """TC-M5 Supertable subquery joins child-table subquery on TIMETRUNCATE(ts)

        Join a supertable subquery to a child-table subquery on daily buckets and verify a positive count.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in supertable/child case
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from stb1 order by ts) a "
            "join "
            "(select ts, val from ct2 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        assert tdSql.getData(0, 0) > 0

    def test_M6_mixed_val_and_cast_order(self):
        """TC-M6 Mixed ORDER BY val and ORDER BY CAST(ts AS BIGINT)

        Mix value ordering with CAST timestamp ordering and compare the join count with the timestamp baseline.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in mixed expression case
        """
        self._prepare_sort_data()
        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by ts) a "
            "join "
            "(select ts, val from t2 order by ts) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        baseline = tdSql.getData(0, 0)

        tdSql.query(
            "select count(*) from "
            "(select ts, val from t1 order by val) a "
            "join "
            "(select ts, val from t2 order by cast(ts as bigint)) b "
            "on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, baseline)
