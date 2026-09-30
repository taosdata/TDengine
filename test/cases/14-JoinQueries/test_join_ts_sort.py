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

    def _check_col(self, sql, col, expected):
        """Run sql, assert column `col` equals `expected` and the row count."""
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, value in enumerate(expected):
            tdSql.checkData(row, col, value)

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
    # E. Subquery JOIN timelines
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

    def test_E5_subquery_window_join_on_ordered_tcol(self):
        """Subquery WINDOW JOIN ignores NULL preserved-side effective time

        Verify that NULL effective times are excluded from ordered subqueries.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-29 Created for join time-column coverage
            - 2026-09-04 Tony Zhang Updated temporal JOIN subquery coverage
        """
        self._prepare_tcol_data()
        tdSql.execute(
            "insert into ct1 values "
            "('2024-01-01 05:00:00', NULL, 5);"
        )
        tdSql.query(
            "select a.t_col, a.val, b.val "
            "from (select t_col, val from ct1 order by t_col) a "
            "left window join "
            "(select t_col, val from ct2 order by t_col) b "
            "window_offset(0h, 0h);"
        )
        tdSql.checkRows(4)
        for row in range(tdSql.queryRows):
            assert tdSql.getData(row, 0) is not None

    def test_E6_subquery_order_alias_conflict(self):
        """Temporal JOIN sorts the selected time despite an alias conflict

        Verify that an alias conflict does not change the selected join time.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Tony Zhang Added temporal JOIN ordering coverage
            - 2026-09-22 Expect the primary column despite an alias conflict
        """
        self._prepare_tcol_data()
        for peer in ("ct2 b", "(ct2 b join ct2 p on b.ts = p.ts)"):
            tdSql.query(
                "select a.val, b.val from "
                "(select t_col as ts, ts as primary_ts, val "
                f"from ct1 order by ct1.ts) a left asof join {peer} "
                "order by a.val;"
            )
            tdSql.checkRows(4)
            for row, expected in enumerate((10, 20, 30, 30)):
                tdSql.checkData(row, 0, row + 1)
                tdSql.checkData(row, 1, expected)

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

    def test_7_1_asof_subquery_input(self):
        """TC-7.1 ASOF JOIN accepts ordered subquery inputs

        Check LEFT and RIGHT ASOF JOIN results with ordered subqueries.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in reject case
            - 2026-09-04 Tony Zhang Updated temporal JOIN subquery coverage
        """
        self._prepare_sort_data()
        self._check_col(
            "select a.ts, a.val, b.val from "
            "(select ts, val from t1 order by ts) a "
            "left asof join "
            "(select ts, val from t2 order by ts) b "
            "order by a.ts;",
            2,
            (None, 10, 20, 30, 40),
        )
        self._check_col(
            "select l.ts, l.val, r.val from t1 l left asof join "
            "(select ts, val from t2 order by ts desc) r "
            "order by l.ts;",
            2,
            (None, 10, 20, 30, 40),
        )
        self._check_col(
            "select a.val, b.val from "
            "(select ts, val from t1 order by ts) a "
            "right asof join "
            "(select ts, val from t2 order by ts) b "
            "order by b.ts;",
            0,
            (1, 2, 3, 4, 5),
        )

    def test_8_1_window_subquery_input(self):
        """TC-8.1 WINDOW JOIN accepts ordered subquery inputs

        Check LEFT and RIGHT WINDOW JOIN results with ordered subqueries.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-09 Converted from merged_all.in reject case
            - 2026-09-04 Tony Zhang Updated temporal JOIN subquery coverage
        """
        self._prepare_sort_data()
        self._check_col(
            "select a.ts, a.val, b.val from "
            "(select ts, val from t1 order by ts) a "
            "left window join "
            "(select ts, val from t2 order by ts) b "
            "window_offset(-2h, 4h) order by a.ts;",
            2,
            (10, 20, 30, 40, None),
        )
        self._check_col(
            "select a.val, b.val from "
            "(select ts, val from t1 order by ts) a "
            "right window join "
            "(select ts, val from t2 order by ts) b "
            "window_offset(0h, 0h) order by b.ts;",
            0,
            (None,) * 5,
        )

    def test_8_2_virtual_table_temporal_join_input(self):
        """ASOF/WINDOW JOIN accepts virtual-table inputs.

        Check virtual tables on either side with table and subquery inputs.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Tony Zhang Added virtual-table JOIN coverage
        """
        self._prepare_sort_data()
        tdSql.execute(
            "create vtable v_t1("
            "ts timestamp, val int from t1.val);"
        )
        tdSql.execute(
            "create vtable v_t2("
            "ts timestamp, val int from t2.val);"
        )

        self._check_col(
            "select v.ts, v.val, r.val from v_t1 v "
            "left asof join t2 r order by v.ts;",
            2,
            (None, 10, 20, 30, 40),
        )
        self._check_col(
            "select l.val, v.val from t1 l "
            "right asof join v_t2 v order by v.ts;",
            0,
            (1, 2, 3, 4, 5),
        )
        self._check_col(
            "select v.ts, v.val, r.val from v_t1 v left window join "
            "(select ts, val from t2 order by ts) r "
            "window_offset(-2h, 4h) order by v.ts;",
            2,
            (10, 20, 30, 40, None),
        )
        self._check_col(
            "select l.val, v.val from "
            "(select ts, val from t1 order by ts) l "
            "right window join v_t2 v "
            "window_offset(0h, 0h) order by v.ts;",
            0,
            (None,) * 5,
        )

    def test_8_3_window_time_columns(self):
        """WINDOW_TIME selects ordered timestamp columns for WINDOW JOIN.

        Check explicit timestamp columns from tables and virtual tables.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Tony Zhang Added WINDOW_TIME column coverage
        """
        self._prepare_sort_data()
        self._check_col(
            "select l.ts, l.val, r.val from t1 l left window join t2 r "
            "window_time(l.ts, r.ts) window_offset(-2h, 4h) "
            "order by l.ts;",
            2,
            (10, 20, 30, 40, None),
        )

        tdSql.execute(
            "create vtable v_t1("
            "ts timestamp, val int from t1.val);"
        )
        self._check_col(
            "select v.ts, v.val, r.val from v_t1 v "
            "left window join t2 r window_time(v.ts, r.ts) "
            "window_offset(-2h, 4h) order by v.ts;",
            2,
            (10, 20, 30, 40, None),
        )

        self._prepare_tcol_data()
        self._check_col(
            "select l.val, r.val from "
            "(select ts source_ts, t_col event_ts, val from ct1 "
            "order by event_ts) l left window join "
            "(select ts source_ts, t_col event_ts, val from ct2 "
            "order by event_ts) r window_time(l.event_ts, r.event_ts) "
            "window_offset(0h, 0h) order by l.event_ts, l.val;",
            1,
            (10, 10, 20, 40),
        )

        tdSql.error(
            "select * from ct1 l left window join ct2 r "
            "window_time(r.ts, r.ts) window_offset(-1h, 1h);"
        )
        tdSql.error(
            "select * from ct1 l left window join ct2 r "
            "window_time(l.missing, r.ts) window_offset(-1h, 1h);"
        )
        tdSql.error(
            "select * from ct1 l left window join ct2 r "
            "window_time(l.val, r.ts) window_offset(-1h, 1h);"
        )
        tdSql.error(
            "select * from ct1 l left window join ct2 r "
            "window_time(1, r.ts) window_offset(-1h, 1h);"
        )
        tdSql.error(
            "select * from ct1 l left window join ct2 r "
            "window_time(now(), r.ts) window_offset(-1h, 1h);"
        )
        tdSql.error(
            "select * from ct1 l left asof join ct2 r "
            "window_time(l.ts, r.ts);"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Temporal JOIN automatic input ordering
    # ══════════════════════════════════════════════════════════════════════════

    def do_temporal_direction_matrix(self):
        self._prepare_sort_data()
        orders = ("ts", "ts desc", "val desc")
        for side in ("left", "right"):
            for kind in ("asof", "window"):
                suffix = "window_offset(-2h, 4h)" if kind == "window" else ""
                join = f"{side} {kind} join"
                tail = f"{suffix} order by l.ts, r.ts"
                tdSql.query(
                    f"select l.val, r.val from t1 l {join} t2 r {tail}"
                )
                expected = list(tdSql.queryResult)
                for left in orders:
                    for right in orders:
                        tdSql.query(
                            "select l.val, r.val from "
                            f"(select ts, val from t1 order by {left}) l "
                            f"{join} "
                            f"(select ts, val from t2 order by {right}) r "
                            f"{tail}"
                        )
                        tdSql.checkRows(len(expected))
                        for row, values in enumerate(expected):
                            for col, value in enumerate(values):
                                tdSql.checkData(row, col, value)
        print("temporal direction matrix ................ [ passed ]")

    def do_temporal_limit_boundaries(self):
        self._prepare_sort_data()
        for order in ("val desc", "ts desc"):
            self._check_col(
                "select l.val, r.val from "
                f"(select ts, val from t1 order by {order} "
                "limit 2 offset 1) l left window join "
                "(select ts, val from t2 order by ts) r "
                "window_offset(-2h, 4h) jlimit 1 order by l.ts desc;",
                1, (40, 30),
            )
        self._check_col(
            "select l.val, r.val from t1 l left window join "
            "(select ts, val from t2 order by ts desc limit 2) r "
            "window_offset(-2h, 4h) order by l.ts;",
            1, (None, None, None, 40, None),
        )
        self._check_col(
            "select l.val, r.val from t1 l left asof join "
            "(select ts, val from t2 where val < 0 order by val) r "
            "order by l.ts;", 1, (None,) * 5,
        )
        print("temporal limit boundaries ................ [ passed ]")

    def do_temporal_nonprimary_time(self):
        self._prepare_tcol_data()
        tdSql.execute(
            "insert into ct1 values ('2024-01-01 05:00:00', null, 5)"
        )
        tdSql.execute(
            "insert into ct2 values ('2024-01-01 05:00:00', null, 50)"
        )
        tdSql.execute(
            "create vtable v_ct1(ts timestamp, "
            "t_col timestamp from ct1.t_col, val int from ct1.val)"
        )
        for left in ("ct1", "v_ct1"):
            self._check_col(
                f"select l.val, r.val from {left} l left window join ct2 r "
                "window_time(l.t_col, r.t_col) window_offset(0h, 0h) "
                "order by l.val;", 1, (10, 20, 40, 10),
            )
        self._check_col(
            "select l.val, r.val from ct1 l right window join ct2 r "
            "window_time(l.t_col, r.t_col) window_offset(0h, 0h) "
            "order by r.val, l.val;", 0, (1, 4, 2, None, 3),
        )
        self._check_col(
            "select l.val, r.val from "
            "(select ts, t_col, val from ct1 order by t_col desc) l "
            "left window join ct2 r window_time(l.t_col, r.t_col) "
            "window_offset(0h, 0h) order by l.val;",
            1, (10, 20, 40, 10),
        )
        print("temporal nonprimary time ................. [ passed ]")

    def do_temporal_null_time(self):
        self._prepare_tcol_data()
        tdSql.execute(
            "create table temporal_null_l "
            "(ts timestamp, effective_ts timestamp, val int)"
        )
        tdSql.execute(
            "create table temporal_null_r "
            "(ts timestamp, effective_ts timestamp, val int)"
        )
        tdSql.execute(
            "insert into temporal_null_l values "
            "('2024-01-01 00:00:00', '2024-01-01 10:00:00', 1) "
            "('2024-01-01 01:00:00', NULL, 2) "
            "('2024-01-01 02:00:00', '2024-01-01 12:00:00', 3)"
        )
        tdSql.execute(
            "insert into temporal_null_r values "
            "('2024-01-01 00:00:00', '2024-01-01 10:00:00', 10) "
            "('2024-01-01 01:00:00', NULL, 20) "
            "('2024-01-01 02:00:00', '2024-01-01 12:00:00', 30)"
        )

        left = "(select effective_ts, val from temporal_null_l)"
        right = "(select effective_ts, val from temporal_null_r)"
        expected = [(1, 10), (3, 30)]
        for side in ("left", "right"):
            driver = "l" if side == "left" else "r"
            for kind in ("asof", "window"):
                if kind == "asof":
                    op = ">=" if side == "left" else "<="
                    explicit = f"on l.effective_ts {op} r.effective_ts"
                    suffix = ""
                else:
                    explicit = (
                        "window_time(l.effective_ts, r.effective_ts)"
                    )
                    suffix = "window_offset(0s, 0s)"
                for time_clause in ("", explicit):
                    sql = (
                        f"select l.val, r.val from {left} l "
                        f"{side} {kind} join {right} r "
                        f"{time_clause} {suffix} order by {driver}.val"
                    )
                    if kind == "asof" and time_clause:
                        # Ordinary timestamps cannot be explicit ASOF keys.
                        tdSql.error(
                            sql,
                            expectErrInfo="join condition is not supported",
                        )
                        continue
                    tdSql.query(sql)
                    assert list(tdSql.queryResult) == expected, sql
        print("temporal NULL time ....................... [ passed ]")

    def do_temporal_global_order(self):
        self._prepare_tcol_data()
        for child, start in (("ct1", 0), ("ct2", 1)):
            for batch in range(5):
                values = " ".join(
                    f"({1704153600000 + (i * 2 + start) * 1000}, "
                    f"{1704153600000 + (9999 - i * 2 - start) * 1000}, "
                    f"{i * 2 + start})"
                    for i in range(batch * 1000, (batch + 1) * 1000)
                )
                tdSql.execute(f"insert into {child} values {values}")
        tdSql.query(
            "select l.val, r.val from "
            "(select ts, val from stb where ts >= 1704153600000 "
            "order by val desc) l left window join "
            "(select ts, val from stb where ts >= 1704153600000) r "
            "window_offset(0s, 0s) order by l.ts"
        )
        tdSql.checkRows(10000)
        for row in range(10000):
            tdSql.checkData(row, 0, row)
            tdSql.checkData(row, 1, row)
        print("temporal global order .................... [ passed ]")

    def _check_temporal_sort_count(self, sql, expected):
        tdSql.query("explain " + sql)
        count = sum(
            "Sort input_order=" in str(tdSql.getData(row, 0))
            for row in range(tdSql.queryRows)
        )
        assert count == expected, (sql, count, expected)

    def do_temporal_redundant_sort(self):
        self._prepare_sort_data()
        expected = [(value, value) for value in range(1, 6)]
        for join in ("left asof", "right asof",
                     "left window", "right window"):
            suffix = " window_offset(0s, 0s) jlimit 1" \
                if "window" in join else ""
            for side in (0, 1):
                for limit, sorts in (("", 1), (" limit 100", 2)):
                    child = (
                        "(select ts time_alias, val from t1 "
                        f"where val > 0 order by val desc{limit})"
                    )
                    left, right = (child, "t1") if side == 0 \
                        else ("t1", child)
                    sql = (
                        f"select l.val, r.val from {left} l "
                        f"{join} join {right} r{suffix}"
                    )
                    tdSql.query(sql)
                    actual = sorted(
                        (tdSql.getData(row, 0), tdSql.getData(row, 1))
                        for row in range(tdSql.queryRows)
                    )
                    assert actual == expected, (sql, actual)
                    self._check_temporal_sort_count(sql, sorts)

        sql = (
            "select l.val, r.val from "
            "(select ts, val from t1 order by val desc "
            "limit 2 offset 1) l left window join t1 r "
            "window_offset(0s, 0s) jlimit 1"
        )
        tdSql.query(sql)
        tdSql.checkRows(2)
        for row, value in enumerate((3, 4)):
            tdSql.checkData(row, 0, value)
            tdSql.checkData(row, 1, value)
        self._check_temporal_sort_count(sql, 2)
        print("temporal redundant input sort ............ [ passed ]")

    def do_temporal_duplicate_sort_boundary(self):
        self._prepare_tcol_data()
        tdSql.execute(
            "create table pk_rows "
            "(ts timestamp, id int primary key, val int)"
        )
        tdSql.execute(
            "insert into pk_rows values "
            "('2024-01-01 00:00:00', 1, 1) "
            "('2024-01-01 00:00:00', 2, 4)"
        )
        for source, key, sorts in (("ct1", "t_col", 3),
                                   ("pk_rows", "ts", 2)):
            candidates = {10: {1, 4}, 20: {None}, 30: {None}, 40: {None}}
            if source == "ct1":
                candidates.update({20: {2}, 40: {3}})
            expected = sorted(
                (left, right)
                for left, values in candidates.items() for right in values
            )
            for order in ("val desc", "ts desc, val"):
                for limit in ("", " limit 100"):
                    sql = (
                        "select l.val, r.val from ct2 l left window join "
                        f"(select {key}, val from {source} "
                        f"order by {order}{limit}) r "
                        f"window_time(l.{key}, r.{key}) "
                        "window_offset(0s, 0s)"
                    )
                    tdSql.query(sql)
                    tdSql.checkRows(len(expected))
                    actual = sorted(
                        (tdSql.getData(row, 0), tdSql.getData(row, 1))
                        for row in range(tdSql.queryRows)
                    )
                    assert actual == expected, (sql, actual, expected)
                    expected_sorts = sorts if limit else sorts - 1
                    if key == "ts" and order == "ts desc, val":
                        # The primary-time order also serves the join.
                        expected_sorts = 1
                    self._check_temporal_sort_count(sql, expected_sorts)
                    tdSql.query(sql + " jlimit 1")
                    tdSql.checkRows(len(candidates))
                    left_values = []
                    for row in range(tdSql.queryRows):
                        left = tdSql.getData(row, 0)
                        right = tdSql.getData(row, 1)
                        left_values.append(left)
                        assert right in candidates[left], (sql, left, right)
                    assert sorted(left_values) == sorted(candidates), sql
        print("temporal duplicate sort boundary ......... [ passed ]")

    def test_8_4_temporal_input_alignment(self):
        """Correct temporal JOIN input order without changing input rows

        1. Check all directions for LEFT/RIGHT ASOF and WINDOW JOIN.
        2. Preserve LIMIT, OFFSET, JLIMIT and outer ordering.
        3. Check NULL default ASOF and explicit/implicit WINDOW time keys.
        4. Cover non-primary time, virtual tables and duplicate timestamps.
        5. Check global ordering across child tables and data blocks.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Tony Zhang Added temporal input ordering tests
        """
        self.do_temporal_direction_matrix()
        self.do_temporal_limit_boundaries()
        self.do_temporal_nonprimary_time()
        self.do_temporal_null_time()
        self.do_temporal_global_order()

    def test_8_5_temporal_redundant_sort(self):
        """Remove only unobservable temporal input sorting

        1. Compare results and Sort counts with a nonbinding LIMIT control.
        2. Cover both sides of LEFT/RIGHT ASOF and WINDOW JOIN.
        3. Preserve LIMIT/OFFSET and duplicate-time JLIMIT behavior,
           including composite-primary-key inputs.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Tony Zhang Added temporal redundant-sort tests
            - 2026-09-08 Tony Zhang Added nonunique time and original sort tests
        """
        self.do_temporal_redundant_sort()
        self.do_temporal_duplicate_sort_boundary()

    def test_8_6_asof_explicit_time(self):
        """Resolve explicit ASOF time before sorting or filtering inputs.

        Check explicit time keys across input directions, sorts, and filters.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-08 Tony Zhang Covered explicit time selection
        """
        self._prepare_tcol_data()
        base = 1704067200000
        left_rows = [(1, 1), (3, 2), (5, 3), (7, 4)]
        right_rows = [(0, 10), (3, 20), (4, 30), (6, 40)]
        for table, rows in (("asof_l", left_rows), ("asof_r", right_rows)):
            tdSql.execute(
                f"create table {table} "
                "(ts timestamp, other_ts timestamp, val int)"
            )
            for (time, val), other in zip(rows, (9, None, 1, 3)):
                other_sql = "NULL" if other is None else str(base + other)
                tdSql.execute(
                    f"insert into {table} values "
                    f"({base + time}, {other_sql}, {val})"
                )

        comparisons = {
            ">": lambda left, right: left > right,
            ">=": lambda left, right: left >= right,
            "=": lambda left, right: left == right,
            "<": lambda left, right: left < right,
            "<=": lambda left, right: left <= right,
        }
        reverse = {">": "<", ">=": "<=", "=": "=", "<": ">",
                   "<=": ">="}
        variants = (
            ("", left_rows, "", right_rows),
            ("order by other_ts desc", left_rows,
             "order by ts desc", right_rows),
            ("order by val desc limit 2 offset 1", left_rows[1:3],
             "order by other_ts", right_rows),
            ("order by ts desc", left_rows,
             "order by ts desc limit 2 offset 1", right_rows[1:3]),
            ("where val < 0", [], "", right_rows),
            ("", left_rows, "where val < 0", []),
        )
        for side in ("left", "right"):
            for op, compare in comparisons.items():
                for lorder, lrows, rorder, rrows in variants:
                    expected = []
                    probes = lrows if side == "left" else rrows
                    for time, val in probes:
                        if side == "left":
                            candidates = [row for row in rrows
                                          if compare(time, row[0])]
                        else:
                            candidates = [row for row in lrows
                                          if compare(row[0], time)]
                        match = min(candidates, key=lambda row: abs(
                            row[0] - time))[1] if candidates else None
                        expected.append((val, match) if side == "left"
                                        else (match, val))
                    for swapped in (False, True):
                        condition = f"l.time_key {op} r.time_key"
                        if swapped:
                            condition = (
                                f"r.time_key {reverse[op]} l.time_key"
                            )
                        sql = (
                            "select l.val, r.val from "
                            "(select other_ts, ts time_key, val "
                            f"from asof_l {lorder}) l {side} asof join "
                            "(select other_ts, ts time_key, val "
                            f"from asof_r {rorder}) r on {condition} "
                            f"order by {'l' if side == 'left' else 'r'}.val"
                        )
                        tdSql.query(sql)
                        assert list(tdSql.queryResult) == expected, sql

        for condition in (
            "l.time_key >= r.time_key",
            "timetruncate(l.time_key, 1a) >= "
            "timetruncate(r.time_key, 1a)",
        ):
            sql = (
                "select l.val, r.val from "
                "(select other_ts, time_key, val from "
                "(select other_ts, ts time_key, val from asof_l "
                "order by other_ts desc) s) l left asof join "
                "(select other_ts, ts time_key, val from asof_r) r "
                f"on {condition} order by l.val"
            )
            tdSql.query(sql)
            assert list(tdSql.queryResult) == [(1, 10), (2, 20),
                                               (3, 30), (4, 40)], sql

        for condition in (
            "l.ts >= r.ts and l.val >= r.val",
            "l.ts >= r.ts and l.ts <= r.ts",
            "l.ts = r.ts and "
            "timetruncate(l.ts, 1s) = timetruncate(r.ts, 1s)",
        ):
            tdSql.error(
                "select l.val, r.val from asof_l l left asof join "
                f"asof_r r on {condition}"
            )
        tdSql.query(
            "select l.val, r.val from asof_l l left asof join "
            "asof_r r on l.ts >= r.ts and l.val = r.val order by l.val"
        )
        assert list(tdSql.queryResult) == [(val, None) for _, val in left_rows]

    def test_8_7_temporal_tag_inputs(self):
        """Preserve tag sources and grouped non-primary temporal inputs.

        Check tag sources and non-primary time columns on both join sides.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-08 Tony Zhang Covered corrective sorts and tag sources
        """
        self._prepare_tcol_data()
        for side in ("left", "right"):
            tdSql.query(
                "select /*+ NO_BATCH_SCAN() */ l.val, r.val "
                f"from stb l {side} window join stb r on l.g = r.g "
                "window_time(l.t_col, r.t_col) window_offset(0h, 0h) "
                "order by l.val, r.val"
            )
            assert list(tdSql.queryResult) == [
                (1, 1), (1, 4), (2, 2), (3, 3), (4, 1), (4, 4),
                (10, 10), (20, 20), (30, 30), (40, 40),
            ]
        tdSql.execute(
            "create stable tag_source(ts timestamp, val int) "
            "tags(event_ts timestamp, g int)"
        )
        for name, ts, value in (("tag_l", 1704067200000, 1),
                                ("tag_r", 1704067201000, 2)):
            tdSql.execute(
                f"create table {name} using tag_source tags({ts}, 1)"
            )
            tdSql.execute(f"insert into {name} values({ts}, {value})")
        for side in ("left", "right"):
            for keys in ("l.event_ts, r.event_ts", "l.ts, r.event_ts",
                         "l.event_ts, r.ts"):
                sql = (
                    "select /*+ NO_BATCH_SCAN() */ l.val, r.val "
                    f"from tag_source l {side} window join tag_source r "
                    "on l.g = r.g "
                    f"window_time({keys}) window_offset(0s, 0s) "
                    "order by l.val"
                )
                tdSql.query(sql)
                assert list(tdSql.queryResult) == [(1, 1), (2, 2)], sql

        tdSql.execute(
            "create stable virtual_tags(ts timestamp, val int) "
            "tags(event_ts timestamp) virtual 1"
        )
        tdSql.execute(
            "create vtable v_tag_l(val from tag_l.val) "
            "using virtual_tags tags(event_ts from tag_r.event_ts)"
        )
        for source in ("v_tag_l", "virtual_tags"):
            for side in ("left", "right"):
                sql = (
                    f"select v.val, r.val from {source} v "
                    f"{side} window join tag_r r "
                    "window_time(v.event_ts, r.ts) window_offset(0s, 0s)"
                )
                tdSql.query(sql)
                assert list(tdSql.queryResult) == [(1, 2)], sql
            for side in ("left", "right"):
                for explicit in (False, True):
                    condition = "v.event_ts = r.event_ts"
                    if explicit:
                        condition = "v.ts >= r.ts and " + condition
                    sql = (
                        f"select v.val, r.val from {source} v "
                        f"{side} asof join tag_r r on {condition}"
                    )
                    expected = [(1, None)] if side == "left" else \
                        ([(None, 2)] if explicit else [(1, 2)])
                    for _ in range(10):
                        tdSql.query(sql)
                        assert list(tdSql.queryResult) == expected, sql
            for clause in ("order by event_ts", "partition by event_ts",
                           "where event_ts is not null"):
                tdSql.query(f"select val from {source} {clause}")
                assert list(tdSql.queryResult) == [(1,)], clause
            tdSql.query(
                f"select count(*) from {source} group by event_ts "
                "having event_ts is not null"
            )
            assert list(tdSql.queryResult) == [(1,)]

    def test_8_8_temporal_primary_time_priority(self):
        """Prefer projected primary time over earlier timestamp columns

        Check constants, star expansion, aliases and nested projections.
        Preserve selection when either peer contains a JOIN.
        Keep explicit WINDOW_TIME selection ahead of the default time key.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-21 Tony Zhang Added primary time selection tests
            - 2026-09-22 Cover mixed nested temporal JOIN inputs
        """
        self._prepare_tcol_data()
        inputs = (
            "select now() anchor, * from {table}",
            "select t_col, ts time_key, val from {table} "
            "order by t_col desc",
            "select anchor, time_key, val from "
            "(select now() anchor, ts time_key, val from {table}) s "
            "order by val desc",
            "select cast('2024-01-01 10:00:00' as timestamp) anchor, "
            "ts, val from {table}",
        )
        expected = {
            ("left", "asof"): (10, 20, 30, 30),
            ("right", "asof"): (1, 2, 3, 4),
            ("left", "window"): (10, 20, 30, None),
            ("right", "window"): (1, 2, 3, None),
        }
        for source in inputs:
            left = source.format(table="ct1")
            right = source.format(table="ct2")
            for side, driver, col in (("left", "l", 1), ("right", "r", 0)):
                for kind, suffix in (
                    ("asof", ""), ("window", "window_offset(0s, 0s)")
                ):
                    for left_input, right_input in (
                        (f"({left}) l", f"({right}) r"),
                        (f"({left}) l",
                         "(ct2 r join ct2 p on r.ts = p.ts)"),
                        ("(ct1 l join ct1 p on l.ts = p.ts)",
                         f"({right}) r"),
                    ):
                        self._check_col(
                            f"select l.val, r.val from {left_input} "
                            f"{side} {kind} join {right_input} {suffix} "
                            f"order by {driver}.val",
                            col, expected[(side, kind)],
                        )
        for side, col, order, values in (
            ("left", 1, "l.val, r.val", (10, 20, 40, 10)),
            ("right", 0, "r.val, l.val", (1, 4, 2, None, 3)),
        ):
            self._check_col(
                "select l.val, r.val from (select now() anchor, * "
                f"from ct1) l {side} window join "
                "(select now() anchor, * from ct2) r "
                "window_time(l.t_col, r.t_col) window_offset(0s, 0s) "
                f"order by {order}", col, values,
            )

    def test_8_9_temporal_constant_time_fallback(self):
        """Allow the first timestamp to be constant without a primary column

        Check NOW and literal timestamps before ordinary timestamp columns.

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-21 Tony Zhang Added constant time fallback tests
        """
        self._prepare_tcol_data()
        for time_expr in ("now()", "cast('2024-01-01' as timestamp)"):
            # Filter the non-driving side to one row; every driving row then
            # matches it, so the peer column repeats that row's value.
            for side, driver, col, lfilt, rfilt, val in (
                ("left", "l", 1, "", " where val = 20", 20),
                ("right", "r", 0, " where val = 2", "", 2),
            ):
                left = f"select {time_expr} anchor, t_col, val from ct1{lfilt}"
                right = f"select {time_expr} anchor, t_col, val from ct2{rfilt}"
                for kind, suffix in (
                    ("asof", ""), ("window", "window_offset(0s, 0s)")
                ):
                    self._check_col(
                        f"select l.val, r.val from ({left}) l "
                        f"{side} {kind} join ({right}) r {suffix} "
                        f"order by {driver}.val",
                        col, (val,) * 4,
                    )

    # Group 9: Regression (FS section 4.2 unchanged scenarios)

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
