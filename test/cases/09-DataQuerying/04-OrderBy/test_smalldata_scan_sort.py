# This test verifies the SMALLDATA_SCAN_SORT hint for super-table queries.
# With the hint, the planner must emit Sort + plain Table Scan instead of Table Merge Scan.
# Without the hint, the planner must still emit Table Merge Scan (default unchanged).
#
# Two test methods:
#   test_smalldata_scan_sort            -- ORDER BY ts + window contexts (single db).
#   test_smalldata_scan_sort_part_window -- PARTITION BY tag SESSION/STATE correctness
#                                           on single- and multi-vgroup databases.

from new_test_framework.utils import tdLog, tdSql


class TestSmallDataScanSort:
    """Verify SMALLDATA_SCAN_SORT hint for super-table queries."""

    DB = "test_small_data_scan_sort"
    STB = "stb"
    CHILD_COUNT = 4
    ROWS_PER_CHILD = 5

    # Timestamps shared across children so we can check global order.
    # Each child gets the same ts values; global ORDER BY ts must interleave them
    # (or produce them in any ts-consistent order).
    BASE_TS = 1700000000000  # ms epoch

    # Partition-by-tag window coverage (test_smalldata_scan_sort_part_window).
    PART_DB = "test_sdss_part_win"
    PART_TAGS = 5
    PART_ROWS_PER_TAG = 8000
    PART_STEP_MS = 1000  # 1s between consecutive rows of a tag

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # -----------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------

    def _setup_db(self):
        tdSql.execute(f"drop database if exists {self.DB}", show=1)
        tdSql.execute(
            f"create database {self.DB} keep 36500d vgroups 2 precision 'ms'",
            show=1,
        )
        tdSql.execute(f"use {self.DB}", show=1)
        tdSql.execute(
            f"create table {self.STB} (ts timestamp, val int, name varchar(32)) tags (tid int)",
            show=1,
        )
        for i in range(self.CHILD_COUNT):
            tdSql.execute(
                f"create table ct{i} using {self.STB} tags({i})", show=1
            )
        # Insert ROWS_PER_CHILD rows per child; stagger timestamps by child index
        # so rows from different children interleave in global ts order.
        for i in range(self.CHILD_COUNT):
            for j in range(self.ROWS_PER_CHILD):
                ts = self.BASE_TS + j * self.CHILD_COUNT * 1000 + i * 1000
                tdSql.execute(
                    f"insert into ct{i} values ({ts}, {i * 100 + j}, 'ct{i}_row{j}')",
                    show=1,
                )

    def _query_all(self, sql):
        """Run sql, return list of (ts, val, name) tuples."""
        tdSql.query(sql, show=1)
        return [(tdSql.getData(r, 0), tdSql.getData(r, 1), tdSql.getData(r, 2))
                for r in range(tdSql.getRows())]

    def _explain_text(self, sql):
        """Return the EXPLAIN output as a single string."""
        tdSql.query(f"explain {sql}", show=1)
        parts = []
        for r in range(tdSql.getRows()):
            parts.append(str(tdSql.getData(r, 0)))
        return "\n".join(parts)

    # -----------------------------------------------------------------
    # sub-cases
    # -----------------------------------------------------------------

    def _test_result_identity(self):
        """Hinted and non-hinted ORDER BY ts return the same rows in the same order."""
        tdLog.info("=== test: result identity (hinted == non-hinted) ===")

        sql_plain = f"select * from {self.STB} order by ts"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by ts"

        rows_plain = self._query_all(sql_plain)
        rows_hint  = self._query_all(sql_hint)

        total = self.CHILD_COUNT * self.ROWS_PER_CHILD
        assert len(rows_plain) == total, \
            f"non-hinted returned {len(rows_plain)} rows, expected {total}"
        assert len(rows_hint) == total, \
            f"hinted returned {len(rows_hint)} rows, expected {total}"
        assert rows_plain == rows_hint, \
            f"hinted and non-hinted results differ:\nhinted: {rows_hint}\nplain: {rows_plain}"

        # Verify the result is actually ascending in ts.
        for i in range(1, len(rows_hint)):
            assert rows_hint[i][0] >= rows_hint[i - 1][0], \
                f"hinted result not in ascending ts order at row {i}"

        tdLog.info("result identity: PASSED")

    def _test_result_identity_desc(self):
        """Hinted and non-hinted ORDER BY ts DESC return identical (desc-ordered) rows."""
        tdLog.info("=== test: result identity DESC ===")

        sql_plain = f"select * from {self.STB} order by ts desc"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by ts desc"

        rows_plain = self._query_all(sql_plain)
        rows_hint  = self._query_all(sql_hint)

        assert rows_plain == rows_hint, \
            f"hinted DESC and non-hinted DESC results differ"

        # Verify descending order.
        for i in range(1, len(rows_hint)):
            assert rows_hint[i][0] <= rows_hint[i - 1][0], \
                f"hinted DESC result not in descending ts order at row {i}"

        tdLog.info("result identity DESC: PASSED")

    def _test_result_identity_limit(self):
        """Hinted and non-hinted ORDER BY ts LIMIT k return identical top-k rows."""
        tdLog.info("=== test: result identity with LIMIT ===")
        k = self.CHILD_COUNT  # pick exactly one full 'wave'

        sql_plain = f"select * from {self.STB} order by ts limit {k}"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by ts limit {k}"

        rows_plain = self._query_all(sql_plain)
        rows_hint  = self._query_all(sql_hint)

        assert len(rows_plain) == k, f"non-hinted LIMIT returned {len(rows_plain)}, expected {k}"
        assert len(rows_hint) == k, f"hinted LIMIT returned {len(rows_hint)}, expected {k}"
        assert rows_plain == rows_hint, \
            f"hinted and non-hinted LIMIT results differ:\nhinted: {rows_hint}\nplain: {rows_plain}"

        tdLog.info("result identity LIMIT: PASSED")

    def _test_explain_hinted_uses_sort_and_table_scan(self):
        """EXPLAIN of hinted query must contain 'Sort' and 'Table Scan', not 'Table Merge Scan'."""
        tdLog.info("=== test: EXPLAIN shows Sort + Table Scan (no Table Merge Scan) ===")

        sql_hint = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by ts"
        plan = self._explain_text(sql_hint)

        tdLog.info(f"hinted EXPLAIN:\n{plan}")

        assert "Sort" in plan, \
            f"Expected 'Sort' in hinted EXPLAIN plan, got:\n{plan}"
        assert "Table Scan" in plan or "TableScan" in plan, \
            f"Expected 'Table Scan' in hinted EXPLAIN plan, got:\n{plan}"
        assert "Table Merge Scan" not in plan and "TableMergeScan" not in plan, \
            f"Unexpected 'Table Merge Scan' in hinted EXPLAIN plan, got:\n{plan}"

        tdLog.info("EXPLAIN hinted: PASSED")

    def _test_explain_hinted_limit_uses_sort(self):
        """EXPLAIN of hinted query with LIMIT must also contain Sort + Table Scan."""
        tdLog.info("=== test: EXPLAIN LIMIT shows Sort + Table Scan ===")

        sql_hint_limit = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by ts limit 10"
        plan = self._explain_text(sql_hint_limit)

        tdLog.info(f"hinted LIMIT EXPLAIN:\n{plan}")

        assert "Sort" in plan, \
            f"Expected 'Sort' in hinted LIMIT EXPLAIN plan, got:\n{plan}"
        assert "Table Merge Scan" not in plan and "TableMergeScan" not in plan, \
            f"Unexpected 'Table Merge Scan' in hinted LIMIT EXPLAIN plan, got:\n{plan}"

        tdLog.info("EXPLAIN hinted LIMIT: PASSED")

    def _test_explain_non_hinted_uses_merge_scan(self):
        """EXPLAIN of non-hinted ORDER BY ts must contain Table Merge Scan (default unchanged)."""
        tdLog.info("=== test: EXPLAIN non-hinted shows Table Merge Scan ===")

        sql_plain = f"select * from {self.STB} order by ts"
        plan = self._explain_text(sql_plain)

        tdLog.info(f"non-hinted EXPLAIN:\n{plan}")

        assert "Table Merge Scan" in plan or "TableMergeScan" in plan, \
            f"Expected 'Table Merge Scan' in non-hinted EXPLAIN plan (default behaviour changed!), got:\n{plan}"

        tdLog.info("EXPLAIN non-hinted default Table Merge Scan: PASSED")

    # -----------------------------------------------------------------
    # Task 2 sub-cases: window contexts (interval / session / state / partition)
    #
    # For SESSION and STATE_WINDOW on a super table the default plan uses a
    # per-vnode Table Merge Scan as the order source; with the hint that merge
    # scan must be replaced by a plain Table Scan plus an inserted Sort.
    #
    # For INTERVAL the default plan already uses a plain Table Scan plus a
    # window-split Merge(sort) (the interval operator handles intra-vnode block
    # ordering), so the hint is a no-op for interval: hinted and non-hinted plans
    # are identical and neither contains a Table Merge Scan.  We only assert
    # result equality and absence of a merge scan for the interval case.
    # -----------------------------------------------------------------

    def _query_rows(self, sql):
        """Run sql, return all rows as a list of full tuples (all columns)."""
        tdSql.query(sql, show=1)
        return [tuple(tdSql.getData(r, c) for c in range(len(tdSql.queryResult[r])))
                for r in range(tdSql.getRows())]

    def _assert_hint_no_merge_scan(self, sql_hint, label):
        plan = self._explain_text(sql_hint)
        tdLog.info(f"{label} hinted EXPLAIN:\n{plan}")
        assert "Table Merge Scan" not in plan and "TableMergeScan" not in plan, \
            f"Unexpected 'Table Merge Scan' in hinted {label} EXPLAIN plan, got:\n{plan}"
        return plan

    @staticmethod
    def _has_sort_directly_above_table_scan(plan):
        """True if a 'Sort' line directly parents a 'Table Scan' line.

        The plan is printed depth-first with indentation, so the inserted
        per-vnode Sort shows up as a 'Sort' line whose next operator line is the
        'Table Scan'.  Checking this (rather than just 'Sort' anywhere) ensures we
        verify the *inserted* sort, not the unrelated top-level ORDER BY sort.
        """
        op_lines = [ln for ln in plan.splitlines() if "->" in ln]
        for cur, nxt in zip(op_lines, op_lines[1:]):
            if "Sort" in cur and ("Table Scan" in nxt or "TableScan" in nxt):
                return True
        return False

    def _test_interval(self):
        """Hinted vs non-hinted INTERVAL must return identical results; hint shows no merge scan."""
        tdLog.info("=== test: INTERVAL hinted == non-hinted ===")
        sql_plain = f"select _wstart, count(*) from {self.STB} interval(2s) order by _wstart"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ _wstart, count(*) from {self.STB} interval(2s) order by _wstart"

        assert sorted(self._query_rows(sql_plain)) == sorted(self._query_rows(sql_hint)), \
            "hinted and non-hinted INTERVAL results differ"
        # Interval never uses a merge scan in either plan.
        self._assert_hint_no_merge_scan(sql_hint, "INTERVAL")
        tdLog.info("INTERVAL: PASSED")

    def _test_interval_partition(self):
        """Hinted vs non-hinted PARTITION BY ... INTERVAL must return identical results."""
        tdLog.info("=== test: PARTITION BY tbname INTERVAL hinted == non-hinted ===")
        sql_plain = (f"select tbname, _wstart, count(*) from {self.STB} "
                     f"partition by tbname interval(2s) order by tbname, _wstart")
        sql_hint  = (f"select /*+ smalldata_scan_sort() */ tbname, _wstart, count(*) from {self.STB} "
                     f"partition by tbname interval(2s) order by tbname, _wstart")

        assert sorted(self._query_rows(sql_plain)) == sorted(self._query_rows(sql_hint)), \
            "hinted and non-hinted PARTITION/INTERVAL results differ"
        self._assert_hint_no_merge_scan(sql_hint, "PARTITION/INTERVAL")
        tdLog.info("PARTITION/INTERVAL: PASSED")

    def _test_session(self):
        """Hinted SESSION shows Sort + Table Scan (no merge scan); results identical to non-hinted."""
        tdLog.info("=== test: SESSION hinted == non-hinted, hint uses Sort + Table Scan ===")
        sql_plain = f"select _wstart, count(*) from {self.STB} session(ts, 2s) order by _wstart"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ _wstart, count(*) from {self.STB} session(ts, 2s) order by _wstart"

        # Compare as sets (sort in Python) so a plan-induced ordering difference is
        # not hidden by the trailing ORDER BY _wstart.
        assert sorted(self._query_rows(sql_plain)) == sorted(self._query_rows(sql_hint)), \
            "hinted and non-hinted SESSION results differ"

        plan_hint = self._assert_hint_no_merge_scan(sql_hint, "SESSION")
        # Verify the INSERTED per-vnode Sort (Sort directly above Table Scan), not
        # merely that some 'Sort' (e.g. the ORDER BY) appears in the plan.
        assert self._has_sort_directly_above_table_scan(plan_hint), \
            f"Expected an inserted Sort directly above Table Scan in hinted SESSION plan, got:\n{plan_hint}"

        plan_plain = self._explain_text(sql_plain)
        assert "Table Merge Scan" in plan_plain or "TableMergeScan" in plan_plain, \
            f"Expected 'Table Merge Scan' in non-hinted SESSION plan (default changed!), got:\n{plan_plain}"
        tdLog.info("SESSION: PASSED")

    def _test_session_partition(self):
        """Hinted vs non-hinted PARTITION BY ... SESSION must return identical results; hint shows no merge scan."""
        tdLog.info("=== test: PARTITION BY tid SESSION hinted == non-hinted ===")
        sql_plain = (f"select tid, _wstart, count(*) from {self.STB} "
                     f"partition by tid session(ts, 2s) order by tid, _wstart")
        sql_hint  = (f"select /*+ smalldata_scan_sort() */ tid, _wstart, count(*) from {self.STB} "
                     f"partition by tid session(ts, 2s) order by tid, _wstart")

        assert sorted(self._query_rows(sql_plain)) == sorted(self._query_rows(sql_hint)), \
            "hinted and non-hinted PARTITION/SESSION results differ"
        self._assert_hint_no_merge_scan(sql_hint, "PARTITION/SESSION")
        tdLog.info("PARTITION/SESSION: PASSED")

    def _test_state_window(self):
        """Hinted STATE_WINDOW shows Sort + Table Scan (no merge scan); results identical to non-hinted."""
        tdLog.info("=== test: STATE_WINDOW hinted == non-hinted, hint uses Sort + Table Scan ===")
        sql_plain = f"select _wstart, count(*) from {self.STB} state_window(val) order by _wstart"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ _wstart, count(*) from {self.STB} state_window(val) order by _wstart"

        assert sorted(self._query_rows(sql_plain)) == sorted(self._query_rows(sql_hint)), \
            "hinted and non-hinted STATE_WINDOW results differ"

        plan_hint = self._assert_hint_no_merge_scan(sql_hint, "STATE_WINDOW")
        assert self._has_sort_directly_above_table_scan(plan_hint), \
            f"Expected an inserted Sort directly above Table Scan in hinted STATE_WINDOW plan, got:\n{plan_hint}"

        plan_plain = self._explain_text(sql_plain)
        assert "Table Merge Scan" in plan_plain or "TableMergeScan" in plan_plain, \
            f"Expected 'Table Merge Scan' in non-hinted STATE_WINDOW plan (default changed!), got:\n{plan_plain}"
        tdLog.info("STATE_WINDOW: PASSED")

    def _test_order_by_non_ts_noop(self):
        """Negative: hint on ORDER BY a non-timestamp column is a no-op.

        No table merge scan is created for non-ts ordering, and the hint's inserted/kept
        sort key is always the primary timestamp, so the hint must change neither the
        plan nor the results for an ORDER BY on a regular column.
        """
        tdLog.info("=== test: ORDER BY non-ts column — hint is a no-op ===")
        sql_plain = f"select * from {self.STB} order by val"
        sql_hint  = f"select /*+ smalldata_scan_sort() */ * from {self.STB} order by val"

        rows_plain = self._query_all(sql_plain)
        rows_hint  = self._query_all(sql_hint)
        assert rows_plain == rows_hint, \
            f"hinted vs non-hinted ORDER BY val results differ:\nhinted: {rows_hint}\nplain: {rows_plain}"

        plan_plain = self._explain_text(sql_plain)
        plan_hint  = self._explain_text(sql_hint)
        tdLog.info(f"ORDER BY val plain plan:\n{plan_plain}\nhinted plan:\n{plan_hint}")
        assert plan_plain == plan_hint, \
            f"hint altered the plan for ORDER BY a non-ts column (expected no-op):\nhinted:\n{plan_hint}\nplain:\n{plan_plain}"

        tdLog.info("ORDER BY non-ts no-op: PASSED")

    # -----------------------------------------------------------------
    # entry point
    # -----------------------------------------------------------------

    def test_smalldata_scan_sort(self):
        """SMALLDATA_SCAN_SORT hint: Sort+TableScan plan and result correctness.

        Verifies (ORDER BY — Task 1):
        1. Hinted ORDER BY ts returns same rows as non-hinted (ASC).
        2. Hinted ORDER BY ts DESC returns same rows as non-hinted DESC.
        3. Hinted ORDER BY ts LIMIT k returns same top-k rows as non-hinted LIMIT k.
        4. EXPLAIN of hinted query contains 'Sort' + 'Table Scan', no 'Table Merge Scan'.
        5. EXPLAIN of hinted LIMIT query also contains 'Sort', no 'Table Merge Scan'.
        6. EXPLAIN of non-hinted query still contains 'Table Merge Scan' (default unchanged).

        Verifies (window contexts — Task 2):
        7. INTERVAL / PARTITION BY ... INTERVAL: hinted == non-hinted results; hint
           shows no 'Table Merge Scan' (interval already uses a plain scan, so the
           hint is a no-op here).
        8. SESSION / STATE_WINDOW: hinted == non-hinted results; hint shows 'Sort'
           and no 'Table Merge Scan', while non-hinted still shows 'Table Merge Scan'.
        9. PARTITION BY ... SESSION: hinted == non-hinted results; hint shows no
           'Table Merge Scan'.

        Since: v3.3.6.x

        Labels: common

        Jira: None

        History:
            - 2026-06-22 Tony Zhang  Initial implementation for SMALLDATA_SCAN_SORT hint.
            - 2026-06-22 Tony Zhang  Task 2: add interval/session/state/partition variants.

        """
        self._setup_db()

        self._test_result_identity()
        self._test_result_identity_desc()
        self._test_result_identity_limit()
        self._test_explain_hinted_uses_sort_and_table_scan()
        self._test_explain_hinted_limit_uses_sort()
        self._test_explain_non_hinted_uses_merge_scan()
        self._test_order_by_non_ts_noop()

        # Task 2: window contexts.
        self._test_interval()
        self._test_interval_partition()
        self._test_session()
        self._test_session_partition()
        self._test_state_window()

    # -----------------------------------------------------------------
    # Partition-by-tag window correctness (single- and multi-vgroup)
    #
    # The hint inserts a plain ts Sort below a Partition node; because a Sort now
    # sits between the Partition and the scan, partTagsOptimize cannot fold the tag
    # partition into the scan, so the Partition operator regroups the globally
    # ts-ordered stream and each partition stays ts-ordered.  These checks pin that
    # the hinted result equals the (correct) non-hinted result on both a single
    # vnode (no stable split) and multiple vnodes (split via the Sort node).
    # -----------------------------------------------------------------

    def _setup_part_window_db(self, vgroups):
        tdSql.execute(f"drop database if exists {self.PART_DB}", show=1)
        tdSql.execute(f"create database {self.PART_DB} vgroups {vgroups} precision 'ms'", show=1)
        tdSql.execute(f"use {self.PART_DB}", show=1)
        # st is constant within a tag -> one STATE_WINDOW per partition.
        tdSql.execute(f"create table {self.STB} (ts timestamp, val int, st int) tags (gid int)", show=1)
        for t in range(self.PART_TAGS):
            tdSql.execute(f"create table ct{t} using {self.STB} tags({t})", show=1)
        # All tags share the SAME ts range (fully overlapping), inserted in row-major
        # order across tags so that, after FLUSH, a multi-table read returns blocks
        # interleaved by file offset -- the layout that would expose any per-partition
        # ordering bug.
        CHUNK = 500
        for start in range(0, self.PART_ROWS_PER_TAG, CHUNK):
            end = min(start + CHUNK, self.PART_ROWS_PER_TAG)
            for t in range(self.PART_TAGS):
                vals = " ".join(
                    f"({self.BASE_TS + i * self.PART_STEP_MS},{t * 1000000 + i},{t})"
                    for i in range(start, end)
                )
                tdSql.execute(f"insert into ct{t} values {vals}", show=0)
        tdSql.execute(f"flush database {self.PART_DB}", show=1)

    def _check_part_window(self, vgroups):
        # gap (5s) >> step (1s) -> each tag is a single session window.
        sql_plain = (f"select gid, _wstart, count(*) from {self.STB} "
                     f"partition by gid session(ts, 5s) order by gid, _wstart")
        sql_hint = (f"select /*+ smalldata_scan_sort() */ gid, _wstart, count(*) from {self.STB} "
                    f"partition by gid session(ts, 5s) order by gid, _wstart")
        tdLog.info(f"[vgroups={vgroups}] hinted SESSION plan:\n{self._explain_text(sql_hint)}")

        rows_plain = self._query_rows(sql_plain)
        rows_hint = self._query_rows(sql_hint)
        # The non-hinted plan is the correct oracle: exactly one window per tag.
        assert len(rows_plain) == self.PART_TAGS, \
            f"[vgroups={vgroups}] oracle sanity: expected {self.PART_TAGS} session windows, got {len(rows_plain)}: {rows_plain}"
        assert rows_hint == rows_plain, (
            f"[vgroups={vgroups}] SESSION: hinted result differs from non-hinted (per-partition order lost).\n"
            f"hint ({len(rows_hint)} rows): {rows_hint}\nplain ({len(rows_plain)} rows): {rows_plain}")
        tdLog.info(f"[vgroups={vgroups}] SESSION partition-by-tag: PASSED")

        # STATE window: st is constant within a tag -> one state window per tag.
        sql_plain_s = (f"select gid, _wstart, count(*) from {self.STB} "
                       f"partition by gid state_window(st) order by gid, _wstart")
        sql_hint_s = (f"select /*+ smalldata_scan_sort() */ gid, _wstart, count(*) from {self.STB} "
                      f"partition by gid state_window(st) order by gid, _wstart")
        rows_plain_s = self._query_rows(sql_plain_s)
        rows_hint_s = self._query_rows(sql_hint_s)
        assert rows_hint_s == rows_plain_s, (
            f"[vgroups={vgroups}] STATE_WINDOW: hinted result differs from non-hinted (per-partition order lost).\n"
            f"hint ({len(rows_hint_s)} rows): {rows_hint_s}\nplain ({len(rows_plain_s)} rows): {rows_plain_s}")
        tdLog.info(f"[vgroups={vgroups}] STATE_WINDOW partition-by-tag: PASSED")

    def test_smalldata_scan_sort_part_window(self):
        """SMALLDATA_SCAN_SORT preserves per-partition order for SESSION/STATE windows.

        PARTITION BY tag SESSION/STATE on a super table, on both single-vgroup (no
        stable split) and multi-vgroup (split via the Sort node).  With gap >> row
        step every tag forms exactly ONE window, so the correct plan returns TAGS
        rows; an interleaved-partition plan would return more.  Hinted result must
        equal the (correct) non-hinted result.

        Since: v3.3.6.x

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-22 Tony Zhang  Partition-window correctness coverage for the hint.
        """
        for vgroups in (1, 3):
            self._setup_part_window_db(vgroups)
            self._check_part_window(vgroups)
