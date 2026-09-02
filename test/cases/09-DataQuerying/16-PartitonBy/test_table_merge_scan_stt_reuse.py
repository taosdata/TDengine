import time

from new_test_framework.utils import tdLog, tdSql


class TestTableMergeScanSttReuse:
    """Test correctness of Table Merge Scan with STT reuse optimization.

    Verifies that the sort_for_group() hint (Table Merge Scan / seq_grp_order
    path) produces identical results to the default partition path, across
    state_window, multi-fileset, empty-group, UNION ALL, and LIMIT/OFFSET
    scenarios.
    """

    DB = "test_stt_reuse"
    STB = "meters"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _create_db(self, duration="1d", vgroups=2):
        tdSql.execute(f"drop database if exists {self.DB}")
        # drop database is asynchronous: vnodes are torn down by background
        # transactions, and an immediate same-name create can hit "Database in
        # dropping status".  Wait until the database disappears from the catalog.
        for _ in range(30):
            tdSql.query(
                f"select * from information_schema.ins_databases "
                f"where name = '{self.DB}'",
                queryTimes=1,
            )
            if tdSql.queryRows == 0:
                break
            time.sleep(1)
        tdSql.execute(
            f"create database {self.DB} vgroups {vgroups} duration {duration} "
            f"stt_trigger 1"
        )
        tdSql.execute(f"use {self.DB}")

    def _create_stb(self):
        tdSql.execute(
            f"create table {self.STB} "
            f"(ts timestamp, vb bool, vi int, vf float) "
            f"tags (site binary(20), zone int, tracker int)"
        )

    def _create_ctables(self, specs):
        """Create child tables from a list of (name, site, zone, tracker)."""
        for name, site, zone, tracker in specs:
            tdSql.execute(
                f"create table {name} using {self.STB} "
                f"tags('{site}', {zone}, {tracker})"
            )

    def _insert_rows(self, table, start_ts, count, step_ms=1000):
        """Insert `count` rows starting at `start_ts` (epoch ms).

        vb cycles: true, true, false, false, NULL
        vi cycles: 0..count-1
        vf = vi * 0.1
        """
        batch_size = 500
        for offset in range(0, count, batch_size):
            end = min(offset + batch_size, count)
            vals = []
            for i in range(offset, end):
                ts = start_ts + i * step_ms
                idx = i % 5
                if idx < 2:
                    vb = "true"
                elif idx < 4:
                    vb = "false"
                else:
                    vb = "NULL"
                vals.append(f"({ts}, {vb}, {i}, {i * 0.1})")
            tdSql.execute(f"insert into {table} values " + " ".join(vals))

    def _flush(self):
        tdSql.execute(f"flush database {self.DB}")

    def _add_hint(self, sql):
        """Inject sort_for_group() hint into a SELECT."""
        if sql.strip().lower().startswith("select"):
            return "select /*+ sort_for_group() */ " + sql.strip()[6:]
        return sql

    def _query_ordered(self, sql, order_by):
        """Wrap `sql` in an outer SELECT with ORDER BY for deterministic comparison."""
        return f"select * from ({sql}) _t order by {order_by}"

    def _compare_results(self, sql_base, sql_hint, order_by):
        """Run both queries with ORDER BY wrapper and assert identical results."""
        q1 = self._query_ordered(sql_base, order_by)
        q2 = self._query_ordered(sql_hint, order_by)

        tdSql.query(q1, queryTimes=1)
        res1 = tdSql.queryResult
        rows1 = tdSql.queryRows

        tdSql.query(q2, queryTimes=1)
        res2 = tdSql.queryResult
        rows2 = tdSql.queryRows

        if rows1 != rows2:
            tdLog.exit(
                f"row count mismatch: base={rows1} vs hint={rows2}\n"
                f"  base: {q1}\n  hint: {q2}"
            )

        for i in range(rows1):
            if res1[i] != res2[i]:
                tdLog.exit(
                    f"data mismatch at row {i}:\n"
                    f"  base row: {res1[i]}\n  hint row: {res2[i]}\n"
                    f"  base sql: {q1}\n  hint sql: {q2}"
                )
        tdLog.debug(f"compare OK ({rows1} rows): {sql_base[:80]}...")

    def _compare_with_hint(self, sql, order_by):
        """Compare a SQL with and without sort_for_group() hint."""
        self._compare_results(sql, self._add_hint(sql), order_by)

    def _check_both_empty(self, sql):
        """Verify a SQL returns 0 rows both with and without the hint."""
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)
        tdSql.query(self._add_hint(sql), queryTimes=1)
        tdSql.checkRows(0)

    # ------------------------------------------------------------------
    # data setup: multi-fileset with multiple tag groups
    # ------------------------------------------------------------------

    def _setup_multi_fileset_data(self):
        """Create database with multiple filesets and tag groups.

        Layout:
          - 3 sites × 2 zones × 2 trackers = 12 child tables with data
          - 4 extra empty child tables (site='empty', various zone/tracker)
          - 3 flushes → 3 filesets (data at different time ranges)
        """
        self._create_db(duration="1d", vgroups=2)
        self._create_stb()

        ctable_specs = []
        idx = 0
        sites = ["bj", "sh", "gz"]
        for site in sites:
            for zone in [1, 2]:
                for tracker in [1, 2]:
                    ctable_specs.append((f"d{idx}", site, zone, tracker))
                    idx += 1

        # empty tables: these tag combinations will form empty groups
        for i in range(4):
            ctable_specs.append((f"empty{i}", "empty", i + 1, 1))

        self._create_ctables(ctable_specs)

        # Fileset 1: day 1
        # Offset each table by i ms so timestamps don't collide when
        # multiple tables are merged under the same partition key
        # (e.g. partition by site groups 4 tables per site).
        base_ts = 1700000000000  # 2023-11-14
        for i in range(12):
            self._insert_rows(f"d{i}", base_ts + i, count=100, step_ms=1000)
        self._flush()

        # Fileset 2: day 2
        day2_ts = base_ts + 86400000
        for i in range(12):
            self._insert_rows(f"d{i}", day2_ts + i, count=100, step_ms=1000)
        self._flush()

        # Fileset 3: day 3
        day3_ts = base_ts + 2 * 86400000
        for i in range(12):
            self._insert_rows(f"d{i}", day3_ts + i, count=80, step_ms=1000)
        self._flush()

    # ------------------------------------------------------------------
    # 7.1-1: state_window(bool_col) partition by tag1, tag2, tag3
    # ------------------------------------------------------------------

    def do_state_window_partition_by_tags(self):
        self._setup_multi_fileset_data()

        # partition by single tag
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site state_window(vb)"
        )
        # Note: sort_for_group() hint does NOT produce a "Sort" node for
        # tag-only partition — Table Merge Scan is already the default path.
        # We only verify result correctness here.
        self._compare_with_hint(sql, "_wstart, site")

        # partition by two tags
        sql = (
            f"select _wstart, _wend, count(*), min(vi), max(vi), site, zone "
            f"from {self.STB} partition by site, zone state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        # partition by three tags (the scenario from perf analysis)
        sql = (
            f"select _wstart, _wend, count(*), sum(vi), site, zone, tracker "
            f"from {self.STB} partition by site, zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # partition by tbname + tags
        sql = (
            f"select _wstart, _wend, count(*), tbname, site "
            f"from {self.STB} partition by tbname, site state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, tbname, site")

        # partition by tags with aggregation functions
        sql = (
            f"select _wstart, _wend, count(*), first(vi), last(vi), avg(vf), "
            f"site, zone, tracker "
            f"from {self.STB} partition by site, zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # state_window on integer column (not just bool)
        sql = (
            f"select _wstart, _wend, count(*), site, zone "
            f"from {self.STB} partition by site, zone state_window(vi)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        print("state_window partition by tags ......... [passed]")

    # ------------------------------------------------------------------
    # 7.1-2: multi group / multi fileset / single fileset
    # ------------------------------------------------------------------

    def do_multi_fileset_coverage(self):
        # multi-fileset already set up in do_state_window_partition_by_tags
        # Verify: 3 filesets are exercised via full time range query
        sql = (
            f"select _wstart, _wend, count(*), site, zone, tracker "
            f"from {self.STB} partition by site, zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # Restrict to single fileset (day 1 only)
        sql = (
            f"select _wstart, _wend, count(*), site, zone, tracker "
            f"from {self.STB} "
            f"where ts >= 1700000000000 and ts < 1700086400000 "
            f"partition by site, zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # Restrict to two filesets (day 1 + day 2)
        sql = (
            f"select _wstart, _wend, count(*), site, zone, tracker "
            f"from {self.STB} "
            f"where ts >= 1700000000000 and ts < 1700172800000 "
            f"partition by site, zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # Non-state-window: interval + partition by tags across filesets
        sql = (
            f"select _wstart, count(*), sum(vi), site, zone "
            f"from {self.STB} partition by site, zone interval(1d)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        # session window across filesets
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site session(ts, 2s)"
        )
        self._compare_with_hint(sql, "_wstart, site")

        print("multi fileset coverage ................ [passed]")

    # ------------------------------------------------------------------
    # 7.1-3: empty group (no matching rows)
    # ------------------------------------------------------------------

    def do_empty_group(self):
        # The 4 empty tables (site='empty') should form groups with 0 rows.
        # state_window should produce no output for those groups, but should
        # not affect other groups.

        # partition by site: 'empty' site should have 0 state windows
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site")

        # Verify the empty groups are truly absent from results
        tdSql.query(
            f"select distinct site from ("
            f"  select _wstart, site from {self.STB} "
            f"  partition by site state_window(vb)"
            f") _t"
        )
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) == "empty":
                tdLog.exit("empty group should not appear in state_window results")

        # partition by zone: zone 3, 4 only exist for empty site
        sql = (
            f"select _wstart, _wend, count(*), site, zone "
            f"from {self.STB} partition by site, zone state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        # Filter that selects no rows at all
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} where vi > 999999 "
            f"partition by site state_window(vb)"
        )
        self._check_both_empty(sql)

        # Agg on partition by tags: empty groups counted
        sql = (
            f"select count(*), site from {self.STB} partition by site"
        )
        self._compare_with_hint(sql, "site")

        print("empty group ........................... [passed]")

    # ------------------------------------------------------------------
    # 7.1-4: UNION ALL
    # ------------------------------------------------------------------

    def do_union_all(self):
        # Both branches use merge scan (sort_for_group hint)
        sql_branch1 = (
            f"select _wstart as ws, _wend as we, count(*) as cnt, site "
            f"from {self.STB} "
            f"where ts >= 1700000000000 and ts < 1700086400000 "
            f"partition by site state_window(vb)"
        )
        sql_branch2 = (
            f"select _wstart as ws, _wend as we, count(*) as cnt, site "
            f"from {self.STB} "
            f"where ts >= 1700086400000 and ts < 1700172800000 "
            f"partition by site state_window(vb)"
        )

        sql_base = f"{sql_branch1} union all {sql_branch2}"
        sql_hint = (
            f"{self._add_hint(sql_branch1)} union all "
            f"{self._add_hint(sql_branch2)}"
        )
        self._compare_results(sql_base, sql_hint, "ws, site")

        # UNION ALL with different partition keys
        sql_branch_a = (
            f"select _wstart as ws, count(*) as cnt, site "
            f"from {self.STB} partition by site state_window(vb)"
        )
        sql_branch_b = (
            f"select _wstart as ws, count(*) as cnt, zone as site "
            f"from {self.STB} partition by zone state_window(vb)"
        )
        sql_base = f"{sql_branch_a} union all {sql_branch_b}"
        sql_hint = (
            f"{self._add_hint(sql_branch_a)} union all "
            f"{self._add_hint(sql_branch_b)}"
        )
        self._compare_results(sql_base, sql_hint, "ws, site, cnt")

        # Three-way UNION ALL
        sql_day3 = (
            f"select _wstart as ws, _wend as we, count(*) as cnt, site "
            f"from {self.STB} "
            f"where ts >= 1700172800000 and ts < 1700259200000 "
            f"partition by site state_window(vb)"
        )
        sql_base = f"{sql_branch1} union all {sql_branch2} union all {sql_day3}"
        sql_hint = (
            f"{self._add_hint(sql_branch1)} union all "
            f"{self._add_hint(sql_branch2)} union all "
            f"{self._add_hint(sql_day3)}"
        )
        self._compare_results(sql_base, sql_hint, "ws, site")

        # UNION ALL with non-state-window
        sql_a = (
            f"select count(*) as cnt, site from {self.STB} partition by site"
        )
        sql_b = (
            f"select count(*) as cnt, zone as site from {self.STB} "
            f"partition by zone"
        )
        sql_base = f"{sql_a} union all {sql_b}"
        sql_hint = (
            f"{self._add_hint(sql_a)} union all {self._add_hint(sql_b)}"
        )
        self._compare_results(sql_base, sql_hint, "cnt, site")

        print("union all ............................. [passed]")

    # ------------------------------------------------------------------
    # 7.1-5: LIMIT / OFFSET in merge scan path
    # ------------------------------------------------------------------

    def do_limit_offset(self):
        # SLIMIT on partition by tags + state_window
        sql = (
            f"select _wstart, _wend, count(*), site, zone, tracker "
            f"from {self.STB} "
            f"partition by site, zone, tracker state_window(vb) "
            f"slimit 3"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # SLIMIT + SOFFSET
        sql = (
            f"select _wstart, _wend, count(*), site, zone, tracker "
            f"from {self.STB} "
            f"partition by site, zone, tracker state_window(vb) "
            f"slimit 5 soffset 2"
        )
        self._compare_with_hint(sql, "_wstart, site, zone, tracker")

        # LIMIT per group
        sql = (
            f"select _wstart, _wend, count(*), site, zone "
            f"from {self.STB} "
            f"partition by site, zone state_window(vb) "
            f"limit 2"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        # SLIMIT + LIMIT combined
        sql = (
            f"select _wstart, _wend, count(*), site, zone "
            f"from {self.STB} "
            f"partition by site, zone state_window(vb) "
            f"slimit 4 limit 3"
        )
        self._compare_with_hint(sql, "_wstart, site, zone")

        # partition by without state_window + LIMIT
        sql = (
            f"select count(*), sum(vi), site, zone "
            f"from {self.STB} partition by site, zone "
            f"slimit 3"
        )
        self._compare_with_hint(sql, "site, zone")

        # partition by + interval + SLIMIT
        sql = (
            f"select _wstart, count(*), site "
            f"from {self.STB} partition by site interval(1d) "
            f"slimit 2"
        )
        self._compare_with_hint(sql, "_wstart, site")

        # partition by + interval + SLIMIT + LIMIT
        sql = (
            f"select _wstart, count(*), site "
            f"from {self.STB} partition by site interval(1d) "
            f"slimit 2 limit 1"
        )
        self._compare_with_hint(sql, "_wstart, site")

        # Edge: SLIMIT larger than group count
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site state_window(vb) "
            f"slimit 1000"
        )
        self._compare_with_hint(sql, "_wstart, site")

        # Edge: LIMIT 0
        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site state_window(vb) "
            f"limit 0"
        )
        self._check_both_empty(sql)

        print("limit offset .......................... [passed]")

    # ------------------------------------------------------------------
    # 7.1-extra: single group (no regression on degenerate case)
    # ------------------------------------------------------------------

    def do_single_group(self):
        """Single group should not regress — no reader reuse needed."""
        self._create_db(duration="10d", vgroups=1)
        self._create_stb()
        self._create_ctables([("solo0", "x", 1, 1)])
        self._insert_rows("solo0", 1700000000000, count=200, step_ms=1000)
        self._flush()

        sql = (
            f"select _wstart, _wend, count(*), site "
            f"from {self.STB} partition by site state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, site")

        # partition by tbname (single table)
        sql = (
            f"select _wstart, _wend, count(*), tbname "
            f"from {self.STB} partition by tbname state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, tbname")

        print("single group .......................... [passed]")

    # ------------------------------------------------------------------
    # 7.1-extra: only STT data (no data file blocks)
    # ------------------------------------------------------------------

    def do_stt_only(self):
        """Data only in STT (small writes, no compaction to data blocks)."""
        self._create_db(duration="1d", vgroups=1)
        self._create_stb()

        specs = []
        for i in range(6):
            specs.append((f"stt{i}", "s", i % 3, i % 2))
        self._create_ctables(specs)

        # Small write (below minRows threshold) → stays in STT
        base_ts = 1700000000000
        for i in range(6):
            self._insert_rows(f"stt{i}", base_ts, count=10, step_ms=1000)
        self._flush()

        sql = (
            f"select _wstart, _wend, count(*), zone, tracker "
            f"from {self.STB} partition by zone, tracker state_window(vb)"
        )
        self._compare_with_hint(sql, "_wstart, zone, tracker")

        sql = (
            f"select count(*), sum(vi), zone "
            f"from {self.STB} partition by zone"
        )
        self._compare_with_hint(sql, "zone")

        print("stt only .............................. [passed]")

    # ------------------------------------------------------------------
    # main
    # ------------------------------------------------------------------

    def test_table_merge_scan_stt_reuse(self):
        """Table Merge Scan STT reuse correctness

        Verify sort_for_group (Table Merge Scan) produces identical results
        to the default partition path across various scenarios:
        1. state_window(bool) partition by multiple tags
        2. multi group / multi fileset / single fileset
        3. empty group (no matching rows for some tag combinations)
        4. UNION ALL with merge scan branches
        5. LIMIT / OFFSET in merge scan path
        6. single group (degenerate, no regression)
        7. STT-only data (no data file blocks)

        Catalog:
            - Query:partition

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-29 Tony Zhang Created for Phase 1 STT reuse optimization

        """
        self.do_state_window_partition_by_tags()
        self.do_multi_fileset_coverage()
        self.do_empty_group()
        self.do_union_all()
        self.do_limit_offset()
        self.do_single_group()
        self.do_stt_only()
