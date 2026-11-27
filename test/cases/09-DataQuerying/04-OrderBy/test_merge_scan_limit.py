# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import re
import time

from new_test_framework.utils import tdLog, tdSql


class TestMergeScanLimit:
    """Verify Table Merge Scan LIMIT pruning keeps full-order semantics."""

    DB = "test_merge_scan_limit"
    BASE_TS = 1700000000000
    TABLES = 8
    ROWS_PER_TABLE = 400

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _query_rows(self, sql):
        tdSql.query(sql, show=0)
        return [
            tuple(tdSql.getData(row, col) for col in range(len(tdSql.queryResult[row])))
            for row in range(tdSql.getRows())
        ]

    def _setup_data(self):
        tdSql.execute(f"drop database if exists {self.DB}", show=1)
        tdSql.execute(f"create database {self.DB} vgroups 1 precision 'ms'", show=1)
        tdSql.execute(
            f"create stable {self.DB}.mt (ts timestamp, id int primary key, v int) tags (gid int)",
            show=1,
        )
        # Table i, row j: the timestamp ranges of all tables overlap on purpose
        # so the merged stream contains many cross-table ts ties, while
        # id = i * 100000 + j keeps every full (ts, id) key globally unique and
        # the expected order deterministic.
        for i in range(self.TABLES):
            tdSql.execute(
                f"create table {self.DB}.t{i} using {self.DB}.mt tags ({i})", show=1
            )
            values = " ".join(
                f"({self.BASE_TS + (i * 37 + j * 11) % 2000},{i * 100000 + j},{i * 1000 + j})"
                for j in range(self.ROWS_PER_TABLE)
            )
            tdSql.execute(f"insert into {self.DB}.t{i} values {values}", show=1)
        tdSql.execute(f"flush database {self.DB}", show=1)

    def _assert_limit_matches_full(self, order_by, limit, offset=0, select="ts, id, v", table=None):
        source = f"{self.DB}.{table}" if table else f"{self.DB}.mt"
        full_sql = f"select {select} from {source} order by {order_by}"
        full_rows = self._query_rows(full_sql)
        limit_sql = f"{full_sql} limit {limit} offset {offset}"
        limit_rows = self._query_rows(limit_sql)
        expected = full_rows[offset:offset + limit]
        assert limit_rows == expected, (
            f"merge-scan LIMIT differs from the full order for {source} "
            f"ORDER BY {order_by} LIMIT {limit} OFFSET {offset}: "
            f"{limit_rows} != {expected}"
        )

    def test_merge_scan_limit_results(self):
        """Merge scan LIMIT returns the same rows as the full ordering

        Table Merge Scan prunes per-table input at a timestamp value boundary
        and skips tables whose first row is beyond the live LIMIT threshold.
        Verify the pruned output is row-for-row identical to slicing the
        untruncated ordering, including cross-table timestamp ties that are
        broken by the primary key, LIMIT with OFFSET, both directions, a
        single-table control and an unlimited control run.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-11-16 yihaoDeng Add merge scan LIMIT regression coverage.
        """
        self._setup_data()

        # Merge scan path: ORDER BY ts with the primary key selected makes the
        # merge sort by (ts, id), so cross-table timestamp ties are broken by
        # the primary key. This is the shape the ts-boundary table cut must
        # preserve, including ties split across blocks of the same table.
        self._assert_limit_matches_full("ts", 10)
        self._assert_limit_matches_full("ts", 37)
        self._assert_limit_matches_full("ts", 7, offset=5)
        self._assert_limit_matches_full("ts", 13, offset=201)
        self._assert_limit_matches_full("ts desc", 10)
        self._assert_limit_matches_full("ts desc", 7, offset=5)

        # Sort-operator path (ORDER BY ts, id) must agree with the same full
        # ordering as well; the merge scan change must not affect it.
        self._assert_limit_matches_full("ts, id", 10)
        self._assert_limit_matches_full("ts, id", 7, offset=5)
        self._assert_limit_matches_full("ts desc, id desc", 10)

        # Timestamp-only ordering without the key selected: the ts sequence is
        # deterministic even though ties have no SQL-defined row order.
        full_ts = [row[0] for row in self._query_rows(
            f"select ts from {self.DB}.mt order by ts"
        )]
        for limit, offset in ((10, 0), (7, 5), (50, 300)):
            rows = self._query_rows(
                f"select ts from {self.DB}.mt order by ts limit {limit} offset {offset}"
            )
            assert [row[0] for row in rows] == full_ts[offset:offset + limit], (
                f"timestamp slice mismatch for LIMIT {limit} OFFSET {offset}"
            )

        # Single-table control: the data of one table alone must stay exact.
        self._assert_limit_matches_full("ts", 9, offset=2, table="t0")

        # Unlimited control: the reference itself must return every row.
        all_rows = self._query_rows(f"select ts, id, v from {self.DB}.mt order by ts, id")
        assert len(all_rows) == self.TABLES * self.ROWS_PER_TABLE, len(all_rows)

    def test_merge_scan_limit_budget_above_heap_cap(self):
        """Merge scan LIMIT above the live-threshold heap cap keeps every row

        The live prune threshold is only valid while its heap holds the whole
        LIMIT+OFFSET budget. Once the budget exceeds the 4096-entry cap the
        merge scan must fall back to the per-table timestamp cut instead of
        pruning on a k-th best bound, otherwise a table starting after the cap
        value is skipped and the query returns fewer rows than requested.
        Verify a 4200-row budget over two tables whose first table alone fills
        the cap returns exactly the first 4200 rows of the full ordering, in
        both directions.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-11-16 yihaoDeng Cover budgets above the live-threshold heap cap.
        """
        db = "test_merge_scan_limit_cap"
        tdSql.execute(f"drop database if exists {db}", show=1)
        tdSql.execute(f"create database {db} vgroups 1 precision 'ms'", show=1)
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, id int primary key, v int) tags (gid int)",
            show=1,
        )
        # Two tables with disjoint timestamp ranges: the first table alone
        # holds more rows than the live-threshold heap cap, so its rows fill
        # the heap before the second table is fetched. A k-th best prune bound
        # would skip the second table and stop short of the requested budget.
        for i, (start, table_rows) in enumerate(
            ((self.BASE_TS, 5000), (self.BASE_TS + 100000, 1000))
        ):
            tdSql.execute(f"create table {db}.t{i} using {db}.mt tags ({i})", show=1)
            for base in range(0, table_rows, 1000):
                values = " ".join(
                    f"({start + j},{i * 100000 + j},{j})"
                    for j in range(base, min(base + 1000, table_rows))
                )
                tdSql.execute(f"insert into {db}.t{i} values {values}", show=1)
        tdSql.execute(f"flush database {db}", show=1)

        limit = 4200
        for order_by in ("ts", "ts desc"):
            full = self._query_rows(f"select ts, id, v from {db}.mt order by {order_by}")
            assert len(full) == 6000, len(full)
            rows = self._query_rows(
                f"select ts, id, v from {db}.mt order by {order_by} limit {limit}"
            )
            assert len(rows) == limit, (order_by, len(rows))
            assert rows == full[:limit], (
                f"merge-scan LIMIT {order_by} dropped rows above the heap cap"
            )

    def _check_pruning_before_filter(self):
        db = "test_merge_scan_filter_prune"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, id int primary key, "
            "protocol varchar(32)) tags (gid int)"
        )
        phases = (0, 7, 1, 6, 2, 5, 3, 4)
        rows_per_table = 128
        for table, phase in enumerate(phases):
            tdSql.execute(f"create table {db}.t{table} using {db}.mt tags (0)")
            values = " ".join(
                f"({self.BASE_TS + row * 1000 + phase},"
                f"{table * rows_per_table + row},"
                f"'{'hit' if row % 3 != 2 else 'miss'}')"
                for row in range(rows_per_table)
            )
            tdSql.execute(f"insert into {db}.t{table} values {values}")
        tdSql.execute(f"flush database {db}")

        checked_rows = []
        for order in ("asc", "desc"):
            full_sql = (f"select ts, id, protocol from {db}.mt "
                        f"where protocol match '^hit$' order by ts {order}")
            expected = self._query_rows(full_sql)
            for limit, offset in ((1, 0), (5, 3), (900, 0)):
                limited_sql = f"{full_sql} limit {limit} offset {offset}"
                rows = self._query_rows(limited_sql)
                assert rows == expected[offset:offset + limit], limited_sql

            plan = "\n".join(row[0] for row in self._query_rows(
                f"explain analyze verbose true {full_sql} limit 2 offset 1"
            ))
            assert "Table Merge Scan" in plan, plan
            counts = re.findall(r"\bcheck_rows=(\d+)", plan)
            assert len(counts) == 1 and int(counts[0]) > 0, plan
            checked_rows.append(int(counts[0]))

        assert max(checked_rows) < len(phases) * rows_per_table, checked_rows

    def test_merge_scan_limit_prunes_before_filter(self):
        """Merge scan LIMIT skips out-of-range blocks before filtering

        Check the scanned row count for a filtered timestamp Top-N over
        overlapping sensor ranges with staggered sampling. Compare the
        limited results to the unlimited query for both directions, OFFSET
        and fewer matches than requested.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: TD-32748

        History:
            - 2026-11-20 Tony Zhang Use overlapping sensor ranges.
            - 2026-11-19 Tony Zhang Cover pruning before regex evaluation.
        """
        self._check_pruning_before_filter()

    def _check_pruning_before_load(self):
        db = "test_merge_scan_load_prune"
        phases = (0, 5, 1, 4, 2, 3)
        tables, table_blocks, block_rows = len(phases), 3, 400
        table_rows = table_blocks * block_rows
        block_count = tables * table_blocks
        row_count = tables * table_rows
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups 1 stt_trigger 1 "
            f"minrows 10 maxrows {block_rows}"
        )
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, id int, v int) "
            "tags (gid int)"
        )
        for table, phase in enumerate(phases):
            tdSql.execute(f"create table {db}.t{table} using {db}.mt tags (0)")
            for start in range(0, table_rows, block_rows):
                values = " ".join(
                    f"({self.BASE_TS + row * 1000 + phase},"
                    f"{row * tables + phase},{(row * tables + phase) * 2})"
                    for row in range(start, start + block_rows)
                )
                tdSql.execute(f"insert into {db}.t{table} values {values}")
        tdSql.execute(f"flush database {db}")

        # FLUSH schedules the commit. Wait for the reader to see file blocks.
        probe = (f"explain analyze verbose true select id, v from {db}.mt "
                 "order by ts asc limit 2 offset 1")
        deadline = time.monotonic() + 30
        while True:
            plan = "\n".join(row[0] for row in self._query_rows(probe))
            file_blocks = re.findall(r"\bfile_load_blocks=(\d+)", plan)
            if file_blocks == [str(block_count)]:
                break
            assert time.monotonic() < deadline, plan
            time.sleep(0.2)

        for order in ("asc", "desc"):
            query = f"select id, v from {db}.mt order by ts {order}"
            ids = range(row_count)
            if order == "desc":
                ids = ids[::-1]
            for limit, offset in ((2, 1), (5, 0), (7, 6),
                                  (5, block_rows * tables - 2), (5, 4091),
                                  (5, 4092), (5, row_count - 2)):
                expected = [
                    (row, row * 2) for row in ids[offset:offset + limit]
                ]
                sql = f"{query} limit {limit} offset {offset}"
                assert self._query_rows(sql) == expected, sql

            plan = "\n".join(row[0] for row in self._query_rows(
                f"explain analyze verbose true {query} limit 2 offset 1"
            ))
            assert "Table Merge Scan" in plan, plan
            assert "stt_load_blocks=0" in plan, plan
            assert "composed_blocks=0" in plan, plan
            file_blocks = re.findall(r"\bfile_load_blocks=(\d+)", plan)
            assert file_blocks == [str(block_count)], plan
            checked = re.search(r"\bcheck_rows=(\d+)", plan)
            assert checked and 0 < int(checked[1]) < tables * block_rows, plan
            total = re.search(r"\btotal_blocks=(\d+)", plan)
            assert total and int(total[1]) == tables, plan
        tdSql.execute(f"drop database {db}")
        print("Merge scan pruning before loading [passed]")

    def test_merge_scan_limit_prunes_before_load(self):
        """Merge scan LIMIT skips unloaded file blocks without a filter

        Use several file blocks per sensor with overlapping time ranges.
        Verify that both directions skip disqualified blocks and the rest
        of their subtables. Compare LIMIT and OFFSET across file block
        boundaries to the generated ordering.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: TD-32748

        History:
            - 2026-11-20 Tony Zhang Use overlapping sensor ranges.
            - 2026-11-19 Tony Zhang Cover unfiltered pruning before block load.
        """
        self._check_pruning_before_load()

    def do_filtered_table_skip(self):
        db = "test_merge_scan_filtered_table_skip"
        tables, block_rows, table_rows = 6, 400, 1200
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups 1 stt_trigger 1 keep 36500d "
            f"minrows 10 maxrows {block_rows}"
        )
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, v int) tags (gid int)"
        )
        matching = []
        for table in range(tables):
            tdSql.execute(f"create table {db}.t{table} using {db}.mt tags (0)")
            for start in range(0, table_rows, block_rows):
                values = []
                for row in range(start, start + block_rows):
                    ts = self.BASE_TS + (row + table * 60) * 1000 + table
                    value = row * tables + table
                    values.append(f"({ts},{value})")
                    if value % 7 == 0:
                        matching.append((ts, value))
                tdSql.execute(f"insert into {db}.t{table} values "
                              + " ".join(values))
        tdSql.execute(f"flush database {db}")

        prefix = f"select v from {db}.mt where v % 7 = 0 order by ts"
        explain = "explain analyze verbose true "
        probe = f"{explain}{prefix} asc limit 5 offset 1"
        file_blocks = tables * (table_rows // block_rows)
        deadline = time.monotonic() + 30
        while True:
            plan = "\n".join(row[0] for row in self._query_rows(probe))
            loaded = re.findall(r"\bfile_load_blocks=(\d+)", plan)
            if loaded == [str(file_blocks)]:
                break
            assert time.monotonic() < deadline, plan
            time.sleep(0.2)

        for order in ("asc", "desc"):
            expected = sorted(matching, reverse=order == "desc")[1:6]
            query = f"{prefix} {order} limit 5 offset 1"
            tdSql.query(query)
            tdSql.checkRows(len(expected))
            for index, (_, value) in enumerate(expected):
                tdSql.checkData(index, 0, value)

            plan = "\n".join(
                row[0] for row in self._query_rows(explain + query)
            )
            assert "Table Merge Scan" in plan, plan
            total = re.findall(r"\btotal_blocks=(\d+)", plan)
            assert total == [str(tables)], plan
        tdSql.execute(f"drop database {db}")
        print("Filtered merge scan skips remaining table blocks [passed]")

    def test_merge_scan_limit_skips_filtered_table(self):
        """Merge scan LIMIT skips remaining blocks of filtered subtables

        Stagger subtable starts while keeping their time ranges overlapping.
        Verify the filtered result and that the reader delivers only the
        first block of each table in both scan directions.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: TD-32748

        History:
            - 2026-11-20 Tony Zhang Cover table skip before column filtering.
        """
        self.do_filtered_table_skip()

    def _merge_scan_plan(self, query):
        rows = self._query_rows(f"explain analyze verbose true {query}")
        plan = "\n".join(row[0] for row in rows)
        assert "Table Merge Scan" in plan, plan
        return plan

    def do_trim_crossing_blocks(self):
        db = "test_merge_scan_trim_crossing"
        tables, block_rows, table_rows, tie_rows = 4, 400, 1200, 8
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups 1 stt_trigger 1 keep 36500d "
            f"minrows 10 maxrows {block_rows}"
        )
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, id int primary key, "
            "protocol varchar(32)) tags (gid int)"
        )
        # Either end of each table supplies six matching rows at the same
        # timestamp. The other tables' first blocks therefore cross the cutoff
        # in either direction, whichever table the reader visits first.
        for table in range(tables):
            tdSql.execute(
                f"create table {db}.t{table} using {db}.mt tags ({table})"
            )
            for start in range(0, table_rows, block_rows):
                values = []
                for row in range(start, start + block_rows):
                    ts = self.BASE_TS + row * 1000
                    if row < tie_rows:
                        ts = self.BASE_TS
                    elif row >= table_rows - tie_rows:
                        ts = self.BASE_TS + table_rows * 1000
                    protocol = "'hit'" if row % 4 != 3 else "NULL"
                    values.append(f"({ts},{row * tables + table},{protocol})")
                tdSql.execute(f"insert into {db}.t{table} values "
                              + " ".join(values))
        tdSql.execute(f"flush database {db}")

        prefix = (f"select ts, id, protocol, gid from {db}.mt "
                  "where protocol match '^hit$' order by ts")
        file_blocks = tables * (table_rows // block_rows)
        deadline = time.monotonic() + 30
        while True:
            plan = self._merge_scan_plan(f"{prefix} asc limit 5 offset 1")
            loaded = re.findall(r"\bfile_load_blocks=(\d+)", plan)
            if loaded == [str(file_blocks)]:
                break
            assert time.monotonic() < deadline, plan
            time.sleep(0.2)

        for order in ("asc", "desc"):
            full = f"{prefix} {order}"
            query = f"{full} limit 5 offset 1"
            expected = self._query_rows(full)[1:6]
            assert len(expected) == 5
            assert len({row[0] for row in expected}) == 1, expected
            assert len({row[3] for row in expected}) > 1, expected
            assert self._query_rows(query) == expected, query

            plan = self._merge_scan_plan(query)
            assert "composed_blocks=0" in plan, plan
            # The first block establishes the cutoff. Each later table only
            # checks its tied prefix, then skips the rest of the table.
            rows = block_rows + (tables - 1) * tie_rows
            assert re.findall(r"\bcheck_rows=(\d+)", plan) == [str(rows)], plan
            total = re.findall(r"\btotal_blocks=(\d+)", plan)
            assert total == [str(tables)], plan
        tdSql.execute(f"drop database {db}")
        print("Crossing blocks retain cutoff ties and skip tails [passed]")

    def test_merge_scan_limit_trims_crossing_blocks(self):
        """Filtered merge scan trims crossing blocks and keeps cutoff ties

        Force a block whose timestamp range crosses the live threshold in both
        directions. Compare pagination with the full filtered ordering and
        verify that only the tied prefix is checked on subsequent tables.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: TD-32748

        History:
            - 2026-11-23 Tony Zhang Cover row trimming before column filtering.
        """
        self.do_trim_crossing_blocks()

    def do_filtered_limit_across_filesets(self):
        db = "test_merge_scan_filtered_filesets"
        hour = 3600 * 1000
        first = self.BASE_TS // hour * hour
        middle = first + hour
        table_rows = 5000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 duration 1h "
                      "stt_trigger 16 keep 36500d")
        tdSql.execute(
            f"create stable {db}.mt (ts timestamp, id int primary key, "
            "v int) tags (gid int)"
        )
        for table in range(2):
            tdSql.execute(
                f"create table {db}.t{table} using {db}.mt tags ({table})"
            )
            tdSql.execute(
                f"insert into {db}.t{table} values "
                f"({first + table},{table - 4},1) "
                f"({middle + hour + table},{table - 2},1)"
            )
            for start in range(0, table_rows, 500):
                values = [
                    f"({middle + row * 2 + table},{row * 2 + table},"
                    f"{int(row % 8 != 7)})"
                    for row in range(start, start + 500)
                ]
                tdSql.execute(f"insert into {db}.t{table} values "
                              + " ".join(values))
        tdSql.execute(f"flush database {db}")

        filesets = ("select * from information_schema.ins_filesets "
                    f"where db_name = '{db}'")
        deadline = time.monotonic() + 30
        while len(self._query_rows(filesets)) != 3:
            assert time.monotonic() < deadline, filesets
            time.sleep(0.2)

        rows = [(row * 2 + table, 1) for row in range(table_rows)
                if row % 8 != 7 for table in range(2)]
        all_rows = [(-4, 1), (-3, 1)] + rows + [(-2, 1), (-1, 1)]
        prefix = f"select id, v from {db}.mt where v = 1"
        for order in ("asc", "desc"):
            ordered = rows if order == "asc" else rows[::-1]
            across = all_rows if order == "asc" else all_rows[::-1]
            query = f"{prefix} order by ts {order} limit 4 offset 1"
            # The page starts in an outer fileset and ends in the middle one.
            assert self._query_rows(query) == across[1:5], query
            plan = self._merge_scan_plan(query)
            assert "file_load_blocks=0" in plan, plan
            assert re.search(r"\bstt_load_blocks=[1-9]\d*", plan), plan

            checks = []
            for budget in (4096, 4097):
                offset = budget - 4
                query = (f"{prefix} and ts >= {middle} "
                         f"and ts < {middle + hour} "
                         f"order by ts {order} limit 4 offset {offset}")
                assert self._query_rows(query) == ordered[offset:budget], query
                plan = self._merge_scan_plan(query)
                assert "file_load_blocks=0" in plan, plan
                assert re.search(r"\bstt_load_blocks=[1-9]\d*", plan), plan
                check_rows, = re.findall(r"\bcheck_rows=(\d+)", plan)
                checks.append(int(check_rows))
            assert 0 < checks[0] < checks[1], (order, checks)
        tdSql.execute(f"drop database {db}")
        print("Filtered pages survive fileset and budget boundaries [passed]")

    def test_merge_scan_limit_filtered_fileset_and_budget_boundaries(self):
        """Filtered pagination survives fileset and candidate budget boundaries

        Query three STT filesets in both directions. Check a page across a
        fileset boundary and pages with LIMIT plus OFFSET of 4096 and 4097.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: TD-32748

        History:
            - 2026-11-23 Tony Zhang Cover filtered fileset and heap boundaries.
        """
        self.do_filtered_limit_across_filesets()

    def test_merge_scan_limit_plan(self):
        """Merge scan LIMIT keeps the merge-scan plan shape

        The pruning runs inside Table Merge Scan; it must not introduce a Sort
        operator or otherwise change the plan. Verify the explain output of a
        multi-table ORDER BY ts LIMIT still names Table Merge Scan and has no
        Sort node, and that the ordering direction is preserved.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-11-16 yihaoDeng Add merge scan LIMIT plan assertions.
        """
        self._setup_data()

        for sql, order in (
            (f"select ts, id, v from {self.DB}.mt order by ts limit 10", "asc"),
            (f"select ts, id, v from {self.DB}.mt order by ts desc limit 10", "desc"),
        ):
            tdSql.query("explain " + sql, show=0)
            plan = "\n".join(
                str(tdSql.getData(row, 0)) for row in range(tdSql.getRows())
            )
            tdLog.info(f"merge-scan LIMIT plan for {order}:\n{plan}")
            expected_order = "order=[asc|1 desc|0]" if order == "asc" else "order=[asc|0 desc|1]"
            assert "Table Merge Scan" in plan, plan
            assert "Sort" not in plan, plan
            assert expected_order in plan, plan
