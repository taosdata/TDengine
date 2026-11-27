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
