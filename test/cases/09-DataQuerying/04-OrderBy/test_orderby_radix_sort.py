# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL-3.0-or-later") as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import re
from collections import defaultdict

from new_test_framework.utils import tdLog, tdSql

INT32_MIN = -2147483648
INT32_MAX = 2147483647
UINT32_MAX = 4294967295
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1
UINT64_MAX = 2 ** 64 - 1


class TestOrderByRadixSort:
    dbname = "orderby_radix"
    rows = 5000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_db(self):
        tdSql.prepare(dbname=self.dbname, drop=True, vgroups=1, precision="ms")

    def value_of(self, i):
        # Deterministic mix that includes both extremes, negatives and repeats.
        pick = i % 20
        if pick == 0:
            return INT32_MIN
        if pick == 1:
            return INT32_MAX
        if pick == 2:
            return -1
        if pick == 3:
            return 0
        if pick == 4:
            return INT32_MAX - 1
        return (i * 7919) % 200000 - 100000

    def build_table(self, table, rows, null_every=0):
        tdSql.execute(f"create table {table}(ts timestamp, i8 tinyint, i16 smallint, i32 int, "
                      f"u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, "
                      f"big bigint, ubig bigint unsigned, msg nchar(20))")
        batch = 500
        for start in range(0, rows, batch):
            stop = min(start + batch, rows)
            vals = []
            for i in range(start, stop):
                v = self.value_of(i)
                if null_every and i % null_every == 0:
                    vals.append(f"({1700000000000 + i},NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
                    continue
                u = i % 200000
                i8 = max(-128, min(127, v % 251 - 125))
                i16 = max(-32768, min(32767, v % 60001 - 30000))
                if i % 50 == 0:
                    big = (INT64_MIN, INT64_MAX, -1, 0, 1)[(i // 50) % 5]
                    # Cover the top unsigned bit explicitly: a wrong sign flip
                    # on an unsigned key is invisible while bit 63 stays clear.
                    ubig = (0, 2 ** 63, 2 ** 63 + 1, UINT64_MAX, UINT64_MAX - 1, 2 ** 32 + 1)[
                        (i // 50) % 6
                    ]
                else:
                    big = (i * 2654435761) % (2 ** 63) - 2 ** 62
                    ubig = (i * 0x9E3779B97F4A7C15) & UINT64_MAX
                # msg exercises the varlen fallback next to the fixed-width fast path
                vals.append(f"({1700000000000 + i},{i8},{i16},{v},{u % 256},{u % 65536},{u},{big},{ubig},'m{i % 97}')")
            tdSql.execute(f"insert into {table} values " + ",".join(vals))

    @staticmethod
    def second_column(column):
        return "ubig" if column != "ubig" else "i32"

    def assert_sort_plan(self, table, column):
        # The matrix must actually execute the Sort operator with a two-column
        # block: a single projected column takes the in-place sort path and
        # ORDER BY ts is folded into the scan, neither of which reaches radix.
        extra = self.second_column(column)
        tdSql.query(f"explain verbose true select {column}, {extra} from {table} order by {column}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult if row)
        tdSql.checkEqual(re.search(r"-> Sort[^\n]*columns=2", plan) is not None, True)

    def check_column(self, table, column, direction, expected_rows):
        # Two projected columns keep the sort out of the in-place path.
        tdSql.query(f"select {column}, {self.second_column(column)} from {table} order by {column} {direction}")
        actual = [row[0] for row in tdSql.queryResult]
        tdSql.checkRows(expected_rows)
        nonnull = [v for v in actual if v is not None]
        expected = sorted(nonnull, reverse=(direction == "desc"))
        tdSql.checkEqual(nonnull, expected)
        # The matrix tables are built without NULLs, so every row must carry a
        # key: an independent count, not an identity on the fetched length.
        tdSql.checkEqual(len(nonnull), expected_rows)

    def do_radix_matrix(self):
        self.build_table("radix_all", self.rows)
        expected = self.rows
        # TIMESTAMP is not listed: ORDER BY ts is folded into the scan order
        # (both directions), so no Sort exists to exercise. TIMESTAMP maps to
        # the same I64 radix branch as BIGINT, which is covered below.
        for column in ("i8", "i16", "i32", "u8", "u16", "u32", "big", "ubig"):
            self.assert_sort_plan("radix_all", column)
            for direction in ("asc", "desc"):
                self.check_column("radix_all", column, direction, expected)

    def do_payload_integrity(self):
        # Monotonicity alone would still pass if the payload of a non-key column
        # were copied wrong. Compare full rows grouped by the sort key: the tie
        # order is unspecified, the per-key multiset is not.
        for key in ("i32", "big"):
            tdSql.query(f"select {key}, ts, ubig, i8, msg from radix_all")
            unordered = [tuple(row) for row in tdSql.queryResult]
            tdSql.query(f"select {key}, ts, ubig, i8, msg from radix_all order by {key}")
            ordered = [tuple(row) for row in tdSql.queryResult]

            def grouped(rows):
                buckets = defaultdict(list)
                for row in rows:
                    buckets[row[0]].append(row)
                return {k: sorted(v) for k, v in buckets.items()}

            tdSql.checkEqual(grouped(ordered), grouped(unordered))

    def do_desc_tie_order(self):
        # Descending radix completes the key instead of reversing the sorted
        # permutation, so equal keys keep the same relative order as ascending.
        # Reversing the permutation (the previous behaviour) flipped every tie
        # within a block while leaving the key order correct.
        def tie_order(direction):
            tdSql.query(f"select i32, ts from radix_all order by i32 {direction}")
            buckets = defaultdict(list)
            for value, ts in tdSql.queryResult:
                if value is not None:
                    buckets[value].append(ts)
            return buckets

        tdSql.checkEqual(tie_order("asc"), tie_order("desc"))

    def do_fallback_cases(self):
        # Fewer rows than the radix threshold: the comparison sort path.
        self.build_table("radix_small", 100)
        self.check_column("radix_small", "i32", "asc", 100)

        # NULLs disable the radix path; the non-NULL part must still be ordered
        # and the NULL rows must not be dropped.
        self.build_table("radix_null", 3000, null_every=7)
        # Independent expectation from the generator contract: rows with
        # i % 7 == 0 (i counted from 0) are all-NULL, i.e. 429 of 3000.
        expected_nulls = len(range(0, 3000, 7))
        tdSql.checkEqual(expected_nulls, 429)
        tdSql.query("select i32 from radix_null order by i32 asc")
        values = [row[0] for row in tdSql.queryResult]
        tdSql.checkRows(3000)
        nonnull = [v for v in values if v is not None]
        tdSql.checkEqual(len(nonnull), 3000 - expected_nulls)
        tdSql.checkEqual(nonnull, sorted(nonnull))
        # ASC treats NULL as the smallest key: all NULLs must sit at the head.
        tdSql.checkEqual(values[:expected_nulls], [None] * expected_nulls)
        # And the non-NULL keys are exactly the generated multiset.
        tdSql.checkEqual(sorted(nonnull), sorted(self.value_of(i) for i in range(3000) if i % 7))

    def test_orderby_radix_sort(self):
        """ Fun: ORDER BY a single fixed-width key covers the radix sort path

        1. Build a table larger than the radix threshold with extremes and repeats.
        2. Verify the 1/2/4/8-byte signed/unsigned widths in both directions
           against Python sorting, asserting the plan really sorts a two-column
           block so the radix path is reached.
        3. Verify the small-table and NULL fallbacks keep the rows and the order.
        4. Verify descending keeps the tie order of ascending (stable radix).

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-09 Yihao Deng Add radix sort coverage for 6537430348
            - 2026-11-14 Yihao Deng Cover descending tie stability (6537430348)

        """
        self.prepare_db()
        self.do_radix_matrix()
        self.do_payload_integrity()
        self.do_desc_tie_order()
        self.do_fallback_cases()
