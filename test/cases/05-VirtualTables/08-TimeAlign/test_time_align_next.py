# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""System tests for TIME_ALIGN NEXT (phase 2).

NEXT returns the first non-NULL value at or after the output row's timestamp,
within the same series.  Rows whose successor is not known yet are held by the
virtual-table scan until a later block resolves them (or the series ends).

Covers: basic trailing/leading NULLs, mixed PREV/NEXT columns, variable-length
values (including the empty string), filters/limits, aggregates (percentile uses
the two-pass path), interval windows, a 4200-row source that crosses the scan
block boundary, and virtual super tables whose children interleave on ts.
"""

from new_test_framework.utils import tdLog, tdSql

DB = "ta_align_next"
BASE_TS = 1767225600000
GAP_TS = 1767225600000
BLOCK_ROWS = 4200


def ts(offset):
    return BASE_TS + offset


def next_values(values):
    """Next non-NULL value at or after each row (reverse fill)."""
    res = [None] * len(values)
    nxt = None
    for i in range(len(values) - 1, -1, -1):
        if values[i] is not None:
            nxt = values[i]
        res[i] = nxt
    return res


class TestTimeAlignNext:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        tdSql.execute("create table t_seq(ts timestamp, v int, w int, s varchar(16))")
        tdSql.execute(
            f"insert into t_seq values "
            f"({ts(0)}, 1, 10, 'aa') "
            f"({ts(1000)}, NULL, NULL, NULL) "
            f"({ts(2000)}, NULL, 20, '') "
            f"({ts(3000)}, 3, NULL, NULL) "
            f"({ts(4000)}, NULL, 40, 'bb') "
            f"({ts(5000)}, 5, NULL, NULL) "
            f"({ts(6000)}, NULL, NULL, NULL)")
        tdSql.execute("create vtable vt_seq(ts timestamp, v int from t_seq.v, w int from t_seq.w, "
                      "s varchar(16) from t_seq.s)")

        tdSql.execute("create table t_lead(ts timestamp, v int)")
        tdSql.execute(f"insert into t_lead values ({ts(0)}, NULL) ({ts(1000)}, NULL) ({ts(2000)}, 7)")
        tdSql.execute("create vtable vt_lead(ts timestamp, v int from t_lead.v)")

        cls.big_values = [i if i % 100 == 0 else None for i in range(BLOCK_ROWS)]
        tdSql.execute("create table t_big(ts timestamp, v int)")
        cells = []
        for i, value in enumerate(cls.big_values):
            cells.append(f"({ts(i * 1000)}, {value if value is not None else 'NULL'})")
            if len(cells) == 500:
                tdSql.execute("insert into t_big values " + " ".join(cells))
                cells = []
        if cells:
            tdSql.execute("insert into t_big values " + " ".join(cells))
        tdSql.execute("create vtable vt_big(ts timestamp, v int from t_big.v)")

        tdSql.execute("create table t_c1(ts timestamp, v int)")
        tdSql.execute("create table t_c2(ts timestamp, v int)")
        tdSql.execute(f"insert into t_c1 values ({ts(0)}, 1) ({ts(1000)}, NULL) ({ts(3000)}, 3)")
        tdSql.execute(f"insert into t_c2 values ({ts(2000)}, 10) ({ts(4000)}, NULL) ({ts(5000)}, 20)")
        tdSql.execute("create stable vs(ts timestamp, v int) tags(site int) virtual 1")
        tdSql.execute("create vtable vc1(v from t_c1.v) using vs tags(1)")
        tdSql.execute("create vtable vc2(v from t_c2.v) using vs tags(2)")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {DB}")

    @staticmethod
    def _expect(sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def test_next_basic(self):
        """Trailing/leading NULLs, mixed methods and variable-length values.

        NEXT keeps the timestamp and takes the first non-NULL value at or after
        it; the final NULL run stays NULL.  An empty string is a value, not a
        NULL gap.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, next, matrix, ci
        """
        v = [1, None, None, 3, None, 5, None]
        w = [10, None, 20, None, 40, None, None]
        s = ["aa", None, "", None, "bb", None, None]
        offsets = [0, 1000, 2000, 3000, 4000, 5000, 6000]

        expected = [[ts(off)] + [next_values(v)[i], next_values(w)[i], next_values(s)[i]]
                    for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v, w, s from table(time_align(table vt_seq, '*:NEXT:')) order by ts",
            expected)

        expected = [[ts(off), next_values(v)[i], next_values(w)[i], next_values(s)[i]]
                    for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v_n, w_n, s_n from table(time_align(table vt_seq, "
            "'v:NEXT:_n;w:NEXT:_n;s:NEXT:_n')) order by ts",
            expected)

        prev_v = [1, 1, 1, 3, 3, 5, 5]
        expected = [[ts(off), prev_v[i], next_values(v)[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v_p, v_n from table(time_align(table vt_seq, "
            "'v:PREV:_p;v:NEXT:_n')) order by ts",
            expected)

        raw = [[ts(off), None if v[i] is None else v[i], next_values(v)[i]]
               for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v, v_n from table(time_align(table vt_seq, TRUE, 'v:NEXT:_n')) order by ts",
            raw)

        self._expect(
            "select ts, v from table(time_align(table vt_lead, 'v:NEXT:')) order by ts",
            [[ts(0), 7], [ts(1000), 7], [ts(2000), 7]])

    def test_next_filter_limit(self):
        """Outer filters and limits never truncate the NEXT input.

        The successor may sit outside the outer WHERE/LIMIT range; the scan must
        still use it.  Percentile exercises the two-pass path, whose phase flag
        must follow the held block.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, next, filter, ci
        """
        v = [1, None, None, 3, None, 5, None]
        nv = next_values(v)

        self._expect(
            "select ts, v_n from table(time_align(table vt_seq, 'v:NEXT:_n')) "
            "where ts <= 1767225601000 order by ts",
            [[ts(0), 1], [ts(1000), 3]])
        self._expect(
            "select ts, v_n from table(time_align(table vt_seq, 'v:NEXT:_n')) "
            "where v_n = 3 order by ts",
            [[ts(1000), 3], [ts(2000), 3], [ts(3000), 3]])
        self._expect(
            "select ts, v_n from table(time_align(table vt_seq, 'v:NEXT:_n')) "
            "order by ts limit 2",
            [[ts(0), 1], [ts(1000), 3]])
        self._expect(
            "select ts, v_n from table(time_align(table vt_seq, 'v:NEXT:_n')) "
            "order by ts limit 2 offset 4",
            [[ts(4000), 5], [ts(5000), 5]])
        self._expect(
            "select ts, v_n from table(time_align(table vt_seq, 'v:NEXT:_n')) "
            "order by ts desc limit 3",
            [[ts(6000), None], [ts(5000), 5], [ts(4000), 5]])

        tdSql.query("select percentile(v_n, 50) from table(time_align(table vt_seq, 'v:NEXT:_n'))")
        tdSql.checkData(0, 0, 3)
        tdSql.query("select top(v_n, 2) from table(time_align(table vt_seq, 'v:NEXT:_n'))")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)

        tdSql.query("select _wstart, count(*), sum(v_n) from "
                    "table(time_align(table vt_seq, 'v:NEXT:_n')) interval(2s) order by _wstart")
        expected = [[ts(0), 2, 4], [ts(2000), 2, 6], [ts(4000), 2, 10], [ts(6000), 1, None]]
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def test_next_cross_block(self):
        """A successor beyond the scan block boundary resolves earlier rows.

        The source has 4200 rows: values every 100 rows and a gap that crosses
        the 4096-row scan block.  NEXT must hold the tail of the first block
        until the value at row 4100 arrives.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, next, block, ci
        """
        expected = next_values(self.big_values)
        nonnull = [value for value in expected if value is not None]
        tdSql.query("select count(*), count(v_n), sum(v_n), min(v_n), max(v_n) "
                    "from table(time_align(table vt_big, 'v:NEXT:_n'))")
        tdSql.checkData(0, 0, BLOCK_ROWS)
        tdSql.checkData(0, 1, len(nonnull))
        tdSql.checkData(0, 2, sum(nonnull))
        tdSql.checkData(0, 3, min(nonnull))
        tdSql.checkData(0, 4, max(nonnull))

        for row in (4095, 4096, 4099, 4100, 4101, 4199):
            tdSql.query("select v_n from table(time_align(table vt_big, 'v:NEXT:_n')) "
                        f"where ts = {ts(row * 1000)}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected[row])

    def test_next_vstable(self):
        """NEXT state resets per virtual child and ignores other children.

        Child vc1 has a gap at 1s whose successor is its own 3s value; the
        interleaved vc2 values must not leak into it.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, next, vstable, ci
        """
        expected = [
            [1, ts(0), 1], [1, ts(1000), 3], [1, ts(3000), 3],
            [2, ts(2000), 10], [2, ts(4000), 20], [2, ts(5000), 20],
        ]
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT:_n')) "
            "order by site, ts", expected)
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT:_n')) "
            "order by ts",
            [[1, ts(0), 1], [1, ts(1000), 3], [2, ts(2000), 10],
             [1, ts(3000), 3], [2, ts(4000), 20], [2, ts(5000), 20]])
        # The optional range is measured per virtual child: vc1's gap is 2s,
        # vc2's is 1s, and neither sees the other child's successor.
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT(2s):_n')) "
            "order by ts",
            [[1, ts(0), 1], [1, ts(1000), 3], [2, ts(2000), 10],
             [1, ts(3000), 3], [2, ts(4000), 20], [2, ts(5000), 20]])
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT(1s):_n')) "
            "order by ts",
            [[1, ts(0), 1], [1, ts(1000), None], [2, ts(2000), 10],
             [1, ts(3000), 3], [2, ts(4000), 20], [2, ts(5000), 20]])
        # Descending output only reorders the same values; LIMIT stays above
        # the align stage.
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT(2s):_n')) "
            "order by ts desc",
            [[2, ts(5000), 20], [2, ts(4000), 20], [1, ts(3000), 3],
             [2, ts(2000), 10], [1, ts(1000), 3], [1, ts(0), 1]])
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, 'v:NEXT(2s):_n')) "
            "order by ts desc limit 3",
            [[2, ts(5000), 20], [2, ts(4000), 20], [1, ts(3000), 3]])

    def test_next_negative(self):
        """NEXT obeys the same name/column restrictions as PREV.

        Tags and the primary timestamp cannot be interpolated; unknown methods
        are still rejected.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, next, negative, ci
        """
        tdSql.error("select * from table(time_align(table vs, 'site:NEXT:_a'))", 0x80002602)
        tdSql.error("select * from table(time_align(table vt_seq, 'ts:NEXT:_a'))", 0x80002602)
        tdSql.error("select * from table(time_align(table vt_seq, 'v:LINEAR:_a'))", 0x8000264F)
