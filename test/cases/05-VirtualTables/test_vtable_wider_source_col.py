# Copyright (c) 2016 TAOS Data, Inc. (jhtao@taosdata.com)
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, version 3 or later ("AGPL"), as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""Virtual table columns whose source column is wider than the declared width.

A virtual table column returns the FULL source value even when the source column
is wider than the virtual table's declared width (DDL gates only the type).
Every downstream fixed-size buffer (function interbuf, tuple-store row image,
group-key page cell, sort key buffer, fill/interp cell, scalar scratch buffer)
must therefore either be sized from the widened plan column or refuse the value
cleanly -- never overrun the buffer.

This suite walks the fixed-width consumers with wider-than-declared values:
  1. projection / WHERE / ORDER BY / LIMIT            (passthrough width)
  2. aggregates: MIN/MAX/FIRST/LAST/MODE             (interbuf + str scratch)
  3. GROUP BY the wide column                        (group page cells, key buf)
  4. ORDER BY ... PARTITION BY the wide column       (sort key buffer)
  5. scalar functions: LTRIM/SUBSTR/CONCAT/CAST      (output scratch buffers)
  6. value_changes                                    (prev-value interbuf)
  7. interval query with FILL                         (fill cell buffers)
  8. TOP/BOTTOM/TAIL/UNIQUE over the wide column     (reservoir/tail slots)

Local real-table sources are width-widened during planning, so all of the above
must return correct full-length results (no corruption, no truncation, no
crash).
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

DB = "td_vtb_wide"
TS0 = 1700000000000  # base timestamp for source rows

SRC_WIDE_BYTES = 480          # payload length actually stored in the source
VT_DECLARED_BYTES = 50        # narrow width declared on the virtual table


def _pad(tag, ch, total):
    """A payload of exactly `total` ASCII chars, prefixed with `tag`."""
    return tag + ch * (total - len(tag) - 1) + "e"


# Two groups of values so GROUP BY / PARTITION BY have several distinct keys.
WIDE_A = _pad("groupA", "a", SRC_WIDE_BYTES)
WIDE_B = _pad("groupB", "b", SRC_WIDE_BYTES)
WIDE_C = _pad("groupC", "c", SRC_WIDE_BYTES)

ROWS = [
    # (offset_ms, value, group-for-value_changes)
    (0,    WIDE_A, 0),
    (1000, WIDE_A, 0),   # unchanged vs previous row
    (2000, WIDE_B, 1),   # changed
    (3000, WIDE_B, 1),   # unchanged
    (4000, WIDE_A, 2),   # changed
    (5000, WIDE_C, 3),   # changed
]


class TestVtableWiderSourceCol:
    """Virtual table column wider than declared -- fixed-width consumers."""

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    @pytest.fixture(scope="class", autouse=True)
    def setup_data(self):
        tdLog.info("=== setup: source with wide column + narrow virtual table ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16;")
        tdSql.execute(f"USE {DB};")

        tdSql.execute("CREATE STABLE wsrc (ts TIMESTAMP, wide VARCHAR(500), v INT) "
                      "TAGS (city NCHAR(20));")
        tdSql.execute("CREATE TABLE wsrc0 USING wsrc TAGS ('beijing');")
        for ms, val, _g in ROWS:
            tdSql.execute(f"INSERT INTO wsrc0 VALUES ({TS0 + ms}, '{val}', {ms // 1000});")

        # The virtual table declares the column NARROWER than the source column.
        tdSql.execute(f"CREATE VTABLE v_narrow (ts TIMESTAMP, "
                      f"narrow VARCHAR({VT_DECLARED_BYTES}) FROM wsrc0.wide, v INT FROM wsrc0.v) "
                      f"TAGS (city NCHAR(20) FROM wsrc0.city);")
        # Tag-less twin for the FILL window path: a tag-ref column plus a lower-bounded ts
        # range trips an unrelated reader path (known issue, "Table schema is old"), which
        # would mask the fill-cell width behavior under test here.
        tdSql.execute(f"CREATE VTABLE v_fill (ts TIMESTAMP, "
                      f"narrow VARCHAR({VT_DECLARED_BYTES}) FROM wsrc0.wide);")
        tdLog.info("=== setup complete ===")
        yield
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _rows(self, sql):
        tdSql.query(sql)
        return [tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    def _alive(self, desc):
        """The server must still answer after the stress query."""
        tdSql.query("SELECT SERVER_STATUS();")
        assert int(tdSql.getData(0, 0)) == 1, f"{desc}: server unhealthy after query"
        tdLog.info(f"  PASS (server alive): {desc}")

    # ==================================================================
    # 1. passthrough width
    # ==================================================================

    def test_wide_projection(self):
        """Wide: SELECT returns the full-length source value, not the declared width.

        1. SELECT the narrow column through the virtual table projection
        2. All six source rows come back
        3. Every value keeps the full 480-byte source length, not the declared VARCHAR(50)

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT narrow FROM {DB}.v_narrow;")
        assert len(rows) == len(ROWS)
        got = sorted(r[0] for r in rows)
        exp = sorted(v for _ms, v, _g in ROWS)
        assert got == exp, "projection lost or truncated the wide values"
        tdLog.info(f"  PASS: full {SRC_WIDE_BYTES}-byte values returned "
                   f"through a VARCHAR({VT_DECLARED_BYTES}) declaration")

    def test_wide_where_and_order(self):
        """Wide: WHERE equality and ORDER BY on the wide column.

        1. WHERE narrow = <full 480-byte value> returns exactly the two matching rows
        2. ORDER BY narrow ASC returns the wide values in sorted order
        3. Server stays healthy after sorting the wide column

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT narrow FROM {DB}.v_narrow WHERE narrow = '{WIDE_B}';")
        assert [r[0] for r in rows] == [WIDE_B, WIDE_B], "WHERE on wide value mismatch"

        rows = self._rows(f"SELECT narrow FROM {DB}.v_narrow ORDER BY narrow ASC;")
        vals = [r[0] for r in rows]
        assert vals == sorted(vals), "ORDER BY on wide column misordered"
        self._alive("ORDER BY wide column")

    def test_wide_limit(self):
        """Wide: LIMIT/OFFSET slicing keeps full values.

        1. SELECT with LIMIT 2 OFFSET 1 returns exactly two rows
        2. Each returned value keeps the full 480-byte source length

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT narrow FROM {DB}.v_narrow LIMIT 2 OFFSET 1;")
        assert len(rows) == 2
        for r in rows:
            assert len(r[0]) == SRC_WIDE_BYTES, "LIMIT path truncated the wide value"

    # ==================================================================
    # 2. aggregates with fixed-width interbufs
    # ==================================================================

    def test_wide_min_max(self):
        """Wide: MIN/MAX keep the full-length extreme value.

        1. MIN(narrow) returns the full-length smallest value
        2. MAX(narrow) returns the full-length largest value
        3. Server stays healthy after the aggregates

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT MIN(narrow), MAX(narrow) FROM {DB}.v_narrow;")
        assert rows[0][0] == WIDE_A, f"MIN mismatch: {rows[0][0][:32]}..."
        assert rows[0][1] == WIDE_C, f"MAX mismatch: {rows[0][1][:32]}..."
        self._alive("MIN/MAX on wide column")

    def test_wide_first_last(self):
        """Wide: FIRST/LAST keep the full-length boundary values.

        1. FIRST(narrow) returns the full-length value of the first row
        2. LAST(narrow) returns the full-length value of the last row
        3. Server stays healthy after the aggregates

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT FIRST(narrow), LAST(narrow) FROM {DB}.v_narrow;")
        assert rows[0][0] == ROWS[0][1], "FIRST mismatch"
        assert rows[0][1] == ROWS[-1][1], "LAST mismatch"
        self._alive("FIRST/LAST on wide column")

    def test_wide_last_row(self):
        """Wide: LAST_ROW keeps the full-length last value.

        1. LAST_ROW(narrow) returns the full-length value of the last row
        2. Server stays healthy after the aggregate

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT LAST_ROW(narrow) FROM {DB}.v_narrow;")
        assert rows[0][0] == ROWS[-1][1], "LAST_ROW mismatch"
        self._alive("LAST_ROW on wide column")

    def test_wide_mode(self):
        """Wide: MODE keeps the full-length most-frequent value.

        1. MODE(narrow) returns a full-length most-frequent value
        2. A and B tie at two rows each, so either one is accepted
        3. Server stays healthy after the aggregate

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT MODE(narrow) FROM {DB}.v_narrow;")
        # A and B each appear twice, C once; MODE may pick A or B.
        assert rows[0][0] in (WIDE_A, WIDE_B), f"MODE mismatch: {rows[0][0][:32]}..."
        self._alive("MODE on wide column")

    # ==================================================================
    # 3. GROUP BY (group page cells + key buffer + passthrough interbuf)
    # ==================================================================

    def test_wide_group_by(self):
        """Wide: GROUP BY the wide column buckets and echoes full values.

        1. GROUP BY the wide column buckets rows by their full-length values
        2. Per-group COUNT(*) matches the expected 3/2/1 distribution
        3. Server stays healthy after grouping

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, group

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT narrow, COUNT(*) c FROM {DB}.v_narrow GROUP BY narrow;")
        got = {r[0]: int(r[1]) for r in rows}
        exp = {WIDE_A: 3, WIDE_B: 2, WIDE_C: 1}
        assert got == exp, f"GROUP BY mismatch: {got}"
        self._alive("GROUP BY wide column")

    # ==================================================================
    # 4. ORDER BY ... PARTITION BY (sort key buffer)
    # ==================================================================

    def test_wide_partition_by(self):
        """Wide: ORDER BY ... PARTITION BY assigns group ids over wide keys.

        1. ROW_NUMBER() OVER (PARTITION BY narrow ORDER BY ts) numbers rows within each wide-key group
        2. The per-group sequences are 1..3, 1..2 and 1 for groups A, B and C
        3. Server stays healthy after the windowed sort

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, sort

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT narrow, ROW_NUMBER() OVER (PARTITION BY narrow ORDER BY ts) rn "
                          f"FROM {DB}.v_narrow;")
        got = {}
        for r in rows:
            got.setdefault(r[0], []).append(int(r[1]))
        exp = {WIDE_A: [1, 2, 3], WIDE_B: [1, 2], WIDE_C: [1]}
        assert got == exp, f"PARTITION BY mismatch: {got}"
        self._alive("PARTITION BY wide column")

    # ==================================================================
    # 5. scalar functions over the wide column
    # ==================================================================

    def test_wide_scalar_trim(self):
        """Wide: LTRIM/RTRIM handle the full-length value.

        1. The stored value has no leading spaces, so LTRIM strips nothing
        2. LENGTH(LTRIM(narrow)) is still the full 480 bytes
        3. Server stays healthy after the scalar function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT LENGTH(LTRIM(narrow)) FROM {DB}.v_narrow WHERE narrow = '{WIDE_A}';")
        assert int(rows[0][0]) == SRC_WIDE_BYTES, f"LTRIM length mismatch: {rows[0][0]}"
        self._alive("LTRIM on wide column")

    def test_wide_scalar_substr(self):
        """Wide: SUBSTR reads past the declared width.

        1. SUBSTR(narrow, 1, 100) reads 100 bytes, past the declared VARCHAR(50) width
        2. The result matches the first 100 bytes of the full source value
        3. Server stays healthy after the scalar function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT SUBSTR(narrow, 1, 100) FROM {DB}.v_narrow WHERE narrow = '{WIDE_A}';")
        assert rows[0][0] == WIDE_A[:100], "SUBSTR mismatch beyond declared width"
        self._alive("SUBSTR on wide column")

    def test_wide_scalar_concat(self):
        """Wide: CONCAT builds results wider than the declared width.

        1. CONCAT of two 80-byte SUBSTR pieces builds a 160-byte result, wider than the declared VARCHAR(50)
        2. The result matches the first 160 bytes of the full source value
        3. Server stays healthy after the scalar function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT CONCAT(SUBSTR(narrow, 1, 80), SUBSTR(narrow, 81, 80)) "
                          f"FROM {DB}.v_narrow WHERE narrow = '{WIDE_A}';")
        assert rows[0][0] == WIDE_A[:160], "CONCAT mismatch"
        self._alive("CONCAT on wide column")

    def test_wide_scalar_cast(self):
        """Wide: CAST of the wide value into a narrow VARCHAR truncates cleanly.

        1. CAST(narrow AS VARCHAR(30)) truncates the wide value to 30 bytes cleanly
        2. The result matches the first 30 bytes of the full source value
        3. Server stays healthy after the scalar function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT CAST(narrow AS VARCHAR(30)) FROM {DB}.v_narrow WHERE narrow = '{WIDE_B}';")
        assert rows[0][0] == WIDE_B[:30], "CAST truncation mismatch"
        self._alive("CAST on wide column")

    def test_wide_scalar_md5(self):
        """Wide: MD5 over the full-length value.

        1. MD5 over the full-length wide value produces a 32-character digest
        2. Server stays healthy after the scalar function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT LENGTH(MD5(narrow)) FROM {DB}.v_narrow WHERE narrow = '{WIDE_C}';")
        assert int(rows[0][0]) == 32, f"MD5 length mismatch: {rows[0][0]}"
        self._alive("MD5 on wide column")

    # ==================================================================
    # 6. value_changes (prev-value interbuf)
    # ==================================================================

    def test_wide_value_changes(self):
        """Wide: value_changes counts changes on full-length values.

        1. VALUE_CHANGES(narrow) compares consecutive full-length values
        2. The transitions A->B, B->A and A->C give exactly 3 changes
        3. Server stays healthy after the function

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, function

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT VALUE_CHANGES(narrow) FROM {DB}.v_narrow;")
        # transitions: A->A(0) A->B(1) B->B(0) B->A(2) A->C(3)
        assert int(rows[0][0]) == 3, f"VALUE_CHANGES mismatch: {rows[0][0]}"
        self._alive("VALUE_CHANGES on wide column")

    # ==================================================================
    # 7. interval + FILL (fill cell buffers)
    # ==================================================================

    def test_wide_interval_fill(self):
        """Wide: interval aggregation with FILL NULL survives wide values.

        1. INTERVAL(2s) with FILL(NULL) over the tag-less virtual table returns at least three windows
        2. Every non-NULL LAST(narrow) value keeps the full 480-byte source length
        3. Server stays healthy after the fill query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, fill

        Jira: None

        History:
            - 2026-10-09 Created
        """
        t_lo, t_hi = TS0 - 1000, TS0 + 10000
        rows = self._rows(f"SELECT _wstart, LAST(narrow) FROM {DB}.v_fill "
                          f"WHERE ts >= {t_lo} AND ts < {t_hi} INTERVAL(2s) FILL(NULL);")
        assert len(rows) >= 3, f"expected filled windows, got {len(rows)}"
        vals = [r[1] for r in rows if r[1] not in ("None", "NULL")]
        for v in vals:
            assert len(v) == SRC_WIDE_BYTES, f"fill path value corrupted: len={len(v)}"
        self._alive("INTERVAL FILL on wide column")

    # ==================================================================
    # 8. reservoir/tail-style functions
    # ==================================================================

    def test_wide_top_bottom(self):
        """Wide: TOP/BOTTOM on a varchar column is rejected cleanly at translate time.

        1. SELECT BOTTOM(narrow, 2) raises a clean error (top/bottom are numeric-only)
        2. SELECT TOP(narrow, 2) raises a clean error
        3. Server stays healthy after the rejected queries

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate, negative

        Jira: None

        History:
            - 2026-10-09 Created
        """
        tdSql.error(f"SELECT BOTTOM(narrow, 2) FROM {DB}.v_narrow;")
        tdSql.error(f"SELECT TOP(narrow, 2) FROM {DB}.v_narrow;")
        self._alive("TOP/BOTTOM on wide varchar column rejected cleanly")

    def test_wide_tail(self):
        """Wide: TAIL keeps full-length trailing values.

        1. TAIL(narrow, 3) returns exactly three rows
        2. The values are the full-length trailing three source values, in any order
        3. Server stays healthy after the aggregate

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT TAIL(narrow, 3) FROM {DB}.v_narrow;")
        assert len(rows) == 3
        # TAIL's output order follows the plan, not necessarily ts order; the memory-safety
        # property under test is that all three values are full-length and belong to the
        # trailing set.
        exp = sorted([ROWS[-3][1], ROWS[-2][1], ROWS[-1][1]])
        got = sorted(r[0] for r in rows)
        assert got == exp, "TAIL mismatch"
        self._alive("TAIL on wide column")

    def test_wide_concat_agg(self):
        """Wide: aggregate CONCAT over a wide column stays intact.

        1. CONCAT(narrow, narrow) over the v=0 row doubles the wide value to 960 bytes
        2. The result matches the source value concatenated with itself
        3. Server stays healthy after the query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-09 Created
        """
        rows = self._rows(f"SELECT CONCAT(narrow, narrow) FROM {DB}.v_narrow WHERE v = 0;")
        assert rows[0][0] == WIDE_A + WIDE_A, "aggregate CONCAT mismatch"
        self._alive("aggregate CONCAT on wide column")

    def test_wide_concat_agg_overflow_rejected(self):
        """Wide: GROUP_CONCAT past the 64KB block fails cleanly, not by overrun.

        1. Create a VARCHAR(65517) source table and a narrow virtual table over it
        2. Insert two 40000-byte values so GROUP_CONCAT overshoots the fixed 64KB result block
        3. GROUP_CONCAT either errors out or returns NULL -- a clean refusal, never a crash or corruption
        4. Server stays healthy after the oversized aggregate

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate, negative

        Jira: None

        History:
            - 2026-10-09 Created
        """
        # GROUP_CONCAT accumulates values across rows into one fixed TSDB_MAX_FIELD_LEN
        # result block; two 40000-byte values overshoot it.  The guard must surface as a
        # clean outcome: either a query error or a NULL result (the value was refused, not
        # silently truncated or corrupted); a crash would fail the whole class instead.
        half = "x" * 40000
        tdSql.execute(f"CREATE TABLE {DB}.wbig (ts TIMESTAMP, wide VARCHAR(65517));")
        tdSql.execute(f"CREATE VTABLE {DB}.v_big2 (ts TIMESTAMP, "
                      f"narrow VARCHAR({VT_DECLARED_BYTES}) FROM {DB}.wbig.wide);")
        try:
            tdSql.execute(f"INSERT INTO {DB}.wbig VALUES ({TS0 + 1000000}, '{half}'), "
                          f"({TS0 + 1001000}, '{half}');")
            got = None
            failed = False
            try:
                rows = self._rows(f"SELECT GROUP_CONCAT(narrow) FROM {DB}.v_big2;")
                got = rows[0][0] if rows else None
            except Exception as e:
                failed = True
                tdLog.info(f"  PASS: oversized GROUP_CONCAT rejected: {e}")
            if not failed:
                assert got in ("None", "NULL", None), \
                    f"oversized GROUP_CONCAT returned {len(str(got))} bytes, not a clean refusal"
                tdLog.info("  PASS: oversized GROUP_CONCAT returned NULL (value refused)")
        finally:
            tdSql.execute(f"DROP TABLE IF EXISTS {DB}.v_big2;")
            tdSql.execute(f"DROP TABLE IF EXISTS {DB}.wbig;")
        self._alive("oversized GROUP_CONCAT")

    # ==================================================================
    # 9. super-table shaped virtual scan
    # ==================================================================

    def test_wide_vstable_scan(self):
        """Wide: virtual super table projection keeps full-length values.

        1. Create a tag-carrying virtual super table over the wide source column
        2. SELECT narrow returns all six source rows
        3. Every value keeps the full 480-byte source length
        4. Server stays healthy after the virtual super table scan

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-09 Created
        """
        tdSql.execute(f"CREATE VTABLE {DB}.vs_narrow (ts TIMESTAMP, "
                      f"narrow VARCHAR({VT_DECLARED_BYTES}) FROM wsrc0.wide) "
                      f"TAGS (city NCHAR(20) FROM wsrc0.city);")
        try:
            rows = self._rows(f"SELECT narrow FROM {DB}.vs_narrow;")
            assert len(rows) == len(ROWS)
            got = sorted(r[0] for r in rows)
            assert got == sorted(v for _ms, v, _g in ROWS)
        finally:
            tdSql.execute(f"DROP TABLE {DB}.vs_narrow;")
        self._alive("virtual super table wide projection")
