# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# or later ("AGPL") as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public
# License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""Aggregation GROUP BY a tag-ref tag on a virtual super table.

Regression tests for defect 7139368814 (SIGSEGV in the client planner):
when the virtual super table carries a tag ref (TAGS ... FROM src.tag),
GROUP BY that ref tag with a pushable time WHERE (so no condition stays on
VirtualScan) used to let vstableAggOptimizeWithPartition cast the
STagRefSourceLogicNode child (VirtualScan children[0]) to SScanLogicNode
and read sourceTableName bytes as pScanPseudoCols -> SIGSEGV.

The fix falls back to the unoptimized DynQueryCtrl VTB_SCAN path; these
tests pin both crash-freedom and correct group/aggregate values, plus the
four non-crashing counter-example shapes.
"""

from new_test_framework.utils import tdLog, tdSql

DB = "test_vtag_ref_agg_gb"

# source children: table -> (loc tag, data values)
SRC = {
    'src_x':  ('x', [1, 2, 3]),
    'src_x2': ('x', [4, 6]),
    'src_y':  ('y', [10, 20, 30, 40]),
}

# expected GROUP BY loc groups over all NOW-inserted rows
EXP_GROUPS = [
    ('x', 3.2, 5),    # avg(1,2,3,4,6), count 5
    ('y', 25.0, 4),   # avg(10,20,30,40), count 4
]


class TestVtableTagRefAggGroupBy:
    """GROUP BY tag-ref tag aggregation on a virtual super table."""

    def setup_class(cls):
        tdLog.info(f"=== setup: {DB} ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB} FORCE;")
        tdSql.execute(f"CREATE DATABASE {DB};")
        tdSql.execute(f"USE {DB};")

        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, v INT) TAGS (loc VARCHAR(100));")
        for tbl, (loc, _) in SRC.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING src_stb TAGS ('{loc}');")
        for tbl, (_, vals) in SRC.items():
            sql = ", ".join(f"(NOW+{i}a, {v})" for i, v in enumerate(vals))
            tdSql.execute(f"INSERT INTO {tbl} VALUES {sql};")

        # virtual super table whose only tag is a tag ref
        tdSql.execute("CREATE STABLE vst (ts TIMESTAMP, v INT) "
                      "TAGS (`addr` VARCHAR(100)) VIRTUAL 1;")
        for tbl in SRC:
            tdSql.execute(f"CREATE VTABLE vt_{tbl} (v FROM {DB}.{tbl}.v) "
                          f"USING vst (`addr`) TAGS (FROM {DB}.{tbl}.loc);")

        # second virtual super table mixing a local literal tag and a tag ref,
        # for the GROUP BY local-tag counter-example
        tdSql.execute("CREATE STABLE vst_mixed (ts TIMESTAMP, v INT) "
                      "TAGS (element VARCHAR(100), `addr` VARCHAR(100)) VIRTUAL 1;")
        for tbl in SRC:
            tdSql.execute(f"CREATE VTABLE vm_{tbl} (v FROM {DB}.{tbl}.v) "
                          f"USING vst_mixed TAGS ('room', FROM {DB}.{tbl}.loc);")

        tdLog.info("=== setup complete ===")

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB} FORCE;")

    def _check_groups(self, sql, expected, desc):
        """Assert grouped agg rows (order-independent)."""
        tdSql.query(sql)
        nrows = tdSql.queryRows
        actual = sorted(
            (str(tdSql.getData(i, 0)), str(tdSql.getData(i, 1)), str(tdSql.getData(i, 2)))
            for i in range(nrows))
        exp = sorted((str(g), str(a), str(c)) for g, a, c in expected)
        assert actual == exp, f"{desc}: expected {exp}, got {actual}"
        tdLog.info(f"  PASS: {desc} ({nrows} groups)")

    # ------------------------------------------------------------------
    # Crash shape from defect 7139368814 (pre-fix: client SIGSEGV 139)
    # ------------------------------------------------------------------

    def test_group_by_reftag_with_pushed_time_window(self):
        """GROUP BY tag-ref tag with a pushable time WHERE must not crash

        Defect 7139368814: the time window is pushed down so VirtualScan has
        no residual condition, which used to let VStableAgg run and crash on
        the TagRefSource child. Must return correct per-group aggregates.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        self._check_groups(
            f"SELECT `addr`, AVG(v), COUNT(v) FROM {DB}.vst "
            f"WHERE _c0 > now()-60s AND _c0 < now() GROUP BY `addr`;",
            EXP_GROUPS, "reftag group by, pushed time window")

    def test_group_by_reftag_without_where(self):
        """GROUP BY tag-ref tag with no WHERE must not crash

        Same plan shape as the defect (VirtualScan has no conditions at
        all), previously the same type-confused VStableAgg path.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        self._check_groups(
            f"SELECT `addr`, AVG(v), COUNT(v) FROM {DB}.vst GROUP BY `addr`;",
            EXP_GROUPS, "reftag group by, no WHERE")

    def test_group_by_reftag_limit(self):
        """Defect-report SQL shape: GROUP BY tag-ref tag with LIMIT

        Mirrors the reporter's statement (AVG alias + LIMIT 10 + a one
        second window). Rows are inserted at NOW so the window matches.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query(f"SELECT AVG(v) AS metric FROM {DB}.vst "
                    f"WHERE _c0 > now()-60s AND _c0 < now() "
                    f"GROUP BY `addr` LIMIT 10;")
        assert tdSql.queryRows == 2, f"expected 2 groups, got {tdSql.queryRows}"
        tdLog.info("  PASS: reftag group by with LIMIT")

    # ------------------------------------------------------------------
    # Counter-examples that never crashed; must stay correct
    # ------------------------------------------------------------------

    def test_group_by_local_tag(self):
        """GROUP BY a local literal tag (not a tag ref) must not crash

        Counter-example 1 of the defect: with no ref tag used by the query,
        VirtualScan has no TagRefSource child and VStableAgg stays valid.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        all_vals = [v for _, vals in SRC.values() for v in vals]
        self._check_groups(
            f"SELECT element, AVG(v), COUNT(v) FROM {DB}.vst_mixed "
            f"WHERE _c0 > now()-60s AND _c0 < now() GROUP BY element;",
            [('room', sum(all_vals) / len(all_vals), len(all_vals))],
            "local tag group by")

    def test_group_by_data_column(self):
        """GROUP BY a data column skips VStableAgg (keysHasCol) and works

        Counter-example 2 of the defect.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        all_vals = [v for _, vals in SRC.values() for v in vals]
        tdSql.query(f"SELECT v, AVG(v), COUNT(v) FROM {DB}.vst "
                    f"WHERE _c0 > now()-60s AND _c0 < now() GROUP BY v;")
        rows = {}
        for i in range(tdSql.queryRows):
            rows[int(tdSql.getData(i, 0))] = (float(tdSql.getData(i, 1)), int(tdSql.getData(i, 2)))
        expected = {v: (float(v), 1) for v in all_vals}
        assert rows == expected, f"data col group by: expected {expected}, got {rows}"
        tdLog.info("  PASS: data column group by")

    def test_group_by_reftag_with_reftag_filter(self):
        """GROUP BY tag-ref tag plus a ref-tag WHERE keeps the guard active

        Counter-example 3: the ref-tag condition stays on VirtualScan, so
        the pre-existing hasTagRef guard already skipped VStableAgg.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate, group_by

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        self._check_groups(
            f"SELECT `addr`, AVG(v), COUNT(v) FROM {DB}.vst "
            f"WHERE `addr` = 'y' AND _c0 > now()-60s AND _c0 < now() "
            f"GROUP BY `addr`;",
            [('y', 25.0, 4)], "reftag group by with reftag filter")

    def test_single_vtable_agg(self):
        """Aggregation on a single vtable (pushed to source) must not crash

        Counter-example 4 of the defect.

        Catalog:
            - VirtualTable

        Since: v3.4.3.4

        Labels: virtual, query, tag_ref, aggregate

        History:
            - 2026-10-19 Created for defect 7139368814
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query(f"SELECT AVG(v), COUNT(v) FROM {DB}.vt_src_y "
                    f"WHERE _c0 > now()-60s AND _c0 < now();")
        assert tdSql.queryRows == 1
        assert str(tdSql.getData(0, 0)) == '25.0', tdSql.getData(0, 0)
        assert str(tdSql.getData(0, 1)) == '4', tdSql.getData(0, 1)
        tdLog.info("  PASS: single vtable agg")
