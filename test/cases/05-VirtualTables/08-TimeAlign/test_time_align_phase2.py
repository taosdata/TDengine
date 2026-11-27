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
"""System tests for TIME_ALIGN phase 2: non-virtual tables and views.

Ordinary tables, child tables and super tables are aligned inside TableScan;
views are aligned by a Projection above the view's top operator.  Outer WHERE
and LIMIT always run after interpolation, while a view's own filter runs
before it.

A view does not distinguish column types: every output column, including
TIMESTAMP and tag columns, can be aligned.  PREV/NEXT follow the view's own
output row order, the time range parameter is ignored, and the aligned schema
only carries the requested columns.  A view declared with PARTITION BY keeps
one independent sequence per partition, so NEXT never crosses a partition
boundary even when one merged view block carries several partitions.
"""

import taos

from new_test_framework.utils import TDSql, tdLog, tdSql

DB = "ta_align_p2"
BASE_TS = 1767225600000

PAR_SYNTAX_ERROR = 0x80002600
PAR_INVALID_COLUMN = 0x80002602
PAR_DUPLICATED_COLUMN = 0x8000263C
PAR_INVALID_COLUMNS_NUM = 0x8000263F
PAR_PERMISSION_DENIED = 0x80002644
PAR_NOT_ALLOWED_FUNC = 0x8000264F


def ts(offset):
    return BASE_TS + offset


def ts_ms(value):
    if isinstance(value, (int, float)):
        return int(value)
    return int(value.timestamp() * 1000)


def prev_values(values):
    res = []
    last = None
    for value in values:
        if value is not None:
            last = value
        res.append(last)
    return res


def next_values(values):
    res = [None] * len(values)
    nxt = None
    for i in range(len(values) - 1, -1, -1):
        if values[i] is not None:
            nxt = values[i]
        res[i] = nxt
    return res


V = [1, None, None, 3, None]
W = [10, None, 20, None, 40]
S = ["aa", None, "", None, "bb"]


class TestTimeAlignPhase2:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        tdSql.execute("create table t(ts timestamp, v int, w int, s varchar(16))")
        tdSql.execute(
            f"insert into t values "
            f"({ts(0)}, 1, 10, 'aa') "
            f"({ts(1000)}, NULL, NULL, NULL) "
            f"({ts(2000)}, NULL, 20, '') "
            f"({ts(3000)}, 3, NULL, NULL) "
            f"({ts(4000)}, NULL, 40, 'bb')")

        tdSql.execute("create table st(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table c1 using st tags(1)")
        tdSql.execute("create table c2 using st tags(2)")
        tdSql.execute(f"insert into c1 values ({ts(0)}, 1) ({ts(1000)}, NULL) ({ts(3000)}, 3)")
        tdSql.execute(f"insert into c2 values ({ts(0)}, 10) ({ts(2000)}, 20) ({ts(4000)}, NULL)")

        tdSql.execute("create view vw as select ts, v, w, s from t")  # data columns include ts
        tdSql.execute(f"create view vfw as select ts, v from t where ts <= {ts(2000)}")
        tdSql.execute("create view vwin as select _wstart as ts, count(*) as cnt, sum(v) as sv from t interval(2s)")
        tdSql.execute("create view vu as select ts, v from t union all select ts, w as v from t")
        # A view has no primary timestamp; an explicit ORDER BY makes the
        # output order (and therefore PREV/NEXT) deterministic.
        tdSql.execute("create view vs as select site, ts, v from st order by site, ts")
        tdSql.execute("create view vj as select a.ts as ts, a.v as va, b.w as wb "
                      "from t a join t b on a.ts = b.ts")
        tdSql.execute("create view vnots as select v, w, s from t")
        # Output-order views: DISTINCT and DESC have no timestamp order.
        tdSql.execute("create view vdist as select distinct ts, v from t order by v")
        tdSql.execute("create view vdasc as select ts, v from t order by ts desc")

        # Extra view shapes: every supported view top operator gets an align
        # query in test_views.
        tdSql.execute("create view vasc as select ts, v from t order by ts asc limit 3")
        tdSql.execute("create view vexpr as select ts, v * 2 as v2, w + 1 as w1 from t")
        tdSql.execute("create view vnest as select ts, v2 from vexpr")
        tdSql.execute("create view vfill as select _wstart as ts, avg(w) as aw from t interval(1s) fill(prev)")
        tdSql.execute("create view vfilln as select _wstart as ts, avg(w) as aw from t interval(1s)")
        tdSql.execute("create view vslide as select _wstart as ts, count(*) as cnt, sum(v) as sv "
                      "from t interval(2s) sliding(1s)")
        tdSql.execute("create view vcw as select _wstart as ts, count(*) as cnt, sum(v) as sv "
                      "from t count_window(2)")
        tdSql.execute("create view vstate as select _wstart as ts, _wend as te, count(*) as cnt, "
                      "first(v) as fv from t state_window(v)")
        tdSql.execute("create view vses as select _wstart as ts, count(*) as cnt, sum(v) as sv "
                      "from t session(ts, 500a)")
        tdSql.execute("create view vtime as select _wstart as ts, _wend as te, _wduration as dur, "
                      "count(*) as cnt from t interval(2s)")
        tdSql.execute("create view vpart as select site, ts, v from st partition by site")
        tdSql.execute("create view vleft as select a.ts as ts, a.v as av, b.w as bw "
                      "from t a left join t b on a.ts = b.ts")
        # FS 4.8.3.5: nesting needs a view between the two table functions.
        tdSql.execute("create view va as select * from table(time_align(table t, 'v:PREV:_p'))")

        # PARTITION BY + INTERVAL: the first partition ends with a NULL window
        # before the second partition has a value.  Per-partition state must
        # keep NEXT from crossing the boundary (regression for the merged
        # Merge-Aligned-Interval block that carries several partitions).
        tdSql.execute("create table stleak(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table l1 using stleak tags(1)")
        tdSql.execute("create table l2 using stleak tags(2)")
        tdSql.execute(f"insert into l1 values ({ts(0)}, 1) ({ts(2000)}, 3) ({ts(4000)}, NULL)")
        tdSql.execute(f"insert into l2 values ({ts(0)}, 10)")
        tdSql.execute("create view vpartgap as select site, _wstart as ts, sum(v) as sv from stleak "
                      "partition by site interval(2s) order by site, ts")

        # Window-type matrix: one NULL-producing window belongs to site 1 while
        # site 2 has a value.  With partition grouping preserved, the NULL must
        # never be filled from the other site.
        tdSql.execute("create table stwin(ts timestamp, v int, s int) tags(site int)")
        tdSql.execute("create table w1 using stwin tags(1)")
        tdSql.execute("create table w2 using stwin tags(2)")
        tdSql.execute(f"insert into w1 values ({ts(0)}, 1, 1) ({ts(2000)}, NULL, 2) ({ts(4000)}, NULL, 3)")
        tdSql.execute(f"insert into w2 values ({ts(0)}, 10, 1)")
        tdSql.execute("create view vwin_int as select site, _wstart as ts, sum(v) as sv "
                      "from stwin partition by site interval(2s)")
        tdSql.execute("create view vwin_slide as select site, _wstart as ts, sum(v) as sv "
                      "from stwin partition by site interval(2s) sliding(1s)")
        tdSql.execute("create view vwin_ses as select site, _wstart as ts, sum(v) as sv "
                      "from stwin partition by site session(ts, 500a)")
        tdSql.execute("create view vwin_state as select site, _wstart as ts, sum(v) as sv "
                      "from stwin partition by site state_window(s)")
        tdSql.execute("create view vwin_cnt as select site, _wstart as ts, sum(v) as sv "
                      "from stwin partition by site count_window(2)")

        # GROUP BY view: every row is a group; site 1's group value is NULL and
        # must not take site 2's value.
        tdSql.execute("create table stgrp(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table g1 using stgrp tags(1)")
        tdSql.execute("create table g2 using stgrp tags(2)")
        tdSql.execute(f"insert into g1 values ({ts(0)}, NULL) ({ts(2000)}, NULL)")
        tdSql.execute(f"insert into g2 values ({ts(0)}, 10)")
        tdSql.execute("create view vgrp as select site, max(v) as sv from stgrp group by site")

        # External window: an external window table supplies the boundaries;
        # fill(null) emits every window of each partition.
        tdSql.execute("create table extwin(ts timestamp, endtime timestamp, mark int)")
        tdSql.execute(f"insert into extwin values ({ts(0)}, {ts(600000)}, 1) "
                      f"({ts(600000)}, {ts(1200000)}, 2) "
                      f"({ts(1200000)}, {ts(1800000)}, 3) "
                      f"({ts(1800000)}, {ts(2400000)}, 4)")
        tdSql.execute("create table stext(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table x1 using stext tags(1)")
        tdSql.execute("create table x2 using stext tags(2)")
        tdSql.execute(f"insert into x1 values ({ts(0)}, 1)")
        tdSql.execute(f"insert into x2 values ({ts(0)}, 10)")
        tdSql.execute("create view vwin_ext as select site, cast(_wstart as bigint) as ts, sum(v) as sv "
                      "from stext partition by site "
                      "external_window((select ts, endtime, mark from extwin) w) fill(null)")

        tdSql.execute("create table tside(ts timestamp, label int)")
        tdSql.execute(f"insert into tside values ({ts(0)}, 100) ({ts(2000)}, 200) ({ts(5000)}, 500)")

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

    def test_ordinary_table(self):
        """PREV/NEXT, mixed methods and variable-length values on a real table.

        The table function replaces the unaligned source columns; timestamps
        are unchanged and an empty string is a value, not a NULL gap.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, table, ci
        """
        offsets = [0, 1000, 2000, 3000, 4000]
        expected = [[ts(off), prev_values(V)[i], next_values(V)[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v_p, v_n from table(time_align(table t, 'v:PREV:_p;v:NEXT:_n')) order by ts",
            expected)

        expected = [[ts(off), prev_values(W)[i], next_values(W)[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, w_p, w_n from table(time_align(table t, 'w:PREV:_p;w:NEXT:_n')) order by ts",
            expected)

        expected = [[ts(off), prev_values(S)[i], next_values(S)[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, s_p, s_n from table(time_align(table t, 's:PREV:_p;s:NEXT:_n')) order by ts",
            expected)

        expected = [[ts(off), V[i], next_values(V)[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v, v_n from table(time_align(table t, TRUE, 'v:NEXT:_n')) order by ts",
            expected)

    def test_ordinary_table_query_ops(self):
        """Filters, limits, ordering and aggregates run after interpolation.

        WHERE ts and LIMIT must not truncate the NEXT input; ORDER BY DESC is
        honoured by a Sort above the aligned scan; percentile exercises the
        two-pass scan.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, filter, ci
        """
        nv = next_values(V)
        self._expect(
            "select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) where v_n = 3 order by ts",
            [[ts(1000), 3], [ts(2000), 3], [ts(3000), 3]])
        self._expect(
            "select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) "
            f"where ts <= {ts(1000)} order by ts",
            [[ts(0), 1], [ts(1000), 3]])
        self._expect(
            "select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) order by ts limit 2 offset 2",
            [[ts(2000), nv[2]], [ts(3000), nv[3]]])
        self._expect(
            "select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) order by ts desc limit 3",
            [[ts(4000), None], [ts(3000), 3], [ts(2000), 3]])

        tdSql.query("select count(*), count(v_n), sum(v_n) from "
                    "table(time_align(table t, 'v:NEXT:_n'))")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 10)
        tdSql.query("select percentile(v_n, 50) from table(time_align(table t, 'v:NEXT:_n'))")
        tdSql.checkData(0, 0, 3)

        tdSql.query("select _wstart, count(*), sum(v_n) from "
                    "table(time_align(table t, 'v:NEXT:_n')) interval(2s) order by _wstart")
        expected = [[ts(0), 2, 4], [ts(2000), 2, 6], [ts(4000), 1, None]]
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        # SESSION(500a) splits every 1s gap into its own window.
        self._expect(
            "select _wstart, count(*), sum(v_n) from "
            "table(time_align(table t, 'v:NEXT:_n')) session(ts, 500a) order by _wstart",
            [[ts(off), 1, nv[i]] for i, off in enumerate((0, 1000, 2000, 3000, 4000))])

        # COUNT_WINDOW(2): fixed two-row windows.
        self._expect(
            "select _wstart, count(*), sum(v_n) from "
            "table(time_align(table t, 'v:NEXT:_n')) count_window(2) order by _wstart",
            [[ts(0), 2, 4], [ts(2000), 2, 6], [ts(4000), 1, None]])

        # STATE_WINDOW over the aligned column: 1 then 3,3,3.
        self._expect(
            "select _wstart, _wend, count(*), first(v_n) from "
            "table(time_align(table t, 'v:NEXT:_n')) state_window(v_n) order by _wstart",
            [[ts(0), ts(0), 1, 1], [ts(1000), ts(3000), 3, 3]])

        # INTERVAL(2s) SLIDING(1s): overlapping windows; the first window
        # starts one second before the first row.
        self._expect(
            "select _wstart, count(*), sum(v_n) from "
            "table(time_align(table t, 'v:NEXT:_n')) interval(2s) sliding(1s) order by _wstart",
            [[ts(-1000), 1, 1], [ts(0), 2, 4], [ts(1000), 2, 6],
             [ts(2000), 2, 6], [ts(3000), 2, 3], [ts(4000), 1, None]])

        # FILL(PREV) / FILL(NEXT) apply to the window results, not to the
        # aligned input.
        self._expect(
            "select _wstart, avg(w_p) from "
            "table(time_align(table t, 'w:PREV:_p')) interval(1s) fill(prev) order by _wstart",
            [[ts(off), value] for off, value in zip((0, 1000, 2000, 3000, 4000), (10, 10, 20, 20, 40))])
        self._expect(
            "select _wstart, avg(w_n) from "
            "table(time_align(table t, 'w:NEXT:_n')) interval(1s) fill(next) order by _wstart",
            [[ts(off), value] for off, value in zip((0, 1000, 2000, 3000, 4000), (10, 20, 20, 40, 40))])

        # GROUP BY aligned column + HAVING.
        self._expect(
            "select v_n, count(*) from table(time_align(table t, 'v:NEXT:_n')) "
            "group by v_n having count(*) >= 2 order by v_n",
            [[3, 3]])

        # DISTINCT over the aligned column.
        self._expect(
            "select distinct v_p from table(time_align(table t, 'v:PREV:_p')) order by v_p",
            [[1], [3]])

        # FREE expressions over two differently aligned columns.
        pv = prev_values(V)
        pw = prev_values(W)
        self._expect(
            "select ts, v_n * w_p as prod from "
            "table(time_align(table t, 'v:NEXT:_n;w:PREV:_p')) order by ts",
            [[ts(off), None if nv[i] is None or pw[i] is None else nv[i] * pw[i]]
             for i, off in enumerate((0, 1000, 2000, 3000, 4000))])

        # ORDER BY an aligned column puts NULL first.
        self._expect(
            "select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) order by v_n, ts",
            [[ts(4000), None], [ts(0), 1], [ts(1000), 3], [ts(2000), 3], [ts(3000), 3]])

        # Derived table around the aligned scan.
        self._expect(
            "select * from (select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) "
            "where v_n = 3) q order by ts",
            [[ts(1000), 3], [ts(2000), 3], [ts(3000), 3]])

        # UNION ALL of two different alignments: compare the value multiset
        # per timestamp (branch order is not part of the contract).
        tdSql.query("select ts, v_n from table(time_align(table t, 'v:NEXT:_n')) union all "
                    "select ts, w_p from table(time_align(table t, 'w:PREV:_p')) order by ts")
        tdSql.checkRows(10)
        per_ts = {}
        for row in tdSql.queryResult:
            per_ts.setdefault(ts_ms(row[0]), []).append(row[1])
        for i, off in enumerate((0, 1000, 2000, 3000, 4000)):
            assert sorted(per_ts.get(ts(off), []), key=lambda x: (x is None, x)) == \
                sorted([nv[i], pw[i]], key=lambda x: (x is None, x))

        # JOIN with a plain table, and two aligned table functions joined.
        self._expect(
            "select x.ts, x.v_n, s.label from table(time_align(table t, 'v:NEXT:_n')) x "
            "join tside s on x.ts = s.ts order by x.ts",
            [[ts(0), 1, 100], [ts(2000), 3, 200]])
        self._expect(
            "select x.ts, x.v_n, s.label from table(time_align(table t, 'v:NEXT:_n')) x "
            "left join tside s on x.ts = s.ts order by x.ts",
            [[ts(0), 1, 100], [ts(1000), 3, None], [ts(2000), 3, 200],
             [ts(3000), 3, None], [ts(4000), None, None]])
        self._expect(
            "select a.ts, a.v_n, b.w_p from table(time_align(table t, 'v:NEXT:_n')) a "
            "join table(time_align(table t, 'w:PREV:_p')) b on a.ts = b.ts order by a.ts",
            [[ts(off), nv[i], pw[i]] for i, off in enumerate((0, 1000, 2000, 3000, 4000))])

        # Raw columns are filtered after interpolation without truncating it.
        self._expect(
            "select ts, v, v_n from table(time_align(table t, TRUE, 'v:NEXT:_n')) "
            "where v is not null order by ts",
            [[ts(0), 1, 1], [ts(3000), 3, 3]])

    def test_real_table_raw_columns(self):
        """Raw output keeps table semantics: tags only for super tables.

        FS 4.5.4: output_raw=TRUE on a child table emits its data columns but
        not its tags, while a super table also emits the tag columns at the
        end.  Both shapes keep ts + raw columns + aligned columns in order.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, schema, ci
        """
        # Child table: ts, v (raw), v_n.  No tag column.
        tdSql.query("select * from table(time_align(table c1, TRUE, 'v:NEXT:_n')) order by ts")
        assert tdSql.queryCols == 3, f"child raw columns: expected 3, got {tdSql.queryCols}"
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, ts(0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        # Super table: ts, v (raw), v_n, site (tag).
        tdSql.query("select * from table(time_align(table st, TRUE, 'v:NEXT:_n')) order by site, ts")
        assert tdSql.queryCols == 4, f"stable raw columns: expected 4, got {tdSql.queryCols}"
        tdSql.checkRows(6)
        expected = [[ts(0), 1, 1, 1], [ts(1000), None, 3, 1], [ts(3000), 3, 3, 1],
                    [ts(0), 10, 10, 2], [ts(2000), 20, 20, 2], [ts(4000), None, None, 2]]
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        # output_raw=FALSE on the same sources exposes only ts + aligned cols.
        tdSql.query("select * from table(time_align(table c1, 'v:NEXT:_n')) order by ts")
        assert tdSql.queryCols == 2, f"child aligned columns: expected 2, got {tdSql.queryCols}"
        tdSql.query("select * from table(time_align(table st, 'v:NEXT:_n')) order by ts")
        assert tdSql.queryCols == 2, f"stable aligned columns: expected 2, got {tdSql.queryCols}"

    def test_real_table_types(self):
        """Every user-creatable column type goes through PREV/NEXT unchanged.

        The real-table align stage runs the same fill functions as the virtual
        table one; BOOL, all integer widths, FLOAT, DOUBLE, BINARY, NCHAR,
        VARBINARY, GEOMETRY and both DECIMAL widths must keep their values and
        type metadata in the aligned-only and raw+aligned output.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, datatype, ci
        """
        type_columns = [
            ("c_bool", "bool"),
            ("c_ts", "timestamp"),
            ("c_tint", "tinyint"),
            ("c_utint", "tinyint unsigned"),
            ("c_sint", "smallint"),
            ("c_usint", "smallint unsigned"),
            ("c_int", "int"),
            ("c_uint", "int unsigned"),
            ("c_bint", "bigint"),
            ("c_ubint", "bigint unsigned"),
            ("c_float", "float"),
            ("c_double", "double"),
            ("c_bin", "binary(8)"),
            ("c_nchar", "nchar(8)"),
            ("c_vbin", "varbinary(8)"),
            ("c_geo", "geometry(64)"),
            ("c_dec", "decimal(10,2)"),
            ("c_dec38", "decimal(38,4)"),
        ]
        null_cells = ", ".join("NULL" for _ in type_columns)
        tdSql.execute("drop table if exists t_all_types")
        column_ddl = ", ".join(f"{name} {typ}" for name, typ in type_columns)
        tdSql.execute(f"create table t_all_types (ts timestamp, {column_ddl})")
        tdSql.execute(
            f"insert into t_all_types values "
            f"({ts(0)}, true, {ts(0)}, 1, 11, 101, 1001, 10001, 100001, 1000001, 10000001, "
            f"1.5, 10.5, 'abc', 'nchar', 'vbin', \"POINT(1.0 1.0)\", "
            f"123.45, 1234567890123456789012345678.1234) "
            f"({ts(1000)}, {null_cells}) "
            f"({ts(2000)}, false, {ts(2000)}, -1, 12, -101, 1002, -10001, 100002, "
            f"-1000001, 10000002, 2.5, 20.5, 'def', 'nchar2', 'vbin2', \"POINT(2.0 2.0)\", "
            f"-123.45, -1234567890123456789012345678.1234) "
            f"({ts(3000)}, {null_cells})")

        try:
            # The raw query is the reference for every type.
            tdSql.query("select * from t_all_types order by ts")
            raw_rows = [list(row) for row in tdSql.queryResult]
            tdSql.checkRows(4)

            last = [None] * len(raw_rows[0])
            expected = []
            for row in raw_rows:
                current = [row[0]]
                for index in range(1, len(row)):
                    if row[index] is not None:
                        last[index] = row[index]
                    current.append(last[index])
                expected.append(current)

            # Aligned-only output: ts + one aligned column per type, keeping
            # the type (DECIMAL scale included).
            tdSql.query("select * from table(time_align(table t_all_types)) order by ts")
            tdSql.checkRows(4)
            assert tdSql.queryCols == 1 + len(type_columns), (
                f"expected {1 + len(type_columns)} columns, got {tdSql.queryCols}")
            aligned_rows = [list(row) for row in tdSql.queryResult]
            for row_index in range(4):
                for col_index in range(len(type_columns) + 1):
                    assert aligned_rows[row_index][col_index] == expected[row_index][col_index], (
                        f"row {row_index} col {col_index}: "
                        f"{aligned_rows[row_index][col_index]!r} != {expected[row_index][col_index]!r}")

            # output_raw=TRUE: raw columns first, aligned columns after, with
            # the same values and type metadata in both halves.
            tdSql.query("select * from table(time_align(table t_all_types, TRUE, '*::_a')) order by ts")
            tdSql.checkRows(4)
            assert tdSql.queryCols == 1 + len(type_columns) * 2, (
                f"expected {1 + len(type_columns) * 2} columns, got {tdSql.queryCols}")
            raw_aligned_rows = [list(row) for row in tdSql.queryResult]
            for row_index in range(4):
                for col_index, _ in enumerate(type_columns):
                    raw_value = raw_aligned_rows[row_index][1 + col_index]
                    aligned_value = raw_aligned_rows[row_index][1 + len(type_columns) + col_index]
                    assert raw_value == raw_rows[row_index][1 + col_index], (
                        f"raw row {row_index} col {col_index}: {raw_value!r}")
                    assert aligned_value == expected[row_index][1 + col_index], (
                        f"aligned row {row_index} col {col_index}: {aligned_value!r}")

            # WHERE on GEOMETRY/DECIMAL aligned columns uses the aligned values.
            tdSql.query("select c_bool_p, c_geo_p, c_dec38_p from "
                        "table(time_align(table t_all_types, '*:PREV:_p')) "
                        "where c_geo_p is not null order by ts")
            tdSql.checkRows(4)
            for row_index in range(4):
                tdSql.checkData(row_index, 0, expected[row_index][1])
                tdSql.checkData(row_index, 1, expected[row_index][16])
                tdSql.checkData(row_index, 2, expected[row_index][18])
        finally:
            tdSql.execute("drop table if exists t_all_types")

    def test_syntax_variants(self):
        """Syntax variants on ordinary, child, super tables and views.

        Default arguments, the explicit default list, upper-case keywords,
        spaces around separators, quoted identifiers, repeated '*' and table
        aliases all produce the documented output schema on every phase-2
        input type.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, syntax, ci
        """
        offsets = (0, 1000, 2000, 3000, 4000)
        pv, pw, ps = prev_values(V), prev_values(W), prev_values(S)

        # Ordinary table: default arguments = FALSE + '*:PREV:'.  The first
        # table argument can be written without the inner TABLE keyword.
        self._expect("select * from table(time_align(t)) order by ts",
                     [[ts(off), pv[i], pw[i], ps[i]] for i, off in enumerate(offsets)])
        self._expect(f"select * from table(time_align({DB}.t)) order by ts",
                     [[ts(off), pv[i], pw[i], ps[i]] for i, off in enumerate(offsets)])
        self._expect("select * from table(time_align(table t)) order by ts",
                     [[ts(off), pv[i], pw[i], ps[i]] for i, off in enumerate(offsets)])
        self._expect("select ts, v, w from table(time_align(table t, FALSE, '*:PREV:')) order by ts",
                     [[ts(off), pv[i], pw[i]] for i, off in enumerate(offsets)])
        # Case-insensitive keywords, function and method.
        self._expect("SELECT TS, V_N FROM TABLE(TIME_ALIGN(TABLE t, FALSE, '*:NEXT:_N')) ORDER BY TS",
                     [[ts(off), next_values(V)[i]] for i, off in enumerate(offsets)])
        self._expect("SeLeCt ts, v_mc FrOm TaBlE(tImE_aLiGn(TaBlE t, 'v:PrEv:_mc')) OrDeR bY ts",
                     [[ts(off), pv[i]] for i, off in enumerate(offsets)])
        # Whitespace around the separators is trimmed.
        self._expect("select ts, v_sp from table(time_align(table t, ' v : PREV : _sp ')) order by ts",
                     [[ts(off), pv[i]] for i, off in enumerate(offsets)])
        # Quoted identifier source.
        self._expect("select ts, `v_q` from table(time_align(table t, '`v` : PREV : _q')) order by ts",
                     [[ts(off), pv[i]] for i, off in enumerate(offsets)])
        # Repeated '*' plus an explicit definition, addressed through an alias.
        self._expect("select x.ts, x.v_a, x.v_b from "
                     "table(time_align(table t, '*:PREV:_a;v:PREV:_b')) x order by x.ts",
                     [[ts(off), pv[i], pv[i]] for i, off in enumerate(offsets)])
        # Single-column forms: 'col', 'col:', 'col::', 'col::suffix' and
        # 'col:METHOD:suffix'; the first three all produce the source name.
        for definition in ("v", "v:", "v::"):
            self._expect(f"select ts, v from table(time_align(table t, '{definition}')) order by ts",
                         [[ts(off), pv[i]] for i, off in enumerate(offsets)])
        self._expect("select ts, v, v_c1, v_c2 from "
                     "table(time_align(table t, 'v;v::_c1;v:PREV:_c2')) order by ts",
                     [[ts(off), pv[i], pv[i], pv[i]] for i, off in enumerate(offsets)])

        # Child table: default arguments use the child's single data column.
        self._expect("select * from table(time_align(table c1)) order by ts",
                     [[ts(0), 1], [ts(1000), 1], [ts(3000), 3]])

        # Super table: '*' expands data columns only; tags stay selectable.
        self._expect("select site, ts, v from table(time_align(table st)) order by site, ts",
                     [[1, ts(0), 1], [1, ts(1000), 1], [1, ts(3000), 3],
                      [2, ts(0), 10], [2, ts(2000), 20], [2, ts(4000), 20]])

        # View: default arguments expand every data column (including the
        # view's timestamp column) and PREV them in view order.
        self._expect("select * from table(time_align(table vw)) order by ts",
                     [[ts(off), pv[i], pw[i], ps[i]] for i, off in enumerate(offsets)])
        # A view's timestamp column is an ordinary output column and is only
        # visible through output_raw or when named by a definition.
        self._expect("select ts, `v_q` from "
                     "table(time_align(table vw, TRUE, '`v`::_q')) order by ts",
                     [[ts(off), pv[i]] for i, off in enumerate(offsets)])
        self._expect("select v.ts, v.v_n from "
                     "table(time_align(table vw, TRUE, 'v:NEXT:_n')) v order by v.ts",
                     [[ts(off), next_values(V)[i]] for i, off in enumerate(offsets)])

    def test_super_table(self):
        """Super tables keep per-child state and expose tags for grouping.

        Interleaved child rows must not leak a successor across devices; global
        ordering and interval windows are restored above the aligned scan.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, stable, ci
        """
        self._expect(
            "select site, ts, v_n from table(time_align(table st, 'v:NEXT:_n')) order by site, ts",
            [[1, ts(0), 1], [1, ts(1000), 3], [1, ts(3000), 3],
             [2, ts(0), 10], [2, ts(2000), 20], [2, ts(4000), None]])
        self._expect(
            "select site, ts, v_n from table(time_align(table st, 'v:NEXT:_n')) order by ts, site",
            [[1, ts(0), 1], [2, ts(0), 10], [1, ts(1000), 3],
             [2, ts(2000), 20], [1, ts(3000), 3], [2, ts(4000), None]])

        tdSql.query("select _wstart, count(*), sum(v_n) from "
                    "table(time_align(table st, 'v:NEXT:_n')) interval(2s) order by _wstart")
        expected = [[ts(0), 3, 14], [ts(2000), 2, 23], [ts(4000), 1, None]]
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

        self._expect(
            "select site, count(*), sum(v_n) from table(time_align(table st, 'v:NEXT:_n')) "
            "partition by site order by site",
            [[1, 3, 7], [2, 3, 30]])

        self._expect(
            "select site, ts, v, v_n from table(time_align(table st, TRUE, 'v:NEXT:_n')) order by site, ts",
            [[1, ts(0), 1, 1], [1, ts(1000), None, 3], [1, ts(3000), 3, 3],
             [2, ts(0), 10, 10], [2, ts(2000), 20, 20], [2, ts(4000), None, None]])

        self._expect(
            "select ts, v_n from table(time_align(table c1, 'v:NEXT:_n')) order by ts",
            [[ts(0), 1], [ts(1000), 3], [ts(3000), 3]])

    def test_super_table_multivgroup_scan(self):
        """Multi-vgroup super-table align survives arbitrary scan interleaving.

        Eight children in four vgroups are scanned as independent block
        streams: blocks of different children arrive in arbitrary order and
        every child spans several scan blocks.  Align state is keyed by the
        block uid, so PREV/NEXT never leaks across children.  The table merge
        scan applies the same stage to each per-table block before the ts
        merge.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, stable, scan, ci
        """
        db = "ta_scan_vg"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"use {db}")
        try:
            children = 8
            rows = 5001
            tdSql.execute("create table st_scan(ts timestamp, v int) tags(site int)")
            for i in range(children):
                tdSql.execute(f"create table sc{i} using st_scan tags({i})")
            # Only the first and the second-to-last row hold a value: the NEXT
            # gap spans thousands of rows (i.e. several scan blocks) and the
            # trailing NULL row has no successor.
            for i in range(children):
                cells = []
                for k in range(rows):
                    if k == 0:
                        value = i + 1
                    elif k == rows - 2:
                        value = i + 101
                    else:
                        value = "NULL"
                    cells.append(f"({ts(k * 1000)}, {value})")
                    if len(cells) == 500:
                        tdSql.execute(f"insert into sc{i} values " + " ".join(cells))
                        cells = []
                if cells:
                    tdSql.execute(f"insert into sc{i} values " + " ".join(cells))

            tdSql.query(f"select distinct vgroup_id from information_schema.ins_tables "
                        f"where db_name = '{db}' and table_name like 'sc%'")
            assert len(tdSql.queryResult) >= 2, tdSql.queryResult

            tdSql.query("select site, count(*), first(v_p), last(v_p), first(v_n), last(v_n) "
                        "from table(time_align(table st_scan, 'v:PREV:_p;v:NEXT:_n')) "
                        "partition by site order by site")
            tdSql.checkRows(children)
            for i in range(children):
                tdSql.checkData(i, 0, i)
                tdSql.checkData(i, 1, rows)
                tdSql.checkData(i, 2, i + 1)
                tdSql.checkData(i, 3, i + 101)
                tdSql.checkData(i, 4, i + 1)
                tdSql.checkData(i, 5, i + 101)

            probes = [0, 1, rows - 2, rows - 1]
            tdSql.query("select site, ts, v_p, v_n from "
                        "table(time_align(table st_scan, 'v:PREV:_p;v:NEXT:_n')) "
                        f"where ts in ({', '.join(str(ts(k * 1000)) for k in probes)}) "
                        "order by site, ts")
            tdSql.checkRows(children * len(probes))
            row = 0
            for i in range(children):
                expected = [
                    (i, ts(0), i + 1, i + 1),
                    (i, ts(1000), i + 1, i + 101),
                    (i, ts((rows - 2) * 1000), i + 101, i + 101),
                    (i, ts((rows - 1) * 1000), i + 101, None),
                ]
                for exp in expected:
                    for col, value in enumerate(exp):
                        tdSql.checkData(row, col, value)
                    row += 1

            # DESC pushdown across vgroups: the merge scan keeps one sequence
            # per child and the filled values match the ascending result.
            tdSql.query("explain select site, ts, v_p from "
                        "table(time_align(table st_scan, 'v:PREV:_p')) order by ts desc")
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Sort" not in plan, plan
            tdSql.query("select site, ts, v_p from "
                        "table(time_align(table st_scan, 'v:PREV:_p')) "
                        f"where ts in ({ts(0)}, {ts((rows - 2) * 1000)}, {ts((rows - 1) * 1000)}) "
                        "order by site, ts desc")
            tdSql.checkRows(children * 3)
            row = 0
            for i in range(children):
                expected = [(i, ts((rows - 1) * 1000), i + 101),
                            (i, ts((rows - 2) * 1000), i + 101),
                            (i, ts(0), i + 1)]
                for exp in expected:
                    for col, value in enumerate(exp):
                        tdSql.checkData(row, col, value)
                    row += 1

        finally:
            tdSql.execute(f"use {DB}")
            tdSql.execute(f"drop database if exists {db}")

    def test_partition_grouping(self):
        """Partitioned output groups several tables but fill stays per table.

        PARTITION BY tag may be pushed into the scan so blocks arrive grouped
        by tag; TIME_ALIGN still fills each child table independently because
        interpolation happens before the grouping.  PARTITION BY tbname makes
        every group one table.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, partition, stable, ci
        """
        tdSql.execute("drop table if exists st_grp")
        tdSql.execute("create table st_grp(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table sg1 using st_grp tags(1)")
        tdSql.execute("create table sg2 using st_grp tags(1)")
        tdSql.execute("create table sg3 using st_grp tags(2)")
        tdSql.execute(f"insert into sg1 values ({ts(0)}, 1) ({ts(1000)}, NULL) ({ts(2000)}, NULL)")
        tdSql.execute(f"insert into sg2 values ({ts(500)}, 10) ({ts(1500)}, 20) ({ts(2500)}, NULL)")
        tdSql.execute(f"insert into sg3 values ({ts(100)}, 100) ({ts(1100)}, NULL)")
        try:
            # tag 1 groups sg1 and sg2.  A fill state keyed by the tag group
            # would let sg1's trailing NULLs take sg2's 20.
            self._expect(
                "select site, ts, v_n from table(time_align(table st_grp, 'v:NEXT:_n')) "
                "partition by site order by ts",
                [[1, ts(0), 1], [2, ts(100), 100], [1, ts(500), 10],
                 [1, ts(1000), None], [2, ts(1100), None], [1, ts(1500), 20],
                 [1, ts(2000), None], [1, ts(2500), None]])
            self._expect(
                "select site, ts, v_p from table(time_align(table st_grp, 'v:PREV:_p')) "
                "partition by site order by ts",
                [[1, ts(0), 1], [2, ts(100), 100], [1, ts(500), 10],
                 [1, ts(1000), 1], [2, ts(1100), 100], [1, ts(1500), 20],
                 [1, ts(2000), 1], [1, ts(2500), 20]])
            self._expect(
                "select ts, v_n from table(time_align(table st_grp, 'v:NEXT:_n')) "
                "partition by tbname order by ts",
                [[ts(0), 1], [ts(100), 100], [ts(500), 10], [ts(1000), None],
                 [ts(1100), None], [ts(1500), 20], [ts(2000), None], [ts(2500), None]])
        finally:
            tdSql.execute("drop table if exists st_grp")

    def test_scan_hosts(self):
        """Every scan shape that can host align runs the stage before the cache.

        A plain table scan hosts the stage directly; a table merge scan hosts
        it on each per-table block before the ts merge; last/last_row cache
        scans and tag-only scans are not applicable (they change or bypass the
        aligned row values); system tables cannot be TIME_ALIGN inputs, which
        also rules out the block-info/table-count scans of SHOW TABLE
        DISTRIBUTED.  Stream queries are out of scope.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, plan, scan, ci
        """

        def plan(sql):
            tdSql.query(f"explain {sql}")
            return "\n".join(str(row[0]) for row in tdSql.queryResult)

        table_plan = plan("select ts, v_n from table(time_align(table t, 'v:NEXT:_n'))")
        assert "Table Scan" in table_plan, table_plan
        assert "Table Merge Scan" not in table_plan, table_plan

        # Tag-only projection still reads data columns; a tag scan would not
        # produce the align outputs.
        tag_plan = plan("select site from table(time_align(table st, 'v:PREV:_p'))")
        assert "Tag Scan" not in tag_plan, tag_plan
        assert "Table Scan" in tag_plan, tag_plan
        self._expect("select site from table(time_align(table st, 'v:PREV:_p')) order by site",
                     [[1], [1], [1], [2], [2], [2]])

        # A global ts-order consumer (count window) promotes the super-table
        # scan to a merge scan; the fill stage still runs per child before the
        # merge.
        merge_plan = plan("select _wstart, count(*) from "
                          "table(time_align(table st, 'v:NEXT:_n')) count_window(1000)")
        assert "Table Merge Scan" in merge_plan, merge_plan
        # The merge scan must produce the same aligned values as the plain scan.
        self._expect("select count(*), sum(v_n), sum(v_p) from "
                     "table(time_align(table st, 'v:PREV:_p;v:NEXT:_n')) count_window(1000)",
                     [[6, 37, 55]])

        # The last/last_row cache changes the last row value under PREV, so the
        # cache scan must be rejected and the aligned value computed by a scan.
        tdSql.execute("drop database if exists ta_align_cache")
        tdSql.execute("create database ta_align_cache cachemodel 'both'")
        tdSql.execute("use ta_align_cache")
        try:
            tdSql.execute("create table ct(ts timestamp, v int)")
            tdSql.execute(f"insert into ct values ({ts(0)}, 5) ({ts(1000)}, NULL) ({ts(2000)}, NULL)")
            last_plan = plan("select last_row(v_p) from table(time_align(table ct, 'v:PREV:_p'))")
            assert "Last Row Scan" not in last_plan, last_plan
            self._expect("select last(v_p), last_row(v_p), last(v_n), last_row(v_n) from "
                         "table(time_align(table ct, 'v:PREV:_p;v:NEXT:_n'))",
                         [[5, 5, 5, None]])
            self._expect("select last(v), last_row(v) from ct", [[5, None]])
        finally:
            tdSql.execute(f"use {DB}")
            tdSql.execute("drop database if exists ta_align_cache")

        # System tables (and their block-info/table-count scans) cannot be a
        # TIME_ALIGN input.
        tdSql.error("select * from table(time_align(table information_schema.ins_tables))",
                    0x8000268E)

    def test_views(self):
        """Views align above their own plan; the view's own clauses run first.

        Projection, filter, LIMIT/ORDER, expression, nested, join, left-join,
        UNION ALL, partition-by and all window shapes (interval, sliding,
        count, state, session, fill) interpolate on the view's output rows.
        Every view output column, including timestamp and tag columns, is an
        interpolation source.  Views over super tables keep tags for
        filtering/grouping; a view over a TIME_ALIGN result can be aligned
        again.  The same view can feed two align stages at once without sharing
        state.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, view, ci
        """
        offsets = [0, 1000, 2000, 3000, 4000]
        nv, nw = next_values(V), next_values(W)
        expected = [[ts(off), nv[i], nw[i]] for i, off in enumerate(offsets)]
        self._expect(
            "select ts, v_n, w_n from table(time_align(table vw, TRUE, 'v:NEXT:_n;w:NEXT:_n')) "
            "order by ts", expected)

        self._expect(
            "select ts, v_p from table(time_align(table vw, TRUE, 'v:PREV:_p')) order by ts",
            [[ts(off), prev_values(V)[i]] for i, off in enumerate(offsets)])

        # The view filter removes ts=3000/4000 before interpolation: the row at
        # ts=2000 has no successor inside the view and stays NULL.  The outer
        # WHERE is applied afterwards and does not change values.
        self._expect(
            "select ts, v_n from table(time_align(table vfw, TRUE, 'v:NEXT:_n')) order by ts",
            [[ts(0), 1], [ts(1000), None], [ts(2000), None]])
        self._expect(
            "select ts, v_n from table(time_align(table vfw, TRUE, 'v:NEXT:_n')) "
            f"where ts >= {ts(1000)} order by ts",
            [[ts(1000), None], [ts(2000), None]])

        self._expect(
            "select ts, sv_p from table(time_align(table vwin, TRUE, 'sv:PREV:_p')) order by ts",
            [[ts(0), 1], [ts(2000), 3], [ts(4000), 3]])
        self._expect(
            "select ts, sv_n from table(time_align(table vwin, TRUE, 'sv:NEXT:_n')) order by ts",
            [[ts(0), 1], [ts(2000), 3], [ts(4000), None]])

        # The ordered view over the super table is one sequence in its own
        # output order (site 1, then site 2); tags stay available for
        # filtering and grouping above the align stage.
        self._expect(
            "select site, ts, v_n from table(time_align(table vs, TRUE, 'v:NEXT:_n')) "
            "order by site, ts",
            [[1, ts(0), 1], [1, ts(1000), 3], [1, ts(3000), 3],
             [2, ts(0), 10], [2, ts(2000), 20], [2, ts(4000), None]])
        tdSql.query("select * from table(time_align(table vs)) order by ts")
        assert tdSql.queryCols == 3, f"view over stable default columns: {tdSql.queryCols}"
        self._expect(
            "select site, sum(v_n) from table(time_align(table vs, 'v:NEXT:_n')) "
            "group by site order by site",
            [[1, 7], [2, 30]])
        self._expect(
            "select ts, v_n from table(time_align(table vs, TRUE, 'v:NEXT:_n')) "
            "where site = 1 order by ts",
            [[ts(0), 1], [ts(1000), 3], [ts(3000), 3]])

        # Partition-by view: each site is its own series.
        self._expect(
            "select site, ts, v_n from table(time_align(table vpart, TRUE, 'v:NEXT:_n')) "
            "order by site, ts",
            [[1, ts(0), 1], [1, ts(1000), 3], [1, ts(3000), 3],
             [2, ts(0), 10], [2, ts(2000), 20], [2, ts(4000), None]])

        # The view's own LIMIT bounds the interpolation input: ts=3000/4000
        # are not in the view and cannot fill the tail.
        self._expect(
            "select ts, v_n from table(time_align(table vasc, TRUE, 'v:NEXT:_n')) order by ts",
            [[ts(0), 1], [ts(1000), None], [ts(2000), None]])
        self._expect(
            "select ts, v_p from table(time_align(table vasc, TRUE, 'v:PREV:_p')) order by ts",
            [[ts(0), 1], [ts(1000), 1], [ts(2000), 1]])

        # Expression view: the aligned source may be a computed column.
        self._expect(
            "select ts, v2_p, w1_p from "
            "table(time_align(table vexpr, TRUE, 'v2:PREV:_p;w1:PREV:_p')) order by ts",
            [[ts(0), 2, 11], [ts(1000), 2, 11], [ts(2000), 2, 21],
             [ts(3000), 6, 21], [ts(4000), 6, 41]])

        # Nested view over an expression view.
        self._expect(
            "select ts, v2_n from table(time_align(table vnest, TRUE, 'v2:NEXT:_n')) order by ts",
            [[ts(0), 2], [ts(1000), 6], [ts(2000), 6], [ts(3000), 6], [ts(4000), None]])

        # Window views: FILL inside the view, gap windows, sliding windows,
        # count windows, state windows and session windows.
        self._expect(
            "select ts, aw_n from table(time_align(table vfill, TRUE, 'aw:NEXT:_n')) order by ts",
            [[ts(0), 10], [ts(1000), 10], [ts(2000), 20], [ts(3000), 20], [ts(4000), 40]])
        self._expect(
            "select ts, aw_n from table(time_align(table vfilln, TRUE, 'aw:NEXT:_n')) order by ts",
            [[ts(0), 10], [ts(1000), 20], [ts(2000), 20], [ts(3000), 40], [ts(4000), 40]])
        self._expect(
            "select ts, aw_p from table(time_align(table vfilln, TRUE, 'aw:PREV:_p')) order by ts",
            [[ts(0), 10], [ts(1000), 10], [ts(2000), 20], [ts(3000), 20], [ts(4000), 40]])
        # SLIDING(1s): the window starting at ts=1000 only contains NULL
        # values, so its sum is NULL and NEXT takes the next window's 3.
        self._expect(
            "select ts, cnt_n, sv_n from "
            "table(time_align(table vslide, TRUE, 'cnt:NEXT:_n;sv:NEXT:_n')) order by ts",
            [[ts(-1000), 1, 1], [ts(0), 2, 1], [ts(1000), 2, 3],
             [ts(2000), 2, 3], [ts(3000), 2, 3], [ts(4000), 1, None]])
        self._expect(
            "select ts, cnt_n, sv_n from "
            "table(time_align(table vcw, TRUE, 'cnt:NEXT:_n;sv:NEXT:_n')) order by ts",
            [[ts(0), 2, 1], [ts(2000), 2, 3], [ts(4000), 1, None]])
        # STATE_WINDOW skips NULL states: rows at ts=0 and ts=3000.
        self._expect(
            "select ts, cnt_n, fv_n from "
            "table(time_align(table vstate, TRUE, 'cnt:NEXT:_n;fv:NEXT:_n')) order by ts",
            [[ts(0), 1, 1], [ts(3000), 1, 3]])
        self._expect(
            "select ts, cnt_n, sv_n from "
            "table(time_align(table vses, TRUE, 'cnt:NEXT:_n;sv:NEXT:_n')) order by ts",
            [[ts(off), 1, nv[i]] for i, off in enumerate(offsets)])
        self._expect(
            "select ts, te_p, dur_p, cnt_p from "
            "table(time_align(table vtime, TRUE, 'te:PREV:_p;dur:PREV:_p;cnt:PREV:_p')) order by ts",
            [[ts(0), ts(2000), 2000, 2], [ts(2000), ts(4000), 2000, 2],
             [ts(4000), ts(6000), 2000, 1]])

        # Left-join view: unmatched rows keep their own timeline.
        self._expect(
            "select ts, av_n, bw_n from "
            "table(time_align(table vleft, TRUE, 'av:NEXT:_n;bw:NEXT:_n')) order by ts",
            [[ts(0), 1, 10], [ts(1000), 3, 20], [ts(2000), 3, 20],
             [ts(3000), 3, 40], [ts(4000), None, 40]])

        # JOIN view: the join is evaluated first and interpolation runs on the
        # joined rows, so both sides see the same successor timeline.
        self._expect(
            "select ts, va_n, wb_n from "
            "table(time_align(table vj, TRUE, 'va:NEXT:_n;wb:NEXT:_n')) order by ts",
            expected)

        # A view can be wrapped around a TIME_ALIGN result and aligned again
        # (direct nesting is rejected, FS 4.8.3.5).
        self._expect(
            "select ts, v_p from table(time_align(table va)) order by ts",
            [[ts(off), prev_values(V)[i]] for i, off in enumerate(offsets)])
        self._expect(
            "select ts, v_p_n from table(time_align(table va, TRUE, 'v_p:NEXT:_n')) order by ts",
            [[ts(off), prev_values(V)[i]] for i, off in enumerate(offsets)])

        # UNION ALL view: the row set is the union of both branches.  Series
        # identity across branches depends on the set-op plan shape, so only
        # the row-set contract is asserted here.
        tdSql.query("select ts, v_n from "
                    "table(time_align(table vu, TRUE, 'v:NEXT:_n')) order by ts")
        tdSql.checkRows(10)
        counts = {}
        for row in tdSql.queryResult:
            key = ts_ms(row[0])
            counts[key] = counts.get(key, 0) + 1
        for offset in (0, 1000, 2000, 3000, 4000):
            assert counts.get(ts(offset)) == 2

        # Aligned view joined with a plain table.
        self._expect(
            "select x.ts, x.v_n, s.label from "
            "table(time_align(table vw, TRUE, 'v:NEXT:_n')) x "
            "join tside s on x.ts = s.ts order by x.ts",
            [[ts(0), 1, 100], [ts(2000), 3, 200]])

        # Multi-consumer view: the same view feeds two align stages.  UNION
        # ALL keeps both branches' row sets, and a self-join of two aligned
        # references pairs them by timestamp without leaking state.
        tdSql.query("select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT:_n')) "
                    "union all select ts, w_n from "
                    "table(time_align(table vw, TRUE, 'w:NEXT:_n')) order by ts")
        tdSql.checkRows(10)
        per_ts = {}
        for row in tdSql.queryResult:
            per_ts.setdefault(ts_ms(row[0]), []).append(row[1])
        for i, off in enumerate(offsets):
            assert sorted(per_ts.get(ts(off), []), key=lambda x: (x is None, x)) == \
                sorted([nv[i], nw[i]], key=lambda x: (x is None, x))

        self._expect(
            "select a.ts, a.v_n, b.w_n from "
            "table(time_align(table vw, TRUE, 'v:NEXT:_n')) a "
            "join table(time_align(table vw, TRUE, 'w:NEXT:_n')) b on a.ts = b.ts "
            "order by a.ts",
            [[ts(off), nv[i], nw[i]] for i, off in enumerate(offsets)])

        # Empty suffix with output_raw=FALSE: the filled column owns the
        # source name, so the raw pass-through must stay invisible.
        self._expect("select v from table(time_align(table vw, 'v:PREV:'))",
                     [[value] for value in prev_values(V)])
        self._expect("select v from table(time_align(table vw, 'v:NEXT:'))",
                     [[value] for value in next_values(V)])

        # A view timestamp column is an ordinary data column: it can be named
        # by a definition and interpolated like any other column.
        self._expect(
            "select ts_p from table(time_align(table vw, 'ts:PREV:_p')) order by ts_p",
            [[ts(off)] for off in offsets])
        self._expect(
            "select ts_n from table(time_align(table vw, 'ts:NEXT:_n')) order by ts_n",
            [[ts(off)] for off in offsets])

        # A window above the align host groups on the raw ts column that
        # output_raw=TRUE exposes; interpolation itself still runs per row.
        tdSql.query("select _wstart, count(*), sum(v_n) from "
                    "table(time_align(table vw, TRUE, 'v:NEXT:_n')) interval(2s) "
                    "order by _wstart")
        tdSql.checkRows(3)
        assert [row[1] for row in tdSql.queryResult] == [2, 2, 1]
        assert [row[2] for row in tdSql.queryResult] == [4, 6, None]

    def test_view_output_order(self):
        """Non-scan inputs interpolate in view output order, not table ts order.

        A DISTINCT view ordered by value resolves PREV/NEXT against its
        produced row order.  A view delivered in timestamp DESC order follows
        that descending row order, so PREV/NEXT mean the previous/next output
        row without direction compensation.  A view without any timestamp
        column aligns normally.

        Grouping follows the view's output shape: a PARTITION BY view keeps
        one independent sequence per partition only when it has no view-level
        ORDER BY/DISTINCT.  Sorting or deduplicating the view output collapses
        the grouping, so the whole output becomes one sequence.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, view, order, ci
        """
        # DISTINCT + ORDER BY v: the three NULL rows come first, then v=1 and
        # v=3.  NEXT must therefore be 1 for every NULL row and for ts=0;
        # ordering by the table timestamp would give 3/3/NULL instead.
        tdSql.query("select ts, v_n from table(time_align(table vdist, TRUE, 'v:NEXT:_n'))")
        tdSql.checkRows(5)
        got = {ts_ms(row[0]): row[1] for row in tdSql.queryResult}
        assert got == {ts(0): 1, ts(1000): 1, ts(2000): 1, ts(3000): 3, ts(4000): 1}, got

        # DESC view: PREV/NEXT follow the view output row order (ts=4000 first),
        # so they refer to the previous and next output rows; no timestamp
        # direction compensation is applied.
        tdSql.query("select ts, v_n, v_p from "
                    "table(time_align(table vdasc, TRUE, 'v:NEXT:_n;v:PREV:_p'))")
        tdSql.checkRows(5)
        expected = [[ts(4000), 3, None], [ts(3000), 3, 3], [ts(2000), 1, 3],
                    [ts(1000), 1, 3], [ts(0), 1, 1]]
        for row_index, row in enumerate(expected):
            assert ts_ms(tdSql.queryResult[row_index][0]) == row[0], \
                f"row {row_index} ts: {tdSql.queryResult[row_index][0]!r} != {row[0]!r}"
            for col_index in range(1, len(row)):
                assert tdSql.queryResult[row_index][col_index] == row[col_index], \
                    f"row {row_index} col {col_index}: {tdSql.queryResult[row_index][col_index]!r} != {row[col_index]!r}"

        # A view-level ORDER BY collapses the partition grouping: the sorted
        # output is one sequence, so site 1's trailing NULL is followed by
        # site 2's value and NEXT resolves to 10.
        self._expect(
            "select site, ts, sv_n from "
            "table(time_align(table vpartgap, TRUE, 'sv:NEXT:_n')) order by site, ts",
            [[1, ts(0), 1], [1, ts(2000), 3], [1, ts(4000), 10], [2, ts(0), 10]])
        self._expect(
            "select site, ts, sv_p from "
            "table(time_align(table vpartgap, TRUE, 'sv:PREV:_p')) order by site, ts",
            [[1, ts(0), 1], [1, ts(2000), 3], [1, ts(4000), 3], [2, ts(0), 10]])

        # Without a view-level ORDER BY the view keeps one block per
        # partition (the block group id is preserved through the view plan),
        # and each partition is aligned independently.
        tdSql.execute("create view vpartgap2 as select site, _wstart as ts, sum(v) as sv "
                      "from stleak partition by site interval(2s)")
        try:
            self._expect(
                "select site, ts, sv_n from "
                "table(time_align(table vpartgap2, TRUE, 'sv:NEXT:_n')) order by site, ts",
                [[1, ts(0), 1], [1, ts(2000), 3], [1, ts(4000), None], [2, ts(0), 10]])
            self._expect(
                "select site, ts, sv_p from "
                "table(time_align(table vpartgap2, TRUE, 'sv:PREV:_p')) order by site, ts",
                [[1, ts(0), 1], [1, ts(2000), 3], [1, ts(4000), 3], [2, ts(0), 10]])
        finally:
            tdSql.execute("drop view if exists vpartgap2")

        # A partition can span several scan blocks.  The state key is derived
        # from the key values, so the NEXT gap still resolves across blocks
        # and stays inside its own partition.
        table = "t_partblk"
        tdSql.execute(f"drop table if exists {table}")
        tdSql.execute(f"create table {table}(ts timestamp, v int) tags(site int)")
        tdSql.execute(f"create table pb1 using {table} tags(1)")
        tdSql.execute(f"create table pb2 using {table} tags(2)")
        rows = 4200
        for site, base in ((1, 7), (2, 70)):
            cells = []
            for i in range(rows):
                value = base if i == 0 else (base + 2 if i == rows - 1 else "NULL")
                cells.append(f"({ts(i * 1000)}, {value})")
                if len(cells) == 700:
                    tdSql.execute(f"insert into pb{site} values " + " ".join(cells))
                    cells = []
            if cells:
                tdSql.execute(f"insert into pb{site} values " + " ".join(cells))
        tdSql.execute(f"create view v_partblk as select site, ts, v from {table} partition by site")
        try:
            tdSql.query("select site, count(*), first(v_n), last(v_n) from "
                        "table(time_align(table v_partblk, TRUE, 'v:NEXT:_n')) "
                        "group by site order by site")
            tdSql.checkRows(2)
            for row_index, base in enumerate((7, 70)):
                tdSql.checkData(row_index, 0, row_index + 1)
                tdSql.checkData(row_index, 1, rows)
                tdSql.checkData(row_index, 2, base)
                tdSql.checkData(row_index, 3, base + 2)
        finally:
            tdSql.execute("drop view if exists v_partblk")
            tdSql.execute(f"drop table if exists {table}")

        # A view without a timestamp column needs no primary timestamp: the
        # default '*' PREVs every data column, and explicit definitions work
        # with or without output_raw.
        self._expect("select * from table(time_align(table vnots))",
                     [[1, 10, 'aa'], [1, 10, 'aa'], [1, 20, ''], [3, 20, ''], [3, 40, 'bb']])
        self._expect("select * from table(time_align(table vnots, '*::_a'))",
                     [[1, 10, 'aa'], [1, 10, 'aa'], [1, 20, ''], [3, 20, ''], [3, 40, 'bb']])
        self._expect("select v_n from table(time_align(table vnots, 'v:NEXT:_n'))",
                     [[1], [3], [3], [3], [None]])
        self._expect(
            "select * from table(time_align(table vnots, TRUE, 'v:NEXT:_n'))",
            [[1, 10, 'aa', 1], [None, None, None, 3], [None, 20, '', 3],
             [3, None, None, 3], [None, 40, 'bb', None]])

    def test_window_and_group_grouping(self):
        """Every window type and GROUP BY keeps its partition grouping.

        A PARTITION BY view without ORDER BY keeps one block per group; a
        GROUP BY view does the same.  A NULL value in one group must never be
        filled from another group's non-NULL value.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, view, window, grouping, ci
        """

        def check(view, expected):
            self._expect(
                f"select site, ts, sv_n from table(time_align(table {view}, TRUE, 'sv:NEXT:_n')) "
                "order by site, ts", expected)

        # INTERVAL: site 1 produces two trailing NULL windows.
        check("vwin_int",
              [[1, ts(0), 1], [1, ts(2000), None], [1, ts(4000), None], [2, ts(0), 10]])
        # SLIDING(1s): all windows after the first two are NULL for site 1.
        check("vwin_slide",
              [[1, ts(-1000), 1], [1, ts(0), 1], [1, ts(1000), None],
               [1, ts(2000), None], [1, ts(3000), None], [1, ts(4000), None],
               [2, ts(-1000), 10], [2, ts(0), 10]])
        # SESSION: the second session of site 1 is NULL.
        check("vwin_ses",
              [[1, ts(0), 1], [1, ts(2000), None], [1, ts(4000), None], [2, ts(0), 10]])
        # STATE_WINDOW: the later states have a NULL sum.
        check("vwin_state",
              [[1, ts(0), 1], [1, ts(2000), None], [1, ts(4000), None], [2, ts(0), 10]])
        # COUNT_WINDOW(2): the second window of site 1 is NULL.
        check("vwin_cnt",
              [[1, ts(0), 1], [1, ts(4000), None], [2, ts(0), 10]])
        # EXTERNAL_WINDOW: site 1's later windows are NULL and must stay
        # NULL instead of taking site 2's 10.
        check("vwin_ext",
              [[1, ts(0), 1], [1, ts(600000), None], [1, ts(1200000), None],
               [1, ts(1800000), None],
               [2, ts(0), 10], [2, ts(600000), None], [2, ts(1200000), None],
               [2, ts(1800000), None]])
        # GROUP BY: one row per group; site 1's value is NULL.
        self._expect(
            "select site, sv_n from table(time_align(table vgrp, TRUE, 'sv:NEXT:_n')) "
            "order by site",
            [[1, None], [2, 10]])

    def test_star_matrix(self):
        """'*' combines with every phase-2 align input type.

        Ordinary table, child table, super table, view with a timestamp
        column, ordered view over a super table, view without a timestamp
        column and a view over a TIME_ALIGN result are expanded by '*'.
        Default PREV, NEXT, suffixed PREV, repeated '*' and '*' plus an
        explicit definition keep the documented schema on each type; table
        tags are never expanded, while every view output column (including
        timestamp and tag columns) is an ordinary interpolation source.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, syntax, star, ci
        """

        def columns():
            return [desc[0] for desc in tdSql.cursor.description]

        # Ordinary table: data columns are v,w,s; ts stays the row timeline.
        tdSql.query("select * from table(time_align(table t, '*:NEXT:_n')) order by ts")
        assert columns() == ["ts", "v_n", "w_n", "s_n"], columns()
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 3, "aa")
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 3, "bb")

        # Child table: one data column.
        tdSql.query("select * from table(time_align(table c1, '*::_a')) order by ts")
        assert columns() == ["ts", "v_a"], columns()
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(2, 1, 3)

        # Super table: '*' excludes the tag and keeps per-device state.
        tdSql.query("select * from table(time_align(table st, '*:NEXT:_n')) order by ts")
        assert columns() == ["ts", "v_n"], columns()
        tdSql.checkRows(6)
        values = sorted(tdSql.queryResult, key=lambda row: (ts_ms(row[0]), row[1] is None, row[1] or 0))
        assert [row[1] for row in values] == [1, 10, 3, 20, 3, None], values

        # View with a timestamp column: '*' expands ts as a data column.
        tdSql.query("select * from table(time_align(table vw, '*:NEXT:_n')) order by ts_n")
        assert columns() == ["ts_n", "v_n", "w_n", "s_n"], columns()
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, ts(0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 1, None)

        # Ordered view over a super table: every view output column, including
        # the tag column, is expanded by '*'.
        tdSql.query("select * from table(time_align(table vs, '*::_a')) order by ts_a")
        assert columns() == ["site_a", "ts_a", "v_a"], columns()
        tdSql.checkRows(6)

        # View without a timestamp column: three data columns.
        tdSql.query("select * from table(time_align(table vnots, '*:NEXT:_n'))")
        assert columns() == ["v_n", "w_n", "s_n"], columns()
        tdSql.checkRows(5)

        # View over a TIME_ALIGN result: '*' covers ts and the filled column.
        tdSql.query("select * from table(time_align(table va, '*::_a')) order by ts_a")
        assert columns() == ["ts_a", "v_p_a"], columns()
        tdSql.checkRows(5)

        # Repeated '*' plus an explicit definition; definition order wins.
        tdSql.query("select * from table(time_align(table t, '*:NEXT:_a;*:PREV:_b')) order by ts")
        assert columns() == ["ts", "v_a", "w_a", "s_a", "v_b", "w_b", "s_b"], columns()
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 4, 1)

        tdSql.query("select * from table(time_align(table vw, '*:NEXT:_a;v:PREV:_b')) "
                    "order by ts_a")
        assert columns() == ["ts_a", "v_a", "w_a", "s_a", "v_b"], columns()
        tdSql.checkData(0, 4, 1)

        # output_raw=TRUE: raw columns first (tags included, in view order),
        # then aligned data columns in definition order.
        tdSql.query("select * from table(time_align(table st, TRUE, '*::_a')) order by ts")
        assert columns() == ["ts", "v", "v_a", "site"], columns()
        tdSql.checkRows(6)

        tdSql.query("select * from table(time_align(table vs, TRUE, '*::_a')) order by ts_a")
        assert columns() == ["site", "ts", "v", "site_a", "ts_a", "v_a"], columns()
        tdSql.checkRows(6)

        tdSql.query("select * from table(time_align(table vnots, TRUE, '*::_a'))")
        assert columns() == ["v", "w", "s", "v_a", "w_a", "s_a"], columns()
        tdSql.checkRows(5)

    def test_view_permission(self):
        """SELECT on the view authorizes TIME_ALIGN(view) like a direct query.

        FS 6.2: the privilege model matches a direct query on the view.  A
        user with only view SELECT can align the view and read its output;
        after REVOKE the same query is denied again.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, negative, auth, ci
        """
        user = "ta_align_p2_viewperm"
        password = "test12@#*"
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"create user {user} pass '{password}'")
        tdSql.execute(f"grant use on database {DB} to {user}")
        tdSql.execute(f"grant select on view {DB}.vw to {user}")
        tdSql.execute("reset query cache")

        kwargs = {"user": user, "password": password}
        if getattr(self, "host", None):
            kwargs["host"] = self.host
        if getattr(self, "port", None):
            kwargs["port"] = self.port

        conn = taos.connect(**kwargs)
        try:
            limited = TDSql()
            limited.init(conn.cursor())
            limited.execute(f"use {DB}")
            limited.query("select count(*) from table(time_align(table vw))")
            limited.checkData(0, 0, 5)
            limited.query("select count(*) from table(time_align(table vw, 'v:NEXT:_n'))")
            limited.checkData(0, 0, 5)

            tdSql.execute(f"revoke select on view {DB}.vw from {user}")
            tdSql.execute("reset query cache")
            limited.error("select * from table(time_align(table vw)) limit 1",
                          PAR_PERMISSION_DENIED)
        finally:
            conn.close()
            tdSql.execute(f"drop user if exists {user}")

    def test_explain_plan(self):
        """EXPLAIN keeps the align host visible where the design requires it.

        A view carries one extra Projection above its own plan, while a real
        table is aligned inside TableScan and shows no additional node.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, explain, ci
        """
        tdSql.query("explain select ts, v_n from table(time_align(table t, 'v:NEXT:_n'))")
        real_plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        tdSql.query("explain select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT:_n'))")
        view_plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert view_plan.count("Projection") == real_plan.count("Projection") + 1, \
            f"real plan:\n{real_plan}\nview plan:\n{view_plan}"

    def test_next_hold_limit(self):
        """A NEXT gap larger than the held-row limit fails the query.

        The first row has a value and every later row is NULL, so NEXT keeps
        the whole table pending.  Once the stage exceeds its row cap the query
        must fail with an out-of-memory error instead of growing without
        bound.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, memory, negative, ci
        """
        table = "t_biggap"
        tdSql.execute(f"drop table if exists {table}")
        tdSql.execute(f"create table {table}(ts timestamp, v int)")
        tdSql.execute(f"insert into {table} values ({ts(0)}, 1)")
        rows = 1000 * 1000 + 100
        batch = []
        for i in range(1, rows):
            batch.append(f"({ts(i * 1000)}, NULL)")
            if len(batch) == 10000:
                tdSql.execute(f"insert into {table} values " + " ".join(batch))
                batch = []
        if batch:
            tdSql.execute(f"insert into {table} values " + " ".join(batch))
        tdSql.query(f"select count(*) from {table}")
        tdSql.checkData(0, 0, rows)
        tdSql.execute(f"create view v_biggap as select ts, v from {table}")
        tdSql.error(
            "select ts, v_n from table(time_align(table v_biggap, TRUE, 'v:NEXT:_n'))",
            0x80000102)
        tdSql.execute("drop view if exists v_biggap")
        tdSql.execute(f"drop table {table}")

    def test_negative(self):
        """Tags and hidden columns stay rejected; the view ts is not special.

        Column visibility follows the virtual-table rules; unknown definitions,
        duplicate outputs, raw-name conflicts, argument-order errors and
        four-segment definitions are rejected for real tables and views alike.
        A view column that no definition names stays invisible, including the
        view's timestamp column; a view has no implicit primary timestamp.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, negative, ci
        """
        tdSql.error("select * from table(time_align(table st, 'site:NEXT:_a'))", PAR_INVALID_COLUMN)
        tdSql.error("select * from table(time_align(table t, 'ts:NEXT:_a'))", PAR_INVALID_COLUMN)
        tdSql.error("select * from table(time_align(table t, 'v:LINEAR:_a'))", PAR_NOT_ALLOWED_FUNC)
        tdSql.error("select v from table(time_align(table t, 'w:PREV:_p'))", PAR_INVALID_COLUMN)
        tdSql.error("select w from table(time_align(table vw, 'v:PREV:_p'))", PAR_INVALID_COLUMN)
        # A view's timestamp column is not emitted unless a definition names it
        # (or output_raw is TRUE); it has no primary-timestamp fallback.
        tdSql.error("select ts from table(time_align(table vw, 'v:PREV:_p'))", PAR_INVALID_COLUMN)
        tdSql.error("select ts from table(time_align(table vnots, 'v:PREV:_p'))",
                    PAR_INVALID_COLUMN)

        # Tags on child tables cannot be interpolated, but a view exposes every
        # output column (including tag columns) as an interpolation source.
        tdSql.error("select * from table(time_align(table c1, 'site:PREV:_a'))", PAR_INVALID_COLUMN)
        tdSql.query("select site_p from table(time_align(table vs, 'site:PREV:_p'))")
        tdSql.checkRows(6)
        for i, value in enumerate((1, 1, 1, 2, 2, 2)):
            tdSql.checkData(i, 0, value)

        # Definition errors share the parser rules for real tables and views:
        # duplicate outputs, raw-name conflicts, unknown columns/methods,
        # empty definitions, argument order/type and the four-segment limit.
        for source in ("t", "vw"):
            tdSql.error(f"select * from table(time_align(table {source}, 'v:PREV:_a;v:PREV:_a'))",
                        PAR_DUPLICATED_COLUMN)
            tdSql.error(f"select * from table(time_align(table {source}, TRUE))",
                        PAR_DUPLICATED_COLUMN)
            tdSql.error(f"select * from table(time_align(table {source}, TRUE, 'v:PREV:'))",
                        PAR_DUPLICATED_COLUMN)
            tdSql.error(f"select * from table(time_align(table {source}, 'nocol:PREV:_a'))",
                        PAR_INVALID_COLUMN)
            tdSql.error(f"select * from table(time_align(table {source}, 'v:_a'))",
                        PAR_NOT_ALLOWED_FUNC)
            tdSql.error(f"select * from table(time_align(table {source}, ''))", PAR_SYNTAX_ERROR)
            tdSql.error(f"select * from table(time_align(table {source}, 'v:PREV:_a', TRUE))",
                        PAR_SYNTAX_ERROR)
            tdSql.error(f"select * from table(time_align(table {source}, 'v:PREV:_a', 'v:PREV:_b'))",
                        PAR_SYNTAX_ERROR)
            tdSql.error(f"select * from table(time_align(table {source}, 'v:PREV:_a:x'))",
                        PAR_SYNTAX_ERROR)
            tdSql.error(f"select * from table(time_align(table {source}, 123))", PAR_SYNTAX_ERROR)

        # A view column not named by the definition stays invisible, exactly
        # like a real-table column.
        tdSql.error("select cnt from table(time_align(table vwin, 'sv:PREV:_p'))", PAR_INVALID_COLUMN)

    def test_permission(self):
        """SELECT privilege on the input real table is enforced.

        FS 6.1: TIME_ALIGN uses the same privilege model as a direct query.
        A user with only USE is denied, GRANT SELECT on the table allows both
        the direct query and the table function, and REVOKE denies again.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, negative, auth, ci
        """
        user = "ta_align_p2_perm"
        password = "test12@#*"
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"create user {user} pass '{password}'")
        tdSql.execute(f"grant use on database {DB} to {user}")
        tdSql.execute("reset query cache")

        kwargs = {"user": user, "password": password}
        if getattr(self, "host", None):
            kwargs["host"] = self.host
        if getattr(self, "port", None):
            kwargs["port"] = self.port

        conn = taos.connect(**kwargs)
        try:
            limited = TDSql()
            limited.init(conn.cursor())
            limited.execute(f"use {DB}")
            for sql in ("select * from t limit 1",
                        "select * from table(time_align(table t)) limit 1"):
                limited.error(sql, PAR_PERMISSION_DENIED)

            tdSql.execute(f"grant select on table {DB}.t to {user}")
            tdSql.execute("reset query cache")
            limited.query("select count(*) from t")
            limited.checkData(0, 0, 5)
            limited.query("select count(*) from table(time_align(table t))")
            limited.checkData(0, 0, 5)

            tdSql.execute(f"revoke select on table {DB}.t from {user}")
            tdSql.execute("reset query cache")
            limited.error("select * from table(time_align(table t)) limit 1",
                          PAR_PERMISSION_DENIED)
        finally:
            conn.close()
            tdSql.execute(f"drop user if exists {user}")
    def test_time_range_next(self):
        """NEXT(duration) bounds how far the successor value may be.

        The distance is measured from the current row to the successor's
        timestamp and is a closed interval.  Only NULL rows are interpolated;
        out-of-range positions become NULL and a later candidate never replaces
        them.  Monotonic scan inputs resolve expired positions early; view
        inputs keep the output-row order.  Super tables, partition-by scans and
        virtual table scans keep one sequence per subtable.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, time_range, ci
        """
        import datetime

        def utc_ms(year, month, day):
            return int(datetime.datetime(year, month, day, 12, tzinfo=datetime.timezone.utc).timestamp() * 1000)

        def expect_second_col(sql, expected):
            tdSql.query(sql)
            tdSql.checkRows(len(expected))
            for i, value in enumerate(expected):
                tdSql.checkData(i, 1, value)

        def insert_rows(table, offsets, values):
            cells = " ".join(
                f"({offset}, {'NULL' if value is None else value})" for offset, value in zip(offsets, values))
            tdSql.execute(f"insert into {table} values {cells}")

        # Value at 0s, a gap, a value at 5s and a gap to 9s.
        offsets = [0, 1000, 2000, 3000, 5000, 6000, 9000]
        values = [1, None, None, None, 9, None, None]
        tdSql.execute("create table tn_next(ts timestamp, v int)")
        insert_rows("tn_next", [ts(o) for o in offsets], values)

        # Closed interval: the row at 3s is exactly 2s before the value at 5s.
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(2s):_n')) order by ts",
            [1, None, None, 9, 9, None, None])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(1s):_n')) order by ts",
            [1, None, None, None, 9, None, None])
        # A row that has its own value is never replaced.
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(4s):_n')) order by ts",
            [1, 9, 9, 9, 9, None, None])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(3s):_n')) order by ts",
            [1, None, 9, 9, 9, None, None])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(0s):_n')) order by ts",
            [1, None, None, None, 9, None, None])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT:_n')) order by ts",
            [1, 9, 9, 9, 9, None, None])
        for method in ("v:NEXT(2S):_n", "v:NEXT( 2s ):_n", "v:NEXT (2s):_n"):
            expect_second_col(
                f"select ts, v_n from table(time_align(table tn_next, '{method}')) order by ts",
                [1, None, None, 9, 9, None, None])
        # '*' expansion applies the range to every data column.
        expect_second_col(
            "select ts, v_s from table(time_align(table tn_next, '*:NEXT(2s):_s')) order by ts",
            [1, None, None, 9, 9, None, None])

        # Different ranges for different columns are independent.
        tdSql.query("select ts, v_1, w_4 from table(time_align(table t, "
                    "'v:NEXT(1s):_1;w:NEXT(4s):_4')) order by ts")
        tdSql.checkRows(5)
        expected = [(1, 10), (None, 20), (3, 20), (3, 40), (None, 40)]
        for i, (v1, w4) in enumerate(expected):
            tdSql.checkData(i, 1, v1)
            tdSql.checkData(i, 2, w4)

        # Fixed units.
        unit_values = [1, None, None, None, 9, None, None]
        for unit, step in (("a", 1), ("s", 1000), ("m", 60000), ("h", 3600000),
                           ("d", 86400000), ("w", 604800000)):
            table = f"tn_u_{unit}"
            tdSql.execute(f"create table {table}(ts timestamp, v int)")
            insert_rows(table, [ts(i * step) for i in (0, 1, 2, 3, 4, 5, 8)], unit_values)
            expect_second_col(
                f"select ts, v_n from table(time_align(table {table}, 'v:NEXT(2{unit}):_n')) order by ts",
                [1, None, 9, 9, 9, None, None])

        # Units below the input precision convert to 0 ticks.
        tdSql.execute("create table tn_u_sub(ts timestamp, v int)")
        insert_rows("tn_u_sub", [ts(i) for i in (0, 1, 2, 3, 4, 5, 8)], unit_values)
        for unit in ("b", "u"):
            expect_second_col(
                f"select ts, v_n from table(time_align(table tn_u_sub, 'v:NEXT(2{unit}):_n')) order by ts",
                [1, None, None, None, 9, None, None])

        # Calendar units: Apr 12 is within one month of Mar 15, Apr 18 is not.
        tdSql.execute("create table tn_cal(ts timestamp, v int)")
        cal_rows = [
            (utc_ms(2023, 3, 15), None),
            (utc_ms(2023, 4, 12), 7),
            (utc_ms(2023, 4, 18), None),
            (utc_ms(2023, 9, 14), 8),
        ]
        insert_rows("tn_cal", [r[0] for r in cal_rows], [r[1] for r in cal_rows])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_cal, 'v:NEXT(1n):_n')) order by ts",
            [7, 7, None, 8])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_cal, 'v:NEXT(6n):_n')) order by ts",
            [7, 7, 8, 8])

        # Monotonic scan input with a long NULL run: positions are resolved as
        # soon as their deadline passes, and the tail within the range is filled
        # across blocks.
        tdSql.execute("create table tn_big(ts timestamp, v int)")
        total = 5000
        batch = []
        for i in range(total):
            value = 1 if i == 0 else (9 if i == total - 1 else None)
            batch.append(f"({ts(i * 1000)}, {'NULL' if value is None else value})")
            if len(batch) == 500:
                tdSql.execute("insert into tn_big values " + " ".join(batch))
                batch = []
        if batch:
            tdSql.execute("insert into tn_big values " + " ".join(batch))
        tdSql.query("select count(v_n) from table(time_align(table tn_big, 'v:NEXT(10s):_n'))")
        tdSql.checkData(0, 0, 12)
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_big, 'v:NEXT(10s):_n')) order by ts "
            f"limit 3 offset {total - 12}",
            [None, 9, 9])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_big, 'v:NEXT(10s):_n')) order by ts "
            f"limit 2 offset {total - 2}",
            [9, 9])

        # Super table merge scan: one sequence per child, aligned before merge.
        tdSql.execute("create stable tn_st(ts timestamp, v int) tags(site int)")
        tdSql.execute("create table tn_c1 using tn_st tags(1)")
        tdSql.execute("create table tn_c2 using tn_st tags(2)")
        tdSql.execute(f"insert into tn_c1 values ({ts(0)}, 1) ({ts(1000)}, NULL) ({ts(2000)}, NULL)")
        tdSql.execute(f"insert into tn_c2 values ({ts(100)}, 10) ({ts(3100)}, NULL) ({ts(5100)}, 20)")
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_st, 'v:NEXT(2s):_n')) order by ts",
            [1, 10, None, None, 20, 20])
        tdSql.query("select site, ts, v_n from table(time_align(table tn_st, TRUE, 'v:NEXT(2s):_n')) "
                    "partition by site order by site, ts")
        tdSql.checkRows(6)
        expected = [(1, ts(0), 1), (1, ts(1000), None), (1, ts(2000), None),
                    (2, ts(100), 10), (2, ts(3100), 20), (2, ts(5100), 20)]
        for i, (site, timestamp, value) in enumerate(expected):
            tdSql.checkData(i, 0, site)
            tdSql.checkData(i, 1, ts_ms(timestamp))
            tdSql.checkData(i, 2, value)
        tdSql.query("select tbname, ts, v_n from table(time_align(table tn_st, TRUE, 'v:NEXT(2s):_n')) "
                    "partition by tbname order by tbname, ts")
        tdSql.checkRows(6)
        for i, value in enumerate([1, None, None, 10, 20, 20]):
            tdSql.checkData(i, 2, value)

        # Virtual table input (virtual table scan host).
        tdSql.execute("create vtable tn_vt(ts timestamp, v int from tn_next.v)")
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_vt, 'v:NEXT(2s):_n')) order by ts",
            [1, None, None, 9, 9, None, None])
        tdSql.execute("create vtable tn_vcal(ts timestamp, v int from tn_cal.v)")
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_vcal, 'v:NEXT(1n):_n')) order by ts",
            [7, 7, None, 8])

        # Descending output keeps the scan ascending: the Sort stays above the
        # align stage, so the values equal the ascending result and only the
        # row order changes.  This holds for the scan host, the merge scan and
        # the virtual table scan alike.
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(2s):_n')) order by ts desc",
            [None, None, 9, 9, None, None, 1])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_next, 'v:NEXT(2s):_n')) "
            "order by ts desc limit 3",
            [None, None, 9])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_st, 'v:NEXT(2s):_n')) order by ts desc",
            [20, 20, None, None, 10, 1])
        tdSql.query("select site, ts, v_n from table(time_align(table tn_st, TRUE, "
                    "'v:NEXT(2s):_n')) partition by site order by site, ts desc")
        tdSql.checkRows(6)
        expected = [(1, ts(2000), None), (1, ts(1000), None), (1, ts(0), 1),
                    (2, ts(5100), 20), (2, ts(3100), 20), (2, ts(100), 10)]
        for i, (site, timestamp, value) in enumerate(expected):
            tdSql.checkData(i, 0, site)
            tdSql.checkData(i, 1, ts_ms(timestamp))
            tdSql.checkData(i, 2, value)
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_vt, 'v:NEXT(2s):_n')) order by ts desc",
            [None, None, 9, 9, None, None, 1])
        expect_second_col(
            "select ts, v_n from table(time_align(table tn_vt, 'v:NEXT(2s):_n')) "
            "order by ts desc limit 3",
            [None, None, 9])
        # DESC is pushed into the scan / merge scan / virtual scan: no Sort is
        # left above the align stage.
        for sql in (
            "explain select ts, v_n from table(time_align(table tn_next, 'v:NEXT(2s):_n')) "
            "order by ts desc",
            "explain select ts, v_n from table(time_align(table tn_st, 'v:NEXT(2s):_n')) "
            "order by ts desc",
            "explain select ts, v_n from table(time_align(table tn_vt, 'v:NEXT(2s):_n')) "
            "order by ts desc",
        ):
            tdSql.query(sql)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Sort" not in plan, plan
        # LAST() may also request a descending scan; the direction-aware stage
        # keeps the filled values identical to ascending order.
        tdSql.query("select last(v_p), first(v_p) from table(time_align(table tn_next, 'v:PREV:_p'))")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 1)

        # View input follows the view's output order; the range is ignored and
        # no time axis is required.
        expect_second_col(
            "select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT(2s):_n')) order by ts",
            [1, 3, 3, 3, None])
        tdSql.query("select v_n from table(time_align(table vw, 'v:NEXT(2s):_n'))")
        tdSql.checkRows(5)
        for i, value in enumerate([1, 3, 3, 3, None]):
            tdSql.checkData(i, 0, value)
        # The outer DESC sort runs above the view align stage too.
        expect_second_col(
            "select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT(2s):_n')) order by ts desc",
            [None, 3, 3, 3, 1])
        # The range is ignored for views: NEXT(1s) still crosses a 2s gap.
        expect_second_col(
            "select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT(1s):_n')) order by ts",
            [1, 3, 3, 3, None])

        # Partition-by view keeps per-group state: site 1 resolves inside its
        # group and site 2's trailing NULL stays NULL.
        tdSql.query("select site, ts, v_n from table(time_align(table vpart, TRUE, 'v:NEXT(2s):_n')) "
                    "order by site, ts")
        tdSql.checkRows(6)
        expected = [(1, ts(0), 1), (1, ts(1000), 3), (1, ts(3000), 3),
                    (2, ts(0), 10), (2, ts(2000), 20), (2, ts(4000), None)]
        for i, (site, timestamp, value) in enumerate(expected):
            tdSql.checkData(i, 0, site)
            tdSql.checkData(i, 1, ts_ms(timestamp))
            tdSql.checkData(i, 2, value)

        # INTERVAL and GROUP BY views keep one sequence per group: the range
        # never fills a NULL group from another group's value.
        tdSql.query("select site, ts, sv_n from table(time_align(table vwin_int, TRUE, "
                    "'sv:NEXT(2s):_n')) order by site, ts")
        tdSql.checkRows(4)
        expected = [(1, ts(0), 1), (1, ts(2000), None), (1, ts(4000), None), (2, ts(0), 10)]
        for i, (site, timestamp, value) in enumerate(expected):
            tdSql.checkData(i, 0, site)
            tdSql.checkData(i, 1, ts_ms(timestamp))
            tdSql.checkData(i, 2, value)

        # The range is ignored for view input: a GROUP BY view without a
        # TIMESTAMP column still interpolates in output-row order.
        tdSql.query("select site, sv_n from table(time_align(table vgrp, TRUE, 'sv:NEXT(2s):_n')) "
                    "order by site")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 10)
        # With a timestamp data column the grouped view keeps one sequence per
        # group: site 1's NULL never takes site 2's value.
        tdSql.execute("create view vgrp2 as select site, max(ts) as ts, max(v) as sv "
                      "from stgrp group by site")
        tdSql.query("select site, sv_n from table(time_align(table vgrp2, TRUE, 'sv:NEXT(2s):_n')) "
                    "order by site")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 10)

        # A view without a TIMESTAMP data column also accepts the range
        # syntax; the range has no effect.
        tdSql.query("select v_n from table(time_align(table vnots, 'v:NEXT(2s):_n'))")
        tdSql.checkRows(5)
        for i, value in enumerate([1, 3, 3, 3, None]):
            tdSql.checkData(i, 0, value)

    def test_desc_matrix(self):
        """Descending delivery: every push and no-push host keeps time semantics.

        Rows delivered in timestamp DESC order are filled direction aware, so
        PREV/NEXT values match the ascending result and only the output order
        differs.  The matrix covers the push paths (plain scan, merge scan,
        single/multi-source virtual table, view Projection, LAST() scan order,
        DESC+LIMIT) and the paths where the sort stays above the align stage
        (ORDER BY an aligned value, tagged partition, virtual stable tag
        partition, a view ordered by a non-timestamp column, DISTINCT/value
        order).  Pushed plans must contain no Sort; retained plans must.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, desc, matrix, ci
        """
        import datetime

        def utc_ms(year, month, day):
            return int(datetime.datetime(year, month, day, 12, tzinfo=datetime.timezone.utc).timestamp() * 1000)

        def fill_range(timestamps, values, method, span=None):
            out = [None] * len(values)
            if method == "PREV":
                value = vts = None
                for i, cur in enumerate(values):
                    if cur is not None:
                        value, vts, out[i] = cur, timestamps[i], cur
                    elif value is not None and (span is None or timestamps[i] - vts <= span):
                        out[i] = value
            else:
                value = vts = None
                for i in range(len(values) - 1, -1, -1):
                    cur = values[i]
                    if cur is not None:
                        value, vts, out[i] = cur, timestamps[i], cur
                    elif value is not None and (span is None or vts - timestamps[i] <= span):
                        out[i] = value
            return out

        def plan_of(sql):
            tdSql.query("explain " + sql)
            return "\n".join(str(row[0]) for row in tdSql.queryResult)

        # Half-second source disjoint from t: a multi-source vtable, a virtual
        # stable with one child per source, and a single-source vtable.
        tdSql.execute("create table d_os(ts timestamp, v int)")
        tdSql.execute(f"insert into d_os values ({ts(500)}, NULL) ({ts(1500)}, 10) "
                      f"({ts(2500)}, NULL) ({ts(3500)}, 30) ({ts(4500)}, NULL)")
        tdSql.execute("create vtable d_vt(ts timestamp, v int from t.v)")
        tdSql.execute("create vtable d_multi(ts timestamp, v int from t.v, w int from d_os.v)")
        tdSql.execute("create stable d_vs(ts timestamp, v int) tags(site int) virtual 1")
        tdSql.execute("create vtable d_vs1(v from t.v) using d_vs tags(1)")
        tdSql.execute("create vtable d_vs2(v from d_os.v) using d_vs tags(2)")

        ts5 = [ts(i * 1000) for i in range(5)]
        vp5 = fill_range(ts5, V, "PREV", 2000)
        vn5 = fill_range(ts5, V, "NEXT", 1000)
        # Plain table scan: both methods and ranges; DESC is pushed (no Sort).
        self._expect(
            "select ts, v_p, v_n from table(time_align(table t, 'v:PREV(2s):_p;v:NEXT(1s):_n')) "
            "order by ts desc",
            [[ts5[i], vp5[i], vn5[i]] for i in range(4, -1, -1)])
        assert "Sort" not in plan_of(
            "select ts, v_p from table(time_align(table t, 'v:PREV(2s):_p')) order by ts desc")

        # Merge scan: DESC enters the per-child scans, merged with ts desc.
        # ts=0 is excluded because two children share it: the merge output
        # order for that tie is not guaranteed.  Each child is still filled
        # independently before the merge, so only the row order is omitted.
        self._expect(
            "select ts, v_n from table(time_align(table st, 'v:NEXT:_n')) "
            f"where ts in ({ts(1000)}, {ts(2000)}, {ts(3000)}, {ts(4000)}) order by ts desc",
            [[ts(4000), None], [ts(3000), 3], [ts(2000), 20], [ts(1000), 3]])
        assert "Sort" not in plan_of(
            "select ts, v_n from table(time_align(table st, 'v:NEXT:_n')) order by ts desc")

        # Single-source virtual table (align eliminated into its source scan).
        self._expect(
            "select ts, v_p from table(time_align(table d_vt, 'v:PREV(2s):_p')) order by ts desc",
            [[ts5[i], vp5[i]] for i in range(4, -1, -1)])
        assert "Sort" not in plan_of(
            "select ts, v_p from table(time_align(table d_vt, 'v:PREV(2s):_p')) order by ts desc")

        # Multi-source virtual table (VirtualScan host): one sequence per row,
        # each column filled independently along the delivered direction.
        ts10 = [ts(o) for o in (0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500)]
        v10 = [1, None, None, None, None, None, 3, None, None, None]
        w10 = [None, None, None, 10, None, None, None, 30, None, None]
        vp10 = fill_range(ts10, v10, "PREV", 2000)
        wn10 = fill_range(ts10, w10, "NEXT", 2000)
        self._expect(
            "select ts, v_p, w_n from table(time_align(table d_multi, "
            "'v:PREV(2s):_p;w:NEXT(2s):_n')) order by ts desc",
            [[ts10[i], vp10[i], wn10[i]] for i in range(9, -1, -1)])
        assert "Sort" not in plan_of(
            "select ts, v_p from table(time_align(table d_multi, 'v:PREV(2s):_p')) order by ts desc")

        # View Projection host: a plain view with an outer DESC sort keeps
        # timestamp semantics because its output row order is ascending.  A
        # timestamp-DESC view follows its output row order, so NEXT refers to
        # the next output row and no direction compensation is applied.
        vn2 = fill_range(ts5, V, "NEXT", 2000)
        self._expect(
            "select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT(2s):_n')) order by ts desc",
            [[ts5[i], vn2[i]] for i in range(4, -1, -1)])
        self._expect(
            "select ts, v_n from table(time_align(table vdasc, TRUE, 'v:NEXT(2s):_n')) order by ts desc",
            [[ts5[4], 3], [ts5[3], 3], [ts5[2], 1], [ts5[1], 1], [ts5[0], 1]])
        # The outer DESC sort stays above the view align stage: it must not
        # change the view output row order used by PREV/NEXT.
        assert "Sort" in plan_of(
            "select ts, v_n from table(time_align(table vw, TRUE, 'v:NEXT(2s):_n')) order by ts desc")

        # DESC + LIMIT: the limit stays above the align stage (plain scan) or
        # is applied by VirtualScan after it; the window rows stay correct.
        self._expect(
            "select ts, v_p from table(time_align(table t, 'v:PREV(2s):_p')) "
            "order by ts desc limit 3",
            [[ts5[i], vp5[i]] for i in (4, 3, 2)])
        # Both columns must stay selected: a projected-away source drops its
        # timestamps from the virtual table timeline.
        tdSql.query("select ts, v_n, w_n from table(time_align(table d_multi, "
                    "'v:NEXT(2s):_n;w:NEXT(2s):_n')) order by ts desc limit 3")
        tdSql.checkRows(3)
        got = [(ts_ms(row[0]), row[1], row[2]) for row in tdSql.queryResult]
        assert got == [(ts(4500), None, None), (ts(4000), None, None),
                       (ts(3500), None, 30)], got

        # LAST() may request a descending scan; the values equal ascending.
        tdSql.query("select last(v_p), first(v_p) from table(time_align(table t, 'v:PREV:_p'))")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 1)
        assert "Sort" not in plan_of(
            "select last(v_p) from table(time_align(table t, 'v:PREV:_p'))")

        # ORDER BY an aligned value: the Sort stays above the align stage.
        tdSql.query("select ts, v_p from table(time_align(table t, 'v:PREV:_p')) "
                    "where v_p is not null order by v_p desc")
        tdSql.checkRows(5)
        for i, value in enumerate([3, 3, 1, 1, 1]):
            tdSql.checkData(i, 1, value)
        assert "Sort" in plan_of(
            "select ts, v_p from table(time_align(table t, 'v:PREV:_p')) order by v_p desc")

        # Partitioned output uses a group sort: DESC is not pushed and per-child
        # state still decides the values.
        self._expect(
            "select site, ts, v_n from table(time_align(table st, 'v:NEXT:_n')) "
            "partition by site order by site, ts desc",
            [[1, ts(3000), 3], [1, ts(1000), 3], [1, ts(0), 1],
             [2, ts(4000), None], [2, ts(2000), 20], [2, ts(0), 10]])
        assert "Sort" in plan_of(
            "select site, ts, v_n from table(time_align(table st, 'v:NEXT:_n')) "
            "partition by site order by site, ts desc")
        # PARTITION BY tbname: one group per table, DESC ordered inside each.
        self._expect(
            "select tbname, ts, v_p from table(time_align(table st, 'v:PREV:_p')) "
            "partition by tbname order by tbname, ts desc",
            [["c1", ts(3000), 3], ["c1", ts(1000), 1], ["c1", ts(0), 1],
             ["c2", ts(4000), 20], ["c2", ts(2000), 20], ["c2", ts(0), 10]])
        assert "Sort" in plan_of(
            "select tbname, ts, v_p from table(time_align(table st, 'v:PREV:_p')) "
            "partition by tbname order by tbname, ts desc")

        # Virtual stable: the tag partition keeps the Sort above; each child
        # keeps its own PREV state.
        ds1 = fill_range([ts(0), ts(1000), ts(2000), ts(3000), ts(4000)], V, "PREV", 2000)
        dsv = [None, 10, None, 30, None]
        ds2_ts = [ts(500), ts(1500), ts(2500), ts(3500), ts(4500)]
        ds2 = fill_range(ds2_ts, dsv, "PREV", 2000)
        asc = ([(1, ts(0), ds1[0]), (2, ts(500), ds2[0]), (1, ts(1000), ds1[1]),
                (2, ts(1500), ds2[1]), (1, ts(2000), ds1[2]), (2, ts(2500), ds2[2]),
                (1, ts(3000), ds1[3]), (2, ts(3500), ds2[3]), (1, ts(4000), ds1[4]),
                (2, ts(4500), ds2[4])])
        self._expect(
            "select site, ts, v_p from table(time_align(table d_vs, 'v:PREV(2s):_p')) "
            "order by ts desc",
            [list(row) for row in reversed(asc)])
        assert "Sort" in plan_of(
            "select site, ts, v_p from table(time_align(table d_vs, 'v:PREV(2s):_p')) order by ts desc")

        # A view ordered by a non-timestamp column keeps output-row order: the
        # direction flag is unknown, so no timestamp compensation happens.
        tdSql.execute("create table d_valw(ts timestamp, v int, w int)")
        tdSql.execute(f"insert into d_valw values ({ts(0)}, 1, 10) ({ts(1000)}, NULL, 30) "
                      f"({ts(2000)}, NULL, 20) ({ts(3000)}, 3, 40) ({ts(4000)}, NULL, 50)")
        tdSql.execute("create view d_vw_val as select ts, v, w from d_valw order by w desc")
        tdSql.query("select ts, v_n, v_p from "
                    "table(time_align(table d_vw_val, TRUE, 'v:NEXT:_n;v:PREV:_p'))")
        tdSql.checkRows(5)
        got = {ts_ms(row[0]): (row[1], row[2]) for row in tdSql.queryResult}
        assert got == {ts(0): (1, 1), ts(1000): (1, 3), ts(2000): (1, 3),
                       ts(3000): (3, 3), ts(4000): (3, None)}, got
        assert "Sort" in plan_of(
            "select ts, v_n from table(time_align(table d_vw_val, TRUE, 'v:NEXT:_n'))")

        # Outer WHERE stays above the align stage and does not truncate input.
        self._expect(
            "select ts, v_p from table(time_align(table t, 'v:PREV(2s):_p')) "
            f"where ts >= {ts(2000)} order by ts desc",
            [[ts(4000), 3], [ts(3000), 3], [ts(2000), 1]])

        # Calendar range along DESC delivery.
        cal = [(utc_ms(2023, 3, 15), 7), (utc_ms(2023, 4, 12), None),
               (utc_ms(2023, 4, 18), None), (utc_ms(2023, 6, 12), None),
               (utc_ms(2023, 6, 18), None), (utc_ms(2023, 9, 14), 8),
               (utc_ms(2023, 12, 11), None), (utc_ms(2023, 12, 18), None)]
        tdSql.execute("create table d_cal(ts timestamp, v int)")
        tdSql.execute("insert into d_cal values " +
                      " ".join(f"({t}, {'NULL' if v is None else v})" for t, v in cal))
        cal_prev = [7, 7, None, None, None, 8, None, None]
        self._expect(
            "select ts, v_p from table(time_align(table d_cal, 'v:PREV(1n):_p')) order by ts desc",
            [[cal[i][0], cal_prev[i]] for i in range(7, -1, -1)])
        tdSql.execute("create vtable d_calvt(ts timestamp, v int from d_cal.v)")
        self._expect(
            "select ts, v_p from table(time_align(table d_calvt, 'v:PREV(1n):_p')) order by ts desc",
            [[cal[i][0], cal_prev[i]] for i in range(7, -1, -1)])
        assert "Sort" not in plan_of(
            "select ts, v_p from table(time_align(table d_cal, 'v:PREV(1n):_p')) order by ts desc")

        # Units below the time-axis precision become zero ticks; DESC does not
        # change that.
        tdSql.execute("create table d_sub(ts timestamp, v int)")
        tdSql.execute(f"insert into d_sub values ({ts(0)}, 1) ({ts(4000)}, 9)")
        self._expect(
            "select ts, v_p from table(time_align(table d_sub, 'v:PREV(2u):_p')) "
            "order by ts desc",
            [[ts(4000), 9], [ts(0), 1]])
        assert "Sort" not in plan_of(
            "select ts, v_p from table(time_align(table d_sub, 'v:PREV(2u):_p')) order by ts desc")

    def test_composite_primary_key(self):
        """Composite primary keys: equal timestamps fill in primary-key order.

        A table with ``(ts, pk)`` primary key has several rows per timestamp.
        TIME_ALIGN runs once per series row sequence, which is ordered by the
        composite key: PREV of a NULL row at the same timestamp uses the
        smaller pk value and NEXT the larger pk value.  Both ascending and
        descending delivery keep that order (DESC flips it together with the
        timestamp), and a super table keeps the state per child.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, composite_pk, desc, ci
        """
        def plan_of(sql):
            tdSql.query("explain " + sql)
            return "\n".join(str(row[0]) for row in tdSql.queryResult)

        # t=0: pk1=10, pk2=NULL, pk3=30; t=1: pk1=NULL, pk2=20;
        # t=2: pk1=NULL, pk2=NULL.
        tdSql.execute("create table d_pk1(ts timestamp, pk int primary key, v int)")
        tdSql.execute(f"insert into d_pk1 values ({ts(0)}, 1, 10) ({ts(0)}, 2, NULL) "
                      f"({ts(0)}, 3, 30) ({ts(1000)}, 1, NULL) ({ts(1000)}, 2, 20) "
                      f"({ts(2000)}, 1, NULL) ({ts(2000)}, 2, NULL)")
        keys = [(ts(0), 1), (ts(0), 2), (ts(0), 3),
                (ts(1000), 1), (ts(1000), 2), (ts(2000), 1), (ts(2000), 2)]
        prev_all = [10, 10, 30, 30, 20, 20, 20]
        next_all = [10, 30, 30, 20, 20, None, None]

        def pk_map(sql, ncols):
            tdSql.query(sql)
            tdSql.checkRows(len(keys))
            out = {}
            for row in tdSql.queryResult:
                out[(ts_ms(row[0]), row[1])] = tuple(row[2:2 + ncols])
            return out

        expect = {key: (prev_all[i], next_all[i]) for i, key in enumerate(keys)}
        got = pk_map("select ts, pk, v_p, v_n from "
                     "table(time_align(table d_pk1, TRUE, 'v:PREV:_p;v:NEXT:_n')) order by ts desc", 2)
        assert got == expect, got
        # DESC delivery is pushed into the scan (no Sort above the align stage).
        tdSql.query("explain select ts, v_p from table(time_align(table d_pk1, 'v:PREV:_p')) "
                    "order by ts desc")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Sort" not in plan, plan
        # Unordered publication keeps the ascending composite-key order.
        got = pk_map("select ts, pk, v_p, v_n from "
                     "table(time_align(table d_pk1, TRUE, 'v:PREV:_p;v:NEXT:_n'))", 2)
        assert got == expect, got

        # Range 0 accepts only the same-timestamp neighbours in pk order.
        prev0 = [10, 10, 30, None, 20, None, None]
        next0 = [10, 30, 30, 20, 20, None, None]
        expect = {key: (prev0[i], next0[i]) for i, key in enumerate(keys)}
        got = pk_map("select ts, pk, v_p, v_n from "
                     "table(time_align(table d_pk1, TRUE, 'v:PREV(0s):_p;v:NEXT(0s):_n')) "
                     "order by ts desc", 2)
        assert got == expect, got

        # Range 2 reaches the previous/next timestamp as well.
        expect = {key: (prev_all[i], next_all[i]) for i, key in enumerate(keys)}
        got = pk_map("select ts, pk, v_p, v_n from "
                     "table(time_align(table d_pk1, TRUE, 'v:PREV(2s):_p;v:NEXT(2s):_n')) "
                     "order by ts desc", 2)
        assert got == expect, got

        # Super table with composite-key children: state stays per child and
        # the equal-timestamp order is still the child's pk order.
        tdSql.execute("create stable d_pks(ts timestamp, pk int primary key, v int) tags(site int)")
        tdSql.execute("create table dk1 using d_pks tags(1)")
        tdSql.execute("create table dk2 using d_pks tags(2)")
        tdSql.execute(f"insert into dk1 values ({ts(0)}, 1, 1) ({ts(0)}, 2, NULL) "
                      f"({ts(0)}, 3, 3) ({ts(1000)}, 1, NULL) ({ts(1000)}, 2, 2)")
        tdSql.execute(f"insert into dk2 values ({ts(0)}, 1, 100) ({ts(0)}, 2, NULL) "
                      f"({ts(0)}, 3, 300) ({ts(1000)}, 1, NULL) ({ts(1000)}, 2, 200)")
        sk = [(1, ts(0), 1), (1, ts(0), 2), (1, ts(0), 3), (1, ts(1000), 1), (1, ts(1000), 2),
              (2, ts(0), 1), (2, ts(0), 2), (2, ts(0), 3), (2, ts(1000), 1), (2, ts(1000), 2)]
        sp = [1, 1, 3, 3, 2, 100, 100, 300, 300, 200]
        sn = [1, 3, 3, 2, 2, 100, 300, 300, 200, 200]

        tdSql.query("select site, ts, pk, v_p, v_n from "
                    "table(time_align(table d_pks, TRUE, 'v:PREV:_p;v:NEXT:_n')) order by ts desc")
        tdSql.checkRows(len(sk))
        got = {}
        for row in tdSql.queryResult:
            got[(row[0], ts_ms(row[1]), row[2])] = (row[3], row[4])
        expect = {key: (sp[i], sn[i]) for i, key in enumerate(sk)}
        assert got == expect, got
        tdSql.query("explain select ts, v_p from table(time_align(table d_pks, 'v:PREV:_p')) "
                    "order by ts desc")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Sort" not in plan, plan

        # Partitioned DESC output keeps one group per child; the full-key sort
        # makes the row order deterministic.
        expected = [list(key) + [sp[i], sn[i]] for i, key in enumerate(sk)]
        expected.sort(key=lambda r: (r[0], -r[1], -r[2]))
        self._expect(
            "select site, ts, pk, v_p, v_n from "
            "table(time_align(table d_pks, TRUE, 'v:PREV:_p;v:NEXT:_n')) "
            "partition by site order by site, ts desc, pk desc",
            [[r[0], r[1], r[2], r[3], r[4]] for r in expected])
        assert "Sort" in plan_of(
            "select site, ts, pk, v_p from table(time_align(table d_pks, TRUE, 'v:PREV:_p')) "
            "partition by site order by site, ts desc, pk desc")
        # A virtual table cannot reference a composite-key source (engine
        # constraint): the scenario exists on table/super-table scans only.
        tdSql.error("create vtable d_pkv(ts timestamp, v int from d_pk1.v)", 0x8000268D)

    def test_count_raw_vs_aligned(self):
        """count() on raw (not interpolated) and aligned columns side by side.

        The raw source column keeps its own NULLs; the aligned column and the
        optional range change only the second count.  A data column that no
        definition names is passed through raw and must behave like any other
        non-interpolated column.  Per-child and per-window counts follow the
        same rule, and a composite-key table counts rows, raw values and
        aligned values independently.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, phase2, aggregate, count, ci
        """
        # t: v=[1,None,None,3,None], w=[10,None,20,None,40].
        self._expect(
            "select count(*), count(v), count(w), count(v_p), count(v_n) from "
            "table(time_align(table t, TRUE, 'v:PREV:_p;v:NEXT:_n'))",
            [[5, 2, 3, 5, 4]])
        # A column named by no definition stays raw; only v is interpolated.
        self._expect(
            "select count(*), count(v), count(w), count(v_p) from "
            "table(time_align(table t, TRUE, 'v:PREV:_p'))",
            [[5, 2, 3, 5]])
        # Ranges reduce the aligned count without touching the raw count:
        # PREV(2s) fills all five rows, NEXT(1s) keeps its own value, the 2s
        # successor and the 1s successor but not the row two seconds later.
        self._expect(
            "select count(v), count(v_p), count(v_n) from "
            "table(time_align(table t, TRUE, 'v:PREV(2s):_p;v:NEXT(1s):_n'))",
            [[2, 5, 3]])
        # Super table: raw and aligned counts stay per child.
        self._expect(
            "select site, count(*), count(v), count(v_p), count(v_n) from "
            "table(time_align(table st, TRUE, 'v:PREV:_p;v:NEXT:_n')) "
            "partition by site order by site",
            [[1, 3, 2, 3, 3], [2, 3, 2, 3, 2]])
        # Per-window raw vs aligned counts.
        self._expect(
            "select _wstart, count(*), count(v), count(v_p), count(v_n) from "
            "table(time_align(table t, TRUE, 'v:PREV:_p;v:NEXT:_n')) "
            "interval(2s) order by _wstart",
            [[ts(0), 2, 1, 2, 2], [ts(2000), 2, 1, 2, 2], [ts(4000), 1, 0, 1, 0]])
        # Composite primary key: one row per (ts, pk), raw values only where
        # the source has them, aligned values per row.
        tdSql.execute("create table d_cnt(ts timestamp, pk int primary key, v int)")
        tdSql.execute(f"insert into d_cnt values ({ts(0)}, 1, 10) ({ts(0)}, 2, NULL) "
                      f"({ts(0)}, 3, 30) ({ts(1000)}, 1, NULL) ({ts(1000)}, 2, 20) "
                      f"({ts(2000)}, 1, NULL) ({ts(2000)}, 2, NULL)")
        self._expect(
            "select count(*), count(v), count(v_p), count(v_n) from "
            "table(time_align(table d_cnt, TRUE, 'v:PREV:_p;v:NEXT:_n'))",
            [[7, 3, 7, 5]])
