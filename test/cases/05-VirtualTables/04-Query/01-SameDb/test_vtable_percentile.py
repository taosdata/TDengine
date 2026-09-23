###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Exact PERCENTILE on single-source virtual tables (feat/apOnVtable).

PERCENTILE is a REPEAT_SCAN_FUNC (exact, tMemBucket based) and is "single-table
only": it needs two passes (pass 1 = min/max/count, pass 2 = fill the bucket).
`translateRepeatScanFunc` allows the two single-source virtual table kinds
to run the two passes (virtual child / virtual normal); virtual super tables
stay rejected.

Two execution paths, both covered here:
- no column-value WHERE: EliminateVirtualScan collapses the VirtualScan into a
  direct `Aggregate(percentile) -> Table Scan on <origin>`, reusing the normal
  single-table two-pass (scanSeq[0]=2);
- column-value WHERE present: the VirtualScan survives elimination, the
  VtableTwoPass optimizer rule sets twoPassMode on it, and the VirtualScan
  merge operator drives the two passes itself, stamping PRE_SCAN/MAIN_SCAN on
  its output blocks. Phase 2 re-scans the origins by resetting the exchange and
  reusing the remote reader (tsdReaderResetStatus), so both passes read the
  same version.

This suite is broad: percentile combined with many other SQL features, each
checked against the underlying real table (the virtual table must produce
identical results). It also pins the features percentile must REJECT
(partition by / event / count window / virtual super table / ref-tag columns
under two-pass).

Dataset: org0 and org1 each hold ROWS rows (c_int, c_bigint, c_float, c_double);
vc0 -> org0, vc1 -> org1 (virtual child), vntb -> org0 (virtual normal),
vcref -> org0 with its tag bound as a tag-ref to tagsrc0 (ref-tag rejection test).
Built once per class in setup_class (all tests are read-only).

Catalog:
    - VirtualTable

Since: v3.4.3.0
Labels: virtual,percentile,ci,functional
"""
import math
import os
import re
import subprocess
import tempfile

import pytest

from new_test_framework.utils import tdLog, tdSql, tdDnodes


DB = "test_vtable_percentile"
ROWS = 50000  # rows per origin table — exercises real two-pass / tMemBucket volume
NUMERIC_COLS = ["c_int", "c_bigint", "c_float", "c_double"]
COL_TYPES = {"c_int": "int", "c_bigint": "bigint", "c_float": "float", "c_double": "double"}
PS = [0, 25, 50, 75, 100]
TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE = -2147473825  # 0x8000265F
TSDB_CODE_PAR_NOT_SUPPORT_VTABLE_TWO_PASS = -2147473688  # 0x800026E8
# event/count window are rejected earlier by the window-func check than the single-table check
TSDB_CODE_PAR_NOT_ALLOWED_FUNC = -2147473841  # 0x8000264F
# strict=True makes these XPASS (and thus fail the suite) once the underlying
# limitation is fixed — that is the revisit signal. When it fires, drop the
# marker and turn the case into a regular pass/expectation test.
VTABLE_TBNAME_AGG_XFAIL = (
    "Known vtable resolver limitation: tbname alongside an aggregate without "
    "GROUP BY is rejected on virtual child tables (0x260c); normal tables "
    "allow it (tbname is a per-table constant)."
)
VTABLE_STATE_WINDOW_EXPR_XFAIL = (
    "Known vtable parse limitation: state_window(<expression>) fails to parse "
    "on virtual tables (0x2600); interval/session windows (no expression arg) "
    "work."
)


class TestVtablePercentile:
    updatecfgDict = {"minReservedMemorySize": "1024"}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls._make_dataset()

    # ---- dataset ----------------------------------------------------------
    @classmethod
    def _make_dataset(cls):
        tdSql.execute(f"drop database if exists {DB};")
        tdSql.execute(f"create database {DB} vgroups 2;")
        tdSql.execute(f"use {DB};")
        for t in ("org0", "org1"):
            tdSql.execute(f"create table {t} (ts timestamp, c_int int, c_bigint bigint, "
                          f"c_float float, c_double double)")
        # org1 starts a day later so the two origins also differ in time range
        # (they already differ in vgroup and value range via `base`).
        for tbl, base, day in (("org0", 0, 1), ("org1", ROWS, 2)):
            vals = []
            for i in range(ROWS):
                v = base + i
                h, rem = divmod(i, 3600)
                mm, ss = divmod(rem, 60)
                ts = f"2020-01-{day:02d} {h:02d}:{mm:02d}:{ss:02d}.000"
                vals.append(f"('{ts}',{v},{v * 1000},{v + 0.5},{round(v * 1.1, 2)})")
            for j in range(0, len(vals), 1000):
                tdSql.execute(f"insert into {tbl} values " + ",".join(vals[j:j + 1000]))
        tdSql.execute("create stable vstb (ts timestamp, c_int int, c_bigint bigint, "
                      "c_float float, c_double double) tags (tg int) virtual 1")
        tdSql.execute("create vtable vc0 (" + ", ".join(f"org0.{c}" for c in NUMERIC_COLS)
                      + ") using vstb tags (0)")
        tdSql.execute("create vtable vc1 (" + ", ".join(f"org1.{c}" for c in NUMERIC_COLS)
                      + ") using vstb tags (1)")
        ntb = ["ts timestamp"] + [f"{c} {COL_TYPES[c]} from org0.{c}" for c in NUMERIC_COLS]
        tdSql.execute(f"create vtable vntb ({', '.join(ntb)})")
        # tag-ref fixture: vcref's tg tag is a reference to tagsrc0.tref, so a query
        # referencing tg gives the VirtualScan a TAG_REF_SOURCE child
        tdSql.execute("create stable tagsrc (ts timestamp, v int) tags (tref int)")
        tdSql.execute("create table tagsrc0 using tagsrc tags (7)")
        tdSql.execute("create vtable vcref (" + ", ".join(f"org0.{c}" for c in NUMERIC_COLS)
                      + ") using vstb tags (tg from tagsrc0.tref)")

    # ---- helpers ----------------------------------------------------------
    def _rows(self, sql):
        tdSql.query(sql)
        return [tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    def _scalar(self, sql):
        r = self._rows(sql)
        assert len(r) == 1 and len(r[0]) == 1, f"expected 1x1, got {r}: {sql}"
        return r[0][0]

    def _assert_close(self, a, b, msg, rel_tol=1e-9):
        """Scalar equality with the same float tolerance as _cmp: the two execution
        paths reassociate float ops differently, so the last bits may differ legally.
        Ints compare exactly (float64 is lossless only up to 2^53); a NULL cell is a
        hard failure with a readable message, not a TypeError from float(None).
        float32 columns pass a wider rel_tol: two legal roundings of the same value
        sit ~2^-24 apart, far above 1e-9 (the current dataset stores v+0.5, exactly
        representable below 2^23, so both paths are bit-identical — the wider
        tolerance guards future datasets that are not)."""
        assert a is not None and b is not None, f"{msg}: unexpected NULL ({a!r} vs {b!r})"
        if isinstance(a, int) and isinstance(b, int):
            assert a == b, f"{msg}: {a} != {b}"
        else:
            assert math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=1e-9), \
                f"{msg}: {a} != {b}"

    def _cmp(self, tmpl, msg, rel_tol=1e-9, abs_tol=1e-9):
        """Run tmpl (with {T}) on vc0 and org0; assert identical result sets.

        Numeric cells are compared with a small tolerance rather than exact
        equality: the two paths (collapsed scan on org0 vs two-pass VirtualScan
        merge on vc0) reassociate float/double ops in a different order, so the
        last bits of AVG/PERCENTILE results can legitimately differ.
        """
        a = self._rows(tmpl.format(T="vc0"))
        b = self._rows(tmpl.format(T="org0"))
        assert len(a) == len(b), f"{msg}: vc0 != org0\n vc0={a[:5]}\n org0={b[:5]}"
        for ra, rb in zip(a, b):
            assert len(ra) == len(rb), f"{msg}: vc0 != org0\n vc0={ra}\n org0={rb}"
            for va, vb in zip(ra, rb):
                if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                    assert math.isclose(float(va), float(vb), rel_tol=rel_tol, abs_tol=abs_tol), \
                        f"{msg}: vc0 != org0\n cell {va!r} != {vb!r}\n vc0={ra}\n org0={rb}"
                else:
                    assert va == vb, f"{msg}: vc0 != org0\n cell {va!r} != {vb!r}\n vc0={ra}\n org0={rb}"

    # ---- core: percentile on vtable == underlying table (all cols, all p) -
    def test_child_matches_origin_all_cols_all_p(self):
        """Virtual child percentile equals the origin table for all columns and p values.

        For every numeric column and every p in {0,25,50,75,100}, percentile on
        the virtual child vc0 must match percentile on the origin org0.

        Since: v3.4.3.0
        """
        for col in NUMERIC_COLS:
            # float32 columns: two legal roundings differ by ~2^-24 relative
            rel_tol = 1e-6 if col == "c_float" else 1e-9
            for p in PS:
                got = self._scalar(f"select percentile({col},{p}) from {DB}.vc0;")
                exp = self._scalar(f"select percentile({col},{p}) from {DB}.org0;")
                tdLog.info(f"vc0 percentile({col},{p})={got} (origin {exp})")
                self._assert_close(got, exp, f"vc0 {col} p{p}", rel_tol=rel_tol)

    def test_normal_matches_origin(self):
        """Virtual normal table percentile equals the origin table.

        p50 on the virtual normal table vntb must match p50 on the origin org0
        for every numeric column.

        Since: v3.4.3.0
        """
        for col in NUMERIC_COLS:
            rel_tol = 1e-6 if col == "c_float" else 1e-9
            self._assert_close(self._scalar(f"select percentile({col},50) from {DB}.vntb;"),
                               self._scalar(f"select percentile({col},50) from {DB}.org0;"),
                               f"vntb {col} p50", rel_tol=rel_tol)

    def test_child_vc1_matches_origin(self):
        """A second virtual child (different origin) also matches its origin.

        p50 on vc1 (origin org1, a different vgroup and data range) must match
        p50 on org1 for every numeric column.

        Since: v3.4.3.0
        """
        for col in NUMERIC_COLS:
            rel_tol = 1e-6 if col == "c_float" else 1e-9
            self._assert_close(self._scalar(f"select percentile({col},50) from {DB}.vc1;"),
                               self._scalar(f"select percentile({col},50) from {DB}.org1;"),
                               f"vc1 {col} p50", rel_tol=rel_tol)

    # ---- combined with OTHER aggregations --------------------------------
    def test_with_other_aggregates(self):
        """percentile combines with other aggregates in one select list.

        Runs percentile together with avg/count/min/max/sum/stddev/apercentile,
        plus several percentile calls on different columns in one query, and
        compares the whole result set against the origin table.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50), avg(c_int), count(*), min(c_int), "
                  "max(c_int), sum(c_int), stddev(c_int), apercentile(c_int,50) from {T};",
                  "mixed aggregates")
        # multiple percentiles + multiple columns at once
        self._cmp("select percentile(c_int,0), percentile(c_int,50), percentile(c_int,100), "
                  "percentile(c_double,50) from {T};", "multi-percentile")

    # ---- GROUP BY --------------------------------------------------------
    def test_group_by_expression(self):
        """percentile under GROUP BY an expression matches the origin.

        Groups by c_int/10000 so each group holds a distinct value range; the
        grouped percentile result set must equal the origin's.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50) from {T} group by c_int/10000 order by 1;",
                  "group by expression")

    @pytest.mark.xfail(reason=VTABLE_TBNAME_AGG_XFAIL, strict=True)
    def test_group_by_tbname_value(self):
        """tbname projected next to a bare aggregate on the virtual child.

        Without GROUP BY this is a single-row aggregate whose tbname is the
        table-level constant "vc0" -- no per-table grouping is exercised here;
        the case pins the vtable resolver's rejection of tbname alongside an
        aggregate (0x260c), which normal tables allow. Only the percentile
        value is compared, since tbname differs between vc0 and org0.
        Currently xfails on that known resolver limitation
        (see VTABLE_TBNAME_AGG_XFAIL).

        Since: v3.4.3.0
        """
        # tbname differs (vc0 vs org0); compare only the percentile value
        a = self._rows(f"select tbname, percentile(c_int,50) from {DB}.vc0;")[0][1]
        b = self._rows(f"select tbname, percentile(c_int,50) from {DB}.org0;")[0][1]
        self._assert_close(a, b, "group by tbname percentile value")

    # ---- WHERE / ORDER / LIMIT -------------------------------------------
    def test_where_filter(self):
        """A column-value WHERE keeps the VirtualScan and still matches the origin.

        Filtering on c_int/c_double values exercises the virtual-side two-pass
        (twoPassMode) path rather than the collapsed single-table scan.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50), percentile(c_double,50) from {T} "
                  "where c_int > 10000 and c_int < 40000 and c_double > 5000;", "where filter")

    def test_time_range_filter(self):
        """A timestamp time-range WHERE matches the origin.

        Restricts the scan to one hour of data so both passes run on the same
        bounded range.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50) from {T} "
                  "where ts >= '2020-01-01 01:00:00' and ts < '2020-01-01 02:00:00';",
                  "time range")

    def test_order_by_limit(self):
        """ORDER BY the percentile result with LIMIT matches the origin.

        Orders by percentile descending and keeps one row; both sides must pick
        the same extreme value.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50) from {T} order by percentile(c_int,50) desc limit 1;",
                  "order by + limit")

    # ---- scalar wrapping + subquery --------------------------------------
    def test_scalar_wrap_and_subquery(self):
        """percentile wrapped in a scalar function and inside a subquery.

        round(percentile(...)) on top of the two-pass result, and the whole
        aggregation placed in a subquery, must both match the origin.

        Since: v3.4.3.0
        """
        self._cmp("select round(percentile(c_int,50),1), round(percentile(c_double,50),1) from {T};",
                  "scalar wrap")
        self._cmp("select * from (select percentile(c_int,50) p50, percentile(c_double,50) pd "
                  "from {T});", "subquery")

    # ---- time-series windows (interval / state / session) ----------------
    def test_interval_window(self):
        """percentile inside an interval window matches the origin.

        Per-hour p50 on two columns via interval(1h); the whole windowed result
        set must equal the origin's.

        Since: v3.4.3.0
        """
        self._cmp("select _wstart, percentile(c_int,50), percentile(c_double,50) from {T} "
                  "interval(1h) order by _wstart;", "interval window")

    @pytest.mark.xfail(reason=VTABLE_STATE_WINDOW_EXPR_XFAIL, strict=True)
    def test_state_window(self):
        """percentile inside a state window on an expression.

        state_window(c_int%10) over the virtual table; currently xfails on a
        known vtable parse limitation (see VTABLE_STATE_WINDOW_EXPR_XFAIL).

        Since: v3.4.3.0
        """
        self._cmp("select _wstart, percentile(c_int,50) from {T} state_window(c_int%10) "
                  "order by _wstart limit 20;", "state window")

    def test_session_window(self):
        """percentile inside a session window matches the origin.

        session(ts, 30s) with the first 20 windows compared; the virtual table
        must produce the same windowing as the origin.

        Since: v3.4.3.0
        """
        self._cmp("select _wstart, percentile(c_int,50) from {T} session(ts, 30s) "
                  "order by _wstart limit 20;", "session window")

    # ---- window/group combined with a column WHERE -------------------------
    # A column-value WHERE keeps the VirtualScan from being eliminated, so these
    # exercise the virtual-side two-pass (twoPassMode) through WINDOW / AGG-group
    # nodes, not the plain single-table path.
    def test_interval_window_with_where(self):
        """interval window plus a column WHERE forces the virtual two-pass path.

        With a column-value WHERE the VirtualScan survives elimination, so the
        window runs on top of twoPassMode rather than the collapsed scan.

        Since: v3.4.3.0
        """
        self._cmp("select _wstart, percentile(c_int,50), percentile(c_double,50) from {T} "
                  "where c_int > 10000 and c_int < 40000 interval(1h) order by _wstart;",
                  "interval window + where")

    def test_session_window_with_where(self):
        """session window plus a column WHERE forces the virtual two-pass path.

        Same shape as test_interval_window_with_where but with session(ts, 30s)
        on top of the surviving VirtualScan.

        Since: v3.4.3.0
        """
        self._cmp("select _wstart, percentile(c_int,50) from {T} "
                  "where c_int > 10000 and c_int < 40000 session(ts, 30s) "
                  "order by _wstart limit 20;", "session window + where")

    def test_group_by_with_where(self):
        """GROUP BY plus a column WHERE forces the virtual two-pass path.

        Groups by c_int/5000 over the filtered virtual scan; the grouped
        percentile result set must equal the origin's.

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50) from {T} "
                  "where c_int > 10000 and c_int < 40000 group by c_int/5000 order by 1;",
                  "group by + where")

    def test_empty_result_two_pass(self):
        """An empty phase-1 (WHERE matches nothing) still matches the origin.

        c_int > 100000 selects nothing (max is ROWS-1), so phase-1 produces no
        rows; the restart and the empty phase-2 must behave exactly like the
        origin's collapsed scan (same result shape, same NULL/empty outcome).

        Since: v3.4.3.0
        """
        self._cmp("select percentile(c_int,50), percentile(c_double,50) from {T} "
                  "where c_int > 100000;", "empty phase-1 two-pass")

    # ---- exact vs approximate cross-check --------------------------------
    def test_percentile_close_to_apercentile(self):
        """Exact percentile stays close to apercentile on the virtual table.

        For every column, p50 (exact) must lie within 2% of the value range of
        apercentile p50 — a sanity cross-check between the two algorithms.

        Since: v3.4.3.0
        """
        for col in NUMERIC_COLS:
            exact = float(self._scalar(f"select percentile({col},50) from {DB}.vc0;"))
            approx = float(self._scalar(f"select apercentile({col},50) from {DB}.vc0;"))
            lo = float(self._scalar(f"select percentile({col},0) from {DB}.vc0;"))
            hi = float(self._scalar(f"select percentile({col},100) from {DB}.vc0;"))
            tol = max(1.0, (hi - lo) * 0.02)
            assert abs(exact - approx) <= tol, f"{col}: |{exact}-{approx}| > {tol}"

    # ---- multi-branch: every percentile branch must get the two-pass -------
    # The VtableTwoPass rule converts one branch per optimizer round; a UNION ALL
    # with two percentile-over-vtable branches must convert BOTH (regression guard:
    # an unconverted branch silently loses its PRE_SCAN pass -> wrong percentile).
    def test_union_all_two_branches(self):
        """A UNION ALL of two percentile-over-vtable branches converts both.

        The VtableTwoPass rule converts one branch per optimizer round; this
        guards against an unconverted branch silently losing its PRE_SCAN pass
        and returning a wrong percentile.

        Since: v3.4.3.0
        """
        a = self._scalar(f"select percentile(c_int,50) from {DB}.vc0 where c_int > 10000 "
                         f"and c_int < 40000;")
        b = self._scalar(f"select percentile(c_int,50) from {DB}.vc1 where c_int > 51000 "
                         f"and c_int < 81000;")
        rows = self._rows(f"select percentile(c_int,50) from {DB}.vc0 where c_int > 10000 "
                          f"and c_int < 40000 "
                          f"union all "
                          f"select percentile(c_int,50) from {DB}.vc1 where c_int > 51000 "
                          f"and c_int < 81000;")
        # UNION ALL row order is not guaranteed without ORDER BY — compare as a set,
        # with the same float tolerance as _cmp/_assert_close (union branches vs the
        # standalone scalar queries are different execution paths).
        got, exp = sorted(rows), sorted([(a,), (b,)])
        assert len(got) == len(exp), f"union all branches: {rows} != {[(a,), (b,)]}"
        for (ra,), (rb,) in zip(got, exp):
            self._assert_close(ra, rb, "union all branch percentile")

    # ---- FILL on top of the two-pass driver -------------------------------
    def test_interval_fill_with_where(self):
        """FILL(prev) over the two-pass driver matches the origin.

        A bounded time range (FILL requires one) plus a column WHERE, so the
        fill runs above the surviving VirtualScan in two-pass mode.

        Since: v3.4.3.0
        """
        # FILL requires a bounded query time range (0x263B otherwise)
        self._cmp("select _wstart, percentile(c_int,50) from {T} "
                  "where ts >= '2020-01-01 01:00:00' and ts <= '2020-01-01 12:00:00' "
                  "and c_int > 10000 and c_int < 40000 interval(1h) fill(prev) order by _wstart;",
                  "interval + fill + where")

    # ---- features percentile must REJECT ---------------------------------
    def test_partition_by_rejected(self):
        """PARTITION BY above percentile on a virtual table is rejected.

        The unsupported plan shape fails at plan time with the window-function
        error code rather than silently returning a wrong percentile.

        Since: v3.4.3.0
        """
        tdSql.error(f"select percentile(c_int,50) from {DB}.vc0 partition by tbname;",
                    expectedErrno=TSDB_CODE_PAR_NOT_ALLOWED_FUNC)

    def test_event_window_rejected(self):
        """EVENT WINDOW with percentile on a virtual table is rejected.

        The window-function check refuses percentile inside an event window
        before the single-table check is reached.

        Since: v3.4.3.0
        """
        tdSql.error(f"select _wstart, percentile(c_int,50) from {DB}.vc0 "
                    f"event_window start with c_int=0 end with c_int!=0;",
                    expectedErrno=TSDB_CODE_PAR_NOT_ALLOWED_FUNC)

    def test_count_window_rejected(self):
        """COUNT WINDOW with percentile on a virtual table is rejected.

        The window-function check refuses percentile inside a count window
        before the single-table check is reached.

        Since: v3.4.3.0
        """
        tdSql.error(f"select _wstart, percentile(c_int,50) from {DB}.vc0 count_window(100);",
                    expectedErrno=TSDB_CODE_PAR_NOT_ALLOWED_FUNC)

    def test_virtual_super_table_rejected(self):
        """percentile on a virtual super table is still rejected.

        Virtual super tables are multi-source and stay outside the two-pass
        support; the query fails with the single-table error code.

        Since: v3.4.3.0
        """
        tdSql.error(f"select percentile(c_int,50) from {DB}.vstb;",
                    expectedErrno=TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE)

    def test_tag_ref_two_pass_rejected(self):
        """percentile referencing a ref-tag column is rejected at plan time.

        vcref's tg tag is a tag reference (TAGS (tg FROM tagsrc0.tref)), so
        grouping by / projecting tg gives the VirtualScan a TAG_REF_SOURCE
        child. The two-pass restart cannot re-fetch its 1-row tag block (the
        remote tag-ref task is not kept alive past phase-1 EOF), so the
        VtableTwoPass rule rejects the plan with 0x800026E8 instead of letting
        it fail after a wasted phase-1 pass.

        Since: v3.4.3.0
        """
        tdSql.error(f"select tg, percentile(c_int,50) from {DB}.vcref "
                    f"where c_int > 10000 and c_int < 40000 group by tg;",
                    expectedErrno=TSDB_CODE_PAR_NOT_SUPPORT_VTABLE_TWO_PASS)

    # ---- queryPolicy=4: client-local execution of the two-pass plan --------
    # Under QUERY_POLICY_CLIENT the aggregate/exchange subplan runs inside the
    # client's embedded qworker while the origin TableScan subplan (a SCAN-type
    # data-bind task) stays on the vnode; the exchange fetches it over RPC, so
    # the phase-2 rescan directive (DYN_TYPE_TWO_PASS_RESCAN) is delivered the
    # same way as under the default policy. These cases pin that contract: the
    # two-pass result must be identical to the server-executed one.
    def _run_cli_query_policy4(self, sqls, assert_local_exec=False):
        """Run sqls through the taos CLI with a client config forcing queryPolicy=4.

        The in-process connector session cannot switch queryPolicy (it is a
        client-scope cfg), so the CLI runs as a subprocess with its own minimal
        cfg directory. Returns the raw CLI stdout. With assert_local_exec the
        client debug log must additionally show the aggregation task launched
        LOCAL and the origin scan task launched REMOTE — result equality alone
        cannot distinguish real local execution from a silently ignored cfg
        (both return the same numbers).
        """
        cli = tdDnodes.dnodes[0].getPath("taos")
        assert os.path.isfile(cli) and os.access(cli, os.X_OK), f"taos CLI not found: {cli}"
        with tempfile.TemporaryDirectory(prefix="taos_qp4_") as cfg_dir:
            log_dir = os.path.join(cfg_dir, "log")
            os.makedirs(log_dir)
            with open(os.path.join(cfg_dir, "taos.cfg"), "w") as f:
                f.write("queryPolicy 4\nlogDir %s\ndebugFlag 137\n" % log_dir)
            sql_file = os.path.join(cfg_dir, "run.sql")
            with open(sql_file, "w") as f:
                f.write("\n".join(s.rstrip(";") + ";" for s in sqls) + "\n")
            r = subprocess.run([cli, "-c", cfg_dir, "-h", "localhost", "-f", sql_file],
                               capture_output=True, text=True, timeout=180)
            log = ""
            if assert_local_exec:
                for name in sorted(os.listdir(log_dir)):
                    if name.startswith("taoslog"):
                        with open(os.path.join(log_dir, name), errors="replace") as f:
                            log += f.read()
        assert r.returncode == 0, f"taos CLI failed: rc={r.returncode}\n{r.stdout}\n{r.stderr}"
        # The CLI exits 0 even when a statement fails, and a failed statement
        # renders no data rows -- without these checks a broken query could
        # pass vacuously (an "empty result" assertion cannot tell a genuine
        # 0-row result from an errored statement). Require every statement to
        # report success and none to report an error.
        n_ok = r.stdout.count("Query OK")
        assert n_ok == len(sqls), \
            f"expected {len(sqls)} succeeded statements under queryPolicy=4, saw {n_ok} status lines:\n{r.stdout}"
        assert "DB error" not in r.stdout, f"statement failed under queryPolicy=4:\n{r.stdout}"
        if assert_local_exec:
            assert "start to launch LOCAL task" in log and "start to launch REMOTE task" in log, \
                "queryPolicy=4 did not engage client-local execution (missing LOCAL/REMOTE " \
                "task markers in the client debug log); the two-pass result checks below " \
                "would pass vacuously if the policy were ignored"
        return r.stdout

    @staticmethod
    def _cli_rows(stdout):
        """Extract result rows (as string cells) from taos CLI output.

        Only data lines are kept: every cell must parse as a number, be NULL,
        or look like a timestamp, which filters out column headers, separators,
        EXPLAIN plan rows and status lines. Returns one list-of-cells per data
        line.
        """
        def _cell_ok(c):
            if c == "NULL":
                return True
            try:
                float(c)
                return True
            except ValueError:
                return re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", c) is not None

        rows = []
        for line in stdout.splitlines():
            if "|" not in line:
                continue
            cells = [c.strip() for c in line.split("|")[:-1]]
            if cells and all(c and _cell_ok(c) for c in cells):
                rows.append(cells)
        return rows

    def _assert_cli_rows_close(self, got_rows, exp_rows, msg):
        """Compare two _cli_rows result sets with the suite's float tolerance.

        Numeric cells use math.isclose; timestamp/NULL cells compare as
        strings (both sides render identical formats).
        """
        assert got_rows and len(got_rows) == len(exp_rows), \
            f"{msg}: row count {len(got_rows)} != {len(exp_rows)}\n{got_rows}\nvs\n{exp_rows}"
        for va, vb in zip(got_rows, exp_rows):
            for a, b in zip(va, vb):
                if a == b:
                    continue
                try:
                    assert math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9), \
                        f"{msg}: cell {a!r} != {b!r}\n{va}\nvs\n{vb}"
                except ValueError:
                    assert False, f"{msg}: cell {a!r} != {b!r}\n{va}\nvs\n{vb}"

    def test_local_exec_two_pass_where(self):
        """queryPolicy=4 runs the two-pass where-filter plan locally, result unchanged.

        A column-value WHERE keeps the VirtualScan (twoPassMode), so the client
        executes Aggregate -> VirtualScan -> Exchange locally and drives the
        origin rescan over RPC. The percentile must equal the origin's, and the
        client debug log must prove local execution actually engaged.

        Since: v3.4.3.0
        """
        out = self._run_cli_query_policy4([
            f"explain select percentile(c_int,50) from {DB}.vc0 "
            f"where c_int > 10000 and c_int < 40000 and c_double > 500;",
            f"select percentile(c_int,50) from {DB}.vc0 "
            f"where c_int > 10000 and c_int < 40000 and c_double > 500;",
        ], assert_local_exec=True)
        assert "Data Exchange" in out, f"two-pass plan shape missing under queryPolicy=4:\n{out}"
        rows = self._cli_rows(out)
        assert rows, f"no data rows parsed from CLI output:\n{out}"
        got = rows[-1][-1]
        exp = self._scalar(f"select percentile(c_int,50) from {DB}.org0 "
                           f"where c_int > 10000 and c_int < 40000 and c_double > 500;")
        assert got is not None and math.isclose(float(got), float(exp), rel_tol=1e-9, abs_tol=1e-9), \
            f"localExec p50 {got} != origin {exp}"

    def test_local_exec_task_placement(self):
        """Under queryPolicy=4 the aggregation task is LOCAL, the origin scan REMOTE.

        Pins the exact task split that makes the two-pass phase-2 directive
        deliverable: the exchange runs in the client's embedded qworker (LOCAL)
        while the origin TableScan runs on the vnode (REMOTE) and is fetched
        over RPC. If a scheduling change ever placed the origin task locally,
        the phase-2 rescan fetch would lose its param channel -- this fails
        before any wrong result can be produced.

        Since: v3.4.3.0
        """
        out = self._run_cli_query_policy4([
            f"select percentile(c_int,50), percentile(c_double,75) from {DB}.vc0 "
            f"where c_int > 10000 and c_int < 40000 and c_double > 500;",
        ], assert_local_exec=True)
        rows = self._cli_rows(out)
        exp_int = self._scalar(f"select percentile(c_int,50) from {DB}.org0 "
                               f"where c_int > 10000 and c_int < 40000 and c_double > 500;")
        exp_dbl = self._scalar(f"select percentile(c_double,75) from {DB}.org0 "
                               f"where c_int > 10000 and c_int < 40000 and c_double > 500;")
        assert len(rows) == 1 and len(rows[0]) == 2, f"expected 1x2, got {rows}"
        assert math.isclose(float(rows[0][0]), float(exp_int), rel_tol=1e-9, abs_tol=1e-9)
        assert math.isclose(float(rows[0][1]), float(exp_dbl), rel_tol=1e-9, abs_tol=1e-9)

    def test_local_exec_window_with_where(self):
        """queryPolicy=4: interval window over the surviving VirtualScan.

        The WINDOW node drives one two-pass cycle per window; every window's
        p50 under client-local execution must equal the origin's.

        Since: v3.4.3.0
        """
        tmpl = ("select _wstart, percentile(c_int,50) from {T} "
                "where c_int > 10000 and c_int < 40000 interval(1h) order by _wstart;")
        vc = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.vc0")]))
        org = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.org0")]))
        self._assert_cli_rows_close(vc, org, "localExec interval window")

    def test_local_exec_union_all_two_branches(self):
        """queryPolicy=4: UNION ALL converts both percentile branches.

        The VtableTwoPass rule converts one UNION ALL branch per optimizer
        round; under client-local execution both branch percentiles must still
        match their standalone (server-executed) values.

        Since: v3.4.3.0
        """
        a = self._scalar(f"select percentile(c_int,50) from {DB}.vc0 where c_int > 10000 "
                         f"and c_int < 40000;")
        b = self._scalar(f"select percentile(c_int,50) from {DB}.vc1 where c_int > 51000 "
                         f"and c_int < 81000;")
        out = self._run_cli_query_policy4([
            f"select percentile(c_int,50) from {DB}.vc0 where c_int > 10000 and c_int < 40000 "
            f"union all "
            f"select percentile(c_int,50) from {DB}.vc1 where c_int > 51000 and c_int < 81000;"])
        got = sorted(float(r[0]) for r in self._cli_rows(out))
        exp = sorted(float(v) for v in (a, b))
        assert len(got) == 2 and len(exp) == 2, f"expected 2 branches, got {got}"
        for ga, gb in zip(got, exp):
            assert math.isclose(ga, gb, rel_tol=1e-9, abs_tol=1e-9), f"{got} != {exp}"

    def test_local_exec_vc1_and_mixed_aggs(self):
        """queryPolicy=4: second virtual child and mixed aggregate select lists.

        vc1 points at org1 (different vgroup and time base) so its two-pass
        runs against another origin; a mixed select (percentile + avg/count/
        min/max) and several percentiles in one statement must all match the
        origin under client-local execution.

        Since: v3.4.3.0
        """
        got = self._cli_rows(self._run_cli_query_policy4(
            [f"select percentile(c_int,50) from {DB}.vc1 where c_int > 51000 and c_int < 81000;"]))
        exp = self._scalar(f"select percentile(c_int,50) from {DB}.org1 "
                           f"where c_int > 51000 and c_int < 81000;")
        assert len(got) == 1 and len(got[0]) == 1, f"expected 1x1 from vc1, got {got}"
        assert math.isclose(float(got[0][0]), float(exp), rel_tol=1e-9, abs_tol=1e-9)

        tmpl = ("select percentile(c_int,50), percentile(c_double,50), avg(c_int), count(*), "
                "min(c_int), max(c_int), sum(c_int) from {T} "
                "where c_int > 10000 and c_int < 40000;")
        vc = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.vc0")]))
        org = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.org0")]))
        self._assert_cli_rows_close(vc, org, "localExec mixed aggregates")

    def test_local_exec_two_pass_group_and_empty(self):
        """queryPolicy=4: grouped two-pass and empty-result two-pass both match.

        GROUP BY over the surviving VirtualScan runs one two-pass cycle per
        group in the client; a WHERE matching nothing must produce no rows
        (empty phase-1/phase-2, no spurious NULL or error).

        Since: v3.4.3.0
        """
        tmpl = ("select percentile(c_int,50), percentile(c_double,50) from {T} "
                "where c_int > 10000 and c_int < 40000 group by c_int/5000 order by 1;")
        vc_rows = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.vc0")]))
        org_rows = self._cli_rows(self._run_cli_query_policy4([tmpl.format(T=f"{DB}.org0")]))
        self._assert_cli_rows_close(vc_rows, org_rows, "localExec group")

        out = self._run_cli_query_policy4([
            f"select percentile(c_int,50) from {DB}.vc0 where c_int > {ROWS * 100};"])
        # "0 row(s)" pins a genuinely empty result (runner already rejects an
        # errored statement) instead of any output without data rows.
        assert re.search(r"Query OK,\s*0\s*row", out) and not self._cli_rows(out), \
            f"expected empty result under queryPolicy=4:\n{out}"

    def test_local_exec_collapsed_and_normal(self):
        """queryPolicy=4 on the collapsed single-table path and virtual normal table.

        Without a column WHERE the VirtualScan is eliminated and the whole
        two-pass runs inside one origin subplan; vntb (virtual normal) keeps a
        vtable resolver but no exchange split. Both must still match the origin.

        Since: v3.4.3.0
        """
        out = self._run_cli_query_policy4([
            f"select percentile(c_int,50) from {DB}.vc0;",
            f"select percentile(c_int,50) from {DB}.vntb where c_int > 10000 and c_int < 40000;",
        ])
        rows = self._cli_rows(out)
        exp_plain = self._scalar(f"select percentile(c_int,50) from {DB}.org0;")
        exp_where = self._scalar(f"select percentile(c_int,50) from {DB}.org0 "
                                 f"where c_int > 10000 and c_int < 40000;")
        assert len(rows) == 2 and len(rows[0]) == 1 and len(rows[1]) == 1, \
            f"expected two 1x1 results, got {rows}"
        assert math.isclose(float(rows[0][0]), float(exp_plain), rel_tol=1e-9, abs_tol=1e-9), \
            f"collapsed p50 {rows[0][0]} != {exp_plain}"
        assert math.isclose(float(rows[1][0]), float(exp_where), rel_tol=1e-9, abs_tol=1e-9), \
            f"vntb p50 {rows[1][0]} != {exp_where}"

    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {DB};")
