# Regression test for the scatter position mismatch bug in multi-table
# fan-out vtable ref resolve (fix: scatterOrder vs indexList).
#
# Bug: when a vtable's column list interleaves references to two or more
# physical tables routed to the same remote vnode (e.g. vtable columns
# [t1.v1, t2.v1, t1.v2]), streamPrepareAndFireOneVgResolve groups them
# into contiguous per-table groups (t1{v1,v2}, t2{v1}), but
# streamScatterOneVgResolve was using indexList (vtable definition order)
# instead of scatterOrder (group-flatten order) to write response items
# back, causing silent value swap between columns.
#
# NOTE on vgroup topology:
#   This bug only triggers in streamScatterOneVgResolve, which is only
#   reached for REMOTE vgroups (vgId != TD_VID(pVnode)).  Local tables
#   take the streamBatchExecuteLocalVg path and are unaffected.
#   Therefore the test puts each source table in its own database (each
#   with vgroups=1), separate from the vtable's database.  This
#   guarantees the source tables are always on a remote vnode relative
#   to the vtable, with no retry logic needed.
#
# Case TC_2TABLE: columns [phys_a.col_x, phys_b.col_x, phys_a.col_y]
#   phys_a in db_a (vgroups=1), phys_b in db_b (vgroups=1)
#   vtable in db_vt (vgroups=1)
#   Groups after re-grouping: phys_a{col_x,col_y}, phys_b{col_x}
#   Server response order: col_x(10), col_y(20), col_x(30)
#   Correct scatter: vcol_ax=10, vcol_bx=30, vcol_ay=20
#   Bug (swap):      vcol_ax=10, vcol_bx=20, vcol_ay=30
#
# Case TC_3TABLE: columns [p1.a, p2.a, p3.a, p1.b, p2.b, p3.b]
#   p1/p2/p3 each in their own db (vgroups=1), vtable in db_vt (vgroups=1)
#   Groups: p1{a,b}, p2{a,b}, p3{a,b}
#   Server response: p1.a, p1.b, p2.a, p2.b, p3.a, p3.b
#   scatterOrder positions: [0, 3, 1, 4, 2, 5]
#   Correct: v_p1a=1, v_p2a=3, v_p3a=5, v_p1b=2, v_p2b=4, v_p3b=6

import time
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableInterleavedColScatter:
    """Stream vtable ref resolve scatter position correctness.

    Covers the bug where vtable columns interleave references across multiple
    physical tables on the same remote vnode, causing silent column value swap
    in the scatter phase (indexList vs scatterOrder mismatch).

    Catalog:
        - Streams:VirtualTable

    Since: v3.3.6.0

    Labels: common,ci

    History:
        - 2026-06-11 Created for scatter position mismatch fix regression.
    """

    precision = "ms"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vtable_interleaved_col_scatter(self):
        """Vtable ref resolve scatter must not swap column values when vtable
        columns interleave references across multiple physical tables routed
        to the same remote vnode."""
        tdSql.query("alter all dnodes 'debugflag 131'")
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()
        self._tc_2table()
        self._tc_3table()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _waitRows(self, sql, expected, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query(sql, queryTimes=1)
                if tdSql.getRows() >= expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"timeout waiting for {expected} rows: {sql}")

    def _checkData(self, label, sql, expected):
        """Assert query result equals expected list-of-tuples."""
        self._waitRows(sql, len(expected))
        tdSql.query(sql)
        actual = [
            tuple(tdSql.getData(r, c) for c in range(len(expected[0])))
            for r in range(tdSql.getRows())
        ]
        if actual != expected:
            tdLog.exit(
                f"{label}: mismatch\n  expected: {expected}\n  actual:   {actual}"
            )

    # ------------------------------------------------------------------
    # TC_2TABLE
    # phys_a in db_a, phys_b in db_b, vtable in db_vt (all vgroups=1).
    # Vtable column order: [phys_a.col_x, phys_b.col_x, phys_a.col_y]
    # Both source tables are remote to db_vt's vnode; they happen to be
    # routed to different vnodes, so each gets its own RPC handle.
    # Within the phys_a handle: only one table -> no interleaving issue.
    # To get both tables in one handle (triggering the interleave path),
    # put phys_a and phys_b in the SAME source db (db_src, vgroups=1).
    # ------------------------------------------------------------------
    def _tc_2table(self):
        tdLog.info("TC_2TABLE: interleaved cols from 2 tables on same remote vnode")

        # All source tables in one db (vgroups=1) so they share the same
        # remote vnode.  vtable in a separate db (vgroups=1).
        tdSql.execute("create database sc2t_src vgroups 1 buffer 8 "
                      f"precision '{TestStreamVtableInterleavedColScatter.precision}'")
        tdSql.execute("create database sc2t_vt  vgroups 1 buffer 8 "
                      f"precision '{TestStreamVtableInterleavedColScatter.precision}'")

        tdSql.execute("create table sc2t_src.phys_a (ts timestamp, col_x int, col_y int)")
        tdSql.execute("create table sc2t_src.phys_b (ts timestamp, col_x int)")

        tdSql.execute(
            "create stable sc2t_vt.vstb ("
            "  ts timestamp, vcol_ax int, vcol_bx int, vcol_ay int"
            ") tags (tid int) virtual 1"
        )
        # Vtable column order: [phys_a.col_x, phys_b.col_x, phys_a.col_y]
        # Both phys_a and phys_b are on sc2t_src's vnode (remote to sc2t_vt).
        # One RPC handle covers both tables; grouping produces
        # phys_a{col_x,col_y} then phys_b{col_x}, interleaving the original
        # definition order and triggering the scatter bug path.
        tdSql.execute(
            "create vtable sc2t_vt.vct ("
            "  vcol_ax from sc2t_src.phys_a.col_x, "
            "  vcol_bx from sc2t_src.phys_b.col_x, "
            "  vcol_ay from sc2t_src.phys_a.col_y"
            ") using sc2t_vt.vstb tags(1)"
        )

        tdSql.execute(
            "create stream sc2t_vt.s_2t "
            "count_window(1) "
            "from sc2t_vt.vct "
            "into sc2t_vt.res_2t (firstts, lastts, sum_ax, sum_bx, sum_ay) "
            "as select first(_c0), last_row(_c0), "
            "  sum(vcol_ax), sum(vcol_bx), sum(vcol_ay) "
            "from sc2t_vt.vct;"
        )
        tdStream.checkStreamStatus("s_2t")

        # Insert: phys_a.col_x=10, phys_a.col_y=20, phys_b.col_x=30
        # Correct scatter: sum_ax=10, sum_bx=30, sum_ay=20
        # Bug (swap):      sum_ax=10, sum_bx=20, sum_ay=30
        tdSql.execute("insert into sc2t_src.phys_a values ('2025-01-01 00:00:00', 10, 20);")
        tdSql.execute("insert into sc2t_src.phys_b values ('2025-01-01 00:00:00', 30);")

        # Second batch: same values; each count_window(1) row doubles.
        tdSql.execute("insert into sc2t_src.phys_a values ('2025-01-01 00:00:05', 10, 20);")
        tdSql.execute("insert into sc2t_src.phys_b values ('2025-01-01 00:00:05', 30);")
        self._checkData(
            "TC_2TABLE-2",
            "select sum_ax, sum_bx, sum_ay from sc2t_vt.res_2t order by firstts",
            [(20, 60, 40)],
        )

    # ------------------------------------------------------------------
    # TC_3TABLE
    # p1/p2/p3 all in sc3t_src (vgroups=1), vtable in sc3t_vt (vgroups=1).
    # Vtable column order: [p1.a, p2.a, p3.a, p1.b, p2.b, p3.b]
    # One RPC handle for sc3t_src's vnode covers all three tables.
    # Groups: p1{a,b}, p2{a,b}, p3{a,b}
    # Server response: p1.a, p1.b, p2.a, p2.b, p3.a, p3.b
    # scatterOrder maps rsp positions [0,1,2,3,4,5] back to vtable slots
    # [0,3,1,4,2,5]: v_p1a, v_p1b, v_p2a, v_p2b, v_p3a, v_p3b.
    # Correct: v_p1a=1, v_p2a=3, v_p3a=5, v_p1b=2, v_p2b=4, v_p3b=6
    # ------------------------------------------------------------------
    def _tc_3table(self):
        tdLog.info("TC_3TABLE: interleaved cols from 3 tables on same remote vnode")

        tdSql.execute("create database sc3t_src vgroups 1 buffer 8 "
                      f"precision '{TestStreamVtableInterleavedColScatter.precision}'")
        tdSql.execute("create database sc3t_vt  vgroups 1 buffer 8 "
                      f"precision '{TestStreamVtableInterleavedColScatter.precision}'")

        tdSql.execute("create table sc3t_src.p1 (ts timestamp, a int, b int)")
        tdSql.execute("create table sc3t_src.p2 (ts timestamp, a int, b int)")
        tdSql.execute("create table sc3t_src.p3 (ts timestamp, a int, b int)")

        tdSql.execute(
            "create stable sc3t_vt.vstb ("
            "  ts timestamp, "
            "  v_p1a int, v_p2a int, v_p3a int, "
            "  v_p1b int, v_p2b int, v_p3b int"
            ") tags (tid int) virtual 1"
        )
        # Column order deliberately interleaves all three tables:
        # [p1.a, p2.a, p3.a, p1.b, p2.b, p3.b]
        # After grouping: p1{a,b}, p2{a,b}, p3{a,b}
        # Server response: p1.a, p1.b, p2.a, p2.b, p3.a, p3.b
        # scatterOrder must map: rsp[0]->slot0, rsp[1]->slot3,
        #   rsp[2]->slot1, rsp[3]->slot4, rsp[4]->slot2, rsp[5]->slot5
        tdSql.execute(
            "create vtable sc3t_vt.vct ("
            "  v_p1a from sc3t_src.p1.a, "
            "  v_p2a from sc3t_src.p2.a, "
            "  v_p3a from sc3t_src.p3.a, "
            "  v_p1b from sc3t_src.p1.b, "
            "  v_p2b from sc3t_src.p2.b, "
            "  v_p3b from sc3t_src.p3.b"
            ") using sc3t_vt.vstb tags(1)"
        )

        tdSql.execute(
            "create stream sc3t_vt.s_3t "
            "count_window(1) "
            "from sc3t_vt.vct "
            "into sc3t_vt.res_3t ("
            "  firstts, lastts, "
            "  s_p1a, s_p2a, s_p3a, s_p1b, s_p2b, s_p3b) "
            "as select first(_c0), last_row(_c0), "
            "  sum(v_p1a), sum(v_p2a), sum(v_p3a), "
            "  sum(v_p1b), sum(v_p2b), sum(v_p3b) "
            "from sc3t_vt.vct;"
        )
        tdStream.checkStreamStatus("s_3t")

        # Insert: p1(a=1,b=2), p2(a=3,b=4), p3(a=5,b=6)
        # Correct: s_p1a=1, s_p2a=3, s_p3a=5, s_p1b=2, s_p2b=4, s_p3b=6
        # Any scatter swap produces at least one wrong value.
        tdSql.execute("insert into sc3t_src.p1 values ('2025-01-01 00:00:00', 1, 2);")
        tdSql.execute("insert into sc3t_src.p2 values ('2025-01-01 00:00:00', 3, 4);")
        tdSql.execute("insert into sc3t_src.p3 values ('2025-01-01 00:00:00', 5, 6);")

        # Second batch: same values.
        tdSql.execute("insert into sc3t_src.p1 values ('2025-01-01 00:00:10', 1, 2);")
        tdSql.execute("insert into sc3t_src.p2 values ('2025-01-01 00:00:10', 3, 4);")
        tdSql.execute("insert into sc3t_src.p3 values ('2025-01-01 00:00:10', 5, 6);")
        self._checkData(
            "TC_3TABLE-2",
            "select s_p1a, s_p2a, s_p3a, s_p1b, s_p2b, s_p3b "
            "from sc3t_vt.res_3t order by firstts",
            [(2, 6, 10, 4, 8, 12)],
        )
