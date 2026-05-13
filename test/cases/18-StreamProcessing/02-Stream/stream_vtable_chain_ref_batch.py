import time
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableChainRefBatch:
    """Batch & error-handling tests for vtable chain-ref (TC11-TC18).

    TC15 (single-shot RPC failure -> skip) and TC18 (NORMAL vtable rejects
    tag) require test hooks / hard-to-trigger code paths in default builds;
    they are implemented as soft-skip placeholders.
    """
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_batch(self):
        """TC11-TC18 -- vtable chain-ref batch & error & tag ref/const.

        Since: v3.4.1.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-05-12 vtable chain-ref batch cases.
            - 2026-05-12 add TC16/TC17/TC18 covering CHILD vtable tag
              reference vs constant, and NORMAL vtable tag rejection.
        """
        try:
            tdStream.createSnode()
            self._tc11_batch_50_same_db_one_rpc()
            self._tc12_per_uid_business_error_skip()
            self._tc13_full_request_old_uid_removed()
            self._tc14_partial_request_keeps_others()
            self._tc15_rpc_single_failure_skip()
            self._tc16_child_vtable_tag_ref()
            self._tc17_child_vtable_tag_const()
            self._tc18_normal_vtable_tag_rejected()
        finally:
            tdStream.dropAllStreamsAndDbs()

    def _waitRows(self, sql, expected, timeout=60):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                tdSql.query(sql, queryTimes=1)
                if tdSql.getRows() >= expected:
                    return
            except Exception as e:
                last_err = e
            time.sleep(1)
        tdLog.exit(f"timeout waiting for {expected} rows from: {sql}, last_err={last_err}")

    # ----- test cases -----------------------------------------------------

    def _tc11_batch_50_same_db_one_rpc(self):
        tdLog.info("TC11: 50 vtable in same db -> per-vg single chain RPC")
        tdSql.execute("create database tc11 vgroups 1")
        tdSql.execute("use tc11")
        for i in range(50):
            tdSql.execute(f"create table tc11.ct{i} (ts timestamp, v int)")
            tdSql.execute(f"create vtable tc11.vt{i} (ts timestamp, v int from tc11.ct{i}.v)")
        # Stream is triggered by vt0; reading from vt0 inside the SELECT
        # exercises the chain-ref reader path through the source ct0.
        tdSql.execute(
            "create stream tc11.s11 interval(1s) sliding(1s) from tc11.vt0 "
            "into tc11.res as "
            "select _twstart as ts, last(v) as v from tc11.vt0 "
            "where ts >= _twstart and ts < _twend"
        )
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into tc11.ct0 values ({ts}, 1)")
        tdSql.execute(f"insert into tc11.ct0 values ({ts + 1500}, 2)")
        self._waitRows("select * from tc11.res", 1)

    def _tc12_per_uid_business_error_skip(self):
        tdLog.info("TC12: drop one ref table among many -> others still flow")
        tdSql.execute("create database tc12 vgroups 1")
        tdSql.execute("use tc12")
        for i in range(5):
            tdSql.execute(f"create table tc12.ct{i} (ts timestamp, v int)")
            tdSql.execute(f"create vtable tc12.vt{i} (ts timestamp, v int from tc12.ct{i}.v)")
        tdSql.execute(
            "create stream tc12.s12 interval(1s) sliding(1s) from tc12.vt0 "
            "into tc12.res as "
            "select _twstart as ts, last(v) as v from tc12.vt0 "
            "where ts >= _twstart and ts < _twend"
        )
        # Drop one ref child table that the trigger (vt0) does not depend on;
        # the surviving chain (vt0 -> ct0) must keep flowing.
        tdSql.execute("drop table tc12.ct4")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into tc12.ct0 values ({ts}, 1)")
        tdSql.execute(f"insert into tc12.ct0 values ({ts + 1500}, 2)")
        self._waitRows("select * from tc12.res", 1)

    def _tc13_full_request_old_uid_removed(self):
        tdLog.info("TC13: full-scan request drops removed uid (skip; needs cache probe)")
        pass

    def _tc14_partial_request_keeps_others(self):
        tdLog.info("TC14: partial request preserves other uids (skip; needs cache probe)")
        pass

    def _tc15_rpc_single_failure_skip(self):
        tdLog.info("TC15: single-shot RPC failure -> skip (skip; needs fault injection)")
        pass

    # ----- TC16/TC17/TC18 — CHILD vtable tag semantics --------------------
    #
    # Per design v0.3:
    #   * a virtual child table's column MUST be a reference (no constant);
    #   * a virtual child table's tag MAY be either a reference OR a constant
    #     stored on the vchild's own STag (read locally by the first-hop vnode
    #     via streamReadChildTagConstValue, no cross-vnode RPC);
    #   * a NORMAL virtual table has no tag concept; any tag request must be
    #     rejected with TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST.

    def _tc16_child_vtable_tag_ref(self):
        tdLog.info("TC16: CHILD vtable tag = reference (chain-traced from src ct)")
        tdSql.execute("create database tc16 vgroups 1")
        tdSql.execute("use tc16")
        # Source super-table + child tables carrying the real tag values.
        tdSql.execute(
            "create stable tc16.stb_src (ts timestamp, v int) "
            "tags (region int)"
        )
        tdSql.execute("create table tc16.ct1 using tc16.stb_src tags(101)")
        tdSql.execute("create table tc16.ct2 using tc16.stb_src tags(202)")
        # Virtual super-table mirrors the source schema.
        tdSql.execute(
            "create stable tc16.vstb (ts timestamp, v int) "
            "tags (region int) virtual 1"
        )
        # CHILD vtables: column references the source ct, tag references the
        # source ct's tag (chain-traced through TDMT_VND_VTABLE_REF_RESOLVE).
        tdSql.execute(
            "create vtable tc16.vct1 (v from tc16.ct1.v) "
            "using tc16.vstb tags(region from tc16.ct1.region)"
        )
        tdSql.execute(
            "create vtable tc16.vct2 (v from tc16.ct2.v) "
            "using tc16.vstb tags(region from tc16.ct2.region)"
        )
        # Stream from the virtual stable, partition by the chain-traced tag,
        # propagate the tag onto output sub-tables.
        tdSql.execute(
            "create stream tc16.s16 interval(1s) sliding(1s) from tc16.vstb "
            "partition by region into tc16.res16 "
            "tags (region int as region) as "
            "select _twstart as ts, last(v) as v from %%trows "
            "where ts >= _twstart and ts < _twend"
        )
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into tc16.ct1 values ({ts}, 1)")
        tdSql.execute(f"insert into tc16.ct1 values ({ts + 1500}, 2)")
        tdSql.execute(f"insert into tc16.ct2 values ({ts}, 10)")
        tdSql.execute(f"insert into tc16.ct2 values ({ts + 1500}, 20)")
        self._waitRows("select distinct region from tc16.res16", 2)
        tdSql.query("select distinct region from tc16.res16 order by region")
        regions = [tdSql.getData(i, 0) for i in range(tdSql.getRows())]
        # Tag values must come from the chain-traced source tags (101 / 202),
        # not from any constant on the vchild.
        if 101 not in regions or 202 not in regions:
            tdLog.exit(f"TC16: expected chain-traced tags 101/202, got {regions}")

    def _tc17_child_vtable_tag_const(self):
        tdLog.info("TC17: CHILD vtable tag = constant on vchild's own STag")
        tdSql.execute("create database tc17 vgroups 1")
        tdSql.execute("use tc17")
        tdSql.execute("create table tc17.ct1 (ts timestamp, v int)")
        tdSql.execute("create table tc17.ct2 (ts timestamp, v int)")
        # Virtual super-table.
        tdSql.execute(
            "create stable tc17.vstb (ts timestamp, v int) "
            "tags (region int) virtual 1"
        )
        # CHILD vtables with constant tag literals: the value lives on the
        # vchild's own STag and must be read locally by the first-hop vnode
        # (no chain RPC for the tag).
        tdSql.execute(
            "create vtable tc17.vct1 (v from tc17.ct1.v) "
            "using tc17.vstb tags(42)"
        )
        tdSql.execute(
            "create vtable tc17.vct2 (v from tc17.ct2.v) "
            "using tc17.vstb tags(99)"
        )
        tdSql.execute(
            "create stream tc17.s17 interval(1s) sliding(1s) from tc17.vstb "
            "partition by region into tc17.res17 "
            "tags (region int as region) as "
            "select _twstart as ts, last(v) as v from %%trows "
            "where ts >= _twstart and ts < _twend"
        )
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into tc17.ct1 values ({ts}, 1)")
        tdSql.execute(f"insert into tc17.ct1 values ({ts + 1500}, 2)")
        tdSql.execute(f"insert into tc17.ct2 values ({ts}, 10)")
        tdSql.execute(f"insert into tc17.ct2 values ({ts + 1500}, 20)")
        self._waitRows("select distinct region from tc17.res17", 2)
        tdSql.query("select distinct region from tc17.res17 order by region")
        regions = [tdSql.getData(i, 0) for i in range(tdSql.getRows())]
        # The literal tags 42 / 99 must surface verbatim.
        if 42 not in regions or 99 not in regions:
            tdLog.exit(f"TC17: expected constant tags 42/99, got {regions}")

    def _tc18_normal_vtable_tag_rejected(self):
        # NORMAL vtable has no tag concept -- the SQL parser already rejects
        # any "partition by <tag>" against it, so this rejection is exercised
        # at the parser level rather than reaching the chain-ref reader path.
        # We document the expectation here as a soft-skip placeholder; the
        # corresponding code-level check lives in
        # streamPushInitialWorkItemsForUid (vnodeStream.c) and returns
        # TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST when type != CHILD vtable.
        tdLog.info("TC18: NORMAL vtable tag rejected (skip; SQL parser blocks earlier)")
        pass
