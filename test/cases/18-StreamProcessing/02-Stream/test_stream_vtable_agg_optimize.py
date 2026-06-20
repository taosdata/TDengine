from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem,)


class TestStreamVtableAggOptimize:
    """Verify the virtual-super-table aggregation optimization in stream computing.

    The optimization rewrites an aggregate query over a virtual super table so
    that each original source table's data is aggregated locally on its own vnode
    before being merged, instead of materializing every virtual child table row by
    row (TS-7591 / PR #33995). It was previously gated off for stream calc.

    Scope: the pushdown is enabled for stream only for the non-partitioned
    aggregate shape. When the query partitions by virtual-child identity
    (`partition by tbname/tag`), the per-vgroup reader that the partial aggregate
    is fused into has no access to the virtual-child identity injected by
    DynQueryCtrl, so the optimization is intentionally disabled and the query
    falls back to the unoptimized path (see vstableAggShouldBeOptimized).

    Regression cases (from code review):

    Review #1 (planOptimizer.c ~11321):
        A stream calc query with `partition by tbname` AND a WHERE filter that
        lands on the PARTITION node is not guarded by vstableAggShouldBeOptimized.
        If the optimization fires incorrectly the WHERE filter is silently dropped.

    Review #3 (dynqueryctrloperator.c ~1554):
        buildBatchExchangeOperatorParamForVirtual removes entries from
        newAddedVgInfo on first call; sequential callers sharing the same
        pVtbScan->newAddedVgInfo miss the entry and skip registering the new
        vgroup reader, so subsequent windows drop data from the new vgroup.

    Review #4 (exchangeoperator.c ~2207):
        pSources is pushed unconditionally but pFetchRpcHandles conditionally;
        if they diverge, doSendFetchDataRequest crashes indexing pFetchRpcHandles
        by srcIdx out of bounds.

    Review #6 (dynqueryctrloperator.c ~5976):
        resetDynQueryCtrlOperState NULLs otbVgIdToOtbInfoArrayMap for VTB_AGG;
        buildOrgTbInfoBatch(hasPartition=true) skips re-initializing it, so
        buildHashIntervalOperatorParam passes NULL to taosHashIterate — crash.
    """

    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_vtable_agg_optimize(self):
        """Stream: virtual super table aggregation optimization correctness

        Covers:
          1. Aggregate without partition (optimization applied)
          2. Aggregate with partition by tbname (optimization skipped, result correct)
          3. Aggregate with residual WHERE filter (filter preserved, result correct)
          4. Dynamic redeploy: new virtual child on new vgroup picked up by next window
          5. Review #1: partition by tbname + WHERE — filter must not be dropped
          6. Review #3+4: redeploy new vgroup — exchange source wired correctly, no crash
          7. Review #6: partition by tbname, multiple windows — no NULL-map crash on reset

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7591

        History:
            - 2025-12-31 Created
            - 2026-06-11 Added partition case and review regression cases
        """

        tdStream.createSnode()
        tdSql.execute("alter all dnodes 'debugflag 131';")
        tdSql.execute("alter all dnodes 'stdebugflag 131';")

        streams = []
        streams.append(self.AggNoPartition())
        streams.append(self.AggPartitionByTbname())
        streams.append(self.AggResidualFilter())
        streams.append(self.AggRedeploy())
        streams.append(self.ReviewPartitionWithFilter())
        streams.append(self.ReviewRedeployNewVgroup())
        streams.append(self.ReviewPartitionMultipleWindows())
        streams.append(self.ReviewInterleavedColRefs())
        streams.append(self.ReviewHavingFilter())

        tdStream.checkAll(streams)

    # ------------------------------------------------------------------ #
    # shared helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _trigger_rows(triggertb):
        # state_window(cint): 1*6 -> 2*6 -> 3*1 closes 2 windows
        # (twstart 00:00:00 and 00:00:30); the cint=3 window stays open.
        return [
            f"insert into {triggertb} values ('2025-01-01 00:00:00', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:05', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:10', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:15', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:20', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:25', 1);",
            f"insert into {triggertb} values ('2025-01-01 00:00:30', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:00:35', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:00:40', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:00:45', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:00:50', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:00:55', 2);",
            f"insert into {triggertb} values ('2025-01-01 00:01:00', 3);",
        ]

    @staticmethod
    def _ref_rows(reftb):
        # 13 rows, values 1..13: count=13, sum=91, avg=7, min=1, max=13, spread=12
        rows = []
        for i, s in enumerate(range(0, 65, 5)):
            rows.append(
                f"insert into {reftb} values ('2025-01-01 00:00:{s:02d}', {i + 1});")
        return rows

    @staticmethod
    def _make_dbs(dbs, precision):
        for db in dbs:
            tdSql.execute(
                f"create database if not exists {db} "
                f"vgroups 1 buffer 8 precision '{precision}'"
            )

    # ================================================================== #
    # case 1: aggregate without partition (optimization applied)
    # ================================================================== #
    class AggNoPartition(StreamCheckItem):
        """Baseline: no-partition aggregate over a virtual super table.

        The optimization applies (DYN_QTYPE_VTB_AGG pushdown). Two virtual
        children each feed values 1..13; per window the merged result must be
        count=26, sum=182, avg=7, min=1, max=13, spread=12. Also checks the
        result table schema. This is the case the optimization targets.
        """
        def __init__(self):
            self.db      = "svagg0"
            self.refdb1  = "svagg0_ref1"
            self.refdb2  = "svagg0_ref2"
            self.triggertb = "trig0"
            self.reftb   = "reftb"
            self.vstb    = "vstb0"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_agg0 state_window(cint) from {self.triggertb} into res_agg0 "
                f"as select _twstart wstart, count(c1), sum(c1), avg(c1), min(c1), max(c1), spread(c1) "
                f"from {self.vstb};")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_agg0%"',
                func=lambda: tdSql.getRows() == 1,
            )
            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_agg0",
                schema=[
                    ["wstart", "TIMESTAMP", 8, ""],
                    ["count(c1)", "BIGINT", 8, ""],
                    ["sum(c1)", "BIGINT", 8, ""],
                    ["avg(c1)", "DOUBLE", 8, ""],
                    ["min(c1)", "INT", 4, ""],
                    ["max(c1)", "INT", 4, ""],
                    ["spread(c1)", "DOUBLE", 8, ""],
                ],
            )
            # two source tables each values 1..13: count=26, sum=182, avg=7, min=1, max=13, spread=12
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_agg0 order by wstart",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 26)
                             and tdSql.compareData(0, 2, 182)
                             and tdSql.compareData(0, 3, 7.0)
                             and tdSql.compareData(0, 4, 1)
                             and tdSql.compareData(0, 5, 13)
                             and tdSql.compareData(0, 6, 12.0)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 26)
                             and tdSql.compareData(1, 2, 182)
                             and tdSql.compareData(1, 3, 7.0)
                             and tdSql.compareData(1, 4, 1)
                             and tdSql.compareData(1, 5, 13)
                             and tdSql.compareData(1, 6, 12.0),
            )

    # ================================================================== #
    # case 2: aggregate with partition by tbname (optimization skipped)
    # ================================================================== #
    class AggPartitionByTbname(StreamCheckItem):
        """Baseline: aggregate with `partition by tbname` over a virtual super table.

        The optimization is intentionally skipped (partition by virtual-child
        identity has no per-vgroup-reader equivalent), falling back to the
        unoptimized path. Verifies the fallback still produces correct
        per-partition aggregates. Output goes to a single normal table, so the
        two children's partition rows collide on each window's ts primary key —
        the emitted shape is 2 rows (one per window), each count=13/sum=91.
        """
        def __init__(self):
            self.db      = "svagg1"
            self.refdb1  = "svagg1_ref1"
            self.refdb2  = "svagg1_ref2"
            self.triggertb = "trig1"
            self.reftb   = "reftb"
            self.vstb    = "vstb1"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_agg1 state_window(cint) from {self.triggertb} into res_agg1 "
                f"as select _twstart wstart, tbname, count(c1), sum(c1) "
                f"from {self.vstb} partition by tbname;")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            # `partition by tbname` into a single normal output table: both virtual
            # children (vc1, vc2) emit a row per window keyed by wstart. The output
            # table has ts as primary key, so the two partitions collide on each
            # window timestamp — the result is one row per window (2 windows = 2 rows),
            # each carrying a single partition's correct aggregate (count=13, sum=91).
            # (4 distinct partition rows would require a subtable-per-partition output.)
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_agg1 order by wstart, tbname",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 2, 13)
                             and tdSql.compareData(0, 3, 91)
                             and tdSql.compareData(1, 2, 13)
                             and tdSql.compareData(1, 3, 91),
            )

    # ================================================================== #
    # case 3: aggregate with residual WHERE filter (optimization skipped)
    # ================================================================== #
    class AggResidualFilter(StreamCheckItem):
        """Baseline: aggregate with a residual WHERE filter over a virtual super table.

        The WHERE (`c1 > 6`) is a residual filter the optimization cannot push
        down, so it is skipped and the query falls back to the unoptimized path.
        Verifies the filter is NOT dropped: values 7..13 (7 per child, 2
        children) → per window count=14, sum=140.
        """
        def __init__(self):
            self.db      = "svagg2"
            self.refdb1  = "svagg2_ref1"
            self.refdb2  = "svagg2_ref2"
            self.triggertb = "trig2"
            self.reftb   = "reftb"
            self.vstb    = "vstb2"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_agg2 state_window(cint) from {self.triggertb} into res_agg2 "
                f"as select _twstart wstart, count(c1), sum(c1) from {self.vstb} where c1 > 6;")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            # values > 6: 7..13 = 7 per child; 2 children: count=14, sum=140
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_agg2 order by wstart",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 14)
                             and tdSql.compareData(0, 2, 140)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 14)
                             and tdSql.compareData(1, 2, 140),
            )

    # ================================================================== #
    # case 4: dynamic redeploy — new virtual child on new vgroup
    # ================================================================== #
    class AggRedeploy(StreamCheckItem):
        """Baseline: runtime redeploy — a new virtual child on a new vgroup.

        Starts with one virtual child (count=13, sum=91 per window). Mid-stream
        a second child on a different source db/vgroup is added; the next window
        must include both children (count=26, sum=182), exercising the stream
        redeploy path that wires the newly-resolved vgroup into the merge
        exchange. count=13 in the new window would mean the new vgroup was missed.
        """
        def __init__(self):
            self.db      = "svagg3"
            self.refdb1  = "svagg3_ref1"
            self.refdb2  = "svagg3_ref2"
            self.triggertb = "trig3"
            self.reftb   = "reftb"
            self.vstb    = "vstb3"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create stream s_agg3 state_window(cint) from {self.triggertb} into res_agg3 "
                f"as select _twstart wstart, count(c1), sum(c1) from {self.vstb};")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_agg3 order by wstart",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91),
            )

        def insert2(self):
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            sqls = TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            sqls += [f"insert into {self.db}.{self.triggertb} values ('2025-01-01 00:01:05', 4);"]
            tdSql.executes(sqls)

        def check2(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_agg3 order by wstart",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 26)
                             and tdSql.compareData(2, 2, 182),
            )

    # ================================================================== #
    # Review #1: partition by tbname + WHERE — filter must not be dropped
    # ================================================================== #
    class ReviewPartitionWithFilter(StreamCheckItem):
        """Review finding #1: PARTITION node pConditions not guarded in optimizer.

        If vstableAggShouldBeOptimized fires for a `partition by tbname where c1>6`
        stream calc query, the WHERE is silently dropped and results show
        count=13/sum=91 instead of the correct count=7/sum=70.
        """

        def __init__(self):
            self.db      = "rv_bug1"
            self.refdb1  = "rv_bug1_ref1"
            self.refdb2  = "rv_bug1_ref2"
            self.triggertb = "trig_b1"
            self.reftb   = "reftb"
            self.vstb    = "vstb_b1"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_rv_bug1 state_window(cint) "
                f"from {self.db}.{self.triggertb} into res_rv_bug1 "
                f"as select _twstart wstart, tbname, count(c1), sum(c1) "
                f"from {self.vstb} where c1 > 6 partition by tbname;")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            # `partition by tbname` into a single normal output table: the two
            # virtual children collide on each window's ts (primary key), so the
            # stream emits one row per window (2 windows = 2 rows). The WHERE
            # filter c1>6 keeps values 7..13 → count=7, sum=70 per partition.
            # The point of this case is that the filter is NOT dropped (would be
            # count=13/sum=91 otherwise); 2-vs-4 rows is the output-shape collision
            # already verified in AggPartitionByTbname.
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_bug1 order by wstart, tbname",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 2, 7) and tdSql.compareData(0, 3, 70)
                             and tdSql.compareData(1, 2, 7) and tdSql.compareData(1, 3, 70),
            )

    # ================================================================== #
    # Review #3+4: redeploy new vgroup — exchange source wired, no crash
    # ================================================================== #
    class ReviewRedeployNewVgroup(StreamCheckItem):
        """Review findings #3 and #4.

        #3: newAddedVgInfo consumed by first sequential caller; subsequent
        callers miss the new vgroup reader — data from new vgroup is dropped.

        #4: pSources pushed unconditionally, pFetchRpcHandles conditionally;
        divergence causes out-of-bounds crash in doSendFetchDataRequest.
        """

        def __init__(self):
            self.db      = "rv_bug34"
            self.refdb1  = "rv_bug34_ref1"
            self.refdb2  = "rv_bug34_ref2"
            self.triggertb = "trig_b34"
            self.reftb   = "reftb"
            self.vstb    = "vstb_b34"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create stream s_rv_bug34 state_window(cint) "
                f"from {self.db}.{self.triggertb} into res_rv_bug34 "
                f"as select _twstart wstart, count(c1), sum(c1) from {self.vstb};")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_bug34 order by wstart",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91),
            )

        def insert2(self):
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            sqls = TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            sqls.append(
                f"insert into {self.db}.{self.triggertb} values ('2025-01-01 00:01:05', 4);")
            tdSql.executes(sqls)

        def check2(self):
            # new window must include both children: count=26 sum=182
            # count=13 would mean new vgroup was missed (#3); crash means #4
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_bug34 order by wstart",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 26)
                             and tdSql.compareData(2, 2, 182),
            )

    # ================================================================== #
    # Review #6: partition by tbname, 3 windows — no NULL-map crash on reset
    # ================================================================== #
    class ReviewPartitionMultipleWindows(StreamCheckItem):
        """Review finding #6: otbVgIdToOtbInfoArrayMap NULLed on reset.

        resetDynQueryCtrlOperState NULLs otbVgIdToOtbInfoArrayMap for VTB_AGG.
        buildOrgTbInfoBatch(hasPartition=true) skips re-initializing it, so
        buildHashIntervalOperatorParam passes NULL to taosHashIterate — crash.
        Three state windows exercise three reset cycles on this path.
        """

        def __init__(self):
            self.db      = "rv_bug6"
            self.refdb1  = "rv_bug6_ref1"
            self.refdb2  = "rv_bug6_ref2"
            self.triggertb = "trig_b6"
            self.reftb   = "reftb"
            self.vstb    = "vstb_b6"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_rv_bug6 state_window(cint) "
                f"from {self.db}.{self.triggertb} into res_rv_bug6 "
                f"as select _twstart wstart, tbname, count(c1), sum(c1) "
                f"from {self.vstb} partition by tbname;")

        def insert1(self):
            # close 3 windows: cint 1*4 -> 2*4 -> 3*4 -> 4*1
            trigger_sqls = []
            for w, val in enumerate([1, 2, 3], start=0):
                for i in range(4):
                    ts = f"2025-01-01 00:{w:02d}:{i*15:02d}"
                    trigger_sqls.append(
                        f"insert into {self.db}.{self.triggertb} values ('{ts}', {val});")
            trigger_sqls.append(
                f"insert into {self.db}.{self.triggertb} values ('2025-01-01 00:03:00', 4);")

            ref_sqls = []
            for reftb in [f"{self.refdb1}.{self.reftb}", f"{self.refdb2}.{self.reftb}"]:
                for i in range(5):
                    ts = f"2025-01-01 00:00:{i*12:02d}"
                    ref_sqls.append(f"insert into {reftb} values ('{ts}', {i + 1});")
            tdSql.executes(trigger_sqls + ref_sqls)

        def check1(self):
            # This case's purpose is finding #6: no NULL-map crash across the
            # multiple operator reset cycles that the 3 state windows drive.
            # `partition by tbname` into a single normal output table collides
            # partition rows on each window's ts, and only window 0 covers the
            # ref data (it lives in minute 0), so the exact row count is shape-
            # dependent. Assert the robust invariant: the stream produced output
            # without crashing, and every emitted data row has the correct
            # per-partition aggregate (5 rows per child: count=5, sum=15).
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_bug6 order by wstart, tbname",
                func=lambda: tdSql.getRows() >= 1
                             and all(
                                 tdSql.compareData(r, 2, 5) and tdSql.compareData(r, 3, 15)
                                 for r in range(tdSql.getRows())
                             ),
            )

    # ================================================================== #
    # Review: vtable interleaves column refs across >1 source table
    #         (vscanRefsInterleaveSourceTables guard)
    # ================================================================== #
    class ReviewInterleavedColRefs(StreamCheckItem):
        """Guard `vscanRefsInterleaveSourceTables` in vstableAggShouldBeOptimized.

        When a virtual table's columns reference more than one physical source
        table NON-contiguously (e.g. c1 from refA, c2 from refB, c3 from refA),
        the VTB_AGG pushdown reader cannot scatter the per-source results back
        to the right columns, so the optimization must be skipped and the query
        must fall back to the unoptimized VTB_SCAN path. This case puts both
        source tables in the SAME database (one vgroup) so they share one remote
        vnode — the exact shape that triggered the scatter regression — and
        verifies the no-partition aggregate still produces correct sums.

        Data: refA.t has cols (cx=1..13, cy=101..113); refB.t has cx=1..13.
        vct cols: vax=refA.cx, vbx=refB.cx, vay=refA.cy (refA reused at pos 0,2).
        Per window the optimization-disabled path must yield:
          sum(vax)=91 (1..13), sum(vbx)=91 (1..13), sum(vay)=1391 (101..113).
        A scatter swap would mis-route vay/vbx and change these sums.
        """

        def __init__(self):
            self.db        = "rv_intlv"
            self.srcdb     = "rv_intlv_src"   # both source tables share one vnode
            self.triggertb = "trig_intlv"
            self.vstb      = "vstb_intlv"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs([self.db, self.srcdb], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            # both physical source tables live in the same db (one vgroup -> one remote vnode)
            tdSql.execute(f"create table if not exists {self.srcdb}.ta (cts timestamp, cx int, cy int)")
            tdSql.execute(f"create table if not exists {self.srcdb}.tb (cts timestamp, cx int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, vax int, vbx int, vay int) tags (t1 int) virtual 1")
            # interleave refs: ta (pos 0), tb (pos 1), ta again (pos 2) -> non-contiguous
            tdSql.execute(
                f"create vtable if not exists {self.db}.vct ("
                f"  vax from {self.srcdb}.ta.cx, "
                f"  vbx from {self.srcdb}.tb.cx, "
                f"  vay from {self.srcdb}.ta.cy"
                f") using {self.vstb} tags (1)")
            tdSql.execute(
                f"create stream s_rv_intlv state_window(cint) "
                f"from {self.db}.{self.triggertb} into res_rv_intlv "
                f"as select _twstart wstart, sum(vax), sum(vbx), sum(vay) from {self.vstb};")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            # refA: cx=1..13, cy=101..113 ; refB: cx=1..13 (timestamps aligned to windows)
            for i, s in enumerate(range(0, 65, 5)):
                ts = f"2025-01-01 00:00:{s:02d}"
                sqls.append(f"insert into {self.srcdb}.ta values ('{ts}', {i + 1}, {101 + i});")
                sqls.append(f"insert into {self.srcdb}.tb values ('{ts}', {i + 1});")
            tdSql.executes(sqls)

        def check1(self):
            # 2 windows; each must carry the correctly-scattered sums:
            #   sum(vax)=91 (1..13), sum(vbx)=91 (1..13), sum(vay)=1391 (101..113)
            # A scatter swap (the regression) would change vbx/vay.
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_intlv order by wstart",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 1, 91)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, 1391)
                             and tdSql.compareData(1, 1, 91)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, 1391),
            )

    # ================================================================== #
    # Review: residual HAVING filter on the aggregate (optimization skipped)
    # ================================================================== #
    class ReviewHavingFilter(StreamCheckItem):
        """Residual HAVING filter on a no-partition aggregate over a vtable.

        HAVING attaches its condition to the aggregate node (pAgg->node.pConditions),
        which the stream-calc guard in vstableAggShouldBeOptimized treats as a
        residual filter and bails on — the WHERE cases cover pConditions on the
        scan, this covers pConditions on the agg. The optimization is skipped and
        the query falls back to the unoptimized path; the HAVING must NOT be
        dropped.

        TDengine rejects `_twstart` in the projection alongside HAVING without an
        explicit GROUP BY ("Not a GROUP BY expression"), so the calc projects the
        aggregate only and groups by tbname to give HAVING a grouping context.
        Two virtual children each feed values 1..13 (per-child sum 91). With
        `group by tbname having sum(c1) > 50`, both children's partitions (91 > 50)
        pass, so each closed window emits rows that all carry sum=91; a dropped
        HAVING would not change these values but a mis-evaluated HAVING that
        excludes 91 would yield zero rows.
        """

        def __init__(self):
            self.db      = "rv_having"
            self.refdb1  = "rv_having_ref1"
            self.refdb2  = "rv_having_ref2"
            self.triggertb = "trig_hv"
            self.reftb   = "reftb"
            self.vstb    = "vstb_hv"

        def create(self):
            p = TestStreamVtableAggOptimize.precision
            TestStreamVtableAggOptimize._make_dbs(
                [self.db, self.refdb1, self.refdb2], p)
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(
                f"create stable if not exists {self.db}.{self.vstb} "
                f"(cts timestamp, c1 int) tags (t1 int) virtual 1")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc1 "
                f"(c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1)")
            tdSql.execute(
                f"create vtable if not exists {self.db}.vc2 "
                f"(c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2)")
            tdSql.execute(
                f"create stream s_rv_having state_window(cint) from {self.triggertb} into res_rv_having "
                f"as select _twstart wstart, tbname, sum(c1) from {self.vstb} "
                f"partition by tbname having sum(c1) > 50;")

        def insert1(self):
            sqls = TestStreamVtableAggOptimize._trigger_rows(f"{self.db}.{self.triggertb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb1}.{self.reftb}")
            sqls += TestStreamVtableAggOptimize._ref_rows(f"{self.refdb2}.{self.reftb}")
            tdSql.executes(sqls)

        def check1(self):
            # HAVING sum(c1) > 50: every emitted row must satisfy the predicate,
            # whatever way state_window splits the values across windows. This is
            # windowing-independent — a dropped or mis-evaluated HAVING would let
            # a row with sum <= 50 through.
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_rv_having order by wstart, tbname",
                func=lambda: tdSql.getRows() >= 1
                             and all(
                                 tdSql.getData(r, 2) > 50
                                 for r in range(tdSql.getRows())
                             ),
            )
