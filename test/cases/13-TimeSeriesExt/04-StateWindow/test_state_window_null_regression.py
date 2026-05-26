###################################################################
#           Copyright (c) 2025 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import time
from new_test_framework.utils import (
    tdLog, tdSql, etool, tdStream,
)


class TestStateWindowNullRegression:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_state_window_null_regression(self):
        """Regression for multi-column state window NULL handling.

        Bug definitions:
          P1 (dirty state): stCompareStateValuesWithRow phase-2 initializes
              an undefined column BEFORE the cut-window decision, so the old
              window's state is contaminated with the new row's value.
          P2 (zeroth vs undefined): stIsStateValuesEqualZeroths treats an
              undefined column (calloc zero) as equal to zeroth_state(0, ...),
              causing windows with genuinely undefined columns to be filtered.
          P3 (pending-defined pollution): deferred partial-NULL rows initialize
              the pending shadow, and stCompareStateValuesWithRow skips the
              comparison because stateKeyDefined is still false, missing a
              mismatch that should trigger a cut-window.

        Deferred partial-NULL consistency (P2-3):
          P1-1: stCommitPendingToState skips undefined columns, so the
              committed pending value is lost and the next row is treated
              as initialization instead of mismatch.
          P1-2: splitStandalone adds stStateKeysAllDefined guard absent
              from query path, preventing standalone split when old
              window has undefined columns.

        Test plan:
          1. Query baseline: per-column NULL semantics correctness
          2. P1+P2: stream zeroth with undef-then-defined column during cut
          3. P2-only: stream zeroth with column that stays undefined
          4. P3-only: pending-defined pollution regression (extend(2) + zeroth)
          5. P3 pending-init-mismatch: query / realtime / history
          6. Deferred EXTEND(0) same/different: three-path consistency
          7. Deferred EXTEND(2) front-only with undefined old column

        Catalog:
            - TimeSeriesExt:StateWindow

        Since: v3.4.1.0

        Labels: state window, multi-col, null, deferred, regression, ci

        Jira: None

        History:
            - 2026-04-21 Tony Zhang Regression for P1+P2 null handling fixes
            - 2026-04-22 Tony Zhang Three-path consistency for P2-3

        """
        self.do_prepare()

        self.do_query_partial_null_baseline()
        self.do_query_dual_side_partial_null_extend_matrix()
        self.do_query_front_only_partial_null_extend2_standalone()
        self.do_query_pending_init_mismatch()
        self.do_stream_p1_p2_zeroth()
        self.do_stream_p2_only_zeroth()
        self.do_stream_p2_only_zeroth_history()
        self.do_stream_p3_pending_defined_pollution()
        self.do_stream_dual_side_partial_null_extend_matrix_realtime()
        self.do_stream_dual_side_partial_null_extend_matrix_history()
        self.do_stream_front_only_partial_null_extend2_standalone_realtime()
        self.do_stream_front_only_partial_null_extend2_standalone_history()
        self.do_stream_pending_init_mismatch_realtime()
        self.do_stream_pending_init_mismatch_history()

        self.do_query_extend0_same()
        self.do_stream_extend0_same_realtime()
        self.do_stream_extend0_same_history()

        self.do_query_extend0_different()
        self.do_stream_extend0_different_realtime()
        self.do_stream_extend0_different_history()

        self.do_query_extend2_front_only_undef()
        self.do_stream_extend2_front_only_undef_realtime()
        self.do_stream_extend2_front_only_undef_history()

        self.do_query_extend2_front_only_internal_allnull()
        self.do_stream_extend2_front_only_internal_allnull_realtime()
        self.do_stream_extend2_front_only_internal_allnull_history()

        self.do_query_null_absorb_single_vs_multi_col()
        self.do_stream_null_absorb_realtime()
        self.do_stream_null_absorb_history()

    def do_prepare(self):
        tdSql.execute("drop database if exists test_sw_null_reg")
        tdSql.execute(
            "create database test_sw_null_reg keep 3650"
        )
        tdSql.execute("use test_sw_null_reg")
        try:
            tdStream.createSnode()
        except Exception:
            tdLog.info("snode already exists, skip")

        tdSql.execute("drop database if exists test_sw_defer")
        tdSql.execute("create database test_sw_defer keep 3650")

        print("prepare .......................... [passed]\n")

    def wait_result_count(self, sql, expect, timeout=60):
        cnt = 0
        for _ in range(timeout):
            try:
                tdSql.query(sql)
                cnt = tdSql.getData(0, 0)
                if cnt is not None and cnt >= expect:
                    return
            except Exception:
                pass
            time.sleep(1)
        tdSql.query(sql.replace("count(*)", "*"), show=True)
        raise Exception(
            f"timeout: expected >= {expect} rows, got {cnt}"
        )

    # --- impl ---

    def do_query_partial_null_baseline(self):
        """Verify per-column NULL semantics in the query executor.

        ntb_base data (c1, c2):
          Row 0: (1,    NULL)  open W1, p1=1, p2=undef
          Row 1: (1,    NULL)  c1 same, c2 skip → W1
          Row 2: (2,    NULL)  c1 change → cut W1(cnt=2), W2
          Row 3: (1,    10)    c1 change → cut W2(cnt=1), W3
          Row 4: (NULL, 20)    c1 skip, c2=20≠10 → cut W3(cnt=1), W4
          Row 5: (NULL, NULL)  allNull → null path (absorbed)
          Row 6: (2,    NULL)  c1 undef→2 (init), c2 skip → W4
          Row 7: (2,    20)    c1 same, c2 same → W4
          Row 8: (1,    NULL)  c1 change → cut W4(cnt=4), W5
        """
        print("query partial-null baseline ........ [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_base "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_base values
            ('2025-10-01 10:00:00', 1,    NULL, 100),
            ('2025-10-01 10:00:01', 1,    NULL, 200),
            ('2025-10-01 10:00:02', 2,    NULL, 300),
            ('2025-10-01 10:00:03', 1,    10,   400),
            ('2025-10-01 10:00:04', NULL, 20,   500),
            ('2025-10-01 10:00:05', NULL, NULL, 600),
            ('2025-10-01 10:00:06', 2,    NULL, 700),
            ('2025-10-01 10:00:07', 2,    20,   800),
            ('2025-10-01 10:00:08', 1,    NULL, 900)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), c1, c2 "
            "from ntb_base state_window(c1, c2) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(5)
        # W1: (1, undef), rows 0-1, cnt=2
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        # W2: (2, undef), row 2, cnt=1
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, 2)
        # W3: (1, 10), row 3, cnt=1
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 1)
        tdSql.checkData(2, 4, 10)
        # W4: (undef→2, 20), rows 4,5,6,7, cnt=4
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(3, 4, 20)
        # W5: (1, undef), row 8, cnt=1
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 3, 1)

        print("query partial-null baseline ....... [passed]\n")

    def do_stream_p1_p2_zeroth(self):
        """P1+P2: when an undefined column transitions to defined
        in the same row that triggers cut-window, the old state
        must NOT be contaminated (P1), and zeroth must treat
        undefined columns as "not equal" (P2).

        state_window(c1, c2) zeroth_state(0, NO_ZEROTH)

          Row 0: (NULL, 'a')  W1: c1=undef, c2='a', cnt=1
          Row 1: (NULL, 'a')  W1 cont, cnt=2
          Row 2: (0,    'b')  c1: undef→0 (P1), c2 change → cut
                              W1 zeroth: c1=undef ≠ 0 → NOT filtered
                              W2: c1=0, c2='b', cnt=1
          Row 3: (0,    'b')  W2 cont, cnt=2
          Row 4: (1,    'c')  c1 change → cut W2
                              W2 zeroth: c1=0=zeroth → FILTERED
                              W3: c1=1, c2='c', cnt=1
          Row 5: (1,    'c')  W3 cont, cnt=2
          sentinel: next day

        Expected: 2 rows (W1 + W3). W2 filtered.
        P1 bug: W1 contaminated to (0,'a') → zeroth match → 1 row
        P2 bug: W1 c1=0(calloc)=zeroth → 1 row
        """
        print("P1+P2: zeroth + undefined ........ [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_p1p2 "
            "(ts timestamp, c1 int, c2 varchar(20), v int)"
        )
        tdSql.execute(
            "create stream s_p1p2 "
            "state_window(c1, c2) extend(0) "
            "zeroth_state(0, NO_ZEROTH) "
            "from ntb_p1p2 "
            "stream_options(fill_history) "
            "into res_p1p2 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_p1p2")

        tdSql.execute("""insert into ntb_p1p2 values
            ('2025-10-01 10:00:00', NULL, 'a',  100),
            ('2025-10-01 10:00:01', NULL, 'a',  200),
            ('2025-10-01 10:00:02', 0,    'b',  300),
            ('2025-10-01 10:00:03', 0,    'b',  400),
            ('2025-10-01 10:00:04', 1,    'c',  500),
            ('2025-10-01 10:00:05', 1,    'c',  600),
            ('2025-10-02 10:00:00', 9,    'z', 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_p1p2 "
            "where wstart < '2025-10-02'",
            2,
        )

        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_p1p2 "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        # W1: c1=undef, c2='a', cnt=2, sum_v=300
        tdSql.checkData(
            0, 0, "2025-10-01 10:00:00.000"
        )
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W3: c1=1, c2='c', cnt=2, sum_v=1100
        tdSql.checkData(
            1, 0, "2025-10-01 10:00:04.000"
        )
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1100)

        print("P1+P2: zeroth + undefined ........ [passed]\n")

    def do_stream_p2_only_zeroth(self):
        """P2-only: when a state column stays NULL (undefined)
        across a window boundary, zeroth must treat it as "not
        equal" to any concrete zeroth value.

        state_window(c1, c2) zeroth_state(0, NO_ZEROTH)

          Row 0: (NULL, 10)  W1: c1=undef, c2=10, cnt=1
          Row 1: (NULL, 20)  c1 skip, c2 change → cut W1
                             W1 zeroth: c1=undef → NOT filtered
                             W2: c1=undef, c2=20, cnt=1
          Row 2: (NULL, 20)  W2 cont, cnt=2
          Row 3: (1,    20)  c1 undef→1 (init only), c2 same → W2 cont, cnt=3
          Row 4: (1,    30)  c2 change → cut W2
                             W2 zeroth: c1=1≠0 → NOT filtered
                             W3: c1=1, c2=30, cnt=1
          Row 5: (1,    30)  W3 cont, cnt=2
          sentinel: next day

        Expected: 3 rows (W1 + W2 + W3). None filtered.
        P2 bug: W1 c1=0(calloc)=zeroth → filtered → fewer rows
        """
        print("P2-only: zeroth + undefined ...... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_p2 "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_p2 "
            "state_window(c1, c2) extend(0) "
            "zeroth_state(0, NO_ZEROTH) "
            "from ntb_p2 "
            "stream_options(fill_history) "
            "into res_p2 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_p2")

        tdSql.execute("""insert into ntb_p2 values
            ('2025-10-01 10:00:00', NULL, 10,  100),
            ('2025-10-01 10:00:01', NULL, 20,  200),
            ('2025-10-01 10:00:02', NULL, 20,  300),
            ('2025-10-01 10:00:03', 1,    20,  400),
            ('2025-10-01 10:00:04', 1,    30,  500),
            ('2025-10-01 10:00:05', 1,    30,  600),
            ('2025-10-02 10:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_p2 where wstart < '2025-10-02'",
            3,
        )

        tdSql.query(
            "select wstart, wend, cnt, sum_v from res_p2 where wstart < '2025-10-02' order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        # W1: c1=undef, c2=10, cnt=1, sum_v=100
        tdSql.checkData(
            0, 0, "2025-10-01 10:00:00.000"
        )
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        # W2: c1=undef→1, c2=20, cnt=3, sum_v=900
        tdSql.checkData(
            1, 0, "2025-10-01 10:00:01.000"
        )
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)
        # W3: c1=1, c2=30, cnt=2, sum_v=1100
        tdSql.checkData(
            2, 0, "2025-10-01 10:00:04.000"
        )
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)

        print("P2-only: zeroth + undefined ...... [passed]\n")

    def do_stream_p2_only_zeroth_history(self):
        """History path: validate fill_history replay keeps the same
        undefined->defined semantics as realtime path.

        Data pattern is identical to do_stream_p2_only_zeroth():
        W1: (undef,10) cnt=1
        W2: (undef->1,20) cnt=3
        W3: (1,30) cnt=2
        """
        print("P2-only history: fill_history ...... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_p2_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_p2_hist values
            ('2025-10-01 10:00:00', NULL, 10,  100),
            ('2025-10-01 10:00:01', NULL, 20,  200),
            ('2025-10-01 10:00:02', NULL, 20,  300),
            ('2025-10-01 10:00:03', 1,    20,  400),
            ('2025-10-01 10:00:04', 1,    30,  500),
            ('2025-10-01 10:00:05', 1,    30,  600),
            ('2025-10-02 10:00:00', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_p2_hist "
            "state_window(c1, c2) extend(0) "
            "zeroth_state(0, NO_ZEROTH) "
            "from ntb_p2_hist "
            "stream_options(fill_history) "
            "into res_p2_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_p2_hist")

        self.wait_result_count(
            "select count(*) from res_p2_hist where wstart < '2025-10-02'",
            3,
        )

        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_p2_hist where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 10:00:01.000")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)
        tdSql.checkData(2, 0, "2025-10-01 10:00:04.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)

        print("P2-only history: fill_history ...... [passed]\n")

    def do_stream_p3_pending_defined_pollution(self):
        """P3-only: prevent zeroth pollution from pending-defined shadow.

        Use extend(2) so deferred partial-NULL rows are assigned to the new
        window. The old window keeps c1 undefined and must NOT be filtered by
        zeroth_state(0,10).

        Data:
          R0 (NULL,10)  -> W1 open
          R1 (1,NULL)   -> deferred partial-NULL
          R2 (1,20)     -> cut (c2 change), dual-side compatible on c1
                            with extend(2), R1 belongs to new window
          R3 (1,20)     -> W2 continue
          R4 (2,30)     -> cut W2, open W3
          sentinel next day closes W3

        Expected before next day:
          W1 cnt=1 sum=100
          W2 cnt=3 sum=900
          W3 cnt=1 sum=500
        """
        print("P3-only: pending-defined pollution . [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_p3 "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_p3 "
            "state_window(c1, c2) extend(2) "
            "zeroth_state(0, 10) "
            "from ntb_p3 "
            "stream_options(fill_history) "
            "into res_p3 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_p3")

        tdSql.execute("""insert into ntb_p3 values
            ('2025-10-01 11:00:00', NULL, 10,  100),
            ('2025-10-01 11:00:01', 1,    NULL, 200),
            ('2025-10-01 11:00:02', 1,    20,   300),
            ('2025-10-01 11:00:03', 1,    20,   400),
            ('2025-10-01 11:00:04', 2,    30,   500),
            ('2025-10-02 11:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_p3 where wstart < '2025-10-02'",
            3,
        )

        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_p3 where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 11:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 11:00:00.001")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)
        tdSql.checkData(2, 0, "2025-10-01 11:00:03.001")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 500)

        print("P3-only: pending-defined pollution . [passed]\n")

    def do_query_dual_side_partial_null_extend_matrix(self):
        """Query path: dual-side compatible partial-NULL row follows EXTEND.

        Pattern (c1, c2):
          R0: (1,    NULL)  open W1
          R1: (NULL, 10)    dual-side compatible deferred row
          R2: (2,    NULL)  c1 change triggers cut
          R3: (2,    NULL)  W2 continue

        Expected:
          EXTEND(0): R1 to old window     -> [2, 2]
          EXTEND(1): R1 to old window     -> [2, 2]
          EXTEND(2): R1 to new window     -> [1, 3]
        """
        print("query dual-side extend matrix ...... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_ext_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext_q values
            ('2025-10-01 13:00:00.000', 1,    NULL, 100),
            ('2025-10-01 13:00:01.001', NULL, 10,   200),
            ('2025-10-01 13:00:02.002', 2,    NULL, 300),
            ('2025-10-01 13:00:03.003', 2,    NULL, 400)
        """)

        tdSql.query(
            """select _wstart, _wend, count(*), c1, c2 
            from ntb_ext_q state_window(c1, c2) extend(0) order by _wstart""",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 13:00:01.001")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 10)
        tdSql.checkData(1, 0, "2025-10-01 13:00:02.002")
        tdSql.checkData(1, 1, "2025-10-01 13:00:03.003")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, None)

        tdSql.query(
            """select _wstart, _wend, count(*), c1, c2 
            from ntb_ext_q state_window(c1, c2) extend(1) 
            order by _wstart""",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 13:00:02.001")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 10)
        tdSql.checkData(1, 0, "2025-10-01 13:00:02.002")
        tdSql.checkData(1, 1, "2025-10-01 13:00:03.003")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, None)

        tdSql.query(
            """select _wstart, _wend, count(*), c1, c2 
            from ntb_ext_q state_window(c1, c2) extend(2) 
            order by _wstart""",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 13:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 0, "2025-10-01 13:00:00.001")
        tdSql.checkData(1, 1, "2025-10-01 13:00:03.003")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 10)

        print("query dual-side extend matrix ...... [passed]\n")

    def do_query_front_only_partial_null_extend2_standalone(self):
        """Query path: front-only compatible partial-NULL with EXTEND(2) must stand alone."""
        print("query front-only extend2 standalone [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_ext_front_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext_front_q values
            ('2025-10-01 13:10:00.000', 1,    10, 100),
            ('2025-10-01 13:10:01.001', 1,   NULL, 200),
            ('2025-10-01 13:10:02.002', 2,    10, 300),
            ('2025-10-01 13:10:03.003', 2,    10, 400)
        """)

        tdSql.query(
            """select _wstart, _wend, count(*), c1, c2 
            from ntb_ext_front_q state_window(c1, c2) extend(2) 
            order by _wstart""",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 13:10:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "2025-10-01 13:10:00.001")
        tdSql.checkData(1, 1, "2025-10-01 13:10:01.001")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, "2025-10-01 13:10:01.002")
        tdSql.checkData(2, 1, "2025-10-01 13:10:03.003")
        tdSql.checkData(2, 2, 2)
        print("query front-only extend2 standalone [passed]\n")

    def do_stream_dual_side_partial_null_extend_matrix_realtime(self):
        """Realtime stream path: dual-side compatible partial-NULL follows EXTEND."""
        print("stream dual-side extend realtime ... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute("create table ntb_ext_rt_0 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute(
            "create stream s_ext_rt_0 "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext_rt_0 "
            "stream_options(fill_history) "
            "into res_ext_rt_0 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_rt_0")
        tdSql.execute("""insert into ntb_ext_rt_0 values
            ('2025-10-01 13:30:00.000', 1,    NULL, 100),
            ('2025-10-01 13:30:00.001', NULL, 10,   200),
            ('2025-10-01 13:30:00.002', 2,    NULL, 300),
            ('2025-10-01 13:30:00.003', 2,    NULL, 400),
            ('2025-10-02 13:30:00.000', 9,    99, 9999)
        """)
        self.wait_result_count(
            "select count(*) from res_ext_rt_0 where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_rt_0 "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:30:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, "2025-10-01 13:30:00.002")
        tdSql.checkData(1, 2, 2)

        tdSql.execute("create table ntb_ext_rt_1 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute(
            "create stream s_ext_rt_1 "
            "state_window(c1, c2) extend(1) "
            "from ntb_ext_rt_1 "
            "stream_options(fill_history) "
            "into res_ext_rt_1 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_rt_1")
        tdSql.execute("""insert into ntb_ext_rt_1 values
            ('2025-10-01 13:30:00.000', 1,    NULL, 100),
            ('2025-10-01 13:30:00.001', NULL, 10,   200),
            ('2025-10-01 13:30:00.002', 2,    NULL, 300),
            ('2025-10-01 13:30:00.003', 2,    NULL, 400),
            ('2025-10-02 13:30:00.000', 9,    99, 9999)
        """)
        self.wait_result_count(
            "select count(*) from res_ext_rt_1 where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_rt_1 "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:30:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, "2025-10-01 13:30:00.002")
        tdSql.checkData(1, 2, 2)

        tdSql.execute("create table ntb_ext_rt_2 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute(
            "create stream s_ext_rt_2 "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext_rt_2 "
            "stream_options(fill_history) "
            "into res_ext_rt_2 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_rt_2")
        tdSql.execute("""insert into ntb_ext_rt_2 values
            ('2025-10-01 13:30:00.000', 1,    NULL, 100),
            ('2025-10-01 13:30:00.001', NULL, 10,   200),
            ('2025-10-01 13:30:00.002', 2,    NULL, 300),
            ('2025-10-01 13:30:00.003', 2,    NULL, 400),
            ('2025-10-02 13:30:00.000', 9,    99, 9999)
        """)
        self.wait_result_count(
            "select count(*) from res_ext_rt_2 where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_rt_2 "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 13:30:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 13:30:00.001")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)

        print("stream dual-side extend realtime ... [passed]\n")

    def do_stream_dual_side_partial_null_extend_matrix_history(self):
        """History stream path: dual-side compatible partial-NULL follows EXTEND."""
        print("stream dual-side extend history .... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute("create table ntb_ext_hist_0 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute("""insert into ntb_ext_hist_0 values
            ('2025-10-01 14:00:00.000', 1,    NULL, 100),
            ('2025-10-01 14:00:00.001', NULL, 10,   200),
            ('2025-10-01 14:00:00.002', 2,    NULL, 300),
            ('2025-10-01 14:00:00.003', 2,    NULL, 400),
            ('2025-10-02 14:00:00.000', 9,    99, 9999)
        """)
        tdSql.execute(
            "create stream s_ext_hist_0 "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext_hist_0 "
            "stream_options(fill_history) "
            "into res_ext_hist_0 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_hist_0")
        self.wait_result_count(
            "select count(*) from res_ext_hist_0",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_hist_0 "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 14:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        tdSql.checkData(1, 0, "2025-10-01 14:00:00.002")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        tdSql.checkData(2, 0, "2025-10-02 14:00:00.000")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 9999)

        tdSql.execute("create table ntb_ext_hist_1 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute("""insert into ntb_ext_hist_1 values
            ('2025-10-01 14:00:00.000', 1,    NULL, 100),
            ('2025-10-01 14:00:00.001', NULL, 10,   200),
            ('2025-10-01 14:00:00.002', 2,    NULL, 300),
            ('2025-10-01 14:00:00.003', 2,    NULL, 400),
            ('2025-10-02 14:00:00.000', 9,    99, 9999)
        """)
        tdSql.execute(
            "create stream s_ext_hist_1 "
            "state_window(c1, c2) extend(1) "
            "from ntb_ext_hist_1 "
            "stream_options(fill_history) "
            "into res_ext_hist_1 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_hist_1")
        self.wait_result_count(
            "select count(*) from res_ext_hist_1",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_hist_1 "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 14:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        tdSql.checkData(1, 0, "2025-10-01 14:00:00.002")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        tdSql.checkData(2, 0, "2025-10-02 14:00:00.000")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 9999)

        tdSql.execute("create table ntb_ext_hist_2 (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute("""insert into ntb_ext_hist_2 values
            ('2025-10-01 14:00:00.000', 1,    NULL, 100),
            ('2025-10-01 14:00:00.001', NULL, 10,   200),
            ('2025-10-01 14:00:00.002', 2,    NULL, 300),
            ('2025-10-01 14:00:00.003', 2,    NULL, 400),
            ('2025-10-02 14:00:00.000', 9,    99, 9999)
        """)
        tdSql.execute(
            "create stream s_ext_hist_2 "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext_hist_2 "
            "stream_options(fill_history) "
            "into res_ext_hist_2 as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_hist_2")
        self.wait_result_count(
            "select count(*) from res_ext_hist_2",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_hist_2 "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 14:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 14:00:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 14:00:00.001")
        tdSql.checkData(1, 1, "2025-10-01 14:00:00.003")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)
        tdSql.checkData(2, 0, "2025-10-01 14:00:00.004")
        tdSql.checkData(2, 1, "2025-10-02 14:00:00.000")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 9999)

        print("stream dual-side extend history .... [passed]\n")

    def do_stream_front_only_partial_null_extend2_standalone_realtime(self):
        """Realtime stream path: front-only compatible partial-NULL with EXTEND(2) stands alone."""
        print("stream front-only extend2 realtime . [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute("create table ntb_ext_front_rt (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute(
            "create stream s_ext_front_rt "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext_front_rt "
            "stream_options(fill_history) "
            "into res_ext_front_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_front_rt")
        tdSql.execute("""insert into ntb_ext_front_rt values
            ('2025-10-01 13:40:00.000', 1,    10, 100),
            ('2025-10-01 13:40:00.001', 1,   NULL, 200),
            ('2025-10-01 13:40:00.002', 2,    10, 300),
            ('2025-10-01 13:40:00.003', 2,    10, 400),
            ('2025-10-02 13:40:00.000', 9,    99, 9999)
        """)
        self.wait_result_count(
            "select count(*) from res_ext_front_rt where wstart < '2025-10-02'",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext_front_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 13:40:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "2025-10-01 13:40:00.001")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, "2025-10-01 13:40:00.002")
        tdSql.checkData(2, 2, 2)
        print("stream front-only extend2 realtime . [passed]\n")

    def do_stream_front_only_partial_null_extend2_standalone_history(self):
        """History stream path: front-only compatible partial-NULL with EXTEND(2) stands alone."""
        print("stream front-only extend2 history .. [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute("create table ntb_ext_front_hist (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute("""insert into ntb_ext_front_hist values
            ('2025-10-01 14:10:00.000', 1,    10, 100),
            ('2025-10-01 14:10:00.001', 1,   NULL, 200),
            ('2025-10-01 14:10:00.002', 2,    10, 300),
            ('2025-10-01 14:10:00.003', 2,    10, 400),
            ('2025-10-02 14:10:00.000', 9,    99, 9999)
        """)
        tdSql.execute(
            "create stream s_ext_front_hist "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext_front_hist "
            "stream_options(fill_history) "
            "into res_ext_front_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext_front_hist")
        self.wait_result_count(
            "select count(*) from res_ext_front_hist where wstart < '2025-10-02'",
            4,
        )
        tdSql.query(
            """select wstart, wend, cnt, sum_v 
            from res_ext_front_hist 
            where wstart < '2025-10-02' 
            order by wstart""",
            show=True,
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-10-01 14:10:00.000")
        tdSql.checkData(0, 1, "2025-10-01 14:10:00.000")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 14:10:00.001")
        tdSql.checkData(1, 1, "2025-10-01 14:10:00.001")
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, 200)
        tdSql.checkData(2, 0, "2025-10-01 14:10:00.002")
        tdSql.checkData(2, 1, "2025-10-01 14:10:00.003")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 700)
        tdSql.checkData(3, 0, "2025-10-01 14:10:00.004")
        tdSql.checkData(3, 1, "2025-10-02 14:10:00.000")
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 9999)

        print("stream front-only extend2 history .. [passed]\n")

    # --- pending-initialized-then-mismatch regression ---

    def do_query_pending_init_mismatch(self):
        """Query: pending shadow initialized to x by deferred partial-NULL,
        then next all-non-NULL row gives y != x.  Must cut window.

        state_window(c1, c2):
          R0: (NULL, 10)  W1 open, c1=undef, c2=10
          R1: (1,  NULL)  partial-NULL, deferred; pending c1=1
          R2: (2,    10)  c1: pending=1 vs row=2 → mismatch → cut
                          W1 cnt=2 (R0 + R1 deferred)
                          W2 open: c1=2, c2=10, cnt=1
          R3: (2,    10)  W2 cont, cnt=2

        Second pattern:
          R4: (1,    10)  c1 change → cut W2(cnt=2), W3 open
          R5: (NULL, 20)  partial-NULL, c2: 20≠10 → cut
                          W3 cnt=1
                          W4 open: c1=undef, c2=20, cnt=1
          R6: (1,    30)  c2: 30≠20 → cut W4(cnt=1), W5 open
          R7: (1,    30)  W5 cont, cnt=2
        """
        print("query pending-init-mismatch ....... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_pim "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_pim values
            ('2025-10-01 12:00:00', NULL, 10,  100),
            ('2025-10-01 12:00:01', 1,   NULL, 200),
            ('2025-10-01 12:00:02', 2,    10,  300),
            ('2025-10-01 12:00:03', 2,    10,  400),
            ('2025-10-01 12:00:04', 1,    10,  500),
            ('2025-10-01 12:00:05', NULL, 20,  600),
            ('2025-10-01 12:00:06', 1,    30,  700),
            ('2025-10-01 12:00:07', 1,    30,  800)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), c1, c2 "
            "from ntb_pim state_window(c1, c2) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(5)
        # W1: (undef→1, 10), R0+R1, cnt=2
        tdSql.checkData(0, 2, 2)
        # W2: (2, 10), R2+R3, cnt=2
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 10)
        # W3: (1, 10), R4, cnt=1
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 1)
        tdSql.checkData(2, 4, 10)
        # W4: (undef, 20), R5, cnt=1
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 4, 20)
        # W5: (1, 30), R6+R7, cnt=2
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 1)
        tdSql.checkData(4, 4, 30)

        print("query pending-init-mismatch ....... [passed]\n")

    def do_stream_pending_init_mismatch_realtime(self):
        """Realtime stream: pending-initialized-then-mismatch must cut window.

        Same data pattern as do_query_pending_init_mismatch.
        """
        print("pending-init-mismatch realtime .... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_pim_rt "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_pim_rt "
            "state_window(c1, c2) extend(0) "
            "from ntb_pim_rt "
            "stream_options(fill_history) "
            "into res_pim_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_pim_rt")

        tdSql.execute("""insert into ntb_pim_rt values
            ('2025-10-01 12:00:00', NULL, 10,  100),
            ('2025-10-01 12:00:01', 1,   NULL, 200),
            ('2025-10-01 12:00:02', 2,    10,  300),
            ('2025-10-01 12:00:03', 2,    10,  400),
            ('2025-10-01 12:00:04', 1,    10,  500),
            ('2025-10-01 12:00:05', NULL, 20,  600),
            ('2025-10-01 12:00:06', 1,    30,  700),
            ('2025-10-01 12:00:07', 1,    30,  800),
            ('2025-10-02 12:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_pim_rt "
            "where wstart < '2025-10-02'",
            5,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_pim_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(5)
        # W1: cnt=2, sum=300
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: cnt=2, sum=700
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        # W3: cnt=1, sum=500
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 500)
        # W4: cnt=1, sum=600
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 600)
        # W5: cnt=2, sum=1500
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 1500)

        print("pending-init-mismatch realtime .... [passed]\n")

    def do_stream_pending_init_mismatch_history(self):
        """History stream: pending-initialized-then-mismatch must cut window.

        Data inserted before stream creation to exercise fill_history path.
        """
        print("pending-init-mismatch history ..... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_pim_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_pim_hist values
            ('2025-10-01 12:00:00', NULL, 10,  100),
            ('2025-10-01 12:00:01', 1,   NULL, 200),
            ('2025-10-01 12:00:02', 2,    10,  300),
            ('2025-10-01 12:00:03', 2,    10,  400),
            ('2025-10-01 12:00:04', 1,    10,  500),
            ('2025-10-01 12:00:05', NULL, 20,  600),
            ('2025-10-01 12:00:06', 1,    30,  700),
            ('2025-10-01 12:00:07', 1,    30,  800),
            ('2025-10-02 12:00:00', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_pim_hist "
            "state_window(c1, c2) extend(0) "
            "from ntb_pim_hist "
            "stream_options(fill_history) "
            "into res_pim_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v "
            "from %%trows;"
        )
        tdStream.checkStreamStatus("s_pim_hist")

        self.wait_result_count(
            "select count(*) from res_pim_hist "
            "where wstart < '2025-10-02'",
            5,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_pim_hist "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(5)
        # W1: cnt=2, sum=300
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: cnt=2, sum=700
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        # W3: cnt=1, sum=500
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 500)
        # W4: cnt=1, sum=600
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 600)
        # W5: cnt=2, sum=1500
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 1500)

        print("pending-init-mismatch history ..... [passed]\n")

    # ================================================================
    # Scenario 1a: EXTEND(0) - same all-non-NULL -> later changed
    # ================================================================
    #
    # Data (c1, c2, v):
    #   R0: (NULL, 10, 100) -> W1 open: c1=undef, c2=10
    #   R1: (5,  NULL, 200) -> deferred partial-NULL, pending c1=5
    #                          EXTEND(0) -> deferred to old window on cut
    #   R2: (5,    10, 300) -> all-non-NULL. pending c1=5 == row c1=5,
    #                          c2=10 == 10 -> equal -> W1 continue
    #                          (syncStateKeysFromPending: c1 now defined=5)
    #   R3: (5,    10, 400) -> same -> W1 continue
    #   R4: (7,    10, 500) -> c1=7 != 5 -> CUT W1, W2 open
    #   R5: (7,    10, 600) -> W2 continue
    #
    # Expected:
    #   W1: R0-R3, cnt=4, sum=1000
    #   W2: R4-R5, cnt=2, sum=1100

    def do_query_extend0_same(self):
        """Query baseline: deferred partial-NULL, then same all-non-NULL."""
        print("query ext0-same .................. [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_same_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext0_same_q values
            ('2025-10-01 16:00:00', NULL, 10, 100),
            ('2025-10-01 16:00:01', 5,   NULL, 200),
            ('2025-10-01 16:00:02', 5,    10, 300),
            ('2025-10-01 16:00:03', 5,    10, 400),
            ('2025-10-01 16:00:04', 7,    10, 500),
            ('2025-10-01 16:00:05', 7,    10, 600)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), sum(v), c1, c2 "
            "from ntb_ext0_same_q state_window(c1, c2) extend(0) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(2)
        # W1: R0-R3, cnt=4, sum=1000
        tdSql.checkData(0, 0, "2025-10-01 16:00:00.000")
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 1000)
        tdSql.checkData(0, 4, 5)
        tdSql.checkData(0, 5, 10)
        # W2: R4-R5, cnt=2, sum=1100
        tdSql.checkData(1, 0, "2025-10-01 16:00:04.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1100)
        tdSql.checkData(1, 4, 7)
        tdSql.checkData(1, 5, 10)

        print("query ext0-same .................. [passed]\n")

    def do_stream_extend0_same_realtime(self):
        """Realtime stream: must match query ext0-same."""
        print("stream ext0-same realtime ........ [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_same_rt "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_ext0_same_rt "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext0_same_rt "
            "stream_options(fill_history) "
            "into res_ext0_same_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext0_same_rt")

        tdSql.execute("""insert into ntb_ext0_same_rt values
            ('2025-10-01 16:00:00', NULL, 10, 100),
            ('2025-10-01 16:00:01', 5,   NULL, 200),
            ('2025-10-01 16:00:02', 5,    10, 300),
            ('2025-10-01 16:00:03', 5,    10, 400),
            ('2025-10-01 16:00:04', 7,    10, 500),
            ('2025-10-01 16:00:05', 7,    10, 600),
            ('2025-10-02 16:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_ext0_same_rt "
            "where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext0_same_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 16:00:00.000")
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 1000)
        tdSql.checkData(1, 0, "2025-10-01 16:00:04.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1100)

        print("stream ext0-same realtime ........ [passed]\n")

    def do_stream_extend0_same_history(self):
        """History stream: must match query ext0-same."""
        print("stream ext0-same history ......... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_same_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext0_same_hist values
            ('2025-10-01 16:00:00', NULL, 10, 100),
            ('2025-10-01 16:00:01', 5,   NULL, 200),
            ('2025-10-01 16:00:02', 5,    10, 300),
            ('2025-10-01 16:00:03', 5,    10, 400),
            ('2025-10-01 16:00:04', 7,    10, 500),
            ('2025-10-01 16:00:05', 7,    10, 600),
            ('2025-10-02 16:00:00', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_ext0_same_hist "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext0_same_hist "
            "stream_options(fill_history) "
            "into res_ext0_same_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext0_same_hist")

        self.wait_result_count(
            "select count(*) from res_ext0_same_hist "
            "where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext0_same_hist "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-10-01 16:00:00.000")
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 1000)
        tdSql.checkData(1, 0, "2025-10-01 16:00:04.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1100)

        print("stream ext0-same history ......... [passed]\n")

    # ================================================================
    # Scenario 1b: EXTEND(0) - different all-non-NULL -> cut
    # ================================================================
    #
    # Data (c1, c2, v):
    #   R0: (NULL, 10, 100) -> W1 open: c1=undef, c2=10
    #   R1: (5,  NULL, 200) -> deferred partial-NULL, pending c1=5
    #                          EXTEND(0) -> deferred to old window on cut
    #   R2: (7,    10, 300) -> all-non-NULL. pending c1=5 != row c1=7
    #                          -> CUT! Deferred (R1) committed to old W1.
    #                          W1 closed: R0+R1, cnt=2, sum=300
    #                          W2 open: c1=7, c2=10
    #   R3: (7,    10, 400) -> same -> W2 continue
    #   R4: (7,    20, 500) -> c2=20 != 10 -> CUT W2, W3 open
    #   R5: (7,    20, 600) -> W3 continue
    #
    # Expected:
    #   W1: cnt=2, sum=300
    #   W2: cnt=2, sum=700
    #   W3: cnt=2, sum=1100
    #
    # P1-1 bug (stream): stCommitPendingToState skips c1 (undefined),
    # so R2's c1=7 is treated as initialization (no cut).
    # Buggy result: W1 cnt=4, W2 cnt=2 (only 2 windows).

    def do_query_extend0_different(self):
        """Query baseline: deferred partial-NULL, then different all-non-NULL."""
        print("query ext0-diff .................. [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_diff_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext0_diff_q values
            ('2025-10-01 17:00:00', NULL, 10, 100),
            ('2025-10-01 17:00:01', 5,   NULL, 200),
            ('2025-10-01 17:00:02', 7,    10, 300),
            ('2025-10-01 17:00:03', 7,    10, 400),
            ('2025-10-01 17:00:04', 7,    20, 500),
            ('2025-10-01 17:00:05', 7,    20, 600)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), sum(v), c1, c2 "
            "from ntb_ext0_diff_q state_window(c1, c2) extend(0) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(3)
        # W1: R0+R1, cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 17:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: R2+R3, cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 17:00:02.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        tdSql.checkData(1, 4, 7)
        tdSql.checkData(1, 5, 10)
        # W3: R4+R5, cnt=2, sum=1100
        tdSql.checkData(2, 0, "2025-10-01 17:00:04.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)
        tdSql.checkData(2, 4, 7)
        tdSql.checkData(2, 5, 20)

        print("query ext0-diff .................. [passed]\n")

    def do_stream_extend0_different_realtime(self):
        """Realtime stream: must match query ext0-diff.

        If P1-1 bug exists, stream produces only 2 windows instead of 3.
        """
        print("stream ext0-diff realtime ........ [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_diff_rt "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_ext0_diff_rt "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext0_diff_rt "
            "stream_options(fill_history) "
            "into res_ext0_diff_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext0_diff_rt")

        tdSql.execute("""insert into ntb_ext0_diff_rt values
            ('2025-10-01 17:00:00', NULL, 10, 100),
            ('2025-10-01 17:00:01', 5,   NULL, 200),
            ('2025-10-01 17:00:02', 7,    10, 300),
            ('2025-10-01 17:00:03', 7,    10, 400),
            ('2025-10-01 17:00:04', 7,    20, 500),
            ('2025-10-01 17:00:05', 7,    20, 600),
            ('2025-10-02 17:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_ext0_diff_rt "
            "where wstart < '2025-10-02'",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext0_diff_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        # W1: cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 17:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 17:00:02.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        # W3: cnt=2, sum=1100
        tdSql.checkData(2, 0, "2025-10-01 17:00:04.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)

        print("stream ext0-diff realtime ........ [passed]\n")

    def do_stream_extend0_different_history(self):
        """History stream: must match query ext0-diff.

        Data inserted before stream creation to exercise fill_history.
        """
        print("stream ext0-diff history ......... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext0_diff_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext0_diff_hist values
            ('2025-10-01 17:00:00', NULL, 10, 100),
            ('2025-10-01 17:00:01', 5,   NULL, 200),
            ('2025-10-01 17:00:02', 7,    10, 300),
            ('2025-10-01 17:00:03', 7,    10, 400),
            ('2025-10-01 17:00:04', 7,    20, 500),
            ('2025-10-01 17:00:05', 7,    20, 600),
            ('2025-10-02 17:00:00', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_ext0_diff_hist "
            "state_window(c1, c2) extend(0) "
            "from ntb_ext0_diff_hist "
            "stream_options(fill_history) "
            "into res_ext0_diff_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext0_diff_hist")

        self.wait_result_count(
            "select count(*) from res_ext0_diff_hist "
            "where wstart < '2025-10-02'",
            3,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext0_diff_hist "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        # W1: cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 17:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 17:00:02.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        # W3: cnt=2, sum=1100
        tdSql.checkData(2, 0, "2025-10-01 17:00:04.000")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)

        print("stream ext0-diff history ......... [passed]\n")

    # ================================================================
    # Scenario 2: EXTEND(2) - front-only with undefined old column
    # ================================================================
    #
    # Data (c1, c2, v):
    #   R0: (NULL, 10, 100) -> W1 open: c1=undef, c2=10
    #   R1: (5,  NULL, 200) -> deferred partial-NULL: c1=5 (init pending),
    #                          c2=NULL (skip). R0 and R1 have DIFFERENT
    #                          NULL patterns so they are NOT same-key rows.
    #   R2: (2,    10, 300) -> all-non-NULL. pending c1=5 != row c1=2
    #                          -> CUT!
    #                          R1: c1=5 != new c1=2 -> not dual-side
    #                          -> front-only
    #                          BUT old window has undefined column (c1),
    #                          R1 is still filling in undef columns for
    #                          the old window. Per 4.5: deferred rows that
    #                          are completing an incomplete old window
    #                          should stay in the old window, NOT standalone.
    #                          -> no standalone split
    #                          W1 closed: R0+R1, cnt=2, sum=300
    #                          W2 open: c1=2, c2=10
    #   R3: (2,    10, 400) -> W2 continue
    #
    # Expected:
    #   W1: cnt=2, sum=300
    #   W2: cnt=2, sum=700
    #
    # P1-2 bug (query): shouldSplitDeferredPartialStandalone does NOT
    # check stateKeysAllDefined, so query incorrectly standalone-splits
    # R1 even though old window state is still incomplete.
    # Buggy query result: 3 windows (cnt=1, cnt=1, cnt=2).
    # Stream path (has allDefined guard) is correct.

    def do_query_extend2_front_only_undef(self):
        """Query baseline: EXTEND(2) front-only with undefined old column."""
        print("query ext2-front-undef ........... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_front_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext2_front_q values
            ('2025-10-01 18:00:00', NULL, 10, 100),
            ('2025-10-01 18:00:01', 5,  NULL, 200),
            ('2025-10-01 18:00:02', 2,    10, 300),
            ('2025-10-01 18:00:03', 2,    10, 400)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), sum(v), c1, c2 from ntb_ext2_front_q state_window(c1, c2) extend(2) order by _wstart",
            show=True,
        )
        tdSql.checkRows(2)
        # W1: R0+R1 (R1 fills undef c1, stays in old window), cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 18:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 18:00:01.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        tdSql.checkData(0, 4, 5)
        tdSql.checkData(0, 5, 10)
        # W2: R2+R3, cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 18:00:01.001")
        tdSql.checkData(1, 1, "2025-10-01 18:00:03.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 10)

        print("query ext2-front-undef ........... [passed]\n")

    def do_stream_extend2_front_only_undef_realtime(self):
        """Realtime stream: must match query ext2-front-undef.

        Stream path correctly produces 2 windows (has allDefined guard).
        """
        print("stream ext2-front-undef rt ....... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_front_rt "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_ext2_front_rt "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext2_front_rt "
            "stream_options(fill_history) "
            "into res_ext2_front_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext2_front_rt")

        tdSql.execute("""insert into ntb_ext2_front_rt values
            ('2025-10-01 18:00:00', NULL, 10, 100),
            ('2025-10-01 18:00:01', 5,  NULL, 200),
            ('2025-10-01 18:00:02', 2,    10, 300),
            ('2025-10-01 18:00:03', 2,    10, 400),
            ('2025-10-02 18:00:00', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_ext2_front_rt "
            "where wstart < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext2_front_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        # W1: R0+R1, cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 18:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: R2+R3, cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 18:00:01.001")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)

        print("stream ext2-front-undef rt ....... [passed]\n")

    def do_stream_extend2_front_only_undef_history(self):
        """History stream: must match query ext2-front-undef.

        Data inserted before stream creation to exercise fill_history.
        """
        print("stream ext2-front-undef hist ..... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_front_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext2_front_hist values
            ('2025-10-01 18:00:00', NULL, 10, 100),
            ('2025-10-01 18:00:01', 5,  NULL, 200),
            ('2025-10-01 18:00:02', 2,    10, 300),
            ('2025-10-01 18:00:03', 2,    10, 400),
            ('2025-10-02 18:00:00', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_ext2_front_hist "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext2_front_hist "
            "stream_options(fill_history) "
            "into res_ext2_front_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext2_front_hist")

        self.wait_result_count(
            "select count(*) from res_ext2_front_hist "
            "where wend < '2025-10-02'",
            2,
        )
        tdSql.query(
            "select wstart, wend, cnt, sum_v "
            "from res_ext2_front_hist "
            "where wend < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(2)
        # W1: R0+R1, cnt=2, sum=300
        tdSql.checkData(0, 0, "2025-10-01 18:00:00.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 300)
        # W2: R2+R3, cnt=2, sum=700
        tdSql.checkData(1, 0, "2025-10-01 18:00:01.001")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 700)

        print("stream ext2-front-undef hist ..... [passed]\n")

    # ================================================================
    # Scenario 3: EXTEND(2) - front-only deferred group with internal all-NULL
    # ================================================================
    #
    # Data (c1, c2, v):
    #   R0: (1,   10, 100) -> W1 open, old state fully defined
    #   R1: (1, NULL, 200) -> deferred partial-NULL, front-compatible
    #   R2: (NULL,NULL,300)-> all-NULL row inside the same deferred group
    #   R3: (1, NULL, 400) -> same deferred state key as R1
    #   R4: (2,   10, 500) -> cut, R1-R3 should standalone as one group
    #   R5: (2,   10, 600) -> W3 continue
    #
    # Expected:
    #   W1: cnt=1, sum=100
    #   W2: cnt=3, sum=900  (R1+R2+R3)
    #   W3: cnt=2, sum=1100

    def do_query_extend2_front_only_internal_allnull(self):
        """Query baseline: standalone deferred group includes internal all-NULL rows."""
        print("query ext2-front-internal-null ... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_internal_q "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext2_internal_q values
            ('2025-10-01 19:00:00.000', 1,    10, 100),
            ('2025-10-01 19:00:00.001', 1,  NULL, 200),
            ('2025-10-01 19:00:00.002', NULL,NULL, 300),
            ('2025-10-01 19:00:00.003', 1,  NULL, 400),
            ('2025-10-01 19:00:00.004', 2,    10, 500),
            ('2025-10-01 19:00:00.005', 2,    10, 600)
        """)

        tdSql.query(
            "select _wstart, _wend, count(*), sum(v) from ntb_ext2_internal_q state_window(c1, c2) extend(2) order by _wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 100)
        tdSql.checkData(1, 0, "2025-10-01 19:00:00.001")
        tdSql.checkData(1, 1, "2025-10-01 19:00:00.003")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 900)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 1100)

        print("query ext2-front-internal-null ... [passed]\n")

    def do_stream_extend2_front_only_internal_allnull_realtime(self):
        """Realtime stream: standalone deferred group includes internal all-NULL rows."""
        print("stream ext2-front-internal rt ... [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_internal_rt "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute(
            "create stream s_ext2_internal_rt "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext2_internal_rt "
            "stream_options(fill_history) "
            "into res_ext2_internal_rt as "
            "select _twstart wstart, _twend wend, _twrownum rownum, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext2_internal_rt")

        tdSql.execute("""insert into ntb_ext2_internal_rt values
            ('2025-10-01 19:00:00.000', 1,    10, 100),
            ('2025-10-01 19:00:00.001', 1,  NULL, 200),
            ('2025-10-01 19:00:00.002', NULL,NULL, 300),
            ('2025-10-01 19:00:00.003', 1,  NULL, 400),
            ('2025-10-01 19:00:00.004', 2,    10, 500),
            ('2025-10-01 19:00:00.005', 2,    10, 600),
            ('2025-10-02 19:00:00.000', 9,    99, 9999)
        """)

        self.wait_result_count(
            "select count(*) from res_ext2_internal_rt "
            "where wstart < '2025-10-02'",
            3,
        )
        tdSql.query(
            "select wstart, wend, rownum, cnt, sum_v "
            "from res_ext2_internal_rt "
            "where wstart < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 100)
        tdSql.checkData(1, 0, "2025-10-01 19:00:00.001")
        tdSql.checkData(1, 1, "2025-10-01 19:00:00.003")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 3)
        tdSql.checkData(1, 4, 900)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 1100)

        print("stream ext2-front-internal rt ... [passed]\n")

    def do_stream_extend2_front_only_internal_allnull_history(self):
        """History stream: standalone deferred group includes internal all-NULL rows."""
        print("stream ext2-front-internal hist . [start]")
        tdSql.execute("use test_sw_defer")
        tdSql.execute(
            "create table ntb_ext2_internal_hist "
            "(ts timestamp, c1 int, c2 int, v int)"
        )
        tdSql.execute("""insert into ntb_ext2_internal_hist values
            ('2025-10-01 19:00:00.000', 1,    10, 100),
            ('2025-10-01 19:00:00.001', 1,  NULL, 200),
            ('2025-10-01 19:00:00.002', NULL,NULL, 300),
            ('2025-10-01 19:00:00.003', 1,  NULL, 400),
            ('2025-10-01 19:00:00.004', 2,    10, 500),
            ('2025-10-01 19:00:00.005', 2,    10, 600),
            ('2025-10-02 19:00:00.000', 9,    99, 9999)
        """)

        tdSql.execute(
            "create stream s_ext2_internal_hist "
            "state_window(c1, c2) extend(2) "
            "from ntb_ext2_internal_hist "
            "stream_options(fill_history) "
            "into res_ext2_internal_hist as "
            "select _twstart wstart, _twend wend, _twrownum rownum, "
            "count(*) cnt, sum(v) sum_v from %%trows;"
        )
        tdStream.checkStreamStatus("s_ext2_internal_hist")

        self.wait_result_count(
            "select count(*) from res_ext2_internal_hist "
            "where wend < '2025-10-02'",
            3,
        )
        tdSql.query(
            "select wstart, wend, rownum, cnt, sum_v "
            "from res_ext2_internal_hist "
            "where wend < '2025-10-02' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 100)
        tdSql.checkData(1, 0, "2025-10-01 19:00:00.001")
        tdSql.checkData(1, 1, "2025-10-01 19:00:00.003")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 3)
        tdSql.checkData(1, 4, 900)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 1100)

        print("stream ext2-front-internal hist . [passed]\n")

    def do_query_null_absorb_single_vs_multi_col(self):
        """Leading/trailing all-NULL rows should be dropped.

        Partial-NULL rows are still valid state rows and may open a window.

        Data (v1, v2):
          Row 0: (NULL, NULL)
          Row 1: (1,    NULL)
          Row 2: (NULL, 1)
          Row 3: (1,    1)
          Row 4: (1,    2)
          Row 5: (1,    NULL)
          Row 6: (NULL, 2)
          Row 7: (NULL, NULL)

        state_window(v1): 1 window, cnt=5, v1=1
          head/tail all-NULL rows dropped, internal NULL row kept

        state_window(v2): 2 windows
          W1: rows 2-3, cnt=2, v2=1
          W2: rows 4-6, cnt=3, v2=2  (internal NULL kept)

        state_window(v1, v2): 2 windows
          W1: rows 1-3, cnt=3  (row 1 partial-NULL opens window)
          W2: rows 4-6, cnt=3  (row 6 partial-NULL stays in window)

        state_window(v2, v1): same as (v1, v2)
        """
        print("query edge-null drop single vs multi [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_absorb "
            "(ts timestamp, v1 int, v2 int)"
        )
        tdSql.execute("""insert into ntb_absorb values
            ('2026-04-28 10:26:07.610', NULL, NULL),
            ('2026-04-28 10:26:11.152', 1,    NULL),
            ('2026-04-28 10:26:15.323', NULL, 1),
            ('2026-04-28 10:26:18.275', 1,    1),
            ('2026-04-28 10:26:25.196', 1,    2),
            ('2026-04-28 10:26:27.000', 1,    NULL),
            ('2026-04-28 10:26:28.618', NULL, 2),
            ('2026-04-28 10:26:31.190', NULL, NULL)
        """)

        # state_window(v1): 1 window, cnt=5
        tdSql.query(
            "select _wstart, _wend, count(*), v1 "
            "from ntb_absorb state_window(v1) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-04-28 10:26:11.152")
        tdSql.checkData(0, 1, "2026-04-28 10:26:27.000")
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 1)

        # state_window(v2): 2 windows
        tdSql.query(
            "select _wstart, _wend, count(*), v2 "
            "from ntb_absorb state_window(v2) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-04-28 10:26:15.323")
        tdSql.checkData(0, 1, "2026-04-28 10:26:18.275")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2026-04-28 10:26:25.196")
        tdSql.checkData(1, 1, "2026-04-28 10:26:28.618")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 2)

        # state_window(v1, v2): 2 windows, partial-NULL can start window
        tdSql.query(
            "select _wstart, _wend, count(*), v1, v2 "
            "from ntb_absorb state_window(v1, v2) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-04-28 10:26:11.152")
        tdSql.checkData(0, 1, "2026-04-28 10:26:18.275")
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(1, 0, "2026-04-28 10:26:25.196")
        tdSql.checkData(1, 1, "2026-04-28 10:26:28.618")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(1, 4, 2)

        # state_window(v2, v1): same result
        tdSql.query(
            "select _wstart, _wend, count(*), v2, v1 "
            "from ntb_absorb state_window(v2, v1) "
            "order by _wstart",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-04-28 10:26:11.152")
        tdSql.checkData(0, 1, "2026-04-28 10:26:18.275")
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(1, 0, "2026-04-28 10:26:25.196")
        tdSql.checkData(1, 1, "2026-04-28 10:26:28.618")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 1)

        print("query edge-null drop single vs multi [passed]\n")

    # ================================================================
    # Stream null-absorb: DEFAULT mode head/tail NULL absorption
    # ================================================================

    def do_stream_null_absorb_realtime(self):
        """Realtime stream: head NULLs at the edge should be dropped.

        Data (v1):
          R0: NULL  → head NULL, dropped
          R1: NULL  → head NULL, dropped
          R2: 1     → open window
          R3: 1     → same
          R4: 1     → same
          R5: 9     → sentinel CUT

        Expected: W1 cnt=3, wstart=R2.ts
        """
        print("stream null-drop realtime ........ [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_absorb_rt "
            "(ts timestamp, v1 int)"
        )
        tdSql.execute(
            "create stream s_absorb_rt "
            "state_window(v1) "
            "from ntb_absorb_rt "
            "stream_options(fill_history) "
            "into res_absorb_rt as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt from %%trows;"
        )
        tdStream.checkStreamStatus("s_absorb_rt")

        tdSql.execute("""insert into ntb_absorb_rt values
            ('2026-04-28 10:26:00.000', NULL),
            ('2026-04-28 10:26:01.000', NULL),
            ('2026-04-28 10:26:02.000', 1),
            ('2026-04-28 10:26:03.000', 1),
            ('2026-04-28 10:26:04.000', 1),
            ('2026-04-29 00:00:00.000', 9)
        """)

        self.wait_result_count(
            "select count(*) from res_absorb_rt "
            "where wstart < '2026-04-29'",
            1,
        )
        tdSql.query(
            "select wstart, wend, cnt "
            "from res_absorb_rt "
            "where wstart < '2026-04-29' "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-04-28 10:26:02.000")
        tdSql.checkData(0, 1, "2026-04-28 10:26:04.000")
        tdSql.checkData(0, 2, 3)

        print("stream null-drop realtime ........ [passed]\n")

    def do_stream_null_absorb_history(self):
        """History stream: head + tail NULLs at the edge should be dropped.

        Data (v1):
          R0: NULL  → head NULL, dropped
          R1: NULL  → head NULL, dropped
          R2: 1     → open window
          R3: 1     → same
          R4: 1     → same
          R5: NULL  → tail NULL, dropped
          R6: NULL  → tail NULL, dropped

        allTableProcessed=true → trailing NULLs are discarded
        Expected: W1 cnt=3, wstart=R2.ts
        """
        print("stream null-drop history ......... [start]")
        tdSql.execute("use test_sw_null_reg")
        tdSql.execute(
            "create table ntb_absorb_hist "
            "(ts timestamp, v1 int)"
        )
        tdSql.execute("""insert into ntb_absorb_hist values
            ('2026-04-28 10:26:00.000', NULL),
            ('2026-04-28 10:26:01.000', NULL),
            ('2026-04-28 10:26:02.000', 1),
            ('2026-04-28 10:26:03.000', 1),
            ('2026-04-28 10:26:04.000', 1),
            ('2026-04-28 10:26:05.000', NULL),
            ('2026-04-28 10:26:06.000', NULL)
        """)

        tdSql.execute(
            "create stream s_absorb_hist "
            "state_window(v1) "
            "from ntb_absorb_hist "
            "stream_options(fill_history) "
            "into res_absorb_hist as "
            "select _twstart wstart, _twend wend, "
            "count(*) cnt from %%trows;"
        )
        tdStream.checkStreamStatus("s_absorb_hist")

        self.wait_result_count(
            "select count(*) from res_absorb_hist",
            1,
        )
        tdSql.query(
            "select wstart, wend, cnt "
            "from res_absorb_hist "
            "order by wstart",
            show=True,
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-04-28 10:26:02.000")
        tdSql.checkData(0, 1, "2026-04-28 10:26:04.000")
        tdSql.checkData(0, 2, 3)

        print("stream null-drop history ......... [passed]\n")
