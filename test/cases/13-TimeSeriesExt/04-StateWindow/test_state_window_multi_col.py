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

from new_test_framework.utils import tdLog, tdSql, etool, tdStream, StreamItem


class TestStateWindowMultiCol:
    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to execute {__file__}")

    def test_state_window_multi_col(self):
        """Multi-column state window tests (basic query, NULL, EXTEND, ZEROTH, error, expr, stream)

        1. Q-1~Q-5: multi-col basic queries with different type combos, aggregations, pseudo columns
        2. Q-9~Q-10: multi-col with partition by tbname, GROUP_KEY rewrite
        3. N-1~N-3: multi-col NULL handling (single col null, both null)
        4. E-1~E-3: multi-col + EXTEND(0/1/2)
        5. Z-1: multi-col ZEROTH_STATE with all cols specified
        6. ERR-6: single-col backward compatibility
        7. EX-1~EX-3: multi-col with expression keys (expr+col, dual-expr, col+expr)
        8. S-4: stream trigger with expression keys (expr+col, col+expr, dual-expr)

        Catalog:
            - TimeSeriesExt:StateWindow

        Since: v3.3.8.5

        Labels: state window, multi-col, ci

        Jira: None

        History:
            - 2026-04-02 Auto-generated from test plan
            - 2026-04-07 Add expression key tests for multi-col state window

        """
        self.do_prepare_data()
        self.do_multi_col_basic_query()
        self.do_multi_col_type_combos()
        self.do_multi_col_aggregations()
        self.do_multi_col_pseudo_columns()
        self.do_multi_col_partition_by()
        self.do_multi_col_group_key_rewrite()
        self.do_multi_col_null_handling()
        self.do_multi_col_extend()
        self.do_multi_col_zeroth()
        self.do_single_col_backward_compat()
        self.do_error_inputs()
        self.do_multi_col_expr_query()
        self.do_stream_compute_multi_col()
        self.do_stream_trigger_multi_col()
        self.do_stream_trigger_multi_col_zeroth()
        self.do_stream_trigger_multi_col_expr()

    # --- util ---

    def do_prepare_data(self):
        tdSql.execute("drop database if exists test_sw_mc")
        tdSql.execute("create database test_sw_mc keep 3650")
        tdSql.execute("use test_sw_mc")

        # normal table with int + bool columns
        tdSql.execute("create table ntb1 (ts timestamp, c_int int, c_bool bool, c_str varchar(20), v double)")
        tdSql.execute("""insert into ntb1 values
            ('2025-10-01 10:00:00', 1, true,  'a', 10.0),
            ('2025-10-01 10:00:01', 1, true,  'a', 20.0),
            ('2025-10-01 10:00:02', 1, false, 'a', 30.0),
            ('2025-10-01 10:00:03', 2, false, 'b', 40.0),
            ('2025-10-01 10:00:04', 2, false, 'b', 50.0),
            ('2025-10-01 10:00:05', 2, true,  'a', 60.0),
            ('2025-10-01 10:00:06', 1, true,  'a', 70.0),
            ('2025-10-01 10:00:07', 1, true,  'a', 80.0),
            ('2025-10-01 10:00:08', 1, true,  'b', 90.0),
            ('2025-10-01 10:00:09', 1, true,  'b', 100.0)
        """)

        # table with NULLs for null-handling tests
        tdSql.execute("create table ntb_null (ts timestamp, c1 int, c2 int, v int)")
        tdSql.execute("""insert into ntb_null values
            ('2025-10-01 10:00:00', 1,    10,   100),
            ('2025-10-01 10:00:01', null,  10,   200),
            ('2025-10-01 10:00:02', 1,    10,   300),
            ('2025-10-01 10:00:03', 1,    null,  400),
            ('2025-10-01 10:00:04', 1,    20,   500),
            ('2025-10-01 10:00:05', null,  null,  600),
            ('2025-10-01 10:00:06', 2,    20,   700),
            ('2025-10-01 10:00:07', 2,    20,   800),
            ('2025-10-01 10:00:08', 1,    10,   900)
        """)

        # super table for partition-by tests
        tdSql.execute("create table stb1 (ts timestamp, c_int int, c_bool bool, v double) tags (gid int)")
        tdSql.execute("create table ctb1 using stb1 tags(1)")
        tdSql.execute("create table ctb2 using stb1 tags(2)")
        tdSql.execute("""insert into ctb1 values
            ('2025-10-01 10:00:00', 1, true,  10.0),
            ('2025-10-01 10:00:01', 1, true,  20.0),
            ('2025-10-01 10:00:02', 2, false, 30.0),
            ('2025-10-01 10:00:03', 2, false, 40.0),
            ('2025-10-01 10:00:04', 1, true,  50.0)
        """)
        tdSql.execute("""insert into ctb2 values
            ('2025-10-01 10:00:00', 3, true,  100.0),
            ('2025-10-01 10:00:01', 3, true,  200.0),
            ('2025-10-01 10:00:02', 3, false, 300.0)
        """)

        # single-col backward compat table
        tdSql.execute("create table ntb_compat (ts timestamp, s int, v int)")
        tdSql.execute("""insert into ntb_compat values
            ('2025-10-01 10:00:00', 1, 10),
            ('2025-10-01 10:00:01', 1, 20),
            ('2025-10-01 10:00:02', 2, 30),
            ('2025-10-01 10:00:03', 2, 40),
            ('2025-10-01 10:00:04', 1, 50)
        """)

        print("prepare data ......................... [passed]")

    # --- impl ---

    def do_multi_col_basic_query(self):
        # Q-1: dual-col STATE_WINDOW(c_int, c_bool) basic query
        # ntb1 data: (1,T)(1,T)(1,F)(2,F)(2,F)(2,T)(1,T)(1,T)(1,T)(1,T)
        # Windows: (1,T)x2, (1,F)x1, (2,F)x2, (2,T)x1, (1,T)x2, (1,T)x2
        # Wait - c_str changes at row 8: (1,T,'a') vs (1,T,'b')
        # state_window(c_int, c_bool): groups by (c_int, c_bool) only
        # (1,T)x2 | (1,F)x1 | (2,F)x2 | (2,T)x1 | (1,T)x4
        tdSql.query("select _wstart, _wend, count(*), c_int, c_bool from ntb1 state_window(c_int, c_bool)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)   # (1, true) rows 0-1
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, True)
        tdSql.checkData(1, 2, 1)   # (1, false) row 2
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(1, 4, False)
        tdSql.checkData(2, 2, 2)   # (2, false) rows 3-4
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, False)
        tdSql.checkData(3, 2, 1)   # (2, true) row 5
        tdSql.checkData(3, 3, 2)
        tdSql.checkData(3, 4, True)
        tdSql.checkData(4, 2, 4)   # (1, true) rows 6-9
        tdSql.checkData(4, 3, 1)
        tdSql.checkData(4, 4, True)

        print("Q-1: basic dual-col query ............. [passed]")

    def do_multi_col_type_combos(self):
        # Q-2: STATE_WINDOW(c_int, c_str) -- int + varchar combo
        # ntb1: (1,'a')(1,'a')(1,'a')(2,'b')(2,'b')(2,'a')(1,'a')(1,'a')(1,'b')(1,'b')
        # Windows: (1,a)x3 | (2,b)x2 | (2,a)x1 | (1,a)x2 | (1,b)x2
        tdSql.query("select _wstart, _wend, count(*), c_int, c_str from ntb1 state_window(c_int, c_str)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, "a")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, "b")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, "a")
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, "a")
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 1)
        tdSql.checkData(4, 4, "b")

        # Q-3: three-col STATE_WINDOW(c_int, c_bool, c_str)
        # ntb1: (1,T,a)(1,T,a)(1,F,a)(2,F,b)(2,F,b)(2,T,a)(1,T,a)(1,T,a)(1,T,b)(1,T,b)
        # Windows: (1,T,a)x2 | (1,F,a)x1 | (2,F,b)x2 | (2,T,a)x1 | (1,T,a)x2 | (1,T,b)x2
        tdSql.query("select _wstart, _wend, count(*), c_int, c_bool, c_str from ntb1 state_window(c_int, c_bool, c_str)", show=True)
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(5, 2, 2)

        print("Q-2,Q-3: type combos ................. [passed]")

    def do_multi_col_aggregations(self):
        # Q-4: multi-col + aggregation functions
        tdSql.query("select count(*), sum(v), avg(v), min(v), max(v), first(v), last(v) from ntb1 state_window(c_int, c_bool)", show=True)
        tdSql.checkRows(5)
        # Window 0: (1,T) rows 0-1, v=10,20
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 30.0)
        tdSql.checkData(0, 2, 15.0)
        tdSql.checkData(0, 3, 10.0)
        tdSql.checkData(0, 4, 20.0)
        tdSql.checkData(0, 5, 10.0)
        tdSql.checkData(0, 6, 20.0)

        print("Q-4: multi-col aggregations .......... [passed]")

    def do_multi_col_pseudo_columns(self):
        # Q-5: multi-col + pseudo columns _wstart, _wend, _wduration
        tdSql.query("select _wstart, _wend, _wduration, count(*) from ntb1 state_window(c_int, c_bool)", show=True)
        tdSql.checkRows(5)
        # Window 0: 10:00:00 - 10:00:01, duration=1000ms
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 10:00:01.000")
        tdSql.checkData(0, 2, 1000)
        tdSql.checkData(0, 3, 2)
        # Window 4: 10:00:06 - 10:00:09, duration=3000ms
        tdSql.checkData(4, 0, "2025-10-01 10:00:06.000")
        tdSql.checkData(4, 1, "2025-10-01 10:00:09.000")
        tdSql.checkData(4, 2, 3000)
        tdSql.checkData(4, 3, 4)

        print("Q-5: pseudo columns .................. [passed]")

    def do_multi_col_partition_by(self):
        # Q-9: multi-col + partition by tbname (super table)
        tdSql.query(
            "select _wstart, _wend, count(*), c_int, c_bool, tbname "
            "from stb1 partition by tbname state_window(c_int, c_bool) "
            "order by tbname, _wstart",
            show=True,
        )
        # ctb1: (1,T)x2 | (2,F)x2 | (1,T)x1 → 3 windows
        # ctb2: (3,T)x2 | (3,F)x1 → 2 windows
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 5, "ctb1")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 5, "ctb1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 5, "ctb1")
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 5, "ctb2")
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 5, "ctb2")

        print("Q-9: partition by tbname .............. [passed]")

    def do_multi_col_group_key_rewrite(self):
        # Q-10: SELECT c_int, c_bool in state_window(c_int, c_bool) → GROUP_KEY rewrite
        tdSql.query("select c_int, c_bool, count(*) from ntb1 state_window(c_int, c_bool)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, False)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, False)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(4, 1, True)
        tdSql.checkData(4, 2, 4)

        print("Q-10: GROUP_KEY rewrite ............... [passed]")

    def do_multi_col_null_handling(self):
        # N-1~N-3: NULL handling in multi-col state_window
        # ntb_null: (1,10)(null,10)(1,10)(1,null)(1,20)(null,null)(2,20)(2,20)(1,10)
        # Null rows between same-state rows ARE counted (wrapped/absorbed).
        # Row 0 (1,10), Row 1 (null,10)=null absorbed, Row 2 (1,10)=same → W1 count=3
        # Row 3 (1,null)=null between different states, not absorbed
        # Row 4 (1,20) → W2 count=1
        # Row 5 (null,null)=null between different states
        # Row 6,7 (2,20) → W3 count=2
        # Row 8 (1,10) → W4 count=1
        tdSql.query("select _wstart, _wend, count(*), c1, c2 from ntb_null state_window(c1, c2)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 3)   # (1,10) rows 0,1,2 (row 1 null absorbed)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 10)
        tdSql.checkData(1, 2, 1)   # (1,20) row 4
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(1, 4, 20)
        tdSql.checkData(2, 2, 2)   # (2,20) rows 6,7
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 20)
        tdSql.checkData(3, 2, 1)   # (1,10) row 8
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, 10)

        # Cross-validate with single-col
        # state_window(c1): rows 0(1),1(null),2(1),3(1),4(1) → W1 with null absorbed, then 5(null),6(2),7(2) → W2, 8(1) → W3
        tdSql.query("select count(*), c1 from ntb_null state_window(c1)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)   # c1=1: rows 0-4 (row 1 null absorbed)
        tdSql.checkData(1, 0, 2)   # c1=2: rows 6,7
        tdSql.checkData(2, 0, 1)   # c1=1: row 8

        # state_window(c2): rows 0(10),1(10),2(10) → W1, 3(null) between, 4(20),5(null),6(20),7(20) → W2 with null absorbed, 8(10) → W3
        tdSql.query("select count(*), c2 from ntb_null state_window(c2)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)   # c2=10: rows 0,1,2
        tdSql.checkData(1, 0, 4)   # c2=20: rows 4,5,6,7 (row 5 null absorbed)
        tdSql.checkData(2, 0, 1)   # c2=10: row 8

        print("N-1~N-3: null handling ............... [passed]")

    def do_multi_col_extend(self):
        # E-1: dual-col + EXTEND(0) -- same as no extend
        tdSql.query("select _wstart, _wend, count(*), c_int, c_bool from ntb1 state_window(c_int, c_bool) extend(0)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 10:00:01.000")
        tdSql.checkData(0, 2, 2)

        # E-2: EXTEND(1) -- tail extends
        tdSql.query("select _wstart, _wend, count(*), c_int, c_bool from ntb1 state_window(c_int, c_bool) extend(1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 10:00:01.999")
        tdSql.checkData(0, 2, 2)
        # last window: no next window, keeps original end
        tdSql.checkData(4, 0, "2025-10-01 10:00:06.000")
        tdSql.checkData(4, 1, "2025-10-01 10:00:09.000")
        tdSql.checkData(4, 2, 4)

        # E-3: EXTEND(2) -- head also extends
        tdSql.query("select _wstart, _wend, count(*), c_int, c_bool from ntb1 state_window(c_int, c_bool) extend(2)", show=True)
        tdSql.checkRows(5)
        # first window: no previous, keeps original start
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 10:00:01.000")
        tdSql.checkData(0, 2, 2)
        # second window: extends head to prev_end + 1ms
        tdSql.checkData(1, 0, "2025-10-01 10:00:01.001")
        tdSql.checkData(1, 1, "2025-10-01 10:00:02.000")
        tdSql.checkData(1, 2, 1)

        print("E-1~E-3: extend ...................... [passed]")

    def do_multi_col_zeroth(self):
        # Z-1: all cols specified zeroth values
        # Use ntb_null, state_window(c1, c2) zeroth_state(1, 10)
        # AND semantics: ALL cols must match their zeroth value for the window to be filtered
        # Windows: W1(1,10) W2(1,20) W3(2,20) W4(1,10)
        # W1: c1==1 AND c2==10 → filtered
        # W2: c1==1 but c2!=10 → kept
        # W3: c1!=1 → kept
        # W4: c1==1 AND c2==10 → filtered
        tdSql.query(
            "select _wstart, _wend, count(*), c1, c2 from ntb_null "
            "state_window(c1, c2) extend(0) zeroth_state(1, 10)",
            show=True,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 20)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 20)

        print("Z-1: zeroth_state .................... [passed]")

    def do_single_col_backward_compat(self):
        # ERR-6: single-col state_window still works correctly
        tdSql.query("select _wstart, _wend, count(*), s from ntb_compat state_window(s)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-01 10:00:00.000")
        tdSql.checkData(0, 1, "2025-10-01 10:00:01.000")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2025-10-01 10:00:02.000")
        tdSql.checkData(1, 1, "2025-10-01 10:00:03.000")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, "2025-10-01 10:00:04.000")
        tdSql.checkData(2, 1, "2025-10-01 10:00:04.000")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, 1)

        print("ERR-6: backward compat ............... [passed]")

    def do_error_inputs(self):
        # ERR-1: float/double type should error
        tdSql.execute("create table if not exists ntb_err (ts timestamp, c_float float, c_double double, c_int int)")
        tdSql.execute("insert into ntb_err values('2025-10-01 10:00:00', 1.1, 2.2, 1)")
        tdSql.error(
            "select count(*) from ntb_err state_window(c_float)",
            expectErrInfo="Only support STATE_WINDOW on integer/bool/varchar column",
            show=True,
        )
        tdSql.error(
            "select count(*) from ntb_err state_window(c_double)",
            expectErrInfo="Only support STATE_WINDOW on integer/bool/varchar column",
            show=True,
        )

        # ERR-2: timestamp type should error
        tdSql.error(
            "select count(*) from ntb_err state_window(ts)",
            expectErrInfo="Only support STATE_WINDOW on integer/bool/varchar column",
            show=True,
        )

        print("ERR-1~ERR-2: error inputs ............ [passed]")

    def do_multi_col_expr_query(self):
        # EX-1: expression + column mix: STATE_WINDOW(CASE WHEN c_int > 1 THEN 1 ELSE 0 END, c_bool)
        # ntb1 data with expr(c_int>1 → 1 else 0):
        # Row 0: (0, T), Row 1: (0, T), Row 2: (0, F), Row 3: (1, F),
        # Row 4: (1, F), Row 5: (1, T), Row 6: (0, T), Row 7: (0, T),
        # Row 8: (0, T), Row 9: (0, T)
        # Windows: (0,T)x2 | (0,F)x1 | (1,F)x2 | (1,T)x1 | (0,T)x4
        tdSql.query(
            "select _wstart, _wend, count(*), "
            "case when c_int > 1 then 1 else 0 end as e1, c_bool "
            "from ntb1 state_window(case when c_int > 1 then 1 else 0 end, c_bool)",
            show=True,
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)  # (0, true) rows 0-1
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, True)
        tdSql.checkData(1, 2, 1)  # (0, false) row 2
        tdSql.checkData(1, 3, 0)
        tdSql.checkData(1, 4, False)
        tdSql.checkData(2, 2, 2)  # (1, false) rows 3-4
        tdSql.checkData(2, 3, 1)
        tdSql.checkData(2, 4, False)
        tdSql.checkData(3, 2, 1)  # (1, true) row 5
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(3, 4, True)
        tdSql.checkData(4, 2, 4)  # (0, true) rows 6-9
        tdSql.checkData(4, 3, 0)
        tdSql.checkData(4, 4, True)

        # EX-2: two expressions: STATE_WINDOW(c_int > 1, c_bool::int)
        # same grouping as EX-1 since bool→int maps true→1 false→0
        tdSql.query(
            "select _wstart, _wend, count(*) "
            "from ntb1 state_window(case when c_int > 1 then 1 else 0 end, "
            "case when c_bool then 1 else 0 end)",
            show=True,
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 4)

        # EX-3: column + expression (reversed order, expr at position 1)
        # STATE_WINDOW(c_bool, CASE WHEN c_int > 1 THEN 1 ELSE 0 END)
        # Row 0: (T, 0), Row 1: (T, 0), Row 2: (F, 0), Row 3: (F, 1),
        # Row 4: (F, 1), Row 5: (T, 1), Row 6: (T, 0), Row 7: (T, 0),
        # Row 8: (T, 0), Row 9: (T, 0)
        # Windows: (T,0)x2 | (F,0)x1 | (F,1)x2 | (T,1)x1 | (T,0)x4
        tdSql.query(
            "select _wstart, _wend, count(*) "
            "from ntb1 state_window(c_bool, case when c_int > 1 then 1 else 0 end)",
            show=True,
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 4)

        print("EX-1~EX-3: multi-col expr query ...... [passed]")

    def do_stream_compute_multi_col(self):
        # S-1: stream compute with multi-col state_window (count_window trigger + inner state_window)
        tdSql.execute("use test_sw_mc")
        try:
            tdStream.createSnode()
        except Exception:
            tdLog.info("snode already exists, skip creation")

        # fresh table so stream sees all data (no fill_history needed)
        tdSql.execute("create table if not exists ntb_sc (ts timestamp, c1 int, c2 bool, v double)")

        streams: list[StreamItem] = []
        stream = StreamItem(
            id=0,
            stream='''create stream smc_comp0 count_window(5) from ntb_sc
                        into res_smc_comp0 as
                        select _wstart wstart, _wend wend, count(*) cnt,
                        c1, c2
                        from ntb_sc where ts >= _twstart and ts <= _twend
                        state_window(c1, c2)''',
            res_query='''select wstart, wend, cnt, c1, c2 from res_smc_comp0
                        where wend <= "2025-10-01 10:00:04.000"''',
            exp_query='''select _wstart, _wend, count(*), c1, c2 from ntb_sc
                        where ts >= "2025-10-01 10:00:00.000" and ts <= "2025-10-01 10:00:04.000"
                        state_window(c1, c2)''',
        )
        streams.append(stream)

        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data after stream creation
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:00', 1, true,  10.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:01', 1, true,  20.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:02', 2, false, 30.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:03', 2, false, 40.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:04', 1, true,  50.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:05', 3, true,  60.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:06', 3, true,  70.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:07', 3, false, 80.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:08', 1, true,  90.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:09', 1, true, 100.0)")
        tdSql.execute("insert into ntb_sc values('2025-10-01 10:00:10', 2, true, 110.0)")

        for s in streams:
            s.checkResults()

        print("S-1: stream compute multi-col ........ [passed]")

    def do_stream_trigger_multi_col(self):
        # S-2: stream trigger with multi-col state_window
        # state_window(c_int, c_bool) is the stream's own trigger
        tdSql.execute("use test_sw_mc")

        # create a fresh table for stream trigger test to avoid existing data conflicts
        tdSql.execute("create table if not exists ntb_st (ts timestamp, c1 int, c2 bool, v double)")

        streams: list[StreamItem] = []
        stream = StreamItem(
            id=1,
            stream='''create stream smc_trig0 state_window(c1, c2) extend(0) from ntb_st
                        stream_options(fill_history) into res_smc_trig0 as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from ntb_st where ts >= _twstart and ts <= _twend''',
            res_query='''select wstart, wdur, wend, cnt, sum_v from res_smc_trig0
                        where wstart <= "2025-10-02 10:00:09.000"''',
            exp_query='''select _wstart wstart, _wduration wdur, _wend wend, cnt, sum_v from (
                        select _wstart, _wduration, _wend, count(*) cnt, sum(v) sum_v
                        from ntb_st state_window(c1, c2)
                        ) t where _wstart <= "2025-10-02 10:00:09.000"''',
        )
        streams.append(stream)

        stream = StreamItem(
            id=2,
            stream='''create stream smc_trig1 state_window(c1, c2) extend(1) from ntb_st
                        stream_options(fill_history) into res_smc_trig1 as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from ntb_st where ts >= _twstart and ts <= _twend''',
            res_query='''select wstart, wdur, wend, cnt, sum_v from res_smc_trig1
                        where wstart <= "2025-10-02 10:00:09.000"''',
            exp_query='''select _wstart wstart, _wduration wdur, _wend wend, cnt, sum_v from (
                        select _wstart, _wduration, _wend, count(*) cnt, sum(v) sum_v
                        from ntb_st state_window(c1, c2) extend(1)
                        ) t where _wstart <= "2025-10-02 10:00:09.000"''',
        )
        streams.append(stream)

        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus('''smc_trig0''')
        tdStream.checkStreamStatus('''smc_trig1''')

        # insert data with multi-col state changes
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:00', 1, true,  10.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:01', 1, true,  20.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:02', 1, false, 30.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:03', 2, false, 40.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:04', 2, true,  50.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:05', null, null, 60.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:06', 2, true,  70.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:07', 1, true,  80.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:08', null, true, 90.0)")
        tdSql.execute("insert into ntb_st values('2025-10-02 10:00:09', 1, true,  100.0)")
        tdSql.execute("insert into ntb_st values('2025-10-03 10:00:00', 3, false, 110.0)")

        for s in streams:
            s.checkResults()

        print("S-2: stream trigger multi-col ........ [passed]")

    def do_stream_trigger_multi_col_zeroth(self):
        # S-3: stream trigger multi-col + ZEROTH_STATE
        # state_window(c1, c2) with zeroth_state filtering
        tdSql.execute("use test_sw_mc")

        tdSql.execute("create table if not exists ntb_stz (ts timestamp, c1 int, c2 bool, v double)")

        stream = StreamItem(
            id=3,
            stream='''create stream smc_trigz state_window(c1, c2) extend(0)
                        zeroth_state(1, true) from ntb_stz
                        stream_options(fill_history) into res_smc_trigz as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from %%trows;''',
            res_query='''select * from res_smc_trigz''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(v)
                        from ntb_stz where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(c1, c2) extend(0)
                        having(c1 != 1 or c2 != true)''',
        )

        stream.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute("""insert into ntb_stz values
            ('2025-10-02 10:00:00', 1, true,  10.0),
            ('2025-10-02 10:00:01', 1, true,  20.0),
            ('2025-10-02 10:00:02', 2, false, 30.0),
            ('2025-10-02 10:00:03', 2, false, 40.0),
            ('2025-10-02 10:00:04', 1, false, 50.0),
            ('2025-10-02 10:00:05', 1, true,  60.0),
            ('2025-10-02 10:00:06', 3, true,  70.0),
            ('2025-10-03 10:00:00', 9, false, 999.0)
        """, show=True)

        stream.checkResults()

        print("S-3: stream trigger multi-col zeroth . [passed]")

    def do_stream_trigger_multi_col_expr(self):
        # S-4: stream trigger with expression + column mix in state_window
        # This specifically tests the fix for multi-key expr slotId == -1 path
        tdSql.execute("use test_sw_mc")

        tdSql.execute("create table if not exists ntb_ste (ts timestamp, c1 int, c2 bool, v double)")

        # expr + column: state_window(CASE WHEN c1 > 1 THEN 1 ELSE 0 END, c2)
        streams: list[StreamItem] = []
        stream = StreamItem(
            id=4,
            stream='''create stream smc_expr0
                        state_window(case when c1 > 1 then 1 else 0 end, c2)
                        extend(0) from ntb_ste
                        stream_options(fill_history) into res_smc_expr0 as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from ntb_ste where ts >= _twstart and ts <= _twend''',
            res_query='''select wstart, wdur, wend, cnt, sum_v from res_smc_expr0
                        where wstart <= "2025-10-05 10:00:09.000"''',
            exp_query='''select _wstart wstart, _wduration wdur, _wend wend, cnt, sum_v from (
                        select _wstart, _wduration, _wend, count(*) cnt, sum(v) sum_v
                        from ntb_ste
                        state_window(case when c1 > 1 then 1 else 0 end, c2)
                        ) t where _wstart <= "2025-10-05 10:00:09.000"''',
        )
        streams.append(stream)

        # column + expr (reversed order)
        stream = StreamItem(
            id=5,
            stream='''create stream smc_expr1
                        state_window(c2, case when c1 > 1 then 1 else 0 end)
                        extend(0) from ntb_ste
                        stream_options(fill_history) into res_smc_expr1 as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from ntb_ste where ts >= _twstart and ts <= _twend''',
            res_query='''select wstart, wdur, wend, cnt, sum_v from res_smc_expr1
                        where wstart <= "2025-10-05 10:00:09.000"''',
            exp_query='''select _wstart wstart, _wduration wdur, _wend wend, cnt, sum_v from (
                        select _wstart, _wduration, _wend, count(*) cnt, sum(v) sum_v
                        from ntb_ste
                        state_window(c2, case when c1 > 1 then 1 else 0 end)
                        ) t where _wstart <= "2025-10-05 10:00:09.000"''',
        )
        streams.append(stream)

        # two expressions
        stream = StreamItem(
            id=6,
            stream='''create stream smc_expr2
                        state_window(case when c1 > 1 then 1 else 0 end,
                                     case when c2 then 1 else 0 end)
                        extend(0) from ntb_ste
                        stream_options(fill_history) into res_smc_expr2 as
                        select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt, sum(v) sum_v
                        from ntb_ste where ts >= _twstart and ts <= _twend''',
            res_query='''select wstart, wdur, wend, cnt, sum_v from res_smc_expr2
                        where wstart <= "2025-10-05 10:00:09.000"''',
            exp_query='''select _wstart wstart, _wduration wdur, _wend wend, cnt, sum_v from (
                        select _wstart, _wduration, _wend, count(*) cnt, sum(v) sum_v
                        from ntb_ste
                        state_window(case when c1 > 1 then 1 else 0 end,
                                     case when c2 then 1 else 0 end)
                        ) t where _wstart <= "2025-10-05 10:00:09.000"''',
        )
        streams.append(stream)

        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus('''smc_expr0''')
        tdStream.checkStreamStatus('''smc_expr1''')
        tdStream.checkStreamStatus('''smc_expr2''')

        # Insert data with patterns that exercise expression-based state grouping:
        # c1: 1,1,1,2,2,2,1,1,1,1  → expr(c1>1): 0,0,0,1,1,1,0,0,0,0
        # c2: T,T,F,F,F,T,T,T,T,T
        # (expr,c2): (0,T)(0,T)(0,F)(1,F)(1,F)(1,T)(0,T)(0,T)(0,T)(0,T)
        # Windows: (0,T)x2 | (0,F)x1 | (1,F)x2 | (1,T)x1 | (0,T)x4
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:00', 1, true,  10.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:01', 1, true,  20.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:02', 1, false, 30.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:03', 2, false, 40.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:04', 2, false, 50.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:05', 2, true,  60.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:06', 1, true,  70.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:07', 1, true,  80.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:08', 1, true,  90.0)")
        tdSql.execute("insert into ntb_ste values('2025-10-05 10:00:09', 1, true, 100.0)")
        # sentinel row to close the last window
        tdSql.execute("insert into ntb_ste values('2025-10-06 10:00:00', 9, false, 999.0)")

        for s in streams:
            s.checkResults()

        print("S-4: stream trigger multi-col expr ... [passed]")
