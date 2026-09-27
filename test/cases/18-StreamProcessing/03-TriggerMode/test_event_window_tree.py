# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
import pytest
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestEventWindowTree:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _query_until_rows(self, sql, rows, attempts=20):
        for _ in range(attempts):
            tdSql.query(sql)
            if tdSql.queryRows == rows:
                return
            time.sleep(0.5)
        tdSql.checkRows(rows)

    def _query_until_result(self, sql, rows, attempts=30):
        for _ in range(attempts):
            tdSql.query(sql)
            if tdSql.queryRows == len(rows):
                matched = True
                for row_idx, row in enumerate(rows):
                    for col_idx, value in enumerate(row):
                        if not tdSql.checkData(row_idx, col_idx, value, exit=False):
                            matched = False
                            break
                    if not matched:
                        break
                if matched:
                    return
            time.sleep(0.5)

        tdSql.query(sql)
        tdSql.checkRows(len(rows))
        for row_idx, row in enumerate(rows):
            for col_idx, value in enumerate(row):
                tdSql.checkData(row_idx, col_idx, value)

    def test_event_window_tree_paths(self):
        """EVENT_WINDOW nested START WITH emits stable condition paths.

        Switch between nested leaves and verify the envelope, group, and leaf
        condition paths and row counts emitted on window close.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-23 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_tree "
            "event_window(start with ((current >= 12, current >= 10), voltage < 215) end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_tree (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 11, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_tree order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 3),
                ("2025-01-01 00:00:00.000", "0", 2),
                ("2025-01-01 00:00:00.000", "0.0", 1),
                ("2025-01-01 00:00:01.000", "0.1", 1),
                ("2025-01-01 00:00:02.000", "1", 1),
            ],
        )

    def test_event_window_tree_compatibility(self):
        """Existing single-condition and flat sub-event streams remain legal.

        Create single-condition and flat sub-event streams without END WITH
        and verify that both trigger tasks reach the Running state.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-23 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_compat", vgroups=1)
        tdSql.execute("create table t (ts timestamp, c0 int, c1 int)")
        tdSql.execute(
            "create stream s_single event_window(start with c0 = 1) from t "
            "into r_single as select _twstart, count(*) from %%trows"
        )
        tdSql.execute(
            "create stream s_flat event_window(start with (c0 = 1, c1 = 1)) from t "
            "into r_flat as select _twstart, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()

    @pytest.mark.parametrize("fill_history", [False, True], ids=["realtime", "history"])
    @pytest.mark.parametrize("use_event_tree", [False, True], ids=["ordinary", "tree"])
    @pytest.mark.parametrize("explicit_end", [False, True], ids=["implicit_end", "explicit_end"])
    def test_event_window_tree_single_leaf_end_condition(self, fill_history, use_event_tree, explicit_end):
        """A single leaf closes on mismatch only when END WITH is omitted.

        Compare implicit and explicit closing in ordinary and event-tree paths,
        for realtime input and history replay, checking window bounds and counts.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7125293823

        History:
            - 2026-09-27 Created
        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_single_end", vgroups=1)
        tdSql.execute("create table t (ts timestamp, c0 int, c1 int)")
        inserts = [
            "insert into t values ('2025-01-01 00:00:00.000', 1, 0)",
            "insert into t values ('2025-01-01 00:00:01.000', 0, 0)",
            "insert into t values ('2025-01-01 00:00:02.000', 1, 0)",
            "insert into t values ('2025-01-01 00:00:03.000', 0, 0)",
            "insert into t values ('2025-01-01 00:00:04.000', 0, 1)",
        ]
        if fill_history:
            tdSql.executes(inserts)
        end_clause = " end with c1 = 1" if explicit_end else ""
        options = "stream_options(fill_history) " if fill_history else ""
        leaf_true_for = " true_for(count 1)" if use_event_tree else ""
        tdSql.execute(
            f"create stream s_single event_window(start with c0 = 1{leaf_true_for}{end_clause}) "
            f"from t {options}into r_single (ts, te, nrows, cnt) "
            "as select _twstart, _twend, _twrownum, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        if not fill_history:
            tdSql.executes(inserts)
        expected = (
            [("2025-01-01 00:00:00.000", "2025-01-01 00:00:04.000", 5, 5)]
            if explicit_end
            else [
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1, 1),
                ("2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1, 1),
            ]
        )
        self._query_until_result("select ts, te, nrows, cnt from r_single order by ts", expected)

    def test_event_window_tree_single_condition_close_reopens_from_next_row(self):
        """Single-condition event tree starts a fresh window after close.

        Insert successive START and END matches and verify that each closed
        single-condition window is followed by a new window with its own bounds.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_single_close", vgroups=1)
        tdSql.execute("create table t (ts timestamp, cint int, cuint int)")
        tdSql.execute(
            "create stream s_single "
            "event_window(start with cint > 1 end with cuint < 5) "
            "from t stream_options(event_type(window_close)) "
            "into r_single as select _twstart, _twend, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 0, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 1, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 2, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:03.000', 3, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:04.000', 4, 1)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:05.000', 0, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:06.000', 1, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:07.000', 2, 1)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:08.000', 2, 9)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:09.000', 2, 1)")
        self._query_until_result(
            "select * from r_single order by 1",
            [
                ("2025-01-01 00:00:02.000", "2025-01-01 00:00:04.000", 3),
                ("2025-01-01 00:00:07.000", "2025-01-01 00:00:07.000", 1),
                ("2025-01-01 00:00:08.000", "2025-01-01 00:00:09.000", 2),
            ],
        )

    def test_event_window_tree_global_true_for_does_not_delay_single_leaf_start(self):
        """Global true_for filters output but does not delay single leaf start.

        Feed windows of different durations and verify that the global duration
        filter preserves the first START row and suppresses short windows.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_global_tf", vgroups=1)
        tdSql.execute("create table t (ts timestamp, cint int, cuint int unsigned)")
        tdSql.execute(
            "create stream s_global_tf "
            "event_window(start with cint > 1 end with cuint < 5) true_for(3s) "
            "from t stream_options(event_type(window_close)) "
            "into r_global_tf as select first(_c0), last_row(_c0), _twrownum, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.executes(
            [
                "insert into t values ('2025-01-01 00:00:00.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:01.000', 1, 9)",
                "insert into t values ('2025-01-01 00:00:02.000', 1, 9)",
                "insert into t values ('2025-01-01 00:00:03.000', 1, 4)",
                "insert into t values ('2025-01-01 00:00:04.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:05.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:06.000', 2, 4)",
                "insert into t values ('2025-01-01 00:00:07.000', 3, 1)",
                "insert into t values ('2025-01-01 00:00:08.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:09.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:10.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:11.000', 4, 4)",
            ]
        )
        self._query_until_result(
            "select * from r_global_tf order by 1",
            [
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:03.000", 4, 4),
                ("2025-01-01 00:00:08.000", "2025-01-01 00:00:11.000", 4, 4),
            ],
        )

    def test_event_window_tree_history_global_true_for_does_not_delay_single_leaf_start(self):
        """History replay keeps global true_for separate from single leaf start.

        Replay existing rows with fill_history and verify that the global duration
        filter preserves the first START row and suppresses short windows.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_history_global_tf", vgroups=1)
        tdSql.execute("create table t (ts timestamp, cint int, cuint int unsigned)")
        tdSql.executes(
            [
                "insert into t values ('2025-01-01 00:00:00.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:01.000', 1, 9)",
                "insert into t values ('2025-01-01 00:00:02.000', 1, 9)",
                "insert into t values ('2025-01-01 00:00:03.000', 1, 4)",
                "insert into t values ('2025-01-01 00:00:04.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:05.000', 2, 9)",
                "insert into t values ('2025-01-01 00:00:06.000', 2, 4)",
                "insert into t values ('2025-01-01 00:00:07.000', 3, 1)",
                "insert into t values ('2025-01-01 00:00:08.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:09.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:10.000', 4, 9)",
                "insert into t values ('2025-01-01 00:00:11.000', 4, 4)",
            ]
        )
        tdSql.execute(
            "create stream s_history_global_tf "
            "event_window(start with cint > 1 end with cuint < 5) true_for(3s) "
            "from t stream_options(fill_history|event_type(window_close)) "
            "into r_history_global_tf as select first(_c0), last_row(_c0), _twrownum, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        self._query_until_result(
            "select * from r_history_global_tf order by 1",
            [
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:03.000", 4, 4),
                ("2025-01-01 00:00:08.000", "2025-01-01 00:00:11.000", 4, 4),
            ],
        )

    def test_event_window_tree_fill_history_matches_realtime_paths(self):
        """EVENT_WINDOW tree history replay matches realtime condition paths.

        Create the stream after inserting data and verify that history replay
        produces the expected envelope, group, and leaf paths and row counts.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_history", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 11, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        tdSql.execute(
            "create stream s_tree "
            "event_window(start with ((current >= 12, current >= 10), voltage < 215) end with voltage < 200) "
            "from t stream_options(fill_history|event_type(window_close)) "
            "into r_tree (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        self._query_until_result(
            "select ts, path, cnt from r_tree order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 3),
                ("2025-01-01 00:00:00.000", "0", 2),
                ("2025-01-01 00:00:00.000", "0.0", 1),
                ("2025-01-01 00:00:01.000", "0.1", 1),
                ("2025-01-01 00:00:02.000", "1", 1),
            ],
        )

    def test_event_window_tree_end_without_leaf_uses_last_leaf_row(self):
        """EVENT_WINDOW tree closes at the last descendant leaf row.

        Make END match a row that satisfies no START leaf and verify that the
        closing envelope and descendant windows exclude that final row.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_end", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_tree "
            "event_window(start with ((current >= 12, current >= 10), voltage > 215) end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_tree (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 210)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 11, 210)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_tree order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 2),
                ("2025-01-01 00:00:00.000", "0", 2),
                ("2025-01-01 00:00:00.000", "0.0", 1),
                ("2025-01-01 00:00:01.000", "0.1", 1),
            ],
        )

    def test_event_window_tree_leaf_true_for_resets_after_close(self):
        """Single leaf true_for streak is cleared when the window closes.

        Close two consecutive windows whose leaf requires two matching rows
        and verify that each window starts from a fresh leaf streak.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_leaf_tf_reset", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_leaf_tf_reset "
            "event_window(start with (current >= 10 true_for(count 2), current >= 100) end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_leaf_tf_reset (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 10, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 10, 190)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 10, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:03.000', 10, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_leaf_tf_reset order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 2),
                ("2025-01-01 00:00:00.000", "0", 2),
                ("2025-01-01 00:00:02.000", "", 2),
                ("2025-01-01 00:00:02.000", "0", 2),
            ],
        )

    def test_event_window_tree_leaf_true_for_is_local(self):
        """EVENT_WINDOW tree applies true_for on the matching leaf only.

        Switch from a leaf with a local count threshold to its sibling and verify
        that each leaf retains its own window bounds and row count.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_true_for", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_tree "
            "event_window(start with ((current >= 12 true_for(count 2), current >= 10), voltage < 215) "
            "end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_tree (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 11, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:03.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_tree order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 4),
                ("2025-01-01 00:00:00.000", "0", 3),
                ("2025-01-01 00:00:00.000", "0.0", 2),
                ("2025-01-01 00:00:02.000", "0.1", 1),
                ("2025-01-01 00:00:03.000", "1", 1),
            ],
        )

    def test_event_window_tree_parallel_pending_fallback(self):
        """Lower priority leaf wins from its own streak if higher priority breaks.

        Break the higher-priority pending streak while the lower-priority leaf
        meets its threshold, then verify the selected path starts at its first match.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_pending", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_pending "
            "event_window(start with (current >= 12 true_for(count 3), current >= 10 true_for(count 2)) "
            "end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_pending (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 11, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_pending order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 2),
                ("2025-01-01 00:00:00.000", "1", 2),
            ],
        )

    def test_event_window_tree_backtracking_preemption(self):
        """Higher priority leaf preempts active lower priority from its streak start.

        Satisfy a higher-priority leaf after a lower-priority window opens and
        verify that preemption splits the windows at the new streak start.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_preempt", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_preempt "
            "event_window(start with (current >= 12 true_for(count 2), current >= 10 true_for(count 1)) "
            "end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_preempt (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 10, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:03.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_preempt order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 3),
                ("2025-01-01 00:00:00.000", "1", 1),
                ("2025-01-01 00:00:01.000", "0", 2),
            ],
        )

    def test_event_window_tree_parent_true_for_is_global(self):
        """Global true_for suppresses parent nodes but not local leaf windows.

        Close a window before the global count threshold is met and verify that
        the matching leaf emits a result while the envelope remains filtered.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_parent_tf", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_parent_tf "
            "event_window(start with (current >= 12 true_for(count 1), voltage < 100 true_for(count 3)) "
            "end with voltage < 200) "
            "true_for(count 3) "
            "from t stream_options(event_type(window_close)) "
            "into r_parent_tf (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_parent_tf order by ts, path",
            [("2025-01-01 00:00:00.000", "0", 2)],
        )

    def test_event_window_tree_global_true_for_filters_parent_open(self):
        """Global true_for suppresses parent open.

        Request WINDOW_OPEN for a window below the global count threshold and
        verify that only the leaf whose local threshold is satisfied emits a row.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_parent_tf_open", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_parent_tf_open "
            "event_window(start with (current >= 12 true_for(count 1), voltage < 100 true_for(count 3)) "
            "end with voltage < 200) "
            "true_for(count 3) "
            "from t stream_options(event_type(window_open)) "
            "into r_parent_tf_open (ts, path primary key, current) "
            "as select _twstart, _event_condition_path, current from t where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 1, 190)")
        self._query_until_result(
            "select ts, path, current from r_parent_tf_open order by ts, path",
            [("2025-01-01 00:00:00.000", "0", 12)],
        )

    def test_event_window_tree_global_true_for_emits_parent_open_and_close(self):
        """Satisfied global true_for emits non-leaf open and close.

        Satisfy the global count threshold on the closing row and verify that
        both the envelope and leaf emit their opening and closing results.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_parent_tf_both", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_parent_tf_both "
            "event_window(start with (current >= 12 true_for(count 1), voltage < 100 true_for(count 3)) "
            "end with voltage < 200) "
            "true_for(count 3) "
            "from t stream_options(event_type(window_open|window_close)) "
            "into r_parent_tf_both (end_ts, path primary key, start_ts, current) "
            "as select _twend, _event_condition_path, _twstart, current from t where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 12, 190)")
        self._query_until_result(
            "select start_ts, end_ts, path, current from r_parent_tf_both order by path, end_ts",
            [
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", "", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", "", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", "0", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", "0", 12),
            ],
        )

    def test_event_window_tree_history_global_true_for_emits_parent_open_and_close(self):
        """History replay emits satisfied non-leaf open and close.

        Replay rows that satisfy the global count threshold at close and verify
        opening and closing results for both the envelope and leaf.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_history_parent_tf_both", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 12, 190)")
        tdSql.execute(
            "create stream s_history_parent_tf_both "
            "event_window(start with (current >= 12 true_for(count 1), voltage < 100 true_for(count 3)) "
            "end with voltage < 200) "
            "true_for(count 3) "
            "from t stream_options(fill_history|event_type(window_open|window_close)) "
            "into r_history_parent_tf_both (end_ts, path primary key, start_ts, current) "
            "as select _twend, _event_condition_path, _twstart, current from t where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        self._query_until_result(
            "select start_ts, end_ts, path, current from r_history_parent_tf_both order by path, end_ts",
            [
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", "", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", "", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", "0", 12),
                ("2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", "0", 12),
            ],
        )

    def test_event_window_tree_global_true_for_does_not_duplicate_parent_open(self):
        """Non-leaf open already emitted before close is not emitted again.

        Open a nested leaf after its ancestors satisfy the global count threshold
        and verify exactly one opening and one closing result for each path.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_parent_tf_no_dup_open", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_parent_tf_no_dup_open "
            "event_window(start with ((current >= 12 true_for(count 3), voltage < 100 true_for(count 1)), "
            "current < 0 true_for(count 1)) end with voltage < 200) "
            "true_for(count 2) "
            "from t stream_options(event_type(window_open|window_close)) "
            "into r_parent_tf_no_dup_open (start_ts, event_time primary key, path, end_ts, current) "
            "as select _twstart, cast(_tlocaltime as bigint), _event_condition_path, _twend, current "
            "from t where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 12, 190)")
        self._query_until_result(
            "select path, start_ts, end_ts, count(*) from r_parent_tf_no_dup_open "
            "group by path, start_ts, end_ts order by path, end_ts",
            [
                ("", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
                ("0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
                ("0.0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("0.0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
            ],
        )

    def test_event_window_tree_history_global_true_for_does_not_duplicate_parent_open(self):
        """History replay does not duplicate non-leaf open at close.

        Replay a nested leaf whose ancestors satisfy the global count threshold
        and verify exactly one opening and one closing result for each path.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-25 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_history_parent_tf_no_dup_open", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 12, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 12, 190)")
        tdSql.execute(
            "create stream s_history_parent_tf_no_dup_open "
            "event_window(start with ((current >= 12 true_for(count 3), voltage < 100 true_for(count 1)), "
            "current < 0 true_for(count 1)) end with voltage < 200) "
            "true_for(count 2) "
            "from t stream_options(fill_history|event_type(window_open|window_close)) "
            "into r_history_parent_tf_no_dup_open (start_ts, event_time primary key, path, end_ts, current) "
            "as select _twstart, cast(_tlocaltime as bigint), _event_condition_path, _twend, current "
            "from t where ts = _twstart"
        )
        tdStream.checkStreamStatus()
        self._query_until_result(
            "select path, start_ts, end_ts, count(*) from r_history_parent_tf_no_dup_open "
            "group by path, start_ts, end_ts order by path, end_ts",
            [
                ("", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
                ("0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
                ("0.0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:00.000", 1),
                ("0.0", "2025-01-01 00:00:00.000", "2025-01-01 00:00:02.000", 1),
            ],
        )

    def test_event_window_tree_pending_break_keeps_active_leaf(self):
        """Broken higher priority pending leaf does not interrupt active lower leaf.

        Break a higher-priority pending streak while a lower-priority leaf is
        active and verify that the active window retains all its matching rows.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: None

        History:
            - 2026-06-24 Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()
        tdSql.prepare(dbname="ewtree_pending_break", vgroups=1)
        tdSql.execute("create table t (ts timestamp, current int, voltage int)")
        tdSql.execute(
            "create stream s_pending_break "
            "event_window(start with (current >= 20 true_for(count 2), current >= 10 true_for(count 1)) "
            "end with voltage < 200) "
            "from t stream_options(event_type(window_close)) "
            "into r_pending_break (ts, path primary key, cnt) "
            "as select _twstart, _event_condition_path, count(*) from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute("insert into t values ('2025-01-01 00:00:00.000', 10, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:01.000', 20, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:02.000', 10, 230)")
        tdSql.execute("insert into t values ('2025-01-01 00:00:03.000', 1, 190)")
        self._query_until_result(
            "select ts, path, cnt from r_pending_break order by ts, path",
            [
                ("2025-01-01 00:00:00.000", "", 3),
                ("2025-01-01 00:00:00.000", "1", 3),
            ],
        )
