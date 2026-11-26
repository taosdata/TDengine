from new_test_framework.utils import tdLog, tdSql, tdStream

import time


class TestStreamPreviousWindowPlaceholder:
    STATE_BASE_TS = 1735689600000
    COUNT_BASE_TS = 1735689800000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdStream.dropAllStreamsAndDbs()
        tdSql.query("show snodes")
        if tdSql.queryRows == 0:
            tdStream.createSnode()
        tdSql.prepare(dbname="prev_window", vgroups=1)
        tdSql.execute("create stable meters (ts timestamp, phase int, v int) tags(id int)")
        tdSql.execute("create table d1 using meters tags(1)")
        tdSql.execute("create table d2 using meters tags(2)")

    def wait_stream_ready(self, stream_name):
        for _ in range(30):
            tdSql.query(
                "select * from information_schema.ins_stream_tasks "
                f"where type = 'Trigger' and status = 'Running' and stream_name = '{stream_name}'"
            )
            if tdSql.queryRows > 0:
                time.sleep(2)
                return
            time.sleep(1)
        tdLog.exit(f"{stream_name} trigger task did not become ready")

    def check_previous_windows(self, table, expected):
        tdSql.checkResultsByArray(
            f"select cast(ts as bigint), cast(prev_start as bigint), "
            f"cast(prev_end as bigint), cnt from {table} order by ts",
            expected,
            retry=60,
        )

    def do_state_window_and_partition(self):
        base_ts = self.STATE_BASE_TS
        sql = (
            "create stream prev_state state_window(phase) from meters partition by tbname "
            "stream_options(event_type(window_close)) "
            "into prev_state_out output_subtable(concat('prev_', tbname)) "
            "as select _twstart as ts, _tpre_wstart as prev_start, "
            "_tpre_wend as prev_end, count(*) as cnt from %%trows"
        )
        tdSql.execute(sql)
        self.wait_stream_ready("prev_state")
        tdSql.executes([
            f"insert into d1 values({base_ts}, 1, 1) ({base_ts + 1000}, 1, 2) "
            f"({base_ts + 2000}, 2, 3) ({base_ts + 3000}, 2, 4) ({base_ts + 4000}, 3, 5) "
            f"({base_ts + 5000}, 3, 6) ({base_ts + 6000}, 4, 7)",
            f"insert into d2 values({base_ts}, 7, 1) ({base_ts + 1000}, 8, 2) "
            f"({base_ts + 2000}, 9, 3) ({base_ts + 3000}, 10, 4)",
        ])
        self.check_previous_windows(
            "prev_d1",
            [
                (base_ts, None, None, 2),
                (base_ts + 2000, base_ts, base_ts + 1000, 2),
                (base_ts + 4000, base_ts + 2000, base_ts + 3000, 2),
            ],
        )
        self.check_previous_windows(
            "prev_d2",
            [
                (base_ts, None, None, 1),
                (base_ts + 1000, base_ts, base_ts, 1),
                (base_ts + 2000, base_ts + 1000, base_ts + 1000, 1),
            ],
        )
        tdLog.info("state window and partition ......................... [ passed ]")

    def do_count_successor_recalculation(self):
        base_ts = self.COUNT_BASE_TS
        name = "prev_count_recalc_ordinary"
        source = f"{name}_source"
        trigger = "count_window(1)"
        tdSql.execute(f"create table {source} (ts timestamp, phase int, v int)")
        tdSql.execute(
            f"create stream {name} {trigger} from {source} "
            "stream_options(delete_recalc|event_type(window_close)) "
            f"into {name}_out as select _twstart as ts, _tpre_wstart as prev_start, "
            "_tpre_wend as prev_end, count(*) as cnt from %%trows"
        )
        self.wait_stream_ready(name)
        tdSql.execute(
            f"insert into {source} values({base_ts + 10000}, 1, 1) "
            f"({base_ts + 20000}, 1, 1) ({base_ts + 30000}, 2, 1) ({base_ts + 40000}, 3, 1)"
        )
        query = (f"select cast(prev_start as bigint), cast(prev_end as bigint), cnt "
                 f"from {name}_out where ts = {base_ts + 20000}")
        tdSql.checkResultsByArray(query, [(base_ts + 10000, base_ts + 10000, 1)], retry=60)

        # The successor itself has no changed source rows and must still be refreshed.
        tdSql.execute(f"insert into {source} values({base_ts + 15000}, 1, 1)")
        tdSql.checkResultsByArray(query, [(base_ts + 15000, base_ts + 15000, 1)], retry=60)
        tdSql.execute(f"delete from {source} where ts = {base_ts + 15000}")
        tdSql.checkResultsByArray(query, [(base_ts + 10000, base_ts + 10000, 1)], retry=60)
        tdSql.execute(f"delete from {source} where ts = {base_ts + 10000}")
        tdSql.checkResultsByArray(query, [(None, None, 1)], retry=60)
        tdSql.execute(f"insert into {source} values({base_ts + 5000}, 1, 1)")
        tdSql.checkResultsByArray(query, [(base_ts + 5000, base_ts + 5000, 1)], retry=60)

        tdLog.info("count recalculation refreshes successor predecessors ......................... [ passed ]")

    def test_count_previous_window_recalculation(self):
        """Previous window dependencies survive count window corrections

        1. Refresh the existing successor after a late window is inserted
        2. Refresh the successor after deleting its predecessor, including the first window
        3. Cover ordinary count windows without changing the successor's source rows

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7067739386

        Jira: None

        History:
            - 2026-09-20 Codex Added automatic predecessor dependency regression

        """
        self.prepare()
        self.do_count_successor_recalculation()

    def test_previous_window_placeholder(self):
        """Previous window placeholders

        1. Verify NULL predecessor bounds for the first state window.
        2. Verify direct predecessors remain isolated between partitions.

        Catalog:
            - Stream

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7067739386

        Jira: None

        History:
            - 2026-08-06 Peng Rongkun Created
            - 2026-09-10 Peng Rongkun Covered the final closed window in historical state processing
            - 2026-09-11 Codex Wait for asynchronous window results before assertions
            - 2026-11-06 Codex Keep focused predecessor regressions and remove slow coverage matrices

        """
        self.prepare()
        self.do_state_window_and_partition()

    def do_null_previous_scan_range(self):
        self.prepare()
        base = self.COUNT_BASE_TS
        tdSql.execute("create table range_trigger (ts timestamp, v int)")
        tdSql.execute("create table range_data (ts timestamp, v int)")
        tdSql.execute(f"insert into range_data values({base - 1000}, 1) ({base}, 2) ({base + 1000}, 3)")
        tdSql.execute(
            "create stream prev_null_range count_window(1) from range_trigger "
            "stream_options(event_type(window_close)) into prev_null_range_out "
            "as select _twstart as ts, count(*) as cnt from range_data "
            "where ts > _tpre_wend and ts <= _twend"
        )
        self.wait_stream_ready("prev_null_range")
        tdSql.execute(f"insert into range_trigger values({base}, 1)")
        query = "select cast(ts as bigint), cnt from prev_null_range_out order by ts"
        tdSql.checkResultsByArray(query, [(base, 0)], retry=60)
        tdSql.execute(f"insert into range_trigger values({base + 1000}, 2)")
        tdSql.checkResultsByArray(query, [(base, 0), (base + 1000, 1)], retry=60)

    def test_null_previous_scan_range(self):
        """NULL predecessor bounds produce empty input on an ordinary computation table.

        1. Put older rows in a separate computation table before triggering the first window.
        2. Assert that the first aggregate sees no rows, then reuse the query with a valid predecessor.

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7067739386

        Jira: None

        History:
            - 2026-09-24 Add extracted NULL timestamp range regression.

        """
        self.do_null_previous_scan_range()

    def do_deleted_tail_previous_window(self):
        self.prepare()
        base = self.COUNT_BASE_TS
        for only_window in [False, True]:
            name = f"prev_tail_{int(only_window)}"
            tdSql.execute(f"create table {name}_source (ts timestamp, v int)")
            tdSql.execute(
                f"create stream {name} count_window(1) from {name}_source "
                "stream_options(delete_recalc|event_type(window_close)) "
                f"into {name}_out as select _twstart as ts, _tpre_wstart as prev_start, "
                "_tpre_wend as prev_end, count(*) as cnt from %%trows"
            )
            self.wait_stream_ready(name)
            rows = [(base + 1000, 1)] if only_window else [(base, 1), (base + 1000, 2)]
            tdSql.execute(f"insert into {name}_source values " + " ".join(f"({ts}, {v})" for ts, v in rows))
            tdSql.checkResultsByArray(f"select count(*) from {name}_out", [(len(rows),)], retry=60)
            tdSql.execute(f"delete from {name}_source where ts = {base + 1000}")
            # Leave a gap so this exercises a future successor, not one that was
            # already present when deletion replay took its source snapshot.
            time.sleep(5)
            tdSql.execute(f"insert into {name}_source values({base + 2000}, 3)")
            expected_previous = None if only_window else base
            query = (f"select cast(prev_start as bigint), cast(prev_end as bigint), cnt "
                     f"from {name}_out where ts = {base + 2000}")
            tdSql.checkResultsByArray(query, [(expected_previous, expected_previous, 1)], retry=60)
            tdSql.execute(f"insert into {name}_source values({base + 3000}, 4)")
            tdSql.checkResultsByArray(
                query.replace(str(base + 2000), str(base + 3000)),
                [(base + 2000, base + 2000, 1)], retry=60,
            )

    def do_deleted_session_open_previous_window(self):
        self.prepare()
        base = self.COUNT_BASE_TS
        name = "prev_session_open_delete"
        source = f"{name}_source"
        tdSql.execute(f"create table {source} (ts timestamp, v int)")
        tdSql.execute(
            f"create stream {name} session(ts, 1s) from {source} "
            "stream_options(delete_recalc|event_type(window_open)) "
            f"into {name}_out as select _twstart as ts, _twend as win_end, _twduration as win_duration, "
            "_tpre_wstart as prev_start, "
            f"_tpre_wend as prev_end from {source} where ts = _twstart"
        )
        self.wait_stream_ready(name)
        tdSql.execute(
            f"insert into {source} values({base + 100}, 1) ({base + 200}, 2) "
            f"({base + 2000}, 3) ({base + 2100}, 4)"
        )
        query = (
            f"select cast(prev_start as bigint), cast(prev_end as bigint) from {name}_out "
            f"where ts = {base + 2000}"
        )
        tdSql.checkResultsByArray(query, [(base + 100, base + 200)], retry=60)
        tdSql.execute(f"delete from {source} where ts = {base + 100}")
        # OPEN must be repaired without new input closing the successor's gap.
        tdSql.checkResultsByArray(query, [(base + 200, base + 200)], retry=60)
        # Another row in the unclosed successor also exercises its deferred OPEN repair.
        tdSql.execute(f"insert into {source} values({base + 2200}, 5)")
        tdSql.checkResultsByArray(
            f"select cast(win_end as bigint), win_duration, cast(prev_start as bigint), "
            f"cast(prev_end as bigint) from {name}_out where ts = {base + 2000}",
            [(base + 2000, 0, base + 200, base + 200)], retry=60,
        )

    def test_deleted_tail_previous_window(self):
        """The first future window repairs its predecessor after tail deletion.

        1. Delete the last of two closed windows, allow deletion replay to run, then insert a future window.
        2. Repeat with the only closed window deleted and expect a NULL predecessor.
        3. Verify another future window uses the new realtime predecessor normally.
        4. Repair an already emitted SESSION OPEN after moving its predecessor's start, without new input.

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7067739386

        Jira: None

        History:
            - 2026-09-24 Add future successor regression after deleting the tail.
            - 2026-10-08 Add event-tree tail deletion coverage.
            - 2026-10-26 Add SESSION boundary-deletion coverage.
            - 2026-10-26 Cover OPEN-only SESSION repair without successor closure.

        """
        self.do_deleted_tail_previous_window()
        self.do_deleted_session_open_previous_window()

    def do_future_open_after_predecessor_deletion(self):
        self.prepare()
        base = self.COUNT_BASE_TS
        name = "prev_future_open"
        source = f"{name}_source"
        tdSql.execute(f"create table {source} (ts timestamp, phase int)")
        tdSql.execute(
            f"create stream {name} event_window(start with phase = 1 end with phase = 0) "
            f"from {source} stream_options(delete_recalc|event_type(window_open)) into {name}_out "
            "as select _twstart as ts, _twend as win_end, _tpre_wstart as prev_start, "
            f"_tpre_wend as prev_end from {source} where ts = _twstart"
        )
        self.wait_stream_ready(name)
        tdSql.execute(
            f"insert into {source} values({base}, 1)({base + 50}, 0)"
            f"({base + 100}, 1)({base + 200}, 0)"
        )
        tdSql.checkResultsByArray(f"select count(*) from {name}_out", [(2,)], retry=60)
        tdSql.execute(f"delete from {source} where ts between {base + 100} and {base + 200}")
        # Keep the successor absent until deletion replay has taken its snapshot.
        time.sleep(5)
        tdSql.execute(f"insert into {source} values({base + 300}, 1)")
        query = (f"select cast(win_end as bigint), cast(prev_start as bigint), "
                 f"cast(prev_end as bigint) from {name}_out where ts = {base + 300}")
        tdSql.checkResultsByArray(query, [(base + 300, base, base + 50)], retry=60)
        tdSql.execute(f"insert into {source} values({base + 400}, 0)")
        tdSql.checkResultsByArray(query, [(base + 300, base, base + 50)], retry=60)
        tdLog.info("future EVENT OPEN repairs its predecessor ......................... [ passed ]")

    def test_future_open_after_predecessor_deletion(self):
        """Repair future EVENT OPENs after their predecessor has been deleted.

        1. Delete a closed predecessor before its future successor exists.
        2. Verify the future OPEN uses the surviving earlier window.
        3. Preserve the repaired predecessor when the successor closes.

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7067739386

        Jira: None

        History:
            - 2026-10-27 Cover future OPEN repair and inline TRUE_FOR.
            - 2026-10-28 Retain corrected predecessors across MAX_DELAY periods.
            - 2026-10-30 Cover CLOSE-only and default events during MAX_DELAY predecessor repair.
            - 2026-11-02 Cover deletion of both the predecessor and the successor's first START.

        """
        self.do_future_open_after_predecessor_deletion()
