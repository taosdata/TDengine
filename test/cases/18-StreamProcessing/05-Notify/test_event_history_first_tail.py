# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream
from notify_check import NotifyLog
from stream_notify_server import start_notify_server_background, stop_notify_server_background


class TestEventHistoryFirstTail:
    @classmethod
    def setup_class(cls):
        tdStream.createSnode()

    @pytest.mark.parametrize(
        "virtual,qualified,notify,calculate,natural_close",
        [
            pytest.param(False, True, True, True, False, id="physical"),
            pytest.param(True, True, True, True, False, id="virtual"),
            pytest.param(True, False, True, True, False, id="short"),
            pytest.param(True, True, False, True, False, id="muted"),
            pytest.param(True, True, True, False, False, id="notify_only"),
            pytest.param(True, True, True, True, True, id="natural_close"),
        ],
    )
    def test_history_tail_closes_before_realtime(
        self, virtual, qualified, notify, calculate, natural_close, tmp_path
    ):
        """FILL_HISTORY_FIRST closes eligible EVENT tails without realtime input.

        Check historical boundaries, TRUE_FOR, notification subscriptions,
        calculation results and independent realtime and recalculation windows.

        Catalog:
            - Streams:05-Notify:EventWindow

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7169527842

        History:
            - 2026-11-16 OpenAI Added historical EVENT tail regression
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        base = 1735689600000
        rt = base + 3600000
        end = base + (660000 if natural_close else 600000)
        path = os.path.join(tmp_path, "tail.log")
        try:
            tdSql.prepare(dbname="history_tail", vgroups=1)
            tdSql.execute("create table src (ts timestamp, v int)")
            source = "src"
            if virtual:
                tdSql.execute("create vtable vt (ts timestamp, v int from src.v)")
                source = "vt"
            tdSql.execute(f"insert into src values ({base},1) ({base+20000},1) ({base+600000},1)")
            if natural_close:
                tdSql.execute(f"insert into src values ({base+660000},0) ({base+680000},0)")
            duration = "10m" if qualified else "11m"
            notify_options = "notify_options(notify_history)" if notify else ""
            query = (
                "into result as select _twstart ts, _twend wend, _twrownum wrows, count(*) cnt "
                f"from {source} where ts>=_twstart and ts<_twend"
                if calculate else ""
            )
            tdSql.execute(
                "create stream s event_window(start with v>0 end with v<=0) "
                f"true_for({duration},start(20s),end(20s)) from {source} "
                "stream_options(fill_history_first|event_type(window_close)|ignore_disorder) "
                "notify('ws://localhost:12345/tail') on(window_open|window_close) "
                f"{notify_options} {query}"
            )
            tdStream.checkStreamStatus("s")
            self._wait_query(
                "select history_progress_pct from information_schema.ins_streams "
                "where db_name='history_tail' and stream_name='s'", [(100,)]
            )
            expected = ["WINDOW_OPEN", "WINDOW_CLOSE"] if qualified and notify else []
            events = self._wait_events(path, expected)
            if expected:
                assert events[0].windowStart == events[1].windowStart == base
                assert events[1].windowEnd == end
                assert events[0].triggerId == events[1].triggerId
                if not natural_close:
                    assert events[1].raw["triggerCondition"]["fieldValues"] == {}
            if calculate and qualified:
                rows = [(base, end, 4 if natural_close else 3, 3 if natural_close else 2)]
                self._wait_query("select cast(ts as bigint), cast(wend as bigint), wrows, cnt from result", rows)
            elif calculate:
                self._wait_query(
                    "select table_name from information_schema.ins_tables "
                    "where db_name='history_tail' and table_name='result'", []
                )

            if virtual and qualified and notify and calculate and not natural_close:
                # A bounded recalculation with no END row must remain open.
                tdSql.execute(f"recalculate stream history_tail.s from {base} to {base+600000}")
                self._wait_query(
                    "select status, progress from information_schema.ins_stream_recalculates "
                    "where stream_name='s'", [("Finished", "100%")]
                )
                expected += ["WINDOW_OPEN"]
                self._wait_events(path, expected)
                tdSql.execute(f"insert into src values ({rt},1) ({rt+20000},1) ({rt+600000},1)")
                expected += ["WINDOW_OPEN"]
                self._wait_events(path, expected)
                tdSql.execute(f"insert into src values ({rt+660000},0) ({rt+680000},0)")
                expected += ["WINDOW_CLOSE"]
                events = self._wait_events(path, expected)
                assert events[-2].triggerId == events[-1].triggerId != events[0].triggerId
                assert events[-1].windowStart == rt
                assert events[-1].windowEnd == rt + 660000
                self._wait_query(
                    "select cast(ts as bigint), cast(wend as bigint), wrows, cnt from result order by ts",
                    [(base, end, 3, 2), (rt, rt+660000, 4, 3)],
                )
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    @pytest.mark.parametrize(
        "nested,switch_at_tail,calculate",
        [
            pytest.param(False, False, False, id="flat"),
            pytest.param(True, False, False, id="tree"),
            pytest.param(False, True, False, id="flat_tail_switch_notify"),
            pytest.param(False, True, True, id="flat_tail_switch_calc"),
        ],
    )
    def test_history_tail_closes_condition_tree(self, nested, switch_at_tail, calculate, tmp_path):
        """Close historical EVENT parent and leaf tails with matching identities.

        Switch START conditions before the tail, then verify every OPEN has one
        CLOSE with the same condition path and parent identity. A switch on the
        final row must advance both the parent notification and calculation end.

        Catalog:
            - Streams:05-Notify:EventWindow

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7169527842

        History:
            - 2026-11-16 OpenAI Added condition tree tail regression
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        base = 1735689600000
        end = base + (20000 if switch_at_tail else 600000)
        path = os.path.join(tmp_path, "tail.log")
        try:
            tdSql.prepare(dbname="history_tail_tree", vgroups=1)
            tdSql.execute("create table src (ts timestamp, v int)")
            tdSql.execute(f"insert into src values ({base},1) ({base+20000},2)")
            if not switch_at_tail:
                tdSql.execute(f"insert into src values ({base+600000},2)")
            conditions = "(v=1,(v=2,v=3))" if nested else "(v=1,v=2)"
            # Row count distinguishes the parent and first child at the same start.
            # Avoid _event_condition_path, which would select the tree engine.
            query = (
                "into result (ts, wrows primary key, wend, cnt) "
                "as select _twstart, _twrownum, _twend, count(*) "
                "from src where ts>=_twstart and ts<=_twend"
                if calculate else ""
            )
            tdSql.execute(
                f"create stream s event_window(start with {conditions} end with v=0) from src "
                "stream_options(fill_history_first|event_type(window_close)|ignore_disorder) "
                "notify('ws://localhost:12345/tail') on(window_open|window_close) "
                f"notify_options(notify_history) {query}"
            )
            tdStream.checkStreamStatus("s")
            self._wait_query(
                "select history_progress_pct from information_schema.ins_streams "
                "where db_name='history_tail_tree' and stream_name='s'", [(100,)]
            )
            window_count = 4 if nested else 3
            events = self._wait_events(path, None, count=window_count * 2)
            opens = {e.triggerId: e for e in events if e.eventType == "WINDOW_OPEN"}
            closes = {e.triggerId: e for e in events if e.eventType == "WINDOW_CLOSE"}
            assert len(opens) == len(closes) == window_count
            assert opens.keys() == closes.keys()
            bounds = sorted((e.windowStart, e.windowEnd) for e in closes.values())
            expected_bounds = [(base, base), (base, end), (base+20000, end)]
            if nested:
                expected_bounds.append((base+20000, end))
            assert bounds == expected_bounds
            for trigger_id, close in closes.items():
                opening = opens[trigger_id]
                assert opening.raw["triggerCondition"]["conditionPath"] == close.raw["triggerCondition"]["conditionPath"]
                assert opening.raw["parentTriggerId"] == close.raw["parentTriggerId"]
            if calculate:
                self._wait_query(
                    "select cast(ts as bigint), wrows, cast(wend as bigint), cnt from result order by ts, wrows",
                    [(base, 1, base, 1), (base, 2, end, 2), (end, 1, end, 1)],
                )
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    def _wait_query(self, sql, expected):
        for _ in range(300):
            tdSql.query(sql)
            if tdSql.queryRows == len(expected) and all(
                tdSql.checkData(row, col, value, exit=False)
                for row, values in enumerate(expected) for col, value in enumerate(values)
            ):
                return
            time.sleep(0.2)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def _wait_events(self, path, expected, count=None):
        count = len(expected) if count is None else count
        deadline = time.monotonic() + 60
        stable_since = None
        events = []
        while time.monotonic() < deadline:
            events = list(NotifyLog(path).events()) if os.path.exists(path) else []
            observed = [e.eventType for e in events]
            assert len(events) <= count, [e.raw for e in events]
            if len(events) == count and (expected is None or observed == expected):
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since >= 2:
                    tdLog.info(f"history tail notifications: {[e.raw for e in events]}")
                    return events
            else:
                stable_since = None
            time.sleep(0.2)
        assert len(events) == count, [e.raw for e in events]
        assert expected is None or [e.eventType for e in events] == expected
        return events
