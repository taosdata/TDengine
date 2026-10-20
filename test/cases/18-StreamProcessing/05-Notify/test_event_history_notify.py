# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream
from notify_check import NotifyLog
from stream_notify_server import (
    start_notify_server_background,
    stop_notify_server_background,
)


class TestEventHistoryNotify:
    @classmethod
    def setup_class(cls):
        tdStream.createSnode()

    @pytest.mark.parametrize(
        "virtual,closed,true_for,notify_history,calculate,expected",
        [
            (False, False, "", True, False, ["WINDOW_OPEN"]),
            (True, False, "", True, False, ["WINDOW_OPEN"]),
            (False, True, "", True, False, ["WINDOW_OPEN", "WINDOW_CLOSE"]),
            (True, True, "", True, False, ["WINDOW_OPEN", "WINDOW_CLOSE"]),
            (False, False, "true_for(10s)", True, False, ["WINDOW_OPEN"]),
            (True, False, "true_for(10s)", True, False, ["WINDOW_OPEN"]),
            (False, True, "true_for(10s)", True, False, ["WINDOW_OPEN", "WINDOW_CLOSE"]),
            (True, True, "true_for(10s)", True, False, ["WINDOW_OPEN", "WINDOW_CLOSE"]),
            (True, False, "true_for(30m)", True, False, []),
            (True, True, "true_for(31m)", True, False, []),
            (True, False, "true_for(10s)", False, False, []),
            (True, False, "true_for(10s)", True, True, ["WINDOW_OPEN"]),
        ],
    )
    def test_history_open_does_not_wait_for_close(
        self, virtual, closed, true_for, notify_history, calculate, expected, tmp_path
    ):
        """History sends qualified EVENT opens without waiting for a closing row.

        Cover physical and virtual sources, closed and unclosed history windows,
        TRUE_FOR filtering, NOTIFY_HISTORY, calculation and realtime continuation.

        Catalog:
            - Streams:05-Notify:EventWindow

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7139860088

        History:
            - 2026-10-19 OpenAI Added history TRUE_FOR notification regression
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        try:
            tdSql.execute("create database history_notify_src vgroups 1")
            tdSql.execute("create database history_notify_vt vgroups 1")
            tdSql.execute("create table history_notify_src.src (ts timestamp, `熔体温度` double)")
            tdSql.execute(
                "create stable history_notify_vt.vst "
                "(ts timestamp, dummy tinyint, `熔体温度` double, unused double) "
                "tags (element varchar(64)) virtual 1"
            )
            tdSql.execute(
                "create vtable history_notify_vt.`vt_1号感应熔炼炉 MF-01_b7rp82` "
                "(`熔体温度` from history_notify_src.src.`熔体温度`) "
                "using history_notify_vt.vst tags ('MF-01')"
            )
            values = [
                1081.556194, 1081.139438, 1082.30543, 1080.042004,
                1077.958645, 1079.274158, 1080.963619, 1079.25861,
                1080.069717, 1077.263606, 1079.901744, 1081.612622,
                1078.809739, 1077.821641, 1079.859374, 1081.395971,
                1078.799792, 1077.821641, 1076.762894, 1206.595273,
                1202.385873, 1200.015669, 1201.467049, 1203.759287,
                1210.390441, 1209.653732, 1203.722475, 1203.599468,
                1203.635689, 1206.422754,
            ]
            if closed:
                values.append(800)
            tdSql.execute(
                "insert into history_notify_src.src values " + " ".join(
                    f"('2026-09-12 15:{6 + i:02d}:00.000', {value})"
                    for i, value in enumerate(values)
                )
            )
            source = (
                "history_notify_vt.`vt_1号感应熔炼炉 MF-01_b7rp82`"
                if virtual else "history_notify_src.src"
            )
            tdSql.query(f"select ts, `熔体温度` from {source} order by ts")
            tdSql.checkRows(len(values))
            calc_event = "window_open" if calculate else "window_close"
            notify_options = "notify_options(notify_history)" if notify_history else ""
            query = (
                "into history_notify_vt.result as select _twstart ts, count(*) cnt "
                f"from {source} where ts = _twstart"
                if calculate else ""
            )
            tdSql.execute(
                "create stream history_notify_vt.s "
                "event_window(start with `熔体温度` > 1000 end with `熔体温度` < 900) "
                f"{true_for} from {source} "
                "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=0)|"
                f"event_type({calc_event})|ignore_disorder|fill_history) "
                "notify('ws://localhost:12345/history') on(window_open|window_close) "
                f"{notify_options} {query}"
            )
            tdStream.checkStreamStatus("s")
            path = os.path.join(tmp_path, "history.log")
            if not expected:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    tdSql.query(
                        "select history_progress_pct from information_schema.ins_streams "
                        "where stream_name='s'"
                    )
                    if tdSql.getData(0, 0) == 100:
                        break
                    time.sleep(0.2)
                tdSql.checkData(0, 0, 100)
            events = self._wait_events(path, expected)
            tdLog.info(f"virtual={virtual}, closed={closed}, true_for={true_for}: {[e.raw for e in events]}")
            assert [e.eventType for e in events] == expected
            assert all(e.windowStart == 1789196760000 for e in events)
            if expected:
                assert events[0].raw["triggerCondition"]["fieldValues"]["熔体温度"] == values[0]
            if closed and expected:
                assert events[1].windowEnd == 1789198560000
                assert events[0].triggerId == events[1].triggerId
            if calculate:
                tdSql.query("select cast(ts as bigint), cnt from history_notify_vt.result")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 1789196760000)
                tdSql.checkData(0, 1, 1)
            if not closed and expected and not calculate:
                tdSql.execute(
                    "insert into history_notify_src.src values "
                    "('2026-09-12 15:36:00.000',1100) ('2026-09-12 15:37:00.000',800)"
                )
                events = self._wait_events(path, ["WINDOW_OPEN", "WINDOW_OPEN", "WINDOW_CLOSE"])
                assert events[1].windowStart == 1789198560000
                assert events[2].windowStart == 1789198560000
                assert events[2].windowEnd == 1789198620000
                assert events[1].triggerId == events[2].triggerId
                assert events[0].triggerId != events[1].triggerId
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    def _wait_events(self, path, expected):
        deadline = time.monotonic() + 60
        stable_since = None
        events = []
        while time.monotonic() < deadline:
            events = list(NotifyLog(path).events()) if os.path.exists(path) else []
            observed = [event.eventType for event in events]
            assert len(observed) <= len(expected), observed
            if observed == expected:
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since >= 2:
                    return events
            else:
                stable_since = None
            time.sleep(0.2)
        assert [event.eventType for event in events] == expected
        return events
