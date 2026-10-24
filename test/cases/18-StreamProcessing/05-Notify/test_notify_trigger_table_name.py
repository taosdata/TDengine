# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import time

import pytest

from new_test_framework.utils import sc, tdSql, tdStream
from notify_check import NotifyLog
from stream_notify_server import start_notify_server_background, stop_notify_server_background


class TestNotifyTriggerTableName:
    @classmethod
    def setup_class(cls):
        tdStream.createSnode()

    @pytest.mark.parametrize("virtual,partition,calculate,history,nested,restart", [
        (False, "tbname", False, False, False, True),
        (True, "tbname", False, False, False, True),
        (False, "tbname", True, False, False, False),
        (True, "tbname", True, False, False, False),
        (False, "site", False, False, False, False),
        (True, "site", True, False, False, False),
        (False, "", False, False, False, False),
        (True, "", True, False, False, False),
        (False, "tbname", False, True, False, False),
        (True, "tbname", False, True, False, False),
        (False, "tbname", False, False, True, False),
        (True, "tbname", True, False, True, False),
        (False, "tbname", True, True, True, False),
        (True, "tbname", False, True, True, False),
    ])
    def test_trigger_table_name(self, virtual, partition, calculate, history, nested, restart, tmp_path):
        """Notifications identify the trigger subtable independently of output tables.

        Check the first OPEN without a calculation, then CLOSE, preserving group
        and trigger identities and requiring an explicit string for every event.

        Catalog:
            - Streams:05-Notify:TriggerTableName

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/68d89fc0cfbfe8e03b718ac7/detail/7144670810

        History:
            - 2026-10-23 OpenAI Added trigger table name coverage
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        try:
            tdSql.execute("create database notify_name vgroups 1")
            tdSql.execute("create stable notify_name.src (ts timestamp, v int, scope int) tags (site int)")
            physical_names = ["sensor_a", "sensor_b"] if partition else ["sensor_a"]
            for index, name in enumerate(physical_names):
                tdSql.execute(f"create table notify_name.{name} using notify_name.src tags ({index + 1})")
            source = "src"
            trigger_names = physical_names
            if virtual:
                tdSql.execute("create stable notify_name.vst (ts timestamp, v int, scope int) tags (site int) virtual 1")
                trigger_names = [f"虚拟_{name}" for name in physical_names]
                for index, (name, physical) in enumerate(zip(trigger_names, physical_names)):
                    tdSql.execute(
                        f"create vtable notify_name.`{name}` (v from notify_name.{physical}.v, "
                        f"scope from notify_name.{physical}.scope) using notify_name.vst tags ({index + 1})"
                    )
                source = "vst"
            gids = {}
            for name in trigger_names:
                tdSql.query(f"select uid from information_schema.ins_tables where db_name='notify_name' and table_name='{name}'")
                gids[name] = str(tdSql.getData(0, 0))
            tdSql.execute("create table notify_name.calculation_source (ts timestamp, x int)")
            tdSql.execute("insert into notify_name.calculation_source values ('2026-10-23 00:00:00', 123)")
            trigger = (
                "window (interval(1h) as parent, count_window(2,2) as leaf)"
                if nested else "event_window(start with v > 0 end with v = 0)"
            )
            if partition != "tbname":
                trigger = "interval(1s)"
            grouping = f"partition by {partition}" if partition else ""
            options = "event_type(window_open|window_close)" if nested else "event_type(window_close)"
            options += "|fill_history" if history else ""
            notify_options = "notify_options(notify_history)" if history else ""
            window_start = "leaf._twstart" if nested else "_twstart"
            query = (
                f"into notify_name.result as select {window_start} ts, count(*) total "
                "from notify_name.calculation_source"
                if calculate else ""
            )
            if history:
                for index, name in enumerate(physical_names):
                    self._insert(name, index, False)
                    self._insert(name, index, True)
            endpoint = f"names_{virtual}_{partition}_{calculate}_{history}_{nested}_{restart}"
            tdSql.execute(
                f"create stream notify_name.s {trigger} from notify_name.{source} {grouping} "
                f"stream_options({options}) notify('ws://localhost:12345/{endpoint}') "
                f"on(window_open|window_close) {notify_options} {query}"
            )
            tdStream.checkStreamStatus("s")
            path = os.path.join(tmp_path, f"{endpoint}.log")
            for index, (physical, name) in enumerate(zip(physical_names, trigger_names)):
                if not history:
                    if restart and index == 1:
                        sc.dnodeStop(1)
                        sc.dnodeStart(1)
                        tdStream.checkStreamStatus("s")
                    self._insert(physical, index, False)
                    if not nested:
                        events = self._wait_events(path, 1, 1792684800000 + index * 10000)
                        opened = events[-1]
                        assert opened.eventType == "WINDOW_OPEN", opened.raw
                        self._check_name(opened, name if partition == "tbname" else "")
                    self._insert(physical, index, True)
                    self._wait_events(path, 2, 1792684800000 + index * 10000)
            if history:
                self._wait_events(path, len(physical_names) * 2)
            events = list(NotifyLog(path).events())
            for index, name in enumerate(trigger_names):
                start = 1792684800000 + index * 10000
                group_events = [event for event in events if event.windowStart == start]
                types = [event.eventType for event in group_events]
                if restart and index == 0:
                    # Existing recovery may replay a completed window before the checkpoint.
                    assert set(types) == {"WINDOW_CLOSE", "WINDOW_OPEN"}, [e.raw for e in events]
                else:
                    assert sorted(types) == ["WINDOW_CLOSE", "WINDOW_OPEN"], [e.raw for e in events]
                assert len({event.triggerId for event in group_events}) == 1
                for event in group_events:
                    self._check_name(event, name if partition == "tbname" else "")
                    if partition == "tbname":
                        assert event.raw["groupId"] == gids[name], event.raw
                    if not partition:
                        assert event.raw["groupId"] == "0", event.raw
                    if calculate and event.eventType == "WINDOW_CLOSE":
                        assert event.tableName and event.tableName != name, event.raw
                        assert event.result is not None, event.raw
            # A lookup continuation must not cause a second window effect.
            time.sleep(1)
            assert len(NotifyLog(path).events()) == len(events)
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    @pytest.mark.parametrize("virtual", [False, True])
    def test_idle_resume_names(self, virtual, tmp_path):
        """IDLE and RESUME retain the trigger subtable name.

        Wait for an idle group, resume it with a new row, and validate the
        independent group notifications for physical and virtual subtables.

        Catalog:
            - Streams:05-Notify:TriggerTableName

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/68d89fc0cfbfe8e03b718ac7/detail/7144670810

        History:
            - 2026-10-23 OpenAI Added idle and resume name coverage
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        try:
            tdSql.execute("create database notify_name vgroups 1")
            tdSql.execute("create stable notify_name.src (ts timestamp, v int, scope int) tags (site int)")
            tdSql.execute("create table notify_name.sensor_a using notify_name.src tags (1)")
            source, name = "src", "sensor_a"
            if virtual:
                tdSql.execute("create stable notify_name.vst (ts timestamp, v int, scope int) tags (site int) virtual 1")
                tdSql.execute(
                    "create vtable notify_name.virtual_a (v from notify_name.sensor_a.v, "
                    "scope from notify_name.sensor_a.scope) using notify_name.vst tags (1)"
                )
                source, name = "vst", "virtual_a"
            tdSql.execute(
                "create stream notify_name.s event_window(start with v > 0 end with v = 0) "
                f"from notify_name.{source} partition by tbname "
                "stream_options(event_type(idle|resume)|idle_timeout(2s)) "
                f"notify('ws://localhost:12345/idle_{virtual}') on(idle|resume)"
            )
            tdStream.checkStreamStatus("s")
            self._insert("sensor_a", 0, False)
            path = os.path.join(tmp_path, f"idle_{virtual}.log")
            events = self._wait_events(path, 1)
            assert events[0].eventType == "IDLE", events[0].raw
            self._check_name(events[0], name)
            self._insert("sensor_a", 0, True)
            events = self._wait_events(path, 2)
            assert events[1].eventType == "RESUME", events[1].raw
            self._check_name(events[1], name)
            assert events[0].raw["groupId"] == events[1].raw["groupId"]
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    @pytest.mark.parametrize("source,partition", [
        ("sensor_a", "tbname"),
        ("src", "site + 1, site + 2, tbname"),
    ])
    @pytest.mark.parametrize("calculate", [False, True])
    def test_partition_name_expressions(self, source, partition, calculate, tmp_path):
        """Direct child sources and mixed partition expressions retain the table name.

        Check both direct OPEN and calculated CLOSE notifications when the source
        is a child table or tbname follows expressions sharing the same tag input.

        Catalog:
            - Streams:05-Notify:TriggerTableName

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/68d89fc0cfbfe8e03b718ac7/detail/7144670810

        History:
            - 2026-10-24 OpenAI Added review regression coverage
        """
        tdStream.dropAllStreamsAndDbs()
        start_notify_server_background(port=12345, log_path=str(tmp_path))
        try:
            tdSql.execute("create database notify_name vgroups 1")
            tdSql.execute("create stable notify_name.src (ts timestamp, v int, scope int) tags (site int)")
            tdSql.execute("create table notify_name.sensor_a using notify_name.src tags (1)")
            tdSql.execute("create table notify_name.calculation_source (ts timestamp, x int)")
            tdSql.execute("insert into notify_name.calculation_source values ('2026-10-23 00:00:00', 123)")
            query = (
                "into notify_name.result as select _twstart ts, count(*) total "
                "from notify_name.calculation_source" if calculate else ""
            )
            endpoint = f"expressions_{source}_{calculate}"
            tdSql.execute(
                "create stream notify_name.s event_window(start with v > 0 end with v = 0) "
                f"from notify_name.{source} partition by {partition} "
                "stream_options(event_type(window_close)) "
                f"notify('ws://localhost:12345/{endpoint}') on(window_open|window_close) {query}"
            )
            tdStream.checkStreamStatus("s")
            path = os.path.join(tmp_path, f"{endpoint}.log")
            self._insert("sensor_a", 0, False)
            self._insert("sensor_a", 0, True)
            events = self._wait_events(path, 2)
            assert sorted(event.eventType for event in events) == ["WINDOW_CLOSE", "WINDOW_OPEN"]
            for event in events:
                self._check_name(event, "sensor_a")
                if calculate and event.eventType == "WINDOW_CLOSE":
                    assert event.result is not None, event.raw
        finally:
            tdStream.dropAllStreamsAndDbs()
            stop_notify_server_background()

    @staticmethod
    def _insert(name, index, close):
        ts = 1792684800000 + index * 10000 + (1000 if close else 0)
        tdSql.execute(f"insert into notify_name.{name} values ({ts}, {0 if close else 1}, 1)")

    @staticmethod
    def _check_name(event, expected):
        assert "triggerTableName" in event.raw, event.raw
        assert isinstance(event.raw["triggerTableName"], str), event.raw
        assert event.raw["triggerTableName"] == expected, event.raw

    @staticmethod
    def _wait_events(path, count, window_start=None):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            events = list(NotifyLog(path).events()) if os.path.exists(path) else []
            if window_start is not None:
                events = [event for event in events if event.windowStart == window_start]
            if len(events) >= count:
                assert len(events) == count, [event.raw for event in events]
                return events
            time.sleep(0.2)
        assert False, f"Expected {count} notifications in {path}"
