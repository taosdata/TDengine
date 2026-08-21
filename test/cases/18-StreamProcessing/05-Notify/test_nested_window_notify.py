import os
import re
import shutil
import time

from new_test_framework.utils import tdLog, tdSql, tdStream
from notify_check import NotifyLog
from stream_notify_server import (
    start_notify_server_background,
    stop_notify_server_background,
)


CALLER_DIR = os.path.dirname(os.path.realpath(__file__))
NOTIFY_RESULT_DIR = os.path.join(CALLER_DIR, "notify_result_tmp")
NOTIFY_PORT = 12345


class TestNestedWindowNotify:
    RETRY_SECONDS = 60
    STABLE_SECONDS = 3

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_same_leaf_range_in_different_lineage_has_distinct_trigger_id(self):
        """Nested notifications distinguish equal leaf ranges by lineage.

        Catalog:
            - Streams:05-Notify:NestedWindow
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 nested lineage notification coverage
        """
        self._run_isolated("nw_notify_lineage", self._check_distinct_lineage_ids)

    def test_same_leaf_open_close_reuses_trigger_id(self):
        """Nested leaf OPEN and CLOSE reuse one trigger identifier.

        Catalog:
            - Streams:05-Notify:NestedWindow
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 nested OPEN/CLOSE identity coverage
        """
        self._run_isolated("nw_notify_pair", self._check_open_close_id)

    def test_nested_multi_start_event_preserves_parent_child_ids(self):
        """Nested multi-start EVENT notifications preserve child parent IDs.

        Catalog:
            - Streams:05-Notify:NestedWindow
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 nested multi-start EVENT coverage
        """
        self._run_isolated("nw_notify_event", self._check_multi_start_event_ids)

    def test_calc_notify_only_does_not_write_result(self):
        """Nested CALC_NOTIFY_ONLY notifies without writing a result table.

        Catalog:
            - Streams:05-Notify:NestedWindow
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 nested notify-only coverage
        """
        self._run_isolated("nw_notify_only", self._check_calc_notify_only)

    def _run_isolated(self, log_name, scenario):
        tdStream.dropAllStreamsAndDbs()
        self._reset_notify_dir()
        start_notify_server_background(port=NOTIFY_PORT, log_path=NOTIFY_RESULT_DIR)
        time.sleep(1)
        log_path = os.path.join(NOTIFY_RESULT_DIR, f"{log_name}.log")
        try:
            scenario(log_path)
        finally:
            stop_notify_server_background()
            tdStream.dropAllStreamsAndDbs()

    def _check_distinct_lineage_ids(self, log_path):
        tdSql.executes(
            [
                "create database nw_notify_lineage vgroups 1",
                "create table nw_notify_lineage.src "
                "(ts timestamp,parent varchar(8),v int)",
                "create stream nw_notify_lineage.s_lineage window ("
                "state_window(parent) extend(1) as w_parent,"
                "interval(10s) sliding(10s) as w_leaf) "
                "from nw_notify_lineage.src stream_options("
                "event_type(window_close)|flush_on_outer_close) "
                f"notify('ws://localhost:{NOTIFY_PORT}/nw_notify_lineage') "
                "on(window_close) into nw_notify_lineage.r_lineage "
                "(leaf_start,parent_key primary key,leaf_end,parent_start,"
                "members,total) as "
                "select w_leaf._twstart,cast(w_parent._twstart as bigint),"
                "w_leaf._twend,w_parent._twstart,count(*),sum(v) "
                "from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_lineage")
        tdSql.execute(
            "insert into nw_notify_lineage.src values "
            "('2025-08-16 00:20:00','A',10) "
            "('2025-08-16 00:20:01','A',20) "
            "('2025-08-16 00:20:02','B',100) "
            "('2025-08-16 00:20:03','A',1000) "
            "('2025-08-16 00:20:04','A',2000) "
            "('2025-08-16 00:20:05','C',10000)"
        )

        self._wait_stable_exact_rows(
            "select leaf_start,leaf_end,parent_start,members,total "
            "from nw_notify_lineage.r_lineage order by parent_start",
            [
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:00",
                    2,
                    30,
                ),
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:02",
                    1,
                    100,
                ),
                (
                    "2025-08-16 00:20:00",
                    "2025-08-16 00:20:09.999000",
                    "2025-08-16 00:20:03",
                    2,
                    3000,
                ),
            ],
        )
        events = self._wait_stable_event_count(log_path, 3)
        expected_start = 1755274800000
        expected_end = 1755274809999
        assert all(event.get("eventType") == "WINDOW_CLOSE" for event in events), events
        assert all(event.get("triggerType") == "Interval" for event in events), events
        assert all(event.get("windowStart") == expected_start for event in events), events
        assert all(event.get("windowEnd") == expected_end for event in events), events
        assert all(event.get("tableName") == "r_lineage" for event in events), events

        payload_rows = []
        for event in events:
            rows = (event.get("result") or {}).get("data") or []
            assert len(rows) == 1, event
            row = rows[0]
            payload_rows.append(
                (
                    row.get("leaf_start"),
                    row.get("leaf_end"),
                    row.get("parent_start"),
                    row.get("members"),
                    row.get("total"),
                )
            )
        assert sorted(payload_rows, key=lambda row: row[2]) == [
            (expected_start, expected_end, 1755274800000, 2, 30),
            (expected_start, expected_end, 1755274802000, 1, 100),
            (expected_start, expected_end, 1755274803000, 2, 3000),
        ], events
        trigger_ids = [event.get("triggerId") for event in events]
        self._assert_canonical_trigger_ids(trigger_ids)
        assert len(set(trigger_ids)) == 3, events

    def _check_open_close_id(self, log_path):
        tdSql.executes(
            [
                "create database nw_notify_pair vgroups 1",
                "create table nw_notify_pair.src "
                "(ts timestamp,parent varchar(8),v int)",
                "create stream nw_notify_pair.s_pair window ("
                "state_window(parent) extend(1) as w_parent,"
                "count_window(2,2) as w_leaf) from nw_notify_pair.src "
                "stream_options(event_type(window_open|window_close)) "
                f"notify('ws://localhost:{NOTIFY_PORT}/nw_notify_pair') "
                "on(window_open|window_close) into nw_notify_pair.r_pair "
                "(leaf_start,parent_start,members,total) as "
                "select w_leaf._twstart,w_parent._twstart,count(*),sum(v) "
                "from nw_notify_pair.src where ts>=_twstart and ts<=_twend",
            ]
        )
        tdStream.checkStreamStatus("s_pair")
        tdSql.execute(
            "insert into nw_notify_pair.src values "
            "('2025-08-16 00:30:00','A',10) "
            "('2025-08-16 00:30:01','A',20)"
        )

        events = self._wait_stable_event_count(log_path, 2)
        assert [event.get("eventType") for event in events] == [
            "WINDOW_OPEN",
            "WINDOW_CLOSE",
        ], events
        assert all(event.get("triggerType") == "Count" for event in events), events
        assert all(event.get("windowStart") == 1755275400000 for event in events), events
        trigger_ids = [event.get("triggerId") for event in events]
        self._assert_canonical_trigger_ids(trigger_ids)
        assert trigger_ids[0] == trigger_ids[1], events

    def _check_multi_start_event_ids(self, log_path):
        tdSql.executes(
            [
                "create database nw_notify_event vgroups 1",
                "create table nw_notify_event.src "
                "(ts timestamp,scope varchar(8),start_a int,start_b int)",
                "create stream nw_notify_event.s_event window ("
                "state_window(scope) extend(1) as w_parent,"
                "event_window(start with (start_a=1,start_b=1)) as w_leaf) "
                "from nw_notify_event.src stream_options("
                "event_type(window_open|window_close)) "
                f"notify('ws://localhost:{NOTIFY_PORT}/nw_notify_event') "
                "on(window_open|window_close) into nw_notify_event.r_event "
                "(leaf_start,parent_start,members) as "
                "select w_leaf._twstart,w_parent._twstart,count(*) "
                "from nw_notify_event.src where ts>=_twstart and ts<=_twend",
            ]
        )
        tdStream.checkStreamStatus("s_event")
        tdSql.execute(
            "insert into nw_notify_event.src values "
            "('2025-08-16 00:40:00','A',1,0)"
        )
        self._wait_event_count(log_path, 1)
        tdSql.execute(
            "insert into nw_notify_event.src values "
            "('2025-08-16 00:40:01','A',0,1)"
        )

        events = self._wait_stable_event_count(log_path, 4)
        actual = [self._subevent_key(event) for event in events]
        expected = [
            ("WINDOW_OPEN", -1, 0, 1755276000000),
            ("WINDOW_OPEN", 0, 0, 1755276000000),
            ("WINDOW_CLOSE", 0, 0, 1755276000000),
            ("WINDOW_OPEN", 1, 1, 1755276001000),
        ]
        assert actual == expected, events
        assert all(event.get("triggerType") == "Event" for event in events), events

        parent_id = events[0].get("triggerId")
        first_child_id = events[1].get("triggerId")
        second_child_id = events[3].get("triggerId")
        self._assert_canonical_trigger_ids(
            [parent_id, first_child_id, events[2].get("triggerId"), second_child_id]
        )
        assert first_child_id != parent_id, events
        assert second_child_id not in (parent_id, first_child_id), events
        assert events[2].get("triggerId") == first_child_id, events
        assert all(
            event.get("parentTriggerId") == parent_id for event in events[1:]
        ), events

    def _check_calc_notify_only(self, log_path):
        tdSql.executes(
            [
                "create database nw_notify_only vgroups 1",
                "create table nw_notify_only.src "
                "(ts timestamp,scope varchar(8),v int)",
                "create stream nw_notify_only.s_notify_only window ("
                "state_window(scope) extend(1) as w_parent,"
                "count_window(1,1) as w_leaf) from nw_notify_only.src "
                "stream_options(calc_notify_only|event_type(window_close)) "
                f"notify('ws://localhost:{NOTIFY_PORT}/nw_notify_only') "
                "on(window_close) into nw_notify_only.r_notify_only "
                "(leaf_start,total) as select w_leaf._twstart,sum(v) from %%trows",
            ]
        )
        tdStream.checkStreamStatus("s_notify_only")
        tdSql.execute(
            "insert into nw_notify_only.src values "
            "('2025-08-16 00:50:00','A',42)"
        )

        events = self._wait_stable_event_count(log_path, 1)
        event = events[0]
        assert event.get("eventType") == "WINDOW_CLOSE", events
        assert event.get("triggerType") == "Count", events
        assert event.get("tableName") == "r_notify_only", events
        rows = (event.get("result") or {}).get("data") or []
        assert len(rows) == 1 and rows[0].get("total") == 42, events
        self._wait_stable_absent_or_empty_table("nw_notify_only", "r_notify_only")

    @staticmethod
    def _reset_notify_dir():
        if os.path.isdir(NOTIFY_RESULT_DIR):
            for name in os.listdir(NOTIFY_RESULT_DIR):
                path = os.path.join(NOTIFY_RESULT_DIR, name)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        else:
            os.makedirs(NOTIFY_RESULT_DIR, exist_ok=True)

    @staticmethod
    def _load_events(log_path):
        return [event.raw for event in NotifyLog(log_path).events()]

    def _wait_event_count(self, log_path, expected_count):
        deadline = time.monotonic() + self.RETRY_SECONDS
        last_events = []
        while time.monotonic() < deadline:
            try:
                last_events = self._load_events(log_path)
            except (FileNotFoundError, ValueError):
                last_events = []
            if len(last_events) > expected_count:
                raise AssertionError(
                    f"expected at most {expected_count} events, got {last_events!r}"
                )
            if len(last_events) == expected_count:
                return last_events
            time.sleep(0.5)
        raise AssertionError(
            f"expected {expected_count} events, got {last_events!r}"
        )

    def _wait_stable_event_count(self, log_path, expected_count):
        events = self._wait_event_count(log_path, expected_count)
        deadline = time.monotonic() + self.STABLE_SECONDS
        while time.monotonic() < deadline:
            time.sleep(0.5)
            try:
                events = self._load_events(log_path)
            except (FileNotFoundError, ValueError):
                events = []
            if len(events) != expected_count:
                raise AssertionError(
                    f"event count changed from {expected_count}: {events!r}"
                )
        return events

    def _wait_stable_exact_rows(self, sql, expected):
        deadline = time.monotonic() + self.RETRY_SECONDS
        actual = []
        while time.monotonic() < deadline:
            try:
                actual = self._query_rows(sql)
            except Exception:
                actual = []
            if self._rows_equal(actual, expected):
                stable_deadline = time.monotonic() + self.STABLE_SECONDS
                while time.monotonic() < stable_deadline:
                    time.sleep(0.5)
                    try:
                        actual = self._query_rows(sql)
                    except Exception:
                        actual = []
                    if not self._rows_equal(actual, expected):
                        break
                else:
                    return
            time.sleep(0.5)
        raise AssertionError(f"unexpected rows for {sql!r}: {actual!r}")

    def _wait_stable_absent_or_empty_table(self, db_name, table_name):
        deadline = time.monotonic() + self.STABLE_SECONDS
        observations = []
        tdSql.execute(f"use {db_name}")
        while time.monotonic() < deadline:
            tdSql.query(f"show tables like '{table_name}'", queryTimes=1)
            table_count = tdSql.getRows()
            if table_count == 0:
                observations.append("absent")
            elif table_count == 1:
                tdSql.query(f"select count(*) from {table_name}", queryTimes=1)
                row_count = tdSql.getData(0, 0)
                observations.append(("present", row_count))
                if row_count != 0:
                    raise AssertionError(
                        f"CALC_NOTIFY_ONLY wrote {row_count} rows to {table_name}"
                    )
            else:
                raise AssertionError(
                    f"unexpected table count for {table_name}: {table_count}"
                )
            time.sleep(0.5)
        tdLog.info(f"CALC_NOTIFY_ONLY result observations: {observations!r}")

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql, queryTimes=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.getCols()))
            for row in range(tdSql.getRows())
        ]

    @staticmethod
    def _rows_equal(actual, expected):
        try:
            return tdSql.checkEqual(actual, expected)
        except Exception:
            return False

    @staticmethod
    def _subevent_key(event):
        condition = event.get("triggerCondition") or {}
        return (
            event.get("eventType"),
            event.get("windowIndex"),
            condition.get("conditionIndex"),
            event.get("windowStart"),
        )

    @staticmethod
    def _assert_canonical_trigger_ids(trigger_ids):
        assert all(
            isinstance(trigger_id, str)
            and re.fullmatch(r"[0-9a-f]{32}", trigger_id)
            for trigger_id in trigger_ids
        ), trigger_ids
