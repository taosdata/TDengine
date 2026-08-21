import datetime
import os
import re
import shutil
import sys
import tempfile
import time

NOTIFY_HELPER_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "05-Notify")
)
if NOTIFY_HELPER_DIR not in sys.path:
    sys.path.insert(0, NOTIFY_HELPER_DIR)

from new_test_framework.utils import (
    clusterComCheck,
    sc,
    tdLog,
    tdSql,
    tdStream,
)
from notify_check import NotifyLog
from stream_notify_server import (
    start_notify_server_background,
    stop_notify_server_background,
)


class TestNestedWindowRecovery:
    RETRY_SECONDS = 60
    STABLE_SECONDS = 3
    NOTIFY_PORT = 12346

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_restart_with_ancestor_open_only(self):
        """Nested recovery: restart with only the ancestor scope open.

        Catalog:
            - Streams:08-Recalc:NestedWindowRecovery
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 ancestor-only recovery cut
        """
        self._run_isolated("nw_recover_ancestor", self._check_ancestor_open_only)

    def test_restart_with_active_leaf(self):
        """Nested recovery: restart with an active COUNT leaf.

        Catalog:
            - Streams:08-Recalc:NestedWindowRecovery
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 active-leaf recovery cut
        """
        self._run_isolated("nw_recover_active", self._check_active_leaf)

    def test_restart_with_completed_and_active_leaves(self):
        """Nested recovery: restart after one leaf closes and another opens.

        Catalog:
            - Streams:08-Recalc:NestedWindowRecovery
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 mixed-leaf recovery cut
        """
        self._run_isolated("nw_recover_mixed", self._check_completed_and_active)

    def test_two_gid_multi_vgroup_normal_source_resume(self):
        """Nested recovery: two normal-source gids resume across vgroups.

        Catalog:
            - Streams:08-Recalc:NestedWindowRecovery
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 normal multi-vgroup recovery
        """
        self._run_isolated("nw_recover_gids", self._check_normal_multi_vgroup)

    def test_cross_db_virtual_multi_vgroup_resume(self):
        """Nested recovery: cross-DB virtual inputs resume across vgroups.

        Catalog:
            - Streams:08-Recalc:NestedWindowRecovery
        Since: v3.4.2.0
        Labels: common,ci,integration,functional
        Feishu: None
        History:
            - 2026-08-16 GPT-5 Added P0 virtual multi-vgroup recovery
        """
        self._run_isolated("nw_recover_virtual", self._check_virtual_multi_vgroup)

    def _run_isolated(self, db_name, scenario):
        self._dnode_stopped = False
        self._notify_dir = tempfile.mkdtemp(
            prefix=f"nested-window-recovery-{db_name}-"
        )
        first_error = None
        first_traceback = None
        try:
            tdStream.dropAllStreamsAndDbs()
            start_notify_server_background(
                port=self.NOTIFY_PORT, log_path=self._notify_dir
            )
            time.sleep(1)
            scenario(db_name)
        except BaseException as error:
            first_error = error
            first_traceback = error.__traceback__
        finally:
            try:
                if self._dnode_stopped:
                    sc.dnodeStart(1)
                    clusterComCheck.checkDnodes(1)
                    self._dnode_stopped = False
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
            try:
                tdStream.dropAllStreamsAndDbs()
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
            try:
                stop_notify_server_background()
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
            try:
                shutil.rmtree(self._notify_dir)
            except BaseException as error:
                if first_error is None:
                    first_error = error
                    first_traceback = error.__traceback__
        if first_error is not None:
            raise first_error.with_traceback(first_traceback)

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

    def _notify_clause(self, db_name, stream_name):
        path = f"{db_name}_{stream_name}"
        return (
            f"notify('ws://localhost:{self.NOTIFY_PORT}/{path}') "
            "on(window_close)"
        )

    def _notify_log_path(self, db_name, stream_name):
        return os.path.join(self._notify_dir, f"{db_name}_{stream_name}.log")

    @staticmethod
    def _timestamp_ms(value):
        return int(datetime.datetime.fromisoformat(value).timestamp() * 1000)

    def _load_notifications(self, db_name, stream_name):
        path = self._notify_log_path(db_name, stream_name)
        return list(NotifyLog(path).events())

    def _wait_notifications(
        self,
        db_name,
        stream_name,
        table_name,
        expected,
        stable_seconds=None,
    ):
        if stable_seconds is None:
            stable_seconds = self.STABLE_SECONDS
        expected_keys = [
            (self._timestamp_ms(start), self._timestamp_ms(end), trigger_type)
            for start, end, trigger_type in expected
        ]

        def observe():
            try:
                events = self._load_notifications(db_name, stream_name)
            except (FileNotFoundError, ValueError):
                events = []
            for event in events:
                assert event.streamName == f"{db_name}.{stream_name}", events
                assert event.eventType == "WINDOW_CLOSE", events
                assert event.tableName == table_name or event.tableName.startswith(
                    f"{table_name}_"
                ), events
                assert isinstance(event.triggerId, str), events
                assert re.fullmatch(r"[0-9a-f]{32}", event.triggerId), events
            trigger_ids = [event.triggerId for event in events]
            assert len(trigger_ids) == len(set(trigger_ids)), events
            keys = [
                (event.windowStart, event.windowEnd, event.triggerType)
                for event in events
            ]
            return events, sorted(keys), sorted(expected_keys)

        deadline = time.monotonic() + self.RETRY_SECONDS
        actual_events = []
        actual_keys = []
        expected_sorted = sorted(expected_keys)
        while time.monotonic() < deadline:
            actual_events, actual_keys, expected_sorted = observe()
            if len(actual_keys) > len(expected_sorted):
                raise AssertionError(
                    f"duplicate notifications for {db_name}.{stream_name}: "
                    f"{actual_keys!r}"
                )
            if actual_keys == expected_sorted:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                f"unexpected notifications for {db_name}.{stream_name}: "
                f"actual={actual_keys!r}, expected={expected_sorted!r}"
            )

        stable_deadline = time.monotonic() + stable_seconds
        while time.monotonic() < stable_deadline:
            time.sleep(0.5)
            stable_events, stable_keys, _ = observe()
            if stable_keys != expected_sorted:
                raise AssertionError(
                    f"notifications changed for {db_name}.{stream_name}: "
                    f"{stable_keys!r}"
                )
            actual_events = stable_events
        tdLog.info(
            f"stable public notifications for {db_name}.{stream_name}: "
            f"{actual_keys!r}"
        )
        return actual_events

    def _wait_publications(
        self, count_sql, rows_sql, expected, stable_seconds=None
    ):
        if stable_seconds is None:
            stable_seconds = self.STABLE_SECONDS
        expected_count = len(expected)
        deadline = time.monotonic() + self.RETRY_SECONDS
        actual_count = None
        actual_rows = []
        while time.monotonic() < deadline:
            try:
                count_rows = self._query_rows(count_sql)
                actual_count = count_rows[0][0] if len(count_rows) == 1 else None
                actual_rows = self._query_rows(rows_sql)
            except Exception:
                actual_count = None
                actual_rows = []
            if actual_count == expected_count and self._rows_equal(
                actual_rows, expected
            ):
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                "unexpected publications: "
                f"count={actual_count!r}, rows={actual_rows!r}, sql={rows_sql!r}"
            )

        stable_deadline = time.monotonic() + stable_seconds
        while time.monotonic() < stable_deadline:
            time.sleep(0.5)
            count_rows = self._query_rows(count_sql)
            actual_count = count_rows[0][0] if len(count_rows) == 1 else None
            actual_rows = self._query_rows(rows_sql)
            if actual_count != expected_count or not self._rows_equal(
                actual_rows, expected
            ):
                raise AssertionError(
                    "publications changed during stable period: "
                    f"count={actual_count!r}, rows={actual_rows!r}"
                )
        tdLog.info(
            "stable public publications: "
            f"count={actual_count!r}, rows={actual_rows!r}"
        )
        return actual_rows

    def _wait_stream_status(self, db_name, stream_name, expected_status):
        sql = (
            "select status,message from information_schema.ins_streams "
            f"where db_name='{db_name}' and stream_name='{stream_name}'"
        )
        deadline = time.monotonic() + self.RETRY_SECONDS
        actual = []
        while time.monotonic() < deadline:
            try:
                actual = self._query_rows(sql)
            except Exception:
                actual = []
            if len(actual) == 1 and actual[0][0] == expected_status:
                tdLog.info(
                    f"stream {stream_name} terminal {expected_status}: {actual!r}"
                )
                return actual[0]
            if expected_status == "Running" and actual and actual[0][0] == "Failed":
                raise AssertionError(
                    f"stream failed before reaching Running: {actual!r}"
                )
            time.sleep(0.5)
        raise AssertionError(
            f"stream {stream_name} did not reach {expected_status}: {actual!r}"
        )

    def _wait_trigger_running(self, stream_name):
        sql = (
            "select status,`message` from information_schema.ins_stream_tasks "
            f"where stream_name='{stream_name}' and type='Trigger'"
        )
        deadline = time.monotonic() + self.RETRY_SECONDS
        actual = []
        while time.monotonic() < deadline:
            try:
                actual = self._query_rows(sql)
            except Exception:
                actual = []
            if len(actual) == 1 and actual[0][0] == "Running":
                tdLog.info(f"trigger terminal running row: {actual!r}")
                return actual[0]
            if actual and actual[0][0] == "Failed":
                raise AssertionError(f"trigger failed for {stream_name}: {actual!r}")
            time.sleep(0.5)
        raise AssertionError(
            f"trigger did not reach Running for {stream_name}: {actual!r}"
        )

    def _restart_dnode(self):
        self._dnode_stopped = True
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        self._dnode_stopped = False

    def _create_count_pair(self, db_name, output_db):
        tdSql.executes(
            [
                f"create database {db_name} vgroups 1",
                f"create stable {db_name}.control_src "
                "(ts timestamp,v int) tags(marker int)",
                f"create table {db_name}.control using {db_name}.control_src tags(1)",
                f"create stable {db_name}.target_src "
                "(ts timestamp,v int) tags(marker int)",
                f"create table {db_name}.target using {db_name}.target_src tags(1)",
                f"create table {db_name}.sentinel using {db_name}.target_src tags(2)",
                f"create stream {db_name}.s_control window ("
                "interval(1h) sliding(1h) as w_outer,count_window(2,2)) "
                f"from {db_name}.control_src partition by tbname "
                f"{self._notify_clause(db_name, 's_control')} "
                f"into {output_db}.r_control output_subtable("
                "concat('r_control_',tbname)) "
                "(published_at,publication_id composite key,leaf_start,outer_start,"
                "first_v,last_v,members,total) "
                "tags(source varchar(64) as tbname) as select now(),"
                "cast(_tlocaltime as bigint),"
                "_twstart,"
                "w_outer._twstart,first(v),last(v),count(*),sum(v) from %%trows",
                f"create stream {db_name}.s_target window ("
                "interval(1h) sliding(1h) as w_outer,count_window(2,2)) "
                f"from {db_name}.target_src partition by tbname "
                f"{self._notify_clause(db_name, 's_target')} "
                f"into {output_db}.r_target output_subtable("
                "concat('r_target_',tbname)) "
                "(published_at,publication_id composite key,leaf_start,outer_start,"
                "first_v,last_v,members,total) "
                "tags(source varchar(64) as tbname) as select now(),"
                "cast(_tlocaltime as bigint),"
                "_twstart,"
                "w_outer._twstart,first(v),last(v),count(*),sum(v) from %%trows",
            ]
        )
        self._wait_trigger_running("s_control")
        self._wait_trigger_running("s_target")

    @staticmethod
    def _count_rows_sql(output_db, table_name):
        return (
            "select source,leaf_start,outer_start,first_v,last_v,members,total "
            f"from {output_db}.{table_name} "
            "order by source,leaf_start,published_at"
        )

    def _check_ancestor_open_only(self, db_name):
        output_db = db_name
        self._create_count_pair(db_name, output_db)
        control_rows = self._count_rows_sql(output_db, "r_control")
        target_rows = self._count_rows_sql(output_db, "r_target")
        control_expected = [
            (
                "control",
                "2025-08-18 00:00:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
            (
                "control",
                "2025-08-18 00:00:02",
                "2025-08-18 00:00:00",
                30,
                40,
                2,
                70,
            ),
        ]
        tdSql.execute(
            f"insert into {db_name}.control values "
            "('2025-08-18 00:00:00',10) ('2025-08-18 00:00:01',20)"
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected[:1],
        )
        self._wait_notifications(
            db_name,
            "s_control",
            "r_control",
            [("2025-08-18 00:00:00", "2025-08-18 00:00:01", "Count")],
        )
        tdSql.execute(
            f"insert into {db_name}.control values "
            "('2025-08-18 00:00:02',30) ('2025-08-18 00:00:03',40)"
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )
        self._wait_notifications(
            db_name,
            "s_control",
            "r_control",
            [
                ("2025-08-18 00:00:00", "2025-08-18 00:00:01", "Count"),
                ("2025-08-18 00:00:02", "2025-08-18 00:00:03", "Count"),
            ],
        )
        tdSql.execute(
            f"insert into {db_name}.target values "
            "('2025-08-18 00:00:00',10) ('2025-08-18 00:00:01',20)"
        )
        sentinel = [
            (
                "target",
                "2025-08-18 00:00:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, sentinel
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [("2025-08-18 00:00:00", "2025-08-18 00:00:01", "Count")],
        )
        self._restart_dnode()
        self._wait_stream_status(db_name, "s_target", "Running")
        tdSql.execute(
            f"insert into {db_name}.target values "
            "('2025-08-18 00:00:02',30) ('2025-08-18 00:00:03',40)"
        )
        expected = [
            (
                "target",
                "2025-08-18 00:00:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
            (
                "target",
                "2025-08-18 00:00:02",
                "2025-08-18 00:00:00",
                30,
                40,
                2,
                70,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, expected
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [
                ("2025-08-18 00:00:00", "2025-08-18 00:00:01", "Count"),
                ("2025-08-18 00:00:02", "2025-08-18 00:00:03", "Count"),
            ],
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )

    def _check_active_leaf(self, db_name):
        output_db = db_name
        self._create_count_pair(db_name, output_db)
        control_rows = self._count_rows_sql(output_db, "r_control")
        target_rows = self._count_rows_sql(output_db, "r_target")
        control_expected = [
            (
                "control",
                "2025-08-18 00:10:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            )
        ]
        tdSql.execute(
            f"insert into {db_name}.control values "
            "('2025-08-18 00:10:00',10) ('2025-08-18 00:10:01',20)"
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )
        self._wait_notifications(
            db_name,
            "s_control",
            "r_control",
            [("2025-08-18 00:10:00", "2025-08-18 00:10:01", "Count")],
        )
        tdSql.execute(
            f"insert into {db_name}.target values ('2025-08-18 00:10:00',10) "
            f"{db_name}.sentinel values "
            "('2025-08-18 00:10:10',100) ('2025-08-18 00:10:11',200)"
        )
        sentinel = [
            (
                "sentinel",
                "2025-08-18 00:10:10",
                "2025-08-18 00:00:00",
                100,
                200,
                2,
                300,
            )
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, sentinel
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [("2025-08-18 00:10:10", "2025-08-18 00:10:11", "Count")],
        )
        self._restart_dnode()
        self._wait_stream_status(db_name, "s_target", "Running")
        tdSql.execute(
            f"insert into {db_name}.target values ('2025-08-18 00:10:01',20)"
        )
        expected = [
            (
                "sentinel",
                "2025-08-18 00:10:10",
                "2025-08-18 00:00:00",
                100,
                200,
                2,
                300,
            ),
            (
                "target",
                "2025-08-18 00:10:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            )
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, expected
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [
                ("2025-08-18 00:10:00", "2025-08-18 00:10:01", "Count"),
                ("2025-08-18 00:10:10", "2025-08-18 00:10:11", "Count"),
            ],
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )

    def _check_completed_and_active(self, db_name):
        output_db = db_name
        self._create_count_pair(db_name, output_db)
        control_rows = self._count_rows_sql(output_db, "r_control")
        target_rows = self._count_rows_sql(output_db, "r_target")
        control_expected = [
            (
                "control",
                "2025-08-18 00:20:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
            (
                "control",
                "2025-08-18 00:20:02",
                "2025-08-18 00:00:00",
                30,
                40,
                2,
                70,
            ),
        ]
        tdSql.execute(
            f"insert into {db_name}.control values "
            "('2025-08-18 00:20:00',10) ('2025-08-18 00:20:01',20)"
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected[:1],
        )
        self._wait_notifications(
            db_name,
            "s_control",
            "r_control",
            [("2025-08-18 00:20:00", "2025-08-18 00:20:01", "Count")],
        )
        tdSql.execute(
            f"insert into {db_name}.control values "
            "('2025-08-18 00:20:02',30) ('2025-08-18 00:20:03',40)"
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )
        self._wait_notifications(
            db_name,
            "s_control",
            "r_control",
            [
                ("2025-08-18 00:20:00", "2025-08-18 00:20:01", "Count"),
                ("2025-08-18 00:20:02", "2025-08-18 00:20:03", "Count"),
            ],
        )
        tdSql.execute(
            f"insert into {db_name}.target values "
            "('2025-08-18 00:20:00',10) ('2025-08-18 00:20:01',20) "
            "('2025-08-18 00:20:02',30) "
            f"{db_name}.sentinel values "
            "('2025-08-18 00:20:10',100) ('2025-08-18 00:20:11',200)"
        )
        pre_cut = [
            (
                "sentinel",
                "2025-08-18 00:20:10",
                "2025-08-18 00:00:00",
                100,
                200,
                2,
                300,
            ),
            (
                "target",
                "2025-08-18 00:20:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, pre_cut
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [
                ("2025-08-18 00:20:00", "2025-08-18 00:20:01", "Count"),
                ("2025-08-18 00:20:10", "2025-08-18 00:20:11", "Count"),
            ],
        )
        self._restart_dnode()
        self._wait_stream_status(db_name, "s_target", "Running")
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, pre_cut
        )
        tdSql.execute(
            f"insert into {db_name}.target values ('2025-08-18 00:20:03',40)"
        )
        expected = [
            (
                "sentinel",
                "2025-08-18 00:20:10",
                "2025-08-18 00:00:00",
                100,
                200,
                2,
                300,
            ),
            (
                "target",
                "2025-08-18 00:20:00",
                "2025-08-18 00:00:00",
                10,
                20,
                2,
                30,
            ),
            (
                "target",
                "2025-08-18 00:20:02",
                "2025-08-18 00:00:00",
                30,
                40,
                2,
                70,
            )
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_target", target_rows, expected
        )
        self._wait_notifications(
            db_name,
            "s_target",
            "r_target",
            [
                ("2025-08-18 00:20:00", "2025-08-18 00:20:01", "Count"),
                ("2025-08-18 00:20:02", "2025-08-18 00:20:03", "Count"),
                ("2025-08-18 00:20:10", "2025-08-18 00:20:11", "Count"),
            ],
        )
        self._wait_publications(
            f"select count(*) from {output_db}.r_control",
            control_rows,
            control_expected,
        )

    def _find_two_physical_table_pairs(self, db_name, stable_name, prefix):
        by_vgroup = {}
        for index in range(32):
            table = f"{prefix}{index}"
            tdSql.execute(
                f"create table {db_name}.{table} using {db_name}.{stable_name} "
                f"tags({index + 1})"
            )
            rows = self._query_rows(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name='{db_name}' and table_name='{table}'"
            )
            by_vgroup.setdefault(rows[0][0], []).append(table)
            ready = [item for item in by_vgroup.items() if len(item[1]) >= 2]
            if len(ready) >= 2:
                chosen = ready[:2]
                tdLog.info(f"selected physical vgroup table pairs: {chosen!r}")
                return [(vgid, tables[0], tables[1]) for vgid, tables in chosen]
        raise AssertionError(
            f"physical tables did not provide two vgroup pairs: {by_vgroup!r}"
        )

    def _check_normal_multi_vgroup(self, db_name):
        output_db = db_name
        tdSql.executes(
            [
                f"create database {db_name} vgroups 4",
                f"create stable {db_name}.src (ts timestamp,v int) tags(gid int)",
            ]
        )
        pairs = self._find_two_physical_table_pairs(db_name, "src", "g")
        target_a, sentinel_a = pairs[0][1], pairs[0][2]
        target_b, sentinel_b = pairs[1][1], pairs[1][2]
        tdSql.executes(
            [
                f"alter table {db_name}.{target_a} set tag gid=1",
                f"alter table {db_name}.{target_b} set tag gid=2",
                f"alter table {db_name}.{sentinel_a} set tag gid=101",
                f"alter table {db_name}.{sentinel_b} set tag gid=102",
                f"create stream {db_name}.s_gids window ("
                "interval(1h) sliding(1h) as w_outer,session(ts,10s)) "
                f"from {db_name}.src partition by gid "
                f"{self._notify_clause(db_name, 's_gids')} "
                f"into {output_db}.r_gids output_subtable("
                "concat('r_gid_',cast(%%1 as varchar))) "
                "(published_at,publication_id composite key,leaf_start,outer_start,"
                "members,total) tags(gid int as %%1) as select "
                "now(),cast(_tlocaltime as bigint),_twstart,"
                "w_outer._twstart,count(*),sum(v) from %%trows",
            ]
        )
        self._wait_trigger_running("s_gids")
        tdSql.execute(
            f"insert into {db_name}.{target_a} values "
            "('2025-08-18 00:30:00',10) "
            f"{db_name}.{target_b} values ('2025-08-18 00:30:00',100) "
            f"{db_name}.{sentinel_a} values "
            "('2025-08-18 00:30:00',1000) "
            "('2025-08-18 00:30:01',2000) "
            "('2025-08-18 00:30:20',4000) "
            f"{db_name}.{sentinel_b} values "
            "('2025-08-18 00:30:00',10000) "
            "('2025-08-18 00:30:01',20000) "
            "('2025-08-18 00:30:20',40000)"
        )
        rows_sql = (
            "select gid,leaf_start,outer_start,members,total "
            f"from {output_db}.r_gids order by gid,leaf_start,published_at"
        )
        pre_cut = [
            (
                101,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                3000,
            ),
            (
                102,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                30000,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_gids", rows_sql, pre_cut
        )
        self._wait_notifications(
            db_name,
            "s_gids",
            "r_gid",
            [
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
            ],
        )
        self._restart_dnode()
        self._wait_stream_status(db_name, "s_gids", "Running")
        tdSql.execute(
            f"insert into {db_name}.{target_a} values "
            "('2025-08-18 00:30:01',20) ('2025-08-18 00:30:20',40) "
            f"{db_name}.{target_b} values "
            "('2025-08-18 00:30:01',200) ('2025-08-18 00:30:20',400)"
        )
        expected = [
            (
                1,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                30,
            ),
            (
                2,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                300,
            ),
            (
                101,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                3000,
            ),
            (
                102,
                "2025-08-18 00:30:00",
                "2025-08-18 00:00:00",
                2,
                30000,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_gids", rows_sql, expected
        )
        self._wait_notifications(
            db_name,
            "s_gids",
            "r_gid",
            [
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
                ("2025-08-18 00:30:00", "2025-08-18 00:30:01", "Session"),
            ],
        )

    def _check_virtual_multi_vgroup(self, db_name):
        physical_db = f"{db_name}_physical"
        output_db = db_name
        tdSql.executes(
            [
                f"create database {physical_db} vgroups 4",
                f"create stable {physical_db}.src "
                "(ts timestamp,v int,status int) tags(slot int)",
            ]
        )
        pairs = self._find_two_physical_table_pairs(physical_db, "src", "p")
        target_a, sentinel_a = pairs[0][1], pairs[0][2]
        target_b, sentinel_b = pairs[1][1], pairs[1][2]
        tdSql.executes(
            [
                f"create database {db_name} vgroups 2",
                f"create stable {db_name}.vsrc "
                "(ts timestamp,v int,status int) tags(device int) virtual 1",
                f"create vtable {db_name}.vt_a ("
                f"v from {physical_db}.{target_a}.v,"
                f"status from {physical_db}.{target_a}.status) "
                f"using {db_name}.vsrc tags(1)",
                f"create vtable {db_name}.vt_b ("
                f"v from {physical_db}.{target_b}.v,"
                f"status from {physical_db}.{target_b}.status) "
                f"using {db_name}.vsrc tags(2)",
                f"create vtable {db_name}.vt_sa ("
                f"v from {physical_db}.{sentinel_a}.v,"
                f"status from {physical_db}.{sentinel_a}.status) "
                f"using {db_name}.vsrc tags(101)",
                f"create vtable {db_name}.vt_sb ("
                f"v from {physical_db}.{sentinel_b}.v,"
                f"status from {physical_db}.{sentinel_b}.status) "
                f"using {db_name}.vsrc tags(102)",
                f"create stream {db_name}.s_virtual window ("
                "interval(1h) sliding(1h) as w_outer,count_window(2,2)) "
                f"from {db_name}.vsrc partition by tbname "
                f"{self._notify_clause(db_name, 's_virtual')} "
                f"into {output_db}.r_virtual output_subtable("
                "concat('r_v_',tbname)) "
                "(published_at,publication_id composite key,leaf_start,outer_start,"
                "members,total) tags(source varchar(64) as tbname) as select "
                "now(),cast(_tlocaltime as bigint),_twstart,"
                "w_outer._twstart,count(*),sum(v) from %%trows",
            ]
        )
        self._wait_trigger_running("s_virtual")
        tdSql.execute(
            f"insert into {physical_db}.{target_a} values "
            "('2025-08-18 00:40:00',10,1) "
            f"{physical_db}.{target_b} values "
            "('2025-08-18 00:40:00',100,1) "
            f"{physical_db}.{sentinel_a} values "
            "('2025-08-18 00:40:00',1000,1) "
            "('2025-08-18 00:40:01',2000,1) "
            f"{physical_db}.{sentinel_b} values "
            "('2025-08-18 00:40:00',10000,1) "
            "('2025-08-18 00:40:01',20000,1)"
        )
        rows_sql = (
            "select source,leaf_start,outer_start,members,total "
            f"from {output_db}.r_virtual "
            "order by source,leaf_start,published_at"
        )
        pre_cut = [
            (
                "vt_sa",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                3000,
            ),
            (
                "vt_sb",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                30000,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_virtual", rows_sql, pre_cut
        )
        self._wait_notifications(
            db_name,
            "s_virtual",
            "r_v",
            [
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
            ],
        )
        self._restart_dnode()
        self._wait_stream_status(db_name, "s_virtual", "Running")
        tdSql.execute(
            f"insert into {physical_db}.{target_a} values "
            "('2025-08-18 00:40:01',20,1) "
            f"{physical_db}.{target_b} values "
            "('2025-08-18 00:40:01',200,1)"
        )
        expected = [
            (
                "vt_a",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                30,
            ),
            (
                "vt_b",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                300,
            ),
            (
                "vt_sa",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                3000,
            ),
            (
                "vt_sb",
                "2025-08-18 00:40:00",
                "2025-08-18 00:00:00",
                2,
                30000,
            ),
        ]
        self._wait_publications(
            f"select count(*) from {output_db}.r_virtual", rows_sql, expected
        )
        self._wait_notifications(
            db_name,
            "s_virtual",
            "r_v",
            [
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
                ("2025-08-18 00:40:00", "2025-08-18 00:40:01", "Count"),
            ],
        )
