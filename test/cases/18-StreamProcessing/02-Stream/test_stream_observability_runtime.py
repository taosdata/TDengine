import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamObservabilityRuntime:
    STREAM_SQL = (
        "select realtime_lag_ms, input_rows_per_sec_1m, "
        "output_rows_per_sec_1m, runner_result_latency_avg_1m_ms "
        "from information_schema.ins_streams "
        "where db_name='obs' and stream_name='s_obs'"
    )
    TASK_SQL = (
        "select task_id, `type`, deploy_id, last_update, "
        "input_rows_per_sec_1m, "
        "output_rows_per_sec_1m, runner_result_latency_avg_1m_ms "
        "from information_schema.ins_stream_tasks "
        "where stream_name='s_obs' order by `type`, task_id"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql, queryTimes=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.queryCols))
            for row in range(tdSql.getRows())
        ]

    @classmethod
    def _wait_for_task_roles(cls, timeout=20):
        deadline = time.monotonic() + timeout
        rows = []
        while time.monotonic() < deadline:
            rows = cls._query_rows(cls.TASK_SQL)
            types = {row[1] for row in rows}
            runners_by_deploy = {}
            for row in rows:
                if row[1] == "Runner":
                    runners_by_deploy.setdefault(row[2], []).append(row)
            if (
                {"Reader", "Trigger", "Runner"} <= types
                and sum(row[1] == "Reader" for row in rows) == 3
                and sum(row[1] == "Trigger" for row in rows) == 1
                and set(runners_by_deploy) == {0, 1, 2}
                and all(len(runners) == 2 for runners in runners_by_deploy.values())
            ):
                return rows
            time.sleep(0.5)
        raise AssertionError(f"stream task roles did not become ready: {rows!r}")

    @classmethod
    def _wait_for_runtime_values(cls, timeout=30, after_updates=None):
        deadline = time.monotonic() + timeout
        stream_rows = []
        task_rows = []
        while time.monotonic() < deadline:
            stream_rows = cls._query_rows(cls.STREAM_SQL)
            task_rows = cls._query_rows(cls.TASK_SQL)
            if cls._runtime_values_are_visible(
                stream_rows, task_rows, after_updates=after_updates
            ):
                return stream_rows, task_rows
            time.sleep(1)
        raise AssertionError(
            "runtime metrics did not become visible: "
            f"streams={stream_rows!r}, tasks={task_rows!r}"
        )

    @staticmethod
    def _runtime_values_are_visible(stream_rows, task_rows, after_updates=None):
        if len(stream_rows) != 1:
            return False
        lag, input_rate, output_rate, latency = stream_rows[0]
        if (
            lag is None
            or input_rate is None
            or input_rate <= 0
            or output_rate is None
            or output_rate <= 0
            or latency is None
            or latency < 0
        ):
            return False

        try:
            TestStreamObservabilityRuntime._assert_task_applicability(
                task_rows, window_ready=True
            )
        except AssertionError:
            return False
        if after_updates is not None:
            current_updates = {row[0]: row[3] for row in task_rows}
            if current_updates == after_updates:
                return False
        return True

    @staticmethod
    def _assert_task_applicability(rows, window_ready):
        readers = [row for row in rows if row[1] == "Reader"]
        triggers = [row for row in rows if row[1] == "Trigger"]
        runners_by_deploy = {}
        for row in rows:
            if row[1] == "Runner":
                runners_by_deploy.setdefault(row[2], []).append(row)

        if len(readers) != 3 or len(triggers) != 1:
            raise AssertionError(f"required Reader/Trigger topology is missing: {rows!r}")
        if set(runners_by_deploy) != {0, 1, 2} or any(
            len(runners) != 2 for runners in runners_by_deploy.values()
        ):
            raise AssertionError(f"required Runner deploy topology is missing: {rows!r}")

        # This fixture creates the trigger Reader before its two calc Readers and
        # appends the root Runner last in every deploy. Task IDs are assigned in
        # that construction order, so role identity does not depend on metrics or
        # the lossy extra_info field.
        entry_reader_id = min(int(row[0], 16) for row in readers)
        active_final_runners = []

        for row in rows:
            task_id, task_type, _, _, input_rate, output_rate, latency = row
            if task_type == "Reader":
                entry_reader = int(task_id, 16) == entry_reader_id
                if window_ready and entry_reader:
                    if input_rate is None or input_rate <= 0:
                        raise AssertionError(f"entry Reader input is invalid: {row!r}")
                elif input_rate is not None:
                    raise AssertionError(f"inapplicable Reader exposes input: {row!r}")
                if output_rate is not None or latency is not None:
                    raise AssertionError(f"Reader exposes Runner metrics: {row!r}")
            elif task_type == "Runner":
                if input_rate is not None:
                    raise AssertionError(f"Runner exposes Reader input: {row!r}")
            elif task_type == "Trigger":
                if (
                    input_rate is not None
                    or output_rate is not None
                    or latency is not None
                ):
                    raise AssertionError(f"Trigger exposes task runtime metrics: {row!r}")
            elif (
                input_rate is not None
                or output_rate is not None
                or latency is not None
            ):
                raise AssertionError(f"inapplicable task exposes runtime metrics: {row!r}")

        for deploy_id, runners in runners_by_deploy.items():
            ordinary, final = sorted(runners, key=lambda row: int(row[0], 16))
            if window_ready:
                if any(value is not None for value in ordinary[4:7]):
                    raise AssertionError(
                        f"ordinary Runner in deploy {deploy_id} exposes metrics: "
                        f"{ordinary!r}"
                    )
                output_rate, latency = final[5], final[6]
                if output_rate is None or output_rate < 0:
                    raise AssertionError(f"final Runner metrics are invalid: {final!r}")
                if output_rate == 0 and latency is not None:
                    raise AssertionError(
                        f"empty final Runner exposes latency: {final!r}"
                    )
                if output_rate > 0:
                    if latency is None or latency < 0:
                        raise AssertionError(
                            f"active final Runner latency is invalid: {final!r}"
                        )
                    active_final_runners.append(final)
            elif any(value is not None for value in ordinary[4:7] + final[4:7]):
                raise AssertionError(
                    f"Runner window in deploy {deploy_id} is ready too early: "
                    f"{runners!r}"
                )

        if window_ready and not active_final_runners:
            raise AssertionError(f"active final Runner is missing: {rows!r}")

    @staticmethod
    def _assert_schema():
        expected = {
            "realtime_lag_ms": "BIGINT",
            "input_rows_per_sec_1m": "DOUBLE",
            "output_rows_per_sec_1m": "DOUBLE",
            "runner_result_latency_avg_1m_ms": "DOUBLE",
        }
        tdSql.query("desc information_schema.ins_streams")
        stream_schema = {
            tdSql.getData(row, 0): str(tdSql.getData(row, 1)).upper()
            for row in range(tdSql.getRows())
        }
        for name, data_type in expected.items():
            if stream_schema.get(name) != data_type:
                raise AssertionError(
                    f"ins_streams.{name} type is {stream_schema.get(name)!r}, "
                    f"expected {data_type}"
                )

        tdSql.query("desc information_schema.ins_stream_tasks")
        task_schema = {
            tdSql.getData(row, 0): str(tdSql.getData(row, 1)).upper()
            for row in range(tdSql.getRows())
        }
        for name in (
            "input_rows_per_sec_1m",
            "output_rows_per_sec_1m",
            "runner_result_latency_avg_1m_ms",
        ):
            if task_schema.get(name) != "DOUBLE":
                raise AssertionError(
                    f"ins_stream_tasks.{name} type is {task_schema.get(name)!r}, "
                    "expected DOUBLE"
                )

    def test_stream_observability_runtime(self):
        """Runtime observability crosses the heartbeat-to-view pipeline.

        1. Verify the appended system-view schema and first-window NULLs.
        2. Write batches for more than 65 seconds and wait for heartbeats.
        3. Verify stable runtime values and task-role applicability.

        Catalog: Streams:Observability
        Since: v3.4.0.0
        Labels: stream,observability,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7045278024

        History:
            - 2026-08-13 OpenAI GPT-5 Created
        """
        tdStream.createSnode(1)
        tdSql.executes(
            [
                "drop database if exists obs",
                "create database obs vgroups 1 buffer 8",
                "create table obs.events (ts timestamp, value int)",
                "create table obs.marker (ts timestamp, value int)",
                "insert into obs.marker values(now, 7)",
                "create table obs.result (wstart timestamp, total bigint)",
                "create stream obs.s_obs interval(1s) sliding(1s) "
                "from obs.events into obs.result "
                "as select _twstart wstart, _twrownum total from obs.events "
                "where ts >= _twstart and ts < _twend and value >= "
                "(select last_row(value) from obs.marker)",
            ]
        )
        tdStream.checkStreamStatus("s_obs")

        self._assert_schema()
        tdSql.query(self.STREAM_SQL)
        tdSql.checkRows(1)
        if any(tdSql.getData(0, col) is not None for col in range(1, 4)):
            raise AssertionError(f"stream window is ready too early: {tdSql.queryResult!r}")

        initial_task_rows = self._wait_for_task_roles()
        self._assert_task_applicability(initial_task_rows, window_ready=False)

        write_started = time.monotonic()
        batch = 0
        while time.monotonic() - write_started <= 66:
            now_ms = int(time.time() * 1000)
            tdSql.execute(
                "insert into obs.events values"
                f"({now_ms}, {batch * 2}) ({now_ms + 1}, {batch * 2 + 1})"
            )
            batch += 1
            time.sleep(1)
        write_elapsed = time.monotonic() - write_started
        if write_elapsed <= 65:
            raise AssertionError(f"runtime write interval was too short: {write_elapsed}")

        stream_rows, task_rows = self._wait_for_runtime_values()
        self._assert_task_applicability(task_rows, window_ready=True)
        tdLog.info(f"first stable runtime snapshot: {stream_rows!r}, {task_rows!r}")

        first_updates = {row[0]: row[3] for row in task_rows}
        time.sleep(2)
        stream_rows, task_rows = self._wait_for_runtime_values(
            timeout=10, after_updates=first_updates
        )
        self._assert_task_applicability(task_rows, window_ready=True)
        tdLog.info(f"second stable runtime snapshot: {stream_rows!r}, {task_rows!r}")
