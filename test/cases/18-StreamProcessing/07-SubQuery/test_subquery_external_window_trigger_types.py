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

import os
import re
import time

from new_test_framework.utils import tdLog, tdSql, tdStream
from new_test_framework.utils.srvCtl import sc


class TestSubqueryExternalWindowTriggerTypes:
    case_db = "ext_win_triggers"
    source_table = "source_data"
    wait_seconds = 30

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def wait_result_rows(self, result_table, expected_rows):
        actual_rows = 0
        for _ in range(self.wait_seconds):
            tdSql.query(f"select count(*) from `{self.case_db}`.`{result_table}`")
            actual_rows = tdSql.getData(0, 0)
            if actual_rows == expected_rows:
                return
            time.sleep(1)
        tdLog.exit(
            f"stream result {self.case_db}.{result_table} row count mismatch, "
            f"expected {expected_rows}, actual {actual_rows}"
        )

    def reset_taosd_log(self):
        log_files = [sc.dnodeLogPath(1) + f"/taosdlog.{index}" for index in range(2)]
        previous_mtimes = {}
        for log_file in log_files:
            try:
                previous_mtimes[log_file] = os.stat(log_file).st_mtime_ns
            except FileNotFoundError:
                previous_mtimes[log_file] = None

        tdSql.execute("alter all dnodes 'resetlog'")
        marker = "reset log file"
        for _ in range(self.wait_seconds):
            active_logs = []
            for log_file in log_files:
                try:
                    stat_result = os.stat(log_file)
                    if stat_result.st_mtime_ns == previous_mtimes[log_file]:
                        continue
                    with open(log_file, "r", encoding="utf-8", errors="ignore") as log:
                        if marker in log.read():
                            active_logs.append((stat_result.st_mtime_ns, log_file))
                except FileNotFoundError:
                    continue
            if active_logs:
                active_log = max(active_logs)[1]
                tdLog.info(f"active taosd log after reset: {active_log}")
                return active_log
            time.sleep(1)
        tdLog.exit(f"cannot find active taosd log containing {marker}")

    def check_ext_window_log(self, stream_name, log_file, expected_ext_window):
        pattern = re.compile(rf"extWindow:{expected_ext_window}(?!\d)")
        for _ in range(self.wait_seconds):
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as log:
                    if pattern.search(log.read()):
                        return
            except FileNotFoundError:
                tdLog.info(f"taosd log file does not exist yet: {log_file}")
            time.sleep(1)
        tdLog.exit(f"stream {stream_name} did not emit extWindow:{expected_ext_window} in {log_file}")

    def prepare_schema(self):
        tdSql.execute(f"drop database if exists `{self.case_db}`")
        tdSql.execute(
            f"create database `{self.case_db}` vgroups 1 precision 'ms' duration 5d keep 3650d "
            "stt_trigger 1 wal_retention_period 3600"
        )
        tdSql.execute(f"use `{self.case_db}`")
        tdSql.execute(
            f"create table `{self.source_table}` ("
            "ts timestamp, status_value int, marker int, value int)"
        )

    def create_stream(
        self,
        stream_name,
        result_table,
        trigger_clause,
        start_expression="_twstart",
        end_expression="_twend",
        boundary_condition="_c0 >= _twstart and _c0 <= _twend",
        output_condition="row_count > 0",
    ):
        tdSql.execute(
            f"create stream `{self.case_db}`.`{stream_name}` {trigger_clause} "
            f"from `{self.case_db}`.`{self.source_table}` "
            f"into `{self.case_db}`.`{result_table}` as "
            "select start_time, end_time, row_count, value_sum from ("
            f"select {start_expression} as start_time, {end_expression} as end_time, "
            "count(*) as row_count, sum(value) as value_sum "
            f"from `{self.case_db}`.`{self.source_table}` "
            f"where {boundary_condition}"
            f") where {output_condition}"
        )
        tdStream.waitStreamStatus(self.case_db, stream_name, "Running")

    def check_result(self, result_table, expected_results):
        self.wait_result_rows(result_table, len(expected_results))
        tdSql.query(
            f"select start_time, end_time, row_count, value_sum "
            f"from `{self.case_db}`.`{result_table}` order by start_time"
        )
        tdSql.checkRows(len(expected_results))
        for row, expected_result in enumerate(expected_results):
            expected_start, expected_end, expected_count, expected_sum = expected_result
            if expected_start is not None:
                tdSql.checkData(row, 0, expected_start)
            if expected_end is not None:
                tdSql.checkData(row, 1, expected_end)
            tdSql.checkData(row, 2, expected_count)
            tdSql.checkData(row, 3, expected_sum)

    def run_scenario(
        self,
        stream_name,
        result_table,
        trigger_clause,
        insert_sql,
        expected_result,
        start_expression="_twstart",
        end_expression="_twend",
        boundary_condition="_c0 >= _twstart and _c0 <= _twend",
        output_condition="row_count > 0",
        expected_ext_window=1,
    ):
        tdStream.ensureSnode()
        self.prepare_schema()
        log_file = self.reset_taosd_log()
        self.create_stream(
            stream_name,
            result_table,
            trigger_clause,
            start_expression,
            end_expression,
            boundary_condition,
            output_condition,
        )
        tdSql.execute(insert_sql)
        self.check_result(result_table, expected_result)
        self.check_ext_window_log(stream_name, log_file, expected_ext_window)

    # --- impl ---
    def do_session_window_subquery(self):
        tdLog.info("verify session window derived query uses external window")
        self.run_scenario(
            stream_name="s_session",
            result_table="r_session",
            trigger_clause="session(ts, 2s)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "('2026-01-01 00:00:00', 1, 0, 10) "
                "('2026-01-01 00:00:01', 1, 0, 20) "
                "('2026-01-01 00:00:05', 1, 0, 30)"
            ),
            expected_result=[("2026-01-01 00:00:00", "2026-01-01 00:00:01", 2, 30)],
            expected_ext_window=1,
        )
        tdLog.info("session window derived external window ......................... [ passed ]")

    def do_state_window_subquery(self):
        tdLog.info("verify state window derived query uses external window")
        self.run_scenario(
            stream_name="s_state",
            result_table="r_state",
            trigger_clause="state_window(status_value)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "('2026-01-01 00:00:00', 1, 0, 10) "
                "('2026-01-01 00:00:01', 1, 0, 20) "
                "('2026-01-01 00:00:02', 2, 0, 30)"
            ),
            expected_result=[("2026-01-01 00:00:00", "2026-01-01 00:00:01", 2, 30)],
            expected_ext_window=1,
        )
        tdLog.info("state window derived external window ........................... [ passed ]")

    def do_event_window_subquery(self):
        tdLog.info("verify event window derived query uses external window")
        self.run_scenario(
            stream_name="s_event",
            result_table="r_event",
            trigger_clause="event_window(start with marker = 1 end with marker = 2)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "('2026-01-01 00:00:00', 1, 1, 10) "
                "('2026-01-01 00:00:01', 1, 0, 20) "
                "('2026-01-01 00:00:02', 1, 2, 30)"
            ),
            expected_result=[("2026-01-01 00:00:00", "2026-01-01 00:00:02", 3, 60)],
            expected_ext_window=1,
        )
        tdLog.info("event window derived external window ........................... [ passed ]")

    def do_interval_window_subquery(self):
        tdLog.info("verify interval window derived query uses external window")
        self.run_scenario(
            stream_name="s_interval",
            result_table="r_interval",
            trigger_clause="interval(2s) sliding(2s)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "('2026-01-01 00:00:00', 1, 0, 10) "
                "('2026-01-01 00:00:01', 1, 0, 20) "
                "('2026-01-01 00:00:03', 1, 0, 30)"
            ),
            expected_result=[("2026-01-01 00:00:00", "2026-01-01 00:00:02", 2, 30)],
        )
        tdLog.info("interval window derived external window ........................ [ passed ]")

    def do_sliding_window_subquery(self):
        tdLog.info("verify sliding window derived query uses external window")
        self.run_scenario(
            stream_name="s_sliding",
            result_table="r_sliding",
            trigger_clause="sliding(2s)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "('2026-01-01 00:00:00', 1, 0, 10) "
                "('2026-01-01 00:00:03', 1, 0, 20)"
            ),
            expected_result=[
                ("2026-01-01 00:00:02", "2026-01-01 00:00:04", 1, 20),
            ],
            start_expression="_tcurrent_ts",
            end_expression="_tnext_ts",
            boundary_condition="_c0 >= _tcurrent_ts and _c0 <= _tnext_ts",
            output_condition="value_sum >= 20",
        )
        tdLog.info("sliding window derived external window ......................... [ passed ]")

    def do_period_window_subquery(self):
        tdLog.info("verify period window derived query uses external window")
        self.run_scenario(
            stream_name="s_period",
            result_table="r_period",
            trigger_clause="period(2s)",
            insert_sql=(
                f"insert into `{self.source_table}` values "
                "(now, 1, 0, 10)"
            ),
            expected_result=[(None, None, 1, 10)],
            start_expression="cast(_tprev_localtime / 1000000 as timestamp)",
            end_expression="cast(_tlocaltime / 1000000 as timestamp)",
            boundary_condition=(
                "_c0 >= cast(_tprev_localtime / 1000000 as timestamp) "
                "and _c0 <= cast(_tlocaltime / 1000000 as timestamp)"
            ),
        )
        tdLog.info("period window derived external window .......................... [ passed ]")

    # --- main ---
    def test_session_window_subquery_external_window(self):
        """Verify session-window derived aggregates use external-window execution.

        1. Create a session stream whose calculation statement contains a derived table.
        2. Restrict calculation rows with _c0 >= _twstart and _c0 <= _twend.
        3. Verify the closed session result and enabled external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added session external-window regression.

        """
        self.do_session_window_subquery()

    def test_state_window_subquery_external_window(self):
        """Verify state-window derived aggregates use external-window execution.

        1. Create a state stream whose calculation statement contains a derived table.
        2. Restrict calculation rows with _c0 >= _twstart and _c0 <= _twend.
        3. Verify the closed state result and enabled external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added state external-window regression.

        """
        self.do_state_window_subquery()

    def test_event_window_subquery_external_window(self):
        """Verify event-window derived aggregates use external-window execution.

        1. Create an event stream whose calculation statement contains a derived table.
        2. Restrict calculation rows with _c0 >= _twstart and _c0 <= _twend.
        3. Verify the closed event result and enabled external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added event external-window regression.

        """
        self.do_event_window_subquery()

    def test_interval_window_subquery_external_window(self):
        """Verify interval-window derived calculations use external-window boundaries.

        1. Create an interval stream whose calculation statement contains a derived table.
        2. Restrict calculation rows with _c0 >= _twstart and _c0 <= _twend.
        3. Verify the closed interval result and external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added interval external-window regression.

        """
        self.do_interval_window_subquery()

    def test_sliding_window_subquery_external_window(self):
        """Verify sliding-window derived calculations use external-window boundaries.

        1. Create a sliding stream whose calculation statement contains a derived table.
        2. Restrict rows with sliding-current and sliding-next timestamps.
        3. Verify the sliding result and external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added sliding external-window regression.

        """
        self.do_sliding_window_subquery()

    def test_period_window_subquery_external_window(self):
        """Verify period-window derived calculations use external-window boundaries.

        1. Create a period stream whose calculation statement contains a derived table.
        2. Restrict rows with cast period-local start and end timestamps.
        3. Verify the period result and external-window execution log.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7104999936

        History:
            - 2026-08-31 Wang Mingming Added period external-window regression.

        """
        self.do_period_window_subquery()
