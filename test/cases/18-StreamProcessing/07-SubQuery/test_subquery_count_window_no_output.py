import os
import time

from new_test_framework.utils import tdLog, tdSql, tdStream
from new_test_framework.utils.srvCtl import sc


class TestSubqueryCountWindowNoOutput:
    case_db = "poc"
    stable = "st_vehicle_telemetry"
    child_table = "vt_0"
    result_table = "alarm_rl_filter"
    stream_name = "s_rl"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.wait_seconds = 30

    def wait_result_rows(self, expected_rows):
        actual_rows = 0
        for _ in range(self.wait_seconds):
            tdSql.query(f"select count(*) from `{self.case_db}`.`{self.result_table}`")
            actual_rows = tdSql.getData(0, 0)
            if actual_rows == expected_rows:
                return
            time.sleep(1)
        tdLog.exit(
            f"stream {self.case_db}.{self.stream_name} result row count mismatch, "
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

    def check_ext_window_log(self, log_file):
        pattern = "extWindow:1"
        for _ in range(self.wait_seconds):
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as log:
                    if pattern in log.read():
                        return
            except FileNotFoundError:
                tdLog.info(f"taosd log file does not exist yet: {log_file}")
            time.sleep(1)
        tdLog.exit(f"taosd log {log_file} does not contain {pattern}")

    def stream_sql(self):
        return f"""
            create stream `{self.case_db}`.`{self.stream_name}` count_window(1)
            from `{self.case_db}`.`{self.stable}` partition by tbname
            into `{self.case_db}`.`{self.result_table}`
            as
            select `ts`, `vin`, `cnt`, f_ip from (
                select _twstart as `ts`, `vin`, count(*) as `cnt`, first(`IPS_DCDCOutVoltAct`) as f_ip
                from %%tbname
                where _c0 >= _twstart and _c0 <= _twend
            ) where f_ip >= 5
        """

    # --- impl ---
    def create_snode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def prepare_schema(self):
        tdLog.info("prepare count_window subquery schema")
        tdSql.execute(f"drop database if exists `{self.case_db}`")
        tdSql.execute(
            f"create database `{self.case_db}` vgroups 1 precision 'ms' duration 5d keep 30d "
            "stt_trigger 1 wal_retention_period 3600"
        )
        tdSql.execute(f"use `{self.case_db}`")
        tdSql.execute(
            f"create table `{self.stable}` ("
            "ts timestamp, "
            "`IPS_DCDCOutVoltAct` int, "
            "`VVM_VehModeSt` int, "
            "`UINM_PLGSWSt` int, "
            "`PLG_LatchFaultSt` int, "
            "`PLG_BasicLatchFaultSt` int, "
            "`PLG_LeftAPSSt` int, "
            "`PLG_RightAPSSt` int"
            ") tags (vin binary(32))"
        )
        tdSql.execute(f"create table `{self.child_table}` using `{self.stable}` tags('VIN00000000000000000')")

    def write_trigger_data(self):
        tdLog.info("write count_window trigger data")
        tdSql.execute(
            f"insert into `{self.child_table}` values "
            "(now - 2s, 12, 0, 2, 1, 1, 1, 1) "
            "(now - 1s, 12, 0, 2, 1, 1, 1, 1)"
        )

    def create_stream(self):
        tdLog.info("create count_window subquery stream")
        tdSql.execute(self.stream_sql())
        tdStream.waitStreamStatus(self.case_db, self.stream_name, "Running")

    def check_result(self, log_file):
        tdLog.info("check count_window subquery stream result")
        self.wait_result_rows(2)
        tdSql.query(f"select `cnt`, f_ip from `{self.case_db}`.`{self.result_table}` order by `ts`")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 12)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 12)
        self.check_ext_window_log(log_file)

        print("count_window subquery no output regression ......................... [ passed ]")

    def do_count_window_subquery_no_output(self):
        self.create_snode()
        self.prepare_schema()
        log_file = self.reset_taosd_log()
        self.create_stream()
        self.write_trigger_data()
        self.check_result(log_file)

    # --- main ---
    def test_subquery_count_window_no_output(self):
        """Verify count_window subquery emits rows with boundary predicates.

        1. Create one super table and one child table with two trigger rows.
        2. Create a count_window(1) stream whose calculation query uses a derived table.
        3. Verify the stream emits one result row for each trigger row.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-28 Wang Mingming Added count_window no-output regression.

        """
        self.do_count_window_subquery_no_output()
