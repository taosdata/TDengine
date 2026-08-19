import json
import os
import statistics
import subprocess
import time

import psutil

from new_test_framework.utils import tdLog, tdSql, tdStream, etool


class TestSubqueryExternalWindow:
    case_db = "poc"
    stable = "st_vehicle_telemetry"
    table_prefix = "vt_"
    result_with_filter = "alarm_rl_filter"
    result_no_filter = "alarm_rl_no_filter"
    stream_with_filter = "s_rl"
    stream_no_filter = "s_rl_no_filter"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.table_count = int(os.getenv("SUBQUERY_EXTWIN_TABLES", "4000"))
        cls.measure_seconds = int(os.getenv("SUBQUERY_EXTWIN_MEASURE_SECONDS", "60"))
        cls.warmup_seconds = int(os.getenv("SUBQUERY_EXTWIN_WARMUP_SECONDS", "30"))
        cls.insert_rows = int(
            os.getenv("SUBQUERY_EXTWIN_INSERT_ROWS", str(max(cls.measure_seconds + cls.warmup_seconds + 30, 60)))
        )
        cls.cpu_ratio_limit = float(os.getenv("SUBQUERY_EXTWIN_CPU_RATIO_LIMIT", "3.0"))
        cls.cpu_abs_tolerance = float(os.getenv("SUBQUERY_EXTWIN_CPU_ABS_TOLERANCE", "20.0"))
        cls.benchmark_thread_count = int(os.getenv("SUBQUERY_EXTWIN_BENCH_THREADS", "8"))
        cls.create_table_threads = int(os.getenv("SUBQUERY_EXTWIN_CREATE_THREADS", "8"))
        cls.work_dir = os.getenv("SUBQUERY_EXTWIN_WORK_DIR", "/tmp")

    # --- util ---
    def benchmark_json_path(self, suffix):
        return os.path.join(self.work_dir, f"test_subquery_external_window_{suffix}.json")

    def tags_csv_path(self):
        return os.path.join(self.work_dir, "test_subquery_external_window_tags.csv")

    def benchmark_log_path(self, suffix):
        return os.path.join(self.work_dir, f"test_subquery_external_window_{suffix}.log")

    def write_tags_file(self):
        path = self.tags_csv_path()
        with open(path, "w", encoding="utf-8") as fp:
            for idx in range(self.table_count):
                fp.write(f"'VIN{idx:017d}'\n")
        return path

    def write_benchmark_json(self, suffix, drop_db, insert_rows, child_table_exists):
        tags_file = self.write_tags_file()
        config = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": self.benchmark_thread_count,
            "create_table_thread_count": self.create_table_threads,
            "result_file": self.benchmark_log_path(suffix),
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 4000,
            "prepared_rand": 10000,
            "chinese": "no",
            "escape_character": "yes",
            "databases": [
                {
                    "dbinfo": {
                        "name": self.case_db,
                        "drop": "yes" if drop_db else "no",
                        "vgroups": 4,
                        "precision": "ms",
                        "duration": "5d",
                        "keep": "30d",
                        "stt_trigger": 1,
                        "wal_retention_period": 3600,
                    },
                    "super_tables": [
                        {
                            "name": self.stable,
                            "child_table_exists": "yes" if child_table_exists else "no",
                            "childtable_count": self.table_count,
                            "childtable_prefix": self.table_prefix,
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "non_stop_mode": "no",
                            "insert_rows": insert_rows,
                            "interlace_rows": 1,
                            "insert_interval": 1000,
                            "start_timestamp": "now",
                            "start_fillback_time": "auto",
                            "timestamp_step": 1000,
                            "tags_file": tags_file,
                            "columns": [
                                {"type": "INT", "name": "`IPS_DCDCOutVoltAct`", "min": 12, "max": 12},
                                {"type": "INT", "name": "`VVM_VehModeSt`", "min": 0, "max": 0},
                                {"type": "INT", "name": "`UINM_PLGSWSt`", "min": 2, "max": 2},
                                {"type": "INT", "name": "`PLG_LatchFaultSt`", "min": 1, "max": 1},
                                {"type": "INT", "name": "`PLG_BasicLatchFaultSt`", "min": 1, "max": 1},
                                {"type": "INT", "name": "`PLG_LeftAPSSt`", "min": 1, "max": 1},
                                {"type": "INT", "name": "`PLG_RightAPSSt`", "min": 1, "max": 1},
                            ],
                            "tags": [
                                {"type": "BINARY", "name": "vin", "len": 32},
                            ],
                        }
                    ],
                }
            ],
        }
        path = self.benchmark_json_path(suffix)
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(config, fp, indent=2)
        return path

    def run_benchmark_once(self, suffix, drop_db, insert_rows, child_table_exists):
        json_path = self.write_benchmark_json(suffix, drop_db, insert_rows, child_table_exists)
        cmd = [etool.benchMarkFile(), "-f", json_path]
        tdLog.info(f"run taosBenchmark: {' '.join(cmd)}")
        with open(self.benchmark_log_path(suffix), "w", encoding="utf-8") as fp:
            proc = subprocess.run(cmd, stdout=fp, stderr=subprocess.STDOUT, text=True, check=False)
        if proc.returncode != 0:
            tdLog.exit(f"taosBenchmark failed, suffix={suffix}, rc={proc.returncode}")

    def start_benchmark_async(self, suffix):
        json_path = self.write_benchmark_json(suffix, False, self.insert_rows, True)
        cmd = [etool.benchMarkFile(), "-f", json_path]
        tdLog.info(f"start taosBenchmark: {' '.join(cmd)}")
        fp = open(self.benchmark_log_path(suffix), "w", encoding="utf-8")
        proc = subprocess.Popen(cmd, stdout=fp, stderr=subprocess.STDOUT, text=True)
        return proc, fp

    def stop_benchmark(self, proc, fp):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        fp.close()

    def find_taosd_process(self):
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.info["name"] == "taosd":
                return psutil.Process(proc.info["pid"])
        tdLog.exit("taosd process not found")

    def wait_stream_status(self, stream, status, wait_seconds=60):
        actual = None
        for _ in range(wait_seconds):
            tdSql.query(
                f"select status from information_schema.ins_streams "
                f"where db_name='{self.case_db}' and stream_name='{stream}'"
            )
            if tdSql.getRows() == 1:
                actual = tdSql.getData(0, 0)
                if actual == status:
                    return
            time.sleep(1)
        tdLog.exit(f"stream {self.case_db}.{stream} expect status {status}, actual {actual}")

    def stop_stream(self, stream):
        tdSql.execute(f"stop stream {self.case_db}.{stream}")
        self.wait_stream_status(stream, "Stopped")

    def start_stream(self, stream):
        tdSql.execute(f"start stream {self.case_db}.{stream}")
        self.wait_stream_status(stream, "Running")

    def create_streams(self):
        tdSql.execute(self.stream_sql_with_filter())
        tdSql.execute(self.stream_sql_no_filter())
        self.stop_stream(self.stream_with_filter)
        self.stop_stream(self.stream_no_filter)

    def stream_sql_with_filter(self):
        return f"""
            create stream `{self.case_db}`.`{self.stream_with_filter}` interval(10s) sliding(1s)
            from `{self.case_db}`.`{self.stable}` partition by tbname stream_options(ignore_nodata_trigger)
            into `{self.case_db}`.`{self.result_with_filter}` output_subtable('alarm_rl') (`ts`, `vin` composite key, cnt)
            as
            select `ts`, `vin`, `cnt` from (
                select _twstart as `ts`, `vin`, count(*) as `cnt`
                from %%tbname
                where _c0 >= _twstart and _c0 < _twend
                  and (`IPS_DCDCOutVoltAct` >= 11 and `VVM_VehModeSt` = 0)
                  and (`UINM_PLGSWSt` = 2 or (`PLG_LatchFaultSt` in (1,2,3)
                    or (`PLG_BasicLatchFaultSt` = 1 or (`PLG_LeftAPSSt` in (1,2,3)
                    or `PLG_RightAPSSt` in (1,2,3)))))
            ) where `cnt` >= 5
        """

    def stream_sql_no_filter(self):
        return f"""
            create stream `{self.case_db}`.`{self.stream_no_filter}` interval(10s) sliding(1s)
            from `{self.case_db}`.`{self.stable}` partition by tbname stream_options(ignore_nodata_trigger)
            into `{self.case_db}`.`{self.result_no_filter}` output_subtable('alarm_rl') (`ts`, `vin` composite key, cnt)
            as
            select _twstart as `ts`, `vin`, count(*) as `cnt`
            from %%tbname
            where _c0 >= _twstart and _c0 < _twend
              and (`IPS_DCDCOutVoltAct` >= 11 and `VVM_VehModeSt` = 0)
              and (`UINM_PLGSWSt` = 2 or (`PLG_LatchFaultSt` in (1,2,3)
                or (`PLG_BasicLatchFaultSt` = 1 or (`PLG_LeftAPSSt` in (1,2,3)
                or `PLG_RightAPSSt` in (1,2,3)))))
        """

    def prepare_stream_case(self, suffix):
        self.run_benchmark_once(f"{suffix}_prepare", True, 0, False)
        tdSql.query(f"select count(*) from information_schema.ins_tables where db_name='{self.case_db}'")
        tdSql.checkData(0, 0, self.table_count)
        self.create_streams()

    def sample_stream_cpu(self, stream, suffix):
        self.start_stream(stream)
        bench_proc, bench_fp = self.start_benchmark_async(suffix)
        taosd = self.find_taosd_process()
        samples = []
        try:
            taosd.cpu_percent(interval=None)
            for second in range(self.warmup_seconds + self.measure_seconds):
                cpu = taosd.cpu_percent(interval=1)
                if second >= self.warmup_seconds:
                    samples.append(cpu)
                tdLog.info(f"stream={stream} cpu_sample={cpu:.2f}")
        finally:
            self.stop_benchmark(bench_proc, bench_fp)
            self.stop_stream(stream)

        if not samples:
            tdLog.exit(f"no CPU samples collected for stream {stream}")
        avg_cpu = statistics.mean(samples)
        max_cpu = max(samples)
        tdLog.info(f"stream={stream} avg_cpu={avg_cpu:.2f} max_cpu={max_cpu:.2f} samples={len(samples)}")
        return avg_cpu

    # --- impl ---
    def do_compare_external_window_cpu(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

        self.prepare_stream_case("with_filter")
        cpu_with_filter = self.sample_stream_cpu(self.stream_with_filter, "with_filter_write")

        self.prepare_stream_case("no_filter")
        cpu_no_filter = self.sample_stream_cpu(self.stream_no_filter, "no_filter_write")

        allowed_cpu = max(cpu_no_filter * self.cpu_ratio_limit, cpu_no_filter + self.cpu_abs_tolerance)
        tdLog.info(
            f"cpu_with_filter={cpu_with_filter:.2f}, cpu_no_filter={cpu_no_filter:.2f}, "
            f"allowed_cpu={allowed_cpu:.2f}"
        )
        if cpu_with_filter > allowed_cpu:
            tdLog.exit(
                f"external window CPU regression: filtered stream avg {cpu_with_filter:.2f} "
                f"is higher than allowed {allowed_cpu:.2f}, no-filter avg {cpu_no_filter:.2f}"
            )

        print("compare external window stream cpu ......................... [ passed ]")

    # --- main ---
    def test_subquery_external_window(self):
        """Compare stream CPU for derived-table filter and direct aggregate forms.

        1. Create one super table and child tables with taosBenchmark.
        2. Create the two stream SQL shapes from the external-window scenario.
        3. Run one stream at a time with START/STOP STREAM and collect taosd CPU.
        4. Verify the derived-table filter form does not consume much more CPU.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.2.0

        Labels: common,manual,performance

        Jira: None

        History:
            - 2026-08-11 Wang Mingming Added external-window CPU comparison.

        """
        self.do_compare_external_window_cpu()
