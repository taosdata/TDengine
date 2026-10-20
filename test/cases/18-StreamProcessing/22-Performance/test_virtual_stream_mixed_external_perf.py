from new_test_framework.utils import tdLog, tdSql, etool

import datetime
import os
import sys
import threading
import time

import psutil

from new_test_framework.utils import tdStream


_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
_EXT_HELPER_DIR = os.path.abspath(
    os.path.join(
        _CASE_DIR,
        "..",
        "..",
        "05-VirtualTables",
        "01-DDL",
        "03-ExtSource",
    )
)
_FQ_DIR = os.path.abspath(
    os.path.join(
        _CASE_DIR,
        "..",
        "..",
        "09-DataQuerying",
        "19-FederatedQuery",
    )
)
_FS_DIR = os.path.abspath(os.path.join(_CASE_DIR, "..", "99-Others"))

for helper_dir in (_EXT_HELPER_DIR, _FQ_DIR, _FS_DIR):
    if helper_dir not in sys.path:
        sys.path.insert(0, helper_dir)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_mysql_table,
    create_remote_db,
)
from fs_common import ensure_snode  # noqa: E402


class TaosdResourceSampler:
    def __init__(self, interval_sec=1.0):
        self.interval_sec = interval_sec
        self.stop_event = threading.Event()
        self.samples = []
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(self.interval_sec * 2, 1.0))

    def summary(self):
        if not self.samples:
            return {
                "rss_peak_mb": 0.0,
                "cpu_peak_pct": 0.0,
                "sample_count": 0,
            }

        return {
            "rss_peak_mb": max(item["rss_mb"] for item in self.samples),
            "cpu_peak_pct": max(item["cpu_pct"] for item in self.samples),
            "sample_count": len(self.samples),
        }

    def _run(self):
        processes = self._taosd_processes()
        for process in processes:
            try:
                process.cpu_percent(None)
            except psutil.Error:
                pass

        while not self.stop_event.wait(self.interval_sec):
            rss_bytes = 0
            cpu_pct = 0.0
            alive = []
            for process in processes:
                try:
                    rss_bytes += process.memory_info().rss
                    cpu_pct += process.cpu_percent(None)
                    alive.append(process)
                except psutil.Error:
                    continue

            if not alive:
                processes = self._taosd_processes()
                continue

            processes = alive
            self.samples.append(
                {
                    "rss_mb": rss_bytes / 1024 / 1024,
                    "cpu_pct": cpu_pct,
                }
            )

    @staticmethod
    def _taosd_processes():
        processes = []
        for process in psutil.process_iter(["name", "cmdline"]):
            try:
                name = process.info.get("name") or ""
                cmdline = " ".join(process.info.get("cmdline") or [])
                if name == "taosd" or "/taosd" in cmdline:
                    processes.append(process)
            except psutil.Error:
                continue
        return processes


class TestVirtualStreamMixedExternalPerf:
    DB = "vs_mixed_ext_perf"
    MYSQL_DB = "vs_mixed_ext_perf_mysql"
    MYSQL_SOURCE = "vs_mixed_ext_perf_mysql_src"
    STREAM = "mixed_ext_perf_stream"
    VSTB = "mixed_ext_perf_vstb"
    VTABLE = "mixed_ext_perf_vt"
    SINK = "mixed_ext_perf_sink"
    LOCAL_TABLE = "perf_local"
    REMOTE_TABLE = "perf_remote"
    BASE_MS = 1767225600000
    STEP_MS = int(os.environ.get("VSTREAM_EXT_PERF_STEP_MS", "10"))
    DEFAULT_ROWS = int(os.environ.get("VSTREAM_EXT_PERF_ROWS", "5000"))
    DEFAULT_BATCH = int(os.environ.get("VSTREAM_EXT_PERF_BATCH", "1000"))
    DEFAULT_COUNT_WINDOW = int(os.environ.get("VSTREAM_EXT_PERF_COUNT_WINDOW", "2500"))
    DEFAULT_TIMEOUT_SEC = int(os.environ.get("VSTREAM_EXT_PERF_TIMEOUT_SEC", "180"))
    LARGE_ROWS = int(os.environ.get("VSTREAM_EXT_PERF_LARGE_ROWS", "100000"))
    LARGE_BATCH = int(os.environ.get("VSTREAM_EXT_PERF_LARGE_BATCH", "5000"))
    LARGE_COUNT_WINDOW = int(
        os.environ.get("VSTREAM_EXT_PERF_LARGE_COUNT_WINDOW", "20000")
    )
    LARGE_TIMEOUT_SEC = int(os.environ.get("VSTREAM_EXT_PERF_LARGE_TIMEOUT_SEC", "900"))
    SAMPLE_INTERVAL_SEC = float(os.environ.get("VSTREAM_EXT_PERF_SAMPLE_SEC", "1"))

    @classmethod
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()

    @classmethod
    def teardown_class(cls):
        try:
            cls._cleanup_fixture()
        finally:
            cls.env.teardown_env()

    @classmethod
    def _cleanup_fixture(cls):
        tdLog.info("cleaning mixed external performance fixture")
        try:
            tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.{cls.STREAM}", queryTimes=30)
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB} FORCE")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.MYSQL_SOURCE}")
        finally:
            try:
                ExtSrcEnv.mysql_drop_db(cls.MYSQL_DB)
            except Exception as error:
                tdLog.info(
                    f"remote database cleanup skipped: "
                    f"database={cls.MYSQL_DB}, error={error}"
                )

    @classmethod
    def _prepare_fixture(cls, rows, batch, count_window):
        cls._validate_scale(rows, batch, count_window)
        cls._cleanup_fixture()
        tdLog.info(
            f"creating mixed external performance fixture: "
            f"rows={rows}, batch={batch}, count_window={count_window}"
        )

        create_remote_db("mysql", cls.MYSQL_DB)
        create_mysql_table(
            cls.MYSQL_DB,
            cls.REMOTE_TABLE,
            "ts DATETIME(3) NOT NULL PRIMARY KEY, ext_v BIGINT",
        )
        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(
            f"CREATE TABLE {cls.DB}.{cls.LOCAL_TABLE} "
            "(ts TIMESTAMP, local_v BIGINT)"
        )
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.VSTB} "
            "(ts TIMESTAMP, local_v BIGINT, ext_v BIGINT) "
            "TAGS (site INT) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.VTABLE} ("
            f"local_v FROM {cls.DB}.{cls.LOCAL_TABLE}.local_v, "
            f"ext_v FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.{cls.REMOTE_TABLE}.ext_v) "
            f"USING {cls.DB}.{cls.VSTB} TAGS(1)"
        )

    @classmethod
    def _create_stream(cls, count_window):
        tdLog.info("creating mixed external throughput stream")
        tdSql.execute(
            f"CREATE STREAM {cls.DB}.{cls.STREAM} "
            f"COUNT_WINDOW({count_window}) "
            f"FROM {cls.DB}.{cls.VTABLE} "
            "STREAM_OPTIONS(IGNORE_DISORDER) "
            f"INTO {cls.DB}.{cls.SINK} "
            "AS SELECT first(_c0) AS ts, count(*) AS row_count, "
            "sum(local_v) AS local_sum, sum(ext_v) AS ext_sum FROM %%trows"
        )
        tdStream.checkStreamStatus(cls.STREAM)

    @classmethod
    def _insert_remote_rows(cls, rows, batch):
        tdLog.info(
            f"inserting remote MySQL rows for mixed external throughput: "
            f"rows={rows}, batch={batch}"
        )
        for start in range(0, rows, batch):
            end = min(start + batch, rows)
            values = []
            for index in range(start, end):
                values.append(
                    f"('{cls._timestamp(index)}', {cls._ext_value(index)})"
                )
            ExtSrcEnv.mysql_exec(
                cls.MYSQL_DB,
                [f"INSERT INTO {cls.REMOTE_TABLE} VALUES {', '.join(values)}"],
            )

    @classmethod
    def _insert_local_rows(cls, rows, batch):
        tdLog.info(
            f"inserting local TDengine rows for mixed external throughput: "
            f"rows={rows}, batch={batch}"
        )
        for start in range(0, rows, batch):
            end = min(start + batch, rows)
            values = []
            for index in range(start, end):
                values.append(
                    f"('{cls._timestamp(index)}', {cls._local_value(index)})"
                )
            tdSql.execute(
                f"INSERT INTO {cls.DB}.{cls.LOCAL_TABLE} VALUES {', '.join(values)}"
            )

    @classmethod
    def _wait_sink_totals(cls, rows, count_window, timeout_sec):
        expected_windows = rows // count_window
        expected_local = cls._sum_local_values(rows)
        expected_ext = cls._sum_ext_values(rows)
        sql = (
            "SELECT count(*), cast(sum(row_count) AS BIGINT), "
            "cast(sum(local_sum) AS BIGINT), cast(sum(ext_sum) AS BIGINT) "
            f"FROM {cls.DB}.{cls.SINK}"
        )
        deadline = time.time() + timeout_sec
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                if result:
                    last = tuple(result[0])
                    if last == (expected_windows, rows, expected_local, expected_ext):
                        return {
                            "sink_rows": last[0],
                            "total_rows": last[1],
                            "total_local": last[2],
                            "total_ext": last[3],
                        }
            except Exception as error:
                tdLog.info(f"mixed external throughput sink not ready: error={error}")
            time.sleep(0.5)

        cls._log_failure_snapshot(rows, count_window)
        raise TimeoutError(
            f"mixed external throughput sink did not match: "
            f"got={last}, expected={(expected_windows, rows, expected_local, expected_ext)}"
        )

    @classmethod
    def _log_failure_snapshot(cls, rows, count_window):
        tdLog.info(f"mixed external failure snapshot: rows={rows}, window={count_window}")
        queries = [
            "SELECT status, message FROM information_schema.ins_streams "
            f"WHERE db_name='{cls.DB}' AND stream_name='{cls.STREAM}'",
            f"SELECT count(*), min(ts), max(ts), sum(local_v) FROM {cls.DB}.{cls.LOCAL_TABLE}",
            f"SELECT count(*), min(ts), max(ts), count(local_v), count(ext_v), "
            f"sum(local_v), sum(ext_v) FROM {cls.DB}.{cls.VTABLE}",
            f"SELECT ts, row_count, local_sum, ext_sum FROM {cls.DB}.{cls.SINK} ORDER BY ts LIMIT 200",
            f"SELECT ts, local_v, ext_v FROM {cls.DB}.{cls.VTABLE} "
            "WHERE local_v IS NULL OR ext_v IS NULL ORDER BY ts LIMIT 20",
        ]
        for sql in queries:
            try:
                tdLog.info(f"mixed external snapshot: sql={sql}, result={tdSql.getResult(sql)}")
            except Exception as error:
                tdLog.info(f"mixed external snapshot unavailable: sql={sql}, error={error}")
        try:
            result = ExtSrcEnv.mysql_query(
                cls.MYSQL_DB,
                f"SELECT count(*), min(ts), max(ts), sum(ext_v) FROM {cls.REMOTE_TABLE}",
            )
            tdLog.info(f"mixed external MySQL snapshot: result={result}")
        except Exception as error:
            tdLog.info(f"mixed external MySQL snapshot unavailable: error={error}")

    @classmethod
    def _timestamp(cls, index):
        value_ms = cls.BASE_MS + index * cls.STEP_MS
        return datetime.datetime.fromtimestamp(
            value_ms / 1000, datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @staticmethod
    def _local_value(index):
        return index

    @staticmethod
    def _ext_value(index):
        return index * 10

    @classmethod
    def _sum_local_values(cls, rows):
        return rows * (rows - 1) // 2

    @classmethod
    def _sum_ext_values(cls, rows):
        return cls._sum_local_values(rows) * 10

    @staticmethod
    def _validate_scale(rows, batch, count_window):
        if rows <= 0:
            raise ValueError("VSTREAM_EXT_PERF_ROWS must be greater than 0")
        if batch <= 0:
            raise ValueError("VSTREAM_EXT_PERF_BATCH must be greater than 0")
        if count_window <= 0:
            raise ValueError("VSTREAM_EXT_PERF_COUNT_WINDOW must be greater than 0")
        if rows % count_window != 0:
            raise ValueError(
                "VSTREAM_EXT_PERF_ROWS must be a multiple of "
                "VSTREAM_EXT_PERF_COUNT_WINDOW"
            )
        # A count window closes on local rows and is never recomputed, so the remote rows must
        # already be visible to the external reader when the window closes. Require at least two
        # insert batches per window: the first batch gives the reader a fetch round while the
        # window is still open.
        if batch >= count_window:
            raise ValueError(
                "VSTREAM_EXT_PERF_BATCH must be smaller than "
                "VSTREAM_EXT_PERF_COUNT_WINDOW so a window never closes "
                "inside a single insert batch"
            )

    def _run_throughput_scenario(
        self,
        scenario,
        rows,
        batch,
        count_window,
        timeout_sec,
        sample_interval_sec,
    ):
        self._prepare_fixture(rows, batch, count_window)
        sampler = TaosdResourceSampler(sample_interval_sec)
        try:
            self._create_stream(count_window)
            self._insert_remote_rows(rows, batch)

            sampler.start()
            start = time.monotonic()
            self._insert_local_rows(rows, batch)
            totals = self._wait_sink_totals(rows, count_window, timeout_sec)
            elapsed = time.monotonic() - start
        finally:
            sampler.stop()

        resource = sampler.summary()
        throughput = rows / elapsed if elapsed > 0 else 0.0
        tdLog.info("=== Virtual Stream Mixed External Throughput ===")
        tdLog.info(f"scenario                : {scenario}")
        tdLog.info(f"input_rows              : {rows}")
        tdLog.info(f"batch_size              : {batch}")
        tdLog.info(f"count_window            : {count_window}")
        tdLog.info(f"actual_sink_rows        : {totals['sink_rows']}")
        tdLog.info(f"emit_elapsed_ms         : {elapsed * 1000:.2f}")
        tdLog.info(f"throughput_rows_per_sec : {throughput:.2f}")
        tdLog.info(f"taosd_rss_peak_mb       : {resource['rss_peak_mb']:.2f}")
        tdLog.info(f"taosd_cpu_peak_pct      : {resource['cpu_peak_pct']:.2f}")
        tdLog.info(f"resource_samples        : {resource['sample_count']}")
        tdLog.info("result_correct          : true")

    def test_virtual_stream_mixed_tdengine_mysql_throughput(self):
        """Stream: mixed TDengine and MySQL virtual table throughput

        1. Create one TDengine table and one MySQL external table.
        2. Create one virtual table that references both sources.
        3. Create a COUNT_WINDOW stream and measure rows-per-second throughput
           from local trigger write start to verified sink output.
        4. Report taosd RSS and CPU peak values during the throughput window.

        Catalog:
            - Streams:Performance

        Since: v3.4.0.0

        Labels: performance

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add mixed external throughput benchmark
        """
        self._run_throughput_scenario(
            "tdengine_mysql_count_window",
            self.DEFAULT_ROWS,
            self.DEFAULT_BATCH,
            self.DEFAULT_COUNT_WINDOW,
            self.DEFAULT_TIMEOUT_SEC,
            self.SAMPLE_INTERVAL_SEC,
        )
        tdLog.success(f"{__file__} successfully executed")

    def test_virtual_stream_mixed_tdengine_mysql_large_scale_throughput(self):
        """Stream: large-scale mixed TDengine and MySQL throughput

        1. Create one TDengine table and one MySQL external table.
        2. Write a large input set to the external table before stream trigger.
        3. Write a large local trigger set and wait for all COUNT_WINDOW
           aggregation results to reach the sink table.
        4. Report rows-per-second throughput, taosd RSS peak and CPU peak.

        Catalog:
            - Streams:Performance

        Since: v3.4.0.0

        Labels: performance

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add large-scale mixed external benchmark
        """
        self._run_throughput_scenario(
            "tdengine_mysql_count_window_large_scale",
            self.LARGE_ROWS,
            self.LARGE_BATCH,
            self.LARGE_COUNT_WINDOW,
            self.LARGE_TIMEOUT_SEC,
            self.SAMPLE_INTERVAL_SEC,
        )
        tdLog.success(f"{__file__} successfully executed")
