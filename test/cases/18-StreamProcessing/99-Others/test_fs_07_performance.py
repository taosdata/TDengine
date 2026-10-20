"""FS performance smoke cases for stream and federated query."""

import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
)

sys.path.insert(0, "cases/18-StreamProcessing/99-Others")
from fs_common import (  # noqa: E402
    FS_BASE_MS,
    dt_str,
    ensure_snode,
    wait_stream_window_closed,
)


class TestFsPerformance(FederatedQueryTestMixin):
    """FS performance smoke coverage for external trigger streams."""

    DB = "fs_perf"
    BASE_MS = FS_BASE_MS + 86_400_000
    WINDOW_ROWS = int(os.environ.get("FS_STREAM_FQ_PERF_WINDOW_ROWS", "1000"))
    BATCH_ROWS = int(os.environ.get("FS_STREAM_FQ_PERF_BATCH_ROWS", "1000"))
    SMALL_ROWS = int(os.environ.get("FS_STREAM_FQ_PERF_SMALL_ROWS", "5000"))
    MEDIUM_ROWS = int(os.environ.get("FS_STREAM_FQ_PERF_MEDIUM_ROWS", "20000"))
    LARGE_ROWS = int(os.environ.get("FS_STREAM_FQ_PERF_LARGE_ROWS", "100000"))
    SMALL_TIMEOUT = int(os.environ.get("FS_STREAM_FQ_PERF_SMALL_TIMEOUT", "180"))
    MEDIUM_TIMEOUT = int(os.environ.get("FS_STREAM_FQ_PERF_MEDIUM_TIMEOUT", "360"))
    LARGE_TIMEOUT = int(os.environ.get("FS_STREAM_FQ_PERF_LARGE_TIMEOUT", "900"))

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} PRECISION 'ms'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        finally:
            cls.env.teardown_env()

    def _prepare_remote_source(self, source: str, remote_db: str) -> None:
        self._cleanup_src(source)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), remote_db)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), remote_db, [
            "DROP TABLE IF EXISTS `src_t`",
            "CREATE TABLE `src_t` ("
            "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
            "  name VARCHAR(32), flag TINYINT(1))",
        ])
        self._mk_mysql_real(source, database=remote_db)

    def _cleanup_remote_source(self, source: str, remote_db: str) -> None:
        self._cleanup_src(source)
        try:
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), remote_db)
        except Exception as err:
            tdLog.info(
                f"remote MySQL cleanup skipped: db={remote_db}, error={err}"
            )

    def _create_count_stream(self, stream: str, sink: str, source: str, remote_db: str) -> None:
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        sql = (
            f"CREATE STREAM {stream} COUNT_WINDOW({self.WINDOW_ROWS}) "
            f"FROM {source}.{remote_db}.src_t "
            f"INTO {self.DB}.{sink} AS "
            "SELECT cast(first(ts)/1000 as timestamp) AS ts, "
            "COUNT(*) AS cnt, AVG(val) AS avg_val FROM %%trows"
        )
        tdLog.info(f"create performance stream SQL: {sql}")
        tdSql.execute(sql)
        tdStream.checkStreamStatus(stream)

    def _insert_remote_rows(self, remote_db: str, rows: int, base_ms: int) -> float:
        start_time = time.monotonic()
        for start in range(0, rows, self.BATCH_ROWS):
            end = min(start + self.BATCH_ROWS, rows)
            values = []
            for index in range(start, end):
                ts_ms = base_ms + index
                values.append(
                    f"('{dt_str(ts_ms)}', {index % 1000}, "
                    f"{float(index % 1000):.1f}, 'perf_{index % 10}', {index % 2})"
                )
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(),
                remote_db,
                [f"INSERT INTO `src_t` VALUES {', '.join(values)}"],
            )
        return time.monotonic() - start_time

    def _verify_sink_totals(self, stream: str, sink: str, rows: int, timeout: int) -> dict:
        expected_windows = rows // self.WINDOW_ROWS
        wait_stream_window_closed(
            stream,
            self.DB,
            sink,
            expected_rows=expected_windows,
            timeout=timeout,
            poll=1.0,
        )
        tdSql.query(
            f"SELECT COUNT(*), SUM(cnt), MIN(cnt), MAX(cnt) "
            f"FROM {self.DB}.{sink}"
        )
        sink_rows = tdSql.getData(0, 0) or 0
        cnt_sum = tdSql.getData(0, 1) or 0
        min_cnt = tdSql.getData(0, 2) or 0
        max_cnt = tdSql.getData(0, 3) or 0
        tdSql.checkEqual(sink_rows, expected_windows)
        tdSql.checkEqual(cnt_sum, rows)
        tdSql.checkEqual(min_cnt, self.WINDOW_ROWS)
        tdSql.checkEqual(max_cnt, self.WINDOW_ROWS)
        return {
            "sink_rows": sink_rows,
            "cnt_sum": cnt_sum,
            "min_cnt": min_cnt,
            "max_cnt": max_cnt,
        }

    def _validate_scale(self, rows: int) -> None:
        if rows <= 0:
            raise ValueError("FS_STREAM_FQ_PERF_*_ROWS must be greater than 0")
        if self.WINDOW_ROWS <= 0:
            raise ValueError("FS_STREAM_FQ_PERF_WINDOW_ROWS must be greater than 0")
        if self.BATCH_ROWS <= 0:
            raise ValueError("FS_STREAM_FQ_PERF_BATCH_ROWS must be greater than 0")
        if rows % self.WINDOW_ROWS != 0:
            raise ValueError(
                "FS_STREAM_FQ_PERF_*_ROWS must be a multiple of "
                "FS_STREAM_FQ_PERF_WINDOW_ROWS"
            )

    def do_count_window_performance(self, scale: str, rows: int, timeout: int) -> None:
        self._validate_scale(rows)
        source = f"fs_perf_{scale}_m"
        remote_db = f"fs_perf_{scale}_mdb"
        stream = f"s_perf_{scale}"
        sink = f"sink_perf_{scale}"
        base_ms = self.BASE_MS + {
            "small": 0,
            "medium": 7_200_000,
            "large": 14_400_000,
        }[scale]

        try:
            # Prepare inside the protected region: a half-initialized remote database/source after
            # a failure must still be released by the finally block below (_cleanup_remote_source
            # tolerates partial state).
            self._prepare_remote_source(source, remote_db)
            self._create_count_stream(stream, sink, source, remote_db)
            insert_elapsed = self._insert_remote_rows(remote_db, rows, base_ms)
            emit_start = time.monotonic()
            totals = self._verify_sink_totals(stream, sink, rows, timeout)
            emit_elapsed = time.monotonic() - emit_start
            total_elapsed = insert_elapsed + emit_elapsed
            rows_per_sec = rows / total_elapsed if total_elapsed > 0 else 0.0
            emit_rows_per_sec = rows / emit_elapsed if emit_elapsed > 0 else 0.0
            tdLog.info("=== Stream Federated Query Performance ===")
            tdLog.info(f"scale                   : {scale}")
            tdLog.info(f"input_rows              : {rows}")
            tdLog.info(f"window_rows             : {self.WINDOW_ROWS}")
            tdLog.info(f"batch_rows              : {self.BATCH_ROWS}")
            tdLog.info(f"sink_windows            : {totals['sink_rows']}")
            tdLog.info(f"sink_cnt_sum            : {totals['cnt_sum']}")
            tdLog.info(f"remote_insert_elapsed_ms: {insert_elapsed * 1000:.2f}")
            tdLog.info(f"emit_elapsed_ms         : {emit_elapsed * 1000:.2f}")
            tdLog.info(f"total_elapsed_ms        : {total_elapsed * 1000:.2f}")
            tdLog.info(f"total_rows_per_sec      : {rows_per_sec:.2f}")
            tdLog.info(f"emit_rows_per_sec       : {emit_rows_per_sec:.2f}")
            tdLog.info(f"fs performance {scale} ................. [ passed ]")
        finally:
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            self._cleanup_remote_source(source, remote_db)

    def test_count_window_external_trigger_small_perf(self):
        """Stream federated query count-window performance, small scale

        1. Create a MySQL external source as the stream trigger source.
        2. Create a COUNT_WINDOW stream that reads rows through %%trows.
        3. Insert the small-scale remote input set after the stream is running.
        4. Verify all count-window results and log throughput metrics.

        Catalog:
            - Streams:Performance

        Since: v3.4.2.0

        Labels: performance

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-19 Wang Mingming Add stream federated performance scale

        """
        self.do_count_window_performance("small", self.SMALL_ROWS, self.SMALL_TIMEOUT)
        tdLog.success(f"{__file__} successfully executed")

    def test_count_window_external_trigger_medium_perf(self):
        """Stream federated query count-window performance, medium scale

        1. Create a MySQL external source as the stream trigger source.
        2. Create a COUNT_WINDOW stream that reads rows through %%trows.
        3. Insert the medium-scale remote input set after the stream is running.
        4. Verify all count-window results and log throughput metrics.

        Catalog:
            - Streams:Performance

        Since: v3.4.2.0

        Labels: performance

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-19 Wang Mingming Add stream federated performance scale

        """
        self.do_count_window_performance("medium", self.MEDIUM_ROWS, self.MEDIUM_TIMEOUT)
        tdLog.success(f"{__file__} successfully executed")

    def test_count_window_external_trigger_large_perf(self):
        """Stream federated query count-window performance, large scale

        1. Create a MySQL external source as the stream trigger source.
        2. Create a COUNT_WINDOW stream that reads rows through %%trows.
        3. Insert the large-scale remote input set after the stream is running.
        4. Verify all count-window results and log throughput metrics.

        Catalog:
            - Streams:Performance

        Since: v3.4.2.0

        Labels: performance

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-19 Wang Mingming Add stream federated performance scale

        """
        self.do_count_window_performance("large", self.LARGE_ROWS, self.LARGE_TIMEOUT)
        tdLog.success(f"{__file__} successfully executed")
