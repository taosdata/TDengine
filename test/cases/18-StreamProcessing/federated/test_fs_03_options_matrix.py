"""FS §4.5 STREAM_OPTIONS matrix for stream and federated query.

The control-side matrix verifies that every currently supported option is
accepted on an external trigger table and produces its documented observable
behavior after targeted data changes. Unsupported history options are covered
by the create-error suite.
"""

import sys
import time
from typing import List, Optional, Tuple

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    FS_BASE_MS,
    ms_to_dt,
    verify_sink_rows,
    FsSharedFixtureMixin,
)


class TestFsOptionsMatrix(FsSharedFixtureMixin, FederatedQueryTestMixin):
    """FS §4.5 — STREAM_OPTIONS x external source compatibility."""

    DB = "fs_opt"

    _BASE_MS = FS_BASE_MS

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

    # _insert_rows and the InfluxDB partition fixture come from the mixin.
    _ms_to_dt = staticmethod(ms_to_dt)

    def _verify(self, stream: str, sink: str, expected: list, label: str):
        verify_sink_rows(self.DB, stream, sink, expected, label)
        tdLog.info(
            f"stream result: stream={stream}, sink={sink}, rows={expected}"
        )

    def _run_option_case(
        self,
        src_name: str,
        case_id: str,
        options: Optional[str],
        exercise,
        trigger: str = "INTERVAL(1m) SLIDING(1m)",
    ):
        remote_db = "ucctl_mdb"
        stream = f"s_opt_{case_id}"
        sink = f"sink_opt_{case_id}"
        ExtSrcEnv.mysql_exec_cfg(
            self._mysql_cfg(), remote_db, ["DELETE FROM `src_t`"]
        )
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        option_clause = (
            f"STREAM_OPTIONS({options}) " if options is not None else ""
        )
        sql = (
            f"CREATE STREAM {stream} {trigger} "
            f"FROM {src_name}.{remote_db}.src_t "
            f"{option_clause}"
            f"INTO {self.DB}.{sink} AS "
            "SELECT cast(_twstart/1000 as timestamp) AS ts, "
            "COUNT(*) AS cnt, AVG(val) AS avg_val FROM %%trows"
        )
        option_label = options if options is not None else "NO_OPTIONS"
        tdLog.info(f"=== STREAM_OPTIONS case start: {option_label} ===")
        tdLog.info(f"create stream SQL: {sql}")
        try:
            tdSql.execute(sql)
            tdStream.checkStreamStatus(stream)
            exercise(stream, sink, remote_db)
            tdLog.info(
                f"STREAM_OPTIONS result: {option_label} is effective [passed]"
            )
        finally:
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def _sink_count(self, sink: str) -> int:
        try:
            tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.{sink}")
            return tdSql.getData(0, 0) or 0
        except Exception as err:
            if "not exist" in str(err).lower():
                return 0
            raise

    def _assert_sink_stays(self, sink: str, expected: int, seconds: float = 4):
        tdLog.debug(
            f"observe sink stability: sink={sink}, expected={expected}, "
            f"seconds={seconds}"
        )
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            actual = self._sink_count(sink)
            assert actual == expected, (
                f"sink {sink} changed unexpectedly: {actual} != {expected}"
            )
            time.sleep(0.5)
        tdLog.info(
            f"sink remained stable: sink={sink}, rows={expected}, "
            f"seconds={seconds}"
        )

    def _check_disorder_option(
        self,
        src_name: str,
        case_id: str,
        options: Optional[str],
        base_offset: int,
    ):
        base = self._BASE_MS + base_offset
        option_label = options or "NO_OPTIONS"
        old_data = [
            (base - 120_000, 100, 1.0, "old", 0),
            (base - 60_000, 101, 1.0, "old", 0),
        ]
        normal_data = [
            (base, 102, 1.0, "normal", 0),
            (base + 60_000, 103, 1.0, "normal", 0),
        ]
        fresh_data = [
            (base + 120_000, 104, 1.0, "fresh", 0),
            (base + 180_000, 105, 1.0, "fresh", 0),
        ]
        expected = [
            (self._ms_to_dt(base), 1, 102),
            (self._ms_to_dt(base + 60_000), 1, 103),
            (self._ms_to_dt(base + 120_000), 1, 104),
        ]

        def exercise(stream: str, sink: str, remote_db: str):
            self._insert_rows("m", remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                f"{option_label} result: normal data produced one sink row"
            )

            self._insert_rows("m", remote_db, old_data, src_name)
            self._insert_rows("m", remote_db, fresh_data, src_name)
            wait_stream_window_closed(stream, self.DB, sink, 3, timeout=20)
            self._verify(stream, sink, expected, src_name)
            tdLog.info(
                f"{option_label} result: disorder behavior verified, rows=3"
            )

        self._run_option_case(src_name, case_id, options, exercise)

    def _check_delete_option(self, src_name: str):
        base = self._BASE_MS + 1_800_000
        normal_data = [
            (base, 102, 1.0, "normal", 0),
            (base + 10_000, 103, 1.0, "normal", 0),
            (base + 60_000, 104, 1.0, "normal", 0),
        ]
        fresh_data = [
            (base + 120_000, 104, 1.0, "fresh", 0),
            (base + 180_000, 105, 1.0, "fresh", 0),
        ]
        expected = [
            (self._ms_to_dt(base), 2, 102.5),
            (self._ms_to_dt(base + 60_000), 1, 104),
            (self._ms_to_dt(base + 120_000), 1, 104),
        ]

        def exercise(stream: str, sink: str, remote_db: str):
            self._insert_rows("m", remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                "DELETE_RECALC result: initial data produced one closed window"
            )

            self._delete_rows("m", remote_db, base + 10_000, src_name)
            self._insert_rows("m", remote_db, fresh_data, src_name)
            wait_stream_window_closed(stream, self.DB, sink, 3, timeout=20)
            self._verify(stream, sink, expected, src_name)
            tdLog.info("DELETE_RECALC result: delete behavior verified, rows=3")

        self._run_option_case(src_name, "delrecalc", "DELETE_RECALC", exercise)

    def _check_drop_output_option(self, src_name: str):
        base = self._BASE_MS + 2_400_000
        normal_data = [
            (base, 102, 1.0, "normal", 0),
            (base + 60_000, 104, 1.0, "normal", 0),
        ]
        expected = [(self._ms_to_dt(base), 1, 102)]

        def exercise(stream: str, sink: str, remote_db: str):
            self._insert_rows("m", remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                "DELETE_OUTPUT_TABLE result: initial data produced one sink row"
            )

            self._drop_table("m", remote_db, src_name)
            time.sleep(5)
            wait_stream_window_closed(stream, self.DB, sink, 1, timeout=20)
            self._verify(stream, sink, expected, src_name)
            tdLog.info(
                "DELETE_OUTPUT_TABLE result: source drop behavior verified, rows=1"
            )

        self._run_option_case(
            src_name, "deleteouttable", "DELETE_OUTPUT_TABLE", exercise
        )

    def do_control_option_compatibility_matrix(self):
        def body(src_name: str):
            self._check_disorder_option(src_name, "no_options", None, 0)
            self._check_disorder_option(
                src_name, "expired", "EXPIRED_TIME(1d)", 600_000
            )
            self._check_disorder_option(
                src_name, "ignore_disorder", "IGNORE_DISORDER", 1_200_000
            )
            self._check_delete_option(src_name)
            self._check_drop_output_option(src_name)

        self._with_std_sources(
            "ucctl", body, skip_pg=True, skip_influx=True
        )
        print("control option compatibility matrix ........ [ passed ]")

    def test_control_option_compatibility_matrix(self):
        """Verify uncovered STREAM_OPTIONS on an external trigger table.

        1. Write targeted data for each supported external stream option
        2. Verify sink rows, trigger counts, ignored rows, or output suppression
        3. Verify external deletes do not recalculate or remove sink data

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-17 Wang Mingming Added control option coverage

        """
        self.do_control_option_compatibility_matrix()

    def _run_count_window_case(
        self,
        prefix: str,
        options: Optional[str],
        batches: List[Tuple[list, list]],
    ):
        option_label = options or "NO_OPTIONS"

        def body(src_name: str):
            remote_db = f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            option_clause = (
                f"STREAM_OPTIONS({options}) " if options is not None else ""
            )
            sql = (
                f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                f"FROM {src_name}.{remote_db}.src_t "
                f"{option_clause}"
                f"INTO {self.DB}.{sink} AS "
                "SELECT cast(_twstart/1000 as timestamp) AS ts, "
                "COUNT(*) AS cnt, AVG(val) AS avg_val FROM %%trows"
            )
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdLog.info(
                f"=== COUNT_WINDOW option case start: {option_label} ==="
            )
            tdLog.info(f"create stream SQL: {sql}")
            try:
                tdSql.execute(sql)
                tdStream.checkStreamStatus(stream)
                for batch_index, (rows, expected) in enumerate(batches, start=1):
                    self._insert_rows("m", remote_db, rows, src_name)
                    self._verify(stream, sink, expected, src_name)
                    tdLog.info(
                        f"{option_label} batch verified: "
                        f"batch={batch_index}, sink_rows={len(expected)}"
                    )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_pg=True, skip_influx=True)

    def _run_watermark_case(
        self, options: Optional[str], expected_batches: list
    ):
        batch1 = [
            (self._BASE_MS, 6, 6.5, "zeta", 0),
            (self._BASE_MS + 60_000, 8, 8.5, "theta", 0),
        ]
        batch2 = [
            (self._BASE_MS + 10_000, 7, 7.5, "eta", 1),
            (self._BASE_MS + 120_000, 9, 9.5, "hey", 0),
        ]
        self._run_count_window_case(
            "uc001",
            options,
            [(batch1, expected_batches[0]), (batch2, expected_batches[1])],
        )
        print(f"watermark case {options or 'NO_OPTIONS'} ........ [ passed ]")

    def do_opt_001_watermark(self):
        expected = [
            [(self._ms_to_dt(self._BASE_MS), 1, 6.0)],
            [
                (self._ms_to_dt(self._BASE_MS), 1, 6.0),
                (self._ms_to_dt(self._BASE_MS + 10_000), 1, 7.0),
                (self._ms_to_dt(self._BASE_MS + 60_000), 1, 8.0),
            ],
        ]
        self._run_watermark_case("WATERMARK(1m)", expected)

    def do_opt_001_small_watermark(self):
        expected = [
            [(self._ms_to_dt(self._BASE_MS), 1, 6.0)],
            [
                (self._ms_to_dt(self._BASE_MS), 1, 6.0),
                (self._ms_to_dt(self._BASE_MS + 60_000), 1, 8.0),
            ],
        ]
        self._run_watermark_case("WATERMARK(10s)", expected)

    def do_opt_001_no_watermark(self):
        expected = [
            [
                (self._ms_to_dt(self._BASE_MS), 1, 6.0),
                (self._ms_to_dt(self._BASE_MS + 60_000), 1, 8.0),
            ],
            [
                (self._ms_to_dt(self._BASE_MS), 1, 6.0),
                (self._ms_to_dt(self._BASE_MS + 60_000), 1, 8.0),
                (self._ms_to_dt(self._BASE_MS + 120_000), 1, 9.0),
            ],
        ]
        self._run_watermark_case(None, expected)

    def test_opt_001_watermark(self):
        """Verify a one-minute WATERMARK on an external trigger table.

        1. Create a count-window stream with WATERMARK(1m)
        2. Verify ordered and disordered input batches

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-18 Wang Mingming Refactored option coverage

        """
        self.do_opt_001_watermark()

    def test_opt_001_small_watermark(self):
        """Verify a ten-second WATERMARK on an external trigger table.

        1. Create a count-window stream with WATERMARK(10s)
        2. Verify rows outside the accepted disorder range are ignored

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-18 Wang Mingming Refactored option coverage

        """
        self.do_opt_001_small_watermark()

    def test_opt_001_no_watermark(self):
        """Verify count-window behavior without a WATERMARK option.

        1. Create an external count-window stream without WATERMARK
        2. Verify the same ordered and disordered input batches

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-18 Wang Mingming Refactored option coverage

        """
        self.do_opt_001_no_watermark()

    def do_pre_filter_column(self):
        rows = [
            (self._BASE_MS, 6, 6.5, "zeta", 0),
            (self._BASE_MS + 60_000, 8, 8.5, "theta", 0),
            (self._BASE_MS + 10_000, 7, 7.5, "eta", 1),
            (self._BASE_MS + 120_000, 9, 9.5, "hey", 0),
        ]
        expected = [
            (self._ms_to_dt(self._BASE_MS + 60_000), 1, 8.0),
            (self._ms_to_dt(self._BASE_MS + 120_000), 1, 9.0),
        ]
        self._run_count_window_case(
            "uc001", "PRE_FILTER(val>7)", [(rows, expected)]
        )
        print("column pre-filter ........................... [ passed ]")

    def test_opt_005_pre_filter_column(self):
        """Verify PRE_FILTER pushdown using an external value column.

        1. Create a stream with PRE_FILTER(val>7)
        2. Verify only matching rows reach the destination table

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-18 Wang Mingming Refactored option coverage

        """
        self.do_pre_filter_column()

    def do_pre_filter_tag(self):
        expected = [
            (self._ms_to_dt(self._BASE_MS), 2, 2),
            (self._ms_to_dt(self._BASE_MS + 60_000), 2, 2),
            (self._ms_to_dt(self._BASE_MS + 120_000), 2, 2),
            (self._ms_to_dt(self._BASE_MS + 180_000), 2, 2),
        ]
        src = "uc006sp_tbname"
        i_db = "uc006sp_tdb"
        stream = "s_uc006tsp"
        sink_stb = "sink_t_uc006sp_stb"
        sql = (
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src}.{i_db}.src_t PARTITION BY host "
            f"STREAM_OPTIONS(PRE_FILTER(host='a')) "
            f"INTO {self.DB}.{sink_stb} AS "
            "SELECT cast(_twstart/1000000 as timestamp) AS ts, "
            "COUNT(*) AS cnt, AVG(val) AS avg_val FROM %%trows"
        )

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdLog.info("=== PRE_FILTER tag case start: host='a' ===")
            tdLog.info(f"create stream SQL: {sql}")
            tdSql.execute(sql)
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)
            wait_stream_window_closed(
                stream, self.DB, sink_stb, expected_rows=4, timeout=120
            )
            self._verify(stream, sink_stb, expected, src)
            tdLog.info("PRE_FILTER tag result verified: host=a, sink_rows=4")
        finally:
            try:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            finally:
                self._teardown_partition_influx(src, i_db)
        print("tag pre-filter .............................. [ passed ]")

    def test_opt_006_pre_filter_tag(self):
        """Verify PRE_FILTER pushdown using an external partition tag.

        1. Create a partitioned stream with PRE_FILTER(host='a')
        2. Verify only matching InfluxDB series reach the destination table

        Catalog:
            - Streams:FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-18 Wang Mingming Refactored option coverage

        """
        self.do_pre_filter_tag()
