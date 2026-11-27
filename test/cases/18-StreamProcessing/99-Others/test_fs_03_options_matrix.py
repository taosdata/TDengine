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

sys.path.insert(0, "cases/18-StreamProcessing/99-Others")
from fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    wait_influx18_reader_bootstrap,
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

    def _verify(self, stream: str, sink: str, expected: list, label: str,
                cols: str = "ts, cnt, avg_val"):
        verify_sink_rows(self.DB, stream, sink, expected, label, cols)
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
        # Per-source remote database (ucctl_mdb for MySQL, ucctl_sdb for SQL
        # Server — _with_std_sources provisions both).
        remote_db = f"ucctl_{src_name[-1]}db"
        # SQL Server 3-segment refs carry the schema in the middle segment
        # (database comes from the source default, DS §5.3.3).
        ref_mid = "dbo" if src_name.endswith("_s") else remote_db
        stream = f"s_opt_{case_id}"
        sink = f"sink_opt_{case_id}"
        if src_name.endswith("_s"):
            ExtSrcEnv.sqlserver_exec(remote_db, ["DELETE FROM dbo.src_t"])
        else:
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
            f"FROM {src_name}.{ref_mid}.src_t "
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
            self._insert_rows(src_name[-1], remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                f"{option_label} result: normal data produced one sink row"
            )

            self._insert_rows(src_name[-1], remote_db, old_data, src_name)
            self._insert_rows(src_name[-1], remote_db, fresh_data, src_name)
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
            self._insert_rows(src_name[-1], remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                "DELETE_RECALC result: initial data produced one closed window"
            )

            self._delete_rows(src_name[-1], remote_db, base + 10_000, src_name)
            self._insert_rows(src_name[-1], remote_db, fresh_data, src_name)
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
            self._insert_rows(src_name[-1], remote_db, normal_data, src_name)
            self._assert_sink_stays(sink, 1)
            tdLog.info(
                "DELETE_OUTPUT_TABLE result: initial data produced one sink row"
            )

            self._drop_table(src_name[-1], remote_db, src_name)
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

        # The control-option matrix still provisions no Oracle leg: its body
        # derives a per-source remote database name and middle segment, neither
        # of which exists for Oracle (one namespace level, service name in the
        # source default).  Streams accept Oracle external tables now, so this
        # skip is a pending gap rather than a product limitation.
        self._with_std_sources(
            "ucctl", body, skip_pg=True, skip_influx=True, skip_sqlserver=True,
            skip_oracle=True
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
            # SQL Server 3-segment refs carry the schema in the middle segment
            # (database comes from the source default, DS §5.3.3).
            ref_mid = "dbo" if src_name.endswith("_s") else remote_db
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            option_clause = (
                f"STREAM_OPTIONS({options}) " if options is not None else ""
            )
            sql = (
                f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                f"FROM {src_name}.{ref_mid}.src_t "
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
                    self._insert_rows(src_name[-1], remote_db, rows, src_name)
                    self._verify(stream, sink, expected, src_name)
                    tdLog.info(
                        f"{option_label} batch verified: "
                        f"batch={batch_index}, sink_rows={len(expected)}"
                    )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        # Same pending gap as the control-option matrix above: the body builds
        # a per-source remote database name and middle segment, which Oracle
        # does not have (one namespace level, table in the login schema).
        self._with_std_sources(prefix, body, skip_pg=True, skip_influx=True,
                               skip_sqlserver=True, skip_oracle=True)

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

    def test_influx18_trigger_prefilter_does_not_filter_explicit_calc_source(self):
        """PRE_FILTER filters trigger rows, not an explicit external calc table.

        1. Create an InfluxDB trigger measurement and a separate calc measurement
        2. Trigger one interval with a row satisfying PRE_FILTER(val > 7)
        3. Verify the calc query aggregates matching and non-matching calc rows

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-04 Wang Mingming Added review regression for trigger/calc filter separation
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("explicit calc PRE_FILTER regression applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_pf_calc_db"
        stream = "s_influx18_pf_calc"
        sink = "sink_influx18_pf_calc"
        base_ns = self._BASE_MS * 1_000_000
        seed = [
            f"trigger_m,host=a val=0i {base_ns - 60_000_000_000}",
            f"calc_m,host=a val=0i {base_ns - 60_000_000_000}",
        ]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                    f"FROM {source}.{remote_db}.trigger_m "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                    "COUNT(*) AS cnt, SUM(val) AS total "
                    f"FROM {source}.{remote_db}.calc_m "
                    "WHERE time >= _twstart AND time < _twend",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"trigger_m,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [
                        f"trigger_m,host=a val=8i {base_ns}",
                        f"calc_m,host=a val=8i {base_ns}",
                        f"calc_m,host=a val=6i {base_ns + 1_000_000}",
                        f"trigger_m,host=a val=8i {base_ns + 60_000_000_000}",
                    ],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 2, 14)],
                    "influx18 trigger PRE_FILTER vs explicit calc source",
                    "ts, cnt, total",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_pre_filter_catches_existing_series_later(self):
        """A field PRE_FILTER must not freeze the initial InfluxDB series list.

        1. Create two series whose initial values do not match PRE_FILTER
        2. Write a matching value to the second series after the stream is ready
        3. Verify the new matching row triggers and produces output

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-04 Wang Mingming Added review regression for dynamic series discovery
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("dynamic series PRE_FILTER regression applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_pf_dynamic_db"
        stream = "s_influx18_pf_dynamic"
        sink = "sink_influx18_pf_dynamic"
        base_ns = self._BASE_MS * 1_000_000
        seed = [
            f"metrics,host=a val=0i {base_ns - 60_000_000_000}",
            f"metrics,host=b val=0i {base_ns - 60_000_000_000}",
        ]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=b val=20i {base_ns}"],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 field PRE_FILTER discovers an existing series later",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_event_window_pure_count_star_keeps_rows(self):
        """A pure COUNT(*) calculation must keep every row of a trigger-only source.

        1. Create an InfluxDB 1.8 EVENT_WINDOW stream whose trigger reads val
        2. Count the trigger rows with COUNT(*) FROM %%trows, which selects no value column
        3. Verify both closed windows report two rows instead of being dropped

        The calculation projection is the timestamp only, so the virtual table owns a
        trigger binding but no calculation value column.  Excluding such a target from
        the calculation fanout (the fix for COUNT(*) overcounting a second source) would
        also drop the rows of the very source that opened the window.

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-11 Wang Mingming Added pure COUNT(*) calculation regression
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("pure COUNT(*) calculation regression applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_pf_cntstar_db"
        stream = "s_influx18_pf_cntstar"
        sink = "sink_influx18_pf_cntstar"
        base_ns = self._BASE_MS * 1_000_000
        second_ns = base_ns + 60_000_000_000
        seed = [f"metrics,host=a val=0i {base_ns - 120_000_000_000}"]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} "
                    "EVENT_WINDOW(START WITH val > 0 END WITH val < 0) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [
                        f"metrics,host=a val=5i {base_ns}",
                        f"metrics,host=a val=-5i {base_ns + 1_000_000}",
                        f"metrics,host=a val=7i {second_ns}",
                        f"metrics,host=a val=-7i {second_ns + 1_000_000}",
                    ],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=2, timeout=180)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 2), (self._ms_to_dt(self._BASE_MS + 60_000), 2)],
                    "influx18 EVENT_WINDOW with a pure COUNT(*) calculation",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_pre_filter_ignores_non_matching_updates(self):
        """Non-matching rows must not open windows or advance trigger work.

        1. Create an InfluxDB 1.8 stream with PRE_FILTER(val > 7)
        2. Write a newer non-matching row and verify no sink row is emitted
        3. Write a matching row and verify exactly one window is emitted

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-05 Wang Mingming Added PRE_FILTER watermark regression
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("PRE_FILTER watermark regression applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_pf_watermark_db"
        stream = "s_influx18_pf_watermark"
        sink = "sink_influx18_pf_watermark"
        base_ns = self._BASE_MS * 1_000_000
        seed = [f"metrics,host=a val=0i {base_ns - 60_000_000_000}"]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=a val=0i {base_ns}"],
                )
                self._assert_sink_stays(sink, 0, seconds=3)

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=a val=20i {base_ns + 60_000_000_000}"],
                )
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS + 60_000), 1)],
                    "influx18 PRE_FILTER emits after a matching update",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_discovers_new_series_after_stream_start(self):
        """A new InfluxDB series is registered after the initial enumeration.

        1. Create a stream with one existing series
        2. Write the first matching point for a previously unseen tag tuple
        3. Verify the new series triggers and produces output

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-05 Wang Mingming Added dynamic new-series coverage
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("dynamic new-series discovery applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_new_series_db"
        stream = "s_influx18_new_series"
        sink = "sink_influx18_new_series"
        base_ns = self._BASE_MS * 1_000_000
        seed = [f"metrics,host=a val=0i {base_ns - 60_000_000_000}"]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=b val=20i {base_ns}"],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 new series after stream start",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_detects_new_tag_key_after_stream_start(self):
        """A new InfluxDB tag key rebuilds the cached series identity state.

        1. Create a stream while the measurement has only the host tag
        2. Write a new series that introduces a region tag
        3. Verify the refreshed series identity triggers and produces output

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-05 Wang Mingming Added tag-schema change coverage
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("tag-schema refresh applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_tag_schema_db"
        stream = "s_influx18_tag_schema"
        sink = "sink_influx18_tag_schema"
        base_ns = self._BASE_MS * 1_000_000
        # Keep the historical value matching PRE_FILTER so a schema refresh
        # cannot silently replay it under a new UID.
        seed = [f"metrics,host=a val=20i {base_ns - 60_000_000_000}"]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=b,region=x val=21i {base_ns}"],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 tag schema change after stream start",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_first_tag_key_keeps_untagged_series_identity(self):
        """Adding the first tag key must not re-identify the untagged series.

        1. Write one untagged point that matches PRE_FILTER before the stream
           (pre-stream history, kept out by the LAST_TS cutoff)
        2. Create the stream while the measurement still has no tag keys
        3. Write a series that introduces the first tag key
        4. Verify only the new point is counted: the untagged series keeps its
           uid, so its history is not replayed under a new identity

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-06 Wang Mingming Added zero-tag to first-tag identity coverage
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("tag-schema refresh applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_zero_tag_db"
        stream = "s_influx18_zero_tag"
        sink = "sink_influx18_zero_tag"
        base_ns = self._BASE_MS * 1_000_000
        # Untagged history that matches PRE_FILTER: replaying it under a new uid
        # would add a second window at base-60s, which is what this case rejects.
        seed = [f"metrics val=20i {base_ns - 60_000_000_000}"]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=b val=21i {base_ns}"],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 first tag key keeps the untagged series uid",
                    "ts, cnt",
                )
                # A replayed history point would land after the first window closed.
                self._assert_sink_stays(sink, 1, seconds=8)
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_series_snapshot_handles_many_series(self):
        """The chunked 1.8 series snapshot enumerates a high-cardinality measurement.

        1. Create 130 series before creating the stream
        2. Write a matching row to the last series
        3. Verify the existing series is registered and produces output

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-05 Wang Mingming Added multi-page series enumeration coverage
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("multi-page series enumeration applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_series_pages_db"
        stream = "s_influx18_series_pages"
        sink = "sink_influx18_series_pages"
        base_ns = self._BASE_MS * 1_000_000
        seed = [
            f"metrics,host=s{i:03d} val=0i {base_ns - 60_000_000_000}"
            for i in range(130)
        ]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} COUNT_WINDOW(1) "
                    f"FROM {source}.{remote_db}.metrics PARTITION BY tbname "
                    "STREAM_OPTIONS(PRE_FILTER(val > 7)) "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=s000 val=0i {ts_ns}"]
                    ),
                )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [f"metrics,host=s129 val=20i {base_ns}"],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=180)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 multi-page series enumeration",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    def test_influx18_series_identity_excludes_other_series(self):
        """A stream over a SERIES vtable must read only the bound tag tuple.

        1. Create an InfluxDB measurement with host a and host b
        2. Bind a virtual normal table to host a via SERIES
        3. Trigger an interval and require its count to exclude host b

        Catalog:
            - Streams:FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-04 Wang Mingming Added review regression for SERIES identity
        """
        if self._influx_cfg().version != "1.8":
            tdLog.info("SERIES identity regression applies only to InfluxDB 1.8")
            return

        remote_db = "fs_opt_series_identity_db"
        vtable = "vt_influx18_series_identity"
        stream = "s_influx18_series_identity"
        sink = "sink_influx18_series_identity"
        vdb = "fs_opt_series_identity_vdb"
        base_ns = self._BASE_MS * 1_000_000
        seed = [
            f"metrics,host=a val=0i {base_ns - 60_000_000_000}",
            f"metrics,host=b val=0i {base_ns - 60_000_000_000}",
        ]
        with self._influx_fixture(remote_db, seed) as (cfg, source):
            try:
                tdSql.execute(f"USE {self.DB}")
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(f"DROP DATABASE IF EXISTS {vdb}")
                # InfluxDB 1.8 timestamps are nanosecond-precision; the SERIES
                # vtable must live in an ns-precision DB so its ts column can
                # hold the referenced external ts without truncation.
                tdSql.execute(f"CREATE DATABASE {vdb} PRECISION 'ns'")
                tdSql.execute(
                    f"CREATE VTABLE {vdb}.{vtable} ("
                    "  ts TIMESTAMP,"
                    "  val BIGINT FROM s.val"
                    f") SERIES s AS {source}.{remote_db}.metrics (host='a')"
                )
                tdSql.execute(
                    f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                    f"FROM {vdb}.{vtable} "
                    f"INTO {self.DB}.{sink} AS "
                    "SELECT cast(_twstart/1000000 as timestamp) AS ts, COUNT(*) AS cnt "
                    "FROM %%trows",
                    queryTimes=1,
                )
                tdStream.checkStreamStatus(stream)
                # checkStreamStatus only proves START was queued; the reader
                # samples its history cutoff afterwards and a batch written
                # before that pull becomes history no META pull asks for again.
                wait_influx18_reader_bootstrap(
                    stream,
                    self._BASE_MS,
                    lambda ts_ns: ExtSrcEnv.influx_write_cfg(
                        cfg, remote_db, [f"metrics,host=a val=0i {ts_ns}"]
                    ),
                )

                # SERIES vtable: the external reader is deployed lazily after
                # the trigger reaches Running (identity resolution plus reader
                # deploy over extra heartbeat rounds).  checkStreamStatus only
                # waits for the Running trigger, so writing data right away can
                # race the delayed LAST_TS: the watermark then already includes
                # the post-stream batch and the window never closes.  Wait for
                # the second reader task (vtable reader + external reader)
                # before writing.
                deadline = time.time() + 30
                while time.time() < deadline:
                    tdSql.query(
                        f"select * from information_schema.ins_stream_tasks "
                        f"where stream_name = '{stream}' and type = 'Reader'"
                    )
                    if tdSql.getRows() >= 2:
                        break
                    time.sleep(0.5)
                else:
                    raise TimeoutError(
                        f"external reader not ready for stream {stream}"
                    )

                ExtSrcEnv.influx_write_cfg(
                    cfg,
                    remote_db,
                    [
                        f"metrics,host=a val=1i {base_ns}",
                        f"metrics,host=b val=1i {base_ns + 1_000_000}",
                        f"metrics,host=a val=1i {base_ns + 60_000_000_000}",
                    ],
                )
                wait_stream_window_closed(stream, self.DB, sink, expected_rows=1, timeout=120)
                self._verify(
                    stream,
                    sink,
                    [(self._ms_to_dt(self._BASE_MS), 1)],
                    "influx18 SERIES identity excludes host b",
                    "ts, cnt",
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(f"DROP DATABASE IF EXISTS {vdb}")
