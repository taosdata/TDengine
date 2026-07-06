"""FS §9 — End-to-end use cases for stream + federated query.

Real CREATE STREAM grammar (from removed reference tests + FS §9):
    CREATE STREAM <name> INTERVAL(...) SLIDING(...)
      FROM   <ext_src>.<remote_db_or_schema>.<table>   -- ext-driven
        OR   <local_db>.<local_table>                  -- local-driven
      [PARTITION BY tbname|tag]
      INTO   <local_db>.<sink>
      AS SELECT _twstart AS ts, ... FROM %%trows [JOIN <ext>.<...>.<tbl>] ...;

Three-segment external reference uses the *remote* db / schema created by
``_with_std_sources`` (``{prefix}_{m,p,i}db``); PG middle segment is the
schema (``public``).

Cases:
  FS-UC-001  Periodic summary: ext-driven INTERVAL stream over external metric.
  FS-UC-002  Local fact joined with external dim table (PG-only, FS §9.2).
  FS-UC-003  Single stream referencing two distinct EXTERNAL SOURCEs.
  FS-UC-004  InfluxDB measurement -> TSDB subtable split (FS §9.3).
"""

import datetime
import sys

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
    get_stream_ext_meta,
)


def _mid_segment(prefix: str, src_name: str) -> str:
    """For src '{prefix}_{m|p|i}', return remote db (mysql/influx) or schema (pg)."""
    suffix = src_name[-1]
    if suffix == "p":
        return "public"
    return f"{prefix}_{suffix}db"


class TestFsUseCases(FederatedQueryTestMixin):
    """FS §9 — Use cases (FS-UC-001..004)."""

    DB = "fs_uc"

    # ------------------------------------------------------------------
    # Shared test data for FS-UC-001
    # Fixed base timestamp: 2025-03-03 00:00:00 UTC (epoch ms).
    # Each row is in its own 1-minute window (INTERVAL 1m SLIDING 1m).
    # batch1: windows 08:00/08:01/08:02   batch2: windows 08:03/08:04
    # ------------------------------------------------------------------
    _BASE_MS = 1740960000000  # 2025-03-03 00:00:00.000 UTC

    # (ts_ms, val, score, name, flag)
    _BATCH1 = [
        (_BASE_MS,          6, 6.5, 'zeta',  0),
        (_BASE_MS + 60_000, 7, 7.5, 'eta',   1),
        (_BASE_MS + 120_000, 8, 8.5, 'theta', 0),
    ]
    _BATCH1_MORE = [
        (_BASE_MS,          6, 6.5, 'zeta',  0),
        (_BASE_MS + 60_000, 7, 7.5, 'eta',   1),
        (_BASE_MS + 80_000, 8, 7.6, 'hello',   1),
        (_BASE_MS + 120_000, 9, 8.5, 'theta', 0),
        (_BASE_MS + 140_000, 10, 8.6, 'too', 0),
    ]
    _BATCH2 = [
        (_BASE_MS + 180_000, 9,  9.5,  'iota',  0),
        (_BASE_MS + 240_000, 10, 10.5, 'kappa', 1),
    ]

    @classmethod
    def setup_class(cls):
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} PRECISION 'ms'")
        tdSql.execute(f"alter dnode 1 'debugflag 135'")
        tdSql.execute(f"alter dnode 1 'qdebugflag 143'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        finally:
            cls.env.teardown_env()

    # ------------------------------------------------------------------
    # Shared helpers for FS-UC-001
    # ------------------------------------------------------------------

    @staticmethod
    def _ms_to_dt(ms: int) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ms / 1000.0)

    @staticmethod
    def _dt_str(ms: int) -> str:
        """Convert epoch ms to 'YYYY-MM-DD HH:MM:SS.mmm' in local time."""
        dt = datetime.datetime.fromtimestamp(ms / 1000.0)
        return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms % 1000:03d}"

    def _insert_rows(self, suffix: str, db_or_bucket: str, rows: list, label: str):
        """Write rows into the external source for the given suffix type."""
        if suffix == 'm':
            rows_sql = ", ".join(
                f"('{self._dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), db_or_bucket,
                [f"INSERT INTO `src_t` VALUES {rows_sql}"]
            )
            tdLog.info(f"[{label}] inserted {len(rows)} MySQL rows")
        elif suffix == 'p':
            rows_sql = ", ".join(
                f"('{self._dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.pg_exec_cfg(
                self._pg_cfg(), db_or_bucket,
                [f"INSERT INTO public.src_t VALUES {rows_sql}"]
            )
            tdLog.info(f"[{label}] inserted {len(rows)} PG rows")
        elif suffix == 'i':
            lines = [
                f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
                f'{ts * 1_000_000}'
                for ts, val, score, name, flag in rows
            ]
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] wrote {len(rows)} InfluxDB points")

    def _verify(self, stream: str, sink: str, expected: list, label: str):
        wait_stream_window_closed(stream, self.DB, sink,
                                  expected_rows=len(expected), timeout=120)
        res = tdSql.getResult(
            f"SELECT ts, cnt, avg_val FROM {self.DB}.{sink} ORDER BY ts"
        )
        tdSql.checkEqual(res, expected)
        tdLog.info(f"[{label}] verified {len(expected)} sink rows OK")

    def _create_uc001_stream(self, stream: str, sink: str,
                             src_name: str, mid: str):
        """Create the FS-UC-001 INTERVAL stream and wait until Running.

        _twstart precision differs by source type:
          MySQL / PG  : microseconds  -> divide by 1_000 to get ms timestamp
          InfluxDB    : nanoseconds   -> divide by 1_000_000 to get ms timestamp
        """
        # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
        twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        tdSql.execute(
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src_name}.{mid}.src_t "
            f"INTO {self.DB}.{sink} AS "
            f"SELECT cast(_twstart/{twstart_divisor} as timestamp) AS ts, "
            f"COUNT(*) AS cnt, AVG(val) AS avg_val "
            f"FROM %%trows"
        )
        tdLog.info(f"[{src_name}] stream created; waiting for Running status...")
        tdStream.checkStreamStatus(stream)
    
    def _create_uc001_stream_no_trows(self, stream: str, sink: str,
                             src_name: str, mid: str):
        """Create the FS-UC-001 INTERVAL stream and wait until Running.

        _twstart precision differs by source type:
          MySQL / PG  : microseconds  -> divide by 1_000 to get ms timestamp
          InfluxDB    : nanoseconds   -> divide by 1_000_000 to get ms timestamp
        """
        # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
        twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        tdSql.execute(
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src_name}.{mid}.src_t "
            f"INTO {self.DB}.{sink} AS "
            f"SELECT cast(_twstart/{twstart_divisor} as timestamp) AS ts, "
            f"COUNT(*) AS cnt, AVG(val) AS avg_val "
            f"FROM {src_name}.{mid}.src_t"
        )
        tdLog.info(f"[{src_name}] stream created; waiting for Running status...")
        tdStream.checkStreamStatus(stream)

    def _create_uc001_stream_no_trows_with_wstart(self, stream: str, sink: str,
                             src_name: str, mid: str):
        """Create the FS-UC-001 INTERVAL stream and wait until Running.

        _twstart precision differs by source type:
          MySQL / PG  : microseconds  -> divide by 1_000 to get ms timestamp
          InfluxDB    : nanoseconds   -> divide by 1_000_000 to get ms timestamp
        """
        # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
        twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
        ts_col = "time" if src_name.endswith("_i") else "ts"
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        tdSql.execute(
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src_name}.{mid}.src_t "
            f"INTO {self.DB}.{sink} AS "
            f"SELECT cast(_twstart/{twstart_divisor} as timestamp) AS ts, "
            f"COUNT(*) AS cnt, AVG(val) AS avg_val "
            f"FROM {src_name}.{mid}.src_t where {ts_col} >=_twstart and {ts_col} < _twend"
        )
        tdLog.info(f"[{src_name}] stream created; waiting for Running status...")
        tdStream.checkStreamStatus(stream)
    
    def _create_uc001_stream_no_trows_with_range(self, stream: str, sink: str,
                             src_name: str, mid: str):
        """Create the FS-UC-001 INTERVAL stream and wait until Running.

        _twstart precision differs by source type:
          MySQL / PG  : microseconds  -> divide by 1_000 to get ms timestamp
          InfluxDB    : nanoseconds   -> divide by 1_000_000 to get ms timestamp
        """
        # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
        twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
        ts_col = "time" if src_name.endswith("_i") else "ts"
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        tdSql.execute(
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src_name}.{mid}.src_t "
            f"INTO {self.DB}.{sink} AS "
            f"SELECT cast(_twstart/{twstart_divisor} as timestamp) AS ts, "
            f"COUNT(*) AS cnt, AVG(val) AS avg_val "
            f"FROM {src_name}.{mid}.src_t where {ts_col} >= {self._BASE_MS * twstart_divisor} and {ts_col} <= {(self._BASE_MS + 120_000)*twstart_divisor}"
        )
        tdLog.info(f"[{src_name}] stream created; waiting for Running status...")
        tdStream.checkStreamStatus(stream)

    # def _uc001_influx_multi_subtable(self):
    #     """InfluxDB multi-subtable coverage for the OR-batched fetch path.

    #     One measurement (src_t) carries 3 device tags -> 3 subtables/series.
    #     PARTITION BY tbname makes the influx reader discover the tag column,
    #     register one uid per device and fetch them through the OR-compound query
    #     built by buildInfluxBatchSql / consumed by fetchDataBatchInflux
    #     (WHERE (device='d1' AND ts>..) OR (device='d2' AND ts>..) OR ...).

    #     Each device carries a distinct val (1/2/3) so the per-subtable AVG(val)
    #     identifies which series was fetched: seeing all of 1.0/2.0/3.0 proves the
    #     OR batch pulled every subtable (a broken OR would drop some devices)."""
    #     src = "uc001mi_i"
    #     i_db = "uc001mi_idb"
    #     stream = "s_uc001mi"
    #     sink_stb = "sink_uc001mi_stb"
    #     devices = (("d1", 1), ("d2", 2), ("d3", 3))

    #     self._cleanup_src(src)
    #     ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
    #     try:
    #         # 3 device tags x 5 timestamps (BASE .. BASE+240s). Line protocol tags
    #         # go after the measurement name: `src_t,device=d1 val=1i ... <ts_ns>`.
    #         lines = []
    #         for k in range(5):
    #             ts_ns = (self._BASE_MS + k * 60_000) * 1_000_000
    #             for dev, v in devices:
    #                 lines.append(
    #                     f'src_t,device={dev} val={v}i,score=1.0,name="x",flag=0i {ts_ns}'
    #                 )
    #         ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, lines)
    #         self._mk_influx_real(src, database=i_db)

    #         tdSql.execute(f"USE {self.DB}")
    #         tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
    #         tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
    #         tdSql.execute(
    #             f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
    #             f"FROM {src}.{i_db}.src_t PARTITION BY tbname "
    #             f"INTO {self.DB}.{sink_stb} "
    #             f"OUTPUT_SUBTABLE(CONCAT('m_', tbname)) AS "
    #             f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
    #             f"COUNT(*) AS cnt, AVG(val) AS avg_val "
    #             f"FROM %%trows"
    #         )
    #         tdStream.checkStreamStatus(stream)

    #         # 3 devices x 4 closed windows = 12 sink rows (the last window stays
    #         # open with no later trigger row). Each device's window has 1 row so
    #         # cnt=1 and avg_val = the device's val (1.0 / 2.0 / 3.0), four each.
    #         wait_stream_window_closed(stream, self.DB, sink_stb,
    #                                   expected_rows=12, timeout=120)
    #         res = tdSql.getResult(
    #             f"SELECT avg_val FROM {self.DB}.{sink_stb} ORDER BY avg_val"
    #         )
    #         expected = [(1.0,)] * 4 + [(2.0,)] * 4 + [(3.0,)] * 4
    #         tdSql.checkEqual(res, expected)
    #         tdLog.info("[uc001mi] verified 3 influx subtables via OR-batched fetch")

    #         tdSql.execute(f"DROP STREAM {stream}")
    #         tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
    #     finally:
    #         self._cleanup_src(src)
    #         try:
    #             ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
    #         except Exception:
    #             pass

    def test_uc_001b_influx_subset_partition(self):
        """InfluxDB PARTITION BY a tag *subset* -> many uids share one groupId.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader (uid = hash(full tagset host|region)).

        The stream partitions by host ONLY.  streamReaderExt.c must therefore
        derive groupId = hash(host) alone, so the two region-series of a host
        (e.g. a/x and a/y) collapse into a single group even though they are
        distinct uids.  This is the discriminating check for the uid vs groupId
        split: correct behavior yields 2 groups; a regression that keyed groupId
        off the full tagset (uid == groupId) would yield 4.

        Values are chosen so AVG(val) per host proves the collapse:
          host a: region x val=1, region y val=3  -> AVG = 2.0
          host b: region x val=2, region y val=4  -> AVG = 3.0
        A per-full-tag (broken) grouping would instead expose the raw
        1/2/3/4 values across 4 groups.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_i"
        i_db = "uc001sp_idb"
        stream = "s_uc001sp"
        sink_stb = "sink_uc001sp_stb"
        # (host, region, val)
        series = (
            ("a", "x", 1), ("a", "y", 3),
            ("b", "x", 2), ("b", "y", 4),
        )

        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            def build_lines(ts_list_ms, value_map):
                lines = []
                for ts_ms in ts_list_ms:
                    ts_ns = ts_ms * 1_000_000
                    for host, region, _ in series:
                        lines.append(
                            f'src_t,host={host},region={region} '
                            f'val={value_map[(host, region)]}i,score=1.0,name="x",flag=0i {ts_ns}'
                        )
                return lines

            # Prewrite an older batch before CREATE STREAM. If the stream
            # incorrectly backfills historical rows, these values would surface
            # as avg 20.0 / 30.0 for hosts a / b and break the final assertion.
            pre_lines = build_lines(
                [
                    self._BASE_MS - 180_000,
                    self._BASE_MS - 120_000,
                    self._BASE_MS - 60_000,
                ],
                {
                    ("a", "x"): 10,
                    ("a", "y"): 30,
                    ("b", "x"): 20,
                    ("b", "y"): 40,
                },
            )
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, pre_lines)
            self._mk_influx_real(src, database=i_db)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY host "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', cast(host as varchar(32)))) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            # 4 series x 5 timestamps (BASE .. BASE+240s). Line-protocol tags
            # (host, region) go after the measurement name.
            post_lines = build_lines(
                [self._BASE_MS + k * 60_000 for k in range(5)],
                {
                    ("a", "x"): 1,
                    ("a", "y"): 3,
                    ("b", "x"): 2,
                    ("b", "y"): 4,
                },
            )
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, post_lines)

            # Only the post-stream batch should be visible: 2 host groups x 4
            # closed windows = 8 sink rows (last window stays open). Each host
            # window aggregates BOTH region series -> cnt=2 and avg_val = 2.0
            # (host a) / 3.0 (host b).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=8, timeout=120)

            # First confirm exactly the two expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved "host"
            # group-col value is both present and case-correct, not just that some
            # 2 groups exist).
            tb_res = tdSql.getResult(
                f"SELECT DISTINCT tbname FROM {self.DB}.{sink_stb}"
            )
            actual_tbnames = sorted(row[0] for row in tb_res)
            tdSql.checkEqual(actual_tbnames, ["h_a", "h_b"])

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-host
            # aggregate values -- not just an unordered (cnt, avg_val) bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_host = {
                "h_a": 2.0,  # region x val=1, region y val=3 -> AVG = 2.0
                "h_b": 3.0,  # region x val=2, region y val=4 -> AVG = 3.0
            }
            for sub_tbname, avg_val in expected_by_host.items():
                res = tdSql.getResult(
                    f"SELECT ts, cnt, avg_val FROM {self.DB}.{sub_tbname} ORDER BY ts"
                )
                expected = [
                    (self._ms_to_dt(ts_ms), 2, avg_val) for ts_ms in window_starts_ms
                ]
                tdSql.checkEqual(res, expected)

            tdLog.info("[uc001sp] verified subset PARTITION BY host: "
                       "4 uids collapsed into 2 groups, "
                       "row-by-row match on h_a/h_b subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000),    1, 7.0),
            (self._ms_to_dt(self._BASE_MS + 120_000),   1, 8.0),
            (self._ms_to_dt(self._BASE_MS + 180_000),   1, 9.0),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid)
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._insert_rows(suffix, db_or_bucket, self._BATCH2, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (two-phase verify)
    #
    # Insert batch1 first, verify that only the two fully-closed windows
    # (08:00, 08:01) appear in the sink.  Then insert batch2 and verify
    # that the remaining two windows (08:02, 08:03) are also produced.
    # This specifically exercises the cross-batch window-close path where
    # batch2's arrival causes 08:02's window to close.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_twice(self):
        """Ext-driven INTERVAL stream: verify incremental window closing.

        Phase 1: batch1 (08:00..08:02) → windows 08:00 and 08:01 close.
        Phase 2: batch2 (08:03..08:04) → windows 08:02 and 08:03 close.
        """
        prefix = "uc001"

        # After batch1: windows 08:00 and 08:01 are closed by 08:02's arrival.
        EXPECTED1 = [
            (self._ms_to_dt(self._BASE_MS),          1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000), 1, 7.0),
        ]

        # After batch2: 08:02 closed by 08:03, 08:03 closed by 08:04.
        EXPECTED2 = [
            (self._ms_to_dt(self._BASE_MS),             1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000),    1, 7.0),
            (self._ms_to_dt(self._BASE_MS + 120_000),   1, 8.0),
            (self._ms_to_dt(self._BASE_MS + 180_000),   1, 9.0),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid)

            # Phase 1: batch1 closes windows 08:00 and 08:01.
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._verify(stream, sink, EXPECTED1, src_name)

            # Phase 2: batch2 closes windows 08:02 and 08:03.
            self._insert_rows(suffix, db_or_bucket, self._BATCH2, src_name)
            self._verify(stream, sink, EXPECTED2, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED_NO_TROWS = [
            (self._ms_to_dt(self._BASE_MS),             8, 4.5),
            (self._ms_to_dt(self._BASE_MS + 60_000),    8, 4.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream_no_trows(stream, sink, src_name, mid)
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._verify(stream, sink, EXPECTED_NO_TROWS, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)
    
    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows_with_wstart(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             1, 6),
            (self._ms_to_dt(self._BASE_MS + 60_000),    2, 7.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream_no_trows_with_wstart(stream, sink, src_name, mid)
            self._insert_rows(suffix, db_or_bucket, self._BATCH1_MORE, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)
    
    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows_with_range(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             4, 7.5),
            (self._ms_to_dt(self._BASE_MS + 60_000),    4, 7.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream_no_trows_with_range(stream, sink, src_name, mid)
            self._insert_rows(suffix, db_or_bucket, self._BATCH1_MORE, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)
        
    # ------------------------------------------------------------------
    # FS-UC-002 — Local fact + ext dim JOIN (FS §9.2)
    # ------------------------------------------------------------------
    # def test_uc_002_local_fact_join_ext_dim(self):
    #     """Local fact stable driving stream, JOIN ext dim table (PG-only)."""
    #     prefix = "uc002"
    #     tdSql.execute(f"USE {self.DB}")
    #     tdSql.execute("DROP STABLE IF EXISTS fact_st")
    #     tdSql.execute(
    #         "CREATE STABLE fact_st (ts TIMESTAMP, v INT) TAGS (device_id INT)"
    #     )
    #     tdSql.execute("CREATE TABLE fact_a USING fact_st TAGS(1)")
    #     tdSql.execute("CREATE TABLE fact_b USING fact_st TAGS(2)")

    #     def body(src_name: str):
    #         mid = _mid_segment(prefix, src_name)
    #         stream = f"s_{src_name}"
    #         sink = f"sink_{src_name}"
    #         tdSql.execute(f"USE {self.DB}")
    #         tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
    #         tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
    #         tdSql.execute(
    #             f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
    #             f"FROM {self.DB}.fact_st PARTITION BY tbname "
    #             f"INTO {self.DB}.{sink} AS "
    #             f"SELECT _twstart AS ts, COUNT(*) AS cnt, FIRST(d.name) AS dim_name "
    #             f"FROM %%trows r "
    #             f"LEFT JOIN {src_name}.{mid}.src_t d ON r.v = d.val"
    #         )
    #         ts0 = int(time.time() * 1000) - 120_000
    #         for tbl in ("fact_a", "fact_b"):
    #             for i in range(3):
    #                 tdSql.execute(
    #                     f"INSERT INTO {self.DB}.{tbl} VALUES "
    #                     f"({ts0 + i * 20_000}, {i + 1})"
    #                 )
    #         wait_stream_window_closed(stream, self.DB, sink,
    #                                   expected_rows=1, timeout=90)
    #         meta = get_stream_ext_meta(stream)
    #         assert meta.get("ext_error_count", 0) == 0, meta
    #         tdSql.execute(f"DROP STREAM {stream}")
    #         tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

    #     # PG-only mirrors FS §9.2 (pg_crm.crm.device_segment exemplar).
    #     self._with_std_sources(prefix, body, skip_mysql=True, skip_influx=True)

    # ------------------------------------------------------------------
    # FS-UC-003 — Two ext sources in one stream
    # ------------------------------------------------------------------
    def test_uc_003_two_sources_one_stream(self):
        """Single stream whose calc query JOINs two distinct EXTERNAL SOURCEs.

        The trigger reads source A; the calc SELECT joins source A with source B
        (two real external tables, NOT %%trows — %%trows may not appear in a join
        condition, rejected by the parser).  This exercises per-calc-scan ext-spec
        binding: each of the two calc readers must bind to its own source by name
        (src_a -> extSpecs[src_a], src_b -> extSpecs[src_b])."""
        prefix_a, prefix_b = "uc003a", "uc003b"
        src_a, src_b = f"{prefix_a}_m", f"{prefix_b}_m"
        db_a, db_b = f"{prefix_a}_mdb", f"{prefix_b}_mdb"
        self._cleanup_src(src_a, src_b)
        try:
            # Two MySQL sources with identical schema; data is inserted after the
            # stream is Running (see below) so the trigger drives the windows.
            for prefix, db in ((prefix_a, db_a), (prefix_b, db_b)):
                ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
                ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                    "DROP TABLE IF EXISTS `src_t`",
                    "CREATE TABLE `src_t` ("
                    "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
                    "  name VARCHAR(32), flag TINYINT(1))",
                ])
                self._mk_mysql_real(f"{prefix}_m", database=db)

            stream = "s_uc003"
            sink = "sink_uc003"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            # calc JOINs two real external tables (source A + source B). Using
            # %%trows in the join condition is rejected by the parser, so the calc
            # reads the ext table of source A directly and joins source B.
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_a}.{db_a}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast(_twstart/1000 as timestamp) AS ts, "
                f"SUM(a.val + b.val) AS s "
                f"FROM {src_a}.{db_a}.src_t a "
                f"JOIN {src_b}.{db_b}.src_t b ON a.ts = b.ts"
            )
            tdStream.checkStreamStatus(stream)

            # 5 rows per source aligned on ts so the calc JOIN matches row-by-row.
            # source A val = 1..5, source B val = 10,20,..,50.  With a direct ext
            # scan (no %%trows), each triggered window aggregates the whole joined
            # table, so every closed window's s = SUM(a.val+b.val) over all rows
            #   = (1+2+3+4+5) + (10+20+30+40+50) = 15 + 150 = 165.
            # The value 165 proves BOTH sources are joined (source A alone -> 15,
            # source B alone -> 150), i.e. each calc reader bound to the right src.
            rows_a = [(self._BASE_MS + k * 60_000, k + 1,        1.0, 'a', 0) for k in range(5)]
            rows_b = [(self._BASE_MS + k * 60_000, (k + 1) * 10, 1.0, 'b', 0) for k in range(5)]
            self._insert_rows('m', db_a, rows_a, src_a)
            self._insert_rows('m', db_b, rows_b, src_b)

            # 5 trigger rows -> the first 4 windows close (the last stays open with
            # no later trigger row), each holding the full joined-table sum 165.
            EXPECTED = [
                (self._ms_to_dt(self._BASE_MS),            165.0),
                (self._ms_to_dt(self._BASE_MS + 60_000),   165.0),
                (self._ms_to_dt(self._BASE_MS + 120_000),  165.0),
                (self._ms_to_dt(self._BASE_MS + 180_000),  165.0),
            ]
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=len(EXPECTED), timeout=120)
            res = tdSql.getResult(f"SELECT ts, s FROM {self.DB}.{sink} ORDER BY ts")
            tdSql.checkEqual(res, EXPECTED)
            tdLog.info(f"[uc003] verified {len(EXPECTED)} joined sink rows OK")

            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        finally:
            self._cleanup_src(src_a, src_b)
            for db in (db_a, db_b):
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
                except Exception:
                    pass

    # # ------------------------------------------------------------------
    # # FS-UC-004 — InfluxDB measurement split (FS §9.3)
    # # ------------------------------------------------------------------
    # def test_uc_004_influx_subtable_split(self):
    #     """InfluxDB-driven stream with PARTITION BY tbname + OUTPUT_SUBTABLE."""
    #     prefix = "uc004"

    #     def body(src_name: str):
    #         mid = _mid_segment(prefix, src_name)
    #         stream = f"s_{src_name}"
    #         sink_stb = f"sink_{src_name}_stb"
    #         tdSql.execute(f"USE {self.DB}")
    #         tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
    #         tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
    #         tdSql.execute(
    #             f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
    #             f"FROM {src_name}.{mid}.src_t PARTITION BY tbname "
    #             f"INTO {self.DB}.{sink_stb} "
    #             f"OUTPUT_SUBTABLE(CONCAT('m_', tbname)) AS "
    #             f"SELECT _twstart AS ts, AVG(val) AS avg_val "
    #             f"FROM %%trows"
    #         )
    #         wait_stream_window_closed(stream, self.DB, sink_stb,
    #                                   expected_rows=1, timeout=60)
    #         meta = get_stream_ext_meta(stream)
    #         assert meta.get("ext_error_count", 0) == 0, meta
    #         tdSql.execute(f"DROP STREAM {stream}")
    #         tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")

    #     # InfluxDB only — relational sources reject PARTITION BY on ext tables.
    #     self._with_std_sources(prefix, body, skip_mysql=True, skip_pg=True)
