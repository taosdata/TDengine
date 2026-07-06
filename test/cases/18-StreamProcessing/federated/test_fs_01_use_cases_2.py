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
        