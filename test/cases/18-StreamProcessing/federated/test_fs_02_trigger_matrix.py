"""FS §4.4 — Trigger type matrix for stream + federated query.

Each test verifies that the corresponding trigger type works when the
trigger source is an external table. MySQL is used as the common backend
for relational triggers; InfluxDB is used where ``PARTITION BY`` is needed.

Cases (one per trigger type per FS §4.4 table):
  FS-TM-001  PERIOD
  FS-TM-002  SLIDING (no INTERVAL)
  FS-TM-003  INTERVAL + SLIDING (covered in test_fs_01 but re-asserted here)
  FS-TM-004  SESSION
  FS-TM-005  STATE_WINDOW
  FS-TM-006  EVENT_WINDOW
  FS-TM-007  COUNT_WINDOW

Each case above also has a "_trows" companion test that keeps the same
trigger clause and data, but changes the calc SELECT's FROM clause from the
raw ext table to %%trows (the trigger's own rows), verifying the trigger
row set is exposed correctly through %%trows for federated ext sources.
"""

import time
import datetime
import sys

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
    TSDB_CODE_INVALID_PARA,
    _STD_ROWS,
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    verify_sink_data,
)


class TestFsTriggerMatrix(FederatedQueryTestMixin):
    """FS §4.4 — Trigger type x external trigger table compatibility."""

    DB = "fs_tm"

    @classmethod
    def setup_class(cls):
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

    @staticmethod
    def _ms_to_dt(ms: int) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ms / 1000.0)

    @staticmethod
    def _dt_str(ms: int) -> str:
        """Convert epoch ms to 'YYYY-MM-DD HH:MM:SS.mmm' in local time."""
        dt = datetime.datetime.fromtimestamp(ms / 1000.0)
        return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms % 1000:03d}"

    def _insert_ext_rows(self, prefix: str, src_name: str, rows: list) -> None:
        """Write (ts_ms, val, score, name, flag) rows into the ext source *after*
        CREATE STREAM, so the trigger observes genuinely new data crossing a
        window/slide boundary (pre-existing rows at stream-create time are not
        backfilled -- see FS-UC-001b). Mirrors TestFsUseCases._insert_rows."""
        suffix = src_name[-1]
        db_or_bucket = f"{prefix}_{suffix}db"
        if suffix == "m":
            rows_sql = ", ".join(
                f"('{self._dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), db_or_bucket,
                [f"INSERT INTO `src_t` VALUES {rows_sql}"]
            )
            tdLog.info(f"[{src_name}] inserted {len(rows)} MySQL trigger rows")
        elif suffix == "p":
            rows_sql = ", ".join(
                f"('{self._dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.pg_exec_cfg(
                self._pg_cfg(), db_or_bucket,
                [f"INSERT INTO public.src_t VALUES {rows_sql}"]
            )
            tdLog.info(f"[{src_name}] inserted {len(rows)} PG trigger rows")
        elif suffix == "i":
            lines = [
                f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
                f'{ts * 1_000_000}'
                for ts, val, score, name, flag in rows
            ]
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{src_name}] wrote {len(rows)} InfluxDB trigger points")

    def test_nested_external_trigger_rejected(self):
        """Nested WINDOW: a real external trigger catalog is rejected.

        Validate nested external trigger rejected behavior.

        Catalog:
            - Streams:Federated:TriggerMatrix

        Since: v3.4.2.0

        Labels: common,ci,integration,functional,negative

        Feishu: None

        History:
            - 2026-08-16 Codex Added P0 external nested-trigger rejection
        """
        prefix = "nwext"

        def body(src_name: str):
            mid = f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}_nested"
            sink = f"{self.DB}.sink_{src_name}_nested"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            try:
                tdSql.error(
                    f"CREATE STREAM {stream} WINDOW ("
                    "INTERVAL(1m) SLIDING(1m) AS w_outer, COUNT_WINDOW(2,1)) "
                    f"FROM {src_name}.{mid}.src_t "
                    f"INTO {sink} AS SELECT _twstart, count(*) FROM %%trows",
                    expectedErrno=TSDB_CODE_INVALID_PARA,
                )
            finally:
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {sink}")

        self._with_std_sources(
            prefix, body, skip_pg=True, skip_influx=True
        )

    # ------------------------------------------------------------------
    # Helper: run one CREATE STREAM with given trigger header, verify
    # stream becomes ready.
    # ------------------------------------------------------------------
    def _run_trigger_case(self, prefix: str, trigger_clause: str, *,
                          expected_rows: int = 1, partition: str = "",
                          backend: str = "mysql",
                          expected_cnt_sum: int = 0,
                          expected: list = None,
                          post_create_rows: list = None,
                          wait_timeout: float = 60,
                          calc_from: str = "ext"):
        """Execute one trigger-type case against the named backend (mysql/influx).

        When `expected` is given (a list of exact result-row tuples, ordered
        by the sink's first column), the sink table is verified by exact row
        comparison instead of the generic cnt-based verify_sink_data check --
        this SELECTs raw ext-table rows, so there is no `cnt` column.

        When `post_create_rows` is given, those (ts_ms, val, score, name, flag)
        rows are written into the ext source *after* the stream reaches Running,
        guaranteeing the trigger observes new data crossing a window/slide
        boundary instead of relying on pre-existing (non-backfilled) rows.

        `calc_from` selects the calc SELECT's FROM clause:
          "ext"   : FROM the ext table directly (default, raw passthrough).
          "trows" : FROM %%trows (the trigger's own rows), same raw passthrough.
        """
        def body(src_name: str):
            # 3-segment ext table path is source.mid.table. For PG, mid is the
            # *schema* (always "public" here); the database is a fixed mount-level
            # property. For MySQL/InfluxDB, mid is the database itself.
            mid = "public" if src_name.endswith("_p") else f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
            twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
            # InfluxDB's implicit timestamp column is named "time"; MySQL/PG use "ts".
            ts_col = "time" if src_name.endswith("_i") else "ts"
            # trigger_clause may reference the trigger-source ts column via the
            # "{ts_col}" placeholder (e.g. SESSION) -- resolve it per source.
            resolved_trigger_clause = trigger_clause.format(ts_col=ts_col)
            calc_source = "%%trows" if calc_from == "trows" else f"{src_name}.{mid}.src_t"
            sql = (
                f"CREATE STREAM {stream} {resolved_trigger_clause} "
                f"FROM {src_name}.{mid}.src_t "
                f"{partition} "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast({ts_col}/{twstart_divisor} as timestamp), val FROM {calc_source}"
            )
            tdLog.info(f"trigger SQL: {sql}")
            tdSql.execute(sql)
            if post_create_rows is not None:
                tdStream.checkStreamStatus(stream)
                self._insert_ext_rows(prefix, src_name, post_create_rows)
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=expected_rows, timeout=wait_timeout)
            if expected is not None:
                res = tdSql.getResult(f"SELECT * FROM {self.DB}.{sink} ORDER BY 1")
                tdSql.checkEqual(res, expected)
                tdLog.info(f"[{stream}] verified {len(expected)} sink rows OK")
            else:
                verify_sink_data(stream, self.DB, sink,
                                 expected_total_rows=expected_rows,
                                 expected_cnt_sum=expected_cnt_sum)
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body)

    # FS-TM-001 PERIOD ----------------------------------------------------
    def test_tm_001_period(self):
        """PERIOD trigger pulls external table on schedule.

        Validate tm 001 period behavior.

        Since: v3.4.2.0

        """
        # PERIOD(10s, 10a) = every 10s with 10ms offset (from existing test_trigger_type.py).
        # The calc SQL is a raw passthrough of src_t, so the sink content is the
        # fixed _STD_ROWS dataset itself -- deterministic regardless of how many
        # times PERIOD has fired, since each firing re-selects the same 5 rows
        # (same ts primary keys) and upserts them, rather than appending.
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS]
        self._run_trigger_case("tm001", "PERIOD(10s, 10a)",
                               expected_rows=len(expected), expected=expected)

    def test_tm_001_period_trows(self):
        """PERIOD trigger, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 001 period trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000  # 2025-03-03, distinct from _STD_ROWS' 2024-01-01 range
        new_rows = [
            (base_ms,          101, 10.1, 'tm002_a', 0),
            (base_ms + 60_000, 102, 10.2, 'tm002_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]

        self._run_trigger_case("tm001t", "PERIOD(10s, 10a)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, calc_from="trows")

    # FS-TM-002 SLIDING ---------------------------------------------------
    def test_tm_002_sliding_only(self):
        """SLIDING without INTERVAL — bucketless time-driven trigger.

        Two rows spaced 1 slide period apart are written *after* CREATE
        STREAM (pre-existing rows at stream-create time are not backfilled,
        see FS-UC-001b), guaranteeing the trigger observes new data crossing
        a slide boundary and fires at least once. The calc SQL is a raw
        passthrough of src_t (same pattern as FS-TM-001/PERIOD), so a firing
        re-dumps the whole external table: STD_ROWS plus the two new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000  # 2025-03-03, distinct from _STD_ROWS' 2024-01-01 range
        new_rows = [
            (base_ms,          101, 10.1, 'tm002_a', 0),
            (base_ms + 60_000, 102, 10.2, 'tm002_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case("tm002", "SLIDING(1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90)

    def test_tm_002_sliding_only_trows(self):
        """SLIDING without INTERVAL, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 002 sliding only trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000
        new_rows = [
            (base_ms,          101, 10.1, 'tm002t_a', 0),
            (base_ms + 60_000, 102, 10.2, 'tm002t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows[:1]]
        self._run_trigger_case("tm002t", "SLIDING(1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90,
                               calc_from="trows")

    # FS-TM-003 INTERVAL + SLIDING (re-asserted) --------------------------
    def test_tm_003_interval_sliding(self):
        """INTERVAL + SLIDING — canonical time window.

        Two rows spaced 1 window apart are written *after* CREATE STREAM so
        the first row's window definitely closes (its boundary is crossed by
        the second row's ts), guaranteeing a firing instead of relying on
        pre-existing (non-backfilled) rows. Same raw-passthrough calc SQL as
        FS-TM-001/FS-TM-002, so the sink ends up with STD_ROWS plus the two
        new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000 + 3600_000  # offset from tm002's base to avoid PK clashes across runs
        new_rows = [
            (base_ms,          201, 20.1, 'tm003_a', 0),
            (base_ms + 60_000, 202, 20.2, 'tm003_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case("tm003", "INTERVAL(1m) SLIDING(1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90)

    def test_tm_003_interval_sliding_trows(self):
        """INTERVAL + SLIDING, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 003 interval sliding trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000 + 3600_000
        new_rows = [
            (base_ms,          201, 20.1, 'tm003t_a', 0),
            (base_ms + 60_000, 202, 20.2, 'tm003t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows[:1]]
        self._run_trigger_case("tm003t", "INTERVAL(1m) SLIDING(1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90,
                               calc_from="trows")

    # FS-TM-004 SESSION ---------------------------------------------------
    def test_tm_004_session(self):
        """SESSION(ts, gap) — session window over external ts column.

        Two rows spaced 90s apart (> the 1m gap) are written *after* CREATE
        STREAM: the gap exceeds the session threshold, so the session
        containing the first row closes as soon as the second row arrives
        (pre-existing rows are not backfilled, see FS-UC-001b). Same
        raw-passthrough calc SQL as FS-TM-001..003, so the sink ends up with
        STD_ROWS plus the two new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000 + 2 * 3600_000
        new_rows = [
            (base_ms,          301, 30.1, 'tm004_a', 0),
            (base_ms + 90_000, 302, 30.2, 'tm004_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case("tm004", "SESSION({ts_col}, 1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90)

    def test_tm_004_session_trows(self):
        """SESSION, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 004 session trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000 + 2 * 3600_000
        new_rows = [
            (base_ms,          301, 30.1, 'tm004t_a', 0),
            (base_ms + 90_000, 302, 30.2, 'tm004t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows[:1]]
        self._run_trigger_case("tm004t", "SESSION({ts_col}, 1m)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90,
                               calc_from="trows")

    # FS-TM-005 STATE_WINDOW ----------------------------------------------
    def test_tm_005_state_window(self):
        """STATE_WINDOW(col) — state-change windowing on flag column.

        Two rows with different `flag` values are written *after* CREATE
        STREAM: the state change on the second row closes the state window
        opened by the first row, guaranteeing a firing. Same raw-passthrough
        calc SQL as FS-TM-001..004, so the sink ends up with STD_ROWS plus
        the two new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000 + 3 * 3600_000
        new_rows = [
            (base_ms,          401, 40.1, 'tm005_a', 0),
            (base_ms + 60_000, 402, 40.2, 'tm005_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case("tm005", "STATE_WINDOW(flag)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90)

    def test_tm_005_state_window_trows(self):
        """STATE_WINDOW, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 005 state window trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000 + 3 * 3600_000
        new_rows = [
            (base_ms,          401, 40.1, 'tm005t_a', 0),
            (base_ms + 60_000, 402, 40.2, 'tm005t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows[:1]]
        self._run_trigger_case("tm005t", "STATE_WINDOW(flag)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90,
                               calc_from="trows")

    # FS-TM-006 EVENT_WINDOW ----------------------------------------------
    def test_tm_006_event_window(self):
        """EVENT_WINDOW(START WITH ... END WITH ...) — event-bracketed window.

        Two rows are written *after* CREATE STREAM: the first (val=1)
        satisfies START WITH val > 0 and opens the event window; the second
        (val=5) satisfies END WITH val > 4 and closes it, guaranteeing a
        firing. Same raw-passthrough calc SQL as FS-TM-001..005, so the sink
        ends up with STD_ROWS plus the two new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000 + 4 * 3600_000
        new_rows = [
            (base_ms,          1, 50.1, 'tm006_a', 0),
            (base_ms + 60_000, 5, 50.2, 'tm006_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case(
            "tm006",
            "EVENT_WINDOW(START WITH val > 0 END WITH val > 4)",
            expected_rows=len(expected), expected=expected,
            post_create_rows=new_rows, wait_timeout=90,
        )

    def test_tm_006_event_window_trows(self):
        """EVENT_WINDOW, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 006 event window trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000 + 4 * 3600_000
        new_rows = [
            (base_ms,          1, 50.1, 'tm006t_a', 0),
            (base_ms + 60_000, 5, 50.2, 'tm006t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        self._run_trigger_case(
            "tm006t",
            "EVENT_WINDOW(START WITH val > 0 END WITH val > 4)",
            expected_rows=len(expected), expected=expected,
            post_create_rows=new_rows, wait_timeout=90,
            calc_from="trows",
        )

    # FS-TM-007 COUNT_WINDOW ----------------------------------------------
    def test_tm_007_count_window(self):
        """COUNT_WINDOW(n) — row-count window on external trigger.

        Exactly 2 rows (== COUNT_WINDOW(2)'s size) are written *after*
        CREATE STREAM, guaranteeing one complete count-window closes. Same
        raw-passthrough calc SQL as FS-TM-001..006, so the sink ends up with
        STD_ROWS plus the two new rows.

        Since: v3.4.2.0

        """
        base_ms = 1741000000000 + 5 * 3600_000
        new_rows = [
            (base_ms,          601, 60.1, 'tm007_a', 0),
            (base_ms + 60_000, 602, 60.2, 'tm007_b', 1),
        ]
        expected = sorted(
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in _STD_ROWS] +
            [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        )
        self._run_trigger_case("tm007", "COUNT_WINDOW(2)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90)

    def test_tm_007_count_window_trows(self):
        """COUNT_WINDOW, calc SQL reads %%trows instead of the ext table directly.

        Validate tm 007 count window trows behavior.

        Since: v3.4.2.0

        """
        base_ms = 1741100000000 + 5 * 3600_000
        new_rows = [
            (base_ms,          601, 60.1, 'tm007t_a', 0),
            (base_ms + 60_000, 602, 60.2, 'tm007t_b', 1),
        ]
        expected = [(self._ms_to_dt(ts), val) for ts, val, _, _, _ in new_rows]
        self._run_trigger_case("tm007t", "COUNT_WINDOW(2)",
                               expected_rows=len(expected), expected=expected,
                               post_create_rows=new_rows, wait_timeout=90,
                               calc_from="trows")
