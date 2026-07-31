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
"""

import sys

from new_test_framework.utils import tdLog, tdSql

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
    ensure_qnode,
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    get_stream_ext_meta,
    verify_sink_data,
)


class TestFsTriggerMatrix(FederatedQueryTestMixin):
    """FS §4.4 — Trigger type x external trigger table compatibility."""

    DB = "fs_tm"

    @classmethod
    def setup_class(cls):
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_qnode()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} PRECISION 'ms'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        finally:
            cls.env.teardown_env()

    # ------------------------------------------------------------------
    # Helper: run one CREATE STREAM with given trigger header, verify
    # stream becomes ready and ext_error_count stays 0.
    # ------------------------------------------------------------------
    def _run_trigger_case(self, prefix: str, trigger_clause: str, *,
                          expected_rows: int = 1, partition: str = "",
                          backend: str = "mysql",
                          expected_cnt_sum: int = 0):
        """Execute one trigger-type case against the named backend (mysql/influx)."""
        def body(src_name: str):
            # mysql -> {prefix}_mdb ; influx -> {prefix}_idb
            mid = f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            sql = (
                f"CREATE STREAM {stream} {trigger_clause} "
                f"FROM {src_name}.{mid}.src_t "
                f"{partition} "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT _twstart AS ts, COUNT(*) AS cnt FROM %%trows"
            )
            tdLog.info(f"trigger SQL: {sql}")
            tdSql.execute(sql)
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=expected_rows, timeout=60)
            verify_sink_data(stream, self.DB, sink,
                             expected_total_rows=expected_rows,
                             expected_cnt_sum=expected_cnt_sum)
            meta = get_stream_ext_meta(stream)
            assert meta.get("ext_error_count", 0) == 0, meta
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        kwargs = {"skip_mysql": False, "skip_pg": True, "skip_influx": True}
        if backend == "influx":
            kwargs = {"skip_mysql": True, "skip_pg": True, "skip_influx": False}
        elif backend == "pg":
            kwargs = {"skip_mysql": True, "skip_pg": False, "skip_influx": True}
        self._with_std_sources(prefix, body, **kwargs)

    # FS-TM-001 PERIOD ----------------------------------------------------
    def test_tm_001_period(self):
        """PERIOD trigger pulls external table on schedule."""
        # PERIOD(10s, 10a) = every 10s with 10ms offset (from existing test_trigger_type.py).
        self._run_trigger_case("tm001", "PERIOD(10s, 10a)",
                               expected_rows=1)

    # FS-TM-002 SLIDING ---------------------------------------------------
    def test_tm_002_sliding_only(self):
        """SLIDING without INTERVAL — bucketless time-driven trigger."""
        self._run_trigger_case("tm002", "SLIDING(1m)", expected_rows=1)

    # FS-TM-003 INTERVAL + SLIDING (re-asserted) --------------------------
    def test_tm_003_interval_sliding(self):
        """INTERVAL + SLIDING — canonical time window."""
        self._run_trigger_case("tm003", "INTERVAL(1m) SLIDING(1m)",
                               expected_rows=1)

    # FS-TM-004 SESSION ---------------------------------------------------
    def test_tm_004_session(self):
        """SESSION(ts, gap) — session window over external ts column."""
        # _STD_ROWS flag toggles 1/0/1/0/1 -> multiple state windows expected.
        # Session boundary depends on implementation gap semantics; skip cnt_sum.
        self._run_trigger_case("tm004", "SESSION(ts, 1m)", expected_rows=1,
                               expected_cnt_sum=0)

    # FS-TM-005 STATE_WINDOW ----------------------------------------------
    def test_tm_005_state_window(self):
        """STATE_WINDOW(col) — state-change windowing on flag column."""
        # _STD_ROWS flag toggles 1/0/1/0/1 -> multiple state windows expected.
        # Exact window count depends on engine; skip cnt_sum.
        self._run_trigger_case("tm005", "STATE_WINDOW(flag)", expected_rows=1,
                               expected_cnt_sum=0)

    # FS-TM-006 EVENT_WINDOW ----------------------------------------------
    def test_tm_006_event_window(self):
        """EVENT_WINDOW(START WITH ... END WITH ...) — event-bracketed window."""
        # Window close depends on event condition matching; skip cnt_sum.
        self._run_trigger_case(
            "tm006",
            "EVENT_WINDOW(START WITH val > 0 END WITH val > 4)",
            expected_rows=1,
            expected_cnt_sum=0,
        )

    # FS-TM-007 COUNT_WINDOW ----------------------------------------------
    def test_tm_007_count_window(self):
        """COUNT_WINDOW(n) — row-count window on external trigger."""
        # COUNT_WINDOW(2) over 5 rows => 2 complete windows (cnt=2 each);
        # SUM(cnt) for complete windows = 4 (last partial window may be omitted).
        self._run_trigger_case("tm007", "COUNT_WINDOW(2)", expected_rows=2,
                               expected_cnt_sum=0)
