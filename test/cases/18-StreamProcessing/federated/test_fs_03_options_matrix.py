"""FS §4.5 — STREAM_OPTIONS matrix for stream + federated query.

Six representative options out of FS §4.5 table — chosen for verifiability:

  FS-OPT-001  WATERMARK  — accepted but degenerates to IGNORE_DISORDER.
  FS-OPT-002  EXPIRED_TIME — rows with ts < now-exp_time are skipped.
  FS-OPT-003  IGNORE_DISORDER — redundant but accepted.
  FS-OPT-004  FILL_HISTORY(start_time) — initial range query, then incremental.
  FS-OPT-005  PRE_FILTER(expr) — predicate pushdown to remote.
  FS-OPT-006  MAX_DELAY(delay) — processing-time forced trigger.

Verification scope: stream creation succeeds and ext_error_count == 0
within timeout. Detailed semantic verification of pushdown / fill_history
behaviour is out of scope for this matrix (covered in FS query suite).
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
)


class TestFsOptionsMatrix(FederatedQueryTestMixin):
    """FS §4.5 — STREAM_OPTIONS x external source compatibility."""

    DB = "fs_opt"

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

    def _run_with_option(self, prefix: str, option_clause: str,
                         *, expected_rows: int = 1):
        """Run a canonical INTERVAL(1m) stream with the given STREAM_OPTIONS clause."""
        def body(src_name: str):
            mid = f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            sql = (
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_name}.{mid}.src_t "
                f"{option_clause} "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT _twstart AS ts, COUNT(*) AS cnt FROM %%trows"
            )
            tdLog.info(f"options SQL: {sql}")
            tdSql.execute(sql)
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=expected_rows, timeout=60)
            meta = get_stream_ext_meta(stream)
            assert meta.get("ext_error_count", 0) == 0, meta
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body,
                               skip_pg=True, skip_influx=True)

    # FS-OPT-001 WATERMARK ----------------------------------------------
    def test_opt_001_watermark_accepted(self):
        """WATERMARK accepted on ext trigger; FS §4.5 says degenerates to IGNORE_DISORDER."""
        self._run_with_option("opt001", "STREAM_OPTIONS(WATERMARK(1m))")

    # FS-OPT-002 EXPIRED_TIME -------------------------------------------
    def test_opt_002_expired_time(self):
        """EXPIRED_TIME enforced: rows ts < now-exp_time are skipped on ext source."""
        # 1 day expiry — _STD_ROWS at 2024-01-01 are older than 1d so they'd
        # be skipped; this case only verifies the option is accepted by the
        # planner. Window-content verification belongs in a behavioural test.
        self._run_with_option("opt002", "STREAM_OPTIONS(EXPIRED_TIME(1d))",
                              expected_rows=0)

    # FS-OPT-003 IGNORE_DISORDER ----------------------------------------
    def test_opt_003_ignore_disorder_redundant(self):
        """IGNORE_DISORDER allowed (redundant for ext sources)."""
        self._run_with_option("opt003", "STREAM_OPTIONS(IGNORE_DISORDER)")

    # FS-OPT-004 FILL_HISTORY -------------------------------------------
    def test_opt_004_fill_history(self):
        """FILL_HISTORY(start_time) issues initial range query then incremental."""
        # _STD_ROWS start at 2024-01-01 00:00:00; pick a start strictly older.
        self._run_with_option(
            "opt004",
            "STREAM_OPTIONS(FILL_HISTORY('2023-12-31 00:00:00.000'))",
        )

    # FS-OPT-005 PRE_FILTER ---------------------------------------------
    def test_opt_005_pre_filter_pushdown(self):
        """PRE_FILTER(expr) accepted; expr should push down to remote WHERE."""
        # PRE_FILTER with a column from the standard schema.
        self._run_with_option("opt005",
                              "STREAM_OPTIONS(PRE_FILTER(val > 0))")

    # FS-OPT-006 MAX_DELAY ----------------------------------------------
    def test_opt_006_max_delay(self):
        """MAX_DELAY(delay) — processing-time forced trigger accepted."""
        self._run_with_option("opt006", "STREAM_OPTIONS(MAX_DELAY(2s))")
