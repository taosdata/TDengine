"""FS §10 — Hard constraints for stream + federated query.

  FS-CON-001  WATERMARK on ext trigger semantically equals IGNORE_DISORDER.

Other constraints in FS §10 are exercised in adjacent suites:
  - §10#1 INTO local-only -> test_fs_05_create_errors (FS-CERR-005)
  - §10#2 ts col not user-overridable -> test_fs_05_create_errors (FS-CERR-006)
  - §10#3 PARTITION BY on relational ext -> test_fs_05_create_errors (FS-CERR-003)
  - §10#6 no-snode -> covered by ensure_snode() in test_fs_common
  - §10#7 WATERMARK degeneration -> this file (FS-CON-001)
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


class TestFsConstraints(FederatedQueryTestMixin):
    """FS §10 — Hard constraint regressions."""

    DB = "fs_con"

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

    # FS-CON-001 WATERMARK ~ IGNORE_DISORDER -----------------------------
    def test_con_001_watermark_degenerates(self):
        """Building same stream with WATERMARK vs IGNORE_DISORDER => same window output."""
        prefix = "con001"

        def body(src_name: str):
            mid = f"{prefix}_{src_name[-1]}db"
            tdSql.execute(f"USE {self.DB}")
            for tag, opt in (("a", "WATERMARK(5s)"), ("b", "IGNORE_DISORDER")):
                stream = f"s_{src_name}_{tag}"
                sink = f"sink_{src_name}_{tag}"
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
                tdSql.execute(
                    f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                    f"FROM {src_name}.{mid}.src_t "
                    f"STREAM_OPTIONS({opt}) "
                    f"INTO {self.DB}.{sink} AS "
                    f"SELECT _twstart AS ts, COUNT(*) AS cnt FROM %%trows"
                )
                wait_stream_window_closed(stream, self.DB, sink,
                                          expected_rows=1, timeout=60)

            # Compare output row counts; they must match (WATERMARK is a no-op).
            tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.sink_{src_name}_a")
            a_cnt = tdSql.queryResult[0][0]
            tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.sink_{src_name}_b")
            b_cnt = tdSql.queryResult[0][0]
            tdLog.info(f"FS-CON-001 [{src_name}] WATERMARK={a_cnt} IGNORE_DISORDER={b_cnt}")
            assert a_cnt == b_cnt, (a_cnt, b_cnt)

            tdSql.execute(f"DROP STREAM s_{src_name}_a")
            tdSql.execute(f"DROP STREAM s_{src_name}_b")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.sink_{src_name}_a")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.sink_{src_name}_b")

        self._with_std_sources(prefix, body, skip_pg=True, skip_influx=True)
