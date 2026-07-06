"""FS §4.6 — ts column auto-derivation and time advancement.

  FS-TS-001  ts column is auto-derived from the PK timestamp column.
  FS-TS-002  Stream creation rejects external table without a PK ts column.
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


class TestFsTsColumn(FederatedQueryTestMixin):
    """FS §4.6 — ts column rules for external trigger tables."""

    DB = "fs_ts"

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

    # FS-TS-001 ts col auto-derivation -----------------------------------
    def test_ts_001_auto_derived(self):
        """Standard src_t has PK ts column => stream uses it automatically."""
        prefix = "ts001"

        def body(src_name: str):
            mid = f"{prefix}_{src_name[-1]}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_name}.{mid}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT _twstart AS ts, COUNT(*) AS cnt FROM %%trows"
            )
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=1, timeout=60)
            meta = get_stream_ext_meta(stream)
            # ext_last_ts should advance as windows close.
            assert int(meta.get("ext_last_ts") or 0) > 0, meta
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_pg=True, skip_influx=True)

    # FS-TS-002 missing ts col -> CREATE STREAM rejected -----------------
    def test_ts_002_missing_ts_col_rejected(self):
        """External table without a PK timestamp col => STREAM_EXT_TS_COLUMN_MISSING."""
        prefix = "ts002"
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        try:
            # Create a remote table with NO primary key timestamp column.
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "DROP TABLE IF EXISTS `no_ts`",
                "CREATE TABLE `no_ts` (id INT PRIMARY KEY, val INT)",
                "INSERT INTO `no_ts` VALUES (1, 1), (2, 2)",
            ])
            self._mk_mysql_real(src, database=db)
            tdSql.execute(f"USE {self.DB}")
            sql = (
                f"CREATE STREAM s_ts002 INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.no_ts "
                f"INTO {self.DB}.sink_ts002 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            tdSql.error(sql)
            tdLog.info("FS-TS-002: ext table without PK ts column rejected at CREATE STREAM")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass
