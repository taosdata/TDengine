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
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import (  # noqa: E402
    ensure_snode,
)


class TestFsTsColumn(FederatedQueryTestMixin):
    """FS §4.6 — ts column rules for external trigger tables."""

    DB = "fs_ts"

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
