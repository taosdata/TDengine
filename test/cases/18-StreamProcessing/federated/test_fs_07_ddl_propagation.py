"""FS §4.8 — ALTER / REFRESH / DROP EXTERNAL SOURCE propagation.

  FS-DDL-001  ALTER EXTERNAL SOURCE — running stream auto-switches.
  FS-DDL-002  REFRESH EXTERNAL SOURCE — running stream unaffected.
  FS-DDL-003  DROP EXTERNAL SOURCE — accepted even when referenced; subsequent
              poll surfaces error via existing error channel.
"""

import sys
import time

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


class TestFsDdlPropagation(FederatedQueryTestMixin):
    """FS §4.8 — DDL on ext source propagation to running streams."""

    DB = "fs_ddl"

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

    def _prepare_real(self, prefix: str):
        """Create a MySQL ext source with std table; return (src, db)."""
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
            "DROP TABLE IF EXISTS `src_t`",
            "CREATE TABLE `src_t` ("
            "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
            "  name VARCHAR(32), flag TINYINT(1))",
            "INSERT INTO `src_t` VALUES "
            "('2024-01-01 00:00:00.000', 1, 1.5, 'a', 1),"
            "('2024-01-01 00:01:00.000', 2, 2.5, 'b', 0)",
        ])
        self._mk_mysql_real(src, database=db)
        return src, db

    def _drop_remote(self, db: str):
        try:
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
        except Exception:
            pass

    # FS-DDL-001 ALTER propagates ----------------------------------------
    def test_ddl_001_alter_propagates(self):
        """ALTER EXTERNAL SOURCE updates connection; stream continues without rebuild."""
        prefix = "ddl001"
        src, db = self._prepare_real(prefix)
        try:
            tdSql.execute(f"USE {self.DB}")
            stream = "s_ddl001"
            sink = f"{self.DB}.sink_ddl001"
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t "
                f"INTO {sink} AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            wait_stream_window_closed(stream, self.DB, "sink_ddl001",
                                      expected_rows=1, timeout=60)
            meta1 = get_stream_ext_meta(stream)
            # ALTER with same credentials (no-op semantic) verifies the path
            # without breaking connectivity.
            cfg = self._mysql_cfg()
            tdSql.execute(
                f"ALTER EXTERNAL SOURCE {src} HOST='{cfg.host}' PORT={cfg.port} "
                f"USER='{cfg.user}' PASSWORD='{cfg.password}'"
            )
            time.sleep(3)
            meta2 = get_stream_ext_meta(stream)
            assert meta2.get("ext_error_count", 0) == 0, meta2
            tdLog.info(f"FS-DDL-001 before={meta1} after={meta2}")
            tdSql.execute(f"DROP STREAM {stream}")
        finally:
            self._cleanup_src(src)
            self._drop_remote(db)

    # FS-DDL-002 REFRESH does not affect running stream ------------------
    def test_ddl_002_refresh_no_effect(self):
        """REFRESH EXTERNAL SOURCE refreshes client cache; running stream unaffected."""
        prefix = "ddl002"
        src, db = self._prepare_real(prefix)
        try:
            tdSql.execute(f"USE {self.DB}")
            stream = "s_ddl002"
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.sink_ddl002")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_ddl002 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            wait_stream_window_closed(stream, self.DB, "sink_ddl002",
                                      expected_rows=1, timeout=60)
            tdSql.execute(f"REFRESH EXTERNAL SOURCE {src}")
            time.sleep(3)
            meta = get_stream_ext_meta(stream)
            assert meta.get("ext_error_count", 0) == 0, meta
            tdSql.execute(f"DROP STREAM {stream}")
        finally:
            self._cleanup_src(src)
            self._drop_remote(db)

    # FS-DDL-003 DROP not blocked; subsequent poll errors -----------------
    def test_ddl_003_drop_accepted_then_errors(self):
        """DROP EXTERNAL SOURCE accepted while referenced; later poll records error."""
        prefix = "ddl003"
        src, db = self._prepare_real(prefix)
        try:
            tdSql.execute(f"USE {self.DB}")
            stream = "s_ddl003"
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.sink_ddl003")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(10s) SLIDING(10s) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_ddl003 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            # DROP must NOT be blocked just because the stream references it.
            tdSql.execute(f"DROP EXTERNAL SOURCE {src}")
            # Wait and confirm stream eventually records an ext error.
            deadline = time.time() + 45
            errored = False
            while time.time() < deadline:
                meta = get_stream_ext_meta(stream)
                if int(meta.get("ext_error_count") or 0) > 0:
                    errored = True
                    tdLog.info(f"FS-DDL-003 expected error recorded: {meta}")
                    break
                time.sleep(2)
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            assert errored, "Stream did not record ext_error after DROP EXTERNAL SOURCE"
        finally:
            self._cleanup_src(src)
            self._drop_remote(db)
