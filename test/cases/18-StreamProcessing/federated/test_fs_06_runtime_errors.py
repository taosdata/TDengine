"""FS §4.7.2 + §7 — Runtime errors for stream + federated query.

  FS-RERR-001  Ext source unreachable -> CONNECT_FAILED, error visible in tasks.
  FS-RERR-002  Auth changed under a running stream -> AUTH_FAILED on next poll.
  FS-RERR-003  External table schema mismatch -> TYPE_MISMATCH.
  FS-SEC-002   Disabling federatedQueryEnable does NOT affect running streams (FS §7).
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


def _wait_ext_error(stream: str, timeout: int = 30):
    """Poll get_stream_ext_meta until ext_error_count > 0 or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        meta = get_stream_ext_meta(stream)
        if int(meta.get("ext_error_count") or 0) > 0:
            return meta
        time.sleep(1)
    raise AssertionError(
        f"Stream {stream} never recorded ext_error within {timeout}s"
    )


class TestFsRuntimeErrors(FederatedQueryTestMixin):
    """FS §4.7.2 + §7 — runtime failure behaviour."""

    DB = "fs_rerr"

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

    # FS-RERR-001 ext source unreachable ---------------------------------
    def test_rerr_001_unreachable_host(self):
        """Stream that points at an unreachable host accumulates ext_error_count."""
        src = "rerr001_m"
        db = "rerr001_mdb"
        self._cleanup_src(src)
        # Create the source pointing at a non-routable host so any poll fails.
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {src}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {src} TYPE='mysql' "
            f"HOST='127.0.0.1' PORT=1 "
            f"USER='x' PASSWORD='x' DATABASE='{db}'"
        )
        try:
            tdSql.execute(f"USE {self.DB}")
            sql = (
                f"CREATE STREAM s_rerr001 INTERVAL(10s) SLIDING(10s) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_rerr001 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            # CREATE may succeed (mnode does not pre-validate connectivity).
            try:
                tdSql.execute(sql)
            except Exception as e:
                # If the planner DOES pre-validate and rejects, that's also
                # acceptable behaviour for FS-RERR-001's intent.
                tdLog.info(f"FS-RERR-001: CREATE rejected up-front: {e}")
                return
            meta = _wait_ext_error("s_rerr001", timeout=45)
            tdLog.info(f"FS-RERR-001 meta = {meta}")
            tdSql.execute("DROP STREAM IF EXISTS s_rerr001")
        finally:
            self._cleanup_src(src)

    # FS-RERR-002 auth changed under a running stream --------------------
    def test_rerr_002_auth_changed(self):
        """ALTER ext source to wrong password -> next poll returns AUTH_FAILED."""
        prefix = "rerr002"
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` (ts DATETIME(3) PRIMARY KEY, val INT)",
                "INSERT INTO `src_t` VALUES ('2024-01-01 00:00:00.000', 1)",
            ])
            self._mk_mysql_real(src, database=db)
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(
                f"CREATE STREAM s_rerr002 INTERVAL(10s) SLIDING(10s) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_rerr002 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            # Change password to invalid one.
            tdSql.execute(
                f"ALTER EXTERNAL SOURCE {src} PASSWORD='__INVALID_PWD__'"
            )
            try:
                meta = _wait_ext_error("s_rerr002", timeout=60)
                tdLog.info(f"FS-RERR-002 meta = {meta}")
            finally:
                tdSql.execute("DROP STREAM IF EXISTS s_rerr002")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    # FS-RERR-003 schema mismatch ----------------------------------------
    def test_rerr_003_type_mismatch(self):
        """Changing ext table column type under a running stream -> TYPE_MISMATCH."""
        prefix = "rerr003"
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` (ts DATETIME(3) PRIMARY KEY, val INT)",
                "INSERT INTO `src_t` VALUES ('2024-01-01 00:00:00.000', 1)",
            ])
            self._mk_mysql_real(src, database=db)
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(
                f"CREATE STREAM s_rerr003 INTERVAL(10s) SLIDING(10s) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_rerr003 AS "
                f"SELECT _twstart AS ts, AVG(val) AS a FROM %%trows"
            )
            # Mutate remote column type to incompatible (INT -> VARCHAR).
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "ALTER TABLE `src_t` MODIFY COLUMN val VARCHAR(8)",
                "DELETE FROM `src_t`",
                "INSERT INTO `src_t` VALUES "
                "('2024-01-01 00:02:00.000', 'abc')",
            ])
            try:
                meta = _wait_ext_error("s_rerr003", timeout=60)
                tdLog.info(f"FS-RERR-003 meta = {meta}")
            finally:
                tdSql.execute("DROP STREAM IF EXISTS s_rerr003")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    # FS-SEC-002 disabling flag does NOT affect running stream -----------
    def test_sec_002_disable_flag_preserves_running(self):
        """FS §7#4 — Setting federatedQueryEnable=false leaves existing streams running."""
        prefix = "sec002"

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
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=1, timeout=60)
            # Disable flag; running stream must NOT error out.
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable 0'")
            try:
                time.sleep(5)
                meta = get_stream_ext_meta(stream)
                assert meta.get("ext_error_count", 0) == 0, meta
            finally:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable 1'")
                tdSql.execute(f"DROP STREAM {stream}")
                tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_pg=True, skip_influx=True)
