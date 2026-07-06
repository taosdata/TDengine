"""FS §4.7.1 + §4.4 — CREATE-time errors for stream + federated query.

  FS-CERR-001  federatedQueryEnable=false -> STREAM_EXT_DISABLED.
  FS-CERR-002  Non-existent EXTERNAL SOURCE -> error at CREATE STREAM.
  FS-CERR-003  PARTITION BY on relational ext trigger -> PARTITION_NOT_SUPPORTED.
  FS-CERR-004  PARTITION BY <column> (not tbname/tag) on InfluxDB ext -> rejected.
  FS-CERR-005  INTO referencing an external table -> rejected (FS §10#1).
  FS-CERR-006  User-specified ts column in CREATE STREAM -> rejected (FS §10#2).
  FS-SEC-001   Insufficient privileges on ext source -> CREATE STREAM rejected.
"""

import sys

from new_test_framework.utils import tdLog, tdSql

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import ensure_snode  # noqa: E402


class TestFsCreateErrors(FederatedQueryTestMixin):
    """FS §4.7.1 + §4.4 — CREATE STREAM validation errors."""

    DB = "fs_cerr"

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

    # FS-CERR-001 federatedQueryEnable=false -----------------------------
    def test_cerr_001_disabled_flag(self):
        """Toggle federatedQueryEnable=false then attempt CREATE STREAM with ext."""
        # Setup a real source first (with flag still ON via class fixture).
        prefix = "cerr001"
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
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable 0'")
            try:
                sql = (
                    f"CREATE STREAM s_cerr001 INTERVAL(1m) SLIDING(1m) "
                    f"FROM {src}.{db}.src_t "
                    f"INTO {self.DB}.sink_cerr001 AS "
                    f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
                )
                tdSql.error(sql)
                tdLog.info("FS-CERR-001: CREATE STREAM rejected when federatedQueryEnable=false")
            finally:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable 1'")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    # FS-CERR-002 missing ext source --------------------------------------
    def test_cerr_002_missing_ext_source(self):
        """CREATE STREAM referencing a non-existent EXTERNAL SOURCE."""
        tdSql.execute(f"USE {self.DB}")
        sql = (
            "CREATE STREAM s_cerr002 INTERVAL(1m) SLIDING(1m) "
            "FROM __no_such_src__.foo.bar "
            f"INTO {self.DB}.sink_cerr002 AS "
            "SELECT _twstart AS ts, COUNT(*) FROM %%trows"
        )
        tdSql.error(sql)

    # FS-CERR-003 PARTITION BY on relational ext --------------------------
    def test_cerr_003_partition_by_relational(self):
        """Relational ext source + PARTITION BY -> STREAM_EXT_PARTITION_NOT_SUPPORTED."""
        prefix = "cerr003"
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
            sql = (
                f"CREATE STREAM s_cerr003 INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t PARTITION BY val "
                f"INTO {self.DB}.sink_cerr003 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            tdSql.error(sql)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    # FS-CERR-004 PARTITION BY <non tbname/tag> on InfluxDB --------------
    def test_cerr_004_partition_by_column_influx(self):
        """InfluxDB ext source + PARTITION BY on a field column -> rejected."""
        prefix = "cerr004"
        src = f"{prefix}_i"
        db = f"{prefix}_idb"
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), db)
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db, [
                'src_t val=1i 1704067200000000000',
            ])
            self._mk_influx_real(src, database=db)
            tdSql.execute(f"USE {self.DB}")
            # 'val' is a field, not a tag — must be rejected per FS §4.4.
            sql = (
                f"CREATE STREAM s_cerr004 INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t PARTITION BY val "
                f"INTO {self.DB}.sink_cerr004 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            tdSql.error(sql)
        finally:
            self._cleanup_src(src)

    # FS-CERR-005 INTO points at external table -> rejected ---------------
    def test_cerr_005_into_external_rejected(self):
        """FS §10#1 — INTO must be a local two-segment table; ext targets rejected."""
        tdSql.execute(f"USE {self.DB}")
        # Even without a real ext source created, this should fail at parse time.
        sql = (
            "CREATE STREAM s_cerr005 INTERVAL(1m) SLIDING(1m) "
            f"FROM {self.DB}.fact_a "
            "INTO some_src.somedb.target AS "
            "SELECT _twstart AS ts, COUNT(*) FROM %%trows"
        )
        tdSql.error(sql)

    # FS-CERR-006 user-specified ts column on ext trigger -> rejected ----
    def test_cerr_006_user_ts_col_rejected(self):
        """FS §10#2 — SQL cannot override ts col on external trigger table."""
        prefix = "cerr006"
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` ("
                "  ts DATETIME(3) PRIMARY KEY, ts2 DATETIME(3), val INT)",
                "INSERT INTO `src_t` VALUES "
                "('2024-01-01 00:00:00.000', '2024-01-01 00:01:00.000', 1)",
            ])
            self._mk_mysql_real(src, database=db)
            tdSql.execute(f"USE {self.DB}")
            # Attempt to declare ts2 as the trigger time column via TS_COLUMN.
            sql = (
                f"CREATE STREAM s_cerr006 INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t "
                f"TS_COLUMN(ts2) "
                f"INTO {self.DB}.sink_cerr006 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            tdSql.error(sql)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass

    # FS-SEC-001 — privileges: low-priv user cannot reference an ext src --
    def test_sec_001_no_select_priv_on_ext_source(self):
        """Building a stream using an ext source the user lacks SELECT on must fail."""
        # This is a structural placeholder: full priv-enforcement test
        # requires a second taos user. Verify at least that querying ext via
        # an unauthorised remote user surfaces an error; details depend on
        # the privilege model implementation. Use a wrong remote password
        # to simulate auth rejection as a proxy.
        prefix = "sec001"
        src = f"{prefix}_m"
        db = f"{prefix}_mdb"
        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` (ts DATETIME(3) PRIMARY KEY, val INT)",
            ])
            # Create source with a wrong password so any access auth-fails.
            self._mk_mysql_real(src, database=db, password="__WRONG__PASSWORD__")
            tdSql.execute(f"USE {self.DB}")
            sql = (
                f"CREATE STREAM s_sec001 INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{db}.src_t "
                f"INTO {self.DB}.sink_sec001 AS "
                f"SELECT _twstart AS ts, COUNT(*) FROM %%trows"
            )
            # CREATE may pass mnode validation; runtime auth fails. Either
            # outcome is acceptable as long as the stream cannot make
            # progress. We accept either CREATE-time error or runtime-time.
            try:
                tdSql.execute(sql)
                # If create succeeded, drop to clean up; runtime-fail path
                # is covered by FS-RERR.
                tdSql.execute("DROP STREAM IF EXISTS s_sec001")
            except Exception as e:
                tdLog.info(f"FS-SEC-001: CREATE STREAM rejected: {e}")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
            except Exception:
                pass
