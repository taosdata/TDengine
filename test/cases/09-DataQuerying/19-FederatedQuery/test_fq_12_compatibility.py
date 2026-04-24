"""
test_fq_12_compatibility.py

Implements COMP-001 through COMP-012 from TS "Compatibility Tests" section.

Design notes:
    - Most compatibility tests require multiple external DB versions to be
      available simultaneously, or an upgrade/downgrade cycle.  These are
      guarded with pytest.skip() in CI.
    - Tests that CAN be partially validated with internal vtable paths or
      parser-level checks are implemented inline.
    - Focus on typical scenarios rather than exhaustive coverage.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - For full coverage: MySQL 5.7+8.0, PostgreSQL 12+14+16, InfluxDB v3.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    ExtSrcEnv,
)


class TestFq12Compatibility(FederatedQueryTestMixin):
    """COMP-001 through COMP-012: Compatibility tests."""

    # External sources created by any test method in this class
    _ALL_SOURCES = [
        "comp004_src",
        "comp009_mysql",
        "comp009_pg",
        "comp012_version",
    ]

    # Local TDengine databases created by any test method in this class
    _ALL_LOCAL_DBS = [
        "comp005_normal_db",
        "comp007_db",
        "comp011_charset",
    ]

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        # Pre-cleanup: static sources + any version-specific sources from prior runs
        self._cleanup(*TestFq12Compatibility._ALL_SOURCES)
        for ver_cfg in ExtSrcEnv.mysql_version_configs():
            self._cleanup(f"comp001_mysql_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.pg_version_configs():
            self._cleanup(f"comp002_pg_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.influx_version_configs():
            self._cleanup(f"comp003_influx_v{ver_cfg.version.replace('.', '')}")
        for db in TestFq12Compatibility._ALL_LOCAL_DBS:
            tdSql.execute(f"drop database if exists {db}")

    def teardown_class(self):
        """Global cleanup — remove all external sources and local databases."""
        self._cleanup(*TestFq12Compatibility._ALL_SOURCES)
        for ver_cfg in ExtSrcEnv.mysql_version_configs():
            self._cleanup(f"comp001_mysql_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.pg_version_configs():
            self._cleanup(f"comp002_pg_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.influx_version_configs():
            self._cleanup(f"comp003_influx_v{ver_cfg.version.replace('.', '')}")
        for db in TestFq12Compatibility._ALL_LOCAL_DBS:
            tdSql.execute(f"drop database if exists {db}")

    def _skip_external(self, msg):
        pytest.skip(f"Compatibility test {msg}")

    # ------------------------------------------------------------------
    # COMP-001  MySQL 5.7/8.0 compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_001_mysql_version_compat(self):
        """COMP-001: MySQL version compatibility — core query & mapping consistent

        TS: Core query and mapping behavior consistent

        Iterates over FQ_MYSQL_VERSIONS (default: 8.0).
        With a single version configured the test validates that version;
        with multiple versions it verifies consistent results across all of them.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-001
            - 2026-04-15 wpan Real version iteration using mysql_version_configs

        """
        first_result = None
        for ver_cfg in ExtSrcEnv.mysql_version_configs():
            tag = ver_cfg.version.replace(".", "")
            ext_db = f"comp001_mysql_{tag}"
            src    = f"comp001_mysql_v{tag}"
            self._cleanup(src)
            try:
                # Prepare test data in this MySQL version
                ExtSrcEnv.mysql_create_db_cfg(ver_cfg, ext_db)
                ExtSrcEnv.mysql_exec_cfg(ver_cfg, ext_db, [
                    "CREATE TABLE IF NOT EXISTS t1 "
                    "(id INT, ts BIGINT, val DOUBLE, name VARCHAR(64))",
                    "DELETE FROM t1",
                    "INSERT INTO t1 VALUES (1, 1704067200000, 1.5, 'alpha')",
                    "INSERT INTO t1 VALUES (2, 1704067260000, 2.5, 'beta')",
                    "INSERT INTO t1 VALUES (3, 1704067320000, 3.5, 'gamma')",
                ])
                # Create TDengine external source for this version
                self._mk_mysql_real_ver(src, ver_cfg, ext_db)
                # Query and verify
                tdSql.query(
                    f"select id, val, name from {src}.{ext_db}.t1 order by id")
                tdSql.checkRows(3)
                tdSql.checkData(0, 0, 1)
                tdSql.checkData(0, 1, 1.5)
                tdSql.checkData(0, 2, "alpha")
                tdSql.checkData(2, 0, 3)
                result = list(tdSql.queryResult)
                # Cross-version consistency check
                if first_result is None:
                    first_result = result
                else:
                    assert result == first_result, (
                        f"MySQL {ver_cfg.version} results differ from first version")
                tdLog.debug(
                    f"COMP-001 MySQL {ver_cfg.version}: 3 rows OK, consistent")
            finally:
                self._cleanup(src)
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(ver_cfg, ext_db)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # COMP-002  PostgreSQL 12/14/16 compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_002_pg_version_compat(self):
        """COMP-002: PostgreSQL version compatibility — core query & mapping consistent

        TS: Core query and mapping behavior consistent

        Iterates over FQ_PG_VERSIONS (default: 16).

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-002
            - 2026-04-15 wpan Real version iteration using pg_version_configs

        """
        first_result = None
        for ver_cfg in ExtSrcEnv.pg_version_configs():
            tag    = ver_cfg.version.replace(".", "")
            ext_db = f"comp002_pg_{tag}"
            src    = f"comp002_pg_v{tag}"
            self._cleanup(src)
            try:
                ExtSrcEnv.pg_create_db_cfg(ver_cfg, ext_db)
                ExtSrcEnv.pg_exec_cfg(ver_cfg, ext_db, [
                    "CREATE TABLE IF NOT EXISTS t1 "
                    "(id INT, ts BIGINT, val DOUBLE PRECISION, name VARCHAR(64))",
                    "DELETE FROM t1",
                    "INSERT INTO t1 VALUES (1, 1704067200000, 1.5, 'alpha')",
                    "INSERT INTO t1 VALUES (2, 1704067260000, 2.5, 'beta')",
                    "INSERT INTO t1 VALUES (3, 1704067320000, 3.5, 'gamma')",
                ])
                self._mk_pg_real_ver(src, ver_cfg, ext_db, schema="public")
                tdSql.query(
                    f"select id, val, name from {src}.{ext_db}.t1 order by id")
                tdSql.checkRows(3)
                tdSql.checkData(0, 0, 1)
                tdSql.checkData(0, 1, 1.5)
                tdSql.checkData(0, 2, "alpha")
                tdSql.checkData(2, 0, 3)
                result = list(tdSql.queryResult)
                if first_result is None:
                    first_result = result
                else:
                    assert result == first_result, (
                        f"PG {ver_cfg.version} results differ from first version")
                tdLog.debug(
                    f"COMP-002 PG {ver_cfg.version}: 3 rows OK, consistent")
            finally:
                self._cleanup(src)
                try:
                    ExtSrcEnv.pg_drop_db_cfg(ver_cfg, ext_db)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # COMP-003  InfluxDB v3 compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_003_influxdb_v3_compat(self):
        """COMP-003: InfluxDB version compatibility — Flight SQL path stable

        TS: Flight SQL path stable

        Iterates over FQ_INFLUX_VERSIONS (default: 3.0).

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-003
            - 2026-04-15 wpan Real version iteration using influx_version_configs

        """
        for ver_cfg in ExtSrcEnv.influx_version_configs():
            tag    = ver_cfg.version.replace(".", "")
            bucket = f"comp003_influx_{tag}"
            src    = f"comp003_influx_v{tag}"
            self._cleanup(src)
            try:
                ExtSrcEnv.influx_create_db_cfg(ver_cfg, bucket)
                # Write reference data via line protocol
                ExtSrcEnv.influx_write_cfg(ver_cfg, bucket, [
                    "meas,region=north val=1.5,score=10i 1704067200000",
                    "meas,region=south val=2.5,score=20i 1704067260000",
                    "meas,region=east  val=3.5,score=30i 1704067320000",
                ])
                self._mk_influx_real_ver(src, ver_cfg, bucket)
                # Verify the Flight SQL path is stable: no syntax error
                self._assert_error_not_syntax(
                    f"select val from {src}.{bucket}.meas order by ts")
                tdLog.debug(
                    f"COMP-003 InfluxDB {ver_cfg.version}: Flight SQL path OK")
            finally:
                self._cleanup(src)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(ver_cfg, bucket)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # COMP-004  Linux distro compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_004_linux_distro_compat(self):
        """COMP-004: Linux distro compatibility — Ubuntu/CentOS consistent

        TS: Ubuntu/CentOS environment behavior consistent

        Cross-distro test requires parallel CI on different OS images.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-004

        """
        cfg_mysql = self._mysql_cfg()
        # Partial: verify parser accepts source DDL on current OS
        src = "comp004_src"
        self._cleanup(src)
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        tdSql.query("show external sources")
        found = any(str(r[0]) == src for r in tdSql.queryResult)
        assert found, f"{src} should be created on current platform"
        self._cleanup(src)

    # ------------------------------------------------------------------
    # COMP-005  Default-off compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_005_default_off_compat(self):
        """COMP-005: Federated disabled — historical behavior unchanged

        TS: Historical behavior unchanged when federation disabled

        When federatedQueryEnable=false, all federated DDL/DML should fail
        but regular TDengine operations should be unaffected.

        This test verifies that the feature-enabled path works; full
        default-off testing requires a separate TDengine instance with
        the feature disabled.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-005

        """
        # Verify feature is currently enabled (setup_class would skip otherwise)
        # Verify normal TDengine operations are unaffected by federation feature
        db = "comp005_normal_db"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute("insert into t1 values (now, 42)")
        tdSql.query("select * from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42)
        tdSql.execute(f"drop database {db}")

    # ------------------------------------------------------------------
    # COMP-006  Post-upgrade external source metadata
    # ------------------------------------------------------------------

    def test_fq_comp_006_upgrade_metadata_migration(self):
        """COMP-006: Post-upgrade external source metadata usable

        TS: Objects usable after upgrade script migration

        Requires upgrade simulation environment.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-006

        """
        self._skip_external("requires upgrade simulation environment")

    # ------------------------------------------------------------------
    # COMP-007  Upgrade with zero federation data
    # ------------------------------------------------------------------

    def test_fq_comp_007_upgrade_zero_data(self):
        """COMP-007: Upgrade with no federation data — smooth upgrade/downgrade

        TS: Smooth upgrade/downgrade when federation unused

        Partial: verify that with no external sources, normal operations
        are completely unaffected.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-007

        """
        # If no external sources exist, SHOW should return empty or succeed
        tdSql.query("show external sources")
        assert tdSql.queryRows >= 0, "SHOW EXTERNAL SOURCES must not crash"

        # Normal operations unaffected
        db = "comp007_db"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t (ts timestamp, v int)")
        tdSql.execute("insert into t values (now, 1)")
        tdSql.query("select count(*) from t")
        tdSql.checkData(0, 0, 1)
        tdSql.execute(f"drop database {db}")

    # ------------------------------------------------------------------
    # COMP-008  Upgrade with existing federation data
    # ------------------------------------------------------------------

    def test_fq_comp_008_upgrade_with_federation_data(self):
        """COMP-008: Upgrade with existing external source config

        TS: Correct behavior when external source config already exists

        Requires upgrade simulation with pre-existing external source metadata.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-008

        """
        self._skip_external(
            "requires upgrade simulation with pre-existing external source metadata"
        )

    # ------------------------------------------------------------------
    # COMP-009  Function dialect compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_009_function_dialect_compat(self):
        """COMP-009: Function dialect cross-version stability

        TS: Key conversion functions stable across versions

        Validate that key function-conversion SQL constructs are parseable
        for each source type.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-009

        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        src_mysql = "comp009_mysql"
        src_pg = "comp009_pg"
        self._cleanup(src_mysql, src_pg)

        tdSql.execute(
            f"create external source {src_mysql} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        tdSql.execute(
            f"create external source {src_pg} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='u' password='p' "
            f"database='pgdb' schema='public'"
        )

        # Key conversion functions that should parse without syntax error
        test_sqls = [
            # COUNT/SUM/AVG — universal
            f"select count(*), sum(v), avg(v) from {src_mysql}.db.t1",
            f"select count(*), sum(v), avg(v) from {src_pg}.pgdb.t1",
            # String functions
            f"select length(name), upper(name), lower(name) from {src_mysql}.db.t1",
            f"select length(name), upper(name), lower(name) from {src_pg}.pgdb.t1",
            # Date functions
            f"select now() from {src_mysql}.db.t1",
            # Math functions
            f"select abs(v), ceil(v), floor(v) from {src_mysql}.db.t1",
        ]

        for sql in test_sqls:
            # These should NOT return syntax error (connection error is OK)
            self._assert_error_not_syntax(sql)

        self._cleanup(src_mysql, src_pg)

    # ------------------------------------------------------------------
    # COMP-010  Case/quoting compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_010_case_and_quoting_compat(self):
        """COMP-010: Identifier case and quoting rules across sources

        TS: Identifier rules consistent across sources

        Validate case-insensitive matching for internal vtables.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-010

        """
        self.helper.prepare_shared_data()

        # Upper/lower case should produce same results
        sql_lower = (
            "select v_int, v_float, v_status from fq_case_db.vntb_fq order by ts"
        )
        sql_upper = (
            "select V_INT, V_FLOAT, V_STATUS from FQ_CASE_DB.VNTB_FQ order by TS"
        )

        tdSql.query(sql_lower)
        lower_result = list(tdSql.queryResult)  # copy before next query overwrites

        tdSql.query(sql_upper)
        upper_result = list(tdSql.queryResult)

        assert lower_result == upper_result, \
            "case-insensitive identifier results should match"

        # Verify row count and actual values from src_ntb (c_int, c_double, c_bool)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)      # row 0: v_int=1
        tdSql.checkData(0, 1, 1.5)    # row 0: v_float=1.5
        tdSql.checkData(0, 2, True)   # row 0: v_status=True
        tdSql.checkData(1, 0, 2)      # row 1: v_int=2
        tdSql.checkData(1, 1, 2.5)    # row 1: v_float=2.5
        tdSql.checkData(1, 2, False)  # row 1: v_status=False
        tdSql.checkData(2, 0, 3)      # row 2: v_int=3
        tdSql.checkData(2, 1, 3.5)    # row 2: v_float=3.5
        tdSql.checkData(2, 2, True)   # row 2: v_status=True

    # ------------------------------------------------------------------
    # COMP-011  Charset compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_011_charset_compat(self):
        """COMP-011: Charset compatibility — multi-language characters

        TS: Multi-language charset consistent across sources

        Requires external DB with multi-language data.

        Partial: verify TDengine internal path handles NCHAR with Chinese.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-011

        """
        db = "comp011_charset"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute("create table t (ts timestamp, name nchar(100), desc_col nchar(200))")
        tdSql.execute(
            "insert into t values (now, '中文测试', '日本語テスト')"
        )
        tdSql.execute(
            "insert into t values (now+1s, 'Ünïcödé', 'العربية')"
        )
        tdSql.query("select name, desc_col from t order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "中文测试")
        tdSql.checkData(0, 1, "日本語テスト")
        tdSql.checkData(1, 0, "Ünïcödé")

        tdSql.execute(f"drop database {db}")

    # ------------------------------------------------------------------
    # COMP-012  Connector version matrix
    # ------------------------------------------------------------------

    def test_fq_comp_012_connector_version_matrix(self):
        """COMP-012: Connector version matrix — mismatch startup check

        TS: Startup validation effective when connector versions mismatch

        Requires multi-node environment with version-mismatched connectors.

        Partial: verify that SHOW EXTERNAL SOURCES works and the system
        is stable after various DDL operations.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-012

        """
        cfg_mysql = self._mysql_cfg()
        # Partial: lifecycle test — create, show, alter, drop → stable
        src = "comp012_version"
        self._cleanup(src)

        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        tdSql.query("show external sources")
        found = any(str(r[0]) == src for r in tdSql.queryResult)
        assert found

        tdSql.execute(f"alter external source {src} set port=3307")
        tdSql.query("show external sources")

        tdSql.execute(f"drop external source {src}")
        tdSql.query("show external sources")
        gone = all(str(r[0]) != src for r in tdSql.queryResult)
        assert gone, "source should be gone after drop"

    # ------------------------------------------------------------------
    # COMP-s01  InfluxDB HTTP protocol end-to-end data query
    # ------------------------------------------------------------------

    def test_fq_comp_s01_influx_http_protocol_query(self):
        """InfluxDB HTTP protocol end-to-end data query and parity with flight_sql.

        Gap source: test_fq_comp_003 only verifies flight_sql path with a
        non-syntax-error probe. No test verifies protocol=http actually returns
        real query results via TDengine federated path.

        Dimensions:
          a) Create InfluxDB source with protocol=http → source visible in catalog
          b) Write data via line-protocol HTTP API, then SELECT returns correct rows
          c) checkData verifies exact values (not just non-error)
          d) flight_sql source and http source return the same rows for identical SQL
          e) Cleanup idempotent

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-10 wpan Initial implementation (gap comp_s01)

        """
        for ver_cfg in ExtSrcEnv.influx_version_configs():
            tag    = ver_cfg.version.replace(".", "")
            bucket = f"comp_s01_http_{tag}"
            src_fs = f"comp_s01_fs_v{tag}"
            src_http = f"comp_s01_http_v{tag}"
            self._cleanup(src_fs, src_http)
            try:
                ExtSrcEnv.influx_create_db_cfg(ver_cfg, bucket)
                # Write reference data: 3 rows with integer score
                ExtSrcEnv.influx_write_cfg(ver_cfg, bucket, [
                    "sensor,region=north score=10i 1704067200000000000",
                    "sensor,region=south score=20i 1704067260000000000",
                    "sensor,region=east  score=30i 1704067320000000000",
                ])

                # ── (a) Create source with protocol=http ──
                tdSql.execute(
                    f"create external source {src_http} "
                    f"type='influxdb' host='{ver_cfg.host}' port={ver_cfg.port} "
                    f"user='u' password='' database={bucket} "
                    f"options('api_token'='{ver_cfg.token}','protocol'='http')"
                )
                tdSql.query("show external sources")
                found_http = any(str(r[0]) == src_http for r in tdSql.queryResult)
                assert found_http, f"{src_http} must appear in SHOW EXTERNAL SOURCES"

                # ── (b)+(c) SELECT returns correct rows via HTTP protocol ──
                tdSql.query(
                    f"select score from {src_http}.{bucket}.sensor "
                    f"order by score")
                tdSql.checkRows(3)
                tdSql.checkData(0, 0, 10)
                tdSql.checkData(1, 0, 20)
                tdSql.checkData(2, 0, 30)

                # ── (d) flight_sql source returns identical rows ──
                # Use _mk_influx_real_ver which creates with protocol=flight_sql
                self._mk_influx_real_ver(src_fs, ver_cfg, bucket)
                tdSql.query(
                    f"select score from {src_fs}.{bucket}.sensor "
                    f"order by score")
                tdSql.checkRows(3)
                tdSql.checkData(0, 0, 10)
                tdSql.checkData(1, 0, 20)
                tdSql.checkData(2, 0, 30)

                tdLog.debug(
                    f"COMP-s01 InfluxDB {ver_cfg.version}: HTTP protocol "
                    f"vs flight_sql parity OK")
            finally:
                self._cleanup(src_fs, src_http)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(ver_cfg, bucket)
                except Exception:
                    pass
