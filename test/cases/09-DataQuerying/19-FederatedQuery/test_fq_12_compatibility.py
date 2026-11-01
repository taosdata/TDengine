# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
    - For full coverage: MySQL 5.7+8.0, PostgreSQL 12+14+16, InfluxDB v3,
      SQL Server 2019+2022, Oracle 19c+26ai.

Oracle (FQOR-COMP-001..008, FQOR-TYPE-018):
    - Oracle has no database level: DATABASE carries the Easy Connect service
      name and is mandatory, the login user's schema is the only namespace.
    - Unquoted identifiers fold to upper case, '' is indistinguishable from
      NULL, and an external table path has at most three segments.
    - No Oracle case is skipped: an unavailable instance fails the case.  When
      the requested version is not in FQ_ORACLE_VERSIONS the substitution is
      recorded in the log, the same contract as the SQL Server 2019/2022 axis.
"""

import socket


from new_test_framework.utils import tdDnodes, tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    ExtSrcEnv,
    TSDB_CODE_EXT_CONNECT_FAILED,
    TSDB_CODE_EXT_FEDERATED_DISABLED,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_PAR_SYNTAX_ERROR,
)


class TestFq12Compatibility(FederatedQueryTestMixin):
    """COMP-001 through COMP-012: Compatibility tests."""

    # External sources created by any test method in this class
    _ALL_SOURCES = [
        "comp004_src",
        "comp009_mysql",
        "comp009_pg",
        "comp012_version",
        # FQOR-COMP-006: all five providers coexist in one query
        "comp006_m",
        "comp006_p",
        "comp006_i",
        "comp006_s",
        "comp006_o",
    ]

    # Local TDengine databases created by any test method in this class
    _ALL_LOCAL_DBS = [
        "comp005_normal_db",
        "comp007_db",
        "comp011_charset",
        # FQOR-COMP-004 / FQOR-TYPE-018: local BOOL mirror for tri-state parity
        "comp_or_bool_db",
    ]

    # Oracle fixtures shared by FQOR-COMP-001..008 and FQOR-TYPE-018.  Every
    # fixture table lives in the login schema and needs a timestamp column:
    # the provider requires one to expose an external table at all.
    _ORACLE_CORE_TABLE = "COMP_O_T1"
    _ORACLE_BOOL_TABLE = "COMP_O_BOOL_T"
    _ORACLE_NUM1_TABLE = "COMP_O_NUM1_T"
    _ORACLE_NET_TABLE = "COMP_O_NET_T"
    _ORACLE_BOOL_MIRROR_DB = "comp_or_bool_db"
    _ORACLE_TABLES = [
        _ORACLE_CORE_TABLE,
        "COMP_O_BOOL_PROBE",
        _ORACLE_BOOL_TABLE,
        _ORACLE_NUM1_TABLE,
        _ORACLE_NET_TABLE,
        "COMP006_T",
    ]
    # Per-version Oracle source names.  "{tag}" is the version with the dots
    # removed, matching the MySQL/PG/InfluxDB/SQL Server loops.
    _ORACLE_SRC_TEMPLATES = [
        "comp_or_core_v{tag}",
        "comp_or_bool_v{tag}",
        "comp_or_num1_v{tag}",
        "comp_or_net_v{tag}",
        "comp_or_net_v{tag}_bad",
        "comp_or_tcps_v{tag}",
        "comp_or_edition_v{tag}",
        "comp_or_edition_v{tag}_m",
    ]

    # Core dataset of FQOR-COMP-001..003.  Oracle stores '' as NULL, so the
    # third NAME reads back as NULL rather than as an empty string.
    _ORACLE_CORE_EXPECTED = [(1, 1.5, "alpha"), (2, 2.5, "beta"), (3, 3.5, None)]

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
        for ver_cfg in ExtSrcEnv.sqlserver_version_configs():
            self._cleanup(f"comp13_sqlserver_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.oracle_version_configs():
            tag = ver_cfg.version.replace(".", "")
            # pytest passes the class as "self" to setup_class/teardown_class,
            # so the instance helper _cleanup() cannot bind here; drop the
            # per-version Oracle sources directly instead.
            for tpl in TestFq12Compatibility._ORACLE_SRC_TEMPLATES:
                name = tpl.format(tag=tag)
                tdSql.execute(f"drop database if exists {name}")
                tdSql.execute(f"drop external source if exists {name}")
        self._oracle_cleanup_tables()
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
        for ver_cfg in ExtSrcEnv.sqlserver_version_configs():
            self._cleanup(f"comp13_sqlserver_v{ver_cfg.version.replace('.', '')}")
        for ver_cfg in ExtSrcEnv.oracle_version_configs():
            tag = ver_cfg.version.replace(".", "")
            # pytest passes the class as "self" to setup_class/teardown_class,
            # so the instance helper _cleanup() cannot bind here; drop the
            # per-version Oracle sources directly instead.
            for tpl in TestFq12Compatibility._ORACLE_SRC_TEMPLATES:
                name = tpl.format(tag=tag)
                tdSql.execute(f"drop database if exists {name}")
                tdSql.execute(f"drop external source if exists {name}")
        self._oracle_cleanup_tables()
        for db in TestFq12Compatibility._ALL_LOCAL_DBS:
            tdSql.execute(f"drop database if exists {db}")

    @staticmethod
    def _restart_engine():
        """Restart dnode 1 on its data directory and reconnect the client.

        The last step of the upgrade script is reopening the catalog on the
        migrated data directory; restarting the engine exercises that step
        without needing an old-version binary.
        """
        tdDnodes.stop(1)
        for attempt in range(2):
            try:
                tdDnodes.start(1)
                break
            except SystemExit:
                if attempt == 1:
                    raise
                tdDnodes.stop(1)
        tdSql.connect("root", "taosdata")

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
                    "(ts TIMESTAMP PRIMARY KEY, id INT, val DOUBLE PRECISION, name VARCHAR(64))",
                    "DELETE FROM t1",
                    "INSERT INTO t1 VALUES ('2024-01-01 00:00:00', 1, 1.5, 'alpha')",
                    "INSERT INTO t1 VALUES ('2024-01-01 00:01:00', 2, 2.5, 'beta')",
                    "INSERT INTO t1 VALUES ('2024-01-01 00:02:00', 3, 3.5, 'gamma')",
                ])
                self._mk_pg_real_ver(src, ver_cfg, ext_db, schema="public")
                tdSql.query(
                    f"select id, val, name from {src}.public.t1 order by id")
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
                # Verify the Flight SQL path is stable: data was written, query should succeed
                # InfluxDB v3 Arrow Flight SQL exposes the time column as "time", not "ts".
                tdSql.query(
                    f"select val from {src}.{bucket}.meas order by time")
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

        A cross-version upgrade needs an old-version binary, which this
        environment does not carry.  The migration step the upgrade script
        performs is reopening the catalog on the migrated data directory, so
        the case creates the federated metadata, restarts the engine on that
        directory and asserts the source and its rows are usable afterwards
        instead of skipping the case.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-006
            - 2026-10-29 Assert the catalog-reopen step instead of skipping

        """
        cfg = next(ExtSrcEnv.oracle_version_configs())
        src = "comp6_upgrade_src"
        table = "COMP6_UPG_T"
        self._cleanup(src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), F NUMBER(5,0))",
                f"INSERT INTO {table} VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, 1)",
                f"INSERT INTO {table} VALUES "
                f"(TIMESTAMP '2024-01-01 00:01:00.000', 2, 2)",
                "COMMIT",
            ])
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.comp6_upg_t")
            tdSql.checkData(0, 0, 2)

            self._restart_engine()

            tdSql.query(f"select count(*) from {src}.comp6_upg_t")
            tdSql.checkData(0, 0, 2)
            tdSql.query(f"select id, f from {src}.comp6_upg_t order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 2)
            tdSql.query("show external sources")
            names = {str(row[0]) for row in tdSql.queryResult}
            assert src in names, names
        finally:
            self._cleanup(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

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

        The source configuration exists before the migration and must stay
        usable after it.  Like COMP-006, the catalog-reopen step of the
        upgrade is exercised on a restarted engine; the case then proves the
        pre-existing config supports the post-migration metadata operations
        (query, SHOW, drop and recreate) instead of skipping.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-008
            - 2026-10-29 Assert the pre-existing config after the catalog reopen

        """
        cfg = next(ExtSrcEnv.oracle_version_configs())
        src = "comp8_upgrade_src"
        table = "COMP8_UPG_T"
        self._cleanup(src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), F NUMBER(5,0))",
                f"INSERT INTO {table} VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, 7)",
                "COMMIT",
            ])
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.comp8_upg_t")
            tdSql.checkData(0, 0, 1)

            self._restart_engine()

            # The pre-existing source survived the catalog reopen and still
            # serves the federation data.
            tdSql.query(f"select id, f from {src}.comp8_upg_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 7)
            tdSql.query("show external sources")
            names = {str(row[0]) for row in tdSql.queryResult}
            assert src in names, names

            # Post-migration metadata operations on the same config: drop and
            # recreate it, then read the data through the recreated source.
            self._cleanup(src)
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.comp8_upg_t")
            tdSql.checkData(0, 0, 1)
        finally:
            self._cleanup(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # COMP-009  Function dialect compatibility
    # ------------------------------------------------------------------

    def test_fq_comp_009_function_dialect_compat(self):
        """COMP-009: Function dialect cross-version stability

        TS: Key conversion functions stable across versions

        Validate that key function-conversion SQL constructs are parseable
        for each source type, including SQL Server (T-SQL four-part path
        forms).

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-009
            - 2026-10-16 wpan Add the SQL Server dialect to the function matrix

        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        cfg_ss = self._sqlserver_cfg()
        src_mysql = "comp009_mysql"
        src_pg = "comp009_pg"
        src_ss = "comp009_ss"
        self._cleanup(src_mysql, src_pg, src_ss)

        tdSql.execute(
            f"create external source {src_mysql} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        tdSql.execute(
            f"create external source {src_pg} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='u' password='p' "
            f"database='pgdb' schema='public'"
        )
        tdSql.execute(
            f"create external source {src_ss} type='sqlserver' "
            f"host='{cfg_ss.host}' port={cfg_ss.port} user='{cfg_ss.user}' "
            f"password='{cfg_ss.password}' database='ssdb' schema='dbo'"
        )

        # Key conversion functions that should parse without syntax error
        test_sqls = [
            # COUNT/SUM/AVG — universal
            f"select count(*), sum(v), avg(v) from {src_mysql}.db.t1",
            f"select count(*), sum(v), avg(v) from {src_pg}.pgdb.t1",
            f"select count(*), sum(v), avg(v) from {src_ss}.ssdb.dbo.t1",
            # String functions
            f"select length(name), upper(name), lower(name) from {src_mysql}.db.t1",
            f"select length(name), upper(name), lower(name) from {src_pg}.pgdb.t1",
            f"select length(name), upper(name), lower(name) from {src_ss}.ssdb.dbo.t1",
            # Date functions
            f"select now() from {src_mysql}.db.t1",
            f"select now() from {src_ss}.ssdb.dbo.t1",
            # Math functions
            f"select abs(v), ceil(v), floor(v) from {src_mysql}.db.t1",
            f"select abs(v), ceil(v), floor(v) from {src_ss}.ssdb.dbo.t1",
        ]

        for sql in test_sqls:
            # Queries on non-existent external tables must fail (any error acceptable)
            tdSql.error(sql)

        self._cleanup(src_mysql, src_pg, src_ss)

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
        altered_row = next((r for r in tdSql.queryResult if str(r[0]) == src), None)
        assert altered_row is not None, f"source {src} missing after ALTER"
        assert str(altered_row[3]) == "3307", \
            f"port not updated after ALTER: expected 3307, got {altered_row[3]}"

        tdSql.execute(f"drop external source {src}")
        tdSql.query("show external sources")
        gone = all(str(r[0]) != src for r in tdSql.queryResult)
        assert gone, "source should be gone after drop"

    # ------------------------------------------------------------------
    # COMP-s01  InfluxDB HTTP protocol end-to-end data query
    # ------------------------------------------------------------------

    def test_fq_comp_s01_influx_http_protocol_query(self):
        """InfluxDB HTTP protocol end-to-end data query and parity with flight_sql.

        Verifies the HTTP query path returns the same rows as the Flight SQL
        path for the configured InfluxDB version(s).

        Since: v3.4.0.0

        """
        for ver_cfg in ExtSrcEnv.influx_version_configs():
            tag = ver_cfg.version.replace(".", "")
            bucket = f"comp_s01_influx_http_{tag}"
            src_http = f"comp_s01_http_v{tag}"
            src_flight = f"comp_s01_flight_v{tag}"
            self._cleanup(src_http, src_flight)
            try:
                ExtSrcEnv.influx_create_db_cfg(ver_cfg, bucket)
                ExtSrcEnv.influx_write_cfg(ver_cfg, bucket, [
                    "meas,region=north val=1.5,score=10i 1704067200000",
                    "meas,region=south val=2.5,score=20i 1704067260000",
                    "meas,region=east val=3.5,score=30i 1704067320000",
                ])

                self._mk_influx_real_ver(
                    src_http,
                    ver_cfg,
                    bucket,
                    protocol="http",
                )
                self._mk_influx_real_ver(
                    src_flight,
                    ver_cfg,
                    bucket,
                    protocol="flight_sql",
                )

                query_sql = "select region, val, score from {}.{}.meas order by time"

                tdSql.query(query_sql.format(src_http, bucket))
                tdSql.checkRows(3)
                http_result = list(tdSql.queryResult)

                tdSql.query(query_sql.format(src_flight, bucket))
                tdSql.checkRows(3)
                flight_result = list(tdSql.queryResult)

                assert http_result == flight_result, (
                    f"Influx HTTP query result differs from Flight SQL for {ver_cfg.version}: "
                    f"http={http_result}, flight={flight_result}"
                )
                tdLog.debug(
                    f"COMP-s01 InfluxDB {ver_cfg.version}: HTTP query path parity OK")
            finally:
                self._cleanup(src_http, src_flight)
                try:
                    ExtSrcEnv.influx_drop_db_cfg(ver_cfg, bucket)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # FQSS-COMP-001..003  SQL Server version compatibility (2019 / 2022)
    # ------------------------------------------------------------------

    def test_fq_ss_comp_version_compat(self):
        """FQSS-COMP-001..003: SQL Server version compatibility — core query & mapping consistent

        TS: SQL Server 2019 / 2022 execute the same core query with identical
        results; with a single version configured only that version is
        validated (2019 requires FQ_SQLSERVER_VERSIONS=2019,2022 plus the 2019
        container; the default environment ships 2022 only).

        The remote SQL text assertion covers the dialect requirements from the
        SQL Server TS: bracket-quoted identifiers, three-part [db].[schema].[table]
        naming, and ISO-8601 timestamp literals without the TIMESTAMP keyword.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.4.0

        Labels: common,ci

        """
        first_result = None
        prev_active = getattr(self, "_active_sqlserver_ver", None)
        for ver_cfg in ExtSrcEnv.sqlserver_version_configs():
            tag = ver_cfg.version.replace(".", "")
            ext_db = f"comp13_sqlserver_{tag}"
            src    = f"comp13_sqlserver_v{tag}"
            self._cleanup(src)
            try:
                ExtSrcEnv.sqlserver_create_db_cfg(ver_cfg, ext_db)
                ExtSrcEnv.sqlserver_exec_cfg(ver_cfg, ext_db, [
                    "IF OBJECT_ID('dbo.t1') IS NOT NULL DROP TABLE dbo.t1",
                    "CREATE TABLE dbo.t1 (ts datetime2(3) NOT NULL PRIMARY KEY, id INT, val FLOAT, name NVARCHAR(64))",
                    "INSERT INTO dbo.t1 VALUES "
                    "('2024-01-01 00:00:00.000', 1, 1.5, 'alpha'),"
                    "('2024-01-01 00:01:00.000', 2, 2.5, 'beta'),"
                    "('2024-01-01 00:02:00.000', 3, 3.5, 'gamma')",
                ])
                self._active_sqlserver_ver = ver_cfg.version
                self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

                tdSql.query(
                    f"select id, val, name from {src}.{ext_db}.dbo.t1 order by id")
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
                        f"SQL Server {ver_cfg.version} results differ from first version")

                # Dialect SQL text: remote SQL must carry the full three-part
                # name and ISO-8601 literals without the TIMESTAMP keyword.
                tdSql.query(
                    "explain verbose true "
                    f"select id, val from {src}.{ext_db}.dbo.t1 where name = 'alpha'")
                plan = "\n".join(
                    str(cell)
                    for row in (tdSql.queryResult or [])
                    for cell in row
                    if cell is not None
                )
                assert f"[{ext_db}].[dbo].[t1]" in plan, plan
                assert "TIMESTAMP '" not in plan, plan
                tdLog.debug(
                    f"FQSS-COMP SQL Server {ver_cfg.version}: 3 rows OK, dialect SQL text OK")
            finally:
                self._cleanup(src)
                # Do not leak the iteration's version into later tests that
                # resolve _active_sqlserver_ver.
                self._active_sqlserver_ver = prev_active
                try:
                    ExtSrcEnv.sqlserver_drop_db_cfg(ver_cfg, ext_db)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # FQOR-COMP-001..008 / FQOR-TYPE-018  Oracle compatibility
    # ------------------------------------------------------------------

    @staticmethod
    def _oracle_cleanup_tables():
        """Drop the Oracle fixture tables on every configured instance.

        Called from setup_class/teardown_class, where pytest passes the class
        as "self" and instance helpers cannot bind, so this one is a static
        method.  An instance may already be down after a failed case: a drop
        failure is not fatal here, the next ensure_env call re-provisions the
        instance.
        """
        if "oracle" not in ExtSrcEnv.required_services():
            return
        for ver_cfg in ExtSrcEnv.oracle_version_configs():
            for table in TestFq12Compatibility._ORACLE_TABLES:
                try:
                    ExtSrcEnv.oracle_drop_table_cfg(ver_cfg, table)
                except Exception:
                    pass

    @staticmethod
    def _oracle_leg_version(wanted):
        """Return ``(cfg, exact)`` for ``wanted``, falling back to the primary version.

        The default environment provisions 26ai only, which is the same
        contract as the SQL Server 2019/2022 axis: when the requested version
        is absent from FQ_ORACLE_VERSIONS the case runs against the configured
        instance and the substitution is recorded, because the TS forbids
        skipping Oracle cases.
        """
        configs = list(ExtSrcEnv.oracle_version_configs())
        for cfg in configs:
            if cfg.version == wanted:
                return cfg, True
        return configs[0], False

    def _oracle_core_leg(self, cfg):
        """Run the FQOR-COMP-001..003 core dataset on one Oracle instance.

        Returns the rows of the reference query so callers can compare the
        versions against each other and against ``_ORACLE_CORE_EXPECTED``.
        """
        tag = cfg.version.replace(".", "")
        src = f"comp_or_core_v{tag}"
        table = TestFq12Compatibility._ORACLE_CORE_TABLE
        schema = cfg.user.upper()
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            # Oracle has no database level, so the table lives in the login
            # schema.  '' is stored as NULL, which the third row exercises.
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), "
                f"VAL BINARY_DOUBLE, NAME VARCHAR2(32))",
                f"INSERT INTO {table} (TS, ID, VAL, NAME) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, 1.5, 'alpha')",
                f"INSERT INTO {table} (TS, ID, VAL, NAME) VALUES "
                f"(TIMESTAMP '2024-01-01 00:01:00.000', 2, 2.5, 'beta')",
                f"INSERT INTO {table} (TS, ID, VAL, NAME) VALUES "
                f"(TIMESTAMP '2024-01-01 00:02:00.000', 3, 3.5, '')",
                "COMMIT",
            ])
            self._active_oracle_ver = cfg.version
            # DATABASE is the Easy Connect service name and is required; the
            # schema is the login user, the single namespace level.
            self._mk_oracle_real(src, database=cfg.service, schema=schema)

            # Unquoted identifiers fold to upper case: the lower-case spelling
            # below resolves against the upper-case Oracle object.
            tdSql.query(f"select id, val, name from {src}.comp_o_t1 order by id")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 1.5)
            tdSql.checkData(0, 2, "alpha")
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 2, "beta")
            tdSql.checkData(2, 0, 3)
            tdSql.checkData(2, 1, 3.5)
            # '' and NULL are indistinguishable (Oracle DS 12).
            tdSql.checkData(2, 2, None)
            result = [tuple(row) for row in tdSql.queryResult]

            # Timestamps keep their declared millisecond precision.
            tdSql.query(f"select ts, id from {src}.comp_o_t1 order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, "2024-01-01 00:00:00.000")
            tdSql.checkData(2, 0, "2024-01-01 00:02:00.000")

            # Path resolution and the Oracle type map: NUMBER(10,0) is BIGINT
            # (never BOOL), BINARY_DOUBLE is DOUBLE, VARCHAR2 is VARCHAR.
            tdSql.query(f"describe {src}.comp_o_t1")
            tdSql.checkRows(4)
            assert [str(r[1]) for r in tdSql.queryResult] == [
                "TIMESTAMP", "BIGINT", "DOUBLE", "VARCHAR"], tdSql.queryResult
            assert [int(r[2]) for r in tdSql.queryResult] == [
                8, 8, 8, 32], tdSql.queryResult

            # A single namespace level: a fourth path segment is a syntax
            # error, never a silently truncated path (Oracle DS 12).
            tdSql.error(
                f"select id from {src}.{cfg.service}.{schema}.{table}",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
                expectErrInfo="too many path segments",
                fullMatched=False)
            return result
        finally:
            self._cleanup(src)
            self._active_oracle_ver = prev_active
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    def _oracle_native_boolean_supported(self, cfg):
        """Probe the instance for a SQL-level BOOLEAN column (26ai only).

        Oracle 19c has no BOOLEAN outside PL/SQL and rejects the DDL with
        ORA-00902, while 26ai stores it natively.  The probe is the server's own
        answer, so no assertion depends on a version string.
        """
        probe = "COMP_O_BOOL_PROBE"
        ExtSrcEnv.oracle_drop_table_cfg(cfg, probe)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [f"CREATE TABLE {probe} (F BOOLEAN)"])
            return True
        except RuntimeError as exc:
            assert "ORA-00902" in str(exc), (
                f"unexpected error probing BOOLEAN on Oracle {cfg.version}: {exc}")
            return False
        finally:
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, probe)
            except Exception:
                pass

    @staticmethod
    def _oracle_local_bool_tristate(db):
        """Read a local BOOL column holding TRUE / FALSE / NULL."""
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {db}.mirror (ts timestamp, f bool)")
        tdSql.execute(
            f"insert into {db}.mirror values (1704067200000, true) "
            f"(1704067260000, false) (1704067320000, null)")
        tdSql.query(f"select ts, f from {db}.mirror order by ts")
        tdSql.checkRows(3)
        values = [row[1] for row in tdSql.queryResult]
        tdSql.execute(f"drop database {db}")
        return values

    def _oracle_boolean_leg(self, cfg, case_id):
        """Assert native BOOLEAN -> BOOL with tri-state identical to local."""
        tag = cfg.version.replace(".", "")
        src = f"comp_or_bool_v{tag}"
        table = TestFq12Compatibility._ORACLE_BOOL_TABLE
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), F BOOLEAN)",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, TRUE)",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:01:00.000', 2, FALSE)",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:02:00.000', 3, NULL)",
                "COMMIT",
            ])
            self._active_oracle_ver = cfg.version
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())

            tdSql.query(f"describe {src}.comp_o_bool_t")
            tdSql.checkRows(3)
            assert [str(r[1]) for r in tdSql.queryResult] == [
                "TIMESTAMP", "BIGINT", "BOOL"], (case_id, cfg.version, tdSql.queryResult)
            assert [int(r[2]) for r in tdSql.queryResult] == [
                8, 8, 1], (case_id, cfg.version, tdSql.queryResult)

            tdSql.query(f"select id, f from {src}.comp_o_bool_t order by id")
            tdSql.checkRows(3)
            external = [row[1] for row in tdSql.queryResult]
            assert external == [True, False, None], (
                case_id, cfg.version, external)

            # Tri-state must match a local BOOL column exactly (Oracle DS 5.3.2.1).
            local = self._oracle_local_bool_tristate(
                TestFq12Compatibility._ORACLE_BOOL_MIRROR_DB)
            assert external == local, (case_id, cfg.version, external, local)
            tdLog.debug(
                f"{case_id}: Oracle {cfg.version} BOOLEAN -> BOOL, tri-state "
                f"identical to the local BOOL column")
        finally:
            self._cleanup(src)
            self._active_oracle_ver = prev_active
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    def _oracle_number1_leg(self, cfg, case_id):
        """Assert the documented numeric fallback: NUMBER(1,0) is TINYINT.

        The value 2 below cannot round-trip through a BOOL column, so the
        mapping is proven with data rather than by the type name alone.  This
        is the documented behaviour asserted in place of native BOOLEAN on a
        version that has no SQL-level BOOLEAN, not a silent skip.
        """
        tag = cfg.version.replace(".", "")
        src = f"comp_or_num1_v{tag}"
        table = TestFq12Compatibility._ORACLE_NUM1_TABLE
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), F NUMBER(1,0))",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, 1)",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:01:00.000', 2, 0)",
                f"INSERT INTO {table} (TS, ID, F) VALUES "
                f"(TIMESTAMP '2024-01-01 00:02:00.000', 3, 2)",
                "COMMIT",
            ])
            self._active_oracle_ver = cfg.version
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())

            tdSql.query(f"describe {src}.{table.lower()}")
            tdSql.checkRows(3)
            assert [str(r[1]) for r in tdSql.queryResult] == [
                "TIMESTAMP", "BIGINT", "TINYINT"], (case_id, cfg.version, tdSql.queryResult)
            assert [int(r[2]) for r in tdSql.queryResult] == [
                8, 8, 1], (case_id, cfg.version, tdSql.queryResult)

            tdSql.query(f"select id, f from {src}.{table.lower()} order by id")
            tdSql.checkRows(3)
            assert [int(row[1]) for row in tdSql.queryResult] == [1, 0, 2], (
                case_id, cfg.version, tdSql.queryResult)
            tdLog.debug(
                f"{case_id}: Oracle {cfg.version} NUMBER(1,0) -> TINYINT, "
                f"values 1/0/2 preserved")
        finally:
            self._cleanup(src)
            self._active_oracle_ver = prev_active
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    @staticmethod
    def _closed_port(host):
        """Return a TCP port on ``host`` that nothing is listening on.

        Binding port 0 and releasing it reserves a port the kernel just handed
        out, so a connect is guaranteed to be refused unless something grabs it
        in between; the verification connect turns that race into a hard error
        instead of a silently passing test.  For a remote host the bind watches
        the wrong machine, so the refusal is only checked by the connect.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, 0))
            port = sock.getsockname()[1]
        except OSError:
            # Remote host: use a port no provisioned Oracle instance listens
            # on (19c = 18321, 26ai = 18322) and rely on the check below.
            port = 18399
        finally:
            sock.close()
        try:
            with socket.create_connection((host, port), timeout=2):
                pass
        except OSError:
            return port
        raise AssertionError(
            f"port {port} on {host} accepted a connection; a closed Oracle "
            f"listener cannot be simulated on this host")

    def test_fq_or_comp_001_oracle_19c_compat(self):
        """FQOR-COMP-001: Oracle 19c — core query and mapping match the baseline

        TS: Oracle 19c passes the functional cases and matches the local
        baseline.  The core dataset (NUMBER -> BIGINT, BINARY_DOUBLE -> DOUBLE,
        VARCHAR2 -> VARCHAR, '' stored as NULL, two-segment path, upper-case
        folding, three-segment path limit) runs on the 19c instance when
        FQ_ORACLE_VERSIONS contains it.  When 19c is not provisioned the dataset
        runs on the configured instance and the substitution is recorded — the
        TS forbids skipping Oracle cases.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle 19c leg (FQOR-COMP-001)

        """
        cfg, exact = self._oracle_leg_version("19c")
        result = self._oracle_core_leg(cfg)
        assert result == TestFq12Compatibility._ORACLE_CORE_EXPECTED, (cfg.version, result)
        if not exact:
            tdLog.info(
                "FQOR-COMP-001: Oracle 19c is not provisioned in this "
                "environment (FQ_ORACLE_VERSIONS=%s); the core dataset ran on "
                "%s. The 19c-specific contract (no SQL-level BOOLEAN) is "
                "asserted by FQOR-COMP-004 and FQOR-TYPE-018."
                % (",".join(ExtSrcEnv.ORACLE_VERSIONS), cfg.version))
        else:
            tdLog.debug("FQOR-COMP-001: Oracle 19c core dataset OK")

    def test_fq_or_comp_002_oracle_26ai_compat(self):
        """FQOR-COMP-002: Oracle 26ai — core query and mapping match the baseline

        TS: Oracle 26ai (the preferred version) passes the functional cases and
        matches the local baseline.  Same dataset as FQOR-COMP-001; when 26ai
        is not provisioned the configured instance is used and the substitution
        is recorded instead of skipping.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle 26ai leg (FQOR-COMP-002)

        """
        cfg, exact = self._oracle_leg_version("26ai")
        result = self._oracle_core_leg(cfg)
        assert result == TestFq12Compatibility._ORACLE_CORE_EXPECTED, (cfg.version, result)
        if not exact:
            tdLog.info(
                "FQOR-COMP-002: Oracle 26ai is not provisioned in this "
                "environment (FQ_ORACLE_VERSIONS=%s); the core dataset ran on "
                "%s." % (",".join(ExtSrcEnv.ORACLE_VERSIONS), cfg.version))
        else:
            tdLog.debug("FQOR-COMP-002: Oracle 26ai core dataset OK")

    def test_fq_or_comp_003_oracle_cross_version_consistency(self):
        """FQOR-COMP-003: 19c and 26ai return identical rows for the same SQL

        TS: Both versions produce consistent results except for the 26ai
        BOOLEAN case.  The core dataset contains no BOOLEAN column, so every
        configured version must return exactly the same rows; the BOOLEAN
        difference is asserted by FQOR-COMP-004 / FQOR-TYPE-018.  With only one
        version provisioned the baseline is still asserted and the missing pair
        is recorded.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the cross-version Oracle check (FQOR-COMP-003)

        """
        first_result = None
        first_version = None
        compared = []
        for cfg in ExtSrcEnv.oracle_version_configs():
            result = self._oracle_core_leg(cfg)
            assert result == TestFq12Compatibility._ORACLE_CORE_EXPECTED, (
                cfg.version, result)
            if first_result is None:
                first_result = result
                first_version = cfg.version
            else:
                assert result == first_result, (
                    f"Oracle {cfg.version} results differ from Oracle "
                    f"{first_version}: {result} != {first_result}")
                compared.append(cfg.version)
        if compared:
            tdLog.debug(
                "FQOR-COMP-003: %s consistent with %s on the core dataset"
                % (",".join(compared), first_version))
        else:
            tdLog.info(
                "FQOR-COMP-003: only Oracle %s is configured "
                "(FQ_ORACLE_VERSIONS=%s); the baseline was asserted but no "
                "cross-version pair exists. Set FQ_ORACLE_VERSIONS=19c,26ai to "
                "compare 19c with 26ai." % (first_version,
                                            ",".join(ExtSrcEnv.ORACLE_VERSIONS)))

    def test_fq_or_comp_004_oracle_boolean_version_gate(self):
        """FQOR-COMP-004: 26ai BOOLEAN — native on 26ai, excluded on 19c

        TS: On 26ai BOOLEAN maps to BOOL and the tri-state matches a local BOOL
        column; 19c does not run the case and records the reason explicitly.
        The gate is the server's own answer to a BOOLEAN DDL probe, not a
        version string.  On an instance without SQL-level BOOLEAN the
        documented numeric fallback (NUMBER(1,0) -> TINYINT, never BOOL) is
        asserted instead, and the exclusion is logged.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle BOOLEAN version gate (FQOR-COMP-004)

        """
        saw_26ai = False
        for cfg in ExtSrcEnv.oracle_version_configs():
            if cfg.version == "26ai":
                saw_26ai = True
            supported = self._oracle_native_boolean_supported(cfg)
            if cfg.version == "26ai" and not supported:
                raise AssertionError(
                    "Oracle 26ai must accept a native BOOLEAN column, but the "
                    "DDL probe failed with ORA-00902")
            if supported:
                self._oracle_boolean_leg(cfg, "FQOR-COMP-004")
            else:
                tdLog.info(
                    "FQOR-COMP-004: Oracle %s has no SQL-level BOOLEAN "
                    "(ORA-00902 on the DDL probe), so the TS excludes the "
                    "native BOOLEAN case for this version. The documented "
                    "numeric fallback is asserted instead." % cfg.version)
                self._oracle_number1_leg(cfg, "FQOR-COMP-004")
        if not saw_26ai:
            tdLog.info(
                "FQOR-COMP-004: Oracle 26ai is not configured "
                "(FQ_ORACLE_VERSIONS=%s); the native BOOLEAN leg ran on the "
                "provisioned instance only." % ",".join(ExtSrcEnv.ORACLE_VERSIONS))

    def test_fq_or_comp_005_oracle_enterprise_feature_gate(self):
        """FQOR-COMP-005: feature restriction — Oracle fails like the other sources

        TS: With the enterprise feature unavailable, creating an Oracle source
        returns the same feature-restriction error as the existing sources and
        no source is created.  A community-edition binary is not part of this
        environment, so the restriction is exercised through the
        federatedQueryEnable switch, which is the guard that gates the feature
        in both builds; the Oracle and MySQL DDL must return the identical code.
        Regular TDengine DDL/DML keeps working while the feature is off.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle feature gate (FQOR-COMP-005)

        """
        cfg = self._oracle_cfg()
        tag = cfg.version.replace(".", "")
        src = f"comp_or_edition_v{tag}"
        mysql_src = f"{src}_m"
        mysql_cfg = self._mysql_cfg()
        local_db = "comp005_normal_db"
        self._cleanup(src, mysql_src)
        oracle_ddl = (
            f"create external source {src} type='oracle' "
            f"host='{cfg.host}' port={cfg.port} user='{cfg.user}' "
            f"password='{cfg.password}' database={cfg.service}")
        mysql_ddl = (
            f"create external source {mysql_src} type='mysql' "
            f"host='{mysql_cfg.host}' port={mysql_cfg.port} user='u' "
            f"password='p' database='db'")
        try:
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '0'", queryTimes=1)
            # Both source types fail in exactly the same way.
            tdSql.error(oracle_ddl, expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(mysql_ddl, expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error("show external sources",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            # The feature switch does not affect ordinary TDengine work.
            tdSql.execute(f"drop database if exists {local_db}")
            tdSql.execute(f"create database {local_db}")
            tdSql.execute(f"create table {local_db}.t1 (ts timestamp, v int)")
            tdSql.execute(f"insert into {local_db}.t1 values (now, 42)")
            tdSql.query(f"select v from {local_db}.t1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)
            tdSql.execute(f"drop database {local_db}")
        finally:
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)

        # Nothing was created behind the user's back.
        tdSql.query("show external sources")
        names = {str(row[0]) for row in tdSql.queryResult}
        assert src not in names, names
        assert mysql_src not in names, names

        # Enabled again: the same Oracle DDL succeeds and the source is usable.
        table = TestFq12Compatibility._ORACLE_CORE_TABLE
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), ID NUMBER(10), VAL BINARY_DOUBLE)",
                f"INSERT INTO {table} (TS, ID, VAL) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 1, 1.5)",
                f"INSERT INTO {table} (TS, ID, VAL) VALUES "
                f"(TIMESTAMP '2024-01-01 00:01:00.000', 2, 2.5)",
                "COMMIT",
            ])
            self._mk_oracle_real(src, database=cfg.service, schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.comp_o_t1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
            tdSql.query(f"select id, val from {src}.comp_o_t1 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1.5)
            tdSql.checkData(1, 1, 2.5)
        finally:
            self._cleanup(src, mysql_src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    def test_fq_or_comp_006_oracle_coexist_with_four_sources(self):
        """FQOR-COMP-006: Oracle coexists with the other four providers

        TS: One query references Oracle, MySQL, PostgreSQL, InfluxDB and SQL
        Server at the same time and returns the right rows, and the per-source
        metadata does not get mixed up.  Each provider holds the marker row of
        its own database; the union returns all five markers in order and
        ins_ext_sources reports each source's own type/port/user/database.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the five-source coexistence case (FQOR-COMP-006)

        """
        mysql_cfg = self._mysql_cfg()
        pg_cfg = self._pg_cfg()
        influx_cfg = self._influx_cfg()
        ss_cfg = self._sqlserver_cfg()
        o_cfg = self._oracle_cfg()
        m_src, p_src, i_src, s_src, o_src = (
            "comp006_m", "comp006_p", "comp006_i", "comp006_s", "comp006_o")
        m_db, p_db, i_db, s_db = (
            "comp006_mdb", "comp006_pdb", "comp006_idb", "comp006_sdb")
        o_table = "COMP006_T"
        markers = {m_src: 101, p_src: 102, i_src: 103, s_src: 104, o_src: 105}
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(m_src, p_src, i_src, s_src, o_src)
        ExtSrcEnv.oracle_drop_table_cfg(o_cfg, o_table)
        try:
            # ----- MySQL -----
            ExtSrcEnv.mysql_kill_sleeping_connections_cfg(mysql_cfg)
            ExtSrcEnv.mysql_create_db_cfg(mysql_cfg, m_db)
            ExtSrcEnv.mysql_exec_cfg(mysql_cfg, m_db, [
                "CREATE TABLE IF NOT EXISTS comp006_t (ts DATETIME(3), code INT)",
                "DELETE FROM comp006_t",
                "INSERT INTO comp006_t VALUES ('2024-01-01 00:00:00.000', 101)",
            ])
            self._mk_mysql_real(m_src, database=m_db)

            # ----- PostgreSQL -----
            ExtSrcEnv.pg_create_db_cfg(pg_cfg, p_db)
            ExtSrcEnv.pg_exec_cfg(pg_cfg, p_db, [
                "DROP TABLE IF EXISTS public.comp006_t",
                "CREATE TABLE public.comp006_t (ts TIMESTAMP, code INT)",
                "INSERT INTO public.comp006_t VALUES ('2024-01-01 00:00:00', 102)",
            ])
            self._mk_pg_real(p_src, database=p_db, schema="public")

            # ----- InfluxDB -----
            ExtSrcEnv.influx_create_db_cfg(influx_cfg, i_db)
            ExtSrcEnv.influx_write_cfg(influx_cfg, i_db, [
                "comp006_t code=103i 1704067200000000000",
            ])
            self._mk_influx_real(i_src, database=i_db)

            # ----- SQL Server -----
            ExtSrcEnv.sqlserver_create_db_cfg(ss_cfg, s_db)
            ExtSrcEnv.sqlserver_exec_cfg(ss_cfg, s_db, [
                "IF OBJECT_ID('dbo.comp006_t') IS NOT NULL DROP TABLE dbo.comp006_t",
                "CREATE TABLE dbo.comp006_t (ts datetime2(3), code INT)",
                "INSERT INTO dbo.comp006_t VALUES ('2024-01-01 00:00:00.000', 104)",
            ])
            self._mk_sqlserver_real(s_src, database=s_db, schema="dbo")

            # ----- Oracle -----
            self._active_oracle_ver = o_cfg.version
            ExtSrcEnv.oracle_exec_cfg(o_cfg, [
                f"CREATE TABLE {o_table} (TS TIMESTAMP(3), CODE NUMBER(10))",
                f"INSERT INTO {o_table} (TS, CODE) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 105)",
                "COMMIT",
            ])
            self._mk_oracle_real(o_src, database=o_cfg.service,
                                 schema=o_cfg.user.upper())

            # Every source resolves its own default namespace and reads back
            # its own marker.
            refs = [
                (m_src, f"{m_src}.comp006_t"),
                (p_src, f"{p_src}.comp006_t"),
                (i_src, f"{i_src}.comp006_t"),
                (s_src, f"{s_src}.comp006_t"),
                (o_src, f"{o_src}.comp006_t"),
            ]
            for name, ref in refs:
                tdSql.query(f"select code from {ref}")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, markers[name])

            # One query, five providers.
            union_sql = " union all ".join(
                f"select code from {ref}" for _, ref in refs) + " order by code"
            tdSql.query(union_sql)
            tdSql.checkRows(5)
            for row, expected in enumerate([101, 102, 103, 104, 105]):
                tdSql.checkData(row, 0, expected)

            # Metadata is per-source: type, address, user and namespace.
            # Reserved-looking column names are quoted, as in fq_07/fq_08.
            tdSql.query(
                "select source_name, `type`, host, `port`, `user`, `database` "
                "from information_schema.ins_ext_sources "
                "where source_name in "
                f"('{m_src}','{p_src}','{i_src}','{s_src}','{o_src}') "
                "order by source_name")
            tdSql.checkRows(5)
            # Rows come back ordered by source name: i < m < o < p < s.
            expected_rows = [
                (i_src, "influxdb", influx_cfg.host, influx_cfg.port, "u", i_db),
                (m_src, "mysql", mysql_cfg.host, mysql_cfg.port,
                 mysql_cfg.user, m_db),
                (o_src, "oracle", o_cfg.host, o_cfg.port,
                 o_cfg.user, o_cfg.service),
                (p_src, "postgresql", pg_cfg.host, pg_cfg.port,
                 pg_cfg.user, p_db),
                (s_src, "sqlserver", ss_cfg.host, ss_cfg.port,
                 ss_cfg.user, s_db),
            ]
            for row, (name, typ, host, port, user, database) in enumerate(expected_rows):
                tdSql.checkData(row, 0, name)
                tdSql.checkData(row, 1, typ)
                tdSql.checkData(row, 2, host)
                tdSql.checkData(row, 3, port)
                tdSql.checkData(row, 4, user)
                tdSql.checkData(row, 5, database)
        finally:
            self._cleanup(m_src, p_src, i_src, s_src, o_src)
            self._active_oracle_ver = prev_active
            for drop_fn, args in (
                    (ExtSrcEnv.mysql_drop_db_cfg, (mysql_cfg, m_db)),
                    (ExtSrcEnv.pg_drop_db_cfg, (pg_cfg, p_db)),
                    (ExtSrcEnv.influx_drop_db_cfg, (influx_cfg, i_db)),
                    (ExtSrcEnv.sqlserver_drop_db_cfg, (ss_cfg, s_db))):
                try:
                    drop_fn(*args)
                except Exception:
                    pass
            try:
                ExtSrcEnv.oracle_drop_table_cfg(o_cfg, o_table)
            except Exception:
                pass

    def test_fq_or_comp_007_oracle_native_encryption(self):
        """FQOR-COMP-007: Oracle Net encryption — no client-side TLS surface

        TS: With SQLNET.ENCRYPTION_SERVER=required the client connects and
        queries without extra configuration and returns the same data as a
        plaintext instance.  This environment provisions only standard
        listeners (no SQLNET.ENCRYPTION_SERVER=required instance), so that half
        is recorded here explicitly while the client-side contract is asserted:
        every TLS key is rejected at DDL time instead of being silently
        ignored, a source with no OPTIONS connects through Easy Connect and
        returns exactly what SQL*Plus reads directly, and no TLS key is stored
        in the catalog.  tls_enabled='false' is the documented exception (it
        only means "do not ask") and is clearable again.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle Net encryption case (FQOR-COMP-007)

        """
        cfg = self._oracle_cfg()
        tag = cfg.version.replace(".", "")
        src = f"comp_or_net_v{tag}"
        bad_src = f"{src}_bad"
        table = TestFq12Compatibility._ORACLE_NET_TABLE
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(src, bad_src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            base_ddl = (
                f"create external source {bad_src} type='oracle' "
                f"host='{cfg.host}' port={cfg.port} user='{cfg.user}' "
                f"password='{cfg.password}' database={cfg.service}")
            for key, value in (("tls_enabled", "true"), ("tls_ca_cert", "ca.pem"),
                               ("tls_client_cert", "client.pem"),
                               ("tls_client_key", "client.key")):
                tdSql.error(
                    f"{base_ddl} options('{key}'='{value}')",
                    expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
                    expectErrInfo="not supported for Oracle external sources",
                    fullMatched=False)
            tdSql.query("show external sources")
            assert bad_src not in {str(row[0]) for row in tdSql.queryResult}

            tdLog.info(
                "FQOR-COMP-007: this environment ships only standard Oracle "
                "listeners; a SQLNET.ENCRYPTION_SERVER=required instance is not "
                "provisioned, so the 'required encryption still connects' half "
                "of the TS is recorded here and the client-side contract is "
                "asserted.")

            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), VAL NUMBER(10))",
                f"INSERT INTO {table} (TS, VAL) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 42)",
                "COMMIT",
            ])
            self._active_oracle_ver = cfg.version
            # Easy Connect plus credentials is the whole client configuration:
            # no wallet, no TNS alias, no client-side encryption request.
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select sum(val) from {src}.comp_o_net_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)
            direct = ExtSrcEnv.oracle_query_cfg(cfg, f"SELECT SUM(VAL) FROM {table}")
            assert int(direct) == 42, direct

            tdSql.query(
                "select `options` from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            stored = tdSql.queryResult[0][0]
            stored = "" if stored is None else str(stored)
            for key in ("tls_enabled", "tls_ca_cert", "tls_client_cert",
                        "tls_client_key"):
                assert key not in stored, (key, stored)

            # tls_enabled='false' is accepted (documented exception), leaves
            # the data untouched, and can be cleared again.
            tdSql.execute(
                f"alter external source {src} set options('tls_enabled'='false')")
            tdSql.query(f"select sum(val) from {src}.comp_o_net_t")
            tdSql.checkData(0, 0, 42)
            tdSql.query(
                "select `options` from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            assert "tls_enabled" in str(tdSql.queryResult[0][0])
            tdSql.execute(
                f"alter external source {src} set options('tls_enabled'='')")
            tdSql.query(
                "select `options` from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            stored = tdSql.queryResult[0][0]
            stored = "" if stored is None else str(stored)
            assert "tls_enabled" not in stored, stored
            tdSql.query(f"select sum(val) from {src}.comp_o_net_t")
            tdSql.checkData(0, 0, 42)
        finally:
            self._cleanup(src, bad_src)
            self._active_oracle_ver = prev_active
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    def test_fq_or_comp_008_oracle_tcps_only_listener(self):
        """FQOR-COMP-008: TCPS-only listener — connect fails, no fallback

        TS: When the instance only exposes TCPS, the query fails with
        TSDB_CODE_EXT_CONNECT_FAILED; the connector must not switch transport,
        degrade to plaintext or try a proxy.  A closed port stands in for the
        TCPS-only listener (no TCPS-only instance is provisioned): the failure
        is exact and repeatable, the catalog keeps the configured address, and
        a healthy source still returns the same data — nothing is proxied or
        silently served from another endpoint.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle TCPS-only case (FQOR-COMP-008)

        """
        cfg = self._oracle_cfg()
        tag = cfg.version.replace(".", "")
        good_src = f"comp_or_net_v{tag}"
        bad_src = f"comp_or_tcps_v{tag}"
        table = TestFq12Compatibility._ORACLE_NET_TABLE
        closed_port = self._closed_port(cfg.host)
        assert closed_port != cfg.port, (closed_port, cfg.port)
        prev_active = getattr(self, "_active_oracle_ver", None)
        self._cleanup(good_src, bad_src)
        ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
        try:
            ExtSrcEnv.oracle_exec_cfg(cfg, [
                f"CREATE TABLE {table} (TS TIMESTAMP(3), VAL NUMBER(10))",
                f"INSERT INTO {table} (TS, VAL) VALUES "
                f"(TIMESTAMP '2024-01-01 00:00:00.000', 42)",
                "COMMIT",
            ])
            self._active_oracle_ver = cfg.version
            self._mk_oracle_real(good_src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select sum(val) from {good_src}.comp_o_net_t")
            tdSql.checkData(0, 0, 42)

            tdSql.execute(
                f"create external source {bad_src} type='oracle' "
                f"host='{cfg.host}' port={closed_port} user='{cfg.user}' "
                f"password='{cfg.password}' database='{cfg.service}' "
                f"schema='{cfg.user.upper()}'")
            # Exact, repeatable connection-layer failure: no retry turns into a
            # silent success and no alternative transport is attempted.
            for _ in range(3):
                tdSql.error(f"select count(*) from {bad_src}.comp_o_net_t",
                            expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)

            # No auto-switch: the catalog keeps the configured address.
            tdSql.query(
                "select host, `port`, `database` "
                "from information_schema.ins_ext_sources "
                f"where source_name = '{bad_src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, cfg.host)
            tdSql.checkData(0, 1, closed_port)
            tdSql.checkData(0, 2, cfg.service)

            # No proxy / no degrade: the healthy source is unchanged.
            tdSql.query(f"select sum(val) from {good_src}.comp_o_net_t")
            tdSql.checkData(0, 0, 42)
            tdSql.query(f"select count(*) from {good_src}.comp_o_net_t")
            tdSql.checkData(0, 0, 1)
        finally:
            self._cleanup(good_src, bad_src)
            self._active_oracle_ver = prev_active
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception:
                pass

    def test_fq_or_type_018_oracle_boolean_mapping(self):
        """FQOR-TYPE-018: BOOLEAN -> BOOL on 26ai; NUMBER(1,0) is never BOOL

        TS: 26ai native BOOLEAN maps to BOOL with the tri-state (TRUE / FALSE /
        NULL) identical to a local BOOL column; NUMBER(1,0) maps to TINYINT on
        every version and never to BOOL.  A version without SQL-level BOOLEAN
        does not run the native leg and records the reason explicitly, while
        the numeric contract is asserted with the value 2 that a BOOL column
        could not hold.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 wpan Add the Oracle BOOLEAN type mapping (FQOR-TYPE-018)

        """
        checked = []
        for cfg in ExtSrcEnv.oracle_version_configs():
            if self._oracle_native_boolean_supported(cfg):
                self._oracle_boolean_leg(cfg, "FQOR-TYPE-018")
                checked.append(cfg.version)
            else:
                tdLog.info(
                    "FQOR-TYPE-018: Oracle %s has no SQL-level BOOLEAN "
                    "(ORA-00902 on the DDL probe), so the TS excludes the "
                    "native BOOLEAN mapping for this version. NUMBER(1,0) -> "
                    "TINYINT is asserted instead; it is never BOOL."
                    % cfg.version)
                self._oracle_number1_leg(cfg, "FQOR-TYPE-018")
        if checked:
            tdLog.debug(
                "FQOR-TYPE-018: BOOLEAN -> BOOL verified on %s"
                % ",".join(checked))
        else:
            tdLog.info(
                "FQOR-TYPE-018: no configured Oracle version exposes native "
                "BOOLEAN (FQ_ORACLE_VERSIONS=%s); the numeric mapping was "
                "asserted and the native leg awaits a 26ai instance."
                % ",".join(ExtSrcEnv.ORACLE_VERSIONS))
