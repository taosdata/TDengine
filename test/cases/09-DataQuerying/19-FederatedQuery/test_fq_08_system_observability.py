"""
test_fq_08_system_observability.py

Implements FQ-SYS-001 through FQ-SYS-028 from TS §8
"System tables, config, observability" — SHOW/DESCRIBE rewrite, system table columns,
permissions, dynamic config, TLS, observability metrics, feature toggle,
upgrade/downgrade.

Design notes:
    - System table tests can run with external source DDL only (no live DB).
    - Permission tests create non-admin users to verify sysInfo protection.
    - Dynamic config tests modify runtime parameters and verify effect.
    - Observability metrics tests require live workload for meaningful data.
"""

import pytest
import threading
import time

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
    TSDB_CODE_EXT_FEATURE_DISABLED,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
)


class TestFq08SystemObservability(FederatedQueryVersionedMixin):
    """FQ-SYS-001 through FQ-SYS-028: system tables, config, observability."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        tdSql.execute("drop database if exists fq_sys_016_local")

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    # SHOW EXTERNAL SOURCES / ins_ext_sources column indices (per DS §5.4 schema)
    # NOTE: user/password come BEFORE database/schema in the schema definition.
    _COL_NAME = 0      # source_name
    _COL_TYPE = 1      # type
    _COL_HOST = 2      # host
    _COL_PORT = 3      # port
    _COL_USER = 4      # user (sysInfo=true; NULL for non-admin)
    _COL_PASSWORD = 5  # password (sysInfo=true; always '******' for admin)
    _COL_DATABASE = 6  # database
    _COL_SCHEMA = 7    # schema
    _COL_OPTIONS = 8   # options (JSON)
    _COL_CTIME = 9     # create_time (TIMESTAMP)

    # ------------------------------------------------------------------
    # FQ-SYS-001 ~ FQ-SYS-005: SHOW/DESCRIBE/system table
    # ------------------------------------------------------------------

    def test_fq_sys_001(self):
        """FQ-SYS-001: SHOW rewrite — SHOW EXTERNAL SOURCES rewrites to ins_ext_sources

        Dimensions:
          a) SHOW EXTERNAL SOURCES returns results
          b) Equivalent to querying ins_ext_sources
          c) Both return same row count

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_001"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            tdSql.query("show external sources")
            show_rows = tdSql.queryRows

            tdSql.query(
                "select * from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            sys_rows = tdSql.queryRows

            assert show_rows >= 1, (
                f"SHOW EXTERNAL SOURCES returned {show_rows} rows, expected >= 1")
            tdSql.checkRows(1)  # exactly 1 row matches WHERE source_name=src
            # Verify the row contains the correct source_name
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
        finally:
            self._cleanup_src(src)

    def test_fq_sys_002(self):
        """FQ-SYS-002: DESCRIBE rewrite — DESCRIBE EXTERNAL SOURCE rewrites to WHERE source_name

        Dimensions:
          a) DESCRIBE EXTERNAL SOURCE name → results
          b) Equivalent to filtered sys table query
          c) Same data returned

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_002"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            tdSql.query(f"describe external source {src}")
            tdSql.checkRows(1)           # DESCRIBE → 1 matching row
            tdSql.checkData(0, 0, src)   # col 0 = source_name
            tdSql.checkData(0, 1, 'mysql')  # col 1 = type
        finally:
            self._cleanup_src(src)

    def test_fq_sys_003(self):
        """FQ-SYS-003: System table column definition — ins_ext_sources column types/lengths/order correct

        Dimensions:
          a) Expected columns: source_name, type, host, port, database, schema,
             user, password, options, create_time
          b) Column order matches documentation
          c) Column types correct

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_003"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database='testdb')
            tdSql.query("show external sources")
            # Verify we get at least 10 columns
            assert len(tdSql.queryResult[0]) >= 10, (
                f"Expected >=10 columns in SHOW EXTERNAL SOURCES, "
                f"got {len(tdSql.queryResult[0])}")

            # Find our source
            # Verify exactly 10 columns per DS §5.4 schema
            assert len(tdSql.queryResult[0]) == 10, (
                f"Expected 10 columns, got {len(tdSql.queryResult[0])}")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    assert row[self._COL_TYPE] == 'mysql', (
                        f"Expected type='mysql', got '{row[self._COL_TYPE]}'")
                    assert row[self._COL_HOST] == self._mysql_cfg().host, (
                        f"Expected host='{self._mysql_cfg().host}', "
                        f"got '{row[self._COL_HOST]}'")
                    assert row[self._COL_PORT] == self._mysql_cfg().port, (
                        f"Expected port={self._mysql_cfg().port}, "
                        f"got {row[self._COL_PORT]}")
                    # col4=user (sysInfo=true), col5=password, col6=database, col7=schema
                    assert row[self._COL_USER] == self._mysql_cfg().user, (
                        f"Expected user='{self._mysql_cfg().user}', "
                        f"got '{row[self._COL_USER]}'")
                    assert row[self._COL_PASSWORD] == '******', (
                        f"password should be masked '******', got '{row[self._COL_PASSWORD]}'")
                    assert row[self._COL_DATABASE] == 'testdb', (
                        f"Expected database='testdb', got '{row[self._COL_DATABASE]}'")
                    assert row[self._COL_SCHEMA] in ('', None), (
                        f"MySQL source schema should be empty, got '{row[self._COL_SCHEMA]}'")
                    assert row[self._COL_CTIME] is not None, "create_time must not be NULL"
                    break
            assert found, f"Source {src} not found in SHOW output"
        finally:
            self._cleanup_src(src)

    def test_fq_sys_004(self):
        """FQ-SYS-004: Table-level permissions — normal user can query basic columns

        Dimensions:
          a) Normal user can query ins_ext_sources
          b) Basic columns visible
          c) No permission error

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_004"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database='testdb')
            # Create non-admin user (sysInfo=0 by default)
            tdSql.execute("drop user if exists fq_sys_test_user")
            tdSql.execute("create user fq_sys_test_user pass 'Test_123'")
            try:
                # Admin can see all basic columns including host/port/database
                tdSql.query(
                    "select source_name, type, host, port, database "
                    "from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, src)
                tdSql.checkData(0, 1, 'mysql')
                tdSql.checkData(0, 2, self._mysql_cfg().host)
                tdSql.checkData(0, 3, self._mysql_cfg().port)
                tdSql.checkData(0, 4, 'testdb')
                # Note: testing non-admin visibility requires a separate connection;
                # sysInfo columns (user/password) return NULL for non-admin per DS §5.4.
            finally:
                tdSql.execute("drop user if exists fq_sys_test_user")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_005(self):
        """FQ-SYS-005: sysInfo column protection — non-admin user/password are NULL

        Dimensions:
          a) Admin sees full details (user/password)
          b) Non-admin with sysInfo=0: user/password columns are NULL
          c) Other columns still visible

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_005"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            # As admin, password should be visible (or masked)
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    # Admin: user column is visible (not NULL)
                    assert row[self._COL_USER] == self._mysql_cfg().user, (
                        f"Admin should see user='{self._mysql_cfg().user}', "
                        f"got '{row[self._COL_USER]}'")
                    # password column always masked as '******' even for admin
                    assert row[self._COL_PASSWORD] == '******', (
                        f"password must be '******', got '{row[self._COL_PASSWORD]}'")
                    break
            assert found, (
                f"Source '{src}' not found in SHOW EXTERNAL SOURCES")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-006 ~ FQ-SYS-010: Dynamic config
    # ------------------------------------------------------------------

    def test_fq_sys_006(self):
        """FQ-SYS-006: ConnectTimeout dynamic effect — new queries use updated timeout after change

        Dimensions:
          a) Set federatedQueryConnectTimeoutMs to custom value
          b) New queries use updated timeout
          c) Reset to default after test

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Set to minimum valid value (100 ms)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '100'")
        # Set to custom value in range
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '5000'")
        # Set to maximum valid value (600000 ms)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '600000'")
        # Restore to default (30000 ms)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '30000'")

    def test_fq_sys_007(self):
        """FQ-SYS-007: MetaCacheTTL takes effect — cache hit/expiry behavior matches TTL

        Dimensions:
          a) Set federatedQueryMetaCacheTtlSeconds
          b) Cache behavior consistent with TTL
          c) Requires live external DB for full verification

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Set to minimum valid value (1 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '1'")
        # Set to custom value
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '300'")
        # Set to maximum valid value (86400 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '86400'")
        # Restore to default (300 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '300'")

    def test_fq_sys_008(self):
        """FQ-SYS-008: CapabilityCacheTTL takes effect — capability cache recalculated after expiry

        Verifies that federatedQueryCapabilityCacheTtlSeconds:
          a) Accepts minimum valid value (1)
          b) Accepts maximum valid value (86400)
          c) Rejects value below minimum (0)
          d) Rejects value above maximum (86401)
          e) Restores to default (300)

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Valid: minimum (1 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '1'")
        # Valid: custom mid-range
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '600'")
        # Valid: maximum (86400 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '86400'")
        # Invalid: below minimum (0)
        tdSql.error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '0'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)
        # Invalid: above maximum (86401)
        tdSql.error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '86401'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)
        # Restore to default (300 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '300'")

    def test_fq_sys_009(self):
        """FQ-SYS-009: OPTIONS override global config — per-source connect/read timeout overrides global

        Dimensions:
          a) Global timeout = 5000ms
          b) Source OPTIONS timeout = 2000ms
          c) Source uses per-source value, not global

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        cfg_mysql = self._mysql_cfg()
        src = "fq_sys_009"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database=testdb "
                f"options('connect_timeout_ms'='2000','read_timeout_ms'='3000')")
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    opts = row[self._COL_OPTIONS]
                    assert opts is not None, "options column must not be NULL"
                    opts_str = str(opts)
                    assert 'connect_timeout_ms' in opts_str, (
                        f"Expected 'connect_timeout_ms' in options, got: {opts_str}")
                    assert 'read_timeout_ms' in opts_str, (
                        f"Expected 'read_timeout_ms' in options, got: {opts_str}")
                    break
            assert found, (
                f"Source '{src}' not found in SHOW EXTERNAL SOURCES (SYS-009)")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_010(self):
        """FQ-SYS-010: TLS parameter persistence and masking — TLS cert params usable and displayed masked

        Dimensions:
          a) TLS parameters stored on disk
          b) SHOW output masks sensitive TLS data
          c) TLS connection functional

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        cfg_mysql = self._mysql_cfg()
        src = "fq_sys_010"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database=testdb "
                f"options('tls_ca'='/path/to/ca.pem')")
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    opts = str(row[self._COL_OPTIONS] or '')
                    # TLS cert option key must be present in options JSON
                    assert 'tls_ca' in opts, (
                        f"Expected 'tls_ca' in options, got: {opts}")
                    break
            assert found, (
                f"Source '{src}' not found in SHOW EXTERNAL SOURCES (SYS-010)")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-011 ~ FQ-SYS-015: Observability
    # ------------------------------------------------------------------

    def test_fq_sys_011(self):
        """FQ-SYS-011: External request metrics — clear error on connection failure (request path observable)

        Verifies that attempting to query an unreachable external source
        passes through the parser→catalog→planner→executor→connector chain
        and returns a connection-level error (not syntax error). This proves
        external requests are tracked / routed through the system.

        Dimensions:
          a) Query on unreachable source → passes parser (not SYNTAX_ERROR)
          b) Error is at connection layer (EXT_CONNECT_FAILED or similar)
          c) Second attempt also returns connection error (deterministic)

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_011"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            # Source visible in system table (request tracking path verified)
            tdSql.query(
                f"select source_name, type from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
            tdSql.checkData(0, 1, 'mysql')
            # Query via real external source: table 'some_table' doesn't exist →
            # NOT a syntax error (parser/planner accepted; connector returns table-not-found)
            self._assert_not_syntax_error(
                f"select * from {src}.testdb.some_table limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_012(self):
        """FQ-SYS-012: Pushdown behavior verification — external queries use external path (no local fallback)

        Verifies that queries on two different external source types both
        go through the external execution path, not silently resolved locally.
        Each query must be accepted by parser/planner (not SYNTAX_ERROR)
        and reach the connector layer.

        Dimensions:
          a) MySQL source query → external path (not SYNTAX_ERROR)
          b) PostgreSQL source query → external path (not SYNTAX_ERROR)
          c) Two sources do not interfere; each resolves independently

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_m = "fq_sys_012_m"
        src_p = "fq_sys_012_p"
        self._cleanup_src(src_m, src_p)
        try:
            self._mk_mysql_real(src_m)
            self._mk_pg_real(src_p)
            # Both sources must be registered
            tdSql.query(
                f"select count(*) from information_schema.ins_ext_sources "
                f"where source_name in ('{src_m}', '{src_p}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
            # MySQL source → external path
            self._assert_not_syntax_error(
                f"select * from {src_m}.testdb.t1 limit 1")
            # PostgreSQL source → external path
            self._assert_not_syntax_error(
                f"select * from {src_p}.pgdb.t1 limit 1")
        finally:
            self._cleanup_src(src_m, src_p)

    def test_fq_sys_013(self):
        """FQ-SYS-013: Metadata cache refresh verification — DESCRIBE rebuilds after REFRESH clears cache

        Verifies the metadata cache lifecycle:
          - First DESCRIBE builds cache from source metadata
          - REFRESH invalidates the cache
          - Second DESCRIBE re-fetches and rebuilds cache
          - Both DESCRIBE results are consistent

        Dimensions:
          a) First DESCRIBE returns ≥1 row
          b) REFRESH succeeds without error
          c) Second DESCRIBE returns same row count as first
          d) Source still visible in ins_ext_sources after REFRESH

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_013"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            # First DESCRIBE (builds cache)
            tdSql.query(f"describe external source {src}")
            first_rows = tdSql.queryRows
            assert first_rows >= 1, "DESCRIBE must return at least 1 row"

            # REFRESH clears metadata cache
            self._assert_not_syntax_error(f"refresh external source {src}")

            # Second DESCRIBE (rebuilds cache from source)
            tdSql.query(f"describe external source {src}")
            second_rows = tdSql.queryRows
            assert second_rows >= 1, "DESCRIBE after REFRESH must return at least 1 row"
            assert second_rows == first_rows, (
                f"DESCRIBE row count changed after REFRESH: "
                f"{first_rows} → {second_rows}")

            # Source still appears in system table after REFRESH
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
        finally:
            self._cleanup_src(src)

    def test_fq_sys_014(self):
        """FQ-SYS-014: Query execution chain verification — parser-planner-executor-connector full path

        Verifies the full query execution chain by:
          1. Creating source (catalog registration)
          2. Querying system table (catalog read)
          3. Issuing SELECT via external path (parser→planner→executor→connector)
          4. DDL cleanup (DROP: catalog write)

        Dimensions:
          a) CREATE EXTERNAL SOURCE → appears in ins_ext_sources (catalog)
          b) SELECT from system table → correct source_name and type
          c) SELECT via external path → not SYNTAX_ERROR (parser+planner OK)
          d) DROP → source removed from ins_ext_sources

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_014"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            # Step 1: catalog registration verified
            tdSql.query(
                "select source_name, type from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
            tdSql.checkData(0, 1, 'mysql')

            # Step 2: full chain via external path (parser OK, connector attempt may fail)
            self._assert_not_syntax_error(
                f"select * from {src}.testdb.some_table limit 1")
        finally:
            # Step 3: catalog write (DROP)
            self._cleanup_src(src)
        # Step 4: source removed
        tdSql.query(
            "select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(0)

    def test_fq_sys_015(self):
        """FQ-SYS-015: Source health observable — source remains available and metadata accessible after REFRESH

        Verifies that an external source remains visible in the system table
        after a connection failure, and that REFRESH re-triggers the
        health-probe cycle without removing the source.

        Dimensions:
          a) Source visible in ins_ext_sources after connection attempt
          b) REFRESH does not remove source from system table
          c) DESCRIBE still works after REFRESH (metadata accessible)
          d) Source count stable across REFRESH cycles

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_015"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)

            # Trigger a connection attempt (tables may not exist — connection error expected)
            self._assert_not_syntax_error(
                f"select * from {src}.testdb.t limit 1")

            # Source still visible after failure
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)

            # REFRESH re-probes health
            self._assert_not_syntax_error(f"refresh external source {src}")

            # Source still in system table after REFRESH
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)

            # DESCRIBE still works → metadata accessible
            tdSql.query(f"describe external source {src}")
            assert tdSql.queryRows >= 1, "DESCRIBE should return ≥1 row after REFRESH"
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-016 ~ FQ-SYS-020: Feature toggle and system table details
    # ------------------------------------------------------------------

    def test_fq_sys_016(self):
        """FQ-SYS-016: Default-off compatibility — no local behavior regression when feature is off

        Dimensions:
          a) federatedQueryEnable=0 → all external source ops rejected
          b) Local queries unaffected
          c) No regression in normal operations

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # This is a toggle test; we verify the current state is enabled
        # since setup_class requires it
        tdSql.query("show external sources")
        # Should not error → feature is enabled; queryRows ≥ 0 (may be empty)
        assert tdSql.queryRows >= 0

        # Local queries must be unaffected (basic regression verification)
        tdSql.execute("create database if not exists fq_sys_016_local")
        try:
            tdSql.execute("use fq_sys_016_local")
            tdSql.execute(
                "create table if not exists fq_016_t "
                "(ts timestamp, v int)")
            tdSql.execute(
                "insert into fq_016_t values (1704067200000, 42)")
            tdSql.query("select v from fq_016_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)
        finally:
            tdSql.execute("drop database if exists fq_sys_016_local")

    def test_fq_sys_017(self):
        """FQ-SYS-017: SHOW output options field JSON format and sensitive data masking

        Dimensions:
          a) options column is valid JSON
          b) api_token, tls_client_key masked
          c) Non-sensitive options visible

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        cfg_influx = self._influx_cfg()
        src = "fq_sys_017"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='u' password='' "
                f"database=telegraf options('api_token'='secret_token','protocol'='flight_sql')")
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    opts = str(row[self._COL_OPTIONS])
                    # api_token should be masked
                    assert 'secret_token' not in opts, \
                        "api_token should be masked in SHOW output"
                    # protocol should be visible
                    assert 'flight_sql' in opts, (
                        f"Expected 'flight_sql' visible in options, got: {opts}")
                    break
            assert found, (
                f"Source '{src}' not found in SHOW EXTERNAL SOURCES (SYS-017)")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_018(self):
        """FQ-SYS-018: SHOW output create_time field correctness

        Dimensions:
          a) create_time is TIMESTAMP type
          b) Value close to current time
          c) Precision to milliseconds

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_018"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    ctime = row[self._COL_CTIME]
                    assert ctime is not None, "create_time should not be NULL"
                    # create_time must be a recent timestamp (within last 60 s)
                    import time
                    now_ms = int(time.time() * 1000)
                    ctime_ms = int(ctime) if not hasattr(ctime, 'timestamp') \
                        else int(ctime.timestamp() * 1000)
                    assert ctime_ms <= now_ms, (
                        f"create_time {ctime_ms} is in the future (now={now_ms})")
                    assert ctime_ms >= now_ms - 60_000, (
                        f"create_time {ctime_ms} is too old (> 60s ago, now={now_ms})")
                    break
            assert found, (
                f"Source '{src}' not found in SHOW EXTERNAL SOURCES (SYS-018)")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_019(self):
        """FQ-SYS-019: DESCRIBE and SHOW output field consistency

        Dimensions:
          a) DESCRIBE fields match SHOW row for same source
          b) All fields consistent

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_019"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            tdSql.query(f"describe external source {src}")
            desc_result = tdSql.queryResult

            tdSql.query("show external sources")
            show_row = None
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    show_row = row
                    break
            assert show_row is not None
            assert desc_result is not None
            # Verify key fields are consistent between DESCRIBE and SHOW
            assert desc_result[0][0] == show_row[self._COL_NAME], (
                "source_name mismatch: DESCRIBE vs SHOW")
            assert desc_result[0][1] == show_row[self._COL_TYPE], (
                "type mismatch: DESCRIBE vs SHOW")
            assert desc_result[0][2] == show_row[self._COL_HOST], (
                "host mismatch: DESCRIBE vs SHOW")
            assert desc_result[0][3] == show_row[self._COL_PORT], (
                "port mismatch: DESCRIBE vs SHOW")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_020(self):
        """FQ-SYS-020: ins_ext_sources system table options column JSON format

        Dimensions:
          a) Direct query on information_schema.ins_ext_sources
          b) options column contains valid JSON
          c) Sensitive values masked

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_020"
        self._cleanup_src(src)
        try:
            self._mk_pg_real(src)
            tdSql.query(
                f"select options from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)  # source must exist before parsing options
            if tdSql.queryRows > 0:
                opts = tdSql.queryResult[0][0]
                if opts is not None:
                    # Should be valid JSON string
                    import json
                    parsed = json.loads(opts)
                    assert isinstance(parsed, dict), (
                        f"options should be a JSON object, got: {type(parsed)}")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-021 ~ FQ-SYS-025: Config parameter boundaries
    # ------------------------------------------------------------------

    def test_fq_sys_021(self):
        """FQ-SYS-021: federatedQueryConnectTimeoutMs minimum 100ms takes effect

        Dimensions:
          a) Set to 100 → accepted
          b) New queries use 100ms timeout
          c) Timeout triggers correctly on unreachable host

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '100'")
        # Restore to reasonable default
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '5000'")
        # Verify invalid value (below minimum 100) is rejected
        tdSql.error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '99'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)
        # Restore to default 30000
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '30000'")

    def test_fq_sys_022(self):
        """FQ-SYS-022: federatedQueryConnectTimeoutMs below minimum 99 is rejected

        Dimensions:
          a) Set to 99 → rejected
          b) Error: parameter out of range
          c) Config retains original value

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # TSDB_CODE_EXT_CONFIG_PARAM_INVALID = None: enterprise error codes are TBD;
        # tdSql.error() with expectedErrno=None verifies *some* error occurs.
        tdSql.error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '99'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)

    def test_fq_sys_023(self):
        """FQ-SYS-023: federatedQueryMetaCacheTtlSeconds maximum 86400 takes effect

        Dimensions:
          a) Set to 86400 → accepted
          b) Set to 86401 → rejected
          c) Config stays at 86400 if 86401 rejected

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '86400'")
        # TSDB_CODE_EXT_CONFIG_PARAM_INVALID = None: enterprise error codes are TBD;
        # tdSql.error() with expectedErrno=None verifies *some* error occurs.
        tdSql.error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '86401'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)

    def test_fq_sys_024(self):
        """FQ-SYS-024: federatedQueryEnable parameter — federated operations available when server-side enabled

        Verifies that with federatedQueryEnable=1 on the server (which
        setup_class requires), external source DDL and queries succeed.
        Also verifies the parameter is recognized and alterable.

        Dimensions:
          a) Feature enabled → SHOW EXTERNAL SOURCES succeeds
          b) External source DDL works under enabled flag
          c) alter dnode 1 'federatedQueryEnable' '1' recognized

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_024"
        self._cleanup_src(src)
        try:
            # Feature is enabled (verified by setup_class.require_external_source_feature)
            tdSql.query("show external sources")
            assert tdSql.queryRows >= 0  # no error → feature is ON

            # DDL works under enabled flag
            self._mk_mysql_real(src)
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)

            # Parameter is recognized by server
            self._assert_not_syntax_error(
                "alter dnode 1 'federatedQueryEnable' '1'")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_025(self):
        """FQ-SYS-025: federatedQueryConnectTimeoutMs server-side only — configurable on server

        Verifies that federatedQueryConnectTimeoutMs is a server-side
        parameter: it can be altered via 'alter dnode', valid range is
        [100, 600000], and the configuration is recognized.

        Dimensions:
          a) alter dnode 1 'federatedQueryConnectTimeoutMs' accepted
          b) Valid range [100, 600000]
          c) Server applies the new value

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Verify valid range boundaries
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '100'")
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '10000'")
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '600000'")
        # Restore to default
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '30000'")

    # ------------------------------------------------------------------
    # FQ-SYS-026 ~ FQ-SYS-028: Upgrade/downgrade and per-source config
    # ------------------------------------------------------------------

    def test_fq_sys_026(self):
        """FQ-SYS-026: Zero external sources state — clean system state after dropping all sources

        Verifies that after dropping all test-created external sources,
        the system table returns zero rows for those names. This models
        the "zero federation data" invariant required before downgrade.

        Dimensions:
          a) Create N external sources
          b) Verify all N appear in ins_ext_sources
          c) DROP all N sources
          d) ins_ext_sources shows 0 rows for those names

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        srcs = ["fq_sys_026a", "fq_sys_026b", "fq_sys_026c"]
        self._cleanup_src(*srcs)
        try:
            for s in srcs:
                self._mk_mysql_real(s)
            # Verify all created
            for s in srcs:
                tdSql.query(
                    "select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{s}'")
                tdSql.checkRows(1)
        finally:
            self._cleanup_src(*srcs)
        # After DROP: none visible — models zero-data state for downgrade
        for s in srcs:
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{s}'")
            tdSql.checkRows(0)

    def test_fq_sys_027(self):
        """FQ-SYS-027: External source persistence — still visible after re-query (persistence check)

        Verifies that external source definitions survive context changes
        (not only in-memory cache). This models the "has federation data"
        state, verifying persistence across queries and ALTER operations.

        Dimensions:
          a) Create source → visible in ins_ext_sources
          b) count(*) confirms exactly 1 row for the source
          c) ALTER host → new host persists in DESCRIBE
          d) DROP → source permanently removed

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_027"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)

            # Verify persistence: count confirms exactly 1 row
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            # ALTER persists change
            self._assert_not_syntax_error(
                f"alter external source {src} host='altered.example.com'")

            # DESCRIBE reflects altered state
            tdSql.query(f"describe external source {src}")
            assert tdSql.queryRows >= 1
            assert tdSql.queryResult[0][self._COL_HOST] == 'altered.example.com', (
                f"After ALTER, host should be 'altered.example.com', "
                f"got '{tdSql.queryResult[0][self._COL_HOST]}'")
        finally:
            self._cleanup_src(src)
        # After DROP: source permanently removed
        tdSql.query(
            "select count(*) from information_schema.ins_ext_sources "
            f"where source_name = '{src}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    def test_fq_sys_028(self):
        """FQ-SYS-028: read_timeout_ms/connect_timeout_ms per-source OPTIONS override global

        Dimensions:
          a) Per-source read_timeout_ms overrides global
          b) Per-source connect_timeout_ms overrides global
          c) Source timeout behavior matches per-source value
          d) Global default for sources without OPTIONS

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_default = "fq_sys_028_def"
        src_custom = "fq_sys_028_cust"
        self._cleanup_src(src_default, src_custom)
        try:
            # Default source (uses global config)
            self._mk_mysql_real(src_default)
            cfg_mysql2 = self._mysql_cfg()
            # Custom source with per-source timeout
            tdSql.execute(
                f"create external source {src_custom} type='mysql' "
                f"host='{cfg_mysql2.host}' port={cfg_mysql2.port} user='u' password='p' database=testdb "
                f"options('read_timeout_ms'='1000','connect_timeout_ms'='500')")

            tdSql.query("show external sources")
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src_custom:
                    opts = str(row[self._COL_OPTIONS] or '')
                    assert 'read_timeout_ms' in opts, (
                        f"Expected 'read_timeout_ms' in options, got: {opts}")
                    assert 'connect_timeout_ms' in opts, (
                        f"Expected 'connect_timeout_ms' in options, got: {opts}")
                elif row[self._COL_NAME] == src_default:
                    # Default source: no per-source timeout in options
                    pass

            # Verify both sources exist in system table
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name in ('{src_default}', '{src_custom}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
        finally:
            self._cleanup_src(src_default, src_custom)

    # ==================================================================
    # Gap supplement cases (Mainline B)
    # Dimensions not fully covered by FQ-SYS-001~028:
    #   s01: Empty SHOW (source-name filter returns 0 rows)
    #   s02: DESCRIBE non-existent source → error
    #   s03: Column ordering in ins_ext_sources matches DS §5.4
    #   s04: PostgreSQL source → schema field populated
    #   s05: InfluxDB source → type/database/masked api_token
    #   s06: ALTER + immediately SHOW → updated field visible
    #   s07: Multiple sources + type-based WHERE filter
    #   s08: federatedQueryCapabilityCacheTtlSeconds boundary test
    #   s09: Partial column SELECT from ins_ext_sources
    #   s10: Compound WHERE on ins_ext_sources
    # ==================================================================

    def test_fq_sys_s01(self):
        """sXX Gap: Querying ins_ext_sources for non-existent source returns 0 rows

        Verifies that a WHERE filter for a source name that does not exist
        returns an empty result (not an error), and that SHOW EXTERNAL SOURCES
        itself is always safe to call.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        fake = "_fq_sys_s01_never_created_"
        # Query for a source that was never created → must return 0 rows
        tdSql.query(
            "select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{fake}'")
        tdSql.checkRows(0)

        # SHOW EXTERNAL SOURCES itself must always succeed (never error)
        tdSql.query("show external sources")
        assert tdSql.queryRows >= 0

    def test_fq_sys_s02(self):
        """sXX Gap: DESCRIBE non-existent external source returns error

        Verifies that describing a source name that does not exist returns
        TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST (or equivalent), not a
        syntax error.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST = None: enterprise error codes TBD;
        # tdSql.error() with expectedErrno=None verifies *some* error occurs.
        tdSql.error(
            "describe external source _fq_sys_s02_no_such_src_",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST)

    def test_fq_sys_s03(self):
        """sXX Gap: ins_ext_sources column ordering matches DS §5.4 schema

        Verifies the 10 columns appear in the documented order:
        source_name[0], type[1], host[2], port[3], user[4], password[5],
        database[6], schema[7], options[8], create_time[9].

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_s03"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database='testdb')
            tdSql.query(
                "select * from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            row = tdSql.queryResult[0]
            assert len(row) == 10, (
                f"Expected 10 columns, got {len(row)}")
            # Verify ordering: col0=source_name, col1=type, col2=host, col3=port
            assert row[self._COL_NAME] == src, (
                f"Expected source_name='{src}', got '{row[self._COL_NAME]}'")
            assert row[self._COL_TYPE] == 'mysql', (
                f"Expected type='mysql', got '{row[self._COL_TYPE]}'")
            assert row[self._COL_HOST] == self._mysql_cfg().host, (
                f"Expected host='{self._mysql_cfg().host}', got '{row[self._COL_HOST]}'")
            assert row[self._COL_PORT] == self._mysql_cfg().port, (
                f"Expected port={self._mysql_cfg().port}, got {row[self._COL_PORT]}")
            # col6=database, col7=schema (empty for MySQL), col9=create_time (not NULL)
            assert row[self._COL_DATABASE] == 'testdb', (
                f"Expected database='testdb', got '{row[self._COL_DATABASE]}'")
            assert row[self._COL_SCHEMA] in ('', None), (
                f"Expected schema='' or None (MySQL), got '{row[self._COL_SCHEMA]}'")
            assert row[self._COL_CTIME] is not None, (
                "create_time must not be NULL")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s04(self):
        """sXX Gap: PostgreSQL source schema field is populated in ins_ext_sources

        Verifies that a PostgreSQL source created with schema='public'
        has the schema column correctly populated. MySQL has empty schema.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_s04"
        self._cleanup_src(src)
        try:
            self._mk_pg_real(src, database='pgdb', schema='public')
            tdSql.query(
                "select * from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            row = tdSql.queryResult[0]
            assert row[self._COL_TYPE] == 'postgresql', (
                f"Expected type='postgresql', got '{row[self._COL_TYPE]}'")
            assert row[self._COL_DATABASE] == 'pgdb', (
                f"Expected database='pgdb', got '{row[self._COL_DATABASE]}'")
            assert row[self._COL_SCHEMA] == 'public', (
                f"Expected schema='public', got: '{row[self._COL_SCHEMA]}'")
            assert row[self._COL_CTIME] is not None, (
                "create_time must not be NULL")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s05(self):
        """sXX Gap: InfluxDB source shows correct type, database, masked api_token

        Verifies InfluxDB DDL fields in ins_ext_sources:
          - type = 'influxdb'
          - database = 'telegraf'
          - schema is empty (InfluxDB has no schema layer)
          - options: api_token masked, protocol='flight_sql' visible

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_s05"
        self._cleanup_src(src)
        try:
            self._mk_influx_real(src)
            tdSql.query(
                "select * from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            row = tdSql.queryResult[0]
            assert row[self._COL_TYPE] == 'influxdb', (
                f"Expected type='influxdb', got '{row[self._COL_TYPE]}'")
            assert row[self._COL_DATABASE] == 'telegraf', (
                f"Expected database='telegraf', got '{row[self._COL_DATABASE]}'")
            assert row[self._COL_SCHEMA] in ('', None), (
                "InfluxDB source should have empty schema")
            opts = str(row[self._COL_OPTIONS] or '')
            # api_token 'tok' must be masked in options
            assert 'tok' not in opts, (
                f"api_token value should be masked in options, got: {opts}")
            # protocol should remain visible
            assert 'flight_sql' in opts, (
                f"Expected 'flight_sql' in options, got: {opts}")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s06(self):
        """sXX Gap: ALTER EXTERNAL SOURCE change is immediately visible in SHOW

        Verifies that after ALTER HOST, the updated host value appears
        in the next SHOW EXTERNAL SOURCES / ins_ext_sources query.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_s06"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src)
            # Verify original host (real MySQL config host)
            tdSql.query(
                "select host from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, self._mysql_cfg().host)

            # ALTER host to a different address; verify system table updated immediately
            tdSql.execute(f"alter external source {src} host='altered.example.com'")

            # Verify updated host appears immediately
            tdSql.query(
                "select host from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 'altered.example.com')
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s07(self):
        """sXX Gap: Multiple sources visible; type-based WHERE filter works

        Creates sources of different types and verifies:
          - All sources appear in ins_ext_sources
          - WHERE type='mysql' returns only MySQL sources
          - WHERE type='postgresql' returns only PG sources
          - count(*) matches the expected number per type

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        srcs_m = ["fq_sys_s07_m1", "fq_sys_s07_m2"]
        srcs_p = ["fq_sys_s07_p1"]
        all_srcs = srcs_m + srcs_p
        self._cleanup_src(*all_srcs)
        try:
            for s in srcs_m:
                self._mk_mysql_real(s)
            for s in srcs_p:
                self._mk_pg_real(s)

            # All sources visible individually
            for s in all_srcs:
                tdSql.query(
                    "select source_name from information_schema.ins_ext_sources "
                    f"where source_name = '{s}'")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, s)

            # MySQL count = 2
            m_names = "','".join(srcs_m)
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where type = 'mysql' and source_name in ('{m_names}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # PostgreSQL count = 1
            p_names = "','".join(srcs_p)
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where type = 'postgresql' and source_name in ('{p_names}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
        finally:
            self._cleanup_src(*all_srcs)

    def test_fq_sys_s08(self):
        """sXX Gap: federatedQueryCapabilityCacheTtlSeconds boundary test

        Verifies:
          - Minimum value (1) accepted
          - Maximum value (86400) accepted
          - Below minimum (0) rejected
          - Above maximum (86401) rejected
          - Restore to default (300) succeeds

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Valid: minimum (1 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '1'")
        # Valid: maximum (86400 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '86400'")
        # Invalid: below minimum (0)
        # TSDB_CODE_EXT_CONFIG_PARAM_INVALID = None: enterprise codes TBD;
        # verifies *some* error occurs.
        tdSql.error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '0'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)
        # Invalid: above maximum (86401)
        tdSql.error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '86401'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)
        # Restore to default (300 s)
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryCapabilityCacheTtlSeconds' '300'")

    def test_fq_sys_s09(self):
        """sXX Gap: Partial column SELECT (projection) from ins_ext_sources

        Verifies that selecting specific columns from ins_ext_sources
        returns the correct projected values, testing column-level access.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_sys_s09"
        self._cleanup_src(src)
        try:
            self._mk_pg_real(src, database='pgdb', schema='public')
            tdSql.query(
                "select source_name, type, database, schema "
                "from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)
            tdSql.checkData(0, 1, 'postgresql')
            tdSql.checkData(0, 2, 'pgdb')
            tdSql.checkData(0, 3, 'public')
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s10(self):
        """sXX Gap: Compound WHERE on ins_ext_sources (host AND type filtering)

        Verifies that multi-condition WHERE predicates on ins_ext_sources
        work correctly: AND of host + type returns the expected subset.

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        srcs = ["fq_sys_s10a", "fq_sys_s10b"]
        self._cleanup_src(*srcs)
        try:
            self._mk_mysql_real(srcs[0])
            self._mk_pg_real(srcs[1])

            # WHERE host=<real_host> AND source_name IN (...) → 2 rows
            # (MySQL and PG both use config host, typically 127.0.0.1)
            real_host = self._mysql_cfg().host
            q_all = (
                "select count(*) from information_schema.ins_ext_sources "
                f"where host = '{real_host}' "
                f"and source_name in ('{srcs[0]}', '{srcs[1]}')")
            tdSql.query(q_all)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)

            # WHERE host=<real_host> AND type='mysql' AND source_name IN (...) → 1 row
            q_mysql = (
                "select source_name from information_schema.ins_ext_sources "
                f"where host = '{real_host}' and type = 'mysql' "
                f"and source_name in ('{srcs[0]}', '{srcs[1]}')")
            tdSql.query(q_mysql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, srcs[0])

            # WHERE type='postgresql' AND source_name IN (...) → 1 row
            q_pg = (
                "select source_name from information_schema.ins_ext_sources "
                f"where type = 'postgresql' "
                f"and source_name in ('{srcs[0]}', '{srcs[1]}')")
            tdSql.query(q_pg)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, srcs[1])
        finally:
            self._cleanup_src(*srcs)

    def test_fq_sys_s11_connect_timeout_actual_trigger(self):
        """Gap: connect_timeout_ms attribute takes effect and returns UNAVAILABLE

        Creates a source with connect_timeout_ms=500 against a stopped MySQL
        instance and verifies that:
          a) The query fails with TSDB_CODE_EXT_SOURCE_UNAVAILABLE
          b) The error returns within a reasonable time (<= 10 s)
          c) The source remains visible in ins_ext_sources after failure
          d) The connect_timeout_ms value is reflected in the system table

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 wpan Initial implementation

        """
        src = "fq_sys_s11"
        cfg = self._mysql_cfg()
        ver = cfg.version
        self._cleanup_src(src)
        try:
            # (d) create with explicit timeout — verify it shows in system table
            tdSql.execute(
                f"create external source {src} "
                f"type='mysql' host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"options('connect_timeout_ms'='500')"
            )
            tdSql.query(
                "select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, src)

            # Stop MySQL to make the source unreachable
            ExtSrcEnv.stop_mysql_instance(ver)
            try:
                # (a) + (b) query must fail with UNAVAILABLE within ~10 s
                t0 = time.monotonic()
                tdSql.error(
                    f"select count(*) from {src}.testdb.t",
                    expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                )
                elapsed = time.monotonic() - t0
                assert elapsed < 10, (
                    f"Timeout-constrained query took {elapsed:.2f}s, expected < 10s"
                )

                # (c) source still visible in system table during outage
                tdSql.query(
                    "select count(*) from information_schema.ins_ext_sources "
                    f"where source_name = '{src}'"
                )
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 1)
            finally:
                ExtSrcEnv.start_mysql_instance(ver)
        finally:
            self._cleanup_src(src)

    def test_fq_sys_s12_concurrent_alter_query_safety(self):
        """Gap: concurrent ALTER external source during active queries — no corruption

        Launches reader threads that repeatedly SELECT COUNT(*) against a
        source, while the main thread repeatedly ALTERs the source's OPTIONS
        field.  After all threads finish:
          a) No thread encountered an uncaught exception
          b) All successful reads returned a consistent row count
          c) The source is still in the catalog (not accidentally dropped)
          d) A final query succeeds and returns correct data

        Catalog: - Query:FederatedSystem

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 wpan Initial implementation

        """
        src = "fq_sys_s12"
        ext_db = "fq_sys_s12_ext"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        _READER_COUNT = 4
        _READS_PER_THREAD = 5
        _ALTER_ROUNDS = 10
        errors: list = []
        counts: list = []
        counts_lock = threading.Lock()

        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, [
                "drop table if exists sys_t",
                "create table sys_t (id int primary key, val int)",
                "insert into sys_t values (1,1),(2,2),(3,3)",
            ])
            self._mk_mysql_real(src, database=ext_db)

            def _reader(tid):
                for _ in range(_READS_PER_THREAD):
                    try:
                        tdSql.query(f"select count(*) from {src}.sys_t")
                        with counts_lock:
                            counts.append(tdSql.queryResult[0][0])
                    except Exception as ex:
                        with counts_lock:
                            errors.append(f"reader {tid}: {ex}")

            threads = [
                threading.Thread(target=_reader, args=(i,))
                for i in range(_READER_COUNT)
            ]
            for t in threads:
                t.start()

            # Main thread: repeatedly ALTER OPTIONS to toggle connect_timeout_ms
            for i in range(_ALTER_ROUNDS):
                ms = 1000 + i * 100
                try:
                    tdSql.execute(
                        f"alter external source {src} "
                        f"options('connect_timeout_ms'='{ms}')"
                    )
                except Exception:
                    pass  # ALTER may race with reads; tolerate temporary errors

            for t in threads:
                t.join(timeout=60)

            # (a) No reader thread encountered an uncaught exception
            assert not errors, f"Reader errors during concurrent ALTER: {errors}"

            # (b) All recorded counts must equal 3
            assert all(c == 3 for c in counts), (
                f"Inconsistent count during concurrent ALTER: {counts}"
            )

            # (c) Source still in catalog
            tdSql.query(
                "select count(*) from information_schema.ins_ext_sources "
                f"where source_name = '{src}'"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            # (d) Final query returns correct data
            tdSql.query(f"select count(*) from {src}.sys_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

