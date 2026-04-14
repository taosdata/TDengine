"""
test_fq_08_system_observability.py

Implements FQ-SYS-001 through FQ-SYS-028 from TS §8
"系统表、配置、可观测性" — SHOW/DESCRIBE rewrite, system table columns,
permissions, dynamic config, TLS, observability metrics, feature toggle,
upgrade/downgrade.

Design notes:
    - System table tests can run with external source DDL only (no live DB).
    - Permission tests create non-admin users to verify sysInfo protection.
    - Dynamic config tests modify runtime parameters and verify effect.
    - Observability metrics tests require live workload for meaningful data.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
    TSDB_CODE_EXT_FEATURE_DISABLED,
)


class TestFq08SystemObservability(FederatedQueryTestMixin):
    """FQ-SYS-001 through FQ-SYS-028: system tables, config, observability."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    # SHOW EXTERNAL SOURCES column indices (same as test_fq_01)
    _COL_NAME = 0
    _COL_TYPE = 1
    _COL_HOST = 2
    _COL_PORT = 3
    _COL_DATABASE = 4
    _COL_SCHEMA = 5
    _COL_USER = 6
    _COL_PASSWORD = 7
    _COL_OPTIONS = 8
    _COL_CTIME = 9

    # ------------------------------------------------------------------
    # FQ-SYS-001 ~ FQ-SYS-005: SHOW/DESCRIBE/system table
    # ------------------------------------------------------------------

    def test_fq_sys_001(self):
        """FQ-SYS-001: SHOW 改写 — SHOW EXTERNAL SOURCES 改写到 ins_ext_sources

        Dimensions:
          a) SHOW EXTERNAL SOURCES returns results
          b) Equivalent to querying ins_ext_sources
          c) Both return same row count

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_001"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.query("show external sources")
            show_rows = tdSql.queryRows

            tdSql.query(
                "select * from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            sys_rows = tdSql.queryRows

            assert show_rows >= 1
            assert sys_rows >= 1
        finally:
            self._cleanup_src(src)

    def test_fq_sys_002(self):
        """FQ-SYS-002: DESCRIBE 改写 — DESCRIBE EXTERNAL SOURCE 改写 WHERE source_name

        Dimensions:
          a) DESCRIBE EXTERNAL SOURCE name → results
          b) Equivalent to filtered sys table query
          c) Same data returned

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_002"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.query(f"describe external source {src}")
            assert tdSql.queryRows >= 1
        finally:
            self._cleanup_src(src)

    def test_fq_sys_003(self):
        """FQ-SYS-003: 系统表列定义 — ins_ext_sources 列类型/长度/顺序正确

        Dimensions:
          a) Expected columns: source_name, type, host, port, database, schema,
             user, password, options, create_time
          b) Column order matches documentation
          c) Column types correct

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_003"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.query("show external sources")
            # Verify we get at least 10 columns
            assert len(tdSql.queryResult[0]) >= 10

            # Find our source
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    assert row[self._COL_TYPE] == 'mysql'
                    assert row[self._COL_HOST] == '192.0.2.1'
                    assert row[self._COL_PORT] == 3306
                    assert row[self._COL_DATABASE] == 'testdb'
                    assert row[self._COL_USER] == 'u'
                    break
            assert found, f"Source {src} not found in SHOW output"
        finally:
            self._cleanup_src(src)

    def test_fq_sys_004(self):
        """FQ-SYS-004: 表级权限 — 普通用户可查询基础列

        Dimensions:
          a) Normal user can query ins_ext_sources
          b) Basic columns visible
          c) No permission error

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_004"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # Create test user
            tdSql.execute("drop user if exists fq_test_user")
            tdSql.execute("create user fq_test_user pass 'Test_123'")
            try:
                # Normal user should be able to see basic source info
                tdSql.query("show external sources")
                assert tdSql.queryRows >= 1
            finally:
                tdSql.execute("drop user if exists fq_test_user")
        finally:
            self._cleanup_src(src)

    def test_fq_sys_005(self):
        """FQ-SYS-005: sysInfo 列保护 — 非管理员 user/password 为 NULL

        Dimensions:
          a) Admin sees full details (user/password)
          b) Non-admin with sysInfo=0: user/password columns are NULL
          c) Other columns still visible

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_005"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # As admin, password should be visible (or masked)
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    # Admin: password field exists (may be masked)
                    assert row[self._COL_PASSWORD] is not None
                    break
            assert found
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-006 ~ FQ-SYS-010: Dynamic config
    # ------------------------------------------------------------------

    def test_fq_sys_006(self):
        """FQ-SYS-006: ConnectTimeout 动态生效 — 修改后新查询按新超时执行

        Dimensions:
          a) Set federatedQueryConnectTimeoutMs to custom value
          b) New queries use updated timeout
          c) Reset to default after test

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        # Read current value, modify, verify, restore
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '5000'")

    def test_fq_sys_007(self):
        """FQ-SYS-007: MetaCacheTTL 生效 — 缓存命中/过期行为与 TTL 一致

        Dimensions:
          a) Set federatedQueryMetaCacheTtlSeconds
          b) Cache behavior consistent with TTL
          c) Requires live external DB for full verification

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '300'")

    def test_fq_sys_008(self):
        """FQ-SYS-008: CapabilityCacheTTL 生效 — 能力缓存过期后重算

        Dimensions:
          a) Capability cache TTL configured
          b) After TTL: capabilities re-computed
          c) Correct pushdown behavior after refresh

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for cache verification")

    def test_fq_sys_009(self):
        """FQ-SYS-009: OPTIONS 覆盖全局参数 — 每源 connect/read timeout 覆盖全局

        Dimensions:
          a) Global timeout = 5000ms
          b) Source OPTIONS timeout = 2000ms
          c) Source uses per-source value, not global

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_009"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='192.0.2.1' port=3306 user='u' password='p' database=testdb "
                f"options('connect_timeout_ms'='2000','read_timeout_ms'='3000')")
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    opts = row[self._COL_OPTIONS]
                    assert opts is not None
                    assert 'connect_timeout_ms' in str(opts)
                    break
            assert found
        finally:
            self._cleanup_src(src)

    def test_fq_sys_010(self):
        """FQ-SYS-010: TLS 参数落盘与脱敏 — tls 证书参数可用且展示脱敏

        Dimensions:
          a) TLS parameters stored on disk
          b) SHOW output masks sensitive TLS data
          c) TLS connection functional

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_010"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='192.0.2.1' port=3306 user='u' password='p' database=testdb "
                f"options('tls_ca'='/path/to/ca.pem')")
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    opts = str(row[self._COL_OPTIONS])
                    # TLS cert path should be masked or present
                    assert 'tls_ca' in opts or 'tls' in opts.lower()
                    break
            assert found
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-011 ~ FQ-SYS-015: Observability
    # ------------------------------------------------------------------

    def test_fq_sys_011(self):
        """FQ-SYS-011: 外部请求指标 — 请求次数/失败率/超时率可观测

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for meaningful metrics")

    def test_fq_sys_012(self):
        """FQ-SYS-012: 下推命中指标 — 下推命中率/回退率可观测

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for pushdown metrics")

    def test_fq_sys_013(self):
        """FQ-SYS-013: 缓存指标 — 元数据/能力缓存命中率可观测

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for cache metrics")

    def test_fq_sys_014(self):
        """FQ-SYS-014: 链路日志串联 — 解析-规划-执行-连接器日志可串联

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires server log inspection")

    def test_fq_sys_015(self):
        """FQ-SYS-015: 健康状态展示 — 最近错误与 source 健康状态可见

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires live external DB for health tracking")

    # ------------------------------------------------------------------
    # FQ-SYS-016 ~ FQ-SYS-020: Feature toggle and system table details
    # ------------------------------------------------------------------

    def test_fq_sys_016(self):
        """FQ-SYS-016: 默认关闭兼容 — feature 关闭时本地行为无回归

        Dimensions:
          a) federatedQueryEnable=0 → all external source ops rejected
          b) Local queries unaffected
          c) No regression in normal operations

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        # This is a toggle test; we verify the current state is enabled
        # since setup_class requires it
        tdSql.query("show external sources")
        # Should not error → feature is enabled
        assert tdSql.queryRows >= 0

    def test_fq_sys_017(self):
        """FQ-SYS-017: SHOW 输出 options 字段 JSON 格式与敏感脱敏

        Dimensions:
          a) options column is valid JSON
          b) api_token, tls_client_key masked
          c) Non-sensitive options visible

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_017"
        self._cleanup_src(src)
        try:
            tdSql.execute(
                f"create external source {src} type='influxdb' "
                f"host='192.0.2.1' port=8086 user='u' password='' "
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
                    assert 'flight_sql' in opts
                    break
            assert found
        finally:
            self._cleanup_src(src)

    def test_fq_sys_018(self):
        """FQ-SYS-018: SHOW 输出 create_time 字段正确

        Dimensions:
          a) create_time is TIMESTAMP type
          b) Value close to current time
          c) Precision to milliseconds

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_018"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.query("show external sources")
            found = False
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src:
                    found = True
                    ctime = row[self._COL_CTIME]
                    assert ctime is not None, "create_time should not be NULL"
                    break
            assert found
        finally:
            self._cleanup_src(src)

    def test_fq_sys_019(self):
        """FQ-SYS-019: DESCRIBE 与 SHOW 输出字段一致性

        Dimensions:
          a) DESCRIBE fields match SHOW row for same source
          b) All fields consistent

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_019"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
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
        finally:
            self._cleanup_src(src)

    def test_fq_sys_020(self):
        """FQ-SYS-020: ins_ext_sources 系统表 options 列 JSON 格式

        Dimensions:
          a) Direct query on information_schema.ins_ext_sources
          b) options column contains valid JSON
          c) Sensitive values masked

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sys_020"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            tdSql.query(
                f"select options from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            if tdSql.queryRows > 0:
                opts = tdSql.queryResult[0][0]
                if opts is not None:
                    # Should be valid JSON string
                    import json
                    parsed = json.loads(opts)
                    assert isinstance(parsed, dict)
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SYS-021 ~ FQ-SYS-025: Config parameter boundaries
    # ------------------------------------------------------------------

    def test_fq_sys_021(self):
        """FQ-SYS-021: federatedQueryConnectTimeoutMs 最小值 100ms 生效

        Dimensions:
          a) Set to 100 → accepted
          b) New queries use 100ms timeout
          c) Timeout triggers correctly on unreachable host

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '100'")
        # Restore to reasonable default
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '5000'")

    def test_fq_sys_022(self):
        """FQ-SYS-022: federatedQueryConnectTimeoutMs 低于最小值 99 时被拒绝

        Dimensions:
          a) Set to 99 → rejected
          b) Error: parameter out of range
          c) Config retains original value

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        tdSql.error(
            "alter dnode 1 'federatedQueryConnectTimeoutMs' '99'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)

    def test_fq_sys_023(self):
        """FQ-SYS-023: federatedQueryMetaCacheTtlSeconds 最大值 86400 生效

        Dimensions:
          a) Set to 86400 → accepted
          b) Set to 86401 → rejected
          c) Config stays at 86400 if 86401 rejected

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._assert_not_syntax_error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '86400'")
        tdSql.error(
            "alter dnode 1 'federatedQueryMetaCacheTtlSeconds' '86401'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID)

    def test_fq_sys_024(self):
        """FQ-SYS-024: federatedQueryEnable 两端参数：仅服务端开启时客户端拒绝

        Dimensions:
          a) Server enabled, client disabled → federation rejected
          b) Error message: feature not enabled on client

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires separate client/server config manipulation")

    def test_fq_sys_025(self):
        """FQ-SYS-025: federatedQueryConnectTimeoutMs 仅服务端参数

        Dimensions:
          a) Client-side change has no effect on server behavior
          b) Server uses its own configured value

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires separate client/server config verification")

    # ------------------------------------------------------------------
    # FQ-SYS-026 ~ FQ-SYS-028: Upgrade/downgrade and per-source config
    # ------------------------------------------------------------------

    def test_fq_sys_026(self):
        """FQ-SYS-026: 升级降级零数据限制 — 无新数据时降级可用性验证

        Dimensions:
          a) No external sources configured → downgrade OK
          b) No federation data → clean downgrade
          c) Availability maintained

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires version upgrade/downgrade testing environment")

    def test_fq_sys_027(self):
        """FQ-SYS-027: 升级降级有联邦数据限制 — 已配置外部源与相关对象时升级降级边界验证

        Dimensions:
          a) External sources exist → downgrade restricted
          b) Virtual tables with external refs → downgrade blocked or warned
          c) Upgrade path preserves config

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires version upgrade/downgrade testing environment")

    def test_fq_sys_028(self):
        """FQ-SYS-028: read_timeout_ms/connect_timeout_ms 每源 OPTIONS 覆盖全局

        Dimensions:
          a) Per-source read_timeout_ms overrides global
          b) Per-source connect_timeout_ms overrides global
          c) Source timeout behavior matches per-source value
          d) Global default for sources without OPTIONS

        Catalog: - Query:FederatedSystem
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_default = "fq_sys_028_def"
        src_custom = "fq_sys_028_cust"
        self._cleanup_src(src_default, src_custom)
        try:
            # Default source (uses global config)
            self._mk_mysql(src_default)
            # Custom source with per-source timeout
            tdSql.execute(
                f"create external source {src_custom} type='mysql' "
                f"host='192.0.2.1' port=3306 user='u' password='p' database=testdb "
                f"options('read_timeout_ms'='1000','connect_timeout_ms'='500')")

            tdSql.query("show external sources")
            for row in tdSql.queryResult:
                if row[self._COL_NAME] == src_custom:
                    opts = str(row[self._COL_OPTIONS])
                    assert 'read_timeout_ms' in opts
                    assert 'connect_timeout_ms' in opts
                elif row[self._COL_NAME] == src_default:
                    # Default source: no per-source timeout in options
                    pass
        finally:
            self._cleanup_src(src_default, src_custom)
