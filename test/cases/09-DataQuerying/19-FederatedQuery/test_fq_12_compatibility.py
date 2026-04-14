"""
test_fq_12_compatibility.py

Implements COMP-001 through COMP-012 from TS "兼容性测试" section.

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
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_EXT_FEATURE_DISABLED,
)


class TestFq12Compatibility(FederatedQueryTestMixin):
    """COMP-001 through COMP-012: Compatibility tests."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    def _skip_external(self, msg):
        pytest.skip(f"Compatibility test {msg}")

    # ------------------------------------------------------------------
    # COMP-001  MySQL 5.7/8.0 兼容
    # ------------------------------------------------------------------

    def test_fq_comp_001_mysql_version_compat(self):
        """COMP-001: MySQL 5.7/8.0 compatibility

        TS: 核心查询与映射行为一致

        Requires MySQL 5.7 and 8.0 instances side by side.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-001

        """
        self._skip_external("requires MySQL 5.7 and 8.0 instances")

    # ------------------------------------------------------------------
    # COMP-002  PostgreSQL 12/14/16 兼容
    # ------------------------------------------------------------------

    def test_fq_comp_002_pg_version_compat(self):
        """COMP-002: PostgreSQL 12/14/16 compatibility

        TS: 核心查询与映射行为一致

        Requires PG 12, 14, 16 instances.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-002

        """
        self._skip_external("requires PostgreSQL 12, 14, and 16 instances")

    # ------------------------------------------------------------------
    # COMP-003  InfluxDB v3 兼容
    # ------------------------------------------------------------------

    def test_fq_comp_003_influxdb_v3_compat(self):
        """COMP-003: InfluxDB v3 compatibility — Flight SQL path stable

        TS: Flight SQL 路径稳定

        Requires InfluxDB v3 instance.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-003

        """
        self._skip_external("requires InfluxDB v3 instance")

    # ------------------------------------------------------------------
    # COMP-004  Linux 发行版兼容
    # ------------------------------------------------------------------

    def test_fq_comp_004_linux_distro_compat(self):
        """COMP-004: Linux distro compatibility — Ubuntu/CentOS consistent

        TS: Ubuntu/CentOS 环境行为一致

        Cross-distro test requires parallel CI on different OS images.

        Catalog:
            - Query:FederatedCompatibility

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Rewrite to match TS COMP-004

        """
        # Partial: verify parser accepts source DDL on current OS
        src = "comp004_src"
        self._cleanup(src)
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='192.0.2.1' port=3306 user='u' password='p' database='db'"
        )
        tdSql.query("show external sources")
        found = any(str(r[0]) == src for r in tdSql.queryResult)
        assert found, f"{src} should be created on current platform"
        self._cleanup(src)

    # ------------------------------------------------------------------
    # COMP-005  默认关闭兼容性
    # ------------------------------------------------------------------

    def test_fq_comp_005_default_off_compat(self):
        """COMP-005: Federated disabled — historical behavior unchanged

        TS: 关闭联邦时历史行为不变

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
    # COMP-006  升级后外部源元数据
    # ------------------------------------------------------------------

    def test_fq_comp_006_upgrade_metadata_migration(self):
        """COMP-006: Post-upgrade external source metadata usable

        TS: 升级脚本迁移后对象可用

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
    # COMP-007  升级后零数据场景
    # ------------------------------------------------------------------

    def test_fq_comp_007_upgrade_zero_data(self):
        """COMP-007: Upgrade with no federation data — smooth upgrade/downgrade

        TS: 未使用联邦时可平滑升级降级

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
    # COMP-008  升级后已写入场景
    # ------------------------------------------------------------------

    def test_fq_comp_008_upgrade_with_federation_data(self):
        """COMP-008: Upgrade with existing external source config

        TS: 已存在外部源配置时行为正确

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
    # COMP-009  函数方言兼容
    # ------------------------------------------------------------------

    def test_fq_comp_009_function_dialect_compat(self):
        """COMP-009: Function dialect cross-version stability

        TS: 关键转换函数跨版本稳定

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
        src_mysql = "comp009_mysql"
        src_pg = "comp009_pg"
        self._cleanup(src_mysql, src_pg)

        tdSql.execute(
            f"create external source {src_mysql} type='mysql' "
            f"host='192.0.2.1' port=3306 user='u' password='p' database='db'"
        )
        tdSql.execute(
            f"create external source {src_pg} type='postgresql' "
            f"host='192.0.2.1' port=5432 user='u' password='p' "
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
    # COMP-010  大小写/引号兼容
    # ------------------------------------------------------------------

    def test_fq_comp_010_case_and_quoting_compat(self):
        """COMP-010: Identifier case and quoting rules across sources

        TS: 标识符规则跨源一致

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
        lower_result = tdSql.queryResult

        tdSql.query(sql_upper)
        upper_result = tdSql.queryResult

        assert lower_result == upper_result, \
            "case-insensitive identifier results should match"

        # Verify row count
        tdSql.checkRows(3)

    # ------------------------------------------------------------------
    # COMP-011  字符集兼容
    # ------------------------------------------------------------------

    def test_fq_comp_011_charset_compat(self):
        """COMP-011: Charset compatibility — multi-language characters

        TS: 多语言字符集跨源一致

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
    # COMP-012  连接器版本矩阵
    # ------------------------------------------------------------------

    def test_fq_comp_012_connector_version_matrix(self):
        """COMP-012: Connector version matrix — mismatch startup check

        TS: 连接器版本不一致时启动校验有效

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
        # Partial: lifecycle test — create, show, alter, drop → stable
        src = "comp012_version"
        self._cleanup(src)

        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='192.0.2.1' port=3306 user='u' password='p' database='db'"
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
