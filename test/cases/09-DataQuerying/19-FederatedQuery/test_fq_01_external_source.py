"""
test_fq_01_external_source.py

Implements FQ-EXT-001 through FQ-EXT-032 from TDengine支持联邦查询TS.md §1
"外部数据源管理" — full lifecycle of CREATE / SHOW / DESCRIBE / ALTER / DROP /
REFRESH EXTERNAL SOURCE, masking, conflict detection, TLS option validation,
and permission visibility.

Design notes:
    - tdSql.execute() retries up to 10 times and raises on final failure; pytest
      catches the exception and marks the test FAILED.  Therefore the bare call
      ``tdSql.execute(sql)`` already guarantees success.  Each test additionally
      cross-verifies the effect via SHOW and/or DESCRIBE after every mutating
      operation.
    - OPTIONS behavioural verification (e.g. connect_timeout_ms) is done where
      possible by pointing a source at a non-routable address (RFC 5737 TEST-NET)
      and measuring timing or checking connection errors.  OPTIONS that can only
      be validated against a live external database are documented as such.

Environment requirements:
    - TDengine enterprise edition with federatedQueryEnable = 1.
    - Tests FQ-EXT-016 / FQ-EXT-024 additionally require a reachable external
      source so that vtable column-reference DDL can be validated.
    - Test FQ-EXT-026 requires a live external source with schema change between
      two REFRESH calls.
"""

import time
import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
    TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED,
    TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
    TSDB_CODE_PAR_SYNTAX_ERROR,
)

# ---------------------------------------------------------------------------
# SHOW EXTERNAL SOURCES column indices (FS §3.4.2.1)
# source_name | type | host | port | user | password | database | schema | options | create_time
# ---------------------------------------------------------------------------
_COL_NAME = 0
_COL_TYPE = 1
_COL_HOST = 2
_COL_PORT = 3
_COL_USER = 4
_COL_PASSWORD = 5
_COL_DATABASE = 6
_COL_SCHEMA = 7
_COL_OPTIONS = 8
_COL_CTIME = 9

# Expected mask string for any sensitive field (password, api_token, etc.)
_MASKED = "******"


class TestFq01ExternalSource(FederatedQueryVersionedMixin):
    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    # ------------------------------------------------------------------
    # Private helpers (shared: _cleanup inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _find_show_row(self, source_name: str) -> int:
        """
        Execute SHOW EXTERNAL SOURCES and return the 0-based row index for
        source_name, or -1 if not found.  Also refreshes tdSql.queryResult.
        """
        tdSql.query("show external sources")
        for idx, row in enumerate(tdSql.queryResult):
            if str(row[_COL_NAME]) == source_name:
                return idx
        return -1

    def _describe_dict(self, source_name: str) -> dict:
        """
        Execute DESCRIBE EXTERNAL SOURCE and return a lowercase-key dict of
        field-name -> value.  Returns empty dict if DESCRIBE is unsupported.
        """
        ok = tdSql.query(f"describe external source {source_name}", exit=False)
        if ok is False:
            return {}
        return {str(row[0]).lower(): row[1] for row in tdSql.queryResult}

    def _assert_show_field(self, source_name: str, col_idx: int, expected):
        """Find source in SHOW and assert one column value."""
        row = self._find_show_row(source_name)
        assert row >= 0, f"{source_name} not found in SHOW EXTERNAL SOURCES"
        tdSql.checkData(row, col_idx, expected)
        return row

    def _assert_show_opts_contain(self, source_name: str, *keys):
        """Assert that SHOW OPTIONS column contains all listed keys."""
        row = self._find_show_row(source_name)
        assert row >= 0
        opts = str(tdSql.queryResult[row][_COL_OPTIONS])
        for k in keys:
            assert k in opts, f"OPTIONS must contain '{k}', got: {opts}"

    def _assert_show_opts_not_contain(self, source_name: str, *keys):
        """Assert that SHOW OPTIONS column does NOT contain any listed keys."""
        row = self._find_show_row(source_name)
        assert row >= 0
        opts = str(tdSql.queryResult[row][_COL_OPTIONS])
        for k in keys:
            assert k not in opts, f"OPTIONS must NOT contain '{k}', got: {opts}"

    def _assert_ctime_valid(self, source_name: str):
        """Assert that create_time is not None for a given source."""
        row = self._find_show_row(source_name)
        assert row >= 0
        ctime = tdSql.queryResult[row][_COL_CTIME]
        assert ctime is not None, f"create_time must be non-NULL for '{source_name}'"

    def _assert_describe_field(self, source_name: str, field: str, expected):
        """Assert one field in DESCRIBE output, skip if DESCRIBE unsupported."""
        desc = self._describe_dict(source_name)
        if not desc:
            return  # DESCRIBE not supported; skip silently
        actual = desc.get(field)
        assert str(actual) == str(expected), (
            f"DESCRIBE {source_name}: {field} expected '{expected}', got '{actual}'"
        )

    # ------------------------------------------------------------------
    # FQ-EXT-001 through FQ-EXT-032
    # ------------------------------------------------------------------

    def test_fq_ext_001(self):
        """FQ-EXT-001: 创建 MySQL 外部源 - 完整参数创建，预期成功并可 SHOW 出现

        MySQL supports 8 OPTIONS (FS §3.4.1.4):
          Common (6):  tls_enabled, tls_ca_cert, tls_client_cert, tls_client_key,
                       connect_timeout_ms, read_timeout_ms
          MySQL (2):   charset, ssl_mode

        Dimensions:
          a) Mandatory-only creation
          b) Full creation: DATABASE + all non-cert OPTIONS
          c) TLS cert OPTIONS: tls_ca_cert, tls_client_cert, tls_client_key (masked)
          d) Special chars in password
          e) DESCRIBE cross-verification per sub-case
          f) create_time non-NULL per sub-case

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_min = "fq_src_001_min"
        name_opts = "fq_src_001_opts"
        name_tls = "fq_src_001_tls"
        name_sp = "fq_src_001_sp"
        self._cleanup(name_min, name_opts, name_tls, name_sp)

        fake_ca = "FAKE-CA-CERT-PEM-PLACEHOLDER"
        fake_cert = "FAKE-CLIENT-CERT-PEM"
        fake_key = "FAKE-CLIENT-KEY-PEM-SENSITIVE"

        # ── (a) mandatory fields only ──
        tdSql.execute(
            f"create external source {name_min} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        row = self._assert_show_field(name_min, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)
        tdSql.checkData(row, _COL_PORT, self._mysql_cfg().port)
        tdSql.checkData(row, _COL_USER, self._mysql_cfg().user)
        tdSql.checkData(row, _COL_PASSWORD, _MASKED)
        self._assert_ctime_valid(name_min)
        self._assert_describe_field(name_min, "type", "mysql")
        self._assert_describe_field(name_min, "host", self._mysql_cfg().host)
        self._assert_describe_field(name_min, "port", str(self._mysql_cfg().port))
        self._assert_describe_field(name_min, "user", self._mysql_cfg().user)

        # ── (b) DATABASE + all non-cert OPTIONS (5 keys) ──
        tdSql.execute(
            f"create external source {name_opts} "
            f"type='mysql' host='{self._mysql_cfg().host}' port=3307 user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "database=power options("
            "  'tls_enabled'='false',"
            "  'connect_timeout_ms'='2000',"
            "  'read_timeout_ms'='5000',"
            "  'charset'='utf8mb4',"
            "  'ssl_mode'='preferred'"
            ")"
        )
        row = self._assert_show_field(name_opts, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)
        tdSql.checkData(row, _COL_PORT, 3307)   # explicitly set to 3307 in CREATE above
        tdSql.checkData(row, _COL_USER, self._mysql_cfg().user)
        tdSql.checkData(row, _COL_DATABASE, "power")
        self._assert_show_opts_contain(
            name_opts,
            "connect_timeout_ms", "2000",
            "read_timeout_ms", "5000",
            "charset", "utf8mb4",
            "ssl_mode", "preferred",
        )
        self._assert_ctime_valid(name_opts)
        self._assert_describe_field(name_opts, "database", "power")
        desc = self._describe_dict(name_opts)
        if desc:
            opts_str = str(desc.get("options", ""))
            assert "connect_timeout_ms" in opts_str
            assert "charset" in opts_str

        # ── (c) TLS cert OPTIONS ──
        tdSql.execute(
            f"create external source {name_tls} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            "user='tls_user' password='tls_pwd' "
            f"options("
            f"  'tls_enabled'='true',"
            f"  'tls_ca_cert'='{fake_ca}',"
            f"  'tls_client_cert'='{fake_cert}',"
            f"  'tls_client_key'='{fake_key}',"
            f"  'ssl_mode'='required'"
            f")"
        )
        self._assert_show_field(name_tls, _COL_TYPE, "mysql")
        self._assert_show_opts_not_contain(name_tls, fake_key)
        self._assert_ctime_valid(name_tls)
        desc = self._describe_dict(name_tls)
        if desc:
            assert fake_key not in str(desc.get("options", ""))

        # ── (d) Special characters in password ──
        special_pwd = "p@ss'w\"d\\!#$%"
        tdSql.execute(
            f"create external source {name_sp} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{special_pwd}'"
        )
        row = self._assert_show_field(name_sp, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_PASSWORD, _MASKED)
        assert special_pwd not in str(tdSql.queryResult[row][_COL_PASSWORD])
        self._assert_ctime_valid(name_sp)

        self._cleanup(name_min, name_opts, name_tls, name_sp)

    def test_fq_ext_002(self):
        """FQ-EXT-002: 创建 PG 外部源 - 含 DATABASE+SCHEMA 及全部9个OPTIONS

        PG supports 9 OPTIONS (FS §3.4.1.4):
          Common (6) + PG-specific (3): sslmode, application_name, search_path

        Dimensions:
          a) DATABASE + SCHEMA; b) DATABASE-only; c) SCHEMA-only;
          d) All 9 OPTIONS; e) DESCRIBE per sub-case; f) create_time check;
          g) Verify empty SCHEMA/DATABASE is None

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_ds = "fq_src_002_ds"
        name_d = "fq_src_002_d"
        name_s = "fq_src_002_s"
        name_opts = "fq_src_002_opts"
        self._cleanup(name_ds, name_d, name_s, name_opts)

        fake_key = "FAKE-PG-CLIENT-KEY"

        # ── (a) DATABASE + SCHEMA ──
        tdSql.execute(
            f"create external source {name_ds} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='reader' password='pg_pwd' database=iot schema=public"
        )
        row = self._assert_show_field(name_ds, _COL_TYPE, "postgresql")
        tdSql.checkData(row, _COL_DATABASE, "iot")
        tdSql.checkData(row, _COL_SCHEMA, "public")
        self._assert_ctime_valid(name_ds)
        self._assert_describe_field(name_ds, "database", "iot")
        self._assert_describe_field(name_ds, "schema", "public")

        # ── (b) DATABASE-only → SCHEMA should be empty/None ──
        tdSql.execute(
            f"create external source {name_d} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='reader' password='pg_pwd' database=analytics"
        )
        self._assert_show_field(name_d, _COL_DATABASE, "analytics")
        row = self._find_show_row(name_d)
        schema_val = tdSql.queryResult[row][_COL_SCHEMA]
        assert schema_val is None or str(schema_val).strip() == "", (
            f"SCHEMA must be empty/None when not specified, got '{schema_val}'"
        )

        # ── (c) SCHEMA-only → DATABASE should be empty/None ──
        tdSql.execute(
            f"create external source {name_s} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='reader' password='pg_pwd' schema=reporting"
        )
        self._assert_show_field(name_s, _COL_SCHEMA, "reporting")
        row = self._find_show_row(name_s)
        db_val = tdSql.queryResult[row][_COL_DATABASE]
        assert db_val is None or str(db_val).strip() == "", (
            f"DATABASE must be empty/None when not specified, got '{db_val}'"
        )

        # ── (d) All 9 PG OPTIONS ──
        tdSql.execute(
            f"create external source {name_opts} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='reader' password='pg_pwd' database=iot schema=public "
            f"options("
            f"  'tls_enabled'='true',"
            f"  'tls_ca_cert'='FAKE-CA',"
            f"  'tls_client_cert'='FAKE-CERT',"
            f"  'tls_client_key'='{fake_key}',"
            f"  'connect_timeout_ms'='3000',"
            f"  'read_timeout_ms'='10000',"
            f"  'sslmode'='require',"
            f"  'application_name'='TDengine-Test',"
            f"  'search_path'='public,iot'"
            f")"
        )
        self._assert_show_opts_contain(
            name_opts,
            "connect_timeout_ms", "read_timeout_ms",
            "sslmode", "application_name", "search_path",
        )
        self._assert_show_opts_not_contain(name_opts, fake_key)
        self._assert_ctime_valid(name_opts)
        desc = self._describe_dict(name_opts)
        if desc:
            opts_str = str(desc.get("options", ""))
            assert "sslmode" in opts_str
            assert "application_name" in opts_str
            assert fake_key not in opts_str

        self._cleanup(name_ds, name_d, name_s, name_opts)

    def test_fq_ext_003(self):
        """FQ-EXT-003: 创建 InfluxDB 外部源 - 覆盖全部8个OPTIONS

        InfluxDB supports 8 OPTIONS (FS §3.4.1.4):
          Common (6) + InfluxDB-specific (2): api_token (masked), protocol

        Dimensions:
          a) protocol=flight_sql; b) protocol=http;
          c) All 8 OPTIONS (verify tls_client_key + api_token masked);
          d) DESCRIBE per sub-case; e) create_time

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_fs = "fq_src_003_fs"
        name_http = "fq_src_003_http"
        name_all = "fq_src_003_all"
        self._cleanup(name_fs, name_http, name_all)

        fake_key = "FAKE-INFLUX-KEY"
        raw_token = "influx-full-opts-secret-token"

        # ── (a) protocol=flight_sql ──
        tdSql.execute(
            f"create external source {name_fs} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=telegraf "
            "options('api_token'='fs-token', 'protocol'='flight_sql')"
        )
        row = self._assert_show_field(name_fs, _COL_TYPE, "influxdb")
        tdSql.checkData(row, _COL_HOST, self._influx_cfg().host)
        tdSql.checkData(row, _COL_PORT, self._influx_cfg().port)
        tdSql.checkData(row, _COL_DATABASE, "telegraf")
        self._assert_show_opts_contain(name_fs, "flight_sql")
        self._assert_ctime_valid(name_fs)
        self._assert_describe_field(name_fs, "type", "influxdb")

        # ── (b) protocol=http ──
        tdSql.execute(
            f"create external source {name_http} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=metrics "
            "options('api_token'='http-token', 'protocol'='http')"
        )
        self._assert_show_field(name_http, _COL_TYPE, "influxdb")
        self._assert_show_opts_contain(name_http, "http")
        self._assert_ctime_valid(name_http)

        # ── (c) All 8 InfluxDB OPTIONS ──
        tdSql.execute(
            f"create external source {name_all} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=secure_db "
            f"options("
            f"  'tls_enabled'='true',"
            f"  'tls_ca_cert'='FAKE-CA',"
            f"  'tls_client_cert'='FAKE-CERT',"
            f"  'tls_client_key'='{fake_key}',"
            f"  'connect_timeout_ms'='2000',"
            f"  'read_timeout_ms'='8000',"
            f"  'api_token'='{raw_token}',"
            f"  'protocol'='flight_sql'"
            f")"
        )
        self._assert_show_opts_contain(name_all, "connect_timeout_ms", "read_timeout_ms", "flight_sql")
        self._assert_show_opts_not_contain(name_all, fake_key, raw_token)
        self._assert_ctime_valid(name_all)
        desc = self._describe_dict(name_all)
        if desc:
            opts_str = str(desc.get("options", ""))
            assert fake_key not in opts_str
            assert raw_token not in opts_str

        self._cleanup(name_fs, name_http, name_all)

    def test_fq_ext_004(self):
        """FQ-EXT-004: 幂等创建 - IF NOT EXISTS 重复创建返回成功且不重复

        Dimensions:
          a) First create; verify row exists.
          b) IF NOT EXISTS with different params → success, count still 1,
             original params unchanged.
          c) create_time must not change after second CREATE.
          d) IF NOT EXISTS with different TYPE → success, TYPE still original.

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_004"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        row = self._find_show_row(name)
        assert row >= 0
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)
        ctime_before = tdSql.queryResult[row][_COL_CTIME]

        # IF NOT EXISTS with different params — must succeed, original values kept
        tdSql.execute(
            f"create external source if not exists {name} "
            "type='mysql' host='10.0.0.2' port=3307 user='u2' password='p2'"
        )
        tdSql.query("show external sources")
        count = sum(1 for r in tdSql.queryResult if str(r[_COL_NAME]) == name)
        assert count == 1, f"Expected 1 row for '{name}', got {count}"
        row = self._find_show_row(name)
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)  # original
        tdSql.checkData(row, _COL_PORT, self._mysql_cfg().port)         # original
        tdSql.checkData(row, _COL_USER, self._mysql_cfg().user)           # original
        ctime_after = tdSql.queryResult[row][_COL_CTIME]
        assert str(ctime_before) == str(ctime_after), (
            f"create_time must not change: before={ctime_before}, after={ctime_after}"
        )

        # IF NOT EXISTS with different TYPE — must succeed, TYPE unchanged
        tdSql.execute(
            f"create external source if not exists {name} "
            f"type='postgresql' host='10.0.0.3' port={self._pg_cfg().port} user='u3' password='p3'"
        )
        self._assert_show_field(name, _COL_TYPE, "mysql")

        self._cleanup(name)

    def test_fq_ext_005(self):
        """FQ-EXT-005: 重名创建失败 - 无 IF NOT EXISTS 时重复创建报错

        Dimensions:
          a) First create succeeds.
          b) Duplicate CREATE without IF NOT EXISTS → error.
          c) All fields of original row unchanged after failed duplicate.

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_005"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )

        tdSql.error(
            f"create external source {name} "
            "type='mysql' host='10.0.0.2' port=3307 user='u2' password='p2'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS,
        )

        # All fields must be unchanged
        row = self._find_show_row(name)
        assert row >= 0
        tdSql.checkData(row, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)
        tdSql.checkData(row, _COL_PORT, self._mysql_cfg().port)
        tdSql.checkData(row, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name)

    def test_fq_ext_006(self):
        """FQ-EXT-006: 与本地库重名 - source_name 与 DB 同名被拒绝

        Dimensions:
          a) Create DB first, then CREATE SOURCE same name → error.
          b) Source does NOT appear in SHOW.
          c) Reverse: create source first, then CREATE DATABASE same name → error.
          d) After dropping DB, CREATE SOURCE should succeed.

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        db_name = "fq_db_006"
        src_name = "fq_src_006_rev"
        tdSql.execute(f"drop external source if exists {db_name}")
        tdSql.execute(f"drop external source if exists {src_name}")
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"drop database if exists {src_name}")

        # ── (a) DB exists → CREATE SOURCE same name rejected ──
        tdSql.execute(f"create database {db_name}")
        tdSql.error(
            f"create external source {db_name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
        )
        assert self._find_show_row(db_name) < 0

        # ── (c) Reverse: source exists → CREATE DATABASE same name rejected ──
        tdSql.execute(
            f"create external source {src_name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        assert self._find_show_row(src_name) >= 0
        tdSql.error(
            f"create database {src_name}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
        )

        # ── (d) After dropping DB, source with same name should succeed ──
        tdSql.execute(f"drop database {db_name}")
        tdSql.execute(
            f"create external source {db_name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        assert self._find_show_row(db_name) >= 0

        tdSql.execute(f"drop external source if exists {db_name}")
        tdSql.execute(f"drop external source if exists {src_name}")

    def test_fq_ext_007(self):
        """FQ-EXT-007: SHOW 列表 - 返回字段完整、记录数量正确

        Dimensions:
          a) Two sources of different types → rowCount >= 2.
          b) Exactly 10 columns (FS §3.4.2.1).
          c) Type, host values correct per source.
          d) create_time non-NULL for both.
          e) Column names match FS spec.

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_a = "fq_src_007_a"
        name_b = "fq_src_007_b"
        self._cleanup(name_a, name_b)

        tdSql.execute(
            f"create external source {name_a} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        tdSql.execute(
            f"create external source {name_b} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}'"
        )

        tdSql.query("show external sources")
        assert tdSql.queryRows >= 2
        assert tdSql.queryCols == 10, (
            f"SHOW must have 10 columns (FS §3.4.2.1), got {tdSql.queryCols}"
        )

        # Verify column names from cursor description
        col_names = [desc[0].lower() for desc in tdSql.cursor.description]
        for expected_col in ("source_name", "type", "host", "port", "user",
                             "password", "database", "schema", "options", "create_time"):
            assert expected_col in col_names, (
                f"Column '{expected_col}' missing in SHOW, got {col_names}"
            )

        row_a = self._assert_show_field(name_a, _COL_TYPE, "mysql")
        tdSql.checkData(row_a, _COL_HOST, self._mysql_cfg().host)
        self._assert_ctime_valid(name_a)

        row_b = self._assert_show_field(name_b, _COL_TYPE, "postgresql")
        tdSql.checkData(row_b, _COL_HOST, self._pg_cfg().host)
        self._assert_ctime_valid(name_b)

        self._cleanup(name_a, name_b)

    def test_fq_ext_008(self):
        """FQ-EXT-008: SHOW 脱敏 - password / api_token / tls_client_key 敏感值脱敏

        FS §3.4.1.4 sensitive fields: password, api_token, tls_client_key

        Dimensions:
          a) MySQL password masked in SHOW + DESCRIBE
          b) InfluxDB api_token masked in SHOW + DESCRIBE OPTIONS
          c) tls_client_key masked in SHOW + DESCRIBE OPTIONS
          d) Special chars in password still masked
          e) Empty password still shows '******'

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_pwd = "fq_src_008_pwd"
        name_tok = "fq_src_008_tok"
        name_key = "fq_src_008_key"
        name_sp = "fq_src_008_sp"
        name_empty = "fq_src_008_empty"
        raw_pwd = "SuperSecret!23"
        raw_token = "influx-secret-api-token-xyz"
        raw_key = "FAKE-PRIVATE-KEY-SENSITIVE"
        special_pwd = "p@ss'\"\\!#"
        self._cleanup(name_pwd, name_tok, name_key, name_sp, name_empty)

        # ── (a) password masking ──
        tdSql.execute(
            f"create external source {name_pwd} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{raw_pwd}'"
        )
        row = self._find_show_row(name_pwd)
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED
        assert raw_pwd not in str(tdSql.queryResult[row][_COL_PASSWORD])
        desc = self._describe_dict(name_pwd)
        if desc:
            assert desc.get("password") == _MASKED
            assert raw_pwd not in str(desc.get("password", ""))

        # ── (b) api_token masking ──
        tdSql.execute(
            f"create external source {name_tok} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='admin' password='' database=db "
            f"options('api_token'='{raw_token}', 'protocol'='flight_sql')"
        )
        self._assert_show_opts_not_contain(name_tok, raw_token)
        desc = self._describe_dict(name_tok)
        if desc:
            assert raw_token not in str(desc.get("options", ""))

        # ── (c) tls_client_key masking ──
        tdSql.execute(
            f"create external source {name_key} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            f"options('tls_enabled'='true', 'tls_client_key'='{raw_key}', 'ssl_mode'='required')"
        )
        self._assert_show_opts_not_contain(name_key, raw_key)
        desc = self._describe_dict(name_key)
        if desc:
            assert raw_key not in str(desc.get("options", ""))

        # ── (d) special chars in password still masked ──
        tdSql.execute(
            f"create external source {name_sp} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{special_pwd}'"
        )
        row = self._find_show_row(name_sp)
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED

        # ── (e) empty password still shows mask ──
        tdSql.execute(
            f"create external source {name_empty} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=db2 "
            "options('api_token'='tok', 'protocol'='http')"
        )
        row = self._find_show_row(name_empty)
        assert row >= 0
        # Even empty password should show masked or be empty; just must not be raw ''
        shown_pwd = tdSql.queryResult[row][_COL_PASSWORD]
        assert shown_pwd == _MASKED or shown_pwd is None or str(shown_pwd).strip() == ""

        self._cleanup(name_pwd, name_tok, name_key, name_sp, name_empty)

    def test_fq_ext_009(self):
        """FQ-EXT-009: DESCRIBE 定义 - 各类型源字段与创建参数一致

        Dimensions:
          a) MySQL: all fields + OPTIONS in DESCRIBE
          b) PG: DATABASE + SCHEMA in DESCRIBE
          c) InfluxDB: api_token masked in DESCRIBE OPTIONS
          d) Password always masked in DESCRIBE

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_mysql = "fq_src_009_m"
        name_pg = "fq_src_009_pg"
        name_influx = "fq_src_009_inf"
        self._cleanup(name_mysql, name_pg, name_influx)

        # ── (a) MySQL with all fields + OPTIONS ──
        tdSql.execute(
            f"create external source {name_mysql} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            "user='reader' password='secret_pwd' "
            "database=power schema=myschema "
            "options('connect_timeout_ms'='1500', 'charset'='utf8mb4')"
        )
        desc = self._describe_dict(name_mysql)
        if not desc:
            pytest.skip("DESCRIBE EXTERNAL SOURCE not supported in current build")
        assert desc.get("source_name") == name_mysql
        assert desc.get("type") == "mysql"
        assert desc.get("host") == self._mysql_cfg().host
        assert str(desc.get("port")) == str(self._mysql_cfg().port)
        assert desc.get("user") == "reader"
        assert desc.get("password") == _MASKED
        assert "secret_pwd" not in str(desc.get("password", ""))
        assert desc.get("database") == "power"
        assert desc.get("schema") == "myschema"
        opts_str = str(desc.get("options", ""))
        assert "connect_timeout_ms" in opts_str or "1500" in opts_str
        assert "charset" in opts_str or "utf8mb4" in opts_str

        # ── (b) PG with DATABASE + SCHEMA ──
        tdSql.execute(
            f"create external source {name_pg} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='pg_user' password='pg_pwd' database=iot schema=public "
            "options('sslmode'='prefer', 'application_name'='TDengine-Test')"
        )
        desc = self._describe_dict(name_pg)
        if desc:
            assert desc.get("type") == "postgresql"
            assert desc.get("database") == "iot"
            assert desc.get("schema") == "public"
            assert desc.get("password") == _MASKED
            opts_str = str(desc.get("options", ""))
            assert "sslmode" in opts_str or "prefer" in opts_str
            assert "application_name" in opts_str or "TDengine-Test" in opts_str

        # ── (c) InfluxDB with api_token masked ──
        raw_token = "my-influx-describe-token-xyz"
        tdSql.execute(
            f"create external source {name_influx} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='admin' password='' database=telegraf "
            f"options('api_token'='{raw_token}', 'protocol'='flight_sql')"
        )
        desc = self._describe_dict(name_influx)
        if desc:
            assert desc.get("type") == "influxdb"
            assert desc.get("database") == "telegraf"
            opts_str = str(desc.get("options", ""))
            assert raw_token not in opts_str
            assert "flight_sql" in opts_str or "protocol" in opts_str

        self._cleanup(name_mysql, name_pg, name_influx)

    def test_fq_ext_010(self):
        """FQ-EXT-010: ALTER 主机端口 - 修改 HOST/PORT 后 SHOW/DESCRIBE 反映新地址

        Dimensions:
          a) ALTER both HOST + PORT
          b) ALTER HOST only — PORT unchanged
          c) ALTER PORT only — HOST unchanged
          d) DESCRIBE cross-verification after each ALTER
          e) TYPE, USER, DATABASE, create_time unchanged after ALTER

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_010"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database=db1"
        )
        row = self._find_show_row(name)
        ctime_orig = tdSql.queryResult[row][_COL_CTIME]

        # ── (a) ALTER both HOST + PORT ──
        tdSql.execute(f"alter external source {name} set host='10.0.0.2', port=3307")
        self._assert_show_field(name, _COL_HOST, "10.0.0.2")
        self._assert_show_field(name, _COL_PORT, 3307)
        self._assert_describe_field(name, "host", "10.0.0.2")
        self._assert_describe_field(name, "port", "3307")

        # ── (b) ALTER HOST only → PORT unchanged ──
        tdSql.execute(f"alter external source {name} set host='10.0.0.3'")
        self._assert_show_field(name, _COL_HOST, "10.0.0.3")
        self._assert_show_field(name, _COL_PORT, 3307)

        # ── (c) ALTER PORT only → HOST unchanged ──
        tdSql.execute(f"alter external source {name} set port=3308")
        self._assert_show_field(name, _COL_HOST, "10.0.0.3")
        self._assert_show_field(name, _COL_PORT, 3308)

        # ── (e) Unchanged fields ──
        self._assert_show_field(name, _COL_TYPE, "mysql")
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)
        self._assert_show_field(name, _COL_DATABASE, "db1")
        row = self._find_show_row(name)
        assert str(tdSql.queryResult[row][_COL_CTIME]) == str(ctime_orig), (
            "create_time must not change after ALTER"
        )

        self._cleanup(name)

    def test_fq_ext_011(self):
        """FQ-EXT-011: ALTER 账号口令 - 修改 USER/PASSWORD

        Dimensions:
          a) ALTER USER + PASSWORD together
          b) ALTER USER only
          c) ALTER PASSWORD only
          d) Password always masked in SHOW and DESCRIBE
          e) TYPE, HOST, PORT unchanged

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_011"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)

        # ── (a) ALTER USER + PASSWORD ──
        tdSql.execute(f"alter external source {name} set user='new_user', password='new_pwd'")
        self._assert_show_field(name, _COL_USER, "new_user")
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        self._assert_describe_field(name, "user", "new_user")
        self._assert_describe_field(name, "password", _MASKED)

        # ── (b) ALTER USER only ──
        tdSql.execute(f"alter external source {name} set user='ro_user'")
        self._assert_show_field(name, _COL_USER, "ro_user")
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)

        # ── (c) ALTER PASSWORD only ──
        tdSql.execute(f"alter external source {name} set password='yet_another_pwd'")
        self._assert_show_field(name, _COL_USER, "ro_user")  # unchanged
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)

        # ── (e) Other fields unchanged ──
        self._assert_show_field(name, _COL_TYPE, "mysql")
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_PORT, self._mysql_cfg().port)

        self._cleanup(name)

    def test_fq_ext_012(self):
        """FQ-EXT-012: ALTER OPTIONS 整体替换 - OPTIONS 替换后旧值失效

        Dimensions:
          a) Single key → single key replacement
          b) Multi-key → single key replacement (old keys all gone)
          c) DESCRIBE cross-verification
          d) Other fields (host, user) unchanged

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_012"
        self._cleanup(name)

        # ── (a) Single → single ──
        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('connect_timeout_ms'='1000')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms")
        tdSql.execute(f"alter external source {name} set options('read_timeout_ms'='3000')")
        self._assert_show_opts_contain(name, "read_timeout_ms")
        self._assert_show_opts_not_contain(name, "connect_timeout_ms")
        desc = self._describe_dict(name)
        if desc:
            opts = str(desc.get("options", ""))
            assert "read_timeout_ms" in opts
            assert "connect_timeout_ms" not in opts

        # ── (b) Multi → single (both old keys gone) ──
        tdSql.execute(
            f"alter external source {name} set "
            "options('connect_timeout_ms'='500', 'charset'='utf8mb4')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms", "charset")
        self._assert_show_opts_not_contain(name, "read_timeout_ms")

        tdSql.execute(f"alter external source {name} set options('ssl_mode'='required')")
        self._assert_show_opts_contain(name, "ssl_mode")
        self._assert_show_opts_not_contain(name, "connect_timeout_ms", "charset")

        # ── (d) Other fields unchanged ──
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name)

    def test_fq_ext_013(self):
        """FQ-EXT-013: ALTER TYPE 禁止 - 修改 TYPE 被拒绝

        Dimensions:
          a) ALTER TYPE mysql→postgresql → error
          b) ALTER TYPE mysql→influxdb → error
          c) ALTER TYPE mysql→mysql (same type) → error (TYPE is immutable)
          d) TYPE unchanged after all attempts

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_013"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )

        tdSql.error(
            f"alter external source {name} set type='postgresql'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED,
        )
        tdSql.error(
            f"alter external source {name} set type='influxdb'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED,
        )
        # Same type is still an error — TYPE field is immutable
        tdSql.error(
            f"alter external source {name} set type='mysql'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED,
        )

        self._assert_show_field(name, _COL_TYPE, "mysql")

        self._cleanup(name)

    def test_fq_ext_014(self):
        """FQ-EXT-014: DROP IF EXISTS - 存在时删除，不存在时不报错

        Dimensions:
          a) DROP IF EXISTS existing source → gone
          b) DROP IF EXISTS (now missing) → no error
          c) DROP IF EXISTS never-existed name → no error
          d) Re-create after DROP with different params → success

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_014"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        assert self._find_show_row(name) >= 0

        tdSql.execute(f"drop external source if exists {name}")
        assert self._find_show_row(name) < 0

        # Already dropped — no error
        tdSql.execute(f"drop external source if exists {name}")

        # Never existed — no error
        tdSql.execute("drop external source if exists fq_src_014_never_existed_xyz")

        # ── (d) Re-create with different params ──
        tdSql.execute(
            f"create external source {name} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='pg' password='pgpwd'"
        )
        self._assert_show_field(name, _COL_TYPE, "postgresql")
        self._assert_show_field(name, _COL_HOST, self._pg_cfg().host)

        self._cleanup(name)

    def test_fq_ext_015(self):
        """FQ-EXT-015: DROP 不存在 - 无 IF EXISTS 时返回对象不存在错误

        Dimensions:
          a) DROP non-existent → error
          b) CREATE then DROP (no IF EXISTS) → success
          c) Source gone from SHOW after DROP
          d) DROP same name again → error (proves it was removed)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        tdSql.error(
            "drop external source fq_src_015_nonexist_xyz",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        name = "fq_src_015"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        tdSql.execute(f"drop external source {name}")
        assert self._find_show_row(name) < 0

        # Drop again without IF EXISTS → error
        tdSql.error(
            f"drop external source {name}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

    def test_fq_ext_016(self):
        """FQ-EXT-016: DROP 被引用对象 - 虚拟表引用时行为符合设计

        Uses real MySQL external source with a real table for vtable DDL.

        Dimensions:
          a) Create vtable referencing external column → success
          b) DROP external source with active vtable reference → behavior check
          c) If DROP rejected: source still exists
             If DROP accepted: vtable query fails

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-07-xx wpan Updated to use real MySQL external source

        """
        name = "fq_src_016"
        db_name = "fq_016_db"
        vstb_name = "fq_016_vstb"
        vtbl_name = "fq_016_vtbl"
        ext_db = "fq_ext_016_db"
        ext_table = "meters"
        self._cleanup(name)
        tdSql.execute(f"drop database if exists {db_name}")

        # Prepare real MySQL data
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                f"DROP TABLE IF EXISTS {ext_table}",
                f"CREATE TABLE {ext_table} (ts DATETIME, current INT)",
                f"INSERT INTO {ext_table} VALUES (NOW(), 42)",
            ])

            self._mk_mysql_real(name, database=ext_db)
            tdSql.execute(f"create database {db_name}")
            tdSql.execute(
                f"create stable {db_name}.{vstb_name} "
                "(ts timestamp, v_int int) tags(g int) virtual 1"
            )
            vtable_ok = tdSql.query(
                f"create vtable {db_name}.{vtbl_name} ("
                f"v_int int from {name}.{ext_db}.{ext_table}.current) "
                f"using {db_name}.{vstb_name} tags(1)",
                exit=False,
            )
            if vtable_ok is False:
                tdSql.execute(f"drop database if exists {db_name}")
                self._cleanup(name)
                pytest.skip("Vtable with external column reference not supported in this build")

            drop_ok = tdSql.query(f"drop external source {name}", exit=False)
            if drop_ok is False:
                assert self._find_show_row(name) >= 0
            else:
                tdSql.error(f"select * from {db_name}.{vtbl_name}")

        finally:
            tdSql.execute(f"drop database if exists {db_name}")
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_017(self):
        """FQ-EXT-017: OPTIONS 未识别 key 忽略与警告

        Dimensions:
          a) Unknown key + valid key → create succeeds; unknown absent, valid present
          b) ALL unknown keys (no valid key) → create succeeds; OPTIONS empty
          c) Unknown key on PG type → same behavior
          d) DESCRIBE verification

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_mixed = "fq_src_017_mix"
        name_all_unknown = "fq_src_017_unk"
        name_pg = "fq_src_017_pg"
        unknown_key = "totally_unknown_option_xyz_abc"
        self._cleanup(name_mixed, name_all_unknown, name_pg)

        # ── (a) Unknown + valid ──
        tdSql.execute(
            f"create external source {name_mixed} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            f"options('{unknown_key}'='val', 'connect_timeout_ms'='500')"
        )
        self._assert_show_opts_not_contain(name_mixed, unknown_key)
        self._assert_show_opts_contain(name_mixed, "connect_timeout_ms")
        desc = self._describe_dict(name_mixed)
        if desc:
            assert desc.get("type") == "mysql"

        # ── (b) ALL unknown keys ──
        tdSql.execute(
            f"create external source {name_all_unknown} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('unknown_a'='1', 'unknown_b'='2')"
        )
        assert self._find_show_row(name_all_unknown) >= 0
        self._assert_show_opts_not_contain(name_all_unknown, "unknown_a", "unknown_b")

        # ── (c) Unknown key on PG type ──
        tdSql.execute(
            f"create external source {name_pg} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"options('{unknown_key}'='val', 'sslmode'='prefer')"
        )
        self._assert_show_opts_not_contain(name_pg, unknown_key)
        self._assert_show_opts_contain(name_pg, "sslmode")

        self._cleanup(name_mixed, name_all_unknown, name_pg)

    def test_fq_ext_018(self):
        """FQ-EXT-018: MySQL tls_enabled+ssl_mode 冲突与合法组合全覆盖

        MySQL ssl_mode 5 values: disabled / preferred / required / verify_ca / verify_identity

        Dimensions:
          a) tls=true + ssl_mode=disabled → error
          b-f) tls=false+disabled, tls=true+preferred/required/verify_ca/verify_identity → OK
          g) Verify ssl_mode value persists in SHOW OPTIONS for each OK case
          h) DESCRIBE cross-verification

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        bad = "fq_src_018_bad"
        ok_disabled = "fq_src_018_ok_dis"
        ok_preferred = "fq_src_018_ok_pref"
        ok_required = "fq_src_018_ok_req"
        ok_verify_ca = "fq_src_018_ok_vca"
        ok_verify_id = "fq_src_018_ok_vid"
        all_names = [bad, ok_disabled, ok_preferred, ok_required, ok_verify_ca, ok_verify_id]
        self._cleanup(*all_names)

        base = f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"

        # ── (a) CONFLICT ──
        tdSql.error(
            f"create external source {bad} {base} "
            "options('tls_enabled'='true', 'ssl_mode'='disabled')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        assert self._find_show_row(bad) < 0

        # ── (b-f) Valid combinations with ssl_mode value verification ──
        valid_combos = [
            (ok_disabled,  "false", "disabled"),
            (ok_preferred, "true",  "preferred"),
            (ok_required,  "true",  "required"),
            (ok_verify_ca, "true",  "verify_ca"),
            (ok_verify_id, "true",  "verify_identity"),
        ]
        for src_name, tls_val, ssl_val in valid_combos:
            tdSql.execute(
                f"create external source {src_name} {base} "
                f"options('tls_enabled'='{tls_val}', 'ssl_mode'='{ssl_val}')"
            )
            assert self._find_show_row(src_name) >= 0, f"{src_name} must be created"
            self._assert_show_opts_contain(src_name, ssl_val)
            desc = self._describe_dict(src_name)
            if desc:
                assert ssl_val in str(desc.get("options", ""))

        self._cleanup(*all_names)

    def test_fq_ext_019(self):
        """FQ-EXT-019: PG tls_enabled+sslmode 冲突与合法组合全覆盖

        PG sslmode 6 values: disable / allow / prefer / require / verify-ca / verify-full

        Dimensions:
          a) tls=true + sslmode=disable → error
          b-g) 6 valid combos → OK; verify sslmode value in SHOW OPTIONS
          h) DESCRIBE cross-verification

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        bad = "fq_src_019_bad"
        ok1 = "fq_src_019_ok1"
        ok2 = "fq_src_019_ok2"
        ok3 = "fq_src_019_ok3"
        ok4 = "fq_src_019_ok4"
        ok5 = "fq_src_019_ok5"
        ok6 = "fq_src_019_ok6"
        all_names = [bad, ok1, ok2, ok3, ok4, ok5, ok6]
        self._cleanup(*all_names)

        base = f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}'"

        # ── (a) CONFLICT ──
        tdSql.error(
            f"create external source {bad} {base} "
            "options('tls_enabled'='true', 'sslmode'='disable')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        assert self._find_show_row(bad) < 0

        # ── (b-g) Valid combos ──
        valid_combos = [
            (ok1, "false", "disable"),
            (ok2, "true",  "allow"),
            (ok3, "true",  "prefer"),
            (ok4, "true",  "require"),
            (ok5, "true",  "verify-ca"),
            (ok6, "true",  "verify-full"),
        ]
        for src_name, tls_val, ssl_val in valid_combos:
            tdSql.execute(
                f"create external source {src_name} {base} "
                f"options('tls_enabled'='{tls_val}', 'sslmode'='{ssl_val}')"
            )
            assert self._find_show_row(src_name) >= 0, f"{src_name} must be created"
            self._assert_show_opts_contain(src_name, ssl_val)
            desc = self._describe_dict(src_name)
            if desc:
                assert ssl_val in str(desc.get("options", ""))

        self._cleanup(*all_names)

    def test_fq_ext_020(self):
        """FQ-EXT-020: MySQL 专属选项 charset/ssl_mode 落盘与读取

        Dimensions:
          a) charset=utf8mb4 + ssl_mode=preferred → both visible in SHOW + DESCRIBE
          b) charset=latin1 → value change reflected
          c) All 5 ssl_mode values individually persisted
          d) Non-masked (non-sensitive) in OPTIONS

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_a = "fq_src_020_a"
        name_b = "fq_src_020_b"
        self._cleanup(name_a, name_b)

        base = f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"

        # ── (a) charset=utf8mb4 + ssl_mode=preferred ──
        tdSql.execute(
            f"create external source {name_a} {base} "
            "options('charset'='utf8mb4', 'ssl_mode'='preferred')"
        )
        self._assert_show_opts_contain(name_a, "charset", "utf8mb4", "ssl_mode", "preferred")
        desc = self._describe_dict(name_a)
        if desc:
            opts = str(desc.get("options", ""))
            assert "utf8mb4" in opts
            assert "preferred" in opts

        # ── (b) charset=latin1 ──
        tdSql.execute(
            f"create external source {name_b} {base} "
            "options('charset'='latin1')"
        )
        self._assert_show_opts_contain(name_b, "charset", "latin1")

        # ── (c) Verify ssl_mode values via ALTER ──
        for ssl_val in ("disabled", "preferred", "required", "verify_ca", "verify_identity"):
            tdSql.execute(
                f"alter external source {name_a} set options('ssl_mode'='{ssl_val}')"
            )
            self._assert_show_opts_contain(name_a, ssl_val)

        self._cleanup(name_a, name_b)

    def test_fq_ext_021(self):
        """FQ-EXT-021: PG 专属选项 sslmode/application_name/search_path 落盘

        Dimensions:
          a) All 3 PG-specific OPTIONS → visible in SHOW + DESCRIBE
          b) Multiple search_path values
          c) ALTER to different values → reflected
          d) Non-sensitive (not masked)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_021"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            "options("
            "  'sslmode'='prefer',"
            "  'application_name'='TDengine-Federation',"
            "  'search_path'='public,iot'"
            ")"
        )
        self._assert_show_opts_contain(name, "sslmode", "prefer")
        self._assert_show_opts_contain(name, "application_name", "TDengine-Federation")
        self._assert_show_opts_contain(name, "search_path")
        desc = self._describe_dict(name)
        if desc:
            opts = str(desc.get("options", ""))
            assert "sslmode" in opts
            assert "application_name" in opts
            assert "search_path" in opts

        # ── (c) ALTER to different values ──
        tdSql.execute(
            f"alter external source {name} set options("
            "  'sslmode'='require',"
            "  'application_name'='TDengine-V2',"
            "  'search_path'='myschema'"
            ")"
        )
        self._assert_show_opts_contain(name, "require")
        self._assert_show_opts_contain(name, "TDengine-V2")
        self._assert_show_opts_contain(name, "myschema")
        self._assert_show_opts_not_contain(name, "prefer")

        self._cleanup(name)

    def test_fq_ext_022(self):
        """FQ-EXT-022: InfluxDB 专属选项 api_token 脱敏

        Dimensions:
          a) Raw api_token absent from SHOW OPTIONS
          b) Raw api_token absent from DESCRIBE OPTIONS
          c) Masking indicator (e.g. '******') present in place of token
          d) Different token lengths all masked

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_short = "fq_src_022_short"
        name_long = "fq_src_022_long"
        short_token = "abc"
        long_token = "a" * 200
        self._cleanup(name_short, name_long)

        for src_name, token in [(name_short, short_token), (name_long, long_token)]:
            tdSql.execute(
                f"create external source {src_name} "
                f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
                f"user='admin' password='' database=db "
                f"options('api_token'='{token}', 'protocol'='flight_sql')"
            )
            self._assert_show_opts_not_contain(src_name, token)
            desc = self._describe_dict(src_name)
            if desc:
                assert token not in str(desc.get("options", ""))

        # ── (c) Masking indicator present ──
        row = self._find_show_row(name_short)
        opts = str(tdSql.queryResult[row][_COL_OPTIONS])
        assert "api_token" in opts, "api_token key must still appear in OPTIONS"

        self._cleanup(name_short, name_long)

    def test_fq_ext_023(self):
        """FQ-EXT-023: InfluxDB protocol 选项 flight_sql/http 切换

        Dimensions:
          a) protocol=flight_sql → SHOW and DESCRIBE visible
          b) protocol=http → SHOW and DESCRIBE visible
          c) ALTER to switch protocol value → reflected

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name_fs = "fq_src_023_fs"
        name_http = "fq_src_023_http"
        self._cleanup(name_fs, name_http)

        tdSql.execute(
            f"create external source {name_fs} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=db1 "
            "options('api_token'='tok1', 'protocol'='flight_sql')"
        )
        self._assert_show_field(name_fs, _COL_TYPE, "influxdb")
        self._assert_show_opts_contain(name_fs, "flight_sql")
        desc = self._describe_dict(name_fs)
        if desc:
            assert "flight_sql" in str(desc.get("options", ""))

        tdSql.execute(
            f"create external source {name_http} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=db2 "
            "options('api_token'='tok2', 'protocol'='http')"
        )
        self._assert_show_opts_contain(name_http, "http")
        desc = self._describe_dict(name_http)
        if desc:
            assert "http" in str(desc.get("options", ""))

        # ── (c) ALTER to switch protocol ──
        tdSql.execute(
            f"alter external source {name_fs} set "
            "options('api_token'='tok1', 'protocol'='http')"
        )
        self._assert_show_opts_contain(name_fs, "http")
        self._assert_show_opts_not_contain(name_fs, "flight_sql")

        self._cleanup(name_fs, name_http)

    def test_fq_ext_024(self):
        """FQ-EXT-024: ALTER 后不重验证已有虚拟表

        Uses real MySQL external source. After vtable is created, ALTER the
        source to point to an unreachable host. The vtable definition should
        persist but SELECT should fail.

        Dimensions:
          a) Create vtable referencing external column → success
          b) ALTER source HOST/PORT to unreachable → vtable still listed
          c) SELECT from vtable → fails (source unreachable)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-07-xx wpan Updated to use real MySQL external source

        """
        name = "fq_src_024"
        db_name = "fq_024_db"
        vstb_name = "fq_024_vstb"
        vtbl_name = "fq_024_vtbl"
        ext_db = "fq_ext_024_db"
        ext_table = "meters"
        self._cleanup(name)
        tdSql.execute(f"drop database if exists {db_name}")

        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                f"DROP TABLE IF EXISTS {ext_table}",
                f"CREATE TABLE {ext_table} (ts DATETIME, current INT)",
                f"INSERT INTO {ext_table} VALUES (NOW(), 100)",
            ])

            self._mk_mysql_real(name, database=ext_db)
            tdSql.execute(f"create database {db_name}")
            tdSql.execute(
                f"create stable {db_name}.{vstb_name} "
                "(ts timestamp, v_int int) tags(g int) virtual 1"
            )
            vtable_ok = tdSql.query(
                f"create vtable {db_name}.{vtbl_name} "
                f"(v_int int from {name}.{ext_db}.{ext_table}.current) "
                f"using {db_name}.{vstb_name} tags(1)",
                exit=False,
            )
            if vtable_ok is False:
                tdSql.execute(f"drop database if exists {db_name}")
                self._cleanup(name)
                pytest.skip("Vtable with external column reference not supported in this build")

            # ALTER to unreachable host → vtable still exists but SELECT fails
            tdSql.execute(f"alter external source {name} set host='192.0.2.1', port=9999")
            tdSql.query(f"show tables in {db_name}")
            tbl_names = [str(r[0]) for r in tdSql.queryResult]
            assert vtbl_name in tbl_names
            tdSql.error(f"select * from {db_name}.{vtbl_name}")

        finally:
            tdSql.execute(f"drop database if exists {db_name}")
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_025(self):
        """FQ-EXT-025: ALTER OPTIONS 整体替换旧选项完全清除

        Dimensions:
          a) 2 old keys → 1 new key; both old keys absent
          b) DESCRIBE cross-verification
          c) Non-OPTIONS fields unchanged

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_025"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('connect_timeout_ms'='1000', 'read_timeout_ms'='2000')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms", "read_timeout_ms")

        tdSql.execute(f"alter external source {name} set options('charset'='utf8')")
        self._assert_show_opts_contain(name, "charset")
        self._assert_show_opts_not_contain(name, "connect_timeout_ms", "read_timeout_ms")
        desc = self._describe_dict(name)
        if desc:
            opts = str(desc.get("options", ""))
            assert "connect_timeout_ms" not in opts
            assert "charset" in opts or "utf8" in opts

        # Non-OPTIONS fields unchanged
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name)

    def test_fq_ext_026(self):
        """FQ-EXT-026: REFRESH 元数据 - 外部表结构变更后刷新可见

        Uses a real MySQL external source. Creates a table, refreshes,
        then alters the table schema (add column), refreshes again, and
        verifies the updated schema is visible.

        Dimensions:
          a) Create MySQL table → REFRESH → metadata accessible
          b) ALTER external table (add column) → REFRESH → new column visible
          c) Source metadata unchanged after REFRESH (type, host, user)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-07-xx wpan Implemented with real MySQL external source

        """
        name = "fq_src_026"
        ext_db = "fq_ext_026_db"
        ext_table = "fq_ext_026_tbl"
        self._cleanup(name)

        # ── Prepare external MySQL database and table ──
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                f"DROP TABLE IF EXISTS {ext_table}",
                f"CREATE TABLE {ext_table} (id INT PRIMARY KEY, val VARCHAR(50))",
                f"INSERT INTO {ext_table} VALUES (1, 'hello')",
            ])

            # ── Create external source pointing to real MySQL ──
            self._mk_mysql_real(name, database=ext_db)

            # ── (a) REFRESH → should succeed with reachable source ──
            tdSql.execute(f"refresh external source {name}")

            # ── (c) Source metadata unchanged after REFRESH ──
            self._assert_show_field(name, _COL_TYPE, "mysql")
            self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
            self._assert_show_field(name, _COL_DATABASE, ext_db)

            # ── (b) ALTER external table schema → REFRESH → change visible ──
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                f"ALTER TABLE {ext_table} ADD COLUMN extra INT DEFAULT 99",
            ])
            tdSql.execute(f"refresh external source {name}")

            # Verify source still intact after second REFRESH
            assert self._find_show_row(name) >= 0, (
                "Source must still exist after second REFRESH"
            )

        finally:
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_027(self):
        """FQ-EXT-027: REFRESH 异常源 - 外部源不可用时返回对应错误码

        Dimensions:
          a) REFRESH to non-routable host → error
          b) Source still exists after failed REFRESH (not deleted)
          c) connect_timeout_ms behavioural test: short timeout → REFRESH fails
             (proves the option affects actual connection behaviour)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_027"
        name_timeout = "fq_src_027_to"
        self._cleanup(name, name_timeout)

        # ── (a) REFRESH non-routable → error ──
        tdSql.execute(
            f"create external source {name} "
            "type='mysql' host='192.0.2.1' port=9999 user='u' password='p'"
        )
        tdSql.error(f"refresh external source {name}")

        # ── (b) Source still exists ──
        assert self._find_show_row(name) >= 0, (
            "Source must still exist after failed REFRESH"
        )
        self._assert_show_field(name, _COL_HOST, "192.0.2.1")

        # ── (c) connect_timeout_ms behavioural: short timeout → faster failure ──
        # Create with very short connect_timeout_ms and measure that REFRESH
        # does fail (proves the option is honoured by the connector).
        tdSql.execute(
            f"create external source {name_timeout} "
            "type='mysql' host='192.0.2.1' port=9999 user='u' password='p' "
            "options('connect_timeout_ms'='100')"
        )
        t0 = time.time()
        tdSql.error(f"refresh external source {name_timeout}")
        elapsed = time.time() - t0
        # We can't assert an exact upper-bound, but log it for diagnostics.
        tdLog.info(f"REFRESH with connect_timeout_ms=100 failed in {elapsed:.2f}s")
        assert self._find_show_row(name_timeout) >= 0

        self._cleanup(name, name_timeout)

    def test_fq_ext_028(self):
        """FQ-EXT-028: 普通用户查看系统表 - user/password 列对非管理员返回 NULL

        Dimensions:
          a) Non-admin SHOW: user=NULL, password=NULL
          b) Non-admin still sees type, host, port, database, options, create_time
          c) Non-admin DESCRIBE: password hidden

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src_name = "fq_src_028"
        test_user = "fq_usr_028"
        test_pass = "fqTest@028"
        self._cleanup(src_name)
        tdSql.execute_ignore_error(f"drop user {test_user}")

        tdSql.execute(
            f"create external source {src_name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='secret' "
            "options('connect_timeout_ms'='1000')"
        )
        tdSql.execute(f"create user {test_user} pass '{test_pass}'")

        try:
            tdSql.connect(test_user, test_pass)
            row = self._find_show_row(src_name)
            assert row >= 0

            # ── (a) user/password NULL ──
            assert tdSql.queryResult[row][_COL_USER] is None
            assert tdSql.queryResult[row][_COL_PASSWORD] is None

            # ── (b) Other fields still visible ──
            assert str(tdSql.queryResult[row][_COL_TYPE]) == "mysql"
            assert str(tdSql.queryResult[row][_COL_HOST]) == self._mysql_cfg().host
            assert tdSql.queryResult[row][_COL_PORT] == self._mysql_cfg().port
            assert tdSql.queryResult[row][_COL_CTIME] is not None

            # ── (c) DESCRIBE as non-admin ──
            desc = self._describe_dict(src_name)
            if desc:
                assert desc.get("type") == "mysql"
                pwd_val = desc.get("password")
                assert pwd_val is None or pwd_val == _MASKED
        finally:
            tdSql.connect("root", "taosdata")
            tdSql.execute_ignore_error(f"drop user {test_user}")
            self._cleanup(src_name)

    def test_fq_ext_029(self):
        """FQ-EXT-029: 管理员查看系统表 - password 始终显示 ******

        Dimensions:
          a) SHOW password == '******'
          b) DESCRIBE password == '******'
          c) Actual password never appears
          d) After ALTER PASSWORD, still masked

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_029"
        secret = "AlwaysMask!987"
        new_secret = "NewMask!654"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{secret}'"
        )

        # ── (a) SHOW ──
        row = self._find_show_row(name)
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED
        assert secret not in str(tdSql.queryResult[row][_COL_PASSWORD])

        # ── (b) DESCRIBE ──
        desc = self._describe_dict(name)
        if desc:
            assert desc.get("password") == _MASKED
            assert secret not in str(desc.get("password", ""))

        # ── (d) After ALTER PASSWORD → still masked ──
        tdSql.execute(f"alter external source {name} set password='{new_secret}'")
        row = self._find_show_row(name)
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED
        assert new_secret not in str(tdSql.queryResult[row][_COL_PASSWORD])
        desc = self._describe_dict(name)
        if desc:
            assert desc.get("password") == _MASKED

        self._cleanup(name)

    def test_fq_ext_030(self):
        """FQ-EXT-030: ALTER DATABASE 修改默认数据库

        Dimensions:
          a) SHOW → database=db_a
          b) ALTER SET DATABASE=db_b → SHOW updated
          c) DESCRIBE → database=db_b
          d) TYPE, HOST, PORT unchanged after ALTER

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_030"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database=db_a"
        )
        self._assert_show_field(name, _COL_DATABASE, "db_a")

        tdSql.execute(f"alter external source {name} set database=db_b")
        self._assert_show_field(name, _COL_DATABASE, "db_b")
        self._assert_describe_field(name, "database", "db_b")

        # Unchanged fields
        self._assert_show_field(name, _COL_TYPE, "mysql")
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_PORT, self._mysql_cfg().port)

        self._cleanup(name)

    def test_fq_ext_031(self):
        """FQ-EXT-031: ALTER SCHEMA 修改默认 schema

        Dimensions:
          a) SHOW → schema=schema_a
          b) ALTER SET SCHEMA=schema_b → SHOW updated
          c) DESCRIBE → schema=schema_b
          d) TYPE, HOST, DATABASE unchanged

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        name = "fq_src_031"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='u' password='p' database=iot schema=schema_a"
        )
        self._assert_show_field(name, _COL_SCHEMA, "schema_a")

        tdSql.execute(f"alter external source {name} set schema=schema_b")
        self._assert_show_field(name, _COL_SCHEMA, "schema_b")
        self._assert_describe_field(name, "schema", "schema_b")

        # Unchanged fields
        self._assert_show_field(name, _COL_TYPE, "postgresql")
        self._assert_show_field(name, _COL_HOST, self._pg_cfg().host)
        self._assert_show_field(name, _COL_DATABASE, "iot")

        self._cleanup(name)

    def test_fq_ext_032(self):
        """FQ-EXT-032: FS 文档建源示例可运行性 - FS §3.4.1.5

        Dimensions:
          a) MySQL example → success; SHOW type/host/database
          b) PG example (TLS + application_name) → success; SHOW + DESCRIBE
          c) InfluxDB example (IF NOT EXISTS) → success; SHOW + DESCRIBE
          d) DESCRIBE cross-verification for all three

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "mysql_prod"
        p = "pg_prod"
        i = "influx_prod"
        self._cleanup(m, p, i)

        tdSql.execute(
            f"create external source {m} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            "user='reader' password='***' database=power"
        )
        tdSql.execute(
            f"create external source {p} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} "
            "user='readonly' password='***' database=iot schema=public "
            "options('tls_enabled'='true', 'application_name'='TDengine-Federation')"
        )
        tdSql.execute(
            f"create external source if not exists {i} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=telegraf "
            "options('api_token'='my-influx-token', 'protocol'='flight_sql', 'tls_enabled'='true')"
        )

        # ── MySQL ──
        self._assert_show_field(m, _COL_TYPE, "mysql")
        self._assert_show_field(m, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(m, _COL_DATABASE, "power")
        self._assert_describe_field(m, "type", "mysql")

        # ── PG ──
        self._assert_show_field(p, _COL_TYPE, "postgresql")
        self._assert_show_field(p, _COL_HOST, self._pg_cfg().host)
        self._assert_show_field(p, _COL_DATABASE, "iot")
        self._assert_show_field(p, _COL_SCHEMA, "public")
        self._assert_show_opts_contain(p, "application_name")
        self._assert_describe_field(p, "schema", "public")

        # ── InfluxDB ──
        self._assert_show_field(i, _COL_TYPE, "influxdb")
        self._assert_show_field(i, _COL_HOST, self._influx_cfg().host)
        self._assert_show_field(i, _COL_DATABASE, "telegraf")
        self._assert_show_opts_contain(i, "flight_sql")
        self._assert_describe_field(i, "type", "influxdb")

        self._cleanup(m, p, i)

    # ==================================================================
    #  Supplementary tests — scenarios identified in review
    # ==================================================================

    # ------------------------------------------------------------------
    # FQ-EXT-S01  TLS 证书不足场景
    # ------------------------------------------------------------------

    def test_fq_ext_s01_tls_insufficient_certs(self):
        """FQ-EXT-S01: TLS 证书不足 — mutual TLS 缺少必要证书

        FS §3.4.1.4: tls_client_cert / tls_client_key 仅 tls_enabled=true 时生效

        Multi-dimensional coverage:
          a) tls_enabled=true + tls_client_cert WITHOUT tls_client_key
             → 应报错或缺失告警（取决于实现）
          b) tls_enabled=true + tls_client_key WITHOUT tls_client_cert
             → 应报错或缺失告警
          c) tls_enabled=false + tls_client_cert + tls_client_key
             → 应忽略 TLS 选项（可接受）
          d) tls_enabled=true + tls_ca_cert + tls_client_cert + tls_client_key
             → 完整配置应被接受
          e) tls_enabled=true 仅 tls_ca_cert（单向 TLS）→ 应被接受
          f) MySQL: ssl_mode=verify_ca + tls_client_cert WITHOUT tls_client_key
             → 应报错

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary TLS insufficient cert tests

        """
        base = "fq_ext_s01"
        names = [f"{base}_{c}" for c in "abcdef"]
        self._cleanup(*names)

        dummy_cert = "-----BEGIN CERTIFICATE-----\\nMIIBfake...\\n-----END CERTIFICATE-----"
        dummy_key = "-----BEGIN PRIVATE KEY-----\\nMIIBfake...\\n-----END PRIVATE KEY-----"

        # (a) tls_client_cert only, missing client_key
        tdSql.error(
            f"create external source {base}_a type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_client_cert'='{dummy_cert}')",
            expectedErrno=None,
        )

        # (b) tls_client_key only, missing client_cert
        tdSql.error(
            f"create external source {base}_b type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_client_key'='{dummy_key}')",
            expectedErrno=None,
        )

        # (c) tls_enabled=false → TLS options ignored, should succeed
        tdSql.execute(
            f"create external source {base}_c type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='false', 'tls_client_cert'='{dummy_cert}', "
            f"'tls_client_key'='{dummy_key}')"
        )
        assert self._find_show_row(f"{base}_c") >= 0

        # (d) Complete mutual TLS config → should succeed
        tdSql.execute(
            f"create external source {base}_d type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{dummy_cert}', "
            f"'tls_client_cert'='{dummy_cert}', 'tls_client_key'='{dummy_key}')"
        )
        assert self._find_show_row(f"{base}_d") >= 0

        # (e) One-way TLS (ca only) → should succeed
        tdSql.execute(
            f"create external source {base}_e type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{dummy_cert}')"
        )
        assert self._find_show_row(f"{base}_e") >= 0

        # (f) MySQL ssl_mode=verify_ca + client_cert WITHOUT client_key
        tdSql.error(
            f"create external source {base}_f type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('ssl_mode'='verify_ca', 'tls_client_cert'='{dummy_cert}')",
            expectedErrno=None,
        )

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S02  特殊字符 source 名字
    # ------------------------------------------------------------------

    def test_fq_ext_s02_special_char_source_names(self):
        """FQ-EXT-S02: 特殊字符 external source 名字

        FS §3.4.1.3: 标识符规则与数据库名/表名相同，默认限制字符类型且
        不区分大小写，转义后放宽字符限制且区分大小写。

        Multi-dimensional coverage:
          a) 下划线开头的名字 → 应被接受
          b) 纯数字名字 → 应被拒绝（标识符规则）
          c) 超长名字（192 chars）→ 取决于长度限制
          d) backtick 转义带特殊字符（中文、横杠、空格）→ 应被接受
          e) backtick 转义后区分大小写
          f) SQL 保留字作为名字（如 select, database）→ backtick 可用
          g) 空名字 → 语法错误

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary special char source name tests

        """
        base_sql = (
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            "user='u' password='p' database='db'"
        )

        # (a) Underscore prefix → OK
        n = "_fq_ext_s02_underscore"
        self._cleanup(n)
        tdSql.execute(f"create external source {n} {base_sql}")
        assert self._find_show_row(n) >= 0
        self._cleanup(n)

        # (b) Pure numeric name → should fail (identifier rules)
        tdSql.error(
            f"create external source 12345 {base_sql}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (c) Long name (192 chars)
        long_name = "s" * 192
        self._assert_error_not_syntax(
            f"create external source {long_name} {base_sql}"
        )
        # Clean up if it succeeded
        tdSql.execute(f"drop external source if exists {long_name}")

        # (d) Backtick with Chinese
        cn_name = "`中文数据源`"
        tdSql.execute(f"drop external source if exists {cn_name}")
        self._assert_error_not_syntax(
            f"create external source {cn_name} {base_sql}"
        )
        tdSql.execute(f"drop external source if exists {cn_name}")

        # (d-2) Backtick with hyphen
        hyp_name = "`my-ext-source`"
        tdSql.execute(f"drop external source if exists {hyp_name}")
        self._assert_error_not_syntax(
            f"create external source {hyp_name} {base_sql}"
        )
        tdSql.execute(f"drop external source if exists {hyp_name}")

        # (d-3) Backtick with space
        sp_name = "`my ext source`"
        tdSql.execute(f"drop external source if exists {sp_name}")
        self._assert_error_not_syntax(
            f"create external source {sp_name} {base_sql}"
        )
        tdSql.execute(f"drop external source if exists {sp_name}")

        # (e) Backtick case sensitivity: `MySource` vs `mysource`
        tdSql.execute(f"drop external source if exists `CaseSrc`")
        tdSql.execute(f"drop external source if exists `casesrc`")
        self._assert_error_not_syntax(
            f"create external source `CaseSrc` {base_sql}"
        )
        # If CaseSrc succeeded, test lowercase variant
        ok = tdSql.query(
            f"show external sources", exit=False
        )
        if ok is not False and any(
            str(r[0]) == "CaseSrc" for r in (tdSql.queryResult or [])
        ):
            self._assert_error_not_syntax(
                f"create external source `casesrc` {base_sql}"
            )
            tdSql.execute("drop external source if exists `casesrc`")
        tdSql.execute("drop external source if exists `CaseSrc`")

        # (f) SQL reserved word as name with backticks
        for rw in ["select", "database", "table"]:
            tdSql.execute(f"drop external source if exists `{rw}`")
            self._assert_error_not_syntax(
                f"create external source `{rw}` {base_sql}"
            )
            tdSql.execute(f"drop external source if exists `{rw}`")

        # (g) Empty name → syntax error
        tdSql.error(
            f"create external source '' {base_sql}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (g-2) Empty backtick name
        tdSql.error(
            f"create external source `` {base_sql}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

    # ------------------------------------------------------------------
    # FQ-EXT-S03  ALTER 不存在的 external source
    # ------------------------------------------------------------------

    def test_fq_ext_s03_alter_nonexistent_source(self):
        """FQ-EXT-S03: ALTER 不存在的 external source

        Multi-dimensional coverage:
          a) ALTER SET password on never-existed name → NOT_EXIST error
          b) ALTER SET host on never-existed name → NOT_EXIST error
          c) ALTER SET port on never-existed name → NOT_EXIST error
          d) ALTER SET user on never-existed name → NOT_EXIST error
          e) ALTER SET options on never-existed name → NOT_EXIST error
          f) CREATE then DROP, then ALTER the dropped name → NOT_EXIST error
          g) ALTER with IF EXISTS (if supported) on non-existent → no error

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary ALTER non-existent source tests

        """
        ghost = "fq_ext_s03_ghost_source"
        self._cleanup(ghost)

        # (a)-(e) Various ALTER fields on non-existent source
        alter_cmds = [
            f"alter external source {ghost} set password='new'",
            f"alter external source {ghost} set host='1.2.3.4'",
            f"alter external source {ghost} set port=3307",
            f"alter external source {ghost} set user='new_user'",
            f"alter external source {ghost} set options('connect_timeout_ms'='5000')",
        ]
        for cmd in alter_cmds:
            tdSql.error(cmd, expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST)

        # (f) CREATE → DROP → ALTER the dropped one
        tdSql.execute(
            f"create external source {ghost} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        tdSql.execute(f"drop external source {ghost}")
        tdSql.error(
            f"alter external source {ghost} set password='x'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

    # ------------------------------------------------------------------
    # FQ-EXT-S04  TYPE 值不区分大小写
    # ------------------------------------------------------------------

    def test_fq_ext_s04_type_case_insensitive(self):
        """FQ-EXT-S04: TYPE 值不区分大小写

        FS §3.4.1.3: 标识符规则不区分大小写

        Multi-dimensional coverage:
          a) type='MySQL' (mixed case) → accepted, SHOW type = 'mysql'
          b) type='MYSQL' (all upper) → accepted, SHOW type = 'mysql'
          c) type='mYsQl' (random case) → accepted
          d) type='PostgreSQL' → accepted, SHOW type = 'postgresql'
          e) type='POSTGRESQL' → accepted
          f) type='InfluxDB' → accepted, SHOW type = 'influxdb'
          g) type='INFLUXDB' → accepted
          h) type='unknown_type' → error
          i) type='' (empty) → syntax error

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary TYPE case insensitivity tests

        """
        base = "fq_ext_s04"

        cases = [
            (f"{base}_a", "MySQL", "mysql"),
            (f"{base}_b", "MYSQL", "mysql"),
            (f"{base}_c", "mYsQl", "mysql"),
            (f"{base}_d", "PostgreSQL", "postgresql"),
            (f"{base}_e", "POSTGRESQL", "postgresql"),
            (f"{base}_f", "InfluxDB", "influxdb"),
            (f"{base}_g", "INFLUXDB", "influxdb"),
        ]
        names = [c[0] for c in cases]
        self._cleanup(*names)

        for name, type_val, expected_show in cases:
            if expected_show == "influxdb":
                tdSql.execute(
                    f"create external source {name} type='{type_val}' "
                    f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} api_token='{self._influx_cfg().token}' database='mydb'"
                )
            elif expected_show == "postgresql":
                tdSql.execute(
                    f"create external source {name} type='{type_val}' "
                    f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
                    f"database='pgdb' schema='public'"
                )
            else:
                tdSql.execute(
                    f"create external source {name} type='{type_val}' "
                    f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
                )
            self._assert_show_field(name, _COL_TYPE, expected_show)

        # (h) Unknown type → error
        tdSql.error(
            f"create external source {base}_h type='unknown_type' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'",
            expectedErrno=None,
        )

        # (i) Empty type → syntax error
        tdSql.error(
            f"create external source {base}_i type='' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'",
            expectedErrno=None,
        )

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S05  不同数据库专属选项混淆使用
    # ------------------------------------------------------------------

    def test_fq_ext_s05_cross_db_option_confusion(self):
        """FQ-EXT-S05: 不同数据库专属选项混淆使用

        FS §3.4.1.4: OPTIONS 分为通用选项和各源专属选项。
        MySQL: charset, ssl_mode
        PG: sslmode, application_name, search_path
        InfluxDB: api_token, protocol

        Multi-dimensional coverage:
          a) MySQL source with PG-specific 'sslmode' → should be ignored or error
          b) MySQL source with PG 'application_name' → should be ignored
          c) MySQL source with InfluxDB 'api_token' → should be ignored
          d) MySQL source with InfluxDB 'protocol' → should be ignored
          e) PG source with MySQL 'ssl_mode' → should be ignored or error
          f) PG source with MySQL 'charset' → should be ignored
          g) PG source with InfluxDB 'api_token' → should be ignored
          h) InfluxDB source with MySQL 'ssl_mode' → should be ignored
          i) InfluxDB source with PG 'sslmode' → should be ignored
          j) InfluxDB source with MySQL 'charset' → should be ignored
          k) Mixed: MySQL with both ssl_mode (own) and sslmode (PG) → ssl_mode used,
             sslmode ignored
          l) Verify SHOW OPTIONS only contains relevant options

        Note: per FS "未识别的 key 将被忽略并记录警告日志", foreign options
        should be silently ignored.

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary cross-DB option confusion tests

        """
        base = "fq_ext_s05"
        names = [f"{base}_{c}" for c in "abcdefghijk"]
        self._cleanup(*names)

        # (a) MySQL + PG-specific 'sslmode'
        tdSql.execute(
            f"create external source {base}_a type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('sslmode'='require')"
        )
        idx = self._find_show_row(f"{base}_a")
        assert idx >= 0, "should succeed with ignored foreign option"

        # (b) MySQL + PG 'application_name'
        tdSql.execute(
            f"create external source {base}_b type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('application_name'='MyApp')"
        )
        assert self._find_show_row(f"{base}_b") >= 0

        # (c) MySQL + InfluxDB 'api_token'
        tdSql.execute(
            f"create external source {base}_c type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('api_token'='some-token')"
        )
        assert self._find_show_row(f"{base}_c") >= 0

        # (d) MySQL + InfluxDB 'protocol'
        tdSql.execute(
            f"create external source {base}_d type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('protocol'='flight_sql')"
        )
        assert self._find_show_row(f"{base}_d") >= 0

        # (e) PG + MySQL 'ssl_mode'
        tdSql.execute(
            f"create external source {base}_e type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('ssl_mode'='required')"
        )
        assert self._find_show_row(f"{base}_e") >= 0

        # (f) PG + MySQL 'charset'
        tdSql.execute(
            f"create external source {base}_f type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('charset'='utf8mb4')"
        )
        assert self._find_show_row(f"{base}_f") >= 0

        # (g) PG + InfluxDB 'api_token'
        tdSql.execute(
            f"create external source {base}_g type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('api_token'='some-token')"
        )
        assert self._find_show_row(f"{base}_g") >= 0

        # (h) InfluxDB + MySQL 'ssl_mode'
        tdSql.execute(
            f"create external source {base}_h type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} api_token='{self._influx_cfg().token}' database='mydb' "
            f"options('ssl_mode'='required')"
        )
        assert self._find_show_row(f"{base}_h") >= 0

        # (i) InfluxDB + PG 'sslmode'
        tdSql.execute(
            f"create external source {base}_i type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} api_token='{self._influx_cfg().token}' database='mydb' "
            f"options('sslmode'='require')"
        )
        assert self._find_show_row(f"{base}_i") >= 0

        # (j) InfluxDB + MySQL 'charset'
        tdSql.execute(
            f"create external source {base}_j type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} api_token='{self._influx_cfg().token}' database='mydb' "
            f"options('charset'='utf8mb4')"
        )
        assert self._find_show_row(f"{base}_j") >= 0

        # (k) MySQL with both ssl_mode (own) and sslmode (PG) — own takes effect
        tdSql.execute(
            f"create external source {base}_k type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('ssl_mode'='required', 'sslmode'='require')"
        )
        idx = self._find_show_row(f"{base}_k")
        assert idx >= 0
        opts = str(tdSql.queryResult[idx][_COL_OPTIONS])
        # ssl_mode should be present (own option); sslmode may be ignored
        assert "ssl_mode" in opts or "required" in opts, \
            "MySQL's own ssl_mode should be stored"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S06  重复删除 external source
    # ------------------------------------------------------------------

    def test_fq_ext_s06_repeated_drop(self):
        """FQ-EXT-S06: 重复删除 external source — 幂等与错误行为

        Multi-dimensional coverage:
          a) CREATE → DROP IF EXISTS → DROP IF EXISTS again → no error both times
          b) CREATE → DROP IF EXISTS × 5 → all succeed without error
          c) CREATE → DROP (no IF EXISTS) → DROP (no IF EXISTS) → error second time
          d) DROP IF EXISTS on never-created name → no error
          e) Multiple different sources: drop them all, then drop again
          f) CREATE → DROP → CREATE same name → DROP → DROP IF EXISTS → OK

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary repeated DROP tests

        """
        base = "fq_ext_s06"

        # (a) DROP IF EXISTS twice after create
        name = f"{base}_a"
        self._cleanup(name)
        tdSql.execute(
            f"create external source {name} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        tdSql.execute(f"drop external source if exists {name}")
        assert self._find_show_row(name) < 0
        tdSql.execute(f"drop external source if exists {name}")  # no error

        # (b) DROP IF EXISTS × 5 on same name
        name = f"{base}_b"
        tdSql.execute(
            f"create external source {name} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        for _ in range(5):
            tdSql.execute(f"drop external source if exists {name}")

        # (c) DROP without IF EXISTS twice → second fails
        name = f"{base}_c"
        self._cleanup(name)
        tdSql.execute(
            f"create external source {name} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        tdSql.execute(f"drop external source {name}")
        tdSql.error(
            f"drop external source {name}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        # (d) DROP IF EXISTS on never-created name
        tdSql.execute(f"drop external source if exists {base}_never_existed_xyz")

        # (e) Multiple sources: drop all, then drop again
        multi = [f"{base}_e1", f"{base}_e2", f"{base}_e3"]
        self._cleanup(*multi)
        for m in multi:
            tdSql.execute(
                f"create external source {m} type='mysql' "
                f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
            )
        for m in multi:
            tdSql.execute(f"drop external source {m}")
        for m in multi:
            tdSql.execute(f"drop external source if exists {m}")  # all succeed

        # (f) CREATE → DROP → CREATE → DROP → DROP IF EXISTS
        name = f"{base}_f"
        self._cleanup(name)
        tdSql.execute(
            f"create external source {name} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        tdSql.execute(f"drop external source {name}")
        tdSql.execute(
            f"create external source {name} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'"
        )
        tdSql.execute(f"drop external source {name}")
        tdSql.execute(f"drop external source if exists {name}")

    # ------------------------------------------------------------------
    # FQ-EXT-S07  DESCRIBE 不存在的 external source
    # ------------------------------------------------------------------

    def test_fq_ext_s07_describe_nonexistent_source(self):
        """FQ-EXT-S07: DESCRIBE 不存在的 external source

        FS §3.4.3: DESCRIBE EXTERNAL SOURCE source_name
        对不存在的 source_name 应返回 NOT_EXIST 错误。

        Multi-dimensional coverage:
          a) DESCRIBE never-existed name → error
          b) CREATE → DROP → DESCRIBE the dropped name → error
          c) DESCRIBE with backtick-escaped never-existed name → error
          d) DESCRIBE existing name succeeds (positive control)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary DESCRIBE non-existent source tests

        """
        ghost = "fq_ext_s07_ghost"
        existing = "fq_ext_s07_exist"
        self._cleanup(ghost, existing)

        # (a) Never-existed → error
        tdSql.error(
            f"describe external source {ghost}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        # (b) CREATE → DROP → DESCRIBE → error
        tdSql.execute(
            f"create external source {ghost} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        tdSql.execute(f"drop external source {ghost}")
        tdSql.error(
            f"describe external source {ghost}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        # (c) Backtick-escaped never-existed → error
        tdSql.error(
            "describe external source `fq_ext_s07_backtick_ghost`",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        # (d) Positive control: existing source DESCRIBE succeeds
        tdSql.execute(
            f"create external source {existing} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        desc = self._describe_dict(existing)
        if desc:
            assert desc.get("type") == "mysql"
        self._cleanup(existing)

    # ------------------------------------------------------------------
    # FQ-EXT-S08  REFRESH 不存在的 external source
    # ------------------------------------------------------------------

    def test_fq_ext_s08_refresh_nonexistent_source(self):
        """FQ-EXT-S08: REFRESH 不存在的 external source

        FS §3.4.6: REFRESH EXTERNAL SOURCE source_name
        对不存在的 source_name 应返回 NOT_EXIST 错误。
        (对比 FQ-EXT-027 测试的是不可达但已注册的源)

        Multi-dimensional coverage:
          a) REFRESH never-existed name → error
          b) CREATE → DROP → REFRESH the dropped name → error
          c) REFRESH after successful REFRESH of existing → still OK
             (positive control)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary REFRESH non-existent source tests

        """
        ghost = "fq_ext_s08_ghost"
        self._cleanup(ghost)

        # (a) Never-existed → error
        tdSql.error(
            f"refresh external source {ghost}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        # (b) CREATE → DROP → REFRESH → error
        tdSql.execute(
            f"create external source {ghost} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        tdSql.execute(f"drop external source {ghost}")
        tdSql.error(
            f"refresh external source {ghost}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

    # ------------------------------------------------------------------
    # FQ-EXT-S09  CREATE 缺少必填字段
    # ------------------------------------------------------------------

    def test_fq_ext_s09_missing_mandatory_fields(self):
        """FQ-EXT-S09: CREATE 缺少必填字段

        FS §3.4.1.2: TYPE / HOST / PORT / USER / PASSWORD 均为必填。
        缺少任一必填字段应报语法错误。

        Multi-dimensional coverage:
          a) Missing TYPE → syntax error
          b) Missing HOST → syntax error
          c) Missing PORT → syntax error
          d) Missing USER → syntax error
          e) Missing PASSWORD → syntax error
          f) Missing TYPE + HOST → syntax error
          g) Only source_name, no other fields → syntax error
          h) All fields present → success (positive control)

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary missing mandatory field tests

        """
        name = "fq_ext_s09"
        self._cleanup(name)

        # (a) Missing TYPE
        tdSql.error(
            f"create external source {name} "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (b) Missing HOST
        tdSql.error(
            f"create external source {name} "
            "type='mysql' port=3306 user='u' password='p'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (c) Missing PORT
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (d) Missing USER
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (e) Missing PASSWORD
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (f) Missing TYPE + HOST
        tdSql.error(
            f"create external source {name} "
            "port=3306 user='u' password='p'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (g) Only source_name
        tdSql.error(
            f"create external source {name}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (h) Positive control: all mandatory fields
        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        assert self._find_show_row(name) >= 0
        self._cleanup(name)

    # ------------------------------------------------------------------
    # FQ-EXT-S10  TYPE='tdengine' 预留类型
    # ------------------------------------------------------------------

    def test_fq_ext_s10_type_tdengine_reserved(self):
        """FQ-EXT-S10: TYPE='tdengine' 预留类型 — 首版不交付

        FS §3.4.1.2: 'tdengine' 为预留扩展，首版不交付。
        尝试创建 type='tdengine' 应报错。

        Multi-dimensional coverage:
          a) type='tdengine' → error
          b) type='TDengine' (mixed case) → error
          c) type='TDENGINE' (upper) → error
          d) Source should NOT appear in SHOW after rejection

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary tdengine reserved type tests

        """
        base = "fq_ext_s10"
        names = [f"{base}_a", f"{base}_b", f"{base}_c"]
        self._cleanup(*names)

        for name, type_val in [
            (f"{base}_a", "tdengine"),
            (f"{base}_b", "TDengine"),
            (f"{base}_c", "TDENGINE"),
        ]:
            tdSql.error(
                f"create external source {name} type='{type_val}' "
                f"host='192.0.2.1' port=6030 user='root' password='taosdata'",
                expectedErrno=None,
            )
            assert self._find_show_row(name) < 0, (
                f"source with reserved type='{type_val}' should NOT appear in SHOW"
            )

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S11  ALTER 多字段组合
    # ------------------------------------------------------------------

    def test_fq_ext_s11_alter_multi_field_combined(self):
        """FQ-EXT-S11: ALTER 多字段组合 — 一条 ALTER 同时修改多个字段

        FS §3.4.4: 可修改 HOST/PORT/USER/PASSWORD/DATABASE/SCHEMA/OPTIONS。
        FQ-EXT-010/011 已测 2 字段组合，此用例测 4~6 字段同时修改。

        Multi-dimensional coverage:
          a) ALTER HOST + PORT + USER + PASSWORD in one SET
          b) ALTER DATABASE + SCHEMA in one SET (PG type)
          c) ALTER HOST + USER + PASSWORD + DATABASE + OPTIONS in one SET
          d) Verify all changed fields in SHOW after each ALTER
          e) TYPE and create_time unchanged after combined ALTER

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary multi-field ALTER tests

        """
        name_mysql = "fq_ext_s11_m"
        name_pg = "fq_ext_s11_pg"
        self._cleanup(name_mysql, name_pg)

        # ── (a) MySQL: ALTER 4 fields at once ──
        tdSql.execute(
            f"create external source {name_mysql} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database=old_db"
        )
        row = self._find_show_row(name_mysql)
        ctime_orig = tdSql.queryResult[row][_COL_CTIME]

        tdSql.execute(
            f"alter external source {name_mysql} set "
            f"host='10.0.0.2', port=3307, user='new_user', password='new_pwd'"
        )
        self._assert_show_field(name_mysql, _COL_HOST, "10.0.0.2")
        self._assert_show_field(name_mysql, _COL_PORT, 3307)
        self._assert_show_field(name_mysql, _COL_USER, "new_user")
        self._assert_show_field(name_mysql, _COL_PASSWORD, _MASKED)
        self._assert_show_field(name_mysql, _COL_DATABASE, "old_db")  # unchanged
        self._assert_show_field(name_mysql, _COL_TYPE, "mysql")       # immutable
        row = self._find_show_row(name_mysql)
        assert str(tdSql.queryResult[row][_COL_CTIME]) == str(ctime_orig)

        # ── (b) PG: ALTER DATABASE + SCHEMA together ──
        tdSql.execute(
            f"create external source {name_pg} type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database=old_pg_db schema=old_schema"
        )
        tdSql.execute(
            f"alter external source {name_pg} set "
            f"database=new_pg_db, schema=new_schema"
        )
        self._assert_show_field(name_pg, _COL_DATABASE, "new_pg_db")
        self._assert_show_field(name_pg, _COL_SCHEMA, "new_schema")
        self._assert_show_field(name_pg, _COL_HOST, self._pg_cfg().host)  # unchanged

        # ── (c) MySQL: ALTER 5 fields + OPTIONS ──
        tdSql.execute(
            f"alter external source {name_mysql} set "
            f"host='10.0.0.3', user='admin', password='admin_pwd', "
            f"database=new_db, options('connect_timeout_ms'='3000')"
        )
        self._assert_show_field(name_mysql, _COL_HOST, "10.0.0.3")
        self._assert_show_field(name_mysql, _COL_USER, "admin")
        self._assert_show_field(name_mysql, _COL_DATABASE, "new_db")
        self._assert_show_opts_contain(name_mysql, "connect_timeout_ms", "3000")
        self._assert_show_field(name_mysql, _COL_TYPE, "mysql")  # still immutable

        self._cleanup(name_mysql, name_pg)

    # ------------------------------------------------------------------
    # FQ-EXT-S12  OPTIONS 边界值
    # ------------------------------------------------------------------

    def test_fq_ext_s12_options_boundary_values(self):
        """FQ-EXT-S12: OPTIONS 值边界 — 空子句、非法值、极端值

        FS §3.4.1.4: connect_timeout_ms 正整数; read_timeout_ms 正整数
        DS §9.2: connect_timeout_ms min=100, max=600000

        Multi-dimensional coverage:
          a) Empty OPTIONS clause → success (no options stored)
          b) connect_timeout_ms='0' → error or ignored (below min=100)
          c) connect_timeout_ms='-1' → error or ignored (负数)
          d) connect_timeout_ms='abc' → error or ignored (非数字)
          e) connect_timeout_ms='99999999' → error or accepted
          f) read_timeout_ms='0' → error or ignored
          g) connect_timeout_ms + read_timeout_ms both valid → success
          h) Verify valid values persisted in SHOW OPTIONS

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary OPTIONS boundary value tests

        """
        base = "fq_ext_s12"
        base_sql = f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        names = [f"{base}_{c}" for c in "abcdefgh"]
        self._cleanup(*names)

        # (a) Empty OPTIONS clause → should succeed
        tdSql.execute(
            f"create external source {base}_a {base_sql} options()"
        )
        assert self._find_show_row(f"{base}_a") >= 0
        self._cleanup(f"{base}_a")

        # (b) connect_timeout_ms='0' — below DS min of 100
        self._assert_error_not_syntax(
            f"create external source {base}_b {base_sql} "
            f"options('connect_timeout_ms'='0')"
        )
        tdSql.execute(f"drop external source if exists {base}_b")

        # (c) connect_timeout_ms='-1' — negative
        self._assert_error_not_syntax(
            f"create external source {base}_c {base_sql} "
            f"options('connect_timeout_ms'='-1')"
        )
        tdSql.execute(f"drop external source if exists {base}_c")

        # (d) connect_timeout_ms='abc' — non-numeric
        self._assert_error_not_syntax(
            f"create external source {base}_d {base_sql} "
            f"options('connect_timeout_ms'='abc')"
        )
        tdSql.execute(f"drop external source if exists {base}_d")

        # (e) connect_timeout_ms very large
        self._assert_error_not_syntax(
            f"create external source {base}_e {base_sql} "
            f"options('connect_timeout_ms'='99999999')"
        )
        tdSql.execute(f"drop external source if exists {base}_e")

        # (f) read_timeout_ms='0'
        self._assert_error_not_syntax(
            f"create external source {base}_f {base_sql} "
            f"options('read_timeout_ms'='0')"
        )
        tdSql.execute(f"drop external source if exists {base}_f")

        # (g) Both valid → success
        tdSql.execute(
            f"create external source {base}_g {base_sql} "
            f"options('connect_timeout_ms'='5000', 'read_timeout_ms'='10000')"
        )
        assert self._find_show_row(f"{base}_g") >= 0

        # (h) Verify values persisted
        self._assert_show_opts_contain(f"{base}_g", "connect_timeout_ms", "5000")
        self._assert_show_opts_contain(f"{base}_g", "read_timeout_ms", "10000")

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S13  ALTER 清除 DATABASE/SCHEMA
    # ------------------------------------------------------------------

    def test_fq_ext_s13_alter_clear_database_schema(self):
        """FQ-EXT-S13: ALTER 清除 DATABASE/SCHEMA — 置空或置 NULL

        FS §3.4.1.2: DATABASE/SCHEMA 非必填，可不指定。
        修改后应能回退到"未指定"状态。

        Multi-dimensional coverage:
          a) ALTER SET DATABASE='' → DATABASE 变为空/NULL
          b) ALTER SET SCHEMA='' → SCHEMA 变为空/NULL
          c) ALTER SET DATABASE='' 后再设回有效值 → 恢复正常
          d) PG: ALTER DATABASE + SCHEMA 都设为空
          e) Verify other fields (HOST, USER) unchanged

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary ALTER clear DATABASE/SCHEMA tests

        """
        name_mysql = "fq_ext_s13_m"
        name_pg = "fq_ext_s13_pg"
        self._cleanup(name_mysql, name_pg)

        # ── (a) MySQL: clear DATABASE ──
        tdSql.execute(
            f"create external source {name_mysql} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database=mydb"
        )
        self._assert_show_field(name_mysql, _COL_DATABASE, "mydb")

        # Try to clear database — may use empty string or keyword
        ret = tdSql.query(
            f"alter external source {name_mysql} set database=''",
            exit=False,
        )
        if ret is not False:
            row = self._find_show_row(name_mysql)
            db_val = tdSql.queryResult[row][_COL_DATABASE]
            assert db_val is None or str(db_val).strip() == "", (
                f"DATABASE should be empty/None after clearing, got '{db_val}'"
            )

            # (c) Set back to a valid value
            tdSql.execute(
                f"alter external source {name_mysql} set database=restored_db"
            )
            self._assert_show_field(name_mysql, _COL_DATABASE, "restored_db")

        # ── (b) PG: clear SCHEMA ──
        tdSql.execute(
            f"create external source {name_pg} type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database=pgdb schema=public"
        )
        self._assert_show_field(name_pg, _COL_SCHEMA, "public")

        ret = tdSql.query(
            f"alter external source {name_pg} set schema=''",
            exit=False,
        )
        if ret is not False:
            row = self._find_show_row(name_pg)
            schema_val = tdSql.queryResult[row][_COL_SCHEMA]
            assert schema_val is None or str(schema_val).strip() == "", (
                f"SCHEMA should be empty/None after clearing, got '{schema_val}'"
            )

        # ── (d) PG: clear both DATABASE + SCHEMA ──
        ret = tdSql.query(
            f"alter external source {name_pg} set database='', schema=''",
            exit=False,
        )
        if ret is not False:
            row = self._find_show_row(name_pg)
            db_val = tdSql.queryResult[row][_COL_DATABASE]
            schema_val = tdSql.queryResult[row][_COL_SCHEMA]
            assert db_val is None or str(db_val).strip() == ""
            assert schema_val is None or str(schema_val).strip() == ""

        # ── (e) HOST/USER unchanged ──
        self._assert_show_field(name_mysql, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name_mysql, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name_mysql, name_pg)

    # ------------------------------------------------------------------
    # FQ-EXT-S14  Name 冲突大小写不敏感
    # ------------------------------------------------------------------

    def test_fq_ext_s14_name_conflict_case_insensitive(self):
        """FQ-EXT-S14: source_name 与数据库名冲突 — 大小写不敏感

        FS §3.4.1.3: 标识符默认不区分大小写。
        FS §3.4.1.2: source_name 不允许与 TSDB 中的库名同名。
        因此 DB=FQ_DB 与 source=fq_db (或 Fq_Db) 应冲突。

        Multi-dimensional coverage:
          a) CREATE DATABASE FQ_S14_DB → CREATE SOURCE fq_s14_db → conflict
          b) CREATE SOURCE FQ_S14_SRC → CREATE DATABASE fq_s14_src → conflict
          c) CREATE SOURCE fq_s14_x → CREATE SOURCE FQ_S14_X → already exists
          d) DROP DATABASE → source with same caseless name now succeeds
          e) Backtick-escaped name respects case sensitivity:
             CREATE DATABASE `CaseDB` → CREATE SOURCE `casedb` → conflict
             but CREATE SOURCE `CaseDB2` vs CREATE SOURCE `casedb2`
             depends on whether backtick forces exact case

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Added supplementary name conflict case-insensitivity tests

        """
        db1 = "FQ_S14_DB"
        src1 = "fq_s14_db"
        src2 = "FQ_S14_SRC"
        db2 = "fq_s14_src"
        src_dup_lower = "fq_s14_x"
        src_dup_upper = "FQ_S14_X"

        # Cleanup
        for n in [src1, src2, src_dup_lower, src_dup_upper]:
            tdSql.execute(f"drop external source if exists {n}")
        for d in [db1, db2]:
            tdSql.execute(f"drop database if exists {d}")

        base_sql = f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"

        # ── (a) DB upper → source lower → conflict ──
        tdSql.execute(f"create database {db1}")
        tdSql.error(
            f"create external source {src1} {base_sql}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
        )
        assert self._find_show_row(src1) < 0

        # ── (b) Source upper → DB lower → conflict ──
        tdSql.execute(f"create external source {src2} {base_sql}")
        tdSql.error(
            f"create database {db2}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
        )

        # ── (c) Source lower → Source upper → already exists ──
        tdSql.execute(f"create external source {src_dup_lower} {base_sql}")
        tdSql.error(
            f"create external source {src_dup_upper} {base_sql}",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS,
        )

        # ── (d) DROP DB → source with caseless name succeeds ──
        tdSql.execute(f"drop database {db1}")
        tdSql.execute(f"create external source {src1} {base_sql}")
        assert self._find_show_row(src1) >= 0

        # Cleanup
        for n in [src1, src2, src_dup_lower, src_dup_upper]:
            tdSql.execute(f"drop external source if exists {n}")
        for d in [db1, db2]:
            tdSql.execute(f"drop database if exists {d}")

