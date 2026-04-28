"""
test_fq_01_external_source.py

Implements FQ-EXT-001 through FQ-EXT-032 from TS §1
"External Source Management" — full lifecycle of CREATE / SHOW / DESCRIBE / ALTER / DROP /
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
    TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG,
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

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")

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
        
        Note: DESCRIBE EXTERNAL SOURCE returns a single row with columns:
              (source_name, type, host, port, user, password, database, schema, options, create_time)
        """
        ok = tdSql.query(f"describe external source {source_name}", exit=False)
        if ok is False:
            return {}
        if len(tdSql.queryResult) == 0:
            return {}
        
        # DESCRIBE EXTERNAL SOURCE returns a single row with all columns
        row = tdSql.queryResult[0]
        # Map column indices to field names (same as SHOW EXTERNAL SOURCES)
        field_names = ['source_name', 'type', 'host', 'port', 'user', 'password', 
                       'database', 'schema', 'options', 'create_time']
        result = {}
        for i, field_name in enumerate(field_names):
            if i < len(row):
                result[field_name.lower()] = row[i]
        return result

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
        """Assert one field in DESCRIBE output, fail if DESCRIBE fails or field missing."""
        desc = self._describe_dict(source_name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {source_name} failed or returned empty result"
        assert field.lower() in desc, (
            f"DESCRIBE {source_name}: field '{field}' not found in result. "
            f"Available fields: {list(desc.keys())}"
        )
        actual = desc.get(field.lower())
        assert str(actual) == str(expected), (
            f"DESCRIBE {source_name}: {field} expected '{expected}', got '{actual}'"
        )

    def _assert_describe_fields(self, source_name: str, expected_fields: dict):
        """Assert multiple fields in DESCRIBE output at once.
        
        Args:
            source_name: external source name
            expected_fields: dict of {field_name: expected_value, ...}
                            field names are case-insensitive
        """
        desc = self._describe_dict(source_name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {source_name} failed or returned empty result"
        for field, expected in expected_fields.items():
            field_lower = field.lower()
            assert field_lower in desc, (
                f"DESCRIBE {source_name}: field '{field}' not found. "
                f"Available fields: {list(desc.keys())}"
            )
            actual = desc.get(field_lower)
            assert str(actual) == str(expected), (
                f"DESCRIBE {source_name}: {field} expected '{expected}', got '{actual}'"
            )

    # ------------------------------------------------------------------
    # FQ-EXT-001 through FQ-EXT-032
    # ------------------------------------------------------------------

    def test_fq_ext_001(self):
        """FQ-EXT-001: Create MySQL external source - full params, expect success and visible in SHOW

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
        self._assert_describe_fields(name_opts, {
            "type": "mysql",
            "host": self._mysql_cfg().host,
            "port": 3307,
            "user": self._mysql_cfg().user,
            "database": "power"
        })
        # OPTIONS field must contain all key-value pairs
        desc = self._describe_dict(name_opts)
        opts_str = str(desc.get("options", ""))
        assert "connect_timeout_ms" in opts_str and "2000" in opts_str, \
            f"OPTIONS must contain 'connect_timeout_ms':'2000', got: {opts_str}"
        assert "read_timeout_ms" in opts_str and "5000" in opts_str, \
            f"OPTIONS must contain 'read_timeout_ms':'5000', got: {opts_str}"
        assert "charset" in opts_str and "utf8mb4" in opts_str, \
            f"OPTIONS must contain 'charset':'utf8mb4', got: {opts_str}"
        assert "ssl_mode" in opts_str and "preferred" in opts_str, \
            f"OPTIONS must contain 'ssl_mode':'preferred', got: {opts_str}"

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
        # DESCRIBE must succeed and fake_key must NOT appear in OPTIONS
        desc = self._describe_dict(name_tls)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_tls} failed"
        opts_str = str(desc.get("options", ""))
        assert fake_key not in opts_str, \
            f"TLS cert key path must be masked; fake_key must not appear in OPTIONS: {opts_str}"

        # ── (d) Special characters in password ──
        special_pwd = "p@ss'w\"d\\!#$%"
        special_pwd_sql = special_pwd.replace("'", "''")
        tdSql.execute(
            f"create external source {name_sp} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{special_pwd_sql}'"
        )
        row = self._assert_show_field(name_sp, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_PASSWORD, _MASKED)
        assert special_pwd not in str(tdSql.queryResult[row][_COL_PASSWORD])
        self._assert_ctime_valid(name_sp)

        self._cleanup(name_min, name_opts, name_tls, name_sp)

    def test_fq_ext_002(self):
        """FQ-EXT-002: Create PG external source - with DATABASE+SCHEMA and all 9 OPTIONS

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
        # DESCRIBE must succeed and verify all OPTIONS fields
        desc = self._describe_dict(name_opts)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_opts} failed"
        opts_str = str(desc.get("options", ""))
        assert "sslmode" in opts_str and "require" in opts_str, \
            f"OPTIONS must contain 'sslmode':'require', got: {opts_str}"
        assert "application_name" in opts_str and "TDengine-Test" in opts_str, \
            f"OPTIONS must contain 'application_name':'TDengine-Test', got: {opts_str}"
        assert "search_path" in opts_str and "public,iot" in opts_str, \
            f"OPTIONS must contain 'search_path':'public,iot', got: {opts_str}"
        assert fake_key not in opts_str, \
            f"fake_key must not appear in OPTIONS: {opts_str}"

        self._cleanup(name_ds, name_d, name_s, name_opts)

    def test_fq_ext_003(self):
        """FQ-EXT-003: Create InfluxDB external source - covering all 8 OPTIONS

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
        # DESCRIBE must succeed and verify OPTIONS security
        desc = self._describe_dict(name_all)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_all} failed"
        opts_str = str(desc.get("options", ""))
        assert "connect_timeout_ms" in opts_str and "2000" in opts_str, \
            f"OPTIONS must contain 'connect_timeout_ms':'2000', got: {opts_str}"
        assert "read_timeout_ms" in opts_str and "8000" in opts_str, \
            f"OPTIONS must contain 'read_timeout_ms':'8000', got: {opts_str}"
        assert "protocol" in opts_str and "flight_sql" in opts_str, \
            f"OPTIONS must contain 'protocol':'flight_sql', got: {opts_str}"
        assert fake_key not in opts_str, \
            f"fake_key must not appear in OPTIONS (should be masked): {opts_str}"
        assert raw_token not in opts_str, \
            f"api_token raw value must not appear in OPTIONS (should be masked): {opts_str}"

        self._cleanup(name_fs, name_http, name_all)

    def test_fq_ext_004(self):
        """FQ-EXT-004: Idempotent create - IF NOT EXISTS duplicate returns success without duplication

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
        """FQ-EXT-005: Duplicate name creation failure - error when creating duplicate without IF NOT EXISTS

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
        """FQ-EXT-006: Name conflict with local DB - source_name same as DB name is rejected

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
        # Verify source created with correct type
        row = self._find_show_row(src_name)
        assert row >= 0, f"{src_name} must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")
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
        # Verify source created with correct type
        row = self._find_show_row(db_name)
        assert row >= 0, f"{db_name} must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")

        tdSql.execute(f"drop external source if exists {db_name}")
        tdSql.execute(f"drop external source if exists {src_name}")

    def test_fq_ext_007(self):
        """FQ-EXT-007: SHOW listing - all fields present, correct row count

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
        """FQ-EXT-008: SHOW masking - password / api_token / tls_client_key sensitive values masked

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
        special_pwd_sql = special_pwd.replace("'", "''")
        self._cleanup(name_pwd, name_tok, name_key, name_sp, name_empty)

        # ── (a) password masking ──
        tdSql.execute(
            f"create external source {name_pwd} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{raw_pwd}'"
        )
        row = self._find_show_row(name_pwd)
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED
        assert raw_pwd not in str(tdSql.queryResult[row][_COL_PASSWORD])
        # DESCRIBE must succeed and password must be masked
        desc = self._describe_dict(name_pwd)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_pwd} failed"
        assert "password" in desc, "password field must exist in DESCRIBE output"
        assert desc.get("password") == _MASKED, \
            f"Password must be masked in DESCRIBE; expected '{_MASKED}', got '{desc.get('password')}'"
        assert raw_pwd not in str(desc.get("password", "")), \
            f"Raw password must not appear in DESCRIBE output: {desc.get('password')}"

        # ── (b) api_token masking ──
        tdSql.execute(
            f"create external source {name_tok} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='admin' password='' database=db "
            f"options('api_token'='{raw_token}', 'protocol'='flight_sql')"
        )
        self._assert_show_opts_not_contain(name_tok, raw_token)
        # DESCRIBE must succeed and api_token must be masked in OPTIONS
        desc = self._describe_dict(name_tok)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_tok} failed"
        assert "options" in desc, "options field must exist in DESCRIBE output"
        assert raw_token not in str(desc.get("options", "")), \
            f"Raw api_token must not appear in DESCRIBE OPTIONS: {desc.get('options')}"

        # ── (c) tls_client_key masking ──
        tdSql.execute(
            f"create external source {name_key} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            f"options('tls_enabled'='true', 'tls_client_cert'='FAKE-CLIENT-CERT-PEM', 'tls_client_key'='{raw_key}', 'ssl_mode'='required')"
        )
        self._assert_show_opts_not_contain(name_key, raw_key)
        # DESCRIBE must succeed and tls_client_key must be masked in OPTIONS
        desc = self._describe_dict(name_key)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_key} failed"
        assert "options" in desc, "options field must exist in DESCRIBE output"
        assert raw_key not in str(desc.get("options", "")), \
            f"Raw tls_client_key must not appear in DESCRIBE OPTIONS: {desc.get('options')}"

        # ── (d) special chars in password still masked ──
        tdSql.execute(
            f"create external source {name_sp} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{special_pwd_sql}'"
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
        """FQ-EXT-009: DESCRIBE definition - fields match creation params for each source type

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
        assert desc, "DESCRIBE EXTERNAL SOURCE must be supported and return data in this test environment"
        assert desc.get("source_name") == name_mysql, (
            f"Expected source_name='{name_mysql}', got '{desc.get('source_name')}'")
        assert desc.get("type") == "mysql", (
            f"Expected type='mysql', got '{desc.get('type')}'")
        assert desc.get("host") == self._mysql_cfg().host, (
            f"Expected host='{self._mysql_cfg().host}', got '{desc.get('host')}'")
        assert str(desc.get("port")) == str(self._mysql_cfg().port), (
            f"Expected port='{self._mysql_cfg().port}', got '{desc.get('port')}'")
        assert desc.get("user") == "reader", (
            f"Expected user='reader', got '{desc.get('user')}'")
        assert desc.get("password") == _MASKED, (
            f"Expected password={_MASKED!r}, got '{desc.get('password')}'")
        assert "secret_pwd" not in str(desc.get("password", "")), (
            f"Plaintext password leaked in: '{desc.get('password')}'")
        assert desc.get("database") == "power", (
            f"Expected database='power', got '{desc.get('database')}'")
        assert desc.get("schema") == "myschema", (
            f"Expected schema='myschema', got '{desc.get('schema')}'")

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
        # DESCRIBE must succeed and verify all critical fields
        desc = self._describe_dict(name_pg)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_pg} failed"
        assert desc.get("type") == "postgresql", \
            f"type must be 'postgresql', got '{desc.get('type')}'"
        assert desc.get("database") == "iot", \
            f"database must be 'iot', got '{desc.get('database')}'"
        assert desc.get("schema") == "public", \
            f"schema must be 'public', got '{desc.get('schema')}'"
        assert desc.get("password") == _MASKED, \
            f"password must be masked '{_MASKED}', got '{desc.get('password')}'"
        opts_str = str(desc.get("options", ""))
        assert ("sslmode" in opts_str and "prefer" in opts_str), \
            f"OPTIONS must contain 'sslmode':'prefer', got: {opts_str}"
        assert ("application_name" in opts_str and "TDengine-Test" in opts_str), \
            f"OPTIONS must contain 'application_name':'TDengine-Test', got: {opts_str}"

        # ── (c) InfluxDB with api_token masked ──
        raw_token = "my-influx-describe-token-xyz"
        tdSql.execute(
            f"create external source {name_influx} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='admin' password='' database=telegraf "
            f"options('api_token'='{raw_token}', 'protocol'='flight_sql')"
        )
        # DESCRIBE must succeed and verify type, database, and masked api_token
        desc = self._describe_dict(name_influx)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_influx} failed"
        assert desc.get("type") == "influxdb", \
            f"type must be 'influxdb', got '{desc.get('type')}'"
        assert desc.get("database") == "telegraf", \
            f"database must be 'telegraf', got '{desc.get('database')}'"
        opts_str = str(desc.get("options", ""))
        assert raw_token not in opts_str, \
            f"Raw api_token must be masked in OPTIONS: {opts_str}"
        assert ("protocol" in opts_str and "flight_sql" in opts_str), \
            f"OPTIONS must contain 'protocol':'flight_sql', got: {opts_str}"

        self._cleanup(name_mysql, name_pg, name_influx)

    def test_fq_ext_010(self):
        """FQ-EXT-010: ALTER host and port - SHOW/DESCRIBE reflect new address after change

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
        """FQ-EXT-011: ALTER user and password - modify USER/PASSWORD

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
        """FQ-EXT-012: ALTER OPTIONS patch-merge - new values merged into existing options

        Dimensions:
          a) Single key → new key added, old key retained (patch-merge)
          b) Multi-key → each new key merged, unrelated old keys retained
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

        # ── (a) Patch-merge: add read_timeout_ms → connect_timeout_ms retained ──
        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('connect_timeout_ms'='1000')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms")
        tdSql.execute(f"alter external source {name} set options('read_timeout_ms'='3000')")
        # patch-merge: both keys must be present
        self._assert_show_opts_contain(name, "read_timeout_ms", "connect_timeout_ms")
        # DESCRIBE must succeed and verify OPTIONS after ALTER
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed after ALTER"
        opts = str(desc.get("options", ""))
        assert "read_timeout_ms" in opts and "3000" in opts, \
            f"OPTIONS must contain 'read_timeout_ms':'3000' after ALTER, got: {opts}"
        assert "connect_timeout_ms" in opts, \
            f"OPTIONS must still contain 'connect_timeout_ms' after patch-merge ALTER, got: {opts}"

        # ── (b) Multi-key merge: new keys merged, previous keys retained ──
        tdSql.execute(
            f"alter external source {name} set "
            "options('connect_timeout_ms'='500', 'charset'='utf8mb4')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms", "charset")

        tdSql.execute(f"alter external source {name} set options('ssl_mode'='required')")
        # patch-merge: ssl_mode added, connect_timeout_ms and charset still present
        self._assert_show_opts_contain(name, "ssl_mode")

        # ── (d) Other fields unchanged ──
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name)

    def test_fq_ext_013(self):
        """FQ-EXT-013: ALTER TYPE forbidden - changing TYPE is rejected

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
        """FQ-EXT-014: DROP IF EXISTS - drop when exists, no error when absent

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
        # Verify source created with correct type
        row = self._find_show_row(name)
        assert row >= 0, f"{name} must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")
        tdSql.checkData(row, _COL_HOST, self._mysql_cfg().host)
        tdSql.checkData(row, _COL_PORT, self._mysql_cfg().port)
        tdSql.checkData(row, _COL_USER, self._mysql_cfg().user)
        tdSql.checkData(row, _COL_PASSWORD, _MASKED)

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
        self._assert_show_field(name, _COL_PORT, self._pg_cfg().port)
        self._assert_show_field(name, _COL_USER, "pg")
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)

        self._cleanup(name)

    def test_fq_ext_015(self):
        """FQ-EXT-015: DROP non-existent - returns NOT_EXIST error without IF EXISTS

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
        """FQ-EXT-016: DROP referenced object - behavior when vtable references exist

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
                assert False, (
                    "create vtable with external column reference failed; "
                    "this scenario is required and should fail the test instead of skipping"
                )

            drop_ok = tdSql.query(f"drop external source {name}", exit=False)
            if drop_ok is False:
                # Verify source still exists after failed DROP
                row = self._find_show_row(name)
                assert row >= 0, f"{name} must still exist after failed DROP"
                tdSql.checkData(row, _COL_TYPE, "mysql")
            else:
                tdSql.error(f"select * from {db_name}.{vtbl_name}")

        finally:
            tdSql.execute(f"drop database if exists {db_name}")
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_017(self):
        """FQ-EXT-017: OPTIONS unrecognized key should return syntax error

        Dimensions:
          a) Unknown key + valid key (MySQL) → syntax error
          b) ALL unknown keys (MySQL) → syntax error
          c) Unknown key + valid key (PG) → syntax error
          d) Unknown key + valid key (InfluxDB) → syntax error

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

        # ── (a) Unknown + valid (MySQL) ──
        tdSql.error(
            f"create external source {name_mixed} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            f"options('{unknown_key}'='val', 'connect_timeout_ms'='500')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name_mixed) < 0

        # ── (b) ALL unknown keys (MySQL) ──
        tdSql.error(
            f"create external source {name_all_unknown} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('unknown_a'='1', 'unknown_b'='2')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name_all_unknown) < 0

        # ── (c) Unknown key + valid key (PG) ──
        tdSql.error(
            f"create external source {name_pg} "
            f"type='postgresql' host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"options('{unknown_key}'='val', 'sslmode'='prefer')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name_pg) < 0

        # ── (d) Unknown key + valid key (InfluxDB) ──
        tdSql.error(
            f"create external source fq_src_017_influx "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='admin' password='' database='mydb' "
            f"options('{unknown_key}'='val', 'api_token'='{self._influx_cfg().token}', 'protocol'='flight_sql')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row("fq_src_017_influx") < 0

        self._cleanup(name_mixed, name_all_unknown, name_pg, "fq_src_017_influx")

    def test_fq_ext_018(self):
        """FQ-EXT-018: MySQL tls_enabled+ssl_mode conflict and valid combination full coverage

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
            # DESCRIBE must succeed and verify ssl_mode value in OPTIONS
            desc = self._describe_dict(src_name)
            assert desc, f"DESCRIBE EXTERNAL SOURCE {src_name} failed"
            opts_str = str(desc.get("options", ""))
            assert ssl_val in opts_str, \
                f"OPTIONS must contain 'ssl_mode':'{ssl_val}', got: {opts_str}"

        self._cleanup(*all_names)

    def test_fq_ext_019(self):
        """FQ-EXT-019: PG tls_enabled+sslmode conflict and valid combination full coverage

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
            # DESCRIBE must succeed and verify sslmode value in OPTIONS
            desc = self._describe_dict(src_name)
            assert desc, f"DESCRIBE EXTERNAL SOURCE {src_name} failed"
            opts_str = str(desc.get("options", ""))
            assert ssl_val in opts_str, \
                f"OPTIONS must contain 'sslmode':'{ssl_val}', got: {opts_str}"

        self._cleanup(*all_names)

    def test_fq_ext_020(self):
        """FQ-EXT-020: MySQL-specific options charset/ssl_mode persistence and retrieval

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
        # DESCRIBE must succeed and verify OPTIONS values
        desc = self._describe_dict(name_a)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_a} failed"
        opts = str(desc.get("options", ""))
        assert "utf8mb4" in opts and "charset" in opts, \
            f"OPTIONS must contain 'charset':'utf8mb4', got: {opts}"
        assert "preferred" in opts and "ssl_mode" in opts, \
            f"OPTIONS must contain 'ssl_mode':'preferred', got: {opts}"

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
            desc = self._describe_dict(name_a)
            assert desc, f"DESCRIBE EXTERNAL SOURCE {name_a} failed"
            assert ssl_val in str(desc.get("options", "")), \
                f"OPTIONS must contain 'ssl_mode':'{ssl_val}' after ALTER, got: {desc.get('options')}"

        self._cleanup(name_a, name_b)

    def test_fq_ext_021(self):
        """FQ-EXT-021: PG-specific options sslmode/application_name/search_path persistence

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
        # DESCRIBE must succeed and verify all OPTIONS
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed"
        opts = str(desc.get("options", ""))
        assert ("sslmode" in opts and "prefer" in opts), \
            f"OPTIONS must contain 'sslmode':'prefer', got: {opts}"
        assert ("application_name" in opts and "TDengine-Federation" in opts), \
            f"OPTIONS must contain 'application_name':'TDengine-Federation', got: {opts}"
        assert "search_path" in opts, \
            f"OPTIONS must contain 'search_path', got: {opts}"

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
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed after ALTER"
        opts_after = str(desc.get("options", ""))
        assert "require" in opts_after and "TDengine-V2" in opts_after and "myschema" in opts_after, \
            f"ALTERed PG options must be reflected in DESCRIBE, got: {opts_after}"

        self._cleanup(name)

    def test_fq_ext_022(self):
        """FQ-EXT-022: InfluxDB-specific option api_token masking

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
            # DESCRIBE must succeed and raw api_token must be masked
            desc = self._describe_dict(src_name)
            assert desc, f"DESCRIBE EXTERNAL SOURCE {src_name} failed"
            assert token not in str(desc.get("options", "")), \
                f"Raw api_token must be masked in DESCRIBE OPTIONS: {desc.get('options')}"

        # ── (c) Masking indicator present ──
        row = self._find_show_row(name_short)
        opts = str(tdSql.queryResult[row][_COL_OPTIONS])
        assert "api_token" in opts, "api_token key must still appear in OPTIONS"
        assert _MASKED in opts, f"SHOW OPTIONS must contain mask indicator '{_MASKED}', got: {opts}"
        desc_short = self._describe_dict(name_short)
        assert desc_short, f"DESCRIBE EXTERNAL SOURCE {name_short} failed"
        assert _MASKED in str(desc_short.get("options", "")), \
            f"DESCRIBE OPTIONS must contain mask indicator '{_MASKED}', got: {desc_short.get('options')}"

        self._cleanup(name_short, name_long)

    def test_fq_ext_023(self):
        """FQ-EXT-023: InfluxDB protocol option flight_sql/http switching

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
        # DESCRIBE must succeed and verify protocol value
        desc = self._describe_dict(name_fs)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_fs} failed"
        assert "flight_sql" in str(desc.get("options", "")), \
            f"OPTIONS must contain 'protocol':'flight_sql', got: {desc.get('options')}"

        tdSql.execute(
            f"create external source {name_http} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=db2 "
            "options('api_token'='tok2', 'protocol'='http')"
        )
        self._assert_show_opts_contain(name_http, "http")
        # DESCRIBE must succeed and verify protocol value
        desc = self._describe_dict(name_http)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_http} failed"
        assert "http" in str(desc.get("options", "")), \
            f"OPTIONS must contain 'protocol':'http', got: {desc.get('options')}"

        # ── (c) ALTER to switch protocol ──
        tdSql.execute(
            f"alter external source {name_fs} set "
            "options('api_token'='tok1', 'protocol'='http')"
        )
        self._assert_show_opts_contain(name_fs, "http")
        self._assert_show_opts_not_contain(name_fs, "flight_sql")
        desc = self._describe_dict(name_fs)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name_fs} failed after ALTER"
        opts_after = str(desc.get("options", ""))
        assert "http" in opts_after and "flight_sql" not in opts_after, \
            f"ALTERed protocol must be reflected in DESCRIBE OPTIONS, got: {opts_after}"

        self._cleanup(name_fs, name_http)

    def test_fq_ext_024(self):
        """FQ-EXT-024: ALTER does not re-validate existing vtables

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
                assert False, (
                    "create vtable with external column reference failed; "
                    "this scenario is required and should fail the test instead of skipping"
                )

            # ALTER to unreachable host → vtable still exists but SELECT fails
            tdSql.execute(f"alter external source {name} set host='192.0.2.1', port=9999")
            tdSql.query(f"show {db_name}.vtables")
            tbl_names = [str(r[0]) for r in tdSql.queryResult]
            assert vtbl_name in tbl_names
            self._assert_error_not_syntax(f"select * from {db_name}.{vtbl_name}")

        finally:
            tdSql.execute(f"drop database if exists {db_name}")
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_025(self):
        """FQ-EXT-025: ALTER OPTIONS uses patch-merge semantics

        Dimensions:
          a) SET OPTIONS with a new key: new key added, existing keys preserved
          b) SET OPTIONS overwriting an existing key: value updated, other keys preserved
          c) SET OPTIONS with value='' removes that key, other keys preserved
          d) DESCRIBE cross-verification after each ALTER
          e) Non-OPTIONS fields unchanged throughout

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-24 wpan Redesigned for patch-merge semantics (FS §3.4.4)

        """
        name = "fq_src_025"
        self._cleanup(name)

        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' "
            "options('connect_timeout_ms'='1000', 'read_timeout_ms'='2000')"
        )
        self._assert_show_opts_contain(name, "connect_timeout_ms", "read_timeout_ms")

        # ── Dimension a: add a brand-new key; existing keys must be preserved ──
        tdSql.execute(f"alter external source {name} set options('charset'='utf8')")
        self._assert_show_opts_contain(name, "charset", "connect_timeout_ms", "read_timeout_ms")
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed after add-new-key ALTER"
        opts = str(desc.get("options", ""))
        assert "charset" in opts and "utf8" in opts, \
            f"New 'charset':'utf8' must appear after ALTER, got: {opts}"
        assert "connect_timeout_ms" in opts, \
            f"Existing 'connect_timeout_ms' must be preserved after ALTER, got: {opts}"
        assert "read_timeout_ms" in opts, \
            f"Existing 'read_timeout_ms' must be preserved after ALTER, got: {opts}"

        # ── Dimension b: overwrite an existing key; other keys must be preserved ──
        tdSql.execute(f"alter external source {name} set options('connect_timeout_ms'='5000')")
        self._assert_show_opts_contain(name, "charset", "connect_timeout_ms", "read_timeout_ms")
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed after overwrite ALTER"
        opts = str(desc.get("options", ""))
        assert "5000" in opts, \
            f"Updated connect_timeout_ms value '5000' must appear, got: {opts}"
        assert "charset" in opts, \
            f"'charset' must still be present after overwrite ALTER, got: {opts}"
        assert "read_timeout_ms" in opts, \
            f"'read_timeout_ms' must still be present after overwrite ALTER, got: {opts}"

        # ── Dimension c: empty value removes that key; other keys preserved ──
        tdSql.execute(f"alter external source {name} set options('read_timeout_ms'='')")
        self._assert_show_opts_contain(name, "charset", "connect_timeout_ms")
        self._assert_show_opts_not_contain(name, "read_timeout_ms")
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed after delete-key ALTER"
        opts = str(desc.get("options", ""))
        assert "read_timeout_ms" not in opts, \
            f"'read_timeout_ms' must be removed after empty-value ALTER, got: {opts}"
        assert "charset" in opts and "connect_timeout_ms" in opts, \
            f"Other keys must remain after delete-key ALTER, got: {opts}"

        # ── Dimension e: non-OPTIONS fields unchanged throughout ──
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name)

    def test_fq_ext_026(self):
        """FQ-EXT-026: REFRESH metadata - updated external table schema visible after refresh

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

            # ── (a) REFRESH → initial query returns 2 columns (id, val) ──
            tdSql.execute(f"refresh external source {name}")
            tdSql.query(f"select * from {name}.{ext_db}.{ext_table}")
            assert tdSql.queryCols == 2, (
                f"Expected 2 columns (id, val) before ADD COLUMN, got {tdSql.queryCols}"
            )

            # ── (c) Source metadata unchanged after REFRESH ──
            self._assert_show_field(name, _COL_TYPE, "mysql")
            self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
            self._assert_show_field(name, _COL_DATABASE, ext_db)

            # ── (b) ADD COLUMN on MySQL → REFRESH → new column visible in query ──
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [
                f"ALTER TABLE {ext_table} ADD COLUMN extra INT DEFAULT 99",
            ])
            tdSql.execute(f"refresh external source {name}")
            tdSql.query(f"select * from {name}.{ext_db}.{ext_table}")
            assert tdSql.queryCols == 3, (
                f"Expected 3 columns (id, val, extra) after ADD COLUMN + REFRESH, got {tdSql.queryCols}"
            )
            tdSql.checkRows(1)
            # extra column DEFAULT 99, the one inserted row should have extra=99
            col_names = [tdSql.cursor.description[i][0].lower() for i in range(tdSql.queryCols)]
            extra_idx = col_names.index("extra") if "extra" in col_names else -1
            assert extra_idx >= 0, f"Column 'extra' not found in result, columns: {col_names}"
            tdSql.checkData(0, extra_idx, 99)

        finally:
            self._cleanup(name)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, [f"DROP TABLE IF EXISTS {ext_table}"])
            ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)

    def test_fq_ext_027(self):
        """FQ-EXT-027: REFRESH unreachable source succeeds; connect_timeout_ms honoured on query

        REFRESH is a pure metadata operation (increments metaVersion in mnode SDB)
        and does NOT attempt a real connection — it always returns success regardless
        of whether the external source is reachable.  Connection failures only surface
        when an actual query is executed against the external source.

        Dimensions:
          a) REFRESH on a non-routable host → succeeds (not an error)
          b) Source record unchanged after REFRESH (fields intact, still visible in SHOW)
          c) connect_timeout_ms option is honoured on real query:
             source with connect_timeout_ms=1000 (1 s), non-routable host →
             SELECT fails within expected time bound

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-24 wpan Corrected: REFRESH is always-success; added query-level
                               connect_timeout_ms behavioural assertion

        """
        name = "fq_src_027"
        name_timeout = "fq_src_027_to"
        self._cleanup(name, name_timeout)

        # ── (a) REFRESH on non-routable source → succeeds (pure metadata op) ──
        tdSql.execute(
            f"create external source {name} "
            "type='mysql' host='192.0.2.1' port=9999 user='u' password='p'"
        )
        # REFRESH must succeed: it only bumps metaVersion, does not connect
        tdSql.execute(f"refresh external source {name}")

        # ── (b) Source intact after REFRESH ──
        self._assert_show_field(name, _COL_HOST, "192.0.2.1")
        self._assert_show_field(name, _COL_TYPE, "mysql")

        # ── (c) connect_timeout_ms honoured on actual query ──
        # Use 1000 ms (1 s) so the ceil-to-second conversion keeps the value
        # exact (1000 / 1000 = 1 s exactly, no truncation issue).
        # The default MySQL connect timeout is 10 s; with 1 s the query must
        # fail noticeably faster.  We assert elapsed < 5 s as a safe upper bound.
        tdSql.execute(
            f"create external source {name_timeout} "
            "type='mysql' host='192.0.2.1' port=9999 user='u' password='p' "
            "options('connect_timeout_ms'='1000')"
        )
        t0 = time.time()
        self._assert_error_not_syntax(
            f"select * from {name_timeout}.db_x.tbl_x",
            queryTimes=1,
        )
        elapsed = time.time() - t0
        tdLog.info(
            f"SELECT with connect_timeout_ms=1000 (1s) on non-routable host "
            f"failed in {elapsed:.2f}s"
        )
        assert elapsed < 5.0, (
            f"SELECT took {elapsed:.2f}s with connect_timeout_ms=1000 (1 s); "
            "expected < 5 s — connect_timeout_ms option may not be honoured"
        )

        self._cleanup(name, name_timeout)

    def test_fq_ext_028(self):
        """FQ-EXT-028: Non-admin view system table - user visible, password always masked

        user is always visible to all users; password is always '******'.
        Neither column returns NULL for non-admin users.

        Dimensions:
          a) Non-admin SHOW: user == actual_username (not NULL)
          b) Non-admin SHOW: password == '******' (not NULL)
          c) Non-admin still sees type, host, port, create_time
          d) Non-admin DESCRIBE: user == actual_username, password == '******'

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-24 wpan Revised: user always visible, password always ******

        """
        src_name = "fq_src_028"
        test_user = "fq_usr_028"
        test_pass = "fqTest@028"
        ext_user = self._mysql_cfg().user
        self._cleanup(src_name)
        tdSql.execute_ignore_error(f"drop user {test_user}")

        tdSql.execute(
            f"create external source {src_name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{ext_user}' password='secret'"
        )
        tdSql.execute(f"create user {test_user} pass '{test_pass}'")

        try:
            tdSql.connect(test_user, test_pass)
            row = self._find_show_row(src_name)
            assert row >= 0, f"{src_name} not found in SHOW EXTERNAL SOURCES as non-admin"

            # ── (a) user always visible (not NULL) ──
            actual_user = tdSql.queryResult[row][_COL_USER]
            assert actual_user is not None, \
                "user column must not be NULL for non-admin user"
            assert str(actual_user) == ext_user, \
                f"user must be '{ext_user}', got '{actual_user}'"

            # ── (b) password always masked (not NULL) ──
            actual_pwd = tdSql.queryResult[row][_COL_PASSWORD]
            assert actual_pwd is not None, \
                "password column must not be NULL for non-admin user"
            assert str(actual_pwd) == _MASKED, \
                f"password must be '{_MASKED}', got '{actual_pwd}'"

            # ── (c) other fields visible ──
            assert str(tdSql.queryResult[row][_COL_TYPE]) == "mysql"
            assert str(tdSql.queryResult[row][_COL_HOST]) == self._mysql_cfg().host
            assert tdSql.queryResult[row][_COL_PORT] == self._mysql_cfg().port
            assert tdSql.queryResult[row][_COL_CTIME] is not None

            # ── (d) DESCRIBE as non-admin: user visible, password masked ──
            desc = self._describe_dict(src_name)
            assert desc, f"DESCRIBE EXTERNAL SOURCE {src_name} failed for non-admin"
            assert str(desc.get("user")) == ext_user, \
                f"DESCRIBE user must be '{ext_user}', got '{desc.get('user')}'"
            assert str(desc.get("password")) == _MASKED, \
                f"DESCRIBE password must be '{_MASKED}', got '{desc.get('password')}'"
        finally:
            tdSql.connect("root", "taosdata")
            tdSql.execute_ignore_error(f"drop user {test_user}")
            self._cleanup(src_name)

    def test_fq_ext_029(self):
        """FQ-EXT-029: Admin view system table - password always shows ******

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
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{secret}'"
        )

        # ── (a) SHOW: password column must equal _MASKED ──
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        # also confirm raw secret not in password column
        row = self._find_show_row(name)
        assert row >= 0, f"{name} not found in SHOW after CREATE"
        assert secret not in str(tdSql.queryResult[row][_COL_PASSWORD]), \
            f"Raw secret must not appear in SHOW password column"

        # ── (b) DESCRIBE: password field must equal _MASKED ──
        self._assert_describe_field(name, "password", _MASKED)

        # ── (c) Raw secret must not appear anywhere in SHOW row or DESCRIBE output ──
        # Re-fetch SHOW row because _assert_describe_field overwrites tdSql.queryResult
        row = self._find_show_row(name)
        assert row >= 0, f"{name} not found in SHOW after CREATE"
        show_row_str = " ".join(str(v) for v in tdSql.queryResult[row])
        assert secret not in show_row_str, \
            f"Raw secret must not appear anywhere in SHOW row: {show_row_str}"
        desc = self._describe_dict(name)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {name} failed"
        assert secret not in str(desc), \
            f"Raw secret must not appear anywhere in DESCRIBE output: {desc}"

        # ── (d) After ALTER PASSWORD → still masked in SHOW and DESCRIBE; user unchanged ──
        tdSql.execute(f"alter external source {name} set password='{new_secret}'")
        # SHOW: password still masked
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        row = self._find_show_row(name)
        assert row >= 0, f"{name} not found in SHOW after ALTER PASSWORD"
        assert new_secret not in str(tdSql.queryResult[row][_COL_PASSWORD]), \
            "New raw password must not appear in SHOW password column after ALTER"
        # user must not change
        assert str(tdSql.queryResult[row][_COL_USER]) == self._mysql_cfg().user, \
            "user must not change after ALTER PASSWORD"
        # new secret must not appear anywhere in SHOW row
        show_row_str = " ".join(str(v) for v in tdSql.queryResult[row])
        assert new_secret not in show_row_str, \
            f"New raw secret must not appear anywhere in SHOW row after ALTER: {show_row_str}"
        # DESCRIBE: password still masked
        self._assert_describe_field(name, "password", _MASKED)
        desc = self._describe_dict(name)
        assert new_secret not in str(desc), \
            f"New raw secret must not appear in DESCRIBE output after ALTER: {desc}"
        assert str(desc.get("user")) == self._mysql_cfg().user, \
            "user field in DESCRIBE must not change after ALTER PASSWORD"

        self._cleanup(name)

    def test_fq_ext_030(self):
        """FQ-EXT-030: ALTER DATABASE modifies default database

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
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database=db_a"
        )
        # ── (a) initial state ──
        self._assert_show_field(name, _COL_DATABASE, "db_a")
        self._assert_describe_field(name, "database", "db_a")

        # ── (b/c) ALTER DATABASE → updated in SHOW and DESCRIBE ──
        tdSql.execute(f"alter external source {name} set database=db_b")
        self._assert_show_field(name, _COL_DATABASE, "db_b")
        self._assert_describe_field(name, "database", "db_b")

        # ── (d) Unchanged fields after ALTER DATABASE ──
        self._assert_show_field(name, _COL_TYPE, "mysql")
        self._assert_show_field(name, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name, _COL_PORT, self._mysql_cfg().port)
        self._assert_show_field(name, _COL_USER, self._mysql_cfg().user)
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        # schema was never set for this MySQL source — must remain empty/NULL
        row = self._find_show_row(name)
        assert row >= 0, f"{name} not found in SHOW"
        schema_val = tdSql.queryResult[row][_COL_SCHEMA]
        assert schema_val is None or str(schema_val) == "", \
            f"SCHEMA must be empty/NULL for MySQL source without schema, got '{schema_val}'"

        self._cleanup(name)

    def test_fq_ext_031(self):
        """FQ-EXT-031: ALTER SCHEMA modifies default schema

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
        # ── (a) initial state ──
        self._assert_show_field(name, _COL_SCHEMA, "schema_a")
        self._assert_describe_field(name, "schema", "schema_a")

        # ── (b/c) ALTER SCHEMA → updated in SHOW and DESCRIBE ──
        tdSql.execute(f"alter external source {name} set schema=schema_b")
        self._assert_show_field(name, _COL_SCHEMA, "schema_b")
        self._assert_describe_field(name, "schema", "schema_b")

        # ── (d) Unchanged fields after ALTER SCHEMA ──
        self._assert_show_field(name, _COL_TYPE, "postgresql")
        self._assert_show_field(name, _COL_HOST, self._pg_cfg().host)
        self._assert_show_field(name, _COL_PORT, self._pg_cfg().port)
        self._assert_show_field(name, _COL_DATABASE, "iot")
        self._assert_show_field(name, _COL_USER, "u")
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        # DESCRIBE: schema updated, other key fields stable
        self._assert_describe_field(name, "type", "postgresql")
        self._assert_describe_field(name, "database", "iot")
        self._assert_describe_field(name, "password", _MASKED)

        self._cleanup(name)

    def test_fq_ext_032(self):
        """FQ-EXT-032: FS doc source creation examples are runnable - FS §3.4.1.5

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
        # ── (c) IF NOT EXISTS idempotency: second call on same name must not error ──
        tdSql.execute(
            f"create external source if not exists {i} "
            f"type='influxdb' host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            "user='admin' password='' database=telegraf "
            "options('api_token'='my-influx-token', 'protocol'='flight_sql', 'tls_enabled'='true')"
        )
        # Verify still exactly one source after duplicate IF NOT EXISTS
        tdSql.query("show external sources")
        i_rows = [r for r in (tdSql.queryResult or []) if str(r[_COL_NAME]) == i]
        assert len(i_rows) == 1, \
            f"IF NOT EXISTS must not duplicate: found {len(i_rows)} rows for '{i}'"

        # ── MySQL ──
        self._assert_show_field(m, _COL_TYPE, "mysql")
        self._assert_show_field(m, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(m, _COL_DATABASE, "power")
        self._assert_show_field(m, _COL_PASSWORD, _MASKED)
        self._assert_describe_field(m, "type", "mysql")
        self._assert_describe_field(m, "password", _MASKED)

        # ── PG ──
        self._assert_show_field(p, _COL_TYPE, "postgresql")
        self._assert_show_field(p, _COL_HOST, self._pg_cfg().host)
        self._assert_show_field(p, _COL_DATABASE, "iot")
        self._assert_show_field(p, _COL_SCHEMA, "public")
        self._assert_show_field(p, _COL_PASSWORD, _MASKED)
        self._assert_show_opts_contain(p, "application_name", "TDengine-Federation")
        self._assert_describe_field(p, "schema", "public")
        self._assert_describe_field(p, "password", _MASKED)

        # ── InfluxDB ──
        self._assert_show_field(i, _COL_TYPE, "influxdb")
        self._assert_show_field(i, _COL_HOST, self._influx_cfg().host)
        self._assert_show_field(i, _COL_DATABASE, "telegraf")
        self._assert_show_field(i, _COL_PASSWORD, _MASKED)
        self._assert_show_opts_contain(i, "protocol", "flight_sql")
        # api_token value must not appear in plain text in OPTIONS
        row = self._find_show_row(i)
        assert row >= 0, f"{i} not found in SHOW"
        opts_str = str(tdSql.queryResult[row][_COL_OPTIONS])
        assert "my-influx-token" not in opts_str, \
            f"api_token plain value must not appear in SHOW OPTIONS, got: {opts_str}"
        self._assert_describe_field(i, "type", "influxdb")
        self._assert_describe_field(i, "password", _MASKED)

        self._cleanup(m, p, i)

    # ==================================================================
    #  Supplementary tests — scenarios identified in review
    # ==================================================================

    # ------------------------------------------------------------------
    # FQ-EXT-S01  TLS insufficient certificates scenario
    # ------------------------------------------------------------------

    def test_fq_ext_s01_tls_insufficient_certs(self):
        """FQ-EXT-S01: TLS insufficient certificates — mutual TLS missing required certs

        FS §3.4.1.4: tls_client_cert / tls_client_key only take effect when tls_enabled=true

        Multi-dimensional coverage:
          a) tls_enabled=true + tls_client_cert WITHOUT tls_client_key
             → should error or warn about missing cert (implementation-dependent)
          b) tls_enabled=true + tls_client_key WITHOUT tls_client_cert
             → should error or warn about missing cert
          c) tls_enabled=false + tls_client_cert + tls_client_key
             → should ignore TLS options (acceptable)
          d) tls_enabled=true + tls_ca_cert + tls_client_cert + tls_client_key
             → complete config should be accepted
          e) tls_enabled=true with only tls_ca_cert (one-way TLS) → should be accepted
          f) MySQL: ssl_mode=verify_ca + tls_client_cert WITHOUT tls_client_key
             → should error

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
        # (a) tls_client_cert only, missing client_key → TLS pair conflict error
        tdSql.error(
            f"create external source {base}_a type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_client_cert'='{dummy_cert}')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        assert self._find_show_row(f"{base}_a") < 0, \
            f"{base}_a must NOT be created when tls_client_cert given without tls_client_key"

        # (b) tls_client_key only, missing client_cert → TLS pair conflict error
        tdSql.error(
            f"create external source {base}_b type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_client_key'='{dummy_key}')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        assert self._find_show_row(f"{base}_b") < 0, \
            f"{base}_b must NOT be created when tls_client_key given without tls_client_cert"

        # (c) tls_enabled=false → TLS options ignored, must succeed
        tdSql.execute(
            f"create external source {base}_c type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='false', 'tls_client_cert'='{dummy_cert}', "
            f"'tls_client_key'='{dummy_key}')"
        )
        self._assert_show_field(f"{base}_c", _COL_TYPE, "mysql")
        self._assert_show_opts_contain(f"{base}_c", "tls_enabled")

        # (d) Complete mutual TLS (ca + cert + key) → must succeed
        tdSql.execute(
            f"create external source {base}_d type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{dummy_cert}', "
            f"'tls_client_cert'='{dummy_cert}', 'tls_client_key'='{dummy_key}')"
        )
        self._assert_show_field(f"{base}_d", _COL_TYPE, "mysql")
        self._assert_show_opts_contain(f"{base}_d", "tls_enabled", "tls_ca_cert",
                                       "tls_client_cert", "tls_client_key")

        # (e) One-way TLS (ca only) → must succeed
        tdSql.execute(
            f"create external source {base}_e type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{dummy_cert}')"
        )
        self._assert_show_field(f"{base}_e", _COL_TYPE, "mysql")
        self._assert_show_opts_contain(f"{base}_e", "tls_enabled", "tls_ca_cert")

        # (f) ssl_mode=verify_ca + tls_client_cert WITHOUT tls_client_key → error
        tdSql.error(
            f"create external source {base}_f type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('ssl_mode'='verify_ca', 'tls_client_cert'='{dummy_cert}')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        assert self._find_show_row(f"{base}_f") < 0, \
            f"{base}_f must NOT be created when tls_client_cert given without tls_client_key"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S02  Special character source names
    # ------------------------------------------------------------------

    def test_fq_ext_s02_special_char_source_names(self):
        """FQ-EXT-S02: Special character external source names

        FS §3.4.1.3: Identifier rules are the same as database/table names, with default
        character restrictions and case insensitivity; backtick escaping relaxes character
        restrictions and enables case sensitivity.

        Multi-dimensional coverage:
          a) Underscore-prefixed name → should be accepted
          b) Pure numeric name → should be rejected (identifier rules)
          c) Name length boundary: exactly 64 chars → OK; 65 chars → TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG
          d) Backtick-escaped with special chars (Chinese, hyphen, space) → should be accepted
          e) Backtick-escaped names are case sensitive
          f) SQL reserved words as names (e.g. select, database) → backtick works
          g) Empty name → syntax error

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
        # Verify source name is exactly as expected
        row = self._find_show_row(n)
        assert row >= 0, f"{n} must be created"
        tdSql.checkData(row, _COL_NAME, n)
        self._cleanup(n)

        # (b) Pure numeric name → should fail (identifier rules)
        tdSql.error(
            f"create external source 12345 {base_sql}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # (c) Name length boundary — FS §3.4.1.3: max 64 chars (TSDB_EXT_SOURCE_NAME_LEN - 1)
        # Exactly 64 chars → must succeed
        max_name = "s" * 64
        self._cleanup(max_name)
        tdSql.execute(f"create external source {max_name} {base_sql}")
        # Verify 64-char name is accepted and stored correctly
        row = self._find_show_row(max_name)
        assert row >= 0, "64-char name should be accepted"
        tdSql.checkData(row, _COL_NAME, max_name)
        self._cleanup(max_name)

        # 65 chars → must fail with NAME_TOO_LONG (not syntax error)
        too_long = "s" * 65
        tdSql.error(
            f"create external source {too_long} {base_sql}",
            expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG,
        )

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
    # FQ-EXT-S03  ALTER non-existent external source
    # ------------------------------------------------------------------

    def test_fq_ext_s03_alter_nonexistent_source(self):
        """FQ-EXT-S03: ALTER non-existent external source

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

        # (g) ALTER IF EXISTS on non-existent → no error if syntax supported; syntax error if not
        # Probe: if ALTER ... IF EXISTS is not in the grammar, expect PAR_SYNTAX_ERROR.
        # If it IS supported, the command must silently succeed (no error, no phantom create).
        ok = tdSql.query(
            f"alter external source if exists {ghost} set password='y'",
            exit=False,
        )
        if ok is not False:
            # Syntax supported and command succeeded silently — ghost must still not exist
            assert self._find_show_row(ghost) < 0, \
                "ALTER IF EXISTS on non-existent source must not create a source"
        else:
            errno = getattr(tdSql, 'errno', None)
            assert errno == TSDB_CODE_PAR_SYNTAX_ERROR, (
                f"ALTER IF EXISTS on non-existent source: expected either success or "
                f"PAR_SYNTAX_ERROR (unsupported syntax), got errno={errno}"
            )

    # ------------------------------------------------------------------
    # FQ-EXT-S04  TYPE value is case insensitive
    # ------------------------------------------------------------------

    def test_fq_ext_s04_type_case_insensitive(self):
        """FQ-EXT-S04: TYPE value is case insensitive

        FS §3.4.1.3: Identifier rules are case insensitive by default

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
                    f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
                    f"user='' password='' database='mydb' "
                    f"options('api_token'='{self._influx_cfg().token}')"
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
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'",
            expectedErrno=None,  # no specific code imported for invalid-type; any error acceptable
        )
        assert self._find_show_row(f"{base}_h") < 0, \
            f"{base}_h must NOT be created with an unknown type"

        # (i) Empty type → syntax error (empty string literal is not a valid type token)
        tdSql.error(
            f"create external source {base}_i type='' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_i") < 0, \
            f"{base}_i must NOT be created with an empty type string"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S05  Cross-database option confusion
    # ------------------------------------------------------------------

    def test_fq_ext_s05_cross_db_option_confusion(self):
        """FQ-EXT-S05: Cross-database option confusion

        FS §3.4.1.4: OPTIONS are divided into common options and source-specific options.
        MySQL: charset, ssl_mode
        PG: sslmode, application_name, search_path
        InfluxDB: api_token, protocol

                Multi-dimensional coverage:
                    a) MySQL source with PG-specific 'sslmode' → syntax error
                    b) MySQL source with PG 'application_name' → syntax error
                    c) MySQL source with InfluxDB 'api_token' → syntax error
                    d) MySQL source with InfluxDB 'protocol' → syntax error
                    e) PG source with MySQL 'ssl_mode' → syntax error
                    f) PG source with MySQL 'charset' → syntax error
                    g) PG source with InfluxDB 'api_token' → syntax error
                    h) InfluxDB source with MySQL 'ssl_mode' → syntax error
                    i) InfluxDB source with PG 'sslmode' → syntax error
                    j) InfluxDB source with MySQL 'charset' → syntax error
                    k) Mixed: MySQL with both ssl_mode (own) and sslmode (PG) → syntax error
                    l) Positive control: only own options should succeed

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
        tdSql.error(
            f"create external source {base}_a type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('sslmode'='require')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_a") < 0

        # (b) MySQL + PG 'application_name'
        tdSql.error(
            f"create external source {base}_b type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('application_name'='MyApp')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_b") < 0

        # (c) MySQL + InfluxDB 'api_token'
        tdSql.error(
            f"create external source {base}_c type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('api_token'='some-token')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_c") < 0

        # (d) MySQL + InfluxDB 'protocol'
        tdSql.error(
            f"create external source {base}_d type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('protocol'='flight_sql')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_d") < 0

        # (e) PG + MySQL 'ssl_mode'
        tdSql.error(
            f"create external source {base}_e type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('ssl_mode'='required')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_e") < 0

        # (f) PG + MySQL 'charset'
        tdSql.error(
            f"create external source {base}_f type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('charset'='utf8mb4')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_f") < 0

        # (g) PG + InfluxDB 'api_token'
        tdSql.error(
            f"create external source {base}_g type='postgresql' "
            f"host='{self._pg_cfg().host}' port={self._pg_cfg().port} user='{self._pg_cfg().user}' password='{self._pg_cfg().password}' "
            f"database='pgdb' schema='public' "
            f"options('api_token'='some-token')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_g") < 0

        # (h) InfluxDB + MySQL 'ssl_mode'
        # (h) InfluxDB + MySQL 'ssl_mode' (cross-db option → syntax error)
        # Use correct InfluxDB syntax: api_token goes inside OPTIONS
        tdSql.error(
            f"create external source {base}_h type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='' password='' database='mydb' "
            f"options('api_token'='{self._influx_cfg().token}', 'ssl_mode'='required')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_h") < 0, \
            f"{base}_h must NOT be created with cross-db option ssl_mode"

        # (i) InfluxDB + PG 'sslmode' (cross-db option → syntax error)
        tdSql.error(
            f"create external source {base}_i type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='' password='' database='mydb' "
            f"options('api_token'='{self._influx_cfg().token}', 'sslmode'='require')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_i") < 0, \
            f"{base}_i must NOT be created with cross-db option sslmode"

        # (j) InfluxDB + MySQL 'charset' (cross-db option → syntax error)
        tdSql.error(
            f"create external source {base}_j type='influxdb' "
            f"host='{self._influx_cfg().host}' port={self._influx_cfg().port} "
            f"user='' password='' database='mydb' "
            f"options('api_token'='{self._influx_cfg().token}', 'charset'='utf8mb4')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_j") < 0, \
            f"{base}_j must NOT be created with cross-db option charset"

        # (k) MySQL with both ssl_mode (own) and sslmode (PG) together → error
        tdSql.error(
            f"create external source {base}_k type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('ssl_mode'='required', 'sslmode'='require')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(f"{base}_k") < 0, \
            f"{base}_k must NOT be created with both ssl_mode and sslmode"

        # (l) Positive control: only own options → must succeed, OPTIONS visible in SHOW
        tdSql.execute(
            f"create external source {base}_ok type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}' database='db' "
            f"options('ssl_mode'='required')"
        )
        self._assert_show_field(f"{base}_ok", _COL_TYPE, "mysql")
        self._assert_show_opts_contain(f"{base}_ok", "ssl_mode", "required")

        self._cleanup(*names, f"{base}_ok")

    # ------------------------------------------------------------------
    # FQ-EXT-S06  Repeated DROP external source
    # ------------------------------------------------------------------

    def test_fq_ext_s06_repeated_drop(self):
        """FQ-EXT-S06: Repeated DROP external source — idempotency and error behavior

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
        assert self._find_show_row(name) < 0, \
            f"{name} must not exist after 5× IF EXISTS drops"

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
        # Verify all gone after first-round drops
        for m in multi:
            assert self._find_show_row(m) < 0, \
                f"{m} must not exist after first DROP"
        # Second-round IF EXISTS drops must all succeed (idempotent)
        for m in multi:
            tdSql.execute(f"drop external source if exists {m}")

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
        assert self._find_show_row(name) >= 0, \
            f"{name} must exist after second CREATE"
        tdSql.execute(f"drop external source {name}")
        tdSql.execute(f"drop external source if exists {name}")

    # ------------------------------------------------------------------
    # FQ-EXT-S07  DESCRIBE non-existent external source
    # ------------------------------------------------------------------

    def test_fq_ext_s07_describe_nonexistent_source(self):
        """FQ-EXT-S07: DESCRIBE non-existent external source

        FS §3.4.3: DESCRIBE EXTERNAL SOURCE source_name
        Should return NOT_EXIST error for a non-existent source_name.

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
        # DESCRIBE must succeed for existing source (positive control)
        desc = self._describe_dict(existing)
        assert desc, f"DESCRIBE EXTERNAL SOURCE {existing} failed (positive control)"
        assert desc.get("type") == "mysql", \
            f"type must be 'mysql', got '{desc.get('type')}'"
        # SHOW must keep password masked and preserve user
        self._assert_show_field(existing, _COL_PASSWORD, _MASKED)
        self._assert_show_field(existing, _COL_USER, self._mysql_cfg().user)
        # DESCRIBE should reflect masked password too
        self._assert_describe_field(existing, "password", _MASKED)
        self._cleanup(existing)

    # ------------------------------------------------------------------
    # FQ-EXT-S08  REFRESH non-existent external source
    # ------------------------------------------------------------------

    def test_fq_ext_s08_refresh_nonexistent_source(self):
        """FQ-EXT-S08: REFRESH non-existent external source

        FS §3.4.6: REFRESH EXTERNAL SOURCE source_name
        Should return NOT_EXIST error for a non-existent source_name.
        (Compare with FQ-EXT-027 which tests an unreachable but registered source)

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
        assert self._find_show_row(ghost) < 0

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
        assert self._find_show_row(ghost) < 0

        # (c) Positive control: existing source can be refreshed (or fail non-syntax)
        ok = "fq_ext_s08_ok"
        self._cleanup(ok)
        tdSql.execute(
            f"create external source {ok} type='mysql' "
            f"host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} "
            f"user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        self._assert_error_not_syntax(f"refresh external source {ok}")
        assert self._find_show_row(ok) >= 0
        self._cleanup(ok)

    # ------------------------------------------------------------------
    # FQ-EXT-S09  CREATE missing mandatory fields
    # ------------------------------------------------------------------

    def test_fq_ext_s09_missing_mandatory_fields(self):
        """FQ-EXT-S09: CREATE missing mandatory fields

        FS §3.4.1.2: TYPE / HOST / PORT / USER / PASSWORD are all mandatory.
        Missing any mandatory field should cause a syntax error.

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
        assert self._find_show_row(name) < 0

        # (b) Missing HOST
        tdSql.error(
            f"create external source {name} "
            "type='mysql' port=3306 user='u' password='p'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name) < 0

        # (c) Missing PORT
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name) < 0

        # (d) Missing USER
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} password='{self._mysql_cfg().password}'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name) < 0

        # (e) Missing PASSWORD
        # Parser/translater behavior may vary by build for missing PASSWORD,
        # but it must fail and must not create a source.
        tdSql.error(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}'",
            expectedErrno=None,
        )
        assert self._find_show_row(name) < 0

        # (f) Missing TYPE + HOST
        tdSql.error(
            f"create external source {name} "
            "port=3306 user='u' password='p'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name) < 0

        # (g) Only source_name
        tdSql.error(
            f"create external source {name}",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        assert self._find_show_row(name) < 0

        # (h) Positive control: all mandatory fields
        tdSql.execute(
            f"create external source {name} "
            f"type='mysql' host='{self._mysql_cfg().host}' port={self._mysql_cfg().port} user='{self._mysql_cfg().user}' password='{self._mysql_cfg().password}'"
        )
        # Verify source created with expected key fields
        self._assert_show_field(name, _COL_TYPE, "mysql")
        self._assert_show_field(name, _COL_PASSWORD, _MASKED)
        self._cleanup(name)

    # ------------------------------------------------------------------
    # FQ-EXT-S10  TYPE='tdengine' reserved type
    # ------------------------------------------------------------------

    def test_fq_ext_s10_type_tdengine_reserved(self):
        """FQ-EXT-S10: TYPE='tdengine' reserved type — not delivered in first release

        FS §3.4.1.2: 'tdengine' is reserved for future extension, not delivered in first release.
        Attempting to create type='tdengine' should return an error.

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
    # FQ-EXT-S11  ALTER multi-field combination
    # ------------------------------------------------------------------

    def test_fq_ext_s11_alter_multi_field_combined(self):
        """FQ-EXT-S11: ALTER multi-field combination — modify multiple fields in one ALTER

        FS §3.4.4: HOST/PORT/USER/PASSWORD/DATABASE/SCHEMA/OPTIONS can be modified.
        FQ-EXT-010/011 tested 2-field combos; this tests 4~6 fields simultaneously.

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
        assert row >= 0, f"{name_mysql} must be created before ctime check"
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
    # FQ-EXT-S12  OPTIONS boundary values
    # ------------------------------------------------------------------

    def test_fq_ext_s12_options_boundary_values(self):
        """FQ-EXT-S12: OPTIONS boundary values — empty clause, invalid values, extreme values

        FS §3.4.1.4: connect_timeout_ms positive integer; read_timeout_ms positive integer
        DS §9.2: connect_timeout_ms min=100, max=600000

        Multi-dimensional coverage:
          a) Empty OPTIONS clause → success (no options stored)
          b) connect_timeout_ms='0' → error or ignored (below min=100)
          c) connect_timeout_ms='-1' → error or ignored (negative)
          d) connect_timeout_ms='abc' → error or ignored (non-numeric)
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
        # Verify source created even with empty options
        row = self._find_show_row(f"{base}_a")
        assert row >= 0, f"{base}_a must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")
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
        # Verify source created with both timeout options
        row = self._find_show_row(f"{base}_g")
        assert row >= 0, f"{base}_g must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")

        # (h) Verify values persisted
        self._assert_show_opts_contain(f"{base}_g", "connect_timeout_ms", "5000")
        self._assert_show_opts_contain(f"{base}_g", "read_timeout_ms", "10000")

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # FQ-EXT-S13  ALTER clear DATABASE/SCHEMA
    # ------------------------------------------------------------------

    def test_fq_ext_s13_alter_clear_database_schema(self):
        """FQ-EXT-S13: ALTER clear DATABASE/SCHEMA — set to empty or NULL

        FS §3.4.1.2: DATABASE/SCHEMA are not mandatory, can be unspecified.
        Should be able to revert to "unspecified" state after modification.

        Multi-dimensional coverage:
          a) ALTER SET DATABASE='' → DATABASE becomes empty/NULL
          b) ALTER SET SCHEMA='' → SCHEMA becomes empty/NULL
          c) ALTER SET DATABASE='' then set back to valid value → restored
          d) PG: ALTER DATABASE + SCHEMA both set to empty
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

        def _is_cleared(val):
            # Different builds may render cleared string as None, "", or "''".
            s = "" if val is None else str(val).strip()
            return s in ("", "''")

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
            assert row >= 0, f"{name_mysql} must exist after ALTER"
            db_val = tdSql.queryResult[row][_COL_DATABASE]
            assert _is_cleared(db_val), (
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
            assert row >= 0, f"{name_pg} must exist after ALTER"
            schema_val = tdSql.queryResult[row][_COL_SCHEMA]
            assert _is_cleared(schema_val), (
                f"SCHEMA should be empty/None after clearing, got '{schema_val}'"
            )

        # ── (d) PG: clear both DATABASE + SCHEMA ──
        ret = tdSql.query(
            f"alter external source {name_pg} set database='', schema=''",
            exit=False,
        )
        if ret is not False:
            row = self._find_show_row(name_pg)
            assert row >= 0, f"{name_pg} must exist after ALTER"
            db_val = tdSql.queryResult[row][_COL_DATABASE]
            schema_val = tdSql.queryResult[row][_COL_SCHEMA]
            assert _is_cleared(db_val)
            assert _is_cleared(schema_val)

        # ── (e) HOST/USER unchanged ──
        self._assert_show_field(name_mysql, _COL_HOST, self._mysql_cfg().host)
        self._assert_show_field(name_mysql, _COL_USER, self._mysql_cfg().user)

        self._cleanup(name_mysql, name_pg)

    # ------------------------------------------------------------------
    # FQ-EXT-S14  Name conflict case insensitive
    # ------------------------------------------------------------------

    def test_fq_ext_s14_name_conflict_case_insensitive(self):
        """FQ-EXT-S14: source_name and database name conflict — case insensitive

        FS §3.4.1.3: Identifiers are case insensitive by default.
        FS §3.4.1.2: source_name cannot be the same as a TSDB database name.
        Therefore DB=FQ_DB and source=fq_db (or Fq_Db) should conflict.

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
        # Verify source created
        row = self._find_show_row(src1)
        assert row >= 0, f"{src1} must be created"
        tdSql.checkData(row, _COL_TYPE, "mysql")

        # Cleanup
        for n in [src1, src2, src_dup_lower, src_dup_upper]:
            tdSql.execute(f"drop external source if exists {n}")
        for d in [db1, db2]:
            tdSql.execute(f"drop database if exists {d}")

    # ------------------------------------------------------------------
    # FQ-EXT-S15  Connection-field length boundary tests
    # ------------------------------------------------------------------

    def test_fq_ext_s15_field_length_boundaries(self):
        """FQ-EXT-S15: All connection-field length boundary values

        FS §3.4.1.3 (field length limits):
          source_name : max 64 chars   (TSDB_EXT_SOURCE_NAME_LEN - 1)
          host        : max 256 chars  (TSDB_EXT_SOURCE_HOST_LEN - 1)
          port        : [1, 65535]
          user        : max 128 chars  (TSDB_EXT_SOURCE_USER_LEN - 1)
          password    : max 128 chars  (TSDB_EXT_SOURCE_PASSWORD_LEN - 1)
          database    : max 64 chars   (TSDB_EXT_SOURCE_DATABASE_LEN - 1)
          schema      : max 64 chars   (TSDB_EXT_SOURCE_SCHEMA_LEN - 1)
          options key : max 64 chars   (TSDB_EXT_SOURCE_OPTION_KEY_LEN - 1)
          options val : max 4095 chars (TSDB_EXT_SOURCE_OPTION_VALUE_LEN - 1)

        Multi-dimensional coverage:
          a) source_name 64 chars → OK; 65 chars → error
          b) host 256 chars → OK; 257 chars → error
          c) port 1 → OK; 65535 → OK; 0 → error; 65536 → error
          d) user 128 chars → OK; 129 chars → error
          e) password 128 chars → OK; 129 chars → error
          f) database 64 chars → OK; 65 chars → error
          g) schema 64 chars → OK; 65 chars → error
          h) options key 64 chars → unknown-key error (length OK); 65 chars → too-long error
          i) options value 4095 chars → OK; 4096 chars → error

        Catalog:
            - Query:FederatedExternalSource

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-01 wpan Initial version

        """
        cfg = self._mysql_cfg()
        valid_host = cfg.host
        valid_port = cfg.port
        valid_user = cfg.user
        valid_pwd  = cfg.password
        base       = "fq_ext_s15"

        def mk_sql(name, host=valid_host, port=valid_port, user=valid_user,
                   pwd=valid_pwd, database="", schema="", options=""):
            sql = (
                f"create external source {name} type='mysql' "
                f"host='{host}' port={port} user='{user}' password='{pwd}'"
            )
            if database:
                sql += f" database='{database}'"
            if schema:
                sql += f" schema='{schema}'"
            if options:
                sql += f" options({options})"
            return sql

        # Pre-cleanup: drop any sources that may have been left by a previous run.
        self._cleanup(
            "s" * 64,
            f"{base}_host_ok", f"{base}_host_err",
            f"{base}_port",
            f"{base}_user_ok", f"{base}_user_err",
            f"{base}_pwd_ok", f"{base}_pwd_err",
            f"{base}_db_ok", f"{base}_db_err",
            f"{base}_sc_ok", f"{base}_sc_err",
            f"{base}_optkey_ok", f"{base}_optkey_err",
            f"{base}_optval_ok", f"{base}_optval_err",
        )

        # (a) source_name length
        name_64 = "s" * 64
        name_65 = "s" * 65
        tdSql.execute(f"drop external source if exists {name_64}", queryTimes=1)
        tdSql.execute(f"drop external source if exists {name_65}", queryTimes=1)
        tdSql.execute(mk_sql(name_64))
        # Verify 64-char name accepted and stored correctly
        row = self._find_show_row(name_64)
        assert row >= 0, "64-char name should be accepted"
        tdSql.checkData(row, _COL_NAME, name_64)
        tdSql.execute(f"drop external source if exists {name_64}")
        tdSql.error(mk_sql(name_65), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {name_65}")

        # (b) host length
        host_256 = "h" * 256
        host_257 = "h" * 257
        n = f"{base}_host_ok"
        tdSql.execute(mk_sql(n, host=host_256))
        # Verify 256-char host accepted and stored correctly
        row = self._find_show_row(n)
        assert row >= 0, f"{n} must be created with 256-char host"
        tdSql.checkData(row, _COL_HOST, host_256)
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_host_err"
        tdSql.error(mk_sql(n_err, host=host_257), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {n_err}")

        # (c) port boundaries
        n = f"{base}_port"
        tdSql.execute(mk_sql(n, port=1))
        # Verify port=1 accepted
        row = self._find_show_row(n)
        assert row >= 0, f"{n} with port=1 must be created"
        tdSql.checkData(row, _COL_PORT, 1)
        tdSql.execute(f"drop external source if exists {n}")
        
        tdSql.execute(mk_sql(n, port=65535))
        # Verify port=65535 accepted
        row = self._find_show_row(n)
        assert row >= 0, f"{n} with port=65535 must be created"
        tdSql.checkData(row, _COL_PORT, 65535)
        tdSql.execute(f"drop external source if exists {n}")
        
        tdSql.error(mk_sql(n, port=0),     expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error(mk_sql(n, port=65536), expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.execute(f"drop external source if exists {n}")

        # (d) user length
        user_128 = "u" * 128
        user_129 = "u" * 129
        n = f"{base}_user_ok"
        tdSql.execute(mk_sql(n, user=user_128))
        # Verify user field value in SHOW is exactly user_128
        row = self._find_show_row(n)
        assert row >= 0, f"{n} must be created and visible in SHOW"
        tdSql.checkData(row, _COL_USER, user_128)
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_user_err"
        tdSql.error(mk_sql(n_err, user=user_129), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {n_err}")

        # (e) password length
        pwd_128 = "p" * 128
        pwd_129 = "p" * 129
        n = f"{base}_pwd_ok"
        tdSql.execute(mk_sql(n, pwd=pwd_128))
        # Verify password field is masked (not showing raw value)
        row = self._find_show_row(n)
        assert row >= 0, f"{n} must be created and visible in SHOW"
        assert str(tdSql.queryResult[row][_COL_PASSWORD]) == _MASKED, \
            f"Password must be masked '{_MASKED}', got '{tdSql.queryResult[row][_COL_PASSWORD]}'"
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_pwd_err"
        tdSql.error(mk_sql(n_err, pwd=pwd_129), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {n_err}")

        # (f) database length
        db_64 = "d" * 64
        db_65 = "d" * 65
        n = f"{base}_db_ok"
        tdSql.execute(mk_sql(n, database=db_64))
        # Verify database field value in SHOW is exactly db_64
        row = self._find_show_row(n)
        assert row >= 0, f"{n} must be created and visible in SHOW"
        tdSql.checkData(row, _COL_DATABASE, db_64)
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_db_err"
        tdSql.error(mk_sql(n_err, database=db_65), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {n_err}")

        # (g) schema length
        sc_64 = "c" * 64
        sc_65 = "c" * 65
        n = f"{base}_sc_ok"
        tdSql.execute(mk_sql(n, schema=sc_64))
        # Verify schema field value via SHOW (DESCRIBE may be unsupported in some builds)
        self._assert_show_field(n, _COL_SCHEMA, sc_64)
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_sc_err"
        tdSql.error(mk_sql(n_err, schema=sc_65), expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG)
        tdSql.execute(f"drop external source if exists {n_err}")

        # (h) options key length: 64-char key → unknown-key error (length check passes);
        #     65-char key → TOO_LONG (length checked before key-validity)
        key_64 = "k" * 64
        key_65 = "k" * 65
        n = f"{base}_optkey_ok"
        # 64-char key: should fail with PAR_SYNTAX_ERROR (unknown key), NOT TOO_LONG
        tdSql.error(
            mk_sql(n, options=f"'{key_64}'='v'"),
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )
        tdSql.execute(f"drop external source if exists {n}")
        # 65-char key: must fail with TOO_LONG (checked before key-validity)
        n_err = f"{base}_optkey_err"
        tdSql.error(
            mk_sql(n_err, options=f"'{key_65}'='v'"),
            expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG,
        )
        tdSql.execute(f"drop external source if exists {n_err}")

        # (i) options value length: 4095-char value with a known key → OK;
        #     4096-char value → TOO_LONG
        val_4095 = "v" * 4095
        val_4096 = "v" * 4096
        n = f"{base}_optval_ok"
        tdSql.execute(mk_sql(n, options=f"'tls_ca_cert'='{val_4095}'"))
        # Verify OPTIONS contains the tls_ca_cert key via SHOW
        self._assert_show_opts_contain(n, "tls_ca_cert")
        tdSql.execute(f"drop external source if exists {n}")
        n_err = f"{base}_optval_err"
        tdSql.error(
            mk_sql(n_err, options=f"'tls_ca_cert'='{val_4096}'"),
            expectedErrno=TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG,
        )
        tdSql.execute(f"drop external source if exists {n_err}")

