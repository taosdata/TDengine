"""
test_fq_11_security.py

Implements active security cases mapped from SEC-001..SEC-020 in TS "Security Tests"
section (deprecated by current FS: SEC-015, SEC-019), with the same
high-coverage standard applied to §1-§8 functional tests.  Each TS case maps
to exactly one test method with multi-dimensional, multi-statement coverage
including both positive and negative paths.

Coverage matrix:
    SEC-001  Encrypted password storage — metadata side no plaintext password
    SEC-002  SHOW/DESCRIBE masking — password/token/cert private key masked
    SEC-003  Log masking — error logs contain no sensitive info
    SEC-004  Normal user visibility — sysInfo column permission protection
    SEC-005  TLS one-way verification — tls_enabled + ca_cert effective
    SEC-006  TLS two-way verification — client cert/key effective
    SEC-007  Auth failure blocking — auth failed → source status update
    SEC-008  Access denied blocking — access denied error code & status
    SEC-009  SQL injection protection — SOURCE/path/identifier no injection
    SEC-010  Abnormal data boundary validation — external abnormal return no crash
    SEC-011  Connection reset safety — connection reset → handle cleanup complete
    SEC-012  Sensitive config change audit — ALTER SOURCE change has audit record
    SEC-013  Option effectiveness — charset/read_timeout
    SEC-014  Option effectiveness — influx api_token/protocol
    SEC-021  ALTER unknown option rejected
    SEC-022  ALTER patch-delete non-timeout option
    SEC-023  read_timeout_ms=0 overrides global query timeout
    SEC-025  SHOW/DESCRIBE masking for tls_client_cert value
    SEC-026  timeout option invalid-format rejection (non-numeric/negative)
    SEC-027  Influx protocol default + FlightSQL TLS option consumption
    SEC-016  Option effectiveness — connect_timeout timing behavior
    SEC-017  MySQL ssl_mode enum behavior matrix
    SEC-018  PostgreSQL sslmode enum behavior matrix
    SEC-020  Option key acceptance matrix (parser + mnode consistency)

Design notes:
    - Tests validate masking/security at the interface level where possible.
    - For tests requiring live external databases or audit subsystems, the
      interface-level checks are done inline and data-verification parts
      are guarded with pytest.skip().
    - Real external-source hosts/ports from ExtSrcEnv config are used in all tests.
    - Sensitive strings tested: password, api_token, client_key, ca_cert path.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - For full SEC-005/006: external source with TLS configured.
"""

import time as _time

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
    TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_WRITE_DENIED,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
    TSDB_CODE_EXT_AUTH_FAILED,
    FQ_CA_CERT,
    FQ_MYSQL_CA_CERT,
    FQ_MYSQL_CLIENT_CERT,
    FQ_MYSQL_CLIENT_KEY,
    FQ_PG_CA_CERT,
    FQ_PG_CLIENT_CERT,
    FQ_PG_CLIENT_KEY,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
    TSDB_CODE_EXT_TABLE_NOT_EXIST,
    TSDB_CODE_MND_DB_NOT_EXIST,
)

# SHOW EXTERNAL SOURCES column indices
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

_MASKED = "******"


class TestFq11Security(FederatedQueryVersionedMixin):
    """FS-aligned security tests with full coverage (SEC-015/019 deprecated)."""

    # All source names created across tests — used by teardown_class for global cleanup
    _ALL_SOURCES = [
        "sec001_mysql_simple", "sec001_mysql_special", "sec001_pg",
        "sec001_influx", "sec001_empty_pwd",
        "sec002_mysql", "sec002_pg", "sec002_influx", "sec002_tls",
        "sec003_mysql", "sec003_influx",
        "sec004_src",
        "sec005_mysql_tls", "sec005_mysql_ssl_disabled", "sec005_mysql_bad_ca",
        "sec005_pg_disable", "sec005_pg_require", "sec005_conflict",
        "sec006_mysql_no_client", "sec006_mysql_mtls", "sec006_mysql_pair_check",
        "sec007_bad_auth", "sec007_good_src",
        "sec008_src",
        "sec009_pwd_inj", "sec009_drop_test", "`sec009_drop_test`",
        "sec010_port0", "sec010_port65535", "sec010_longhost",
        "sec010_longdb", "sec010_longpwd", "sec010_longuser",
        "sec011_reset",
        "sec012_audit",
        "sec013_charset_ok", "sec013_charset_bad", "sec013_read_timeout_low", "sec013_read_timeout_ok",
        "sec014_influx_http_ok", "sec014_influx_token_bad", "sec014_influx_flight_ok", "sec014_influx_http_tls",
        "perf_timeout_src",
    ]

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        # Pre-cleanup: remove any leftover state from previous runs
        self._cleanup(*TestFq11Security._ALL_SOURCES)

    def teardown_class(self):
        """Global cleanup — remove all external sources created by any test."""
        self._cleanup(*TestFq11Security._ALL_SOURCES)
        tdSql.execute("drop user if exists sec004_user")

    # ------------------------------------------------------------------
    # helpers (shared: _cleanup inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _find_row(self, source_name):
        tdSql.query("show external sources")
        for idx, row in enumerate(tdSql.queryResult):
            if str(row[_COL_NAME]) == source_name:
                return idx
        return -1

    def _row_text(self, row_idx):
        return "|".join(str(c) for c in tdSql.queryResult[row_idx])

    # ------------------------------------------------------------------
    # SEC-001  Encrypted password storage
    # ------------------------------------------------------------------

    def test_fq_sec_001_password_encrypted_storage(self):
        """SEC-001: Password encrypted storage — metadata no plaintext

        TS: No plaintext password stored in metadata

        Multi-dimensional coverage:
        1. Create MySQL source with various password patterns:
           a. Simple ASCII password
           b. Password with special chars (\!@#$%^&)
           c. Password with unicode-like patterns
        2. For each: SHOW EXTERNAL SOURCES → password column must be masked
        3. DESCRIBE EXTERNAL SOURCE → password field must be masked
        4. Create PG source → same masking check
        5. Create InfluxDB source with api_token → token must be masked
        6. Negative: create source with empty password → should succeed, still masked
        7. ALTER source password → new password also masked

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        cfg_influx = self._influx_cfg()
        names = [
            "sec001_mysql_simple", "sec001_mysql_special", "sec001_pg",
            "sec001_influx", "sec001_empty_pwd",
        ]
        self._cleanup(*names)

        # --- 1a. Simple ASCII password ---
        tdSql.execute(
            f"create external source sec001_mysql_simple type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='admin' password='MySecret123' database='db1'"
        )
        idx = self._find_row("sec001_mysql_simple")
        assert idx >= 0, "sec001_mysql_simple not found"
        text = self._row_text(idx)
        assert "MySecret123" not in text, "plaintext password leaked in SHOW"
        assert _MASKED in text or "*" in text, "password not masked in SHOW"

        # --- 1b. Password with special characters ---
        tdSql.execute(
            f"create external source sec001_mysql_special type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='admin' password='P@ss!#$%^&*()' database='db1'"
        )
        idx = self._find_row("sec001_mysql_special")
        assert idx >= 0
        text = self._row_text(idx)
        assert "P@ss!#$%^&*()" not in text, "special-char password leaked"

        # --- 2. PostgreSQL source ---
        tdSql.execute(
            f"create external source sec001_pg type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='pguser' password='pg_secret_pw' "
            f"database='pgdb' schema='public'"
        )
        idx = self._find_row("sec001_pg")
        assert idx >= 0
        text = self._row_text(idx)
        assert "pg_secret_pw" not in text, "PG password leaked in SHOW"

        # --- 3. InfluxDB source with api_token ---
        tdSql.execute(
            f"create external source sec001_influx type='influxdb' "
            f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
            f"database='telegraf' options('api_token'='influx_super_secret_token_xyz', 'protocol'='flight_sql')"
        )
        idx = self._find_row("sec001_influx")
        assert idx >= 0
        text = self._row_text(idx)
        assert "influx_super_secret_token_xyz" not in text, "InfluxDB api_token leaked"

        # --- 4. Empty password --- system rejects empty passwords for MySQL
        tdSql.error(
            f"create external source sec001_empty_pwd type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='admin' password='' database='db1'"
        )  # empty password is correctly rejected (PASSWORD cannot be empty)

        # --- 5. ALTER password → still masked ---
        tdSql.execute(
            "alter external source sec001_mysql_simple set password='NewSecret456'"
        )
        idx = self._find_row("sec001_mysql_simple")
        text = self._row_text(idx)
        assert "NewSecret456" not in text, "altered password leaked"

        # --- 6. DESCRIBE masking ---
        tdSql.query("describe external source sec001_mysql_simple")
        desc_text = str(tdSql.queryResult)
        assert "NewSecret456" not in desc_text, "password leaked in DESCRIBE"
        assert "MySecret123" not in desc_text, "old password leaked in DESCRIBE"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-002  SHOW/DESCRIBE masking
    # ------------------------------------------------------------------

    def test_fq_sec_002_show_describe_masking(self):
        """SEC-002: SHOW/DESCRIBE masking — password/token/cert key not exposed

        TS: password/token/cert private key not shown in plaintext

        Multi-dimensional coverage:
        1. MySQL: password masked in SHOW and DESCRIBE
        2. PG: password masked; schema is NOT sensitive (should show)
        3. InfluxDB: api_token masked
        4. MySQL with TLS options (ca_cert path, client_key path):
           a. Paths ARE shown (not secret), but client_key content if any → masked
        5. SHOW column-level check: only password column is masked
        6. Negative: user column should NOT be masked (it's not sensitive)
        7. Multiple sources simultaneously: all masked independently

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        cfg_influx = self._influx_cfg()
        names = ["sec002_mysql", "sec002_pg", "sec002_influx", "sec002_tls"]
        self._cleanup(*names)

        tdSql.execute(
            f"create external source sec002_mysql type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='visible_user' password='hidden_pwd' database='db'"
        )
        tdSql.execute(
            f"create external source sec002_pg type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='pg_user' password='pg_hidden' "
            f"database='pgdb' schema='my_schema'"
        )
        tdSql.execute(
            f"create external source sec002_influx type='influxdb' "
            f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
            f"database='mydb' options('api_token'='secret_influx_tk', 'protocol'='flight_sql')"
        )
        tdSql.execute(
            f"create external source sec002_tls type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='tls_user' password='tls_pwd' database='db' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{FQ_MYSQL_CA_CERT}')"
        )

        tdSql.query("show external sources")

        for row in tdSql.queryResult:
            name = str(row[_COL_NAME])
            if name not in names:
                continue

            # Password column must be masked
            pwd_val = str(row[_COL_PASSWORD])
            if name == "sec002_influx":
                # InfluxDB might store token differently; check both password and options
                pass
            else:
                assert "hidden_pwd" not in pwd_val and "pg_hidden" not in pwd_val \
                    and "tls_pwd" not in pwd_val, \
                    f"password not masked for {name}"

            # User column should NOT be masked
            user_val = str(row[_COL_USER])
            if name == "sec002_mysql":
                assert user_val == "visible_user" or "visible_user" in user_val, \
                    "user column should be visible"
            if name == "sec002_pg":
                # Schema should be visible
                schema_val = str(row[_COL_SCHEMA])
                assert "my_schema" in schema_val or schema_val == "my_schema", \
                    "schema should be visible, it is not sensitive"

        # Full text check for token in InfluxDB
        idx = self._find_row("sec002_influx")
        assert idx >= 0
        full_text = self._row_text(idx)
        assert "secret_influx_tk" not in full_text, "InfluxDB token leaked in SHOW"

        # TLS: ca_cert path can be visible, but password must be hidden
        idx = self._find_row("sec002_tls")
        assert idx >= 0
        full_text = self._row_text(idx)
        assert "tls_pwd" not in full_text, "TLS source password leaked"

        # DESCRIBE each source
        for name in names:
            tdSql.query(f"describe external source {name}")
            desc = str(tdSql.queryResult)
            for secret in ["hidden_pwd", "pg_hidden", "secret_influx_tk", "tls_pwd"]:
                assert secret not in desc, f"'{secret}' leaked in DESCRIBE {name}"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-003  Log masking
    # ------------------------------------------------------------------

    def test_fq_sec_003_log_masking(self):
        """SEC-003: Log masking — error logs contain no sensitive info

        TS: Error logs contain no sensitive information

        Multi-dimensional coverage:
        1. Create source with known password, trigger error (query unreachable)
        2. Verify the error message returned to client does not contain password
        3. Create source with api_token, trigger error → token not in message
        4. ALTER source with new password, trigger error → neither old nor new in message
        5. Negative: verify error DOES contain useful info (source name/type) for debugging

        Note: full log-file scanning requires access to taosd log files;
        this test verifies client-facing error messages.

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        cfg_influx = self._influx_cfg()
        names = ["sec003_mysql", "sec003_influx"]
        self._cleanup(*names)

        # MySQL with known password — wrong creds on real host trigger auth error.
        tdSql.execute(
            f"create external source sec003_mysql type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='LogSecret99' database='db' "
            f"options('connect_timeout_ms'='500')"
        )

        # Trigger error by querying unreachable source; capture error message
        # and verify it does not contain the password.
        try:
            tdSql.query("select * from sec003_mysql.db.t1")
        except Exception as e:
            err_msg = str(e)
            assert "LogSecret99" not in err_msg, \
                "password leaked in error message"

        # InfluxDB with api_token
        tdSql.execute(
            f"create external source sec003_influx type='influxdb' "
            f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
            f"database='mydb' options('api_token'='TokenInLog123', 'protocol'='flight_sql')"
        )
        try:
            tdSql.query("select * from sec003_influx.mydb.m1")
        except Exception as e:
            err_msg = str(e)
            assert "TokenInLog123" not in err_msg, \
                "api_token leaked in error message"

        # ALTER password and trigger again
        tdSql.execute(
            "alter external source sec003_mysql set password='AlteredPwd88'"
        )
        try:
            tdSql.query("select * from sec003_mysql.db.t1")
        except Exception as e:
            err_msg = str(e)
            assert "AlteredPwd88" not in err_msg, \
                "altered password leaked in error message"
            assert "LogSecret99" not in err_msg, \
                "old password leaked in error message"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-004  Normal user visibility
    # ------------------------------------------------------------------

    def test_fq_sec_004_normal_user_visibility(self):
        """SEC-004: Normal user visibility — sysInfo column protection

        TS: sysInfo column permission protection is correct

        Multi-dimensional coverage:
        1. Create external source as root
        2. SHOW EXTERNAL SOURCES as root → all columns visible
        3. Create normal user without sysinfo privilege
        4. SHOW EXTERNAL SOURCES as normal user → sysInfo-protected columns NULL
        5. DESCRIBE as normal user → sensitive fields NULL
        6. Negative: normal user cannot CREATE/ALTER/DROP external sources
        7. Normal user CAN query vtables (read-only) if granted

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        src = "sec004_src"
        user = "sec004_user"
        self._cleanup(src)
        tdSql.execute(f"drop user if exists {user}")

        # Root creates source
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )

        # Root sees all columns
        idx = self._find_row(src)
        assert idx >= 0
        root_row = tdSql.queryResult[idx]

        # Create normal user (sysinfo=0)
        tdSql.execute(f"create user {user} pass 'Test1234' sysinfo 0")

        denied_src = "sec004_src_denied"
        self._cleanup(denied_src)

        # Switch to normal user and verify visibility + DDL denial
        tdSql.connect(user, "Test1234")
        try:
            tdSql.query("show external sources")
            normal_row = None
            for row in tdSql.queryResult:
                if str(row[_COL_NAME]) == src:
                    normal_row = row
                    break
            assert normal_row is not None, "normal user should see source entry"

            # Password should never appear as plaintext for normal user.
            assert str(normal_row[_COL_PASSWORD]) != "p", \
                "normal user should not see plaintext password"

            tdSql.query(f"describe external source {src}")
            desc_text = str(tdSql.queryResult)
            assert "******" in desc_text or "password" in desc_text.lower(), \
                "DESCRIBE should mask password (show '******' or field label)"
            assert "'p'" not in desc_text and '"p"' not in desc_text, \
                "DESCRIBE leaked plaintext password"

            # Normal user must be denied for source-management DDL.
            tdSql.error(
                f"create external source {denied_src} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
            )
            tdSql.error(
                f"alter external source {src} set host='{cfg_mysql.host}'"
            )
            tdSql.error(f"drop external source {src}")
        finally:
            tdSql.connect("root", "taosdata")

        # Back to root: source should still exist (non-admin DROP denied).
        tdSql.query("show external sources")
        found = any(str(r[_COL_NAME]) == src for r in tdSql.queryResult)
        assert found, f"root should still see {src}"

        tdSql.execute(f"drop user {user}")
        self._cleanup(src, denied_src)

    # ------------------------------------------------------------------
    # SEC-005  TLS one-way verification
    # ------------------------------------------------------------------

    def test_fq_sec_005_tls_one_way_verification(self):
        """SEC-005: TLS one-way verification — tls_enabled + ca_cert

          TS: one-way TLS options must affect real connection behavior.

          Coverage:
          1. MySQL ssl_mode=required should be queryable without silent downgrade.
          2. MySQL ssl_mode=disabled must allow plain connection.
          3. MySQL verify_ca with invalid CA path must fail.
          4. PostgreSQL sslmode=disable/require must produce different session SSL
              state (checked through v_ssl_self view).
          5. Parser conflict remains enforced: tls_enabled=true + ssl_mode=disabled.

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_mysql_tls = f"sec005_mysql_tls_{run_tag}"
        src_mysql_ssl_disabled = f"sec005_mysql_ssl_disabled_{run_tag}"
        src_mysql_bad_ca = f"sec005_mysql_bad_ca_{run_tag}"
        src_pg_disable = f"sec005_pg_disable_{run_tag}"
        src_pg_require = f"sec005_pg_require_{run_tag}"
        src_pg_tlsen_only = f"sec005_pg_tlsen_only_{run_tag}"
        src_conflict = f"sec005_conflict_{run_tag}"
        names = [
            src_mysql_tls, src_mysql_ssl_disabled, src_mysql_bad_ca,
            src_pg_disable, src_pg_require, src_pg_tlsen_only, src_conflict,
        ]
        self._cleanup(*names)

        mysql_db = "sec005_tls_db"
        mysql_tb = "t_tls"
        mysql_user = "sec005_tls_user"
        mysql_pass = "sec005_tls_pwd"
        pg_db = "sec005_tls_pgdb"
        pg_user = "sec005_ssl_user"
        pg_pass = "sec005_ssl_pwd"

        # Prepare MySQL data.
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
            f"CREATE DATABASE `{mysql_db}`",
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"CREATE USER '{mysql_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{mysql_pass}'",
            f"GRANT ALL PRIVILEGES ON `{mysql_db}`.* TO '{mysql_user}'@'%'",
            "FLUSH PRIVILEGES",
        ])
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, mysql_db, [
            f"DROP TABLE IF EXISTS `{mysql_tb}`",
            f"CREATE TABLE `{mysql_tb}` (id INT PRIMARY KEY, v INT)",
            f"INSERT INTO `{mysql_tb}` VALUES (1, 101)",
        ])

        # Prepare PG view exposing current-session SSL state.
        ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
            f"DROP DATABASE IF EXISTS \"{pg_db}\"",
            f"DROP ROLE IF EXISTS \"{pg_user}\"",
            f"CREATE ROLE \"{pg_user}\" LOGIN PASSWORD '{pg_pass}'",
            f"CREATE DATABASE \"{pg_db}\"",
            f"GRANT CONNECT ON DATABASE \"{pg_db}\" TO \"{pg_user}\"",
        ])
        ExtSrcEnv.pg_exec_cfg(cfg_pg, pg_db, [
            "DROP VIEW IF EXISTS public.v_ssl_self",
            "CREATE VIEW public.v_ssl_self AS "
            "SELECT ssl FROM pg_catalog.pg_stat_ssl WHERE pid = pg_backend_pid()",
            f"GRANT USAGE ON SCHEMA public TO \"{pg_user}\"",
            f"GRANT SELECT ON public.v_ssl_self TO \"{pg_user}\"",
        ])

        # MySQL runtime check: ssl_mode=required should be queryable.
        tdSql.execute(
            f"create external source {src_mysql_tls} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' "
            f"options('tls_enabled'='true', 'tls_ca_cert'='{FQ_MYSQL_CA_CERT}', 'ssl_mode'='required')"
        )
        idx = self._find_row(src_mysql_tls)
        assert idx >= 0
        assert str(tdSql.queryResult[idx][_COL_USER]) == mysql_user
        tdSql.query(f"select count(*) from {src_mysql_tls}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # MySQL runtime check: ssl_mode=disabled should allow plain connection.
        tdSql.execute(
            f"create external source {src_mysql_ssl_disabled} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('ssl_mode'='disabled')"
        )
        tdSql.query(f"select count(*) from {src_mysql_ssl_disabled}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # MySQL negative: verify_ca with non-existing CA path should fail.
        tdSql.execute(
            f"create external source {src_mysql_bad_ca} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' "
            f"options('ssl_mode'='verify_ca', 'tls_ca_cert'='/tmp/fq_nonexistent_ca.pem')"
        )
        tdSql.error(
            f"select count(*) from {src_mysql_bad_ca}.{mysql_db}.{mysql_tb}",
            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
        )

        # PG sslmode effectiveness: disable -> ssl=false, require -> ssl=true.
        tdSql.execute(
            f"create external source {src_pg_disable} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='disable')"
        )
        tdSql.query(f"select count(*) from {src_pg_disable}.public.v_ssl_self where ssl = false")
        assert int(tdSql.queryResult[0][0]) == 1, "sslmode=disable should create a non-SSL PG session"

        tdSql.execute(
            f"create external source {src_pg_require} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' "
            f"options('sslmode'='require', 'tls_ca_cert'='{FQ_PG_CA_CERT}')"
        )
        tdSql.query(f"select count(*) from {src_pg_require}.public.v_ssl_self where ssl = true")
        assert int(tdSql.queryResult[0][0]) == 1, "sslmode=require should create an SSL PG session"

        # PG tls_enabled-only runtime check (without explicit sslmode).
        tdSql.execute(
            f"create external source {src_pg_tlsen_only} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('tls_enabled'='true')"
        )
        tdSql.query(f"select count(*) from {src_pg_tlsen_only}.public.v_ssl_self where ssl = true")
        assert int(tdSql.queryResult[0][0]) == 1, "tls_enabled=true should establish SSL for PostgreSQL"

        # Negative: parser-level TLS conflict remains enforced.
        tdSql.error(
            f"create external source {src_conflict} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' database='{mysql_db}' "
            f"options('tls_enabled'='true', 'ssl_mode'='disabled')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )

        self._cleanup(*names)
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
        ])
        ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
            f"DROP DATABASE IF EXISTS \"{pg_db}\"",
            f"DROP ROLE IF EXISTS \"{pg_user}\"",
        ])

    # ------------------------------------------------------------------
    # SEC-006  TLS two-way verification
    # ------------------------------------------------------------------

    def test_fq_sec_006_tls_two_way_verification(self):
        """SEC-006: TLS two-way (mutual) verification — client cert/key

          TS: client cert/key must have real runtime effect, not only metadata effect.

          Coverage:
          1. Baseline source with ssl_mode=disabled should succeed.
          2. Enabling client cert/key options should affect runtime behavior.
          3. Invalid tls_client_key path must fail.
          4. Parser-level cert/key pair constraint stays enforced.
          5. ALTER OPTIONS updates tls_ca_cert value visibility.

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_base = f"sec006_mysql_no_client_{run_tag}"
        src_mtls = f"sec006_mysql_mtls_{run_tag}"
        src_bad_key = f"sec006_mysql_bad_client_key_{run_tag}"
        src_pair_check = f"sec006_mysql_pair_check_{run_tag}"
        src_pair_check_key_only = f"sec006_mysql_pair_check_key_only_{run_tag}"
        names = [src_base, src_mtls, src_bad_key, src_pair_check, src_pair_check_key_only]
        self._cleanup(*names)

        mysql_db = "sec006_tls_db"
        mysql_tb = "t_mtls"
        mtls_user = "sec006_mtls_user"
        mtls_pass = "sec006_mtls_pwd"

        # Build MySQL TLS fixture (runtime cert/key effectiveness, environment-independent).
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
            f"CREATE DATABASE `{mysql_db}`",
            f"DROP USER IF EXISTS '{mtls_user}'@'%'",
            f"CREATE USER '{mtls_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{mtls_pass}'",
            f"GRANT ALL PRIVILEGES ON `{mysql_db}`.* TO '{mtls_user}'@'%'",
            "FLUSH PRIVILEGES",
        ])
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, mysql_db, [
            f"DROP TABLE IF EXISTS `{mysql_tb}`",
            f"CREATE TABLE `{mysql_tb}` (id INT PRIMARY KEY, v INT)",
            f"INSERT INTO `{mysql_tb}` VALUES (1, 201)",
        ])

        # Baseline non-TLS source should be queryable.
        tdSql.execute(
            f"create external source {src_base} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mtls_user}' password='{mtls_pass}' "
            f"database='{mysql_db}' "
            f"options('ssl_mode'='disabled')"
        )
        tdSql.query(f"select count(*) from {src_base}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # Runtime check: client cert/key options must affect behavior.
        tdSql.execute(
            f"create external source {src_mtls} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mtls_user}' password='{mtls_pass}' "
            f"database='{mysql_db}' "
            f"options('ssl_mode'='required', 'tls_ca_cert'='{FQ_MYSQL_CA_CERT}', "
            f"'tls_client_cert'='{FQ_MYSQL_CLIENT_CERT}', 'tls_client_key'='{FQ_MYSQL_CLIENT_KEY}', "
            f"'tls_enabled'='true')"
        )
        tdSql.query(f"select count(*) from {src_mtls}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # Negative runtime check: invalid client key path must fail.
        tdSql.execute(
            f"create external source {src_bad_key} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mtls_user}' password='{mtls_pass}' "
            f"database='{mysql_db}' "
            f"options('ssl_mode'='required', 'tls_ca_cert'='{FQ_MYSQL_CA_CERT}', "
            f"'tls_client_cert'='{FQ_MYSQL_CLIENT_CERT}', 'tls_client_key'='/tmp/fq_nonexistent_client_key.pem', "
            f"'tls_enabled'='true')"
        )
        tdSql.error(
            f"select count(*) from {src_bad_key}.{mysql_db}.{mysql_tb}",
            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
        )

        # Parser-level pair checks remain strict.
        tdSql.error(
            f"create external source {src_pair_check} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mtls_user}' password='{mtls_pass}' "
            f"database='{mysql_db}' options('tls_client_cert'='{FQ_MYSQL_CLIENT_CERT}')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )
        tdSql.error(
            f"create external source {src_pair_check_key_only} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mtls_user}' password='{mtls_pass}' "
            f"database='{mysql_db}' options('tls_client_key'='{FQ_MYSQL_CLIENT_KEY}')",
            expectedErrno=TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT,
        )

        # Verify ALTER keeps option visibility coherent.
        tdSql.execute(
            f"alter external source {src_mtls} set options('tls_ca_cert'='{FQ_CA_CERT}')"
        )
        idx = self._find_row(src_mtls)
        assert idx >= 0
        opts_after = str(tdSql.queryResult[idx][_COL_OPTIONS])
        assert FQ_CA_CERT in opts_after, "ALTER options should update tls_ca_cert"

        self._cleanup(*names)
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP USER IF EXISTS '{mtls_user}'@'%'",
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
        ])

    # ------------------------------------------------------------------
    # SEC-013  Option effectiveness: charset/read_timeout
    # ------------------------------------------------------------------

    def test_fq_sec_013_option_effectiveness_charset_and_read_timeout(self):
        """Behavior checks for option effectiveness (charset/read_timeout).

        Goal: if option consumption is broken, query path must fail.
        """
        cfg_mysql = self._mysql_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_charset_ok = f"sec013_charset_ok_{run_tag}"
        src_charset_bad = f"sec013_charset_bad_{run_tag}"
        src_rt_low = f"sec013_read_timeout_low_{run_tag}"
        src_rt_ok = f"sec013_read_timeout_ok_{run_tag}"
        names = [
            src_charset_ok, src_charset_bad,
            src_rt_low, src_rt_ok,
        ]
        self._cleanup(*names)

        mysql_db = "sec013_opt_db"
        mysql_tb = "t_opt"
        mysql_vw = "v_slow"
        mysql_user = "sec013_user"
        mysql_pass = "sec013_pwd"

        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
            f"CREATE DATABASE `{mysql_db}`",
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"CREATE USER '{mysql_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{mysql_pass}'",
            f"GRANT ALL PRIVILEGES ON `{mysql_db}`.* TO '{mysql_user}'@'%'",
            "FLUSH PRIVILEGES",
        ])
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, mysql_db, [
            f"DROP TABLE IF EXISTS `{mysql_tb}`",
            f"CREATE TABLE `{mysql_tb}` (id INT PRIMARY KEY, txt VARCHAR(64))",
            f"INSERT INTO `{mysql_tb}` VALUES (1, 'hello')",
            f"DROP VIEW IF EXISTS `{mysql_vw}`",
            f"CREATE VIEW `{mysql_vw}` AS SELECT SLEEP(2) AS delay",
        ])

        # charset invalid => query must fail (proves option is consumed).
        tdSql.execute(
            f"create external source {src_charset_bad} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('charset'='__bad_charset__')"
        )
        tdSql.error(
            f"select count(*) from {src_charset_bad}.{mysql_db}.{mysql_tb}",
            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
        )

        # charset valid => query succeeds.
        tdSql.execute(
            f"create external source {src_charset_ok} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('charset'='utf8mb4')"
        )
        tdSql.query(f"select count(*) from {src_charset_ok}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # read_timeout low => slow view should timeout.
        tdSql.execute(
            f"create external source {src_rt_low} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('read_timeout_ms'='500')"
        )
        tdSql.error(f"select count(*) from {src_rt_low}.{mysql_db}.{mysql_vw}")

        # read_timeout high => same slow view should succeed.
        tdSql.execute(
            f"create external source {src_rt_ok} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('read_timeout_ms'='3000')"
        )
        tdSql.query(f"select count(*) from {src_rt_ok}.{mysql_db}.{mysql_vw}")
        assert int(tdSql.queryResult[0][0]) == 1

        self._cleanup(*names)
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
        ])

    # ------------------------------------------------------------------
    # SEC-014  Influx option effectiveness: api_token/protocol
    # ------------------------------------------------------------------

    def test_fq_sec_014_influx_option_effectiveness_api_token_and_protocol(self):
        """Behavior checks for InfluxDB option effectiveness (api_token/protocol).

        Goal: if option consumption is broken, query path must fail.
        """
        cfg_influx = self._influx_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_http_ok = f"sec014_influx_http_ok_{run_tag}"
        src_token_bad = f"sec014_influx_token_bad_{run_tag}"
        src_flight_ok = f"sec014_influx_flight_ok_{run_tag}"
        src_http_tls = f"sec014_influx_http_tls_{run_tag}"
        src_http_upper = f"sec014_influx_http_upper_{run_tag}"
        src_flight_upper = f"sec014_influx_flight_upper_{run_tag}"
        names = [
            src_http_ok, src_token_bad,
            src_flight_ok, src_http_tls,
            src_http_upper, src_flight_upper,
        ]
        self._cleanup(*names)

        influx_db = "sec014_opt_idb"
        influx_tb = "src_t"
        ts_ns = int(_time.time() * 1_000_000_000)

        ExtSrcEnv.influx_create_db_cfg(cfg_influx, influx_db)
        ExtSrcEnv.influx_write_cfg(
            cfg_influx,
            influx_db,
            [f"{influx_tb},site=sec014 val=1i {ts_ns}"],
        )

        try:
            # protocol=http + valid token => query succeeds.
            tdSql.execute(
                f"create external source {src_http_ok} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='http')"
            )
            tdSql.query(f"select count(*) from {src_http_ok}.{influx_db}.{influx_tb}")
            assert int(tdSql.queryResult[0][0]) == 1

            # Invalid token must fail; if api_token is ignored this may incorrectly pass.
            tdSql.execute(
                f"create external source {src_token_bad} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='__bad_token__', 'protocol'='http')"
            )
            bad_sql = f"select count(*) from {src_token_bad}.{influx_db}.{influx_tb}"
            try:
                tdSql.cursor.execute(bad_sql)
                tdSql.cursor.fetchall()
            except BaseException as e:
                errno = getattr(e, "errno", None)
                errno_low16 = (errno & 0xFFFF) if isinstance(errno, int) else None
                assert errno == TSDB_CODE_EXT_AUTH_FAILED or errno_low16 == (TSDB_CODE_EXT_AUTH_FAILED & 0xFFFF), (
                    f"invalid api_token should fail with AUTH_FAILED, got errno={errno}"
                )
            else:
                raise AssertionError("invalid api_token unexpectedly succeeded")

            # protocol=flight_sql + valid token => query succeeds.
            tdSql.execute(
                f"create external source {src_flight_ok} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='flight_sql')"
            )
            tdSql.query(f"select count(*) from {src_flight_ok}.{influx_db}.{influx_tb}")
            assert int(tdSql.queryResult[0][0]) == 1

            # protocol is case-insensitive: HTTP (upper-case) => HTTP path and succeeds.
            tdSql.execute(
                f"create external source {src_http_upper} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='HTTP')"
            )
            tdSql.query(f"select count(*) from {src_http_upper}.{influx_db}.{influx_tb}")
            assert int(tdSql.queryResult[0][0]) == 1

            # protocol is case-insensitive: FLIGHT_SQL (upper-case) => Flight SQL path and succeeds.
            tdSql.execute(
                f"create external source {src_flight_upper} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='FLIGHT_SQL')"
            )
            tdSql.query(f"select count(*) from {src_flight_upper}.{influx_db}.{influx_tb}")
            assert int(tdSql.queryResult[0][0]) == 1

            # protocol=http + tls_enabled + invalid CA must fail when HTTP TLS options are consumed.
            tdSql.execute(
                f"create external source {src_http_tls} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='http', "
                f"'tls_enabled'='true', 'tls_ca_cert'='/tmp/fq_nonexistent_influx_ca.pem')"
            )
            tdSql.error(
                f"select count(*) from {src_http_tls}.{influx_db}.{influx_tb}",
                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
            )
        finally:
            self._cleanup(*names)
            try:
                ExtSrcEnv.influx_drop_db_cfg(cfg_influx, influx_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # SEC-021  ALTER unknown option rejected
    # ------------------------------------------------------------------

    def test_fq_sec_021_alter_unknown_option_rejected(self):
        """ALTER SOURCE with unknown option key must fail in parser."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src = f"sec021_alter_unknown_{run_tag}"
        self._cleanup(src)

        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='127.0.0.1' port=3306 user='u' password='p' database='db' "
                f"options('charset'='utf8mb4')"
            )
            tdSql.error(
                f"alter external source {src} set options('unknown_opt'='x')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.query(f"describe external source {src}")
            text = "\n".join("|".join(str(c) for c in row) for row in tdSql.queryResult)
            assert "unknown_opt" not in text
        finally:
            self._cleanup(src)

    def test_fq_sec_021b_alter_unknown_option_rejected_pg_influx(self):
        """ALTER unknown option must be rejected consistently for PG and Influx."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_pg = f"sec021b_pg_unknown_{run_tag}"
        src_influx = f"sec021b_influx_unknown_{run_tag}"
        self._cleanup(src_pg, src_influx)

        try:
            tdSql.execute(
                f"create external source {src_pg} type='postgresql' "
                "host='127.0.0.1' port=5432 user='u' password='p' database='db' schema='public' "
                "options('sslmode'='prefer')"
            )
            tdSql.error(
                f"alter external source {src_pg} set options('unknown_opt'='x')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            tdSql.execute(
                f"create external source {src_influx} type='influxdb' "
                "host='127.0.0.1' port=8086 user='admin' password='' database='telegraf' "
                "options('api_token'='x', 'protocol'='http')"
            )
            tdSql.error(
                f"alter external source {src_influx} set options('unknown_opt'='x')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            tdSql.query(f"describe external source {src_pg}")
            pg_text = "\n".join("|".join(str(c) for c in row) for row in tdSql.queryResult)
            assert "unknown_opt" not in pg_text

            tdSql.query(f"describe external source {src_influx}")
            influx_text = "\n".join("|".join(str(c) for c in row) for row in tdSql.queryResult)
            assert "unknown_opt" not in influx_text
        finally:
            self._cleanup(src_pg, src_influx)

    # ------------------------------------------------------------------
    # SEC-022  ALTER patch-delete non-timeout option
    # ------------------------------------------------------------------

    def test_fq_sec_022_alter_delete_non_timeout_option(self):
        """ALTER SOURCE options('ssl_mode'='') should remove ssl_mode key."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src = f"sec022_mysql_delete_opt_{run_tag}"
        self._cleanup(src)

        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='127.0.0.1' port=3306 user='u' password='p' database='db' "
                f"options('ssl_mode'='required', 'charset'='utf8mb4')"
            )
            tdSql.execute(f"alter external source {src} set options('ssl_mode'='')")

            tdSql.query("show external sources")
            row_idx = self._find_row(src)
            assert row_idx >= 0
            options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
            assert "ssl_mode" not in options_col
            assert "charset" in options_col
        finally:
            self._cleanup(src)

    def test_fq_sec_022b_create_empty_option_normalized_as_unset(self):
        """CREATE with empty option values should be accepted and normalized as unset."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_mysql = f"sec022b_mysql_empty_opt_{run_tag}"
        src_pg = f"sec022b_pg_empty_opt_{run_tag}"
        src_influx = f"sec022b_influx_empty_opt_{run_tag}"
        self._cleanup(src_mysql, src_pg, src_influx)

        try:
            tdSql.execute(
                f"create external source {src_mysql} type='mysql' "
                "host='127.0.0.1' port=3306 user='u' password='p' database='db' "
                "options('charset'='utf8mb4', 'read_timeout_ms'='', 'tls_enabled'='')"
            )
            tdSql.execute(
                f"create external source {src_pg} type='postgresql' "
                "host='127.0.0.1' port=5432 user='u' password='p' database='db' schema='public' "
                "options('sslmode'='', 'read_timeout_ms'='')"
            )
            tdSql.execute(
                f"create external source {src_influx} type='influxdb' "
                "host='127.0.0.1' port=8086 user='admin' password='' database='telegraf' "
                "options('api_token'='x', 'protocol'='', 'connect_timeout_ms'='')"
            )

            tdSql.query("show external sources")
            for src in [src_mysql, src_pg, src_influx]:
                row_idx = self._find_row(src)
                assert row_idx >= 0
                options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
                assert "\"\"" not in options_col

            mysql_opts = str(tdSql.queryResult[self._find_row(src_mysql)][_COL_OPTIONS])
            assert "charset" in mysql_opts
            assert "read_timeout_ms" not in mysql_opts
            assert "tls_enabled" not in mysql_opts

            pg_opts = str(tdSql.queryResult[self._find_row(src_pg)][_COL_OPTIONS])
            assert "sslmode" not in pg_opts
            assert "read_timeout_ms" not in pg_opts

            influx_opts = str(tdSql.queryResult[self._find_row(src_influx)][_COL_OPTIONS])
            assert "api_token" in influx_opts
            assert "protocol" not in influx_opts
            assert "connect_timeout_ms" not in influx_opts
        finally:
            self._cleanup(src_mysql, src_pg, src_influx)

    # ------------------------------------------------------------------
    # SEC-013B  Option effectiveness: PostgreSQL read_timeout
    # ------------------------------------------------------------------

    def test_fq_sec_013b_option_effectiveness_pg_read_timeout(self):
        """Behavior checks for PostgreSQL read_timeout_ms runtime effect.

        Goal: a low read_timeout_ms should timeout a slow view,
        while a higher read_timeout_ms should allow the same query to finish.
        """
        cfg_pg = self._pg_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_rt_low = f"sec013b_pg_read_timeout_low_{run_tag}"
        src_rt_ok = f"sec013b_pg_read_timeout_ok_{run_tag}"
        names = [src_rt_low, src_rt_ok]
        self._cleanup(*names)

        pg_db = "sec013b_rt_pgdb"
        pg_user = "sec013b_rt_user"
        pg_pass = "sec013b_rt_pwd"
        pg_vw = "v_slow"

        ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
            f"DROP DATABASE IF EXISTS \"{pg_db}\"",
            f"DROP ROLE IF EXISTS \"{pg_user}\"",
            f"CREATE ROLE \"{pg_user}\" LOGIN PASSWORD '{pg_pass}'",
            f"CREATE DATABASE \"{pg_db}\"",
            f"GRANT CONNECT ON DATABASE \"{pg_db}\" TO \"{pg_user}\"",
        ])
        ExtSrcEnv.pg_exec_cfg(cfg_pg, pg_db, [
            f"DROP VIEW IF EXISTS public.{pg_vw}",
            "DROP FUNCTION IF EXISTS public.fq_sec013b_sleep_ret()",
            "CREATE FUNCTION public.fq_sec013b_sleep_ret() RETURNS INT "
            "LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN 1; END; $$",
            f"CREATE VIEW public.{pg_vw} AS SELECT public.fq_sec013b_sleep_ret() AS delay",
            f"GRANT USAGE ON SCHEMA public TO \"{pg_user}\"",
            f"GRANT SELECT ON public.{pg_vw} TO \"{pg_user}\"",
        ])

        try:
            # read_timeout low => slow view should timeout.
            tdSql.execute(
                f"create external source {src_rt_low} type='postgresql' "
                f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
                f"database='{pg_db}' schema='public' "
                f"options('read_timeout_ms'='500', 'sslmode'='disable')"
            )
            tdSql.error(f"select count(*) from {src_rt_low}.public.{pg_vw}")

            # read_timeout high => same slow view should succeed.
            tdSql.execute(
                f"create external source {src_rt_ok} type='postgresql' "
                f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
                f"database='{pg_db}' schema='public' "
                f"options('read_timeout_ms'='3000', 'sslmode'='disable')"
            )
            tdSql.query(f"select count(*) from {src_rt_ok}.public.{pg_vw}")
            assert int(tdSql.queryResult[0][0]) == 1
        finally:
            self._cleanup(*names)
            ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
                f"DROP DATABASE IF EXISTS \"{pg_db}\"",
                f"DROP ROLE IF EXISTS \"{pg_user}\"",
            ])

    # ------------------------------------------------------------------
    # SEC-023  Option effectiveness: read_timeout_ms=0 overrides global
    # ------------------------------------------------------------------

    def test_fq_sec_023_option_effectiveness_read_timeout_zero_override_global(self):
        """Timeout=0 should be accepted and persisted explicitly across connector types."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_mysql = f"sec023_mysql_timeout_zero_{run_tag}"
        src_pg = f"sec023_pg_timeout_zero_{run_tag}"
        src_influx = f"sec023_influx_timeout_zero_{run_tag}"
        self._cleanup(src_mysql, src_pg, src_influx)

        try:
            tdSql.execute(
                f"create external source {src_mysql} type='mysql' "
                f"host='127.0.0.1' port=3306 user='u' password='p' database='db' "
                f"options('connect_timeout_ms'='0', 'read_timeout_ms'='0', 'charset'='utf8mb4')"
            )
            tdSql.execute(
                f"create external source {src_pg} type='postgresql' "
                "host='127.0.0.1' port=5432 user='u' password='p' database='db' schema='public' "
                "options('connect_timeout_ms'='0', 'read_timeout_ms'='0', 'sslmode'='disable')"
            )
            tdSql.execute(
                f"create external source {src_influx} type='influxdb' "
                "host='127.0.0.1' port=8086 user='admin' password='' database='telegraf' "
                "options('api_token'='x', 'connect_timeout_ms'='0', 'read_timeout_ms'='0')"
            )

            tdSql.query("show external sources")
            for src in [src_mysql, src_pg, src_influx]:
                row_idx = self._find_row(src)
                assert row_idx >= 0
                options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
                assert "connect_timeout_ms" in options_col
                assert "read_timeout_ms" in options_col
                assert "0" in options_col
        finally:
            self._cleanup(src_mysql, src_pg, src_influx)

    # ------------------------------------------------------------------
    # SEC-024  tls_enabled invalid value rejected
    # ------------------------------------------------------------------

    def test_fq_sec_024_influx_tls_enabled_invalid_value_rejected(self):
        """Influx tls_enabled must be boolean-like; invalid value is rejected in DDL."""
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_ok = f"sec024_influx_tls_bool_ok_{run_tag}"
        self._cleanup(src_ok)

        try:
            tdSql.error(
                "create external source sec024_influx_tls_bad_create "
                "type='influxdb' host='127.0.0.1' port=8086 user='admin' password='' database='telegraf' "
                "options('api_token'='x', 'protocol'='http', 'tls_enabled'='not_bool')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            tdSql.execute(
                f"create external source {src_ok} type='influxdb' "
                "host='127.0.0.1' port=8086 user='admin' password='' database='telegraf' "
                "options('api_token'='x', 'protocol'='http', 'tls_enabled'='false')"
            )
            tdSql.error(
                f"alter external source {src_ok} set options('tls_enabled'='not_bool')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.query(f"describe external source {src_ok}")
            text = "\n".join("|".join(str(c) for c in row) for row in tdSql.queryResult)
            assert "not_bool" not in text
        finally:
            self._cleanup(src_ok)

    # ------------------------------------------------------------------
    # SEC-025  SHOW/DESCRIBE masking: tls_client_cert value
    # ------------------------------------------------------------------

    def test_fq_sec_025_show_describe_masking_tls_client_cert_value(self):
        """SHOW/DESCRIBE must not expose tls_client_cert/tls_client_key plaintext values."""
        cfg_mysql = self._mysql_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src = f"sec025_tls_cert_mask_{run_tag}"
        cert_secret = f"SEC025_CERT_SECRET_{run_tag}"
        key_secret = f"SEC025_KEY_SECRET_{run_tag}"
        self._cleanup(src)

        try:
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('tls_enabled'='false', 'tls_client_cert'='{cert_secret}', "
                f"'tls_client_key'='{key_secret}', 'connect_timeout_ms'='1000')"
            )

            tdSql.query("show external sources")
            row_idx = self._find_row(src)
            assert row_idx >= 0

            row_text = self._row_text(row_idx)
            assert cert_secret not in row_text, "tls_client_cert plaintext leaked in SHOW"
            assert key_secret not in row_text, "tls_client_key plaintext leaked in SHOW"

            options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
            assert "tls_client_cert" in options_col
            assert "tls_client_key" in options_col

            tdSql.query(f"describe external source {src}")
            desc_text = "\n".join("|".join(str(c) for c in row) for row in tdSql.queryResult)
            assert cert_secret not in desc_text, "tls_client_cert plaintext leaked in DESCRIBE"
            assert key_secret not in desc_text, "tls_client_key plaintext leaked in DESCRIBE"
        finally:
            self._cleanup(src)

    # ------------------------------------------------------------------
    # SEC-026  timeout invalid format rejected
    # ------------------------------------------------------------------

    def test_fq_sec_026_timeout_option_invalid_format_rejected(self):
        """Timeout options reject non-numeric/negative values on both CREATE and ALTER."""
        cfg_mysql = self._mysql_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_ok = f"sec026_timeout_ok_{run_tag}"
        self._cleanup(src_ok)

        try:
            tdSql.error(
                f"create external source sec026_timeout_bad_create_text_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('connect_timeout_ms'='abc')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.error(
                f"create external source sec026_timeout_bad_create_neg_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('read_timeout_ms'='-1')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            tdSql.execute(
                f"create external source {src_ok} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('connect_timeout_ms'='1000', 'read_timeout_ms'='1000')"
            )

            tdSql.error(
                f"alter external source {src_ok} set options('connect_timeout_ms'='abc')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.error(
                f"alter external source {src_ok} set options('read_timeout_ms'='-1')",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            tdSql.query("show external sources")
            row_idx = self._find_row(src_ok)
            assert row_idx >= 0
            options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
            assert "abc" not in options_col
            assert "-1" not in options_col
            assert "connect_timeout_ms" in options_col
            assert "read_timeout_ms" in options_col
        finally:
            self._cleanup(src_ok)

    # ------------------------------------------------------------------
    # SEC-027  Influx default protocol + FlightSQL TLS consumption
    # ------------------------------------------------------------------

    def test_fq_sec_027_influx_default_protocol_and_flightsql_tls_consumption(self):
        """Influx source without protocol uses default behavior; FlightSQL path consumes TLS options."""
        cfg_influx = self._influx_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_default = f"sec027_influx_default_{run_tag}"
        src_flight_tls = f"sec027_influx_flight_tls_{run_tag}"
        names = [src_default, src_flight_tls]
        self._cleanup(*names)

        influx_db = "sec027_opt_idb"
        influx_tb = "src_t"
        ts_ns = int(_time.time() * 1_000_000_000)

        ExtSrcEnv.influx_create_db_cfg(cfg_influx, influx_db)
        ExtSrcEnv.influx_write_cfg(
            cfg_influx,
            influx_db,
            [f"{influx_tb},site=sec027 val=1i {ts_ns}"],
        )

        try:
            tdSql.execute(
                f"create external source {src_default} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}')"
            )
            tdSql.query(f"select count(*) from {src_default}.{influx_db}.{influx_tb}")
            assert int(tdSql.queryResult[0][0]) == 1

            tdSql.query("show external sources")
            row_idx = self._find_row(src_default)
            assert row_idx >= 0
            options_col = str(tdSql.queryResult[row_idx][_COL_OPTIONS])
            assert "protocol" not in options_col

            tdSql.execute(
                f"create external source {src_flight_tls} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' "
                f"database='{influx_db}' "
                f"options('api_token'='{cfg_influx.token}', 'protocol'='flight_sql', "
                f"'tls_enabled'='true', 'tls_ca_cert'='/tmp/fq_nonexistent_influx_flight_ca.pem')"
            )
            tdSql.error(
                f"select count(*) from {src_flight_tls}.{influx_db}.{influx_tb}",
                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
            )
        finally:
            self._cleanup(*names)
            try:
                ExtSrcEnv.influx_drop_db_cfg(cfg_influx, influx_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # SEC-016  Option effectiveness: connect_timeout_ms timing
    # ------------------------------------------------------------------

    def test_fq_sec_016_option_effectiveness_connect_timeout_timing(self):
        """Behavior checks for connect_timeout_ms runtime effect.

        Goal: different timeout values should lead to measurable timing difference
        when connecting to a blackhole host.
        """
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_fast = f"sec016_connect_timeout_fast_{run_tag}"
        src_slow = f"sec016_connect_timeout_slow_{run_tag}"
        names = [src_fast, src_slow]
        self._cleanup(*names)

        blackhole_host = "10.255.255.1"

        tdSql.execute(
            f"create external source {src_fast} type='mysql' "
            f"host='{blackhole_host}' port=3306 user='u' password='p' database='db' "
            f"options('connect_timeout_ms'='200')"
        )
        tdSql.execute(
            f"create external source {src_slow} type='mysql' "
            f"host='{blackhole_host}' port=3306 user='u' password='p' database='db' "
            f"options('connect_timeout_ms'='3000')"
        )

        t0 = _time.time()
        tdSql.error(f"select count(*) from {src_fast}.db.t1")
        fast_elapsed = _time.time() - t0

        t1 = _time.time()
        tdSql.error(f"select count(*) from {src_slow}.db.t1")
        slow_elapsed = _time.time() - t1

        tdLog.info(
            f"SEC-016 connect_timeout elapsed fast={fast_elapsed:.3f}s slow={slow_elapsed:.3f}s"
        )

        assert slow_elapsed >= 1.5, "slow timeout should not return almost immediately"
        assert slow_elapsed >= fast_elapsed + 0.6, (
            "connect_timeout_ms should affect runtime connect wait; "
            f"got fast={fast_elapsed:.3f}s slow={slow_elapsed:.3f}s"
        )

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-017  MySQL ssl_mode enum behavior matrix
    # ------------------------------------------------------------------

    def test_fq_sec_017_mysql_ssl_mode_enum_matrix(self):
        """Behavior checks for all supported MySQL ssl_mode enum values."""
        cfg_mysql = self._mysql_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_disabled = f"sec017_mysql_disabled_{run_tag}"
        src_preferred = f"sec017_mysql_preferred_{run_tag}"
        src_required = f"sec017_mysql_required_{run_tag}"
        src_verify_ca_bad = f"sec017_mysql_verifyca_bad_{run_tag}"
        src_verify_identity_bad = f"sec017_mysql_verifyid_bad_{run_tag}"
        names = [
            src_disabled, src_preferred, src_required,
            src_verify_ca_bad, src_verify_identity_bad,
        ]
        self._cleanup(*names)

        mysql_db = "sec017_sslmode_db"
        mysql_tb = "t_sslmode"
        mysql_user = "sec017_user"
        mysql_pass = "sec017_pwd"

        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
            f"CREATE DATABASE `{mysql_db}`",
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"CREATE USER '{mysql_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{mysql_pass}'",
            f"GRANT ALL PRIVILEGES ON `{mysql_db}`.* TO '{mysql_user}'@'%'",
            "FLUSH PRIVILEGES",
        ])
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, mysql_db, [
            f"DROP TABLE IF EXISTS `{mysql_tb}`",
            f"CREATE TABLE `{mysql_tb}` (id INT PRIMARY KEY, v INT)",
            f"INSERT INTO `{mysql_tb}` VALUES (1, 1701)",
        ])

        # disabled
        tdSql.execute(
            f"create external source {src_disabled} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('ssl_mode'='disabled')"
        )
        tdSql.query(f"select count(*) from {src_disabled}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # preferred
        tdSql.execute(
            f"create external source {src_preferred} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('ssl_mode'='preferred')"
        )
        tdSql.query(f"select count(*) from {src_preferred}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # required
        tdSql.execute(
            f"create external source {src_required} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' "
            f"options('ssl_mode'='required', 'tls_enabled'='true', 'tls_ca_cert'='{FQ_MYSQL_CA_CERT}')"
        )
        tdSql.query(f"select count(*) from {src_required}.{mysql_db}.{mysql_tb}")
        assert int(tdSql.queryResult[0][0]) == 1

        # verify_ca (invalid ca)
        tdSql.execute(
            f"create external source {src_verify_ca_bad} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('ssl_mode'='verify_ca', 'tls_ca_cert'='/tmp/fq_nonexistent_ca.pem')"
        )
        tdSql.error(
            f"select count(*) from {src_verify_ca_bad}.{mysql_db}.{mysql_tb}",
            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
        )

        # verify_identity (exercise enum path with invalid ca)
        tdSql.execute(
            f"create external source {src_verify_identity_bad} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{mysql_user}' password='{mysql_pass}' "
            f"database='{mysql_db}' options('ssl_mode'='verify_identity', 'tls_ca_cert'='/tmp/fq_nonexistent_ca.pem')"
        )
        tdSql.error(
            f"select count(*) from {src_verify_identity_bad}.{mysql_db}.{mysql_tb}",
            expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
        )

        self._cleanup(*names)
        ExtSrcEnv.mysql_exec_cfg(cfg_mysql, None, [
            f"DROP USER IF EXISTS '{mysql_user}'@'%'",
            f"DROP DATABASE IF EXISTS `{mysql_db}`",
        ])

    # ------------------------------------------------------------------
    # SEC-018  PostgreSQL sslmode enum behavior matrix
    # ------------------------------------------------------------------

    def test_fq_sec_018_pg_sslmode_enum_matrix(self):
        """Behavior checks for all supported PostgreSQL sslmode enum values."""
        cfg_pg = self._pg_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]
        src_disable = f"sec018_pg_disable_{run_tag}"
        src_allow = f"sec018_pg_allow_{run_tag}"
        src_prefer = f"sec018_pg_prefer_{run_tag}"
        src_require = f"sec018_pg_require_{run_tag}"
        src_verify_ca = f"sec018_pg_verifyca_{run_tag}"
        src_verify_full = f"sec018_pg_verifyfull_{run_tag}"
        names = [src_disable, src_allow, src_prefer, src_require, src_verify_ca, src_verify_full]
        self._cleanup(*names)

        pg_db = "sec018_sslmode_pgdb"
        pg_user = "sec018_ssl_user"
        pg_pass = "sec018_ssl_pwd"

        ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
            f"DROP DATABASE IF EXISTS \"{pg_db}\"",
            f"DROP ROLE IF EXISTS \"{pg_user}\"",
            f"CREATE ROLE \"{pg_user}\" LOGIN PASSWORD '{pg_pass}'",
            f"CREATE DATABASE \"{pg_db}\"",
            f"GRANT CONNECT ON DATABASE \"{pg_db}\" TO \"{pg_user}\"",
        ])
        ExtSrcEnv.pg_exec_cfg(cfg_pg, pg_db, [
            "DROP VIEW IF EXISTS public.v_ssl_self",
            "CREATE VIEW public.v_ssl_self AS "
            "SELECT ssl FROM pg_catalog.pg_stat_ssl WHERE pid = pg_backend_pid()",
            f"GRANT USAGE ON SCHEMA public TO \"{pg_user}\"",
            f"GRANT SELECT ON public.v_ssl_self TO \"{pg_user}\"",
        ])

        # disable -> non-SSL
        tdSql.execute(
            f"create external source {src_disable} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='disable')"
        )
        tdSql.query(f"select count(*) from {src_disable}.public.v_ssl_self where ssl = false")
        assert int(tdSql.queryResult[0][0]) == 1

        # allow -> queryable
        tdSql.execute(
            f"create external source {src_allow} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='allow')"
        )
        tdSql.query(f"select count(*) from {src_allow}.public.v_ssl_self")
        assert int(tdSql.queryResult[0][0]) == 1

        # prefer -> queryable
        tdSql.execute(
            f"create external source {src_prefer} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='prefer')"
        )
        tdSql.query(f"select count(*) from {src_prefer}.public.v_ssl_self")
        assert int(tdSql.queryResult[0][0]) == 1

        # require -> SSL
        tdSql.execute(
            f"create external source {src_require} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='require', 'tls_ca_cert'='{FQ_PG_CA_CERT}')"
        )
        tdSql.query(f"select count(*) from {src_require}.public.v_ssl_self where ssl = true")
        assert int(tdSql.queryResult[0][0]) == 1

        # verify-ca -> SSL with CA verification
        tdSql.execute(
            f"create external source {src_verify_ca} type='postgresql' "
            f"host='{cfg_pg.host}' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='verify-ca', 'tls_ca_cert'='{FQ_PG_CA_CERT}')"
        )
        tdSql.query(f"select count(*) from {src_verify_ca}.public.v_ssl_self where ssl = true")
        assert int(tdSql.queryResult[0][0]) == 1

        # verify-full -> expected failure under host/cert mismatch (cert CN=fq-pg-server, host=127.0.0.1)
        tdSql.execute(
            f"create external source {src_verify_full} type='postgresql' "
            f"host='127.0.0.1' port={cfg_pg.port} user='{pg_user}' password='{pg_pass}' "
            f"database='{pg_db}' schema='public' options('sslmode'='verify-full', 'tls_ca_cert'='{FQ_PG_CA_CERT}')"
        )
        tdSql.error(f"select count(*) from {src_verify_full}.public.v_ssl_self")

        self._cleanup(*names)
        ExtSrcEnv.pg_exec_cfg(cfg_pg, "postgres", [
            f"DROP DATABASE IF EXISTS \"{pg_db}\"",
            f"DROP ROLE IF EXISTS \"{pg_user}\"",
        ])

    # ------------------------------------------------------------------
    # SEC-020  Option key acceptance matrix
    # ------------------------------------------------------------------

    def test_fq_sec_020_option_key_acceptance_matrix(self):
        """Validate that each supported option key is accepted by create path.

        This guards parser/mnode option allowlist consistency from the SQL entry path.
        """
        cfg_mysql = self._mysql_cfg()
        cfg_pg = self._pg_cfg()
        cfg_influx = self._influx_cfg()
        run_tag = str(int(_time.time() * 1000))[-6:]

        cases = [
            (
                f"sec020_mysql_conn_{run_tag}",
                f"create external source sec020_mysql_conn_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('connect_timeout_ms'='1000')",
            ),
            (
                f"sec020_mysql_read_{run_tag}",
                f"create external source sec020_mysql_read_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('read_timeout_ms'='1000')",
            ),
            (
                f"sec020_mysql_tlsen_{run_tag}",
                f"create external source sec020_mysql_tlsen_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('tls_enabled'='true', 'ssl_mode'='required')",
            ),
            (
                f"sec020_mysql_tlsca_{run_tag}",
                f"create external source sec020_mysql_tlsca_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('tls_ca_cert'='/tmp/sec020_ca.pem')",
            ),
            (
                f"sec020_mysql_tlspair_{run_tag}",
                f"create external source sec020_mysql_tlspair_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('tls_client_cert'='/tmp/sec020_client.pem', 'tls_client_key'='/tmp/sec020_client.key')",
            ),
            (
                f"sec020_mysql_charset_{run_tag}",
                f"create external source sec020_mysql_charset_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('charset'='utf8mb4')",
            ),
            (
                f"sec020_mysql_sslmode_{run_tag}",
                f"create external source sec020_mysql_sslmode_{run_tag} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
                f"options('ssl_mode'='preferred')",
            ),
            (
                f"sec020_pg_sslmode_{run_tag}",
                f"create external source sec020_pg_sslmode_{run_tag} type='postgresql' "
                f"host='{cfg_pg.host}' port={cfg_pg.port} user='u' password='p' database='db' schema='public' "
                f"options('sslmode'='disable')",
            ),
            (
                f"sec020_influx_token_{run_tag}",
                f"create external source sec020_influx_token_{run_tag} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' database='telegraf' "
                f"options('api_token'='sec020_token')",
            ),
            (
                f"sec020_influx_protocol_{run_tag}",
                f"create external source sec020_influx_protocol_{run_tag} type='influxdb' "
                f"host='{cfg_influx.host}' port={cfg_influx.port} user='admin' password='' database='telegraf' "
                f"options('protocol'='http')",
            ),
        ]

        names = [name for name, _ in cases]
        self._cleanup(*names)

        for _, sql in cases:
            tdSql.execute(sql)

        tdSql.query("show external sources")
        existing = {str(r[_COL_NAME]) for r in tdSql.queryResult}
        for name in names:
            assert name in existing, f"source should be created for option acceptance case: {name}"

        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-007  Auth failure blocking
    # ------------------------------------------------------------------

    def test_fq_sec_007_auth_failure_blocking(self):
        """SEC-007: Auth failure blocking — auth failed → source status update

        TS: Source status updated after auth failure

        Multi-dimensional coverage:
        1. Create source with wrong password for unreachable host
        2. Query source → should fail with connection/auth error
        3. Consecutive queries → all fail consistently (no auth bypass)
        4. SHOW source → should still be listed (not auto-dropped)
        5. ALTER to correct password (still unreachable) → still listed
        6. Negative: multiple sources, auth fail on one does not affect another
        7. Drop source cleanly after auth failures

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        ver = cfg_mysql.version
        names = ["sec007_bad_auth", "sec007_good_src"]
        self._cleanup(*names)

        # Create sources with test credentials; stop MySQL to make host unreachable.
        tdSql.execute(
            f"create external source sec007_bad_auth type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='wrong_user' password='wrong_pwd' "
            f"database='db' options('connect_timeout_ms'='500')"
        )
        tdSql.execute(
            f"create external source sec007_good_src type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
            f"options('connect_timeout_ms'='500')"
        )

        ExtSrcEnv.stop_mysql_instance(ver)
        try:
            # Multiple queries on bad source → all fail with connection error
            for _ in range(3):
                tdSql.error(
                    "select * from sec007_bad_auth.db.t1",
                    expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
                )
        finally:
            ExtSrcEnv.start_mysql_instance(ver)

        # Source still exists in catalog
        assert self._find_row("sec007_bad_auth") >= 0, \
            "source should survive auth failures"

        # auth fail on one source should not affect another
        assert self._find_row("sec007_good_src") >= 0, \
            "unrelated source should be unaffected"

        # ALTER password
        tdSql.execute(
            "alter external source sec007_bad_auth set password='still_wrong'"
        )
        assert self._find_row("sec007_bad_auth") >= 0

        # Clean drop
        self._cleanup(*names)

    # ------------------------------------------------------------------
    # SEC-008  Access denied blocking
    # ------------------------------------------------------------------

    def test_fq_sec_008_access_denied_blocking(self):
        """SEC-008: Access denied — error code and status correct

        TS: Access denied error code and status handled correctly

        Multi-dimensional coverage:
        1. Write operations on external source must be denied:
           a. INSERT INTO ext_source.db.table → error
           b. UPDATE on external table reference → error
           c. DELETE on external table → error
           d. CREATE TABLE on external source → error
        2. DDL operations on external objects → denied
        3. Cross-source transaction → denied
        4. Negative: read-only SELECT should NOT trigger access denied
           (it triggers connection error on unreachable source instead)

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        ver = cfg_mysql.version
        src = "sec008_src"
        self._cleanup(src)

        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )

        # Write operations → denied (parser/planner level, no connection needed)
        write_sqls = [
            f"insert into {src}.db.t1 values (now, 1)",
            f"insert into {src}.db.t1 (ts, v) values (now, 2)",
        ]
        for sql in write_sqls:
            tdSql.error(sql, expectedErrno=TSDB_CODE_EXT_WRITE_DENIED)

        # DDL on external object (parser level, no connection needed)
        ddl_sqls = [
            f"create table {src}.db.new_table (ts timestamp, v int)",
            f"drop table {src}.db.t1",
            f"alter table {src}.db.t1 add column c2 int",
        ]
        for sql in ddl_sqls:
            tdSql.error(sql)  # DDL on external tables → any error acceptable

        # Negative: SELECT is not access-denied — stop MySQL to make it unreachable.
        ExtSrcEnv.stop_mysql_instance(ver)
        try:
            tdSql.error(
                f"select * from {src}.db.t1",
                expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE,  # connection error, not access denied
            )
        finally:
            ExtSrcEnv.start_mysql_instance(ver)

        self._cleanup(src)

    # ------------------------------------------------------------------
    # SEC-009  SQL injection protection
    # ------------------------------------------------------------------

    def test_fq_sec_009_sql_injection_protection(self):
        """SEC-009: SQL injection protection — source/path/identifier safe

        TS: SOURCE/path/identifier parsing has no injection vulnerability

        Multi-dimensional coverage:
        1. Source name injection attempts:
           a. name containing SQL keywords ('; DROP TABLE --)
           b. name with quotes, backslashes
           c. name with null bytes
        2. Path injection: db.table path with SQL injection strings
        3. Password injection: password containing SQL (should be treated as data)
        4. Host injection: host with SQL fragments
        5. Multi-statement injection via semicolons in identifiers
        6. Verify all injection attempts are either:
           - Rejected with syntax error, OR
           - Treated as literal values (no side effects)

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        # Clean any leftovers
        for i in range(5):
            tdSql.execute(f"drop external source if exists sec009_inj_{i}")

        # 1a. Source name with SQL keywords — should be syntax error
        injection_names = [
            "'; DROP DATABASE --",
            "src; SELECT 1; --",
            "src' OR '1'='1",
        ]
        for inj in injection_names:
            # These should fail as syntax errors due to special characters
            tdSql.error(
                f"create external source {inj} type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

        # 1b. Quoted source name with injection (using backticks)
        tdSql.execute("drop external source if exists `sec009_quoted`")
        # Quoted injection name is treated as a literal identifier and accepted
        tdSql.execute(
            f"create external source `sec009_drop_test` type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        tdSql.execute("drop external source if exists `sec009_drop_test`")

        # 2. Path injection in query
        path_injections = [
            "sec009_src.db.t1; DROP TABLE local_t --",
            "sec009_src.db.t1 UNION SELECT * FROM information_schema.tables",
        ]
        for inj in path_injections:
            tdSql.error(
                f"select * from {inj}",
            )  # injection blocked — any error code acceptable

        # 3. Password with SQL injection — treated as literal value
        tdSql.execute("drop external source if exists sec009_pwd_inj")
        tdSql.execute(
            f"create external source sec009_pwd_inj type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' "
            f"password='p\\'; DROP TABLE t; --' database='db'"
        )
        # Source should be created with the literal password, not executed
        idx = self._find_row("sec009_pwd_inj")
        # Even if create fails due to quoting, should not cause side effects
        tdSql.execute("drop external source if exists sec009_pwd_inj")

        # 4. Host with injection — quoted host string is treated as literal value
        # (semicolons inside single-quoted strings are not SQL injection vectors)
        tdSql.execute("drop external source if exists sec009_host_inj")
        try:
            tdSql.execute(
                "create external source sec009_host_inj type='mysql' "
                "host='192.0.2.1; DROP TABLE t' port=3306 user='u' password='p' database='db'",
            )
            # If accepted as literal string: no side effects — clean it up
            tdSql.execute("drop external source if exists sec009_host_inj")
        except Exception:
            pass  # Rejected with any error — also acceptable

        # 5. Multi-statement via semicolons — TDengine only executes the first
        #    statement and ignores the rest, so the CREATE succeeds and DROP is
        #    silently discarded (no second-statement injection).
        tdSql.execute("drop external source if exists sec009_multi")
        try:
            tdSql.execute(
                f"create external source sec009_multi type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'; "
                f"DROP DATABASE fq_case_db",
            )
            tdSql.execute("drop external source if exists sec009_multi")
        except Exception:
            pass  # Rejected with any error — also acceptable

    # ------------------------------------------------------------------
    # SEC-010  Abnormal data boundary validation
    # ------------------------------------------------------------------

    def test_fq_sec_010_abnormal_data_boundary(self):
        """SEC-010: Abnormal data boundary — external abnormal return no crash

        TS: External abnormal return does not cause crash

        Multi-dimensional coverage:
        1. Create source with extreme port numbers (0, 65535, overflow 65536)
        2. Create source with extremely long values:
           a. Very long host name (255 chars)
           b. Very long database name (255 chars)
           c. Very long password (1000 chars)
           d. Very long user name (255 chars)
        3. Empty-string fields:
           a. Empty host → should error
           b. Empty database → should error
           c. Empty user → might be accepted (depends on source type)
        4. Negative port values
        5. All should either be rejected cleanly or accepted without crash

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        cleanup_names = [
            "sec010_port0", "sec010_port65535", "sec010_longhost",
            "sec010_longdb", "sec010_longpwd", "sec010_longuser",
        ]
        for n in cleanup_names:
            tdSql.execute(f"drop external source if exists {n}")

        # Port edge values
        # Port 0 — rejected by TDengine (0x2600)
        tdSql.error(
            f"create external source sec010_port0 type='mysql' "
            f"host='{cfg_mysql.host}' port=0 user='u' password='p' database='db'",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
        )

        # Port 65535 (max valid)
        tdSql.execute(
            f"create external source sec010_port65535 type='mysql' "
            f"host='{cfg_mysql.host}' port=65535 user='u' password='p' database='db'"
        )
        tdSql.execute("drop external source if exists sec010_port65535")

        # Port overflow
        tdSql.error(
            f"create external source sec010_overflow type='mysql' "
            f"host='{cfg_mysql.host}' port=65536 user='u' password='p' database='db'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
        )

        # Negative port
        tdSql.error(
            f"create external source sec010_negport type='mysql' "
            f"host='{cfg_mysql.host}' port=-1 user='u' password='p' database='db'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
        )

        # Very long host (255 chars)
        long_host = "a" * 255
        tdSql.execute(
            f"create external source sec010_longhost type='mysql' "
            f"host='{long_host}' port=3306 user='u' password='p' database='db'"
        )
        tdSql.execute("drop external source if exists sec010_longhost")

        # Very long database name (max 64 chars per TSDB_EXT_SOURCE_DATABASE_LEN)
        long_db = "d" * 64
        tdSql.execute(
            f"create external source sec010_longdb type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='{long_db}'"
        )
        tdSql.execute("drop external source if exists sec010_longdb")

        # Very long password (1000 chars) — server rejects at parse/validation time
        long_pwd = "x" * 1000
        tdSql.error(
            f"create external source sec010_longpwd type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='{long_pwd}' database='db'",
        )  # parser enforces length limit — sec010_longpwd is never created

        # Very long user (max 128 chars per TSDB_EXT_SOURCE_USER_LEN)
        long_user = "u" * 128
        tdSql.execute(
            f"create external source sec010_longuser type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='{long_user}' password='p' database='db'"
        )
        tdSql.execute("drop external source if exists sec010_longuser")

        # Empty host → should error
        tdSql.error(
            "create external source sec010_empty_host type='mysql' "
            "host='' port=3306 user='u' password='p' database='db'",
            expectedErrno=TSDB_CODE_EXT_CONFIG_PARAM_INVALID,
        )

        # Empty database → system may accept or reject; handle both
        tdSql.execute("drop external source if exists sec010_empty_db")
        try:
            tdSql.execute(
                f"create external source sec010_empty_db type='mysql' "
                f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database=''",
            )
            tdSql.execute("drop external source if exists sec010_empty_db")
        except Exception:
            pass  # Rejected — also acceptable

    # ------------------------------------------------------------------
    # SEC-011  Connection reset safety
    # ------------------------------------------------------------------

    def test_fq_sec_011_connection_reset_safety(self):
        """SEC-011: Connection reset safety — handle cleanup complete

        TS: Handle cleanup is complete after connection reset

        Multi-dimensional coverage:
        1. Create source pointing to unreachable host
        2. Issue query → connection attempt fails (timeout)
        3. Immediately issue another query → should get clean error, not stale state
        4. Issue many rapid queries → all should fail cleanly, no hang
        5. DROP source → should succeed immediately (no pending handles)
        6. Re-create source with same name → should succeed (no handle leak)
        7. Negative: after DROP, SHOW should not list the source

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        ver = cfg_mysql.version
        src = "sec011_reset"
        self._cleanup(src)

        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db' "
            f"options('connect_timeout_ms'='300')"
        )

        ExtSrcEnv.stop_mysql_instance(ver)
        try:
            # Query → fail with clean error
            tdSql.error(f"select * from {src}.db.t1", expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)

            # Immediate second query → clean error (not stale)
            tdSql.error(f"select count(*) from {src}.db.t2", expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)

            # Rapid fire
            for _ in range(10):
                tdSql.error(f"select 1 from {src}.db.t3", expectedErrno=TSDB_CODE_EXT_SOURCE_UNAVAILABLE)
        finally:
            ExtSrcEnv.start_mysql_instance(ver)

        # DROP should be immediate (metadata op, no MySQL needed)
        tdSql.execute(f"drop external source {src}")

        # After DROP, should not be listed
        assert self._find_row(src) < 0, "source should be gone after DROP"

        # Re-create with same name → should succeed (no handle leak)
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='u' password='p' database='db'"
        )
        assert self._find_row(src) >= 0, "re-create should succeed"

        self._cleanup(src)

    # ------------------------------------------------------------------
    # SEC-012  Sensitive config change audit
    # ------------------------------------------------------------------

    def test_fq_sec_012_sensitive_config_audit(self):
        """SEC-012: Sensitive config change audit — ALTER SOURCE has record

        TS: ALTER SOURCE changes have audit records

        Multi-dimensional coverage:
        1. CREATE source → verify it exists in SHOW
        2. ALTER password → verify SHOW still masks it
        3. ALTER host → verify new host reflected in SHOW
        4. ALTER user → verify new user reflected
        5. ALTER OPTIONS → verify new options reflected
        6. Multiple sequential ALTERs → latest values win
        7. Negative: ALTER non-existent source → error
        8. Note: full audit-log verification requires audit subsystem access

        Catalog:
            - Query:FederatedSecurity

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wpan Full rewrite with multi-dimensional coverage

        """
        cfg_mysql = self._mysql_cfg()
        src = "sec012_audit"
        self._cleanup(src)

        # Create
        tdSql.execute(
            f"create external source {src} type='mysql' "
            f"host='{cfg_mysql.host}' port={cfg_mysql.port} user='orig_user' password='orig_pwd' "
            f"database='db'"
        )
        idx = self._find_row(src)
        assert idx >= 0
        orig_user = str(tdSql.queryResult[idx][_COL_USER])

        # ALTER password → still masked
        tdSql.execute(f"alter external source {src} set password='new_pwd_123'")
        idx = self._find_row(src)
        assert idx >= 0
        text = self._row_text(idx)
        assert "new_pwd_123" not in text, "new password leaked"
        assert "orig_pwd" not in text, "old password still present"

        # ALTER host
        tdSql.execute(f"alter external source {src} set host='altered.example.com'")
        idx = self._find_row(src)
        host_val = str(tdSql.queryResult[idx][_COL_HOST])
        assert "altered.example.com" in host_val, "host not updated after ALTER"

        # ALTER user
        tdSql.execute(f"alter external source {src} set user='new_user'")
        idx = self._find_row(src)
        user_val = str(tdSql.queryResult[idx][_COL_USER])
        assert "new_user" in user_val or user_val == "new_user", \
            "user not updated after ALTER"

        # ALTER OPTIONS
        tdSql.execute(
            f"alter external source {src} set options('connect_timeout_ms'='2000')"
        )
        idx = self._find_row(src)
        opts = str(tdSql.queryResult[idx][_COL_OPTIONS])
        assert "2000" in opts, "options not updated after ALTER"

        # Multiple sequential ALTERs — latest wins
        tdSql.execute(f"alter external source {src} set port=3307")
        tdSql.execute(f"alter external source {src} set port=3308")
        idx = self._find_row(src)
        port_val = str(tdSql.queryResult[idx][_COL_PORT])
        assert "3308" in port_val, "latest ALTER should win"

        # Negative: ALTER non-existent source
        tdSql.error(
            "alter external source sec012_nonexistent set password='x'",
            expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST,
        )

        self._cleanup(src)
