import os
import pytest

from new_test_framework.utils import tdLog, tdSql, tdCom


# === Standard TDengine error codes (community edition) ===
TSDB_CODE_PAR_SYNTAX_ERROR = int(0x80002600)
TSDB_CODE_PAR_TABLE_NOT_EXIST = int(0x80002603)
TSDB_CODE_PAR_INVALID_REF_COLUMN = int(0x8000268D)
TSDB_CODE_PAR_SUBQUERY_IN_EXPR = int(0x800026A7)
TSDB_CODE_MND_DB_NOT_EXIST = int(0x80000388)
TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH = int(0x80006208)

# === External Source Management error codes (enterprise edition) ===
# TODO: Replace None with the actual hex code once the enterprise feature ships.
#       Using None means tdSql.error() checks only that *some* error occurs.

# CREATE EXTERNAL SOURCE: source name already exists (no IF NOT EXISTS)
TSDB_CODE_MND_EXTERNAL_SOURCE_ALREADY_EXISTS = None

# DROP / ALTER EXTERNAL SOURCE: source name not found (no IF EXISTS)
TSDB_CODE_MND_EXTERNAL_SOURCE_NOT_EXIST = None

# CREATE EXTERNAL SOURCE: name conflicts with an existing local database name
TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT = None

# ALTER EXTERNAL SOURCE: attempted to change the immutable TYPE field
TSDB_CODE_MND_EXTERNAL_SOURCE_ALTER_TYPE_DENIED = None

# OPTIONS conflict: tls_enabled=true + ssl_mode=disabled (MySQL) or sslmode=disable (PG)
TSDB_CODE_EXT_OPTIONS_TLS_CONFLICT = None

# === Path resolution / type mapping / pushdown / vtable DDL error codes ===
# TODO: Replace None with actual hex codes once each feature ships.

# Path: external source name not found in catalog
TSDB_CODE_EXT_SOURCE_NOT_FOUND = None

# Path: default DATABASE/SCHEMA not configured when short path used
TSDB_CODE_EXT_DEFAULT_NS_MISSING = None

# Path: invalid number of path segments
TSDB_CODE_EXT_INVALID_PATH = None

# Type mapping: external column type cannot be mapped to any TDengine type
TSDB_CODE_EXT_TYPE_NOT_MAPPABLE = None

# Type mapping: external table has no column mappable to TIMESTAMP primary key
TSDB_CODE_EXT_NO_TS_KEY = None

# SQL: syntax/feature not supported on external tables
TSDB_CODE_EXT_SYNTAX_UNSUPPORTED = None

# SQL: pushdown execution failed at remote side
TSDB_CODE_EXT_PUSHDOWN_FAILED = None

# SQL: external source is unavailable (connection/auth/resource failure)
TSDB_CODE_EXT_SOURCE_UNAVAILABLE = None

# Write: INSERT/UPDATE/DELETE on external table denied
TSDB_CODE_EXT_WRITE_DENIED = None

# Stream: stream computation on external tables not supported
TSDB_CODE_EXT_STREAM_NOT_SUPPORTED = None

# Subscribe: subscription on external tables not supported
TSDB_CODE_EXT_SUBSCRIBE_NOT_SUPPORTED = None

# VTable DDL: referenced external source does not exist
TSDB_CODE_FOREIGN_SERVER_NOT_EXIST = None

# VTable DDL: referenced external database does not exist
TSDB_CODE_FOREIGN_DB_NOT_EXIST = None

# VTable DDL: referenced external table does not exist
TSDB_CODE_FOREIGN_TABLE_NOT_EXIST = None

# VTable DDL: referenced external column does not exist
TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST = None

# VTable DDL: virtual-table declared type incompatible with external column mapping
TSDB_CODE_FOREIGN_TYPE_MISMATCH = None

# VTable DDL: external table has no column mappable to TIMESTAMP primary key
TSDB_CODE_FOREIGN_NO_TS_KEY = None

# System: configuration parameter value out of range or invalid
TSDB_CODE_EXT_CONFIG_PARAM_INVALID = None

# Community edition: federated query feature disabled
TSDB_CODE_EXT_FEATURE_DISABLED = None


# =====================================================================
# External source direct-connection helpers
# =====================================================================

class ExtSrcEnv:
    """Direct connections to external databases for test data setup/teardown.

    Connection parameters are configurable via environment variables.
    Each test case uses these helpers to prepare test data in the real
    external source BEFORE querying via TDengine federated query.
    """

    MYSQL_HOST = os.getenv("FQ_MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = int(os.getenv("FQ_MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("FQ_MYSQL_USER", "root")
    MYSQL_PASS = os.getenv("FQ_MYSQL_PASS", "taosdata")

    PG_HOST = os.getenv("FQ_PG_HOST", "127.0.0.1")
    PG_PORT = int(os.getenv("FQ_PG_PORT", "5432"))
    PG_USER = os.getenv("FQ_PG_USER", "postgres")
    PG_PASS = os.getenv("FQ_PG_PASS", "taosdata")

    INFLUX_HOST = os.getenv("FQ_INFLUX_HOST", "127.0.0.1")
    INFLUX_PORT = int(os.getenv("FQ_INFLUX_PORT", "8086"))
    INFLUX_TOKEN = os.getenv("FQ_INFLUX_TOKEN", "test-token")
    INFLUX_ORG = os.getenv("FQ_INFLUX_ORG", "test-org")

    _env_checked = False

    @classmethod
    def ensure_env(cls):
        """Run ensure_ext_env.sh once per process to start external sources."""
        if cls._env_checked:
            return
        import subprocess
        script = os.path.join(os.path.dirname(__file__), "ensure_ext_env.sh")
        if os.path.exists(script):
            ret = subprocess.call(["bash", script])
            if ret != 0:
                raise RuntimeError(
                    f"ensure_ext_env.sh failed (exit={ret})")
        cls._env_checked = True

    # ---- MySQL helpers ----

    @classmethod
    def mysql_exec(cls, database, sqls):
        """Execute SQL statements on MySQL. database=None for server-level."""
        import pymysql
        conn = pymysql.connect(
            host=cls.MYSQL_HOST, port=cls.MYSQL_PORT,
            user=cls.MYSQL_USER, password=cls.MYSQL_PASS,
            database=database, autocommit=True, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def mysql_query(cls, database, sql):
        """Query MySQL, return list of row-tuples."""
        import pymysql
        conn = pymysql.connect(
            host=cls.MYSQL_HOST, port=cls.MYSQL_PORT,
            user=cls.MYSQL_USER, password=cls.MYSQL_PASS,
            database=database, charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    @classmethod
    def mysql_create_db(cls, db):
        """Create MySQL database (idempotent)."""
        cls.mysql_exec(None, [
            f"CREATE DATABASE IF NOT EXISTS `{db}` "
            f"CHARACTER SET utf8mb4"])

    @classmethod
    def mysql_drop_db(cls, db):
        """Drop MySQL database (idempotent)."""
        cls.mysql_exec(None, [f"DROP DATABASE IF EXISTS `{db}`"])

    # ---- PostgreSQL helpers ----

    @classmethod
    def pg_exec(cls, database, sqls):
        """Execute SQL statements on PG. database=None uses 'postgres'."""
        import psycopg2
        conn = psycopg2.connect(
            host=cls.PG_HOST, port=cls.PG_PORT,
            user=cls.PG_USER, password=cls.PG_PASS,
            dbname=database or "postgres")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for sql in sqls:
                    cur.execute(sql)
        finally:
            conn.close()

    @classmethod
    def pg_query(cls, database, sql):
        """Query PG, return list of row-tuples."""
        import psycopg2
        conn = psycopg2.connect(
            host=cls.PG_HOST, port=cls.PG_PORT,
            user=cls.PG_USER, password=cls.PG_PASS,
            dbname=database or "postgres")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    @classmethod
    def pg_create_db(cls, db):
        """Create PG database (idempotent)."""
        rows = cls.pg_query(
            "postgres",
            f"SELECT 1 FROM pg_database WHERE datname='{db}'")
        if not rows:
            cls.pg_exec("postgres", [f'CREATE DATABASE "{db}"'])

    @classmethod
    def pg_drop_db(cls, db):
        """Drop PG database — terminates active connections first."""
        cls.pg_exec("postgres", [
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname='{db}' AND pid <> pg_backend_pid()",
            f'DROP DATABASE IF EXISTS "{db}"',
        ])

    # ---- InfluxDB helpers ----

    @classmethod
    def influx_create_db(cls, bucket):
        """Create InfluxDB bucket (idempotent). Buckets are the InfluxDB equivalent of databases."""
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v2/buckets"
        headers = {
            "Authorization": f"Token {cls.INFLUX_TOKEN}",
            "Content-Type": "application/json",
        }
        # Check existence first
        r = requests.get(url, headers=headers,
                         params={"org": cls.INFLUX_ORG, "name": bucket})
        if r.status_code == 200:
            if any(b["name"] == bucket for b in r.json().get("buckets", [])):
                return  # already exists
        # Resolve org ID
        org_url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v2/orgs"
        r_org = requests.get(org_url, headers={"Authorization": f"Token {cls.INFLUX_TOKEN}"},
                             params={"org": cls.INFLUX_ORG})
        r_org.raise_for_status()
        orgs = r_org.json().get("orgs", [])
        if not orgs:
            raise RuntimeError(f"InfluxDB org '{cls.INFLUX_ORG}' not found")
        org_id = orgs[0]["id"]
        payload = {"orgID": org_id, "name": bucket, "retentionRules": []}
        r_create = requests.post(url, json=payload, headers=headers)
        if r_create.status_code not in (200, 201, 422):
            r_create.raise_for_status()

    @classmethod
    def influx_drop_db(cls, bucket):
        """Drop InfluxDB bucket (idempotent)."""
        import requests
        url = f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}/api/v2/buckets"
        headers = {"Authorization": f"Token {cls.INFLUX_TOKEN}"}
        r = requests.get(url, headers=headers,
                         params={"org": cls.INFLUX_ORG, "name": bucket})
        if r.status_code != 200:
            return
        for b in r.json().get("buckets", []):
            if b["name"] == bucket:
                del_r = requests.delete(f"{url}/{b['id']}", headers=headers)
                if del_r.status_code not in (200, 204, 404):
                    del_r.raise_for_status()
                break

    @classmethod
    def influx_write(cls, bucket, lines):
        """Write line-protocol data to InfluxDB."""
        import requests
        url = (f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}"
               f"/api/v2/write")
        params = {"org": cls.INFLUX_ORG, "bucket": bucket,
                  "precision": "ms"}
        headers = {"Authorization": f"Token {cls.INFLUX_TOKEN}",
                   "Content-Type": "text/plain"}
        r = requests.post(url, params=params, headers=headers,
                          data="\n".join(lines))
        r.raise_for_status()

    @classmethod
    def influx_query_csv(cls, bucket, flux_query):
        """Run a Flux query, return CSV text."""
        import requests
        url = (f"http://{cls.INFLUX_HOST}:{cls.INFLUX_PORT}"
               f"/api/v2/query")
        headers = {"Authorization": f"Token {cls.INFLUX_TOKEN}",
                   "Content-Type": "application/vnd.flux",
                   "Accept": "application/csv"}
        r = requests.post(url, headers=headers, data=flux_query)
        r.raise_for_status()
        return r.text


# =====================================================================
# Shared test mixin — eliminates duplicated helpers across test files
# =====================================================================

class FederatedQueryTestMixin:
    """Mixin providing common helper methods for federated query tests.

    Test classes can inherit from this mixin to get:
      - External source creation/cleanup shortcuts
      - Assertion helpers with proper verification
    """

    # ------------------------------------------------------------------
    # Source lifecycle helpers
    # ------------------------------------------------------------------

    def _cleanup_src(self, *names):
        """Drop external sources by name (idempotent)."""
        for n in names:
            tdSql.execute(f"drop external source if exists {n}")

    # Alias used by some files
    _cleanup = _cleanup_src

    def _mk_mysql(self, name, database="testdb"):
        """Create a MySQL external source pointing to RFC 5737 TEST-NET."""
        sql = (f"create external source {name} "
               f"type='mysql' host='192.0.2.1' port=3306 "
               f"user='u' password='p'")
        if database:
            sql += f" database={database}"
        tdSql.execute(sql)

    def _mk_pg(self, name, database="pgdb", schema="public"):
        """Create a PostgreSQL external source pointing to RFC 5737 TEST-NET."""
        sql = (f"create external source {name} "
               f"type='postgresql' host='192.0.2.1' port=5432 "
               f"user='u' password='p'")
        if database:
            sql += f" database={database}"
        if schema:
            sql += f" schema={schema}"
        tdSql.execute(sql)

    def _mk_influx(self, name, database="telegraf"):
        """Create an InfluxDB external source pointing to RFC 5737 TEST-NET."""
        sql = (f"create external source {name} "
               f"type='influxdb' host='192.0.2.1' port=8086 "
               f"user='u' password=''")
        if database:
            sql += f" database={database}"
        sql += " options('api_token'='tok','protocol'='flight_sql')"
        tdSql.execute(sql)

    # ------------------------------------------------------------------
    # Real external source creation (connects to actual databases)
    # ------------------------------------------------------------------

    def _mk_mysql_real(self, name, database="testdb"):
        """Create MySQL external source pointing to real test MySQL."""
        sql = (f"create external source {name} "
               f"type='mysql' host='{ExtSrcEnv.MYSQL_HOST}' "
               f"port={ExtSrcEnv.MYSQL_PORT} "
               f"user='{ExtSrcEnv.MYSQL_USER}' "
               f"password='{ExtSrcEnv.MYSQL_PASS}'")
        if database:
            sql += f" database={database}"
        tdSql.execute(sql)

    def _mk_pg_real(self, name, database="pgdb", schema="public"):
        """Create PG external source pointing to real test PostgreSQL."""
        sql = (f"create external source {name} "
               f"type='postgresql' host='{ExtSrcEnv.PG_HOST}' "
               f"port={ExtSrcEnv.PG_PORT} "
               f"user='{ExtSrcEnv.PG_USER}' "
               f"password='{ExtSrcEnv.PG_PASS}'")
        if database:
            sql += f" database={database}"
        if schema:
            sql += f" schema={schema}"
        tdSql.execute(sql)

    def _mk_influx_real(self, name, database="telegraf"):
        """Create InfluxDB external source pointing to real test InfluxDB."""
        sql = (f"create external source {name} "
               f"type='influxdb' host='{ExtSrcEnv.INFLUX_HOST}' "
               f"port={ExtSrcEnv.INFLUX_PORT} "
               f"user='u' password=''")
        if database:
            sql += f" database={database}"
        sql += (f" options('api_token'='{ExtSrcEnv.INFLUX_TOKEN}',"
                f"'protocol'='flight_sql')")
        tdSql.execute(sql)

    # ------------------------------------------------------------------
    # Assertion helpers
    # ------------------------------------------------------------------

    def _assert_error_not_syntax(self, sql):
        """Execute *sql* expecting an error; assert it is NOT a syntax error.

        Proves the parser accepted the SQL; the failure is expected at
        catalog/connection level (source unreachable, etc.).
        """
        ok = tdSql.query(sql, exit=False)
        if ok is not False:
            return  # query succeeded (possible in future builds)
        errno = (getattr(tdSql, 'errno', None)
                 or getattr(tdSql, 'queryResult', None))
        if (TSDB_CODE_PAR_SYNTAX_ERROR is not None
                and errno == TSDB_CODE_PAR_SYNTAX_ERROR):
            raise AssertionError(
                f"Expected non-syntax error for SQL, "
                f"but got PAR_SYNTAX_ERROR: {sql}"
            )

    # Alias used by some files
    _assert_not_syntax_error = _assert_error_not_syntax

    def _assert_external_context(self, table_name="meters"):
        """Assert current context is external after USE external_source.

        A 1-seg query on *table_name* must NOT return PAR_TABLE_NOT_EXIST
        (which would indicate local resolution) or SYNTAX_ERROR.  Instead
        it should produce a connection/catalog-level error proving the
        context is external.

        Prerequisite: a local table with the same *table_name* must exist
        in the current (local) database, so that PAR_TABLE_NOT_EXIST can
        only mean "resolved locally and not found" vs "resolved externally".
        """
        ok = tdSql.query(f"select * from {table_name} limit 1", exit=False)
        if ok is not False:
            return  # query succeeded — may happen if real external DB is up
        errno = (getattr(tdSql, 'errno', None)
                 or getattr(tdSql, 'queryResult', None))
        if (TSDB_CODE_PAR_TABLE_NOT_EXIST is not None
                and errno == TSDB_CODE_PAR_TABLE_NOT_EXIST):
            raise AssertionError(
                f"After USE external, 1-seg '{table_name}' resolved locally "
                f"(got PAR_TABLE_NOT_EXIST). Expected external resolution error."
            )
        if (TSDB_CODE_PAR_SYNTAX_ERROR is not None
                and errno == TSDB_CODE_PAR_SYNTAX_ERROR):
            raise AssertionError(
                f"After USE external, 1-seg '{table_name}' got SYNTAX_ERROR. "
                f"Expected external resolution error."
            )

    def _assert_local_context(self, db, table_name, expected_val):
        """Assert current context is local *db* by verifying data.

        A 1-seg query on *table_name* returns *expected_val* at row 0 col 1,
        proving USE local_db took effect.
        """
        tdSql.query(f"select * from {table_name} order by ts limit 1")
        tdSql.checkData(0, 1, expected_val)

    def _assert_describe_field(self, source_name, field, expected):
        """DESCRIBE external source and assert *field* equals *expected*.

        Useful for verifying ALTER operations actually took effect.
        """
        tdSql.query(f"describe external source {source_name}")
        desc = {str(r[0]).lower(): str(r[1]) for r in tdSql.queryResult}
        actual = desc.get(field.lower(), "")
        assert actual == str(expected), (
            f"Expected {field}={expected} for source '{source_name}', "
            f"got '{actual}'. Full desc: {desc}"
        )


class FederatedQueryCaseHelper:
    BASE_DB = "fq_case_db"
    SRC_DB = "fq_src_db"

    def __init__(self, case_file: str):
        self.case_dir = os.path.dirname(os.path.abspath(case_file))
        self.in_dir = os.path.join(self.case_dir, "in")
        self.ans_dir = os.path.join(self.case_dir, "ans")
        os.makedirs(self.in_dir, exist_ok=True)
        os.makedirs(self.ans_dir, exist_ok=True)

    def prepare_shared_data(self):
        sqls = [
            f"drop database if exists {self.SRC_DB}",
            f"drop database if exists {self.BASE_DB}",
            f"create database {self.SRC_DB}",
            f"create database {self.BASE_DB}",
            f"use {self.SRC_DB}",
            "create table src_ntb (ts timestamp, c_int int, c_double double, c_bool bool, c_str binary(16))",
            "insert into src_ntb values (1704067200000, 1, 1.5, true, 'alpha')",
            "insert into src_ntb values (1704067260000, 2, 2.5, false, 'beta')",
            "insert into src_ntb values (1704067320000, 3, 3.5, true, 'gamma')",
            "create stable src_stb (ts timestamp, val int, extra float, flag bool) tags(region int, owner nchar(16))",
            "create table src_ctb_a using src_stb tags(1, 'north')",
            "create table src_ctb_b using src_stb tags(2, 'south')",
            "insert into src_ctb_a values (1704067200000, 11, 1.1, true)",
            "insert into src_ctb_a values (1704067260000, 12, 1.2, false)",
            "insert into src_ctb_b values (1704067200000, 21, 2.1, true)",
            "insert into src_ctb_b values (1704067260000, 22, 2.2, true)",
            f"use {self.BASE_DB}",
            "create table local_dim (ts timestamp, sensor_id int, weight int, owner binary(16))",
            "insert into local_dim values (1704067200000, 11, 100, 'team_a')",
            "insert into local_dim values (1704067260000, 21, 200, 'team_b')",
            "create stable vstb_fq (ts timestamp, v_int int, v_float float, v_status bool) tags(vg int) virtual 1",
            (
                "create vtable vctb_fq ("
                "v_int from fq_src_db.src_ctb_a.val, "
                "v_float from fq_src_db.src_ctb_a.extra, "
                "v_status from fq_src_db.src_ctb_a.flag"
                ") using vstb_fq tags(1)"
            ),
            (
                "create vtable vctb_fq_b ("
                "v_int from fq_src_db.src_ctb_b.val, "
                "v_float from fq_src_db.src_ctb_b.extra, "
                "v_status from fq_src_db.src_ctb_b.flag"
                ") using vstb_fq tags(2)"
            ),
            (
                "create vtable vntb_fq ("
                "ts timestamp, "
                "v_int int from fq_src_db.src_ntb.c_int, "
                "v_float double from fq_src_db.src_ntb.c_double, "
                "v_status bool from fq_src_db.src_ntb.c_bool"
                ")"
            ),
        ]
        tdSql.executes(sqls)

    def require_external_source_feature(self):
        if tdSql.query("show external sources", exit=False) is False:
            pytest.skip("external source feature is unavailable in current build")

    def assert_query_result(self, sql: str, expected_rows):
        tdSql.query(sql)
        tdSql.checkRows(len(expected_rows))
        for row_idx, row_data in enumerate(expected_rows):
            for col_idx, expected in enumerate(row_data):
                tdSql.checkData(row_idx, col_idx, expected)

    def assert_error_code(self, sql: str, expected_errno: int):
        tdSql.error(sql, expectedErrno=expected_errno)

    def batch_query_and_check(self, sql_list, expected_result_list):
        tdSql.queryAndCheckResult(sql_list, expected_result_list)

    def compare_sql_files(self, case_name: str, uut_sql_list, ref_sql_list, db_name=None):
        if db_name is None:
            db_name = self.BASE_DB

        uut_sql_file = os.path.join(self.in_dir, f"{case_name}.sql")
        ref_sql_file = os.path.join(self.in_dir, f"{case_name}.ref.sql")
        expected_result_file = ""

        try:
            self._write_sql_file(uut_sql_file, db_name, uut_sql_list)
            self._write_sql_file(ref_sql_file, db_name, ref_sql_list)

            expected_result_file = tdCom.generate_query_result(ref_sql_file, f"{case_name}_ref")
            tdCom.compare_testcase_result(uut_sql_file, expected_result_file, f"{case_name}_uut")
        finally:
            for path in (uut_sql_file, ref_sql_file, expected_result_file):
                if path and os.path.exists(path):
                    os.remove(path)

    @staticmethod
    def _write_sql_file(file_path: str, db_name: str, sql_lines):
        with open(file_path, "w", encoding="utf-8") as fout:
            fout.write(f"use {db_name};\n")
            for sql in sql_lines:
                stmt = sql.strip().rstrip(";") + ";"
                fout.write(stmt + "\n")

    @staticmethod
    def assert_plan_contains(sql: str, keyword: str):
        tdSql.query(f"explain verbose true {sql}")
        for row in tdSql.queryResult:
            for col in row:
                if col is not None and keyword in str(col):
                    return
        tdLog.exit(f"expected keyword '{keyword}' not found in plan")
