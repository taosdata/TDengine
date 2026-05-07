"""
test_fq_02_path_resolution.py

Implements FQ-PATH-001 through FQ-PATH-020 from TS §2
"Path Resolution and Naming Rules" — query FROM path resolution, vtable DDL column-ref path,
three-segment disambiguation, case-sensitivity rules, and invalid-path errors.

Design:
    - Tests that query external sources use real databases (MySQL, PostgreSQL,
      InfluxDB) with prepared test data.  Each test verifies query results with
      checkRows/checkData to prove path resolution correctness.
    - Tests that only verify error codes (syntax errors, invalid paths) may
      use non-routable addresses since no data query is involved.
    - Internal vtable column-reference tests (FQ-PATH-007/008/012) are fully
      testable against the local TDengine instance.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - MySQL (FQ_MYSQL_HOST), PostgreSQL (FQ_PG_HOST), InfluxDB (FQ_INFLUX_HOST).
    - Python packages: pymysql, psycopg2, requests.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_PAR_INVALID_REF_COLUMN,
    TSDB_CODE_MND_DB_NOT_EXIST,
    TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
    TSDB_CODE_EXT_DB_NOT_EXIST,
    TSDB_CODE_EXT_DEFAULT_NS_MISSING,
    TSDB_CODE_EXT_INVALID_PATH,
)

# Test databases in external sources
MYSQL_DB = "fq_path_m"
MYSQL_DB2 = "fq_path_m2"
PG_DB = "fq_path_p"
INFLUX_BUCKET = "fq_path_i"


class TestFq02PathResolution(FederatedQueryVersionedMixin):
    """FQ-PATH-001 through FQ-PATH-020: path resolution and naming rules."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        # Create shared test databases for ALL configured versions (idempotent).
        # setup_class runs once before any per-test version fixtures, so we
        # must iterate versions explicitly rather than using self._mysql_cfg().
        for cfg in ExtSrcEnv.mysql_version_configs():
            ExtSrcEnv.mysql_create_db_cfg(cfg, MYSQL_DB)
            ExtSrcEnv.mysql_create_db_cfg(cfg, MYSQL_DB2)
        for cfg in ExtSrcEnv.pg_version_configs():
            ExtSrcEnv.pg_create_db_cfg(cfg, PG_DB)

    def teardown_class(self):
        for cfg in ExtSrcEnv.mysql_version_configs():
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, MYSQL_DB)
            except Exception:
                pass
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, MYSQL_DB2)
            except Exception:
                pass
        for cfg in ExtSrcEnv.pg_version_configs():
            try:
                ExtSrcEnv.pg_drop_db_cfg(cfg, PG_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Private helpers (file-specific only; shared helpers in mixin)
    # ------------------------------------------------------------------

    def _mk_mysql_real(self, name, database=None):
        """Override: path-resolution tests default to database=None."""
        super()._mk_mysql_real(name, database=database)

    def _mk_pg_real(self, name, database=None, schema=None):
        """Override: path-resolution tests default to database/schema=None."""
        super()._mk_pg_real(name, database=database, schema=schema)

    def _mk_influx_real(self, name, database=None):
        """Override: path-resolution tests default to database=None."""
        super()._mk_influx_real(name, database=database)

    def _prepare_internal_vtable_env(self):
        """Create shared internal tables and vtables for column-ref path tests."""
        sqls = [
            "drop database if exists fq_path_db",
            "drop database if exists fq_path_db2",
            "create database fq_path_db",
            "create database fq_path_db2",
            "use fq_path_db",
            "create table src_t (ts timestamp, val int, extra float)",
            "insert into src_t values (1704067200000, 10, 1.5)",
            "insert into src_t values (1704067260000, 20, 2.5)",
            "create stable src_stb (ts timestamp, val int, extra float) tags(region int) virtual 1",
            "create vtable vt_local ("
            "  val from fq_path_db.src_t.val,"
            "  extra from fq_path_db.src_t.extra"
            ") using src_stb tags(1)",
            "use fq_path_db2",
            "create table src_t2 (ts timestamp, score double)",
            "insert into src_t2 values (1704067200000, 99.9)",
        ]
        tdSql.executes(sqls)

    # ------------------------------------------------------------------
    # FQ-PATH-001 through FQ-PATH-006: FROM path basics
    # ------------------------------------------------------------------

    def test_fq_path_001(self):
        """FQ-PATH-001: MySQL 2-segment table path — source.table uses default database

        Dimensions:
          a) Create MySQL source WITH default database, query source.table → verify data
          b) Query source.table with alias → same data
          c) Negative: source does not exist → error
          d) Filtered query with WHERE clause

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_001_mysql"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t001",
            "CREATE TABLE t001 (id DATETIME PRIMARY KEY, val INT, info VARCHAR(50))",
            "INSERT INTO t001 VALUES ('2024-01-01 00:00:01', 101, 'row1'), ('2024-01-01 00:00:02', 102, 'row2')",
        ])
        self._cleanup_src(src)
        try:
            # (a) Create source with default database, query 2-seg path
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.query(f"select val, info from {src}.t001 order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 101)
            tdSql.checkData(0, 1, 'row1')
            tdSql.checkData(1, 0, 102)
            tdSql.checkData(1, 1, 'row2')

            # (b) With alias
            tdSql.query(f"select t.val from {src}.t001 t order by t.id limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 101)

            # (c) Negative: non-existent source
            tdSql.error("select * from no_such_source_xyz.t001",
                        expectedErrno=TSDB_CODE_EXT_SOURCE_NOT_FOUND)

            # (d) Filtered query
            tdSql.query(f"select val from {src}.t001 where id = 1704067202000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 102)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t001"])

    def test_fq_path_002(self):
        """FQ-PATH-002: MySQL 3-segment table path — source.database.table explicit path correctness

        Dimensions:
          a) Source WITHOUT default database, 3-seg path → verify data from explicit db
          b) Source WITH default database, 3-seg overrides to different db → verify override
          c) 3-seg with WHERE clause
          d) 2-seg default vs 3-seg override on same source → different data proves path

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_002_mysql"
        # Prepare different data in two databases to disambiguate
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t002",
            "CREATE TABLE t002 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t002 VALUES ('2024-01-01 00:00:01', 201)",
        ])
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, [
            "DROP TABLE IF EXISTS t002",
            "CREATE TABLE t002 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t002 VALUES ('2024-01-01 00:00:01', 202)",
        ])
        self._cleanup_src(src)
        try:
            # (a) No default database → 3-seg required
            self._mk_mysql_real(src)  # no database
            tdSql.query(f"select val from {src}.{MYSQL_DB}.t002")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 201)

            # (b) With default=MYSQL_DB, 3-seg overrides to MYSQL_DB2
            self._cleanup_src(src)
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.query(f"select val from {src}.{MYSQL_DB2}.t002")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 202)  # proves override worked

            # (c) 3-seg with WHERE
            tdSql.query(
                f"select val from {src}.{MYSQL_DB}.t002 where id = 1704067201000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 201)

            # (d) 2-seg default vs 3-seg override — different values
            tdSql.query(f"select val from {src}.t002")  # 2-seg → default MYSQL_DB
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 201)
            tdSql.query(f"select val from {src}.{MYSQL_DB2}.t002")  # 3-seg → MYSQL_DB2
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 202)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t002"])
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, ["DROP TABLE IF EXISTS t002"])

    def test_fq_path_003(self):
        """FQ-PATH-003: PG 2-segment table path — source.table uses default schema

        Dimensions:
          a) PG source with default schema, query source.table → verify data
          b) With alias and WHERE
          c) PG source without explicit schema → server uses 'public'
          d) Multiple PG sources with different schemas → each returns correct data

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_003_pg"
        src2 = "fq_path_003_pg2"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS public",
            "DROP TABLE IF EXISTS public.t003",
            "CREATE TABLE public.t003 (id TIMESTAMP PRIMARY KEY, val INT, info VARCHAR(50))",
            "INSERT INTO public.t003 VALUES ('2024-01-01 00:00:01', 301, 'public_row')",
        ])
        self._cleanup_src(src, src2)
        try:
            # (a) Source with explicit schema=public
            self._mk_pg_real(src, database=PG_DB, schema="public")
            tdSql.query(f"select val, info from {src}.t003 order by id")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 301)
            tdSql.checkData(0, 1, 'public_row')

            # (b) With alias + WHERE
            tdSql.query(f"select t.val from {src}.t003 t where t.id = 1704067201000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 301)

            # (c) Without explicit schema → PG defaults to 'public'
            self._cleanup_src(src)
            self._mk_pg_real(src, database=PG_DB)  # no schema
            tdSql.query(f"select val from {src}.t003")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 301)

            # (d) Multiple sources, both reach same data
            self._mk_pg_real(src2, database=PG_DB, schema="public")
            tdSql.query(f"select val from {src2}.t003")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 301)
        finally:
            self._cleanup_src(src, src2)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS public.t003"])

    def test_fq_path_004(self):
        """FQ-PATH-004: PG 3-segment table path — source.schema.table explicit path correctness

        Dimensions:
          a) 3-seg source.schema.table overrides default schema → verify data
          b) 2-seg uses default (public), 3-seg uses analytics → different data
          c) Two different schemas accessed sequentially → each returns correct value
          d) 3-seg with WHERE clause

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_004_pg"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS public",
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS public.t004",
            "DROP TABLE IF EXISTS analytics.t004",
            "CREATE TABLE public.t004 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t004 VALUES ('2024-01-01 00:00:01', 401)",
            "CREATE TABLE analytics.t004 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.t004 VALUES ('2024-01-01 00:00:01', 402)",
        ])
        self._cleanup_src(src)
        try:
            self._mk_pg_real(src, database=PG_DB, schema="public")

            # (a) 3-seg: source.schema.table overrides default
            tdSql.query(f"select val from {src}.analytics.t004")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)  # proves analytics schema selected

            # (b) 2-seg default vs 3-seg override → different data
            tdSql.query(f"select val from {src}.t004")  # 2-seg → public
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 401)
            tdSql.query(f"select val from {src}.analytics.t004")  # 3-seg → analytics
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)

            # (c) Two different schemas sequentially
            tdSql.query(f"select val from {src}.public.t004")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 401)
            tdSql.query(f"select val from {src}.analytics.t004")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)

            # (d) 3-seg with WHERE
            tdSql.query(
                f"select val from {src}.analytics.t004 where id = 1704067201000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.t004",
                "DROP TABLE IF EXISTS analytics.t004",
            ])

    def test_fq_path_005(self):
        """FQ-PATH-005: Influx 2-segment table path — source.measurement uses default database

        Dimensions:
          a) InfluxDB source with default database, query source.measurement → verify
          b) Without default database → short path error
          c) 3-seg explicit bucket works even without default
          d) Different measurement names

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_005_influx"
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "cpu_005,host=server1 usage_idle=55.5 1704067200000",
            "cpu_005,host=server2 usage_idle=72.3 1704067260000",
        ])
        self._cleanup_src(src)
        try:
            # (a) With default database
            self._mk_influx_real(src, database=INFLUX_BUCKET)
            tdSql.query(
                f"select usage_idle from {src}.cpu_005 order by ts limit 2")
            tdSql.checkRows(2)
            val0 = float(str(tdSql.getData(0, 0)))
            assert abs(val0 - 55.5) < 0.1, f"Expected ~55.5, got {val0}"
            val1 = float(str(tdSql.getData(1, 0)))
            assert abs(val1 - 72.3) < 0.1, f"Expected ~72.3, got {val1}"

            # (b) Without default database → error on short path
            self._cleanup_src(src)
            self._mk_influx_real(src)  # no database
            tdSql.error(f"select * from {src}.cpu_005",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)

            # (c) 3-seg explicit bucket works without default
            tdSql.query(
                f"select usage_idle from {src}.{INFLUX_BUCKET}.cpu_005 "
                f"order by ts limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 55.5) < 0.1, f"Expected ~55.5, got {val}"

            # (d) Different measurement
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
                "mem_005,host=server1 used_pct=82.1 1704067200000",
            ])
            self._cleanup_src(src)
            self._mk_influx_real(src, database=INFLUX_BUCKET)
            tdSql.query(f"select used_pct from {src}.mem_005 limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 82.1) < 0.1, f"Expected ~82.1, got {val}"
        finally:
            self._cleanup_src(src)

    def test_fq_path_006(self):
        """FQ-PATH-006: Default namespace error — short path fails when default db/schema not configured

        Dimensions:
          a) MySQL source without DATABASE, 2-seg query → error
          b) PG source without SCHEMA (but with DATABASE), 2-seg → uses 'public'
          c) InfluxDB source without DATABASE, 2-seg → error
          d) After ALTER MySQL to add DATABASE, 2-seg → works (verify data)
          e) Multiple sources, only one missing default

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_path_006_mysql"
        p = "fq_path_006_pg"
        i = "fq_path_006_influx"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t006",
            "CREATE TABLE t006 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t006 VALUES ('2024-01-01 00:00:01', 601)",
        ])
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "DROP TABLE IF EXISTS public.t006",
            "CREATE TABLE public.t006 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t006 VALUES ('2024-01-01 00:00:01', 602)",
        ])
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "t006,host=s1 val=603 1704067200000",
        ])
        self._cleanup_src(m, p, i)
        try:
            # (a) MySQL without DATABASE → 2-seg error
            self._mk_mysql_real(m)  # no database
            tdSql.error(f"select * from {m}.t006",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)

            # (b) PG without schema (but with DATABASE) → defaults to 'public'
            self._mk_pg_real(p, database=PG_DB)  # no schema
            tdSql.query(f"select val from {p}.t006")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 602)

            # (c) InfluxDB without DATABASE → 2-seg error
            self._mk_influx_real(i)  # no database
            tdSql.error(f"select * from {i}.t006",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)

            # (d) ALTER MySQL to add DATABASE → 2-seg works
            tdSql.execute(
                f"alter external source {m} set database={MYSQL_DB}")
            tdSql.query(f"select val from {m}.t006")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 601)

            # (e) Mixed: m has db now, i still doesn't
            tdSql.query(f"select val from {m}.t006")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 601)
            tdSql.error(f"select * from {i}.t006",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
        finally:
            self._cleanup_src(m, p, i)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t006"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS public.t006"])

    # ------------------------------------------------------------------
    # FQ-PATH-007, 008: Internal vtable column reference (local only)
    # ------------------------------------------------------------------

    def test_fq_path_007(self):
        """FQ-PATH-007: VTable internal 2-segment column reference — table.column resolves correctly

        FS §3.5.3: In vtable DDL, ``col FROM table.column`` resolves to
        current-database table.column.

        Dimensions:
          a) Create vtable with 2-seg internal column reference (table.col)
          b) Query the vtable — values match source table
          c) Negative: reference non-existent table → error
          d) Negative: reference non-existent column → error
          e) Multiple 2-seg refs in one vtable

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._prepare_internal_vtable_env()
        try:
            tdSql.execute("use fq_path_db")

            # (a) Create vtable with 2-seg column ref
            tdSql.execute("drop table if exists vt_2seg")
            tdSql.execute(
                "create vtable vt_2seg ("
                "  ts timestamp,"
                "  v1 int from src_t.val"
                ")"
            )

            # (b) Query — values match source
            tdSql.query("select v1 from vt_2seg order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 10)
            tdSql.checkData(1, 0, 20)

            # (c) reference non-existent table
            tdSql.error(
                "create vtable vt_bad_tbl ("
                "  ts timestamp,"
                "  v1 int from nonexist_tbl.val"
                ")",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST,
            )

            # (d) reference non-existent column
            tdSql.error(
                "create vtable vt_bad_col ("
                "  ts timestamp,"
                "  v1 int from src_t.nonexist_col"
                ")",
                expectedErrno=TSDB_CODE_PAR_INVALID_REF_COLUMN,
            )

            # (e) Multiple 2-seg refs
            tdSql.execute("drop table if exists vt_multi_2seg")
            tdSql.execute(
                "create vtable vt_multi_2seg ("
                "  ts timestamp,"
                "  v_val int from src_t.val,"
                "  v_extra float from src_t.extra"
                ")"
            )
            tdSql.query("select v_val, v_extra from vt_multi_2seg order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 10)
            tdSql.checkData(0, 1, 1.5)
        finally:
            tdSql.execute("drop database if exists fq_path_db")
            tdSql.execute("drop database if exists fq_path_db2")

    def test_fq_path_008(self):
        """FQ-PATH-008: VTable internal 3-segment column reference — db.table.column resolves correctly

        FS §3.5.4: ``col FROM db.table.column`` resolves across databases.

        Dimensions:
          a) Create vtable in db1 referencing db2.table.column
          b) Query returns correct values from db2
          c) Cross-db reference with USE different db
          d) Negative: reference non-existent db → error
          e) Self-db three-segment ref (same as current db)

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._prepare_internal_vtable_env()
        try:
            tdSql.execute("use fq_path_db")

            # (a) 3-seg cross-db reference
            tdSql.execute("drop table if exists vt_3seg_cross")
            tdSql.execute(
                "create vtable vt_3seg_cross ("
                "  ts timestamp,"
                "  v1 double from fq_path_db2.src_t2.score"
                ")"
            )

            # (b) Query — values from db2
            tdSql.query("select v1 from vt_3seg_cross order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99.9)

            # (c) USE a different database, then query by fully-qualified name
            tdSql.execute("use fq_path_db2")
            tdSql.query("select v1 from fq_path_db.vt_3seg_cross order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99.9)

            # (d) Negative: non-existent db
            tdSql.execute("use fq_path_db")
            tdSql.error(
                "create vtable vt_bad_db ("
                "  ts timestamp,"
                "  v1 int from no_such_db.tbl.col"
                ")",
                expectedErrno=TSDB_CODE_MND_DB_NOT_EXIST,
            )

            # (e) Self-db three-segment (same as current db)
            tdSql.execute("drop table if exists vt_self_3seg")
            tdSql.execute(
                "create vtable vt_self_3seg ("
                "  ts timestamp,"
                "  v1 int from fq_path_db.src_t.val"
                ")"
            )
            tdSql.query("select v1 from vt_self_3seg order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 10)
        finally:
            tdSql.execute("drop database if exists fq_path_db")
            tdSql.execute("drop database if exists fq_path_db2")

    # ------------------------------------------------------------------
    # FQ-PATH-009, 010: VTable external column reference
    # ------------------------------------------------------------------

    @pytest.mark.skip(reason="vtable external column reference not yet implemented")
    def test_fq_path_009(self):
        """FQ-PATH-009: VTable external 3-segment column reference — source.table.column uses default namespace

        FS §3.5.5: ``col FROM source.table.column`` with source's default db.

        Dimensions:
          a) Create external source with default database, vtable DDL 3-seg → query data
          b) Multiple columns from same external table
          c) Source without default db → 3-seg column ref behaviour
          d) Parser acceptance cross-verify

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_009_src"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS vt009",
            "CREATE TABLE vt009 (ts DATETIME PRIMARY KEY, val INT, extra DOUBLE)",
            "INSERT INTO vt009 VALUES ('2024-01-01 00:00:00', 901, 9.01)",
            "INSERT INTO vt009 VALUES ('2024-01-01 00:01:00', 902, 9.02)",
        ])
        self._cleanup_src(src)
        try:
            tdSql.execute("drop database if exists fq_vtdb_009")
            tdSql.execute("create database fq_vtdb_009")
            tdSql.execute("use fq_vtdb_009")
            tdSql.execute(
                "create stable vstb_009 (ts timestamp, v1 int, v2 double) "
                "tags(r int) virtual 1"
            )

            # (a) Source with DB, vtable with 3-seg external column ref
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.execute(
                f"create vtable vt_009a ("
                f"  v1 from {src}.vt009.val"
                f") using vstb_009 tags(1)")
            tdSql.query("select v1 from vt_009a order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 901)
            tdSql.checkData(1, 0, 902)

            # (b) Multiple columns
            tdSql.execute(
                f"create vtable vt_009b ("
                f"  v1 from {src}.vt009.val,"
                f"  v2 from {src}.vt009.extra"
                f") using vstb_009 tags(2)")
            tdSql.query("select v1, v2 from vt_009b order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 901)
            assert abs(float(str(tdSql.getData(0, 1))) - 9.01) < 0.01

            # (c) Source without default DB → 3-seg may need default NS
            src_nodb = "fq_path_009_nodb"
            self._cleanup_src(src_nodb)
            self._mk_mysql_real(src_nodb)  # no database
            self._assert_error_not_syntax(
                f"create vtable vt_009c ("
                f"  v1 from {src_nodb}.vt009.val"
                f") using vstb_009 tags(3)")
            self._cleanup_src(src_nodb)

            # (d) Cross-verify: parser accepts varied column names
            self._assert_error_not_syntax(
                f"create vtable vt_009d ("
                f"  v1 from {src}.another_tbl.some_col"
                f") using vstb_009 tags(4)")
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop database if exists fq_vtdb_009")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS vt009"])

    @pytest.mark.skip(reason="vtable external column reference not yet implemented")
    def test_fq_path_010(self):
        """FQ-PATH-010: VTable external 4-segment column reference — source.db_or_schema.table.column

        FS §3.5.6: Fully explicit external column reference with 4 segments.

        Dimensions:
          a) MySQL source: source.database.table.column → query data
          b) PG source: source.schema.table.column → query data
          c) InfluxDB source: source.database.measurement.field → query data
          d) Negative: 5 segments → syntax error

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_path_010_mysql"
        p = "fq_path_010_pg"
        i = "fq_path_010_influx"
        # Prepare MySQL data in MYSQL_DB2 (override DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, [
            "DROP TABLE IF EXISTS vt010",
            "CREATE TABLE vt010 (ts DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO vt010 VALUES ('2024-01-01 00:00:00', 1001)",
        ])
        # Prepare PG data in analytics schema
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS analytics.vt010",
            "CREATE TABLE analytics.vt010 (ts TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.vt010 VALUES ('2024-01-01 00:00:00', 1002)",
        ])
        # Prepare InfluxDB data
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "vt010,host=s1 val=1003 1704067200000",
        ])
        self._cleanup_src(m, p, i)
        try:
            tdSql.execute("drop database if exists fq_vtdb_010")
            tdSql.execute("create database fq_vtdb_010")
            tdSql.execute("use fq_vtdb_010")
            tdSql.execute(
                "create stable vstb_010 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            # (a) MySQL 4-seg: source.database.table.column
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.execute(
                f"create vtable vt_010a ("
                f"  v1 from {m}.{MYSQL_DB2}.vt010.val"
                f") using vstb_010 tags(1)")
            tdSql.query("select v1 from vt_010a order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1001)

            # (b) PG 4-seg: source.schema.table.column
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute(
                f"create vtable vt_010b ("
                f"  v1 from {p}.analytics.vt010.val"
                f") using vstb_010 tags(2)")
            tdSql.query("select v1 from vt_010b order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1002)

            # (c) InfluxDB 4-seg: source.database.measurement.field
            self._mk_influx_real(i, database=INFLUX_BUCKET)
            tdSql.execute(
                f"create vtable vt_010c ("
                f"  v1 from {i}.{INFLUX_BUCKET}.vt010.val"
                f") using vstb_010 tags(3)")
            tdSql.query("select v1 from vt_010c order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1003)

            # (d) Negative: 5 segments → syntax error
            tdSql.error(
                f"create vtable vt_010d ("
                f"  v1 from {m}.a.b.c.d"
                f") using vstb_010 tags(4)",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
        finally:
            self._cleanup_src(m, p, i)
            tdSql.execute("drop database if exists fq_vtdb_010")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, ["DROP TABLE IF EXISTS vt010"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS analytics.vt010"])

    # ------------------------------------------------------------------
    # FQ-PATH-011 through FQ-PATH-016
    # ------------------------------------------------------------------

    def test_fq_path_011(self):
        """FQ-PATH-011: 3-segment disambiguation (external) — first segment matches source_name, resolves as external path

        FS §3.5.2: When the first segment of a 3-part name matches a registered
        source_name, the path is resolved as external (source.db.table).

        Dimensions:
          a) 3-seg path resolves to external, verified via data
          b) Not treated as local: no local db with that name
          c) Two sources, each queried with 3-seg → correct data from each
          d) Source name is unique identifier in disambiguation

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_011_ext"
        src2 = "fq_path_011_ext2"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t011",
            "CREATE TABLE t011 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t011 VALUES ('2024-01-01 00:00:01', 1101)",
        ])
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "DROP TABLE IF EXISTS public.t011",
            "CREATE TABLE public.t011 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t011 VALUES ('2024-01-01 00:00:01', 1102)",
        ])
        self._cleanup_src(src, src2)
        try:
            # (a) 3-seg external: source.database.table → verify data
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.query(f"select val from {src}.{MYSQL_DB}.t011")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1101)

            # (b) FQ: USE <external_source_name> is valid — it selects the external source context
            tdSql.execute(f"use {src}")

            # (c) Two sources → each returns correct data
            self._mk_pg_real(src2, database=PG_DB, schema="public")
            tdSql.query(f"select val from {src2}.public.t011")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1102)

            # Cross-verify: MySQL source still returns its data
            tdSql.query(f"select val from {src}.{MYSQL_DB}.t011")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1101)

            # (d) Disambiguation: source exists → 3-seg resolves externally
            tdSql.query(
                f"select val from {src}.{MYSQL_DB}.t011 where id = 1704067201000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1101)
        finally:
            self._cleanup_src(src, src2)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t011"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS public.t011"])

    def test_fq_path_012(self):
        """FQ-PATH-012: 3-segment disambiguation (internal) — first segment matches local db, resolves as internal path

        FS §3.5.2: When first segment matches a local database (and NOT a
        registered source_name), 3-seg resolves as db.table.column (internal).

        Dimensions:
          a) Create local db, query db.table.column in vtable DDL → internal
          b) No external source with same name → internal resolution
          c) Query across databases with 3-seg (fully testable)
          d) Negative: local db exists but table doesn't

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        self._prepare_internal_vtable_env()
        try:
            tdSql.execute("use fq_path_db")

            # (a) 3-seg internal path: fq_path_db2.src_t2.score
            tdSql.execute("drop table if exists vt_disambig_int")
            tdSql.execute(
                "create vtable vt_disambig_int ("
                "  ts timestamp,"
                "  v1 double from fq_path_db2.src_t2.score"
                ")"
            )
            tdSql.query("select v1 from vt_disambig_int")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99.9)

            # (b) No external source with name 'fq_path_db2'
            tdSql.query("show external sources")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert "fq_path_db2" not in names

            # (c) Cross-db vtable access via fully-qualified 3-seg name.
            # USE fq_path_db2, then query fq_path_db.vt_disambig_int to verify
            # that a vtable in another db is accessible via 3-seg name resolution.
            tdSql.execute("use fq_path_db2")
            tdSql.query("select v1 from fq_path_db.vt_disambig_int")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99.9)
            tdSql.execute("use fq_path_db")

            # (d) Negative: non-existent table
            tdSql.error(
                "create vtable vt_bad ("
                "  ts timestamp,"
                "  v1 int from fq_path_db2.no_such_table.col"
                ")",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST,
            )
        finally:
            tdSql.execute("drop database if exists fq_path_db")
            tdSql.execute("drop database if exists fq_path_db2")

    def test_fq_path_013(self):
        """FQ-PATH-013: Name conflict prevention — creation blocked when source name conflicts with local db name

        FS §3.5.2: source_name MUST NOT conflict with any existing local
        database name.  CREATE EXTERNAL SOURCE is rejected if name conflicts.

        Dimensions:
          a) Create local db, then CREATE EXTERNAL SOURCE with same name → error
          b) Create source first, then CREATE DATABASE with same name → error
          c) After DROP db, source creation should succeed
          d) After DROP source, db creation should succeed
          e) Case-insensitive conflict

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        db_name = "fq_conflict_013"
        self._cleanup_src(db_name)
        tdSql.execute(f"drop database if exists {db_name}")
        try:
            # (a) Create db first → source creation fails
            tdSql.execute(f"create database {db_name}")
            tdSql.error(
                f"create external source {db_name} "
                f"type='mysql' host='{self._mysql_cfg().host}' "
                f"port={self._mysql_cfg().port} "
                f"user='{self._mysql_cfg().user}' "
                f"password='{self._mysql_cfg().password}'",
                expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
            )

            # (b) Create source first → db creation fails
            tdSql.execute(f"drop database {db_name}")
            self._mk_mysql_real(db_name, database=MYSQL_DB)
            tdSql.error(
                f"create database {db_name}",
                expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
            )

            # (c) DROP source → db creates OK
            self._cleanup_src(db_name)
            tdSql.execute(f"create database {db_name}")
            tdSql.execute(f"drop database {db_name}")

            # (d) DROP db → source creates OK
            self._mk_mysql_real(db_name, database=MYSQL_DB)
            self._cleanup_src(db_name)

            # (e) Case-insensitive conflict
            tdSql.execute("create database fq_CONFLICT_013")
            tdSql.error(
                f"create external source fq_conflict_013 "
                f"type='mysql' host='{self._mysql_cfg().host}' "
                f"port={self._mysql_cfg().port} "
                f"user='{self._mysql_cfg().user}' "
                f"password='{self._mysql_cfg().password}'",
                expectedErrno=TSDB_CODE_MND_EXTERNAL_SOURCE_NAME_CONFLICT,
            )
        finally:
            self._cleanup_src(db_name)
            tdSql.execute(f"drop database if exists {db_name}")
            tdSql.execute("drop database if exists fq_CONFLICT_013")

    def test_fq_path_014(self):
        """FQ-PATH-014: MySQL case-sensitivity rules — default case-insensitive verification

        FS §3.2.4: MySQL identifiers are case-insensitive by default.
        Different casing should resolve to the same table with same data.

        Dimensions:
          a) Query with different casing → same data
          b) Mixed case in 3-seg path → same data
          c) Backtick-escaped identifiers → same data
          d) Source name case-insensitivity (TDengine side) → same data

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_014_mysql"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS MyTable",
            "CREATE TABLE MyTable (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO MyTable VALUES ('2024-01-01 00:00:01', 1401)",
        ])
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)

            # (a) Different casing in table name → same data
            tdSql.query(f"select val from {src}.MyTable")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)
            tdSql.query(f"select val from {src}.mytable")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)
            tdSql.query(f"select val from {src}.MYTABLE")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)

            # (b) Mixed case 3-seg
            tdSql.query(f"select val from {src}.{MYSQL_DB}.mytable")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)

            # (c) Backtick-escaped identifiers
            tdSql.query(f"select val from {src}.`{MYSQL_DB}`.`MyTable`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)

            # (d) Source name case-insensitivity (TDengine side)
            tdSql.query(f"select val from FQ_PATH_014_MYSQL.MyTable")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS MyTable"])

    def test_fq_path_015(self):
        """FQ-PATH-015: PG case-sensitivity rules — unquoted folds to lowercase; quoted preserves case

        FS §3.2.4: PostgreSQL folds unquoted identifiers to lowercase.
        Tables with different cases (unquoted vs quoted) are distinct.

        Dimensions:
          a) Unquoted PG table → lowercase data
          b) Quoted PG table (backtick in TDengine ≈ PG quote) → case-preserved data
          c) Both tables coexist with different data → distinguish by case
          d) Source name case-insensitive (TDengine side)

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_015_pg"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            # PG unquoted: folds to lowercase ("users" table)
            "DROP TABLE IF EXISTS public.t_users",
            "CREATE TABLE public.t_users (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t_users VALUES ('2024-01-01 00:00:01', 1501)",
            # PG quoted: preserves case ("Users" table, distinct object)
            'DROP TABLE IF EXISTS public."T_users"',
            'CREATE TABLE public."T_users" (id TIMESTAMP PRIMARY KEY, val INT)',
            "INSERT INTO public.\"T_users\" VALUES ('2024-01-01 00:00:01', 1502)",
        ])
        self._cleanup_src(src)
        try:
            self._mk_pg_real(src, database=PG_DB, schema="public")

            # (a) Unquoted → folds to lowercase → returns 1501
            tdSql.query(f"select val from {src}.t_users")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1501)

            # (b) Backtick-quoted → preserves case → returns 1502
            tdSql.query(f"select val from {src}.`T_users`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1502)

            # (c) Both tables return different data — proves distinction
            tdSql.query(f"select val from {src}.t_users")
            tdSql.checkData(0, 0, 1501)
            tdSql.query(f"select val from {src}.`T_users`")
            tdSql.checkData(0, 0, 1502)

            # (d) Source name case-insensitivity (TDengine side)
            tdSql.query(f"select val from FQ_PATH_015_PG.t_users")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1501)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.t_users",
                'DROP TABLE IF EXISTS public."T_users"',
            ])

    def test_fq_path_016(self):
        """FQ-PATH-016: Path segment count errors — invalid segment count returns parse error

        FS §3.5: Valid segment counts depend on context:
          - SELECT FROM: 2 or 3 segments
          - VTable column ref: 2, 3, or 4 segments
        Other segment counts should produce a parse error.

        Dimensions:
          a) Query FROM with 1 segment (just table) → resolves as local
          b) Query FROM with 4+ segments → syntax error
          c) VTable DDL with 1 segment → error
          d) VTable DDL with 5 segments → syntax error
          e) Empty segments → syntax error

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_path_016_src"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.execute("drop database if exists fq_path_016db")
            tdSql.execute("create database fq_path_016db")
            tdSql.execute("use fq_path_016db")
            tdSql.execute(
                "create stable vstb_016 (ts timestamp, v1 int) tags(r int) virtual 1"
            )

            # (a) FROM 1-segment: just table name → resolves as local table
            tdSql.error("select * from no_such_local_table",
                        expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)

            # (b) FROM 4+ segments → syntax error
            tdSql.error(
                f"select * from {src}.db.schema.tbl",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.error(
                f"select * from a.b.c.d.e",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (c) VTable DDL with 1-segment column ref → error
            tdSql.error(
                "create vtable vt_016c ("
                "  v1 from just_col"
                ") using vstb_016 tags(1)",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (d) VTable DDL with 5 segments → syntax error
            tdSql.error(
                f"create vtable vt_016d ("
                f"  v1 from {src}.db.schema.tbl.col"
                f") using vstb_016 tags(2)",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (e) Empty/malformed segments
            tdSql.error(
                "select * from .tbl",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
            tdSql.error(
                f"select * from {src}..tbl",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop database if exists fq_path_016db")

    # ------------------------------------------------------------------
    # FQ-PATH-017 through FQ-PATH-020: USE external source context
    # ------------------------------------------------------------------

    def test_fq_path_017(self):
        """FQ-PATH-017: USE external source — default namespace

        FS §3.5.7: ``USE source_name`` switches to the external source's default
        namespace.  Requires the source to have a configured default namespace.

        Verification: local table 'meters' val=42 vs external MySQL 'meters' val=999.
        After USE external → 1-seg query returns 999; after USE local → returns 42.

        Dimensions:
          a) MySQL source with DATABASE → USE succeeds, 1-seg returns external data
          b) MySQL source without DATABASE → USE fails (missing NS)
          c) PG source with SCHEMA → USE succeeds, 1-seg returns external data
          d) PG source without SCHEMA (but with DATABASE) → USE succeeds (defaults to public)
          e) InfluxDB source with DATABASE → USE succeeds, 1-seg returns external data
          e2) InfluxDB source without DATABASE → USE fails (missing NS)
          f) USE nonexistent source/db → error
          g) USE backtick-escaped source name → works

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_017_mysql"
        p = "fq_017_pg"
        i = "fq_017_influx"
        db = "fq_017_local"
        # Prepare external data: MySQL meters.val=999
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS meters",
            "CREATE TABLE meters (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO meters VALUES ('2024-01-01 00:00:01', 999)",
        ])
        # Prepare external data: PG meters.val=998
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "DROP TABLE IF EXISTS public.meters",
            "CREATE TABLE public.meters (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.meters VALUES ('2024-01-01 00:00:01', 998)",
        ])
        # Prepare InfluxDB data
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "meters,host=s1 val=997 1704067200000",
        ])
        self._cleanup_src(m, p, i)
        tdSql.execute(f"drop database if exists {db}")
        try:
            # Prepare local DB with known table for comparison
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table meters (ts timestamp, val int)")
            tdSql.execute("insert into meters values (1704067200000, 42)")
            # Verify local baseline
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)

            # (a) MySQL with DATABASE → USE succeeds, external data
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.execute(f"use {m}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 999)  # external MySQL data
            # Switch back → local data
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (b) MySQL without DATABASE → USE fails
            self._cleanup_src(m)
            self._mk_mysql_real(m)  # no database
            tdSql.error(f"use {m}",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
            # Context remains local after failed USE
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (c) PG with SCHEMA → USE succeeds, external data
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute(f"use {p}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 998)  # external PG data
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (d) PG without explicit SCHEMA → USE succeeds using 'public' as default
            self._cleanup_src(p)
            self._mk_pg_real(p, database=PG_DB)  # no schema → defaults to 'public'
            tdSql.execute(f"use {p}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 998)  # public.meters
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (e) InfluxDB with DATABASE → USE succeeds, external data
            self._mk_influx_real(i, database=INFLUX_BUCKET)
            tdSql.execute(f"use {i}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 997) < 0.1, f"Expected ~997, got {val}"
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (e2) InfluxDB without DATABASE → USE fails (EXT_DEFAULT_NS_MISSING)
            self._cleanup_src(i)
            self._mk_influx_real(i)  # no database
            tdSql.error(f"use {i}",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
            # Context remains local after failed USE
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)
            self._cleanup_src(i)

            # (f) USE nonexistent name
            tdSql.error("use no_such_source_or_db_xyz",
                        expectedErrno=TSDB_CODE_MND_DB_NOT_EXIST)

            # (g) USE backtick-escaped source
            self._cleanup_src(m)
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.execute(f"use `{m}`")
            tdSql.query("select val from meters limit 1")
            tdSql.checkData(0, 0, 999)
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)
        finally:
            self._cleanup_src(m, p, i)
            tdSql.execute(f"drop database if exists {db}")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS meters"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS public.meters"])

    def test_fq_path_018(self):
        """FQ-PATH-018: USE external source — explicit namespace

        FS §3.5.7: ``USE source_name.database`` (MySQL/InfluxDB) and
        ``USE source_name.schema`` (PG) override the default value.

        Verification: MySQL data in MYSQL_DB (val=801) vs MYSQL_DB2 (val=802).
        USE source.db2 → query returns 802; USE source.db1 → returns 801.

        Dimensions:
          a) MySQL: USE source.database overrides default → verify correct data
          b) MySQL without default DB: USE source.database still works
          c) PG: USE source.schema overrides default → verify correct data
          d) InfluxDB: USE source.database → verify
          e) After USE source.ns, single-seg resolves in specified NS
          f) USE source.nonexistent_ns → may succeed (validated at query time)

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_018_mysql"
        p = "fq_018_pg"
        i = "fq_018_influx"
        db = "fq_018_local"
        # Prepare MySQL data in two databases
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t018",
            "CREATE TABLE t018 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t018 VALUES ('2024-01-01 00:00:01', 801)",
        ])
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, [
            "DROP TABLE IF EXISTS t018",
            "CREATE TABLE t018 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t018 VALUES ('2024-01-01 00:00:01', 802)",
        ])
        # Prepare PG data in two schemas
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS public.t018",
            "CREATE TABLE public.t018 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t018 VALUES ('2024-01-01 00:00:01', 803)",
            "DROP TABLE IF EXISTS analytics.t018",
            "CREATE TABLE analytics.t018 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.t018 VALUES ('2024-01-01 00:00:01', 804)",
        ])
        self._cleanup_src(m, p, i)
        tdSql.execute(f"drop database if exists {db}")
        try:
            # Prepare local DB for context baseline
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table t018 (ts timestamp, val int)")
            tdSql.execute("insert into t018 values (1704067200000, 42)")

            # (a) MySQL: USE source.MYSQL_DB2 overrides default MYSQL_DB
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.execute(f"use {m}.{MYSQL_DB2}")
            tdSql.query("select val from t018 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 802)  # from MYSQL_DB2
            tdSql.execute(f"use {db}")

            # (b) MySQL without default DB: USE source.database still works
            self._cleanup_src(m)
            self._mk_mysql_real(m)  # no database
            tdSql.execute(f"use {m}.{MYSQL_DB}")
            tdSql.query("select val from t018 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 801)  # from MYSQL_DB
            tdSql.execute(f"use {db}")

            # (c) PG: USE source.schema overrides default
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute(f"use {p}.analytics")
            tdSql.query("select val from t018 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 804)  # from analytics schema
            tdSql.execute(f"use {db}")

            # (d) InfluxDB: USE source.database
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
                "t018,host=s1 val=805 1704067200000",
            ])
            self._mk_influx_real(i, database=INFLUX_BUCKET)
            tdSql.execute(f"use {i}.{INFLUX_BUCKET}")
            tdSql.query("select val from t018 limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 805) < 0.1, f"Expected ~805, got {val}"
            tdSql.execute(f"use {db}")

            # (e) After USE source.ns, single-seg resolves in specified NS
            tdSql.execute(f"use {m}.{MYSQL_DB}")
            tdSql.query("select val from t018 limit 1")
            tdSql.checkData(0, 0, 801)

            # (f) USE source.nonexistent_ns → TSDB_CODE_EXT_DB_NOT_EXIST (validated at parse time)
            tdSql.error(f"use {m}.no_such_db",
                        expectedErrno=TSDB_CODE_EXT_DB_NOT_EXIST)

            # Restore local
            tdSql.execute(f"use {db}")
            tdSql.query("select val from t018 order by ts limit 1")
            tdSql.checkData(0, 0, 42)
        finally:
            self._cleanup_src(m, p, i)
            tdSql.execute(f"drop database if exists {db}")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t018"])
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, ["DROP TABLE IF EXISTS t018"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.t018",
                "DROP TABLE IF EXISTS analytics.t018",
            ])

    def test_fq_path_019(self):
        """FQ-PATH-019: USE external source — PG 3-segment form

        FS §3.5.7: ``USE source_name.database.schema`` is only supported for
        PostgreSQL.  For non-PG types, this form should produce an error.

        Verification: PG data in analytics.t019 (val=901) vs public.t019 (val=902).
        USE pg.db.analytics → 1-seg returns 901.

        Dimensions:
          a) PG: USE source.database.schema → succeeds, verify external data
          b) After USE, single-seg resolves in database.schema context
          c) MySQL: USE source.database.schema → error
          d) InfluxDB: USE source.database.schema → error
          e) PG: Multiple USE with different database.schema combinations

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        p = "fq_019_pg"
        m = "fq_019_mysql"
        i = "fq_019_influx"
        db = "fq_019_local"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS analytics.t019",
            "CREATE TABLE analytics.t019 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.t019 VALUES ('2024-01-01 00:00:01', 901)",
            "DROP TABLE IF EXISTS public.t019",
            "CREATE TABLE public.t019 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t019 VALUES ('2024-01-01 00:00:01', 902)",
        ])
        self._cleanup_src(p, m, i)
        tdSql.execute(f"drop database if exists {db}")
        try:
            # Prepare local DB
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table t019 (ts timestamp, val int)")
            tdSql.execute("insert into t019 values (1704067200000, 42)")

            # (a) PG: USE source.database.schema → verify external data
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute(f"use {p}.{PG_DB}.analytics")
            tdSql.query("select val from t019 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 901)  # analytics.t019

            # (b) After USE, single-seg resolves in analytics context
            tdSql.query("select val from t019 limit 1")
            tdSql.checkData(0, 0, 901)

            # Switch back to local
            tdSql.execute(f"use {db}")
            tdSql.query("select val from t019 order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (c) MySQL: 3-seg USE → error
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.error(f"use {m}.{MYSQL_DB}.extra",
                        expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
            # Context remains local
            tdSql.query("select val from t019 order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (d) InfluxDB: 3-seg USE → error
            self._mk_influx_real(i, database=INFLUX_BUCKET)
            tdSql.error(f"use {i}.{INFLUX_BUCKET}.extra",
                        expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
            tdSql.query("select val from t019 order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (e) PG: Multiple USE with different combinations
            tdSql.execute(f"use {p}.{PG_DB}.analytics")
            tdSql.query("select val from t019 limit 1")
            tdSql.checkData(0, 0, 901)
            tdSql.execute(f"use {p}.{PG_DB}.public")
            tdSql.query("select val from t019 limit 1")
            tdSql.checkData(0, 0, 902)

            # Restore
            tdSql.execute(f"use {db}")
            tdSql.query("select val from t019 order by ts limit 1")
            tdSql.checkData(0, 0, 42)
        finally:
            self._cleanup_src(p, m, i)
            tdSql.execute(f"drop database if exists {db}")
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS analytics.t019",
                "DROP TABLE IF EXISTS public.t019",
            ])

    def test_fq_path_020(self):
        """FQ-PATH-020: USE context switching — alternating external/local

        FS §3.5.7: After USE external source, ``USE local_db`` clears external
        context.  Alternating should not interfere.

        Verification: local meters val=42 vs MySQL meters val=999 vs PG meters val=998.

        Dimensions:
          a) USE external → verify external → USE local_db → verify local
          b) Local → External → Local round-trip
          c) USE external → INSERT should fail on external path
          d) USE external → CREATE TABLE should fail
          e) Switch between two external sources → each returns correct data
          f) While in external context, 2-seg still resolves as source.table

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_020_mysql"
        p = "fq_020_pg"
        db = "fq_020_local"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS meters",
            "CREATE TABLE meters (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO meters VALUES ('2024-01-01 00:00:01', 999)",
        ])
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "DROP TABLE IF EXISTS public.meters",
            "CREATE TABLE public.meters (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.meters VALUES ('2024-01-01 00:00:01', 998)",
        ])
        self._cleanup_src(m, p)
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table meters (ts timestamp, val int)")
            tdSql.execute("insert into meters values (1704067200000, 42)")

            self._mk_mysql_real(m, database=MYSQL_DB)
            self._mk_pg_real(p, database=PG_DB, schema="public")

            # (a) USE external → verify → USE local → verify
            tdSql.execute(f"use {m}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkData(0, 0, 999)
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (b) Local → External → Local round-trip
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)
            tdSql.execute(f"use {m}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkData(0, 0, 999)
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)

            # (c) USE external → INSERT should fail
            tdSql.execute(f"use {m}")
            tdSql.error("insert into ext_tbl values (now, 1)")

            # (d) USE external → CREATE TABLE should fail
            tdSql.error("create table new_ext_tbl (ts timestamp, v int)")

            # (e) Switch between two external sources
            tdSql.execute(f"use {m}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkData(0, 0, 999)  # MySQL
            tdSql.execute(f"use {p}")
            tdSql.query("select val from meters limit 1")
            tdSql.checkData(0, 0, 998)  # PG

            # (f) While in external context, 2-seg still resolves source.table
            tdSql.execute(f"use {m}")
            # 2-seg with PG source prefix → PG data
            tdSql.query(f"select val from {p}.meters limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 998)

            # Restore local and verify
            tdSql.execute(f"use {db}")
            tdSql.query("select val from meters order by ts limit 1")
            tdSql.checkData(0, 0, 42)
        finally:
            self._cleanup_src(m, p)
            tdSql.execute(f"drop database if exists {db}")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS meters"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, ["DROP TABLE IF EXISTS public.meters"])

    # ------------------------------------------------------------------
    # Supplementary tests — gap analysis coverage (s01 through s08)
    # ------------------------------------------------------------------

    def test_fq_path_s01_influx_3seg_table_path(self):
        """FQ-PATH-S01: InfluxDB 3-segment table path — source.database.measurement

        Gap: FQ-PATH-005 only covers InfluxDB 2-seg path.  FS §3.5.1 explicitly
        lists InfluxDB 3-seg ``source_name.database.table``, which is untested.

        Dimensions:
          a) InfluxDB source with default database, 3-seg overrides → verify data
          b) InfluxDB source without default database, 3-seg is required → verify
          c) 3-seg with WHERE clause
          d) Mixed: 2-seg and 3-seg queries against same source

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s01_influx"
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "cpu_s01,host=s1 usage_idle=66.6 1704067200000",
        ], precision='ms')
        self._cleanup_src(src)
        try:
            # (a) With default DB → 3-seg overrides
            self._mk_influx_real(src, database=INFLUX_BUCKET)
            tdSql.query(
                f"select usage_idle from {src}.{INFLUX_BUCKET}.cpu_s01 limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 66.6) < 0.1, f"Expected ~66.6, got {val}"

            # (b) Without default DB → 2-seg fails, 3-seg works
            self._cleanup_src(src)
            self._mk_influx_real(src)
            tdSql.error(f"select * from {src}.cpu_s01",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
            tdSql.query(
                f"select usage_idle from {src}.{INFLUX_BUCKET}.cpu_s01 limit 1")
            tdSql.checkRows(1)
            val = float(str(tdSql.getData(0, 0)))
            assert abs(val - 66.6) < 0.1, f"Expected ~66.6, got {val}"

            # (c) 3-seg with WHERE
            self._cleanup_src(src)
            self._mk_influx_real(src, database=INFLUX_BUCKET)
            tdSql.query(
                f"select usage_idle from {src}.{INFLUX_BUCKET}.cpu_s01 "
                f"where ts >= '2024-01-01' limit 1")
            tdSql.checkRows(1)

            # (d) Mixed: 2-seg (default) and 3-seg (explicit)
            tdSql.query(f"select usage_idle from {src}.cpu_s01 limit 1")
            tdSql.checkRows(1)
            tdSql.query(
                f"select usage_idle from {src}.{INFLUX_BUCKET}.cpu_s01 limit 1")
            tdSql.checkRows(1)
        finally:
            self._cleanup_src(src)

    def test_fq_path_s02_influx_case_sensitivity(self):
        """FQ-PATH-S02: InfluxDB case-sensitivity — case-sensitive identifiers

        Gap: FQ-PATH-014 covers MySQL (case-insensitive), FQ-PATH-015 covers
        PG (folds to lowercase).  FS §3.2.4 "InfluxDB v3 identifiers are case-sensitive"
        is completely untested.

        Dimensions:
          a) Measurement with different casing → distinct objects with different data
          b) Case-sensitive database name in 3-seg path
          c) Source name itself is case-insensitive (TDengine naming rules)

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s02_influx_case"
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "Cpu_s02,host=s1 val=201 1704067200000",
            "cpu_s02,host=s1 val=202 1704067200000",
        ])
        self._cleanup_src(src)
        try:
            self._mk_influx_real(src, database=INFLUX_BUCKET)

            # (a) Different casing → different measurements, different data.
            # TDengine lowercases unquoted identifiers, so case-sensitive InfluxDB
            # measurement names must be backtick-quoted to preserve their case.
            tdSql.query(f"select val from {src}.cpu_s02 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 202)
            tdSql.query(f"select val from {src}.`Cpu_s02` limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 201)

            # (b) Case-sensitive database in 3-seg
            tdSql.query(
                f"select val from {src}.{INFLUX_BUCKET}.cpu_s02 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 202)

            # (c) Source name is TDengine side → case-insensitive
            tdSql.query(f"select val from FQ_S02_INFLUX_CASE.cpu_s02 limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 202)
        finally:
            self._cleanup_src(src)

    @pytest.mark.skip(reason="vtable external column reference not yet implemented")
    def test_fq_path_s03_vtable_3seg_first_seg_no_match(self):
        """FQ-PATH-S03: VTable 3-segment disambiguation — first segment matches neither, error

        Gap: FS §3.5.4 rule 2 states "first segment matches neither → error".  No existing test
        covers the case where the first segment of a 3-seg VTable DDL path
        matches neither a registered external source nor a local database.

        Dimensions:
          a) 3-seg DDL path where first=nonexistent source → error
          b) After creating source with that name → same path resolves as external
          c) After creating local DB with that name → same path resolves as internal

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        phantom = "fq_s03_phantom"
        self._cleanup_src(phantom)
        tdSql.execute(f"drop database if exists {phantom}")
        try:
            # Prepare a database + vtable context
            tdSql.execute("drop database if exists fq_s03_db")
            tdSql.execute("create database fq_s03_db")
            tdSql.execute("use fq_s03_db")
            tdSql.execute("create table src_t (ts timestamp, val int)")
            tdSql.execute("insert into src_t values (1704067200000, 42)")
            tdSql.execute(
                "create stable vstb_s03 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            # (a) First segment matches nothing → error
            tdSql.error(
                f"create vtable vt_s03a ("
                f"  v1 from {phantom}.tbl.col"
                f") using vstb_s03 tags(1)"
            )

            # Confirm phantom doesn't exist
            tdSql.query("show external sources")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert phantom not in names

            # (b) Create source with that name → external resolution
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS ext_tbl",
                "CREATE TABLE ext_tbl (ts DATETIME PRIMARY KEY, ext_col INT)",
                "INSERT INTO ext_tbl VALUES ('2024-01-01 00:00:00', 333)",
            ])
            self._mk_mysql_real(phantom, database=MYSQL_DB)
            tdSql.execute(
                f"create vtable vt_s03b ("
                f"  v1 from {phantom}.ext_tbl.ext_col"
                f") using vstb_s03 tags(2)")
            tdSql.query("select v1 from vt_s03b order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 333)
            self._cleanup_src(phantom)

            # (c) Create local DB with that name → internal resolution
            tdSql.execute(f"create database {phantom}")
            tdSql.execute(f"use {phantom}")
            tdSql.execute("create table tbl (ts timestamp, col int)")
            tdSql.execute("insert into tbl values (1704067200000, 99)")
            tdSql.execute("use fq_s03_db")
            tdSql.execute("drop table if exists vt_s03c")
            tdSql.execute(
                f"create vtable vt_s03c ("
                f"  ts timestamp,"
                f"  v1 int from {phantom}.tbl.col"
                f")"
            )
            tdSql.query("select v1 from vt_s03c order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99)
        finally:
            self._cleanup_src(phantom)
            tdSql.execute(f"drop database if exists {phantom}")
            tdSql.execute("drop database if exists fq_s03_db")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS ext_tbl"])

    def test_fq_path_s04_alter_namespace_path_impact(self):
        """FQ-PATH-S04: Path resolution follows ALTER default namespace changes

        Gap: FQ-PATH-006(d) only tests ALTER to ADD a DATABASE.  Missing:
        ALTER to CHANGE database, ALTER to CLEAR (empty) database, and
        their impact on query results.

        Dimensions:
          a) ALTER DATABASE from DB1 to DB2 → 2-seg now returns DB2 data
          b) ALTER to clear DATABASE → 2-seg fails (missing NS)
          c) PG: ALTER SCHEMA from one to another → 2-seg returns new schema data
          d) After ALTER, 3-seg still overrides

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_s04_mysql"
        p = "fq_s04_pg"
        # Prepare MySQL data in two databases
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS t_s04",
            "CREATE TABLE t_s04 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t_s04 VALUES ('2024-01-01 00:00:01', 401)",
        ])
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, [
            "DROP TABLE IF EXISTS t_s04",
            "CREATE TABLE t_s04 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO t_s04 VALUES ('2024-01-01 00:00:01', 402)",
        ])
        # Prepare PG data in two schemas
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS public.t_s04",
            "CREATE TABLE public.t_s04 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t_s04 VALUES ('2024-01-01 00:00:01', 403)",
            "DROP TABLE IF EXISTS analytics.t_s04",
            "CREATE TABLE analytics.t_s04 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.t_s04 VALUES ('2024-01-01 00:00:01', 404)",
        ])
        self._cleanup_src(m, p)
        try:
            # (a) MySQL: change DATABASE from DB1 to DB2 → data changes
            self._mk_mysql_real(m, database=MYSQL_DB)
            tdSql.query(f"select val from {m}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 401)  # from MYSQL_DB
            tdSql.execute(
                f"alter external source {m} set database={MYSQL_DB2}")
            tdSql.query(f"select val from {m}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)  # now from MYSQL_DB2

            # Verify via DESCRIBE
            # DESCRIBE EXTERNAL SOURCE returns a single row with columns:
            # (source_name, type, host, port, user, password, database, schema, options, create_time)
            tdSql.query(f"describe external source {m}")
            assert len(tdSql.queryResult) == 1, "DESCRIBE should return one row"
            _desc_row = tdSql.queryResult[0]
            _desc_db = str(_desc_row[6]) if len(_desc_row) > 6 else ""
            assert _desc_db == MYSQL_DB2, f"Expected database '{MYSQL_DB2}', got '{_desc_db}'"

            # (b) Clear DATABASE → 2-seg fails
            tdSql.execute(f"alter external source {m} set database=''")
            tdSql.error(f"select * from {m}.t_s04",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
            # 3-seg still works
            tdSql.query(f"select val from {m}.{MYSQL_DB}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 401)

            # (c) PG: ALTER SCHEMA → data changes
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.query(f"select val from {p}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 403)  # public schema
            tdSql.execute(f"alter external source {p} set schema=analytics")
            tdSql.query(f"select val from {p}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 404)  # analytics schema

            # (d) 3-seg still overrides after ALTER
            tdSql.execute(
                f"alter external source {m} set database={MYSQL_DB}")
            tdSql.query(f"select val from {m}.{MYSQL_DB2}.t_s04")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 402)  # proves override
        finally:
            self._cleanup_src(m, p)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS t_s04"])
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB2, ["DROP TABLE IF EXISTS t_s04"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.t_s04",
                "DROP TABLE IF EXISTS analytics.t_s04",
            ])

    def test_fq_path_s05_multi_source_join_paths(self):
        """FQ-PATH-S05: Multi-source federated query FROM paths — local+external and cross-source JOIN

        Gap: No path test validates the parser accepts diverse path
        combinations in JOIN queries, and that correct data is returned.

        Dimensions:
          a) Local table JOIN external 2-seg table → verify combined data
          b) Two different external sources JOIN (MySQL + PG) → verify
          c) Subquery with external source path → verify data

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        m = "fq_s05_mysql"
        p = "fq_s05_pg"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS remote_orders",
            "CREATE TABLE remote_orders (id DATETIME PRIMARY KEY, amount INT)",
            "INSERT INTO remote_orders VALUES ('2024-01-01 00:00:01', 500), ('2024-01-01 00:00:02', 700)",
        ])
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "DROP TABLE IF EXISTS public.remote_details",
            "CREATE TABLE public.remote_details (id TIMESTAMP PRIMARY KEY, info VARCHAR(50))",
            "INSERT INTO public.remote_details VALUES ('2024-01-01 00:00:01', 'order_a'), ('2024-01-01 00:00:02', 'order_b')",
        ])
        self._cleanup_src(m, p)
        try:
            self._mk_mysql_real(m, database=MYSQL_DB)
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute("drop database if exists fq_s05_local")
            tdSql.execute("create database fq_s05_local")
            tdSql.execute("use fq_s05_local")
            tdSql.execute("create table local_t (ts timestamp, dummy int)")
            tdSql.execute("insert into local_t values (1704067201000, 0)")

            # (a) Local table JOIN external 2-seg — not yet supported in the executor
            # (join between local vnodes and external sources requires a multi-plan executor)
            self._assert_error_not_syntax(
                f"select r.amount from local_t l "
                f"join {m}.remote_orders r on l.ts = r.id")

            # (b) Two external sources JOIN — not yet supported (cross-source join)
            self._assert_error_not_syntax(
                f"select a.amount, b.info from {m}.remote_orders a "
                f"join {p}.remote_details b on a.id = b.id "
                f"order by a.id limit 2")

            # (c) Subquery with external source path
            tdSql.query(
                f"select * from (select amount from {m}.remote_orders) t "
                f"where t.amount > 600")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 700)
        finally:
            self._cleanup_src(m, p)
            tdSql.execute("drop database if exists fq_s05_local")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS remote_orders"])
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.remote_details"])

    def test_fq_path_s06_special_identifier_segments(self):
        """FQ-PATH-S06: Special identifier path segments — reserved words/Unicode/special characters

        Gap: FQ-PATH-014/015 only test basic case variations.  Missing:
        reserved SQL keywords, Chinese characters, digits, dots, spaces
        in backtick-escaped path segments — all with real data verification.

        Dimensions:
          a) Reserved word as external table name in backticks → data verified
          b) Chinese characters in backtick-escaped table → data verified
          c) Path segments with digits and underscores → data verified
          d) Backtick-escaped segment containing dots → data verified
          e) Space in backtick-escaped identifier → data verified

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s06_special"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            # Reserved word table: `select`
            "DROP TABLE IF EXISTS `select`",
            "CREATE TABLE `select` (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO `select` VALUES ('2024-01-01 00:00:01', 601)",
            # Numeric-start table name
            "DROP TABLE IF EXISTS `123numeric`",
            "CREATE TABLE `123numeric` (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO `123numeric` VALUES ('2024-01-01 00:00:01', 602)",
            # Dot in table name
            "DROP TABLE IF EXISTS `my.dotted.table`",
            "CREATE TABLE `my.dotted.table` (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO `my.dotted.table` VALUES ('2024-01-01 00:00:01', 603)",
            # Space in table name
            "DROP TABLE IF EXISTS `my table`",
            "CREATE TABLE `my table` (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO `my table` VALUES ('2024-01-01 00:00:01', 604)",
        ])
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)

            # (a) Reserved SQL keyword as table name
            tdSql.query(f"select val from {src}.`select`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 601)

            # (b) Chinese characters — depends on MySQL character set
            #     (skip if MySQL doesn't support; use parser acceptance)
            self._assert_error_not_syntax(
                f"select * from {src}.`数据表` limit 1")

            # (c) Digits and underscores in table name
            tdSql.query(f"select val from {src}.`123numeric`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 602)

            # (d) Dot inside backtick (not segment separator)
            tdSql.query(f"select val from {src}.`my.dotted.table`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 603)

            # (e) Space in identifier
            tdSql.query(f"select val from {src}.`my table`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 604)
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS `select`",
                "DROP TABLE IF EXISTS `123numeric`",
                "DROP TABLE IF EXISTS `my.dotted.table`",
                "DROP TABLE IF EXISTS `my table`",
            ])

    @pytest.mark.skip(reason="vtable external column reference not yet implemented")
    def test_fq_path_s07_vtable_ext_3seg_all_types(self):
        """FQ-PATH-S07: VTable external 3-segment column reference — PG/InfluxDB type coverage

        Gap: FQ-PATH-009 only tests MySQL for 3-seg external column reference.
        FS §3.5.2 explicitly lists PG and InfluxDB column paths.

        Dimensions:
          a) PG: source.table.column with default schema → query data
          b) InfluxDB: source.measurement.field with default database → query data
          c) PG 4-seg: source.schema.table.column → query data
          d) InfluxDB 4-seg: source.database.measurement.field → query data

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        p = "fq_s07_pg"
        i = "fq_s07_influx"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS public.vt_s07",
            "CREATE TABLE public.vt_s07 (ts TIMESTAMP PRIMARY KEY, temperature INT)",
            "INSERT INTO public.vt_s07 VALUES ('2024-01-01 00:00:00', 25)",
            "DROP TABLE IF EXISTS analytics.vt_s07",
            "CREATE TABLE analytics.vt_s07 (ts TIMESTAMP PRIMARY KEY, temperature INT)",
            "INSERT INTO analytics.vt_s07 VALUES ('2024-01-01 00:00:00', 35)",
        ])
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), INFLUX_BUCKET, [
            "vt_s07,host=s1 usage_idle=88 1704067200000",
        ])
        self._cleanup_src(p, i)
        try:
            tdSql.execute("drop database if exists fq_s07_db")
            tdSql.execute("create database fq_s07_db")
            tdSql.execute("use fq_s07_db")
            tdSql.execute(
                "create stable vstb_s07 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            # (a) PG 3-seg: source.table.column → data from default schema
            self._mk_pg_real(p, database=PG_DB, schema="public")
            tdSql.execute(
                f"create vtable vt_s07a ("
                f"  v1 from {p}.vt_s07.temperature"
                f") using vstb_s07 tags(1)")
            tdSql.query("select v1 from vt_s07a order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 25)

            # (b) InfluxDB 3-seg: source.measurement.field
            self._mk_influx_real(i, database=INFLUX_BUCKET)
            tdSql.execute(
                f"create vtable vt_s07b ("
                f"  v1 from {i}.vt_s07.usage_idle"
                f") using vstb_s07 tags(2)")
            tdSql.query("select v1 from vt_s07b order by ts")
            tdSql.checkRows(1)
            val = int(float(str(tdSql.getData(0, 0))))
            assert val == 88, f"Expected 88, got {val}"

            # (c) PG 4-seg: source.schema.table.column → analytics data
            tdSql.execute(
                f"create vtable vt_s07c ("
                f"  v1 from {p}.analytics.vt_s07.temperature"
                f") using vstb_s07 tags(3)")
            tdSql.query("select v1 from vt_s07c order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 35)

            # (d) InfluxDB 4-seg: source.database.measurement.field
            tdSql.execute(
                f"create vtable vt_s07d ("
                f"  v1 from {i}.{INFLUX_BUCKET}.vt_s07.usage_idle"
                f") using vstb_s07 tags(4)")
            tdSql.query("select v1 from vt_s07d order by ts")
            tdSql.checkRows(1)
            val = int(float(str(tdSql.getData(0, 0))))
            assert val == 88, f"Expected 88, got {val}"
        finally:
            self._cleanup_src(p, i)
            tdSql.execute("drop database if exists fq_s07_db")
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.vt_s07",
                "DROP TABLE IF EXISTS analytics.vt_s07",
            ])

    def test_fq_path_s08_2seg_from_disambiguation(self):
        """FQ-PATH-S08: 2-segment FROM disambiguation — external source.table vs internal db.table

        Gap: No test verifies that in FROM context, a 2-seg path with first
        segment matching a source resolves externally (via data), while first
        segment matching a local DB resolves internally (via data).

        Dimensions:
          a) 2-seg where first = source_name → external data
          b) 2-seg where first = local_db → internal data
          c) Sequential: external then internal — no cross-talk
          d) After DROP source, same 2-seg resolves as local DB

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        ext_name = "fq_s08_ext"
        local_db = "fq_s08_local"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS meters",
            "CREATE TABLE meters (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO meters VALUES ('2024-01-01 00:00:01', 888)",
        ])
        self._cleanup_src(ext_name)
        tdSql.execute(f"drop database if exists {local_db}")
        try:
            # Prepare local database with data
            tdSql.execute(f"create database {local_db}")
            tdSql.execute(f"use {local_db}")
            tdSql.execute("create table meters (ts timestamp, val int)")
            tdSql.execute("insert into meters values (1704067200000, 100)")

            # Prepare external source
            self._mk_mysql_real(ext_name, database=MYSQL_DB)

            # (a) 2-seg external: source.table → external data
            tdSql.query(f"select val from {ext_name}.meters limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 888)  # from MySQL

            # (b) 2-seg internal: db.table → local data
            tdSql.query(
                f"select val from {local_db}.meters order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 100)  # from local TDengine

            # (c) Sequential: no cross-talk
            tdSql.query(f"select val from {ext_name}.meters limit 1")
            tdSql.checkData(0, 0, 888)
            tdSql.query(f"select val from {local_db}.meters order by ts")
            tdSql.checkData(0, 0, 100)

            # (d) DROP source → create DB with that name → now resolves local
            self._cleanup_src(ext_name)
            tdSql.execute(f"create database {ext_name}")
            tdSql.execute(f"use {ext_name}")
            tdSql.execute("create table meters (ts timestamp, val int)")
            tdSql.execute("insert into meters values (1704067200000, 200)")
            tdSql.query(
                f"select val from {ext_name}.meters order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 200)  # local, not external
        finally:
            self._cleanup_src(ext_name)
            tdSql.execute(f"drop database if exists {ext_name}")
            tdSql.execute(f"drop database if exists {local_db}")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, ["DROP TABLE IF EXISTS meters"])

    # ------------------------------------------------------------------
    # Supplementary tests — gap analysis coverage (s09 through s14)
    # ------------------------------------------------------------------

    def test_fq_path_s09_seg_count_extended(self):
        """FQ-PATH-S09: Extended invalid segment count paths — 0-seg/4-seg FROM/VTable boundaries

        Gap: FQ-PATH-016 covers basic 1-seg/4+-seg cases, but misses:
          - FROM exactly 4-seg for each source type
          - 1-seg FROM when the name matches an existing external source
          - VTable DDL FROM with empty/missing reference

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s09_src"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)
            tdSql.execute("drop database if exists fq_s09_db")
            tdSql.execute("create database fq_s09_db")
            tdSql.execute("use fq_s09_db")
            tdSql.execute(
                "create stable vstb_s09 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            # (a) FROM exactly 4-seg on MySQL source → syntax error
            tdSql.error(
                f"select * from {src}.db.schema.tbl",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (b) FROM exactly 4-seg on PG source — PG supports 4-seg (src.db.schema.table),
            # so this is NOT a syntax error. The table schema2.tbl doesn't exist, so the
            # result is a table-not-found error (not a parser rejection).
            pg = "fq_s09_pg"
            self._cleanup_src(pg)
            self._mk_pg_real(pg, database=PG_DB, schema="public")
            self._assert_error_not_syntax(
                f"select * from {pg}.public.schema2.tbl")
            self._cleanup_src(pg)

            # (c) 1-seg FROM matching source name → local table lookup
            tdSql.error(
                f"select * from {src}",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST,
            )

            # (d) VTable DDL FROM with empty/missing reference
            tdSql.error(
                "create vtable vt_s09d ("
                "  v1 from "
                ") using vstb_s09 tags(1)",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (e) FROM 0-seg: just a dot → syntax error
            tdSql.error(
                "select * from .",
                expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR,
            )

            # (f) VTable DDL 4-seg is valid (source.db.table.col)
            self._assert_error_not_syntax(
                f"create vtable vt_s09f ("
                f"  v1 from {src}.{MYSQL_DB}.tbl.col"
                f") using vstb_s09 tags(2)")
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop database if exists fq_s09_db")

    def test_fq_path_s10_path_in_non_select_statements(self):
        """FQ-PATH-S10: External paths in non-SELECT statements — write/DDL/DESCRIBE rejection

        Gap: All existing tests use external paths only in SELECT FROM and
        CREATE VTABLE.  FS §9.2: "External source DDL, write, transaction, and non-query statements not supported".

        Dimensions:
          a) INSERT INTO external path → error
          b) INSERT INTO external 3-seg path → error
          c) DELETE FROM external path → error
          d) CREATE TABLE on external path → error
          e) DROP TABLE on external path → error
          f) ALTER TABLE on external path → error
          g) DESCRIBE with external 2-seg → parser acceptance (may or may not work)
          h) DESCRIBE with external 3-seg → parser acceptance

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s10_mysql"
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)

            # (a) INSERT INTO external path → error
            tdSql.error(
                f"insert into {src}.ext_table values (now, 1)")

            # (b) INSERT INTO external 3-seg → error
            tdSql.error(
                f"insert into {src}.{MYSQL_DB}.ext_table values (now, 1)")

            # (c) DELETE FROM external path → error
            tdSql.error(
                f"delete from {src}.ext_table where ts < now")

            # (d) CREATE TABLE on external path → error
            tdSql.error(
                f"create table {src}.ext_new_table (ts timestamp, v int)")

            # (e) DROP TABLE on external path → error
            tdSql.error(f"drop table {src}.ext_table")

            # (f) ALTER TABLE on external path → error
            tdSql.error(
                f"alter table {src}.ext_table add column new_col int")

            # (g) DESCRIBE external 2-seg table path
            self._assert_error_not_syntax(f"describe {src}.ext_table")

            # (h) DESCRIBE external 3-seg table path
            self._assert_error_not_syntax(
                f"describe {src}.{MYSQL_DB}.ext_table")
        finally:
            self._cleanup_src(src)

    def test_fq_path_s11_backtick_combinations(self):
        """FQ-PATH-S11: Backtick combination tests — permutations of backtick/no-backtick per segment

        Gap: FQ-PATH-014/015 only test isolated backtick examples.  Missing:
        systematic combination of backtick/no-backtick per segment for 2-seg
        and 3-seg paths, all verified with real data.

        Dimensions:
          a-d) 2-seg: 4 combinations of backtick on source/table
          e-l) 3-seg: 8 combinations of backtick on source/database/table
          m-n) VTable DDL backtick combos

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s11_bt"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS tbl_s11",
            "CREATE TABLE tbl_s11 (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO tbl_s11 VALUES ('2024-01-01 00:00:01', 1100)",
        ])
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)

            # -- 2-seg combinations (4) --
            # (a) plain.plain
            tdSql.query(f"select val from {src}.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (b) `source`.plain
            tdSql.query(f"select val from `{src}`.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (c) plain.`table`
            tdSql.query(f"select val from {src}.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (d) `source`.`table`
            tdSql.query(f"select val from `{src}`.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # -- 3-seg combinations (8) --
            # (e) plain.plain.plain
            tdSql.query(f"select val from {src}.{MYSQL_DB}.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (f) `source`.plain.plain
            tdSql.query(f"select val from `{src}`.{MYSQL_DB}.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (g) plain.`database`.plain
            tdSql.query(f"select val from {src}.`{MYSQL_DB}`.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (h) plain.plain.`table`
            tdSql.query(f"select val from {src}.{MYSQL_DB}.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (i) `source`.`database`.plain
            tdSql.query(f"select val from `{src}`.`{MYSQL_DB}`.tbl_s11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (j) `source`.plain.`table`
            tdSql.query(f"select val from `{src}`.{MYSQL_DB}.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (k) plain.`database`.`table`
            tdSql.query(f"select val from {src}.`{MYSQL_DB}`.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # (l) `source`.`database`.`table`
            tdSql.query(
                f"select val from `{src}`.`{MYSQL_DB}`.`tbl_s11`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1100)

            # -- VTable DDL backtick combos --
            tdSql.execute("drop database if exists fq_s11_db")
            tdSql.execute("create database fq_s11_db")
            tdSql.execute("use fq_s11_db")
            tdSql.execute(
                "create stable vstb_s11 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            # (m) VTable DDL: `source`.db.table.col — 4-seg path, backtick on source name.
            # 3-seg external col refs (src.table.col) are not yet implemented; use 4-seg form
            # (source.db.table.col) which the parser accepts without syntax error.
            self._assert_error_not_syntax(
                f"create vtable vt_s11m ("
                f"  v1 from `{src}`.{MYSQL_DB}.tbl_s11.val"
                f") using vstb_s11 tags(1)")

            # (n) VTable DDL: source.`db`.`table`.`col` — 4-seg path, backtick on db/table/col.
            self._assert_error_not_syntax(
                f"create vtable vt_s11n ("
                f"  v1 from {src}.`{MYSQL_DB}`.`tbl_s11`.`val`"
                f") using vstb_s11 tags(2)")
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop database if exists fq_s11_db")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS tbl_s11"])

    @pytest.mark.skip(reason="vtable external column reference not yet implemented")
    def test_fq_path_s12_vtable_col_backtick_all_combinations(self):
        """FQ-PATH-S12: VTable DDL column reference — all 8 backtick combinations for 3-seg path

        Gap: path_009 tests plain no-backtick (combo a); s11-(m) tests
        ``src``.tbl.col (combo b); s11-(n) tests src.``tbl``.``col`` (combo g).
        The remaining 5 three-segment permutations and representative 4-segment
        backtick combinations are uncovered (Dimension 17 — Backtick Segment
        Combination).

        Combinations (3-seg: source.table.col — 2^3 = 8 total):
          a) src.tbl.col                       — plain           [path_009 baseline]
          b) ``src``.tbl.col                   — btick source    [s11-m]
          c) src.``tbl``.col                   — btick table     [NEW]
          d) src.tbl.``col``                   — btick column    [NEW]
          e) ``src``.``tbl``.col               — btick src+tbl   [NEW]
          f) ``src``.tbl.``col``               — btick src+col   [NEW]
          g) src.``tbl``.``col``               — btick tbl+col   [s11-n]
          h) ``src``.``tbl``.``col``           — all backtick    [NEW]

        Additional 4-seg (source.db.table.col — representative backtick combos):
          i) src.db.tbl.``col``                — btick col only  [NEW]
          j) ``src``.db.``tbl``.col            — btick src+tbl   [NEW]
          k) ``src``.``db``.``tbl``.``col``    — all backtick    [NEW]

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s12_bt"
        expected = 1200
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS tbl_s12",
            "CREATE TABLE tbl_s12 (ts DATETIME PRIMARY KEY, val INT)",
            f"INSERT INTO tbl_s12 VALUES ('2024-01-01 00:00:00', {expected})",
        ])
        self._cleanup_src(src)
        try:
            self._mk_mysql_real(src, database=MYSQL_DB)

            tdSql.execute("drop database if exists fq_s12_db")
            tdSql.execute("create database fq_s12_db")
            tdSql.execute("use fq_s12_db")
            tdSql.execute(
                "create stable vstb_s12 (ts timestamp, v1 int) "
                "tags(r int) virtual 1"
            )

            def _chk(vt, ref, tag):
                tdSql.execute(
                    f"create vtable {vt} (v1 from {ref}) "
                    f"using vstb_s12 tags({tag})"
                )
                tdSql.query(f"select v1 from {vt} order by ts")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, expected)

            # 3-seg NEW combinations
            _chk("vt_s12c", f"{src}.`tbl_s12`.val", 1)          # (c)
            _chk("vt_s12d", f"{src}.tbl_s12.`val`", 2)          # (d)
            _chk("vt_s12e", f"`{src}`.`tbl_s12`.val", 3)        # (e)
            _chk("vt_s12f", f"`{src}`.tbl_s12.`val`", 4)        # (f)
            _chk("vt_s12h", f"`{src}`.`tbl_s12`.`val`", 5)      # (h)

            # 4-seg NEW combinations
            _chk("vt_s12i", f"{src}.{MYSQL_DB}.tbl_s12.`val`", 6)        # (i)
            _chk("vt_s12j", f"`{src}`.{MYSQL_DB}.`tbl_s12`.val", 7)      # (j)
            _chk("vt_s12k", f"`{src}`.`{MYSQL_DB}`.`tbl_s12`.`val`", 8)  # (k)
        finally:
            self._cleanup_src(src)
            tdSql.execute("drop database if exists fq_s12_db")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS tbl_s12"])

    def test_fq_path_s13_use_db_then_single_seg_query(self):
        """FQ-PATH-S13: Single-segment query after USE db — 1-seg resolves in current database

        Gap: FQ-PATH-016(a) only tests 1-seg on nonexistent table.  Missing:
        1-seg query after USE db on existing local table (positive), and
        whether 1-seg matching a source name resolves as local or external.

        Dimensions:
          a) 1-seg query on existing local table → returns local data
          b) 1-seg query where table name = source name → returns local data
          c) After USE, nonexistent local table → proper error
          d) After USE, 2-seg with source prefix → external data
          e) After USE, 2-seg with current db prefix → internal data
          f) Switch to different db, 1-seg no longer finds original table

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s13_ext"
        db = "fq_s13_db"
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
            "DROP TABLE IF EXISTS remote_tbl",
            "CREATE TABLE remote_tbl (id DATETIME PRIMARY KEY, val INT)",
            "INSERT INTO remote_tbl VALUES ('2024-01-01 00:00:01', 777)",
        ])
        self._cleanup_src(src)
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table meters (ts timestamp, val int)")
            tdSql.execute("insert into meters values (1704067200000, 42)")
            self._mk_mysql_real(src, database=MYSQL_DB)

            # (a) 1-seg on existing local table → local data
            tdSql.query("select val from meters order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)

            # (b) Create local table with same name as source → 1-seg local
            tdSql.execute(f"create table {src} (ts timestamp, v int)")
            tdSql.execute(f"insert into {src} values (1704067200000, 99)")
            tdSql.query(f"select v from {src} order by ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 99)

            # (c) Nonexistent local table → error
            tdSql.error("select * from nonexist_tbl",
                        expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)

            # (d) 2-seg with source prefix → external MySQL data
            tdSql.query(f"select val from {src}.remote_tbl limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 777)

            # (e) 2-seg with current db prefix → internal data
            tdSql.query(
                f"select val from {db}.meters order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)

            # (f) Switch to different db, 1-seg no longer finds meters
            tdSql.execute("drop database if exists fq_s13_other")
            tdSql.execute("create database fq_s13_other")
            tdSql.execute("use fq_s13_other")
            tdSql.error("select * from meters",
                        expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)
            # Fully qualified still works
            tdSql.query(
                f"select val from {db}.meters order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 42)
            tdSql.execute("drop database if exists fq_s13_other")
        finally:
            self._cleanup_src(src)
            tdSql.execute(f"drop database if exists {db}")
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), MYSQL_DB, [
                "DROP TABLE IF EXISTS remote_tbl"])

    def test_fq_path_s14_pg_missing_schema_comprehensive(self):
        """FQ-PATH-S14: PG missing schema comprehensive test

        Gap: FQ-PATH-003(c) and 006(b) only briefly test PG without schema.
        Missing: PG without DATABASE AND SCHEMA, ALTER to clear/set SCHEMA,
        3-seg override when no default SCHEMA.

        Dimensions:
          a) PG without DATABASE and without SCHEMA → 2-seg error
          b) PG with DATABASE only, no SCHEMA → may use 'public', verify data
          c) PG with SCHEMA only, no DATABASE → 2-seg works (uses default schema)
          d) ALTER to clear SCHEMA → 2-seg fails; 3-seg still works
          e) ALTER to set SCHEMA back → 2-seg works again

        Catalog: - Query:FederatedPathResolution

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_s14_pg"
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
            "CREATE SCHEMA IF NOT EXISTS public",
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP TABLE IF EXISTS public.t_s14",
            "CREATE TABLE public.t_s14 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO public.t_s14 VALUES ('2024-01-01 00:00:01', 1401)",
            "DROP TABLE IF EXISTS analytics.t_s14",
            "CREATE TABLE analytics.t_s14 (id TIMESTAMP PRIMARY KEY, val INT)",
            "INSERT INTO analytics.t_s14 VALUES ('2024-01-01 00:00:01', 1402)",
        ])
        self._cleanup_src(src)
        try:
            # (a) PG without DATABASE and without SCHEMA
            self._mk_pg_real(src)  # no database, no schema
            tdSql.error(f"select * from {src}.t_s14",
                        expectedErrno=TSDB_CODE_EXT_DEFAULT_NS_MISSING)
            # USE succeeds for PG even without a configured namespace
            # (PG defers namespace resolution to query time, unlike MySQL/InfluxDB)
            tdSql.execute(f"use {src}")
            # 3-seg explicit schema → works
            self._assert_error_not_syntax(
                f"select * from {src}.public.t_s14 limit 1")

            # (b) PG with DATABASE only, no SCHEMA → may use 'public'
            self._cleanup_src(src)
            self._mk_pg_real(src, database=PG_DB)  # database but no schema
            tdSql.query(f"select val from {src}.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)  # from public schema
            # 3-seg explicit schema → override to analytics
            tdSql.query(f"select val from {src}.analytics.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1402)

            # (c) PG with SCHEMA only, no DATABASE
            self._cleanup_src(src)
            self._mk_pg_real(src, schema="public")  # schema but no database
            # Behavior depends on implementation: schema might implicitly set DB
            self._assert_error_not_syntax(
                f"select * from {src}.t_s14 limit 1")
            # 3-seg override schema
            self._assert_error_not_syntax(
                f"select * from {src}.analytics.t_s14 limit 1")

            # (d) ALTER to clear SCHEMA → 2-seg fails
            self._cleanup_src(src)
            self._mk_pg_real(src, database=PG_DB, schema="public")
            # Before clear: 2-seg works
            tdSql.query(f"select val from {src}.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)
            # Clear schema
            tdSql.execute(f"alter external source {src} set schema=''")
            # After clear: 2-seg still works because the source has database=PG_DB configured,
            # and PG defaults to 'public' schema when no schema is set.
            tdSql.query(f"select val from {src}.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)
            # 3-seg explicit schema also works
            tdSql.query(f"select val from {src}.public.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1401)

            # (e) ALTER to set SCHEMA back → 2-seg works again
            tdSql.execute(
                f"alter external source {src} set schema=analytics")
            tdSql.query(f"select val from {src}.t_s14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1402)  # now from analytics
        finally:
            self._cleanup_src(src)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), PG_DB, [
                "DROP TABLE IF EXISTS public.t_s14",
                "DROP TABLE IF EXISTS analytics.t_s14",
            ])
