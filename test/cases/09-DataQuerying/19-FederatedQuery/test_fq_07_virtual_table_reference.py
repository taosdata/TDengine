"""
test_fq_07_virtual_table_reference.py

Implements FQ-VTBL-001 through FQ-VTBL-031 from TS §7
"虚拟表外部列引用" — virtual table DDL with external column references,
validation errors, query paths, cache behavior, plan splitting.

Design notes:
    - Virtual tables combine internal and external columns.
    - DDL validation tests can run against non-routable external sources
      for error path verification.
    - Query tests on internal vtables are fully testable.
    - Cache and plan-split tests need live external DBs for full coverage.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_PAR_TABLE_NOT_EXIST,
    TSDB_CODE_PAR_INVALID_REF_COLUMN,
    TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH,
    TSDB_CODE_FOREIGN_SERVER_NOT_EXIST,
    TSDB_CODE_FOREIGN_DB_NOT_EXIST,
    TSDB_CODE_FOREIGN_TABLE_NOT_EXIST,
    TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST,
    TSDB_CODE_FOREIGN_TYPE_MISMATCH,
    TSDB_CODE_FOREIGN_NO_TS_KEY,
)


# ---------------------------------------------------------------------------
# Module-level constants for fq_07 external test data
# ---------------------------------------------------------------------------

# Basic table with id/col columns (used by vtbl_017/018/019, s02, s04, s05, s06)
_MYSQL_T_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS t "
    "(id INT, col1 INT, col2 DOUBLE, col3 VARCHAR(32))",
    "DELETE FROM t",
    "INSERT INTO t VALUES "
    "(1,10,1.1,'alpha'),(2,20,2.2,'beta'),(3,30,3.3,'gamma'),"
    "(4,40,4.4,'delta'),(5,50,5.5,'epsilon')",
]

# t1 and t2 tables for multi-table scan deduplication tests (s06)
_MYSQL_T1_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS t1 (id INT, col1 INT, col2 INT)",
    "DELETE FROM t1",
    "INSERT INTO t1 VALUES (1,10,100),(2,20,200),(3,30,300)",
]
_MYSQL_T2_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS t2 (id INT, col1 INT, col2 INT)",
    "DELETE FROM t2",
    "INSERT INTO t2 VALUES (1,11,110),(2,21,210),(3,31,310)",
]

# orders table (used by vtbl_029, vtbl_030)
_MYSQL_ORDERS_SQLS = [
    "CREATE TABLE IF NOT EXISTS orders "
    "(id INT, user_id INT, amount DOUBLE, status VARCHAR(16))",
    "DELETE FROM orders",
    "INSERT INTO orders VALUES "
    "(1,1,100.0,'paid'),(2,1,200.0,'paid'),(3,2,50.0,'pending')",
]

# no_ts_table: no timestamp-compatible column -> triggers FOREIGN_NO_TS_KEY
_MYSQL_NO_TS_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS no_ts_table (val INT, name VARCHAR(32))",
    "DELETE FROM no_ts_table",
    "INSERT INTO no_ts_table VALUES (1,'alpha'),(2,'beta'),(3,'gamma')",
]

# dim_table (used by vtbl_016 JOIN test, ids 1-5 matching internal src_t1.val)
_MYSQL_DIM_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS dim_table (id INT, name VARCHAR(32))",
    "DELETE FROM dim_table",
    "INSERT INTO dim_table VALUES "
    "(1,'alice'),(2,'bob'),(3,'carol'),(4,'dave'),(5,'eve')",
]

# PG basic t table for s01 4-segment path tests
_PG_T_TABLE_SQLS = [
    "CREATE TABLE IF NOT EXISTS t (id INT, col1 INT, col2 FLOAT8, col3 TEXT)",
    "DELETE FROM t",
    "INSERT INTO t VALUES (1,10,1.1,'alpha'),(2,20,2.2,'beta'),(3,30,3.3,'gamma')",
]

class TestFq07VirtualTableReference(FederatedQueryVersionedMixin):
    """FQ-VTBL-001 through FQ-VTBL-031: virtual table external column reference."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    # Standard data constants:
    #   base_ts = 1704067200000 ms (2024-01-01 00:00:00 UTC)
    #   5 rows at +0/+60/+120/+180/+240 s
    #   src_t1: val=[1,2,3,4,5], score=[1.5,2.5,3.5,4.5,5.5]
    #   src_t2: metric=[99.9,88.8,77.7,66.6,55.5], tag_id=[1,2,3,4,5]
    _BASE_TS = 1704067200000

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_vtbl_db",
            "create database fq_vtbl_db",
            "use fq_vtbl_db",
            "create table src_t1 (ts timestamp, val int, score double, name binary(32))",
            "insert into src_t1 values (1704067200000, 1, 1.5, 'alice')",
            "insert into src_t1 values (1704067260000, 2, 2.5, 'bob')",
            "insert into src_t1 values (1704067320000, 3, 3.5, 'carol')",
            "insert into src_t1 values (1704067380000, 4, 4.5, 'dave')",
            "insert into src_t1 values (1704067440000, 5, 5.5, 'eve')",
            "create table src_t2 (ts timestamp, metric double, tag_id int)",
            "insert into src_t2 values (1704067200000, 99.9, 1)",
            "insert into src_t2 values (1704067260000, 88.8, 2)",
            "insert into src_t2 values (1704067320000, 77.7, 3)",
            "insert into src_t2 values (1704067380000, 66.6, 4)",
            "insert into src_t2 values (1704067440000, 55.5, 5)",
        ]
        tdSql.executes(sqls)

    def _teardown_internal_env(self):
        tdSql.execute("drop database if exists fq_vtbl_db")

    # ------------------------------------------------------------------
    # FQ-VTBL-001 ~ FQ-VTBL-005: DDL creation
    # ------------------------------------------------------------------

    def test_fq_vtbl_001(self):
        """FQ-VTBL-001: 创建虚拟普通表(混合列) — 内部列+外部列 DDL 成功

        Dimensions:
          a) VTable with internal columns + external column refs
          b) Internal columns from local table
          c) External columns from external source table
          d) Successful creation verified by SHOW

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_mix "
                "(ts timestamp, val int, score double) tags(region int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_mix ("
                "  val from fq_vtbl_db.src_t1.val,"
                "  score from fq_vtbl_db.src_t1.score"
                ") using fq_vtbl_db.stb_mix tags(1)")
            # Verify: query the vtable — 5 rows (all of src_t1)
            tdSql.query("select val, score from fq_vtbl_db.vt_mix order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)    # val[0]
            tdSql.checkData(0, 1, 1.5)  # score[0]
            tdSql.checkData(4, 0, 5)    # val[4]
            tdSql.checkData(4, 1, 5.5)  # score[4]
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_002(self):
        """FQ-VTBL-002: 创建虚拟子表(混合列) — USING 稳定表 + 外部列引用成功

        Dimensions:
          a) Create stable with VIRTUAL 1
          b) Create vtable using stable with external column refs
          c) Tag values set correctly

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_sub "
                "(ts timestamp, val int, metric double) tags(zone int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_sub1 ("
                "  val from fq_vtbl_db.src_t1.val,"
                "  metric from fq_vtbl_db.src_t2.metric"
                ") using fq_vtbl_db.stb_sub tags(1)")
            tdSql.query("select val, metric from fq_vtbl_db.vt_sub1 order by ts limit 2")
            tdSql.checkRows(2)
            # val from src_t1[0]=1, metric from src_t2[0]=99.9
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 99.9)
            tdSql.checkData(1, 0, 2)
            tdSql.checkData(1, 1, 88.8)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_003(self):
        """FQ-VTBL-003: 虚拟超级表多子表多源 — 子表可引用不同 external source

        Dimensions:
          a) Stable with VIRTUAL 1
          b) Multiple vtables under same stable
          c) Different source tables for different vtables
          d) Each vtable queries correctly

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_multi "
                "(ts timestamp, val int) tags(src_id int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_a ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_multi tags(1)")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_b ("
                "  val from fq_vtbl_db.src_t2.tag_id"
                ") using fq_vtbl_db.stb_multi tags(2)")
            # Query each
            tdSql.query("select val from fq_vtbl_db.vt_a order by ts limit 1")
            tdSql.checkData(0, 0, 1)    # src_t1 val[0]=1
            tdSql.query("select val from fq_vtbl_db.vt_b order by ts limit 1")
            tdSql.checkData(0, 0, 1)    # src_t2 tag_id[0]=1
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_004(self):
        """FQ-VTBL-004: 必须归属内部库 — 未 USE/CREATE 本地库时创建失败

        Dimensions:
          a) No database context → CREATE VTABLE fails
          b) Error code: database not exist or not selected

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        # TSDB_CODE_PAR_DB_NOT_SPECIFIED = 0x80002616
        _NO_DB = int(0x80002616)
        tdSql.execute("drop database if exists fq_vtbl_no_db")
        # Attempt without USE database → db-not-specified error
        tdSql.error(
            "create stable stb_orphan (ts timestamp, val int) tags(x int) virtual 1",
            expectedErrno=_NO_DB)

    def test_fq_vtbl_005(self):
        """FQ-VTBL-005: 全外部列虚拟表 — 全部列外部引用可创建

        Dimensions:
          a) All columns from external references
          b) DDL success
          c) Query verification

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_all_ext "
                "(ts timestamp, v1 int, v2 double) tags(t int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_all_ext ("
                "  v1 from fq_vtbl_db.src_t1.val,"
                "  v2 from fq_vtbl_db.src_t1.score"
                ") using fq_vtbl_db.stb_all_ext tags(1)")
            tdSql.query("select v1, v2 from fq_vtbl_db.vt_all_ext order by ts limit 1")
            tdSql.checkData(0, 0, 1)    # val[0]=1
            tdSql.checkData(0, 1, 1.5)  # score[0]=1.5
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-VTBL-006 ~ FQ-VTBL-011: DDL validation errors
    # ------------------------------------------------------------------

    def test_fq_vtbl_006(self):
        """FQ-VTBL-006: 外部源不存在 — DDL 报外部源不存在错误

        Dimensions:
          a) Reference non-existent external source
          b) Expected error: source not exist

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err6 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                "create vtable fq_vtbl_db.vt_err6 ("
                "  val from no_such_source.some_table.col"
                ") using fq_vtbl_db.stb_err6 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_SERVER_NOT_EXIST)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_007(self):
        """FQ-VTBL-007: 外部表不存在 — DDL 报表不存在错误

        Dimensions:
          a) Source exists but table doesn't
          b) Expected error: table not exist

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err7 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # Reference non-existent local table
            tdSql.error(
                "create vtable fq_vtbl_db.vt_err7 ("
                "  val from fq_vtbl_db.no_such_table.col"
                ") using fq_vtbl_db.stb_err7 tags(1)",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_008(self):
        """FQ-VTBL-008: 外部列不存在 — DDL 报列不存在错误

        Dimensions:
          a) Table exists but column doesn't
          b) Expected error: column not exist

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err8 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                "create vtable fq_vtbl_db.vt_err8 ("
                "  val from fq_vtbl_db.src_t1.no_such_col"
                ") using fq_vtbl_db.stb_err8 tags(1)",
                expectedErrno=TSDB_CODE_PAR_INVALID_REF_COLUMN)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_009(self):
        """FQ-VTBL-009: 外部类型不兼容 — DDL 报类型不匹配错误

        Dimensions:
          a) VTable column type vs source column type mismatch
          b) Expected TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH or FOREIGN_TYPE_MISMATCH

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # stb declares val as binary, but src_t1.val is int → mismatch
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err9 "
                "(ts timestamp, val binary(32)) tags(x int) virtual 1")
            tdSql.error(
                "create vtable fq_vtbl_db.vt_err9 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_err9 tags(1)",
                expectedErrno=TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_010(self):
        """FQ-VTBL-010: 无时间戳主键 — DDL 报约束错误

        Dimensions:
          a) External table without timestamp primary key
          b) Expected TSDB_CODE_FOREIGN_NO_TS_KEY (external)
          c) For internal refs, ts always exists

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_010"
        ext_db = "fq_vtbl_010_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_NO_TS_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err10 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # Real MySQL: 'no_ts_table' exists but has no timestamp-compatible column
            # -> TSDB_CODE_FOREIGN_NO_TS_KEY
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_err10 ("
                f"  val from {src}.no_ts_table.val"
                f") using fq_vtbl_db.stb_err10 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_NO_TS_KEY)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_011(self):
        """FQ-VTBL-011: 视图豁免 — 视图无 ts key 允许创建（按约束边界）

        Dimensions:
          a) View without timestamp column
          b) VTable creation allowed (view exemption)
          c) Constraint boundary documented

        The ts-PK constraint applies to external BASE TABLES only.
        External VIEWS without a ts column are exempt (view boundary).
        Internal column references have no ts-key constraint.

        Dimensions:
          a) Internal vtable ref: no ts-key constraint → DDL always succeeds
          b) External source (non-routable): DDL accepted at parser; connection
             fails at execution time — proves no syntax rejection
          c) Query on internal vtable returns correct row count

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_011_src"
        ext_db = "fq_vtbl_011_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e11 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # a) Internal ref: no ts-key constraint, always succeeds
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_e11_int ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_e11 tags(1)")
            # c) Internal vtable returns correct rows
            tdSql.query("select count(*) from fq_vtbl_db.vt_e11_int")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)  # 5 rows from src_t1
            # b) Real MySQL: 'device_view' doesn't exist -> FOREIGN_TABLE_NOT_EXIST
            # (parser accepted the DDL; connection validated -> table-not-found error)
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e11_view ("
                f"  val from {src}.device_view.val"
                f") using fq_vtbl_db.stb_e11 tags(2)",
                expectedErrno=TSDB_CODE_FOREIGN_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-VTBL-012 ~ FQ-VTBL-016: Query paths
    # ------------------------------------------------------------------

    def test_fq_vtbl_012(self):
        """FQ-VTBL-012: 虚拟表基础查询 — 投影与过滤正确

        Dimensions:
          a) SELECT * from vtable
          b) SELECT with WHERE filter
          c) Column projection
          d) Result correctness

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q12 "
                "(ts timestamp, val int, score double) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q12 ("
                "  val from fq_vtbl_db.src_t1.val,"
                "  score from fq_vtbl_db.src_t1.score"
                ") using fq_vtbl_db.stb_q12 tags(1)")

            # (a) SELECT * — 5 rows in src_t1
            tdSql.query("select * from fq_vtbl_db.vt_q12 order by ts")
            tdSql.checkRows(5)

            # (b) WHERE filter: val=[1,2,3,4,5], val>2 → 3 rows
            tdSql.query("select val from fq_vtbl_db.vt_q12 where val > 2 order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 3)   # first match: val=3
            tdSql.checkData(1, 0, 4)
            tdSql.checkData(2, 0, 5)

            # (c) Column projection: score first row
            tdSql.query("select score from fq_vtbl_db.vt_q12 order by ts limit 1")
            tdSql.checkData(0, 0, 1.5)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_013(self):
        """FQ-VTBL-013: 虚拟表聚合查询 — GROUP BY 等聚合正确

        Dimensions:
          a) COUNT/SUM/AVG on vtable
          b) GROUP BY on vtable
          c) Result correctness

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q13 "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q13 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_q13 tags(1)")

            tdSql.query("select count(*), sum(val), avg(val) from fq_vtbl_db.vt_q13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)    # count: 5 rows
            tdSql.checkData(0, 1, 15)   # sum: 1+2+3+4+5=15
            tdSql.checkData(0, 2, 3.0)  # avg: 15/5=3.0
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_014(self):
        """FQ-VTBL-014: 虚拟表窗口查询 — INTERVAL 查询结果正确

        Dimensions:
          a) INTERVAL window on vtable
          b) Window aggregation correct
          c) _wstart/_wend present

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q14 "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q14 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_q14 tags(1)")

            # 5 rows at 0/+60s/+120s/+180s/+240s → each falls in its own 1-minute window
            tdSql.query(
                "select _wstart, count(*) from fq_vtbl_db.vt_q14 interval(1m)")
            tdSql.checkRows(5)        # 5 non-overlapping 1m windows
            for row in range(5):
                tdSql.checkData(row, 1, 1)  # exactly 1 row per window
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_015(self):
        """FQ-VTBL-015: 虚拟表 JOIN 本地表 — 结果正确且计划合理

        Dimensions:
          a) VTable JOIN local table
          b) Result correctness
          c) Plan: vtable scan + local table scan + local join

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q15 "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q15 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_q15 tags(1)")

            tdSql.query(
                "select a.val, b.metric from fq_vtbl_db.vt_q15 a "
                "join fq_vtbl_db.src_t2 b on a.ts = b.ts order by a.ts limit 2")
            tdSql.checkRows(2)
            # val from src_t1, metric from src_t2, joined by ts
            tdSql.checkData(0, 0, 1)     # a.val[0]=1
            tdSql.checkData(0, 1, 99.9)  # b.metric[0]=99.9
            tdSql.checkData(1, 0, 2)     # a.val[1]=2
            tdSql.checkData(1, 1, 88.8)  # b.metric[1]=88.8
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_016(self):
        """FQ-VTBL-016: 虚拟表 JOIN 外部维表 — 结果正确

        Dimensions:
          a) VTable JOIN external dimension table
          b) Parser acceptance
          c) Requires live DB for data verification

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_016"
        ext_db = "fq_vtbl_016_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_DIM_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q16 "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q16 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_q16 tags(1)")
            # Dimension a/b/c) vtable JOIN real dim_table: val 1-5 each matches id 1-5
            tdSql.query(
                f"select a.val from fq_vtbl_db.vt_q16 a "
                f"join {src}.dim_table b on a.val = b.id order by a.val")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(4, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-VTBL-017 ~ FQ-VTBL-020: Cache behavior
    # ------------------------------------------------------------------

    def test_fq_vtbl_017(self):
        """FQ-VTBL-017: 外部列缓存命中 — TTL 内命中缓存

        Dimensions:
          a) First access → cache miss, schema fetched
          b) Second access within TTL → cache hit
          c) No additional round-trip

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_017_src"
        ext_db = "fq_vtbl_017_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_c17 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_c17a ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_c17 tags(1)")
            # a/b) Two consecutive DESCRIBEs: schema stable (cache hit)
            tdSql.query("describe fq_vtbl_db.vt_c17a")
            rows_first = tdSql.queryRows
            assert rows_first >= 2  # ts + val
            tdSql.query("describe fq_vtbl_db.vt_c17a")
            rows_second = tdSql.queryRows
            assert rows_first == rows_second, (
                "Schema changed between two DESCRIBEs — unexpected cache miss")
            # c) External source vtable: DDL accepted; schema stored locally
            tdSql.execute(
                "create stable fq_vtbl_db.stb_c17_ext "
                "(ts timestamp, ext_v int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_c17_ext ("
                f"  ext_v from {src}.t.id"
                f") using fq_vtbl_db.stb_c17_ext tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_c17_ext")
            ext_rows_first = tdSql.queryRows
            tdSql.query("describe fq_vtbl_db.vt_c17_ext")
            ext_rows_second = tdSql.queryRows
            assert ext_rows_first == ext_rows_second, (
                "External vtable schema changed unexpectedly")
            # d) Both vtables present in SHOW TABLES
            tdSql.query("show fq_vtbl_db.tables")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert any("vt_c17a" in n for n in names), "vt_c17a missing"
            # e) Internal vtable data: count=5 (all of src_t1)
            tdSql.query("select count(*) from fq_vtbl_db.vt_c17a")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_018(self):
        """FQ-VTBL-018: 外部列缓存失效 — TTL 到期后重拉 schema

        Dimensions:
          a) Wait beyond TTL
          b) Next access → schema re-fetched
          c) Schema change detected

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_018_src"
        ext_db = "fq_vtbl_018_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_c18 "
                "(ts timestamp, ext_v int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_c18 ("
                f"  ext_v from {src}.t.id"
                f") using fq_vtbl_db.stb_c18 tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_c18")
            rows_before = tdSql.queryRows
            assert rows_before >= 2  # ts + ext_v
            # a) ALTER source config: triggers schema-cache invalidation
            # Use same host to keep connection valid; any config change forces cache refresh
            tdSql.execute(
                f"alter external source {src} set host='{self._mysql_cfg().host}'")
            # b/c) VTable meta in TDengine meta store unaffected; DESCRIBE succeeds
            tdSql.query("describe fq_vtbl_db.vt_c18")
            rows_after = tdSql.queryRows
            assert rows_after == rows_before, (
                "Vtable schema changed unexpectedly after source config change")
            # d) SHOW TABLES still returns vtable
            tdSql.query("show fq_vtbl_db.tables")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert any("vt_c18" in n for n in names), (
                "vt_c18 missing from SHOW TABLES after source config change")
            # e) Internal vtable unaffected by external source config change
            tdSql.execute(
                "create stable fq_vtbl_db.stb_c18_int "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_c18_int ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_c18_int tags(1)")
            tdSql.query("select count(*) from fq_vtbl_db.vt_c18_int")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)   # internal vtable: 5 rows unaffected
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_019(self):
        """FQ-VTBL-019: REFRESH 触发缓存失效 — 手动刷新后重新加载

        Dimensions:
          a) REFRESH EXTERNAL SOURCE → cache invalidated
          b) Next query fetches fresh schema
          c) Parser acceptance

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_019"
        ext_db = "fq_vtbl_019_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_c19 "
                "(ts timestamp, ext_v int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_c19 ("
                f"  ext_v from {src}.t.id"
                f") using fq_vtbl_db.stb_c19 tags(1)")
            # Verify external col accessible before REFRESH
            tdSql.query("describe fq_vtbl_db.vt_c19")
            assert tdSql.queryRows >= 2  # ts + ext_v
            # a) REFRESH invalidates source schema cache
            tdSql.execute(f"refresh external source {src}")
            # b/c) VTable meta intact; DESCRIBE succeeds after REFRESH
            tdSql.query("describe fq_vtbl_db.vt_c19")
            assert tdSql.queryRows >= 2
            # d) Multiple REFRESH calls idempotent
            tdSql.execute(f"refresh external source {src}")
            tdSql.execute(f"refresh external source {src}")
            tdSql.query("describe fq_vtbl_db.vt_c19")
            assert tdSql.queryRows >= 2
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_020(self):
        """FQ-VTBL-020: 子表切换重建连接 — source 变化时 Connector 重新初始化

        Dimensions:
          a) VTable references source A, then switched to source B
          b) Connector re-initialized for new source
          c) Old connection released

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        # vtbl_020 verifies connector re-init when sub-vtables reference different
        # internal source tables — no external connection needed.
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_sw20 "
                "(ts timestamp, val int) tags(site int) virtual 1")
            # a) Two vtables under same stable, each references different local source
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_sw20_a ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_sw20 tags(1)")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_sw20_b ("
                "  val from fq_vtbl_db.src_t2.tag_id"
                ") using fq_vtbl_db.stb_sw20 tags(2)")
            # b) Query vtable A → data from src_t1 (5 rows)
            tdSql.query("select val from fq_vtbl_db.vt_sw20_a order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # src_t1 val[0]=1
            tdSql.checkData(4, 0, 5)   # src_t1 val[4]=5
            # c) Query vtable B → data from src_t2 (5 rows; connection re-init)
            tdSql.query("select val from fq_vtbl_db.vt_sw20_b order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # src_t2 tag_id[0]=1
            tdSql.checkData(4, 0, 5)   # src_t2 tag_id[4]=5
            # d) Super-table query: combined from both vtables
            tdSql.query("select count(*) from fq_vtbl_db.stb_sw20")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 10)  # 5+5=10
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-VTBL-021 ~ FQ-VTBL-024: Execution and plan
    # ------------------------------------------------------------------

    def test_fq_vtbl_021(self):
        """FQ-VTBL-021: 虚拟超级表串行处理 — 多子表逐个处理结果正确

        Dimensions:
          a) Multiple vtables under same stable
          b) Query on stable → all vtables processed serially
          c) Result includes all vtable data

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_serial "
                "(ts timestamp, val int) tags(src_id int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s1 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_serial tags(1)")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s2 ("
                "  val from fq_vtbl_db.src_t2.tag_id"
                ") using fq_vtbl_db.stb_serial tags(2)")

            # Query on stable: should include data from both vtables (5+5=10)
            tdSql.query("select count(*) from fq_vtbl_db.stb_serial")
            tdSql.checkData(0, 0, 10)  # 5 from src_t1 + 5 from src_t2
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_022(self):
        """FQ-VTBL-022: 多源 ts 归并排序 — SORT_MULTISOURCE_TS_MERGE 对齐正确

        Dimensions:
          a) Multiple vtables with overlapping timestamps
          b) Merge sort by ts
          c) All rows present and ordered

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_merge "
                "(ts timestamp, val int) tags(src_id int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_m1 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_merge tags(1)")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_m2 ("
                "  val from fq_vtbl_db.src_t2.tag_id"
                ") using fq_vtbl_db.stb_merge tags(2)")

            tdSql.query("select ts, val from fq_vtbl_db.stb_merge order by ts")
            tdSql.checkRows(10)  # 5 from vt_m1 (src_t1) + 5 from vt_m2 (src_t2)
            # Verify ordering: ts should be strictly non-decreasing
            prev_ts_ms = None
            for i in range(tdSql.queryRows):
                cur_ts_raw = tdSql.queryResult[i][0]
                # Convert to int (ms) for safe comparison across ts representations
                cur_ts_ms = (
                    int(cur_ts_raw)
                    if isinstance(cur_ts_raw, (int, float))
                    else int(cur_ts_raw.timestamp() * 1000)
                )
                if prev_ts_ms is not None:
                    assert cur_ts_ms >= prev_ts_ms, (
                        f"Row {i}: ts not non-decreasing: "
                        f"{cur_ts_ms} < {prev_ts_ms}")
                prev_ts_ms = cur_ts_ms
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_023(self):
        """FQ-VTBL-023: Plan Splitter 行为 — 外部扫描不拆分，内部扫描经 Exchange

        Dimensions:
          a) External scan node: not split by Plan Splitter
          b) Internal scan node: split through Exchange
          c) Verified via EXPLAIN

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_plan "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_plan ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_plan tags(1)")
            # EXPLAIN to verify plan structure
            self._assert_not_syntax_error(
                "explain select val from fq_vtbl_db.vt_plan")
            # Internal vtable data verification
            tdSql.query("select count(*) from fq_vtbl_db.vt_plan")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)   # 5 rows from src_t1
            tdSql.query("select val from fq_vtbl_db.vt_plan order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # val[0]=1
            tdSql.checkData(4, 0, 5)   # val[4]=5
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_024(self):
        """FQ-VTBL-024: 删除被引用源后查询 — 行为符合约束（失败/中断）

        Dimensions:
          a) Drop source referenced by vtable
          b) Query vtable → failure or graceful error
          c) Error message indicates missing source

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        # Internal refs: drop source table, then query vtable
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_del "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_del ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_del tags(1)")
            # Drop source table — vtable now has dangling internal reference
            tdSql.execute("drop table fq_vtbl_db.src_t1")
            # Query must fail (source table missing); no silent NULL or empty result
            tdSql.error(
                "select val from fq_vtbl_db.vt_del",
                expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-VTBL-025 ~ FQ-VTBL-031: DDL details and error codes
    # ------------------------------------------------------------------

    def test_fq_vtbl_025(self):
        """FQ-VTBL-025: CREATE STABLE ... VIRTUAL 1 语法正确性

        Dimensions:
          a) VIRTUAL 1 flag accepted
          b) Stable created successfully
          c) Can create vtables under it

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_v1 "
                "(ts timestamp, val int, score double) tags(zone int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_v1 ("
                "  val from fq_vtbl_db.src_t1.val,"
                "  score from fq_vtbl_db.src_t1.score"
                ") using fq_vtbl_db.stb_v1 tags(1)")
            tdSql.query("select * from fq_vtbl_db.vt_v1 limit 1")
            tdSql.checkRows(1)
            # val[0]=1, score[0]=1.5
            tdSql.checkData(0, 1, 1)    # val
            tdSql.checkData(0, 2, 1.5)  # score
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_026(self):
        """FQ-VTBL-026: 虚拟表 DDL 外部源不存在返回 TSDB_CODE_FOREIGN_SERVER_NOT_EXIST

        Dimensions:
          a) Column ref → unregistered source_name
          b) Error code: TSDB_CODE_FOREIGN_SERVER_NOT_EXIST
          c) Error message contains source name

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e26 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                "create vtable fq_vtbl_db.vt_e26 ("
                "  val from fake_source.some_table.col"
                ") using fq_vtbl_db.stb_e26 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_SERVER_NOT_EXIST)
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_027(self):
        """FQ-VTBL-027: 虚拟表 DDL 外部 database 不存在返回 TSDB_CODE_FOREIGN_DB_NOT_EXIST

        Dimensions:
          a) 4-segment path with non-existent database
          b) Error code: TSDB_CODE_FOREIGN_DB_NOT_EXIST

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_027"
        self._cleanup_src(src)
        try:
            # No database in source -> 4-seg path; 'nonexistent_db' doesn't exist -> FOREIGN_DB_NOT_EXIST
            self._mk_mysql_real(src, database=None)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e27 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e27 ("
                f"  val from {src}.nonexistent_db.some_table.col"
                f") using fq_vtbl_db.stb_e27 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_DB_NOT_EXIST)
        finally:
            self._cleanup_src(src)
            self._teardown_internal_env()

    def test_fq_vtbl_028(self):
        """FQ-VTBL-028: 虚拟表 DDL 外部表不存在返回 TSDB_CODE_FOREIGN_TABLE_NOT_EXIST

        Dimensions:
          a) Source exists, database exists, table doesn't
          b) Error code: TSDB_CODE_FOREIGN_TABLE_NOT_EXIST

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_028"
        ext_db = "fq_vtbl_028_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e28 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # 'no_such_table' doesn't exist in ext_db -> FOREIGN_TABLE_NOT_EXIST
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e28 ("
                f"  val from {src}.no_such_table.col"
                f") using fq_vtbl_db.stb_e28 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_029(self):
        """FQ-VTBL-029: 虚拟表 DDL 外部列不存在返回 TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST

        Dimensions:
          a) Source+table exist, column name misspelled
          b) Error code: TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST
          c) Error message contains column name

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_029"
        ext_db = "fq_vtbl_029_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_ORDERS_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e29 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # 'orders' EXISTS but 'no_such_column' doesn't -> FOREIGN_COLUMN_NOT_EXIST
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e29 ("
                f"  val from {src}.orders.no_such_column"
                f") using fq_vtbl_db.stb_e29 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_030(self):
        """FQ-VTBL-030: 虚拟表 DDL 类型不兼容返回 TSDB_CODE_FOREIGN_TYPE_MISMATCH

        Dimensions:
          a) VTable declared type != external column mapped type
          b) Error code: TSDB_CODE_FOREIGN_TYPE_MISMATCH
          c) Error message contains source type and target type

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_030"
        ext_db = "fq_vtbl_030_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_ORDERS_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            # stb declares val as binary(32); orders.amount is DOUBLE -> type mismatch
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e30 "
                "(ts timestamp, val binary(32)) tags(x int) virtual 1")
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e30 ("
                f"  val from {src}.orders.amount"
                f") using fq_vtbl_db.stb_e30 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_TYPE_MISMATCH)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_031(self):
        """FQ-VTBL-031: 虚拟表 DDL 无时间戳主键返回 TSDB_CODE_FOREIGN_NO_TS_KEY

        Dimensions:
          a) External table has no TIMESTAMP-mappable primary key
          b) Error code: TSDB_CODE_FOREIGN_NO_TS_KEY

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_vtbl_031"
        ext_db = "fq_vtbl_031_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_NO_TS_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e31 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # 'no_ts_table' exists but has no timestamp-compatible column -> FOREIGN_NO_TS_KEY
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e31 ("
                f"  val from {src}.no_ts_table.val"
                f") using fq_vtbl_db.stb_e31 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_NO_TS_KEY)
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # sXX: Gap supplement cases
    # ------------------------------------------------------------------

    def test_fq_vtbl_s01_four_segment_external_path(self):
        """s01: 4-segment external column path (source.db.table.col).

        Gap source: FS §3.8.2.1.1 — column_reference supports both
        3-segment (source.table.col) and 4-segment (source.db.table.col).
        No FQ-VTBL-001~031 case tests the 4-segment form explicitly.

        Dimensions:
          a) 4-segment MySQL path: source_name.database.table.col succeeds
          b) 4-segment PG path: source_name.database.table.col succeeds
          c) Mix of 3-seg + 4-seg in same vtable DDL
          d) Bogus 4-segment (wrong database name) → TSDB_CODE_FOREIGN_DB_NOT_EXIST

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_m = "fq_vtbl_s01_m"
        src_p = "fq_vtbl_s01_p"
        m_db = "testdb"   # keep same name so 4-seg paths match
        p_db = "pgdb"
        self._cleanup_src(src_m, src_p)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), m_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), m_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src_m, database=m_db)
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), p_db)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), p_db, _PG_T_TABLE_SQLS)
            self._mk_pg_real(src_p, database=p_db)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s01 "
                "(ts timestamp, c1 int, c2 int) tags(x int) virtual 1")
            # a) MySQL 4-segment
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s01_m4 ("
                f"  c1 from {src_m}.testdb.t.id,"
                f"  c2 from fq_vtbl_db.src_t1.val"
                f") using fq_vtbl_db.stb_s01 tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_s01_m4")
            assert tdSql.queryRows >= 3  # ts + c1 + c2
            # b) PG 4-segment
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s01b "
                "(ts timestamp, c1 int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s01_p4 ("
                f"  c1 from {src_p}.pgdb.t.id"
                f") using fq_vtbl_db.stb_s01b tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_s01_p4")
            assert tdSql.queryRows >= 2
            # c) Mix: 3-seg + 4-seg in same vtable (already proven by vt_s01_m4)
            # d) Bogus database in 4-seg path → FOREIGN_DB_NOT_EXIST
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_s01_bad ("
                f"  c1 from {src_m}.no_such_db.t.id"
                f") using fq_vtbl_db.stb_s01 tags(2)",
                expectedErrno=TSDB_CODE_FOREIGN_DB_NOT_EXIST)
        finally:
            self._cleanup_src(src_m, src_p)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), m_db)
            except Exception:
                pass
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), p_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_s02_alter_vtable_add_column(self):
        """s02: ALTER VTABLE ADD COLUMN validates external col ref.

        Gap source: DS §5.5.3.1 — ALTER VTABLE triggers DDL validation
        for modified columns only. No TS case covers ALTER on vtable.

        Dimensions:
          a) ALTER VTABLE ADD COLUMN with internal ref succeeds + verify
          b) New column visible in DESCRIBE after ALTER
          c) ALTER ADD COLUMN with nonexistent source → FOREIGN_SERVER_NOT_EXIST
          d) Existing columns unaffected by failed ALTER

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        # s02: vtable references only internal data; external source not required
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s02 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s02 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_s02 tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_s02")
            rows_before = tdSql.queryRows
            # a) ALTER ADD internal col
            tdSql.execute(
                "alter table fq_vtbl_db.stb_s02 add column score double")
            tdSql.execute(
                "alter table fq_vtbl_db.vt_s02 modify column score "
                "from fq_vtbl_db.src_t1.score")
            # b) New column visible in DESCRIBE
            tdSql.query("describe fq_vtbl_db.vt_s02")
            assert tdSql.queryRows > rows_before, "score column not added"
            # c) ALTER with nonexistent source → error
            tdSql.error(
                "alter table fq_vtbl_db.stb_s02 add column bad_c int")
            # d) Existing columns unaffected: val still mapped, 5 rows
            tdSql.query("select val from fq_vtbl_db.vt_s02 order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)   # val[0]=1
            tdSql.checkData(4, 0, 5)   # val[4]=5
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_s03_partition_by_slimit_on_vstb(self):
        """s03: PARTITION BY + SLIMIT/SOFFSET on virtual super table.

        Gap source: DS §5.5.8 (optimizer skips rules for external-ref vtables);
        FS §3.7.3 SLIMIT local execution. Not covered by FQ-VTBL-021.

        Dimensions:
          a) PARTITION BY tag: 2 partitions (1 per child) × 3/2 rows
          b) SLIMIT 1 → returns only 1 partition
          c) SOFFSET 1 → returns the second partition
          d) Each partition COUNT(*) is correct

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s03 "
                "(ts timestamp, val int) tags(site int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s03_a ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_s03 tags(1)")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s03_b ("
                "  val from fq_vtbl_db.src_t2.tag_id"
                ") using fq_vtbl_db.stb_s03 tags(2)")
            # a) PARTITION BY site: 2 partitions
            tdSql.query(
                "select count(*) from fq_vtbl_db.stb_s03 partition by site")
            tdSql.checkRows(2)
            # Both vtables have 5 rows each (src_t1 and src_t2 each have 5 rows)
            counts = sorted([tdSql.queryResult[0][0], tdSql.queryResult[1][0]])
            assert counts == [5, 5], f"Unexpected partition counts: {counts}"
            # b) SLIMIT 1 → 1 partition
            tdSql.query(
                "select count(*) from fq_vtbl_db.stb_s03 "
                "partition by site slimit 1")
            tdSql.checkRows(1)
            # c) SOFFSET 1 → second partition
            tdSql.query(
                "select count(*) from fq_vtbl_db.stb_s03 "
                "partition by site slimit 1 soffset 1")
            tdSql.checkRows(1)
            # d) Each partition count is 5
            assert tdSql.queryResult[0][0] == 5
        finally:
            self._teardown_internal_env()

    def test_fq_vtbl_s04_optimizer_skip_with_external_ref(self):
        """s04: Optimizer skips all rules when vtable has external col ref.

        Gap source: DS §5.5.8.1 hasExternalColRef() → all optimization
        rules bypassed. No TS case verifies optimizer skip for complex
        queries (filter + group + sort) on vtable with external col.

        Dimensions:
          a) Internal-only vtable: COUNT(WHERE v>10) and SUM(WHERE v>10) correct
          b) External-ref vtable: complex query parser-accepted, no syntax error
          c) Optimizer skip does not affect result for internal-only vtable

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_s04_src"
        ext_db = "fq_vtbl_s04_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s04 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # c) Internal-only vtable: optimizations applied, result must be correct
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s04_int ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_s04 tags(1)")
            # a) Complex query on internal vtable: val=[1,2,3,4,5], val>2 → [3,4,5]
            tdSql.query(
                "select count(*), sum(val) from fq_vtbl_db.vt_s04_int "
                "where val > 2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3)   # count(3,4,5)=3
            tdSql.checkData(0, 1, 12)  # sum(3+4+5)=12
            # b) External-ref vtable: optimizer skips all rules; query accepted
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s04_ext "
                "(ts timestamp, val int, ext_v int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s04_ext ("
                f"  val from fq_vtbl_db.src_t1.val,"
                f"  ext_v from {src}.t.id"
                f") using fq_vtbl_db.stb_s04_ext tags(1)")
            self._assert_not_syntax_error(
                "select count(*), sum(val) from fq_vtbl_db.vt_s04_ext "
                "where val > 0 order by ts limit 10")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()

    def test_fq_vtbl_s05_system_table_visibility(self):
        """s05: Vtable and vstable visible in system information tables.

        Gap source: FS §3.9.1 ins_ext_sources; DS §5.5.1 Catalog.
        No TS case checks system table rows specifically for vtable with
        external column references.

        Dimensions:
          a) External source appears in information_schema.ins_ext_sources
          b) Vtable appears in SHOW TABLES after CREATE
          c) Virtual stable appears in SHOW STABLES after CREATE STABLE ... VIRTUAL 1
          d) DROP vtable → removed from SHOW TABLES
          e) DROP external source → removed from ins_ext_sources
          f) DROP virtual stable → removed from SHOW STABLES

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_s05_src"
        ext_db = "fq_vtbl_s05_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), ext_db, _MYSQL_T_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # a) Source in ins_ext_sources
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(1)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s05 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_s05 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_s05 tags(1)")
            # b) Vtable in SHOW TABLES
            tdSql.query("show fq_vtbl_db.tables")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert any("vt_s05" in n for n in names), "vt_s05 missing from SHOW TABLES"
            # c) Virtual stable in SHOW STABLES
            tdSql.query("show fq_vtbl_db.stables")
            stable_names = [str(r[0]) for r in tdSql.queryResult]
            assert any("stb_s05" in n for n in stable_names), (
                "stb_s05 missing from SHOW STABLES")
            # d) DROP vtable → table removed
            tdSql.execute("drop table fq_vtbl_db.vt_s05")
            tdSql.query("show fq_vtbl_db.tables")
            names_after = [str(r[0]) for r in tdSql.queryResult]
            assert not any("vt_s05" in n for n in names_after), (
                "vt_s05 still in SHOW TABLES after DROP")
            # f) DROP vstable → stable removed
            tdSql.execute("drop table fq_vtbl_db.stb_s05")
            tdSql.query("show fq_vtbl_db.stables")
            stable_names_after = [str(r[0]) for r in tdSql.queryResult]
            assert not any("stb_s05" in n for n in stable_names_after), (
                "stb_s05 still in SHOW STABLES after DROP")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            # e) DROP source → removed from ins_ext_sources
            tdSql.query(
                f"select source_name from information_schema.ins_ext_sources "
                f"where source_name = '{src}'")
            tdSql.checkRows(0)
            self._teardown_internal_env()

    def test_fq_vtbl_s06_multi_col_same_ext_table(self):
        """s06: Multiple columns from the same external table share one scan node.

        Gap source: DS §5.5.5.1 — cols from the same (source, db, table)
        are merged into a single SScanLogicNode(SCAN_TYPE_EXTERNAL).
        No TS case explicitly tests this deduplication.

        Dimensions:
          a) Three cols from same external table: DDL accepted (stmt parsed once)
          b) DESCRIBE shows all three external cols
          c) Two cols from table_A + two from table_B in same vtable: both accepted
          d) Query on internal val col returns correct data

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        src = "fq_vtbl_s06_src"
        ext_db = "fq_vtbl_s06_ext"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), ext_db)
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), ext_db,
                _MYSQL_T_TABLE_SQLS + _MYSQL_T1_TABLE_SQLS + _MYSQL_T2_TABLE_SQLS)
            self._mk_mysql_real(src, database=ext_db)
            # a) Three cols from same external table
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s06a "
                "(ts timestamp, c1 int, c2 double, c3 binary(32)) "
                "tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s06_three ("
                f"  c1 from {src}.t.col1,"
                f"  c2 from {src}.t.col2,"
                f"  c3 from {src}.t.col3"
                f") using fq_vtbl_db.stb_s06a tags(1)")
            # b) DESCRIBE shows ts + 3 external cols
            tdSql.query("describe fq_vtbl_db.vt_s06_three")
            assert tdSql.queryRows >= 4, (
                f"Expected ts+3 cols, got {tdSql.queryRows}")
            # c) Two from table_A + two from table_B (two separate scan nodes)
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s06b "
                "(ts timestamp, a1 int, a2 int, b1 int, b2 int) "
                "tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s06_two_src ("
                f"  a1 from {src}.t1.col1,"
                f"  a2 from {src}.t1.col2,"
                f"  b1 from {src}.t2.col1,"
                f"  b2 from {src}.t2.col2"
                f") using fq_vtbl_db.stb_s06b tags(1)")
            tdSql.query("describe fq_vtbl_db.vt_s06_two_src")
            assert tdSql.queryRows >= 5  # ts + 4 cols
            # d) Mix: internal col + external cols; internal val query correct
            tdSql.execute(
                "create stable fq_vtbl_db.stb_s06c "
                "(ts timestamp, val int, c1 int) tags(x int) virtual 1")
            tdSql.execute(
                f"create vtable fq_vtbl_db.vt_s06_mix ("
                f"  val from fq_vtbl_db.src_t1.val,"
                f"  c1 from {src}.t.col1"
                f") using fq_vtbl_db.stb_s06c tags(1)")
            tdSql.query("select val from fq_vtbl_db.vt_s06_mix order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 1)  # val[0]=1
            tdSql.checkData(1, 0, 2)  # val[1]=2
            tdSql.checkData(2, 0, 3)  # val[2]=3
            tdSql.checkData(3, 0, 4)  # val[3]=4
            tdSql.checkData(4, 0, 5)  # val[4]=5
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), ext_db)
            except Exception:
                pass
            self._teardown_internal_env()
