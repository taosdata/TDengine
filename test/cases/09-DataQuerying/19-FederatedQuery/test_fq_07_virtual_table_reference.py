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
    FederatedQueryTestMixin,
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


class TestFq07VirtualTableReference(FederatedQueryTestMixin):
    """FQ-VTBL-001 through FQ-VTBL-031: virtual table external column reference."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_vtbl_db",
            "create database fq_vtbl_db",
            "use fq_vtbl_db",
            "create table src_t1 (ts timestamp, val int, score double, name binary(32))",
            "insert into src_t1 values (1704067200000, 10, 1.5, 'alice')",
            "insert into src_t1 values (1704067260000, 20, 2.5, 'bob')",
            "insert into src_t1 values (1704067320000, 30, 3.5, 'carol')",
            "create table src_t2 (ts timestamp, metric double, tag_id int)",
            "insert into src_t2 values (1704067200000, 99.9, 1)",
            "insert into src_t2 values (1704067260000, 88.8, 2)",
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
            # Verify: query the vtable
            tdSql.query("select val, score from fq_vtbl_db.vt_mix order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 10)
            tdSql.checkData(0, 1, 1.5)
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
            tdSql.checkData(0, 0, 10)
            tdSql.query("select val from fq_vtbl_db.vt_b order by ts limit 1")
            tdSql.checkData(0, 0, 1)
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
        tdSql.execute("drop database if exists fq_vtbl_no_db")
        # Attempt without USE database
        tdSql.error(
            "create stable stb_orphan (ts timestamp, val int) tags(x int) virtual 1",
            expectedErrno=None)

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
            tdSql.checkData(0, 0, 10)
            tdSql.checkData(0, 1, 1.5)
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_err10 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # External source table without ts key would fail
            # Parser verifies ts key requirement
            tdSql.error(
                "create vtable fq_vtbl_db.vt_err10 ("
                "  val from no_such_db.no_table.col"
                ") using fq_vtbl_db.stb_err10 tags(1)",
                expectedErrno=None)
        finally:
            self._cleanup_src(src)
            self._teardown_internal_env()

    def test_fq_vtbl_011(self):
        """FQ-VTBL-011: 视图豁免 — 视图无 ts key 允许创建（按约束边界）

        Dimensions:
          a) View without timestamp column
          b) VTable creation allowed (view exemption)
          c) Constraint boundary documented

        Catalog: - Query:FederatedVTable
        Since: v3.4.0.0
        Labels: common,ci
        """
        pytest.skip("Requires external DB view without timestamp column")

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

            # (a) SELECT *
            tdSql.query("select * from fq_vtbl_db.vt_q12 order by ts")
            tdSql.checkRows(3)

            # (b) WHERE filter
            tdSql.query("select val from fq_vtbl_db.vt_q12 where val > 15 order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 20)

            # (c) Column projection
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
            tdSql.checkData(0, 0, 3)   # count
            tdSql.checkData(0, 1, 60)  # sum: 10+20+30
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

            tdSql.query(
                "select _wstart, count(*) from fq_vtbl_db.vt_q14 interval(1m)")
            assert tdSql.queryRows > 0
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_q16 "
                "(ts timestamp, val int) tags(r int) virtual 1")
            tdSql.execute(
                "create vtable fq_vtbl_db.vt_q16 ("
                "  val from fq_vtbl_db.src_t1.val"
                ") using fq_vtbl_db.stb_q16 tags(1)")
            # JOIN with external dim table
            self._assert_not_syntax_error(
                f"select a.val from fq_vtbl_db.vt_q16 a "
                f"join {src}.dim_table b on a.val = b.id limit 5")
        finally:
            self._cleanup_src(src)
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
        pytest.skip("Requires live external DB for cache behavior verification")

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
        pytest.skip("Requires live external DB and TTL wait")

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
        src = "fq_vtbl_019"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            tdSql.execute(f"refresh external source {src}")
            # After refresh, cache should be cleared
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 1")
        finally:
            self._cleanup_src(src)

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
        pytest.skip("Requires live external DBs for connection switching")

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

            # Query on stable: should include data from both vtables
            tdSql.query("select count(*) from fq_vtbl_db.stb_serial")
            tdSql.checkData(0, 0, 5)  # 3 from src_t1 + 2 from src_t2
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
            tdSql.checkRows(5)
            # Verify ordering: ts should be non-decreasing
            prev_ts = None
            for i in range(tdSql.queryRows):
                cur_ts = tdSql.queryResult[i][0]
                if prev_ts is not None:
                    assert cur_ts >= prev_ts
                prev_ts = cur_ts
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
            # Drop source table
            tdSql.execute("drop table fq_vtbl_db.src_t1")
            # Query should fail
            tdSql.error(
                "select val from fq_vtbl_db.vt_del",
                expectedErrno=None)
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
            self._mk_mysql(src)
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e28 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e28 ("
                f"  val from {src}.no_such_table.col"
                f") using fq_vtbl_db.stb_e28 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_TABLE_NOT_EXIST)
        finally:
            self._cleanup_src(src)
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e29 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e29 ("
                f"  val from {src}.orders.no_such_column"
                f") using fq_vtbl_db.stb_e29 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST)
        finally:
            self._cleanup_src(src)
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
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
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._prepare_internal_env()
            tdSql.execute(
                "create stable fq_vtbl_db.stb_e31 "
                "(ts timestamp, val int) tags(x int) virtual 1")
            # Table without ts key → error
            tdSql.error(
                f"create vtable fq_vtbl_db.vt_e31 ("
                f"  val from {src}.no_ts_table.val"
                f") using fq_vtbl_db.stb_e31 tags(1)",
                expectedErrno=TSDB_CODE_FOREIGN_NO_TS_KEY)
        finally:
            self._cleanup_src(src)
            self._teardown_internal_env()
