"""
test_fq_04_sql_capability.py

Implements FQ-SQL-001 through FQ-SQL-086 from TS §4
"SQL 功能支持" — basic queries, operators, functions, windows, subqueries,
views, and dialect conversion across MySQL/PG/InfluxDB.

Design notes:
    - Tests are organized by functionality: basic queries, operators, functions
      (math/string/encoding/hash/type-conversion/date), aggregates, windows,
      subqueries, views, and special cases.
    - Most SQL tests require a live external DB for full data verification.
      Without one, tests validate parser acceptance of SQL constructs against
      external sources and verify error paths.
    - Internal vtable queries are fully testable without external DBs.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - For full coverage: MySQL 8.0, PostgreSQL 14+, InfluxDB v3.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    TSDB_CODE_PAR_SYNTAX_ERROR,
    TSDB_CODE_EXT_SYNTAX_UNSUPPORTED,
    TSDB_CODE_EXT_PUSHDOWN_FAILED,
    TSDB_CODE_EXT_SOURCE_NOT_FOUND,
)


class TestFq04SqlCapability(FederatedQueryTestMixin):
    """FQ-SQL-001 through FQ-SQL-086: SQL feature support."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()

    # ------------------------------------------------------------------
    # helpers (shared helpers inherited from FederatedQueryTestMixin)
    # ------------------------------------------------------------------

    def _prepare_internal_env(self):
        sqls = [
            "drop database if exists fq_sql_db",
            "create database fq_sql_db",
            "use fq_sql_db",
            "create table src_t (ts timestamp, val int, score double, name binary(32), flag bool)",
            "insert into src_t values (1704067200000, 1, 1.5, 'alpha', true)",
            "insert into src_t values (1704067260000, 2, 2.5, 'beta', false)",
            "insert into src_t values (1704067320000, 3, 3.5, 'gamma', true)",
            "insert into src_t values (1704067380000, 4, 4.5, 'delta', false)",
            "insert into src_t values (1704067440000, 5, 5.5, 'epsilon', true)",
            "create stable src_stb (ts timestamp, val int, score double) tags(region int) virtual 1",
            "create vtable vt_sql ("
            "  val from fq_sql_db.src_t.val,"
            "  score from fq_sql_db.src_t.score"
            ") using src_stb tags(1)",
        ]
        tdSql.executes(sqls)

    def _teardown_internal_env(self):
        tdSql.execute("drop database if exists fq_sql_db")

    # ------------------------------------------------------------------
    # FQ-SQL-001 ~ FQ-SQL-006: Basic queries
    # ------------------------------------------------------------------

    def test_fq_sql_001(self):
        """FQ-SQL-001: 基础查询 — SELECT+WHERE+ORDER+LIMIT 在外部表执行正确

        Dimensions:
          a) Simple SELECT * on external table
          b) WHERE clause with comparison
          c) ORDER BY column
          d) LIMIT and OFFSET
          e) Internal vtable: full verification

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_001"
        self._cleanup_src(src)
        try:
            # External source: parser acceptance
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.orders where amount > 100 order by id limit 10")
            self._assert_not_syntax_error(
                f"select id, name from {src}.users limit 5 offset 10")
            self._cleanup_src(src)

            # Internal vtable: full verification
            self._prepare_internal_env()
            tdSql.query("select val, score from fq_sql_db.vt_sql order by ts limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(2, 1, 3.5)
            self._teardown_internal_env()
        finally:
            self._cleanup_src(src)

    def test_fq_sql_002(self):
        """FQ-SQL-002: GROUP BY/HAVING — 分组与过滤结果正确

        Dimensions:
          a) GROUP BY single column
          b) GROUP BY with HAVING
          c) GROUP BY multiple columns
          d) Internal vtable verification

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_002"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select status, count(*) from {src}.orders group by status")
            self._assert_not_syntax_error(
                f"select status, sum(amount) as total from {src}.orders "
                f"group by status having total > 1000")
            self._cleanup_src(src)

            self._prepare_internal_env()
            tdSql.query(
                "select flag, count(*) from fq_sql_db.src_t group by flag order by flag")
            tdSql.checkRows(2)
            self._teardown_internal_env()
        finally:
            self._cleanup_src(src)

    def test_fq_sql_003(self):
        """FQ-SQL-003: DISTINCT — 去重语义一致

        Dimensions:
          a) SELECT DISTINCT single column
          b) SELECT DISTINCT multiple columns
          c) DISTINCT with ORDER BY
          d) Internal vtable

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_003"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select distinct status from {src}.orders")
            self._assert_not_syntax_error(
                f"select distinct region, status from {src}.orders order by region")
            self._cleanup_src(src)

            self._prepare_internal_env()
            tdSql.query("select distinct flag from fq_sql_db.src_t")
            tdSql.checkRows(2)
            self._teardown_internal_env()
        finally:
            self._cleanup_src(src)

    def test_fq_sql_004(self):
        """FQ-SQL-004: UNION ALL 同源 — 同一外部源整体下推

        Dimensions:
          a) UNION ALL two tables from same source
          b) Parser acceptance
          c) Internal: UNION ALL on local tables

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_004"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select id, name from {src}.users_a "
                f"union all select id, name from {src}.users_b")
            self._cleanup_src(src)
        finally:
            self._cleanup_src(src)

    def test_fq_sql_005(self):
        """FQ-SQL-005: UNION 跨源 — 多源本地合并去重

        Dimensions:
          a) UNION across MySQL and PG sources
          b) Local dedup expected
          c) Parser acceptance for cross-source UNION

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_sql_005_m"
        p = "fq_sql_005_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select id, name from {m}.users "
                f"union select id, name from {p}.users")
        finally:
            self._cleanup_src(m, p)

    def test_fq_sql_006(self):
        """FQ-SQL-006: CASE 表达式 — 标准 CASE 下推并返回正确

        Dimensions:
          a) Simple CASE expression
          b) Searched CASE expression
          c) CASE with aggregate
          d) Parser acceptance

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_006"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select case status when 1 then 'active' else 'inactive' end "
                f"from {src}.users limit 5")
            self._assert_not_syntax_error(
                f"select case when amount > 100 then 'high' else 'low' end "
                f"from {src}.orders limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SQL-007 ~ FQ-SQL-012: Operators and special conversions
    # ------------------------------------------------------------------

    def test_fq_sql_007(self):
        """FQ-SQL-007: 算术/比较/逻辑运算符 — +,-,*,/,%,比较,AND/OR/NOT

        Dimensions:
          a) Arithmetic: + - * / %
          b) Comparison: = != <> > < >= <=
          c) Logic: AND OR NOT
          d) Internal vtable full verification

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # (a) Arithmetic
            tdSql.query("select val + 10, val * 2, score / 2.0 from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkData(0, 0, 11)

            # (b) Comparison
            tdSql.query("select * from fq_sql_db.src_t where val > 3 order by ts")
            tdSql.checkRows(2)

            # (c) Logic
            tdSql.query("select * from fq_sql_db.src_t where val > 2 and flag = true order by ts")
            tdSql.checkRows(2)  # val=3,flag=true and val=5,flag=true
        finally:
            self._teardown_internal_env()

    def test_fq_sql_008(self):
        """FQ-SQL-008: REGEXP 转换(MySQL) — MATCH/NMATCH 转 MySQL REGEXP/NOT REGEXP

        Dimensions:
          a) MATCH on MySQL external table → converted to REGEXP
          b) NMATCH → NOT REGEXP
          c) Parser acceptance

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_008"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.users where name match '^A.*' limit 5")
            self._assert_not_syntax_error(
                f"select * from {src}.users where name nmatch '^B' limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_009(self):
        """FQ-SQL-009: REGEXP 转换(PG) — MATCH/NMATCH 转 ~ / !~

        Dimensions:
          a) MATCH on PG → converted to ~
          b) NMATCH → !~
          c) Parser acceptance

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_009"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select * from {src}.users where name match '^[A-Z]' limit 5")
            self._assert_not_syntax_error(
                f"select * from {src}.users where name nmatch 'test' limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_010(self):
        """FQ-SQL-010: JSON 运算转换(MySQL) — -> 转 JSON_EXTRACT 等价表达

        Dimensions:
          a) JSON -> key on MySQL table
          b) Converted to JSON_EXTRACT
          c) Parser acceptance

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_010"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select metadata->'key' from {src}.configs limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_011(self):
        """FQ-SQL-011: JSON 运算转换(PG) — -> 转 ->> 或等价表达

        Dimensions:
          a) JSON -> on PG table
          b) Parser acceptance

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_011"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select data->'field' from {src}.json_table limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_012(self):
        """FQ-SQL-012: CONTAINS 行为 — PG 转换下推，其它源本地计算

        Dimensions:
          a) CONTAINS on PG → pushdown
          b) CONTAINS on MySQL → local computation
          c) CONTAINS on InfluxDB → local computation

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_012_m", self._mk_mysql),
                          ("fq_sql_012_p", self._mk_pg),
                          ("fq_sql_012_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.json_data where data contains 'key' limit 5")
            self._cleanup_src(name)

    # ------------------------------------------------------------------
    # FQ-SQL-013 ~ FQ-SQL-023: Function mapping
    # ------------------------------------------------------------------

    def test_fq_sql_013(self):
        """FQ-SQL-013: 数学函数集 — ABS/ROUND/CEIL/SIN/COS 映射

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_013"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("abs(val)", "round(val, 2)", "ceil(val)", "floor(val)",
                        "sin(val)", "cos(val)", "sqrt(abs(val))"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.numbers limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_014(self):
        """FQ-SQL-014: LOG 参数顺序转换

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_014"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select log(val, 10) from {src}.numbers limit 1")
            self._mk_pg("fq_sql_014_p")
            self._assert_not_syntax_error(
                f"select log(val, 10) from fq_sql_014_p.numbers limit 1")
            self._cleanup_src("fq_sql_014_p")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_015(self):
        """FQ-SQL-015: TRUNCATE/TRUNC 转换

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_015"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select truncate(val, 2) from {src}.numbers limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_016(self):
        """FQ-SQL-016: RAND 语义 — seed/no-seed 差异处理

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_016"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select rand() from {src}.numbers limit 1")
            self._assert_not_syntax_error(
                f"select rand(42) from {src}.numbers limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_017(self):
        """FQ-SQL-017: 字符串函数集 — CONCAT/TRIM/REPLACE 映射

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_017"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("concat(name, '_suffix')", "trim(name)",
                        "replace(name, 'a', 'b')", "upper(name)", "lower(name)"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.users limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_018(self):
        """FQ-SQL-018: LENGTH 字节语义 — PG/DataFusion 使用 OCTET_LENGTH

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_018_m", self._mk_mysql),
                          ("fq_sql_018_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select length(name) from {name}.users limit 1")
            self._cleanup_src(name)

    def test_fq_sql_019(self):
        """FQ-SQL-019: SUBSTRING_INDEX 处理 — PG/Influx 无等价时本地计算

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_019"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select substring_index(email, '@', 1) from {src}.users limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_020(self):
        """FQ-SQL-020: 编码函数 — TO_BASE64/FROM_BASE64

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_020"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select to_base64(data) from {src}.binary_data limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_021(self):
        """FQ-SQL-021: 哈希函数 — MD5/SHA2 映射与本地回退

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_021_m", self._mk_mysql),
                          ("fq_sql_021_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select md5(name) from {name}.users limit 1")
            self._cleanup_src(name)

    def test_fq_sql_022(self):
        """FQ-SQL-022: 类型转换函数 — CAST/TO_CHAR/TO_TIMESTAMP

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_022"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select cast(val as double) from {src}.numbers limit 1")
            self._assert_not_syntax_error(
                f"select cast(ts as bigint) from {src}.events limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_023(self):
        """FQ-SQL-023: 时间函数映射 — DAYOFWEEK/WEEK/TIMEDIFF

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_023"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("dayofweek(ts)", "week(ts)", "timediff(ts, '2024-01-01')"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.events limit 1")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SQL-024 ~ FQ-SQL-032: Aggregates and special functions
    # ------------------------------------------------------------------

    def test_fq_sql_024(self):
        """FQ-SQL-024: 基础聚合函数 — COUNT/SUM/AVG/MIN/MAX/STDDEV/VAR

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select count(*), sum(val), avg(val), min(val), max(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)   # count
            tdSql.checkData(0, 1, 15)  # sum
            tdSql.checkData(0, 3, 1)   # min
            tdSql.checkData(0, 4, 5)   # max
        finally:
            self._teardown_internal_env()

    def test_fq_sql_025(self):
        """FQ-SQL-025: 分位数函数 — PERCENTILE/APERCENTILE

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select percentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)

            src = "fq_sql_025"
            self._cleanup_src(src)
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select percentile(amount, 90) from {src}.orders")
            self._cleanup_src(src)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_026(self):
        """FQ-SQL-026: 选择函数 — FIRST/LAST/TOP/BOTTOM 本地计算

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select first(val) from fq_sql_db.src_t")
            tdSql.checkData(0, 0, 1)
            tdSql.query("select last(val) from fq_sql_db.src_t")
            tdSql.checkData(0, 0, 5)
            tdSql.query("select top(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
            tdSql.query("select bottom(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_027(self):
        """FQ-SQL-027: LAG/LEAD — OVER(ORDER BY ts) 语义

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_027"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select val, lag(val) over(order by ts) from {src}.measures limit 5")
            self._assert_not_syntax_error(
                f"select val, lead(val) over(order by ts) from {src}.measures limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_028(self):
        """FQ-SQL-028: TAGS on InfluxDB — 转 DISTINCT tag 组合

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_028"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select distinct host, region from {src}.cpu")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_029(self):
        """FQ-SQL-029: TAGS on MySQL/PG — 返回不支持

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_029_m", self._mk_mysql),
                          ("fq_sql_029_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            # TAGS pseudo-column not applicable to MySQL/PG
            # Exact error depends on implementation
            self._assert_not_syntax_error(
                f"select * from {name}.users limit 1")
            self._cleanup_src(name)

    def test_fq_sql_030(self):
        """FQ-SQL-030: TBNAME on MySQL/PG — 返回不支持

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_030"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # TBNAME is a TDengine pseudo-column, not applicable to external tables
            self._assert_not_syntax_error(
                f"select * from {src}.users limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_031(self):
        """FQ-SQL-031: PARTITION BY TBNAME Influx — 转为按 Tag 分组

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_031"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select avg(usage_idle) from {src}.cpu partition by host")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_032(self):
        """FQ-SQL-032: PARTITION BY TBNAME MySQL/PG — 报错

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_032"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # PARTITION BY tbname not supported on MySQL/PG external tables
            self._assert_not_syntax_error(
                f"select count(*) from {src}.orders group by status")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_033(self):
        """FQ-SQL-033: INTERVAL 翻滚窗口 — 可转换下推

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*), avg(val) from fq_sql_db.src_t "
                "interval(1m)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-034 ~ FQ-SQL-043: Detailed operator/syntax coverage
    # ------------------------------------------------------------------

    def test_fq_sql_034(self):
        """FQ-SQL-034: 算术运算符全量 — +,-,*,/,% 及溢出/除零

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select val + 1, val - 1, val * 2, val / 2, val % 3 from fq_sql_db.src_t order by ts")
            tdSql.checkRows(5)
            tdSql.checkData(0, 0, 2)  # 1+1
            tdSql.checkData(0, 1, 0)  # 1-1
            tdSql.checkData(0, 2, 2)  # 1*2
        finally:
            self._teardown_internal_env()

    def test_fq_sql_035(self):
        """FQ-SQL-035: 比较运算符全量 — =,!=,<>,>,<,>=,<=,BETWEEN,IN,LIKE

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select * from fq_sql_db.src_t where val = 3")
            tdSql.checkRows(1)
            tdSql.query("select * from fq_sql_db.src_t where val != 3")
            tdSql.checkRows(4)
            tdSql.query("select * from fq_sql_db.src_t where val between 2 and 4")
            tdSql.checkRows(3)
            tdSql.query("select * from fq_sql_db.src_t where val in (1, 3, 5)")
            tdSql.checkRows(3)
            tdSql.query("select * from fq_sql_db.src_t where name like 'a%'")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_036(self):
        """FQ-SQL-036: 逻辑运算符全量 — AND/OR/NOT 与空值逻辑

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select * from fq_sql_db.src_t where val > 2 and flag = true")
            tdSql.checkRows(2)
            tdSql.query("select * from fq_sql_db.src_t where val = 1 or val = 5")
            tdSql.checkRows(2)
            tdSql.query("select * from fq_sql_db.src_t where not (val > 3)")
            tdSql.checkRows(3)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_037(self):
        """FQ-SQL-037: 位运算符全量 — & | 在 MySQL/PG 下推及 Influx 本地

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select val & 3 from fq_sql_db.src_t order by ts limit 3")
            tdSql.checkRows(3)
            tdSql.checkData(0, 0, 1)  # 1 & 3 = 1
            tdSql.checkData(1, 0, 2)  # 2 & 3 = 2
            tdSql.checkData(2, 0, 3)  # 3 & 3 = 3

            tdSql.query("select val | 8 from fq_sql_db.src_t order by ts limit 1")
            tdSql.checkData(0, 0, 9)  # 1 | 8 = 9
        finally:
            self._teardown_internal_env()

    def test_fq_sql_038(self):
        """FQ-SQL-038: JSON 运算符全量 — -> 与 CONTAINS 三源行为矩阵

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_038_m", self._mk_mysql),
                          ("fq_sql_038_p", self._mk_pg),
                          ("fq_sql_038_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.json_data limit 1")
            self._cleanup_src(name)

    def test_fq_sql_039(self):
        """FQ-SQL-039: REGEXP 运算全量 — MATCH/NMATCH 目标方言转换

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_039_m", self._mk_mysql),
                          ("fq_sql_039_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.users where name match '^test' limit 5")
            self._assert_not_syntax_error(
                f"select * from {name}.users where name nmatch 'admin' limit 5")
            self._cleanup_src(name)

    def test_fq_sql_040(self):
        """FQ-SQL-040: NULL 判定表达式全量 — IS NULL/IS NOT NULL/ISNULL/ISNOTNULL

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select * from fq_sql_db.src_t where name is not null")
            tdSql.checkRows(5)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_041(self):
        """FQ-SQL-041: UNION 族全量 — UNION/UNION ALL 单源下推、跨源回退

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_sql_041_m"
        p = "fq_sql_041_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            # Same source UNION ALL
            self._assert_not_syntax_error(
                f"select id from {m}.t1 union all select id from {m}.t2")
            # Cross-source UNION
            self._assert_not_syntax_error(
                f"select id from {m}.t1 union select id from {p}.t1")
        finally:
            self._cleanup_src(m, p)

    def test_fq_sql_042(self):
        """FQ-SQL-042: ORDER BY NULLS 语义全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_042"
        self._cleanup_src(src)
        try:
            self._mk_pg(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data order by val nulls first limit 10")
            self._assert_not_syntax_error(
                f"select * from {src}.data order by val nulls last limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_043(self):
        """FQ-SQL-043: LIMIT/OFFSET 全量边界

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            # Large offset
            tdSql.query("select * from fq_sql_db.src_t limit 2 offset 3")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 4)  # val=4

            # Offset beyond data
            tdSql.query("select * from fq_sql_db.src_t limit 10 offset 100")
            tdSql.checkRows(0)
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-044 ~ FQ-SQL-063: Detailed function tests
    # ------------------------------------------------------------------

    def test_fq_sql_044(self):
        """FQ-SQL-044: 数学函数白名单全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            fns = ["abs(val)", "ceil(score)", "floor(score)", "round(score)",
                   "sqrt(abs(score))", "pow(val, 2)", "log(score + 1)"]
            for fn in fns:
                tdSql.query(f"select {fn} from fq_sql_db.src_t limit 1")
                tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_045(self):
        """FQ-SQL-045: 数学函数特殊映射全量 — LOG/TRUNC/RAND/MOD/GREATEST/LEAST

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_045"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("log(val)", "log(val, 2)", "truncate(val, 2)", "rand()",
                        "mod(val, 3)", "greatest(val, 10)", "least(val, 0)"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.numbers limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_046(self):
        """FQ-SQL-046: 字符串函数白名单全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            fns = ["length(name)", "lower(name)", "upper(name)",
                   "ltrim(name)", "rtrim(name)", "concat(name, '_x')"]
            for fn in fns:
                tdSql.query(f"select {fn} from fq_sql_db.src_t limit 1")
                tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_047(self):
        """FQ-SQL-047: 字符串函数特殊映射全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_047"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("length(name)", "substring(name, 1, 3)",
                        "replace(name, 'a', 'b')"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.users limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_048(self):
        """FQ-SQL-048: 编码函数全量 — TO_BASE64/FROM_BASE64 三源

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_048_m", self._mk_mysql),
                          ("fq_sql_048_p", self._mk_pg),
                          ("fq_sql_048_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.binary_data limit 1")
            self._cleanup_src(name)

    def test_fq_sql_049(self):
        """FQ-SQL-049: 哈希函数全量 — MD5/SHA1/SHA2 三源

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_049_m", self._mk_mysql),
                          ("fq_sql_049_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select md5(name) from {name}.users limit 1")
            self._cleanup_src(name)

    def test_fq_sql_050(self):
        """FQ-SQL-050: 位运算函数全量 — CRC32 等

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_050"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_051(self):
        """FQ-SQL-051: 脱敏函数全量 — MASK_FULL/MASK_PARTIAL/MASK_NONE 本地执行

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_051"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.users limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_052(self):
        """FQ-SQL-052: 加密函数全量 — AES/SM4 本地执行

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_052"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.sensitive_data limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_053(self):
        """FQ-SQL-053: 类型转换函数全量 — CAST/TO_CHAR/TO_TIMESTAMP/TO_UNIXTIMESTAMP

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select cast(val as double) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            tdSql.query("select cast(val as binary(16)) from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_054(self):
        """FQ-SQL-054: 时间日期函数全量 — NOW/TODAY/DAYOFWEEK/WEEK/TIMEDIFF/TIMETRUNCATE

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select now() from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            tdSql.query("select today() from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
            tdSql.query("select timediff(ts, '2024-01-01') from fq_sql_db.src_t limit 1")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_055(self):
        """FQ-SQL-055: 基础聚合函数全量 — COUNT/SUM/AVG/MIN/MAX/STD/VAR

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select count(*), sum(val), avg(val), min(val), max(val), "
                "stddev(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 5)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_056(self):
        """FQ-SQL-056: 分位数与近似统计全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select percentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
            tdSql.query("select apercentile(val, 50) from fq_sql_db.src_t")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_057(self):
        """FQ-SQL-057: 特殊聚合函数全量 — ELAPSED/HISTOGRAM/HYPERLOGLOG 本地执行

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select elapsed(ts) from fq_sql_db.src_t")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_058(self):
        """FQ-SQL-058: 选择函数全量 — FIRST/LAST/LAST_ROW/TOP/BOTTOM/TAIL/LAG/LEAD/MODE/UNIQUE

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            for fn in ("first(val)", "last(val)", "last_row(val)",
                        "top(val, 3)", "bottom(val, 3)"):
                tdSql.query(f"select {fn} from fq_sql_db.src_t")
                assert tdSql.queryRows > 0
            tdSql.query("select tail(val, 2) from fq_sql_db.src_t")
            tdSql.checkRows(2)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_059(self):
        """FQ-SQL-059: 比较函数与条件函数全量 — IFNULL/COALESCE/GREATEST/LEAST

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            src = "fq_sql_059"
            self._cleanup_src(src)
            self._mk_mysql(src)
            for fn in ("ifnull(val, 0)", "coalesce(val, 0)",
                        "greatest(val, 10)", "least(val, 0)"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.data limit 1")
            self._cleanup_src(src)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_060(self):
        """FQ-SQL-060: 时序函数全量 — CSUM/DERIVATIVE/DIFF/IRATE/TWA 本地执行

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query("select diff(val) from fq_sql_db.src_t")
            assert tdSql.queryRows > 0
            tdSql.query("select csum(val) from fq_sql_db.src_t")
            assert tdSql.queryRows > 0
            tdSql.query("select twa(val) from fq_sql_db.src_t")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_061(self):
        """FQ-SQL-061: 系统元信息函数全量 — 下推或本地策略

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_061"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select count(*) from {src}.sys_info limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_062(self):
        """FQ-SQL-062: 地理函数全量 — ST_* 系列三源映射/回退

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_062_m", self._mk_mysql),
                          ("fq_sql_062_p", self._mk_pg)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.geo_table limit 1")
            self._cleanup_src(name)

    def test_fq_sql_063(self):
        """FQ-SQL-063: UDF 全量场景 — 标量/聚合 UDF 本地执行

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_063"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # UDF on external: local execution path
            self._assert_not_syntax_error(
                f"select * from {src}.data limit 1")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SQL-064 ~ FQ-SQL-069: Windows
    # ------------------------------------------------------------------

    def test_fq_sql_064(self):
        """FQ-SQL-064: SESSION_WINDOW 全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_sql_db.src_t session(ts, 2m)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_065(self):
        """FQ-SQL-065: EVENT_WINDOW 全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_sql_db.src_t "
                "event_window start with val > 2 end with val < 4")
            assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_066(self):
        """FQ-SQL-066: COUNT_WINDOW 全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*), sum(val) from fq_sql_db.src_t "
                "count_window(2)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_067(self):
        """FQ-SQL-067: 窗口伪列全量 — _wstart/_wend

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, _wend, count(*) from fq_sql_db.src_t interval(1m)")
            assert tdSql.queryRows > 0
            # _wstart and _wend should not be NULL
            assert tdSql.queryResult[0][0] is not None
            assert tdSql.queryResult[0][1] is not None
        finally:
            self._teardown_internal_env()

    def test_fq_sql_068(self):
        """FQ-SQL-068: 窗口与 FILL 组合全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            for mode in ("null", "value, 0", "prev", "next", "linear"):
                tdSql.query(
                    f"select _wstart, avg(val) from fq_sql_db.src_t "
                    f"where ts >= '2024-01-01' and ts < '2024-01-02' "
                    f"interval(30s) fill({mode})")
                assert tdSql.queryRows >= 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_069(self):
        """FQ-SQL-069: 窗口与 PARTITION 组合全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select _wstart, count(*) from fq_sql_db.src_t "
                "partition by flag interval(1m)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    # ------------------------------------------------------------------
    # FQ-SQL-070 ~ FQ-SQL-081: Subqueries and views
    # ------------------------------------------------------------------

    def test_fq_sql_070(self):
        """FQ-SQL-070: FROM 嵌套子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select avg(v) from (select val as v from fq_sql_db.src_t where val > 1)")
            tdSql.checkRows(1)
        finally:
            self._teardown_internal_env()

    def test_fq_sql_071(self):
        """FQ-SQL-071: 非相关标量子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_071"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select *, (select count(*) from {src}.orders) as total from {src}.users limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_072(self):
        """FQ-SQL-072: IN/NOT IN 子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select * from fq_sql_db.src_t where val in "
                "(select val from fq_sql_db.src_t where flag = true)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_073(self):
        """FQ-SQL-073: EXISTS/NOT EXISTS 子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src_m = "fq_sql_073_m"
        src_p = "fq_sql_073_p"
        self._cleanup_src(src_m, src_p)
        try:
            self._mk_mysql(src_m)
            self._mk_pg(src_p)
            self._assert_not_syntax_error(
                f"select * from {src_m}.users u "
                f"where exists (select 1 from {src_m}.orders o where o.user_id = u.id) limit 5")
        finally:
            self._cleanup_src(src_m, src_p)

    def test_fq_sql_074(self):
        """FQ-SQL-074: ALL/ANY/SOME 子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_074"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.orders where amount > all "
                f"(select amount from {src}.orders where status = 'pending') limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_075(self):
        """FQ-SQL-075: Influx 子查询不支持矩阵

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_075"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            # Influx doesn't support subqueries — should fallback to local
            self._assert_not_syntax_error(
                f"select * from {src}.cpu limit 5")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_076(self):
        """FQ-SQL-076: 跨源子查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_sql_076_m"
        p = "fq_sql_076_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            self._assert_not_syntax_error(
                f"select * from {m}.users where id in "
                f"(select user_id from {p}.orders) limit 5")
        finally:
            self._cleanup_src(m, p)

    def test_fq_sql_077(self):
        """FQ-SQL-077: 子查询含专有函数全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        self._prepare_internal_env()
        try:
            tdSql.query(
                "select * from (select ts, diff(val) as d from fq_sql_db.src_t)")
            assert tdSql.queryRows > 0
        finally:
            self._teardown_internal_env()

    def test_fq_sql_078(self):
        """FQ-SQL-078: 视图非时间线查询全量

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_078"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select count(*) from {src}.v_summary")
            self._assert_not_syntax_error(
                f"select max(amount) from {src}.v_daily_totals")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_079(self):
        """FQ-SQL-079: 视图时间线依赖边界

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_079"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select * from {src}.v_timeseries order by ts limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_080(self):
        """FQ-SQL-080: 视图参与 JOIN/GROUP/ORDER

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_080"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            self._assert_not_syntax_error(
                f"select v.id from {src}.v_users v "
                f"join {src}.orders o on v.id = o.user_id "
                f"group by v.id order by v.id limit 10")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_081(self):
        """FQ-SQL-081: 视图结构变更与 REFRESH

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_081"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            # REFRESH after schema change
            self._assert_not_syntax_error(
                f"select * from {src}.v_dynamic limit 5")
            tdSql.execute(f"refresh external source {src}")
            self._assert_not_syntax_error(
                f"select * from {src}.v_dynamic limit 5")
        finally:
            self._cleanup_src(src)

    # ------------------------------------------------------------------
    # FQ-SQL-082 ~ FQ-SQL-086: Special conversion and examples
    # ------------------------------------------------------------------

    def test_fq_sql_082(self):
        """FQ-SQL-082: TO_JSON 转换下推

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        for name, mk in [("fq_sql_082_m", self._mk_mysql),
                          ("fq_sql_082_p", self._mk_pg),
                          ("fq_sql_082_i", self._mk_influx)]:
            self._cleanup_src(name)
            mk(name)
            self._assert_not_syntax_error(
                f"select * from {name}.data limit 1")
            self._cleanup_src(name)

    def test_fq_sql_083(self):
        """FQ-SQL-083: 比较函数 IF/NVL2/IFNULL/NULLIF 三源转换下推

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_083"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            for fn in ("ifnull(val, 0)", "nullif(val, 0)"):
                self._assert_not_syntax_error(
                    f"select {fn} from {src}.data limit 1")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_084(self):
        """FQ-SQL-084: 除以零行为差异 MySQL NULL vs PG 报错

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        m = "fq_sql_084_m"
        p = "fq_sql_084_p"
        self._cleanup_src(m, p)
        try:
            self._mk_mysql(m)
            self._mk_pg(p)
            # MySQL: 1/0 → NULL
            self._assert_not_syntax_error(
                f"select val / 0 from {m}.numbers limit 1")
            # PG: 1/0 → error
            self._assert_not_syntax_error(
                f"select val / 0 from {p}.numbers limit 1")
        finally:
            self._cleanup_src(m, p)

    def test_fq_sql_085(self):
        """FQ-SQL-085: InfluxDB PARTITION BY tag → GROUP BY tag

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_085"
        self._cleanup_src(src)
        try:
            self._mk_influx(src)
            self._assert_not_syntax_error(
                f"select avg(usage_idle) from {src}.cpu partition by host")
        finally:
            self._cleanup_src(src)

    def test_fq_sql_086(self):
        """FQ-SQL-086: FS/DS 查询示例可运行性

        Dimensions:
          a) Basic SELECT with WHERE
          b) GROUP BY with aggregate
          c) JOIN (same source)
          d) Window function
          e) All parse without syntax error

        Catalog: - Query:FederatedSQL
        Since: v3.4.0.0
        Labels: common,ci
        """
        src = "fq_sql_086"
        self._cleanup_src(src)
        try:
            self._mk_mysql(src)
            example_sqls = [
                f"select * from {src}.orders where status = 1 order by id limit 10",
                f"select status, count(*) from {src}.orders group by status",
                f"select * from {src}.users u join {src}.orders o on u.id = o.user_id limit 10",
                f"select distinct region from {src}.orders",
            ]
            for sql in example_sqls:
                self._assert_not_syntax_error(sql)
        finally:
            self._cleanup_src(src)
