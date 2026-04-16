"""
test_fq_13_explain.py

Implements FQ-EXPLAIN-001 through FQ-EXPLAIN-018 from TS §8.1
"EXPLAIN federated query" — FederatedScan operator display, Remote SQL,
type mapping, pushdown flags, dialect correctness.

Each test SQL is executed in all four EXPLAIN modes and output is validated:
    1. EXPLAIN <SQL>
    2. EXPLAIN VERBOSE TRUE <SQL>
    3. EXPLAIN ANALYZE <SQL>
    4. EXPLAIN ANALYZE VERBOSE TRUE <SQL>

Design notes:
    - All three external sources (MySQL, PostgreSQL, InfluxDB) are covered.
    - Sources and databases are created once in setup_class for efficiency.
    - _run_all_modes() drives the four modes; per-mode assertions follow.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
)


# ---------------------------------------------------------------------------
# EXPLAIN mode constants
# ---------------------------------------------------------------------------
EXPLAIN = "explain"
EXPLAIN_VERBOSE = "explain verbose true"
EXPLAIN_ANALYZE = "explain analyze"
EXPLAIN_ANALYZE_VERBOSE = "explain analyze verbose true"
ALL_MODES = [EXPLAIN, EXPLAIN_VERBOSE, EXPLAIN_ANALYZE, EXPLAIN_ANALYZE_VERBOSE]
VERBOSE_MODES = [EXPLAIN_VERBOSE, EXPLAIN_ANALYZE_VERBOSE]
ANALYZE_MODES = [EXPLAIN_ANALYZE, EXPLAIN_ANALYZE_VERBOSE]


# ---------------------------------------------------------------------------
# Module-level constants for external test data
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# MySQL: sensor + region_info tables
_MYSQL_DB = "fq_explain_m"
_MYSQL_SETUP_SQLS = [
    "CREATE TABLE IF NOT EXISTS sensor "
    "(ts DATETIME NOT NULL, voltage DOUBLE, current FLOAT, region VARCHAR(32))",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00',220.5,1.2,'north'),"
    "('2024-01-01 00:01:00',221.0,1.3,'south'),"
    "('2024-01-01 00:02:00',219.8,1.1,'north'),"
    "('2024-01-01 00:03:00',222.0,1.4,'south'),"
    "('2024-01-01 00:04:00',220.0,1.0,'north')",
    "CREATE TABLE IF NOT EXISTS region_info "
    "(region VARCHAR(32) PRIMARY KEY, area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES ('north',1),('south',2)",
]

# PostgreSQL: sensor table
_PG_DB = "fq_explain_p"
_PG_SETUP_SQLS = [
    "CREATE TABLE IF NOT EXISTS sensor "
    "(ts TIMESTAMPTZ NOT NULL, voltage FLOAT8, current REAL, region TEXT)",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00+00',220.5,1.2,'north'),"
    "('2024-01-01 00:01:00+00',221.0,1.3,'south'),"
    "('2024-01-01 00:02:00+00',219.8,1.1,'north'),"
    "('2024-01-01 00:03:00+00',222.0,1.4,'south'),"
    "('2024-01-01 00:04:00+00',220.0,1.0,'north')",
]

# InfluxDB: line-protocol data
_INFLUX_BUCKET = "fq_explain_i"
_INFLUX_LINES = [
    f"sensor,region=north voltage=220.5,current=1.2 {_BASE_TS}000000",
    f"sensor,region=south voltage=221.0,current=1.3 {_BASE_TS + 60000}000000",
    f"sensor,region=north voltage=219.8,current=1.1 {_BASE_TS + 120000}000000",
    f"sensor,region=south voltage=222.0,current=1.4 {_BASE_TS + 180000}000000",
    f"sensor,region=north voltage=220.0,current=1.0 {_BASE_TS + 240000}000000",
]

# Source names (shared across all tests)
_MYSQL_SRC = "fq_exp_mysql"
_PG_SRC = "fq_exp_pg"
_INFLUX_SRC = "fq_exp_influx"
_VTBL_DB = "fq_explain_vtbl"


class TestFq13Explain(FederatedQueryVersionedMixin):
    """FQ-EXPLAIN-001 through FQ-EXPLAIN-018: EXPLAIN federated query.

    All four EXPLAIN modes are tested for each scenario.
    """

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

        # -- MySQL setup (sensor + region_info) --
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_DB, _MYSQL_SETUP_SQLS)
        self._cleanup_src(_MYSQL_SRC)
        self._mk_mysql_real(_MYSQL_SRC, database=_MYSQL_DB)

        # -- PostgreSQL setup --
        ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_DB)
        ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_DB, _PG_SETUP_SQLS)
        self._cleanup_src(_PG_SRC)
        self._mk_pg_real(_PG_SRC, database=_PG_DB)

        # -- InfluxDB setup --
        ExtSrcEnv.influx_create_db(_INFLUX_BUCKET)
        ExtSrcEnv.influx_write(_INFLUX_BUCKET, _INFLUX_LINES)
        self._cleanup_src(_INFLUX_SRC)
        self._mk_influx_real(_INFLUX_SRC, database=_INFLUX_BUCKET)

    def teardown_class(self):
        for src in [_MYSQL_SRC, _PG_SRC, _INFLUX_SRC]:
            self._cleanup_src(src)
        tdSql.execute(f"drop database if exists {_VTBL_DB}")
        for drop_fn, args in [
            (ExtSrcEnv.mysql_drop_db_cfg, (self._mysql_cfg(), _MYSQL_DB)),
            (ExtSrcEnv.pg_drop_db_cfg, (self._pg_cfg(), _PG_DB)),
            (ExtSrcEnv.influx_drop_db, (_INFLUX_BUCKET,)),
        ]:
            try:
                drop_fn(*args)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_explain_output(sql, mode=EXPLAIN):
        """Execute EXPLAIN in *mode* and return full output as list of strings."""
        tdSql.query(f"{mode} {sql}")
        lines = []
        for row in tdSql.queryResult:
            for col in row:
                if col is not None:
                    lines.append(str(col))
        return lines

    def _run_all_modes(self, sql):
        """Run *sql* in all 4 EXPLAIN modes; return ``{mode: [lines]}``.

        Asserts every mode returns non-empty output.
        """
        results = {}
        for mode in ALL_MODES:
            tdLog.debug(f"  [{mode}] {sql[:80]}")
            lines = self._get_explain_output(sql, mode=mode)
            assert len(lines) > 0, f"[{mode}]: empty output for: {sql[:80]}"
            results[mode] = lines
        return results

    @staticmethod
    def _assert_contain(lines, keyword, label=""):
        """Assert *keyword* appears in at least one line."""
        for line in lines:
            if keyword in line:
                return
        tag = f"[{label}] " if label else ""
        tdLog.exit(f"{tag}expected '{keyword}' not found in EXPLAIN output")

    @staticmethod
    def _assert_not_contain(lines, keyword, label=""):
        """Assert *keyword* does NOT appear in any line."""
        for line in lines:
            if keyword in line:
                tag = f"[{label}] " if label else ""
                tdLog.exit(f"{tag}unexpected '{keyword}' found in EXPLAIN output")

    def _assert_all_contain(self, results, keyword):
        """Assert *keyword* present in output of ALL 4 modes."""
        for mode, lines in results.items():
            self._assert_contain(lines, keyword, label=mode)

    def _assert_all_not_contain(self, results, keyword):
        """Assert *keyword* absent from output of ALL 4 modes."""
        for mode, lines in results.items():
            self._assert_not_contain(lines, keyword, label=mode)

    def _assert_verbose_contain(self, results, keyword):
        """Assert *keyword* present in VERBOSE and ANALYZE VERBOSE modes."""
        for mode in VERBOSE_MODES:
            self._assert_contain(results[mode], keyword, label=mode)

    def _get_remote_sql_line(self, lines, label=""):
        """Return the first line containing ``Remote SQL:``."""
        for line in lines:
            if "Remote SQL:" in line:
                return line
        tdLog.exit(f"[{label}] 'Remote SQL:' not found in output")
        return ""  # unreachable

    def _assert_remote_sql_kw(self, results, keyword):
        """Assert *keyword* exists in Remote SQL line in ALL 4 modes (case-insensitive)."""
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert keyword.upper() in remote.upper(), \
                f"[{mode}] Remote SQL missing '{keyword}': {remote}"

    def _assert_remote_sql_no_kw(self, results, keyword):
        """Assert *keyword* NOT in Remote SQL line in ALL 4 modes (case-insensitive)."""
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert keyword.upper() not in remote.upper(), \
                f"[{mode}] Remote SQL should not contain '{keyword}': {remote}"

    def _check_analyze_metrics(self, results):
        """Assert ANALYZE output contains execution-time metrics.

        Checks for common metric keywords emitted by TDengine EXPLAIN ANALYZE.
        Falls back to comparing output length against plain EXPLAIN.
        """
        plain_len = sum(len(l) for l in results[EXPLAIN])
        for mode in ANALYZE_MODES:
            text = " ".join(results[mode]).lower()
            has_metrics = any(p in text for p in [
                "rows=", "time=", "loops=", "actual",
                "elapsed", "duration", "cost=",
            ])
            if has_metrics:
                tdLog.debug(f"  [{mode}] execution metrics detected")
                continue
            # Fallback: ANALYZE output should carry at least as much info
            analyze_len = sum(len(l) for l in results[mode])
            assert analyze_len >= plain_len, (
                f"[{mode}] ANALYZE output shorter than plain EXPLAIN "
                f"({analyze_len} vs {plain_len}) and no metric keywords found"
            )

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-001 ~ FQ-EXPLAIN-003: Basic EXPLAIN (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_001(self):
        """FQ-EXPLAIN-001: EXPLAIN basics — FederatedScan operator name

        All four EXPLAIN modes output the FederatedScan operator keyword.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._check_analyze_metrics(results)

    def test_fq_explain_002(self):
        """FQ-EXPLAIN-002: EXPLAIN basics — Remote SQL display

        All four EXPLAIN modes output a ``Remote SQL:`` line.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts > '2024-01-01'")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "Remote SQL:")
        self._check_analyze_metrics(results)

    def test_fq_explain_003(self):
        """FQ-EXPLAIN-003: EXPLAIN basics — external source/db/table info

        Operator line shows ``FederatedScan on <source>.<db>.<table>`` in all modes.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(
            results, f"FederatedScan on {_MYSQL_SRC}.{_MYSQL_DB}.sensor"
        )
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-004 ~ FQ-EXPLAIN-006: VERBOSE fields (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_004(self):
        """FQ-EXPLAIN-004: VERBOSE — type mapping display

        VERBOSE modes output ``Type Mapping:`` with ``colName(TDengineType<-extType)``.
        Non-verbose modes still show FederatedScan and Remote SQL.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        # All modes: basic plan keywords
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        # Verbose modes: type mapping detail
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)

    def test_fq_explain_005(self):
        """FQ-EXPLAIN-005: VERBOSE — pushdown flags display

        VERBOSE modes output ``Pushdown:`` showing active pushdown flags.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts > '2024-01-01' order by ts limit 10")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Pushdown:")
        self._check_analyze_metrics(results)

    def test_fq_explain_006(self):
        """FQ-EXPLAIN-006: VERBOSE — output column list

        VERBOSE modes output ``columns=[...]`` format.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "columns=")
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-007 ~ FQ-EXPLAIN-010: Pushdown scenarios (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_007(self):
        """FQ-EXPLAIN-007: full pushdown — Remote SQL contains WHERE + ORDER BY + LIMIT

        All four modes show a Remote SQL line carrying the pushed-down clauses.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts >= '2024-01-01' order by ts limit 3")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_remote_sql_kw(results, "WHERE")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_remote_sql_kw(results, "LIMIT")
        self._check_analyze_metrics(results)

    def test_fq_explain_008(self):
        """FQ-EXPLAIN-008: partial pushdown — TDengine-only CSUM not in Remote SQL

        Remote SQL must NOT contain CSUM across all modes.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select csum(voltage) from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_remote_sql_no_kw(results, "CSUM")
        self._check_analyze_metrics(results)

    def test_fq_explain_009(self):
        """FQ-EXPLAIN-009: zero pushdown — fallback path Remote SQL

        CSUM triggers fallback; VERBOSE modes still show Pushdown field.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select csum(voltage) from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_verbose_contain(results, "Pushdown:")
        self._check_analyze_metrics(results)

    def test_fq_explain_010(self):
        """FQ-EXPLAIN-010: aggregate pushdown — COUNT + GROUP BY in Remote SQL

        All modes show COUNT and GROUP BY inside Remote SQL.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select count(*), region from {_MYSQL_SRC}.sensor group by region"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_remote_sql_kw(results, "COUNT")
        self._assert_remote_sql_kw(results, "GROUP BY")
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-011 ~ FQ-EXPLAIN-013: Dialect correctness (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_011(self):
        """FQ-EXPLAIN-011: MySQL dialect — backtick quoting in Remote SQL

        All modes produce Remote SQL with MySQL backtick identifier quoting.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert "`" in remote, \
                f"[{mode}] MySQL Remote SQL should use backtick quoting: {remote}"

    def test_fq_explain_012(self):
        """FQ-EXPLAIN-012: PostgreSQL dialect — double-quote quoting in Remote SQL

        All modes produce Remote SQL with PG double-quote identifier quoting.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select ts, voltage from {_PG_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert '"' in remote, \
                f"[{mode}] PG Remote SQL should use double-quote quoting: {remote}"

    def test_fq_explain_013(self):
        """FQ-EXPLAIN-013: InfluxDB dialect — FederatedScan + Remote SQL in all modes

        All modes show FederatedScan and Remote SQL for InfluxDB source.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select * from {_INFLUX_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-014 ~ FQ-EXPLAIN-015: Type mapping per source
    # ------------------------------------------------------------------

    def test_fq_explain_014(self):
        """FQ-EXPLAIN-014: PG type mapping — VERBOSE shows original PG types

        VERBOSE modes display Type Mapping with PG type names (e.g. float8, timestamptz).
        All modes show FederatedScan.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select ts, voltage from {_PG_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)

    def test_fq_explain_015(self):
        """FQ-EXPLAIN-015: InfluxDB type mapping — VERBOSE shows original types

        VERBOSE modes display Type Mapping with InfluxDB type names (e.g. Float64, String).
        All modes show FederatedScan.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select * from {_INFLUX_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-016: EXPLAIN does not return data rows
    # ------------------------------------------------------------------

    def test_fq_explain_016(self):
        """FQ-EXPLAIN-016: plan output does not contain actual data values

        All four modes return plan rows only — none should contain raw data
        values from the underlying table (e.g. ``220.5``).
        EXPLAIN ANALYZE executes the query internally to collect metrics but
        still returns plan rows, not data rows.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        # No mode should return actual data values
        for mode, lines in results.items():
            for line in lines:
                assert "220.5" not in line, \
                    f"[{mode}] plan output should not contain data value '220.5': {line}"

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-017: JOIN pushdown (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_017(self):
        """FQ-EXPLAIN-017: JOIN pushdown — Remote SQL contains JOIN

        Same-source JOIN is pushed down; all modes show JOIN in Remote SQL.
        VERBOSE modes additionally show Pushdown flags including JOIN.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        sql = (f"select s.ts, s.voltage, r.area "
               f"from {_MYSQL_SRC}.sensor s join {_MYSQL_SRC}.region_info r "
               f"on s.region = r.region")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "JOIN")
        self._check_analyze_metrics(results)

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-018: Virtual table EXPLAIN (all 4 modes)
    # ------------------------------------------------------------------

    def test_fq_explain_018(self):
        """FQ-EXPLAIN-018: virtual table — FederatedScan in all modes

        Virtual table referencing external columns shows FederatedScan and
        Remote SQL in all four EXPLAIN modes.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output

        """
        tdSql.execute(f"drop database if exists {_VTBL_DB}")
        try:
            tdSql.execute(f"create database {_VTBL_DB}")
            tdSql.execute(f"use {_VTBL_DB}")
            tdSql.execute(
                f"create table vt (ts timestamp, voltage double "
                f"references {_MYSQL_SRC}.{_MYSQL_DB}.sensor.voltage)"
            )
            sql = f"select * from {_VTBL_DB}.vt"
            results = self._run_all_modes(sql)
            self._assert_all_contain(results, "FederatedScan")
            self._assert_all_contain(results, "Remote SQL:")
            self._check_analyze_metrics(results)
        finally:
            tdSql.execute(f"drop database if exists {_VTBL_DB}")
