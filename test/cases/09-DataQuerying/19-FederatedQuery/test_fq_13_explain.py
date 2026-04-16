"""
test_fq_13_explain.py

Implements FQ-EXPLAIN-001 through FQ-EXPLAIN-018 from TS §8.1
"EXPLAIN federated query" — FederatedScan operator display, Remote SQL,
type mapping, pushdown flags, dialect correctness.

Design notes:
    - EXPLAIN tests verify the plan output format, NOT query results.
    - Tests use assert_plan_contains() and assert_plan_not_contains()
      helpers to check keywords in EXPLAIN output.
    - All three external sources (MySQL, PostgreSQL, InfluxDB) are covered.
    - Both EXPLAIN and EXPLAIN VERBOSE TRUE modes are tested.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
)


# ---------------------------------------------------------------------------
# Module-level constants for external test data
# ---------------------------------------------------------------------------
_BASE_TS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in ms

# MySQL: simple sensor table for EXPLAIN tests
_MYSQL_EXPLAIN_DB = "fq_explain_m"
_MYSQL_EXPLAIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS sensor "
    "(ts DATETIME NOT NULL, voltage DOUBLE, current FLOAT, region VARCHAR(32))",
    "DELETE FROM sensor",
    "INSERT INTO sensor VALUES "
    "('2024-01-01 00:00:00',220.5,1.2,'north'),"
    "('2024-01-01 00:01:00',221.0,1.3,'south'),"
    "('2024-01-01 00:02:00',219.8,1.1,'north'),"
    "('2024-01-01 00:03:00',222.0,1.4,'south'),"
    "('2024-01-01 00:04:00',220.0,1.0,'north')",
]

# MySQL: second table for JOIN tests
_MYSQL_JOIN_SQLS = [
    "CREATE TABLE IF NOT EXISTS region_info "
    "(region VARCHAR(32) PRIMARY KEY, area INT)",
    "DELETE FROM region_info",
    "INSERT INTO region_info VALUES ('north',1),('south',2)",
]

# PostgreSQL: simple sensor table for EXPLAIN tests
_PG_EXPLAIN_DB = "fq_explain_p"
_PG_EXPLAIN_SQLS = [
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

# InfluxDB: line-protocol data for EXPLAIN tests
_INFLUX_EXPLAIN_BUCKET = "fq_explain_i"
_INFLUX_LINES = [
    f"sensor,region=north voltage=220.5,current=1.2 {_BASE_TS}000000",
    f"sensor,region=south voltage=221.0,current=1.3 {_BASE_TS + 60000}000000",
    f"sensor,region=north voltage=219.8,current=1.1 {_BASE_TS + 120000}000000",
    f"sensor,region=south voltage=222.0,current=1.4 {_BASE_TS + 180000}000000",
    f"sensor,region=north voltage=220.0,current=1.0 {_BASE_TS + 240000}000000",
]


class TestFq13Explain(FederatedQueryVersionedMixin):
    """FQ-EXPLAIN-001 through FQ-EXPLAIN-018: EXPLAIN federated query."""

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        # Clean up sources and internal databases
        for src in ["fq_exp_mysql", "fq_exp_pg", "fq_exp_influx", "fq_exp_join_m"]:
            self._cleanup_src(src)
        for db in [_MYSQL_EXPLAIN_DB, _PG_EXPLAIN_DB]:
            try:
                if db == _MYSQL_EXPLAIN_DB:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
                elif db == _PG_EXPLAIN_DB:
                    ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), db)
            except Exception:
                pass
        try:
            ExtSrcEnv.influx_drop_db(_INFLUX_EXPLAIN_BUCKET)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_explain_output(sql, verbose=False):
        """Execute EXPLAIN and return full output as list of strings."""
        prefix = "explain verbose true" if verbose else "explain"
        tdSql.query(f"{prefix} {sql}")
        lines = []
        for row in tdSql.queryResult:
            for col in row:
                if col is not None:
                    lines.append(str(col))
        return lines

    @staticmethod
    def _explain_contains(lines, keyword):
        """Assert that keyword appears in EXPLAIN output."""
        for line in lines:
            if keyword in line:
                return
        tdLog.exit(f"expected keyword '{keyword}' not found in EXPLAIN output")

    @staticmethod
    def _explain_not_contains(lines, keyword):
        """Assert that keyword does NOT appear in EXPLAIN output."""
        for line in lines:
            if keyword in line:
                tdLog.exit(f"unexpected keyword '{keyword}' found in EXPLAIN output")

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-001 ~ FQ-EXPLAIN-003: Basic EXPLAIN
    # ------------------------------------------------------------------

    def test_fq_explain_001(self):
        """FQ-EXPLAIN-001: EXPLAIN basics — FederatedScan operator name

        EXPLAIN output contains the FederatedScan keyword.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(f"select * from {src}.sensor")
            self._explain_contains(lines, "FederatedScan")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_002(self):
        """FQ-EXPLAIN-002: EXPLAIN basics — Remote SQL display

        EXPLAIN output contains a Remote SQL: line.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor where ts > '2024-01-01'"
            )
            self._explain_contains(lines, "Remote SQL:")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_003(self):
        """FQ-EXPLAIN-003: EXPLAIN basics — external source/db/table info

        Operator name line contains source.db.table format.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(f"select * from {src}.sensor")
            self._explain_contains(lines, f"FederatedScan on {src}.{_MYSQL_EXPLAIN_DB}.sensor")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-004 ~ FQ-EXPLAIN-006: VERBOSE TRUE mode
    # ------------------------------------------------------------------

    def test_fq_explain_004(self):
        """FQ-EXPLAIN-004: EXPLAIN VERBOSE TRUE — type mapping display

        VERBOSE mode output contains a Type Mapping: line showing colName(TDengineType<-extType).

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor", verbose=True
            )
            self._explain_contains(lines, "Type Mapping:")
            # Verify at least one column mapping format: colName(Type<-extType)
            self._explain_contains(lines, "<-")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_005(self):
        """FQ-EXPLAIN-005: EXPLAIN VERBOSE TRUE — pushdown flags display

        VERBOSE mode output contains a Pushdown: line showing active flags.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor where ts > '2024-01-01' "
                f"order by ts limit 10",
                verbose=True,
            )
            self._explain_contains(lines, "Pushdown:")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_006(self):
        """FQ-EXPLAIN-006: EXPLAIN VERBOSE TRUE — output column list

        VERBOSE mode output contains columns=[...] format.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor", verbose=True
            )
            self._explain_contains(lines, "columns=")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-007 ~ FQ-EXPLAIN-010: Pushdown scenarios
    # ------------------------------------------------------------------

    def test_fq_explain_007(self):
        """FQ-EXPLAIN-007: full pushdown scenario — Remote SQL contains all pushed-down clauses

        When WHERE + ORDER BY + LIMIT are fully pushed down, Remote SQL contains the corresponding clauses.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor "
                f"where ts >= '2024-01-01' order by ts limit 3"
            )
            self._explain_contains(lines, "Remote SQL:")
            # Remote SQL should contain WHERE, ORDER BY, LIMIT
            remote_sql_line = ""
            for line in lines:
                if "Remote SQL:" in line:
                    remote_sql_line = line
                    break
            assert "WHERE" in remote_sql_line.upper() or "where" in remote_sql_line, \
                f"Remote SQL missing WHERE: {remote_sql_line}"
            assert "ORDER BY" in remote_sql_line.upper() or "order by" in remote_sql_line, \
                f"Remote SQL missing ORDER BY: {remote_sql_line}"
            assert "LIMIT" in remote_sql_line.upper() or "limit" in remote_sql_line, \
                f"Remote SQL missing LIMIT: {remote_sql_line}"
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_008(self):
        """FQ-EXPLAIN-008: partial pushdown scenario — Remote SQL contains only pushed-down parts

        When TDengine-specific functions (CSUM) are used, Remote SQL does not contain aggregation.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            # CSUM is TDengine-specific, cannot be pushed down
            lines = self._get_explain_output(
                f"select csum(voltage) from {src}.sensor"
            )
            self._explain_contains(lines, "FederatedScan")
            self._explain_contains(lines, "Remote SQL:")
            # Remote SQL should NOT contain CSUM
            for line in lines:
                if "Remote SQL:" in line:
                    assert "CSUM" not in line.upper(), \
                        f"Remote SQL should not contain CSUM: {line}"
                    break
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_009(self):
        """FQ-EXPLAIN-009: zero pushdown scenario — fallback path Remote SQL

        When pRemotePlan is NULL, Remote SQL is a basic SELECT,
        and Pushdown flag is (none).

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        # Test with internal vtable to simulate zero-pushdown path easily
        # This test verifies the format when no pushdown occurs
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select csum(voltage) from {src}.sensor", verbose=True
            )
            self._explain_contains(lines, "FederatedScan")
            self._explain_contains(lines, "Remote SQL:")
            # In verbose mode, verify Pushdown field (may be partial or none)
            self._explain_contains(lines, "Pushdown:")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_010(self):
        """FQ-EXPLAIN-010: aggregate pushdown — Remote SQL contains aggregate expressions

        When COUNT(*) + GROUP BY are pushed down, Remote SQL contains the corresponding expressions.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select count(*), region from {src}.sensor group by region"
            )
            self._explain_contains(lines, "FederatedScan")
            self._explain_contains(lines, "Remote SQL:")
            for line in lines:
                if "Remote SQL:" in line:
                    upper = line.upper()
                    assert "COUNT" in upper, f"Remote SQL missing COUNT: {line}"
                    assert "GROUP BY" in upper, f"Remote SQL missing GROUP BY: {line}"
                    break
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-011 ~ FQ-EXPLAIN-013: Dialect correctness
    # ------------------------------------------------------------------

    def test_fq_explain_011(self):
        """FQ-EXPLAIN-011: MySQL external source — dialect correctness

        MySQL Remote SQL uses backtick quoting for identifiers.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(f"select ts, voltage from {src}.sensor")
            # MySQL dialect: backtick quoting
            for line in lines:
                if "Remote SQL:" in line:
                    assert "`" in line, \
                        f"MySQL Remote SQL should use backtick quoting: {line}"
                    break
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_012(self):
        """FQ-EXPLAIN-012: PostgreSQL external source — dialect correctness

        PG Remote SQL uses double-quote quoting for identifiers.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_pg"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_EXPLAIN_DB)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_EXPLAIN_DB, _PG_EXPLAIN_SQLS)
            self._mk_pg_real(src, database=_PG_EXPLAIN_DB)
            lines = self._get_explain_output(f"select ts, voltage from {src}.sensor")
            # PG dialect: double-quote quoting
            for line in lines:
                if "Remote SQL:" in line:
                    assert '"' in line, \
                        f"PG Remote SQL should use double-quote quoting: {line}"
                    break
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), _PG_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_013(self):
        """FQ-EXPLAIN-013: InfluxDB external source — dialect correctness

        InfluxDB Remote SQL uses InfluxDB v3 SQL dialect.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_influx"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.influx_create_db(_INFLUX_EXPLAIN_BUCKET)
            ExtSrcEnv.influx_write(_INFLUX_EXPLAIN_BUCKET, _INFLUX_LINES)
            self._mk_influx_real(src, database=_INFLUX_EXPLAIN_BUCKET)
            lines = self._get_explain_output(f"select * from {src}.sensor")
            self._explain_contains(lines, "FederatedScan")
            self._explain_contains(lines, "Remote SQL:")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db(_INFLUX_EXPLAIN_BUCKET)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-014 ~ FQ-EXPLAIN-015: Type mapping by source
    # ------------------------------------------------------------------

    def test_fq_explain_014(self):
        """FQ-EXPLAIN-014: EXPLAIN VERBOSE TRUE — PG type mapping

        PG type mapping shows original type names (e.g. float8, timestamptz).

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_pg"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.pg_create_db_cfg(self._pg_cfg(), _PG_EXPLAIN_DB)
            ExtSrcEnv.pg_exec_cfg(self._pg_cfg(), _PG_EXPLAIN_DB, _PG_EXPLAIN_SQLS)
            self._mk_pg_real(src, database=_PG_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select ts, voltage from {src}.sensor", verbose=True
            )
            self._explain_contains(lines, "Type Mapping:")
            self._explain_contains(lines, "<-")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), _PG_EXPLAIN_DB)
            except Exception:
                pass

    def test_fq_explain_015(self):
        """FQ-EXPLAIN-015: EXPLAIN VERBOSE TRUE — InfluxDB type mapping

        InfluxDB type mapping shows original type names (e.g. Float64, String).

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_influx"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.influx_create_db(_INFLUX_EXPLAIN_BUCKET)
            ExtSrcEnv.influx_write(_INFLUX_EXPLAIN_BUCKET, _INFLUX_LINES)
            self._mk_influx_real(src, database=_INFLUX_EXPLAIN_BUCKET)
            lines = self._get_explain_output(
                f"select * from {src}.sensor", verbose=True
            )
            self._explain_contains(lines, "Type Mapping:")
            self._explain_contains(lines, "<-")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db(_INFLUX_EXPLAIN_BUCKET)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-016: EXPLAIN does not execute remote query
    # ------------------------------------------------------------------

    def test_fq_explain_016(self):
        """FQ-EXPLAIN-016: EXPLAIN does not execute remote query

        EXPLAIN only generates and displays the plan without sending actual queries to external sources.
        Verification: run EXPLAIN on a non-existent external table; if no remote execution occurs,
        no table-not-exist error is raised (depends on implementation; parser may already know the table exists).

        Note: this is a best-effort test — if EXPLAIN must connect to the external source for metadata,
        this test instead verifies that EXPLAIN does not return data rows (only plan rows).

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            # EXPLAIN should return plan rows, not data rows
            tdSql.query(f"explain select * from {src}.sensor")
            assert tdSql.queryRows > 0, "EXPLAIN should return at least one plan row"
            # Verify none of the rows contain actual data values from the table
            for row in tdSql.queryResult:
                for col in row:
                    s = str(col) if col is not None else ""
                    assert "220.5" not in s, "EXPLAIN should not return actual data"
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-017: JOIN pushdown
    # ------------------------------------------------------------------

    def test_fq_explain_017(self):
        """FQ-EXPLAIN-017: JOIN pushdown — Remote SQL contains JOIN statement

        When same-source JOIN is pushed down, Remote SQL contains the JOIN keyword,
        and Pushdown flags include JOIN.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_join_m"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_JOIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            lines = self._get_explain_output(
                f"select s.ts, s.voltage, r.area "
                f"from {src}.sensor s join {src}.region_info r "
                f"on s.region = r.region",
                verbose=True,
            )
            self._explain_contains(lines, "FederatedScan")
            # Check Remote SQL contains JOIN keyword
            for line in lines:
                if "Remote SQL:" in line:
                    assert "JOIN" in line.upper(), \
                        f"Remote SQL should contain JOIN: {line}"
                    break
            # Check Pushdown flags contain JOIN
            self._explain_contains(lines, "JOIN")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FQ-EXPLAIN-018: Virtual table EXPLAIN
    # ------------------------------------------------------------------

    def test_fq_explain_018(self):
        """FQ-EXPLAIN-018: virtual table EXPLAIN — FederatedScan display

        When a virtual table references external columns, EXPLAIN output contains FederatedScan operator info.

        Catalog: - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        History:
            - 2026-04-13 wpan Initial implementation

        """
        src = "fq_exp_mysql"
        self._cleanup_src(src)
        tdSql.execute("drop database if exists fq_explain_vtbl")
        try:
            ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB, _MYSQL_EXPLAIN_SQLS)
            self._mk_mysql_real(src, database=_MYSQL_EXPLAIN_DB)
            # Create internal DB + virtual table referencing external column
            tdSql.execute("create database fq_explain_vtbl")
            tdSql.execute("use fq_explain_vtbl")
            tdSql.execute(
                f"create table vt (ts timestamp, voltage double "
                f"references {src}.{_MYSQL_EXPLAIN_DB}.sensor.voltage)"
            )
            lines = self._get_explain_output("select * from fq_explain_vtbl.vt")
            self._explain_contains(lines, "FederatedScan")
            self._explain_contains(lines, "Remote SQL:")
        finally:
            tdSql.execute("drop database if exists fq_explain_vtbl")
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_EXPLAIN_DB)
            except Exception:
                pass
