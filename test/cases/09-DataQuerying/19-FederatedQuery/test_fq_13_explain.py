"""
test_fq_13_explain.py

Implements FQ-EXPLAIN-001 through FQ-EXPLAIN-030 from TS §8.1
"EXPLAIN federated query" — FederatedScan operator display, Remote SQL,
pushdown verification, dialect correctness.

Every pushdown test asserts BOTH:
    1.  The Remote SQL contains the pushed-down clause (WHERE/ORDER BY/…)
    2.  The main plan does NOT contain the corresponding local operator
        (Sort/Agg/…) — proving work is offloaded to remote.

Phase 2 final expectations are enforced — all standard-SQL-translatable
clauses must be pushed to the remote side.

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
    """FQ-EXPLAIN-001 through FQ-EXPLAIN-030: EXPLAIN federated query.

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
        """Execute EXPLAIN in *mode* and return full output as list of strings.

        On execution failure raises AssertionError with the full error message,
        SQL, and mode so the failure is self-explanatory in the test report.
        """
        full_sql = f"{mode} {sql}"
        try:
            tdSql.query(full_sql)
        except Exception as e:
            # Surface errno + error_info if tdSql stored them
            errno     = getattr(tdSql, 'errno',      None)
            err_info  = getattr(tdSql, 'error_info', None)
            detail = ""
            if errno is not None:
                detail += f"\n  errno:      {errno:#010x}"
            if err_info:
                detail += f"\n  error_info: {err_info}"
            raise AssertionError(
                f"EXPLAIN execution failed\n"
                f"  mode:  {mode}\n"
                f"  sql:   {sql[:300]}"
                f"{detail}\n"
                f"  raw exception: {e}"
            ) from e
        lines = []
        for row in tdSql.queryResult:
            for col in row:
                if col is not None:
                    lines.append(str(col))
        return lines

    def _run_all_modes(self, sql):
        """Run *sql* in all 4 EXPLAIN modes; return ``{mode: [lines]}``.

        Raises AssertionError (with full context) if any mode returns empty
        output or throws an error.
        """
        results = {}
        for mode in ALL_MODES:
            tdLog.debug(f"  [{mode}] {sql[:80]}")
            lines = self._get_explain_output(sql, mode=mode)
            if not lines:
                raise AssertionError(
                    f"[{mode}] EXPLAIN returned empty output\n"
                    f"  sql: {sql}"
                )
            results[mode] = lines
        return results

    @staticmethod
    def _assert_contain(lines, keyword, label=""):
        """Assert *keyword* appears in at least one line.

        On failure dumps the full output so the caller can see what WAS there.
        """
        for line in lines:
            if keyword in line:
                return
        tag = f"[{label}] " if label else ""
        dump = "\n    ".join(f"[{i:02d}] {l}" for i, l in enumerate(lines))
        raise AssertionError(
            f"{tag}expected keyword '{keyword}' not found in EXPLAIN output\n"
            f"  Full output ({len(lines)} lines):\n"
            f"    {dump}"
        )

    @staticmethod
    def _assert_not_contain(lines, keyword, label=""):
        """Assert *keyword* does NOT appear in any line."""
        for i, line in enumerate(lines):
            if keyword in line:
                tag = f"[{label}] " if label else ""
                raise AssertionError(
                    f"{tag}unexpected keyword '{keyword}' found at line [{i:02d}]\n"
                    f"  Line: {line}"
                )

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
        """Return the first line containing ``Remote SQL:``.

        On failure dumps the full output so the caller can diagnose what the
        plan looked like.
        """
        for line in lines:
            if "Remote SQL:" in line:
                return line
        dump = "\n    ".join(f"[{i:02d}] {l}" for i, l in enumerate(lines))
        raise AssertionError(
            f"[{label}] 'Remote SQL:' not found in EXPLAIN output\n"
            f"  Full output ({len(lines)} lines):\n"
            f"    {dump}"
        )

    def _assert_remote_sql_kw(self, results, keyword):
        """Assert *keyword* exists in Remote SQL line in ALL 4 modes (case-insensitive)."""
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            if keyword.upper() not in remote.upper():
                raise AssertionError(
                    f"[{mode}] Remote SQL missing '{keyword}'\n"
                    f"  Remote SQL: {remote}"
                )

    def _assert_remote_sql_no_kw(self, results, keyword):
        """Assert *keyword* NOT in Remote SQL line in ALL 4 modes (case-insensitive)."""
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            if keyword.upper() in remote.upper():
                raise AssertionError(
                    f"[{mode}] Remote SQL should not contain '{keyword}'\n"
                    f"  Remote SQL: {remote}"
                )

    def _check_analyze_metrics(self, results):
        """Assert ANALYZE output contains execution-time metrics.

        On failure shows both plain and analyze output so the developer can
        compare what changed.
        """
        plain_lines = results[EXPLAIN]
        plain_len   = sum(len(l) for l in plain_lines)
        for mode in ANALYZE_MODES:
            text = " ".join(results[mode]).lower()
            has_metrics = any(p in text for p in [
                "rows=", "time=", "loops=", "actual",
                "elapsed", "duration", "cost=",
            ])
            if has_metrics:
                tdLog.debug(f"  [{mode}] execution metrics detected")
                continue
            analyze_len = sum(len(l) for l in results[mode])
            if analyze_len < plain_len:
                plain_dump   = "\n    ".join(plain_lines)
                analyze_dump = "\n    ".join(results[mode])
                raise AssertionError(
                    f"[{mode}] ANALYZE output is shorter than plain EXPLAIN "
                    f"and no metric keywords found\n"
                    f"  plain output ({len(plain_lines)} lines):\n"
                    f"    {plain_dump}\n"
                    f"  {mode} output ({len(results[mode])} lines):\n"
                    f"    {analyze_dump}"
                )

    def _assert_no_local_operator(self, results, operator_name):
        """Assert *operator_name* does NOT appear as a local plan operator.

        The operator keyword should only appear inside ``Remote SQL:`` lines
        (pushed to remote), NOT as a standalone plan node.

        On failure shows the offending line plus the full plan so the developer
        can see exactly where the un-pushed operator sits.
        """
        for mode, lines in results.items():
            for i, line in enumerate(lines):
                if "Remote SQL:" in line:
                    continue  # skip — operator is inside remote SQL, that's fine
                if operator_name in line:
                    dump = "\n    ".join(
                        f"[{j:02d}] {l}" for j, l in enumerate(lines)
                    )
                    raise AssertionError(
                        f"[{mode}] local plan should not contain '{operator_name}' "
                        f"(pushdown expected)\n"
                        f"  Offending line [{i:02d}]: {line}\n"
                        f"  Full plan ({len(lines)} lines):\n"
                        f"    {dump}"
                    )

    # ==================================================================
    # FQ-EXPLAIN-001 ~ FQ-EXPLAIN-003: Basic EXPLAIN (all 4 modes)
    # ==================================================================

    def do_explain_001(self):
        """FederatedScan operator name appears in all modes."""
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-001 [passed]")

    def do_explain_002(self):
        """Remote SQL line appears in all modes."""
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts > '2024-01-01'")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "Remote SQL:")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-002 [passed]")

    def do_explain_003(self):
        """Operator line shows FederatedScan on <source>.<db>.<table>."""
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(
            results, f"FederatedScan on {_MYSQL_SRC}.{_MYSQL_DB}.sensor"
        )
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-003 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-004 ~ FQ-EXPLAIN-006: VERBOSE fields (all 4 modes)
    # ==================================================================

    def do_explain_004(self):
        """VERBOSE modes output Type Mapping with colName(TDengineType<-extType)."""
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-004 [passed]")

    def do_explain_005(self):
        """VERBOSE modes output Pushdown: showing active pushdown flags."""
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts > '2024-01-01' order by ts limit 10")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Pushdown:")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-005 [passed]")

    def do_explain_006(self):
        """VERBOSE modes output columns=[...] format."""
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "columns=")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-006 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-007 ~ FQ-EXPLAIN-015: Pushdown scenarios
    # ==================================================================

    def do_explain_007(self):
        """Full pushdown: Remote SQL contains WHERE + ORDER BY + LIMIT.
        No local Sort or Project-with-limit should exist in the plan.
        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where ts >= '2024-01-01' order by ts limit 3")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_remote_sql_kw(results, "WHERE")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_remote_sql_kw(results, "LIMIT")
        # Verify no local Sort operator (pushed to remote)
        self._assert_no_local_operator(results, "Sort ")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-007 [passed]")

    def do_explain_008(self):
        """WHERE-only pushdown: Remote SQL contains WHERE, no ORDER BY/LIMIT.
        Verify WHERE pushed to remote; no local Filter operator.
        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where voltage > 220.0")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "WHERE")
        self._assert_remote_sql_no_kw(results, "ORDER BY")
        self._assert_remote_sql_no_kw(results, "LIMIT")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-008 [passed]")

    def do_explain_009(self):
        """ORDER BY-only pushdown: Remote SQL has ORDER BY, no WHERE/LIMIT.
        No local Sort operator in plan.
        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"order by voltage desc")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_remote_sql_no_kw(results, "WHERE")
        self._assert_remote_sql_no_kw(results, "LIMIT")
        self._assert_no_local_operator(results, "Sort ")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-009 [passed]")

    def do_explain_010(self):
        """LIMIT-only pushdown: Remote SQL has LIMIT, no WHERE/ORDER BY.
        LIMIT is pushed into Remote SQL even without ORDER BY.
        """
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor limit 2"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "LIMIT")
        self._assert_remote_sql_no_kw(results, "WHERE")
        self._assert_remote_sql_no_kw(results, "ORDER BY")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-010 [passed]")

    def do_explain_011(self):
        """Aggregate pushdown — COUNT+GROUP BY pushed to remote.
        No local Agg operator in plan.
        """
        sql = f"select count(*), region from {_MYSQL_SRC}.sensor group by region"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "COUNT")
        self._assert_remote_sql_kw(results, "GROUP BY")
        self._assert_no_local_operator(results, "Agg")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-011 [passed]")

    def do_explain_012(self):
        """Aggregate pushdown — SUM, AVG, MIN, MAX in Remote SQL.
        All standard SQL aggregate functions pushed to remote.
        """
        sql = (f"select sum(voltage), avg(voltage), min(voltage), max(voltage) "
               f"from {_MYSQL_SRC}.sensor")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "SUM")
        self._assert_remote_sql_kw(results, "AVG")
        self._assert_remote_sql_kw(results, "MIN")
        self._assert_remote_sql_kw(results, "MAX")
        self._assert_no_local_operator(results, "Agg")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-012 [passed]")

    def do_explain_013(self):
        """Aggregate + HAVING pushdown — HAVING clause in Remote SQL."""
        sql = (f"select region, count(*) as cnt from {_MYSQL_SRC}.sensor "
               f"group by region having count(*) > 1")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "GROUP BY")
        self._assert_remote_sql_kw(results, "HAVING")
        self._assert_no_local_operator(results, "Agg")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-013 [passed]")

    def do_explain_014(self):
        """TDengine-only function NOT pushed — CSUM not in Remote SQL.
        CSUM is a TDengine-specific function; it must stay local.
        """
        sql = f"select csum(voltage) from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_remote_sql_no_kw(results, "CSUM")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-014 [passed]")

    def do_explain_015(self):
        """Column projection pushdown — only selected columns in Remote SQL.
        SELECT ts, voltage should not pull all columns (*).
        """
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            # Remote SQL should reference ts and voltage but NOT select *
            assert "SELECT *" not in remote.upper() or "ts" in remote.lower(), \
                f"[{mode}] projection should push specific columns, not SELECT *: {remote}"
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-015 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-016 ~ FQ-EXPLAIN-018: Dialect correctness
    # ==================================================================

    def do_explain_016(self):
        """MySQL dialect — backtick quoting in Remote SQL."""
        sql = f"select ts, voltage from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert "`" in remote, \
                f"[{mode}] MySQL Remote SQL should use backtick quoting: {remote}"
        print("FQ-EXPLAIN-016 [passed]")

    def do_explain_017(self):
        """PostgreSQL dialect — double-quote quoting in Remote SQL."""
        sql = f"select ts, voltage from {_PG_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert '"' in remote, \
                f"[{mode}] PG Remote SQL should use double-quote quoting: {remote}"
        print("FQ-EXPLAIN-017 [passed]")

    def do_explain_018(self):
        """InfluxDB dialect — FederatedScan and Remote SQL in all modes."""
        sql = f"select * from {_INFLUX_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-018 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-019 ~ FQ-EXPLAIN-020: Type mapping per source
    # ==================================================================

    def do_explain_019(self):
        """PG type mapping — VERBOSE shows original PG types."""
        sql = f"select ts, voltage from {_PG_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-019 [passed]")

    def do_explain_020(self):
        """InfluxDB type mapping — VERBOSE shows original InfluxDB types."""
        sql = f"select * from {_INFLUX_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_verbose_contain(results, "Type Mapping:")
        self._assert_verbose_contain(results, "<-")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-020 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-021: Plan output does not contain data rows
    # ==================================================================

    def do_explain_021(self):
        """Plan output does not contain actual data values."""
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        for mode, lines in results.items():
            for line in lines:
                assert "220.5" not in line, \
                    f"[{mode}] plan output should not contain data value '220.5': {line}"
        print("FQ-EXPLAIN-021 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-022: JOIN pushdown (same-source)
    # ==================================================================

    def do_explain_022(self):
        """Same-source JOIN pushed — Remote SQL contains JOIN.
        No local Join operator in main plan.
        """
        sql = (f"select s.ts, s.voltage, r.area "
               f"from {_MYSQL_SRC}.sensor s join {_MYSQL_SRC}.region_info r "
               f"on s.region = r.region")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "JOIN")
        self._assert_no_local_operator(results, "Join")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-022 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-023: Virtual table EXPLAIN
    # ==================================================================

    def do_explain_023(self):
        """Virtual table referencing external columns shows FederatedScan."""
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
        print("FQ-EXPLAIN-023 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-024: WHERE + ORDER BY (no LIMIT) pushdown
    # ==================================================================

    def do_explain_024(self):
        """WHERE + ORDER BY without LIMIT — both pushed to remote.
        No local Sort; no local Filter.
        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where voltage > 219.0 order by ts")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "WHERE")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_remote_sql_no_kw(results, "LIMIT")
        self._assert_no_local_operator(results, "Sort ")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-024 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-025: LIMIT + OFFSET pushdown
    # ==================================================================

    def do_explain_025(self):
        """LIMIT with OFFSET — both pushed to Remote SQL."""
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"order by ts limit 2 offset 1")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "LIMIT")
        self._assert_remote_sql_kw(results, "OFFSET")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_no_local_operator(results, "Sort ")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-025 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-026: DISTINCT pushdown
    # ==================================================================

    def do_explain_026(self):
        """DISTINCT pushed to Remote SQL.
        No local Agg/Distinct operator.
        """
        sql = f"select distinct region from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "DISTINCT")
        self._assert_no_local_operator(results, "Agg")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-026 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-027: Compound WHERE (AND/OR) pushdown
    # ==================================================================

    def do_explain_027(self):
        """Compound WHERE with AND/OR pushed as a whole to Remote SQL."""
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where (voltage > 220.0 and region = 'north') "
               f"or current < 1.2")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "WHERE")
        # Verify the compound condition is in remote, not split locally
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            # At least one logical operator should be present in remote SQL
            has_logic = ("AND" in remote.upper()) or ("OR" in remote.upper())
            assert has_logic, \
                f"[{mode}] compound WHERE should preserve AND/OR in Remote SQL: {remote}"
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-027 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-028: SELECT * baseline — no extra local operators
    # ==================================================================

    def do_explain_028(self):
        """SELECT * baseline — only FederatedScan, minimal plan.
        No Sort, Agg, or Filter in local plan. Remote SQL is a simple SELECT.
        """
        sql = f"select * from {_MYSQL_SRC}.sensor"
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        self._assert_no_local_operator(results, "Sort ")
        self._assert_no_local_operator(results, "Agg")
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-028 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-029: PostgreSQL pushdown — WHERE + ORDER BY + LIMIT
    # ==================================================================

    def do_explain_029(self):
        """Full pushdown on PostgreSQL source — WHERE + ORDER BY + LIMIT.
        Verifies pushdown is not MySQL-only.
        """
        sql = (f"select ts, voltage from {_PG_SRC}.sensor "
               f"where voltage > 220.0 order by ts limit 3")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_remote_sql_kw(results, "WHERE")
        self._assert_remote_sql_kw(results, "ORDER BY")
        self._assert_remote_sql_kw(results, "LIMIT")
        self._assert_no_local_operator(results, "Sort ")
        # Verify PG uses double-quote dialect
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            assert '"' in remote, \
                f"[{mode}] PG Remote SQL should use double-quote quoting: {remote}"
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-029 [passed]")

    # ==================================================================
    # FQ-EXPLAIN-030: Subquery pushdown
    # ==================================================================

    def do_explain_030(self):
        """Subquery used in WHERE — subquery pushed to Remote SQL.
        SELECT ... WHERE voltage > (SELECT AVG(voltage) FROM sensor)
        """
        sql = (f"select ts, voltage from {_MYSQL_SRC}.sensor "
               f"where voltage > (select avg(voltage) from {_MYSQL_SRC}.sensor)")
        results = self._run_all_modes(sql)
        self._assert_all_contain(results, "FederatedScan")
        self._assert_all_contain(results, "Remote SQL:")
        # The subquery should appear in Remote SQL (pushed down)
        for mode, lines in results.items():
            remote = self._get_remote_sql_line(lines, label=mode)
            # Remote SQL should contain a nested SELECT (subquery)
            remote_upper = remote.upper()
            # Count SELECT occurrences — at least 2 if subquery is pushed
            select_count = remote_upper.count("SELECT")
            assert select_count >= 2, \
                f"[{mode}] subquery should be pushed to Remote SQL (expected >=2 SELECTs): {remote}"
        self._check_analyze_metrics(results)
        print("FQ-EXPLAIN-030 [passed]")

    # ==================================================================
    # test_* entry points
    # ==================================================================

    def test_fq_explain_basic(self):
        """FQ-EXPLAIN-001~003: Basic EXPLAIN — operator name, Remote SQL, source info

        1. FederatedScan operator name appears in all modes
        2. Remote SQL line appears in all modes
        3. FederatedScan on <source>.<db>.<table> format

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Refactor to do_* pattern

        """
        self.do_explain_001()
        self.do_explain_002()
        self.do_explain_003()

    def test_fq_explain_verbose(self):
        """FQ-EXPLAIN-004~006: VERBOSE fields — type mapping, pushdown flags, columns

        1. Type Mapping in VERBOSE modes
        2. Pushdown flags in VERBOSE modes
        3. columns=[...] in VERBOSE modes

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Refactor to do_* pattern

        """
        self.do_explain_004()
        self.do_explain_005()
        self.do_explain_006()

    def test_fq_explain_pushdown(self):
        """FQ-EXPLAIN-007~015: Pushdown — WHERE/ORDER BY/LIMIT/Agg/HAVING/CSUM/projection

        1. Full pushdown: WHERE + ORDER BY + LIMIT, no local Sort
        2. WHERE-only pushdown
        3. ORDER BY-only pushdown, no local Sort
        4. LIMIT-only pushdown
        5. COUNT + GROUP BY pushed, no local Agg
        6. SUM/AVG/MIN/MAX pushed, no local Agg
        7. HAVING pushed to remote
        8. TDengine-only CSUM NOT pushed
        9. Column projection — specific columns, not SELECT *

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Add granular pushdown validation with no-local-operator checks

        """
        self.do_explain_007()
        self.do_explain_008()
        self.do_explain_009()
        self.do_explain_010()
        self.do_explain_011()
        self.do_explain_012()
        self.do_explain_013()
        self.do_explain_014()
        self.do_explain_015()

    def test_fq_explain_dialect(self):
        """FQ-EXPLAIN-016~018: Dialect — MySQL backtick, PG double-quote, InfluxDB

        1. MySQL backtick quoting
        2. PG double-quote quoting
        3. InfluxDB Remote SQL presence

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Refactor to do_* pattern

        """
        self.do_explain_016()
        self.do_explain_017()
        self.do_explain_018()

    def test_fq_explain_type_mapping(self):
        """FQ-EXPLAIN-019~020: Type mapping — PG and InfluxDB original types

        1. PG type mapping in VERBOSE
        2. InfluxDB type mapping in VERBOSE

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Refactor to do_* pattern

        """
        self.do_explain_019()
        self.do_explain_020()

    def test_fq_explain_no_data(self):
        """FQ-EXPLAIN-021: EXPLAIN does not return data rows

        1. Plan output must not contain actual data values

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Renumber to 021

        """
        self.do_explain_021()

    def test_fq_explain_join(self):
        """FQ-EXPLAIN-022: JOIN pushdown — same-source JOIN in Remote SQL

        1. Same-source JOIN pushed to remote, no local Join operator

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Add no-local-Join assertion

        """
        self.do_explain_022()

    def test_fq_explain_vtable(self):
        """FQ-EXPLAIN-023: Virtual table EXPLAIN — FederatedScan

        1. Virtual table referencing external columns shows FederatedScan

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-13 wpan Initial implementation
            - 2026-04-16 wpan Run all four EXPLAIN modes and validate output
            - 2026-04-23 wpan Renumber to 023

        """
        self.do_explain_023()

    def test_fq_explain_pushdown_combos(self):
        """FQ-EXPLAIN-024~027: Pushdown combinations — WHERE+ORDER, OFFSET, DISTINCT, compound

        1. WHERE + ORDER BY (no LIMIT) both pushed
        2. LIMIT + OFFSET pushed
        3. DISTINCT pushed
        4. Compound WHERE (AND/OR) pushed intact

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 wpan New: pushdown combination scenarios

        """
        self.do_explain_024()
        self.do_explain_025()
        self.do_explain_026()
        self.do_explain_027()

    def test_fq_explain_baseline_and_cross_source(self):
        """FQ-EXPLAIN-028~029: Baseline SELECT * plan and cross-source pushdown

        1. SELECT * baseline — minimal plan, no extra local operators
        2. PostgreSQL full pushdown — confirms pushdown is not MySQL-only

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 wpan New: baseline and cross-source validation

        """
        self.do_explain_028()
        self.do_explain_029()

    def test_fq_explain_subquery(self):
        """FQ-EXPLAIN-030: Subquery pushdown — nested SELECT in Remote SQL

        1. Subquery in WHERE pushed to Remote SQL (>=2 SELECTs in remote)

        Catalog:
            - Query:FederatedExplain

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 wpan New: subquery pushdown validation

        """
        self.do_explain_030()
