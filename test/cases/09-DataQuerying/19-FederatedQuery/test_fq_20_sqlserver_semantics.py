# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later, as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""SQL Server federated-query semantic-equivalence coverage.

The all-source parity suite uses a deliberately friendly data set (ASCII,
lowercase, unique labels, integral averages).  This file adds the boundary
cases that distinguish TDengine semantics from SQL Server's collation
(case/trailing-space), UTF-16/code-page, NULL-concatenation, AVG(int),
division-by-zero and integer-overflow behaviour.

Every deterministic query is executed twice through the federated-query
planner: once against a local TDengine table and once against the equivalent
SQL Server table.  The two results must match exactly for strings/integers
and within a tight tolerance for floating-point aggregates.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "09-DataQuerying", "19-FederatedQuery")
)
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    ParityTestBase,
    QueryError,
)

_LOCAL_DB = "fq_sem_local"
_VT_DB = "fq_sem_vt_local"
_REMOTE_DB = "fq_sem_ss"
_SRC = "fq_sem_src_ss"

_TOL = 1e-9


def _td_lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        # TDengine literals treat backslash as an escape introducer; double it
        # so the stored value keeps a literal backslash.
        return "'" + v.replace("\\", "\\\\").replace("'", "''") + "'"
    return str(v)


def _ss_lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, str):
        return "N'" + v.replace("'", "''") + "'"
    return str(v)


# (time, id, label, note, nullable_label, nullable_val)
_STR_ROWS = [
    ("2024-01-01 00:00:00.000", 1, "North", "abc", None, None),
    ("2024-01-01 00:01:00.000", 2, "north", "abc ", "x", 1),
    ("2024-01-01 00:02:00.000", 3, "NORTH", "", None, None),
    ("2024-01-01 00:03:00.000", 4, "Ä", "ä", "y", 2),
    ("2024-01-01 00:04:00.000", 5, "😀", "😀x", None, 3),
    ("2024-01-01 00:05:00.000", 6, "Z", "Äx", None, None),
    ("2024-01-01 00:06:00.000", 7, "bs", "ab\\c", None, None),
]

# (time, id, a, b, d) with 32-bit boundary values
_NUM_ROWS = [
    ("2024-01-01 00:00:00.000", 1, 1, 2147483647, 1.5),
    ("2024-01-01 00:01:00.000", 2, 2, 1, 2.5),
    ("2024-01-01 00:02:00.000", 3, 3, -2147483648, -3.0),
    ("2024-01-01 00:03:00.000", 4, 4, 0, 0.0),
]

_TIME_ROWS = [("2024-01-01 20:00:00.000", 1)]


def _rows_sql(rows, lit):
    return ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)


class TestFq20SqlServerSemantics(ParityTestBase):
    """Boundary-result parity for SQL Server federated queries."""

    _FLOAT_TOL = _TOL
    _BASELINE_FILE = os.path.join(
        os.path.dirname(__file__), "ans", "test_fq_20_sqlserver_semantics.txt"
    )

    @property
    def _local_tbl(self):
        return f"{_LOCAL_DB}.sem_str_t"

    def _ext_sources(self):
        return [("SQLServer", f"{_SRC}.sem_str_t")]

    # ------------------------------------------------------------------
    # Setup / teardown
    # ------------------------------------------------------------------

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        os.environ.setdefault("FQ_REQUIRED_SERVICES", "sqlserver")
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        tdSql.execute('ALTER LOCAL "timezone" "UTC"')

        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'us'")
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(
            "CREATE TABLE sem_str_t (time TIMESTAMP, id INT, label NCHAR(32), "
            "note NCHAR(32), nullable_label NCHAR(32), nullable_val INT)"
        )
        tdSql.execute(
            "CREATE TABLE sem_num_t (time TIMESTAMP, id INT, a INT, b INT, d DOUBLE)"
        )
        tdSql.execute("CREATE TABLE sem_time_t (time TIMESTAMP, id INT)")
        tdSql.executes(
            ["INSERT INTO sem_str_t VALUES " + row for row in
             ("(" + ", ".join(_td_lit(v) for v in r) + ")" for r in _STR_ROWS)]
        )
        tdSql.executes(
            ["INSERT INTO sem_num_t VALUES " + row for row in
             ("(" + ", ".join(_td_lit(v) for v in r) + ")" for r in _NUM_ROWS)]
        )
        tdSql.executes(
            ["INSERT INTO sem_time_t VALUES " + row for row in
             ("(" + ", ".join(_td_lit(v) for v in r) + ")" for r in _TIME_ROWS)]
        )

        ExtSrcEnv.sqlserver_create_db(_REMOTE_DB)
        ExtSrcEnv.sqlserver_exec(_REMOTE_DB, [
            "IF OBJECT_ID('dbo.sem_str_t') IS NOT NULL DROP TABLE dbo.sem_str_t",
            "CREATE TABLE dbo.sem_str_t (time datetime2(3) NOT NULL PRIMARY KEY, id int, "
            "label nvarchar(32), note nvarchar(32), nullable_label nvarchar(32), "
            "nullable_val int)",
            "INSERT INTO dbo.sem_str_t VALUES " + _rows_sql(_STR_ROWS, _ss_lit),
            "IF OBJECT_ID('dbo.sem_num_t') IS NOT NULL DROP TABLE dbo.sem_num_t",
            "CREATE TABLE dbo.sem_num_t (time datetime2(3) NOT NULL, id int, a int, "
            "b int, d float)",
            "INSERT INTO dbo.sem_num_t VALUES " + _rows_sql(_NUM_ROWS, _ss_lit),
            "IF OBJECT_ID('dbo.sem_time_t') IS NOT NULL DROP TABLE dbo.sem_time_t",
            "CREATE TABLE dbo.sem_time_t (time datetime2(3) NOT NULL, id int)",
            "INSERT INTO dbo.sem_time_t VALUES " + _rows_sql(_TIME_ROWS, _ss_lit),
        ])

        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SRC}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {_SRC} TYPE='sqlserver' "
            f"HOST='{ExtSrcEnv.SQLSERVER_HOST}' PORT={ExtSrcEnv.SQLSERVER_PORT} "
            f"USER='{ExtSrcEnv.SQLSERVER_USER}' PASSWORD='{ExtSrcEnv.SQLSERVER_PASS}' "
            f"DATABASE={_REMOTE_DB} SCHEMA=dbo"
        )

        # Virtual-table declared filters are rendered by a separate path; this
        # database must be at least as fine as the referenced timestamp column
        # (the fixture's datetime2(6) axis declares microsecond precision).
        tdSql.execute(f"DROP DATABASE IF EXISTS {_VT_DB}")
        tdSql.execute(f"CREATE DATABASE {_VT_DB} PRECISION 'us'")
        tdSql.execute(f"USE {_VT_DB}")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {_VT_DB}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SRC}")
        finally:
            ExtSrcEnv.sqlserver_drop_db(_REMOTE_DB)

    # ------------------------------------------------------------------
    # Comparison helpers
    # ------------------------------------------------------------------

    def _assert_parity(self, sql_template, local_tbl, remote_tbl,
                       float_cols=frozenset(), ordered=True):
        local_sql = sql_template.format(tbl=f"{_LOCAL_DB}.{local_tbl}")
        ext_sql = sql_template.format(tbl=f"{_SRC}.{remote_tbl}")
        ref = self._get_rows(local_sql)
        rows = self._get_rows(ext_sql)
        if not ordered:
            ref = sorted(ref, key=lambda r: [str(x) for x in r])
            rows = sorted(rows, key=lambda r: [str(x) for x in r])
        self._compare_rows(ref, rows, local_sql, ext_sql, "SQLServer", set(float_cols))

    def _run_cases(self, table, cases):
        failures = []
        for case_id, sql_template, float_cols, ordered in cases:
            try:
                self._assert_parity(sql_template, table, table, float_cols, ordered)
                tdLog.info(f"[{case_id}] PASS")
            except (AssertionError, QueryError) as exc:
                failures.append((case_id, sql_template, str(exc)))
                tdLog.info(f"[{case_id}] FAIL: {str(exc).splitlines()[0]}")
        if failures:
            detail = "\n".join(
                f"\n[{case_id}] {sql}\n  {msg}" for case_id, sql, msg in failures
            )
            raise AssertionError(f"{len(failures)} SQL Server semantic case(s) failed:{detail}")

    # ------------------------------------------------------------------
    # String / character semantics
    # ------------------------------------------------------------------

    _STR_CASES = [
        # Equality / inequality: exact binary collation + payload length.
        ("eq_case", "SELECT id FROM {tbl} WHERE label = 'North' ORDER BY id", set(), True),
        ("eq_case_upper", "SELECT id FROM {tbl} WHERE label = 'NORTH' ORDER BY id", set(), True),
        ("eq_case_lower", "SELECT id FROM {tbl} WHERE label = 'north' ORDER BY id", set(), True),
        ("eq_trailing_space", "SELECT id FROM {tbl} WHERE label = 'north ' ORDER BY id", set(), True),
        ("ne_case", "SELECT id FROM {tbl} WHERE label <> 'North' ORDER BY id", set(), True),
        ("eq_note_trailing", "SELECT id FROM {tbl} WHERE note = 'abc' ORDER BY id", set(), True),
        ("eq_note_empty", "SELECT id FROM {tbl} WHERE note = '' ORDER BY id", set(), True),
        ("eq_non_ascii", "SELECT id FROM {tbl} WHERE label = 'Ä' ORDER BY id", set(), True),
        ("eq_emoji", "SELECT id FROM {tbl} WHERE label = '😀' ORDER BY id", set(), True),
        ("eq_nullable_is_null", "SELECT id FROM {tbl} WHERE nullable_label IS NULL ORDER BY id", set(), True),
        # IN/BETWEEN/ordering are collation-sensitive and stay local; LIKE is
        # rewritten exactly (ASCII folding + binary collation) or stays local
        # when the pattern contains '_'.
        ("in_trailing_space", "SELECT id FROM {tbl} WHERE label IN ('North', 'north ') ORDER BY id", set(), True),
        ("between_case", "SELECT id FROM {tbl} WHERE label BETWEEN 'A' AND 'Z' ORDER BY id", set(), True),
        ("lt_case", "SELECT id FROM {tbl} WHERE label < 'north' ORDER BY id", set(), True),
        ("like_non_ascii_lower", "SELECT id FROM {tbl} WHERE note LIKE 'ä%' ORDER BY id", set(), True),
        ("like_non_ascii_upper", "SELECT id FROM {tbl} WHERE note LIKE 'Ä%' ORDER BY id", set(), True),
        # ANSI padding must not make 'abc ' match the pattern 'abc'.
        ("like_no_trailing_space", "SELECT id FROM {tbl} WHERE note LIKE 'abc' ORDER BY id", set(), True),
        ("like_no_trailing_space_pct", "SELECT id FROM {tbl} WHERE note LIKE 'abc%' ORDER BY id", set(), True),
        ("like_pattern_trailing_space", "SELECT id FROM {tbl} WHERE note LIKE 'abc ' ORDER BY id", set(), True),
        ("like_underscore_local", "SELECT id FROM {tbl} WHERE note LIKE 'ab_' ORDER BY id", set(), True),
        # Backslash handling: in TDengine literals '\x' unescapes to 'x', while
        # '\\' is one literal backslash.  The renderer must normalize literal
        # backslashes for T-SQL's ESCAPE clause.
        ("like_parser_unescape", "SELECT id FROM {tbl} WHERE note LIKE 'ab\\c' ORDER BY id", set(), True),
        ("like_literal_backslash", "SELECT id FROM {tbl} WHERE note LIKE 'ab\\\\c' ORDER BY id", set(), True),
        ("like_trailing_backslash", "SELECT id FROM {tbl} WHERE note LIKE 'abc\\\\' ORDER BY id", set(), True),
        ("like_escaped_wildcard", "SELECT id FROM {tbl} WHERE note LIKE 'ab\\\\%' ORDER BY id", set(), True),
        # Ordering / grouping / DISTINCT / MIN / MAX on characters stay local.
        ("order_label", "SELECT id, label FROM {tbl} ORDER BY label, id", set(), True),
        ("distinct_label", "SELECT DISTINCT label FROM {tbl} ORDER BY label", set(), True),
        ("group_label", "SELECT label, COUNT(*) FROM {tbl} GROUP BY label ORDER BY label", set(), True),
        ("min_max_label", "SELECT MIN(label), MAX(label) FROM {tbl}", set(), False),
        # Character functions stay local (UCS4/UTF-8 vs UTF-16/code page).
        ("position_case", "SELECT id, POSITION('O' IN label) FROM {tbl} ORDER BY id", set(), True),
        ("replace_case", "SELECT id, REPLACE(label, 'NORTH', 'x') FROM {tbl} ORDER BY id", set(), True),
        ("lower_upper", "SELECT id, LOWER(label), UPPER(label) FROM {tbl} ORDER BY id", set(), True),
        ("length_char_length", "SELECT id, LENGTH(label), CHAR_LENGTH(label) FROM {tbl} ORDER BY id", set(), True),
        ("substr_char", "SELECT id, SUBSTR(label, 2, 1) FROM {tbl} ORDER BY id", set(), True),
        ("ascii_first_byte", "SELECT id, ASCII(label) FROM {tbl} ORDER BY id", set(), True),
        # CONCAT/CONCAT_WS use the exact NULL-guard rewrite.
        ("concat_null", "SELECT id, CONCAT(label, CAST(nullable_val AS VARCHAR(8))) FROM {tbl} ORDER BY id", set(), True),
        ("concat_ws_null", "SELECT id, CONCAT_WS('-', label, nullable_label) FROM {tbl} ORDER BY id", set(), True),
        ("group_concat", "SELECT id, GROUP_CONCAT(label, ',') AS g FROM {tbl} GROUP BY id ORDER BY id", set(), True),
        # Character JOIN keys use the exact equality rewrite.
        ("join_label", "SELECT a.id, b.id FROM {tbl} a JOIN {tbl} b ON a.label = b.label ORDER BY a.id, b.id", set(), True),
        ("join_note_trailing", "SELECT a.id, b.id FROM {tbl} a JOIN {tbl} b ON a.note = b.note ORDER BY a.id, b.id", set(), True),
    ]

    def test_sqlserver_string_semantics(self):
        """SQL Server string semantics parity.

        Runs equality, LIKE, ordering, grouping, character-function and JOIN
        cases against a local TDengine table and the equivalent SQL Server
        table; both result sets must match exactly.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 dapan1121 Initial semantic-equivalence coverage

        """
        self._run_cases("sem_str_t", self._STR_CASES)

    # ------------------------------------------------------------------
    # Numeric semantics
    # ------------------------------------------------------------------

    _NUM_CASES = [
        # AVG over int must be DOUBLE (T-SQL AVG(int) truncates).
        ("avg_int_fraction", "SELECT AVG(a) FROM {tbl} WHERE id <= 2", {0}, False),
        ("avg_int_all", "SELECT AVG(a) FROM {tbl}", {0}, False),
        # SUM over int must be BIGINT (T-SQL SUM(int) overflows).
        ("sum_int_overflow", "SELECT SUM(b) FROM {tbl} WHERE id IN (1, 2)", set(), False),
        ("sum_small_int", "SELECT SUM(a) FROM {tbl}", set(), False),
        # Arithmetic is double and must not overflow or truncate.
        ("add_int_overflow", "SELECT b + 1 FROM {tbl} WHERE id = 1", {0}, False),
        ("mul_int_overflow", "SELECT b * 2 FROM {tbl} WHERE id = 1", {0}, False),
        ("neg_int_min", "SELECT -b FROM {tbl} WHERE id = 3", {0}, False),
        ("div_two", "SELECT a / 2 FROM {tbl} WHERE id = 1", {0}, False),
        # Division/modulo by zero must return NULL, not error 8134.
        ("div_zero", "SELECT b / 0 FROM {tbl} WHERE id = 1", {0}, False),
        ("mod_zero", "SELECT b % 0 FROM {tbl} WHERE id = 1", {0}, False),
        ("mod_int", "SELECT id, b % 2 FROM {tbl} ORDER BY id", {1}, True),
        ("mod_func", "SELECT id, MOD(a, 2) FROM {tbl} ORDER BY id", {1}, True),
        # COUNT/AVG/aggregate value parity.
        ("count_star", "SELECT COUNT(*) FROM {tbl}", set(), False),
        ("count_distinct", "SELECT COUNT(DISTINCT b) FROM {tbl}", set(), False),
        ("avg_double", "SELECT AVG(d) FROM {tbl}", {0}, False),
        ("min_max_int", "SELECT MIN(b), MAX(b) FROM {tbl}", set(), False),
    ]

    def test_sqlserver_numeric_semantics(self):
        """SQL Server numeric semantics parity.

        Covers AVG(int) truncation, SUM/COUNT overflow, double arithmetic,
        unary minus, division and modulo by zero, and aggregate result types;
        local and SQL Server results must match.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 dapan1121 Initial semantic-equivalence coverage

        """
        self._run_cases("sem_num_t", self._NUM_CASES)

    # ------------------------------------------------------------------
    # Time formatting
    # ------------------------------------------------------------------

    _TIME_CASES = [
        # FORMAT is pinned to en-US; the AM/PM designator must match.
        ("to_char_ampm", "SELECT TO_CHAR(time, 'yyyy-MM-dd HH24:MI:SS AM') FROM {tbl} WHERE id = 1", set(), True),
        ("to_char_plain", "SELECT TO_CHAR(time, 'yyyy-MM-dd HH24:MI:SS') FROM {tbl} WHERE id = 1", set(), True),
        # Explicit timezone must stay local (the third argument is not forwarded).
        ("to_char_explicit_tz", "SELECT TO_CHAR(time, 'yyyy-MM-dd HH24:MI:SS', 'Asia/Shanghai') FROM {tbl} WHERE id = 1", set(), True),
        ("date_fn", "SELECT DATE(time) FROM {tbl} WHERE id = 1", set(), True),
        ("to_unixtimestamp", "SELECT TO_UNIXTIMESTAMP(time) FROM {tbl} WHERE id = 1", set(), True),
        ("iso_week_parts", "SELECT WEEKOFYEAR(time), DAYOFWEEK(time), WEEKDAY(time) FROM {tbl} WHERE id = 1", set(), True),
    ]

    def test_sqlserver_time_format_semantics(self):
        """SQL Server time formatting parity.

        Verifies TO_CHAR culture pinning (AM/PM), explicit-timezone fallback,
        DATE, TO_UNIXTIMESTAMP and ISO week parts against local results.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 dapan1121 Initial semantic-equivalence coverage

        """
        self._run_cases("sem_time_t", self._TIME_CASES)

    # ------------------------------------------------------------------
    # Virtual-table declared filters (separate renderer path)
    # ------------------------------------------------------------------

    def test_sqlserver_vtable_filter_semantics(self):
        """SQL Server VTABLE declared-filter semantics.

        Verifies that character equality in a declared filter is exact while a
        character ordering predicate falls back to the local FederatedScan
        filter and returns the TDengine result.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 dapan1121 Initial semantic-equivalence coverage

        """
        tdSql.execute(f"USE {_VT_DB}")
        for name in ("vt_eq_label", "vt_eq_note", "vt_bad_order"):
            tdSql.execute(f"DROP VTABLE IF EXISTS {name}")

        ref = f"{_SRC}.{_REMOTE_DB}.dbo.sem_str_t.id"
        tdSql.execute(
            f"CREATE VTABLE vt_eq_label (ts TIMESTAMP, id INT FROM {ref} "
            f"WHERE label = 'North')"
        )
        tdSql.query("SELECT id FROM vt_eq_label ORDER BY id")
        assert tdSql.queryResult == [(1,)], f"vtable label filter mismatch: {tdSql.queryResult!r}"

        tdSql.execute(
            f"CREATE VTABLE vt_eq_note (ts TIMESTAMP, id INT FROM {ref} "
            f"WHERE note = 'abc')"
        )
        tdSql.query("SELECT id FROM vt_eq_note ORDER BY id")
        assert tdSql.queryResult == [(1,)], f"vtable note filter mismatch: {tdSql.queryResult!r}"

        # Character ordering cannot be reproduced exactly under the SQL Server
        # collation, so the declared filter is evaluated by the local
        # FederatedScan filter and must return exactly the TDengine result.
        tdSql.execute(
            f"CREATE VTABLE vt_bad_order (ts TIMESTAMP, id INT FROM {ref} "
            f"WHERE label > 'North')"
        )
        tdSql.query(
            f"SELECT id FROM {_LOCAL_DB}.sem_str_t WHERE label > 'North' ORDER BY id")
        local_rows = list(tdSql.queryResult)
        tdSql.query("SELECT id FROM vt_bad_order ORDER BY id")
        assert tdSql.queryResult == local_rows, (
            "declared character-ordering filter mismatch: "
            f"{tdSql.queryResult!r} vs local {local_rows!r}"
        )

        for name in ("vt_eq_label", "vt_eq_note", "vt_bad_order"):
            tdSql.execute(f"DROP VTABLE IF EXISTS {name}")
