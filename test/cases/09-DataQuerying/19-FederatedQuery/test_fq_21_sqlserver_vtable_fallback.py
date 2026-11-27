# -*- coding: utf-8 -*-
"""SQL Server federated-query vtable regression coverage.

Covers the defects found while auditing whether SQL Server honours the
"TDengine features must stay available, fall back to local computation"
contract:

  - vtable queries over a SQL Server source must not crash (a shallow copy of
    the push-down capability left a dangling hash in the external-table node
    and the plan clone segfaulted);
  - declared character ordering (WHERE label > 'x') and supplementary-plane
    literals cannot be rendered exactly by the T-SQL dialect and must be
    evaluated by the local FederatedScan filter instead of being rejected at
    DDL time;
  - EXISTS / IN subqueries over SQL Server sources return the local results;
  - timestamp push-down keeps the microsecond transport precision
    (TO_UNIXTIMESTAMP / SPREAD / CAST on datetime2(3) and datetime2(6));
  - ms / us / ns databases all accept a datetime2(3) vtable and return the
    same rows.

Catalog:
    - Query:FederatedSqlServerSemantics

Since: v3.4.4.0

Labels: common,ci

Jira: None

History:
    - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "09-DataQuerying", "19-FederatedQuery")
)
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from federated_query_common import ExtSrcEnv  # noqa: E402

_LOCAL_DB = "fq_reg_local"
_REMOTE_DB = "fq_reg_ss"
_SRC = "fq_reg_src"

_VT_MS = "fq_reg_vt_ms"
_VT_US = "fq_reg_vt_us"
_VT_NS = "fq_reg_vt_ns"

# (time, id, label, note, grp)
_ROWS = [
    ("2024-01-01 00:00:00.000", 1, "North", "abc", 1),
    ("2024-01-01 00:01:00.000", 2, "north", "abc ", 1),
    ("2024-01-01 00:02:00.000", 3, "NORTH", "", 2),
    ("2024-01-01 00:03:00.000", 4, "Ä", "ä", 2),
    ("2024-01-01 00:04:00.000", 5, "😀", "😀x", 2),
    ("2024-01-01 00:05:00.000", 6, "Z", "Äx", 1),
    ("2024-01-01 00:06:00.000", 7, "Abc", "ab\\c", 2),
]

# datetime2(6) axis: every row is a distinct microsecond value.
_US_ROWS = [
    ("2024-01-01 00:00:00.254321", 1, 10),
    ("2024-01-01 00:00:01.900001", 2, 20),
]


def _td_lit(v):
    if isinstance(v, str):
        return "'" + v.replace("\\", "\\\\").replace("'", "''") + "'"
    return str(v)


def _ss_lit(v):
    if isinstance(v, str):
        return "N'" + v.replace("'", "''") + "'"
    return str(v)


def _rows_sql(rows, lit):
    return ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)


class TestFq21SqlServerVtableFallback:
    """Regression coverage for the SQL Server vtable audit findings."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        os.environ.setdefault("FQ_REQUIRED_SERVICES", "sqlserver")
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        tdSql.execute('ALTER LOCAL "timezone" "UTC"')

        for db in (_LOCAL_DB, _VT_MS, _VT_US, _VT_NS):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'us'")
        tdSql.execute(f"CREATE DATABASE {_VT_MS} PRECISION 'ms'")
        tdSql.execute(f"CREATE DATABASE {_VT_US} PRECISION 'us'")
        tdSql.execute(f"CREATE DATABASE {_VT_NS} PRECISION 'ns'")

        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(
            "CREATE TABLE reg_str (ts TIMESTAMP, id INT, label NCHAR(32), "
            "note NCHAR(32), grp INT)"
        )
        tdSql.executes(
            ["INSERT INTO reg_str VALUES " + "(" + ", ".join(_td_lit(v) for v in r) + ")"
             for r in _ROWS]
        )

        ExtSrcEnv.sqlserver_create_db(_REMOTE_DB)
        ExtSrcEnv.sqlserver_exec(_REMOTE_DB, [
            "IF OBJECT_ID('dbo.reg_str') IS NOT NULL DROP TABLE dbo.reg_str",
            "CREATE TABLE dbo.reg_str (ts datetime2(3) NOT NULL PRIMARY KEY, id int, "
            "label nvarchar(32), note nvarchar(32), grp int)",
            "INSERT INTO dbo.reg_str VALUES " + _rows_sql(_ROWS, _ss_lit),
            "IF OBJECT_ID('dbo.reg_us') IS NOT NULL DROP TABLE dbo.reg_us",
            "CREATE TABLE dbo.reg_us (ts datetime2(6) NOT NULL PRIMARY KEY, id int, v int)",
            "INSERT INTO dbo.reg_us VALUES " + _rows_sql(_US_ROWS, _ss_lit),
        ])

        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SRC}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {_SRC} TYPE='sqlserver' "
            f"HOST='{ExtSrcEnv.SQLSERVER_HOST}' PORT={ExtSrcEnv.SQLSERVER_PORT} "
            f"USER='{ExtSrcEnv.SQLSERVER_USER}' PASSWORD='{ExtSrcEnv.SQLSERVER_PASS}' "
            f"DATABASE={_REMOTE_DB} SCHEMA=dbo"
        )

    @classmethod
    def teardown_class(cls):
        try:
            for db in (_VT_MS, _VT_US, _VT_NS, _LOCAL_DB):
                tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SRC}")
        finally:
            ExtSrcEnv.sqlserver_drop_db(_REMOTE_DB)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _remote_rows(sql):
        tdSql.query(sql)
        return tdSql.queryResult

    def _drop_vtables(self, db):
        for name in ("vt_all", "vt_gt", "vt_ge", "vt_lt", "vt_emoji",
                     "vt_mixed", "vt_us6", "vt_ms6"):
            tdSql.execute(f"DROP VTABLE IF EXISTS {db}.{name}")

    # ------------------------------------------------------------------
    # 1. vtable query over SQL Server must not crash
    # ------------------------------------------------------------------

    def test_vtable_query_over_sqlserver_source(self):
        """SQL Server vtable queries keep the capability hashes they clone.

        A shallow copy of SExtSourceCapability left the external-table node
        pointing at hashes owned (and freed) by the cached source info; the
        plan clone then iterated freed memory and the client segfaulted.  Run
        the vtable queries several times so the freed block is reused.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        ref = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str.id"
        for db in (_VT_MS, _VT_US, _VT_NS):
            self._drop_vtables(db)
            tdSql.execute(
                f"CREATE VTABLE {db}.vt_all (ts TIMESTAMP, id INT FROM {ref}, "
                f"label NCHAR(32) FROM {_SRC}.{_REMOTE_DB}.dbo.reg_str.label)"
            )

        expected = self._remote_rows(
            f"SELECT id, label FROM {_LOCAL_DB}.reg_str ORDER BY id")
        for _ in range(8):
            for db in (_VT_MS, _VT_US, _VT_NS):
                rows = self._remote_rows(
                    f"SELECT id, label FROM {db}.vt_all ORDER BY id")
                assert rows == expected, f"{db}.vt_all mismatch: {rows!r}"
                # A direct external scan in the same session keeps the cache
                # warm; the vtable ref node must not alias that cached state.
                self._remote_rows(
                    f"SELECT id FROM {_SRC}.{_REMOTE_DB}.dbo.reg_str ORDER BY id")
                tdSql.query(f"EXPLAIN VERBOSE TRUE SELECT * FROM {db}.vt_all")

        for db in (_VT_MS, _VT_US, _VT_NS):
            self._drop_vtables(db)

    # ------------------------------------------------------------------
    # 2. declared filters that T-SQL cannot render exactly stay local
    # ------------------------------------------------------------------

    def test_declared_character_ordering_filter_is_local(self):
        """WHERE label > 'Abc' is accepted at DDL and evaluated locally.

        SQL Server compares characters under a collation (case/accent/trailing
        space rules), so an ordering predicate cannot be pushed exactly.  The
        declared filter must fall back to the local FederatedScan filter and
        return exactly the TDengine result.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        ref = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str.id"
        tdSql.execute(f"USE {_VT_US}")
        self._drop_vtables(_VT_US)

        for name, op, lit in (
            ("vt_gt", ">", "Abc"),
            ("vt_ge", ">=", "north"),
            ("vt_lt", "<", "Ä"),
        ):
            tdSql.execute(
                f"CREATE VTABLE {name} (ts TIMESTAMP, id INT FROM {ref} "
                f"WHERE label {op} '{lit}')")
            local_rows = self._remote_rows(
                f"SELECT id FROM {_LOCAL_DB}.reg_str WHERE label {op} '{lit}' "
                f"ORDER BY id")
            ext_rows = self._remote_rows(f"SELECT id FROM {name} ORDER BY id")
            assert ext_rows == local_rows, (
                f"{name}: declared filter {op} '{lit}' returned {ext_rows!r}, "
                f"local result is {local_rows!r}")

        self._drop_vtables(_VT_US)

    def test_declared_non_bmp_filter_is_local(self):
        """A supplementary-plane literal stays in the local filter.

        The SQL Server ODBC SQL path cannot carry the emoji literal, so the
        declared equality filter must be evaluated locally instead of being
        rejected or sent to the remote engine.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        ref = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str.id"
        tdSql.execute(f"USE {_VT_US}")
        self._drop_vtables(_VT_US)

        tdSql.execute(
            f"CREATE VTABLE vt_emoji (ts TIMESTAMP, id INT FROM {ref} "
            f"WHERE note = '😀x')")
        local_rows = self._remote_rows(
            f"SELECT id FROM {_LOCAL_DB}.reg_str WHERE note = '😀x' ORDER BY id")
        ext_rows = self._remote_rows("SELECT id FROM vt_emoji ORDER BY id")
        assert ext_rows == local_rows, (
            f"non-BMP declared filter returned {ext_rows!r}, "
            f"local result is {local_rows!r}")

        self._drop_vtables(_VT_US)

    def test_declared_mixed_filter_splits_pushdown(self):
        """A mixed AND filter pushes the exact part and filters the rest locally.

        `grp = 1` is exactly renderable and stays in the remote WHERE, while
        `label > 'Abc'` must be applied by the local FederatedScan filter.  The
        combined result must equal the local TDengine result.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        ref = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str.id"
        tdSql.execute(f"USE {_VT_US}")
        self._drop_vtables(_VT_US)

        tdSql.execute(
            f"CREATE VTABLE vt_mixed (ts TIMESTAMP, id INT FROM {ref} "
            f"WHERE grp = 1 AND label > 'Abc')")
        local_rows = self._remote_rows(
            f"SELECT id FROM {_LOCAL_DB}.reg_str "
            f"WHERE grp = 1 AND label > 'Abc' ORDER BY id")
        ext_rows = self._remote_rows("SELECT id FROM vt_mixed ORDER BY id")
        assert ext_rows == local_rows, (
            f"mixed declared filter returned {ext_rows!r}, "
            f"local result is {local_rows!r}")

        self._drop_vtables(_VT_US)

    # ------------------------------------------------------------------
    # 3. EXISTS / IN subqueries over SQL Server
    # ------------------------------------------------------------------

    def test_exists_and_in_subqueries(self):
        """EXISTS / IN subqueries over SQL Server return local results.

        The parser rewrites EXISTS into REMOTE_ZERO_ROWS and IN into
        REMOTE_VALUE_LIST; with the SQL Server dialect profile loaded on the
        vtable reference, both must return the same rows as the local plan.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        remote = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str"

        local_exists = self._remote_rows(
            f"SELECT id FROM {_LOCAL_DB}.reg_str "
            f"WHERE EXISTS (SELECT 1 FROM {_LOCAL_DB}.reg_str r WHERE r.id = 4) "
            f"ORDER BY id")
        ext_exists = self._remote_rows(
            f"SELECT id FROM {remote} "
            f"WHERE EXISTS (SELECT 1 FROM {_SRC}.{_REMOTE_DB}.dbo.reg_us u "
            f"WHERE u.id = 1) ORDER BY id")
        assert ext_exists == local_exists, (
            f"EXISTS over SQL Server returned {ext_exists!r}, "
            f"local result is {local_exists!r}")

        local_in = self._remote_rows(
            f"SELECT id FROM {_LOCAL_DB}.reg_str "
            f"WHERE id IN (SELECT id FROM {_LOCAL_DB}.reg_str WHERE id <= 3) "
            f"ORDER BY id")
        ext_in = self._remote_rows(
            f"SELECT id FROM {remote} "
            f"WHERE id IN (SELECT id FROM {_SRC}.{_REMOTE_DB}.dbo.reg_us) "
            f"ORDER BY id")
        expected_in = []
        for row in local_in:
            if row in [(1,), (2,)]:
                expected_in.append(row)
        assert ext_in == expected_in, (
            f"IN (subquery) over SQL Server returned {ext_in!r}, "
            f"expected {expected_in!r}")

        ext_in_local = self._remote_rows(
            f"SELECT id FROM {remote} "
            f"WHERE id IN (SELECT id FROM {_LOCAL_DB}.reg_str WHERE id <= 3) "
            f"ORDER BY id")
        assert ext_in_local == local_in, (
            f"IN (local subquery) over SQL Server returned {ext_in_local!r}, "
            f"local result is {local_in!r}")

    # ------------------------------------------------------------------
    # 4. timestamp push-down keeps microseconds
    # ------------------------------------------------------------------

    def test_timestamp_precision_pushdown(self):
        """TO_UNIXTIMESTAMP / SPREAD / CAST keep the microsecond values.

        The SQL Server transport precision is microsecond regardless of the
        column declaration; a millisecond default in the renderer would
        truncate datetime2(6) values.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        remote3 = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str"
        remote6 = f"{_SRC}.{_REMOTE_DB}.dbo.reg_us"

        rows = self._remote_rows(
            f"SELECT id, TO_UNIXTIMESTAMP(ts) FROM {remote3} "
            f"WHERE id <= 2 ORDER BY id")
        assert rows == [(1, 1704067200000000), (2, 1704067260000000)], rows

        rows = self._remote_rows(
            f"SELECT id, TO_UNIXTIMESTAMP(ts) FROM {remote6} ORDER BY id")
        assert rows == [(1, 1704067200254321), (2, 1704067201900001)], rows

        rows = self._remote_rows(
            f"SELECT id, CAST(ts AS BIGINT) FROM {remote6} ORDER BY id")
        assert rows == [(1, 1704067200254321), (2, 1704067201900001)], rows

        rows = self._remote_rows(f"SELECT SPREAD(ts) FROM {remote6}")
        assert rows == [(1645680.0,)], rows

    # ------------------------------------------------------------------
    # 5. ms / us / ns databases over datetime2(3)
    # ------------------------------------------------------------------

    def test_datetime2_3_vtable_across_precisions(self):
        """ms / us / ns databases accept datetime2(3) and agree on the rows.

        The parser-side extTimestampTypePrecision() validation covers SQL Server
        at DDL and query time: datetime2(3) is millisecond aligned, so every
        database precision accepts it, while datetime2(6) still needs us/ns.

        Catalog:
            - Query:FederatedSqlServerSemantics

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 dapan1121 Regression coverage for the vtable audit findings

        """
        ref = f"{_SRC}.{_REMOTE_DB}.dbo.reg_str.id"
        expected = self._remote_rows(
            f"SELECT id FROM {_LOCAL_DB}.reg_str ORDER BY id")

        for db in (_VT_MS, _VT_US, _VT_NS):
            self._drop_vtables(db)
            tdSql.execute(
                f"CREATE VTABLE {db}.vt_all (ts TIMESTAMP, id INT FROM {ref})")
            rows = self._remote_rows(f"SELECT id FROM {db}.vt_all ORDER BY id")
            assert rows == expected, f"{db}.vt_all mismatch: {rows!r}"
            rows = self._remote_rows(
                f"SELECT id FROM {db}.vt_all "
                f"WHERE ts = '2024-01-01 00:00:00.000'")
            assert rows == [(1,)], f"{db}.vt_all ts filter mismatch: {rows!r}"
            self._drop_vtables(db)

        # The finer datetime2(6) axis cannot be referenced from a ms database.
        tdSql.execute(
            f"CREATE VTABLE {_VT_US}.vt_us6 (ts TIMESTAMP, id INT FROM "
            f"{_SRC}.{_REMOTE_DB}.dbo.reg_us.id)")
        tdSql.query(f"SELECT id FROM {_VT_US}.vt_us6 ORDER BY id")
        tdSql.checkRows(2)
        self._drop_vtables(_VT_US)

        tdSql.execute(f"USE {_VT_MS}")
        tdSql.error(
            f"CREATE VTABLE vt_ms6 (ts TIMESTAMP, id INT FROM "
            f"{_SRC}.{_REMOTE_DB}.dbo.reg_us.id)",
            expectedErrno=0x268f)
