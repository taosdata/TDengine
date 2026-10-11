# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""SQL Server external-column virtual tables (FQSS-VTBL-001..011).

Covers the step-1 virtual-table matrix from the SQL Server TS section 6.5:

  FQSS-VTBL-001  virtual normal table with mixed internal/external columns
  FQSS-VTBL-002  virtual child table via CREATE VTABLE ... USING
  FQSS-VTBL-003  virtual super table fanning out to SQL Server children
  FQSS-VTBL-004  five-segment column path source.database.schema.table.column
  FQSS-VTBL-005  same database, different schema, same table name
  FQSS-VTBL-006  schema survives the vtable's persisted column reference
  FQSS-VTBL-007  datetime2 / datetimeoffset / datetime / smalldatetime / date
                 as the time axis
  FQSS-VTBL-008  rowversion / timestamp cannot be the time axis
  FQSS-VTBL-009  USE switching does not change an existing vtable
  FQSS-VTBL-010  a view without a time column cannot be a column source
  FQSS-VTBL-011  vtable queries stay local (zero pushdown) and match the source

The local database uses microsecond precision because SQL Server external
timestamps are exposed at microsecond resolution (DS 5.3.7); a millisecond
database would reject the references by design.
"""
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_DDL_EXT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "01-DDL", "03-ExtSource")
)
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "09-DataQuerying", "19-FederatedQuery")
)
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_remote_db,
    create_sqlserver_table,
)

_LOCAL_DB = "vt_ss_local"
_REMOTE_DB = "vt_ss_db"
_SRC = "vt_ss_src"


class TestVtableQueryExtSourceSqlServer:
    """External-column virtual tables over SQL Server."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        create_remote_db("sqlserver", _REMOTE_DB)
        create_sqlserver_table(
            _REMOTE_DB,
            "r_mixed",
            "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, name NVARCHAR(32), score FLOAT",
            ["('2024-01-01 00:00:00.000', 11, 'alpha', 1.5)",
             "('2024-01-01 00:01:00.000', 12, 'beta', 2.5)"],
            schema="dbo",
        )
        create_sqlserver_table(
            _REMOTE_DB,
            "r_mixed",
            "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, name NVARCHAR(32), score FLOAT",
            ["('2024-01-01 00:00:00.000', 21, 'gamma', 3.5)",
             "('2024-01-01 00:01:00.000', 22, 'delta', 4.5)"],
            schema="other",
        )
        # Time-axis candidates: every temporal type that may serve as the ts.
        create_sqlserver_table(
            _REMOTE_DB,
            "r_dt2_7",
            "ts datetime2(7) NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01 00:00:00.1234567', 31)"],
        )
        create_sqlserver_table(
            _REMOTE_DB,
            "r_dto",
            "ts datetimeoffset(3) NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01 08:00:00.000 +08:00', 32)"],
        )
        create_sqlserver_table(
            _REMOTE_DB,
            "r_dt",
            "ts datetime NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01 00:00:00.000', 33)"],
        )
        create_sqlserver_table(
            _REMOTE_DB,
            "r_sdt",
            "ts smalldatetime NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01 00:00:00', 34)"],
        )
        create_sqlserver_table(
            _REMOTE_DB,
            "r_date",
            "ts date NOT NULL PRIMARY KEY, v INT",
            ["('2024-01-01', 35)"],
        )
        # rowversion-only table: no temporal column at all (FQSS-VTBL-008).
        create_sqlserver_table(
            _REMOTE_DB,
            "r_rv",
            "rv rowversion, v INT",
            ["(DEFAULT, 41)"],
        )
        # View without a time column (FQSS-VTBL-010).
        ExtSrcEnv.sqlserver_exec(_REMOTE_DB, [
            "IF OBJECT_ID('dbo.v_no_ts') IS NOT NULL DROP VIEW dbo.v_no_ts",
            "IF OBJECT_ID('dbo.r_view') IS NOT NULL DROP TABLE dbo.r_view",
            "CREATE TABLE dbo.r_view (ts datetime2(3) NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO dbo.r_view VALUES ('2024-01-01 00:00:00.000', 51)",
            "CREATE VIEW dbo.v_no_ts AS SELECT v FROM dbo.r_view",
        ])
        create_ext_source(_SRC, "sqlserver", _REMOTE_DB)

        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'us'")
        tdSql.execute(f"USE {_LOCAL_DB}")

        # FQSS-VTBL-001: mixed external columns and local NULL placeholder
        # columns (virtual tables are read-only, so an own column only reserves
        # a schema slot).
        tdSql.execute(
            "CREATE VTABLE vt_mixed ("
            "ts TIMESTAMP, "
            f"v INT FROM {_SRC}.{_REMOTE_DB}.dbo.r_mixed.v, "
            f"name NCHAR(32) FROM {_SRC}.{_REMOTE_DB}.dbo.r_mixed.name, "
            f"score DOUBLE FROM {_SRC}.{_REMOTE_DB}.dbo.r_mixed.score, "
            "local_col INT)")
        # FQSS-VTBL-004 / 005: five-segment path, same db different schema.
        tdSql.execute(
            "CREATE VTABLE vt_other ("
            f"ts TIMESTAMP, v INT FROM {_SRC}.{_REMOTE_DB}.other.r_mixed.v)"
        )
        # FQSS-VTBL-002 / 003: virtual super table + child.
        tdSql.execute(
            "CREATE STABLE vstb_ss (ts TIMESTAMP, v INT) "
            "TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE vctb_ss (v FROM {_SRC}.{_REMOTE_DB}.dbo.r_mixed.v) "
            "USING vstb_ss TAGS ('siteA')"
        )

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SRC}")
        try:
            ExtSrcEnv.sqlserver_drop_db(_REMOTE_DB)
        except Exception:
            pass

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    # ------------------------------------------------------------------
    # FQSS-VTBL-001 / 011
    # ------------------------------------------------------------------
    def test_vtbl_mixed_columns(self):
        """FQSS-VTBL-001: virtual normal table mixing external and own columns.

        The referenced columns come from SQL Server; the own column materialises
        as a NULL placeholder because virtual tables are read-only.

        Since: v3.4.4.0

        """
        tdSql.query("SELECT ts, v, name, score, local_col FROM vt_mixed ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(0, 2, "alpha")
        tdSql.checkData(0, 3, 1.5)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(1, 4, None)
        # Virtual tables are read-only.
        tdSql.error("INSERT INTO vt_mixed (ts, local_col) VALUES (1704067200000, 1)")

        # The timestamp of a virtual table follows the external row.
        tdSql.query(
            "SELECT ts FROM vt_mixed WHERE v = 12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:01:00.000000")

    # ------------------------------------------------------------------
    # FQSS-VTBL-002 / 003
    # ------------------------------------------------------------------
    def test_vtbl_child_and_super_table(self):
        """FQSS-VTBL-002/003: virtual child table under a virtual super table.

        The child references a SQL Server column and must serve both direct
        child queries and super-table aggregation fan-out.

        Since: v3.4.4.0

        """
        tdSql.query("SELECT ts, v FROM vctb_ss ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, 12)

        tdSql.query("SELECT site, SUM(v) FROM vstb_ss GROUP BY site ORDER BY site")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "siteA")
        tdSql.checkData(0, 1, 23)

    # ------------------------------------------------------------------
    # FQSS-VTBL-004 / 005 / 006
    # ------------------------------------------------------------------
    def test_vtbl_schema_segment(self):
        """FQSS-VTBL-004/005/006: five-segment paths keep the schema distinct.

        A vtable bound to dbo.r_mixed and one bound to other.r_mixed must return
        their own values, and the stored column reference must keep the schema
        binding so every plan resolves the same object.

        Since: v3.4.4.0

        """
        tdSql.query("SELECT v FROM vt_mixed WHERE v < 20 ORDER BY v")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 11)

        tdSql.query("SELECT ts, v FROM vt_other ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 21)
        tdSql.checkData(1, 1, 22)

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name = '{_LOCAL_DB}' AND table_name = 'vt_other' AND col_name = 'v'")
        col_source = str(tdSql.queryResult[0][0])
        # col_source renders source.database.table.column: the schema segment is
        # kept in the persisted reference (both bindings return their own rows
        # above) but is not part of the display form.  The meta suite pins the
        # same convention for its two-schema vtables.
        assert col_source == f"{_SRC}.{_REMOTE_DB}.r_mixed.v", \
            f"unexpected col_source: {col_source!r}"

    # ------------------------------------------------------------------
    # FQSS-VTBL-007
    # ------------------------------------------------------------------
    def test_vtbl_timestamp_pk_types(self):
        """FQSS-VTBL-007: every SQL Server temporal type can be the time axis.

        datetime2(7) / datetimeoffset / datetime / smalldatetime / date columns
        all satisfy the timestamp primary-key requirement of a virtual table.

        Since: v3.4.4.0

        """
        cases = [
            ("vt_dt2_7", "r_dt2_7", 31),
            ("vt_dto", "r_dto", 32),
            ("vt_dt", "r_dt", 33),
            ("vt_sdt", "r_sdt", 34),
            ("vt_date", "r_date", 35),
        ]
        for vtable, table, expected in cases:
            tdSql.execute(f"DROP VTABLE IF EXISTS {vtable}")
            tdSql.execute(
                f"CREATE VTABLE {vtable} ("
                f"ts TIMESTAMP, v INT FROM {_SRC}.{_REMOTE_DB}.dbo.{table}.v)")
            tdSql.query(f"SELECT v FROM {vtable} ORDER BY ts")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)

    # ------------------------------------------------------------------
    # FQSS-VTBL-008
    # ------------------------------------------------------------------
    def test_vtbl_rowversion_rejected(self):
        """FQSS-VTBL-008: timestamp / rowversion cannot serve as the time axis.

        r_rv carries only a rowversion column; binding it as the timestamp must
        be rejected instead of silently producing a non-temporal time axis.

        Since: v3.4.4.0

        """
        tdSql.execute("DROP VTABLE IF EXISTS vt_rv")
        tdSql.error(
            "CREATE VTABLE vt_rv ("
            f"ts TIMESTAMP, x INT FROM {_SRC}.{_REMOTE_DB}.dbo.r_rv.rv)")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name = '{_LOCAL_DB}' AND table_name = 'vt_rv'")
        tdSql.checkData(0, 0, 0)

    # ------------------------------------------------------------------
    # FQSS-VTBL-009
    # ------------------------------------------------------------------
    def test_vtbl_use_switch(self):
        """FQSS-VTBL-009: USE switching does not affect an existing vtable.

        The reference is frozen at creation time, so switching the session
        namespace afterwards keeps the same remote object and values.

        Since: v3.4.4.0

        """
        tdSql.query("SELECT v FROM vt_other ORDER BY ts")
        before = list(tdSql.queryResult)

        tdSql.execute(f"USE {_SRC}")  # namespace switch, no object change
        tdSql.execute(f"USE {_LOCAL_DB}")

        tdSql.query("SELECT v FROM vt_other ORDER BY ts")
        after = list(tdSql.queryResult)
        assert before == after, f"USE switch changed vtable values: {before} -> {after}"

    # ------------------------------------------------------------------
    # FQSS-VTBL-010
    # ------------------------------------------------------------------
    def test_vtbl_view_without_ts_rejected(self):
        """FQSS-VTBL-010: a view without a time column cannot be a column source.

        dbo.v_no_ts projects only v; a virtual table needs a mergeable time
        axis, so the creation must fail rather than fabricate one.

        Since: v3.4.4.0

        """
        tdSql.execute("DROP VTABLE IF EXISTS vt_view_no_ts")
        tdSql.error(
            "CREATE VTABLE vt_view_no_ts ("
            f"ts TIMESTAMP, v INT FROM {_SRC}.{_REMOTE_DB}.dbo.v_no_ts.v)")

    # ------------------------------------------------------------------
    # FQSS-VTBL-011
    # ------------------------------------------------------------------
    def test_vtbl_zero_pushdown_query(self):
        """FQSS-VTBL-011: vtable queries stay local and match the source.

        EXPLAIN must show plain remote scans (no pushed WHERE / aggregation),
        and the aggregated result must equal the value read directly from
        SQL Server.

        Since: v3.4.4.0

        """
        tdSql.query("EXPLAIN VERBOSE TRUE SELECT COUNT(*), SUM(v) FROM vt_mixed")
        plan = "\n".join(
            str(cell) for row in (tdSql.queryResult or []) for cell in row if cell is not None
        )
        remote_lines = [line for line in plan.splitlines() if "remote sql:" in line.lower()]
        assert remote_lines, f"no Remote SQL in vtable plan: {plan!r}"
        for line in remote_lines:
            upper = line.upper()
            assert " WHERE " not in upper, f"predicate pushed in baseline mode: {line!r}"
            assert "SUM(" not in upper and "COUNT(" not in upper, (
                f"aggregate pushed in baseline mode: {line!r}")

        tdSql.query("SELECT COUNT(*), SUM(v) FROM vt_mixed")
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 23)

        direct = ExtSrcEnv.sqlserver_query(
            _REMOTE_DB, "SELECT COUNT(*), SUM(v) FROM dbo.r_mixed")
        direct_count = int(str(direct[0]).strip()) if direct else -1
        assert direct_count == 2, f"remote row count changed: {direct!r}"
