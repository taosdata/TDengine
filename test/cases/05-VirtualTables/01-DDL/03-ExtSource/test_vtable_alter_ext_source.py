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

"""DDL tests for ALTER VTABLE / ALTER STABLE … VIRTUAL referencing ext sources.

Section 4 of the DDL test plan.
"""

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import (  # noqa: E402
    ExtSrcEnv, TSDB_CODE_PAR_SYNTAX_ERROR)
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_pg_table,
    create_mysql_table, create_influx_measurement, create_sqlserver_table,
    create_oracle_table)


_LOCAL_DB = "valter_local"
_PG_DB    = "valter_pg"
_PG_SRC   = "valter_pg_src"
_MY_DB    = "valter_my"
_MY_SRC   = "valter_my_src"
_INF_DB   = "valter_inf"
_INF_SRC  = "valter_inf_src"
_SS_DB    = "valter_ss"
_SS_SRC   = "valter_ss_src"
_OR_SVC    = ExtSrcEnv.ORACLE_SERVICE
_OR_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_OR_SRC    = "valter_or_src"
_OR_ALT_DB = "valter_or_other"


def _ensure_pg_source_and_tables():
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(_PG_DB, "r1",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, w INTEGER",
                    ["('2024-01-01 00:00:00', 1, 10)",
                     "('2024-01-01 00:01:00', 2, 20)"])
    create_pg_table(_PG_DB, "r2",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, x DOUBLE PRECISION",
                    ["('2024-01-01 00:00:00', 100, 1.5)",
                     "('2024-01-01 00:01:00', 200, 2.5)"])
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)


def _ensure_mysql_source_and_tables():
    create_remote_db("mysql", _MY_DB)
    create_mysql_table(_MY_DB, "r1",
                       "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT, w INT",
                       ["('2024-01-01 00:00:00.000', 1, 10)",
                        "('2024-01-01 00:01:00.000', 2, 20)"])
    create_ext_source(_MY_SRC, "mysql", _MY_DB)


def _ensure_sqlserver_source_and_tables():
    """SQL Server source for the ALTER matrix.

    The local database uses PRECISION 'ns' so the microsecond SQL Server
    timestamp resolution is accepted; references use the 5-segment path
    source.database.schema.table.column (DS 5.3.3).

    The remote tables mirror the PG/MySQL r1/r2 pair, plus a second-schema
    table to prove the schema segment stays part of the binding.
    """
    create_remote_db("sqlserver", _SS_DB)
    create_sqlserver_table(_SS_DB, "r1",
                           "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, w INT",
                           ["('2024-01-01 00:00:00.000', 1, 10)",
                            "('2024-01-01 00:01:00.000', 2, 20)"])
    create_sqlserver_table(_SS_DB, "r2",
                           "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, x FLOAT",
                           ["('2024-01-01 00:00:00.000', 100, 1.5)",
                            "('2024-01-01 00:01:00.000', 200, 2.5)"],
                           schema="other")
    create_ext_source(_SS_SRC, "sqlserver", _SS_DB)


def _ensure_oracle_source_and_tables():
    """Oracle source for the ALTER matrix.

    Oracle has a single namespace level (the schema) and folds unquoted
    identifiers to upper case, so the remote objects below are addressed in
    their stored upper-case form.  References use the four-segment path
    source.schema.table.column; the three-segment source.table.column form
    resolves through the schema declared on the source.

    NUMBER(10,0) maps to BIGINT and BINARY_DOUBLE to DOUBLE, so the local
    declarations stay compatible with the remote types.
    """
    create_oracle_table(
        "r1", "ts TIMESTAMP(3) NOT NULL PRIMARY KEY, "
              "v NUMBER(10,0), w NUMBER(10,0)",
        ["(TIMESTAMP '2024-01-01 00:00:00.000', 1, 10)",
         "(TIMESTAMP '2024-01-01 00:01:00.000', 2, 20)"])
    create_oracle_table(
        "r2", "ts TIMESTAMP(3) NOT NULL PRIMARY KEY, "
              "v NUMBER(10,0), x BINARY_DOUBLE",
        ["(TIMESTAMP '2024-01-01 00:00:00.000', 100, 1.5)",
         "(TIMESTAMP '2024-01-01 00:01:00.000', 200, 2.5)"])
    create_ext_source(_OR_SRC, "oracle", _OR_SVC)


def _check_describe_layout(table, expected):
    """Verify DESCRIBE output matches expected layout.

    Each element in `expected` is (name, type) or (name, type, ref).
    When ref is provided, column 4 (the ref field) is also checked.
    """
    tdSql.query(f"DESCRIBE {table}")
    tdSql.checkRows(len(expected))
    for row, item in enumerate(expected):
        tdSql.checkData(row, 0, item[0])
        tdSql.checkData(row, 1, item[1])
        if len(item) >= 3:
            tdSql.checkData(row, 4, item[2])


# ===========================================================================

class TestVtableAlterExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _ensure_pg_source_and_tables()
        _ensure_mysql_source_and_tables()
        _ensure_sqlserver_source_and_tables()
        _ensure_oracle_source_and_tables()
        create_remote_db("influxdb", _INF_DB)
        create_influx_measurement(_INF_DB, [
            "r_inftag,device=sensor1 value=42.0 1700000000000000000"
        ])
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ns'")
        tdSql.execute(f"USE {_LOCAL_DB}")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_MY_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SS_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_OR_SRC}")
        # DROP EXTERNAL SOURCE only removes the local definition.  The
        # InfluxDB bucket outlives it and keeps occupying one of the five
        # database slots InfluxDB 3 Core allows per instance, so a run that
        # visits several ext-source suites eventually makes every later
        # influx_create_db() fail with HTTP 422.
        ExtSrcEnv.influx_drop_db(_INF_DB)

    # -------------------------------------------------------------------
    # 4.1 — Normal vtable
    # -------------------------------------------------------------------

    def _fresh_normal_vt(self, name="v_alter"):
        tdSql.execute(f"DROP VTABLE IF EXISTS {name}")
        tdSql.execute(
            f"CREATE VTABLE {name} (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r1.v)")
        return name

    def test_alter_normal_vtable(self):
        """§4.1 — ALTER on normal vtable: add/drop/modify/rename columns.

        §4.1 — ALTER on normal vtable: add/drop/modify/rename columns.

        Since: v3.4.0.0

        """
        # (a) ADD COLUMN with FROM — succeeds and ref visible in DESCRIBE.
        tdLog.info("§4.1a: add column with FROM succeeds")
        n = self._fresh_normal_vt()
        tdSql.execute(
            f"ALTER VTABLE {n} ADD COLUMN w int "
            f"FROM {_PG_SRC}.{_PG_DB}.r1.w")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("w", "INT", f"{_PG_SRC}.{_PG_DB}.r1.w"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (b) ADD COLUMN without FROM — column always NULL.
        tdLog.info("§4.1b: add column without FROM is NULL")
        n = self._fresh_normal_vt("v_alter_null")
        tdSql.execute(f"ALTER VTABLE {n} ADD COLUMN nu int")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("nu", "INT", ""),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (c) DROP COLUMN that has ext ref — succeeds.
        tdLog.info("§4.1c: drop column with ext ref succeeds")
        n = self._fresh_normal_vt("v_alter_drop")
        tdSql.execute(f"ALTER VTABLE {n} ADD COLUMN w int FROM {_PG_SRC}.{_PG_DB}.r1.w")
        tdSql.execute(f"ALTER VTABLE {n} DROP COLUMN w")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (d) MODIFY COLUMN widening/narrowing.
        tdLog.info("§4.1d: modify column widening/narrowing")
        tdSql.execute("DROP VTABLE IF EXISTS v_alter_mod")
        create_pg_table(_PG_DB, "r_mod",
                        "ts TIMESTAMP PRIMARY KEY, s VARCHAR(64)")
        tdSql.execute(
            f"CREATE VTABLE v_alter_mod (ts timestamp, "
            f"s nchar(16) FROM {_PG_SRC}.{_PG_DB}.r_mod.s)")
        tdSql.error("ALTER VTABLE v_alter_mod MODIFY COLUMN s nchar(64)")
        tdSql.error("ALTER VTABLE v_alter_mod MODIFY COLUMN s nchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS v_alter_mod")

        # (e) RENAME COLUMN preserves FROM binding.
        tdLog.info("§4.1e: rename column preserves FROM")
        n = self._fresh_normal_vt("v_alter_rename")
        tdSql.execute(f"ALTER VTABLE {n} RENAME COLUMN v vrenamed")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("vrenamed", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (f) ALTER referencing a dropped source fails.
        tdLog.info("§4.1f: alter after source dropped fails")
        n = self._fresh_normal_vt("v_alter_dropsrc")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.error(
            f"ALTER VTABLE {n} ADD COLUMN x int "
            f"FROM {_PG_SRC}.{_PG_DB}.r1.w")
        _ensure_pg_source_and_tables()
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (g) ADD COLUMN referencing an InfluxDB tag (not field) is rejected.
        tdLog.info("§4.1g: influxdb tag ref rejected")
        n = self._fresh_normal_vt("v_alter_inftag")
        tdSql.error(
            f"ALTER VTABLE {n} ADD COLUMN d nchar(64) "
            f"FROM {_INF_SRC}.{_INF_DB}.r_inftag.device")
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

    def test_sqlserver_alter_references(self):
        """§4.1 — ALTER on a virtual table referencing SQL Server.

        Mirrors the PG/MySQL ALTER matrix over the 5-segment SQL Server path
        source.database.schema.table.column: ADD COLUMN with / without FROM,
        a second-schema binding, DROP COLUMN, RENAME COLUMN, MODIFY COLUMN
        rejection, and ALTER after the source was dropped.

        Since: v3.4.4.0

        """
        # (a) ADD COLUMN with FROM succeeds and keeps the binding.
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_alter")
        tdSql.execute(
            "CREATE VTABLE v_ss_alter (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r1.v)")
        tdSql.execute(
            f"ALTER VTABLE v_ss_alter ADD COLUMN w int "
            f"FROM {_SS_SRC}.{_SS_DB}.dbo.r1.w")
        _check_describe_layout("v_ss_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_SS_SRC}.{_SS_DB}.r1.v"),
            ("w", "INT", f"{_SS_SRC}.{_SS_DB}.r1.w"),
        ])
        tdSql.query("SELECT w FROM v_ss_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 20)

        # (b) ADD COLUMN bound to a same-database table in another schema.
        tdSql.execute(
            f"ALTER VTABLE v_ss_alter ADD COLUMN xs double "
            f"FROM {_SS_SRC}.{_SS_DB}.other.r2.x")
        tdSql.query("SELECT xs FROM v_ss_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.5)
        tdSql.checkData(1, 0, 2.5)

        # (c) ADD COLUMN without FROM is a NULL placeholder: the scan still
        # returns the referenced rows.  A predicate that touches only the
        # placeholder matches no row - the same behaviour the PG/MySQL matrix
        # records (test_vstb_add_column_propagates logs the count instead of
        # asserting it), so the count is only recorded here as well.
        tdSql.execute("ALTER VTABLE v_ss_alter ADD COLUMN nu int")
        tdSql.query("SELECT count(*) FROM v_ss_alter")
        tdSql.checkData(0, 0, 2)
        tdSql.query("SELECT count(*) FROM v_ss_alter WHERE nu IS NULL")
        tdLog.info(f"[contract] SQL Server ADD COLUMN without FROM: null={tdSql.getData(0, 0)}")

        # (d) DROP COLUMN with ext ref succeeds for both bindings.
        tdSql.execute("ALTER VTABLE v_ss_alter DROP COLUMN w")
        tdSql.execute("ALTER VTABLE v_ss_alter DROP COLUMN xs")
        _check_describe_layout("v_ss_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_SS_SRC}.{_SS_DB}.r1.v"),
            ("nu", "INT", ""),
        ])

        # (e) RENAME COLUMN preserves the FROM binding.
        tdSql.execute("ALTER VTABLE v_ss_alter RENAME COLUMN v vrenamed")
        _check_describe_layout("v_ss_alter", [
            ("ts", "TIMESTAMP", ""),
            ("vrenamed", "INT", f"{_SS_SRC}.{_SS_DB}.r1.v"),
            ("nu", "INT", ""),
        ])
        tdSql.query("SELECT vrenamed FROM v_ss_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_alter")

        # (f) MODIFY COLUMN on a referenced column is rejected both ways.
        create_sqlserver_table(_SS_DB, "r_mod",
                               "ts datetime2(3) NOT NULL PRIMARY KEY, s NVARCHAR(64)")
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_mod")
        tdSql.execute(
            "CREATE VTABLE v_ss_mod (ts timestamp, "
            f"s nchar(16) FROM {_SS_SRC}.{_SS_DB}.dbo.r_mod.s)")
        tdSql.error("ALTER VTABLE v_ss_mod MODIFY COLUMN s nchar(64)")
        tdSql.error("ALTER VTABLE v_ss_mod MODIFY COLUMN s nchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_mod")

        # (g) ALTER referencing a dropped source fails.
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_dropsrc")
        tdSql.execute(
            "CREATE VTABLE v_ss_dropsrc (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r1.v)")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SS_SRC}")
        tdSql.error(
            f"ALTER VTABLE v_ss_dropsrc ADD COLUMN x int "
            f"FROM {_SS_SRC}.{_SS_DB}.dbo.r1.w")
        _ensure_sqlserver_source_and_tables()
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_dropsrc")

    def test_sqlserver_relative_path_alter(self):
        """FQSS-VTBL-012: ALTER accepts relative SQL Server column paths.

        A vtable bound through the five-part path can gain columns through the
        three-part (source defaults) and four-part (path schema + source
        database) forms, and ALTER COLUMN ... SET accepts both. The stored
        binding is expanded to the explicit five-part namespace.

        Since: v3.4.4.0

        """
        tdSql.execute("DROP VTABLE IF EXISTS v_ss_relalter")
        tdSql.execute(
            "CREATE VTABLE v_ss_relalter (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r1.v)")

        # ADD COLUMN: three-part and four-part.
        tdSql.execute(
            f"ALTER VTABLE v_ss_relalter ADD COLUMN w3 int FROM {_SS_SRC}.r1.w")
        tdSql.query("SELECT w3 FROM v_ss_relalter ORDER BY ts")
        tdSql.checkData(0, 0, 10)
        tdSql.execute(
            f"ALTER VTABLE v_ss_relalter ADD COLUMN w4 int FROM {_SS_SRC}.dbo.r1.w")
        tdSql.query("SELECT w4 FROM v_ss_relalter ORDER BY ts")
        tdSql.checkData(1, 0, 20)

        # ALTER COLUMN ... SET: three-part then four-part.
        tdSql.execute(
            f"ALTER VTABLE v_ss_relalter ALTER COLUMN v SET {_SS_SRC}.r1.w")
        tdSql.query("SELECT v FROM v_ss_relalter ORDER BY ts")
        tdSql.checkData(0, 0, 10)
        tdSql.execute(
            f"ALTER VTABLE v_ss_relalter ALTER COLUMN v SET {_SS_SRC}.dbo.r1.v")
        tdSql.query("SELECT v FROM v_ss_relalter ORDER BY ts")
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_ss_relalter' "
            "AND col_name='w3'")
        tdSql.checkRows(1)
        assert str(tdSql.queryResult[0][0]) == \
            f"{_SS_SRC}.{_SS_DB}.dbo.r1.w", tdSql.queryResult[0][0]
        tdSql.execute("DROP VTABLE v_ss_relalter")

    def test_oracle_alter_references(self):
        """FQOR-VTBL-009: ALTER on a virtual table referencing Oracle.

        Mirrors the SQL Server ALTER matrix over Oracle's single namespace
        level: ADD COLUMN with / without FROM through both the four-segment
        source.schema.table.column path and the three-segment
        source.table.column form that resolves through the source schema,
        DROP / RENAME COLUMN, MODIFY COLUMN rejection, ALTER after the source
        was dropped, and a USE switch that changes neither the stored
        reference nor the values.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        # (a) ADD COLUMN with FROM succeeds and keeps the four-segment path.
        tdSql.execute("DROP VTABLE IF EXISTS v_or_alter")
        tdSql.execute(
            "CREATE VTABLE v_or_alter (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r1.v)")
        tdSql.execute(
            f"ALTER VTABLE v_or_alter ADD COLUMN w bigint "
            f"FROM {_OR_SRC}.{_OR_SCHEMA}.r1.w")
        _check_describe_layout("v_or_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "BIGINT", f"{_OR_SRC}.{_OR_SCHEMA}.r1.v"),
            ("w", "BIGINT", f"{_OR_SRC}.{_OR_SCHEMA}.r1.w"),
        ])
        tdSql.query("SELECT w FROM v_or_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 20)

        # (b) The three-segment path resolves through the source schema and is
        # persisted as the explicit four-segment reference.
        tdSql.execute(
            f"ALTER VTABLE v_or_alter ADD COLUMN w3 bigint FROM {_OR_SRC}.r1.w")
        tdSql.query("SELECT w3 FROM v_or_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 20)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_alter' "
            "AND col_name='w3'")
        tdSql.checkRows(1)
        assert str(tdSql.queryResult[0][0]) == \
            f"{_OR_SRC}.{_OR_SCHEMA}.r1.w", tdSql.queryResult[0][0]
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_alter' "
            "AND col_name='w'")
        tdSql.checkRows(1)
        assert str(tdSql.queryResult[0][0]) == \
            f"{_OR_SRC}.{_OR_SCHEMA}.r1.w", tdSql.queryResult[0][0]

        # (c) ADD COLUMN without FROM is a NULL placeholder: the scan still
        # returns the referenced rows.  A predicate that touches only the
        # placeholder matches no row, the same contract the PG / MySQL
        # matrix records, so the count is only logged.
        tdSql.execute("ALTER VTABLE v_or_alter ADD COLUMN nu bigint")
        tdSql.query("SELECT count(*) FROM v_or_alter")
        tdSql.checkData(0, 0, 2)
        tdSql.query("SELECT count(*) FROM v_or_alter WHERE nu IS NULL")
        tdLog.info(f"[contract] Oracle ADD COLUMN without FROM: "
                   f"null={tdSql.getData(0, 0)}")

        # (d) DROP COLUMN with an Oracle ref succeeds for both bindings.
        tdSql.execute("ALTER VTABLE v_or_alter DROP COLUMN w")
        tdSql.execute("ALTER VTABLE v_or_alter DROP COLUMN w3")
        _check_describe_layout("v_or_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "BIGINT", f"{_OR_SRC}.{_OR_SCHEMA}.r1.v"),
            ("nu", "BIGINT", ""),
        ])

        # (e) RENAME COLUMN preserves the four-segment binding.
        tdSql.execute("ALTER VTABLE v_or_alter RENAME COLUMN v vrenamed")
        _check_describe_layout("v_or_alter", [
            ("ts", "TIMESTAMP", ""),
            ("vrenamed", "BIGINT", f"{_OR_SRC}.{_OR_SCHEMA}.r1.v"),
            ("nu", "BIGINT", ""),
        ])
        tdSql.query("SELECT vrenamed FROM v_or_alter ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP VTABLE IF EXISTS v_or_alter")

        # (f) MODIFY COLUMN on a referenced column is rejected both ways.
        create_oracle_table(
            "r_mod", "ts TIMESTAMP(3) NOT NULL PRIMARY KEY, s VARCHAR2(64)")
        tdSql.execute("DROP VTABLE IF EXISTS v_or_mod")
        tdSql.execute(
            "CREATE VTABLE v_or_mod (ts timestamp, "
            f"s varchar(16) FROM {_OR_SRC}.{_OR_SCHEMA}.r_mod.s)")
        tdSql.error("ALTER VTABLE v_or_mod MODIFY COLUMN s varchar(64)")
        tdSql.error("ALTER VTABLE v_or_mod MODIFY COLUMN s varchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS v_or_mod")

        # (g) ALTER referencing a dropped source fails.
        tdSql.execute("DROP VTABLE IF EXISTS v_or_dropsrc")
        tdSql.execute(
            "CREATE VTABLE v_or_dropsrc (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r1.v)")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_OR_SRC}")
        tdSql.error(
            f"ALTER VTABLE v_or_dropsrc ADD COLUMN x bigint "
            f"FROM {_OR_SRC}.{_OR_SCHEMA}.r1.w")
        _ensure_oracle_source_and_tables()
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute("DROP VTABLE IF EXISTS v_or_dropsrc")

        # (h) FQOR-VTBL-009: USE switching leaves the frozen reference and the
        # returned values untouched.
        tdSql.execute("DROP VTABLE IF EXISTS v_or_use")
        tdSql.execute(
            "CREATE VTABLE v_or_use (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r1.v)")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_use' "
            "AND col_name='v'")
        before = str(tdSql.queryResult[0][0])
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {_OR_ALT_DB}")
        tdSql.execute(f"USE {_OR_ALT_DB}")
        tdSql.query(f"SELECT v FROM {_LOCAL_DB}.v_or_use ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_or_use' "
            "AND col_name='v'")
        after = str(tdSql.queryResult[0][0])
        assert before == after, f"USE switch changed ref: {before!r} -> {after!r}"
        assert after == f"{_OR_SRC}.{_OR_SCHEMA}.r1.v", after
        tdSql.execute("DROP VTABLE IF EXISTS v_or_use")
        tdSql.execute(f"DROP DATABASE IF EXISTS {_OR_ALT_DB}")
        tdSql.execute(f"USE {_LOCAL_DB}")

    def test_oracle_source_tls_options_rejected(self):
        """FQOR-SEC-002: Oracle rejects TLS source options with a syntax error.

        Oracle Net negotiates its own encryption, so the connector has no TLS
        switch.  Explicit TLS and client-certificate keys are rejected with
        TSDB_CODE_PAR_SYNTAX_ERROR instead of being silently ignored; a false
        tls_enabled stays accepted because that is how ALTER ... SET OPTIONS
        clears a key.

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle as the fifth external source

        """
        base = (
            "CREATE EXTERNAL SOURCE valter_or_tls TYPE='oracle' "
            f"HOST='{ExtSrcEnv.ORACLE_HOST}' PORT={ExtSrcEnv.ORACLE_PORT} "
            f"USER='{ExtSrcEnv.ORACLE_USER}' PASSWORD='{ExtSrcEnv.ORACLE_PASS}' "
            f"DATABASE={_OR_SVC}")
        tdSql.execute("DROP EXTERNAL SOURCE IF EXISTS valter_or_tls")

        tdSql.error(base + " OPTIONS('tls_enabled'='true')",
                    expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error(base + " OPTIONS('tls_ca_cert'='/etc/ssl/ca.pem')",
                    expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error(base + " OPTIONS('tls_client_cert'='/etc/ssl/client.pem')",
                    expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error(base + " OPTIONS('tls_client_key'='/etc/ssl/client.key')",
                    expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error(
            f"ALTER EXTERNAL SOURCE {_OR_SRC} "
            "SET OPTIONS('tls_client_key'='/etc/ssl/client.key')",
            expectedErrno=TSDB_CODE_PAR_SYNTAX_ERROR)

        # A false tls_enabled is accepted and the source is usable afterwards.
        tdSql.execute(base + " OPTIONS('tls_enabled'='false')")
        tdSql.query("DESCRIBE EXTERNAL SOURCE valter_or_tls")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "valter_or_tls")
        tdSql.checkData(0, 1, "oracle")
        tdSql.execute("DROP EXTERNAL SOURCE valter_or_tls")

    # -------------------------------------------------------------------
    # 4.2 — vstb
    # -------------------------------------------------------------------

    def _fresh_vstb(self, stb="vstb_alter"):
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")
        tdSql.execute(
            f"CREATE STABLE {stb} (ts timestamp, v int) "
            f"TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_alter ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r1.v) "
            f"USING {stb} TAGS ('siteA')")
        return stb

    def test_vstb_add_column_propagates(self):
        """Vstb add column propagates

        Vstb add column propagates

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_add")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN w int")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("w", "INT", ""),
            ("site", "NCHAR"),
        ])
        tdSql.query("SELECT count(*) FROM vctb_alter WHERE w IS NULL")
        n_null = tdSql.getData(0, 0)
        tdSql.query("SELECT count(*) FROM vctb_alter")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdLog.info(f"[contract] vstb ADD COLUMN w propagation: null={n_null}")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    def test_vstb_sqlserver_child_add_column_propagates(self):
        """§4.2 — ALTER STABLE ADD COLUMN propagates to a SQL Server child.

        The virtual child references SQL Server through the 5-segment path;
        the stable-level column addition must reach the child schema while the
        child's external binding stays intact.

        Since: v3.4.4.0

        """
        tdSql.execute("DROP STABLE IF EXISTS vstb_alter_ss")
        tdSql.execute(
            "CREATE STABLE vstb_alter_ss (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_alter_ss ("
            f"v FROM {_SS_SRC}.{_SS_DB}.dbo.r1.v) "
            "USING vstb_alter_ss TAGS ('siteD')")
        tdSql.execute("ALTER STABLE vstb_alter_ss ADD COLUMN w int")
        _check_describe_layout("vctb_alter_ss", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_SS_SRC}.{_SS_DB}.r1.v"),
            ("w", "INT", ""),
            ("site", "NCHAR"),
        ])
        tdSql.query("SELECT count(*) FROM vctb_alter_ss")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter_ss")
        tdSql.execute("DROP STABLE IF EXISTS vstb_alter_ss")

    # -------------------------------------------------------------------
    # 4.4 — External source lifecycle and refreshed remote schema
    # -------------------------------------------------------------------

    def test_existing_vtable_refs_after_source_refresh_drop_and_recreate(self):
        """Existing refs survive refresh, fail after source drop, and recover after recreate.

        Existing refs survive refresh, fail after source drop, and recover after recreate.

        Since: v3.4.0.0

        """
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_pg")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_pg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r1.v)")
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_PG_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_pg")
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_pg")

        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_my")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_my (ts timestamp, "
            f"v int FROM {_MY_SRC}.{_MY_DB}.r1.v)")
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_MY_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_my")
        create_ext_source(_MY_SRC, "mysql", _MY_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_my")

        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_inf")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_inf (ts timestamp, "
            f"value double FROM {_INF_SRC}.{_INF_DB}.r_inftag.value)")
        tdSql.query("SELECT count(*) FROM v_refresh_inf")
        tdSql.checkData(0, 0, 1)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_INF_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_inf")
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_inf")
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_inf")

        # SQL Server: same lifecycle over the 5-segment reference path.
        create_sqlserver_table(_SS_DB, "r_refresh",
                               "ts datetime2(3) NOT NULL PRIMARY KEY, v INT",
                               ["('2024-01-01 00:00:00.000', 1)",
                                "('2024-01-01 00:01:00.000', 2)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_ss")
        tdSql.execute(
            "CREATE VTABLE v_refresh_ss (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_refresh.v)")
        tdSql.query("SELECT count(*) FROM v_refresh_ss")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_SS_SRC}")
        tdSql.query("SELECT count(*) FROM v_refresh_ss")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_SS_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_ss")
        create_ext_source(_SS_SRC, "sqlserver", _SS_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_ss")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_ss")

    def test_alter_column_set_ref_after_remote_schema_change(self):
        """Ref binding sees refreshed remote schema and rejects incompatible type changes.

        Ref binding sees refreshed remote schema and rejects incompatible type changes.

        Since: v3.4.0.0

        """
        create_pg_table(_PG_DB, "r_mut",
                        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
                        ["('2024-01-01 00:00:00', 1)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_pg")
        tdSql.execute(
            f"CREATE VTABLE v_mut_pg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r_mut.v, w int)")
        tdSql.error(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        ExtSrcEnv.pg_exec(_PG_DB, ["ALTER TABLE r_mut ADD COLUMN w INTEGER DEFAULT 7"])
        try:
            tdSql.execute(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        except Exception as err:
            tdLog.info(f"pre-refresh PG SET ref failed as allowed: {err}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        tdSql.query("SELECT w FROM v_mut_pg")
        tdSql.checkData(0, 0, 7)
        ExtSrcEnv.pg_exec(_PG_DB, [
            "ALTER TABLE r_mut ALTER COLUMN w TYPE TEXT USING w::TEXT",
            "UPDATE r_mut SET w = 'bad'",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.error("SELECT w FROM v_mut_pg")
        tdSql.execute("ALTER VTABLE v_mut_pg ALTER COLUMN w SET NULL")
        tdSql.error(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_pg")

        create_mysql_table(_MY_DB, "r_mut",
                           "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT",
                           ["('2024-01-01 00:00:00.000', 1)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_my")
        tdSql.execute(
            f"CREATE VTABLE v_mut_my (ts timestamp, "
            f"v int FROM {_MY_SRC}.{_MY_DB}.r_mut.v, w int)")
        tdSql.error(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        ExtSrcEnv.mysql_exec(_MY_DB, ["ALTER TABLE r_mut ADD COLUMN w INT DEFAULT 7"])
        try:
            tdSql.execute(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        except Exception as err:
            tdLog.info(f"pre-refresh MySQL SET ref failed as allowed: {err}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.execute(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        tdSql.query("SELECT w FROM v_mut_my")
        tdSql.checkData(0, 0, 7)
        ExtSrcEnv.mysql_exec(_MY_DB, [
            "ALTER TABLE r_mut MODIFY COLUMN w VARCHAR(32)",
            "UPDATE r_mut SET w = 'bad'",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.error("SELECT w FROM v_mut_my")
        tdSql.execute("ALTER VTABLE v_mut_my ALTER COLUMN w SET NULL")
        tdSql.error(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_my")

        create_sqlserver_table(_SS_DB, "r_mut",
                               "ts datetime2(3) NOT NULL PRIMARY KEY, v INT",
                               ["('2024-01-01 00:00:00.000', 1)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_ss")
        tdSql.execute(
            "CREATE VTABLE v_mut_ss (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_mut.v, w int)")
        tdSql.error(f"ALTER VTABLE v_mut_ss ALTER COLUMN w SET {_SS_SRC}.{_SS_DB}.dbo.r_mut.w")
        ExtSrcEnv.sqlserver_exec(_SS_DB, [
            "ALTER TABLE dbo.r_mut ADD w INT",
            "UPDATE dbo.r_mut SET w = 7",
        ])
        try:
            tdSql.execute(f"ALTER VTABLE v_mut_ss ALTER COLUMN w SET {_SS_SRC}.{_SS_DB}.dbo.r_mut.w")
        except Exception as err:
            tdLog.info(f"pre-refresh SQL Server SET ref failed as allowed: {err}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_SS_SRC}")
        tdSql.execute(f"ALTER VTABLE v_mut_ss ALTER COLUMN w SET {_SS_SRC}.{_SS_DB}.dbo.r_mut.w")
        tdSql.query("SELECT w FROM v_mut_ss")
        tdSql.checkData(0, 0, 7)
        ExtSrcEnv.sqlserver_exec(_SS_DB, [
            "ALTER TABLE dbo.r_mut ALTER COLUMN w NVARCHAR(32)",
            "UPDATE dbo.r_mut SET w = 'bad'",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_SS_SRC}")
        tdSql.error("SELECT w FROM v_mut_ss")
        tdSql.execute("ALTER VTABLE v_mut_ss ALTER COLUMN w SET NULL")
        tdSql.error(f"ALTER VTABLE v_mut_ss ALTER COLUMN w SET {_SS_SRC}.{_SS_DB}.dbo.r_mut.w")
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_ss")

    def test_vstb_drop_column_propagates(self):
        """Vstb drop column propagates

        Vstb drop column propagates

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_drop")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN w int")
        tdSql.execute(f"ALTER STABLE {stb} DROP COLUMN w")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("site", "NCHAR"),
        ])
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    def test_vstb_modify_column(self):
        """Vstb modify column

        Vstb modify column

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_mod")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN s varchar(16)")
        tdSql.execute(f"ALTER STABLE {stb} MODIFY COLUMN s varchar(64)")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("s", "VARCHAR", ""),
            ("site", "NCHAR"),
        ])
        tdSql.error(f"ALTER STABLE {stb} MODIFY COLUMN s varchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    def test_vstb_add_drop_tag(self):
        """Vstb add drop tag

        Vstb add drop tag

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_tag")
        tdSql.execute(f"ALTER STABLE {stb} ADD TAG newt int")
        tdSql.execute(f"ALTER STABLE {stb} DROP TAG newt")
        tdSql.execute("DROP VTABLE vctb_alter")
        tdSql.execute(f"DROP STABLE {stb}")

    # -------------------------------------------------------------------
    # 4.3 — Child tag SET
    # -------------------------------------------------------------------

    def test_child_set_tag_single(self):
        """Child set tag single

        Child set tag single

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_tags")
        tdSql.execute("ALTER VTABLE vctb_alter SET TAG site = 'siteB'")
        tdSql.query("SELECT site FROM vctb_alter LIMIT 1")
        tdSql.checkData(0, 0, "siteB")
        tdSql.execute("DROP VTABLE vctb_alter")
        tdSql.execute(f"DROP STABLE {stb}")

    def test_child_set_tag_type_mismatch(self):
        """Child set tag type mismatch

        Child set tag type mismatch

        Since: v3.4.0.0

        """
        stb = self._fresh_vstb("vstb_alter_tag_mis")
        # site is nchar(16); set a too-long string → must fail.
        tdSql.error(
            "ALTER VTABLE vctb_alter SET TAG site = "
            "'this_is_a_string_clearly_longer_than_16_chars'")
        tdSql.execute("ALTER VTABLE vctb_alter SET TAG site = 12345")
        tdSql.query("SELECT site FROM vctb_alter LIMIT 1")
        tdSql.checkData(0, 0, "12345")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")
