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

"""DDL meta-info tests for vtables / vstbs referencing ext sources.

Section 6 of the DDL test plan: DESCRIBE, SHOW CREATE, INFORMATION_SCHEMA.
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
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_pg_table,
    create_influx_measurement, create_sqlserver_table, create_oracle_table,
    extract_show_create_sql as _extract_show_create_sql)


_LOCAL_DB = "vmeta_local"
_PG_DB    = "vmeta_pg"
_PG_SRC   = "vmeta_pg_src"
_INF_DB   = "vmeta_inf"
_INF_SRC  = "vmeta_inf_src"
_SS_DB    = "vmeta_ss"
_SS_SRC   = "vmeta_ss_src"
# Oracle has no per-source database: the source carries the Easy Connect
# service name and the schema (the upper-case login user, because unquoted
# Oracle identifiers fold to upper case).
_OR_SVC    = ExtSrcEnv.ORACLE_SERVICE
_OR_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_OR_SRC    = "vmeta_or_src"

# 2024-01-01 00:00:00 UTC in nanoseconds; every Oracle time-axis fixture is
# anchored here so the expected epoch values are checkable by hand.
_OR_BASE = 1704067200000000000


def _create_oracle_remote(table, ts_type, columns, rows=None):
    """Create an Oracle fixture whose first column is the primary-key axis."""
    create_oracle_table(
        table, f"ts {ts_type} NOT NULL PRIMARY KEY, {columns}", rows=rows)


def _ensure_oracle_env():
    """Provision the Oracle fixtures for the metadata and time-axis cases.

    `r` mirrors the SQL Server fixture (same column names, one row) so the
    metadata assertions line up; the remaining tables give each time-axis
    declaration its own object.  Oracle accepts only DATE and TIMESTAMP
    primary keys: "TIMESTAMP ... WITH TIME ZONE" cannot be one at all
    (ORA-02329), while "TIMESTAMP ... WITH LOCAL TIME ZONE" can.
    """
    _create_oracle_remote(
        "r", "TIMESTAMP(3)", "v NUMBER(10,0), w NUMBER(10,0)",
        ["(TIMESTAMP '2024-01-01 00:00:00.000', 11, 10)"])
    _create_oracle_remote(
        "r_date", "DATE", "v NUMBER(10,0)",
        ["(DATE '2024-01-01', 11)", "(DATE '2024-01-02', 12)"])
    _create_oracle_remote(
        "r_ts6", "TIMESTAMP(6)", "v NUMBER(10,0)",
        ["(TIMESTAMP '2024-01-01 00:00:00.123456', 11)",
         "(TIMESTAMP '2024-01-01 00:01:00.654321', 12)"])
    _create_oracle_remote(
        "r_ts9", "TIMESTAMP(9)", "v NUMBER(10,0)",
        ["(TIMESTAMP '2024-01-01 00:00:00.123456789', 11)",
         "(TIMESTAMP '2024-01-01 00:01:00.987654321', 12)"])
    # The literal carries the zone, so the stored instant does not depend on
    # the time zone of the session that inserts it.
    _create_oracle_remote(
        "r_tstzl", "TIMESTAMP(6) WITH LOCAL TIME ZONE", "v NUMBER(10,0)",
        ["(TIMESTAMP '2024-01-01 00:00:00.123456 +00:00', 11)",
         "(TIMESTAMP '2024-01-01 00:01:00.654321 +00:00', 12)"])
    # A declared filter on a CLOB can never be rendered remotely: Oracle has
    # no equality operator on LOBs, so the comparison stays a local filter.
    _create_oracle_remote(
        "r_filter", "TIMESTAMP(3)", "v NUMBER(10,0), note CLOB",
        ["(TIMESTAMP '2024-01-01 00:00:00.000', 11, 'alpha')",
         "(TIMESTAMP '2024-01-01 00:00:01.000', 12, 'beta')"])


def _ensure_env():
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(_PG_DB, "r",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, w INTEGER",
                    ["('2024-01-01 00:00:00', 1, 10)"])
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_remote_db("influxdb", _INF_DB)
    create_influx_measurement(_INF_DB, [
        "m_meta,device=d1 value=1.0 1700000000000000000",
        "m_meta,device=d1 value=2.0 1700000001000000000",
    ])
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_meta value=1i 1000000000000000000"])
    create_ext_source(_INF_SRC, "influxdb", _INF_DB)
    create_remote_db("sqlserver", _SS_DB)
    # The same table name under two schemas: metadata must keep the schema
    # segment of the persisted reference, and col_source must render it
    # (source.database.schema.table.column) so the display form stays
    # re-parseable.
    create_sqlserver_table(_SS_DB, "r",
                           "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, w INT",
                           ["('2024-01-01 00:00:00.000', 11, 10)"])
    create_sqlserver_table(_SS_DB, "r",
                           "ts datetime2(3) NOT NULL PRIMARY KEY, v INT, w INT",
                           ["('2024-01-01 00:00:00.000', 21, 20)"],
                           schema="other")
    create_ext_source(_SS_SRC, "sqlserver", _SS_DB)
    _ensure_oracle_env()
    create_ext_source(_OR_SRC, "oracle", _OR_SVC)


# ===========================================================================

class TestVtableMetaExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _ensure_env()
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ns'")
        tdSql.execute(f"USE {_LOCAL_DB}")
        # The connector sets the Oracle session time zone to the client's, so
        # pin it: the expected epoch values then hold for every time-axis
        # declaration, including TIMESTAMP WITH LOCAL TIME ZONE.
        tdSql.query("show local variables like 'timezone'")
        cls._original_timezone = str(tdSql.getData(0, 1)).split(" ")[0]
        tdSql.execute('alter local "timezone" "UTC"')

        # One normal vtable + one vstb with one child for the whole class.
        tdSql.execute(
            f"CREATE VTABLE v_meta (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v, "
            f"w int FROM {_PG_SRC}.{_PG_DB}.r.w, "
            f"local_only int)")
        tdSql.execute(
            "CREATE STABLE vstb_meta (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_meta ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) "
            f"USING vstb_meta TAGS ('siteA')")
        tdSql.execute(
            "CREATE VTABLE v_series_meta (ts timestamp, "
            "value double FROM s1.value) "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.m_meta (device='d1')")
        tdSql.execute(
            "CREATE STABLE vstb_series_meta (ts timestamp, value double) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_series_meta (value FROM s1.value) "
            "USING vstb_series_meta TAGS ('siteSeries') "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.m_meta (device='d1')")

        # SQL Server: same table/column names under two schemas, plus a
        # virtual child table. References use the full 5-segment path
        # source.database.schema.table.column.
        tdSql.execute(
            f"CREATE VTABLE v_meta_ss (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r.v, "
            f"w int FROM {_SS_SRC}.{_SS_DB}.dbo.r.w, "
            f"local_only int)")
        tdSql.execute(
            "CREATE VTABLE v_meta_ss_other (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.other.r.v)")
        tdSql.execute(
            "CREATE STABLE vstb_ss_meta (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_ss_meta ("
            f"v FROM {_SS_SRC}.{_SS_DB}.dbo.r.v) "
            f"USING vstb_ss_meta TAGS ('siteSS')")

        # Oracle: the persisted reference keeps the schema segment, and
        # NUMBER(10,0) maps to BIGINT, so the local columns are bigint.
        tdSql.execute(
            "CREATE VTABLE v_meta_or (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r.v, "
            f"w bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r.w, "
            "local_only int)")
        tdSql.execute(
            "CREATE STABLE vstb_or_meta (ts timestamp, v bigint) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_or_meta ("
            f"v FROM {_OR_SRC}.{_OR_SCHEMA}.r.v) "
            f"USING vstb_or_meta TAGS ('siteOR')")
        # One vtable per supported time-axis declaration (FQOR-VTBL-007).
        for vtb, table in (("v_meta_or_date", "r_date"),
                           ("v_meta_or_ts6", "r_ts6"),
                           ("v_meta_or_ts9", "r_ts9"),
                           ("v_meta_or_tstzl", "r_tstzl")):
            tdSql.execute(
                f"CREATE VTABLE {vtb} (ts timestamp, "
                f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.{table}.v)")
        # Declared filter on a LOB column: Oracle cannot compare it remotely,
        # so the FederatedScan operator has to evaluate it locally
        # (FQOR-VTBL-011).
        tdSql.execute(
            "CREATE VTABLE v_meta_or_filter (ts timestamp, "
            f"v bigint FROM {_OR_SRC}.{_OR_SCHEMA}.r_filter.v "
            "WHERE note = 'alpha')")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SS_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_OR_SRC}")
        # DROP EXTERNAL SOURCE only removes the local definition.  The
        # InfluxDB bucket outlives it and keeps occupying one of the five
        # database slots InfluxDB 3 Core allows per instance, so a run that
        # visits several ext-source suites eventually makes every later
        # influx_create_db() fail with HTTP 422.
        ExtSrcEnv.influx_drop_db(_INF_DB)
        original = getattr(cls, "_original_timezone", None)
        if original:
            tdSql.execute(f'alter local "timezone" "{original}"')

    # -------------------------------------------------------------------
    # 6.1 — Normal vtable meta
    # -------------------------------------------------------------------

    def test_describe_vtable_shows_columns(self):
        """Describe bound and unbound columns of a normal virtual table

        1. Describe the PostgreSQL-backed virtual table
        2. Verify the timestamp, two referenced columns, and unbound column appear in declaration order

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing DESCRIBE coverage

        """
        tdSql.query("DESCRIBE v_meta")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(3, 0, "local_only")

    def test_show_create_vtable_roundtrip(self):
        """Replay SHOW CREATE for an external-backed normal virtual table

        1. Verify SHOW CREATE includes the external source, database, and unbound column
        2. Recreate the table under a new name and verify its column order

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing SHOW CREATE replay coverage

        """
        tdSql.query("SELECT table_name FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta'")
        tdSql.checkRows(1)
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        if _PG_SRC not in sql or _PG_DB not in sql or "local_only" not in sql:
            tdLog.exit(f"expected {_PG_SRC}, {_PG_DB} and local_only in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_rt")
        tdSql.execute(sql.replace("v_meta", "v_meta_rt"))
        tdSql.query("DESCRIBE v_meta_rt")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(3, 0, "local_only")
        tdSql.execute("DROP VTABLE v_meta_rt")

    def test_information_schema_ins_tables(self):
        """Expose the normal virtual-table type in information_schema

        1. Look up the virtual table in ins_tables by database and table name
        2. Verify exactly one entry exists and its type identifies a virtual table

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing table-type metadata coverage

        """
        tdSql.query("SELECT table_name, `type` FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta'")
        tdSql.checkRows(1)
        table_type = str(tdSql.getData(0, 1)).upper()
        if "VIRTUAL" not in table_type:
            tdLog.exit(f"expected VIRTUAL in {table_type}")

    def test_information_schema_ins_columns_from_ref(self):
        """Expose external column references in information_schema

        1. Read all four normal virtual-table columns from ins_columns
        2. Verify referenced columns have full source paths and timestamp and unbound columns have NULL sources

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing column-reference metadata coverage

        """
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta' "
            "AND col_name IN ('local_only', 'ts', 'v', 'w') "
            "ORDER BY col_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "local_only")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "ts")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "v")
        tdSql.checkData(2, 1, f"{_PG_SRC}.{_PG_DB}.public.r.v")
        tdSql.checkData(3, 0, "w")
        tdSql.checkData(3, 1, f"{_PG_SRC}.{_PG_DB}.public.r.w")

    def test_refresh_external_source_preserves_existing_metadata(self):
        """Preserve virtual-table metadata when refreshing an unchanged source

        1. Compare SHOW CREATE output before and after refreshing the PostgreSQL source
        2. Verify both external column references remain unchanged in ins_columns

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing REFRESH metadata coverage

        """
        sql_before = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        sql_after = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        if sql_before != sql_after:
            tdLog.exit(f"SHOW CREATE changed after REFRESH: before={sql_before}, after={sql_after}")

        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta' "
            "AND col_name IN ('v', 'w') ORDER BY col_name")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.public.r.v")
        tdSql.checkData(1, 0, "w")
        tdSql.checkData(1, 1, f"{_PG_SRC}.{_PG_DB}.public.r.w")

    def test_show_create_quoted_identifier_vtable_roundtrip(self):
        """Replay external references containing quoted mixed-case identifiers

        1. Create a PostgreSQL table with mixed-case identifiers and query it through a virtual table
        2. Replay SHOW CREATE under a new virtual-table name and verify the value remains 33

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing quoted-identifier replay coverage

        """
        create_pg_table(_PG_DB, '"MetaCaseTable"',
                        'ts TIMESTAMP PRIMARY KEY, "MixedValue" INTEGER',
                        ["('2024-01-01 00:00:00', 33)"])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case")
        tdSql.execute(
            f"CREATE VTABLE v_meta_case (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.`MetaCaseTable`.`MixedValue`)")
        tdSql.query("SELECT v FROM v_meta_case")
        tdSql.checkData(0, 0, 33)

        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta_case")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case_rt")
        tdSql.execute(sql.replace("v_meta_case", "v_meta_case_rt"))
        tdSql.query("SELECT v FROM v_meta_case_rt")
        tdSql.checkData(0, 0, 33)
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case_rt")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case")

    # -------------------------------------------------------------------
    # 6.2 — vstb meta
    # -------------------------------------------------------------------

    def test_describe_vstable(self):
        """Describe the data columns and tag of a virtual super table

        1. Describe the virtual super table
        2. Verify the timestamp, value column, and site tag appear in declaration order

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing super-table DESCRIBE coverage

        """
        tdSql.query("DESCRIBE vstb_meta")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "site")

    def test_show_create_stable_includes_virtual(self):
        """Preserve the virtual flag when replaying super-table SHOW CREATE

        1. Verify SHOW CREATE contains the VIRTUAL clause
        2. Replay the statement under a new name and verify the super table is registered

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing virtual-super-table replay coverage

        """
        sql = _extract_show_create_sql("SHOW CREATE STABLE vstb_meta")
        if "VIRTUAL" not in sql.upper():
            tdLog.exit(f"expected VIRTUAL in {sql}")
        # round-trip
        tdSql.execute("DROP STABLE IF EXISTS vstb_meta_rt")
        tdSql.execute(sql.replace("vstb_meta", "vstb_meta_rt"))
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_stables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_meta_rt'")
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP STABLE vstb_meta_rt")

    def test_ins_stables_virtual_flag(self):
        """Expose the virtual-super-table flag in information_schema

        1. Look up the super table in ins_stables by database and table name
        2. Verify exactly one entry exists with isvirtual set to true

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing virtual-flag metadata coverage

        """
        tdSql.query("SELECT isvirtual FROM information_schema.ins_stables "
                    f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_meta'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)

    def test_child_vtable_ins_columns_shows_from(self):
        """Expose a virtual child table's external column reference

        1. Read the child table's value column from ins_columns
        2. Verify its source includes the PostgreSQL source, database, table, and column

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing child column-reference coverage

        """
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_meta' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.public.r.v")

    def test_show_create_series_vtable_roundtrip(self):
        """Replay SHOW CREATE for a normal virtual table with an InfluxDB SERIES

        1. Verify SHOW CREATE retains the SERIES source and device tag condition
        2. Recreate the table and verify its resolved column source and two-row query result

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing normal SERIES replay coverage

        """
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_series_meta")
        if "SERIES" not in sql.upper() or _INF_SRC not in sql or "device='d1'" not in sql:
            tdLog.exit(f"expected SERIES clause in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS v_series_meta_rt")
        tdSql.execute(sql.replace("v_series_meta", "v_series_meta_rt"))
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_series_meta_rt' "
            "AND col_name='value'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_INF_SRC}.{_INF_DB}.m_meta.value")
        tdSql.query("SELECT count(*) FROM v_series_meta_rt")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE v_series_meta_rt")

    def test_show_create_series_child_vtable_roundtrip(self):
        """Replay SHOW CREATE for a virtual child table with an InfluxDB SERIES

        1. Verify SHOW CREATE retains both the parent USING clause and the SERIES clause
        2. Recreate the child and verify its resolved column source and two-row query result

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing child SERIES replay coverage

        """
        sql = _extract_show_create_sql("SHOW CREATE VTABLE vctb_series_meta")
        if "SERIES" not in sql.upper() or "USING `vstb_series_meta`" not in sql:
            tdLog.exit(f"expected USING + SERIES clause in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_series_meta_rt")
        tdSql.execute(sql.replace("vctb_series_meta", "vctb_series_meta_rt"))
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_series_meta_rt' "
            "AND col_name='value'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_INF_SRC}.{_INF_DB}.m_meta.value")
        tdSql.query("SELECT count(*) FROM vctb_series_meta_rt")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE vctb_series_meta_rt")

    # -------------------------------------------------------------------
    # 6.3 — SQL Server meta
    # -------------------------------------------------------------------

    def test_describe_sqlserver_vtable(self):
        """Describe a SQL Server-backed normal virtual table

        1. Describe the virtual table whose columns reference SQL Server
        2. Verify the column order and that the 5-segment reference resolves data

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 Add SQL Server as the fourth external source

        """
        tdSql.query("DESCRIBE v_meta_ss")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(3, 0, "local_only")
        tdSql.query("SELECT v, w FROM v_meta_ss")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 10)

    def test_sqlserver_ins_columns_keeps_schema_binding(self):
        """Expose SQL Server column references and keep the schema binding

        1. Read the referenced columns of the dbo- and other-schema vtables from ins_columns
        2. Verify the col_source display form (full reference incl. schema) and
           that both bindings resolve their own schema

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 Add SQL Server as the fourth external source

        """
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta_ss' "
            "AND col_name IN ('local_only', 'ts', 'v', 'w') "
            "ORDER BY col_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "local_only")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "ts")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "v")
        tdSql.checkData(2, 1, f"{_SS_SRC}.{_SS_DB}.dbo.r.v")
        tdSql.checkData(3, 0, "w")
        tdSql.checkData(3, 1, f"{_SS_SRC}.{_SS_DB}.dbo.r.w")

        # col_source renders the full external reference, including the
        # schema segment that the create path named explicitly, so the
        # rendered statement stays re-parseable for SQL Server.
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta_ss_other' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_SS_SRC}.{_SS_DB}.other.r.v")
        tdSql.query("SELECT v FROM v_meta_ss")
        tdSql.checkData(0, 0, 11)
        tdSql.query("SELECT v FROM v_meta_ss_other")
        tdSql.checkData(0, 0, 21)

    def test_sqlserver_child_vtable_ins_columns_shows_from(self):
        """Expose a SQL Server-backed virtual child table's column reference

        1. Read the child table's value column from ins_columns
        2. Verify its source includes the SQL Server source, database, table, and column

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 Add SQL Server as the fourth external source

        """
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_ss_meta' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_SS_SRC}.{_SS_DB}.dbo.r.v")

    def test_show_create_sqlserver_vtable_contains_source(self):
        """Render a SQL Server reference in SHOW CREATE

        1. Verify SHOW CREATE includes the SQL Server source, database, table, column, and unbound column
        2. Confirm the statement is not replayed: the rendered 4-segment path drops the schema

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 Add SQL Server as the fourth external source

        """
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta_ss")
        for expected in (_SS_SRC, _SS_DB, "r", "v", "local_only"):
            if expected not in sql:
                tdLog.exit(f"expected {expected} in {sql}")
        # SHOW CREATE renders the full reference — source.database.schema.table
        # .column — for SQL Server, so the rendered statement can be replayed
        # as-is: the resolved schema binding survives the round trip.
        if "dbo" not in sql:
            tdLog.exit(f"expected the schema segment in {sql}")
        tdSql.query("SELECT v FROM v_meta_ss")
        tdSql.checkData(0, 0, 11)

        tdSql.execute(f"drop vtable if exists {_LOCAL_DB}.v_meta_ss_copy")
        tdSql.execute(sql.replace("v_meta_ss", "v_meta_ss_copy", 1))
        tdSql.query(f"SELECT v FROM {_LOCAL_DB}.v_meta_ss_copy")
        tdSql.checkData(0, 0, 11)
        tdSql.execute(f"drop vtable if exists {_LOCAL_DB}.v_meta_ss_copy")

    # -------------------------------------------------------------------
    # 6.4 — Oracle meta
    #
    # Oracle has a single namespace level (the schema), so a column
    # reference is either source.table.column (the source's schema) or
    # source.schema.table.column; the persisted form always carries the
    # schema, spelled the way the create path named it.
    # -------------------------------------------------------------------

    def test_describe_oracle_vtable(self):
        """Describe an Oracle-backed virtual table and its child

        1. Describe the normal Oracle vtable and check the mapped column types
           (NUMBER(10,0) is BIGINT, there is no Oracle integer of INT width)
        2. Verify the referenced values and the child table's DESCRIBE

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle to the backend matrix

        """
        tdSql.query("DESCRIBE v_meta_or")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 0, "local_only")
        tdSql.checkData(3, 1, "INT")
        tdSql.query("SELECT v, w, local_only FROM v_meta_or")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, None)

        tdSql.query("DESCRIBE vstb_or_meta")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 0, "site")
        tdSql.query("DESCRIBE vctb_or_meta")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(1, 1, "BIGINT")

    def test_oracle_ins_columns_keeps_schema_binding(self):
        """Expose Oracle column references with the schema segment in place

        1. Read the normal vtable's referenced and unbound columns from ins_columns
        2. Verify col_source is source.schema.table.column and that a child
           vtable carries the same binding

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle to the backend matrix

        """
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta_or' "
            "AND col_name IN ('local_only', 'ts', 'v', 'w') "
            "ORDER BY col_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "local_only")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "ts")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "v")
        tdSql.checkData(2, 1, f"{_OR_SRC}.{_OR_SCHEMA}.r.v")
        tdSql.checkData(3, 0, "w")
        tdSql.checkData(3, 1, f"{_OR_SRC}.{_OR_SCHEMA}.r.w")

        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_or_meta' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_OR_SRC}.{_OR_SCHEMA}.r.v")
        tdSql.query("SELECT v FROM vctb_or_meta")
        tdSql.checkData(0, 0, 11)

    def test_show_create_oracle_vtable_roundtrip(self):
        """Replay SHOW CREATE of an Oracle-backed virtual table

        1. Verify SHOW CREATE names the source, the schema, the table, the
           column and the unbound column
        2. Recreate the table under a new name and verify the value and the
           persisted reference survive the replay

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle to the backend matrix

        """
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta_or")
        for expected in (_OR_SRC, _OR_SCHEMA, "r", "v", "local_only"):
            if expected not in sql:
                tdLog.exit(f"expected {expected} in {sql}")
        # The rendered reference keeps the schema segment, so the statement is
        # self-contained and can be replayed as-is.
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_or_copy")
        tdSql.execute(sql.replace("v_meta_or", "v_meta_or_copy", 1))
        tdSql.query("SELECT v, w FROM v_meta_or_copy")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 10)
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta_or_copy' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_OR_SRC}.{_OR_SCHEMA}.r.v")
        tdSql.execute("DROP VTABLE v_meta_or_copy")

    # -------------------------------------------------------------------
    # 6.5 — Oracle time axes (FQOR-VTBL-007)
    # -------------------------------------------------------------------

    def test_oracle_time_axes(self):
        """FQOR-VTBL-007: every Oracle declaration that can be a time axis

        A remote table backs a virtual table only when it has a DATE or
        TIMESTAMP primary key.  DATE, TIMESTAMP(6) and TIMESTAMP(9) are
        covered directly, and TIMESTAMP(6) WITH LOCAL TIME ZONE is covered as
        the aware form that can still be a key: Oracle rejects "TIMESTAMP
        WITH TIME ZONE" as a primary key (ORA-02329), so that declaration can
        never be a time axis.  A range query must return exactly the rows of a
        direct read, with the sub-microsecond tail of TIMESTAMP(9) intact.

        1. Read each axis with cast(ts as bigint) and compare the expected
           nanosecond epochs
        2. Run a closed range query over the TIMESTAMP(6)/TIMESTAMP(9) axes
           and compare it with the direct read

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle to the backend matrix

        """
        cases = (
            # DATE stores whole seconds, so the epoch has no fraction.
            ("v_meta_or_date", [(_OR_BASE, 11), (_OR_BASE + 86400 * 10 ** 9, 12)]),
            ("v_meta_or_ts6", [(_OR_BASE + 123456000, 11),
                               (_OR_BASE + 60654321000, 12)]),
            ("v_meta_or_ts9", [(_OR_BASE + 123456789, 11),
                               (_OR_BASE + 60987654321, 12)]),
            ("v_meta_or_tstzl", [(_OR_BASE + 123456000, 11),
                                 (_OR_BASE + 60654321000, 12)]),
        )
        for vtb, expected in cases:
            tdSql.query(f"SELECT cast(ts as bigint), v FROM {vtb} ORDER BY ts")
            assert list(tdSql.queryResult) == expected, (vtb, tdSql.queryResult)

        # A range bounded by the last expected row must return both rows of
        # the direct read; a coarser conversion would drop the last digits and
        # move the upper bound past a TIMESTAMP(9)/TIMESTAMP(6) value.
        for vtb, expected in (cases[1], cases[2], cases[3]):
            last = expected[-1][0]
            tdSql.query(
                f"SELECT cast(ts as bigint), v FROM {vtb} "
                f"WHERE ts >= {_OR_BASE} AND ts <= {last} ORDER BY ts")
            assert list(tdSql.queryResult) == expected, (vtb, tdSql.queryResult)

    # -------------------------------------------------------------------
    # 6.6 — Oracle declared filter (FQOR-VTBL-011)
    # -------------------------------------------------------------------

    def test_oracle_declared_filter_runs_locally(self):
        """FQOR-VTBL-011: an unrenderable Oracle declared filter stays local

        `note` is a CLOB and Oracle has no equality operator on LOBs, so the
        declared filter can never be part of the remote WHERE clause.  The
        planner must split it out of the stored filter and leave it as a
        FederatedScan filter, which also has to fetch the column for the local
        evaluation.

        1. Verify EXPLAIN shows a remote SQL without the literal and a local
           Filter carrying the condition
        2. Verify the rows equal the same predicate evaluated over a local
           mirror table

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Add Oracle to the backend matrix

        """
        tdSql.query("EXPLAIN VERBOSE TRUE SELECT v FROM v_meta_or_filter")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        remote = [ln for ln in plan.splitlines() if "Remote SQL:" in ln]
        assert remote, plan
        assert "alpha" not in remote[0].lower(), plan
        local = [ln for ln in plan.splitlines()
                 if "Filter:" in ln and "conditions=" in ln]
        assert local, plan
        assert any("note" in ln and "alpha" in ln for ln in local), plan

        # Local mirror: the same two rows as TDengine rows, filtered by the
        # same predicate.  The vtable must agree with it exactly.
        tdSql.execute("DROP TABLE IF EXISTS meta_filter_mirror")
        tdSql.execute(
            "CREATE TABLE meta_filter_mirror (ts timestamp, v bigint, "
            "note varchar(8))")
        tdSql.execute(
            "INSERT INTO meta_filter_mirror VALUES "
            f"({_OR_BASE}, 11, 'alpha') ({_OR_BASE + 10 ** 9}, 12, 'beta')")
        tdSql.query(
            "SELECT cast(ts as bigint), v FROM v_meta_or_filter ORDER BY ts")
        remote_rows = list(tdSql.queryResult)
        tdSql.query(
            "SELECT cast(ts as bigint), v FROM meta_filter_mirror "
            "WHERE note = 'alpha' ORDER BY ts")
        mirror_rows = list(tdSql.queryResult)
        assert remote_rows == mirror_rows == [(_OR_BASE, 11)], \
            (remote_rows, mirror_rows)
        tdSql.execute("DROP TABLE meta_filter_mirror")
