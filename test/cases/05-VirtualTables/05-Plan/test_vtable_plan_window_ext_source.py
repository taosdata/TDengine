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
import os
import sys
import time

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vtable_util import ExtSrcEnv, VtableQueryUtil


_LOCAL_DB = "vt_window_plan"
_EXTERNAL_DB = "vt_window_plan_ext"
_EXTERNAL_SOURCES = {
    "mysql": "vt_window_plan_mysql_src",
    "postgres": "vt_window_plan_pg_src",
    "influx": "vt_window_plan_influx_src",
    "sqlserver": "vt_window_plan_ss_src",
    "oracle": "vt_window_plan_oracle_src",
}
# Oracle has one namespace level - the login schema - and no per-source
# database, so its remote objects are qualified as source.schema.table.  The
# table name carries the file prefix because every Oracle fixture of this suite
# lives in the same shared schema.
_ORACLE_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_ORACLE_TABLE = "VT_WINDOW_PLAN_ORA_VALUES"


class TestVTablePlanWindowExtSource:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_qnode()
        cls.adapters = {
            backend: VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]()
            for backend in _EXTERNAL_SOURCES
        }
        for adapter in cls.adapters.values():
            adapter.ensure_running()

        cls.prepare_external_data()
        cls.create_external_sources()
        cls.create_virtual_tables()

    @classmethod
    def prepare_external_data(cls):
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.mysql_create_db(_EXTERNAL_DB)
        ExtSrcEnv.mysql_exec(
            _EXTERNAL_DB,
            [
                "create table remote_values "
                "(ts datetime(6) primary key, state_col int, v int)",
                "insert into remote_values values "
                "('2024-01-01 00:00:00', 1, 10), "
                "('2024-01-01 00:00:01', 1, 20), "
                "('2024-01-01 00:00:02', 2, 30), "
                "('2024-01-01 00:00:03', 2, 40)",
            ],
        )

        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_create_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_exec(
            _EXTERNAL_DB,
            [
                "create table remote_values "
                "(ts timestamp(6) primary key, state_col integer, v integer)",
                "insert into remote_values values "
                "('2024-01-01 00:00:00', 1, 100), "
                "('2024-01-01 00:00:01', 1, 200), "
                "('2024-01-01 00:00:02', 2, 300), "
                "('2024-01-01 00:00:03', 2, 400)",
            ],
        )

        ExtSrcEnv.influx_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_create_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_write(
            _EXTERNAL_DB,
            [
                "remote_values state_col=1i,v=1000i 1704038400000000000",
                "remote_values state_col=1i,v=2000i 1704038401000000000",
                "remote_values state_col=2i,v=3000i 1704038402000000000",
                "remote_values state_col=2i,v=4000i 1704038403000000000",
            ],
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            result = ExtSrcEnv.influx_query_sql(
                _EXTERNAL_DB, 'select count(*) from "remote_values"'
            )
            if result and int(result[0].get("count(*)", 0)) == 4:
                break
            time.sleep(1)
        else:
            raise AssertionError("InfluxDB remote_values is not visible")

        ExtSrcEnv.sqlserver_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.sqlserver_create_db(_EXTERNAL_DB)
        # SQL Server values differ from the MySQL / PostgreSQL / InfluxDB rows
        # so the Remote SQL text and the merged window results stay
        # distinguishable between sources.
        ExtSrcEnv.sqlserver_exec(
            _EXTERNAL_DB,
            [
                "create table dbo.remote_values "
                "(ts datetime2(6) not null primary key, state_col int, v int)",
                "insert into dbo.remote_values values "
                "('2024-01-01 00:00:00', 1, 10000), "
                "('2024-01-01 00:00:01', 1, 20000), "
                "('2024-01-01 00:00:02', 2, 30000), "
                "('2024-01-01 00:00:03', 2, 40000)",
            ],
        )

        cfg = next(ExtSrcEnv.oracle_version_configs())
        ExtSrcEnv.oracle_drop_table_cfg(cfg, _ORACLE_TABLE)
        # Oracle values differ from the other sources' rows for the same reason
        # SQL Server's do: the Remote SQL text and the merged window results
        # stay distinguishable between sources.
        ExtSrcEnv._oracle_sql(
            cfg,
            [
                f"CREATE TABLE {_ORACLE_TABLE} "
                "(ts TIMESTAMP(3) NOT NULL PRIMARY KEY, "
                "state_col NUMBER(10,0), v NUMBER(10,0))",
                # Oracle has no multi-row VALUES clause and a bare string date
                # would go through NLS_TIMESTAMP_FORMAT (ORA-01861).
                f"INSERT ALL "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:00.000', 1, 100000) "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:01.000', 1, 200000) "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:02.000', 2, 300000) "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:03.000', 2, 400000) "
                "SELECT 1 FROM dual",
                "COMMIT",
            ],
        )

    @classmethod
    def create_external_sources(cls):
        for source in _EXTERNAL_SOURCES.values():
            tdSql.execute(f"drop external source if exists {source}")

        mysql = cls.adapters["mysql"]
        tdSql.execute(
            f"create external source {_EXTERNAL_SOURCES['mysql']} type='mysql' "
            f"host='{mysql.host}' port={mysql.port} user='{mysql.user}' "
            f"password='{mysql.password}' database={_EXTERNAL_DB}"
        )

        postgres = cls.adapters["postgres"]
        tdSql.execute(
            f"create external source {_EXTERNAL_SOURCES['postgres']} "
            f"type='postgresql' host='{postgres.host}' port={postgres.port} "
            f"user='{postgres.user}' password='{postgres.password}' "
            f"database={_EXTERNAL_DB} schema=public"
        )

        influx = cls.adapters["influx"]
        tdSql.execute(
            f"create external source {_EXTERNAL_SOURCES['influx']} "
            f"type='influxdb' host='{influx.host}' port={influx.port} "
            f"user='u' password='' database={_EXTERNAL_DB} "
            f"options('api_token'='{influx.token}','protocol'='flight_sql')"
        )

        sqlserver = cls.adapters["sqlserver"]
        tdSql.execute(
            f"create external source {_EXTERNAL_SOURCES['sqlserver']} "
            f"type='sqlserver' host='{sqlserver.host}' port={sqlserver.port} "
            f"user='{sqlserver.user}' password='{sqlserver.password}' "
            f"database={_EXTERNAL_DB} schema=dbo"
        )

        # Oracle carries the Easy Connect service name in DATABASE and the
        # schema (the only namespace level) in SCHEMA.
        oracle = cls.adapters["oracle"]
        tdSql.execute(
            f"create external source {_EXTERNAL_SOURCES['oracle']} "
            f"type='oracle' host='{oracle.host}' port={oracle.port} "
            f"user='{oracle.user}' password='{oracle.password}' "
            f"database={ExtSrcEnv.ORACLE_SERVICE} schema={_ORACLE_SCHEMA}"
        )

    @classmethod
    def create_virtual_tables(cls):
        mysql_prefix = (
            f"{_EXTERNAL_SOURCES['mysql']}."
            f"{_EXTERNAL_DB}.remote_values"
        )
        postgres_prefix = (
            f"{_EXTERNAL_SOURCES['postgres']}."
            f"{_EXTERNAL_DB}.remote_values"
        )
        influx_prefix = (
            f"{_EXTERNAL_SOURCES['influx']}."
            f"{_EXTERNAL_DB}.remote_values"
        )
        # SQL Server: the middle segment of a three-part path is the schema
        # (DS §5.3.4), so the database is qualified explicitly (five-part
        # column path source.database.schema.table.column).
        sqlserver_prefix = (
            f"{_EXTERNAL_SOURCES['sqlserver']}."
            f"{_EXTERNAL_DB}.dbo.remote_values"
        )
        # Oracle: one namespace level only, so the middle segment is the
        # schema and the service name never appears in a path.
        oracle_prefix = (
            f"{_EXTERNAL_SOURCES['oracle']}."
            f"{_ORACLE_SCHEMA}.{_ORACLE_TABLE}"
        )

        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        tdSql.execute(f"create database {_LOCAL_DB} precision 'ns'")
        tdSql.execute(f"use {_LOCAL_DB}")
        tdSql.execute(
            "create table local_values (ts timestamp, state_col int, v int)"
        )
        tdSql.execute(
            "insert into local_values values "
            "('2024-01-01 00:00:00', 1, 1), "
            "('2024-01-01 00:00:01', 1, 2), "
            "('2024-01-01 00:00:02', 2, 3), "
            "('2024-01-01 00:00:03', 2, 4)"
        )

        tdSql.execute(
            "create vtable vt_window_mysql (ts timestamp, "
            f"state_col int from {mysql_prefix}.state_col, "
            f"remote_v int from {mysql_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_postgres (ts timestamp, "
            f"state_col int from {postgres_prefix}.state_col, "
            f"remote_v int from {postgres_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_influx (ts timestamp, "
            f"state_col bigint from {influx_prefix}.state_col, "
            f"remote_v bigint from {influx_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_local_mysql (ts timestamp, "
            "state_col int from local_values.state_col, "
            f"remote_v int from {mysql_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_local_postgres (ts timestamp, "
            "state_col int from local_values.state_col, "
            f"remote_v int from {postgres_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_local_influx (ts timestamp, "
            "state_col int from local_values.state_col, "
            f"remote_v bigint from {influx_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_sqlserver (ts timestamp, "
            f"state_col int from {sqlserver_prefix}.state_col, "
            f"remote_v int from {sqlserver_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_local_sqlserver (ts timestamp, "
            "state_col int from local_values.state_col, "
            f"remote_v int from {sqlserver_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_oracle (ts timestamp, "
            f"state_col bigint from {oracle_prefix}.state_col, "
            f"remote_v bigint from {oracle_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_local_oracle (ts timestamp, "
            "state_col int from local_values.state_col, "
            f"remote_v bigint from {oracle_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_external_mixed (ts timestamp, "
            f"state_col int from {mysql_prefix}.state_col, "
            "local_v int from local_values.v, "
            f"postgres_v int from {postgres_prefix}.v, "
            f"influx_v bigint from {influx_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_all_mixed (ts timestamp, "
            "state_col int from local_values.state_col, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_prefix}.v, "
            f"postgres_v int from {postgres_prefix}.v, "
            f"influx_v bigint from {influx_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_cross_state_cond (ts timestamp, "
            "local_state int from local_values.state_col, "
            f"mysql_state int from {mysql_prefix}.state_col, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_all_mixed_ss (ts timestamp, "
            "state_col int from local_values.state_col, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_prefix}.v, "
            f"postgres_v int from {postgres_prefix}.v, "
            f"influx_v bigint from {influx_prefix}.v, "
            f"sqlserver_v int from {sqlserver_prefix}.v)"
        )
        tdSql.execute(
            "create vtable vt_window_all_mixed_oracle (ts timestamp, "
            "state_col int from local_values.state_col, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_prefix}.v, "
            f"postgres_v int from {postgres_prefix}.v, "
            f"influx_v bigint from {influx_prefix}.v, "
            f"oracle_v bigint from {oracle_prefix}.v)"
        )

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        for source in _EXTERNAL_SOURCES.values():
            tdSql.execute(f"drop external source if exists {source}")
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.sqlserver_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.oracle_drop_table_cfg(
            next(ExtSrcEnv.oracle_version_configs()), _ORACLE_TABLE)

    @staticmethod
    def do_oracle_window():
        """Run the state window over the Oracle-backed virtual tables.

        Oracle is a step-1 (zero pushdown) source, so the window is computed
        locally over a plain base scan.  The window boundaries are compared
        with the virtual table's own converted timestamp instead of a fixed
        epoch: an Oracle TIMESTAMP is tz-naive, so its epoch follows the
        client timezone (DS 5.3.7) and would otherwise pin the assertion to
        the timezone this test happens to run under.
        """
        for table in ("vt_window_oracle", "vt_window_local_oracle"):
            tdSql.query(
                f"select cast(ts as bigint) from {_LOCAL_DB}.{table} "
                "order by ts limit 1")
            tdSql.checkRows(1)
            first_ts = int(tdSql.queryResult[0][0])
            tdSql.query(
                "select cast(_wstart as bigint), state_col, "
                f"sum(remote_v), count(*) from {_LOCAL_DB}.{table} "
                "state_window(state_col, 1) order by _wstart"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, first_ts)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 300000)
            tdSql.checkData(0, 3, 2)
            # The second state starts two seconds after the first row.
            tdSql.checkData(1, 0, first_ts + 2_000_000_000)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(1, 2, 700000)
            tdSql.checkData(1, 3, 2)
        tdSql.query(
            "select sum(local_v), sum(mysql_v), sum(postgres_v), "
            f"sum(influx_v), sum(oracle_v) from {_LOCAL_DB}.vt_window_all_mixed_oracle")
        tdSql.checkRows(1)
        actual = tuple(tdSql.queryResult[0])
        assert actual == (10, 100, 1000, 10000, 1000000), actual

    @staticmethod
    def compare_plan_and_results():
        test_case = "test_vtable_plan_window_ext_source"
        input_file = os.path.join(
            os.path.dirname(__file__), "in", f"{test_case}.in"
        )
        answer_file = os.path.join(
            os.path.dirname(__file__), "ans", f"{test_case}.ans"
        )
        tdCom.compare_testcase_result(
            input_file, answer_file, test_case,
            result_transform=ExtSrcEnv.baseline_environment,
        )
        tdLog.info("federated VtableWindow plans ............... [ passed ]")

    def test_vtable_window_ext_source(self):
        """Optimize state windows over federated virtual-table sources.

        1. Cover pure MySQL, PostgreSQL, and InfluxDB state-window plans.
        2. Cover each external backend mixed with an internal table source.
        3. Cover internal and federated StateWindow origins with mixed sources.
        4. Verify explain plans and state-window query results with in/ans.
        5. Run the Oracle, local-plus-Oracle and all-source state windows,
           and compare their EXPLAIN VERBOSE plans from the shared golden.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-16 Codex Added federated VtableWindow plan coverage
            - 2026-07-31 Codex Added single-external-origin cost fallback coverage
            - 2026-10-26 Add Oracle state-window coverage
            - 2026-10-27 Record the Oracle plan blocks in the shared golden

        """
        self.compare_plan_and_results()
        self.do_oracle_window()
