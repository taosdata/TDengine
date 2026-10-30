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
from new_test_framework.utils import tdCom, tdLog, tdSql
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vtable_util import ExtSrcEnv, VtableQueryUtil


_LOCAL_DB = "vt_pushdown_agg"
_EXTERNAL_DB = "vt_pushdown_agg_ext"
_EXTERNAL_SOURCES = {
    "mysql": "vt_pushdown_agg_mysql_src",
    "postgres": "vt_pushdown_agg_pg_src",
    "influx": "vt_pushdown_agg_influx_src",
    "sqlserver": "vt_pushdown_agg_ss_src",
    "oracle": "vt_pushdown_agg_oracle_src",
}
# Oracle has one namespace level - the login schema - and no per-source
# database, so its remote objects are qualified as source.schema.table.  The
# table name carries the file prefix because every Oracle fixture of this suite
# lives in the same shared schema.
_ORACLE_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_ORACLE_TABLE = "VT_PUSHDOWN_AGG_ORA_VALUES"


class TestVTablePlanPushdownAggExtSource:
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
                "(ts datetime(6) primary key, v int)",
                "insert into remote_values values "
                "('2024-01-01 00:00:00', 10), "
                "('2024-01-01 00:00:01', 20), "
                "('2024-01-01 00:00:02', 30)",
            ],
        )

        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_create_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_exec(
            _EXTERNAL_DB,
            [
                "create table remote_values "
                "(ts timestamp(6) primary key, v integer)",
                "insert into remote_values values "
                "('2024-01-01 00:00:00', 100), "
                "('2024-01-01 00:00:01', 200), "
                "('2024-01-01 00:00:02', 300)",
            ],
        )

        ExtSrcEnv.influx_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_create_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_write(
            _EXTERNAL_DB,
            [
                "remote_values v=1000i 1704067200000000000",
                "remote_values v=2000i 1704067201000000000",
                "remote_values v=3000i 1704067202000000000",
            ],
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            result = ExtSrcEnv.influx_query_sql(
                _EXTERNAL_DB, 'select count(*) from "remote_values"'
            )
            if result and int(result[0].get("count(*)", 0)) == 3:
                break
            time.sleep(1)
        else:
            raise AssertionError("InfluxDB remote_values is not visible")

        ExtSrcEnv.sqlserver_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.sqlserver_create_db(_EXTERNAL_DB)
        ExtSrcEnv.sqlserver_exec(
            _EXTERNAL_DB,
            [
                "create table dbo.remote_values "
                "(ts datetime2(6) not null primary key, v int)",
                "insert into dbo.remote_values values "
                "('2024-01-01 00:00:00', 10000), "
                "('2024-01-01 00:00:01', 20000), "
                "('2024-01-01 00:00:02', 30000)",
            ],
        )

        cfg = next(ExtSrcEnv.oracle_version_configs())
        ExtSrcEnv.oracle_drop_table_cfg(cfg, _ORACLE_TABLE)
        ExtSrcEnv._oracle_sql(
            cfg,
            [
                f"CREATE TABLE {_ORACLE_TABLE} "
                "(ts TIMESTAMP(3) NOT NULL PRIMARY KEY, v NUMBER(10,0))",
                # Oracle has no multi-row VALUES clause and a bare string date
                # would go through NLS_TIMESTAMP_FORMAT (ORA-01861).
                f"INSERT ALL "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:00.000', 100000) "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:01.000', 200000) "
                f"INTO {_ORACLE_TABLE} VALUES "
                "(TIMESTAMP '2024-01-01 00:00:02.000', 300000) "
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
        mysql_ref = (
            f"{_EXTERNAL_SOURCES['mysql']}.{_EXTERNAL_DB}.remote_values.v"
        )
        postgres_ref = (
            f"{_EXTERNAL_SOURCES['postgres']}.{_EXTERNAL_DB}.remote_values.v"
        )
        influx_ref = (
            f"{_EXTERNAL_SOURCES['influx']}.{_EXTERNAL_DB}.remote_values.v"
        )
        # SQL Server: the middle segment of a three-part path is the schema
        # (DS §5.3.4), so the database is qualified explicitly (five-part
        # column path source.database.schema.table.column).
        sqlserver_ref = (
            f"{_EXTERNAL_SOURCES['sqlserver']}.{_EXTERNAL_DB}.dbo.remote_values.v"
        )
        # Oracle: one namespace level only, so the middle segment is the
        # schema and the service name never appears in a path.
        oracle_ref = (
            f"{_EXTERNAL_SOURCES['oracle']}.{_ORACLE_SCHEMA}.{_ORACLE_TABLE}.v"
        )

        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        tdSql.execute(f"create database {_LOCAL_DB} precision 'ns'")
        tdSql.execute(f"use {_LOCAL_DB}")
        tdSql.execute("create table local_values (ts timestamp, v int)")
        tdSql.execute(
            "insert into local_values values "
            "('2024-01-01 00:00:00', 1), "
            "('2024-01-01 00:00:01', 2), "
            "('2024-01-01 00:00:02', 3)"
        )
        tdSql.execute(
            "create vtable vt_mysql (ts timestamp, "
            f"local_v int from local_values.v, remote_v int from {mysql_ref})"
        )
        tdSql.execute(
            "create vtable vt_postgres (ts timestamp, "
            f"local_v int from local_values.v, remote_v int from {postgres_ref})"
        )
        tdSql.execute(
            "create vtable vt_influx (ts timestamp, "
            f"local_v int from local_values.v, remote_v bigint from {influx_ref})"
        )
        tdSql.execute(
            "create vtable vt_sqlserver (ts timestamp, "
            f"local_v int from local_values.v, remote_v int from {sqlserver_ref})"
        )
        tdSql.execute(
            "create vtable vt_oracle (ts timestamp, "
            f"local_v int from local_values.v, remote_v bigint from {oracle_ref})"
        )
        tdSql.execute(
            "create vtable vt_external_mixed (ts timestamp, "
            f"mysql_v int from {mysql_ref}, "
            f"postgres_v int from {postgres_ref}, "
            f"influx_v bigint from {influx_ref})"
        )
        tdSql.execute(
            "create vtable vt_all_mixed (ts timestamp, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_ref}, "
            f"postgres_v int from {postgres_ref}, "
            f"influx_v bigint from {influx_ref})"
        )
        tdSql.execute(
            "create vtable vt_all_mixed_ss (ts timestamp, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_ref}, "
            f"postgres_v int from {postgres_ref}, "
            f"influx_v bigint from {influx_ref}, "
            f"sqlserver_v int from {sqlserver_ref})"
        )
        tdSql.execute(
            "create vtable vt_all_mixed_oracle (ts timestamp, "
            "local_v int from local_values.v, "
            f"mysql_v int from {mysql_ref}, "
            f"postgres_v int from {postgres_ref}, "
            f"influx_v bigint from {influx_ref}, "
            f"oracle_v bigint from {oracle_ref})"
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
    def check_row(sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(1)
        actual = tuple(tdSql.queryResult[0])
        assert actual == expected, f"{sql}: expected {expected}, got {actual}"

    @classmethod
    def do_oracle_aggregate(cls):
        """Aggregate the Oracle-backed and all-source virtual tables.

        Oracle is a step-1 (zero pushdown) source, so the remote SQL is a plain
        base scan and only the merged aggregate values are checked here.  The
        plan itself is not compared yet; see the TODO in the test docstring.
        """
        cls.check_row(
            "select sum(local_v), sum(remote_v) from vt_oracle", (6, 600000))
        cls.check_row(
            "select sum(local_v), sum(mysql_v), sum(postgres_v), "
            "sum(influx_v), sum(oracle_v) from vt_all_mixed_oracle",
            (6, 60, 600, 6000, 600000))

    @staticmethod
    def do_compare_pushdown_agg_plan():
        test_case = "test_vtable_plan_pushdown_agg_ext_source"
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
        print("federated pushdownAgg plans ................ [ passed ]")

    def test_pushdown_agg_ext_source(self):
        """Push virtual-table aggregates into local and external scans.

        1. Cover MySQL, PostgreSQL, InfluxDB and Oracle aggregate pushdown.
        2. Cover local-plus-external and mixed-external source plans,
           including the Oracle single-source and all-source virtual tables.
        3. Verify merged aggregate results and output-column order.
        4. Compare the EXPLAIN VERBOSE plans of every source, Oracle included.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-15 Codex Added federated pushdownAgg plan coverage
            - 2026-10-26 Add Oracle aggregate value coverage
            - 2026-10-27 Record the Oracle plan blocks in the shared golden

        """
        self.do_compare_pushdown_agg_plan()
        self.do_oracle_aggregate()
