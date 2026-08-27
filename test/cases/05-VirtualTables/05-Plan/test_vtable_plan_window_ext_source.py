###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

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
}


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

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        for source in _EXTERNAL_SOURCES.values():
            tdSql.execute(f"drop external source if exists {source}")
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_drop_db(_EXTERNAL_DB)

    @staticmethod
    def compare_plan_and_results():
        test_case = "test_vtable_plan_window_ext_source"
        input_file = os.path.join(
            os.path.dirname(__file__), "in", f"{test_case}.in"
        )
        answer_file = os.path.join(
            os.path.dirname(__file__), "ans", f"{test_case}.ans"
        )
        tdCom.compare_testcase_result(input_file, answer_file, test_case)
        tdLog.info("federated VtableWindow plans ............... [ passed ]")

    def test_vtable_window_ext_source(self):
        """Optimize state windows over federated virtual-table sources.

        1. Cover pure MySQL, PostgreSQL, and InfluxDB state-window plans.
        2. Cover each external backend mixed with an internal table source.
        3. Cover internal and federated StateWindow origins with mixed sources.
        4. Verify explain plans and state-window query results with in/ans.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-16 Codex Added federated VtableWindow plan coverage
            - 2026-07-31 Codex Added single-external-origin cost fallback coverage

        """
        self.compare_plan_and_results()
