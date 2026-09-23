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
}


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
        mysql_ref = (
            f"{_EXTERNAL_SOURCES['mysql']}.{_EXTERNAL_DB}.remote_values.v"
        )
        postgres_ref = (
            f"{_EXTERNAL_SOURCES['postgres']}.{_EXTERNAL_DB}.remote_values.v"
        )
        influx_ref = (
            f"{_EXTERNAL_SOURCES['influx']}.{_EXTERNAL_DB}.remote_values.v"
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

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        for source in _EXTERNAL_SOURCES.values():
            tdSql.execute(f"drop external source if exists {source}")
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.influx_drop_db(_EXTERNAL_DB)

    @staticmethod
    def do_compare_pushdown_agg_plan():
        test_case = "test_vtable_plan_pushdown_agg_ext_source"
        input_file = os.path.join(
            os.path.dirname(__file__), "in", f"{test_case}.in"
        )
        answer_file = os.path.join(
            os.path.dirname(__file__), "ans", f"{test_case}.ans"
        )
        tdCom.compare_testcase_result(input_file, answer_file, test_case)
        print("federated pushdownAgg plans ................ [ passed ]")

    def test_pushdown_agg_ext_source(self):
        """Push virtual-table aggregates into local and external scans.

        1. Cover MySQL, PostgreSQL, and InfluxDB aggregate pushdown.
        2. Cover local-plus-external and mixed-external source plans.
        3. Verify merged aggregate results and output-column order.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-15 Codex Added federated pushdownAgg plan coverage

        """
        self.do_compare_pushdown_agg_plan()
