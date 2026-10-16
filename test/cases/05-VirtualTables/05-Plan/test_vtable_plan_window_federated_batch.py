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

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vtable_util import ExtSrcEnv, VtableQueryUtil


_LOCAL_DB = "vt_window_federated_batch"
_EXTERNAL_DB = "vt_window_federated_batch_ext"
_MYSQL_SOURCE_A = "vt_window_batch_mysql_a"
_MYSQL_SOURCE_B = "vt_window_batch_mysql_b"
_POSTGRES_SOURCE = "vt_window_batch_pg"
_EXTERNAL_SOURCES = (
    _MYSQL_SOURCE_A,
    _MYSQL_SOURCE_B,
    _POSTGRES_SOURCE,
)


class TestVTablePlanWindowFederatedBatch:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_qnode()
        cls.adapters = {
            backend: VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]()
            for backend in ("mysql", "postgres")
        }
        for adapter in cls.adapters.values():
            adapter.ensure_running()

        cls.prepare_external_data()
        cls.create_external_sources()
        cls.create_virtual_stables()

    @classmethod
    def prepare_external_data(cls):
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.mysql_create_db(_EXTERNAL_DB)
        ExtSrcEnv.mysql_exec(
            _EXTERNAL_DB,
            [
                "create table remote_primary "
                "(ts datetime(6) primary key, state_col int, v int)",
                "insert into remote_primary values "
                "('2024-01-01 00:00:00', 1, 10), "
                "('2024-01-01 00:00:01', 1, 20), "
                "('2024-01-01 00:00:02', 2, 30), "
                "('2024-01-01 00:00:03', 2, 40)",
                "create table remote_aux "
                "(ts datetime(6) primary key, v int)",
                "insert into remote_aux values "
                "('2024-01-01 00:00:00', 100), "
                "('2024-01-01 00:00:01', 200), "
                "('2024-01-01 00:00:02', 300), "
                "('2024-01-01 00:00:03', 400)",
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
                "('2024-01-01 00:00:00', 1000), "
                "('2024-01-01 00:00:01', 2000), "
                "('2024-01-01 00:00:02', 3000), "
                "('2024-01-01 00:00:03', 4000)",
            ],
        )

    @classmethod
    def create_external_sources(cls):
        for source in _EXTERNAL_SOURCES:
            tdSql.execute(f"drop external source if exists {source}")

        mysql = cls.adapters["mysql"]
        for source in (_MYSQL_SOURCE_A, _MYSQL_SOURCE_B):
            tdSql.execute(
                f"create external source {source} type='mysql' "
                f"host='{mysql.host}' port={mysql.port} user='{mysql.user}' "
                f"password='{mysql.password}' database={_EXTERNAL_DB}"
            )

        postgres = cls.adapters["postgres"]
        tdSql.execute(
            f"create external source {_POSTGRES_SOURCE} type='postgresql' "
            f"host='{postgres.host}' port={postgres.port} "
            f"user='{postgres.user}' password='{postgres.password}' "
            f"database={_EXTERNAL_DB} schema=public"
        )

    @classmethod
    def create_virtual_stables(cls):
        mysql_primary_a = (
            f"{_MYSQL_SOURCE_A}.{_EXTERNAL_DB}.remote_primary"
        )
        mysql_primary_b = (
            f"{_MYSQL_SOURCE_B}.{_EXTERNAL_DB}.remote_primary"
        )
        mysql_aux_a = f"{_MYSQL_SOURCE_A}.{_EXTERNAL_DB}.remote_aux"
        mysql_aux_b = f"{_MYSQL_SOURCE_B}.{_EXTERNAL_DB}.remote_aux"
        postgres = f"{_POSTGRES_SOURCE}.{_EXTERNAL_DB}.remote_values"

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
            "create stable vst_single_source "
            "(ts timestamp, state_col int, source_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_single_source ("
            f"state_col from {mysql_primary_a}.state_col, "
            f"source_v from {mysql_primary_a}.v) "
            "using vst_single_source tags (1)"
        )

        tdSql.execute(
            "create stable vst_same_source "
            "(ts timestamp, state_col int, primary_v int, aux_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_same_source ("
            f"state_col from {mysql_primary_a}.state_col, "
            f"primary_v from {mysql_primary_a}.v, "
            f"aux_v from {mysql_aux_a}.v) "
            "using vst_same_source tags (2)"
        )

        tdSql.execute(
            "create stable vst_mixed_sources "
            "(ts timestamp, state_col int, mysql_a_v int, "
            "mysql_b_v int, postgres_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_mixed_sources ("
            f"state_col from {mysql_primary_a}.state_col, "
            f"mysql_a_v from {mysql_primary_a}.v, "
            f"mysql_b_v from {mysql_aux_b}.v, "
            f"postgres_v from {postgres}.v) "
            "using vst_mixed_sources tags (3)"
        )

        tdSql.execute(
            "create stable vst_local_external "
            "(ts timestamp, state_col int, local_v int, mysql_a_v int, "
            "mysql_b_v int, postgres_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_local_external ("
            "state_col from local_values.state_col, "
            "local_v from local_values.v, "
            f"mysql_a_v from {mysql_primary_b}.v, "
            f"mysql_b_v from {mysql_aux_b}.v, "
            f"postgres_v from {postgres}.v) "
            "using vst_local_external tags (4)"
        )

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        for source in _EXTERNAL_SOURCES:
            tdSql.execute(f"drop external source if exists {source}")
        ExtSrcEnv.mysql_drop_db(_EXTERNAL_DB)
        ExtSrcEnv.pg_drop_db(_EXTERNAL_DB)

    @staticmethod
    def compare_plan_and_results():
        for window_type in ("state", "session", "event", "interval"):
            test_case = (
                f"test_vstable_plan_test_window_federated_{window_type}"
            )
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
        tdLog.info("federated VStableWindow plans .............. [ passed ]")

    @staticmethod
    def check_window_results():
        state_cases = (
            (
                "select _wstart, _wend, state_col, sum(source_v) "
                f"from {_LOCAL_DB}.vst_single_source "
                "state_window(state_col, 0) order by _wstart",
                ((1, 30), (2, 70)),
            ),
            (
                "select _wstart, _wend, state_col, sum(primary_v), "
                f"sum(aux_v) from {_LOCAL_DB}.vst_same_source "
                "state_window(state_col, 0) order by _wstart",
                ((1, 30, 300), (2, 70, 700)),
            ),
            (
                "select _wstart, _wend, state_col, sum(mysql_a_v), "
                "sum(mysql_b_v), sum(postgres_v), avg(mysql_a_v), "
                f"avg(postgres_v) from {_LOCAL_DB}.vst_mixed_sources "
                "state_window(state_col, 0) order by _wstart",
                (
                    (1, 30, 300, 3000, 15.0, 1500.0),
                    (2, 70, 700, 7000, 35.0, 3500.0),
                ),
            ),
            (
                "select _wstart, _wend, state_col, sum(local_v), "
                "sum(mysql_a_v), sum(mysql_b_v), sum(postgres_v) "
                f"from {_LOCAL_DB}.vst_local_external "
                "state_window(state_col, 0) order by _wstart",
                ((1, 3, 30, 300, 3000), (2, 7, 70, 700, 7000)),
            ),
        )

        session_cases = (
            (
                "select _wstart, _wend, sum(primary_v), avg(aux_v) "
                f"from {_LOCAL_DB}.vst_same_source "
                "session(ts, 1500a) order by _wstart",
                ((100, 250.0),),
            ),
            (
                "select _wstart, _wend, sum(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_mixed_sources "
                "session(ts, 1500a) order by _wstart",
                ((100, 250.0, 2500.0),),
            ),
            (
                "select _wstart, _wend, avg(local_v), avg(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_local_external "
                "session(ts, 1500a) order by _wstart",
                ((2.5, 25.0, 250.0, 2500.0),),
            ),
        )

        event_cases = (
            (
                "select _wstart, _wend, sum(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_mixed_sources "
                "event_window start with state_col = 1 "
                "end with state_col = 2 order by _wstart",
                ((60, 200.0, 2000.0),),
            ),
            (
                "select _wstart, _wend, avg(local_v), avg(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_local_external "
                "event_window start with state_col = 1 "
                "end with state_col = 2 order by _wstart",
                ((2.0, 20.0, 200.0, 2000.0),),
            ),
        )

        interval_cases = (
            (
                "select _wstart, _wend, sum(primary_v), avg(aux_v), "
                "first(primary_v), last(aux_v), count(primary_v) "
                f"from {_LOCAL_DB}.vst_same_source "
                "interval(2s) order by _wstart",
                (
                    (30, 150.0, 10, 200, 2),
                    (70, 350.0, 30, 400, 2),
                ),
            ),
            (
                "select _wstart, _wend, sum(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_mixed_sources "
                "interval(2s) order by _wstart",
                (
                    (30, 150.0, 1500.0),
                    (70, 350.0, 3500.0),
                ),
            ),
            (
                "select _wstart, _wend, avg(local_v), avg(mysql_a_v), "
                "avg(mysql_b_v), avg(postgres_v) "
                f"from {_LOCAL_DB}.vst_local_external "
                "interval(2s) order by _wstart",
                (
                    (1.5, 15.0, 150.0, 1500.0),
                    (3.5, 35.0, 350.0, 3500.0),
                ),
            ),
        )

        window_cases = state_cases + session_cases + event_cases + interval_cases
        for sql, expected_rows in window_cases:
            tdSql.query(sql)
            tdSql.checkRows(len(expected_rows))
            for row_index, expected in enumerate(expected_rows):
                for value_index, value in enumerate(expected):
                    tdSql.checkData(row_index, value_index + 2, value)

        tdSql.query(
            "select _wstart, _wend, _wduration "
            f"from {_LOCAL_DB}.vst_mixed_sources "
            "state_window(state_col, 0) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2024-01-01 00:00:00.000000000")
        tdSql.checkData(0, 1, "2024-01-01 00:00:01.000000000")
        tdSql.checkData(0, 2, 1000000000)
        tdSql.checkData(1, 0, "2024-01-01 00:00:02.000000000")
        tdSql.checkData(1, 1, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(1, 2, 1000000000)

        tdLog.info("federated VStableWindow results ............ [ passed ]")

    def test_vstable_window_federated_batch(self):
        """Optimize virtual-stable windows for federated source batches.

        1. Cover one external table under one sourceName.
        2. Cover multiple external tables under the same sourceName.
        3. Cover references mixed across multiple external sourceNames.
        4. Cover references mixed across local and external sources.
        5. Verify state, session, event, and interval plans with separate in/ans.
        6. Verify plans that must not enter the VStableWindow optimization.
        7. Verify state, session, event, and interval results across sources.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-21 Codex Added federated VStableWindow batch coverage
            - 2026-07-22 Codex Added unified StateWindow result coverage
            - 2026-07-22 Codex Split window plan baselines and added negative coverage
            - 2026-07-31 Codex Added federated cost and batch-safety fallback coverage
            - 2026-08-04 Codex Added partial/merge AVG coverage for state, session, and event windows
            - 2026-08-25 Codex Added unordered federated interval batch coverage

        """
        self.check_window_results()
        self.compare_plan_and_results()
