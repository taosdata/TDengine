###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_vtable_plan_window_federated_batch import (
    TestVTablePlanWindowFederatedBatch as _FederatedEnv,
)
from vtable_util import ExtSrcEnv


class TestVTablePlanAggFederated:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        _FederatedEnv.setup_class()
        ExtSrcEnv.mysql_exec(
            "vt_window_federated_batch_ext",
            [
                "create table remote_empty "
                "(ts datetime(6) primary key, v int)",
                "create table remote_last_early "
                "(ts datetime(6) primary key, v int)",
                "insert into remote_last_early values "
                "('2024-01-01 00:00:00', 111), "
                "('2024-01-01 00:00:02', 333)",
                "create table remote_last_tie_a "
                "(ts datetime(6) primary key, v int)",
                "insert into remote_last_tie_a values "
                "('2024-01-01 00:00:04', 444)",
                "create table remote_last_tie_b "
                "(ts datetime(6) primary key, v int)",
                "insert into remote_last_tie_b values "
                "('2024-01-01 00:00:04', 555)",
                "create table remote_text "
                "(ts datetime(6) primary key, v varchar(16))",
                "insert into remote_text values "
                "('2024-01-01 00:00:00', 'a'), "
                "('2024-01-01 00:00:01', 'b'), "
                "('2024-01-01 00:00:02', 'c'), "
                "('2024-01-01 00:00:03', 'd')",
            ],
        )
        ExtSrcEnv.pg_exec(
            "vt_window_federated_batch_ext",
            [
                "create table remote_last_early "
                "(ts timestamp(6) primary key, v integer)",
                "insert into remote_last_early values "
                "('2024-01-01 00:00:00', 11111), "
                "('2024-01-01 00:00:01', 22222)",
                "create table remote_last_tie_a "
                "(ts timestamp(6) primary key, v integer)",
                "insert into remote_last_tie_a values "
                "('2024-01-01 00:00:04', 44444)",
                "create table remote_last_tie_b "
                "(ts timestamp(6) primary key, v integer)",
                "insert into remote_last_tie_b values "
                "('2024-01-01 00:00:04', 55555)",
                "create table remote_text "
                "(ts timestamp(6) primary key, v varchar(16))",
                "insert into remote_text values "
                "('2024-01-01 00:00:00', 'w'), "
                "('2024-01-01 00:00:01', 'x'), "
                "('2024-01-01 00:00:02', 'y'), "
                "('2024-01-01 00:00:03', 'z')",
            ],
        )
        tdSql.execute("use vt_window_federated_batch")
        tdSql.execute(
            "create vtable vct_local_external_2 ("
            "state_col from local_values.state_col, "
            "local_v from local_values.v, "
            "mysql_a_v from "
            "vt_window_batch_mysql_b.vt_window_federated_batch_ext.remote_primary.v, "
            "mysql_b_v from "
            "vt_window_batch_mysql_b.vt_window_federated_batch_ext.remote_aux.v, "
            "postgres_v from "
            "vt_window_batch_pg.vt_window_federated_batch_ext.remote_values.v) "
            "using vst_local_external tags (5)"
        )
        tdSql.execute(
            "create stable vst_tag_federated "
            "(ts timestamp, local_v int, mysql_v int) "
            "tags (scenario int) virtual 1"
        )
        for suffix, tag in (("a", 4), ("b", 5)):
            tdSql.execute(
                f"create vtable vct_tag_federated_{suffix} ("
                "local_v from local_values.v, "
                "mysql_v from "
                "vt_window_batch_mysql_b.vt_window_federated_batch_ext.remote_primary.v) "
                f"using vst_tag_federated tags ({tag})"
            )
        tdSql.execute(
            "create stable vst_tag_types "
            "(ts timestamp, local_v int, mysql_v int) "
            "tags (scenario int, label varchar(16)) virtual 1"
        )
        for suffix, scenario, label, remote_table in (
            ("a", 7, "'alpha'", "remote_primary"),
            ("b", 8, "''", "remote_aux"),
            ("c", 9, "null", "remote_primary"),
        ):
            tdSql.execute(
                f"create vtable vct_tag_types_{suffix} ("
                "local_v from local_values.v, "
                "mysql_v from "
                f"vt_window_batch_mysql_b.vt_window_federated_batch_ext.{remote_table}.v) "
                f"using vst_tag_types tags ({scenario}, {label})"
            )
        tdSql.execute(
            "create stable vst_empty_branch "
            "(ts timestamp, local_v int, empty_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_empty_branch ("
            "local_v from local_values.v, "
            "empty_v from "
            "vt_window_batch_mysql_a.vt_window_federated_batch_ext.remote_empty.v) "
            "using vst_empty_branch tags (6)"
        )
        tdSql.execute(
            "create stable vst_null_tag "
            "(ts timestamp, source_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_null_tag ("
            "source_v from "
            "vt_window_batch_mysql_a.vt_window_federated_batch_ext.remote_primary.v) "
            "using vst_null_tag tags (null)"
        )
        tdSql.execute(
            "create stable vst_last_federated "
            "(ts timestamp, mysql_v int, postgres_v int) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_last_federated ("
            "mysql_v from "
            "vt_window_batch_mysql_a.vt_window_federated_batch_ext.remote_last_early.v, "
            "postgres_v from "
            "vt_window_batch_pg.vt_window_federated_batch_ext.remote_values.v) "
            "using vst_last_federated tags (10)"
        )
        tdSql.execute(
            "create vtable vct_last_federated_early ("
            "mysql_v from "
            "vt_window_batch_mysql_a.vt_window_federated_batch_ext.remote_last_early.v, "
            "postgres_v from "
            "vt_window_batch_pg.vt_window_federated_batch_ext.remote_last_early.v) "
            "using vst_last_federated tags (11)"
        )
        tdSql.execute(
            "create stable vst_last_tie "
            "(ts timestamp, mysql_v int, postgres_v int) "
            "tags (scenario int) virtual 1"
        )
        for suffix, scenario in (("a", 20), ("b", 21)):
            tdSql.execute(
                f"create vtable vct_last_tie_{suffix} ("
                "mysql_v from "
                "vt_window_batch_mysql_a.vt_window_federated_batch_ext."
                f"remote_last_tie_{suffix}.v, "
                "postgres_v from "
                "vt_window_batch_pg.vt_window_federated_batch_ext."
                f"remote_last_tie_{suffix}.v) "
                f"using vst_last_tie tags ({scenario})"
            )
        tdSql.execute(
            "create stable vst_string_federated "
            "(ts timestamp, mysql_v nchar(16), postgres_v nchar(16)) "
            "tags (scenario int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct_string_federated ("
            "mysql_v from "
            "vt_window_batch_mysql_a.vt_window_federated_batch_ext.remote_text.v, "
            "postgres_v from "
            "vt_window_batch_pg.vt_window_federated_batch_ext.remote_text.v) "
            "using vst_string_federated tags (30)"
        )

    @classmethod
    def teardown_class(cls):
        _FederatedEnv.teardown_class()

    @staticmethod
    def check_row(sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(1)
        for column, value in enumerate(expected):
            tdSql.checkData(0, column, value)

    def do_global_aggregate(self):
        db = "vt_window_federated_batch.vst_local_external"
        self.check_row(
            "select sum(local_v), sum(mysql_a_v), sum(mysql_b_v), "
            f"sum(postgres_v) from {db}",
            (20, 200, 2000, 20000),
        )
        self.check_row(
            "select count(local_v), avg(mysql_a_v), min(mysql_b_v), "
            f"max(postgres_v) from {db}",
            (8, 25, 100, 4000),
        )
        self.check_row(
            "select spread(local_v), round(stddev(mysql_a_v)), "
            f"round(variance(mysql_b_v)), first(postgres_v), last(postgres_v) from {db}",
            (3, 11, 12500, 1000, 4000),
        )
        self.check_row(
            "select sum(source_v) from "
            "vt_window_federated_batch.vst_single_source",
            (100,),
        )
        self.check_row(
            "select sum(primary_v), sum(aux_v) from "
            "vt_window_federated_batch.vst_same_source",
            (100, 1000),
        )
        mixed = "vt_window_federated_batch.vst_mixed_sources"
        self.check_row(
            "select sum(mysql_a_v), sum(mysql_b_v), "
            f"sum(postgres_v) from {mixed}",
            (100, 1000, 10000),
        )
        self.check_row(
            "select avg(mysql_a_v), avg(mysql_b_v), "
            f"avg(postgres_v) from {mixed}",
            (25, 250, 2500),
        )
        self.check_row(
            "select min(mysql_a_v), max(mysql_b_v), "
            f"count(postgres_v) from {mixed}",
            (10, 400, 4),
        )
        self.check_row(
            "select first(mysql_a_v), last(mysql_b_v), "
            f"spread(postgres_v) from {mixed}",
            (10, 400, 3000),
        )
        self.check_row(
            "select sum(local_v), sum(empty_v) from "
            "vt_window_federated_batch.vst_empty_branch",
            (10, None),
        )
        self.check_row(
            "select count(scenario), sum(scenario) from "
            "vt_window_federated_batch.vst_null_tag",
            (0, None),
        )
        print("federated VStableAgg global aggregates ........ [ passed ]")

    def do_partition_aggregate(self):
        db = "vt_window_federated_batch.vst_local_external"
        mixed = "vt_window_federated_batch.vst_mixed_sources"
        tdSql.query(
            "select sum(local_v), sum(mysql_a_v), "
            f"sum(mysql_b_v), sum(postgres_v) from {db} "
            "partition by scenario order by 1"
        )
        tdSql.checkRows(2)
        for row in range(2):
            for column, value in enumerate((10, 100, 1000, 10000)):
                tdSql.checkData(row, column, value)

        tdSql.query(
            "select sum(mysql_a_v), sum(mysql_b_v), "
            f"sum(postgres_v) from {mixed} "
            "partition by scenario order by 1"
        )
        tdSql.checkRows(1)
        for column, value in enumerate((100, 1000, 10000)):
            tdSql.checkData(0, column, value)
        print("federated VStableAgg partitions ............... [ passed ]")

    @staticmethod
    def plan_contains(sql, keyword):
        tdSql.query(f"explain verbose true {sql}", queryTimes=1)
        return any(
            value is not None and keyword in str(value)
            for row in tdSql.queryResult
            for value in row
        )

    def do_last_star(self):
        db = "vt_window_federated_batch.vst_last_federated"
        last_star = f"select last(*) from {db}"
        if not self.plan_contains(
            last_star,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=1",
        ):
            raise AssertionError("federated last(*) did not use VStableAgg")

        tdSql.query(last_star, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(0, 1, 333)
        tdSql.checkData(0, 2, 4000)

        multi_unit_ts = (
            "select last(ts) from "
            "vt_window_federated_batch.vst_mixed_sources"
        )
        if not self.plan_contains(
            multi_unit_ts,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=1",
        ):
            raise AssertionError(
                "multi-unit federated last(ts) did not use VStableAgg"
            )
        tdSql.query(multi_unit_ts, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:03.000000000")

        last_tag = f"select last(*), last(scenario), last(tbname) from {db}"
        if not self.plan_contains(
            last_tag,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=0",
        ):
            raise AssertionError(
                "federated last(*) with Tag/TBNAME did not use VStableAgg"
            )
        tdSql.query(last_tag, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(0, 1, 333)
        tdSql.checkData(0, 2, 4000)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(0, 4, "vct_last_federated")

        partitioned = (
            f"select last(*) from {db} partition by scenario order by 1"
        )
        if not self.plan_contains(
            partitioned,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=1 batch_process_child=1",
        ):
            raise AssertionError(
                "partitioned federated last(*) did not use VStableAgg"
            )
        tdSql.query(partitioned, queryTimes=1)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2024-01-01 00:00:02.000000000")
        tdSql.checkData(0, 1, 333)
        tdSql.checkData(0, 2, 22222)
        tdSql.checkData(1, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(1, 1, 333)
        tdSql.checkData(1, 2, 4000)

        last_row_star = f"select last_row(*) from {db}"
        if not self.plan_contains(
            last_row_star,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=1",
        ):
            raise AssertionError("federated last_row(*) did not use VStableAgg")
        tdSql.query(last_row_star, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 4000)

        last_row_tag = (
            f"select last_row(*), last_row(scenario), last_row(tbname) from {db}"
        )
        if not self.plan_contains(
            last_row_tag,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=0",
        ):
            raise AssertionError(
                "federated last_row(*) with Tag/TBNAME did not use VStableAgg"
            )
        tdSql.query(last_row_tag, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 4000)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(0, 4, "vct_last_federated")

        partitioned_last_row = (
            f"select last_row(*) from {db} partition by scenario order by 1"
        )
        if not self.plan_contains(
            partitioned_last_row,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=1 batch_process_child=1",
        ):
            raise AssertionError(
                "partitioned federated last_row(*) did not use VStableAgg"
            )
        tdSql.query(partitioned_last_row, queryTimes=1)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2024-01-01 00:00:02.000000000")
        tdSql.checkData(0, 1, 333)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, "2024-01-01 00:00:03.000000000")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, 4000)

        tie_db = "vt_window_federated_batch.vst_last_tie"
        tie_last = f"select last(*) from {tie_db}"
        if not self.plan_contains(
            tie_last,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=1",
        ):
            raise AssertionError(
                "same-timestamp federated last(*) did not use VStableAgg"
            )
        tdSql.query(tie_last, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:04.000000000")
        tdSql.checkData(0, 1, 444)
        tdSql.checkData(0, 2, 44444)

        tie_last_row = f"select last_row(*) from {tie_db}"
        if not self.plan_contains(
            tie_last_row,
            "Dynamic Query Control for Virtual Stable Agg "
            "(has_partition=0 batch_process_child=1",
        ):
            raise AssertionError(
                "same-timestamp federated last_row(*) did not use VStableAgg"
            )
        tdSql.query(tie_last_row, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:04.000000000")
        tdSql.checkData(0, 1, 444)
        tdSql.checkData(0, 2, 44444)

        tie_last_tag = (
            f"select last(*), last(scenario), last(tbname) from {tie_db}"
        )
        tdSql.query(tie_last_tag, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-01-01 00:00:04.000000000")
        tdSql.checkData(0, 1, 444)
        tdSql.checkData(0, 2, 44444)
        tdSql.checkData(0, 3, 20)
        tdSql.checkData(0, 4, "vct_last_tie_a")
        print("federated VStableAgg last(*) .................. [ passed ]")

    def do_extended_distributable_functions(self):
        mixed = "vt_window_federated_batch.vst_mixed_sources"
        marker = "Dynamic Query Control for Virtual Stable Agg "
        plan_queries = (
            f"select round(apercentile(mysql_a_v, 50)) from {mixed}",
            f"select top(mysql_a_v, 2) from {mixed}",
            f"select bottom(postgres_v, 2) from {mixed}",
            f"select first(*) from {mixed}",
            f"select last_row(*) from {mixed}",
            f"select min(ts), max(ts) from {mixed}",
            f"select hyperloglog(mysql_b_v) from {mixed}",
            f"select corr(state_col, mysql_a_v) from {mixed}",
            f"select std(mysql_a_v), stddev_samp(mysql_a_v), var_samp(mysql_a_v) from {mixed}",
            "select histogram(mysql_a_v, 'user_input', "
            f"'[0,20,40,60]', 0) from {mixed}",
            "select group_concat(mysql_v, ','), group_concat(postgres_v, ',') "
            "from vt_window_federated_batch.vst_string_federated",
        )
        for sql in plan_queries:
            if not self.plan_contains(sql, marker):
                raise AssertionError(
                    f"federated distributable aggregate did not use VStableAgg: {sql}"
                )

        self.check_row(
            "select round(apercentile(mysql_a_v, 50)), "
            "hyperloglog(mysql_b_v), round(corr(state_col, mysql_a_v), 6), "
            "round(std(mysql_a_v), 6), round(stddev_samp(mysql_a_v), 6), "
            f"round(var_samp(mysql_a_v), 6) from {mixed}",
            (20, 4, 0.894427, 11.18034, 12.909944, 166.666667),
        )

        tdSql.query(
            "select histogram(mysql_a_v, 'user_input', "
            f"'[0,20,40,60]', 0) from {mixed}"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '{"lower_bin":0, "upper_bin":20, "count":2}')
        tdSql.checkData(1, 0, '{"lower_bin":20, "upper_bin":40, "count":2}')
        tdSql.checkData(2, 0, '{"lower_bin":40, "upper_bin":60, "count":0}')

        tdSql.query(
            "select group_concat(mysql_v, ','), group_concat(postgres_v, ',') "
            "from vt_window_federated_batch.vst_string_federated"
        )
        tdSql.checkRows(1)
        assert sorted(tdSql.queryResult[0][0].split(",")) == ["a", "b", "c", "d"]
        assert sorted(tdSql.queryResult[0][1].split(",")) == ["w", "x", "y", "z"]

        tdSql.query(f"select top(mysql_a_v, 2) from {mixed} order by 1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 30)
        tdSql.checkData(1, 0, 40)

        tdSql.query(f"select bottom(postgres_v, 2) from {mixed} order by 1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1000)
        tdSql.checkData(1, 0, 2000)

        self.check_row(
            f"select first(*), min(ts), max(ts) from {mixed}",
            (
                "2024-01-01 00:00:00.000000000",
                1,
                10,
                100,
                1000,
                "2024-01-01 00:00:00.000000000",
                "2024-01-01 00:00:03.000000000",
            ),
        )
        self.check_row(
            f"select last_row(*) from {mixed}",
            (
                "2024-01-01 00:00:03.000000000",
                2,
                40,
                400,
                4000,
            ),
        )
        print("federated VStableAgg extended functions ...... [ passed ]")

    def do_tag_aggregate_and_expression_fallback(self):
        tag_db = "vt_window_federated_batch.vst_tag_federated"
        self.check_row(f"select sum(scenario) from {tag_db}", (36,))
        self.check_row(f"select count(scenario) from {tag_db}", (8,))
        self.check_row(
            f"select sum(mysql_v + scenario) from {tag_db}",
            (236,),
        )
        self.check_row(
            f"select sum(mysql_v), max(scenario) from {tag_db}",
            (200, 5),
        )
        self.check_row(
            f"select sum(mysql_v + length(tbname)) from {tag_db}",
            (352,),
        )
        self.check_row(
            "select min(label) from vt_window_federated_batch.vst_tag_types",
            ("",),
        )
        self.check_row(
            "select sum(mysql_v + length(label)) from "
            "vt_window_federated_batch.vst_tag_types",
            (1120,),
        )
        tdSql.query(
            f"select sum(scenario) from {tag_db} "
            "partition by scenario order by 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 16)
        tdSql.checkData(1, 0, 20)
        tdSql.query(
            f"select count(scenario) from {tag_db} "
            "partition by scenario order by 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 4)
        print("federated VStableAgg tags and fallback ......... [ passed ]")

    def do_plan_golden(self):
        test_case = "test_vstable_plan_test_agg_federated"
        tdCom.compare_testcase_result(
            os.path.join(os.path.dirname(__file__), "in", f"{test_case}.in"),
            os.path.join(os.path.dirname(__file__), "ans", f"{test_case}.ans"),
            test_case,
        )
        print("federated VStableAgg plans ..................... [ passed ]")

    def test_vstable_agg_federated(self):
        """Aggregate one virtual stable across local and external sources.

        1. Cover local, MySQL, and PostgreSQL partial aggregation branches.
        2. Cover global, tag, partition, and partition-plus-tag quadrants.
        3. Cover distributable aggregate functions and child-scope switching.
        4. Cover selection, multi-row, approximate, and multi-parameter functions.
        5. Keep unsupported cross-source expressions on the fallback path.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-24 Codex Added federated VStableAgg coverage
            - 2026-08-12 Codex Aligned federated distributable function coverage

        """
        self.do_global_aggregate()
        self.do_partition_aggregate()
        self.do_last_star()
        self.do_extended_distributable_functions()
        self.do_tag_aggregate_and_expression_fallback()
        self.do_plan_golden()
