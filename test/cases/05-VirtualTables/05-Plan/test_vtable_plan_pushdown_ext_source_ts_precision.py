###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdCom, tdLog, tdSql
from datetime import datetime, timedelta, timezone
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vtable_util import ExtSrcEnv, VtableQueryUtil


pytestmark = pytest.mark.timeout(3600)

_BASE_TIME_NS = 1704067200000000000
_BASE_TIME = datetime.fromtimestamp(_BASE_TIME_NS // 1_000_000_000)
_BASE_TIME_MS = _BASE_TIME_NS // 1_000_000
_ROW_COUNT = 2001
_LAST_VALUE = _ROW_COUNT - 1
_VTABLE_PRECISIONS = ("ms", "us", "ns")
_PRECISION_RANK = {"ms": 0, "us": 1, "ns": 2}
_TICKS_PER_MILLISECOND = {"ms": 1, "us": 1_000, "ns": 1_000_000}
_EXTERNAL_CASES = (
    {
        "backend": "mysql",
        "source_precision": "ms",
        "source_name": "vtp_mysql_ms_src",
        "external_db": "vtp_mysql_ms_ext",
        "timestamp_type": "datetime(3)",
        "value_type": "int",
        "expected_rows": {"ms": 2001, "us": 2001, "ns": 2001},
    },
    {
        "backend": "mysql",
        "source_precision": "us",
        "source_name": "vtp_mysql_us_src",
        "external_db": "vtp_mysql_us_ext",
        "timestamp_type": "datetime(6)",
        "value_type": "int",
        "expected_rows": {"ms": 3, "us": 2001, "ns": 2001},
    },
    {
        "backend": "postgres",
        "source_precision": "ms",
        "source_name": "vtp_pg_ms_src",
        "external_db": "vtp_pg_ms_ext",
        "timestamp_type": "timestamp(3)",
        "value_type": "int",
        "expected_rows": {"ms": 2001, "us": 2001, "ns": 2001},
    },
    {
        "backend": "postgres",
        "source_precision": "us",
        "source_name": "vtp_pg_us_src",
        "external_db": "vtp_pg_us_ext",
        "timestamp_type": "timestamp(6)",
        "value_type": "int",
        "expected_rows": {"ms": 3, "us": 2001, "ns": 2001},
    },
    {
        "backend": "influx",
        "source_precision": "ns",
        "source_name": "vtp_influx_ns_src",
        "external_db": "vtp_influx_ns_ext",
        "timestamp_type": None,
        "value_type": "bigint",
        "expected_rows": {"ms": 1, "us": 3, "ns": 2001},
    },
)
_MIXED_CASES = (
    {
        "name": "mixed_precision",
        "sources": (("mysql", "ms"), ("mysql", "us")),
        "include_native": False,
    },
    {
        "name": "mixed_external",
        "sources": (("mysql", "ms"), ("postgres", "us"), ("influx", "ns")),
        "include_native": False,
    },
    {
        "name": "mixed_native_external",
        "sources": (("mysql", "us"), ("postgres", "ms"), ("influx", "ns")),
        "include_native": True,
    },
)


def _local_db(case, vtable_precision):
    return (
        f"vtp_{case['backend']}_{case['source_precision']}_"
        f"{vtable_precision}"
    )


def _mixed_db(vtable_precision):
    return f"vtp_mixed_{vtable_precision}"


def _source_column(source):
    return f"v_{source[0]}_{source[1]}"


def _external_case(source):
    return next(
        case
        for case in _EXTERNAL_CASES
        if (case["backend"], case["source_precision"]) == source
    )


def _precision_is_legal(source_precision, vtable_precision):
    return (
        _PRECISION_RANK[vtable_precision]
        >= _PRECISION_RANK[source_precision]
    )


def _source_timestamps(source_precision, vtable_precision):
    source_ticks = _TICKS_PER_MILLISECOND[source_precision]
    target_ticks = _TICKS_PER_MILLISECOND[vtable_precision]
    base_timestamp = _BASE_TIME_MS * target_ticks
    return {
        base_timestamp + value * target_ticks // source_ticks
        for value in range(_ROW_COUNT)
    }


def _source_value_at_timestamp(
    source_precision, vtable_precision, target_timestamp
):
    source_ticks = _TICKS_PER_MILLISECOND[source_precision]
    target_ticks = _TICKS_PER_MILLISECOND[vtable_precision]
    base_timestamp = _BASE_TIME_MS * target_ticks
    # Rows are source-time ordered, so later rows overwrite earlier rows that
    # collapse onto the same virtual-table timestamp.
    return max(
        (
            value
            for value in range(_ROW_COUNT)
            if base_timestamp + value * target_ticks // source_ticks
            == target_timestamp
        ),
        default=None,
    )


def _native_timestamps(vtable_precision):
    base_timestamp = _BASE_TIME_MS * _TICKS_PER_MILLISECOND[vtable_precision]
    return {base_timestamp + value for value in range(_ROW_COUNT)}


def _millisecond_timestamp_literal(value, precision):
    ticks_per_millisecond = _TICKS_PER_MILLISECOND[precision]
    assert value % ticks_per_millisecond == 0
    epoch_milliseconds = value // ticks_per_millisecond
    seconds, milliseconds = divmod(epoch_milliseconds, 1_000)
    date_time = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return (
        date_time.strftime("%Y-%m-%dT%H:%M:%S")
        + f".{milliseconds:03d}+00:00"
    )


class TestVTablePlanPushdownExtSourceTsPrecision:
    @classmethod
    def setup_class(cls):
        adapters = {}
        ExtSrcEnv.ensure_qnode()

        for case in _EXTERNAL_CASES:
            backend = case["backend"]
            if backend not in adapters:
                adapter = VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]()
                adapter.ensure_running()
                adapters[backend] = adapter

            tdSql.execute(
                f"drop external source if exists {case['source_name']}"
            )
            cls._prepare_external_data(case)
            cls._create_external_source(case, adapters[backend])
            cls._create_virtual_tables(case)

        cls._create_mixed_source_vtables()

    @classmethod
    def _prepare_external_data(cls, case):
        if case["backend"] == "mysql":
            cls._prepare_mysql_data(case)
        elif case["backend"] == "postgres":
            cls._prepare_postgres_data(case)
        else:
            cls._prepare_influx_data(case)

    @staticmethod
    def _prepare_mysql_data(case):
        external_db = case["external_db"]
        ExtSrcEnv.mysql_exec(
            None, [f"create database if not exists `{external_db}`"]
        )
        ExtSrcEnv.mysql_exec(
            external_db,
            [
                "drop table if exists dense_ts",
                f"create table dense_ts (ts {case['timestamp_type']} "
                "not null primary key, v int not null)",
            ],
        )

        tick_us = 1000 if case["source_precision"] == "ms" else 1
        values = []
        for value in range(_ROW_COUNT):
            timestamp = _BASE_TIME + timedelta(microseconds=value * tick_us)
            timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
            if case["source_precision"] == "ms":
                timestamp_text = timestamp_text[:-3]
            values.append(f"('{timestamp_text}', {value})")

        for start in range(0, _ROW_COUNT, 500):
            batch = ",".join(values[start : start + 500])
            ExtSrcEnv.mysql_exec(
                external_db,
                [f"insert into dense_ts (ts, v) values {batch}"],
            )

    @staticmethod
    def _prepare_postgres_data(case):
        external_db = case["external_db"]
        ExtSrcEnv.pg_create_db(external_db)
        base_time = _BASE_TIME.strftime("%Y-%m-%d %H:%M:%S")
        interval = (
            "1 millisecond"
            if case["source_precision"] == "ms"
            else "1 microsecond"
        )
        ExtSrcEnv.pg_exec(
            external_db,
            [
                "drop table if exists dense_ts",
                f"create table dense_ts (ts {case['timestamp_type']} "
                "not null primary key, v integer not null)",
                "insert into dense_ts (ts, v) "
                f"select timestamp '{base_time}' + "
                f"value * interval '{interval}', value "
                f"from generate_series(0, {_LAST_VALUE}) as values(value)",
            ],
        )

    @staticmethod
    def _prepare_influx_data(case):
        external_db = case["external_db"]
        ExtSrcEnv.influx_drop_db(external_db)
        ExtSrcEnv.influx_create_db(external_db)
        lines = [
            f"dense_ts v={value}i {_BASE_TIME_NS + value}"
            for value in range(_ROW_COUNT)
        ]
        ExtSrcEnv.influx_write(external_db, lines)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                result = ExtSrcEnv.influx_query_sql(
                    external_db, 'select count(*) from "dense_ts"'
                )
                if result and int(result[0].get("count(*)", 0)) == _ROW_COUNT:
                    return
            except Exception as err:
                tdLog.info(f"waiting for InfluxDB dense_ts visibility: {err}")
            time.sleep(1)
        raise AssertionError(
            f"InfluxDB dense_ts did not expose {_ROW_COUNT} rows within 30 seconds"
        )

    @staticmethod
    def _create_external_source(case, adapter):
        source_name = case["source_name"]
        external_db = case["external_db"]
        if case["backend"] == "mysql":
            tdSql.execute(
                f"create external source {source_name} type='mysql' "
                f"host='{adapter.host}' port={adapter.port} "
                f"user='{adapter.user}' password='{adapter.password}' "
                f"database={external_db}"
            )
        elif case["backend"] == "postgres":
            tdSql.execute(
                f"create external source {source_name} type='postgresql' "
                f"host='{adapter.host}' port={adapter.port} "
                f"user='{adapter.user}' password='{adapter.password}' "
                f"database={external_db} schema=public"
            )
        else:
            tdSql.execute(
                f"create external source {source_name} type='influxdb' "
                f"host='{adapter.host}' port={adapter.port} "
                f"user='u' password='' database={external_db} "
                f"options('api_token'='{adapter.token}',"
                f"'protocol'='flight_sql')"
            )

    @staticmethod
    def _create_virtual_tables(case):
        for vtable_precision in _VTABLE_PRECISIONS:
            local_db = _local_db(case, vtable_precision)
            tdSql.execute(f"drop database if exists {local_db}")
            tdSql.execute(
                f"create database {local_db} vgroups 2 "
                f"precision '{vtable_precision}'"
            )
            if not _precision_is_legal(
                case["source_precision"], vtable_precision
            ):
                continue
            tdSql.execute(f"use {local_db}")
            tdSql.execute(
                "create vtable dense_vtable (ts timestamp, "
                f"v {case['value_type']} from {case['source_name']}."
                f"{case['external_db']}.dense_ts.v)"
            )
            tdSql.execute(
                "create stable dense_vstable (ts timestamp, "
                f"v {case['value_type']}) tags (source nchar(32)) virtual 1"
            )
            tdSql.execute(
                "create vtable dense_vchild ("
                f"v from {case['source_name']}.{case['external_db']}.dense_ts.v) "
                "using dense_vstable "
                f"tags ('{case['backend']}_{case['source_precision']}')"
            )

    @classmethod
    def _create_mixed_source_vtables(cls):
        for vtable_precision in _VTABLE_PRECISIONS:
            local_db = _mixed_db(vtable_precision)
            tdSql.execute(f"drop database if exists {local_db}")
            tdSql.execute(
                f"create database {local_db} vgroups 2 "
                f"precision '{vtable_precision}'"
            )
            tdSql.execute(f"use {local_db}")
            cls._prepare_native_data(vtable_precision)

            for mixed_case in _MIXED_CASES:
                if any(
                    not _precision_is_legal(source[1], vtable_precision)
                    for source in mixed_case["sources"]
                ):
                    continue
                column_defs = ["ts timestamp"]
                if mixed_case["include_native"]:
                    column_defs.append("v_native int from dense_native.v")
                for source in mixed_case["sources"]:
                    case = _external_case(source)
                    column_defs.append(
                        f"{_source_column(source)} {case['value_type']} from "
                        f"{case['source_name']}.{case['external_db']}.dense_ts.v"
                    )
                tdSql.execute(
                    f"create vtable {mixed_case['name']} "
                    f"({', '.join(column_defs)})"
                )

    @staticmethod
    def _prepare_native_data(vtable_precision):
        tdSql.execute("create table dense_native (ts timestamp, v int)")
        base_timestamp = (
            _BASE_TIME_MS * _TICKS_PER_MILLISECOND[vtable_precision]
        )
        values = [
            f"({base_timestamp + value}, {value})"
            for value in range(_ROW_COUNT)
        ]
        for start in range(0, _ROW_COUNT, 500):
            batch = ",".join(values[start : start + 500])
            tdSql.execute(f"insert into dense_native values {batch}")

    def do_validate_dense_timestamp_results(self):
        for case in _EXTERNAL_CASES:
            for vtable_precision in _VTABLE_PRECISIONS:
                if not _precision_is_legal(
                    case["source_precision"], vtable_precision
                ):
                    continue
                local_db = _local_db(case, vtable_precision)
                tdSql.execute(f"use {local_db}")
                tdSql.query("select cast(ts as bigint) from dense_vtable")
                tdSql.checkRows(case["expected_rows"][vtable_precision])
                baseline_timestamps = sorted(
                    int(row[0]) for row in (tdSql.queryResult or [])
                )
                lower_timestamp = baseline_timestamps[0]
                upper_timestamp = (
                    lower_timestamp
                    + _TICKS_PER_MILLISECOND[vtable_precision]
                )
                expected_range = [
                    timestamp
                    for timestamp in baseline_timestamps
                    if lower_timestamp <= timestamp < upper_timestamp
                ]
                lower_literal = _millisecond_timestamp_literal(
                    lower_timestamp, vtable_precision
                )
                upper_literal = _millisecond_timestamp_literal(
                    upper_timestamp, vtable_precision
                )

                tdSql.query(
                    "select cast(ts as bigint) from dense_vtable "
                    f"where ts >= '{lower_literal}' "
                    f"and ts < '{upper_literal}'"
                )
                tdSql.checkRows(len(expected_range))
                assert sorted(
                    int(row[0]) for row in (tdSql.queryResult or [])
                ) == expected_range

                tdSql.query(
                    "select cast(ts as bigint), v from dense_vtable "
                    f"where ts = '{lower_literal}'"
                )
                tdSql.checkRows(1)
                assert int(tdSql.getData(0, 0)) == lower_timestamp
                expected_value = _source_value_at_timestamp(
                    case["source_precision"],
                    vtable_precision,
                    lower_timestamp,
                )
                assert expected_value is not None
                assert int(tdSql.getData(0, 1)) == expected_value

                print(
                    f"{case['backend']} {case['source_precision']} -> "
                    f"{vtable_precision} dense timestamp results ... [ passed ]"
                )

    def do_validate_vstable_timestamp_filter_results(self):
        for case in _EXTERNAL_CASES:
            for vtable_precision in _VTABLE_PRECISIONS:
                if not _precision_is_legal(
                    case["source_precision"], vtable_precision
                ):
                    continue
                expected_timestamp = (
                    _BASE_TIME_MS
                    * _TICKS_PER_MILLISECOND[vtable_precision]
                )
                timestamp_literal = _millisecond_timestamp_literal(
                    expected_timestamp, vtable_precision
                )
                tdSql.query(
                    "select cast(ts as bigint), v, source from "
                    f"{_local_db(case, vtable_precision)}.dense_vstable "
                    f"where ts = '{timestamp_literal}'"
                )
                tdSql.checkRows(1)
                assert int(tdSql.getData(0, 0)) == expected_timestamp
                assert tdSql.getData(0, 1) is not None
                assert tdSql.getData(0, 2) == (
                    f"{case['backend']}_{case['source_precision']}"
                )

        print("virtual stable timestamp filter results ... [ passed ]")

    def do_validate_mixed_source_results(self):
        for vtable_precision in _VTABLE_PRECISIONS:
            base_timestamp = (
                _BASE_TIME_MS * _TICKS_PER_MILLISECOND[vtable_precision]
            )
            upper_timestamp = (
                base_timestamp
                + _TICKS_PER_MILLISECOND[vtable_precision]
            )
            lower_literal = _millisecond_timestamp_literal(
                base_timestamp, vtable_precision
            )
            upper_literal = _millisecond_timestamp_literal(
                upper_timestamp, vtable_precision
            )

            for mixed_case in _MIXED_CASES:
                if any(
                    not _precision_is_legal(source[1], vtable_precision)
                    for source in mixed_case["sources"]
                ):
                    continue
                columns = []
                expected_by_column = {}
                if mixed_case["include_native"]:
                    columns.append("v_native")
                    expected_by_column["v_native"] = _native_timestamps(
                        vtable_precision
                    )
                for source in mixed_case["sources"]:
                    column = _source_column(source)
                    columns.append(column)
                    expected_by_column[column] = _source_timestamps(
                        source[1], vtable_precision
                    )

                expected_by_column = {
                    column: {
                        timestamp
                        for timestamp in timestamps
                        if base_timestamp <= timestamp < upper_timestamp
                    }
                    for column, timestamps in expected_by_column.items()
                }
                expected_timestamps = set().union(
                    *expected_by_column.values()
                )
                projection = ", ".join(columns)
                tdSql.query(
                    f"select cast(ts as bigint), {projection} "
                    f"from {_mixed_db(vtable_precision)}."
                    f"{mixed_case['name']} "
                    f"where ts >= '{lower_literal}' "
                    f"and ts < '{upper_literal}' order by ts"
                )
                tdSql.checkRows(len(expected_timestamps))
                rows = tdSql.queryResult or []
                assert [int(row[0]) for row in rows] == sorted(
                    expected_timestamps
                )
                for column_index, column in enumerate(columns, start=1):
                    actual_timestamps = {
                        int(row[0])
                        for row in rows
                        if row[column_index] is not None
                    }
                    assert actual_timestamps == expected_by_column[column]

                tdSql.query(
                    f"select cast(ts as bigint), {projection} "
                    f"from {_mixed_db(vtable_precision)}."
                    f"{mixed_case['name']} where ts = '{lower_literal}'"
                )
                tdSql.checkRows(1)
                assert int(tdSql.getData(0, 0)) == base_timestamp
                expected_values = []
                if mixed_case["include_native"]:
                    expected_values.append(0)
                for source in mixed_case["sources"]:
                    expected_value = _source_value_at_timestamp(
                        source[1], vtable_precision, base_timestamp
                    )
                    assert expected_value is not None
                    expected_values.append(expected_value)
                assert [
                    int(value)
                    for value in (tdSql.queryResult or [])[0][1:]
                ] == expected_values

                print(
                    f"{mixed_case['name']} -> {vtable_precision} dense "
                    "timestamp results ... [ passed ]"
                )

    @staticmethod
    def do_validate_cross_precision_join_results():
        case = _external_case(("mysql", "ms"))
        precision_pairs = (
            ("ms", "us"),
            ("us", "ns"),
            ("ms", "ns"),
            ("ns", "ms"),
        )

        for left_precision, right_precision in precision_pairs:
            left_table = f"{_local_db(case, left_precision)}.dense_vtable"
            right_table = f"{_local_db(case, right_precision)}.dense_vtable"
            join_sql = (
                f"select count(*) from {left_table} left_vtable "
                f"join {right_table} right_vtable "
                "on left_vtable.ts = right_vtable.ts"
            )
            tdSql.query(f"explain verbose true {join_sql}")
            assert any(
                "__timestamp_scale" in str(row[0])
                for row in (tdSql.queryResult or [])
            )

            tdSql.query(join_sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, _ROW_COUNT)

        print("cross-precision virtual table joins ... [ passed ]")

    @staticmethod
    def do_compare_timestamp_precision_plans_and_results():
        test_case = "test_vtable_plan_pushdown_ext_source_ts_precision"
        input_file = os.path.join(
            os.path.dirname(__file__), "in", f"{test_case}.in"
        )
        answer_file = os.path.join(
            os.path.dirname(__file__), "ans", f"{test_case}.ans"
        )
        tdCom.compare_testcase_result(input_file, answer_file, test_case)

    @staticmethod
    def do_compare_vstable_timestamp_filter_plans():
        test_case = "test_vstable_plan_pushdown_ext_source_ts_filter"
        input_file = os.path.join(
            os.path.dirname(__file__), "in", f"{test_case}.in"
        )
        answer_file = os.path.join(
            os.path.dirname(__file__), "ans", f"{test_case}.ans"
        )
        tdCom.compare_testcase_result(input_file, answer_file, test_case)

    def test_timestamp_precision_matrix(self):
        """Validate timestamp pushdown for single and mixed sources.

        1. Check dense results for 11 legal single-source precision combinations.
        2. Check four legal mixed-precision, mixed-database and native/external cases.
        3. Check that virtual scans retain their own precision in cross-precision joins.
        4. Compare EXPLAIN plans and matching query results.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-14 Codex Added external timestamp precision matrix
            - 2026-07-15 Codex Added matching query-result comparisons

        """
        self.do_validate_dense_timestamp_results()
        self.do_validate_mixed_source_results()
        self.do_validate_cross_precision_join_results()
        self.do_compare_timestamp_precision_plans_and_results()

    def test_vstable_timestamp_filter_plan(self):
        """Validate federated timestamp pushdown plans for virtual super tables.

        1. Check timestamp-filter results for all 11 legal precision combinations.
        2. Compare timestamp ranges and FederatedScan nodes in each plan.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-14 Codex Added virtual super table timestamp checks

        """
        self.do_validate_vstable_timestamp_filter_results()
        self.do_compare_vstable_timestamp_filter_plans()
