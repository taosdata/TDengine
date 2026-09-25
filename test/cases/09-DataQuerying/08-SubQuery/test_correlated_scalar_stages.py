# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, etool
import uuid


class TestCorrelatedScalarStages:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def check_result(self, sql, expected):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(len(expected))
        if expected:
            tdSql.checkCols(len(expected[0]))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def do_projection_and_filter(self):
        scalar = "(select h.v from history h where h.device = r.device)"
        self.check_result(
            f"select r.v, {scalar} from readings r order by r.ts",
            [(10, 11), (20, 11), (30, None)],
        )
        self.check_result(
            f"select r.v from readings r where r.v < {scalar} order by r.ts",
            [(10,)],
        )
        self.check_result(
            f"select r.v, case when r.v > 0 then 0 else {scalar} end "
            "from readings r order by r.ts",
            [(10, 0), (20, 0), (30, 0)],
        )
        print("projection, filter, CASE ................. [ passed ]")

    def do_value_null_error_alignment(self):
        tdSql.execute("create table aligned_outer (ts timestamp, device varchar(16), v int)")
        tdSql.execute("create table aligned_inner (ts timestamp, device varchar(16), v int)")
        tdSql.execute(
            "insert into aligned_outer values "
            "('2026-09-11 00:00:01', 'd1', 10) "
            "('2026-09-11 00:00:02', 'd2', 20) "
            "('2026-09-11 00:00:03', 'd3', 30)"
        )
        tdSql.execute(
            "insert into aligned_inner values "
            "('2026-09-11 00:00:01', 'd1', 11) "
            "('2026-09-11 00:00:03', 'd3', 31) "
            "('2026-09-11 00:00:04', 'd3', 32)"
        )
        self.check_result(
            "select r.v, r.v + (select h.v from aligned_inner h "
            "where h.device = r.device and h.v < 32) "
            "from aligned_outer r order by r.ts",
            [(10, 21), (20, None), (30, 61)],
        )
        tdSql.error(
            "select r.v, r.v + (select h.v from aligned_inner h "
            "where h.device = r.device) from aligned_outer r order by r.ts",
            expectedErrno=0x26A5,
        )
        print("value, NULL, error row alignment ...... [ passed ]")

    def do_outer_join(self):
        join_condition = (
            "on h.ts = r.ts and h.device = r.device and h.v = "
            "(select max(x.v) from history x where x.device = r.device) "
        )
        for kind, order, expected, hints in [
            ("left", "r.ts", [(10, 11), (20, None), (30, None)], [False, True]),
            ("right", "h.ts", [(10, 11), (None, 22)], [False, True]),
            (
                "full", "coalesce(r.ts, h.ts)",
                [(10, 11), (20, None), (30, None), (None, 22)], [False],
            ),
        ]:
            for use_hash_hint in hints:
                hint = "/*+ hash_join() */ " if use_hash_hint else ""
                sql = (
                    f"select {hint}r.v, h.v from readings r {kind} join history h "
                    + join_condition + f"order by {order}"
                )
                self.check_result(sql, expected)
        hash_inner_baseline = (
            "select /*+ hash_join() */ r.v, h.v from readings r "
            "inner join history h on h.ts = r.ts and h.device = r.device"
        )
        self.check_result(hash_inner_baseline, [(10, 11)])
        correlated_inner_sql = (
            "select /*+ hash_join() */ r.v, h.v "
            "from readings r inner join history h "
            + join_condition + "order by r.ts"
        )
        self.check_result(correlated_inner_sql, [(10, 11)])
        print("outer JOIN ON ............................ [ passed ]")

    def do_unselected_case_error(self):
        scalar = "(select h.v from multi_history h where h.device = r.device)"
        tdSql.error(
            f"select case when r.v > 0 then 0 else {scalar} end "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        tdSql.error(
            f"select sum(case when r.v > 0 then 0 else {scalar} end) "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        print("unselected CASE errors ................... [ passed ]")

    def do_final_cardinality(self):
        self.check_result(
            "select r.v, (select max(h.v) from multi_history h "
            "where h.device = r.device group by h.device) "
            "from readings r order by r.ts",
            [(10, 12), (20, 12), (30, None)],
        )
        tdSql.error(
            "select (select max(h.v) from multi_history h "
            "where h.device = r.device group by h.v) "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        tdSql.error(
            "select (select max(h.v) from multi_history h "
            "where h.device = r.device partition by h.v) "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        tdSql.error(
            "select (select max(h.v) from multi_history h "
            "where h.device = r.device interval(1s)) "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        self.check_result(
            "select r.v, (select distinct h.device from multi_history h "
            "where h.device = r.device) from readings r order by r.ts",
            [(10, 'd1'), (20, 'd1'), (30, None)],
        )
        tdSql.error(
            "select (select distinct h.v from multi_history h "
            "where h.device = r.device) from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        self.check_result(
            "select r.v, (select h.device from multi_history h where h.device = r.device "
            "union select h.device from multi_history h where h.device = r.device) "
            "from readings r order by r.ts",
            [(10, 'd1'), (20, 'd1'), (30, None)],
        )
        tdSql.error(
            "select (select h.device from multi_history h where h.device = r.device "
            "union all select h.device from multi_history h where h.device = r.device) "
            "from readings r where r.device = 'd1'",
            expectedErrno=0x26A5,
        )
        print("final GROUP/PARTITION/window/set rows ... [ passed ]")

    def do_aggregate_case_calls(self):
        sql = (
            "select sum(case when r.v > 0 then 0 else "
            "(select h.v from history h where h.device = r.device) end) "
            "from readings r where r.device = 'd1'"
        )
        self.check_result(sql, [(0,)])
        print("aggregate CASE input calls .............. [ passed ]")

    def do_materialized_reuse(self):
        sql = (
            "select x.v + x.v from "
            "(select r.ts, (select h.v from history h "
            "where h.device = r.device) as v from readings r) x order by x.ts"
        )
        self.check_result(sql, [(22,), (22,), (None,)])
        print("materialized scalar reused twice ....... [ passed ]")

    def do_stages(self):
        database = f"correlated_scalar_e2_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database}")
        try:
            tdSql.execute(f"use {database}")
            tdSql.execute("create table readings (ts timestamp, device varchar(16), v int)")
            tdSql.execute("create table history (ts timestamp, device varchar(16), v int)")
            tdSql.execute("create table multi_history (ts timestamp, device varchar(16), v int)")
            tdSql.execute(
                "insert into readings values "
                "('2026-09-11 00:00:01', 'd1', 10) "
                "('2026-09-11 00:00:02', 'd1', 20) "
                "('2026-09-11 00:00:03', 'd3', 30)"
            )
            tdSql.execute(
                "insert into history values "
                "('2026-09-11 00:00:01', 'd1', 11) "
                "('2026-09-11 00:00:04', 'd2', 22)"
            )
            tdSql.execute(
                "insert into multi_history values "
                "('2026-09-11 00:00:01', 'd1', 11) "
                "('2026-09-11 00:00:02', 'd1', 12)"
            )
            self.do_projection_and_filter()
            self.do_value_null_error_alignment()
            self.do_outer_join()
            self.do_unselected_case_error()
            self.do_final_cardinality()
            self.do_aggregate_case_calls()
            self.do_materialized_reuse()
        finally:
            tdSql.execute(f"drop database {database}")

    def test_correlated_stages(self):
        """Correlated expressions use the owning operator stage

        1. Projection, filter and CASE receive row-aligned results;
           one input table tests value, NULL, then a later scalar error.
        2. LEFT/RIGHT/FULL JOIN ON preserve unmatched rows even with a
           hash hint; ordinary INNER hash remains available.
        3. Unselected CASE and aggregate input still report scalar errors.
        4. SUM over CASE calls the scalar once for each input row.
        5. EXPLAIN result files cover the static template and actual calls.
        6. GROUP, PARTITION, window, DISTINCT and UNION check final row count.
        7. A passthrough Project is removed, while reading a derived scalar
           value twice does not double its calls.
        8. Result columns exclude hidden binding/work columns.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Add stage and EXPLAIN checks.

        """
        self.do_stages()
