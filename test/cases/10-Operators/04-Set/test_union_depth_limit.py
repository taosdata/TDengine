# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

from itertools import permutations

from new_test_framework.utils import tdLog, tdSql


class TestUnionDepthLimit:
    branch_num = 3000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    @classmethod
    def _build_union_all_sql(cls, branch_num):
        return " union all ".join(["select 1"] * branch_num)

    # --- impl ---
    def do_accept_large_set_operator_chain(self):
        sql = self._build_union_all_sql(self.branch_num)
        tdSql.query(sql)
        tdSql.checkRows(self.branch_num)
        # UNION ALL does not guarantee row order without ORDER BY; row count verifies every branch.
        tdSql.query("select 1", queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select 1 union select 1 union all select 2")
        tdSql.checkRows(2)
        tdSql.query("select 1 union all select 2 union all select 3")
        tdSql.checkRows(3)
        tdSql.query("select 1 union all select 2 union select 2")
        tdSql.checkRows(2)
        tdSql.query("(select 1 union select 1) union all select 2")
        tdSql.checkRows(2)
        tdSql.query("select 1 union all (select 1 union select 2)")
        tdSql.checkRows(3)
        tdSql.query("(select 1 union all select 2) union all (select 3 union all select 4)")
        tdSql.checkRows(4)
        print("large union chain ......................... [ passed ]")

    def do_align_operand_types(self):
        wide_value = "z" * 4500
        operands = [
            "select cast('a' as varchar(16)) as value",
            "select cast('b' as varchar(64)) as value",
            f"select cast('{wide_value}' as varchar(5000)) as value",
        ]
        for order in permutations(operands):
            for operator in ("union", "union all"):
                tdSql.query(f" {operator} ".join(order) + " order by 1")
                tdSql.checkRows(3)
                for row, value in enumerate(("a", "b", wide_value)):
                    tdSql.checkData(row, 0, value)
        for sql in (
            f"({operands[0]} union {operands[1]}) union all {operands[2]}",
            f"{operands[2]} union all ({operands[0]} union {operands[1]})",
            f"({operands[0]} union all {operands[1]}) union {operands[2]}",
            f"{operands[2]} union ({operands[0]} union all {operands[1]})",
        ):
            tdSql.query(sql + " order by 1")
            tdSql.checkRows(3)
            for row, value in enumerate(("a", "b", wide_value)):
                tdSql.checkData(row, 0, value)
        tdSql.query("select null union all select 'a' union all select 'long value'")
        tdSql.checkRows(3)
        assert sorted(str(row[0]) for row in tdSql.queryResult) == ["None", "a", "long value"]
        print("union operand types ...................... [ passed ]")

    def do_align_timestamp_precision(self):
        operands = []
        try:
            for precision in ("ms", "us", "ns"):
                database = f"union_alignment_{precision}"
                tdSql.execute(f"drop database if exists {database}")
                tdSql.execute(f"create database {database} precision '{precision}'")
                tdSql.execute(f"create table {database}.t (ts timestamp, value int)")
                tdSql.execute(f"insert into {database}.t values ('2024-01-01 00:00:00', 1)")
                operands.append(f"select ts as aligned_ts from {database}.t")
            for order in permutations(operands):
                for operator in ("union", "union all"):
                    tdSql.query(f" {operator} ".join(order) + " order by 1")
                    tdSql.checkRows(1 if operator == "union" else 3)
                    for row in range(tdSql.queryRows):
                        tdSql.checkData(row, 0, "2024-01-01 00:00:00.000000000")
            for sql in (
                f"({operands[0]} union {operands[1]}) union all {operands[2]}",
                f"{operands[2]} union all ({operands[0]} union {operands[1]})",
                f"({operands[0]} union all {operands[1]}) union {operands[2]}",
                f"{operands[2]} union ({operands[0]} union all {operands[1]})",
            ):
                tdSql.query(sql + " order by 1")
                tdSql.checkRows(2 if "union all (" in sql or ") union all" in sql else 1)
                for row in range(tdSql.queryRows):
                    tdSql.checkData(row, 0, "2024-01-01 00:00:00.000000000")
        finally:
            for precision in ("ms", "us", "ns"):
                tdSql.execute(f"drop database if exists union_alignment_{precision}")
        print("union timestamp precision ................ [ passed ]")

    # --- main ---
    def test_union_depth_limit(self):
        """Execute a large UNION ALL chain without recursive AST traversal

        1. Execute 3000 UNION ALL operands and verify every row is returned
        2. Verify the same connection can execute a normal query afterwards
        3. Verify mixed UNION and UNION ALL semantics remain unchanged
        4. Align all operands and nested set operators to the final column width
        5. Align ms/us/ns timestamps for every operand order and mixed set operators

        Catalog:
            - Query:Union

        Since: v3.4.2.8

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-08 add flat set-operator regression
            - 2026-10-09 add operand type and timestamp precision regressions

        """
        self.do_accept_large_set_operator_chain()
        self.do_align_operand_types()
        self.do_align_timestamp_precision()
