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

from new_test_framework.utils import tdLog, tdSql, etool


class TestExecutorBlockBoundarySecurity:
    DB = "executor_block_boundary"
    VALUE_LEN = 60000
    BASE_TS = 1735689600000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.large_a = "a" * cls.VALUE_LEN
        cls.large_b = "b" * cls.VALUE_LEN

    # --- util ---

    def require_plan(self, sql, *operators):
        tdSql.query(f"explain {sql}")
        plan = "\n".join(str(cell) for row in tdSql.queryResult for cell in row).lower()
        for operator in operators:
            if operator.lower() not in plan:
                raise AssertionError(f"expected {operator!r} in plan:\n{plan}")

    def check_server_health(self):
        tdSql.query("select server_version()")
        tdSql.checkRows(1)

    # --- impl ---

    def do_prepare_data(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} vgroups 3")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute(
            "create stable boundary_st "
            "(ts timestamp, seq int, v varchar(61000), marker varchar(8)) tags(tid int)"
        )
        for table_id in range(12):
            tdSql.execute(f"create table boundary_{table_id} using boundary_st tags({table_id})")

        tdSql.execute(
            f"insert into boundary_0 values({self.BASE_TS}, 0, '{self.large_a}', 'left')"
        )
        tdSql.execute(
            f"insert into boundary_1 values({self.BASE_TS + 1}, 1, '{self.large_b}', 'right')"
        )
        for table_id in range(2, 12):
            tdSql.execute(
                f"insert into boundary_{table_id} values({self.BASE_TS + table_id}, {table_id}, "
                f"'short-{table_id}', 'short')"
            )

        tdSql.execute(
            "create table join_left "
            "(ts timestamp, id int, k varchar(61000), payload varchar(16))"
        )
        tdSql.execute(
            "create table join_right "
            "(ts timestamp, id int, k varchar(61000), payload varchar(16))"
        )
        tdSql.execute(
            f"insert into join_left values({self.BASE_TS}, 1, '{self.large_a}', 'left-value')"
        )
        tdSql.execute(
            f"insert into join_right values({self.BASE_TS}, 1, '{self.large_a}', 'right-value')"
        )

        tdSql.execute("create table fill_values (ts timestamp, v varchar(61000))")
        tdSql.execute(
            f"insert into fill_values values({self.BASE_TS}, '{self.large_a}') "
            f"({self.BASE_TS + 2000}, '{self.large_b}')"
        )
        print("prepare executor boundary data ................. [ passed ]")

    def do_reject_oversized_variable_input(self):
        tdSql.execute("create table reject_oversized (ts timestamp, v varchar(2))")
        tdSql.error(
            f"insert into reject_oversized values({self.BASE_TS}, '{self.large_a}')"
        )
        tdSql.query("select count(*) from reject_oversized")
        tdSql.checkData(0, 0, 0)
        self.check_server_health()
        print("oversized variable input rejected .............. [ passed ]")

    def do_exchange_large_value(self):
        sql = (
            "select seq, length(v), substr(v, 1, 1), substr(v, 60000, 1) "
            "from boundary_st order by seq"
        )
        self.require_plan(sql, "exchange")
        tdSql.query(sql)
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, self.VALUE_LEN)
        tdSql.checkData(0, 2, "a")
        tdSql.checkData(0, 3, "a")
        tdSql.checkData(1, 1, self.VALUE_LEN)
        tdSql.checkData(1, 2, "b")
        tdSql.checkData(1, 3, "b")
        print("large values through exchange ................ [ passed ]")

    def do_concat_across_exchange(self):
        sql = (
            "select seq, length(concat(marker, v, marker)), "
            "substr(concat(marker, v, marker), 1, 4), "
            "substr(concat(marker, v, marker), 60005, 4), "
            "substr(concat(marker, v, marker), 60006, 4) "
            "from boundary_st where seq < 2 order by seq"
        )
        self.require_plan(sql, "exchange")
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, self.VALUE_LEN + 8)
        tdSql.checkData(0, 2, "left")
        tdSql.checkData(0, 3, "left")
        tdSql.checkData(1, 1, self.VALUE_LEN + 10)
        tdSql.checkData(1, 2, "righ")
        tdSql.checkData(1, 4, "righ")
        print("concat width through exchange ................ [ passed ]")

    def do_join_large_key_and_value(self):
        sql = (
            "select l.id, length(l.k), length(l.payload), length(r.payload), "
            "substr(l.k, 60000, 1) "
            "from join_left l inner join join_right r "
            "on l.ts = r.ts and l.k = r.k and l.id = r.id"
        )
        self.require_plan(sql, "join", "exchange")
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, self.VALUE_LEN)
        tdSql.checkData(0, 2, 10)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, "a")
        print("join large key and value ..................... [ passed ]")

    def do_fill_large_value(self):
        sql = (
            "select _wstart, last(v) from fill_values "
            "interval(1s) fill(prev) order by _wstart"
        )
        self.require_plan(sql, "fill")
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, self.large_a)
        tdSql.checkData(1, 1, self.large_a)
        tdSql.checkData(2, 1, self.large_b)
        print("fill large variable value .................... [ passed ]")

    def do_sort_and_group_large_key(self):
        sort_sql = (
            "select seq, length(v), substr(v, 1, 1) from boundary_st "
            "where seq < 2 order by v"
        )
        self.require_plan(sort_sql, "sort", "exchange")
        tdSql.query(sort_sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, self.VALUE_LEN)
        tdSql.checkData(0, 2, "a")
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 2, "b")

        group_sql = (
            "select length(v), substr(v, 1, 1), count(*) from boundary_st "
            "where seq < 2 group by v order by v"
        )
        self.require_plan(group_sql, "exchange")
        tdSql.query(group_sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, self.VALUE_LEN)
        tdSql.checkData(0, 1, "a")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 1, "b")
        tdSql.checkData(1, 2, 1)
        print("sort and group large key ..................... [ passed ]")

    def do_check_server_health(self):
        self.check_server_health()

    # --- main ---

    def test_executor_block_boundary_security(self):
        """Protect executor block boundaries through real SQL entry points.

        1. Pass 60 KB variable values through multi-vgroup exchange
        2. Expand string results with CONCAT before exchange
        3. Join with 60 KB keys and values
        4. Preserve 60 KB values across FILL state
        5. Sort and group by 60 KB keys
        6. Reject an oversized VARCHAR value at the SQL/RPC entry point
        7. Verify the server remains available

        Catalog:
            - Query:Executor

        Since: v3.4.0.0

        Labels: common,ci,integration,functional,asan,security

        Jira: None

        History:
            - 2026-09-17 Codex added executor block boundary regression coverage
            - 2026-09-29 Codex added oversized variable input negative coverage

        """
        self.do_prepare_data()
        self.do_reject_oversized_variable_input()
        self.do_exchange_large_value()
        self.do_concat_across_exchange()
        self.do_join_large_key_and_value()
        self.do_fill_large_value()
        self.do_sort_and_group_large_key()
        self.do_check_server_health()
