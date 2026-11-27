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
import pytest


class TestEmptySubqueryConstants:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def check_result(self, sql, expected):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(len(expected))
        if expected:
            tdSql.checkCols(len(expected[0]))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def explain_text(self, sql):
        tdSql.query(f"explain {sql}", queryTimes=1)
        return "\n".join(str(row[0]) for row in tdSql.queryResult)

    def check_empty_and_live_subqueries(self, empty_sql, live_sql):
        outer_values = [(1,), (2,), (None,)]
        # Each live control returns exactly one row whose value is 2.
        cases = (
            (empty_sql, True, [(None, False, True, False, True, False, False, True)] * 3),
            (live_sql, False, [
                (2, False, True, True, False, False, True, True),
                (2, True, False, True, False, True, False, False),
                (2, None, None, True, False, None, None, None),
            ]),
        )
        for subquery, is_empty, expected in cases:
            sql = (
                f"select ({subquery}) as sub_v, "
                f"v in ({subquery}), v not in ({subquery}), "
                f"exists ({subquery}), not exists ({subquery}), "
                f"v = any ({subquery}), v != some ({subquery}), v < all ({subquery}) "
                "from outer_rows"
            )
            self.check_result(f"{sql} order by ts", expected)
            self.check_result(
                f"select v from outer_rows where v in ({subquery}) order by ts",
                [] if is_empty else [(2,)],
            )
            self.check_result(
                f"select v from outer_rows where exists ({subquery}) order by ts",
                [] if is_empty else outer_values,
            )
            self.check_result(
                f"select v from outer_rows where ({subquery}) is null order by ts",
                outer_values if is_empty else [],
            )
            self.check_result(
                f"select count(*) from outer_rows having exists ({subquery})",
                [] if is_empty else [(3,)],
            )
            plan = self.explain_text(sql)
            assert "Scan on outer_rows" in plan, f"{sql}\n{plan}"
            if is_empty:
                assert "inner_rows" not in plan, f"{sql}\n{plan}"
                assert "InitPlan" not in plan, f"{sql}\n{plan}"
            else:
                assert "Scan on inner_rows" in plan, f"{sql}\n{plan}"

    def prepare_data(self):
        tdSql.execute("create table outer_rows (ts timestamp, v int)")
        tdSql.execute("create table inner_rows (ts timestamp, v int, label varchar(16))")
        tdSql.execute(
            "insert into outer_rows values "
            "('2026-10-13 00:00:01', 1) "
            "('2026-10-13 00:00:02', 2) "
            "('2026-10-13 00:00:03', null)"
        )
        tdSql.execute(
            "insert into inner_rows values "
            "('2026-10-13 00:00:01', 1, 'one') "
            "('2026-10-13 00:00:02', 2, 'two')"
        )

    def check_correlated_pair(self, empty, live, live_values):
        for subquery, values, is_empty in (
            (empty, [None] * 3, True),
            (live, live_values, False),
        ):
            sql = f"select r.v, ({subquery}) from outer_rows r order by r.ts"
            self.check_result(sql, list(zip((1, 2, None), values)))
            self.check_result(
                f"select r.v from outer_rows r where ({subquery}) is null order by r.ts",
                [(value,) for value, result in zip((1, 2, None), values) if result is None],
            )
            plan = self.explain_text(sql)
            assert "Scan on outer_rows" in plan, f"{sql}\n{plan}"
            if is_empty:
                assert "inner_rows" not in plan, f"{sql}\n{plan}"
                assert "Correlated Scalar Subquery" not in plan, f"{sql}\n{plan}"
                assert "InitPlan" not in plan, f"{sql}\n{plan}"
            else:
                assert "Scan on inner_rows" in plan, f"{sql}\n{plan}"
                assert "Correlated Scalar Subquery" in plan, f"{sql}\n{plan}"

    # --- impl ---
    def do_empty_expressions(self):
        for suffix in (
            "where 1 = 0",
            "limit 0",
            "where ts > '2026-10-13 00:00:02' and ts < '2026-10-13 00:00:01'",
        ):
            empty = f"select v from inner_rows {suffix}"
            self.check_result(
                f"select ({empty}) as empty_v, "
                f"v in ({empty}), v not in ({empty}), "
                f"exists ({empty}), not exists ({empty}), "
                f"v = any ({empty}), v != some ({empty}), v < all ({empty}) "
                "from outer_rows order by ts",
                [(None, False, True, False, True, False, False, True)] * 3,
            )
            self.check_result(
                f"select v from outer_rows where v in ({empty})", [],
            )
            self.check_result(
                f"select v from outer_rows where v not in ({empty}) order by ts",
                [(1,), (2,), (None,)],
            )
        self.check_result(
            "select ifnull((select v from inner_rows where false), 7), "
            "concat(ifnull((select label from inner_rows limit 0), 'empty'), '!'), "
            "(select ts from inner_rows limit 0) is null "
            "from outer_rows order by ts",
            [(7, 'empty!', True)] * 3,
        )
        print("empty scalar and predicate expressions .......... [ passed ]")

    def do_having_empty_subqueries(self):
        for empty_sql, live_sql in (
            (
                "select count(*) from inner_rows having 1 = 0",
                "select count(*) from inner_rows having 1 = 1",
            ),
            (
                "select v from inner_rows group by v having 2 < 1",
                "select v from inner_rows group by v having v = 2",
            ),
        ):
            self.check_empty_and_live_subqueries(empty_sql, live_sql)
        print("HAVING empties the subquery result ................ [ passed ]")

    def do_all_empty_union_subqueries(self):
        empty_branches = (
            "select v from inner_rows where 1 = 0",
            "select v from inner_rows where 2 < 1",
            "select v from inner_rows where null",
        )
        for operator in ("union all", "union"):
            for branch_count in (2, 3):
                separator = f" {operator} "
                branches = list(empty_branches[:branch_count])
                empty_sql = separator.join(branches)
                branches[-1] = "select v from inner_rows where v = 2"
                live_sql = separator.join(branches)
                self.check_empty_and_live_subqueries(empty_sql, live_sql)
        print("all UNION subquery branches are empty ............. [ passed ]")

    def do_outer_clauses(self):
        empty = "select v from inner_rows where 1 = 0"
        self.check_result(
            f"select v from outer_rows where ({empty}) is null order by ts",
            [(1,), (2,), (None,)],
        )
        self.check_result(
            f"select v from outer_rows where not (v < all ({empty}))", [],
        )
        self.check_result(
            f"select count(*) from outer_rows having exists ({empty})", [],
        )
        self.check_result(
            f"select count(*) from outer_rows having not exists ({empty})", [(3,)],
        )
        print("empty subqueries in where and having ............ [ passed ]")

    def do_remaining_subqueries(self):
        empty = "select v from inner_rows where 1 = 0"
        self.check_result(
            f"select v, ({empty}), "
            "(select v from inner_rows where v = 2), "
            f"v in ({empty}), v in (select v from inner_rows) "
            "from outer_rows where v is not null order by ts",
            [(1, None, 2, False, True), (2, None, 2, False, True)],
        )
        self.check_result(
            f"select (select v from inner_rows where v in ({empty})), "
            f"(select v from inner_rows where not exists ({empty}) and v = 2) "
            "from outer_rows order by ts",
            [(None, 2)] * 3,
        )
        self.check_result(
            f"select ({empty}), r.v, "
            "(select h.v from inner_rows h where h.v = r.v and h.v > 1) "
            "from outer_rows r order by r.ts",
            [(None, 1, None), (None, 2, 2), (None, None, None)],
        )
        self.check_result(
            "select r.v, (select h.v from inner_rows h "
            "where h.v = r.v and 1 = 0) from outer_rows r order by r.ts",
            [(1, None), (2, None), (None, None)],
        )
        print("remaining nested and correlated subqueries ....... [ passed ]")

    def do_correlated_empty_subqueries(self):
        source = "from inner_rows h where h.v = r.v"
        live = f"select h.v {source}"
        for suffix in (
            "and 1 = 0",
            "and (1 = 0 or 2 < 1)",
            "and not (1 = 1)",
            "and (1 = null)",
            "and (h.v > r.v and (1 = null))",
            "limit 0",
            "and h.ts > '2026-10-13 00:00:02' and h.ts < '2026-10-13 00:00:01'",
            "group by h.v having 1 = 0",
        ):
            self.check_correlated_pair(f"{live} {suffix}", live, [1, 2, None])
        self.check_correlated_pair(
            f"select count(*) {source} having 1 = 0",
            f"select count(*) {source} having 1 = 1",
            [1, 1, None],
        )
        self.check_correlated_pair(
            f"select count(*) {source} and 1 = 0 having 1 = 1",
            f"select count(*) {source} having 1 = 1",
            [1, 1, None],
        )
        for operator in ("union", "union all"):
            for branch_count in (2, 3):
                separator = f" {operator} "
                branches = [f"{live} and {condition}" for condition in ("1 = 0", "2 < 1", "1 = null")]
                empty = separator.join(branches[:branch_count])
                for live_first in (False, True):
                    controls = branches[:branch_count]
                    controls[0 if live_first else -1] = live
                    self.check_correlated_pair(empty, separator.join(controls), [1, 2, None])
        empty = f"select count(*) {source} having 1 = 0"
        sql = (
            f"select ({empty}), r.v, ({live}), "
            f"({live} and 1 = 0 union all {live} and 2 < 1), "
            "(select max(v) from inner_rows) from outer_rows r order by r.ts"
        )
        self.check_result(sql, [(None, 1, 1, None, 2), (None, 2, 2, None, 2), (None, None, None, None, 2)])
        plan = self.explain_text(sql)
        assert plan.count("Correlated Scalar Subquery") == 1, plan
        assert plan.count("InitPlan") == 1, plan
        for subquery, expected in (
            ("select h.v from inner_rows h where not (h.v = r.v and (1 = null))", [2, 1, None]),
            (f"{live} or 1 = 0", [1, 2, None]),
            (f"{live} and (1 = 1 or h.v > r.v)", [1, 2, None]),
            (f"select h.v from inner_rows h where (h.v = r.v and 1 = 0) or h.v = 2", [2, 2, 2]),
            (f"{live} group by h.v having h.v > 1", [None, 2, None]),
            (f"select count(*) {source} having count(*) > 0", [1, 1, None]),
            (f"select count(*) {source}", [1, 1, 0]),
            (f"select avg(h.v) {source}", [1, 2, None]),
        ):
            sql = f"select r.v, ({subquery}) from outer_rows r order by r.ts"
            self.check_result(sql, list(zip((1, 2, None), expected)))
            plan = self.explain_text(sql)
            assert "Correlated Scalar Subquery" in plan, f"{sql}\n{plan}"
            assert "Scan on inner_rows" in plan, f"{sql}\n{plan}"
        tdSql.error(
            f"select ({live} union all {live}) from outer_rows r",
            expectedErrno=0x26A5,
        )
        print("correlated constant and runtime empty results ...... [ passed ]")

    def do_unsupported_correlated_predicates(self):
        source = "select h.v from inner_rows h where h.v = r.v"
        for subquery in (
            f"{source} and 1 = 0",
            f"{source} limit 0",
            f"{source} group by h.v having 1 = 0",
            f"{source} and 1 = 0 union all {source} and 2 < 1",
        ):
            for predicate in (
                f"r.v in ({subquery})", f"r.v not in ({subquery})",
                f"exists ({subquery})", f"not exists ({subquery})",
                f"r.v = any ({subquery})", f"r.v != some ({subquery})",
                f"r.v < all ({subquery})",
            ):
                tdSql.error(
                    f"select r.v from outer_rows r where {predicate}",
                    expectedErrno=0x26A6,
                )
        print("constant empty non-scalar correlation is rejected .. [ passed ]")

    def do_quantified_comparison_matrix(self):
        operators = (
            ("=", lambda left, right: left == right),
            ("!=", lambda left, right: left != right),
            ("<", lambda left, right: left < right),
            ("<=", lambda left, right: left <= right),
            (">", lambda left, right: left > right),
            (">=", lambda left, right: left >= right),
        )
        for subquery, is_empty in (
            ("select v from inner_rows where 1 = 0", True),
            ("select v from inner_rows limit 0", True),
            ("select v from inner_rows group by v having 1 = 0", True),
            ("select v from inner_rows where 1 = 0 union select v from inner_rows where null", True),
            ("select v from inner_rows where 1 = 0 union all select v from inner_rows where null", True),
            ("select v from inner_rows", False),
        ):
            expressions = []
            expected = [[], [], []]
            for operator, compare in operators:
                for quantifier in ("any", "some", "all"):
                    expressions.append(f"v {operator} {quantifier} ({subquery})")
                    for index, value in enumerate((1, 2, None)):
                        if is_empty:
                            result = quantifier == "all"
                        elif value is None:
                            result = None
                        else:
                            quantify = all if quantifier == "all" else any
                            result = quantify(compare(value, item) for item in (1, 2))
                        expected[index].append(result)
            self.check_result(
                f"select {', '.join(expressions)} from outer_rows order by ts", expected,
            )
        null_row = "select cast(null as int) from inner_rows limit 1"
        self.check_result(
            f"select ({null_row}), exists ({null_row}), v in ({null_row}), "
            f"v not in ({null_row}), v = any ({null_row}), v < all ({null_row}) "
            "from outer_rows order by ts",
            [(None, True, None, None, None, None)] * 3,
        )
        tdSql.error(
            "select (select v from inner_rows where 1 = 0), "
            "(select v from inner_rows) from outer_rows",
            expectedErrno=0x26A5,
        )
        print("quantified operators and NULL row versus zero rows . [ passed ]")

    def do_empty_derived_tables(self):
        live = "select ts, v from inner_rows where v = 2"
        for empty in (
            "select ts, v from inner_rows where 1 = 0",
            "select ts, v from inner_rows limit 0",
            "select ts, v from inner_rows group by ts, v having 1 = 0",
            "select ts, v from inner_rows where 1 = 0 union all select ts, v from inner_rows where null",
            "select ts, v from inner_rows where 1 = 0 union select ts, v from inner_rows where null",
        ):
            self.check_result(f"select d.v from ({empty}) d", [])
            self.check_result(
                f"select (select d.v from ({empty}) d), "
                f"exists (select d.v from ({empty}) d) from outer_rows order by ts",
                [(None, False)] * 3,
            )
            for subquery, is_empty in ((empty, True), (live, False)):
                for join_type in ("inner", "left", "right", "full"):
                    if join_type in ("left", "full"):
                        expected = [(1, None), (2, None if is_empty else 2), (None, None)]
                    else:
                        expected = [] if is_empty else [(2, 2)]
                    self.check_result(
                        f"select r.v, d.v from outer_rows r {join_type} join ({subquery}) d "
                        "on r.ts = d.ts order by r.ts",
                        expected,
                    )
        print("empty derived tables preserve JOIN semantics ....... [ passed ]")

    def do_scalar_expression_positions(self):
        for source in (
            "from inner_rows where v = 2",
            "from inner_rows h where h.v = r.v",
        ):
            live = f"select v {source}"
            correlated = "h.v = r.v" in source
            for empty in (
                f"select count(*) {source} having 1 = 0",
                f"{live} and 1 = 0 union all {live} and 2 < 1",
            ):
                for subquery, is_empty in ((empty, True), (live, False)):
                    values = [None] * 3 if is_empty else ([1, 2, None] if correlated else [2] * 3)
                    self.check_result(
                        f"select ifnull(({subquery}), 7) + 1, "
                        f"case when ({subquery}) is null then 'empty' else 'live' end "
                        "from outer_rows r order by r.ts",
                        [(8 if value is None else value + 1, "empty" if value is None else "live")
                         for value in values],
                    )
                    order = [(None,), (1,), (2,)] if correlated and not is_empty else [(1,), (2,), (None,)]
                    self.check_result(
                        f"select r.v from outer_rows r order by ifnull(({subquery}), 0), r.ts", order,
                    )
                    for clause in ("group by", "partition by"):
                        self.check_result(
                            f"select count(*) from outer_rows r {clause} ({subquery}) order by count(*) desc",
                            [(1,)] * 3 if correlated and not is_empty else [(3,)],
                        )
                    self.check_result(
                        "select r.v, i.v from outer_rows r left join inner_rows i "
                        f"on r.ts = i.ts and ({subquery}) is null order by r.ts",
                        [(1, 1 if is_empty else None), (2, 2 if is_empty else None), (None, None)],
                    )
        print("empty scalars across SQL expression positions ...... [ passed ]")

    def do_correlated_sort_boundaries(self):
        tdSql.execute("create table sort_rows (ts timestamp, device varchar(16))")
        tdSql.execute(
            "insert into sort_rows values "
            "('2026-10-13 00:00:01', 'one') "
            "('2026-10-13 00:00:02', 'one') "
            "('2026-10-13 00:00:03', 'two') "
            "('2026-10-13 00:00:04', 'missing')"
        )
        for projection, expected in (
            ("max(h.v)", [("missing", None), ("one", 1), ("two", 2)]),
            ("max(h.label)", [("missing", None), ("one", "one"), ("two", "two")]),
        ):
            self.check_result(
                f"select distinct r.device, (select {projection} from inner_rows h "
                "where h.label = r.device) as result from sort_rows r order by r.device",
                expected,
            )
        self.check_result(
            "select distinct r.device, (select h.label from inner_rows h "
            "where h.label = r.device and 1 = 0) as result "
            "from sort_rows r order by r.device",
            [("missing", None), ("one", None), ("two", None)],
        )
        print("DISTINCT correlated results preserve empty sort slots [ passed ]")

    def do_aggregate_controls(self):
        count = "select count(*) from inner_rows where 1 = 0"
        self.check_result(
            f"select ({count}), exists ({count}), 0 in ({count}), 0 = all ({count}) "
            "from outer_rows order by ts",
            [(0, True, True, True)] * 3,
        )
        self.check_result(
            "select (select count(*) from inner_rows limit 0), "
            "exists (select count(*) from inner_rows limit 0), "
            "(select count(*) from inner_rows where 1 = 0 group by v), "
            "exists (select count(*) from inner_rows where 1 = 0 group by v) "
            "from outer_rows order by ts",
            [(None, False, None, False)] * 3,
        )
        print("empty aggregate input and empty output ........... [ passed ]")

    def do_union_branches(self):
        empty = "select v from inner_rows where 1 = 0"
        empty_branch = (
            "select (select min(v) from inner_rows), v "
            "from outer_rows where 1 = 0"
        )
        live_branch = (
            f"select ({empty}), (select max(v) from inner_rows) from outer_rows"
        )
        for sql in (
            f"{empty_branch} union all {live_branch}",
            f"{live_branch} union all {empty_branch}",
        ):
            self.check_result(sql, [(None, 2)] * 3)
        empty_first = f"{empty} union all select v from inner_rows"
        empty_last = f"select v from inner_rows union all {empty}"
        self.check_result(
            f"select v in ({empty_first}), v not in ({empty_last}), "
            f"v + 3 in ({empty_first}), v + 3 not in ({empty_last}) "
            "from outer_rows where v is not null order by ts",
            [(True, False, False, True)] * 2,
        )
        print("union branches keep surviving subqueries .......... [ passed ]")

    def do_eliminated_plans(self):
        live_plan = self.explain_text(
            "select v, (select max(v) from inner_rows) from outer_rows"
        )
        assert "Scan on inner_rows" in live_plan, live_plan
        empty = "select v from inner_rows where 1 = 0"
        empty_plan = self.explain_text(
            f"select ({empty}), v in ({empty}), v not in ({empty}), "
            f"exists ({empty}), not exists ({empty}), "
            f"v = any ({empty}), v < all ({empty}) from outer_rows"
        )
        assert "Scan on outer_rows" in empty_plan, empty_plan
        assert "inner_rows" not in empty_plan, empty_plan
        assert "InitPlan" not in empty_plan, empty_plan
        print("empty expression subquery plans eliminated ......... [ passed ]")

    def do_external_window_boundaries(self):
        tdSql.execute("create table window_bounds (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into window_bounds values "
            "('2026-10-13 00:00:00', '2026-10-13 00:00:03')"
        )
        for suffix in ("where 1 = 0", "limit 0"):
            empty_window = (
                "select count(*) from inner_rows external_window("
                f"(select ts, end_ts from window_bounds {suffix}) w)"
            )
            self.check_result(empty_window, [])
            self.check_result(
                f"select ({empty_window}), exists ({empty_window}) "
                "from outer_rows order by ts",
                [(None, False)] * 3,
            )
        live_window = (
            "select count(*) from inner_rows external_window("
            "(select ts, end_ts from window_bounds) w)"
        )
        self.check_result(live_window, [(2,)])
        self.check_result(
            f"select ({live_window}), exists ({live_window}) "
            "from outer_rows order by ts",
            [(2, True)] * 3,
        )
        empty_plan = self.explain_text(
            "select (select count(*) from inner_rows external_window("
            "(select ts, end_ts from window_bounds where 1 = 0) w)) "
            "from outer_rows"
        )
        assert "Scan on outer_rows" in empty_plan, empty_plan
        assert "inner_rows" not in empty_plan, empty_plan
        assert "window_bounds" not in empty_plan, empty_plan
        assert "InitPlan" not in empty_plan, empty_plan
        print("empty external window boundaries .................. [ passed ]")

    def do_empty_subquery_constants(self, scenario):
        database = f"db_empty_subquery_{scenario}"
        tdSql.execute(f"drop database if exists {database}")
        tdSql.execute(f"create database {database}")
        try:
            tdSql.execute(f"use {database}")
            self.prepare_data()
            if scenario == "baseline":
                self.do_empty_expressions()
                self.do_having_empty_subqueries()
                self.do_all_empty_union_subqueries()
                self.do_outer_clauses()
                self.do_remaining_subqueries()
                self.do_aggregate_controls()
                self.do_union_branches()
                self.do_eliminated_plans()
                self.do_external_window_boundaries()
            else:
                getattr(self, f"do_{scenario}")()
        finally:
            tdSql.execute(f"drop database if exists {database}")
        print("empty subquery constants ......................... [ passed ]")

    # --- main ---
    @pytest.mark.parametrize("scenario", (
        "baseline", "correlated_empty_subqueries", "unsupported_correlated_predicates",
        "quantified_comparison_matrix", "empty_derived_tables", "scalar_expression_positions",
        "correlated_sort_boundaries",
    ))
    def test_empty_subquery_constants(self, scenario):
        """Preserve results when constant folding eliminates empty subqueries

        1. Check scalar, IN, EXISTS and quantified expressions with empty results.
        2. Check outer WHERE/HAVING folding, surviving siblings and nested queries.
        3. Preserve correlated parameter binding and COUNT over empty input.
        4. Preserve surviving UNION ALL branches and check eliminated plans.
        5. Distinguish zero external window boundaries from a populated window.
        6. Cover subqueries emptied by HAVING or all UNION/UNION ALL branches.
        7. Check live controls, outer filters and elimination of their subquery plans.
        8. Cover correlated constant/runtime emptiness and surviving templates.
        9. Reject unsupported correlation even when its result is constant empty.
        10. Cover quantified operators, derived JOINs and scalar expression positions.
        11. Preserve unmaterialized sort slots in DISTINCT correlated queries.

        Catalog:
            - Query

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-13 Simon Guan Created for 6638636215
            - 2026-10-26 Jing Sima Cover empty HAVING and all-empty UNION subqueries
            - 2026-10-26 Jing Sima Extend correlation, comparison and expression coverage

        """
        self.do_empty_subquery_constants(scenario)
