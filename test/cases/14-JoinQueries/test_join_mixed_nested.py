from new_test_framework.utils import tdLog, tdSql

from collections import Counter
import re

import pytest


class TestJoinMixedNested:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdSql.execute("drop database if exists test_join_mixed_nested")
        tdSql.execute("create database test_join_mixed_nested vgroups 2 keep 36500")
        tdSql.execute("use test_join_mixed_nested")
        for table in ("a", "b", "c", "d"):
            tdSql.execute(
                f"create table {table} (ts timestamp, tk timestamp, v int)"
            )
        tdSql.execute(
            "insert into a values (1000, NULL, 0) (2000, 10000, 1) "
            "(3000, 20000, 2)"
        )
        tdSql.execute(
            "insert into b values (1000, NULL, 0) (2000, 12000, 1) "
            "(3000, 22000, 2)"
        )
        tdSql.execute(
            "insert into c values (1000, NULL, 0) (2000, 9000, 9) "
            "(3000, 19000, 19)"
        )

        tdSql.execute("insert into d select * from c")

    @pytest.fixture
    def join_regression_data(self):
        database = "join_mixed_nested_regression"
        tdSql.execute(f"drop database if exists {database}")
        try:
            tdSql.execute(
                f"create database {database} vgroups 1 keep 36500"
            )
            tdSql.execute(f"use {database}")
            for table in ("a", "b", "c", "d"):
                tdSql.execute(
                    f"create table {table} "
                    "(ts timestamp, tk timestamp, v int)"
                )
                values = (0, 9, 19) if table == "c" else (0, 1, 2)
                tdSql.execute(
                    f"insert into {table} values "
                    f"(1000,10000,{values[0]}) "
                    f"(2000,11000,{values[1]}) "
                    f"(3000,12000,{values[2]})"
                )
            yield
        finally:
            tdSql.execute(f"drop database if exists {database}")

    def check_rows(self, sql, expected):
        tdSql.query(sql)
        assert sorted(tdSql.queryResult) == sorted(expected), (
            sql, tdSql.queryResult, expected
        )

    def explain_time_keys(self, sql, verbose=True):
        tdSql.query(
            f"explain verbose {'true' if verbose else 'false'} {sql}"
        )
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        keys = {}
        for line in plan.splitlines():
            for side in ("Left", "Right"):
                marker = f"{side} Time Key: "
                if marker not in line:
                    continue
                assert side not in keys, plan
                keys[side] = line.split(marker, 1)[1]
        return keys, plan

    def do_default_window(self):
        self.check_rows(
            "select a.v, c.v from a left window join "
            "(b join c on b.ts = c.ts) window_offset(0s, 0s)",
            [(0, 0), (1, 9), (2, 19)],
        )
        self.check_rows(
            "select a.v, c.v from (a join b on a.ts = b.ts) "
            "left window join c window_offset(0s, 0s)",
            [(0, 0), (1, 9), (2, 19)],
        )
        self.check_rows(
            "select a.v, c.v from (a left asof join b on a.ts >= b.ts) "
            "left window join c window_offset(0s, 0s)",
            [(0, 0), (1, 9), (2, 19)],
        )
        print("nested default WINDOW ..................... [ passed ]")

    def do_explicit_time_and_null(self):
        for join in ("join", "left join", "left semi join"):
            self.check_rows(
                f"select a.v, c.v from (a {join} b on a.ts = b.ts) "
                "left asof join (c join d on c.ts = d.ts) on a.tk >= c.tk",
                [(1, 9), (2, 19)],
            )
        self.check_rows(
            "select a.v, c.v from (a join b on a.ts = b.ts) "
            "left window join (c join d on c.ts = d.ts) "
            "window_time(a.tk, c.tk) "
            "window_offset(-1s, 0s)",
            [(1, 9), (2, 19)],
        )
        self.check_rows(
            "select a.v, c.v from (a join b on a.ts = b.ts) "
            "left window join (c join d on c.ts = d.ts) "
            "window_time(a.tk, c.tk) "
            "window_offset(-1s, 0s) jlimit 1",
            [(1, 9), (2, 19)],
        )
        self.check_rows(
            "select a.v, c.v from (c join d on c.ts = d.ts) "
            "right asof join "
            "(a join b on a.ts = b.ts) on c.tk <= a.tk",
            [(1, 9), (2, 19)],
        )
        self.check_rows(
            "select a.v, c.v from (c join d on c.ts = d.ts) "
            "right window join (a join b on a.ts = b.ts) "
            "window_time(c.tk, a.tk) window_offset(-1s, 0s)",
            [(1, 9), (2, 19)],
        )
        print("nested explicit time and NULL ............. [ passed ]")

    def do_full_time_keys(self):
        tdSql.execute("create table f1 (ts timestamp, v int)")
        tdSql.execute("create table f2 (ts timestamp, v int)")
        tdSql.execute("create table f3 (ts timestamp, v int)")
        tdSql.execute("insert into f1 values (1000, 1) (3000, 3)")
        tdSql.execute("insert into f2 values (2000, 2) (3000, 30)")
        tdSql.execute("insert into f3 values (500, 5) (1500, 15) (2500, 25)")
        cases = (
            ("f1.ts", [(1, 5), (3, 25)],
             [(5, None), (15, 1), (25, 1)],
             [(5, None), (15, 1), (25, None)]),
            ("f2.ts", [(2, 15), (3, 25)],
             [(5, None), (15, None), (25, 2)],
             [(5, None), (15, None), (25, 2)]),
            ("coalesce(f1.ts, f1.ts)", [(1, 5), (3, 25)],
             [(5, None), (15, 1), (25, 1)],
             [(5, None), (15, 1), (25, None)]),
            ("coalesce(f1.ts, f2.ts)", [(1, 5), (2, 15), (3, 25)],
             [(5, None), (15, 1), (25, 2)],
             [(5, None), (15, 1), (25, 2)]),
        )
        for expr, driving_rows, asof_rows, window_rows in cases:
            source = (
                f"(select {expr} event_ts, coalesce(f1.v, f2.v) k "
                "from f1 full join f2 on f1.ts = f2.ts) q"
            )
            for side in ("left", "right"):
                for full_drives in (True, False):
                    full_on_left = (side == "left") == full_drives
                    left, right = (
                        (source, "f3") if full_on_left else ("f3", source)
                    )
                    left_key, right_key = (
                        ("q.event_ts", "f3.ts") if full_on_left else
                        ("f3.ts", "q.event_ts")
                    )
                    columns = "q.k, f3.v" if full_drives else "f3.v, q.k"
                    op = ">=" if side == "left" else "<="
                    for kind, suffix, matched_rows in (
                        ("asof", f" on {left_key} {op} {right_key}",
                         asof_rows),
                        ("window", f" window_time({left_key}, {right_key})"
                         " window_offset(-1s, 0s)", window_rows),
                    ):
                        self.check_rows(
                            f"select {columns} from {left} {side} {kind} "
                            f"join {right}{suffix}",
                            driving_rows if full_drives else matched_rows,
                        )
                    if full_drives:
                        self.check_rows(
                            f"select {columns} from {left} {side} window "
                            f"join {right} "
                            f"window_time({left_key}, {right_key}) "
                            "window_offset(0s, 0s)",
                            [(key, None) for key, _ in driving_rows],
                        )
        source = "(f1 full join f2 on f1.ts = f2.ts)"
        for side in ("left", "right"):
            left, right = (
                (source, "f3") if side == "left" else ("f3", source)
            )
            for kind, suffix in (
                ("asof", ""),
                ("window", " window_offset(-1s, 0s)"),
            ):
                self.check_rows(
                    f"select coalesce(f1.v, f2.v), f3.v from {left} "
                    f"{side} {kind} join {right}{suffix}",
                    [(1, 5), (3, 25)],
                )
        print("FULL OUTER time keys and NULL .............. [ passed ]")

    def do_subquery_time_selection(self):
        primary_rows = [(0, 0), (1, 9), (2, 19)]
        inputs = (
            ("select now() anchor, * from a", "ts",
             primary_rows, primary_rows),
            ("select tk, ts, v from a order by v desc limit 2", "ts",
             [(1, 9), (2, 19)], [(1, 9), (2, 19)]),
            ("select anchor, time_key, v from "
             "(select now() anchor, ts time_key, v from a) s "
             "order by v desc", "time_key", primary_rows, primary_rows),
            ("select tk, ts first_key, ts second_key, v from a",
             "first_key", primary_rows, primary_rows),
            ("select tk fallback, ts + 1s shifted, v from a", "shifted",
             [(0, 9), (1, 19), (2, 19)],
             [(0, 9), (1, 19), (2, None)]),
            ("select tk, event_ts, v from "
             "(select tk, ts + 1s event_ts, v from a) s", "event_ts",
             [(0, 9), (1, 19), (2, 19)], [(0, 9), (1, 19), (2, None)]),
            ("select tk, event_ts, v from "
             "(select tk, ts + 1s event_ts, v from a "
             "order by ts limit 2) s", "event_ts",
             [(0, 9), (1, 19)], [(0, 9), (1, 19)]),
            ("select tk + 1s fallback, tk other_time, v from a",
             "fallback", [(1, 19), (2, 19)], [(1, None), (2, None)]),
            ("select tk fallback, first(ts) time_key, v from a",
             "time_key", [(0, 0)], [(0, 0)]),
            ("select tk fallback, last(ts) time_key, v from a",
             "time_key", [(2, 19)], [(2, 19)]),
            ("select tk fallback, v from a", "fallback",
             [(1, 19), (2, 19)], [(1, None), (2, None)]),
            ("select cast(2000 as timestamp) anchor, tk, v from a",
             "anchor", [(0, 9), (1, 9), (2, 9)],
             [(0, 9), (1, 9), (2, 9)]),
            ("select now() anchor, tk, v from a", "anchor",
             [(0, 19), (1, 19), (2, 19)],
             [(0, None), (1, None), (2, None)]),
            ("select cast(null as timestamp) anchor, tk, v from a",
             "anchor", [], []),
        )
        peer = "(c join d on c.ts = d.ts)"
        for source, key, asof_rows, window_rows in inputs:
            for side, key_side in (("left", "Left"), ("right", "Right")):
                left, right = (
                    (f"({source}) q", peer) if side == "left" else
                    (peer, f"({source}) q")
                )
                for kind, suffix, expected in (
                    ("asof", "", asof_rows),
                    ("window", " window_offset(0s, 0s)", window_rows),
                ):
                    sql = (f"select q.v, c.v from {left} "
                           f"{side} {kind} join {right}{suffix}")
                    self.check_rows(sql, expected)
                    keys, plan = self.explain_time_keys(sql)
                    assert f"`q`.`{key}` Order:" in keys[key_side], plan
        for side in ("left", "right"):
            subquery = "(select tk, ts, ts + 1s shifted, v from a) q"
            left, right = (
                (subquery, peer) if side == "left" else (peer, subquery)
            )
            condition = (
                "q.shifted >= c.ts" if side == "left" else
                "c.ts <= q.shifted"
            )
            self.check_rows(
                f"select q.v, c.v from {left} {side} asof join {right} "
                f"on {condition}", [(0, 9), (1, 19), (2, 19)],
            )
            keys = "q.tk, c.tk" if side == "left" else "c.tk, q.tk"
            self.check_rows(
                f"select q.v, c.v from {left} {side} window join {right} "
                f"window_time({keys}) window_offset(-1s, 0s)",
                [(1, 9), (2, 19)],
            )
        print("subquery time selection ................... [ passed ]")

    def do_join_input_primary_time(self):
        inputs = (
            ("((select tk, ts, v from a) q join b on q.v = b.v)", False),
            ("(select a.tk, a.ts, a.v from a "
             "join b on a.v = b.v) q", False),
            ("(select cast(2000 as timestamp) anchor, a.ts, a.v from a "
             "join b on a.v = b.v) q", False),
            ("(select a.tk, a.ts, a.v from a join b on a.v = b.v "
             "order by a.v desc limit 2) q", True),
        )
        for source, limited in inputs:
            for on_left in (True, False):
                left, right = (source, "c") if on_left else ("c", source)
                key_side = "Left" if on_left else "Right"
                for direction in ("left", "right"):
                    expected = [(1, 9), (2, 19)]
                    if not limited:
                        expected = [(0, 0)] + expected
                    elif (direction == "left") != on_left:
                        expected = [(None, 0)] + expected
                    for kind in ("asof", "window"):
                        suffix = (
                            " window_offset(0s, 0s)" if kind == "window"
                            else ""
                        )
                        sql = (f"select q.v, c.v from {left} {direction} "
                               f"{kind} join {right}{suffix}")
                        tdSql.query(sql)
                        actual = Counter(tdSql.queryResult)
                        assert actual == Counter(expected), (
                            sql, tdSql.queryResult, expected
                        )
                        keys, plan = self.explain_time_keys(sql)
                        assert "`q`.`ts` Order:" in keys[key_side], plan
                        assert "algo=Hash" in plan, plan
                        assert "Sort" in plan, plan
        print("unordered JOIN primary time ............... [ passed ]")

    def do_expression_time_depth(self):
        cases = (
            ("a.ts + 1s", (
                [(0, 9), (1, 19), (2, 19)],
                [(None, 0), (0, 9), (1, 19)],
                [(0, 9), (1, 19), (2, None)],
                [(None, 0), (0, 9), (1, 19)],
            )),
            ("a.ts - 1s", (
                [(0, None), (1, 0), (2, 9)],
                [(1, 0), (2, 9), (2, 19)],
                [(0, None), (1, 0), (2, 9)],
                [(1, 0), (2, 9), (None, 19)],
            )),
            ("timetruncate(a.ts, 1s)", (
                [(0, 0), (1, 9), (2, 19)],
            ) * 4),
        )
        joins = ("left asof", "right asof", "left window", "right window")
        for expr, results in cases:
            for join_source in (False, True):
                source = f"select a.tk anchor, {expr} time_key, a.v from a"
                if join_source:
                    source += " join b on a.v = b.v"
                for depth in range(3):
                    for peer in ("c", "(c join d on c.v = d.v)"):
                        for join, expected in zip(joins, results):
                            suffix = (
                                " window_offset(0s, 0s)" if "window" in join
                                else ""
                            )
                            sql = (f"select q.v, c.v from ({source}) q "
                                   f"{join} join {peer}{suffix}")
                            tdSql.query(sql)
                            actual = Counter(tdSql.queryResult)
                            assert actual == Counter(expected), (
                                depth, sql, tdSql.queryResult, expected
                            )
                            keys, plan = self.explain_time_keys(sql)
                            assert "`q`.`time_key` Order:" in keys["Left"], plan
                    source = f"select anchor, time_key, v from ({source}) s"
        print("expression time projection depth .......... [ passed ]")

    def do_input_boundaries(self):
        self.check_rows(
            "select q.v, c.v from (select tk, ts, v from a) q "
            "left asof join (c join d on c.ts = d.ts)",
            [(0, 0), (1, 9), (2, 19)],
        )
        for suffix, expected in (
            ("", [(0, 0), (1, 9), (2, 19)]),
            (" limit 2", [(0, 0), (1, 9)]),
        ):
            self.check_rows(
                "select q.v, c.v from "
                "(select tk event_ts, ts, v from a order by ts" + suffix +
                ") q left asof join (c join d on c.ts = d.ts)",
                expected,
            )
        self.check_rows(
            "select a.v, c.v from "
            "(a left join b on a.ts = b.ts and b.v = 1) "
            "left asof join (c join d on c.ts = d.ts) on a.tk >= c.tk",
            [(1, 9), (2, 19)],
        )
        tdSql.execute("create table e (ts timestamp, tk timestamp, v int)")
        self.check_rows(
            "select a.v, c.v from (a join e on a.ts = e.ts) "
            "left asof join (c join d on c.ts = d.ts) on a.tk >= c.tk",
            [],
        )
        self.check_rows(
            "select a.v, c.v from (a join b on a.ts = b.ts) "
            "left asof join (c join e on c.ts = e.ts) on a.tk >= c.tk",
            [(1, None), (2, None)],
        )
        self.check_rows(
            "select a.v, c.v from (a join b on a.ts = b.ts) "
            "left window join (c join e on c.ts = e.ts) "
            "window_time(a.tk, c.tk) window_offset(-1s, 0s)",
            [(1, None), (2, None)],
        )
        tdSql.execute("insert into e values (4000, 12000, 1) (5000, 12000, 1)")
        self.check_rows(
            "select a.v, c.v from (a join e on a.v = e.v) "
            "left asof join (c join d on c.ts = d.ts) on a.tk >= c.tk",
            [(1, 9), (1, 9)],
        )
        self.check_rows(
            "select a.v, c.v from (a join e on a.v = e.v) "
            "left window join (c join d on c.ts = d.ts) "
            "window_time(a.tk, c.tk) window_offset(-1s, 0s)",
            [(1, 9), (1, 9)],
        )
        print("temporal input boundaries ................. [ passed ]")

    def do_explain(self):
        database = "`test_join_mixed_nested`"
        sql = (
            "select a.v, c.v from a left asof join c "
            "on a.ts >= c.ts"
        )
        keys, plan = self.explain_time_keys(sql)
        assert keys == {
            "Left": f"{database}.`a`.`ts` Order: asc",
            "Right": f"{database}.`c`.`ts` Order: asc",
        }, plan
        keys, plan = self.explain_time_keys(
            "select a.v, c.v from "
            "(a join b on a.ts = b.ts) left asof join "
            "(c join d on c.ts = d.ts) on a.tk >= c.tk"
        )
        assert keys == {
            "Left": f"{database}.`a`.`tk` Order: asc",
            "Right": f"{database}.`c`.`tk` Order: asc",
        }, plan
        assert "IS NOT NULL" in plan and "Sort" in plan, plan

        keys, plan = self.explain_time_keys(
            "select a.v, c.v from a left window join c "
            "window_time(a.tk, c.ts) window_offset(-1s, 0s)"
        )
        assert keys == {
            "Left": f"{database}.`a`.`tk` Order: asc",
            "Right": f"{database}.`c`.`ts` Order: asc",
        }, plan

        keys, plan = self.explain_time_keys(
            "select a_desc.v, c_desc.v from "
            "(select ts, v from a order by ts desc) a_desc "
            "left asof join "
            "(select ts, v from c order by ts desc) c_desc "
            "on a_desc.ts >= c_desc.ts"
        )
        assert keys == {
            "Left": "`a_desc`.`ts` Order: desc",
            "Right": "`c_desc`.`ts` Order: desc",
        }, plan

        alias = "a" * 180
        keys, plan = self.explain_time_keys(
            "select " + alias + ".v from a " + alias +
            " left asof join c on timetruncate(" + alias +
            ".ts, 1s) >= c.ts"
        )
        assert keys["Left"].startswith(
            "timetruncate(" + database + ".`" + alias + "`.`ts`, "
        ), plan
        assert keys["Left"].endswith(") Order: asc"), plan
        assert keys["Right"] == f"{database}.`c`.`ts` Order: asc", plan

        keys, plan = self.explain_time_keys(sql, verbose=False)
        assert keys == {}, plan
        print("temporal EXPLAIN keys ...................... [ passed ]")

    def build_ten_level_join(self):
        q1 = (
            "select j0.ts event_ts, j0.v v "
            "from j0 join j1 on j0.ts = j1.ts"
        )
        q2 = (
            "select q1.event_ts, q1.v "
            f"from ({q1}) q1 left join j2 on q1.event_ts = j2.ts"
        )
        q3 = (
            "select j3.ts event_ts, coalesce(q2.v, j3.v) v "
            f"from ({q2}) q2 right join j3 on q2.event_ts = j3.ts"
        )
        q4 = (
            "select coalesce(q3.event_ts, j4.ts) event_ts, "
            "coalesce(q3.v, j4.v) v "
            f"from ({q3}) q3 full join j4 on q3.event_ts = j4.ts"
        )
        q5 = (
            "select q4.event_ts, q4.v "
            f"from ({q4}) q4 left semi join j5 "
            "on q4.event_ts = j5.ts"
        )
        q6 = (
            "select q5.event_ts, q5.v "
            f"from ({q5}) q5 left anti join j6 "
            "on q5.event_ts = j6.ts"
        )
        q7 = (
            "select q6.event_ts, q6.v "
            f"from ({q6}) q6 left asof join j7 "
            "on q6.event_ts >= j7.ts"
        )
        q8 = (
            "select q7.event_ts, q7.v "
            f"from ({q7}) q7 left window join j8 "
            "window_time(q7.event_ts, j8.ts) window_offset(0s, 0s)"
        )
        q9 = (
            "select j9.ts event_ts, coalesce(q8.v, j9.v) v "
            f"from ({q8}) q8 right asof join j9 "
            "on q8.event_ts <= j9.ts"
        )
        q10 = (
            "select j10.ts event_ts, coalesce(q9.v, j10.v) v "
            f"from ({q9}) q9 right window join j10 "
            "window_time(q9.event_ts, j10.ts) window_offset(0s, 0s)"
        )
        return f"select event_ts, v from ({q10}) q10"

    def do_ten_level_explain(self):
        for index in range(11):
            tdSql.execute(f"create table j{index} (ts timestamp, v int)")
            timestamp = 2000 if index == 6 else 1000
            tdSql.execute(
                f"insert into j{index} values ({timestamp}, {index})"
            )

        sql = self.build_ten_level_join()
        tdSql.query("explain verbose true " + sql)
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        join_lines = [
            line for line in plan.splitlines() if " Join (" in line
        ]
        join_nodes = [
            line.strip().removeprefix("-> ").split(" (", 1)[0]
            for line in join_lines
        ]
        assert join_nodes == [
            "Right Window Join",
            "Right ASOF Join",
            "Left Window Join",
            "Left ASOF Join",
            "Left Anti Join",
            "Left Semi Join",
            "Full Join",
            "Right Join",
            "Left Join",
            "Inner Join",
        ], plan
        assert plan.count("algo=Merge") == 8, plan
        assert plan.count("algo=Hash") == 2, plan

        time_keys = [
            line.strip()
            for line in plan.splitlines()
            if " Time Key:" in line
        ]
        database = "`test_join_mixed_nested`"
        assert time_keys == [
            "Left Time Key: `q9`.`event_ts` Order: asc",
            f"Right Time Key: {database}.`j10`.`ts` Order: asc",
            "Left Time Key: `q8`.`event_ts` Order: asc",
            f"Right Time Key: {database}.`j9`.`ts` Order: asc",
            "Left Time Key: `q7`.`event_ts` Order: asc",
            f"Right Time Key: {database}.`j8`.`ts` Order: asc",
            "Left Time Key: `q6`.`event_ts` Order: asc",
            f"Right Time Key: {database}.`j7`.`ts` Order: asc",
        ], plan
        sort_count = sum(
            "-> Sort " in line for line in plan.splitlines()
        )
        assert sort_count == 4, plan

        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        print("ten-level mixed JOIN plan .................. [ passed ]")

    def do_inherited_time_column(self):
        for table in ("t1", "t2", "t3"):
            tdSql.execute(f"create table {table} (ts timestamp, v int)")
        tdSql.execute("insert into t1 values (1500, 1)")
        tdSql.execute("insert into t2 values (1000, 2)")
        tdSql.execute("insert into t3 values (1000, 10) (1500, 15)")
        child = "t1 left asof join t2 on timetruncate(t1.ts, 1s) >= t2.ts"
        self.check_rows(
            "select t1.v, t3.v from (" + child + ") "
            "left window join t3 window_offset(0s, 0s)",
            [(1, 15)],
        )
        self.check_rows(
            "select q.v, t3.v from "
            "(select timetruncate(t1.ts, 1s) event_ts, t1.v from " + child +
            ") q left window join t3 window_offset(0s, 0s)",
            [(1, 10)],
        )
        print("inherited public time column .............. [ passed ]")

    def check_join_algorithms(self, sql, expected, algorithms):
        tdSql.query("explain " + sql)
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert re.findall(r"algo=(Hash|Merge|Theta)", plan) == algorithms, plan
        tdSql.query(sql)
        assert Counter(tdSql.queryResult) == Counter(expected), (
            sql, tdSql.queryResult, expected
        )

    def do_mixed_physical_algorithms(self):
        for table, values in (
            ("m1", "(1000,10) (2000,20) (4000,40)"),
            ("m2", "(1000,15) (3000,30) (4000,40)"),
            ("m3", "(1000,100) (2000,200) (5000,500)"),
            ("m4", "(1000,1000) (2000,2000) (4000,4000)"),
        ):
            tdSql.execute(f"create table {table} (ts timestamp, v int)")
            tdSql.execute(f"insert into {table} values {values}")

        # The unmatched middle row exposes unordered child JOIN output.
        for child in ("Hash", "Merge", "Theta"):
            subquery = (
                f"(select /*+ join_algo({child}) */ m1.ts, "
                "m1.v av, m2.v bv from m1 left join m2 "
                "on m1.ts=m2.ts) q"
            )
            for parent in ("Hash", "Merge", "Theta"):
                for right in (False, True):
                    source = (
                        f"m3 left join {subquery}" if right else
                        f"{subquery} left join m3"
                    )
                    expected = [(10, 15, 100), (20, None, 200)]
                    expected.append(
                        (None, None, 500) if right else (40, 40, None)
                    )
                    self.check_join_algorithms(
                        f"select /*+ join_algo({parent}) */ q.av,q.bv,m3.v "
                        f"from {source} on q.ts=m3.ts",
                        expected, [parent, child],
                    )

        self.check_join_algorithms(
            "select /*+ join_algo(hash) */ q.av,q.bv,q.cv,m4.v from "
            "(select /*+ join_algo(merge) */ t.ts,t.av,t.bv,m3.v cv from "
            "(select /*+ join_algo(theta) */ m1.ts,m1.v av,m2.v bv "
            "from m1 left join m2 on m1.ts=m2.ts) t "
            "left join m3 on t.ts=m3.ts) q left join m4 on q.ts=m4.ts",
            [(10, 15, 100, 1000), (20, None, 200, 2000),
             (40, 40, None, 4000)],
            ["Hash", "Merge", "Theta"],
        )

        for kind in ("asof", "window"):
            condition = (
                "on q.ts>=m3.ts" if kind == "asof" else
                "window_time(q.ts,m3.ts) window_offset(0s,0s)"
            )
            self.check_join_algorithms(
                "select q.av,q.bv,m3.v from "
                "(select /*+ join_algo(theta) */ m1.ts,m1.v av,m2.v bv "
                "from m1 left join m2 on m1.ts=m2.ts) q "
                f"left {kind} join m3 {condition}",
                [(10, 15, 100), (20, None, 200),
                 (40, 40, 200 if kind == "asof" else None)],
                ["Merge", "Theta"],
            )
            condition = (
                "on m1.ts=m2.ts" if kind == "asof" else
                "window_time(m1.ts,m2.ts) window_offset(0s,0s)"
            )
            self.check_join_algorithms(
                "select /*+ join_algo(theta) */ q.av,q.bv,m3.v from "
                "(select m1.ts,m1.v av,m2.v bv from m1 "
                f"left {kind} join m2 {condition}) q "
                "left join m3 on q.ts=m3.ts",
                [(10, 15, 100), (20, None, 200), (40, 40, None)],
                ["Theta", "Merge"],
            )
        print("mixed physical JOIN algorithms ............. [ passed ]")

    def do_preserved_join_primary_time(self, time_expr, expected):
        sql = (
            "select q.v, c.v from "
            f"(select a.tk fallback, {time_expr} time_key, a.v from "
            "(a join b on a.v = b.v) left join d on a.v = d.v) q "
            "left window join c window_offset(0s, 0s)"
        )
        self.check_rows(sql, expected)
        keys, plan = self.explain_time_keys(sql)
        assert "`q`.`time_key` Order:" in keys["Left"], plan
        print("preserved JOIN primary time ................ [ passed ]")

    def do_hash_join_input_order(self):
        sql = (
            "q.v, c.v from (select /*+ HASH_JOIN() */ a.ts, a.v from "
            "(select * from a order by ts desc limit 10) a join "
            "(select * from b order by ts desc limit 10) b "
            "on a.v = 2 - b.v) q left join "
            "(select * from c order by ts desc limit 10) c on q.ts = c.ts"
        )
        expected = [(0, 0), (1, 9), (2, 19)]
        self.check_rows(f"select /*+ MERGE_JOIN() */ {sql}", expected)
        # Compare rows in Python so an outer ORDER BY cannot alter the plan.
        self.check_rows(f"select {sql}", expected)
        print("Hash JOIN input order ..................... [ passed ]")

    def do_hash_join_output_order(self):
        sql = (
            "select /*+ HASH_JOIN() */ a.ts from "
            "(select ts from a order by ts desc limit 10) a join "
            "(select ts from b order by ts desc limit 10) b "
            "on a.ts = b.ts order by a.ts desc"
        )
        tdSql.query(f"explain verbose true {sql}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        hash_joins = [
            line for line in plan.splitlines()
            if "Join (" in line and "algo=Hash" in line
        ]
        assert len(hash_joins) == 1, plan
        assert "Sort input_order=unknown output_order=desc" in plan

        tdSql.query(sql)
        rows = tdSql.queryResult
        assert len(rows) == 3, rows
        tdSql.query("select ts from a order by ts desc")
        assert rows == tdSql.queryResult, (rows, tdSql.queryResult)
        print("Hash JOIN output order .................... [ passed ]")

    def do_hash_join_stored_order(self):
        sql = (
            "q.v, c.v from (select /*+ HASH_JOIN() */ a.ts, a.v "
            "from ((select * from a order by ts asc limit 10) a "
            "left asof join b on a.ts >= b.ts) "
            "join d on a.v = 2 - d.v) q left join c on q.ts = c.ts"
        )
        expected = [(0, 0), (1, 9), (2, 19)]
        self.check_rows("select " + sql, expected)
        self.check_rows("select /*+ MERGE_JOIN() */ " + sql, expected)
        print("Hash JOIN stored order .................... [ passed ]")

    def do_semi_matching_time_order(self, right):
        tdSql.execute("create table peer (ts timestamp, v int)")
        tdSql.execute(
            "insert into peer values (1000,1) (2000,2) (3000,0)"
        )
        source = (
            "peer b right semi join a" if right else
            "a left semi join peer b"
        ) + " on a.v = b.v"
        expected = [(0, 19), (1, 0), (2, 9)]
        for hint in ("", "/*+ HASH_JOIN() */", "/*+ MERGE_JOIN() */"):
            direct = (
                f"select {hint} a.v, c.v from ({source}) "
                "join c on b.ts = c.ts"
            )
            self.check_rows(direct, expected)
            for key in ("a.ts", "b.ts"):
                for order in ("asc", "desc"):
                    sql = (
                        f"select {hint} q.v, c.v from "
                        "(select /*+ HASH_JOIN() */ a.ts driver_time, "
                        f"b.ts match_time, a.v from {source} "
                        f"order by {key} {order} limit 3) q "
                        "join c on q.match_time = c.ts"
                    )
                    self.check_rows(sql, expected)
                    tdSql.query(f"explain verbose true {sql}")
                    plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                    joins = [line for line in plan.splitlines()
                             if "Join (" in line]
                    assert joins, plan
                    if "MERGE" in hint:
                        assert "algo=Merge" in joins[0], plan
                    elif "HASH" in hint:
                        assert "algo=Hash" in joins[0], plan
        for hint in ("/*+ HASH_JOIN() */", "/*+ MERGE_JOIN() */"):
            equal_time = source.replace("a.v = b.v", "a.ts = b.ts")
            self.check_rows(
                f"select {hint} a.v, c.v from ({equal_time}) "
                "join c on b.ts = c.ts",
                [(0, 0), (1, 9), (2, 19)],
            )
            bucket_time = source.replace(
                "a.v = b.v",
                "timetruncate(a.ts,1m) = timetruncate(b.ts,1m) "
                "and a.v = b.v",
            )
            self.check_rows(
                f"select {hint} a.v, c.v from ({bucket_time}) "
                "join c on b.ts = c.ts", expected,
            )
        self.check_rows(
            "select q.v, c.v from (select b.ts match_time, "
            f"a.ts driver_time, a.v from {source}) q "
            "left window join c window_offset(0s,0s)", expected,
        )
        self.check_rows(
            f"select a.v, c.v from ({source}) left window join c "
            "window_time(b.ts,c.ts) window_offset(0s,0s)", expected,
        )
        print("SEMI matching time order .................. [ passed ]")

    @pytest.mark.parametrize("right", [False, True])
    def test_semi_matching_time_order(self, join_regression_data, right):
        """Order nested SEMI inputs by the actual parent join key

        1. Match ordinary values to out-of-order peer timestamps
        2. Check Hash and Merge above direct and sorted derived inputs

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-24 Tony Zhang Cover matching-side timestamp order
        """
        self.do_semi_matching_time_order(right)

    def do_semi_anti_post_filters(self, right):
        tdSql.execute("create table peer (ts timestamp, v int, x int)")
        tdSql.execute(
            "insert into peer values (1000,0,9) (2000,1,4)"
        )
        for algo in ("HASH", "MERGE"):
            hint = f"/*+ {algo}_JOIN() */"
            for kind in ("semi", "anti"):
                source = (
                    f"peer b right {kind} join a" if right else
                    f"a left {kind} join peer b"
                ) + " on a.ts = b.ts"
                sql = f"select {hint} a.v, b.x from {source}"
                if kind == "semi":
                    self.check_rows(sql, [(0, 9), (1, 4)])
                    self.check_rows(sql + " where b.x > 5", [(0, 9)])
                    self.check_rows(sql + " where b.x < 5", [(1, 4)])
                    self.check_rows(sql + " where b.x is null", [])
                else:
                    self.check_rows(sql, [(2, None)])
                    self.check_rows(sql + " where b.x is null", [(2, None)])
                    self.check_rows(sql + " where b.x is not null", [])
                    self.check_rows(
                        f"select {hint} a.v, b.ts from {source}",
                        [(2, None)],
                    )
        tdSql.execute("insert into peer values (4000,0,2) (5000,0,8)")
        source = (
            "peer b right semi join a" if right else
            "a left semi join peer b"
        ) + " on a.v = b.v"
        source = source.replace(
            "a.v = b.v",
            "timetruncate(a.ts,1m) = timetruncate(b.ts,1m) "
            "and a.v = b.v",
        )
        for algo in ("HASH", "MERGE"):
            sql = f"select /*+ {algo}_JOIN() */ a.v, b.x from {source}"
            tdSql.query(sql)
            rows = tdSql.queryResult
            assert len(rows) == 2, (sql, rows)
            assert (1, 4) in rows, (sql, rows)
            assert any(row in rows for row in ((0, 9), (0, 2), (0, 8))), rows
            tdSql.query(sql + " where b.x > 5")
            assert tdSql.queryResult in ([], [(0, 9)], [(0, 8)]), (
                sql, tdSql.queryResult
            )
            tdSql.query(sql + " and b.x > 5")
            assert tdSql.queryResult in ([(0, 9)], [(0, 8)]), (
                sql, tdSql.queryResult
            )
        print("SEMI/ANTI post filters .................... [ passed ]")

    @pytest.mark.parametrize("right", [False, True])
    def test_semi_anti_post_filters(self, join_regression_data, right):
        """Filter selected SEMI peers and NULL-extended ANTI outputs

        1. Check both algorithms with unique matches and NULL peer columns
        2. Keep post-filter and ON semantics distinct for multiple matches

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-24 Tony Zhang Cover SEMI/ANTI matching-side filters
        """
        self.do_semi_anti_post_filters(right)

    def do_semi_join_nullable_time(self, right):
        tdSql.execute("create table nullable_a (ts timestamp, v int)")
        tdSql.execute("insert into nullable_a values (1000, 0)")
        input_sql = "(nullable_a a right join b on a.ts = b.ts)"
        source = (
            f"c right semi join {input_sql}" if right else
            f"{input_sql} left semi join c"
        )
        sql = (
            "select q.v, d.v from (select a.ts + 1s nullable_time, "
            f"b.ts preserved_time, b.v from {source} on b.ts = c.ts) q "
            "left window join d window_offset(0s, 0s)"
        )
        self.check_rows(sql, [(0, 0), (1, 1), (2, 2)])
        keys, plan = self.explain_time_keys(sql)
        assert "`q`.`preserved_time` Order:" in keys["Left"], plan
        print("SEMI JOIN nullable time ................... [ passed ]")

    def test_hash_join_stored_order(self, join_regression_data):
        """Reject stale ordering above a Hash JOIN

        1. Feed an ordered ASOF subtree into a non-time Hash JOIN
        2. Join its unordered output to a bare scan without losing matches

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-23 Tony Zhang Cover the stored order fallback
        """
        self.do_hash_join_stored_order()

    @pytest.mark.parametrize("right", [False, True])
    def test_semi_join_nullable_time(self, join_regression_data, right):
        """Keep nullable timestamps ordinary through a nested SEMI JOIN

        1. Preserve NULL-extended rows from the retained JOIN subtree
        2. Select its non-null primary time instead of a nullable expression

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-23 Tony Zhang Cover both SEMI JOIN directions
        """
        self.do_semi_join_nullable_time(right)

    @pytest.mark.parametrize("time_expr, expected", [
        ("a.ts", [(0, 0), (1, 9), (2, 19)]),
        ("a.ts + 1s", [(0, 9), (1, 19), (2, None)]),
        ("timetruncate(a.ts, 1s)", [(0, 0), (1, 9), (2, 19)]),
    ])
    def test_preserved_join_primary_time(self, join_regression_data,
                                        time_expr, expected):
        """Prefer primary time from a nested preserved JOIN input

        1. Put an ordinary timestamp before the preserved primary timestamp
        2. Check matched and unmatched rows using the selected primary time
        3. Preserve primary semantics for shifted and truncated time

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-23 Tony Zhang Cover primary time through nested LEFT JOIN
        """
        self.do_preserved_join_primary_time(time_expr, expected)

    def test_hash_join_input_order(self, join_regression_data):
        """Preserve matches when sorted leaves feed an unordered Hash JOIN

        1. Permute timestamp order through a non-time Hash JOIN condition
        2. Check default LEFT JOIN and explicit Merge against complete rows

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-23 Tony Zhang Cover default JOIN over unordered Hash
              output
        """
        self.do_hash_join_input_order()

    def test_hash_join_does_not_claim_sorted_output(
        self, join_regression_data
    ):
        """Do not propagate sorted leaf order across Hash JOIN

        1. Confirm EXPLAIN marks the Sort input as unordered
        2. Preserve descending query results with a final Sort

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-29 Tony Zhang Cover Hash order propagation
        """
        self.do_hash_join_output_order()

    def test_temporal_contract(self):
        """Temporal joins over mixed nested inputs

        1. Default WINDOW keys survive omitted output columns
        2. Explicit time keys filter NULL rows on both input sides
        3. FULL OUTER permits single-side time keys and filters NULL times
        4. EXPLAIN names the actual time keys and direction
        5. Empty, duplicate and limited inputs retain temporal semantics
        6. Nested temporal consumers inherit the public time column
        7. Ten distinct JOIN types retain their physical tree and time keys
        8. Mix physical algorithms with unordered child output on either side
        9. Use Hash, Merge and Theta together in one four-table query
        10. Preserve primary time priority and ordinary/constant fallback
        11. Choose primary time before sorting unordered Hash JOIN inputs
        12. Preserve expression time selection across transparent projections

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-13 Tony Zhang Add nested temporal contract regression
            - 2026-09-22 Tony Zhang Strengthen temporal EXPLAIN assertions
            - 2026-09-22 Tony Zhang Cover mixed physical algorithms and ordering
            - 2026-09-22 Tony Zhang Preserve subquery time selection beside
              JOINs
        """
        self.prepare()
        self.do_explain()
        self.do_ten_level_explain()
        self.do_default_window()
        self.do_explicit_time_and_null()
        self.do_full_time_keys()
        self.do_subquery_time_selection()
        self.do_join_input_primary_time()
        self.do_expression_time_depth()
        self.do_input_boundaries()
        self.do_inherited_time_column()
        self.do_mixed_physical_algorithms()


class TestJoinMixedNestedOrdinary:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def check_rows(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def do_prepare_data(self):
        tdSql.prepare(dbname="join_mixed_nested", vgroups=1)
        for table in ("a", "b", "c", "d", "e", "f", "g", "h"):
            tdSql.execute(f"create table {table} (ts timestamp, v int)")
        tdSql.execute("create table fanout (ts timestamp, v int)")

        tdSql.execute(
            "insert into a values ('2026-01-01 00:00:01', 11) "
            "('2026-01-01 00:00:02', 12) "
            "('2026-01-01 00:00:04', 14)"
        )
        tdSql.execute(
            "insert into b values ('2026-01-01 00:00:01', 21) "
            "('2026-01-01 00:00:03', 23) "
            "('2026-01-01 00:00:04', 24)"
        )
        tdSql.execute(
            "insert into c values ('2026-01-01 00:00:01', 31) "
            "('2026-01-01 00:00:02', 32) "
            "('2026-01-01 00:00:04', 34) "
            "('2026-01-01 00:00:06', 36)"
        )
        tdSql.execute(
            "insert into d values ('2026-01-01 00:00:01', 41) "
            "('2026-01-01 00:00:04', 44) "
            "('2026-01-01 00:00:05', 45)"
        )
        tdSql.execute(
            "insert into e values ('2026-01-01 00:00:01', 51) "
            "('2026-01-01 00:00:03', 53) "
            "('2026-01-01 00:00:05', 55)"
        )
        tdSql.execute("insert into f select * from a")
        tdSql.execute("insert into g select * from b")
        tdSql.execute("insert into h select * from c")

        values = []
        for row in range(5000):
            second = row // 1000
            millisecond = row % 1000
            values.append(
                f"('2026-01-01 00:00:{second:02d}."
                f"{millisecond:03d}', {row})"
            )
            if len(values) == 1000:
                tdSql.execute("insert into fanout values " + " ".join(values))
                values.clear()

    def do_check_left_deep(self, hint=""):
        self.check_rows(
            f"select {hint} a.v, b.v, c.v, d.v, e.v "
            "from a join b on a.ts = b.ts "
            "left join c on b.ts = c.ts "
            "right join d on c.ts = d.ts "
            "full join e on d.ts = e.ts "
            "order by ifnull(d.ts, e.ts)",
            [
                (11, 21, 31, 41, 51),
                (None, None, None, None, 53),
                (14, 24, 34, 44, None),
                (None, None, None, 45, 55),
            ],
        )

    def do_check_right_deep(self, hint=""):
        if not hint:
            tdSql.query(
                "explain verbose true select a.ts, b.ts, c.ts "
                "from a left join (b full join c on b.ts = c.ts) "
                "on a.ts = b.ts"
            )
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Left Join" in plan and "algo=Hash" in plan, plan
            assert "Full Join" in plan and "algo=Merge" in plan, plan
        self.check_rows(
            f"select {hint} a.ts, b.ts, c.ts, a.v, b.v, c.v "
            "from a left join "
            "(b full join c on b.ts = c.ts) on a.ts = b.ts "
            "order by a.ts",
            [
                (
                    "2026-01-01 00:00:01.000",
                    "2026-01-01 00:00:01.000",
                    "2026-01-01 00:00:01.000",
                    11,
                    21,
                    31,
                ),
                (
                    "2026-01-01 00:00:02.000",
                    None,
                    None,
                    12,
                    None,
                    None,
                ),
                (
                    "2026-01-01 00:00:04.000",
                    "2026-01-01 00:00:04.000",
                    "2026-01-01 00:00:04.000",
                    14,
                    24,
                    34,
                ),
            ],
        )

    def do_check_outer_on_condition(self, hint=""):
        self.check_rows(
            f"select {hint} a.v, b.ts, b.v from a "
            "left join b on a.ts = b.ts and b.v = 21 order by a.ts",
            [
                (11, "2026-01-01 00:00:01.000", 21),
                (12, None, None),
                (14, None, None),
            ],
        )

    def do_check_hash_outer_continuation(self):
        time_condition = (
            "timetruncate(a.ts, 1h) = timetruncate(f.ts, 1h) "
            "and f.v >= 0"
        )
        self.check_rows(
            "select /*+ hash_join() */ count(*) from a "
            f"left join fanout f on {time_condition}",
            [(15000,)],
        )
        self.check_rows(
            "select /*+ hash_join() */ count(*) from fanout f "
            f"right join a on {time_condition}",
            [(15000,)],
        )

    def do_check_nullable_parent_keys(self, hint=""):
        left = "(a full join b on a.ts = b.ts)"
        right = "(c full join d on c.ts = d.ts)"
        for join_type, rows in (
            ("join", 2),
            ("left join", 4),
            ("right join", 5),
            ("full join", 7),
        ):
            self.check_rows(
                f"select {hint} count(*) from {left} {join_type} {right} "
                "on b.ts = c.ts",
                [(rows,)],
            )

    def do_check_balanced_eight_table_tree(self):
        left = (
            "((a join b on a.ts = b.ts) "
            "join (c join d on c.ts = d.ts) on a.ts = c.ts)"
        )
        right = (
            "((e join f on e.ts = f.ts) "
            "join (g join h on g.ts = h.ts) on e.ts = g.ts)"
        )
        sql = f"select count(*) from {left} join {right} on a.ts = e.ts"
        tdSql.query("explain verbose true " + sql)
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert plan.count("Inner Join") == 7, plan
        assert plan.count("algo=") == 7, plan
        self.check_rows(sql, [(1,)])

    def do_check_nested_semi_anti(self):
        left = "(a left join b on a.ts = b.ts)"
        right = "(c join d on c.ts = d.ts)"
        for join_type, projection, expected in (
            ("left semi join", "a.v", [(11,), (14,)]),
            ("left anti join", "a.v", [(12,)]),
            ("right semi join", "c.v", [(31,), (34,)]),
            ("right anti join", "c.v", []),
        ):
            self.check_rows(
                f"select {projection} from {left} {join_type} {right} "
                f"on a.ts = c.ts order by {projection}",
                expected,
            )

    def do_check_virtual_table_tree(self):
        self.check_rows(
            "select a.v, v.db_name from (a join b on a.ts = b.ts) "
            "join information_schema.ins_vgroups v "
            "on a.v = length(v.db_name) - 6 "
            "where v.db_name = 'join_mixed_nested'",
            [(11, "join_mixed_nested")],
        )

    def do_check_derived_table(self):
        self.check_rows(
            "select q.av, q.bv, c.v from "
            "(select a.ts, a.v av, b.v bv from a "
            "left join b on a.ts = b.ts) q "
            "full join c on q.ts = c.ts "
            "order by ifnull(q.ts, c.ts)",
            [
                (11, 21, 31),
                (12, None, 32),
                (14, 24, 34),
                (None, None, 36),
            ],
        )

    def do_check_predicate_position(self):
        join_sql = (
            "from a left join b on a.ts = b.ts "
            "left join c on b.ts = c.ts and c.v = 31 "
        )
        self.check_rows(
            "select a.v, b.v, c.v " + join_sql + "order by a.ts",
            [(11, 21, 31), (12, None, None), (14, 24, None)],
        )
        self.check_rows(
            "select a.v, b.v, c.v " + join_sql +
            "where c.v = 31 order by a.ts",
            [(11, 21, 31)],
        )

    def do_test(self):
        self.do_prepare_data()
        self.do_check_left_deep()
        self.do_check_left_deep("/*+ hash_join() */")
        self.do_check_right_deep()
        self.do_check_right_deep("/*+ hash_join() */")
        self.do_check_right_deep("/*+ merge_join() */")
        self.do_check_derived_table()
        self.do_check_predicate_position()
        self.do_check_outer_on_condition()
        self.do_check_outer_on_condition("/*+ hash_join() */")
        self.do_check_hash_outer_continuation()
        self.do_check_nullable_parent_keys()
        self.do_check_nullable_parent_keys("/*+ hash_join() */")
        self.do_check_balanced_eight_table_tree()
        self.do_check_nested_semi_anti()
        self.do_check_virtual_table_tree()

    def test_join_mixed_nested(self):
        """Mixed nested ordinary joins

        1. Mix INNER, LEFT, RIGHT, and FULL joins in a five-table tree
        2. Execute right-deep and derived-table join trees
        3. Verify NULL extension and ON versus WHERE semantics
        4. Verify default, HASH_JOIN, and MERGE_JOIN execution
        5. Cover an eight-table balanced tree and all SEMI/ANTI directions
        6. Consume a nested virtual-table JOIN tree

        Catalog:
            - Query:Join

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-8-28 Initial mixed nested ordinary join coverage

        """
        self.do_test()
