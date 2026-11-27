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

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from itertools import product
import json
import math
import os
from pathlib import Path
import re
from statistics import median
import threading
import time
from types import SimpleNamespace
import uuid

import pytest


class TestJoinParallel:
    @pytest.fixture
    def join_sql(self):
        from new_test_framework.utils import tdLog, tdSql

        self.sql = tdSql
        self.log = tdLog
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def make_rows(self, table_index, count=5, profile="small"):
        rows = []
        for index in range(count):
            if profile == "small":
                key = (1, 2, 2, None, 100 + table_index)[index]
                tick = (index + 1 if index < 4 else 10 + table_index)
                value = (table_index + 1) * 100 + index + 1
            else:
                tick = index + 1
                key = index // 2 + 1 if profile == "duplicate" else index + 1
                if profile == "skew" and index < min(8, count):
                    key = 1
                if profile == "all":
                    key = 1
                value = (table_index + 1) * 1000000 + index + 1
            stamp = 1750046400000 + tick * 1000
            text = None if key is None else f"k{key}"
            time_key = None if index == 2 else stamp
            number = None if key is None else float(key)
            rows.append((stamp, key, text, value, time_key, number, "x" * 64))
        return rows

    def sql_value(self, value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return str(value)

    def insert_rows(self, table, rows):
        for offset in range(0, len(rows), 256):
            values = [
                "(" + ",".join(self.sql_value(value) for value in row) + ")"
                for row in rows[offset:offset + 256]
            ]
            self.sql.execute(f"insert into {table} values " + " ".join(values))

    def drop_data(self):
        if getattr(self, "database", None):
            self.sql.execute(f"drop database if exists {self.database}")
            self.database = None

    def prepare_data(self, count=5, profile="small", vgroups=1, kind="normal",
                     names="abcdefghi", payload_width=64, column_types=None,
                     counts=None):
        self.drop_data()
        self.database = f"join_parallel_{uuid.uuid4().hex[:10]}"
        self.sql.execute(
            f"create database {self.database} vgroups {vgroups} keep 36500"
        )
        self.sql.execute(f"use {self.database}")
        self.rows = {}
        schema_template = (
            "(ts timestamp, k {key}, s {text}, v int, tk timestamp, "
            "f double, payload binary(256))"
        )
        for index, table in enumerate(names):
            key_type, text_type = (column_types or {}).get(
                table, ("int", "binary(32)")
            )
            schema = schema_template.format(key=key_type, text=text_type)
            table_count = count if counts is None else counts.get(table, count)
            rows = self.make_rows(index, table_count, profile)
            rows = [row[:6] + ("x" * payload_width,) for row in rows]
            self.rows[table] = rows
            table_kind = kind
            if kind == "mixed":
                table_kind = ("normal", "child", "super")[index % 3]
            if table_kind == "normal":
                self.sql.execute(f"create table {table} {schema}")
                self.insert_rows(table, rows)
            elif table_kind == "child":
                self.sql.execute(
                    f"create stable st_{table} {schema} tags(g int)"
                )
                self.sql.execute(
                    f"create table {table} using st_{table} tags(0)"
                )
                self.insert_rows(table, rows)
            else:
                self.sql.execute(f"create stable {table} {schema} tags(g int)")
                for shard in range(2):
                    child = f"{table}_{shard}"
                    self.sql.execute(
                        f"create table {child} using {table} tags({shard})"
                    )
                    self.insert_rows(child, rows[shard::2])
        schema = schema_template.format(key="int", text="binary(32)")
        self.sql.execute(f"create table empty {schema}")
        self.rows["empty"] = []

    def replace_rows(self, table, rows):
        self.rows[table] = rows
        self.sql.execute(f"delete from {table}")
        self.insert_rows(table, rows)

    def separate_keys(self):
        for index, name in enumerate("abcd"):
            rows = []
            for offset, row in enumerate(self.rows[name]):
                text = f"s{offset // 4}-{(offset + index) % 2}"
                rows.append(row[:2] + (text,) + row[3:])
            self.replace_rows(name, rows)

    def inner_rows(self, edges, names="abcd"):
        result = [(row,) for row in self.rows[names[0]]]
        for position, name in enumerate(names[1:], 1):
            conditions = [(names.index(left), left_col, right_col)
                          for left, left_col, right, right_col in edges
                          if right == name]
            assert conditions and all(i < position for i, _, _ in conditions)
            index = {}
            for row in self.rows[name]:
                key = tuple(row[column] for _, _, column in conditions)
                if None not in key:
                    index.setdefault(key, []).append(row)
            joined = []
            for rows in result:
                key = tuple(rows[i][column] for i, column, _ in conditions)
                joined.extend(rows + (row,) for row in index.get(key, []))
            result = joined
        return result

    def equivalence_cases(self):
        separate = [("a", 1, "b", 1), ("a", 2, "c", 2),
                    ("a", 0, "d", 0)]
        multiple = [("a", column, name, column)
                    for name in "bcd" for column in (1, 2)]
        query = self.join_query().replace("a.k=c.k", "a.s=c.s")
        query = query.replace("a.k=d.k", "a.ts=d.ts")
        both = self.join_query(hint="hash_join")
        for name in "bcd":
            both = both.replace(f"a.k={name}.k",
                                f"a.k={name}.k and a.s={name}.s")
        return [
            ("separate-classes", query,
             self.values(self.inner_rows(separate)), 3),
            ("multiple-classes", both,
             self.values(self.inner_rows(multiple)), 2),
        ]

    def explain_equalities(self, condition):
        database = r"(?:[A-Za-z_]\w*\.)?"
        pattern = (
            rf"(?<![\w.]){database}([A-Za-z_]\w*)\.(k|s)\s*=\s*"
            rf"{database}([A-Za-z_]\w*)\.\2(?![\w.])"
        )
        return re.findall(pattern, condition.replace("`", ""))

    def explain_has_time_key(self, details, side, alias, key):
        pattern = (
            rf"\b{re.escape(side)} Time Key:\s*(?:[A-Za-z_]\w*\.)?"
            rf"{re.escape(alias)}\.{re.escape(key)}(?![\w.])"
        )
        return re.search(pattern, details.replace("`", "")) is not None

    def check_explain_column_parsers(self):
        equalities = (
            ("a.k=d.k", [("a", "k", "d")]),
            ("`db_1`.`a`.`k`=`db_1`.`d`.`k` and db_1.b.s=c.s",
             [("a", "k", "d"), ("b", "s", "c")]),
            ("a.k=db_1.d.k", [("a", "k", "d")]),
            ("db_1.a.k=db_1.d.s", []),
            ("db_1.a.k=db_1.d.k_extra", []),
            ("db_1.a.k_extra=db_1.d.k", []),
            ("catalog.db_1.a.k=db_1.d.k", []),
        )
        for condition, expected in equalities:
            assert self.explain_equalities(condition) == expected, condition
        time_keys = (
            ("Left Time Key: a.ts Order: asc", True),
            ("Left Time Key: `db_1`.`a`.`ts` Order: asc", True),
            ("Left Time Key: db_1.b.ts Order: asc", False),
            ("Left Time Key: db_1.a.ts_extra Order: asc", False),
            ("Left Time Key: db_1.a.ts.other Order: asc", False),
            ("Right Time Key: db_1.a.ts Order: asc", False),
            ("Left Time Key: db_1.a.tk Order: asc", False),
        )
        for details, expected in time_keys:
            actual = self.explain_has_time_key(details, "Left", "a", "ts")
            assert actual == expected, details

    def check_eq_classes(self, plan):
        for node in self.join_nodes(self.plan_nodes(plan)):
            assert "algo=Hash" in node["label"], plan
            left, right = (self.table_names([child])
                           for child in node["children"])
            condition = " ".join(line for line in node["details"]
                                 if line.startswith("Join Full Cond:"))
            pairs = self.explain_equalities(condition)
            columns = {column for a, column, b in pairs
                       if (a in left and b in right)
                       or (a in right and b in left)}
            assert columns == {"k", "s"}, (node, columns)

    def reordered_query(self):
        query = (
            "select * from a join b on a.s=b.s "
            "join c on a.k=c.k join d on b.k=d.k"
        )
        edges = [("a", 2, "b", 2), ("a", 1, "c", 1),
                 ("b", 1, "d", 1)]
        expected = [tuple(value for row in rows for value in row)
                    for rows in self.inner_rows(edges)]
        return query, expected

    def normalize_rows(self, rows):
        return [tuple(round(value.timestamp() * 1000)
                      if hasattr(value, "timestamp") else value
                      for value in row) for row in rows]

    def equi_rows(self, names, key="k", null_equal=False):
        column = {"ts": 0, "k": 1, "s": 2, "tk": 4, "f": 5}[key]
        indexes = []
        for name in names:
            index = {}
            for row in self.rows[name]:
                value = row[column]
                if value is not None or null_equal:
                    index.setdefault(value, []).append(row)
            indexes.append(index)
        common = set(indexes[0]).intersection(*(set(i) for i in indexes[1:]))
        return [
            combination
            for value in common
            for combination in product(*(index[value] for index in indexes))
        ]

    def values(self, combinations):
        return [
            tuple(None if row is None else row[3] for row in combination)
            for combination in combinations
        ]

    def join_query(self, names="abcd", key="k", shape="star", hint=""):
        names = list(names)

        def build(group):
            if len(group) == 1:
                return group[0]
            middle = len(group) // 2
            left, right = group[:middle], group[middle:]
            return (
                f"({build(left)} join {build(right)} "
                f"on {left[0]}.{key}={right[0]}.{key})"
            )

        if shape == "balanced":
            source = build(names)
        else:
            source = names[0]
            for index, name in enumerate(names[1:], 1):
                previous = names[0] if shape == "star" else names[index - 1]
                source += f" join {name} on {previous}.{key}={name}.{key}"
        columns = ",".join(f"{name}.v as v{i}" for i, name in enumerate(names))
        comment = f"/*+ {hint}() */ " if hint else ""
        return f"select {comment}{columns} from {source}"

    def explain(self, query):
        self.sql.query("explain verbose true " + query)
        return "\n".join(str(row[0]) for row in self.sql.queryResult)

    def parse_execution_ms(self, plan):
        footers = [line.strip() for line in plan.splitlines()
                   if line.strip().startswith("Execution Time:")]
        assert len(footers) == 1, (
            f"Expected one Execution Time footer: {footers}"
        )
        match = re.fullmatch(
            r"Execution Time:\s*([+-]?\d+(?:\.\d+)?)\s+ms", footers[0]
        )
        assert match is not None, f"Invalid Execution Time: {footers[0]}"
        value = float(match[1])
        assert math.isfinite(value) and value >= 0, footers[0]
        return value

    def analyze_execution_ms(self, query):
        self.sql.query("EXPLAIN ANALYZE VERBOSE true " + query)
        plan = "\n".join(str(row[0]) for row in self.sql.queryResult)
        return self.parse_execution_ms(plan)

    def plan_nodes(self, plan):
        roots, stack = [], []
        for line in plan.splitlines():
            match = re.match(r"^(\s*)->\s*(.*)", line)
            if not match:
                indent = len(line) - len(line.lstrip())
                for depth, node in reversed(stack):
                    if line.strip() and indent > depth:
                        node["details"].append(line.strip())
                        break
                continue
            indent, label = len(match[1]), match[2]
            node = {"label": label, "details": [], "children": []}
            while stack and stack[-1][0] >= indent:
                stack.pop()
            (stack[-1][1]["children"] if stack else roots).append(node)
            stack.append((indent, node))
        assert roots, plan
        return roots

    def walk_plan(self, nodes):
        for node in nodes:
            yield node
            yield from self.walk_plan(node["children"])

    def is_join(self, node):
        return re.search(r"\bJoin\s*\(", node["label"]) is not None

    def join_type(self, node):
        return node["label"].split("(", 1)[0].strip()

    def join_nodes(self, nodes):
        return [node for node in self.walk_plan(nodes) if self.is_join(node)]

    def scan_table(self, node):
        match = re.search(
            r"\b(?:Table(?: Merge)?|Tag) Scan on ([^\s(]+)", node["label"]
        )
        if match:
            return match[1].rsplit(".", 1)[-1].strip("`")
        return None

    def table_names(self, nodes):
        return frozenset(
            self.scan_table(node) for node in self.walk_plan(nodes)
            if self.scan_table(node) is not None
        )

    def join_tree(self, nodes, opaque=None):
        trees = []
        for node in nodes:
            boundary = opaque(node) if opaque else None
            table = self.scan_table(node)
            if boundary is not None:
                trees.append(boundary)
            elif self.is_join(node):
                assert len(node["children"]) == 2, node
                trees.append((
                    self.join_type(node),
                    *(self.join_tree([child], opaque)
                      for child in node["children"]),
                ))
            elif table is not None:
                trees.append(table)
            else:
                trees.append(self.join_tree(node["children"], opaque))
        assert len(trees) == 1, nodes
        return trees[0]

    def join_shape(self, nodes, opaque=None):
        def without_types(tree):
            if isinstance(tree, str):
                return tree
            return tuple(without_types(child) for child in tree[1:])

        return without_types(self.join_tree(nodes, opaque))

    def input_shape(self, names, balanced=False):
        if len(names) == 1:
            return names[0]
        if balanced:
            middle = len(names) // 2
            return (self.input_shape(names[:middle], True),
                    self.input_shape(names[middle:], True))
        tree = names[0]
        for name in names[1:]:
            tree = (tree, name)
        return tree

    def join_height(self, nodes, opaque=None):
        return max(
            (0 if opaque and opaque(node) is not None else
             int(self.is_join(node))
             + self.join_height(node["children"], opaque)
             for node in nodes),
            default=0,
        )

    def paths_to_join(self, node, exchanges=0):
        exchanges += int(node["label"].startswith("Data Exchange "))
        if self.is_join(node):
            return [(node, exchanges)]
        return [
            path
            for child in node["children"]
            for path in self.paths_to_join(child, exchanges)
        ]

    def check_split_paths(self, roots, expected_parents=None):
        parents = 0
        for node in self.join_nodes(roots):
            assert len(node["children"]) == 2, node
            sides = [self.paths_to_join(child) for child in node["children"]]
            if not all(sides):
                continue
            parents += 1
            for paths in sides:
                assert len(paths) == 1, (node, paths)
                assert paths[0][1] == 1, (node, paths)
        assert parents > 0, roots
        if expected_parents is not None:
            assert parents == expected_parents, (parents, roots)

    def check_plan(self, query, joins, height=None, split=False,
                   algorithm=None, tree=None, shape=None, join_types=None,
                   split_parents=None):
        plan = self.explain(query)
        roots = self.plan_nodes(plan)
        nodes = self.join_nodes(roots)
        assert len(nodes) == joins, plan
        if height is not None:
            assert self.join_height(roots) == height, plan
        if tree is not None:
            assert self.join_tree(roots) == tree, plan
        if shape is not None:
            assert self.join_shape(roots) == shape, plan
        if join_types is not None:
            types = [self.join_type(node) for node in nodes]
            if "Outer Join" in join_types:
                types = ["Outer Join" if kind in ("Left Join", "Right Join")
                         else kind for kind in types]
            assert Counter(types) == Counter(join_types), plan
        if split or split_parents is not None:
            self.check_split_paths(roots, split_parents)
        if algorithm:
            algorithms = [f"algo={algorithm}" in node["label"]
                          for node in nodes]
            assert all(algorithms), plan
        return plan

    def check_rows(self, query, expected, ordered=False):
        self.sql.query(query)
        actual = self.normalize_rows(self.sql.queryResult)
        if ordered:
            assert actual == expected, (query, actual, expected)
        else:
            assert Counter(actual) == Counter(expected), (
                query, actual, expected
            )
        return actual

    def outer_rows(self, left, right, mode, predicate):
        result, used = [], set()
        left_width = len(left[0]) if left else 2
        right_width = len(right[0]) if right else 2
        for left_row in left:
            matches = [i for i, right_row in enumerate(right)
                       if predicate(left_row, right_row)]
            if mode == "semi":
                if matches:
                    result.append(left_row)
                continue
            if mode == "anti":
                if not matches:
                    result.append(left_row)
                continue
            for index in matches:
                used.add(index)
                result.append(left_row + right[index])
            if not matches and mode in ("left", "full"):
                result.append(left_row + (None,) * right_width)
        if mode in ("right", "full"):
            result.extend((None,) * left_width + row
                          for i, row in enumerate(right) if i not in used)
        return result

    def mixed_case(self, mode="left", residual=False):
        left = self.equi_rows("ab")
        right = self.equi_rows("cd")
        threshold = self.rows["c"][len(self.rows["c"]) // 2][3] - 1
        predicate = lambda l, r: (
            l[1][1] == r[0][1] and (not residual or r[0][3] > threshold)
        )
        expected = self.outer_rows(left, right, mode, predicate)
        kinds = {"inner": "join", "left": "left join", "right": "right join",
                 "full": "full join", "semi": "left semi join",
                 "anti": "left anti join"}
        columns = "a.v,b.v" if mode in ("semi", "anti") else "a.v,b.v,c.v,d.v"
        query = (
            f"select {columns} from (a join b on a.k=b.k) {kinds[mode]} "
            "(c join d on c.k=d.k) on b.k=c.k"
        )
        if residual:
            query += f" and c.v>{threshold}"
        return query, expected

    def mixed_chain(self):
        left = self.equi_rows("ab")
        middle = [(row,) for row in self.rows["c"]]
        joined = self.outer_rows(
            left, middle, "left",
            lambda a, c: a[1][1] == c[0][1] and c[0][3] > 302,
        )
        right = [(row,) for row in self.rows["d"]]
        expected = self.outer_rows(
            joined, right, "left", lambda a, d: a[2] is not None
            and a[2][1] is not None and a[2][1] == d[0][1],
        )
        query = (
            "select a.v,b.v,c.v,d.v from a join b on a.k=b.k "
            "left join c on b.k=c.k and c.v>302 "
            "left join d on c.k=d.k"
        )
        return query, self.values(expected)

    def mixed_rewrite_cases(self):
        left = self.equi_rows("ab")
        middle = [(row,) for row in self.rows["c"]]
        right = [(row,) for row in self.rows["d"]]
        c_limit, d_limit = self.rows["c"][1][3], self.rows["d"][1][3]
        prefix = "select a.v,b.v,c.v,d.v from a join b on a.k=b.k "
        chain = self.outer_rows(
            left, middle, "left", lambda a, c: c[0][1] is not None
            and a[1][1] == c[0][1] and c[0][3] > c_limit,
        )
        cases = []
        for null_accepting in (False, True):
            def matches(left_row, right_row):
                row = left_row[2]
                if row is None or row[1] is None:
                    return null_accepting
                return (right_row[0][1] is not None
                        and row[1] == right_row[0][1])

            expected = self.outer_rows(chain, right, "left", matches)
            query = (prefix + f"left join c on b.k=c.k and c.v>{c_limit} "
                     "left join d on c.k=d.k")
            if null_accepting:
                query += " or c.k is null"
            name = ("on-null-accepting" if null_accepting
                    else "on-null-rejecting")
            height = 3 if null_accepting else 2
            kind = "Left Join" if null_accepting else "Outer Join"
            cases.append((name, query, self.values(expected), height,
                          {"Inner Join": 1, kind: 2}))

        joined = self.outer_rows(
            left, middle, "left", lambda a, c: c[0][1] is not None
            and a[0][1] == c[0][1] and c[0][3] > c_limit,
        )
        star = self.outer_rows(
            joined, right, "left", lambda a, d: d[0][1] is not None
            and a[0][1] == d[0][1] and d[0][3] > d_limit,
        )
        query = (prefix + f"left join c on a.k=c.k and c.v>{c_limit} "
                 f"left join d on a.k=d.k and d.v>{d_limit} where ")
        conditions = (
            ("where-null-rejecting", "c.v>0 and d.v>0",
             lambda row: row[2] is not None and row[3] is not None
             and row[2][3] > 0 and row[3][3] > 0, 2),
            ("where-is-null", "c.v is null and d.v is null",
             lambda row: row[2] is None and row[3] is None, 3),
            ("where-null-accepting",
             "(c.v>0 or c.v is null) and (d.v>0 or d.v is null)",
             lambda row: (row[2] is None or row[2][3] > 0)
             and (row[3] is None or row[3][3] > 0), 3),
        )
        for name, condition, predicate, height in conditions:
            expected = self.values([row for row in star if predicate(row)])
            kinds = ({"Inner Join": 3} if height == 2
                     else {"Inner Join": 1, "Left Join": 2})
            cases.append((name, query + condition, expected, height, kinds))
        return cases

    def new_connection(self):
        import taos

        # Match the framework endpoint and credentials without logging them.
        source = self.sql.cursor._connection
        return taos.connect(
            host=source._host, port=source._port, user=source._user,
            password=source._password, config=source._config,
        )

    # --- functional cases ---
    def do_jp_01(self):
        for count in (1, 2, 3, 4, 8, 9):
            names = "abcdefghi"[:count]
            expected = self.values(self.equi_rows(names, "ts"))
            for shape in ("chain", "star", "balanced"):
                query = self.join_query(names, "ts", shape)
                self.check_rows(query, expected)
                height = math.ceil(math.log2(count))
                if count == 9 and shape != "balanced":
                    height = count - 1
                original = None
                if count <= 3 or count == 9 or shape == "balanced":
                    original = self.input_shape(names, shape == "balanced")
                self.check_plan(query, count - 1, height, shape=original)

    def do_jp_02(self):
        self.check_explain_column_parsers()
        for key in ("k", "ts", "s", "tk"):
            query = self.join_query(key=key)
            self.check_rows(query, self.values(self.equi_rows("abcd", key)))
            self.check_plan(query, 3, 2)
        for expression in ("{a}.f={b}.f", "cast({a}.k as bigint)="
                           "cast({b}.k as bigint)"):
            query = self.join_query()
            for name in "bcd":
                query = query.replace(
                    f"a.k={name}.k", expression.format(a="a", b=name)
                )
            self.check_rows(query, self.values(self.equi_rows("abcd")))
            self.check_plan(query, 3, 3)
        query = self.join_query()
        for name in "bcd":
            query = query.replace(
                f"a.k={name}.k",
                f"(a.k={name}.k or (a.k is null and {name}.k is null))",
            )
        expected = self.values(self.equi_rows("abcd", null_equal=True))
        self.check_rows(query, expected)
        self.check_plan(query, 3, 3)
        self.separate_keys()
        for name, query, expected, height in self.equivalence_cases():
            assert expected, name
            self.check_rows(query, expected)
            plan = self.check_plan(query, 3, height)
            if name == "multiple-classes":
                self.check_eq_classes(plan)
        for key, types in (
            ("k", {name: ("bigint", "binary(32)") for name in "bcd"}),
            ("s", {name: ("int", "nchar(32)") for name in "bcd"}),
        ):
            self.prepare_data(column_types=types)
            query = self.join_query(key=key)
            self.check_rows(query, self.values(self.equi_rows("abcd", key)))
            self.check_plan(query, 3, 3)

    def do_jp_03(self):
        combinations = self.equi_rows("abcd")
        query = self.join_query()
        cases = (
            ("a.v>=102 and d.v<>403", lambda r: r[0][3] >= 102
             and r[3][3] != 403),
            ("a.v=101 or d.v=402", lambda r: r[0][3] == 101
             or r[3][3] == 402),
            ("not (a.v=101)", lambda r: r[0][3] != 101),
            ("b.v<c.v and 1=1", lambda r: r[1][3] < r[2][3]),
            ("a.v+c.v=b.v+d.v-200", lambda r: r[0][3] + r[2][3]
             == r[1][3] + r[3][3] - 200),
        )
        for condition, predicate in cases:
            expected = self.values(
                [row for row in combinations if predicate(row)]
            )
            self.check_rows(query + " where " + condition, expected)
            self.check_plan(query + " where " + condition, 3, 2)
        cross = (
            "select a.v,b.v,c.v,d.v from a cross join b "
            "join c on b.k=c.k join d on c.k=d.k"
        )
        expected = [(a[3],) + values for a in self.rows["a"]
                    for values in self.values(self.equi_rows("bcd"))]
        self.check_rows(cross, expected)

    def do_jp_04(self):
        expected = self.values(self.equi_rows("abcd"))
        query = self.join_query()
        self.check_rows(query, expected)
        assert [column[0] for column in self.sql.cursor.description] == [
            "v0", "v1", "v2", "v3"
        ]
        empty = self.join_query(["a", "b", "c", "empty"])
        self.check_rows(empty, [])
        self_query = (
            "select x.v,y.v,z.v,w.v from a x join a y on x.k=y.k "
            "join a z on x.k=z.k join a w on x.k=w.k"
        )
        self.check_rows(self_query, self.values(self.equi_rows("aaaa")))
        star = query.replace("a.v as v0", "a.*")
        self.sql.query(star)
        columns = [column[0] for column in self.sql.cursor.description]
        assert columns == ["ts", "k", "s", "v", "tk", "f", "payload",
                           "v1", "v2", "v3"]
        rows = list(self.sql.queryResult)
        projected = [(r[3],) + tuple(r[7:]) for r in rows]
        assert Counter(projected) == Counter(expected)
        by_value = {row[3]: row for row in self.rows["a"]}
        for row in rows:
            reference = by_value[row[3]]
            assert tuple(row[1:4]) == reference[1:4]
            assert tuple(row[5:7]) == reference[5:7]
            for column in (0, 4):
                actual = row[column]
                if hasattr(actual, "timestamp"):
                    actual = round(actual.timestamp() * 1000)
                assert actual == reference[column]
        ordered = sorted(expected)[:3]
        self.check_rows(query + " order by v0,v1,v2,v3 limit 3", ordered, True)
        row = self.rows["a"][0]
        self.rows["a"][0] = row[:3] + (None,) + row[4:]
        self.sql.execute(f"delete from a where ts={row[0]}")
        self.insert_rows("a", [self.rows["a"][0]])
        combinations = self.equi_rows("abcd")
        self.check_rows(query, self.values(combinations))
        selected = [r for r in combinations
                    if r[0][3] is None or r[3][3] == 402]
        self.check_rows(query + " where a.v is null or d.v=402",
                        self.values(selected))
        self.prepare_data()
        self.separate_keys()
        query, expected = self.reordered_query()
        assert expected
        self.check_rows(query, expected)
        columns = [column[0] for column in self.sql.cursor.description]
        assert columns == ["ts", "k", "s", "v", "tk", "f", "payload"] * 4
        plan = self.check_plan(query, 3, 2, split=True)
        root = self.join_nodes(self.plan_nodes(plan))[0]
        assert {self.table_names([child]) for child in root["children"]} == {
            frozenset("ac"), frozenset("bd")
        }, plan

    def do_jp_05(self):
        for names in ("abcd", "abcdefgh"):
            query = self.join_query(names, "ts", "balanced")
            self.check_rows(query, self.values(self.equi_rows(names, "ts")))
            first = self.check_plan(
                query, len(names) - 1, int(math.log2(len(names))), split=True,
                shape=self.input_shape(names, True),
                split_parents=len(names) // 2 - 1,
            )
            assert self.explain(query) == first

    def do_jp_06(self):
        for vgroups in (1, 4):
            for kind in ("normal", "child", "super", "mixed"):
                self.prepare_data(vgroups=vgroups, kind=kind)
                query = self.join_query(key="ts")
                expected = self.values(self.equi_rows("abcd", "ts"))
                self.check_rows(query, expected)
                self.check_plan(query, 3, 2, split=True)

    def do_jp_07(self):
        for hint, algorithm, key in (("hash_join", "Hash", "k"),
                                     ("merge_join", "Merge", "ts")):
            query = self.join_query(key=key, hint=hint)
            self.check_rows(query, self.values(self.equi_rows("abcd", key)))
            self.check_plan(query, 3, 2, split=True, algorithm=algorithm)

    def fetch_query(self, query, delay=0.0):
        start = time.perf_counter()
        cpu = time.process_time()
        self.sql.cursor.execute(query)
        first = self.sql.cursor.fetchmany(1024)
        first_ms = (time.perf_counter() - start) * 1000
        rows = list(first)
        while first:
            if delay:
                time.sleep(delay)
            first = self.sql.cursor.fetchmany(1024)
            rows.extend(first)
        return rows, {
            "total_ms": (time.perf_counter() - start) * 1000,
            "first_ms": first_ms,
            "client_cpu_ms": (time.process_time() - cpu) * 1000,
        }

    def runtime_queries(self):
        select = "select a.v as v0,b.v as v1,c.v as v2,d.v as v3 from "
        inputs = "(a join b on a.ts=b.ts) left {} join "
        inputs += "(c join d on c.ts=d.ts)"
        window = select.replace(" from ", ",a.k as group_key from ")
        # WINDOW JOIN permits grouping only in an enclosing SELECT.
        window += inputs.format("window")
        window += " window_time(a.ts,c.ts) window_offset(0s,0s) jlimit 1"
        window = ("select q.v0,q.v1,q.v2,q.v3 from (" + window +
                  ") q partition by q.group_key")
        cases = [
            ("hash", self.join_query(shape="balanced", hint="hash_join")),
            ("merge", self.join_query(key="ts", shape="balanced",
                                       hint="merge_join")),
            ("asof", select + inputs.format("asof") + " on a.ts>=c.ts"),
            ("window", window),
        ]
        # Group boundaries keep the root from coalescing every small block.
        return [(kind, query if kind == "window" else
                 query + " partition by a.k") for kind, query in cases]

    def check_runtime_plan(self, kind, query):
        algorithm = {"hash": "Hash", "merge": "Merge"}.get(kind)
        plan = self.check_plan(query, 3, 2, split=True, algorithm=algorithm)
        if kind in ("asof", "window"):
            self.check_time_keys(plan, "a", "c", "ts")

    def check_runtime_result(self, cursor, count, kind):
        seen = bytearray(count * (8 if kind == "hash" else 1))
        total = 0
        while True:
            batch = cursor.fetchmany(1024)
            if not batch:
                break
            for row in batch:
                assert len(row) == 4, row
                indices = [value - (column + 1) * 1000000 - 1
                           for column, value in enumerate(row)]
                assert all(0 <= index < count for index in indices), row
                if kind == "hash":
                    group = indices[0] // 2
                    assert all(index // 2 == group for index in indices), row
                    ordinal = group * 16
                    ordinal += sum((index % 2) << column
                                   for column, index in enumerate(indices))
                else:
                    assert len(set(indices)) == 1, row
                    ordinal = indices[0]
                assert not seen[ordinal], f"Duplicate output: {row}"
                seen[ordinal] = 1
                total += 1
        assert total == len(seen) and all(seen), total
        return total

    def do_jp_08(self):
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        count = int(os.getenv("JOIN_PARALLEL_BUFFER_ROWS", "8192"))
        assert count >= 8192 and count % 2 == 0
        self.prepare_data(count, "duplicate", names="abcd")
        self.runtime_evidence = []
        for kind, query in self.runtime_queries():
            self.check_runtime_plan(kind, query)
            connection = self.new_connection()
            cursor = None
            completed = False
            try:
                cursor = connection.cursor()
                cursor.execute(f"use {self.database}")
                # Older native bindings pass req_id through a signed C int.
                query_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
                observer = JoinRuntimeLogs(query_id)
                cursor.execute(query, req_id=query_id)
                observer.wait_paused()
                observer.mark_resume()
                total = self.check_runtime_result(cursor, count, kind)
                proof = observer.check_resumed()
                if kind == "hash":
                    proof["producers"] = observer.check_producers()
                proof.update(query_id=query_id, input_rows=count, rows=total,
                             kind=kind)
                self.runtime_evidence.append(proof)
                completed = True
                self.log.info(f"JP-08 {kind} full/pause/resume: {proof}")
            finally:
                try:
                    if cursor is not None and not completed:
                        cursor.stop_query()
                finally:
                    try:
                        if cursor is not None:
                            cursor.close()
                    finally:
                        connection.close()

    def do_jp_09(self):
        query = self.join_query(key="f")
        expected = self.values(self.equi_rows("abcd", "f"))
        plan = self.check_plan(query, 3, 3, shape=self.input_shape("abcd"))
        for _ in range(3):
            self.check_rows(query, expected)
            assert self.explain(query) == plan
        query = self.join_query("abcdefghi", "ts")
        self.check_rows(query, self.values(self.equi_rows("abcdefghi", "ts")))
        self.check_plan(query, 8, 8, shape=self.input_shape("abcdefghi"))

    def cancel_query(self):
        marker = f"jp_cancel_{uuid.uuid4().hex[:12]}"
        query = self.join_query(hint="hash_join").replace("v0", marker, 1)
        connection = self.new_connection()
        done, outcome = threading.Event(), {}
        cleanup_lock = threading.Lock()

        def consume():
            cursor = None
            try:
                cursor = connection.cursor()
                outcome["cursor"] = cursor
                cursor.execute(f"use {self.database}")
                cursor.execute(query)
                while cursor.fetchmany(256):
                    pass
                outcome["completed"] = True
            except Exception as error:
                outcome["error"] = str(error).lower()
            finally:
                try:
                    with cleanup_lock:
                        try:
                            if cursor is not None:
                                cursor.close()
                        finally:
                            connection.close()
                finally:
                    done.set()

        worker = threading.Thread(target=consume, daemon=True)
        worker.start()
        query_id = None
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and not done.is_set():
                self.sql.query(
                    "select kill_id,sql from performance_schema.perf_queries"
                )
                matches = [row[0] for row in self.sql.queryResult
                           if marker in str(row[1])]
                if matches:
                    query_id = str(matches[0])
                    break
                done.wait(0.1)
            assert query_id is not None, outcome
            start = time.perf_counter()
            self.sql.execute(f"kill query '{query_id}'")
            assert done.wait(20), "Query did not exit after KILL QUERY"
            elapsed = (time.perf_counter() - start) * 1000
            error = outcome.get("error", "")
            assert "cancel" in error or "kill" in error, outcome
            deadline = time.monotonic() + 20
            while True:
                self.sql.query(
                    "select sql from performance_schema.perf_queries"
                )
                if not any(marker in str(row[0])
                           for row in self.sql.queryResult):
                    break
                assert time.monotonic() < deadline, "Query record remained"
                time.sleep(0.1)
            return {
                "cancel_ms": elapsed,
                "record_gone_ms": (time.perf_counter() - start) * 1000,
            }
        finally:
            try:
                if not done.is_set() and query_id:
                    self.sql.execute(f"kill query '{query_id}'")
            finally:
                if not done.is_set():
                    with cleanup_lock:
                        cursor = outcome.get("cursor")
                        if cursor is not None:
                            cursor.stop_query()
                worker.join(timeout=20)
                assert not worker.is_alive(), "Cancelled query worker leaked"

    def do_jp_10(self):
        from taos.cinterface import taos_errno
        from taos.error import Error
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        count = int(os.getenv("JOIN_PARALLEL_BUFFER_ROWS", "8192"))
        assert count >= 8192 and count % 2 == 0
        self.prepare_data(count, "duplicate", names="abcd")
        self.lifecycle_evidence = []
        used_ids = set()
        for kind, query in self.runtime_queries():
            self.check_runtime_plan(kind, query)
            for action in ("stop", "close"):
                connection = self.new_connection()
                cursor = None
                try:
                    cursor = connection.cursor()
                    cursor.execute(f"use {self.database}")
                    query_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
                    assert query_id not in used_ids
                    used_ids.add(query_id)
                    observer = JoinRuntimeLogs(query_id)
                    cursor.execute(query, req_id=query_id)
                    observer.wait_paused()
                    observer.mark_cancel()
                    cancel_code = None
                    if action == "stop":
                        cursor.stop_query()
                        # Row fetch checks killed state before scheduling work.
                        try:
                            row = cursor.fetchone()
                        except Error as error:
                            cancel_code = error.errno & 0xffff
                        else:
                            assert row is None, "Output after cancel"
                            cancel_code = taos_errno(cursor._result) & 0xffff
                        assert cancel_code in (0x020f, 0x022d), cancel_code
                        # Close must not mask a stop that leaves tasks alive.
                        proof = observer.check_cancelled()
                        cursor.close()
                    else:
                        cursor.close()
                        proof = observer.check_cancelled()
                    cursor = None
                    followup_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff
                    assert followup_id and followup_id not in used_ids
                    used_ids.add(followup_id)
                    cursor = connection.cursor()
                    cursor.execute(query, req_id=followup_id)
                    total = self.check_runtime_result(cursor, count, kind)
                    proof.update(kind=kind, action=action, query_id=query_id,
                                 input_rows=count, cancel_code=cancel_code,
                                 followup_query_id=followup_id,
                                 followup_rows=total)
                    self.lifecycle_evidence.append(proof)
                    self.log.info(f"JP-10 {kind}/{action} cleanup: {proof}")
                finally:
                    try:
                        if cursor is not None:
                            cursor.close()
                    finally:
                        connection.close()
        self.prepare_data()
        self.check_rows(self.join_query(), self.values(self.equi_rows("abcd")))
        query = self.join_query() + " order by v0,v1,v2,v3 limit 3"
        self.check_rows(query, sorted(self.values(self.equi_rows("abcd")))[:3],
                        ordered=True)

    def do_jp_10_inflight(self):
        count = 8192
        self.prepare_data(count, "duplicate", names="abcd")
        self.inflight_evidence = []
        first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
        for index, (kind, query) in enumerate(self.runtime_queries()):
            self.check_runtime_plan(kind, query)
            query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
            followup_id = query_id % 0x7fffffff + 1
            proof = self.run_inflight_query(kind, query, count, query_id,
                                            followup_id)
            self.inflight_evidence.append(proof)
            self.log.info(f"JP-10 {kind} inflight cleanup: {proof}")

    def run_inflight_query(self, kind, query, count, query_id, followup_id):
        import ctypes
        from taos.cinterface import _libtaos
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        gate = getattr(self, "fetch_gate_dir", None)
        assert gate is not None, "Runner must provide its private fetch gate"
        gate = Path(gate).resolve(strict=True)
        configured = Path(os.environ["JOIN_PARALLEL_FETCH_GATE_DIR"])
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        assert gate.is_dir() and gate == configured.resolve(strict=True)
        assert gate == Path(manifest["fetch_gate_dir"]).resolve(strict=True)
        for marker in ("arm", "held", "release"):
            assert not (gate / marker).exists(), f"Stale fetch gate: {marker}"

        # Independent function objects avoid changing taospy's native bindings.
        # Its query wrapper also frees error results before raising an error.
        raw_query = _libtaos["taos_query_with_reqid"]
        raw_query.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int64)
        raw_query.restype = ctypes.c_void_p
        raw_fetch = _libtaos["taos_fetch_row"]
        raw_fetch.argtypes = (ctypes.c_void_p,)
        raw_fetch.restype = ctypes.c_void_p
        raw_errno = _libtaos["taos_errno"]
        raw_errno.argtypes = (ctypes.c_void_p,)
        raw_errno.restype = ctypes.c_int
        raw_free = _libtaos["taos_free_result"]
        raw_free.argtypes = (ctypes.c_void_p,)
        raw_free.restype = None
        returned = threading.Event()
        allow_cleanup = threading.Event()
        finished = threading.Event()
        outcome = {}

        def execute():
            connection = None
            cursor = None
            result = None
            try:
                try:
                    connection = self.new_connection()
                    cursor = connection.cursor()
                    cursor.execute(f"use {self.database}")
                    result = raw_query(connection._conn, query.encode(),
                                       query_id)
                    assert result, "Native query returned a NULL result"
                    outcome["result_retained"] = True
                    code = raw_errno(result) & 0xffff
                    while code == 0:
                        row = raw_fetch(result)
                        code = raw_errno(result) & 0xffff
                        if not row:
                            break
                    outcome["cancel_code"] = code
                    assert code in (0x020f, 0x022d), (
                        f"Query completed without cancellation: {code:#x}"
                    )
                except BaseException as error:
                    outcome["query_error"] = repr(error)
                finally:
                    returned.set()
                assert allow_cleanup.wait(60), "Result retention timed out"
                if result is not None:
                    raw_free(result)
                    result = None
                    outcome["result_freed"] = True
                if outcome.get("run_followup"):
                    cursor.close()
                    cursor = connection.cursor()
                    cursor.execute(query, req_id=followup_id)
                    outcome["followup_rows"] = self.check_runtime_result(
                        cursor, count, kind)
            except BaseException as error:
                outcome["worker_error"] = repr(error)
            finally:
                cleanup = []
                if result is not None:
                    cleanup.append(lambda: raw_free(result))
                if cursor is not None:
                    cleanup.append(cursor.close)
                if connection is not None:
                    cleanup.append(connection.close)
                try:
                    for close in cleanup:
                        try:
                            close()
                        except BaseException as error:
                            outcome.setdefault("cleanup_errors", []).append(
                                repr(error))
                finally:
                    finished.set()

        released = False

        def release_gate():
            nonlocal released
            if not released:
                descriptor = os.open(gate / "release",
                                     os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                     0o600)
                released = True
                os.close(descriptor)

        worker = threading.Thread(target=execute, daemon=True)
        started = False
        try:
            descriptor = os.open(gate / "arm",
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as marker:
                marker.write(f"{query_id}\n")
            observer = JoinRuntimeLogs(query_id)
            worker.start()
            started = True
            observer.wait_inflight_held()
            kill_mapping = observer.wait_kill_id()
            kill_id = kill_mapping["kill_id"]
            assert re.fullmatch(r"[0-9a-fA-F]+:[0-9a-fA-F]+", kill_id), kill_id
            observer.mark_inflight_cancel()
            self.sql.execute(f"kill query '{kill_id}'")
            assert returned.wait(20), "Native query did not return after KILL"
            assert "query_error" not in outcome, outcome
            assert outcome.get("cancel_code") in (0x020f, 0x022d), outcome
            assert outcome.get("result_retained"), outcome
            assert not outcome.get("result_freed"), outcome
            observer.wait_inflight_detached()
            observer.mark_inflight_release()
            release_gate()
            proof = observer.check_inflight_cancelled()
            assert not outcome.get("result_freed"), outcome
            outcome["run_followup"] = True
            allow_cleanup.set()
            assert finished.wait(20), "Same-connection follow-up did not finish"
            worker.join(timeout=1)
            assert not worker.is_alive(), "Native query worker remained alive"
            assert "worker_error" not in outcome, outcome
            assert "cleanup_errors" not in outcome, outcome
            assert outcome.get("result_freed"), outcome
            expected = count * (8 if kind == "hash" else 1)
            assert outcome.get("followup_rows") == expected, outcome
            proof.update(kind=kind, action="kill", query_id=query_id,
                         input_rows=count, cancel_code=outcome["cancel_code"],
                         kill_id=kill_id, kill_mapping=kill_mapping,
                         result_retained=True,
                         followup_query_id=followup_id,
                         followup_rows=outcome["followup_rows"])
        finally:
            try:
                release_gate()
            finally:
                allow_cleanup.set()
                if started:
                    worker.join(timeout=20)
                    assert not worker.is_alive(), "Native query worker leaked"

        # Every callback and the worker are gone before rearming this gate.
        assert (gate / "arm").read_text() == f"{query_id}\n"
        markers = [gate / name for name in ("arm", "held", "release")]
        assert all(marker.is_file() for marker in markers), markers
        for marker in markers:
            marker.unlink()
        return proof

    def match_state_query(self, kind, count, scenario="equal"):
        assert kind in ("asof", "window"), kind
        columns = ",".join(f"{name}.v" for name in "abcd")
        columns += "," + ",".join(f"{name}.payload" for name in "abcd")
        query = (f"select {columns} from (a join b on a.ts=b.ts) "
                 f"left {kind} join (c join d on c.ts=d.ts)")
        if kind == "asof":
            operator = {"strict-backward": ">", "forward": "<=",
                        "strict-forward": "<"}.get(scenario, ">=")
            query += f" on a.tk{operator}c.tk"
        else:
            offsets = "0s,0s" if scenario == "equal" else "-1s,1s"
            query += f" window_time(a.tk,c.tk) window_offset({offsets})"
        query += f" jlimit {count}"
        if scenario == "null-filter":
            query += " where c.v is null or (a.v+c.v)%2=0"
        return query

    def prepare_match_data(self, kind, scenario, count):
        self.prepare_data(count, "duplicate", names="abcd")
        base = 1750046400000
        for name in "abcd":
            rows = []
            for index, row in enumerate(self.rows[name]):
                stamp, payload = base, row[6]
                if name in "cd":
                    if scenario == "strict-backward":
                        stamp += {0: 0, 1: 1, 2: -1000}.get(index, -1)
                    elif scenario in ("forward", "strict-forward"):
                        stamp += {0: 0, 1: -1, 2: 1000}.get(index, 1)
                    elif scenario == "range":
                        stamp += {0: -1001, 1: -1000, 2: 1000,
                                  3: 1001}.get(index, 0)
                if scenario == "null-filter":
                    if index == 0:
                        stamp = None
                    elif name in "ab" and index == 1:
                        stamp -= 5000
                    if index == (2 if name in "ab" else 3):
                        payload = None
                rows.append(row[:4] + (stamp, row[5], payload))
            self.replace_rows(name, rows)

    def match_expected_rows(self, kind, scenario):
        left = self.inner_rows([("a", 0, "b", 0)], "ab")
        right = self.inner_rows([("c", 0, "d", 0)], "cd")
        comparisons = {
            "strict-backward": lambda probe, build: probe > build,
            "forward": lambda probe, build: probe <= build,
            "strict-forward": lambda probe, build: probe < build,
        }
        compare = comparisons.get(scenario, lambda probe, build: probe >= build)
        expected = []
        for probe in left:
            stamp = probe[0][4]
            if stamp is None:
                continue
            candidates = []
            for build in right:
                other = build[0][4]
                if other is None:
                    continue
                if kind == "asof":
                    matches = compare(stamp, other)
                else:
                    width = 0 if scenario == "equal" else 1000
                    matches = stamp - width <= other <= stamp + width
                if matches:
                    candidates.append(build)
            # JLIMIT covers every eligible row, independent of tie ordering.
            assert len(candidates) <= len(self.rows["c"])
            for build in candidates or [(None, None)]:
                if (scenario == "null-filter" and build[0] is not None and
                        (probe[0][3] + build[0][3]) % 2 != 0):
                    continue
                joined = probe + build
                expected.append(tuple(row[3] if row else None for row in joined)
                                + tuple(row[6] if row else None
                                        for row in joined))
        return expected

    def check_match_rows(self, rows, count, expected=None):
        if expected is not None:
            remaining = Counter(expected)
            total = 0
            for row in rows:
                row = tuple(row)
                assert remaining[row] > 0, f"Unexpected matched output: {row}"
                remaining[row] -= 1
                total += 1
            assert total == len(expected) and not any(remaining.values()), (
                f"Incomplete matched output: {total}/{len(expected)}")
            return total
        seen = bytearray(count * count)
        total = 0
        for row in rows:
            assert len(row) == 8, row
            indices = [value - (column + 1) * 1000000 - 1
                       for column, value in enumerate(row[:4])]
            assert all(0 <= index < count for index in indices), row
            assert indices[0] == indices[1] and indices[2] == indices[3], row
            assert all(value == "x" * 64 for value in row[4:]), row
            ordinal = indices[0] * count + indices[2]
            assert not seen[ordinal], f"Duplicate matched output: {row}"
            seen[ordinal] = 1
            total += 1
        assert total == len(seen) and all(seen), total
        return total

    def do_jp_10_match(self):
        from new_test_framework.utils.join_runtime_logs import MATCH_SCENARIOS

        count = 128
        self.match_evidence = []
        first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
        index = 0
        for scenario, kind, total in MATCH_SCENARIOS:
            self.prepare_match_data(kind, scenario, count)
            query = self.match_state_query(kind, count, scenario)
            expected = self.match_expected_rows(kind, scenario)
            assert len(expected) == total, (scenario, len(expected), total)
            plan = self.check_plan(query, 3, 2, split=True)
            self.check_time_keys(plan, "a", "c", "tk")
            actions = ("resume", "kill") if scenario == "equal" else ("resume",)
            for action in actions:
                query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
                followup_id = query_id % 0x7fffffff + 1
                proof = self.run_match_query(kind, action, query, count,
                                             query_id, followup_id, scenario,
                                             expected)
                self.match_evidence.append(proof)
                self.log.info(f"JP-10 {kind}/{scenario}/{action}: {proof}")
                index += 1

    def run_match_query(self, kind, action, query, count, query_id,
                        followup_id, scenario="equal", expected=None):
        import ctypes
        from taos.cinterface import _libtaos
        from taos.result import TaosResult
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        gate = getattr(self, "match_gate_dir", None)
        assert gate is not None, "Runner must provide its private match gate"
        gate = Path(gate).resolve(strict=True)
        configured = Path(os.environ["JOIN_PARALLEL_MATCH_GATE_DIR"])
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        assert gate.is_dir() and gate == configured.resolve(strict=True)
        assert gate == Path(manifest["match_gate_dir"]).resolve(strict=True)
        for marker in ("arm", "held", "release"):
            assert not (gate / marker).exists(), f"Stale match gate: {marker}"

        raw_query = _libtaos["taos_query_with_reqid"]
        raw_query.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int64)
        raw_query.restype = ctypes.c_void_p
        raw_fetch = _libtaos["taos_fetch_row"]
        raw_fetch.argtypes = (ctypes.c_void_p,)
        raw_fetch.restype = ctypes.c_void_p
        raw_errno = _libtaos["taos_errno"]
        raw_errno.argtypes = (ctypes.c_void_p,)
        raw_errno.restype = ctypes.c_int
        raw_free = _libtaos["taos_free_result"]
        raw_free.argtypes = (ctypes.c_void_p,)
        raw_free.restype = None
        returned = threading.Event()
        allow_result_free = threading.Event()
        result_freed = threading.Event()
        allow_followup = threading.Event()
        finished = threading.Event()
        outcome = {}
        expected_count = (len(expected) if expected is not None
                          else count * count)

        def execute():
            connection = None
            cursor = None
            result = None
            try:
                try:
                    connection = self.new_connection()
                    cursor = connection.cursor()
                    cursor.execute(f"use {self.database}")
                    result = raw_query(connection._conn, query.encode(),
                                       query_id)
                    assert result, "Native query returned a NULL result"
                    outcome["result_retained"] = True
                    code = raw_errno(result) & 0xffff
                    if action == "resume":
                        assert code == 0, code
                        view = TaosResult(result, close_after=False)
                        try:
                            outcome["rows"] = self.check_match_rows(
                                view.fetch_all(), count, expected)
                        finally:
                            view.close()
                        code = raw_errno(result) & 0xffff
                        assert code == 0, code
                    else:
                        while code == 0:
                            row = raw_fetch(result)
                            code = raw_errno(result) & 0xffff
                            if not row:
                                break
                        assert code in (0x020f, 0x022d), (
                            f"Query completed without cancellation: {code:#x}"
                        )
                    outcome["cancel_code"] = code
                except BaseException as error:
                    outcome["query_error"] = repr(error)
                finally:
                    returned.set()
                assert allow_result_free.wait(60), "Result retention timed out"
                if result is not None:
                    raw_free(result)
                    result = None
                    outcome["result_freed"] = True
                result_freed.set()
                assert allow_followup.wait(60), "Cleanup proof timed out"
                if outcome.get("run_followup"):
                    cursor.close()
                    cursor = connection.cursor()
                    cursor.execute(query, req_id=followup_id)
                    rows = iter(lambda: cursor.fetchmany(1024), [])
                    outcome["followup_rows"] = self.check_match_rows(
                        (row for batch in rows for row in batch), count,
                        expected)
            except BaseException as error:
                outcome["worker_error"] = repr(error)
            finally:
                cleanup = []
                if result is not None:
                    cleanup.append(lambda: raw_free(result))
                if cursor is not None:
                    cleanup.append(cursor.close)
                if connection is not None:
                    cleanup.append(connection.close)
                try:
                    for close in cleanup:
                        try:
                            close()
                        except BaseException as error:
                            outcome.setdefault("cleanup_errors", []).append(
                                repr(error))
                finally:
                    finished.set()

        released = False

        def release_gate():
            nonlocal released
            if not released:
                descriptor = os.open(gate / "release",
                                     os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                     0o600)
                released = True
                os.close(descriptor)

        worker = threading.Thread(target=execute, daemon=True)
        started = False
        kill_mapping = None
        try:
            descriptor = os.open(gate / "arm",
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as marker:
                marker.write(f"{query_id}\n")
            observer = JoinRuntimeLogs(query_id)
            worker.start()
            started = True
            observer.wait_match_held(kind)
            if action == "kill":
                kill_mapping = observer.wait_kill_id()
                observer.mark_match_cancel()
                self.sql.execute(f"kill query '{kill_mapping['kill_id']}'")
                assert returned.wait(20), "Query did not return after KILL"
                assert "query_error" not in outcome, outcome
                assert outcome.get("cancel_code") in (0x020f, 0x022d), outcome
                assert outcome.get("result_retained"), outcome
                assert not outcome.get("result_freed"), outcome
            else:
                observer.mark_match_resume()
            observer.mark_match_release()
            release_gate()
            assert returned.wait(20), "Native match query did not return"
            assert "query_error" not in outcome, outcome
            if action == "resume":
                assert outcome.get("rows") == expected_count, outcome
                allow_result_free.set()
                assert result_freed.wait(20), "Completed result did not free"
                proof = observer.check_match_resumed()
            else:
                proof = observer.check_match_cancelled()
                assert not outcome.get("result_freed"), outcome
                allow_result_free.set()
                assert result_freed.wait(20), "Cancelled result did not free"
            outcome["run_followup"] = True
            allow_followup.set()
            assert finished.wait(20), "Same-connection follow-up did not finish"
            worker.join(timeout=1)
            assert not worker.is_alive(), "Native query worker remained alive"
            assert "worker_error" not in outcome, outcome
            assert "cleanup_errors" not in outcome, outcome
            assert outcome.get("result_freed"), outcome
            assert outcome.get("followup_rows") == expected_count, outcome
            proof.update(kind=kind, action=action, query_id=query_id,
                         scenario=scenario, input_rows=count,
                         expected_rows=expected_count,
                         followup_query_id=followup_id,
                         followup_rows=outcome["followup_rows"])
            if action == "kill":
                proof.update(cancel_code=outcome["cancel_code"],
                             result_retained=True, kill_mapping=kill_mapping,
                             kill_id=kill_mapping["kill_id"])
            else:
                proof.update(rows=outcome["rows"], result_freed=True)
        finally:
            try:
                release_gate()
            finally:
                allow_result_free.set()
                allow_followup.set()
                if started:
                    worker.join(timeout=20)
                    assert not worker.is_alive(), "Native query worker leaked"

        assert (gate / "arm").read_text() == f"{query_id}\n"
        markers = [gate / name for name in ("arm", "held", "release")]
        assert all(marker.is_file() for marker in markers), markers
        for marker in markers:
            marker.unlink()
        return proof

    def run_timeout_worker(self, state):
        import ctypes
        from taos.cinterface import _libtaos

        raw_query = _libtaos["taos_query_with_reqid"]
        raw_query.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int64)
        raw_query.restype = ctypes.c_void_p
        raw_fetch = _libtaos["taos_fetch_row"]
        raw_fetch.argtypes = (ctypes.c_void_p,)
        raw_fetch.restype = ctypes.c_void_p
        raw_errno = _libtaos["taos_errno"]
        raw_errno.argtypes = (ctypes.c_void_p,)
        raw_errno.restype = ctypes.c_int
        raw_free = _libtaos["taos_free_result"]
        raw_free.argtypes = (ctypes.c_void_p,)
        raw_free.restype = None
        outcome = state["outcome"]
        connection = None
        cursor = None
        result = None
        try:
            try:
                connection = self.new_connection()
                cursor = connection.cursor()
                cursor.execute(f"use {self.database}")
                started = time.monotonic()
                result = raw_query(connection._conn, state["query"].encode(),
                                   state["query_id"])
                assert result, "Native query returned a NULL result"
                outcome["result_retained_at_timeout"] = True
                code = raw_errno(result) & 0xffff
                while code == 0:
                    row = raw_fetch(result)
                    code = raw_errno(result) & 0xffff
                    if not row:
                        break
                outcome["elapsed_seconds"] = time.monotonic() - started
                outcome["cancel_code"] = code
                assert code == 0x0019, (
                    f"Expected RPC timeout, received: {code:#x}"
                )
                assert outcome["elapsed_seconds"] >= state["read_timeout"]
            except BaseException as error:
                outcome["query_error"] = repr(error)
            finally:
                state["returned"].set()
            assert state["allow_result_free"].wait(120), (
                "Timeout result retention timed out"
            )
            if result is not None:
                raw_free(result)
                result = None
                outcome["result_freed"] = True
            state["result_freed"].set()
            assert state["allow_followup"].wait(60), "Cleanup proof timed out"
            if outcome.get("run_followup"):
                cursor.close()
                cursor = connection.cursor()
                cursor.execute(state["query"], req_id=state["followup_id"])
                outcome["followup_rows"] = self.check_runtime_result(
                    cursor, state["count"], state["kind"])
        except BaseException as error:
            outcome["worker_error"] = repr(error)
        finally:
            cleanup = []
            if result is not None:
                cleanup.append(lambda: raw_free(result))
            if cursor is not None:
                cleanup.append(cursor.close)
            if connection is not None:
                cleanup.append(connection.close)
            try:
                for close in cleanup:
                    try:
                        close()
                    except BaseException as error:
                        outcome.setdefault("cleanup_errors", []).append(
                            repr(error))
            finally:
                state["finished"].set()

    def release_timeout_gate(self, state):
        if not state["released"]:
            descriptor = os.open(state["gate"] / "release",
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            state["released"] = True
            os.close(descriptor)

    def do_jp_10_timeout(self):
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        count, read_timeout = 8192, 64
        self.prepare_data(count, "duplicate", names="abcd")
        queries = self.runtime_queries()
        for kind, query in queries:
            self.check_runtime_plan(kind, query)
        root = getattr(self, "timeout_gate_dir", None)
        assert root is not None, "Runner must provide its private timeout gate"
        root = Path(root).resolve(strict=True)
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        configured = Path(os.environ["JOIN_PARALLEL_TIMEOUT_GATE_DIR"])
        assert root.is_dir() and root == configured.resolve(strict=True)
        assert root == Path(manifest["timeout_gate_dir"]).resolve(strict=True)
        assert root.parent == Path(manifest["data_root"]).resolve(strict=True)
        assert manifest["read_timeout"] == read_timeout, manifest
        assert not any(root.iterdir()), "Timeout gate root must be empty"
        self.timeout_evidence = []
        first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
        states = []
        try:
            for index, (kind, query) in enumerate(queries):
                query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
                gate = root / str(query_id)
                gate.mkdir(mode=0o700)
                descriptor = os.open(gate / "arm",
                                     os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                     0o600)
                with os.fdopen(descriptor, "w") as marker:
                    marker.write(f"{query_id}\n")
                state = {"kind": kind, "query": query, "count": count,
                         "query_id": query_id,
                         "followup_id": query_id % 0x7fffffff + 1,
                         "read_timeout": read_timeout, "gate": gate,
                         "observer": JoinRuntimeLogs(query_id), "outcome": {},
                         "returned": threading.Event(),
                         "allow_result_free": threading.Event(),
                         "result_freed": threading.Event(),
                         "allow_followup": threading.Event(),
                         "finished": threading.Event(), "released": False,
                         "started": False}
                state["worker"] = threading.Thread(
                    target=self.run_timeout_worker, args=(state,), daemon=True)
                states.append(state)
            deadline = time.monotonic() + read_timeout + 35
            for state in states:
                state["worker"].start()
                state["started"] = True
            for state in states:
                state["observer"].wait_timeout_held(state["kind"])
                assert not state["returned"].is_set(), state["outcome"]
                state["observer"].mark_timeout()
            # No SQL is sent while the four independent RPC timers expire.
            for state in states:
                assert state["returned"].wait(
                    max(0, deadline - time.monotonic())), (
                        "Native query did not return after readTimeout"
                    )
                outcome = state["outcome"]
                assert "query_error" not in outcome, outcome
                assert outcome.get("cancel_code") == 0x0019, outcome
                assert outcome.get("result_retained_at_timeout"), outcome
                assert not outcome.get("result_freed"), outcome
                state["observer"].wait_rpc_timeout()
            # Error results own scheduler jobs until normal API release.
            for state in states:
                state["allow_result_free"].set()
            for state in states:
                assert state["result_freed"].wait(15), (
                    "Timeout result did not free"
                )
                assert state["outcome"].get("result_freed"), state["outcome"]
                state["observer"].wait_rpc_drop()
            for state in states:
                state["observer"].mark_timeout_release()
                self.release_timeout_gate(state)
            for state in states:
                state["proof"] = state["observer"].check_timeout_cleaned()
            for state in states:
                state["outcome"]["run_followup"] = True
                state["allow_followup"].set()
            for state in states:
                assert state["finished"].wait(20), (
                    "Same-connection follow-up did not finish"
                )
                state["worker"].join(timeout=1)
                outcome = state["outcome"]
                assert not state["worker"].is_alive(), "Query worker remained"
                assert "worker_error" not in outcome, outcome
                assert "cleanup_errors" not in outcome, outcome
                expected = count * (8 if state["kind"] == "hash" else 1)
                assert outcome.get("followup_rows") == expected, outcome
            for state in states:
                proof, outcome = state["proof"], state["outcome"]
                proof.update(kind=state["kind"], action="timeout",
                             query_id=state["query_id"], input_rows=count,
                             read_timeout=read_timeout, cancel_code=0x0019,
                             elapsed_seconds=outcome["elapsed_seconds"],
                             result_retained_at_timeout=True,
                             result_freed_before_release=True,
                             followup_query_id=state["followup_id"],
                             followup_rows=outcome["followup_rows"])
                self.timeout_evidence.append(proof)
                self.log.info(f"JP-10 {state['kind']} RPC timeout: {proof}")
        finally:
            cleanup_errors = []
            for state in states:
                try:
                    self.release_timeout_gate(state)
                except BaseException as error:
                    cleanup_errors.append(repr(error))
            for state in states:
                state["allow_result_free"].set()
                state["allow_followup"].set()
            deadline = time.monotonic() + 20
            alive = []
            for state in states:
                if state["started"]:
                    state["worker"].join(
                        timeout=max(0, deadline - time.monotonic()))
                    if state["worker"].is_alive():
                        alive.append(state["kind"])
            assert not alive, f"Native timeout workers leaked: {alive}"
            assert not cleanup_errors, cleanup_errors
        for state in states:
            gate = state["gate"]
            assert (gate / "arm").read_text() == f"{state['query_id']}\n"
            markers = [gate / name for name in ("arm", "held", "release")]
            assert all(marker.is_file() for marker in markers), markers
            for marker in markers:
                marker.unlink()
            gate.rmdir()

    def failure_response_policy(self):
        self.sql.query("show dnode 1 variables like 'queryRspPolicy'")
        values = [int(row[2]) for row in self.sql.queryResult
                  if len(row) > 2 and row[1] == "queryRspPolicy"]
        assert len(values) == 1 and values[0] in (0, 1), values
        return values[0]

    def set_failure_response_policy(self, policy):
        assert policy in (0, 1), policy
        self.sql.execute(f"alter all dnodes 'queryRspPolicy' '{policy}'")
        assert self.failure_response_policy() == policy

    def write_failure_command(self, state, task_id, command):
        assert type(task_id) is int and task_id > 0, task_id
        assert command in ("fail", "pass"), command
        if task_id not in state["commands"]:
            path = state["gate"] / f"release.{task_id}"
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                 0o600)
            with os.fdopen(descriptor, "w") as marker:
                marker.write(command + "\n")
            state["commands"][task_id] = command

    def run_failure_worker(self, state):
        import ctypes
        from taos.cinterface import _libtaos

        raw_query = _libtaos["taos_query_with_reqid"]
        raw_query.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int64)
        raw_query.restype = ctypes.c_void_p
        raw_fetch = _libtaos["taos_fetch_row"]
        raw_fetch.argtypes = (ctypes.c_void_p,)
        raw_fetch.restype = ctypes.c_void_p
        raw_errno = _libtaos["taos_errno"]
        raw_errno.argtypes = (ctypes.c_void_p,)
        raw_errno.restype = ctypes.c_int
        raw_free = _libtaos["taos_free_result"]
        raw_free.argtypes = (ctypes.c_void_p,)
        raw_free.restype = None
        outcome = state["outcome"]
        connection = None
        cursor = None
        result = None
        try:
            try:
                connection = self.new_connection()
                cursor = connection.cursor()
                cursor.execute(f"use {self.database}")
                result = raw_query(connection._conn, state["query"].encode(),
                                   state["query_id"])
                assert result, "Native query returned a NULL result"
                outcome["result_retained_at_failure"] = True
                code = raw_errno(result) & 0xffff
                while code == 0:
                    row = raw_fetch(result)
                    code = raw_errno(result) & 0xffff
                    if not row:
                        break
                outcome["error_code"] = code
                assert code == 0x0732, (
                    f"Expected producer execution error, received: {code:#x}"
                )
            except BaseException as error:
                outcome["query_error"] = repr(error)
            finally:
                state["returned"].set()
            assert state["allow_result_free"].wait(60), (
                "Producer error result retention timed out"
            )
            if result is not None:
                raw_free(result)
                result = None
                outcome["result_freed"] = True
            state["result_freed"].set()
            assert state["allow_followup"].wait(60), "Cleanup proof timed out"
            if outcome.get("run_followup"):
                cursor.close()
                cursor = connection.cursor()
                cursor.execute(state["query"], req_id=state["followup_id"])
                outcome["followup_rows"] = self.check_runtime_result(
                    cursor, state["count"], state["kind"])
        except BaseException as error:
            outcome["worker_error"] = repr(error)
        finally:
            cleanup = []
            if result is not None:
                cleanup.append(lambda: raw_free(result))
            if cursor is not None:
                cleanup.append(cursor.close)
            if connection is not None:
                cleanup.append(connection.close)
            try:
                for close in cleanup:
                    try:
                        close()
                    except BaseException as error:
                        outcome.setdefault("cleanup_errors", []).append(
                            repr(error))
            finally:
                state["finished"].set()

    def run_failure_query(self, kind, side, query, count, query_id,
                          followup_id, root):
        from new_test_framework.utils.join_runtime_logs import JoinRuntimeLogs

        gate = root / str(query_id)
        gate.mkdir(mode=0o700)
        assert not any(gate.iterdir()), "Failure gate must be empty"
        state = {"kind": kind, "query": query, "count": count,
                 "query_id": query_id, "followup_id": followup_id,
                 "gate": gate, "commands": {}, "outcome": {},
                 "returned": threading.Event(),
                 "allow_result_free": threading.Event(),
                 "result_freed": threading.Event(),
                 "allow_followup": threading.Event(),
                 "finished": threading.Event()}
        worker = threading.Thread(target=self.run_failure_worker,
                                  args=(state,), daemon=True)
        started = False
        try:
            descriptor = os.open(gate / "arm",
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as marker:
                marker.write(f"{query_id}\n")
            observer = JoinRuntimeLogs(query_id)
            worker.start()
            started = True
            binding = observer.wait_failure_held(kind, side)
            target, peer = binding["target_key"], binding["peer_key"]
            assert target[0] == peer[0] == query_id and target != peer, binding
            observer.mark_failure()
            self.write_failure_command(state, target[3], "fail")
            observer.wait_failure_injected()
            observer.mark_failure_release()
            self.write_failure_command(state, peer[3], "pass")
            assert state["returned"].wait(20), (
                "Native query did not return the producer failure"
            )
            outcome = state["outcome"]
            assert "query_error" not in outcome, outcome
            assert outcome.get("error_code") == 0x0732, outcome
            assert outcome.get("result_retained_at_failure"), outcome
            assert not outcome.get("result_freed"), outcome
            observer.wait_failure_propagated()
            state["allow_result_free"].set()
            assert state["result_freed"].wait(15), "Error result did not free"
            assert outcome.get("result_freed"), outcome
            observer.wait_failure_drop()
            proof = observer.check_failure_cleaned()
            outcome["run_followup"] = True
            state["allow_followup"].set()
            assert state["finished"].wait(20), (
                "Same-connection follow-up did not finish"
            )
            worker.join(timeout=1)
            assert not worker.is_alive(), "Producer failure worker remained"
            assert "worker_error" not in outcome, outcome
            assert "cleanup_errors" not in outcome, outcome
            expected = count * (8 if kind == "hash" else 1)
            assert outcome.get("followup_rows") == expected, outcome
            proof.update(kind=kind, side=side, action="failure",
                         query_id=query_id, input_rows=count,
                         error_code=0x0732, result_retained_at_failure=True,
                         result_released_normally=True,
                         followup_query_id=followup_id,
                         followup_rows=outcome["followup_rows"])
        finally:
            errors = []
            for marker in gate.glob("held.*"):
                match = re.fullmatch(r"held\.(\d+)", marker.name)
                if match is None:
                    continue
                try:
                    self.write_failure_command(state, int(match[1]), "pass")
                except BaseException as error:
                    errors.append(repr(error))
            state["allow_result_free"].set()
            state["allow_followup"].set()
            if started:
                worker.join(timeout=20)
                assert not worker.is_alive(), "Producer failure worker leaked"
            assert not errors, errors

        assert (gate / "arm").read_text() == f"{query_id}\n"
        task_ids = {target[3], peer[3]}
        expected = {"arm"}
        expected.update(f"held.{task_id}" for task_id in task_ids)
        expected.update(f"release.{task_id}" for task_id in task_ids)
        assert {path.name for path in gate.iterdir()} == expected, gate
        for name in expected:
            (gate / name).unlink()
        gate.rmdir()
        return proof

    def do_jp_10_failure(self):
        root = getattr(self, "failure_gate_dir", None)
        assert root is not None, "Runner must provide its private failure gate"
        root = Path(root).resolve(strict=True)
        configured = Path(os.environ["JOIN_PARALLEL_FAILURE_GATE_DIR"])
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        assert root.is_dir() and root == configured.resolve(strict=True)
        assert root == Path(manifest["failure_gate_dir"]).resolve(strict=True)
        assert root.parent == Path(manifest["data_root"]).resolve(strict=True)
        assert not any(root.iterdir()), "Failure gate root must be empty"
        policy = self.failure_response_policy()
        self.failure_evidence = []
        try:
            self.set_failure_response_policy(1)
            count = 8192
            self.prepare_data(count, "duplicate", names="abcd")
            first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
            for kind_index, (kind, query) in enumerate(self.runtime_queries()):
                self.check_runtime_plan(kind, query)
                for side_index, side in enumerate(("left", "right")):
                    index = kind_index * 2 + side_index
                    query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
                    followup_id = query_id % 0x7fffffff + 1
                    proof = self.run_failure_query(kind, side, query, count,
                                                   query_id, followup_id, root)
                    self.failure_evidence.append(proof)
                    self.log.info(
                        f"JP-10 {kind}/{side} producer error: {proof}")
        finally:
            self.set_failure_response_policy(policy)

    def run_retry_worker(self, state):
        from taos.cinterface import taos_errno

        outcome = state["outcome"]
        connection = cursor = None
        try:
            try:
                connection = self.new_connection()
                cursor = connection.cursor()
                cursor.execute(f"use {self.database}")
                cursor.execute(state["query"], req_id=state["query_id"])
                outcome["rows"] = self.check_runtime_result(
                    cursor, state["count"], state["kind"])
                assert cursor._result is not None
                assert taos_errno(cursor._result) == 0
                outcome["result_retained"] = True
            except BaseException as error:
                outcome["query_error"] = repr(error)
            finally:
                state["returned"].set()
            assert state["allow_free"].wait(60), (
                "Retry result retention expired"
            )
            if cursor is not None:
                cursor.close()
                cursor = None
            outcome["result_freed"] = True
            state["freed"].set()
            assert state["allow_followup"].wait(60), "Retry cleanup expired"
            if outcome.get("run_followup"):
                cursor = connection.cursor()
                cursor.execute(state["query"], req_id=state["followup_id"])
                outcome["followup_rows"] = self.check_runtime_result(
                    cursor, state["count"], state["kind"])
        except BaseException as error:
            outcome["worker_error"] = repr(error)
        finally:
            for item in (cursor, connection):
                if item is not None:
                    try:
                        item.close()
                    except BaseException as error:
                        outcome.setdefault("cleanup_errors", []).append(
                            repr(error))
            state["finished"].set()

    @staticmethod
    def write_retry_marker(gate, name, query_id):
        path = gate / name
        value = f"{query_id}\n"
        if path.exists():
            assert path.read_text() == value, path
            return
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                             0o600)
        with os.fdopen(descriptor, "w") as marker:
            marker.write(value)

    def run_retry_query(self, kind, query, count, query_id, followup_id, root):
        from new_test_framework.utils.join_runtime_logs import JoinRetryLogs

        gate = root / str(query_id)
        gate.mkdir(mode=0o700)
        state = {"kind": kind, "query": query, "count": count,
                 "query_id": query_id, "followup_id": followup_id,
                 "outcome": {}, "returned": threading.Event(),
                 "allow_free": threading.Event(), "freed": threading.Event(),
                 "allow_followup": threading.Event(),
                 "finished": threading.Event()}
        worker = threading.Thread(target=self.run_retry_worker, args=(state,),
                                  daemon=True)
        started = False
        try:
            self.write_retry_marker(gate, "arm", query_id)
            observer = JoinRetryLogs(query_id)
            worker.start()
            started = True
            observer.wait_retry_held(kind)
            self.write_retry_marker(gate, "inject", query_id)
            observer.wait_retry_started()
            assert not state["returned"].is_set(), state["outcome"]
            observer.mark_old_release()
            self.write_retry_marker(gate, "release", query_id)
            observer.check_old_cleaned()
            assert not state["returned"].is_set(), state["outcome"]
            observer.mark_next_release()
            self.write_retry_marker(gate, "next-release", query_id)
            assert state["returned"].wait(20), "Retried query did not complete"
            outcome = state["outcome"]
            assert "query_error" not in outcome, outcome
            assert outcome.get("result_retained"), outcome
            assert not outcome.get("result_freed"), outcome
            observer.mark_result_free()
            state["allow_free"].set()
            assert state["freed"].wait(15), "Retry result did not free"
            assert outcome.get("result_freed"), outcome
            proof = observer.check_retry_cleaned()
            outcome["run_followup"] = True
            state["allow_followup"].set()
            assert state["finished"].wait(20), "Retry follow-up did not finish"
            worker.join(timeout=1)
            assert not worker.is_alive(), "Retry worker remained alive"
            assert "worker_error" not in outcome, outcome
            assert "cleanup_errors" not in outcome, outcome
            expected = count * (8 if kind == "hash" else 1)
            assert outcome.get("rows") == outcome.get("followup_rows") == (
                expected), outcome
            proof.update(kind=kind, action="retry", query_id=query_id,
                         input_rows=count, rows=outcome["rows"],
                         retry_code=0x0914, result_retained=True,
                         result_released_normally=True,
                         new_execution_waiting_at_old_release=True,
                         followup_query_id=followup_id,
                         followup_rows=outcome["followup_rows"])
        finally:
            for name in ("inject", "next-release", "release"):
                self.write_retry_marker(gate, name, query_id)
            state["allow_free"].set()
            state["allow_followup"].set()
            if started:
                worker.join(timeout=20)
                assert not worker.is_alive(), "Retry worker leaked"
        expected = {"arm", "held", "inject", "next-held", "next-release",
                    "release"}
        assert {path.name for path in gate.iterdir()} == expected, gate
        assert (gate / "held").read_text() == (
            f"{observer.old_held['fields']['exchangeId']}\n")
        for name in expected:
            (gate / name).unlink()
        gate.rmdir()
        return proof

    def do_jp_10_retry(self):
        root = getattr(self, "retry_gate_dir", None)
        assert root is not None, "Runner must provide private retry gates"
        root = Path(root).resolve(strict=True)
        configured = Path(os.environ["JOIN_PARALLEL_RETRY_GATE_DIR"])
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        assert root.is_dir() and root == configured.resolve(strict=True)
        assert root == Path(manifest["retry_gate_dir"]).resolve(strict=True)
        assert root.parent == Path(manifest["data_root"]).resolve(strict=True)
        assert not any(root.iterdir()), "Retry gate root must be empty"
        policy = self.failure_response_policy()
        self.retry_evidence = []
        try:
            self.set_failure_response_policy(1)
            count = 8192
            self.prepare_data(count, "duplicate", names="abcd")
            first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
            for index, (kind, query) in enumerate(self.runtime_queries()):
                self.check_runtime_plan(kind, query)
                query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
                followup_id = query_id % 0x7fffffff + 1
                proof = self.run_retry_query(kind, query, count, query_id,
                                              followup_id, root)
                self.retry_evidence.append(proof)
                self.log.info(f"JP-10 {kind} retry cleanup: {proof}")
        finally:
            self.set_failure_response_policy(policy)

    def idle_timeout_setting(self):
        self.sql.query("show dnode 1 variables like 'queryNoFetchTimeoutSec'")
        values = [int(row[2]) for row in self.sql.queryResult
                  if len(row) > 2 and row[1] == "queryNoFetchTimeoutSec"]
        assert len(values) == 1 and 60 <= values[0] <= 1000000000, values
        return values[0]

    def set_idle_timeout(self, value):
        assert type(value) is int and 60 <= value <= 1000000000, value
        self.sql.execute(f"alter dnode 1 'queryNoFetchTimeoutSec' '{value}'")
        assert self.idle_timeout_setting() == value

    def do_jp_10_idle(self):
        import ctypes
        from taos.cinterface import _libtaos
        from new_test_framework.utils.join_runtime_logs import JoinIdleLogs

        root = getattr(self, "idle_gate_dir", None)
        assert root is not None, "Runner must provide private idle gates"
        root = Path(root).resolve(strict=True)
        config = Path(self.sql.cursor._connection._config).resolve()
        manifest = json.loads((config.parent / "manifest.json").read_text())
        assert root == Path(os.environ["JOIN_PARALLEL_IDLE_GATE_DIR"]).resolve()
        assert root == Path(manifest["idle_gate_dir"]).resolve()
        assert root.parent == Path(manifest["data_root"]).resolve()
        assert not any(root.iterdir()), "Idle gate root must be empty"
        assert manifest["idle_timeout"] == 60
        assert manifest["idle_scan_interval"] == 1
        fetch = _libtaos["taos_fetch_row"]
        fetch.argtypes = (ctypes.c_void_p,)
        fetch.restype = ctypes.c_void_p
        errno = _libtaos["taos_errno"]
        errno.argtypes = (ctypes.c_void_p,)
        errno.restype = ctypes.c_int
        previous = self.idle_timeout_setting()
        self.idle_evidence = []
        states = []
        try:
            self.set_idle_timeout(60)
            count = 8192
            self.prepare_data(count, "duplicate", names="abcd")
            first_id = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
            for index, (kind, query) in enumerate(self.runtime_queries()):
                self.check_runtime_plan(kind, query)
                query_id = (first_id + 2 * index - 1) % 0x7fffffff + 1
                gate = root / str(query_id)
                gate.mkdir(mode=0o700)
                self.write_retry_marker(gate, "arm", query_id)
                connection = self.new_connection()
                state = {"kind": kind, "query": query, "gate": gate,
                         "query_id": query_id,
                         "followup_id": query_id % 0x7fffffff + 1,
                         "connection": connection, "cursor": None}
                states.append(state)
                cursor = connection.cursor()
                state["cursor"] = cursor
                cursor.execute(f"use {self.database}")
                observer = JoinIdleLogs(query_id)
                state["started_wall"] = time.time()
                state["started"] = time.monotonic()
                cursor.execute(query, req_id=query_id)
                observer.wait_paused()
                observer.mark_idle(kind)
                state["observer"] = observer
            deadline = time.monotonic() + 95
            for state in states:
                proof = state["observer"].check_idle_cleaned(
                    timeout=max(0, deadline - time.monotonic()))
                cursor, connection = state["cursor"], state["connection"]
                assert cursor._result is not None and connection._conn
                assert cursor._connection is connection
                finished_wall = time.time()
                elapsed = time.monotonic() - state["started"]
                # qworker ages inactivity in wall-clock seconds, including
                # real clock adjustments while the query is retained.
                wall_elapsed = finished_wall - state["started_wall"]
                assert wall_elapsed >= 59, (
                    f"Idle expiry bypassed the real clock: {wall_elapsed:.3f}s"
                    f" (monotonic: {elapsed:.3f}s)")
                state.update(proof=proof, elapsed=elapsed,
                             finished_wall=finished_wall)
            for state in states:
                cursor, connection = state["cursor"], state["connection"]
                # Expiry releases the task's RPC handle, so the retained
                # result sees BROKEN_LINK when it tries to fetch again.
                assert fetch(cursor._result) is None, (
                    "Expired query returned rows after server cleanup")
                code = errno(cursor._result) & 0xffff
                assert code == 0x0018, f"Unexpected expired result: {code:#x}"
                cursor.close()
                state["cursor"] = None
                cursor = connection.cursor()
                state["cursor"] = cursor
                cursor.execute(state["query"], req_id=state["followup_id"])
                rows = self.check_runtime_result(cursor, count, state["kind"])
                proof = state["proof"]
                proof.update(kind=state["kind"], action="idle",
                             query_id=state["query_id"], input_rows=count,
                             idle_timeout=60, idle_scan_interval=1,
                             elapsed_seconds=state["elapsed"],
                             wall_started_seconds=state["started_wall"],
                             wall_finished_seconds=state["finished_wall"],
                             expiry_code=0x073b, expired_result_code=code,
                             result_retained_at_expiry=True,
                             connection_retained_at_expiry=True,
                             result_released_after_cleanup=True,
                             followup_query_id=state["followup_id"],
                             followup_rows=rows)
                self.idle_evidence.append(proof)
                self.log.info(f"JP-10 {state['kind']} idle cleanup: {proof}")
        finally:
            errors = []
            for state in states:
                for resource in (state["cursor"], state["connection"]):
                    if resource is not None:
                        try:
                            resource.close()
                        except BaseException as error:
                            errors.append(repr(error))
            self.set_idle_timeout(previous)
            assert not errors, errors
        for proof in self.idle_evidence:
            proof["idle_timeout_restored"] = True
        for state in states:
            gate = state["gate"]
            assert {value.name for value in gate.iterdir()} == {"arm"}
            assert (gate / "arm").read_text() == f"{state['query_id']}\n"
            (gate / "arm").unlink()
            gate.rmdir()

    def do_jp_11(self):
        for mode in ("inner", "left", "right", "full", "semi", "anti"):
            for residual in (False, True):
                query, expected = self.mixed_case(mode, residual)
                self.check_rows(query, self.values(expected))
        query, expected = self.mixed_case("left", True)
        null_rows = [row for row in expected if row[2] is None]
        self.check_rows(query + " where c.v is null", self.values(null_rows))
        for name, query, expected, height, kinds in self.mixed_rewrite_cases():
            if not name.startswith("on-"):
                continue
            assert expected, name
            self.check_rows(query, expected)
            shape = ((("a", "b"), "c"), "d") if height == 3 else None
            self.check_plan(query, 3, height, shape=shape, join_types=kinds)
        prefix = "select a.v,b.v,c.v,d.v from (a join b on a.k=b.k) "
        for direction in ("left", "right"):
            query = (prefix + f"{direction} semi join "
                     "(c join d on c.k=d.k) on b.k=c.k where a.k=1")
            self.check_rows(query, [(101, 201, 301, 401)])
        query, expected = self.mixed_case("anti", True)
        query = query.replace("select a.v,b.v ", "select a.v,b.v,c.v,d.v ")
        self.check_rows(query, self.values([row + (None, None)
                                           for row in expected]))

    def do_jp_12(self):
        query, expected = self.mixed_case("left", True)
        cases = (
            ("c.v is null", lambda row: row[2] is None),
            ("c.v>302", lambda row: row[2] is not None and row[2][3] > 302),
            ("c.v>302 or c.v is null", lambda row: row[2] is None
             or row[2][3] > 302),
            ("c.v>302 and a.v>101", lambda row: row[2] is not None
             and row[2][3] > 302 and row[0][3] > 101),
            ("coalesce(c.v,1)=1", lambda row: row[2] is None),
            ("abs(c.v)>302", lambda row: row[2] is not None
             and abs(row[2][3]) > 302),
        )
        for condition, predicate in cases:
            rows = [row for row in expected if predicate(row)]
            self.check_rows(query + " where " + condition, self.values(rows))
        for name, query, expected, height, kinds in self.mixed_rewrite_cases():
            if not name.startswith("where-"):
                continue
            assert expected, name
            self.check_rows(query, expected)
            shape = ((("a", "b"), "c"), "d") if height == 3 else None
            self.check_plan(query, 3, height, shape=shape, join_types=kinds)

    def separate_times(self):
        for index, name in enumerate("abcdefgh"):
            shift = (index % 4) * 10000
            rows = [(row[0] + shift,) + row[1:4]
                    + (None if row[4] is None else row[4] + shift,)
                    + row[5:] for row in self.rows[name]]
            self.replace_rows(name, rows)

    def temporal_query(self, kind="asof", explicit=True, join_key="ts"):
        left = self.join_query("abcd", join_key).split(" from ", 1)[1]
        right = self.join_query("efgh", join_key).split(" from ", 1)[1]
        columns = ",".join(f"{name}.v" for name in "abcdefgh")
        query = f"select {columns} from ({left}) left {kind} join ({right})"
        if kind == "asof":
            if explicit:
                query += " on a.tk>=e.tk"
        elif explicit:
            query += " window_time(a.tk,e.tk) window_offset(0s,0s) jlimit 1"
        else:
            query += " window_offset(0s,0s)"
        column = 4 if explicit else 0
        expected = []
        right_rows = self.equi_rows("efgh", join_key)
        for row in self.equi_rows("abcd", join_key):
            time_key = row[0][column]
            if time_key is None:
                continue
            candidates = [r for r in right_rows if r[0][column] is not None]
            if kind == "asof":
                candidates = [r for r in candidates
                              if r[0][column] <= time_key]
                candidates = sorted(candidates, key=lambda r: r[0][column])[-1:]
            else:
                candidates = [r for r in candidates
                              if r[0][column] == time_key][:1]
            expected.extend(row + candidate for candidate in candidates)
            if not candidates:
                expected.append(row + (None,) * 4)
        return query, self.values(expected)

    def temporal_nodes(self, roots):
        return [node for node in self.join_nodes(roots)
                if re.search(r"\b(?:ASOF|Window) Join\b", node["label"])]

    def check_time_keys(self, plan, left, right, key):
        nodes = self.temporal_nodes(self.plan_nodes(plan))
        assert len(nodes) == 1, plan
        details = " ".join(nodes[0]["details"])
        for side, name in (("Left", left), ("Right", right)):
            assert self.explain_has_time_key(details, side, name, key), plan

    def temporal_leaf_query(self):
        query = (
            "select c.v,a.v,b.v,d.v,e.v from c "
            "join (a left asof join b) on c.k=a.k "
            "join d on c.k=d.k join e on c.k=e.k"
        )
        expected = []
        for c, a, d, e in self.equi_rows("cade"):
            candidates = [b for b in self.rows["b"] if b[0] <= a[0]]
            b = max(candidates, key=lambda row: row[0], default=None)
            expected.append((c, a, b, d, e))
        return query, self.values(expected)

    def boundary_queries(self, key="ts"):
        limit_query = (
            "select q.v,b.v,c.v,d.v from "
            f"(select {key},v from a order by ts limit 2) q "
            f"join b on q.{key}=b.{key} join c on q.{key}=c.{key} "
            f"join d on q.{key}=d.{key}"
        )
        combinations = self.equi_rows("abcd", key)
        limited = [row for row in combinations
                   if row[0] in sorted(self.rows["a"])[:2]]
        grouped = {}
        for row in self.rows["a"]:
            grouped[row[1]] = max(grouped.get(row[1], row[3]), row[3])
        matches = [row for row in self.equi_rows("bcd")
                   if row[0][1] in grouped]
        suffix = "join b on q.k=b.k join c on q.k=c.k join d on q.k=d.k"
        return [
            ("limit", limit_query, self.values(limited), r"\blimit=2\b"),
            ("aggregate", "select q.v,b.v,c.v,d.v from "
             "(select k,max(v) as v from a group by k) q " + suffix,
             [(grouped[r[0][1]],) + tuple(row[3] for row in r)
              for r in matches], r"\bGroupAggregate\b"),
            ("distinct", "select q.k,b.v,c.v,d.v from "
             "(select distinct k from a) q " + suffix,
             [(r[0][1],) + tuple(row[3] for row in r) for r in matches],
             r"\b(?:GroupAggregate|Distinct)\b"),
        ]

    def check_boundary(self, plan, pattern):
        roots = self.plan_nodes(plan)
        matches = [node for node in self.walk_plan(roots)
                   if re.search(pattern, node["label"] + " "
                                + " ".join(node["details"]))]
        assert matches, plan
        assert all(self.table_names([node]) == frozenset("a")
                   for node in matches), plan

    def do_jp_13(self):
        self.check_explain_column_parsers()
        for nullable_right in (False, True):
            if nullable_right:
                row = self.rows["e"][1]
                self.rows["e"][1] = row[:4] + (None,) + row[5:]
                self.sql.execute("delete from e")
                self.insert_rows("e", self.rows["e"])
            for kind in ("asof", "window"):
                for explicit in (False, True):
                    query, expected = self.temporal_query(kind, explicit)
                    self.check_rows(query, expected)
                    plan = self.check_plan(query, 7, 3, split=True)
                    key = "tk" if explicit else "ts"
                    self.check_time_keys(plan, "a", "e", key)
        self.prepare_data(5, "unique")
        self.separate_times()
        for kind in ("asof", "window"):
            for explicit in (False, True):
                query, expected = self.temporal_query(kind, explicit, "k")
                self.check_rows(query, expected)
                plan = self.check_plan(query, 7, 3, split=True)
                key = "tk" if explicit else "ts"
                self.check_time_keys(plan, "a", "e", key)
        self.prepare_data()
        query, expected = self.temporal_leaf_query()
        self.check_rows(query, expected)
        plan = self.check_plan(query, 4, 3)
        self.check_time_keys(plan, "a", "b", "ts")
        roots = self.plan_nodes(plan)
        node = self.temporal_nodes(roots)[0]
        assert self.table_names([node]) == frozenset("ab"), plan
        opaque = lambda candidate: "x" if candidate is node else None
        assert self.join_height(roots, opaque=opaque) == 2, plan
        assert self.join_shape([node]) == ("a", "b"), plan
        for name, query, expected, pattern in self.boundary_queries():
            assert expected, name
            self.check_rows(query, expected)
            plan = self.check_plan(query, 3, 2, split=True)
            self.check_boundary(plan, pattern)
        self.check_rows(
            "select distinct a.k from a join b on a.k=b.k "
            "join c on a.k=c.k join d on a.k=d.k", [(1,), (2,)]
        )

    def do_functional(self, case_id, requirements):
        self.log.info(f"{case_id}: {requirements}")
        try:
            if case_id not in ("JP-10-failure", "JP-10-retry", "JP-10-idle"):
                self.prepare_data()
            getattr(self, "do_" + case_id.lower().replace("-", "_"))()
        finally:
            self.drop_data()

    # --- performance comparisons ---
    def performance_queries(self, case_id):
        names = ("abcd", "abcdefgh") if case_id == "PF-01" else ("abcd",)
        cases = []
        for group in names:
            key = "ts" if case_id in ("PF-01", "PF-10") else "k"
            shapes = (("chain", "star", "balanced") if case_id == "PF-01"
                      else ("star",))
            for shape in shapes:
                query = self.join_query(group, key, shape)
                cases.append((f"{len(group)}-{shape}", query,
                              self.values(self.equi_rows(group, key))))
        if case_id == "PF-02":
            cases = [(key, self.join_query(key=key),
                      self.values(self.equi_rows("abcd", key)))
                     for key in ("ts", "k", "s", "f")]
            cases.extend((name, query, expected)
                         for name, query, expected, _
                         in self.equivalence_cases())
        elif case_id == "PF-03":
            query = self.join_query().replace(
                " from ", ",a.payload,b.payload,c.payload,d.payload from ", 1
            )
            combinations = self.equi_rows("abcd")
            expected = [tuple(row[3] for row in rows)
                        + tuple(row[6] for row in rows)
                        for rows in combinations]
            cases = [("wide-duplicates", query, expected)]
            cases.append(("reordered-columns", *self.reordered_query()))
        elif case_id == "PF-05":
            cases = [(hint, self.join_query(key=key, hint=hint),
                      self.values(self.equi_rows("abcd", key)))
                     for hint, key in (("hash_join", "k"),
                                       ("merge_join", "ts"))]
        elif case_id == "PF-07":
            cases = [(f"{names}-{shape}-{key}",
                      self.join_query(names, key, shape),
                      self.values(self.equi_rows(names, key)))
                     for names, shape, key in (
                         ("ab", "chain", "ts"), ("abcd", "balanced", "ts"),
                         ("abcdefghi", "star", "ts"), ("abcd", "star", "f"),
                     )]
        elif case_id == "PF-09":
            cases = []
            for mode in ("left", "full", "semi", "anti"):
                query, combinations = self.mixed_case(mode, True)
                cases.append((mode, query, self.values(combinations)))
            cases.append(("left-chain", *self.mixed_chain()))
            cases.extend((name, query, expected)
                         for name, query, expected, _, _
                         in self.mixed_rewrite_cases())
        elif case_id == "PF-10":
            cases = [(f"{kind}-{explicit}",
                      *self.temporal_query(kind, explicit, "k"))
                     for kind in ("asof", "window")
                     for explicit in (False, True)]
            cases.append(("temporal-leaf", *self.temporal_leaf_query()))
            cases.extend((name, query, expected)
                         for name, query, expected, _
                         in self.boundary_queries("k"))
        return cases

    def concurrent_fetch(self, query, workers):
        def fetch():
            connection = self.new_connection()
            cursor = connection.cursor()
            try:
                cursor.execute(f"use {self.database}")
                start = time.perf_counter()
                cursor.execute(query)
                rows = cursor.fetchall()
                return rows, (time.perf_counter() - start) * 1000
            finally:
                cursor.close()
                connection.close()

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda _: fetch(), range(workers)))
        elapsed = (time.perf_counter() - start) * 1000
        return [rows for rows, _ in results], {
            "total_ms": elapsed,
            "query_median_ms": median(latency for _, latency in results),
            "query_max_ms": max(latency for _, latency in results),
            "queries_per_second": workers * 1000 / elapsed,
        }

    def performance_sample(self, query, expected, delay=0.0, workers=1):
        import psutil

        process = psutil.Process()
        peak = [process.memory_info().rss]
        stop = threading.Event()

        def observe():
            while not stop.wait(0.01):
                peak[0] = max(peak[0], process.memory_info().rss)

        observer = threading.Thread(target=observe, daemon=True)
        observer.start()
        try:
            if workers > 1:
                results, metrics = self.concurrent_fetch(query, workers)
            else:
                result, metrics = self.fetch_query(query, delay)
                results = [result]
        finally:
            peak[0] = max(peak[0], process.memory_info().rss)
            stop.set()
            observer.join()
        reference = Counter(expected)
        for result in results:
            assert Counter(self.normalize_rows(result)) == reference, query
        metrics["client_peak_rss_bytes"] = peak[0]
        # ANALYZE executes again with profiling and drains results itself.
        # It is separate from normal fetch, worker and client measurements.
        # Both builds must use the same analysis and logging configuration.
        metrics["execution_ms"] = self.analyze_execution_ms(query)
        return metrics

    def performance_configuration(self, case_id):
        mode = os.getenv("JOIN_PARALLEL_PERF_MODE", "verify")
        assert mode in ("measure", "verify"), mode
        for variable in ("JOIN_PARALLEL_PERF_DIR", "JOIN_PARALLEL_BUILD_ID"):
            assert os.environ.get(variable), f"Set {variable} before timing"
        budget = None
        if mode == "verify":
            variable = "JOIN_PARALLEL_PERF_BUDGETS"
            assert os.environ.get(variable), f"Set {variable} before timing"
            budgets = json.loads(Path(os.environ[variable]).read_text())
            budget = budgets[case_id]
            required = "cancel_ms" if case_id == "PF-08" else "execution_ms"
            assert required in budget, f"{case_id} needs a {required} budget"
            if case_id != "PF-08":
                assert set(budget) == {"execution_ms"}, (
                    "Only execution_ms is used for performance acceptance"
                )
        return mode, budget

    def save_comparison(self, case_id, name, samples, metadata, budget, mode):
        assert mode in ("measure", "verify"), mode
        assert len(samples) >= 7, "Keep at least seven timed samples"
        if mode == "verify":
            assert isinstance(budget, dict) and budget, "Missing ratio budget"
        else:
            assert budget is None, "Measurement mode does not use a budget"
        phase = os.environ.get("JOIN_PARALLEL_PERF_PHASE", "candidate")
        assert phase in ("baseline", "candidate"), phase
        directory = Path(os.environ["JOIN_PARALLEL_PERF_DIR"])
        path = directory / phase / f"{case_id}-{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        medians = {key: median(sample[key] for sample in samples)
                   for key in samples[0]}
        report = {
            "metadata": metadata, "budget": budget, "samples": samples,
            "medians": medians, "build": os.environ["JOIN_PARALLEL_BUILD_ID"],
            "mode": mode, "acceptance": "not_evaluated",
        }
        violations = {}
        try:
            if phase == "candidate":
                baseline_path = directory / "baseline" / path.name
                assert baseline_path.is_file(), (
                    f"Missing baseline: {baseline_path}"
                )
                baseline = json.loads(baseline_path.read_text())
                assert baseline.get("mode") == mode, "Comparison mode mismatch"
                assert baseline["metadata"] == metadata, (
                    "Dataset/config mismatch"
                )
                assert baseline["budget"] == budget, (
                    "Budget changed after baseline"
                )
                ratios, unavailable = {}, {}
                for metric, value in medians.items():
                    reference = baseline["medians"].get(metric)
                    if reference is None or reference <= 0:
                        unavailable[metric] = "missing_or_nonpositive_baseline"
                    else:
                        ratios[metric] = value / reference
                report["candidate_to_baseline"] = ratios
                report["unavailable_ratios"] = unavailable
                if mode == "verify":
                    for metric, maximum in budget.items():
                        assert metric in ratios and maximum > 0, metric
                        ratio = ratios[metric]
                        passed = (ratio >= maximum
                                  if metric == "queries_per_second"
                                  else ratio <= maximum)
                        if not passed:
                            violations[metric] = {"ratio": ratio,
                                                  "budget": maximum}
                    report["violations"] = violations
                    report["acceptance"] = "failed" if violations else "passed"
        finally:
            path.write_text(json.dumps(report, indent=2) + "\n")
        assert not violations, (case_id, name, violations)
        self.log.info(
            f"{case_id}/{name}: {phase}/{mode} {medians}; "
            f"acceptance={report['acceptance']}; report={path}"
        )

    def do_performance_asymmetric_hash(self):
        case_id = "PF-12"
        mode, budget = self.performance_configuration(case_id)
        count = int(os.getenv("JOIN_PARALLEL_PERF_ROWS", "2048"))
        rounds = int(os.getenv("JOIN_PARALLEL_PERF_ROUNDS", "7"))
        small = max(4, count // 10)
        setups = (("left-smaller", {"a": small, "b": count}),
                  ("right-smaller", {"a": count, "b": small}))
        try:
            for name, sizes in setups:
                self.prepare_data(count, "unique", 1, "normal", "ab", 64,
                                  counts=sizes)
                query = self.join_query("ab", "k", "chain", hint="hash_join")
                expected = self.values(self.equi_rows("ab", "k"))
                dataset_hash = sha256(
                    json.dumps(self.rows, sort_keys=True).encode()
                ).hexdigest()
                plan = self.explain(query)
                metadata = {
                    "query": query, "rows_per_table": count,
                    "profile": "unique", "kind": "normal", "vgroups": 1,
                    "dataset_sha256": dataset_hash, "payload_width": 64,
                    "workers": 1, "rounds": rounds,
                    "endpoint": str(self.sql.cursor._connection._host),
                    "port": self.sql.cursor._connection._port,
                    "execution_measurement":
                        "explain_analyze_verbose_single_query",
                }
                samples = []
                for iteration in range(rounds + 1):
                    start = time.perf_counter()
                    self.explain(query)
                    explain_ms = (time.perf_counter() - start) * 1000
                    sample = self.performance_sample(query, expected)
                    sample["explain_ms"] = explain_ms
                    if iteration:
                        samples.append(sample)
                self.save_comparison(case_id, name, samples, metadata, budget,
                                     mode)
                path = Path(os.environ["JOIN_PARALLEL_PERF_DIR"])
                phase = os.getenv("JOIN_PARALLEL_PERF_PHASE", "candidate")
                plan_path = path / phase / f"{case_id}-{name}.plan"
                plan_path.write_text(plan)
        finally:
            self.drop_data()

    def do_performance(self, case_id, functional_cases, requirements):
        self.log.info(f"{case_id} ({functional_cases}): {requirements}")
        if case_id == "PF-12":
            self.do_performance_asymmetric_hash()
            return
        mode, budget = self.performance_configuration(case_id)
        count = int(os.getenv("JOIN_PARALLEL_PERF_ROWS", "2048"))
        profile = ("duplicate" if case_id in ("PF-02", "PF-03")
                   else "unique")
        if case_id == "PF-07":
            count, profile = 5, "small"
        if case_id == "PF-08":
            count, profile = 256, "all"
        if case_id == "PF-11":
            profile = "skew"
        assert count >= 4
        rounds = int(os.getenv("JOIN_PARALLEL_PERF_ROUNDS", "7"))
        assert rounds >= 7
        kind = "super" if case_id == "PF-04" else "normal"
        vgroups = 4 if case_id == "PF-04" else 1
        names = "abcdefghi"
        payload_width = 256 if case_id == "PF-03" else 64
        try:
            self.prepare_data(count, profile, vgroups, kind, names,
                              payload_width)
            if case_id in ("PF-02", "PF-03"):
                self.separate_keys()
            if case_id == "PF-10":
                self.separate_times()
            if case_id == "PF-08":
                configurations = [("cancel", self.join_query(hint="hash_join"),
                                   [])]
            else:
                configurations = self.performance_queries(case_id)
            dataset_hash = sha256(
                json.dumps(self.rows, sort_keys=True).encode()
            ).hexdigest()
            for name, query, expected in configurations:
                worker_counts = (1, 2, 4) if case_id == "PF-11" else (1,)
                for workers in worker_counts:
                    samples = []
                    plan = self.explain(query) if query else ""
                    metadata = {
                        "query": query, "rows_per_table": count,
                        "profile": profile, "kind": kind, "vgroups": vgroups,
                        "dataset_sha256": dataset_hash,
                        "payload_width": payload_width,
                        "workers": workers, "rounds": rounds,
                        "endpoint": str(self.sql.cursor._connection._host),
                        "port": self.sql.cursor._connection._port,
                    }
                    if case_id != "PF-08":
                        metadata["execution_measurement"] = (
                            "explain_analyze_verbose_single_query"
                        )
                    for iteration in range(rounds + 1):
                        if case_id == "PF-08":
                            sample = self.cancel_query()
                        else:
                            start = time.perf_counter()
                            self.explain(query)
                            explain_ms = (time.perf_counter() - start) * 1000
                            delay = 0.0
                            sample = self.performance_sample(
                                query, expected, delay, workers
                            )
                            sample["explain_ms"] = explain_ms
                        if iteration:
                            samples.append(sample)
                    # EXPLAIN includes transport; RSS/CPU are client metrics.
                    report_name = f"{name}-{workers}"
                    self.save_comparison(
                        case_id, report_name, samples, metadata, budget, mode
                    )
                    path = Path(os.environ["JOIN_PARALLEL_PERF_DIR"])
                    phase = os.getenv("JOIN_PARALLEL_PERF_PHASE", "candidate")
                    plan_path = path / phase / f"{case_id}-{report_name}.plan"
                    plan_path.write_text(plan)
        finally:
            self.drop_data()

    # --- main ---
    # SQL result and plan checks do not establish runtime concurrency.
    @pytest.mark.parametrize(
        "case_id, requirements",
        [
            pytest.param(
                "JP-01",
                "4/8-table chains, stars and balanced trees; 1/2/3/9-input "
                "boundaries; result bags, heights and unchanged input order",
                id="JP-01-tree-search",
            ),
            pytest.param(
                "JP-02",
                "Column equality classes, NULLs and duplicate keys; separate "
                "and shared classes; float, cast, OR and mixed-type plans",
                id="JP-02-equivalence-classes",
            ),
            pytest.param(
                "JP-03",
                "AND/OR/NOT, constant, one-sided and non-equality WHERE "
                "conditions; result bags, join heights and CROSS results",
                id="JP-03-predicate-placement",
            ),
            pytest.param(
                "JP-04",
                "Empty/unmatched inputs, one-to-many and many-to-many keys, "
                "self joins and aliases; full SELECT * column order after "
                "input reordering; values, NULLs and bag multiplicity",
                id="JP-04-results-and-columns",
            ),
            pytest.param(
                "JP-05",
                "4/8-table balanced trees; all eligible nested Exchange "
                "paths, extraction counts, input order and repeatable plans",
                id="JP-05-subtree-splitting",
            ),
            pytest.param(
                "JP-06",
                "Ordinary, child, super and mixed tables with 1/4 vgroups; "
                "result bags, join heights and Exchange paths",
                id="JP-06-distributed-inputs",
            ),
            pytest.param(
                "JP-07",
                "Hash and Merge hints; result bags, chosen algorithms, "
                "join heights and Exchange paths",
                id="JP-07-execution-overlap",
            ),
            pytest.param(
                "JP-09",
                "Float-key stars and oversized regions retain input shape; "
                "repeated planning and query results remain unchanged",
                id="JP-09-fallback",
            ),
            pytest.param(
                "JP-11",
                "Mixed JOIN result bags and matching-side references; "
                "LEFT-chain ON conditions with legal and illegal rewrite "
                "plans, join types and preserved input shape",
                id="JP-11-mixed-joins",
            ),
            pytest.param(
                "JP-12",
                "WHERE IS NULL, AND/OR, COALESCE and ABS results; LEFT-chain "
                "PDC simplification and retained OUTER plans, with join "
                "types, heights and input shape assertions",
                id="JP-12-null-analysis-and-pdc",
            ),
            pytest.param(
                "JP-13",
                "LEFT ASOF/WINDOW explicit/default keys, selected time "
                "columns and NULL results; temporal subtrees as outer "
                "inputs; LIMIT, DISTINCT and GROUP BY boundary plans",
                id="JP-13-temporal-and-region-boundaries",
            ),
        ],
    )
    def test_join_parallel_functional(self, join_sql, case_id, requirements):
        """Parallel JOIN functional scenarios JP-01 through JP-13.

        1. Check result bags, columns, join shapes and nested Exchange paths.
        2. Check mixed-chain ON/WHERE rewrite plans and temporal boundaries.
        3. Check representative SQL for each supported JOIN shape.
        4. Planner GTest covers allocation/attribute failures and ownership.

        Catalog:
            - Query

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-23 Tony Zhang added individually skipped TS scenarios.
            - 2026-10-26 Tony Zhang updated plan checks and coverage text.
            - 2026-11-03 Tony Zhang enabled SQL/plan groups.
            - 2026-11-09 Tony Zhang verified task backpressure and resume.
        """
        self.do_functional(case_id, requirements)

    @pytest.fixture
    def join_runtime(self, tmp_path, case_id):
        from new_test_framework import join_parallel_runtime as runtime

        args = SimpleNamespace(
            build_dir=None, output=tmp_path / "runtime", case=case_id,
            port=int(os.getenv("JOIN_PARALLEL_TEST_PORT", "17660")),
            ready_timeout=90,
        )
        return runtime.runtime_server(args)

    def do_runtime_case(self, server, case_id):
        from new_test_framework import join_parallel_runtime as runtime

        with server as environment:
            runtime.check_runtime_case(environment, case_id, timeout=180)

    @pytest.mark.parametrize("case_id", [
        pytest.param("JP-08", id="backpressure"),
        pytest.param("JP-10", id="stop-and-close"),
        pytest.param("JP-10-inflight", id="cancel-pending-fetch"),
        pytest.param("JP-10-match", id="temporal-resume"),
        pytest.param("JP-10-timeout", id="rpc-timeout"),
        pytest.param("JP-10-failure", id="producer-failure"),
        pytest.param("JP-10-retry", id="retry"),
        pytest.param("JP-10-idle", id="idle-cleanup"),
    ])
    def test_join_parallel_runtime(self, join_runtime, case_id):
        """Check parallel JOIN execution and cleanup around interruptions.

        Run each scenario on an isolated server and validate SQL results and
        runtime evidence for the relevant task, session and fetch transitions.

        Catalog:
            - Query

        Since: v3.4.4.0

        Labels: common,ci
        """
        self.do_runtime_case(join_runtime, case_id)

    def do_migration_case(self, directory, kind):
        from new_test_framework import join_parallel_migration as migration

        args = SimpleNamespace(
            build_dir=None, output=directory, kind=kind,
            port=int(os.getenv("JOIN_PARALLEL_TEST_PORT", "18660")),
            ready_timeout=45, timeout=120,
        )
        migration.run(args)

    @pytest.mark.parametrize("kind", ["hash", "merge", "asof", "window"])
    def test_join_parallel_migration(self, tmp_path, kind):
        """Verify parallel JOIN recovery after a real qnode disconnect.

        Run the query on an isolated three-node cluster, stop the targeted
        compute process and check migration, cleanup and same-connection rows.

        Catalog:
            - Query

        Since: v3.4.4.0

        Labels: common,ci
        """
        self.do_migration_case(tmp_path / "migration", kind)

    @pytest.mark.parametrize(
        "case_id, functional_cases, requirements",
        [
            pytest.param(
                "PF-01",
                "JP-01,JP-05",
                "4/8-table chains, stars and balanced trees on unique "
                "timestamps; end-to-end, first-fetch and EXPLAIN timings",
                id="PF-01-tree-and-split-benefits",
            ),
            pytest.param(
                "PF-02",
                "JP-02,JP-03",
                "Star joins, column key types and separate/shared equality "
                "classes; query timings and client resource measurements",
                id="PF-02-star-inference-benefits",
            ),
            pytest.param(
                "PF-03",
                "JP-04",
                "Duplicate keys, wide rows and reordered SELECT * columns; "
                "query timings, client CPU and client peak RSS",
                id="PF-03-cardinality-and-row-width",
            ),
            pytest.param(
                "PF-04",
                "JP-06",
                "Supertable inputs with four vgroups; query timings, "
                "client CPU and client peak RSS",
                id="PF-04-distributed-costs",
            ),
            pytest.param(
                "PF-05",
                "JP-07",
                "Hash and Merge hints; end-to-end and first-fetch latency, "
                "EXPLAIN output and client resource measurements",
                id="PF-05-hash-and-merge-overlap",
            ),
            pytest.param(
                "PF-07",
                "JP-01,JP-09",
                "Small data, balanced/oversized regions and float-key stars; "
                "query timings and EXPLAIN elapsed time",
                id="PF-07-no-benefit-overhead",
            ),
            pytest.param(
                "PF-08",
                "JP-10",
                "KILL QUERY timing until the client worker exits and until "
                "the query record disappears from perf_queries",
                id="PF-08-exit-and-reclamation",
            ),
            pytest.param(
                "PF-09",
                "JP-11,JP-12",
                "Mixed JOIN result cases and LEFT-chain ON/WHERE rewrites; "
                "query timings and EXPLAIN elapsed time",
                id="PF-09-mixed-join-benefits",
            ),
            pytest.param(
                "PF-10",
                "JP-13",
                "Temporal inputs, temporal subtrees and subquery boundaries; "
                "query timings and client resource measurements",
                id="PF-10-boundary-costs",
            ),
            pytest.param(
                "PF-11",
                "JP-01,JP-06,JP-07",
                "Skewed inputs with 1/2/4 concurrent queries; throughput, "
                "median/max latency and client peak RSS",
                id="PF-11-scale-and-contention",
            ),
            pytest.param(
                "PF-12",
                "JP-07",
                "Asymmetric input sizes under a hash join; left and right "
                "build sides; query timings and EXPLAIN elapsed time",
                id="PF-12-asymmetric-build-side",
            ),
        ],
    )
    @pytest.mark.skipif(
        not os.environ.get("JOIN_PARALLEL_PERF_PHASE"),
        reason=(
            "Set JOIN_PARALLEL_PERF_PHASE=baseline/candidate and configure "
            "JOIN_PARALLEL_PERF_MODE to run performance comparisons"
        ),
    )
    def test_join_parallel_performance(
        self, join_sql, case_id, functional_cases, requirements
    ):
        """Parallel JOIN performance comparisons PF-01 through PF-12.

        1. Reuse functional SQL and compare results with Python expectations.
           PF-01 covers 4/8-table chains, stars and balanced trees.
        2. Sample baseline and candidate builds in separate configured runs.
        3. Check matching SQL, data, configuration and comparison mode.
        4. Warm up once; retain at least seven timed samples and medians.
        5. Record execution_ms from a separate EXPLAIN ANALYZE execution.
        6. Profiling adds overhead; match analysis/log settings across builds.
        7. Cancellation measures client exit and query record removal.
        8. Measure mode records ratios with acceptance=not_evaluated.
        9. Verify execution_ms budgets; cancellation keeps cancel_ms budgets.
        10. Normal SELECT results and diagnostic client metrics remain recorded.

        Catalog:
            - Query

        Since: v3.4.4.0

        Labels: common

        Jira: None

        History:
            - 2026-10-23 Tony Zhang added skipped TS performance comparisons.
            - 2026-10-26 Tony Zhang aligned descriptions with collected metrics.
            - 2026-11-03 Tony Zhang added measure/verify modes.
            - 2026-11-04 Tony Zhang separated execution-only acceptance.
        """
        self.do_performance(case_id, functional_cases, requirements)
