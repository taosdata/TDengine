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
from new_test_framework.utils.srvCtl import sc

from collections import defaultdict


class TestGroupedCacheSplit:
    hint = "/*+ NO_CONCURRENT_LAST_SCAN() */"
    projection = (
        "count(*), count(v), sum(v), min(w), max(w), avg(w), "
        "last(v), last_row(v), last(w)"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def sql_value(self, value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return str(value)

    def prepare(self, mode):
        self.db = f"grouped_cache_split_{mode}"
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(
            f"create database {self.db} vgroups 4 keep 36500 "
            f"cachemodel '{mode}' replica {getattr(self, 'replicaVar', 1)}"
        )
        tdSql.execute(f"use {self.db}")
        tdSql.execute(
            "create stable st(ts timestamp, v int, w int) "
            "tags(t int, label varchar(20), city nchar(20))"
        )
        self.records = []
        self.tables = []
        for i in range(17):
            name = f"g{i:02d}"
            tag = None if i in (3, 7) else i % 3
            label = ("a", "ab", "bc", None)[i % 4]
            city = ("杭州", "北京", None)[i % 3]
            tags = ",".join(map(self.sql_value, (tag, label, city)))
            tdSql.execute(f"create table {name} using st tags({tags})")
            self.tables.append(dict(tbname=name, t=tag, label=label,
                                    city=city))
            if i == 16:
                continue
            for row in range(4):
                ts = 1704067200000 + i * 100 + row
                value = None if i % 5 == 0 or row == 3 else i * 10 + row
                other = i - row
                tdSql.execute(
                    f"insert into {name} values "
                    f"({ts},{self.sql_value(value)},{other})"
                )
                self.records.append(
                    dict(tbname=name, t=tag, label=label, city=city,
                         ts=ts, v=value, w=other)
                )

    def aggregate(self, records):
        if not records:
            return (0, 0, None, None, None, None, None, None, None)
        values = [r["v"] for r in records if r["v"] is not None]
        others = [r["w"] for r in records]
        nonnull = [r for r in records if r["v"] is not None]
        newest = max(records, key=lambda r: r["ts"])
        last = max(nonnull, key=lambda r: r["ts"])["v"] if nonnull else None
        return (
            len(records), len(values), sum(values) if values else None,
            min(others), max(others), sum(others) / len(others),
            last, newest["v"], newest["w"],
        )

    def expected(self, key, records=None, include_key=True, having=None,
                 tables=None):
        groups = defaultdict(list)
        for table in self.tables if tables is None else tables:
            groups[key(table)]
        for record in self.records if records is None else records:
            groups[key(record)].append(record)
        result = []
        for group, rows in groups.items():
            aggregate = self.aggregate(rows)
            if having is None or having(aggregate):
                result.append((group if include_key else ()) + aggregate)
        return result

    def with_hint(self, sql):
        return sql.replace("select ", f"select {self.hint} ", 1)

    def check_rows(self, sql, expected, use_hint=False):
        if use_hint:
            sql = self.with_hint(sql)
        tdSql.query(sql)
        actual = [tuple(row) for row in tdSql.queryResult]
        assert sorted(actual, key=repr) == sorted(expected, key=repr), (
            sql, actual, expected
        )

    def check_plan(self, sql, cached=True, use_hint=False, grouped=True):
        if use_hint:
            sql = self.with_hint(sql)
        tdSql.query("explain verbose true " + sql)
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert ("Last Row Scan" in plan) == cached, (sql, plan)
        if cached:
            assert "Table Scan" in plan, (sql, plan)
            if grouped:
                assert "GroupAggregate" in plan, (sql, plan)

    def do_hint_control(self):
        values = self.expected(lambda r: (r["tbname"],))
        expected = [(r[0], r[1], r[7]) for r in values]
        for clause in ("partition by", "group by"):
            sql = ("select tbname, count(*), last(v) from st "
                   f"{clause} tbname")
            self.check_plan(sql)
            self.check_rows(sql, expected)
            self.check_plan(sql, False, use_hint=True)
            self.check_rows(sql, expected, use_hint=True)
            for hint in ("NO_CONCURRENT_LAST_SCAN",
                         "NO_CONCURRENT_LAST_SCAN(1)",
                         "CONCURRENT_LAST_SCAN()"):
                invalid = sql.replace("select ", f"select /*+ {hint} */ ", 1)
                self.check_plan(invalid)
                self.check_rows(invalid, expected, use_hint=False)

        expected = [(sum(r[1] for r in values),
                     sum(r[7] or 0 for r in values))]
        outer = (f"select {self.hint} sum(n), sum(v) from "
                 "(select count(*) n, last(v) v from st "
                 "partition by tbname)")
        self.check_plan(outer, use_hint=False)
        self.check_rows(outer, expected, use_hint=False)
        inner = ("select sum(n), sum(v) from "
                 f"(select {self.hint} count(*) n, last(v) v from st "
                 "partition by tbname)")
        self.check_plan(inner, False, use_hint=False)
        self.check_rows(inner, expected, use_hint=False)
        print("grouped cache split hint control ........ [ passed ]")

    def do_ungrouped_hint_control(self):
        aggregate = self.aggregate(self.records)
        for projection, expected in (
            ("count(*), last(v)", [(aggregate[0], aggregate[6])]),
            ("count(*), last_row(v)", [(aggregate[0], aggregate[7])]),
            (self.projection, [aggregate]),
        ):
            sql = f"select {projection} from st"
            for disabled in (False, True):
                self.check_plan(sql, not disabled, use_hint=disabled,
                                grouped=False)
                self.check_rows(sql, expected, use_hint=disabled)

        expected = [(aggregate[0], aggregate[6])]
        inner = "count(*) n, last(v) v from st"
        outer_hint = (f"select {self.hint} sum(n), sum(v) from "
                      f"(select {inner})")
        inner_hint = ("select sum(n), sum(v) from "
                      f"(select {self.hint} {inner})")
        self.check_plan(outer_hint, grouped=False)
        self.check_rows(outer_hint, expected)
        self.check_plan(inner_hint, False, grouped=False)
        self.check_rows(inner_hint, expected)
        sql = "select count(*), last(v) from st"
        mixed = f"{self.with_hint(sql)} union all {sql}"
        self.check_plan(mixed, grouped=False)
        self.check_rows(mixed, expected * 2)
        print("ungrouped cache split hint control ...... [ passed ]")

    def check_subquery_hint_plan(self, sql, branches, grouped):
        tdSql.query("explain verbose true " + sql)
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        nodes = [line for line in plan.splitlines() if "-> " in line]
        for table, enabled in branches:
            scans = [line for line in nodes if
                     f"Table Scan on {table} " in line]
            assert len(scans) == 1, (sql, plan)
            assert sum(f"Last Row Scan on {table} " in line
                       for line in nodes) == int(enabled), (sql, plan)
            load = "no" if enabled else "data"
            assert f"data_load={load}" in scans[0], (sql, plan)
        cached = sum(enabled for _, enabled in branches)
        assert sum("mode=column" in line for line in nodes) == (
            0 if grouped else cached
        ), (sql, plan)
        if grouped:
            assert sum("GroupAggregate" in line for line in nodes) == (
                cached
            ), (sql, plan)

    def do_subquery_hint_scope(self):
        tdSql.execute("create stable hint_peer(ts timestamp, v int) "
                      "tags(t int)")
        tdSql.execute("create table hint_peer_0 using hint_peer tags(0)")
        tdSql.execute("insert into hint_peer_0 values "
                      "(1704067300000,7) (1704067300001,9)")
        hints = ("", self.hint + " ")
        for clause in ("", " partition by tbname", " group by tbname"):
            grouped = bool(clause)
            if grouped:
                aggregates = [r[1:] for r in self.expected(
                    lambda r: (r["tbname"],))]
            else:
                aggregates = [self.aggregate(self.records)]
            for function, index in (("last", 6), ("last_row", 7)):
                values = [r[index] for r in aggregates if r[index] is not None]
                expected = [(sum(r[0] for r in aggregates),
                             sum(values) if values else None)]
                body = f"count(*) n, {function}(v) v from st{clause}"
                for mask in range(8):
                    inner = "select " + hints[(mask >> 2) & 1] + body
                    middle = ("select " + hints[(mask >> 1) & 1] +
                              f"n, v from ({inner})")
                    sql = ("select " + hints[mask & 1] +
                           f"sum(n), sum(v) from ({middle})")
                    self.check_subquery_hint_plan(
                        sql, (("st", not (mask & 4)),), grouped)
                    self.check_rows(sql, expected)

                    left = "select " + hints[(mask >> 1) & 1] + body
                    peer = body.replace("from st", "from hint_peer", 1)
                    right = "select " + hints[(mask >> 2) & 1] + peer
                    sql = ("select " + hints[mask & 1] +
                           f"sum(n), sum(v) from ({left} union all {right})")
                    self.check_subquery_hint_plan(sql, (
                        ("st", not (mask & 2)),
                        ("hint_peer", not (mask & 4)),
                    ), grouped)
                    count, value = expected[0]
                    self.check_rows(sql, [(count + 2, (value or 0) + 9)])
        tdSql.execute("drop stable hint_peer")
        print("subquery hint scope and EXPLAIN ......... [ passed ]")

    def do_grouping(self, cached):
        cases = (
            ("tbname", lambda r: (r["tbname"],)),
            ("t", lambda r: (r["t"],)),
            ("t, label, city", lambda r: (r["t"], r["label"], r["city"])),
            ("t, tbname", lambda r: (r["t"], r["tbname"])),
            ("t % 2, substr(label,1,1)",
             lambda r: (None if r["t"] is None else r["t"] % 2,
                        None if r["label"] is None else r["label"][:1])),
        )
        for clause in ("partition by", "group by"):
            for keys, key in cases:
                for include_key in (True, False):
                    select = f"{keys}, " if include_key else ""
                    sql = (f"select {select}{self.projection} from st "
                           f"{clause} {keys}")
                    self.check_plan(sql, cached)
                    self.check_rows(sql, self.expected(key, None, include_key))
        print("grouped cache split keys ................ [ passed ]")

    def do_nondeterministic_keys(self):
        tdSql.execute("create stable random_st(ts timestamp, v int) "
                      "tags(t int)")
        try:
            tdSql.execute("create table random_ct using random_st tags(1)")
            tdSql.execute("insert into random_ct values(1704067200000,7)")
            for clause in ("partition by", "group by"):
                for key in ("t + rand()", "abs(t + rand())", "t + rand(1)"):
                    sql = ("select count(*), last(v) from random_st "
                           f"{clause} {key}")
                    self.check_plan(sql, False)
                    self.check_rows(sql, [(1, 7)])
        finally:
            tdSql.execute("drop stable random_st")
        print("grouped cache random key fallback ....... [ passed ]")

    def do_mixed_merge_dependencies(self):
        aggregate = self.aggregate(self.records)
        groups = self.expected(lambda r: (r["t"],), include_key=False)
        values = [aggregate[6]] + [r[6] for r in groups]
        values = [v for v in values if v is not None]
        expected = [(aggregate[0] + sum(r[0] for r in groups),
                     sum(values) if values else None)]
        plain = "select count(*) n, last(v) v from st"
        grouped = plain + " partition by t"
        for left, right in ((plain, grouped), (grouped, plain)):
            sql = ("select sum(n), sum(v) from "
                   f"({left} union all {right})")
            tdSql.query("explain verbose true " + sql)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            nodes = [(len(line) - len(line.lstrip()), line)
                     for line in plan.splitlines() if "-> " in line]
            columns = [i for i, (_, line) in enumerate(nodes)
                       if "mode=column" in line]
            assert len(columns) == 1, (sql, plan)
            start = columns[0]
            end = next((i for i in range(start + 1, len(nodes))
                        if nodes[i][0] <= nodes[start][0]), len(nodes))
            for branch in (nodes[start:end], nodes[:start] + nodes[end:]):
                assert sum("Table Scan on st " in line
                           for _, line in branch) == 1, (sql, plan)
                assert any("Last Row Scan on st " in line
                           for _, line in branch), (sql, plan)
            self.check_rows(sql, expected)
        print("mixed merge dependencies ............... [ passed ]")

    def do_projected_consumers(self):
        rows = self.expected(lambda r: (r["tbname"],))
        expected = sorted([(r[1], r[7], r[0]) for r in rows],
                          key=lambda r: (-r[0], r[2]))
        buckets = defaultdict(list)
        for row in rows:
            buckets[row[1]].append(row[7])
        regrouped = []
        for count, values in buckets.items():
            nonnull = [v for v in values if v is not None]
            regrouped.append((count, len(values),
                              sum(nonnull) if nonnull else None))
        inner = ("select last(v) latest, tbname name, count(*) n "
                 "from st partition by tbname")
        for use_hint in (False, True):
            body = self.with_hint(inner) if use_hint else inner
            sql = (f"select n, latest, name from ({body}) "
                   "order by 1 desc, 3")
            self.check_plan(sql, not use_hint)
            tdSql.query(sql)
            actual = [tuple(row) for row in tdSql.queryResult]
            assert actual == expected, (sql, actual, expected)
            sql = ("select n, count(*) group_count, sum(latest) total "
                   f"from ({body}) group by n order by 1")
            self.check_plan(sql, not use_hint)
            self.check_rows(sql, regrouped)
        print("projected consumer bindings ............ [ passed ]")

    def do_hidden_having(self):
        for clause, key in (
            ("partition by tbname", lambda r: (r["tbname"],)),
            ("group by t", lambda r: (r["t"],)),
        ):
            cases = (
                ("last(w), sum(v)",
                 "count(*) > 2 and last_row(v) is null",
                 lambda a: a[0] > 2 and a[7] is None,
                 lambda a: (a[8], a[2])),
                ("count(*), last(w)", "sum(w) > 0 and last(v) > 50",
                 lambda a: a[5] is not None and a[5] > 0 and
                           a[6] is not None and a[6] > 50,
                 lambda a: (a[0], a[8])),
            )
            for projection, condition, predicate, output in cases:
                expected = [output(a) for a in self.expected(
                    key, include_key=False, having=predicate)]
                sql = f"select {projection} from st {clause} having {condition}"
                for use_hint in (False, True):
                    self.check_plan(sql, not use_hint, use_hint=use_hint)
                    self.check_rows(sql, expected, use_hint=use_hint)
        print("hidden HAVING aggregate bindings ........ [ passed ]")

    def do_having_and_consumers(self):
        sql = ("select tbname as table_name, count(*) as row_count, "
               "last(v) as latest_value from st partition by tbname")
        expected = [(row[0], row[1], row[7]) for row in self.expected(
            lambda r: (r["tbname"],))]
        self.check_rows(sql, expected)
        assert [column[0] for column in tdSql.cursor.description] == [
            "table_name", "row_count", "latest_value"]
        sql = (f"select tbname, {self.projection} from st "
               "partition by tbname having count(*) > 2 "
               "and last(v) > 50")
        expected = self.expected(
            lambda r: (r["tbname"],),
            having=lambda a: a[0] > 2 and a[6] is not None and a[6] > 50,
        )
        self.check_plan(sql)
        self.check_rows(sql, expected)
        self.check_rows(sql + " order by tbname", sorted(expected))

        for index, expression in ((7, "last_row(v)"), (8, "last_row(w)")):
            for negate in (False, True):
                condition = "is not null" if negate else "is null"
                query = (f"select tbname, {self.projection} from st "
                         f"partition by tbname having {expression} "
                         f"{condition}")
                self.check_rows(query, self.expected(
                    lambda r: (r["tbname"],),
                    having=lambda a: (a[index] is not None) == negate))

        expected = {r[0]: (r[1], r[7]) for r in
                    self.expected(lambda r: (r["tbname"],))}
        for use_hint in (False, True):
            for offset in (1, len(expected) - 1, len(expected)):
                sql = ("select tbname, count(*), last(v) from st "
                       f"partition by tbname slimit 3 soffset {offset}")
                self.check_plan(sql, False, use_hint=use_hint)
                tdSql.query(self.with_hint(sql) if use_hint else sql)
                tdSql.checkRows(min(3, len(expected) - offset))
                for name, count, last in tdSql.queryResult:
                    assert (count, last) == expected[name]

        sql = ("select tbname, count(*), last(v) from st "
               "group by tbname order by tbname limit 3 offset 1")
        expected = sorted(self.expected(lambda r: (r["tbname"],)))[1:4]
        self.check_rows(sql, [(r[0], r[1], r[7]) for r in expected])

        sql = ("select sum(n), sum(l) from "
               "(select count(*) n, last(v) l from st partition by tbname)")
        values = self.expected(lambda r: (r["tbname"],))
        self.check_rows(sql, [(sum(r[1] for r in values),
                               sum(r[7] or 0 for r in values))])
        sql = ("select cast(_wstart as bigint), sum(n) from "
               "(select last_row(ts) as ts, count(*) as n from st "
               "partition by tbname) interval(1s)")
        windows = defaultdict(int)
        for table in self.tables:
            records = [r for r in self.records
                       if r["tbname"] == table["tbname"]]
            if records:
                newest = max(r["ts"] for r in records)
                windows[newest // 1000 * 1000] += len(records)
        self.check_rows(sql, list(windows.items()))
        self.check_plan(sql, False)
        sql = (f"select tbname, {self.projection} from st "
               "where t = 1 partition by tbname")
        rows = [r for r in self.records if r["t"] == 1]
        self.check_plan(sql)
        tables = [t for t in self.tables if t["t"] == 1]
        self.check_rows(sql, self.expected(lambda r: (r["tbname"],), rows,
                                          tables=tables))
        sql = (f"select tbname, {self.projection} from st "
               "where tbname = 'g16' partition by tbname")
        self.check_rows(sql, [("g16",) + self.aggregate([])])
        print("grouped cache split consumers ........... [ passed ]")

    def do_fallbacks(self):
        cases = (
            ("v", "", lambda r: (r["v"],), self.records),
            ("t, v", "", lambda r: (r["t"], r["v"]), self.records),
            ("tbname", "where v > 10", lambda r: (r["tbname"],),
             [r for r in self.records if r["v"] is not None and r["v"] > 10]),
            ("tbname", "where ts >= 1704067200100",
             lambda r: (r["tbname"],), self.records[4:]),
        )
        for keys, where, key, records in cases:
            sql = (f"select {keys}, {self.projection} from st {where} "
                   f"partition by {keys}")
            self.check_plan(sql, False)
            tables = self.tables if keys == "tbname" else []
            self.check_rows(sql, self.expected(key, records, tables=tables))
        print("grouped cache split fallbacks ........... [ passed ]")

    def do_additional_aggregates(self):
        sql = ("select tbname, count(*), sum(v), last(v+1), last(w), "
               "last_row(v) from st partition by tbname")
        expected = [(r[0], r[1], r[3],
                     None if r[7] is None else r[7] + 1, r[9], r[8])
                    for r in self.expected(lambda r: (r["tbname"],))]
        self.check_plan(sql)
        self.check_rows(sql, expected)
        sql = ("select tbname, count(*), last(v+1) from st "
               "partition by tbname")
        self.check_plan(sql, False)
        self.check_rows(sql, [(r[0], r[1], r[3]) for r in expected])
        sql = ("select tbname, count(distinct v), last(v)+1, "
               "last_row(w) from st partition by tbname")
        groups = defaultdict(list)
        for table in self.tables:
            groups[table["tbname"]]
        for row in self.records:
            groups[row["tbname"]].append(row)
        expected = []
        for name, records in groups.items():
            if not records:
                continue
            aggregate = self.aggregate(records)
            count = len({r["v"] for r in records if r["v"] is not None})
            last = None if aggregate[6] is None else aggregate[6] + 1
            expected.append((name, count, last, aggregate[8]))
        self.check_rows(sql, expected)
        sql = ("select tbname, count(*), cast(first(ts) as bigint), "
               "cast(last(ts) as bigint), cast(last_row(ts) as bigint) "
               "from st partition by tbname")
        self.check_plan(sql)
        self.check_rows(sql, [
            (name, len(rows), min(r["ts"] for r in rows) if rows else None,
             max(r["ts"] for r in rows) if rows else None,
             max(r["ts"] for r in rows) if rows else None)
            for name, rows in groups.items()
        ])
        print("grouped cache split aggregates .......... [ passed ]")

    def do_union(self):
        for keys, key in (
            ("tbname", lambda r: (r["tbname"],)),
            ("t", lambda r: (r["t"],)),
        ):
            sql = (f"select {keys}, {self.projection} from st "
                   f"partition by {keys}")
            expected = self.expected(key)
            self.check_rows(f"{sql} union all {sql}", expected * 2)
            self.check_rows(f"{sql} union {sql}", expected)
        print("grouped cache split union ............... [ passed ]")

    def do_run(self):
        for mode in ("both", "last_value", "last_row", "none"):
            self.prepare(mode)
            try:
                self.do_grouping(mode != "none")
                if mode == "both":
                    self.do_hint_control()
                    self.do_ungrouped_hint_control()
                    self.do_subquery_hint_scope()
                    self.do_mixed_merge_dependencies()
                    self.do_projected_consumers()
                    self.do_hidden_having()
                    self.do_nondeterministic_keys()
                    self.do_having_and_consumers()
                    self.do_fallbacks()
                    self.do_additional_aggregates()
                    self.do_union()
                    self.do_wide_values()
                    self.do_cache_lifecycle()
            finally:
                tdSql.execute(f"drop database if exists {self.db}")

    def do_wide_values(self):
        tdSql.execute("drop stable if exists wide")
        tdSql.execute("create stable wide(ts timestamp, v varchar(16384), "
                      "u nchar(100)) tags(t int)")
        groups = defaultdict(list)
        for i in range(8):
            tag = None if i in (0, 4) else i % 3
            tdSql.execute(f"create table w{i} using wide "
                          f"tags({self.sql_value(tag)})")
            for j in range(3):
                ts = 1704067200000 + i * 100 + j
                value = None if tag is None else str(ts) + "x" * 15000
                text = None if j == 2 else f"杭州{i}_{j}"
                tdSql.execute(f"insert into w{i} values ({ts},"
                              f"{self.sql_value(value)},"
                              f"{self.sql_value(text)})")
                groups[tag].append((ts, value, text))
        sql = ("select t, count(*), first(v), last(v), last(u) "
               "from wide group by t")
        self.check_plan(sql)
        tdSql.execute(f"flush database {self.db}")
        expected = []
        for key, records in groups.items():
            ordered = sorted(records)
            strings = [r[1] for r in ordered if r[1] is not None]
            unicode = [r[2] for r in ordered if r[2] is not None]
            expected.append((key, len(records),
                             strings[0] if strings else None,
                             strings[-1] if strings else None,
                             unicode[-1] if unicode else None))
        self.check_rows(sql, expected)
        tdSql.execute("drop stable wide")

    def restart_nodes(self):
        tdSql.query("show dnodes")
        nodes = [row[0] for row in tdSql.queryResult]
        for node in nodes:
            sc.dnodeStop(node)
        for node in nodes:
            sc.dnodeStart(node)
        tdSql.execute(f"use {self.db}")

    def do_cache_lifecycle(self):
        tdSql.execute(f"flush database {self.db}")
        self.restart_nodes()
        self.do_grouping(True)

        # Prime caches before deleting both whole tables and latest values.
        tdSql.execute("delete from g00")
        tdSql.execute("delete from g01")
        tdSql.execute("delete from g02 where ts = 1704067200203")
        tdSql.execute("delete from g03 where ts >= 1704067200302")
        self.records = [r for r in self.records if
                        r["tbname"] not in ("g00", "g01") and
                        not (r["tbname"] == "g02" and
                             r["ts"] == 1704067200203) and
                        not (r["tbname"] == "g03" and
                             r["ts"] >= 1704067200302)]
        for stage in ("deleted", "flushed", "restarted"):
            if stage == "flushed":
                tdSql.execute(f"flush database {self.db}")
            elif stage == "restarted":
                self.restart_nodes()
            self.do_grouping(True)
            for clause in ("partition by", "group by"):
                sql = ("select tbname, count(*), last(v), last_row(v) "
                       f"from st where tbname in ('g00', 'g01') "
                       f"{clause} tbname")
                self.check_plan(sql)
                self.check_rows(sql, [("g00", 0, None, None),
                                      ("g01", 0, None, None)])
        print("grouped cache restart and DELETE ........ [ passed ]")

    def test_grouped_cache_split(self):
        """Split grouped cache scans by default with a SELECT hint to disable

        1. Check NULL, composite and hidden keys against Python aggregates
        2. Check HAVING, subqueries, group limits and empty tables
        3. Check cache modes and ordinary-column/time-filter fallbacks
        4. Check wide VARCHAR and NCHAR through automatic rewriting
        5. Verify flush, restart and DELETE including fully deleted tables
        6. Check grouped and ungrouped splitting and SELECT hint scope
        7. Check nested and UNION subquery hints against EXPLAIN scan counts
        8. Check mixed merges, reordered consumers and hidden HAVING aggregates

        Catalog:
            - Query:PartitionBy

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Add grouped cache aggregate split regression
            - 2026-10-28 Check automatic rewriting and cache lifecycle
            - 2026-11-03 Require the SELECT concurrency hint
            - 2026-11-06 Cover random keys and hinted SLIMIT/SOFFSET fallback
            - 2026-11-09 Default concurrency and disable either split by hint

        """
        self.do_run()
