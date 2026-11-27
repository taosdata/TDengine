# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import re

from new_test_framework.utils import tdLog, tdSql


class TestGroupByTagScalar:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_tag_group_reuse(self):
        """Reuse scalar tag group results across scan and aggregation

        Verify selected and implicitly consumed scalar tag group keys on memory
        and flushed data, including repeated queries, NULL tags, multiple keys,
        select-value wrappers, mixed tbname groups, and non-pushdown group keys.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-10-27 Add scalar tag group reuse regression
            - 2026-11-02 Cover mixed tbname and scalar tag groups
        """
        db = "group_tag_scalar_reuse"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 2 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable meters(ts timestamp, current double) "
                "tags(location varchar(32))"
            )
            rows = (
                ("d0", "'California.00000'", "(1451606400000,1)(1451606403000,3)"),
                ("d1", "'California.00000'", "(1451606404000,5)"),
                ("d2", "'California.00011'", "(1451606405000,7)"),
                ("d3", "NULL", "(1451606406000,9)"),
                ("d4", "'x'", "(1451606407000,11)"),
            )
            for table, tag, values in rows:
                tdSql.execute(f"create table {table} using meters tags({tag})")
                tdSql.execute(f"insert into {table} values {values}")

            for round_id in range(2):
                tdLog.info(f"scalar tag group checks after flush round {round_id}")
                self.check_scalar_group_queries()
                tdSql.execute(f"flush database {db}")

            self.check_scalar_group_queries()
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def test_unconsumed_tag_pseudo_columns_pruned(self):
        """Do not retain tag pseudo columns no consumer reads

        Verify that a grouped query whose projection only aggregates a data
        column does not make the scan fetch and fill the group tag column.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-11-23 Add unconsumed tag pseudo column regression
        """
        db = "group_tag_scalar_prune"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 1 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable meters(ts timestamp, v int) "
                "tags(t1 int, label varchar(20))"
            )
            for i in range(4):
                tdSql.execute(
                    f"create table p{i} using meters tags({i % 2},'l{i}')"
                )
                tdSql.execute(
                    f"insert into p{i} values ({1704067200000 + i},{i})"
                )
            for sql in (
                "select sum(v) from meters group by t1",
                "select count(*) from meters group by t1, label",
            ):
                tdSql.query(f"explain verbose true {sql}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                scan_lines = [
                    line for line in plan.splitlines()
                    if "Table Scan on meters" in line
                    or "Table Merge Scan on meters" in line
                ]
                assert len(scan_lines) == 1, plan
                # EXPLAIN omits pseudo_columns entirely when the list is empty.
                assert "pseudo_columns=" not in scan_lines[0], plan
                assert "Aggregate" in plan, plan
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def test_grouped_topic_rejected(self):
        """Reject scalar tag GROUP BY in CREATE TOPIC AS SELECT

        Verify that the parser rejects grouped topics with the expected error
        code and message before scalar tag groups can reach a stream scan.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-10-28 Add grouped topic rejection guard
        """
        db = "group_tag_scalar_topic"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable meters(ts timestamp, current double) "
                "tags(location varchar(32))"
            )
            scalar = "substr(location,11,5)"
            # Guard the unreachable TMQ premise: if grouped topics are enabled,
            # re-review partTags materialization gating for SCAN_TYPE_STREAM.
            tdSql.error(
                f"create topic grouped_tag_scalar as select {scalar},count(*) "
                f"from meters group by {scalar}",
                expectedErrno=0x80002639,
                expectErrInfo="Invalid topic query",
                fullMatched=False,
            )
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def test_scalar_tag_group_shared_keys_and_null(self):
        """Reuse scalar tag group results for shared keys and NULL values

        Verify that tables sharing one scalar group key and NULL tag values are
        answered from the materialized group results across multi-row blocks,
        including repeated execution and integer tag expressions.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-11-13 Add shared key and NULL scalar group coverage
        """
        db = "group_tag_scalar_shared"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 1 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable meters(ts timestamp, v int) "
                "tags(loc varchar(8), level int)"
            )
            rows = (
                ("a", "'aa'", "1", "(1451606400000,1)(1451606401000,2)(1451606402000,3)"),
                ("b", "'aa'", "1", "(1451606403000,4)(1451606404000,5)"),
                ("c", "NULL", "2", "(1451606405000,6)(1451606406000,7)(1451606407000,8)(1451606408000,9)"),
                ("d", "'bb'", "NULL", "(1451606409000,10)"),
            )
            for table, loc, level, values in rows:
                tdSql.execute(f"create table {table} using meters tags({loc},{level})")
                tdSql.execute(f"insert into {table} values {values}")

            scalar = "substr(loc,1,1)"
            selected = (
                f"select {scalar},count(*),sum(v) from meters "
                f"where loc like 'a%' group by {scalar} order by {scalar}"
            )
            for _ in range(2):
                tdSql.query(f"explain verbose true {selected}")
                plan = "\n".join(row[0] for row in tdSql.queryResult)
                scan_lines = [
                    line
                    for line in plan.splitlines()
                    if "Table Scan on meters" in line or "Table Merge Scan on meters" in line
                ]
                assert len(scan_lines) == 1, plan
                assert re.search(r"\bpseudo_columns=2\b", scan_lines[0]), plan

                tdSql.query(selected)
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, "a")
                tdSql.checkData(0, 1, 5)
                tdSql.checkData(0, 2, 15)

            # A NULL tag yields one NULL scalar group key repeated over the
            # four rows of table c.
            tdSql.query(
                f"select {scalar},count(*),sum(v) from meters "
                f"where loc is null group by {scalar}"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 4)
            tdSql.checkData(0, 2, 30)

            # Integer tag expression: selected output and per-group aggregates.
            tdSql.query(
                "select level+1,count(*),sum(v) from meters "
                "where level is not null group by level+1 order by level+1"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, 5)
            tdSql.checkData(0, 2, 15)
            tdSql.checkData(1, 0, 3)
            tdSql.checkData(1, 1, 4)
            tdSql.checkData(1, 2, 30)

            # A NULL integer tag keeps its own NULL group.
            tdSql.query(
                "select level+1,count(*),sum(v) from meters "
                "where level is null group by level+1"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 10)
        finally:
            tdSql.execute(f"drop database if exists {db}")

    @staticmethod
    def check_scalar_group_queries():
        scalar = "substr(location,11,5)"
        selected = (
            f"select {scalar},count(*),avg(current) from meters "
            f"where location like 'California.%' group by {scalar} order by {scalar}"
        )
        tdSql.query(f"explain verbose true {selected}")
        plan = "\n".join(row[0] for row in tdSql.queryResult)
        scan_lines = [
            line
            for line in plan.splitlines()
            if "Table Scan on meters" in line or "Table Merge Scan on meters" in line
        ]
        assert len(scan_lines) == 1, plan
        assert re.search(r"\bpseudo_columns=2\b", scan_lines[0]), plan

        expected = ((".0000", 3, 3.0), (".0001", 1, 7.0))
        for _ in range(2):
            tdSql.query(selected)
            tdSql.checkRows(2)
            for row, values in enumerate(expected):
                for col, value in enumerate(values):
                    tdSql.checkData(row, col, value)

        tdSql.query(
            f"select count(*),avg(current) from meters where location like 'California.%' "
            f"group by {scalar} order by {scalar}"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 3.0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 7.0)

        tdSql.query(
            f"select count(*) from meters group by {scalar} "
            f"having {scalar}='.0001' order by {scalar}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select {scalar},first(current),last(current) from meters "
            f"where location like 'California.%' group by {scalar} order by {scalar}"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, ".0000")
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(0, 2, 5.0)
        tdSql.checkData(1, 0, ".0001")
        tdSql.checkData(1, 1, 7.0)
        tdSql.checkData(1, 2, 7.0)

        expected_scalar_location = (
            (".0000", "California.00000", 3),
            (".0001", "California.00011", 1),
        )
        for keys in (f"{scalar},location", f"location,{scalar}"):
            tdSql.query(
                f"select {scalar},location,count(*) from meters "
                f"where location like 'California.%' group by {keys} order by location"
            )
            tdSql.checkRows(2)
            for row, values in enumerate(expected_scalar_location):
                for col, value in enumerate(values):
                    tdSql.checkData(row, col, value)

        tdSql.query(f"select {scalar},count(*) from meters where location is null group by {scalar}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select substr(tbname,1,1),count(*) from meters group by substr(tbname,1,1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "d")
        tdSql.checkData(0, 1, 6)

        expected = (
            ("d0", ".0000", 2, 2.0),
            ("d1", ".0000", 1, 5.0),
            ("d2", ".0001", 1, 7.0),
            ("d3", None, 1, 9.0),
            ("d4", "", 1, 11.0),
        )
        for expr in (scalar, "substr(upper(location),11,5)"):
            for keys in (f"tbname,{expr}", f"{expr},tbname"):
                mixed = (
                    f"select tbname,{expr},count(*),avg(current) from meters "
                    f"group by {keys}"
                )
                # tbname already separates tables, so the scalar key must not be
                # materialized into an extra scan output. A materialized mixed
                # plan reports a third pseudo column for the scalar result.
                tdSql.query(f"explain verbose true {mixed}")
                plan = "\n".join(row[0] for row in tdSql.queryResult)
                scan_lines = [
                    line
                    for line in plan.splitlines()
                    if "Table Scan on meters" in line
                    or "Table Merge Scan on meters" in line
                ]
                assert len(scan_lines) == 1, plan
                assert re.search(r"\bpseudo_columns=2\b", scan_lines[0]), plan

                tdSql.query(
                    f"select tbname,{expr},count(*),avg(current) from meters "
                    f"group by {keys} order by tbname"
                )
                tdSql.checkRows(5)
                for row, values in enumerate(expected):
                    for col, value in enumerate(values):
                        tdSql.checkData(row, col, value)

                tdSql.query(
                    f"select tbname,count(*) from meters where location like 'California.%' "
                    f"group by {keys} order by {expr},tbname"
                )
                tdSql.checkRows(3)
                for row, (table, count) in enumerate((("d0", 2), ("d1", 1), ("d2", 1))):
                    tdSql.checkData(row, 0, table)
                    tdSql.checkData(row, 1, count)

        expected_current_scalar = (
            (1.0, ".0000", 1),
            (3.0, ".0000", 1),
            (5.0, ".0000", 1),
            (7.0, ".0001", 1),
        )
        for keys in (f"{scalar},current", f"current,{scalar}"):
            tdSql.query(
                f"select current,{scalar},count(*) from meters where location like 'California.%' "
                f"group by {keys} order by current"
            )
            tdSql.checkRows(4)
            for row, values in enumerate(expected_current_scalar):
                for col, value in enumerate(values):
                    tdSql.checkData(row, col, value)


class TestGroupByTagScalarLastCache:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def sql(value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return str(value)

    def test_scalar_group_last_cache(self):
        """Keep last-cache plans for scalar tag group keys

        Verify that materialized scalar tag group keys do not disable the
        last/last_row cache scan (including the aggregate split path) and that
        the results stay equal to a Python aggregate.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-11-23 Add scalar group key last-cache regression
        """
        db = "group_tag_scalar_last_cache"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(
                f"create database {db} vgroups 1 keep 5000 cachemodel 'both'"
            )
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable st(ts timestamp, v int, w int) "
                "tags(t int, label varchar(20))"
            )
            tables = []
            for i in range(6):
                tag = None if i == 5 else i % 3
                label = ("a", "ab", "bc")[i % 3]
                name = f"g{i}"
                tdSql.execute(
                    f"create table {name} using st tags({self.sql(tag)},"
                    f"{self.sql(label)})"
                )
                rows = []
                for j in range(3):
                    ts = 1704067200000 + i * 100 + j
                    v = None if (i + j) % 4 == 0 else i * 10 + j
                    w = i - j
                    rows.append(dict(ts=ts, v=v, w=w))
                    tdSql.execute(
                        f"insert into {name} values ({ts},{self.sql(v)},{w})"
                    )
                tables.append(dict(t=tag, label=label, rows=rows))

            groups = {}
            for table in tables:
                key = (None if table["t"] is None else table["t"] % 2,
                       None if table["label"] is None else table["label"][:1])
                groups.setdefault(key, []).extend(table["rows"])
            expected = []
            for key, rows in groups.items():
                values = [r["v"] for r in rows if r["v"] is not None]
                others = [r["w"] for r in rows]
                newest = max(rows, key=lambda r: r["ts"])
                nonnull = [r for r in rows if r["v"] is not None]
                last = max(nonnull, key=lambda r: r["ts"])["v"] \
                    if nonnull else None
                expected.append((
                    key[0], key[1], len(rows), len(values),
                    sum(values) if values else None,
                    min(others), max(others), sum(others) / len(others),
                    last, newest["v"], newest["w"],
                ))
            expected.sort(key=repr)

            projection = (
                "count(*), count(v), sum(v), min(w), max(w), avg(w), "
                "last(v), last_row(v), last(w)"
            )
            queries = (
                f"select t % 2, substr(label,1,1), {projection} from st "
                f"group by t % 2, substr(label,1,1)",
                "select t % 2, substr(label,1,1), last(v), last_row(v), "
                "last(w) from st group by t % 2, substr(label,1,1)",
                "select t % 2, last(v), last_row(v) from st group by t % 2",
            )
            for index, sql in enumerate(queries):
                tdSql.query(f"explain verbose true {sql}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                assert "Last Row Scan" in plan, (sql, plan)
                if index == 0:
                    # Mixing last/last_row with ordinary aggregates splits the
                    # grouped cache scan into a GroupAggregate branch.
                    assert "GroupAggregate" in plan, (sql, plan)
                else:
                    assert "Aggregate" in plan, (sql, plan)
                tdSql.query(sql)
                actual = sorted([tuple(row) for row in tdSql.queryResult],
                                key=repr)
                assert actual, (sql, actual)
                if index == 0:
                    assert actual == expected, (sql, actual, expected)
                tdSql.query(sql)
                again = sorted([tuple(row) for row in tdSql.queryResult],
                               key=repr)
                assert again == actual, (sql, actual, again)
        finally:
            tdSql.execute(f"drop database if exists {db}")


class TestGroupByTagScalarCache:
    # With the tag group cache enabled the planner keeps the cache-compatible
    # non-materialized plan; repeated grouped scans must use the existing cache
    # path instead of losing the cache to reusable scalar group data.
    updatecfgDict = {"tagFilterCache": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_group_cache_path(self):
        """Keep the tag group cache for repeated scalar group scans

        Verify that enabling tagFilterCache selects the non-materialized plan,
        that cold and hot executions return the same rows, and that a larger
        table list does not add a materialized scan output.

        Since: v3.4.3.3

        Labels: common,ci,integration,functional

        Jira: TD-29705

        History:
            - 2026-11-23 Add scalar group cache cold/hot regression
        """
        db = "group_tag_scalar_cache"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 2 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable cst(ts timestamp, v int) "
                "tags(location varchar(32))"
            )
            groups = {}
            for i in range(120):
                location = f"loc{(i % 12):02d}.{i:03d}"
                name = f"c{i:03d}"
                tdSql.execute(
                    f"create table {name} using cst tags('{location}')"
                )
                tdSql.execute(
                    f"insert into {name} values "
                    f"({1704067200000 + i},{i})"
                )
                groups.setdefault(location[:3], []).append(i)
            expected = sorted(
                (key, len(values), sum(values))
                for key, values in groups.items()
            )

            sql = ("select /*+ NO_GROUP_KEY_REUSE() */ "
                   "substr(location,1,3), count(*), sum(v) from cst "
                   "group by substr(location,1,3) order by 1")
            tdSql.query(f"explain verbose true {sql}")
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            scan_lines = [
                line for line in plan.splitlines()
                if "Table Scan on cst" in line
            ]
            assert len(scan_lines) == 1, plan
            assert re.search(r"\bpseudo_columns=1\b", scan_lines[0]), plan

            for attempt in range(4):
                tdSql.query(sql)
                actual = [tuple(row) for row in tdSql.queryResult]
                assert actual == expected, (attempt, actual, expected)
        finally:
            tdSql.execute(f"drop database if exists {db}")
