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
import re


class TestVtablePlanOrderLimit:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.db = "vtable_order_limit"
        cls.base_ts = 1750046400000
        cls.size = 20003
        cls.rows = {}
        tdSql.execute(f"drop database if exists {cls.db}")
        tdSql.execute(f"create database {cls.db} vgroups 2 keep 36500")
        tdSql.execute(f"use {cls.db}")
        tdSql.execute("create stable vst (ts timestamp, x int, y int) tags (id int) virtual 1")
        for child in range(4):
            for prefix in ("src", "aux"):
                tdSql.execute(f"create table {prefix}{child} (ts timestamp, v int)")
            rows = []
            for i in range(cls.size if child < 3 else 0):
                ts = cls.base_ts + i * 10 + child
                x = child * 10000 + i
                y = -x if i % 2 == 0 else None
                rows.append((ts, x, y))
            if rows:
                rows.append((cls.base_ts + cls.size * 10 + child, None, -99999))
            cls.rows[child] = rows
            for start in range(0, len(rows), 500):
                batch = rows[start:start + 500]
                for prefix, col in (("src", 1), ("aux", 2)):
                    values = " ".join(f"({r[0]},{r[col]})" for r in batch if r[col] is not None)
                    if values:
                        tdSql.execute(f"insert into {prefix}{child} values {values}")
            tdSql.execute(
                f"create vtable vc{child} (x from {cls.db}.src{child}.v, "
                f"y from {cls.db}.aux{child}.v) using vst tags ({child})"
            )
        tdSql.execute(
            f"create vtable vn (ts timestamp, x int from {cls.db}.src0.v, "
            f"y int from {cls.db}.aux0.v)"
        )

    def plan(self, sql):
        tdSql.query(f"explain verbose true {sql}")
        return "\n".join(str(row[0]) for row in tdSql.queryResult)

    def plan_nodes(self, plan, operator):
        lines = plan.splitlines()
        nodes = []
        for i, header in enumerate(lines):
            if not header.lstrip().startswith(f"-> {operator}"):
                continue
            output = None
            for detail in lines[i + 1:]:
                if detail.lstrip().startswith("-> "):
                    break
                if detail.lstrip().startswith("Output: columns="):
                    output = detail
                    break
            assert output is not None, plan
            nodes.append((header, output))
        return nodes

    def check_plan_limit(self, plan, operator, limit, count=None):
        nodes = self.plan_nodes(plan, operator)
        assert nodes, plan
        if count is not None:
            assert len(nodes) == count, plan
        for _, output in nodes:
            match = re.search(r"\blimit=(-?\d+)\b", output)
            actual = int(match.group(1)) if match else None
            assert actual == limit, plan
        return nodes

    def check_source_limit(self, plan, limit):
        self.check_plan_limit(plan, "Table Scan on ", limit)

    def check_plan_rows(self, plan, operator, rows, count):
        nodes = self.plan_nodes(plan, operator)
        assert len(nodes) == count, plan
        for header, _ in nodes:
            match = re.search(r"\brows=([\d.]+)", header)
            assert match and float(match.group(1)) == rows, plan

    def do_limit_pushdown_explain(self, table, order, scenario):
        predicate = ""
        if scenario == "time_filter":
            predicate = f" where ts >= {self.base_ts + 10000} and ts < {self.base_ts + 50000}"
        elif scenario == "merged_filter":
            predicate = " where x < 30 and y is not null"
        sort_key = f"ts {order}"
        if scenario == "multiple_sort_keys":
            sort_key += ", x asc"
        elif scenario == "value_sort":
            sort_key = f"x {order}"
        finite_limit = scenario != "no_limit"
        offset = 5 if scenario == "offset" else 0
        sql = f"select * from {self.db}.{table}{predicate} order by {sort_key}"
        if finite_limit:
            sql += f" limit 7 offset {offset}"
        plan = self.plan(sql)

        timestamp_sort = scenario not in ("multiple_sort_keys", "value_sort")
        virtual_limit = 7 + offset if finite_limit and timestamp_sort else None
        source_limit = None if scenario == "merged_filter" else virtual_limit
        self.check_plan_limit(plan, "Virtual Table Scan on ", virtual_limit, 1)
        sources = self.check_plan_limit(plan, "Table Scan on ", source_limit,
                                        1 if table == "vst" else 2)
        ordered_scan = timestamp_sort and (table != "vst" or finite_limit)
        scan_order = order if ordered_scan else "asc"
        expected_order = "order=[asc|0 desc|1]" if scan_order == "desc" else "order=[asc|1 desc|0]"
        assert all(expected_order in header for header, _ in sources), plan

        sorts = self.plan_nodes(plan, "Sort")
        keep_sort = table == "vst" or not timestamp_sort
        assert len(sorts) == int(keep_sort), plan
        if keep_sort:
            self.check_plan_limit(plan, "Sort", 7 + offset if finite_limit else None, 1)
        if finite_limit:
            root = self.check_plan_limit(plan, "Projection", 7, 1)
            match = re.search(r"\boffset=(-?\d+)\b", root[0][1])
            assert (int(match.group(1)) if match else 0) == offset, plan
        for _, output in sources + self.plan_nodes(plan, "Virtual Table Scan on "):
            match = re.search(r"\boffset=(-?\d+)\b", output)
            assert match is None or int(match.group(1)) == 0, plan
        print(f"explain {table}/{order}/{scenario} ................. [ passed ]")

    def check_single(self, table, order, limit=None, offset=0, predicate="", keep=lambda r: True):
        expected = sorted(filter(keep, self.rows[0]), reverse=order == "desc")
        sql = f"select cast(ts as bigint), x, y from {self.db}.{table}{predicate} order by ts {order}"
        if limit is not None:
            sql += f" limit {limit} offset {offset}"
            expected = expected[offset:offset + limit]
        plan = self.plan(sql)
        assert "Virtual Table Scan" in plan, plan
        assert "Sort" not in plan, plan
        self.check_source_limit(plan, None if predicate else
                                (None if limit is None else limit + offset))
        tdSql.query(sql)
        assert tdSql.queryResult == expected, (sql, tdSql.queryResult[:10], expected[:10])

    def do_single_table(self):
        for table in ("vn", "vc0"):
            for order in ("asc", "desc"):
                self.check_single(table, order)
                self.check_single(table, order, 1)
                self.check_single(table, order, 7, 3)
                self.check_single(table, order, 20, self.size - 2)
                self.check_single(table, order, 7, predicate=" where x < 30 and y is not null",
                                  keep=lambda r: r[1] is not None and r[1] < 30 and r[2] is not None)
        print("single virtual table ordered scan ................ [ passed ]")

    def check_super(self, order, limit, offset=0, predicate="", keep=lambda r: True):
        expected = [(ts, x, y, child) for child, rows in self.rows.items() for ts, x, y in rows]
        expected = sorted(filter(keep, expected), key=lambda r: r[0], reverse=order == "desc")
        expected = expected[offset:offset + limit]
        sql = (f"select cast(ts as bigint), x, y, id from {self.db}.vst{predicate} "
               f"order by ts {order} limit {limit} offset {offset}")
        plan = self.plan(sql)
        assert "Virtual Table Scan" in plan, plan
        assert "Sort" in plan, plan
        if limit > 0:
            lines = plan.splitlines()
            scan_output = [lines[i + 1] for i, line in enumerate(lines[:-1])
                           if "Virtual Table Scan" in line]
            assert any(f"limit={limit + offset}" in line for line in scan_output), plan
            assert ("order=[asc|0 desc|1]" if order == "desc" else
                    "order=[asc|1 desc|0]") in plan, plan
        if not predicate or "x <" in predicate:
            self.check_source_limit(plan, None if predicate or limit == 0 else limit + offset)
        tdSql.query(sql)
        assert tdSql.queryResult == expected, (sql, tdSql.queryResult[:10], expected[:10])

    def do_super_table(self):
        for order in ("asc", "desc"):
            for limit, offset in ((1, 0), (7, 0), (9, 5), (7, 13000), (20000, 0), (0, 0)):
                self.check_super(order, limit, offset)
            self.check_super(order, 7, predicate=" where x < 30 and y is not null",
                             keep=lambda r: r[1] is not None and r[1] < 30 and r[2] is not None)
            self.check_super(order, 5, 2, " where id = 1", keep=lambda r: r[3] == 1)
        tdSql.query(f"select * from {self.db}.vc3 order by ts desc limit 3")
        tdSql.checkRows(0)
        print("virtual super table local top N .................. [ passed ]")

    def do_local_topn_work(self):
        for table in ("vn", "vc0", "vst"):
            for offset in (0, 5):
                cap = 7 + offset
                sql = f"select * from {self.db}.{table} order by ts desc limit 7 offset {offset}"
                tdSql.query(f"explain analyze verbose true {sql}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                self.check_plan_limit(plan, "Virtual Table Scan on ", cap, 1)
                self.check_plan_limit(plan, "Table Scan on ", cap, 1 if table == "vst" else 2)
                self.check_plan_rows(plan, "Projection", 7, 1)
                self.check_plan_rows(plan, "Virtual Table Scan on ",
                                     3 * cap if table == "vst" else cap, 1)
                # Super-table scan statistics average the six nonempty origin
                # readers across the two vgroups; each origin emits cap rows.
                self.check_plan_rows(plan, "Table Scan on ",
                                     3 * cap if table == "vst" else cap,
                                     1 if table == "vst" else 2)
                if table == "vst":
                    self.check_plan_rows(plan, "Sort", cap, 1)
        print("analyze verifies source limits and output rows .... [ passed ]")

    def do_disk_prefix(self):
        tdSql.execute(f"flush database {self.db}")
        for order in ("asc", "desc"):
            self.check_single("vn", order, 7)
            self.check_single("vc0", order, 7, 5)
            self.check_super(order, 7)
            self.check_super(order, 20000)
            self.check_super(order, 7, predicate=" where x < 30 and y is not null",
                             keep=lambda r: r[1] is not None and r[1] < 30 and r[2] is not None)
        print("file block prefixes and filtered fallback ......... [ passed ]")

    def do_precision_fallback(self):
        db = f"{self.db}_us"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} precision 'us' keep 36500")
        tdSql.execute(f"create table {db}.src (ts timestamp, v int)")
        values = " ".join(f"({self.base_ts * 1000 + i * 100}, {i})" for i in range(100))
        tdSql.execute(f"insert into {db}.src values {values}")
        tdSql.execute(f"flush database {db}")
        tdSql.execute(f"create vtable {self.db}.vn_us (ts timestamp, "
                      f"x int from {db}.src.v, y int from {self.db}.aux3.v)")
        tdSql.execute(f"create vtable {self.db}.vc_us (x from {db}.src.v, "
                      f"y from {self.db}.aux3.v) using {self.db}.vst tags (8)")
        for table, predicate in (("vn_us", ""), ("vst", " where id = 8")):
            sql = (f"select cast(ts as bigint) from {self.db}.{table}{predicate} "
                   "order by ts desc limit 7")
            tdSql.query(sql)
            assert tdSql.queryResult == [(self.base_ts + i,) for i in range(9, 2, -1)], sql
        print("finer source timestamps preserve virtual rows ..... [ passed ]")

    def do_shared_source_ties(self):
        tdSql.execute(
            f"create vtable {self.db}.vcdup (x from {self.db}.src0.v, "
            f"y from {self.db}.aux0.v) using {self.db}.vst tags (9)"
        )
        ts, x, y = self.rows[0][-1]
        sql = (f"select cast(ts as bigint), x, y, id from {self.db}.vst "
               "where id in (0, 9) order by ts desc limit 2")
        tdSql.query(sql)
        assert sorted(tdSql.queryResult, key=lambda r: r[3]) == [(ts, x, y, 0), (ts, x, y, 9)]
        sql = (f"select cast(ts as bigint), id from {self.db}.vst "
               "where id in (0, 9) order by ts desc, id asc limit 1 offset 1")
        plan = self.plan(sql)
        lines = plan.splitlines()
        scan_output = [lines[i + 1] for i, line in enumerate(lines[:-1])
                       if "Virtual Table Scan" in line]
        assert all("limit=" not in line for line in scan_output), plan
        tdSql.query(sql)
        assert tdSql.queryResult == [(ts, 9)]
        print("shared sources, ties and multiple sort keys ....... [ passed ]")

    def do_fallback(self):
        rows = [r for values in self.rows.values() for r in values if r[1] is not None]
        expected = sorted(rows, key=lambda r: r[1], reverse=True)[:7]
        sql = f"select cast(ts as bigint), x, y from {self.db}.vst where x is not null order by x desc limit 7"
        tdSql.query(sql)
        assert tdSql.queryResult == expected
        print("non timestamp order fallback ..................... [ passed ]")

    @pytest.mark.parametrize("table", ("vn", "vc0", "vst"))
    @pytest.mark.parametrize("order", ("asc", "desc"))
    @pytest.mark.parametrize("scenario", (
        "limit", "offset", "time_filter", "merged_filter",
        "multiple_sort_keys", "value_sort", "no_limit",
    ))
    def test_vtable_limit_pushdown_explain(self, table, order, scenario):
        """Plan: identify source LIMIT pushdown and safe fallback from EXPLAIN

        1. Check normal, child and super table scans in both timestamp directions
        2. Check exact per-operator LIMIT values and global OFFSET preservation
        3. Check source caps are absent for merged filters and unsupported sort keys
        4. Check Sort removal for single tables and global Sort retention for super tables

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Joey Sima Add EXPLAIN pushdown and fallback assertions

        """
        self.do_limit_pushdown_explain(table, order, scenario)

    def test_vtable_order_limit(self):
        """Query: ordered virtual scans and per-child Top N

        1. Check ascending and descending multi-source scans across block boundaries
        2. Check local Top N, global OFFSET, sparse columns, filters and empty children
        3. Check fallback for sorting by a value column
        4. Check ANALYZE row counts at source, virtual scan, sort and projection layers

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Joey Sima Add ordered scan and Top N coverage

        """
        self.do_single_table()
        self.do_super_table()
        self.do_fallback()
        self.do_local_topn_work()
        self.do_disk_prefix()
        self.do_shared_source_ties()
        self.do_precision_fallback()
