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

import math
import re


class TestIntervalLimitScan:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.base_ts = 1750032000000
        cls.database = "interval_limit_scan"

    def prepare(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(
            f"create database {self.database} vgroups 2 keep 365000 "
            "minrows 100 maxrows 200"
        )
        tdSql.execute(f"use {self.database}")
        tdSql.execute("create stable meters(ts timestamp, v int) tags(fleet int)")
        for table in range(4):
            tdSql.execute(f"create table t{table} using meters tags({table % 2})")
            for minute in range(24):
                # Different starting windows and sparse data prevent one
                # table's local prefix from being mistaken for a global one.
                bucket = minute + 2 * table if table < 3 else 15 + 3 * minute
                values = []
                for row in range(300):
                    ts = self.base_ts + bucket * 60000 + row * 150
                    value = "NULL" if minute == 8 else table * 100000 + minute * 1000 + row
                    values.append(f"({ts},{value})")
                tdSql.execute(f"insert into t{table} values " + " ".join(values))

    def plan(self, sql):
        tdSql.query("explain verbose true " + sql)
        return "\n".join(str(row[0]) for row in tdSql.queryResult)

    def check_prefix(self, sql, limit, offset=0, prune=True, group=False):
        tdSql.query(sql)
        full = list(tdSql.queryResult)
        if group:
            counts = {}
            expected = []
            for row in full:
                key = row[0]
                index = counts.get(key, 0)
                if offset <= index < offset + limit:
                    expected.append(row)
                counts[key] = index + 1
        else:
            expected = full[offset:offset + limit]

        limited = f"{sql} limit {limit} offset {offset}"
        assert "interval_limit=" not in self.plan(limited)
        tdSql.query(limited)
        unhinted = list(tdSql.queryResult)
        limited = limited.replace("select ", "select /*+ INTERVAL_LIMIT() */ ", 1)
        plan = self.plan(limited)
        assert ("interval_limit=" in plan) == prune, plan
        tdSql.query(limited)
        actual = list(tdSql.queryResult)
        assert actual == unhinted, (limited, actual, unhinted)
        assert len(actual) == len(expected), (limited, actual, expected)
        for row, reference in zip(actual, expected):
            for value, wanted in zip(row, reference):
                if isinstance(value, float) and isinstance(wanted, (int, float)):
                    assert math.isclose(value, wanted, rel_tol=1e-10, abs_tol=1e-10)
                else:
                    assert value == wanted, (limited, row, reference)

    def do_ordered_prefixes(self):
        for table in ("meters", "t0", "t3"):
            for direction in ("asc", "desc"):
                sql = (
                    f"select _wstart, sum(v), avg(v), min(v), max(v), count(v) "
                    f"from {table} interval(1m) order by _wstart {direction}"
                )
                # A reverse output over an ascending scan must keep the full
                # input; only an already matching scan order can be pruned.
                self.check_prefix(sql, 5, prune=direction == "asc")
                self.check_prefix(sql, 3, 2, prune=direction == "asc")
        self.check_prefix(
            "select _wstart, max(v), count(v) from meters "
            "where v >= 6000 interval(1m) order by _wstart", 5
        )
        self.check_prefix(
            f"select _wstart, max(v) from t0 where ts >= {self.base_ts + 420000} "
            "interval(1m) order by _wstart", 3
        )
        self.check_prefix(
            "select fleet, _wstart, max(v), sum(v), count(v) from meters "
            "partition by fleet interval(1m)",
            3, 1, prune=False, group=True
        )
        self.check_prefix(
            "select _wstart, count(v), max(v) from meters interval(1m)",
            0, prune=False
        )
        print("ordered interval prefixes ......................... [ passed ]")

    def do_fallbacks(self):
        self.check_prefix(
            "select _wstart, sum(v) from meters interval(1m) "
            "having sum(v) >= 0 order by _wstart",
            3, prune=False
        )
        assert "interval_limit=" not in self.plan(
            "select /*+ INTERVAL_LIMIT() */ _wstart, sum(v) from meters interval(1m) "
            "having sum(v) > 100000000 order by _wstart limit 3"
        )
        self.check_prefix(
            "select _wstart, max(v) from meters interval(1m) "
            "order by max(v) desc, _wstart", 5, prune=False
        )
        self.check_prefix(
            "select _wstart, sum(v) from meters interval(1m) "
            "sliding(30s) order by _wstart", 5, prune=False
        )
        self.check_prefix(
            f"select _wstart, sum(v) from t3 where ts >= {self.base_ts} "
            f"and ts < {self.base_ts + 7200000} interval(1m) fill(prev) "
            "order by _wstart", 5, prune=False
        )
        print("interval limit fallbacks ......................... [ passed ]")

    def do_scan_reduction(self):
        # An expression forces raw data so check_rows measures actual input,
        # including on clean blocks that could otherwise use block SMA.
        sql = "select _wstart, max(v + 1) from meters interval(1m)"
        rows = []
        for hint in ("", "/*+ INTERVAL_LIMIT() */ "):
            query = sql.replace("select ", "select " + hint, 1) + " limit 5"
            tdSql.query("explain analyze verbose true " + query)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            counts = [int(n) for n in re.findall(r"check_rows=(\d+)", plan)]
            assert counts, plan
            rows.append(sum(counts))
        assert rows[1] < rows[0] * 0.7, rows
        print(f"interval scan reduction {rows} ......................... [ passed ]")

    def do_flush(self):
        tdSql.execute(f"flush database {self.database}")

    def do_hint_scope(self):
        inner = "select _wstart as ts, max(v) as v from meters interval(1m) limit 5"
        sql = f"select /*+ INTERVAL_LIMIT() */ * from ({inner})"
        assert "interval_limit=" not in self.plan(sql)
        sql = "select * from (" + inner.replace("select ", "select /*+ interval_limit() */ ", 1) + ")"
        assert "interval_limit=5" in self.plan(sql)
        assert "interval_limit=" not in self.plan(
            "select /*+ INTERVAL_LIMIT(1) */ _wstart, max(v) from meters interval(1m) limit 5"
        )
        tdSql.query(inner)
        expected = list(tdSql.queryResult)
        combined = inner.replace("select ", "select /*+ FAST_INTERVAL() INTERVAL_LIMIT() */ ", 1)
        assert "interval_limit=5" in self.plan(combined)
        tdSql.query(combined)
        assert tdSql.queryResult == expected

    def do_write_memory_rows(self):
        tdSql.execute(
            f"insert into t0 values({self.base_ts + 1000}, 999999) "
            f"({self.base_ts + 60000 * 100}, 999999)"
        )

    def test_interval_limit_scan(self):
        """Opt in to pruning complete ordered interval windows with a hint

        1. Compare LIMIT/OFFSET with full results for dense and sparse tables
        2. Keep complete boundary windows, filtered rows and tag-group merges
        3. Preserve HAVING, FILL, sliding and aggregate-sort fallbacks
        4. Verify that file and mixed file/memory scans read fewer rows
        5. Keep pruning disabled by default and scope hints to their SELECT

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 Codex Added ordered interval scan prefix checks

        """
        self.prepare()
        self.do_hint_scope()
        self.do_ordered_prefixes()
        self.do_fallbacks()
        self.do_flush()
        self.do_ordered_prefixes()
        self.do_scan_reduction()
        self.do_write_memory_rows()
        self.do_ordered_prefixes()
        self.do_scan_reduction()
