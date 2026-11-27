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

import re

from new_test_framework.utils import tdLog, tdSql


class TestScalarOrRangeResults:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_data(self):
        self.rows = []
        self.base_ts = 1700000000000
        tdSql.execute("drop database if exists scalar_or_range_results")
        tdSql.execute("create database scalar_or_range_results keep 36500 stt_trigger 1")
        tdSql.execute("use scalar_or_range_results")
        tdSql.execute("create table samples (ts timestamp, id int, a bigint, b bigint, c bigint, d double)")

        for group in range(12):
            values = []
            for offset in range(128):
                row_id = group * 128 + offset
                a = None if offset % 41 == 0 else (1, 50, 100)[group % 3]
                b = None if offset % 53 == 0 else (0, 2, 50)[group % 3]
                c = 0 if offset % 7 == 0 else 20
                d = None if offset % 67 == 0 else (1.5 if group % 3 == 0 else 2.5)
                self.rows.append((row_id, a, b, c, d))
                columns = (self.base_ts + row_id, row_id, a, b, c, d)
                values.append("(" + ",".join("NULL" if value is None else str(value) for value in columns) + ")")
            tdSql.execute("insert into samples values " + " ".join(values))
            tdSql.execute("flush database scalar_or_range_results")
        print("scalar OR range data prepared [passed]")

    def compare_rows(self, condition, predicate):
        tdSql.query(f"select id,a,b,c,d from samples where {condition} order by ts")
        actual = [tuple(row) for row in tdSql.queryResult]
        expected = [row for row in self.rows if predicate(row)]
        assert actual == expected, (
            f"{condition}: expected {len(expected)} rows, got {len(actual)}; "
            f"first mismatch: {next(((i, lhs, rhs) for i, (lhs, rhs) in enumerate(zip(actual, expected)) if lhs != rhs), None)}"
        )

    def compare_all_results(self):
        nonnull = lambda value: value is not None
        cases = [
            ("abs(c)>0 and (a=1 or b=2)",
             lambda row: row[3] > 0 and (row[1] == 1 or row[2] == 2)),
            ("abs(c)>0 and (a<10 or a>90)",
             lambda row: row[3] > 0 and nonnull(row[1]) and (row[1] < 10 or row[1] > 90)),
            ("abs(c)>0 and (a=1 or abs(b)=2)",
             lambda row: row[3] > 0 and (row[1] == 1 or (nonnull(row[2]) and abs(row[2]) == 2))),
            ("abs(c)>0 and (a is null or b=2)",
             lambda row: row[3] > 0 and (row[1] is None or row[2] == 2)),
            ("abs(c)>0 and (d='1.5' or b!=0)",
             lambda row: row[3] > 0 and (row[4] == 1.5 or (nonnull(row[2]) and row[2] != 0))),
            ("abs(c)>0 and (d='1.5e0' or b=2)",
             lambda row: row[3] > 0 and (row[4] == 1.5 or row[2] == 2)),
            ("abs(c)>0 and (a=cast(1 as decimal(18,0)) or b=2)",
             lambda row: row[3] > 0 and (row[1] == 1 or row[2] == 2)),
        ]
        for condition, predicate in cases:
            self.compare_rows(condition, predicate)
        print("scalar OR range ordered results [passed]")

    def test_scalar_or_range_results(self):
        """Compare full ordered scalar OR results against an independent row oracle.

        1. Insert fixed rows with separated value ranges and NULLs, then flush each range.
        2. Compare every returned column for cross-column, same-column, and function ORs.
        3. Cover numeric strings and DECIMAL constants that require conservative pruning.

        Catalog:
            - Query:Filter

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-04 xsren Add ordered result comparisons for scalar OR range pruning

        """
        self.prepare_data()
        self.compare_all_results()

    def test_subquery_filter_range(self):
        """Prune batch scan blocks using completed subquery results.

        1. Compare scalar, membership and quantified filters on disk blocks with an independent row oracle.
        2. Preserve NULL, empty input and the residual timestamp predicate before aggregation and LIMIT.
        3. Verify pruning after the first block and descending supertable results across child tables.

        Catalog:
            - Query:Filter

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-17 Joey Sima Add runtime subquery range coverage

        """
        db = "subquery_filter_range"
        start = 1730000000000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 minrows 100 maxrows 200 stt_trigger 1 keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute("create stable range_series (ts timestamp, v int, label varchar(8), spare int, detail int) "
                      "tags (site int) sma(v, label, spare)")
        for child in range(2):
            tdSql.execute(f"create table c{child} using range_series tags ({child})")
            for begin in range(0, 1000, 200):
                rows = []
                for i in range(begin, begin + 200):
                    label = "NULL" if i % 2 else "'present'"
                    rows.append(f"({start + 2 * i + child},{i},{label},NULL,{1000 + i})")
                values = " ".join(rows)
                tdSql.execute(f"insert into c{child} values {values}")
        tdSql.execute("create table bounds (ts timestamp, v int)")
        tdSql.execute(f"insert into bounds values ({start + 902},451) ({start + 906},453) ({start + 908},NULL)")
        tdSql.execute(f"flush database {db}")

        subset = "select v from bounds"
        valid = f"{subset} where v is not null"
        cases = [
            (f"v = (select min(v) from bounds)", lambda v: v == 451),
            (f"v in ({subset})", lambda v: v in (451, 453)),
            (f"v <= all ({valid})", lambda v: v <= 451),
            (f"v > any ({subset})", lambda v: v > 451),
            (f"not (v > all ({subset}))", lambda v: v <= 453),
            (f"v = all ({valid}) or v=999", lambda v: v == 999),
            (f"v != any ({valid})", lambda v: True),
            (f"v not in ({subset}) or v=7", lambda v: v == 7),
            (f"v != all ({valid})", lambda v: v not in (451, 453)),
            (f"v <= all ({subset} where v < 0)", lambda v: True),
        ]
        for condition, predicate in cases:
            tdSql.query(f"select v from c0 where {condition} order by ts desc", queryTimes=1)
            assert [row[0] for row in tdSql.queryResult] == [i for i in reversed(range(1000)) if predicate(i)]

        ts_filter = "ts in (select ts from bounds where v is not null)"
        tdSql.query(f"select count(*) from c0 where {ts_filter}", queryTimes=1)
        tdSql.checkData(0, 0, 2)
        tdSql.query(f"select v from c0 where ts > {start + 902} and {ts_filter} order by ts limit 1", queryTimes=1)
        tdSql.checkData(0, 0, 453)
        tdSql.query(f"select v from range_series where {ts_filter} or v=7 order by ts desc", queryTimes=1)
        assert [row[0] for row in tdSql.queryResult] == [453, 451, 7, 7]

        narrow = "v in (select v from c1 where v between 451 and 550)"
        tdSql.query(f"select v from c0 where {narrow} and label is null order by ts", queryTimes=1)
        assert [row[0] for row in tdSql.queryResult] == list(range(451, 551, 2))
        tdSql.query(f"select v from c0 where {narrow} or label is null order by ts", queryTimes=1)
        assert [row[0] for row in tdSql.queryResult] == [v for v in range(1000) if v % 2 or 451 <= v <= 550]
        tdSql.query(f"select v, spare from c0 where {narrow} order by ts", queryTimes=1)
        assert list(map(tuple, tdSql.queryResult)) == [(v, None) for v in range(451, 551)]
        tdSql.query(f"select count(*) from c0 where {narrow} or spare is null", queryTimes=1)
        tdSql.checkData(0, 0, 1000)
        tdSql.query(f"select detail from c0 where {narrow} or detail = 1007 order by ts", queryTimes=1)
        assert [row[0] for row in tdSql.queryResult] == [1007, *range(1451, 1551)]
        for projection, condition in (("v", f"v in ({subset})"), ("v", f"{narrow} and label is null"),
                                      ("v, spare", narrow), ("v, detail", narrow)):
            tdSql.query(f"explain analyze verbose true select {projection} from c0 where {condition}", queryTimes=1)
            plan = "\n".join(row[0] for row in tdSql.queryResult)
            outer = re.split(r"(?m)^\s*InitPlan \d+", plan, maxsplit=1)[0]
            match = re.search(r"check_rows=(\d+)", outer)
            assert match and 0 < int(match.group(1)) < 1000, (projection, condition, outer)

        tdSql.execute("create table empty_outer (ts timestamp, v int)")
        for column in ("v", "ts"):
            scalar = f"{column} = (select {column} from bounds where v is not null)"
            empty = f"{column} in (select {column} from bounds where v < 0)"
            tdSql.query(f"select {column} from empty_outer where {scalar}", queryTimes=1)
            tdSql.checkRows(0)
            for condition in (scalar, f"{empty} and {scalar}", f"{empty} or {scalar}"):
                tdSql.error(f"select {column} from c0 where {condition}", expectedErrno=0x26A5)

    def test_subquery_range_comparison_types(self):
        """Compare range pruning with row evaluation for exact and floating-point types.

        1. Flush blocks containing neighboring unsigned integers, decimals and floating-point values.
        2. Evaluate each predicate as a projected expression and compare every matching row with WHERE.

        Catalog:
            - Query:Filter

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-17 Joey Sima Cover subquery comparison domains and conservative fallback

        """
        db = "subquery_range_types"
        start = 1730000000000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 minrows 100 maxrows 200 stt_trigger 1 keep 36500")
        tdSql.execute(f"use {db}")
        schema = "(ts timestamp, id int, u bigint unsigned, d decimal(28,2), f float, g double)"
        tdSql.execute(f"create table samples {schema}")
        tdSql.execute(f"create table bound_values {schema}")
        blocks = [
            ("18446744073709551613", "9007199254740992.99", "0.99999995", "0.9999999999999999"),
            ("18446744073709551614", "9007199254740993.00", "1.0", "1.0"),
            ("18446744073709551615", "9007199254740993.01", "1.0000001", "1.0000000000000002"),
            ("NULL", "NULL", "NULL", "NULL"),
        ]
        for group, fields in enumerate(blocks):
            values = ",".join(fields)
            first = 200 * group
            rows = " ".join(f"({start + i},{i},{values})" for i in range(first, first + 200))
            tdSql.execute(f"insert into samples values {rows}")
        tdSql.execute(f"insert into bound_values values ({start},0,{','.join(blocks[1])})")
        tdSql.execute(f"flush database {db}")
        for column in ("u", "d", "f", "g"):
            for op in ("=", "!=", "<", "<=", ">", ">=", "in", "not in"):
                predicate = f"{column} {op} (select {column} from bound_values)"
                tdSql.query(f"select id, {predicate} from samples order by ts", queryTimes=1)
                expected = [row[0] for row in tdSql.queryResult if row[1]]
                tdSql.query(f"select id from samples where {predicate} order by ts", queryTimes=1)
                assert [row[0] for row in tdSql.queryResult] == expected, predicate
