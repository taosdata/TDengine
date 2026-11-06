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
