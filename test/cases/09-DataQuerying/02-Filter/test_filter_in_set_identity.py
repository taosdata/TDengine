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


class TestFilterInSetIdentity:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.base_ts = 1793462400000

    def prepare_data(self, column_type, table_kind):
        tdSql.prepare("filter_in_set_identity", drop=True, keep=36500)
        columns = f"ts timestamp, code {column_type}, value int"
        rows = [
            (0, "month_a", 1),
            (2, "month_a", 2),
            (4, "month_b", 3),
            (3, "min_a", 4),
            (5, "min_b", 5),
            (7, "min_a", 6),
            (9, "min_b", 7),
            (6, None, 8),
            (8, "other", 9),
        ]
        if table_kind == "super":
            tdSql.execute(f"create stable st ({columns}) tags (asset_id varchar(16))")
            tdSql.execute("create table t using st tags ('asset')")
            tdSql.execute("create table ignored using st tags ('other')")
            tdSql.execute(
                f"insert into ignored values ({self.base_ts + 1}, 'month_a', 10)"
            )
            self.table = "st"
            self.asset_filter = "asset_id = 'asset' and "
        else:
            tdSql.execute(f"create table t ({columns})")
            self.table = "t"
            self.asset_filter = ""
        values = " ".join(
            f"({self.base_ts + offset}, {'null' if code is None else repr(code)}, {value})"
            for offset, code, value in rows
        )
        tdSql.execute(f"insert into t values {values}")

    def check_values(self, condition, expected):
        tdSql.query(
            f"select value from {self.table} where {self.asset_filter}({condition}) order by value"
        )
        tdSql.checkRows(len(expected))
        for row, value in enumerate(expected):
            tdSql.checkData(row, 0, value)

    def do_time_ranges(self):
        ranges = [
            (0, 5, 3, 10, [1, 2, 3, 4, 5, 6, 7]),
            (0, 3, 3, 10, [1, 2, 4, 5, 6, 7]),
            (0, 10, 3, 7, [1, 2, 3, 4, 5]),
            (0, 3, 7, 10, [1, 2, 6, 7]),
        ]
        for start1, end1, start2, end2, expected in ranges:
            first = (
                "code in ('month_a', 'month_b') "
                f"and ts >= {self.base_ts + start1} and ts < {self.base_ts + end1}"
            )
            second = (
                "code in ('min_a', 'min_b') "
                f"and ts >= {self.base_ts + start2} and ts < {self.base_ts + end2}"
            )
            self.check_values(f"({first}) or ({second})", expected)
            self.check_values(f"({second}) or ({first})", expected)
            tdSql.query(
                f"select count(*), sum(value) from {self.table} "
                f"where {self.asset_filter}(({first}) or ({second}))"
            )
            tdSql.checkData(0, 0, len(expected))
            tdSql.checkData(0, 1, sum(expected))
        print("IN sets with overlapping and disjoint time ranges ........ [ passed ]")

    def do_shared_sets(self):
        self.check_values(
            "(code in ('month_a', 'month_b') or code in ('min_a', 'min_b')) "
            "and ((value >= 1 and value <= 3) or (value >= 4 and value <= 7))",
            [1, 2, 3, 4, 5, 6, 7],
        )
        self.check_values(
            "(code in ('month_a', 'month_b', 'month_a') and value >= 1 and value <= 9) "
            "or (code in ('min_a', 'min_b') and value >= 1 and value <= 9)",
            [1, 2, 3, 4, 5, 6, 7],
        )
        self.check_values(
            "(code in ('month_a', 'month_b') and value >= 1 and value < 3) "
            "or (code in ('month_b', 'month_a') and value >= 3 and value <= 3)",
            [1, 2, 3],
        )
        self.check_values(
            "(code in ('month_a', 'month_b') and value >= 1 and value <= 9) "
            "or (code in ('min_a', 'min_b') and value >= 1 and value <= 9)",
            [1, 2, 3, 4, 5, 6, 7],
        )
        print("Shared IN objects, duplicate literals and scalar ranges ... [ passed ]")

    @pytest.mark.parametrize("column_type", ["varchar(64)", "nchar(64)", "varbinary(64)"])
    @pytest.mark.parametrize("table_kind", ["normal", "super"])
    def test_filter_in_set_identity(self, column_type, table_kind):
        """Preserve distinct string IN sets during filter rewriting

        1. Check overlapping, adjacent, nested and disjoint time ranges in both OR orders
        2. Verify complete row identities and aggregate values for each range
        3. Cover shared IN objects, repeated literals, NULLs and ordinary numeric ranges
        4. Exercise VARCHAR, NCHAR and VARBINARY on regular tables and supertables

        Catalog:
            - Query:Filter

        Since: v3.3.6.0

        Labels: common,ci,integration,functional

        Jira: TD-7156277085

        History:
            - 2026-11-04 Jing Sima Add regression coverage for IN set identity

        """
        self.prepare_data(column_type, table_kind)
        self.do_time_ranges()
        self.do_shared_sets()
