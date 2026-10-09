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


class TestSplitScan:

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def _rows(self, sql):
        tdSql.query(sql, queryTimes=1)
        return [
            [tdSql.getData(row, col) for col in range(len(tdSql.cursor.description))]
            for row in range(tdSql.getRows())
        ]

    def _assert_same_rows(self, expected, actual):
        expected = sorted(expected, key=repr)
        actual = sorted(actual, key=repr)
        tdSql.checkAssert(expected == actual, f"split_scan result mismatch: expected={expected}, actual={actual}")

    # --- impl ---
    def do_prepare_data(self):
        tdSql.prepare("db_split_scan", drop=True, vgroups=2)
        tdSql.execute("use db_split_scan")
        tdSql.execute("create stable st(ts timestamp, v double) tags(t int)")

        base = 1704067200000
        for table in range(4):
            tdSql.execute(f"create table ct{table} using st tags({table})")
            values = " ".join(
                f"({base + window * 10000}, {table * 10 + window + 1}.0)"
                for window in range(3)
            )
            tdSql.execute(f"insert into ct{table} values {values}")

        print("prepare split scan data .............. [ passed ]")

    def do_verify_split_scan(self):
        sql = (
            "select tbname, _wstart as time, avg(v) as v from st "
            "where ts >= 1704067200000 and ts < 1704067230000 "
            "partition by tbname interval(10s)"
        )
        split_sql = sql.replace("select", "select /*+ split_scan() */", 1)

        expected = self._rows(sql)
        tdSql.checkRows(12)

        tdSql.query(f"explain verbose true {split_sql}", queryTimes=1)
        plan = "\n".join(str(row) for row in tdSql.queryResult)
        tdSql.checkAssert("Data Exchange" in plan, f"split_scan exchange missing: {plan}")
        tdSql.checkAssert("Table Scan" in plan, f"split_scan table scan missing: {plan}")

        for _ in range(3):
            self._assert_same_rows(expected, self._rows(split_sql))

        print("split scan interval results .......... [ passed ]")

    # --- main ---
    def test_split_scan(self):
        """Split scan interval result and plan isolation

        1. Verify SPLIT_SCAN() preserves partition-by-tbname interval results
        2. Verify the split execution plan contains a data exchange and table scan
        3. Run this in a dedicated CI case so the successful class teardown destroys taosd

        Catalog: Timeseries:TimeWindow

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-30 wpan Add SPLIT_SCAN regression coverage

        """
        self.do_prepare_data()
        self.do_verify_split_scan()
