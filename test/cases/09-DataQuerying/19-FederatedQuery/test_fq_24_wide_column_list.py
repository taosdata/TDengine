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

"""
test_fq_24_wide_column_list.py

Regression for federated-query planning of wide external tables.

The FQ pushdown diagnostic in fqPushdownOptimize builds the scan column list
in a fixed 512-byte stack buffer.  The 512 is the local log buffer's size, not
a user-visible limit: the documented limits are 64-byte identifiers, 4096
referenced columns and 128 tags, and a query below them must succeed even when
its column names do not fit the log buffer.  A table whose expanded list is
longer than 512 bytes used to overflow the buffer and abort or segfault the
client planner.  The fixture keeps only the columns needed to cross that
boundary: a key column and twenty long-named attribute columns, all renamed
relative to the source report.

Catalog: - Query:FederatedPlanner

Since: v3.4.4.0

Labels: common,ci

Jira: None

History:
    - 2026-11-06 wpan Add wide external-table column-list regression
"""
from new_test_framework.utils import tdLog, tdSql

from federated_query_common import ExtSrcEnv, FederatedQueryTestMixin


class TestFq24WideColumnList(FederatedQueryTestMixin):
    TABLE = "FQ24_WIDE_COL_FIXTURE"
    SOURCE = "fq24_wide_cols_src"
    ROW_KEY = "FQ-WIDE-0001"
    ATTR_COUNT = 20

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()

    def setup_method(self):
        self.cfg = self._oracle_cfg()
        self._cleanup_src(self.SOURCE)
        ExtSrcEnv.oracle_drop_table_cfg(self.cfg, self.TABLE)
        ExtSrcEnv.oracle_exec_cfg(self.cfg, self._fixture_sqls())
        self._mk_oracle_real(self.SOURCE, database=self.cfg.service,
                             schema=ExtSrcEnv.ORACLE_USER.upper())

    def teardown_method(self):
        self._cleanup_src(self.SOURCE)
        ExtSrcEnv.oracle_drop_table_cfg(self.cfg, self.TABLE)

    def _attr_name(self, idx):
        return f"PROCESS_ATTRIBUTE_VALUE_{idx:04d}"

    def _attr_value(self, idx):
        return f"attr-value-{idx:04d}"

    def _fixture_sqls(self):
        attr_names = [self._attr_name(i) for i in range(1, self.ATTR_COUNT + 1)]
        columns = ",\n  ".join(f"{name} VARCHAR2(64)" for name in attr_names)
        names = ", ".join(attr_names)
        values = ", ".join(f"'{self._attr_value(i)}'"
                           for i in range(1, self.ATTR_COUNT + 1))
        ddl = (f"CREATE TABLE {self.TABLE} (\n"
               f"  ROW_KEY VARCHAR2(64) NOT NULL,\n"
               f"  {columns}\n)")
        insert = (f"INSERT INTO {self.TABLE} (ROW_KEY, {names}) "
                  f"VALUES ('{self.ROW_KEY}', {values})")
        return [ddl, insert]

    def do_wide_column_select(self):
        src_table = f"{self.SOURCE}.{self.TABLE.lower()}"

        # The two projections the source report used to confirm the table must
        # keep returning the expected row.
        tdSql.query(f"select row_key from {src_table} where row_key='{self.ROW_KEY}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.ROW_KEY)

        tdSql.query(f"select row_key from {src_table} where row_key='{self.ROW_KEY}' limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.ROW_KEY)

        # describe reports the external-table column order used by select *;
        # locate every value through it so the assertion does not depend on
        # the engine-side column ordering.
        tdSql.query(f"describe {src_table}")
        tdSql.checkRows(self.ATTR_COUNT + 1)
        fields = [str(row[0]).lower() for row in tdSql.queryResult]
        for i in range(1, self.ATTR_COUNT + 1):
            assert self._attr_name(i).lower() in fields, f"missing column {self._attr_name(i)}"
        assert "row_key" in fields

        # select * expands the full column list.  Before the fix the planner
        # overflowed its 512-byte stack buffer here and the process died.
        tdSql.query(f"select * from {src_table} where row_key='{self.ROW_KEY}' limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, fields.index("row_key"), self.ROW_KEY)
        for i in range(1, self.ATTR_COUNT + 1):
            tdSql.checkData(0, fields.index(self._attr_name(i).lower()),
                            self._attr_value(i))

        # Crossing the diagnostic buffer is not a query error: the plan and the
        # session must stay healthy after the full column list is expanded.
        tdSql.query("select 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        print("wide external-table column list ................. [ passed ]")

    def test_fq_wide_column_list(self):
        """Select every column of a wide external table without corrupting the planner

        1. Single-column projections on the wide Oracle table return the row
        2. describe exposes the full external-table schema
        3. select * expands the whole column list and returns all values

        Catalog: - Query:FederatedPlanner

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None
        """
        self.do_wide_column_select()
