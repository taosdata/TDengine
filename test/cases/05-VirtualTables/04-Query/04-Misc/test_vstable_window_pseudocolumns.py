# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdSql


class TestVstableWindowPseudocolumns:
    def setup_class(cls):
        tdSql.prepare(dbname="test_vstable_window_pseudo", drop=True)
        tdSql.execute("create stable source_stb(ts timestamp, status int, val int) tags(gid int)")
        tdSql.execute("create table source_ctb using source_stb tags(1)")
        tdSql.execute(
            "insert into source_ctb values "
            "(1700000000000, 1, 10) (1700000060000, 1, 20) "
            "(1700000120000, 2, 30) (1700000180000, 2, 40) "
            "(1700000300000, 1, 50) (1700000360000, 1, 60)"
        )
        tdSql.execute(
            "create stable vst(ts timestamp, status int, val int) tags(gid int) virtual 1"
        )
        tdSql.execute(
            "create vtable vct(status from source_ctb.status, val from source_ctb.val) "
            "using vst tags(1)"
        )

    @staticmethod
    def _check_window(columns, clause, expected_rows, optimized=True):
        tdSql.query(f"select {columns} from source_ctb {clause}")
        tdSql.checkRows(expected_rows)
        expected = list(tdSql.queryResult)

        query = f"select {columns} from vst {clause}"
        tdSql.query(f"explain verbose true {query}")
        uses_dynamic_window = any(
            "Dynamic Query Control for Virtual Table Window" in str(value)
            for row in tdSql.queryResult for value in row
        )
        assert uses_dynamic_window == optimized, query

        tdSql.query(query)
        tdSql.checkRows(expected_rows)
        for row_idx, row in enumerate(expected):
            for col_idx, value in enumerate(row):
                tdSql.checkData(row_idx, col_idx, value)

    def test_state_window_pseudocolumns(self):
        """Virtual supertable state windows return pseudo columns without crashing.

        Compare the customer EXTEND and TRUE_FOR query and pseudo-column
        subsets with the same query over the referenced physical table.

        Catalog: query/window

        Since: v3.4.3.9

        Labels: virtual,state_window,regression

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7160177088

        History:
            - 2026-11-06 Codex Created
        """
        self._check_window("_wstart, _wend", "state_window(status) extend(1) true_for(60s)", 3)
        for columns in ("_wstart, _wend", "_wend", "_wstart", "_wduration, _wend, _wstart"):
            self._check_window(columns, "state_window(status)", 3)
            self._check_window(columns, "where status = 2 state_window(status)", 1, optimized=False)
        self._check_window("_wstart, _wend", "state_window(status) true_for(120s)", 0)
        self._check_window("_wstart, _wend", "where status = 99 state_window(status)", 0, optimized=False)

    def test_session_event_window_pseudocolumns(self):
        """Virtual supertable session and event windows preserve pseudo-column results.

        Exercise the shared dynamic window calculation path with no aggregate
        functions and with an aggregate function alongside window boundaries.

        Catalog: query/window

        Since: v3.4.3.9

        Labels: virtual,session_window,event_window,regression

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7160177088

        History:
            - 2026-11-06 Codex Created
        """
        for clause, rows in (
            ("session(ts, 70s)", 2),
            ("event_window start with status = 1 end with status = 2", 1),
            ("state_window(status)", 3),
        ):
            self._check_window("_wstart, _wend, _wduration", clause, rows)
            self._check_window("_wstart, _wend, count(val)", clause, rows)
