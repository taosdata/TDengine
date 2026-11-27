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

# -*- coding: utf-8 -*-
import os
import sys
import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import VTABLE_VIRTUAL_REF_CROSS_RAW_DB, VtableQueryUtil


class TestVtableQueryCrossDbStbMergeRepeat:
    """Regression for the cross-db virtual_ref merge task hang (defect 7173555991)."""

    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    TIMERANGE_BOOL_SQL = (
        "select * from test_vtable_select.vtb_virtual_stb "
        "where ts >= '2020-10-01 00:00:00.400' "
        "and ts < '2020-10-01 00:00:00.600' "
        "and bool_col = true "
        "order by ts, int_tag limit 100;"
    )
    EXPECTED_TS_COUNT = 30
    FIRST_EXPECTED_TS = "2020-10-01 00:00:00.402"
    QUERY_TIMEOUT_SECS = 180

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(ref_mode="virtual_ref")

    @staticmethod
    def _fetch_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    @pytest.mark.timeout(900)
    def test_cross_db_ref_timerange_bool_filter_repeat(self):
        """Repeatedly run the cross-db virtual_ref timerange + bool filter merge query.

        The cross-db virtual_ref merge parent task once stalled forever
        (EXECUTING until the CI 1200s timeout) when the dynamic vtable scan
        re-fetched the already PART_SUCC child scan tasks: the merge task never
        got its fetch response back. Every round here must complete and return
        the shared baseline rows, both through the in-process client and
        through the taos CLI subprocess that reproduced the original hang.

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-20 Created -- regression for cross-db virtual_ref merge task hang (defect 7173555991).
        """
        self.do_repeated_inprocess_queries()
        self.do_repeated_cli_queries()

    def do_repeated_inprocess_queries(self):
        sql = self.TIMERANGE_BOOL_SQL
        baseline = self._fetch_rows(sql)
        assert len(baseline) == self.EXPECTED_TS_COUNT, f"baseline rows: {baseline}"
        for i in range(20):
            rows = self._fetch_rows(sql)
            assert rows == baseline, f"round {i}: result mismatch, rows: {rows}"
        print("repeated in-process cross-db virtual_ref merge query ... [ passed ]")

    def do_repeated_cli_queries(self):
        # The taos CLI subprocess (fresh client, default queryPolicy) is the
        # exact execution path on which the merge task hang was observed.
        # Note: the batch-mode output echoes the SQL line, which itself
        # contains the two '2020-10-01' literals, so count only data rows
        # (lines whose first field is a timestamp) instead of substring hits.
        for i in range(10):
            # Each CLI round is bounded so a regression of the hang fails here
            # with a clear TimeoutExpired instead of the CI 1200s timeout.
            result_file = tdCom.generate_query_result_file(
                "vtable_cross_db_ref_merge_repeat", i, self.TIMERANGE_BOOL_SQL,
                timeout=self.QUERY_TIMEOUT_SECS)
            try:
                with open(result_file, "r", encoding="utf-8") as f:
                    content = f.read()
            finally:
                try:
                    os.remove(result_file)
                except OSError:
                    pass

            data_rows = [ln for ln in content.splitlines()
                         if ln.lstrip().startswith("2020-10-01")]
            assert len(data_rows) == self.EXPECTED_TS_COUNT, \
                f"round {i}: expected {self.EXPECTED_TS_COUNT} rows, got {len(data_rows)}:\n{content}"
            assert data_rows[0].lstrip().startswith(self.FIRST_EXPECTED_TS), \
                f"round {i}: first row not sorted/expected:\n{data_rows[0]}"
        print("repeated CLI cross-db virtual_ref merge query ...... [ passed ]")

    @pytest.mark.timeout(600)
    def test_cross_db_ref_use_db_error_fail_fast(self):
        """Cross-db virtual_ref query must fail fast when the useDb rsp carries an error.

        1. Break the virtual_ref chain by dropping the raw source database so
           the mnode useDb (TDMT_MND_GET_DB_INFO) rsp for it carries an error
        2. The same merge query must then fail quickly (in-process and via the
           taos CLI subprocess) instead of hanging the merge task forever or
           crashing while cleaning up the failed useDb rsp

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-20 Created -- negative useDb-error fail-fast regression for cross-db virtual_ref merge task hang (defect 7173555991).
        """
        self.do_use_db_error_fail_fast()

    def do_use_db_error_fail_fast(self):
        # Break the virtual_ref chain at the raw source db: the mnode useDb
        # (TDMT_MND_GET_DB_INFO) rsp for the dropped db now carries an error
        # code, and the query must fail quickly instead of hanging the merge
        # task forever or crashing while cleaning up the failed rsp. A
        # regression either hangs (bounded by the pytest timeout / CLI timeout
        # here) or crashes (caught by ASAN in CI).
        tdSql.execute(f"drop database {VTABLE_VIRTUAL_REF_CROSS_RAW_DB};")

        # 1. In-process: the query must fail quickly, not hang and not return
        #    the baseline rows silently.
        start = time.time()
        err_info = tdSql.error(self.TIMERANGE_BOOL_SQL)
        elapsed = time.time() - start
        tdLog.info(f"in-process error query failed in {elapsed:.2f}s: {err_info}")
        assert elapsed < self.QUERY_TIMEOUT_SECS, \
            f"query took {elapsed:.1f}s to fail, expected fast failure"

        # 2. CLI subprocess: bounded by the helper timeout; the output must
        #    contain no data rows, i.e. the query did not succeed.
        result_file = tdCom.generate_query_result_file(
            "vtable_cross_db_ref_use_db_error", 0, self.TIMERANGE_BOOL_SQL,
            timeout=self.QUERY_TIMEOUT_SECS)
        try:
            with open(result_file, "r", encoding="utf-8") as f:
                content = f.read()
        finally:
            try:
                os.remove(result_file)
            except OSError:
                pass

        data_rows = [ln for ln in content.splitlines()
                     if ln.lstrip().startswith("2020-10-01")]
        assert len(data_rows) == 0, \
            f"expected the query to fail with no data rows, got {len(data_rows)}:\n{content}"
        print("cross-db virtual_ref useDb error fail-fast ...... [ passed ]")
