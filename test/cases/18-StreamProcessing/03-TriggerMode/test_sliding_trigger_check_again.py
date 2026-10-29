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

import os

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSlidingTriggerCheckAgain:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sparse_sliding_groups(self):
        """Sparse sliding input must not enumerate a year of empty windows.

        1. Insert two distant batches into one group and ordinary data into another.
        2. Check both groups finish promptly with only nonempty windows.

        Catalog:
            - StreamProcessing:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7139408328

        History:
            - 2026-10-19 Added regression for CI job 709879.
        """
        tdStream.dropAllStreamsAndDbs()
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()
        tdSql.prepare(dbname="sparse_sliding", vgroups=2)
        tdSql.execute("create stable meters (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table gap using meters tags (1)")
        tdSql.execute("create table peer using meters tags (2)")
        tdSql.execute(
            "create stream sparse sliding(10s) from meters partition by tbname "
            "stream_options(ignore_nodata_trigger) into result "
            "as select _tcurrent_ts ts, count(*) c, sum(v) s from %%trows"
        )
        tdStream.checkStreamStatus()
        tdSql.execute(
            "insert into gap values "
            "('2025-01-01 00:00:00.001', 1) "
            "('2026-01-01 00:00:00.001', 2) "
            "('2026-01-01 00:00:10.000', 3) "
            "peer values ('2025-01-01 00:00:00.001', 7) ('2025-01-01 00:00:10.000', 11)"
        )
        tdSql.checkResultsByArray(
            sql="select ts, c, s from sparse_sliding.result order by ts, s",
            exp_result=[
                ["2025-01-01 00:00:10.000", 1, 1],
                ["2025-01-01 00:00:10.000", 2, 18],
                ["2026-01-01 00:00:10.000", 2, 5],
            ],
            retry=30,
        )

    def test_sliding_trigger_check_again(self):
        """Sliding trigger should continue checking after one calc batch

        1. Create a sliding stream with 1ms(step unit `a`) and calc based on %%trows.
        2. Insert trigger rows spanning more than STREAM_CALC_REQ_MAX_WIN_NUM windows.
        3. Verify all windows are eventually materialized instead of stopping after the first batch.

        Since: v3.4.1.2

        Labels: common,ci,integration,functional
        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6945766706

        History:
            - 2026-4-14 Jinqing Kuang Created
        """

        tdStream.dropAllStreamsAndDbs()
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

        tdSql.prepare(dbname="sdb", vgroups=1)
        tdSql.execute("create table trigger_tb (ts timestamp, v int)")

        tdSql.execute(
            "create stream s_check_again interval(1a) sliding(1a) from trigger_tb into out_check_again "
            "as select _twstart ts, _twrownum tc, count(*) c from trigger_tb where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            "insert into trigger_tb values ('2025-01-01 00:00:00.000', 0), ('2025-01-01 00:01:00.000', 1)"
        )

        tdSql.checkResultsByFunc(
            sql="select first(ts), last(ts), count(*), min(tc), max(tc), sum(tc), min(c), max(c), sum(c) from sdb.out_check_again",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:01:00.000")
            and tdSql.compareData(0, 2, 60001)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(0, 4, 1)
            and tdSql.compareData(0, 5, 2)
            and tdSql.compareData(0, 6, 0)
            and tdSql.compareData(0, 7, 1)
            and tdSql.compareData(0, 8, 2)
        )

        tdSql.execute("insert into trigger_tb values ('2025-01-01 00:02:00.000', 1)")

        tdSql.checkResultsByFunc(
            sql="select first(ts), last(ts), count(*), min(tc), max(tc), sum(tc), min(c), max(c), sum(c) from sdb.out_check_again",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:02:00.000")
            and tdSql.compareData(0, 2, 120001)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(0, 4, 1)
            and tdSql.compareData(0, 5, 3)
            and tdSql.compareData(0, 6, 0)
            and tdSql.compareData(0, 7, 1)
            and tdSql.compareData(0, 8, 3)
        )
