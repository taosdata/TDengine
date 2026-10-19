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

from pathlib import Path

from new_test_framework.utils import tdSql


class TestGroupCacheStableJoinRegression:
    """Regression coverage for the dynamic stable-join/group-cache crash."""

    def setup_class(self):
        sql_file = Path(__file__).with_name("reproduce_groupcache_join_complex.sql")
        for statement in sql_file.read_text(encoding="utf-8").split(";"):
            statement = statement.strip()
            if statement and not statement.lower().startswith("select"):
                tdSql.execute(statement)

    def test_groupcache_stable_join_with_varcols(self):
        """Verify stable joins with variable-length columns and group cache.

        1. Create variable-length stable-table data with NULL values
        2. Execute a dynamic stable-table join with grouped aggregation
        3. Verify the join result and ensure the executor does not crash

        Catalog:
            - Query:Join

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-14 Codex added regression coverage for stable join capacity
        """
        tdSql.query(
            "select t_q.point30, sum(t_p.point1 * t_q.point1) / sum(t_q.point1) "
            "from (select ts, time_point, company_name, point30, point1 "
            "from spot_trading_anhui where base_path = "
            "'ods/安徽现货数据/私有/江苏协鑫售电有限公司/日曲线统计/' "
            "and file_path like '%2026-08-01%' and point31 = '电量') t_q "
            "join (select ts, time_point, company_name, point30, point1 "
            "from spot_trading_anhui where base_path = "
            "'ods/安徽现货数据/私有/江苏协鑫售电有限公司/日曲线统计/' "
            "and file_path like '%2026-08-01%' and point31 = '电价') t_p "
            "on t_q.ts = t_p.ts and t_q.company_name = t_p.company_name "
            "and t_q.point30 = t_p.point30 group by t_q.point30 limit 5"
        )
        tdSql.checkRows(8)
