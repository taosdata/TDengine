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

from new_test_framework.utils import tdSql


class TestWindowGroupOrder:
    def setup_class(cls):
        tdSql.prepare(dbname="window_group_order", drop=True)
        tdSql.execute(
            "create stable run (ts timestamp, power double) tags (turbine_id int)"
        )
        for turbine_id, powers in [(1, [2.0, 2.4, 2.1]), (2, [1.6, 1.7, 1.5])]:
            tdSql.execute(f"create table run_{turbine_id} using run tags ({turbine_id})")
            for minute, power in enumerate(powers):
                tdSql.execute(
                    f"insert into run_{turbine_id} values "
                    f"('2026-01-01 08:0{minute}:00', {power})"
                )

    def test_group_order_aggregate(self):
        """Rank grouped rows by aggregate expressions.

        Check the reported totals, hidden aggregates, arithmetic and ties.
        Keep invalid nested functions and ungrouped columns rejected.

        Catalog:
            - Function:Window

        Since: v3.4.3.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7166763974

        History:
            - 2026-11-13 Kane Kuang Created
        """
        tdSql.query(
            "select turbine_id, sum(power) as total, "
            "rank() over (order by sum(power) desc) as rn "
            "from run group by turbine_id order by turbine_id"
        )
        tdSql.checkRows(2)
        for row, values in enumerate([(1, 6.5, 1), (2, 4.8, 2)]):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

        for expr in [
            "sum(power)", "avg(power)", "min(power)", "max(power)",
            "first(power)", "last(power)", "abs(sum(power)) + 1",
        ]:
            tdSql.query(
                f"select turbine_id, rank() over (order by {expr} desc) "
                "from run group by turbine_id order by turbine_id"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 2)

        tdSql.query(
            "select turbine_id, rank() over (order by count(*) desc) "
            "from run group by turbine_id order by turbine_id"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        tdSql.query("select rank() over (order by sum(power)) from run")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        for argument in ["turbine_id", "*", "1"]:
            tdSql.query(
                f"select turbine_id, count({argument}) over (order by sum(power) desc) "
                "from run group by turbine_id order by turbine_id"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 2)
        for argument in ["*", "1"]:
            tdSql.query(f"select count({argument}) over (order by sum(power)) from run")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select turbine_id from run group by turbine_id "
            "order by rank() over (order by sum(power) desc) desc"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)

        tdSql.query(
            "select turbine_id, rank() over (order by sum(power) desc) "
            "from run group by turbine_id having sum(power) > 5"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            "select turbine_id, rank() over (order by sum(power) desc) "
            "from run where power < 0 group by turbine_id"
        )
        tdSql.checkRows(0)

        tdSql.error(
            "select rank() over (order by sum(avg(power))) "
            "from run group by turbine_id"
        )
        tdSql.error(
            "select rank() over (order by rank() over (order by turbine_id)) "
            "from run group by turbine_id"
        )
        tdSql.error(
            "select rank() over (order by sum(power) + power) "
            "from run group by turbine_id"
        )
        tdSql.error(
            "select rank() over (order by top(power, 2)) "
            "from run group by turbine_id"
        )
        tdSql.execute("create table run_3 using run tags (3)")
        tdSql.execute("insert into run_3 values ('2026-01-01 08:00:00', 6.5)")
        tdSql.query(
            "select turbine_id, rank() over (order by sum(power) desc), "
            "dense_rank() over (order by sum(power) desc) "
            "from run group by turbine_id order by turbine_id"
        )
        tdSql.checkRows(3)
        for row, values in enumerate([(1, 1, 1), (2, 3, 2), (3, 1, 1)]):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)
