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

from new_test_framework.utils import tdLog, tdSql
from pathlib import Path
from new_test_framework.utils import tdCom


class TestUnionPerfRepro:
    db_name = "union_perf_repro_336"
    stable_name = "host_linux_1m_net_metrics"
    table_prefix = "host_linux_1m_net_metrics_"
    table_count = 3
    rows_per_table = 8
    base_ts = 1788105600000
    end_ts = 1788191940000
    ip_address = "10.12.180.40"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls._prepare_data()

    @classmethod
    def _prepare_data(cls):
        tdSql.execute(f"create database if not exists {cls.db_name}")
        tdSql.execute(f"use {cls.db_name}")
        tdSql.execute(
            f"""
            create stable if not exists {cls.stable_name} (
                ts timestamp,
                traffic_in float,
                traffic_out float,
                packet_errors_in float,
                packet_errors_out float,
                packet_drops_in float,
                packet_drops_out float,
                link_status float
            ) tags (
                host_name binary(64),
                host_group binary(64),
                ip_address binary(64),
                if_name binary(64)
            )
            """
        )

        for idx in range(cls.table_count):
            child_table = f"{cls.table_prefix}{idx}"
            tdSql.execute(
                f"create table if not exists {child_table} using {cls.stable_name} "
                f"tags ('host_{idx:02d}', 'group_{idx % 2}', '{cls.ip_address}', 'eth{idx}')"
            )

            tdSql.query(f"select count(*) from {child_table}", queryTimes=1)
            row_count = int(tdSql.getData(0, 0))
            if row_count > 0:
                tdLog.info(f"[union perf] reuse {child_table}, rows={row_count}")
                continue

            values = []
            for row in range(cls.rows_per_table):
                ts = cls.base_ts + row * 60000
                values.append(
                    f"({ts}, {1000 + row}, {900 + row}, {row % 3}, {row % 2}, {row % 4}, {row % 5}, 1)"
                )
            tdSql.execute(f"insert into {child_table} values " + ",".join(values))
            tdLog.info(f"[union perf] init {child_table}, rows={cls.rows_per_table}")

    @classmethod
    def _branch_sqls(cls):
        base_select = """
SELECT _wstart AS bucket,
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`,
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`,
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`,
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`,
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`,
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`,
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`,
       '{type_name}' as type,
       host_name,
       host_group,
       ip_address,
       if_name
FROM {stable_name}
WHERE ip_address = '{ip_address}'
  and ts >= {base_ts}
  AND ts < {end_ts}
{time_filters}
partition by host_name,host_group,ip_address,if_name
INTERVAL({interval}) FILL(NONE)
"""

        branch_specs = [
            ("3天", ["AND timediff({end_ts},{base_ts},1a) <= 259200000"], "1m"),
            (
                "3-7天",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 259200000",
                    "AND timediff({end_ts},{base_ts},1a) <= 604800000",
                ],
                "2m",
            ),
            (
                "7-15天",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 604800000",
                    "AND timediff({end_ts},{base_ts},1a) <= 1296000000",
                ],
                "5m",
            ),
            (
                "15-30天",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 1296000000",
                    "AND timediff({end_ts},{base_ts},1a) <= 2592000000",
                ],
                "10m",
            ),
            (
                "30-90天",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 2592000000",
                    "AND timediff({end_ts},{base_ts},1a) <= 7776000000",
                ],
                "30m",
            ),
            (
                "90天-180天",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 7776000000",
                    "AND timediff({end_ts},{base_ts},1a) <= 15552000000",
                ],
                "1h",
            ),
            (
                "180天-1年",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 15552000000",
                    "AND timediff({end_ts},{base_ts},1a) <= 31536000000",
                ],
                "2h",
            ),
            (
                "1年-3年",
                [
                    "AND timediff({end_ts},{base_ts},1a) > 31536000000",
                    "AND timediff({end_ts},{base_ts},1a) <= 94608000000",
                ],
                "5h",
            ),
            ("3年以上", ["AND timediff({end_ts},{base_ts},1a) > 94608000000"], "1d"),
        ]

        sqls = []
        for type_name, time_filters, interval in branch_specs:
            sqls.append(
                base_select.format(
                    type_name=type_name,
                    stable_name=cls.stable_name,
                    ip_address=cls.ip_address,
                    base_ts=cls.base_ts,
                    end_ts=cls.end_ts,
                    time_filters="\n  ".join(
                        filter_line.format(base_ts=cls.base_ts, end_ts=cls.end_ts)
                        for filter_line in time_filters
                    ),
                    interval=interval,
                ).strip()
            )
        return sqls

    @classmethod
    def _union_sql(cls):
        return "\nUNION ALL\n".join(cls._branch_sqls())

    @classmethod
    def _nested_union_sql(cls):
        return "SELECT * FROM (" + cls._union_sql() + ") t_a_0"

    @classmethod
    def _explain_plan(cls, sql):
        tdSql.query("explain verbose true " + sql, queryTimes=1)
        return "\n".join(" ".join(str(col) for col in row) for row in tdSql.queryResult)

    def _check_non_from_subquery_pruning(self):
        subquery = (
            "select ts from host_linux_1m_net_metrics where 1=0 "
            "union all select ts from host_linux_1m_net_metrics"
        )
        cases = [
            (
                "select-root",
                f"select ts from host_linux_1m_net_metrics where ts in ({subquery})",
                2,
            ),
            (
                "set-operator-root",
                f"select ts from host_linux_1m_net_metrics where ts in ({subquery}) "
                "union all select ts from host_linux_1m_net_metrics",
                3,
            ),
        ]
        for case_name, sql, expected_scans in cases:
            plan = self._explain_plan(sql)
            if "Filter: false" in plan or "Filter: conditions=false" in plan:
                raise Exception(f"{case_name} retained an empty non-FROM subquery scan branch")
            if plan.count("Table Scan on") != expected_scans:
                raise Exception(f"{case_name} did not prune its empty non-FROM subquery branch")

    def do_traversal_cases(self, case_dir):
        tdCom.compare_testcase_result(
            str(case_dir / "t" / "union_traversal.sql"),
            str(case_dir / "r" / "union_traversal.result"),
            "union_traversal",
        )
        print("union traversal cases ......................... [ passed ]")

    def do_nested_union_result_materialization(self, nested_union_sql, expected_rows):
        rows = tdSql.query(nested_union_sql, queryTimes=1)
        tdSql.checkRows(expected_rows)
        tdSql.checkData(0, 8, "3天")
        tdLog.info(f"[union perf] nested union materialized rows={rows}")
        print("nested union result materialization ........... [ passed ]")

    def test_union_perf_repro(self):
        """union all empty branch pruning

        1. Build a field-shaped UNION ALL workload with empty time-range branches
        2. Compare branch results and explain plans to verify empty-branch pruning
        3. Cover nested SELECT, derived tables, JOIN inputs and empty FROM traversal

        Catalog:
            - Query:Union

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-03 Codex add field-shaped UNION ALL empty-branch regression
            - 2026-09-20 Codex add nested UNION result materialization regression
        """

        case_dir = Path(__file__).resolve().parent
        tdSql.execute(f"use {self.db_name}")

        branch_sqls = self._branch_sqls()
        branch_row_total = 0
        for idx, branch_sql in enumerate(branch_sqls, start=1):
            rows = tdSql.query(branch_sql, queryTimes=1)
            branch_row_total += rows
            tdLog.info(f"[union perf] branch {idx}/9 rows={rows}")

        union_sql = self._union_sql()
        union_rows = tdSql.query(union_sql, queryTimes=1)
        tdLog.info(f"[union perf] full union rows={union_rows}")
        tdSql.checkRows(branch_row_total)

        active_plan = self._explain_plan(branch_sqls[0])
        union_plan = self._explain_plan(union_sql)
        active_scan_count = active_plan.count("Table Scan on")
        union_scan_count = union_plan.count("Table Scan on")
        tdLog.info(f"[union perf] active branch explain table scans={active_scan_count}")
        tdLog.info(f"[union perf] full union explain table scans={union_scan_count}")

        if "Filter: false" in union_plan or "Filter: conditions=false" in union_plan:
            raise Exception("empty UNION ALL branch was planned as Table Scan with Filter: false")
        if union_scan_count != active_scan_count:
            raise Exception(
                f"empty UNION ALL branches were not pruned, active scans={active_scan_count}, union scans={union_scan_count}"
            )

        nested_union_sql = self._nested_union_sql()
        nested_plan = self._explain_plan(nested_union_sql)
        nested_scan_count = nested_plan.count("Table Scan on")
        tdLog.info(f"[union perf] nested union explain table scans={nested_scan_count}")
        if nested_scan_count != active_scan_count:
            raise Exception(
                f"nested union empty branches were not pruned, active scans={active_scan_count}, "
                f"nested scans={nested_scan_count}"
            )
        self._check_non_from_subquery_pruning()
        self.do_nested_union_result_materialization(nested_union_sql, branch_row_total)
        self.do_traversal_cases(case_dir)
        self.sqlFile = str(case_dir / "t" / "union_perf_nested.sql")
        self.ansFile = str(case_dir / "r" / "union_perf_nested.result")
        testCase = "union_perf_nested"
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
