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

from new_test_framework.utils import tdLog, tdSql, etool

import os
import statistics
import time

class TestPartitionFirstStateWindowPerformance:
    DB = os.getenv("PF_STATE_PERF_DB", "perf_pf_state_window")
    STB = "meters"
    GROUPS = int(os.getenv("PF_STATE_PERF_GROUPS", "1000"))
    ROWS_PER_GROUP = int(os.getenv("PF_STATE_PERF_ROWS_PER_GROUP", "20"))
    VGROUPS = int(os.getenv("PF_STATE_PERF_VGROUPS", "1"))
    PAYLOAD_BYTES = int(os.getenv("PF_STATE_PERF_PAYLOAD_BYTES", "0"))
    PARTITION_MODE = os.getenv("PF_STATE_PERF_PARTITION_MODE", "tag")
    RUNS = int(os.getenv("PF_STATE_PERF_RUNS", "11"))
    MIN_GAIN_PCT = float(os.getenv(
        "PF_STATE_PERF_MIN_GAIN_PCT", "-15" if PARTITION_MODE == "column" else "10"
    ))
    BASE_TS = 1750000000000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def _prepare_data(self):
        assert self.GROUPS > 0
        assert self.ROWS_PER_GROUP > 0
        assert self.VGROUPS > 0
        assert 0 <= self.PAYLOAD_BYTES <= 4096
        assert self.PARTITION_MODE in ("tag", "column")
        assert self.RUNS > 0

        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(
            f"create database {self.DB} vgroups {self.VGROUPS} precision 'ms'"
        )
        tdSql.execute(f"use {self.DB}")
        payload_column = (
            f", payload binary({self.PAYLOAD_BYTES})"
            if self.PAYLOAD_BYTES > 0
            else ""
        )
        tdSql.execute(
            f"create stable {self.STB} (ts timestamp, val int, st int, "
            "category int"
            f"{payload_column}) "
            "tags (gid int)"
        )

        create_parts = []
        for group_id in range(self.GROUPS):
            create_parts.append(
                f"d{group_id} using {self.STB} tags ({group_id})"
            )
            if len(create_parts) == 50:
                tdSql.execute("create table " + " ".join(create_parts))
                create_parts = []
        if create_parts:
            tdSql.execute("create table " + " ".join(create_parts))

        payload = "x" * self.PAYLOAD_BYTES
        for group_id in range(self.GROUPS):
            values = []
            for row_index in range(self.ROWS_PER_GROUP):
                ts = self.BASE_TS + row_index * 1000
                payload_value = f", '{payload}'" if self.PAYLOAD_BYTES > 0 else ""
                values.append(
                    f"({ts}, {row_index}, 1, {group_id}{payload_value})"
                )
            tdSql.execute(
                f"insert into d{group_id} values " + " ".join(values)
            )

        tdSql.execute(f"flush database {self.DB}")

    def _query_rows(self, sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(row, col) for col in range(len(tdSql.queryResult[row])))
            for row in range(tdSql.getRows())
        ]

    def _measure_ms(self, sql):
        start = time.perf_counter()
        rows = self._query_rows(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000
        return elapsed_ms, rows

    # --- impl ---

    def do_performance_comparison(self):
        self._prepare_data()
        payload_projection = ", first(payload)" if self.PAYLOAD_BYTES > 0 else ""
        partition_key = "gid" if self.PARTITION_MODE == "tag" else "category"
        plain_sql = (
            f"select {partition_key}, _wstart, count(*){payload_projection} "
            f"from {self.STB} partition by {partition_key} "
            f"state_window(st) order by {partition_key}"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ {partition_key}, _wstart, "
            f"count(*){payload_projection} from {self.STB} "
            f"partition by {partition_key} state_window(st) "
            f"order by {partition_key}"
        )

        plain_rows = self._query_rows(plain_sql)
        hinted_rows = self._query_rows(hinted_sql)
        assert hinted_rows == plain_rows
        assert len(hinted_rows) == self.GROUPS
        assert all(row[2] == self.ROWS_PER_GROUP for row in hinted_rows)

        for _ in range(2):
            self._query_rows(plain_sql)
            self._query_rows(hinted_sql)

        plain_samples = []
        hinted_samples = []
        for run_index in range(self.RUNS):
            if run_index % 2 == 0:
                plain_ms, _ = self._measure_ms(plain_sql)
                hinted_ms, _ = self._measure_ms(hinted_sql)
            else:
                hinted_ms, _ = self._measure_ms(hinted_sql)
                plain_ms, _ = self._measure_ms(plain_sql)
            plain_samples.append(plain_ms)
            hinted_samples.append(hinted_ms)

        plain_median = statistics.median(plain_samples)
        hinted_median = statistics.median(hinted_samples)
        gain_pct = (plain_median - hinted_median) * 100 / plain_median

        tdLog.info(
            "PARTITION_FIRST STATE_WINDOW performance: "
            f"groups={self.GROUPS}, rows_per_group={self.ROWS_PER_GROUP}, "
            f"vgroups={self.VGROUPS}, payload_bytes={self.PAYLOAD_BYTES}, "
            f"partition_mode={self.PARTITION_MODE}, "
            f"runs={self.RUNS}, default_median_ms={plain_median:.3f}, "
            f"optimized_median_ms={hinted_median:.3f}, gain_pct={gain_pct:.2f}, "
            f"required_gain_pct={self.MIN_GAIN_PCT:.2f}"
        )
        tdLog.info(f"default samples ms: {plain_samples}")
        tdLog.info(f"optimized samples ms: {hinted_samples}")

        assert gain_pct >= self.MIN_GAIN_PCT, (
            f"performance acceptance failed: gain {gain_pct:.2f}% is below "
            f"required {self.MIN_GAIN_PCT:.2f}%"
        )

        print("partition-first state-window performance ........ [ passed ]")

    # --- main ---

    def test_partition_first_state_window_performance(self):
        """Compare default and partition-first state-window latency

        1. Build a large-number, small-group workload
        2. Verify default and optimized query results are identical
        3. Cover tag or regular-column keys, vgroups, and narrow or wide rows
        4. Compare alternating warm-cache median latency
        5. Enforce a 10% tag gain or a 15% column regression tolerance by default

        Catalog:
            - QueryPerformance

        Since: v3.4.3.0

        Labels: common,performance

        Jira: None

        History:
            - 2026-08-24 Peng Rongkun Add state-window partition benchmark
            - 2026-08-24 Peng Rongkun Increase pressure and add vgroup/row-width parameters
            - 2026-08-25 Peng Rongkun Add regular-column partition mode
            - 2026-09-10 Peng Rongkun Align default column threshold with its plan

        """
        self.do_performance_comparison()
