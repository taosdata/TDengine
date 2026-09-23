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

import pytest


pytestmark = pytest.mark.timeout(360)


class TestPartitionCompactSpill:
    updatecfgDict = {
        "encryptAlgorithm": "sm4",
        "encryptScope": "query_spill",
    }

    DB_NAME = "test_partition_compact_spill"
    TABLE_NAME = "data"
    BATCH_SIZE = 500
    # Exceed 100 MiB even with the minimum 512-byte Partition page.
    GROUP_COUNT = 205600
    BASE_TS = 1700000000000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare_data(self):
        tdSql.execute(f"drop database if exists {self.DB_NAME}")
        tdSql.execute(f"create database {self.DB_NAME}")
        tdSql.execute(f"use {self.DB_NAME}")
        tdSql.execute(
            f"create table {self.TABLE_NAME} "
            f"(ts timestamp, gid int, value varchar(32), metric int)"
        )

        values = []
        timestamp = self.BASE_TS
        expected_rows = 0
        expected_gid_sum = 0
        expected_length_sum = 0
        expected_value_rows = 0
        expected_metric_rows = 0
        expected_metric_sum = 0

        def flush_batch():
            if not values:
                return
            tdSql.execute(
                f"insert into {self.TABLE_NAME} values " + " ".join(values)
            )
            values.clear()

        def append_row(gid, value):
            # Every third row stores NULL in the var-length column and every
            # fourth row in the fixed-length one, so both NULL encodings -
            # the offset marker and the bitmap bit - cross a spill boundary.
            nonlocal timestamp, expected_rows, expected_gid_sum
            nonlocal expected_length_sum, expected_value_rows
            nonlocal expected_metric_rows, expected_metric_sum
            value_null = expected_rows % 3 == 0
            metric_null = expected_rows % 4 == 0
            metric = gid + 1

            value_sql = "NULL" if value_null else f"'{value}'"
            metric_sql = "NULL" if metric_null else f"{metric}"
            values.append(f"({timestamp}, {gid}, {value_sql}, {metric_sql})")

            timestamp += 1
            expected_rows += 1
            expected_gid_sum += gid
            if not value_null:
                expected_length_sum += len(value)
                expected_value_rows += 1
            if not metric_null:
                expected_metric_sum += metric
                expected_metric_rows += 1
            if len(values) == self.BATCH_SIZE:
                flush_batch()

        for _ in range(100):
            append_row(-1, "hot-value")
        for gid in range(self.GROUP_COUNT - 800):
            append_row(gid, f"value-{gid}")
        for _ in range(100):
            append_row(-1, "hot-value")
        for gid in range(self.GROUP_COUNT - 800, self.GROUP_COUNT - 400):
            append_row(gid, f"value-{gid}")
        for _ in range(100):
            append_row(-1, "hot-value")
        for gid in range(self.GROUP_COUNT - 400, self.GROUP_COUNT):
            append_row(gid, f"value-{gid}")
        flush_batch()

        self.expected_rows = expected_rows
        self.expected_gid_sum = expected_gid_sum
        self.expected_length_sum = expected_length_sum
        self.expected_value_rows = expected_value_rows
        self.expected_metric_rows = expected_metric_rows
        self.expected_metric_sum = expected_metric_sum

    # --- impl ---

    def do_compact_spill(self):
        tdSql.query(
            "select count(*), sum(gid), sum(length(value)) from "
            f"(select gid, value from {self.TABLE_NAME} partition by gid)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.expected_rows)
        tdSql.checkData(0, 1, self.expected_gid_sum)
        tdSql.checkData(0, 2, self.expected_length_sum)
        print("partition compact spill ......................... [ passed ]")

    def do_compact_spill_nulls(self):
        # count() skips NULL, so the counts below differ from count(*) only
        # if the NULL markers survive the spill round trip. A lost marker
        # also inflates sum(metric), since the payload of a NULL fixed-length
        # row is zero rather than absent.
        tdSql.query(
            "select count(*), count(value), count(metric), sum(metric) from "
            f"(select gid, value, metric from {self.TABLE_NAME} "
            "partition by gid)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.expected_rows)
        tdSql.checkData(0, 1, self.expected_value_rows)
        tdSql.checkData(0, 2, self.expected_metric_rows)
        tdSql.checkData(0, 3, self.expected_metric_sum)
        print("partition compact spill nulls .................. [ passed ]")

    def do_interp_reserved_column_spill(self):
        start = self.BASE_TS
        end = start + self.expected_rows
        sql = (
            "select gid + 1, cast(_irowts as bigint), interp(gid, 1) "
            f"from {self.TABLE_NAME} partition by gid + 1 "
            f"range({start}, {end}) "
            f"every({self.expected_rows}a) fill(prev)"
        )
        tdSql.query("explain " + sql)
        assert any("Partition" in str(row) for row in tdSql.queryResult)
        tdSql.query(sql, queryTimes=1)
        expected = [(0, start, -1)] + [
            (gid + 1, end, gid) for gid in range(-1, self.GROUP_COUNT)
        ]
        assert sorted(map(tuple, tdSql.queryResult)) == expected
        print("interp reserved spill ............... [ passed ]")

    # --- main ---

    def test_partition_compact_spill(self):
        """Verify group-level compact spill preserves partition results

        1. Fill more than 100 MB of single-page partition groups
        2. Spill the hot group repeatedly as new rows arrive
        3. Verify encrypted variable-length fragments are replayed correctly
        4. Verify NULLs in both a var-length and a fixed-length column
           survive the spill round trip
        5. Verify expression grouping with fill(prev) replays reserved
           columns through encrypted Partition spill files

        Catalog:
            - Query:PartitionBy

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-30 Tony Add compact spill regression coverage
            - 2026-07-30 Tony Cap the regression runtime at six minutes
            - 2026-08-03 Tony Cover NULL round trip through the spill path
            - 2026-09-10 Tony Cover reserved columns in INTERP spill replay

        """
        self.prepare_data()
        self.do_compact_spill()
        self.do_compact_spill_nulls()
        self.do_interp_reserved_column_spill()
