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

from new_test_framework.utils import tdLog, tdSql, etool

import time
import os


class TestDistinctPrimaryKeyBaseline:
    DB = "distinct_pk_baseline"
    BASE_TS = 1760000000000
    TS_COUNT = 2000
    CHILD_COUNT = 4
    BATCH_SIZE = 100

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def insert_rows(self, table, rows):
        for start in range(0, len(rows), self.BATCH_SIZE):
            values = " ".join(
                "(" + ", ".join(str(value) for value in row) + ")"
                for row in rows[start : start + self.BATCH_SIZE]
            )
            tdSql.execute(f"insert into {table} values {values}")

    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.DB} vgroups 4 keep 36500d")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table if not exists plain (ts timestamp, val int)")
        tdSql.execute("create table if not exists keyed (ts timestamp, pk int primary key, val int)")
        tdSql.execute("create stable if not exists meters (ts timestamp, pk int primary key, val int) tags (device int)")
        tdSql.execute("create stable if not exists simple_meters (ts timestamp, val int) tags (device int)")
        tdSql.execute("create table if not exists simple_meter using simple_meters tags (0)")
        tdSql.execute("create table if not exists simple_meter_1 using simple_meters tags (1)")
        for child in range(self.CHILD_COUNT):
            tdSql.execute(f"create table if not exists meter_{child} using meters tags ({child})")

        tdSql.query("select count(*) from plain")
        plain_count = tdSql.getData(0, 0)
        tdSql.query("select count(*) from keyed")
        composite_count = tdSql.getData(0, 0)
        tdSql.query("select count(*) from meters")
        super_count = tdSql.getData(0, 0)
        tdSql.query("select count(*) from simple_meter")
        simple_child_count = tdSql.getData(0, 0)
        tdSql.query("select count(*) from simple_meter_1")
        second_simple_child_count = tdSql.getData(0, 0)
        expected = (self.TS_COUNT, self.TS_COUNT * 2, self.TS_COUNT * 2 * self.CHILD_COUNT)
        if (plain_count, composite_count, super_count) == (0, 0, 0):
            self.insert_rows("plain", [(self.BASE_TS + i, i % 10) for i in range(self.TS_COUNT)])
            composite_rows = [
                (self.BASE_TS + i, pk, i % 10)
                for i in range(self.TS_COUNT)
                for pk in (0, 1)
            ]
            self.insert_rows("keyed", composite_rows)
            for child in range(self.CHILD_COUNT):
                self.insert_rows(f"meter_{child}", composite_rows)
        elif (plain_count, composite_count, super_count) != expected:
            raise AssertionError(
                f"baseline database has unexpected row counts: "
                f"{(plain_count, composite_count, super_count)} != {expected}"
            )
        if simple_child_count == 0:
            self.insert_rows("simple_meter", [(self.BASE_TS + i, i % 10) for i in range(self.TS_COUNT)])
        elif simple_child_count != self.TS_COUNT:
            raise AssertionError(f"simple_meter has {simple_child_count} rows, expected {self.TS_COUNT}")
        if second_simple_child_count == 0:
            self.insert_rows("simple_meter_1", [(self.BASE_TS + i, i % 10) for i in range(self.TS_COUNT)])
        elif second_simple_child_count != self.TS_COUNT:
            raise AssertionError(f"simple_meter_1 has {second_simple_child_count} rows, expected {self.TS_COUNT}")
        print(f"DATASET db={self.DB} rows={expected + (self.TS_COUNT, self.TS_COUNT)} base_ts={self.BASE_TS}")

    def check_results(self):
        cases = (
            ("select distinct ts from plain", self.TS_COUNT),
            ("select distinct ts, val from plain", self.TS_COUNT),
            ("select distinct val from plain", 10),
            ("select distinct ts from keyed", self.TS_COUNT),
            ("select distinct ts from keyed where pk = 1", self.TS_COUNT),
            ("select distinct ts from keyed where 1 = pk and val = 1", self.TS_COUNT // 10),
            ("select distinct ts from keyed where pk = 1 and val = 100", 0),
            ("select distinct ts from meter_0 where pk = 1", self.TS_COUNT),
            ("select distinct ts from keyed where pk in (0, 1)", self.TS_COUNT),
            ("select distinct ts from keyed where pk = 0 or pk = 1", self.TS_COUNT),
            ("select distinct ts from keyed where pk = null", 0),
            ("select distinct pk from keyed", 2),
            ("select distinct ts, pk from keyed", self.TS_COUNT * 2),
            ("select distinct pk, ts from keyed where pk = 1", self.TS_COUNT),
            ("select distinct ts as t, val from plain where val = 1", self.TS_COUNT // 10),
            ("select distinct ts + 1 from plain", self.TS_COUNT),
            ("select distinct ts from meters", self.TS_COUNT),
            ("select distinct pk from meters", 2),
            ("select distinct ts, pk from meters", self.TS_COUNT * 2),
            ("select distinct tbname, ts, pk from meters", self.TS_COUNT * 2 * self.CHILD_COUNT),
            ("select distinct tbname, ts from meters", self.TS_COUNT * self.CHILD_COUNT),
            ("select distinct tbname, ts from simple_meters", self.TS_COUNT * 2),
            ("select distinct ts, pk from meter_0", self.TS_COUNT * 2),
            ("select distinct ts from simple_meter", self.TS_COUNT),
            ("select distinct ts from meters where device = 0", self.TS_COUNT),
            ("select distinct ts from meters where tbname = 'meter_0' and pk = 1", self.TS_COUNT),
            ("select distinct ts from meters where 'meter_0' = tbname and 1 = pk", self.TS_COUNT),
            ("select distinct ts from meters where tbname = 'meter_0' and pk = 1 and val = 1", self.TS_COUNT // 10),
            ("select distinct ts from meters where tbname = 'meter_0' and pk = 1 and val = 100", 0),
            ("select distinct ts from meters where tbname in ('meter_0', 'meter_1') and pk = 1", self.TS_COUNT),
            ("select distinct ts from meters where tbname = 'meter_0' and val = device and pk = 1", self.TS_COUNT // 10),
            ("select distinct ts from meters where tbname = 'meter_0' and pk in (0, 1)", self.TS_COUNT),
            ("select distinct ts from plain order by ts limit 10 offset 5", 10),
        )
        for sql, expected_rows in cases:
            tdSql.query(sql)
            tdSql.checkRows(expected_rows)
        tdSql.query("select distinct ts from plain order by ts limit 10 offset 5")
        tdSql.checkData(0, 0, self.BASE_TS + 5)
        tdSql.checkData(9, 0, self.BASE_TS + 14)
        tdSql.query("select distinct ts as t, val from plain where val = 1 order by t limit 3")
        tdSql.checkData(0, 0, self.BASE_TS + 1)
        tdSql.checkData(2, 0, self.BASE_TS + 21)
        tdSql.query("select distinct pk, ts from keyed where pk = 1 order by ts limit 3")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(2, 1, self.BASE_TS + 2)
        tdSql.query("select distinct pk, ts from keyed order by ts, pk limit 3")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, self.BASE_TS)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, self.BASE_TS)
        for sql, expected in (
            ("select count(distinct ts) from plain", self.TS_COUNT),
            ("select count(distinct ts) from keyed", self.TS_COUNT),
            ("select count(distinct pk) from keyed", 2),
            ("select count(distinct ts) from meters", self.TS_COUNT),
            ("select count(distinct ts) from meter_0", self.TS_COUNT),
            ("select count(distinct ts) from simple_meter", self.TS_COUNT),
            ("select count(distinct ts) from keyed where pk = 1", self.TS_COUNT),
            ("select count(distinct ts) from meter_0 where pk = 1", self.TS_COUNT),
            ("select count(distinct ts) from keyed where 1 = pk and val = 1", self.TS_COUNT // 10),
            ("select count(distinct ts) from keyed where pk = 1 and val = 100", 0),
            ("select count(distinct ts) from keyed where pk in (0, 1)", self.TS_COUNT),
            ("select count(distinct ts) from keyed where pk = 0 or pk = 1", self.TS_COUNT),
            ("select count(distinct ts) from meters where device = 0 and pk = 1", self.TS_COUNT),
            ("select count(distinct ts) from plain where val = 100", 0),
        ):
            tdSql.query(sql)
            tdSql.checkData(0, 0, expected)
        tdSql.query("select count(distinct ts) from keyed where pk = null")
        tdSql.checkRows(0)
        tdSql.query("select count(distinct ts), sum(val) from plain")
        tdSql.checkData(0, 0, self.TS_COUNT)
        tdSql.checkData(0, 1, 9000)
        tdSql.query("select val, count(distinct ts) from plain group by val order by val")
        tdSql.checkRows(10)
        for value in range(10):
            tdSql.checkData(value, 0, value)
            tdSql.checkData(value, 1, self.TS_COUNT // 10)
        tdSql.query("select count(distinct ts), sum(val) from plain where val = 100")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(distinct ts), sum(val) from keyed")
        tdSql.checkData(0, 0, self.TS_COUNT)
        tdSql.checkData(0, 1, 18000)
        tdSql.query("select pk, count(distinct ts) from keyed group by pk order by pk")
        tdSql.checkRows(2)
        for pk in range(2):
            tdSql.checkData(pk, 0, pk)
            tdSql.checkData(pk, 1, self.TS_COUNT)
        tdSql.query("select pk, count(distinct ts) from meter_0 group by pk order by pk")
        tdSql.checkRows(2)
        for pk in range(2):
            tdSql.checkData(pk, 0, pk)
            tdSql.checkData(pk, 1, self.TS_COUNT)
        tdSql.query("select pk, count(distinct ts) from meters group by pk order by pk")
        tdSql.checkRows(2)
        for pk in range(2):
            tdSql.checkData(pk, 0, pk)
            tdSql.checkData(pk, 1, self.TS_COUNT)
        tdSql.query("select val, count(distinct ts) from keyed group by val order by val")
        tdSql.checkRows(10)
        for value in range(10):
            tdSql.checkData(value, 0, value)
            tdSql.checkData(value, 1, self.TS_COUNT // 10)
        for table in ("keyed", "meter_0", "meters"):
            tdSql.query(f"select ts, count(distinct pk) from {table} group by ts order by ts")
            tdSql.checkRows(self.TS_COUNT)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(self.TS_COUNT - 1, 1, 2)
        tdSql.query("select val, count(distinct pk) from keyed group by val order by val")
        tdSql.checkRows(10)
        for value in range(10):
            tdSql.checkData(value, 0, value)
            tdSql.checkData(value, 1, 2)
        tdSql.query("select tbname, count(distinct ts) from simple_meters group by tbname")
        tdSql.checkRows(2)
        for child in range(2):
            tdSql.checkData(child, 1, self.TS_COUNT)
        tdSql.query("select count(distinct ts) from simple_meters partition by tbname")
        tdSql.checkRows(2)
        for child in range(2):
            tdSql.checkData(child, 0, self.TS_COUNT)
        tdSql.query("select tbname, count(distinct ts) from meters group by tbname")
        tdSql.checkRows(self.CHILD_COUNT)
        for child in range(self.CHILD_COUNT):
            tdSql.checkData(child, 1, self.TS_COUNT)
        tdSql.query("select device, count(distinct ts) from simple_meters group by device")
        tdSql.checkRows(2)
        for child in range(2):
            tdSql.checkData(child, 1, self.TS_COUNT)
        print("distinct primary key correctness ........ [passed]")

    def check_plans(self):
        optimized = os.environ.get("DISTINCT_PK_EXPECT_OPTIMIZED")
        super_optimized = os.environ.get("DISTINCT_PK_EXPECT_SUPER_OPTIMIZED") == "1"
        for sql, expect_aggregate in (
            ("select distinct ts from plain", False),
            ("select distinct ts, pk from keyed", False),
            ("select distinct pk, ts from keyed", False),
            ("select distinct pk, ts from keyed order by ts, pk limit 3", True),
            ("select distinct ts, pk from meter_0", False),
            ("select distinct ts from simple_meter", False),
            ("select distinct ts from keyed", True),
            ("select distinct ts from keyed where pk = 1", False),
            ("select distinct ts from meter_0 where pk = 1", False),
            ("select distinct ts from keyed where 1 = pk and val = 1", False),
            ("select distinct ts from keyed where pk in (0, 1)", True),
            ("select distinct ts from keyed where pk = 0 or pk = 1", True),
            ("select distinct ts from meters where device = 0 and pk = 1", True),
            ("select distinct ts from meters where tbname = 'meter_0' and pk = 1", False),
            ("select distinct ts from meters where 'meter_0' = tbname and 1 = pk", False),
            ("select distinct ts from meters where tbname = 'meter_0' and pk in (0, 1)", True),
            ("select distinct ts from meters where tbname in ('meter_0', 'meter_1') and pk = 1", True),
            ("select distinct pk from keyed", True),
            ("select distinct ts + 1 from plain", True),
            ("select distinct ts from meters", True),
            ("select distinct tbname, ts, pk from meters", False),
            ("select distinct tbname, ts from meters", True),
            ("select distinct tbname, ts from simple_meters", False),
        ):
            rows = tdSql.query(f"explain {sql}", row_tag=True)
            has_aggregate = any("GroupAggregate" in str(row[0]) for row in rows)
            if expect_aggregate or optimized is not None:
                expected = expect_aggregate or optimized == "0"
                if sql.startswith("select distinct tbname") and not expect_aggregate:
                    expected = not super_optimized
                if ("where pk = 1" in sql or "where 1 = pk" in sql) and not expect_aggregate:
                    expected = os.environ.get("DISTINCT_PK_EXPECT_FIXED_PK_OPTIMIZED") != "1"
                if "tbname = 'meter_0' and pk = 1" in sql or "'meter_0' = tbname and 1 = pk" in sql:
                    expected = os.environ.get("DISTINCT_PK_EXPECT_SUPER_FIXED_OPTIMIZED") != "1"
                assert has_aggregate == expected, (sql, rows)
        print("distinct primary key plans .............. [passed]")

    def measure_queries(self):
        cases = (
            ("plain_ts", "select distinct ts from plain"),
            ("plain_ts_limit", "select distinct ts from plain order by ts limit 20"),
            ("composite_key", "select distinct ts, pk from keyed"),
            ("composite_ts", "select distinct ts from keyed"),
            ("fixed_pk_ts", "select distinct ts from keyed where pk = 1"),
            ("fixed_pk_ts_negative", "select distinct ts from keyed where pk in (0, 1)"),
            ("super_ts", "select distinct ts from meters"),
            ("super_key", "select distinct ts, pk from meters"),
            ("super_full_key", "select distinct tbname, ts, pk from meters"),
            ("super_partial_key", "select distinct tbname, ts from meters"),
            ("super_ts_limit", "select distinct ts from meters order by ts limit 20"),
            ("super_fixed_child_pk", "select distinct ts from meters where tbname = 'meter_0' and pk = 1"),
            ("super_fixed_child_pk_negative", "select distinct ts from meters where tbname = 'meter_0' and pk in (0, 1)"),
            ("plain_count_distinct", "select count(distinct ts) from plain"),
            ("child_count_distinct", "select count(distinct ts) from simple_meter"),
            ("composite_count_distinct", "select count(distinct ts) from keyed"),
            ("fixed_pk_count_distinct", "select count(distinct ts) from keyed where pk = 1"),
            ("fixed_pk_count_negative", "select count(distinct ts) from keyed where pk in (0, 1)"),
            ("super_group_tbname_count", "select tbname, count(distinct ts) from simple_meters group by tbname"),
            ("super_group_device_count", "select device, count(distinct ts) from simple_meters group by device"),
            ("mixed_count_distinct", "select count(distinct ts), sum(val) from plain"),
            ("grouped_count_distinct", "select val, count(distinct ts) from plain group by val order by val"),
            ("composite_group_pk_count", "select pk, count(distinct ts) from keyed group by pk order by pk"),
            ("composite_group_val_count", "select val, count(distinct ts) from keyed group by val order by val"),
            ("composite_group_ts_count_pk", "select ts, count(distinct pk) from keyed group by ts order by ts"),
            ("composite_group_val_count_pk", "select val, count(distinct pk) from keyed group by val order by val"),
        )
        for name, sql in cases:
            tdSql.query(sql)
            start = time.perf_counter()
            tdSql.query(sql)
            elapsed_ms = (time.perf_counter() - start) * 1000
            print(f"BASELINE name={name} elapsed_ms={elapsed_ms:.3f} rows={tdSql.queryRows}")
        print("distinct primary key baseline ........... [passed]")

    def test_distinct_primary_key_baseline(self):
        """Validate and measure DISTINCT on primary key columns

        1. Cover ordinary, composite-key, and multi-child super tables.
        2. Check DISTINCT, ordered LIMIT, and COUNT(DISTINCT) results.
        3. Check eligible and ineligible execution plans.
        4. Record fixed-workload query elapsed time without deleting data.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-14 Codex Add reproducible pre-optimization baseline.

        """
        self.prepare_data()
        self.check_results()
        self.check_plans()
        self.measure_queries()
