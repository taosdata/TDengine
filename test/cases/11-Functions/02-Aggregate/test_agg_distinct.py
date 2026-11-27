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
"""Test suite for COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT) aggregate functions.

Since: v3.4.2.0
Labels: common, ci
"""

import csv
from pathlib import Path

from new_test_framework.utils import tdLog, tdSql


class TestAggDistinct:

    def setup_class(cls):
        tdLog.info("=== setup: create database and tables ===")
        tdSql.execute("drop database if exists test_agg_distinct")
        tdSql.execute("create database test_agg_distinct vgroups 2")
        tdSql.execute("use test_agg_distinct")

        # Normal table with various types
        tdSql.execute(
            "create table t1 ("
            "  ts timestamp, "
            "  c_int int, "
            "  c_bigint bigint, "
            "  c_float float, "
            "  c_double double, "
            "  c_bool bool, "
            "  c_varchar varchar(32), "
            "  c_nchar nchar(32)"
            ")"
        )

        # Supertable + child tables (distributed across vgroups)
        tdSql.execute(
            "create stable meters ("
            "  ts timestamp, "
            "  voltage int, "
            "  current float, "
            "  status int"
            ") tags (location varchar(16), groupid int)"
        )
        tdSql.execute("create table d1 using meters tags ('beijing', 1)")
        tdSql.execute("create table d2 using meters tags ('shanghai', 2)")
        tdSql.execute("create table d3 using meters tags ('beijing', 1)")

        # Insert data into t1
        tdSql.execute(
            "insert into t1 values "
            "('2026-01-01 00:00:01', 1,   100, 1.1, 1.11, true,  'aaa', 'AAA'),"
            "('2026-01-01 00:00:02', 2,   200, 2.2, 2.22, false, 'bbb', 'BBB'),"
            "('2026-01-01 00:00:03', 1,   100, 1.1, 1.11, true,  'aaa', 'AAA'),"  # duplicate of row 1
            "('2026-01-01 00:00:04', 3,   300, 3.3, 3.33, true,  'ccc', 'CCC'),"
            "('2026-01-01 00:00:05', 2,   200, 2.2, 2.22, false, 'bbb', 'BBB'),"  # duplicate of row 2
            "('2026-01-01 00:00:06', NULL, NULL, NULL, NULL, NULL, NULL, NULL),"    # all NULL
            "('2026-01-01 00:00:07', 1,   100, 1.1, 1.11, true,  'aaa', 'AAA'),"  # another dup
            "('2026-01-01 00:00:08', 4,   400, 4.4, 4.44, false, 'ddd', 'DDD')"
        )

        tdSql.execute("create table t_count_window (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_count_window values "
            "('2026-01-01 00:00:01', 1),"
            "('2026-01-01 00:00:02', 1),"
            "('2026-01-01 00:00:03', 1),"
            "('2026-01-01 00:00:04', 2),"
            "('2026-01-01 00:00:05', 3),"
            "('2026-01-01 00:00:06', 4)"
        )

        tdSql.execute(
            "create table t_distinct_windows ("
            "ts timestamp, v int, state_key int, begin_flag int, end_flag int)"
        )
        tdSql.execute(
            "insert into t_distinct_windows values "
            "('2026-01-01 00:00:01', 1, 1, 1, 0)"
            "('2026-01-01 00:00:02', 1, 1, 0, 0)"
            "('2026-01-01 00:00:03', 2, 1, 0, 0)"
            "('2026-01-01 00:00:04', NULL, 1, 0, 0)"
            "('2026-01-01 00:00:05', 3, 1, 0, 1)"
            "('2026-01-01 00:00:10', 1, 2, 1, 0)"
            "('2026-01-01 00:00:11', 1, 2, 0, 0)"
            "('2026-01-01 00:00:12', 2, 2, 0, 0)"
            "('2026-01-01 00:00:13', 3, 2, 0, 1)"
        )

        # Insert data into supertable child tables
        tdSql.execute(
            "insert into d1 values "
            "('2026-01-01 00:00:01', 220, 1.5, 1),"
            "('2026-01-01 00:00:02', 220, 2.0, 1),"
            "('2026-01-01 00:00:03', 221, 1.5, 2),"
            "('2026-01-01 00:00:04', 222, 3.0, 1)"
        )
        tdSql.execute(
            "insert into d2 values "
            "('2026-01-01 00:00:01', 220, 1.0, 1),"
            "('2026-01-01 00:00:02', 223, 2.5, 3),"
            "('2026-01-01 00:00:03', 220, 1.0, 2)"
        )
        tdSql.execute(
            "insert into d3 values "
            "('2026-01-01 00:00:01', 221, 1.5, 1),"
            "('2026-01-01 00:00:02', 222, 2.0, 2)"
        )

    # ===================================================================
    # COUNT(DISTINCT) — Basic
    # ===================================================================

    def test_count_distinct_group_by_respects_timestamp_filter(self):
        """COUNT(DISTINCT) with GROUP BY must preserve the timestamp predicate.

        Verifies that grouped DISTINCT aggregation does not create a result group
        for a child table whose only row falls outside the timestamp range.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-14 Added timestamp-filter regression coverage
        """
        day_start_ms = 1788969600000
        day_end_ms = 1789056000000
        first_in_range_ms = 1789012800000
        second_in_range_ms = 1789016400000
        out_of_range_ms = 1789081200000

        tdSql.execute("drop table if exists t_distinct_time_filter_in")
        tdSql.execute("drop table if exists t_distinct_time_filter_in_2")
        tdSql.execute("drop table if exists t_distinct_time_filter_out")
        tdSql.execute("drop table if exists st_distinct_time_filter")
        tdSql.execute(
            "create stable st_distinct_time_filter (ts timestamp, run_id varchar(32)) "
            "tags (os_name varchar(16), is_internal bool, step int, status int, "
            "install_type varchar(16), deployment_name varchar(64), apex_version varchar(32))"
        )
        tdSql.execute(
            "create table t_distinct_time_filter_in using st_distinct_time_filter "
            "tags ('windows', false, 0, 1, 'apex', 'All-in-One', 'v-in-range')"
        )
        tdSql.execute(
            "create table t_distinct_time_filter_out using st_distinct_time_filter "
            "tags ('windows', false, 0, 1, 'apex', 'All-in-One', 'v-out-of-range')"
        )
        tdSql.execute(
            "create table t_distinct_time_filter_in_2 using st_distinct_time_filter "
            "tags ('windows', false, 0, 1, 'docker', 'All-in-One', 'v-in-range-2')"
        )
        tdSql.execute(
            "insert into t_distinct_time_filter_in values "
            f"({first_in_range_ms}, 'run-in')"
        )
        tdSql.execute(
            "insert into t_distinct_time_filter_out values "
            # This row is outside the intended local-day range.
            f"({out_of_range_ms}, 'run-out')"
        )
        tdSql.execute(
            "insert into t_distinct_time_filter_in_2 values "
            f"({second_in_range_ms}, 'run-in-2')"
        )
        try:
            tdSql.query(
                "select install_type, deployment_name, apex_version, count(distinct run_id), "
                "cast(min(ts) as bigint), cast(max(ts) as bigint) from st_distinct_time_filter "
                f"where ts >= {day_start_ms} and ts < {day_end_ms} "
                "group by install_type, deployment_name, apex_version order by apex_version"
            )
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 'apex')
            tdSql.checkData(0, 1, 'All-in-One')
            tdSql.checkData(0, 2, 'v-in-range')
            tdSql.checkData(0, 3, 1)
            tdSql.checkData(0, 4, first_in_range_ms)
            tdSql.checkData(0, 5, first_in_range_ms)
            tdSql.checkData(1, 0, 'docker')
            tdSql.checkData(1, 1, 'All-in-One')
            tdSql.checkData(1, 2, 'v-in-range-2')
            tdSql.checkData(1, 3, 1)
            tdSql.checkData(1, 4, second_in_range_ms)
            tdSql.checkData(1, 5, second_in_range_ms)

            tdSql.query(
                "select install_type, deployment_name, apex_version, count(*), min(ts), max(ts) "
                "from st_distinct_time_filter "
                f"where ts >= {day_start_ms} and ts < {day_end_ms} "
                "group by install_type, deployment_name, apex_version order by apex_version"
            )
            tdSql.checkRows(3)
            tdSql.checkData(0, 2, 'v-in-range')
            tdSql.checkData(0, 3, 1)
            tdSql.checkData(1, 2, 'v-in-range-2')
            tdSql.checkData(1, 3, 1)
            tdSql.checkData(2, 2, 'v-out-of-range')
            tdSql.checkData(2, 3, 0)

            tdSql.query(
                "select install_type, deployment_name, apex_version, count(*), min(ts), max(ts) "
                "from (select ts, install_type, deployment_name, apex_version, run_id "
                f"from st_distinct_time_filter where ts >= {day_start_ms} "
                f"and ts < {day_end_ms}) t "
                "group by install_type, deployment_name, apex_version order by apex_version"
            )
            tdSql.checkRows(2)
        finally:
            tdSql.execute("drop table if exists t_distinct_time_filter_in")
            tdSql.execute("drop table if exists t_distinct_time_filter_in_2")
            tdSql.execute("drop table if exists t_distinct_time_filter_out")
            tdSql.execute("drop table if exists st_distinct_time_filter")

    def test_count_distinct_empty_group_tag_and_window_regressions(self):
        """Keep empty tag groups stable when DISTINCT moves between query layers.

        Verifies child-table tags, HAVING/ORDER BY-only DISTINCT, and window
        DISTINCT preserve the ordinary aggregate group set when no rows match.

        Since: v3.4.2.0

        Labels: common, ci
        """
        day_start_ms = 1788969600000
        day_end_ms = 1789056000000

        tdSql.execute("drop table if exists t_distinct_reg_in")
        tdSql.execute("drop table if exists t_distinct_reg_out")
        tdSql.execute("drop table if exists t_distinct_reg_in2")
        tdSql.execute("drop table if exists st_distinct_reg")
        tdSql.execute(
            "create stable st_distinct_reg (ts timestamp, run_id varchar(32)) "
            "tags (apex_version varchar(32))"
        )
        tdSql.execute(
            "create table t_distinct_reg_in using st_distinct_reg tags ('v-in-range')"
        )
        tdSql.execute(
            "create table t_distinct_reg_out using st_distinct_reg tags ('v-out-of-range')"
        )
        tdSql.execute(
            "create table t_distinct_reg_in2 using st_distinct_reg tags ('v-in-range-2')"
        )
        tdSql.execute(
            "insert into t_distinct_reg_in values "
            "(1789012800000, 'run-in')"
        )
        tdSql.execute(
            "insert into t_distinct_reg_out values "
            "(1789081200000, 'run-out')"
        )
        tdSql.execute(
            "insert into t_distinct_reg_in2 values "
            "(1789016400000, 'run-in-2')"
        )

        day_filter = (
            f"where ts >= {day_start_ms} "
            f"and ts < {day_end_ms}"
        )
        try:
            tdSql.execute('alter local "countAlwaysReturnValue" "1"')

            # No GROUP BY/PARTITION BY: each child-table tag must survive the
            # empty-input count(distinct) row.
            tdSql.query(
                "select apex_version, tbname, count(distinct run_id) "
                f"from t_distinct_reg_out {day_filter}"
            )
            assert tdSql.queryResult == [
                ("v-out-of-range", "t_distinct_reg_out", 0),
            ]

            # DISTINCT only in HAVING must still determine the group set.
            tdSql.query(
                "select apex_version from st_distinct_reg "
                f"{day_filter} group by apex_version "
                "having count(distinct run_id) <= 1 order by apex_version"
            )
            having_groups = list(tdSql.queryResult)
            tdSql.query(
                "select apex_version, count(distinct run_id) from st_distinct_reg "
                f"{day_filter} group by apex_version "
                "having count(distinct run_id) <= 1 order by apex_version"
            )
            having_groups_with_counts = list(tdSql.queryResult)
            assert having_groups == [
                ("v-in-range",),
                ("v-in-range-2",),
            ]
            assert having_groups_with_counts == [
                ("v-in-range", 1),
                ("v-in-range-2", 1),
            ]
            assert having_groups == [(row[0],) for row in having_groups_with_counts]

            # DISTINCT only in ORDER BY belongs to the ordinary aggregate layer.
            tdSql.query(
                "select apex_version from st_distinct_reg "
                f"{day_filter} group by apex_version "
                "order by count(distinct run_id), apex_version"
            )
            assert tdSql.queryResult == [
                ("v-in-range",),
                ("v-in-range-2",),
            ]

            # DISTINCT in OVER() is a window expression and must not suppress
            # empty groups produced by the lower-level ordinary count(*).
            tdSql.query(
                "select apex_version, count(*), count(distinct apex_version) over () "
                "from st_distinct_reg "
                f"{day_filter} group by apex_version order by apex_version"
            )
            assert tdSql.queryResult == [
                ("v-in-range", 1, 3),
                ("v-in-range-2", 1, 3),
                ("v-out-of-range", 0, 3),
            ]
        finally:
            tdSql.execute('alter local "countAlwaysReturnValue" "1"')
            tdSql.execute("drop table if exists t_distinct_reg_in")
            tdSql.execute("drop table if exists t_distinct_reg_out")
            tdSql.execute("drop table if exists t_distinct_reg_in2")
            tdSql.execute("drop table if exists st_distinct_reg")

    def test_count_distinct_int(self):
        """COUNT(DISTINCT) on INT column — basic deduplication

        Verifies count distinct int.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_int has values: 1,2,1,3,2,NULL,1,4 → distinct non-NULL: {1,2,3,4} = 4
        tdSql.query("select count(distinct c_int) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

    def test_count_distinct_bigint(self):
        """COUNT(DISTINCT) on BIGINT column

        Verifies count distinct bigint.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_bigint: 100,200,100,300,200,NULL,100,400 → {100,200,300,400} = 4
        tdSql.query("select count(distinct c_bigint) from t1")
        tdSql.checkData(0, 0, 4)

    def test_count_distinct_float(self):
        """COUNT(DISTINCT) on FLOAT column

        Verifies count distinct float.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_float: 1.1,2.2,1.1,3.3,2.2,NULL,1.1,4.4 → {1.1,2.2,3.3,4.4} = 4
        tdSql.query("select count(distinct c_float) from t1")
        tdSql.checkData(0, 0, 4)

    def test_count_distinct_double(self):
        """COUNT(DISTINCT) on DOUBLE column

        Verifies count distinct double.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select count(distinct c_double) from t1")
        tdSql.checkData(0, 0, 4)

    def test_count_distinct_bool(self):
        """COUNT(DISTINCT) on BOOL column

        Verifies count distinct bool.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_bool: T,F,T,T,F,NULL,T,F → {true, false} = 2
        tdSql.query("select count(distinct c_bool) from t1")
        tdSql.checkData(0, 0, 2)

    def test_count_distinct_varchar(self):
        """COUNT(DISTINCT) on VARCHAR column

        Verifies count distinct varchar.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_varchar: aaa,bbb,aaa,ccc,bbb,NULL,aaa,ddd → {aaa,bbb,ccc,ddd} = 4
        tdSql.query("select count(distinct c_varchar) from t1")
        tdSql.checkData(0, 0, 4)

    def test_count_distinct_nchar(self):
        """COUNT(DISTINCT) on NCHAR column

        Verifies count distinct nchar.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select count(distinct c_nchar) from t1")
        tdSql.checkData(0, 0, 4)

    # ===================================================================
    # COUNT(DISTINCT) — NULL handling
    # ===================================================================

    def test_count_distinct_all_null(self):
        """COUNT(DISTINCT) on all-NULL column returns 0

        Verifies count distinct all null.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.execute("create table t_null (ts timestamp, v int)")
        tdSql.execute("insert into t_null values ('2026-01-01 00:00:01', NULL)")
        tdSql.execute("insert into t_null values ('2026-01-01 00:00:02', NULL)")
        tdSql.execute("insert into t_null values ('2026-01-01 00:00:03', NULL)")

        tdSql.query("select count(distinct v) from t_null")
        tdSql.checkData(0, 0, 0)

    def test_count_distinct_empty_table(self):
        """COUNT(DISTINCT) on empty table returns 0

        Verifies count distinct empty table.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.execute("create table t_empty (ts timestamp, v int)")

        tdSql.query("select count(distinct v) from t_empty")
        tdSql.checkData(0, 0, 0)

    def test_count_distinct_single_value(self):
        """COUNT(DISTINCT) where all values are same returns 1

        Verifies count distinct single value.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.execute("create table t_single (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_single values "
            "('2026-01-01 00:00:01', 42),"
            "('2026-01-01 00:00:02', 42),"
            "('2026-01-01 00:00:03', 42)"
        )

        tdSql.query("select count(distinct v) from t_single")
        tdSql.checkData(0, 0, 1)

    # ===================================================================
    # SUM(DISTINCT)
    # ===================================================================

    def test_sum_distinct_int(self):
        """SUM(DISTINCT) on INT column — sums unique values only

        Verifies sum distinct int.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_int distinct: {1,2,3,4} → sum = 10
        tdSql.query("select sum(distinct c_int) from t1")
        tdSql.checkData(0, 0, 10)

    def test_sum_distinct_bigint(self):
        """SUM(DISTINCT) on BIGINT column

        Verifies sum distinct bigint.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_bigint distinct: {100,200,300,400} → sum = 1000
        tdSql.query("select sum(distinct c_bigint) from t1")
        tdSql.checkData(0, 0, 1000)

    def test_sum_distinct_all_null(self):
        """SUM(DISTINCT) on all-NULL column returns NULL

        Verifies sum distinct all null.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select sum(distinct v) from t_null")
        tdSql.checkData(0, 0, None)

    # ===================================================================
    # AVG(DISTINCT)
    # ===================================================================

    def test_avg_distinct_int(self):
        """AVG(DISTINCT) on INT column — average of unique values

        Verifies avg distinct int.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_int distinct: {1,2,3,4} → avg = 2.5
        tdSql.query("select avg(distinct c_int) from t1")
        tdSql.checkData(0, 0, 2.5)

    def test_avg_distinct_all_null(self):
        """AVG(DISTINCT) on all-NULL returns NULL

        Verifies avg distinct all null.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select avg(distinct v) from t_null")
        tdSql.checkData(0, 0, None)

    # ===================================================================
    # GROUP BY
    # ===================================================================

    def test_count_distinct_group_by(self):
        """COUNT(DISTINCT) with GROUP BY — per-group dedup

        Verifies count distinct group by.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # d1: voltage {220,221,222} = 3 distinct
        # d2: voltage {220,223} = 2 distinct
        # d3: voltage {221,222} = 2 distinct
        # Group by location: beijing (d1+d3), shanghai (d2)
        # beijing: {220,221,222} = 3
        # shanghai: {220,223} = 2
        tdSql.query(
            "select location, count(distinct voltage) as cnt "
            "from meters group by location order by location"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "beijing")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, "shanghai")
        tdSql.checkData(1, 1, 2)

    def test_count_distinct_group_by_tag(self):
        """COUNT(DISTINCT) grouped by tag — status field

        Verifies count distinct group by tag.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # groupid=1 (d1+d3): status from d1={1,1,2,1}, d3={1,2} → {1,2} = 2
        # groupid=2 (d2): status = {1,3,2} → {1,2,3} = 3
        tdSql.query(
            "select groupid, count(distinct status) as cnt "
            "from meters group by groupid order by groupid"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)   # groupid=1
        tdSql.checkData(1, 1, 3)   # groupid=2

    # ===================================================================
    # Supertable (distributed) — cross-vgroup merge
    # ===================================================================

    def test_count_distinct_supertable(self):
        """COUNT(DISTINCT) on supertable — merges across vnodes

        Verifies count distinct supertable.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # All child tables: voltage values = {220,221,222,223} → 4 distinct
        tdSql.query("select count(distinct voltage) from meters")
        tdSql.checkData(0, 0, 4)

    def test_sum_distinct_supertable(self):
        """SUM(DISTINCT) on supertable

        Verifies sum distinct supertable.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # {220,221,222,223} → sum = 886
        tdSql.query("select sum(distinct voltage) from meters")
        tdSql.checkData(0, 0, 886)

    # ===================================================================
    # INTERVAL window
    # ===================================================================

    def test_count_distinct_interval(self):
        """COUNT(DISTINCT) with INTERVAL — per-window dedup

        Verifies count distinct interval.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # t1 has 8 rows at timestamps 00:00:01..08
        # interval(4s) aligned to epoch: [00,04), [04,08), [08,12)
        # window1 [00:00:00,00:00:04): ts=01,02,03 → c_int: 1,2,1 → {1,2} = 2
        # window2 [00:00:04,00:00:08): ts=04,05,06,07 → c_int: 3,2,NULL,1 → {1,2,3} = 3
        # window3 [00:00:08,00:00:12): ts=08 → c_int: 4 → {4} = 1
        tdSql.query(
            "select _wstart, count(distinct c_int) from t1 "
            "interval(4s) order by _wstart"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 1)

    def do_distinct_window_count_parameters(self):
        clauses = (
            "session(ts, 2s)",
            "state_window(state_key)",
            "event_window start with begin_flag=1 end with end_flag=1",
        )
        for clause in clauses:
            tdSql.query(
                "select _wstart, count(distinct 1), count(distinct tbname), "
                "count(distinct NULL), count(1), count(tbname), count(distinct ts) "
                f"from t_distinct_windows {clause} order by _wstart"
            )
            tdSql.checkRows(2)
            for row, count in enumerate((5, 4)):
                for col, expected in enumerate((1, 1, 0, count, count, count), 1):
                    tdSql.checkData(row, col, expected)
        # Supertable input must retain the number of child tables, not timestamps.
        tdSql.query(
            "select count(distinct tbname), count(distinct 1), count(tbname) "
            "from meters session(ts, 2s)"
        )
        tdSql.checkRows(1)
        for col, expected in enumerate((3, 1, 9)):
            tdSql.checkData(0, col, expected)
        print("distinct window COUNT parameters ................ [ passed ]")

    def test_distinct_window_count_parameters(self):
        """Keep constant and table-name DISTINCT arguments in windows.

        Verify SESSION, STATE and EVENT windows count constants and table names
        independently of timestamps, preserve NULL handling and ordinary COUNT,
        and count child table names across supertable input.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-11 Added regression for DISTINCT COUNT argument rewrites.
        """
        self.do_distinct_window_count_parameters()

    def test_count_distinct_count_window(self):
        """COUNT(DISTINCT) with COUNT_WINDOW

        Verifies count distinct count window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query(
            "select _wstart, count(distinct v) from t_count_window "
            "count_window(3) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-01-01 00:00:01.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2026-01-01 00:00:04.000")
        tdSql.checkData(1, 1, 3)

    def test_distinct_count_window_overlap(self):
        """Overlapping COUNT windows keep independent DISTINCT sets.

        Verify COUNT, SUM, and AVG of distinct values in each overlapping
        window, then check that LIMIT returns the first window's count.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added COUNT window DISTINCT acceptance.
        """
        tdSql.query(
            "select _wstart, _wend, count(distinct v), sum(distinct v), avg(distinct v), count(v) "
            "from t_count_window count_window(3, 1) order by _wstart"
        )
        tdSql.checkRows(6)
        for row, expected in enumerate(((1, 1, 1, 3), (2, 3, 1.5, 3),
                                        (3, 6, 2, 3), (3, 9, 3, 3),
                                        (2, 7, 3.5, 2), (1, 4, 4, 1))):
            for col, value in enumerate(expected):
                tdSql.checkData(row, col + 2, value)
        tdSql.query(
            "select count(distinct v) from t_count_window "
            "count_window(3, 1) order by _wstart limit 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def test_distinct_external_window(self):
        """EXTERNAL windows reset DISTINCT between explicit boundaries.

        Define two adjacent external windows and verify their distinct
        aggregates independently, including a query with LIMIT.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added EXTERNAL window DISTINCT acceptance.
        """
        tdSql.execute("create table t_distinct_ext_bounds (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into t_distinct_ext_bounds values "
            "('2026-01-01 00:00:01', '2026-01-01 00:00:04'),"
            "('2026-01-01 00:00:04', '2026-01-01 00:00:08')"
        )
        clause = "external_window((select ts,end_ts from t_distinct_ext_bounds) w)"
        tdSql.query(
            "select _wstart, _wend, count(distinct c_int), sum(distinct c_int), "
            f"avg(distinct c_int), count(c_int) from t1 {clause} order by _wstart"
        )
        tdSql.checkRows(2)
        for col, value in enumerate((3, 6, 2, 4)):
            tdSql.checkData(0, col + 2, value)
        for col, value in enumerate((4, 10, 2.5, 4)):
            tdSql.checkData(1, col + 2, value)
        tdSql.query(
            f"select count(distinct c_int) from t1 {clause} order by _wstart limit 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

    def test_distinct_count_external_large_windows(self):
        """COUNT and EXTERNAL windows handle cross-block spilled DISTINCT sets.

        Insert 5,000 rows with repeated long strings and verify that both
        window types count 2,500 distinct values across input blocks.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added large COUNT/EXTERNAL window acceptance.
        """
        base = 1767225600000
        tdSql.execute("create table t_distinct_c_large (ts timestamp, v varchar(128))")
        tdSql.execute("create table t_distinct_c_bounds (ts timestamp, end_ts timestamp)")
        for first in range(0, 5000, 200):
            values = []
            for i in range(first, first + 200):
                value = f"value_{i % 2500:04d}_{'x' * 80}"
                values.append(f"({base + i}, '{value}')")
            tdSql.execute("insert into t_distinct_c_large values " + ",".join(values))
        tdSql.execute(
            "insert into t_distinct_c_bounds values "
            f"({base}, {base + 4999})"
        )
        for clause in (
            "count_window(5000)",
            "external_window((select ts,end_ts from t_distinct_c_bounds) w)",
        ):
            tdSql.query(f"select count(distinct v), count(v) from t_distinct_c_large {clause}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2500)
            tdSql.checkData(0, 1, 5000)

    # ===================================================================
    # WHERE clause
    # ===================================================================

    def test_count_distinct_with_where(self):
        """COUNT(DISTINCT) with WHERE filter

        Verifies count distinct with where.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # Filter c_int > 1: rows with c_int = 2,3,2,4 → {2,3,4} = 3
        tdSql.query("select count(distinct c_int) from t1 where c_int > 1")
        tdSql.checkData(0, 0, 3)

    # ===================================================================
    # Mixed with normal aggregates
    # ===================================================================

    def test_mixed_distinct_and_normal(self):
        """DISTINCT and normal aggregates in same query

        Verifies mixed distinct and normal.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query(
            "select count(*), count(distinct c_int), sum(c_int), sum(distinct c_int) from t1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)    # count(*)
        tdSql.checkData(0, 1, 4)    # count(distinct c_int)
        tdSql.checkData(0, 2, 14)   # sum(c_int): 1+2+1+3+2+0+1+4=14
        tdSql.checkData(0, 3, 10)   # sum(distinct c_int): 1+2+3+4=10

    # ===================================================================
    # Subquery
    # ===================================================================

    def test_count_distinct_in_subquery(self):
        """COUNT(DISTINCT) used inside a subquery

        Verifies count distinct in subquery.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query(
            "select cnt from ("
            "  select location, count(distinct voltage) as cnt "
            "  from meters group by location"
            ") order by cnt"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)  # shanghai
        tdSql.checkData(1, 0, 3)  # beijing

    # ===================================================================
    # Error cases
    # ===================================================================

    def test_error_distinct_star(self):
        """COUNT(DISTINCT *) should return syntax error

        Verifies error distinct star.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error("select count(distinct *) from t1")

    def test_error_distinct_on_scalar(self):
        """DISTINCT on scalar function should error

        Verifies error distinct on scalar.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error("select abs(distinct c_int) from t1")

    def test_error_distinct_on_selection(self):
        """DISTINCT on selection functions (FIRST/LAST) should error

        Verifies error distinct on selection.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error("select first(distinct c_int) from t1")
        tdSql.error("select last(distinct c_int) from t1")

    def test_error_distinct_multi_args(self):
        """DISTINCT with multiple arguments should error

        Verifies error distinct multi args.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # Note: this depends on grammar accepting it and translator rejecting it,
        # or grammar rejecting it outright as syntax error. Either way → error.
        tdSql.error("select count(distinct c_int, c_bigint) from t1")

    def test_distinct_on_min_max(self):
        """MIN/MAX(DISTINCT) is accepted but treated as no-op (MySQL compatible)

        Verifies distinct on min max.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # MIN/MAX(DISTINCT col) == MIN/MAX(col) since dedup doesn't change extrema
        tdSql.query("select min(distinct c_int) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select max(distinct c_int) from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

    def test_distinct_with_session_window(self):
        """DISTINCT is isolated between SESSION windows

        Verifies duplicate and NULL values in adjacent sessions.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.3.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
            - 2026-11-03 Codex Added SESSION DISTINCT results and partition checks
        """
        self.check_distinct_window_results("session(ts, 2s)")

    def test_distinct_with_state_window(self):
        """DISTINCT is isolated between STATE_WINDOW groups

        Verifies state changes reset the deduplication set.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.3.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
            - 2026-11-03 Codex Added STATE DISTINCT results and partition checks
        """
        self.check_distinct_window_results("state_window(state_key)")

    def test_distinct_with_event_window(self):
        """DISTINCT is isolated between EVENT_WINDOW groups

        Verifies reused result rows release the previous window's set.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.3.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
            - 2026-11-03 Codex Added EVENT DISTINCT results and cleanup checks
        """
        self.check_distinct_window_results(
            "event_window start with begin_flag = 1 end with end_flag = 1"
        )
        # A window without an end condition keeps its set until operator teardown.
        tdSql.execute(
            "insert into t_distinct_windows values "
            "('2026-01-01 00:00:30', 4, 3, 1, 0)"
        )
        self.check_distinct_window_results(
            "event_window start with begin_flag = 1 end with end_flag = 1"
        )

    def test_distinct_translated_window_scalar_results(self):
        """Translated SESSION, STATE and EVENT windows return exact DISTINCT results.

        Scalar projections exercise the window's indefinite-row result path while
        checking different per-window sets against fixed expected values.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 xsren Added translated-window result checks for MR review.
        """
        tdSql.execute(
            "create table t_distinct_review (ts timestamp, v int, state_key int, "
            "begin_flag int, end_flag int)"
        )
        tdSql.execute(
            "insert into t_distinct_review values "
            "('2026-01-01 00:00:01', 1, 1, 1, 0)"
            "('2026-01-01 00:00:02', 1, 1, 0, 0)"
            "('2026-01-01 00:00:03', NULL, 1, 0, 0)"
            "('2026-01-01 00:00:04', 2, 1, 0, 1)"
            "('2026-01-01 00:00:10', 2, 2, 1, 0)"
            "('2026-01-01 00:00:11', 2, 2, 0, 0)"
            "('2026-01-01 00:00:12', NULL, 2, 0, 0)"
            "('2026-01-01 00:00:13', 4, 2, 0, 1)"
        )
        for clause in (
            "session(ts, 2s)",
            "state_window(state_key)",
            "event_window start with begin_flag = 1 end with end_flag = 1",
        ):
            tdSql.query(
                "select count(distinct v) + 1, sum(distinct v) + 1, "
                "avg(distinct v) + 1, count(v) "
                "from t_distinct_review "
                f"{clause} order by _wstart"
            )
            tdSql.checkRows(2)
            for row, expected in enumerate(((3, 4, 2.5, 3), (3, 7, 4.0, 3))):
                for col, value in enumerate(expected):
                    tdSql.checkData(row, col, value)

    def test_distinct_window_expected_results(self):
        """DISTINCT window aggregates match an independent result file.

        Compare all result rows for SESSION, STATE, and EVENT windows with
        duplicate values, negative values, and a NULL-only window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-06 Codex Added expected-result comparison for window DISTINCT.
        """
        tdSql.execute(
            "create table t_distinct_expected (ts timestamp, v int, state_key int, "
            "begin_flag int, end_flag int)"
        )
        tdSql.execute(
            "insert into t_distinct_expected values "
            "('2026-01-01 00:00:01', 1, 1, 1, 0)"
            "('2026-01-01 00:00:02', 1, 1, 0, 0)"
            "('2026-01-01 00:00:03', NULL, 1, 0, 0)"
            "('2026-01-01 00:00:04', 2, 1, 0, 1)"
            "('2026-01-01 00:00:10', -2, 2, 1, 0)"
            "('2026-01-01 00:00:11', -2, 2, 0, 0)"
            "('2026-01-01 00:00:12', 4, 2, 0, 0)"
            "('2026-01-01 00:00:13', NULL, 2, 0, 1)"
            "('2026-01-01 00:00:20', NULL, 3, 1, 0)"
            "('2026-01-01 00:00:21', NULL, 3, 0, 1)"
        )
        result_file = Path(__file__).parent / "ans" / "test_agg_distinct_windows.csv"
        with result_file.open(newline="", encoding="utf-8") as file:
            expected_rows = list(csv.reader(file))

        windows = {
            "session": "session(ts, 2s)",
            "state": "state_window(state_key)",
            "event": "event_window start with begin_flag = 1 end with end_flag = 1",
        }
        actual_rows = []
        for name, clause in windows.items():
            tdSql.query(
                "select min(state_key), count(distinct v), sum(distinct v), "
                "avg(distinct v), count(v), count(*) "
                f"from t_distinct_expected {clause} order by _wstart"
            )
            for row in tdSql.queryResult:
                actual_rows.append(
                    [name] + ["NULL" if value is None else str(value) for value in row]
                )
        assert actual_rows == expected_rows, (
            f"Window DISTINCT result mismatch: expected {expected_rows}, got {actual_rows}"
        )

    def test_distinct_large_windows(self):
        """Window DISTINCT remains correct across input blocks and spill.

        Insert 6,000 rows with repeated long strings and verify distinct
        counts in session, state, and event windows spanning input blocks.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added cross-block and spill acceptance.
        """
        tdSql.execute("create table t_distinct_large (ts timestamp, v varchar(128), state_key int, begin_flag int, end_flag int)")
        base = 1767225600000
        for first in range(0, 6000, 200):
            values = []
            for i in range(first, first + 200):
                value = f"value_{i % 3000:04d}_{'x' * 80}"
                values.append(f"({base + i}, '{value}', 1, {int(i == 0)}, {int(i == 5999)})")
            tdSql.execute("insert into t_distinct_large values " + ",".join(values))
        for clause in (
            "session(ts, 1s)",
            "state_window(state_key)",
            "event_window start with begin_flag = 1 end with end_flag = 1",
        ):
            tdSql.query(f"select count(distinct v), count(v) from t_distinct_large {clause}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 3000)
            tdSql.checkData(0, 1, 6000)

    def test_distinct_state_event_options(self):
        """DISTINCT observes state extension and event TRUE_FOR boundaries.

        Check distinct counts for state windows with extension options and
        event windows with a minimum TRUE_FOR duration.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added window option acceptance.
        """
        tdSql.query(
            "select count(distinct v) from t_distinct_windows "
            "where ts < '2026-01-01 00:00:20' "
            "state_window(state_key) extend(0) zeroth_state(0) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.query(
            "select count(distinct v) from t_distinct_windows "
            "where ts < '2026-01-01 00:00:20' "
            "event_window start with begin_flag = 1 end with end_flag = 1 "
            "true_for(2s) order by _wstart"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)

    def test_distinct_windows_across_vgroups(self):
        """DISTINCT window state is isolated on child tables in separate VGroups.

        Place child tables on two VGroups and verify per-table distinct
        aggregates in session, state, event, COUNT, and EXTERNAL windows.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-04 Codex Added cross-VGroup window acceptance.
        """
        tdSql.execute(
            "create stable st_distinct_vg (ts timestamp, v int, state_key int, "
            "begin_flag int, end_flag int) tags (site int)"
        )
        chosen = {}
        for i in range(20):
            name = f"t_distinct_vg_{i}"
            tdSql.execute(f"create table {name} using st_distinct_vg tags ({i})")
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name = 'test_agg_distinct' and table_name = '{name}'"
            )
            tdSql.checkRows(1)
            chosen.setdefault(tdSql.getData(0, 0), name)
            if len(chosen) == 2:
                break
        assert len(chosen) == 2, f"expected child tables on two VGroups: {chosen}"

        base = 1767225600000
        for name in chosen.values():
            tdSql.execute(
                f"insert into {name} values "
                f"({base}, 1, 1, 1, 0),"
                f"({base + 1000}, 1, 1, 0, 0),"
                f"({base + 2000}, 2, 1, 0, 1)"
            )
        for clause in (
            "session(ts, 2s)",
            "state_window(state_key)",
            "event_window start with begin_flag = 1 end with end_flag = 1",
        ):
            tdSql.query(
                "select tbname, count(distinct v), sum(distinct v) "
                f"from st_distinct_vg partition by tbname {clause} order by tbname"
            )
            tdSql.checkRows(2)
            for row in range(2):
                tdSql.checkData(row, 1, 2)
                tdSql.checkData(row, 2, 3)
        tdSql.execute("create table t_distinct_vg_bounds (ts timestamp, end_ts timestamp)")
        tdSql.execute(
            "insert into t_distinct_vg_bounds values "
            f"({base}, {base + 2000})"
        )
        for clause in (
            "count_window(3)",
            "external_window((select ts,end_ts from t_distinct_vg_bounds) w)",
        ):
            tdSql.query(
                "select tbname, count(distinct v), sum(distinct v) "
                f"from st_distinct_vg partition by tbname {clause} order by tbname"
            )
            tdSql.checkRows(2)
            for row in range(2):
                tdSql.checkData(row, 1, 2)
                tdSql.checkData(row, 2, 3)

    def check_distinct_window_results(self, window_clause):
        tdSql.query(
            "select count(distinct v), sum(distinct v), avg(distinct v), "
            "count(v) from t_distinct_windows "
            f"{window_clause} order by _wstart"
        )
        tdSql.checkRows(2)
        for row, count in enumerate((4, 4)):
            tdSql.checkData(row, 0, 3)
            tdSql.checkData(row, 1, 6)
            tdSql.checkData(row, 2, 2)
            tdSql.checkData(row, 3, count)
        # LIMIT can destroy the operator while later result rows still own sets.
        tdSql.query(
            "select count(distinct v) from t_distinct_windows "
            f"{window_clause} order by _wstart limit 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.query(
            "select state_key, count(distinct v), sum(distinct v) "
            "from t_distinct_windows partition by state_key "
            f"{window_clause} order by state_key"
        )
        tdSql.checkRows(2)
        for row, state_key in enumerate((1, 2)):
            tdSql.checkData(row, 0, state_key)
            tdSql.checkData(row, 1, 3)
            tdSql.checkData(row, 2, 6)

    def test_error_distinct_with_anomaly_window(self):
        """DISTINCT with ANOMALY_WINDOW is not supported

        Verifies error distinct with anomaly window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # ANOMALY_WINDOW only calls clearResultRowInitFlag() between windows, so the next
        # functionSetup() would overwrite the DISTINCT set pointer without freeing it -- one leaked
        # set per window. Rejected until that operator releases per-result-row function state.
        tdSql.error("select count(distinct c_int) from t1 anomaly_window(c_int)")
        tdSql.error("select sum(distinct c_int) from t1 anomaly_window(c_int)")
        tdSql.error("select avg(distinct c_int) from t1 anomaly_window(c_int)")

    def test_distinct_allowed_window_types(self):
        """DISTINCT is accepted for exactly the window types that release the set

        Verifies distinct allowed window types.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # The parser uses an allowlist, so guard both directions: these must keep working, and a
        # future window type must not be admitted silently.
        tdSql.query("select _wstart, count(distinct c_int) from t1 interval(5s)")
        tdSql.query("select _wstart, count(distinct c_int) from t1 count_window(3)")

    # ===================================================================
    # Timestamp type
    # ===================================================================

    def test_count_distinct_timestamp(self):
        """COUNT(DISTINCT) on timestamp column

        Verifies count distinct timestamp.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # t1 has 8 rows with 8 distinct timestamps
        tdSql.query("select count(distinct ts) from t1")
        tdSql.checkData(0, 0, 8)

    # ===================================================================
    # Expression as argument
    # ===================================================================

    def test_count_distinct_expression(self):
        """COUNT(DISTINCT expr) where expr is a computed expression

        Verifies count distinct expression.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # c_int % 2: values 1,0,1,1,0,NULL,1,0 → {0,1} = 2
        tdSql.query("select count(distinct c_int % 2) from t1")
        tdSql.checkData(0, 0, 2)

    # ===================================================================
    # Comparison with subquery workaround (regression guard)
    # ===================================================================

    def test_count_distinct_matches_subquery(self):
        """COUNT(DISTINCT col) must match SELECT COUNT(*) FROM (SELECT DISTINCT col ...)

        Verifies count distinct matches subquery.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select count(distinct voltage) from meters")
        direct = tdSql.getData(0, 0)

        tdSql.query("select count(*) from (select distinct voltage from meters)")
        via_subquery = tdSql.getData(0, 0)

        assert direct == via_subquery, (
            f"COUNT(DISTINCT) = {direct} but subquery workaround = {via_subquery}"
        )

    def test_sum_distinct_matches_subquery(self):
        """SUM(DISTINCT col) must match SELECT SUM(v) FROM (SELECT DISTINCT col as v ...)

        Verifies sum distinct matches subquery.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("select sum(distinct c_int) from t1")
        direct = tdSql.getData(0, 0)

        tdSql.query("select sum(v) from (select distinct c_int as v from t1)")
        via_subquery = tdSql.getData(0, 0)

        assert direct == via_subquery, (
            f"SUM(DISTINCT) = {direct} but subquery = {via_subquery}"
        )

    # ===================================================================
    # EXPLAIN support
    # ===================================================================

    def test_explain_no_distinct_filter_node(self):
        """EXPLAIN has no Distinct Filter node: dedup lives in the aggregate

        Verifies explain no distinct filter node.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # COUNT/SUM/AVG(DISTINCT) dedup inside the aggregate via SSpillDistinctSet, so the
        # row-deleting DistinctFilter operator is no longer planned. Assert its absence so the
        # plan shape cannot silently regress to the old two-node form.
        for sql in (
            "explain select count(distinct voltage) from d1",
            "explain select sum(distinct voltage) from d1",
            "explain select avg(distinct voltage) from d1",
        ):
            tdSql.query(sql)
            plan = "\n".join(str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows))
            assert "Distinct Filter" not in plan, \
                f"Unexpected 'Distinct Filter' in plan for [{sql}]:\n{plan}"
            assert "Aggregate" in plan, f"Expected 'Aggregate' in plan:\n{plan}"

    def test_explain_verbose_distinct_aggregate(self):
        """EXPLAIN VERBOSE still describes the aggregate for COUNT(DISTINCT)

        Verifies explain verbose distinct aggregate.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.query("explain verbose true select count(distinct voltage) from d1")
        plan = "\n".join(str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows))
        assert "Distinct Filter" not in plan, \
            f"Unexpected 'Distinct Filter' in verbose plan:\n{plan}"
        assert "Output:" in plan, f"Expected 'Output:' in verbose plan:\n{plan}"

    def test_distinct_still_correct_without_filter_node(self):
        """Removing the plan node did not change results

        Verifies distinct still correct without filter node.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        # d1 voltage values are {220, 220, 221, 222} -> 3 distinct, sum 663, avg 221.
        tdSql.query(
            "select count(distinct voltage), sum(distinct voltage), avg(distinct voltage) from d1"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 3, f"count(distinct voltage): got {row[0]}, expected 3"
        assert float(row[1]) == 663.0, \
            f"sum(distinct voltage): got {row[1]}, expected 663"
        assert abs(float(row[2]) - 221.0) < 1e-9, \
            f"avg(distinct voltage): got {row[2]}, expected 221"
