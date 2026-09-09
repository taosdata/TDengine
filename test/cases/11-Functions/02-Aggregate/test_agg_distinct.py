###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Test suite for COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT) aggregate functions.

Since: v3.4.2.0
Labels: common, ci
"""

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

    def test_error_distinct_with_session_window(self):
        """DISTINCT with SESSION window is not supported

        Verifies error distinct with session window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error("select count(distinct c_int) from t1 session(ts, 2s)")

    def test_error_distinct_with_state_window(self):
        """DISTINCT with STATE_WINDOW is not supported

        Verifies error distinct with state window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error("select count(distinct c_int) from t1 state_window(c_bool)")

    def test_error_distinct_with_event_window(self):
        """DISTINCT with EVENT_WINDOW is not supported

        Verifies error distinct with event window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdSql.error(
            "select count(distinct c_int) from t1 "
            "event_window start with c_int > 1 end with c_int > 3"
        )

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
