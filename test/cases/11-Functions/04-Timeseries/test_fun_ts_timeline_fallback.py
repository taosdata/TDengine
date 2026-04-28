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

"""
Test: Timeline Fallback & Primary Key Behavior Matrix

Two independent test cases sharing the same database setup:
  1. test_pk_baseline — original primary key behavior (Section A-G)
  2. test_degraded_timeline — degraded timestamp fallback behavior (Section H-HG)

Database and tables are created once in _init_data(), called by each test.

Since:  v3.4.1.0
Labels: function, timeseries, timeline_fallback, subquery
Jira:   TS-5791
"""

import os
from new_test_framework.utils import tdLog, tdSql, tdCom


DB_NAME = "test_pk_matrix"

INIT_SQLS = [
    f"DROP DATABASE IF EXISTS {DB_NAME}",
    f"CREATE DATABASE {DB_NAME}",
    f"USE {DB_NAME}",

    # Base table: val/t2 non-monotonic so ORDER BY val/t2 creates true ts disorder
    "CREATE TABLE t_base (ts TIMESTAMP, t2 TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_base VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:03', 30, 'b') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:01', 10, 'a') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:05', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:02', 20, 'b') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:04', 40, 'a')",

    # Table with NULL values for fill_forward test
    "CREATE TABLE t_fill (ts TIMESTAMP, val INT)",
    "INSERT INTO t_fill VALUES "
    "('2024-01-01 00:00:01', 10) ('2024-01-01 00:00:02', NULL) "
    "('2024-01-01 00:00:03', NULL) ('2024-01-01 00:00:04', 40) "
    "('2024-01-01 00:00:05', NULL)",

    # Composite primary key table (duplicate ts with different pk)
    "CREATE TABLE t_dup (ts TIMESTAMP, pk INT PRIMARY KEY, val INT, st NCHAR(10))",
    "INSERT INTO t_dup VALUES "
    "('2024-01-01 00:00:01', 1, 10, 'a') ('2024-01-01 00:00:01', 2, 15, 'a') "
    "('2024-01-01 00:00:02', 1, 20, 'b') ('2024-01-01 00:00:02', 2, 25, 'b') "
    "('2024-01-01 00:00:03', 1, 30, 'a')",

    # Composite PK table with NULLs for fill_forward test
    "CREATE TABLE t_fill_dup (ts TIMESTAMP, pk INT PRIMARY KEY, val INT)",
    "INSERT INTO t_fill_dup VALUES "
    "('2024-01-01 00:00:01', 1, 10) ('2024-01-01 00:00:01', 2, NULL) "
    "('2024-01-01 00:00:02', 1, NULL) ('2024-01-01 00:00:02', 2, 30) "
    "('2024-01-01 00:00:03', 1, 50)",

    # External window boundary table
    "CREATE TABLE t_ext_bounds (ts TIMESTAMP, endtime TIMESTAMP)",
    "INSERT INTO t_ext_bounds VALUES "
    "('2024-01-01 00:00:01', '2024-01-01 00:00:02') "
    "('2024-01-01 00:00:03', '2024-01-01 00:00:05')",

    # --- Degraded timestamp tables ---
    # Ascending event_time
    "CREATE TABLE t_event (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:01', 30, 'b') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:02', 10, 'a') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:03', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:04', 20, 'b') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:05', 40, 'a')",

    # Descending event_time
    "CREATE TABLE t_event_desc (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event_desc VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:05', 40, 'a') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:04', 20, 'b') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:03', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:02', 10, 'a') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:01', 30, 'b')",

    # Random event_time (non-monotonic)
    "CREATE TABLE t_event_rand (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event_rand VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:03', 50, 'a') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:01', 30, 'b') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:05', 40, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:02', 10, 'a') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:04', 20, 'b')",

    # Duplicate event_time
    "CREATE TABLE t_event_dup (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event_dup VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:01', 30, 'b') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:02', 10, 'a') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:02', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:03', 20, 'b') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:04', 40, 'a')",

    # NULL event_time (1 NULL)
    "CREATE TABLE t_event_null (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event_null VALUES "
    "('2024-01-01 00:00:01', NULL, 30, 'b') "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:02', 10, 'a') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:03', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:04', 20, 'b') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:05', 40, 'a')",

    # Multiple NULL event_time
    "CREATE TABLE t_event_nulls (ts TIMESTAMP, event_time TIMESTAMP, val INT, st NCHAR(10))",
    "INSERT INTO t_event_nulls VALUES "
    "('2024-01-01 00:00:01', NULL, 30, 'b') "
    "('2024-01-01 00:00:02', NULL, 10, 'a') "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:03', 50, 'a') "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:04', 20, 'b') "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:05', 40, 'a')",

    # NULL fill_forward table
    "CREATE TABLE t_event_fill (ts TIMESTAMP, event_time TIMESTAMP, val INT)",
    "INSERT INTO t_event_fill VALUES "
    "('2024-01-01 00:00:01', '2024-06-01 00:00:01', 10) "
    "('2024-01-01 00:00:02', '2024-06-01 00:00:02', NULL) "
    "('2024-01-01 00:00:03', '2024-06-01 00:00:03', NULL) "
    "('2024-01-01 00:00:04', '2024-06-01 00:00:04', 40) "
    "('2024-01-01 00:00:05', '2024-06-01 00:00:05', NULL)",
]


def _init_data():
    """Create database and all tables needed by both test cases."""
    for sql in INIT_SQLS:
        tdSql.execute(sql)


class TestTimelineFallback:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_pk_baseline(self):
        """Primary key behavior matrix: original PK behavior under all scenarios.

        Sections A-G: selection functions, elapsed, derivative/twa/irate,
        diff/csum/mavg/statecount/stateduration, lag/lead/fill_forward,
        windows, interp. Scenarios: ascending, descending, ORDER BY val
        (disorder), duplicate timestamps, ORDER BY t2.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TS-5791

        History:
            - 2026-04-29 Split from combined test; data init moved to Python
        """
        _init_data()
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_pk_baseline.in")
        expected_file = os.path.join(case_dir, "ans", "test_pk_baseline.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_pk_baseline"
        )

    def test_degraded_timeline(self):
        """Degraded timeline: functions when subquery strips primary key.

        Sections HA-HG: same function coverage as PK baseline, but using
        degraded timestamp (event_time from subquery). Scenarios: ascending,
        descending, random order, duplicate timestamps, NULL(1), NULL(2+).

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TS-5791

        History:
            - 2026-04-29 Split from combined test; data init moved to Python
        """
        _init_data()
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_degraded_timeline.in")
        expected_file = os.path.join(case_dir, "ans", "test_degraded_timeline.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_degraded_timeline"
        )
