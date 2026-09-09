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
"""COUNT(DISTINCT) combined with PARTITION BY, windows, and plain aggregates.

Regression coverage for a customer report where

    select _wstart, count(*), count(distinct vin)
    from alarm_r1 partition by tbname interval(1d)

returned count(distinct vin) equal to count(*), and every partition collapsed
into a single output row. Three separate defects were involved:

  * mixing DISTINCT with plain aggregates in a window -- the single-child
    DistinctFilter removed the duplicate rows count(*) still had to see, so the
    optimizer skipped itself and dropped DISTINCT entirely
  * DISTINCT on two different columns -- the filter keys on one column only
  * PARTITION BY + window -- the sort path reports the *block* group id, so all
    partitions reported the same id and merged together

Scale mirrors the reported deployment: 10 vgroups, 100 child tables, 10 rows
per table, values repeating so per-partition dedup is observable.

Since: v3.4.2.3
Labels: common
"""

from new_test_framework.utils import tdLog, tdSql


class TestAggDistinctPartition:

    NUM_TABLES = 100
    ROWS_PER_TABLE = 10
    NUM_VINS = 3

    def setup_class(cls):
        tdLog.info("=== setup: 10 vgroups, 100 tables, 10 rows each ===")
        tdSql.execute("drop database if exists test_distinct_part")
        tdSql.execute("create database test_distinct_part vgroups 10")
        tdSql.execute("use test_distinct_part")
        tdSql.execute(
            "create stable alarm_r1 (ts timestamp, vin varchar(32), x int) "
            "tags (dev int)"
        )

        # All rows land in one 1d window so each partition yields exactly one
        # output row: count(*) = ROWS_PER_TABLE, count(distinct vin) = NUM_VINS.
        for n in range(cls.NUM_TABLES):
            tdSql.execute(f"create table t{n} using alarm_r1 tags ({n})")
            values = []
            for i in range(cls.ROWS_PER_TABLE):
                values.append(
                    f"('2026-03-16 {i:02d}:00:00', 'VIN{i % cls.NUM_VINS}', {i})"
                )
            tdSql.execute(f"insert into t{n} values " + ",".join(values))

        # Small multi-day table with hand-checkable per-window distinct counts:
        # 2026-04-01 -> vin {A, B} = 2, x {1, 2, 3}; 2026-04-02 -> vin {C} = 1.
        tdSql.execute("create table m1 using alarm_r1 tags (9001)")
        tdSql.execute(
            "insert into m1 values "
            "('2026-04-01 01:00:00', 'A', 1),"
            "('2026-04-01 02:00:00', 'A', 2),"
            "('2026-04-01 03:00:00', 'B', 3),"
            "('2026-04-02 01:00:00', 'C', 4),"
            "('2026-04-02 02:00:00', 'C', 5)"
        )
        tdLog.info(
            f"Inserted {cls.NUM_TABLES} tables x {cls.ROWS_PER_TABLE} rows + m1"
        )

    def teardown_class(cls):
        tdSql.execute("drop database if exists test_distinct_part")

    def test_count_distinct_partition_by_interval(self):
        """COUNT(DISTINCT) with PARTITION BY tbname + INTERVAL

        Verifies count distinct partition by interval.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("partition by tbname + interval(1d) + count(distinct)")

        tdSql.query(
            "select _wstart, count(distinct vin) from alarm_r1 "
            "where tbname like 't%' partition by tbname interval(1d)"
        )
        rows = tdSql.queryResult
        # The bug collapsed every partition into a single output row.
        assert len(rows) == self.NUM_TABLES, \
            f"Expected {self.NUM_TABLES} partitions, got {len(rows)}"
        for ws, distinct_cnt in rows:
            assert distinct_cnt == self.NUM_VINS, \
                f"count(distinct vin) wrong at {ws}: got {distinct_cnt}, " \
                f"expected {self.NUM_VINS}"

    def test_count_distinct_mixed_with_plain_agg(self):
        """COUNT(*) mixed with COUNT(DISTINCT) in a window

        Verifies count distinct mixed with plain agg.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("count(*) alongside count(distinct) under interval(1d)")

        # The bug returned count(*) for both columns: the DistinctFilter fed
        # deduplicated rows to count(*), or DISTINCT was dropped outright.
        tdSql.query(
            "select _wstart, count(*), count(distinct vin) from alarm_r1 "
            "where tbname like 't%' interval(1d)"
        )
        rows = tdSql.queryResult
        assert len(rows) == 1, f"Expected 1 window, got {len(rows)}"
        total = self.NUM_TABLES * self.ROWS_PER_TABLE
        assert rows[0][1] == total, \
            f"count(*) wrong: got {rows[0][1]}, expected {total}"
        assert rows[0][2] == self.NUM_VINS, \
            f"count(distinct vin) wrong: got {rows[0][2]}, " \
            f"expected {self.NUM_VINS}"

    def test_count_distinct_customer_query(self):
        """The reported query: count(*) + count(distinct) + partition + interval

        Verifies count distinct customer query.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("customer query: all three features combined")

        tdSql.query(
            "select _wstart, count(*), count(distinct vin) from alarm_r1 "
            "where tbname like 't%' partition by tbname interval(1d)"
        )
        rows = tdSql.queryResult
        assert len(rows) == self.NUM_TABLES, \
            f"Expected {self.NUM_TABLES} partitions, got {len(rows)}"
        for ws, total, distinct_cnt in rows:
            assert total == self.ROWS_PER_TABLE, \
                f"count(*) wrong at {ws}: got {total}, " \
                f"expected {self.ROWS_PER_TABLE}"
            assert distinct_cnt == self.NUM_VINS, \
                f"count(distinct vin) wrong at {ws}: got {distinct_cnt}, " \
                f"expected {self.NUM_VINS}"

    def test_count_distinct_multi_column(self):
        """Two DISTINCT aggregates on different columns

        Verifies count distinct multi column.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("count(distinct vin) alongside sum(distinct x)")

        # One shared dedup key cannot serve two columns: the bug returned 5
        # (rows surviving an x-keyed dedup) instead of 3 distinct vin values.
        tdSql.query(
            "select count(distinct vin), sum(distinct x) from alarm_r1 "
            "where tbname = 'm1'"
        )
        rows = tdSql.queryResult
        assert rows[0][0] == 3, \
            f"count(distinct vin) wrong: got {rows[0][0]}, expected 3"
        assert rows[0][1] == 15, \
            f"sum(distinct x) wrong: got {rows[0][1]}, expected 15"

    def test_count_distinct_partition_multi_window(self):
        """Per-partition dedup stays independent per window

        Verifies count distinct partition multi window.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("multi-day partitioned windows on m1")

        tdSql.query(
            "select _wstart, count(distinct vin) from alarm_r1 "
            "where tbname = 'm1' partition by tbname interval(1d) "
            "order by _wstart"
        )
        rows = tdSql.queryResult
        assert len(rows) == 2, f"Expected 2 windows, got {len(rows)}"
        assert rows[0][1] == 2, \
            f"2026-04-01 should have 2 distinct vin, got {rows[0][1]}"
        assert rows[1][1] == 1, \
            f"2026-04-02 should have 1 distinct vin, got {rows[1][1]}"

    def test_count_distinct_interval_no_partition(self):
        """COUNT(DISTINCT) with INTERVAL only — the path that already worked

        Verifies count distinct interval no partition.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("interval(1d) + count(distinct), no partition by")

        tdSql.query(
            "select _wstart, count(distinct vin) from alarm_r1 "
            "where tbname like 't%' interval(1d)"
        )
        rows = tdSql.queryResult
        assert len(rows) == 1, f"Expected 1 window, got {len(rows)}"
        assert rows[0][1] == self.NUM_VINS, \
            f"count(distinct vin) wrong: got {rows[0][1]}, " \
            f"expected {self.NUM_VINS}"

    def test_count_distinct_partition_is_stable(self):
        """Repeated execution returns identical rows

        Verifies count distinct partition is stable.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("repeat partitioned distinct query, results must be stable")

        # The customer also reported timestamps changing between runs.
        #
        # Every row of every partition falls in the same 1d window, so all 100 output rows share
        # one _wstart and "order by _wstart" alone leaves the order among partitions undefined.
        # Select tbname and order by it too, giving a total order -- otherwise a legitimate change
        # in partition emission order would fail this test while a real instability could pass.
        sql = (
            "select tbname, _wstart, count(*), count(distinct vin) from alarm_r1 "
            "where tbname like 't%' partition by tbname interval(1d) "
            "order by tbname, _wstart"
        )
        tdSql.query(sql)
        baseline = list(tdSql.queryResult)
        assert len(baseline) == self.NUM_TABLES

        for attempt in range(5):
            tdSql.query(sql)
            current = list(tdSql.queryResult)
            # Compare the row count first. Reporting only the first differing row would raise
            # StopIteration when the lists differ solely in length and the common prefix matches,
            # hiding the actual row-count change behind an unrelated error.
            assert len(current) == len(baseline), \
                f"Row count changed on attempt {attempt + 1}: " \
                f"got {len(current)}, expected {len(baseline)}"
            diff = next(
                ((i, a, b) for i, (a, b) in enumerate(zip(baseline, current)) if a != b), None
            )
            assert diff is None, \
                f"Result changed on attempt {attempt + 1} at row {diff[0]}: " \
                f"expected {diff[1]}, got {diff[2]}"
