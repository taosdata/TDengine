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
"""Test spill-to-disk for DistinctFilter operator.

Sets pqSortMemThreshold=1 (1MB) via updatecfgDict so the hash-based
DistinctFilter triggers sort-based spill with moderate data volume.

Covers global, INTERVAL, and GROUP BY DISTINCT under the spill path.

Since: v3.4.2.0
Labels: common
"""

from new_test_framework.utils import tdLog, tdSql


class TestAggDistinctSpill:

    # Lower threshold to 1MB to trigger spill with moderate data
    updatecfgDict = {"pqSortMemThreshold": 1}

    def setup_class(cls):
        tdLog.info("=== setup: insert high-cardinality data ===")
        tdSql.execute("drop database if exists test_spill")
        tdSql.execute("create database test_spill vgroups 1")
        tdSql.execute("use test_spill")
        tdSql.execute("create table t1 (ts timestamp, val int)")

        # Insert 200K rows with sequential unique values to guarantee spill
        # With 1MB threshold: each hash entry ~20 bytes → spill at ~50K entries
        batch_size = 5000
        total_rows = 200000
        base_ts = 1700000000000
        for batch_start in range(0, total_rows, batch_size):
            values = []
            for i in range(batch_start, min(batch_start + batch_size, total_rows)):
                values.append(f"({base_ts + i}, {i})")
            tdSql.execute(f"insert into t1 values " + ",".join(values))
        tdLog.info(f"Inserted {total_rows} rows with unique values")

        # --- Interval data: same distinct values repeat across windows ---
        # Window = 1s (1000ms). For each of WIN windows we insert VALS distinct
        # values, each value twice (2 rows -> 2 ms slots). The dedup key for
        # COUNT(DISTINCT) INTERVAL includes the window start, so the same value
        # in different windows must be counted once *per window*, never merged.
        # WIN * VALS composite keys (>> spill threshold) force the sort spill
        # path -- the regression target for the interval double-counting bug.
        cls.IV_WIN = 200
        cls.IV_VALS = 500
        tdSql.execute(
            "create table ti (ts timestamp, val int, wi int)")
        win_ms = 1000
        for w in range(cls.IV_WIN):
            win_base = base_ts + w * win_ms
            values = []
            for v in range(cls.IV_VALS):
                # two rows per (window,value): distinct ts within same 1s window
                values.append(f"({win_base + 2 * v}, {v}, {w})")
                values.append(f"({win_base + 2 * v + 1}, {v}, {w})")
            tdSql.execute("insert into ti values " + ",".join(values))
        tdLog.info(
            f"Inserted interval data: {cls.IV_WIN} windows x {cls.IV_VALS} vals")

        # --- Group data: same distinct values repeat across groups ---
        # Super table partitioned by tag `grp` (the supported GROUP BY form for
        # DistinctFilter: per-group dedup keyed on the block groupId). Each of
        # GP_GRP groups holds GP_VALS distinct values (each inserted twice).
        # GP_GRP * GP_VALS composite (group,value) keys (>> spill threshold)
        # force the sort spill path so group-aware dedup is exercised there.
        cls.GP_GRP = 50
        cls.GP_VALS = 1000
        tdSql.execute(
            "create table stg (ts timestamp, val int) tags (grp int)")
        row_ts = base_ts
        for g in range(cls.GP_GRP):
            tdSql.execute(f"create table tg{g} using stg tags ({g})")
            values = []
            for v in range(cls.GP_VALS):
                values.append(f"({row_ts}, {v})")
                row_ts += 1
                values.append(f"({row_ts}, {v})")
                row_ts += 1
            tdSql.execute(f"insert into tg{g} values " + ",".join(values))
        tdLog.info(
            f"Inserted group data: {cls.GP_GRP} groups x {cls.GP_VALS} vals")

    def teardown_class(cls):
        tdSql.execute("drop database if exists test_spill")

    def test_count_distinct_spill(self):
        """COUNT(DISTINCT) correctness under spill-to-disk.

        Since: v3.4.2.0
        Labels: common
        Jira: None
        """
        tdLog.info("Spill test: count(distinct val) from t1")

        # Via DistinctFilter (hash → spill → sort dedup)
        tdSql.query("select count(distinct val) from t1")
        spill_result = tdSql.getData(0, 0)

        # Reference via subquery (no DistinctFilter, uses SELECT DISTINCT)
        tdSql.query("select count(*) from (select distinct val from t1)")
        ref_result = tdSql.getData(0, 0)

        tdLog.info(f"count(distinct val): spill={spill_result}, ref={ref_result}")
        assert spill_result == ref_result, \
            f"Spill COUNT mismatch: got {spill_result}, expected {ref_result}"
        # With 200K unique values, result should be 200000
        assert spill_result == 200000, f"Expected 200000, got {spill_result}"

    def test_sum_distinct_spill(self):
        """SUM(DISTINCT) correctness under spill-to-disk.

        Since: v3.4.2.0
        Labels: common
        Jira: None
        """
        tdSql.query("select sum(distinct val) from t1")
        sum_spill = tdSql.getData(0, 0)

        tdSql.query("select sum(v) from (select distinct val as v from t1)")
        sum_ref = tdSql.getData(0, 0)

        tdLog.info(f"sum(distinct val): spill={sum_spill}, ref={sum_ref}")
        assert sum_spill == sum_ref, \
            f"Spill SUM mismatch: got {sum_spill}, expected {sum_ref}"
        # sum(0..199999) = 199999 * 200000 / 2 = 19999900000
        assert sum_spill == 19999900000, f"Expected 19999900000, got {sum_spill}"

    def test_count_distinct_interval_spill(self):
        """COUNT(DISTINCT) with INTERVAL under spill-to-disk.

        Regression for interval double-counting: the dedup key includes the
        window start, so the same distinct value appearing in different windows
        must be counted once per window. After spilling to the sort path, rows
        of the same value from different windows could be ordered non-adjacently
        and re-emitted, inflating per-window counts. Each window here has exactly
        IV_VALS distinct values, so every window count must equal IV_VALS.

        Since: v3.4.2.0
        Labels: common
        Jira: None
        """
        tdLog.info("Spill test: count(distinct val) ... interval(1s)")

        tdSql.query(
            "select _wstart, count(distinct val) from ti interval(1s) order by _wstart")
        rows = tdSql.queryResult
        assert len(rows) == self.IV_WIN, \
            f"Expected {self.IV_WIN} windows, got {len(rows)}"

        total = 0
        for wstart, cnt in rows:
            assert cnt == self.IV_VALS, \
                f"Per-window COUNT(DISTINCT) wrong at {wstart}: " \
                f"got {cnt}, expected {self.IV_VALS} (double-count regression)"
            total += cnt

        # Cross-check total against a non-DistinctFilter reference: the number
        # of distinct (window, value) pairs computed via SELECT DISTINCT.
        tdSql.query("select count(*) from (select distinct wi, val from ti)")
        ref_pairs = tdSql.getData(0, 0)
        expected = self.IV_WIN * self.IV_VALS
        tdLog.info(
            f"interval count(distinct): total={total}, ref_pairs={ref_pairs}, "
            f"expected={expected}")
        assert total == expected, \
            f"Total interval count mismatch: got {total}, expected {expected}"
        assert ref_pairs == expected, \
            f"Reference distinct-pair count mismatch: got {ref_pairs}, expected {expected}"

    def test_count_distinct_group_spill(self):
        """COUNT(DISTINCT) with GROUP BY tag under spill-to-disk.

        Super table grouped by tag `grp`: each group has exactly GP_VALS
        distinct values (values repeat across groups), so per-group dedup must
        yield GP_VALS for every group even after the hash set spills to the
        sort path.

        Since: v3.4.2.0
        Labels: common
        Jira: None
        """
        tdLog.info("Spill test: count(distinct val) ... group by grp (tag)")

        tdSql.query(
            "select grp, count(distinct val) from stg group by grp order by grp")
        rows = tdSql.queryResult
        assert len(rows) == self.GP_GRP, \
            f"Expected {self.GP_GRP} groups, got {len(rows)}"

        for grp, cnt in rows:
            assert cnt == self.GP_VALS, \
                f"Per-group COUNT(DISTINCT) wrong at grp={grp}: " \
                f"got {cnt}, expected {self.GP_VALS}"

        # Cross-check via non-DistinctFilter reference (SELECT DISTINCT path).
        tdSql.query(
            "select grp, count(*) from (select distinct grp, val from stg) "
            "group by grp order by grp")
        ref_rows = tdSql.queryResult
        assert len(ref_rows) == self.GP_GRP, \
            f"Reference group count mismatch: got {len(ref_rows)}, expected {self.GP_GRP}"
        for grp, cnt in ref_rows:
            assert cnt == self.GP_VALS, \
                f"Reference per-group distinct wrong at grp={grp}: " \
                f"got {cnt}, expected {self.GP_VALS}"
