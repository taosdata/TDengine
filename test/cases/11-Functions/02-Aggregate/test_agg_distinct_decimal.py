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
"""DISTINCT aggregates over DECIMAL columns.

DECIMAL was the one type DISTINCT sum/avg did not handle per-function: it fell
back to the non-DISTINCT implementation, so no spillable dedup set was created
and the plans without a DistinctFilter had neither a memory bound nor a dedup
guarantee.

Wiring decimal through the shared per-value helper also exposed a double-count
in the AVG accumulate loop that made AVG(DISTINCT) return exactly half the
correct average for every type. It had gone unnoticed because no test covered
DISTINCT on a decimal column, and on the plans previously exercised the
DistinctFilter deduped upstream so the doubling cancelled out.

pqSortMemThreshold is lowered to 1MB so the same cases run through both the
resident and the spilled code paths.

Since: v3.4.2.3
Labels: common
"""

from new_test_framework.utils import tdLog, tdSql


class TestAggDistinctDecimal:

    # 1MB budget: the small tables stay resident, t_big spills.
    updatecfgDict = {"pqSortMemThreshold": 1}

    # t_big holds 1..BIG_DISTINCT twice over, so dedup is observable and the
    # expected aggregates are closed-form.
    BIG_DISTINCT = 60000

    def setup_class(cls):
        tdLog.info("=== setup: decimal tables ===")
        tdSql.execute("drop database if exists test_distinct_decimal")
        tdSql.execute("create database test_distinct_decimal vgroups 2")
        tdSql.execute("use test_distinct_decimal")

        # Small resident case. Values {1.25 .. 5.25} and {1.5 .. 5.5}, each
        # inserted three times.
        #   d64  distinct = 5, sum = 16.25, avg = 3.25
        #   d128 distinct = 5, sum = 17.50, avg = 3.50
        tdSql.execute(
            "create table t_small ("
            "  ts timestamp, "
            "  d64 decimal(18,2), "
            "  d128 decimal(38,4)"
            ")"
        )
        base = 1700000000000
        rows = []
        i = 0
        for _ in range(3):
            for v in range(1, 6):
                rows.append(f"({base + i}, {v}.25, {v}.5000)")
                i += 1
        tdSql.execute("insert into t_small values " + ",".join(rows))

        # NULL handling: NULLs are not values and must not be counted.
        tdSql.execute("create table t_null (ts timestamp, d64 decimal(18,2))")
        tdSql.execute(
            "insert into t_null values "
            f"({base}, 1.00),"
            f"({base + 1}, NULL),"
            f"({base + 2}, 1.00),"
            f"({base + 3}, 2.00),"
            f"({base + 4}, NULL)"
        )

        # Large case, past the 1MB budget so the set spills and finalize replays.
        tdSql.execute(
            "create table t_big ("
            "  ts timestamp, "
            "  d64 decimal(18,2), "
            "  d128 decimal(38,4), "
            "  v int"
            ")"
        )
        batch = []
        i = 0
        for _ in range(2):
            for v in range(1, cls.BIG_DISTINCT + 1):
                batch.append(f"({base + i}, {v}.00, {v}.0000, {v})")
                i += 1
                if len(batch) == 2000:
                    tdSql.execute("insert into t_big values " + ",".join(batch))
                    batch = []
        if batch:
            tdSql.execute("insert into t_big values " + ",".join(batch))
        tdLog.info(f"Inserted {2 * cls.BIG_DISTINCT} rows into t_big")

    def teardown_class(cls):
        tdSql.execute("drop database if exists test_distinct_decimal")

    def test_decimal64_distinct_resident(self):
        """DISTINCT count/sum/avg on decimal(18,2), set stays resident

        Verifies decimal64 distinct resident.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("resident decimal(18,2) distinct aggregates")

        tdSql.query(
            "select count(distinct d64), sum(distinct d64), avg(distinct d64) "
            "from t_small"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 5, f"count(distinct d64): got {row[0]}, expected 5"
        assert float(row[1]) == 16.25, \
            f"sum(distinct d64): got {row[1]}, expected 16.25"
        # The double-count bug returned 1.625 here, exactly half.
        assert abs(float(row[2]) - 3.25) < 1e-9, \
            f"avg(distinct d64): got {row[2]}, expected 3.25"

    def test_decimal128_distinct_resident(self):
        """DISTINCT count/sum/avg on decimal(38,4), set stays resident

        Verifies decimal128 distinct resident.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("resident decimal(38,4) distinct aggregates")

        tdSql.query(
            "select count(distinct d128), sum(distinct d128), avg(distinct d128) "
            "from t_small"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 5, f"count(distinct d128): got {row[0]}, expected 5"
        assert float(row[1]) == 17.5, \
            f"sum(distinct d128): got {row[1]}, expected 17.5"
        assert abs(float(row[2]) - 3.5) < 1e-9, \
            f"avg(distinct d128): got {row[2]}, expected 3.5"

    def test_decimal_distinct_nulls_ignored(self):
        """NULL decimal values are not counted as a distinct value

        Verifies decimal distinct nulls ignored.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("decimal distinct with NULLs")

        tdSql.query(
            "select count(distinct d64), sum(distinct d64), avg(distinct d64) "
            "from t_null"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 2, f"count(distinct d64): got {row[0]}, expected 2"
        assert float(row[1]) == 3.0, \
            f"sum(distinct d64): got {row[1]}, expected 3.0"
        assert abs(float(row[2]) - 1.5) < 1e-9, \
            f"avg(distinct d64): got {row[2]}, expected 1.5"

    def test_decimal64_distinct_spilled(self):
        """DISTINCT on decimal(18,2) past the memory budget, forcing spill

        Verifies decimal64 distinct spilled.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("spilled decimal(18,2) distinct aggregates")

        n = self.BIG_DISTINCT
        expected_sum = n * (n + 1) / 2      # 1800030000
        expected_avg = (n + 1) / 2          # 30000.5

        tdSql.query(
            "select count(distinct d64), sum(distinct d64), avg(distinct d64) "
            "from t_big"
        )
        row = tdSql.queryResult[0]
        assert row[0] == n, f"count(distinct d64): got {row[0]}, expected {n}"
        assert float(row[1]) == expected_sum, \
            f"sum(distinct d64): got {row[1]}, expected {expected_sum}"
        assert abs(float(row[2]) - expected_avg) < 1e-6, \
            f"avg(distinct d64): got {row[2]}, expected {expected_avg}"

    def test_decimal128_distinct_spilled(self):
        """DISTINCT on decimal(38,4) past the memory budget, forcing spill

        Verifies decimal128 distinct spilled.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("spilled decimal(38,4) distinct aggregates")

        n = self.BIG_DISTINCT
        expected_sum = n * (n + 1) / 2
        expected_avg = (n + 1) / 2

        tdSql.query(
            "select count(distinct d128), sum(distinct d128), avg(distinct d128) "
            "from t_big"
        )
        row = tdSql.queryResult[0]
        assert row[0] == n, f"count(distinct d128): got {row[0]}, expected {n}"
        assert float(row[1]) == expected_sum, \
            f"sum(distinct d128): got {row[1]}, expected {expected_sum}"
        assert abs(float(row[2]) - expected_avg) < 1e-6, \
            f"avg(distinct d128): got {row[2]}, expected {expected_avg}"

    def test_decimal_matches_int_on_same_values(self):
        """Decimal and int DISTINCT agree when they hold the same values

        Verifies decimal matches int on same values.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("cross-check decimal against int over identical values")

        # t_big.d64, d128 and v all carry 1..BIG_DISTINCT, so the three columns
        # must produce the same aggregates. This pins decimal to the
        # independently-exercised integer path rather than to a literal alone.
        tdSql.query(
            "select count(distinct v), sum(distinct v), avg(distinct v), "
            "       count(distinct d64), sum(distinct d64), avg(distinct d64), "
            "       count(distinct d128), sum(distinct d128), avg(distinct d128) "
            "from t_big"
        )
        r = tdSql.queryResult[0]
        assert r[0] == r[3] == r[6], \
            f"count mismatch: int={r[0]}, d64={r[3]}, d128={r[6]}"
        assert float(r[1]) == float(r[4]) == float(r[7]), \
            f"sum mismatch: int={r[1]}, d64={r[4]}, d128={r[7]}"
        assert abs(float(r[2]) - float(r[5])) < 1e-6, \
            f"avg mismatch: int={r[2]}, d64={r[5]}"
        assert abs(float(r[2]) - float(r[8])) < 1e-6, \
            f"avg mismatch: int={r[2]}, d128={r[8]}"

    def test_decimal_distinct_mixed_with_plain_agg(self):
        """DISTINCT decimal beside a plain aggregate

        Verifies decimal distinct mixed with plain agg.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("count(*) alongside decimal distinct aggregates")

        # This is a plan shape where the DistinctFilter is not inserted, so the
        # per-function set is the only dedup -- exactly the case decimal used to
        # fall back out of.
        tdSql.query(
            "select count(*), count(distinct d64), sum(distinct d64) from t_small"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 15, f"count(*): got {row[0]}, expected 15"
        assert row[1] == 5, f"count(distinct d64): got {row[1]}, expected 5"
        assert float(row[2]) == 16.25, \
            f"sum(distinct d64): got {row[2]}, expected 16.25"

    def test_plain_decimal_agg_not_deduplicated(self):
        """Non-DISTINCT decimal sum/avg must NOT dedup

        Verifies plain decimal agg not deduplicated.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("plain decimal sum/avg over duplicated values")

        # Regression guard. DISTINCT was detected by inferring from the result buffer size, and
        # sizeof(SDecimalSumRes) == sizeof(SSumRes) + sizeof(SDistinctValueSet) exactly (40 == 32+8),
        # with the AVG pair colliding too (72 >= 56+8). A plain SUM/AVG over a decimal column was
        # therefore mistaken for a DISTINCT variant and silently deduplicated, returning a
        # too-small sum. Detection now reads isDistinct from the plan.
        #
        # Every other case in this suite uses DISTINCT, so none of them can catch this: the defect
        # only appears when DISTINCT is absent.
        tdSql.execute("create table t_dup (ts timestamp, d64 decimal(18,2), d128 decimal(38,4))")
        base = 1700000000000
        # {2, 2, 2, 4}: plain sum = 10 / avg = 2.5; deduped would be sum = 6 / avg = 3.
        tdSql.execute(
            "insert into t_dup values "
            f"({base}, 2.00, 2.0000),"
            f"({base + 1}, 2.00, 2.0000),"
            f"({base + 2}, 2.00, 2.0000),"
            f"({base + 3}, 4.00, 4.0000)"
        )

        tdSql.query(
            "select count(*), sum(d64), avg(d64), sum(d128), avg(d128) from t_dup"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 4, f"count(*): got {row[0]}, expected 4"
        assert float(row[1]) == 10.0, \
            f"sum(d64) deduplicated: got {row[1]}, expected 10.0"
        assert abs(float(row[2]) - 2.5) < 1e-9, \
            f"avg(d64) deduplicated: got {row[2]}, expected 2.5"
        assert float(row[3]) == 10.0, \
            f"sum(d128) deduplicated: got {row[3]}, expected 10.0"
        assert abs(float(row[4]) - 2.5) < 1e-9, \
            f"avg(d128) deduplicated: got {row[4]}, expected 2.5"

        # And DISTINCT on the same data still dedups, so the fix did not simply disable it.
        tdSql.query(
            "select count(distinct d64), sum(distinct d64), avg(distinct d64) from t_dup"
        )
        row = tdSql.queryResult[0]
        assert row[0] == 2, f"count(distinct d64): got {row[0]}, expected 2"
        assert float(row[1]) == 6.0, \
            f"sum(distinct d64): got {row[1]}, expected 6.0"
        assert abs(float(row[2]) - 3.0) < 1e-9, \
            f"avg(distinct d64): got {row[2]}, expected 3.0"

    def test_decimal_distinct_group_by(self):
        """Per-group decimal DISTINCT keeps groups independent

        Verifies decimal distinct group by.

        Catalog:
            - Function:Aggregate:Distinct

        Since: v3.4.2.3

        Labels: common, ci

        Jira: None

        History:
            - 2026-09-01 Jin Minglei Created
        """
        tdLog.info("decimal distinct grouped by a tag")

        tdSql.execute(
            "create stable s_dec (ts timestamp, d64 decimal(18,2)) tags (g int)"
        )
        base = 1700000000000
        # Both children hold {1.00, 2.00}, so each group must report 2 -- not 4,
        # and not 2 for one group and 0 for the other.
        for g in (1, 2):
            tdSql.execute(f"create table s_dec_{g} using s_dec tags ({g})")
            tdSql.execute(
                f"insert into s_dec_{g} values "
                f"({base}, 1.00),({base + 1}, 1.00),"
                f"({base + 2}, 2.00),({base + 3}, 2.00)"
            )

        tdSql.query(
            "select g, count(distinct d64), sum(distinct d64) "
            "from s_dec group by g order by g"
        )
        rows = tdSql.queryResult
        assert len(rows) == 2, f"Expected 2 groups, got {len(rows)}"
        for g, cnt, total in rows:
            assert cnt == 2, f"group {g}: count(distinct)={cnt}, expected 2"
            assert float(total) == 3.0, \
                f"group {g}: sum(distinct)={total}, expected 3.0"
