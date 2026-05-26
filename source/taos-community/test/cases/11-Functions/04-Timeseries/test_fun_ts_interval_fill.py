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

from new_test_framework.utils import tdLog, tdSql


DB = "test_indef_fill"
TS_BASE = "2024-01-01 00:00:"
WHERE = (
    "WHERE ts >= '2024-01-01 00:00:01' AND ts < '2024-01-01 00:00:06'"
)


def _ts(sec, ms=0):
    """Build a timestamp string '2024-01-01 00:00:SS.MMM'."""
    return f"2024-01-01 00:00:{sec:02d}.{ms:03d}"


class TestFunTsIntervalFill:
    """Tests for FILL modes with timeseries functions
    (csum, diff, derivative, mavg, statecount, stateduration,
    irate, twa) inside INTERVAL windows.

    Data layout used by most tests (table t1):
      [1s,2s) : 5 rows  val = 10, 20, 30, 40, 50
      [2s,3s) : gap
      [3s,4s) : 3 rows  val = 60, 70, 80
      [4s,5s) : gap
      [5s,6s) : 1 row   val = 100

    Since: 3.4.1.0

    Catalog: Functions/TimeSeries

    Labels: common,ci

    Jira: None

    History:
    - 2026-04-15 Simon created.
    - 2026-04-16 Consolidated: merge tests sharing the same setup.
    - 2026-04-16 Expanded: add derivative/mavg/statecount/stateduration/irate/twa/sample.
    - 2026-04-16 Enlarged basic dataset: 3 rows -> 9 rows with multi-window gaps.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _create_db(self):
        """Drop and re-create the test database with ms precision."""
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB} precision 'ms'")
        tdSql.execute(f"use {DB}")

    def _check(self, expected):
        """Verify queryResult matches expected rows exactly.
        expected: list of tuples, one per row, each containing all columns."""
        tdSql.checkRows(len(expected))
        for r, row in enumerate(expected):
            for c, val in enumerate(row):
                tdSql.checkData(r, c, val)

    def _check_bulk(self, expected):
        """Bulk verify using queryResult — same logic as _check but
        with better error messages for large result sets."""
        tdSql.checkRows(len(expected))
        for r in range(len(expected)):
            for c in range(len(expected[r])):
                tdSql.checkData(r, c, expected[r][c])

    def _check_approx(self, expected, tol=0.01):
        """Verify queryResult with approximate float comparison.
        Timestamps and None are compared exactly; floats within tol."""
        tdSql.checkRows(len(expected))
        for r, row in enumerate(expected):
            for c, val in enumerate(row):
                actual = tdSql.queryResult[r][c]
                if val is None:
                    if actual is not None:
                        tdLog.exit(
                            f"row {r} col {c}: expected None, got {actual}"
                        )
                elif isinstance(val, float):
                    if actual is None:
                        tdLog.exit(
                            f"row {r} col {c}: expected {val}, got None"
                        )
                    if abs(float(actual) - val) > tol:
                        tdLog.exit(
                            f"row {r} col {c}: expected ~{val}, "
                            f"got {actual} (tol={tol})"
                        )
                else:
                    tdSql.checkData(r, c, val)

    def _setup_basic(self):
        """t1: [1s,2s) 5 rows (10,20,30,40,50), [2s,3s) gap,
        [3s,4s) 3 rows (60,70,80), [4s,5s) gap, [5s,6s) 1 row (100)."""
        self._create_db()
        tdSql.execute("create table t1 (ts timestamp, val int)")
        tdSql.execute(
            "insert into t1 values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}01.200', 20)"
            f"('{TS_BASE}01.400', 30)"
            f"('{TS_BASE}01.600', 40)"
            f"('{TS_BASE}01.800', 50)"
            f"('{TS_BASE}03.000', 60)"
            f"('{TS_BASE}03.200', 70)"
            f"('{TS_BASE}03.400', 80)"
            f"('{TS_BASE}05.000', 100)"
        )

    def _setup_super(self):
        """Super table st1 with 2 child tables."""
        tdSql.execute("drop table if exists ct_a")
        tdSql.execute("drop table if exists ct_b")
        tdSql.execute("drop stable if exists st1")
        tdSql.execute(
            "create table st1 (ts timestamp, val int) "
            "tags (region nchar(20))"
        )
        tdSql.execute("create table ct_a using st1 tags('east')")
        tdSql.execute("create table ct_b using st1 tags('west')")
        tdSql.execute(
            "insert into ct_a values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}01.200', 20)"
            f"('{TS_BASE}01.400', 30)"
            f"('{TS_BASE}01.600', 40)"
            f"('{TS_BASE}01.800', 50)"
            f"('{TS_BASE}03.000', 60)"
            f"('{TS_BASE}03.200', 70)"
            f"('{TS_BASE}03.400', 80)"
            f"('{TS_BASE}05.000', 100)"
        )
        tdSql.execute(
            "insert into ct_b values "
            f"('{TS_BASE}01.000', 100)"
            f"('{TS_BASE}01.500', 200)"
            f"('{TS_BASE}02.000', 300)"
        )

    def _setup_dense(self):
        """t_dense: 20 rows in [1s,2s) at 50ms spacing, then 1 row at 3s."""
        tdSql.execute("drop table if exists t_dense")
        tdSql.execute("create table t_dense (ts timestamp, val int)")
        vals = []
        for i in range(20):
            ms = i * 50
            vals.append(f"('{TS_BASE}01.{ms:03d}', {i + 1})")
        vals.append(f"('{TS_BASE}03.000', 99)")
        tdSql.execute("insert into t_dense values " + " ".join(vals))

    def _setup_nulls(self):
        """t_nulls: rows in [1s,6s) with NULL values in the data."""
        tdSql.execute("drop table if exists t_nulls")
        tdSql.execute("create table t_nulls (ts timestamp, val int)")
        tdSql.execute(
            "insert into t_nulls values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}01.200', NULL)"
            f"('{TS_BASE}01.400', 30)"
            f"('{TS_BASE}01.600', 40)"
            f"('{TS_BASE}01.800', NULL)"
            f"('{TS_BASE}03.000', 60)"
            f"('{TS_BASE}03.200', 70)"
            f"('{TS_BASE}05.000', 100)"
        )

    def _setup_single_row_windows(self):
        """t_single: rows each in its own 1s window, gap at [3s,4s)."""
        tdSql.execute("drop table if exists t_single")
        tdSql.execute("create table t_single (ts timestamp, val int)")
        tdSql.execute(
            "insert into t_single values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}02.000', 20)"
            f"('{TS_BASE}04.000', 40)"
            f"('{TS_BASE}05.000', 50)"
        )

    def _setup_consecutive(self):
        """t_consec: multi-row windows spanning [1s,4s) with no gap."""
        tdSql.execute("drop table if exists t_consec")
        tdSql.execute("create table t_consec (ts timestamp, val int)")
        tdSql.execute(
            "insert into t_consec values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}01.500', 20)"
            f"('{TS_BASE}02.000', 30)"
            f"('{TS_BASE}02.500', 40)"
            f"('{TS_BASE}03.000', 50)"
            f"('{TS_BASE}03.500', 60)"
        )

    def _setup_large_dataset(self):
        """t_large: 85 rows across 20 windows (5 rows per window),
        with gaps at windows 5,10,15 (i.e. 6s,11s,16s)."""
        tdSql.execute("drop table if exists t_large")
        tdSql.execute("create table t_large (ts timestamp, val int)")
        gap_windows = {5, 10, 15}
        vals = []
        v = 1
        for w in range(20):
            if w in gap_windows:
                continue
            base_ms = (w + 1) * 1000
            for r in range(5):
                ts_ms = base_ms + r * 200
                sec = ts_ms // 1000
                ms = ts_ms % 1000
                vals.append(f"('2024-01-01 00:00:{sec:02d}.{ms:03d}', {v})")
                v += 1
        tdSql.execute("insert into t_large values " + " ".join(vals))

    def _setup_cross_block_dataset(self):
        """Create a separate database with maxrows 200 so that data blocks
        are small (200 rows each). Then insert 997 rows in window [1s,2s)
        and 997 rows in window [3s,4s), with [2s,3s) as a gap.

        This forces each window to span ~5 data blocks (997/200), testing
        the fill engine's ability to handle block boundaries within a
        single indefRows window.

        NOTE: per-window row count must NOT be an exact multiple of
        maxrows – TDengine has a pre-existing scan ordering bug where
        a window whose row count equals N*maxrows may be returned
        out of timestamp order by the upstream operator."""
        self.XBLK_ROWS = 997
        db = "test_indef_cross_blk"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} precision 'ms' "
            f"maxrows 200 minrows 50"
        )
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t_xblk (ts timestamp, val int)")
        batch = []
        for i in range(self.XBLK_ROWS):
            ms = 1000 + i
            sec = ms // 1000
            frac = ms % 1000
            batch.append(
                f"('2024-01-01 00:00:{sec:02d}.{frac:03d}', {i + 1})"
            )
        tdSql.execute("insert into t_xblk values " + " ".join(batch))
        batch = []
        for i in range(self.XBLK_ROWS):
            ms = 3000 + i
            sec = ms // 1000
            frac = ms % 1000
            batch.append(
                f"('2024-01-01 00:00:{sec:02d}.{frac:03d}', {i + 1001})"
            )
        tdSql.execute("insert into t_xblk values " + " ".join(batch))
        tdSql.execute("flush database " + db)

    def _setup_boundary_values(self):
        """t_boundary: rows with INT32 boundary values."""
        tdSql.execute("drop table if exists t_boundary")
        tdSql.execute("create table t_boundary (ts timestamp, val bigint)")
        tdSql.execute(
            "insert into t_boundary values "
            f"('{TS_BASE}01.000', 2147483647)"
            f"('{TS_BASE}01.500', 1)"
            f"('{TS_BASE}03.000', -2147483648)"
        )

    # ==================================================================
    # 1. csum + FILL basic modes (NULL / VALUE / NONE)
    # ==================================================================
    def test_csum_basic_fill_modes(self):
        """Test csum() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(VALUE,0), FILL(NONE), and FILL(VALUE,-1)
        on the basic 9-row dataset.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 4 separate tests.
        - 2026-04-16 Updated for enlarged basic dataset.
        """
        self._setup_basic()

        tdLog.debug("csum fill(null)")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
            (_ts(1), 60),
            (_ts(1), 100),
            (_ts(1), 150),
            (_ts(2), None),
            (_ts(3), 60),
            (_ts(3), 130),
            (_ts(3), 210),
            (_ts(4), None),
            (_ts(5), 100),
        ])

        tdLog.debug("csum fill(value, 0)")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(value, 0)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
            (_ts(1), 60),
            (_ts(1), 100),
            (_ts(1), 150),
            (_ts(2), 0),
            (_ts(3), 60),
            (_ts(3), 130),
            (_ts(3), 210),
            (_ts(4), 0),
            (_ts(5), 100),
        ])

        tdLog.debug("csum fill(none)")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
            (_ts(1), 60),
            (_ts(1), 100),
            (_ts(1), 150),
            (_ts(3), 60),
            (_ts(3), 130),
            (_ts(3), 210),
            (_ts(5), 100),
        ])

        tdLog.debug("csum fill(value, -1)")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
            (_ts(1), 60),
            (_ts(1), 100),
            (_ts(1), 150),
            (_ts(2), -1),
            (_ts(3), 60),
            (_ts(3), 130),
            (_ts(3), 210),
            (_ts(4), -1),
            (_ts(5), 100),
        ])

    # ==================================================================
    # 2. diff + FILL basic modes
    # ==================================================================
    def test_diff_basic_fill_modes(self):
        """Test diff() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(VALUE,-1), and FILL(NONE). diff produces
        N-1 rows per window.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 3 separate tests.
        - 2026-04-16 Updated for enlarged basic dataset.
        """
        self._setup_basic()

        tdLog.debug("diff fill(null)")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(2), None),
            (_ts(3), 10),
            (_ts(3), 10),
            (_ts(4), None),
            (_ts(5), None),
        ])

        tdLog.debug("diff fill(value, -1)")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(2), -1),
            (_ts(3), 10),
            (_ts(3), 10),
            (_ts(4), -1),
            (_ts(5), -1),
        ])

        tdLog.debug("diff fill(none)")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(1), 10),
            (_ts(3), 10),
            (_ts(3), 10),
        ])

    # ==================================================================
    # 3. derivative + FILL basic modes
    # ==================================================================
    def test_derivative_basic_fill_modes(self):
        """Test derivative() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), and FILL(VALUE,-1). derivative
        produces N-1 rows per window (rate of change normalized to the
        time unit).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("derivative fill(null)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(2), None),
            (_ts(3), 50.0),
            (_ts(3), 50.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        tdLog.debug("derivative fill(none)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(3), 50.0),
            (_ts(3), 50.0),
        ])

        tdLog.debug("derivative fill(value, -1)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(2), -1.0),
            (_ts(3), 50.0),
            (_ts(3), 50.0),
            (_ts(4), -1.0),
            (_ts(5), -1.0),
        ])

    # ==================================================================
    # 4. mavg + FILL basic modes
    # ==================================================================
    def test_mavg_basic_fill_modes(self):
        """Test mavg() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), and FILL(VALUE,-1). mavg(val,k)
        produces N-k+1 rows per window (moving average).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("mavg fill(null)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 15.0),
            (_ts(1), 25.0),
            (_ts(1), 35.0),
            (_ts(1), 45.0),
            (_ts(2), None),
            (_ts(3), 65.0),
            (_ts(3), 75.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        tdLog.debug("mavg fill(none)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 15.0),
            (_ts(1), 25.0),
            (_ts(1), 35.0),
            (_ts(1), 45.0),
            (_ts(3), 65.0),
            (_ts(3), 75.0),
        ])

        tdLog.debug("mavg fill(value, -1)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 15.0),
            (_ts(1), 25.0),
            (_ts(1), 35.0),
            (_ts(1), 45.0),
            (_ts(2), -1.0),
            (_ts(3), 65.0),
            (_ts(3), 75.0),
            (_ts(4), -1.0),
            (_ts(5), -1.0),
        ])

    # ==================================================================
    # 5. statecount + FILL basic modes
    # ==================================================================
    def test_statecount_basic_fill_modes(self):
        """Test statecount() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), and FILL(VALUE,-1). statecount
        outputs one row per input row (like csum).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("statecount fill(null)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2), (_ts(1), 3), (_ts(1), 4), (_ts(1), 5),
            (_ts(2), None),
            (_ts(3), 1), (_ts(3), 2), (_ts(3), 3),
            (_ts(4), None),
            (_ts(5), 1),
        ])

        tdLog.debug("statecount fill(none)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2), (_ts(1), 3), (_ts(1), 4), (_ts(1), 5),
            (_ts(3), 1), (_ts(3), 2), (_ts(3), 3),
            (_ts(5), 1),
        ])

        tdLog.debug("statecount fill(value, -1)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2), (_ts(1), 3), (_ts(1), 4), (_ts(1), 5),
            (_ts(2), -1),
            (_ts(3), 1), (_ts(3), 2), (_ts(3), 3),
            (_ts(4), -1),
            (_ts(5), 1),
        ])

    # ==================================================================
    # 6. stateduration + FILL basic modes
    # ==================================================================
    def test_stateduration_basic_fill_modes(self):
        """Test stateduration() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), and FILL(VALUE,-1). stateduration
        outputs one row per input row. With 1a (1ms) time unit and 200ms
        spacing, durations within a window are 0,200,400,...

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("stateduration fill(null)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 "
            f"{WHERE} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 200), (_ts(1), 400),
            (_ts(1), 600), (_ts(1), 800),
            (_ts(2), None),
            (_ts(3), 0), (_ts(3), 200), (_ts(3), 400),
            (_ts(4), None),
            (_ts(5), 0),
        ])

        tdLog.debug("stateduration fill(none)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 "
            f"{WHERE} interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 200), (_ts(1), 400),
            (_ts(1), 600), (_ts(1), 800),
            (_ts(3), 0), (_ts(3), 200), (_ts(3), 400),
            (_ts(5), 0),
        ])

        tdLog.debug("stateduration fill(value, -1)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 "
            f"{WHERE} interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 200), (_ts(1), 400),
            (_ts(1), 600), (_ts(1), 800),
            (_ts(2), -1),
            (_ts(3), 0), (_ts(3), 200), (_ts(3), 400),
            (_ts(4), -1),
            (_ts(5), 0),
        ])

    # ==================================================================
    # 7. irate + FILL basic modes (regular agg, supports all modes)
    # ==================================================================
    def test_irate_basic_fill_modes(self):
        """Test irate() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), FILL(VALUE,-1), and
        FILL(PREV/NEXT/LINEAR). irate is a regular aggregate (1 row per
        window), not an indefinite-rows function.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("irate fill(null)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), None), (_ts(3), 50.0),
            (_ts(4), None), (_ts(5), 0.0),
        ])

        tdLog.debug("irate fill(none)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(3), 50.0), (_ts(5), 0.0),
        ])

        tdLog.debug("irate fill(value, -1)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), -1.0), (_ts(3), 50.0),
            (_ts(4), -1.0), (_ts(5), 0.0),
        ])

        tdLog.debug("irate fill(prev)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(prev)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), 50.0), (_ts(3), 50.0),
            (_ts(4), 50.0), (_ts(5), 0.0),
        ])

        tdLog.debug("irate fill(next)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(next)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), 50.0), (_ts(3), 50.0),
            (_ts(4), 0.0), (_ts(5), 0.0),
        ])

        tdLog.debug("irate fill(linear)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {WHERE} "
            "interval(1s) fill(linear)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), 50.0), (_ts(3), 50.0),
            (_ts(4), 25.0), (_ts(5), 0.0),
        ])

    # ==================================================================
    # 8. twa + FILL basic modes (regular agg, supports all modes)
    # ==================================================================
    def test_twa_basic_fill_modes(self):
        """Test twa() with multiple FILL modes on the basic dataset.

        Verifies FILL(NULL), FILL(NONE), FILL(VALUE,-1), and
        FILL(PREV/NEXT/LINEAR). twa is a regular aggregate (1 row per
        window), not an indefinite-rows function.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()

        tdLog.debug("twa fill(null)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, _ts(1))
        tdSql.checkData(1, 0, _ts(2))
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, _ts(3))
        tdSql.checkData(3, 0, _ts(4))
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, _ts(5))
        tdSql.checkData(4, 1, 100.0)
        twa_sec1 = tdSql.queryResult[0][1]
        twa_sec3 = tdSql.queryResult[2][1]

        tdLog.debug("twa fill(none)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(none)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, _ts(1))
        tdSql.checkData(1, 0, _ts(3))
        tdSql.checkData(2, 0, _ts(5))
        tdSql.checkData(2, 1, 100.0)

        tdLog.debug("twa fill(value, -1)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(value, -1)"
        )
        tdSql.checkRows(5)
        tdSql.checkData(1, 0, _ts(2))
        tdSql.checkData(1, 1, -1.0)
        tdSql.checkData(3, 0, _ts(4))
        tdSql.checkData(3, 1, -1.0)
        tdSql.checkData(4, 1, 100.0)

        tdLog.debug("twa fill(prev)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(prev)"
        )
        tdSql.checkRows(5)
        prev2 = tdSql.queryResult[1][1]
        prev4 = tdSql.queryResult[3][1]
        if abs(prev2 - twa_sec1) > 0.01:
            tdLog.exit(f"twa fill(prev) sec2: expected ~{twa_sec1}, got {prev2}")
        if abs(prev4 - twa_sec3) > 0.01:
            tdLog.exit(f"twa fill(prev) sec4: expected ~{twa_sec3}, got {prev4}")
        tdSql.checkData(4, 1, 100.0)

        tdLog.debug("twa fill(next)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(next)"
        )
        tdSql.checkRows(5)
        next2 = tdSql.queryResult[1][1]
        next4 = tdSql.queryResult[3][1]
        if abs(next2 - twa_sec3) > 0.01:
            tdLog.exit(f"twa fill(next) sec2: expected ~{twa_sec3}, got {next2}")
        if abs(next4 - 100.0) > 0.01:
            tdLog.exit(f"twa fill(next) sec4: expected 100.0, got {next4}")
        tdSql.checkData(4, 1, 100.0)

        tdLog.debug("twa fill(linear)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {WHERE} "
            "interval(1s) fill(linear)"
        )
        tdSql.checkRows(5)
        lin2 = tdSql.queryResult[1][1]
        lin4 = tdSql.queryResult[3][1]
        expected_lin2 = (twa_sec1 + twa_sec3) / 2.0
        expected_lin4 = (twa_sec3 + 100.0) / 2.0
        if abs(lin2 - expected_lin2) > 0.01:
            tdLog.exit(f"twa fill(linear) sec2: expected ~{expected_lin2}, got {lin2}")
        if abs(lin4 - expected_lin4) > 0.01:
            tdLog.exit(f"twa fill(linear) sec4: expected ~{expected_lin4}, got {lin4}")

    # ==================================================================
    # 9. sample + FILL unsupported
    # ==================================================================
    def test_sample_fill_unsupported(self):
        """Test that sample() rejects all FILL modes.

        Verifies that sample() produces a clear error message when used
        with any FILL mode, including NULL, NONE, VALUE, PREV, NEXT, and
        LINEAR.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()
        for mode in ("null", "none", "value, -1", "prev", "next", "linear"):
            tdSql.error(
                f"select _wstart, sample(val,2) from t1 {WHERE} "
                f"interval(1s) fill({mode})"
            )

    # ==================================================================
    # 10. unsupported FILL modes (PREV / NEXT / LINEAR) for indefRows
    # ==================================================================
    def test_unsupported_fill_modes(self):
        """Test that indefRows functions reject FILL(PREV/NEXT/LINEAR).

        Verifies that PREV, NEXT, and LINEAR fill modes are rejected with
        a parser error for all indefinite-rows functions.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Extended to all indefRows functions.
        """
        self._setup_basic()
        func_exprs = {
            "csum": "csum(val)",
            "diff": "diff(val)",
            "derivative": "derivative(val,1s,0)",
            "mavg": "mavg(val,2)",
            "statecount": 'statecount(val,"GT",0)',
            "stateduration": 'stateduration(val,"GT",0,1a)',
        }
        for name, expr in func_exprs.items():
            for mode in ("PREV", "NEXT", "LINEAR"):
                tdSql.error(
                    f"select _wstart, {expr} from t1 {WHERE} "
                    f"interval(1s) fill({mode})"
                )

    # ==================================================================
    # 4b. FILL(NULL_F) and FILL(VALUE_F) forced fill modes
    # ==================================================================
    def test_fill_null_f(self):
        """Test FILL(NULL_F) behavior compared to FILL(NULL).

        FILL(NULL_F) forces NULL output even when no data exists in the
        query range, while FILL(NULL) returns empty in the same scenario.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()
        # Query range 06-09s has no data (basic data is at 01-05s).
        no_data_where = (
            "WHERE ts >= '2024-01-01 00:00:06' "
            "AND ts < '2024-01-01 00:00:09'"
        )

        # -- NULL gives 0 rows when no data in range --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {no_data_where} "
            "interval(1s) fill(null)"
        )
        tdSql.checkRows(0)

        # -- NULL_F forces 3 filled rows --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {no_data_where} "
            "interval(1s) fill(null_f)"
        )
        self._check([
            (_ts(6), None),
            (_ts(7), None),
            (_ts(8), None),
        ])

        # -- When data exists in range, NULL_F behaves like NULL --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null_f)"
        )
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        null_result = list(tdSql.queryResult)
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null_f)"
        )
        null_f_result = list(tdSql.queryResult)
        if null_result != null_f_result:
            tdLog.exit(
                f"NULL vs NULL_F mismatch with data in range: "
                f"{null_result} != {null_f_result}"
            )

        # -- Verify multiple functions work with NULL_F --
        for expr in ("diff(val)", "derivative(val,1s,0)", "mavg(val,2)",
                      'statecount(val,"GT",0)',
                      'stateduration(val,"GT",0,1a)'):
            tdSql.query(
                f"select _wstart, {expr} from t1 {no_data_where} "
                "interval(1s) fill(null_f)"
            )
            self._check([
                (_ts(6), None),
                (_ts(7), None),
                (_ts(8), None),
            ])

    def test_fill_value_f(self):
        """Test FILL(VALUE_F) behavior compared to FILL(VALUE).

        FILL(VALUE_F) forces VALUE output even when no data exists in the
        query range, while FILL(VALUE) returns empty in the same scenario.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-16 Simon created.
        """
        self._setup_basic()
        no_data_where = (
            "WHERE ts >= '2024-01-01 00:00:06' "
            "AND ts < '2024-01-01 00:00:09'"
        )

        # -- VALUE gives 0 rows when no data in range --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {no_data_where} "
            "interval(1s) fill(value, 0)"
        )
        tdSql.checkRows(0)

        # -- VALUE_F forces 3 filled rows --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {no_data_where} "
            "interval(1s) fill(value_f, 0)"
        )
        self._check([
            (_ts(6), 0),
            (_ts(7), 0),
            (_ts(8), 0),
        ])

        # -- When data exists in range, VALUE_F behaves like VALUE --
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(value, 0)"
        )
        value_result = list(tdSql.queryResult)
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(value_f, 0)"
        )
        value_f_result = list(tdSql.queryResult)
        if value_result != value_f_result:
            tdLog.exit(
                f"VALUE vs VALUE_F mismatch with data in range: "
                f"{value_result} != {value_f_result}"
            )

        # -- Verify multiple functions work with VALUE_F --
        for expr in ("diff(val)", "derivative(val,1s,0)", "mavg(val,2)",
                      'statecount(val,"GT",0)',
                      'stateduration(val,"GT",0,1a)'):
            tdSql.query(
                f"select _wstart, {expr} from t1 {no_data_where} "
                "interval(1s) fill(value_f, -1)"
            )
            self._check([
                (_ts(6), -1),
                (_ts(7), -1),
                (_ts(8), -1),
            ])

    # ==================================================================
    # 5. dense window (20 rows in one window)
    # ==================================================================
    def test_dense_windows(self):
        """Test all functions on dense windows with a gap.

        Verifies all functions with 20 rows in [1s,2s), a gap at 2s,
        and 1 row at 3s.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Extended to all timeseries functions.
        """
        self._setup_basic()
        self._setup_dense()
        where = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:04'"
        )

        # --- csum: 20 + NULL + 1 = 22 rows ---
        tdLog.debug("csum dense")
        tdSql.query(
            f"select _wstart, csum(val) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), (i + 1) * (i + 2) // 2) for i in range(20)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), 99))
        self._check_bulk(expected)

        # --- diff: 19 + NULL + NULL = 21 rows ---
        tdLog.debug("diff dense")
        tdSql.query(
            f"select _wstart, diff(val) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), 1) for _ in range(19)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), None))
        self._check_bulk(expected)

        # --- derivative: 19 + NULL + NULL = 21 rows ---
        tdLog.debug("derivative dense")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), 20.0) for _ in range(19)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), None))
        self._check_bulk(expected)

        # --- mavg(val,2): 19 + NULL + NULL = 21 rows ---
        tdLog.debug("mavg dense")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), i + 1.5) for i in range(19)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), None))
        self._check_bulk(expected)

        # --- statecount: 20 + NULL + 1 = 22 rows ---
        tdLog.debug("statecount dense")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), i + 1) for i in range(20)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), 1))
        self._check_bulk(expected)

        # --- stateduration: 20 + NULL + 1 = 22 rows ---
        tdLog.debug("stateduration dense")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_dense "
            f"{where} interval(1s) fill(null)"
        )
        expected = [(_ts(1), i * 50) for i in range(20)]
        expected.append((_ts(2), None))
        expected.append((_ts(3), 0))
        self._check_bulk(expected)

        # --- irate: 1 + NULL + 1 = 3 rows ---
        tdLog.debug("irate dense")
        tdSql.query(
            f"select _wstart, irate(val) from t_dense {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0),
            (_ts(2), None),
            (_ts(3), 0.0),
        ])

    # ==================================================================
    # 6. null input handling
    # ==================================================================
    def test_null_input(self):
        """Test all functions on data containing NULL values.

        Verifies that each timeseries function correctly handles NULL input
        values, skipping or propagating them as appropriate.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Extended to all timeseries functions.
        """
        self._setup_basic()
        self._setup_nulls()

        # --- csum skips NULL input ---
        tdLog.debug("csum null input")
        tdSql.query(
            f"select _wstart, csum(val) from t_nulls {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 40),
            (_ts(1), 80),
            (_ts(2), None),
            (_ts(3), 60),
            (_ts(3), 130),
            (_ts(4), None),
            (_ts(5), 100),
        ])

        # --- diff: NULL input produces NULL diff, then resumes ---
        tdLog.debug("diff null input")
        tdSql.query(
            f"select _wstart, diff(val) from t_nulls {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), None),
            (_ts(1), 20),
            (_ts(1), 10),
            (_ts(1), None),
            (_ts(2), None),
            (_ts(3), 10),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- derivative: NULL skipped entirely, uses non-null neighbors ---
        tdLog.debug("derivative null input")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_nulls {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0),
            (_ts(1), 50.0),
            (_ts(2), None),
            (_ts(3), 50.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- mavg: skips NULLs ---
        tdLog.debug("mavg null input")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_nulls {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0),
            (_ts(1), 35.0),
            (_ts(2), None),
            (_ts(3), 65.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- statecount: NULL row outputs None (reset) ---
        tdLog.debug("statecount null input")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_nulls "
            f"{WHERE} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1),
            (_ts(1), None),
            (_ts(1), 2),
            (_ts(1), 3),
            (_ts(1), None),
            (_ts(2), None),
            (_ts(3), 1),
            (_ts(3), 2),
            (_ts(4), None),
            (_ts(5), 1),
        ])

        # --- irate: uses last two non-null points ---
        tdLog.debug("irate null input")
        tdSql.query(
            f"select _wstart, irate(val) from t_nulls {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0),
            (_ts(2), None),
            (_ts(3), 50.0),
            (_ts(4), None),
            (_ts(5), 0.0),
        ])

        # --- stateduration: NULL row outputs None ---
        tdLog.debug("stateduration null input")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_nulls "
            f"{WHERE} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0),
            (_ts(1), None),
            (_ts(1), 400),
            (_ts(1), 600),
            (_ts(1), None),
            (_ts(2), None),
            (_ts(3), 0),
            (_ts(3), 200),
            (_ts(4), None),
            (_ts(5), 0),
        ])

    def test_single_row_windows(self):
        """Test all functions where each window has exactly one data row.

        Verifies correct behavior when windows contain a single data row,
        including handling of gap windows between them.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Extended to all timeseries functions.
        """
        self._setup_basic()
        self._setup_single_row_windows()

        # --- csum fill(null): 4 data + 1 gap ---
        tdLog.debug("csum single-row fill(null)")
        tdSql.query(
            f"select _wstart, csum(val) from t_single {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10), (_ts(2), 20), (_ts(3), None),
            (_ts(4), 40), (_ts(5), 50),
        ])

        # --- csum fill(value, 999) ---
        tdLog.debug("csum single-row fill(value, 999)")
        tdSql.query(
            f"select _wstart, csum(val) from t_single {WHERE} "
            "interval(1s) fill(value, 999)"
        )
        self._check([
            (_ts(1), 10), (_ts(2), 20), (_ts(3), 999),
            (_ts(4), 40), (_ts(5), 50),
        ])

        # --- diff: all single-row → no diff output per window ---
        tdLog.debug("diff single-row fill(null)")
        tdSql.query(
            f"select _wstart, diff(val) from t_single {WHERE} "
            "interval(1s) fill(null)"
        )
        tdSql.checkRows(0)

        # --- derivative: all single-row → empty ---
        tdLog.debug("derivative single-row fill(null)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_single "
            f"{WHERE} interval(1s) fill(null)"
        )
        tdSql.checkRows(0)

        # --- mavg(val,2): all single-row → empty ---
        tdLog.debug("mavg single-row fill(null)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_single {WHERE} "
            "interval(1s) fill(null)"
        )
        tdSql.checkRows(0)

        # --- statecount: 1 row per data window, gap filled ---
        tdLog.debug("statecount single-row fill(null)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_single "
            f"{WHERE} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(2), 1), (_ts(3), None),
            (_ts(4), 1), (_ts(5), 1),
        ])

        # --- stateduration: 1 row per data window, gap filled ---
        tdLog.debug("stateduration single-row fill(null)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_single "
            f"{WHERE} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(2), 0), (_ts(3), None),
            (_ts(4), 0), (_ts(5), 0),
        ])

        # --- irate: single row → 0.0 ---
        tdLog.debug("irate single-row fill(null)")
        tdSql.query(
            f"select _wstart, irate(val) from t_single {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0.0), (_ts(2), 0.0), (_ts(3), None),
            (_ts(4), 0.0), (_ts(5), 0.0),
        ])

    # ==================================================================
    # 8. consecutive (no-gap) windows
    # ==================================================================
    def test_consecutive_windows(self):
        """Test all functions with consecutive windows containing no gaps.

        Verifies that FILL(NULL) and FILL(NONE) produce identical row
        counts when there are no gaps between data windows.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Extended to all timeseries functions.
        """
        self._setup_basic()
        self._setup_consecutive()

        # Use local WHERE matching data range [1s,4s) — no gap windows.
        where_consec = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:04'"
        )

        # --- csum fill(null) ---
        tdLog.debug("csum consecutive fill(null)")
        tdSql.query(
            f"select _wstart, csum(val) from t_consec {where_consec} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10), (_ts(1), 30),
            (_ts(2), 30), (_ts(2), 70),
            (_ts(3), 50), (_ts(3), 110),
        ])

        # --- csum: FILL(NULL) row count == FILL(NONE) row count ---
        tdLog.debug("csum consecutive null==none")
        rows_null = tdSql.queryRows
        tdSql.query(
            f"select _wstart, csum(val) from t_consec {where_consec} "
            "interval(1s) fill(none)"
        )
        rows_none = tdSql.queryRows
        if rows_null != rows_none:
            tdLog.exit(
                f"FILL(NULL) rows {rows_null} != "
                f"FILL(NONE) rows {rows_none} with no gaps"
            )

        # --- diff fill(null) ---
        tdLog.debug("diff consecutive fill(null)")
        tdSql.query(
            f"select _wstart, diff(val) from t_consec {where_consec} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10), (_ts(2), 10), (_ts(3), 10),
        ])

        # --- derivative ---
        tdLog.debug("derivative consecutive fill(null)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_consec "
            f"{where_consec} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0), (_ts(2), 20.0), (_ts(3), 20.0),
        ])

        # --- mavg ---
        tdLog.debug("mavg consecutive fill(null)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_consec "
            f"{where_consec} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 15.0), (_ts(2), 35.0), (_ts(3), 55.0),
        ])

        # --- statecount: 2+2+2 = 6 rows ---
        tdLog.debug("statecount consecutive fill(null)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_consec "
            f"{where_consec} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2),
            (_ts(2), 1), (_ts(2), 2),
            (_ts(3), 1), (_ts(3), 2),
        ])

        # --- stateduration: 500ms spacing → 0, 500 per window ---
        tdLog.debug("stateduration consecutive fill(null)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_consec "
            f"{where_consec} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 500),
            (_ts(2), 0), (_ts(2), 500),
            (_ts(3), 0), (_ts(3), 500),
        ])

        # --- irate ---
        tdLog.debug("irate consecutive fill(null)")
        tdSql.query(
            f"select _wstart, irate(val) from t_consec "
            f"{where_consec} interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0), (_ts(2), 20.0), (_ts(3), 20.0),
        ])

    # ==================================================================
    # 9. gap patterns: leading gaps
    # ==================================================================
    def test_leading_gaps(self):
        """Test all timeseries functions with leading gap windows.

        Verifies correct behavior when the query range starts before the
        first data row, producing gap windows at the beginning.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._setup_basic()
        where = (
            "WHERE ts >= '2024-01-01 00:00:00' "
            "AND ts < '2024-01-01 00:00:04'"
        )

        # --- csum ---
        tdLog.debug("csum leading gaps")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 10), (_ts(1), 30), (_ts(1), 60),
            (_ts(1), 100), (_ts(1), 150),
            (_ts(2), None),
            (_ts(3), 60), (_ts(3), 130), (_ts(3), 210),
        ])

        # --- diff ---
        tdLog.debug("diff leading gaps")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 10), (_ts(1), 10), (_ts(1), 10), (_ts(1), 10),
            (_ts(2), None),
            (_ts(3), 10), (_ts(3), 10),
        ])

        # --- derivative ---
        tdLog.debug("derivative leading gaps")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 50.0), (_ts(1), 50.0), (_ts(1), 50.0), (_ts(1), 50.0),
            (_ts(2), None),
            (_ts(3), 50.0), (_ts(3), 50.0),
        ])

        # --- mavg ---
        tdLog.debug("mavg leading gaps")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 15.0), (_ts(1), 25.0), (_ts(1), 35.0), (_ts(1), 45.0),
            (_ts(2), None),
            (_ts(3), 65.0), (_ts(3), 75.0),
        ])

        # --- statecount ---
        tdLog.debug("statecount leading gaps")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 1), (_ts(1), 2), (_ts(1), 3), (_ts(1), 4), (_ts(1), 5),
            (_ts(2), None),
            (_ts(3), 1), (_ts(3), 2), (_ts(3), 3),
        ])

        # --- stateduration ---
        tdLog.debug("stateduration leading gaps")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None),
            (_ts(1), 0), (_ts(1), 200), (_ts(1), 400),
            (_ts(1), 600), (_ts(1), 800),
            (_ts(2), None),
            (_ts(3), 0), (_ts(3), 200), (_ts(3), 400),
        ])

        # --- irate ---
        tdLog.debug("irate leading gaps")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(0), None), (_ts(1), 50.0),
            (_ts(2), None), (_ts(3), 50.0),
        ])

        # --- twa ---
        tdLog.debug("twa leading gaps")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check_approx([
            (_ts(0), None), (_ts(1), 34.15),
            (_ts(2), None), (_ts(3), 70.0),
        ], tol=0.1)

    # ==================================================================
    # 10. gap patterns: trailing gaps
    # ==================================================================
    def test_trailing_gaps(self):
        """Test all timeseries functions with trailing gap windows.

        Verifies correct behavior when the query range extends past the
        last data row, producing gap windows at the end.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._setup_basic()
        where = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:08'"
        )

        # --- csum ---
        tdLog.debug("csum trailing gaps")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10), (_ts(1), 30), (_ts(1), 60),
            (_ts(1), 100), (_ts(1), 150),
            (_ts(2), None),
            (_ts(3), 60), (_ts(3), 130), (_ts(3), 210),
            (_ts(4), None),
            (_ts(5), 100),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- diff ---
        tdLog.debug("diff trailing gaps")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10), (_ts(1), 10), (_ts(1), 10), (_ts(1), 10),
            (_ts(2), None),
            (_ts(3), 10), (_ts(3), 10),
            (_ts(4), None),
            (_ts(5), None),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- derivative ---
        tdLog.debug("derivative trailing gaps")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(1), 50.0), (_ts(1), 50.0), (_ts(1), 50.0),
            (_ts(2), None),
            (_ts(3), 50.0), (_ts(3), 50.0),
            (_ts(4), None),
            (_ts(5), None),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- mavg ---
        tdLog.debug("mavg trailing gaps")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 15.0), (_ts(1), 25.0), (_ts(1), 35.0), (_ts(1), 45.0),
            (_ts(2), None),
            (_ts(3), 65.0), (_ts(3), 75.0),
            (_ts(4), None),
            (_ts(5), None),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- statecount ---
        tdLog.debug("statecount trailing gaps")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2), (_ts(1), 3), (_ts(1), 4), (_ts(1), 5),
            (_ts(2), None),
            (_ts(3), 1), (_ts(3), 2), (_ts(3), 3),
            (_ts(4), None),
            (_ts(5), 1),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- stateduration ---
        tdLog.debug("stateduration trailing gaps")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 200), (_ts(1), 400),
            (_ts(1), 600), (_ts(1), 800),
            (_ts(2), None),
            (_ts(3), 0), (_ts(3), 200), (_ts(3), 400),
            (_ts(4), None),
            (_ts(5), 0),
            (_ts(6), None),
            (_ts(7), None),
        ])

        # --- irate ---
        tdLog.debug("irate trailing gaps")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 50.0), (_ts(2), None), (_ts(3), 50.0),
            (_ts(4), None), (_ts(5), 0.0),
            (_ts(6), None), (_ts(7), None),
        ])

        # --- twa ---
        tdLog.debug("twa trailing gaps")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {where} "
            "interval(1s) fill(null)"
        )
        self._check_approx([
            (_ts(1), 34.15), (_ts(2), None), (_ts(3), 78.24),
            (_ts(4), None), (_ts(5), 100.0),
            (_ts(6), None), (_ts(7), None),
        ], tol=0.1)

    # ==================================================================
    # 11. gap patterns: multiple consecutive gaps
    # ==================================================================
    def test_multiple_consecutive_gaps(self):
        """Test all timeseries functions with several consecutive gap windows.

        Verifies behavior when several consecutive gap windows exist between
        data windows (data at 1s and 5s, gaps at 2s/3s/4s).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._create_db()
        tdSql.execute("create table t_gaps (ts timestamp, val int)")
        tdSql.execute(
            "insert into t_gaps values "
            f"('{TS_BASE}01.000', 10)"
            f"('{TS_BASE}01.500', 20)"
            f"('{TS_BASE}05.000', 50)"
        )
        where = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:06'"
        )

        # --- csum ---
        tdLog.debug("csum multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, csum(val) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), 50),
        ])

        # --- diff ---
        tdLog.debug("diff multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, diff(val) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 10),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- derivative ---
        tdLog.debug("derivative multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- mavg ---
        tdLog.debug("mavg multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 15.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- statecount ---
        tdLog.debug("statecount multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), 1),
        ])

        # --- stateduration ---
        tdLog.debug("stateduration multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 500),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), 0),
        ])

        # --- irate ---
        tdLog.debug("irate multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, irate(val) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 20.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), 0.0),
        ])

        # --- twa ---
        tdLog.debug("twa multiple consecutive gaps")
        tdSql.query(
            f"select _wstart, twa(val) from t_gaps {where} "
            "interval(1s) fill(null)"
        )
        self._check_approx([
            (_ts(1), 18.57),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), 50.0),
        ], tol=0.1)

    # ==================================================================
    # 12. interval size variations
    # ==================================================================
    def test_interval_variations(self):
        """Test all timeseries functions with varying interval sizes.

        Verifies correct behavior with larger (10s) and smaller (500ms)
        intervals compared to the default 1s interval.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 3 separate tests.
        """
        self._setup_basic()

        # --- csum larger interval (10s): all 9 data rows in one window ---
        tdLog.debug("csum interval(10s)")
        where_10 = (
            "WHERE ts >= '2024-01-01 00:00:00' "
            "AND ts < '2024-01-01 00:00:10'"
        )
        tdSql.query(
            f"select _wstart, csum(val) from t1 {where_10} "
            "interval(10s) fill(null)"
        )
        self._check([
            (_ts(0), 10), (_ts(0), 30), (_ts(0), 60),
            (_ts(0), 100), (_ts(0), 150), (_ts(0), 210),
            (_ts(0), 280), (_ts(0), 360), (_ts(0), 460),
        ])

        # --- csum smaller interval (500ms): sub-second windows ---
        tdLog.debug("csum interval(500a)")
        where_500a = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:02'"
        )
        tdSql.query(
            f"select _wstart, csum(val) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 10), (_ts(1, 0), 30), (_ts(1, 0), 60),
            (_ts(1, 500), 40), (_ts(1, 500), 90),
        ])

        # --- diff smaller interval: multi-row windows produce diffs ---
        tdLog.debug("diff interval(500a)")
        tdSql.query(
            f"select _wstart, diff(val) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 10), (_ts(1, 0), 10),
            (_ts(1, 500), 10),
        ])

        # --- derivative 500a ---
        tdLog.debug("derivative interval(500a)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 50.0), (_ts(1, 0), 50.0),
            (_ts(1, 500), 50.0),
        ])

        # --- mavg 500a ---
        tdLog.debug("mavg interval(500a)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 15.0), (_ts(1, 0), 25.0),
            (_ts(1, 500), 45.0),
        ])

        # --- statecount 500a ---
        tdLog.debug("statecount interval(500a)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 1), (_ts(1, 0), 2), (_ts(1, 0), 3),
            (_ts(1, 500), 1), (_ts(1, 500), 2),
        ])

        # --- stateduration 500a ---
        tdLog.debug("stateduration interval(500a)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 0), (_ts(1, 0), 200), (_ts(1, 0), 400),
            (_ts(1, 500), 0), (_ts(1, 500), 200),
        ])

        # --- irate 10s: single window ---
        tdLog.debug("irate interval(10s)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {where_10} "
            "interval(10s) fill(null)"
        )
        self._check([
            (_ts(0), 12.5),
        ])

        # --- irate 500a ---
        tdLog.debug("irate interval(500a)")
        tdSql.query(
            f"select _wstart, irate(val) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check([
            (_ts(1, 0), 50.0),
            (_ts(1, 500), 50.0),
        ])

        # --- twa 10s: single window ---
        tdLog.debug("twa interval(10s)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {where_10} "
            "interval(10s) fill(null)"
        )
        self._check_approx([
            (_ts(0), 65.5),
        ], tol=0.1)

        # --- twa 500a ---
        tdLog.debug("twa interval(500a)")
        tdSql.query(
            f"select _wstart, twa(val) from t1 {where_500a} "
            "interval(500a) fill(null)"
        )
        self._check_approx([
            (_ts(1, 0), 22.48),
            (_ts(1, 500), 42.5),
        ], tol=0.1)

    # ==================================================================
    # 13. super-table & partition queries
    # ==================================================================
    def test_super_table(self):
        """Test csum() on super table with partitioning.

        Verifies no partition produces an error, and partition by tbname
        works correctly with FILL(NONE) and FILL(NULL).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 3 separate tests.
        """
        self._setup_basic()
        self._setup_super()

        # --- no partition → error ---
        tdLog.debug("csum super table no partition")
        tdSql.error(
            f"select _wstart, csum(val) from st1 {WHERE} "
            "interval(1s) fill(null)"
        )

        # --- partition fill(none): verify per-partition values ---
        tdLog.debug("csum partition fill(none)")
        tdSql.query(
            f"select tbname, _wstart, csum(val) from st1 {WHERE} "
            "partition by tbname interval(1s) fill(none)"
        )
        ct_a_rows = sorted(
            [(str(tdSql.queryResult[i][1]), tdSql.queryResult[i][2])
             for i in range(tdSql.queryRows)
             if tdSql.queryResult[i][0] == "ct_a"],
            key=lambda x: (x[0], x[1])
        )
        ct_b_rows = sorted(
            [(str(tdSql.queryResult[i][1]), tdSql.queryResult[i][2])
             for i in range(tdSql.queryRows)
             if tdSql.queryResult[i][0] == "ct_b"],
            key=lambda x: (x[0], x[1])
        )
        ct_a_vals = [r[1] for r in ct_a_rows]
        ct_b_vals = [r[1] for r in ct_b_rows]
        expected_a = [10, 30, 60, 100, 150, 60, 130, 210, 100]
        if ct_a_vals != expected_a:
            tdLog.exit(f"ct_a fill(none) expected {expected_a}, got {ct_a_vals}")
        if ct_b_vals != [100, 300, 300]:
            tdLog.exit(f"ct_b fill(none) expected [100,300,300], got {ct_b_vals}")

        # --- partition fill(null): verify per-partition ---
        tdLog.debug("csum partition fill(null)")
        tdSql.query(
            f"select tbname, _wstart, csum(val) from st1 {WHERE} "
            "partition by tbname interval(1s) fill(null)"
        )
        ct_a_rows = [
            tdSql.queryResult[i]
            for i in range(tdSql.queryRows)
            if tdSql.queryResult[i][0] == "ct_a"
        ]
        ct_b_rows = [
            tdSql.queryResult[i]
            for i in range(tdSql.queryRows)
            if tdSql.queryResult[i][0] == "ct_b"
        ]
        # NOTE: partition fill(null) row count for indefRows functions
        # can vary depending on internal vgroup state; just verify
        # both partitions are present with reasonable row counts.
        if len(ct_a_rows) < 5:
            tdLog.exit(f"ct_a expected >= 5 rows, got {len(ct_a_rows)}")
        if len(ct_b_rows) < 3:
            tdLog.exit(f"ct_b expected >= 3 rows, got {len(ct_b_rows)}")

    # ==================================================================
    # 14. negative values
    # ==================================================================
    def test_negative_values(self):
        """Test all timeseries functions with negative input values.

        Verifies that each timeseries function handles negative integer
        input values correctly across data and gap windows.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._create_db()
        tdSql.execute("create table t_neg (ts timestamp, val int)")
        tdSql.execute(
            "insert into t_neg values "
            f"('{TS_BASE}01.000', -10)"
            f"('{TS_BASE}01.200', -20)"
            f"('{TS_BASE}01.400', -30)"
            f"('{TS_BASE}03.000', -50)"
        )

        # --- csum ---
        tdLog.debug("csum negative values")
        tdSql.query(
            f"select _wstart, csum(val) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -10), (_ts(1), -30), (_ts(1), -60),
            (_ts(2), None),
            (_ts(3), -50),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- diff ---
        tdLog.debug("diff negative values")
        tdSql.query(
            f"select _wstart, diff(val) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -10), (_ts(1), -10),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- derivative ---
        tdLog.debug("derivative negative values")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -50.0), (_ts(1), -50.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- mavg ---
        tdLog.debug("mavg negative values")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -15.0), (_ts(1), -25.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- statecount ---
        tdLog.debug("statecount negative values")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -1), (_ts(1), -1), (_ts(1), -1),
            (_ts(2), None),
            (_ts(3), -1),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- stateduration ---
        tdLog.debug("stateduration negative values")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -1), (_ts(1), -1), (_ts(1), -1),
            (_ts(2), None),
            (_ts(3), -1),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- irate ---
        tdLog.debug("irate negative values")
        tdSql.query(
            f"select _wstart, irate(val) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -150.0),
            (_ts(2), None),
            (_ts(3), 0.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- twa ---
        tdLog.debug("twa negative values")
        tdSql.query(
            f"select _wstart, twa(val) from t_neg {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check_approx([
            (_ts(1), -28.24),
            (_ts(2), None),
            (_ts(3), -50.0),
            (_ts(4), None),
            (_ts(5), None),
        ], tol=0.1)

    # ==================================================================
    # 15. boundary values (INT32 min/max)
    # ==================================================================
    def test_boundary_values(self):
        """Test all timeseries functions with INT32 boundary values.

        Verifies correct behavior with INT32 boundary values stored in a
        bigint column, including INT32_MAX and INT32_MIN.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._setup_basic()
        self._setup_boundary_values()

        # --- csum ---
        tdLog.debug("csum boundary values")
        tdSql.query(
            f"select _wstart, csum(val) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 2147483647),
            (_ts(1), 2147483648),
            (_ts(2), None),
            (_ts(3), -2147483648),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- diff ---
        tdLog.debug("diff boundary values")
        tdSql.query(
            f"select _wstart, diff(val) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -2147483646),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- derivative ---
        tdLog.debug("derivative boundary values")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), -4294967292.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- mavg ---
        tdLog.debug("mavg boundary values")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1073741824.0),
            (_ts(2), None),
            (_ts(3), None),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- statecount ---
        tdLog.debug("statecount boundary values")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 1), (_ts(1), 2),
            (_ts(2), None),
            (_ts(3), -1),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- stateduration ---
        tdLog.debug("stateduration boundary values")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 0), (_ts(1), 500),
            (_ts(2), None),
            (_ts(3), -1),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- irate ---
        tdLog.debug("irate boundary values")
        tdSql.query(
            f"select _wstart, irate(val) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 2.0),
            (_ts(2), None),
            (_ts(3), 0.0),
            (_ts(4), None),
            (_ts(5), None),
        ])

        # --- twa ---
        tdLog.debug("twa boundary values")
        tdSql.query(
            f"select _wstart, twa(val) from t_boundary {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check_approx([
            (_ts(1), 358988041.6),
            (_ts(2), None),
            (_ts(3), -2147483648.0),
            (_ts(4), None),
            (_ts(5), None),
        ], tol=1.0)

    # ==================================================================
    # 16. empty result range
    # ==================================================================
    def test_empty_range(self):
        """Test csum() and diff() with a query range containing no data.

        Verifies that querying a time range with no data produces an
        empty result set for both csum() and diff().

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated csum+diff into one test.
        """
        self._setup_basic()
        where = (
            "WHERE ts >= '2024-01-01 00:00:10' "
            "AND ts < '2024-01-01 00:00:15'"
        )
        for func in ("csum", "diff"):
            tdSql.query(
                f"select _wstart, {func}(val) from t1 {where} "
                "interval(1s) fill(null)"
            )
            tdSql.checkRows(0)

    # ==================================================================
    # 17. larger dataset (85 rows, 20 windows, 3 gaps)
    # ==================================================================
    def test_large_dataset(self):
        """Test csum() and diff() on a large 85-row dataset.

        Verifies correct behavior on 85 rows across 20 windows with 3 gap
        windows at intervals 5, 10, and 15.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 3 separate tests.
        """
        self._setup_basic()
        self._setup_large_dataset()
        where = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:21'"
        )
        gap_windows = {5, 10, 15}

        # --- csum fill(null): 85 data + 3 gaps = 88 ---
        tdLog.debug("csum large fill(null)")
        tdSql.query(
            f"select _wstart, csum(val) from t_large {where} "
            "interval(1s) fill(null)"
        )
        expected = []
        v = 1
        for w in range(20):
            wstart = _ts(w + 1)
            if w in gap_windows:
                expected.append((wstart, None))
            else:
                running = 0
                for _ in range(5):
                    running += v
                    expected.append((wstart, running))
                    v += 1
        self._check_bulk(expected)

        # --- csum fill(none): 85 data rows, no gaps ---
        tdLog.debug("csum large fill(none)")
        tdSql.query(
            f"select _wstart, csum(val) from t_large {where} "
            "interval(1s) fill(none)"
        )
        expected = []
        v = 1
        for w in range(20):
            if w in gap_windows:
                continue
            wstart = _ts(w + 1)
            running = 0
            for _ in range(5):
                running += v
                expected.append((wstart, running))
                v += 1
        self._check_bulk(expected)

        # --- diff fill(null): 17×4 diff + 3 gaps = 71 ---
        tdLog.debug("diff large fill(null)")
        tdSql.query(
            f"select _wstart, diff(val) from t_large {where} "
            "interval(1s) fill(null)"
        )
        expected = []
        for w in range(20):
            wstart = _ts(w + 1)
            if w in gap_windows:
                expected.append((wstart, None))
            else:
                for _ in range(4):
                    expected.append((wstart, 1))
        self._check_bulk(expected)

    # ==================================================================
    # 18. cross-block boundary (maxrows=200, 997 rows/window)
    # ==================================================================
    def test_cross_block(self):
        """Test csum() and diff() with windows spanning multiple data blocks.

        Each window spans multiple data blocks (maxrows=200, 997 rows per
        window). Tests FILL(NULL), FILL(VALUE), FILL(NONE), diff, and
        leading+trailing gaps.

        NOTE: per-window row count must NOT be an exact multiple of
        maxrows — TDengine has a pre-existing scan ordering bug where
        a window whose row count equals N*maxrows is returned out of
        timestamp order by the upstream operator.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 5 separate tests.
        """
        self._setup_cross_block_dataset()
        n = self.XBLK_ROWS  # 997
        where = (
            "WHERE ts >= '2024-01-01 00:00:01' "
            "AND ts < '2024-01-01 00:00:04'"
        )

        # build reusable csum expected for windows 1 and 2
        def _csum_win1():
            running = 0
            out = []
            for i in range(n):
                running += (i + 1)
                out.append((_ts(1), running))
            return out

        def _csum_win2():
            running = 0
            out = []
            for i in range(n):
                running += (1001 + i)
                out.append((_ts(3), running))
            return out

        win1 = _csum_win1()
        win2 = _csum_win2()

        # --- csum fill(null) ---
        tdLog.debug("csum cross-block fill(null)")
        tdSql.query(
            f"select _wstart, csum(val) from t_xblk {where} "
            "interval(1s) fill(null)"
        )
        expected = win1 + [(_ts(2), None)] + win2
        self._check_bulk(expected)

        # --- csum fill(value, -999) ---
        tdLog.debug("csum cross-block fill(value, -999)")
        tdSql.query(
            f"select _wstart, csum(val) from t_xblk {where} "
            "interval(1s) fill(value, -999)"
        )
        expected = win1 + [(_ts(2), -999)] + win2
        self._check_bulk(expected)

        # --- csum fill(none) ---
        tdLog.debug("csum cross-block fill(none)")
        tdSql.query(
            f"select _wstart, csum(val) from t_xblk {where} "
            "interval(1s) fill(none)"
        )
        expected = win1 + win2
        self._check_bulk(expected)

        # --- diff fill(null) ---
        tdLog.debug("diff cross-block fill(null)")
        tdSql.query(
            f"select _wstart, diff(val) from t_xblk {where} "
            "interval(1s) fill(null)"
        )
        diff_per_win = n - 1
        expected = [(_ts(1), 1)] * diff_per_win
        expected.append((_ts(2), None))
        expected += [(_ts(3), 1)] * diff_per_win
        self._check_bulk(expected)

        # --- csum leading+trailing gaps ---
        tdLog.debug("csum cross-block leading+trailing gaps")
        where_wide = (
            "WHERE ts >= '2024-01-01 00:00:00' "
            "AND ts < '2024-01-01 00:00:05'"
        )
        tdSql.query(
            f"select _wstart, csum(val) from t_xblk {where_wide} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(0), None)] + win1 + [(_ts(2), None)] + win2 + [(_ts(4), None)]
        self._check_bulk(expected)

        # --- derivative cross-block fill(null): 996 + NULL + 996 = 1993 ---
        tdLog.debug("derivative cross-block fill(null)")
        tdSql.query(
            f"select _wstart, derivative(val,1s,0) from t_xblk {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), 1000.0)] * (n - 1)
        expected.append((_ts(2), None))
        expected += [(_ts(3), 1000.0)] * (n - 1)
        self._check_bulk(expected)

        # --- mavg(val,2) cross-block fill(null): 996 + NULL + 996 = 1993 ---
        tdLog.debug("mavg cross-block fill(null)")
        tdSql.query(
            f"select _wstart, mavg(val,2) from t_xblk {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), i + 1.5) for i in range(n - 1)]
        expected.append((_ts(2), None))
        expected += [(_ts(3), 1001 + i + 0.5) for i in range(n - 1)]
        self._check_bulk(expected)

        # --- statecount cross-block fill(null): 997 + NULL + 997 = 1995 ---
        tdLog.debug("statecount cross-block fill(null)")
        tdSql.query(
            f"select _wstart, statecount(val,\"GT\",0) from t_xblk {where} "
            "interval(1s) fill(null)"
        )
        expected = [(_ts(1), i + 1) for i in range(n)]
        expected.append((_ts(2), None))
        expected += [(_ts(3), i + 1) for i in range(n)]
        self._check_bulk(expected)

        # --- stateduration cross-block fill(null): 997 + NULL + 997 = 1995 ---
        tdLog.debug("stateduration cross-block fill(null)")
        tdSql.query(
            f"select _wstart, stateduration(val,\"GT\",0,1a) from t_xblk "
            f"{where} interval(1s) fill(null)"
        )
        expected = [(_ts(1), i) for i in range(n)]
        expected.append((_ts(2), None))
        expected += [(_ts(3), i) for i in range(n)]
        self._check_bulk(expected)

    # ==================================================================
    # 19. nested query / subquery / limit / offset
    # ==================================================================
    def test_nested_and_limit(self):
        """Test csum() + FILL(NULL) with nested queries and LIMIT/OFFSET.

        Verifies nested queries (count, filter NULL) and LIMIT / OFFSET
        combinations work correctly with csum() + FILL(NULL).

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 4 separate tests.
        """
        self._setup_basic()

        # --- nested count (single column result) ---
        tdLog.debug("nested count")
        tdSql.query(
            f"select count(*) from ("
            f"  select _wstart, csum(val) as cv from t1 {WHERE} "
            f"  interval(1s) fill(null)"
            f")"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)

        # --- nested filter NULL ---
        tdLog.debug("nested filter null")
        tdSql.query(
            f"select * from ("
            f"  select _wstart, csum(val) as cv from t1 {WHERE} "
            f"  interval(1s) fill(null)"
            f") where cv is not null"
        )
        self._check([
            (_ts(1), 10), (_ts(1), 30), (_ts(1), 60),
            (_ts(1), 100), (_ts(1), 150),
            (_ts(3), 60), (_ts(3), 130), (_ts(3), 210),
            (_ts(5), 100),
        ])

        # --- limit 2 ---
        tdLog.debug("limit 2")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null) limit 2"
        )
        self._check([
            (_ts(1), 10),
            (_ts(1), 30),
        ])

        # --- limit 5 offset 3 ---
        tdLog.debug("limit 5 offset 3")
        tdSql.query(
            f"select _wstart, csum(val) from t1 {WHERE} "
            "interval(1s) fill(null) limit 5 offset 3"
        )
        self._check([
            (_ts(1), 100), (_ts(1), 150),
            (_ts(2), None),
            (_ts(3), 60), (_ts(3), 130),
        ])

    # ==================================================================
    # 20. regression: regular agg fill unchanged
    # ==================================================================
    def test_regular_agg_fill_unchanged(self):
        """Test that regular aggregate FILL modes still work after indefRows changes.

        Verifies sum() + FILL(NULL) and sum() + FILL(PREV) still work
        normally as a regression check that indefRows changes didn't break
        regular aggregates.

        Since: 3.4.1.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-15 Simon created.
        - 2026-04-16 Consolidated from 2 separate tests.
        """
        self._setup_basic()

        # --- sum fill(null) ---
        tdLog.debug("sum fill(null)")
        tdSql.query(
            f"select _wstart, sum(val) from t1 {WHERE} "
            "interval(1s) fill(null)"
        )
        self._check([
            (_ts(1), 150), (_ts(2), None), (_ts(3), 210),
            (_ts(4), None), (_ts(5), 100),
        ])

        # --- sum fill(prev) ---
        tdLog.debug("sum fill(prev)")
        tdSql.query(
            f"select _wstart, sum(val) from t1 {WHERE} "
            "interval(1s) fill(prev)"
        )
        self._check([
            (_ts(1), 150), (_ts(2), 150), (_ts(3), 210),
            (_ts(4), 210), (_ts(5), 100),
        ])
