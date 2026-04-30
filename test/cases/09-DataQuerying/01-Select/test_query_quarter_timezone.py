"""
Timezone and DST tests for quarter duration alias (q = 3n).

Covers:
    - No DST: Asia/Shanghai
    - DST: Europe/Berlin
    - Cross-DST boundary data (spring / autumn transitions)
    - Session timezone switches between write-time and read-time

Each case asserts interval(1q) == interval(3n) under the active session timezone.

Known limitation covered by xfail cases below:
        - Quarter window boundaries are expected to follow the active session
            timezone.
        - Current behavior can still compute the boundary from a stale timezone,
            then format the returned timestamp in the current session timezone.
        - Those xfail cases document the intended behavior and should be flipped
            to normal assertions once the timezone propagation bug is fixed.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql


class TestQuarterTimezone:
    DB = "test_quarter_tz"
    DEFAULT_TZ = "UTC"
    SESSION_TZ_BOUNDARY_XFAIL = (
        "Known bug: quarter boundary calculation does not reliably use the "
        "active session timezone before formatting _wstart for display. "
        "Expected behavior is local quarter starts at 00:00 in the active "
        "session timezone."
    )

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self):
        self._set_timezone(self.DEFAULT_TZ)

    def _set_timezone(self, timezone):
        tdSql.execute(f"alter local 'timezone {timezone}'")

    def _create_db_and_table(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} precision 'ms'")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table t1 (ts timestamp, val int)")

    def _insert_quarterly_data(self):
        """Insert data spanning Q1-Q4 2024, one row per month at noon."""
        rows = [
            ("2024-01-15 12:00:00", 1),
            ("2024-02-15 12:00:00", 2),
            ("2024-03-15 12:00:00", 3),
            ("2024-04-15 12:00:00", 4),
            ("2024-05-15 12:00:00", 5),
            ("2024-06-15 12:00:00", 6),
            ("2024-07-15 12:00:00", 7),
            ("2024-08-15 12:00:00", 8),
            ("2024-09-15 12:00:00", 9),
            ("2024-10-15 12:00:00", 10),
            ("2024-11-15 12:00:00", 11),
            ("2024-12-15 12:00:00", 12),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into t1 values ('{ts}', {val})")

    def _assert_1q_equals_3n(self, label) -> list:
        """Assert interval(1q) == interval(3n) results."""
        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(3n) order by _wstart"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(1q) order by _wstart"
        )
        if tdSql.queryResult is not None:
            rows_1q: list = tdSql.queryResult
        else:
            rows_1q = []

        assert rows_3n == rows_1q, (
            f"[{label}] interval(1q) != interval(3n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )
        return rows_1q

    def _assert_local_quarter_starts(self, label, expected_starts):
        tdSql.query(
            "select TO_ISO8601(_wstart), count(*) "
            "from t1 interval(1q) order by _wstart"
        )

        rows = tdSql.queryResult if tdSql.queryResult is not None else []
        actual_starts = [row[0] for row in rows]

        assert actual_starts == expected_starts, (
            f"[{label}] unexpected quarter starts\n"
            f"expected: {expected_starts}\n"
            f"actual:   {actual_starts}"
        )

    def _assert_local_quarter_start_dates(self, label, expected_dates):
        tdSql.query(
            "select TO_ISO8601(_wstart), count(*) "
            "from t1 interval(1q) order by _wstart"
        )

        rows = tdSql.queryResult if tdSql.queryResult is not None else []
        actual_dates = [row[0][:10] for row in rows]

        assert actual_dates == expected_dates, (
            f"[{label}] unexpected quarter start dates\n"
            f"expected: {expected_dates}\n"
            f"actual:   {actual_dates}"
        )

    # ----------------------------------------------------------------
    #  No DST: Asia/Shanghai (UTC+8, no DST transitions)
    # ----------------------------------------------------------------

    def test_no_dst_shanghai(self):
        """Quarter interval under Asia/Shanghai (no DST).

        Verify that quarter alias behavior matches month-based quarter windows
        in a timezone without DST transitions.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Asia/Shanghai")
        self._create_db_and_table()
        self._insert_quarterly_data()
        rows = self._assert_1q_equals_3n("Asia/Shanghai")

        self._assert_local_quarter_start_dates(
            "Asia/Shanghai",
            [
                "2024-01-01",
                "2024-04-01",
                "2024-07-01",
                "2024-10-01",
            ],
        )

        # Should have 4 quarters
        assert len(rows) == 4, f"Expected 4 quarters, got {len(rows)}"

    @pytest.mark.xfail(
        reason=SESSION_TZ_BOUNDARY_XFAIL,
        strict=True,
    )
    def test_session_timezone_controls_quarter_boundary_utc(self):
        """Known-bug regression: UTC query should show local quarter starts.

        Verify the intended UTC quarter boundary behavior for the active
        session timezone. This case remains xfail until the known timezone
        propagation bug is fixed.

        Product expectation:
            The quarter windows should start at 00:00 on Jan/Apr/Jul/Oct 1st
            in the active session timezone.

        Current bug:
            The returned _wstart can be computed from a stale timezone and then
            formatted in the current session timezone, so the displayed value is
            no longer aligned to the local quarter boundary.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("UTC")
        self._create_db_and_table()
        self._insert_quarterly_data()
        self._assert_local_quarter_starts(
            "UTC",
            [
                "2024-01-01T00:00:00.000+0000",
                "2024-04-01T00:00:00.000+0000",
                "2024-07-01T00:00:00.000+0000",
                "2024-10-01T00:00:00.000+0000",
            ],
        )

    # ----------------------------------------------------------------
    #  DST: Europe/Berlin
    #  Spring forward: last Sunday of March (2024-03-31 02:00 -> 03:00)
    #  Fall back: last Sunday of October (2024-10-27 03:00 -> 02:00)
    # ----------------------------------------------------------------

    def test_dst_berlin(self):
        """Quarter interval under Europe/Berlin (has DST).

        Verify that quarter alias behavior matches `3n` in a timezone with DST
        transitions.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Europe/Berlin")
        self._create_db_and_table()
        self._insert_quarterly_data()
        rows = self._assert_1q_equals_3n("Europe/Berlin")

        assert len(rows) == 4, f"Expected 4 quarters, got {len(rows)}"

    @pytest.mark.xfail(
        reason=SESSION_TZ_BOUNDARY_XFAIL,
        strict=True,
    )
    def test_session_timezone_controls_quarter_boundary_berlin(self):
        """Known-bug regression: Europe/Berlin should use Berlin quarter starts.

        Verify the intended Berlin-local quarter boundary behavior. This case
        remains xfail until the timezone propagation bug is fixed.

        This case uses a DST timezone so the expected values also verify that
        quarter boundaries are aligned in local time rather than anchored to a
        fixed offset from another timezone.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Europe/Berlin")
        self._create_db_and_table()
        self._insert_quarterly_data()
        self._assert_local_quarter_starts(
            "Europe/Berlin",
            [
                "2024-01-01T00:00:00.000+0100",
                "2024-04-01T00:00:00.000+0200",
                "2024-07-01T00:00:00.000+0200",
                "2024-10-01T00:00:00.000+0200",
            ],
        )

    def test_dst_spring_transition_berlin(self):
        """Data around spring DST transition (March 31, 2024) in Europe/Berlin.

        Verifies no duplicate or missing windows at the DST boundary.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Europe/Berlin")
        self._create_db_and_table()

        # Insert data around the spring DST transition
        rows = [
            ("2024-03-30 12:00:00", 1),   # before spring forward
            ("2024-03-31 12:00:00", 2),   # day of spring forward
            ("2024-04-01 12:00:00", 3),   # after spring forward (Q2 starts)
            ("2024-04-15 12:00:00", 4),
            ("2024-06-15 12:00:00", 5),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into t1 values ('{ts}', {val})")

        self._assert_1q_equals_3n("Berlin-spring-DST")

    def test_dst_autumn_transition_berlin(self):
        """Data around autumn DST transition (October 27, 2024) in Europe/Berlin.

        Verifies no duplicate or missing windows at the DST boundary.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Europe/Berlin")
        self._create_db_and_table()

        rows = [
            ("2024-09-15 12:00:00", 1),
            ("2024-10-26 12:00:00", 2),   # before fall back
            ("2024-10-27 12:00:00", 3),   # day of fall back
            ("2024-10-28 12:00:00", 4),   # after fall back (still Q4)
            ("2024-12-15 12:00:00", 5),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into t1 values ('{ts}', {val})")

        self._assert_1q_equals_3n("Berlin-autumn-DST")

    # ----------------------------------------------------------------
    #  Session timezone switches between write-time and read-time
    # ----------------------------------------------------------------

    def test_write_shanghai_read_berlin(self):
        """Write under Asia/Shanghai, then query under Europe/Berlin in one session.

        Verify that write-time and read-time timezone switches do not break the
        `1q == 3n` behavior.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._create_db_and_table()

        # Insert data under Shanghai timezone (no DST)
        self._set_timezone("Asia/Shanghai")
        self._insert_quarterly_data()

        # Query under Berlin timezone (has DST)
        self._set_timezone("Europe/Berlin")
        self._assert_1q_equals_3n("Shanghai-write/Berlin-read")

    def test_write_berlin_read_shanghai(self):
        """Write under Europe/Berlin, then query under Asia/Shanghai in one session.

        Verify that the reverse write/read timezone switch also preserves the
        `1q == 3n` behavior.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._create_db_and_table()

        self._set_timezone("Europe/Berlin")
        self._insert_quarterly_data()

        self._set_timezone("Asia/Shanghai")
        self._assert_1q_equals_3n("Berlin-write/Shanghai-read")

    def test_cross_dst_boundary_data(self):
        """Client in summer time, data spans winter-summer transition.

        Ensures quarter windows are correct across DST boundary.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._set_timezone("Europe/Berlin")
        self._create_db_and_table()

        # Data in winter time (CET, UTC+1)
        tdSql.execute("insert into t1 values ('2024-01-15 12:00:00', 1)")
        tdSql.execute("insert into t1 values ('2024-02-15 12:00:00', 2)")
        # Data crosses into summer time (CEST, UTC+2)
        tdSql.execute("insert into t1 values ('2024-04-15 12:00:00', 3)")
        tdSql.execute("insert into t1 values ('2024-05-15 12:00:00', 4)")

        rows = self._assert_1q_equals_3n("Berlin-cross-DST")

        # Should have 2 quarters: Q1 and Q2
        assert len(rows) == 2, f"Expected 2 quarters, got {len(rows)}"
