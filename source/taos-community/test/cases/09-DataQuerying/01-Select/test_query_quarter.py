"""
Quarter query tests for duration alias (q/Q = 3n).

Covers:
    - interval(1q) vs interval(3n) result equivalence
    - _wstart / _wend quarter boundary alignment (Q1-Q4)
    - offset + interval(1q) vs offset + interval(3n)
    - leap year (2024-02-29)
    - case insensitivity (1Q)
    - negative cases: sliding(1q), every(1q)
    - scope-lock: non-INTERVAL query-side duration literal with q
    - timezone and DST behavior in Asia/Shanghai and Europe/Berlin
    - session timezone switches between write-time and read-time
"""

import pytest

from new_test_framework.utils import tdLog, tdSql


QUARTER_TEST_DB = "test_quarter"
_quarter_db_initialized = False


def _ensure_quarter_db():
    global _quarter_db_initialized

    if not _quarter_db_initialized:
        tdSql.execute(f"drop database if exists {QUARTER_TEST_DB}")
        tdSql.execute(f"create database {QUARTER_TEST_DB} precision 'ms'")
        _quarter_db_initialized = True

    tdSql.execute(f"use {QUARTER_TEST_DB}")


class TestQuarterInterval:
    DB = QUARTER_TEST_DB
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
        _ensure_quarter_db()

    def setup_method(self, method):
        _ensure_quarter_db()
        self._set_timezone(self.DEFAULT_TZ)
        self._case_name = method.__name__.removeprefix("test_")

    def _table_name(self, suffix="data"):
        return f"quarter_{self._case_name}_{suffix}"

    def _tz_table_name(self, suffix="data"):
        return f"quarter_tz_{self._case_name}_{suffix}"

    def _set_timezone(self, timezone):
        tdSql.execute(f"alter local 'timezone {timezone}'")

    def _prepare_data(self, table_name=None):
        table_name = table_name or self._table_name()
        tdSql.execute(f"drop table if exists {table_name}")
        tdSql.execute(f"create table {table_name} (ts timestamp, val int)")
        # Insert one row per month from 2024-01 to 2025-03 (15 months)
        # Each row at the 15th of the month, 12:00, spacing > 30 min
        rows = [
            ("2024-01-15 12:00:00", 1),
            ("2024-02-15 12:00:00", 2),
            ("2024-02-29 12:00:00", 29),   # leap day
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
            ("2025-01-15 12:00:00", 13),
            ("2025-02-15 12:00:00", 14),
            ("2025-03-15 12:00:00", 15),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into {table_name} values ('{ts}', {val})")

        return table_name

    # ----------------------------------------------------------------
    #  Positive cases
    # ----------------------------------------------------------------

    def test_interval_1q_equals_3n(self):
        """interval(1q) must produce identical results to interval(3n)

        Verify that the quarter alias `1q` returns the same window result as
        the normalized calendar duration `3n`.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(3n)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(1q)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q) != interval(3n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    def test_wstart_wend_quarter_boundaries(self):
        """_wstart should align to Q1(Jan), Q2(Apr), Q3(Jul), Q4(Oct)

        Verify that quarter windows start on the expected Jan/Apr/Jul/Oct
        boundaries and remain aligned after `q` is normalized to `n`.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*) "
            f"from {table_name} interval(1q) order by _wstart"
        )
        # Expected boundaries: 2024-Q1, Q2, Q3, Q4, 2025-Q1
        expected_starts = [
            "2024-01-01",
            "2024-04-01",
            "2024-07-01",
            "2024-10-01",
            "2025-01-01",
        ]
        tdSql.checkRows(5)
        for i, expected in enumerate(expected_starts):
            wstart = str(tdSql.getData(i, 0))
            assert wstart.startswith(expected), (
                f"row {i}: expected _wstart starting with {expected}, got {wstart}"
            )

    def test_offset_1q_equals_offset_3n(self):
        """offset(1n) + interval(1q) must equal offset(1n) + interval(3n)

        Verify that applying the same month offset to `1q` and `3n` produces
        identical quarter window results.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(3n, 1n)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(1q, 1n)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q, 1n) != interval(3n, 1n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    def test_leap_year_quarter(self):
        """2024-02-29 must belong to Q1 (2024-01-01 ~ 2024-04-01)

        Verify that leap-day data is grouped into the correct first-quarter
        window when using the quarter alias.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            f"select _wstart, count(*) from {table_name} "
            "where ts >= '2024-02-29' and ts < '2024-03-01' "
            "interval(1q)"
        )
        tdSql.checkRows(1)
        wstart = str(tdSql.getData(0, 0))
        assert wstart.startswith("2024-01-01"), (
            f"leap day should be in Q1, got _wstart={wstart}"
        )

    def test_case_insensitive_Q(self):
        """1Q (upper case) must work identically to 1q

        Verify that the quarter alias is case-insensitive and accepts both
        uppercase and lowercase suffixes.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            f"select _wstart, count(*) from {table_name} interval(1q)"
        )
        rows_lower = tdSql.queryResult

        tdSql.query(
            f"select _wstart, count(*) from {table_name} interval(1Q)"
        )
        rows_upper = tdSql.queryResult

        assert rows_lower == rows_upper, (
            f"1Q != 1q\nlower: {rows_lower}\nupper: {rows_upper}"
        )

    def test_offset_84d_with_1q(self):
        """offset 84d with 1q must be rejected with the expected boundary error.

        Verify that an oversized fixed-day offset is rejected for `1q` with
        the same boundary error used for `3n`.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        expected = "Interval offset should be shorter than interval"
        err_3n = tdSql.error(f"select _wstart, count(*) from {table_name} interval(3n, 84d)")
        err_1q = tdSql.error(f"select _wstart, count(*) from {table_name} interval(1q, 84d)")

        assert expected in err_3n, f"unexpected 3n error: {err_3n}"
        assert expected in err_1q, f"unexpected 1q error: {err_1q}"

    def test_offset_equal_interval_matches_3n(self):
        """offset equal to interval must be rejected with the expected boundary error.

        Verify that `offset == interval` is rejected consistently for both the
        quarter alias and its normalized month representation.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        expected = "Interval offset should be shorter than interval"
        err_3n = tdSql.error(f"select _wstart, count(*) from {table_name} interval(3n, 3n)")
        err_1q = tdSql.error(f"select _wstart, count(*) from {table_name} interval(1q, 1q)")

        assert expected in err_3n, f"unexpected 3n error: {err_3n}"
        assert expected in err_1q, f"unexpected 1q error: {err_1q}"

    def test_auto_offset_1q_equals_auto_offset_3n(self):
        """AUTO offset with 1q must behave identically to AUTO offset with 3n.

        Verify that automatic offset calculation keeps `1q` behavior aligned
        with `3n` in interval queries.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} where ts >= '2024-02-15 12:00:00' interval(3n, auto)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} where ts >= '2024-02-15 12:00:00' interval(1q, auto)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q, auto) != interval(3n, auto)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    @pytest.mark.parametrize(
        "quarters,months",
        [
            (2, 6),
            (3, 9),
            (4, 12),
        ],
    )
    def test_nq_equals_3n_multiple(self, quarters, months):
        """Nq (N>1) must produce identical results to 3N months

        Verify that multi-quarter input for different N values is normalized
        correctly and matches the equivalent multi-month interval.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({months}n)"
        )
        rows_months = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({quarters}q)"
        )
        rows_quarters = tdSql.queryResult

        assert rows_months == rows_quarters, (
            f"interval({quarters}q) != interval({months}n)\n"
            f"{months}n: {rows_months}\n{quarters}q: {rows_quarters}"
        )

    @pytest.mark.parametrize(
        "quarters,months,offset_q,offset_n",
        [
            (2, 6, 1, 3),     # 2q == 6n, 1q offset (3n) < 6n interval
            (3, 9, 1, 3),     # 3q == 9n, 1q offset (3n) < 9n interval
            (3, 9, 2, 6),     # 3q == 9n, 2q offset (6n) < 9n interval
            (4, 12, 1, 3),    # 4q == 12n, 1q offset (3n) < 12n interval
        ],
    )
    def test_nq_with_offset_unit_q(self, quarters, months, offset_q, offset_n):
        """Nq with q-unit offset must match 3Nn with corresponding n-unit offset

        Verify that interval(Nq, Mq) produces identical results to
        interval(3N*n, M*3*n) where M*3 months = Mq and offset < interval.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({months}n, {offset_n}n)"
        )
        rows_n_offset = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({quarters}q, {offset_q}q)"
        )
        rows_q_offset = tdSql.queryResult

        assert rows_n_offset == rows_q_offset, (
            f"interval({quarters}q, {offset_q}q) != interval({months}n, {offset_n}n)\n"
            f"{months}n offset {offset_n}n: {rows_n_offset}\n"
            f"{quarters}q offset {offset_q}q: {rows_q_offset}"
        )

    @pytest.mark.parametrize(
        "quarters,months,offset_w",
        [
            (2, 6, 1),
            (2, 6, 2),
            (3, 9, 1),
            (4, 12, 1),
        ],
    )
    def test_nq_with_offset_unit_w(self, quarters, months, offset_w):
        """Nq with w-unit offset must match 3Nn with same w-unit offset

        Verify that interval(Nq, Mw) produces identical results to
        interval(3N*n, Mw) (week offset is independent of month/quarter).

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({months}n, {offset_w}w)"
        )
        rows_n_offset = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({quarters}q, {offset_w}w)"
        )
        rows_q_offset = tdSql.queryResult

        assert rows_n_offset == rows_q_offset, (
            f"interval({quarters}q, {offset_w}w) != interval({months}n, {offset_w}w)\n"
            f"{months}n offset {offset_w}w: {rows_n_offset}\n"
            f"{quarters}q offset {offset_w}w: {rows_q_offset}"
        )

    @pytest.mark.parametrize(
        "quarters,months,offset_n",
        [
            (2, 6, 1),
            (2, 6, 2),
            (3, 9, 1),
            (4, 12, 1),
        ],
    )
    def test_nq_with_offset_unit_n(self, quarters, months, offset_n):
        """Nq with n-unit offset must match 3Nn with same n-unit offset

        Verify that interval(Nq, Mn) produces identical results to
        interval(3N*n, Mn).

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({months}n, {offset_n}n)"
        )
        rows_n_offset = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval({quarters}q, {offset_n}n)"
        )
        rows_q_offset = tdSql.queryResult

        assert rows_n_offset == rows_q_offset, (
            f"interval({quarters}q, {offset_n}n) != interval({months}n, {offset_n}n)\n"
            f"{months}n offset {offset_n}n: {rows_n_offset}\n"
            f"{quarters}q offset {offset_n}n: {rows_q_offset}"
        )

    # ----------------------------------------------------------------
    #  Negative cases
    # ----------------------------------------------------------------

    def test_sliding_1q_rejected(self):
        """sliding(1q) should be rejected (calendar unit not allowed in sliding)

        Verify that the quarter alias remains subject to the existing rule that
        calendar units are not allowed in `SLIDING`.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()
        tdSql.error(
            f"select _wstart, count(*) from {table_name} interval(1q) sliding(1q)"
        )

    def test_every_1q_rejected(self):
        """every(1q) should be rejected (calendar unit not allowed in EVERY)

        Verify that the quarter alias is rejected in `EVERY` for the same
        calendar-unit reason as `3n`.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        interp_table = self._table_name("interp")
        tdSql.execute(f"drop table if exists {interp_table}")
        tdSql.execute(f"create table {interp_table} (ts timestamp, val int)")
        tdSql.execute(
            f"insert into {interp_table} values ('2024-01-15 12:00:00', 1)"
        )
        tdSql.execute(
            f"insert into {interp_table} values ('2024-06-15 12:00:00', 2)"
        )
        tdSql.error(
            f"select interp(val) from {interp_table} "
            "range('2024-01-01', '2024-12-31') every(1q) fill(linear)"
        )

    # ----------------------------------------------------------------
    #  Scope-lock: q rejected in absolute-duration contexts
    # ----------------------------------------------------------------

    def test_state_duration_q_rejected(self):
        """q in absolute-duration context (stateduration) must be rejected.

        Since q is normalized to 3n (calendar unit) in parseNatualDuration,
        and stateduration calls parseAbsoluteDuration which rejects calendar
        units, this verifies q does not accidentally bypass that check.

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        table_name = self._prepare_data()
        tdSql.error(
            f"select stateduration(val, 'GT', 0, 1q) from {table_name}"
        )


    def _create_table(self, table_name=None):
        table_name = table_name or self._tz_table_name()
        tdSql.execute(f"drop table if exists {table_name}")
        tdSql.execute(f"create table {table_name} (ts timestamp, val int)")
        return table_name

    def _insert_quarterly_data(self, table_name):
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
            tdSql.execute(f"insert into {table_name} values ('{ts}', {val})")

    def _assert_1q_equals_3n(self, table_name, label) -> list:
        """Assert interval(1q) == interval(3n) results."""
        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(3n) order by _wstart"
        )
        rows_3n = tdSql.queryResult if tdSql.queryResult is not None else []

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            f"from {table_name} interval(1q) order by _wstart"
        )
        rows_1q: list = tdSql.queryResult if tdSql.queryResult is not None else []

        assert rows_3n == rows_1q, (
            f"[{label}] interval(1q) != interval(3n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )
        return rows_1q

    def _assert_local_quarter_starts(self, table_name, label, expected_starts):
        tdSql.query(
            "select TO_ISO8601(_wstart), count(*) "
            f"from {table_name} interval(1q) order by _wstart"
        )

        rows = tdSql.queryResult if tdSql.queryResult is not None else []
        actual_starts = [row[0] for row in rows]

        assert actual_starts == expected_starts, (
            f"[{label}] unexpected quarter starts\n"
            f"expected: {expected_starts}\n"
            f"actual:   {actual_starts}"
        )

    def _assert_local_quarter_start_dates(self, table_name, label, expected_dates):
        tdSql.query(
            "select TO_ISO8601(_wstart), count(*) "
            f"from {table_name} interval(1q) order by _wstart"
        )

        rows = tdSql.queryResult if tdSql.queryResult is not None else []
        actual_dates = [row[0][:10] for row in rows]

        assert actual_dates == expected_dates, (
            f"[{label}] unexpected quarter start dates\n"
            f"expected: {expected_dates}\n"
            f"actual:   {actual_dates}"
        )

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
        table_name = self._create_table()
        self._insert_quarterly_data(table_name)
        rows = self._assert_1q_equals_3n(table_name, "Asia/Shanghai")

        self._assert_local_quarter_start_dates(
            table_name,
            "Asia/Shanghai",
            [
                "2024-01-01",
                "2024-04-01",
                "2024-07-01",
                "2024-10-01",
            ],
        )

        self._assert_local_quarter_starts(
            table_name,
            "Asia/Shanghai",
            [
                "2024-01-01T00:00:00.000+0800",
                "2024-04-01T00:00:00.000+0800",
                "2024-07-01T00:00:00.000+0800",
                "2024-10-01T00:00:00.000+0800",
            ],
        )

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
        table_name = self._create_table()
        self._insert_quarterly_data(table_name)
        self._assert_local_quarter_starts(
            table_name,
            "UTC",
            [
                "2024-01-01T00:00:00.000+0000",
                "2024-04-01T00:00:00.000+0000",
                "2024-07-01T00:00:00.000+0000",
                "2024-10-01T00:00:00.000+0000",
            ],
        )

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
        table_name = self._create_table()
        self._insert_quarterly_data(table_name)
        rows = self._assert_1q_equals_3n(table_name, "Europe/Berlin")

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
        table_name = self._create_table()
        self._insert_quarterly_data(table_name)
        self._assert_local_quarter_starts(
            table_name,
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
        table_name = self._create_table()

        rows = [
            ("2024-03-30 12:00:00", 1),
            ("2024-03-31 12:00:00", 2),
            ("2024-04-01 12:00:00", 3),
            ("2024-04-15 12:00:00", 4),
            ("2024-06-15 12:00:00", 5),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into {table_name} values ('{ts}', {val})")

        self._assert_1q_equals_3n(table_name, "Berlin-spring-DST")

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
        table_name = self._create_table()

        rows = [
            ("2024-09-15 12:00:00", 1),
            ("2024-10-26 12:00:00", 2),
            ("2024-10-27 12:00:00", 3),
            ("2024-10-28 12:00:00", 4),
            ("2024-12-15 12:00:00", 5),
        ]
        for ts, val in rows:
            tdSql.execute(f"insert into {table_name} values ('{ts}', {val})")

        self._assert_1q_equals_3n(table_name, "Berlin-autumn-DST")

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
        table_name = self._create_table()

        self._set_timezone("Asia/Shanghai")
        self._insert_quarterly_data(table_name)

        self._set_timezone("Europe/Berlin")
        self._assert_1q_equals_3n(table_name, "Shanghai-write/Berlin-read")

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
        table_name = self._create_table()

        self._set_timezone("Europe/Berlin")
        self._insert_quarterly_data(table_name)

        self._set_timezone("Asia/Shanghai")
        self._assert_1q_equals_3n(table_name, "Berlin-write/Shanghai-read")

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
        table_name = self._create_table()

        tdSql.execute(f"insert into {table_name} values ('2024-01-15 12:00:00', 1)")
        tdSql.execute(f"insert into {table_name} values ('2024-02-15 12:00:00', 2)")
        tdSql.execute(f"insert into {table_name} values ('2024-04-15 12:00:00', 3)")
        tdSql.execute(f"insert into {table_name} values ('2024-05-15 12:00:00', 4)")

        rows = self._assert_1q_equals_3n(table_name, "Berlin-cross-DST")

        assert len(rows) == 2, f"Expected 2 quarters, got {len(rows)}"
