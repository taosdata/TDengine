"""
Tests for quarter duration alias (q/Q = 3n) in interval queries.

Covers:
  - interval(1q) vs interval(3n) result equivalence
  - _wstart / _wend quarter boundary alignment (Q1-Q4)
  - offset + interval(1q) vs offset + interval(3n)
  - leap year (2024-02-29)
  - case insensitivity (1Q)
  - negative cases: sliding(1q), every(1q)
  - scope-lock: non-INTERVAL query-side duration literal with q
"""

from new_test_framework.utils import tdLog, tdSql


class TestQuarterInterval:
    DB = "test_quarter"

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _prepare_data(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB}")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute(
            "create table t1 (ts timestamp, val int)"
        )
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
            tdSql.execute(f"insert into t1 values ('{ts}', {val})")

    # ----------------------------------------------------------------
    #  Positive cases
    # ----------------------------------------------------------------

    def test_interval_1q_equals_3n(self):
        """interval(1q) must produce identical results to interval(3n)

        Catalog:
            - Query:Interval

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Tony Zhang Created

        """
        self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(3n)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(1q)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q) != interval(3n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    def test_wstart_wend_quarter_boundaries(self):
        """_wstart should align to Q1(Jan), Q2(Apr), Q3(Jul), Q4(Oct)"""
        self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*) "
            "from t1 interval(1q) order by _wstart"
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
        """offset(1n) + interval(1q) must equal offset(1n) + interval(3n)"""
        self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(3n, 1n)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(1q, 1n)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q, 1n) != interval(3n, 1n)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    def test_leap_year_quarter(self):
        """2024-02-29 must belong to Q1 (2024-01-01 ~ 2024-04-01)"""
        self._prepare_data()

        tdSql.query(
            "select _wstart, count(*) from t1 "
            "where ts >= '2024-02-29' and ts < '2024-03-01' "
            "interval(1q)"
        )
        tdSql.checkRows(1)
        wstart = str(tdSql.getData(0, 0))
        assert wstart.startswith("2024-01-01"), (
            f"leap day should be in Q1, got _wstart={wstart}"
        )

    def test_case_insensitive_Q(self):
        """1Q (upper case) must work identically to 1q"""
        self._prepare_data()

        tdSql.query(
            "select _wstart, count(*) from t1 interval(1q)"
        )
        rows_lower = tdSql.queryResult

        tdSql.query(
            "select _wstart, count(*) from t1 interval(1Q)"
        )
        rows_upper = tdSql.queryResult

        assert rows_lower == rows_upper, (
            f"1Q != 1q\nlower: {rows_lower}\nupper: {rows_upper}"
        )

    def test_offset_84d_with_1q(self):
        """offset 84d with 1q must be rejected with the expected boundary error."""
        self._prepare_data()

        expected = "Interval offset should be shorter than interval"
        err_3n = tdSql.error("select _wstart, count(*) from t1 interval(3n, 84d)")
        err_1q = tdSql.error("select _wstart, count(*) from t1 interval(1q, 84d)")

        assert expected in err_3n, f"unexpected 3n error: {err_3n}"
        assert expected in err_1q, f"unexpected 1q error: {err_1q}"

    def test_offset_equal_interval_matches_3n(self):
        """offset equal to interval must be rejected with the expected boundary error."""
        self._prepare_data()

        expected = "Interval offset should be shorter than interval"
        err_3n = tdSql.error("select _wstart, count(*) from t1 interval(3n, 3n)")
        err_1q = tdSql.error("select _wstart, count(*) from t1 interval(1q, 1q)")

        assert expected in err_3n, f"unexpected 3n error: {err_3n}"
        assert expected in err_1q, f"unexpected 1q error: {err_1q}"

    def test_auto_offset_1q_equals_auto_offset_3n(self):
        """AUTO offset with 1q must behave identically to AUTO offset with 3n."""
        self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 where ts >= '2024-02-15 12:00:00' interval(3n, auto)"
        )
        rows_3n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 where ts >= '2024-02-15 12:00:00' interval(1q, auto)"
        )
        rows_1q = tdSql.queryResult

        assert rows_3n == rows_1q, (
            f"interval(1q, auto) != interval(3n, auto)\n"
            f"3n: {rows_3n}\n1q: {rows_1q}"
        )

    def test_2q_equals_6n(self):
        """2q must produce identical results to 6n"""
        self._prepare_data()

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(6n)"
        )
        rows_6n = tdSql.queryResult

        tdSql.query(
            "select _wstart, _wend, count(*), sum(val) "
            "from t1 interval(2q)"
        )
        rows_2q = tdSql.queryResult

        assert rows_6n == rows_2q, (
            f"interval(2q) != interval(6n)\n"
            f"6n: {rows_6n}\n2q: {rows_2q}"
        )

    # ----------------------------------------------------------------
    #  Negative cases
    # ----------------------------------------------------------------

    def test_sliding_1q_rejected(self):
        """sliding(1q) should be rejected (calendar unit not allowed in sliding)"""
        self._prepare_data()
        tdSql.error(
            "select _wstart, count(*) from t1 interval(1q) sliding(1q)"
        )

    def test_every_1q_rejected(self):
        """every(1q) should be rejected (calendar unit not allowed in EVERY)"""
        self._prepare_data()
        tdSql.execute(
            "create table t_interp (ts timestamp, val int)"
        )
        tdSql.execute(
            "insert into t_interp values ('2024-01-15 12:00:00', 1)"
        )
        tdSql.execute(
            "insert into t_interp values ('2024-06-15 12:00:00', 2)"
        )
        tdSql.error(
            "select interp(val) from t_interp "
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
        """
        self._prepare_data()
        tdSql.error(
            "select stateduration(val, 'GT', 0, 1q) from t1"
        )
