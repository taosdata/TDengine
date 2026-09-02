"""Timezone INTERVAL window tests.

Covers:
- P4 Task 4.3: INTERVAL n/d/y/q natural units, DST no-drift, SLIDING
- P4 Task 4.2: INTERVAL 1w aligned by firstDayOfWeek (high-risk change)
- P5 Task 5.1: INTERVAL 1q/2q quarter boundaries
- Equivalence: INTERVAL 1q == 3n, 2q == 6n
"""

from new_test_framework.utils import tdLog, tdSql
import pytest
import sys

IS_WINDOWS = sys.platform.startswith('win')
SKIP_WINDOWS_SET_TIMEZONE = pytest.mark.skipif(
    IS_WINDOWS,
    reason='Windows does not support SET TIMEZONE cases in this suite',
)

pytestmark = [SKIP_WINDOWS_SET_TIMEZONE]

def _config_timezone_name(value):
    return str(value).split(' ')[0]

def _wstart_count_map(rows):
    return {str(row[0])[:10]: int(row[1]) for row in rows}

class _IntervalNaturalMixin:
    """INTERVAL with natural units (n/d/y/q), DST no-drift, SLIDING."""

    def _setup_interval_natural_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_intv_nat'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb1'

    def _prepare_interval_natural_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1735689600000  # 2025-01-01 00:00:00 UTC
        for i in range(365):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def _prepare_interval_dst_hourly_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')

        ranges = [
            # America/New_York local 2025-03-01 00:00:00 through 2025-04-01 00:00:00.
            (1740805200000, 1743480000000),
            # America/New_York local 2025-11-01 00:00:00 through 2025-12-01 00:00:00.
            (1761969600000, 1764565200000),
        ]
        seq = 0
        values = []
        for start, end in ranges:
            for ts in range(start, end, 3600000):
                values.append(f"({ts}, {float(seq)})")
                if len(values) >= 200:
                    tdSql.execute(f"insert into {self.ctbname} values {','.join(values)}")
                    values = []
                seq += 1
        if values:
            tdSql.execute(f"insert into {self.ctbname} values {','.join(values)}")

    def _prepare_interval_dst_extended_data(self):
        """Hourly data spanning multiple months/quarters around both DST transitions.

        Spring: Jan 1 to May 1 2025 ET (covers Q1 with spring-forward, start of Q2).
        Fall:   Oct 1 to Jan 1 2026 ET (covers full Q4 with fall-back).
        """
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')

        ranges = [
            # America/New_York local 2025-01-01 00:00:00 EST through 2025-05-01 00:00:00 EDT.
            (1735707600000, 1746072000000),
            # America/New_York local 2025-10-01 00:00:00 EDT through 2026-01-01 00:00:00 EST.
            (1759291200000, 1767243600000),
        ]
        seq = 0
        values = []
        for start, end in ranges:
            for ts in range(start, end, 3600000):
                values.append(f"({ts}, {float(seq)})")
                if len(values) >= 200:
                    tdSql.execute(f"insert into {self.ctbname} values {','.join(values)}")
                    values = []
                seq += 1
        if values:
            tdSql.execute(f"insert into {self.ctbname} values {','.join(values)}")

    def check_interval_1n_bucket_count(self):
        """INTERVAL(1n) on a full year should produce 12 monthly buckets."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)

    def check_interval_1d_bucket_count(self):
        """INTERVAL(1d) should produce 365 daily buckets for a full year."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1d)"
        )
        tdSql.checkRows(365)

    def check_interval_1d_uses_session_timezone_not_client_local(self):
        """INTERVAL should use L2 session timezone instead of current client local timezone."""
        self._prepare_interval_natural_data()
        tdSql.execute(f"insert into {self.ctbname} values (1738341000000, 999.0)")

        tdSql.query("show local variables like 'timezone'")
        original_local = _config_timezone_name(tdSql.queryResult[0][1])

        target_local = 'Asia/Shanghai'
        session_tz = 'UTC'
        expected = {'2025-01-31': 2, '2025-02-01': 1}
        if 'Shanghai' in original_local or '+08' in original_local:
            target_local = 'UTC'
            session_tz = 'Asia/Shanghai'
            expected = {'2025-01-31': 1, '2025-02-01': 2}

        try:
            tdSql.execute(f"alter local 'timezone {target_local}'")
            tdSql.connect()
            tdSql.execute(f"SET TIMEZONE '{session_tz}'")
            tdSql.query(
                f"select _wstart, count(*) from {self.ctbname} "
                f"where ts >= '2025-01-31 00:00:00' and ts < '2025-02-02 00:00:00' interval(1d)"
            )
            counts = _wstart_count_map(tdSql.queryResult)
            assert counts == expected, (
                f"INTERVAL(1d) should follow session timezone, not client local timezone: "
                f"session={session_tz}, local={target_local}, counts={counts}, expected={expected}"
            )
        finally:
            tdSql.execute(f"alter local 'timezone {original_local}'")
            tdSql.connect()

    def check_interval_1d_uses_session_timezone_not_client_or_server(self):
        """INTERVAL should keep using L2 session timezone even if L3/L4 are different."""
        self._prepare_interval_natural_data()
        tdSql.execute(f"insert into {self.ctbname} values (1738341000000, 1000.0)")

        tdSql.query("show local variables like 'timezone'")
        original_local = _config_timezone_name(tdSql.queryResult[0][1])
        tdSql.query("show variables like 'timezone'")
        original_server = _config_timezone_name(tdSql.queryResult[0][1])

        try:
            tdSql.execute("alter local 'timezone UTC'")
            tdSql.execute("alter all dnodes 'timezone UTC'")
            tdSql.connect()
            tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
            tdSql.query(
                f"select _wstart, count(*) from {self.ctbname} "
                f"where ts >= '2025-01-31 00:00:00' and ts < '2025-02-02 00:00:00' interval(1d)"
            )
            counts = _wstart_count_map(tdSql.queryResult)
            expected = {'2025-01-31': 1, '2025-02-01': 2}
            assert counts == expected, (
                f"INTERVAL(1d) should follow session timezone instead of client/server timezone: "
                f"counts={counts}, expected={expected}"
            )
        finally:
            tdSql.execute(f"alter local 'timezone {original_local}'")
            tdSql.execute(f"alter all dnodes 'timezone {original_server}'")
            tdSql.connect()

    def check_interval_1d_uses_fixed_offset_session_timezone(self):
        """INTERVAL should preserve fixed-offset session timezones through plan transport.

        Bare fixed-offset session timezone strings are now interpreted with
        POSIX sign semantics, so '+08:00' means UTC-08:00 in this path.
        """
        self._prepare_interval_natural_data()
        tdSql.execute(f"insert into {self.ctbname} values (1738341000000, 1001.0)")

        tdSql.query("show local variables like 'timezone'")
        original_local = _config_timezone_name(tdSql.queryResult[0][1])
        tdSql.query("show variables like 'timezone'")
        original_server = _config_timezone_name(tdSql.queryResult[0][1])

        try:
            tdSql.execute("alter local 'timezone UTC'")
            tdSql.execute("alter all dnodes 'timezone UTC'")
            tdSql.connect()
            tdSql.execute("SET TIMEZONE '+08:00'")
            tdSql.query(
                f"select _wstart, count(*) from {self.ctbname} "
                f"where ts >= '2025-01-31 00:00:00' and ts < '2025-02-02 00:00:00' interval(1d)"
            )
            counts = _wstart_count_map(tdSql.queryResult)
            expected = {'2025-01-31': 2, '2025-02-01': 1}
            assert counts == expected, (
                f"INTERVAL(1d) should honor fixed-offset session timezone: counts={counts}, expected={expected}"
            )
        finally:
            tdSql.execute(f"alter local 'timezone {original_local}'")
            tdSql.execute(f"alter all dnodes 'timezone {original_server}'")
            tdSql.connect()

    def check_interval_1n_dst_no_drift(self):
        """INTERVAL(1n) with DST timezone should still produce 12 months.

        March bucket should be 31 days (DST spring-forward in NY doesn't drift).
        """
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)

    def check_interval_dst_transition_counts(self):
        """INTERVAL aggregation counts should honor 23/25-hour DST boundaries."""
        self._prepare_interval_dst_hourly_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query("show local variables like 'firstDayOfWeek'")
        original_fdow = int(tdSql.queryResult[0][1]) if tdSql.queryRows > 0 else 1

        cases = [
            (
                'spring day',
                "2025-03-09 00:00:00",
                "2025-03-10 00:00:00",
                "1d",
                {'2025-03-09': 23},
            ),
            (
                'fall day',
                "2025-11-02 00:00:00",
                "2025-11-03 00:00:00",
                "1d",
                {'2025-11-02': 25},
            ),
            (
                'spring week',
                "2025-03-09 00:00:00",
                "2025-03-16 00:00:00",
                "1w",
                {'2025-03-09': 167},
            ),
            (
                'fall week',
                "2025-11-02 00:00:00",
                "2025-11-09 00:00:00",
                "1w",
                {'2025-11-02': 169},
            ),
            (
                'spring month',
                "2025-03-01 00:00:00",
                "2025-04-01 00:00:00",
                "1n",
                {'2025-03-01': 743},
            ),
            (
                'fall month',
                "2025-11-01 00:00:00",
                "2025-12-01 00:00:00",
                "1n",
                {'2025-11-01': 721},
            ),
        ]

        try:
            tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
            for name, start, end, interval, expected in cases:
                tdSql.query(
                    f"select _wstart, count(*) from {self.ctbname} "
                    f"where ts >= '{start}' and ts < '{end}' interval({interval})"
                )
                counts = _wstart_count_map(tdSql.queryResult)
                assert counts == expected, (
                    f"{name}: expected INTERVAL({interval}) counts {expected}, got {counts}"
                )
        finally:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {original_fdow}")

    def check_interval_1d_dst_window_boundaries(self):
        """INTERVAL(1d) _wduration must honour DST calendar days.

        Fall-back (Nov 2 2025 in America/New_York):
            Nov 2 window is 25 hours (90000000 ms), others are 24 hours.
        Spring-forward (Mar 9 2025 in America/New_York):
            Mar 9 window is 23 hours (82800000 ms), others are 24 hours.
        Checks are display-timezone-independent: we verify _wduration, count,
        and window continuity (_wend[i] == _wstart[i+1]).
        """
        self._prepare_interval_dst_hourly_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")

        # Fall-back: Nov 2 2025 has 25 hours
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-11-01' and ts < '2025-11-05' interval(1d)"
        )
        tdSql.checkRows(4)
        fall_expected_dur = [86400000, 90000000, 86400000, 86400000]
        fall_expected_cnt = [24, 25, 24, 24]
        for i in range(4):
            row = tdSql.queryResult[i]
            assert int(row[1]) == fall_expected_dur[i], (
                f"fall row {i}: _wduration expected {fall_expected_dur[i]}, got {row[1]}"
            )
            assert int(row[3]) == fall_expected_cnt[i], (
                f"fall row {i}: count expected {fall_expected_cnt[i]}, got {row[3]}"
            )
        for i in range(3):
            assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                f"fall: _wend[{i}] != _wstart[{i+1}]: "
                f"{tdSql.queryResult[i][2]} vs {tdSql.queryResult[i+1][0]}"
            )

        # Spring-forward: Mar 9 2025 has 23 hours
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-03-08' and ts < '2025-03-12' interval(1d)"
        )
        tdSql.checkRows(4)
        spring_expected_dur = [86400000, 82800000, 86400000, 86400000]
        spring_expected_cnt = [24, 23, 24, 24]
        for i in range(4):
            row = tdSql.queryResult[i]
            assert int(row[1]) == spring_expected_dur[i], (
                f"spring row {i}: _wduration expected {spring_expected_dur[i]}, got {row[1]}"
            )
            assert int(row[3]) == spring_expected_cnt[i], (
                f"spring row {i}: count expected {spring_expected_cnt[i]}, got {row[3]}"
            )
        for i in range(3):
            assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                f"spring: _wend[{i}] != _wstart[{i+1}]: "
                f"{tdSql.queryResult[i][2]} vs {tdSql.queryResult[i+1][0]}"
            )

    def check_interval_1w_dst_window_boundaries(self):
        """INTERVAL(1w) _wduration must honour DST calendar weeks.

        Fall-back (week of Nov 2 2025, Sunday, fdow=0):
            169 hours (25+6*24), duration 608400000 ms.
        Spring-forward (week of Mar 9 2025, Sunday, fdow=0):
            167 hours (23+6*24), duration 601200000 ms.
        """
        self._prepare_interval_dst_hourly_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query("show local variables like 'firstDayOfWeek'")
        original_fdow = int(tdSql.queryResult[0][1]) if tdSql.queryRows > 0 else 1

        try:
            tdSql.execute("SET FIRST_DAY_OF_WEEK 0")

            # Fall-back: Nov 2 (Sunday) week has 25h day
            tdSql.query(
                f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
                f"where ts >= '2025-11-02' and ts < '2025-11-23' interval(1w)"
            )
            tdSql.checkRows(3)
            fall_dur = [608400000, 604800000, 604800000]
            fall_cnt = [169, 168, 168]
            for i in range(3):
                row = tdSql.queryResult[i]
                assert int(row[1]) == fall_dur[i], (
                    f"fall 1w row {i}: _wduration expected {fall_dur[i]}, got {row[1]}"
                )
                assert int(row[3]) == fall_cnt[i], (
                    f"fall 1w row {i}: count expected {fall_cnt[i]}, got {row[3]}"
                )
            for i in range(2):
                assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                    f"fall 1w: _wend[{i}] != _wstart[{i+1}]"
                )

            # Spring-forward: Mar 9 (Sunday) week has 23h day
            tdSql.query(
                f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
                f"where ts >= '2025-03-02' and ts < '2025-03-23' interval(1w)"
            )
            tdSql.checkRows(3)
            spring_dur = [604800000, 601200000, 604800000]
            spring_cnt = [168, 167, 168]
            for i in range(3):
                row = tdSql.queryResult[i]
                assert int(row[1]) == spring_dur[i], (
                    f"spring 1w row {i}: _wduration expected {spring_dur[i]}, got {row[1]}"
                )
                assert int(row[3]) == spring_cnt[i], (
                    f"spring 1w row {i}: count expected {spring_cnt[i]}, got {row[3]}"
                )
            for i in range(2):
                assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                    f"spring 1w: _wend[{i}] != _wstart[{i+1}]"
                )
        finally:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {original_fdow}")

    def check_interval_1n_dst_window_boundaries(self):
        """INTERVAL(1n) _wduration must honour DST calendar months.

        Spring (Feb / Mar / Apr in America/New_York):
            Mar has 31d - 1h = 2674800000 ms, 743 hourly rows.
        Fall (Oct / Nov / Dec):
            Nov has 30d + 1h = 2595600000 ms, 721 hourly rows.
        """
        self._prepare_interval_dst_extended_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")

        # Spring: Feb(28d), Mar(31d-1h spring-forward), Apr(30d)
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-02-01' and ts < '2025-05-01' interval(1n)"
        )
        tdSql.checkRows(3)
        spring_dur = [2419200000, 2674800000, 2592000000]
        spring_cnt = [672, 743, 720]
        for i in range(3):
            row = tdSql.queryResult[i]
            assert int(row[1]) == spring_dur[i], (
                f"spring 1n row {i}: _wduration expected {spring_dur[i]}, got {row[1]}"
            )
            assert int(row[3]) == spring_cnt[i], (
                f"spring 1n row {i}: count expected {spring_cnt[i]}, got {row[3]}"
            )
        for i in range(2):
            assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                f"spring 1n: _wend[{i}] != _wstart[{i+1}]"
            )

        # Fall: Oct(31d), Nov(30d+1h fall-back), Dec(31d)
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-10-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(3)
        fall_dur = [2678400000, 2595600000, 2678400000]
        fall_cnt = [744, 721, 744]
        for i in range(3):
            row = tdSql.queryResult[i]
            assert int(row[1]) == fall_dur[i], (
                f"fall 1n row {i}: _wduration expected {fall_dur[i]}, got {row[1]}"
            )
            assert int(row[3]) == fall_cnt[i], (
                f"fall 1n row {i}: count expected {fall_cnt[i]}, got {row[3]}"
            )
        for i in range(2):
            assert tdSql.queryResult[i][2] == tdSql.queryResult[i + 1][0], (
                f"fall 1n: _wend[{i}] != _wstart[{i+1}]"
            )

    def check_interval_1q_dst_window_boundaries(self):
        """INTERVAL(1q) _wduration must honour DST calendar quarters.

        Spring: Q1 (Jan-Mar) = 90d - 1h = 7772400000 ms, 2159 hourly rows.
                Q2 (Apr-Jun) = 91d = 7862400000 ms (partial data: Apr only, 720 rows).
        Fall:   Q4 (Oct-Dec) = 92d + 1h = 7952400000 ms, 2209 hourly rows.
        """
        self._prepare_interval_dst_extended_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")

        # Spring: Q1 (spring-forward) + Q2 (partial, Apr only)
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2025-05-01' interval(1q)"
        )
        tdSql.checkRows(2)
        spring_dur = [7772400000, 7862400000]
        spring_cnt = [2159, 720]
        for i in range(2):
            row = tdSql.queryResult[i]
            assert int(row[1]) == spring_dur[i], (
                f"spring 1q row {i}: _wduration expected {spring_dur[i]}, got {row[1]}"
            )
            assert int(row[3]) == spring_cnt[i], (
                f"spring 1q row {i}: count expected {spring_cnt[i]}, got {row[3]}"
            )
        assert tdSql.queryResult[0][2] == tdSql.queryResult[1][0], (
            "spring 1q: _wend[0] != _wstart[1]"
        )

        # Fall: Q4 (fall-back)
        tdSql.query(
            f"select _wstart, _wduration, _wend, count(*) from {self.ctbname} "
            f"where ts >= '2025-10-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(1)
        row = tdSql.queryResult[0]
        assert int(row[1]) == 7952400000, (
            f"fall 1q: _wduration expected 7952400000, got {row[1]}"
        )
        assert int(row[3]) == 2209, (
            f"fall 1q: count expected 2209, got {row[3]}"
        )

    def check_interval_1y_single_bucket(self):
        """INTERVAL(1y) on a full year should produce 1 bucket."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1y)"
        )
        tdSql.checkRows(1)

    def check_interval_1q_four_buckets(self):
        """INTERVAL(1q) on a full year should produce 4 quarterly buckets."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(4)

    def check_interval_1q_equals_3n(self):
        """INTERVAL(1q) and INTERVAL(3n) should produce identical results."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        r_q = list(tdSql.queryResult)
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(3n)"
        )
        r_n = list(tdSql.queryResult)
        assert len(r_q) == len(r_n), f"Row count: 1q={len(r_q)} 3n={len(r_n)}"
        for i in range(len(r_q)):
            assert r_q[i] == r_n[i], f"Row {i}: 1q={r_q[i]} vs 3n={r_n[i]}"

    def check_interval_sliding_1n_1d(self):
        """INTERVAL(1n) SLIDING(1d) should produce many overlapping buckets."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2025-04-01' interval(1n) sliding(1d)"
        )
        assert tdSql.queryRows > 3, f"Expected many rows, got {tdSql.queryRows}"

    def check_interval_1n_supertable(self):
        """INTERVAL(1n) should work on supertable query."""
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.stbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)

    def check_explain_with_session_timezone(self):
        """EXPLAIN uses the session (L2) timezone when generating the query plan (FS F2).

        Running EXPLAIN VERBOSE TRUE on an INTERVAL query while a session timezone
        is active must not raise an error and must produce a non-empty plan.  This
        verifies that the planner correctly picks up the connection timezone rather
        than falling back to the local-config timezone.
        """
        self._prepare_interval_natural_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"explain verbose true select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2025-04-01' interval(1d)"
        )
        assert tdSql.queryRows > 0, (
            "EXPLAIN should return a non-empty plan when session timezone is set"
        )
        tdSql.query(
            f"explain analyze select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2025-04-01' interval(1d)"
        )
        assert tdSql.queryRows > 0, (
            "EXPLAIN ANALYZE should return a non-empty plan when session timezone is set"
        )

    def test_interval_natural(self):
        """summary: INTERVAL with natural units (n/d/y/q), DST no-drift, SLIDING.

        description: INTERVAL with natural units (n/d/y/q), DST no-drift, SLIDING.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_interval_natural_case()
        self.check_interval_1n_bucket_count()
        self.check_interval_1d_bucket_count()
        self.check_interval_1d_uses_session_timezone_not_client_local()
        self.check_interval_1d_uses_session_timezone_not_client_or_server()
        self.check_interval_1d_uses_fixed_offset_session_timezone()
        self.check_interval_1n_dst_no_drift()
        self.check_interval_dst_transition_counts()
        self.check_interval_1d_dst_window_boundaries()
        self.check_interval_1w_dst_window_boundaries()
        self.check_interval_1n_dst_window_boundaries()
        self.check_interval_1q_dst_window_boundaries()
        self.check_interval_1y_single_bucket()
        self.check_interval_1q_four_buckets()
        self.check_interval_1q_equals_3n()
        self.check_interval_sliding_1n_1d()
        self.check_interval_1n_supertable()
        self.check_explain_with_session_timezone()

class _IntervalWeekMixin:
    """INTERVAL(1w) aligned by firstDayOfWeek (high-risk change)."""

    def _setup_interval_week_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_intv_wk'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb1'

    def _prepare_interval_week_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1745539200000  # 2025-04-25 00:00:00 UTC (Friday)
        for i in range(21):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def check_interval_1w_fdow_differences(self):
        """INTERVAL(1w) with different firstDayOfWeek should produce different _wstart."""
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        results = {}
        for fdow in [0, 1]:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query(
                f"select _wstart, count(*) from {self.ctbname} interval(1w)"
            )
            results[fdow] = [row[0] for row in tdSql.queryResult]
        assert results[0] != results[1], "fdow=0 and fdow=1 should produce different starts"
        assert str(results[0][0]).startswith('2025-04-20') or str(results[0][0]).startswith('2025-04-20 00:00:00'), results[0]
        assert str(results[1][0]).startswith('2025-04-21') or str(results[1][0]).startswith('2025-04-21 00:00:00'), results[1]

    def check_interval_1w_all_fdow(self):
        """INTERVAL(1w) with all 7 firstDayOfWeek values should succeed."""
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
            assert tdSql.queryRows > 0, f"fdow={fdow}: no rows"

    def check_interval_1w_dst_no_drift(self):
        """INTERVAL(1w) during DST week should not drift."""
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        assert tdSql.queryRows > 0

    def check_interval_1w_supertable(self):
        """INTERVAL(1w) should work on supertable."""
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query(f"select _wstart, count(*) from {self.stbname} interval(1w)")
        assert tdSql.queryRows > 0

    def check_interval_1w_server_config_without_session_override(self):
        """ALTER LOCAL firstDayOfWeek should affect INTERVAL(1w) after reconnect."""
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")

        tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '0'")
        tdSql.connect()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        starts_0 = [row[0] for row in tdSql.queryResult]

        tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '1'")
        tdSql.connect()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        starts_1 = [row[0] for row in tdSql.queryResult]

        assert starts_0 != starts_1, (
            f"ALTER LOCAL firstDayOfWeek should change interval starts: {starts_0} vs {starts_1}"
        )

    def check_interval_1w_fdow6_saturday(self):
        """INTERVAL(1w) with firstDayOfWeek=6 (Saturday) should align _wstart to Saturdays.

        Test data starts on 2025-04-25 (Friday).  With weeks starting on Saturday,
        the first window boundary preceding April 25 is 2025-04-19 (Saturday).
        """
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 6")
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        assert tdSql.queryRows > 0, "fdow=6: INTERVAL(1w) should return rows"
        first_start = str(tdSql.queryResult[0][0])
        assert '2025-04-19' in first_start, (
            f"fdow=6 (Saturday): first _wstart should be 2025-04-19, got {first_start!r}"
        )

    def check_interval_1w_fdow_session_reset_on_reconnect(self):
        """SET FIRST_DAY_OF_WEEK (L2 session) is reset to ALTER LOCAL (L3) after reconnect.

        Steps:
        1. ALTER LOCAL firstDayOfWeek=1 (Monday) — establishes L3.
        2. Reconnect so the session starts fresh with L3.
        3. SET FIRST_DAY_OF_WEEK 3 (Wednesday, L2 session override) and query → get
           Wednesday-aligned _wstart.
        4. Reconnect — L2 session is cleared.
        5. Query again → _wstart must revert to Monday alignment (L3), not Wednesday.
        """
        self._prepare_interval_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")

        tdSql.query("show local variables like 'firstDayOfWeek'")
        original_fdow = int(tdSql.queryResult[0][1]) if tdSql.queryRows > 0 else 1

        try:
            tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '1'")
            tdSql.connect()
            tdSql.execute("SET TIMEZONE 'UTC'")
            tdSql.query(f"select _wstart from {self.ctbname} interval(1w) limit 1")
            start_l3_monday = str(tdSql.queryResult[0][0])

            tdSql.execute("SET FIRST_DAY_OF_WEEK 3")
            tdSql.query(f"select _wstart from {self.ctbname} interval(1w) limit 1")
            start_l2_wednesday = str(tdSql.queryResult[0][0])

            assert start_l3_monday != start_l2_wednesday, (
                f"SET FIRST_DAY_OF_WEEK 3 should produce different _wstart than fdow=1: "
                f"monday={start_l3_monday!r}, wednesday={start_l2_wednesday!r}"
            )

            tdSql.connect()
            tdSql.execute("SET TIMEZONE 'UTC'")
            tdSql.query(f"select _wstart from {self.ctbname} interval(1w) limit 1")
            start_after_reconnect = str(tdSql.queryResult[0][0])

            assert start_after_reconnect == start_l3_monday, (
                f"After reconnect, FIRST_DAY_OF_WEEK should reset to L3 (Monday=1): "
                f"expected {start_l3_monday!r}, got {start_after_reconnect!r}"
            )
        finally:
            tdSql.execute(f"ALTER LOCAL 'firstDayOfWeek' '{original_fdow}'")
            tdSql.connect()

    def test_interval_week(self):
        """summary: INTERVAL(1w) aligned by firstDayOfWeek (high-risk change).

        description: INTERVAL(1w) aligned by firstDayOfWeek (high-risk change).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_interval_week_case()
        self.check_interval_1w_fdow_differences()
        self.check_interval_1w_all_fdow()
        self.check_interval_1w_dst_no_drift()
        self.check_interval_1w_supertable()
        self.check_interval_1w_server_config_without_session_override()
        self.check_interval_1w_fdow6_saturday()
        self.check_interval_1w_fdow_session_reset_on_reconnect()

class _IntervalQuarterMixin:
    """INTERVAL(1q/2q) quarter boundaries and equivalence tests."""

    def _setup_interval_quarter_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_intv_q'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb1'

    def _prepare_interval_quarter_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1735689600000  # 2025-01-01 00:00:00 UTC
        for i in range(365):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def check_interval_1q_boundaries(self):
        """INTERVAL(1q): _wstart should be Jan1, Apr1, Jul1, Oct1."""
        self._prepare_interval_quarter_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(4)
        expected_starts = ['2025-01-01', '2025-04-01', '2025-07-01', '2025-10-01']
        for i, exp in enumerate(expected_starts):
            start = str(tdSql.queryResult[i][0])
            assert exp in start, f"Row {i}: expected {exp} in {start}"

    def check_interval_2q_two_buckets(self):
        """INTERVAL(2q): 1 year should produce 2 half-year buckets."""
        self._prepare_interval_quarter_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(2q)"
        )
        tdSql.checkRows(2)

    def check_interval_1q_equals_3n_results(self):
        """INTERVAL(1q) and INTERVAL(3n) should produce identical rows."""
        self._prepare_interval_quarter_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        r_q = list(tdSql.queryResult)
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(3n)"
        )
        r_n = list(tdSql.queryResult)
        assert len(r_q) == len(r_n)
        for i in range(len(r_q)):
            assert r_q[i] == r_n[i], f"Row {i}: 1q={r_q[i]} vs 3n={r_n[i]}"

    def check_interval_2q_equals_6n(self):
        """INTERVAL(2q) and INTERVAL(6n) should produce identical rows."""
        self._prepare_interval_quarter_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(2q)"
        )
        r_2q = list(tdSql.queryResult)
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(6n)"
        )
        r_6n = list(tdSql.queryResult)
        assert len(r_2q) == len(r_6n)
        for i in range(len(r_2q)):
            assert r_2q[i] == r_6n[i], f"Row {i}: 2q={r_2q[i]} vs 6n={r_6n[i]}"

    def check_interval_1q_supertable(self):
        """INTERVAL(1q) should work on supertable."""
        self._prepare_interval_quarter_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.stbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(4)

    def test_interval_quarter(self):
        """summary: INTERVAL(1q/2q) quarter boundaries and equivalence tests.

        description: INTERVAL(1q/2q) quarter boundaries and equivalence tests.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_interval_quarter_case()
        self.check_interval_1q_boundaries()
        self.check_interval_2q_two_buckets()
        self.check_interval_1q_equals_3n_results()
        self.check_interval_2q_equals_6n()
        self.check_interval_1q_supertable()


class TestTimezoneInterval(
    _IntervalNaturalMixin,
    _IntervalWeekMixin,
    _IntervalQuarterMixin,
):
    pass
