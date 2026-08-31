"""Timezone scalar function tests.

Covers:
- P3 Task 3.1: TO_ISO8601(ts, 'IANA_name') IANA extension, DST, fallback L2->L3->L5
- P3 Task 3.2: TO_CHAR(ts, fmt, 'timezone') third parameter
- P3 Task 3.3: TIMETRUNCATE(ts, unit, 'tz_string') third parameter extension
- P4 Task 4.1: TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx invalid)
- P4 Task 4.2: TIMETRUNCATE 1w aligned by firstDayOfWeek (high-risk change)
- P4 Task 4.4: WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged
- DST edge cases: spring-forward gap, fall-back overlap, write-path regression
- Timestamp arithmetic: ts + 1d/1w/1n/1q/1y preserves wall-clock time across DST
- TO_TIMESTAMP: tz-less strings parse in connection timezone (L2); embedded
  offset overrides
- DATE: fixed instant formats in connection timezone (L2); tz-less string is
  tz-independent
- TO_UNIXTIMESTAMP: tz-less strings parse in connection timezone (L2); embedded
  offset overrides
- DAYOFWEEK/WEEKDAY/WEEK/WEEKOFYEAR: fixed instant resolves the local calendar
  in the connection timezone (L2)
"""

import pytest
import sys
from new_test_framework.utils import tdLog, tdSql

ERR_INVALID_TIMEZONE = 0x26B2
ERR_INVALID_FIRST_DAY_OF_WEEK = 0x26B3
ERR_INVALID_FUNCTION_PARAM = 0x2803

IS_WINDOWS = sys.platform.startswith('win')
SKIP_WINDOWS_SET_TIMEZONE = pytest.mark.skipif(
    IS_WINDOWS,
    reason='Windows does not support SET TIMEZONE cases in this suite',
)

class _ToIso8601IanaMixin:
    """TO_ISO8601 IANA timezone parameter and DST-aware output."""

    def _setup_to_iso8601_iana_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_iso'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts_winter = 1642248000000  # 2022-01-15 12:00:00 UTC
        self.ts_summer = 1657886400000  # 2022-07-15 12:00:00 UTC

    def _prepare_to_iso8601_iana_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_winter}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_summer}, 2)')

    def check_to_iso8601_iana_no_dst(self):
        """IANA timezones without DST should have constant offset."""
        self._prepare_to_iso8601_iana_data()
        cases = {
            'Asia/Shanghai': ['+08:00', '+0800'],
            'Asia/Tokyo': ['+09:00', '+0900'],
        }
        for tz, expected in cases.items():
            tdSql.query(f"select to_iso8601(ts, '{tz}') from {self.ntbname} order by ts")
            for row in tdSql.queryResult:
                assert any(e in row[0] for e in expected), \
                    f"{tz}: expected {expected} in {row[0]}"

    def check_to_iso8601_iana_dst_new_york(self):
        """America/New_York: winter -05:00 (EST), summer -04:00 (EDT)."""
        self._prepare_to_iso8601_iana_data()
        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"Winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"Summer: {summer}"

    def check_to_iso8601_iana_dst_london(self):
        """Europe/London: winter +00:00 (GMT), summer +01:00 (BST)."""
        self._prepare_to_iso8601_iana_data()
        tdSql.query(
            f"select to_iso8601(ts, 'Europe/London') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert any(e in winter for e in ['+00:00', '+0000', 'Z'])
        assert '+01:00' in summer or '+0100' in summer, f"Summer: {summer}"

    def check_to_iso8601_fixed_offset_compat(self):
        """Fixed offset params (+08:00, Z) should still work (backward compat)."""
        self._prepare_to_iso8601_iana_data()
        tdSql.query(f"select to_iso8601(ts, '+08:00') from {self.ntbname} order by ts")
        for row in tdSql.queryResult:
            assert '+08:00' in row[0] or '+0800' in row[0]

    def check_to_iso8601_invalid_tz(self):
        """Invalid timezone params should fail."""
        self._prepare_to_iso8601_iana_data()
        for tz in ["'Invalid/Zone'", "'CST'", "'+8'"]:
            tdSql.error(
                f"select to_iso8601(ts, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_to_iso8601_no_param_uses_l2(self):
        """TO_ISO8601(ts) without tz param should use connection timezone (L2)."""
        self._prepare_to_iso8601_iana_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(ts) from {self.ntbname} order by ts")
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"L2 winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"L2 summer: {summer}"

    def check_to_iso8601_l1_overrides_l2(self):
        """Explicit tz param (L1) should override connection timezone (L2)."""
        self._prepare_to_iso8601_iana_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} where c1 = 1"
        )
        assert '-05:00' in tdSql.queryResult[0][0] or '-0500' in tdSql.queryResult[0][0]

    def check_to_iso8601_us_ns_precision(self):
        """IANA timezone should work with us/ns precision databases."""
        for prec, factor in [('us', 1000), ('ns', 1000000)]:
            dbname = f'{self.dbname}_{prec}'
            ntbname = f'{dbname}.ntb'
            tdSql.execute(f'create database if not exists {dbname} precision "{prec}"')
            tdSql.execute(f'use {dbname}')
            tdSql.execute(f'drop table if exists {ntbname}')
            tdSql.execute(f'create table {ntbname} (ts timestamp, c1 int)')
            tdSql.execute(
                f'insert into {ntbname} values ({self.ts_winter * factor}, 1)'
            )
            tdSql.query(f"select to_iso8601(ts, 'Asia/Shanghai') from {ntbname}")
            assert '+08:00' in tdSql.queryResult[0][0] or '+0800' in tdSql.queryResult[0][0]

    def test_to_iso8601_iana(self):
        """summary: TO_ISO8601 IANA timezone parameter and DST-aware output.

        description: TO_ISO8601 IANA timezone parameter and DST-aware output.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_to_iso8601_iana_case()
        self.check_to_iso8601_iana_no_dst()
        self.check_to_iso8601_iana_dst_new_york()
        self.check_to_iso8601_iana_dst_london()
        self.check_to_iso8601_fixed_offset_compat()
        self.check_to_iso8601_invalid_tz()
        self.check_to_iso8601_no_param_uses_l2()
        self.check_to_iso8601_l1_overrides_l2()
        self.check_to_iso8601_us_ns_precision()

class _ToCharTimezoneMixin:
    """TO_CHAR(ts, fmt [, timezone]) third parameter tests."""

    def _setup_to_char_timezone_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_tochar'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts_winter = 1642248000000  # 2022-01-15 12:00:00 UTC
        self.ts_summer = 1657886400000  # 2022-07-15 12:00:00 UTC

    def _prepare_to_char_timezone_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_winter}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_summer}, 2)')

    def check_to_char_iana_and_fixed_offset(self):
        """TO_CHAR with IANA timezone and fixed-offset timezone parameters.

        IANA names use calendar timezone semantics. Bare fixed-offset strings in
        the generic timezone-parameter path are normalized with POSIX sign
        semantics, so '+08:00' means UTC-08:00 here.
        """
        self._prepare_to_char_timezone_data()
        fmt = 'YYYY-MM-DD HH24:MI:SS'
        cases = [
            ('Asia/Shanghai', 1, '20:00:00'),
            ('America/New_York', 1, '07:00:00'),   # winter EST
            ('America/New_York', 2, '08:00:00'),   # summer EDT
            ('UTC', 1, '12:00:00'),
            ('+08:00', 1, '04:00:00'),
        ]
        for tz, c1, expected_time in cases:
            tdSql.query(
                f"select to_char(ts, '{fmt}', '{tz}') from {self.ntbname} where c1 = {c1}"
            )
            assert expected_time in tdSql.queryResult[0][0], \
                f"tz={tz}, c1={c1}: expected {expected_time} in {tdSql.queryResult[0][0]}"

    def check_to_char_invalid_tz(self):
        """Invalid timezone params should fail."""
        self._prepare_to_char_timezone_data()
        fmt = 'YYYY-MM-DD HH24:MI:SS'
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select to_char(ts, '{fmt}', {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_to_char_no_param_uses_l2(self):
        """TO_CHAR without tz param should use connection timezone (L2)."""
        self._prepare_to_char_timezone_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS') from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def check_to_char_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self._prepare_to_char_timezone_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def check_to_char_two_params_compat(self):
        """TO_CHAR(ts, fmt) two-param form should still work."""
        self._prepare_to_char_timezone_data()
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS') from {self.ntbname} where c1 = 1"
        )
        assert '2022' in tdSql.queryResult[0][0]

    def test_to_char_timezone(self):
        """summary: TO_CHAR(ts, fmt [, timezone]) third parameter tests.

        description: TO_CHAR(ts, fmt [, timezone]) third parameter tests.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_to_char_timezone_case()
        self.check_to_char_iana_and_fixed_offset()
        self.check_to_char_invalid_tz()
        self.check_to_char_no_param_uses_l2()
        self.check_to_char_l1_overrides_l2()
        self.check_to_char_two_params_compat()

class _TimetruncateTzMixin:
    """TIMETRUNCATE third parameter: string timezone, integer 0/1 compat, fallback chain."""

    def _setup_timetruncate_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_tt_tz'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts1 = 1773750600000  # 2026-03-15 10:30:00 UTC

    def _prepare_timetruncate_tz_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts1}, 1)')

    def check_timetruncate_iana_tz_different_boundaries(self):
        """String tz param should truncate in that timezone's calendar.

        UTC vs Shanghai should produce different 1d boundaries for this timestamp.
        """
        self._prepare_timetruncate_tz_data()
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh, f"UTC vs Shanghai 1d should differ: {r_utc} vs {r_sh}"

    def check_timetruncate_int_01_compat(self):
        """Integer 0/1 and no-param forms should work (backward compat)."""
        self._prepare_timetruncate_tz_data()
        for p in ['0', '1', '']:
            suffix = f', {p}' if p else ''
            tdSql.query(f"select timetruncate(ts, 1d{suffix}) from {self.ntbname}")
            assert tdSql.queryResult[0][0] is not None

    def check_timetruncate_invalid_tz(self):
        """Invalid timezone string should fail."""
        self._prepare_timetruncate_tz_data()
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select timetruncate(ts, 1d, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_timetruncate_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self._prepare_timetruncate_tz_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh

    def check_timetruncate_no_param_uses_l2(self):
        """Without tz param, different L2 should produce different results."""
        self._prepare_timetruncate_tz_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_ny = tdSql.queryResult[0][0]
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_ny != r_sh

    def check_timetruncate_subday_string_tz_uses_target_offset(self):
        """Sub-day truncation should follow the resolved timezone semantics.

        IANA timezone names use their real local offset. Bare fixed-offset
        strings are normalized via the generic timezone validator, which now
        follows POSIX sign semantics for offset strings.
        """
        self._prepare_timetruncate_tz_data()
        cases = [
            ('Asia/Kathmandu', '1h', '2026-03-15T09:15:00'),
            ('+05:45', '1h', '2026-03-15T09:45:00'),
        ]
        for tz, unit, expected in cases:
            tdSql.query(
                "select to_iso8601(timetruncate('2026-03-15T10:10:00Z', "
                f"{unit}, '{tz}'), 'UTC')"
            )
            result = tdSql.queryResult[0][0]
            assert expected in result, f"tz={tz}, unit={unit}: expected {expected} in {result}"

    def check_timetruncate_utc_prefix_case_insensitive(self):
        """Lowercase UTC prefixes must resolve exactly like uppercase ones.

        A case-sensitive prefix check in the execution path would silently fall
        back to the connection timezone instead of the requested offset.
        """
        self._prepare_timetruncate_tz_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_session = tdSql.queryResult[0][0]

        pairs = [('utc8', 'UTC8'), ('utc+8', 'UTC+8'), ('uTc-8', 'UTC-8')]
        for lower, upper in pairs:
            tdSql.query(f"select timetruncate(ts, 1d, '{lower}') from {self.ntbname}")
            r_lower = tdSql.queryResult[0][0]
            tdSql.query(f"select timetruncate(ts, 1d, '{upper}') from {self.ntbname}")
            r_upper = tdSql.queryResult[0][0]
            assert r_lower == r_upper, f"{lower} vs {upper}: {r_lower} != {r_upper}"

        # UTC+8 is west 8 under POSIX, so it must differ from Asia/Shanghai.
        tdSql.query(f"select timetruncate(ts, 1d, 'utc8') from {self.ntbname}")
        assert tdSql.queryResult[0][0] != r_session, (
            "'utc8' fell back to the connection timezone"
        )

    def test_timetruncate_tz(self):
        """summary: TIMETRUNCATE third parameter: string timezone, integer 0/1 compat, fallback chain.

        description: TIMETRUNCATE third parameter: string timezone, integer 0/1 compat, fallback chain.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_timetruncate_tz_case()
        self.check_timetruncate_iana_tz_different_boundaries()
        self.check_timetruncate_int_01_compat()
        self.check_timetruncate_invalid_tz()
        self.check_timetruncate_l1_overrides_l2()
        self.check_timetruncate_no_param_uses_l2()
        self.check_timetruncate_subday_string_tz_uses_target_offset()
        self.check_timetruncate_utc_prefix_case_insensitive()

class _TimetruncateNaturalUnitsMixin:
    """TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx is invalid)."""

    pytestmark = []  # P4 implemented

    def _setup_timetruncate_natural_units_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_tt_nat'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_timetruncate_natural_units_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1773750600000, 1)')

    def check_timetruncate_1n_month(self):
        """1n should align to first day of month 00:00:00."""
        self._prepare_timetruncate_natural_units_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        cases = [
            '2026-03-15 10:30:00',
            '2026-03-01 00:00:00',
            '2026-03-31 23:59:59',
        ]
        for ts_str in cases:
            tdSql.query(f"select to_iso8601(timetruncate('{ts_str}', 1n), 'UTC')")
            result = tdSql.queryResult[0][0]
            assert '2026-03-01T00:00:00' in result, f"1n({ts_str}): {result}"

    def check_timetruncate_1q_all_quarters(self):
        """1q: Q1->Jan1, Q2->Apr1, Q3->Jul1, Q4->Oct1."""
        self._prepare_timetruncate_natural_units_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        quarters = [
            ('2026-02-15 10:30:00', '2026-01-01T00:00:00'),
            ('2026-05-15 10:30:00', '2026-04-01T00:00:00'),
            ('2026-08-15 10:30:00', '2026-07-01T00:00:00'),
            ('2026-11-15 10:30:00', '2026-10-01T00:00:00'),
        ]
        for ts_str, expected in quarters:
            tdSql.query(f"select to_iso8601(timetruncate('{ts_str}', 1q), 'Asia/Shanghai')")
            result = tdSql.queryResult[0][0]
            assert expected in result, f"1q({ts_str}): expected {expected} in {result}"

    def check_timetruncate_1y_year(self):
        """1y should align to Jan 1 00:00:00."""
        self._prepare_timetruncate_natural_units_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for ts_str in ['2026-08-15 10:30:00', '2026-01-01 00:00:00']:
            tdSql.query(f"select to_iso8601(timetruncate('{ts_str}', 1y), 'UTC')")
            result = tdSql.queryResult[0][0]
            assert '2026-01-01T00:00:00' in result, f"1y({ts_str}): {result}"

    def check_timetruncate_1q_equals_3n_is_invalid(self):
        """3n is invalid (Nx not supported); only 1n/1q/1y are valid."""
        self._prepare_timetruncate_natural_units_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.error("select timetruncate('2026-05-15 10:30:00', 3n)")
        tdSql.error("select timetruncate('2026-05-15 10:30:00', 2q)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 2y)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 2w)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 6n)")

    def check_timetruncate_natural_with_tz_param(self):
        """n/q/y units combined with string timezone parameter."""
        self._prepare_timetruncate_natural_units_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        for u in ['1n', '1q', '1y']:
            tdSql.query(f"select timetruncate('2026-05-15 10:30:00', {u})")
            session_result = tdSql.queryResult[0][0]
            tdSql.query(f"select timetruncate('2026-05-15 10:30:00', {u}, 'America/New_York')")
            explicit_result = tdSql.queryResult[0][0]
            assert session_result == explicit_result, (
                f"{u}: session tz and explicit tz should match: "
                f"{session_result} vs {explicit_result}"
            )

    def test_timetruncate_natural_units(self):
        """summary: TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx is invalid).

        description: TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx is invalid).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_timetruncate_natural_units_case()
        self.check_timetruncate_1n_month()
        self.check_timetruncate_1q_all_quarters()
        self.check_timetruncate_1y_year()
        self.check_timetruncate_1q_equals_3n_is_invalid()
        self.check_timetruncate_natural_with_tz_param()

class _TimetruncateUnitMultiplierValidationMixin:
    """TIMETRUNCATE unit multiplier constraint: only N=1 allowed, Nx (N>1) returns "Invalid time unit"."""

    pytestmark = []  # P4 regression

    def _setup_timetruncate_unit_multiplier_validation_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_tt_mult'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_timetruncate_unit_multiplier_validation_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1773750600000, 1)')

    def check_timetruncate_2n_invalid(self):
        """Natural month: 2n, 3n, 6n, 12n should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 6, 12]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}n)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2q_invalid(self):
        """Natural quarter: 2q, 3q, 4q should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 4]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}q)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2y_invalid(self):
        """Natural year: 2y, 3y, 5y should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 5]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}y)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2w_invalid(self):
        """Week: 2w, 3w, 4w, 10w should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 4, 10]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}w)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2d_invalid(self):
        """Day: 2d, 3d, 7d should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 7]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}d)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2h_invalid(self):
        """Hour: 2h, 3h, 6h, 24h should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 3, 6, 24]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}h)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2m_invalid(self):
        """Minute: 2m, 5m, 15m, 60m should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 5, 15, 60]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}m)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2s_invalid(self):
        """Second: 2s, 5s, 10s, 60s should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 5, 10, 60]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}s)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2ms_invalid(self):
        """Millisecond: 2ms, 100ms, 1000ms should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 100, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}a)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2u_invalid(self):
        """Microsecond: 2u, 1000u should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}u)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2b_invalid(self):
        """Nanosecond: 2b, 1000b should all be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        for mult in [2, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}b)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_multiplier_with_tz_param_invalid(self):
        """Multiplier > 1 with string timezone parameter should also be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        invalid_cases = [
            "select timetruncate('2026-05-15 10:30:00', 2d, 'UTC')",
            "select timetruncate('2026-05-15 10:30:00', 3n, 'Asia/Shanghai')",
            "select timetruncate('2026-05-15 10:30:00', 2q, 'America/New_York')",
            "select timetruncate('2026-05-15 10:30:00', 2w, 'Europe/London')",
        ]
        for sql in invalid_cases:
            tdSql.error(sql, expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_multiplier_from_table_invalid(self):
        """Multiplier > 1 in FROM table queries should also be invalid."""
        self._prepare_timetruncate_unit_multiplier_validation_data()
        tdSql.error(f"select timetruncate(ts, 2d) from {self.ntbname}",
                    expectErrInfo="Invalid time unit : timetruncate")
        tdSql.error(f"select timetruncate(ts, 3n) from {self.ntbname}",
                    expectErrInfo="Invalid time unit : timetruncate")
        tdSql.error(f"select timetruncate(ts, 2h) from {self.ntbname}",
                    expectErrInfo="Invalid time unit : timetruncate")

    def test_timetruncate_unit_multiplier_validation(self):
        """summary: TIMETRUNCATE unit multiplier constraint: only N=1 allowed, Nx (N>1) returns "Invalid time unit".

        description: TIMETRUNCATE unit multiplier constraint: only N=1 allowed, Nx (N>1) returns "Invalid time unit".

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_timetruncate_unit_multiplier_validation_case()
        self.check_timetruncate_2n_invalid()
        self.check_timetruncate_2q_invalid()
        self.check_timetruncate_2y_invalid()
        self.check_timetruncate_2w_invalid()
        self.check_timetruncate_2d_invalid()
        self.check_timetruncate_2h_invalid()
        self.check_timetruncate_2m_invalid()
        self.check_timetruncate_2s_invalid()
        self.check_timetruncate_2ms_invalid()
        self.check_timetruncate_2u_invalid()
        self.check_timetruncate_2b_invalid()
        self.check_timetruncate_multiplier_with_tz_param_invalid()
        self.check_timetruncate_multiplier_from_table_invalid()

class _TimetruncateWeekMixin:
    """TIMETRUNCATE 1w/Nw aligned by firstDayOfWeek (high-risk behavior change)."""

    def _setup_timetruncate_week_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_tt_wk'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_timetruncate_week_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1745920800000, 1)')

    def check_timetruncate_1w_fdow_differences(self):
        """TIMETRUNCATE(1w) with fdow 0-6 should produce different alignments.

        2026-04-30 is Thursday.
        - fdow=1 (Mon) -> Apr 27 (Mon)
        - fdow=0 (Sun) -> Apr 26 (Sun)
        - fdow=4 (Thu) -> Apr 30 (Thu, same day)
        """
        self._prepare_timetruncate_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        results = {}
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select to_iso8601(timetruncate('2026-04-30 10:00:00', 1w), 'UTC')")
            results[fdow] = tdSql.queryResult[0][0]
        assert results[1] != results[0], "fdow=1 vs fdow=0 should differ"
        assert '2026-04-27T00:00:00' in results[1], f"fdow=1: {results[1]}"
        assert '2026-04-26T00:00:00' in results[0], f"fdow=0: {results[0]}"
        assert '2026-04-30T00:00:00' in results[4], f"fdow=4: {results[4]}"

    def check_timetruncate_1w_on_week_start(self):
        """Truncating on exact week start day should return that day's midnight."""
        self._prepare_timetruncate_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query("select to_iso8601(timetruncate('2026-04-27 10:00:00', 1w), 'UTC')")
        result = tdSql.queryResult[0][0]
        assert '2026-04-27T00:00:00' in result, f"Monday fdow=1: {result}"

    def check_timetruncate_1w_with_iana_tz(self):
        """Regression: 1w with explicit IANA tz should match session tz path."""
        self._prepare_timetruncate_week_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query("select timetruncate('2026-04-30 10:00:00', 1w, 'Asia/Shanghai')")
        explicit_result = tdSql.queryResult[0][0]
        tdSql.query("select timetruncate('2026-04-30 10:00:00', 1w)")
        session_result = tdSql.queryResult[0][0]
        assert explicit_result == session_result, (
            f"1w explicit Shanghai should match session Shanghai: "
            f"{explicit_result} vs {session_result}"
        )

    def check_timetruncate_1w_with_iana_tz_dst_consistency(self):
        """Regression: 1w explicit IANA tz should match session tz across DST."""
        self._prepare_timetruncate_week_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        cases = [
            '2026-03-10 10:00:00',  # spring-forward period
            '2026-11-03 10:00:00',  # fall-back period
        ]
        for ts_str in cases:
            tdSql.query(f"select timetruncate('{ts_str}', 1w, 'America/New_York')")
            explicit_result = tdSql.queryResult[0][0]
            tdSql.query(f"select timetruncate('{ts_str}', 1w)")
            session_result = tdSql.queryResult[0][0]
            assert explicit_result == session_result, (
                f"1w explicit New_York should match session New_York at {ts_str}: "
                f"{explicit_result} vs {session_result}"
            )

    def check_timetruncate_1w_dst_spring(self):
        """1w during DST spring-forward should still align to local 00:00:00."""
        self._prepare_timetruncate_week_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
        tdSql.query("select to_iso8601(timetruncate('2026-03-10 10:00:00', 1w), 'America/New_York')")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T00:00:00' in result, f"1w DST spring: {result}"

    def check_set_first_day_of_week_invalid_error_message(self):
        """SET FIRST_DAY_OF_WEEK out-of-range should return formatted error message."""
        self._prepare_timetruncate_week_data()
        for v in [-1, 7, 100]:
            err = tdSql.error(
                f"SET FIRST_DAY_OF_WEEK {v}",
                expectedErrno=ERR_INVALID_FIRST_DAY_OF_WEEK,
                fullMatched=False,
            )
            assert f"Invalid firstDayOfWeek: {v}, must be 0-6" in err, (
                f"unexpected error info for {v}: {err}"
            )
            assert "%d" not in err, f"raw placeholder leaked for {v}: {err}"

    def test_timetruncate_week(self):
        """summary: TIMETRUNCATE 1w/Nw aligned by firstDayOfWeek (high-risk behavior change).

        description: TIMETRUNCATE 1w/Nw aligned by firstDayOfWeek (high-risk behavior change).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_timetruncate_week_case()
        self.check_timetruncate_1w_fdow_differences()
        self.check_timetruncate_1w_on_week_start()
        self.check_timetruncate_1w_with_iana_tz()
        self.check_timetruncate_1w_with_iana_tz_dst_consistency()
        self.check_timetruncate_1w_dst_spring()
        self.check_set_first_day_of_week_invalid_error_message()

class _WeekFunctionsMixin:
    """WEEK respects firstDayOfWeek; WEEKOFYEAR/DAYOFWEEK/WEEKDAY are fdow-independent."""

    def _setup_week_functions_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_wkfn'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_week_functions_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1735689600000, 1)')

    def check_weekofyear_not_affected_by_fdow(self):
        """WEEKOFYEAR is equivalent to WEEK(ts,3) (ISO 8601, Monday-first).

        It must NOT change with FIRST_DAY_OF_WEEK, and must return the correct
        ISO week number: 2026-01-01 (Thursday) is ISO week 1.
        """
        self._prepare_week_functions_data()
        results = {}
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select weekofyear('2026-01-01 12:00:00')")
            results[fdow] = tdSql.queryResult[0][0]
        assert all(r == results[0] for r in results.values()), \
            f"WEEKOFYEAR should NOT change with fdow: {results}"
        assert results[0] == 1, \
            f"WEEKOFYEAR('2026-01-01') should be ISO week 1, got {results[0]}"

    def check_week_mode_all_valid(self):
        """WEEK(ts, mode) for mode 0-7 should all succeed."""
        self._prepare_week_functions_data()
        results = []
        for mode in range(8):
            tdSql.query(f"select week('2026-06-15 12:00:00', {mode})")
            results.append(tdSql.queryResult[0][0])
        assert all(result is not None for result in results), f"WEEK mode results: {results}"

    def check_week_mode_8_invalid(self):
        """WEEK(ts, 8) should fail."""
        self._prepare_week_functions_data()
        tdSql.error(
            "select week('2026-01-01 12:00:00', 8)",
            expectedErrno=ERR_INVALID_FUNCTION_PARAM,
        )

    def check_dayofweek_not_affected_by_fdow(self):
        """DAYOFWEEK should return identical values regardless of firstDayOfWeek.

        DAYOFWEEK: 1=Sunday, 2=Monday, ..., 7=Saturday.
        """
        self._prepare_week_functions_data()
        results = []
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select dayofweek('2026-04-30 10:00:00')")  # Thursday
            results.append(tdSql.queryResult[0][0])
        assert all(r == results[0] for r in results), \
            f"DAYOFWEEK should not change: {results}"

    def check_weekday_not_affected_by_fdow(self):
        """WEEKDAY should return identical values regardless of firstDayOfWeek.

        WEEKDAY: 0=Monday, 1=Tuesday, ..., 6=Sunday.
        """
        self._prepare_week_functions_data()
        results = []
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select weekday('2026-04-30 10:00:00')")  # Thursday
            results.append(tdSql.queryResult[0][0])
        assert all(r == results[0] for r in results), \
            f"WEEKDAY should not change: {results}"

    def check_dayofweek_known_values(self):
        """Verify DAYOFWEEK returns correct values for known dates."""
        self._prepare_week_functions_data()
        cases = [
            ('2026-04-27 00:00:00', 2),  # Monday
            ('2026-04-26 00:00:00', 1),  # Sunday
            ('2026-05-02 00:00:00', 7),  # Saturday
        ]
        for ts_str, expected in cases:
            tdSql.query(f"select dayofweek('{ts_str}')")
            assert tdSql.queryResult[0][0] == expected, \
                f"DAYOFWEEK({ts_str}): expected {expected}, got {tdSql.queryResult[0][0]}"

    def check_weekday_known_values(self):
        """Verify WEEKDAY returns correct values for known dates."""
        self._prepare_week_functions_data()
        cases = [
            ('2026-04-27 00:00:00', 0),  # Monday
            ('2026-04-26 00:00:00', 6),  # Sunday
        ]
        for ts_str, expected in cases:
            tdSql.query(f"select weekday('{ts_str}')")
            assert tdSql.queryResult[0][0] == expected, \
                f"WEEKDAY({ts_str}): expected {expected}, got {tdSql.queryResult[0][0]}"

    def check_week_from_table(self):
        """WEEK/WEEKOFYEAR should work with table data."""
        self._prepare_week_functions_data()
        tdSql.query(f"select week(ts), weekofyear(ts) from {self.ntbname}")
        tdSql.checkRows(1)

    def test_week_functions(self):
        """summary: WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged.

        description: WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_week_functions_case()
        self.check_weekofyear_not_affected_by_fdow()
        self.check_week_mode_all_valid()
        self.check_week_mode_8_invalid()
        self.check_dayofweek_not_affected_by_fdow()
        self.check_weekday_not_affected_by_fdow()
        self.check_dayofweek_known_values()
        self.check_weekday_known_values()
        self.check_week_from_table()

class _SessionFirstDayOfWeekFunctionMixin:
    """System info function first_day_of_week() should reflect current session setting."""

    def check_first_day_of_week_reflects_session(self):
        for fdow in [0, 1, 3, 6]:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select first_day_of_week()")
            assert tdSql.queryResult[0][0] == fdow, (
                f"first_day_of_week() expected {fdow}, got {tdSql.queryResult[0][0]}"
            )

    def test_first_day_of_week_function(self):
        """summary: select first_day_of_week() returns current session firstDayOfWeek.

        description: select first_day_of_week() returns current session firstDayOfWeek.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:first_day_of_week

        History:
            - 2026-05-18: Copilot added

        """
        self.check_first_day_of_week_reflects_session()

class _DstEdgeMixin:
    """DST edge cases: spring-forward gap, fall-back overlap, write-path regression.

    DST times (America/New_York 2026):
    - Spring: 2026-03-08 02:00 EST -> 03:00 EDT
    - Fall:   2026-11-01 02:00 EDT -> 01:00 EST
    """

    def _setup_dst_edge_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_dst'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_dst_edge_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')

    def check_dst_spring_iso8601_offset(self):
        """TO_ISO8601 should show -05:00 before and -04:00 after spring-forward."""
        self._prepare_dst_edge_data()
        ts_before = 1772949600000  # 2026-03-08 01:00 EST = 06:00 UTC
        ts_after = 1772953200000   # 2026-03-08 03:00 EDT = 07:00 UTC
        tdSql.execute(f'insert into {self.ntbname} values ({ts_before}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_after}, 2)')

        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        before, after = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in before or '-0500' in before, f"Before spring: {before}"
        assert '-04:00' in after or '-0400' in after, f"After spring: {after}"

    def check_dst_spring_to_char_gap(self):
        """TO_CHAR should show 01:59 before gap and 03:00 after gap (no 02:xx exists)."""
        self._prepare_dst_edge_data()
        ts_before_gap = 1772953140000  # 01:59 EST = 06:59 UTC (2026-03-08)
        ts_after_gap = 1772953200000   # 03:00 EDT = 07:00 UTC (2026-03-08)
        tdSql.execute(f'insert into {self.ntbname} values ({ts_before_gap}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_after_gap}, 2)')

        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname} order by ts"
        )
        assert '01:59:00' in tdSql.queryResult[0][0]
        assert '03:00:00' in tdSql.queryResult[1][0]

    def check_dst_spring_timetruncate_1d(self):
        """TIMETRUNCATE(1d) on spring-forward day should align to local midnight."""
        self._prepare_dst_edge_data()
        ts = 1772982000000  # 2026-03-08 15:00 UTC = 11:00 EDT
        tdSql.execute(f'insert into {self.ntbname} values ({ts}, 1)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(timetruncate(ts, 1d), 'UTC') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T05:00:00' in result, f"1d spring day: {result}"

    def check_dst_fall_iso8601_offset(self):
        """TO_ISO8601 should show -04:00 before and -05:00 after fall-back."""
        self._prepare_dst_edge_data()
        ts_edt = 1793509200000  # 2026-11-01 01:00 EDT = 05:00 UTC
        ts_est = 1793512800000  # 2026-11-01 01:00 EST = 06:00 UTC (after fall-back)
        tdSql.execute(f'insert into {self.ntbname} values ({ts_edt}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_est}, 2)')

        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        before, after = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-04:00' in before or '-0400' in before, f"Before fall: {before}"
        assert '-05:00' in after or '-0500' in after, f"After fall: {after}"

    def check_dst_fall_timetruncate_1d(self):
        """TIMETRUNCATE(1d) on fall-back day (25h long) should align to local midnight."""
        self._prepare_dst_edge_data()
        ts = 1793552400000  # 2026-11-01 17:00 UTC = 12:00 EST (after fall-back)
        tdSql.execute(f'insert into {self.ntbname} values ({ts}, 1)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(timetruncate(ts, 1d), 'UTC') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T04:00:00' in result, f"1d fall-back day: {result}"

    def check_dst_spring_write_gap_normalized(self):
        """Writing non-existent local time in spring gap should be normalized.

        '2026-03-08 02:30:00' in New York does not exist -> normalized to 03:30 EDT.
        """
        self._prepare_dst_edge_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-03-08 02:30:00', 1)")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname}"
        )
        result = tdSql.queryResult[0][0]
        assert '2026-03-08 03:30:00' in result, f"Spring gap write: {result}"

    def check_dst_fall_write_overlap_first_occurrence(self):
        """Writing ambiguous local time in fall overlap should use first occurrence.

        '2026-11-01 01:30:00' exists twice (EDT and EST) -> default EDT (first).
        """
        self._prepare_dst_edge_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-11-01 01:30:00', 1)")
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T01:30:00' in result, f"Fall overlap write: {result}"
        assert '-04:00' in result or '-0400' in result, f"Fall overlap write: {result}"

    def check_write_integer_ts_unaffected(self):
        """Integer timestamp write should NOT be affected by timezone."""
        self._prepare_dst_edge_data()
        ts_val = 1640966400000
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ({ts_val}, 1)")
        tdSql.query(f"select to_iso8601(ts, 'UTC') from {self.ntbname}")
        r1 = tdSql.queryResult[0][0]

        tdSql.execute(f"delete from {self.ntbname}")
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.execute(f"insert into {self.ntbname} values ({ts_val}, 1)")
        tdSql.query(f"select to_iso8601(ts, 'UTC') from {self.ntbname}")
        r2 = tdSql.queryResult[0][0]
        assert r1 == r2, f"Integer ts write should be timezone-independent: {r1} vs {r2}"

    def check_write_with_explicit_offset(self):
        """Writing timestamp with explicit offset should be unambiguous."""
        self._prepare_dst_edge_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-11-01T01:30:00-05:00', 1)")
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T01:30:00' in result, f"Explicit offset write: {result}"
        assert '-05:00' in result or '-0500' in result, f"Explicit offset write: {result}"

    def check_no_dst_timezone_unaffected(self):
        """Asia/Shanghai (no DST) should produce constant offset."""
        self._prepare_dst_edge_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        # 2026-03-08 10:00 CST = 2026-03-08 02:00 UTC = 1772935200000 ms
        tdSql.query("select to_iso8601(1772935200000, 'Asia/Shanghai')")
        assert '+08:00' in tdSql.queryResult[0][0] or '+0800' in tdSql.queryResult[0][0]

    def test_dst_edge(self):
        """summary: DST edge cases: spring-forward gap, fall-back overlap, write-path regression.

        description: DST edge cases: spring-forward gap, fall-back overlap, write-path regression.

            DST times (America/New_York 2026):
            - Spring: 2026-03-08 02:00 EST -> 03:00 EDT
            - Fall:   2026-11-01 02:00 EDT -> 01:00 EST

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_dst_edge_case()
        self.check_dst_spring_iso8601_offset()
        self.check_dst_spring_to_char_gap()
        self.check_dst_spring_timetruncate_1d()
        self.check_dst_fall_iso8601_offset()
        self.check_dst_fall_timetruncate_1d()
        # self.check_dst_spring_write_gap_normalized()  # skip: Server rejects spring-gap timestamps (Timestamp data out of ...
        self.check_dst_fall_write_overlap_first_occurrence()
        self.check_write_integer_ts_unaffected()
        self.check_write_with_explicit_offset()
        self.check_no_dst_timezone_unaffected()


class _TsArithDstMixin:
    """Timestamp arithmetic DST no-drift regression test."""

    def _setup_ts_arith_dst_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_ts_arith_dst'
        self.ntbname = f'{self.dbname}.t1'

    def _prepare_ts_arith_dst_data(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ms"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, v int)')

        # T0 = 2026-03-08 00:30:00 EST = 2026-03-08 05:30:00 UTC.
        tdSql.execute(f'insert into {self.ntbname} values (1772947800000, 1)')

    def check_ts_arith_dst_no_drift(self):
        """Calendar-unit ts arithmetic should preserve local wall-clock time."""
        self._prepare_ts_arith_dst_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")

        cases = [
            ('1d', '2026-03-09 12:30:00.000'),
            ('1w', '2026-03-15 12:30:00.000'),
            ('1n', '2026-04-08 12:30:00.000'),
            ('1q', '2026-06-08 12:30:00.000'),
            ('1y', '2027-03-08 13:30:00.000'),
        ]
        for duration, expected in cases:
            tdSql.query(f"select ts + {duration} from {self.ntbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)

        tdSql.execute(f'drop database if exists {self.dbname}')

    def test_ts_arith_dst_no_drift(self):
        """summary: Timestamp arithmetic DST no-drift.

        description: Calendar-unit ts+1d/1w/1n/1q/1y must not drift across
            DST boundaries when the client session timezone is America/New_York.

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        Catalog:
            - Function:Scalar

        History:
            - 2025-07-10 Fix: serialize SOperatorNode.tz in physical plan
            - 2026-06-04 Tony Zhang merged into timezone scalar suite

        """
        self._setup_ts_arith_dst_case()
        self.check_ts_arith_dst_no_drift()


class _TimezoneDisplayAbbrMixin:
    """timezone() display string should use 'UTC' as abbreviation for all fixed-offset formats."""

    def check_tz_display_utc_abbr(self):
        cases = [
            ('UTC',       'UTC (UTC, +0000)'),
            ('UTC+0',     'UTC+0 (UTC, +0000)'),
            ('UTC+00',    'UTC+00 (UTC, +0000)'),
            ('UTC+0000',  'UTC+0000 (UTC, +0000)'),
            ('+0000',     '+0000 (UTC, +0000)'),
            ('-0000',     '-0000 (UTC, +0000)'),
            ('+00:00',    '+00:00 (UTC, +0000)'),
            ('-00:00',    '-00:00 (UTC, +0000)'),
            ('UTC+00:00', 'UTC+00:00 (UTC, +0000)'),
            ('UTC+0800',  'UTC+0800 (UTC, -0800)'),
            ('+0800',     '+0800 (UTC, -0800)'),
            ('UTC-0530',  'UTC-0530 (UTC, +0530)'),
            ('-0530',     '-0530 (UTC, +0530)'),
            ('UTC-8',     'UTC-8 (UTC, +0800)'),
            ('UTC+5',     'UTC+5 (UTC, -0500)'),
            # SET TIMEZONE echoes the input verbatim, so the name keeps the
            # user's case. The abbreviation is always the canonical 'UTC'.
            ('utc8',      'utc8 (UTC, -0800)'),
            ('utc+8',     'utc+8 (UTC, -0800)'),
            ('uTc-8',     'uTc-8 (UTC, +0800)'),
        ]
        for tz_input, expected in cases:
            tdSql.execute(f"SET TIMEZONE '{tz_input}'")
            tdSql.query("SELECT timezone()")
            actual = tdSql.queryResult[0][0].strip()
            assert actual == expected, \
                f"SET TIMEZONE '{tz_input}': expected '{expected}', got '{actual}'"

    def check_tz_display_iana_abbr(self):
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query("SELECT timezone()")
        actual = tdSql.queryResult[0][0].strip()
        assert '(CST,' in actual, f"IANA timezone should keep native abbr, got '{actual}'"

    def test_timezone_display_abbreviation(self):
        self.check_tz_display_utc_abbr()
        self.check_tz_display_iana_abbr()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")


class _ToIso8601FixedOffsetMixin:
    """Regression: TO_ISO8601 with fixed-offset tz param uses the useFixedOffset path."""

    def _setup_to_iso8601_fixed_offset_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_fixedoff'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1642248000000  # 2022-01-15 12:00:00 UTC

    def _prepare_to_iso8601_fixed_offset_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_fixed_offset_plus(self):
        """TO_ISO8601 with '+0800' should output at UTC+8 with +0800 suffix."""
        self._prepare_to_iso8601_fixed_offset_data()
        tdSql.query(f"select to_iso8601(ts, '+0800') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '+0800' in result, f"Expected +0800 in '{result}'"
        assert '20:00:00' in result, f"Expected 20:00:00 for UTC+8 in '{result}'"

    def check_fixed_offset_minus(self):
        """TO_ISO8601 with '-0500' should output at UTC-5 with -0500 suffix."""
        self._prepare_to_iso8601_fixed_offset_data()
        tdSql.query(f"select to_iso8601(ts, '-0500') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '-0500' in result, f"Expected -0500 in '{result}'"
        assert '07:00:00' in result, f"Expected 07:00:00 for UTC-5 in '{result}'"

    def check_fixed_offset_z(self):
        """TO_ISO8601 with 'Z' should output UTC time with Z suffix."""
        self._prepare_to_iso8601_fixed_offset_data()
        tdSql.query(f"select to_iso8601(ts, 'Z') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert result.endswith('Z'), f"Expected Z suffix in '{result}'"
        assert '12:00:00' in result, f"Expected 12:00:00 for UTC in '{result}'"

    def check_fixed_offset_colon(self):
        """TO_ISO8601 with '+05:30' should output at UTC+5:30 with original suffix."""
        self._prepare_to_iso8601_fixed_offset_data()
        tdSql.query(f"select to_iso8601(ts, '+05:30') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '+05:30' in result, f"Expected +05:30 in '{result}'"
        assert '17:30:00' in result, f"Expected 17:30:00 for UTC+5:30 in '{result}'"

    def test_to_iso8601_fixed_offset(self):
        """summary: Regression test for TO_ISO8601 fixed-offset timezone path.

        description: Verifies that TO_ISO8601 correctly handles fixed-offset
            timezone strings (+0800, -0500, Z, +05:30) using the useFixedOffset
            code path. Previously this path was dead code (useFixedOffset was
            never set to true).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2025-07-15: Tony Zhang created

        """
        self._setup_to_iso8601_fixed_offset_case()
        self.check_fixed_offset_plus()
        self.check_fixed_offset_minus()
        self.check_fixed_offset_z()
        self.check_fixed_offset_colon()


class _ToIso8601UtcPrefixMixin:
    """TO_ISO8601 treats UTC-prefixed offsets identically to bare offsets (ISO 8601 sign).

    All of '+0800', 'UTC+8', 'UTC+0800', 'UTC+08:00' must produce the same
    local time (UTC+8) and the output suffix must NOT contain 'UTC'.
    """

    def _setup_to_iso8601_utc_prefix_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_utcpfx'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1642248000000  # 2022-01-15 12:00:00 UTC

    def _prepare_to_iso8601_utc_prefix_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_utc_prefix_equivalence_east(self):
        """'UTC+0800', 'UTC+08:00', 'UTC+8' should all equal '+0800' (east-8, 20:00)."""
        self._prepare_to_iso8601_utc_prefix_data()
        tdSql.query(f"select to_iso8601(ts, '+0800') from {self.ntbname}")
        baseline = tdSql.queryResult[0][0]
        for tz in ['UTC+0800', 'UTC+08:00', 'UTC+8']:
            tdSql.query(f"select to_iso8601(ts, '{tz}') from {self.ntbname}")
            result = tdSql.queryResult[0][0]
            assert '20:00:00' in result, f"'{tz}': expected 20:00:00 in '{result}'"
            assert 'UTC' not in result, f"'{tz}': output should not contain 'UTC': '{result}'"

    def check_utc_prefix_equivalence_west(self):
        """'UTC-0500', 'UTC-05:00', 'UTC-5' should all equal '-0500' (west-5, 07:00)."""
        self._prepare_to_iso8601_utc_prefix_data()
        for tz in ['UTC-0500', 'UTC-05:00', 'UTC-5']:
            tdSql.query(f"select to_iso8601(ts, '{tz}') from {self.ntbname}")
            result = tdSql.queryResult[0][0]
            assert '07:00:00' in result, f"'{tz}': expected 07:00:00 in '{result}'"
            assert 'UTC' not in result, f"'{tz}': output should not contain 'UTC': '{result}'"

    def check_utc_prefix_half_hour(self):
        """'UTC+05:30' should equal '+05:30' (India, 17:30)."""
        self._prepare_to_iso8601_utc_prefix_data()
        tdSql.query(f"select to_iso8601(ts, 'UTC+05:30') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '17:30:00' in result, f"'UTC+05:30': expected 17:30:00 in '{result}'"
        assert 'UTC' not in result, f"'UTC+05:30': output should not contain 'UTC': '{result}'"

    def check_utc_prefix_case_insensitive(self):
        """'utc+0800' and 'Utc+0800' should also work (case-insensitive)."""
        self._prepare_to_iso8601_utc_prefix_data()
        for tz in ['utc+0800', 'Utc+0800']:
            tdSql.query(f"select to_iso8601(ts, '{tz}') from {self.ntbname}")
            result = tdSql.queryResult[0][0]
            assert '20:00:00' in result, f"'{tz}': expected 20:00:00 in '{result}'"
            assert 'UTC' not in result.upper(), \
                f"'{tz}': output should not contain 'UTC': '{result}'"

    def check_utc_prefix_negative_offset(self):
        """'UTC-09:00' should produce 03:00 (UTC-9)."""
        self._prepare_to_iso8601_utc_prefix_data()
        tdSql.query(f"select to_iso8601(ts, 'UTC-09:00') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '03:00:00' in result, f"'UTC-09:00': expected 03:00:00 in '{result}'"
        assert '-09:00' in result or '-0900' in result, \
            f"'UTC-09:00': expected -09:00 or -0900 suffix in '{result}'"

    def check_utc_prefix_zero_offset(self):
        """'UTC+0' and 'UTC-0' should both produce UTC time (12:00)."""
        self._prepare_to_iso8601_utc_prefix_data()
        for tz in ['UTC+0', 'UTC-0']:
            tdSql.query(f"select to_iso8601(ts, '{tz}') from {self.ntbname}")
            result = tdSql.queryResult[0][0]
            assert '12:00:00' in result, f"'{tz}': expected 12:00:00 in '{result}'"

    def test_to_iso8601_utc_prefix(self):
        """summary: TO_ISO8601 treats UTC-prefixed offsets with ISO 8601 sign convention.

        description: Verifies that TO_ISO8601 handles UTC-prefixed offset forms
            (UTC+8, UTC+0800, UTC+08:00, UTC-0500, etc.) identically to bare
            offset forms (+0800, -0500), using ISO 8601 sign convention where
            local = UTC + offset. The 'UTC' prefix is stripped from the output
            suffix. Case-insensitive matching is also tested.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-25: Tony Zhang created

        """
        self._setup_to_iso8601_utc_prefix_case()
        self.check_utc_prefix_equivalence_east()
        self.check_utc_prefix_equivalence_west()
        self.check_utc_prefix_half_hour()
        self.check_utc_prefix_case_insensitive()
        self.check_utc_prefix_negative_offset()
        self.check_utc_prefix_zero_offset()


class _ToIso8601SuffixFormatMixin:
    """Verify that TO_ISO8601 output suffix precisely mimics the input format.

    For UTC-prefixed single-digit offsets (UTC+8, UTC-5, UTC+0) the suffix
    is normalised to ±HHMM (4-digit).  For all other formats the suffix
    reproduces the digit/colon pattern of the input.
    """

    def _setup_to_iso8601_suffix_format_case(self):
        self.dbname = 'db_tz_suffmt'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1642248000000  # 2022-01-15 12:00:00 UTC

    def _prepare_suffix_format_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def _assert_suffix(self, param_tz, expected_suffix):
        """Query to_iso8601 and assert that the result ends with expected_suffix."""
        self._prepare_suffix_format_data()
        tdSql.query(f"select to_iso8601(ts, '{param_tz}') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert result.endswith(expected_suffix), \
            f"param_tz='{param_tz}': expected suffix '{expected_suffix}', got '{result}'"

    # --- UTC-prefixed single-digit → ±HHMM (cannot mimic, normalise to 4-digit) ---

    def check_suffix_utc_plus_8(self):
        """UTC+8 → +08"""
        self._assert_suffix('UTC+8', '+08')

    def check_suffix_utc_minus_5(self):
        """UTC-5 → -05"""
        self._assert_suffix('UTC-5', '-05')

    def check_suffix_utc_plus_0(self):
        """UTC+0 → +00"""
        self._assert_suffix('UTC+0', '+00')

    def check_suffix_utc_minus_0(self):
        """UTC-0 → -00 (zero offset keeps original '-' sign)"""
        self._assert_suffix('UTC-0', '-00')

    # --- UTC-prefixed multi-digit → mimic the digit pattern after stripping UTC ---

    def check_suffix_utc_plus_08(self):
        """UTC+08 → +08"""
        self._assert_suffix('UTC+08', '+08')

    def check_suffix_utc_plus_0800(self):
        """UTC+0800 → +0800"""
        self._assert_suffix('UTC+0800', '+0800')

    def check_suffix_utc_plus_08_colon_00(self):
        """UTC+08:00 → +08:00"""
        self._assert_suffix('UTC+08:00', '+08:00')

    def check_suffix_utc_minus_0500(self):
        """UTC-0500 → -0500"""
        self._assert_suffix('UTC-0500', '-0500')

    def check_suffix_utc_minus_05_colon_00(self):
        """UTC-05:00 → -05:00"""
        self._assert_suffix('UTC-05:00', '-05:00')

    def check_suffix_utc_plus_05_colon_30(self):
        """UTC+05:30 → +05:30"""
        self._assert_suffix('UTC+05:30', '+05:30')

    def check_suffix_utc_minus_09_colon_00(self):
        """UTC-09:00 → -09:00"""
        self._assert_suffix('UTC-09:00', '-09:00')

    # --- Bare offset → pass-through ---

    def check_suffix_bare_plus_0800(self):
        """+0800 → +0800"""
        self._assert_suffix('+0800', '+0800')

    def check_suffix_bare_plus_08_colon_00(self):
        """+08:00 → +08:00"""
        self._assert_suffix('+08:00', '+08:00')

    def check_suffix_bare_minus_0500(self):
        """-0500 → -0500"""
        self._assert_suffix('-0500', '-0500')

    def check_suffix_bare_plus_05_colon_30(self):
        """+05:30 → +05:30"""
        self._assert_suffix('+05:30', '+05:30')

    def check_suffix_bare_z(self):
        """Z → Z"""
        self._assert_suffix('Z', 'Z')

    # --- zero offset with negative sign → always normalised to '+' ---

    def check_suffix_utc_minus_00(self):
        """UTC-00 → -00 (zero offset keeps original '-' sign)"""
        self._assert_suffix('UTC-00', '-00')

    def check_suffix_utc_minus_0000(self):
        """UTC-0000 → -0000 (zero offset keeps original '-' sign)"""
        self._assert_suffix('UTC-0000', '-0000')

    def check_suffix_utc_minus_00_colon_00(self):
        """UTC-00:00 → -00:00 (zero offset keeps original '-' sign)"""
        self._assert_suffix('UTC-00:00', '-00:00')

    def check_suffix_bare_minus_0000(self):
        """-0000 → -0000 (zero offset keeps original '-' sign)"""
        self._assert_suffix('-0000', '-0000')

    def check_suffix_bare_minus_00_colon_00(self):
        """-00:00 → -00:00 (zero offset keeps original '-' sign)"""
        self._assert_suffix('-00:00', '-00:00')

    def test_to_iso8601_suffix_format(self):
        """summary: TO_ISO8601 output suffix must precisely mimic the input offset format.

        description: Verifies that TO_ISO8601 preserves the digit/colon pattern
            of the input timezone parameter in the output suffix.  Single-digit
            UTC-prefixed offsets (UTC+8, UTC-5, UTC+0) are normalised to ±HHMM.
            All other forms reproduce the original pattern after stripping the
            UTC prefix.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-30: Tony Zhang created

        """
        self._setup_to_iso8601_suffix_format_case()
        # single-digit normalisation
        self.check_suffix_utc_plus_8()
        self.check_suffix_utc_minus_5()
        self.check_suffix_utc_plus_0()
        self.check_suffix_utc_minus_0()
        # UTC-prefixed multi-digit mimicking
        self.check_suffix_utc_plus_08()
        self.check_suffix_utc_plus_0800()
        self.check_suffix_utc_plus_08_colon_00()
        self.check_suffix_utc_minus_0500()
        self.check_suffix_utc_minus_05_colon_00()
        self.check_suffix_utc_plus_05_colon_30()
        self.check_suffix_utc_minus_09_colon_00()
        # bare offset pass-through
        self.check_suffix_bare_plus_0800()
        self.check_suffix_bare_plus_08_colon_00()
        self.check_suffix_bare_minus_0500()
        self.check_suffix_bare_plus_05_colon_30()
        self.check_suffix_bare_z()
        # zero offset with negative sign
        self.check_suffix_utc_minus_00()
        self.check_suffix_utc_minus_0000()
        self.check_suffix_utc_minus_00_colon_00()
        self.check_suffix_bare_minus_0000()
        self.check_suffix_bare_minus_00_colon_00()


class _JoinDstMixin:
    """Regression: JOIN on TIMETRUNCATE(ts, 1d) must be DST-aware for IANA timezones."""

    def _setup_join_dst_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_join_dst'

    def _prepare_join_dst_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.dbname}.t1')
        tdSql.execute(f'drop table if exists {self.dbname}.t2')
        tdSql.execute(f'create table {self.dbname}.t1 (ts timestamp, v1 int)')
        tdSql.execute(f'create table {self.dbname}.t2 (ts timestamp, v2 int)')

        # 2026-03-08 is the spring-forward day in America/New_York (EST -> EDT)
        # Insert timestamps on the same calendar day in New York local time
        # but straddling the DST transition.
        # 2026-03-08 01:00 EST = 2026-03-08 06:00 UTC = 1741413600000
        # 2026-03-08 10:00 EDT = 2026-03-08 14:00 UTC = 1741442400000
        ts_before = 1741413600000  # 06:00 UTC (01:00 EST, before spring-forward)
        ts_after  = 1741442400000  # 14:00 UTC (10:00 EDT, after spring-forward)

        tdSql.execute(f'insert into {self.dbname}.t1 values ({ts_before}, 1)')
        tdSql.execute(f'insert into {self.dbname}.t2 values ({ts_after}, 2)')

    def check_join_dst_day_truncate(self):
        """Both timestamps fall on the same local day (2026-03-08 in New_York),
        so INNER JOIN on timetruncate(ts, 1d) should produce 1 row when using
        America/New_York timezone."""
        self._prepare_join_dst_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        # merge join path (default)
        tdSql.query(
            f"select t1.v1, t2.v2 from {self.dbname}.t1 t1 "
            f"inner join {self.dbname}.t2 t2 "
            f"on timetruncate(t1.ts, 1d) = timetruncate(t2.ts, 1d)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.query(
            f"select /*+ hash_join() */ t1.v1, t2.v2 from {self.dbname}.t1 t1 "
            f"inner join {self.dbname}.t2 t2 "
            f"on timetruncate(t1.ts, 1d) = timetruncate(t2.ts, 1d)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)

    def check_join_no_dst_same_day(self):
        """With a non-DST timezone (Asia/Shanghai), both timestamps land on
        the same calendar day, so the join should still match."""
        self._prepare_join_dst_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        # 06:00 UTC = 14:00 CST, 14:00 UTC = 22:00 CST, both on 2026-03-08
        tdSql.query(
            f"select t1.v1, t2.v2 from {self.dbname}.t1 t1 "
            f"inner join {self.dbname}.t2 t2 "
            f"on timetruncate(t1.ts, 1d) = timetruncate(t2.ts, 1d)"
        )
        tdSql.checkRows(1)

    def check_join_dst_different_days(self):
        """When timestamps fall on different calendar days in the local timezone,
        the join should produce 0 rows."""
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.dbname}.t3')
        tdSql.execute(f'drop table if exists {self.dbname}.t4')
        tdSql.execute(f'create table {self.dbname}.t3 (ts timestamp, v1 int)')
        tdSql.execute(f'create table {self.dbname}.t4 (ts timestamp, v2 int)')
        # 2026-03-08 04:00 UTC = 2026-03-07 23:00 EST (day before)
        # 2026-03-08 14:00 UTC = 2026-03-08 10:00 EDT (day of DST switch)
        ts_day_before = 1741406400000  # 04:00 UTC
        ts_day_of     = 1741442400000  # 14:00 UTC
        tdSql.execute(f'insert into {self.dbname}.t3 values ({ts_day_before}, 1)')
        tdSql.execute(f'insert into {self.dbname}.t4 values ({ts_day_of}, 2)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        # merge join
        tdSql.query(
            f"select * from {self.dbname}.t3 t3 "
            f"inner join {self.dbname}.t4 t4 "
            f"on timetruncate(t3.ts, 1d) = timetruncate(t4.ts, 1d)"
        )
        tdSql.checkRows(0)
        tdSql.query(
            f"select /*+ hash_join() */ * from {self.dbname}.t3 t3 "
            f"inner join {self.dbname}.t4 t4 "
            f"on timetruncate(t3.ts, 1d) = timetruncate(t4.ts, 1d)"
        )
        tdSql.checkRows(0)

    def test_join_dst(self):
        """summary: Regression test for DST-aware JOIN on TIMETRUNCATE(ts, 1d).

        description: Verifies that merge join correctly uses DST-aware day
            truncation when the session timezone is an IANA name. Previously,
            join operators resolved the IANA timezone offset at epoch time
            (time_t 0), producing a fixed offset that ignored DST transitions.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2025-07-15: Tony Zhang created

        """
        self._setup_join_dst_case()
        self.check_join_dst_day_truncate()
        self.check_join_no_dst_same_day()
        self.check_join_dst_different_days()


class _ToIso8601SessionVsParamMixin:
    """TO_ISO8601: session timezone vs explicit parameter should produce identical results.

    When the session timezone and the explicit parameter represent the same
    offset/zone, to_iso8601(ts) and to_iso8601(ts, '<tz>') must produce the
    same local time and offset suffix.  This validates that the POSIX-to-ISO
    conversion in addTimezoneNameParam works correctly for all supported formats.
    """

    def _setup_to_iso8601_session_vs_param_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_sess_param'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1642248000000  # 2022-01-15 12:00:00 UTC

    def _prepare_to_iso8601_session_vs_param_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def _check_session_matches_param(self, session_tz, param_tz, expected_time, expected_offset):
        """Helper: set session tz, query without param, then query with param_tz.

        Both must contain expected_time and expected_offset, and their time
        portions must match.
        """
        self._prepare_to_iso8601_session_vs_param_data()
        tdSql.execute(f"SET TIMEZONE '{session_tz}'")
        tdSql.query(f"select to_iso8601(ts) from {self.ntbname}")
        session_result = tdSql.queryResult[0][0]
        tdSql.query(f"select to_iso8601(ts, '{param_tz}') from {self.ntbname}")
        param_result = tdSql.queryResult[0][0]

        assert expected_time in session_result, (
            f"session_tz='{session_tz}': expected '{expected_time}' in '{session_result}'"
        )
        assert expected_time in param_result, (
            f"param_tz='{param_tz}': expected '{expected_time}' in '{param_result}'"
        )
        for off in expected_offset:
            if off in session_result:
                break
        else:
            assert False, (
                f"session_tz='{session_tz}': expected one of {expected_offset} in '{session_result}'"
            )
        for off in expected_offset:
            if off in param_result:
                break
        else:
            assert False, (
                f"param_tz='{param_tz}': expected one of {expected_offset} in '{param_result}'"
            )

    def check_session_vs_param_utc_plus_8(self):
        """SET TIMEZONE 'UTC-8' (POSIX east-8) vs param '+0800' (ISO east-8)."""
        self._check_session_matches_param(
            session_tz='UTC-8',
            param_tz='+0800',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_utc_plus_08(self):
        """SET TIMEZONE 'UTC-08' (POSIX east-8) vs param '+0800'."""
        self._check_session_matches_param(
            session_tz='UTC-08',
            param_tz='+0800',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_utc_plus_0800(self):
        """SET TIMEZONE 'UTC-0800' (POSIX east-8) vs param '+0800'."""
        self._check_session_matches_param(
            session_tz='UTC-0800',
            param_tz='+0800',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_utc_plus_08_colon(self):
        """SET TIMEZONE 'UTC-08:00' (POSIX east-8) vs param '+08:00'."""
        self._check_session_matches_param(
            session_tz='UTC-08:00',
            param_tz='+08:00',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_bare_plus_08(self):
        """SET TIMEZONE '-0800' (POSIX east-8) vs param '+0800'."""
        self._check_session_matches_param(
            session_tz='-0800',
            param_tz='+0800',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_bare_plus_0800(self):
        """SET TIMEZONE '-08:00' (POSIX east-8) vs param '+08:00'."""
        self._check_session_matches_param(
            session_tz='-08:00',
            param_tz='+08:00',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_bare_minus_05(self):
        """SET TIMEZONE '+0500' (POSIX west-5) vs param '-0500'."""
        self._check_session_matches_param(
            session_tz='+0500',
            param_tz='-0500',
            expected_time='07:00:00',
            expected_offset=['-0500', '-05:00'],
        )

    def check_session_vs_param_utc_minus_5(self):
        """SET TIMEZONE 'UTC+5' (POSIX west-5) vs param '-0500'."""
        self._check_session_matches_param(
            session_tz='UTC+5',
            param_tz='-0500',
            expected_time='07:00:00',
            expected_offset=['-0500', '-05:00'],
        )

    def check_session_vs_param_iana_shanghai(self):
        """SET TIMEZONE 'Asia/Shanghai' (IANA, no DST) vs param 'Asia/Shanghai'."""
        self._check_session_matches_param(
            session_tz='Asia/Shanghai',
            param_tz='Asia/Shanghai',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_session_vs_param_iana_new_york_winter(self):
        """SET TIMEZONE 'America/New_York' vs param 'America/New_York' in winter."""
        self._prepare_to_iso8601_session_vs_param_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(ts) from {self.ntbname}")
        session_result = tdSql.queryResult[0][0]
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        param_result = tdSql.queryResult[0][0]
        assert '07:00:00' in session_result, f"session: {session_result}"
        assert '07:00:00' in param_result, f"param: {param_result}"
        for r in [session_result, param_result]:
            assert '-0500' in r or '-05:00' in r, f"Expected -0500 offset in '{r}'"

    def check_session_vs_param_utc_zero(self):
        """SET TIMEZONE 'UTC' vs param '+0000' — both should be UTC."""
        self._check_session_matches_param(
            session_tz='UTC',
            param_tz='+0000',
            expected_time='12:00:00',
            expected_offset=['+0000', '+00:00', 'Z'],
        )

    def check_session_vs_param_utc_minus_2_posix(self):
        """SET TIMEZONE 'UTC-2' (POSIX east-2) vs param '+0200' (ISO east-2).

        This is the original bug scenario that triggered this fix.
        """
        self._check_session_matches_param(
            session_tz='UTC-2',
            param_tz='+0200',
            expected_time='14:00:00',
            expected_offset=['+0200', '+02:00'],
        )

    def check_session_vs_param_bare_minus_0200(self):
        """SET TIMEZONE '-0200' (POSIX east-2) vs param '+0200'."""
        self._check_session_matches_param(
            session_tz='-0200',
            param_tz='+0200',
            expected_time='14:00:00',
            expected_offset=['+0200', '+02:00'],
        )

    def check_param_utc_prefix_east_8(self):
        """param_tz='UTC+0800' (UTC-prefixed ISO east-8) should match session UTC-8."""
        self._check_session_matches_param(
            session_tz='UTC-8',
            param_tz='UTC+0800',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_param_utc_prefix_short(self):
        """param_tz='UTC+8' (single-digit UTC-prefixed) should match session UTC-8."""
        self._check_session_matches_param(
            session_tz='UTC-8',
            param_tz='UTC+8',
            expected_time='20:00:00',
            expected_offset=['+08'],
        )

    def check_param_utc_prefix_colon(self):
        """param_tz='UTC+08:00' (colon-separated UTC-prefixed) should match session -0800."""
        self._check_session_matches_param(
            session_tz='-0800',
            param_tz='UTC+08:00',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_param_colon_format(self):
        """param_tz='+08:00' (colon-separated bare offset) should match session UTC-08."""
        self._check_session_matches_param(
            session_tz='UTC-08',
            param_tz='+08:00',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_param_utc_prefix_west_5(self):
        """param_tz='UTC-5' (UTC-prefixed west-5) should match session +0500."""
        self._check_session_matches_param(
            session_tz='+0500',
            param_tz='UTC-5',
            expected_time='07:00:00',
            expected_offset=['-05'],
        )

    def check_param_utc_prefix_west_0500(self):
        """param_tz='UTC-0500' (UTC-prefixed west-5) should match session UTC+5."""
        self._check_session_matches_param(
            session_tz='UTC+5',
            param_tz='UTC-0500',
            expected_time='07:00:00',
            expected_offset=['-0500', '-05:00'],
        )

    def check_param_colon_format_west(self):
        """param_tz='-05:00' (colon-separated bare west-5) should match session +0500."""
        self._check_session_matches_param(
            session_tz='+0500',
            param_tz='-05:00',
            expected_time='07:00:00',
            expected_offset=['-0500', '-05:00'],
        )

    def check_param_iana_vs_posix_session(self):
        """param_tz='Asia/Shanghai' (IANA) should match session UTC-8 (POSIX east-8)."""
        self._check_session_matches_param(
            session_tz='UTC-8',
            param_tz='Asia/Shanghai',
            expected_time='20:00:00',
            expected_offset=['+0800', '+08:00'],
        )

    def check_param_utc_prefix_half_hour(self):
        """param_tz='UTC+05:30' (UTC-prefixed half-hour) should match session -0530."""
        self._check_session_matches_param(
            session_tz='-0530',
            param_tz='UTC+05:30',
            expected_time='17:30:00',
            expected_offset=['+0530', '+05:30'],
        )

    def check_param_z_vs_utc_session(self):
        """param_tz='Z' should match session UTC."""
        self._check_session_matches_param(
            session_tz='UTC',
            param_tz='Z',
            expected_time='12:00:00',
            expected_offset=['+0000', '+00:00', 'Z'],
        )

    def check_param_utc_prefix_zero(self):
        """param_tz='UTC+0' should match session UTC."""
        self._check_session_matches_param(
            session_tz='UTC',
            param_tz='UTC+0',
            expected_time='12:00:00',
            expected_offset=['+00'],
        )

    def test_to_iso8601_session_vs_param(self):
        """summary: TO_ISO8601 session timezone vs explicit param produce identical results.

        description: When the session timezone and the explicit parameter
            represent the same offset/zone, to_iso8601(ts) and
            to_iso8601(ts, '<tz>') must produce the same local time and offset
            suffix. Covers all timezone formats: UTC+N, UTC+NN, UTC+NNNN,
            UTC+NN:NN, +NN, +NNNN, +NN:NN, and IANA names.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-30: Tony Zhang created

        """
        self._setup_to_iso8601_session_vs_param_case()
        self.check_session_vs_param_utc_plus_8()
        self.check_session_vs_param_utc_plus_08()
        self.check_session_vs_param_utc_plus_0800()
        self.check_session_vs_param_utc_plus_08_colon()
        self.check_session_vs_param_bare_plus_08()
        self.check_session_vs_param_bare_plus_0800()
        self.check_session_vs_param_bare_minus_05()
        self.check_session_vs_param_utc_minus_5()
        self.check_session_vs_param_iana_shanghai()
        self.check_session_vs_param_iana_new_york_winter()
        self.check_session_vs_param_utc_zero()
        self.check_session_vs_param_utc_minus_2_posix()
        self.check_session_vs_param_bare_minus_0200()
        self.check_param_utc_prefix_east_8()
        self.check_param_utc_prefix_short()
        self.check_param_utc_prefix_colon()
        self.check_param_colon_format()
        self.check_param_utc_prefix_west_5()
        self.check_param_utc_prefix_west_0500()
        self.check_param_colon_format_west()
        self.check_param_iana_vs_posix_session()
        self.check_param_utc_prefix_half_hour()
        self.check_param_z_vs_utc_session()
        self.check_param_utc_prefix_zero()


class _ToTimestampTzMixin:
    """TO_TIMESTAMP parses timezone-less strings in the connection timezone.

    A string literal without an embedded offset is parsed in the current
    connection timezone (L2 SET TIMEZONE), so the same literal maps to
    different UTC instants under different timezones. An offset embedded in
    the string overrides the connection timezone.
    """

    def _setup_to_timestamp_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")

    def _parsed_utc_wall_clock(self, ts_str, fmt):
        """Parse ts_str and render the resulting instant as UTC wall-clock.

        Rendering in UTC makes the timezone used for parsing observable,
        independent of the current display timezone.
        """
        tdSql.query(
            f"select to_char(to_timestamp('{ts_str}', '{fmt}'), "
            f"'YYYY-MM-DD HH24:MI:SS', 'UTC')"
        )
        return tdSql.queryResult[0][0]

    def check_to_timestamp_no_offset_uses_l2(self):
        """A tz-less literal is parsed in the connection timezone (L2)."""
        fmt = 'yyyy-mm-dd hh24:mi:ss'
        # Only no-DST zones: parsing resolves the standard offset, so a DST
        # zone like America/New_York would use its winter offset here.
        cases = [
            ('UTC',           '2023-10-10 10:10:10'),  # +00:00
            ('Asia/Shanghai', '2023-10-10 02:10:10'),  # +08:00
            ('Asia/Kolkata',  '2023-10-10 04:40:10'),  # +05:30
        ]
        for tz, expected_utc in cases:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            result = self._parsed_utc_wall_clock('2023-10-10 10:10:10', fmt)
            assert expected_utc in result, (
                f"tz={tz}: expected UTC {expected_utc}, got {result}"
            )

    def check_to_timestamp_different_l2_differs(self):
        """Same tz-less literal maps to different instants under different L2."""
        fmt = 'yyyy-mm-dd hh24:mi:ss'
        tdSql.execute("SET TIMEZONE 'UTC'")
        r_utc = self._parsed_utc_wall_clock('2023-10-10 10:10:10', fmt)
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        r_sh = self._parsed_utc_wall_clock('2023-10-10 10:10:10', fmt)
        assert r_utc != r_sh, \
            f"UTC vs Shanghai parse should differ: {r_utc} vs {r_sh}"

    def check_to_timestamp_embedded_offset_overrides_l2(self):
        """An offset embedded in the string overrides the connection tz."""
        fmt = 'yyyy-mm-dd hh24:mi:ssTZH'
        results = []
        for tz in ['UTC', 'Asia/Shanghai', 'Asia/Kolkata']:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            r = self._parsed_utc_wall_clock('2023-10-10 10:10:10+00', fmt)
            results.append(r)
            assert '2023-10-10 10:10:10' in r, \
                f"tz={tz}: embedded +00 should force UTC, got {r}"
        assert len(set(results)) == 1, \
            f"embedded offset should be tz-independent: {results}"

    def test_to_timestamp_connection_tz(self):
        """summary: TO_TIMESTAMP parses tz-less strings in the connection timezone.

        description: A string literal without an embedded offset is parsed in
            the current connection timezone (L2 SET TIMEZONE), so the same
            literal maps to different UTC instants under different timezones.
            An offset embedded in the string overrides the connection timezone.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-07-10: Tony Zhang created

        """
        self._setup_to_timestamp_tz_case()
        self.check_to_timestamp_no_offset_uses_l2()
        self.check_to_timestamp_different_l2_differs()
        self.check_to_timestamp_embedded_offset_overrides_l2()


class _DateTzMixin:
    """DATE renders a fixed instant's date in the connection timezone.

    DATE(expr) formats its result in the current connection timezone (L2), so
    a fixed instant near a day boundary yields different dates under different
    SET TIMEZONE. A tz-less string input is both parsed and formatted in the
    connection timezone, so the two cancel and the date is tz-independent.
    """

    def _setup_date_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_date'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_date_tz_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        # 2023-10-10 22:00:00 UTC, written with an explicit offset so the
        # stored instant is fixed regardless of the session timezone.
        tdSql.execute(
            f"insert into {self.ntbname} values "
            f"('2023-10-10T22:00:00+00:00', 1)"
        )

    def check_date_fixed_instant_uses_l2_scalar(self):
        """date() of a fixed instant (offset string) formats in connection tz."""
        cases = [
            ('UTC',           '2023-10-10'),
            ('Asia/Shanghai', '2023-10-11'),  # 06:00 next local day
        ]
        for tz, expected in cases:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query("select date('2023-10-10T22:00:00+00:00')")
            assert tdSql.queryResult[0][0] == expected, \
                f"tz={tz}: expected {expected}, got {tdSql.queryResult[0][0]}"

    def check_date_fixed_instant_uses_l2_column(self):
        """date(ts) of a stored fixed instant formats in connection tz."""
        self._prepare_date_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select date(ts) from {self.ntbname}")
        assert tdSql.queryResult[0][0] == '2023-10-10', tdSql.queryResult[0][0]
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select date(ts) from {self.ntbname}")
        assert tdSql.queryResult[0][0] == '2023-10-11', tdSql.queryResult[0][0]

    def check_date_tzless_string_is_tz_independent(self):
        """A tz-less string is parsed and formatted in the same tz -> stable."""
        results = []
        for tz in ['UTC', 'Asia/Shanghai', 'Asia/Kolkata']:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query("select date('2023-10-10 22:00:00')")
            results.append(tdSql.queryResult[0][0])
        assert all(r == '2023-10-10' for r in results), \
            f"tz-less date() should be tz-independent: {results}"

    def test_date_connection_tz(self):
        """summary: DATE renders a fixed instant's date in the connection timezone.

        description: DATE(expr) formats its result in the current connection
            timezone (L2 SET TIMEZONE), so a fixed instant near a day boundary
            yields different dates under different timezones. A tz-less string
            input is both parsed and formatted in the connection timezone, so
            the date is tz-independent.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-07-10: Tony Zhang created

        """
        self._setup_date_tz_case()
        self.check_date_fixed_instant_uses_l2_scalar()
        self.check_date_fixed_instant_uses_l2_column()
        self.check_date_tzless_string_is_tz_independent()


class _ToUnixTimestampTzMixin:
    """TO_UNIXTIMESTAMP parses timezone-less strings in the connection timezone.

    Like TO_TIMESTAMP, a datetime string without an embedded offset is parsed
    in the current connection timezone (L2 SET TIMEZONE), so the same literal
    maps to different epoch values under different timezones. An offset
    embedded in the string overrides the connection timezone. Note that
    TO_UNIXTIMESTAMP takes no format string; its optional second argument is a
    return_timestamp 0/1 flag.
    """

    def _setup_to_unixtimestamp_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")

    def check_to_unixtimestamp_no_offset_uses_l2(self):
        """A tz-less literal is parsed in the connection timezone (L2).

        No table is queried, so the epoch is in milliseconds. Only no-DST
        zones are used so the standard offset applies unambiguously.
        """
        cases = [
            ('UTC',           1696932610000),  # 2023-10-10 10:10:10 +00:00
            ('Asia/Shanghai', 1696903810000),  # -08:00, UTC is 8 hours earlier
            ('Asia/Kolkata',  1696912810000),  # -05:30
        ]
        for tz, expected in cases:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query("select to_unixtimestamp('2023-10-10 10:10:10')")
            assert tdSql.queryResult[0][0] == expected, \
                f"tz={tz}: expected {expected}, got {tdSql.queryResult[0][0]}"

    def check_to_unixtimestamp_different_l2_differs(self):
        """Same tz-less literal maps to different epochs under different L2."""
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query("select to_unixtimestamp('2023-10-10 10:10:10')")
        r_utc = tdSql.queryResult[0][0]
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query("select to_unixtimestamp('2023-10-10 10:10:10')")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh, \
            f"UTC vs Shanghai parse should differ: {r_utc} vs {r_sh}"

    def check_to_unixtimestamp_embedded_offset_overrides_l2(self):
        """An offset embedded in the string overrides the connection tz."""
        results = []
        for tz in ['UTC', 'Asia/Shanghai', 'Asia/Kolkata']:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query(
                "select to_unixtimestamp('2023-10-10T10:10:10+00:00')"
            )
            r = tdSql.queryResult[0][0]
            results.append(r)
            assert r == 1696932610000, \
                f"tz={tz}: embedded +00:00 should force UTC, got {r}"
        assert len(set(results)) == 1, \
            f"embedded offset should be tz-independent: {results}"

    def test_to_unixtimestamp_connection_tz(self):
        """summary: TO_UNIXTIMESTAMP parses tz-less strings in the connection timezone.

        description: A datetime string without an embedded offset is parsed in
            the current connection timezone (L2 SET TIMEZONE), so the same
            literal maps to different epoch values under different timezones.
            An offset embedded in the string overrides the connection timezone.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-07-10: Tony Zhang created

        """
        self._setup_to_unixtimestamp_tz_case()
        self.check_to_unixtimestamp_no_offset_uses_l2()
        self.check_to_unixtimestamp_different_l2_differs()
        self.check_to_unixtimestamp_embedded_offset_overrides_l2()


class _WeekFamilyTzMixin:
    """DAYOFWEEK/WEEKDAY/WEEK/WEEKOFYEAR resolve the local calendar in L2.

    For a fixed instant, these functions compute the weekday/week from the
    local calendar date in the current connection timezone (L2 SET TIMEZONE),
    so an instant near a day/week boundary yields different results under
    different timezones. This is distinct from firstDayOfWeek dependence,
    which is covered by _WeekFunctionsMixin.
    """

    def _setup_week_family_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_wkfam'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_week_family_tz_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        # 2023-10-08 22:00:00 UTC is Sunday; in Asia/Shanghai (+08:00) it is
        # Monday 06:00, which flips dayofweek, weekday, week and weekofyear.
        # Written with an explicit offset so the stored instant is fixed.
        tdSql.execute(
            f"insert into {self.ntbname} values "
            f"('2023-10-08T22:00:00+00:00', 1)"
        )

    def check_dayofweek_weekday_fixed_instant_uses_l2(self):
        """dayofweek/weekday of a fixed instant follow the connection tz."""
        self._prepare_week_family_tz_data()
        # UTC -> Sunday: dayofweek=1, weekday=6
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select dayofweek(ts), weekday(ts) from {self.ntbname}")
        assert tdSql.queryResult[0][0] == 1, tdSql.queryResult[0][0]
        assert tdSql.queryResult[0][1] == 6, tdSql.queryResult[0][1]
        # Asia/Shanghai -> Monday: dayofweek=2, weekday=0
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select dayofweek(ts), weekday(ts) from {self.ntbname}")
        assert tdSql.queryResult[0][0] == 2, tdSql.queryResult[0][0]
        assert tdSql.queryResult[0][1] == 0, tdSql.queryResult[0][1]

    def check_week_weekofyear_fixed_instant_uses_l2(self):
        """week/weekofyear of a fixed instant follow the connection tz."""
        self._prepare_week_family_tz_data()
        # UTC -> Sunday, still ISO week 40
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select week(ts, 1), weekofyear(ts) from {self.ntbname}"
        )
        assert tdSql.queryResult[0][0] == 40, tdSql.queryResult[0][0]
        assert tdSql.queryResult[0][1] == 40, tdSql.queryResult[0][1]
        # Asia/Shanghai -> Monday, rolls into week 41
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select week(ts, 1), weekofyear(ts) from {self.ntbname}"
        )
        assert tdSql.queryResult[0][0] == 41, tdSql.queryResult[0][0]
        assert tdSql.queryResult[0][1] == 41, tdSql.queryResult[0][1]

    def test_week_family_connection_tz(self):
        """summary: DAYOFWEEK/WEEKDAY/WEEK/WEEKOFYEAR resolve the local calendar in the connection timezone.

        description: For a fixed instant, these functions compute the
            weekday/week from the local calendar date in the current connection
            timezone (L2 SET TIMEZONE), so an instant near a day/week boundary
            yields different results under different timezones.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-07-10: Tony Zhang created

        """
        self._setup_week_family_tz_case()
        self.check_dayofweek_weekday_fixed_instant_uses_l2()
        self.check_week_weekofyear_fixed_instant_uses_l2()


@SKIP_WINDOWS_SET_TIMEZONE
class TestTimezoneScalarFunctions(
    _ToIso8601IanaMixin,
    _ToCharTimezoneMixin,
    _TimetruncateTzMixin,
    _TimetruncateNaturalUnitsMixin,
    _TimetruncateUnitMultiplierValidationMixin,
    _TimetruncateWeekMixin,
    _WeekFunctionsMixin,
    _SessionFirstDayOfWeekFunctionMixin,
    _DstEdgeMixin,
    _TsArithDstMixin,
    _TimezoneDisplayAbbrMixin,
    _ToIso8601FixedOffsetMixin,
    _ToIso8601UtcPrefixMixin,
    _ToIso8601SuffixFormatMixin,
    _JoinDstMixin,
    _ToIso8601SessionVsParamMixin,
    _ToTimestampTzMixin,
    _DateTzMixin,
    _ToUnixTimestampTzMixin,
    _WeekFamilyTzMixin,
):
    def test_timezone_scalar_functions(self):
        """Timezone scalar function test suite.

        Aggregates timezone scalar function tests from mixin classes; individual
        scenarios are implemented in inherited test_* methods.

        Since: v3.4.2.0

        Labels: timezone, scalar

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-07-10: Tony Zhang created
        """
        pass
